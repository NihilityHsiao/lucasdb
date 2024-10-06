use std::sync::atomic::Ordering;

use prost::decode_length_delimiter;

use crate::{
    batch::{log_record_key_with_seq, parse_log_record_key},
    data::{
        data_file::DataFile,
        log_record::{self, LogRecord, LogRecordPos, LogRecordType},
        HINT_FILE_NAME,
    },
    db::Engine,
    fio::IOType,
    merge::{get_merge_path, MERGE_FIN_KEY},
    options::EngineOptions,
    prelude::*,
    utils,
};

impl Engine {
    pub fn merge(&self) -> Result<()> {
        let lock = self.merging_lock.try_lock();
        if lock.is_none() {
            return Err(Errors::MergeInProgress);
        }

        // 判断是否达到阈值,达到了才需要merge
        let reclaim_size = self.reclaim_size.load(Ordering::SeqCst);
        let total_size = utils::file::dir_disk_size(&self.options.dir_path);
        let cur_ratio = reclaim_size as f32 / total_size as f32;
        if cur_ratio < self.options.data_file_merge_ratio {
            return Err(Errors::MergeRatioUnreached {
                now: cur_ratio,
                ratio: self.options.data_file_merge_ratio,
            });
        }

        // 判断磁盘容量剩余空间是否足够容纳merge之后的数据
        let available_size = utils::file::available_disk_size();
        let need_size = total_size - reclaim_size as u64;
        if need_size >= available_size {
            return Err(Errors::MergeSpaceNotEnough {
                actual: available_size,
                expected: need_size,
            });
        }

        // 获取merge的临时目录
        let merge_path = get_merge_path(self.options.dir_path.clone());

        // 删除原来的
        if merge_path.is_dir() {
            std::fs::remove_dir_all(&merge_path).unwrap();
        }

        std::fs::create_dir_all(&merge_path)?;
        // 获取需要merge的文件
        let merge_files = self.rotate_merge_files()?;

        // 在merge_path上新建一个数据库实例
        let mut merge_db_opts = EngineOptions::default();
        merge_db_opts.dir_path = merge_path.clone();
        merge_db_opts.data_file_size = self.options.data_file_size;
        let merge_db = Engine::open(merge_db_opts)?;

        // 打开hint文件,存储索引
        let hint_file = DataFile::new_hint_file(merge_path.clone())?;

        // 处理每个数据文件,重写有效数据
        for data_file in merge_files.iter() {
            let mut offset = 0;
            loop {
                let (mut log_record, size) = match data_file.read_log_record(offset) {
                    Ok(result) => (result.record, result.size),
                    Err(e) => match e {
                        Errors::ReadDataFileEOF => break,
                        _ => return Err(e),
                    },
                };

                // 解码,拿到实际的key
                let (real_key, _) = parse_log_record_key(log_record.key.clone())?;
                if let Some(index_pos) = self.index.get(real_key.clone()) {
                    // 有效数据,重写
                    if index_pos.file_id == data_file.get_file_id() && index_pos.offset == offset {
                        // 去除事务标识
                        log_record.key =
                            log_record_key_with_seq(real_key.clone(), NON_TRANSACTION_SEQ_NO)?;
                        let log_record_pos = merge_db.append_log_record(&mut log_record)?;
                        // 写hint索引
                        hint_file.write_hint_record(real_key.clone(), log_record_pos)?;
                    }
                }
                offset += size as u64;
            }
        }

        // 持久化
        merge_db.sync()?;
        hint_file.sync()?;

        // 标识merge全部完成
        // 拿到最近未参与merge的文件id
        // todo: 这里用了unwrap,有风险
        // 比 non_merge_file_id 小的id都已经完成了merge
        let non_merge_file_id = merge_files.last().unwrap().get_file_id() + 1;
        let merge_fin_file = DataFile::new_merge_fin_file(merge_path.clone())?;
        let merge_fin_record = LogRecord {
            key: MERGE_FIN_KEY.to_vec(),
            value: non_merge_file_id.to_string().into_bytes(),
            rec_type: LogRecordType::Normal,
        };

        let encode_record = merge_fin_record.encode()?;
        merge_fin_file.write(&encode_record)?;
        merge_fin_file.sync()?;

        Ok(())
    }

    /// 拿到需要merge的文件
    fn rotate_merge_files(&self) -> Result<Vec<DataFile>> {
        let mut merge_file_ids = vec![];
        let mut older_files = self.older_files.write();

        for fid in older_files.keys() {
            merge_file_ids.push(*fid);
        }

        // 设置一个新的活跃文件用于写入
        let mut active_file = self.active_file.write();
        active_file.sync()?;
        let active_file_id = active_file.get_file_id();
        let new_active_file = DataFile::new(
            self.options.dir_path.clone(),
            active_file_id + 1,
            IOType::StandardFileIO,
        )?;
        *active_file = new_active_file;

        // 加到旧的数据文件中
        let old_file = DataFile::new(
            self.options.dir_path.clone(),
            active_file_id,
            IOType::StandardFileIO,
        )?;
        older_files.insert(active_file_id, old_file);
        merge_file_ids.push(active_file_id);

        // 从小到大排序，依次merge
        merge_file_ids.sort();

        // 打开所有需要merge的文件
        let mut merge_files = vec![];
        for file_id in merge_file_ids.iter() {
            let data_file = DataFile::new(
                self.options.dir_path.clone(),
                *file_id,
                IOType::StandardFileIO,
            )?;
            merge_files.push(data_file);
        }

        Ok(merge_files)
    }

    pub(crate) fn load_index_from_hint_file(&self) -> Result<()> {
        let hint_file_name = self.options.dir_path.join(HINT_FILE_NAME);
        if !hint_file_name.is_file() {
            return Ok(());
        }

        let hint_file = DataFile::new_hint_file(self.options.dir_path.clone())?;

        let mut offset = 0;
        loop {
            let (log_record, size) = match hint_file.read_log_record(offset) {
                Ok(result) => (result.record, result.size),
                Err(e) => match e {
                    Errors::ReadDataFileEOF => break,
                    _ => return Err(e),
                },
            };
            // 解码value,拿到位置索引
            let log_record_pos = LogRecordPos::decode(log_record.value)?;
            self.index.put(log_record.key, log_record_pos);

            offset += size as u64
        }

        Ok(())
    }
}
