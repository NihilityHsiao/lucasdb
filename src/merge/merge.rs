use std::sync::atomic::Ordering;

use crate::{
    batch::{log_record_key_with_seq, parse_log_record_key},
    data::{
        data_file::DataFile,
        log_record::{LogRecord, LogRecordPos, LogRecordType},
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

        if reclaim_size as u64 >= available_size {
            return Err(Errors::MergeSpaceNotEnough {
                actual: available_size,
                expected: reclaim_size as u64,
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

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        sync::{atomic::AtomicUsize, Arc},
        thread,
    };

    use bytes::Bytes;

    use super::*;
    fn basepath() -> PathBuf {
        "./tmp/merge".into()
    }

    fn setup(name: &str) -> (Engine, EngineOptions) {
        clean(name);

        let path = basepath().join(name);

        let mut opts = EngineOptions::default();
        opts.dir_path = path;
        opts.data_file_size = 32 * 1024 * 1024;
        opts.data_file_merge_ratio = 0f32;

        let path = basepath().join(name);
        if !path.exists() {
            match std::fs::create_dir_all(path.clone()) {
                Ok(_) => {}
                Err(e) => {
                    panic!("{}", format!("setup error: {:?}", e));
                }
            }
        }

        let db = match Engine::open(opts.clone()) {
            Ok(engine) => engine,
            Err(e) => {
                panic!("{}", format!("open db error: {:?}", e));
            }
        };

        (db, opts)
    }

    fn clean(name: &str) {
        let dir_path = basepath().join(name);
        let _ = std::fs::remove_dir_all(dir_path.clone());
        let merge_path = get_merge_path(dir_path.clone());
        let _ = std::fs::remove_dir_all(merge_path);
    }

    #[test]
    fn test_merge_with_empty_data() {
        let name = "empty_data";
        let (db, _) = setup(&name);

        let res = db.merge();
        assert!(res.is_ok());

        clean(&name);
    }

    fn get_test_kv(i: usize) -> (Bytes, Bytes) {
        let key = Bytes::copy_from_slice(format!("test_lucas_db_key_{:09}", i).as_bytes());
        let value = Bytes::copy_from_slice(format!("test_lucas_db_value_{:09}", i).as_bytes());

        (key, value)
    }

    #[test]
    fn test_merge_with_valid_data() {
        let name = "valid_data";
        let (db, opts) = setup(name);

        // 写入数据
        let begin = 0;
        let end = 50000;
        {
            for i in begin..end {
                let (key, value) = get_test_kv(i);
                let put_res = db.put(key, value);
                assert!(put_res.is_ok());
            }
        }

        // 第一次 merge
        {
            let merge_res = db.merge();
            assert!(merge_res.is_ok());
        }

        // 关闭db
        {
            std::mem::drop(db);
        }
        // 重新打开db
        let db = Engine::open(opts.clone()).unwrap();
        // 重新校验
        {
            let keys = db.list_keys().unwrap();
            assert_eq!(keys.len(), end - begin);
        }

        // 校验merge之后的key
        {
            for i in begin..end {
                let (key, value) = get_test_kv(i);
                let get_res = db.get(key);
                assert!(get_res.is_ok());
                let get_value = get_res.unwrap();

                assert_eq!(get_value, value);
            }
        }

        clean(name);
    }

    #[test]
    fn test_merge_with_deleted_data() {
        let name = "deleted_data";
        let (mut db, opts) = setup(name);

        // 写入数据
        let begin = 0;
        let mid = 10000;
        let end = 50000;
        let new_value = Bytes::from("new_value");
        {
            for i in begin..end {
                let (key, value) = get_test_kv(i);
                let put_res = db.put(key, value);
                assert!(put_res.is_ok());
            }
        }

        // 写入部分数据
        {
            for i in begin..mid {
                let (key, _) = get_test_kv(i);
                let put_res = db.put(key, new_value.clone());
                assert!(put_res.is_ok());
            }
        }

        // 删除数据
        {
            for i in mid..end {
                let (key, _) = get_test_kv(i);
                let delete_res = db.delete(key);
                assert!(delete_res.is_ok());
            }
        }

        // merge
        {
            let merge_res = db.merge();
            assert!(merge_res.is_ok());
        }

        // 重启数据库
        {
            std::mem::drop(db);
            db = Engine::open(opts.clone()).expect("failed to reopen database");
        }

        // 校验
        {
            let keys = db.list_keys().expect("listkey error");
            assert_eq!(keys.len(), mid - begin);

            for i in begin..mid {
                let (k, _) = get_test_kv(i);
                let get_res = db.get(k);
                assert!(get_res.is_ok());
                assert_eq!(new_value.clone(), get_res.unwrap());
            }
        }

        clean(name);
    }

    // 全都是无效数据时进行merge
    #[test]
    fn test_merge_with_invalid_data() {
        let name = "invalid_data";
        let (mut db, opts) = setup(name);

        // 写入后删除数据
        let begin = 0;
        let end = 50000;
        {
            for i in begin..end {
                let (key, value) = get_test_kv(i);
                let put_res = db.put(key.clone(), value.clone());
                assert!(put_res.is_ok());

                // 删除
                let delete_res = db.delete(key);
                assert!(delete_res.is_ok());
            }
        }

        // merge
        {
            let merge_res = db.merge();
            assert!(merge_res.is_ok());
        }

        // 重新打开db
        {
            std::mem::drop(db);
            db = Engine::open(opts.clone()).expect("fail to open database");
        }

        // 校验
        {
            let keys = db.list_keys().expect("failed to list keys");
            assert_eq!(0, keys.len());

            for i in begin..end {
                let (key, _) = get_test_kv(i);
                let get_res = db.get(key.clone());
                match get_res {
                    Ok(v) => panic!("{}", format!("should not get this value: {:?}", v)),
                    Err(e) => match e {
                        Errors::KeyNotFound => {}
                        _ => {
                            panic!("unexpected error: {:?}", e)
                        }
                    },
                }
            }
        }

        clean(name);
    }

    // merge的过程中写入/删除数据
    #[test]
    fn test_merge_when_modifying_new_data() {
        let name = "mergeing";
        let (mut db, opts) = setup(name);
        let begin = 0;
        let mid = 10000;
        let end = 50000;

        let new_value = Bytes::from("new-value-in-merge");

        let mut key_count = Arc::new(AtomicUsize::new(0));

        // 准备测试数据
        {
            // 新增数据
            {
                for i in begin..end {
                    let (key, value) = get_test_kv(i);
                    let put_res = db.put(key.clone(), value.clone());
                    assert!(put_res.is_ok());
                    key_count.fetch_add(1, Ordering::SeqCst);
                }
            }

            // 修改数据
            {
                for i in begin..mid {
                    let (key, _) = get_test_kv(i);
                    let put_res = db.put(key.clone(), new_value.clone());
                    assert!(put_res.is_ok());
                }
            }

            // 删除数据
            {
                for i in mid..end {
                    let (key, _) = get_test_kv(i);
                    let delete_res = db.delete(key.clone());
                    assert!(delete_res.is_ok());
                    key_count.fetch_sub(1, Ordering::SeqCst);
                }
            }
        }

        // 并发测试
        {
            let db_arc = Arc::new(db);
            let mut handles = vec![];

            // 线程1: 新增数据
            {
                let db_arc_1 = db_arc.clone();
                let key_count_clone = key_count.clone();

                let handle_1 = thread::spawn(move || {
                    let mut cnt = 0;
                    for i in 60000..100000 {
                        let (key, value) = get_test_kv(i);
                        let put_res = db_arc_1.put(key.clone(), value.clone());
                        assert!(put_res.is_ok());
                        cnt += 1;
                    }
                    key_count_clone.fetch_add(cnt, Ordering::SeqCst);
                });
                handles.push(handle_1);
            }

            // 线程2: merge
            {
                let db_arc_2 = db_arc.clone();
                let handle_2 = thread::spawn(move || {
                    let merge_res = db_arc_2.merge();
                    assert!(merge_res.is_ok());
                });
                handles.push(handle_2);
            }

            // 等待所有线程完成任务
            {
                for handle in handles {
                    handle.join().unwrap();
                }
            }

            // 关闭数据库
            std::mem::drop(db_arc);
        }

        // 校验数据
        {
            db = Engine::open(opts.clone()).expect("failed to open database");
            let keys = db.list_keys().expect("failed to list keys");
            let cnt = key_count.load(Ordering::SeqCst);
            assert_eq!(keys.len(), cnt);
        }

        clean(name);
    }
}
