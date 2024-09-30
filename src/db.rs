use std::{
    collections::HashMap,
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use crate::{
    // batch::{log_record_key_with_seq, parse_log_record_key},
    batch::{log_record_key_with_seq, parse_log_record_key, TransactionRecord},
    data::{
        data_file::{self, DataFile},
        log_record::{LogRecord, LogRecordPos, LogRecordType},
    },
    index,
    options::EngineOptions,
    prelude::*,
    utils,
};
use bytes::Bytes;
use log::error;
use parking_lot::{Mutex, RwLock};

const INITIAL_FILE_ID: u32 = 0;
pub struct Engine {
    options: Arc<EngineOptions>,
    active_file: Arc<RwLock<DataFile>>, // 当前活跃文件
    older_files: Arc<RwLock<HashMap<u32, DataFile>>>, // 旧的数据文件
    pub(crate) index: Box<dyn index::Indexer>, // 数据内存索引(并发安全)
    file_ids: Vec<u32>,                 // 数据库启动时,获取到的id信息,只用于加载索引时使用

    pub(crate) batch_commit_lock: Mutex<()>, // 事务提交的锁,保证事务串行化
    pub(crate) seq_no: Arc<AtomicUsize>,     // 事务序列号
}

impl Engine {
    pub fn open(options: EngineOptions) -> Result<Self> {
        // 校验options
        check_options(&options)?;

        // 判断数据目录是否存在,如果不存在,就创建
        if let Err(e) = utils::create_dir_if_not_exist(&options.dir_path) {
            error!("create database directory error: {}", e);
            return Err(Errors::IO(e));
        }

        // 加载数据文件
        let mut data_files = load_data_files(&options.dir_path)?;
        // 列表中的第一个文件是活跃文件
        data_files.reverse();
        let mut file_ids = vec![];
        for v in data_files.iter() {
            file_ids.push(v.get_file_id());
        }

        let mut older_files = HashMap::new();
        if data_files.len() > 1 {
            // 处理旧的数据文件
            for _ in 0..data_files.len() - 2 {
                let file = data_files.pop().unwrap();
                older_files.insert(file.get_file_id(), file);
            }
        }

        let active_file = match data_files.pop() {
            Some(v) => v,
            None => DataFile::new(options.dir_path.clone(), INITIAL_FILE_ID)?,
        };

        let mut engine = Self {
            options: Arc::new(options.clone()),
            active_file: Arc::new(RwLock::new(active_file)),
            older_files: Arc::new(RwLock::new(older_files)),
            index: Box::new(index::new_indexer(options.index_type)),
            file_ids: file_ids,
            batch_commit_lock: Mutex::new(()),
            seq_no: Arc::new(AtomicUsize::new(1)),
        };

        // 加载内存索引
        let current_seq_no = engine.load_index_from_data_files()?;
        // 更新当前事务序列号
        if current_seq_no > 0 {
            engine.seq_no.store(current_seq_no, Ordering::SeqCst);
        }

        Ok(engine)
    }

    /// 存储`key`/`value`, `key`不能为空
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }
        let mut log_record = LogRecord {
            key: log_record_key_with_seq(key.to_vec(), NON_TRANSACTION_SEQ_NO)?,
            value: value.to_vec(),
            rec_type: LogRecordType::Normal,
        };

        let log_record_pos = self.append_log_record(&mut log_record)?;

        // 更新内存索引
        let ok = self.index.put(key.to_vec(), log_record_pos);

        if !ok {
            return Err(Errors::IndexUpdateFailed);
        }

        Ok(())
    }

    /// 追加写入数据
    /// 返回内存索引信息
    pub(crate) fn append_log_record(&self, log_record: &mut LogRecord) -> Result<LogRecordPos> {
        let dir_path = &self.options.dir_path;

        // 对写入的record进行编码
        let encoded_record = log_record.encode()?;
        let encoded_record_len = encoded_record.len() as u64;

        // 获取到当前活跃文件
        let mut active_file = self.active_file.write();
        // 活跃文件达到阈值了, 需要持久化,然后开一个新的活跃文件
        if active_file.get_write_off() + encoded_record_len > self.options.data_file_size {
            active_file.sync()?;
            // 当前活跃文件成为旧的活跃文件
            let current_active_file_id = active_file.get_file_id();
            let old_file = DataFile::new(dir_path.to_owned(), current_active_file_id)?;

            let mut older_files = self.older_files.write();

            older_files.insert(current_active_file_id, old_file);

            // 打开新的数据文件
            let new_file = DataFile::new(dir_path.clone(), current_active_file_id + 1)?;
            *active_file = new_file;
        }

        // 追加写数据到当前活跃文件
        let write_off = active_file.get_write_off();
        active_file.write(&encoded_record)?;

        // 根据配置项来决定是否持久化
        if self.options.sync_writes {
            active_file.sync()?;
        }

        // 构造内存索引
        Ok(LogRecordPos {
            file_id: active_file.get_file_id(),
            offset: write_off,
        })
    }

    pub fn get(&self, key: Bytes) -> Result<Bytes> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        // 从内存索引中查找key的位置
        let pos = self.index.get(key.to_vec());
        if pos.is_none() {
            return Err(Errors::KeyNotFound);
        }

        let pos = pos.unwrap();
        self.get_value_by_position(&pos)
    }

    pub(crate) fn get_value_by_position(&self, log_record_pos: &LogRecordPos) -> Result<Bytes> {
        // 数据在磁盘中的位置,在哪个文件,偏移量
        let log_record_pos = log_record_pos;

        let active_file = self.active_file.read();
        let older_files = self.older_files.read();

        // 取到磁盘中的数据
        let log_record = match active_file.get_file_id() == log_record_pos.file_id {
            true => active_file.read_log_record(log_record_pos.offset)?.record,
            false => {
                let data_file = older_files.get(&log_record_pos.file_id);
                if data_file.is_none() {
                    return Err(Errors::DataFileNotFound);
                }

                data_file
                    .unwrap()
                    .read_log_record(log_record_pos.offset)?
                    .record
            }
        };

        // 判断这个数据是否有效
        match log_record.rec_type {
            LogRecordType::Deleted => return Err(Errors::KeyNotFound),
            _ => return Ok(log_record.value.into()),
        }
    }

    pub fn delete(&self, key: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        // 从内存索引中取数据
        let pos = self.index.get(key.to_vec());
        if pos.is_none() {
            return Ok(());
        }

        // 构造log_record,写入数据文件
        let mut record = LogRecord {
            key: log_record_key_with_seq(key.to_vec(), NON_TRANSACTION_SEQ_NO)?,
            value: Default::default(),
            rec_type: LogRecordType::Deleted,
        };

        // 追加写入
        self.append_log_record(&mut record)?;

        // 从内存索引中删除
        let ok = self.index.delete(key.to_vec());
        if !ok {
            return Err(Errors::IndexUpdateFailed);
        }

        Ok(())
    }

    /// 启动时用到,从数据文件中加载内存索引
    /// 遍历所有数据文件,将key的位置记录起来
    fn load_index_from_data_files(&mut self) -> Result<usize> {
        let mut current_seq_no = NON_TRANSACTION_SEQ_NO;
        if self.file_ids.is_empty() {
            return Ok(current_seq_no);
        }

        let active_file = self.active_file.read();
        let older_files = self.older_files.read();

        // 暂存事务相关的数据
        let mut transaction_records = HashMap::new();

        for (i, file_id) in self.file_ids.iter().enumerate() {
            let mut offset = 0;
            loop {
                let log_record_res = match *file_id == active_file.get_file_id() {
                    true => active_file.read_log_record(offset),
                    false => {
                        // todo: 删掉unwrap
                        let data_file = older_files.get(file_id).unwrap();
                        data_file.read_log_record(offset)
                    }
                };

                let (mut log_record, size) = match log_record_res {
                    Ok(result) => (result.record, result.size),
                    Err(e) => {
                        // EOF: 读到文件末尾
                        match e {
                            Errors::ReadDataFileEOF => break,
                            _ => return Err(e),
                        }
                    }
                };

                // 构建内存索引
                let log_record_pos = LogRecordPos {
                    file_id: *file_id,
                    offset,
                };

                let (real_key, seq_no) = parse_log_record_key(log_record.key.clone())?;
                if seq_no == NON_TRANSACTION_SEQ_NO {
                    self.update_index(real_key, log_record.rec_type, log_record_pos);
                } else {
                    // 事务数据
                    if log_record.rec_type == LogRecordType::TxnFinished {
                        // 更新内存索引,这是个合法的事务数据
                        let records: &Vec<TransactionRecord> = transaction_records
                            .get(&seq_no)
                            .ok_or(Errors::TxnNumberNotFound(seq_no))?;

                        for txn_record in records.iter() {
                            self.update_index(
                                txn_record.record.key.clone(),
                                txn_record.record.rec_type,
                                txn_record.pos,
                            );
                        }

                        transaction_records.remove(&seq_no);
                    } else {
                        // 批量提交的数据,暂存
                        log_record.key = real_key;
                        transaction_records
                            .entry(seq_no)
                            .or_insert(Vec::new())
                            .push(TransactionRecord {
                                record: log_record,
                                pos: log_record_pos,
                            });
                    }
                }
                if seq_no > current_seq_no {
                    current_seq_no = seq_no;
                }
                offset += size as u64;
            }

            // 设置活跃文件的offset
            if i == self.file_ids.len() - 1 {
                active_file.set_write_off(offset);
            }
        }

        Ok(current_seq_no)
    }

    fn update_index(&self, key: Vec<u8>, rec_type: LogRecordType, pos: LogRecordPos) {
        if rec_type == LogRecordType::Normal {
            self.index.put(key, pos);
        } else if rec_type == LogRecordType::Deleted {
            self.index.delete(key);
        }
    }

    /// 关闭数据库
    pub fn close(&self) -> Result<()> {
        let active_file = self.active_file.read();
        active_file.sync()

        // 其他资源
    }

    /// 持久化活跃文件
    pub fn sync(&self) -> Result<()> {
        let active_file = self.active_file.read();
        active_file.sync()
    }
}

/// 从dir_path中加载数据文件
fn load_data_files(dir_path: &PathBuf) -> Result<Vec<DataFile>> {
    let dir = fs::read_dir(dir_path);
    if dir.is_err() {
        return Err(Errors::DataFileLoadError(dir.unwrap_err()));
    }

    let dir = dir.unwrap();

    let mut file_ids = vec![];

    for file in dir {
        if let Err(_) = file {
            continue;
        }

        let entry = file.unwrap();
        let file_os_str = entry.file_name();
        let file_name = file_os_str.to_str().unwrap_or("");
        if file_name.is_empty() {
            continue;
        }

        // 文件名为 00000.data 这种格式的
        if !file_name.ends_with(DATA_FILE_NAME_SUFFIX) {
            continue;
        }

        let split_names: Vec<&str> = file_name.split(".").collect();
        if split_names.len() != 2 {
            continue;
        }

        let file_id = match split_names[0].parse::<u32>() {
            Ok(file_id) => file_id,
            Err(_) => return Err(Errors::DataFileBroken),
        };

        file_ids.push(file_id);
    }
    let mut data_files = vec![];
    // 没有数据文件
    if file_ids.is_empty() {
        return Ok(data_files);
    }

    // 排序,文件id最大的默认是活跃文件
    file_ids.sort();

    for file_id in file_ids.iter() {
        let data_file = DataFile::new(dir_path.clone(), *file_id)?;
        data_files.push(data_file);
    }
    return Ok(data_files);
}

fn check_options(opts: &EngineOptions) -> Result<()> {
    let dir_path = opts.dir_path.to_str();
    if dir_path.is_none() || dir_path.unwrap().is_empty() {
        return Err(Errors::DirPathIsEmpty);
    }

    if opts.data_file_size <= 0 {
        return Err(Errors::DataFileSizeTooSmall);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn basepath() -> PathBuf {
        "./tmp/db".into()
    }

    fn setup(dir_path: &str) {
        // 创建测试文件夹
        let basepath = PathBuf::from(basepath()).join(dir_path);
        if basepath.exists() {
            return;
        }

        match std::fs::create_dir_all(basepath) {
            Ok(_) => {}
            Err(e) => {
                panic!("error creating directory: {}", e)
            }
        }
    }

    fn clean(dir_path: &str) {
        let _ = std::fs::remove_dir_all(basepath().join(dir_path));
    }
    #[test]
    fn teset_db_open() {
        setup("open");
        let mut opts = EngineOptions::default();
        opts.dir_path = basepath().join("open").into();

        let db_res = Engine::open(opts);
        assert!(db_res.is_ok());
        let _ = db_res.unwrap();
        clean("open");
    }

    #[test]
    fn test_db_put() {
        setup("put");
        let mut opts = EngineOptions::default();
        opts.dir_path = basepath().join("put").into();

        let db_res = Engine::open(opts);
        assert!(db_res.is_ok());
        let db = db_res.unwrap();

        let key = Bytes::from("Hello");
        let value = Bytes::from("World");

        let res = db.put(key.clone(), value.clone());
        assert!(res.is_ok());

        let empty_key = Bytes::from("");
        let res = db.put(empty_key, value.clone());
        assert!(res.is_err());
        match res.unwrap_err() {
            Errors::KeyIsEmpty => {}
            _ => panic!("Unexpected error"),
        }
        clean("put");
    }

    #[test]
    fn test_db_get() {
        setup("get");
        let mut opts = EngineOptions::default();
        opts.dir_path = basepath().join("get").into();

        let db_res = Engine::open(opts);
        assert!(db_res.is_ok());
        let db = db_res.unwrap();

        let key = Bytes::from("Hello");
        let value = Bytes::from("World");

        let res = db.put(key.clone(), value.clone());
        assert!(res.is_ok());

        // 正常数据
        let get_res = db.get(key.clone());
        assert!(get_res.is_ok());
        let get_value = get_res.unwrap();
        assert_eq!(get_value, value.clone());

        // 不存在的数据

        let non_exist_key = Bytes::from("non-existent");
        let res = db.get(non_exist_key);
        assert!(res.is_err());
        match res.unwrap_err() {
            Errors::KeyNotFound => {}
            _ => panic!("Unexpected error"),
        }

        // value 为空
        {
            let key = Bytes::from("LucasDb");
            let value = Bytes::from("");
            let res = db.put(key.clone(), value.clone());
            assert!(res.is_ok());

            let res = db.get(key.clone());
            assert!(res.is_ok());
            let get_value = res.unwrap();
            assert_eq!(get_value, value.clone());
        }
        clean("get");
    }

    #[test]
    fn test_db_delete() {
        setup("delete");
        let mut opts = EngineOptions::default();
        opts.dir_path = basepath().join("delete").into();

        let db_res = Engine::open(opts);
        assert!(db_res.is_ok());
        let db = db_res.unwrap();

        let key = Bytes::from("Hello");
        let value = Bytes::from("World");

        let res = db.put(key.clone(), value.clone());
        assert!(res.is_ok());

        // 删除数据
        let res = db.delete(key.clone());
        assert!(res.is_ok());

        // 再get
        let res = db.get(key.clone());
        assert!(res.is_err());
        match res.unwrap_err() {
            Errors::KeyNotFound => {}
            _ => panic!("Unexpected error"),
        }
        clean("delete");
    }

    #[test]
    fn test_db_close() {
        setup("close");
        let mut opts = EngineOptions::default();
        opts.dir_path = basepath().join("close").into();

        let db_res = Engine::open(opts);
        assert!(db_res.is_ok());
        let db = db_res.unwrap();

        let key = Bytes::from("Hello");
        let value = Bytes::from("World");

        let res = db.put(key.clone(), value.clone());
        assert!(res.is_ok());

        assert_eq!(true, db.close().is_ok());

        clean("close");
    }

    #[test]
    fn test_db_sync() {
        setup("sync");
        let mut opts = EngineOptions::default();
        opts.dir_path = basepath().join("sync").into();

        let db_res = Engine::open(opts);
        assert!(db_res.is_ok());
        let db = db_res.unwrap();

        let key = Bytes::from("Hello");
        let value = Bytes::from("World");

        let res = db.put(key.clone(), value.clone());
        assert!(res.is_ok());

        assert_eq!(true, db.sync().is_ok());

        clean("sync");
    }
}
