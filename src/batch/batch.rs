use crate::{
    data::log_record::{LogRecord, LogRecordType},
    db::Engine,
    options::WriteBatchOptions,
    prelude::*,
};
use std::{
    collections::HashMap,
    sync::{atomic::Ordering, Arc},
};

use bytes::Bytes;
use parking_lot::Mutex;

use super::log_record_key_with_seq;

/// 批量写
pub struct WriteBatch<'a> {
    pending_wirtes: Arc<Mutex<HashMap<Vec<u8>, LogRecord>>>, // 暂存用户写入的数据
    engine: &'a Engine,
    options: WriteBatchOptions,
}

impl Engine {
    pub fn new_write_batch(&self, options: WriteBatchOptions) -> Result<WriteBatch> {
        Ok(WriteBatch {
            pending_wirtes: Arc::new(Mutex::new(HashMap::new())),
            engine: self,
            options,
        })
    }
}

impl WriteBatch<'_> {
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        // 暂存数据
        let log_record = LogRecord {
            key: key.to_vec(),
            value: value.to_vec(),
            rec_type: LogRecordType::Normal,
        };

        let mut pending_write = self.pending_wirtes.lock();

        pending_write.insert(key.to_vec(), log_record);
        Ok(())
    }

    pub fn delete(&self, key: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }
        let mut pending_write = self.pending_wirtes.lock();
        let index_pos = self.engine.index.get(key.to_vec());
        if index_pos.is_none() {
            // 检查pending_wirte
            if pending_write.contains_key(&key.to_vec()) {
                pending_write.remove(&key.to_vec());
            }

            return Ok(());
        }

        // 暂存数据
        let log_record = LogRecord {
            key: key.to_vec(),
            value: Default::default(),
            rec_type: LogRecordType::Deleted,
        };

        pending_write.insert(key.to_vec(), log_record);
        Ok(())
    }

    /// 提交数据,更新内存索引
    pub fn commit(&self) -> Result<()> {
        let mut pending_write = self.pending_wirtes.lock();
        if pending_write.len() == 0 {
            return Ok(());
        }

        if pending_write.len() as u32 > self.options.max_batch_num {
            return Err(Errors::ExceedMaxBatchNum {
                max: self.options.max_batch_num,
                current: pending_write.len() as u32,
            });
        }

        // 加锁保证串行化
        let _lock = self.engine.batch_commit_lock.lock();

        // 获取全局事务序列号
        // 让当前seq_no+1, 然后返回上一个seq_no的值
        let seq_no = self.engine.seq_no.fetch_add(1, Ordering::SeqCst);

        // 写到数据文件中
        let mut positions = HashMap::new();
        for (_, item) in pending_write.iter() {
            let mut record = LogRecord {
                key: log_record_key_with_seq(item.key.clone(), seq_no)?,
                value: item.value.clone(),
                rec_type: item.rec_type,
            };

            let pos = self.engine.append_log_record(&mut record)?;
            positions.insert(item.key.clone(), pos);
        }

        // 标识事务完成
        let mut finish_log_record = LogRecord {
            key: log_record_key_with_seq(TXN_FINISHED_KEY.to_vec(), seq_no)?,
            value: Default::default(),
            rec_type: LogRecordType::TxnFinished,
        };

        self.engine.append_log_record(&mut finish_log_record)?;

        // 如果配置了持久化,就sync
        if self.options.sync_writes {
            self.engine.sync()?;
        }

        // 更新内存索引
        for (_, item) in pending_write.iter() {
            let record_pos = positions.get(&item.key);
            if record_pos.is_none() {
                continue;
            }
            let record_pos = record_pos.unwrap();

            match item.rec_type {
                LogRecordType::Deleted => {
                    if let Some(old_pos) = self.engine.index.delete(item.key.clone()) {
                        self.engine
                            .reclaim_size
                            .fetch_add(old_pos.size, Ordering::SeqCst);
                    }
                }
                _ => {
                    if let Some(old_pos) = self.engine.index.put(item.key.clone(), *record_pos) {
                        self.engine
                            .reclaim_size
                            .fetch_add(old_pos.size, Ordering::SeqCst);
                    }
                }
            }
        }

        // 清空暂存数据
        pending_write.clear();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::options::EngineOptions;

    use super::*;
    fn basepath() -> PathBuf {
        "./tmp/write_batch".into()
    }

    fn setup(dir_name: &str) {
        // 创建测试文件夹
        let basepath = PathBuf::from(basepath()).join(dir_name);
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
    fn test_write_batch_put() {
        setup("put");
        let mut opts = EngineOptions::default();
        opts.dir_path = basepath().join("put");

        let db = Engine::open(opts).expect("failed to open database");

        {
            // 写入不提交, 读取没提交的数据
            let wb = db
                .new_write_batch(WriteBatchOptions::default())
                .expect("new write batch failed");

            let key = Bytes::from("Hello World");
            let value = Bytes::from("Rust World");
            assert_eq!(true, wb.put(key.clone(), value.clone()).is_ok());
            assert_eq!(true, db.get(key.clone()).is_err());

            let key = Bytes::from("key-1");
            let value = Bytes::from("value-1");
            assert_eq!(true, wb.put(key.clone(), value.clone()).is_ok());
            assert_eq!(true, db.get(key.clone()).is_err());

            let key = Bytes::from("key-2");
            let value = Bytes::from("value-2");
            assert_eq!(true, wb.put(key.clone(), value.clone()).is_ok());
            assert_eq!(true, db.get(key.clone()).is_err());
        }

        {
            // 写入并提交, 再读取
            let wb = db
                .new_write_batch(WriteBatchOptions::default())
                .expect("new write batch failed");

            let key = Bytes::from("Hello World");
            let value = Bytes::from("Rust World");
            assert_eq!(true, wb.put(key.clone(), value.clone()).is_ok());
            assert_eq!(true, wb.commit().is_ok()); // commit 1
            let res = db.get(key.clone());
            assert_eq!(true, res.is_ok());
            let res = res.unwrap();
            assert_eq!(res, value.clone());

            let key = Bytes::from("key-1");
            let value = Bytes::from("value-1");
            assert_eq!(true, wb.put(key.clone(), value.clone()).is_ok());
            assert_eq!(true, wb.commit().is_ok()); // commit 2
            let res = db.get(key.clone());
            assert_eq!(true, res.is_ok());
            let res = res.unwrap();
            assert_eq!(res, value.clone());

            let key = Bytes::from("key-2");
            let value = Bytes::from("value-2");
            assert_eq!(true, wb.put(key.clone(), value.clone()).is_ok());
            assert_eq!(true, wb.commit().is_ok()); // commit 3
            let res = db.get(key.clone());
            assert_eq!(true, res.is_ok());
            let res = res.unwrap();
            assert_eq!(res, value.clone());

            // 验证事务序列号
            let seq_no = wb.engine.seq_no.load(Ordering::SeqCst);
            assert_eq!(4, seq_no); // 这里是下一个要用的事务序列号,所以是4,
        }

        clean("put");
    }

    #[test]

    fn test_write_batch_delete() {
        setup("delete");
        let mut opts = EngineOptions::default();
        opts.dir_path = basepath().join("delete");

        let db = Engine::open(opts).expect("failed to open database");

        // 初始化db数据
        {
            let wb = db
                .new_write_batch(WriteBatchOptions::default())
                .expect("new write batch failed");

            let key = Bytes::from("key-1");
            let value = Bytes::from("value-1");
            assert_eq!(true, wb.put(key.clone(), value.clone()).is_ok());

            let key = Bytes::from("key-2");
            let value = Bytes::from("value-2");
            assert_eq!(true, wb.put(key.clone(), value.clone()).is_ok());

            assert_eq!(true, wb.commit().is_ok());
        }

        // 利用 batch 删除数据
        {
            let wb = db
                .new_write_batch(WriteBatchOptions::default())
                .expect("new write batch failed");

            let key = Bytes::from("key-1");
            assert_eq!(true, wb.delete(key.clone()).is_ok());
            let key = Bytes::from("key-2");
            assert_eq!(true, wb.delete(key.clone()).is_ok());

            wb.commit().unwrap();

            let key = Bytes::from("key-1");
            assert_eq!(true, db.get(key.clone()).is_err());
            let key = Bytes::from("key-2");
            assert_eq!(true, db.get(key.clone()).is_err());
        }

        clean("delete");
    }

    #[test]
    fn test_write_batch_after_reopen() {
        // 重启之后读取事务序列号
        setup("reopen");
        let mut opts = EngineOptions::default();
        opts.dir_path = basepath().join("reopen");

        let db = Engine::open(opts.clone()).expect("failed to open database");
        {
            // 写入并提交, 再读取
            let wb = db
                .new_write_batch(WriteBatchOptions::default())
                .expect("new write batch failed");

            let key = Bytes::from("Hello World");
            let value = Bytes::from("Rust World");
            assert_eq!(true, wb.put(key.clone(), value.clone()).is_ok());
            assert_eq!(true, wb.commit().is_ok()); // commit 1

            let key = Bytes::from("key-1");
            let value = Bytes::from("value-1");
            assert_eq!(true, wb.put(key.clone(), value.clone()).is_ok());
            assert_eq!(true, wb.commit().is_ok()); // commit 2

            let key = Bytes::from("key-2");
            let value = Bytes::from("value-2");
            assert_eq!(true, wb.put(key.clone(), value.clone()).is_ok());
            assert_eq!(true, wb.commit().is_ok()); // commit 3

            println!("seq_no: {}", wb.engine.seq_no.load(Ordering::SeqCst));
        }

        db.close().expect("failed to close database");
        let db = Engine::open(opts.clone()).expect("failed to open database");

        // 验证事务序列号
        let seq_no = db.seq_no.load(Ordering::SeqCst);
        assert_eq!(3, seq_no);

        clean("reopen");
    }
}
