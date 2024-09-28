use std::{collections::HashMap, sync::Arc};

use crate::{
    data::{
        data_file::DataFile,
        log_record::{LogRecord, LogRecordPos, LogRecordType},
    },
    index,
    options::EngineOptions,
    prelude::*,
};
use bytes::Bytes;
use parking_lot::RwLock;

pub struct Engine {
    options: Arc<EngineOptions>,
    active_file: Arc<RwLock<DataFile>>, // 当前活跃文件
    older_files: Arc<RwLock<HashMap<u32, DataFile>>>, // 旧的数据文件
    index: Box<dyn index::Indexer>,     // 数据内存索引(并发安全)
}

impl Engine {
    /// 存储`key`/`value`, `key`不能为空
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }
        let mut log_record = LogRecord {
            key: key.to_vec(),
            value: value.to_vec(),
            rec_type: LogRecordType::NORMAL,
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
    fn append_log_record(&self, log_record: &mut LogRecord) -> Result<LogRecordPos> {
        let dir_path = &self.options.dir_path;

        // 对写入的record进行编码
        let encoded_record = log_record.encode();
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
        // 数据在磁盘中的位置,在哪个文件,偏移量
        let log_record_pos = pos.unwrap();

        let active_file = self.active_file.read();
        let older_files = self.older_files.read();

        // 取到磁盘中的数据
        let log_record = match active_file.get_file_id() == log_record_pos.file_id {
            true => active_file.read_log_record(log_record_pos.offset)?,
            false => {
                let data_file = older_files.get(&log_record_pos.file_id);
                if data_file.is_none() {
                    return Err(Errors::DataFileNotFound);
                }

                data_file.unwrap().read_log_record(log_record_pos.offset)?
            }
        };

        // 判断这个数据是否有效
        match log_record.rec_type {
            LogRecordType::NORMAL => return Ok(log_record.value.into()),
            LogRecordType::DELETED => return Err(Errors::KeyNotFound),
        }
    }
}
