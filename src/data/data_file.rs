use crate::prelude::*;
use std::{path::PathBuf, sync::Arc};

use parking_lot::RwLock;

use crate::fio;

use super::log_record::{LogRecord, ReadLogRecord};

/// 数据文件,实际存储多个key-value的文件
/// 一个 DataFile 就对应一个文件
pub struct DataFile {
    file_id: Arc<RwLock<u32>>,
    write_off: Arc<RwLock<u64>>, // 当前写偏移,记录文件写入的位置
    io_manager: Box<dyn fio::IOManager>,
}

impl DataFile {
    pub fn new(dir_path: PathBuf, file_id: u32) -> Result<DataFile> {
        todo!()
    }

    pub fn get_write_off(&self) -> u64 {
        let read_guard = self.write_off.read();
        *read_guard
    }
    pub fn set_write_off(&self, offset: u64) {
        let mut write_guard = self.write_off.write();
        *write_guard = offset;
    }
    pub fn sync(&self) -> Result<()> {
        todo!()
    }

    pub fn get_file_id(&self) -> u32 {
        let read_guard = self.file_id.read();
        *read_guard
    }

    pub fn write(&self, buf: &[u8]) -> Result<usize> {
        todo!()
    }

    /// 给定 `offset` 读取相应的 LogRecord
    pub fn read_log_record(&self, offset: u64) -> Result<ReadLogRecord> {
        todo!()
    }
}
