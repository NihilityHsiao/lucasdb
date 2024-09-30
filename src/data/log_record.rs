/// 数据类型
#[derive(Debug, PartialEq)]
pub enum LogRecordType {
    NORMAL = 1,
    /// 表示这个数据被删除了,merge的时候要清理掉
    DELETED = 2,
}

/// 数据在磁盘中的索引
#[derive(Debug, Clone, Copy)]
pub struct LogRecordPos {
    pub(crate) file_id: u32,
    pub(crate) offset: u64,
}

/// 存储真正的数据
pub struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) rec_type: LogRecordType,
}

impl LogRecord {
    pub fn encode(&mut self) -> Vec<u8> {
        todo!()
    }
}

/// 从数据文件中读取的`LogRecord`的额外信息
pub struct ReadLogRecord {
    pub(crate) record: LogRecord,
    pub(crate) size: u64,
}
