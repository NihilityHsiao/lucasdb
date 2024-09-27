/// 数据在磁盘中的索引
#[derive(Debug, Clone, Copy)]
pub struct LogRecordPos {
    pub(crate) file_id: u32,
    pub(crate) offset: u64,
}
