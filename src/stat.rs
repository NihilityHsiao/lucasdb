/// 记录数据库的统计信息
#[derive(Debug)]
pub(crate) struct Stat {
    /// `key`的总数量
    pub(crate) key_num: usize,
    /// 数据文件的数量
    pub(crate) data_file_num: usize,
    /// 可以回收的数据量
    pub(crate) reclaim_size: usize,
    /// 数据目录占据的磁盘空间大小
    pub(crate) disk_size: usize,
}
