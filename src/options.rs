use std::{ops::Index, path::PathBuf};

/// 数据库配置
#[derive(Debug, Clone)]
pub struct EngineOptions {
    /// 数据库目录
    pub dir_path: PathBuf,
    /// 单个数据文件的大小,单位字节
    pub data_file_size: u64,
    /// 是否每次写入都持久化
    pub sync_writes: bool,
    /// 索引类型
    pub index_type: IndexType,
}

// 索引类型
#[derive(Debug, Clone)]
pub enum IndexType {
    BTree,
    SkipList,
}
