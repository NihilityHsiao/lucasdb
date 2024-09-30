use std::path::PathBuf;

use bon::Builder;

/// 数据库配置
#[derive(Debug, Clone, Builder)]
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

impl Default for EngineOptions {
    fn default() -> Self {
        Self {
            dir_path: std::env::temp_dir().join("lucasdb"),
            data_file_size: 256 * 1024 * 1024,
            sync_writes: false,
            index_type: IndexType::BTree,
        }
    }
}

// 索引类型
#[derive(Debug, Clone)]
pub enum IndexType {
    BTree,
    SkipList,
}
