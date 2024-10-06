use std::path::PathBuf;

use bon::{builder, Builder};

/// 数据库配置
#[derive(Debug, Clone, Builder)]
pub struct EngineOptions {
    /// 数据库目录
    pub dir_path: PathBuf,
    /// 单个数据文件的大小,单位字节
    #[builder(default = 256 * 1024 * 1024)]
    pub data_file_size: u64,
    /// 是否每次写入都持久化
    #[builder(default = false)]
    pub sync_writes: bool,
    /// 索引类型
    pub index_type: IndexType,

    /// 累计写到多少字节后进行持久化
    #[builder(default = 0)]
    pub bytes_per_sync: usize,

    /// 是否使用Mmap加快启动数据库
    #[builder(default = true)]
    pub use_mmap_when_startup: bool,

    /// 达到阈值了就执行merge操作
    #[builder(default = 0.5)]
    pub data_file_merge_ratio: f32,
}

#[derive(Debug, Clone, Builder)]
pub struct IteratorOptions {
    pub prefix: Vec<u8>, // 前缀,过滤用
    pub reverse: bool,   // 是否反向便利
}

#[derive(Debug, Clone, Builder)]
pub struct WriteBatchOptions {
    pub max_batch_num: u32, // 一个Batch最多写多少条数据
    pub sync_writes: bool,  // 提交的时候是否持久化
}

impl Default for EngineOptions {
    fn default() -> Self {
        Self {
            dir_path: std::env::temp_dir().join("lucasdb"),
            data_file_size: 256 * 1024 * 1024,
            sync_writes: false,
            index_type: IndexType::BTree,
            bytes_per_sync: 0,
            use_mmap_when_startup: true,
            data_file_merge_ratio: 0.5,
        }
    }
}

impl Default for IteratorOptions {
    fn default() -> Self {
        Self {
            prefix: Default::default(),
            reverse: false,
        }
    }
}
impl Default for WriteBatchOptions {
    fn default() -> Self {
        Self {
            max_batch_num: 10000,
            sync_writes: true,
        }
    }
}

// 索引类型
#[derive(Debug, Clone)]
pub enum IndexType {
    BTree,
    SkipList,
}
