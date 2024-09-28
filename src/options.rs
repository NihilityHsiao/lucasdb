use std::path::PathBuf;

/// 数据库配置
pub struct EngineOptions {
    /// 数据库目录
    pub dir_path: PathBuf,
    /// 单个数据文件的大小,单位字节
    pub data_file_size: u64,

    /// 是否每次写入都持久化
    pub sync_writes:bool,
}
