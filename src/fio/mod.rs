use crate::prelude::*;

pub mod file_io;

/// 抽象IO接口,接入不同IO类型,比如 标准文件io、mmap等
pub trait IOManager: Sync + Send {
    /// 从文件的指定位置读取数据
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
    /// 写入buf到字节数组中
    fn write(&self, buf: &[u8])-> Result<usize>;
    /// 持久化数据
    fn sync(&self) -> Result<()>;
}
