use std::path::PathBuf;

use file_io::FileIO;
use mmap::MMapIO;

use crate::prelude::*;

pub mod file_io;
pub mod mmap;
/// 抽象IO接口,接入不同IO类型,比如 标准文件io、mmap等
pub trait IOManager: Sync + Send {
    /// 从文件的指定位置读取数据
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
    /// 写入buf到字节数组中
    fn write(&self, buf: &[u8]) -> Result<usize>;
    /// 持久化数据
    fn sync(&self) -> Result<()>;

    /// 获取文件大小
    fn size(&self) -> Result<u64>;
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum IOType {
    StandardFileIO, // 标准文件IO
    MemoryMap,      // 内存映射,用于加快启动速度
}

pub fn new_io_manager(file_name: PathBuf, io_type: IOType) -> Result<Box<dyn IOManager>> {
    match io_type {
        IOType::StandardFileIO => Ok(Box::new(FileIO::new(file_name)?)),
        IOType::MemoryMap => Ok(Box::new(MMapIO::new(file_name)?)),
    }
}
