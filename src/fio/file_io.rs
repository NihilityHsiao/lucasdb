use crate::prelude::*;
use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::PathBuf,
    sync::Arc,
};

use log::error;
#[cfg(windows)]
use parking_lot::RwLock;

use super::IOManager;

/// standard file io
pub struct FileIO {
    fd: Arc<RwLock<File>>,
}

impl FileIO {
    /// `file_name`: 文件路径
    /// 如果 `file_name` 不存在, 会创建一个文件,赋予相应的读写权限
    pub fn new(file_name: PathBuf) -> Result<Self> {
        match OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(file_name)
        {
            Ok(file) => {
                return Ok(Self {
                    fd: Arc::new(RwLock::new(file)),
                })
            }
            Err(e) => {
                error!("open data file error: {}", e);
                return Err(Errors::IO(e));
            }
        }
    }
}

impl IOManager for FileIO {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let read_guard = self.fd.read();
        let mut read_result;

        #[cfg(unix)]
        {
            use std::os::unix::prelude::FileExt;
            read_result = read_guard.read_at(buf, offset);
        }

        #[cfg(windows)]
        {
            use std::os::windows::prelude::FileExt;
            read_result = read_guard.seek_read(buf, offset);
        }

        match read_result {
            Ok(n) => return Ok(n),
            Err(e) => {
                error!("read from data file err: {}", e);
                return Err(Errors::IO(e));
            }
        };
    }

    fn write(&self, buf: &[u8]) -> Result<usize> {
        let mut write_guard = self.fd.write();
        match write_guard.write(buf) {
            Ok(n) => return Ok(n),
            Err(e) => {
                error!("write to data file err: {}", e);
                return Err(Errors::IO(e));
            }
        }
    }

    fn sync(&self) -> Result<()> {
        let read_guard = self.fd.read();
        if let Err(e) = read_guard.sync_all() {
            error!("sync data file err: {}", e);
            return Err(Errors::IO(e));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn basepath() -> &'static str {
        "./tmp/file_io"
    }

    fn get_path(file_name: &str) -> PathBuf {
        PathBuf::from(format!("{}/{}", basepath(), file_name))
    }

    fn setup() {
        // 创建测试文件夹
        let basepath = PathBuf::from(basepath());
        if basepath.exists() {
            return;
        }

        match std::fs::create_dir_all(basepath) {
            Ok(_) => {}
            Err(e) => {
                panic!("error creating directory: {}", e)
            }
        }
    }

    fn clean() {
        let _ = std::fs::remove_dir_all(basepath());
    }

    #[test]
    fn test_file_io_write() {
        setup();

        let path = get_path("write.data");

        let fio_res = FileIO::new(path.clone());
        assert!(fio_res.is_ok());

        let fio = fio_res.unwrap();

        let res1 = fio.write("key-1".as_bytes());
        assert!(res1.is_ok());
        assert_eq!(5, res1.unwrap());

        let res2 = fio.write("hello-lucas".as_bytes());
        assert!(res2.is_ok());
        assert_eq!(11, res2.unwrap());

        clean();
    }

    #[test]
    fn test_file_io_read() {
        setup();

        let path = get_path("read.data");

        let fio_res = FileIO::new(path.clone());
        assert!(fio_res.is_ok());

        let fio = fio_res.unwrap();

        let res1 = fio.write("key-1".as_bytes());
        assert!(res1.is_ok());
        assert_eq!(5, res1.unwrap());

        let res2 = fio.write("hello-lucas".as_bytes());
        assert!(res2.is_ok());
        assert_eq!(11, res2.unwrap());

        // read data from read.data

        let mut buf1 = [0u8; 5];
        let mut offset = 0;
        let read_res1 = fio.read(&mut buf1, offset);
        assert!(read_res1.is_ok());
        let read_res1 = read_res1.unwrap();
        assert_eq!(5, read_res1);

        offset += read_res1 as u64;

        let mut buf2 = [0u8; 11];
        let read_res2 = fio.read(&mut buf2, offset);
        assert!(read_res2.is_ok());
        let read_res2 = read_res2.unwrap();
        assert_eq!(11, read_res2);

        clean();
    }

    #[test]
    fn test_file_io_sync() {
        setup();

        let path = get_path("sync.data");

        let fio_res = FileIO::new(path.clone());
        assert!(fio_res.is_ok());

        let fio = fio_res.unwrap();

        let res1 = fio.write("key-1".as_bytes());
        assert!(res1.is_ok());
        assert_eq!(5, res1.unwrap());

        let res2 = fio.write("hello-lucas".as_bytes());
        assert!(res2.is_ok());
        assert_eq!(11, res2.unwrap());

        let sync_res = fio.sync();
        assert!(sync_res.is_ok());

        clean();
    }
}
