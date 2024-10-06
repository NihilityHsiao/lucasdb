use crate::prelude::*;
use std::{fs::OpenOptions, path::PathBuf, sync::Arc};

use memmap2::Mmap;
use parking_lot::Mutex;

use super::IOManager;

pub struct MMapIO {
    map: Arc<Mutex<Mmap>>,
}

impl MMapIO {
    pub fn new(file_name: PathBuf) -> Result<Self> {
        match OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(file_name)
        {
            Ok(file) => {
                let map = unsafe { Arc::new(Mutex::new(Mmap::map(&file)?)) };
                return Ok(Self { map });
            }
            Err(e) => return Err(Errors::DataFileLoadError(e)),
        }
    }
}

impl IOManager for MMapIO {
    /// 从 offset 位置开始,读取 [offset, offset + buf.len())  -- 左闭右开
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let map_arr = self.map.lock();
        let end = offset + buf.len() as u64;
        if end > map_arr.len() as u64 {
            return Err(Errors::ReadDataFileEOF);
        }

        let val = &map_arr[offset as usize..end as usize];
        buf.copy_from_slice(val);
        Ok(val.len())
    }

    fn write(&self, buf: &[u8]) -> Result<usize> {
        unimplemented!("mmap unsupport write()");
    }

    fn sync(&self) -> Result<()> {
        unimplemented!("mmap unsupport sync()");
    }

    fn size(&self) -> Result<u64> {
        let map_arr = self.map.lock();
        Ok(map_arr.len() as u64)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::fio::file_io::FileIO;

    use super::*;

    fn basepath() -> &'static str {
        "./tmp/mmap"
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

    // #[test]
    // fn test_file_io_write() {
    //     setup();

    //     let path = get_path("write.data");

    //     let fio_res = MMapIO::new(path.clone());
    //     assert!(fio_res.is_ok());

    //     let fio = fio_res.unwrap();

    //     let res1 = fio.write("key-1".as_bytes());
    //     assert!(res1.is_ok());
    //     assert_eq!(5, res1.unwrap());

    //     let res2 = fio.write("hello-lucas".as_bytes());
    //     assert!(res2.is_ok());
    //     assert_eq!(11, res2.unwrap());

    //     clean();
    // }

    #[test]
    fn test_file_io_read() {
        setup();

        let path = get_path("read.data");

        // 文件为空
        {
            let mmap_res = MMapIO::new(path.clone());
            assert!(mmap_res.is_ok());
            let mmap_io = mmap_res.unwrap();

            let mut buf = [0u8; 10];
            let read_res = mmap_io.read(&mut buf, 0);
            assert!(read_res.is_err());

            match read_res.err().unwrap() {
                Errors::ReadDataFileEOF => {}
                _ => panic!("unexpected error"),
            }
        }

        // 读数据
        {
            let fio_res = FileIO::new(path.clone());
            assert!(fio_res.is_ok());
            let fio = fio_res.unwrap();
            fio.write(b"aa").unwrap();
            fio.write(b"bb").unwrap();
            fio.write(b"cc").unwrap();

            let mmap_res = MMapIO::new(path.clone());
            assert!(mmap_res.is_ok());
            let mmap_io = mmap_res.unwrap();

            let mut buf = [0u8; 2];
            let mut offset = 0;
            let read_res = mmap_io.read(&mut buf, offset);
            assert!(read_res.is_ok());
            let read = read_res.unwrap();
            assert_eq!(2, read);
            assert_eq!(b"aa", &buf);
            offset += read as u64;

            let read_res = mmap_io.read(&mut buf, offset);
            assert!(read_res.is_ok());
            let read = read_res.unwrap();
            assert_eq!(2, read);
            assert_eq!(b"bb", &buf);
            offset += read as u64;

            let read_res = mmap_io.read(&mut buf, offset);
            assert!(read_res.is_ok());
            let read = read_res.unwrap();
            assert_eq!(2, read);
            assert_eq!(b"cc", &buf);
        }

        clean();
    }

    // #[test]
    // fn test_file_io_sync() {
    //     setup();

    //     let path = get_path("sync.data");

    //     let fio_res = MMapIO::new(path.clone());
    //     assert!(fio_res.is_ok());

    //     let fio = fio_res.unwrap();

    //     let res1 = fio.write("key-1".as_bytes());
    //     assert!(res1.is_ok());
    //     assert_eq!(5, res1.unwrap());

    //     let res2 = fio.write("hello-lucas".as_bytes());
    //     assert!(res2.is_ok());
    //     assert_eq!(11, res2.unwrap());

    //     let sync_res = fio.sync();
    //     assert!(sync_res.is_ok());

    //     clean();
    // }
}
