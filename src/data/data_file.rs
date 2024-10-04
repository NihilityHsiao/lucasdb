use crate::{
    data::log_record::{max_log_record_header_size, LogRecordType},
    fio::new_io_manager,
    prelude::*,
};
use std::{path::PathBuf, sync::Arc};

use bytes::{Buf, BytesMut};
use parking_lot::RwLock;
use prost::{decode_length_delimiter, length_delimiter_len};

use crate::fio;

use super::{
    log_record::{LogRecord, LogRecordPos, ReadLogRecord},
    HINT_FILE_NAME, MERGE_FINISHED_FILE_NAME,
};

/// 数据文件,实际存储多个key-value的文件
/// 一个 DataFile 就对应一个文件
/// DataFile中存储的`LogRecord`是编码之后的
/// Header: Type(1字节) + KeySize(可变长编码) + ValueSize(可变长编码)
/// Body(Key + Value + CRC)
pub struct DataFile {
    file_id: Arc<RwLock<u32>>,
    write_off: Arc<RwLock<u64>>, // 当前写偏移,记录文件写入的位置
    io_manager: Box<dyn fio::IOManager>,
}

impl DataFile {
    pub fn new(dir_path: PathBuf, file_id: u32) -> Result<DataFile> {
        // 根据 dir_path 和 file_id 构建出完整的文件名称
        let file_name = get_data_file_name(&dir_path, file_id);

        let io_manager = new_io_manager(file_name)?;
        Ok(DataFile {
            file_id: Arc::new(RwLock::new(file_id)),
            write_off: Arc::new(RwLock::new(0)),
            io_manager: Box::new(io_manager),
        })
    }

    /// hint索引文件
    pub fn new_hint_file(dir_path: PathBuf) -> Result<DataFile> {
        // 根据 dir_path 和 file_id 构建出完整的文件名称
        let file_name = dir_path.join(HINT_FILE_NAME);

        let io_manager = new_io_manager(file_name)?;
        Ok(DataFile {
            file_id: Arc::new(RwLock::new(0)),
            write_off: Arc::new(RwLock::new(0)),
            io_manager: Box::new(io_manager),
        })
    }

    /// 标识merge完成的文件
    pub fn new_merge_fin_file(dir_path: PathBuf) -> Result<DataFile> {
        // 根据 dir_path 和 file_id 构建出完整的文件名称
        let file_name = dir_path.join(MERGE_FINISHED_FILE_NAME);

        let io_manager = new_io_manager(file_name)?;
        Ok(DataFile {
            file_id: Arc::new(RwLock::new(0)),
            write_off: Arc::new(RwLock::new(0)),
            io_manager: Box::new(io_manager),
        })
    }

    pub fn get_write_off(&self) -> u64 {
        let read_guard = self.write_off.read();
        *read_guard
    }
    pub fn set_write_off(&self, offset: u64) {
        let mut write_guard = self.write_off.write();
        *write_guard = offset;
    }
    pub fn sync(&self) -> Result<()> {
        self.io_manager.sync()
    }

    pub fn get_file_id(&self) -> u32 {
        let read_guard = self.file_id.read();
        *read_guard
    }

    pub fn write(&self, buf: &[u8]) -> Result<usize> {
        let n_bytes = self.io_manager.write(buf)?;
        let mut write_off = self.write_off.write();
        *write_off += n_bytes as u64;

        Ok(n_bytes)
    }

    pub fn write_hint_record(&self, key: Vec<u8>, pos: LogRecordPos) -> Result<()> {
        let hint_record = LogRecord {
            key,
            value: pos.encode()?,
            rec_type: LogRecordType::Normal,
        };
        let encoded_record = hint_record.encode()?;
        self.write(&encoded_record)?;
        Ok(())
    }

    /// 给定 `offset` 读取相应的 LogRecord
    pub fn read_log_record(&self, offset: u64) -> Result<ReadLogRecord> {
        let mut header_buf = BytesMut::zeroed(max_log_record_header_size());
        self.io_manager.read(&mut header_buf, offset)?;

        // 第一个字节是 Type
        let rec_type = header_buf.get_u8();

        // key、value的长度
        let key_size = decode_length_delimiter(&mut header_buf)?;
        let value_size = decode_length_delimiter(&mut header_buf)?;

        // 达到文件末尾了
        if key_size == 0 && value_size == 0 {
            return Err(Errors::ReadDataFileEOF);
        }

        // 获取实际Header大小
        let actual_header_size =
            length_delimiter_len(key_size) + length_delimiter_len(value_size) + 1; // 1是type的长度

        let mut kv_buf = BytesMut::zeroed(key_size + value_size + CRC_SIZE);
        self.io_manager
            .read(&mut kv_buf, offset + actual_header_size as u64)?;

        let log_record = LogRecord {
            key: kv_buf.get(..key_size).unwrap().to_vec(),
            value: kv_buf.get(key_size..kv_buf.len() - 4).unwrap().to_vec(),
            rec_type: LogRecordType::from_u8(rec_type),
        };

        // 校验 crc
        kv_buf.advance(key_size + value_size); // 移动指针,当前指向的crc的值
        let crc = kv_buf.get_u32();
        if crc != log_record.get_crc() {
            return Err(Errors::InvalidLogRecordCrc);
        }

        Ok(ReadLogRecord {
            record: log_record,
            size: actual_header_size + key_size + value_size + CRC_SIZE,
        })
    }
}

pub fn get_data_file_name(path: &PathBuf, file_id: u32) -> PathBuf {
    let v = format!("{:09}{}", file_id, DATA_FILE_NAME_SUFFIX);
    path.join(v)
}
#[cfg(test)]
mod tests {
    use super::*;
    fn basepath() -> PathBuf {
        "./tmp/data_file".into()
    }

    fn setup(dir_path: &str) {
        let _ = std::fs::remove_dir_all(basepath().join(dir_path));

        // 创建测试文件夹
        let basepath = PathBuf::from(basepath().join(dir_path));
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

    fn clean(dir_path: &str) {
        let _ = std::fs::remove_dir_all(basepath().join(dir_path));
    }
    #[test]
    fn test_data_file_new() {
        setup("new");
        let dir_path = PathBuf::from(basepath().join("new"));
        {
            let file_id = 0;
            let data_file_res = DataFile::new(dir_path.clone(), file_id);
            assert!(data_file_res.is_ok());
            let data_file = data_file_res.unwrap();
            assert_eq!(file_id, data_file.get_file_id());
        }

        {
            let file_id = 1;
            let data_file_res = DataFile::new(dir_path.clone(), file_id);
            assert!(data_file_res.is_ok());
            let data_file = data_file_res.unwrap();
            assert_eq!(file_id, data_file.get_file_id());
        }

        {
            let file_id = 6999123;
            let data_file_res = DataFile::new(dir_path.clone(), file_id);
            assert!(data_file_res.is_ok());
            let data_file = data_file_res.unwrap();
            assert_eq!(file_id, data_file.get_file_id());
        }

        clean("new");
    }

    #[test]
    fn test_data_file_write() {
        setup("write");
        let dir_path = PathBuf::from(basepath().join("write"));
        let file_id = 1;
        {
            let data_file_res = DataFile::new(dir_path.clone(), file_id);
            assert!(data_file_res.is_ok());
            let data_file = data_file_res.unwrap();
            assert_eq!(file_id, data_file.get_file_id());

            // 写入
            let buf = "abc".as_bytes();
            let write_res = data_file.write(buf);
            assert!(write_res.is_ok());
            assert_eq!(buf.len(), write_res.unwrap());
        }

        {
            let data_file_res = DataFile::new(dir_path.clone(), file_id);
            assert!(data_file_res.is_ok());
            let data_file = data_file_res.unwrap();
            assert_eq!(file_id, data_file.get_file_id());

            // 写入
            let buf = "LucasDb".as_bytes();
            let write_res = data_file.write(buf);
            assert!(write_res.is_ok());
            assert_eq!(buf.len(), write_res.unwrap());
        }

        {
            let data_file_res = DataFile::new(dir_path.clone(), file_id);
            assert!(data_file_res.is_ok());
            let data_file = data_file_res.unwrap();
            assert_eq!(file_id, data_file.get_file_id());

            // 写入
            let buf = "I Love Rust".as_bytes();
            let write_res = data_file.write(buf);
            assert!(write_res.is_ok());
            assert_eq!(buf.len(), write_res.unwrap());
        }

        clean("write");
    }

    #[test]
    fn test_data_file_sync() {
        setup("sync");
        let dir_path = PathBuf::from(basepath().join("sync"));
        let file_id = 2;

        {
            let data_file_res = DataFile::new(dir_path.clone(), file_id);
            assert!(data_file_res.is_ok());
            let data_file = data_file_res.unwrap();
            assert_eq!(file_id, data_file.get_file_id());

            // 写入
            let buf = "abc".as_bytes();
            let write_res = data_file.write(buf);
            assert!(write_res.is_ok());
            assert_eq!(buf.len(), write_res.unwrap());

            let sync_res = data_file.sync();
            assert!(sync_res.is_ok());
        }
        clean("sync");
    }

    #[test]
    fn test_data_file_read_log_record() {
        setup("read");
        let dir_path = PathBuf::from(basepath().join("read"));
        let file_id = 4;
        let mut offset = 0;

        let data_file_res = DataFile::new(dir_path.clone(), file_id);
        assert!(data_file_res.is_ok());
        let data_file = data_file_res.unwrap();
        assert_eq!(file_id, data_file.get_file_id());
        // 写入 - 读取 - 写入 - 读取
        {
            // 写入数据
            let key = "lucas".as_bytes().to_vec();
            let value = "LucasDBValue".as_bytes().to_vec();
            let log_record = LogRecord {
                key: key.clone(),
                value: value.clone(),
                rec_type: LogRecordType::Normal,
            };

            let encode_res = log_record.encode();
            assert!(encode_res.is_ok());
            let encode = encode_res.unwrap();

            let write_res = data_file.write(&encode);
            assert!(write_res.is_ok());

            // 从起始位置读取信息
            let read_log_record_res = data_file.read_log_record(offset);
            offset += write_res.unwrap() as u64;

            assert!(read_log_record_res.is_ok());
            let read_log_record = read_log_record_res.unwrap();

            assert_eq!(read_log_record.record.key, key);
            assert_eq!(read_log_record.record.value, value);

            // 写入新数据
            let key = "Hello".as_bytes().to_vec();
            let value = "Rust".as_bytes().to_vec();
            let log_record = LogRecord {
                key: key.clone(),
                value: value.clone(),
                rec_type: LogRecordType::Normal,
            };

            let encode_res = log_record.encode();
            assert!(encode_res.is_ok());
            let encode = encode_res.unwrap();

            let write_res = data_file.write(&encode);
            assert!(write_res.is_ok());

            // 从新的位置读取数据

            let read_log_record_res = data_file.read_log_record(offset);
            offset += write_res.unwrap() as u64;

            assert!(read_log_record_res.is_ok());
            let read_log_record = read_log_record_res.unwrap();

            assert_eq!(read_log_record.record.key, key);
            assert_eq!(read_log_record.record.value, value);
        }

        // 写入 DELETE - 读取 DELETE
        {
            // 写入新数据
            let key = "Hello".as_bytes().to_vec();
            // let value = "Rust".as_bytes().to_vec();
            let log_record = LogRecord {
                key: key.clone(),
                value: Default::default(),
                rec_type: LogRecordType::Deleted,
            };

            let encode_res = log_record.encode();
            assert!(encode_res.is_ok());
            let encode = encode_res.unwrap();

            let write_res = data_file.write(&encode);
            assert!(write_res.is_ok());

            // 从新的位置读取数据

            let read_log_record_res = data_file.read_log_record(offset);
            // offset += write_res.unwrap() as u64;

            assert!(read_log_record_res.is_ok());
            let read_log_record = read_log_record_res.unwrap();

            assert_eq!(read_log_record.record.key, key);
            assert_eq!(read_log_record.record.rec_type, LogRecordType::Deleted);
        }

        clean("read");
    }
}
