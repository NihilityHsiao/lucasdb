use crate::prelude::*;
use bytes::{BufMut, BytesMut};
use prost::{decode_length_delimiter, encode_length_delimiter, length_delimiter_len};

/// 数据类型
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum LogRecordType {
    Normal = 1,
    /// 表示这个数据被删除了,merge的时候要清理掉
    Deleted = 2,
    /// 标识事务完成
    TxnFinished = 3,
}
impl LogRecordType {
    pub fn from_u8(value: u8) -> Self {
        match value {
            1 => LogRecordType::Normal,
            2 => LogRecordType::Deleted,
            3 => LogRecordType::TxnFinished,
            _ => panic!("Invalid log record type"),
        }
    }
}

/// 数据在磁盘中的索引
#[derive(Debug, Clone, Copy)]
pub struct LogRecordPos {
    /// 文件id,表示`LogRecord`存放到了哪个文件中
    pub(crate) file_id: u32,
    /// 偏移量,表示`LogRecord`存储到了数据文件的哪个位置(起始点)
    pub(crate) offset: u64,
    /// `LogReocrd`编码后 在磁盘上占据的空间
    pub(crate) size: usize,
}
impl LogRecordPos {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut buf = BytesMut::new();
        encode_length_delimiter(self.file_id as usize, &mut buf)?;
        encode_length_delimiter(self.offset as usize, &mut buf)?;
        encode_length_delimiter(self.size as usize, &mut buf)?;
        Ok(buf.to_vec())
    }

    pub fn decode(pos: Vec<u8>) -> Result<LogRecordPos> {
        let mut buf = BytesMut::new();
        buf.put_slice(&pos);

        let file_id = decode_length_delimiter(&mut buf)? as u32;
        let offset = decode_length_delimiter(&mut buf)? as u64;
        let size = decode_length_delimiter(&mut buf)?;

        Ok(LogRecordPos {
            file_id,
            offset,
            size,
        })
    }
}

/// 存储真正的数据
#[derive(Debug)]
pub struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) rec_type: LogRecordType,
}

impl LogRecord {
    /// 对 `LogRecord` 进行编码
    /// ```md
    /// | type    | key size          | value size          | key   | value | crc 校验值  |
    /// | ----    | ----------------- | ------------------  | ----- | ----- | ---------- |
    /// | 1 字节  | 变长 (最大 5 字节)  | 变长 (最大 5 字节)   | 变长  | 变长   | 4 字节     |
    /// ```
    pub fn encode(&self) -> Result<Vec<u8>> {
        let (enc_buf, _) = self.encode_and_get_crc()?;
        Ok(enc_buf)
    }
    pub fn get_crc(&self) -> u32 {
        let (_, crc) = self.encode_and_get_crc().unwrap_or((Vec::new(), 0));
        crc
    }
    /// 返回 `LogRecord` 编码后的长度
    fn encoded_length(&self) -> usize {
        std::mem::size_of::<u8>()
            + length_delimiter_len(self.key.len())
            + length_delimiter_len(self.value.len())
            + self.key.len()
            + self.value.len()
            + CRC_SIZE
    }

    fn encode_and_get_crc(&self) -> Result<(Vec<u8>, u32)> {
        let mut buf = BytesMut::new();
        buf.reserve(self.encoded_length());

        // 第一个字节:type
        buf.put_u8(self.rec_type as u8);

        // 存放 key、value的长度
        encode_length_delimiter(self.key.len(), &mut buf)?;
        encode_length_delimiter(self.value.len(), &mut buf)?;

        // 实际的key、value
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.value);

        // 存放crc
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf);
        let crc = hasher.finalize();

        buf.put_u32(crc);

        Ok((buf.to_vec(), crc))
    }
}

/// 从数据文件中读取的`LogRecord`的额外信息
#[derive(Debug)]
pub struct ReadLogRecord {
    pub(crate) record: LogRecord,
    /// 头部大小 + Key Size + Value Size + CRC校验值的大小
    pub(crate) size: usize,
}

/// 获取单个 `LogRecord`的 header 部分的最大值
/// Type + KeySize + ValueSize
/// 其中 KeySize 和 ValueSize 都是 u32类型的, 是可变长编码,根据整数大小来决定使用多少个字节
pub fn max_log_record_header_size() -> usize {
    // Type +  KeySize + ValueSize
    std::mem::size_of::<u8>() + length_delimiter_len(std::u32::MAX as usize) * 2
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use bytes::{Buf, Bytes};

    use super::*;
    fn basepath() -> &'static str {
        "./tmp/log_record"
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
    fn test_log_record_encode() {
        setup();

        // 正常的 log record
        {
            let key = "lucas".as_bytes().to_vec();
            let value = "DbTest".as_bytes().to_vec();
            let log_record = LogRecord {
                key: key,
                value: value,
                rec_type: LogRecordType::Normal,
            };

            // 编码
            let encode_res = log_record.encode();
            assert!(encode_res.is_ok());
            let encode = encode_res.unwrap();
            assert!(encode.len() > 5);

            // 计算crc,是否正确
            let buf = &encode[..encode.len() - 4]; // 最后4字节是CRC
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(buf);
            let recalculated_crc = hasher.finalize();

            assert_eq!(recalculated_crc, log_record.get_crc());
        }

        // value为空的 log record
        {
            let key = "lucas2".as_bytes().to_vec();
            let value = "".as_bytes().to_vec();
            let log_record = LogRecord {
                key: key,
                value: value,
                rec_type: LogRecordType::Normal,
            };

            // 编码
            let encode_res = log_record.encode();
            assert!(encode_res.is_ok());
            let encode = encode_res.unwrap();
            assert!(encode.len() > 5);

            // 计算crc,是否正确
            let buf = &encode[..encode.len() - 4]; // 最后4字节是CRC
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(buf);
            let recalculated_crc = hasher.finalize();

            assert_eq!(recalculated_crc, log_record.get_crc());
        }

        // type 为 deleted 的 log_record
        {
            let key = "lucas3".as_bytes().to_vec();
            let log_record = LogRecord {
                key: key,
                value: Default::default(),
                rec_type: LogRecordType::Deleted,
            };

            // 编码
            let encode_res = log_record.encode();
            assert!(encode_res.is_ok());
            let encode = encode_res.unwrap();
            assert!(encode.len() > 5);

            // 计算crc,是否正确
            let buf = &encode[..encode.len() - 4]; // 最后4字节是CRC
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(buf);
            let recalculated_crc = hasher.finalize();

            assert_eq!(recalculated_crc, log_record.get_crc());
        }
    }

    #[test]
    fn test_log_record_pos_decode() {
        let pos = LogRecordPos {
            file_id: 1,
            offset: 2,
            size: 3,
        };

        let encoded_pos = pos.encode().unwrap();

        let decoded_pos = LogRecordPos::decode(encoded_pos).unwrap();

        assert_eq!(pos.file_id, decoded_pos.file_id);
        assert_eq!(pos.offset, decoded_pos.offset);
        assert_eq!(pos.size, decoded_pos.size);
    }
}
