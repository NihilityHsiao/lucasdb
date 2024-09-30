use crate::{
    data::log_record::{LogRecord, LogRecordPos},
    prelude::*,
};
use bytes::{BufMut, BytesMut};
use prost::{decode_length_delimiter, encode_length_delimiter};

pub mod batch;

/// 启动数据库时,暂存事务数据
pub struct TransactionRecord {
    pub(crate) record: LogRecord,
    pub(crate) pos: LogRecordPos,
}

/// 给key的前面加上seq_no编码
pub(crate) fn log_record_key_with_seq(key: Vec<u8>, seq_no: usize) -> Result<Vec<u8>> {
    let mut enc_key = BytesMut::new();
    encode_length_delimiter(seq_no, &mut enc_key)?;
    enc_key.extend_from_slice(&key.to_vec());
    Ok(enc_key.to_vec())
}

/// 从一个LogRecord的key中解析出真正的Key和序列号
pub(crate) fn parse_log_record_key(key: Vec<u8>) -> Result<(Vec<u8>, usize)> {
    let mut buf = BytesMut::new();
    buf.put_slice(&key);
    let seq_no = decode_length_delimiter(&mut buf)?;
    Ok((buf.to_vec(), seq_no))
}
