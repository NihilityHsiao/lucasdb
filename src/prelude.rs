// ! Crate prelude
pub use crate::errors::{Errors, Result};

// 数据文件的后缀, 00001.data
pub const DATA_FILE_NAME_SUFFIX: &str = ".data";
pub const CRC_SIZE: usize = 4;

// KEY的名称
pub const TXN_FINISHED_KEY: &[u8] = "transaction_finished".as_bytes();
// 标识它不是事务数据
pub(crate) const NON_TRANSACTION_SEQ_NO: usize = 0;
