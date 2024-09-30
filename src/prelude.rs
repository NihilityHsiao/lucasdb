// ! Crate prelude
pub use crate::errors::Errors;

pub type Result<T> = std::result::Result<T, Errors>;

// 数据文件的后缀, 00001.data
pub const DATA_FILE_NAME_SUFFIX: &str = ".data";
