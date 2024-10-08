pub type Result<T> = std::result::Result<T, Errors>;

#[derive(Debug, thiserror::Error)]
pub enum Errors {
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error("key is empty")]
    KeyIsEmpty,

    #[error("key not found")]
    KeyNotFound,

    #[error("failed to update index")]
    IndexUpdateFailed,

    #[error("failed to find data file")]
    DataFileNotFound,

    #[error("database dir path can not be empty")]
    DirPathIsEmpty,

    #[error("database data file size must be greater than 0")]
    DataFileSizeTooSmall,

    #[error("failed to read database data file directory, {0}")]
    DataFileLoadError(std::io::Error),

    #[error("data file has been broken")]
    DataFileBroken,

    #[error("read data file eof")]
    ReadDataFileEOF,

    #[error(transparent)]
    DecodeError(#[from] prost::DecodeError),

    #[error(transparent)]
    EncodeError(#[from] prost::EncodeError),

    #[error("invalid log record crc")]
    InvalidLogRecordCrc,

    #[error("exceed the max batch num, max:{}, current:{}", max, current)]
    ExceedMaxBatchNum { max: u32, current: u32 },

    #[error("transaction sequence number not found: {0}")]
    TxnNumberNotFound(usize),

    #[error("merge is in progress")]
    MergeInProgress,
    #[error("seq number file not found")]
    SeqNoFileNotExist,
    #[error(transparent)]
    FromUtf8Error(#[from] std::string::FromUtf8Error),
    #[error(transparent)]
    ParseIntError(#[from] std::num::ParseIntError),

    #[error("the database dir is used by another process")]
    DatabaseIsUsing,
    #[error("invalid merge ratio")]
    InvalidMergeRatio,

    #[error("do not reach the merge ratio, now:{0}, ratio:{1}", now, ratio)]
    MergeRatioUnreached { now: f32, ratio: f32 },

    #[error(
        "disk space is not enough for merge, actual:{}, expected:{}",
        actual,
        expected
    )]
    MergeSpaceNotEnough { actual: u64, expected: u64 },

    #[error("failed to copy database directory")]
    FailedToBackupDatabase,

    #[error("wrong type operation, expected:{}, actual:{}", expected, actual)]
    WrongTypeOperation { expected: String, actual: String },
}
