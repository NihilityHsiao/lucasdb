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
}
