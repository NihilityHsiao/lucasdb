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
}
