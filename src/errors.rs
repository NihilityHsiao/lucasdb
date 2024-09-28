#[derive(Debug, thiserror::Error)]
pub enum Errors {
    #[error(transparent)]
    IO(#[from] std::io::Error),
}
