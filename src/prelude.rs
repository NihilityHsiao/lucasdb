// ! Crate prelude
pub use crate::errors::Errors;

pub type Result<T> = std::result::Result<T, Errors>;
