//! self defined error type

use thiserror::Error;

#[allow(missing_docs)]
#[derive(Error, Debug)]
pub enum KvsError {
    #[error("unknown error")]
    Unknown,
    #[error("Key not found")]
    KeyNotFound,
}
