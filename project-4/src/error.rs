//! self defined error type

use thiserror::Error;

#[allow(missing_docs)]
#[derive(Error, Debug)]
pub enum KvsError {
    #[error("unknown error")]
    Unknown,
    #[error("Key not found")]
    KeyNotFound,
    #[error("Invalid IP address format, accept IP:PORT")]
    InvalidIPAddressFormat,
    #[error("Invalid argument number")]
    InvalidArgumentNumber,
    #[error("Wrong engine, expect {expect:?}, actual {actual:?}")]
    WrongEngine { expect: String, actual: String },
}
