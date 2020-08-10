#![warn(missing_docs)]
//! a simple key/value store
pub use engines::kvs::KvStore;
pub use engines::KvsEngine;

pub mod error;
pub mod model;
pub mod logger;
pub mod engines;
pub mod server;
pub mod client;
pub mod thread_pool;

/// simply type
pub type Result<T> = std::result::Result<T, anyhow::Error>;

