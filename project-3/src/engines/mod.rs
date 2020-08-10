//! kvs engine

use crate::Result;
pub mod kvs;
pub mod sled;

/// defines the storage interface called by KvsServer
pub trait KvsEngine{
    /// Set the value of a string key to a string.
    /// Return an error if the value is not written successfully.
    fn set(&mut self, key: String, value: String) -> Result<()>;

    /// Get the string value of a string key. If the key does not exist, return None.
    /// Return an error if the value is not read successfully.
    fn get(&mut self, key: String) -> Result<Option<String>>;

    /// Remove a given string key.
    /// Return an error if the key does not exit or value is not read successfully.
    fn remove(&mut self, key: String) -> Result<()>;

    /// The engine name
    fn engine_name(&self) -> String;
}