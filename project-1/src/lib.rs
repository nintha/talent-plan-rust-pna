#![warn(missing_docs)]
//! a simple key/value store
use std::collections::HashMap;

/// store keys and values
pub struct KvStore {
    map: HashMap<String, String>,
}

impl KvStore {
    /// new an instant
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    /// storing a key with associated value
    pub fn set(&mut self, key: String, value: String) {
        self.map.insert(key, value);
    }

    /// get a value from key
    pub fn get(&mut self, key: String) -> Option<String> {
        self.map.get(&key).map(|x| x.to_owned())
    }

    /// remove a key and associated value
    pub fn remove(&mut self, key: String) {
        self.map.remove(&key);
    }
}

impl Default for KvStore {
    fn default() -> Self {
        Self::new()
    }
}
