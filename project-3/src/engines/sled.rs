//! wrap sled as kvs engine
use std::path::PathBuf;

use sled::Db;

use crate::engines::KvsEngine;
use crate::error::KvsError;
use crate::Result;
use chrono::Local;
use std::time::Duration;

/// store keys and values
pub struct SledKvsEngine {
    db: Db,
    next_flush_time: i64,
}

impl SledKvsEngine {
    /// Open the SledKvsEngine at a given path
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let db = sled::open(path.into())?;
        Ok(Self { db, next_flush_time: 0 })
    }
}

impl SledKvsEngine {
    pub(crate) const FLUSH_DELAY: i64 = 0;
    fn debounce_flush(&mut self) -> Result<()> {
        if SledKvsEngine::FLUSH_DELAY == 0 {
            self.db.flush()?;
            return Ok(());
        }
        let now = Local::now().timestamp_millis();

        // call it too frequently
        if now < self.next_flush_time {
            return Ok(());
        }

        self.next_flush_time = now + SledKvsEngine::FLUSH_DELAY;
        let db_clone = self.db.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(SledKvsEngine::FLUSH_DELAY as u64));
            if let Err(e) = db_clone.flush() {
                log::error!("[SledKvsEngine] flush error, {}", e);
            }
        });

        Ok(())
    }
}

impl KvsEngine for SledKvsEngine {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.db.insert(key, value.as_bytes())?;
        self.debounce_flush()?;
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        let rs = self.db
            .get(key)?
            .map(|ivec| ivec.to_vec());

        if let Some(bytes) = rs {
            return Ok(Some(String::from_utf8(bytes)?));
        }
        Ok(None)
    }

    fn remove(&mut self, key: String) -> Result<()> {
        let option = self.db.remove(key)?;
        self.debounce_flush()?;
        option.map(|_| ()).ok_or(KvsError::KeyNotFound.into())
    }

    fn engine_name(&self) -> String {
        return "sled".to_owned();
    }
}
