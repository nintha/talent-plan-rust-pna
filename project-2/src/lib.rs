#![warn(missing_docs)]
//! a simple key/value store
use crate::error::KvsError;
use crate::model::Behavior;
use std::collections::HashMap;
use std::fs::{OpenOptions, File};
use std::io::{Read, Write, Seek, SeekFrom};
use std::path::PathBuf;
use anyhow::Context;

pub mod error;
pub mod model;

/// simply type
pub type Result<T> = std::result::Result<T, anyhow::Error>;

/// store keys and values
pub struct KvStore {
    map: HashMap<String, (u64, usize)>,
    path: PathBuf,
    operation_count: u64,
    offset: u64,
}

impl KvStore {
    /// Open the KvStore at a given path. Return the KvStore.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let mut path = path.into();
        path.push("x.log");
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .read(true)
            .open(path.clone())?;
        let mut text = String::new();
        file.read_to_string(&mut text)?;

        let mut store = KvStore { map: HashMap::new(), path, operation_count: 0, offset: 0 };
        store.init_from_file_text(text)?;
        // store.compact()?;
        Ok(store)
    }

    /// storing a key with associated value
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let behavior = Behavior::Set {
            key: key.clone(),
            value: value.clone(),
        };
        let json = self.log_behavior(&behavior)
            .with_context(|| format!("Failed to log behavior, key={}, value={}", &key, &value))?;
        self.map.insert(key.to_owned(), (self.offset, json.len()));
        self.offset += json.len() as u64 + 1;

        if let Some(v) = self.get(key.clone())? {
            if v != value {
                eprintln!("[set] Failed set key {} to value {}, read value={}", &key, &value, &v);
            }
        } else {
            eprintln!("[set] Failed set key {} to value {}", &key, &value);
        }
        self.update_operation_count()?;
        Ok(())
    }

    /// get a value from key
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let option = if let Some(x) = self.map.get(&key) {
            let mut file = OpenOptions::new().read(true).open(self.path.clone())?;
            let buffer = KvStore::read_file_offset(&mut file, x.0, x.1)?;
            let line = String::from_utf8(buffer)?;
            let behavior = serde_json::from_str::<Behavior>(&line)
                .with_context(|| format!("Failed to deserialize behavior from {}", line))?;
            if let Behavior::Set { key: _, value } = behavior {
                Some(value)
            } else {
                None
            }
        } else {
            None
        };
        Ok(option)
    }

    /// remove a key and associated value
    pub fn remove(&mut self, key: String) -> Result<()> {
        let option = self.map.remove(&key);
        let behavior = Behavior::Remove { key: key.clone() };
        let json = self.log_behavior(&behavior)?;
        self.offset += json.len() as u64 + 1;

        self.update_operation_count()?;
        option.map(|_| ()).ok_or(KvsError::KeyNotFound.into())
    }

    fn update_operation_count(&mut self) -> Result<()> {
        self.operation_count += 1;
        if self.operation_count % 1000 == 0 {
            self.compact()?;
        }
        Ok(())
    }

    /// compact the log
    fn compact(&mut self) -> Result<()> {
        let mut read_only_file = OpenOptions::new()
            .read(true)
            .open(self.path.clone())?;
        let mut text = String::new();
        for entry in &self.map {
            let point = entry.1;
            let vec = KvStore::read_file_offset(&mut read_only_file, point.0, point.1)?;
            let json = String::from_utf8(vec)
                .with_context(|| format!("Failed to get string from vec"))?;
            // println!("[compact] map entry, {}: '{}', {}, {}", entry.0, &json, point.0, point.1);
            text += &format!("{}\n", json);
        }
        drop(read_only_file);
        let mut truncate_file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(self.path.clone())?;
        truncate_file.write_all(text.as_bytes())?;
        self.init_from_file_text(text)?;
        Ok(())
    }

    /// append behavior serialized json to file
    /// @return the json serialized from behavior
    fn log_behavior(&mut self, behavior: &Behavior) -> Result<String> {
        let mut file = OpenOptions::new()
            .append(true)
            .open(self.path.clone())?;
        let json = serde_json::to_string(&behavior)?;
        file.write_all(format!("{}\n", json).as_bytes())?;
        file.flush()?;
        Ok(json)
    }

    /// 从偏移值读取文件内容
    fn read_file_offset(file: &mut File, offset: u64, size: usize) -> Result<Vec<u8>> {
        file.seek(SeekFrom::Start(offset))?;
        let mut buffer = Vec::with_capacity(size);
        buffer.resize(size, 0);
        file.read_exact(&mut buffer)?;
        Ok(buffer)
    }

    /// 从文件内容初始化KvStore
    fn init_from_file_text(&mut self, file_text: String) -> Result<()> {
        self.offset = 0;
        self.map.clear();
        for line in file_text.lines() {
            let len = line.len();
            let behavior = serde_json::from_str::<Behavior>(line)
                .with_context(|| format!("Failed to deserialize behavior from {}", line))?;
            match behavior {
                Behavior::Set { key, value: _ } => {
                    // map中保存behavior在文件中的偏移值和它的长度
                    self.map.insert(key, (self.offset, len));
                }
                Behavior::Remove { key } => {
                    self.map.remove(&key);
                }
                _ => {}
            }
            self.offset += len as u64 + 1;
        }
        Ok(())
    }
}
