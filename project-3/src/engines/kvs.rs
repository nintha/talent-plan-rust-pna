//! self implementation kvs engine

use std::collections::{HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{PathBuf, Path};

use anyhow::Context;

use crate::engines::KvsEngine;
use crate::error::KvsError;
use crate::model::Behavior;
use crate::Result;
use std::sync::{Mutex, Arc};
use chrono::Local;
use crate::engines::sled::SledKvsEngine;
use std::time::Duration;

/// store keys and values
pub struct KvStore {
    map: HashMap<String, StoreValue>,
    path: PathBuf,
    operation_count: u64,
    offset: u64,
    behavior_queue: Arc<Mutex<VecDeque<Behavior>>>,
    next_flush_time: i64,
}

impl KvStore {
    /// Open the KvStore at a given path. Return the KvStore.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let mut path = path.into();
        path.push("x.log");
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .read(true)
            .open(path.clone())?;

        let mut store = KvStore {
            map: HashMap::new(),
            path,
            operation_count: 0,
            offset: 0,
            behavior_queue: Arc::new(Mutex::new(VecDeque::new())),
            next_flush_time: 0,
        };
        store.init_from_buffer_reader(BufReader::new(file))?;
        Ok(store)
    }

    fn update_operation_count(&mut self) -> Result<()> {
        self.operation_count += 1;
        if self.operation_count % 2048 == 0 {
            self.compact()?;
        }
        Ok(())
    }

    /// compact the log
    fn compact(&mut self) -> Result<()> {
        let mut text = String::new();
        // convert StoreValue::Memory to StoreValue::Value
        for entry in &mut self.map {
            let value = entry.1.to_value(&self.path)?;
            *entry.1 = StoreValue::Memory(value.clone());
            let behavior = Behavior::Set {
                key: entry.0.to_owned(),
                value,
            };
            text += &format!("{}\n", serde_json::to_string(&behavior)?);
        }
        let mut truncate_file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(self.path.clone())?;
        truncate_file.write_all(text.as_bytes())?;
        self.init_from_file_text(text)?;
        // clear queue
        let arc_clone = self.behavior_queue.clone();
        if let Ok(x) = arc_clone.lock().as_mut() {
            x.clear();
        }

        // let mut read_only_file = OpenOptions::new()
        //     .read(true)
        //     .open(self.path.clone())?;
        // let mut text = String::new();
        // for entry in &self.map {
        //     let point = entry.1;
        //     let vec = KvStore::read_file_offset(&mut read_only_file, point.0, point.1)?;
        //     let json = String::from_utf8(vec)
        //         .with_context(|| format!("Failed to get string from vec"))?;
        //     // println!("[compact] map entry, {}: '{}', {}, {}", entry.0, &json, point.0, point.1);
        //     text += &format!("{}\n", json);
        // }
        // drop(read_only_file);
        // let mut truncate_file = OpenOptions::new()
        //     .write(true)
        //     .truncate(true)
        //     .open(self.path.clone())?;
        // truncate_file.write_all(text.as_bytes())?;
        // self.init_from_file_text(text)?;
        Ok(())
    }

    /// append behavior serialized json to file
    /// @return the json serialized from behavior
    fn log_behavior(&mut self, behavior: Behavior) -> Result<()> {
        let arc_clone = self.behavior_queue.clone();
        if let Ok(x) = arc_clone.lock().as_mut() {
            x.push_back(behavior);
        }
        self.debounce_flush()?;
        Ok(())
    }

    /// 从偏移值读取文件内容
    fn read_file_offset(file: &mut File, offset: u64, size: usize) -> Result<Vec<u8>> {
        file.seek(SeekFrom::Start(offset))?;
        let mut buffer = Vec::with_capacity(size);
        buffer.resize(size, 0);
        file.read_exact(&mut buffer)?;
        Ok(buffer)
    }

    /// 从文本内容初始化KvStore
    fn init_from_file_text(&mut self, file_text: String) -> Result<()> {
        self.offset = 0;
        self.map.clear();
        for line in file_text.lines() {
            self.apply_behavior_from_line(line)?;
        }
        Ok(())
    }

    /// 从缓冲初始化KvStore
    fn init_from_buffer_reader(&mut self, reader: BufReader<File>) -> Result<()> {
        self.offset = 0;
        self.map.clear();
        for line in reader.lines() {
            self.apply_behavior_from_line(&line?)?;
        }
        Ok(())
    }

    /// 从文本行应用操作
    fn apply_behavior_from_line(&mut self, line: &str) -> Result<()> {
        let len = line.len();
        let behavior = serde_json::from_str::<Behavior>(line)
            .with_context(|| format!("Failed to deserialize behavior from {}", line))?;
        match behavior {
            Behavior::Set { key, value: _ } => {
                // map中保存behavior在文件中的偏移值和它的长度
                self.map.insert(key, StoreValue::File { offset: self.offset, len });
            }
            Behavior::Remove { key } => {
                self.map.remove(&key);
            }
            _ => {}
        }
        self.offset += len as u64 + 1;
        Ok(())
    }

    fn flush(path: &Path, behavior_queue: Arc<Mutex<VecDeque<Behavior>>>) -> Result<()> {
        let mut file = OpenOptions::new()
            .append(true)
            .open(path)?;
        if let Ok(queue) = behavior_queue.lock().as_mut() {
            while let Some(behavior) = queue.pop_front() {
                let json = serde_json::to_string(&behavior)?;
                file.write_all(format!("{}\n", json).as_bytes())?;
            }
        }
        file.flush()?;
        Ok(())
    }

    fn debounce_flush(&mut self) -> Result<()> {
        if SledKvsEngine::FLUSH_DELAY == 0 {
            Self::flush(self.path.as_path(), self.behavior_queue.clone())?;
            self.update_operation_count()?;
            return Ok(());
        }
        let now = Local::now().timestamp_millis();

        // call it too frequently
        if now < self.next_flush_time {
            return Ok(());
        }

        self.next_flush_time = now + SledKvsEngine::FLUSH_DELAY;
        let queue = self.behavior_queue.clone();
        let path = self.path.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(SledKvsEngine::FLUSH_DELAY as u64));
            if let Err(e) = Self::flush(path.as_path(), queue) {
                log::error!("[KvsStore] flush error, {}", e);
            }
        });

        self.update_operation_count()?;
        Ok(())
    }
}

impl KvsEngine for KvStore {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.map.insert(key.to_owned(), StoreValue::Memory(value.clone()));
        let behavior = Behavior::Set {
            key: key.clone(),
            value: value.clone(),
        };
        self.log_behavior(behavior)?;
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        let option = self
            .map.get(&key)
            .map(|x| x.to_value(self.path.as_path()));
        if let Some(v) = option {
            Ok(Some(v?))
        } else {
            Ok(None)
        }
    }

    fn remove(&mut self, key: String) -> Result<()> {
        let option = self.map.remove(&key);
        if option.is_none() {
            Err(KvsError::KeyNotFound)?
        }
        let behavior = Behavior::Remove { key: key.clone() };
        self.log_behavior(behavior)?;
        Ok(())
    }

    fn engine_name(&self) -> String {
        return "kvs".to_owned();
    }
}

impl Drop for KvStore {
    fn drop(&mut self) {
        if let Err(e) = Self::flush(self.path.as_path(), self.behavior_queue.clone()) {
            log::error!("[KvStore][drop] flush error, {}", e);
        }
    }
}

/// kv存储值
enum StoreValue {
    Memory(String),
    File {
        offset: u64,
        len: usize,
    },
}

impl StoreValue {
    fn to_value(&self, path: &Path) -> Result<String> {
        let text = match self {
            StoreValue::Memory(v) => v.to_owned(),
            StoreValue::File { offset, len } => {
                let mut file = OpenOptions::new().read(true).open(path)?;
                let buffer = KvStore::read_file_offset(&mut file, *offset, *len)?;
                let line = String::from_utf8(buffer)?;
                let behavior = serde_json::from_str::<Behavior>(&line)
                    .with_context(|| format!("Failed to deserialize behavior from {}", line))?;
                if let Behavior::Set { key: _, value } = behavior {
                    value
                } else {
                    log::error!("[store value] deserialize error, not Behavior::Set, {:?}", &behavior);
                    Err(KvsError::Unknown)?
                }
            }
        };
        Ok(text)
    }
}