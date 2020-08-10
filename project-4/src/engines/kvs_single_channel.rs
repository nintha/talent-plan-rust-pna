//! self implementation kvs engine

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;

use anyhow::Context;

use crate::engines::KvsEngine;
use crate::error::KvsError;
use crate::model::Behavior;
use crate::Result;

/// store keys and values
#[derive(Clone)]
pub struct KvStore {
    tx: Sender<ChannelMessage>,
}

impl KvStore {
    /// Open the KvStore at a given path. Return the KvStore.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        let (tx, rx) = channel::<ChannelMessage>();
        thread::spawn(move || {
            match KvsCore::open(path) {
                Ok(mut core) => {
                    if let Err(e) = core.receive_channel_message(rx) {
                        log::error!("[KvsCore] receive message error, {}", e);
                    }
                }
                Err(e) => {
                    log::error!("[KvsCore] open error, {}", e);
                }
            }
            log::warn!("[KvsCore] closed");
        });

        Ok( KvStore { tx })
    }


    fn request_behavior(&self, behavior: Behavior) -> Result<Option<String>> {
        let (tx, rx) = channel::<Option<String>>();
        let cm = ChannelMessage {
            behavior,
            callback: tx,
        };
        self.tx.send(cm).map_err(|se| {
            log::error!("send channel message error, {:?}, {}", se.0.behavior, se);
            KvsError::Unknown
        })?;
        Ok(rx.recv()?)
    }
}

impl KvsEngine for KvStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        let behavior = Behavior::Set {
            key: key.clone(),
            value: value.clone(),
        };
        self.request_behavior(behavior)?;
        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        let behavior = Behavior::Get { key: key.clone() };
        let string = self.request_behavior(behavior)?;
        Ok(string)
    }

    fn remove(&self, key: String) -> Result<()> {
        let behavior = Behavior::Remove { key: key.clone() };

        if self.request_behavior(behavior)?.is_none() {
            Err(KvsError::KeyNotFound)?
        }
        Ok(())
    }

    fn engine_name(&self) -> String {
        return "kvs".to_owned();
    }
}

/// kv存储值
#[derive(Clone, Debug)]
enum StoreValue {
    Memory(String),
    File {
        offset: u64,
        len: usize,
        path: PathBuf,
    },
}

impl StoreValue {
    fn to_value(&self) -> Result<String> {
        let text = match self {
            StoreValue::Memory(v) => v.to_owned(),
            StoreValue::File { offset, len, path } => {
                let mut file = OpenOptions::new().read(true).open(path)?;
                let buffer = KvsCore::read_file_offset(&mut file, *offset, *len)?;
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

/// 通道消息
struct ChannelMessage {
    behavior: Behavior,
    callback: Sender<Option<String>>,
}

/// 基于消息的kvs核心实现
struct KvsCore {
    map: HashMap<String, StoreValue>,
    path: PathBuf,
    operation_count: u64,
    offset: u64,
}

impl KvsCore {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let mut path = path.into();
        path.push("x.log");
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .read(true)
            .open(path.clone())?;

        let mut core = KvsCore {
            map: HashMap::new(),
            path,
            operation_count: 0,
            offset: 0,
        };
        core.init_from_buffer_reader(BufReader::new(file))?;
        Ok(core)
    }

    /// receive and handle message until channel closed
    fn receive_channel_message(&mut self, rx: Receiver<ChannelMessage>) -> Result<()> {
        while let Ok(cm) = rx.recv() {
            log::info!("[receive_channel_message] recv: {:?}", cm.behavior);
            match &cm.behavior {
                Behavior::Set { key, ref value } => {
                    // TODO 将StoreValue::Memory转换成StoreValue::File
                    self.map.insert(key.to_owned(), StoreValue::Memory(value.to_owned()));
                    self.flush(&cm.behavior)?;
                    cm.callback.send(None)?;
                }
                Behavior::Get { key } => {
                    let option = self.map.get(key).map(|sv| {
                        sv.to_value().ok()
                    }).flatten();
                    cm.callback.send(option)?;
                }
                Behavior::Remove { key } => {
                    let option = self.map.remove(key).map(|sv| {
                        sv.to_value().ok()
                    }).flatten();
                    self.flush(&cm.behavior)?;
                    cm.callback.send(option)?;
                }
            }
        }
        log::info!("[receive_channel_message] rx end");
        Ok(())
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
        for entry in self.map.iter_mut() {
            let value = entry.1.to_value()?;
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
        for line in file_text.lines() {
            self.apply_behavior_from_line(line)?;
        }
        Ok(())
    }

    /// 从缓冲初始化KvStore
    fn init_from_buffer_reader(&mut self, reader: BufReader<File>) -> Result<()> {
        self.offset = 0;
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
                self.map.insert(key, StoreValue::File {
                    offset: self.offset,
                    len,
                    path: self.path.clone(),
                });
            }
            Behavior::Remove { key } => {
                self.map.remove(&key);
            }
            _ => {}
        }
        self.offset += len as u64 + 1;
        Ok(())
    }

    fn flush(&mut self, behavior: &Behavior) -> Result<()> {
        let mut file = OpenOptions::new()
            .append(true)
            .open(&self.path)?;
        let json = serde_json::to_string(&behavior)?;
        file.write_all(format!("{}\n", json).as_bytes())?;
        file.flush()?;
        self.update_operation_count()?;
        Ok(())
    }
}
