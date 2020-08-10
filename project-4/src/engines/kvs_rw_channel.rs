//! self implementation kvs engine

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::thread;

use anyhow::Context;

use crate::engines::KvsEngine;
use crate::error::KvsError;
use crate::model::Behavior;
use crate::Result;
use std::sync::{RwLock, Arc};
use crossbeam::{Sender, Receiver};
use crate::thread_pool::{RayonThreadPool, ThreadPool};

/// store keys and values
#[derive(Clone)]
pub struct KvStore {
    tx_reader: Sender<ChannelMessage>,
    tx_writer: Sender<ChannelMessage>,
}

impl KvStore {
    /// Open the KvStore at a given path. Return the KvStore.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        let (tx_reader, rx_reader) = crossbeam::unbounded::<ChannelMessage>();
        let (tx_writer, rx_writer) = crossbeam::unbounded::<ChannelMessage>();
        thread::spawn(move || {
            match KvsCore::open(path) {
                Ok(mut core) => {
                    if let Err(e) = core.receive_channel_message(rx_reader, rx_writer) {
                        log::error!("[KvsCore] receive message error, {}", e);
                    }
                }
                Err(e) => {
                    log::error!("[KvsCore] open error, {}", e);
                }
            }
            log::warn!("[KvsCore] closed");
        });


        Ok(KvStore { tx_reader, tx_writer })
    }


    fn request_behavior(&self, cmtx: &Sender<ChannelMessage>, behavior: Behavior) -> Result<Option<String>> {
        let (tx, rx) = crossbeam::unbounded::<Option<String>>();
        let cm = ChannelMessage {
            behavior,
            callback: tx,
        };
        cmtx.send(cm).map_err(|se| {
            log::error!("send channel message error, {:?}, {}", se.0.behavior, se);
            KvsError::Unknown
        })?;
        Ok(rx.recv()?)
    }

    fn request_reader_behavior(&self, behavior: Behavior) -> Result<Option<String>> {
        self.request_behavior(&self.tx_reader, behavior)
    }

    fn request_writer_behavior(&self, behavior: Behavior) -> Result<Option<String>> {
        self.request_behavior(&self.tx_writer, behavior)
    }
}

impl KvsEngine for KvStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        let behavior = Behavior::Set {
            key: key.clone(),
            value: value.clone(),
        };
        self.request_writer_behavior(behavior)?;
        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        let behavior = Behavior::Get { key: key.clone() };
        let string = self.request_reader_behavior(behavior)?;
        Ok(string)
    }

    fn remove(&self, key: String) -> Result<()> {
        let behavior = Behavior::Remove { key: key.clone() };

        if self.request_writer_behavior(behavior)?.is_none() {
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
    map: Arc<RwLock<HashMap<String, StoreValue>>>,
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
            map: Arc::new(RwLock::new(HashMap::new())),
            path,
            operation_count: 0,
            offset: 0,
        };
        core.init_from_buffer_reader(BufReader::new(file))?;
        Ok(core)
    }

    /// receive and handle message until channel closed
    fn receive_channel_message(
        &mut self,
        rx_reader: Receiver<ChannelMessage>,
        rx_writer: Receiver<ChannelMessage>,
    ) -> Result<()> {
        let threads = num_cpus::get() as u32;
        let thread_pool = RayonThreadPool::new(threads)?;
        for _ in 0..threads {
            let map_clone = self.map.clone();
            let rx_reader = rx_reader.clone();
            thread_pool.spawn(move || {
                let map = map_clone;
                while let Ok(cm) = rx_reader.recv() {
                    match &cm.behavior {
                        Behavior::Get { key } => {
                            if let Ok(guard) = map.read() {
                                let option = guard.get(key).map(|sv| {
                                    sv.to_value().ok()
                                }).flatten();
                                cm.callback.send(option).unwrap();
                            }
                        }
                        _ => unreachable!()
                    }
                }
            });
        }

        while let Ok(cm) = rx_writer.recv() {
            // log::info!("[receive_channel_message] recv: {:?}", cm.behavior);
            match &cm.behavior {
                Behavior::Set { key, ref value } => {
                    // TODO 将StoreValue::Memory转换成StoreValue::File
                    match self.map.write() {
                        Ok(mut guard) => {
                            guard.insert(key.to_owned(), StoreValue::Memory(value.to_owned()));
                        }
                        Err(e) => {
                            log::error!("behavior set error, {}", e);
                            Err(KvsError::Unknown)?
                        }
                    };
                    cm.callback.send(None)?;
                }
                Behavior::Remove { key } => {
                    let option = match self.map.write() {
                        Ok(mut guard) => {
                            guard.remove(key).map(|sv| {
                                sv.to_value().ok()
                            }).flatten()
                        }
                        Err(e) => {
                            log::error!("behavior remove error, {}", e);
                            Err(KvsError::Unknown)?
                        }
                    };
                    cm.callback.send(option)?;
                }
                _ => unreachable!()
            }
            self.flush(&cm.behavior)?;
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
        let mut map = self.map.write().map_err(|e| {
            log::error!("compact hold read lock error, {}", e);
            KvsError::Unknown
        })?;

        let mut text = String::new();
        // convert StoreValue::Memory to StoreValue::Value
        for entry in map.iter_mut() {
            let value = entry.1.to_value()?;
            *entry.1 = StoreValue::Memory(value.clone());
            let behavior = Behavior::Set {
                key: entry.0.to_owned(),
                value,
            };
            text += &format!("{}\n", serde_json::to_string(&behavior)?);
        }
        // release lock;
        drop(map);

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

        let mut map = self.map.write().map_err(|e| {
            log::error!("[apply_behavior_from_line] hold read lock error, {}", e);
            KvsError::Unknown
        })?;

        match behavior {
            Behavior::Set { key, value: _ } => {
                // map中保存behavior在文件中的偏移值和它的长度
                map.insert(key, StoreValue::File {
                    offset: self.offset,
                    len,
                    path: self.path.clone(),
                });
            }
            Behavior::Remove { key } => {
                map.remove(&key);
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
