//! 基于共享队列的线程池
//!
use std::thread;

use crossbeam::{Receiver, Sender};

use crate::*;
use crate::thread_pool::ThreadPool;

///
#[allow(dead_code)]
pub struct SharedQueueThreadPool {
    threads: u32,
    tx: Sender<ThreadPoolMessage>,
}

impl SharedQueueThreadPool {
    /// 向线程池里面补充新的线程
    fn add_thread(rx: Receiver<ThreadPoolMessage>) {
        std::thread::spawn(move || {
            let panic_probe = PanicProbe {
                rx: rx.clone(),
            };
            while let Ok(message) = rx.recv() {
                match message {
                    ThreadPoolMessage::RunJob(job) => {
                        job();
                    }
                    ThreadPoolMessage::Shutdown => {
                        break;
                    }
                }
            }
            drop(panic_probe);
        });
    }
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self> {
        let (tx, rx) = crossbeam::unbounded();
        for _ in 0..threads {
            let receiver = rx.clone();
            SharedQueueThreadPool::add_thread(receiver);
        }
        Ok(Self { threads, tx })
    }

    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        if let Err(e) = self.tx.send(ThreadPoolMessage::RunJob(Box::new(job))) {
            log::error!("[SharedQueueThreadPool] spawn error, {}", e);
        }
    }
}

impl Drop for SharedQueueThreadPool {
    fn drop(&mut self) {
        for _ in 0..self.threads {
            if let Err(e) = self.tx.send(ThreadPoolMessage::Shutdown) {
                log::error!("[SharedQueueThreadPool] send shutdown error, {}", e);
            }
        }
    }
}

enum ThreadPoolMessage {
    RunJob(Box<dyn FnOnce() + Send + 'static>),
    Shutdown,
}

/// 用于探测线程恐慌的探针
struct PanicProbe {
    rx: Receiver<ThreadPoolMessage>
}

impl Drop for PanicProbe {
    fn drop(&mut self) {
        if thread::panicking() {
            SharedQueueThreadPool::add_thread(self.rx.clone());
        }
    }
}