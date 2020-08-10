use crate::*;
use crate::thread_pool::ThreadPool;

/// 未池化的线程池，使用效果和直接创建线程一致
#[allow(dead_code)]
pub struct NaiveThreadPool {
    threads: u32
}

impl ThreadPool for NaiveThreadPool {
    fn new(threads: u32) -> Result<Self>{
        Ok(NaiveThreadPool { threads })
    }

    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        std::thread::spawn(job);
    }
}