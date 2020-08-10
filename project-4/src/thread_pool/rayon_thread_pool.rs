use crate::thread_pool::ThreadPool;
use crate::*;

/// 基于Rayon线程池的封装
pub struct RayonThreadPool{
    thread_pool: rayon::ThreadPool,
}

impl ThreadPool for RayonThreadPool{
    fn new(threads: u32) -> Result<Self> {
        let thread_pool = rayon::ThreadPoolBuilder::new().num_threads(threads as usize).build()?;
        Ok(Self{thread_pool})
    }

    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        self.thread_pool.spawn(job);
    }
}