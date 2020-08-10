//! thread pool

pub use naive_thread_pool::NaiveThreadPool;
pub use shared_queue_thread_pool::SharedQueueThreadPool;
pub use rayon_thread_pool::RayonThreadPool;
use crate::*;

mod naive_thread_pool;
mod shared_queue_thread_pool;
mod rayon_thread_pool;

///
pub trait ThreadPool<RET=Self> {
    /// Creates a new thread pool, immediately spawning the specified number of threads.
    /// Returns an error if any thread fails to spawn. All previously-spawned threads are terminated.
    fn new(threads: u32) -> Result<RET>;
    /// Spawn a function into the threadpool.
    /// Spawning always succeeds, but if the function panics the threadpool continues to
    /// operate with the same number of threads — the thread count is not reduced nor is
    /// the thread pool destroyed, corrupted or invalidated.
    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static;
}

