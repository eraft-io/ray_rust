//! Worker process management for the Raylet.
//!
//! A "worker" in Ray Rust is a unit of execution capacity. In the initial
//! single-node implementation, workers are lightweight in-process task runners
//! backed by tokio blocking threads. Future versions will support real
//! subprocess workers for Python/Java function execution.

use ray_core::id::WorkerId;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Worker status constants.
pub const WORKER_IDLE: u8 = 0;
pub const WORKER_BUSY: u8 = 1;
pub const WORKER_DEAD: u8 = 2;

/// Handle to a worker process (or in-process task runner).
pub struct WorkerHandle {
    /// Unique worker identifier.
    pub worker_id: WorkerId,
    /// Process ID (0 for in-process workers).
    pub pid: u32,
    /// Current status: IDLE, BUSY, or DEAD.
    pub status: Arc<AtomicU8>,
}

impl WorkerHandle {
    /// Create a new in-process worker.
    pub fn new_in_process() -> Self {
        Self {
            worker_id: WorkerId::new(),
            pid: std::process::id(),
            status: Arc::new(AtomicU8::new(WORKER_IDLE)),
        }
    }

    /// Check if the worker is idle (available for work).
    pub fn is_idle(&self) -> bool {
        self.status.load(Ordering::Acquire) == WORKER_IDLE
    }

    /// Check if the worker is busy (currently executing).
    pub fn is_busy(&self) -> bool {
        self.status.load(Ordering::Acquire) == WORKER_BUSY
    }

    /// Check if the worker is dead (process exited).
    pub fn is_dead(&self) -> bool {
        self.status.load(Ordering::Acquire) == WORKER_DEAD
    }

    /// Mark the worker as busy.
    pub fn mark_busy(&self) {
        self.status.store(WORKER_BUSY, Ordering::Release);
    }

    /// Mark the worker as idle.
    pub fn mark_idle(&self) {
        self.status.store(WORKER_IDLE, Ordering::Release);
    }

    /// Mark the worker as dead.
    pub fn mark_dead(&self) {
        self.status.store(WORKER_DEAD, Ordering::Release);
    }
}

/// Manages a pool of workers, providing allocation and recycling.
pub struct WorkerPool {
    workers: RwLock<Vec<Arc<WorkerHandle>>>,
    max_workers: usize,
}

impl WorkerPool {
    /// Create a new worker pool with the given capacity.
    pub fn new(max_workers: usize) -> Self {
        info!(max_workers, "WorkerPool created");
        Self {
            workers: RwLock::new(Vec::with_capacity(max_workers)),
            max_workers,
        }
    }

    /// Get an idle worker from the pool, or spawn a new one if under capacity.
    ///
    /// Returns `None` if no idle workers are available and the pool is at capacity.
    pub async fn get_worker(&self) -> Option<Arc<WorkerHandle>> {
        // First, try to find an existing idle worker
        {
            let workers = self.workers.read().await;
            for w in workers.iter() {
                if w.is_idle() {
                    w.mark_busy();
                    debug!(worker_id = ?w.worker_id, "Reusing idle worker");
                    return Some(w.clone());
                }
            }
        }

        // No idle workers; spawn a new one if under capacity
        let mut workers = self.workers.write().await;
        // Re-check after acquiring write lock (another task may have freed one)
        for w in workers.iter() {
            if w.is_idle() {
                w.mark_busy();
                debug!(worker_id = ?w.worker_id, "Reusing idle worker (after write lock)");
                return Some(w.clone());
            }
        }

        if workers.len() < self.max_workers {
            let handle = Arc::new(WorkerHandle::new_in_process());
            handle.mark_busy();
            workers.push(handle.clone());
            debug!(
                worker_id = ?handle.worker_id,
                pool_size = workers.len(),
                "Spawned new worker"
            );
            return Some(handle);
        }

        warn!("WorkerPool exhausted: no idle workers and at capacity ({})", self.max_workers);
        None
    }

    /// Return a worker to the pool (mark as idle for reuse).
    pub async fn return_worker(&self, handle: &WorkerHandle) {
        if handle.is_dead() {
            warn!(worker_id = ?handle.worker_id, "Returning dead worker");
            // Remove dead workers from the pool
            let mut workers = self.workers.write().await;
            workers.retain(|w| !w.is_dead());
            return;
        }
        handle.mark_idle();
        debug!(worker_id = ?handle.worker_id, "Worker returned to pool");
    }

    /// Kill a specific worker (mark as dead and remove from pool).
    pub async fn kill_worker(&self, worker_id: &WorkerId) {
        let workers = self.workers.read().await;
        for w in workers.iter() {
            if &w.worker_id == worker_id {
                w.mark_dead();
                info!(?worker_id, "Worker killed");
                break;
            }
        }
        // Clean up dead workers
        drop(workers);
        let mut workers = self.workers.write().await;
        workers.retain(|w| !w.is_dead());
    }

    /// Get the number of idle workers.
    pub async fn idle_count(&self) -> usize {
        self.workers.read().await.iter().filter(|w| w.is_idle()).count()
    }

    /// Get the number of busy workers.
    pub async fn busy_count(&self) -> usize {
        self.workers.read().await.iter().filter(|w| w.is_busy()).count()
    }

    /// Get the total number of workers in the pool.
    pub async fn total_count(&self) -> usize {
        self.workers.read().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_worker_pool_spawn_and_return() {
        let pool = WorkerPool::new(2);

        // Get first worker
        let w1 = pool.get_worker().await.unwrap();
        assert!(w1.is_busy());
        assert_eq!(pool.total_count().await, 1);

        // Get second worker
        let w2 = pool.get_worker().await.unwrap();
        assert!(w2.is_busy());
        assert_eq!(pool.total_count().await, 2);

        // Pool exhausted
        assert!(pool.get_worker().await.is_none());

        // Return first worker
        pool.return_worker(&w1).await;
        assert!(w1.is_idle());

        // Now we can get a worker again (reuses w1)
        let w3 = pool.get_worker().await.unwrap();
        assert_eq!(w3.worker_id, w1.worker_id);
    }

    #[tokio::test]
    async fn test_worker_pool_kill() {
        let pool = WorkerPool::new(2);

        let w = pool.get_worker().await.unwrap();
        let wid = w.worker_id.clone();
        assert_eq!(pool.total_count().await, 1);

        pool.kill_worker(&wid).await;
        assert_eq!(pool.total_count().await, 0);
    }

    #[tokio::test]
    async fn test_worker_handle_status() {
        let h = WorkerHandle::new_in_process();
        assert!(h.is_idle());

        h.mark_busy();
        assert!(h.is_busy());
        assert!(!h.is_idle());

        h.mark_dead();
        assert!(h.is_dead());
        assert!(!h.is_busy());
    }
}
