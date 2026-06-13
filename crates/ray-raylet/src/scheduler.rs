//! Local scheduler for the Raylet.
//!
//! The `LocalScheduler` is responsible for:
//! - Maintaining a queue of pending tasks
//! - Matching tasks to available resources (FIFO + resource-aware)
//! - Dispatching tasks to the execution engine
//! - Writing results to the object store
//! - Handling task cancellation and status queries

use crate::executor::TaskExecutor;
use crate::resource_manager::ResourceManager;
use crate::worker::WorkerPool;
use async_trait::async_trait;
use ray_core::error::{RayError, RayResult};
use ray_core::id::TaskId;
use ray_core::traits::{ObjectStore, Scheduler, TaskSpec, TaskStatus};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Message sent to the scheduler event loop.
enum SchedulerCommand {
    SubmitTask(TaskSpec),
    CancelTask(TaskId),
    GetStatus(TaskId, tokio::sync::oneshot::Sender<RayResult<TaskStatus>>),
}

/// Local scheduler that runs inside the Raylet process.
///
/// Uses a single-threaded event loop to avoid locking overhead,
/// communicating via an mpsc channel.
pub struct LocalScheduler {
    cmd_tx: mpsc::Sender<SchedulerCommand>,
}

impl LocalScheduler {
    /// Create and start a new local scheduler.
    ///
    /// The scheduler runs its event loop as a background tokio task.
    pub fn new(
        resource_manager: Arc<ResourceManager>,
        executor: Arc<dyn TaskExecutor>,
        object_store: Arc<dyn ObjectStore>,
        worker_pool: Arc<WorkerPool>,
    ) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::channel(4096);

        tokio::spawn(Self::event_loop(
            resource_manager,
            executor,
            object_store,
            worker_pool,
            cmd_rx,
        ));

        Self { cmd_tx }
    }

    /// The main scheduler event loop.
    ///
    /// Processes commands sequentially and dispatches tasks when resources
    /// become available.
    async fn event_loop(
        resource_manager: Arc<ResourceManager>,
        executor: Arc<dyn TaskExecutor>,
        object_store: Arc<dyn ObjectStore>,
        worker_pool: Arc<WorkerPool>,
        mut cmd_rx: mpsc::Receiver<SchedulerCommand>,
    ) {
        let mut task_statuses: HashMap<TaskId, TaskStatus> = HashMap::new();
        let mut pending_queue: Vec<TaskSpec> = Vec::new();

        info!("LocalScheduler event loop started");

        while let Some(cmd) = cmd_rx.recv().await {
            match cmd {
                SchedulerCommand::SubmitTask(spec) => {
                    let task_id = spec.task_id.clone();
                    debug!(?task_id, "Task submitted to local scheduler");

                    // Try to allocate resources immediately
                    if resource_manager
                        .try_allocate(&spec.required_resources)
                        .unwrap_or(false)
                    {
                        task_statuses.insert(task_id.clone(), TaskStatus::Running);
                        // Dispatch to executor in background
                        Self::dispatch_task(
                            spec,
                            executor.clone(),
                            object_store.clone(),
                            worker_pool.clone(),
                            resource_manager.clone(),
                        );
                        debug!(?task_id, "Task dispatched (resources allocated)");
                    } else {
                        task_statuses.insert(task_id.clone(), TaskStatus::Pending);
                        pending_queue.push(spec);
                        debug!(?task_id, "Task queued (insufficient resources)");
                    }
                }

                SchedulerCommand::CancelTask(task_id) => {
                    debug!(?task_id, "Cancelling task");
                    if let Some(status) = task_statuses.get_mut(&task_id) {
                        *status = TaskStatus::Cancelled;
                    }
                    pending_queue.retain(|s| s.task_id != task_id);
                }

                SchedulerCommand::GetStatus(task_id, reply_tx) => {
                    let status = task_statuses
                        .get(&task_id)
                        .copied()
                        .ok_or_else(|| RayError::TaskNotFound(format!("{:?}", task_id)));
                    let _ = reply_tx.send(status);
                }
            }

            // Try to drain the pending queue after each command
            let mut still_pending = Vec::new();
            for spec in pending_queue.drain(..) {
                if resource_manager
                    .try_allocate(&spec.required_resources)
                    .unwrap_or(false)
                {
                    let task_id = spec.task_id.clone();
                    task_statuses.insert(task_id.clone(), TaskStatus::Running);
                    Self::dispatch_task(
                        spec,
                        executor.clone(),
                        object_store.clone(),
                        worker_pool.clone(),
                        resource_manager.clone(),
                    );
                    debug!(?task_id, "Pending task now running");
                } else {
                    still_pending.push(spec);
                }
            }
            pending_queue = still_pending;
        }

        warn!("LocalScheduler event loop exited");
    }

    /// Dispatch a task for execution in a background tokio task.
    ///
    /// The task runs through: executor.execute() -> write results to object store
    /// -> release resources + return worker.
    fn dispatch_task(
        spec: TaskSpec,
        executor: Arc<dyn TaskExecutor>,
        object_store: Arc<dyn ObjectStore>,
        worker_pool: Arc<WorkerPool>,
        resource_manager: Arc<ResourceManager>,
    ) {
        let task_id = spec.task_id.clone();
        let return_ids = spec.return_ids.clone();
        let required_resources = spec.required_resources.clone();

        tokio::spawn(async move {
            // Get a worker from the pool
            let worker = worker_pool.get_worker().await;

            let result = executor.execute(&spec).await;

            match result {
                Ok(data) => {
                    // Write result to object store using return_ids
                    if let Some(return_id) = return_ids.first() {
                        if let Err(e) = object_store.put(return_id.clone(), data).await {
                            error!(?task_id, ?return_id, error = %e, "Failed to write task result to object store");
                        } else {
                            debug!(?task_id, ?return_id, "Task result written to object store");
                        }
                    }
                }
                Err(e) => {
                    error!(?task_id, error = %e, "Task execution failed");
                }
            }

            // Return worker and release resources
            if let Some(w) = worker {
                worker_pool.return_worker(&w).await;
            }
            resource_manager.release(&required_resources);

            debug!(?task_id, "Task completed, resources released");
        });
    }
}

#[async_trait]
impl Scheduler for LocalScheduler {
    async fn submit_task(&self, task_spec: TaskSpec) -> RayResult<()> {
        self.cmd_tx
            .send(SchedulerCommand::SubmitTask(task_spec))
            .await
            .map_err(|_| RayError::Internal("Scheduler channel closed".to_string()))
    }

    async fn cancel_task(&self, task_id: &TaskId) -> RayResult<()> {
        self.cmd_tx
            .send(SchedulerCommand::CancelTask(task_id.clone()))
            .await
            .map_err(|_| RayError::Internal("Scheduler channel closed".to_string()))
    }

    async fn get_task_status(&self, task_id: &TaskId) -> RayResult<TaskStatus> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.cmd_tx
            .send(SchedulerCommand::GetStatus(task_id.clone(), tx))
            .await
            .map_err(|_| RayError::Internal("Scheduler channel closed".to_string()))?;
        rx.await
            .map_err(|_| RayError::Internal("Scheduler reply channel dropped".to_string()))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::LocalExecutor;
    use crate::resource_manager::ResourceManager;
    use crate::worker::WorkerPool;
    use ray_core::id::*;
    use ray_core::resource::Resources;
    use ray_core::traits::ObjectStore;
    use ray_object_store::InMemoryObjectStore;

    fn make_scheduler() -> (LocalScheduler, Arc<InMemoryObjectStore>) {
        let rm = Arc::new(ResourceManager::new(Resources::new().set("CPU", 4.0)));
        let executor: Arc<dyn TaskExecutor> = Arc::new(LocalExecutor);
        let store: Arc<InMemoryObjectStore> = Arc::new(InMemoryObjectStore::new(0));
        let store_dyn: Arc<dyn ObjectStore> = store.clone();
        let pool = Arc::new(WorkerPool::new(4));
        let scheduler = LocalScheduler::new(rm, executor, store_dyn, pool);
        (scheduler, store)
    }

    #[tokio::test]
    async fn test_submit_and_status() {
        let (scheduler, _store) = make_scheduler();

        let task_id = TaskId::new();
        let return_id = ObjectId::new();
        let spec = TaskSpec {
            task_id: task_id.clone(),
            job_id: JobId::new(),
            function_name: "test_func".to_string(),
            function_payload: vec![42, 43, 44],
            return_ids: vec![return_id.clone()],
            dependency_ids: vec![],
            required_resources: Resources::new().set("CPU", 1.0),
            max_retries: 0,
        };

        scheduler.submit_task(spec).await.unwrap();

        // Give the event loop + executor time to process
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        let status = scheduler.get_task_status(&task_id).await.unwrap();
        assert_eq!(status, TaskStatus::Running);
    }

    #[tokio::test]
    async fn test_task_writes_result_to_store() {
        let (scheduler, store) = make_scheduler();

        let return_id = ObjectId::new();
        let payload = vec![10, 20, 30];
        let spec = TaskSpec {
            task_id: TaskId::new(),
            job_id: JobId::new(),
            function_name: "echo".to_string(),
            function_payload: payload.clone(),
            return_ids: vec![return_id.clone()],
            dependency_ids: vec![],
            required_resources: Resources::new().set("CPU", 1.0),
            max_retries: 0,
        };

        scheduler.submit_task(spec).await.unwrap();

        // Wait for execution + store write
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let result = store.get(&return_id, 1000).await.unwrap();
        assert_eq!(result, payload);
    }

    #[tokio::test]
    async fn test_cancel_task() {
        let (scheduler, _store) = make_scheduler();

        let task_id = TaskId::new();
        let spec = TaskSpec {
            task_id: task_id.clone(),
            job_id: JobId::new(),
            function_name: "test".to_string(),
            function_payload: vec![],
            return_ids: vec![],
            dependency_ids: vec![],
            // Request more CPU than available to keep it pending
            required_resources: Resources::new().set("CPU", 100.0),
            max_retries: 0,
        };

        scheduler.submit_task(spec).await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let status = scheduler.get_task_status(&task_id).await.unwrap();
        assert_eq!(status, TaskStatus::Pending);

        scheduler.cancel_task(&task_id).await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let status = scheduler.get_task_status(&task_id).await.unwrap();
        assert_eq!(status, TaskStatus::Cancelled);
    }
}
