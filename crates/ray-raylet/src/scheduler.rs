//! Local scheduler for the Raylet.
//!
//! The `LocalScheduler` is responsible for:
//! - Maintaining a queue of pending tasks
//! - Matching tasks to available resources (FIFO + resource-aware)
//! - Dispatching tasks to local workers
//! - Handling task cancellation and status queries

use crate::resource_manager::ResourceManager;
use async_trait::async_trait;
use ray_core::error::{RayError, RayResult};
use ray_core::id::TaskId;
use ray_core::traits::{Scheduler, TaskSpec, TaskStatus};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

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
    pub fn new(resource_manager: Arc<ResourceManager>) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::channel(4096);

        tokio::spawn(Self::event_loop(resource_manager, cmd_rx));

        Self { cmd_tx }
    }

    /// The main scheduler event loop.
    ///
    /// Processes commands sequentially and dispatches tasks when resources
    /// become available.
    async fn event_loop(
        resource_manager: Arc<ResourceManager>,
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
                        // TODO: Dispatch to a worker process
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
                    debug!(?task_id, "Pending task now running");
                } else {
                    still_pending.push(spec);
                }
            }
            pending_queue = still_pending;
        }

        warn!("LocalScheduler event loop exited");
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
    use ray_core::id::*;
    use ray_core::Resources;

    #[tokio::test]
    async fn test_submit_and_status() {
        let rm = Arc::new(ResourceManager::new(Resources::new().set("CPU", 4.0)));
        let scheduler = LocalScheduler::new(rm);

        let task_id = TaskId::new();
        let spec = TaskSpec {
            task_id: task_id.clone(),
            job_id: JobId::new(),
            function_name: "test_func".to_string(),
            function_payload: vec![],
            return_ids: vec![],
            dependency_ids: vec![],
            required_resources: Resources::new().set("CPU", 1.0),
            max_retries: 0,
        };

        scheduler.submit_task(spec).await.unwrap();

        // Give the event loop time to process
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let status = scheduler.get_task_status(&task_id).await.unwrap();
        assert_eq!(status, TaskStatus::Running);
    }
}
