//! `ray-runtime` — Local single-process runtime for Ray Rust.
//!
//! The `LocalRuntime` wires together all core components (GCS, Scheduler,
//! ObjectStore, Executor, WorkerPool, ResourceManager) into a single process,
//! enabling local task submission, execution, and result retrieval without
//! starting any gRPC servers.
//!
//! This is the entry point for single-node mode and testing.

use ray_core::error::RayResult;
use ray_core::id::*;
use ray_core::resource::Resources;
use ray_core::traits::{ObjectStore, Scheduler, TaskSpec, TaskStatus};
use ray_gcs::InMemoryGcsStore;
use ray_object_store::InMemoryObjectStore;
use ray_raylet::executor::{FunctionRegistryExecutor, TaskExecutor, TaskFn};
use ray_raylet::resource_manager::ResourceManager;
use ray_raylet::scheduler::LocalScheduler;
use ray_raylet::worker::WorkerPool;
use std::sync::Arc;
use tracing::info;

/// Configuration for the local runtime.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// Number of CPU cores available for scheduling.
    pub num_cpus: usize,
    /// Maximum number of concurrent workers.
    pub max_workers: usize,
    /// Maximum memory in bytes for the object store (0 = unlimited).
    pub object_store_memory: usize,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        let num_cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        Self {
            num_cpus,
            max_workers: num_cpus * 2,
            object_store_memory: 0,
        }
    }
}

/// Local single-process Ray runtime.
///
/// Wires together all core components and exposes a high-level API for
/// submitting tasks, registering functions, putting/getting objects,
/// and querying task status.
pub struct LocalRuntime {
    gcs: Arc<InMemoryGcsStore>,
    scheduler: Arc<LocalScheduler>,
    object_store: Arc<InMemoryObjectStore>,
    resource_manager: Arc<ResourceManager>,
    executor: Arc<FunctionRegistryExecutor>,
    worker_pool: Arc<WorkerPool>,
    node_id: NodeId,
}

impl LocalRuntime {
    /// Create and start a new local runtime with the given configuration.
    pub fn new(config: RuntimeConfig) -> RayResult<Self> {
        let node_id = NodeId::new();

        let resources = Resources::new().set("CPU", config.num_cpus as f64);
        let resource_manager = Arc::new(ResourceManager::new(resources));
        let object_store = Arc::new(InMemoryObjectStore::new(config.object_store_memory));
        let executor = Arc::new(FunctionRegistryExecutor::new());
        let worker_pool = Arc::new(WorkerPool::new(config.max_workers));
        let gcs = Arc::new(InMemoryGcsStore::new());

        let executor_dyn: Arc<dyn TaskExecutor> = executor.clone();
        let object_store_dyn: Arc<dyn ObjectStore> = object_store.clone();

        let scheduler = Arc::new(LocalScheduler::new(
            resource_manager.clone(),
            executor_dyn,
            object_store_dyn,
            worker_pool.clone(),
        ));

        info!(
            ?node_id,
            num_cpus = config.num_cpus,
            max_workers = config.max_workers,
            "LocalRuntime started"
        );

        Ok(Self {
            gcs,
            scheduler,
            object_store,
            resource_manager,
            executor,
            worker_pool,
            node_id,
        })
    }

    /// Submit a task for execution.
    pub async fn submit_task(&self, spec: TaskSpec) -> RayResult<()> {
        self.scheduler.submit_task(spec).await
    }

    /// Submit a function call by name with the given payload.
    ///
    /// Creates a `TaskSpec` with a single return ID and submits it.
    /// Returns the `ObjectId` that will hold the result.
    pub async fn submit_fn(
        &self,
        function_name: &str,
        payload: Vec<u8>,
    ) -> RayResult<ObjectId> {
        let return_id = ObjectId::new();
        let spec = TaskSpec {
            task_id: TaskId::new(),
            job_id: JobId::new(),
            function_name: function_name.to_string(),
            function_payload: payload,
            return_ids: vec![return_id.clone()],
            dependency_ids: vec![],
            required_resources: Resources::new().set("CPU", 1.0),
            max_retries: 0,
        };
        self.scheduler.submit_task(spec).await?;
        Ok(return_id)
    }

    /// Get an object by ID, waiting up to `timeout_ms` milliseconds.
    pub async fn get_object(&self, id: &ObjectId, timeout_ms: i64) -> RayResult<Vec<u8>> {
        self.object_store.get(id, timeout_ms).await
    }

    /// Put data into the object store and return a new `ObjectId`.
    pub async fn put_object(&self, data: Vec<u8>) -> RayResult<ObjectId> {
        let id = ObjectId::new();
        self.object_store.put(id.clone(), data).await?;
        Ok(id)
    }

    /// Cancel a submitted or running task.
    pub async fn cancel_task(&self, task_id: &TaskId) -> RayResult<()> {
        self.scheduler.cancel_task(task_id).await
    }

    /// Get the status of a task.
    pub async fn get_task_status(&self, task_id: &TaskId) -> RayResult<TaskStatus> {
        self.scheduler.get_task_status(task_id).await
    }

    /// Register a named function for use by tasks.
    ///
    /// The function receives the task's `function_payload` bytes and
    /// returns result bytes.
    pub async fn register_function(&self, name: impl Into<String>, f: TaskFn) {
        self.executor.register_fn(name, f).await;
    }

    /// Unregister a named function.
    pub async fn unregister_function(&self, name: &str) {
        self.executor.unregister_fn(name).await;
    }

    /// Access the GCS store.
    pub fn gcs(&self) -> &Arc<InMemoryGcsStore> {
        &self.gcs
    }

    /// Access the object store.
    pub fn object_store(&self) -> &Arc<InMemoryObjectStore> {
        &self.object_store
    }

    /// Access the resource manager.
    pub fn resource_manager(&self) -> &Arc<ResourceManager> {
        &self.resource_manager
    }

    /// Access the function registry executor.
    pub fn executor(&self) -> &Arc<FunctionRegistryExecutor> {
        &self.executor
    }

    /// Access the worker pool.
    pub fn worker_pool(&self) -> &Arc<WorkerPool> {
        &self.worker_pool
    }

    /// Get the local node ID.
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Shut down the runtime (consumes self).
    ///
    /// Background tasks will finish naturally when their channels close.
    pub async fn shutdown(self) {
        info!("LocalRuntime shutting down");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_runtime_put_and_get() {
        let runtime = LocalRuntime::new(RuntimeConfig {
            num_cpus: 2,
            max_workers: 4,
            object_store_memory: 0,
        })
        .unwrap();

        let data = vec![1, 2, 3, 4, 5];
        let obj_id = runtime.put_object(data.clone()).await.unwrap();
        let result = runtime.get_object(&obj_id, 1000).await.unwrap();
        assert_eq!(result, data);

        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn test_runtime_submit_fn_and_get() {
        let runtime = LocalRuntime::new(RuntimeConfig {
            num_cpus: 2,
            max_workers: 4,
            object_store_memory: 0,
        })
        .unwrap();

        // Register a function that doubles each byte
        runtime
            .register_function(
                "double",
                Arc::new(|payload: &[u8]| -> RayResult<Vec<u8>> {
                    Ok(payload.iter().flat_map(|b| [*b, *b]).collect())
                }),
            )
            .await;

        let return_id = runtime.submit_fn("double", vec![1, 2, 3]).await.unwrap();

        // Wait for execution
        let result = runtime.get_object(&return_id, 5000).await.unwrap();
        assert_eq!(result, vec![1, 1, 2, 2, 3, 3]);

        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn test_runtime_task_status() {
        let runtime = LocalRuntime::new(RuntimeConfig {
            num_cpus: 2,
            max_workers: 4,
            object_store_memory: 0,
        })
        .unwrap();

        let task_id = TaskId::new();
        let spec = TaskSpec {
            task_id: task_id.clone(),
            job_id: JobId::new(),
            function_name: "noop".to_string(),
            function_payload: vec![],
            return_ids: vec![],
            dependency_ids: vec![],
            required_resources: Resources::new().set("CPU", 1.0),
            max_retries: 0,
        };

        runtime.submit_task(spec).await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        let status = runtime.get_task_status(&task_id).await.unwrap();
        assert_eq!(status, TaskStatus::Running);

        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn test_runtime_cancel_task() {
        let runtime = LocalRuntime::new(RuntimeConfig {
            num_cpus: 1,
            max_workers: 2,
            object_store_memory: 0,
        })
        .unwrap();

        // Submit a task that requires more resources than available
        let task_id = TaskId::new();
        let spec = TaskSpec {
            task_id: task_id.clone(),
            job_id: JobId::new(),
            function_name: "big_task".to_string(),
            function_payload: vec![],
            return_ids: vec![],
            dependency_ids: vec![],
            required_resources: Resources::new().set("CPU", 100.0),
            max_retries: 0,
        };

        runtime.submit_task(spec).await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let status = runtime.get_task_status(&task_id).await.unwrap();
        assert_eq!(status, TaskStatus::Pending);

        runtime.cancel_task(&task_id).await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let status = runtime.get_task_status(&task_id).await.unwrap();
        assert_eq!(status, TaskStatus::Cancelled);

        runtime.shutdown().await;
    }
}
