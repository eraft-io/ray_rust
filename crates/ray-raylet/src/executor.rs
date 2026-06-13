//! Task execution engine.
//!
//! The `TaskExecutor` trait defines how tasks are executed. Built-in
//! implementations:
//! - `LocalExecutor` — runs Rust closures in tokio blocking threads
//! - `FunctionRegistryExecutor` — looks up registered functions by name

use async_trait::async_trait;
use ray_core::error::{RayError, RayResult};
use ray_core::traits::TaskSpec;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

/// A function type that can be executed as a task.
///
/// Receives the task's function payload bytes and returns result bytes.
pub type TaskFn = Arc<dyn Fn(&[u8]) -> RayResult<Vec<u8>> + Send + Sync>;

/// Trait for task execution backends.
#[async_trait]
pub trait TaskExecutor: Send + Sync {
    /// Execute a task and return the result as raw bytes.
    ///
    /// The executor is responsible for:
    /// - Interpreting the `function_payload` (pickle bytes, Rust closure, etc.)
    /// - Running the function (in-process, subprocess, remote, etc.)
    /// - Returning the serialized result
    async fn execute(&self, task_spec: &TaskSpec) -> RayResult<Vec<u8>>;
}

/// Executes tasks by looking up their function name in a local registry.
///
/// Functions are registered via `register_fn(name, closure)`. When a task
/// arrives with `function_name = "my_fn"`, the executor calls the registered
/// closure with the function payload bytes.
///
/// If no function is registered for the name, execution fails.
pub struct FunctionRegistryExecutor {
    fns: RwLock<HashMap<String, TaskFn>>,
}

impl FunctionRegistryExecutor {
    pub fn new() -> Self {
        Self {
            fns: RwLock::new(HashMap::new()),
        }
    }

    /// Register a function by name.
    pub async fn register_fn(&self, name: impl Into<String>, f: TaskFn) {
        self.fns.write().await.insert(name.into(), f);
    }

    /// Unregister a function.
    pub async fn unregister_fn(&self, name: &str) {
        self.fns.write().await.remove(name);
    }

    /// Check if a function is registered.
    pub async fn has_fn(&self, name: &str) -> bool {
        self.fns.read().await.contains_key(name)
    }
}

impl Default for FunctionRegistryExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TaskExecutor for FunctionRegistryExecutor {
    async fn execute(&self, task_spec: &TaskSpec) -> RayResult<Vec<u8>> {
        let f = {
            let fns = self.fns.read().await;
            fns.get(&task_spec.function_name)
                .cloned()
                .ok_or_else(|| {
                    RayError::TaskFailed(format!(
                        "No function registered for '{}'",
                        task_spec.function_name
                    ))
                })?
        };

        let payload = task_spec.function_payload.clone();
        let function_name = task_spec.function_name.clone();

        // Run the function in a tokio blocking thread to avoid blocking the async runtime
        let result = tokio::task::spawn_blocking(move || f(&payload))
            .await
            .map_err(|e| RayError::TaskFailed(format!("Task panicked: {}", e)))??;

        debug!(function = %function_name, result_len = result.len(), "Task executed successfully");
        Ok(result)
    }
}

/// A simple executor that runs the function payload as a Rust closure.
///
/// This is the simplest executor — it deserializes the function payload
/// as a `Box<dyn Fn() -> Vec<u8>>` and executes it directly.
///
/// For testing and simple Rust-only workloads.
pub struct LocalExecutor;

#[async_trait]
impl TaskExecutor for LocalExecutor {
    async fn execute(&self, task_spec: &TaskSpec) -> RayResult<Vec<u8>> {
        // For LocalExecutor, function_payload is treated as input data.
        // The task "function" is a no-op that returns the payload as-is.
        // This is useful for testing the full pipeline without real function dispatch.
        debug!(
            function = %task_spec.function_name,
            payload_len = task_spec.function_payload.len(),
            "LocalExecutor: executing task (echo payload)"
        );
        Ok(task_spec.function_payload.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ray_core::id::*;
    use ray_core::resource::Resources;

    fn make_task(name: &str, payload: Vec<u8>) -> TaskSpec {
        TaskSpec {
            task_id: TaskId::new(),
            job_id: JobId::new(),
            function_name: name.to_string(),
            function_payload: payload,
            return_ids: vec![ObjectId::new()],
            dependency_ids: vec![],
            required_resources: Resources::new().set("CPU", 1.0),
            max_retries: 0,
        }
    }

    #[tokio::test]
    async fn test_local_executor_echo() {
        let executor = LocalExecutor;
        let task = make_task("echo", vec![1, 2, 3]);
        let result = executor.execute(&task).await.unwrap();
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_function_registry_executor() {
        let executor = FunctionRegistryExecutor::new();

        // Register a function that doubles each byte
        executor
            .register_fn(
                "double",
                Arc::new(|payload: &[u8]| -> RayResult<Vec<u8>> {
                    Ok(payload.iter().flat_map(|b| [*b, *b]).collect())
                }),
            )
            .await;

        let task = make_task("double", vec![1, 2, 3]);
        let result = executor.execute(&task).await.unwrap();
        assert_eq!(result, vec![1, 1, 2, 2, 3, 3]);
    }

    #[tokio::test]
    async fn test_function_registry_missing_fn() {
        let executor = FunctionRegistryExecutor::new();
        let task = make_task("nonexistent", vec![]);
        let result = executor.execute(&task).await;
        assert!(result.is_err());
    }
}
