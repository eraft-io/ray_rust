//! End-to-end integration tests for the local Ray runtime.
//!
//! These tests exercise the complete pipeline: task submission → scheduling
//! → execution → result storage → retrieval, all within a single process.

use ray_core::error::RayResult;
use ray_core::id::*;
use ray_core::resource::Resources;
use ray_core::traits::{GcsStore, JobInfo, TaskSpec, TaskStatus};
use ray_runtime::{LocalRuntime, RuntimeConfig};
use std::sync::Arc;

fn make_runtime() -> LocalRuntime {
    LocalRuntime::new(RuntimeConfig {
        num_cpus: 4,
        max_workers: 8,
        object_store_memory: 0,
    })
    .unwrap()
}

// ── Test: put and get object ──

#[tokio::test]
async fn test_local_put_get_object() {
    let runtime = make_runtime();

    let data = vec![10, 20, 30, 40, 50];
    let obj_id = runtime.put_object(data.clone()).await.unwrap();

    let result = runtime.get_object(&obj_id, 1000).await.unwrap();
    assert_eq!(result, data);

    runtime.shutdown().await;
}

// ── Test: full task lifecycle (put → submit → execute → get) ──

#[tokio::test]
async fn test_local_task_lifecycle() {
    let runtime = make_runtime();

    // 1. Register a function that appends [0xFF] to the payload
    runtime
        .register_function(
            "append_marker",
            Arc::new(|payload: &[u8]| -> RayResult<Vec<u8>> {
                let mut result = payload.to_vec();
                result.push(0xFF);
                Ok(result)
            }),
        )
        .await;

    // 2. Put an input object
    let input_data = vec![1, 2, 3];
    let input_id = runtime.put_object(input_data.clone()).await.unwrap();

    // 3. Submit a task that depends on the input object
    let return_id = runtime
        .submit_fn("append_marker", input_data.clone())
        .await
        .unwrap();

    // 4. Get the result (wait up to 5 seconds)
    let result = runtime.get_object(&return_id, 5000).await.unwrap();
    assert_eq!(result, vec![1, 2, 3, 0xFF]);

    // 5. Verify the input object is still accessible
    let still_there = runtime.get_object(&input_id, 100).await.unwrap();
    assert_eq!(still_there, input_data);

    runtime.shutdown().await;
}

// ── Test: multiple tasks in parallel ──

#[tokio::test]
async fn test_local_parallel_tasks() {
    let runtime = make_runtime();

    // Register a function that sums all bytes
    runtime
        .register_function(
            "sum_bytes",
            Arc::new(|payload: &[u8]| -> RayResult<Vec<u8>> {
                let sum: u32 = payload.iter().map(|b| *b as u32).sum();
                Ok(sum.to_le_bytes().to_vec())
            }),
        )
        .await;

    // Submit 4 tasks in parallel
    let mut return_ids = Vec::new();
    for i in 0u8..4 {
        let payload = vec![i; 10]; // e.g. [0,0,...,0], [1,1,...,1], etc.
        let rid = runtime.submit_fn("sum_bytes", payload).await.unwrap();
        return_ids.push((rid, i as u32 * 10));
    }

    // Collect all results
    for (rid, expected_sum) in return_ids {
        let result = runtime.get_object(&rid, 5000).await.unwrap();
        let sum = u32::from_le_bytes([result[0], result[1], result[2], result[3]]);
        assert_eq!(sum, expected_sum);
    }

    runtime.shutdown().await;
}

// ── Test: task cancellation ──

#[tokio::test]
async fn test_local_task_cancel() {
    let runtime = make_runtime();

    let task_id = TaskId::new();
    let spec = TaskSpec {
        task_id: task_id.clone(),
        job_id: JobId::new(),
        function_name: "slow_fn".to_string(),
        function_payload: vec![],
        return_ids: vec![],
        dependency_ids: vec![],
        // Request more CPU than available so it stays pending
        required_resources: Resources::new().set("CPU", 100.0),
        max_retries: 0,
    };

    runtime.submit_task(spec).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    // Should be pending (not enough resources)
    let status = runtime.get_task_status(&task_id).await.unwrap();
    assert_eq!(status, TaskStatus::Pending);

    // Cancel it
    runtime.cancel_task(&task_id).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    let status = runtime.get_task_status(&task_id).await.unwrap();
    assert_eq!(status, TaskStatus::Cancelled);

    runtime.shutdown().await;
}

// ── Test: function execution failure ──

#[tokio::test]
async fn test_local_function_failure() {
    let runtime = make_runtime();

    // Register a function that always fails
    runtime
        .register_function(
            "always_fail",
            Arc::new(|_payload: &[u8]| -> RayResult<Vec<u8>> {
                Err(ray_core::error::RayError::TaskFailed(
                    "Intentional failure".to_string(),
                ))
            }),
        )
        .await;

    let return_id = runtime.submit_fn("always_fail", vec![]).await.unwrap();

    // The task will fail, so the object will never be created.
    // get_object should timeout.
    let result = runtime.get_object(&return_id, 200).await;
    assert!(result.is_err());

    runtime.shutdown().await;
}

// ── Test: GCS job management ──

#[tokio::test]
async fn test_gcs_job_management() {
    let runtime = make_runtime();

    let job_id = JobId::new();
    let job_info = JobInfo {
        job_id: job_id.clone(),
        driver_ip: "127.0.0.1".to_string(),
        start_time_ms: 1000,
        end_time_ms: 0,
        is_dead: false,
        config: std::collections::HashMap::new(),
    };

    // Add job
    runtime.gcs().add_job(job_info).await.unwrap();

    // Get all jobs
    let jobs = runtime.gcs().get_all_jobs().await.unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].job_id, job_id);
    assert!(!jobs[0].is_dead);

    // Mark job as finished
    runtime.gcs().mark_job_finished(&job_id).await.unwrap();

    let jobs = runtime.gcs().get_all_jobs().await.unwrap();
    assert_eq!(jobs.len(), 1);
    assert!(jobs[0].is_dead);
    assert!(jobs[0].end_time_ms > 0);

    runtime.shutdown().await;
}

// ── Test: GCS resource usage tracking ──

#[tokio::test]
async fn test_gcs_resource_usage() {
    let runtime = make_runtime();

    let node_id = NodeId::new();
    let usage = ray_core::traits::ResourceUsageInfo {
        node_id: node_id.clone(),
        available: Resources::new().set("CPU", 3.0),
        total: Resources::new().set("CPU", 4.0),
        timestamp_ms: 12345,
    };

    runtime.gcs().report_resource_usage(usage).await.unwrap();

    let all_usage = runtime.gcs().get_all_resource_usage().await.unwrap();
    assert_eq!(all_usage.len(), 1);
    assert_eq!(all_usage[0].node_id, node_id);
    assert_eq!(all_usage[0].available.get("CPU"), 3.0);
    assert_eq!(all_usage[0].total.get("CPU"), 4.0);

    runtime.shutdown().await;
}

// ── Test: object store contains check ──

#[tokio::test]
async fn test_object_store_contains() {
    use ray_core::traits::ObjectStore;

    let runtime = make_runtime();

    let obj_id = ObjectId::new();
    assert!(!runtime.object_store().contains(&obj_id).await.unwrap());

    runtime.put_object(vec![42]).await.unwrap();
    // Note: put_object creates a new ObjectId, so we can't check the above one
    // Let's test with the actual put flow
    let id2 = runtime.put_object(vec![1, 2]).await.unwrap();
    assert!(runtime.object_store().contains(&id2).await.unwrap());

    runtime.shutdown().await;
}

// ── Test: task status transitions ──

#[tokio::test]
async fn test_task_status_flow() {
    let runtime = make_runtime();

    // Register a function that sleeps briefly
    runtime
        .register_function(
            "quick",
            Arc::new(|payload: &[u8]| -> RayResult<Vec<u8>> {
                Ok(payload.to_vec())
            }),
        )
        .await;

    let task_id = TaskId::new();
    let return_id = ObjectId::new();
    let spec = TaskSpec {
        task_id: task_id.clone(),
        job_id: JobId::new(),
        function_name: "quick".to_string(),
        function_payload: vec![99],
        return_ids: vec![return_id.clone()],
        dependency_ids: vec![],
        required_resources: Resources::new().set("CPU", 1.0),
        max_retries: 0,
    };

    runtime.submit_task(spec).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Task should be running (or already finished)
    let status = runtime.get_task_status(&task_id).await.unwrap();
    assert!(status == TaskStatus::Running || status == TaskStatus::Finished);

    // Wait for result
    let result = runtime.get_object(&return_id, 5000).await.unwrap();
    assert_eq!(result, vec![99]);

    runtime.shutdown().await;
}
