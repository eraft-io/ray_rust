//! Local runtime demo — run with:
//!
//! ```bash
//! cargo run -p ray-runtime --example local_demo
//! ```

use ray_core::error::RayResult;
use ray_runtime::{LocalRuntime, RuntimeConfig};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing (optional, for log output)
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    println!("=== Ray Rust Local Runtime Demo ===\n");

    // 1. Create a local runtime (4 CPUs, 8 workers)
    println!("[1] Creating LocalRuntime...");
    let runtime = LocalRuntime::new(RuntimeConfig {
        num_cpus: 4,
        max_workers: 8,
        object_store_memory: 0, // 0 = unlimited
    })?;
    println!("    Runtime created with node_id: {:?}\n", runtime.node_id());

    // 2. Register a function: doubles each byte in the payload
    println!("[2] Registering 'double_bytes' function...");
    runtime
        .register_function(
            "double_bytes",
            Arc::new(|payload: &[u8]| -> RayResult<Vec<u8>> {
                Ok(payload.iter().map(|b| b.wrapping_mul(2)).collect())
            }),
        )
        .await;
    println!("    Function registered.\n");

    // 3. Put an object into the object store
    println!("[3] Putting object into object store...");
    let input = vec![10, 20, 30];
    let obj_id = runtime.put_object(input.clone()).await?;
    let data = runtime.get_object(&obj_id, 1000).await?;
    println!("    Input:  {:?}", input);
    println!("    Output: {:?}", data);
    assert_eq!(data, input);
    println!();

    // 4. Submit a task and get the result
    println!("[4] Submitting task 'double_bytes' with payload [1, 2, 3]...");
    let result_id = runtime.submit_fn("double_bytes", vec![1, 2, 3]).await?;
    let result = runtime.get_object(&result_id, 5000).await?;
    println!("    Result: {:?}", result);
    assert_eq!(result, vec![2, 4, 6]);
    println!();

    // 5. Submit multiple tasks in parallel
    println!("[5] Submitting 4 parallel tasks...");
    let mut result_ids = Vec::new();
    for i in 0u8..4 {
        let rid = runtime
            .submit_fn("double_bytes", vec![i, i + 1, i + 2])
            .await?;
        result_ids.push((rid, i));
    }

    for (rid, i) in result_ids {
        let result = runtime.get_object(&rid, 5000).await?;
        let expected: Vec<u8> = vec![i, i + 1, i + 2]
            .iter()
            .map(|b| b.wrapping_mul(2))
            .collect();
        println!("    Task {}: input=[{}, {}, {}] -> {:?}",
            i, i, i + 1, i + 2, result);
        assert_eq!(result, expected);
    }
    println!();

    // 6. Register another function: computes sum of bytes
    println!("[6] Registering 'sum_bytes' function...");
    runtime
        .register_function(
            "sum_bytes",
            Arc::new(|payload: &[u8]| -> RayResult<Vec<u8>> {
                let sum: u32 = payload.iter().map(|b| *b as u32).sum();
                Ok(sum.to_le_bytes().to_vec())
            }),
        )
        .await;

    let result_id = runtime.submit_fn("sum_bytes", vec![10, 20, 30]).await?;
    let result = runtime.get_object(&result_id, 5000).await?;
    let sum = u32::from_le_bytes([result[0], result[1], result[2], result[3]]);
    println!("    sum([10, 20, 30]) = {}", sum);
    assert_eq!(sum, 60);
    println!();

    // 7. Shutdown
    println!("[7] Shutting down runtime...");
    runtime.shutdown().await;

    println!("\n=== Demo complete! ===");
    Ok(())
}
