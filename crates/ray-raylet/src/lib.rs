//! `ray-raylet` — Per-node daemon for task scheduling and worker management.
//!
//! Each node in a Ray cluster runs a Raylet that:
//! - Accepts task submissions from local workers
//! - Performs local resource-aware scheduling
//! - Manages worker process lifecycle
//! - Coordinates object location tracking
//! - Communicates with other Raylets for distributed scheduling
//!
//! This crate provides:
//! - Generated gRPC service stubs (from `raylet.proto`; common types from `ray-core`)
//! - A `RayletServer` hosting the tonic gRPC service
//! - `LocalScheduler` for resource-aware task scheduling on a single node
//! - `ResourceManager` for tracking node-local resource allocation

pub mod executor;
pub mod resource_manager;
pub mod scheduler;
pub mod server;
pub mod worker;

// Include generated protobuf code (service-specific only; common types via ray_core::proto)
#[allow(clippy::all)]
#[allow(unused_imports)]
pub mod proto {
    pub mod raylet {
        include!("ray.raylet.rs");
    }
}

pub use executor::{FunctionRegistryExecutor, LocalExecutor, TaskExecutor};
pub use resource_manager::ResourceManager;
pub use scheduler::LocalScheduler;
pub use server::RayletServer;
pub use worker::{WorkerHandle, WorkerPool};
