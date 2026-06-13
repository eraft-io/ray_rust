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
//! - Generated gRPC service stubs (from `raylet.proto` + `common.proto`)
//! - A `RayletServer` hosting the tonic gRPC service
//! - `LocalScheduler` for resource-aware task scheduling on a single node
//! - `ResourceManager` for tracking node-local resource allocation

pub mod resource_manager;
pub mod scheduler;
pub mod server;

// Include generated protobuf code
#[allow(clippy::all)]
#[allow(unused_imports)]
pub mod proto {
    pub mod common {
        include!("ray.common.rs");
    }
    pub mod raylet {
        include!("ray.raylet.rs");
    }
}

pub use resource_manager::ResourceManager;
pub use scheduler::LocalScheduler;
pub use server::RayletServer;
