//! `ray-gcs` — Global Control Store implementation.
//!
//! The GCS is the central metadata store for a Ray cluster. It manages:
//! - Node membership and health (via heartbeat)
//! - Actor registry and lifecycle
//! - Job registration and status
//! - Global resource usage tracking
//!
//! This crate provides:
//! - Generated gRPC service stubs (from `gcs.proto` + `common.proto`)
//! - An in-memory `GcsStore` implementation
//! - A `GcsServer` that hosts the tonic gRPC service

pub mod server;
pub mod store;

// Include generated protobuf code
#[allow(clippy::all)]
#[allow(unused_imports)]
pub mod proto {
    pub mod common {
        include!("ray.common.rs");
    }
    pub mod gcs {
        include!("ray.gcs.rs");
    }
}

pub use server::GcsServer;
pub use store::InMemoryGcsStore;
