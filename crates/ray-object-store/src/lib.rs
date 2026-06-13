//! `ray-object-store` — Distributed immutable object storage (Plasma replacement).
//!
//! Provides:
//! - In-memory object store backed by shared memory (mmap) for zero-copy reads
//! - Object reference counting and garbage collection
//! - Spilling to disk / remote storage when memory is full
//! - Cross-process object sharing via shared memory segments
//! - gRPC service for distributed object operations

pub mod server;
pub mod shared_memory;
pub mod store;

// Include generated protobuf code (service-specific only; common types via ray_core::proto)
#[allow(clippy::all)]
#[allow(unused_imports)]
pub mod proto {
    pub mod object_store {
        include!("ray.object_store.rs");
    }
}

pub use server::ObjectStoreServer;
pub use store::InMemoryObjectStore;
