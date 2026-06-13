//! `ray-core` — Shared types, traits, and error definitions for the Ray Rust runtime.
//!
//! This crate provides the foundational abstractions used by all other Ray crates:
//! - Core identifier types (TaskId, ActorId, ObjectId, NodeId, etc.)
//! - Serialization traits for zero-copy and cross-language data exchange
//! - Error types used throughout the system
//! - Async traits for pluggable storage, scheduling, and communication backends
//! - Proto ↔ Core bidirectional type conversions

pub mod error;
pub mod id;
pub mod proto_conv;
pub mod resource;
pub mod serialize;
pub mod traits;

pub use error::{RayError, RayResult};
pub use id::*;
pub use proto_conv::proto;
pub use resource::Resources;
