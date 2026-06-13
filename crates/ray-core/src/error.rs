//! Error types for the Ray runtime.

use thiserror::Error;

/// Top-level error type for all Ray operations.
#[derive(Debug, Error)]
pub enum RayError {
    #[error("Object not found: {0}")]
    ObjectNotFound(String),

    #[error("Object already exists: {0}")]
    ObjectAlreadyExists(String),

    #[error("Task failed: {0}")]
    TaskFailed(String),

    #[error("Task not found: {0}")]
    TaskNotFound(String),

    #[error("Actor not found: {0}")]
    ActorNotFound(String),

    #[error("Actor died: {0}")]
    ActorDied(String),

    #[error("Node not found: {0}")]
    NodeNotFound(String),

    #[error("Insufficient resources: {0}")]
    InsufficientResources(String),

    #[error("Scheduling failed: {0}")]
    SchedulingFailed(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Deserialization error: {0}")]
    DeserializationError(String),

    #[error("Shared memory error: {0}")]
    SharedMemoryError(String),

    #[error("Timeout: {0}")]
    Timeout(String),

    #[error("Transport error: {0}")]
    Transport(String),

    #[error("GCS error: {0}")]
    GcsError(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Convenience Result type for Ray operations.
pub type RayResult<T> = Result<T, RayError>;
