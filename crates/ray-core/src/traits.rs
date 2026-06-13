//! Async trait interfaces for pluggable runtime backends.
//!
//! These traits define the contract between the Ray runtime components.
//! They are implemented by concrete backends (in-memory, gRPC, etc.)
//! and can be swapped for testing.

use crate::error::RayResult;
use crate::id::*;
use crate::resource::Resources;
use async_trait::async_trait;

/// Task execution status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    Pending,
    Scheduled,
    Running,
    Finished,
    Failed,
    Cancelled,
}

/// Actor lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActorState {
    Pending,
    Alive,
    Restarting,
    Dead,
}

/// Specification for a task to be executed.
#[derive(Debug, Clone)]
pub struct TaskSpec {
    pub task_id: TaskId,
    pub job_id: JobId,
    pub function_name: String,
    pub function_payload: Vec<u8>,
    pub return_ids: Vec<ObjectId>,
    pub dependency_ids: Vec<ObjectId>,
    pub required_resources: Resources,
    pub max_retries: i32,
}

/// Specification for creating an actor.
#[derive(Debug, Clone)]
pub struct ActorSpec {
    pub actor_id: ActorId,
    pub job_id: JobId,
    pub class_name: String,
    pub class_payload: Vec<u8>,
    pub constructor_args: Vec<u8>,
    pub required_resources: Resources,
    pub max_restarts: i32,
}

// ──────────────────────────────────────────────
//  Object Store trait
// ──────────────────────────────────────────────

/// Trait for the distributed object store backend.
#[async_trait]
pub trait ObjectStore: Send + Sync {
    /// Store an object with the given ID and data.
    async fn put(&self, object_id: ObjectId, data: Vec<u8>) -> RayResult<()>;

    /// Retrieve an object by ID. Blocks until available or timeout.
    async fn get(&self, object_id: &ObjectId, timeout_ms: i64) -> RayResult<Vec<u8>>;

    /// Delete an object by ID.
    async fn delete(&self, object_id: &ObjectId) -> RayResult<()>;

    /// Check if an object exists locally.
    async fn contains(&self, object_id: &ObjectId) -> RayResult<bool>;

    /// Wait for at least `num_ready` objects to become available.
    async fn wait(
        &self,
        object_ids: &[ObjectId],
        num_ready: i32,
        timeout_ms: i64,
    ) -> RayResult<Vec<bool>>;
}

// ──────────────────────────────────────────────
//  Scheduler trait
// ──────────────────────────────────────────────

/// Trait for the task scheduler backend.
#[async_trait]
pub trait Scheduler: Send + Sync {
    /// Submit a task for scheduling.
    async fn submit_task(&self, task_spec: TaskSpec) -> RayResult<()>;

    /// Cancel a scheduled or running task.
    async fn cancel_task(&self, task_id: &TaskId) -> RayResult<()>;

    /// Get the status of a task.
    async fn get_task_status(&self, task_id: &TaskId) -> RayResult<TaskStatus>;
}

// ──────────────────────────────────────────────
//  GCS (Global Control Store) trait
// ──────────────────────────────────────────────

/// Node information stored in GCS.
#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub node_id: NodeId,
    pub address: String,
    pub port: i32,
    pub total_resources: Resources,
    pub available_resources: Resources,
    pub is_alive: bool,
}

/// Actor information stored in GCS.
#[derive(Debug, Clone)]
pub struct ActorInfo {
    pub actor_id: ActorId,
    pub job_id: JobId,
    pub class_name: String,
    pub state: ActorState,
    pub node_id: NodeId,
    pub num_restarts: i32,
}

/// Job information stored in GCS.
#[derive(Debug, Clone)]
pub struct JobInfo {
    pub job_id: JobId,
    pub driver_ip: String,
    pub start_time_ms: i64,
    pub end_time_ms: i64,
    pub is_dead: bool,
    pub config: std::collections::HashMap<String, String>,
}

/// Resource usage information for a node, stored in GCS.
#[derive(Debug, Clone)]
pub struct ResourceUsageInfo {
    pub node_id: NodeId,
    pub available: Resources,
    pub total: Resources,
    pub timestamp_ms: i64,
}

/// Trait for the Global Control Store backend.
#[async_trait]
pub trait GcsStore: Send + Sync {
    // ── Node operations ──
    async fn register_node(&self, node_info: NodeInfo) -> RayResult<()>;
    async fn unregister_node(&self, node_id: &NodeId) -> RayResult<()>;
    async fn get_all_nodes(&self) -> RayResult<Vec<NodeInfo>>;

    // ── Actor operations ──
    async fn register_actor(&self, actor_spec: ActorSpec) -> RayResult<ActorId>;
    async fn get_actor(&self, actor_id: &ActorId) -> RayResult<Option<ActorInfo>>;
    async fn get_all_actors(&self, job_id: Option<&JobId>) -> RayResult<Vec<ActorInfo>>;
    async fn kill_actor(&self, actor_id: &ActorId) -> RayResult<()>;

    // ── Job operations ──
    async fn add_job(&self, job_info: JobInfo) -> RayResult<()> {
        let _ = job_info;
        Err(crate::error::RayError::Internal("add_job not implemented".into()))
    }
    async fn mark_job_finished(&self, job_id: &JobId) -> RayResult<()> {
        let _ = job_id;
        Err(crate::error::RayError::Internal("mark_job_finished not implemented".into()))
    }
    async fn get_all_jobs(&self) -> RayResult<Vec<JobInfo>> {
        Err(crate::error::RayError::Internal("get_all_jobs not implemented".into()))
    }

    // ── Resource usage operations ──
    async fn report_resource_usage(&self, usage: ResourceUsageInfo) -> RayResult<()> {
        let _ = usage;
        Err(crate::error::RayError::Internal("report_resource_usage not implemented".into()))
    }
    async fn get_all_resource_usage(&self) -> RayResult<Vec<ResourceUsageInfo>> {
        Err(crate::error::RayError::Internal("get_all_resource_usage not implemented".into()))
    }
}
