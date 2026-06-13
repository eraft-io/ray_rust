//! Global scheduler implementation.
//!
//! The `GlobalScheduler` maintains a view of the cluster and makes
//! placement decisions for tasks that overflow local node capacity.

use crate::policy::{SchedulingPolicy, SpreadPolicy};
use ray_core::error::{RayError, RayResult};
use ray_core::id::NodeId;
use ray_core::resource::Resources;
use ray_core::traits::TaskSpec;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tracing::{info, warn};

/// View of a single node's resources, maintained by the global scheduler.
#[derive(Debug, Clone)]
pub struct NodeView {
    pub node_id: NodeId,
    pub address: String,
    pub port: i32,
    pub available: Resources,
    pub total: Resources,
    pub is_alive: bool,
}

/// The global scheduler decides which node should execute a given task.
///
/// It receives resource reports from Raylets (via GCS heartbeats) and
/// uses a pluggable `SchedulingPolicy` to make placement decisions.
pub struct GlobalScheduler {
    /// Current view of all nodes in the cluster.
    nodes: RwLock<HashMap<NodeId, NodeView>>,
    /// The scheduling policy to use for placement decisions.
    policy: Arc<dyn SchedulingPolicy>,
}

impl GlobalScheduler {
    /// Create a new global scheduler with the given policy.
    pub fn new(policy: Arc<dyn SchedulingPolicy>) -> Self {
        info!("GlobalScheduler initialized");
        Self {
            nodes: RwLock::new(HashMap::new()),
            policy,
        }
    }

    /// Create with the default spread policy.
    pub fn with_default_policy() -> Self {
        Self::new(Arc::new(SpreadPolicy))
    }

    /// Update the view of a node (called when a heartbeat is received).
    pub fn update_node(&self, view: NodeView) {
        let node_id = view.node_id.clone();
        self.nodes.write().unwrap().insert(node_id, view);
    }

    /// Remove a node from the view (called when a node dies or is removed).
    pub fn remove_node(&self, node_id: &NodeId) {
        warn!(?node_id, "Node removed from global scheduler");
        self.nodes.write().unwrap().remove(node_id);
    }

    /// Schedule a task: pick the best node based on the policy.
    pub fn schedule_task(&self, task: &TaskSpec) -> RayResult<NodeId> {
        let nodes = self.nodes.read().unwrap();
        let alive_nodes: Vec<&NodeView> = nodes.values().filter(|n| n.is_alive).collect();

        if alive_nodes.is_empty() {
            return Err(RayError::SchedulingFailed(
                "No alive nodes in cluster".to_string(),
            ));
        }

        self.policy
            .select_node(&alive_nodes, &task.required_resources)
            .map(|v| v.node_id.clone())
    }

    /// Get a snapshot of all node views.
    pub fn get_all_nodes(&self) -> Vec<NodeView> {
        self.nodes.read().unwrap().values().cloned().collect()
    }

    /// Get the number of alive nodes.
    pub fn alive_node_count(&self) -> usize {
        self.nodes.read().unwrap().values().filter(|n| n.is_alive).count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ray_core::id::TaskId;

    fn make_node(id: NodeId, cpu: f64) -> NodeView {
        NodeView {
            node_id: id,
            address: "127.0.0.1".to_string(),
            port: 6379,
            available: Resources::new().set("CPU", cpu),
            total: Resources::new().set("CPU", cpu),
            is_alive: true,
        }
    }

    #[test]
    fn test_schedule_task() {
        let scheduler = GlobalScheduler::with_default_policy();

        let n1 = make_node(NodeId::new(), 4.0);
        let n2 = make_node(NodeId::new(), 8.0);
        scheduler.update_node(n1);
        scheduler.update_node(n2);

        let task = TaskSpec {
            task_id: TaskId::new(),
            job_id: ray_core::id::JobId::new(),
            function_name: "test".to_string(),
            function_payload: vec![],
            return_ids: vec![],
            dependency_ids: vec![],
            required_resources: Resources::new().set("CPU", 1.0),
            max_retries: 0,
        };

        let node_id = scheduler.schedule_task(&task).unwrap();
        assert!(scheduler
            .nodes
            .read()
            .unwrap()
            .contains_key(&node_id));
    }

    #[test]
    fn test_no_alive_nodes() {
        let scheduler = GlobalScheduler::with_default_policy();

        let task = TaskSpec {
            task_id: TaskId::new(),
            job_id: ray_core::id::JobId::new(),
            function_name: "test".to_string(),
            function_payload: vec![],
            return_ids: vec![],
            dependency_ids: vec![],
            required_resources: Resources::new().set("CPU", 1.0),
            max_retries: 0,
        };

        assert!(scheduler.schedule_task(&task).is_err());
    }
}
