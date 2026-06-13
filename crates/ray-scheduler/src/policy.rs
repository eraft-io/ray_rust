//! Scheduling policies for the global scheduler.
//!
//! Policies determine how tasks are mapped to nodes. Built-in policies:
//! - **Spread**: Distribute tasks evenly across nodes (good for load balancing)
//! - **Pack**: Concentrate tasks on fewer nodes (good for resource efficiency)
//! - **Locality-aware**: Prefer nodes that have the task's input data (future)

use crate::global::NodeView;
use ray_core::error::{RayError, RayResult};
use ray_core::resource::Resources;

/// Trait for pluggable scheduling policies.
pub trait SchedulingPolicy: Send + Sync {
    /// Select the best node for a task with the given resource requirements.
    fn select_node<'a>(
        &self,
        nodes: &[&'a NodeView],
        required: &Resources,
    ) -> RayResult<&'a NodeView>;

    /// Name of the policy (for logging/debugging).
    fn name(&self) -> &str;
}

/// Spread policy: pick the node with the most available resources.
///
/// This distributes load evenly across the cluster, which is good for
/// avoiding hotspots and ensuring consistent latency.
pub struct SpreadPolicy;

impl SchedulingPolicy for SpreadPolicy {
    fn select_node<'a>(
        &self,
        nodes: &[&'a NodeView],
        required: &Resources,
    ) -> RayResult<&'a NodeView> {
        // Filter nodes that can satisfy the resource requirement
        let eligible: Vec<&&NodeView> = nodes
            .iter()
            .filter(|n| n.available.can_satisfy(required))
            .collect();

        if eligible.is_empty() {
            return Err(RayError::InsufficientResources(format!(
                "No node has enough resources for {:?}",
                required
            )));
        }

        // Pick the node with the most available CPU (spread)
        eligible
            .into_iter()
            .max_by(|a, b| {
                a.available
                    .get("CPU")
                    .partial_cmp(&b.available.get("CPU"))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .copied()
            .ok_or_else(|| RayError::SchedulingFailed("Spread policy: no eligible node".into()))
    }

    fn name(&self) -> &str {
        "spread"
    }
}

/// Pack policy: pick the node with the least available resources
/// that can still satisfy the requirement.
///
/// This consolidates tasks on fewer nodes, leaving other nodes
/// free for larger tasks or reducing the number of active nodes.
pub struct PackPolicy;

impl SchedulingPolicy for PackPolicy {
    fn select_node<'a>(
        &self,
        nodes: &[&'a NodeView],
        required: &Resources,
    ) -> RayResult<&'a NodeView> {
        let eligible: Vec<&&NodeView> = nodes
            .iter()
            .filter(|n| n.available.can_satisfy(required))
            .collect();

        if eligible.is_empty() {
            return Err(RayError::InsufficientResources(format!(
                "No node has enough resources for {:?}",
                required
            )));
        }

        // Pick the node with the least available CPU (pack)
        eligible
            .into_iter()
            .min_by(|a, b| {
                a.available
                    .get("CPU")
                    .partial_cmp(&b.available.get("CPU"))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .copied()
            .ok_or_else(|| RayError::SchedulingFailed("Pack policy: no eligible node".into()))
    }

    fn name(&self) -> &str {
        "pack"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ray_core::id::NodeId;

    fn make_node(cpu: f64) -> NodeView {
        NodeView {
            node_id: NodeId::new(),
            address: "127.0.0.1".to_string(),
            port: 6379,
            available: Resources::new().set("CPU", cpu),
            total: Resources::new().set("CPU", 8.0),
            is_alive: true,
        }
    }

    #[test]
    fn test_spread_policy() {
        let n1 = make_node(2.0);
        let n2 = make_node(6.0);
        let n3 = make_node(4.0);
        let nodes: Vec<&NodeView> = vec![&n1, &n2, &n3];

        let policy = SpreadPolicy;
        let required = Resources::new().set("CPU", 1.0);
        let selected = policy.select_node(&nodes, &required).unwrap();
        assert_eq!(selected.available.get("CPU"), 6.0);
    }

    #[test]
    fn test_pack_policy() {
        let n1 = make_node(2.0);
        let n2 = make_node(6.0);
        let n3 = make_node(4.0);
        let nodes: Vec<&NodeView> = vec![&n1, &n2, &n3];

        let policy = PackPolicy;
        let required = Resources::new().set("CPU", 1.0);
        let selected = policy.select_node(&nodes, &required).unwrap();
        assert_eq!(selected.available.get("CPU"), 2.0);
    }
}
