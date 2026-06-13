//! In-memory implementation of the GCS store.
//!
//! This is the default backend for GCS. For production clusters,
//! this could be swapped with an etcd-backed or Redis-backed store
//! via the `GcsStore` trait.

use async_trait::async_trait;
use ray_core::error::{RayError, RayResult};
use ray_core::id::*;
use ray_core::traits::{ActorInfo, ActorSpec, ActorState, GcsStore, NodeInfo};
use std::collections::HashMap;
use std::sync::RwLock;
use tracing::{info, warn};

/// In-memory GCS store backed by `HashMap`s protected by `RwLock`.
///
/// Suitable for single-head-node deployments. For multi-replica GCS,
/// replace with a distributed store (etcd, Redis, etc.).
pub struct InMemoryGcsStore {
    nodes: RwLock<HashMap<NodeId, NodeInfo>>,
    actors: RwLock<HashMap<ActorId, ActorInfo>>,
}

impl InMemoryGcsStore {
    pub fn new() -> Self {
        Self {
            nodes: RwLock::new(HashMap::new()),
            actors: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for InMemoryGcsStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl GcsStore for InMemoryGcsStore {
    // ── Node operations ──

    async fn register_node(&self, node_info: NodeInfo) -> RayResult<()> {
        let node_id = node_info.node_id.clone();
        info!(?node_id, "Registering node in GCS");
        self.nodes.write().unwrap().insert(node_id, node_info);
        Ok(())
    }

    async fn unregister_node(&self, node_id: &NodeId) -> RayResult<()> {
        warn!(?node_id, "Unregistering node from GCS");
        self.nodes.write().unwrap().remove(node_id);
        Ok(())
    }

    async fn get_all_nodes(&self) -> RayResult<Vec<NodeInfo>> {
        let nodes = self.nodes.read().unwrap();
        Ok(nodes.values().cloned().collect())
    }

    // ── Actor operations ──

    async fn register_actor(&self, actor_spec: ActorSpec) -> RayResult<ActorId> {
        let actor_id = actor_spec.actor_id.clone();
        let job_id = actor_spec.job_id.clone();
        info!(?actor_id, ?job_id, "Registering actor in GCS");

        let actor_info = ActorInfo {
            actor_id: actor_id.clone(),
            job_id,
            class_name: actor_spec.class_name,
            state: ActorState::Pending,
            node_id: NodeId::default(),
            num_restarts: 0,
        };

        self.actors.write().unwrap().insert(actor_id.clone(), actor_info);
        Ok(actor_id)
    }

    async fn get_actor(&self, actor_id: &ActorId) -> RayResult<Option<ActorInfo>> {
        let actors = self.actors.read().unwrap();
        Ok(actors.get(actor_id).cloned())
    }

    async fn get_all_actors(&self, job_id: Option<&JobId>) -> RayResult<Vec<ActorInfo>> {
        let actors = self.actors.read().unwrap();
        let filtered: Vec<ActorInfo> = if let Some(jid) = job_id {
            actors.values().filter(|a| &a.job_id == jid).cloned().collect()
        } else {
            actors.values().cloned().collect()
        };
        Ok(filtered)
    }

    async fn kill_actor(&self, actor_id: &ActorId) -> RayResult<()> {
        info!(?actor_id, "Killing actor");
        let mut actors = self.actors.write().unwrap();
        if let Some(actor) = actors.get_mut(actor_id) {
            actor.state = ActorState::Dead;
            Ok(())
        } else {
            Err(RayError::ActorNotFound(format!("{:?}", actor_id)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ray_core::Resources;

    #[tokio::test]
    async fn test_register_and_get_nodes() {
        let store = InMemoryGcsStore::new();
        let node = NodeInfo {
            node_id: NodeId::new(),
            address: "127.0.0.1".to_string(),
            port: 6379,
            total_resources: Resources::new().set("CPU", 4.0),
            available_resources: Resources::new().set("CPU", 4.0),
            is_alive: true,
        };
        store.register_node(node).await.unwrap();
        let nodes = store.get_all_nodes().await.unwrap();
        assert_eq!(nodes.len(), 1);
    }
}
