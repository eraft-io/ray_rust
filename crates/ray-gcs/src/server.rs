//! GCS gRPC server implementation.
//!
//! Hosts the `GcsService` defined in `gcs.proto` using tonic,
//! backed by a pluggable `GcsStore` trait.

use crate::proto::gcs::gcs_service_server::{GcsService, GcsServiceServer};
use crate::proto::gcs::*;
use ray_core::error::RayResult;
use ray_core::id::*;
use ray_core::traits::GcsStore;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::info;

/// The GCS gRPC server, wrapping an inner store implementation.
pub struct GcsServer<S: GcsStore> {
    store: Arc<S>,
}

impl<S: GcsStore + 'static> GcsServer<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self { store }
    }

    /// Start the gRPC server on the given address.
    pub async fn serve(self, addr: std::net::SocketAddr) -> RayResult<()> {
        info!(%addr, "Starting GCS gRPC server");
        tonic::transport::Server::builder()
            .add_service(GcsServiceServer::new(self))
            .serve(addr)
            .await
            .map_err(|e| ray_core::error::RayError::Transport(e.to_string()))
    }
}

#[tonic::async_trait]
impl<S: GcsStore + 'static> GcsService for GcsServer<S> {
    // ── Node management ──

    async fn register_node(
        &self,
        request: Request<RegisterNodeRequest>,
    ) -> Result<Response<RegisterNodeReply>, Status> {
        let req = request.into_inner();
        let node_info = req
            .node_info
            .ok_or_else(|| Status::invalid_argument("node_info is required"))?;

        let core_node = ray_core::traits::NodeInfo {
            node_id: NodeId::from_vec(
                &node_info
                    .node_id
                    .as_ref()
                    .map(|n| n.id.clone())
                    .unwrap_or_default(),
            ),
            address: node_info.node_manager_address.clone(),
            port: node_info.node_manager_port,
            total_resources: ray_core::Resources::new(), // TODO: convert from proto
            available_resources: ray_core::Resources::new(),
            is_alive: true,
        };

        self.store
            .register_node(core_node)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(RegisterNodeReply {
            success: true,
            message: "Node registered".to_string(),
        }))
    }

    async fn unregister_node(
        &self,
        request: Request<UnregisterNodeRequest>,
    ) -> Result<Response<UnregisterNodeReply>, Status> {
        let req = request.into_inner();
        let node_id = req
            .node_id
            .ok_or_else(|| Status::invalid_argument("node_id is required"))?;
        let id = NodeId::from_vec(&node_id.id);

        self.store
            .unregister_node(&id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(UnregisterNodeReply { success: true }))
    }

    async fn get_all_node_info(
        &self,
        _request: Request<GetAllNodeInfoRequest>,
    ) -> Result<Response<GetAllNodeInfoReply>, Status> {
        let nodes = self
            .store
            .get_all_nodes()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let node_info_list = nodes
            .into_iter()
            .map(|n| crate::proto::gcs::NodeInfo {
                node_id: Some(crate::proto::common::NodeId {
                    id: n.node_id.to_vec(),
                }),
                node_manager_address: n.address,
                node_manager_port: n.port,
                is_alive: n.is_alive,
                ..Default::default()
            })
            .collect();

        Ok(Response::new(GetAllNodeInfoReply { node_info_list }))
    }

    async fn report_node_heartbeat(
        &self,
        request: Request<NodeHeartbeatRequest>,
    ) -> Result<Response<NodeHeartbeatReply>, Status> {
        let _req = request.into_inner();
        // TODO: Update node heartbeat timestamp and available resources
        Ok(Response::new(NodeHeartbeatReply {
            is_draining: false,
        }))
    }

    // ── Actor management ──

    async fn register_actor(
        &self,
        request: Request<RegisterActorRequest>,
    ) -> Result<Response<RegisterActorReply>, Status> {
        let req = request.into_inner();
        let _spec = req
            .actor_spec
            .ok_or_else(|| Status::invalid_argument("actor_spec is required"))?;

        // TODO: Convert proto ActorSpec to core ActorSpec and register
        let actor_id = ActorId::new();
        Ok(Response::new(RegisterActorReply {
            success: true,
            actor_id: Some(crate::proto::common::ActorId {
                id: actor_id.to_vec(),
            }),
        }))
    }

    async fn get_actor_info(
        &self,
        request: Request<GetActorInfoRequest>,
    ) -> Result<Response<GetActorInfoReply>, Status> {
        let req = request.into_inner();
        let actor_id = req
            .actor_id
            .ok_or_else(|| Status::invalid_argument("actor_id is required"))?;
        let id = ActorId::from_vec(&actor_id.id);

        let actor_info = self
            .store
            .get_actor(&id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        match actor_info {
            Some(info) => Ok(Response::new(GetActorInfoReply {
                actor_info: Some(crate::proto::gcs::ActorInfo {
                    actor_id: Some(crate::proto::common::ActorId {
                        id: info.actor_id.to_vec(),
                    }),
                    class_name: info.class_name,
                    ..Default::default()
                }),
            })),
            None => Err(Status::not_found("Actor not found")),
        }
    }

    async fn get_all_actor_info(
        &self,
        request: Request<GetAllActorInfoRequest>,
    ) -> Result<Response<GetAllActorInfoReply>, Status> {
        let req = request.into_inner();
        let job_id = req.job_id.map(|j| JobId::from_vec(&j.id));

        let actors = self
            .store
            .get_all_actors(job_id.as_ref())
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let actor_info_list = actors
            .into_iter()
            .map(|a| crate::proto::gcs::ActorInfo {
                actor_id: Some(crate::proto::common::ActorId {
                    id: a.actor_id.to_vec(),
                }),
                class_name: a.class_name,
                ..Default::default()
            })
            .collect();

        Ok(Response::new(GetAllActorInfoReply { actor_info_list }))
    }

    async fn kill_actor(
        &self,
        request: Request<KillActorRequest>,
    ) -> Result<Response<KillActorReply>, Status> {
        let req = request.into_inner();
        let actor_id = req
            .actor_id
            .ok_or_else(|| Status::invalid_argument("actor_id is required"))?;
        let id = ActorId::from_vec(&actor_id.id);

        self.store
            .kill_actor(&id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(KillActorReply { success: true }))
    }

    // ── Job management ──

    async fn add_job(
        &self,
        request: Request<AddJobRequest>,
    ) -> Result<Response<AddJobReply>, Status> {
        let _req = request.into_inner();
        // TODO: Register job in store
        Ok(Response::new(AddJobReply { success: true }))
    }

    async fn mark_job_finished(
        &self,
        request: Request<MarkJobFinishedRequest>,
    ) -> Result<Response<MarkJobFinishedReply>, Status> {
        let _req = request.into_inner();
        // TODO: Mark job as finished in store
        Ok(Response::new(MarkJobFinishedReply { success: true }))
    }

    async fn get_all_job_info(
        &self,
        _request: Request<GetAllJobInfoRequest>,
    ) -> Result<Response<GetAllJobInfoReply>, Status> {
        // TODO: Return all jobs from store
        Ok(Response::new(GetAllJobInfoReply {
            job_info_list: vec![],
        }))
    }

    // ── Resource management ──

    async fn report_resource_usage(
        &self,
        request: Request<ReportResourceUsageRequest>,
    ) -> Result<Response<ReportResourceUsageReply>, Status> {
        let _req = request.into_inner();
        // TODO: Update resource usage for the node
        Ok(Response::new(ReportResourceUsageReply {}))
    }

    async fn get_all_resource_usage(
        &self,
        _request: Request<GetAllResourceUsageRequest>,
    ) -> Result<Response<GetAllResourceUsageReply>, Status> {
        // TODO: Return all resource usage
        Ok(Response::new(GetAllResourceUsageReply {
            usage_list: vec![],
        }))
    }
}
