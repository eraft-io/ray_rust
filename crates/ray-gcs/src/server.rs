//! GCS gRPC server implementation.
//!
//! Hosts the `GcsService` defined in `gcs.proto` using tonic,
//! backed by a pluggable `GcsStore` trait.

use crate::proto::gcs::gcs_service_server::{GcsService, GcsServiceServer};
use crate::proto::gcs::*;
use ray_core::error::RayResult;
use ray_core::id::*;
use ray_core::proto_conv;
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
            node_id: proto_conv::id_from_option(node_info.node_id),
            address: node_info.node_manager_address,
            port: node_info.node_manager_port,
            total_resources: proto_conv::resources_from_option(node_info.total_resources.as_ref()),
            available_resources: proto_conv::resources_from_option(node_info.available_resources.as_ref()),
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
        let id: NodeId = node_id.into();

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
            .map(|n| {
                let fields = proto_conv::node_info_to_proto_fields(&n);
                NodeInfo {
                    node_id: fields.node_id,
                    node_manager_address: fields.address,
                    node_manager_port: fields.port,
                    total_resources: fields.total_resources,
                    available_resources: fields.available_resources,
                    is_alive: fields.is_alive,
                    ..Default::default()
                }
            })
            .collect();

        Ok(Response::new(GetAllNodeInfoReply { node_info_list }))
    }

    async fn report_node_heartbeat(
        &self,
        request: Request<NodeHeartbeatRequest>,
    ) -> Result<Response<NodeHeartbeatReply>, Status> {
        let req = request.into_inner();
        // Update available resources for the node
        if let Some(node_id_proto) = req.node_id {
            let node_id: NodeId = node_id_proto.into();
            if let Some(avail) = req.available_resources {
                let resources = proto_conv::resources_from_proto(&avail);
                // Update the node's available resources in the store
                let mut nodes = self.store.get_all_nodes().await.unwrap_or_default();
                if let Some(node) = nodes.iter_mut().find(|n| n.node_id == node_id) {
                    node.available_resources = resources;
                    let _ = self.store.register_node(node.clone()).await;
                }
            }
        }
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
        let spec = req
            .actor_spec
            .ok_or_else(|| Status::invalid_argument("actor_spec is required"))?;

        let core_spec = proto_conv::actor_spec_from_proto(&spec)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let actor_id = core_spec.actor_id.clone();

        self.store
            .register_actor(core_spec)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(RegisterActorReply {
            success: true,
            actor_id: Some(actor_id.into()),
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
        let id: ActorId = actor_id.into();

        let actor_info = self
            .store
            .get_actor(&id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        match actor_info {
            Some(info) => {
                let fields = proto_conv::actor_info_to_proto_fields(&info);
                Ok(Response::new(GetActorInfoReply {
                    actor_info: Some(ActorInfo {
                        actor_id: fields.actor_id,
                        job_id: fields.job_id,
                        class_name: fields.class_name,
                        state: fields.state,
                        node_id: fields.node_id,
                        num_restarts: fields.num_restarts,
                        ..Default::default()
                    }),
                }))
            }
            None => Err(Status::not_found("Actor not found")),
        }
    }

    async fn get_all_actor_info(
        &self,
        request: Request<GetAllActorInfoRequest>,
    ) -> Result<Response<GetAllActorInfoReply>, Status> {
        let req = request.into_inner();
        let job_id = req.job_id.map(|j| -> JobId { j.into() });

        let actors = self
            .store
            .get_all_actors(job_id.as_ref())
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let actor_info_list = actors
            .into_iter()
            .map(|a| {
                let fields = proto_conv::actor_info_to_proto_fields(&a);
                ActorInfo {
                    actor_id: fields.actor_id,
                    job_id: fields.job_id,
                    class_name: fields.class_name,
                    state: fields.state,
                    node_id: fields.node_id,
                    num_restarts: fields.num_restarts,
                    ..Default::default()
                }
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
        let id: ActorId = actor_id.into();

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
        let req = request.into_inner();
        let job_info = req
            .job_info
            .ok_or_else(|| Status::invalid_argument("job_info is required"))?;

        let core_job = ray_core::traits::JobInfo {
            job_id: proto_conv::id_from_option(job_info.job_id),
            driver_ip: job_info.driver_ip,
            start_time_ms: job_info.start_time_ms,
            end_time_ms: job_info.end_time_ms,
            is_dead: job_info.is_dead,
            config: job_info.config,
        };

        self.store
            .add_job(core_job)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(AddJobReply { success: true }))
    }

    async fn mark_job_finished(
        &self,
        request: Request<MarkJobFinishedRequest>,
    ) -> Result<Response<MarkJobFinishedReply>, Status> {
        let req = request.into_inner();
        let job_id = req
            .job_id
            .ok_or_else(|| Status::invalid_argument("job_id is required"))?;
        let id: JobId = job_id.into();

        self.store
            .mark_job_finished(&id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(MarkJobFinishedReply { success: true }))
    }

    async fn get_all_job_info(
        &self,
        _request: Request<GetAllJobInfoRequest>,
    ) -> Result<Response<GetAllJobInfoReply>, Status> {
        let jobs = self
            .store
            .get_all_jobs()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let job_info_list = jobs
            .into_iter()
            .map(|j| JobInfo {
                job_id: proto_conv::id_to_option(j.job_id),
                driver_ip: j.driver_ip,
                start_time_ms: j.start_time_ms,
                end_time_ms: j.end_time_ms,
                is_dead: j.is_dead,
                config: j.config,
            })
            .collect();

        Ok(Response::new(GetAllJobInfoReply { job_info_list }))
    }

    // ── Resource management ──

    async fn report_resource_usage(
        &self,
        request: Request<ReportResourceUsageRequest>,
    ) -> Result<Response<ReportResourceUsageReply>, Status> {
        let req = request.into_inner();
        let usage = req
            .usage
            .ok_or_else(|| Status::invalid_argument("usage is required"))?;

        let core_usage = ray_core::traits::ResourceUsageInfo {
            node_id: proto_conv::id_from_option(usage.node_id),
            available: proto_conv::resources_from_option(usage.available.as_ref()),
            total: proto_conv::resources_from_option(usage.total.as_ref()),
            timestamp_ms: usage.timestamp_ms,
        };

        self.store
            .report_resource_usage(core_usage)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(ReportResourceUsageReply {}))
    }

    async fn get_all_resource_usage(
        &self,
        _request: Request<GetAllResourceUsageRequest>,
    ) -> Result<Response<GetAllResourceUsageReply>, Status> {
        let usage_list = self
            .store
            .get_all_resource_usage()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let usage_list = usage_list
            .into_iter()
            .map(|u| ResourceUsage {
                node_id: proto_conv::id_to_option(u.node_id),
                available: proto_conv::resources_to_option(&u.available),
                total: proto_conv::resources_to_option(&u.total),
                timestamp_ms: u.timestamp_ms,
            })
            .collect();

        Ok(Response::new(GetAllResourceUsageReply { usage_list }))
    }
}
