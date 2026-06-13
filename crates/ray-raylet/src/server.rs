//! Raylet gRPC server implementation.

use crate::proto::raylet::raylet_service_server::{RayletService, RayletServiceServer};
use crate::proto::raylet::*;
use crate::resource_manager::ResourceManager;
use crate::scheduler::LocalScheduler;
use ray_core::error::RayResult;
use ray_core::id::*;
use ray_core::proto_conv;
use ray_core::traits::{ObjectStore, Scheduler};
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::{debug, info};

/// The Raylet gRPC server, combining local scheduling and resource management.
pub struct RayletServer {
    scheduler: Arc<LocalScheduler>,
    resource_manager: Arc<ResourceManager>,
    object_store: Arc<dyn ObjectStore>,
    node_id: NodeId,
}

impl RayletServer {
    pub fn new(
        scheduler: Arc<LocalScheduler>,
        resource_manager: Arc<ResourceManager>,
        object_store: Arc<dyn ObjectStore>,
        node_id: NodeId,
    ) -> Self {
        Self {
            scheduler,
            resource_manager,
            object_store,
            node_id,
        }
    }

    /// Start the Raylet gRPC server on the given address.
    pub async fn serve(self, addr: std::net::SocketAddr) -> RayResult<()> {
        info!(%addr, ?self.node_id, "Starting Raylet gRPC server");
        tonic::transport::Server::builder()
            .add_service(RayletServiceServer::new(self))
            .serve(addr)
            .await
            .map_err(|e| ray_core::error::RayError::Transport(e.to_string()))
    }
}

#[tonic::async_trait]
impl RayletService for RayletServer {
    // ── Task management ──

    async fn submit_task(
        &self,
        request: Request<SubmitTaskRequest>,
    ) -> Result<Response<SubmitTaskReply>, Status> {
        let req = request.into_inner();
        let task_spec = req
            .task_spec
            .ok_or_else(|| Status::invalid_argument("task_spec is required"))?;

        let core_spec = proto_conv::task_spec_from_proto(&task_spec)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        self.scheduler
            .submit_task(core_spec)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(SubmitTaskReply {
            accepted: true,
            reject_reason: String::new(),
        }))
    }

    async fn cancel_task(
        &self,
        request: Request<CancelTaskRequest>,
    ) -> Result<Response<CancelTaskReply>, Status> {
        let req = request.into_inner();
        let task_id = req
            .task_id
            .ok_or_else(|| Status::invalid_argument("task_id is required"))?;
        let id: TaskId = task_id.into();

        self.scheduler
            .cancel_task(&id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(CancelTaskReply { success: true }))
    }

    async fn get_task_status(
        &self,
        request: Request<GetTaskStatusRequest>,
    ) -> Result<Response<GetTaskStatusReply>, Status> {
        let req = request.into_inner();
        let task_id = req
            .task_id
            .ok_or_else(|| Status::invalid_argument("task_id is required"))?;
        let id: TaskId = task_id.into();

        let status = self
            .scheduler
            .get_task_status(&id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(GetTaskStatusReply {
            status: proto_conv::task_status_to_proto(status),
            error_message: String::new(),
        }))
    }

    // ── Worker management ──

    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequest>,
    ) -> Result<Response<RegisterWorkerReply>, Status> {
        let req = request.into_inner();
        info!(language = %req.language, "Worker registered");
        Ok(Response::new(RegisterWorkerReply {
            success: true,
            node_id: proto_conv::id_to_option(self.node_id.clone()),
        }))
    }

    async fn unregister_worker(
        &self,
        request: Request<UnregisterWorkerRequest>,
    ) -> Result<Response<UnregisterWorkerReply>, Status> {
        let req = request.into_inner();
        info!(reason = %req.disconnect_reason, "Worker unregistered");
        Ok(Response::new(UnregisterWorkerReply { success: true }))
    }

    // ── Object location ──

    async fn wait_for_object(
        &self,
        request: Request<WaitForObjectRequest>,
    ) -> Result<Response<WaitForObjectReply>, Status> {
        let req = request.into_inner();
        let object_id = req
            .object_id
            .ok_or_else(|| Status::invalid_argument("object_id is required"))?;
        let id: ObjectId = object_id.into();

        let exists = self
            .object_store
            .contains(&id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(WaitForObjectReply {
            ready: exists,
            location_node_id: if exists {
                proto_conv::id_to_option(self.node_id.clone())
            } else {
                None
            },
        }))
    }

    async fn get_object_locations(
        &self,
        request: Request<GetObjectLocationsRequest>,
    ) -> Result<Response<GetObjectLocationsReply>, Status> {
        let req = request.into_inner();
        let mut locations = Vec::new();

        for obj_id_proto in &req.object_ids {
            let id: ObjectId = obj_id_proto.clone().into();
            let exists = self
                .object_store
                .contains(&id)
                .await
                .unwrap_or(false);

            if exists {
                locations.push(ObjectLocation {
                    object_id: Some(obj_id_proto.clone()),
                    node_ids: vec![self.node_id.clone().into()],
                    object_size: 0, // TODO: track object sizes
                });
            }
        }

        Ok(Response::new(GetObjectLocationsReply { locations }))
    }

    // ── Inter-raylet communication ──

    async fn transfer_object(
        &self,
        _request: Request<TransferObjectRequest>,
    ) -> Result<Response<TransferObjectReply>, Status> {
        // TODO: Implement cross-node object transfer (cluster mode)
        Ok(Response::new(TransferObjectReply { success: true }))
    }

    async fn request_worker_lease(
        &self,
        request: Request<RequestWorkerLeaseRequest>,
    ) -> Result<Response<RequestWorkerLeaseReply>, Status> {
        let req = request.into_inner();
        let required = req
            .required_resources
            .as_ref()
            .map(proto_conv::resources_from_proto)
            .unwrap_or_default();

        if self
            .resource_manager
            .try_allocate(&required)
            .unwrap_or(false)
        {
            Ok(Response::new(RequestWorkerLeaseReply {
                fulfilled: true,
                worker_address: Some(ray_core::proto::common::Address {
                    ip_address: "127.0.0.1".to_string(),
                    port: 0,
                    node_id: proto_conv::id_to_option(self.node_id.clone()),
                    worker_id: None,
                }),
                node_id: proto_conv::id_to_option(self.node_id.clone()),
            }))
        } else {
            Ok(Response::new(RequestWorkerLeaseReply {
                fulfilled: false,
                worker_address: None,
                node_id: None,
            }))
        }
    }

    async fn release_worker_lease(
        &self,
        _request: Request<ReleaseWorkerLeaseRequest>,
    ) -> Result<Response<ReleaseWorkerLeaseReply>, Status> {
        // In single-node mode, worker lease release is handled by the scheduler
        // after task completion. This RPC is a no-op for now.
        debug!("ReleaseWorkerLease called (no-op in single-node mode)");
        Ok(Response::new(ReleaseWorkerLeaseReply { success: true }))
    }
}
