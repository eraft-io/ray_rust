//! Raylet gRPC server implementation.

use crate::proto::raylet::raylet_service_server::{RayletService, RayletServiceServer};
use crate::proto::raylet::*;
use crate::resource_manager::ResourceManager;
use crate::scheduler::LocalScheduler;
use ray_core::error::RayResult;
use ray_core::id::*;
use ray_core::traits::Scheduler;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::info;

/// The Raylet gRPC server, combining local scheduling and resource management.
#[allow(dead_code)]
pub struct RayletServer {
    scheduler: Arc<LocalScheduler>,
    resource_manager: Arc<ResourceManager>,
    node_id: NodeId,
}

impl RayletServer {
    pub fn new(
        scheduler: Arc<LocalScheduler>,
        resource_manager: Arc<ResourceManager>,
        node_id: NodeId,
    ) -> Self {
        Self {
            scheduler,
            resource_manager,
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
        let _task_spec = req
            .task_spec
            .ok_or_else(|| Status::invalid_argument("task_spec is required"))?;

        // TODO: Convert proto TaskSpec to core TaskSpec and submit to scheduler
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
        let id = TaskId::from_vec(&task_id.id);

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
        let id = TaskId::from_vec(&task_id.id);

        let status = self
            .scheduler
            .get_task_status(&id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(GetTaskStatusReply {
            status: match status {
                ray_core::traits::TaskStatus::Pending => 1,
                ray_core::traits::TaskStatus::Scheduled => 2,
                ray_core::traits::TaskStatus::Running => 3,
                ray_core::traits::TaskStatus::Finished => 4,
                ray_core::traits::TaskStatus::Failed => 5,
                ray_core::traits::TaskStatus::Cancelled => 6,
            },
            error_message: String::new(),
        }))
    }

    // ── Worker management ──

    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequest>,
    ) -> Result<Response<RegisterWorkerReply>, Status> {
        let req = request.into_inner();
        info!(?req.language, "Worker registered");
        Ok(Response::new(RegisterWorkerReply {
            success: true,
            node_id: Some(crate::proto::common::NodeId {
                id: self.node_id.to_vec(),
            }),
        }))
    }

    async fn unregister_worker(
        &self,
        request: Request<UnregisterWorkerRequest>,
    ) -> Result<Response<UnregisterWorkerReply>, Status> {
        let req = request.into_inner();
        info!(?req.disconnect_reason, "Worker unregistered");
        Ok(Response::new(UnregisterWorkerReply { success: true }))
    }

    // ── Object location ──

    async fn wait_for_object(
        &self,
        request: Request<WaitForObjectRequest>,
    ) -> Result<Response<WaitForObjectReply>, Status> {
        let _req = request.into_inner();
        // TODO: Query object store for object location
        Ok(Response::new(WaitForObjectReply {
            ready: false,
            location_node_id: None,
        }))
    }

    async fn get_object_locations(
        &self,
        request: Request<GetObjectLocationsRequest>,
    ) -> Result<Response<GetObjectLocationsReply>, Status> {
        let _req = request.into_inner();
        // TODO: Return object locations from the distributed directory
        Ok(Response::new(GetObjectLocationsReply {
            locations: vec![],
        }))
    }

    // ── Inter-raylet communication ──

    async fn transfer_object(
        &self,
        request: Request<TransferObjectRequest>,
    ) -> Result<Response<TransferObjectReply>, Status> {
        let _req = request.into_inner();
        // TODO: Initiate object transfer to target node
        Ok(Response::new(TransferObjectReply { success: true }))
    }

    async fn request_worker_lease(
        &self,
        request: Request<RequestWorkerLeaseRequest>,
    ) -> Result<Response<RequestWorkerLeaseReply>, Status> {
        let _req = request.into_inner();
        // TODO: Try to lease a worker from this node
        Ok(Response::new(RequestWorkerLeaseReply {
            fulfilled: false,
            worker_address: None,
            node_id: None,
        }))
    }

    async fn release_worker_lease(
        &self,
        request: Request<ReleaseWorkerLeaseRequest>,
    ) -> Result<Response<ReleaseWorkerLeaseReply>, Status> {
        let _req = request.into_inner();
        // TODO: Release the worker lease
        Ok(Response::new(ReleaseWorkerLeaseReply { success: true }))
    }
}
