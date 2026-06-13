//! Object Store gRPC server implementation.

use crate::proto::object_store::object_store_service_server::{
    ObjectStoreService, ObjectStoreServiceServer,
};
use crate::proto::object_store::*;
use crate::store::InMemoryObjectStore;
use ray_core::error::RayResult;
use ray_core::id::ObjectId;
use ray_core::traits::ObjectStore;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::info;

/// Object Store gRPC server.
pub struct ObjectStoreServer {
    store: Arc<InMemoryObjectStore>,
}

impl ObjectStoreServer {
    pub fn new(store: Arc<InMemoryObjectStore>) -> Self {
        Self { store }
    }

    /// Start the gRPC server on the given address.
    pub async fn serve(self, addr: std::net::SocketAddr) -> RayResult<()> {
        info!(%addr, "Starting ObjectStore gRPC server");
        tonic::transport::Server::builder()
            .add_service(ObjectStoreServiceServer::new(self))
            .serve(addr)
            .await
            .map_err(|e| ray_core::error::RayError::Transport(e.to_string()))
    }
}

#[tonic::async_trait]
impl ObjectStoreService for ObjectStoreServer {
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutReply>, Status> {
        let req = request.into_inner();
        let obj = req
            .object_data
            .ok_or_else(|| Status::invalid_argument("object_data is required"))?;

        let object_id = obj
            .object_id
            .ok_or_else(|| Status::invalid_argument("object_id is required"))?;
        let id: ObjectId = object_id.into();

        self.store
            .put(id, obj.data)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(PutReply {
            success: true,
            error_message: String::new(),
        }))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetReply>, Status> {
        let req = request.into_inner();
        let object_id = req
            .object_id
            .ok_or_else(|| Status::invalid_argument("object_id is required"))?;
        let id: ObjectId = object_id.clone().into();

        match self.store.get(&id, req.timeout_ms).await {
            Ok(data) => Ok(Response::new(GetReply {
                found: true,
                object_data: Some(ObjectData {
                    object_id: Some(object_id),
                    data,
                    metadata: vec![],
                }),
                error_message: String::new(),
            })),
            Err(e) => Ok(Response::new(GetReply {
                found: false,
                object_data: None,
                error_message: e.to_string(),
            })),
        }
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteReply>, Status> {
        let req = request.into_inner();
        let mut num_deleted = 0;
        for obj_id in req.object_ids {
            let id: ObjectId = obj_id.into();
            if self.store.delete(&id).await.is_ok() {
                num_deleted += 1;
            }
        }
        Ok(Response::new(DeleteReply { num_deleted }))
    }

    async fn contains(
        &self,
        request: Request<ContainsRequest>,
    ) -> Result<Response<ContainsReply>, Status> {
        let req = request.into_inner();
        let object_id = req
            .object_id
            .ok_or_else(|| Status::invalid_argument("object_id is required"))?;
        let id: ObjectId = object_id.into();

        let exists = self
            .store
            .contains(&id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(ContainsReply { exists }))
    }

    async fn batch_get(
        &self,
        request: Request<BatchGetRequest>,
    ) -> Result<Response<BatchGetReply>, Status> {
        let req = request.into_inner();
        let mut results = Vec::new();
        let mut found_flags = Vec::new();

        for obj_id in &req.object_ids {
            let id: ObjectId = obj_id.clone().into();
            match self.store.get(&id, req.timeout_ms).await {
                Ok(data) => {
                    results.push(ObjectData {
                        object_id: Some(obj_id.clone()),
                        data,
                        metadata: vec![],
                    });
                    found_flags.push(true);
                }
                Err(_) => {
                    found_flags.push(false);
                }
            }
        }

        Ok(Response::new(BatchGetReply {
            results,
            found_flags,
        }))
    }

    async fn batch_put(
        &self,
        request: Request<BatchPutRequest>,
    ) -> Result<Response<BatchPutReply>, Status> {
        let req = request.into_inner();
        let mut num_succeeded = 0;
        let mut error_messages = Vec::new();

        for obj_data in req.object_data_list {
            let obj_id = obj_data
                .object_id
                .ok_or_else(|| Status::invalid_argument("object_id required"))?;
            let id: ObjectId = obj_id.into();

            match self.store.put(id, obj_data.data).await {
                Ok(()) => num_succeeded += 1,
                Err(e) => error_messages.push(e.to_string()),
            }
        }

        Ok(Response::new(BatchPutReply {
            num_succeeded,
            error_messages,
        }))
    }

    async fn get_object_info(
        &self,
        request: Request<GetObjectInfoRequest>,
    ) -> Result<Response<GetObjectInfoReply>, Status> {
        let req = request.into_inner();
        let object_id = req
            .object_id
            .ok_or_else(|| Status::invalid_argument("object_id is required"))?;
        let id: ObjectId = object_id.clone().into();

        match self.store.get_entry(&id) {
            Some(entry) => {
                let elapsed = entry.create_time.elapsed();
                Ok(Response::new(GetObjectInfoReply {
                    found: true,
                    metadata: Some(ObjectMetadata {
                        object_id: Some(object_id),
                        data_size: entry.data.len() as i64,
                        metadata_size: 0,
                        total_size: entry.data.len() as i64,
                        ref_count: entry.ref_count() as i32,
                        is_spilled: entry.is_spilled,
                        spill_url: entry.spill_url.clone().unwrap_or_default(),
                        create_time_ms: elapsed.as_millis() as i64,
                    }),
                }))
            }
            None => Ok(Response::new(GetObjectInfoReply {
                found: false,
                metadata: None,
            })),
        }
    }

    async fn wait(&self, request: Request<WaitRequest>) -> Result<Response<WaitReply>, Status> {
        let req = request.into_inner();
        let object_ids: Vec<ObjectId> = req
            .object_ids
            .iter()
            .map(|oid| oid.clone().into())
            .collect();

        let ready_flags = self
            .store
            .wait(&object_ids, req.num_ready, req.timeout_ms)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let num_ready = ready_flags.iter().filter(|&&r| r).count() as i32;

        Ok(Response::new(WaitReply {
            ready_flags,
            num_ready,
        }))
    }

    async fn spill_objects(
        &self,
        request: Request<SpillObjectsRequest>,
    ) -> Result<Response<SpillObjectsReply>, Status> {
        let req = request.into_inner();
        let storage_url = if req.storage_url.is_empty() {
            "/tmp/ray_spill".to_string()
        } else {
            req.storage_url
        };

        let mut num_spilled = 0;
        let mut error_messages = Vec::new();

        for obj_id_proto in &req.object_ids {
            let id: ObjectId = obj_id_proto.clone().into();
            match self.store.spill_to_disk(&id, &storage_url).await {
                Ok(()) => num_spilled += 1,
                Err(e) => error_messages.push(e.to_string()),
            }
        }

        Ok(Response::new(SpillObjectsReply {
            num_spilled,
            error_messages,
        }))
    }

    async fn restore_objects(
        &self,
        request: Request<RestoreObjectsRequest>,
    ) -> Result<Response<RestoreObjectsReply>, Status> {
        let req = request.into_inner();
        let mut num_restored = 0;

        for obj_id_proto in &req.object_ids {
            let id: ObjectId = obj_id_proto.clone().into();
            if self.store.restore_from_disk(&id).await.is_ok() {
                num_restored += 1;
            }
        }

        Ok(Response::new(RestoreObjectsReply { num_restored }))
    }

    async fn add_reference(
        &self,
        request: Request<AddReferenceRequest>,
    ) -> Result<Response<AddReferenceReply>, Status> {
        let req = request.into_inner();
        let object_id = req
            .object_id
            .ok_or_else(|| Status::invalid_argument("object_id is required"))?;
        let id: ObjectId = object_id.into();

        self.store.add_reference(&id);
        Ok(Response::new(AddReferenceReply { success: true }))
    }

    async fn remove_reference(
        &self,
        request: Request<RemoveReferenceRequest>,
    ) -> Result<Response<RemoveReferenceReply>, Status> {
        let req = request.into_inner();
        let object_id = req
            .object_id
            .ok_or_else(|| Status::invalid_argument("object_id is required"))?;
        let id: ObjectId = object_id.into();

        let evicted = self.store.remove_reference(&id);
        Ok(Response::new(RemoveReferenceReply {
            success: true,
            evicted,
        }))
    }
}
