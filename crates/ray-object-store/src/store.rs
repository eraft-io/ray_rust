//! In-memory object store implementation.
//!
//! This is the core storage engine. Objects are stored in a `HashMap`
//! protected by a `RwLock` for concurrent access. Large objects can be
//! backed by shared memory for zero-copy cross-process reads.

use async_trait::async_trait;
use ray_core::error::{RayError, RayResult};
use ray_core::id::ObjectId;
use ray_core::traits::ObjectStore;
use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};
use tracing::{debug, info};

/// Metadata for a stored object.
#[derive(Debug, Clone)]
pub struct ObjectEntry {
    pub object_id: ObjectId,
    pub data: Vec<u8>,
    pub create_time: Instant,
    pub ref_count: u32,
    pub is_spilled: bool,
    pub spill_url: Option<String>,
}

/// In-memory object store.
///
/// For a single-node deployment, objects live in process memory.
/// For cross-process sharing, objects can be promoted to shared memory
/// via the `shared_memory` module.
pub struct InMemoryObjectStore {
    objects: RwLock<HashMap<ObjectId, ObjectEntry>>,
    /// Maximum memory budget in bytes (0 = unlimited).
    max_memory_bytes: usize,
}

impl InMemoryObjectStore {
    /// Create a new object store with the given memory budget.
    pub fn new(max_memory_bytes: usize) -> Self {
        Self {
            objects: RwLock::new(HashMap::new()),
            max_memory_bytes,
        }
    }

    /// Get the total size of all stored objects.
    pub fn total_size(&self) -> usize {
        self.objects
            .read()
            .unwrap()
            .values()
            .map(|e| e.data.len())
            .sum()
    }

    /// Get the number of stored objects.
    pub fn object_count(&self) -> usize {
        self.objects.read().unwrap().len()
    }

    /// Evict objects with zero reference count to free memory.
    pub fn evict_zero_ref_objects(&self) -> usize {
        let mut objects = self.objects.write().unwrap();
        let before = objects.len();
        objects.retain(|_, entry| entry.ref_count > 0);
        let evicted = before - objects.len();
        if evicted > 0 {
            info!(evicted, "Evicted zero-ref objects");
        }
        evicted
    }
}

#[async_trait]
impl ObjectStore for InMemoryObjectStore {
    async fn put(&self, object_id: ObjectId, data: Vec<u8>) -> RayResult<()> {
        // Check memory budget
        if self.max_memory_bytes > 0 {
            let current = self.total_size();
            if current + data.len() > self.max_memory_bytes {
                // Try to evict zero-ref objects first
                self.evict_zero_ref_objects();
                let current = self.total_size();
                if current + data.len() > self.max_memory_bytes {
                    return Err(RayError::InsufficientResources(format!(
                        "Object store full: {} + {} > {} bytes",
                        current,
                        data.len(),
                        self.max_memory_bytes
                    )));
                }
            }
        }

        let entry = ObjectEntry {
            object_id: object_id.clone(),
            data,
            create_time: Instant::now(),
            ref_count: 1,
            is_spilled: false,
            spill_url: None,
        };

        self.objects.write().unwrap().insert(object_id, entry);
        debug!("Object stored");
        Ok(())
    }

    async fn get(&self, object_id: &ObjectId, timeout_ms: i64) -> RayResult<Vec<u8>> {
        let deadline = Instant::now() + Duration::from_millis(timeout_ms as u64);

        loop {
            {
                let objects = self.objects.read().unwrap();
                if let Some(entry) = objects.get(object_id) {
                    return Ok(entry.data.clone());
                }
            }

            if Instant::now() >= deadline {
                return Err(RayError::Timeout(format!(
                    "Object {:?} not available after {}ms",
                    object_id, timeout_ms
                )));
            }

            // Wait briefly before retrying
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn delete(&self, object_id: &ObjectId) -> RayResult<()> {
        let mut objects = self.objects.write().unwrap();
        objects.remove(object_id);
        debug!(?object_id, "Object deleted");
        Ok(())
    }

    async fn contains(&self, object_id: &ObjectId) -> RayResult<bool> {
        let objects = self.objects.read().unwrap();
        Ok(objects.contains_key(object_id))
    }

    async fn wait(
        &self,
        object_ids: &[ObjectId],
        num_ready: i32,
        timeout_ms: i64,
    ) -> RayResult<Vec<bool>> {
        let deadline = Instant::now() + Duration::from_millis(timeout_ms as u64);

        loop {
            let (ready_flags, done) = {
                let objects = self.objects.read().unwrap();
                let ready_flags: Vec<bool> = object_ids
                    .iter()
                    .map(|id| objects.contains_key(id))
                    .collect();

                let ready_count = ready_flags.iter().filter(|&&r| r).count() as i32;
                let done = ready_count >= num_ready || Instant::now() >= deadline;
                (ready_flags, done)
            }; // `objects` guard dropped here

            if done {
                return Ok(ready_flags);
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_put_and_get() {
        let store = InMemoryObjectStore::new(0);
        let id = ObjectId::new();
        let data = vec![1, 2, 3, 4, 5];

        store.put(id.clone(), data.clone()).await.unwrap();
        let result = store.get(&id, 1000).await.unwrap();
        assert_eq!(result, data);
    }

    #[tokio::test]
    async fn test_contains() {
        let store = InMemoryObjectStore::new(0);
        let id = ObjectId::new();
        assert!(!store.contains(&id).await.unwrap());

        store.put(id.clone(), vec![42]).await.unwrap();
        assert!(store.contains(&id).await.unwrap());
    }

    #[tokio::test]
    async fn test_delete() {
        let store = InMemoryObjectStore::new(0);
        let id = ObjectId::new();
        store.put(id.clone(), vec![1]).await.unwrap();
        store.delete(&id).await.unwrap();
        assert!(!store.contains(&id).await.unwrap());
    }
}
