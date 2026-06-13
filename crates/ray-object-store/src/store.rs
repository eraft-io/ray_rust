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
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

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

impl ObjectEntry {
    /// Return current reference count.
    pub fn ref_count(&self) -> u32 {
        self.ref_count
    }
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

    /// Get a clone of an object entry (if it exists).
    pub fn get_entry(&self, object_id: &ObjectId) -> Option<ObjectEntry> {
        self.objects.read().unwrap().get(object_id).cloned()
    }

    /// Increment the reference count for an object.
    pub fn add_reference(&self, object_id: &ObjectId) {
        let mut objects = self.objects.write().unwrap();
        if let Some(entry) = objects.get_mut(object_id) {
            entry.ref_count += 1;
            debug!(?object_id, new_count = entry.ref_count, "Reference added");
        } else {
            warn!(?object_id, "add_reference: object not found");
        }
    }

    /// Decrement the reference count. Returns true if the object was evicted
    /// (ref count reached zero and was removed).
    pub fn remove_reference(&self, object_id: &ObjectId) -> bool {
        let mut objects = self.objects.write().unwrap();
        let should_remove = if let Some(entry) = objects.get_mut(object_id) {
            entry.ref_count = entry.ref_count.saturating_sub(1);
            debug!(?object_id, new_count = entry.ref_count, "Reference removed");
            entry.ref_count == 0
        } else {
            false
        };
        if should_remove {
            objects.remove(object_id);
            info!(?object_id, "Object evicted (zero references)");
            true
        } else {
            false
        }
    }

    /// Spill an object's data to disk at the given directory.
    pub async fn spill_to_disk(&self, object_id: &ObjectId, dir: &str) -> RayResult<()> {
        let data = {
            let objects = self.objects.read().unwrap();
            let entry = objects.get(object_id).ok_or_else(|| {
                RayError::ObjectNotFound(format!("{:?}", object_id))
            })?;
            entry.data.clone()
        };

        let spill_dir = PathBuf::from(dir);
        tokio::fs::create_dir_all(&spill_dir)
            .await
            .map_err(|e| RayError::Io(e))?;

        let hex_id: String = object_id
            .as_bytes()
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect();
        let spill_path = spill_dir.join(format!("{}.bin", hex_id));

        tokio::fs::write(&spill_path, &data)
            .await
            .map_err(|e| RayError::Io(e))?;

        let mut objects = self.objects.write().unwrap();
        if let Some(entry) = objects.get_mut(object_id) {
            entry.is_spilled = true;
            entry.spill_url = Some(spill_path.to_string_lossy().to_string());
        }

        info!(?object_id, path = %spill_path.display(), "Object spilled to disk");
        Ok(())
    }

    /// Restore an object's data from a previously spilled disk file.
    pub async fn restore_from_disk(&self, object_id: &ObjectId) -> RayResult<()> {
        let spill_path = {
            let objects = self.objects.read().unwrap();
            let entry = objects.get(object_id).ok_or_else(|| {
                RayError::ObjectNotFound(format!("{:?}", object_id))
            })?;
            entry.spill_url.clone().ok_or_else(|| {
                RayError::ObjectNotFound(format!("{:?} not spilled", object_id))
            })?
        };

        let path = Path::new(&spill_path);
        let data = tokio::fs::read(path)
            .await
            .map_err(|e| RayError::Io(e))?;

        {
            let mut objects = self.objects.write().unwrap();
            if let Some(entry) = objects.get_mut(object_id) {
                entry.data = data;
                entry.is_spilled = false;
                entry.spill_url = None;
            }
        } // guard dropped before await

        // Clean up the spill file
        let _ = tokio::fs::remove_file(path).await;

        info!(?object_id, "Object restored from disk");
        Ok(())
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

    #[tokio::test]
    async fn test_reference_counting() {
        let store = InMemoryObjectStore::new(0);
        let id = ObjectId::new();
        store.put(id.clone(), vec![1, 2]).await.unwrap();

        // Initial ref_count is 1 (set on put)
        assert_eq!(store.get_entry(&id).unwrap().ref_count(), 1);

        store.add_reference(&id);
        assert_eq!(store.get_entry(&id).unwrap().ref_count(), 2);

        // remove_reference decrements but doesn't evict when > 0
        assert!(!store.remove_reference(&id));
        assert_eq!(store.get_entry(&id).unwrap().ref_count(), 1);

        // Last remove evicts
        assert!(store.remove_reference(&id));
        assert!(store.get_entry(&id).is_none());
    }

    #[tokio::test]
    async fn test_spill_and_restore() {
        let store = InMemoryObjectStore::new(0);
        let id = ObjectId::new();
        let data = vec![10, 20, 30, 40];
        store.put(id.clone(), data.clone()).await.unwrap();

        let spill_dir = std::env::temp_dir().join("ray_spill_test");
        let spill_dir_str = spill_dir.to_str().unwrap();

        store.spill_to_disk(&id, spill_dir_str).await.unwrap();
        let entry = store.get_entry(&id).unwrap();
        assert!(entry.is_spilled);

        store.restore_from_disk(&id).await.unwrap();
        let entry = store.get_entry(&id).unwrap();
        assert!(!entry.is_spilled);
        assert_eq!(entry.data, data);

        // Clean up
        let _ = std::fs::remove_dir_all(&spill_dir);
    }
}
