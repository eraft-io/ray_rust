//! Shared memory abstraction for cross-process zero-copy object sharing.
//!
//! This module wraps platform-specific shared memory APIs (mmap on Unix,
//! CreateFileMapping on Windows) to provide a uniform interface for the
//! object store to expose data to other processes without copying.

use ray_core::error::{RayError, RayResult};
use ray_core::serialize::{ZeroCopyRead, ZeroCopyWrite};
use tracing::debug;

/// A shared memory region that can be accessed across processes.
///
/// Each `ShmRegion` represents a named shared memory segment.
/// Multiple processes can open the same segment by name.
pub struct ShmRegion {
    /// Unique name of the shared memory segment.
    name: String,
    /// Size of the region in bytes.
    size: usize,
    /// The inner shared memory object.
    inner: shared_memory::Shmem,
}

impl ShmRegion {
    /// Create a new shared memory region with the given name and size.
    pub fn create(name: &str, size: usize) -> RayResult<Self> {
        let shmem = shared_memory::ShmemConf::new()
            .size(size)
            .os_id(name)
            .create()
            .map_err(|e| RayError::SharedMemoryError(format!("create failed: {:?}", e)))?;

        debug!(name, size, "Created shared memory region");

        Ok(Self {
            name: name.to_string(),
            size,
            inner: shmem,
        })
    }

    /// Open an existing shared memory region by name.
    pub fn open(name: &str, size: usize) -> RayResult<Self> {
        let shmem = shared_memory::ShmemConf::new()
            .os_id(name)
            .open()
            .map_err(|e| RayError::SharedMemoryError(format!("open failed: {:?}", e)))?;

        debug!(name, "Opened shared memory region");

        Ok(Self {
            name: name.to_string(),
            size,
            inner: shmem,
        })
    }

    /// Get the name of this shared memory region.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl ZeroCopyRead for ShmRegion {
    fn as_bytes(&self) -> &[u8] {
        // SAFETY: The shared memory region is owned by this struct and
        // the slice lifetime is tied to `self`. Concurrent mutation is
        // the caller's responsibility (objects in Ray are immutable after put).
        unsafe { std::slice::from_raw_parts(self.inner.as_ptr(), self.size) }
    }
}

impl ZeroCopyWrite for ShmRegion {
    fn as_bytes_mut(&mut self) -> &mut [u8] {
        // SAFETY: Same as above. Mutable access is exclusive via `&mut self`.
        unsafe { std::slice::from_raw_parts_mut(self.inner.as_ptr(), self.size) }
    }
}

/// Pool of shared memory regions, used by the object store to allocate
/// and manage cross-process accessible buffers.
pub struct ShmPool {
    regions: std::collections::HashMap<String, ShmRegion>,
    base_dir: String,
    next_id: u64,
}

impl ShmPool {
    /// Create a new shared memory pool.
    pub fn new(base_dir: &str) -> Self {
        Self {
            regions: std::collections::HashMap::new(),
            base_dir: base_dir.to_string(),
            next_id: 0,
        }
    }

    /// Allocate a new shared memory region and return its name.
    pub fn allocate(&mut self, size: usize) -> RayResult<String> {
        let name = format!("{}_{}", self.base_dir, self.next_id);
        self.next_id += 1;

        let region = ShmRegion::create(&name, size)?;
        self.regions.insert(name.clone(), region);
        Ok(name)
    }

    /// Get a reference to an existing region by name.
    pub fn get(&self, name: &str) -> Option<&ShmRegion> {
        self.regions.get(name)
    }

    /// Release a shared memory region.
    pub fn release(&mut self, name: &str) {
        self.regions.remove(name);
        debug!(name, "Released shared memory region");
    }
}
