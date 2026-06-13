//! Resource manager for tracking node-local resource allocation.
//!
//! The `ResourceManager` keeps track of total and available resources
//! on a single node, and handles allocation/release for tasks.

use ray_core::error::RayResult;
use ray_core::resource::Resources;
use std::sync::RwLock;
use tracing::{debug, warn};

/// Manages resource allocation on a single node.
pub struct ResourceManager {
    total: Resources,
    available: RwLock<Resources>,
}

impl ResourceManager {
    /// Create a new resource manager with the given total resources.
    pub fn new(total: Resources) -> Self {
        let available = RwLock::new(total.clone());
        Self { total, available }
    }

    /// Try to allocate resources for a task.
    /// Returns `Ok(true)` if allocated, `Ok(false)` if insufficient resources.
    pub fn try_allocate(&self, required: &Resources) -> RayResult<bool> {
        let mut available = self.available.write().unwrap();
        if available.can_satisfy(required) {
            available.subtract(required);
            debug!(?required, "Resources allocated");
            Ok(true)
        } else {
            warn!(?required, "Insufficient resources");
            Ok(false)
        }
    }

    /// Release resources back to the pool.
    pub fn release(&self, resources: &Resources) {
        let mut available = self.available.write().unwrap();
        available.add(resources);
        debug!(?resources, "Resources released");
    }

    /// Get a snapshot of available resources.
    pub fn available(&self) -> Resources {
        self.available.read().unwrap().clone()
    }

    /// Get a snapshot of total resources.
    pub fn total(&self) -> Resources {
        self.total.clone()
    }

    /// Get resource utilization as a fraction (0.0 to 1.0) for each resource.
    pub fn utilization(&self) -> Resources {
        let available = self.available.read().unwrap();
        let mut util = Resources::new();
        for (name, &total_qty) in &self.total.map {
            let avail_qty = available.get(name);
            if total_qty > 0.0 {
                util.map
                    .insert(name.clone(), (total_qty - avail_qty) / total_qty);
            }
        }
        util
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_allocate_and_release() {
        let rm = ResourceManager::new(Resources::new().set("CPU", 4.0).set("GPU", 2.0));

        let req = Resources::new().set("CPU", 1.0).set("GPU", 1.0);
        assert!(rm.try_allocate(&req).unwrap());
        assert_eq!(rm.available().get("CPU"), 3.0);
        assert_eq!(rm.available().get("GPU"), 1.0);

        rm.release(&req);
        assert_eq!(rm.available().get("CPU"), 4.0);
        assert_eq!(rm.available().get("GPU"), 2.0);
    }

    #[test]
    fn test_insufficient_resources() {
        let rm = ResourceManager::new(Resources::new().set("CPU", 2.0));
        let req = Resources::new().set("CPU", 4.0);
        assert!(!rm.try_allocate(&req).unwrap());
    }
}
