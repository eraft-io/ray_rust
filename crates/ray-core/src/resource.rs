//! Resource types for task and actor scheduling.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

/// Resource specification — a map of resource name → quantity.
///
/// Common resource keys: `"CPU"`, `"GPU"`, `"memory"`, `"object_store_memory"`.
/// Custom resource labels are also supported.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Resources {
    pub map: HashMap<String, f64>,
}

impl Resources {
    /// Create an empty resource set.
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    /// Create a resource set with the given CPU and memory.
    pub fn with_cpu_and_memory(cpu: f64, memory_bytes: f64) -> Self {
        let mut map = HashMap::new();
        map.insert("CPU".to_string(), cpu);
        map.insert("memory".to_string(), memory_bytes);
        Self { map }
    }

    /// Add or update a resource.
    pub fn set(mut self, name: impl Into<String>, quantity: f64) -> Self {
        self.map.insert(name.into(), quantity);
        self
    }

    /// Get the quantity of a specific resource (0.0 if not present).
    pub fn get(&self, name: &str) -> f64 {
        self.map.get(name).copied().unwrap_or(0.0)
    }

    /// Check if this resource set can satisfy the `required` resource set.
    pub fn can_satisfy(&self, required: &Resources) -> bool {
        for (name, &qty) in &required.map {
            if self.get(name) < qty {
                return false;
            }
        }
        true
    }

    /// Subtract `other` from this resource set (for allocation).
    pub fn subtract(&mut self, other: &Resources) {
        for (name, &qty) in &other.map {
            let current = self.map.entry(name.clone()).or_insert(0.0);
            *current -= qty;
            if *current <= 0.0 {
                self.map.remove(name);
            }
        }
    }

    /// Add `other` to this resource set (for release).
    pub fn add(&mut self, other: &Resources) {
        for (name, &qty) in &other.map {
            *self.map.entry(name.clone()).or_insert(0.0) += qty;
        }
    }
}

impl fmt::Display for Resources {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let pairs: Vec<String> = self
            .map
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect();
        write!(f, "Resources({{{}}})", pairs.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_can_satisfy() {
        let available = Resources::new().set("CPU", 4.0).set("GPU", 2.0);
        let required = Resources::new().set("CPU", 2.0).set("GPU", 1.0);
        assert!(available.can_satisfy(&required));

        let too_much = Resources::new().set("CPU", 8.0);
        assert!(!available.can_satisfy(&too_much));
    }

    #[test]
    fn test_subtract_add() {
        let mut available = Resources::new().set("CPU", 4.0).set("GPU", 2.0);
        let used = Resources::new().set("CPU", 1.0).set("GPU", 1.0);
        available.subtract(&used);
        assert_eq!(available.get("CPU"), 3.0);
        assert_eq!(available.get("GPU"), 1.0);

        available.add(&used);
        assert_eq!(available.get("CPU"), 4.0);
        assert_eq!(available.get("GPU"), 2.0);
    }
}
