//! Core identifier types used across the Ray runtime.
//!
//! Each ID wraps a fixed-size byte array for efficient comparison,
//! hashing, and serialization. New IDs are generated via UUID v4.

use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

/// Macro to define a fixed-size identifier type.
macro_rules! define_id {
    ($name:ident, $size:expr, $doc:expr) => {
        #[doc = $doc]
        #[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
        pub struct $name([u8; $size]);

        impl $name {
            /// Create a new random identifier.
            pub fn new() -> Self {
                let uuid = Uuid::new_v4();
                let mut bytes = [0u8; $size];
                let uuid_bytes = uuid.as_bytes();
                let copy_len = uuid_bytes.len().min($size);
                bytes[..copy_len].copy_from_slice(&uuid_bytes[..copy_len]);
                Self(bytes)
            }

            /// Create from raw bytes.
            pub fn from_bytes(bytes: [u8; $size]) -> Self {
                Self(bytes)
            }

            /// Create from a byte slice (panics if wrong length).
            pub fn from_slice(slice: &[u8]) -> Self {
                let mut bytes = [0u8; $size];
                let copy_len = slice.len().min($size);
                bytes[..copy_len].copy_from_slice(&slice[..copy_len]);
                Self(bytes)
            }

            /// Get the raw bytes.
            pub fn as_bytes(&self) -> &[u8; $size] {
                &self.0
            }

            /// Convert to a byte vector (for protobuf).
            pub fn to_vec(&self) -> Vec<u8> {
                self.0.to_vec()
            }

            /// Create from a byte vector (for protobuf).
            pub fn from_vec(v: &[u8]) -> Self {
                Self::from_slice(v)
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self([0u8; $size])
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({})", stringify!($name), hex::encode(&self.0[..4]))
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", hex::encode(&self.0[..4]))
            }
        }
    };
}

// Simple hex encoding without external dependency
mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{:02x}", b)).collect()
    }
}

define_id!(TaskId, 16, "Unique identifier for a task.");
define_id!(ActorId, 16, "Unique identifier for an actor.");
define_id!(WorkerId, 16, "Unique identifier for a worker process.");
define_id!(NodeId, 16, "Unique identifier for a node in the cluster.");
define_id!(JobId, 16, "Unique identifier for a job.");
define_id!(ObjectId, 28, "Unique identifier for an object (task_id + return_index).");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_id_generation() {
        let id1 = TaskId::new();
        let id2 = TaskId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_id_from_bytes() {
        let bytes = [42u8; 16];
        let id = TaskId::from_bytes(bytes);
        assert_eq!(id.as_bytes(), &bytes);
    }

    #[test]
    fn test_object_id_size() {
        let id = ObjectId::new();
        assert_eq!(id.as_bytes().len(), 28);
    }
}
