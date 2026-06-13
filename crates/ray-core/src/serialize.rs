//! Serialization traits for cross-language and zero-copy data exchange.
//!
//! The Ray runtime needs to serialize/deserialize:
//! - Function payloads (Python functions via pickle, Rust functions via bincode)
//! - Task arguments and return values
//! - Object data for the object store

use crate::error::{RayError, RayResult};
use serde::{de::DeserializeOwned, Serialize};

/// Serialize a value to bytes using bincode (fast binary format).
pub fn serialize<T: Serialize>(value: &T) -> RayResult<Vec<u8>> {
    bincode::serialize(value).map_err(|e| RayError::SerializationError(e.to_string()))
}

/// Deserialize a value from bytes using bincode.
pub fn deserialize<T: DeserializeOwned>(bytes: &[u8]) -> RayResult<T> {
    bincode::deserialize(bytes).map_err(|e| RayError::DeserializationError(e.to_string()))
}

/// Serialize to JSON (for debugging and cross-language interop).
pub fn serialize_json<T: Serialize>(value: &T) -> RayResult<String> {
    serde_json::to_string(value).map_err(|e| RayError::SerializationError(e.to_string()))
}

/// Deserialize from JSON.
pub fn deserialize_json<T: DeserializeOwned>(json: &str) -> RayResult<T> {
    serde_json::from_str(json).map_err(|e| RayError::DeserializationError(e.to_string()))
}

/// Trait for objects that can provide a zero-copy byte slice.
///
/// This is used by the object store to expose shared memory buffers
/// directly without copying data.
pub trait ZeroCopyRead {
    /// Get a reference to the underlying bytes without copying.
    ///
    /// # Safety
    /// The returned slice must remain valid for the lifetime of `self`.
    fn as_bytes(&self) -> &[u8];

    /// Get the size of the data in bytes.
    fn len(&self) -> usize {
        self.as_bytes().len()
    }

    /// Check if the data is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Trait for objects that support writing data in-place (pre-allocated buffer).
///
/// Used by the object store to allow writers to fill a shared memory
/// region directly, avoiding an extra copy.
pub trait ZeroCopyWrite {
    /// Get a mutable reference to the underlying buffer.
    fn as_bytes_mut(&mut self) -> &mut [u8];

    /// Write data into the buffer at the given offset.
    fn write_at(&mut self, offset: usize, data: &[u8]) -> RayResult<()> {
        let buf = self.as_bytes_mut();
        let end = offset + data.len();
        if end > buf.len() {
            return Err(RayError::SharedMemoryError(format!(
                "write out of bounds: offset {} + len {} > buffer {}",
                offset,
                data.len(),
                buf.len()
            )));
        }
        buf[offset..end].copy_from_slice(data);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip() {
        let original = vec![1u32, 2, 3, 42];
        let bytes = serialize(&original).unwrap();
        let restored: Vec<u32> = deserialize(&bytes).unwrap();
        assert_eq!(original, restored);
    }

    #[test]
    fn test_json_roundtrip() {
        let original = vec![1i64, 2, 3];
        let json = serialize_json(&original).unwrap();
        let restored: Vec<i64> = deserialize_json(&json).unwrap();
        assert_eq!(original, restored);
    }
}
