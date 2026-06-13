//! Python runtime bindings — the core of the `ray_rust` Python module.
//!
//! This module implements the Python-facing API using PyO3.
//! It manages a global tokio runtime and delegates operations to
//! the underlying Rust crates.

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::sync::Arc;
use tokio::runtime::Runtime;

use ray_core::id::ObjectId;
use ray_object_store::InMemoryObjectStore;
use ray_core::traits::ObjectStore;

// ──────────────────────────────────────────────
//  Global state
// ──────────────────────────────────────────────

/// Global tokio runtime (lazy-initialized).
static RUNTIME: std::sync::OnceLock<Runtime> = std::sync::OnceLock::new();

/// Global object store instance.
static OBJECT_STORE: std::sync::OnceLock<Arc<InMemoryObjectStore>> = std::sync::OnceLock::new();

/// Whether the runtime has been initialized.
static INITIALIZED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn get_runtime() -> PyResult<&'static Runtime> {
    RUNTIME.get().ok_or_else(|| {
        PyRuntimeError::new_err("Ray runtime not initialized. Call ray_rust.init() first.")
    })
}

fn get_object_store() -> PyResult<&'static Arc<InMemoryObjectStore>> {
    OBJECT_STORE.get().ok_or_else(|| {
        PyRuntimeError::new_err("Ray runtime not initialized. Call ray_rust.init() first.")
    })
}

// ──────────────────────────────────────────────
//  PyModule entry point
// ──────────────────────────────────────────────

/// The Python module definition.
///
/// Registers all public functions and classes that are accessible from Python.
#[pymodule]
pub fn ray_rust(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Initialize tracing subscriber for logging (only once)
    let _ = tracing_subscriber::fmt()
        .with_env_filter("ray=info")
        .try_init();

    // ── Top-level functions ──
    m.add_function(wrap_pyfunction!(py_init, m)?)?;
    m.add_function(wrap_pyfunction!(py_shutdown, m)?)?;
    m.add_function(wrap_pyfunction!(py_put, m)?)?;
    m.add_function(wrap_pyfunction!(py_get, m)?)?;
    m.add_function(wrap_pyfunction!(py_wait, m)?)?;
    m.add_function(wrap_pyfunction!(py_is_initialized, m)?)?;

    // ── Classes ──
    m.add_class::<PyObjectRef>()?;
    m.add_class::<PyTaskResult>()?;

    Ok(())
}

// ──────────────────────────────────────────────
//  Python-exposed functions
// ──────────────────────────────────────────────

/// Initialize the Ray runtime.
///
/// ```python
/// ray_rust.init(address="auto", num_cpus=4)
/// ```
#[pyfunction]
#[pyo3(signature = (address="auto", num_cpus=None))]
fn py_init(address: &str, num_cpus: Option<usize>) -> PyResult<()> {
    if INITIALIZED.load(std::sync::atomic::Ordering::SeqCst) {
        return Ok(());
    }

    let cpus = num_cpus.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
    });

    tracing::info!(address, cpus, "Initializing Ray Rust runtime");

    // Create tokio runtime
    let rt = Runtime::new().map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    RUNTIME
        .set(rt)
        .map_err(|_| PyRuntimeError::new_err("Runtime already set"))?;

    // Create object store (default 2GB memory budget)
    let store = Arc::new(InMemoryObjectStore::new(2 * 1024 * 1024 * 1024));
    OBJECT_STORE
        .set(store)
        .map_err(|_| PyRuntimeError::new_err("Object store already set"))?;

    INITIALIZED.store(true, std::sync::atomic::Ordering::SeqCst);

    tracing::info!("Ray Rust runtime initialized");
    Ok(())
}

/// Shut down the Ray runtime.
///
/// ```python
/// ray_rust.shutdown()
/// ```
#[pyfunction]
fn py_shutdown() -> PyResult<()> {
    tracing::info!("Shutting down Ray Rust runtime");
    INITIALIZED.store(false, std::sync::atomic::Ordering::SeqCst);
    // Note: We can't drop the OnceLock values, but we mark as uninitialized.
    // A production implementation would use a more sophisticated lifecycle.
    Ok(())
}

/// Check if the runtime is initialized.
///
/// ```python
/// is_init = ray_rust.is_initialized()
/// ```
#[pyfunction]
fn py_is_initialized() -> PyResult<bool> {
    Ok(INITIALIZED.load(std::sync::atomic::Ordering::SeqCst))
}

/// Put a Python object into the object store.
///
/// The data is serialized as bytes (pickle format from Python side).
///
/// ```python
/// ref = ray_rust.put(b"serialized_data")
/// ```
#[pyfunction]
fn py_put(_py: Python<'_>, data: &[u8]) -> PyResult<PyObjectRef> {
    let rt = get_runtime()?;
    let store = get_object_store()?;

    let object_id = ObjectId::new();
    let id_clone = object_id.clone();
    let data_owned = data.to_vec();
    let store_clone = store.clone();

    rt.block_on(async move {
        store_clone.put(id_clone, data_owned).await
    })
    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    Ok(PyObjectRef {
        object_id: object_id.to_vec(),
    })
}

/// Get an object from the object store.
///
/// ```python
/// data = ray_rust.get(ref, timeout_ms=10000)
/// ```
#[pyfunction]
#[pyo3(signature = (object_ref, timeout_ms=10000))]
fn py_get(_py: Python<'_>, object_ref: &PyObjectRef, timeout_ms: i64) -> PyResult<Vec<u8>> {
    let rt = get_runtime()?;
    let store = get_object_store()?;

    let object_id = ObjectId::from_vec(&object_ref.object_id);
    let store_clone = store.clone();

    let data = rt
        .block_on(async move { store_clone.get(&object_id, timeout_ms).await })
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    Ok(data)
}

/// Wait for objects to become available.
///
/// ```python
/// ready, not_ready = ray_rust.wait([ref1, ref2], num_returns=1, timeout_ms=5000)
/// ```
#[pyfunction]
#[pyo3(signature = (object_refs, num_returns=1, timeout_ms=5000))]
fn py_wait(
    _py: Python<'_>,
    object_refs: Vec<PyObjectRef>,
    num_returns: i32,
    timeout_ms: i64,
) -> PyResult<(Vec<PyObjectRef>, Vec<PyObjectRef>)> {
    let rt = get_runtime()?;
    let store = get_object_store()?;

    let object_ids: Vec<ObjectId> = object_refs
        .iter()
        .map(|r| ObjectId::from_vec(&r.object_id))
        .collect();

    let store_clone = store.clone();
    let ready_flags = rt
        .block_on(async move {
            store_clone
                .wait(&object_ids, num_returns, timeout_ms)
                .await
        })
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    let mut ready = Vec::new();
    let mut not_ready = Vec::new();

    for (r, is_ready) in object_refs.into_iter().zip(ready_flags) {
        if is_ready {
            ready.push(r);
        } else {
            not_ready.push(r);
        }
    }

    Ok((ready, not_ready))
}

// ──────────────────────────────────────────────
//  Python-exposed classes
// ──────────────────────────────────────────────

/// A reference to an object stored in the distributed object store.
///
/// Analogous to `ray.ObjectRef` in the Python API.
#[pyclass(name = "ObjectRef")]
#[derive(Clone)]
pub struct PyObjectRef {
    /// The raw object ID bytes.
    #[pyo3(get)]
    pub object_id: Vec<u8>,
}

#[pymethods]
impl PyObjectRef {
    fn __repr__(&self) -> String {
        format!(
            "ObjectRef({})",
            self.object_id
                .iter()
                .take(4)
                .map(|b| format!("{:02x}", b))
                .collect::<String>()
        )
    }

    fn __eq__(&self, other: &PyObjectRef) -> bool {
        self.object_id == other.object_id
    }

    fn __hash__(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        self.object_id.hash(&mut hasher);
        hasher.finish()
    }
}

/// Result of a remote task execution.
#[pyclass(name = "TaskResult")]
pub struct PyTaskResult {
    /// Whether the task succeeded.
    #[pyo3(get)]
    pub success: bool,
    /// The result data (if successful).
    #[pyo3(get)]
    pub data: Option<Vec<u8>>,
    /// Error message (if failed).
    #[pyo3(get)]
    pub error: Option<String>,
}

#[pymethods]
impl PyTaskResult {
    fn __repr__(&self) -> String {
        if self.success {
            format!(
                "TaskResult(success, data_len={})",
                self.data.as_ref().map_or(0, |d| d.len())
            )
        } else {
            format!(
                "TaskResult(failed: {})",
                self.error.as_deref().unwrap_or("unknown")
            )
        }
    }
}
