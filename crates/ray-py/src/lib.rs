//! `ray-py` — Python bindings for the Ray Rust runtime via PyO3.
//!
//! This crate exposes the Ray Core API to Python, allowing users to:
//! - Initialize a Ray runtime (`ray.init()`)
//! - Put/get objects (`ray.put()`, `ray.get()`)
//! - Submit remote tasks (`ray.remote()`)
//! - Create actors
//! - Query cluster status
//!
//! The Python module name is `ray_rust` and is designed to be used as
//! a backend for the existing `ray` Python package.
//!
//! ## Usage from Python
//! ```python
//! import ray_rust
//!
//! ray_rust.init(address="auto")
//! obj_ref = ray_rust.put([1, 2, 3])
//! result = ray_rust.get(obj_ref)
//! ```

pub mod runtime;

// Re-export the pymodule entry point so Cargo cdylib picks it up.
pub use runtime::ray_rust;
