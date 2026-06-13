//! `ray-scheduler` — Global distributed scheduler for the Ray cluster.
//!
//! The global scheduler sits between the GCS and the per-node Raylets.
//! It is responsible for:
//! - Maintaining a global view of cluster resources (from GCS)
//! - Making placement decisions for tasks that cannot be scheduled locally
//! - Implementing scheduling policies (spread, pack, locality-aware)
//! - Handling node failures and task re-scheduling
//!
//! Architecture:
//! ```text
//!   ┌──────────┐     ┌──────────────┐     ┌──────────┐
//!   │  Worker  │────▶│  Raylet      │────▶│  Global  │
//!   │          │     │  (local)     │     │  Sched   │
//!   └──────────┘     └──────────────┘     └──────────┘
//!                                                │
//!                                          ┌─────▼─────┐
//!                                          │   GCS     │
//!                                          └───────────┘
//! ```

pub mod global;
pub mod policy;

pub use global::GlobalScheduler;
pub use policy::SchedulingPolicy;
