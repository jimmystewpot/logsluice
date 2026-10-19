//! Windowed Aggregate Core crate.
//!
//! Core primitives, storage, and worker logic for time-windowed log/metric aggregation.

pub mod cardinality;
pub mod codec;
pub mod config;
pub mod emission;
pub mod error;
pub mod key;
pub mod l1;
pub mod position;
pub mod resource;
pub mod simd;
pub mod store;
pub mod watermark;
pub mod window;
pub mod worker;
