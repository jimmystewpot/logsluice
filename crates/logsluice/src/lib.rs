//! Standalone async Tokio pipeline, OTLP/JSON log router, and slice renderer for `logsluice`.

pub mod config;
pub mod event;
pub mod extract;
pub mod metrics;
pub mod pipeline;
pub mod render;
pub mod router;

/// Compatibility module alias for [`pipeline`].
pub use crate::pipeline as transform;

pub use crate::config::{
    AdapterConfig, EnvelopeMode, SluiceConfig, StorageAdapterConfig, StorageConfig,
    WindowedAggregateConfig,
};
pub use crate::event::{
    AckFinalizer, AckFinalizers, AckNotifier, AckStatus, FieldPath, LogEvent, ObjectMap, Value,
};
pub use crate::extract::{MetricValue, metric_value};
pub use crate::pipeline::{SluicePipeline, WindowedAggregate};
pub use crate::router::{Router, RouterOutput, RouterStats};
