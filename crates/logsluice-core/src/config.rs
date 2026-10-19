//! Configuration types and validation for windowed aggregate engine.

use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::{AggError, AggResult};

/// Emission mode for window aggregates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EmitMode {
    /// Emit delta slice records.
    #[default]
    DeltaSlice,
    /// Emit cumulative running totals.
    Cumulative,
}

/// Strategy for window emission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EmitStrategy {
    /// Emit only when window closes.
    #[default]
    OnClose,
    /// Emit periodically throughout window lifetime.
    Periodic,
}

/// Behavior when cardinality cap is reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum OnCap {
    /// Route excess events to overflow bucket.
    #[default]
    Overflow,
    /// Evict cold keys from memory.
    EvictCold,
}

/// Policy when disk space limit is reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum OnDiskFull {
    /// Route excess records to overflow bucket.
    #[default]
    OverflowBucket,
    /// Drop newest records.
    DropNewest,
    /// Block ingestion until disk space is reclaimed.
    Block,
}

/// Policy upon process shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum OnShutdown {
    /// Emit partial delta records before shutting down.
    #[default]
    EmitPartial,
    /// Persist dirty state to disk without emitting.
    Persist,
}

/// Supported aggregation operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregationKind {
    /// Count occurrences of events.
    Count,
    /// Sum metric field values.
    Sum,
    /// Find minimum metric field value.
    Min,
    /// Find maximum metric field value.
    Max,
}

/// Specification for a single aggregation metric.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregationSpec {
    /// Output field name for the aggregation result.
    pub name: String,
    /// Kind of aggregation.
    pub kind: AggregationKind,
    /// Source event field to aggregate (absent for count).
    #[serde(default)]
    pub field: Option<String>,
}

/// Emission timing configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmitConfig {
    /// Emission strategy (on_close or periodic).
    pub strategy: EmitStrategy,
    /// Periodic emission interval (required if strategy is periodic, forbidden if on_close).
    #[serde(default, with = "humantime_serde::option")]
    pub interval: Option<Duration>,
    /// Maximum jitter applied to window closure to spread flush spikes.
    #[serde(with = "humantime_serde")]
    pub close_jitter_max: Duration,
}

impl Default for EmitConfig {
    fn default() -> Self {
        Self {
            strategy: EmitStrategy::OnClose,
            interval: None,
            close_jitter_max: Duration::from_secs(600), // 10m
        }
    }
}

/// Storage engine configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Base directory for state storage.
    pub data_dir: PathBuf,
    /// Total cache memory budget across all shards.
    pub cache_size_total: u64,
    /// Maximum keys coalesced in L1 buffer before flush.
    pub l1_coalesce_max_keys: usize,
    /// Flush interval for L1 buffer.
    #[serde(with = "humantime_serde")]
    pub l1_flush_interval: Duration,
    /// Maximum memtable size for header storage.
    pub hdr_max_memtable_size: u64,
    /// Maximum memtable size for exemplar tail storage.
    pub tail_max_memtable_size: u64,
    /// Total disk budget for state.
    pub max_disk_bytes: u64,
    /// Disk space reserved for control and emergency operations.
    pub control_reserve_bytes: u64,
    /// Policy when disk budget is exhausted.
    pub on_disk_full: OnDiskFull,
    /// Whether to synchronously flush write-ahead logs.
    pub sync_wal: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        let max_disk_bytes = 50 * 1024 * 1024 * 1024; // 50 GiB
        Self {
            data_dir: PathBuf::from("/var/lib/vector/agg_state"),
            cache_size_total: 1536 * 1024 * 1024, // 1.5 GiB = 1_610_612_736
            l1_coalesce_max_keys: 100_000,
            l1_flush_interval: Duration::from_secs(30),
            hdr_max_memtable_size: 32 * 1024 * 1024, // 32 MiB
            tail_max_memtable_size: 16 * 1024 * 1024, // 16 MiB
            max_disk_bytes,
            control_reserve_bytes: max_disk_bytes / 10, // 5 GiB
            on_disk_full: OnDiskFull::OverflowBucket,
            sync_wal: true,
        }
    }
}

/// Window definition and lifecycle timing configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowConfig {
    /// Window duration.
    #[serde(with = "humantime_serde")]
    pub duration: Duration,
    /// Window alignment offset.
    #[serde(with = "humantime_serde")]
    pub offset: Duration,
    /// Event field containing the timestamp.
    pub timestamp_field: String,
    /// Maximum allowed clock skew between partitions.
    #[serde(with = "humantime_serde")]
    pub allowed_skew: Duration,
    /// Maximum lateness allowed for events before they are quarantined/dropped.
    #[serde(with = "humantime_serde")]
    pub allowed_lateness: Duration,
    /// Maximum drift into future before quarantining events.
    #[serde(with = "humantime_serde")]
    pub max_future_drift: Duration,
    /// Inactivity timeout before source watermark is marked idle.
    #[serde(with = "humantime_serde")]
    pub idle_timeout: Duration,
    /// Threshold for catching up watermark state.
    #[serde(with = "humantime_serde")]
    pub caught_up_threshold: Duration,
    /// Forced window closure wall-clock delay limit.
    #[serde(with = "humantime_serde")]
    pub max_close_delay: Duration,
    /// Emission interval for late event revisions.
    #[serde(with = "humantime_serde")]
    pub late_emit_interval: Duration,
    /// Maximum duration allowed for window closure tickets.
    #[serde(with = "humantime_serde")]
    pub max_closing_duration: Duration,
    /// Field identifying source producer when partition position is absent.
    pub source_id_field: String,
    /// Maximum distinct sources tracked for watermark.
    pub max_tracked_sources: usize,
    /// Emission settings.
    pub emit: EmitConfig,
}

impl Default for WindowConfig {
    fn default() -> Self {
        Self {
            duration: Duration::from_secs(86400), // 24h
            offset: Duration::ZERO,
            timestamp_field: "@timestamp".to_string(),
            allowed_skew: Duration::from_secs(30),
            allowed_lateness: Duration::from_secs(3600), // 1h
            max_future_drift: Duration::from_secs(300),  // 5m
            idle_timeout: Duration::from_secs(300),      // 5m
            caught_up_threshold: Duration::from_secs(120), // 2m
            max_close_delay: Duration::from_secs(7200),  // 2h
            late_emit_interval: Duration::from_secs(300), // 5m
            max_closing_duration: Duration::from_secs(7200), // 2h
            source_id_field: "observer.hostname".to_string(),
            max_tracked_sources: 10_000,
            emit: EmitConfig::default(),
        }
    }
}

/// Cardinality control configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CardinalityConfig {
    /// Maximum distinct aggregate keys allowed per window.
    pub max_keys_per_window: usize,
    /// Maximum fraction of total keys allowed for a single tenant/resource.
    pub max_resource_share: f64,
    /// Action taken when cardinality cap is reached.
    pub on_cap: OnCap,
    /// High-water mark fraction for cold eviction.
    pub evict_high_water: f64,
    /// Evaluation interval for eviction of cold keys.
    #[serde(with = "humantime_serde")]
    pub evict_interval: Duration,
}

impl Default for CardinalityConfig {
    fn default() -> Self {
        Self {
            max_keys_per_window: 2_000_000,
            max_resource_share: 0.25,
            on_cap: OnCap::Overflow,
            evict_high_water: 0.9,
            evict_interval: Duration::from_secs(60), // 1m
        }
    }
}

/// Output record generation configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputConfig {
    /// Output event field name indicating routing status.
    pub route_field: String,
    /// Identifier of the emitting pod/instance.
    pub emitter_id: String,
    /// Emission rate limit in records per second.
    pub max_emit_rate_per_sec: u64,
    /// Maximum slice records packed in a single delivery ticket.
    pub max_records_per_ticket: usize,
    /// Maximum slice records packed in an output envelope.
    pub max_records_per_envelope: usize,
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self {
            route_field: "_wa_route".to_string(),
            emitter_id: "${HOSTNAME}".to_string(),
            max_emit_rate_per_sec: 100_000,
            max_records_per_ticket: 500,
            max_records_per_envelope: 500,
        }
    }
}

/// Shutdown handling configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShutdownConfig {
    /// Action to take on process shutdown.
    pub on_shutdown: OnShutdown,
    /// Maximum time allowed to drain pending emissions on shutdown.
    #[serde(with = "humantime_serde")]
    pub drain_timeout: Duration,
}

impl Default for ShutdownConfig {
    fn default() -> Self {
        Self {
            on_shutdown: OnShutdown::EmitPartial,
            drain_timeout: Duration::from_secs(50),
        }
    }
}

/// Complete configuration for the windowed aggregate engine.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EngineConfig {
    /// Number of worker shards.
    pub shards: u16,
    /// Emission mode (delta slice or cumulative).
    pub emit_mode: EmitMode,
    /// Storage subsystem configuration.
    pub storage: StorageConfig,
    /// Window lifecycle configuration.
    pub window: WindowConfig,
    /// Cardinality control configuration.
    #[serde(alias = "cardinality_control")]
    pub cardinality: CardinalityConfig,
    /// Output generation configuration.
    pub output: OutputConfig,
    /// Shutdown behavior configuration.
    pub shutdown: ShutdownConfig,
    /// Configured metric aggregations.
    #[serde(default)]
    pub aggregations: Vec<AggregationSpec>,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            shards: 4,
            emit_mode: EmitMode::DeltaSlice,
            storage: StorageConfig::default(),
            window: WindowConfig::default(),
            cardinality: CardinalityConfig::default(),
            output: OutputConfig::default(),
            shutdown: ShutdownConfig::default(),
            aggregations: Vec::new(),
        }
    }
}

impl EngineConfig {
    /// Validate configuration invariants according to design specification §8.1.
    pub fn validate(&self) -> AggResult<()> {
        if self.shards == 0 {
            return Err(AggError::Config("shards must be > 0".to_string()));
        }

        match self.window.emit.strategy {
            EmitStrategy::Periodic => match self.window.emit.interval {
                None => {
                    return Err(AggError::Config(
                        "window.emit.interval is required when emit.strategy is periodic"
                            .to_string(),
                    ));
                }
                Some(i) => {
                    if i.is_zero() || i >= self.window.duration {
                        return Err(AggError::Config(format!(
                            "window.emit.interval ({:?}) must be > 0 and < window.duration ({:?})",
                            i, self.window.duration
                        )));
                    }
                }
            },
            EmitStrategy::OnClose => {
                if self.window.emit.interval.is_some() {
                    return Err(AggError::Config(
                        "window.emit.interval must not be set when emit.strategy is on_close"
                            .to_string(),
                    ));
                }
            }
        }

        if self.window.max_close_delay <= self.window.allowed_skew {
            return Err(AggError::Config(format!(
                "window.max_close_delay ({:?}) must be greater than window.allowed_skew ({:?})",
                self.window.max_close_delay, self.window.allowed_skew
            )));
        }

        if self.window.allowed_lateness > self.window.max_close_delay {
            return Err(AggError::Config(format!(
                "window.allowed_lateness ({:?}) must be less than or equal to window.max_close_delay ({:?})",
                self.window.allowed_lateness, self.window.max_close_delay
            )));
        }

        if self.storage.control_reserve_bytes >= self.storage.max_disk_bytes {
            return Err(AggError::Config(format!(
                "storage.control_reserve_bytes ({}) must be strictly less than storage.max_disk_bytes ({})",
                self.storage.control_reserve_bytes, self.storage.max_disk_bytes
            )));
        }

        if !(self.cardinality.max_resource_share > 0.0
            && self.cardinality.max_resource_share <= 1.0)
        {
            return Err(AggError::Config(format!(
                "cardinality.max_resource_share ({}) must be in (0.0, 1.0]",
                self.cardinality.max_resource_share
            )));
        }

        for spec in &self.aggregations {
            match spec.kind {
                AggregationKind::Sum | AggregationKind::Min | AggregationKind::Max => {
                    if spec.field.is_none() {
                        return Err(AggError::Config(format!(
                            "aggregations[{}].field is required for aggregation kind {:?}",
                            spec.name, spec.kind
                        )));
                    }
                }
                AggregationKind::Count => {
                    if spec.field.is_some() {
                        return Err(AggError::Config(format!(
                            "aggregations[{}].field must not be set for count aggregation",
                            spec.name
                        )));
                    }
                }
            }
        }

        Ok(())
    }
}
