//! Pipeline configuration, YAML/JSON schema, and validation.

use std::collections::BTreeSet;
use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use indexmap::IndexMap;
use logsluice_core::codec::plan::AggregationPlan;
use logsluice_core::config::{
    AggregationKind, AggregationSpec, CardinalityConfig, EmitConfig, EmitMode as CoreEmitMode,
    EmitStrategy as CoreEmitStrategy, EngineConfig, OnCap as CoreOnCap,
    OnDiskFull as CoreOnDiskFull, OnShutdown as CoreOnShutdown, OutputConfig as CoreOutputConfig,
    ShutdownConfig as CoreShutdownConfig, StorageConfig as CoreStorageConfig, WindowConfig,
};
use logsluice_core::error::{AggError, AggResult};
use logsluice_core::simd::SimdMode as CoreSimdMode;
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

use crate::event::FieldPath;

/// Ingestion envelope processing mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvelopeMode {
    /// Automatically detect envelope format from event structure.
    #[default]
    Auto,
    /// Native flat/nested JSON log event format.
    #[serde(alias = "native")]
    VectorNative,
    /// OpenTelemetry Logs envelope structure (`resourceLogs`).
    Otlp,
}

impl EnvelopeMode {
    /// Alias for [`EnvelopeMode::VectorNative`].
    #[allow(non_upper_case_globals)]
    pub const Native: Self = Self::VectorNative;
}

/// Emission mode for window aggregates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EmitMode {
    /// Emit delta slice records.
    #[default]
    DeltaSlice,
    /// Emit cumulative running totals.
    Cumulative,
}

impl From<EmitMode> for CoreEmitMode {
    fn from(mode: EmitMode) -> Self {
        match mode {
            EmitMode::DeltaSlice => CoreEmitMode::DeltaSlice,
            EmitMode::Cumulative => CoreEmitMode::Cumulative,
        }
    }
}

/// Hardware SIMD acceleration dispatch mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SimdMode {
    /// Automatically detect CPU features at runtime.
    #[default]
    Auto,
    /// Force scalar implementation.
    Scalar,
}

impl From<SimdMode> for CoreSimdMode {
    fn from(mode: SimdMode) -> Self {
        match mode {
            SimdMode::Auto => CoreSimdMode::Auto,
            SimdMode::Scalar => CoreSimdMode::Scalar,
        }
    }
}

/// Acceleration configuration for SIMD vectorization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct AccelerationConfig {
    /// SIMD dispatch mode (auto or scalar).
    #[serde(default)]
    pub simd_mode: SimdMode,
}

/// Policy when disk budget is exhausted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
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

impl From<OnDiskFull> for CoreOnDiskFull {
    fn from(o: OnDiskFull) -> Self {
        match o {
            OnDiskFull::OverflowBucket => CoreOnDiskFull::OverflowBucket,
            OnDiskFull::DropNewest => CoreOnDiskFull::DropNewest,
            OnDiskFull::Block => CoreOnDiskFull::Block,
        }
    }
}

/// Behavior when cardinality cap is reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnCap {
    /// Route excess events to overflow bucket.
    #[default]
    Overflow,
    /// Evict cold keys from memory.
    EvictCold,
}

impl From<OnCap> for CoreOnCap {
    fn from(c: OnCap) -> Self {
        match c {
            OnCap::Overflow => CoreOnCap::Overflow,
            OnCap::EvictCold => CoreOnCap::EvictCold,
        }
    }
}

/// Strategy for window emission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EmitStrategy {
    /// Emit only when window closes.
    #[default]
    OnClose,
    /// Emit periodically throughout window lifetime.
    Periodic,
}

impl From<EmitStrategy> for CoreEmitStrategy {
    fn from(s: EmitStrategy) -> Self {
        match s {
            EmitStrategy::OnClose => CoreEmitStrategy::OnClose,
            EmitStrategy::Periodic => CoreEmitStrategy::Periodic,
        }
    }
}

/// Emission timing configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmitAdapterConfig {
    /// Emission strategy (on_close or periodic).
    #[serde(default)]
    pub strategy: EmitStrategy,
    /// Periodic emission interval.
    #[serde(default, with = "humantime_serde::option")]
    pub interval: Option<Duration>,
    /// Maximum jitter applied to window closure to spread flush spikes.
    #[serde(default = "default_close_jitter_max", with = "humantime_serde")]
    pub close_jitter_max: Duration,
}

fn default_close_jitter_max() -> Duration {
    Duration::from_secs(600)
}

impl Default for EmitAdapterConfig {
    fn default() -> Self {
        Self {
            strategy: EmitStrategy::OnClose,
            interval: None,
            close_jitter_max: default_close_jitter_max(),
        }
    }
}

impl From<EmitAdapterConfig> for EmitConfig {
    fn from(e: EmitAdapterConfig) -> Self {
        EmitConfig {
            strategy: e.strategy.into(),
            interval: e.interval,
            close_jitter_max: e.close_jitter_max,
        }
    }
}

/// Cardinality control configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CardinalityAdapterConfig {
    /// Maximum distinct aggregate keys allowed per window.
    #[serde(default = "default_max_keys_per_window")]
    pub max_keys_per_window: usize,
    /// Maximum fraction of total keys allowed for a single tenant/resource.
    #[serde(default = "default_max_resource_share")]
    pub max_resource_share: f64,
    /// Action taken when cardinality cap is reached.
    #[serde(default)]
    pub on_cap: OnCap,
    /// High-water mark fraction for cold eviction.
    #[serde(default = "default_evict_high_water")]
    pub evict_high_water: f64,
    /// Evaluation interval for eviction of cold keys.
    #[serde(default = "default_evict_interval", with = "humantime_serde")]
    pub evict_interval: Duration,
}

fn default_max_keys_per_window() -> usize {
    2_000_000
}
fn default_max_resource_share() -> f64 {
    0.25
}
fn default_evict_high_water() -> f64 {
    0.9
}
fn default_evict_interval() -> Duration {
    Duration::from_secs(60)
}

impl Default for CardinalityAdapterConfig {
    fn default() -> Self {
        Self {
            max_keys_per_window: default_max_keys_per_window(),
            max_resource_share: default_max_resource_share(),
            on_cap: OnCap::Overflow,
            evict_high_water: default_evict_high_water(),
            evict_interval: default_evict_interval(),
        }
    }
}

impl From<CardinalityAdapterConfig> for CardinalityConfig {
    fn from(c: CardinalityAdapterConfig) -> Self {
        CardinalityConfig {
            max_keys_per_window: c.max_keys_per_window,
            max_resource_share: c.max_resource_share,
            on_cap: c.on_cap.into(),
            evict_high_water: c.evict_high_water,
            evict_interval: c.evict_interval,
        }
    }
}

/// Output record generation configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputAdapterConfig {
    /// Output event field name indicating routing status.
    #[serde(default = "default_route_field")]
    pub route_field: String,
    /// Identifier of the emitting pod/instance.
    #[serde(default = "default_emitter_id")]
    pub emitter_id: String,
    /// Emission rate limit in records per second.
    #[serde(default = "default_max_emit_rate_per_sec")]
    pub max_emit_rate_per_sec: u64,
    /// Maximum slice records packed in a single delivery ticket.
    #[serde(default = "default_records_per")]
    pub max_records_per_ticket: usize,
    /// Maximum slice records packed in an output envelope.
    #[serde(default = "default_records_per")]
    pub max_records_per_envelope: usize,
}

fn default_route_field() -> String {
    "_wa_route".to_string()
}
fn default_emitter_id() -> String {
    "${HOSTNAME}".to_string()
}
fn default_max_emit_rate_per_sec() -> u64 {
    100_000
}
fn default_records_per() -> usize {
    500
}

impl Default for OutputAdapterConfig {
    fn default() -> Self {
        Self {
            route_field: default_route_field(),
            emitter_id: default_emitter_id(),
            max_emit_rate_per_sec: default_max_emit_rate_per_sec(),
            max_records_per_ticket: default_records_per(),
            max_records_per_envelope: default_records_per(),
        }
    }
}

impl From<OutputAdapterConfig> for CoreOutputConfig {
    fn from(o: OutputAdapterConfig) -> Self {
        CoreOutputConfig {
            route_field: o.route_field,
            emitter_id: o.emitter_id,
            max_emit_rate_per_sec: o.max_emit_rate_per_sec,
            max_records_per_ticket: o.max_records_per_ticket,
            max_records_per_envelope: o.max_records_per_envelope,
        }
    }
}

/// Action to take on process shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnShutdown {
    /// Emit partial delta records before shutting down.
    #[default]
    EmitPartial,
    /// Persist dirty state to disk without emitting.
    Persist,
}

impl From<OnShutdown> for CoreOnShutdown {
    fn from(o: OnShutdown) -> Self {
        match o {
            OnShutdown::EmitPartial => CoreOnShutdown::EmitPartial,
            OnShutdown::Persist => CoreOnShutdown::Persist,
        }
    }
}

/// Shutdown handling configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShutdownAdapterConfig {
    /// Action to take on process shutdown.
    #[serde(default)]
    pub on_shutdown: OnShutdown,
    /// Maximum time allowed to drain pending emissions on shutdown.
    #[serde(default = "default_drain_timeout", with = "humantime_serde")]
    pub drain_timeout: Duration,
}

fn default_drain_timeout() -> Duration {
    Duration::from_secs(50)
}

impl Default for ShutdownAdapterConfig {
    fn default() -> Self {
        Self {
            on_shutdown: OnShutdown::EmitPartial,
            drain_timeout: default_drain_timeout(),
        }
    }
}

impl From<ShutdownAdapterConfig> for CoreShutdownConfig {
    fn from(s: ShutdownAdapterConfig) -> Self {
        CoreShutdownConfig {
            on_shutdown: s.on_shutdown.into(),
            drain_timeout: s.drain_timeout,
        }
    }
}

/// Source position extraction configuration for partition tracking and deduplication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourcePositionConfig {
    /// Field path containing topic name.
    #[serde(default = "default_topic_field")]
    pub topic_field: String,
    /// Field path containing partition number.
    #[serde(default = "default_partition_field")]
    pub partition_field: String,
    /// Field path containing partition offset.
    #[serde(default = "default_offset_field")]
    pub offset_field: String,
}

fn default_topic_field() -> String {
    "topic".to_string()
}
fn default_partition_field() -> String {
    "partition".to_string()
}
fn default_offset_field() -> String {
    "offset".to_string()
}

impl Default for SourcePositionConfig {
    fn default() -> Self {
        Self {
            topic_field: default_topic_field(),
            partition_field: default_partition_field(),
            offset_field: default_offset_field(),
        }
    }
}

/// Storage configuration supporting human-readable byte sizes (e.g. `1.5GiB`, `50GiB`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageAdapterConfig {
    /// Base directory for state storage.
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
    /// Total cache memory budget across all shards.
    #[serde(
        default = "default_cache_size_total",
        deserialize_with = "parse_byte_size"
    )]
    pub cache_size_total: u64,
    /// Maximum keys coalesced in L1 buffer before flush.
    #[serde(default = "default_l1_coalesce_max_keys")]
    pub l1_coalesce_max_keys: usize,
    /// Flush interval for L1 buffer.
    #[serde(default = "default_l1_flush_interval", with = "humantime_serde")]
    pub l1_flush_interval: Duration,
    /// Maximum memtable size for header storage.
    #[serde(
        default = "default_hdr_max_memtable_size",
        deserialize_with = "parse_byte_size"
    )]
    pub hdr_max_memtable_size: u64,
    /// Maximum memtable size for exemplar tail storage.
    #[serde(
        default = "default_tail_max_memtable_size",
        deserialize_with = "parse_byte_size"
    )]
    pub tail_max_memtable_size: u64,
    /// Total disk budget for state.
    #[serde(
        default = "default_max_disk_bytes",
        deserialize_with = "parse_byte_size"
    )]
    pub max_disk_bytes: u64,
    /// Disk space reserved for control and emergency operations.
    #[serde(
        default = "default_control_reserve_bytes",
        deserialize_with = "parse_byte_size"
    )]
    pub control_reserve_bytes: u64,
    /// Policy when disk budget is exhausted.
    #[serde(default)]
    pub on_disk_full: OnDiskFull,
    /// Whether to synchronously flush write-ahead logs.
    #[serde(default = "default_sync_wal")]
    pub sync_wal: bool,
    /// Background compaction and flush worker threads per shard database.
    #[serde(default = "default_storage_worker_threads")]
    pub worker_threads: usize,
    /// Maximum size of all memtables in bytes across keyspaces for this shard database (`None` = unlimited).
    #[serde(default, deserialize_with = "parse_byte_size_option")]
    pub max_write_buffer_bytes: Option<u64>,
    /// Upper limit of cached file descriptors per shard database (`None` = engine default).
    #[serde(default)]
    pub max_cached_files: Option<usize>,
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("/var/lib/vector/agg_state")
}
fn default_cache_size_total() -> u64 {
    1536 * 1024 * 1024 // 1.5 GiB
}
fn default_l1_coalesce_max_keys() -> usize {
    100_000
}
fn default_l1_flush_interval() -> Duration {
    Duration::from_secs(30)
}
fn default_hdr_max_memtable_size() -> u64 {
    32 * 1024 * 1024 // 32 MiB
}
fn default_tail_max_memtable_size() -> u64 {
    16 * 1024 * 1024 // 16 MiB
}
fn default_max_disk_bytes() -> u64 {
    50 * 1024 * 1024 * 1024 // 50 GiB
}
fn default_control_reserve_bytes() -> u64 {
    5 * 1024 * 1024 * 1024 // 5 GiB
}
fn default_sync_wal() -> bool {
    true
}
fn default_storage_worker_threads() -> usize {
    2
}

impl Default for StorageAdapterConfig {
    fn default() -> Self {
        Self {
            data_dir: default_data_dir(),
            cache_size_total: default_cache_size_total(),
            l1_coalesce_max_keys: default_l1_coalesce_max_keys(),
            l1_flush_interval: default_l1_flush_interval(),
            hdr_max_memtable_size: default_hdr_max_memtable_size(),
            tail_max_memtable_size: default_tail_max_memtable_size(),
            max_disk_bytes: default_max_disk_bytes(),
            control_reserve_bytes: default_control_reserve_bytes(),
            on_disk_full: OnDiskFull::OverflowBucket,
            sync_wal: true,
            worker_threads: default_storage_worker_threads(),
            max_write_buffer_bytes: None,
            max_cached_files: None,
        }
    }
}

/// Helper function to parse byte size strings or numbers.
///
/// # Errors
/// Returns a deserialization error if the value is negative, unparseable, or uses an unknown unit.
pub fn parse_byte_size<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    struct ByteSizeVisitor;

    impl Visitor<'_> for ByteSizeVisitor {
        type Value = u64;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a byte size string (e.g. '1.5GiB', '50GiB') or an integer")
        }

        fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E> {
            Ok(v)
        }

        fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            if v < 0 {
                Err(E::custom("byte size cannot be negative"))
            } else {
                Ok(v as u64)
            }
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            parse_bytes_str(v).map_err(E::custom)
        }
    }

    deserializer.deserialize_any(ByteSizeVisitor)
}

fn parse_bytes_str(s: &str) -> Result<u64, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty byte size string".to_string());
    }

    let end_idx = s
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(s.len());
    let (num_str, unit_str) = s.split_at(end_idx);

    let num: f64 = num_str
        .parse()
        .map_err(|e| format!("invalid number in byte size '{s}': {e}"))?;

    let unit = unit_str.trim().to_ascii_lowercase();
    let multiplier: f64 = match unit.as_str() {
        "" | "b" | "bytes" | "byte" => 1.0,
        "k" | "kb" => 1_000.0,
        "kib" => 1024.0,
        "m" | "mb" => 1_000_000.0,
        "mib" => 1024.0 * 1024.0,
        "g" | "gb" => 1_000_000_000.0,
        "gib" => 1024.0 * 1024.0 * 1024.0,
        "t" | "tb" => 1_000_000_000_000.0,
        "tib" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        _ => return Err(format!("unknown byte unit '{unit_str}' in '{s}'")),
    };

    let total = num * multiplier;
    if total < 0.0 || total > u64::MAX as f64 {
        return Err(format!("byte size out of range: '{s}'"));
    }

    Ok(total.round() as u64)
}

/// Helper function to parse optional byte size strings or numbers.
///
/// # Errors
/// Returns a deserialization error if the value is negative, unparseable, or uses an unknown unit.
pub fn parse_byte_size_option<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    struct ByteSizeField(#[serde(deserialize_with = "parse_byte_size")] u64);

    Option::<ByteSizeField>::deserialize(deserializer).map(|opt| opt.map(|w| w.0))
}

/// Window adapter configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowAdapterConfig {
    /// Window type (e.g. "tumbling").
    #[serde(default = "default_window_type")]
    pub r#type: String,
    /// Window duration.
    #[serde(default = "default_duration", with = "humantime_serde")]
    pub duration: Duration,
    /// Window offset.
    #[serde(default = "default_offset", with = "humantime_serde")]
    pub offset: Duration,
    /// Field containing event timestamp.
    #[serde(default = "default_timestamp_field")]
    pub timestamp_field: String,
    /// Allowed clock skew.
    #[serde(default = "default_allowed_skew", with = "humantime_serde")]
    pub allowed_skew: Duration,
    /// Allowed lateness before late routing.
    #[serde(default = "default_allowed_lateness", with = "humantime_serde")]
    pub allowed_lateness: Duration,
    /// Maximum future drift before quarantine.
    #[serde(default = "default_max_future_drift", with = "humantime_serde")]
    pub max_future_drift: Duration,
    /// Inactivity timeout for watermark idle sources.
    #[serde(default = "default_idle_timeout", with = "humantime_serde")]
    pub idle_timeout: Duration,
    /// Watermark catch up threshold.
    #[serde(default = "default_caught_up_threshold", with = "humantime_serde")]
    pub caught_up_threshold: Duration,
    /// Maximum close delay forced wall-clock timeout.
    #[serde(default = "default_max_close_delay", with = "humantime_serde")]
    pub max_close_delay: Duration,
    /// Interval between late emission checks.
    #[serde(default = "default_late_emit_interval", with = "humantime_serde")]
    pub late_emit_interval: Duration,
    /// Maximum duration allowed for closing ticket delivery before spill.
    #[serde(default = "default_max_closing_duration", with = "humantime_serde")]
    pub max_closing_duration: Duration,
    /// Source ID field when source position is absent.
    #[serde(default = "default_source_id_field")]
    pub source_id_field: String,
    /// Maximum number of tracked sources.
    #[serde(default = "default_max_tracked_sources")]
    pub max_tracked_sources: usize,
    /// Emission settings.
    #[serde(default)]
    pub emit: EmitAdapterConfig,
}

fn default_window_type() -> String {
    "tumbling".to_string()
}
fn default_duration() -> Duration {
    Duration::from_secs(86400)
}
fn default_offset() -> Duration {
    Duration::ZERO
}
fn default_timestamp_field() -> String {
    "@timestamp".to_string()
}
fn default_allowed_skew() -> Duration {
    Duration::from_secs(30)
}
fn default_allowed_lateness() -> Duration {
    Duration::from_secs(3600)
}
fn default_max_future_drift() -> Duration {
    Duration::from_secs(300)
}
fn default_idle_timeout() -> Duration {
    Duration::from_secs(300)
}
fn default_caught_up_threshold() -> Duration {
    Duration::from_secs(120)
}
fn default_max_close_delay() -> Duration {
    Duration::from_secs(7200)
}
fn default_late_emit_interval() -> Duration {
    Duration::from_secs(300)
}
fn default_max_closing_duration() -> Duration {
    Duration::from_secs(7200)
}
fn default_source_id_field() -> String {
    "observer.hostname".to_string()
}
fn default_max_tracked_sources() -> usize {
    10000
}

impl Default for WindowAdapterConfig {
    fn default() -> Self {
        Self {
            r#type: default_window_type(),
            duration: default_duration(),
            offset: default_offset(),
            timestamp_field: default_timestamp_field(),
            allowed_skew: default_allowed_skew(),
            allowed_lateness: default_allowed_lateness(),
            max_future_drift: default_max_future_drift(),
            idle_timeout: default_idle_timeout(),
            caught_up_threshold: default_caught_up_threshold(),
            max_close_delay: default_max_close_delay(),
            late_emit_interval: default_late_emit_interval(),
            max_closing_duration: default_max_closing_duration(),
            source_id_field: default_source_id_field(),
            max_tracked_sources: default_max_tracked_sources(),
            emit: EmitAdapterConfig::default(),
        }
    }
}

/// Exemplar body policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BodyPolicy {
    /// Keep first observed exemplar body.
    #[default]
    #[serde(alias = "keep_exemplar")]
    KeepFirstExemplar,
    /// Replace body with template placeholder.
    ReplaceWithTemplate,
    /// Ignore body.
    #[serde(alias = "drop")]
    Ignore,
}

impl BodyPolicy {
    /// Alias for [`BodyPolicy::KeepFirstExemplar`].
    #[allow(non_upper_case_globals)]
    pub const KeepExemplar: Self = Self::KeepFirstExemplar;
    /// Alias for [`BodyPolicy::Ignore`].
    #[allow(non_upper_case_globals)]
    pub const Drop: Self = Self::Ignore;
}

/// Unmatched log attributes policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnmatchedLogAttributes {
    /// Drop unmatched log attributes.
    #[default]
    Drop,
    /// Keep first observed exemplar attributes.
    KeepFirst,
}

/// Type alias for [`UnmatchedLogAttributes`].
pub type UnmatchedPolicy = UnmatchedLogAttributes;

/// Attributes policy configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttributesPolicyConfig {
    /// Whether to preserve OTLP resource and instrumentation scope.
    #[serde(default = "default_true")]
    pub preserve_resource_and_scope: bool,
    /// Resource attributes to exclude from canonical hashing and grouping.
    #[serde(default)]
    pub resource_attributes_exclude: BTreeSet<String>,
    /// Exemplar body retention policy.
    #[serde(default)]
    pub body_policy: BodyPolicy,
    /// Target field for log template when template policy is enabled.
    #[serde(default = "default_template_target_field")]
    pub template_target_field: String,
    /// Policy for unmatched log attributes.
    #[serde(default)]
    pub unmatched_log_attributes: UnmatchedLogAttributes,
    /// Attributes excluded from exemplar.
    #[serde(default)]
    pub exclude_attributes: BTreeSet<String>,
}

fn default_true() -> bool {
    true
}
fn default_template_target_field() -> String {
    "log.template".to_string()
}

impl Default for AttributesPolicyConfig {
    fn default() -> Self {
        Self {
            preserve_resource_and_scope: true,
            resource_attributes_exclude: BTreeSet::new(),
            body_policy: BodyPolicy::KeepFirstExemplar,
            template_target_field: default_template_target_field(),
            unmatched_log_attributes: UnmatchedLogAttributes::Drop,
            exclude_attributes: BTreeSet::new(),
        }
    }
}

/// Condition under which a normalizer is applied.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WhenCondition {
    /// Event field name to inspect.
    pub field: String,
    /// Required equality value.
    pub equals: String,
}

/// Normalizer specification for a dimension or body field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NormalizerConfig {
    /// IP CIDR prefix masking.
    IpCidr {
        /// Number of bits preserved for IPv4 addresses (0..=32).
        #[serde(default = "default_ipv4_prefix")]
        ipv4_prefix: u8,
        /// Number of bits preserved for IPv6 addresses (0..=128).
        #[serde(default = "default_ipv6_prefix")]
        ipv6_prefix: u8,
        /// Optional condition for applying this normalizer.
        #[serde(default)]
        when: Option<WhenCondition>,
    },
    /// Log message structural fingerprinting.
    LogFingerprint,
}

fn default_ipv4_prefix() -> u8 {
    24
}
fn default_ipv6_prefix() -> u8 {
    48
}

/// Definition of an aggregation in configuration (supports simple name or mapping).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AggregationDefinition {
    /// Simple count string: `count`.
    Simple(String),
    /// Detailed definition map, e.g. `{ sum: "network.bytes" }`.
    Detailed(IndexMap<String, String>),
}

/// Primary configuration for the standalone `logsluice` windowed aggregation pipeline.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SluiceConfig {
    /// Optional component type identifier (for host config compatibility).
    #[serde(default)]
    pub r#type: Option<String>,
    /// Optional upstream component inputs (for host config compatibility).
    #[serde(default)]
    pub inputs: Option<Vec<String>>,
    /// Ingestion envelope mode.
    #[serde(default)]
    pub envelope_mode: EnvelopeMode,
    /// Emission mode (delta slice or cumulative).
    #[serde(default)]
    pub emit_mode: EmitMode,
    /// Shard count.
    #[serde(default = "default_shards")]
    pub shards: u16,
    /// Hardware SIMD acceleration configuration.
    #[serde(default)]
    pub acceleration: AccelerationConfig,
    /// Source position extraction configuration.
    #[serde(default)]
    pub source_position: Option<SourcePositionConfig>,
    /// Storage configuration.
    #[serde(default)]
    pub storage: StorageAdapterConfig,
    /// Window configuration.
    pub window: WindowAdapterConfig,
    /// Cardinality control configuration.
    #[serde(default, alias = "cardinality_control")]
    pub cardinality: CardinalityAdapterConfig,
    /// Attribute handling policy.
    #[serde(default)]
    pub attributes_policy: AttributesPolicyConfig,
    /// Normalizers applied to group by dimensions and body.
    #[serde(default)]
    pub normalizers: IndexMap<String, NormalizerConfig>,
    /// Group by dimension field paths.
    #[serde(default)]
    pub group_by: Vec<String>,
    /// Configured metric aggregations.
    #[serde(default)]
    pub aggregations: IndexMap<String, AggregationDefinition>,
    /// Output generation configuration.
    #[serde(default)]
    pub output: OutputAdapterConfig,
    /// Shutdown handling configuration.
    #[serde(default)]
    pub shutdown: ShutdownAdapterConfig,
}

/// Compatibility type alias for [`SluiceConfig`].
pub type AdapterConfig = SluiceConfig;

/// Compatibility type alias for [`SluiceConfig`].
pub type WindowedAggregateConfig = SluiceConfig;

/// Compatibility type alias for [`StorageAdapterConfig`].
pub type StorageConfig = StorageAdapterConfig;

fn default_shards() -> u16 {
    4
}

impl Default for SluiceConfig {
    fn default() -> Self {
        Self {
            r#type: None,
            inputs: None,
            envelope_mode: EnvelopeMode::default(),
            emit_mode: EmitMode::default(),
            shards: default_shards(),
            acceleration: AccelerationConfig::default(),
            source_position: None,
            storage: StorageAdapterConfig::default(),
            window: WindowAdapterConfig::default(),
            cardinality: CardinalityAdapterConfig::default(),
            attributes_policy: AttributesPolicyConfig::default(),
            normalizers: IndexMap::new(),
            group_by: Vec::new(),
            aggregations: IndexMap::new(),
            output: OutputAdapterConfig::default(),
            shutdown: ShutdownAdapterConfig::default(),
        }
    }
}

impl SluiceConfig {
    /// Compiles and validates the core [`EngineConfig`] from this configuration.
    ///
    /// # Errors
    /// Returns [`AggError::Config`] if any aggregation definition, field path, or engine invariant is invalid.
    pub fn engine(&self) -> AggResult<EngineConfig> {
        let _ = FieldPath::parse(&self.window.timestamp_field).map_err(AggError::Config)?;
        let _ = FieldPath::parse(&self.window.source_id_field).map_err(AggError::Config)?;
        if !self.output.route_field.is_empty() {
            let _ = FieldPath::parse(&self.output.route_field).map_err(AggError::Config)?;
        }
        if !self.attributes_policy.template_target_field.is_empty() {
            let _ = FieldPath::parse(&self.attributes_policy.template_target_field)
                .map_err(AggError::Config)?;
        }
        if let Some(ref sp) = self.source_position {
            let _ = FieldPath::parse(&sp.topic_field).map_err(AggError::Config)?;
            let _ = FieldPath::parse(&sp.partition_field).map_err(AggError::Config)?;
            let _ = FieldPath::parse(&sp.offset_field).map_err(AggError::Config)?;
        }
        for dim in &self.group_by {
            let _ = FieldPath::parse(dim).map_err(AggError::Config)?;
        }
        for norm in self.normalizers.values() {
            if let NormalizerConfig::IpCidr {
                when: Some(cond), ..
            } = norm
            {
                let _ = FieldPath::parse(&cond.field).map_err(AggError::Config)?;
            }
        }

        let mut specs = Vec::with_capacity(self.aggregations.len());

        for (name, def) in &self.aggregations {
            let _ = FieldPath::parse(name).map_err(AggError::Config)?;
            match def {
                AggregationDefinition::Simple(kind_str) => match kind_str.as_str() {
                    "count" => specs.push(AggregationSpec {
                        name: name.clone(),
                        kind: AggregationKind::Count,
                        field: None,
                    }),
                    other => {
                        return Err(AggError::Config(format!(
                            "unsupported aggregation kind '{other}' for metric '{name}'"
                        )));
                    }
                },
                AggregationDefinition::Detailed(map) => {
                    if map.is_empty() {
                        return Err(AggError::Config(format!(
                            "empty aggregation definition for metric '{name}'"
                        )));
                    }

                    let mut found = false;
                    for (kind_str, field_str) in map {
                        let _ = FieldPath::parse(field_str).map_err(AggError::Config)?;
                        match kind_str.as_str() {
                            "count" => {
                                specs.push(AggregationSpec {
                                    name: name.clone(),
                                    kind: AggregationKind::Count,
                                    field: None,
                                });
                                found = true;
                            }
                            "sum" => {
                                specs.push(AggregationSpec {
                                    name: name.clone(),
                                    kind: AggregationKind::Sum,
                                    field: Some(field_str.clone()),
                                });
                                found = true;
                            }
                            "min" => {
                                specs.push(AggregationSpec {
                                    name: name.clone(),
                                    kind: AggregationKind::Min,
                                    field: Some(field_str.clone()),
                                });
                                found = true;
                            }
                            "max" => {
                                specs.push(AggregationSpec {
                                    name: name.clone(),
                                    kind: AggregationKind::Max,
                                    field: Some(field_str.clone()),
                                });
                                found = true;
                            }
                            other => {
                                return Err(AggError::Config(format!(
                                    "unsupported aggregation kind '{other}' for metric '{name}'"
                                )));
                            }
                        }
                    }

                    if !found {
                        return Err(AggError::Config(format!(
                            "no valid aggregation kind found for metric '{name}'"
                        )));
                    }
                }
            }
        }

        let storage = CoreStorageConfig {
            data_dir: self.storage.data_dir.clone(),
            cache_size_total: self.storage.cache_size_total,
            l1_coalesce_max_keys: self.storage.l1_coalesce_max_keys,
            l1_flush_interval: self.storage.l1_flush_interval,
            hdr_max_memtable_size: self.storage.hdr_max_memtable_size,
            tail_max_memtable_size: self.storage.tail_max_memtable_size,
            max_disk_bytes: self.storage.max_disk_bytes,
            control_reserve_bytes: self.storage.control_reserve_bytes,
            on_disk_full: self.storage.on_disk_full.into(),
            sync_wal: self.storage.sync_wal,
            worker_threads: self.storage.worker_threads,
            max_write_buffer_bytes: self.storage.max_write_buffer_bytes,
            max_cached_files: self.storage.max_cached_files,
        };

        let window = WindowConfig {
            duration: self.window.duration,
            offset: self.window.offset,
            timestamp_field: self.window.timestamp_field.clone(),
            allowed_skew: self.window.allowed_skew,
            allowed_lateness: self.window.allowed_lateness,
            max_future_drift: self.window.max_future_drift,
            idle_timeout: self.window.idle_timeout,
            caught_up_threshold: self.window.caught_up_threshold,
            max_close_delay: self.window.max_close_delay,
            late_emit_interval: self.window.late_emit_interval,
            max_closing_duration: self.window.max_closing_duration,
            source_id_field: self.window.source_id_field.clone(),
            max_tracked_sources: self.window.max_tracked_sources,
            emit: self.window.emit.clone().into(),
        };

        let engine = EngineConfig {
            shards: self.shards,
            emit_mode: self.emit_mode.into(),
            storage,
            window,
            cardinality: self.cardinality.clone().into(),
            output: self.output.clone().into(),
            shutdown: self.shutdown.clone().into(),
            aggregations: specs,
        };

        engine.validate()?;
        Ok(engine)
    }

    /// Compiles and validates both the [`EngineConfig`] and [`AggregationPlan`] from this configuration.
    ///
    /// # Errors
    /// Returns [`AggError`] if validation or plan compilation fails.
    pub fn into_engine_and_plan(&self) -> AggResult<(EngineConfig, AggregationPlan)> {
        let engine = self.engine()?;
        let plan = AggregationPlan::new(&engine.aggregations)?;
        Ok((engine, plan))
    }

    /// Computes the deterministic codec schema bytes:
    /// `plan schema bytes || group_by || serialized normalizers || attributes_policy`.
    #[must_use]
    pub fn codec_schema(&self) -> Vec<u8> {
        let Ok((_engine, plan)) = self.into_engine_and_plan() else {
            return Vec::new();
        };

        let mut bytes = plan.schema_bytes();

        for dim in &self.group_by {
            bytes.extend_from_slice(dim.as_bytes());
            bytes.push(0);
        }

        // Canonical deterministic normalizer representation
        let norm_json = serde_json::to_vec(&self.normalizers).unwrap_or_default();
        bytes.extend_from_slice(&norm_json);
        bytes.push(0);

        let body_policy_str = match self.attributes_policy.body_policy {
            BodyPolicy::KeepFirstExemplar => "keep_first_exemplar",
            BodyPolicy::ReplaceWithTemplate => "replace_with_template",
            BodyPolicy::Ignore => "ignore",
        };
        bytes.extend_from_slice(body_policy_str.as_bytes());
        bytes.push(0);

        bytes.push(u8::from(self.attributes_policy.preserve_resource_and_scope));

        let res_excl_count =
            u32::try_from(self.attributes_policy.resource_attributes_exclude.len())
                .unwrap_or(u32::MAX);
        bytes.extend_from_slice(&res_excl_count.to_le_bytes());
        for s in &self.attributes_policy.resource_attributes_exclude {
            let len = u32::try_from(s.len()).unwrap_or(u32::MAX);
            bytes.extend_from_slice(&len.to_le_bytes());
            bytes.extend_from_slice(s.as_bytes());
        }

        let tmpl_len =
            u32::try_from(self.attributes_policy.template_target_field.len()).unwrap_or(u32::MAX);
        bytes.extend_from_slice(&tmpl_len.to_le_bytes());
        bytes.extend_from_slice(self.attributes_policy.template_target_field.as_bytes());

        let unmatched_byte = match self.attributes_policy.unmatched_log_attributes {
            UnmatchedLogAttributes::Drop => 0u8,
            UnmatchedLogAttributes::KeepFirst => 1u8,
        };
        bytes.push(unmatched_byte);

        let excl_count =
            u32::try_from(self.attributes_policy.exclude_attributes.len()).unwrap_or(u32::MAX);
        bytes.extend_from_slice(&excl_count.to_le_bytes());
        for s in &self.attributes_policy.exclude_attributes {
            let len = u32::try_from(s.len()).unwrap_or(u32::MAX);
            bytes.extend_from_slice(&len.to_le_bytes());
            bytes.extend_from_slice(s.as_bytes());
        }

        bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::de::value::{Error as DeValueError, I64Deserializer};

    #[test]
    fn parse_byte_size_option_handles_null_int_and_invalid_inputs() {
        let null_cfg: StorageAdapterConfig =
            serde_yaml_ng::from_str("max_write_buffer_bytes: null").expect("null should parse");
        assert_eq!(null_cfg.max_write_buffer_bytes, None);

        let int_cfg: StorageAdapterConfig =
            serde_yaml_ng::from_str("max_write_buffer_bytes: 67108864").expect("int should parse");
        assert_eq!(int_cfg.max_write_buffer_bytes, Some(67_108_864));

        let tb_cfg: StorageAdapterConfig =
            serde_yaml_ng::from_str("max_write_buffer_bytes: \"1TB\"").expect("1TB should parse");
        assert_eq!(tb_cfg.max_write_buffer_bytes, Some(1_000_000_000_000));

        for (raw, expected) in [
            ("512B", 512_u64),
            ("2KB", 2_000),
            ("2KiB", 2_048),
            ("3MB", 3_000_000),
            ("3MiB", 3 * 1024 * 1024),
            ("4GB", 4_000_000_000),
            ("4GiB", 4 * 1024 * 1024 * 1024),
            ("1TiB", 1024 * 1024 * 1024 * 1024),
        ] {
            let cfg: StorageAdapterConfig =
                serde_yaml_ng::from_str(&format!("max_write_buffer_bytes: \"{raw}\""))
                    .expect("unit should parse");
            assert_eq!(cfg.max_write_buffer_bytes, Some(expected));
        }

        let pos_i64 =
            parse_byte_size(I64Deserializer::<DeValueError>::new(4096)).expect("i64 should parse");
        assert_eq!(pos_i64, 4096);

        for invalid_yaml in [
            "max_write_buffer_bytes: \"\"",
            "max_write_buffer_bytes: -1",
            "max_write_buffer_bytes: [1, 2]",
            "max_write_buffer_bytes: \"..B\"",
            "max_write_buffer_bytes: \"10PB\"",
            "max_write_buffer_bytes: \"999999999999999999999TiB\"",
        ] {
            let res: Result<StorageAdapterConfig, _> = serde_yaml_ng::from_str(invalid_yaml);
            assert!(res.is_err(), "expected error for {invalid_yaml}");
        }
    }

    #[test]
    fn sluice_config_defaults_and_enum_conversions_roundtrip() {
        let default_window = WindowAdapterConfig::default();
        assert_eq!(default_window.r#type, "tumbling");
        assert_eq!(default_window.duration, Duration::from_secs(86400));

        let mut cfg = SluiceConfig::default();
        assert_eq!(cfg.shards, 4);
        assert_eq!(CoreSimdMode::from(SimdMode::Auto), CoreSimdMode::Auto);
        assert_eq!(CoreSimdMode::from(SimdMode::Scalar), CoreSimdMode::Scalar);
        assert_eq!(
            CoreOnDiskFull::from(OnDiskFull::Block),
            CoreOnDiskFull::Block
        );

        let yaml_defaults: SluiceConfig = serde_yaml_ng::from_str(
            r#"
window: {}
source_position: {}
normalizers:
  client.ip:
    type: ip_cidr
    when:
      field: env
      equals: prod
  message:
    type: log_fingerprint
"#,
        )
        .expect("minimal yaml should parse");
        assert_eq!(yaml_defaults.window, default_window);
        assert_eq!(
            yaml_defaults.source_position,
            Some(SourcePositionConfig::default())
        );

        cfg.emit_mode = EmitMode::Cumulative;
        cfg.storage.on_disk_full = OnDiskFull::DropNewest;
        cfg.cardinality.on_cap = OnCap::EvictCold;
        cfg.window.emit.strategy = EmitStrategy::Periodic;
        cfg.window.emit.interval = Some(Duration::from_secs(60));
        cfg.shutdown.on_shutdown = OnShutdown::Persist;
        cfg.attributes_policy.body_policy = BodyPolicy::ReplaceWithTemplate;
        cfg.attributes_policy.unmatched_log_attributes = UnmatchedLogAttributes::KeepFirst;
        cfg.attributes_policy
            .resource_attributes_exclude
            .insert("host.id".to_string());
        cfg.attributes_policy
            .exclude_attributes
            .insert("trace_id".to_string());
        cfg.source_position = Some(SourcePositionConfig::default());
        cfg.normalizers = yaml_defaults.normalizers;
        cfg.group_by = vec!["service.name".to_string()];
        cfg.aggregations.insert(
            "events".to_string(),
            AggregationDefinition::Simple("count".to_string()),
        );
        cfg.aggregations.insert(
            "detailed_count".to_string(),
            AggregationDefinition::Detailed(IndexMap::from([(
                "count".to_string(),
                "event.id".to_string(),
            )])),
        );
        cfg.aggregations.insert(
            "bytes_sum".to_string(),
            AggregationDefinition::Detailed(IndexMap::from([(
                "sum".to_string(),
                "network.bytes".to_string(),
            )])),
        );
        cfg.aggregations.insert(
            "lat_min".to_string(),
            AggregationDefinition::Detailed(IndexMap::from([(
                "min".to_string(),
                "latency".to_string(),
            )])),
        );
        cfg.aggregations.insert(
            "lat_max".to_string(),
            AggregationDefinition::Detailed(IndexMap::from([(
                "max".to_string(),
                "latency".to_string(),
            )])),
        );

        let (engine, plan) = cfg
            .into_engine_and_plan()
            .expect("valid config should compile");
        assert_eq!(engine.emit_mode, CoreEmitMode::Cumulative);
        assert_eq!(engine.storage.on_disk_full, CoreOnDiskFull::DropNewest);
        assert_eq!(engine.cardinality.on_cap, CoreOnCap::EvictCold);
        assert_eq!(engine.window.emit.strategy, CoreEmitStrategy::Periodic);
        assert_eq!(engine.shutdown.on_shutdown, CoreOnShutdown::Persist);
        assert_eq!(plan.specs().len(), 5);

        let schema_replace = cfg.codec_schema();
        assert!(!schema_replace.is_empty());

        cfg.attributes_policy.body_policy = BodyPolicy::Ignore;
        cfg.attributes_policy.unmatched_log_attributes = UnmatchedLogAttributes::Drop;
        let schema_ignore = cfg.codec_schema();
        assert!(!schema_ignore.is_empty());
        assert_ne!(schema_replace, schema_ignore);
    }

    #[test]
    fn into_engine_and_plan_rejects_invalid_aggregation_specs() {
        for missing_field_kind in ["sum", "min", "max", "unknown_kind"] {
            let mut cfg = SluiceConfig::default();
            cfg.aggregations.insert(
                "metric".to_string(),
                AggregationDefinition::Simple(missing_field_kind.to_string()),
            );
            let err = cfg
                .into_engine_and_plan()
                .expect_err("simple non-count or unknown aggregation must fail");
            assert!(matches!(err, AggError::Config(_)));
            assert!(cfg.codec_schema().is_empty());
        }

        for (kind, field) in [
            ("sum", ""),
            ("min", ""),
            ("max", ""),
            ("unknown_kind", "latency"),
        ] {
            let mut cfg = SluiceConfig::default();
            cfg.aggregations.insert(
                "metric".to_string(),
                AggregationDefinition::Detailed(IndexMap::from([(
                    kind.to_string(),
                    field.to_string(),
                )])),
            );
            let err = cfg
                .into_engine_and_plan()
                .expect_err("invalid detailed aggregation spec must fail");
            assert!(matches!(err, AggError::Config(_)));
        }

        let mut empty_detailed = SluiceConfig::default();
        empty_detailed.aggregations.insert(
            "metric".to_string(),
            AggregationDefinition::Detailed(IndexMap::new()),
        );
        assert!(matches!(
            empty_detailed.into_engine_and_plan(),
            Err(AggError::Config(_))
        ));
    }
}
