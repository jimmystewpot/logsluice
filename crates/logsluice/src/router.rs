//! Router component mapping events to shards, checking watermarks and horizons,
//! and routing late/quarantined records to passthrough.
//!
//! Native flat JSON and OTLP batches used to carry two copies of timestamp,
//! severity, normalizer, and shard logic. Envelope parsing stays separate
//! because the JSON shapes differ. Both paths fill one [`PreparedRecord`] and
//! [`Router::publish_prepared`] assigns the shard. `crates/logsluice/tests/router.rs`
//! locks the observable behavior.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use indexmap::IndexMap;
use logsluice_core::codec::plan::AggregationPlan;
use logsluice_core::config::{AggregationKind, AggregationSpec};
use logsluice_core::error::AggResult;
use logsluice_core::key::{KeyBuilder, StorageKey24};
use logsluice_core::position::SourcePosition;
use logsluice_core::resource::ResourceScopePayload;
use logsluice_core::simd::SimdKernelTable;
use logsluice_core::watermark::{Admit, SourceKey, WatermarkTracker};
use logsluice_core::window::WindowClock;
use logsluice_core::worker::{ExemplarSource, IngestItem, PurgeHorizon};

use crate::config::{BodyPolicy, EnvelopeMode, SluiceConfig, UnmatchedLogAttributes};
use crate::event::{FieldPath, LogEvent, ObjectMap, Value, value_to_bytes, value_to_string_lossy};
use crate::extract::{
    MetricValue, NormalizerExecutor, SourcePositionPaths, extract_otlp_time_nano_ms,
    extract_timestamp_ms, metric_value,
};

/// OTLP log records keyed by `(resourceLogs index, scopeLogs index)`.
type OtlpIndexedRecords = BTreeMap<(usize, usize), Vec<Value>>;

/// Quarantined OTLP records grouped by the unchanged reason string, then by scope.
type OtlpQuarantineGroups = BTreeMap<&'static str, OtlpIndexedRecords>;

/// Router performance counters and error metrics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RouterStats {
    /// Number of metric field type errors encountered (unparseable metric value -> 0).
    pub field_type_errors_total: u64,
    /// Number of metric fields coerced from floats to integers.
    pub field_coercions_total: u64,
    /// Events routed to quarantine (missing/invalid timestamp, future drift).
    pub quarantined_total: u64,
    /// Events routed to late output (past lateness horizon).
    pub late_total: u64,
}

/// Router dispatch destination batches.
#[derive(Debug, Clone, PartialEq)]
pub struct RouterOutput {
    /// Batches of admitted ingest items partitioned per shard.
    pub per_shard: Vec<Vec<IngestItem>>,
    /// Unadmitted events forwarded as passthrough (quarantine or late).
    pub passthrough: Vec<LogEvent>,
}

impl RouterOutput {
    /// Creates a new `RouterOutput` with empty vectors for `shards` count.
    #[must_use]
    pub fn new(shards: usize) -> Self {
        Self {
            per_shard: (0..shards).map(|_| Vec::new()).collect(),
            passthrough: Vec::new(),
        }
    }
}

/// Dispatches events into windowed shards based on timestamp, grouping, and watermark state.
pub struct Router {
    cfg: Arc<SluiceConfig>,
    plan: AggregationPlan,
    clock: WindowClock,
    kernels: SimdKernelTable,
    key_builder: KeyBuilder,
    horizons: Vec<PurgeHorizon>,
    watermark: WatermarkTracker,
    stats: RouterStats,
    shards: u16,
    detected_otlp: Arc<AtomicBool>,

    timestamp_path: FieldPath,
    source_position_paths: Option<SourcePositionPaths>,
    source_id_path: FieldPath,
    route_path: FieldPath,
    reason_path: FieldPath,
    group_by_paths: Vec<FieldPath>,
    metric_paths: Vec<Option<FieldPath>>,
    metric_field_names: BTreeSet<String>,
    group_by_set: BTreeSet<String>,
    normalizers: IndexMap<String, NormalizerExecutor>,
    resource_logs_path: FieldPath,
    body_path: FieldPath,
    message_path: FieldPath,
    severity_path: FieldPath,
    severity_number_path: FieldPath,
    severity_camel_path: FieldPath,
    level_path: FieldPath,
    resource_path: FieldPath,
    resources_path: FieldPath,
    scope_path: FieldPath,
    attributes_path: FieldPath,
    default_resource: Arc<ResourceScopePayload>,
    default_resource_hash: u64,
}

/// Shared middle between a native flat JSON event and one OTLP log record.
///
/// Native flat JSON and OTLP batches used to carry two copies of timestamp,
/// severity, normalizer, and shard logic. This record is that shared middle:
/// each envelope parser fills one (OTLP once per log record) and
/// [`Router::publish_prepared`] turns it into an [`IngestItem`] with the same
/// shard formula. Envelope parsing stays separate because the JSON shapes
/// differ. `crates/logsluice/tests/router.rs` locks the observable behavior.
///
/// Counter banks and exemplar attrs are not fields here: `on_admit` computes
/// them only after the record is admitted and they go straight into the
/// [`IngestItem`]. Late events therefore do not bump metric coercion or
/// type-error counters. `position` is the native source position; OTLP leaves it empty
/// and stamps the envelope position onto the last admitted item of each shard
/// after the `resourceLogs` walk.
struct PreparedRecord {
    event_ms: i64,
    position: Option<SourcePosition>,
    severity: u8,
    body: Option<Bytes>,
    dims: Vec<Option<Bytes>>,
    resource: Arc<ResourceScopePayload>,
    resource_hash: u64,
}

/// Outcome of publishing one [`PreparedRecord`].
enum RecordDisposition {
    /// Past the lateness horizon or the target shard's purge horizon.
    Late,
    /// Admitted. `Some` is the shard that received the item.
    Admitted { pushed_shard: Option<usize> },
}

impl Router {
    /// Constructs a new `Router` instance.
    ///
    /// # Errors
    /// Returns [`AggError`](logsluice_core::error::AggError) if configuration validation or window clock creation fails.
    pub fn new(
        cfg: Arc<SluiceConfig>,
        kernels: SimdKernelTable,
        salt: u64,
        horizons: Vec<PurgeHorizon>,
        watermark: WatermarkTracker,
    ) -> AggResult<Self> {
        let (engine, plan) = cfg.into_engine_and_plan()?;
        let clock = WindowClock::new(cfg.window.duration, cfg.window.offset)?;

        let timestamp_path = FieldPath::parse_or_literal(cfg.window.timestamp_field.as_str());
        let source_position_paths = cfg.source_position.as_ref().map(SourcePositionPaths::new);
        let source_id_path = FieldPath::parse_or_literal(cfg.window.source_id_field.as_str());
        let route_field_str = if cfg.output.route_field.is_empty() {
            "_wa_route"
        } else {
            cfg.output.route_field.as_str()
        };
        let route_path = FieldPath::parse_or_literal(route_field_str);
        let reason_path = FieldPath::parse_or_literal("_wa_reason");

        let group_by_paths = cfg
            .group_by
            .iter()
            .map(|s| FieldPath::parse_or_literal(s.as_str()))
            .collect();
        let group_by_set: BTreeSet<String> = cfg.group_by.iter().cloned().collect();

        let metric_paths = engine
            .aggregations
            .iter()
            .map(|s| {
                s.field
                    .as_ref()
                    .map(|f| FieldPath::parse_or_literal(f.as_str()))
            })
            .collect();
        let metric_field_names: BTreeSet<String> = engine
            .aggregations
            .iter()
            .filter_map(|s| s.field.clone())
            .collect();

        let normalizers = cfg
            .normalizers
            .iter()
            .map(|(k, v)| (k.clone(), NormalizerExecutor::new(v.clone())))
            .collect();

        let resource_logs_path = FieldPath::parse_or_literal("resourceLogs");
        let body_path = FieldPath::parse_or_literal("body");
        let message_path = FieldPath::parse_or_literal("message");
        let severity_path = FieldPath::parse_or_literal("severity");
        let severity_number_path = FieldPath::parse_or_literal("severity_number");
        let severity_camel_path = FieldPath::parse_or_literal("severityNumber");
        let level_path = FieldPath::parse_or_literal("level");
        let resource_path = FieldPath::parse_or_literal("resource");
        let resources_path = FieldPath::parse_or_literal("resources");
        let scope_path = FieldPath::parse_or_literal("scope");
        let attributes_path = FieldPath::parse_or_literal("attributes");

        let default_resource = Arc::new(ResourceScopePayload {
            resource: BTreeMap::new(),
            scope_name: String::new(),
            scope_version: String::new(),
            scope_attrs: BTreeMap::new(),
        });
        let default_resource_hash = default_resource.canonical_hash();

        Ok(Self {
            shards: cfg.shards,
            cfg,
            plan,
            clock,
            kernels,
            key_builder: KeyBuilder::new(salt),
            horizons,
            watermark,
            stats: RouterStats::default(),
            detected_otlp: Arc::new(AtomicBool::new(false)),

            timestamp_path,
            source_position_paths,
            source_id_path,
            route_path,
            reason_path,
            group_by_paths,
            metric_paths,
            metric_field_names,
            group_by_set,
            normalizers,
            resource_logs_path,
            body_path,
            message_path,
            severity_path,
            severity_number_path,
            severity_camel_path,
            level_path,
            resource_path,
            resources_path,
            scope_path,
            attributes_path,
            default_resource,
            default_resource_hash,
        })
    }

    /// Returns a shared atomic handle indicating whether OTLP input has been detected.
    #[must_use]
    pub fn detected_otlp_handle(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.detected_otlp)
    }

    /// Returns the current computed watermark in epoch milliseconds.
    #[must_use]
    pub fn watermark(&self, now_ms: i64) -> Option<i64> {
        self.watermark.watermark(now_ms)
    }

    /// Returns the router statistics counters.
    #[must_use]
    pub fn stats(&self) -> RouterStats {
        self.stats
    }

    /// Returns the configured number of shards.
    #[must_use]
    pub fn shards(&self) -> u16 {
        self.shards
    }

    fn extract_position(&self, log: &LogEvent) -> Option<SourcePosition> {
        self.source_position_paths
            .as_ref()
            .and_then(|p| p.extract(log))
    }

    fn source_key_from(&self, event: &LogEvent, position: Option<&SourcePosition>) -> SourceKey {
        if let Some(pos) = position {
            SourceKey::Partition(pos.topic.clone(), pos.partition)
        } else if let Some(id_val) = event.get(&self.source_id_path) {
            SourceKey::Source(Arc::from(value_to_string_lossy(id_val).into_owned()))
        } else {
            SourceKey::Default
        }
    }

    fn stamp_route(&self, event: &mut LogEvent, route: &str, reason: &str) {
        let _ = event.insert(&self.route_path, Value::from(route));
        let _ = event.insert(&self.reason_path, Value::from(reason));
    }

    /// Tags the event `quarantine` / `reason`, increments `quarantined_total`, and passthroughs it.
    ///
    /// Reason strings stay `missing_timestamp`, `invalid_timestamp`, and `future_drift`.
    /// OTLP record-level failures do not use this path: one batch can emit several
    /// filtered envelopes, so those reasons are grouped instead.
    fn quarantine_event(&mut self, mut event: LogEvent, reason: &str, out: &mut RouterOutput) {
        self.stats.quarantined_total += 1;
        self.stamp_route(&mut event, "quarantine", reason);
        out.passthrough.push(event);
    }

    fn note_otlp_quarantine(
        &mut self,
        groups: &mut OtlpQuarantineGroups,
        reason: &'static str,
        res_idx: usize,
        scope_idx: usize,
        record: Value,
    ) {
        groups
            .entry(reason)
            .or_default()
            .entry((res_idx, scope_idx))
            .or_default()
            .push(record);
        self.stats.quarantined_total += 1;
    }

    /// Publishes `record` on the shard `(digest as u64) % horizons`.
    ///
    /// Native flat JSON and OTLP batches used to carry two copies of this
    /// digest, lateness, and [`IngestItem`] construction. `on_admit` finishes
    /// counter banks and exemplar attrs only after admission, because the two
    /// envelopes look up metric fields differently and late records must not
    /// change coercion counters. Envelope parsing stays outside this method.
    /// `crates/logsluice/tests/router.rs` locks the observable behavior.
    fn publish_prepared(
        &mut self,
        record: PreparedRecord,
        now_ms: i64,
        out: &mut RouterOutput,
        on_admit: impl FnOnce(&mut Self) -> (Vec<[u64; 4]>, Vec<i64>, Vec<(String, Bytes)>),
    ) -> RecordDisposition {
        let digest = self.key_builder.digest_dims(
            record.resource_hash,
            record.severity,
            record.body.as_deref(),
            &record.dims,
        );
        let shard = ((digest as u64) % (self.horizons.len() as u64)) as usize;
        let window_start = self.clock.start_of(record.event_ms);
        let window_end = self.clock.end_of(window_start);
        let allowed_lateness_ms = self.cfg.window.allowed_lateness.as_millis() as i64;
        let watermark = self.watermark.watermark(now_ms).unwrap_or(i64::MIN);
        let is_late = window_end.saturating_add(allowed_lateness_ms) <= watermark
            || window_end <= self.horizons[shard].get();
        if is_late {
            self.stats.late_total += 1;
            return RecordDisposition::Late;
        }

        let (counters, minmax_values, attrs) = on_admit(self);

        let pushed = shard < out.per_shard.len();
        if pushed {
            out.per_shard[shard].push(IngestItem {
                key: StorageKey24 {
                    epoch_ms: window_start as u64,
                    digest,
                },
                resource: record.resource,
                resource_hash: record.resource_hash,
                position: record.position,
                event_ms: record.event_ms,
                counters,
                minmax_values,
                exemplar: ExemplarSource {
                    severity: record.severity,
                    body: record.body,
                    dims: record.dims,
                    attrs,
                },
            });
        }
        RecordDisposition::Admitted {
            pushed_shard: pushed.then_some(shard),
        }
    }

    fn native_severity(&self, event: &LogEvent) -> u8 {
        severity_from([
            event.get(&self.severity_number_path),
            event.get(&self.severity_camel_path),
            event.get(&self.severity_path),
            event.get(&self.level_path),
        ])
    }

    fn native_dims(&self, event: &LogEvent) -> Vec<Option<Bytes>> {
        let mut dims = Vec::with_capacity(self.cfg.group_by.len());
        for (i, dim_name) in self.cfg.group_by.iter().enumerate() {
            let raw_val = event.get(&self.group_by_paths[i]);
            let normalizer = self.normalizers.get(dim_name);
            let kernels = &self.kernels;
            dims.push(dimension_bytes(normalizer, raw_val, |norm, val| {
                norm.apply(event, val, kernels)
            }));
        }
        dims
    }

    fn native_body(&self, event: &LogEvent) -> Option<Bytes> {
        let policy = self.cfg.attributes_policy.body_policy;
        let normalizer = self
            .normalizers
            .get("body")
            .or_else(|| self.normalizers.get("message"));
        let kernels = &self.kernels;
        let raw_body = event
            .get(&self.body_path)
            .or_else(|| event.get(&self.message_path));
        apply_body_policy(
            policy,
            normalizer,
            kernels,
            raw_body,
            |norm, val, kernels| norm.apply(event, val, kernels),
        )
    }

    fn is_excluded_unmatched_key(&self, key: &str) -> bool {
        let stripped = key.strip_prefix("attributes.").unwrap_or(key);
        let configured = |candidate: &str| {
            self.group_by_set.contains(candidate)
                || self.metric_field_names.contains(candidate)
                || self
                    .cfg
                    .attributes_policy
                    .exclude_attributes
                    .contains(candidate)
                || candidate == self.cfg.window.timestamp_field.as_str()
                || candidate == self.cfg.window.source_id_field.as_str()
                || candidate == self.cfg.output.route_field.as_str()
                || self.cfg.source_position.as_ref().is_some_and(|sp| {
                    candidate == sp.topic_field.as_str()
                        || candidate == sp.partition_field.as_str()
                        || candidate == sp.offset_field.as_str()
                })
        };
        if matches_key_or_stripped(key, stripped, configured) {
            return true;
        }
        // Literal names are matched on the stripped key only. A key of
        // `attributes.@timestamp` is excluded because stripping yields `@timestamp`;
        // checking the raw key here would not add any of these names.
        matches!(
            stripped,
            "@timestamp"
                | "timestamp"
                | "time"
                | "timeUnixNano"
                | "observedTimeUnixNano"
                | "_wa_route"
                | "_wa_reason"
                | "topic"
                | "partition"
                | "offset"
                | "message"
                | "body"
                | "severity"
                | "severity_number"
                | "severityNumber"
                | "severityText"
                | "level"
                | "resource"
                | "resources"
                | "scope"
                | "resourceLogs"
                | "attributes"
        )
    }

    fn extract_native_unmatched_attrs(&self, log: &LogEvent) -> Vec<(String, Bytes)> {
        if self.cfg.attributes_policy.unmatched_log_attributes != UnmatchedLogAttributes::KeepFirst
        {
            return Vec::new();
        }
        let mut collected: BTreeMap<String, Bytes> = BTreeMap::new();

        if let Some(Value::Object(attr_map)) = log.get(&self.attributes_path) {
            let mut leaves = Vec::new();
            flatten_object_leaves("", attr_map, &mut leaves);
            for (full_key, leaf_val) in leaves {
                if !self.is_excluded_unmatched_key(&full_key) && !matches!(leaf_val, Value::Null) {
                    collected.insert(
                        full_key,
                        Bytes::from(value_to_string_lossy(leaf_val).into_owned()),
                    );
                }
            }
        }

        let root_map = log.as_map();
        for (k, v) in root_map {
            let k_str = k.as_str();
            if matches!(
                k_str,
                "resource" | "resources" | "scope" | "attributes" | "window" | "resourceLogs"
            ) || self.is_excluded_unmatched_key(k_str)
            {
                continue;
            }
            if let Value::Object(sub_map) = v {
                let mut leaves = Vec::new();
                flatten_object_leaves(k_str, sub_map, &mut leaves);
                for (full_key, leaf_val) in leaves {
                    if !self.is_excluded_unmatched_key(&full_key)
                        && !matches!(leaf_val, Value::Null)
                    {
                        collected.entry(full_key).or_insert_with(|| {
                            Bytes::from(value_to_string_lossy(leaf_val).into_owned())
                        });
                    }
                }
            } else if !matches!(v, Value::Null) {
                collected
                    .entry(k_str.to_string())
                    .or_insert_with(|| Bytes::from(value_to_string_lossy(v).into_owned()));
            }
        }

        collected.into_iter().collect()
    }

    fn extract_otlp_unmatched_attrs(&self, attrs_val: Option<&Value>) -> Vec<(String, Bytes)> {
        if self.cfg.attributes_policy.unmatched_log_attributes != UnmatchedLogAttributes::KeepFirst
        {
            return Vec::new();
        }
        let Some(attrs_val) = attrs_val else {
            return Vec::new();
        };
        let mut collected: BTreeMap<String, Bytes> = BTreeMap::new();
        match attrs_val {
            Value::Array(arr) => {
                for item in arr {
                    if let Some(obj) = item.as_object()
                        && let Some(k) = obj.get("key").map(value_to_string_lossy)
                        && !self.is_excluded_unmatched_key(k.as_ref())
                        && let Some(val) = obj.get("value")
                    {
                        let bytes = extract_otlp_attr_bytes(val);
                        collected.insert(k.into_owned(), bytes);
                    }
                }
            }
            Value::Object(map) => {
                for (k, v) in map {
                    let k_str = k.as_str();
                    if !self.is_excluded_unmatched_key(k_str) {
                        let bytes = extract_otlp_attr_bytes(v);
                        collected.insert(k_str.to_string(), bytes);
                    }
                }
            }
            _ => {}
        }
        collected.into_iter().collect()
    }

    fn extract_native_resource(&self, log: &LogEvent) -> (Arc<ResourceScopePayload>, u64) {
        if !self.cfg.attributes_policy.preserve_resource_and_scope {
            return (self.default_resource.clone(), self.default_resource_hash);
        }

        let mut resource_map = BTreeMap::new();
        let res_val = log
            .get(&self.resource_path)
            .or_else(|| log.get(&self.resources_path));
        if let Some(Value::Object(res_obj)) = res_val {
            let mut leaves = Vec::new();
            flatten_object_leaves("", res_obj, &mut leaves);
            for (full_key, leaf_val) in leaves {
                if !self
                    .cfg
                    .attributes_policy
                    .resource_attributes_exclude
                    .contains(&full_key)
                {
                    resource_map.insert(full_key, extract_otlp_attr_value(Some(leaf_val)));
                }
            }
        }

        let mut scope_name = String::new();
        let mut scope_version = String::new();
        let mut scope_attrs = BTreeMap::new();
        if let Some(Value::Object(scope_obj)) = log.get(&self.scope_path) {
            let mut leaves = Vec::new();
            flatten_object_leaves("", scope_obj, &mut leaves);
            for (full_key, leaf_val) in leaves {
                match full_key.as_str() {
                    "name" => scope_name = value_to_string_lossy(leaf_val).into_owned(),
                    "version" => scope_version = value_to_string_lossy(leaf_val).into_owned(),
                    _ => {
                        scope_attrs.insert(full_key, extract_otlp_attr_value(Some(leaf_val)));
                    }
                }
            }
        }

        if resource_map.is_empty()
            && scope_name.is_empty()
            && scope_version.is_empty()
            && scope_attrs.is_empty()
        {
            (self.default_resource.clone(), self.default_resource_hash)
        } else {
            let payload = Arc::new(ResourceScopePayload {
                resource: resource_map,
                scope_name,
                scope_version,
                scope_attrs,
            });
            let hash = payload.canonical_hash();
            (payload, hash)
        }
    }

    /// Routes an event to its target shard or passthrough.
    pub fn route(&mut self, event: LogEvent, now_ms: i64, out: &mut RouterOutput) {
        let is_otlp = match self.cfg.envelope_mode {
            EnvelopeMode::Otlp => true,
            EnvelopeMode::VectorNative => false,
            EnvelopeMode::Auto => event.get(&self.resource_logs_path).is_some(),
        };

        if is_otlp {
            self.route_otlp(event, now_ms, out);
        } else {
            self.route_native(event, now_ms, out);
        }
    }

    fn route_native(&mut self, mut event: LogEvent, now_ms: i64, out: &mut RouterOutput) {
        let event_ms = match event.get(&self.timestamp_path) {
            None => {
                self.quarantine_event(event, "missing_timestamp", out);
                return;
            }
            Some(v) => match extract_timestamp_ms(v) {
                Ok(ms) => ms,
                Err(_) => {
                    self.quarantine_event(event, "invalid_timestamp", out);
                    return;
                }
            },
        };

        let position = self.extract_position(&event);
        let source_key = self.source_key_from(&event, position.as_ref());
        if self.watermark.observe(source_key, event_ms, now_ms) == Admit::Quarantine {
            self.quarantine_event(event, "future_drift", out);
            return;
        }

        let (resource, resource_hash) = self.extract_native_resource(&event);
        let record = PreparedRecord {
            event_ms,
            position,
            severity: self.native_severity(&event),
            body: self.native_body(&event),
            dims: self.native_dims(&event),
            resource,
            resource_hash,
        };

        match self.publish_prepared(record, now_ms, out, |router| {
            let metric_paths = &router.metric_paths;
            let (counters, minmax_values) = fill_metric_banks(
                &router.plan,
                router.cfg.window.timestamp_field.as_str(),
                &mut router.stats,
                |idx, _spec| {
                    metric_paths[idx]
                        .as_ref()
                        .and_then(|path| event.get(path).map(Cow::Borrowed))
                },
                None,
            );
            let attrs = router.extract_native_unmatched_attrs(&event);
            (counters, minmax_values, attrs)
        }) {
            RecordDisposition::Late => {
                self.stamp_route(&mut event, "late", "late");
                out.passthrough.push(event);
            }
            RecordDisposition::Admitted { .. } => {
                let _ = event.take_finalizers();
            }
        }
    }

    fn route_otlp(&mut self, mut event: LogEvent, now_ms: i64, out: &mut RouterOutput) {
        self.detected_otlp.store(true, Ordering::Relaxed);

        let outer_position = self.extract_position(&event);
        let outer_source_key = self.source_key_from(&event, outer_position.as_ref());

        let Some(resource_logs) = event
            .get(&self.resource_logs_path)
            .and_then(Value::as_array)
            .map(Vec::as_slice)
        else {
            self.quarantine_event(event, "missing_timestamp", out);
            return;
        };

        let mut routed_shards: BTreeSet<usize> = BTreeSet::new();
        let mut quarantine_groups = OtlpQuarantineGroups::new();
        let mut late_records = OtlpIndexedRecords::new();

        for (res_idx, rl) in resource_logs.iter().enumerate() {
            let Some(rl_obj) = rl.as_object() else {
                continue;
            };

            // Parse Resource (when preserve_resource_and_scope is true)
            let mut resource_map = BTreeMap::new();
            if self.cfg.attributes_policy.preserve_resource_and_scope
                && let Some(res) = rl_obj.get("resource").and_then(Value::as_object)
            {
                if let Some(attrs) = res.get("attributes").and_then(Value::as_array) {
                    for attr in attrs {
                        if let Some(attr_obj) = attr.as_object()
                            && let Some(k) = attr_obj.get("key").map(value_to_string_lossy)
                            && !self
                                .cfg
                                .attributes_policy
                                .resource_attributes_exclude
                                .contains(k.as_ref())
                        {
                            let v_json = extract_otlp_attr_value(attr_obj.get("value"));
                            resource_map.insert(k.into_owned(), v_json);
                        }
                    }
                } else {
                    for (k, v) in res {
                        if !self
                            .cfg
                            .attributes_policy
                            .resource_attributes_exclude
                            .contains(k.as_str())
                        {
                            let v_json = extract_otlp_attr_value(Some(v));
                            resource_map.insert(k.to_string(), v_json);
                        }
                    }
                }
            }

            let Some(scope_logs) = rl_obj.get("scopeLogs").and_then(Value::as_array) else {
                continue;
            };

            for (scope_idx, sl) in scope_logs.iter().enumerate() {
                let Some(sl_obj) = sl.as_object() else {
                    continue;
                };

                let (resource_payload, resource_hash) =
                    if self.cfg.attributes_policy.preserve_resource_and_scope {
                        let mut scope_name = String::new();
                        let mut scope_version = String::new();
                        let mut scope_attrs = BTreeMap::new();

                        if let Some(scope) = sl_obj.get("scope").and_then(Value::as_object) {
                            if let Some(n) = scope.get("name") {
                                scope_name = value_to_string_lossy(n).into_owned();
                            }
                            if let Some(v) = scope.get("version") {
                                scope_version = value_to_string_lossy(v).into_owned();
                            }
                            if let Some(attrs) = scope.get("attributes").and_then(Value::as_array) {
                                for attr in attrs {
                                    if let Some(attr_obj) = attr.as_object()
                                        && let Some(k) =
                                            attr_obj.get("key").map(value_to_string_lossy)
                                    {
                                        let v_json = extract_otlp_attr_value(attr_obj.get("value"));
                                        scope_attrs.insert(k.into_owned(), v_json);
                                    }
                                }
                            }
                        }

                        let payload = Arc::new(ResourceScopePayload {
                            resource: resource_map.clone(),
                            scope_name,
                            scope_version,
                            scope_attrs,
                        });
                        let hash = payload.canonical_hash();
                        (payload, hash)
                    } else {
                        (self.default_resource.clone(), self.default_resource_hash)
                    };

                let Some(log_records) = sl_obj.get("logRecords").and_then(Value::as_array) else {
                    continue;
                };

                for lr in log_records {
                    let Some(lr_obj) = lr.as_object() else {
                        self.note_otlp_quarantine(
                            &mut quarantine_groups,
                            "missing_timestamp",
                            res_idx,
                            scope_idx,
                            lr.clone(),
                        );
                        continue;
                    };

                    let event_ms = if let Some(ts_val) = lr_obj
                        .get(self.cfg.window.timestamp_field.as_str())
                        .and_then(|v| extract_timestamp_ms(v).ok())
                    {
                        ts_val
                    } else if let Some(nano_val) = lr_obj.get("timeUnixNano") {
                        match extract_otlp_time_nano_ms(nano_val) {
                            Some(ms) => ms,
                            None => {
                                self.note_otlp_quarantine(
                                    &mut quarantine_groups,
                                    "invalid_timestamp",
                                    res_idx,
                                    scope_idx,
                                    lr.clone(),
                                );
                                continue;
                            }
                        }
                    } else {
                        self.note_otlp_quarantine(
                            &mut quarantine_groups,
                            "missing_timestamp",
                            res_idx,
                            scope_idx,
                            lr.clone(),
                        );
                        continue;
                    };

                    if self
                        .watermark
                        .observe(outer_source_key.clone(), event_ms, now_ms)
                        == Admit::Quarantine
                    {
                        self.note_otlp_quarantine(
                            &mut quarantine_groups,
                            "future_drift",
                            res_idx,
                            scope_idx,
                            lr.clone(),
                        );
                        continue;
                    }

                    let lr_attrs = lr_obj.get("attributes");
                    let mut dims = Vec::with_capacity(self.cfg.group_by.len());
                    for dim_name in &self.cfg.group_by {
                        let raw_val = extract_value_from_otlp_record(lr_obj, lr_attrs, dim_name);
                        let normalizer = self.normalizers.get(dim_name);
                        let kernels = &self.kernels;
                        dims.push(dimension_bytes(
                            normalizer,
                            raw_val.as_ref(),
                            |norm, val| {
                                norm.apply_with_resolver(val, kernels, |field| {
                                    extract_value_from_otlp_record(lr_obj, lr_attrs, field)
                                        .map(|v| value_to_string_lossy(&v).into_owned())
                                })
                            },
                        ));
                    }

                    // The attribute scans only run when neither top-level
                    // field parses, as before the shared record.
                    let attr_severity = |name: &str| {
                        extract_value_from_otlp_record(lr_obj, lr_attrs, name)
                            .as_ref()
                            .and_then(parse_severity_value)
                    };
                    let severity = [lr_obj.get("severityNumber"), lr_obj.get("severityText")]
                        .into_iter()
                        .find_map(|value| value.and_then(parse_severity_value))
                        .or_else(|| attr_severity("severity"))
                        .or_else(|| attr_severity("level"))
                        .unwrap_or(0);

                    let raw_body = extract_value_from_otlp_record(lr_obj, lr_attrs, "body")
                        .or_else(|| lr_obj.get("body").cloned());
                    let body_val = raw_body.map(|val| match val {
                        Value::Object(mut map) => {
                            map.remove("stringValue").unwrap_or(Value::Object(map))
                        }
                        other => other,
                    });
                    let policy = self.cfg.attributes_policy.body_policy;
                    let normalizer = self
                        .normalizers
                        .get("body")
                        .or_else(|| self.normalizers.get("message"));
                    let kernels = &self.kernels;
                    let exemplar_body = apply_body_policy(
                        policy,
                        normalizer,
                        kernels,
                        body_val.as_ref(),
                        |norm, val, kernels| {
                            norm.apply_with_resolver(val, kernels, |field| {
                                extract_value_from_otlp_record(lr_obj, lr_attrs, field)
                                    .map(|v| value_to_string_lossy(&v).into_owned())
                            })
                        },
                    );

                    let record = PreparedRecord {
                        event_ms,
                        position: None,
                        severity,
                        body: exemplar_body,
                        dims,
                        resource: Arc::clone(&resource_payload),
                        resource_hash,
                    };
                    match self.publish_prepared(record, now_ms, out, |router| {
                        let timestamp_field = router.cfg.window.timestamp_field.as_str();
                        let (counters, minmax_values) = fill_metric_banks(
                            &router.plan,
                            timestamp_field,
                            &mut router.stats,
                            |_idx, spec| {
                                let field_name =
                                    spec.field.as_deref().unwrap_or(spec.name.as_str());
                                extract_value_from_otlp_record(lr_obj, lr_attrs, field_name)
                                    .map(Cow::Owned)
                            },
                            Some(event_ms),
                        );
                        let attrs = router.extract_otlp_unmatched_attrs(lr_attrs);
                        (counters, minmax_values, attrs)
                    }) {
                        RecordDisposition::Late => {
                            late_records
                                .entry((res_idx, scope_idx))
                                .or_default()
                                .push(lr.clone());
                        }
                        RecordDisposition::Admitted {
                            pushed_shard: Some(shard),
                        } => {
                            routed_shards.insert(shard);
                        }
                        RecordDisposition::Admitted { pushed_shard: None } => {}
                    }
                }
            }
        }

        // Attach outer_position to the last item pushed to each shard that received records from this OTLP envelope.
        if let Some(pos) = outer_position {
            for shard in &routed_shards {
                if let Some(last_item) = out.per_shard.get_mut(*shard).and_then(|v| v.last_mut()) {
                    last_item.position = Some(pos.clone());
                }
            }
        }

        if !routed_shards.is_empty() && quarantine_groups.is_empty() && late_records.is_empty() {
            let _ = event.take_finalizers();
            return;
        }

        // Emit filtered OTLP passthrough envelopes for quarantined records (grouped by reason) and late records.
        for (reason, grouped) in &quarantine_groups {
            let pt = self.build_filtered_otlp_event(
                &event,
                resource_logs,
                grouped,
                "quarantine",
                reason,
            );
            out.passthrough.push(pt);
        }

        if !late_records.is_empty() {
            let pt = self.build_filtered_otlp_event(
                &event,
                resource_logs,
                &late_records,
                "late",
                "late",
            );
            out.passthrough.push(pt);
        }
    }

    fn build_filtered_otlp_event(
        &self,
        template_log: &LogEvent,
        resource_logs: &[Value],
        grouped: &OtlpIndexedRecords,
        route: &str,
        reason: &str,
    ) -> LogEvent {
        let mut filtered_log = template_log.clone();
        let mut by_res: BTreeMap<usize, Vec<(usize, &Vec<Value>)>> = BTreeMap::new();
        for (&(res_idx, scope_idx), recs) in grouped {
            by_res.entry(res_idx).or_default().push((scope_idx, recs));
        }

        let mut filtered_rls = Vec::with_capacity(by_res.len());
        for (res_idx, scopes) in by_res {
            let Some(rl_obj_orig) = resource_logs.get(res_idx).and_then(Value::as_object) else {
                continue;
            };
            let mut rl_obj = rl_obj_orig.clone();
            let orig_scopes = rl_obj_orig.get("scopeLogs").and_then(Value::as_array);

            let mut filtered_sls = Vec::with_capacity(scopes.len());
            for (scope_idx, recs) in scopes {
                let mut sl_obj = orig_scopes
                    .and_then(|arr| arr.get(scope_idx))
                    .and_then(Value::as_object)
                    .cloned()
                    .unwrap_or_default();
                sl_obj.insert("logRecords".into(), Value::Array(recs.clone()));
                filtered_sls.push(Value::Object(sl_obj));
            }
            rl_obj.insert("scopeLogs".into(), Value::Array(filtered_sls));
            filtered_rls.push(Value::Object(rl_obj));
        }

        let _ = filtered_log.insert(&self.resource_logs_path, Value::Array(filtered_rls));
        self.stamp_route(&mut filtered_log, route, reason);
        filtered_log
    }
}

/// True when `pred` holds for `key` or for `key` with a leading `attributes.` removed.
///
/// Callers still apply name literals to the stripped form alone.
fn matches_key_or_stripped(key: &str, stripped: &str, pred: impl Fn(&str) -> bool) -> bool {
    pred(key) || (key != stripped && pred(stripped))
}

fn severity_from<'a>(candidates: impl IntoIterator<Item = Option<&'a Value>>) -> u8 {
    candidates
        .into_iter()
        .find_map(|value| value.and_then(parse_severity_value))
        .unwrap_or(0)
}

fn dimension_bytes(
    normalizer: Option<&NormalizerExecutor>,
    raw: Option<&Value>,
    normalize: impl FnOnce(&NormalizerExecutor, &Value) -> Option<Bytes>,
) -> Option<Bytes> {
    let val = raw?;
    if let Some(norm) = normalizer {
        normalize(norm, val)
    } else {
        Some(Bytes::from(value_to_string_lossy(val).into_owned()))
    }
}

fn apply_body_policy(
    policy: BodyPolicy,
    normalizer: Option<&NormalizerExecutor>,
    kernels: &SimdKernelTable,
    raw_body: Option<&Value>,
    normalize: impl FnOnce(&NormalizerExecutor, &Value, &SimdKernelTable) -> Option<Bytes>,
) -> Option<Bytes> {
    if policy == BodyPolicy::Ignore {
        return None;
    }
    let val = raw_body?;
    if let Some(norm) = normalizer {
        normalize(norm, val, kernels)
    } else if policy == BodyPolicy::ReplaceWithTemplate {
        Some(fingerprint_body(kernels, val))
    } else {
        Some(value_to_bytes(val))
    }
}

fn fingerprint_body(kernels: &SimdKernelTable, body_val: &Value) -> Bytes {
    let input = value_to_bytes(body_val);
    let mut out_bytes = Vec::with_capacity(input.len());
    (kernels.fingerprint_log_bytes)(&input, &mut out_bytes);
    Bytes::from(out_bytes)
}

/// Fills counter banks and min/max slots.
///
/// `missing_timestamp_ms` is `Some(event_ms)` on the OTLP path only. A missing
/// min/max field named `@timestamp`, the configured timestamp field, or `time`
/// then stores that millis value. Native passes `None` so a missing path stays
/// zero. Coerced min/max values do not increment `field_coercions_total`.
///
/// `lookup` returns a [`Cow`] so the native path can borrow the event's value
/// while OTLP hands over the value it already had to build.
fn fill_metric_banks<'v>(
    plan: &AggregationPlan,
    timestamp_field: &str,
    stats: &mut RouterStats,
    mut lookup: impl FnMut(usize, &AggregationSpec) -> Option<Cow<'v, Value>>,
    missing_timestamp_ms: Option<i64>,
) -> (Vec<[u64; 4]>, Vec<i64>) {
    let mut counters = vec![[0u64; 4]; plan.layout().banks];
    let mut minmax_values = vec![0i64; plan.layout().minmax_slots];

    for (idx, spec) in plan.specs().iter().enumerate() {
        if let Some((bank, lane)) = plan.counter_slot(idx) {
            match spec.kind {
                AggregationKind::Count => {
                    counters[bank][lane] = 1;
                }
                AggregationKind::Sum => {
                    if let Some(val) = lookup(idx, spec) {
                        counters[bank][lane] = sum_counter(stats, &val);
                    }
                }
                AggregationKind::Min | AggregationKind::Max => {}
            }
        } else if let Some(slot) = plan.minmax_slot(idx) {
            if let Some(val) = lookup(idx, spec) {
                minmax_values[slot] = minmax_counter(stats, &val);
            } else if let Some(event_ms) = missing_timestamp_ms {
                let field_name = spec.field.as_deref().unwrap_or(spec.name.as_str());
                if field_name == "@timestamp"
                    || field_name == timestamp_field
                    || field_name == "time"
                {
                    minmax_values[slot] = event_ms;
                }
            }
        }
    }

    (counters, minmax_values)
}

fn sum_counter(stats: &mut RouterStats, val: &Value) -> u64 {
    match metric_value(val) {
        MetricValue::Int(i) => i as u64,
        MetricValue::Coerced(i) => {
            stats.field_coercions_total += 1;
            i as u64
        }
        MetricValue::Invalid => {
            stats.field_type_errors_total += 1;
            0
        }
    }
}

fn minmax_counter(stats: &mut RouterStats, val: &Value) -> i64 {
    match metric_value(val) {
        MetricValue::Int(i) | MetricValue::Coerced(i) => i,
        MetricValue::Invalid => match extract_timestamp_ms(val) {
            Ok(ms) => ms,
            Err(_) => {
                stats.field_type_errors_total += 1;
                0
            }
        },
    }
}

fn parse_severity_value(val: &Value) -> Option<u8> {
    match val {
        Value::Number(n) => {
            if let Some(i) = n.as_i64()
                && (1..=24).contains(&i)
            {
                Some(i as u8)
            } else {
                None
            }
        }
        Value::Object(map) => map
            .get("intValue")
            .and_then(parse_severity_value)
            .or_else(|| map.get("stringValue").and_then(parse_severity_value)),
        _ => {
            let s = value_to_string_lossy(val);
            let trimmed = s.trim();
            if let Ok(n) = trimmed.parse::<i64>() {
                if (1..=24).contains(&n) {
                    return Some(n as u8);
                }
                return None;
            }
            match trimmed.to_ascii_uppercase().as_str() {
                "TRACE" | "TRACE1" | "TRACE2" | "TRACE3" | "TRACE4" => Some(1),
                "DEBUG" | "DEBUG1" | "DEBUG2" | "DEBUG3" | "DEBUG4" => Some(5),
                "INFO" | "INFORMATION" | "INFO1" | "INFO2" | "INFO3" | "INFO4" => Some(9),
                "WARN" | "WARNING" | "WARN1" | "WARN2" | "WARN3" | "WARN4" => Some(13),
                "ERROR" | "ERR" | "ERROR1" | "ERROR2" | "ERROR3" | "ERROR4" => Some(17),
                "FATAL" | "CRITICAL" | "CRIT" | "EMERG" | "ALERT" | "FATAL1" | "FATAL2"
                | "FATAL3" | "FATAL4" => Some(21),
                _ => None,
            }
        }
    }
}

fn extract_otlp_attr_bytes(val: &Value) -> Bytes {
    if let Some(map) = val.as_object() {
        if let Some(s) = map.get("stringValue") {
            return Bytes::from(value_to_string_lossy(s).into_owned());
        }
        if let Some(i) = map.get("intValue") {
            return Bytes::from(value_to_string_lossy(i).into_owned());
        }
        if let Some(b) = map.get("boolValue") {
            return Bytes::from(value_to_string_lossy(b).into_owned());
        }
        if let Some(d) = map.get("doubleValue") {
            return Bytes::from(value_to_string_lossy(d).into_owned());
        }
    }
    Bytes::from(value_to_string_lossy(val).into_owned())
}

fn extract_otlp_attr_value(val: Option<&Value>) -> Value {
    match val {
        None => Value::Null,
        Some(Value::Object(map)) => {
            if let Some(s) = map.get("stringValue") {
                Value::String(value_to_string_lossy(s).into_owned())
            } else if let Some(i) = map.get("intValue").and_then(Value::as_i64) {
                Value::Number(serde_json::Number::from(i))
            } else if let Some(b) = map.get("boolValue").and_then(Value::as_bool) {
                Value::Bool(b)
            } else {
                Value::String(Value::Object(map.clone()).to_string())
            }
        }
        Some(Value::String(s)) => Value::String(s.clone()),
        Some(Value::Number(n)) => Value::Number(n.clone()),
        Some(Value::Bool(b)) => Value::Bool(*b),
        Some(other) => Value::String(value_to_string_lossy(other).into_owned()),
    }
}

fn extract_value_from_otlp_record<'a>(
    record: &'a ObjectMap,
    attrs: Option<&'a Value>,
    key: &str,
) -> Option<Value> {
    if let Some(v) = record.get(key) {
        return Some(v.clone());
    }
    if let Some(attrs_val) = attrs {
        match attrs_val {
            Value::Array(arr) => {
                for item in arr {
                    if let Some(obj) = item.as_object()
                        && obj.get("key").map(value_to_string_lossy) == Some(key.into())
                        && let Some(val) = obj.get("value")
                    {
                        if let Some(m) = val.as_object() {
                            if let Some(s) = m.get("stringValue") {
                                return Some(s.clone());
                            }
                            if let Some(i) = m.get("intValue") {
                                return Some(i.clone());
                            }
                        }
                        return Some(val.clone());
                    }
                }
            }
            Value::Object(map) => {
                if let Some(v) = map.get(key) {
                    return Some(v.clone());
                }
            }
            _ => {}
        }
    }
    None
}

fn flatten_object_leaves<'a>(prefix: &str, map: &'a ObjectMap, out: &mut Vec<(String, &'a Value)>) {
    for (k, v) in map {
        let full_key = if prefix.is_empty() {
            k.to_string()
        } else {
            format!("{prefix}.{k}")
        };
        if let Value::Object(sub_map) = v {
            flatten_object_leaves(&full_key, sub_map, out);
        } else {
            out.push((full_key, v));
        }
    }
}
