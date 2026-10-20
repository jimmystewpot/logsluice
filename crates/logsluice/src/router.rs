//! Router component mapping events to shards, checking watermarks and horizons,
//! and routing late/quarantined records to passthrough.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use indexmap::IndexMap;
use logsluice_core::codec::plan::AggregationPlan;
use logsluice_core::config::AggregationKind;
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

    fn is_excluded_unmatched_key(&self, key: &str) -> bool {
        let stripped = key.strip_prefix("attributes.").unwrap_or(key);
        if self.group_by_set.contains(key) || self.group_by_set.contains(stripped) {
            return true;
        }
        if self.metric_field_names.contains(key) || self.metric_field_names.contains(stripped) {
            return true;
        }
        if self.cfg.attributes_policy.exclude_attributes.contains(key)
            || self
                .cfg
                .attributes_policy
                .exclude_attributes
                .contains(stripped)
        {
            return true;
        }
        if key == self.cfg.window.timestamp_field
            || stripped == self.cfg.window.timestamp_field
            || key == self.cfg.window.source_id_field
            || stripped == self.cfg.window.source_id_field
            || key == self.cfg.output.route_field
            || stripped == self.cfg.output.route_field
        {
            return true;
        }
        if let Some(ref sp) = self.cfg.source_position
            && (key == sp.topic_field
                || stripped == sp.topic_field
                || key == sp.partition_field
                || stripped == sp.partition_field
                || key == sp.offset_field
                || stripped == sp.offset_field)
        {
            return true;
        }
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
        // 1. Timestamp extraction
        let ts_val = event.get(&self.timestamp_path);
        let event_ms = match ts_val {
            None => {
                let _ = event.insert(&self.route_path, Value::from("quarantine"));
                let _ = event.insert(&self.reason_path, Value::from("missing_timestamp"));
                self.stats.quarantined_total += 1;
                out.passthrough.push(event);
                return;
            }
            Some(v) => match extract_timestamp_ms(v) {
                Ok(ms) => ms,
                Err(_) => {
                    let _ = event.insert(&self.route_path, Value::from("quarantine"));
                    let _ = event.insert(&self.reason_path, Value::from("invalid_timestamp"));
                    self.stats.quarantined_total += 1;
                    out.passthrough.push(event);
                    return;
                }
            },
        };

        // Source position extraction
        let position = self.extract_position(&event);

        let source_key = if let Some(ref pos) = position {
            SourceKey::Partition(pos.topic.clone(), pos.partition)
        } else if let Some(id_val) = event.get(&self.source_id_path) {
            SourceKey::Source(Arc::from(value_to_string_lossy(id_val).into_owned()))
        } else {
            SourceKey::Default
        };

        // 2. Future drift check
        if self.watermark.observe(source_key, event_ms, now_ms) == Admit::Quarantine {
            let _ = event.insert(&self.route_path, Value::from("quarantine"));
            let _ = event.insert(&self.reason_path, Value::from("future_drift"));
            self.stats.quarantined_total += 1;
            out.passthrough.push(event);
            return;
        }

        // 3. Extract dimensions and apply normalizers
        let mut dims = Vec::with_capacity(self.cfg.group_by.len());
        for (i, dim_name) in self.cfg.group_by.iter().enumerate() {
            let path = &self.group_by_paths[i];
            let raw_val = event.get(path);
            let dim_bytes = match raw_val {
                None => None,
                Some(val) => {
                    if let Some(norm) = self.normalizers.get(dim_name) {
                        norm.apply(&event, val, &self.kernels)
                    } else {
                        Some(Bytes::from(value_to_string_lossy(val).into_owned()))
                    }
                }
            };
            dims.push(dim_bytes);
        }

        // Extract severity
        let severity = event
            .get(&self.severity_number_path)
            .and_then(parse_severity_value)
            .or_else(|| {
                event
                    .get(&self.severity_camel_path)
                    .and_then(parse_severity_value)
            })
            .or_else(|| {
                event
                    .get(&self.severity_path)
                    .and_then(parse_severity_value)
            })
            .or_else(|| event.get(&self.level_path).and_then(parse_severity_value))
            .unwrap_or(0);

        // Extract resource and scope according to preserve_resource_and_scope
        let (resource_payload, resource_hash) = self.extract_native_resource(&event);

        // Extract body according to body policy
        let exemplar_body = match self.cfg.attributes_policy.body_policy {
            BodyPolicy::Ignore => None,
            BodyPolicy::KeepFirstExemplar | BodyPolicy::ReplaceWithTemplate => {
                let raw_body = event
                    .get(&self.body_path)
                    .or_else(|| event.get(&self.message_path));
                match raw_body {
                    None => None,
                    Some(val) => {
                        if let Some(norm) = self
                            .normalizers
                            .get("body")
                            .or_else(|| self.normalizers.get("message"))
                        {
                            norm.apply(&event, val, &self.kernels)
                        } else if self.cfg.attributes_policy.body_policy
                            == BodyPolicy::ReplaceWithTemplate
                        {
                            let input = value_to_bytes(val);
                            let mut out_bytes = Vec::new();
                            (self.kernels.fingerprint_log_bytes)(&input, &mut out_bytes);
                            Some(Bytes::from(out_bytes))
                        } else {
                            Some(value_to_bytes(val))
                        }
                    }
                }
            }
        };

        // Compute 128-bit digest and target shard
        let body_bytes_ref = exemplar_body.as_deref();
        let dim_bytes_refs: Vec<Option<&[u8]>> =
            dims.iter().map(|d| d.as_ref().map(|b| &**b)).collect();
        let digest =
            self.key_builder
                .digest(resource_hash, severity, body_bytes_ref, &dim_bytes_refs);

        let shard = ((digest as u64) % (self.horizons.len() as u64)) as usize;

        // 4. Late / Horizon check against target shard
        let window_start = self.clock.start_of(event_ms);
        let window_end = self.clock.end_of(window_start);
        let allowed_lateness_ms = self.cfg.window.allowed_lateness.as_millis() as i64;
        let watermark = self.watermark.watermark(now_ms).unwrap_or(i64::MIN);

        let is_late = window_end.saturating_add(allowed_lateness_ms) <= watermark
            || window_end <= self.horizons[shard].get();

        if is_late {
            let _ = event.insert(&self.route_path, Value::from("late"));
            let _ = event.insert(&self.reason_path, Value::from("late"));
            self.stats.late_total += 1;
            out.passthrough.push(event);
            return;
        }

        // 5. Extract metrics
        let mut counters = vec![[0u64; 4]; self.plan.layout().banks];
        let mut minmax_values = vec![0i64; self.plan.layout().minmax_slots];

        for (idx, spec) in self.plan.specs().iter().enumerate() {
            if let Some((bank, lane)) = self.plan.counter_slot(idx) {
                match spec.kind {
                    AggregationKind::Count => {
                        counters[bank][lane] = 1;
                    }
                    AggregationKind::Sum => {
                        if let Some(ref path) = self.metric_paths[idx]
                            && let Some(val) = event.get(path)
                        {
                            match metric_value(val) {
                                MetricValue::Int(i) => {
                                    counters[bank][lane] = i as u64;
                                }
                                MetricValue::Coerced(i) => {
                                    counters[bank][lane] = i as u64;
                                    self.stats.field_coercions_total += 1;
                                }
                                MetricValue::Invalid => {
                                    counters[bank][lane] = 0;
                                    self.stats.field_type_errors_total += 1;
                                }
                            }
                        }
                    }
                    _ => {}
                }
            } else if let Some(slot) = self.plan.minmax_slot(idx)
                && let Some(ref path) = self.metric_paths[idx]
                && let Some(val) = event.get(path)
            {
                match metric_value(val) {
                    MetricValue::Int(i) | MetricValue::Coerced(i) => {
                        minmax_values[slot] = i;
                    }
                    MetricValue::Invalid => {
                        if let Ok(ms) = extract_timestamp_ms(val) {
                            minmax_values[slot] = ms;
                        } else {
                            minmax_values[slot] = 0;
                            self.stats.field_type_errors_total += 1;
                        }
                    }
                }
            }
        }

        let attrs = self.extract_native_unmatched_attrs(&event);

        // Take finalizers for accepted event
        let _ = event.take_finalizers();

        let item = IngestItem {
            key: StorageKey24 {
                epoch_ms: window_start as u64,
                digest,
            },
            resource: resource_payload,
            resource_hash,
            position,
            event_ms,
            counters,
            minmax_values,
            exemplar: ExemplarSource {
                severity,
                body: exemplar_body,
                dims,
                attrs,
            },
        };

        if shard < out.per_shard.len() {
            out.per_shard[shard].push(item);
        }
    }

    fn route_otlp(&mut self, mut event: LogEvent, now_ms: i64, out: &mut RouterOutput) {
        self.detected_otlp.store(true, Ordering::Relaxed);

        let outer_position = self.extract_position(&event);
        let outer_source_key = if let Some(ref pos) = outer_position {
            SourceKey::Partition(pos.topic.clone(), pos.partition)
        } else if let Some(id_val) = event.get(&self.source_id_path) {
            SourceKey::Source(Arc::from(value_to_string_lossy(id_val).into_owned()))
        } else {
            SourceKey::Default
        };

        if event
            .get(&self.resource_logs_path)
            .and_then(Value::as_array)
            .is_none()
        {
            let _ = event.insert(&self.route_path, Value::from("quarantine"));
            let _ = event.insert(&self.reason_path, Value::from("missing_timestamp"));
            self.stats.quarantined_total += 1;
            out.passthrough.push(event);
            return;
        }
        let Some(resource_logs) = event
            .get(&self.resource_logs_path)
            .and_then(Value::as_array)
            .map(Vec::as_slice)
        else {
            return;
        };

        let mut routed_shards: BTreeSet<usize> = BTreeSet::new();
        let mut quarantine_groups: BTreeMap<&'static str, BTreeMap<(usize, usize), Vec<Value>>> =
            BTreeMap::new();
        let mut late_records: BTreeMap<(usize, usize), Vec<Value>> = BTreeMap::new();

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
                        quarantine_groups
                            .entry("missing_timestamp")
                            .or_default()
                            .entry((res_idx, scope_idx))
                            .or_default()
                            .push(lr.clone());
                        self.stats.quarantined_total += 1;
                        continue;
                    };

                    // 1. Timestamp extraction
                    let event_ms = if let Some(ts_val) = lr_obj
                        .get(self.cfg.window.timestamp_field.as_str())
                        .and_then(|v| extract_timestamp_ms(v).ok())
                    {
                        ts_val
                    } else if let Some(nano_val) = lr_obj.get("timeUnixNano") {
                        match extract_otlp_time_nano_ms(nano_val) {
                            Some(ms) => ms,
                            None => {
                                quarantine_groups
                                    .entry("invalid_timestamp")
                                    .or_default()
                                    .entry((res_idx, scope_idx))
                                    .or_default()
                                    .push(lr.clone());
                                self.stats.quarantined_total += 1;
                                continue;
                            }
                        }
                    } else {
                        quarantine_groups
                            .entry("missing_timestamp")
                            .or_default()
                            .entry((res_idx, scope_idx))
                            .or_default()
                            .push(lr.clone());
                        self.stats.quarantined_total += 1;
                        continue;
                    };

                    // 2. Future drift check
                    if self
                        .watermark
                        .observe(outer_source_key.clone(), event_ms, now_ms)
                        == Admit::Quarantine
                    {
                        quarantine_groups
                            .entry("future_drift")
                            .or_default()
                            .entry((res_idx, scope_idx))
                            .or_default()
                            .push(lr.clone());
                        self.stats.quarantined_total += 1;
                        continue;
                    }

                    // Extract dimensions from log record attributes and apply normalizers
                    let lr_attrs = lr_obj.get("attributes");
                    let mut dims = Vec::with_capacity(self.cfg.group_by.len());
                    for dim_name in &self.cfg.group_by {
                        let raw_val = extract_value_from_otlp_record(lr_obj, lr_attrs, dim_name);
                        let dim_bytes = match raw_val {
                            None => None,
                            Some(val) => {
                                if let Some(norm) = self.normalizers.get(dim_name) {
                                    norm.apply_with_resolver(&val, &self.kernels, |f| {
                                        extract_value_from_otlp_record(lr_obj, lr_attrs, f)
                                            .map(|v| value_to_string_lossy(&v).into_owned())
                                    })
                                } else {
                                    Some(Bytes::from(value_to_string_lossy(&val).into_owned()))
                                }
                            }
                        };
                        dims.push(dim_bytes);
                    }

                    // Extract severity
                    let severity = lr_obj
                        .get("severityNumber")
                        .and_then(parse_severity_value)
                        .or_else(|| lr_obj.get("severityText").and_then(parse_severity_value))
                        .or_else(|| {
                            extract_value_from_otlp_record(lr_obj, lr_attrs, "severity")
                                .and_then(|v| parse_severity_value(&v))
                        })
                        .or_else(|| {
                            extract_value_from_otlp_record(lr_obj, lr_attrs, "level")
                                .and_then(|v| parse_severity_value(&v))
                        })
                        .unwrap_or(0);

                    // Extract body and apply normalizers according to body policy
                    let exemplar_body = match self.cfg.attributes_policy.body_policy {
                        BodyPolicy::Ignore => None,
                        BodyPolicy::KeepFirstExemplar | BodyPolicy::ReplaceWithTemplate => {
                            let raw_body = extract_value_from_otlp_record(lr_obj, lr_attrs, "body")
                                .or_else(|| lr_obj.get("body").cloned());
                            match raw_body {
                                None => None,
                                Some(val) => {
                                    let body_val = match &val {
                                        Value::Object(map) => map
                                            .get("stringValue")
                                            .cloned()
                                            .unwrap_or_else(|| val.clone()),
                                        _ => val,
                                    };
                                    if let Some(norm) = self
                                        .normalizers
                                        .get("body")
                                        .or_else(|| self.normalizers.get("message"))
                                    {
                                        norm.apply_with_resolver(&body_val, &self.kernels, |f| {
                                            extract_value_from_otlp_record(lr_obj, lr_attrs, f)
                                                .map(|v| value_to_string_lossy(&v).into_owned())
                                        })
                                    } else if self.cfg.attributes_policy.body_policy
                                        == BodyPolicy::ReplaceWithTemplate
                                    {
                                        let input = value_to_bytes(&body_val);
                                        let mut out_bytes = Vec::new();
                                        (self.kernels.fingerprint_log_bytes)(
                                            &input,
                                            &mut out_bytes,
                                        );
                                        Some(Bytes::from(out_bytes))
                                    } else {
                                        Some(value_to_bytes(&body_val))
                                    }
                                }
                            }
                        }
                    };

                    // Compute 128-bit digest and target shard
                    let body_ref = exemplar_body.as_deref();
                    let dim_bytes_refs: Vec<Option<&[u8]>> =
                        dims.iter().map(|d| d.as_ref().map(|b| &**b)).collect();
                    let digest =
                        self.key_builder
                            .digest(resource_hash, severity, body_ref, &dim_bytes_refs);

                    let shard = ((digest as u64) % (self.horizons.len() as u64)) as usize;

                    // 3. Late / Horizon check against target shard
                    let window_start = self.clock.start_of(event_ms);
                    let window_end = self.clock.end_of(window_start);
                    let allowed_lateness_ms = self.cfg.window.allowed_lateness.as_millis() as i64;
                    let watermark = self.watermark.watermark(now_ms).unwrap_or(i64::MIN);

                    let is_late = window_end.saturating_add(allowed_lateness_ms) <= watermark
                        || window_end <= self.horizons[shard].get();

                    if is_late {
                        late_records
                            .entry((res_idx, scope_idx))
                            .or_default()
                            .push(lr.clone());
                        self.stats.late_total += 1;
                        continue;
                    }

                    // 4. Extract metrics into counters and minmax_values
                    let mut counters = vec![[0u64; 4]; self.plan.layout().banks];
                    let mut minmax_values = vec![0i64; self.plan.layout().minmax_slots];

                    for (idx, spec) in self.plan.specs().iter().enumerate() {
                        let field_name = spec.field.as_deref().unwrap_or(spec.name.as_str());
                        if let Some((bank, lane)) = self.plan.counter_slot(idx) {
                            match spec.kind {
                                AggregationKind::Count => {
                                    counters[bank][lane] = 1;
                                }
                                AggregationKind::Sum => {
                                    if let Some(val) =
                                        extract_value_from_otlp_record(lr_obj, lr_attrs, field_name)
                                    {
                                        match metric_value(&val) {
                                            MetricValue::Int(i) => {
                                                counters[bank][lane] = i as u64;
                                            }
                                            MetricValue::Coerced(i) => {
                                                counters[bank][lane] = i as u64;
                                                self.stats.field_coercions_total += 1;
                                            }
                                            MetricValue::Invalid => {
                                                counters[bank][lane] = 0;
                                                self.stats.field_type_errors_total += 1;
                                            }
                                        }
                                    }
                                }
                                _ => {}
                            }
                        } else if let Some(slot) = self.plan.minmax_slot(idx) {
                            if let Some(val) =
                                extract_value_from_otlp_record(lr_obj, lr_attrs, field_name)
                            {
                                match metric_value(&val) {
                                    MetricValue::Int(i) | MetricValue::Coerced(i) => {
                                        minmax_values[slot] = i;
                                    }
                                    MetricValue::Invalid => {
                                        if let Ok(ms) = extract_timestamp_ms(&val) {
                                            minmax_values[slot] = ms;
                                        } else {
                                            minmax_values[slot] = 0;
                                            self.stats.field_type_errors_total += 1;
                                        }
                                    }
                                }
                            } else if field_name == "@timestamp"
                                || field_name == self.cfg.window.timestamp_field.as_str()
                                || field_name == "time"
                            {
                                minmax_values[slot] = event_ms;
                            }
                        }
                    }

                    let attrs = self.extract_otlp_unmatched_attrs(lr_attrs);

                    let item = IngestItem {
                        key: StorageKey24 {
                            epoch_ms: window_start as u64,
                            digest,
                        },
                        resource: resource_payload.clone(),
                        resource_hash,
                        position: None,
                        event_ms,
                        counters,
                        minmax_values,
                        exemplar: ExemplarSource {
                            severity,
                            body: exemplar_body,
                            dims,
                            attrs,
                        },
                    };

                    if shard < out.per_shard.len() {
                        out.per_shard[shard].push(item);
                        routed_shards.insert(shard);
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
        grouped: &BTreeMap<(usize, usize), Vec<Value>>,
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
        let _ = filtered_log.insert(&self.route_path, Value::from(route));
        let _ = filtered_log.insert(&self.reason_path, Value::from(reason));
        filtered_log
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
