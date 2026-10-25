//! Slice record rendering to native [`LogEvent`] and OpenTelemetry Logs envelopes.

use std::cell::RefCell;
use std::sync::LazyLock;

use chrono::SecondsFormat;
use logsluice_core::codec::plan::AggregationPlan;
use logsluice_core::codec::ticket::EmitReason;
use logsluice_core::config::AggregationKind;
use logsluice_core::emission::SliceRecord;

use crate::config::{BodyPolicy, NormalizerConfig, SluiceConfig};
use crate::event::{FieldPath, LogEvent, ObjectMap, Value, value_to_string_lossy};

struct RenderPaths {
    window_emit_reason: FieldPath,
    window_slice_seq: FieldPath,
    window_key_digest: FieldPath,
    window_slice_id: FieldPath,
    window_emitter_id: FieldPath,
    window_start: FieldPath,
    window_end: FieldPath,
    scope_name: FieldPath,
    scope_version: FieldPath,
    severity_number: FieldPath,
    severity: FieldPath,
    body: FieldPath,
    default_route: FieldPath,
    log_template: FieldPath,
    resource_logs: FieldPath,
}

static RENDER_PATHS: LazyLock<RenderPaths> = LazyLock::new(|| RenderPaths {
    window_emit_reason: FieldPath::parse_or_literal("window.emit_reason"),
    window_slice_seq: FieldPath::parse_or_literal("window.slice_seq"),
    window_key_digest: FieldPath::parse_or_literal("window.key_digest"),
    window_slice_id: FieldPath::parse_or_literal("window.slice_id"),
    window_emitter_id: FieldPath::parse_or_literal("window.emitter_id"),
    window_start: FieldPath::parse_or_literal("window.start"),
    window_end: FieldPath::parse_or_literal("window.end"),
    scope_name: FieldPath::parse_or_literal("scope.name"),
    scope_version: FieldPath::parse_or_literal("scope.version"),
    severity_number: FieldPath::parse_or_literal("severity_number"),
    severity: FieldPath::parse_or_literal("severity"),
    body: FieldPath::parse_or_literal("body"),
    default_route: FieldPath::parse_or_literal("_wa_route"),
    log_template: FieldPath::parse_or_literal("log.template"),
    resource_logs: FieldPath::parse_or_literal("resourceLogs"),
});

fn severity_text(sev: u8) -> Option<&'static str> {
    match sev {
        1..=4 => Some("TRACE"),
        5..=8 => Some("DEBUG"),
        9..=12 => Some("INFO"),
        13..=16 => Some("WARN"),
        17..=20 => Some("ERROR"),
        21..=24 => Some("FATAL"),
        _ => None,
    }
}

fn resolve_template_bytes<'a>(rec: &'a SliceRecord, cfg: &SluiceConfig) -> Option<&'a [u8]> {
    for (i, dim_name) in cfg.group_by.iter().enumerate() {
        if matches!(
            cfg.normalizers.get(dim_name),
            Some(NormalizerConfig::LogFingerprint)
        ) && let Some(Some(b)) = rec.tail.dims.get(i)
        {
            return Some(b.as_ref());
        }
    }
    rec.tail.body.as_deref()
}

fn format_rfc3339_millis(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms).map_or_else(
        || ms.to_string(),
        |dt| dt.to_rfc3339_opts(SecondsFormat::Millis, true),
    )
}

/// Converts a `serde_json::Value` into the event `Value` representation.
#[must_use]
pub fn json_to_value(v: &serde_json::Value) -> Value {
    v.clone()
}

fn otlp_value_string(s: impl Into<String>) -> Value {
    let mut map = ObjectMap::new();
    map.insert("stringValue".into(), Value::from(s.into()));
    Value::Object(map)
}

fn otlp_value_int(i: i64) -> Value {
    let mut map = ObjectMap::new();
    map.insert("intValue".into(), Value::from(i));
    Value::Object(map)
}

fn otlp_attr(key: &str, val: Value) -> Value {
    let mut map = ObjectMap::new();
    map.insert("key".into(), Value::from(key.to_string()));
    map.insert("value".into(), val);
    Value::Object(map)
}

fn otlp_attr_string(key: &str, s: impl Into<String>) -> Value {
    otlp_attr(key, otlp_value_string(s))
}

fn otlp_attr_int(key: &str, i: i64) -> Value {
    otlp_attr(key, otlp_value_int(i))
}

fn emit_reason_str(reason: EmitReason) -> &'static str {
    match reason {
        EmitReason::Close => "close",
        EmitReason::Periodic => "periodic",
        EmitReason::LateRevision => "late_revision",
        EmitReason::Evict => "evict",
        EmitReason::Drain => "drain",
    }
}

/// Parses a `resource.{key}` or `scope.{key}` path.
///
/// Dotted keys are nested by the parser, and the path is data so it cannot be
/// interned without retaining every attribute name for the process lifetime.
#[must_use]
fn parse_dynamic_path(formatted: &str) -> FieldPath {
    FieldPath::parse_or_literal(formatted)
}

/// Inserts `value` at `raw`, reusing a parsed [`FieldPath`] for that string.
///
/// One record carries the route, the template field, every aggregation name,
/// and each `group_by` dimension. A single cached path is overwritten by the
/// next name on that record, so the lookup never hits. The list stops at 64
/// entries so a thread does not retain every path it has ever rendered.
fn insert_parsed_path(event: &mut LogEvent, raw: &str, value: Value) {
    const MAX_CACHED_PATHS: usize = 64;
    thread_local! {
        static CACHED: RefCell<Vec<FieldPath>> = const { RefCell::new(Vec::new()) };
    }
    CACHED.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(path) = cache.iter().find(|path| path.as_str() == raw) {
            let _ = event.insert(path, value);
            return;
        }
        let parsed = FieldPath::parse_or_literal(raw);
        let _ = event.insert(&parsed, value);
        if cache.len() < MAX_CACHED_PATHS {
            cache.push(parsed);
        }
    });
}

fn route_field_name(cfg: &SluiceConfig) -> &str {
    if cfg.output.route_field.is_empty() {
        RENDER_PATHS.default_route.as_str()
    } else {
        cfg.output.route_field.as_str()
    }
}

fn template_field_name(cfg: &SluiceConfig) -> &str {
    if cfg.attributes_policy.template_target_field.is_empty() {
        RENDER_PATHS.log_template.as_str()
    } else {
        cfg.attributes_policy.template_target_field.as_str()
    }
}

/// Already-formatted summary values for one slice.
///
/// Native writes these into a flat event. OTLP wraps the same strings and ints.
struct SummaryFields<'a> {
    emit_reason: &'static str,
    slice_seq: i64,
    key_digest: String,
    slice_id: &'a str,
    emitter_id: &'a str,
    window_start: String,
    window_end: String,
    /// Set when `tail.severity > 0`. Native still drops names that are dimensions.
    severity_number: Option<i64>,
    severity_text: Option<&'static str>,
    body: Option<SummaryBody<'a>>,
}

enum SummaryBody<'a> {
    Exemplar(String),
    Template { field: &'a str, value: String },
}

enum SummaryAgg<'a> {
    Int { name: &'a str, value: i64 },
    Timestamp { name: &'a str, value: String },
}

/// Formats window metadata, severity text, and body or template text.
///
/// Aggregation values stay an iterator (`summary_aggregations`) so a slice does
/// not allocate a `Vec` that each renderer walks once. `render_native` and
/// `render_otlp` had started to format window fields independently, so a new
/// `EmitReason` or timestamp precision had to be updated twice. Encoding stays
/// split because native events and OTLP envelopes are different JSON shapes.
#[must_use]
fn build_summary<'a>(rec: &'a SliceRecord, cfg: &'a SluiceConfig) -> SummaryFields<'a> {
    let (severity_number, severity_text) = if rec.tail.severity > 0 {
        (
            Some(i64::from(rec.tail.severity)),
            severity_text(rec.tail.severity),
        )
    } else {
        (None, None)
    };

    SummaryFields {
        emit_reason: emit_reason_str(rec.emit_reason),
        slice_seq: i64::from(rec.slice_seq),
        key_digest: format!("{:032x}", rec.digest),
        slice_id: rec.slice_id.as_str(),
        emitter_id: rec.emitter_id.as_str(),
        window_start: format_rfc3339_millis(rec.window_start_ms),
        window_end: format_rfc3339_millis(rec.window_end_ms),
        severity_number,
        severity_text,
        body: summary_body(rec, cfg),
    }
}

fn summary_body<'a>(rec: &SliceRecord, cfg: &'a SluiceConfig) -> Option<SummaryBody<'a>> {
    match cfg.attributes_policy.body_policy {
        BodyPolicy::KeepFirstExemplar => rec
            .tail
            .body
            .as_deref()
            .filter(|body| !body.is_empty())
            .map(|body| SummaryBody::Exemplar(String::from_utf8_lossy(body).into_owned())),
        BodyPolicy::ReplaceWithTemplate => resolve_template_bytes(rec, cfg)
            .filter(|tmpl| !tmpl.is_empty())
            .map(|tmpl| SummaryBody::Template {
                field: template_field_name(cfg),
                value: String::from_utf8_lossy(tmpl).into_owned(),
            }),
        BodyPolicy::Ignore => None,
    }
}

fn summary_aggregations<'a>(
    rec: &'a SliceRecord,
    plan: &'a AggregationPlan,
) -> impl Iterator<Item = SummaryAgg<'a>> + 'a {
    plan.specs()
        .iter()
        .enumerate()
        .filter_map(move |(idx, spec)| {
            if let Some((bank, lane)) = plan.counter_slot(idx) {
                let val = rec
                    .counters
                    .get(bank)
                    .and_then(|b| b.get(lane))
                    .copied()
                    .unwrap_or(0);
                let i_val = i64::try_from(val).unwrap_or(i64::MAX);
                Some(SummaryAgg::Int {
                    name: spec.name.as_str(),
                    value: i_val,
                })
            } else if let Some(slot) = plan.minmax_slot(idx) {
                let (min_val, max_val) = rec.minmax.get(slot).copied().unwrap_or((0, 0));
                let val = match spec.kind {
                    AggregationKind::Min => min_val,
                    _ => max_val,
                };
                let is_timestamp = spec
                    .field
                    .as_deref()
                    .is_some_and(|f| f.contains("time") || f == "@timestamp")
                    || spec.name.contains("time");
                if is_timestamp && val > 0 {
                    Some(SummaryAgg::Timestamp {
                        name: spec.name.as_str(),
                        value: format_rfc3339_millis(val),
                    })
                } else {
                    Some(SummaryAgg::Int {
                        name: spec.name.as_str(),
                        value: val,
                    })
                }
            } else {
                None
            }
        })
}

/// Renders a single [`SliceRecord`] into native [`LogEvent`] format following the output contract.
#[must_use]
pub fn render_native(rec: &SliceRecord, cfg: &SluiceConfig, plan: &AggregationPlan) -> LogEvent {
    let mut event = LogEvent::default();
    let paths = &*RENDER_PATHS;
    let summary = build_summary(rec, cfg);

    // 1. Route field
    if cfg.output.route_field.is_empty() {
        let _ = event.insert(&paths.default_route, Value::from("summary"));
    } else {
        insert_parsed_path(
            &mut event,
            cfg.output.route_field.as_str(),
            Value::from("summary"),
        );
    }

    // 2. Window metadata fields
    let _ = event.insert(&paths.window_emit_reason, Value::from(summary.emit_reason));
    let _ = event.insert(&paths.window_slice_seq, Value::from(summary.slice_seq));
    let _ = event.insert(&paths.window_key_digest, Value::from(summary.key_digest));
    let _ = event.insert(&paths.window_slice_id, Value::from(summary.slice_id));
    let _ = event.insert(&paths.window_emitter_id, Value::from(summary.emitter_id));
    let _ = event.insert(&paths.window_start, Value::from(summary.window_start));
    let _ = event.insert(&paths.window_end, Value::from(summary.window_end));

    // 3. Resource and scope fields (only when preserve_resource_and_scope is true)
    if cfg.attributes_policy.preserve_resource_and_scope {
        for (k, v) in &rec.resource.resource {
            let path = format!("resource.{k}");
            let _ = event.insert(&parse_dynamic_path(&path), json_to_value(v));
        }

        if !rec.resource.scope_name.is_empty() {
            let _ = event.insert(
                &paths.scope_name,
                Value::from(rec.resource.scope_name.clone()),
            );
        }
        if !rec.resource.scope_version.is_empty() {
            let _ = event.insert(
                &paths.scope_version,
                Value::from(rec.resource.scope_version.clone()),
            );
        }
        for (k, v) in &rec.resource.scope_attrs {
            let path = format!("scope.{k}");
            let _ = event.insert(&parse_dynamic_path(&path), json_to_value(v));
        }
    }

    // 4. Dimensions and exemplar tail
    for (i, dim_name) in cfg.group_by.iter().enumerate() {
        if let Some(Some(b)) = rec.tail.dims.get(i) {
            insert_parsed_path(
                &mut event,
                dim_name.as_str(),
                Value::from(String::from_utf8_lossy(b).into_owned()),
            );
        }
    }

    // Names already present as dimensions stay on those paths only.
    if let Some(number) = summary.severity_number
        && !cfg
            .group_by
            .iter()
            .any(|g| g == paths.severity_number.as_str())
    {
        let _ = event.insert(&paths.severity_number, Value::from(number));
    }
    if let Some(text) = summary.severity_text
        && !cfg.group_by.iter().any(|g| g == paths.severity.as_str())
    {
        let _ = event.insert(&paths.severity, Value::from(text));
    }

    match summary.body {
        Some(SummaryBody::Exemplar(text)) => {
            let _ = event.insert(&paths.body, Value::from(text));
        }
        Some(SummaryBody::Template { field, value }) => {
            if field == paths.log_template.as_str() {
                let _ = event.insert(&paths.log_template, Value::from(value));
            } else {
                insert_parsed_path(&mut event, field, Value::from(value));
            }
        }
        None => {}
    }

    for (k, v) in &rec.tail.attrs {
        let path = if k.starts_with("attributes.") {
            k.clone()
        } else {
            format!("attributes.{k}")
        };
        let _ = event.insert(
            &FieldPath::parse_or_literal(path.as_str()),
            Value::from(String::from_utf8_lossy(v).into_owned()),
        );
    }

    // 5. Aggregations from plan
    for agg in summary_aggregations(rec, plan) {
        let (name, val) = match agg {
            SummaryAgg::Int { name, value } => (name, Value::from(value)),
            SummaryAgg::Timestamp { name, value } => (name, Value::from(value)),
        };
        insert_parsed_path(&mut event, name, val);
    }

    event
}

/// Renders a slice of records into chunked OpenTelemetry Logs (`resourceLogs`) envelopes.
#[must_use]
pub fn render_otlp(
    recs: &[SliceRecord],
    cfg: &SluiceConfig,
    plan: &AggregationPlan,
    max_per_envelope: usize,
) -> Vec<LogEvent> {
    if recs.is_empty() {
        return Vec::new();
    }

    let paths = &*RENDER_PATHS;
    let chunk_limit = if max_per_envelope == 0 {
        500
    } else {
        max_per_envelope
    };
    let mut envelopes = Vec::new();

    for chunk in recs.chunks(chunk_limit) {
        let mut event = LogEvent::default();

        // Group chunk records by resource canonical hash and scope
        let mut resource_groups: Vec<(
            std::sync::Arc<logsluice_core::resource::ResourceScopePayload>,
            Vec<&SliceRecord>,
        )> = Vec::new();
        for rec in chunk {
            if let Some(group) = resource_groups.iter_mut().find(|(res, _)| {
                std::sync::Arc::ptr_eq(res, &rec.resource) || **res == *rec.resource
            }) {
                group.1.push(rec);
            } else {
                resource_groups.push((rec.resource.clone(), vec![rec]));
            }
        }

        let mut resource_logs = Vec::new();

        for (resource_payload, records) in resource_groups {
            // Resource object
            let mut res_map = ObjectMap::new();
            let mut res_attrs = Vec::new();
            if cfg.attributes_policy.preserve_resource_and_scope {
                for (k, v) in &resource_payload.resource {
                    let otlp_v = match v {
                        Value::String(s) => otlp_value_string(s),
                        Value::Number(n) => {
                            if let Some(i) = n.as_i64() {
                                otlp_value_int(i)
                            } else {
                                otlp_value_string(n.to_string())
                            }
                        }
                        Value::Bool(b) => {
                            let mut m = ObjectMap::new();
                            m.insert("boolValue".into(), Value::Bool(*b));
                            Value::Object(m)
                        }
                        _ => otlp_value_string(value_to_string_lossy(v).into_owned()),
                    };
                    res_attrs.push(otlp_attr(k, otlp_v));
                }
            }
            res_map.insert("attributes".into(), Value::Array(res_attrs));

            // ScopeLogs
            let mut scope_map = ObjectMap::new();
            if cfg.attributes_policy.preserve_resource_and_scope {
                scope_map.insert(
                    "name".into(),
                    Value::from(resource_payload.scope_name.clone()),
                );
                scope_map.insert(
                    "version".into(),
                    Value::from(resource_payload.scope_version.clone()),
                );
                let mut scope_attrs = Vec::new();
                for (k, v) in &resource_payload.scope_attrs {
                    scope_attrs.push(otlp_attr_string(k, value_to_string_lossy(v).into_owned()));
                }
                scope_map.insert("attributes".into(), Value::Array(scope_attrs));
            } else {
                scope_map.insert("name".into(), Value::from(String::new()));
                scope_map.insert("version".into(), Value::from(String::new()));
                scope_map.insert("attributes".into(), Value::Array(Vec::new()));
            }

            let mut log_records = Vec::with_capacity(records.len());
            for rec in records {
                let summary = build_summary(rec, cfg);
                let mut lr_obj = ObjectMap::new();
                let time_nano = rec.window_start_ms.saturating_mul(1_000_000);
                lr_obj.insert("timeUnixNano".into(), Value::from(time_nano));

                let end_nano = rec.window_end_ms.saturating_mul(1_000_000);
                lr_obj.insert("observedTimeUnixNano".into(), Value::from(end_nano));

                if let Some(number) = summary.severity_number {
                    lr_obj.insert("severityNumber".into(), Value::from(number));
                }
                if let Some(text) = summary.severity_text {
                    lr_obj.insert("severityText".into(), Value::from(text));
                }

                // Optional template attr, route, seven window fields, then
                // dimensions, exemplar attrs, and aggs.
                let template_attr =
                    usize::from(matches!(summary.body, Some(SummaryBody::Template { .. })));
                let mut lr_attrs = Vec::with_capacity(
                    template_attr
                        + 8
                        + plan.specs().len()
                        + rec.tail.attrs.len()
                        + cfg.group_by.len(),
                );

                match summary.body {
                    Some(SummaryBody::Exemplar(text)) => {
                        lr_obj.insert("body".into(), otlp_value_string(text));
                    }
                    Some(SummaryBody::Template { field, value }) => {
                        lr_attrs.push(otlp_attr_string(field, value));
                    }
                    None => {}
                }

                lr_attrs.push(otlp_attr_string(route_field_name(cfg), "summary"));
                lr_attrs.push(otlp_attr_string(
                    paths.window_emit_reason.as_str(),
                    summary.emit_reason,
                ));
                lr_attrs.push(otlp_attr_int(
                    paths.window_slice_seq.as_str(),
                    summary.slice_seq,
                ));
                lr_attrs.push(otlp_attr_string(
                    paths.window_key_digest.as_str(),
                    summary.key_digest,
                ));
                lr_attrs.push(otlp_attr_string(
                    paths.window_slice_id.as_str(),
                    summary.slice_id,
                ));
                lr_attrs.push(otlp_attr_string(
                    paths.window_emitter_id.as_str(),
                    summary.emitter_id,
                ));
                lr_attrs.push(otlp_attr_string(
                    paths.window_start.as_str(),
                    summary.window_start,
                ));
                lr_attrs.push(otlp_attr_string(
                    paths.window_end.as_str(),
                    summary.window_end,
                ));

                // Dimensions
                for (i, dim_name) in cfg.group_by.iter().enumerate() {
                    if let Some(Some(b)) = rec.tail.dims.get(i) {
                        lr_attrs.push(otlp_attr_string(
                            dim_name,
                            String::from_utf8_lossy(b).into_owned(),
                        ));
                    }
                }

                // Custom exemplar attributes
                for (k, v) in &rec.tail.attrs {
                    lr_attrs.push(otlp_attr_string(k, String::from_utf8_lossy(v).into_owned()));
                }

                // Aggregations
                for agg in summary_aggregations(rec, plan) {
                    match agg {
                        SummaryAgg::Int { name, value } => {
                            lr_attrs.push(otlp_attr_int(name, value));
                        }
                        SummaryAgg::Timestamp { name, value } => {
                            lr_attrs.push(otlp_attr_string(name, value));
                        }
                    }
                }

                lr_obj.insert("attributes".into(), Value::Array(lr_attrs));
                log_records.push(Value::Object(lr_obj));
            }

            let mut sl_obj = ObjectMap::new();
            sl_obj.insert("scope".into(), Value::Object(scope_map));
            sl_obj.insert("logRecords".into(), Value::Array(log_records));

            let mut rl_obj = ObjectMap::new();
            rl_obj.insert("resource".into(), Value::Object(res_map));
            rl_obj.insert(
                "scopeLogs".into(),
                Value::Array(vec![Value::Object(sl_obj)]),
            );

            resource_logs.push(Value::Object(rl_obj));
        }

        let _ = event.insert(&paths.resource_logs, Value::Array(resource_logs));
        envelopes.push(event);
    }

    envelopes
}
