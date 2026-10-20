//! Slice record rendering to native [`LogEvent`] and OpenTelemetry Logs envelopes.

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
}

static RENDER_PATHS: LazyLock<RenderPaths> = LazyLock::new(|| RenderPaths {
    window_emit_reason: FieldPath::parse_or_literal("window.emit_reason"),
    window_slice_seq: FieldPath::parse_or_literal("window.slice_seq"),
    window_key_digest: FieldPath::parse_or_literal("window.key_digest"),
    window_slice_id: FieldPath::parse_or_literal("window.slice_id"),
    window_emitter_id: FieldPath::parse_or_literal("window.emitter_id"),
    window_start: FieldPath::parse_or_literal("window.start"),
    window_end: FieldPath::parse_or_literal("window.end"),
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

/// Renders a single [`SliceRecord`] into native [`LogEvent`] format following the output contract.
#[must_use]
pub fn render_native(rec: &SliceRecord, cfg: &SluiceConfig, plan: &AggregationPlan) -> LogEvent {
    let mut event = LogEvent::default();

    // 1. Route field
    let route_field = if cfg.output.route_field.is_empty() {
        "_wa_route"
    } else {
        cfg.output.route_field.as_str()
    };
    let _ = event.insert(
        &FieldPath::parse_or_literal(route_field),
        Value::from("summary"),
    );

    // 2. Window metadata fields
    let paths = &*RENDER_PATHS;
    let _ = event.insert(
        &paths.window_emit_reason,
        Value::from(emit_reason_str(rec.emit_reason)),
    );
    let _ = event.insert(
        &paths.window_slice_seq,
        Value::from(i64::from(rec.slice_seq)),
    );
    let _ = event.insert(
        &paths.window_key_digest,
        Value::from(format!("{:032x}", rec.digest)),
    );
    let _ = event.insert(&paths.window_slice_id, Value::from(rec.slice_id.clone()));
    let _ = event.insert(
        &paths.window_emitter_id,
        Value::from(rec.emitter_id.clone()),
    );

    let start_str = format_rfc3339_millis(rec.window_start_ms);
    let _ = event.insert(&paths.window_start, Value::from(start_str));

    let end_str = format_rfc3339_millis(rec.window_end_ms);
    let _ = event.insert(&paths.window_end, Value::from(end_str));

    // 3. Resource and scope fields (only when preserve_resource_and_scope is true)
    if cfg.attributes_policy.preserve_resource_and_scope {
        for (k, v) in &rec.resource.resource {
            let path = format!("resource.{k}");
            let _ = event.insert(
                &FieldPath::parse_or_literal(path.as_str()),
                json_to_value(v),
            );
        }

        if !rec.resource.scope_name.is_empty() {
            let _ = event.insert(
                &FieldPath::parse_or_literal("scope.name"),
                Value::from(rec.resource.scope_name.clone()),
            );
        }
        if !rec.resource.scope_version.is_empty() {
            let _ = event.insert(
                &FieldPath::parse_or_literal("scope.version"),
                Value::from(rec.resource.scope_version.clone()),
            );
        }
        for (k, v) in &rec.resource.scope_attrs {
            let path = format!("scope.{k}");
            let _ = event.insert(
                &FieldPath::parse_or_literal(path.as_str()),
                json_to_value(v),
            );
        }
    }

    // 4. Dimensions and exemplar tail
    for (i, dim_name) in cfg.group_by.iter().enumerate() {
        if let Some(Some(b)) = rec.tail.dims.get(i) {
            let _ = event.insert(
                &FieldPath::parse_or_literal(dim_name.as_str()),
                Value::from(String::from_utf8_lossy(b).into_owned()),
            );
        }
    }

    if rec.tail.severity > 0 {
        if !cfg.group_by.iter().any(|g| g == "severity_number") {
            let _ = event.insert(
                &FieldPath::parse_or_literal("severity_number"),
                Value::from(i64::from(rec.tail.severity)),
            );
        }
        if !cfg.group_by.iter().any(|g| g == "severity")
            && let Some(sev_str) = severity_text(rec.tail.severity)
        {
            let _ = event.insert(
                &FieldPath::parse_or_literal("severity"),
                Value::from(sev_str),
            );
        }
    }

    match cfg.attributes_policy.body_policy {
        BodyPolicy::KeepFirstExemplar => {
            if let Some(ref body) = rec.tail.body
                && !body.is_empty()
            {
                let _ = event.insert(
                    &FieldPath::parse_or_literal("body"),
                    Value::from(String::from_utf8_lossy(body).into_owned()),
                );
            }
        }
        BodyPolicy::ReplaceWithTemplate => {
            if let Some(tmpl_bytes) = resolve_template_bytes(rec, cfg)
                && !tmpl_bytes.is_empty()
            {
                let target_field = if cfg.attributes_policy.template_target_field.is_empty() {
                    "log.template"
                } else {
                    cfg.attributes_policy.template_target_field.as_str()
                };
                let _ = event.insert(
                    &FieldPath::parse_or_literal(target_field),
                    Value::from(String::from_utf8_lossy(tmpl_bytes).into_owned()),
                );
            }
        }
        BodyPolicy::Ignore => {}
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
    for (idx, spec) in plan.specs().iter().enumerate() {
        if let Some((bank, lane)) = plan.counter_slot(idx) {
            let val = rec.counters.get(bank).map_or(0, |b| b[lane]);
            let i_val = i64::try_from(val).unwrap_or(i64::MAX);
            let _ = event.insert(
                &FieldPath::parse_or_literal(spec.name.as_str()),
                Value::from(i_val),
            );
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
            let val_to_insert = if is_timestamp && val > 0 {
                Value::from(format_rfc3339_millis(val))
            } else {
                Value::from(val)
            };
            let _ = event.insert(
                &FieldPath::parse_or_literal(spec.name.as_str()),
                val_to_insert,
            );
        }
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
            if let Some(group) = resource_groups
                .iter_mut()
                .find(|(res, _)| **res == *rec.resource)
            {
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
                let mut lr_obj = ObjectMap::new();
                let time_nano = rec.window_start_ms.saturating_mul(1_000_000);
                lr_obj.insert("timeUnixNano".into(), Value::from(time_nano));

                let end_nano = rec.window_end_ms.saturating_mul(1_000_000);
                lr_obj.insert("observedTimeUnixNano".into(), Value::from(end_nano));

                if rec.tail.severity > 0 {
                    lr_obj.insert(
                        "severityNumber".into(),
                        Value::from(i64::from(rec.tail.severity)),
                    );
                    if let Some(sev_str) = severity_text(rec.tail.severity) {
                        lr_obj.insert("severityText".into(), Value::from(sev_str));
                    }
                }

                // Attributes
                let mut lr_attrs = Vec::new();

                match cfg.attributes_policy.body_policy {
                    BodyPolicy::KeepFirstExemplar => {
                        if let Some(ref body) = rec.tail.body
                            && !body.is_empty()
                        {
                            lr_obj.insert(
                                "body".into(),
                                otlp_value_string(String::from_utf8_lossy(body).into_owned()),
                            );
                        }
                    }
                    BodyPolicy::ReplaceWithTemplate => {
                        if let Some(tmpl_bytes) = resolve_template_bytes(rec, cfg)
                            && !tmpl_bytes.is_empty()
                        {
                            let target_field =
                                if cfg.attributes_policy.template_target_field.is_empty() {
                                    "log.template"
                                } else {
                                    cfg.attributes_policy.template_target_field.as_str()
                                };
                            lr_attrs.push(otlp_attr_string(
                                target_field,
                                String::from_utf8_lossy(tmpl_bytes).into_owned(),
                            ));
                        }
                    }
                    BodyPolicy::Ignore => {}
                }

                let route_field = if cfg.output.route_field.is_empty() {
                    "_wa_route"
                } else {
                    cfg.output.route_field.as_str()
                };
                lr_attrs.push(otlp_attr_string(route_field, "summary"));
                lr_attrs.push(otlp_attr_string(
                    "window.emit_reason",
                    emit_reason_str(rec.emit_reason),
                ));
                lr_attrs.push(otlp_attr_int("window.slice_seq", i64::from(rec.slice_seq)));
                lr_attrs.push(otlp_attr_string(
                    "window.key_digest",
                    format!("{:032x}", rec.digest),
                ));
                lr_attrs.push(otlp_attr_string("window.slice_id", &rec.slice_id));
                lr_attrs.push(otlp_attr_string("window.emitter_id", &rec.emitter_id));

                let start_str = format_rfc3339_millis(rec.window_start_ms);
                lr_attrs.push(otlp_attr_string("window.start", start_str));

                let end_str = format_rfc3339_millis(rec.window_end_ms);
                lr_attrs.push(otlp_attr_string("window.end", end_str));

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
                for (idx, spec) in plan.specs().iter().enumerate() {
                    if let Some((bank, lane)) = plan.counter_slot(idx) {
                        let val = rec.counters.get(bank).map_or(0, |b| b[lane]);
                        let i_val = i64::try_from(val).unwrap_or(i64::MAX);
                        lr_attrs.push(otlp_attr_int(&spec.name, i_val));
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
                            let ts_str = format_rfc3339_millis(val);
                            lr_attrs.push(otlp_attr_string(&spec.name, ts_str));
                        } else {
                            lr_attrs.push(otlp_attr_int(&spec.name, val));
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

        let _ = event.insert(
            &FieldPath::parse_or_literal("resourceLogs"),
            Value::Array(resource_logs),
        );
        envelopes.push(event);
    }

    envelopes
}
