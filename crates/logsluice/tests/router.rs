#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use logsluice::config::{AdapterConfig, BodyPolicy, SimdMode, UnmatchedLogAttributes};
use logsluice::event::{FieldPath, LogEvent, ObjectMap, Value, value_to_string_lossy};
use logsluice::extract::*;
use logsluice::router::*;
use logsluice_core::{
    simd::SimdKernelTable,
    watermark::{SourceKey, WatermarkConfig, WatermarkTracker},
    worker::PurgeHorizon,
};

const NOW: i64 = 1792195800000; // 2026-10-17T00:10:00Z in ms
const DAY_MS: i64 = 86_400_000;
const T: &str = "2026-10-17T00:00:01Z";

fn cfg() -> AdapterConfig {
    serde_yaml_ng::from_str(include_str!("fixtures/spec_example.yaml")).unwrap()
}

fn ev(fields: &[(&str, Value)]) -> LogEvent {
    let mut l = LogEvent::default();
    for (k, v) in fields {
        l.insert(&FieldPath::parse(k).unwrap(), v.clone());
    }
    l
}

fn test_router(cfg: AdapterConfig) -> Router {
    let cfg = Arc::new(cfg);
    let kernels = SimdKernelTable::detect(cfg.acceleration.simd_mode);
    let horizons = (0..cfg.shards).map(|_| PurgeHorizon::new(0)).collect();
    let watermark_cfg = WatermarkConfig {
        allowed_skew_ms: cfg.window.allowed_skew.as_millis() as i64,
        max_future_drift_ms: cfg.window.max_future_drift.as_millis() as i64,
        idle_timeout_ms: cfg.window.idle_timeout.as_millis() as i64,
        caught_up_threshold_ms: cfg.window.caught_up_threshold.as_millis() as i64,
        seed_grace_ms: 0,
        max_tracked_sources: cfg.window.max_tracked_sources,
    };
    let tracker = WatermarkTracker::new(watermark_cfg, 0);
    Router::new(cfg, kernels, 12345, horizons, tracker).unwrap()
}

fn test_router_with_watermark(cfg: AdapterConfig, watermark_ms: i64) -> Router {
    let cfg = Arc::new(cfg);
    let kernels = SimdKernelTable::detect(cfg.acceleration.simd_mode);
    let horizons = (0..cfg.shards).map(|_| PurgeHorizon::new(0)).collect();
    let allowed_skew_ms = cfg.window.allowed_skew.as_millis() as i64;
    let watermark_cfg = WatermarkConfig {
        allowed_skew_ms,
        max_future_drift_ms: cfg.window.max_future_drift.as_millis() as i64,
        idle_timeout_ms: cfg.window.idle_timeout.as_millis() as i64,
        caught_up_threshold_ms: cfg.window.caught_up_threshold.as_millis() as i64,
        seed_grace_ms: 0,
        max_tracked_sources: cfg.window.max_tracked_sources,
    };
    let mut tracker = WatermarkTracker::new(watermark_cfg, 0);
    tracker.observe(SourceKey::Default, watermark_ms + allowed_skew_ms, NOW);
    Router::new(cfg, kernels, 12345, horizons, tracker).unwrap()
}

fn test_router_with_horizon(cfg: AdapterConfig, horizon_ms: i64) -> Router {
    let cfg = Arc::new(cfg);
    let kernels = SimdKernelTable::detect(cfg.acceleration.simd_mode);
    let horizons = (0..cfg.shards)
        .map(|_| PurgeHorizon::new(horizon_ms))
        .collect();
    let watermark_cfg = WatermarkConfig {
        allowed_skew_ms: cfg.window.allowed_skew.as_millis() as i64,
        max_future_drift_ms: cfg.window.max_future_drift.as_millis() as i64,
        idle_timeout_ms: cfg.window.idle_timeout.as_millis() as i64,
        caught_up_threshold_ms: cfg.window.caught_up_threshold.as_millis() as i64,
        seed_grace_ms: 0,
        max_tracked_sources: cfg.window.max_tracked_sources,
    };
    let tracker = WatermarkTracker::new(watermark_cfg, 0);
    Router::new(cfg, kernels, 12345, horizons, tracker).unwrap()
}

fn otlp_fixture(services: &[(&str, &[i64])]) -> LogEvent {
    let mut resource_logs = Vec::new();

    for (svc, timestamps) in services {
        let mut resource_attrs = Vec::new();
        let mut attr = ObjectMap::new();
        attr.insert("key".into(), Value::from("service.name"));
        let mut str_val = ObjectMap::new();
        str_val.insert("stringValue".into(), Value::from(*svc));
        attr.insert("value".into(), Value::Object(str_val));
        resource_attrs.push(Value::Object(attr));

        let mut resource = ObjectMap::new();
        resource.insert("attributes".into(), Value::Array(resource_attrs));

        let mut log_records = Vec::new();
        for &ts_ms in *timestamps {
            let mut record = ObjectMap::new();
            record.insert("timeUnixNano".into(), Value::from(ts_ms * 1_000_000));
            log_records.push(Value::Object(record));
        }

        let mut scope = ObjectMap::new();
        scope.insert("name".into(), Value::from("test-scope"));
        scope.insert("version".into(), Value::from("1.0.0"));

        let mut scope_log = ObjectMap::new();
        scope_log.insert("scope".into(), Value::Object(scope));
        scope_log.insert("logRecords".into(), Value::Array(log_records));

        let mut rl = ObjectMap::new();
        rl.insert("resource".into(), Value::Object(resource));
        rl.insert(
            "scopeLogs".into(),
            Value::Array(vec![Value::Object(scope_log)]),
        );

        resource_logs.push(Value::Object(rl));
    }

    let mut log = LogEvent::default();
    log.insert_path("resourceLogs", Value::Array(resource_logs));
    log
}

#[test]
fn test_missing_and_invalid_timestamp_quarantined() {
    let mut r = test_router(cfg());
    let mut out = RouterOutput::new(4);
    r.route(ev(&[("source.ip", "10.0.0.1".into())]), NOW, &mut out);
    r.route(ev(&[("@timestamp", "not-a-time".into())]), NOW, &mut out);
    let reasons: Vec<_> = out
        .passthrough
        .iter()
        .map(|e| value_to_string_lossy(e.get_path("_wa_reason").unwrap()).into_owned())
        .collect();
    assert_eq!(reasons, vec!["missing_timestamp", "invalid_timestamp"]);
    assert!(
        out.passthrough
            .iter()
            .all(|e| { value_to_string_lossy(e.get_path("_wa_route").unwrap()) == "quarantine" })
    );
}

#[test]
fn test_metric_value_coercion() {
    assert_eq!(metric_value(&Value::from(-5)), MetricValue::Int(-5));
    assert_eq!(metric_value(&Value::from(2.9_f64)), MetricValue::Coerced(2));
    assert_eq!(metric_value(&Value::from("abc")), MetricValue::Invalid);
    let mut r = test_router(cfg());
    let mut out = RouterOutput::new(4);
    r.route(
        ev(&[("@timestamp", T.into()), ("network.bytes", Value::from(-5))]),
        NOW,
        &mut out,
    );
    r.route(
        ev(&[("@timestamp", T.into()), ("network.bytes", "abc".into())]),
        NOW,
        &mut out,
    );
    let items: Vec<_> = out.per_shard.iter().flatten().collect();
    assert_eq!(items[0].counters[0][1], (-5i64) as u64);
    assert_eq!(items[1].counters[0][1], 0);
    assert_eq!(r.stats().field_type_errors_total, 1);
}

#[test]
fn test_otlp_mixed_windows_and_scopes() {
    let mut r = test_router(cfg());
    let mut out = RouterOutput::new(4);
    r.route(
        otlp_fixture(&[("svc-a", &[DAY_MS - 1, DAY_MS]), ("svc-b", &[1])]),
        NOW,
        &mut out,
    );
    let items: Vec<_> = out.per_shard.iter().flatten().collect();
    assert_eq!(items.len(), 3);
    let epochs: std::collections::BTreeSet<_> = items.iter().map(|i| i.key.epoch_ms).collect();
    assert_eq!(epochs.len(), 2);
    let a: Vec<_> = items
        .iter()
        .filter(|i| i.resource.resource["service.name"] == "svc-a")
        .collect();
    assert!(std::sync::Arc::ptr_eq(&a[0].resource, &a[1].resource));
}

#[test]
fn late_and_horizon_route_to_late() {
    let mut r = test_router_with_watermark(cfg(), DAY_MS + 3_600_000);
    let mut out = RouterOutput::new(4);
    r.route(
        ev(&[("@timestamp", "1970-01-01T00:00:01Z".into())]),
        NOW,
        &mut out,
    );
    assert_eq!(
        value_to_string_lossy(out.passthrough[0].get_path("_wa_route").unwrap()),
        "late"
    );
    let mut r = test_router_with_horizon(cfg(), DAY_MS);
    let mut out = RouterOutput::new(4);
    r.route(
        ev(&[("@timestamp", "1970-01-01T00:00:01Z".into())]),
        NOW,
        &mut out,
    );
    assert_eq!(
        value_to_string_lossy(out.passthrough[0].get_path("_wa_route").unwrap()),
        "late"
    );
}

#[test]
fn source_position_extracted_and_shard_deterministic() {
    let mut r = test_router(cfg());
    let mut out = RouterOutput::new(4);
    let e = ev(&[
        ("@timestamp", T.into()),
        ("source.ip", "10.0.0.1".into()),
        ("topic", "fw".into()),
        ("partition", Value::from(3)),
        ("offset", Value::from(42)),
    ]);
    r.route(e.clone(), NOW, &mut out);
    r.route(e, NOW, &mut out);
    let (shard, items) = out
        .per_shard
        .iter()
        .enumerate()
        .find(|(_, v)| !v.is_empty())
        .unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].position.as_ref().unwrap().offset, 42);
    assert_eq!(shard as u64, (items[0].key.digest as u64) % 4);
}

#[test]
fn test_per_shard_horizon_independence() {
    let cfg = Arc::new(cfg());
    let kernels = SimdKernelTable::detect(cfg.acceleration.simd_mode);
    let watermark_cfg = WatermarkConfig {
        allowed_skew_ms: cfg.window.allowed_skew.as_millis() as i64,
        max_future_drift_ms: cfg.window.max_future_drift.as_millis() as i64,
        idle_timeout_ms: cfg.window.idle_timeout.as_millis() as i64,
        caught_up_threshold_ms: cfg.window.caught_up_threshold.as_millis() as i64,
        seed_grace_ms: 0,
        max_tracked_sources: cfg.window.max_tracked_sources,
    };
    let tracker = WatermarkTracker::new(watermark_cfg, 0);

    let horizons: Vec<_> = (0..cfg.shards)
        .map(|i| {
            if i == 0 {
                PurgeHorizon::new(DAY_MS)
            } else {
                PurgeHorizon::new(0)
            }
        })
        .collect();

    let mut r = Router::new(cfg, kernels, 12345, horizons, tracker).unwrap();

    let mut found_shard0_late = false;
    let mut found_other_admitted = false;

    for ip_suffix in 0..100 {
        let ip = format!("10.0.0.{ip_suffix}");
        let e = ev(&[
            ("@timestamp", "1970-01-01T00:00:01Z".into()),
            ("source.ip", ip.into()),
        ]);
        let mut out = RouterOutput::new(4);
        r.route(e, NOW, &mut out);

        if !out.passthrough.is_empty() {
            let reason = value_to_string_lossy(out.passthrough[0].get_path("_wa_route").unwrap());
            if reason == "late" {
                found_shard0_late = true;
            }
        } else {
            let (shard, items) = out
                .per_shard
                .iter()
                .enumerate()
                .find(|(_, v)| !v.is_empty())
                .unwrap();
            if shard != 0 && !items.is_empty() {
                found_other_admitted = true;
            }
        }
        if found_shard0_late && found_other_admitted {
            break;
        }
    }

    assert!(
        found_shard0_late,
        "Event routing to shard 0 must be marked late"
    );
    assert!(
        found_other_admitted,
        "Event routing to other shards must be admitted"
    );
}

#[test]
fn test_otlp_metrics_and_normalizers() {
    let r_cfg = cfg();
    let mut r = test_router(r_cfg);
    let mut out = RouterOutput::new(4);

    let mut record = ObjectMap::new();
    record.insert("timeUnixNano".into(), Value::from(10_000_000_000_i64));

    let mut attrs = Vec::new();
    let mut ip_attr = ObjectMap::new();
    ip_attr.insert("key".into(), Value::from("source.ip"));
    let mut ip_val = ObjectMap::new();
    ip_val.insert("stringValue".into(), Value::from("192.168.1.55"));
    ip_attr.insert("value".into(), Value::Object(ip_val));
    attrs.push(Value::Object(ip_attr));

    let mut act_attr = ObjectMap::new();
    act_attr.insert("key".into(), Value::from("event.action"));
    let mut act_val = ObjectMap::new();
    act_val.insert("stringValue".into(), Value::from("deny"));
    act_attr.insert("value".into(), Value::Object(act_val));
    attrs.push(Value::Object(act_attr));

    let mut bytes_attr = ObjectMap::new();
    bytes_attr.insert("key".into(), Value::from("network.bytes"));
    let mut bytes_val = ObjectMap::new();
    bytes_val.insert("intValue".into(), Value::from(500));
    bytes_attr.insert("value".into(), Value::Object(bytes_val));
    attrs.push(Value::Object(bytes_attr));

    record.insert("attributes".into(), Value::Array(attrs));

    let mut scope_log = ObjectMap::new();
    scope_log.insert(
        "logRecords".into(),
        Value::Array(vec![Value::Object(record)]),
    );

    let mut res_log = ObjectMap::new();
    res_log.insert(
        "scopeLogs".into(),
        Value::Array(vec![Value::Object(scope_log)]),
    );

    let mut root = ObjectMap::new();
    root.insert(
        "resourceLogs".into(),
        Value::Array(vec![Value::Object(res_log)]),
    );

    let event = LogEvent::from_map(root);
    r.route(event, NOW, &mut out);

    let items: Vec<_> = out.per_shard.iter().flatten().collect();
    assert_eq!(items.len(), 1);

    let dim_ip = items[0].exemplar.dims[0].as_ref().unwrap();
    assert_eq!(std::str::from_utf8(dim_ip).unwrap(), "192.168.1.0");

    assert_eq!(items[0].counters[0][0], 1);
    assert_eq!(items[0].counters[0][1], 500);

    assert_eq!(items[0].minmax_values[0], 10_000);
    assert_eq!(items[0].minmax_values[1], 10_000);
}

#[test]
fn otlp_mixed_batch_routes_valid_records_and_attaches_position_per_shard() {
    use std::time::Duration;

    let mut cfg: AdapterConfig =
        serde_yaml_ng::from_str(include_str!("fixtures/spec_example.yaml")).unwrap();
    cfg.shards = 2;
    cfg.attributes_policy.unmatched_log_attributes = UnmatchedLogAttributes::KeepFirst;
    let cfg = Arc::new(cfg);
    let kernels = SimdKernelTable::detect(SimdMode::Scalar);
    let horizons = vec![PurgeHorizon::new(i64::MIN), PurgeHorizon::new(i64::MIN)];
    let wm = WatermarkTracker::new(
        WatermarkConfig {
            allowed_skew_ms: Duration::from_secs(30).as_millis() as i64,
            max_future_drift_ms: Duration::from_secs(600).as_millis() as i64,
            idle_timeout_ms: Duration::from_secs(300).as_millis() as i64,
            caught_up_threshold_ms: Duration::from_secs(120).as_millis() as i64,
            seed_grace_ms: 0,
            max_tracked_sources: 10_000,
        },
        0,
    );
    let mut router = Router::new(cfg, kernels, 42, horizons, wm).unwrap();

    let mut root = ObjectMap::new();
    root.insert("topic".into(), Value::from("logs-otlp"));
    root.insert("partition".into(), Value::from(0));
    root.insert("offset".into(), Value::from(77));

    let make_rec = |ts_opt: Option<&str>, msg: &str, host: &str| -> Value {
        let mut r = ObjectMap::new();
        if let Some(ts) = ts_opt {
            r.insert("timeUnixNano".into(), Value::from(ts));
        }
        r.insert("severityText".into(), Value::from("ERROR"));
        let mut body_map = ObjectMap::new();
        body_map.insert("stringValue".into(), Value::from(msg));
        r.insert("body".into(), Value::Object(body_map));
        let mut attr_kv = ObjectMap::new();
        attr_kv.insert("key".into(), Value::from("host"));
        let mut attr_val = ObjectMap::new();
        attr_val.insert("stringValue".into(), Value::from(host));
        attr_kv.insert("value".into(), Value::Object(attr_val));
        let mut extra_kv = ObjectMap::new();
        extra_kv.insert("key".into(), Value::from("request_id"));
        let mut extra_val = ObjectMap::new();
        extra_val.insert("stringValue".into(), Value::from("req-123"));
        extra_kv.insert("value".into(), Value::Object(extra_val));
        let mut excl_kv = ObjectMap::new();
        excl_kv.insert("key".into(), Value::from("trace_id"));
        let mut excl_val = ObjectMap::new();
        excl_val.insert("stringValue".into(), Value::from("drop-me"));
        excl_kv.insert("value".into(), Value::Object(excl_val));
        r.insert(
            "attributes".into(),
            Value::Array(vec![
                Value::Object(attr_kv),
                Value::Object(extra_kv),
                Value::Object(excl_kv),
            ]),
        );
        Value::Object(r)
    };

    let mut scope_log = ObjectMap::new();
    scope_log.insert(
        "logRecords".into(),
        Value::Array(vec![
            make_rec(Some("1700000005000000000"), "fail A", "web-1"),
            make_rec(Some("1700000005000000000"), "fail B", "web-2"),
            make_rec(None, "missing ts", "web-3"),
        ]),
    );
    let mut res_log = ObjectMap::new();
    res_log.insert(
        "scopeLogs".into(),
        Value::Array(vec![Value::Object(scope_log)]),
    );
    root.insert(
        "resourceLogs".into(),
        Value::Array(vec![Value::Object(res_log)]),
    );

    let mut out = RouterOutput::new(2);
    router.route(LogEvent::from_map(root), 1_700_000_006_000, &mut out);

    assert!(
        router
            .detected_otlp_handle()
            .load(std::sync::atomic::Ordering::Relaxed)
    );

    let total_routed: usize = out.per_shard.iter().map(|s| s.len()).sum();
    assert_eq!(total_routed, 2);
    assert_eq!(out.passthrough.len(), 1);

    let pt_log = &out.passthrough[0];
    assert_eq!(
        value_to_string_lossy(pt_log.get_path("_wa_route").unwrap()),
        "quarantine"
    );
    assert_eq!(
        value_to_string_lossy(pt_log.get_path("_wa_reason").unwrap()),
        "missing_timestamp"
    );
    let pt_records = pt_log
        .get_path("resourceLogs")
        .and_then(|v| v.as_array())
        .and_then(|arr| arr.first())
        .and_then(|rl| rl.as_object())
        .and_then(|o| o.get("scopeLogs"))
        .and_then(|v| v.as_array())
        .and_then(|arr| arr.first())
        .and_then(|sl| sl.as_object())
        .and_then(|o| o.get("logRecords"))
        .and_then(|v| v.as_array())
        .unwrap();
    assert_eq!(pt_records.len(), 1);

    for shard_items in &out.per_shard {
        if let Some(last) = shard_items.last() {
            assert_eq!(
                last.position
                    .as_ref()
                    .map(|p| (&*p.topic, p.partition, p.offset)),
                Some(("logs-otlp", 0, 77))
            );
            assert!(last.exemplar.attrs.iter().any(|(k, _)| k == "request_id"));
            assert!(!last.exemplar.attrs.iter().any(|(k, _)| k == "trace_id"));
            assert_eq!(last.exemplar.severity, 17);
        }
    }
}

#[test]
fn codec_schema_changes_when_attributes_policy_changes() {
    let cfg1: AdapterConfig =
        serde_yaml_ng::from_str(include_str!("fixtures/spec_example.yaml")).unwrap();
    let mut cfg2 = cfg1.clone();
    cfg2.attributes_policy.preserve_resource_and_scope =
        !cfg1.attributes_policy.preserve_resource_and_scope;
    assert_ne!(cfg1.codec_schema(), cfg2.codec_schema());

    let mut cfg3 = cfg1.clone();
    cfg3.attributes_policy
        .exclude_attributes
        .insert("custom_secret".into());
    assert_ne!(cfg1.codec_schema(), cfg3.codec_schema());

    let mut cfg4 = cfg1.clone();
    cfg4.attributes_policy
        .resource_attributes_exclude
        .insert("extra.exclude".into());
    assert_ne!(cfg1.codec_schema(), cfg4.codec_schema());

    let mut cfg5 = cfg1.clone();
    cfg5.attributes_policy.template_target_field = "custom.template".into();
    assert_ne!(cfg1.codec_schema(), cfg5.codec_schema());

    let mut cfg6 = cfg1.clone();
    cfg6.attributes_policy.unmatched_log_attributes = UnmatchedLogAttributes::KeepFirst;
    assert_ne!(cfg1.codec_schema(), cfg6.codec_schema());
}

#[test]
fn native_attributes_policy_resource_scope_severity_and_body_drop() {
    let mut c = cfg();
    c.attributes_policy.preserve_resource_and_scope = true;
    c.attributes_policy.unmatched_log_attributes = UnmatchedLogAttributes::KeepFirst;
    c.attributes_policy.body_policy = BodyPolicy::Drop;

    let mut r = test_router(c.clone());
    let mut out = RouterOutput::new(4);

    let e = ev(&[
        ("@timestamp", T.into()),
        ("source.ip", "10.0.0.1".into()),
        ("network.bytes", Value::from(100)),
        ("severity", "WARN".into()),
        ("body", "should be dropped".into()),
        ("resource.service.name", "payments".into()),
        ("resource.k8s.pod.uid", "excluded-pod-uid".into()),
        ("scope.name", "auth-lib".into()),
        ("scope.version", "2.1".into()),
        ("attributes.env", "prod".into()),
        ("attributes.trace_id", "excluded-trace".into()),
    ]);
    r.route(e, NOW, &mut out);

    let item = out.per_shard.iter().flatten().next().unwrap();
    assert_eq!(item.exemplar.severity, 13);
    assert!(item.exemplar.body.is_none());
    assert!(
        item.exemplar
            .attrs
            .iter()
            .any(|(k, v)| k == "env" && v.as_ref() == b"prod")
    );
    assert!(!item.exemplar.attrs.iter().any(|(k, _)| k == "trace_id"));
    assert!(!item.exemplar.attrs.iter().any(|(k, _)| k == "source.ip"));
    assert!(
        !item
            .exemplar
            .attrs
            .iter()
            .any(|(k, _)| k == "network.bytes")
    );
    assert_eq!(item.resource.scope_name, "auth-lib");
    assert_eq!(item.resource.scope_version, "2.1");
    assert_eq!(
        item.resource.resource.get("service.name"),
        Some(&serde_json::Value::String("payments".into()))
    );
    assert!(!item.resource.resource.contains_key("k8s.pod.uid"));
    assert!(!item.resource.resource.contains_key("k8s"));

    c.attributes_policy.preserve_resource_and_scope = false;
    let mut r2 = test_router(c);
    let mut out2 = RouterOutput::new(4);
    r2.route(
        ev(&[
            ("@timestamp", T.into()),
            ("source.ip", "10.0.0.1".into()),
            ("resource.service.name", "payments".into()),
            ("scope.name", "auth-lib".into()),
        ]),
        NOW,
        &mut out2,
    );
    let item2 = out2.per_shard.iter().flatten().next().unwrap();
    assert_eq!(item2.resource_hash, item2.resource.canonical_hash());
    assert!(item2.resource.resource.is_empty());
    assert!(item2.resource.scope_name.is_empty());
}

#[test]
fn otlp_mixed_batch_isolates_multiple_quarantine_reasons_and_late_records() {
    let mut r = test_router_with_watermark(cfg(), DAY_MS + 3_600_000);
    let mut out = RouterOutput::new(4);

    let make_lr = |ts_opt: Option<&str>| -> Value {
        let mut m = ObjectMap::new();
        if let Some(ts) = ts_opt {
            m.insert("timeUnixNano".into(), Value::from(ts));
        }
        Value::Object(m)
    };

    let valid_ns = ((NOW - 1_000) * 1_000_000).to_string();
    let future_ns = ((NOW + 10_000_000) * 1_000_000).to_string();
    let late_ns = (1_000 * 1_000_000).to_string();

    let mut scope_log = ObjectMap::new();
    scope_log.insert(
        "logRecords".into(),
        Value::Array(vec![
            make_lr(Some(&valid_ns)),
            make_lr(None),
            make_lr(Some("not-a-number")),
            make_lr(Some(&future_ns)),
            make_lr(Some(&late_ns)),
        ]),
    );
    let mut res_log = ObjectMap::new();
    res_log.insert(
        "scopeLogs".into(),
        Value::Array(vec![Value::Object(scope_log)]),
    );
    let mut root = ObjectMap::new();
    root.insert(
        "resourceLogs".into(),
        Value::Array(vec![Value::Object(res_log)]),
    );

    r.route(LogEvent::from_map(root), NOW, &mut out);

    let routed: Vec<_> = out.per_shard.iter().flatten().collect();
    assert_eq!(routed.len(), 1);
    assert_eq!(r.stats().quarantined_total, 3);
    assert_eq!(r.stats().late_total, 1);
    assert_eq!(out.passthrough.len(), 4);
}

#[test]
fn otlp_late_and_quarantine_passthrough_unchanged() {
    use logsluice::config::EnvelopeMode;

    let mut base_cfg = cfg();
    base_cfg.envelope_mode = EnvelopeMode::Otlp;
    let mut r = test_router_with_watermark(base_cfg, DAY_MS + 3_600_000);
    let mut out = RouterOutput::new(4);

    // 1. Envelope without resourceLogs array -> whole event quarantined as missing_timestamp
    let non_array_env = ev(&[("topic", "otlp-raw".into()), ("resourceLogs", "bad".into())]);
    r.route(non_array_env, NOW, &mut out);
    assert_eq!(out.passthrough.len(), 1);
    assert_eq!(
        out.passthrough[0].get_path("_wa_route"),
        Some(&Value::from("quarantine"))
    );
    assert_eq!(
        out.passthrough[0].get_path("_wa_reason"),
        Some(&Value::from("missing_timestamp"))
    );
    assert_eq!(
        out.passthrough[0].get_path("topic"),
        Some(&Value::from("otlp-raw"))
    );

    // 2. Mixed OTLP envelope with resource & scope metadata, valid, quarantined, and late records
    let mut out2 = RouterOutput::new(4);
    let valid_ns = ((NOW - 1_000) * 1_000_000).to_string();
    let late_ns = (1_000 * 1_000_000).to_string();

    let make_lr = |id: &str, ts_opt: Option<&str>| -> Value {
        let mut m = ObjectMap::new();
        if let Some(ts) = ts_opt {
            m.insert("timeUnixNano".into(), Value::from(ts));
        }
        let mut body = ObjectMap::new();
        body.insert("stringValue".into(), Value::from(id));
        m.insert("body".into(), Value::Object(body));
        Value::Object(m)
    };

    let lr_valid = make_lr("valid-1", Some(&valid_ns));
    let lr_missing = make_lr("missing-1", None);
    let lr_invalid = make_lr("invalid-1", Some("bad-ts"));
    let lr_late = make_lr("late-1", Some(&late_ns));

    let mut scope_meta = ObjectMap::new();
    scope_meta.insert("name".into(), Value::from("my-scope"));
    scope_meta.insert("version".into(), Value::from("2.0"));

    let mut scope_log = ObjectMap::new();
    scope_log.insert("scope".into(), Value::Object(scope_meta.clone()));
    scope_log.insert(
        "logRecords".into(),
        Value::Array(vec![
            lr_valid,
            lr_missing.clone(),
            lr_invalid.clone(),
            lr_late.clone(),
        ]),
    );

    let mut res_meta = ObjectMap::new();
    res_meta.insert("service.name".into(), Value::from("checkout"));

    let mut res_log = ObjectMap::new();
    res_log.insert("resource".into(), Value::Object(res_meta.clone()));
    res_log.insert(
        "scopeLogs".into(),
        Value::Array(vec![Value::Object(scope_log)]),
    );

    let mut root = ObjectMap::new();
    root.insert("topic".into(), Value::from("otlp-logs"));
    root.insert(
        "resourceLogs".into(),
        Value::Array(vec![Value::Object(res_log)]),
    );

    r.route(LogEvent::from_map(root), NOW, &mut out2);

    let make_expected_passthrough = |route: &str, reason: &str, rec: Value| -> LogEvent {
        let mut sl = ObjectMap::new();
        sl.insert("scope".into(), Value::Object(scope_meta.clone()));
        sl.insert("logRecords".into(), Value::Array(vec![rec]));
        let mut rl = ObjectMap::new();
        rl.insert("resource".into(), Value::Object(res_meta.clone()));
        rl.insert("scopeLogs".into(), Value::Array(vec![Value::Object(sl)]));
        let mut map = ObjectMap::new();
        map.insert("topic".into(), Value::from("otlp-logs"));
        map.insert("resourceLogs".into(), Value::Array(vec![Value::Object(rl)]));
        map.insert("_wa_route".into(), Value::from(route));
        map.insert("_wa_reason".into(), Value::from(reason));
        LogEvent::from_map(map)
    };

    assert_eq!(
        out2.passthrough,
        vec![
            make_expected_passthrough("quarantine", "invalid_timestamp", lr_invalid),
            make_expected_passthrough("quarantine", "missing_timestamp", lr_missing),
            make_expected_passthrough("late", "late", lr_late),
        ]
    );
}

#[test]
fn otlp_router_covers_severity_variants_and_any_value_types() {
    use logsluice::config::{EnvelopeMode, NormalizerConfig};

    let mut c = cfg();
    c.output.route_field = String::new();
    c.attributes_policy.preserve_resource_and_scope = true;
    c.attributes_policy.unmatched_log_attributes = UnmatchedLogAttributes::KeepFirst;
    c.attributes_policy.body_policy = BodyPolicy::ReplaceWithTemplate;

    let mut r = test_router(c.clone());
    assert_eq!(r.shards(), 4);
    let _ = r.watermark(0);

    let valid_ns = (NOW - 1_000) * 1_000_000;

    let make_otlp_record = |sev_num: Option<Value>,
                            sev_text: Option<&str>,
                            attrs: Value,
                            body: Option<Value>|
     -> Value {
        let mut rec = ObjectMap::new();
        rec.insert("timeUnixNano".into(), Value::from(valid_ns));
        if let Some(sn) = sev_num {
            rec.insert("severityNumber".into(), sn);
        }
        if let Some(st) = sev_text {
            rec.insert("severityText".into(), Value::from(st));
        }
        if let Some(b) = body {
            rec.insert("body".into(), b);
        }
        rec.insert("attributes".into(), attrs);
        Value::Object(rec)
    };

    let rich_attrs = serde_json::json!([
        {"key": "source.ip", "value": {"stringValue": "10.0.0.1"}},
        {"key": "network.bytes", "value": {"doubleValue": 42.8}},
        {"key": "int_attr", "value": {"intValue": 100}},
        {"key": "int_str_attr", "value": {"intValue": "200"}},
        {"key": "bool_attr", "value": {"boolValue": true}},
        {"key": "double_attr", "value": {"doubleValue": 3.5}},
        {"key": "arr_attr", "value": {"arrayValue": {"values": [{"stringValue": "a"}]}}},
        {"key": "kv_attr", "value": {"kvlistValue": {"values": [{"key": "k", "value": {"intValue": 1}}]}}}
    ]);

    let map_attrs_with_level = serde_json::json!({
        "source.ip": "10.0.0.2",
        "network.bytes": "not-a-number",
        "network.packets": 42.8,
        "level": "CRITICAL",
        "custom_map_attr": {"boolValue": false}
    });

    let records = vec![
        // Numeric severityNumber 1..=24
        make_otlp_record(
            Some(Value::from(1)),
            None,
            rich_attrs.clone(),
            Some(serde_json::json!({"stringValue": "conn from 10.0.0.1"})),
        ),
        make_otlp_record(Some(Value::from(24)), None, rich_attrs.clone(), None),
        // Out-of-range severityNumber 99 falling back to severityText
        make_otlp_record(
            Some(Value::from(99)),
            Some("FATAL"),
            rich_attrs.clone(),
            None,
        ),
        make_otlp_record(None, Some("CRITICAL"), rich_attrs.clone(), None),
        make_otlp_record(None, Some("TRACE"), rich_attrs.clone(), None),
        make_otlp_record(None, Some("DEBUG"), rich_attrs.clone(), None),
        make_otlp_record(None, Some("INFO"), rich_attrs.clone(), None),
        make_otlp_record(None, Some("WARN"), rich_attrs.clone(), None),
        make_otlp_record(None, Some("ERROR"), rich_attrs.clone(), None),
        // Numeric string severityText ("13" and out-of-range "99") and object-wrapped severityNumber
        make_otlp_record(
            Some(serde_json::json!({"intValue": 9})),
            None,
            rich_attrs.clone(),
            None,
        ),
        make_otlp_record(
            Some(serde_json::json!({"stringValue": "13"})),
            None,
            rich_attrs,
            None,
        ),
        // Map-style attributes with fallback to "level" and invalid network.bytes
        make_otlp_record(
            None,
            Some("99"),
            map_attrs_with_level,
            Some(Value::from("raw body 192.168.1.1")),
        ),
        // Non-object logRecord entry (quarantined as missing_timestamp)
        Value::from("invalid-record-entry"),
    ];

    let otlp_envelope = serde_json::json!({
        "host": "collector-1",
        "resourceLogs": [
            "non-object-rl",
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "api"}},
                        {"key": "host.id", "value": {"intValue": 7}},
                        {"key": "fips", "value": {"boolValue": true}},
                        {"key": "extra", "value": {"arrayValue": {"values": []}}}
                    ]
                },
                "scopeLogs": [
                    "non-object-sl",
                    {
                        "scope": {
                            "name": "otel-scope",
                            "version": "1.0",
                            "attributes": [
                                {"key": "scope.env", "value": {"stringValue": "prod"}}
                            ]
                        },
                        "logRecords": records
                    }
                ]
            }
        ]
    });

    let mut out = RouterOutput::new(4);
    r.route(LogEvent::from_value(otlp_envelope).unwrap(), NOW, &mut out);

    let items: Vec<_> = out.per_shard.iter().flatten().collect();
    assert_eq!(items.len(), 12);
    assert_eq!(out.passthrough.len(), 1);
    assert!(r.watermark(NOW).is_some());
    assert!(r.stats().field_coercions_total >= 1);
    assert!(r.stats().field_type_errors_total >= 1);

    let severities: Vec<u8> = items.iter().map(|i| i.exemplar.severity).collect();
    assert!(severities.contains(&1)); // TRACE / 1
    assert!(severities.contains(&24)); // 24
    assert!(severities.contains(&21)); // FATAL / CRITICAL
    assert!(severities.contains(&5)); // DEBUG
    assert!(severities.contains(&9)); // INFO
    assert!(severities.contains(&13)); // WARN
    assert!(severities.contains(&17)); // ERROR

    // Verify native routing edge cases: VectorNative mode, future drift, resources fallback, scope_attrs, body normalizer, and Min/Max coercions
    let mut native_cfg = c;
    native_cfg.envelope_mode = EnvelopeMode::VectorNative;
    native_cfg
        .normalizers
        .insert("body".into(), NormalizerConfig::LogFingerprint);
    let mut nr = test_router(native_cfg);
    let mut nout = RouterOutput::new(4);

    // Future drift quarantine in native routing
    let future_ts = "2026-10-18T00:00:00Z";
    nr.route(
        ev(&[("@timestamp", future_ts.into()), ("host", "src-1".into())]),
        NOW,
        &mut nout,
    );
    assert_eq!(nout.passthrough.len(), 1);
    assert_eq!(
        nout.passthrough[0].get_path("_wa_reason"),
        Some(&Value::from("future_drift"))
    );

    // Valid native event with resources fallback, scope extra attr, nested unmatched attr, float metric coercion
    let mut nout2 = RouterOutput::new(4);
    nr.route(
        ev(&[
            ("@timestamp", T.into()),
            ("host", "src-1".into()),
            ("severityNumber", Value::from(5)),
            ("message", "hello from 10.0.0.9".into()),
            ("network.bytes", Value::from(12.7_f64)),
            ("resources.service.name", "worker".into()),
            ("scope.name", "native-scope".into()),
            ("scope.custom_tag", "v1".into()),
            ("nested.leaf", "kept".into()),
            ("top_scalar", Value::from(42)),
        ]),
        NOW,
        &mut nout2,
    );
    let nitem = nout2.per_shard.iter().flatten().next().unwrap();
    assert_eq!(nitem.exemplar.severity, 5);
    assert_eq!(
        nitem.exemplar.body.as_deref(),
        Some(b"hello from <IP>".as_slice())
    );
    assert_eq!(
        nitem.resource.resource.get("service.name"),
        Some(&Value::from("worker"))
    );
    assert_eq!(
        nitem.resource.scope_attrs.get("custom_tag"),
        Some(&Value::from("v1"))
    );
    assert!(
        nitem
            .exemplar
            .attrs
            .iter()
            .any(|(k, v)| k == "nested.leaf" && v.as_ref() == b"kept")
    );
    assert!(
        nitem
            .exemplar
            .attrs
            .iter()
            .any(|(k, v)| k == "top_scalar" && v.as_ref() == b"42")
    );
}
