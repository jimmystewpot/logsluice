#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use logsluice::config::{AdapterConfig, BodyPolicy, NormalizerConfig};
use logsluice::event::{LogEvent, value_to_string_lossy};
use logsluice::render::{render_native, render_otlp};
use logsluice_core::codec::plan::AggregationPlan;
use logsluice_core::codec::tail::ExemplarTail;
use logsluice_core::codec::ticket::EmitReason;
use logsluice_core::emission::SliceRecord;
use logsluice_core::resource::ResourceScopePayload;

fn cfg() -> AdapterConfig {
    serde_yaml_ng::from_str(
        r#"
window:
  type: tumbling
  duration: 1d
  allowed_lateness: 0s
group_by:
  - source.ip
  - destination.ip
aggregations:
  event_count: count
"#,
    )
    .unwrap()
}

fn plan() -> AggregationPlan {
    let engine = cfg().engine().unwrap();
    AggregationPlan::new(&engine.aggregations).unwrap()
}

fn fixture_resource(service: &str) -> Arc<ResourceScopePayload> {
    let mut resource = BTreeMap::new();
    resource.insert(
        "service.name".to_string(),
        serde_json::Value::String(service.to_string()),
    );
    Arc::new(ResourceScopePayload {
        resource,
        scope_name: "test-scope".to_string(),
        scope_version: "1.0".to_string(),
        scope_attrs: BTreeMap::new(),
    })
}

fn fixture_record(digest: u128, seq: u32, reason: EmitReason) -> SliceRecord {
    SliceRecord {
        window_start_ms: 1_700_000_000_000,
        window_end_ms: 1_700_086_400_000,
        emit_reason: reason,
        emitter_id: "pod-1".to_string(),
        digest,
        slice_seq: seq,
        slice_id: format!("pod-1:1700000000000:{digest:032x}:{seq}"),
        resource: fixture_resource("auth"),
        tail: ExemplarTail {
            severity: 0,
            body: Some(Bytes::from("login success")),
            dims: vec![Some(Bytes::from("192.168.1.1")), Some(Bytes::from("443"))],
            attrs: vec![],
        },
        counters: vec![[3, 0, 0, 0]],
        minmax: vec![],
    }
}

fn fixture_record_with_dims(dims: Vec<Option<Bytes>>) -> SliceRecord {
    SliceRecord {
        window_start_ms: 1_700_000_000_000,
        window_end_ms: 1_700_086_400_000,
        emit_reason: EmitReason::Close,
        emitter_id: "pod-1".to_string(),
        digest: 0x123,
        slice_seq: 1,
        slice_id: "pod-1:1700000000000:00000000000000000000000000000123:1".to_string(),
        resource: fixture_resource("auth"),
        tail: ExemplarTail {
            severity: 0,
            body: None,
            dims,
            attrs: vec![],
        },
        counters: vec![[1, 0, 0, 0]],
        minmax: vec![],
    }
}

fn fixture_records(count: usize, service: &str) -> Vec<SliceRecord> {
    (0..count)
        .map(|i| {
            let digest = (i as u128) + 1;
            SliceRecord {
                window_start_ms: 1_700_000_000_000,
                window_end_ms: 1_700_086_400_000,
                emit_reason: EmitReason::Close,
                emitter_id: "pod-1".to_string(),
                digest,
                slice_seq: 1,
                slice_id: format!("pod-1:1700000000000:{digest:032x}:1"),
                resource: fixture_resource(service),
                tail: ExemplarTail {
                    severity: 0,
                    body: None,
                    dims: vec![Some(Bytes::from("10.0.0.1")), Some(Bytes::from("80"))],
                    attrs: vec![],
                },
                counters: vec![[1, 0, 0, 0]],
                minmax: vec![],
            }
        })
        .collect()
}

fn log_records_len(e: &LogEvent) -> usize {
    let mut total = 0;
    if let Some(resource_logs) = e.get_path("resourceLogs").and_then(|v| v.as_array()) {
        for rl in resource_logs {
            if let Some(scope_logs) = rl
                .as_object()
                .and_then(|o| o.get("scopeLogs"))
                .and_then(|v| v.as_array())
            {
                for sl in scope_logs {
                    if let Some(lrs) = sl
                        .as_object()
                        .and_then(|o| o.get("logRecords"))
                        .and_then(|v| v.as_array())
                    {
                        total += lrs.len();
                    }
                }
            }
        }
    }
    total
}

fn resource_service_name(e: &LogEvent) -> String {
    let Some(resource_logs) = e.get_path("resourceLogs").and_then(|v| v.as_array()) else {
        return String::new();
    };
    let Some(rl) = resource_logs.first().and_then(|v| v.as_object()) else {
        return String::new();
    };
    let Some(res) = rl.get("resource").and_then(|v| v.as_object()) else {
        return String::new();
    };
    if let Some(attrs) = res.get("attributes").and_then(|v| v.as_array()) {
        for attr in attrs {
            let Some(obj) = attr.as_object() else {
                continue;
            };
            if obj.get("key").map(value_to_string_lossy) == Some("service.name".into())
                && let Some(val) = obj.get("value")
            {
                if let Some(v_obj) = val.as_object()
                    && let Some(s) = v_obj.get("stringValue")
                {
                    return value_to_string_lossy(s).into_owned();
                }
                return value_to_string_lossy(val).into_owned();
            }
        }
    }
    if let Some(s) = res.get("service.name") {
        return value_to_string_lossy(s).into_owned();
    }
    String::new()
}

#[test]
fn render_native_contract_fields() {
    let rec = fixture_record(0xabc, 2, EmitReason::Close);
    let e = render_native(&rec, &cfg(), &plan());
    let g = |p: &str| value_to_string_lossy(e.get_path(p).unwrap()).into_owned();
    assert_eq!(g("_wa_route"), "summary");
    assert_eq!(g("window.emit_reason"), "close");
    assert_eq!(g("window.slice_seq"), "2");
    assert_eq!(g("window.key_digest"), format!("{:032x}", 0xabc));
    assert_eq!(g("window.slice_id"), rec.slice_id);
    assert_eq!(g("event_count"), "3");
    assert_eq!(g("resource.service.name"), "auth");
}

#[test]
fn render_native_matches_golden_json() {
    let rec = fixture_record(0xabc, 2, EmitReason::Close);
    let e = render_native(&rec, &cfg(), &plan());
    let actual = serde_json::Value::Object(e.as_map().clone());
    let expected = serde_json::json!({
        "_wa_route": "summary",
        "body": "login success",
        "destination": {
            "ip": "443"
        },
        "event_count": 3,
        "resource": {
            "service": {
                "name": "auth"
            }
        },
        "scope": {
            "name": "test-scope",
            "version": "1.0"
        },
        "source": {
            "ip": "192.168.1.1"
        },
        "window": {
            "emit_reason": "close",
            "emitter_id": "pod-1",
            "end": "2023-11-15T22:13:20.000Z",
            "key_digest": "00000000000000000000000000000abc",
            "slice_id": "pod-1:1700000000000:00000000000000000000000000000abc:2",
            "slice_seq": 2,
            "start": "2023-11-14T22:13:20.000Z"
        }
    });
    assert_eq!(actual, expected);
}

#[test]
fn test_render_omits_missing_dims() {
    let rec = fixture_record_with_dims(vec![None, Some("443".into())]);
    let e = render_native(&rec, &cfg(), &plan());
    assert!(e.get_path("source.ip").is_none());
    assert_eq!(
        value_to_string_lossy(e.get_path("destination.ip").unwrap()),
        "443"
    );
}

#[test]
fn otlp_envelopes_chunked() {
    let recs = fixture_records(1_200, "auth");
    let out = render_otlp(&recs, &cfg(), &plan(), 500);
    let sizes: Vec<_> = out.iter().map(log_records_len).collect();
    assert_eq!(sizes, vec![500, 500, 200]);
    assert!(out.iter().all(|e| resource_service_name(e) == "auth"));
}

#[test]
fn render_respects_body_policy_replace_with_template_and_preserve_resource_flag() {
    let mut cfg: AdapterConfig =
        serde_yaml_ng::from_str(include_str!("fixtures/spec_example.yaml")).unwrap();
    cfg.attributes_policy.body_policy = BodyPolicy::ReplaceWithTemplate;
    cfg.attributes_policy.template_target_field = "message_template".into();
    cfg.attributes_policy.preserve_resource_and_scope = false;

    let eng = cfg.engine().unwrap();
    let plan = AggregationPlan::new(&eng.aggregations).unwrap();

    let mut rec = fixture_record(0xabc, 0, EmitReason::Close);
    rec.tail.body = Some(Bytes::from("<ip> <uuid>"));
    rec.tail.severity = 17; // ERROR
    rec.counters = vec![[3, 100, 5, 0]];
    rec.minmax = vec![
        (1_700_000_000_000, 1_700_000_000_000),
        (1_700_000_005_000, 1_700_000_005_000),
    ];

    let ev = render_native(&rec, &cfg, &plan);
    assert_eq!(
        value_to_string_lossy(ev.get_path("message_template").unwrap()),
        "<ip> <uuid>"
    );
    assert!(ev.get_path("body").is_none());
    assert_eq!(
        value_to_string_lossy(ev.get_path("severity").unwrap()),
        "ERROR"
    );
    assert_eq!(
        ev.get_path("severity_number").and_then(|v| v.as_i64()),
        Some(17)
    );
    assert!(ev.get_path("resource.service.name").is_none());
    assert!(ev.get_path("resource.\"service.name\"").is_none());
    assert!(ev.get_path("scope.name").is_none());
}

#[test]
fn render_otlp_and_fingerprinted_dim_respect_attributes_policy() {
    let mut cfg: AdapterConfig =
        serde_yaml_ng::from_str(include_str!("fixtures/spec_example.yaml")).unwrap();
    cfg.attributes_policy.body_policy = BodyPolicy::ReplaceWithTemplate;
    cfg.attributes_policy.template_target_field = "message_template".into();
    cfg.attributes_policy.preserve_resource_and_scope = false;
    cfg.group_by = vec![
        "source.ip".into(),
        "destination.ip".into(),
        "message".into(),
    ];
    cfg.normalizers
        .insert("message".into(), NormalizerConfig::LogFingerprint);

    let eng = cfg.engine().unwrap();
    let plan = AggregationPlan::new(&eng.aggregations).unwrap();

    let mut rec = fixture_record(0xabc, 0, EmitReason::Close);
    rec.tail.dims = vec![
        Some(Bytes::from("10.0.0.1")),
        Some(Bytes::from("10.0.0.2")),
        Some(Bytes::from("from-dim <ip> <uuid>")),
    ];
    rec.tail.body = Some(Bytes::from("fallback-body"));
    rec.tail.severity = 13; // WARN
    rec.counters = vec![[3, 100, 5, 0]];
    rec.minmax = vec![
        (1_700_000_000_000, 1_700_000_000_000),
        (1_700_000_005_000, 1_700_000_005_000),
    ];

    let native_ev = render_native(&rec, &cfg, &plan);
    assert_eq!(
        value_to_string_lossy(native_ev.get_path("message_template").unwrap()),
        "from-dim <ip> <uuid>"
    );
    assert_eq!(
        value_to_string_lossy(native_ev.get_path("severity").unwrap()),
        "WARN"
    );

    let otlp_evs = render_otlp(std::slice::from_ref(&rec), &cfg, &plan, 500);
    assert_eq!(otlp_evs.len(), 1);
    assert_eq!(resource_service_name(&otlp_evs[0]), "");
    let lr = otlp_evs[0]
        .get_path("resourceLogs")
        .and_then(|v| v.as_array())
        .and_then(|a| a.first())
        .and_then(|rl| rl.as_object())
        .and_then(|o| o.get("scopeLogs"))
        .and_then(|v| v.as_array())
        .and_then(|a| a.first())
        .and_then(|sl| sl.as_object())
        .and_then(|o| o.get("logRecords"))
        .and_then(|v| v.as_array())
        .and_then(|a| a.first())
        .and_then(|lr| lr.as_object())
        .unwrap();
    assert!(lr.get("body").is_none());
    assert_eq!(lr.get("severityNumber").and_then(|v| v.as_i64()), Some(13));
    assert_eq!(
        lr.get("severityText").map(value_to_string_lossy),
        Some("WARN".into())
    );
}

#[test]
fn render_otlp_groups_records_sharing_an_arc_without_deep_compare() {
    let cfg = cfg();
    let plan = plan();

    let shared_res = fixture_resource("auth");
    let mut rec1 = fixture_record(0x1, 0, EmitReason::Close);
    rec1.resource = shared_res.clone();

    let mut rec2 = fixture_record(0x2, 0, EmitReason::Close);
    rec2.resource = shared_res.clone();

    // Distinct Arc instance with equal payload value
    let mut rec3 = fixture_record(0x3, 0, EmitReason::Close);
    rec3.resource = fixture_resource("auth");

    let recs = vec![rec1, rec2, rec3];
    let otlp_evs = render_otlp(&recs, &cfg, &plan, 500);
    assert_eq!(otlp_evs.len(), 1);

    let resource_logs = otlp_evs[0]
        .get_path("resourceLogs")
        .and_then(|v| v.as_array())
        .unwrap();
    // All three records should be grouped under a single resource logs entry
    assert_eq!(resource_logs.len(), 1);
}

#[test]
fn render_covers_periodic_reason_scope_attrs_and_typed_resource_values() {
    let mut custom_cfg: AdapterConfig = serde_yaml_ng::from_str(
        r#"
window:
  type: tumbling
  duration: 1d
  allowed_lateness: 0s
group_by:
  - source.ip
aggregations:
  event_count: count
  bytes_total: { sum: "network.bytes" }
  min_latency: { min: "latency_ms" }
  max_latency: { max: "latency_ms" }
  first_seen_time: { min: "@timestamp" }
  last_seen_time: { max: "@timestamp" }
"#,
    )
    .unwrap();
    custom_cfg.output.route_field = String::new();
    custom_cfg.attributes_policy.preserve_resource_and_scope = true;
    custom_cfg.attributes_policy.body_policy = BodyPolicy::ReplaceWithTemplate;
    custom_cfg.attributes_policy.template_target_field = String::new();

    let eng = custom_cfg.engine().unwrap();
    let custom_plan = AggregationPlan::new(&eng.aggregations).unwrap();

    let mut resource_map = BTreeMap::new();
    resource_map.insert("service.name".to_string(), serde_json::json!("billing"));
    resource_map.insert("host.id".to_string(), serde_json::json!(42));
    resource_map.insert("cpu.load".to_string(), serde_json::json!(0.75));
    resource_map.insert("fips.enabled".to_string(), serde_json::json!(true));
    resource_map.insert("tags".to_string(), serde_json::json!(["prod", "us-east"]));
    resource_map.insert("meta".to_string(), serde_json::json!({"zone": "a"}));

    let mut scope_attrs = BTreeMap::new();
    scope_attrs.insert("otel.library".to_string(), serde_json::json!("custom-sdk"));

    let rich_resource = Arc::new(ResourceScopePayload {
        resource: resource_map,
        scope_name: "billing-scope".to_string(),
        scope_version: "3.2.1".to_string(),
        scope_attrs,
    });

    let mut rec = fixture_record(0x456, 7, EmitReason::Periodic);
    rec.resource = rich_resource;
    rec.tail.severity = 21; // FATAL
    rec.tail.dims = vec![Some(Bytes::from("10.1.2.3"))];
    rec.tail.attrs = vec![
        ("env".to_string(), Bytes::from("production")),
        ("attributes.region".to_string(), Bytes::from("us-east-1")),
    ];
    rec.counters = vec![[10, 2048, 4, 0]];
    rec.minmax = vec![
        (12, 95),
        (12, 95),
        (1_700_000_001_000, 1_700_000_009_000),
        (1_700_000_001_000, 1_700_000_009_000),
    ];

    let native_ev = render_native(&rec, &custom_cfg, &custom_plan);
    assert_eq!(
        native_ev.get_path("_wa_route"),
        Some(&serde_json::json!("summary"))
    );
    assert_eq!(
        native_ev.get_path("window.emit_reason"),
        Some(&serde_json::json!("periodic"))
    );
    assert_eq!(
        native_ev.get_path("severity"),
        Some(&serde_json::json!("FATAL"))
    );
    assert_eq!(
        native_ev.get_path("severity_number"),
        Some(&serde_json::json!(21))
    );
    assert_eq!(
        native_ev.get_path("log.template"),
        Some(&serde_json::json!("login success"))
    );
    assert_eq!(
        native_ev.get_path("scope.otel.library"),
        Some(&serde_json::json!("custom-sdk"))
    );
    assert_eq!(
        native_ev.get_path("resource.host.id"),
        Some(&serde_json::json!(42))
    );
    assert_eq!(
        native_ev.get_path("resource.fips.enabled"),
        Some(&serde_json::json!(true))
    );
    assert_eq!(
        native_ev.get_path("attributes.env"),
        Some(&serde_json::json!("production"))
    );
    assert_eq!(
        native_ev.get_path("attributes.region"),
        Some(&serde_json::json!("us-east-1"))
    );
    assert_eq!(
        native_ev.get_path("min_latency"),
        Some(&serde_json::json!(12))
    );
    assert_eq!(
        native_ev.get_path("max_latency"),
        Some(&serde_json::json!(95))
    );
    assert_eq!(
        native_ev.get_path("first_seen_time"),
        Some(&serde_json::json!("2023-11-14T22:13:21.000Z"))
    );
    assert_eq!(
        native_ev.get_path("last_seen_time"),
        Some(&serde_json::json!("2023-11-14T22:13:29.000Z"))
    );

    // Check remaining EmitReason variants and severity levels (TRACE, DEBUG, INFO, out-of-range, Ignore body)
    let mut cfg_ignore = custom_cfg.clone();
    cfg_ignore.attributes_policy.body_policy = BodyPolicy::Ignore;
    for (reason, expected_reason, sev, expected_sev) in [
        (EmitReason::LateRevision, "late_revision", 1, Some("TRACE")),
        (EmitReason::Evict, "evict", 5, Some("DEBUG")),
        (EmitReason::Drain, "drain", 9, Some("INFO")),
        (EmitReason::Close, "close", 99, None),
    ] {
        let mut r = rec.clone();
        r.emit_reason = reason;
        r.tail.severity = sev;
        let ev = render_native(&r, &cfg_ignore, &custom_plan);
        assert_eq!(
            ev.get_path("window.emit_reason"),
            Some(&serde_json::json!(expected_reason))
        );
        assert_eq!(
            ev.get_path("severity").and_then(|v| v.as_str()),
            expected_sev
        );
        assert!(ev.get_path("body").is_none());
    }

    // Empty slice returns empty vec; max_per_envelope = 0 defaults to 500
    assert!(render_otlp(&[], &custom_cfg, &custom_plan, 0).is_empty());

    let otlp_out = render_otlp(std::slice::from_ref(&rec), &custom_cfg, &custom_plan, 0);
    assert_eq!(otlp_out.len(), 1);
    let rl = &otlp_out[0]
        .get_path("resourceLogs")
        .and_then(|v| v.as_array())
        .unwrap()[0];
    let res_attrs = rl
        .get("resource")
        .and_then(|r| r.get("attributes"))
        .and_then(|a| a.as_array())
        .unwrap();
    assert_eq!(res_attrs.len(), 6);

    let sl = &rl.get("scopeLogs").and_then(|s| s.as_array()).unwrap()[0];
    let scope_attrs_arr = sl
        .get("scope")
        .and_then(|s| s.get("attributes"))
        .and_then(|a| a.as_array())
        .unwrap();
    assert_eq!(scope_attrs_arr.len(), 1);

    let lr = &sl.get("logRecords").and_then(|l| l.as_array()).unwrap()[0];
    assert_eq!(lr.get("severityText"), Some(&serde_json::json!("FATAL")));
    let lr_attrs = lr.get("attributes").and_then(|a| a.as_array()).unwrap();
    assert!(lr_attrs.iter().any(|a| {
        a.get("key") == Some(&serde_json::json!("window.emit_reason"))
            && a.get("value") == Some(&serde_json::json!({"stringValue": "periodic"}))
    }));
    assert!(lr_attrs.iter().any(|a| {
        a.get("key") == Some(&serde_json::json!("min_latency"))
            && a.get("value") == Some(&serde_json::json!({"intValue": 12}))
    }));
}
