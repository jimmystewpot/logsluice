//! Criterion performance benchmarks for logsluice render and router hot paths.
#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use logsluice::{
    config::SluiceConfig,
    event::LogEvent,
    render::render_native,
    router::{Router, RouterOutput},
};
use logsluice_core::{
    codec::{plan::AggregationPlan, tail::ExemplarTail, ticket::EmitReason},
    emission::SliceRecord,
    resource::ResourceScopePayload,
    simd::{SimdKernelTable, SimdMode},
    watermark::{WatermarkConfig, WatermarkTracker},
    worker::PurgeHorizon,
};

fn bench_render_native_1k_records(c: &mut Criterion) {
    let yaml = r#"
window:
  type: tumbling
  duration: 1d
  allowed_lateness: 0s
group_by:
  - service.name
  - host.name
  - http.status_code
aggregations:
  event_count: count
  latency_sum:
    sum: duration_ms
output:
  route_field: _wa_route
attributes_policy:
  preserve_resource_and_scope: true
"#;
    let cfg: SluiceConfig = serde_yaml_ng::from_str(yaml).expect("config");
    let engine = cfg.engine().expect("engine");
    let plan = AggregationPlan::new(&engine.aggregations).expect("plan");

    let mut res_map = BTreeMap::new();
    res_map.insert(
        "service.name".to_string(),
        serde_json::json!("auth-service"),
    );
    res_map.insert(
        "deployment.environment".to_string(),
        serde_json::json!("production"),
    );
    let resource = Arc::new(ResourceScopePayload {
        resource: res_map,
        scope_name: "test-scope".to_string(),
        scope_version: "1.0.0".to_string(),
        scope_attrs: Default::default(),
    });

    let records: Vec<SliceRecord> = (0..1_000)
        .map(|i| SliceRecord {
            window_start_ms: 1_700_000_000_000,
            window_end_ms: 1_700_086_400_000,
            emit_reason: EmitReason::Close,
            emitter_id: "pod-0".to_string(),
            digest: i as u128,
            slice_seq: 1,
            slice_id: format!("pod-0:1700000000000:{i:032x}:1"),
            resource: Arc::clone(&resource),
            tail: ExemplarTail {
                severity: 9,
                body: Some(Bytes::from("HTTP request failed with status 500")),
                dims: vec![
                    Some(Bytes::from("auth-service")),
                    Some(Bytes::from("web-01.us-east.corp")),
                    Some(Bytes::from("500")),
                ],
                attrs: vec![
                    ("env".to_string(), Bytes::from("production")),
                    ("region".to_string(), Bytes::from("us-east-1")),
                ],
            },
            counters: vec![[10, 500, 0, 0]],
            minmax: vec![],
        })
        .collect();

    c.bench_function("render_native_1k_records", |b| {
        b.iter(|| {
            for rec in &records {
                std::hint::black_box(render_native(rec, &cfg, &plan));
            }
        });
    });
}

fn bench_router_route_known_key(c: &mut Criterion) {
    let yaml = r#"
shards: 2
window:
  type: tumbling
  duration: 1d
  allowed_lateness: 0s
group_by:
  - service
  - host
  - status
aggregations:
  event_count: count
  bytes_sum:
    sum: bytes
"#;
    let cfg: SluiceConfig = serde_yaml_ng::from_str(yaml).expect("config");
    let cfg = Arc::new(cfg);
    let kernels = SimdKernelTable::detect(SimdMode::Auto);
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
    let mut router = Router::new(cfg, kernels, 12345, horizons, tracker).expect("router");

    let mut event = LogEvent::default();
    event.insert_path("timestamp", 1_700_000_001_000i64);
    event.insert_path("service", "auth-api");
    event.insert_path("host", "worker-01");
    event.insert_path("status", "200");
    event.insert_path("bytes", 1024i64);
    event.insert_path("body", "request processed successfully");
    event.insert_path("severity_number", 9i64);

    let mut out = RouterOutput::new(2);
    router.route(event.clone(), 1_700_000_002_000, &mut out);
    for shard_items in &mut out.per_shard {
        shard_items.clear();
    }
    out.passthrough.clear();

    c.bench_function("router_route_known_key", |b| {
        b.iter_batched(
            || event.clone(),
            |ev| {
                router.route(ev, 1_700_000_002_000, &mut out);
                for shard_items in &mut out.per_shard {
                    shard_items.clear();
                }
                out.passthrough.clear();
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(
    benches,
    bench_render_native_1k_records,
    bench_router_route_known_key
);
criterion_main!(benches);
