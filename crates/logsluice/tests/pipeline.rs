#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::pin::Pin;
use std::time::Duration;

use futures::channel::mpsc;
use futures::{Stream, StreamExt};
use logsluice::config::{EnvelopeMode, SluiceConfig};
use logsluice::event::{AckStatus, LogEvent, ObjectMap, Value, value_to_string_lossy};
use logsluice::pipeline::SluicePipeline;

const T0: i64 = 86_400_000 * 20_000; // 1_728_000_000_000
const DAY_MS: i64 = 86_400_000;

fn transform_with_tempdir(
    rx: mpsc::UnboundedReceiver<LogEvent>,
) -> Pin<Box<dyn Stream<Item = LogEvent> + Send>> {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::TempDir::keep(dir);

    let yaml = format!(
        r#"
storage:
  data_dir: {:?}
  l1_flush_interval: 50ms
window:
  type: tumbling
  duration: 1d
  allowed_skew: 30s
  allowed_lateness: 0s
  late_emit_interval: 50ms
  max_close_delay: 1000d
  max_future_drift: 2d
  emit:
    close_jitter_max: 0s
group_by:
  - source.ip
aggregations:
  event_count: count
shutdown:
  drain_timeout: 5s
"#,
        data_dir
    );

    let cfg: SluiceConfig = serde_yaml_ng::from_str(&yaml).unwrap();
    let pipeline = SluicePipeline::new(cfg).unwrap();
    Box::new(pipeline).transform_stream(rx)
}

fn send_events(tx: &mpsc::UnboundedSender<LogEvent>, events: &[(&str, i64)]) {
    for (ip, ts) in events {
        let mut log = LogEvent::default();
        let rfc = chrono::DateTime::from_timestamp_millis(*ts)
            .unwrap()
            .to_rfc3339();
        log.insert_path("@timestamp", Value::from(rfc));
        log.insert_path("source.ip", Value::from(*ip));
        tx.unbounded_send(log).unwrap();
    }
}

fn send_watermark_advancing_event(tx: &mpsc::UnboundedSender<LogEvent>, ts: i64) {
    let mut log = LogEvent::default();
    let rfc = chrono::DateTime::from_timestamp_millis(ts)
        .unwrap()
        .to_rfc3339();
    log.insert_path("@timestamp", Value::from(rfc));
    log.insert_path("source.ip", Value::from("10.99.99.99"));
    tx.unbounded_send(log).unwrap();
}

async fn collect_summaries_acking(
    mut stream: Pin<Box<dyn Stream<Item = LogEvent> + Send>>,
    expected_count: usize,
) -> Vec<LogEvent> {
    let mut collected = Vec::new();
    while let Some(mut log) = stream.next().await {
        if let Some(r) = log.get_path("_wa_route")
            && value_to_string_lossy(r) == "summary"
        {
            let finalizers = log.take_finalizers();
            finalizers.update_status(AckStatus::Delivered);
            collected.push(log);
            if collected.len() >= expected_count {
                break;
            }
        }
    }
    collected
}

fn sum_by_ip(summaries: &[LogEvent]) -> BTreeMap<String, u64> {
    let mut map = BTreeMap::new();
    for log in summaries {
        let ip = log
            .get_path("source.ip")
            .map(|v| value_to_string_lossy(v).into_owned())
            .unwrap_or_default();
        let count = log
            .get_path("event_count")
            .and_then(|v| v.as_i64())
            .unwrap_or(0) as u64;
        *map.entry(ip).or_insert(0) += count;
    }
    map
}

#[tokio::test]
async fn end_to_end_on_close_with_acks() {
    let (tx, rx) = futures::channel::mpsc::unbounded();
    let out = transform_with_tempdir(rx);
    send_events(
        &tx,
        &[("10.0.0.1", T0), ("10.0.0.1", T0 + 1), ("10.0.0.2", T0 + 2)],
    );
    send_watermark_advancing_event(&tx, T0 + DAY_MS + 31_000);
    let summaries = collect_summaries_acking(out, 2).await;
    let expected: BTreeMap<String, u64> =
        [("10.0.0.1".to_string(), 2), ("10.0.0.2".to_string(), 1)].into();
    assert_eq!(sum_by_ip(&summaries), expected);
}

#[tokio::test]
async fn input_end_triggers_drain() {
    let (tx, rx) = futures::channel::mpsc::unbounded();
    let out = transform_with_tempdir(rx);
    send_events(&tx, &[("10.0.0.1", T0)]);
    drop(tx);
    let summaries = collect_summaries_acking(out, 1).await;
    assert_eq!(
        value_to_string_lossy(summaries[0].get_path("window.emit_reason").unwrap()),
        "drain"
    );
}

#[tokio::test]
async fn passthrough_events_preserve_finalizers() {
    let (tx, rx) = futures::channel::mpsc::unbounded();
    let mut out = transform_with_tempdir(rx);

    let mut log = LogEvent::default();
    log.insert_path("source.ip", Value::from("10.0.0.99"));
    let (log_with_notifier, mut receiver) = log.with_ack_notifier();

    tx.unbounded_send(log_with_notifier).unwrap();

    let mut passthrough = out.next().await.expect("expected passthrough event");
    assert_eq!(
        value_to_string_lossy(passthrough.get_path("_wa_route").unwrap()),
        "quarantine"
    );
    let finalizers = passthrough.take_finalizers();
    finalizers.update_status(AckStatus::Delivered);
    drop(finalizers);

    let status = receiver.try_recv();
    assert_eq!(status, Ok(AckStatus::Delivered));
}

#[tokio::test]
async fn auto_envelope_mode_emits_otlp_when_input_is_otlp() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut cfg: SluiceConfig =
        serde_yaml_ng::from_str(include_str!("fixtures/spec_example.yaml")).unwrap();
    cfg.storage.data_dir = dir.path().to_path_buf();
    cfg.window.duration = Duration::from_secs(10);
    cfg.window.allowed_skew = Duration::from_secs(1);
    cfg.window.allowed_lateness = Duration::ZERO;
    cfg.window.max_close_delay = Duration::from_secs(5);
    cfg.window.emit.close_jitter_max = Duration::from_millis(1);
    cfg.envelope_mode = EnvelopeMode::Auto;
    cfg.shards = 1;

    let pipeline = SluicePipeline::new(cfg).unwrap();
    let (tx, rx) = futures::channel::mpsc::unbounded();
    let mut out_stream = Box::new(pipeline).transform_stream(rx);

    let mut rec = ObjectMap::new();
    rec.insert("timeUnixNano".into(), Value::from("1700000001000000000"));
    let mut body = ObjectMap::new();
    body.insert("stringValue".into(), Value::from("hello otlp"));
    rec.insert("body".into(), Value::Object(body));
    let mut scope_log = ObjectMap::new();
    scope_log.insert("logRecords".into(), Value::Array(vec![Value::Object(rec)]));
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

    tx.unbounded_send(LogEvent::from_map(root)).unwrap();
    drop(tx);

    let mut outputs = Vec::new();
    while let Some(mut ev) = out_stream.next().await {
        let finalizers = ev.take_finalizers();
        finalizers.update_status(AckStatus::Delivered);
        outputs.push(ev);
    }
    assert_eq!(outputs.len(), 1);
    assert!(
        outputs[0].get_path("resourceLogs").is_some(),
        "EnvelopeMode::Auto must render OTLP resourceLogs when input was OTLP"
    );

    let meta_bytes = std::fs::read(dir.path().join("pod_meta.json")).unwrap();
    let meta: logsluice_core::store::meta::PodMeta = serde_json::from_slice(&meta_bytes).unwrap();
    assert_eq!(meta.window_duration_ms, 10_000);
    assert_eq!(meta.window_offset_ms, 0);
}
