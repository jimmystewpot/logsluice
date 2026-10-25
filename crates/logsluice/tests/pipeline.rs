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

#[tokio::test]
async fn closed_output_receiver_does_not_commit_ticket() {
    let dir = tempfile::TempDir::new().unwrap();
    let yaml = format!(
        r#"
storage:
  data_dir: {:?}
  l1_flush_interval: 20ms
  sync_wal: false
shards: 1
window:
  type: tumbling
  duration: 1d
  allowed_skew: 30s
  allowed_lateness: 0s
  late_emit_interval: 20ms
  max_close_delay: 1000d
  max_future_drift: 2d
  emit:
    close_jitter_max: 0s
group_by:
  - source.ip
aggregations:
  event_count: count
shutdown:
  drain_timeout: 400ms
"#,
        dir.path()
    );

    let cfg: SluiceConfig = serde_yaml_ng::from_str(&yaml).unwrap();
    let pipeline = SluicePipeline::new(cfg.clone()).unwrap();
    let (tx, rx) = futures::channel::mpsc::unbounded();
    let out = Box::new(pipeline).transform_stream(rx);

    // Send an aggregatable event and wait for its L1->L2 flush ack so the window is persisted.
    let mut agg_event = LogEvent::default();
    let rfc = chrono::DateTime::from_timestamp_millis(T0)
        .unwrap()
        .to_rfc3339();
    agg_event.insert_path("@timestamp", Value::from(rfc));
    agg_event.insert_path("source.ip", Value::from("10.0.0.1"));
    let (agg_event_with_ack, flush_ack_rx) = agg_event.with_ack_notifier();
    tx.unbounded_send(agg_event_with_ack).unwrap();
    assert_eq!(flush_ack_rx.await.unwrap(), AckStatus::Delivered);

    // Drop the output receiver before emission occurs.
    drop(out);

    let mut untransmitted = LogEvent::default();
    untransmitted.insert_path("source.ip", Value::from("10.0.0.98"));
    let (untransmitted_with_ack, ack_rx) = untransmitted.with_ack_notifier();
    tx.unbounded_send(untransmitted_with_ack).unwrap();

    // Closing input triggers drain ticket emission while the output receiver is closed.
    drop(tx);

    let upstream_status = ack_rx.await.unwrap();
    assert_ne!(upstream_status, AckStatus::Delivered);

    // Wait for drain_timeout to elapse and the shard store thread to shut down.
    tokio::time::sleep(Duration::from_millis(550)).await;

    let meta_bytes = std::fs::read(dir.path().join("pod_meta.json")).unwrap();
    let meta: logsluice_core::store::meta::PodMeta = serde_json::from_slice(&meta_bytes).unwrap();
    let (_engine, plan) = cfg.into_engine_and_plan().unwrap();
    let store_opts = logsluice_core::store::shard_store::StoreOptions {
        shard_dir: dir.path().join("shard_0"),
        cache_bytes: 1024 * 1024,
        hdr_memtable_bytes: 1024 * 1024,
        tail_memtable_bytes: 1024 * 1024,
        sync_wal: false,
        layout: plan.layout(),
        worker_threads: 1,
        max_write_buffer_bytes: None,
        max_cached_files: None,
    };

    let mut store = None;
    for _ in 0..40 {
        if let Ok(opened) =
            logsluice_core::store::shard_store::ShardStore::open(store_opts.clone(), &meta)
        {
            store = Some(opened);
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let store = store.expect("shard store should open after pipeline shutdown");
    assert!(
        !store.tickets().unwrap().is_empty(),
        "ticket must remain unacked when output channel is closed"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn startup_does_not_starve_current_thread_runtime() {
    let dir = tempfile::TempDir::new().unwrap();
    let data_dir = dir.path().to_path_buf();
    let yaml = format!(
        r#"
storage:
  data_dir: {:?}
  l1_flush_interval: 20ms
  sync_wal: false
shards: 4
window:
  type: tumbling
  duration: 1d
  allowed_skew: 30s
  allowed_lateness: 0s
  late_emit_interval: 20ms
  max_close_delay: 1000d
  max_future_drift: 2d
  emit:
    close_jitter_max: 0s
group_by:
  - source.ip
aggregations:
  event_count: count
shutdown:
  drain_timeout: 400ms
"#,
        data_dir
    );

    let cfg: SluiceConfig = serde_yaml_ng::from_str(&yaml).unwrap();

    // Pre-populate 4 shards on disk inside spawn_blocking so test setup does not affect the ticker
    let cfg_clone = cfg.clone();
    let data_dir_clone = data_dir.clone();
    tokio::task::spawn_blocking(move || {
        let codec_schema = cfg_clone.codec_schema();
        let meta = logsluice_core::store::meta::PodMeta::load_or_init_with_window(
            &data_dir_clone,
            cfg_clone.shards,
            &codec_schema,
            Some(cfg_clone.output.emitter_id.as_str()),
            cfg_clone.window.duration.as_millis() as i64,
            cfg_clone.window.offset.as_millis() as i64,
        )
        .unwrap();

        let (_engine, plan) = cfg_clone.into_engine_and_plan().unwrap();
        let kernels =
            logsluice_core::simd::SimdKernelTable::detect(cfg_clone.acceleration.simd_mode);

        for s in 0..cfg_clone.shards {
            let shard_dir = data_dir_clone.join(format!("shard_{s}"));
            std::fs::create_dir_all(&shard_dir).unwrap();
            let store_opts = logsluice_core::store::shard_store::StoreOptions {
                shard_dir,
                cache_bytes: 1024 * 1024,
                hdr_memtable_bytes: 1024 * 1024,
                tail_memtable_bytes: 1024 * 1024,
                sync_wal: false,
                layout: plan.layout(),
                worker_threads: 1,
                max_write_buffer_bytes: None,
                max_cached_files: None,
            };
            let mut store =
                logsluice_core::store::shard_store::ShardStore::open(store_opts, &meta).unwrap();

            // Insert enough keys across windows so that opening and recovering takes >200ms
            for window_idx in 0..2 {
                let epoch_ms = (window_idx as u64) * 86_400_000;
                let entries: Vec<logsluice_core::store::shard_store::FlushEntry> = (0..1_000)
                    .map(|i| logsluice_core::store::shard_store::FlushEntry {
                        key: logsluice_core::key::StorageKey24 {
                            epoch_ms,
                            digest: (s as u128) << 32 | (window_idx << 16) | i as u128,
                        },
                        is_new: true,
                        last_update_s: 0,
                        delta: vec![[1, 0, 0, 0]],
                        minmax: vec![(0, 0)],
                        resource_scope_id: 0,
                        tail: None,
                    })
                    .collect();
                store
                    .apply_flush(
                        logsluice_core::store::shard_store::FlushBatch {
                            entries,
                            offsets: vec![],
                            new_resources: vec![],
                            resource_refs: vec![],
                        },
                        &kernels,
                    )
                    .unwrap();
            }
        }
    })
    .await
    .unwrap();

    // Spawn a 10ms ticker task on the current_thread runtime
    let (ticker_ready_tx, ticker_ready_rx) = tokio::sync::oneshot::channel::<()>();
    let (ticker_stop_tx, mut ticker_stop_rx) = tokio::sync::oneshot::channel::<()>();
    let ticker_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(10));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // First tick completes immediately
        interval.tick().await;
        let mut last_tick = tokio::time::Instant::now();
        let _ = ticker_ready_tx.send(());
        let mut max_gap = Duration::ZERO;
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let now = tokio::time::Instant::now();
                    let gap = now.duration_since(last_tick);
                    if gap > max_gap {
                        max_gap = gap;
                    }
                    last_tick = now;
                }
                _ = &mut ticker_stop_rx => {
                    break;
                }
            }
        }
        max_gap
    });

    // Ensure the ticker is armed and running before starting the pipeline
    ticker_ready_rx.await.unwrap();

    // Start pipeline and pump an event through it
    let pipeline = SluicePipeline::new(cfg).unwrap();
    let (tx, rx) = futures::channel::mpsc::unbounded();
    let mut out_stream = Box::new(pipeline).transform_stream(rx);

    // Send an event with ack notifier and wait for ack so we know startup is complete
    let mut event = LogEvent::default();
    let rfc = chrono::DateTime::from_timestamp_millis(T0)
        .unwrap()
        .to_rfc3339();
    event.insert_path("@timestamp", Value::from(rfc));
    event.insert_path("source.ip", Value::from("10.0.0.1"));
    let (event_with_ack, ack_rx) = event.with_ack_notifier();
    tx.unbounded_send(event_with_ack).unwrap();

    assert_eq!(ack_rx.await.unwrap(), AckStatus::Delivered);

    // Stop ticker and collect maximum gap
    let _ = ticker_stop_tx.send(());
    let max_gap = ticker_handle.await.unwrap();

    // Clean up pipeline stream
    drop(tx);
    while let Some(mut ev) = out_stream.next().await {
        let finalizers = ev.take_finalizers();
        finalizers.update_status(AckStatus::Delivered);
    }

    // Assert that the current-thread runtime was not starved during startup.
    // Flake-sensitive parameter: 150ms bound for 10ms ticker while recovery takes >200ms.
    const MAX_ALLOWED_STARVATION_GAP: Duration = Duration::from_millis(150);
    assert!(
        max_gap < MAX_ALLOWED_STARVATION_GAP,
        "Current-thread executor was starved during startup: max gap between 10ms ticks was {max_gap:?}, exceeding bound {MAX_ALLOWED_STARVATION_GAP:?}"
    );
}

#[test]
fn config_parses_storage_limits() {
    let yaml = r#"
storage:
  data_dir: "/tmp/state"
  max_write_buffer_bytes: 64MiB
  max_cached_files: 256
shards: 1
window:
  type: tumbling
  duration: 10s
group_by:
  - service
aggregations:
  event_count: count
"#;
    let cfg: SluiceConfig = serde_yaml_ng::from_str(yaml).unwrap();
    assert_eq!(cfg.storage.max_write_buffer_bytes, Some(64 * 1024 * 1024));
    assert_eq!(cfg.storage.max_cached_files, Some(256));

    let (engine, _plan) = cfg.into_engine_and_plan().unwrap();
    assert_eq!(
        engine.storage.max_write_buffer_bytes,
        Some(64 * 1024 * 1024)
    );
    assert_eq!(engine.storage.max_cached_files, Some(256));
}
