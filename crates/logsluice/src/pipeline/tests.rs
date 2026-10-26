use super::*;
use crate::event::Value;
use logsluice_core::codec::tail::ExemplarTail;
use logsluice_core::emission::{EmitReason, SliceRecord};
use logsluice_core::key::StorageKey24;
use logsluice_core::resource::ResourceScopePayload;
use logsluice_core::worker::{EmitBatch, ExemplarSource};

static TEST_SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn test_shard_runtime(
    worker: ShardWorker<AckFinalizers>,
    store_handle: StoreHandle<AckFinalizers>,
    control_tx: tokio_mpsc::Sender<ControlMessage>,
    output: futures_mpsc::Sender<LogEvent>,
    cfg: Arc<SluiceConfig>,
    plan: Arc<AggregationPlan>,
) -> ShardRuntime {
    ShardRuntime {
        shard_idx: 0,
        worker,
        store_handle,
        control_tx,
        cfg,
        plan,
        detected_otlp: Arc::new(AtomicBool::new(false)),
        output,
        router_stats: Arc::new(std::sync::Mutex::new(RouterStats::default())),
        fail_next_flush: None,
    }
}

use ::metrics::{
    Counter, CounterFn, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit,
};
use std::collections::HashMap;
use std::sync::OnceLock;

type CounterKey = (String, Vec<(String, String)>);
type CounterMap = Arc<std::sync::Mutex<HashMap<CounterKey, u64>>>;

#[derive(Clone)]
struct PipelineTestCounter {
    name: String,
    labels: Vec<(String, String)>,
    values: CounterMap,
}

impl CounterFn for PipelineTestCounter {
    fn increment(&self, value: u64) {
        if let Ok(mut guard) = self.values.lock() {
            *guard
                .entry((self.name.clone(), self.labels.clone()))
                .or_insert(0) += value;
        }
    }

    fn absolute(&self, value: u64) {
        if let Ok(mut guard) = self.values.lock() {
            guard.insert((self.name.clone(), self.labels.clone()), value);
        }
    }
}

#[derive(Default, Clone)]
struct PipelineTestRecorder {
    counters: CounterMap,
}

impl Recorder for PipelineTestRecorder {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        let labels = key
            .labels()
            .map(|l| (l.key().to_string(), l.value().to_string()))
            .collect();
        Counter::from_arc(Arc::new(PipelineTestCounter {
            name: key.name().to_string(),
            labels,
            values: Arc::clone(&self.counters),
        }))
    }

    fn register_gauge(&self, _key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        Gauge::noop()
    }

    fn register_histogram(&self, _key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        Histogram::noop()
    }
}

fn init_pipeline_test_recorder() -> CounterMap {
    static COUNTERS: OnceLock<CounterMap> = OnceLock::new();
    COUNTERS
        .get_or_init(|| {
            let map: CounterMap = Arc::new(std::sync::Mutex::new(HashMap::new()));
            let recorder = PipelineTestRecorder {
                counters: Arc::clone(&map),
            };
            recorder.describe_counter("c".into(), None, "".into());
            recorder.describe_gauge("g".into(), None, "".into());
            recorder.describe_histogram("h".into(), None, "".into());
            static META: Metadata<'static> =
                Metadata::new(module_path!(), ::metrics::Level::INFO, None);
            let key = Key::from_name("init");
            let _ = recorder.register_gauge(&key, &META);
            let _ = recorder.register_histogram(&key, &META);
            recorder.register_counter(&key, &META).increment(0);
            let _ = ::metrics::set_global_recorder(recorder);
            map
        })
        .clone()
}

#[tokio::test]
async fn pipeline_flush_failure_marks_ack_errored() {
    let _serial = TEST_SERIAL.lock().await;
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
  drain_timeout: 200ms
"#,
        dir.path()
    );

    let cfg: SluiceConfig = serde_yaml_ng::from_str(&yaml).unwrap();
    let pipeline = SluicePipeline::new(cfg).unwrap();
    let armed = pipeline.clone();
    let (tx, rx) = futures_mpsc::unbounded();
    let _out = Box::new(pipeline).transform_stream(rx);
    // Arm after spawn. The flag is read when the flush is sent, so the
    // shard can already be parked in `select!`.
    armed.arm_fail_next_flush();

    let mut log = LogEvent::default();
    let rfc = chrono::DateTime::from_timestamp_millis(1_728_000_000_000)
        .unwrap()
        .to_rfc3339();
    log.insert_path("@timestamp", Value::from(rfc));
    log.insert_path("source.ip", Value::from("10.0.0.1"));
    let (log_with_ack, ack_rx) = log.with_ack_notifier();

    tx.unbounded_send(log_with_ack).unwrap();

    let status = tokio::time::timeout(Duration::from_secs(2), ack_rx)
        .await
        .expect("timed out waiting for flush ack")
        .expect("ack channel closed");
    assert_eq!(status, AckStatus::Errored);

    drop(tx);
}

#[tokio::test]
async fn flush_micro_batch_marks_errored_when_shard_closed() {
    let (tx, rx) = tokio_mpsc::channel::<(Vec<IngestItem>, AckFinalizers)>(1);
    drop(rx);

    let (finalizer, receiver) = AckNotifier::new_with_receiver();
    let mut mb = ShardMicroBatch::new();
    mb.items.push(IngestItem {
        key: StorageKey24 {
            epoch_ms: 1_700_000_000_000,
            digest: 42,
        },
        resource: empty_resource(),
        resource_hash: 0,
        position: None,
        event_ms: 1_700_000_000_100,
        counters: vec![[1, 0, 0, 0]],
        minmax_values: Vec::new(),
        exemplar: ExemplarSource {
            severity: 9,
            body: None,
            dims: vec![Some(bytes::Bytes::from_static(b"api"))],
            attrs: Vec::new(),
        },
    });
    mb.finalizer.push(finalizer);

    let mut batches = vec![mb];
    let ingest_txs = vec![tx];

    flush_micro_batch(0, &mut batches, &ingest_txs).await;

    let status = tokio::time::timeout(Duration::from_secs(1), receiver)
        .await
        .expect("timeout waiting for ack")
        .expect("ack channel closed");
    assert_eq!(status, AckStatus::Errored);
}

#[tokio::test]
async fn empty_resource_logs_envelope_still_delivered() {
    let _serial = TEST_SERIAL.lock().await;
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
  drain_timeout: 200ms
"#,
        dir.path()
    );

    let cfg: SluiceConfig = serde_yaml_ng::from_str(&yaml).unwrap();
    let pipeline = SluicePipeline::new(cfg).unwrap();
    let (tx, rx) = futures_mpsc::unbounded();
    let _out = Box::new(pipeline).transform_stream(rx);

    let mut log = LogEvent::default();
    log.insert_path("resourceLogs", Value::Array(Vec::new()));
    let (log_with_ack, ack_rx) = log.with_ack_notifier();

    tx.unbounded_send(log_with_ack).unwrap();

    let status = tokio::time::timeout(Duration::from_secs(2), ack_rx)
        .await
        .expect("timed out waiting for empty envelope ack")
        .expect("ack channel closed");
    assert_eq!(status, AckStatus::Delivered);

    drop(tx);
}

#[tokio::test(flavor = "current_thread")]
async fn pipeline_shard_loop_does_not_block_when_store_queue_full() {
    let _serial = TEST_SERIAL.lock().await;
    use std::collections::BTreeMap;

    let dir = tempfile::TempDir::new().unwrap();
    let yaml = format!(
        r#"
storage:
  data_dir: {:?}
  l1_flush_interval: 10ms
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
  drain_timeout: 500ms
"#,
        dir.path()
    );

    let cfg: SluiceConfig = serde_yaml_ng::from_str(&yaml).unwrap();
    std::fs::create_dir_all(&cfg.storage.data_dir).unwrap();
    let shard_dir = cfg.storage.data_dir.join("shard_0");
    std::fs::create_dir_all(&shard_dir).unwrap();

    let codec_schema = cfg.codec_schema();
    let pod_meta = PodMeta::load_or_init_with_window(
        &cfg.storage.data_dir,
        cfg.shards,
        &codec_schema,
        Some(cfg.output.emitter_id.as_str()),
        cfg.window.duration.as_millis() as i64,
        cfg.window.offset.as_millis() as i64,
    )
    .unwrap();

    let (engine, plan) = cfg.clone().into_engine_and_plan().unwrap();
    let engine = Arc::new(engine);
    let plan = Arc::new(plan);
    let clock = WindowClock::new(cfg.window.duration, cfg.window.offset).unwrap();
    let kernels = SimdKernelTable::detect(cfg.acceleration.simd_mode);

    let store_opts = StoreOptions {
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
    let store = ShardStore::open(store_opts, &pod_meta).unwrap();
    let worker_ctx = WorkerContext {
        cfg: engine,
        shard: 0,
        emitter_id: Arc::from(pod_meta.emitter_id.as_str()),
        kernels,
        plan: Arc::clone(&plan),
        clock,
    };
    let recovered = RecoveredState::load(&store, &worker_ctx).unwrap();
    let (store_handle, store_events, store_thread) =
        StoreThread::spawn::<AckFinalizers>(store, kernels).unwrap();
    let worker = ShardWorker::new(worker_ctx, recovered, PurgeHorizon::new(0));

    // Stall the store thread and fill its 64-slot bounded command queue.
    let barrier = Arc::new(std::sync::Barrier::new(2));
    store_handle
        .try_send(StoreCommand::TestPause(Arc::clone(&barrier)))
        .expect("enqueue TestPause");
    barrier.wait();

    for epoch_ms in 0..64 {
        store_handle
            .try_send(StoreCommand::DeleteKeys {
                epoch_ms,
                keys: Vec::new(),
            })
            .expect("fill bounded command queue");
    }

    let (ingest_tx, ingest_rx) = tokio_mpsc::channel::<(Vec<IngestItem>, AckFinalizers)>(64);
    let (control_tx, control_rx) = tokio_mpsc::channel::<ControlMessage>(64);
    let (output_tx, _output_rx) = futures_mpsc::channel::<LogEvent>(64);

    let shard_handle = tokio::spawn(
        test_shard_runtime(
            worker,
            store_handle.clone(),
            control_tx.clone(),
            output_tx,
            Arc::new(cfg),
            plan,
        )
        .shard_loop(store_events, store_thread, ingest_rx, control_rx),
    );

    let resource = Arc::new(ResourceScopePayload {
        resource: BTreeMap::new(),
        scope_name: "test".to_string(),
        scope_version: "1.0".to_string(),
        scope_attrs: BTreeMap::new(),
    });
    let resource_hash = resource.canonical_hash();
    let (ack_notifier, ack_rx) = AckNotifier::new_with_receiver();
    let item = IngestItem {
        key: StorageKey24 {
            epoch_ms: 0,
            digest: 1,
        },
        resource,
        resource_hash,
        position: None,
        event_ms: 1_000,
        counters: vec![[1, 0, 0, 0]],
        minmax_values: vec![],
        exemplar: ExemplarSource::default(),
    };

    ingest_tx
        .send((vec![item], AckFinalizers::from_single(ack_notifier)))
        .await
        .unwrap();
    control_tx
        .send(ControlMessage::Poll {
            watermark: None,
            now_ms: 1_000,
        })
        .await
        .unwrap();

    let other_task_progressed = Arc::new(AtomicBool::new(false));
    let runtime_stalled = Arc::new(AtomicBool::new(false));
    let progressed_wd = Arc::clone(&other_task_progressed);
    let stalled_wd = Arc::clone(&runtime_stalled);
    let barrier_wd = Arc::clone(&barrier);

    let watchdog = std::thread::spawn(move || {
        let start = std::time::Instant::now();
        while !progressed_wd.load(Ordering::Acquire) {
            if start.elapsed() >= Duration::from_millis(250) {
                stalled_wd.store(true, Ordering::Release);
                barrier_wd.wait();
                return;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    });

    for _ in 0..8 {
        tokio::task::yield_now().await;
    }

    let progressed_task = Arc::clone(&other_task_progressed);
    tokio::spawn(async move {
        progressed_task.store(true, Ordering::Release);
    })
    .await
    .unwrap();

    if !runtime_stalled.load(Ordering::Acquire) {
        barrier.wait();
    }
    watchdog.join().unwrap();

    assert!(
        !runtime_stalled.load(Ordering::Acquire),
        "shard_loop blocked the current_thread Tokio runtime when store command queue was full"
    );

    control_tx
        .send(ControlMessage::Poll {
            watermark: None,
            now_ms: 2_000,
        })
        .await
        .unwrap();

    let status = tokio::time::timeout(Duration::from_secs(2), ack_rx)
        .await
        .expect("timed out waiting for deferred flush ack")
        .expect("ack channel closed");
    assert_eq!(status, AckStatus::Delivered);

    control_tx
        .send(ControlMessage::Drain {
            deadline_ms: 10_000,
        })
        .await
        .unwrap();
    shard_handle.await.unwrap();
}

fn test_config(dir: &std::path::Path) -> SluiceConfig {
    let yaml = format!(
        r#"
storage:
  data_dir: {:?}
  l1_flush_interval: 10ms
  sync_wal: false
shards: 1
window:
  type: tumbling
  duration: 10s
  allowed_skew: 0s
  allowed_lateness: 0s
  late_emit_interval: 10ms
  max_close_delay: 10ms
  max_future_drift: 1d
  emit:
close_jitter_max: 0s
group_by:
  - service
aggregations:
  event_count: count
shutdown:
  drain_timeout: 100ms
"#,
        dir
    );
    serde_yaml_ng::from_str(&yaml).expect("valid config")
}

fn empty_resource() -> Arc<ResourceScopePayload> {
    Arc::new(ResourceScopePayload {
        resource: std::collections::BTreeMap::new(),
        scope_name: String::new(),
        scope_version: String::new(),
        scope_attrs: std::collections::BTreeMap::new(),
    })
}

fn sample_slice_record(ticket_seq: u32) -> SliceRecord {
    SliceRecord {
        window_start_ms: 1_700_000_000_000,
        window_end_ms: 1_700_000_010_000,
        emit_reason: EmitReason::Close,
        emitter_id: "test-emitter".to_string(),
        digest: u128::from(ticket_seq),
        slice_seq: ticket_seq,
        slice_id: format!("test-emitter:1700000000000:{ticket_seq:032x}:{ticket_seq}"),
        resource: empty_resource(),
        tail: ExemplarTail {
            severity: 9,
            body: None,
            dims: vec![Some(bytes::Bytes::from_static(b"api"))],
            attrs: Vec::new(),
        },
        counters: vec![[1, 0, 0, 0]],
        minmax: Vec::new(),
    }
}

#[tokio::test]
async fn passthrough_closed_output_channel_marks_finalizer_errored_and_stops_pipeline() {
    let _serial = TEST_SERIAL.lock().await;
    let dir = tempfile::TempDir::new().expect("tempdir");
    let cfg = test_config(dir.path());
    let pipeline = SluicePipeline::new(cfg).expect("pipeline");
    let (tx, rx) = futures_mpsc::unbounded();
    let out_stream = Box::new(pipeline).transform_stream(rx);

    // Immediately close the downstream output channel.
    drop(out_stream);

    // Send an event without @timestamp so it routes to passthrough (quarantine).
    let mut quarantine_event = LogEvent::default();
    quarantine_event.insert_path("service", Value::from("auth"));
    let (quarantine_with_ack, ack_rx) = quarantine_event.with_ack_notifier();
    tx.unbounded_send(quarantine_with_ack).expect("send event");

    let status = tokio::time::timeout(Duration::from_secs(2), ack_rx)
        .await
        .expect("ack should resolve within timeout")
        .expect("ack oneshot should not drop");
    assert_eq!(status, AckStatus::Errored);

    // Keep `tx` alive and verify `run_pipeline` exits its loop and drops `rx`.
    tokio::time::timeout(Duration::from_millis(500), async {
        while !tx.is_closed() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("run_pipeline should stop and drop input stream when output_tx is closed");
}

#[tokio::test]
async fn closed_output_channel_nacks_remaining_batches_and_stops_shard_loop() {
    let _serial = TEST_SERIAL.lock().await;
    let dir = tempfile::TempDir::new().expect("tempdir");
    let cfg = test_config(dir.path());
    let codec_schema = cfg.codec_schema();
    let pod_meta = PodMeta::load_or_init_with_window(
        dir.path(),
        1,
        &codec_schema,
        Some("test-emitter"),
        10_000,
        0,
    )
    .expect("pod meta");
    let (engine, plan) = cfg.clone().into_engine_and_plan().expect("engine and plan");
    let shard_dir = dir.path().join("shard_0");
    std::fs::create_dir_all(&shard_dir).expect("shard dir");
    let store = ShardStore::open(
        StoreOptions {
            shard_dir,
            cache_bytes: 1024 * 1024,
            hdr_memtable_bytes: 1024 * 1024,
            tail_memtable_bytes: 1024 * 1024,
            sync_wal: false,
            layout: plan.layout(),
            worker_threads: 1,
            max_write_buffer_bytes: None,
            max_cached_files: None,
        },
        &pod_meta,
    )
    .expect("open shard store");
    let kernels = SimdKernelTable::detect(cfg.acceleration.simd_mode);
    let worker_ctx = WorkerContext {
        cfg: Arc::new(engine),
        shard: 0,
        emitter_id: Arc::from("test-emitter"),
        kernels,
        plan: Arc::new(plan.clone()),
        clock: WindowClock::new(cfg.window.duration, cfg.window.offset).expect("clock"),
    };
    let recovered = RecoveredState::load(&store, &worker_ctx).expect("recovered");
    let (store_handle, store_events, store_thread) =
        StoreThread::spawn::<AckFinalizers>(store, kernels).expect("store thread");
    let worker = ShardWorker::new(worker_ctx, recovered, PurgeHorizon::new(0));

    let (output_tx, output_rx) = futures_mpsc::channel::<LogEvent>(16);
    drop(output_rx);

    let (ack_tx, mut ack_rx) = tokio_mpsc::channel::<ControlMessage>(16);
    let out = WorkerOutput {
        commands: Vec::new(),
        emit: vec![
            EmitBatch {
                ticket_id: 10,
                records: vec![sample_slice_record(1)],
            },
            EmitBatch {
                ticket_id: 20,
                records: vec![sample_slice_record(2)],
            },
        ],
        release: Vec::new(),
        failed_release: Vec::new(),
    };

    let mut runtime = test_shard_runtime(
        worker,
        store_handle,
        ack_tx,
        output_tx,
        Arc::new(cfg),
        Arc::new(plan),
    );
    let output_open = runtime.emit(out).await;
    assert!(!output_open);

    let msg1 = ack_rx
        .try_recv()
        .expect("first batch should be nacked immediately");
    assert!(matches!(
        msg1,
        ControlMessage::Ack {
            ticket_id: 10,
            delivered: false
        }
    ));
    let msg2 = ack_rx
        .try_recv()
        .expect("remaining batch should be nacked immediately");
    assert!(matches!(
        msg2,
        ControlMessage::Ack {
            ticket_id: 20,
            delivered: false
        }
    ));

    // Now verify `shard_loop` terminates when `emit` fails on a closed output channel.
    let (ingest_tx, ingest_rx) = tokio_mpsc::channel::<(Vec<IngestItem>, AckFinalizers)>(16);
    let (control_tx, control_rx) = tokio_mpsc::channel::<ControlMessage>(16);
    runtime.control_tx = control_tx.clone();
    let loop_handle =
        tokio::spawn(runtime.shard_loop(store_events, store_thread, ingest_rx, control_rx));

    let item = IngestItem {
        key: StorageKey24 {
            epoch_ms: 1_700_000_000_000,
            digest: 42,
        },
        resource: empty_resource(),
        resource_hash: 0,
        position: None,
        event_ms: 1_700_000_000_100,
        counters: vec![[1, 0, 0, 0]],
        minmax_values: Vec::new(),
        exemplar: ExemplarSource {
            severity: 9,
            body: None,
            dims: vec![Some(bytes::Bytes::from_static(b"api"))],
            attrs: Vec::new(),
        },
    };
    let (ingest_finalizer, ingest_ack_rx) = AckNotifier::new_with_receiver();
    ingest_tx
        .send((vec![item], AckFinalizers::from_single(ingest_finalizer)))
        .await
        .expect("send ingest");
    control_tx
        .send(ControlMessage::Poll {
            watermark: Some(1_700_000_020_000),
            now_ms: 1_700_000_020_000,
        })
        .await
        .expect("send poll to flush and close");
    assert_eq!(
        ingest_ack_rx.await.expect("flush ack"),
        AckStatus::Delivered
    );

    // Trigger the close scan after L1 flush completes.
    control_tx
        .send(ControlMessage::Poll {
            watermark: Some(1_700_000_020_000),
            now_ms: 1_700_000_020_010,
        })
        .await
        .expect("send second poll");

    tokio::time::timeout(Duration::from_millis(500), loop_handle)
        .await
        .expect("shard_loop should exit when output_tx is closed")
        .expect("shard_loop task should not panic");
}

#[tokio::test]
async fn retry_ticks_reuse_last_watermark_avoiding_forced_close() {
    let _serial = TEST_SERIAL.lock().await;
    let counters = init_pipeline_test_recorder();
    if let Ok(mut guard) = counters.lock() {
        guard.insert(
            (
                "logsluice_forced_closes_total".to_string(),
                vec![("shard".to_string(), "0".to_string())],
            ),
            0,
        );
        guard.insert(
            (
                "logsluice_events_ingested_total".to_string(),
                vec![("shard".to_string(), "0".to_string())],
            ),
            0,
        );
    }

    let dir = tempfile::TempDir::new().unwrap();
    let yaml = format!(
        r#"
storage:
  data_dir: {:?}
  l1_flush_interval: 10ms
  sync_wal: false
shards: 1
window:
  type: tumbling
  duration: 10s
  allowed_skew: 1s
  allowed_lateness: 2s
  late_emit_interval: 20ms
  max_close_delay: 2s
  max_future_drift: 2d
  emit:
close_jitter_max: 0s
group_by:
  - source.ip
aggregations:
  event_count: count
shutdown:
  drain_timeout: 500ms
"#,
        dir.path()
    );

    let cfg: SluiceConfig = serde_yaml_ng::from_str(&yaml).unwrap();
    std::fs::create_dir_all(&cfg.storage.data_dir).unwrap();
    let shard_dir = cfg.storage.data_dir.join("shard_0");
    std::fs::create_dir_all(&shard_dir).unwrap();

    let codec_schema = cfg.codec_schema();
    let pod_meta = PodMeta::load_or_init_with_window(
        &cfg.storage.data_dir,
        cfg.shards,
        &codec_schema,
        Some(cfg.output.emitter_id.as_str()),
        cfg.window.duration.as_millis() as i64,
        cfg.window.offset.as_millis() as i64,
    )
    .unwrap();

    let (engine, plan) = cfg.clone().into_engine_and_plan().unwrap();
    let engine = Arc::new(engine);
    let plan = Arc::new(plan);
    let clock = WindowClock::new(cfg.window.duration, cfg.window.offset).unwrap();
    let kernels = SimdKernelTable::detect(cfg.acceleration.simd_mode);

    let store_opts = StoreOptions {
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
    let store = ShardStore::open(store_opts, &pod_meta).unwrap();
    let worker_ctx = WorkerContext {
        cfg: engine,
        shard: 0,
        emitter_id: Arc::from(pod_meta.emitter_id.as_str()),
        kernels,
        plan: Arc::clone(&plan),
        clock,
    };
    let recovered = RecoveredState::load(&store, &worker_ctx).unwrap();
    let (store_handle, store_events, store_thread) =
        StoreThread::spawn::<AckFinalizers>(store, kernels).unwrap();
    let mut worker = ShardWorker::new(worker_ctx, recovered, PurgeHorizon::new(0));

    // Stall store thread and saturate command queue
    let barrier = Arc::new(std::sync::Barrier::new(2));
    store_handle
        .try_send(StoreCommand::TestPause(Arc::clone(&barrier)))
        .expect("enqueue TestPause");
    barrier.wait();

    for epoch_ms in 0..64 {
        store_handle
            .try_send(StoreCommand::DeleteKeys {
                epoch_ms,
                keys: Vec::new(),
            })
            .expect("fill bounded command queue");
    }
    worker.defer_command(StoreCommand::DeleteKeys {
        epoch_ms: 0,
        keys: Vec::new(),
    });

    let (ingest_tx, ingest_rx) = tokio_mpsc::channel::<(Vec<IngestItem>, AckFinalizers)>(64);
    let (control_tx, control_rx) = tokio_mpsc::channel::<ControlMessage>(64);
    let (output_tx, _output_rx) = futures_mpsc::channel::<LogEvent>(64);

    let shard_handle = tokio::spawn(
        test_shard_runtime(
            worker,
            store_handle.clone(),
            control_tx.clone(),
            output_tx,
            Arc::new(cfg),
            plan,
        )
        .shard_loop(store_events, store_thread, ingest_rx, control_rx),
    );

    let now_wall = chrono::Utc::now().timestamp_millis();
    let event_ms = now_wall - 20_000;
    let window_end = clock.end_of(clock.start_of(event_ms));

    // 1. Establish watermark >= window_end before the window is created
    control_tx
        .send(ControlMessage::Poll {
            watermark: Some(window_end),
            now_ms: now_wall,
        })
        .await
        .expect("send initial poll");

    // 2. Ingest an item into that window
    let resource = Arc::new(ResourceScopePayload {
        resource: std::collections::BTreeMap::new(),
        scope_name: "test".to_string(),
        scope_version: "1.0".to_string(),
        scope_attrs: std::collections::BTreeMap::new(),
    });
    let resource_hash = resource.canonical_hash();
    let (ack_notifier, _ack_rx) = AckNotifier::new_with_receiver();
    let item = IngestItem {
        key: StorageKey24 {
            epoch_ms: clock.start_of(event_ms) as u64,
            digest: 1,
        },
        resource,
        resource_hash,
        position: None,
        event_ms,
        counters: vec![[1, 0, 0, 0]],
        minmax_values: Vec::new(),
        exemplar: ExemplarSource::default(),
    };

    ingest_tx
        .send((vec![item], AckFinalizers::from_single(ack_notifier)))
        .await
        .expect("send ingest");

    // 3. Wait for drain_interval.tick() (25ms) to fire and execute worker.poll(...)
    tokio::time::sleep(Duration::from_millis(80)).await;

    // 4. Send Poll to trigger metrics export
    control_tx
        .send(ControlMessage::Poll {
            watermark: Some(window_end),
            now_ms: chrono::Utc::now().timestamp_millis(),
        })
        .await
        .expect("send second poll");

    let start = std::time::Instant::now();
    loop {
        let ingested = counters
            .lock()
            .unwrap()
            .get(&(
                "logsluice_events_ingested_total".to_string(),
                vec![("shard".to_string(), "0".to_string())],
            ))
            .copied()
            .unwrap_or(0);
        if ingested == 1 {
            break;
        }
        assert!(start.elapsed() <= Duration::from_secs(2));
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let forced_closes = counters
        .lock()
        .unwrap()
        .get(&(
            "logsluice_forced_closes_total".to_string(),
            vec![("shard".to_string(), "0".to_string())],
        ))
        .copied()
        .unwrap_or(0);

    // Before the fix: drain_interval.tick() calls worker.poll(None, now_ms),
    // treating the window close as forced (forced_closes == 1).
    // With the fix: shard_loop retains last_watermark (Some(window_end)),
    // closing cleanly (forced_closes == 0).
    assert_eq!(forced_closes, 0);

    // Cleanup
    barrier.wait();
    control_tx
        .send(ControlMessage::Drain { deadline_ms: 0 })
        .await
        .expect("send drain");
    shard_handle.await.expect("join shard_handle");
}

#[tokio::test]
async fn ack_is_applied_while_l1_is_full_and_flush_stalled() {
    let _serial = TEST_SERIAL.lock().await;
    let dir = tempfile::TempDir::new().unwrap();
    let yaml = format!(
        r#"
storage:
  data_dir: {:?}
  l1_flush_interval: 10ms
  l1_coalesce_max_keys: 1
  sync_wal: false
shards: 1
window:
  type: tumbling
  duration: 10s
  allowed_skew: 0s
  allowed_lateness: 0s
  late_emit_interval: 10ms
  max_close_delay: 10ms
  max_future_drift: 1d
  emit:
close_jitter_max: 0s
group_by:
  - service
aggregations:
  event_count: count
shutdown:
  drain_timeout: 100ms
"#,
        dir.path()
    );

    let cfg: SluiceConfig = serde_yaml_ng::from_str(&yaml).unwrap();
    std::fs::create_dir_all(&cfg.storage.data_dir).unwrap();
    let shard_dir = cfg.storage.data_dir.join("shard_0");
    std::fs::create_dir_all(&shard_dir).unwrap();

    let codec_schema = cfg.codec_schema();
    let pod_meta = PodMeta::load_or_init_with_window(
        &cfg.storage.data_dir,
        cfg.shards,
        &codec_schema,
        Some(cfg.output.emitter_id.as_str()),
        cfg.window.duration.as_millis() as i64,
        cfg.window.offset.as_millis() as i64,
    )
    .unwrap();

    let (engine, plan) = cfg.clone().into_engine_and_plan().unwrap();
    let engine = Arc::new(engine);
    let plan = Arc::new(plan);
    let clock = WindowClock::new(cfg.window.duration, cfg.window.offset).unwrap();
    let kernels = SimdKernelTable::detect(cfg.acceleration.simd_mode);

    let store_opts = StoreOptions {
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
    let mut store = ShardStore::open(store_opts, &pod_meta).unwrap();
    let worker_ctx = WorkerContext {
        cfg: engine,
        shard: 0,
        emitter_id: Arc::from(pod_meta.emitter_id.as_str()),
        kernels,
        plan: Arc::clone(&plan),
        clock,
    };
    let mut recovered = RecoveredState::load(&store, &worker_ctx).unwrap();

    let ticket = logsluice_core::codec::ticket::Ticket {
        id: 12345,
        window_epoch_ms: 0,
        reason: EmitReason::Periodic,
        entries: Vec::new(),
    };
    store.put_tickets(std::slice::from_ref(&ticket)).unwrap();
    recovered.ledger.restore(vec![ticket]);

    let (store_handle, mut real_store_events, store_thread) =
        StoreThread::spawn::<AckFinalizers>(store, kernels).unwrap();
    let worker = ShardWorker::new(worker_ctx, recovered, PurgeHorizon::new(0));

    let (_controlled_events_tx, controlled_events_rx) =
        futures_mpsc::unbounded::<StoreEvent<AckFinalizers>>();

    let (ingest_tx, ingest_rx) = tokio_mpsc::channel::<(Vec<IngestItem>, AckFinalizers)>(64);
    let (control_tx, control_rx) = tokio_mpsc::channel::<ControlMessage>(64);
    let (output_tx, _output_rx) = futures_mpsc::channel::<LogEvent>(64);

    let shard_handle = tokio::spawn(
        test_shard_runtime(
            worker,
            store_handle.clone(),
            control_tx.clone(),
            output_tx,
            Arc::new(cfg),
            plan,
        )
        .shard_loop(controlled_events_rx, store_thread, ingest_rx, control_rx),
    );

    let resource = empty_resource();
    let resource_hash = resource.canonical_hash();
    let item1 = IngestItem {
        key: StorageKey24 {
            epoch_ms: 0,
            digest: 1,
        },
        resource: resource.clone(),
        resource_hash,
        position: None,
        event_ms: 1_000,
        counters: vec![[1, 0, 0, 0]],
        minmax_values: Vec::new(),
        exemplar: ExemplarSource::default(),
    };
    let (notifier1, _rx1) = AckNotifier::new_with_receiver();
    ingest_tx
        .send((vec![item1], AckFinalizers::from_single(notifier1)))
        .await
        .unwrap();

    let item2 = IngestItem {
        key: StorageKey24 {
            epoch_ms: 0,
            digest: 2,
        },
        resource,
        resource_hash,
        position: None,
        event_ms: 1_000,
        counters: vec![[1, 0, 0, 0]],
        minmax_values: Vec::new(),
        exemplar: ExemplarSource::default(),
    };
    let (notifier2, _rx2) = AckNotifier::new_with_receiver();
    ingest_tx
        .send((vec![item2], AckFinalizers::from_single(notifier2)))
        .await
        .unwrap();

    // Wait until store thread processes Flush(swapped) and sends FlushDone on real_store_events.
    // Because shard_loop uses controlled_events_rx (which has not delivered FlushDone),
    // shard_loop's worker still has flush_in_flight == true and active.len() >= 1,
    // so worker.can_ingest() == false.
    let ev = tokio::time::timeout(Duration::from_millis(500), real_store_events.next())
        .await
        .expect("store thread should complete flush")
        .expect("real_store_events open");
    assert!(matches!(ev, StoreEvent::FlushDone { .. }));

    // Now send Ack.
    control_tx
        .send(ControlMessage::Ack {
            ticket_id: 12345,
            delivered: true,
        })
        .await
        .unwrap();

    // The Ack must be applied (emitting AckApplied on real_store_events) while flush is stalled.
    let ack_ev = tokio::time::timeout(Duration::from_millis(200), real_store_events.next())
        .await
        .expect("Ack should be applied while L1 is full and flush is stalled");
    assert!(matches!(
        ack_ev,
        Some(StoreEvent::AckApplied {
            ticket_id: 12345,
            ..
        })
    ));

    // Cleanup
    shard_handle.abort();
}

#[tokio::test]
async fn shard_loop_startup_and_exit_drain_queued_ingest_as_errored() {
    let _serial = TEST_SERIAL.lock().await;
    let dir = tempfile::TempDir::new().unwrap();
    let yaml = format!(
        r#"
storage:
  data_dir: {:?}
  l1_flush_interval: 10ms
  max_disk_bytes: 1024B
  control_reserve_bytes: 512B
  on_disk_full: block
  sync_wal: false
shards: 1
window:
  type: tumbling
  duration: 10s
  allowed_skew: 0s
  allowed_lateness: 0s
  late_emit_interval: 10ms
  max_close_delay: 10ms
  max_future_drift: 1d
  emit:
close_jitter_max: 0s
group_by:
  - service
aggregations:
  event_count: count
shutdown:
  drain_timeout: 100ms
"#,
        dir.path()
    );

    let cfg: SluiceConfig = serde_yaml_ng::from_str(&yaml).unwrap();
    std::fs::create_dir_all(&cfg.storage.data_dir).unwrap();
    let shard_dir = cfg.storage.data_dir.join("shard_0");
    std::fs::create_dir_all(&shard_dir).unwrap();

    let codec_schema = cfg.codec_schema();
    let pod_meta = PodMeta::load_or_init_with_window(
        &cfg.storage.data_dir,
        cfg.shards,
        &codec_schema,
        Some(cfg.output.emitter_id.as_str()),
        cfg.window.duration.as_millis() as i64,
        cfg.window.offset.as_millis() as i64,
    )
    .unwrap();

    let (engine, plan) = cfg.clone().into_engine_and_plan().unwrap();
    let engine = Arc::new(engine);
    let plan = Arc::new(plan);
    let clock = WindowClock::new(cfg.window.duration, cfg.window.offset).unwrap();
    let kernels = SimdKernelTable::detect(cfg.acceleration.simd_mode);

    // Part A: Startup with a recovered ticket and closed output_rx drains queued ingest_rx as Errored.
    let store_opts = StoreOptions {
        shard_dir: shard_dir.clone(),
        cache_bytes: 1024 * 1024,
        hdr_memtable_bytes: 1024 * 1024,
        tail_memtable_bytes: 1024 * 1024,
        sync_wal: false,
        layout: plan.layout(),
        worker_threads: 1,
        max_write_buffer_bytes: None,
        max_cached_files: None,
    };
    let mut store = ShardStore::open(store_opts.clone(), &pod_meta).unwrap();
    let resource = empty_resource();
    let resource_hash = resource.canonical_hash();
    let mut flush_buf = logsluice_core::l1::L1Buffer::<AckFinalizers>::new(16);
    flush_buf.record_resource(1, Arc::clone(&resource), 0);
    flush_buf.accumulate(
        StorageKey24 {
            epoch_ms: 0,
            digest: 42,
        },
        true,
        1,
        &[[1, 0, 0, 0]],
        &[],
        1,
        Some(ExemplarTail {
            severity: 9,
            body: None,
            dims: vec![Some(bytes::Bytes::from_static(b"api"))],
            attrs: Vec::new(),
        }),
        &kernels,
    );
    let (flush_batch, _) = flush_buf.into_flush();
    store.apply_flush(flush_batch, &kernels).unwrap();
    let ticket = logsluice_core::codec::ticket::Ticket {
        id: 77,
        window_epoch_ms: 0,
        reason: EmitReason::Close,
        entries: vec![logsluice_core::codec::ticket::TicketEntry {
            digest: 42,
            delta: vec![[1, 0, 0, 0]],
            minmax: Vec::new(),
            slice_seq: 1,
        }],
    };
    store.put_tickets(std::slice::from_ref(&ticket)).unwrap();

    let worker_ctx = WorkerContext {
        cfg: Arc::clone(&engine),
        shard: 0,
        emitter_id: Arc::from(pod_meta.emitter_id.as_str()),
        kernels,
        plan: Arc::clone(&plan),
        clock,
    };
    let recovered = RecoveredState::load(&store, &worker_ctx).unwrap();
    let (store_handle_a, store_events_a, store_thread_a) =
        StoreThread::spawn::<AckFinalizers>(store, kernels).unwrap();
    let mut worker_a = ShardWorker::new(worker_ctx.clone(), recovered, PurgeHorizon::new(0));
    worker_a.set_disk_bytes_for_test(Some(2048));

    let (ingest_tx_a, ingest_rx_a) = tokio_mpsc::channel::<(Vec<IngestItem>, AckFinalizers)>(16);
    let (control_tx_a, control_rx_a) = tokio_mpsc::channel::<ControlMessage>(16);
    let (output_tx_a, output_rx_a) = futures_mpsc::channel::<LogEvent>(16);
    drop(output_rx_a);

    let make_item = |digest: u128| IngestItem {
        key: StorageKey24 {
            epoch_ms: 0,
            digest,
        },
        resource: Arc::clone(&resource),
        resource_hash,
        position: None,
        event_ms: 1_000,
        counters: vec![[1, 0, 0, 0]],
        minmax_values: Vec::new(),
        exemplar: ExemplarSource {
            severity: 9,
            body: None,
            dims: vec![Some(bytes::Bytes::from_static(b"api"))],
            attrs: Vec::new(),
        },
    };

    let (notifier_a, ack_rx_a) = AckNotifier::new_with_receiver();
    ingest_tx_a
        .send((vec![make_item(99)], AckFinalizers::from_single(notifier_a)))
        .await
        .unwrap();
    control_tx_a
        .send(ControlMessage::Poll {
            watermark: None,
            now_ms: 1_000,
        })
        .await
        .unwrap();

    test_shard_runtime(
        worker_a,
        store_handle_a,
        control_tx_a,
        output_tx_a,
        Arc::new(cfg.clone()),
        Arc::clone(&plan),
    )
    .shard_loop(store_events_a, store_thread_a, ingest_rx_a, control_rx_a)
    .await;

    assert_eq!(ack_rx_a.await, Ok(AckStatus::Errored));

    // Part B: Runtime loop exit with pending_ingest (OnDiskFull::Block) and queued ingest_rx.
    let store_b = ShardStore::open(store_opts, &pod_meta).unwrap();
    let (store_handle_b, _real_events_b, store_thread_b) =
        StoreThread::spawn::<AckFinalizers>(store_b, kernels).unwrap();
    let mut worker_b = ShardWorker::new(worker_ctx, RecoveredState::empty(), PurgeHorizon::new(0));
    worker_b.set_disk_bytes_for_test(Some(2048));

    let (mut store_events_tx_b, store_events_rx_b) =
        futures_mpsc::unbounded::<StoreEvent<AckFinalizers>>();
    let (ingest_tx_b, ingest_rx_b) = tokio_mpsc::channel::<(Vec<IngestItem>, AckFinalizers)>(16);
    let (control_tx_b, control_rx_b) = tokio_mpsc::channel::<ControlMessage>(16);
    let (output_tx_b, output_rx_b) = futures_mpsc::channel::<LogEvent>(16);

    let handle_b = tokio::spawn(
        test_shard_runtime(
            worker_b,
            store_handle_b,
            control_tx_b.clone(),
            output_tx_b,
            Arc::new(cfg),
            plan,
        )
        .shard_loop(store_events_rx_b, store_thread_b, ingest_rx_b, control_rx_b),
    );

    let (notifier_b1, ack_rx_b1) = AckNotifier::new_with_receiver();
    ingest_tx_b
        .send((
            vec![make_item(100)],
            AckFinalizers::from_single(notifier_b1),
        ))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;

    let (notifier_b2, ack_rx_b2) = AckNotifier::new_with_receiver();
    ingest_tx_b
        .send((
            vec![make_item(101)],
            AckFinalizers::from_single(notifier_b2),
        ))
        .await
        .unwrap();

    // Exercise pending_ingest retry on store_events before breaking out of loop.
    store_events_tx_b
        .send(StoreEvent::KeysDeleted {
            epoch_ms: 0,
            count: 0,
        })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;

    control_tx_b
        .send(ControlMessage::Poll {
            watermark: None,
            now_ms: 2_000,
        })
        .await
        .unwrap();

    drop(output_rx_b);
    store_events_tx_b
        .send(StoreEvent::SpillReplay {
            entries: vec![(99, vec![sample_slice_record(1)])],
            spill_files: 0,
            quarantined: 0,
        })
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(1), handle_b)
        .await
        .expect("shard_loop should exit when output_rx_b is dropped")
        .expect("shard_loop task should not panic");

    assert_eq!(ack_rx_b1.await, Ok(AckStatus::Errored));
    assert_eq!(ack_rx_b2.await, Ok(AckStatus::Errored));
}

#[tokio::test]
async fn pipeline_flushes_micro_batch_at_256_items() {
    let _serial = TEST_SERIAL.lock().await;
    let dir = tempfile::TempDir::new().unwrap();
    let cfg = test_config(dir.path());
    let pipeline = SluicePipeline::new(cfg).unwrap();
    let (tx, rx) = futures_mpsc::unbounded();
    let mut out_stream = Box::new(pipeline).transform_stream(rx);

    let now_nano = chrono::Utc::now().timestamp_millis() * 1_000_000;
    let mut log_records = Vec::with_capacity(260);
    for _ in 0..260 {
        let mut str_val = crate::event::ObjectMap::new();
        str_val.insert("stringValue".into(), Value::from("api"));
        let mut attr = crate::event::ObjectMap::new();
        attr.insert("key".into(), Value::from("service"));
        attr.insert("value".into(), Value::Object(str_val));

        let mut record = crate::event::ObjectMap::new();
        record.insert("timeUnixNano".into(), Value::from(now_nano));
        record.insert("attributes".into(), Value::Array(vec![Value::Object(attr)]));
        log_records.push(Value::Object(record));
    }

    let mut scope_log = crate::event::ObjectMap::new();
    scope_log.insert("logRecords".into(), Value::Array(log_records));

    let mut rl = crate::event::ObjectMap::new();
    rl.insert(
        "scopeLogs".into(),
        Value::Array(vec![Value::Object(scope_log)]),
    );

    let mut envelope = LogEvent::default();
    envelope.insert_path("resourceLogs", Value::Array(vec![Value::Object(rl)]));

    let (envelope_with_ack, ack_rx) = envelope.with_ack_notifier();
    tx.unbounded_send(envelope_with_ack).unwrap();

    let status = tokio::time::timeout(Duration::from_secs(2), ack_rx)
        .await
        .expect("timed out waiting for micro-batch flush ack")
        .expect("ack channel closed");
    assert_eq!(status, AckStatus::Delivered);

    drop(tx);

    let mut total_event_count = 0i64;
    while let Some(mut emitted) = out_stream.next().await {
        if let Some(Value::Array(rls)) = emitted.as_map().get("resourceLogs") {
            for rl_val in rls {
                if let Some(sls) = rl_val.get("scopeLogs").and_then(Value::as_array) {
                    for sl_val in sls {
                        if let Some(lrs) = sl_val.get("logRecords").and_then(Value::as_array) {
                            for lr_val in lrs {
                                if let Some(attrs) =
                                    lr_val.get("attributes").and_then(Value::as_array)
                                {
                                    for attr in attrs {
                                        if attr.get("key").and_then(Value::as_str)
                                            == Some("event_count")
                                            && let Some(cnt) = attr
                                                .get("value")
                                                .and_then(|v| v.get("intValue"))
                                                .and_then(Value::as_i64)
                                        {
                                            total_event_count += cnt;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        emitted
            .take_finalizers()
            .update_status(AckStatus::Delivered);
    }

    assert_eq!(total_event_count, 260);
}

#[tokio::test]
async fn pipeline_failure_emits_one_quarantine_event() {
    let dir = tempfile::TempDir::new().unwrap();
    let blocker = dir.path().join("not-a-directory");
    std::fs::File::create(&blocker).unwrap();

    // `SluicePipeline::new` only validates config. `create_dir_all` on a regular
    // file fails later, inside `run_pipeline`.
    let mut cfg = test_config(dir.path());
    cfg.storage.data_dir = blocker;
    let pipeline = SluicePipeline::new(cfg).unwrap();

    let (tx, rx) = futures_mpsc::unbounded();
    drop(tx);
    let mut out = Box::new(pipeline).transform_stream(rx);

    let event = tokio::time::timeout(Duration::from_secs(2), out.next())
        .await
        .expect("timed out waiting for pipeline failure event")
        .expect("stream ended without an event");
    let route = event
        .get_path("_wa_route")
        .map(crate::event::value_to_string_lossy);
    assert_eq!(route.as_deref(), Some("quarantine"));
    let reason = event
        .get_path("_wa_reason")
        .map(|v| crate::event::value_to_string_lossy(v).into_owned())
        .unwrap_or_default();
    assert!(reason.contains("pipeline_failed"), "reason was {reason}");

    let next = tokio::time::timeout(Duration::from_secs(2), out.next())
        .await
        .expect("timed out waiting for the output stream to end");
    assert!(next.is_none(), "expected exactly one output event");
}

#[tokio::test]
async fn pipeline_failure_nacks_buffered_input() {
    let dir = tempfile::TempDir::new().unwrap();
    let blocker = dir.path().join("not-a-directory");
    std::fs::File::create(&blocker).unwrap();

    let mut cfg = test_config(dir.path());
    cfg.storage.data_dir = blocker;
    let pipeline = SluicePipeline::new(cfg).unwrap();

    let (tx, rx) = futures_mpsc::unbounded();
    let (event, ack_rx) = LogEvent::default().with_ack_notifier();
    tx.unbounded_send(event).unwrap();

    let mut out = Box::new(pipeline).transform_stream(rx);
    let failure = tokio::time::timeout(Duration::from_secs(2), out.next())
        .await
        .expect("timed out waiting for pipeline failure event")
        .expect("stream ended without an event");
    let route = failure
        .get_path("_wa_route")
        .map(crate::event::value_to_string_lossy);
    assert_eq!(route.as_deref(), Some("quarantine"));

    let status = tokio::time::timeout(Duration::from_secs(2), ack_rx)
        .await
        .expect("timed out waiting for the unread event ack")
        .expect("ack channel closed");
    assert_eq!(status, AckStatus::Errored);
    drop(tx);
}

#[tokio::test]
async fn startup_failure_on_one_shard_releases_other_shard_locks() {
    let _serial = TEST_SERIAL.lock().await;
    let dir = tempfile::TempDir::new().unwrap();
    // A regular file where shard_1's directory should go makes its open fail,
    // while shard_0 opens fine and takes its fjall lock.
    let blocker = dir.path().join("shard_1");
    std::fs::File::create(&blocker).unwrap();

    let mut cfg = test_config(dir.path());
    cfg.shards = 2;
    let pipeline = SluicePipeline::new(cfg.clone()).unwrap();
    let (tx, rx) = futures_mpsc::unbounded::<LogEvent>();
    let mut out = Box::new(pipeline).transform_stream(rx);

    let failure = tokio::time::timeout(Duration::from_secs(5), out.next())
        .await
        .expect("timed out waiting for pipeline failure event")
        .expect("stream ended without an event");
    let route = failure
        .get_path("_wa_route")
        .map(crate::event::value_to_string_lossy);
    assert_eq!(route.as_deref(), Some("quarantine"));
    let next = tokio::time::timeout(Duration::from_secs(5), out.next())
        .await
        .expect("timed out waiting for the output stream to end");
    assert!(next.is_none(), "expected exactly one output event");
    drop(tx);

    // The failure event is only sent after every init handle was awaited, so
    // shard_0's store has already been dropped. Reopening both shards must
    // succeed and the pipeline must shut down without a failure event.
    std::fs::remove_file(&blocker).unwrap();
    let pipeline = SluicePipeline::new(cfg).unwrap();
    let (tx, rx) = futures_mpsc::unbounded::<LogEvent>();
    let mut out = Box::new(pipeline).transform_stream(rx);
    drop(tx);
    let mut events = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(ev) = out.next().await {
            events.push(ev);
        }
    })
    .await
    .expect("timed out waiting for the reopened pipeline to shut down");
    assert!(
        events.iter().all(|ev| ev
            .get_path("_wa_route")
            .map(crate::event::value_to_string_lossy)
            .as_deref()
            != Some("quarantine")),
        "reopening shard_0 failed; its lock was not released"
    );
}
