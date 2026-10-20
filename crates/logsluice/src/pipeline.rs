//! Standalone async Tokio pipeline for windowed aggregate log processing.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures::channel::mpsc as futures_mpsc;
use futures::{SinkExt, Stream, StreamExt};
use logsluice_core::codec::plan::AggregationPlan;
use logsluice_core::error::AggResult;
use logsluice_core::simd::SimdKernelTable;
use logsluice_core::store::meta::PodMeta;
use logsluice_core::store::shard_store::{ShardStore, StoreOptions};
use logsluice_core::store::thread::{StoreCommand, StoreEvent, StoreHandle, StoreThread};
use logsluice_core::watermark::{SourceKey, WatermarkConfig, WatermarkTracker};
use logsluice_core::window::WindowClock;
use logsluice_core::worker::recovery::RecoveredState;
use logsluice_core::worker::{IngestItem, PurgeHorizon, ShardWorker, WorkerContext, WorkerOutput};
use tokio::sync::mpsc as tokio_mpsc;

use crate::config::{EnvelopeMode, SluiceConfig};
use crate::event::{AckFinalizers, AckNotifier, AckStatus, LogEvent};
use crate::metrics;
use crate::render::{render_native, render_otlp};
use crate::router::{Router, RouterOutput, RouterStats};

enum ShardMessage {
    Ingest {
        items: Vec<IngestItem>,
        finalizer: AckFinalizers,
    },
    Ack {
        ticket_id: u64,
        delivered: bool,
    },
    Poll {
        watermark: Option<i64>,
        now_ms: i64,
    },
    Drain {
        deadline_ms: i64,
    },
}

struct ShardMicroBatch {
    items: Vec<IngestItem>,
    finalizer: AckFinalizers,
    first_added: Option<tokio::time::Instant>,
}

impl ShardMicroBatch {
    fn new() -> Self {
        Self {
            items: Vec::with_capacity(256),
            finalizer: AckFinalizers::default(),
            first_added: None,
        }
    }
}

/// Durable windowed aggregate stream pipeline.
#[derive(Clone, Debug)]
pub struct SluicePipeline {
    cfg: SluiceConfig,
}

/// Compatibility type alias for [`SluicePipeline`].
pub type WindowedAggregate = SluicePipeline;

impl SluicePipeline {
    /// Creates a new [`SluicePipeline`] instance after validating engine configuration.
    ///
    /// # Errors
    /// Returns [`AggError`](logsluice_core::error::AggError) if configuration validation or engine compilation fails.
    pub fn new(cfg: SluiceConfig) -> AggResult<Self> {
        let _ = cfg.engine()?;
        Ok(Self { cfg })
    }

    /// Transforms an asynchronous input stream of [`LogEvent`]s into an output stream of summary,
    /// late, and quarantine [`LogEvent`]s.
    pub fn transform_stream<S>(
        self: Box<Self>,
        input_rx: S,
    ) -> Pin<Box<dyn Stream<Item = LogEvent> + Send>>
    where
        S: Stream<Item = LogEvent> + Send + 'static,
    {
        let (output_tx, output_rx) = futures_mpsc::channel::<LogEvent>(4096);
        let cfg = self.cfg;
        let boxed_input: Pin<Box<dyn Stream<Item = LogEvent> + Send>> = Box::pin(input_rx);

        tokio::spawn(async move {
            let _ = run_pipeline(cfg, boxed_input, output_tx).await;
        });

        Box::pin(output_rx)
    }

    /// Convenience wrapper around [`SluicePipeline::transform_stream`] taking `self` by value.
    pub fn transform<S>(self, input_rx: S) -> Pin<Box<dyn Stream<Item = LogEvent> + Send>>
    where
        S: Stream<Item = LogEvent> + Send + 'static,
    {
        Box::new(self).transform_stream(input_rx)
    }
}

#[allow(clippy::too_many_arguments)]
async fn emit_worker_output(
    out: WorkerOutput<AckFinalizers>,
    worker: &mut ShardWorker<AckFinalizers>,
    store_handle: &StoreHandle<AckFinalizers>,
    output_tx: &mut futures_mpsc::Sender<LogEvent>,
    ack_tx: &tokio_mpsc::Sender<ShardMessage>,
    cfg: &SluiceConfig,
    plan: &AggregationPlan,
    detected_otlp: &AtomicBool,
) -> bool {
    let mut cmds = out.commands.into_iter();
    while let Some(cmd) = cmds.next() {
        if let Err(unsent) = store_handle.try_send(cmd) {
            worker.defer_command(unsent);
            worker.requeue_commands(cmds);
            break;
        }
    }

    for finalizers in out.release {
        finalizers.update_status(AckStatus::Delivered);
    }

    for finalizers in out.failed_release {
        finalizers.update_status(AckStatus::Errored);
    }

    let use_otlp = match cfg.envelope_mode {
        EnvelopeMode::Otlp => true,
        EnvelopeMode::VectorNative => false,
        EnvelopeMode::Auto => detected_otlp.load(Ordering::Relaxed),
    };

    let mut batches_iter = out.emit.into_iter();
    while let Some(emit_batch) = batches_iter.next() {
        let (finalizer, receiver) = AckNotifier::new_with_receiver();
        let rendered_logs = if use_otlp {
            render_otlp(
                &emit_batch.records,
                cfg,
                plan,
                cfg.output.max_records_per_envelope,
            )
        } else {
            emit_batch
                .records
                .iter()
                .map(|r| render_native(r, cfg, plan))
                .collect()
        };

        for mut log in rendered_logs {
            log.merge_finalizers(AckFinalizers::from_single(finalizer.clone()));
            if output_tx.send(log).await.is_err() {
                finalizer.update_status(AckStatus::Errored);
                drop(finalizer);
                let _ = ack_tx
                    .send(ShardMessage::Ack {
                        ticket_id: emit_batch.ticket_id,
                        delivered: false,
                    })
                    .await;
                for rem in batches_iter {
                    let _ = ack_tx
                        .send(ShardMessage::Ack {
                            ticket_id: rem.ticket_id,
                            delivered: false,
                        })
                        .await;
                }
                return false;
            }
        }
        drop(finalizer);

        let ack_tx_clone = ack_tx.clone();
        let ticket_id = emit_batch.ticket_id;
        tokio::spawn(async move {
            let delivered = matches!(receiver.await, Ok(AckStatus::Delivered));
            let _ = ack_tx_clone
                .send(ShardMessage::Ack {
                    ticket_id,
                    delivered,
                })
                .await;
        });
    }

    true
}

#[allow(clippy::too_many_arguments)]
async fn shard_loop(
    shard_idx: u16,
    mut worker: ShardWorker<AckFinalizers>,
    store_handle: StoreHandle<AckFinalizers>,
    mut store_events: futures::channel::mpsc::UnboundedReceiver<StoreEvent<AckFinalizers>>,
    store_thread: std::thread::JoinHandle<()>,
    mut shard_rx: tokio_mpsc::Receiver<ShardMessage>,
    shard_tx: tokio_mpsc::Sender<ShardMessage>,
    mut output_tx: futures_mpsc::Sender<LogEvent>,
    cfg: Arc<SluiceConfig>,
    plan: Arc<AggregationPlan>,
    router_stats: Arc<std::sync::Mutex<RouterStats>>,
    detected_otlp: Arc<AtomicBool>,
) {
    // 1. Initial recovery output
    let rec_out = worker.recovery_output();
    if !emit_worker_output(
        rec_out,
        &mut worker,
        &store_handle,
        &mut output_tx,
        &shard_tx,
        &cfg,
        &plan,
        &detected_otlp,
    )
    .await
    {
        while let Ok(msg) = shard_rx.try_recv() {
            if let ShardMessage::Ingest { finalizer, .. } = msg {
                finalizer.update_status(AckStatus::Errored);
            }
        }
        let _ = store_handle.send(StoreCommand::Shutdown);
        let _ = store_thread.join();
        return;
    }

    let mut draining = false;
    let mut drain_deadline: Option<i64> = None;
    let mut drain_interval = tokio::time::interval(Duration::from_millis(25));
    let mut pending_ingest: Option<(Vec<IngestItem>, AckFinalizers)> = None;
    let mut last_watermark: Option<i64> = None;

    loop {
        tokio::select! {
            biased;

            Some(store_ev) = store_events.next() => {
                let now_ms = chrono::Utc::now().timestamp_millis();
                let out = worker.on_event(store_ev, now_ms);
                if !emit_worker_output(out, &mut worker, &store_handle, &mut output_tx, &shard_tx, &cfg, &plan, &detected_otlp).await {
                    break;
                }

                if let Some((items, finalizer)) = pending_ingest.take()
                    && let Err(batch) = worker.try_ingest(items, finalizer, now_ms)
                {
                    pending_ingest = Some(batch);
                }

                if draining && pending_ingest.is_none() && is_drain_complete(&worker, drain_deadline, now_ms) {
                    break;
                }
            }

            Some(msg) = shard_rx.recv(), if pending_ingest.is_none() && worker.can_ingest() => {
                let now_ms = chrono::Utc::now().timestamp_millis();
                match msg {
                    ShardMessage::Ingest { items, finalizer } => {
                        if let Err(batch) = worker.try_ingest(items, finalizer, now_ms) {
                            pending_ingest = Some(batch);
                        }
                    }
                    ShardMessage::Ack { ticket_id, delivered } => {
                        let out = worker.on_ack(ticket_id, delivered, now_ms);
                        if !emit_worker_output(out, &mut worker, &store_handle, &mut output_tx, &shard_tx, &cfg, &plan, &detected_otlp).await {
                            break;
                        }

                        if draining && pending_ingest.is_none() && is_drain_complete(&worker, drain_deadline, now_ms) {
                            break;
                        }
                    }
                    ShardMessage::Poll { watermark, now_ms } => {
                        last_watermark = watermark;
                        let out = worker.poll(watermark, now_ms);
                        if !emit_worker_output(out, &mut worker, &store_handle, &mut output_tx, &shard_tx, &cfg, &plan, &detected_otlp).await {
                            break;
                        }
                        let r_stats = {
                            router_stats.lock().map_or_else(|_| RouterStats::default(), |guard| *guard)
                        };
                        let mut w_stats = worker.stats();
                        w_stats.store_cmd_queue_depth = w_stats
                            .store_cmd_queue_depth
                            .saturating_add(store_handle.queue_depth());
                        metrics::export(shard_idx, &w_stats, &r_stats);

                        if draining && pending_ingest.is_none() && is_drain_complete(&worker, drain_deadline, now_ms) {
                            break;
                        }
                    }
                    ShardMessage::Drain { deadline_ms } => {
                        draining = true;
                        drain_deadline = Some(deadline_ms);
                        let out = worker.begin_drain(deadline_ms);
                        if !emit_worker_output(out, &mut worker, &store_handle, &mut output_tx, &shard_tx, &cfg, &plan, &detected_otlp).await {
                            break;
                        }

                        if pending_ingest.is_none() && is_drain_complete(&worker, Some(deadline_ms), now_ms) {
                            break;
                        }
                    }
                }
            }

            _ = drain_interval.tick(), if draining || worker.pending_commands_len() > 0 => {
                let now_ms = chrono::Utc::now().timestamp_millis();
                if let Some((items, finalizer)) = pending_ingest.take()
                    && let Err(batch) = worker.try_ingest(items, finalizer, now_ms)
                {
                    pending_ingest = Some(batch);
                }
                let out = worker.poll(last_watermark, now_ms);
                if !emit_worker_output(out, &mut worker, &store_handle, &mut output_tx, &shard_tx, &cfg, &plan, &detected_otlp).await {
                    break;
                }

                if draining && pending_ingest.is_none() && is_drain_complete(&worker, drain_deadline, now_ms) {
                    break;
                }
            }
        }
    }

    if let Some((_, finalizer)) = pending_ingest.take() {
        finalizer.update_status(AckStatus::Errored);
    }
    while let Ok(msg) = shard_rx.try_recv() {
        if let ShardMessage::Ingest { finalizer, .. } = msg {
            finalizer.update_status(AckStatus::Errored);
        }
    }

    // Cleanly terminate store thread and join
    let _ = store_handle.send(StoreCommand::Shutdown);
    let _ = store_thread.join();
}

fn is_drain_complete<F>(worker: &ShardWorker<F>, deadline_ms: Option<i64>, now_ms: i64) -> bool {
    if let Some(dl) = deadline_ms
        && now_ms >= dl
    {
        return true;
    }
    worker.is_drain_idle()
}

async fn flush_micro_batch(
    shard_idx: usize,
    micro_batches: &mut [ShardMicroBatch],
    shard_txs: &[tokio_mpsc::Sender<ShardMessage>],
) {
    let mb = &mut micro_batches[shard_idx];
    if mb.items.is_empty() {
        return;
    }

    let items = std::mem::take(&mut mb.items);
    let finalizer = std::mem::take(&mut mb.finalizer);
    mb.first_added = None;

    if let Err(tokio_mpsc::error::SendError(ShardMessage::Ingest { finalizer, .. })) = shard_txs
        [shard_idx]
        .send(ShardMessage::Ingest { items, finalizer })
        .await
    {
        finalizer.update_status(AckStatus::Errored);
    }
}

async fn run_pipeline(
    cfg: SluiceConfig,
    mut input: Pin<Box<dyn Stream<Item = LogEvent> + Send>>,
    mut output_tx: futures_mpsc::Sender<LogEvent>,
) -> AggResult<()> {
    std::fs::create_dir_all(&cfg.storage.data_dir)?;

    let codec_schema = cfg.codec_schema();
    let pod_meta = PodMeta::load_or_init_with_window(
        &cfg.storage.data_dir,
        cfg.shards,
        &codec_schema,
        Some(cfg.output.emitter_id.as_str()),
        cfg.window.duration.as_millis() as i64,
        cfg.window.offset.as_millis() as i64,
    )?;

    let (engine, plan) = cfg.into_engine_and_plan()?;
    let engine = Arc::new(engine);
    let plan = Arc::new(plan);
    let clock = WindowClock::new(cfg.window.duration, cfg.window.offset)?;
    let kernels = SimdKernelTable::detect(cfg.acceleration.simd_mode.into());
    let shards_count = usize::from(cfg.shards);

    let horizons: Vec<PurgeHorizon> = (0..cfg.shards).map(|_| PurgeHorizon::new(0)).collect();

    let mut shard_txs = Vec::with_capacity(shards_count);
    let mut prepared_shards = Vec::with_capacity(shards_count);
    let mut all_known_partitions = Vec::new();

    let cfg_arc = Arc::new(cfg.clone());
    let router_stats_shared = Arc::new(std::sync::Mutex::new(RouterStats::default()));

    for s in 0..cfg.shards {
        let shard_dir = cfg.storage.data_dir.join(format!("shard_{s}"));
        std::fs::create_dir_all(&shard_dir)?;

        let store_opts = StoreOptions {
            shard_dir,
            cache_bytes: (cfg.storage.cache_size_total / u64::from(cfg.shards)).max(1024 * 1024),
            hdr_memtable_bytes: cfg.storage.hdr_max_memtable_size.max(1024 * 1024),
            tail_memtable_bytes: cfg.storage.tail_max_memtable_size.max(1024 * 1024),
            sync_wal: cfg.storage.sync_wal,
            layout: plan.layout(),
            worker_threads: cfg.storage.worker_threads,
        };

        let store = ShardStore::open(store_opts, &pod_meta)?;
        let worker_ctx = WorkerContext {
            cfg: engine.clone(),
            shard: s,
            emitter_id: Arc::from(pod_meta.emitter_id.as_str()),
            kernels,
            plan: plan.clone(),
            clock,
        };

        let recovered = RecoveredState::load(&store, &worker_ctx)?;
        all_known_partitions.extend(recovered.known_partitions.clone());

        let (store_handle, store_events, store_thread) =
            StoreThread::spawn::<AckFinalizers>(store, kernels)?;
        let worker = ShardWorker::new(worker_ctx, recovered, horizons[usize::from(s)].clone());

        let (shard_tx, shard_rx) = tokio_mpsc::channel::<ShardMessage>(1024);
        shard_txs.push(shard_tx.clone());
        prepared_shards.push((
            s,
            worker,
            store_handle,
            store_events,
            store_thread,
            shard_rx,
            shard_tx,
        ));
    }

    // Set up WatermarkTracker and Router
    let watermark_cfg = WatermarkConfig {
        allowed_skew_ms: cfg
            .window
            .allowed_skew
            .as_millis()
            .try_into()
            .unwrap_or(i64::MAX),
        max_future_drift_ms: cfg
            .window
            .max_future_drift
            .as_millis()
            .try_into()
            .unwrap_or(i64::MAX),
        idle_timeout_ms: cfg
            .window
            .idle_timeout
            .as_millis()
            .try_into()
            .unwrap_or(i64::MAX),
        caught_up_threshold_ms: cfg
            .window
            .caught_up_threshold
            .as_millis()
            .try_into()
            .unwrap_or(i64::MAX),
        seed_grace_ms: cfg
            .window
            .max_close_delay
            .as_millis()
            .try_into()
            .unwrap_or(i64::MAX),
        max_tracked_sources: cfg.window.max_tracked_sources,
    };
    let started_ms = chrono::Utc::now().timestamp_millis();
    let mut tracker = WatermarkTracker::new(watermark_cfg, started_ms);
    tracker.seed_known(
        all_known_partitions
            .into_iter()
            .map(|(t, p)| SourceKey::Partition(t, p)),
    );

    let mut router = Router::new(cfg_arc.clone(), kernels, pod_meta.salt, horizons, tracker)?;
    let detected_otlp = router.detected_otlp_handle();

    let mut shard_handles = Vec::with_capacity(shards_count);
    for (s, worker, store_handle, store_events, store_thread, shard_rx, shard_tx) in prepared_shards
    {
        let handle = tokio::spawn(shard_loop(
            s,
            worker,
            store_handle,
            store_events,
            store_thread,
            shard_rx,
            shard_tx,
            output_tx.clone(),
            cfg_arc.clone(),
            plan.clone(),
            router_stats_shared.clone(),
            detected_otlp.clone(),
        ));
        shard_handles.push(handle);
    }

    let mut micro_batches: Vec<ShardMicroBatch> =
        (0..shards_count).map(|_| ShardMicroBatch::new()).collect();

    let mut micro_batch_interval = tokio::time::interval(Duration::from_millis(1));
    let poll_duration = cfg
        .storage
        .l1_flush_interval
        .min(cfg.window.late_emit_interval)
        .min(Duration::from_secs(1));
    let mut poll_interval = tokio::time::interval(poll_duration);

    'pipeline_loop: loop {
        tokio::select! {
            item = input.next() => {
                match item {
                    Some(mut event) => {
                        let now_ms = chrono::Utc::now().timestamp_millis();
                        let finalizers = event.take_finalizers();

                        let mut router_out = RouterOutput::new(shards_count);
                        router.route(event, now_ms, &mut router_out);

                        // Forward passthrough events with original finalizers preserved
                        for mut pt in router_out.passthrough {
                            pt.merge_finalizers(finalizers.clone());
                            if output_tx.send(pt).await.is_err() {
                                finalizers.update_status(AckStatus::Errored);
                                for batch in &mut micro_batches {
                                    if !batch.items.is_empty() {
                                        batch.finalizer.update_status(AckStatus::Errored);
                                        batch.items.clear();
                                    }
                                }
                                break 'pipeline_loop;
                            }
                        }

                        // Buffer items per shard
                        for (s_idx, mut items) in router_out.per_shard.into_iter().enumerate() {
                            if !items.is_empty() {
                                let mb = &mut micro_batches[s_idx];
                                mb.items.append(&mut items);
                                mb.finalizer.merge(finalizers.clone());
                                if mb.first_added.is_none() {
                                    mb.first_added = Some(tokio::time::Instant::now());
                                }
                                if mb.items.len() >= 256 {
                                    flush_micro_batch(s_idx, &mut micro_batches, &shard_txs).await;
                                }
                            }
                        }
                    }
                    None => {
                        // End of input stream
                        break;
                    }
                }
            }

            _ = micro_batch_interval.tick() => {
                for s_idx in 0..shards_count {
                    if let Some(t) = micro_batches[s_idx].first_added
                        && t.elapsed() >= Duration::from_millis(1)
                    {
                        flush_micro_batch(s_idx, &mut micro_batches, &shard_txs).await;
                    }
                }
            }

            _ = poll_interval.tick() => {
                if let Ok(mut stats_lock) = router_stats_shared.lock() {
                    *stats_lock = router.stats();
                }
                let now_ms = chrono::Utc::now().timestamp_millis();
                let watermark = router.watermark(now_ms);
                for tx in &shard_txs {
                    let _ = tx.send(ShardMessage::Poll { watermark, now_ms }).await;
                }
            }
        }
    }

    drop(input);

    // Flush remaining items across all micro-batches
    for s_idx in 0..shards_count {
        flush_micro_batch(s_idx, &mut micro_batches, &shard_txs).await;
    }

    // Begin drain
    let now_ms = chrono::Utc::now().timestamp_millis();
    let drain_timeout_ms = cfg
        .shutdown
        .drain_timeout
        .as_millis()
        .try_into()
        .unwrap_or(50_000);
    let deadline_ms = now_ms.saturating_add(drain_timeout_ms);

    for tx in &shard_txs {
        let _ = tx.send(ShardMessage::Drain { deadline_ms }).await;
    }

    // Await completion of all shard tasks
    for handle in shard_handles {
        let _ = handle.await;
    }

    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::event::Value;
    use logsluice_core::codec::tail::ExemplarTail;
    use logsluice_core::emission::{EmitReason, SliceRecord};
    use logsluice_core::key::StorageKey24;
    use logsluice_core::resource::ResourceScopePayload;
    use logsluice_core::worker::{EmitBatch, ExemplarSource};

    static TEST_SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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
        fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {
        }
        fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}
        fn describe_histogram(
            &self,
            _key: KeyName,
            _unit: Option<Unit>,
            _description: SharedString,
        ) {
        }

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
        let (tx, rx) = futures_mpsc::unbounded();
        let _out = Box::new(pipeline).transform_stream(rx);

        StoreThread::fail_next_flush();

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
        let (tx, rx) = tokio_mpsc::channel(1);
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
        let shard_txs = vec![tx];

        flush_micro_batch(0, &mut batches, &shard_txs).await;

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
        let kernels = SimdKernelTable::detect(cfg.acceleration.simd_mode.into());

        let store_opts = StoreOptions {
            shard_dir,
            cache_bytes: 1024 * 1024,
            hdr_memtable_bytes: 1024 * 1024,
            tail_memtable_bytes: 1024 * 1024,
            sync_wal: false,
            layout: plan.layout(),
            worker_threads: 1,
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

        let (shard_tx, shard_rx) = tokio_mpsc::channel::<ShardMessage>(64);
        let (output_tx, _output_rx) = futures_mpsc::channel::<LogEvent>(64);
        let router_stats = Arc::new(std::sync::Mutex::new(RouterStats::default()));
        let detected_otlp = Arc::new(AtomicBool::new(false));

        let shard_handle = tokio::spawn(shard_loop(
            0,
            worker,
            store_handle.clone(),
            store_events,
            store_thread,
            shard_rx,
            shard_tx.clone(),
            output_tx,
            Arc::new(cfg),
            plan,
            router_stats,
            detected_otlp,
        ));

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

        shard_tx
            .send(ShardMessage::Ingest {
                items: vec![item],
                finalizer: AckFinalizers::from_single(ack_notifier),
            })
            .await
            .unwrap();
        shard_tx
            .send(ShardMessage::Poll {
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

        shard_tx
            .send(ShardMessage::Poll {
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

        shard_tx
            .send(ShardMessage::Drain {
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
            },
            &pod_meta,
        )
        .expect("open shard store");
        let kernels = SimdKernelTable::detect(cfg.acceleration.simd_mode.into());
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
        let mut worker = ShardWorker::new(worker_ctx, recovered, PurgeHorizon::new(0));

        let (mut output_tx, output_rx) = futures_mpsc::channel::<LogEvent>(16);
        drop(output_rx);

        let (ack_tx, mut ack_rx) = tokio_mpsc::channel::<ShardMessage>(16);
        let detected_otlp = AtomicBool::new(false);
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

        let output_open = emit_worker_output(
            out,
            &mut worker,
            &store_handle,
            &mut output_tx,
            &ack_tx,
            &cfg,
            &plan,
            &detected_otlp,
        )
        .await;
        assert!(!output_open);

        let msg1 = ack_rx
            .try_recv()
            .expect("first batch should be nacked immediately");
        assert!(matches!(
            msg1,
            ShardMessage::Ack {
                ticket_id: 10,
                delivered: false
            }
        ));
        let msg2 = ack_rx
            .try_recv()
            .expect("remaining batch should be nacked immediately");
        assert!(matches!(
            msg2,
            ShardMessage::Ack {
                ticket_id: 20,
                delivered: false
            }
        ));

        // Now verify `shard_loop` terminates when `emit_worker_output` fails on a closed `output_tx`.
        let (shard_tx, shard_rx) = tokio_mpsc::channel::<ShardMessage>(16);
        let loop_handle = tokio::spawn(shard_loop(
            0,
            worker,
            store_handle,
            store_events,
            store_thread,
            shard_rx,
            shard_tx.clone(),
            output_tx,
            Arc::new(cfg),
            Arc::new(plan),
            Arc::new(std::sync::Mutex::new(RouterStats::default())),
            Arc::new(AtomicBool::new(false)),
        ));

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
        shard_tx
            .send(ShardMessage::Ingest {
                items: vec![item],
                finalizer: AckFinalizers::from_single(ingest_finalizer),
            })
            .await
            .expect("send ingest");
        shard_tx
            .send(ShardMessage::Poll {
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
        shard_tx
            .send(ShardMessage::Poll {
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
        let kernels = SimdKernelTable::detect(cfg.acceleration.simd_mode.into());

        let store_opts = StoreOptions {
            shard_dir,
            cache_bytes: 1024 * 1024,
            hdr_memtable_bytes: 1024 * 1024,
            tail_memtable_bytes: 1024 * 1024,
            sync_wal: false,
            layout: plan.layout(),
            worker_threads: 1,
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

        let (shard_tx, shard_rx) = tokio_mpsc::channel::<ShardMessage>(64);
        let (output_tx, _output_rx) = futures_mpsc::channel::<LogEvent>(64);
        let router_stats = Arc::new(std::sync::Mutex::new(RouterStats::default()));
        let detected_otlp = Arc::new(AtomicBool::new(false));

        let shard_handle = tokio::spawn(shard_loop(
            0,
            worker,
            store_handle.clone(),
            store_events,
            store_thread,
            shard_rx,
            shard_tx.clone(),
            output_tx,
            Arc::new(cfg),
            plan,
            router_stats,
            detected_otlp,
        ));

        let now_wall = chrono::Utc::now().timestamp_millis();
        let event_ms = now_wall - 20_000;
        let window_end = clock.end_of(clock.start_of(event_ms));

        // 1. Establish watermark >= window_end before the window is created
        shard_tx
            .send(ShardMessage::Poll {
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

        shard_tx
            .send(ShardMessage::Ingest {
                items: vec![item],
                finalizer: AckFinalizers::from_single(ack_notifier),
            })
            .await
            .expect("send ingest");

        // 3. Wait for drain_interval.tick() (25ms) to fire and execute worker.poll(...)
        tokio::time::sleep(Duration::from_millis(80)).await;

        // 4. Send Poll to trigger metrics export
        shard_tx
            .send(ShardMessage::Poll {
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
            if start.elapsed() > Duration::from_secs(2) {
                panic!(
                    "timed out waiting for shard_loop to process second Poll and export metrics"
                );
            }
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
        shard_tx
            .send(ShardMessage::Drain { deadline_ms: 0 })
            .await
            .expect("send drain");
        shard_handle.await.expect("join shard_handle");
    }
}
