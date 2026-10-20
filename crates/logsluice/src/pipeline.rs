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

async fn emit_worker_output(
    out: WorkerOutput<AckFinalizers>,
    store_handle: &StoreHandle<AckFinalizers>,
    output_tx: &mut futures_mpsc::Sender<LogEvent>,
    ack_tx: &tokio_mpsc::Sender<ShardMessage>,
    cfg: &SluiceConfig,
    plan: &AggregationPlan,
    detected_otlp: &AtomicBool,
) {
    for cmd in out.commands {
        let _ = store_handle.send(cmd);
    }

    let use_otlp = match cfg.envelope_mode {
        EnvelopeMode::Otlp => true,
        EnvelopeMode::VectorNative => false,
        EnvelopeMode::Auto => detected_otlp.load(Ordering::Relaxed),
    };

    for emit_batch in out.emit {
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
            let _ = output_tx.send(log).await;
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

    for finalizers in out.release {
        finalizers.update_status(AckStatus::Delivered);
    }
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
    emit_worker_output(
        rec_out,
        &store_handle,
        &mut output_tx,
        &shard_tx,
        &cfg,
        &plan,
        &detected_otlp,
    )
    .await;

    let mut draining = false;
    let mut drain_deadline: Option<i64> = None;
    let mut drain_interval = tokio::time::interval(Duration::from_millis(25));
    let mut pending_ingest: Option<(Vec<IngestItem>, AckFinalizers)> = None;

    loop {
        tokio::select! {
            biased;

            Some(store_ev) = store_events.next() => {
                let now_ms = chrono::Utc::now().timestamp_millis();
                let is_failed = matches!(store_ev, StoreEvent::Failed(_));
                let out = worker.on_event(store_ev, now_ms);
                if is_failed {
                    for f in &out.release {
                        f.update_status(AckStatus::Errored);
                    }
                }
                emit_worker_output(out, &store_handle, &mut output_tx, &shard_tx, &cfg, &plan, &detected_otlp).await;

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
                        emit_worker_output(out, &store_handle, &mut output_tx, &shard_tx, &cfg, &plan, &detected_otlp).await;

                        if draining && pending_ingest.is_none() && is_drain_complete(&worker, drain_deadline, now_ms) {
                            break;
                        }
                    }
                    ShardMessage::Poll { watermark, now_ms } => {
                        let out = worker.poll(watermark, now_ms);
                        emit_worker_output(out, &store_handle, &mut output_tx, &shard_tx, &cfg, &plan, &detected_otlp).await;
                        let r_stats = {
                            router_stats.lock().map_or_else(|_| RouterStats::default(), |guard| *guard)
                        };
                        metrics::export(shard_idx, &worker.stats(), &r_stats);

                        if draining && pending_ingest.is_none() && is_drain_complete(&worker, drain_deadline, now_ms) {
                            break;
                        }
                    }
                    ShardMessage::Drain { deadline_ms } => {
                        draining = true;
                        drain_deadline = Some(deadline_ms);
                        let out = worker.begin_drain(deadline_ms);
                        emit_worker_output(out, &store_handle, &mut output_tx, &shard_tx, &cfg, &plan, &detected_otlp).await;

                        if pending_ingest.is_none() && is_drain_complete(&worker, Some(deadline_ms), now_ms) {
                            break;
                        }
                    }
                }
            }

            _ = drain_interval.tick(), if draining => {
                let now_ms = chrono::Utc::now().timestamp_millis();
                if let Some((items, finalizer)) = pending_ingest.take()
                    && let Err(batch) = worker.try_ingest(items, finalizer, now_ms)
                {
                    pending_ingest = Some(batch);
                }
                let out = worker.poll(None, now_ms);
                emit_worker_output(out, &store_handle, &mut output_tx, &shard_tx, &cfg, &plan, &detected_otlp).await;

                if pending_ingest.is_none() && is_drain_complete(&worker, drain_deadline, now_ms) {
                    break;
                }
            }
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

    let _ = shard_txs[shard_idx]
        .send(ShardMessage::Ingest { items, finalizer })
        .await;
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

    loop {
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
                            let _ = output_tx.send(pt).await;
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
