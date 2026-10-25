//! Standalone async Tokio pipeline for windowed aggregate log processing.
//!
//! The shard task used to be a fourteen-argument function, and every
//! `tokio::select!` arm copied the same emit-then-maybe-drain check.
//! `ShardRuntime` holds the worker, store handle, and output channel so
//! those arms share one method. This is a structure change; the shard
//! state machine is unchanged.

use std::future::poll_fn;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

use futures::channel::mpsc as futures_mpsc;
use futures::{SinkExt, Stream, StreamExt};
use logsluice_core::codec::plan::AggregationPlan;
use logsluice_core::error::{AggError, AggResult};
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
use crate::event::{AckFinalizers, AckNotifier, AckStatus, FieldPath, LogEvent, Value};
use crate::metrics;
use crate::render::{render_native, render_otlp};
use crate::router::{Router, RouterOutput, RouterStats};

enum ControlMessage {
    Ack { ticket_id: u64, delivered: bool },
    Poll { watermark: Option<i64>, now_ms: i64 },
    Drain { deadline_ms: i64 },
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
            // The failure is already on the output stream; the spawn has nowhere else to put it.
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

/// Work that belongs between a successful emit and the drain-complete check.
///
/// The store-event arm retries a deferred ingest batch there. The poll arm
/// publishes metrics there. Other arms do neither. Keeping that step inside
/// the helper preserves the old order while every `select!` arm makes one call.
enum AfterEmit {
    RetryPendingIngest,
    ExportMetrics,
    Nothing,
}

/// Per-shard task state that used to be fourteen arguments to `shard_loop`.
///
/// Each `select!` arm copied the same emit-and-maybe-finish-drain check.
/// Those arms now call one method. This is a structure change; the state
/// machine is unchanged.
struct ShardRuntime {
    shard_idx: u16,
    worker: ShardWorker<AckFinalizers>,
    store_handle: StoreHandle<AckFinalizers>,
    control_tx: tokio_mpsc::Sender<ControlMessage>,
    cfg: Arc<SluiceConfig>,
    plan: Arc<AggregationPlan>,
    detected_otlp: Arc<AtomicBool>,
    output: futures_mpsc::Sender<LogEvent>,
    router_stats: Arc<Mutex<RouterStats>>,
}

impl ShardRuntime {
    /// Forwards worker output to the store thread and the output channel.
    ///
    /// Returns `false` when the output channel is closed. Remaining emission
    /// tickets are nacked on the control sender.
    async fn emit(&mut self, out: WorkerOutput<AckFinalizers>) -> bool {
        let mut cmds = out.commands.into_iter();
        while let Some(cmd) = cmds.next() {
            if let Err(unsent) = self.store_handle.try_send(cmd) {
                self.worker.defer_command(unsent);
                self.worker.requeue_commands(cmds);
                break;
            }
        }

        for finalizers in out.release {
            finalizers.update_status(AckStatus::Delivered);
        }

        for finalizers in out.failed_release {
            finalizers.update_status(AckStatus::Errored);
        }

        let use_otlp = match self.cfg.envelope_mode {
            EnvelopeMode::Otlp => true,
            EnvelopeMode::VectorNative => false,
            EnvelopeMode::Auto => self.detected_otlp.load(Ordering::Relaxed),
        };

        let mut batches_iter = out.emit.into_iter();
        while let Some(emit_batch) = batches_iter.next() {
            let (finalizer, receiver) = AckNotifier::new_with_receiver();
            let rendered_logs = if use_otlp {
                render_otlp(
                    &emit_batch.records,
                    &self.cfg,
                    &self.plan,
                    self.cfg.output.max_records_per_envelope,
                )
            } else {
                emit_batch
                    .records
                    .iter()
                    .map(|r| render_native(r, &self.cfg, &self.plan))
                    .collect()
            };

            for mut log in rendered_logs {
                log.merge_finalizers(AckFinalizers::from_single(finalizer.clone()));
                if self.output.send(log).await.is_err() {
                    finalizer.update_status(AckStatus::Errored);
                    drop(finalizer);
                    let _ = self
                        .control_tx
                        .send(ControlMessage::Ack {
                            ticket_id: emit_batch.ticket_id,
                            delivered: false,
                        })
                        .await;
                    for rem in batches_iter {
                        let _ = self
                            .control_tx
                            .send(ControlMessage::Ack {
                                ticket_id: rem.ticket_id,
                                delivered: false,
                            })
                            .await;
                    }
                    return false;
                }
            }
            drop(finalizer);

            let ack_tx_clone = self.control_tx.clone();
            let ticket_id = emit_batch.ticket_id;
            tokio::spawn(async move {
                let delivered = matches!(receiver.await, Ok(AckStatus::Delivered));
                let _ = ack_tx_clone
                    .send(ControlMessage::Ack {
                        ticket_id,
                        delivered,
                    })
                    .await;
            });
        }

        true
    }

    /// Emits `out`, runs the arm-specific step, and reports whether to stop.
    ///
    /// Returns `true` when the output channel closed, or when a drain is in
    /// progress, no ingest batch is waiting, and [`is_drain_complete`] holds.
    async fn emit_then_maybe_finish_drain(
        &mut self,
        out: WorkerOutput<AckFinalizers>,
        draining: bool,
        pending_ingest: &mut Option<(Vec<IngestItem>, AckFinalizers)>,
        drain_deadline: Option<i64>,
        now_ms: i64,
        after_emit: AfterEmit,
    ) -> bool {
        if !self.emit(out).await {
            return true;
        }
        if matches!(after_emit, AfterEmit::RetryPendingIngest) {
            retry_pending_ingest(&mut self.worker, pending_ingest, now_ms);
        }
        if matches!(after_emit, AfterEmit::ExportMetrics) {
            self.export_metrics();
        }
        draining
            && pending_ingest.is_none()
            && is_drain_complete(&self.worker, drain_deadline, now_ms)
    }

    fn export_metrics(&self) {
        let r_stats = self
            .router_stats
            .lock()
            .map_or_else(|_| RouterStats::default(), |guard| *guard);
        let mut w_stats = self.worker.stats();
        w_stats.store_cmd_queue_depth = w_stats
            .store_cmd_queue_depth
            .saturating_add(self.store_handle.queue_depth());
        metrics::export(self.shard_idx, &w_stats, &r_stats);
    }

    /// Runs the shard task until the output channel closes or a drain finishes.
    ///
    /// The three receivers and the store thread stay local to this loop.
    /// Biased select, deferred ingest retry, the drain interval, errored
    /// finalizers on exit, and [`StoreCommand::Shutdown`] are unchanged.
    async fn shard_loop(
        mut self,
        mut store_events: futures_mpsc::UnboundedReceiver<StoreEvent<AckFinalizers>>,
        store_thread: std::thread::JoinHandle<()>,
        mut ingest_rx: tokio_mpsc::Receiver<(Vec<IngestItem>, AckFinalizers)>,
        mut control_rx: tokio_mpsc::Receiver<ControlMessage>,
    ) {
        let rec_out = self.worker.recovery_output();
        let downstream_open = self.emit(rec_out).await;

        let mut draining = false;
        let mut drain_deadline: Option<i64> = None;
        let mut drain_interval = tokio::time::interval(Duration::from_millis(25));
        let mut pending_ingest: Option<(Vec<IngestItem>, AckFinalizers)> = None;
        let mut last_watermark: Option<i64> = None;

        // Downstream already gone during recovery: skip the loop and clean up.
        if downstream_open {
            loop {
                tokio::select! {
                    biased;

                    Some(store_ev) = store_events.next() => {
                        let now_ms = chrono::Utc::now().timestamp_millis();
                        let out = self.worker.on_event(store_ev, now_ms);
                        if self.emit_then_maybe_finish_drain(
                            out,
                            draining,
                            &mut pending_ingest,
                            drain_deadline,
                            now_ms,
                            AfterEmit::RetryPendingIngest,
                        ).await {
                            break;
                        }
                    }

                    Some((items, finalizer)) = ingest_rx.recv(), if pending_ingest.is_none() && self.worker.can_ingest() => {
                        let now_ms = chrono::Utc::now().timestamp_millis();
                        if let Err(batch) = self.worker.try_ingest(items, finalizer, now_ms) {
                            pending_ingest = Some(batch);
                        }
                    }

                    Some(msg) = control_rx.recv() => {
                        let now_ms = chrono::Utc::now().timestamp_millis();
                        match msg {
                            ControlMessage::Ack { ticket_id, delivered } => {
                                let out = self.worker.on_ack(ticket_id, delivered, now_ms);
                                if self.emit_then_maybe_finish_drain(
                                    out,
                                    draining,
                                    &mut pending_ingest,
                                    drain_deadline,
                                    now_ms,
                                    AfterEmit::Nothing,
                                ).await {
                                    break;
                                }
                            }
                            ControlMessage::Poll { watermark, now_ms } => {
                                last_watermark = watermark;
                                let out = self.worker.poll(watermark, now_ms);
                                if self.emit_then_maybe_finish_drain(
                                    out,
                                    draining,
                                    &mut pending_ingest,
                                    drain_deadline,
                                    now_ms,
                                    AfterEmit::ExportMetrics,
                                ).await {
                                    break;
                                }
                            }
                            ControlMessage::Drain { deadline_ms } => {
                                draining = true;
                                drain_deadline = Some(deadline_ms);
                                let out = self.worker.begin_drain(deadline_ms);
                                if self.emit_then_maybe_finish_drain(
                                    out,
                                    draining,
                                    &mut pending_ingest,
                                    drain_deadline,
                                    now_ms,
                                    AfterEmit::Nothing,
                                ).await {
                                    break;
                                }
                            }
                        }
                    }

                    _ = drain_interval.tick(), if draining || self.worker.pending_commands_len() > 0 => {
                        let now_ms = chrono::Utc::now().timestamp_millis();
                        retry_pending_ingest(&mut self.worker, &mut pending_ingest, now_ms);
                        let out = self.worker.poll(last_watermark, now_ms);
                        if self.emit_then_maybe_finish_drain(
                            out,
                            draining,
                            &mut pending_ingest,
                            drain_deadline,
                            now_ms,
                            AfterEmit::Nothing,
                        ).await {
                            break;
                        }
                    }
                }
            }
        }

        if let Some((_, finalizer)) = pending_ingest.take() {
            finalizer.update_status(AckStatus::Errored);
        }
        while let Ok((_, finalizer)) = ingest_rx.try_recv() {
            finalizer.update_status(AckStatus::Errored);
        }
        while control_rx.try_recv().is_ok() {}

        // Cleanly terminate store thread and join
        let _ = self.store_handle.send(StoreCommand::Shutdown);
        let _ = store_thread.join();
    }
}

fn retry_pending_ingest(
    worker: &mut ShardWorker<AckFinalizers>,
    pending_ingest: &mut Option<(Vec<IngestItem>, AckFinalizers)>,
    now_ms: i64,
) {
    if let Some((items, finalizer)) = pending_ingest.take()
        && let Err(batch) = worker.try_ingest(items, finalizer, now_ms)
    {
        *pending_ingest = Some(batch);
    }
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
    ingest_txs: &[tokio_mpsc::Sender<(Vec<IngestItem>, AckFinalizers)>],
) {
    let mb = &mut micro_batches[shard_idx];
    if mb.items.is_empty() {
        return;
    }

    let items = std::mem::take(&mut mb.items);
    let finalizer = std::mem::take(&mut mb.finalizer);
    mb.first_added = None;

    if let Err(tokio_mpsc::error::SendError((_, finalizer))) =
        ingest_txs[shard_idx].send((items, finalizer)).await
    {
        finalizer.update_status(AckStatus::Errored);
    }
}

async fn run_pipeline(
    cfg: SluiceConfig,
    input: Pin<Box<dyn Stream<Item = LogEvent> + Send>>,
    mut output_tx: futures_mpsc::Sender<LogEvent>,
) -> AggResult<()> {
    let route_field = cfg.output.route_field.clone();
    let mut input = input;
    if let Err(err) = run_pipeline_inner(cfg, &mut input, &mut output_tx).await {
        // `AckInner` defaults to Delivered. Events still sitting in the input
        // channel were never routed, and dropping the stream would ack them
        // as delivered. Nack the ones already buffered, then emit the
        // quarantine event. Do not wait for the sender to close.
        nack_buffered_input(&mut input).await;
        emit_pipeline_failure(&route_field, &mut output_tx, &err).await;
        drop(output_tx);
        return Err(err);
    }
    Ok(())
}

/// Marks finalizers on events already queued in `input` as [`AckStatus::Errored`].
///
/// A pending poll stops the walk. Startup failure used to return before the
/// read loop, and the unread `LogEvent`s dropped with the default Delivered ack.
async fn nack_buffered_input(input: &mut Pin<Box<dyn Stream<Item = LogEvent> + Send>>) {
    loop {
        let item = poll_fn(|cx| match input.as_mut().poll_next(cx) {
            Poll::Ready(value) => Poll::Ready(value),
            Poll::Pending => Poll::Ready(None),
        })
        .await;
        let Some(mut event) = item else {
            break;
        };
        event.take_finalizers().update_status(AckStatus::Errored);
    }
}

/// One terminal quarantine event so a storage or config failure stays visible
/// on the output stream instead of ending it with no signal.
async fn emit_pipeline_failure(
    route_field: &str,
    output_tx: &mut futures_mpsc::Sender<LogEvent>,
    err: &AggError,
) {
    let route_name = if route_field.is_empty() {
        "_wa_route"
    } else {
        route_field
    };
    let mut event = LogEvent::default();
    let _ = event.insert(
        &FieldPath::parse_or_literal(route_name),
        Value::from("quarantine"),
    );
    let _ = event.insert(
        &FieldPath::parse_or_literal("_wa_reason"),
        Value::from(format!("pipeline_failed: {err}")),
    );
    // Downstream already gone.
    let _ = output_tx.send(event).await;
}

async fn run_pipeline_inner(
    cfg: SluiceConfig,
    input: &mut Pin<Box<dyn Stream<Item = LogEvent> + Send>>,
    output_tx: &mut futures_mpsc::Sender<LogEvent>,
) -> AggResult<()> {
    let data_dir = cfg.storage.data_dir.clone();
    let shards = cfg.shards;
    let codec_schema = cfg.codec_schema();
    let emitter_id = cfg.output.emitter_id.clone();
    let window_duration_ms = cfg.window.duration.as_millis() as i64;
    let window_offset_ms = cfg.window.offset.as_millis() as i64;

    let pod_meta = tokio::task::spawn_blocking(move || -> AggResult<PodMeta> {
        std::fs::create_dir_all(&data_dir)?;
        PodMeta::load_or_init_with_window(
            &data_dir,
            shards,
            &codec_schema,
            Some(emitter_id.as_str()),
            window_duration_ms,
            window_offset_ms,
        )
    })
    .await
    .map_err(|e| AggError::Io(std::io::Error::other(e.to_string())))??;
    let pod_meta = Arc::new(pod_meta);

    let (engine, plan) = cfg.into_engine_and_plan()?;
    let engine = Arc::new(engine);
    let plan = Arc::new(plan);
    let clock = WindowClock::new(cfg.window.duration, cfg.window.offset)?;
    let kernels = SimdKernelTable::detect(cfg.acceleration.simd_mode);
    let shards_count = usize::from(cfg.shards);

    let mut init_handles = Vec::with_capacity(shards_count);
    for s in 0..cfg.shards {
        let shard_dir = cfg.storage.data_dir.join(format!("shard_{s}"));
        let store_opts = StoreOptions {
            shard_dir,
            cache_bytes: (cfg.storage.cache_size_total / u64::from(cfg.shards)).max(1024 * 1024),
            hdr_memtable_bytes: cfg.storage.hdr_max_memtable_size.max(1024 * 1024),
            tail_memtable_bytes: cfg.storage.tail_max_memtable_size.max(1024 * 1024),
            sync_wal: cfg.storage.sync_wal,
            layout: plan.layout(),
            worker_threads: cfg.storage.worker_threads,
            max_write_buffer_bytes: cfg.storage.max_write_buffer_bytes,
            max_cached_files: cfg.storage.max_cached_files,
        };
        let worker_ctx = WorkerContext {
            cfg: engine.clone(),
            shard: s,
            emitter_id: Arc::from(pod_meta.emitter_id.as_str()),
            kernels,
            plan: plan.clone(),
            clock,
        };
        let pod_meta_clone = pod_meta.clone();

        let handle =
            tokio::task::spawn_blocking(move || -> AggResult<(ShardStore, RecoveredState)> {
                std::fs::create_dir_all(&store_opts.shard_dir)?;
                let store = ShardStore::open(store_opts, &pod_meta_clone)?;
                let recovered = RecoveredState::load(&store, &worker_ctx)?;
                Ok((store, recovered))
            });
        init_handles.push((s, handle));
    }

    let horizons: Vec<PurgeHorizon> = (0..cfg.shards).map(|_| PurgeHorizon::new(0)).collect();

    let mut ingest_txs = Vec::with_capacity(shards_count);
    let mut control_txs = Vec::with_capacity(shards_count);
    let mut prepared_shards = Vec::with_capacity(shards_count);
    let mut all_known_partitions = Vec::new();

    let cfg_arc = Arc::new(cfg.clone());
    let router_stats_shared = Arc::new(Mutex::new(RouterStats::default()));

    // Await every shard open before spawning any store thread. Returning on the
    // first error used to drop the remaining `spawn_blocking` opens and the
    // already-spawned store threads, so the failure event could reach
    // `output_rx` while those threads still held fjall locks.
    let mut opened = Vec::with_capacity(shards_count);
    let mut first_err = None;
    for (s, handle) in init_handles {
        match handle
            .await
            .map_err(|e| AggError::Io(std::io::Error::other(e.to_string())))
            .and_then(|res| res)
        {
            Ok(pair) => opened.push((s, pair)),
            Err(e) => {
                first_err.get_or_insert(e);
            }
        }
    }
    if let Some(e) = first_err {
        return Err(e);
    }

    for (s, (store, recovered)) in opened {
        all_known_partitions.extend(recovered.known_partitions.clone());

        let (store_handle, store_events, store_thread) =
            match StoreThread::spawn::<AckFinalizers>(store, kernels) {
                Ok(spawned) => spawned,
                Err(e) => {
                    // Stop and join the threads spawned for earlier shards.
                    for shard in prepared_shards {
                        let (_, _, handle, _, thread, _, _, _): (
                            _,
                            _,
                            StoreHandle<AckFinalizers>,
                            _,
                            std::thread::JoinHandle<()>,
                            _,
                            _,
                            _,
                        ) = shard;
                        let _ = handle.send(StoreCommand::Shutdown);
                        let _ = thread.join();
                    }
                    return Err(e);
                }
            };
        let worker_ctx = WorkerContext {
            cfg: engine.clone(),
            shard: s,
            emitter_id: Arc::from(pod_meta.emitter_id.as_str()),
            kernels,
            plan: plan.clone(),
            clock,
        };
        let worker = ShardWorker::new(worker_ctx, recovered, horizons[usize::from(s)].clone());

        let (ingest_tx, ingest_rx) = tokio_mpsc::channel::<(Vec<IngestItem>, AckFinalizers)>(1024);
        let (control_tx, control_rx) = tokio_mpsc::channel::<ControlMessage>(1024);
        ingest_txs.push(ingest_tx);
        control_txs.push(control_tx.clone());
        prepared_shards.push((
            s,
            worker,
            store_handle,
            store_events,
            store_thread,
            ingest_rx,
            control_rx,
            control_tx,
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
    for (s, worker, store_handle, store_events, store_thread, ingest_rx, control_rx, control_tx) in
        prepared_shards
    {
        let runtime = ShardRuntime {
            shard_idx: s,
            worker,
            store_handle,
            control_tx,
            cfg: cfg_arc.clone(),
            plan: plan.clone(),
            detected_otlp: detected_otlp.clone(),
            output: output_tx.clone(),
            router_stats: router_stats_shared.clone(),
        };
        let handle =
            tokio::spawn(runtime.shard_loop(store_events, store_thread, ingest_rx, control_rx));
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
                                    flush_micro_batch(s_idx, &mut micro_batches, &ingest_txs).await;
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
                        flush_micro_batch(s_idx, &mut micro_batches, &ingest_txs).await;
                    }
                }
            }

            _ = poll_interval.tick() => {
                if let Ok(mut stats_lock) = router_stats_shared.lock() {
                    *stats_lock = router.stats();
                }
                let now_ms = chrono::Utc::now().timestamp_millis();
                let watermark = router.watermark(now_ms);
                for tx in &control_txs {
                    let _ = tx.send(ControlMessage::Poll { watermark, now_ms }).await;
                }
            }
        }
    }

    // Drop the input receiver before drain, same as when `input` was owned.
    // Replace it with an empty stream so a startup-failure return can still
    // nack whatever was buffered. This line only runs after the read loop ends.
    drop(std::mem::replace(input, Box::pin(futures::stream::empty())));

    // Flush remaining items across all micro-batches
    for s_idx in 0..shards_count {
        flush_micro_batch(s_idx, &mut micro_batches, &ingest_txs).await;
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

    for tx in &control_txs {
        let _ = tx.send(ControlMessage::Drain { deadline_ms }).await;
    }

    // Await completion of all shard tasks
    for handle in shard_handles {
        let _ = handle.await;
    }

    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;
