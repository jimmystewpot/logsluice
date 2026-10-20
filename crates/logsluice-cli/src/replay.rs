//! In-process replay harness driving ShardWorker and StoreThread with crash simulation.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Duration;

use futures_channel::mpsc::UnboundedReceiver;
use hashbrown::HashSet;
use logsluice_core::{
    codec::plan::AggregationPlan,
    config::{AggregationKind, AggregationSpec, EngineConfig},
    error::{AggError, AggResult},
    key::StorageKey24,
    resource::ResourceScopePayload,
    simd::{SimdKernelTable, SimdMode},
    store::{
        meta::PodMeta,
        shard_store::{ShardStore, StoreOptions},
        thread::{StoreCommand, StoreEvent, StoreHandle, StoreThread},
    },
    window::WindowClock,
    worker::{
        EmitBatch, ExemplarSource, IngestItem, PurgeHorizon, RecoveredState, ShardWorker,
        WorkerContext, WorkerOutput,
    },
};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

use crate::generate::EventGenerator;

/// Downstream deduplicating consumer sink tracking unique slices.
pub struct DownstreamSink {
    seen_slices: HashSet<String>,
    duplicates_removed: usize,
    total_counters: u64,
}

impl Default for DownstreamSink {
    fn default() -> Self {
        Self::new()
    }
}

impl DownstreamSink {
    /// Creates a new empty downstream sink.
    #[must_use]
    pub fn new() -> Self {
        Self {
            seen_slices: HashSet::new(),
            duplicates_removed: 0,
            total_counters: 0,
        }
    }

    /// Ingests an emitted delivery batch, recording duplicates and lane 0 counters.
    pub fn handle_batch(&mut self, batch: &EmitBatch) {
        for record in &batch.records {
            if !self.seen_slices.insert(record.slice_id.clone()) {
                self.duplicates_removed += 1;
            } else {
                let count = record.counters.first().map_or(0, |c| c[0]);
                self.total_counters += count;
            }
        }
    }

    /// Number of unique slices seen.
    #[must_use]
    pub fn slices_len(&self) -> usize {
        self.seen_slices.len()
    }

    /// Number of duplicate slices discarded.
    #[must_use]
    pub fn duplicates_removed(&self) -> usize {
        self.duplicates_removed
    }

    /// Accumulated counter lane 0 sum.
    #[must_use]
    pub fn total_counters(&self) -> u64 {
        self.total_counters
    }
}

struct ShardDriver {
    worker: ShardWorker<u32>,
    store_handle: StoreHandle<u32>,
    rx_event: UnboundedReceiver<StoreEvent<u32>>,
    join_handle: Option<std::thread::JoinHandle<()>>,
    horizon: PurgeHorizon,
    clock: WindowClock,
    ctx: WorkerContext,
    meta: PodMeta,
    store_opts: StoreOptions,
    unacked_tickets: Vec<u64>,
}

impl ShardDriver {
    fn open(
        shard_idx: u16,
        cfg: Arc<EngineConfig>,
        plan: Arc<AggregationPlan>,
        meta: PodMeta,
    ) -> AggResult<Self> {
        let shard_dir = cfg.storage.data_dir.join(format!("shard_{shard_idx}"));
        std::fs::create_dir_all(&shard_dir)?;

        let store_opts = StoreOptions {
            shard_dir,
            cache_bytes: 8 << 20,
            hdr_memtable_bytes: 1 << 20,
            tail_memtable_bytes: 1 << 20,
            sync_wal: true,
            layout: plan.layout(),
            worker_threads: cfg.storage.worker_threads,
            max_write_buffer_bytes: cfg.storage.max_write_buffer_bytes,
            max_cached_files: cfg.storage.max_cached_files,
        };

        let store = ShardStore::open(store_opts.clone(), &meta)?;
        let kernels = SimdKernelTable::detect(SimdMode::Scalar);
        let clock = WindowClock::new(cfg.window.duration, cfg.window.offset)?;

        let ctx = WorkerContext {
            cfg: Arc::clone(&cfg),
            shard: shard_idx,
            emitter_id: Arc::from(meta.emitter_id.as_str()),
            kernels,
            plan: Arc::clone(&plan),
            clock,
        };

        let horizon = PurgeHorizon::new(0);
        let recovered = RecoveredState::load(&store, &ctx)?;
        let (store_handle, rx_event, join_handle) = StoreThread::spawn(store, kernels)?;
        let mut worker = ShardWorker::new(ctx.clone(), recovered, horizon.clone());
        let rec_out = worker.recovery_output();

        let mut driver = Self {
            worker,
            store_handle,
            rx_event,
            join_handle: Some(join_handle),
            horizon,
            clock,
            ctx,
            meta,
            store_opts,
            unacked_tickets: Vec::new(),
        };

        let mut dummy_sink = DownstreamSink::new();
        driver.pump(rec_out, &mut dummy_sink, 0)?;

        Ok(driver)
    }

    fn pump(
        &mut self,
        mut out: WorkerOutput<u32>,
        downstream: &mut DownstreamSink,
        now_ms: i64,
    ) -> AggResult<()> {
        loop {
            for batch in out.emit {
                downstream.handle_batch(&batch);
                self.unacked_tickets.push(batch.ticket_id);
            }

            let cmd_count = out.commands.len();
            if cmd_count == 0 {
                break;
            }

            for cmd in out.commands {
                self.store_handle
                    .send(cmd)
                    .map_err(|_| AggError::Channel("failed to send store command".into()))?;
            }

            let mut next_out = WorkerOutput::default();
            for _ in 0..cmd_count {
                let ev = recv(&mut self.rx_event, Duration::from_secs(10))?;
                let sub_out = self.worker.on_event(ev, now_ms);
                next_out.commands.extend(sub_out.commands);
                next_out.emit.extend(sub_out.emit);
                next_out.release.extend(sub_out.release);
            }
            out = next_out;
        }
        Ok(())
    }

    fn maybe_ack_tickets(
        &mut self,
        now_ms: i64,
        downstream: &mut DownstreamSink,
        nack_rate: Option<f64>,
        rng: &mut StdRng,
        keep_unacked: usize,
    ) -> AggResult<()> {
        while self.unacked_tickets.len() > keep_unacked {
            let ticket_id = self.unacked_tickets.remove(0);
            let delivered = match nack_rate {
                Some(rate) if rate > 0.0 => rng.random_range(0.0..1.0) >= rate,
                _ => true,
            };
            let out = self.worker.on_ack(ticket_id, delivered, now_ms);
            self.pump(out, downstream, now_ms)?;
        }
        Ok(())
    }

    fn simulate_crash(&mut self, downstream: &mut DownstreamSink, now_ms: i64) -> AggResult<()> {
        // 1. Flush in-memory L1 to Fjall
        let flush_out = self.worker.flush_l1(now_ms);
        self.pump(flush_out, downstream, now_ms)?;

        // 2. Shut down StoreThread and drop worker
        let _ = self.store_handle.send(StoreCommand::Shutdown);
        if let Some(h) = self.join_handle.take() {
            let _ = h.join();
        }

        // 3. Reopen PodMeta and ShardStore from disk
        self.meta = PodMeta::load_or_init_with_window(
            &self.ctx.cfg.storage.data_dir,
            self.meta.shards,
            &self.ctx.plan.schema_bytes(),
            Some(&self.meta.emitter_id),
            self.meta.window_duration_ms,
            self.meta.window_offset_ms,
        )?;
        let store = ShardStore::open(self.store_opts.clone(), &self.meta)?;
        let recovered = RecoveredState::load(&store, &self.ctx)?;
        let kernels = SimdKernelTable::detect(SimdMode::Scalar);
        let (store_handle, rx_event, join_handle) = StoreThread::spawn(store, kernels)?;
        self.store_handle = store_handle;
        self.rx_event = rx_event;
        self.join_handle = Some(join_handle);

        // 4. Create new ShardWorker with recovered state
        self.worker = ShardWorker::new(self.ctx.clone(), recovered, self.horizon.clone());

        // 5. Run recovery output (which will re-issue rows for all unacked tickets)
        let rec_out = self.worker.recovery_output();
        self.unacked_tickets.clear();
        self.pump(rec_out, downstream, now_ms)?;

        Ok(())
    }

    fn shutdown(&mut self) {
        let _ = self.store_handle.send(StoreCommand::Shutdown);
        if let Some(h) = self.join_handle.take() {
            let _ = h.join();
        }
    }
}

fn recv<T>(rx: &mut UnboundedReceiver<T>, timeout: Duration) -> AggResult<T> {
    let start = std::time::Instant::now();
    loop {
        #[allow(deprecated)]
        match rx.try_next() {
            Ok(Some(item)) => return Ok(item),
            Ok(None) => return Err(AggError::Channel("channel closed unexpectedly".into())),
            Err(_) => {
                if start.elapsed() >= timeout {
                    return Err(AggError::Channel(
                        "timeout waiting for storage event".into(),
                    ));
                }
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }
}

/// Options configuring the replay driver.
#[derive(Clone, Debug)]
pub struct ReplayOptions {
    /// Total number of events to generate.
    pub events: usize,
    /// Cardinality of distinct keys in range `[0..keys)`.
    pub keys: usize,
    /// Random seed for deterministic generation.
    pub seed: u64,
    /// Number of worker shards.
    pub shards: u16,
    /// Configured window duration in milliseconds.
    pub window_duration_ms: u64,
    /// Storage root directory.
    pub data_dir: std::path::PathBuf,
    /// Frequency of crash simulation in events.
    pub crash_every: Option<usize>,
    /// Optional negative acknowledgment rate in `[0.0, 1.0]`.
    pub nack_rate: Option<f64>,
    /// If true, flushes L1 and stops without advancing watermarks past window close.
    pub stop_before_close: bool,
}

/// Executes deterministic log replay across workers.
///
/// # Errors
/// Returns [`AggError`] on configuration or storage errors, or if totals do not match.
pub fn run_replay(opts: ReplayOptions) -> AggResult<()> {
    let mut cfg = EngineConfig::default();
    cfg.output.emitter_id = "pod-0".to_string();
    cfg.storage.data_dir = opts.data_dir.clone();
    cfg.window.duration = Duration::from_millis(opts.window_duration_ms);
    cfg.window.emit.close_jitter_max = Duration::ZERO;
    cfg.aggregations = vec![AggregationSpec {
        name: "event_count".into(),
        kind: AggregationKind::Count,
        field: None,
    }];
    let cfg = Arc::new(cfg);

    let plan = Arc::new(AggregationPlan::new(&cfg.aggregations)?);
    let meta = PodMeta::load_or_init_with_window(
        &opts.data_dir,
        opts.shards,
        &plan.schema_bytes(),
        Some(&cfg.output.emitter_id),
        opts.window_duration_ms as i64,
        0,
    )?;

    let mut drivers = Vec::with_capacity(opts.shards as usize);
    for s in 0..opts.shards {
        drivers.push(ShardDriver::open(
            s,
            Arc::clone(&cfg),
            Arc::clone(&plan),
            meta.clone(),
        )?);
    }

    let default_resource = Arc::new(ResourceScopePayload {
        resource: [(
            "service.name".to_string(),
            serde_json::json!("replay-service"),
        )]
        .into(),
        scope_name: "replay-scope".to_string(),
        scope_version: "1.0.0".to_string(),
        scope_attrs: Default::default(),
    });
    let default_resource_hash = default_resource.canonical_hash();

    let generator = EventGenerator::new(opts.seed, opts.keys, opts.events, 1_000);
    let mut rng = StdRng::seed_from_u64(opts.seed.wrapping_add(1));
    let mut downstream = DownstreamSink::new();
    let mut items_per_shard: Vec<Vec<IngestItem>> =
        (0..opts.shards as usize).map(|_| Vec::new()).collect();
    let mut current_now_ms = 1_000;
    let mut event_count = 0;

    for event in generator {
        event_count += 1;
        current_now_ms = event.event_ms;
        let digest = (event.key_id as u128) + 1;
        let target_shard = (digest as u64 % opts.shards as u64) as usize;
        let epoch_ms = drivers[target_shard].clock.start_of(event.event_ms) as u64;

        let item = IngestItem {
            key: StorageKey24 { epoch_ms, digest },
            resource: Arc::clone(&default_resource),
            resource_hash: default_resource_hash,
            position: None,
            event_ms: event.event_ms,
            counters: vec![[1, 0, 0, 0]],
            minmax_values: vec![event.event_ms, event.event_ms],
            exemplar: ExemplarSource::default(),
        };
        items_per_shard[target_shard].push(item);

        let is_batch_boundary = event_count % 256 == 0 || event_count == opts.events;
        let is_crash_boundary = opts.crash_every.is_some_and(|m| event_count % m == 0);

        if is_batch_boundary || is_crash_boundary {
            for s in 0..opts.shards as usize {
                if !items_per_shard[s].is_empty() {
                    let batch = std::mem::take(&mut items_per_shard[s]);
                    let _ = drivers[s].worker.ingest(batch, 0, current_now_ms);
                    drivers[s].pump(WorkerOutput::default(), &mut downstream, current_now_ms)?;

                    if !opts.stop_before_close {
                        let watermark = Some(current_now_ms.saturating_sub(500));
                        let poll_out = drivers[s].worker.poll(watermark, current_now_ms);
                        drivers[s].pump(poll_out, &mut downstream, current_now_ms)?;
                        drivers[s].maybe_ack_tickets(
                            current_now_ms,
                            &mut downstream,
                            opts.nack_rate,
                            &mut rng,
                            2,
                        )?;
                    }
                }
            }
        }

        if is_crash_boundary {
            for driver in &mut drivers {
                driver.simulate_crash(&mut downstream, current_now_ms)?;
            }
        }
    }

    if opts.stop_before_close {
        for driver in &mut drivers {
            let flush_out = driver.worker.flush_l1(current_now_ms);
            driver.pump(flush_out, &mut downstream, current_now_ms)?;
            driver.shutdown();
        }
        return Ok(());
    }

    // Advance time and watermark to close all windows
    let final_now_ms = current_now_ms + 100_000;
    let final_watermark = Some(final_now_ms);

    for driver in &mut drivers {
        let poll_out = driver.worker.poll(final_watermark, final_now_ms);
        driver.pump(poll_out, &mut downstream, final_now_ms)?;
        driver.maybe_ack_tickets(final_now_ms, &mut downstream, opts.nack_rate, &mut rng, 0)?;
        driver.shutdown();
    }

    let total = downstream.total_counters();
    let totals_match = total == opts.events as u64;
    let slices = downstream.slices_len();
    let duplicates_removed = downstream.duplicates_removed();

    println!(
        "totals_match={totals_match} events={} slices={slices} duplicates_removed={duplicates_removed}",
        opts.events
    );

    if !totals_match {
        return Err(AggError::Config(format!(
            "totals mismatch: expected {}, got {}",
            opts.events, total
        )));
    }

    Ok(())
}
