#![allow(dead_code)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_channel::mpsc::UnboundedReceiver;
use logsluice_core::{
    codec::{
        plan::{AggregationPlan, HeaderLayout},
        tail::ExemplarTail,
    },
    config::{AggregationKind, AggregationSpec, EngineConfig},
    emission::SliceRecord,
    key::StorageKey24,
    position::SourcePosition,
    resource::ResourceScopePayload,
    simd::{SimdKernelTable, SimdMode},
    store::{
        meta::PodMeta,
        shard_store::{FlushBatch, FlushEntry, ShardStore, StoreOptions},
        thread::{StoreCommand, StoreEvent, StoreHandle, StoreThread},
    },
    window::WindowClock,
    worker::{
        ExemplarSource, IngestItem, IngestReport, PurgeHorizon, RecoveredState, ShardWorker,
        WorkerContext, WorkerOutput, WorkerStats,
    },
};
use tempfile::TempDir;

pub const TEST_LAYOUT: HeaderLayout = HeaderLayout {
    banks: 1,
    minmax_slots: 2,
};

pub fn store_with_layout() -> (ShardStore, SimdKernelTable, TempDir) {
    let d = tempfile::tempdir().expect("failed to create temp dir");
    let meta =
        PodMeta::load_or_init(d.path(), "pod-0", 1, b"schema").expect("failed to init pod meta");
    let k = SimdKernelTable::detect(SimdMode::Scalar);
    let opts = StoreOptions {
        shard_dir: d.path().join("shard_0"),
        cache_bytes: 8 << 20,
        hdr_memtable_bytes: 1 << 20,
        tail_memtable_bytes: 1 << 20,
        sync_wal: true,
        layout: TEST_LAYOUT,
        worker_threads: 2,
    };
    let store = ShardStore::open(opts, &meta).expect("failed to open shard store");
    (store, k, d)
}

pub fn store_with_keys(keys: &[u128]) -> (ShardStore, SimdKernelTable, TempDir) {
    let (mut store, k, d) = store_with_layout();
    let entries: Vec<FlushEntry> = keys
        .iter()
        .map(|&digest| FlushEntry {
            key: StorageKey24 {
                epoch_ms: 0,
                digest,
            },
            is_new: true,
            resource_scope_id: 1,
            delta: vec![[1, 0, 0, 0]],
            minmax: vec![(5, 5), (5, 5)],
            last_update_s: 1,
            tail: Some(tail()),
        })
        .collect();

    let batch = FlushBatch {
        entries,
        offsets: vec![(("topic".into(), 0), 1)],
        new_resources: vec![],
        resource_refs: vec![],
    };

    store
        .apply_flush(batch, &k)
        .expect("failed to seed shard store");
    (store, k, d)
}

pub fn tail() -> ExemplarTail {
    ExemplarTail {
        severity: 9,
        body: Some(bytes::Bytes::from_static(b"example-log-body")),
        dims: vec![],
        attrs: vec![],
    }
}

pub fn recv<T>(rx: &mut UnboundedReceiver<T>, timeout: Duration) -> T {
    let start = Instant::now();
    loop {
        #[allow(deprecated)]
        match rx.try_next() {
            Ok(Some(item)) => return item,
            Ok(None) => panic!("receiver channel closed unexpectedly"),
            Err(_) => {
                if start.elapsed() >= timeout {
                    panic!("timeout waiting for message on receiver channel");
                }
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }
}

pub fn spec_example() -> Vec<AggregationSpec> {
    vec![
        AggregationSpec {
            name: "event_count".into(),
            kind: AggregationKind::Count,
            field: None,
        },
        AggregationSpec {
            name: "total_bytes".into(),
            kind: AggregationKind::Sum,
            field: Some("network.bytes".into()),
        },
        AggregationSpec {
            name: "total_packets".into(),
            kind: AggregationKind::Sum,
            field: Some("network.packets".into()),
        },
        AggregationSpec {
            name: "first_seen".into(),
            kind: AggregationKind::Min,
            field: Some("@timestamp".into()),
        },
        AggregationSpec {
            name: "last_seen".into(),
            kind: AggregationKind::Max,
            field: Some("@timestamp".into()),
        },
    ]
}

/// Integrated test harness managing a ShardWorker, StoreThread, and timing simulation.
pub struct Harness {
    _dir: Option<TempDir>,
    worker: ShardWorker<u32>,
    store_handle: StoreHandle<u32>,
    rx_event: UnboundedReceiver<StoreEvent<u32>>,
    join_handle: Option<std::thread::JoinHandle<()>>,
    horizon: PurgeHorizon,
    clock: WindowClock,
    default_resource: Arc<ResourceScopePayload>,
    default_resource_hash: u64,
    now_ms: i64,
    emitted: Vec<SliceRecord>,
    all_emitted: Vec<SliceRecord>,
    unresolved_tickets: Vec<u64>,
    released: Vec<u32>,
    store_barrier: Option<Arc<std::sync::Barrier>>,
    _cfg: Arc<EngineConfig>,
}

impl Drop for Harness {
    fn drop(&mut self) {
        if let Some(barrier) = self.store_barrier.take() {
            barrier.wait();
        }
        let _ = self.store_handle.send(StoreCommand::Shutdown);
        if let Some(h) = self.join_handle.take() {
            let _ = h.join();
        }
    }
}

impl Harness {
    /// Creates a new `Harness` with customized `EngineConfig`.
    pub fn new<F: FnOnce(&mut EngineConfig)>(modify_cfg: F) -> Self {
        let d = tempfile::tempdir().expect("failed to create temp dir");

        let mut cfg = EngineConfig::default();
        cfg.output.emitter_id = "pod-0".to_string();
        cfg.storage.data_dir = d.path().to_path_buf();
        cfg.aggregations = spec_example();
        modify_cfg(&mut cfg);
        let cfg = Arc::new(cfg);

        let plan = Arc::new(AggregationPlan::new(&cfg.aggregations).expect("failed to build plan"));
        let meta = PodMeta::load_or_init(d.path(), "pod-0", 1, &plan.schema_bytes())
            .expect("failed to init pod meta");
        let kernels = SimdKernelTable::detect(SimdMode::Scalar);
        let opts = StoreOptions {
            shard_dir: d.path().join("shard_0"),
            cache_bytes: 8 << 20,
            hdr_memtable_bytes: 1 << 20,
            tail_memtable_bytes: 1 << 20,
            sync_wal: true,
            layout: plan.layout(),
            worker_threads: cfg.storage.worker_threads,
        };
        let store = ShardStore::open(opts, &meta).expect("failed to open shard store");
        let (store_handle, rx_event, join_handle) =
            StoreThread::spawn(store, kernels).expect("failed to spawn store thread");

        let clock =
            WindowClock::new(cfg.window.duration, cfg.window.offset).expect("valid window clock");

        let ctx = WorkerContext {
            cfg: Arc::clone(&cfg),
            shard: 0,
            emitter_id: Arc::from("pod-0"),
            kernels,
            plan,
            clock,
        };

        let horizon = PurgeHorizon::new(0);
        let worker = ShardWorker::new(ctx, RecoveredState::empty(), horizon.clone());

        let default_resource = Arc::new(ResourceScopePayload {
            resource: [(
                "service.name".to_string(),
                serde_json::json!("test-service"),
            )]
            .into(),
            scope_name: "test-scope".to_string(),
            scope_version: "1.0.0".to_string(),
            scope_attrs: Default::default(),
        });
        let default_resource_hash = default_resource.canonical_hash();

        Self {
            _dir: Some(d),
            worker,
            store_handle,
            rx_event,
            join_handle: Some(join_handle),
            horizon,
            clock,
            default_resource,
            default_resource_hash,
            now_ms: 0,
            emitted: Vec::new(),
            all_emitted: Vec::new(),
            unresolved_tickets: Vec::new(),
            released: Vec::new(),
            store_barrier: None,
            _cfg: cfg,
        }
    }

    /// Ingests `(key, event_ms)` pairs with default resource and finalizer 0.
    pub fn ingest(&mut self, items: &[(u128, i64)]) -> IngestReport {
        self.ingest_with_finalizer(items, 0)
    }

    /// Ingests `(key, event_ms)` pairs with a specific finalizer token.
    pub fn ingest_with_finalizer(&mut self, items: &[(u128, i64)], finalizer: u32) -> IngestReport {
        let ingest_items: Vec<IngestItem> = items
            .iter()
            .map(|&(digest, event_ms)| IngestItem {
                key: StorageKey24 {
                    epoch_ms: self.clock.start_of(event_ms) as u64,
                    digest,
                },
                resource: Arc::clone(&self.default_resource),
                resource_hash: self.default_resource_hash,
                position: None,
                event_ms,
                counters: vec![[1, 0, 0, 0]],
                minmax_values: vec![event_ms, event_ms],
                exemplar: ExemplarSource::default(),
            })
            .collect();
        self.worker.ingest(ingest_items, finalizer, self.now_ms)
    }

    /// Ingests `(key, event_ms, offset)` pairs with topic "t", partition 0.
    pub fn ingest_at_offsets(&mut self, items: &[(u128, i64, i64)]) -> IngestReport {
        let ingest_items: Vec<IngestItem> = items
            .iter()
            .map(|&(digest, event_ms, offset)| IngestItem {
                key: StorageKey24 {
                    epoch_ms: self.clock.start_of(event_ms) as u64,
                    digest,
                },
                resource: Arc::clone(&self.default_resource),
                resource_hash: self.default_resource_hash,
                position: Some(SourcePosition {
                    topic: Arc::from("t"),
                    partition: 0,
                    offset,
                }),
                event_ms,
                counters: vec![[1, 0, 0, 0]],
                minmax_values: vec![event_ms, event_ms],
                exemplar: ExemplarSource::default(),
            })
            .collect();
        self.worker.ingest(ingest_items, 0, self.now_ms)
    }

    /// Advances simulated clock and watermark, pumping all resulting commands and events synchronously.
    pub fn advance(&mut self, now_ms: i64, watermark: Option<i64>) {
        self.now_ms = now_ms;
        let out = self.worker.poll(watermark, now_ms);
        self.pump(out);
    }

    /// Synchronously pumps storage commands to the StoreThread and dispatches resulting events.
    pub fn pump(&mut self, mut out: WorkerOutput<u32>) {
        loop {
            for batch in out.emit {
                self.emitted.extend(batch.records.clone());
                self.all_emitted.extend(batch.records);
                self.unresolved_tickets.push(batch.ticket_id);
            }
            self.released.extend(out.release);

            let cmd_count = out.commands.len();
            if cmd_count == 0 {
                break;
            }

            for cmd in out.commands {
                self.store_handle.send(cmd).expect("send store command");
            }

            let mut next_out = WorkerOutput::default();
            for _ in 0..cmd_count {
                let ev = recv(&mut self.rx_event, Duration::from_secs(5));
                let sub_out = self.worker.on_event(ev, self.now_ms);
                next_out.commands.extend(sub_out.commands);
                next_out.emit.extend(sub_out.emit);
                next_out.release.extend(sub_out.release);
            }
            out = next_out;
        }
    }

    /// Returns and clears records emitted since the last call to `drain_emitted`.
    pub fn drain_emitted(&mut self) -> Vec<SliceRecord> {
        std::mem::take(&mut self.emitted)
    }

    /// Returns all records ever emitted by this harness.
    pub fn all_emitted(&self) -> Vec<SliceRecord> {
        self.all_emitted.clone()
    }

    /// Acknowledges all unresolved delivery batches as successfully delivered.
    pub fn ack_all(&mut self) {
        let tickets = std::mem::take(&mut self.unresolved_tickets);
        for id in tickets {
            let out = self.worker.on_ack(id, true, self.now_ms);
            self.pump(out);
        }
    }

    /// Acknowledges all unresolved delivery batches as failed delivery.
    pub fn nack_all(&mut self) {
        let tickets = std::mem::take(&mut self.unresolved_tickets);
        for id in tickets {
            let out = self.worker.on_ack(id, false, self.now_ms);
            self.pump(out);
        }
    }

    /// Returns and clears finalizers released since the last call to `released`.
    pub fn released(&mut self) -> Vec<u32> {
        std::mem::take(&mut self.released)
    }

    /// Sums counter lane 0 (`event_count`) across records matching `key`.
    pub fn count_for(&self, records: &[SliceRecord], key: u128) -> u64 {
        records
            .iter()
            .filter(|r| r.digest == key)
            .map(|r| r.counters.first().map_or(0, |c| c[0]))
            .sum()
    }

    /// Sums counter lane 0 (`event_count`) across all emitted records.
    pub fn total_count(&self, records: &[SliceRecord]) -> u64 {
        records
            .iter()
            .map(|r| r.counters.first().map_or(0, |c| c[0]))
            .sum()
    }

    /// Sums counter lane 0 (`event_count`) across overflow records.
    pub fn overflow_count(&self, records: &[SliceRecord]) -> u64 {
        records
            .iter()
            .filter(|r| (r.digest & (!0u128 << 32)) == (!0u128 << 32))
            .map(|r| r.counters.first().map_or(0, |c| c[0]))
            .sum()
    }

    /// Returns sorted list of currently active or tracked window epochs.
    pub fn live_windows(&self) -> Vec<u64> {
        self.worker.live_windows()
    }

    /// Returns the current purge horizon timestamp in epoch milliseconds.
    pub fn horizon(&self) -> i64 {
        self.horizon.get()
    }

    /// Takes a snapshot of current worker statistics.
    pub fn stats(&self) -> WorkerStats {
        self.worker.stats()
    }

    /// Returns the number of disk spill files created.
    pub fn spill_files(&self) -> usize {
        self.worker.spill_files()
    }

    /// Returns a mutable reference to the underlying worker.
    pub fn worker(&mut self) -> &mut ShardWorker<u32> {
        &mut self.worker
    }

    /// Pauses the background storage thread at a synchronization barrier.
    pub fn hold_store(&mut self) {
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        self.store_barrier = Some(barrier.clone());
        self.store_handle
            .send(StoreCommand::TestPause(barrier.clone()))
            .expect("failed to send TestPause command");
        barrier.wait();
    }

    /// Resumes the background storage thread previously paused by `hold_store`.
    pub fn release_store(&mut self) {
        if let Some(barrier) = self.store_barrier.take() {
            barrier.wait();
        }
    }

    /// Arms a simulated flush failure on the next flush processed by the store thread.
    pub fn fail_next_flush(&mut self) {
        self.store_handle.fail_next_flush();
    }

    /// Injects a simulated failure on the next `PersistTickets` command processed by the store thread.
    pub fn fail_next_persist(&mut self) {
        self.store_handle
            .send(StoreCommand::FailNextPersist)
            .expect("failed to send FailNextPersist command");
    }

    /// Returns the path to the temporary data directory.
    pub fn data_dir(&self) -> &std::path::Path {
        self._dir.as_ref().expect("tempdir").path()
    }

    /// Injects a simulated failure on the next `Rows` command processed by the store thread.
    pub fn fail_next_rows(&mut self) {
        self.store_handle
            .send(StoreCommand::FailNextRows)
            .expect("failed to send FailNextRows command");
    }

    /// Injects a simulated failure on the next `Spill` command processed by the store thread.
    pub fn fail_next_spill(&mut self) {
        self.store_handle
            .send(StoreCommand::FailNextSpill)
            .expect("failed to send FailNextSpill command");
    }

    /// Advances time by `timeout_ms` and executes a clean shutdown drain.
    pub fn drain(&mut self, timeout_ms: i64) {
        self.now_ms += timeout_ms;
        let out = self.worker.begin_drain(self.now_ms);
        self.pump(out);
    }

    /// Restarts the harness without calling `recovery_output()`.
    pub fn restart_unpumped(self) -> Self {
        self.restart_unpumped_with(|_| {})
    }

    /// Restarts the harness without calling `recovery_output()`, invoking `mutate_store` before loading `RecoveredState`.
    pub fn restart_unpumped_with<M>(mut self, mutate_store: M) -> Self
    where
        M: FnOnce(&mut ShardStore),
    {
        if let Some(barrier) = self.store_barrier.take() {
            barrier.wait();
        }
        let _ = self.store_handle.send(StoreCommand::Shutdown);
        if let Some(h) = self.join_handle.take() {
            let _ = h.join();
        }

        let dir = self._dir.take().expect("tempdir");
        let plan =
            Arc::new(AggregationPlan::new(&self._cfg.aggregations).expect("failed to build plan"));
        let meta = PodMeta::load_or_init(
            dir.path(),
            &self._cfg.output.emitter_id,
            1,
            &plan.schema_bytes(),
        )
        .expect("failed to init pod meta");
        let kernels = SimdKernelTable::detect(SimdMode::Scalar);
        let opts = StoreOptions {
            shard_dir: dir.path().join("shard_0"),
            cache_bytes: 8 << 20,
            hdr_memtable_bytes: 1 << 20,
            tail_memtable_bytes: 1 << 20,
            sync_wal: true,
            layout: plan.layout(),
            worker_threads: self._cfg.storage.worker_threads,
        };
        let mut store = ShardStore::open(opts, &meta).expect("failed to reopen shard store");
        mutate_store(&mut store);

        let ctx = WorkerContext {
            cfg: Arc::clone(&self._cfg),
            shard: 0,
            emitter_id: Arc::from(self._cfg.output.emitter_id.as_str()),
            kernels,
            plan,
            clock: self.clock,
        };
        let recovered = RecoveredState::load(&store, &ctx).expect("failed to load recovered state");

        let (store_handle, rx_event, join_handle) =
            StoreThread::spawn(store, kernels).expect("failed to spawn store thread");

        let worker = ShardWorker::new(ctx, recovered, self.horizon.clone());

        Self {
            _dir: Some(dir),
            worker,
            store_handle,
            rx_event,
            join_handle: Some(join_handle),
            horizon: self.horizon.clone(),
            clock: self.clock,
            default_resource: self.default_resource.clone(),
            default_resource_hash: self.default_resource_hash,
            now_ms: self.now_ms,
            emitted: Vec::new(),
            all_emitted: Vec::new(),
            unresolved_tickets: Vec::new(),
            released: Vec::new(),
            store_barrier: None,
            _cfg: self._cfg.clone(),
        }
    }

    /// Restarts the harness: shuts down worker and store thread, closes DB,
    /// reopens DB, loads RecoveredState, creates new worker, and runs recovery output.
    pub fn restart(self) -> Self {
        self.restart_with(|_| {}, |_| {})
    }

    /// Restarts the harness with hooks to mutate the `ShardStore` before loading `RecoveredState`
    /// and to configure the harness before pumping `recovery_output()`.
    pub fn restart_with<M, B>(self, mutate_store: M, before_recovery_pump: B) -> Self
    where
        M: FnOnce(&mut ShardStore),
        B: FnOnce(&mut Self),
    {
        let mut new_h = self.restart_unpumped_with(mutate_store);
        let recovery_out = new_h.worker.recovery_output();
        before_recovery_pump(&mut new_h);
        new_h.pump(recovery_out);
        new_h
    }

    /// Shuts down the worker and store thread, reopens the underlying `ShardStore`,
    /// and invokes `f` for direct storage inspection.
    pub fn inspect_store<R, F: FnOnce(&mut ShardStore) -> R>(mut self, f: F) -> R {
        if let Some(barrier) = self.store_barrier.take() {
            barrier.wait();
        }
        let _ = self.store_handle.send(StoreCommand::Shutdown);
        if let Some(h) = self.join_handle.take() {
            let _ = h.join();
        }

        let dir = self._dir.take().expect("tempdir");
        let plan =
            Arc::new(AggregationPlan::new(&self._cfg.aggregations).expect("failed to build plan"));
        let meta = PodMeta::load_or_init(
            dir.path(),
            &self._cfg.output.emitter_id,
            1,
            &plan.schema_bytes(),
        )
        .expect("failed to init pod meta");
        let opts = StoreOptions {
            shard_dir: dir.path().join("shard_0"),
            cache_bytes: 8 << 20,
            hdr_memtable_bytes: 1 << 20,
            tail_memtable_bytes: 1 << 20,
            sync_wal: true,
            layout: plan.layout(),
            worker_threads: self._cfg.storage.worker_threads,
        };
        let mut store = ShardStore::open(opts, &meta).expect("failed to reopen shard store");
        f(&mut store)
    }
}
