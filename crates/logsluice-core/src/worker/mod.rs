//! Core shard worker state machine managing ingestion, L1 buffering, and window lifecycles.

pub mod lifecycle;
pub mod recovery;
pub mod spill;

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use bytes::Bytes;
use hashbrown::HashMap;

use crate::cardinality::Admission;
use crate::codec::plan::AggregationPlan;
use crate::codec::tail::ExemplarTail;
use crate::codec::ticket::Ticket;
use crate::config::{EmitStrategy, EngineConfig, OnCap, OnDiskFull, OnShutdown};
use crate::emission::{EmitReason, Pacer, SliceRecord, TicketLedger, records_for};
use crate::key::StorageKey24;
use crate::l1::DoubleBuffer;
use crate::position::OffsetTracker;
pub use crate::position::SourcePosition;
pub use crate::resource::ResourceScopePayload;
use crate::resource::ResourceScopeTable;
use crate::simd::SimdKernelTable;
use crate::store::shard_store::JoinedRow;
use crate::store::thread::{StoreCommand, StoreEvent};
use crate::window::WindowClock;

pub use self::lifecycle::{ClosingState, WindowPhase, WindowState, close_jitter_ms};
pub use self::recovery::RecoveredState;
pub use self::spill::SpillWriter;

/// Replay check interval for spilled aggregate files in milliseconds (60 seconds).
pub const SPILL_RETRY_INTERVAL_MS: i64 = 60_000;

#[derive(Clone, Debug)]
struct UnackedTicket {
    ticket_id: u64,
    first_emitted_ms: i64,
    records: Vec<SliceRecord>,
    backoff_ms: i64,
    next_retry_ms: Option<i64>,
}

/// Input item descriptor ingested by a shard worker.
#[derive(Clone, Debug, PartialEq)]
pub struct IngestItem {
    /// 24-byte storage key with window epoch and grouping digest.
    pub key: StorageKey24,
    /// Interned resource and instrumentation scope metadata.
    pub resource: Arc<ResourceScopePayload>,
    /// Precomputed canonical hash of the resource payload.
    pub resource_hash: u64,
    /// Optional stream partition offset for replay deduplication.
    pub position: Option<SourcePosition>,
    /// Event occurrence timestamp in epoch milliseconds.
    pub event_ms: i64,
    /// Counter values per bank.
    pub counters: Vec<[u64; 4]>,
    /// Min/max values for numeric slots.
    pub minmax_values: Vec<i64>,
    /// Exemplar tail payload and attributes.
    pub exemplar: ExemplarSource,
}

/// Raw exemplar payload and dimensions before conversion to `ExemplarTail`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExemplarSource {
    /// Syslog or log severity value.
    pub severity: u8,
    /// Raw log message body.
    pub body: Option<Bytes>,
    /// Dimension values matching aggregation plan.
    pub dims: Vec<Option<Bytes>>,
    /// Custom key-value exemplar attributes.
    pub attrs: Vec<(String, Bytes)>,
}

impl ExemplarSource {
    /// Converts this exemplar source into an `ExemplarTail` instance.
    #[must_use]
    pub fn into_tail(self) -> ExemplarTail {
        ExemplarTail {
            severity: self.severity,
            body: self.body,
            dims: self.dims,
            attrs: self.attrs,
        }
    }
}

/// Shared immutable context configuring a shard worker.
#[derive(Clone, Debug)]
pub struct WorkerContext {
    /// Global engine configuration.
    pub cfg: Arc<EngineConfig>,
    /// Zero-based shard index.
    pub shard: u16,
    /// Emitter identifier string.
    pub emitter_id: Arc<str>,
    /// SIMD vectorization function pointers.
    pub kernels: SimdKernelTable,
    /// Metric aggregation plan and header layout.
    pub plan: Arc<AggregationPlan>,
    /// Window discretization clock.
    pub clock: WindowClock,
}

/// Atomic purge horizon tracking the latest purged window boundary across shards.
#[derive(Clone, Debug)]
pub struct PurgeHorizon(Arc<AtomicI64>);

impl PurgeHorizon {
    /// Creates a new `PurgeHorizon` initialized with `initial`.
    #[must_use]
    pub fn new(initial: i64) -> Self {
        Self(Arc::new(AtomicI64::new(initial)))
    }

    /// Loads the current purge horizon value.
    #[must_use]
    pub fn get(&self) -> i64 {
        self.0.load(Ordering::Acquire)
    }

    /// Advances the purge horizon to at least `horizon`, never lowering the current value.
    pub fn set(&self, horizon: i64) {
        self.0.fetch_max(horizon, Ordering::AcqRel);
    }
}

impl Default for PurgeHorizon {
    fn default() -> Self {
        Self::new(0)
    }
}

/// Summary report returned upon processing an ingest batch.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IngestReport {
    /// Number of items newly accepted and aggregated.
    pub accepted: u64,
    /// Number of items discarded as stream replays.
    pub replayed: u64,
    /// Number of items routed to cardinality overflow.
    pub overflowed: u64,
    /// Whether L1 buffer capacity was reached and in-flight flush prevented acceptance.
    pub backpressure: bool,
}

/// Output produced by worker operations (poll, on_event, on_ack).
#[derive(Debug)]
pub struct WorkerOutput<F> {
    /// Commands to dispatch to the shard storage thread.
    pub commands: Vec<StoreCommand<F>>,
    /// Aggregate slice record batches ready for emission.
    pub emit: Vec<EmitBatch>,
    /// Flushed finalizer tokens ready for release.
    pub release: Vec<F>,
    /// Finalizer tokens from failed flushes that must be marked as errored.
    pub failed_release: Vec<F>,
}

impl<F> Default for WorkerOutput<F> {
    fn default() -> Self {
        Self {
            commands: Vec::new(),
            emit: Vec::new(),
            release: Vec::new(),
            failed_release: Vec::new(),
        }
    }
}

/// Emission batch containing records tied to an emission delivery ticket.
#[derive(Clone, Debug, PartialEq)]
pub struct EmitBatch {
    /// Associated delivery ticket ID.
    pub ticket_id: u64,
    /// Emitted aggregate slice records.
    pub records: Vec<SliceRecord>,
}

/// Snapshot of operational metrics and SLIs owned by the shard worker.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct WorkerStats {
    /// Total events accepted for aggregation.
    pub events_ingested_total: u64,
    /// Total events discarded due to source offset replay.
    pub replayed_events_total: u64,
    /// Current number of distinct known keys tracked across all active windows.
    pub keys_known: usize,
    /// Total keys routed to overflow due to cardinality limits.
    pub keys_overflowed_total: u64,
    /// Total keys evicted under cold-eviction policies.
    pub keys_evicted_total: u64,
    /// Number of storage keys currently in-flight across unacknowledged tickets.
    pub tickets_inflight: usize,
    /// Total aggregate slices re-emitted upon crash recovery.
    pub slices_reemitted_total: u64,
    /// Total windows forced to close due to wall-clock timeouts.
    pub forced_closes_total: u64,
    /// Current database directory disk usage in bytes.
    pub disk_bytes: u64,
    /// Total active entries in the resource scope intern table.
    pub resources_table_entries: usize,
    /// Total events dropped due to disk space exhaustion.
    pub dropped_events_total: u64,
    /// Number of disk spill files currently stored.
    pub spill_files: usize,
}

/// Shard aggregation worker managing in-memory buffering, state machines, and lifecycle transitions.
pub struct ShardWorker<F> {
    ctx: WorkerContext,
    horizon: PurgeHorizon,
    double_buf: DoubleBuffer<F>,
    offset_tracker: OffsetTracker,
    resources: ResourceScopeTable,
    ledger: TicketLedger,
    pacer: Pacer,
    paced_emit_queue: VecDeque<EmitBatch>,
    windows: HashMap<u64, WindowState>,
    last_flush_ms: i64,
    stats: WorkerStats,
    pending_commands: Vec<StoreCommand<F>>,
    pending_rows: HashMap<u64, Vec<JoinedRow>>,
    disk_override: Option<u64>,
    disk_full: bool,
    spill_writer: SpillWriter,
    unacked_tickets: HashMap<u64, UnackedTicket>,
    last_spill_replay_ms: i64,
    drain_in_progress: bool,
    drain_deadline_ms: Option<i64>,
    restored_tickets: Vec<Ticket>,
    recovery_ticket_queue: VecDeque<u64>,
    store_queries_inflight: usize,
    persist_tickets_inflight: usize,
    evict_ack_epochs: HashMap<u64, u64>,
}

impl<F> ShardWorker<F> {
    /// Creates a new `ShardWorker` initialized with context, recovered state, and purge horizon.
    #[must_use]
    pub fn new(ctx: WorkerContext, recovered: RecoveredState, horizon: PurgeHorizon) -> Self {
        let l1_cap = ctx.cfg.storage.l1_coalesce_max_keys;
        let rate_limit = ctx.cfg.output.max_emit_rate_per_sec as u32;
        let shard_base_dir = if ctx.shard == 0 && !ctx.cfg.storage.data_dir.join("shard_0").exists()
        {
            ctx.cfg.storage.data_dir.clone()
        } else {
            ctx.cfg
                .storage
                .data_dir
                .join(format!("shard_{}", ctx.shard))
        };
        let spill_writer = SpillWriter::new(shard_base_dir);

        Self {
            ctx,
            horizon,
            double_buf: DoubleBuffer::new(l1_cap),
            offset_tracker: recovered.offset_tracker,
            resources: recovered.resources,
            ledger: recovered.ledger,
            pacer: Pacer::new(rate_limit),
            paced_emit_queue: VecDeque::new(),
            windows: recovered.windows,
            last_flush_ms: 0,
            stats: WorkerStats::default(),
            pending_commands: Vec::new(),
            pending_rows: HashMap::new(),
            disk_override: None,
            disk_full: false,
            spill_writer,
            unacked_tickets: HashMap::new(),
            last_spill_replay_ms: 0,
            drain_in_progress: false,
            drain_deadline_ms: None,
            restored_tickets: recovered.tickets,
            recovery_ticket_queue: VecDeque::new(),
            store_queries_inflight: 0,
            persist_tickets_inflight: 0,
            evict_ack_epochs: HashMap::new(),
        }
    }

    /// Returns the effective database disk usage in bytes, respecting any test override.
    #[must_use]
    pub fn effective_disk_bytes(&self) -> u64 {
        self.disk_override.unwrap_or(self.stats.disk_bytes)
    }

    /// Sets the disk usage in bytes (called on FlushDone) and evaluates disk-full policies.
    pub fn set_disk_bytes(&mut self, bytes: u64) {
        self.stats.disk_bytes = bytes;
        self.check_disk_threshold();
    }

    /// Pins the simulated disk usage in bytes for testing.
    #[doc(hidden)]
    pub fn set_disk_override(&mut self, bytes: Option<u64>) {
        self.disk_override = bytes;
        self.check_disk_threshold();
    }

    /// Alias for [`ShardWorker::set_disk_override`] for test harnesses.
    #[doc(hidden)]
    pub fn set_disk_bytes_for_test(&mut self, bytes: Option<u64>) {
        self.set_disk_override(bytes);
    }

    fn check_disk_threshold(&mut self) {
        let disk = self.effective_disk_bytes();
        let threshold = self
            .ctx
            .cfg
            .storage
            .max_disk_bytes
            .saturating_sub(self.ctx.cfg.storage.control_reserve_bytes);
        let recovery_threshold = (threshold as f64 * 0.90) as u64;

        if disk >= threshold {
            self.disk_full = true;
            if self.ctx.cfg.storage.on_disk_full == OnDiskFull::OverflowBucket {
                for window in self.windows.values_mut() {
                    if window.is_active() {
                        window.guard.freeze_new_keys(true);
                    }
                }
            }
        } else if self.disk_full && disk < recovery_threshold {
            self.disk_full = false;
            if self.ctx.cfg.storage.on_disk_full == OnDiskFull::OverflowBucket {
                for window in self.windows.values_mut() {
                    if window.is_active() {
                        window.guard.freeze_new_keys(false);
                    }
                }
            }
        }
    }

    /// Returns the number of disk spill files currently stored.
    #[must_use]
    pub fn spill_files(&self) -> usize {
        self.spill_writer.count_files()
    }

    /// Returns `true` if there are any in-flight tickets or paced emission batches queued.
    #[must_use]
    pub fn has_inflight_tickets(&self) -> bool {
        self.ledger.inflight_len() > 0 || !self.paced_emit_queue.is_empty()
    }

    /// Returns `true` if all buffered state and pending drain operations have settled.
    #[must_use]
    pub fn is_drain_idle(&self) -> bool {
        if self.double_buf.flush_in_flight()
            || !self.double_buf.active().is_empty()
            || self.drain_in_progress
            || self.store_queries_inflight > 0
            || self.persist_tickets_inflight > 0
            || !self.paced_emit_queue.is_empty()
        {
            return false;
        }
        match self.ctx.cfg.shutdown.on_shutdown {
            OnShutdown::Persist => true,
            OnShutdown::EmitPartial => !self.has_inflight_tickets(),
        }
    }

    fn enqueue_or_emit_paced(
        &mut self,
        batch: EmitBatch,
        now_ms: i64,
        output: &mut WorkerOutput<F>,
    ) {
        self.paced_emit_queue.push_back(batch);
        self.drain_paced_queue(now_ms, output);
    }

    fn drain_paced_queue(&mut self, now_ms: i64, output: &mut WorkerOutput<F>) {
        let burst_cap = (self.ctx.cfg.output.max_emit_rate_per_sec as u32).max(1);
        while let Some(front) = self.paced_emit_queue.front() {
            let want = (front.records.len() as u32).min(burst_cap);
            if want == 0 || self.pacer.take(want, now_ms) > 0 {
                if let Some(batch) = self.paced_emit_queue.pop_front() {
                    output.emit.push(batch);
                }
            } else {
                break;
            }
        }
    }

    /// Re-issues row read requests for all restored unacknowledged tickets.
    pub fn recovery_output(&mut self) -> WorkerOutput<F> {
        let mut output = WorkerOutput::default();
        let tickets = std::mem::take(&mut self.restored_tickets);
        for ticket in tickets {
            let keys: Vec<StorageKey24> = ticket
                .entries
                .iter()
                .map(|e| StorageKey24 {
                    epoch_ms: ticket.window_epoch_ms,
                    digest: e.digest,
                })
                .collect();

            if keys.is_empty() {
                continue;
            }

            self.recovery_ticket_queue.push_back(ticket.id);
            self.store_queries_inflight += 1;
            output.commands.push(StoreCommand::Rows {
                epoch_ms: ticket.window_epoch_ms,
                reason: ticket.reason,
                after: None,
                keys: Some(keys),
                limit: self.ctx.cfg.output.max_records_per_ticket,
            });
        }
        output
    }

    /// Initiates a clean shutdown drain of all buffered in-memory records and active windows.
    pub fn begin_drain(&mut self, deadline_ms: i64) -> WorkerOutput<F> {
        let mut output = WorkerOutput::default();
        output.commands.append(&mut self.pending_commands);
        self.drain_deadline_ms = Some(deadline_ms);

        if !self.double_buf.active().is_empty() {
            if let Some(swapped) = self.double_buf.try_swap() {
                self.drain_in_progress = true;
                output.commands.push(StoreCommand::Flush(swapped));
            } else {
                self.drain_in_progress = true;
            }
        } else if self.double_buf.flush_in_flight() {
            self.drain_in_progress = true;
        } else if self.ctx.cfg.shutdown.on_shutdown == OnShutdown::EmitPartial {
            for window in self.windows.values() {
                self.store_queries_inflight += 1;
                output.commands.push(StoreCommand::Rows {
                    epoch_ms: window.epoch_ms,
                    reason: EmitReason::Drain,
                    after: None,
                    keys: None,
                    limit: self.ctx.cfg.output.max_records_per_ticket,
                });
            }
        }

        output
    }

    /// Explicitly flushes the active L1 buffer to storage if not empty.
    pub fn flush_l1(&mut self, now_ms: i64) -> WorkerOutput<F> {
        let mut output = WorkerOutput::default();
        if !self.double_buf.active().is_empty()
            && let Some(swapped) = self.double_buf.try_swap()
        {
            self.last_flush_ms = now_ms;
            output.commands.push(StoreCommand::Flush(swapped));
        }
        output
    }

    /// Returns true if the worker can accept items (i.e. L1 buffer has not reached capacity while a flush is in flight).
    #[must_use]
    pub fn can_ingest(&self) -> bool {
        !(self.double_buf.active().len() >= self.ctx.cfg.storage.l1_coalesce_max_keys
            && self.double_buf.flush_in_flight())
    }

    /// Ingests a batch of items, applying deduplication, cardinality control, and L1 aggregation.
    pub fn ingest(&mut self, items: Vec<IngestItem>, finalizer: F, now_ms: i64) -> IngestReport {
        match self.try_ingest(items, finalizer, now_ms) {
            Ok(report) => report,
            Err(_) => IngestReport {
                accepted: 0,
                replayed: 0,
                overflowed: 0,
                backpressure: true,
            },
        }
    }

    /// Attempts to ingest items, returning the original unconsumed items and finalizer if backpressured.
    pub fn try_ingest(
        &mut self,
        items: Vec<IngestItem>,
        finalizer: F,
        now_ms: i64,
    ) -> Result<IngestReport, (Vec<IngestItem>, F)> {
        // Check L1 capacity and backpressure before consuming items
        if self.double_buf.active().len() >= self.ctx.cfg.storage.l1_coalesce_max_keys {
            if let Some(swapped) = self.double_buf.try_swap() {
                self.last_flush_ms = now_ms;
                self.pending_commands.push(StoreCommand::Flush(swapped));
            } else {
                return Err((items, finalizer));
            }
        }

        if self.disk_full && self.ctx.cfg.storage.on_disk_full == OnDiskFull::Block {
            let has_new_key = items.iter().any(|item| {
                if let Some(ref pos) = item.position
                    && self.offset_tracker.is_replay(pos)
                {
                    return false;
                }
                let epoch = self.ctx.clock.start_of(item.event_ms) as u64;
                match self.windows.get(&epoch) {
                    Some(w) => !w.known.contains(&item.key.digest),
                    None => true,
                }
            });
            if has_new_key {
                return Err((items, finalizer));
            }
        }

        let mut accepted = 0;
        let mut replayed = 0;
        let mut overflowed = 0;

        for item in items {
            if let Some(ref pos) = item.position
                && self.offset_tracker.is_replay(pos)
            {
                replayed += 1;
                self.stats.replayed_events_total += 1;
                continue;
            }

            let window_epoch = self.ctx.clock.start_of(item.event_ms) as u64;

            if self.disk_full && self.ctx.cfg.storage.on_disk_full == OnDiskFull::DropNewest {
                let is_known = self
                    .windows
                    .get(&window_epoch)
                    .is_some_and(|w| w.known.contains(&item.key.digest));
                if !is_known {
                    self.stats.dropped_events_total += 1;
                    continue;
                }
            }

            let disk_full_overflow =
                self.disk_full && self.ctx.cfg.storage.on_disk_full == OnDiskFull::OverflowBucket;
            let window = self.windows.entry(window_epoch).or_insert_with(|| {
                let mut w = WindowState::new(
                    window_epoch,
                    self.ctx.cfg.cardinality.max_keys_per_window,
                    self.ctx.cfg.cardinality.max_resource_share,
                );
                if disk_full_overflow {
                    w.guard.freeze_new_keys(true);
                }
                w
            });

            let (scope_id, is_new_resource, is_new_window_ref) = self
                .resources
                .intern_with_window_ref(&item.resource, item.resource_hash, window_epoch);

            if is_new_resource {
                self.double_buf.active_mut().record_resource(
                    scope_id,
                    Arc::clone(&item.resource),
                    window_epoch,
                );
            } else if is_new_window_ref {
                self.double_buf
                    .active_mut()
                    .record_resource_ref(scope_id, window_epoch);
            }

            let item_key = StorageKey24 {
                epoch_ms: window_epoch,
                digest: item.key.digest,
            };
            let admission = window
                .guard
                .admit(&mut window.known, item.key.digest, scope_id);
            let (key, is_new, tail) = match admission {
                Admission::New => (item_key, true, Some(item.exemplar.into_tail())),
                Admission::Known => {
                    let tail = if window.evict_in_progress || self.ledger.is_inflight(&item_key) {
                        Some(item.exemplar.into_tail())
                    } else {
                        None
                    };
                    (item_key, false, tail)
                }
                Admission::Overflow(_) => {
                    overflowed += 1;
                    self.stats.keys_overflowed_total += 1;
                    let overflow_key = StorageKey24::overflow(window_epoch, scope_id);
                    let is_new = window.overflow_scopes.insert(scope_id);
                    (overflow_key, is_new, None)
                }
            };

            if window.is_closed() {
                window.late_dirty_keys.insert(key);
            } else if self.ctx.cfg.window.emit.strategy == EmitStrategy::Periodic {
                window.dirty_keys.insert(key);
            }

            let last_update_s = if item.event_ms >= 0 {
                (item.event_ms / 1000) as u32
            } else {
                0
            };

            self.double_buf.active_mut().accumulate(
                key,
                is_new,
                scope_id,
                &item.counters,
                &item.minmax_values,
                last_update_s,
                tail,
                &self.ctx.kernels,
            );

            if let Some(ref pos) = item.position {
                self.offset_tracker.observe(pos);
            }

            accepted += 1;
            self.stats.events_ingested_total += 1;
        }

        let pending_offsets = self.offset_tracker.take_pending();
        if !pending_offsets.is_empty() {
            self.double_buf.active_mut().record_offsets(pending_offsets);
        }
        self.double_buf.active_mut().push_finalizer(finalizer);

        Ok(IngestReport {
            accepted,
            replayed,
            overflowed,
            backpressure: false,
        })
    }

    /// Advances the worker clock and triggers periodic flush, closing transitions, and purging.
    pub fn poll(&mut self, watermark: Option<i64>, now_ms: i64) -> WorkerOutput<F> {
        let mut output = WorkerOutput::default();
        output.commands.append(&mut self.pending_commands);

        self.check_disk_threshold();
        self.drain_paced_queue(now_ms, &mut output);

        // 0a. Check unacknowledged tickets for max_closing_duration spill
        let max_closing_duration_ms = self.ctx.cfg.window.max_closing_duration.as_millis() as i64;
        let mut to_spill = Vec::new();
        for (&ticket_id, unacked) in &self.unacked_tickets {
            if now_ms.saturating_sub(unacked.first_emitted_ms) >= max_closing_duration_ms {
                to_spill.push(ticket_id);
            }
        }

        for ticket_id in to_spill {
            let spill_ok = if let Some(unacked) = self.unacked_tickets.get(&ticket_id) {
                self.spill_writer.spill(ticket_id, &unacked.records).is_ok()
            } else {
                false
            };
            if spill_ok {
                self.unacked_tickets.remove(&ticket_id);
                self.pending_rows.remove(&ticket_id);
                self.last_spill_replay_ms = now_ms;
                if let Some(ticket) = self.ledger.resolve(ticket_id) {
                    let epoch_ms = ticket.window_epoch_ms;
                    if ticket.reason == EmitReason::Evict {
                        self.evict_ack_epochs.insert(ticket.id, epoch_ms);
                    }
                    output.commands.push(StoreCommand::Ack(ticket));
                    if let Some(window) = self.windows.get_mut(&epoch_ms)
                        && let WindowPhase::Closing(ref closing) = window.phase
                        && closing.scan_complete
                        && !self.ledger.has_inflight_for_window(epoch_ms)
                    {
                        window.transition_to_closed(now_ms);
                    }
                }
            }
        }

        // 0b. Retry backoff for unacknowledged tickets
        for unacked in self.unacked_tickets.values_mut() {
            if let Some(retry_ms) = unacked.next_retry_ms
                && now_ms >= retry_ms
            {
                unacked.next_retry_ms = None;
                output.emit.push(EmitBatch {
                    ticket_id: unacked.ticket_id,
                    records: unacked.records.clone(),
                });
            }
        }

        // 0c. Periodic spill replay
        let spill_replay_due = if self.last_spill_replay_ms == 0 {
            now_ms >= SPILL_RETRY_INTERVAL_MS
        } else {
            now_ms.saturating_sub(self.last_spill_replay_ms) >= SPILL_RETRY_INTERVAL_MS
        };
        if spill_replay_due {
            self.last_spill_replay_ms = now_ms;
            if let Ok(pending) = self.spill_writer.pending() {
                for (ticket_id, records) in pending {
                    output.emit.push(EmitBatch { ticket_id, records });
                }
            }
        }

        // 1. Periodic L1 flush check
        let flush_interval_ms = self.ctx.cfg.storage.l1_flush_interval.as_millis() as i64;
        if now_ms.saturating_sub(self.last_flush_ms) >= flush_interval_ms
            && !self.double_buf.active().is_empty()
            && let Some(swapped) = self.double_buf.try_swap()
        {
            self.last_flush_ms = now_ms;
            output.commands.push(StoreCommand::Flush(swapped));
        }

        // 2. Periodic emission cadence check
        if self.ctx.cfg.window.emit.strategy == EmitStrategy::Periodic
            && let Some(interval) = self.ctx.cfg.window.emit.interval
        {
            let interval_ms = interval.as_millis() as i64;
            for window in self.windows.values_mut() {
                if window.is_active()
                    && !window.dirty_keys.is_empty()
                    && !window.periodic_flush_pending
                    && now_ms.saturating_sub(window.last_periodic_emit_ms) >= interval_ms
                {
                    window.last_periodic_emit_ms = now_ms;
                    if !self.double_buf.active().is_empty() {
                        if let Some(swapped) = self.double_buf.try_swap() {
                            self.last_flush_ms = now_ms;
                            output.commands.push(StoreCommand::Flush(swapped));
                            window.periodic_flush_pending = true;
                        }
                    } else if self.double_buf.flush_in_flight() {
                        window.periodic_flush_pending = true;
                    } else {
                        let mut keys: Vec<StorageKey24> = window.dirty_keys.drain().collect();
                        keys.sort_unstable();
                        self.store_queries_inflight += 1;
                        output.commands.push(StoreCommand::Rows {
                            epoch_ms: window.epoch_ms,
                            reason: EmitReason::Periodic,
                            keys: Some(keys),
                            after: None,
                            limit: self.ctx.cfg.output.max_records_per_ticket,
                        });
                    }
                }
            }
        }

        // 3. Late revision cadence check
        let late_interval_ms = self.ctx.cfg.window.late_emit_interval.as_millis() as i64;
        for window in self.windows.values_mut() {
            if window.is_closed()
                && !window.late_dirty_keys.is_empty()
                && !window.late_flush_pending
                && now_ms.saturating_sub(window.last_late_emit_ms) >= late_interval_ms
            {
                window.last_late_emit_ms = now_ms;
                if !self.double_buf.active().is_empty() {
                    if let Some(swapped) = self.double_buf.try_swap() {
                        self.last_flush_ms = now_ms;
                        output.commands.push(StoreCommand::Flush(swapped));
                        window.late_flush_pending = true;
                    }
                } else if self.double_buf.flush_in_flight() {
                    window.late_flush_pending = true;
                } else {
                    let mut keys: Vec<StorageKey24> = window.late_dirty_keys.drain().collect();
                    keys.sort_unstable();
                    self.store_queries_inflight += 1;
                    output.commands.push(StoreCommand::Rows {
                        epoch_ms: window.epoch_ms,
                        reason: EmitReason::LateRevision,
                        keys: Some(keys),
                        after: None,
                        limit: self.ctx.cfg.output.max_records_per_ticket,
                    });
                }
            }
        }

        // 4. Cold key eviction check
        if self.ctx.cfg.cardinality.on_cap == OnCap::EvictCold {
            let evict_interval_ms = self.ctx.cfg.cardinality.evict_interval.as_millis() as i64;
            let evict_threshold = (self.ctx.cfg.cardinality.evict_high_water
                * self.ctx.cfg.cardinality.max_keys_per_window as f64)
                as usize;
            for window in self.windows.values_mut() {
                if window.is_active()
                    && !window.evict_in_progress
                    && window.known.len() >= evict_threshold
                    && now_ms.saturating_sub(window.last_evict_ms) >= evict_interval_ms
                {
                    window.last_evict_ms = now_ms;
                    window.evict_in_progress = true;
                    window.evict_collected_rows.clear();

                    if !self.double_buf.active().is_empty() {
                        if let Some(swapped) = self.double_buf.try_swap() {
                            self.last_flush_ms = now_ms;
                            output.commands.push(StoreCommand::Flush(swapped));
                            window.evict_flush_pending = true;
                        }
                    } else if self.double_buf.flush_in_flight() {
                        window.evict_flush_pending = true;
                    } else {
                        self.store_queries_inflight += 1;
                        output.commands.push(StoreCommand::Rows {
                            epoch_ms: window.epoch_ms,
                            reason: EmitReason::Evict,
                            after: None,
                            keys: None,
                            limit: self.ctx.cfg.output.max_records_per_ticket,
                        });
                    }
                }
            }
        }

        // 5. Active -> Closing transitions
        for window in self.windows.values_mut() {
            if window.is_active() {
                let window_end = self.ctx.clock.end_of(window.epoch_ms as i64);
                let max_close_delay_ms = self.ctx.cfg.window.max_close_delay.as_millis() as i64;
                let should_close = watermark.is_some_and(|w| w >= window_end)
                    || now_ms >= window_end.saturating_add(max_close_delay_ms);

                if should_close {
                    let forced = watermark.is_none_or(|w| w < window_end);
                    if forced {
                        self.stats.forced_closes_total += 1;
                    }
                    let max_jitter_ms =
                        self.ctx.cfg.window.emit.close_jitter_max.as_millis() as i64;
                    let jitter =
                        close_jitter_ms(&self.ctx.emitter_id, self.ctx.shard, max_jitter_ms);
                    window.transition_to_closing(now_ms, jitter, forced);
                }
            }
        }

        // 6. Process Closing windows: flush if needed, then issue scan
        for window in self.windows.values_mut() {
            if let WindowPhase::Closing(ref mut closing) = window.phase
                && now_ms >= closing.emission_start_ms
            {
                if !closing.flush_done {
                    if !self.double_buf.active().is_empty() {
                        if let Some(swapped) = self.double_buf.try_swap() {
                            self.last_flush_ms = now_ms;
                            output.commands.push(StoreCommand::Flush(swapped));
                        }
                    } else if !self.double_buf.flush_in_flight() {
                        closing.flush_done = true;
                    }
                }

                if closing.flush_done && !closing.scan_started {
                    closing.scan_started = true;
                    self.store_queries_inflight += 1;
                    output.commands.push(StoreCommand::Rows {
                        epoch_ms: window.epoch_ms,
                        reason: EmitReason::Close,
                        after: None,
                        keys: None,
                        limit: self.ctx.cfg.output.max_records_per_ticket,
                    });
                }
            }
        }

        // 7. Closed -> Purged transitions
        let mut to_purge = Vec::new();
        for window in self.windows.values() {
            if window.is_closed() {
                let window_end = self.ctx.clock.end_of(window.epoch_ms as i64);
                let lateness_ms = self.ctx.cfg.window.allowed_lateness.as_millis() as i64;
                let max_close_delay_ms = self.ctx.cfg.window.max_close_delay.as_millis() as i64;
                let should_purge = watermark
                    .is_some_and(|w| w > window_end.saturating_add(lateness_ms))
                    || now_ms
                        >= window_end
                            .saturating_add(max_close_delay_ms)
                            .saturating_add(lateness_ms);

                if should_purge && !self.ledger.has_inflight_for_window(window.epoch_ms) {
                    to_purge.push(window.epoch_ms);
                }
            }
        }

        for epoch_ms in to_purge {
            let release_resources = self.resources.release_window(epoch_ms);
            output.commands.push(StoreCommand::DropWindow {
                epoch_ms,
                release_resources,
            });
            let window_end = self.ctx.clock.end_of(epoch_ms as i64);
            self.horizon.set(window_end);
            self.windows.remove(&epoch_ms);
        }

        output
    }

    /// Handles an asynchronous event emitted by the shard storage thread.
    pub fn on_event(&mut self, ev: StoreEvent<F>, now_ms: i64) -> WorkerOutput<F> {
        let mut output = WorkerOutput::default();
        output.commands.append(&mut self.pending_commands);

        match ev {
            StoreEvent::FlushDone {
                finalizers,
                stats: _,
                disk_bytes,
            } => {
                self.double_buf.flush_completed();
                output.release.extend(finalizers);
                self.set_disk_bytes(disk_bytes);

                if self.drain_in_progress {
                    if !self.double_buf.active().is_empty() {
                        if let Some(swapped) = self.double_buf.try_swap() {
                            self.last_flush_ms = now_ms;
                            output.commands.push(StoreCommand::Flush(swapped));
                        }
                    } else {
                        self.drain_in_progress = false;
                        if self.ctx.cfg.shutdown.on_shutdown == OnShutdown::EmitPartial {
                            for window in self.windows.values() {
                                self.store_queries_inflight += 1;
                                output.commands.push(StoreCommand::Rows {
                                    epoch_ms: window.epoch_ms,
                                    reason: EmitReason::Drain,
                                    after: None,
                                    keys: None,
                                    limit: self.ctx.cfg.output.max_records_per_ticket,
                                });
                            }
                        }
                    }
                }

                // Resume scan initiation for any closing window that was waiting on flush
                for window in self.windows.values_mut() {
                    if let WindowPhase::Closing(ref mut closing) = window.phase
                        && now_ms >= closing.emission_start_ms
                        && !closing.flush_done
                    {
                        if !self.double_buf.active().is_empty() {
                            if let Some(swapped) = self.double_buf.try_swap() {
                                self.last_flush_ms = now_ms;
                                output.commands.push(StoreCommand::Flush(swapped));
                            }
                        } else {
                            closing.flush_done = true;
                            if !closing.scan_started {
                                closing.scan_started = true;
                                self.store_queries_inflight += 1;
                                output.commands.push(StoreCommand::Rows {
                                    epoch_ms: window.epoch_ms,
                                    reason: EmitReason::Close,
                                    after: None,
                                    keys: None,
                                    limit: self.ctx.cfg.output.max_records_per_ticket,
                                });
                            }
                        }
                    }

                    if window.periodic_flush_pending {
                        if !self.double_buf.active().is_empty() {
                            if let Some(swapped) = self.double_buf.try_swap() {
                                self.last_flush_ms = now_ms;
                                output.commands.push(StoreCommand::Flush(swapped));
                            }
                        } else {
                            window.periodic_flush_pending = false;
                            let mut keys: Vec<StorageKey24> = window.dirty_keys.drain().collect();
                            keys.sort_unstable();
                            if !keys.is_empty() {
                                self.store_queries_inflight += 1;
                                output.commands.push(StoreCommand::Rows {
                                    epoch_ms: window.epoch_ms,
                                    reason: EmitReason::Periodic,
                                    keys: Some(keys),
                                    after: None,
                                    limit: self.ctx.cfg.output.max_records_per_ticket,
                                });
                            }
                        }
                    }

                    if window.late_flush_pending {
                        if !self.double_buf.active().is_empty() {
                            if let Some(swapped) = self.double_buf.try_swap() {
                                self.last_flush_ms = now_ms;
                                output.commands.push(StoreCommand::Flush(swapped));
                            }
                        } else {
                            window.late_flush_pending = false;
                            let mut keys: Vec<StorageKey24> =
                                window.late_dirty_keys.drain().collect();
                            keys.sort_unstable();
                            if !keys.is_empty() {
                                self.store_queries_inflight += 1;
                                output.commands.push(StoreCommand::Rows {
                                    epoch_ms: window.epoch_ms,
                                    reason: EmitReason::LateRevision,
                                    keys: Some(keys),
                                    after: None,
                                    limit: self.ctx.cfg.output.max_records_per_ticket,
                                });
                            }
                        }
                    }

                    if window.evict_flush_pending {
                        if !self.double_buf.active().is_empty() {
                            if let Some(swapped) = self.double_buf.try_swap() {
                                self.last_flush_ms = now_ms;
                                output.commands.push(StoreCommand::Flush(swapped));
                            }
                        } else {
                            window.evict_flush_pending = false;
                            self.store_queries_inflight += 1;
                            output.commands.push(StoreCommand::Rows {
                                epoch_ms: window.epoch_ms,
                                reason: EmitReason::Evict,
                                after: None,
                                keys: None,
                                limit: self.ctx.cfg.output.max_records_per_ticket,
                            });
                        }
                    }
                }
            }
            StoreEvent::Rows {
                epoch_ms,
                reason,
                rows,
                next_after,
            } => {
                self.store_queries_inflight = self.store_queries_inflight.saturating_sub(1);

                if let Some(ticket_id) = self.recovery_ticket_queue.pop_front() {
                    if let Some(ticket) = self.ledger.get(ticket_id).cloned()
                        && let Ok(records) = records_for(
                            &ticket,
                            &rows,
                            &self.resources,
                            &self.ctx.emitter_id,
                            &self.ctx.clock,
                            self.ctx.cfg.emit_mode,
                        )
                    {
                        self.stats.slices_reemitted_total += records.len() as u64;
                        self.unacked_tickets.insert(
                            ticket.id,
                            UnackedTicket {
                                ticket_id: ticket.id,
                                first_emitted_ms: now_ms,
                                records: records.clone(),
                                backoff_ms: 5_000,
                                next_retry_ms: None,
                            },
                        );
                        self.enqueue_or_emit_paced(
                            EmitBatch {
                                ticket_id: ticket.id,
                                records,
                            },
                            now_ms,
                            &mut output,
                        );
                    }
                } else if reason == EmitReason::Evict {
                    if let Some(window) = self.windows.get_mut(&epoch_ms) {
                        let max_evict_keep = ((self.ctx.cfg.cardinality.max_keys_per_window as f64
                            * 0.10)
                            .ceil() as usize)
                            .max(1);
                        window
                            .evict_collected_rows
                            .extend(rows.into_iter().filter(|r| !r.key.is_overflow()));
                        if window.evict_collected_rows.len() > max_evict_keep * 2 {
                            window.evict_collected_rows.sort_by(|a, b| {
                                a.header
                                    .last_update_s
                                    .cmp(&b.header.last_update_s)
                                    .then_with(|| a.key.digest.cmp(&b.key.digest))
                            });
                            window.evict_collected_rows.truncate(max_evict_keep);
                        }
                        if let Some(next) = next_after {
                            self.store_queries_inflight += 1;
                            output.commands.push(StoreCommand::Rows {
                                epoch_ms,
                                reason: EmitReason::Evict,
                                after: Some(next),
                                keys: None,
                                limit: self.ctx.cfg.output.max_records_per_ticket,
                            });
                        } else {
                            window.evict_in_progress = false;
                            let mut regular_rows = std::mem::take(&mut window.evict_collected_rows);

                            if !regular_rows.is_empty() {
                                regular_rows.sort_by(|a, b| {
                                    a.header
                                        .last_update_s
                                        .cmp(&b.header.last_update_s)
                                        .then_with(|| a.key.digest.cmp(&b.key.digest))
                                });

                                let evict_count = ((window.known.len() as f64 * 0.10).ceil()
                                    as usize)
                                    .max(1)
                                    .min(max_evict_keep)
                                    .min(regular_rows.len());
                                let to_evict: Vec<JoinedRow> =
                                    regular_rows.into_iter().take(evict_count).collect();

                                let tickets = self.ledger.build(
                                    epoch_ms,
                                    EmitReason::Evict,
                                    &to_evict,
                                    &self.ctx.kernels,
                                    self.ctx.cfg.output.max_records_per_ticket,
                                );

                                if !tickets.is_empty() {
                                    for ticket in &tickets {
                                        self.pending_rows.insert(ticket.id, to_evict.clone());
                                    }
                                    self.persist_tickets_inflight += 1;
                                    output.commands.push(StoreCommand::PersistTickets(tickets));
                                }
                            }
                        }
                    }
                } else {
                    let tickets = self.ledger.build(
                        epoch_ms,
                        reason,
                        &rows,
                        &self.ctx.kernels,
                        self.ctx.cfg.output.max_records_per_ticket,
                    );

                    if !tickets.is_empty() {
                        for ticket in &tickets {
                            self.pending_rows.insert(ticket.id, rows.clone());
                        }
                        self.persist_tickets_inflight += 1;
                        output.commands.push(StoreCommand::PersistTickets(tickets));
                    }

                    if reason == EmitReason::Drain {
                        if let Some(next) = next_after {
                            self.store_queries_inflight += 1;
                            output.commands.push(StoreCommand::Rows {
                                epoch_ms,
                                reason: EmitReason::Drain,
                                after: Some(next),
                                keys: None,
                                limit: self.ctx.cfg.output.max_records_per_ticket,
                            });
                        }
                    } else if let Some(window) = self.windows.get_mut(&epoch_ms)
                        && let WindowPhase::Closing(ref mut closing) = window.phase
                    {
                        if let Some(next) = next_after {
                            closing.scan_cursor = Some(next);
                            self.store_queries_inflight += 1;
                            output.commands.push(StoreCommand::Rows {
                                epoch_ms,
                                reason,
                                after: Some(next),
                                keys: None,
                                limit: self.ctx.cfg.output.max_records_per_ticket,
                            });
                        } else {
                            closing.scan_complete = true;
                            if !self.ledger.has_inflight_for_window(epoch_ms) {
                                window.transition_to_closed(now_ms);
                            }
                        }
                    }
                }
            }
            StoreEvent::TicketsPersisted(ticket_ids) => {
                self.persist_tickets_inflight = self.persist_tickets_inflight.saturating_sub(1);

                for ticket_id in ticket_ids {
                    if let Some(ticket) = self.ledger.get(ticket_id).cloned()
                        && let Some(rows) = self.pending_rows.get(&ticket_id)
                        && let Ok(records) = records_for(
                            &ticket,
                            rows,
                            &self.resources,
                            &self.ctx.emitter_id,
                            &self.ctx.clock,
                            self.ctx.cfg.emit_mode,
                        )
                    {
                        self.pending_rows.remove(&ticket_id);
                        self.unacked_tickets.insert(
                            ticket_id,
                            UnackedTicket {
                                ticket_id,
                                first_emitted_ms: now_ms,
                                records: records.clone(),
                                backoff_ms: 5_000,
                                next_retry_ms: None,
                            },
                        );
                        self.enqueue_or_emit_paced(
                            EmitBatch { ticket_id, records },
                            now_ms,
                            &mut output,
                        );
                    }
                }
            }
            StoreEvent::AckApplied {
                ticket_id,
                evicted_keys,
            } => {
                self.pending_rows.remove(&ticket_id);
                if let Some(epoch_ms) = self.evict_ack_epochs.remove(&ticket_id)
                    && let Some(window) = self.windows.get_mut(&epoch_ms)
                {
                    for (digest, scope_id) in evicted_keys {
                        let key = StorageKey24 { epoch_ms, digest };
                        if !self.double_buf.active().contains_key(&key) {
                            window.dirty_keys.remove(&key);
                            window.guard.forget(&mut window.known, digest, scope_id);
                            self.stats.keys_evicted_total += 1;
                        }
                    }
                }
            }
            StoreEvent::FlushFailed {
                error: _,
                finalizers,
            } => {
                self.double_buf.flush_completed();
                self.drain_in_progress = false;
                output.failed_release.extend(finalizers);
            }
            StoreEvent::Failed(_) => {
                self.store_queries_inflight = 0;
                self.persist_tickets_inflight = 0;
            }
            StoreEvent::KeysDeleted { .. } | StoreEvent::WindowDropped(_) => {}
        }

        output
    }

    /// Handles acknowledgment of an emitted aggregate ticket from downstream delivery.
    pub fn on_ack(&mut self, ticket_id: u64, delivered: bool, now_ms: i64) -> WorkerOutput<F> {
        let mut output = WorkerOutput::default();
        output.commands.append(&mut self.pending_commands);

        if delivered {
            self.unacked_tickets.remove(&ticket_id);
            let _ = self.spill_writer.remove(ticket_id);

            if let Some(ticket) = self.ledger.resolve(ticket_id) {
                let epoch_ms = ticket.window_epoch_ms;
                if ticket.reason == EmitReason::Evict {
                    self.evict_ack_epochs.insert(ticket.id, epoch_ms);
                }
                output.commands.push(StoreCommand::Ack(ticket));
                self.pending_rows.remove(&ticket_id);

                if let Some(window) = self.windows.get_mut(&epoch_ms)
                    && let WindowPhase::Closing(ref closing) = window.phase
                    && closing.scan_complete
                    && !self.ledger.has_inflight_for_window(epoch_ms)
                {
                    window.transition_to_closed(now_ms);
                }
            }
        } else if let Some(unacked) = self.unacked_tickets.get_mut(&ticket_id) {
            unacked.next_retry_ms = Some(now_ms.saturating_add(unacked.backoff_ms));
            unacked.backoff_ms = (unacked.backoff_ms * 2).min(60_000);
        }

        output
    }

    /// Takes a point-in-time snapshot of the worker statistics.
    #[must_use]
    pub fn stats(&self) -> WorkerStats {
        let keys_known: usize = self.windows.values().map(|w| w.known.len()).sum();
        let tickets_inflight = self.ledger.inflight_len();
        let resources_table_entries = self.resources.len();

        WorkerStats {
            events_ingested_total: self.stats.events_ingested_total,
            replayed_events_total: self.stats.replayed_events_total,
            keys_known,
            keys_overflowed_total: self.stats.keys_overflowed_total,
            keys_evicted_total: self.stats.keys_evicted_total,
            tickets_inflight,
            slices_reemitted_total: self.stats.slices_reemitted_total,
            forced_closes_total: self.stats.forced_closes_total,
            disk_bytes: self.effective_disk_bytes(),
            resources_table_entries,
            dropped_events_total: self.stats.dropped_events_total,
            spill_files: self.spill_files(),
        }
    }

    /// Returns a sorted list of all live window epoch timestamps currently tracked.
    #[must_use]
    pub fn live_windows(&self) -> Vec<u64> {
        let mut epochs: Vec<u64> = self.windows.keys().copied().collect();
        epochs.sort_unstable();
        epochs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn purge_horizon_never_regresses() {
        let h = PurgeHorizon::new(0);
        h.set(200);
        h.set(100);
        assert_eq!(h.get(), 200);
    }
}
