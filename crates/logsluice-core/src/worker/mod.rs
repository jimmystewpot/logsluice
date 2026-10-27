//! Core shard worker state machine managing ingestion, L1 buffering, and window lifecycles.

mod events;
pub mod lifecycle;
mod poll;
pub mod recovery;
pub mod spill;

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use bytes::Bytes;
use hashbrown::{HashMap, HashSet};

use crate::cardinality::Admission;
use crate::codec::plan::AggregationPlan;
use crate::codec::tail::ExemplarTail;
use crate::codec::ticket::Ticket;
use crate::config::{EmitStrategy, EngineConfig, OnDiskFull, OnShutdown};
use crate::emission::{EmitReason, Pacer, SliceRecord, TicketLedger};
use crate::key::StorageKey24;
use crate::l1::DoubleBuffer;
use crate::position::OffsetTracker;
pub use crate::position::SourcePosition;
pub use crate::resource::ResourceScopePayload;
use crate::resource::ResourceScopeTable;
use crate::simd::SimdKernelTable;
use crate::store::shard_store::JoinedRow;
use crate::store::thread::StoreCommand;
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
    records: Arc<[SliceRecord]>,
    backoff_ms: i64,
    next_retry_ms: Option<i64>,
    spill_in_flight: bool,
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
    pub records: Arc<[SliceRecord]>,
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
    /// Total recovery ticket re-emission attempts that failed due to missing rows, resources, or storage errors.
    pub recovery_failures_total: u64,
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
    /// Number of storage commands currently queued or deferred awaiting dispatch.
    pub store_cmd_queue_depth: usize,
    /// Total corrupt spill files quarantined during replay scans.
    pub spill_quarantined_total: u64,
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
    pending_commands: VecDeque<StoreCommand<F>>,
    pending_rows: HashMap<u64, Arc<[JoinedRow]>>,
    disk_override: Option<u64>,
    disk_full: bool,
    cached_spill_files: usize,
    spilled_tickets: HashSet<u64>,
    replay_spill_in_flight: bool,
    unacked_tickets: HashMap<u64, UnackedTicket>,
    last_spill_replay_ms: i64,
    drain_in_progress: bool,
    drain_deadline_ms: Option<i64>,
    restored_tickets: Vec<Ticket>,
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
            pending_commands: VecDeque::new(),
            pending_rows: HashMap::new(),
            disk_override: None,
            disk_full: false,
            cached_spill_files: recovered.spill_files,
            spilled_tickets: HashSet::new(),
            replay_spill_in_flight: false,
            unacked_tickets: HashMap::new(),
            last_spill_replay_ms: 0,
            drain_in_progress: false,
            drain_deadline_ms: None,
            restored_tickets: recovered.tickets,
            store_queries_inflight: 0,
            persist_tickets_inflight: 0,
            evict_ack_epochs: HashMap::new(),
        }
    }

    /// Defers a storage command to be retried on the next worker output pass.
    pub fn defer_command(&mut self, cmd: StoreCommand<F>) {
        self.pending_commands.push_back(cmd);
    }

    /// Requeues multiple unsent storage commands in FIFO order to be retried on the next worker output pass.
    pub fn requeue_commands(&mut self, cmds: impl IntoIterator<Item = StoreCommand<F>>) {
        self.pending_commands.extend(cmds);
    }

    /// Returns the number of storage commands currently deferred in the worker retry queue.
    #[must_use]
    pub fn pending_commands_len(&self) -> usize {
        self.pending_commands.len()
    }

    /// Takes commands `try_ingest` queued when it swapped L1.
    ///
    /// A successful ingest can leave `StoreCommand::Flush` here and set
    /// `flush_in_flight` without waiting for the store thread. Callers that
    /// skip `poll` (replay's `--stop-before-close` path) have to pump this
    /// output before the next ingest or before shutdown. This does not swap
    /// the active buffer: the batch just accepted stays coalesced until
    /// `flush_l1` or `poll`.
    pub fn drain_pending_commands(&mut self) -> WorkerOutput<F> {
        let mut output = WorkerOutput::default();
        output.commands.extend(self.pending_commands.drain(..));
        output
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

    /// Returns the cached number of disk spill files currently stored.
    #[must_use]
    pub fn spill_files(&self) -> usize {
        self.cached_spill_files
    }

    /// Returns `true` if there are any in-flight tickets or paced emission batches queued.
    #[must_use]
    pub fn has_inflight_tickets(&self) -> bool {
        self.ledger.inflight_len() > 0
            || self.ledger.ticket_count() > 0
            || !self.paced_emit_queue.is_empty()
    }

    /// Returns `true` if all buffered state and pending drain operations have settled.
    #[must_use]
    pub fn is_drain_idle(&self) -> bool {
        if self.double_buf.flush_in_flight()
            || !self.double_buf.active().is_empty()
            || !self.pending_commands.is_empty()
            || self.drain_in_progress
            || self.store_queries_inflight > 0
            || self.persist_tickets_inflight > 0
            || self.replay_spill_in_flight
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

    fn drain_restored_tickets(&mut self, output: &mut WorkerOutput<F>) {
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
                if let Some(resolved) = self.ledger.resolve(ticket.id) {
                    let epoch_ms = resolved.window_epoch_ms;
                    output.commands.push(StoreCommand::Ack(resolved));
                    if let Some(window) = self.windows.get_mut(&epoch_ms)
                        && let WindowPhase::Closing(ref closing) = window.phase
                        && closing.scan_complete
                        && !self.ledger.has_inflight_for_window(epoch_ms)
                    {
                        window.transition_to_closed(0);
                    }
                }
                continue;
            }

            self.store_queries_inflight += 1;
            output.commands.push(StoreCommand::Rows {
                epoch_ms: ticket.window_epoch_ms,
                reason: ticket.reason,
                after: None,
                keys: Some(keys),
                limit: self.ctx.cfg.output.max_records_per_ticket,
                recovery_ticket: Some(ticket.id),
            });
        }
    }

    /// Re-issues row read requests for all restored unacknowledged tickets.
    pub fn recovery_output(&mut self) -> WorkerOutput<F> {
        let mut output = WorkerOutput::default();
        output.commands.extend(self.pending_commands.drain(..));
        self.drain_restored_tickets(&mut output);
        output
    }

    /// Initiates a clean shutdown drain of all buffered in-memory records and active windows.
    pub fn begin_drain(&mut self, deadline_ms: i64) -> WorkerOutput<F> {
        let mut output = WorkerOutput::default();
        output.commands.extend(self.pending_commands.drain(..));
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
                    recovery_ticket: None,
                });
            }
        }

        output
    }

    /// Explicitly flushes the active L1 buffer to storage if not empty.
    pub fn flush_l1(&mut self, now_ms: i64) -> WorkerOutput<F> {
        let mut output = WorkerOutput::default();
        output.commands.extend(self.pending_commands.drain(..));
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

    /// Attempts to ingest a batch, applying deduplication, cardinality control, and L1 aggregation.
    ///
    /// `Err` means L1 is full while a flush is in flight, or disk-full [`OnDiskFull::Block`]
    /// rejected a new key. The [`Vec`] and finalizer are unchanged and must be retried or
    /// explicitly nacked by the caller. Dropping `Err` drops the events and never acks them.
    pub fn try_ingest(
        &mut self,
        items: Vec<IngestItem>,
        finalizer: F,
        now_ms: i64,
    ) -> Result<IngestReport, (Vec<IngestItem>, F)> {
        // `Err` must leave the worker unchanged. A capacity swap queues a
        // flush and sets `flush_in_flight`, so the disk-full rejection has to
        // win before that mutation. A known key is still admitted under Block.
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

        // Check L1 capacity and backpressure before consuming items.
        if self.double_buf.active().len() >= self.ctx.cfg.storage.l1_coalesce_max_keys {
            if let Some(swapped) = self.double_buf.try_swap() {
                self.last_flush_ms = now_ms;
                self.pending_commands
                    .push_back(StoreCommand::Flush(swapped));
            } else {
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

    /// Handles acknowledgment of an emitted aggregate ticket from downstream delivery.
    pub fn on_ack(&mut self, ticket_id: u64, delivered: bool, now_ms: i64) -> WorkerOutput<F> {
        let mut output = WorkerOutput::default();
        output.commands.extend(self.pending_commands.drain(..));

        if delivered {
            let was_unacked = self.unacked_tickets.remove(&ticket_id);
            let was_spilled = self.spilled_tickets.remove(&ticket_id)
                || was_unacked.as_ref().is_some_and(|u| u.spill_in_flight)
                || (self.cached_spill_files > 0 && self.ledger.get(ticket_id).is_none());
            if was_spilled {
                self.cached_spill_files = self.cached_spill_files.saturating_sub(1);
                output
                    .commands
                    .push(StoreCommand::RemoveSpill { ticket_id });
            }

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
            recovery_failures_total: self.stats.recovery_failures_total,
            forced_closes_total: self.stats.forced_closes_total,
            disk_bytes: self.effective_disk_bytes(),
            resources_table_entries,
            dropped_events_total: self.stats.dropped_events_total,
            spill_files: self.spill_files(),
            store_cmd_queue_depth: self.pending_commands.len(),
            spill_quarantined_total: self.stats.spill_quarantined_total,
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
    use crate::codec::header::NumericHeader;
    use crate::codec::plan::HeaderLayout;
    use crate::config::{AggregationKind, AggregationSpec};
    use crate::simd::SimdMode;
    use crate::store::thread::StoreEvent;

    #[test]
    fn purge_horizon_never_regresses() {
        let h = PurgeHorizon::new(0);
        h.set(200);
        h.set(100);
        assert_eq!(h.get(), 200);
    }

    #[test]
    fn pending_rows_shared_across_chunks() {
        let mut cfg = EngineConfig::default();
        cfg.output.max_records_per_ticket = 100;
        cfg.aggregations = vec![AggregationSpec {
            name: "event_count".into(),
            kind: AggregationKind::Count,
            field: None,
        }];
        let cfg = Arc::new(cfg);
        let plan = Arc::new(AggregationPlan::new(&cfg.aggregations).expect("valid plan"));
        let clock =
            WindowClock::new(cfg.window.duration, cfg.window.offset).expect("valid window clock");
        let kernels = SimdKernelTable::detect(SimdMode::Scalar);
        let ctx = WorkerContext {
            cfg,
            shard: 0,
            emitter_id: Arc::from("pod-0"),
            kernels,
            plan,
            clock,
        };

        let mut worker: ShardWorker<()> =
            ShardWorker::new(ctx, RecoveredState::empty(), PurgeHorizon::new(0));

        let layout = HeaderLayout {
            banks: 1,
            minmax_slots: 0,
        };
        let rows: Vec<JoinedRow> = (0..1000u128)
            .map(|digest| {
                let mut header = NumericHeader::new_zeroed(layout, 1);
                header.total[0] = [1, 0, 0, 0];
                JoinedRow {
                    key: StorageKey24 {
                        epoch_ms: 0,
                        digest,
                    },
                    header,
                    tail: None,
                }
            })
            .collect();

        let out = worker.on_event(
            StoreEvent::Rows {
                epoch_ms: 0,
                reason: EmitReason::Close,
                rows,
                next_after: None,
                recovery_ticket: None,
            },
            0,
        );

        assert_eq!(out.commands.len(), 1);
        assert!(matches!(
            &out.commands[0],
            StoreCommand::PersistTickets(tickets) if tickets.len() == 10
        ));
        assert_eq!(worker.pending_rows.len(), 10);

        let first_arc = worker.pending_rows.values().next().expect("non-empty");
        for rows_arc in worker.pending_rows.values() {
            assert_eq!(Arc::strong_count(rows_arc), 10);
            assert!(Arc::ptr_eq(first_arc, rows_arc));
            assert_eq!(rows_arc.len(), 1000);
        }
    }
}
