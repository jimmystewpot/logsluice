//! Store-event handlers for [`ShardWorker::on_event`](crate::worker::ShardWorker::on_event).
//!
//! `on_event` was one match over every [`StoreEvent`](crate::store::StoreEvent) the store
//! thread can emit. It still drains `pending_commands` first, then each arm calls one
//! method. The match is the state machine for work a previous `poll` or event started.
//! Arm bodies were moved, not reordered or rewritten.
//!
//! `on_flush_done` resumes drain, close, periodic, late, and evict scans waiting on flush.
//! `on_rows` builds tickets, eviction candidates, or the next close/drain cursor. Tickets
//! are persisted before slices are emitted. `on_tickets_persisted` emits that batch and
//! continues a close scan only after the write. `on_ack_applied` drops an evicted key from
//! `KnownKeySet` only when active L1 and any in-flight flush do not still hold it.
//! `on_flush_failed`, `on_persist_tickets_failed`, and `on_rows_failed` roll the phase back.
//! `on_spill_done`, `on_spill_replay`, and `on_spill_failed` track spill files and whether
//! an unacked ticket may be acked or retried. `on_store_failed` clears in-flight flags.
//! `KeysDeleted` and `WindowDropped` stay empty.

use std::sync::Arc;

use crate::config::OnShutdown;
use crate::emission::{EmitReason, SliceRecord, records_for};
use crate::key::StorageKey24;
use crate::l1::DoubleBuffer;
use crate::store::shard_store::{FlushBatch, JoinedRow};
use crate::store::thread::{StoreCommand, StoreEvent};
use crate::worker::lifecycle::WindowPhase;
use crate::worker::{EmitBatch, ShardWorker, UnackedTicket, WorkerOutput};

/// Payload of a [`StoreEvent::Rows`], bundled so `on_rows` stays under
/// clippy's argument limit.
struct RowsEvent {
    epoch_ms: u64,
    reason: EmitReason,
    rows: Vec<JoinedRow>,
    next_after: Option<StorageKey24>,
    recovery_ticket: Option<u64>,
}

/// Coldest-first order: oldest `last_update_s`, then digest. Keys within a
/// window are unique, so this is a total order and an unstable sort/select
/// gives the same result as the stable sort it replaces.
fn coldest_first(a: &JoinedRow, b: &JoinedRow) -> std::cmp::Ordering {
    a.header
        .last_update_s
        .cmp(&b.header.last_update_s)
        .then_with(|| a.key.digest.cmp(&b.key.digest))
}

/// Keeps the `n` coldest rows in place (O(len) select, no scratch buffer),
/// optionally sorting that prefix coldest-first.
fn keep_coldest(rows: &mut Vec<JoinedRow>, n: usize, sort: bool) {
    if n == 0 {
        rows.clear();
        return;
    }
    if n < rows.len() {
        rows.select_nth_unstable_by(n - 1, coldest_first);
        rows.truncate(n);
    }
    if sort {
        rows.sort_unstable_by(coldest_first);
    }
}

/// Starts a flush of the active L1 buffer if it has data and no flush is in
/// flight, then reports whether any buffered data is still not durable.
///
/// Returns `true` while either the active buffer is non-empty or a flush is
/// in flight, so a later waiter in the same `FlushDone` handler doesn't treat
/// a buffer that was just swapped out by an earlier waiter as committed (#41).
fn flush_still_pending<F>(
    double_buf: &mut DoubleBuffer<F>,
    last_flush_ms: &mut i64,
    now_ms: i64,
    commands: &mut Vec<StoreCommand<F>>,
) -> bool {
    if !double_buf.active().is_empty()
        && let Some(swapped) = double_buf.try_swap()
    {
        *last_flush_ms = now_ms;
        commands.push(StoreCommand::Flush(swapped));
    }
    !double_buf.active().is_empty() || double_buf.flush_in_flight()
}

impl<F> ShardWorker<F> {
    /// Handles an asynchronous event emitted by the shard storage thread.
    pub fn on_event(&mut self, ev: StoreEvent<F>, now_ms: i64) -> WorkerOutput<F> {
        let mut output = WorkerOutput::default();
        output.commands.extend(self.pending_commands.drain(..));

        match ev {
            StoreEvent::FlushDone {
                finalizers,
                stats: _,
                disk_bytes,
            } => self.on_flush_done(finalizers, disk_bytes, now_ms, &mut output),
            StoreEvent::Rows {
                epoch_ms,
                reason,
                rows,
                next_after,
                recovery_ticket,
            } => self.on_rows(
                RowsEvent {
                    epoch_ms,
                    reason,
                    rows,
                    next_after,
                    recovery_ticket,
                },
                now_ms,
                &mut output,
            ),
            StoreEvent::TicketsPersisted(ticket_ids) => {
                self.on_tickets_persisted(ticket_ids, now_ms, &mut output);
            }
            StoreEvent::AckApplied {
                ticket_id,
                evicted_keys,
            } => self.on_ack_applied(ticket_id, evicted_keys),
            StoreEvent::FlushFailed {
                error: _,
                batch,
                finalizers,
            } => self.on_flush_failed(batch, finalizers, &mut output),
            StoreEvent::PersistTicketsFailed {
                ticket_ids,
                error: _,
            } => self.on_persist_tickets_failed(ticket_ids, &mut output),
            StoreEvent::RowsFailed {
                epoch_ms,
                reason,
                after,
                keys,
                error: _,
                recovery_ticket,
            } => self.on_rows_failed(epoch_ms, reason, after, keys, recovery_ticket, &mut output),
            StoreEvent::SpillDone {
                ticket_id,
                spill_files,
                disk_bytes,
            } => self.on_spill_done(ticket_id, spill_files, disk_bytes, now_ms, &mut output),
            StoreEvent::SpillReplay {
                entries,
                spill_files,
                quarantined,
            } => self.on_spill_replay(entries, spill_files, quarantined, &mut output),
            StoreEvent::SpillFailed {
                ticket_id,
                error: _,
            } => self.on_spill_failed(ticket_id),
            StoreEvent::Failed(_) => self.on_store_failed(),
            StoreEvent::KeysDeleted { .. } | StoreEvent::WindowDropped(_) => {}
        }

        output
    }

    fn on_flush_done(
        &mut self,
        finalizers: Vec<F>,
        disk_bytes: u64,
        now_ms: i64,
        output: &mut WorkerOutput<F>,
    ) {
        self.double_buf.flush_completed();
        self.offset_tracker.commit_flush();
        output.release.extend(finalizers);
        self.set_disk_bytes(disk_bytes);

        if self.drain_in_progress {
            if flush_still_pending(
                &mut self.double_buf,
                &mut self.last_flush_ms,
                now_ms,
                &mut output.commands,
            ) {
                // Wait for the in-flight flush's FlushDone before proceeding.
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
                            recovery_ticket: None,
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
                if flush_still_pending(
                    &mut self.double_buf,
                    &mut self.last_flush_ms,
                    now_ms,
                    &mut output.commands,
                ) {
                    // Wait for the in-flight flush's FlushDone before proceeding.
                } else {
                    closing.flush_done = true;
                    if !closing.scan_started {
                        closing.scan_started = true;
                        self.store_queries_inflight += 1;
                        output.commands.push(StoreCommand::Rows {
                            epoch_ms: window.epoch_ms,
                            reason: EmitReason::Close,
                            after: closing.scan_cursor,
                            keys: None,
                            limit: self.ctx.cfg.output.max_records_per_ticket,
                            recovery_ticket: None,
                        });
                    }
                }
            }

            if window.periodic_flush_pending {
                if flush_still_pending(
                    &mut self.double_buf,
                    &mut self.last_flush_ms,
                    now_ms,
                    &mut output.commands,
                ) {
                    // Wait for the in-flight flush's FlushDone before proceeding.
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
                            recovery_ticket: None,
                        });
                    }
                }
            }

            if window.late_flush_pending {
                if flush_still_pending(
                    &mut self.double_buf,
                    &mut self.last_flush_ms,
                    now_ms,
                    &mut output.commands,
                ) {
                    // Wait for the in-flight flush's FlushDone before proceeding.
                } else {
                    window.late_flush_pending = false;
                    let mut keys: Vec<StorageKey24> = window.late_dirty_keys.drain().collect();
                    keys.sort_unstable();
                    if !keys.is_empty() {
                        self.store_queries_inflight += 1;
                        output.commands.push(StoreCommand::Rows {
                            epoch_ms: window.epoch_ms,
                            reason: EmitReason::LateRevision,
                            keys: Some(keys),
                            after: None,
                            limit: self.ctx.cfg.output.max_records_per_ticket,
                            recovery_ticket: None,
                        });
                    }
                }
            }

            if window.evict_flush_pending {
                if flush_still_pending(
                    &mut self.double_buf,
                    &mut self.last_flush_ms,
                    now_ms,
                    &mut output.commands,
                ) {
                    // Wait for the in-flight flush's FlushDone before proceeding.
                } else {
                    window.evict_flush_pending = false;
                    self.store_queries_inflight += 1;
                    output.commands.push(StoreCommand::Rows {
                        epoch_ms: window.epoch_ms,
                        reason: EmitReason::Evict,
                        after: None,
                        keys: None,
                        limit: self.ctx.cfg.output.max_records_per_ticket,
                        recovery_ticket: None,
                    });
                }
            }
        }
    }

    fn on_rows(&mut self, event: RowsEvent, now_ms: i64, output: &mut WorkerOutput<F>) {
        let RowsEvent {
            epoch_ms,
            reason,
            rows,
            next_after,
            recovery_ticket,
        } = event;
        self.store_queries_inflight = self.store_queries_inflight.saturating_sub(1);

        if let Some(ticket_id) = recovery_ticket {
            let records = self.ledger.get(ticket_id).map(|ticket| {
                records_for(
                    ticket,
                    &rows,
                    &self.resources,
                    &self.ctx.emitter_id,
                    &self.ctx.clock,
                    self.ctx.cfg.emit_mode,
                )
            });
            match records {
                Some(Ok(records)) => {
                    let records: Arc<[SliceRecord]> = records.into();
                    self.stats.slices_reemitted_total += records.len() as u64;
                    self.unacked_tickets.insert(
                        ticket_id,
                        UnackedTicket {
                            ticket_id,
                            first_emitted_ms: now_ms,
                            records: Arc::clone(&records),
                            backoff_ms: 5_000,
                            next_retry_ms: None,
                            spill_in_flight: false,
                        },
                    );
                    self.enqueue_or_emit_paced(EmitBatch { ticket_id, records }, now_ms, output);
                }
                Some(Err(_)) | None => {
                    self.stats.recovery_failures_total += 1;
                }
            }
        } else if reason == EmitReason::Evict {
            if let Some(window) = self.windows.get_mut(&epoch_ms) {
                let max_evict_keep = ((self.ctx.cfg.cardinality.max_keys_per_window as f64 * 0.10)
                    .ceil() as usize)
                    .max(1);
                window
                    .evict_collected_rows
                    .extend(rows.into_iter().filter(|r| !r.key.is_overflow()));
                if window.evict_collected_rows.len() > max_evict_keep * 2 {
                    keep_coldest(&mut window.evict_collected_rows, max_evict_keep, false);
                }
                if let Some(next) = next_after {
                    self.store_queries_inflight += 1;
                    output.commands.push(StoreCommand::Rows {
                        epoch_ms,
                        reason: EmitReason::Evict,
                        after: Some(next),
                        keys: None,
                        limit: self.ctx.cfg.output.max_records_per_ticket,
                        recovery_ticket: None,
                    });
                } else {
                    window.evict_in_progress = false;
                    let mut regular_rows = std::mem::take(&mut window.evict_collected_rows);

                    if !regular_rows.is_empty() {
                        let evict_count = ((window.known.len() as f64 * 0.10).ceil() as usize)
                            .max(1)
                            .min(max_evict_keep)
                            .min(regular_rows.len());
                        keep_coldest(&mut regular_rows, evict_count, true);
                        let to_evict = regular_rows;

                        let tickets = self.ledger.build(
                            epoch_ms,
                            EmitReason::Evict,
                            &to_evict,
                            &self.ctx.kernels,
                            self.ctx.cfg.output.max_records_per_ticket,
                        );

                        if !tickets.is_empty() {
                            let shared_rows: Arc<[JoinedRow]> = Arc::from(to_evict);
                            for ticket in &tickets {
                                self.pending_rows
                                    .insert(ticket.id, Arc::clone(&shared_rows));
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
            let has_tickets = !tickets.is_empty();

            if has_tickets {
                let shared_rows: Arc<[JoinedRow]> = Arc::from(rows);
                for ticket in &tickets {
                    self.pending_rows
                        .insert(ticket.id, Arc::clone(&shared_rows));
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
                        recovery_ticket: None,
                    });
                }
            } else if reason == EmitReason::Close
                && let Some(window) = self.windows.get_mut(&epoch_ms)
                && let WindowPhase::Closing(ref mut closing) = window.phase
            {
                if has_tickets {
                    closing.pending_next_cursor = next_after;
                } else if let Some(next) = next_after {
                    closing.scan_cursor = Some(next);
                    self.store_queries_inflight += 1;
                    output.commands.push(StoreCommand::Rows {
                        epoch_ms,
                        reason,
                        after: Some(next),
                        keys: None,
                        limit: self.ctx.cfg.output.max_records_per_ticket,
                        recovery_ticket: None,
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

    fn on_tickets_persisted(
        &mut self,
        ticket_ids: Vec<u64>,
        now_ms: i64,
        output: &mut WorkerOutput<F>,
    ) {
        self.persist_tickets_inflight = self.persist_tickets_inflight.saturating_sub(1);

        let mut close_epochs = Vec::new();
        for ticket_id in ticket_ids {
            let Some(ticket) = self.ledger.get(ticket_id) else {
                continue;
            };
            if ticket.reason == EmitReason::Close && !close_epochs.contains(&ticket.window_epoch_ms)
            {
                close_epochs.push(ticket.window_epoch_ms);
            }
            let Some(rows) = self.pending_rows.remove(&ticket_id) else {
                continue;
            };
            let Ok(records) = records_for(
                ticket,
                &rows,
                &self.resources,
                &self.ctx.emitter_id,
                &self.ctx.clock,
                self.ctx.cfg.emit_mode,
            ) else {
                continue;
            };
            let records: Arc<[SliceRecord]> = records.into();
            self.unacked_tickets.insert(
                ticket_id,
                UnackedTicket {
                    ticket_id,
                    first_emitted_ms: now_ms,
                    records: Arc::clone(&records),
                    backoff_ms: 5_000,
                    next_retry_ms: None,
                    spill_in_flight: false,
                },
            );
            self.enqueue_or_emit_paced(EmitBatch { ticket_id, records }, now_ms, output);
        }

        for epoch_ms in close_epochs {
            if let Some(window) = self.windows.get_mut(&epoch_ms)
                && let WindowPhase::Closing(ref mut closing) = window.phase
            {
                if let Some(next) = closing.pending_next_cursor.take() {
                    closing.scan_cursor = Some(next);
                    self.store_queries_inflight += 1;
                    output.commands.push(StoreCommand::Rows {
                        epoch_ms,
                        reason: EmitReason::Close,
                        after: Some(next),
                        keys: None,
                        limit: self.ctx.cfg.output.max_records_per_ticket,
                        recovery_ticket: None,
                    });
                } else if !closing.scan_complete {
                    closing.scan_complete = true;
                    if !self.ledger.has_inflight_for_window(epoch_ms) {
                        window.transition_to_closed(now_ms);
                    }
                }
            }
        }
    }

    fn on_ack_applied(&mut self, ticket_id: u64, evicted_keys: Vec<(u128, u32)>) {
        self.pending_rows.remove(&ticket_id);
        if let Some(epoch_ms) = self.evict_ack_epochs.remove(&ticket_id)
            && let Some(window) = self.windows.get_mut(&epoch_ms)
        {
            for (digest, scope_id) in evicted_keys {
                let key = StorageKey24 { epoch_ms, digest };
                if !self.double_buf.contains_key_active_or_inflight(&key) {
                    window.dirty_keys.remove(&key);
                    window.guard.forget(&mut window.known, digest, scope_id);
                    self.stats.keys_evicted_total += 1;
                }
            }
        }
    }

    fn on_flush_failed(
        &mut self,
        batch: FlushBatch,
        finalizers: Vec<F>,
        output: &mut WorkerOutput<F>,
    ) {
        self.double_buf.flush_completed();
        self.drain_in_progress = false;
        self.double_buf
            .active_mut()
            .restage_resources(batch.new_resources, batch.resource_refs);
        self.offset_tracker.rollback_flush(&batch.offsets);
        for entry in &batch.entries {
            if entry.is_new
                && !self.double_buf.active().contains_key(&entry.key)
                && let Some(window) = self.windows.get_mut(&entry.key.epoch_ms)
            {
                window.dirty_keys.remove(&entry.key);
                window.late_dirty_keys.remove(&entry.key);
                if entry.key.is_overflow() {
                    window.overflow_scopes.remove(&entry.resource_scope_id);
                } else {
                    window.guard.forget(
                        &mut window.known,
                        entry.key.digest,
                        entry.resource_scope_id,
                    );
                }
            }
        }
        for window in self.windows.values_mut() {
            window.periodic_flush_pending = false;
            window.late_flush_pending = false;
            if window.evict_flush_pending {
                window.evict_flush_pending = false;
                window.evict_in_progress = false;
            }
        }
        output.failed_release.extend(finalizers);
    }

    fn on_persist_tickets_failed(&mut self, ticket_ids: Vec<u64>, output: &mut WorkerOutput<F>) {
        self.persist_tickets_inflight = self.persist_tickets_inflight.saturating_sub(1);
        for ticket_id in ticket_ids {
            self.pending_rows.remove(&ticket_id);
            if let Some(ticket) = self.ledger.resolve(ticket_id) {
                match ticket.reason {
                    EmitReason::Close => {
                        if let Some(window) = self.windows.get_mut(&ticket.window_epoch_ms)
                            && let WindowPhase::Closing(ref mut closing) = window.phase
                        {
                            closing.pending_next_cursor = None;
                            closing.scan_started = false;
                            closing.scan_complete = false;
                        }
                    }
                    EmitReason::Periodic => {
                        if let Some(window) = self.windows.get_mut(&ticket.window_epoch_ms) {
                            for entry in &ticket.entries {
                                window.dirty_keys.insert(StorageKey24 {
                                    epoch_ms: ticket.window_epoch_ms,
                                    digest: entry.digest,
                                });
                            }
                            window.last_periodic_emit_ms = 0;
                        }
                    }
                    EmitReason::LateRevision => {
                        if let Some(window) = self.windows.get_mut(&ticket.window_epoch_ms) {
                            for entry in &ticket.entries {
                                window.late_dirty_keys.insert(StorageKey24 {
                                    epoch_ms: ticket.window_epoch_ms,
                                    digest: entry.digest,
                                });
                            }
                            window.last_late_emit_ms = 0;
                        }
                    }
                    EmitReason::Evict => {
                        if let Some(window) = self.windows.get_mut(&ticket.window_epoch_ms) {
                            window.evict_in_progress = false;
                            window.last_evict_ms = 0;
                        }
                    }
                    EmitReason::Drain => {
                        let keys: Vec<StorageKey24> = ticket
                            .entries
                            .iter()
                            .map(|e| StorageKey24 {
                                epoch_ms: ticket.window_epoch_ms,
                                digest: e.digest,
                            })
                            .collect();
                        if !keys.is_empty() {
                            self.store_queries_inflight += 1;
                            output.commands.push(StoreCommand::Rows {
                                epoch_ms: ticket.window_epoch_ms,
                                reason: EmitReason::Drain,
                                after: None,
                                keys: Some(keys),
                                limit: self.ctx.cfg.output.max_records_per_ticket,
                                recovery_ticket: None,
                            });
                        }
                    }
                }
            }
        }
    }

    fn on_rows_failed(
        &mut self,
        epoch_ms: u64,
        reason: EmitReason,
        after: Option<StorageKey24>,
        keys: Option<Vec<StorageKey24>>,
        recovery_ticket: Option<u64>,
        output: &mut WorkerOutput<F>,
    ) {
        self.store_queries_inflight = self.store_queries_inflight.saturating_sub(1);
        if let Some(ticket_id) = recovery_ticket {
            self.stats.recovery_failures_total += 1;
            if let Some(ticket) = self.ledger.get(ticket_id).cloned() {
                self.restored_tickets.push(ticket);
            }
        } else if reason == EmitReason::Drain {
            self.store_queries_inflight += 1;
            output.commands.push(StoreCommand::Rows {
                epoch_ms,
                reason: EmitReason::Drain,
                after,
                keys,
                limit: self.ctx.cfg.output.max_records_per_ticket,
                recovery_ticket: None,
            });
        } else if let Some(window) = self.windows.get_mut(&epoch_ms) {
            match reason {
                EmitReason::Close => {
                    if let WindowPhase::Closing(ref mut closing) = window.phase {
                        closing.scan_started = false;
                    }
                }
                EmitReason::Evict => {
                    window.evict_in_progress = false;
                    window.evict_collected_rows.clear();
                    window.last_evict_ms = 0;
                }
                EmitReason::Periodic => {
                    if let Some(k) = keys {
                        window.dirty_keys.extend(k);
                        window.last_periodic_emit_ms = 0;
                    }
                }
                EmitReason::LateRevision => {
                    if let Some(k) = keys {
                        window.late_dirty_keys.extend(k);
                        window.last_late_emit_ms = 0;
                    }
                }
                EmitReason::Drain => {}
            }
        }
    }

    fn on_spill_done(
        &mut self,
        ticket_id: u64,
        spill_files: usize,
        disk_bytes: u64,
        now_ms: i64,
        output: &mut WorkerOutput<F>,
    ) {
        self.cached_spill_files = spill_files;
        self.set_disk_bytes(disk_bytes);
        if self.unacked_tickets.remove(&ticket_id).is_some() {
            self.spilled_tickets.insert(ticket_id);
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

    fn on_spill_replay(
        &mut self,
        entries: Vec<(u64, Vec<SliceRecord>)>,
        spill_files: usize,
        quarantined: u64,
        output: &mut WorkerOutput<F>,
    ) {
        self.replay_spill_in_flight = false;
        self.cached_spill_files = spill_files;
        self.stats.spill_quarantined_total += quarantined;
        for (ticket_id, records) in entries {
            self.spilled_tickets.insert(ticket_id);
            output.emit.push(EmitBatch {
                ticket_id,
                records: records.into(),
            });
        }
    }

    fn on_spill_failed(&mut self, ticket_id: Option<u64>) {
        if let Some(id) = ticket_id {
            if let Some(unacked) = self.unacked_tickets.get_mut(&id) {
                unacked.spill_in_flight = false;
            }
        } else {
            self.replay_spill_in_flight = false;
        }
    }

    fn on_store_failed(&mut self) {
        self.store_queries_inflight = 0;
        self.persist_tickets_inflight = 0;
        self.replay_spill_in_flight = false;
    }
}

#[cfg(test)]
mod tests {
    use super::{JoinedRow, coldest_first, keep_coldest};
    use crate::codec::header::NumericHeader;
    use crate::codec::plan::HeaderLayout;
    use crate::key::StorageKey24;

    fn row(last_update_s: u32, digest: u128) -> JoinedRow {
        let mut header = NumericHeader::new_zeroed(
            HeaderLayout {
                banks: 1,
                minmax_slots: 0,
            },
            1,
        );
        header.last_update_s = last_update_s;
        JoinedRow {
            key: StorageKey24 {
                epoch_ms: 0,
                digest,
            },
            header,
            tail: None,
        }
    }

    fn keys(rows: &[JoinedRow]) -> Vec<(u32, u128)> {
        rows.iter()
            .map(|r| (r.header.last_update_s, r.key.digest))
            .collect()
    }

    #[test]
    fn keep_coldest_matches_stable_sort_then_truncate() {
        // Deterministic shuffle with repeated timestamps so the digest tie-break matters.
        let mut seed = 0x9e37_79b9_u64;
        let rows: Vec<JoinedRow> = (0..200u128)
            .map(|d| {
                seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                row(
                    ((seed >> 33) % 17) as u32,
                    d.wrapping_mul(0x9e37) % 1000 + d * 1000,
                )
            })
            .collect();
        for n in [0, 1, 7, 50, 199, 200, 500] {
            let mut expected = rows.clone();
            expected.sort_by(coldest_first);
            expected.truncate(n);

            let mut got = rows.clone();
            keep_coldest(&mut got, n, true);
            assert_eq!(keys(&got), keys(&expected), "n = {n}");

            let mut unsorted = rows.clone();
            keep_coldest(&mut unsorted, n, false);
            let mut unsorted_keys = keys(&unsorted);
            unsorted_keys.sort_unstable();
            let mut expected_keys = keys(&expected);
            expected_keys.sort_unstable();
            assert_eq!(unsorted_keys, expected_keys, "n = {n} (unsorted)");
        }
    }
}
