//! Clock-tick phases of [`ShardWorker::poll`](crate::worker::ShardWorker::poll).
//!
//! `poll` was one function: drain deferred store commands, then advance every window.
//! The call order is the state machine. Each phase observes flags and commands produced
//! earlier in the same tick, so these steps stay in this order.
//!
//! Disk threshold, the paced emit queue, and restored recovery tickets still run first,
//! through helpers that already lived beside `poll`. `schedule_closing_spills` spills
//! unacked tickets past `max_closing_duration`. `retry_unacked_tickets` re-emits batches
//! whose backoff is due. `schedule_spill_replay` replays spill files on the store thread.
//! `periodic_l1_flush`, `schedule_periodic_emission`, `schedule_late_revisions`, and
//! `schedule_cold_eviction` arm the interval flush and the periodic, late, and evict scans.
//! `transition_active_to_closing` closes windows past the watermark or max delay.
//! `process_closing_windows` flushes, then issues the close scan. `purge_closed_windows`
//! drops a closed window only after lateness and only with no ticket still in flight.

use crate::config::{EmitStrategy, OnCap};
use crate::emission::EmitReason;
use crate::key::StorageKey24;
use crate::store::thread::StoreCommand;
use crate::worker::lifecycle::{WindowPhase, close_jitter_ms};
use crate::worker::{EmitBatch, SPILL_RETRY_INTERVAL_MS, ShardWorker, WorkerOutput};

impl<F> ShardWorker<F> {
    /// Advances the worker clock and triggers periodic flush, closing transitions, and purging.
    pub fn poll(&mut self, watermark: Option<i64>, now_ms: i64) -> WorkerOutput<F> {
        let mut output = WorkerOutput::default();
        output.commands.extend(self.pending_commands.drain(..));

        self.check_disk_threshold();
        self.drain_paced_queue(now_ms, &mut output);
        if !self.restored_tickets.is_empty() {
            self.drain_restored_tickets(&mut output);
        }

        self.schedule_closing_spills(now_ms, &mut output);
        self.retry_unacked_tickets(now_ms, &mut output);
        self.schedule_spill_replay(now_ms, &mut output);
        self.periodic_l1_flush(now_ms, &mut output);
        self.schedule_periodic_emission(now_ms, &mut output);
        self.schedule_late_revisions(now_ms, &mut output);
        self.schedule_cold_eviction(now_ms, &mut output);
        self.transition_active_to_closing(watermark, now_ms);
        self.process_closing_windows(now_ms, &mut output);
        self.purge_closed_windows(watermark, now_ms, &mut output);

        output
    }

    fn schedule_closing_spills(&mut self, now_ms: i64, output: &mut WorkerOutput<F>) {
        // 0a. Check unacknowledged tickets for max_closing_duration spill
        let max_closing_duration_ms = self.ctx.cfg.window.max_closing_duration.as_millis() as i64;
        let mut to_spill = Vec::new();
        for (&ticket_id, unacked) in &self.unacked_tickets {
            if !unacked.spill_in_flight
                && now_ms.saturating_sub(unacked.first_emitted_ms) >= max_closing_duration_ms
            {
                to_spill.push(ticket_id);
            }
        }
        to_spill.sort_unstable();

        for ticket_id in to_spill {
            if let Some(unacked) = self.unacked_tickets.get_mut(&ticket_id) {
                unacked.spill_in_flight = true;
                output.commands.push(StoreCommand::Spill {
                    ticket_id,
                    records: unacked.records.clone(),
                });
            }
        }
    }

    fn retry_unacked_tickets(&mut self, now_ms: i64, output: &mut WorkerOutput<F>) {
        // 0b. Retry backoff for unacknowledged tickets
        for unacked in self.unacked_tickets.values_mut() {
            if !unacked.spill_in_flight
                && let Some(retry_ms) = unacked.next_retry_ms
                && now_ms >= retry_ms
            {
                unacked.next_retry_ms = None;
                output.emit.push(EmitBatch {
                    ticket_id: unacked.ticket_id,
                    records: unacked.records.clone(),
                });
            }
        }
    }

    fn schedule_spill_replay(&mut self, now_ms: i64, output: &mut WorkerOutput<F>) {
        // 0c. Periodic spill replay
        let spill_replay_due = if self.last_spill_replay_ms == 0 {
            now_ms >= SPILL_RETRY_INTERVAL_MS
        } else {
            now_ms.saturating_sub(self.last_spill_replay_ms) >= SPILL_RETRY_INTERVAL_MS
        };
        if spill_replay_due {
            self.last_spill_replay_ms = now_ms;
            if self.cached_spill_files > 0 && !self.replay_spill_in_flight {
                self.replay_spill_in_flight = true;
                output.commands.push(StoreCommand::ReplaySpill);
            }
        }
    }

    fn periodic_l1_flush(&mut self, now_ms: i64, output: &mut WorkerOutput<F>) {
        // 1. Periodic L1 flush check
        let flush_interval_ms = self.ctx.cfg.storage.l1_flush_interval.as_millis() as i64;
        if now_ms.saturating_sub(self.last_flush_ms) >= flush_interval_ms
            && !self.double_buf.active().is_empty()
            && let Some(swapped) = self.double_buf.try_swap()
        {
            self.last_flush_ms = now_ms;
            output.commands.push(StoreCommand::Flush(swapped));
        }
    }

    fn schedule_periodic_emission(&mut self, now_ms: i64, output: &mut WorkerOutput<F>) {
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
                            recovery_ticket: None,
                        });
                    }
                }
            }
        }
    }

    fn schedule_late_revisions(&mut self, now_ms: i64, output: &mut WorkerOutput<F>) {
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
                        recovery_ticket: None,
                    });
                }
            }
        }
    }

    fn schedule_cold_eviction(&mut self, now_ms: i64, output: &mut WorkerOutput<F>) {
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
                            recovery_ticket: None,
                        });
                    }
                }
            }
        }
    }

    fn transition_active_to_closing(&mut self, watermark: Option<i64>, now_ms: i64) {
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
    }

    fn process_closing_windows(&mut self, now_ms: i64, output: &mut WorkerOutput<F>) {
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
                        after: closing.scan_cursor,
                        keys: None,
                        limit: self.ctx.cfg.output.max_records_per_ticket,
                        recovery_ticket: None,
                    });
                }
            }
        }
    }

    fn purge_closed_windows(
        &mut self,
        watermark: Option<i64>,
        now_ms: i64,
        output: &mut WorkerOutput<F>,
    ) {
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
    }
}
