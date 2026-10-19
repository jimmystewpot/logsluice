//! Deterministic slice identifiers, emission ticket ledger, rate pacer, and slice records.

use std::sync::Arc;

use hashbrown::{HashMap, HashSet};
use serde::{Deserialize, Serialize};

use crate::codec::tail::ExemplarTail;
pub use crate::codec::ticket::{EmitReason, Ticket, TicketEntry};
use crate::config::EmitMode;
use crate::error::{AggError, AggResult};
use crate::key::StorageKey24;
use crate::resource::{ResourceScopePayload, ResourceScopeTable};
use crate::simd::SimdKernelTable;
use crate::store::shard_store::JoinedRow;
use crate::window::WindowClock;

/// Generates a deterministic aggregate slice identifier.
///
/// Format: `<emitter_id>:<window_epoch_ms>:<digest_hex_32>:<slice_seq>`
#[inline]
#[must_use]
pub fn slice_id(emitter_id: &str, window_epoch_ms: u64, digest: u128, slice_seq: u32) -> String {
    format!("{emitter_id}:{window_epoch_ms}:{digest:032x}:{slice_seq}")
}

/// Emission ticket ledger tracking tickets and in-flight storage keys.
#[derive(Clone, Debug)]
pub struct TicketLedger {
    next_id: u64,
    tickets: HashMap<u64, Ticket>,
    inflight: HashSet<StorageKey24>,
}

impl TicketLedger {
    /// Creates a new ticket ledger with the initial ticket identifier.
    #[must_use]
    pub fn new(next_id: u64) -> Self {
        Self {
            next_id,
            tickets: HashMap::new(),
            inflight: HashSet::new(),
        }
    }

    /// Returns `true` if `key` is currently tracked as in flight.
    #[must_use]
    pub fn is_inflight(&self, key: &StorageKey24) -> bool {
        self.inflight.contains(key)
    }

    /// Returns the number of storage keys currently in flight.
    #[must_use]
    pub fn inflight_len(&self) -> usize {
        self.inflight.len()
    }

    /// Returns a reference to the ticket with the given identifier, if tracked.
    #[must_use]
    pub fn get(&self, ticket_id: u64) -> Option<&Ticket> {
        self.tickets.get(&ticket_id)
    }

    /// Returns `true` if there are any tickets currently tracked in flight for `window_epoch_ms`.
    #[must_use]
    pub fn has_inflight_for_window(&self, window_epoch_ms: u64) -> bool {
        self.tickets
            .values()
            .any(|t| t.window_epoch_ms == window_epoch_ms)
    }

    /// Returns the number of tickets currently tracked.
    #[must_use]
    pub fn ticket_count(&self) -> usize {
        self.tickets.len()
    }

    /// Builds emission tickets for `rows` skipping zero-delta and in-flight keys.
    ///
    /// Marked keys are added to the in-flight set, and tickets are chunked up to
    /// `max_per_ticket` entries.
    pub fn build(
        &mut self,
        epoch_ms: u64,
        reason: EmitReason,
        rows: &[JoinedRow],
        k: &SimdKernelTable,
        max_per_ticket: usize,
    ) -> Vec<Ticket> {
        let mut entries = Vec::new();

        for row in rows {
            let key = StorageKey24 {
                epoch_ms,
                digest: row.key.digest,
            };

            if self.is_inflight(&key) {
                continue;
            }

            let mut delta = vec![[0u64; 4]; row.header.total.len()];
            let any_non_zero =
                (k.diff_counter_banks)(&row.header.total, &row.header.emitted, &mut delta);

            if !any_non_zero && reason != EmitReason::Evict {
                continue;
            }

            self.inflight.insert(key);
            entries.push(TicketEntry {
                digest: row.key.digest,
                slice_seq: row.header.slice_seq,
                delta,
                minmax: row.header.minmax.clone(),
            });
        }

        if entries.is_empty() {
            return Vec::new();
        }

        let chunk_size = max_per_ticket.max(1);
        let mut tickets = Vec::with_capacity(entries.len().div_ceil(chunk_size));

        for chunk in entries.chunks(chunk_size) {
            let ticket_id = self.next_id;
            self.next_id = self.next_id.saturating_add(1);

            let ticket = Ticket {
                id: ticket_id,
                window_epoch_ms: epoch_ms,
                reason,
                entries: chunk.to_vec(),
            };

            self.tickets.insert(ticket_id, ticket.clone());
            tickets.push(ticket);
        }

        tickets
    }

    /// Resolves and removes a ticket by `ticket_id`, releasing its in-flight keys.
    pub fn resolve(&mut self, ticket_id: u64) -> Option<Ticket> {
        let ticket = self.tickets.remove(&ticket_id)?;
        for entry in &ticket.entries {
            let key = StorageKey24 {
                epoch_ms: ticket.window_epoch_ms,
                digest: entry.digest,
            };
            self.inflight.remove(&key);
        }
        Some(ticket)
    }

    /// Restores persisted or unacknowledged tickets back into the ledger.
    pub fn restore(&mut self, tickets: Vec<Ticket>) {
        for ticket in tickets {
            self.next_id = self.next_id.max(ticket.id.saturating_add(1));
            for entry in &ticket.entries {
                let key = StorageKey24 {
                    epoch_ms: ticket.window_epoch_ms,
                    digest: entry.digest,
                };
                self.inflight.insert(key);
            }
            self.tickets.insert(ticket.id, ticket);
        }
    }
}

/// Token bucket rate-limiter pacing aggregate slice emissions.
///
/// Burst capacity is capped at `rate_per_sec`.
#[derive(Clone, Debug)]
pub struct Pacer {
    rate_per_sec: u32,
    tokens: f64,
    last_update_ms: Option<i64>,
}

impl Pacer {
    /// Creates a new `Pacer` configured for `rate_per_sec` tokens per second.
    #[must_use]
    pub fn new(rate_per_sec: u32) -> Self {
        Self {
            rate_per_sec,
            tokens: f64::from(rate_per_sec),
            last_update_ms: None,
        }
    }

    /// Takes up to `want` tokens available at `now_ms`.
    ///
    /// Refills tokens based on elapsed milliseconds since the last call, clamped
    /// to the burst limit of `rate_per_sec`.
    pub fn take(&mut self, want: u32, now_ms: i64) -> u32 {
        let elapsed = match self.last_update_ms {
            Some(last) => (now_ms.saturating_sub(last)).max(0),
            None => 0,
        };

        let rate = f64::from(self.rate_per_sec);
        if elapsed > 0 {
            self.tokens = (self.tokens + (elapsed as f64) * rate / 1000.0).min(rate);
        }

        let available = self.tokens.floor() as u32;
        let taken = want.min(available);
        self.tokens -= f64::from(taken);
        self.last_update_ms = Some(now_ms);

        taken
    }
}

/// An emitted slice record prepared for external serialization and delivery.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SliceRecord {
    /// Window start boundary in epoch milliseconds.
    pub window_start_ms: i64,
    /// Window end boundary (exclusive) in epoch milliseconds.
    pub window_end_ms: i64,
    /// Reason triggering this emission.
    pub emit_reason: EmitReason,
    /// Emitter pod or process identifier.
    pub emitter_id: String,
    /// 128-bit grouping digest.
    pub digest: u128,
    /// Slice sequence number for this key.
    pub slice_seq: u32,
    /// Deterministic slice identifier string.
    pub slice_id: String,
    /// Shared interned resource and instrumentation scope payload.
    pub resource: Arc<ResourceScopePayload>,
    /// Exemplar tail attributes and payload.
    pub tail: ExemplarTail,
    /// Counter values per bank (deltas in `DeltaSlice` mode, totals in `Cumulative`).
    pub counters: Vec<[u64; 4]>,
    /// Running min/max values per slot.
    pub minmax: Vec<(i64, i64)>,
}

/// Constructs `SliceRecord` instances corresponding to an emission `ticket`.
///
/// # Errors
/// Returns `AggError::Codec` if a row or resource scope referenced by an entry is missing,
/// or if `window_epoch_ms` cannot be converted to `i64`.
pub fn records_for(
    ticket: &Ticket,
    rows: &[JoinedRow],
    resources: &ResourceScopeTable,
    emitter_id: &str,
    clock: &WindowClock,
    mode: EmitMode,
) -> AggResult<Vec<SliceRecord>> {
    let row_map: HashMap<u128, &JoinedRow> = rows.iter().map(|r| (r.key.digest, r)).collect();

    let window_start_ms = i64::try_from(ticket.window_epoch_ms)
        .map_err(|_| AggError::Codec("window_epoch_ms exceeds i64".to_string()))?;
    let window_end_ms = clock.end_of(window_start_ms);

    let mut records = Vec::with_capacity(ticket.entries.len());

    for entry in &ticket.entries {
        let row = row_map.get(&entry.digest).ok_or_else(|| {
            AggError::Codec(format!("missing row for digest {:032x}", entry.digest))
        })?;

        let resource = resources
            .get(row.header.resource_scope_id)
            .ok_or_else(|| {
                AggError::Codec(format!(
                    "missing resource scope for id {}",
                    row.header.resource_scope_id
                ))
            })?
            .clone();

        let tail = row.tail.clone().unwrap_or_else(|| ExemplarTail {
            severity: 0,
            body: None,
            dims: Vec::new(),
            attrs: Vec::new(),
        });

        let counters = match mode {
            EmitMode::DeltaSlice => entry.delta.clone(),
            EmitMode::Cumulative => row.header.total.clone(),
        };

        let sid = slice_id(
            emitter_id,
            ticket.window_epoch_ms,
            entry.digest,
            entry.slice_seq,
        );

        records.push(SliceRecord {
            window_start_ms,
            window_end_ms,
            emit_reason: ticket.reason,
            emitter_id: emitter_id.to_string(),
            digest: entry.digest,
            slice_seq: entry.slice_seq,
            slice_id: sid,
            resource,
            tail,
            counters,
            minmax: entry.minmax.clone(),
        });
    }

    Ok(records)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::Bytes;

    use super::*;
    use crate::codec::{header::NumericHeader, plan::HeaderLayout, ticket::TicketEntry};
    use crate::simd::SimdMode;

    #[test]
    fn pacer_zero_rate_and_want() {
        let mut p = Pacer::new(0);
        assert_eq!(p.take(10, 100), 0);

        let mut p2 = Pacer::new(50);
        assert_eq!(p2.take(0, 100), 0);
    }

    #[test]
    fn pacer_backwards_time() {
        let mut p = Pacer::new(10);
        assert_eq!(p.take(10, 1000), 10);
        // Time goes backwards
        assert_eq!(p.take(10, 500), 0);
    }

    #[test]
    fn ticket_ledger_empty_and_zero_max_per_ticket() {
        let k = SimdKernelTable::detect(SimdMode::Scalar);
        let mut l = TicketLedger::new(5);

        // Empty rows
        assert!(l.build(0, EmitReason::Drain, &[], &k, 10).is_empty());
        assert_eq!(l.inflight_len(), 0);

        // Zero max_per_ticket handled safely (treats as chunk size 1)
        let layout = HeaderLayout {
            banks: 1,
            minmax_slots: 1,
        };
        let mut h = NumericHeader::new_zeroed(layout, 1);
        h.total[0] = [1, 0, 0, 0];
        let rows = vec![JoinedRow {
            key: StorageKey24 {
                epoch_ms: 0,
                digest: 42,
            },
            header: h,
            tail: None,
        }];
        let tickets = l.build(0, EmitReason::Drain, &rows, &k, 0);
        assert_eq!(tickets.len(), 1);
        assert_eq!(tickets[0].id, 5);
        assert_eq!(tickets[0].entries.len(), 1);

        assert!(l.resolve(999).is_none());
    }

    #[test]
    fn records_for_missing_row_or_resource_errors() {
        let clock = WindowClock::new(Duration::from_secs(60), Duration::ZERO).unwrap();
        let resources = ResourceScopeTable::new();

        let ticket = Ticket {
            id: 1,
            window_epoch_ms: 0,
            reason: EmitReason::Close,
            entries: vec![TicketEntry {
                digest: 123,
                slice_seq: 1,
                delta: vec![[1, 0, 0, 0]],
                minmax: vec![(0, 0)],
            }],
        };

        // Missing row
        let err = records_for(
            &ticket,
            &[],
            &resources,
            "emitter",
            &clock,
            EmitMode::DeltaSlice,
        )
        .unwrap_err();
        assert!(err.to_string().contains("missing row"));

        // Missing resource
        let layout = HeaderLayout {
            banks: 1,
            minmax_slots: 1,
        };
        let h = NumericHeader::new_zeroed(layout, 404);
        let rows = vec![JoinedRow {
            key: StorageKey24 {
                epoch_ms: 0,
                digest: 123,
            },
            header: h,
            tail: None,
        }];
        let err2 = records_for(
            &ticket,
            &rows,
            &resources,
            "emitter",
            &clock,
            EmitMode::DeltaSlice,
        )
        .unwrap_err();
        assert!(err2.to_string().contains("missing resource scope"));
    }

    #[test]
    fn records_for_preserves_custom_tail() {
        let clock = WindowClock::new(Duration::from_secs(60), Duration::ZERO).unwrap();
        let mut resources = ResourceScopeTable::new();
        let payload = Arc::new(ResourceScopePayload {
            resource: Default::default(),
            scope_name: "custom".to_string(),
            scope_version: "0.1".to_string(),
            scope_attrs: Default::default(),
        });
        let (res_id, _) = resources.intern(&payload, payload.canonical_hash(), 0);

        let layout = HeaderLayout {
            banks: 1,
            minmax_slots: 0,
        };
        let mut h = NumericHeader::new_zeroed(layout, res_id);
        h.total[0] = [5, 0, 0, 0];

        let custom_tail = ExemplarTail {
            severity: 9,
            body: Some(Bytes::from_static(b"log payload")),
            dims: vec![Some(Bytes::from_static(b"dim1"))],
            attrs: vec![("env".to_string(), Bytes::from_static(b"prod"))],
        };

        let rows = vec![JoinedRow {
            key: StorageKey24 {
                epoch_ms: 0,
                digest: 999,
            },
            header: h,
            tail: Some(custom_tail.clone()),
        }];

        let ticket = Ticket {
            id: 1,
            window_epoch_ms: 0,
            reason: EmitReason::Periodic,
            entries: vec![TicketEntry {
                digest: 999,
                slice_seq: 1,
                delta: vec![[5, 0, 0, 0]],
                minmax: vec![],
            }],
        };

        let recs = records_for(
            &ticket,
            &rows,
            &resources,
            "emitter-test",
            &clock,
            EmitMode::DeltaSlice,
        )
        .unwrap();

        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].tail, custom_tail);
    }
}
