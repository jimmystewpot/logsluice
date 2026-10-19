//! Shard store state recovery on startup.
//!
//! Reconstructs in-memory window metadata, key sets, cardinality limits, interned resources,
//! offset high-water marks, and unacknowledged emission tickets directly from disk tables.

use std::sync::Arc;

use hashbrown::{HashMap, HashSet};

use crate::cardinality::{CardinalityGuard, KnownKeySet};
use crate::codec::ticket::{EmitReason, Ticket};
use crate::emission::TicketLedger;
use crate::error::AggResult;
use crate::key::StorageKey24;
use crate::position::OffsetTracker;
use crate::resource::ResourceScopeTable;
use crate::store::shard_store::ShardStore;
use crate::worker::WorkerContext;
use crate::worker::lifecycle::{ClosingState, WindowPhase, WindowState};

/// Recovered state restored from storage on startup.
#[derive(Clone, Debug)]
pub struct RecoveredState {
    /// Restored window lifecycle states keyed by window epoch millisecond.
    pub windows: HashMap<u64, WindowState>,
    /// Restored interned resource scope metadata table.
    pub resources: ResourceScopeTable,
    /// Restored consumer partition offset high-water marks.
    pub offset_tracker: OffsetTracker,
    /// Restored emission ticket ledger tracking in-flight keys.
    pub ledger: TicketLedger,
    /// Restored unacknowledged tickets.
    pub tickets: Vec<Ticket>,
    /// Sorted list of all known source stream partitions.
    pub known_partitions: Vec<(Arc<str>, i32)>,
    /// Number of pending disk spill files found on startup.
    pub spill_files: usize,
    /// Resource IDs that had no remaining live window references during recovery.
    pub released_resources: Vec<u32>,
}

impl Default for RecoveredState {
    fn default() -> Self {
        Self::empty()
    }
}

impl RecoveredState {
    /// Creates an empty recovered state placeholder.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            windows: HashMap::new(),
            resources: ResourceScopeTable::new(),
            offset_tracker: OffsetTracker::default(),
            ledger: TicketLedger::new(1),
            tickets: Vec::new(),
            known_partitions: Vec::new(),
            spill_files: 0,
            released_resources: Vec::new(),
        }
    }

    /// Loads and rebuilds state from a persisted `ShardStore`.
    ///
    /// # Errors
    /// Returns [`AggError`] if reading store tables fails.
    pub fn load(store: &ShardStore, ctx: &WorkerContext) -> AggResult<Self> {
        let offsets = store.offsets()?;
        let mut known_partitions: Vec<(Arc<str>, i32)> = offsets.keys().cloned().collect();
        known_partitions.sort_unstable();
        let offset_tracker = OffsetTracker::from_persisted(offsets.into_iter().collect());

        let live_epochs = store.live_windows();
        let live_set: HashSet<u64> = live_epochs.iter().copied().collect();

        let mut resources = ResourceScopeTable::new();
        let mut released_resources = Vec::new();
        let mut pruned_resources = Vec::new();
        for (id, payload, windows) in store.resources()? {
            let orig_len = windows.len();
            let live_refs: Vec<u64> = windows
                .into_iter()
                .filter(|w| live_set.contains(w))
                .collect();
            if live_refs.is_empty() {
                released_resources.push(id);
            } else {
                if live_refs.len() != orig_len {
                    pruned_resources.push((id, payload.clone(), live_refs.clone()));
                }
                resources.restore(id, payload, live_refs);
            }
        }
        released_resources.sort_unstable();
        if !released_resources.is_empty() {
            store.delete_resources(&released_resources)?;
        }
        if !pruned_resources.is_empty() {
            store.update_resources(&pruned_resources)?;
        }

        let tickets = store.tickets()?;
        let mut ledger = TicketLedger::new(1);
        ledger.restore(tickets.clone());

        let mut windows = HashMap::new();
        for epoch_ms in live_epochs {
            let window_keys = store.window_keys(epoch_ms)?;
            let mut known = KnownKeySet::with_capacity(window_keys.len());
            let mut guard = CardinalityGuard::new(
                ctx.cfg.cardinality.max_keys_per_window,
                ctx.cfg.cardinality.max_resource_share,
            );
            let mut resource_counts: HashMap<u32, usize> = HashMap::new();
            let mut overflow_scopes = HashSet::new();

            for (digest, scope_id) in window_keys {
                let key = StorageKey24 { epoch_ms, digest };
                if key.is_overflow() {
                    overflow_scopes.insert((key.digest & 0xFFFF_FFFF) as u32);
                } else {
                    known.insert(digest);
                    *resource_counts.entry(scope_id).or_insert(0) += 1;
                }
            }

            for (scope_id, count) in resource_counts {
                guard.restore(scope_id, count);
            }

            let mut window = WindowState::new(
                epoch_ms,
                ctx.cfg.cardinality.max_keys_per_window,
                ctx.cfg.cardinality.max_resource_share,
            );
            window.known = known;
            window.guard = guard;
            window.overflow_scopes = overflow_scopes;

            let has_close_ticket = tickets
                .iter()
                .any(|t| t.window_epoch_ms == epoch_ms && t.reason == EmitReason::Close);
            if has_close_ticket {
                window.phase = WindowPhase::Closing(ClosingState {
                    transition_ms: 0,
                    emission_start_ms: 0,
                    forced: false,
                    flush_done: true,
                    scan_cursor: None,
                    pending_next_cursor: None,
                    scan_started: true,
                    scan_complete: true,
                });
            }

            windows.insert(epoch_ms, window);
        }

        let spill_files =
            crate::worker::spill::SpillWriter::new(store.shard_dir().join("spill")).count_files();

        Ok(Self {
            windows,
            resources,
            offset_tracker,
            ledger,
            tickets,
            known_partitions,
            spill_files,
            released_resources,
        })
    }

    /// Returns all known `(topic, partition)` pairs found in the recovered offsets.
    #[must_use]
    pub fn known_partitions(&self) -> Vec<(Arc<str>, i32)> {
        self.known_partitions.clone()
    }
}
