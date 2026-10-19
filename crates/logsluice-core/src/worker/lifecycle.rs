//! Window lifecycle state machine and phase transition management.
//!
//! Enforces deterministic lifecycle progression:
//! `Active` -> `Closing` -> `Closed` -> `Purged`.

use hashbrown::HashSet;

use crate::cardinality::{CardinalityGuard, KnownKeySet};
use crate::key::StorageKey24;
use crate::store::shard_store::JoinedRow;

/// Computes deterministic window close jitter in milliseconds.
///
/// Uses XXH3 64-bit hash of `emitter_id` concatenated with big-endian `shard`.
/// Returns 0 when `max_ms <= 0`.
#[must_use]
pub fn close_jitter_ms(emitter_id: &str, shard: u16, max_ms: i64) -> i64 {
    if max_ms <= 0 {
        return 0;
    }
    let mut buf = Vec::with_capacity(emitter_id.len() + 2);
    buf.extend_from_slice(emitter_id.as_bytes());
    buf.extend_from_slice(&shard.to_be_bytes());
    let hash = xxhash_rust::xxh3::xxh3_64(&buf);
    let divisor = (max_ms as u64).saturating_add(1);
    (hash % divisor) as i64
}

/// Internal state while a window is in the `Closing` phase.
#[derive(Clone, Debug, PartialEq)]
pub struct ClosingState {
    /// Timestamp in epoch milliseconds when window entered the `Closing` phase.
    pub transition_ms: i64,
    /// Earliest timestamp in epoch milliseconds when storage scan emission may start.
    pub emission_start_ms: i64,
    /// Whether window closure was forced by wall-clock timeout rather than watermark.
    pub forced: bool,
    /// Whether the mandatory pre-emission L1 buffer flush has completed.
    pub flush_done: bool,
    /// Storage scan resumption cursor. `None` if scan has not started or completed.
    pub scan_cursor: Option<StorageKey24>,
    /// Whether initial row scan request has been dispatched.
    pub scan_started: bool,
    /// Whether all rows pages have been completely scanned.
    pub scan_complete: bool,
}

/// Current lifecycle phase of an aggregate window.
#[derive(Clone, Debug, PartialEq)]
pub enum WindowPhase {
    /// Window is actively accepting new items and aggregating in memory/storage.
    Active,
    /// Window boundary has closed; waiting for jitter, flushing, and scanning storage.
    Closing(ClosingState),
    /// Window scan and emission are complete; late events may revise totals.
    Closed,
}

/// Complete state tracking for an individual aggregation window.
#[derive(Clone, Debug, PartialEq)]
pub struct WindowState {
    /// Window start timestamp in epoch milliseconds.
    pub epoch_ms: u64,
    /// Set of known key digests tracked in this window.
    pub known: KnownKeySet,
    /// Admission guard enforcing cardinality limits for this window.
    pub guard: CardinalityGuard,
    /// Current lifecycle phase.
    pub phase: WindowPhase,
    /// Set of resource scope IDs whose overflow sentinel key has been created in this window.
    pub overflow_scopes: HashSet<u32>,
    /// Dirty storage keys pending periodic emission.
    pub dirty_keys: HashSet<StorageKey24>,
    /// Last periodic emission timestamp in milliseconds.
    pub last_periodic_emit_ms: i64,
    /// Whether L1 flush is in flight before periodic scan.
    pub periodic_flush_pending: bool,
    /// Dirty storage keys received after window closure pending late revision emission.
    pub late_dirty_keys: HashSet<StorageKey24>,
    /// Last late revision emission timestamp in milliseconds.
    pub last_late_emit_ms: i64,
    /// Whether L1 flush is in flight before late revision scan.
    pub late_flush_pending: bool,
    /// Last cold eviction check timestamp in milliseconds.
    pub last_evict_ms: i64,
    /// Whether cold key eviction scan is currently in progress.
    pub evict_in_progress: bool,
    /// Whether L1 flush is in flight before cold eviction scan.
    pub evict_flush_pending: bool,
    /// Rows collected during paged scan for cold eviction candidate selection.
    pub evict_collected_rows: Vec<JoinedRow>,
}

impl WindowState {
    /// Creates a new `WindowState` in the `Active` phase.
    #[must_use]
    pub fn new(epoch_ms: u64, max_keys: usize, max_resource_share: f64) -> Self {
        Self {
            epoch_ms,
            known: KnownKeySet::with_capacity(1024),
            guard: CardinalityGuard::new(max_keys, max_resource_share),
            phase: WindowPhase::Active,
            overflow_scopes: HashSet::new(),
            dirty_keys: HashSet::new(),
            last_periodic_emit_ms: epoch_ms as i64,
            periodic_flush_pending: false,
            late_dirty_keys: HashSet::new(),
            last_late_emit_ms: epoch_ms as i64,
            late_flush_pending: false,
            last_evict_ms: epoch_ms as i64,
            evict_in_progress: false,
            evict_flush_pending: false,
            evict_collected_rows: Vec::new(),
        }
    }

    /// Returns `true` if the window is currently in the `Active` phase.
    #[must_use]
    pub fn is_active(&self) -> bool {
        matches!(self.phase, WindowPhase::Active)
    }

    /// Returns `true` if the window is currently in the `Closing` phase.
    #[must_use]
    pub fn is_closing(&self) -> bool {
        matches!(self.phase, WindowPhase::Closing(_))
    }

    /// Returns `true` if the window is currently in the `Closed` phase.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        matches!(self.phase, WindowPhase::Closed)
    }

    /// Transitions window from `Active` to `Closing` with the specified emission delay.
    pub fn transition_to_closing(&mut self, now_ms: i64, jitter_ms: i64, forced: bool) {
        if self.is_active() {
            let emission_start_ms = now_ms.saturating_add(jitter_ms);
            self.phase = WindowPhase::Closing(ClosingState {
                transition_ms: now_ms,
                emission_start_ms,
                forced,
                flush_done: false,
                scan_cursor: None,
                scan_started: false,
                scan_complete: false,
            });
        }
    }

    /// Transitions window from `Closing` to `Closed`.
    pub fn transition_to_closed(&mut self, now_ms: i64) {
        if self.is_closing() {
            self.phase = WindowPhase::Closed;
            self.last_late_emit_ms = now_ms;
        }
    }
}
