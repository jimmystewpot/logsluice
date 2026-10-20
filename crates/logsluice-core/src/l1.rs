//! In-memory double-buffered L1 aggregation accumulator.
//!
//! Provides thread-local/shard-local L1 buffering with SIMD vector accumulation,
//! sticky allocation tracking, exemplar tail retention, and non-blocking buffer swapping.

use std::sync::Arc;

use hashbrown::HashMap;

use crate::codec::tail::ExemplarTail;
use crate::key::StorageKey24;
use crate::resource::ResourceScopePayload;
use crate::simd::SimdKernelTable;
use crate::store::shard_store::{FlushBatch, FlushEntry};

/// In-memory L1 buffer aggregating key counters and min/max slots before committing to shard store.
#[derive(Debug)]
pub struct L1Buffer<F> {
    entries: HashMap<StorageKey24, FlushEntry>,
    finalizers: Vec<F>,
    offsets: HashMap<(Arc<str>, i32), i64>,
    new_resources: HashMap<u32, Arc<ResourceScopePayload>>,
    resource_refs: Vec<(u32, u64)>,
}

impl<F> Default for L1Buffer<F> {
    fn default() -> Self {
        Self::new(0)
    }
}

impl<F> L1Buffer<F> {
    /// Creates a new empty `L1Buffer` pre-allocated for the specified key capacity.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::with_capacity(capacity),
            finalizers: Vec::new(),
            offsets: HashMap::new(),
            new_resources: HashMap::new(),
            resource_refs: Vec::new(),
        }
    }

    /// Accumulates counters and min/max slots into the buffer for the specified storage key.
    ///
    /// - If the key is new, allocates a new entry with initialized min/max bounds and optional tail.
    /// - If the key exists, merges counter banks with SIMD acceleration, updates min/max bounds,
    ///   updates `last_update_s` to the maximum observed, preserves sticky `is_new`, and retains the
    ///   first exemplar tail seen.
    #[allow(clippy::too_many_arguments)]
    pub fn accumulate(
        &mut self,
        key: StorageKey24,
        is_new: bool,
        resource_scope_id: u32,
        counters: &[[u64; 4]],
        minmax_values: &[i64],
        last_update_s: u32,
        tail: Option<ExemplarTail>,
        k: &SimdKernelTable,
    ) {
        match self.entries.get_mut(&key) {
            Some(entry) => {
                (k.accumulate_counter_banks)(&mut entry.delta, counters);
                (k.update_minmax_slots)(&mut entry.minmax, minmax_values);
                entry.last_update_s = entry.last_update_s.max(last_update_s);
                if is_new {
                    entry.is_new = true;
                }
                if entry.tail.is_none() && tail.is_some() {
                    entry.tail = tail;
                }
            }
            None => {
                let minmax: Vec<(i64, i64)> = minmax_values.iter().map(|&v| (v, v)).collect();
                let entry = FlushEntry {
                    key,
                    is_new,
                    resource_scope_id,
                    delta: counters.to_vec(),
                    minmax,
                    last_update_s,
                    tail,
                };
                self.entries.insert(key, entry);
            }
        }
    }

    /// Pushes a finalizer token to be completed once this buffer's contents are durably committed.
    pub fn push_finalizer(&mut self, f: F) {
        self.finalizers.push(f);
    }

    /// Records high-water mark offsets for topic-partitions to commit alongside aggregate data.
    pub fn record_offsets(&mut self, offsets: Vec<((Arc<str>, i32), i64)>) {
        for ((topic, partition), offset) in offsets {
            self.offsets
                .entry((topic, partition))
                .and_modify(|o| *o = (*o).max(offset))
                .or_insert(offset);
        }
    }

    /// Records a new resource/scope payload and links it to an active window epoch.
    pub fn record_resource(&mut self, id: u32, payload: Arc<ResourceScopePayload>, window: u64) {
        self.new_resources.insert(id, payload);
        self.resource_refs.push((id, window));
    }

    /// Records a reference from an existing resource/scope ID to a newly referenced window epoch.
    pub fn record_resource_ref(&mut self, id: u32, window: u64) {
        self.resource_refs.push((id, window));
    }

    /// Restages uncommitted resource definitions and window references from a failed flush back into this buffer.
    pub fn restage_resources(
        &mut self,
        new_resources: Vec<(u32, Arc<ResourceScopePayload>)>,
        resource_refs: Vec<(u32, u64)>,
    ) {
        for (id, payload) in new_resources {
            self.new_resources.entry(id).or_insert(payload);
        }
        self.resource_refs.extend(resource_refs);
    }

    /// Returns the number of distinct aggregation keys currently in the buffer.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns `true` if the buffer contains no aggregated keys.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns `true` if the buffer currently contains an entry for `key`.
    #[must_use]
    pub fn contains_key(&self, key: &StorageKey24) -> bool {
        self.entries.contains_key(key)
    }

    /// Converts the buffer into an atomic `FlushBatch` and associated finalizers.
    #[must_use]
    pub fn into_flush(self) -> (FlushBatch, Vec<F>) {
        let mut entries: Vec<FlushEntry> = self.entries.into_values().collect();
        entries.sort_unstable_by_key(|e| e.key);
        let mut offsets: Vec<((Arc<str>, i32), i64)> = self.offsets.into_iter().collect();
        offsets.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        let mut new_resources: Vec<(u32, Arc<ResourceScopePayload>)> =
            self.new_resources.into_iter().collect();
        new_resources.sort_unstable_by_key(|(id, _)| *id);
        let mut resource_refs = self.resource_refs;
        resource_refs.sort_unstable();

        (
            FlushBatch {
                entries,
                offsets,
                new_resources,
                resource_refs,
            },
            self.finalizers,
        )
    }
}

/// Double-buffered wrapper around `L1Buffer` supporting non-blocking swaps and in-flight tracking.
#[derive(Debug)]
pub struct DoubleBuffer<F> {
    active: L1Buffer<F>,
    capacity: usize,
    flush_in_flight: bool,
}

impl<F> Default for DoubleBuffer<F> {
    fn default() -> Self {
        Self::new(0)
    }
}

impl<F> DoubleBuffer<F> {
    /// Creates a new `DoubleBuffer` with initial active buffer capacity.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            active: L1Buffer::new(capacity),
            capacity,
            flush_in_flight: false,
        }
    }

    /// Returns a shared reference to the active `L1Buffer`.
    #[must_use]
    pub fn active(&self) -> &L1Buffer<F> {
        &self.active
    }

    /// Returns an exclusive mutable reference to the active `L1Buffer`.
    pub fn active_mut(&mut self) -> &mut L1Buffer<F> {
        &mut self.active
    }

    /// Attempts to swap the active buffer with a fresh one for flushing.
    ///
    /// Returns `None` if a flush is already in flight.
    pub fn try_swap(&mut self) -> Option<L1Buffer<F>> {
        if self.flush_in_flight {
            return None;
        }
        self.flush_in_flight = true;
        let next = L1Buffer::new(self.capacity);
        Some(std::mem::replace(&mut self.active, next))
    }

    /// Signals that the in-flight flush operation has completed, allowing subsequent swaps.
    pub fn flush_completed(&mut self) {
        self.flush_in_flight = false;
    }

    /// Returns `true` if a flush operation is currently in flight.
    #[must_use]
    pub fn flush_in_flight(&self) -> bool {
        self.flush_in_flight
    }
}
