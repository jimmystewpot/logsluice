//! Resource and instrumentation scope interning with window reference counting.
//!
//! Provides deterministic canonical hashing, attribute exclusion filtering,
//! collision-safe interning of resource/scope payloads, and garbage collection
//! keyed on active window epochs.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use hashbrown::HashMap;
use hashbrown::hash_map::Entry;
use serde::{Deserialize, Serialize};

use crate::error::{AggError, AggResult};

/// Payload describing OpenTelemetry resource and instrumentation scope metadata.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceScopePayload {
    /// Resource attributes (e.g. `service.name`, `host.name`).
    pub resource: BTreeMap<String, serde_json::Value>,
    /// Instrumentation scope name.
    pub scope_name: String,
    /// Instrumentation scope version.
    pub scope_version: String,
    /// Instrumentation scope attributes.
    pub scope_attrs: BTreeMap<String, serde_json::Value>,
}

impl ResourceScopePayload {
    /// Creates a clone with specified resource attribute keys excluded.
    ///
    /// This collapses high-churn pod/container attributes so that recurring
    /// services share interned IDs across pod restarts.
    #[must_use]
    pub fn filtered(&self, exclude: &BTreeSet<String>) -> Self {
        let mut resource = self.resource.clone();
        resource.retain(|k, _| !exclude.contains(k));
        Self {
            resource,
            scope_name: self.scope_name.clone(),
            scope_version: self.scope_version.clone(),
            scope_attrs: self.scope_attrs.clone(),
        }
    }

    /// Computes deterministic 64-bit canonical hash using XXH3.
    ///
    /// The hash is computed over deterministic JSON serialization, guaranteed by
    /// `BTreeMap` sorting for resource and scope attribute maps.
    ///
    /// Returns 0 in the unexpected event of serialization failure without panicking.
    #[must_use]
    pub fn canonical_hash(&self) -> u64 {
        match serde_json::to_vec(self) {
            Ok(bytes) => xxhash_rust::xxh3::xxh3_64(&bytes),
            Err(_) => 0,
        }
    }
}

/// An entry in the resource scope table tracking payload, hash, and referencing windows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResourceEntry {
    /// Shared payload pointer.
    pub payload: Arc<ResourceScopePayload>,
    /// Precomputed canonical hash of the payload.
    pub hash: u64,
    /// Set of active window epoch start timestamps referencing this payload.
    pub windows: BTreeSet<u64>,
}

/// Table interning `ResourceScopePayload` instances with collision resolution and window GC.
#[derive(Clone, Debug)]
pub struct ResourceScopeTable {
    next_id: u32,
    by_hash: HashMap<u64, Vec<u32>>,
    entries: HashMap<u32, ResourceEntry>,
}

impl Default for ResourceScopeTable {
    fn default() -> Self {
        Self::new()
    }
}

impl ResourceScopeTable {
    /// Creates a new empty resource scope table.
    #[must_use]
    pub fn new() -> Self {
        Self {
            next_id: 1,
            by_hash: HashMap::new(),
            entries: HashMap::new(),
        }
    }

    /// Returns the number of interned resource entries currently tracked.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns `true` if the table contains no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns a reference to the entries map.
    #[must_use]
    pub fn entries(&self) -> &HashMap<u32, ResourceEntry> {
        &self.entries
    }

    /// Returns an iterator over all `(id, ResourceEntry)` pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&u32, &ResourceEntry)> {
        self.entries.iter()
    }

    /// Looks up a resource payload by its interned ID.
    #[must_use]
    pub fn get(&self, id: u32) -> Option<&Arc<ResourceScopePayload>> {
        self.entries.get(&id).map(|e| &e.payload)
    }

    /// Interns a resource payload for the given window epoch timestamp.
    ///
    /// Looks up candidate entries matching `hash`. If an existing entry has an
    /// identical payload, `window_epoch_ms` is added to its referencing windows and
    /// `(existing_id, false)` is returned.
    ///
    /// If no match exists, allocates a new ID, adds the entry, indexes it in `by_hash`,
    /// and returns `(new_id, true)`.
    ///
    /// If the `u32` ID space is completely exhausted, returns `(0, false)`.
    pub fn intern(
        &mut self,
        payload: &Arc<ResourceScopePayload>,
        hash: u64,
        window_epoch_ms: u64,
    ) -> (u32, bool) {
        self.try_intern(payload, hash, window_epoch_ms)
            .unwrap_or_default()
    }

    /// Interns a resource payload for the given window epoch timestamp, returning
    /// `(scope_id, is_new_resource, is_new_window_ref)`.
    ///
    /// If the `u32` ID space is completely exhausted, returns `(0, false, false)`.
    pub fn intern_with_window_ref(
        &mut self,
        payload: &Arc<ResourceScopePayload>,
        hash: u64,
        window_epoch_ms: u64,
    ) -> (u32, bool, bool) {
        self.try_intern_with_window_ref(payload, hash, window_epoch_ms)
            .unwrap_or_default()
    }

    /// Fallible version of `intern` returning an `AggError` on ID exhaustion.
    ///
    /// # Errors
    /// Returns `AggError::Config` if the `u32` resource ID space is exhausted.
    pub fn try_intern(
        &mut self,
        payload: &Arc<ResourceScopePayload>,
        hash: u64,
        window_epoch_ms: u64,
    ) -> AggResult<(u32, bool)> {
        let (id, is_new_resource, _) =
            self.try_intern_with_window_ref(payload, hash, window_epoch_ms)?;
        Ok((id, is_new_resource))
    }

    /// Fallible version of `intern_with_window_ref` returning `(scope_id, is_new_resource, is_new_window_ref)`.
    ///
    /// # Errors
    /// Returns `AggError::Config` if the `u32` resource ID space is exhausted.
    pub fn try_intern_with_window_ref(
        &mut self,
        payload: &Arc<ResourceScopePayload>,
        hash: u64,
        window_epoch_ms: u64,
    ) -> AggResult<(u32, bool, bool)> {
        // Fast path: check candidate IDs under `hash`.
        if let Some(candidates) = self.by_hash.get(&hash) {
            for &candidate_id in candidates {
                if let Some(entry) = self.entries.get_mut(&candidate_id)
                    && *entry.payload == **payload
                {
                    let is_new_window_ref = entry.windows.insert(window_epoch_ms);
                    return Ok((candidate_id, false, is_new_window_ref));
                }
            }
        }

        // Allocate a new ID safely avoiding collision or overflow.
        let new_id = self.allocate_next_id()?;

        let mut windows = BTreeSet::new();
        windows.insert(window_epoch_ms);

        let entry = ResourceEntry {
            payload: Arc::clone(payload),
            hash,
            windows,
        };

        self.entries.insert(new_id, entry);
        self.by_hash.entry(hash).or_default().push(new_id);

        Ok((new_id, true, true))
    }

    /// Releases a window reference across all entries and evicts those with zero remaining references.
    ///
    /// Returns the list of evicted IDs, sorted in ascending order.
    pub fn release_window(&mut self, window_epoch_ms: u64) -> Vec<u32> {
        let mut evicted = Vec::new();
        let mut evicted_hashes = Vec::new();

        self.entries.retain(|&id, entry| {
            entry.windows.remove(&window_epoch_ms);
            if entry.windows.is_empty() {
                evicted.push(id);
                evicted_hashes.push((id, entry.hash));
                false
            } else {
                true
            }
        });

        // Clean up evicted IDs from by_hash indexing.
        for (id, hash) in evicted_hashes {
            if let Entry::Occupied(mut occ) = self.by_hash.entry(hash) {
                occ.get_mut().retain(|&candidate_id| candidate_id != id);
                if occ.get().is_empty() {
                    occ.remove();
                }
            }
        }

        evicted.sort_unstable();
        evicted
    }

    /// Restores persisted state into the table.
    ///
    /// Inserts the entry and updates the hash index, advancing `next_id` so that
    /// subsequent allocations do not collide with restored IDs.
    pub fn restore(&mut self, id: u32, payload: ResourceScopePayload, windows: Vec<u64>) {
        let hash = payload.canonical_hash();
        let entry = ResourceEntry {
            payload: Arc::new(payload),
            hash,
            windows: windows.into_iter().collect(),
        };

        self.entries.insert(id, entry);

        let list = self.by_hash.entry(hash).or_default();
        if !list.contains(&id) {
            list.push(id);
        }

        self.next_id = self.next_id.max(id.saturating_add(1));
    }

    /// Allocates the next available monotonic `u32` ID.
    fn allocate_next_id(&mut self) -> AggResult<u32> {
        let start = if self.next_id == 0 { 1 } else { self.next_id };
        let mut cand = start;

        loop {
            if cand != 0 && !self.entries.contains_key(&cand) {
                self.next_id = if cand == u32::MAX { 1 } else { cand + 1 };
                return Ok(cand);
            }

            cand = if cand == u32::MAX { 1 } else { cand + 1 };
            if cand == start {
                return Err(AggError::Config("resource id space exhausted".to_string()));
            }
        }
    }
}
