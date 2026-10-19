//! O(1) key membership tracking and resource fair-share admission control.
//!
//! Provides `KnownKeySet` for tracking active key digests in memory and storage,
//! and `CardinalityGuard` to enforce shard key limits and fair-share allocation across
//! interned resource scopes.

use hashbrown::hash_map::Entry;
use hashbrown::{HashMap, HashSet};
use serde::{Deserialize, Serialize};

/// Reason why a key admission was denied and routed to overflow.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OverflowReason {
    /// The shard's maximum key capacity has been reached, or new keys are frozen.
    ShardCap,
    /// The resource scope exceeded its fair-share allocation of active keys.
    ResourceShare,
}

/// Admission decision for an incoming record key.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Admission {
    /// The key is already known and tracked in the shard.
    Known,
    /// The key is newly admitted into the shard.
    New,
    /// The key exceeds cardinality limits and must overflow.
    Overflow(OverflowReason),
}

/// O(1) set of known 128-bit key digests active in memory or storage.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KnownKeySet {
    keys: HashSet<u128>,
}

impl KnownKeySet {
    /// Pre-allocates a `KnownKeySet` with capacity for `n` digests.
    #[must_use]
    pub fn with_capacity(n: usize) -> Self {
        Self {
            keys: HashSet::with_capacity(n),
        }
    }

    /// Returns `true` if the set contains the given key digest.
    #[must_use]
    pub fn contains(&self, digest: &u128) -> bool {
        self.keys.contains(digest)
    }

    /// Inserts a key digest into the set.
    ///
    /// Returns `true` if the digest was not present, or `false` if it was already present.
    pub fn insert(&mut self, digest: u128) -> bool {
        self.keys.insert(digest)
    }

    /// Removes a key digest from the set.
    ///
    /// Returns `true` if the digest was present in the set.
    pub fn remove(&mut self, digest: &u128) -> bool {
        self.keys.remove(digest)
    }

    /// Returns the number of distinct key digests in the set.
    #[must_use]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Returns `true` if the set contains no digests.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Returns an iterator over the key digests.
    pub fn iter(&self) -> impl Iterator<Item = &u128> {
        self.keys.iter()
    }
}

/// Admission controller for key cardinality and resource fair-share.
#[derive(Clone, Debug, PartialEq)]
pub struct CardinalityGuard {
    max_keys: usize,
    per_resource_limit: usize,
    frozen: bool,
    resource_counts: HashMap<u32, usize>,
}

impl CardinalityGuard {
    /// Creates a new `CardinalityGuard`.
    ///
    /// The per-resource limit is computed as `floor(max_keys * max_resource_share)`, clamped to a minimum of 1.
    #[must_use]
    pub fn new(max_keys: usize, max_resource_share: f64) -> Self {
        let per_resource_limit = (max_keys as f64 * max_resource_share).floor() as usize;
        let per_resource_limit = per_resource_limit.max(1);

        Self {
            max_keys,
            per_resource_limit,
            frozen: false,
            resource_counts: HashMap::new(),
        }
    }

    /// Maximum active keys allowed across all resources in this shard.
    #[must_use]
    pub fn max_keys(&self) -> usize {
        self.max_keys
    }

    /// Maximum active keys allowed for any single resource scope.
    #[must_use]
    pub fn per_resource_limit(&self) -> usize {
        self.per_resource_limit
    }

    /// Returns whether the guard is currently frozen against admitting new keys.
    #[must_use]
    pub fn is_frozen(&self) -> bool {
        self.frozen
    }

    /// Returns the current active key count for a given resource scope.
    #[must_use]
    pub fn resource_count(&self, resource_scope_id: u32) -> usize {
        self.resource_counts
            .get(&resource_scope_id)
            .copied()
            .unwrap_or(0)
    }

    /// Freezes or unfreezes new key admissions (used during disk-full overflow handling).
    pub fn freeze_new_keys(&mut self, frozen: bool) {
        self.frozen = frozen;
    }

    /// Admits an incoming key digest for the given resource scope.
    ///
    /// Strict evaluation order:
    /// 1. If key is already in `known` -> `Admission::Known`.
    /// 2. If guard is `frozen` -> `Admission::Overflow(OverflowReason::ShardCap)`.
    /// 3. If `known.len() >= max_keys` -> `Admission::Overflow(OverflowReason::ShardCap)`.
    /// 4. If resource count >= `per_resource_limit` -> `Admission::Overflow(OverflowReason::ResourceShare)`.
    /// 5. Otherwise, key is admitted: inserted into `known`, resource count incremented, returns `Admission::New`.
    pub fn admit(
        &mut self,
        known: &mut KnownKeySet,
        digest: u128,
        resource_scope_id: u32,
    ) -> Admission {
        if known.contains(&digest) {
            return Admission::Known;
        }

        if self.frozen {
            return Admission::Overflow(OverflowReason::ShardCap);
        }

        if known.len() >= self.max_keys {
            return Admission::Overflow(OverflowReason::ShardCap);
        }

        let resource_count = self
            .resource_counts
            .get(&resource_scope_id)
            .copied()
            .unwrap_or(0);
        if resource_count >= self.per_resource_limit {
            return Admission::Overflow(OverflowReason::ResourceShare);
        }

        known.insert(digest);
        *self.resource_counts.entry(resource_scope_id).or_insert(0) += 1;
        Admission::New
    }

    /// Forgets a key digest on eviction, updating both `known` and the resource scope count.
    ///
    /// If the key was present in `known`, the resource count is decremented (and cleaned up if 0).
    pub fn forget(&mut self, known: &mut KnownKeySet, digest: u128, resource_scope_id: u32) {
        if known.remove(&digest)
            && let Entry::Occupied(mut entry) = self.resource_counts.entry(resource_scope_id)
        {
            if *entry.get() <= 1 {
                entry.remove();
            } else {
                *entry.get_mut() -= 1;
            }
        }
    }

    /// Sets or updates the active key count for a resource scope (e.g., during recovery/restore).
    pub fn restore(&mut self, resource_scope_id: u32, count: usize) {
        if count == 0 {
            self.resource_counts.remove(&resource_scope_id);
        } else {
            self.resource_counts.insert(resource_scope_id, count);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_guard_getters_and_properties() {
        let mut guard = CardinalityGuard::new(100, 0.2);
        assert_eq!(guard.max_keys(), 100);
        assert_eq!(guard.per_resource_limit(), 20);
        assert!(!guard.is_frozen());
        assert_eq!(guard.resource_count(1), 0);

        guard.freeze_new_keys(true);
        assert!(guard.is_frozen());
        guard.freeze_new_keys(false);
        assert!(!guard.is_frozen());

        guard.restore(42, 15);
        assert_eq!(guard.resource_count(42), 15);
        guard.restore(42, 0);
        assert_eq!(guard.resource_count(42), 0);
    }

    #[test]
    fn test_known_key_set_iter_and_default() {
        let mut set = KnownKeySet::default();
        assert!(set.is_empty());
        set.insert(12345);
        set.insert(67890);
        let items: Vec<u128> = set.iter().copied().collect();
        assert_eq!(items.len(), 2);
        assert!(items.contains(&12345));
        assert!(items.contains(&67890));
    }

    #[test]
    fn test_serde_admission_and_reasons() {
        let reasons = [OverflowReason::ShardCap, OverflowReason::ResourceShare];
        for reason in reasons {
            let serialized = serde_json::to_string(&reason).expect("serialize reason");
            let deserialized: OverflowReason =
                serde_json::from_str(&serialized).expect("deserialize reason");
            assert_eq!(reason, deserialized);
        }

        let admissions = [
            Admission::Known,
            Admission::New,
            Admission::Overflow(OverflowReason::ShardCap),
            Admission::Overflow(OverflowReason::ResourceShare),
        ];
        for admission in admissions {
            let serialized = serde_json::to_string(&admission).expect("serialize admission");
            let deserialized: Admission =
                serde_json::from_str(&serialized).expect("deserialize admission");
            assert_eq!(admission, deserialized);
        }
    }
}
