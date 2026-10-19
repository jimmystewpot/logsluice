#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

use logsluice_core::cardinality::*;

#[test]
fn test_resource_fair_share_and_shard_cap() {
    let mut known = KnownKeySet::with_capacity(8);
    let mut g = CardinalityGuard::new(8, 0.25); // per-resource limit = 2
    assert_eq!(g.admit(&mut known, 1, 7), Admission::New);
    assert_eq!(g.admit(&mut known, 1, 7), Admission::Known);
    assert_eq!(g.admit(&mut known, 2, 7), Admission::New);
    assert_eq!(
        g.admit(&mut known, 3, 7),
        Admission::Overflow(OverflowReason::ResourceShare)
    );
    for (d, r) in [(10, 1), (11, 1), (20, 2), (21, 2), (30, 3), (31, 3)] {
        assert_eq!(g.admit(&mut known, d, r), Admission::New);
    }
    assert_eq!(known.len(), 8);
    assert_eq!(
        g.admit(&mut known, 40, 4),
        Admission::Overflow(OverflowReason::ShardCap)
    );
    assert_eq!(g.admit(&mut known, 10, 1), Admission::Known); // known keys always admitted
    g.forget(&mut known, 10, 1);
    assert_eq!(g.admit(&mut known, 40, 4), Admission::New);
}

#[test]
fn frozen_guard_overflows_new_keys_only() {
    let mut known = KnownKeySet::with_capacity(4);
    let mut g = CardinalityGuard::new(4, 1.0);
    assert_eq!(g.admit(&mut known, 1, 1), Admission::New);
    g.freeze_new_keys(true);
    assert_eq!(
        g.admit(&mut known, 2, 1),
        Admission::Overflow(OverflowReason::ShardCap)
    );
    assert_eq!(g.admit(&mut known, 1, 1), Admission::Known);
    g.freeze_new_keys(false);
    assert_eq!(g.admit(&mut known, 2, 1), Admission::New);
}

#[test]
fn test_restore_and_forget_edge_cases() {
    let mut known = KnownKeySet::with_capacity(4);
    let mut g = CardinalityGuard::new(4, 0.5); // per-resource limit = 2

    // Restore count directly
    g.restore(99, 2);
    assert_eq!(
        g.admit(&mut known, 1, 99),
        Admission::Overflow(OverflowReason::ResourceShare)
    );

    // Forget a non-existent key should not alter resource count
    g.forget(&mut known, 1, 99);
    assert_eq!(
        g.admit(&mut known, 1, 99),
        Admission::Overflow(OverflowReason::ResourceShare)
    );

    // Restore with lower count allows admission
    g.restore(99, 1);
    assert_eq!(g.admit(&mut known, 1, 99), Admission::New);
    assert_eq!(
        g.admit(&mut known, 2, 99),
        Admission::Overflow(OverflowReason::ResourceShare)
    );

    // Forget key 1 actually present decrements count
    g.forget(&mut known, 1, 99);
    assert_eq!(g.admit(&mut known, 2, 99), Admission::New);
}

#[test]
fn test_known_key_set_methods() {
    let mut known = KnownKeySet::with_capacity(10);
    assert!(known.is_empty());
    assert_eq!(known.len(), 0);

    assert!(known.insert(42));
    assert!(!known.insert(42));
    assert!(known.contains(&42));
    assert!(!known.contains(&43));
    assert_eq!(known.len(), 1);
    assert!(!known.is_empty());

    assert!(known.remove(&42));
    assert!(!known.remove(&42));
    assert_eq!(known.len(), 0);
    assert!(known.is_empty());
}

#[test]
fn test_check_order_priority() {
    let mut known = KnownKeySet::with_capacity(2);
    let mut g = CardinalityGuard::new(2, 0.5); // per_resource_limit = 1

    assert_eq!(g.admit(&mut known, 100, 1), Admission::New);
    assert_eq!(g.admit(&mut known, 200, 2), Admission::New);
    assert_eq!(known.len(), 2);

    // Shard is full, resource 1 limit reached, and guard is frozen
    g.freeze_new_keys(true);

    // 1. Known key is admitted even if frozen and cap exceeded
    assert_eq!(g.admit(&mut known, 100, 1), Admission::Known);

    // 2. Frozen check takes precedence over shard cap / resource share
    assert_eq!(
        g.admit(&mut known, 300, 1),
        Admission::Overflow(OverflowReason::ShardCap)
    );

    g.freeze_new_keys(false);

    // 3. Shard cap takes precedence over resource share
    assert_eq!(
        g.admit(&mut known, 300, 1),
        Admission::Overflow(OverflowReason::ShardCap)
    );
}

#[test]
fn test_per_resource_limit_minimum_one() {
    let mut known = KnownKeySet::with_capacity(10);
    // 10 * 0.01 = 0.1 -> floor is 0 -> clamped to 1
    let mut g = CardinalityGuard::new(10, 0.01);
    assert_eq!(g.admit(&mut known, 1, 1), Admission::New);
    assert_eq!(
        g.admit(&mut known, 2, 1),
        Admission::Overflow(OverflowReason::ResourceShare)
    );
}
