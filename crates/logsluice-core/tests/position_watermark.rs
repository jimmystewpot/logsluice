#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

use logsluice_core::{position::*, watermark::*};
use std::{collections::HashMap, sync::Arc};

const NOW: i64 = 1_000_000_000_000;
fn cfg() -> WatermarkConfig {
    WatermarkConfig {
        allowed_skew_ms: 30_000,
        max_future_drift_ms: 300_000,
        idle_timeout_ms: 300_000,
        caught_up_threshold_ms: 120_000,
        seed_grace_ms: 7_200_000,
        max_tracked_sources: 2,
    }
}
fn p(n: i32) -> SourceKey {
    SourceKey::Partition("t".into(), n)
}

#[test]
fn test_offset_hwm_drops_replay() {
    let mut t = OffsetTracker::from_persisted(HashMap::from([(("t".into(), 0), 100)]));
    let pos = |o| SourcePosition {
        topic: Arc::from("t"),
        partition: 0,
        offset: o,
    };
    assert!(t.is_replay(&pos(100)) && !t.is_replay(&pos(101)));
    t.observe(&pos(150));
    assert!(t.is_replay(&pos(120)));
    assert_eq!(t.take_pending(), vec![(("t".into(), 0), 150)]);
    assert!(t.take_pending().is_empty());
}

#[test]
fn lagging_partition_holds_watermark_even_when_quiet() {
    let mut w = WatermarkTracker::new(cfg(), NOW);
    assert_eq!(w.observe(p(0), NOW - 1_000, NOW), Admit::Accept);
    w.observe(p(1), NOW - 3_600_000, NOW);
    assert_eq!(w.watermark(NOW), Some(NOW - 3_600_000 - 30_000));
    assert_eq!(w.watermark(NOW + 600_000), Some(NOW - 3_600_000 - 30_000));
}

#[test]
fn caught_up_quiet_partition_is_excluded() {
    let mut w = WatermarkTracker::new(cfg(), NOW);
    w.observe(p(0), NOW - 1_000, NOW);
    w.observe(p(1), NOW - 2_000, NOW);
    w.observe(p(0), NOW + 399_000, NOW + 400_000);
    assert_eq!(w.watermark(NOW + 400_000), Some(NOW + 399_000 - 30_000));
    assert_eq!(
        w.watermark(NOW + 800_000),
        Some(NOW + 800_000 - 30_000 - 120_000)
    ); // all idle
}

#[test]
fn future_drift_quarantined_and_does_not_advance() {
    let mut w = WatermarkTracker::new(cfg(), NOW);
    w.observe(p(0), NOW - 1_000, NOW);
    assert_eq!(w.observe(p(0), NOW + 300_001, NOW), Admit::Quarantine);
    assert_eq!(w.watermark(NOW), Some(NOW - 31_000));
}

#[test]
fn seeded_unseen_partition_blocks_until_grace() {
    let mut w = WatermarkTracker::new(cfg(), NOW);
    w.seed_known([p(0)]);
    w.observe(p(1), NOW - 1_000, NOW);
    assert_eq!(w.watermark(NOW), None);
    // after seed grace p0 is dropped; p1 is by then idle-and-caught-up too → all-idle rule applies
    assert_eq!(
        w.watermark(NOW + 7_200_001),
        Some(NOW + 7_200_001 - 30_000 - 120_000)
    );
}

#[test]
fn source_cap_maps_to_default() {
    let mut w = WatermarkTracker::new(cfg(), NOW);
    for s in ["a", "b", "c"] {
        w.observe(SourceKey::Source(s.into()), NOW - 10_000, NOW);
    }
    assert_eq!(w.tracked_len(), 3); // a, b, Default
}
