#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

mod common;
use common::Harness;
use logsluice_core::{codec::ticket::EmitReason, config::*};
use std::time::Duration;

const DAY: i64 = 86_400_000;
const M15: i64 = 900_000;

#[test]
fn test_periodic_delta_sum_exact() {
    let mut h = Harness::new(|c| {
        c.window.emit = EmitConfig {
            strategy: EmitStrategy::Periodic,
            interval: Some(Duration::from_secs(900)),
            close_jitter_max: Duration::ZERO,
        };
    });
    let mut truth = 0;
    for i in 0..3 {
        h.ingest(&[(1, i * M15 + 1), (1, i * M15 + 2)]);
        truth += 2;
        h.advance((i + 1) * M15, Some(i * M15 + 3));
        h.ack_all();
    }
    h.advance(DAY + 31_000, Some(DAY));
    h.ack_all();
    let out = h.all_emitted();
    assert_eq!(h.count_for(&out, 1), truth);
    assert_eq!(
        out.iter()
            .filter(|r| r.emit_reason == EmitReason::Periodic)
            .count(),
        3
    );
    assert!(out.iter().all(|r| r.emit_reason != EmitReason::Close)); // nothing left at close
}

#[test]
fn late_revision_emitted_on_cadence() {
    let mut h = Harness::new(|c| c.window.emit.close_jitter_max = Duration::ZERO);
    h.ingest(&[(1, 1_000)]);
    h.advance(DAY + 31_000, Some(DAY));
    h.ack_all();
    h.ingest(&[(1, 2_000)]); // late, within lateness
    h.advance(DAY + 31_000 + 300_000, Some(DAY + 10));
    let late: Vec<_> = h
        .drain_emitted()
        .into_iter()
        .filter(|r| r.emit_reason == EmitReason::LateRevision)
        .collect();
    assert_eq!(h.count_for(&late, 1), 1);
}

#[test]
fn test_evict_cold_lossless() {
    let mut h = Harness::new(|c| {
        c.cardinality.max_keys_per_window = 10;
        c.cardinality.max_resource_share = 1.0;
        c.cardinality.on_cap = OnCap::EvictCold;
        c.window.emit.close_jitter_max = Duration::ZERO;
    });
    for i in 0..6i64 {
        // chunks spaced 2 min apart (> evict_interval)
        let base = i * 120_000;
        let chunk: Vec<(u128, i64)> = (1..=5).map(|j| ((i * 5 + j) as u128, base + j)).collect();
        h.ingest(&chunk);
        h.advance(base + 120_000, None);
        h.ack_all();
    }
    h.advance(DAY + 31_000, Some(DAY));
    h.ack_all();
    let out = h.all_emitted();
    assert_eq!(h.total_count(&out), 30); // evicted + overflow + close slices, lossless
    assert!(h.stats().keys_evicted_total > 0);
}

#[test]
fn test_disk_reserve_no_deadlock() {
    let mut h = Harness::new(|c| {
        c.storage.max_disk_bytes = 1_000;
        c.storage.control_reserve_bytes = 100;
        c.storage.on_disk_full = OnDiskFull::OverflowBucket;
        c.window.emit.close_jitter_max = Duration::ZERO;
    });
    h.ingest(&[(1, 1_000)]);
    h.worker().set_disk_override(Some(950));
    h.ingest(&[(2, 2_000), (1, 3_000)]); // new key → overflow; known key updates
    h.advance(DAY + 31_000, Some(DAY));
    h.ack_all();
    let out = h.all_emitted();
    assert_eq!(h.count_for(&out, 1), 2);
    assert_eq!(h.overflow_count(&out), 1);
    h.advance(DAY + 3_700_000, Some(DAY + 3_600_001));
    assert!(h.live_windows().is_empty()); // purge still succeeds
}

#[test]
fn block_policy_backpressures_new_keys_only() {
    let mut h = Harness::new(|c| {
        c.storage.max_disk_bytes = 1_000;
        c.storage.control_reserve_bytes = 100;
        c.storage.on_disk_full = OnDiskFull::Block;
    });
    h.ingest(&[(1, 1_000)]);
    h.worker().set_disk_override(Some(950));
    assert!(h.ingest(&[(2, 2_000)]).backpressure);
    assert!(!h.ingest(&[(1, 3_000)]).backpressure);
}

#[test]
fn drop_newest_policy_drops_new_keys_and_counts() {
    let mut h = Harness::new(|c| {
        c.storage.max_disk_bytes = 1_000;
        c.storage.control_reserve_bytes = 100;
        c.storage.on_disk_full = OnDiskFull::DropNewest;
        c.window.emit.close_jitter_max = Duration::ZERO;
    });
    h.ingest(&[(1, 1_000)]);
    h.worker().set_disk_override(Some(950));
    h.ingest(&[(2, 2_000), (1, 3_000)]);
    assert_eq!(h.stats().dropped_events_total, 1);
    h.advance(DAY + 31_000, Some(DAY));
    h.ack_all();
    let out = h.all_emitted();
    assert_eq!(h.count_for(&out, 1), 2);
    assert_eq!(h.count_for(&out, 2), 0);
}

#[test]
fn test_disk_recovery_hysteresis() {
    let mut h = Harness::new(|c| {
        c.storage.max_disk_bytes = 1_000;
        c.storage.control_reserve_bytes = 100;
        c.storage.on_disk_full = OnDiskFull::Block;
    });
    // threshold = 900, recovery_threshold = 810
    h.worker().set_disk_override(Some(950));
    assert!(h.ingest(&[(1, 1_000)]).backpressure);

    // At 850 (between 810 and 900), hysteresis keeps disk_full = true
    h.worker().set_disk_override(Some(850));
    assert!(h.ingest(&[(1, 1_000)]).backpressure);

    // Below 810 (e.g. 800), disk_full recovers to false
    h.worker().set_disk_override(Some(800));
    assert!(!h.ingest(&[(1, 1_000)]).backpressure);
}

#[test]
fn evict_cold_retains_key_in_known_set_when_updated_while_evict_ticket_in_flight() {
    let mut h = Harness::new(|c| {
        c.cardinality.max_keys_per_window = 10;
        c.cardinality.max_resource_share = 1.0;
        c.cardinality.on_cap = OnCap::EvictCold;
        c.cardinality.evict_high_water = 0.8;
        c.cardinality.evict_interval = Duration::from_secs(10);
        c.window.emit.close_jitter_max = Duration::ZERO;
    });

    // Ingest 8 keys at t = 1_000ms..1_070ms to reach high water (8 / 10 >= 0.8).
    let batch: Vec<(u128, i64)> = (0..8).map(|i| ((i + 1) as u128, 1_000 + i * 10)).collect();
    h.ingest(&batch);

    // Trigger EvictCold scan at t = 12_000ms (Harness does not auto-ack, so the evict ticket stays in flight).
    h.advance(12_000, None);
    let evict_records = h.drain_emitted();
    assert_eq!(evict_records.len(), 1);
    assert_eq!(evict_records[0].emit_reason, EmitReason::Evict);

    // While the evict ticket for key 1 (oldest key) is in flight, ingest 10 new events for key 1!
    let updates: Vec<(u128, i64)> = (0..10).map(|i| (1u128, 12_500 + i)).collect();
    h.ingest(&updates);

    // Ack the evict ticket. Because key 1 was updated before the ack was applied,
    // it must NOT be deleted from Fjall or forgotten from KnownKeySet!
    h.ack_all();
    assert_eq!(h.stats().keys_evicted_total, 0);

    // Close the window and verify the +10 delta for key 1 is emitted on close.
    h.advance(DAY + 31_000, Some(DAY));
    let close_recs = h.drain_emitted();
    let k0_close: Vec<_> = close_recs
        .iter()
        .filter(|r| r.digest == evict_records[0].digest && r.emit_reason == EmitReason::Close)
        .collect();
    assert_eq!(k0_close.len(), 1);
    assert_eq!(k0_close[0].counters[0][0], 10);
}

#[test]
fn retry_poll_with_known_watermark_is_not_forced_close() {
    let mut h1 = Harness::new(|c| {
        c.window.duration = Duration::from_secs(60);
        c.window.max_close_delay = Duration::from_secs(10);
        c.window.emit.close_jitter_max = Duration::ZERO;
    });
    h1.ingest(&[(1, 1_000)]);
    let window_end = 60_000;
    let now_past_max_close_delay = 71_000;
    // When watermark is at or after window_end, poll(Some(w), now) closes cleanly.
    h1.advance(now_past_max_close_delay, Some(window_end));
    assert_eq!(h1.stats().forced_closes_total, 0);

    let mut h2 = Harness::new(|c| {
        c.window.duration = Duration::from_secs(60);
        c.window.max_close_delay = Duration::from_secs(10);
        c.window.emit.close_jitter_max = Duration::ZERO;
    });
    h2.ingest(&[(1, 1_000)]);
    // When watermark is None and now >= window_end + max_close_delay, poll(None, now) forces closure.
    h2.advance(now_past_max_close_delay, None);
    assert_eq!(h2.stats().forced_closes_total, 1);
}
