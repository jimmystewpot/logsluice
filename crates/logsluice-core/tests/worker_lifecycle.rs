#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

mod common;
use common::Harness;
use logsluice_core::codec::ticket::EmitReason;

const DAY: i64 = 86_400_000;

#[test]
fn test_on_close_single_slice() {
    let mut h = Harness::new(|c| {
        c.window.emit.close_jitter_max = std::time::Duration::ZERO;
    });
    h.ingest(&[(1, 1_000), (1, 2_000), (1, 3_000), (2, 4_000)]); // (key, event_ms) in window [0, DAY)
    h.advance(DAY + 31_000, Some(DAY)); // watermark reaches end
    let out = h.drain_emitted();
    assert_eq!(out.len(), 2);
    assert!(out.iter().all(|r| r.emit_reason == EmitReason::Close));
    assert_eq!(h.count_for(&out, 1), 3);
    assert_eq!(h.count_for(&out, 2), 1);
    h.ack_all();
    h.advance(DAY + 3_700_000, Some(DAY + 3_600_001)); // past lateness
    assert!(h.live_windows().is_empty());
    assert_eq!(h.horizon(), DAY);
}

#[test]
fn test_forced_close_stuck_partition() {
    let mut h = Harness::new(|c| {
        c.window.emit.close_jitter_max = std::time::Duration::ZERO;
    });
    h.ingest(&[(1, 1_000)]);
    h.advance(DAY + 7_199_000, None);
    assert!(h.drain_emitted().is_empty());
    h.advance(DAY + 7_200_000, None);
    assert_eq!(h.drain_emitted().len(), 1);
    assert_eq!(h.stats().forced_closes_total, 1);
}

#[test]
fn close_waits_for_jitter() {
    let j = logsluice_core::worker::close_jitter_ms("pod-0", 0, 600_000);
    let mut h = Harness::new(|_| {});
    h.ingest(&[(1, 1_000)]);
    h.advance(DAY, Some(DAY));
    if j > 0 {
        assert!(h.drain_emitted().is_empty());
    }
    h.advance(DAY + j, Some(DAY));
    assert_eq!(h.drain_emitted().len(), 1);
}

#[test]
fn finalizers_released_after_flush_interval() {
    let mut h = Harness::new(|_| {});
    h.ingest_with_finalizer(&[(1, 1_000)], 42);
    h.advance(29_000, None);
    assert!(h.released().is_empty());
    h.advance(30_000, None);
    assert_eq!(h.released(), vec![42]);
}

#[test]
fn backpressure_when_l1_full_and_flush_busy() {
    let mut h = Harness::new(|c| c.storage.l1_coalesce_max_keys = 2);
    h.ingest(&[(1, 1), (2, 2)]);
    h.hold_store(); // store thread paused
    assert!(!h.ingest(&[(3, 3), (4, 4)]).backpressure); // swap succeeds, flush in flight
    assert!(h.ingest(&[(5, 5), (6, 6)]).backpressure);
    h.release_store();
    h.advance(1, None);
    assert!(!h.ingest(&[(5, 5), (6, 6)]).backpressure);
}

#[test]
fn poll_purges_out_of_order_windows_keep_max_horizon() {
    let mut h = Harness::new(|c| {
        c.window.emit.close_jitter_max = std::time::Duration::ZERO;
        c.window.allowed_lateness = std::time::Duration::from_millis((2 * DAY) as u64);
    });
    h.ingest(&[(1, 1_000), (2, DAY + 1_000)]);
    h.advance(2 * DAY + 31_000, Some(2 * DAY));
    assert_eq!(h.drain_emitted().len(), 2);
    h.ack_all();

    // Ingest a late event into the older closed window [0, DAY) so it has an in-flight
    // LateRevision ticket when the watermark advances past the purge threshold for both windows.
    h.ingest(&[(1, 2_000)]);
    h.advance(2 * DAY + 331_000, Some(2 * DAY));
    assert_eq!(h.drain_emitted().len(), 1);

    // Newer closed window [DAY, 2 * DAY) is purged first; older closed window [0, DAY) waits on ack.
    h.advance(2 * DAY + 332_000, Some(4 * DAY + 1));
    assert_eq!(h.live_windows(), vec![0]);
    assert_eq!(h.horizon(), 2 * DAY);

    // Acking and polling purges the older closed window [0, DAY) last; horizon must not regress.
    h.ack_all();
    h.advance(2 * DAY + 333_000, Some(4 * DAY + 1));
    assert!(h.live_windows().is_empty());
    assert_eq!(h.horizon(), 2 * DAY);
}
