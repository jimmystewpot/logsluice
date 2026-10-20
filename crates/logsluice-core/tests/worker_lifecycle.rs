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

#[test]
fn non_flush_failure_does_not_clear_flush_in_flight() {
    let mut h = Harness::new(|c| c.storage.l1_coalesce_max_keys = 2);
    h.ingest(&[(1, 1), (2, 2)]);
    let flush_out = h.worker().flush_l1(0);
    assert_eq!(flush_out.commands.len(), 1);

    // Fill the swapped active buffer while the first flush remains in flight.
    assert!(!h.ingest(&[(3, 3), (4, 4)]).backpressure);
    assert!(!h.worker().can_ingest());
    assert!(h.ingest(&[(5, 5), (6, 6)]).backpressure);

    // A non-flush store failure must not mark the in-flight flush as completed.
    let err_out = h.worker().on_event(
        logsluice_core::store::thread::StoreEvent::Failed(
            logsluice_core::error::AggError::Channel("non-flush failure".into()),
        ),
        0,
    );
    assert!(err_out.release.is_empty());
    assert!(err_out.failed_release.is_empty());
    assert!(!h.worker().can_ingest());
    assert!(h.ingest(&[(5, 5), (6, 6)]).backpressure);

    // Completing the flush with FlushFailed clears flush_in_flight and returns failed_release.
    let fail_out = h.worker().on_event(
        logsluice_core::store::thread::StoreEvent::FlushFailed {
            error: logsluice_core::error::AggError::Channel("flush failure".into()),
            batch: logsluice_core::store::shard_store::FlushBatch {
                entries: Vec::new(),
                offsets: Vec::new(),
                new_resources: Vec::new(),
                resource_refs: Vec::new(),
            },
            finalizers: vec![99],
        },
        0,
    );
    assert!(fail_out.release.is_empty());
    assert_eq!(fail_out.failed_release, vec![99]);
    assert!(h.worker().can_ingest());
    assert!(!h.ingest(&[(5, 5), (6, 6)]).backpressure);
}

#[test]
fn flush_failure_allows_retry_and_preserves_resources_and_unwedges_pending_flags() {
    use bytes::Bytes;
    use logsluice_core::{
        config::EmitStrategy,
        key::StorageKey24,
        position::SourcePosition,
        resource::ResourceScopePayload,
        worker::{ExemplarSource, IngestItem},
    };
    use std::sync::Arc;
    use std::time::Duration;

    let mut h = Harness::new(|c| {
        c.window.emit.strategy = EmitStrategy::Periodic;
        c.window.emit.interval = Some(Duration::from_secs(10));
        c.window.emit.close_jitter_max = Duration::ZERO;
        c.storage.l1_flush_interval = Duration::from_secs(60);
    });

    let auth_resource = Arc::new(ResourceScopePayload {
        resource: [("service.name".to_string(), serde_json::json!("auth"))].into(),
        scope_name: "auth-scope".to_string(),
        scope_version: "1.0.0".to_string(),
        scope_attrs: Default::default(),
    });
    let auth_resource_hash = auth_resource.canonical_hash();

    let make_item = || IngestItem {
        key: StorageKey24 {
            epoch_ms: 0,
            digest: 42,
        },
        resource: Arc::clone(&auth_resource),
        resource_hash: auth_resource_hash,
        position: Some(SourcePosition {
            topic: Arc::from("t"),
            partition: 0,
            offset: 1,
        }),
        event_ms: 1_000,
        counters: vec![[1, 0, 0, 0]],
        minmax_values: vec![1_000, 1_000],
        exemplar: ExemplarSource {
            severity: 9,
            body: Some(Bytes::from_static(b"hello")),
            dims: vec![],
            attrs: vec![],
        },
    };

    let r1 = h.worker().ingest(vec![make_item()], 10, 1_000);
    assert_eq!(r1.accepted, 1);
    assert_eq!(r1.replayed, 0);

    // Trigger periodic flush at t=10s, which fails.
    h.fail_next_flush();
    h.advance(10_000, None);
    assert!(h.drain_emitted().is_empty());

    // Re-ingesting the exact same item after FlushFailed must be accepted (not dropped as replay)
    // and must re-capture the exemplar tail.
    let r2 = h.worker().ingest(vec![make_item()], 11, 10_000);
    assert_eq!(r2.accepted, 1);
    assert_eq!(r2.replayed, 0);

    // Next periodic cadence at t=20s must flush and emit (not wedged by periodic_flush_pending).
    h.advance(20_000, None);
    let emitted = h.drain_emitted();
    assert_eq!(emitted.len(), 1);
    assert_eq!(emitted[0].emit_reason, EmitReason::Periodic);
    assert_eq!(
        emitted[0].resource.resource.get("service.name"),
        Some(&serde_json::json!("auth"))
    );
    assert_eq!(emitted[0].tail.body.as_deref(), Some(b"hello".as_slice()));

    // Restart before acking to verify the resource and offset were durably persisted to ShardStore.
    let mut h = h.restart();
    let recovered_emitted = h.drain_emitted();
    assert_eq!(recovered_emitted.len(), 1);
    assert_eq!(
        recovered_emitted[0].resource.resource.get("service.name"),
        Some(&serde_json::json!("auth"))
    );
    assert_eq!(
        recovered_emitted[0].tail.body.as_deref(),
        Some(b"hello".as_slice())
    );
}
