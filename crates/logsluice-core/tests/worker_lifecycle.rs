#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

mod common;
use common::Harness;
use logsluice_core::codec::ticket::EmitReason;

const DAY: i64 = 86_400_000;

fn must_try_ingest(
    worker: &mut logsluice_core::worker::ShardWorker<u32>,
    items: Vec<logsluice_core::worker::IngestItem>,
    finalizer: u32,
    now_ms: i64,
) {
    if let Err((batch, _)) = worker.try_ingest(items, finalizer, now_ms) {
        panic!("try_ingest refused a batch of length {}", batch.len());
    }
}

fn try_ingest_report(
    worker: &mut logsluice_core::worker::ShardWorker<u32>,
    items: Vec<logsluice_core::worker::IngestItem>,
    finalizer: u32,
    now_ms: i64,
) -> logsluice_core::worker::IngestReport {
    match worker.try_ingest(items, finalizer, now_ms) {
        Ok(report) => report,
        Err(_) => logsluice_core::worker::IngestReport {
            accepted: 0,
            replayed: 0,
            overflowed: 0,
            backpressure: true,
        },
    }
}

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

    let r1 = try_ingest_report(h.worker(), vec![make_item()], 10, 1_000);
    assert_eq!(r1.accepted, 1);
    assert_eq!(r1.replayed, 0);

    // Trigger periodic flush at t=10s, which fails.
    h.fail_next_flush();
    h.advance(10_000, None);
    assert!(h.drain_emitted().is_empty());

    // Re-ingesting the exact same item after FlushFailed must be accepted (not dropped as replay)
    // and must re-capture the exemplar tail.
    let r2 = try_ingest_report(h.worker(), vec![make_item()], 11, 10_000);
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

#[test]
fn persist_failure_releases_ledger_and_allows_window_close() {
    let mut h = Harness::new(|c| {
        c.window.emit.close_jitter_max = std::time::Duration::ZERO;
    });
    h.ingest(&[(1, 1_000), (2, 2_000)]);

    // Arm failure for the PersistTickets command triggered by the close scan.
    h.fail_next_persist();
    h.advance(DAY + 31_000, Some(DAY));

    // Persist failed: nothing emitted, and ledger in-flight entries must be rolled back to 0.
    assert!(h.drain_emitted().is_empty());
    assert_eq!(h.stats().tickets_inflight, 0);
    assert!(!h.worker().has_inflight_tickets());

    // Subsequent poll retries the close scan, persists tickets without advancing emitted counters prematurely,
    // and allows the window to close and purge.
    h.advance(DAY + 32_000, Some(DAY));
    let out = h.drain_emitted();
    assert_eq!(out.len(), 2);
    assert_eq!(h.count_for(&out, 1), 1);
    assert_eq!(h.count_for(&out, 2), 1);

    h.ack_all();
    h.advance(DAY + 3_700_000, Some(DAY + 3_600_001));
    assert!(h.live_windows().is_empty());
    assert_eq!(h.horizon(), DAY);
}

#[test]
fn rows_failure_retries_close_scan() {
    let mut h = Harness::new(|c| {
        c.window.emit.close_jitter_max = std::time::Duration::ZERO;
    });
    h.ingest(&[(1, 1_000), (2, 2_000)]);

    // Arm failure for the Rows command triggered by the close scan.
    h.fail_next_rows();
    h.advance(DAY + 31_000, Some(DAY));
    assert!(h.drain_emitted().is_empty());

    // Because scan_started was reset on RowsFailed, the next poll retries the close scan and succeeds.
    h.advance(DAY + 32_000, Some(DAY));
    let out = h.drain_emitted();
    assert_eq!(out.len(), 2);
    assert!(out.iter().all(|r| r.emit_reason == EmitReason::Close));

    h.ack_all();
    h.advance(DAY + 3_700_000, Some(DAY + 3_600_001));
    assert!(h.live_windows().is_empty());
}

#[test]
fn rows_failure_clears_evict_in_progress() {
    let mut h = Harness::new(|c| {
        c.cardinality.max_keys_per_window = 10;
        c.cardinality.max_resource_share = 1.0;
        c.cardinality.on_cap = logsluice_core::config::OnCap::EvictCold;
        c.cardinality.evict_high_water = 0.8;
        c.cardinality.evict_interval = std::time::Duration::from_secs(10);
        c.window.emit.close_jitter_max = std::time::Duration::ZERO;
    });

    // Ingest 8 keys to reach high water (8 / 10 >= 0.8).
    let batch: Vec<(u128, i64)> = (0..8).map(|i| ((i + 1) as u128, 1_000 + i * 10)).collect();
    h.ingest(&batch);

    // Arm failure for the Evict Rows scan at t = 12_000ms.
    h.fail_next_rows();
    h.advance(12_000, None);
    assert!(h.drain_emitted().is_empty());

    // Because evict_in_progress was cleared on RowsFailed, the next eviction interval at t = 23_000ms succeeds.
    h.advance(23_000, None);
    let evict_records = h.drain_emitted();
    assert_eq!(evict_records.len(), 1);
    assert_eq!(evict_records[0].emit_reason, EmitReason::Evict);
    h.ack_all();
    assert_eq!(h.stats().keys_evicted_total, 1);
}

#[test]
fn persist_failure_restores_periodic_and_late_dirty_keys() {
    use logsluice_core::config::EmitStrategy;
    use std::time::Duration;

    let mut h = Harness::new(|c| {
        c.window.emit.strategy = EmitStrategy::Periodic;
        c.window.emit.interval = Some(Duration::from_secs(10));
        c.window.late_emit_interval = Duration::from_secs(10);
        c.window.emit.close_jitter_max = Duration::ZERO;
    });

    h.ingest(&[(1, 1_000)]);
    let flush_out = h.worker().flush_l1(1_000);
    h.pump(flush_out);

    // Trigger periodic Rows and fail PersistTickets.
    h.fail_next_persist();
    h.advance(10_000, None);
    assert!(h.drain_emitted().is_empty());

    // Next poll must re-read the restored dirty_keys and emit the periodic slice without new ingest.
    h.advance(11_000, None);
    let periodic_out = h.drain_emitted();
    assert_eq!(periodic_out.len(), 1);
    assert_eq!(periodic_out[0].emit_reason, EmitReason::Periodic);
    assert_eq!(h.count_for(&periodic_out, 1), 1);
    h.ack_all();

    // Close the window so subsequent events trigger LateRevision.
    h.advance(DAY + 31_000, Some(DAY));
    let _ = h.drain_emitted();
    h.ack_all();

    // Ingest a late event for window 0, flush, and fail PersistTickets on the LateRevision emission.
    h.ingest(&[(1, 2_000)]);
    let late_flush_out = h.worker().flush_l1(DAY + 31_000);
    h.pump(late_flush_out);

    h.fail_next_persist();
    h.advance(DAY + 42_000, Some(DAY));
    assert!(h.drain_emitted().is_empty());

    // Next poll must re-read restored late_dirty_keys and emit the LateRevision slice without new ingest.
    h.advance(DAY + 43_000, Some(DAY));
    let late_out = h.drain_emitted();
    assert_eq!(late_out.len(), 1);
    assert_eq!(late_out[0].emit_reason, EmitReason::LateRevision);
    assert_eq!(h.count_for(&late_out, 1), 1);
    h.ack_all();
}

#[test]
fn paginated_close_scan_persist_failure_resumes_failed_page() {
    use std::time::Duration;

    let mut h = Harness::new(|c| {
        c.output.max_records_per_ticket = 2;
        c.window.emit.close_jitter_max = Duration::ZERO;
    });

    h.ingest(&[(1, 1_000), (2, 2_000), (3, 3_000), (4, 4_000)]);

    // Fail PersistTickets on page 1 of the close scan.
    h.fail_next_persist();
    h.advance(DAY + 31_000, Some(DAY));
    assert!(h.drain_emitted().is_empty());

    // Next poll retries from page 1 and emits all 4 keys across both pages before closing.
    h.advance(DAY + 32_000, Some(DAY));
    let out = h.drain_emitted();
    assert_eq!(out.len(), 4);
    assert_eq!(h.count_for(&out, 1), 1);
    assert_eq!(h.count_for(&out, 2), 1);
    assert_eq!(h.count_for(&out, 3), 1);
    assert_eq!(h.count_for(&out, 4), 1);

    h.ack_all();
    h.advance(DAY + 3_700_000, Some(DAY + 3_600_001));
    assert!(h.live_windows().is_empty());
}

#[test]
fn rows_failure_restores_periodic_and_late_dirty_keys() {
    use logsluice_core::config::EmitStrategy;
    use std::time::Duration;

    let mut h = Harness::new(|c| {
        c.window.emit.strategy = EmitStrategy::Periodic;
        c.window.emit.interval = Some(Duration::from_secs(10));
        c.window.late_emit_interval = Duration::from_secs(10);
        c.window.emit.close_jitter_max = Duration::ZERO;
    });

    h.ingest(&[(1, 1_000)]);
    let flush_out = h.worker().flush_l1(1_000);
    h.pump(flush_out);

    // Fail Rows on Periodic read.
    h.fail_next_rows();
    h.advance(10_000, None);
    assert!(h.drain_emitted().is_empty());

    // Next poll re-issues Periodic Rows with restored dirty_keys and emits.
    h.advance(11_000, None);
    let periodic_out = h.drain_emitted();
    assert_eq!(periodic_out.len(), 1);
    assert_eq!(periodic_out[0].emit_reason, EmitReason::Periodic);
    assert_eq!(h.count_for(&periodic_out, 1), 1);
    h.ack_all();

    // Close the window so subsequent events trigger LateRevision.
    h.advance(DAY + 31_000, Some(DAY));
    let _ = h.drain_emitted();
    h.ack_all();

    // Ingest a late event for window 0, flush, and fail Rows on the LateRevision read.
    h.ingest(&[(1, 2_000)]);
    let late_flush_out = h.worker().flush_l1(DAY + 31_000);
    h.pump(late_flush_out);

    h.fail_next_rows();
    h.advance(DAY + 42_000, Some(DAY));
    assert!(h.drain_emitted().is_empty());

    // Next poll re-issues LateRevision Rows with restored late_dirty_keys and emits.
    h.advance(DAY + 43_000, Some(DAY));
    let late_out = h.drain_emitted();
    assert_eq!(late_out.len(), 1);
    assert_eq!(late_out[0].emit_reason, EmitReason::LateRevision);
    assert_eq!(h.count_for(&late_out, 1), 1);
    h.ack_all();
}

#[test]
fn recovery_rows_failure_retries_on_next_poll() {
    use std::time::Duration;

    let mut h = Harness::new(|c| {
        c.window.emit.close_jitter_max = Duration::ZERO;
    });

    h.ingest(&[(1, 1_000)]);
    h.advance(DAY + 31_000, Some(DAY));
    let initial = h.drain_emitted();
    assert_eq!(initial.len(), 1);

    // Restart without acking so the ticket is restored from `_tickets`.
    let mut h = h.restart_unpumped();
    let recovery_out = h.worker().recovery_output();
    h.fail_next_rows();
    h.pump(recovery_out);
    assert!(h.drain_emitted().is_empty());

    // Calling poll() re-issues the recovery Rows command and emits the recovered slice.
    h.advance(DAY + 32_000, Some(DAY));
    let recovered = h.drain_emitted();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].slice_id, initial[0].slice_id);

    h.ack_all();
    h.advance(DAY + 3_700_000, Some(DAY + 3_600_001));
    assert!(h.live_windows().is_empty());
}

#[test]
fn worker_poll_performs_no_filesystem_io_for_spill() {
    use std::sync::Arc;
    use std::time::Duration;

    use logsluice_core::{
        codec::{header::NumericHeader, plan::AggregationPlan},
        config::EngineConfig,
        key::StorageKey24,
        resource::ResourceScopePayload,
        simd::{SimdKernelTable, SimdMode},
        store::{
            shard_store::{FlushStats, JoinedRow},
            thread::{StoreCommand, StoreEvent},
        },
        window::WindowClock,
        worker::{
            ExemplarSource, IngestItem, PurgeHorizon, RecoveredState, ShardWorker, WorkerContext,
        },
    };

    let dir = tempfile::tempdir().expect("tempdir");
    let mut cfg = EngineConfig::default();
    cfg.output.emitter_id = "pod-0".to_string();
    cfg.storage.data_dir = dir.path().to_path_buf();
    cfg.window.emit.close_jitter_max = Duration::ZERO;
    cfg.window.max_closing_duration = Duration::from_secs(60);
    cfg.aggregations = common::spec_example();
    let cfg = Arc::new(cfg);

    let plan = Arc::new(AggregationPlan::new(&cfg.aggregations).expect("plan"));
    let kernels = SimdKernelTable::detect(SimdMode::Scalar);
    let clock = WindowClock::new(cfg.window.duration, cfg.window.offset).expect("clock");
    let ctx = WorkerContext {
        cfg: Arc::clone(&cfg),
        shard: 0,
        emitter_id: Arc::from("pod-0"),
        kernels,
        plan: Arc::clone(&plan),
        clock,
    };

    let mut worker: ShardWorker<u32> =
        ShardWorker::new(ctx, RecoveredState::empty(), PurgeHorizon::new(0));
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);

    let res = Arc::new(ResourceScopePayload {
        resource: [(
            "service.name".to_string(),
            serde_json::json!("test-service"),
        )]
        .into(),
        scope_name: "test-scope".to_string(),
        scope_version: "1.0.0".to_string(),
        scope_attrs: Default::default(),
    });
    let res_hash = res.canonical_hash();
    let key = StorageKey24 {
        epoch_ms: 0,
        digest: 1,
    };
    must_try_ingest(
        &mut worker,
        vec![IngestItem {
            key,
            resource: res,
            resource_hash: res_hash,
            position: None,
            event_ms: 1_000,
            counters: vec![[1, 0, 0, 0]],
            minmax_values: vec![1_000, 1_000],
            exemplar: ExemplarSource::default(),
        }],
        0,
        1_000,
    );

    // Advance to window close -> Flush
    let out1 = worker.poll(Some(DAY), DAY + 31_000);
    assert!(
        out1.commands
            .iter()
            .any(|c| matches!(c, StoreCommand::Flush(_)))
    );

    // FlushDone -> Rows
    let out2 = worker.on_event(
        StoreEvent::FlushDone {
            finalizers: vec![0],
            stats: FlushStats::default(),
            disk_bytes: 0,
        },
        DAY + 31_000,
    );
    assert!(
        out2.commands
            .iter()
            .any(|c| matches!(c, StoreCommand::Rows { .. }))
    );

    // Rows -> PersistTickets
    let mut hdr = NumericHeader::new_zeroed(plan.layout(), 1);
    hdr.total = vec![[1, 0, 0, 0]];
    hdr.minmax = vec![(1_000, 1_000), (1_000, 1_000)];
    let out3 = worker.on_event(
        StoreEvent::Rows {
            epoch_ms: 0,
            reason: EmitReason::Close,
            rows: vec![JoinedRow {
                key,
                header: hdr,
                tail: Some(common::tail()),
            }],
            next_after: None,
            recovery_ticket: None,
        },
        DAY + 31_000,
    );
    let ticket_id = out3
        .commands
        .iter()
        .find_map(|c| match c {
            StoreCommand::PersistTickets(ts) => Some(ts[0].id),
            _ => None,
        })
        .expect("PersistTickets command");

    // TicketsPersisted -> emits batch
    let out4 = worker.on_event(StoreEvent::TicketsPersisted(vec![ticket_id]), DAY + 31_000);
    assert_eq!(out4.emit.len(), 1);

    // Nack the emitted ticket and poll past max_closing_duration
    let _ = worker.on_ack(ticket_id, false, DAY + 31_000);
    let spill_out = worker.poll(Some(DAY), DAY + 92_000);

    // Tempdir must stay completely empty after poll — no filesystem I/O on worker thread!
    assert_eq!(
        std::fs::read_dir(dir.path()).unwrap().count(),
        0,
        "worker.poll must not perform filesystem I/O for spill"
    );
    assert!(
        spill_out.commands.iter().any(|c| matches!(
            c,
            StoreCommand::Spill {
                ticket_id: id,
                records,
            } if *id == ticket_id && records.len() == 1
        )),
        "expected StoreCommand::Spill, got {:?}",
        spill_out.commands
    );
}

#[test]
fn spill_failure_keeps_unacked_ticket() {
    let mut h = Harness::new(|c| {
        c.window.emit.close_jitter_max = std::time::Duration::ZERO;
        c.window.max_closing_duration = std::time::Duration::from_secs(60);
    });
    h.ingest(&[(1, 1_000)]);
    h.advance(DAY + 31_000, Some(DAY));
    let first = h.drain_emitted();
    assert_eq!(first.len(), 1);
    h.nack_all();

    // Inject spill failure on the store thread before advancing past max_closing_duration.
    h.fail_next_spill();
    h.advance(DAY + 92_000, Some(DAY));

    // Spill failed: no spill file created, and the ticket must remain unacked in the ledger.
    assert_eq!(h.spill_files(), 0);
    assert_eq!(h.stats().spill_files, 0);
    assert!(h.worker().has_inflight_tickets());
    assert_eq!(h.stats().tickets_inflight, 1);

    // Next poll retries the spill and succeeds.
    h.advance(DAY + 93_000, Some(DAY));
    assert_eq!(h.spill_files(), 1);
    assert_eq!(h.stats().spill_files, 1);
    assert!(!h.worker().has_inflight_tickets());
    assert_eq!(h.stats().tickets_inflight, 0);
}

#[test]
fn spill_file_count_is_cached_not_read_per_stats() {
    let mut h = Harness::new(|c| {
        c.window.emit.close_jitter_max = std::time::Duration::ZERO;
        c.window.max_closing_duration = std::time::Duration::from_secs(60);
    });
    h.ingest(&[(1, 1_000)]);
    h.advance(DAY + 31_000, Some(DAY));
    let _ = h.drain_emitted();
    h.nack_all();

    // Trigger spill (92s > 60s max_closing_duration).
    h.advance(DAY + 92_000, Some(DAY));
    assert_eq!(h.stats().spill_files, 1);

    // Mutate the spill directory directly on disk behind the store thread's back.
    // Because ShardWorker caches the spill file count instead of calling read_dir on every stats() call,
    // stats().spill_files must remain 1.
    let spill_dir = h.data_dir().join("shard_0").join("spill");
    std::fs::create_dir_all(&spill_dir).unwrap();
    let bogus_file = spill_dir.join("00000000000000000999.ndjson");
    std::fs::write(&bogus_file, b"{}\n").unwrap();
    assert_eq!(h.stats().spill_files, 1);
    assert_eq!(h.spill_files(), 1);
    std::fs::remove_file(&bogus_file).unwrap();

    // Replay and acknowledge the spilled ticket; cached count drops to 0.
    h.advance(DAY + 153_000, Some(DAY));
    let replayed = h.drain_emitted();
    assert_eq!(replayed.len(), 1);
    h.ack_all();
    assert_eq!(h.stats().spill_files, 0);
    assert_eq!(h.spill_files(), 0);
}

#[test]
fn spill_files_update_worker_disk_bytes_and_trigger_disk_full_policy() {
    use logsluice_core::config::OnDiskFull;
    use std::time::Duration;

    let max_disk_bytes = (64 << 20) + 100;
    let mut h = Harness::new(|c| {
        c.window.emit.close_jitter_max = Duration::ZERO;
        c.window.max_closing_duration = Duration::from_secs(60);
        c.storage.max_disk_bytes = max_disk_bytes;
        c.storage.control_reserve_bytes = 0;
        c.storage.on_disk_full = OnDiskFull::Block;
    });

    h.ingest(&[(1, 1_000)]);
    h.advance(DAY + 31_000, Some(DAY));
    let emitted = h.drain_emitted();
    assert_eq!(emitted.len(), 1);
    h.nack_all();

    let before_spill = h.worker().effective_disk_bytes();
    assert!(
        before_spill > 0 && before_spill < max_disk_bytes,
        "expected initial disk_bytes ({before_spill}) to be in (0, {max_disk_bytes})"
    );

    // Advance past max_closing_duration (60s) with an empty L1 buffer so no Flush runs;
    // the unacked ticket spills to disk on StoreThread and SpillDone updates disk_bytes.
    h.advance(DAY + 92_000, Some(DAY));
    assert_eq!(h.spill_files(), 1);

    let after_spill = h.worker().effective_disk_bytes();
    assert!(
        after_spill > before_spill,
        "expected effective_disk_bytes after spill ({after_spill}) > before_spill ({before_spill})"
    );
    assert!(
        after_spill >= max_disk_bytes,
        "expected effective_disk_bytes after spill ({after_spill}) >= max_disk_bytes ({max_disk_bytes})"
    );

    // Because spill bytes pushed disk usage past max_disk_bytes, OnDiskFull::Block must now backpressure new keys.
    assert!(h.ingest(&[(3, DAY + 2_000)]).backpressure);

    // Replay the spilled slice and acknowledge delivery; removing the spill file shrinks disk_bytes back.
    h.advance(DAY + 153_000, Some(DAY));
    assert_eq!(h.drain_emitted().len(), 1);
    h.ack_all();
    assert_eq!(h.spill_files(), 0);
    assert_eq!(h.worker().effective_disk_bytes(), before_spill);
}

#[test]
fn deferred_flushes_swap_active_buffer_when_inflight_flush_completes() {
    use logsluice_core::{
        codec::plan::AggregationPlan,
        config::{EmitStrategy, EngineConfig, OnCap},
        key::StorageKey24,
        resource::ResourceScopePayload,
        simd::{SimdKernelTable, SimdMode},
        store::thread::{StoreCommand, StoreEvent},
        window::WindowClock,
        worker::{
            ExemplarSource, IngestItem, PurgeHorizon, RecoveredState, ShardWorker, WorkerContext,
        },
    };
    use std::sync::Arc;
    use std::time::Duration;

    fn make_worker(
        modify_cfg: impl FnOnce(&mut EngineConfig),
    ) -> (
        ShardWorker<u32>,
        Arc<ResourceScopePayload>,
        u64,
        tempfile::TempDir,
    ) {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut cfg = EngineConfig::default();
        cfg.output.emitter_id = "pod-0".to_string();
        cfg.storage.data_dir = dir.path().to_path_buf();
        cfg.storage.l1_coalesce_max_keys = 100;
        cfg.storage.l1_flush_interval = Duration::from_secs(3600);
        cfg.aggregations = common::spec_example();
        modify_cfg(&mut cfg);
        let cfg = Arc::new(cfg);

        let plan = Arc::new(AggregationPlan::new(&cfg.aggregations).expect("plan"));
        let kernels = SimdKernelTable::detect(SimdMode::Scalar);
        let clock = WindowClock::new(cfg.window.duration, cfg.window.offset).expect("clock");
        let ctx = WorkerContext {
            cfg: Arc::clone(&cfg),
            shard: 0,
            emitter_id: Arc::from("pod-0"),
            kernels,
            plan,
            clock,
        };
        let worker = ShardWorker::new(ctx, RecoveredState::empty(), PurgeHorizon::new(0));

        let res = Arc::new(ResourceScopePayload {
            resource: [(
                "service.name".to_string(),
                serde_json::json!("test-service"),
            )]
            .into(),
            scope_name: "test-scope".to_string(),
            scope_version: "1.0.0".to_string(),
            scope_attrs: Default::default(),
        });
        let res_hash = res.canonical_hash();
        (worker, res, res_hash, dir)
    }

    fn make_item(
        epoch_ms: u64,
        digest: u128,
        event_ms: i64,
        res: &Arc<ResourceScopePayload>,
        res_hash: u64,
    ) -> IngestItem {
        IngestItem {
            key: StorageKey24 { epoch_ms, digest },
            resource: Arc::clone(res),
            resource_hash: res_hash,
            position: None,
            event_ms,
            counters: vec![[1, 0, 0, 0]],
            minmax_values: vec![event_ms, event_ms],
            exemplar: ExemplarSource::default(),
        }
    }

    // 1. drain_in_progress with active buffer non-empty on FlushDone (line 1003)
    {
        let (mut worker, res, res_hash, _dir) = make_worker(|c| {
            c.window.duration = Duration::from_secs(60);
        });
        must_try_ingest(
            &mut worker,
            vec![make_item(0, 1, 1_000, &res, res_hash)],
            1,
            1_000,
        );
        let out = worker.flush_l1(1_000);
        assert_eq!(out.commands.len(), 1);
        assert!(matches!(out.commands[0], StoreCommand::Flush(_)));

        must_try_ingest(
            &mut worker,
            vec![make_item(0, 2, 2_000, &res, res_hash)],
            2,
            2_000,
        );
        let out = worker.begin_drain(10_000);
        assert!(out.commands.is_empty());

        let out = worker.on_event(
            StoreEvent::FlushDone {
                finalizers: vec![1],
                stats: Default::default(),
                disk_bytes: 0,
            },
            2_000,
        );
        assert_eq!(out.commands.len(), 1);
        assert!(matches!(out.commands[0], StoreCommand::Flush(_)));

        let out = worker.on_event(
            StoreEvent::FlushDone {
                finalizers: vec![2],
                stats: Default::default(),
                disk_bytes: 0,
            },
            2_000,
        );
        assert_eq!(out.commands.len(), 1);
        assert!(matches!(
            out.commands[0],
            StoreCommand::Rows {
                reason: EmitReason::Drain,
                ..
            }
        ));
    }

    // 2. Closing window with active buffer non-empty on FlushDone (line 1032)
    {
        let (mut worker, res, res_hash, _dir) = make_worker(|c| {
            c.window.duration = Duration::from_secs(60);
            c.window.emit.close_jitter_max = Duration::ZERO;
            c.window.max_close_delay = Duration::ZERO;
        });
        must_try_ingest(
            &mut worker,
            vec![make_item(0, 1, 1_000, &res, res_hash)],
            1,
            1_000,
        );
        let out = worker.flush_l1(1_000);
        assert_eq!(out.commands.len(), 1);
        assert!(matches!(out.commands[0], StoreCommand::Flush(_)));

        let out = worker.poll(Some(60_000), 60_000);
        assert!(out.commands.is_empty());

        must_try_ingest(
            &mut worker,
            vec![make_item(0, 2, 2_000, &res, res_hash)],
            2,
            60_000,
        );

        let out = worker.on_event(
            StoreEvent::FlushDone {
                finalizers: vec![1],
                stats: Default::default(),
                disk_bytes: 0,
            },
            60_000,
        );
        assert_eq!(out.commands.len(), 1);
        assert!(matches!(out.commands[0], StoreCommand::Flush(_)));

        let out = worker.on_event(
            StoreEvent::FlushDone {
                finalizers: vec![2],
                stats: Default::default(),
                disk_bytes: 0,
            },
            60_000,
        );
        assert_eq!(out.commands.len(), 1);
        assert!(matches!(
            out.commands[0],
            StoreCommand::Rows {
                reason: EmitReason::Close,
                ..
            }
        ));
    }

    // 3. periodic_flush_pending with active buffer non-empty on FlushDone (line 1058)
    {
        let (mut worker, res, res_hash, _dir) = make_worker(|c| {
            c.window.duration = Duration::from_secs(60);
            c.window.emit.strategy = EmitStrategy::Periodic;
            c.window.emit.interval = Some(Duration::from_millis(500));
        });
        must_try_ingest(
            &mut worker,
            vec![make_item(0, 1, 1_000, &res, res_hash)],
            1,
            0,
        );
        let out = worker.flush_l1(0);
        assert_eq!(out.commands.len(), 1);
        assert!(matches!(out.commands[0], StoreCommand::Flush(_)));

        let out = worker.poll(None, 500);
        assert!(out.commands.is_empty());

        must_try_ingest(
            &mut worker,
            vec![make_item(0, 2, 1_500, &res, res_hash)],
            2,
            500,
        );

        let out = worker.on_event(
            StoreEvent::FlushDone {
                finalizers: vec![1],
                stats: Default::default(),
                disk_bytes: 0,
            },
            500,
        );
        assert_eq!(out.commands.len(), 1);
        assert!(matches!(out.commands[0], StoreCommand::Flush(_)));

        let out = worker.on_event(
            StoreEvent::FlushDone {
                finalizers: vec![2],
                stats: Default::default(),
                disk_bytes: 0,
            },
            500,
        );
        assert_eq!(out.commands.len(), 1);
        assert!(matches!(
            out.commands[0],
            StoreCommand::Rows {
                reason: EmitReason::Periodic,
                ..
            }
        ));
    }

    // 4. poll() late emission flush swap (line 829) and late_flush_pending on FlushDone (line 1085)
    {
        let (mut worker, res, res_hash, _dir) = make_worker(|c| {
            c.window.duration = Duration::from_secs(60);
            c.window.emit.close_jitter_max = Duration::ZERO;
            c.window.max_close_delay = Duration::ZERO;
            c.window.late_emit_interval = Duration::from_millis(500);
        });
        must_try_ingest(
            &mut worker,
            vec![make_item(0, 1, 1_000, &res, res_hash)],
            1,
            1_000,
        );
        let _ = worker.flush_l1(1_000);
        let _ = worker.on_event(
            StoreEvent::FlushDone {
                finalizers: vec![1],
                stats: Default::default(),
                disk_bytes: 0,
            },
            1_000,
        );
        let out = worker.poll(Some(60_000), 60_000);
        assert_eq!(out.commands.len(), 1);
        assert!(matches!(
            out.commands[0],
            StoreCommand::Rows {
                reason: EmitReason::Close,
                ..
            }
        ));

        // Close the window completely via StoreEvent::Rows
        let _ = worker.on_event(
            StoreEvent::Rows {
                epoch_ms: 0,
                reason: EmitReason::Close,
                rows: Vec::new(),
                next_after: None,
                recovery_ticket: None,
            },
            60_000,
        );

        // Ingest a late event into window 0
        must_try_ingest(
            &mut worker,
            vec![make_item(0, 99, 1_000, &res, res_hash)],
            99,
            60_100,
        );

        // Poll at 60_600ms (late_emit_interval elapsed): swaps active buffer (line 829)
        let out = worker.poll(None, 60_600);
        assert_eq!(out.commands.len(), 1);
        assert!(matches!(out.commands[0], StoreCommand::Flush(_)));

        // Ingest another event while late flush is in flight
        must_try_ingest(
            &mut worker,
            vec![make_item(0, 100, 1_000, &res, res_hash)],
            100,
            60_600,
        );

        // FlushDone triggers swap of active buffer (line 1085)
        let out = worker.on_event(
            StoreEvent::FlushDone {
                finalizers: vec![99],
                stats: Default::default(),
                disk_bytes: 0,
            },
            60_600,
        );
        assert_eq!(out.commands.len(), 1);
        assert!(matches!(out.commands[0], StoreCommand::Flush(_)));

        let out = worker.on_event(
            StoreEvent::FlushDone {
                finalizers: vec![100],
                stats: Default::default(),
                disk_bytes: 0,
            },
            60_600,
        );
        assert_eq!(out.commands.len(), 1);
        assert!(matches!(
            out.commands[0],
            StoreCommand::Rows {
                reason: EmitReason::LateRevision,
                ..
            }
        ));
    }

    // 5. evict_flush_pending with active buffer non-empty on FlushDone (line 1113)
    {
        let (mut worker, res, res_hash, _dir) = make_worker(|c| {
            c.window.duration = Duration::from_secs(60);
            c.cardinality.on_cap = OnCap::EvictCold;
            c.cardinality.max_keys_per_window = 2;
            c.cardinality.evict_high_water = 0.5;
            c.cardinality.evict_interval = Duration::from_millis(500);
        });
        must_try_ingest(
            &mut worker,
            vec![make_item(0, 1, 1_000, &res, res_hash)],
            1,
            0,
        );
        let out = worker.flush_l1(0);
        assert_eq!(out.commands.len(), 1);
        assert!(matches!(out.commands[0], StoreCommand::Flush(_)));

        let out = worker.poll(None, 500);
        assert!(out.commands.is_empty());

        must_try_ingest(
            &mut worker,
            vec![make_item(0, 2, 1_500, &res, res_hash)],
            2,
            500,
        );

        let out = worker.on_event(
            StoreEvent::FlushDone {
                finalizers: vec![1],
                stats: Default::default(),
                disk_bytes: 0,
            },
            500,
        );
        assert_eq!(out.commands.len(), 1);
        assert!(matches!(out.commands[0], StoreCommand::Flush(_)));

        let out = worker.on_event(
            StoreEvent::FlushDone {
                finalizers: vec![2],
                stats: Default::default(),
                disk_bytes: 0,
            },
            500,
        );
        assert_eq!(out.commands.len(), 1);
        assert!(matches!(
            out.commands[0],
            StoreCommand::Rows {
                reason: EmitReason::Evict,
                ..
            }
        ));
    }
}
