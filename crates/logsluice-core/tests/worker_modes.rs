#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

mod common;
use bytes::Bytes;
use common::Harness;
use logsluice_core::{
    codec::{plan::AggregationPlan, ticket::EmitReason},
    config::*,
    key::StorageKey24,
    resource::ResourceScopePayload,
    simd::{SimdKernelTable, SimdMode},
    store::{StoreCommand, StoreEvent},
    window::WindowClock,
    worker::{
        ExemplarSource, IngestItem, PurgeHorizon, RecoveredState, ShardWorker, WorkerContext,
    },
};
use std::{sync::Arc, time::Duration};

const DAY: i64 = 86_400_000;
const M15: i64 = 900_000;

fn must_try_ingest(
    worker: &mut ShardWorker<u32>,
    items: Vec<IngestItem>,
    finalizer: u32,
    now_ms: i64,
) {
    if let Err((batch, _)) = worker.try_ingest(items, finalizer, now_ms) {
        panic!("try_ingest refused a batch of length {}", batch.len());
    }
}

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

#[test]
fn evicted_key_with_inflight_flush_keeps_exemplar() {
    let (mut store, kernels, _dir) = common::store_with_layout();
    let mut cfg = EngineConfig::default();
    cfg.cardinality.max_keys_per_window = 1;
    cfg.cardinality.on_cap = OnCap::EvictCold;
    cfg.cardinality.evict_high_water = 1.0;
    cfg.cardinality.evict_interval = Duration::from_secs(10);
    cfg.window.emit.close_jitter_max = Duration::ZERO;
    let cfg = Arc::new(cfg);

    let clock = WindowClock::new(cfg.window.duration, cfg.window.offset).unwrap();
    let plan = Arc::new(AggregationPlan::new(&common::spec_example()).unwrap());
    let ctx = WorkerContext {
        cfg: Arc::clone(&cfg),
        shard: 0,
        emitter_id: Arc::from("pod-0"),
        kernels,
        plan,
        clock,
    };
    let mut worker: ShardWorker<u32> =
        ShardWorker::new(ctx, RecoveredState::empty(), PurgeHorizon::new(0));

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

    // 1. Ingest key K with exemplar-1 and flush to ShardStore.
    let item1 = IngestItem {
        key,
        resource: Arc::clone(&res),
        resource_hash: res_hash,
        position: None,
        event_ms: 1_000,
        counters: vec![[1, 0, 0, 0]],
        minmax_values: vec![1_000, 1_000],
        exemplar: ExemplarSource {
            severity: 1,
            body: Some(Bytes::from_static(b"exemplar-1")),
            dims: Vec::new(),
            attrs: Vec::new(),
        },
    };
    must_try_ingest(&mut worker, vec![item1], 0, 1_000);
    let out = worker.flush_l1(1_000);
    assert_eq!(out.commands.len(), 1);
    let StoreCommand::Flush(buf) = out.commands.into_iter().next().unwrap() else {
        panic!("expected flush command")
    };
    let (batch, finalizers) = buf.into_flush();
    let stats = store.apply_flush(batch, &kernels).unwrap();
    worker.on_event(
        StoreEvent::FlushDone {
            disk_bytes: 0,
            stats,
            finalizers,
        },
        1_000,
    );

    let rows = store.rows(0, None, 10).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].header.total[0][0], 1);
    assert_eq!(
        rows[0].tail.as_ref().and_then(|t| t.body.clone()),
        Some(Bytes::from_static(b"exemplar-1"))
    );

    // 2. Evict K (total equals emitted).
    let out = worker.poll(None, 12_000);
    assert_eq!(out.commands.len(), 1);
    let StoreCommand::Rows {
        epoch_ms,
        reason,
        after,
        limit,
        ..
    } = out.commands.into_iter().next().unwrap()
    else {
        panic!("expected rows command")
    };
    assert_eq!(reason, EmitReason::Evict);
    let rows = store.rows(epoch_ms, after, limit).unwrap();
    let out = worker.on_event(
        StoreEvent::Rows {
            epoch_ms,
            reason,
            rows,
            next_after: None,
            recovery_ticket: None,
        },
        12_000,
    );
    assert_eq!(out.commands.len(), 1);
    let StoreCommand::PersistTickets(tickets) = out.commands.into_iter().next().unwrap() else {
        panic!("expected persist tickets")
    };
    store.put_tickets(&tickets).unwrap();
    let ticket_id = tickets[0].id;
    let out = worker.on_event(StoreEvent::TicketsPersisted(vec![ticket_id]), 12_000);
    assert_eq!(out.emit.len(), 1);
    assert_eq!(out.emit[0].ticket_id, ticket_id);

    // 3. Ingest K again with exemplar-2 so it lands in a batch that is swapped and in flight.
    let item2 = IngestItem {
        key,
        resource: Arc::clone(&res),
        resource_hash: res_hash,
        position: None,
        event_ms: 12_500,
        counters: vec![[1, 0, 0, 0]],
        minmax_values: vec![12_500, 12_500],
        exemplar: ExemplarSource {
            severity: 2,
            body: Some(Bytes::from_static(b"exemplar-2")),
            dims: Vec::new(),
            attrs: Vec::new(),
        },
    };
    must_try_ingest(&mut worker, vec![item2], 1, 12_500);
    let out = worker.flush_l1(12_500);
    assert_eq!(out.commands.len(), 1);
    let StoreCommand::Flush(swapped_buf) = out.commands.into_iter().next().unwrap() else {
        panic!("expected flush command")
    };
    let (swapped, swapped_finalizers) = swapped_buf.into_flush();

    // 4. Deliver the evict AckApplied while swapped batch is in flight.
    let out = worker.on_ack(ticket_id, true, 12_500);
    assert_eq!(out.commands.len(), 1);
    let StoreCommand::Ack(ticket) = out.commands.into_iter().next().unwrap() else {
        panic!("expected ack command")
    };
    let evicted_keys = store.apply_ack(&ticket, &kernels).unwrap();
    assert_eq!(evicted_keys.len(), 1);
    assert!(store.rows(0, None, 10).unwrap().is_empty());

    worker.on_event(
        StoreEvent::AckApplied {
            ticket_id,
            evicted_keys,
        },
        12_500,
    );

    // 5. Flush the in-flight batch.
    let stats = store.apply_flush(swapped, &kernels).unwrap();
    worker.on_event(
        StoreEvent::FlushDone {
            disk_bytes: 0,
            stats,
            finalizers: swapped_finalizers,
        },
        12_500,
    );

    // 6. Assert the stored tail for K still exists and the counters are correct.
    let rows = store.rows(0, None, 10).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].header.total[0][0], 1);
    assert!(rows[0].tail.is_some());
    assert_eq!(
        rows[0].tail.as_ref().and_then(|t| t.body.clone()),
        Some(Bytes::from_static(b"exemplar-2"))
    );
    assert_eq!(worker.stats().keys_evicted_total, 0);

    // 7. Verify K was NOT forgotten from worker.known:
    // A subsequent ingest of K must be treated as known (is_new = false)
    // and must accumulate onto the stored counters instead of overwriting _hdr.
    let item3 = IngestItem {
        key,
        resource: Arc::clone(&res),
        resource_hash: res_hash,
        position: None,
        event_ms: 13_000,
        counters: vec![[5, 0, 0, 0]],
        minmax_values: vec![13_000, 13_000],
        exemplar: ExemplarSource {
            severity: 3,
            body: Some(Bytes::from_static(b"exemplar-3")),
            dims: Vec::new(),
            attrs: Vec::new(),
        },
    };
    must_try_ingest(&mut worker, vec![item3], 2, 13_000);
    let out = worker.flush_l1(13_000);
    assert_eq!(out.commands.len(), 1);
    let StoreCommand::Flush(batch3_buf) = out.commands.into_iter().next().unwrap() else {
        panic!("expected flush command")
    };
    let (batch3, batch3_finalizers) = batch3_buf.into_flush();
    let stats = store.apply_flush(batch3, &kernels).unwrap();
    worker.on_event(
        StoreEvent::FlushDone {
            disk_bytes: 0,
            stats,
            finalizers: batch3_finalizers,
        },
        13_000,
    );

    let rows = store.rows(0, None, 10).unwrap();
    assert_eq!(rows.len(), 1);
    // 1 (from swapped batch) + 5 (from third ingest) = 6!
    assert_eq!(rows[0].header.total[0][0], 6);
    assert_eq!(worker.stats().keys_evicted_total, 0);
}

#[test]
fn pending_commands_stays_bounded_with_stalled_store() {
    // Documented upper bound: 4x the 64-slot StoreCommand channel = 256.
    const MAX_BOUND: usize = 256;

    // --- Scenario A: Live pipeline encountering a stalled store ---
    let mut h = Harness::new(|c| {
        c.window.duration = Duration::from_secs(10);
        c.window.emit = EmitConfig {
            strategy: EmitStrategy::Periodic,
            interval: Some(Duration::from_millis(1_000)),
            close_jitter_max: Duration::ZERO,
        };
        c.window.late_emit_interval = Duration::from_millis(1_000);
        c.window.max_closing_duration = Duration::from_millis(2_000);
        c.cardinality.max_keys_per_window = 10;
        c.cardinality.on_cap = OnCap::EvictCold;
        c.cardinality.evict_interval = Duration::from_millis(1_000);
        c.cardinality.evict_high_water = 0.5;
        c.storage.l1_flush_interval = Duration::from_millis(1_000);
    });

    // 1. Establish an unacknowledged ticket and spill directory during normal operation.
    let spill_dir = h.data_dir().join("shard_0").join("spill");
    std::fs::create_dir_all(&spill_dir).expect("create spill dir");

    // Ingest into initial window (epoch 0) and advance to emit a ticket.
    h.ingest(&[(1, 1_000), (2, 2_000)]);
    h.advance(11_000, Some(10_000));

    // Verify an unacknowledged ticket is in flight.
    assert!(
        h.worker().stats().tickets_inflight > 0,
        "expected in-flight tickets before stall"
    );

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

    // 2. The store now stalls completely: no StoreEvents are processed,
    // and every command produced by worker.poll() is refused and deferred.
    let worker = h.worker();
    let mut now_ms: i64 = 11_000;

    // Drive worker across 500 ticks spanning multiple windows, flushes, and scans.
    for tick in 1..=500 {
        now_ms += 1_000;
        let watermark = Some(now_ms.saturating_sub(2_000));

        // Attempt ingestion across multiple window epochs to exercise L1 flush & backpressure.
        if tick % 2 == 0 {
            let epoch = (now_ms / 10_000) * 10_000;
            let item = IngestItem {
                key: StorageKey24 {
                    epoch_ms: epoch as u64,
                    digest: (tick as u128 % 20) + 10,
                },
                resource: Arc::clone(&res),
                resource_hash: res_hash,
                position: None,
                event_ms: now_ms,
                counters: vec![[1, 0, 0, 0]],
                minmax_values: vec![now_ms, now_ms],
                exemplar: ExemplarSource::default(),
            };
            must_try_ingest(worker, vec![item], tick as u32, now_ms);
        }

        let out = worker.poll(watermark, now_ms);
        for cmd in out.commands {
            worker.defer_command(cmd);
        }

        let pending_len = worker.pending_commands_len();
        let inflight = worker.stats().tickets_inflight;

        assert!(
            pending_len <= MAX_BOUND,
            "pending_commands_len ({pending_len}) exceeded MAX_BOUND ({MAX_BOUND}) at tick {tick}"
        );
        assert!(
            inflight <= MAX_BOUND,
            "tickets_inflight ({inflight}) exceeded MAX_BOUND ({MAX_BOUND}) at tick {tick}"
        );
    }

    // Verify final state remains strictly bounded (measured: pending_commands = 2, tickets_inflight = 2).
    let final_pending = worker.pending_commands_len();
    let final_inflight = worker.stats().tickets_inflight;
    assert!(
        final_pending <= MAX_BOUND,
        "final pending commands ({final_pending}) exceeded bound ({MAX_BOUND})"
    );
    assert!(
        final_inflight <= MAX_BOUND,
        "final in-flight tickets ({final_inflight}) exceeded bound ({MAX_BOUND})"
    );

    // --- Scenario B: Crash-recovered worker with cached spill files under a stalled store ---
    let recovered_dir = tempfile::tempdir().expect("create temp dir");
    let rec_spill_dir = recovered_dir.path().join("shard_0").join("spill");
    std::fs::create_dir_all(&rec_spill_dir).expect("create spill dir");
    std::fs::write(rec_spill_dir.join("00000000000000000042.ndjson"), b"{}\n")
        .expect("write spill file");

    let cfg_rec = Arc::new(EngineConfig::default());
    let plan = Arc::new(AggregationPlan::new(&common::spec_example()).expect("plan"));
    let kernels = SimdKernelTable::detect(SimdMode::Scalar);
    let clock = WindowClock::new(cfg_rec.window.duration, cfg_rec.window.offset).expect("clock");
    let ctx = WorkerContext {
        cfg: cfg_rec,
        shard: 0,
        emitter_id: Arc::from("pod-0"),
        kernels,
        plan,
        clock,
    };
    let recovered = RecoveredState {
        spill_files: 1,
        ..RecoveredState::empty()
    };
    let mut rec_worker: ShardWorker<u32> = ShardWorker::new(ctx, recovered, PurgeHorizon::new(0));

    let mut rec_now_ms: i64 = 0;
    for tick in 1..=500 {
        rec_now_ms += 1_000;
        let out = rec_worker.poll(None, rec_now_ms);
        for cmd in out.commands {
            rec_worker.defer_command(cmd);
        }
        assert!(
            rec_worker.pending_commands_len() <= MAX_BOUND,
            "recovered worker pending commands exceeded bound at tick {tick}"
        );
        assert!(
            rec_worker.stats().tickets_inflight <= MAX_BOUND,
            "recovered worker tickets inflight exceeded bound at tick {tick}"
        );
    }

    assert!(
        rec_worker.pending_commands_len() <= MAX_BOUND,
        "recovered worker final pending commands exceeded bound"
    );
}
