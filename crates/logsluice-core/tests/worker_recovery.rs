#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

mod common;
use common::Harness;
use logsluice_core::codec::ticket::EmitReason;
use std::time::Duration;

const DAY: i64 = 86_400_000;

#[test]
fn test_ticket_replay_same_slice_ids() {
    let mut h = Harness::new(|c| c.window.emit.close_jitter_max = Duration::ZERO);
    h.ingest(&[(1, 1_000), (2, 2_000)]);
    h.advance(DAY + 31_000, Some(DAY));
    let before: Vec<_> = h.drain_emitted().into_iter().map(|r| r.slice_id).collect();
    let mut h = h.restart(); // no acks: tickets remain persisted
    let after: Vec<_> = h.drain_emitted().into_iter().map(|r| r.slice_id).collect();
    assert_eq!(before, after);
}

#[test]
fn restart_rebuilds_known_keys_and_drops_replayed_input() {
    let mut h = Harness::new(|_| {});
    h.ingest_at_offsets(&[(1, 1_000, 10), (2, 2_000, 11)]); // (key, ts, offset) on t/0
    h.advance(30_000, None);
    let mut h = h.restart();
    assert_eq!(h.stats().keys_known, 2);
    let r = h.ingest_at_offsets(&[(1, 1_000, 10), (2, 2_000, 11), (3, 3_000, 12)]);
    assert_eq!((r.replayed, r.accepted), (2, 1));
}

#[test]
fn test_drain_on_shutdown() {
    let mut h = Harness::new(|c| c.window.emit.close_jitter_max = Duration::ZERO);
    h.ingest(&[(1, 1_000), (1, 2_000), (2, 3_000)]);
    h.drain(60_000);
    let drained = h.drain_emitted();
    assert!(drained.iter().all(|r| r.emit_reason == EmitReason::Drain));
    h.ack_all();
    let mut h = h.restart();
    h.ingest(&[(1, 4_000)]);
    h.advance(DAY + 31_000, Some(DAY));
    h.ack_all();
    let mut all = drained;
    all.extend(h.all_emitted());
    assert_eq!(h.count_for(&all, 1), 3);
    assert_eq!(h.count_for(&all, 2), 1);
}

#[test]
fn unacked_tickets_spill_and_replay() {
    let mut h = Harness::new(|c| {
        c.window.emit.close_jitter_max = Duration::ZERO;
        c.window.max_closing_duration = Duration::from_secs(60);
    });
    h.ingest(&[(1, 1_000)]);
    h.advance(DAY + 31_000, Some(DAY));
    let first = h.drain_emitted();
    h.nack_all();
    h.advance(DAY + 92_000, Some(DAY));
    assert_eq!(h.spill_files(), 1);
    assert!(!h.worker().has_inflight_tickets());
    h.advance(DAY + 3_700_000, Some(DAY + 3_600_001));
    assert!(h.live_windows().is_empty()); // window purged despite outage
    let replayed = h.drain_emitted(); // SPILL_RETRY_INTERVAL elapsed since the spill
    assert_eq!(replayed[0].slice_id, first[0].slice_id);
    h.ack_all(); // sink recovered
    h.advance(DAY + 3_701_000, Some(DAY + 3_600_001));
    assert_eq!(h.spill_files(), 0);
}

#[test]
fn multi_window_resource_survives_restart_and_first_window_purge() {
    let mut h = Harness::new(|c| {
        c.window.emit.close_jitter_max = Duration::ZERO;
    });
    // Ingest resource in Window 0 [0, DAY) and flush L1
    h.ingest(&[(1, 1_000)]);
    h.advance(30_000, None);

    // Reuse the same resource in Window 1 [DAY, 2 * DAY) and flush L1
    h.ingest(&[(2, DAY + 1_000)]);
    h.advance(60_000, None);

    // Restart pod: RecoveredState::load must restore both window 0 and window 1 references for the resource
    let mut h = h.restart();

    // Close and purge Window 0 while Window 1 is still active
    h.advance(DAY + 31_000, Some(DAY));
    let w0_emitted = h.drain_emitted();
    assert_eq!(h.count_for(&w0_emitted, 1), 1);
    h.ack_all();
    h.advance(DAY + 3_700_000, Some(DAY + 3_600_001));
    assert_eq!(h.live_windows(), vec![DAY as u64]);

    // Now close Window 1: resource must still exist in the table so records_for succeeds
    h.advance(2 * DAY + 31_000, Some(2 * DAY));
    let w1_emitted = h.drain_emitted();
    assert_eq!(w1_emitted.len(), 1);
    assert_eq!(h.count_for(&w1_emitted, 2), 1);
}

#[test]
fn spilled_ticket_cleans_up_store_tickets_on_restart() {
    let mut h = Harness::new(|c| {
        c.window.emit.close_jitter_max = Duration::ZERO;
        c.window.max_closing_duration = Duration::from_secs(60);
    });
    h.ingest(&[(1, 1_000)]);
    h.advance(DAY + 31_000, Some(DAY));
    let first = h.drain_emitted();
    assert_eq!(first.len(), 1);
    h.nack_all();

    // Trigger spill (92s > 60s max_closing_duration)
    h.advance(DAY + 92_000, Some(DAY));
    assert_eq!(h.spill_files(), 1);

    // Restart immediately after spill; _tickets in ShardStore was cleaned up by StoreCommand::Ack on spill,
    // so recovery_output() must NOT re-emit from _tickets before SPILL_RETRY_INTERVAL_MS
    let mut h = h.restart();
    assert!(h.drain_emitted().is_empty());
    assert_eq!(h.spill_files(), 1);
}

#[test]
fn pacer_rate_limits_burst_and_drains_on_poll() {
    let mut h = Harness::new(|c| {
        c.window.emit.close_jitter_max = Duration::ZERO;
        c.output.max_emit_rate_per_sec = 1;
        c.output.max_records_per_ticket = 1;
    });
    h.ingest(&[(1, 1_000), (2, 2_000)]);
    h.advance(DAY + 31_000, Some(DAY));
    // With rate=1/sec and 1 record per ticket, only the first batch is emitted immediately;
    // the second batch remains in paced_emit_queue
    let first_emit = h.drain_emitted();
    assert_eq!(first_emit.len(), 1);
    assert!(h.worker().has_inflight_tickets());

    // Advance 1 second so Pacer refills 1 token and drains the second batch
    h.advance(DAY + 32_000, Some(DAY));
    let second_emit = h.drain_emitted();
    assert_eq!(second_emit.len(), 1);
}

#[test]
fn shutdown_persist_flushes_without_emitting_drain_slices() {
    let mut h = Harness::new(|c| {
        c.window.emit.close_jitter_max = Duration::ZERO;
        c.shutdown.on_shutdown = logsluice_core::config::OnShutdown::Persist;
    });
    h.ingest(&[(1, 1_000), (1, 1_100), (1, 1_200), (1, 1_300), (1, 1_400)]);
    h.drain(5_000);
    // OnShutdown::Persist must flush L1 to Fjall without emitting any Drain slices.
    assert!(
        h.drain_emitted().is_empty(),
        "OnShutdown::Persist must not emit Drain slices"
    );
    assert!(h.worker().is_drain_idle());

    // After restart, closing the window emits the persisted total (5).
    let mut h = h.restart();
    h.advance(DAY + 31_000, Some(DAY));
    let emitted = h.drain_emitted();
    assert_eq!(emitted.len(), 1);
    assert_eq!(emitted[0].counters[0][0], 5);
}

#[test]
fn multi_resource_overflow_buckets_preserve_per_resource_totals() {
    let mut h = Harness::new(|c| {
        c.cardinality.max_keys_per_window = 2;
        c.cardinality.max_resource_share = 0.5;
        c.cardinality.on_cap = logsluice_core::config::OnCap::Overflow;
        c.window.emit.close_jitter_max = Duration::ZERO;
    });

    let make_res_item = |ts: i64, service: &str, host: &str, count: u64| {
        let mut res_map = std::collections::BTreeMap::new();
        res_map.insert("service.name".to_string(), serde_json::json!(service));
        let payload = std::sync::Arc::new(logsluice_core::resource::ResourceScopePayload {
            resource: res_map,
            scope_name: "app".into(),
            scope_version: "1.0".into(),
            scope_attrs: std::collections::BTreeMap::new(),
        });
        let r_hash = payload.canonical_hash();
        let d = logsluice_core::key::KeyBuilder::new(7).digest(
            r_hash,
            0,
            None,
            &[Some(host.as_bytes())],
        );
        logsluice_core::worker::IngestItem {
            key: logsluice_core::key::StorageKey24 {
                epoch_ms: 0,
                digest: d,
            },
            resource: payload,
            resource_hash: r_hash,
            position: None,
            event_ms: ts,
            counters: vec![[count, 0, 0, 0]],
            minmax_values: vec![ts, ts],
            exemplar: logsluice_core::worker::ExemplarSource::default(),
        }
    };

    // Resource A: h1 admitted (1/1), h2 & h3 overflow for Resource A (3 + 4 = 7).
    // Resource B: h4 admitted (1/1), h5 overflows for Resource B (9).
    h.worker().ingest(
        vec![
            make_res_item(1_000, "svc-a", "h1", 2),
            make_res_item(1_000, "svc-a", "h2", 3),
            make_res_item(1_000, "svc-a", "h3", 4),
            make_res_item(1_000, "svc-b", "h4", 5),
            make_res_item(1_000, "svc-b", "h5", 9),
        ],
        0,
        1_000,
    );

    h.advance(DAY + 31_000, Some(DAY));
    let recs = h.drain_emitted();
    // 2 regular keys + 2 distinct overflow buckets (one for svc-a, one for svc-b) = 4 records.
    assert_eq!(recs.len(), 4);
    let total: u64 = recs.iter().map(|r| r.counters[0][0]).sum();
    assert_eq!(total, 2 + 3 + 4 + 5 + 9);
}

#[test]
fn recovery_rows_failure_does_not_misalign_next_ticket() {
    let mut h = Harness::new(|c| {
        c.window.emit.close_jitter_max = Duration::ZERO;
        c.output.max_records_per_ticket = 1;
    });
    h.ingest(&[(1, 1_000), (2, 2_000)]);
    h.advance(DAY + 31_000, Some(DAY));
    let before = h.drain_emitted();
    assert_eq!(before.len(), 2);
    let before_key1 = before.iter().find(|r| r.digest == 1).unwrap().clone();
    let before_key2 = before.iter().find(|r| r.digest == 2).unwrap().clone();

    let mut h = h.restart_with(
        |_| {},
        |h| {
            h.fail_next_rows();
        },
    );
    let after = h.drain_emitted();
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].digest, 2);
    assert_eq!(after[0].slice_id, before_key2.slice_id);
    assert_eq!(h.stats().recovery_failures_total, 1);
    assert_eq!(h.stats().slices_reemitted_total, 1);

    h.ack_all();

    // Next poll retries only the failed ticket 1 without resetting closing.scan_started or re-scanning key 2.
    h.advance(DAY + 32_000, Some(DAY));
    let retried = h.drain_emitted();
    assert_eq!(retried.len(), 1);
    assert_eq!(retried[0].digest, 1);
    assert_eq!(retried[0].slice_id, before_key1.slice_id);
    assert_eq!(h.stats().slices_reemitted_total, 2);

    h.inspect_store(|store| {
        let tickets = store.tickets().unwrap();
        assert_eq!(tickets.len(), 1);
        assert_eq!(tickets[0].entries[0].digest, 1);
    });
}

#[test]
fn recovery_records_for_error_leaves_ticket_persisted_and_unacked() {
    let mut h = Harness::new(|c| c.window.emit.close_jitter_max = Duration::ZERO);
    h.ingest(&[(1, 1_000)]);
    h.advance(DAY + 31_000, Some(DAY));
    let before = h.drain_emitted();
    assert_eq!(before.len(), 1);

    // Delete the resource scope before recovery so records_for returns Err on recovery Rows.
    let mut h = h.restart_with(
        |store| {
            store.delete_resources(&[1]).unwrap();
        },
        |_| {},
    );
    assert!(h.drain_emitted().is_empty());
    assert_eq!(h.stats().recovery_failures_total, 1);
    assert_eq!(h.stats().slices_reemitted_total, 0);

    // Ticket must remain persisted in _tickets and stored.emitted counters must remain zero.
    h.inspect_store(|store| {
        let tickets = store.tickets().unwrap();
        assert_eq!(tickets.len(), 1);
        let hdr = store
            .header(&logsluice_core::key::StorageKey24 {
                epoch_ms: 0,
                digest: 1,
            })
            .unwrap()
            .unwrap();
        assert_eq!(hdr.total, vec![[1, 0, 0, 0]]);
        assert_eq!(hdr.emitted, vec![[0, 0, 0, 0]]);
        assert_eq!(hdr.slice_seq, 0);
    });
}

#[test]
fn recovery_ticket_with_no_entries_is_resolved() {
    let mut h = Harness::new(|c| c.window.emit.close_jitter_max = Duration::ZERO);
    h.ingest(&[(1, 1_000)]);
    h.advance(DAY + 31_000, Some(DAY));
    let _ = h.drain_emitted();
    h.ack_all();

    let mut h = h.restart_with(
        |store| {
            store
                .put_tickets(&[logsluice_core::codec::ticket::Ticket {
                    id: 42,
                    window_epoch_ms: 0,
                    reason: EmitReason::Close,
                    entries: Vec::new(),
                }])
                .unwrap();
        },
        |_| {},
    );
    assert!(h.drain_emitted().is_empty());
    assert!(!h.worker().has_inflight_tickets());

    // Window 0 must transition to Closed and purge once watermark passes allowed_lateness.
    h.advance(DAY + 3_700_000, Some(DAY + 3_600_001));
    assert!(h.live_windows().is_empty());

    h.inspect_store(|store| {
        assert!(store.tickets().unwrap().is_empty());
    });
}

#[test]
fn recovery_prunes_resource_refs_to_dropped_windows() {
    let mut h = Harness::new(|c| {
        c.window.emit.close_jitter_max = Duration::ZERO;
    });
    // Ingest same resource in Window 0 [0, DAY) and Window 1 [DAY, 2 * DAY) and flush L1
    h.ingest(&[(1, 1_000)]);
    h.advance(30_000, None);
    h.ingest(&[(2, DAY + 1_000)]);
    h.advance(60_000, None);

    // Close, ack, and purge Window 0 while Window 1 is still active.
    // On disk in `_resources`, the resource still lists `[0, DAY]` because its refcount did not hit 0.
    h.advance(DAY + 31_000, Some(DAY));
    let _ = h.drain_emitted();
    h.ack_all();
    h.advance(DAY + 3_700_000, Some(DAY + 3_600_001));
    assert_eq!(h.live_windows(), vec![DAY as u64]);
    assert_eq!(h.stats().resources_table_entries, 1);

    // Restart: RecoveredState::load must prune the stale reference to dropped Window 0.
    let mut h = h.restart();
    assert_eq!(h.live_windows(), vec![DAY as u64]);
    assert_eq!(h.stats().resources_table_entries, 1);

    // Now close, ack, and purge Window 1: because Window 0 ref was pruned, the resource refcount
    // now reaches 0 and it is evicted from ResourceScopeTable and deleted from `_resources`.
    h.advance(2 * DAY + 31_000, Some(2 * DAY));
    let _ = h.drain_emitted();
    h.ack_all();
    h.advance(2 * DAY + 3_700_000, Some(2 * DAY + 3_600_001));
    assert!(h.live_windows().is_empty());
    assert_eq!(h.stats().resources_table_entries, 0);
}

#[test]
fn recovery_releases_unreferenced_resources() {
    use logsluice_core::{
        codec::plan::AggregationPlan,
        config::EngineConfig,
        key::StorageKey24,
        resource::ResourceScopePayload,
        store::shard_store::{FlushBatch, FlushEntry},
        window::WindowClock,
        worker::{RecoveredState, WorkerContext},
    };
    use std::sync::Arc;

    let (mut store, kernels, _dir) = common::store_with_layout();
    let payload = Arc::new(ResourceScopePayload {
        resource: std::collections::BTreeMap::new(),
        scope_name: "orphan-scope".into(),
        scope_version: "1.0".into(),
        scope_attrs: std::collections::BTreeMap::new(),
    });

    // Flush a resource referenced only by window 0, then drop window 0 without deleting the resource
    // (simulating a crash between drop_window and delete_resources).
    store
        .apply_flush(
            FlushBatch {
                entries: vec![FlushEntry {
                    key: StorageKey24 {
                        epoch_ms: 0,
                        digest: 42,
                    },
                    is_new: true,
                    resource_scope_id: 7,
                    delta: vec![[1, 0, 0, 0]],
                    minmax: vec![(1, 1), (1, 1)],
                    last_update_s: 1,
                    tail: None,
                }],
                offsets: vec![],
                new_resources: vec![(7, payload)],
                resource_refs: vec![(7, 0)],
            },
            &kernels,
        )
        .unwrap();
    store.drop_window(0).unwrap();
    assert!(store.live_windows().is_empty());
    assert_eq!(store.resources().unwrap().len(), 1);

    let cfg = Arc::new(EngineConfig {
        aggregations: common::spec_example(),
        ..EngineConfig::default()
    });
    let plan = Arc::new(AggregationPlan::new(&cfg.aggregations).unwrap());
    let clock = WindowClock::new(cfg.window.duration, cfg.window.offset).unwrap();
    let ctx = WorkerContext {
        cfg,
        shard: 0,
        emitter_id: Arc::from("pod-0"),
        kernels,
        plan,
        clock,
    };

    let recovered = RecoveredState::load(&store, &ctx).unwrap();
    assert!(recovered.resources.is_empty());
    assert!(store.resources().unwrap().is_empty());
}

#[test]
fn corrupt_spill_file_increments_quarantine_counter() {
    let h = Harness::new(|_| {});
    let spill_dir = h.data_dir().join("shard_0").join("spill");
    std::fs::create_dir_all(&spill_dir).unwrap();

    let corrupt_orig = spill_dir.join(format!("{:020}.ndjson", 42));
    let corrupt_quarantined = spill_dir.join(format!("{:020}.ndjson.corrupt", 42));
    std::fs::write(&corrupt_orig, b"{\"broken\": json\n").unwrap();

    let mut h = h.restart();
    assert_eq!(h.spill_files(), 1);

    // Advance time past SPILL_RETRY_INTERVAL_MS (60s) to trigger periodic spill replay
    h.advance(60_000, None);

    assert_eq!(h.stats().spill_quarantined_total, 1);
    assert!(!corrupt_orig.exists());
    assert!(corrupt_quarantined.exists());
    assert_eq!(h.spill_files(), 0);
}
