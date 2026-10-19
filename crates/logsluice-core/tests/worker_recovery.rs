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
