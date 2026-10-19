#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

use std::{sync::Arc, time::Duration};

use logsluice_core::{
    codec::{header::*, plan::HeaderLayout, ticket::EmitReason},
    config::EmitMode,
    emission::*,
    key::StorageKey24,
    resource::{ResourceScopePayload, ResourceScopeTable},
    simd::{SimdKernelTable, SimdMode},
    store::shard_store::JoinedRow,
    window::WindowClock,
};

#[test]
fn slice_id_format_exact() {
    assert_eq!(
        slice_id("pod-0", 1_760_000_000_000, 0xabc, 3),
        "pod-0:1760000000000:00000000000000000000000000000abc:3"
    );
}

fn row(d: u128, total: u64, emitted: u64) -> JoinedRow {
    let l = HeaderLayout {
        banks: 1,
        minmax_slots: 2,
    };
    let mut h = NumericHeader::new_zeroed(l, 1);
    h.total[0][0] = total;
    h.emitted[0][0] = emitted;
    JoinedRow {
        key: StorageKey24 {
            epoch_ms: 0,
            digest: d,
        },
        header: h,
        tail: None,
    }
}

#[test]
fn ledger_skips_zero_delta_and_inflight_and_chunks() {
    let k = SimdKernelTable::detect(SimdMode::Scalar);
    let mut l = TicketLedger::new(10);
    let rows: Vec<_> = (1..=5)
        .map(|d| row(d, 3, 1))
        .chain([row(6, 2, 2)])
        .collect();
    let t = l.build(0, EmitReason::Close, &rows, &k, 2);
    assert_eq!(t.iter().map(|t| t.id).collect::<Vec<_>>(), vec![10, 11, 12]);
    assert_eq!(t.iter().map(|t| t.entries.len()).sum::<usize>(), 5);
    assert_eq!(t[0].entries[0].delta[0][0], 2);
    assert!(l.build(0, EmitReason::Close, &rows, &k, 2).is_empty()); // all inflight
    assert!(l.resolve(10).is_some());
    assert_eq!(l.inflight_len(), 3);
}

#[test]
fn ledger_restore_and_inflight_check() {
    let k = SimdKernelTable::detect(SimdMode::Scalar);
    let mut l = TicketLedger::new(1);
    let rows: Vec<_> = (1..=3).map(|d| row(d, 5, 1)).collect();
    let t = l.build(1000, EmitReason::Periodic, &rows, &k, 10);
    assert_eq!(t.len(), 1);
    assert_eq!(t[0].id, 1);
    assert_eq!(l.inflight_len(), 3);
    assert!(l.is_inflight(&StorageKey24 {
        epoch_ms: 1000,
        digest: 1
    }));

    let resolved = l.resolve(1).expect("ticket 1 resolved");
    assert_eq!(l.inflight_len(), 0);
    assert!(!l.is_inflight(&StorageKey24 {
        epoch_ms: 1000,
        digest: 1
    }));

    // Restore ticket back into ledger
    l.restore(vec![resolved]);
    assert_eq!(l.inflight_len(), 3);
    assert!(l.is_inflight(&StorageKey24 {
        epoch_ms: 1000,
        digest: 1
    }));
}

#[test]
fn pacer_token_bucket() {
    let mut p = Pacer::new(100);
    assert_eq!(p.take(100, 0), 100);
    assert_eq!(p.take(1, 0), 0);
    assert_eq!(p.take(100, 500), 50);
    assert_eq!(p.take(500, 10_000), 100); // burst capped at rate
}

#[test]
fn records_for_delta_and_cumulative_modes() {
    let k = SimdKernelTable::detect(SimdMode::Scalar);
    let mut l = TicketLedger::new(1);

    let mut resources = ResourceScopeTable::new();
    let payload = Arc::new(ResourceScopePayload {
        resource: Default::default(),
        scope_name: "test-scope".to_string(),
        scope_version: "1.0.0".to_string(),
        scope_attrs: Default::default(),
    });
    let (res_id, _) = resources.intern(&payload, payload.canonical_hash(), 1_000_000);

    let layout = HeaderLayout {
        banks: 1,
        minmax_slots: 1,
    };
    let mut h = NumericHeader::new_zeroed(layout, res_id);
    h.slice_seq = 2;
    h.total[0] = [10, 20, 0, 0];
    h.emitted[0] = [4, 5, 0, 0];
    h.minmax[0] = (100, 500);

    let rows = vec![JoinedRow {
        key: StorageKey24 {
            epoch_ms: 1_000_000,
            digest: 0x42,
        },
        header: h,
        tail: None,
    }];

    let tickets = l.build(1_000_000, EmitReason::Close, &rows, &k, 10);
    assert_eq!(tickets.len(), 1);

    let clock = WindowClock::new(Duration::from_secs(60), Duration::ZERO).unwrap();

    // Delta mode
    let delta_recs = records_for(
        &tickets[0],
        &rows,
        &resources,
        "emitter-1",
        &clock,
        EmitMode::DeltaSlice,
    )
    .expect("delta records");
    assert_eq!(delta_recs.len(), 1);
    let r = &delta_recs[0];
    assert_eq!(r.window_start_ms, 1_000_000);
    assert_eq!(r.window_end_ms, 1_060_000);
    assert_eq!(r.emit_reason, EmitReason::Close);
    assert_eq!(r.emitter_id, "emitter-1");
    assert_eq!(r.digest, 0x42);
    assert_eq!(r.slice_seq, 2);
    assert_eq!(
        r.slice_id,
        "emitter-1:1000000:00000000000000000000000000000042:2"
    );
    assert_eq!(r.counters[0], [6, 15, 0, 0]);
    assert_eq!(r.minmax, vec![(100, 500)]);
    assert_eq!(r.tail.severity, 0); // empty tail fallback
    assert_eq!(r.resource.scope_name, "test-scope");

    // Cumulative mode
    let cum_recs = records_for(
        &tickets[0],
        &rows,
        &resources,
        "emitter-1",
        &clock,
        EmitMode::Cumulative,
    )
    .expect("cumulative records");
    assert_eq!(cum_recs[0].counters[0], [10, 20, 0, 0]);

    // Serde roundtrip
    let json = serde_json::to_string(&delta_recs[0]).expect("serialize slice record");
    let deserialized: SliceRecord = serde_json::from_str(&json).expect("deserialize slice record");
    assert_eq!(delta_recs[0], deserialized);
}
