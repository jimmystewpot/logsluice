#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

use logsluice_core::{
    codec::{tail::*, ticket::*},
    error::AggError,
    key::StorageKey24,
    resource::ResourceScopePayload,
    simd::{SimdKernelTable, SimdMode},
    store::{meta::PodMeta, shard_store::*},
};
use std::sync::Arc;

const L: HeaderLayout = HeaderLayout {
    banks: 1,
    minmax_slots: 2,
};

fn opts(dir: &std::path::Path) -> StoreOptions {
    StoreOptions {
        shard_dir: dir.join("shard_0"),
        cache_bytes: 8 << 20,
        hdr_memtable_bytes: 1 << 20,
        tail_memtable_bytes: 1 << 20,
        sync_wal: true,
        layout: L,
        worker_threads: 2,
    }
}

fn entry(d: u128, new: bool, n: u64) -> FlushEntry {
    FlushEntry {
        key: StorageKey24 {
            epoch_ms: 0,
            digest: d,
        },
        is_new: new,
        resource_scope_id: 1,
        delta: vec![[n, 0, 0, 0]],
        minmax: vec![(5, 5), (5, 5)],
        last_update_s: 1,
        tail: new.then(|| ExemplarTail {
            severity: 9,
            body: None,
            dims: vec![],
            attrs: vec![],
        }),
    }
}

fn batch(e: Vec<FlushEntry>) -> FlushBatch {
    FlushBatch {
        entries: e,
        offsets: vec![(("t".into(), 0), 99)],
        new_resources: vec![],
        resource_refs: vec![],
    }
}

#[test]
fn pod_meta_salt_stable_and_mismatch_refused() {
    let d = tempfile::tempdir().unwrap();
    let a = PodMeta::load_or_init(d.path(), "pod-0", 4, b"s1").unwrap();
    let b = PodMeta::load_or_init(d.path(), "renamed", 4, b"s1").unwrap();
    assert_eq!(a.salt, b.salt);
    assert_eq!(b.emitter_id, "pod-0");
    assert!(matches!(
        PodMeta::load_or_init(d.path(), "pod-0", 8, b"s1"),
        Err(AggError::MetaMismatch {
            field: "shards",
            ..
        })
    ));
    assert!(matches!(
        PodMeta::load_or_init(d.path(), "pod-0", 4, b"s2"),
        Err(AggError::MetaMismatch {
            field: "codec_schema",
            ..
        })
    ));
}

#[test]
fn test_known_key_rmw_and_blind_new_insert() {
    let d = tempfile::tempdir().unwrap();
    let meta = PodMeta::load_or_init(d.path(), "pod-0", 1, b"s").unwrap();
    let k = SimdKernelTable::detect(SimdMode::Scalar);
    let mut s = ShardStore::open(opts(d.path()), &meta).unwrap();
    let st = s.apply_flush(batch(vec![entry(1, true, 3)]), &k).unwrap();
    assert_eq!((st.hdr_reads, st.tail_writes), (0, 1));
    let st = s.apply_flush(batch(vec![entry(1, false, 2)]), &k).unwrap();
    assert_eq!((st.hdr_reads, st.tail_writes), (1, 0));
    let key = StorageKey24 {
        epoch_ms: 0,
        digest: 1,
    };
    assert_eq!(s.header(&key).unwrap().unwrap().total[0][0], 5);
    assert!(s.tail(&key).unwrap().is_some());
    drop(s);
    let s = ShardStore::open(opts(d.path()), &meta).unwrap();
    assert_eq!(s.offsets().unwrap()[&("t".into(), 0)], 99);
    assert_eq!(s.live_windows(), vec![0]);
}

#[test]
fn rows_merge_join_in_key_order_with_resume() {
    let d = tempfile::tempdir().unwrap();
    let meta = PodMeta::load_or_init(d.path(), "pod-0", 1, b"s").unwrap();
    let k = SimdKernelTable::detect(SimdMode::Scalar);
    let mut s = ShardStore::open(opts(d.path()), &meta).unwrap();
    s.apply_flush(
        batch(vec![
            entry(3, true, 1),
            entry(1, true, 1),
            entry(2, true, 1),
        ]),
        &k,
    )
    .unwrap();
    let first = s.rows(0, None, 2).unwrap();
    assert_eq!(
        first.iter().map(|r| r.key.digest).collect::<Vec<_>>(),
        vec![1, 2]
    );
    let rest = s.rows(0, Some(first[1].key), 2).unwrap();
    assert_eq!(
        rest.iter().map(|r| r.key.digest).collect::<Vec<_>>(),
        vec![3]
    );
    assert!(rest[0].tail.is_some());
}

#[test]
fn tickets_persist_and_ack_applies_once() {
    let d = tempfile::tempdir().unwrap();
    let meta = PodMeta::load_or_init(d.path(), "pod-0", 1, b"s").unwrap();
    let k = SimdKernelTable::detect(SimdMode::Scalar);
    let mut s = ShardStore::open(opts(d.path()), &meta).unwrap();
    s.apply_flush(batch(vec![entry(1, true, 4)]), &k).unwrap();
    let t = Ticket {
        id: 1,
        window_epoch_ms: 0,
        reason: EmitReason::Close,
        entries: vec![TicketEntry {
            digest: 1,
            slice_seq: 0,
            delta: vec![[4, 0, 0, 0]],
            minmax: vec![(5, 5), (5, 5)],
        }],
    };
    s.put_tickets(std::slice::from_ref(&t)).unwrap();
    assert_eq!(s.tickets().unwrap(), vec![t.clone()]);
    s.apply_ack(&t, &k).unwrap();
    let h = s
        .header(&StorageKey24 {
            epoch_ms: 0,
            digest: 1,
        })
        .unwrap()
        .unwrap();
    assert_eq!((h.emitted[0][0], h.slice_seq), (4, 1));
    assert!(s.tickets().unwrap().is_empty());

    // Replay the exact same ack: slice_seq in stored header is now 1, ticket entry slice_seq is 0.
    // It should be ignored idempotently without modifying emitted or slice_seq.
    s.apply_ack(&t, &k).unwrap();
    let h2 = s
        .header(&StorageKey24 {
            epoch_ms: 0,
            digest: 1,
        })
        .unwrap()
        .unwrap();
    assert_eq!((h2.emitted[0][0], h2.slice_seq), (4, 1));
}

#[test]
fn drop_window_removes_keyspaces() {
    let d = tempfile::tempdir().unwrap();
    let meta = PodMeta::load_or_init(d.path(), "pod-0", 1, b"s").unwrap();
    let k = SimdKernelTable::detect(SimdMode::Scalar);
    let mut s = ShardStore::open(opts(d.path()), &meta).unwrap();
    s.apply_flush(batch(vec![entry(1, true, 1)]), &k).unwrap();
    s.drop_window(0).unwrap();
    assert!(s.live_windows().is_empty());
    assert!(
        s.header(&StorageKey24 {
            epoch_ms: 0,
            digest: 1,
        })
        .unwrap()
        .is_none()
    );
}

#[test]
fn rows_for_window_keys_resources_and_disk_bytes() {
    let d = tempfile::tempdir().unwrap();
    let meta = PodMeta::load_or_init(d.path(), "pod-0", 1, b"s").unwrap();
    let k = SimdKernelTable::detect(SimdMode::Scalar);
    let mut s = ShardStore::open(opts(d.path()), &meta).unwrap();

    let res_payload = Arc::new(ResourceScopePayload {
        resource: std::collections::BTreeMap::new(),
        scope_name: "test_scope".into(),
        scope_version: "1.0".into(),
        scope_attrs: std::collections::BTreeMap::new(),
    });

    let b = FlushBatch {
        entries: vec![entry(10, true, 2), entry(20, true, 4)],
        offsets: vec![(("topic_a".into(), 1), 42)],
        new_resources: vec![(100, res_payload.clone())],
        resource_refs: vec![(100, 0)],
    };
    s.apply_flush(b, &k).unwrap();

    // rows_for
    let rows = s
        .rows_for(&[
            StorageKey24 {
                epoch_ms: 0,
                digest: 10,
            },
            StorageKey24 {
                epoch_ms: 0,
                digest: 20,
            },
        ])
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].header.total[0][0], 2);
    assert_eq!(rows[1].header.total[0][0], 4);

    // window_keys
    let w_keys = s.window_keys(0).unwrap();
    assert_eq!(w_keys, vec![(10, 1), (20, 1)]);

    // resources
    let res = s.resources().unwrap();
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].0, 100);
    assert_eq!(res[0].1.scope_name, "test_scope");
    assert_eq!(res[0].2, vec![0]);

    // delete_resources
    s.delete_resources(&[100]).unwrap();
    assert!(s.resources().unwrap().is_empty());

    // delete_keys
    s.delete_keys(
        0,
        &[StorageKey24 {
            epoch_ms: 0,
            digest: 10,
        }],
    )
    .unwrap();
    assert!(
        s.header(&StorageKey24 {
            epoch_ms: 0,
            digest: 10,
        })
        .unwrap()
        .is_none()
    );
    assert!(
        s.header(&StorageKey24 {
            epoch_ms: 0,
            digest: 20,
        })
        .unwrap()
        .is_some()
    );

    // disk_bytes
    let bytes = s.disk_bytes().unwrap();
    assert!(bytes > 0);
}

#[test]
fn shard_store_cross_checks_meta_on_reopen() {
    let d = tempfile::tempdir().unwrap();
    let meta = PodMeta::load_or_init(d.path(), "pod-0", 2, b"schema-v1").unwrap();
    let s = ShardStore::open(opts(d.path()), &meta).unwrap();
    drop(s);

    // Mismatched shards
    let mut bad_meta_shards = meta.clone();
    bad_meta_shards.shards = 4;
    assert!(matches!(
        ShardStore::open(opts(d.path()), &bad_meta_shards),
        Err(AggError::MetaMismatch {
            field: "shards",
            ..
        })
    ));

    // Mismatched codec_schema
    let mut bad_meta_schema = meta.clone();
    bad_meta_schema.codec_schema = b"schema-v2".to_vec();
    assert!(matches!(
        ShardStore::open(opts(d.path()), &bad_meta_schema),
        Err(AggError::MetaMismatch {
            field: "codec_schema",
            ..
        })
    ));
}

#[test]
fn pod_meta_persists_window_clock_and_shard_store_rejects_salt_or_window_mismatch() {
    let dir = tempfile::tempdir().unwrap();

    let meta = PodMeta::load_or_init_with_window(dir.path(), 2, b"schema-v1", None, 60_000, 5_000)
        .unwrap();
    assert_eq!(meta.window_duration_ms, 60_000);
    assert_eq!(meta.window_offset_ms, 5_000);

    let loaded =
        PodMeta::load_or_init_with_window(dir.path(), 2, b"schema-v1", None, 60_000, 5_000)
            .unwrap();
    assert_eq!(loaded.salt, meta.salt);
    assert_eq!(loaded.window_duration_ms, 60_000);
    assert_eq!(loaded.window_offset_ms, 5_000);

    // Mismatched window_duration_ms or window_offset_ms on PodMeta::load_or_init_with_window must fail.
    assert!(matches!(
        PodMeta::load_or_init_with_window(dir.path(), 2, b"schema-v1", None, 300_000, 5_000),
        Err(AggError::MetaMismatch {
            field: "window_duration_ms",
            ..
        })
    ));
    assert!(matches!(
        PodMeta::load_or_init_with_window(dir.path(), 2, b"schema-v1", None, 60_000, 0),
        Err(AggError::MetaMismatch {
            field: "window_offset_ms",
            ..
        })
    ));

    // Opening ShardStore with matching meta succeeds and persists salt/window_duration_ms/window_offset_ms in _meta.
    let store = ShardStore::open(opts(dir.path()), &meta).unwrap();
    drop(store);

    // Reopening ShardStore with a different salt must fail with AggError::MetaMismatch.
    let mut bad_salt_meta = meta.clone();
    bad_salt_meta.salt = meta.salt.wrapping_add(1);
    let err = match ShardStore::open(opts(dir.path()), &bad_salt_meta) {
        Ok(_) => panic!("expected MetaMismatch for salt change"),
        Err(e) => e,
    };
    assert!(matches!(err, AggError::MetaMismatch { field: "salt", .. }));

    // Reopening ShardStore with a different window_duration_ms must also fail with AggError::MetaMismatch.
    let mut bad_win_meta = meta.clone();
    bad_win_meta.window_duration_ms = 300_000;
    let err2 = match ShardStore::open(opts(dir.path()), &bad_win_meta) {
        Ok(_) => panic!("expected MetaMismatch for window_duration_ms change"),
        Err(e) => e,
    };
    assert!(matches!(
        err2,
        AggError::MetaMismatch {
            field: "window_duration_ms",
            ..
        }
    ));

    // Reopening ShardStore with a different window_offset_ms must also fail with AggError::MetaMismatch.
    let mut bad_off_meta = meta.clone();
    bad_off_meta.window_offset_ms = 1_000;
    let err3 = match ShardStore::open(opts(dir.path()), &bad_off_meta) {
        Ok(_) => panic!("expected MetaMismatch for window_offset_ms change"),
        Err(e) => e,
    };
    assert!(matches!(
        err3,
        AggError::MetaMismatch {
            field: "window_offset_ms",
            ..
        }
    ));
}

#[test]
fn conditional_evict_ack_deletes_quiescent_key_and_preserves_key_with_new_flush() {
    use logsluice_core::l1::L1Buffer;

    let dir = tempfile::tempdir().unwrap();
    let kernels = SimdKernelTable::detect(SimdMode::Scalar);
    let meta = PodMeta::load_or_init(dir.path(), "pod-0", 1, b"s").unwrap();
    let mut store = ShardStore::open(opts(dir.path()), &meta).unwrap();

    let epoch = 1_700_000_000_000u64;
    let k_quiescent = StorageKey24 {
        epoch_ms: epoch,
        digest: 0xAAAA,
    };
    let k_updated = StorageKey24 {
        epoch_ms: epoch,
        digest: 0xBBBB,
    };

    let sample_tail = |body: &'static [u8]| ExemplarTail {
        severity: 9,
        body: Some(bytes::Bytes::from_static(body)),
        dims: vec![],
        attrs: vec![],
    };

    // Initial flush: both keys have total = [5, 0, 0, 0], emitted = [0, 0, 0, 0].
    let mut buf1: L1Buffer<()> = L1Buffer::new(16);
    buf1.accumulate(
        k_quiescent,
        true,
        7,
        &[[5, 0, 0, 0]],
        &[10, 20],
        100,
        Some(sample_tail(b"quiescent")),
        &kernels,
    );
    buf1.accumulate(
        k_updated,
        true,
        8,
        &[[5, 0, 0, 0]],
        &[10, 20],
        100,
        Some(sample_tail(b"updated")),
        &kernels,
    );
    let (batch1, _) = buf1.into_flush();
    store.apply_flush(batch1, &kernels).unwrap();

    // Evict ticket built when both keys had delta = [5, 0, 0, 0].
    let evict_ticket = Ticket {
        id: 1,
        window_epoch_ms: epoch,
        reason: EmitReason::Evict,
        entries: vec![
            TicketEntry {
                digest: k_quiescent.digest,
                slice_seq: 0,
                delta: vec![[5, 0, 0, 0]],
                minmax: vec![(10, 10), (20, 20)],
            },
            TicketEntry {
                digest: k_updated.digest,
                slice_seq: 0,
                delta: vec![[5, 0, 0, 0]],
                minmax: vec![(10, 10), (20, 20)],
            },
        ],
    };
    store
        .put_tickets(std::slice::from_ref(&evict_ticket))
        .unwrap();

    // While the evict ticket is in flight, k_updated receives another flush of +3!
    let mut buf2: L1Buffer<()> = L1Buffer::new(16);
    buf2.accumulate(
        k_updated,
        false,
        8,
        &[[3, 0, 0, 0]],
        &[5, 25],
        102,
        None,
        &kernels,
    );
    let (batch2, _) = buf2.into_flush();
    store.apply_flush(batch2, &kernels).unwrap();

    // Now apply_ack for the evict ticket:
    // - k_quiescent has stored.total ([5,0,0,0]) == stored.emitted ([5,0,0,0]) -> deleted & returned in evicted_keys!
    // - k_updated has stored.total ([8,0,0,0]) != stored.emitted ([5,0,0,0]) -> NOT deleted, updated in place!
    let evicted: Vec<(u128, u32)> = store.apply_ack(&evict_ticket, &kernels).unwrap();
    assert_eq!(evicted, vec![(k_quiescent.digest, 7)]);

    assert!(store.header(&k_quiescent).unwrap().is_none());
    assert!(store.tail(&k_quiescent).unwrap().is_none());

    let rows = store.rows(epoch, None, 10).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].key, k_updated);
    assert_eq!(rows[0].header.total, vec![[8, 0, 0, 0]]);
    assert_eq!(rows[0].header.emitted, vec![[5, 0, 0, 0]]);
    assert_eq!(rows[0].header.slice_seq, 1);
    assert!(rows[0].tail.is_some());
}

#[test]
fn existing_key_keeps_first_exemplar_tail() {
    let d = tempfile::tempdir().unwrap();
    let meta = PodMeta::load_or_init(d.path(), "pod-0", 1, b"s").unwrap();
    let k = SimdKernelTable::detect(SimdMode::Scalar);
    let mut s = ShardStore::open(opts(d.path()), &meta).unwrap();

    let key = StorageKey24 {
        epoch_ms: 0,
        digest: 42,
    };
    let tail_a = ExemplarTail {
        severity: 9,
        body: Some(bytes::Bytes::from_static(b"tail-a")),
        dims: vec![],
        attrs: vec![],
    };
    let tail_b = ExemplarTail {
        severity: 13,
        body: Some(bytes::Bytes::from_static(b"tail-b")),
        dims: vec![],
        attrs: vec![],
    };

    let mut e1 = entry(42, true, 1);
    e1.tail = Some(tail_a.clone());
    s.apply_flush(batch(vec![e1]), &k).unwrap();

    let mut e2 = entry(42, false, 2);
    e2.tail = Some(tail_b);
    let st = s.apply_flush(batch(vec![e2]), &k).unwrap();
    assert_eq!(s.tail(&key).unwrap(), Some(tail_a));
    assert_eq!(st.tail_writes, 0);
}

#[test]
fn evicted_key_flush_writes_fallback_tail() {
    let d = tempfile::tempdir().unwrap();
    let meta = PodMeta::load_or_init(d.path(), "pod-0", 1, b"s").unwrap();
    let k = SimdKernelTable::detect(SimdMode::Scalar);
    let mut s = ShardStore::open(opts(d.path()), &meta).unwrap();

    let key = StorageKey24 {
        epoch_ms: 0,
        digest: 77,
    };
    let tail_b = ExemplarTail {
        severity: 13,
        body: Some(bytes::Bytes::from_static(b"fallback-b")),
        dims: vec![],
        attrs: vec![],
    };

    let mut e = entry(77, false, 3);
    e.tail = Some(tail_b.clone());
    let st = s.apply_flush(batch(vec![e]), &k).unwrap();
    assert_eq!(s.tail(&key).unwrap(), Some(tail_b));
    assert_eq!(st.tail_writes, 1);
}

#[test]
fn last_update_s_does_not_regress() {
    let d = tempfile::tempdir().unwrap();
    let meta = PodMeta::load_or_init(d.path(), "pod-0", 1, b"s").unwrap();
    let k = SimdKernelTable::detect(SimdMode::Scalar);
    let mut s = ShardStore::open(opts(d.path()), &meta).unwrap();

    let key = StorageKey24 {
        epoch_ms: 0,
        digest: 99,
    };
    let mut e1 = entry(99, true, 1);
    e1.last_update_s = 100;
    s.apply_flush(batch(vec![e1]), &k).unwrap();

    let mut e2 = entry(99, false, 1);
    e2.last_update_s = 50;
    s.apply_flush(batch(vec![e2]), &k).unwrap();

    let hdr = s.header(&key).unwrap().unwrap();
    assert_eq!(hdr.last_update_s, 100);
}

#[test]
fn store_opens_with_one_worker_thread() {
    let d = tempfile::tempdir().unwrap();
    let meta = PodMeta::load_or_init(d.path(), "pod-0", 1, b"s").unwrap();
    let k = SimdKernelTable::detect(SimdMode::Scalar);

    let mut zero_opts = opts(d.path());
    zero_opts.worker_threads = 0;
    assert!(matches!(
        ShardStore::open(zero_opts, &meta),
        Err(AggError::Config(ref m)) if m.contains("worker_threads")
    ));

    let mut one_opts = opts(d.path());
    one_opts.worker_threads = 1;
    let mut store = ShardStore::open(one_opts, &meta).unwrap();
    let st = store
        .apply_flush(batch(vec![entry(1, true, 5)]), &k)
        .unwrap();
    assert_eq!(st.hdr_writes, 1);
    let key = StorageKey24 {
        epoch_ms: 0,
        digest: 1,
    };
    assert_eq!(store.header(&key).unwrap().unwrap().total[0][0], 5);
}
