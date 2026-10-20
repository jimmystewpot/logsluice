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
        max_write_buffer_bytes: None,
        max_cached_files: None,
    }
}

fn entry(d: u128, new: bool, n: u64) -> FlushEntry {
    entry_with_epoch_and_tail(0, d, new, new, n)
}

fn entry_with_epoch_and_tail(
    epoch_ms: u64,
    d: u128,
    is_new: bool,
    with_tail: bool,
    n: u64,
) -> FlushEntry {
    FlushEntry {
        key: StorageKey24 {
            epoch_ms,
            digest: d,
        },
        is_new,
        resource_scope_id: 1,
        delta: vec![[n, 0, 0, 0]],
        minmax: vec![(5, 5), (5, 5)],
        last_update_s: 1,
        tail: with_tail.then(|| ExemplarTail {
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

    // rows_for_sorted
    let rows = s
        .rows_for_sorted(&[
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
fn disk_bytes_grows_after_flush_and_shrinks_after_drop_window() {
    let d = tempfile::tempdir().unwrap();
    let meta = PodMeta::load_or_init(d.path(), "pod-0", 1, b"s").unwrap();
    let k = SimdKernelTable::detect(SimdMode::Scalar);
    let mut store_opts = opts(d.path());
    store_opts.hdr_memtable_bytes = 1;
    store_opts.tail_memtable_bytes = 1;
    let mut s = ShardStore::open(store_opts.clone(), &meta).unwrap();

    let initial = s.disk_bytes().unwrap();

    // Flush a 64 KiB incompressible exemplar tail so both the journal (~64 KiB) and the
    // rotated window tail SSTable (~64 KiB) contribute to fjall's disk_space().
    let incompressible: Vec<u8> = (0..16_384u32)
        .flat_map(|i| i.wrapping_mul(2_654_435_761).to_le_bytes())
        .collect();
    let mut flush_entry = entry(1, true, 1);
    flush_entry.tail = Some(ExemplarTail {
        severity: 9,
        body: Some(bytes::Bytes::from(incompressible)),
        dims: vec![],
        attrs: vec![],
    });
    s.apply_flush(batch(vec![flush_entry]), &k).unwrap();

    // Wait for fjall's background flush worker to materialize the rotated memtable SSTable.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let after_flush = loop {
        let current = s.disk_bytes().unwrap();
        if current > initial || std::time::Instant::now() >= deadline {
            break current;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    };
    assert!(
        after_flush > initial,
        "expected disk_bytes after flush ({after_flush}) > initial ({initial})"
    );

    s.drop_window(0).unwrap();
    let after_drop = s.disk_bytes().unwrap();
    assert!(
        after_drop < after_flush,
        "expected disk_bytes after drop_window ({after_drop}) < after_flush ({after_flush})"
    );
}

#[test]
fn disk_bytes_includes_spill_files() {
    use logsluice_core::emission::SliceRecord;
    use logsluice_core::worker::spill::SpillWriter;

    let dir = tempfile::tempdir().unwrap();
    let meta = PodMeta::load_or_init(dir.path(), "pod-0", 1, b"s").unwrap();
    let mut store_opts = opts(dir.path());
    store_opts.shard_dir = dir.path().to_path_buf();
    let store = ShardStore::open(store_opts, &meta).unwrap();

    let initial = store.disk_bytes().unwrap();

    let mut spill_writer = SpillWriter::new(dir.path().to_path_buf());
    let record = SliceRecord {
        window_start_ms: 0,
        window_end_ms: 60_000,
        emit_reason: EmitReason::Close,
        emitter_id: "pod-0".to_string(),
        digest: 123,
        slice_seq: 0,
        slice_id: "pod-0:0:0000000000000000000000000000007b:0".to_string(),
        resource: Arc::new(ResourceScopePayload {
            resource: std::collections::BTreeMap::new(),
            scope_name: "test".to_string(),
            scope_version: "1.0".to_string(),
            scope_attrs: std::collections::BTreeMap::new(),
        }),
        tail: ExemplarTail {
            severity: 9,
            body: Some(bytes::Bytes::from_static(b"spill-payload-body")),
            dims: Vec::new(),
            attrs: Vec::new(),
        },
        counters: vec![[1, 0, 0, 0]],
        minmax: vec![(10, 20)],
    };

    spill_writer.spill(42, &[record]).unwrap();
    let spill_file = spill_writer.spill_dir().join(format!("{:020}.ndjson", 42));
    let spill_len = std::fs::metadata(&spill_file).unwrap().len();
    assert!(spill_len > 0);

    assert_eq!(store.disk_bytes().unwrap(), initial + spill_len);

    spill_writer.remove(42).unwrap();
    assert_eq!(store.disk_bytes().unwrap(), initial);
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

#[test]
fn open_removes_orphan_tail_keyspace() {
    let d = tempfile::tempdir().unwrap();
    let meta = PodMeta::load_or_init(d.path(), "pod-0", 1, b"s").unwrap();
    let store_opts = opts(d.path());
    let s = ShardStore::open(store_opts.clone(), &meta).unwrap();
    drop(s);

    {
        let db = fjall::Database::builder(&store_opts.shard_dir)
            .open()
            .unwrap();
        let orphan_tail = db
            .keyspace(
                "w00000000000000000000_tail",
                fjall::KeyspaceCreateOptions::default,
            )
            .unwrap();
        orphan_tail.insert(b"k", b"v").unwrap();
        db.persist(fjall::PersistMode::SyncAll).unwrap();
        assert!(
            db.list_keyspace_names()
                .iter()
                .any(|n| n.as_ref() == "w00000000000000000000_tail")
        );
    }

    let s = ShardStore::open(store_opts, &meta).unwrap();
    assert!(
        !s.list_keyspace_names()
            .iter()
            .any(|n| n == "w00000000000000000000_tail")
    );
}

#[test]
fn drop_window_deletes_tail_before_hdr() {
    let d = tempfile::tempdir().unwrap();
    let meta = PodMeta::load_or_init(d.path(), "pod-0", 1, b"s").unwrap();
    let k = SimdKernelTable::detect(SimdMode::Scalar);
    let mut s = ShardStore::open(opts(d.path()), &meta).unwrap();
    s.apply_flush(batch(vec![entry(1, true, 1)]), &k).unwrap();

    let key = StorageKey24 {
        epoch_ms: 0,
        digest: 1,
    };
    assert!(s.header(&key).unwrap().is_some());
    assert!(s.tail(&key).unwrap().is_some());

    s.set_fail_drop_after_tail(true);
    assert!(s.drop_window(0).is_err());

    // _tail must be deleted first, while _hdr remains as the durable commit marker.
    assert!(
        !s.list_keyspace_names()
            .iter()
            .any(|n| n == "w00000000000000000000_tail")
    );
    assert!(
        s.list_keyspace_names()
            .iter()
            .any(|n| n == "w00000000000000000000_hdr")
    );
    assert_eq!(s.live_windows(), vec![0]);
    assert!(s.header(&key).unwrap().is_some());
    assert!(s.tail(&key).unwrap().is_none());

    // Subsequent retry completes deletion of _hdr.
    s.drop_window(0).unwrap();
    assert!(s.live_windows().is_empty());
    assert!(
        !s.list_keyspace_names()
            .iter()
            .any(|n| n == "w00000000000000000000_hdr")
    );
}

#[test]
fn shard_store_opens_with_limits() {
    let d = tempfile::tempdir().unwrap();
    let meta = PodMeta::load_or_init(d.path(), "pod-0", 1, b"s").unwrap();
    let k = SimdKernelTable::detect(SimdMode::Scalar);

    let mut limited_opts = opts(d.path());
    limited_opts.max_write_buffer_bytes = Some(64 * 1024 * 1024);
    limited_opts.max_cached_files = Some(64);

    let mut store = ShardStore::open(limited_opts, &meta).unwrap();
    let st = store
        .apply_flush(batch(vec![entry(1, true, 42)]), &k)
        .unwrap();
    assert_eq!(st.hdr_writes, 1);

    let key = StorageKey24 {
        epoch_ms: 0,
        digest: 1,
    };
    let hdr = store.header(&key).unwrap().unwrap();
    assert_eq!(hdr.total[0][0], 42);

    // Validate too small max_write_buffer_bytes (< 1 MiB) is rejected
    let mut bad_opts = opts(d.path());
    bad_opts.max_write_buffer_bytes = Some(512 * 1024);
    assert!(matches!(
        ShardStore::open(bad_opts, &meta),
        Err(AggError::Config(ref m)) if m.contains("max_write_buffer_bytes")
    ));

    // Validate 0 max_cached_files is rejected
    let mut bad_fd_opts = opts(d.path());
    bad_fd_opts.max_cached_files = Some(0);
    assert!(matches!(
        ShardStore::open(bad_fd_opts, &meta),
        Err(AggError::Config(ref m)) if m.contains("max_cached_files")
    ));
}

#[test]
fn header_and_tail_lookups_increment_scan_point_gets() {
    let d = tempfile::tempdir().unwrap();
    let meta = PodMeta::load_or_init(d.path(), "pod-0", 1, b"s").unwrap();
    let k = SimdKernelTable::detect(SimdMode::Scalar);
    let mut store = ShardStore::open(opts(d.path()), &meta).unwrap();

    let key = StorageKey24 {
        epoch_ms: 0,
        digest: 1,
    };
    store
        .apply_flush(batch(vec![entry(1, true, 42)]), &k)
        .unwrap();

    assert_eq!(store.scan_point_gets(), 0);
    assert!(store.header(&key).unwrap().is_some());
    assert!(store.tail(&key).unwrap().is_some());
    assert_eq!(store.scan_point_gets(), 2);
    assert_eq!(
        store
            .scan_point_gets_counter()
            .load(std::sync::atomic::Ordering::Relaxed),
        2
    );
}

#[test]
fn rows_scan_does_not_increment_scan_point_gets() {
    let d = tempfile::tempdir().unwrap();
    let meta = PodMeta::load_or_init(d.path(), "pod-0", 1, b"s").unwrap();
    let k = SimdKernelTable::detect(SimdMode::Scalar);
    let mut store = ShardStore::open(opts(d.path()), &meta).unwrap();

    store
        .apply_flush(
            batch(vec![
                entry(1, true, 10),
                entry(2, true, 20),
                entry(3, true, 30),
            ]),
            &k,
        )
        .unwrap();

    assert_eq!(store.scan_point_gets(), 0);
    let rows = store.rows(0, None, 10).unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(store.scan_point_gets(), 0);
}

#[test]
fn rows_for_increments_scan_point_gets() {
    let d = tempfile::tempdir().unwrap();
    let meta = PodMeta::load_or_init(d.path(), "pod-0", 1, b"s").unwrap();
    let k = SimdKernelTable::detect(SimdMode::Scalar);
    let mut store = ShardStore::open(opts(d.path()), &meta).unwrap();

    let key1 = StorageKey24 {
        epoch_ms: 0,
        digest: 1,
    };
    let key2 = StorageKey24 {
        epoch_ms: 0,
        digest: 2,
    };
    store
        .apply_flush(batch(vec![entry(1, true, 10), entry(2, true, 20)]), &k)
        .unwrap();

    assert_eq!(store.scan_point_gets(), 0);
    let rows = store.rows_for(&[key1, key2]).unwrap();
    assert_eq!(rows.len(), 2);
    // 2 keys * 2 gets (hdr + tail) = 4 point gets
    assert_eq!(store.scan_point_gets(), 4);
}

#[test]
fn rows_for_sorted_returns_dense_keys_in_key_order_with_tails() {
    let d = tempfile::tempdir().unwrap();
    let meta = PodMeta::load_or_init(d.path(), "pod-0", 1, b"s").unwrap();
    let k = SimdKernelTable::detect(SimdMode::Scalar);
    let mut store = ShardStore::open(opts(d.path()), &meta).unwrap();

    let mut entries = Vec::with_capacity(100);
    for digest in 1..=100 {
        let with_tail = digest % 2 == 0;
        entries.push(entry_with_epoch_and_tail(
            0,
            digest,
            true,
            with_tail,
            digest as u64,
        ));
    }
    store.apply_flush(batch(entries), &k).unwrap();

    // Pass keys in reverse order to verify sorting
    let input_keys: Vec<StorageKey24> = (1..=100)
        .rev()
        .map(|digest| StorageKey24 {
            epoch_ms: 0,
            digest,
        })
        .collect();

    let rows = store.rows_for_sorted(&input_keys).unwrap();
    assert_eq!(rows.len(), 100);
    for (i, row) in rows.iter().enumerate() {
        let expected_digest = (i + 1) as u128;
        assert_eq!(row.key.epoch_ms, 0);
        assert_eq!(row.key.digest, expected_digest);
        assert_eq!(row.header.total[0][0], expected_digest as u64);
        if expected_digest % 2 == 0 {
            assert!(row.tail.is_some());
        } else {
            assert!(row.tail.is_none());
        }
    }
    assert_eq!(store.scan_point_gets(), 0);
}

#[test]
fn rows_for_sorted_skips_missing_keys_and_collapses_duplicates() {
    let d = tempfile::tempdir().unwrap();
    let meta = PodMeta::load_or_init(d.path(), "pod-0", 1, b"s").unwrap();
    let k = SimdKernelTable::detect(SimdMode::Scalar);
    let mut store = ShardStore::open(opts(d.path()), &meta).unwrap();

    store
        .apply_flush(
            batch(vec![
                entry(10, true, 100),
                entry(20, true, 200),
                entry(30, true, 300),
            ]),
            &k,
        )
        .unwrap();

    // Query with duplicates, missing keys (5, 25, 40), and out-of-order
    let query_keys = vec![
        StorageKey24 {
            epoch_ms: 0,
            digest: 20,
        },
        StorageKey24 {
            epoch_ms: 0,
            digest: 5,
        },
        StorageKey24 {
            epoch_ms: 0,
            digest: 20,
        },
        StorageKey24 {
            epoch_ms: 0,
            digest: 10,
        },
        StorageKey24 {
            epoch_ms: 0,
            digest: 25,
        },
        StorageKey24 {
            epoch_ms: 0,
            digest: 30,
        },
        StorageKey24 {
            epoch_ms: 0,
            digest: 20,
        },
        StorageKey24 {
            epoch_ms: 0,
            digest: 40,
        },
    ];

    let rows = store.rows_for_sorted(&query_keys).unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].key.digest, 10);
    assert_eq!(rows[0].header.total[0][0], 100);
    assert_eq!(rows[1].key.digest, 20);
    assert_eq!(rows[1].header.total[0][0], 200);
    assert_eq!(rows[2].key.digest, 30);
    assert_eq!(rows[2].header.total[0][0], 300);
    assert_eq!(store.scan_point_gets(), 0);
}

#[test]
fn rows_for_sorted_sparse_keys_across_large_window_reseeks() {
    let d = tempfile::tempdir().unwrap();
    let meta = PodMeta::load_or_init(d.path(), "pod-0", 1, b"s").unwrap();
    let k = SimdKernelTable::detect(SimdMode::Scalar);
    let mut store = ShardStore::open(opts(d.path()), &meta).unwrap();

    // 5,000 keys
    let entries: Vec<FlushEntry> = (0..5000).map(|d| entry(d, true, d as u64)).collect();
    store.apply_flush(batch(entries), &k).unwrap();

    // Request digests 1, 2,500, 4,999
    let query_keys = vec![
        StorageKey24 {
            epoch_ms: 0,
            digest: 1,
        },
        StorageKey24 {
            epoch_ms: 0,
            digest: 2500,
        },
        StorageKey24 {
            epoch_ms: 0,
            digest: 4999,
        },
    ];

    let rows = store.rows_for_sorted(&query_keys).unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].key.digest, 1);
    assert_eq!(rows[0].header.total[0][0], 1);
    assert!(rows[0].tail.is_some());

    assert_eq!(rows[1].key.digest, 2500);
    assert_eq!(rows[1].header.total[0][0], 2500);
    assert!(rows[1].tail.is_some());

    assert_eq!(rows[2].key.digest, 4999);
    assert_eq!(rows[2].header.total[0][0], 4999);
    assert!(rows[2].tail.is_some());

    assert_eq!(store.scan_point_gets(), 0);
}

#[test]
fn rows_for_sorted_spans_multiple_epochs_and_absent_epoch() {
    let d = tempfile::tempdir().unwrap();
    let meta = PodMeta::load_or_init(d.path(), "pod-0", 1, b"s").unwrap();
    let k = SimdKernelTable::detect(SimdMode::Scalar);
    let mut store = ShardStore::open(opts(d.path()), &meta).unwrap();

    // Epoch 1000: digests 1, 2
    // Epoch 2000: digests 3, 4
    let entries = vec![
        entry_with_epoch_and_tail(1000, 1, true, true, 10),
        entry_with_epoch_and_tail(1000, 2, true, false, 20),
        entry_with_epoch_and_tail(2000, 3, true, true, 30),
        entry_with_epoch_and_tail(2000, 4, true, false, 40),
    ];
    store.apply_flush(batch(entries), &k).unwrap();

    // Query keys from epoch 1000, absent epoch 1500, epoch 2000, and absent epoch 3000
    let query_keys = vec![
        StorageKey24 {
            epoch_ms: 1000,
            digest: 2,
        },
        StorageKey24 {
            epoch_ms: 1000,
            digest: 1,
        },
        StorageKey24 {
            epoch_ms: 1500,
            digest: 99,
        },
        StorageKey24 {
            epoch_ms: 2000,
            digest: 4,
        },
        StorageKey24 {
            epoch_ms: 2000,
            digest: 3,
        },
        StorageKey24 {
            epoch_ms: 3000,
            digest: 50,
        },
    ];

    let rows = store.rows_for_sorted(&query_keys).unwrap();
    assert_eq!(rows.len(), 4);
    assert_eq!(
        rows[0].key,
        StorageKey24 {
            epoch_ms: 1000,
            digest: 1
        }
    );
    assert_eq!(rows[0].header.total[0][0], 10);
    assert!(rows[0].tail.is_some());

    assert_eq!(
        rows[1].key,
        StorageKey24 {
            epoch_ms: 1000,
            digest: 2
        }
    );
    assert_eq!(rows[1].header.total[0][0], 20);
    assert!(rows[1].tail.is_none());

    assert_eq!(
        rows[2].key,
        StorageKey24 {
            epoch_ms: 2000,
            digest: 3
        }
    );
    assert_eq!(rows[2].header.total[0][0], 30);
    assert!(rows[2].tail.is_some());

    assert_eq!(
        rows[3].key,
        StorageKey24 {
            epoch_ms: 2000,
            digest: 4
        }
    );
    assert_eq!(rows[3].header.total[0][0], 40);
    assert!(rows[3].tail.is_none());

    assert_eq!(store.scan_point_gets(), 0);
}

#[test]
fn rows_for_sorted_matches_header_and_tail_point_lookups() {
    let d = tempfile::tempdir().unwrap();
    let meta = PodMeta::load_or_init(d.path(), "pod-0", 1, b"s").unwrap();
    let k = SimdKernelTable::detect(SimdMode::Scalar);
    let mut store = ShardStore::open(opts(d.path()), &meta).unwrap();

    let mut entries = Vec::with_capacity(50);
    for digest in 1..=50 {
        let with_tail = digest % 3 != 0;
        entries.push(entry_with_epoch_and_tail(
            0,
            digest,
            true,
            with_tail,
            digest as u64 * 7,
        ));
    }
    store.apply_flush(batch(entries), &k).unwrap();

    let keys: Vec<StorageKey24> = (1..=50)
        .map(|digest| StorageKey24 {
            epoch_ms: 0,
            digest,
        })
        .collect();

    let rows = store.rows_for_sorted(&keys).unwrap();
    assert_eq!(rows.len(), 50);

    // Cross-check each returned row against header() and tail()
    // Notice header() and tail() increment scan_point_gets(), so we assert 0 before point lookups
    assert_eq!(store.scan_point_gets(), 0);

    for row in &rows {
        let h = store
            .header(&row.key)
            .unwrap()
            .expect("header should exist");
        assert_eq!(row.header.total, h.total);
        assert_eq!(row.header.last_update_s, h.last_update_s);

        let t = store.tail(&row.key).unwrap();
        assert_eq!(
            row.tail.as_ref().map(|x| x.severity),
            t.as_ref().map(|x| x.severity)
        );
    }

    // 50 headers + 50 tails looked up via point lookups = 100 point gets
    assert_eq!(store.scan_point_gets(), 100);
}
