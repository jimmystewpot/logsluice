#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

use bytes::Bytes;
use logsluice_core::{
    codec::{
        tail::ExemplarTail,
        ticket::{EmitReason, Ticket, TicketEntry},
    },
    key::StorageKey24,
    l1::*,
    resource::ResourceScopePayload,
    simd::{SimdKernelTable, SimdMode},
    store::thread::*,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

mod common;

#[test]
fn swap_refused_while_flush_in_flight() {
    let mut db: DoubleBuffer<u32> = DoubleBuffer::new(4);
    db.active_mut().push_finalizer(1);
    let first = db.try_swap();
    assert!(first.is_some() && db.flush_in_flight());
    assert!(db.try_swap().is_none());
    db.flush_completed();
    assert!(db.try_swap().is_some());
}

#[test]
fn finalizers_released_only_after_durable_flush() {
    let (store, k, _d) = common::store_with_layout();
    let (h, mut ev, _j) = StoreThread::spawn::<u32>(store, k).unwrap();
    let mut buf = L1Buffer::new(4);
    buf.accumulate(
        StorageKey24 {
            epoch_ms: 0,
            digest: 1,
        },
        true,
        1,
        &[[1, 0, 0, 0]],
        &[5, 5],
        0,
        Some(common::tail()),
        &k,
    );
    buf.push_finalizer(77);
    h.send(StoreCommand::Flush(buf)).unwrap();
    match common::recv(&mut ev, Duration::from_secs(5)) {
        StoreEvent::FlushDone {
            finalizers, stats, ..
        } => {
            assert_eq!(finalizers, vec![77]);
            assert_eq!(stats.tail_writes, 1);
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn rows_are_resumable_in_chunks() {
    let (store, k, _d) = common::store_with_keys(&[5, 1, 3]);
    let (h, mut ev, _j) = StoreThread::spawn::<u32>(store, k).unwrap();
    h.send(StoreCommand::Rows {
        epoch_ms: 0,
        reason: EmitReason::Close,
        after: None,
        keys: None,
        limit: 2,
        recovery_ticket: None,
    })
    .unwrap();
    let StoreEvent::Rows {
        rows, next_after, ..
    } = common::recv(&mut ev, Duration::from_secs(5))
    else {
        panic!("expected StoreEvent::Rows")
    };
    assert_eq!(
        rows.iter().map(|r| r.key.digest).collect::<Vec<_>>(),
        vec![1, 3]
    );
    h.send(StoreCommand::Rows {
        epoch_ms: 0,
        reason: EmitReason::Close,
        after: next_after,
        keys: None,
        limit: 2,
        recovery_ticket: None,
    })
    .unwrap();
    let StoreEvent::Rows {
        rows, next_after, ..
    } = common::recv(&mut ev, Duration::from_secs(5))
    else {
        panic!("expected StoreEvent::Rows")
    };
    assert_eq!((rows.len(), next_after), (1, None));
}

#[test]
fn l1_accumulation_merging_sticky_new_and_first_tail_wins() {
    let k = SimdKernelTable::detect(SimdMode::Scalar);
    let mut buf: L1Buffer<String> = L1Buffer::new(8);
    assert!(buf.is_empty());
    assert_eq!(buf.len(), 0);

    let key = StorageKey24 {
        epoch_ms: 1000,
        digest: 42,
    };

    // First accumulate: is_new = false, tail = None
    buf.accumulate(
        key,
        false,
        10,
        &[[10, 20, 30, 40]],
        &[50, 100],
        10,
        None,
        &k,
    );
    assert_eq!(buf.len(), 1);
    assert!(!buf.is_empty());

    // Second accumulate: is_new = true (sticky!), tail = Some(tail1), update last_update_s
    let tail1 = ExemplarTail {
        severity: 3,
        body: Some(Bytes::from_static(b"body1")),
        dims: vec![],
        attrs: vec![],
    };
    buf.accumulate(
        key,
        true,
        10,
        &[[5, 5, 5, 5]],
        &[20, 150],
        25,
        Some(tail1.clone()),
        &k,
    );

    // Third accumulate: is_new = false (must stay sticky true), tail = Some(tail2) (tail1 must win!)
    let tail2 = ExemplarTail {
        severity: 9,
        body: Some(Bytes::from_static(b"body2")),
        dims: vec![],
        attrs: vec![],
    };
    buf.accumulate(
        key,
        false,
        10,
        &[[1, 1, 1, 1]],
        &[100, 80],
        15, // lower than 25, should not regress
        Some(tail2),
        &k,
    );

    buf.push_finalizer("fin-1".to_string());
    buf.record_offsets(vec![
        (("topic-a".into(), 0), 100),
        (("topic-a".into(), 0), 150), // should keep max: 150
        (("topic-b".into(), 1), 200),
    ]);

    let payload = Arc::new(ResourceScopePayload {
        resource: BTreeMap::new(),
        scope_name: "test-scope".to_string(),
        scope_version: "1.0.0".to_string(),
        scope_attrs: BTreeMap::new(),
    });
    buf.record_resource(7, payload.clone(), 1000);

    let (batch, finalizers) = buf.into_flush();
    assert_eq!(finalizers, vec!["fin-1"]);
    assert_eq!(batch.entries.len(), 1);

    let entry = &batch.entries[0];
    assert_eq!(entry.key, key);
    assert!(entry.is_new, "is_new must be sticky true");
    assert_eq!(entry.delta, vec![[16, 26, 36, 46]]);
    assert_eq!(entry.minmax, vec![(20, 100), (80, 150)]);
    assert_eq!(entry.last_update_s, 25);
    assert_eq!(entry.tail, Some(tail1), "first tail seen must be retained");

    assert_eq!(batch.offsets.len(), 2);
    assert_eq!(batch.offsets[0], (("topic-a".into(), 0), 150));
    assert_eq!(batch.offsets[1], (("topic-b".into(), 1), 200));

    assert_eq!(batch.new_resources.len(), 1);
    assert_eq!(batch.new_resources[0].0, 7);
    assert_eq!(batch.resource_refs, vec![(7, 1000)]);
}

#[test]
fn store_thread_tickets_and_ack_lifecycle() {
    let (store, k, _d) = common::store_with_keys(&[10]);
    let (h, mut ev, j) = StoreThread::spawn::<u32>(store, k).unwrap();

    let ticket = Ticket {
        id: 999,
        window_epoch_ms: 0,
        reason: EmitReason::Close,
        entries: vec![TicketEntry {
            digest: 10,
            slice_seq: 0,
            delta: vec![[1, 0, 0, 0]],
            minmax: vec![(5, 5), (5, 5)],
        }],
    };

    h.send(StoreCommand::PersistTickets(vec![ticket.clone()]))
        .unwrap();
    match common::recv(&mut ev, Duration::from_secs(5)) {
        StoreEvent::TicketsPersisted(ids) => assert_eq!(ids, vec![999]),
        other => panic!("unexpected {other:?}"),
    }

    h.send(StoreCommand::Ack(ticket)).unwrap();
    match common::recv(&mut ev, Duration::from_secs(5)) {
        StoreEvent::AckApplied { ticket_id: id, .. } => assert_eq!(id, 999),
        other => panic!("unexpected {other:?}"),
    }

    h.send(StoreCommand::Shutdown).unwrap();
    j.join().unwrap();
}

#[test]
fn store_thread_rows_for_explicit_keys() {
    let (store, k, _d) = common::store_with_keys(&[10, 20, 30]);
    let (h, mut ev, j) = StoreThread::spawn::<u32>(store, k).unwrap();

    let query_keys = vec![
        StorageKey24 {
            epoch_ms: 0,
            digest: 10,
        },
        StorageKey24 {
            epoch_ms: 0,
            digest: 30,
        },
    ];

    h.send(StoreCommand::Rows {
        epoch_ms: 0,
        reason: EmitReason::Periodic,
        after: None,
        keys: Some(query_keys),
        limit: 10,
        recovery_ticket: Some(77),
    })
    .unwrap();

    match common::recv(&mut ev, Duration::from_secs(5)) {
        StoreEvent::Rows {
            epoch_ms,
            reason,
            rows,
            next_after,
            recovery_ticket,
        } => {
            assert_eq!(epoch_ms, 0);
            assert_eq!(reason, EmitReason::Periodic);
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].key.digest, 10);
            assert_eq!(rows[1].key.digest, 30);
            assert_eq!(next_after, None);
            assert_eq!(recovery_ticket, Some(77));
        }
        other => panic!("unexpected {other:?}"),
    }

    h.send(StoreCommand::Shutdown).unwrap();
    j.join().unwrap();
}

#[test]
fn store_thread_delete_keys_and_drop_window() {
    let (store, k, _d) = common::store_with_keys(&[10, 20]);
    let (h, mut ev, j) = StoreThread::spawn::<u32>(store, k).unwrap();

    let delete_key = StorageKey24 {
        epoch_ms: 0,
        digest: 10,
    };
    h.send(StoreCommand::DeleteKeys {
        epoch_ms: 0,
        keys: vec![delete_key],
    })
    .unwrap();

    match common::recv(&mut ev, Duration::from_secs(5)) {
        StoreEvent::KeysDeleted { epoch_ms, count } => {
            assert_eq!(epoch_ms, 0);
            assert_eq!(count, 1);
        }
        other => panic!("unexpected {other:?}"),
    }

    h.send(StoreCommand::DropWindow {
        epoch_ms: 0,
        release_resources: vec![1],
    })
    .unwrap();

    match common::recv(&mut ev, Duration::from_secs(5)) {
        StoreEvent::WindowDropped(epoch) => {
            assert_eq!(epoch, 0);
        }
        other => panic!("unexpected {other:?}"),
    }

    h.send(StoreCommand::Shutdown).unwrap();
    j.join().unwrap();
}

#[test]
fn flush_failure_returns_finalizers() {
    let (store, k, _d) = common::store_with_layout();
    let (h, mut ev, j) = StoreThread::spawn::<u32>(store, k).unwrap();
    let mut buf = L1Buffer::new(4);
    buf.accumulate(
        StorageKey24 {
            epoch_ms: 0,
            digest: 1,
        },
        true,
        1,
        &[[1, 0, 0, 0]],
        &[5, 5],
        0,
        Some(common::tail()),
        &k,
    );
    buf.push_finalizer(77);
    buf.push_finalizer(88);

    h.fail_next_flush();
    h.send(StoreCommand::Flush(buf)).unwrap();
    match common::recv(&mut ev, Duration::from_secs(5)) {
        StoreEvent::FlushFailed {
            error: _,
            batch,
            finalizers,
        } => {
            assert_eq!(finalizers, vec![77, 88]);
            assert_eq!(batch.entries.len(), 1);
            assert_eq!(batch.entries[0].key.digest, 1);
        }
        other => panic!("expected StoreEvent::FlushFailed, got {other:?}"),
    }

    h.send(StoreCommand::Shutdown).unwrap();
    j.join().unwrap();
}

#[test]
fn disk_bytes_failure_after_committed_flush_still_reports_flush_done() {
    let (store, k, _d) = common::store_with_layout();
    let (h, mut ev, j) = StoreThread::spawn::<u32>(store, k).unwrap();
    let mut buf = L1Buffer::new(4);
    buf.accumulate(
        StorageKey24 {
            epoch_ms: 0,
            digest: 1,
        },
        true,
        1,
        &[[1, 0, 0, 0]],
        &[5, 5],
        0,
        Some(common::tail()),
        &k,
    );
    buf.push_finalizer(77);

    h.fail_next_disk_bytes();
    h.send(StoreCommand::Flush(buf)).unwrap();
    match common::recv(&mut ev, Duration::from_secs(5)) {
        StoreEvent::FlushDone {
            finalizers, stats, ..
        } => {
            assert_eq!(finalizers, vec![77]);
            assert_eq!(stats.hdr_writes, 1);
            assert_eq!(stats.tail_writes, 1);
        }
        other => panic!("expected StoreEvent::FlushDone, got {other:?}"),
    }

    h.send(StoreCommand::Rows {
        epoch_ms: 0,
        reason: EmitReason::Close,
        after: None,
        keys: None,
        limit: 10,
        recovery_ticket: None,
    })
    .unwrap();
    match common::recv(&mut ev, Duration::from_secs(5)) {
        StoreEvent::Rows { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].key.digest, 1);
        }
        other => panic!("expected StoreEvent::Rows, got {other:?}"),
    }

    h.send(StoreCommand::Shutdown).unwrap();
    j.join().unwrap();
}

#[test]
fn try_send_returns_command_when_queue_full() {
    let (store, k, _d) = common::store_with_layout();
    let (h, _ev, j) = StoreThread::spawn::<u32>(store, k).unwrap();

    let barrier = Arc::new(std::sync::Barrier::new(2));
    h.try_send(StoreCommand::TestPause(Arc::clone(&barrier)))
        .expect("TestPause should enqueue when queue is empty");
    // Wait until the store thread has popped TestPause and is stalled at the second barrier wait.
    barrier.wait();

    for epoch_ms in 0..64 {
        h.try_send(StoreCommand::DeleteKeys {
            epoch_ms,
            keys: Vec::new(),
        })
        .unwrap_or_else(|_| panic!("expected slot {epoch_ms} to succeed before queue is full"));
    }

    let overflow_key = StorageKey24 {
        epoch_ms: 999,
        digest: 42,
    };
    let res = h.try_send(StoreCommand::DeleteKeys {
        epoch_ms: 999,
        keys: vec![overflow_key],
    });
    match res {
        Err(StoreCommand::DeleteKeys { epoch_ms, keys }) => {
            assert_eq!(epoch_ms, 999);
            assert_eq!(keys, vec![overflow_key]);
        }
        other => panic!("expected Err(StoreCommand::DeleteKeys) when queue full, got {other:?}"),
    }

    // Release the stalled store thread and shut down cleanly.
    barrier.wait();
    h.send(StoreCommand::Shutdown).unwrap();
    j.join().unwrap();
}

#[test]
fn double_buffer_tracks_active_and_inflight_keys_across_swap_and_completion() {
    let mut db: DoubleBuffer<()> = DoubleBuffer::new(16);
    let k1 = StorageKey24 {
        epoch_ms: 1_000,
        digest: 1,
    };
    let k2 = StorageKey24 {
        epoch_ms: 1_000,
        digest: 2,
    };
    let kernels = SimdKernelTable::scalar();

    assert!(!db.contains_key_active_or_inflight(&k1));
    db.active_mut()
        .accumulate(k1, true, 0, &[[1, 0, 0, 0]], &[], 1, None, &kernels);
    assert!(db.contains_key_active_or_inflight(&k1));

    let swapped = db.try_swap().expect("first swap succeeds");
    assert_eq!(swapped.len(), 1);
    assert!(db.active().is_empty());
    assert!(db.contains_key_active_or_inflight(&k1));

    db.active_mut()
        .accumulate(k2, true, 0, &[[1, 0, 0, 0]], &[], 1, None, &kernels);
    assert!(db.contains_key_active_or_inflight(&k1));
    assert!(db.contains_key_active_or_inflight(&k2));

    db.flush_completed();
    assert!(!db.contains_key_active_or_inflight(&k1));
    assert!(db.contains_key_active_or_inflight(&k2));
}

#[test]
fn store_thread_emits_spill_failed_on_io_errors() {
    let (store, k, _d) = common::store_with_layout();
    let shard_dir = store.shard_dir().to_path_buf();
    let (h, mut ev, j) = StoreThread::spawn::<u32>(store, k).unwrap();

    let spill_dir = shard_dir.join("spill");
    std::fs::create_dir_all(spill_dir.join(format!("{:020}.ndjson", 1u64))).unwrap();

    h.send(StoreCommand::RemoveSpill { ticket_id: 1 }).unwrap();
    match common::recv(&mut ev, Duration::from_secs(5)) {
        StoreEvent::SpillFailed {
            ticket_id: Some(1), ..
        } => {}
        other => panic!("expected StoreEvent::SpillFailed for RemoveSpill, got {other:?}"),
    }

    std::fs::remove_dir_all(&spill_dir).unwrap();
    std::fs::write(&spill_dir, b"not-a-dir").unwrap();

    h.send(StoreCommand::ReplaySpill).unwrap();
    match common::recv(&mut ev, Duration::from_secs(5)) {
        StoreEvent::SpillFailed {
            ticket_id: None, ..
        } => {}
        other => panic!("expected StoreEvent::SpillFailed for ReplaySpill, got {other:?}"),
    }

    h.send(StoreCommand::Spill {
        ticket_id: 2,
        records: Vec::new(),
    })
    .unwrap();
    match common::recv(&mut ev, Duration::from_secs(5)) {
        StoreEvent::SpillFailed {
            ticket_id: Some(2), ..
        } => {}
        other => panic!("expected StoreEvent::SpillFailed for Spill, got {other:?}"),
    }

    h.send(StoreCommand::Shutdown).unwrap();
    j.join().unwrap();
}

#[test]
fn store_handle_exposes_scan_point_gets() {
    let (store, k, _d) = common::store_with_layout();
    let (h, _ev, j) = StoreThread::spawn::<u32>(store, k).unwrap();
    assert_eq!(h.scan_point_gets(), 0);
    h.send(StoreCommand::Shutdown).unwrap();
    j.join().unwrap();
}

#[test]
fn store_thread_keyed_rows_do_no_point_reads() {
    let all_keys: Vec<u128> = (1..=2000).collect();
    let (store, k, _d) = common::store_with_keys(&all_keys);
    let (h, mut ev, j) = StoreThread::spawn::<u32>(store, k).unwrap();

    // Pick 500 unsorted keys (e.g., reversed order).
    let query_keys: Vec<StorageKey24> = (1..=500)
        .rev()
        .map(|digest| StorageKey24 {
            epoch_ms: 0,
            digest,
        })
        .collect();

    h.send(StoreCommand::Rows {
        epoch_ms: 0,
        reason: EmitReason::Periodic,
        after: None,
        keys: Some(query_keys),
        limit: 1000,
        recovery_ticket: Some(1),
    })
    .unwrap();

    match common::recv(&mut ev, Duration::from_secs(5)) {
        StoreEvent::Rows {
            epoch_ms,
            reason,
            rows,
            next_after,
            recovery_ticket,
        } => {
            assert_eq!(epoch_ms, 0);
            assert_eq!(reason, EmitReason::Periodic);
            assert_eq!(rows.len(), 500);
            let mut returned_digests: Vec<u128> = rows.iter().map(|r| r.key.digest).collect();
            returned_digests.sort_unstable();
            let expected_digests: Vec<u128> = (1..=500).collect();
            assert_eq!(returned_digests, expected_digests);
            assert_eq!(next_after, None);
            assert_eq!(recovery_ticket, Some(1));
        }
        other => panic!("unexpected {other:?}"),
    }

    assert_eq!(h.scan_point_gets(), 0);

    h.send(StoreCommand::Shutdown).unwrap();
    j.join().unwrap();
}
