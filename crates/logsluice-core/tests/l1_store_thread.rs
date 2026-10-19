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
    })
    .unwrap();

    match common::recv(&mut ev, Duration::from_secs(5)) {
        StoreEvent::Rows {
            epoch_ms,
            reason,
            rows,
            next_after,
        } => {
            assert_eq!(epoch_ms, 0);
            assert_eq!(reason, EmitReason::Periodic);
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].key.digest, 10);
            assert_eq!(rows[1].key.digest, 30);
            assert_eq!(next_after, None);
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
