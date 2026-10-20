#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

use std::process::Command;

use tempfile::TempDir;

fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_logsluice"))
}

#[test]
fn replay_with_crashes_matches_totals() {
    let d = tempfile::tempdir().unwrap();
    let o = bin()
        .args([
            "replay",
            "--events",
            "200000",
            "--keys",
            "5000",
            "--seed",
            "7",
            "--shards",
            "2",
            "--crash-every",
            "25000",
            "--data-dir",
        ])
        .arg(d.path())
        .output()
        .unwrap();
    let s = String::from_utf8_lossy(&o.stdout);
    assert!(o.status.success(), "{s}");
    assert!(s.contains("totals_match=true"));
}

#[test]
fn drain_emits_pending_state() {
    let d = tempfile::tempdir().unwrap();
    assert!(
        bin()
            .args([
                "replay",
                "--events",
                "1000",
                "--keys",
                "10",
                "--seed",
                "1",
                "--shards",
                "1",
                "--stop-before-close",
                "--data-dir"
            ])
            .arg(d.path())
            .status()
            .unwrap()
            .success()
    );
    let o = bin()
        .args(["drain", "--format", "ndjson", "--data-dir"])
        .arg(d.path())
        .output()
        .unwrap();
    let total: u64 = String::from_utf8_lossy(&o.stdout)
        .lines()
        .map(|l| {
            serde_json::from_str::<serde_json::Value>(l).unwrap()["counters"][0][0]
                .as_u64()
                .unwrap()
        })
        .sum();
    assert_eq!(total, 1000);
}

#[test]
fn drain_uses_persisted_window_duration_and_drains_spill_files() {
    let dir = TempDir::new().unwrap();
    let bin = env!("CARGO_BIN_EXE_logsluice");

    // 1. Run replay with custom --window-duration-ms 60000 and --stop-before-close
    let st = Command::new(bin)
        .args([
            "replay",
            "--events",
            "500",
            "--keys",
            "50",
            "--seed",
            "11",
            "--shards",
            "1",
            "--window-duration-ms",
            "60000",
            "--data-dir",
            dir.path().to_str().unwrap(),
            "--stop-before-close",
        ])
        .status()
        .unwrap();
    assert!(st.success());

    // 2. Persist ticket 1 + key k7 (digest: 7) in shard_0 and write matching spill/1.ndjson
    let meta: logsluice_core::store::meta::PodMeta =
        serde_json::from_slice(&std::fs::read(dir.path().join("pod_meta.json")).unwrap()).unwrap();
    let layout =
        logsluice_core::codec::plan::HeaderLayout::from_schema_prefix(&meta.codec_schema).unwrap();
    let store_opts = logsluice_core::store::shard_store::StoreOptions {
        shard_dir: dir.path().join("shard_0"),
        cache_bytes: 8 << 20,
        hdr_memtable_bytes: 1 << 20,
        tail_memtable_bytes: 1 << 20,
        sync_wal: true,
        layout,
        worker_threads: 2,
        max_write_buffer_bytes: None,
        max_cached_files: None,
    };
    let k = logsluice_core::simd::SimdKernelTable::detect(logsluice_core::simd::SimdMode::Scalar);
    let mut store =
        logsluice_core::store::shard_store::ShardStore::open(store_opts.clone(), &meta).unwrap();
    let k7 = logsluice_core::key::StorageKey24 {
        epoch_ms: 1_700_000_000_000,
        digest: 7,
    };
    store
        .apply_flush(
            logsluice_core::store::shard_store::FlushBatch {
                entries: vec![logsluice_core::store::shard_store::FlushEntry {
                    key: k7,
                    is_new: true,
                    resource_scope_id: 0,
                    delta: vec![[42, 0, 0, 0]],
                    minmax: vec![],
                    last_update_s: 1,
                    tail: None,
                }],
                offsets: Vec::new(),
                new_resources: Vec::new(),
                resource_refs: Vec::new(),
            },
            &k,
        )
        .unwrap();
    let spilled_ticket = logsluice_core::codec::ticket::Ticket {
        id: 1,
        window_epoch_ms: 1_700_000_000_000,
        reason: logsluice_core::codec::ticket::EmitReason::Close,
        entries: vec![logsluice_core::codec::ticket::TicketEntry {
            digest: 7,
            slice_seq: 0,
            delta: vec![[42, 0, 0, 0]],
            minmax: vec![],
        }],
    };
    store.put_tickets(&[spilled_ticket]).unwrap();
    drop(store);

    let spill_dir = dir.path().join("shard_0").join("spill");
    let mut writer = logsluice_core::worker::spill::SpillWriter::new(spill_dir);
    let spilled_record = logsluice_core::emission::SliceRecord {
        window_start_ms: 1_700_000_000_000,
        window_end_ms: 1_700_000_060_000,
        emit_reason: logsluice_core::codec::ticket::EmitReason::Close,
        emitter_id: "test-emitter".to_string(),
        digest: 7,
        slice_seq: 0,
        slice_id: "test-emitter:1700000000000:00000000000000000000000000000007:0".to_string(),
        resource: std::sync::Arc::new(logsluice_core::resource::ResourceScopePayload {
            resource: Default::default(),
            scope_name: String::new(),
            scope_version: String::new(),
            scope_attrs: Default::default(),
        }),
        tail: logsluice_core::codec::tail::ExemplarTail {
            severity: 9,
            body: Some("spilled".into()),
            dims: vec![],
            attrs: vec![],
        },
        counters: vec![[42, 0, 0, 0]],
        minmax: vec![],
    };
    writer
        .spill(1, std::slice::from_ref(&spilled_record))
        .unwrap();

    // 3. Run drain and check window_end_ms - window_start_ms == 60_000, spill emission, and k7 dedup
    let out = Command::new(bin)
        .args([
            "drain",
            "--data-dir",
            dir.path().to_str().unwrap(),
            "--format",
            "ndjson",
        ])
        .output()
        .unwrap();
    assert!(out.status.success());

    let stdout = String::from_utf8_lossy(&out.stdout);
    let mut total = 0u64;
    let mut k7_emissions = 0usize;
    for line in stdout.lines().filter(|l| !l.trim().is_empty()) {
        let v: serde_json::Value = serde_json::from_str(line).unwrap();
        let start = v["window_start_ms"].as_i64().unwrap();
        let end = v["window_end_ms"].as_i64().unwrap();
        assert_eq!(end - start, 60_000);
        if start == 1_700_000_000_000 && v["digest"].as_u64() == Some(7) {
            k7_emissions += 1;
        }
        total += v["counters"][0][0].as_u64().unwrap();
    }
    assert_eq!(k7_emissions, 1);
    assert_eq!(total, 500 + 42);
    assert_eq!(writer.count_files(), 0);

    let store_after =
        logsluice_core::store::shard_store::ShardStore::open(store_opts, &meta).unwrap();
    assert!(store_after.tickets().unwrap().is_empty());
}

#[test]
fn drain_rejects_unsupported_format_and_missing_meta() {
    let dir = TempDir::new().unwrap();

    let bad_fmt = bin()
        .args(["drain", "--data-dir"])
        .arg(dir.path())
        .args(["--format", "parquet"])
        .output()
        .unwrap();
    assert!(!bad_fmt.status.success());
    assert!(
        String::from_utf8_lossy(&bad_fmt.stderr).contains("unsupported format"),
        "expected unsupported format error"
    );

    let missing_meta = bin()
        .args(["drain", "--data-dir"])
        .arg(dir.path())
        .args(["--format", "ndjson"])
        .output()
        .unwrap();
    assert!(!missing_meta.status.success());
    assert!(
        String::from_utf8_lossy(&missing_meta.stderr).contains("pod_meta.json not found"),
        "expected missing pod_meta.json error"
    );
}

#[test]
fn replay_with_nack_rate_exercises_rng_and_matches_totals() {
    let d = tempfile::tempdir().unwrap();
    let o = bin()
        .args([
            "replay",
            "--events",
            "5000",
            "--keys",
            "100",
            "--seed",
            "13",
            "--shards",
            "2",
            "--nack-rate",
            "0.25",
            "--data-dir",
        ])
        .arg(d.path())
        .output()
        .unwrap();
    let s = String::from_utf8_lossy(&o.stdout);
    assert!(o.status.success(), "{s}");
    assert!(s.contains("totals_match=true"));
}

#[test]
fn drain_replays_in_flight_ticket_with_original_slice_id_and_no_duplicate_delta() {
    use logsluice_core::{
        codec::{
            plan::HeaderLayout,
            ticket::{EmitReason, Ticket, TicketEntry},
        },
        emission::slice_id,
        key::StorageKey24,
        resource::ResourceScopePayload,
        simd::{SimdKernelTable, SimdMode},
        store::{
            meta::PodMeta,
            shard_store::{FlushBatch, FlushEntry, ShardStore, StoreOptions},
        },
    };
    use std::sync::Arc;

    let dir = TempDir::new().unwrap();
    let meta = PodMeta::load_or_init(dir.path(), "test-pod", 1, b"schema").unwrap();
    let k = SimdKernelTable::detect(SimdMode::Scalar);
    let layout = HeaderLayout {
        banks: 1,
        minmax_slots: 2,
    };
    let opts = StoreOptions {
        shard_dir: dir.path().join("shard_0"),
        cache_bytes: 8 << 20,
        hdr_memtable_bytes: 1 << 20,
        tail_memtable_bytes: 1 << 20,
        sync_wal: true,
        layout,
        worker_threads: 2,
        max_write_buffer_bytes: None,
        max_cached_files: None,
    };

    let mut store = ShardStore::open(opts.clone(), &meta).unwrap();
    let epoch_ms = 1_700_000_000_000u64;
    let digest_a = 0x1111u128;
    let digest_b = 0x2222u128;
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

    // 1. Initial flush: 100 counts for Key A and Key B
    let entry_a = FlushEntry {
        key: StorageKey24 {
            epoch_ms,
            digest: digest_a,
        },
        is_new: true,
        resource_scope_id: 0,
        delta: vec![[100, 0, 0, 0]],
        minmax: vec![(5, 10), (0, 0)],
        last_update_s: 1,
        tail: None,
    };
    let entry_b = FlushEntry {
        key: StorageKey24 {
            epoch_ms,
            digest: digest_b,
        },
        is_new: true,
        resource_scope_id: 0,
        delta: vec![[100, 0, 0, 0]],
        minmax: vec![(5, 10), (0, 0)],
        last_update_s: 1,
        tail: None,
    };
    let batch = FlushBatch {
        entries: vec![entry_a, entry_b],
        offsets: Vec::new(),
        new_resources: vec![(0, res)],
        resource_refs: vec![(0, epoch_ms)],
    };
    store.apply_flush(batch, &k).unwrap();

    // 2. Persist in-flight tickets for Key A and Key B with slice_seq 0, plus a stale slice_seq 99 entry
    let ticket = Ticket {
        id: 42,
        window_epoch_ms: epoch_ms,
        reason: EmitReason::Periodic,
        entries: vec![
            TicketEntry {
                digest: digest_a,
                slice_seq: 0,
                delta: vec![[100, 0, 0, 0]],
                minmax: vec![(5, 10), (0, 0)],
            },
            TicketEntry {
                digest: digest_a,
                slice_seq: 99,
                delta: vec![[999, 0, 0, 0]],
                minmax: vec![(5, 10), (0, 0)],
            },
        ],
    };
    let ticket_b = Ticket {
        id: 43,
        window_epoch_ms: epoch_ms,
        reason: EmitReason::Periodic,
        entries: vec![TicketEntry {
            digest: digest_b,
            slice_seq: 0,
            delta: vec![[100, 0, 0, 0]],
            minmax: vec![(5, 10), (0, 0)],
        }],
    };
    store.put_tickets(&[ticket, ticket_b]).unwrap();

    // 3. Second flush of 50 counts for Key B only (Key A has no new events; Key B total becomes 150)
    let entry_b2 = FlushEntry {
        key: StorageKey24 {
            epoch_ms,
            digest: digest_b,
        },
        is_new: false,
        resource_scope_id: 0,
        delta: vec![[50, 0, 0, 0]],
        minmax: vec![(3, 12), (0, 0)],
        last_update_s: 2,
        tail: None,
    };
    let batch2 = FlushBatch {
        entries: vec![entry_b2],
        offsets: Vec::new(),
        new_resources: Vec::new(),
        resource_refs: vec![(0, epoch_ms)],
    };
    store.apply_flush(batch2, &k).unwrap();

    // Close store so drain can open it cleanly
    drop(store);

    // 4. Run `drain`
    let out = bin()
        .args([
            "drain",
            "--data-dir",
            dir.path().to_str().unwrap(),
            "--format",
            "ndjson",
        ])
        .output()
        .unwrap();
    assert!(out.status.success());

    let stdout = String::from_utf8_lossy(&out.stdout);
    let records: Vec<serde_json::Value> = stdout
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();

    assert_eq!(
        records.len(),
        3,
        "expected exactly 3 records (2 from replayed ticket, 1 for remaining delta of Key B):\n{stdout}"
    );

    // Find records by digest and emit_reason
    let key_a_ticket = records
        .iter()
        .find(|r| r["digest"].as_u64() == Some(digest_a as u64) && r["emit_reason"] == "periodic")
        .expect("key A ticket record must exist");
    assert_eq!(key_a_ticket["slice_seq"], 0);
    assert_eq!(
        key_a_ticket["slice_id"],
        slice_id(&meta.emitter_id, epoch_ms, digest_a, 0)
    );
    assert_eq!(key_a_ticket["counters"][0][0], 100);

    // Key A must NOT have any duplicate record under "drain" because total == emitted
    assert!(
        records.iter().all(
            |r| !(r["digest"].as_u64() == Some(digest_a as u64) && r["emit_reason"] == "drain")
        ),
        "key A must not be emitted a second time under drain"
    );

    let key_b_ticket = records
        .iter()
        .find(|r| r["digest"].as_u64() == Some(digest_b as u64) && r["emit_reason"] == "periodic")
        .expect("key B ticket record must exist");
    assert_eq!(key_b_ticket["slice_seq"], 0);
    assert_eq!(
        key_b_ticket["slice_id"],
        slice_id(&meta.emitter_id, epoch_ms, digest_b, 0)
    );
    assert_eq!(key_b_ticket["counters"][0][0], 100);

    let key_b_drain = records
        .iter()
        .find(|r| r["digest"].as_u64() == Some(digest_b as u64) && r["emit_reason"] == "drain")
        .expect("key B drain record must exist");
    assert_eq!(key_b_drain["slice_seq"], 1);
    assert_eq!(
        key_b_drain["slice_id"],
        slice_id(&meta.emitter_id, epoch_ms, digest_b, 1)
    );
    assert_eq!(key_b_drain["counters"][0][0], 50);

    // 5. Verify that in the store, the ticket has been acknowledged and removed from _tickets
    let store_after = ShardStore::open(opts, &meta).unwrap();
    assert!(
        store_after.tickets().unwrap().is_empty(),
        "persisted tickets must be acknowledged and removed from _tickets after drain replay"
    );
}
