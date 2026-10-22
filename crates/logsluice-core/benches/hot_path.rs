//! Criterion performance benchmarks for hot aggregation paths.
#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use logsluice_core::{
    codec::{
        plan::AggregationPlan,
        tail::ExemplarTail,
        ticket::{EmitReason, Ticket, TicketEntry},
    },
    config::{AggregationKind, AggregationSpec, EngineConfig},
    key::{KeyBuilder, StorageKey24},
    l1::L1Buffer,
    resource::ResourceScopePayload,
    simd::{SimdKernelTable, SimdMode},
    store::{
        meta::PodMeta,
        shard_store::{FlushBatch, FlushEntry, FlushStats, HeaderLayout, ShardStore, StoreOptions},
        thread::StoreEvent,
    },
    window::WindowClock,
    worker::{
        ExemplarSource, IngestItem, PurgeHorizon, RecoveredState, ShardWorker, WorkerContext,
    },
};

fn bench_kernels(c: &mut Criterion) {
    let mut group = c.benchmark_group("kernels");

    let auto_k = SimdKernelTable::detect(SimdMode::Auto);
    let scalar_k = SimdKernelTable::detect(SimdMode::Scalar);

    let mut total_auto = vec![[100u64, 200, 300, 400]; 4];
    let mut total_scalar = vec![[100u64, 200, 300, 400]; 4];
    let delta = vec![[1u64, 2, 3, 4]; 4];

    group.bench_function("auto_accumulate", |b| {
        b.iter(|| {
            (auto_k.accumulate_counter_banks)(&mut total_auto, &delta);
        });
    });

    group.bench_function("scalar_accumulate", |b| {
        b.iter(|| {
            (scalar_k.accumulate_counter_banks)(&mut total_scalar, &delta);
        });
    });

    let total = vec![[100u64, 200, 300, 400]; 4];
    let emitted = vec![[50u64, 100, 150, 200]; 4];
    let mut diff_auto = vec![[0u64; 4]; 4];
    let mut diff_scalar = vec![[0u64; 4]; 4];

    group.bench_function("auto_diff", |b| {
        b.iter(|| {
            (auto_k.diff_counter_banks)(&total, &emitted, &mut diff_auto);
        });
    });

    group.bench_function("scalar_diff", |b| {
        b.iter(|| {
            (scalar_k.diff_counter_banks)(&total, &emitted, &mut diff_scalar);
        });
    });

    group.finish();
}

fn bench_key_digest(c: &mut Criterion) {
    let mut group = c.benchmark_group("key_digest");

    let kb = KeyBuilder::new(0x1234_5678_9abc_def0);
    let resource_scope_hash = 0xdead_beef_feed_cafe;
    let severity = 9u8;
    let body = Some(
        b"example log message body for benchmarking key builder digest performance".as_slice(),
    );
    let dims = [
        Some(b"host-01.us-east.corp".as_slice()),
        Some(b"production".as_slice()),
        Some(b"frontend-gateway".as_slice()),
        Some(b"GET /api/v1/metrics HTTP/1.1".as_slice()),
    ];

    group.bench_function("multi_dimension", |b| {
        b.iter(|| {
            std::hint::black_box(kb.digest(resource_scope_hash, severity, body, &dims));
        });
    });

    group.finish();
}

fn bench_l1_accumulate(c: &mut Criterion) {
    let mut group = c.benchmark_group("l1_accumulate");

    let k = SimdKernelTable::detect(SimdMode::Auto);
    let counters = [[1u64, 0, 0, 0]];
    let minmax = [1000i64, 1000i64];
    let key = StorageKey24 {
        epoch_ms: 0,
        digest: 42,
    };

    let mut l1_existing: L1Buffer<u32> = L1Buffer::new(1024);
    l1_existing.accumulate(key, true, 1, &counters, &minmax, 1, None, &k);

    group.bench_function("existing_keys", |b| {
        b.iter(|| {
            l1_existing.accumulate(key, false, 1, &counters, &minmax, 1, None, &k);
        });
    });

    group.bench_function("new_keys", |b| {
        let mut seed_key = 0u128;
        b.iter_batched(
            || {
                let l1: L1Buffer<u32> = L1Buffer::new(128);
                seed_key += 1;
                (l1, seed_key)
            },
            |(mut l1, seed)| {
                for i in 0..100 {
                    let k_item = StorageKey24 {
                        epoch_ms: 0,
                        digest: seed.wrapping_add(i),
                    };
                    l1.accumulate(k_item, true, 1, &counters, &minmax, 1, None, &k);
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_ingest_batch_256(c: &mut Criterion) {
    let mut group = c.benchmark_group("ingest_batch_256");

    let d = tempfile::tempdir().expect("tempdir");
    let mut cfg = EngineConfig::default();
    cfg.output.emitter_id = "pod-0".to_string();
    cfg.storage.data_dir = d.path().to_path_buf();
    cfg.aggregations = vec![AggregationSpec {
        name: "event_count".into(),
        kind: AggregationKind::Count,
        field: None,
    }];
    let cfg = Arc::new(cfg);
    let plan = Arc::new(AggregationPlan::new(&cfg.aggregations).expect("plan"));
    let _meta = PodMeta::load_or_init(d.path(), "pod-0", 1, &plan.schema_bytes()).expect("meta");
    let kernels = SimdKernelTable::detect(SimdMode::Auto);
    let clock = WindowClock::new(cfg.window.duration, cfg.window.offset).expect("clock");
    let ctx = WorkerContext {
        cfg: Arc::clone(&cfg),
        shard: 0,
        emitter_id: Arc::from("pod-0"),
        kernels,
        plan,
        clock,
    };
    let horizon = PurgeHorizon::new(0);
    let mut worker = ShardWorker::new(ctx, RecoveredState::empty(), horizon);

    let default_resource = Arc::new(ResourceScopePayload {
        resource: [("service.name".to_string(), serde_json::json!("bench"))].into(),
        scope_name: "bench".to_string(),
        scope_version: "1.0.0".to_string(),
        scope_attrs: Default::default(),
    });
    let default_resource_hash = default_resource.canonical_hash();

    let batch: Vec<IngestItem> = (0..256)
        .map(|i| IngestItem {
            key: StorageKey24 {
                epoch_ms: 0,
                digest: (i % 64) as u128,
            },
            resource: Arc::clone(&default_resource),
            resource_hash: default_resource_hash,
            position: None,
            event_ms: 1000 + i as i64,
            counters: vec![[1, 0, 0, 0]],
            minmax_values: vec![1000 + i as i64, 1000 + i as i64],
            exemplar: ExemplarSource::default(),
        })
        .collect();

    group.bench_function("batch_256", |b| {
        b.iter_batched(
            || batch.clone(),
            |items| {
                let ingested = std::hint::black_box(worker.try_ingest(items, 0, 1000));
                std::hint::black_box(&ingested);
                let _ = worker.flush_l1(1000);
                let _ = worker.on_event(
                    StoreEvent::FlushDone {
                        finalizers: vec![0],
                        stats: FlushStats::default(),
                        disk_bytes: 0,
                    },
                    1000,
                );
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_apply_flush_known_keys_100k(c: &mut Criterion) {
    let d = tempfile::tempdir().expect("tempdir");
    let meta = PodMeta::load_or_init(d.path(), "pod-0", 1, b"schema").expect("meta");
    let k = SimdKernelTable::detect(SimdMode::Auto);
    let opts = StoreOptions {
        shard_dir: d.path().join("shard_0"),
        cache_bytes: 64 << 20,
        hdr_memtable_bytes: 32 << 20,
        tail_memtable_bytes: 32 << 20,
        sync_wal: true,
        layout: HeaderLayout {
            banks: 1,
            minmax_slots: 2,
        },
        worker_threads: 2,
        max_write_buffer_bytes: None,
        max_cached_files: None,
    };
    let mut store = ShardStore::open(opts, &meta).expect("store");

    const NUM_KEYS: usize = 100_000;
    let initial_entries: Vec<FlushEntry> = (0..NUM_KEYS)
        .map(|i| FlushEntry {
            key: StorageKey24 {
                epoch_ms: 0,
                digest: i as u128,
            },
            is_new: true,
            resource_scope_id: 1,
            delta: vec![[1, 0, 0, 0]],
            minmax: vec![(1000, 1000), (1000, 1000)],
            last_update_s: 1,
            tail: Some(ExemplarTail {
                severity: 9,
                body: None,
                dims: vec![],
                attrs: vec![],
            }),
        })
        .collect();

    let initial_batch = FlushBatch {
        entries: initial_entries,
        offsets: vec![],
        new_resources: vec![],
        resource_refs: vec![],
    };
    store.apply_flush(initial_batch, &k).expect("initial flush");

    let known_entries: Vec<FlushEntry> = (0..NUM_KEYS)
        .map(|i| FlushEntry {
            key: StorageKey24 {
                epoch_ms: 0,
                digest: i as u128,
            },
            is_new: false,
            resource_scope_id: 1,
            delta: vec![[1, 0, 0, 0]],
            minmax: vec![(1000, 1000), (1000, 1000)],
            last_update_s: 2,
            tail: None,
        })
        .collect();

    let known_batch = FlushBatch {
        entries: known_entries,
        offsets: vec![],
        new_resources: vec![],
        resource_refs: vec![],
    };

    c.bench_function("apply_flush_known_keys_100k", |b| {
        b.iter(|| {
            store.apply_flush_ref(&known_batch, &k).expect("flush");
        });
    });
}

fn bench_put_tickets_many_commands(c: &mut Criterion) {
    let d = tempfile::tempdir().expect("tempdir");
    let meta = PodMeta::load_or_init(d.path(), "pod-0", 1, b"schema").expect("meta");
    let opts = StoreOptions {
        shard_dir: d.path().join("shard_0"),
        cache_bytes: 8 << 20,
        hdr_memtable_bytes: 1 << 20,
        tail_memtable_bytes: 1 << 20,
        sync_wal: true,
        layout: HeaderLayout {
            banks: 1,
            minmax_slots: 2,
        },
        worker_threads: 2,
        max_write_buffer_bytes: None,
        max_cached_files: None,
    };
    let mut store = ShardStore::open(opts, &meta).expect("store");

    const NUM_COMMANDS: usize = 50;
    let commands: Vec<Vec<Ticket>> = (0..NUM_COMMANDS)
        .map(|i| {
            vec![Ticket {
                id: (i + 1) as u64,
                window_epoch_ms: 0,
                reason: EmitReason::Close,
                entries: vec![TicketEntry {
                    digest: i as u128,
                    slice_seq: 1,
                    delta: vec![[1, 0, 0, 0]],
                    minmax: vec![(1000, 1000), (1000, 1000)],
                }],
            }]
        })
        .collect();

    c.bench_function("put_tickets_many_commands", |b| {
        b.iter(|| {
            for cmd in &commands {
                store.put_tickets(cmd).expect("put_tickets");
            }
        });
    });
}

criterion_group!(
    benches,
    bench_kernels,
    bench_key_digest,
    bench_l1_accumulate,
    bench_ingest_batch_256,
    bench_apply_flush_known_keys_100k,
    bench_put_tickets_many_commands
);
criterion_main!(benches);
