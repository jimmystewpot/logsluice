# `logsluice`

[![CI](https://github.com/jimmystewpot/logsluice/actions/workflows/ci.yml/badge.svg)](https://github.com/jimmystewpot/logsluice/actions/workflows/ci.yml)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=jimmystewpot_logsluice&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=jimmystewpot_logsluice)
[![codecov](https://codecov.io/gh/jimmystewpot/logsluice/graph/badge.svg)](https://codecov.io/gh/jimmystewpot/logsluice)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

**`logsluice`** is a high-throughput, crash-exact, LSM-backed tumbling windowed log aggregation engine and OpenTelemetry (`OTLP`) / native log rollup pipeline written in Rust.

It collapses high-volume telemetry streams (300,000+ EPS per cluster across windows up to 24 hours and 8M+ active keys per window) into immutable, deterministically identified summary slices (`window.slice_id`) backed by [`fjall`](https://crates.io/crates/fjall) LSM storage and runtime-dispatched SIMD kernels (`AVX2` on `x86_64`, `NEON` on `aarch64`, with bit-identical scalar fallback).

---

## Crate Architecture

| Crate / Path | `crates.io` | Description |
| :--- | :--- | :--- |
| [`crates/logsluice-core`](crates/logsluice-core) | `logsluice-core` | Runtime-independent, Sans-I/O windowed aggregation engine: SIMD/scalar kernels, XXH3-128 key builder, double-buffered L1 coalescer, Fjall LSM `ShardStore` & `StoreThread`, watermark/offset/cardinality/ticket state machines, and `SpillWriter`. |
| [`crates/logsluice`](crates/logsluice) | `logsluice` | Standalone async `tokio` multi-shard pipeline (`SluicePipeline`), `SluiceConfig`, self-contained `LogEvent` / `FieldPath` / `AckNotifier`, SIMD field normalizers, native & OTLP JSON (`ExportLogsServiceRequest`) router and renderer, and `logsluice_*` metrics. |
| [`crates/logsluice-cli`](crates/logsluice-cli) | `logsluice-cli` (bin: `logsluice`) | Deterministic crash-recovery verification harness (`logsluice replay`) and offline PVC state drain tool (`logsluice drain`). |
| [`bridges/logsluice-vector`](bridges/logsluice-vector) | *(workspace-excluded)* | Optional thin adapter bridging `vector_lib::event::LogEvent` / `EventFinalizers` and `vector_core::transform::TaskTransform` to `logsluice::SluicePipeline`. |

---

## Key Capabilities

- **On-Close Default with Delta Slices (`delta_slice`):** Emits one immutable summary slice per key when a tumbling window closes (or opt-in `periodic` interim deltas), reducing downstream row volume by 10–40× while remaining safe across multi-pod consumer rebalances.
- **End-to-End Crash Exactness:**
  - **Input Idempotency:** Per-shard, per-partition Kafka offset high-water marks (`_offsets`) are committed in the same atomic `fjall::WriteBatch` as counter updates, dropping replayed events after a crash.
  - **Output Idempotency:** Emission intent is persisted to `_tickets` before any slice leaves the pipeline. Slices carry a deterministic `window.slice_id = "{emitter_id}:{window_epoch}:{digest_hex}:{slice_seq}"` so downstream primary-key tables deduplicate replayed slices automatically.
- **Header / Exemplar Tail Separation & `KnownKeySet`:** Fixed-width numeric headers (`w{epoch}_hdr`, ~112 B) are separated from write-once exemplar payloads (`w{epoch}_tail`, LZ4-compressed). An in-memory `KnownKeySet` (`HashSet<u128>`) avoids string allocations for known keys and skips LSM point reads for new keys.
- **Per-Resource Fair-Share Cardinality Protection:** Prevents noisy tenants or port scans from exhausting window key budgets (`max_resource_share: 0.25`), routing excess keys to per-resource overflow buckets (`on_cap: overflow`) or losslessly evicting cold keys (`on_cap: evict_cold`).
- **Native & OpenTelemetry (`OTLP`) Envelope Support:** Automatically detects native flat JSON events and nested `resourceLogs[].scopeLogs[].logRecords[]` OTLP batches, interning `(resource, scope)` pairs per shard and chunking output envelopes (`max_records_per_envelope`).

---

## Quickstart

### Run the Standalone OTLP / Native Rollup Example

```bash
cargo run -p logsluice --example otlp_reduce
```

### Run Deterministic Crash-Recovery Verification (`logsluice replay`)

```bash
cargo run -p logsluice-cli -- replay \
  --events 200000 \
  --keys 5000 \
  --shards 2 \
  --window-duration-ms 10000 \
  --data-dir /tmp/logsluice-replay \
  --crash-every 50000 \
  --nack-rate 0.05
```

### Drain Persisted State Offline from a PVC (`logsluice drain`)

```bash
cargo run -p logsluice-cli -- drain \
  --data-dir /var/lib/logsluice/agg_state \
  --format ndjson > drained-slices.ndjson
```

### Build, Test & Benchmark

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo bench -p logsluice-core --bench hot_path -- --quick
```

---

## Documentation

- [Architecture & Engine Design Specification](docs/architecture/design-specification.md)
- [Design Trade-offs & Measured Benchmark Gates](docs/architecture/tradeoffs-and-benchmarks.md)
- [Operations & CLI Runbook (`logsluice replay` / `logsluice drain` / Sizing / Metrics)](docs/operations/cli-and-runbook.md)
- [Vector & Custom Collector Integration Guide](docs/integration/vector.md)

---

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
