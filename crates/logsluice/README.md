# `logsluice`

[![Crates.io](https://img.shields.io/crates/v/logsluice.svg)](https://crates.io/crates/logsluice)
[![Docs.rs](https://docs.rs/logsluice/badge.svg)](https://docs.rs/logsluice)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://github.com/jimmystewpot/logsluice/blob/main/LICENSE)

Standalone async `tokio` log reduction pipeline, OpenTelemetry (`OTLP`) / JSON router, SIMD field normalisers, and slice renderer built on [`logsluice-core`](https://crates.io/crates/logsluice-core).

## The Problem It Solves

Modern microservices and infrastructure emit repetitive structured JSON and OpenTelemetry (`ExportLogsServiceRequest`) log streams at hundreds of thousands of events per second. Ingesting every raw event into downstream analytics stores (ClickHouse, Elasticsearch, Datadog, Loki) drives up network, indexing, and storage costs—yet naive sampling discards rare error states and breaks exact event counts, while variable fields (IP addresses, UUIDs, numbers, timestamps) defeat simple exact-match deduplication.

## How It Solves It

`logsluice` sits inline in a Rust telemetry pipeline and collapses repetitive log streams by **10–40×** into crash-exact tumbling-window summary slices without losing statistical fidelity or sample exemplars:

- **Pre-Hash Field Normalisation (`extract`):** Normalises high-entropy dimensions before key hashing using SIMD-accelerated log template fingerprinting (`log_fingerprint` replacing dynamic IPs, UUIDs, hex strings, and numbers with placeholders), IPv4/IPv6 CIDR prefix masking (`ip_cidr`), and timestamp bucketing (`timestamp`).
- **Symmetric Native JSON & OpenTelemetry (`OTLP`) Routing (`router`, `render`):** Automatically detects flat native JSON log events and nested `resourceLogs[].scopeLogs[].logRecords[]` OTLP batches, interning `(resource, scope)` pairs per shard, isolating malformed payloads to quarantine events, and rendering chunked downstream summary envelopes (`max_records_per_envelope`).
- **Sharded Async Tokio Pipeline (`pipeline`):** `SluicePipeline` partitions workloads across independent [`logsluice-core`](https://crates.io/crates/logsluice-core) shards backed by `fjall` LSM storage threads, supporting `on_close` and `periodic` delta-slice emission modes across windows from seconds to 24 hours.
- **End-to-End Delivery Acknowledgements (`event`):** Self-contained `LogEvent`, `FieldPath` (supporting dot-separated paths, `"..."` quoted segments, and `\.` escaped dots), and `AckNotifier` / `AckFinalizer` ensure upstream sources are only acknowledged once L1 batches are durably flushed to disk, and downstream slices are only marked emitted once the sink confirms delivery.
- **Declarative Configuration & Observability (`config`, `metrics`):** Deserialises `SluiceConfig` from YAML or JSON with human-readable byte sizes (`1.5GiB`, `32MiB`) and durations (`24h`, `30s`), and exports `logsluice_*` counters and gauges via the [`metrics`](https://crates.io/crates/metrics) facade.

## When to Use Which Crate

- **Use `logsluice`** to embed a complete async `tokio` log rollup and OpenTelemetry reduction pipeline in your service or collector.
- **Use [`logsluice-core`](https://crates.io/crates/logsluice-core)** if you need the runtime-independent, Sans-I/O windowed aggregation state machine and LSM store without `tokio`.
- **Use [`logsluice-cli`](https://crates.io/crates/logsluice-cli)** to run deterministic crash-recovery tests (`logsluice replay`) or drain un-emitted state from offline Kubernetes PVCs (`logsluice drain`).

## Quick Start

Run the included OpenTelemetry reduction example:

```bash
cargo run -p logsluice --example otlp_reduce
```

## License

Licensed under the Apache License, Version 2.0 ([`LICENSE`](https://github.com/jimmystewpot/logsluice/blob/main/LICENSE)).
