# `logsluice-core`

[![Crates.io](https://img.shields.io/crates/v/logsluice-core.svg)](https://crates.io/crates/logsluice-core)
[![Docs.rs](https://docs.rs/logsluice-core/badge.svg)](https://docs.rs/logsluice-core)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://github.com/jimmystewpot/logsluice/blob/main/LICENSE)

Runtime-independent, Sans-I/O tumbling-window log aggregation and deduplication engine for [`logsluice`](https://github.com/jimmystewpot/logsluice).

## The Problem It Solves

Stateful log aggregation and rollup engines in high-volume observability pipelines face three failure modes:

1. **Unbounded Memory & Cardinality Explosions:** Purely in-memory aggregators suffer out-of-memory (OOM) crashes during high-cardinality bursts (e.g. port scans, ephemeral request IDs, or noisy multi-tenant workloads) or when windows span hours.
2. **Data Loss or Double-Counting on Restart:** Naive periodic flushers either drop in-flight counters when a collector pod restarts or re-emit duplicate totals when upstream message queues (such as Kafka) replay unacknowledged offsets.
3. **Runtime Coupling:** Tying the core windowing state machine directly to an async runtime (`tokio`) or a specific collector framework prevents reuse across custom proxies, synchronous workers, and deterministic fault-injection harnesses.

## How It Solves It

- **Sans-I/O State Machine with Dedicated Storage Threads (`worker`, `store`):** `ShardWorker` is a purely synchronous, event-driven state machine with zero async runtime dependencies. All disk I/O runs on a dedicated per-shard OS thread (`StoreThread`) communicating via bounded `StoreCommand` / `StoreEvent` channels.
- **Lock-Free L1 Coalescing & `KnownKeySet` (`l1`, `key`):** Incoming batches are fingerprinted with 128-bit `XXH3-128` digests (`KeyDigest`) and coalesced in a double-buffered in-memory `L1Table`. A pre-reserved `KnownKeySet` (`HashSet<u128>`) avoids string allocations for known keys and skips LSM point reads when flushing brand-new keys.
- **Header / Exemplar Tail Separation in Fjall LSM (`store`, `codec`):** Each L1 flush atomically writes fixed-width numeric headers (`w{epoch}_hdr`, ~112 B) separately from write-once LZ4-compressed exemplar payloads (`w{epoch}_tail`) in a single `fjall::WriteBatch`. Window close and offline drain scans stream `_hdr` and `_tail` via sequential forward merge-join iterators (`ShardStore::rows`) with zero random point lookups.
- **End-to-End Crash Exactness (`position`, `emission`):**
  - **Input Idempotency:** Per-partition input offset high-water marks (`_offsets`) are committed in the same atomic write batch as counter updates, dropping replayed events after a crash.
  - **Output Idempotency:** Before any summary slice leaves the engine, an emission ticket is persisted in `_tickets`. Every emitted slice carries a deterministic `window.slice_id = "{emitter_id}:{window_epoch}:{digest_hex}:{slice_seq}"`, advancing `emitted_counters` only after downstream delivery acknowledgement (`AckStatus::Delivered`).
- **Per-Resource Fair-Share Cardinality Protection (`cardinality`):** `CardinalityGuard` enforces both global window key caps and per-tenant fair-share budgets (`max_resource_share`), routing excess keys to deterministic `_overflow` buckets or losslessly evicting cold keys (`evict_cold`).
- **Runtime-Dispatched SIMD Kernels (`simd`):** Hardware-accelerated `AVX2` (`x86_64`) and `NEON` (`aarch64`) kernels with bit-identical scalar fallbacks for counter accumulation, delta computation, min/max tracking, IPv4/IPv6 CIDR masking, and log template fingerprinting.

## When to Use Which Crate

- **Use [`logsluice`](https://crates.io/crates/logsluice)** if you want a ready-to-run async `tokio` pipeline (`SluicePipeline`) with built-in OpenTelemetry (`OTLP`) and native JSON routing, field normalisers, delivery acknowledgements, and Prometheus metrics.
- **Use `logsluice-core`** directly if you are embedding the Sans-I/O aggregation engine into your own collector, custom thread-per-core runtime, or bespoke wire protocol.
- **Use [`logsluice-cli`](https://crates.io/crates/logsluice-cli)** for the `logsluice replay` verification harness and `logsluice drain` offline Kubernetes PVC recovery tool.

## License

Licensed under the Apache License, Version 2.0 ([`LICENSE`](https://github.com/jimmystewpot/logsluice/blob/main/LICENSE)).
