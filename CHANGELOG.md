# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Replaced point `get()` lookups on `_tail` during offline drain and keyed ticket replay with sequential merge-join scans (`ShardStore::rows_for_sorted`), eliminating random I/O bottlenecks when draining large PVC backlogs. Keyed scan reads now route through `ShardStore::counted_get` to enforce zero point lookups via the `scan_point_gets` metric and `drain: scan_point_gets=` CLI diagnostic output.

## [0.1.0] - 2026-10-20

### Added

- **`logsluice-core` (`0.1.0`):**
  - Runtime-independent, Sans-I/O tumbling windowed aggregation engine (`ShardWorker`, `StoreThread`, `ShardStore`).
  - Single-database-per-shard `fjall` LSM storage with fixed-width `NumericHeader` (`w{epoch}_hdr`) and LZ4-compressed write-once `ExemplarTail` (`w{epoch}_tail`).
  - Runtime-dispatched SIMD kernels (`AVX2` on `x86_64`, `NEON` on `aarch64`, and bit-identical scalar fallback) for counter accumulation, delta computation, min/max tracking, CIDR IP masking, and log template fingerprinting.
  - Lock-free double-buffered L1 coalescer (`L1Table` / `DoubleBuffer`) and pre-reserved `KnownKeySet`.
  - Per-partition watermark tracker (`WatermarkTracker`) with startup seed grace, caught-up idle detection, and wall-clock forced close.
  - Crash-exact input offset high-water marks (`PositionTracker`) and output emission ticket ledger (`TicketLedger` with deterministic `slice_id`s and NDJSON `SpillWriter`).
  - Per-resource fair-share cardinality protection (`CardinalityGuard`) supporting `overflow` and lossless `evict_cold` modes.
- **`logsluice` (`0.1.0`):**
  - Standalone async `tokio` multi-shard pipeline (`SluicePipeline`) with self-contained `LogEvent`, `FieldPath`, and `AckNotifier` end-to-end delivery acknowledgments.
  - Symmetric auto-detecting native JSON and OpenTelemetry (`OTLP` `ExportLogsServiceRequest`) router and chunked envelope renderer.
  - Prometheus-compatible `logsluice_*` metrics exporter.
- **`logsluice-cli` (`0.1.0`):**
  - `logsluice replay` deterministic crash-recovery and negative-acknowledgment verification harness.
  - `logsluice drain` offline PVC state drain tool.
- **`bridges/logsluice-vector`:**
  - Optional Vector `TaskTransform` adapter bridging `vector_lib::event::LogEvent` to `logsluice::SluicePipeline`.
