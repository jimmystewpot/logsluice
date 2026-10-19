# `logsluice`

Standalone async Tokio pipeline, OTLP/JSON log router, field normalizers, and slice renderer for [`logsluice`](https://github.com/jimmystewpot/logsluice).

## Overview

`logsluice` wraps [`logsluice-core`](../logsluice-core) with a self-contained event, field-path, and acknowledgment model:

- **`event`**: [`LogEvent`], [`FieldPath`] (supporting dot-separated paths, `"..."` quoted segments, and `\.` escaped dots), and [`AckNotifier`] / [`AckFinalizer`] / [`AckFinalizers`] for end-to-end delivery acknowledgments.
- **`config`**: [`SluiceConfig`] supporting YAML/JSON deserialization, human-readable byte sizes (`1.5GiB`, `32MiB`), duration parsing (`24h`, `30s`), normalizers (`ip_cidr`, `log_fingerprint`), and deterministic `codec_schema()` generation.
- **`router`**: [`Router`] for sharding native JSON and OpenTelemetry (`resourceLogs`) log envelopes with watermark tracking, per-shard purge horizons, late routing, and quarantine isolation.
- **`render`**: [`render_native`] and [`render_otlp`] for formatting window slice records into downstream summary events.
- **`pipeline`**: [`SluicePipeline`] providing an async `Stream<Item = LogEvent>` transformation pipeline backed by per-shard Fjall LSM storage threads.

## Quick Start

Run the included example:

```bash
cargo run -p logsluice --example otlp_reduce
```
