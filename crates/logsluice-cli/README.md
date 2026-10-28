# `logsluice-cli`

[![Crates.io](https://img.shields.io/crates/v/logsluice-cli.svg)](https://crates.io/crates/logsluice-cli)
[![Docs.rs](https://docs.rs/logsluice-cli/badge.svg)](https://docs.rs/logsluice-cli)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://github.com/jimmystewpot/logsluice/blob/main/LICENSE)

Command-line operational and verification utility (`logsluice`) for the [`logsluice`](https://crates.io/crates/logsluice) log aggregation engine.

## The Problem It Solves

Operating stateful, disk-backed log aggregation pipelines in production introduces two operational challenges:

1. **Proving Crash & NACK Exactness Before Deployment:** It is difficult to verify that a windowed aggregation engine preserves exact counter conservation (`emitted == ingested`) when worker threads crash mid-flush or downstream sinks return negative acknowledgements (NACKs).
2. **Draining Stranded State from Offline Kubernetes PVCs:** When a StatefulSet pod is scaled down, migrated, or decommissioned before its tumbling windows finish closing, un-emitted counter deltas remain persisted in the pod's unmounted persistent volume (`data_dir`).

## How It Solves It

`logsluice-cli` provides two focused subcommands built on [`logsluice-core`](https://crates.io/crates/logsluice-core):

- **`logsluice replay` (Deterministic Fault-Injection Verification):** Generates seeded synthetic event streams across multiple shards, injects periodic worker/store crashes (`--crash-every`) and downstream sink NACKs (`--nack-rate`), deduplicates emitted `window.slice_id` records, and asserts exact end-to-end counter conservation (`totals_match=true`).
- **`logsluice drain` (Zero-Point-Lookup Offline PVC Drain):** Opens an offline `data_dir` on an unmounted volume, drains pending `spill/*.ndjson` files, streams all live Fjall LSM window partitions via sequential forward merge-join scans (`scan_point_gets=0`), and emits every remaining `total - emitted` counter delta as newline-delimited JSON (`ndjson`) on `stdout` with `emit_reason = "Drain"`.

## Installation

```bash
cargo install logsluice-cli
```

This installs the `logsluice` binary.

## Subcommands

### `logsluice replay`

```bash
logsluice replay \
  --events 200000 \
  --keys 5000 \
  --seed 7 \
  --shards 2 \
  --window-duration-ms 10000 \
  --crash-every 25000 \
  --nack-rate 0.1 \
  --data-dir /tmp/logsluice-replay
```

Output:
```text
totals_match=true events=200000 slices=... duplicates_removed=...
```

Flags:
- `--events <N>`: Total number of synthetic events to generate (default: `200000`).
- `--keys <K>`: Number of distinct keys in range `[0..K)` (default: `5000`).
- `--seed <S>`: Deterministic PRNG seed (default: `7`).
- `--shards <S>`: Number of worker shards (default: `2`).
- `--window-duration-ms <MS>`: Tumbling window duration in milliseconds (default: `10000`).
- `--data-dir <PATH>`: Storage root directory.
- `--crash-every <M>`: Simulate worker/store crash and recovery every `M` events.
- `--nack-rate <RATE>`: Simulated downstream negative acknowledgement rate in `[0.0, 1.0]`.
- `--stop-before-close`: Flush L1 to Fjall L2 and exit before advancing watermarks past window close (useful for testing `logsluice drain`).

### `logsluice drain`

```bash
logsluice drain --data-dir /var/lib/logsluice --format ndjson > drained-slices.ndjson
```

## When to Use Which Crate

- **Use `logsluice-cli`** to install the `logsluice` binary for deterministic crash-recovery testing (`logsluice replay`) and offline Kubernetes PVC state draining (`logsluice drain`).
- **Use [`logsluice`](https://crates.io/crates/logsluice)** to embed a standalone async `tokio` pipeline, OTLP/JSON router, field normalisers, and slice renderer in your Rust service or collector.
- **Use [`logsluice-core`](https://crates.io/crates/logsluice-core)** if you need the runtime-independent, Sans-I/O tumbling-window aggregation engine and Fjall LSM store without `tokio`.

## Licence

Licensed under the Apache License, Version 2.0 ([`LICENSE`](https://github.com/jimmystewpot/logsluice/blob/main/LICENSE)).
