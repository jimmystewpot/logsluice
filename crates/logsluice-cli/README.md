# `logsluice-cli`

Deterministic crash-recovery replay harness (`logsluice replay`) and offline PVC state drain utility (`logsluice drain`) for [`logsluice`](https://github.com/jimmystewpot/logsluice).

## Installation

```bash
cargo install logsluice-cli
```

This installs the `logsluice` binary.

## Subcommands

### `logsluice replay`

Deterministically generates synthetic events and drives them through `logsluice-core` `ShardWorker` and `StoreThread` instances, optionally injecting periodic crash-recovery cycles (`--crash-every`) and negative acknowledgments (`--nack-rate`), then verifies exact end-to-end counter conservation after deduplicating emitted `slice_id` records.

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
- `--nack-rate <RATE>`: Simulated downstream negative acknowledgment rate in `[0.0, 1.0]`.
- `--stop-before-close`: Flush L1 to Fjall L2 and exit before advancing watermarks past window close (useful for testing `logsluice drain`).

### `logsluice drain`

Scans an offline `data_dir` (e.g., an unmounted Kubernetes PVC during pod decommission or incident recovery), drains any pending `spill/*.ndjson` files, computes `total - emitted` counter deltas across all live Fjall window partitions, and emits each non-zero delta as a newline-delimited JSON (`ndjson`) `SliceRecord` on `stdout` with `emit_reason = "Drain"`.

```bash
logsluice drain --data-dir /var/lib/logsluice --format ndjson > drained-slices.ndjson
```

## License

Licensed under the Apache License, Version 2.0 ([`LICENSE`](../../LICENSE)).
