# `logsluice` Operations & CLI Runbook

This runbook documents the `logsluice` command-line tool (`crates/logsluice-cli`), capacity planning and sizing formulas, schema migration and scale-down procedures, and the `logsluice_*` Prometheus metrics surface.

---

## 1. CLI Commands (`logsluice`)

Install or build the CLI binary:

```bash
cargo install logsluice-cli
# or from a workspace checkout:
cargo build --release -p logsluice-cli
```

### 1.1 Deterministic Replay & Crash-Recovery Verification (`logsluice replay`)

`logsluice replay` generates a deterministic synthetic stream of log events and drives multi-shard `ShardWorker` + `StoreThread` instances while optionally injecting process crashes (`--crash-every`) and downstream negative acknowledgments (`--nack-rate`). It verifies that deduplicated downstream slice sums (`slice_id`) match exact ground-truth totals.

```bash
logsluice replay \
  --events 200000 \
  --keys 5000 \
  --seed 7 \
  --shards 2 \
  --window-duration-ms 10000 \
  --data-dir /tmp/logsluice-replay \
  --crash-every 50000 \
  --nack-rate 0.05
```

| Flag | Default | Description |
| :--- | :--- | :--- |
| `--events <N>` | `200000` | Total number of synthetic events to generate |
| `--keys <K>` | `5000` | Number of distinct grouping keys in `[0..K)` |
| `--seed <S>` | `7` | Deterministic PRNG seed |
| `--shards <S>` | `2` | Number of worker shards |
| `--window-duration-ms <MS>` | `10000` | Tumbling window duration in milliseconds |
| `--data-dir <PATH>` | *(required)* | Storage root directory (`pod_meta.json` + `shard_0..N`) |
| `--crash-every <M>` | `None` | Simulate unclean crash every `M` events without acking in-flight tickets |
| `--nack-rate <RATE>` | `None` | Simulated downstream negative-ack rate in `[0.0, 1.0]` |
| `--stop-before-close` | `false` | Flush L1 and exit while windows remain open (for testing `logsluice drain`) |

### 1.2 Offline PVC State Drain (`logsluice drain`)

When decommissioning a StatefulSet pod or preparing for a breaking configuration change (changing `shards`, `window.duration`, `group_by`, `normalizers`, or `aggregations`), run `logsluice drain` against the unmounted or quiescent `data_dir` PVC.

```bash
logsluice drain \
  --data-dir /var/lib/logsluice/agg_state \
  --format ndjson > drained-slices.ndjson
```

**How `logsluice drain` works:**
1. Reads `pod_meta.json` (`emitter_id`, `shards`, `window_duration_ms`, `window_offset_ms`, and the 8-byte `HeaderLayout` prefix of `codec_schema`).
2. Emits any previously spilled NDJSON records from `shard_{n}/spill/*.ndjson`.
3. Iterates every live window in `shard_{0..shards-1}` in paged batches of 500 keys using sequential merge-join over `w{epoch}_hdr` and `w{epoch}_tail`, emitting all keys with pending $\Delta > 0$ as `SliceRecord` NDJSON lines with deterministic `slice_id`s.

Upon completion, `logsluice drain` prints diagnostic verification telemetry to `stderr` of the form `drain: scan_point_gets=<count>`. Because both in-flight ticket replay and live-window close scans operate strictly via sequential merge-join range iterators (`ShardStore::rows` and `ShardStore::rows_for_sorted`), `scan_point_gets` remains `0` even across large PVC backlogs, guaranteeing zero random I/O on `_tail` during offline drain.

---

## 2. Capacity Planning & Sizing Formulas

### 2.1 Memory Sizing (per pod)

For $S$ shards, $K_{\text{shard}}$ max keys per shard per window, and up to $W_{\text{live}} = 2$ overlapping windows during `allowed_lateness`:

$$\text{Memory}_{\text{total}} \approx C_{\text{fjall}} + \left(S \times W_{\text{live}} \times 2^{\lceil \log_2(K_{\text{shard}} / 0.875) \rceil} \times 17\text{ B}\right) + \left(S \times W_{\text{live}} \times (M_{\text{hdr}} + M_{\text{tail}})\right) + \text{L1}_{\text{double}}$$

With production defaults ($S = 4$, $K_{\text{shard}} = 2{,}000{,}000$, $C_{\text{fjall}} = 1.5\text{ GiB}$, $M_{\text{hdr}} = 32\text{ MiB}$, $M_{\text{tail}} = 16\text{ MiB}$):
- **`fjall` block cache:** $1.5\text{ GiB}$
- **`KnownKeySet`:** $71\text{ MB} \times 4 \times 1 = 285\text{ MB}$ steady ($570\text{ MB}$ peak during window overlap)
- **LSM Memtables:** $48\text{ MiB} \times 4 = 192\text{ MiB}$ steady ($384\text{ MiB}$ peak)
- **L1 Double Buffers:** $\approx 200\text{ MB}$ across 4 shards
- **Total Pod RSS Target:** $\le 3.5\text{ GiB}$ (measured peak RSS at 8M keys: $1.32\text{ GiB}$)

### 2.2 Disk Sizing (per pod)

Each key occupies:
- **Header (`w{epoch}_hdr`):** $24\text{ B key} + 112\text{ B value} + 16\text{ B LSM overhead} \approx 152\text{ B}$.
- **Exemplar Tail (`w{epoch}_tail`):** $250\text{–}500\text{ B}$ uncompressed (written once per key per window, LZ4-compressed on disk).

At $8\text{M}$ keys per window:
- Steady-state footprint is $8\text{–}15\text{ GB}$ across overlapping windows and journals.
- Configure `storage.max_disk_bytes: 50GiB` with `storage.control_reserve_bytes: 5GiB` (10%) reserved exclusively for control writes (`_offsets`, `_tickets`, `emitted_counters`, and WAL sync) so window close and purge (`delete_keyspace`) never deadlock when disk usage reaches the cap.

---

## 3. Standard Operational Procedures

### 3.1 Schema or Shard-Count Migration
`logsluice` persists `shards`, `salt`, `window_duration_ms`, `window_offset_ms`, and `codec_schema` in `pod_meta.json` and each shard's `_meta` keyspace. Starting a pod with modified grouping fields, normalizers, or shard count returns `AggError::MetaMismatch` to prevent silent hash-routing or binary-layout corruption.

**Migration steps:**
1. Stop the collector pod cleanly (`SIGTERM` triggers `emit_partial` drain).
2. Run `logsluice drain --data-dir /var/lib/logsluice/agg_state --format ndjson` and ship any remaining slices to the downstream staging topic/table.
3. Clear `/var/lib/logsluice/agg_state/*`.
4. Deploy the updated configuration.

### 3.2 Handling Downstream Sink Outages
When downstream acknowledgments fail (`AckStatus::Errored` or `AckStatus::Rejected`), `ShardWorker` retries tickets with exponential backoff using identical `slice_id`s. If an outage exceeds `window.max_closing_duration`, pending tickets are written to `shard_{n}/spill/*.ndjson` so the closed window's LSM keyspaces can be purged on schedule. Once downstream acknowledgments recover, spill files are replayed automatically every 60 seconds.

---

## 4. Prometheus Metrics (`logsluice_*`)

| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `logsluice_events_ingested_total` | Counter | `shard` | Total events accepted into shard L1 buffers |
| `logsluice_replayed_events_total` | Counter | `shard` | Duplicate input events dropped by per-partition offset high-water marks |
| `logsluice_events_routed_total` | Counter | `route` | Events routed to `late` or `quarantine` passthrough |
| `logsluice_keys_known` | Gauge | `shard` | Active distinct keys tracked in `KnownKeySet` on the shard |
| `logsluice_keys_overflowed_total` | Counter | `shard` | New keys redirected to per-resource overflow buckets (`shard_cap`, `resource_share`, or `disk_full`) |
| `logsluice_keys_evicted_total` | Counter | `shard` | Cold keys evicted via `evict_cold` after slice acknowledgment |
| `logsluice_tickets_inflight` | Gauge | `shard` | Emission tickets currently awaiting downstream `AckStatus` |
| `logsluice_slices_reemitted_total` | Counter | `shard` | Slices re-emitted after negative acknowledgment or crash recovery |
| `logsluice_forced_closes_total` | Counter | `shard` | Windows forced into `Closing` by wall-clock `max_close_delay` |
| `logsluice_disk_bytes` | Gauge | `shard` | Current on-disk footprint (`fjall` + spill files) for the shard |
| `logsluice_resources_table_entries` | Gauge | `shard` | Interned `(resource, scope)` entries in `_resources` |
| `logsluice_spill_files` | Gauge | `shard` | Active NDJSON spill files awaiting sink recovery |
| `logsluice_dropped_events_total` | Counter | `shard` | New-key events dropped when `on_disk_full: drop_newest` is active |
| `logsluice_field_type_errors_total` | Counter | — | Events where numeric aggregation fields failed type extraction |
| `logsluice_field_coercions_total` | Counter | — | Events where string values were coerced to numeric aggregation inputs |
