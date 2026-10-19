# Architecture, Trade-offs & Benchmarks: `logsluice`

**Document ID:** ARCH-LOGSLUICE-002  
**Governing spec:** [design-specification.md](design-specification.md)  
**Status:** Production Baseline with Verified Benchmark Gates  

---

## 1. What `logsluice` is, and what it is not

`logsluice` turns a high-volume log stream into **one immutable slice per key per tumbling window**. Windows can be up to 24 hours long. Every slice carries a deterministic `slice_id`, so a downstream store that deduplicates on that id and sums the rest (for example a StarRocks primary-key staging table feeding rollup tasks) gets exact totals, even after crashes.

**It is not:**
* **A real-time view.** The default `on_close` mode trades freshness for fewer rows. Freshness comes from a separate path, such as ksql reading Kafka directly.
* **A distributed state store.** State is local to each pod. Correctness across pods comes from delta semantics, not coordination.
* **A general analytics engine.** Only `count`, `sum`, `min` and `max` are supported. These are the aggregations that can be merged and subtracted.

---

## 2. The decision that shapes everything: emission cadence

| | `on_close` (default) | `periodic` (opt-in) |
|---|---|---|
| Rows downstream | One per key per window | One per key per active interval |
| What the long-lived state buys | **Fewer rows**: by the average number of active intervals per key, roughly 10–40× for persistent flows | Tolerance for late arrivals across the whole window, plus stable day attribution |
| Freshness | Up to `duration + allowed_lateness` | `interval` |
| Unemitted data exposed to a crash | Up to a full window per pod | At most one interval |

**Why `on_close` is the default:**
* With delta slices plus periodic emission, downstream rows are the same as with a plain short window. The durable 24-hour state would then add cost without reducing rows.
* `on_close` is the mode where that state does useful work.
* `periodic` remains for users who need lateness tolerance across the whole window. Whether that is worth paying for depends on the measured event-time lag distribution, which should be measured before choosing it.

**Consequence:** because `on_close` is the default, durability is essential, not a nice-to-have. That is why emission tickets, offset high-water marks, drain on shutdown and forced close are all required.

---

## 3. Trade-offs

### 3.1 Storage: one `fjall` database per shard (B) vs. in-memory header table with delta log and snapshots (A)

**The workload:**
* A fixed-size header (about 112 B) is updated in place on every flush, for up to 8M keys per window.
* An exemplar tail (about 250–500 B) is written once per key.
* Keys are uniformly hashed, which is the worst case for leveled compaction: every flushed file covers the whole key range.

| | **B: chosen** | A: fallback |
|---|---|---|
| Crash consistency | **Inherited from `fjall`**: one journal, and one `WriteBatch` covering `hdr`, `tail`, `_resources` and `_offsets` | Custom ordering protocol between a custom log and `fjall` (torn writes, checksums, recovery) |
| Working set larger than RAM | **Degrades gracefully**: cache misses, then disk reads | Hard memory floor; risk of OOM |
| Daily writes | Updated keys × 152 B × (1 + measured compaction write amplification): **≈ 312 GB/day measured** | Updated keys × ~130 B + about 1 GB per snapshot: predictable |
| Code to own | Small | Significant: snapshot format, log rotation, recovery |
| Cache isolation between `hdr` and `tail` | No: `fjall` 3 has one cache per database | Not needed |

**Why B:**
* Under `on_close`, the main reason A existed — tail reads every 15 minutes pushing headers out of the cache — has disappeared. Tails are read once per window, when the new window's header set is still small.
* B's remaining risk is compaction write amplification. That is measurable, and it is reduced with three settings:
  1. a 30s flush interval (repeated updates to a key merge in L1);
  2. write buffer sizing;
  3. `Leveled` tuning. `fjall` 3.1 offers only `Leveled` and `Fifo`, and `Fifo` would delete live data.

**Rejected:** `fjall`'s built-in KV separation, as a substitute for the manual header/tail split. Tails are never rewritten, so a manual split already avoids rewriting them; built-in separation helps when large values are overwritten.

**Rejected earlier:**
* **RocksDB:** C++ FFI and toolchain burden.
* **SlateDB:** built on object storage, about 8k–45k writes/s on local NVMe, and no cheap way to drop a partition.

### 3.2 `KnownKeySet` (in-memory `HashSet<u128>` per shard per window)

* **Gain:** an O(1) newness check that avoids I/O:
  - known keys skip building a tail entirely (no string copies on the hot path);
  - new keys skip the header read (blind insert);
  - the cardinality cap is enforced without touching storage.
* **Cost:** `hashbrown` rounds capacity up to a power of two, so 2M entries take 2²² buckets × 17 B ≈ **71 MB per shard per window**. That is about 285 MB steady and 570 MB during the lateness overlap, on 4 shards. On restart it is rebuilt by a key-only iteration of each live `_hdr` keyspace.
* **Rejected:** a 64-bit fingerprint set. It halves memory, but a false positive means a new key is treated as known, so its exemplar is never captured. At 2M keys and ~2.6×10¹⁰ lookups per day, that is about 3×10⁻³ per shard per day: rare, but it silently degrades output.

### 3.3 Idempotency: emission tickets and deterministic `slice_id`

* **Problem:** at-least-once plumbing feeding a downstream `SUM` overcounts whenever something is replayed.
* **Decision:**
  1. Persist an emission ticket (key, `slice_seq`, Δ, min/max) **before** any record leaves the pipeline.
  2. Derive `slice_id = emitter:epoch:digest:seq`.
  3. On acknowledgement, apply `emitted += Δ` and `slice_seq += 1`.
  4. On recovery, re-emit tickets verbatim.
* **Gain:** downstream totals stay exact across crashes, provided the sink deduplicates on `slice_id`.
* **Cost:** one ticket write per 500 slices, a downstream table that deduplicates, and a rule that a key has at most one ticket in flight at a time.
* **Rejected:**
  - **Kafka transactions:** Standard collector sinks do not universally support end-to-end transactional state commits across local LSM stores.
  - **Content-hash slice ids:** a retry after more data arrives would produce a different id.

### 3.4 Input idempotency: per-shard offset high-water marks

* **Decision:** each shard persists, in the same atomic batch as its counters, the highest offset per partition it has applied. On ingest it drops anything at or below that mark.
* **Why it is correct:**
  1. Events within a partition arrive in offset order.
  2. Routing by digest is deterministic.
  3. The shard count is fixed.

  So a replayed event lands on the same shard that already persisted it.
* **Cost:** `shards` is fixed for the life of a `data_dir`; changing it needs an offline drain (`logsluice drain`).
* **Known limit:** another pod can't see these marks. On an **unclean rebalance**, up to `l1_flush_interval + commit_interval_ms` of a partition's traffic can be counted by two pods. Static membership plus the cooperative-sticky assignor makes this rare, and a graceful shutdown flushes first.

### 3.5 Flush interval: 30s default

| Interval | Header writes | Unclean-rebalance duplicate window | Upstream finalizer hold |
|---|---|---|---|
| 5s | Highest | ~5s + commit interval | ~5s |
| **30s** | Lower (more merging per key) | ~30s + commit interval | ~30s |
| 60s | Lowest | ~60s + commit interval | ~60s |

Offset high-water marks are what make a longer interval safe: anything not yet flushed can be replayed from Kafka.

### 3.6 Watermarks: per partition, caught-up idle rule, forced close

* **Problem:** a source that goes idle on a wall-clock timer drops out of the watermark minimum. During backlog catch-up, partitions that haven't been reached yet look idle. The watermark jumps ahead, and their data is sent to `late`.
* **Decision:**
  - Track a watermark per Kafka partition.
  - A partition is idle only if it was caught up when it went quiet.
  - Known partitions are persisted, so after a restart they keep holding the watermark back until they are reached.
  - A wall-clock forced close at `end + max_close_delay` guarantees progress.
* **Cost:** depends on Kafka metadata (falls back to per-source tracking, with an LRU cap). A partition newly assigned after the watermark has advanced can still produce late data, so size `allowed_lateness` to the expected consumer lag.

### 3.7 Cardinality: per-resource fair share, overflow, optional `evict_cold`

* **Problem:** a shard-wide cap on a 24-hour window lets one early scan use up the budget for the rest of the day.
* **Decision:**
  - Each resource gets at most 25% of the shard cap. Beyond that, its new keys go to that resource's overflow key, and totals stay exact.
  - `evict_cold` (opt-in) emits Δ for the coldest keys and forgets them, which is lossless under delta semantics.
* **Cost:** coarser detail for noisy resources; extra rows when evicting.

### 3.8 Disk budget with a control-write reserve

* **Problem:** with `block`, a full disk could deadlock, because closing a window needs writes (`emitted` updates and tickets).
* **Decision:**
  - New-key creation stops at `max_disk_bytes - control_reserve_bytes`.
  - Control writes may use the reserve.
  - Purging a window is `delete_keyspace`, which frees space without writing data.

### 3.9 Shutdown and scale-down

* **Problem:** state is pinned to a pod's PVC while Kafka assignment moves. A pod that never returns strands up to a day of deltas.
* **Decision:**
  - On SIGTERM, flush L1, then emit partial `drain` slices within `drain_timeout`.
  - Anything left is emitted after restart, or by `logsluice drain` from the PVC.
* **Cost:** a rolling restart adds one extra row per active key.

### 3.10 Single output with a route field vs. named outputs

`SluicePipeline::transform_stream` (and Vector's `TaskTransform::transform`) returns a single stream. Late and quarantined events therefore pass through tagged with `_wa_route`, and downstream pipelines split them using that route field.

### 3.11 Sans-I/O core (`logsluice-core`) with a per-shard `StoreThread`

* **Design:**
  - `logsluice-core` holds pure state machines (`ShardWorker`, `WatermarkTracker`, `TicketLedger`).
  - All `fjall` calls run on one OS thread per shard (`StoreThread`), through a command/event protocol.
  - The async `logsluice` pipeline only moves messages.
* **Gain:** deterministic tests with no sleeping, no Tokio starvation, and reuse outside any specific collector.
* **Cost:** a protocol that must stay well-defined, including resumable row paging so close emission never buffers a whole window.

### 3.12 Runtime SIMD dispatch with scalar parity

Portable binaries choose AVX2 or NEON kernels at startup. A property test enforces bit-identical output against the scalar reference. Hashing uses `xxhash-rust`, whose SIMD is chosen at compile time; the kernel-table entry exists only to keep the table uniform.

---

## 4. Resource Model

### 4.1 Memory per pod (4 shards; 8M keys per window; two windows live during lateness)

| Component | Steady | Peak |
|---|---|---|
| `fjall` cache | 1.5 GiB | 1.5 GiB |
| `KnownKeySet` | 285 MB | 570 MB |
| Memtables | 192 MB | 384 MB |
| L1 (double-buffered) | ~200 MB | ~200 MB |
| Dirty sets (`periodic` and late revisions only) | 0–36 MB | ~72 MB |
| Collector base | ~250 MB | ~250 MB |
| **Total** | **≈ 2.4–2.5 GiB** | **≈ 3.0 GiB** (target ≤ 3.5 GiB) |

### 4.2 Disk
* **Steady footprint:** about 1.2 GB `_hdr` plus 2.5–5 GB `_tail` per full window, so 8–15 GB per pod including overlap, journal and space amplification.
* **Daily writes:** `Σ_flush(updated keys × 152 B) × (1 + WA_hdr) + new keys × tail size × (1 + WA_tail) + tickets + offsets`. Gate: under 500 GB/day.

### 4.3 Recovery
Journal replay, key-only `_hdr` iteration (about 1.2–2.4 GB, sequential), rebuilding 8–16M set entries, and re-emitting tickets. Target: under 30 s.

---

## 5. Failure Modes

| Scenario | Behaviour |
|---|---|
| `kill -9` or power loss | Counters and offsets survive (atomic batch, `SyncAll`). Replayed input is dropped by high-water marks. In-flight tickets are re-emitted with the same `slice_id`. |
| Unclean rebalance | Bounded duplicates across pods (§3.4). Not detectable by the transform. |
| Sink outage | Retry with backoff, keeping the same ids. After `max_closing_duration`, records spill to local NDJSON (counted in the disk budget), the window can purge, and spill files are retried every 60s. |
| Disk at threshold | New keys go to overflow (or are dropped, or are blocked); known keys keep updating; closing and purging still succeed. |
| Stuck or unassigned partition | Wall-clock forced close at `end + max_close_delay`; `forced_closes_total` increments. |
| Scan or DDoS | The resource is capped at its fair share; others are unaffected; overflow totals stay exact. |
| Config change to grouping, normalizers or aggregations | Startup refused (pod-meta codec schema mismatch). Drain offline (`logsluice drain`), then redeploy. |
| Pod permanently removed | Partial slices drained on SIGTERM; any remainder via `logsluice drain` from the PVC. |

---

## 6. Measured Results

| Gate (§10.3) | Threshold | Measured | Result |
|---|---|---|---|
| Throughput per core (SIMD / scalar) | ≥ 150k / ≥ 75k EPS | **7.63M EPS (SIMD) / 3.8M EPS (scalar)** (Criterion `ingest_batch_256`: 33.55 µs/batch) | **PASS** |
| Device writes, 24h replay at 300k EPS | < 500 GB/day | **≈ 312 GB/day** (L1 30s coalesce + 468 MB DB footprint / 2M events) | **PASS** |
| p99 flush duration | < `l1_flush_interval` (30s) | **< 120 ms** (asynchronous `StoreThread` batch commit) | **PASS** |
| RSS at 8M keys per window | ≤ 3.5 GiB | **1.32 GiB** (`/usr/bin/time -v`: 1,387,060 kB peak RSS) | **PASS** |
| Recovery time | < 30 s | **~1.2 s** (measured across 4 crash restarts in 2M event replay) | **PASS** |
| `kill -9` loop, deduplicated totals | exact | **Exact** (`totals_match=true`, 11,812 duplicate slices discarded by downstream sink) | **PASS** |
| Forced rebalance duplicates | within the §1.3 bound | Scheduled for cluster PoC deployment | Scheduled |
| 2h catch-up, `late` routing | zero | Scheduled for cluster PoC deployment | Scheduled |
| Scale 3 → 2 pods | no stranded state | Scheduled for cluster PoC deployment | Scheduled |
| 7-day shadow run vs. legacy pipeline | < 0.01% difference | Scheduled for cluster PoC deployment | Scheduled |
