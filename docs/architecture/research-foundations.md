# Research Foundations & Academic Literature: `logsluice`

**Document ID:** ARCH-LOGSLUICE-003  
**Governing Spec:** [design-specification.md](design-specification.md)  
**Companion Trade-offs Doc:** [tradeoffs-and-benchmarks.md](tradeoffs-and-benchmarks.md)  
**Status:** Production Baseline Reference  

---

## 1. Overview: Why Day-Scale Log Reduction Requires Both Stream & Storage Theory

Traditional log reduction transforms in observability collectors were designed for short, best-effort, in-memory deduplication:
1. **Inactivity timers instead of event-time windows:** Recurring network flows or noisy service logs continually reset `expire_after_ms`, preventing persistent keys from ever flushing.
2. **Heap-bound state:** Maintaining millions of active keys alongside their full exemplar payloads in a single-threaded `HashMap` either triggers the OOM killer on multi-hour windows or loses up to 24 hours of accumulated counts on a process restart.
3. **At-least-once double-counting:** Replaying upstream Kafka partitions after a crash or sink timeout blindly increments downstream `SUM` tables unless input offsets and output emissions are tied to durable idempotency invariants.

To sustain **300,000+ EPS per cluster** across **tumbling windows up to 24 hours** and **8M+ active keys per window per pod** within a **≤ 3.5 GiB RSS** envelope, `logsluice` synthesises results from six areas of database systems, storage engines, and distributed stream processing research.

---

## 2. Architectural Pillar-to-Paper Matrix

| `logsluice` Mechanism | Core Codebase Components | Supporting Research Paper(s) | Measured Impact in `logsluice` |
| :--- | :--- | :--- | :--- |
| **Header / Exemplar Tail Split (`w{epoch}_hdr` vs. `w{epoch}_tail`)** | `crates/logsluice-core/src/store/shard_store.rs`<br>`crates/logsluice-core/src/codec/header.rs`<br>`crates/logsluice-core/src/codec/tail.rs` | **WiscKey** (Lu et al., *USENIX FAST 2016*) | Keeps hot-updated `_hdr` rows at a plan-fixed width (`16 + 64B + 16M` bytes; 112 B for the 1-bank, 2-minmax reference plan) while writing variable-length LZ4 `_tail` exemplars once per key; sequential merge-join at window close (`scan_point_gets == 0`). |
| **Double-Buffered L1 In-Memory Coalescing (30s default)** | `crates/logsluice-core/src/l1.rs`<br>`crates/logsluice-core/src/store/thread.rs` | **FASTER** (Chandramouli et al., *ACM SIGMOD 2018*) | Absorbs high-frequency in-place counter mutations in memory before batch-committing to `fjall`, bounding device writes to **~312 GB/day** at 300k EPS (gate: `< 500 GB/day`). |
| **Exact In-Memory Membership Filter (`KnownKeySet`)** | `crates/logsluice-core/src/key.rs`<br>`crates/logsluice-core/src/worker/mod.rs` | **Monkey** (Dayan et al., *ACM SIGMOD 2017*) | Eliminates LSM point-read I/O for new keys (blind inserts) and skips exemplar string allocations for known keys with zero false-positive exemplar loss (~71 MB/shard at 2M keys). |
| **Distributive Rollups & Non-Overlapping Stream Slices** | `crates/logsluice-core/src/window.rs`<br>`crates/logsluice-core/src/codec/plan.rs` | **Data Cube** (Gray et al., *ICDE 1996 / DMKD 1997*)<br>**Pane-Based Aggregation** (Li et al., *SIGMOD Record 2005*)<br>**Scotty** (Traub et al., *IEEE ICDE 2018*) | Restricts aggregations to distributive primitives (`count`, `sum`, `min`, `max`) so `close`, `periodic`, `late_revision`, `evict`, and `drain` slices compose losslessly downstream. |
| **Coordination-Free Multi-Pod Delta State (`delta_slice`)** | `crates/logsluice-core/src/emission.rs`<br>`crates/logsluice-core/src/codec/header.rs` | **$\delta$-CRDTs** *(conceptual analogy)* (Almeida et al., *JPDC 2018*) | Emits subtractable counter deltas (`Δ = total − emitted`, summed after `window.slice_id` primary-key deduplication) and join-semilattice `min`/`max` states (`MIN`/`MAX`) across uncoordinated pods. |
| **Per-Partition Event-Time Watermarks & Triggers** | `crates/logsluice-core/src/watermark.rs`<br>`crates/logsluice-core/src/worker/lifecycle.rs` | **The Dataflow Model** (Akidau et al., *PVLDB 2015*)<br>**MillWheel** (Akidau et al., *PVLDB 2013*) | Tracks `W = min(W_p)` across active Kafka partitions with a caught-up idle guard (`caught_up_threshold`) and startup seed grace to prevent premature idle exclusion during backlog catch-up within `max_close_delay` and `allowed_lateness`. |
| **End-to-End Crash Exactness (`_offsets` + `_tickets` + `slice_id`)** | `crates/logsluice-core/src/position.rs`<br>`crates/logsluice-core/src/emission.rs`<br>`crates/logsluice-core/src/worker/recovery.rs` | **MillWheel** (Akidau et al., *PVLDB 2013*)<br>**Asynchronous Barrier Snapshotting** (Carbone et al., *arXiv:1506.08603, 2015*) | Atomic `fjall::WriteBatch` couples input partition high-water marks with counter updates; pre-emission tickets and deterministic `window.slice_id` guarantee exact totals across `kill -9` loops. |
| **Per-Resource Fair-Share Cardinality Isolation & Online Log Fingerprinting** | `crates/logsluice-core/src/cardinality.rs`<br>`crates/logsluice-core/src/simd/mod.rs` | **Monarch** (Adams et al., *PVLDB 2020*)<br>**Drain** (He et al., *IEEE ICWS 2017*) | Caps any single `ResourceScopeId` to `max_resource_share` (25%) with per-resource overflow buckets and lossless cold-key eviction (`evict_cold`), paired with SIMD log template fingerprinting. |
| **Shared-Nothing Sharding & Vectorised Micro-Batch Execution** | `crates/logsluice/src/router.rs`<br>`crates/logsluice-core/src/simd/` | **MonetDB/X100** (Boncz et al., *CIDR 2005*)<br>**H-Store / Shared-Nothing** (Stonebraker et al., *VLDB 2007*) | 256-event micro-batches routed deterministically (`digest_low64 % shards`) to lock-free single-writer shards; isolated `ShardWorker::try_ingest` L1 microbenchmark reaches **7.63M EPS/core** with AVX2/NEON. |

---

## 3. Deep Dives by Architectural Pillar

### 3.1 LSM Storage: Header/Tail Separation, In-Memory Coalescing & Exact Membership Filtering

#### Theoretical Foundation
1. **WiscKey (Lu et al., FAST 2016)** demonstrated that in Log-Structured Merge-trees (LSM-trees), sorting and compacting large values alongside small keys causes severe SSD write amplification. By separating keys and small metadata from larger immutable or rarely updated values, LSM compaction overhead shrinks proportionally to the key-to-value size ratio.
2. **FASTER (Chandramouli et al., SIGMOD 2018)** showed that update-intensive streaming workloads exhibit strong temporal locality: a hot working set of keys receives repeated read-modify-write updates over short intervals. Performing out-of-place log appends for every individual event overwhelms storage bandwidth; absorbing hot updates in-place in memory before transitioning records to secondary storage reduces write traffic by orders of magnitude.
3. **Monkey (Dayan et al., SIGMOD 2017)** proved that LSM point-lookup cost is governed by filter memory allocation across levels, establishing a formal trade-off between in-memory membership structures and disk read I/O.

#### How `logsluice` Applies and Adapts This Research
* **Domain-Specific WiscKey Split (`w{epoch}_hdr` vs. `w{epoch}_tail`):** Every window epoch opens two `fjall` keyspaces per shard:
  * `w{epoch}_hdr` stores the 24-byte `StorageKey24` (`[window_epoch: u64 | digest: u128]`) mapped to a plan-fixed-width `NumericHeader` (`HeaderLayout::encoded_len() = 16 + 64 * banks + 16 * minmax_slots` bytes—for example, 80 B for a count-only plan or 112 B for the reference 1-bank, 2-minmax plan—covering `resource_scope_id`, `slice_seq`, `last_update_s`, `flags`, `total_counters`, `emitted_counters`, and `minmax` slots).
  * `w{epoch}_tail` stores the same `StorageKey24` mapped to an LZ4-compressed `ExemplarTail` (250–500 B of normalised body, grouping values, and first-seen exemplar attributes) that is **written once when the key is first seen in the window and never rewritten**.
* **Eliminating WiscKey's Range-Query Penalty via Sequential Merge-Join:** A classic drawback of WiscKey's unsorted value log (`vLog`) is that range scans require random point lookups to reassemble values. Because `logsluice` keys `w{epoch}_tail` by the exact same big-endian `StorageKey24` as `w{epoch}_hdr` and never mutates tails after insertion, `ShardStore::rows` performs a **sequential forward merge-join** over both keyspaces in lockstep at window close. Point lookups during close/drain scans are strictly zero (`scan_point_gets == 0`).
* **Two-Tier L1-to-L2 Coalescing (`DoubleBufferL1`):** Rather than issuing an LSM read-modify-write on every incoming log record, each `ShardWorker` accumulates counter deltas and min/max updates in place inside an in-memory `l1_active` hash table (`l1_coalesce_max_keys: 100,000`, `l1_flush_interval: 30s`). When swapped to `l1_flushing`, a single OS thread (`StoreThread`) applies all coalesced updates in one atomic `fjall::WriteBatch`.
* **Exact `KnownKeySet` Over Probabilistic Bloom Filters:** While Monkey optimises probabilistic Bloom filters for general-purpose LSMs, a false positive in `logsluice`'s newness check would treat a brand-new key as already known—skipping `ExemplarTail` construction and leaving the key without an exemplar at window close. `logsluice` instead dedicates ~71 MB per shard per live window (`hashbrown::HashSet<u128>` pre-sized to $2^{22}$ buckets for 2M keys) to an **exact** `KnownKeySet`. On an L1 miss:
  * **New keys** skip the `_hdr` point read entirely (**blind insert** into both `_hdr` and `_tail`).
  * **Known keys** skip `ExemplarTail` allocation and string copying entirely on the hot path, and never touch `_tail` during L1 flush.

---

### 3.2 Window Slicing, Distributive Aggregations & Coordination-Free Delta State

#### Theoretical Foundation
1. **Data Cube Aggregation Taxonomy (Gray et al., ICDE 1996 / DMKD 1997)** classifies aggregate functions into three categories:
   * **Distributive** (`COUNT`, `SUM`, `MIN`, `MAX`): The aggregate of a whole dataset can be computed directly by applying the same function to partial sub-aggregates of its partitions.
   * **Algebraic** (`AVG`, `STDDEV`): Computable from a fixed-size tuple of distributive aggregates (e.g., `SUM / COUNT`).
   * **Holistic** (`MEDIAN`, `PERCENTILE`, exact `COUNT(DISTINCT)`): No constant-size sub-aggregate exists; exact evaluation requires retaining all distinct raw elements.
2. **Pane-Based Aggregation (Li et al., SIGMOD Record 2005)** and **Scotty Stream Slicing (Traub et al., ICDE 2018)** prove that dividing a window into non-overlapping partial sub-aggregates ("panes" or "slices") allows arbitrary window completions, early emissions, and out-of-order late arrivals to be evaluated incrementally without retaining raw tuples.
3. **Delta State Replicated Data Types / $\delta$-CRDTs (Almeida et al., JPDC 2018)** formalise how distributed replicas can converge without inter-node coordination by shipping compact incremental state fragments ($\delta$-states) that are joined downstream.

#### How `logsluice` Applies and Adapts This Research
* **Strict Distributive Contract:** `logsluice` deliberately restricts v1 aggregations to `count`, `sum`, `min`, and `max` (with `avg` computed downstream as `sum / count`). Because holistic metrics cannot be represented in a plan-fixed-width `NumericHeader` (`16 + 64 * banks + 16 * minmax_slots` bytes; 112 B in the reference configuration) or subtracted via $\Delta = \text{total\_counters} - \text{emitted\_counters}$, excluding them guarantees bounded memory and exact incremental emission.
* **Unified Lifecycle Slicing (`close`, `periodic`, `late_revision`, `evict`, `drain`):** Every slice emitted by `logsluice` represents a disjoint delta interval (`slice_seq = 0, 1, 2, ...`) for a given `(emitter_id, window_epoch, key_digest)`:
  * In default **`on_close`** mode, a typical key emits a single slice (`slice_seq = 0`) when the window closes, yielding a 10–40× reduction in downstream rows.
  * When operational pressure requires early emission—such as **`evict_cold`** under cardinality pressure, **`drain`** on `SIGTERM` before a pod scales down, **`periodic`** interim snapshots, or **`late_revision`** after window close—the engine simply emits the unacknowledged delta $\Delta = \text{total\_counters} - \text{emitted\_counters}$ and advances `emitted_counters += Δ` upon downstream acknowledgement.
* **Zero Cross-Pod Coordination on Rebalance:** Conceptually similar to $\delta$-CRDTs, collector pods in a `logsluice` `StatefulSet` never exchange state over the network. Whereas running extremums (`min`, `max`) form a true join-semilattice and merge idempotently via downstream `MIN`/`MAX`, additive counter deltas (`count`, `sum`) are not inherently idempotent on their own—applying the same counter slice twice would double-count. `logsluice` therefore pairs commutative delta emission with mandatory downstream primary-key deduplication on `window.slice_id` before aggregating counters with `SUM`. If a graceful Kafka rebalance moves partition $P$ from Pod 0 to Pod 1 mid-window, Pod 0 emits its slice (`emitter_id = pod-0`) and Pod 1 emits its slice (`emitter_id = pod-1`); across disjoint accepted inputs, downstream `SUM` (for counters) and `MIN`/`MAX` (for extremums) produce exact totals. On an **unclean rebalance** (where a partition changes pods before offsets are committed), pod-local `_offsets` high-water marks cannot prevent both pods from counting the uncommitted overlap under distinct `emitter_id`s, bounding duplicate exposure to at most `l1_flush_interval + commit_interval_ms` ([design-specification.md §1.3](design-specification.md#13-delivery-guarantee-normative)).

---

### 3.3 Event-Time Watermarks, Backlog Catch-Up Guards & Forced Close

#### Theoretical Foundation
1. **MillWheel (Akidau et al., PVLDB 2013)** introduced **low watermarks** in distributed stream processing: an assertion that all work for event times $< W$ has been completed by upstream senders, bounded by the minimum watermark across all active inputs.
2. **The Dataflow Model (Akidau et al., PVLDB 2015)** formalised the separation of **Where** in event time records belong (tumbling windows) from **When** in processing time results are materialised (watermarks, early/on-time/late triggers, and allowed lateness).

#### How `logsluice` Applies and Adapts This Research
* **Per-Partition Event-Time Watermarks (`WatermarkTracker`):** For each Kafka `(topic, partition)` $p$, `logsluice` tracks $W_p = \max(\text{event\_time}_p) - \text{allowed\_skew}$, and computes the global watermark as $W = \min_{p \in \text{active}}(W_p)$.
* **Solving the Backlog Catch-Up Idle Trap (`caught_up_threshold` & `seed_grace_ms`):** A well-known failure mode in naive watermark implementations occurs when a consumer restarts after an outage or reads unevenly across partitions: partitions not yet polled for `idle_timeout` appear "idle" on a wall-clock timer and get excluded from $\min(W_p)$, causing a single caught-up partition to jump the global watermark ahead and drop hours of valid backlog data as `late`. `logsluice` prevents this with two guards in [`WatermarkTracker`](../../crates/logsluice-core/src/watermark.rs):
  1. **Caught-up idle rule:** A partition is excluded as idle **only if** it has been quiet for `idle_timeout` **and** its last observed event timestamp was within `caught_up_threshold` (default 2m) of wall clock when it went quiet. A partition that is lagging behind real time is never marked idle.
  2. **Startup seed grace:** On startup, all `(topic, partition)` pairs recovered from `_offsets` across shards are registered via `seed_known`. Until all seeded partitions have been observed (or `seed_grace_ms = max_close_delay` elapses), `watermark()` returns `None` so caught-up partitions cannot close windows ahead of unpolled partitions.
* **Wall-Clock Progress Guarantee & Operational Catch-Up Bounds (`max_close_delay` & `allowed_lateness`):** While the caught-up idle rule and startup seed grace prevent premature idle exclusion, they do not hold windows open indefinitely. Startup grace expires after `seed_grace_ms = max_close_delay`, and `logsluice` enforces a wall-clock forced close at `end + max_close_delay` (incrementing `logsluice_forced_closes_total`) followed by purge at `end + max_close_delay + allowed_lateness`. Likewise, a partition newly assigned after the watermark has already advanced can still deliver late events, so operators must size `max_close_delay` and `allowed_lateness` to cover expected outage and consumer-lag horizons ([design-specification.md §5.3–5.4](design-specification.md#53-watermarks)).

---

### 3.4 Crash-Exact Delivery Without Distributed Two-Phase Commit

#### Theoretical Foundation
1. **MillWheel (Akidau et al., PVLDB 2013)** achieved end-to-end exact accounting without heavyweight distributed two-phase commit (2PC) by combining **input deduplication** with **atomic checkpointing of state mutations and pending "strong productions"** (persisting output intent with deterministic sequence IDs before transmitting downstream, and retrying until acknowledged).
2. **Asynchronous Barrier Snapshotting / ABS (Carbone et al., arXiv:1506.08603, 2015)** showed how multi-stage cyclic/acyclic dataflow graphs (such as Apache Flink) coordinate global checkpoints across network shuffles using stream barriers.

#### How `logsluice` Applies and Adapts This Research
Because `logsluice` operates as a single-stage stateful rollup (requiring no network shuffle between collector pods), it avoids the global barrier alignment latency of Flink ABS and the sink compatibility limitations of Kafka 2PC transactions by implementing MillWheel's **local atomic commit + strong production** pattern per shard:
1. **Input Idempotency via Atomic Offset High-Water Marks (`_offsets`):**
   * Within any Kafka `(topic, partition)`, messages arrive in monotonically increasing offset order, and `Router` maps each record's `XXH3-128` digest deterministically to a fixed shard (`digest_low64 % shards`).
   * During `ShardStore::apply_flush`, the highest offset seen per `(topic, partition)` on that shard is written to the `_offsets` keyspace **in the exact same `fjall::WriteBatch` and WAL `fsync`** as the counter updates in `w{epoch}_hdr` and `w{epoch}_tail` **before** upstream `AckFinalizers` are released.
   * After a `kill -9` crash, Kafka replays from the last committed consumer offset and each replayed event routes to the exact same shard:
     * **Already-flushed events (`offset <= hwm_shard[(topic, partition)]`):** Events whose L1 flush had already been durably committed to `fjall` before the crash (but whose Kafka offset was not yet committed upstream) are dropped before touching L1 (`logsluice_replayed_events_total`), preventing double-counting.
     * **Unflushed L1 events (`offset > hwm_shard[(topic, partition)]`):** Events that were only in volatile `l1_active` / `l1_flushing` memory at the moment of the crash exceed the recovered high-water mark and are ingested and accumulated normally, preventing data loss.
2. **Output Idempotency via Pre-Emission Ticket Ledger (`_tickets`) & Deterministic `slice_id`:**
   * Before any slice leaves a shard, `TicketLedger` persists an emission intent record (`ticket_id`, `reason`, and up to 500 `(key, slice_seq, delta, minmax)` entries) to the `_tickets` keyspace and syncs the journal.
   * Each emitted record carries `window.slice_id = "{emitter_id}:{window_epoch}:{digest_hex}:{slice_seq}"`.
   * Only when downstream `AckStatus::Delivered` arrives does `ShardStore::apply_ack` advance `emitted_counters += delta`, increment `slice_seq += 1`, and delete the ticket.
   * If the collector crashes after sending a batch to Kafka/StarRocks but before persisting the acknowledgement, startup recovery (`worker/recovery.rs`) replays the persisted ticket verbatim with the **identical `window.slice_id`**, which downstream primary-key tables deduplicate automatically.

---

### 3.5 Multi-Tenant Cardinality Isolation & Online Log Fingerprinting

#### Theoretical Foundation
1. **Monarch (Adams et al., PVLDB 2020)** documented how planet-scale multi-tenant telemetry systems defend against "cardinality explosions" (such as a single misconfigured service or port scan generating millions of unique label combinations) by enforcing strict per-tenant memory/cardinality quotas and structured schemas.
2. **Drain (He et al., IEEE ICWS 2017)** demonstrated that unstructured log bodies can be clustered online in streaming fashion by normalising variable tokens (IPs, numbers, hex strings, UUIDs) into fixed structural templates.

#### How `logsluice` Applies and Adapts This Research
* **Per-Resource Fair-Share Quotas (`CardinalityGuard`):** A naive global cap (`max_keys_per_window: 2,000,000`) on a 24-hour window allows one noisy service or port scan at 00:05 UTC to exhaust the key budget for the remaining 23 hours and 55 minutes. `logsluice` interns each `(resource, scope)` pair to a shard-local `ResourceScopeId(u32)` and enforces `max_resource_share` (default `0.25`).
* **Lossless Overflow Buckets & Conditional Cold Eviction:**
  * In `on_cap: overflow` mode, once a `ResourceScopeId` reaches its 25% share, new keys from that resource are routed to `StorageKey24::overflow(epoch, resource_scope_id)`. Aggregate counts and byte/packet sums for that resource remain 100% exact while other resources retain 75% of the shard capacity.
  * In `on_cap: evict_cold` mode, reaching `evict_high_water` (90%) triggers a background scan that emits the coldest 10% of keys (by `last_update_s`) as `evict` delta slices. To prevent race conditions if new events arrive for an evicting key while its ticket is in flight, `ShardStore::apply_ack` deletes the key from `_hdr` and `_tail` **if and only if** `stored.total_counters == stored.emitted_counters`.
* **Hardware-Accelerated Online Normalisation:** Inspired by online log template parsers like Drain, `logsluice` normalises high-churn tokens before hashing the key via runtime-dispatched SIMD kernels (`fingerprint_log_bytes` and conditional `mask_ip_cidr`), alongside `resource_attributes_exclude` (stripping `k8s.pod.uid`, `process.pid`, `service.instance.id`).

---

### 3.6 Shared-Nothing Sharding, Sans-I/O & Vectorised Micro-Batch Execution

#### Theoretical Foundation
1. **MonetDB/X100 (Boncz, Zukowski, Nes, CIDR 2005)** showed that tuple-at-a-time processing suffers from severe instruction-cache misses and dispatch overhead on modern superscalar CPUs, whereas processing cache-resident micro-batches ("vectors") amortises control flow and enables tight SIMD loops.
2. **H-Store / Shared-Nothing Partitioning (Stonebraker et al., VLDB 2007; SIGMOD Record 2005)** argued that high-throughput real-time stream and OLTP engines should eliminate cross-thread mutexes and shared mutable state in favour of deterministic key partitioning across independent single-threaded execution shards.

#### How `logsluice` Applies and Adapts This Research
* **Thin Router + Micro-Batch Channel Dispatch:** `Router` performs envelope detection, timestamp extraction, SIMD normalisation, and `XXH3-128` hashing, accumulating owned `IngestItem`s into per-shard micro-batches of **256 items** (or `1ms` max linger). At 300,000 EPS, channel traffic drops from 300,000 lock acquisitions/sec to ~1,200 batch sends/sec.
* **Zero Cross-Shard Contention:** Each `ShardWorker` exclusively owns its `KnownKeySet`, `ResourceScopeTable`, `DoubleBufferL1`, `TicketLedger`, and dedicated `StoreThread` (`shard_{n}/`). No locks are ever taken across shards.
* **Runtime-Dispatched SIMD Kernels (`SimdKernelTable`):** Counter bank accumulation (`accumulate_counter_banks`), delta subtraction (`diff_counter_banks`), min/max slot updates (`update_minmax_slots`), IPv4/IPv6 CIDR masking (`mask_ip_cidr`), and log byte fingerprinting (`fingerprint_log_bytes`) execute via `AVX2` on `x86_64` and `NEON` on `aarch64`, verified in CI for bit-identical parity against the scalar reference implementation (`crates/logsluice-core/tests/simd_parity.rs`). In Criterion's isolated worker-ingest microbenchmark (`ingest_batch_256`, which feeds pre-built 256-item batches directly into `ShardWorker::try_ingest` with synthetic flush completion), L1 accumulation reaches **7.63M EPS/core** with SIMD (**3.8M EPS/core** scalar), keeping the in-memory hot path well ahead of the 300,000+ EPS full-pipeline cluster target.

---

## 4. Annotated Bibliography

1. **Lu, L., Pillai, T. S., Arpaci-Dusseau, A. C., & Arpaci-Dusseau, R. H. (2016).** *WiscKey: Separating Keys from Values in SSD-Conscious Storage.* In **Proceedings of the 14th USENIX Conference on File and Storage Technologies (FAST '16)**, pp. 133–148.
2. **Chandramouli, B., Prasaad, G., Kossmann, D., Levandoski, J., Hunter, J., & Barnett, M. (2018).** *FASTER: A Concurrent Key-Value Store with In-Place Updates.* In **Proceedings of the 2018 International Conference on Management of Data (ACM SIGMOD '18)**, pp. 275–290.
3. **Dayan, N., Athanassoulis, M., & Idreos, S. (2017).** *Monkey: Optimal Navigable Key-Value Store.* In **Proceedings of the 2017 ACM International Conference on Management of Data (ACM SIGMOD '17)**, pp. 79–94. *(SIGMOD Best Paper Award)*.
4. **Gray, J., Chaudhuri, S., Bosworth, A., Layman, A., Reichart, D., Venkatrao, M., Pellow, F., & Pirahesh, H. (1997).** *Data Cube: A Relational Aggregation Operator Generalizing Group-By, Cross-Tab, and Sub-Totals.* **Data Mining and Knowledge Discovery**, 1(1), 29–53. *(Originally presented at IEEE ICDE '96)*.
5. **Li, J., Maier, D., Tufte, K., Papadimos, V., & Tucker, P. A. (2005).** *No Pane, No Gain: Efficient Evaluation of Sliding-Window Aggregates over Data Streams.* **ACM SIGMOD Record**, 34(1), 39–44.
6. **Traub, J., Grulich, P. M., Cuéllar, A. R., Breß, S., Katsifodimos, A., Rabl, T., & Markl, V. (2018).** *Scotty: Efficient Window Aggregation for Out-of-Order Stream Processing.* In **Proceedings of the 34th IEEE International Conference on Data Engineering (ICDE '18)**, pp. 1300–1303.
7. **Almeida, P. S., Shoker, A., & Baquero, C. (2018).** *Delta State Replicated Data Types.* **Journal of Parallel and Distributed Computing (JPDC)**, 111, 162–173.
8. **Akidau, T., Balikov, A., Bekiroğlu, K., Chernyak, S., Haberman, J., Lax, R., McVeety, S., Mills, D., Nordstrom, P., & Whittle, S. (2013).** *MillWheel: Fault-Tolerant Stream Processing at Internet Scale.* **Proceedings of the VLDB Endowment (PVLDB)**, 6(11), 1033–1044.
9. **Akidau, T., Bradshaw, R., Chambers, C., Chernyak, S., Fernández-Moctezuma, R. J., Lax, R., McVeety, S., Mills, D., Perry, F., Schmidt, E., & Whittle, S. (2015).** *The Dataflow Model: A Practical Approach to Balancing Correctness, Latency, and Cost in Massive-Scale, Unbounded, Out-of-Order Data Processing.* **Proceedings of the VLDB Endowment (PVLDB)**, 8(12), 1792–1803.
10. **Carbone, P., Fóra, G., Ewen, S., Haridi, S., & Tzoumas, K. (2015).** *Lightweight Asynchronous Snapshots for Distributed Dataflows.* **arXiv preprint arXiv:1506.08603**.
11. **Adams, C., et al. (2020).** *Monarch: Google's Planet-Scale In-Memory Time Series Database.* **Proceedings of the VLDB Endowment (PVLDB)**, 13(12), 3181–3194.
12. **He, P., Zhu, J., Zheng, Z., & Lyu, M. R. (2017).** *Drain: An Online Log Parsing Approach with Fixed Depth Tree.* In **Proceedings of the 2017 IEEE International Conference on Web Services (ICWS '17)**, pp. 33–40.
13. **Boncz, P. A., Zukowski, M., & Nes, N. (2005).** *MonetDB/X100: Hyper-Pipelining Query Execution.* In **Proceedings of the 2nd Biennial Conference on Innovative Data Systems Research (CIDR '05)**, pp. 225–237.
14. **Stonebraker, M., Madden, S., Abadi, D. J., Harizopoulos, S., Hachem, N., & Helland, P. (2007).** *The End of an Architectural Era (It's Time for a Complete Rewrite).* In **Proceedings of the 33rd International Conference on Very Large Data Bases (VLDB '07)**, pp. 1150–1160.
