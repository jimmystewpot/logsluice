# `logsluice` Agent & Contributor Guide (`AGENTS.md`)

This document defines the architecture boundaries, coding standards, core state-machine invariants, CI/release guardrails, and verification commands for AI coding agents and contributors working in the `logsluice` repository.

---

## 1. Workspace Structure & Boundaries

- **`crates/logsluice-core` (`logsluice_core`):**
  - Runtime-independent, Sans-I/O windowed aggregation engine.
  - Must **never** depend on `tokio` or any collector framework. All disk I/O runs on a dedicated per-shard OS thread (`StoreThread`) communicating with `ShardWorker` via `StoreCommand` / `StoreEvent` channels.
  - `src/worker/` is decomposed by responsibility (`mod.rs`, `events.rs`, `lifecycle.rs`, `poll.rs`, `recovery.rs`, `spill.rs`), and `src/codec/` shares wire cursor primitives via `cursor.rs`. `StoreCommand` fault-injection variants (`TestPause`, `FailNextPersist`, `FailNextFlush`, `FailNextRows`, `FailNextSpill`) are `#[doc(hidden)]` test hooks, and pipeline-level flush fault injection (`SluicePipeline::fail_next_flush`) is gated behind `#[cfg(test)]`.
- **`crates/logsluice` (`logsluice`):**
  - Standalone async `tokio` multi-shard pipeline (`SluicePipeline`), configuration (`SluiceConfig`), self-contained `LogEvent` / `FieldPath` / `AckNotifier`, field normalisers, native & OTLP JSON router/renderer, and `logsluice_*` metrics.
  - Must **never** depend on unpublished external collector crates (`vector-core`, `vector-lib`), ensuring 100% standalone `crates.io` publishability.
- **`crates/logsluice-cli` (bin: `logsluice`):**
  - CLI verification harness (`logsluice replay`) and offline PVC state drain tool (`logsluice drain`).
- **`bridges/logsluice-vector` (`publish = false`, `[workspace] exclude`):**
  - Optional thin adapter bridging `vector_lib::event::LogEvent` and `vector_core::transform::TaskTransform` to `logsluice::SluicePipeline`.

---

## 2. Dependency, Documentation & Compiler Lint Standards

1. **Single Source of Truth in Root `Cargo.toml`:**
   - Every dependency version is declared once in `[workspace.dependencies]` in the root `Cargo.toml`.
   - Member crates (`crates/logsluice-core`, `crates/logsluice`, `crates/logsluice-cli`) must reference dependencies strictly via `{ workspace = true }` and inherit `[lints] workspace = true`.
   - Never add inline `version = "..."` strings to member crate manifests (enforced by `crates/logsluice-core/tests/repo_hygiene.rs`).
2. **Rust 2024 & Strict Clippy Lints:**
   - `unsafe_op_in_unsafe_fn = "deny"` — every `unsafe` operation inside an `unsafe fn` (such as SIMD intrinsics in `crates/logsluice-core/src/simd/`) must be wrapped in an explicit `unsafe { ... }` block with a safety comment.
   - `clippy::unwrap_used = "deny"`, `clippy::expect_used = "deny"`, `clippy::dbg_macro = "deny"`, `clippy::todo = "deny"` — handle all errors explicitly with `Result` / `Option` combinators or `?` in non-test code.
   - `missing_docs = "warn"` (enforced as error in CI via `-D warnings`) — every public struct, enum, trait, function, and module in `src/**` must have rustdoc comments.
   - Prefer `crate::` imports over `super::` in non-test modules (`super::` is permitted inside `#[cfg(test)]`).
3. **Australian Business English & `crates.io` README Hygiene:**
   - All Markdown documentation, `README.md` files, and new or modified comments/user-facing prose must be written in **Australian Business English** (e.g. *normaliser*, *normalisation*, *serialisation*, *acknowledgement*, *initialise*, *behaviour*, *optimise*, *licence* as a noun), while keeping Rust symbols, SPDX identifiers, and YAML configuration keys (`Normalizer`, `normalizers:`, `AckFinalizers`, `Apache-2.0`, `serde`) exact.
   - Each publishable crate's `README.md` (`crates/*/README.md`) must include `## The Problem It Solves`, `## How It Solves It`, `## When to Use Which Crate`, and `## Licence`, and every Markdown link target (`](...)`) in `crates/*/README.md` must use an absolute `https://` URL (or `#` anchor) so links work when rendered on `crates.io` (enforced by `crates/logsluice-core/tests/repo_hygiene.rs`).

---

## 3. Core Engine & Pipeline Invariants (Do Not Break)

1. **Atomic L1-to-L2 Flush (`ShardStore::apply_flush`):**
   - Each L1 flush writes `w{epoch}_hdr`, `w{epoch}_tail`, `_resources`, and `_offsets` in a **single** `fjall::WriteBatch` followed by WAL sync before releasing upstream `AckFinalizers`.
2. **Double-Buffer Flush Pending & Waiter Preservation (`DoubleBuffer`):**
   - When `DoubleBuffer::try_swap` is refused because a prior L1 flush is still in flight, the pending flush flag (`flush_pending`) must remain set and any `FlushDone` waiters must stay queued until the deferred active buffer is swapped and durably committed on `StoreEvent::FlushDone`.
3. **Emission Ticket Before Output & Shared Payload Ownership (`TicketLedger`):**
   - Slices are persisted as tickets in `_tickets` **before** leaving the pipeline. `emitted_counters` and `slice_seq` advance only after downstream `AckStatus::Delivered` is received.
   - In-flight ticket payloads are stored and shared as `Arc<[SliceRecord]>` across emission, retry, and spill paths to avoid deep cloning `Vec<SliceRecord>`.
4. **Sequential Merge-Join Close Scan (`ShardStore::rows`):**
   - Window close/drain scans — and keyed ticket replay — read `w{epoch}_hdr`/`w{epoch}_tail` via forward range iterators (`ShardStore::rows`, `ShardStore::rows_for_sorted`). Keyed reads in scan APIs must go through `ShardStore::counted_get` so `scan_point_gets` stays 0; never per-key `Keyspace::get` on `_tail`.
5. **Conditional Eviction Deletion & $O(N)$ Selection (`EmitReason::Evict`):**
   - Cold-key eviction (`evict_cold`) selects the coldest keys using `select_nth_unstable_by` ($O(N)$ partial selection) rather than a full sort.
   - In `ShardStore::apply_ack`, an evicted key is deleted from `_hdr` and `_tail` **if and only if** `stored.total_counters == stored.emitted_counters`, and `ShardWorker` only removes the key from `KnownKeySet` if it is not concurrently buffered in `l1_active`.
6. **Multi-Shard Startup Lock Cleanup (`SluicePipeline`):**
   - If `SluicePipeline` encounters an error while initialising shard $k$, all previously opened shard `StoreThread` instances ($0..k$) must be shut down cleanly before returning the error so `fjall` database locks are released immediately.
7. **SIMD / Scalar Bit-Parity:**
   - Any change to `crates/logsluice-core/src/simd/` (`x86_avx2.rs` or `arm_neon.rs`) must maintain bit-identical output with `scalar.rs` (verified by `crates/logsluice-core/tests/simd_parity.rs`).

---

## 4. CI/CD & Release Workflow Guardrails

- **Toolchain & Cross-Compilation Prerequisites:**
  - Workflows must ensure a C linker (`build-essential`) is installed. Cross-compiling `aarch64-unknown-linux-gnu` release binaries with `apt-get install --no-install-recommends` requires both `gcc-aarch64-linux-gnu` and `libc6-dev-arm64-cross` (so `Scrt1.o` and `crti.o` are present).
- **Release Tag Verification & Idempotency (`.github/workflows/release.yml`):**
  - Checkout must use `fetch-depth: 0` and verify that the pushed tag is an ancestor of `origin/main` (`git merge-base --is-ancestor HEAD origin/main`) and matches `v${CARGO_VERSION}` from `Cargo.toml`.
  - Each `cargo publish` step checks `https://crates.io/api/v1/crates/<crate>/<version>` first so workflow re-runs skip already-published versions cleanly.
- **Benchmark Cache Hygiene (`.github/workflows/benchmarks.yml`):**
  - Remove `target/criterion` before invoking `cargo bench` and use `set -o pipefail` when piping benchmark output.

---

## 5. Verification Gate

Before submitting any change, run the full verification suite from the repository root:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo run -p logsluice --example otlp_reduce
cargo bench -p logsluice-core --bench hot_path -- --quick
```
