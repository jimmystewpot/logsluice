# `logsluice` Agent & Contributor Guide (`AGENTS.md`)

This document defines the architecture boundaries, coding standards, core state-machine invariants, and verification commands for AI coding agents and contributors working in the `logsluice` repository.

---

## 1. Workspace Structure & Boundaries

- **`crates/logsluice-core` (`logsluice_core`):**
  - Runtime-independent, Sans-I/O windowed aggregation engine.
  - Must **never** depend on `tokio` or any collector framework. All disk I/O runs on a dedicated per-shard OS thread (`StoreThread`) communicating with `ShardWorker` via `StoreCommand` / `StoreEvent` channels.
- **`crates/logsluice` (`logsluice`):**
  - Standalone async `tokio` multi-shard pipeline (`SluicePipeline`), configuration (`SluiceConfig`), self-contained `LogEvent` / `FieldPath` / `AckNotifier`, field normalizers, native & OTLP JSON router/renderer, and `logsluice_*` metrics.
  - Must **never** depend on unpublished external collector crates (`vector-core`, `vector-lib`), ensuring 100% standalone `crates.io` publishability.
- **`crates/logsluice-cli` (bin: `logsluice`):**
  - CLI verification harness (`logsluice replay`) and offline PVC state drain tool (`logsluice drain`).
- **`bridges/logsluice-vector` (`publish = false`, `[workspace] exclude`):**
  - Optional thin adapter bridging `vector_lib::event::LogEvent` and `vector_core::transform::TaskTransform` to `logsluice::SluicePipeline`.

---

## 2. Dependency & Compiler Lint Standards

1. **Single Source of Truth in Root `Cargo.toml`:**
   - Every dependency version is declared once in `[workspace.dependencies]` in the root `Cargo.toml`.
   - Member crates (`crates/logsluice-core`, `crates/logsluice`, `crates/logsluice-cli`) must reference dependencies strictly via `{ workspace = true }` and inherit `[lints] workspace = true`.
   - Never add inline `version = "..."` strings to member crate manifests (enforced by `crates/logsluice-core/tests/repo_hygiene.rs`).
2. **Rust 2024 & Strict Clippy Lints:**
   - `unsafe_op_in_unsafe_fn = "deny"` — every `unsafe` operation inside an `unsafe fn` (such as SIMD intrinsics in `crates/logsluice-core/src/simd/`) must be wrapped in an explicit `unsafe { ... }` block with a safety comment.
   - `clippy::unwrap_used = "deny"`, `clippy::expect_used = "deny"`, `clippy::dbg_macro = "deny"`, `clippy::todo = "deny"` — handle all errors explicitly with `Result` / `Option` combinators or `?` in non-test code.
   - `missing_docs = "warn"` (enforced as error in CI via `-D warnings`) — every public struct, enum, trait, function, and module in `src/**` must have rustdoc comments.
   - Prefer `crate::` imports over `super::` in non-test modules (`super::` is permitted inside `#[cfg(test)]`).

---

## 3. Core Engine Invariants (Do Not Break)

1. **Atomic L1-to-L2 Flush (`ShardStore::apply_flush`):**
   - Each L1 flush writes `w{epoch}_hdr`, `w{epoch}_tail`, `_resources`, and `_offsets` in a **single** `fjall::WriteBatch` followed by WAL sync before releasing upstream `AckFinalizers`.
2. **Emission Ticket Before Output (`TicketLedger`):**
   - Slices are persisted as tickets in `_tickets` **before** leaving the pipeline. `emitted_counters` and `slice_seq` advance only after downstream `AckStatus::Delivered` is received.
3. **Sequential Merge-Join Close Scan (`ShardStore::rows`):**
   - Window close/drain scans iterate `w{epoch}_hdr` and `w{epoch}_tail` in lockstep using forward range iterators — never per-key point `get()` lookups on `_tail`.
4. **Conditional Eviction Deletion (`EmitReason::Evict`):**
   - In `ShardStore::apply_ack`, an evicted key is deleted from `_hdr` and `_tail` **if and only if** `stored.total_counters == stored.emitted_counters`, and `ShardWorker` only removes the key from `KnownKeySet` if it is not concurrently buffered in `l1_active`.
5. **SIMD / Scalar Bit-Parity:**
   - Any change to `crates/logsluice-core/src/simd/` (`x86_avx2.rs` or `arm_neon.rs`) must maintain bit-identical output with `scalar.rs` (verified by `crates/logsluice-core/tests/simd_parity.rs`).

---

## 4. Verification Gate

Before submitting any change, run the full verification suite from the repository root:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo run -p logsluice --example otlp_reduce
cargo bench -p logsluice-core --bench hot_path -- --quick
```
