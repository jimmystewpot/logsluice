# Contributing to `logsluice`

Thank you for your interest in contributing to `logsluice`!

## Development Prerequisites

- Rust `1.85+` (Rust 2024 edition; configured automatically via `rust-toolchain.toml`).
- Standard `cargo` tools (`rustfmt`, `clippy`).
- Optional: [`cargo-deny`](https://github.com/EmbarkStudios/cargo-deny) for supply-chain and license checks, and [`cargo-llvm-cov`](https://github.com/taiki-e/cargo-llvm-cov) for local coverage reports.

## Building & Testing

Run the full workspace verification gate before opening a pull request:

```bash
# 1. Check formatting
cargo fmt --all -- --check

# 2. Run Clippy with warnings denied across all targets and features
cargo clippy --workspace --all-targets --all-features -- -D warnings

# 3. Run unit, integration, property, CLI, and repository hygiene tests
cargo test --workspace --all-features

# 4. Run the standalone example
cargo run -p logsluice --example otlp_reduce

# 5. Run Criterion benchmark smoke tests
cargo bench -p logsluice-core --bench hot_path -- --quick
```

## Coding Conventions

1. **Workspace Dependency Inheritance:** Add new dependencies to `[workspace.dependencies]` in the root `Cargo.toml` and reference them via `{ workspace = true }` in member crates.
2. **No Panics in Library Code:** `unwrap()`, `expect()`, `dbg!()`, and `todo!()` are denied by workspace Clippy lints in non-test code. Propagate errors via `AggError` / `AggResult`.
3. **Documentation:** All public items in library crates require doc comments (`missing_docs = "warn"`).
4. **SIMD Safety & Bit-Parity:** Any modification to SIMD kernels in `crates/logsluice-core/src/simd/` must wrap unsafe intrinsics in explicit `unsafe { ... }` blocks and pass `crates/logsluice-core/tests/simd_parity.rs`.

## License

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in `logsluice` by you shall be licensed under the [Apache License, Version 2.0](LICENSE), without any additional terms or conditions.
