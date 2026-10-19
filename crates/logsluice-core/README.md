# `logsluice-core`

Runtime-independent, crash-safe windowed log aggregation and reduction engine for [`logsluice`](https://github.com/jimmystewpot/logsluice).

## Architecture

- **Canonical Key Builder (`key`):** Deterministic length-prefixed canonical key serialization hashed with 128-bit `XXH3-128`.
- **SIMD & Scalar Reduction Kernels (`simd`):** Runtime-dispatched AVX2, NEON, and scalar 64-bit integer/IEEE-754 float accumulation (`count`, `sum`, `min`, `max`).
- **L1 Double-Buffered Arena (`l1`):** Lock-free `hashbrown::HashMap` with arena-allocated metric slots and bounded head/tail sample ring buffers.
- **L2 Durable Fjall LSM Store (`store`):** Atomic write-batch persistence across `slices_v1`, `meta_v1`, and `emit_tickets_v1` keyspaces with background `StoreThread` worker offloading.
- **Crash-Safe Lifecycle & Spill (`worker`, `watermark`, `position`, `cardinality`):** Monotonic per-producer watermarks, two-phase emission tickets (`Prepared` -> `Sent` -> GC), and disk-bounded `SpillMode` (`Passthrough`, `LocalDisk`, `DropNewest`).

## License

Licensed under the Apache License, Version 2.0 ([`LICENSE`](../../LICENSE)).
