# `logsluice-vector`

Optional workspace-excluded adapter crate that bridges [`vector`](https://github.com/vectordotdev/vector)'s `vector_lib::event::LogEvent`, `EventFinalizers`, and `vector_core::transform::TaskTransform` to the standalone [`logsluice`](../../crates/logsluice) aggregation pipeline.

## Why is this crate excluded from the default workspace?

`vector-core` and `vector-lib` are unpublished internal workspace crates inside the upstream `vectordotdev/vector` repository. By isolating the Vector-specific glue inside `bridges/logsluice-vector` (listed in root `Cargo.toml` under `[workspace] exclude`), the core `logsluice-core`, `logsluice`, and `logsluice-cli` crates remain 100% standalone and publishable to `crates.io` without requiring a local Vector checkout.

## Architecture

1. **Event Conversion (`VectorEventCodec`):**
   - Converts inbound Vector `LogEvent` maps (converting `Value::Bytes` to UTF-8 strings and `Value::Timestamp` to RFC3339 or millisecond epoch numbers) into `logsluice::event::LogEvent`.
   - Converts outbound `logsluice::event::LogEvent` summary slices back into Vector `LogEvent` instances.
2. **End-to-End Acknowledgement Bridging (`BatchStatusCode` <-> `AckStatus`):**
   - Attaches a `logsluice::event::AckNotifier` to each inbound event before routing into `SluicePipeline`.
   - When `SluicePipeline` flushes L1 into Fjall L2 (`FlushDone`) or forwards a passthrough event, the `AckFinalizer` resolves and updates the upstream Vector `EventFinalizers` / `BatchNotifier`.
   - When `SluicePipeline` emits a summary `LogEvent`, a Vector `BatchNotifier` is attached to the outbound Vector `Event::Log`; when the downstream Vector sink acknowledges or rejects the batch, the status is forwarded back to `logsluice`'s `AckFinalizer::update_status(...)` so `ShardWorker::on_ack(ticket_id, delivered, now_ms)` commits or retries the emission ticket.
3. **`TaskTransform` Adapter (`VectorSluiceTransform`):**
   - Wraps `SluicePipeline` and transforms any `Stream<Item = E>` where `E: VectorEventAdapter` (such as `vector_lib::event::Event` or `LogEvent`).

## Wiring into a Vector Fork

1. Uncomment `vector-core`, `vector-lib`, and `vector-config` in `bridges/logsluice-vector/Cargo.toml` and point them to your Vector workspace (`path = "../../../vector/lib/..."` or git dependency).
2. Implement `VectorEventAdapter` for `vector_lib::event::LogEvent` (or use the snippet in [`docs/integration/vector.md`](../../docs/integration/vector.md)) and register `VectorSluiceTransform` as a `TaskTransform<EventArray>`.

## Licence

Licensed under the Apache License, Version 2.0 ([`LICENSE`](../../LICENSE)).
