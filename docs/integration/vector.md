# Integrating `logsluice` with Vector and Custom Collectors

`logsluice` is architected so that its core engine (`logsluice-core`) and async multi-shard pipeline (`logsluice`) compile, test, and publish to `crates.io` completely standalone without requiring a local Vector checkout.

This guide covers two integration paths:
1. **Standalone Rust Collector / Service Integration** using `logsluice::SluicePipeline` directly from `crates.io`.
2. **Vector `TaskTransform` Integration** using the optional `bridges/logsluice-vector` adapter crate.

---

## 1. Embedding `logsluice` in a Standalone Rust Collector

Add `logsluice` to your `Cargo.toml`:

```toml
[dependencies]
logsluice = "0.1"
tokio = { version = "1", features = ["full"] }
futures = "0.3"
```

### Example: Streaming Native or OTLP Log Events

```rust
use futures::{StreamExt, stream};
use logsluice::config::SluiceConfig;
use logsluice::event::{AckStatus, LogEvent};
use logsluice::pipeline::SluicePipeline;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg: SluiceConfig = serde_json::from_value(json!({
        "envelope_mode": "auto",
        "emit_mode": "delta_slice",
        "shards": 2,
        "storage": {
            "data_dir": "/tmp/logsluice-demo"
        },
        "window": {
            "duration": "1s"
        },
        "group_by": ["service", "status"],
        "aggregations": {
            "event_count": "count"
        }
    }))?;

    let pipeline = SluicePipeline::new(cfg)?;

    let mut ev = LogEvent::from_value(json!({
        "@timestamp": "2026-10-20T00:00:00.100Z",
        "service": "checkout",
        "status": "200",
        "body": "order completed"
    }))
    .ok_or("expected JSON object")?;

    // Optional: attach an end-to-end acknowledgment receiver
    let (ev, ack_rx) = ev.with_ack_notifier();

    let output_stream = Box::new(pipeline).transform_stream(stream::iter(vec![ev]));
    let emitted: Vec<LogEvent> = output_stream.collect().await;

    // Input finalizers resolve once the L1 batch is durably flushed to Fjall
    assert_eq!(ack_rx.await?, AckStatus::Delivered);

    for mut slice in emitted {
        // Acknowledge emitted slice back to the ticket ledger so emitted_counters advance
        let finalizers = slice.take_finalizers();
        finalizers.update_status(AckStatus::Delivered);
        drop(finalizers);
    }

    Ok(())
}
```

---

## 2. Wiring `bridges/logsluice-vector` into a Vector Fork

Because `vector-core` and `vector-lib` are not published to `crates.io` with stable semver releases, the Vector `TaskTransform` adapter lives in `bridges/logsluice-vector` and is listed in `[workspace] exclude` in the root `Cargo.toml`.

### 2.1 Architecture of `bridges/logsluice-vector`

`bridges/logsluice-vector` is a thin translation layer around `logsluice::SluicePipeline`:
1. **Inbound (`vector_to_sluice_event`):** Converts `vector_lib::event::LogEvent` fields into `logsluice::event::LogEvent` (`serde_json::Map<String, Value>`) and bridges `vector_core::event::EventFinalizers` into a `logsluice::event::AckFinalizer` drop guard.
2. **Pipeline Execution:** Delegates routing, L1 coalescing, Fjall persistence, watermark tracking, and ticketed slice emission to `logsluice::SluicePipeline`.
3. **Outbound (`sluice_to_vector_event`):** Converts emitted `logsluice::event::LogEvent` slices back into `vector_lib::event::LogEvent` and wires Vector's `BatchNotifier` to the slice's `AckFinalizers` so downstream sink delivery (`BatchStatus::Delivered` / `Errored` / `Rejected`) updates the `logsluice` ticket ledger.

### 2.2 Registering the Transform in Vector

1. In your Vector repository's `Cargo.toml`, add path or git dependencies on `logsluice` and `logsluice-vector`:
   ```toml
   logsluice = { git = "https://github.com/jimmystewpot/logsluice.git", package = "logsluice" }
   logsluice-vector = { git = "https://github.com/jimmystewpot/logsluice.git", path = "bridges/logsluice-vector" }
   ```
2. Enable the `vector-runtime` feature in `bridges/logsluice-vector/Cargo.toml` and point `vector-lib` / `vector-core` at your local Vector workspace crates.
3. Register `WindowedAggregateConfig` in Vector's transform inventory (`src/transforms/mod.rs`).

### 2.3 Example Vector Pipeline Configuration

```yaml
sources:
  kafka_logs:
    type: kafka
    bootstrap_servers: "kafka:9092"
    group_id: "vector-logsluice"
    topics: ["ingress-logs"]
    acknowledgements:
      enabled: true

transforms:
  sluice_rollup:
    type: windowed_aggregate
    inputs: ["kafka_logs"]
    envelope_mode: auto
    emit_mode: delta_slice
    shards: 4
    source_position:
      topic_field: "topic"
      partition_field: "partition"
      offset_field: "offset"
    storage:
      data_dir: "/var/lib/vector/logsluice"
    window:
      type: tumbling
      duration: 24h
      allowed_lateness: 1h
    group_by:
      - "source.ip"
      - "destination.ip"
      - "destination.port"
      - "event.action"
    aggregations:
      event_count: count
      total_bytes:
        sum: "network.bytes"

  split_routes:
    type: route
    inputs: ["sluice_rollup"]
    route:
      summary: '._wa_route == "summary"'
      late: '._wa_route == "late"'
      quarantine: '._wa_route == "quarantine"'
```
