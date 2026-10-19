//! Runnable example streaming 5 native log events through `SluicePipeline` (`on_close`, 1s window).
#![allow(missing_docs, clippy::print_stdout)]

use std::time::Duration;

use futures::StreamExt;
use indexmap::IndexMap;
use logsluice::config::{AggregationDefinition, EmitStrategy, NormalizerConfig, SluiceConfig};
use logsluice::event::{AckStatus, LogEvent, Value};
use logsluice::pipeline::SluicePipeline;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;

    let mut cfg = SluiceConfig {
        shards: 1,
        normalizers: IndexMap::from([("body".to_string(), NormalizerConfig::LogFingerprint)]),
        group_by: vec!["service.name".to_string(), "http.status".to_string()],
        aggregations: IndexMap::from([
            (
                "event_count".to_string(),
                AggregationDefinition::Simple("count".to_string()),
            ),
            (
                "total_bytes".to_string(),
                AggregationDefinition::Detailed(IndexMap::from([(
                    "sum".to_string(),
                    "http.bytes".to_string(),
                )])),
            ),
        ]),
        ..SluiceConfig::default()
    };
    cfg.storage.data_dir = temp_dir.path().to_path_buf();
    cfg.storage.l1_flush_interval = Duration::from_millis(20);
    cfg.window.duration = Duration::from_secs(1);
    cfg.window.allowed_skew = Duration::ZERO;
    cfg.window.allowed_lateness = Duration::ZERO;
    cfg.window.emit.strategy = EmitStrategy::OnClose;
    cfg.window.emit.close_jitter_max = Duration::ZERO;

    let pipeline = SluicePipeline::new(cfg)?;

    let mut input_events = Vec::with_capacity(5);
    let base_ts_ms: i64 = 1_700_000_000_000;
    for i in 0..5_i64 {
        let mut ev = LogEvent::new();
        ev.insert_path("@timestamp", base_ts_ms + i * 100);
        ev.insert_path("service.name", "checkout-api");
        ev.insert_path("http.status", "200");
        ev.insert_path("http.bytes", 256_i64);
        ev.insert_path("message", format!("processed request #{i}"));
        input_events.push(ev);
    }

    let input_stream = futures::stream::iter(input_events);
    let mut output_stream = Box::new(pipeline).transform_stream(input_stream);

    let mut summaries = Vec::new();
    while let Some(mut ev) = output_stream.next().await {
        let finalizers = ev.take_finalizers();
        finalizers.update_status(AckStatus::Delivered);
        if ev.get_path("_wa_route") == Some(&Value::from("summary")) {
            summaries.push(ev);
        }
    }

    assert!(
        !summaries.is_empty(),
        "expected at least 1 summary event to be emitted"
    );

    for summary in &summaries {
        let rendered = serde_json::to_string_pretty(summary.as_map())?;
        println!("{rendered}");
    }

    Ok(())
}
