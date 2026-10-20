#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

use logsluice_core::config::*;
use logsluice_core::error::AggError;
use std::time::Duration;

#[test]
fn defaults_match_spec() {
    let c = EngineConfig::default();
    assert_eq!(c.window.emit.strategy, EmitStrategy::OnClose);
    assert_eq!(c.window.emit.interval, None);
    assert_eq!(c.storage.l1_flush_interval, Duration::from_secs(30));
    assert_eq!(
        c.storage.control_reserve_bytes,
        c.storage.max_disk_bytes / 10
    );
    assert_eq!(c.cardinality.max_keys_per_window, 2_000_000);
    assert_eq!(c.window.max_close_delay, Duration::from_secs(7200));
    c.validate().unwrap();
}

#[test]
fn periodic_requires_interval_shorter_than_duration() {
    let mut c = EngineConfig::default();
    c.window.emit.strategy = EmitStrategy::Periodic;
    assert!(matches!(
        c.validate(),
        Err(AggError::Config(m)) if m.contains("emit.interval")
    ));
    c.window.emit.interval = Some(Duration::ZERO);
    assert!(c.validate().is_err());
    c.window.emit.interval = Some(c.window.duration);
    assert!(c.validate().is_err());
    c.window.emit.interval = Some(Duration::from_secs(900));
    c.validate().unwrap();
}

#[test]
fn on_close_rejects_interval() {
    let mut c = EngineConfig::default();
    c.window.emit.interval = Some(Duration::from_secs(900));
    assert!(matches!(
        c.validate(),
        Err(AggError::Config(m)) if m.contains("emit.interval")
    ));
}

#[test]
fn close_delay_reserve_and_share_rules() {
    let mut c = EngineConfig::default();
    c.window.max_close_delay = c.window.allowed_skew;
    assert!(c.validate().is_err());
    let mut c = EngineConfig::default();
    c.window.allowed_lateness = c.window.max_close_delay + Duration::from_secs(1);
    assert!(c.validate().is_err());
    let mut c = EngineConfig::default();
    c.storage.control_reserve_bytes = c.storage.max_disk_bytes;
    assert!(c.validate().is_err());
    let mut c = EngineConfig::default();
    c.cardinality.max_resource_share = 0.0;
    assert!(c.validate().is_err());
    let mut c = EngineConfig::default();
    c.cardinality.max_resource_share = 1.05;
    assert!(c.validate().is_err());
    let c = EngineConfig {
        shards: 0,
        ..Default::default()
    };
    assert!(c.validate().is_err());
}

#[test]
fn unknown_aggregation_kind_rejected() {
    assert!(
        serde_json::from_str::<AggregationSpec>(r#"{"name":"x","kind":"avg","field":"a"}"#)
            .is_err()
    );
    assert!(serde_json::from_str::<AggregationSpec>(r#"{"name":"c","kind":"count"}"#).is_ok());
}

#[test]
fn aggregation_field_rules() {
    let mut c = EngineConfig::default();
    c.aggregations.push(AggregationSpec {
        name: "cnt".to_string(),
        kind: AggregationKind::Count,
        field: Some("field1".to_string()),
    });
    assert!(c.validate().is_err());

    c.aggregations.clear();
    c.aggregations.push(AggregationSpec {
        name: "cnt".to_string(),
        kind: AggregationKind::Count,
        field: None,
    });
    c.validate().unwrap();

    for kind in [
        AggregationKind::Sum,
        AggregationKind::Min,
        AggregationKind::Max,
    ] {
        c.aggregations.clear();
        c.aggregations.push(AggregationSpec {
            name: "metric".to_string(),
            kind,
            field: None,
        });
        assert!(c.validate().is_err());

        c.aggregations.clear();
        c.aggregations.push(AggregationSpec {
            name: "metric".to_string(),
            kind,
            field: Some("metric.field".to_string()),
        });
        c.validate().unwrap();
    }
}

#[test]
fn serde_roundtrip() {
    let c = EngineConfig::default();
    let json = serde_json::to_string_pretty(&c).unwrap();
    let deserialized: EngineConfig = serde_json::from_str(&json).unwrap();
    assert_eq!(c, deserialized);
    deserialized.validate().unwrap();
}

#[test]
fn config_rejects_zero_worker_threads() {
    let mut c = EngineConfig::default();
    assert_eq!(c.storage.worker_threads, 2);
    c.validate().unwrap();

    c.storage.worker_threads = 0;
    assert!(matches!(
        c.validate(),
        Err(AggError::Config(ref m)) if m.contains("worker_threads")
    ));
}
