#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

use std::str::FromStr;

use logsluice::event::{
    AckFinalizers, AckNotifier, AckStatus, FieldPath, LogEvent, ObjectMap, Value, value_to_bytes,
    value_to_string_lossy,
};

#[test]
fn field_path_supports_escaped_and_quoted_segments() {
    let p1 = FieldPath::parse(".resource.\"service.name\".host").unwrap();
    assert_eq!(p1.segments(), &["resource", "service.name", "host"]);
    assert_eq!(p1.as_str(), ".resource.\"service.name\".host");

    let p2 = FieldPath::parse(r"resource.service\.name.host").unwrap();
    assert_eq!(p2.segments(), &["resource", "service.name", "host"]);

    let json = serde_json::to_string(&p1).unwrap();
    let roundtrip: FieldPath = serde_json::from_str(&json).unwrap();
    assert_eq!(roundtrip, p1);

    assert!(FieldPath::parse("").is_err());
    assert!(FieldPath::parse(".").is_err());
    assert!(FieldPath::parse("a..b").is_err());
    assert!(FieldPath::parse("a.b.").is_err());
    assert!(FieldPath::parse("a.\"unterminated").is_err());
}

#[test]
fn field_path_get_insert_remove_nested_and_missing() {
    let mut ev = LogEvent::new();
    let path_ip = FieldPath::parse("source.ip").unwrap();
    let path_quoted = FieldPath::parse("resource.\"service.name\"").unwrap();
    let path_missing = FieldPath::parse("source.port.number").unwrap();

    assert!(ev.get(&path_ip).is_none());
    assert!(ev.insert(&path_ip, Value::from("10.0.0.1")).is_none());
    assert_eq!(ev.get(&path_ip), Some(&Value::from("10.0.0.1")));
    assert_eq!(ev.get_path("source.ip"), Some(&Value::from("10.0.0.1")));

    // Mutate in place
    if let Some(v) = ev.get_mut(&path_ip) {
        *v = Value::from("10.0.0.2");
    }
    assert_eq!(ev.get(&path_ip), Some(&Value::from("10.0.0.2")));

    // Quoted segment keeps literal dot in nested map key
    ev.insert(&path_quoted, Value::from("checkout"));
    assert_eq!(ev.get(&path_quoted), Some(&Value::from("checkout")));
    assert!(ev.get_path("resource.service.name").is_none());

    // Missing path returns None on get and remove
    assert!(ev.get(&path_missing).is_none());
    assert!(ev.remove(&path_missing).is_none());

    // Remove nested key
    let removed = ev.remove(&path_ip);
    assert_eq!(removed, Some(Value::from("10.0.0.2")));
    assert!(ev.get(&path_ip).is_none());
}

#[tokio::test]
async fn ack_notifier_waits_for_all_clones_and_escalates_worst_status() {
    let (f1, mut rx) = AckNotifier::new_with_receiver();
    let f2 = f1.clone();
    let f3 = f1.clone();

    f1.update_status(AckStatus::Delivered);
    drop(f1);
    assert!(rx.try_recv().is_err());

    f2.update_status(AckStatus::Errored);
    drop(f2);
    assert!(rx.try_recv().is_err());

    // Attempting to downgrade back to Delivered must keep Errored
    f3.update_status(AckStatus::Delivered);

    let mut set = AckFinalizers::from_single(f3);
    assert!(!set.is_empty());

    let (ev, mut rx2) = LogEvent::new().with_ack_notifier();
    let (_, ev_finalizers) = ev.into_parts();
    ev_finalizers.update_status(AckStatus::Rejected);
    set.merge(ev_finalizers);

    drop(set);

    assert_eq!(rx.await.unwrap(), AckStatus::Errored);
    assert_eq!(rx2.try_recv().unwrap(), AckStatus::Rejected);
}

#[test]
fn field_path_and_log_event_conversions() {
    // FieldPath::from_str, Display, PartialEq, escaped quotes, and error cases
    let p1 = FieldPath::from_str(r#"resource."service\.name".host"#).unwrap();
    let p2: FieldPath = r#"resource."service\.name".host"#.parse().unwrap();
    assert_eq!(p1, p2);
    assert_eq!(p1.to_string(), r#"resource."service\.name".host"#);
    assert_eq!(p1.segments(), &["resource", "service.name", "host"]);

    // Trailing backslash parse errors (unquoted and inside quotes) and empty segment errors
    assert!(FieldPath::from_str("a\\").is_err());
    assert!(FieldPath::from_str("a.\"b\\").is_err());
    assert!(FieldPath::from_str("a..b").is_err());
    assert!(serde_json::from_str::<FieldPath>("\"a\\\\\"").is_err());

    // parse_or_literal fallback on invalid path
    let fallback = FieldPath::parse_or_literal("a..b");
    assert_eq!(fallback.segments(), &["a..b"]);

    // LogEvent::from_value (both Value::Object and non-object None) and From<ObjectMap>
    let mut map = ObjectMap::new();
    map.insert("k".to_string(), Value::from("v"));
    let ev_from_val = LogEvent::from_value(Value::Object(map.clone())).unwrap();
    let mut ev_from_map = LogEvent::from(map);
    assert_eq!(ev_from_val, ev_from_map);
    assert!(LogEvent::from_value(Value::Null).is_none());
    assert!(LogEvent::from_value(Value::from(42)).is_none());

    // as_map_mut, insert overwriting non-object intermediate, get/get_mut/remove on non-object intermediate
    ev_from_map
        .as_map_mut()
        .insert("scalar".to_string(), Value::from(123));
    let nested_under_scalar = FieldPath::parse("scalar.child").unwrap();
    assert!(ev_from_map.get(&nested_under_scalar).is_none());
    assert!(ev_from_map.get_mut(&nested_under_scalar).is_none());
    assert!(ev_from_map.remove(&nested_under_scalar).is_none());

    // insert auto-vivifies over non-object intermediate
    ev_from_map.insert(&nested_under_scalar, Value::from("promoted"));
    assert_eq!(
        ev_from_map.get(&nested_under_scalar),
        Some(&Value::from("promoted"))
    );

    // insert_path and remove_path (valid and invalid paths)
    ev_from_map.insert_path("valid.path", "ok");
    ev_from_map.insert_path("invalid..path", "ignored");
    assert_eq!(ev_from_map.get_path("valid.path"), Some(&Value::from("ok")));
    assert_eq!(
        ev_from_map.remove_path("valid.path"),
        Some(Value::from("ok"))
    );
    assert!(ev_from_map.remove_path("invalid..path").is_none());

    // finalizers, take_finalizers, merge_finalizers, and AckFinalizer PartialEq
    assert!(ev_from_map.finalizers().is_empty());
    let (ev_with_ack, mut rx) = ev_from_map.with_ack_notifier();
    assert!(!ev_with_ack.finalizers().is_empty());
    let mut ev_clone = ev_with_ack.clone();
    assert_eq!(ev_with_ack.finalizers(), ev_clone.finalizers());

    let taken = ev_clone.take_finalizers();
    assert!(ev_clone.finalizers().is_empty());
    let mut merged = AckFinalizers::new();
    merged.merge(taken);
    ev_clone.merge_finalizers(merged);
    drop(ev_with_ack);
    drop(ev_clone);
    assert_eq!(rx.try_recv().unwrap(), AckStatus::Delivered);

    // value_to_bytes and value_to_string_lossy on Null, booleans, numbers, arrays, strings, and nested objects
    assert_eq!(value_to_string_lossy(&Value::Null), "null");
    assert_eq!(value_to_bytes(&Value::Null).as_ref(), b"null");
    assert_eq!(value_to_string_lossy(&Value::Bool(true)), "true");
    assert_eq!(value_to_bytes(&Value::Bool(false)).as_ref(), b"false");
    assert_eq!(value_to_string_lossy(&Value::from(99)), "99");
    assert_eq!(value_to_bytes(&Value::from(99)).as_ref(), b"99");
    assert_eq!(value_to_bytes(&Value::from("hello")).as_ref(), b"hello");

    let arr = serde_json::json!([1, "two"]);
    assert_eq!(value_to_string_lossy(&arr), "[1,\"two\"]");
    assert_eq!(value_to_bytes(&arr).as_ref(), b"[1,\"two\"]");

    let obj = serde_json::json!({"nested": {"a": 1}});
    assert_eq!(value_to_string_lossy(&obj), "{\"nested\":{\"a\":1}}");
    assert_eq!(value_to_bytes(&obj).as_ref(), b"{\"nested\":{\"a\":1}}");
}
