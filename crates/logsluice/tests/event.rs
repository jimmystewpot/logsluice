#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

use logsluice::event::{AckFinalizers, AckNotifier, AckStatus, FieldPath, LogEvent, Value};

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
