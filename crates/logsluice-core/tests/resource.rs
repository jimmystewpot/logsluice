#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use logsluice_core::resource::*;
use serde_json::json;

fn payload(pod: &str) -> ResourceScopePayload {
    ResourceScopePayload {
        resource: BTreeMap::from([
            ("service.name".into(), json!("auth")),
            ("k8s.pod.uid".into(), json!(pod)),
        ]),
        scope_name: "s".into(),
        scope_version: "1".into(),
        scope_attrs: BTreeMap::new(),
    }
}

#[test]
fn exclusion_collapses_pod_churn() {
    let ex = BTreeSet::from(["k8s.pod.uid".to_string()]);
    let (a, b) = (payload("p1").filtered(&ex), payload("p2").filtered(&ex));
    assert_eq!(a, b);
    assert_eq!(a.canonical_hash(), b.canonical_hash());
    assert_ne!(
        payload("p1").canonical_hash(),
        payload("p2").canonical_hash()
    );
}

#[test]
fn intern_refcount_and_gc() {
    let mut t = ResourceScopeTable::new();
    let p = Arc::new(payload("x"));
    let h = p.canonical_hash();
    let (id, new) = t.intern(&p, h, 100);
    assert!(new);
    assert_eq!(t.intern(&p, h, 100), (id, false));
    assert_eq!(t.intern(&p, h, 200), (id, false));
    assert!(t.release_window(100).is_empty()); // still referenced by window 200
    assert_eq!(t.release_window(200), vec![id]);
    assert_eq!(t.len(), 0);
}

#[test]
fn hash_collision_gets_distinct_ids() {
    let mut t = ResourceScopeTable::new();
    let (a, b) = (Arc::new(payload("a")), Arc::new(payload("b")));
    let (ia, _) = t.intern(&a, 5, 1);
    let (ib, nb) = t.intern(&b, 5, 1); // forced same hash, different payload
    assert!(nb && ia != ib);
    assert_eq!(t.get(ib).unwrap().as_ref(), b.as_ref());
}

#[test]
fn test_restore_state_and_advances_next_id() {
    let mut t = ResourceScopeTable::new();
    assert!(t.is_empty());
    assert_eq!(t.len(), 0);

    let p50 = payload("p50");
    t.restore(50, p50.clone(), vec![100, 200]);

    assert_eq!(t.len(), 1);
    assert!(!t.is_empty());
    assert_eq!(t.get(50).unwrap().as_ref(), &p50);

    // Interning a new payload should use an ID >= 51
    let p_new = Arc::new(payload("p_new"));
    let h_new = p_new.canonical_hash();
    let (id_new, is_new) = t.intern(&p_new, h_new, 100);
    assert!(is_new);
    assert!(id_new >= 51);

    // Release window 100: p50 remains (still has window 200), p_new has only window 100 so it's evicted
    let evicted = t.release_window(100);
    assert_eq!(evicted, vec![id_new]);
    assert_eq!(t.get(50).unwrap().as_ref(), &p50);
    assert!(t.get(id_new).is_none());

    // Release window 200: p50 is evicted
    let evicted2 = t.release_window(200);
    assert_eq!(evicted2, vec![50]);
    assert!(t.is_empty());
}

#[test]
fn test_multi_collision_isolation() {
    let mut t = ResourceScopeTable::new();
    let p1 = Arc::new(payload("c1"));
    let p2 = Arc::new(payload("c2"));
    let p3 = Arc::new(payload("c3"));

    // Force all three to share the exact same hash
    let fake_hash = 999;
    let (id1, new1) = t.intern(&p1, fake_hash, 10);
    let (id2, new2) = t.intern(&p2, fake_hash, 20);
    let (id3, new3) = t.intern(&p3, fake_hash, 30);

    assert!(new1 && new2 && new3);
    assert_ne!(id1, id2);
    assert_ne!(id2, id3);
    assert_ne!(id1, id3);

    // Lookups retrieve matching payloads
    assert_eq!(t.get(id1).unwrap().as_ref(), p1.as_ref());
    assert_eq!(t.get(id2).unwrap().as_ref(), p2.as_ref());
    assert_eq!(t.get(id3).unwrap().as_ref(), p3.as_ref());

    // Re-interning matches correct candidate despite identical hash
    assert_eq!(t.intern(&p2, fake_hash, 20), (id2, false));

    // Releasing window 20 evicts only id2
    let evicted = t.release_window(20);
    assert_eq!(evicted, vec![id2]);
    assert_eq!(t.len(), 2);
    assert_eq!(t.get(id1).unwrap().as_ref(), p1.as_ref());
    assert!(t.get(id2).is_none());
    assert_eq!(t.get(id3).unwrap().as_ref(), p3.as_ref());
}

#[test]
fn test_multiple_evictions_sorted() {
    let mut t = ResourceScopeTable::new();
    let p1 = Arc::new(payload("m1"));
    let p2 = Arc::new(payload("m2"));
    let p3 = Arc::new(payload("m3"));

    let (id1, _) = t.intern(&p1, p1.canonical_hash(), 500);
    let (id2, _) = t.intern(&p2, p2.canonical_hash(), 500);
    let (id3, _) = t.intern(&p3, p3.canonical_hash(), 500);

    let mut expected = vec![id1, id2, id3];
    expected.sort_unstable();

    let evicted = t.release_window(500);
    assert_eq!(evicted, expected);
    assert!(t.is_empty());
}

#[test]
fn test_serde_roundtrip() {
    let mut attrs = BTreeMap::new();
    attrs.insert("library.name".to_string(), json!("tokio"));
    let p = ResourceScopePayload {
        resource: BTreeMap::from([
            ("service.name".into(), json!("worker")),
            ("service.version".into(), json!("1.2.3")),
        ]),
        scope_name: "tracer".into(),
        scope_version: "2.0".into(),
        scope_attrs: attrs,
    };

    let serialized = serde_json::to_string(&p).expect("serialization should succeed");
    let deserialized: ResourceScopePayload =
        serde_json::from_str(&serialized).expect("deserialized should succeed");

    assert_eq!(p, deserialized);
    assert_eq!(p.canonical_hash(), deserialized.canonical_hash());
}

#[test]
fn test_filtered_edge_cases() {
    let p = payload("pod-abc");

    // Empty exclusion set
    let ex_empty = BTreeSet::new();
    assert_eq!(p.filtered(&ex_empty), p);

    // Non-existent key
    let ex_missing = BTreeSet::from(["non.existent.key".to_string()]);
    assert_eq!(p.filtered(&ex_missing), p);

    // All keys excluded
    let ex_all = BTreeSet::from(["service.name".to_string(), "k8s.pod.uid".to_string()]);
    let filtered_all = p.filtered(&ex_all);
    assert!(filtered_all.resource.is_empty());
    assert_eq!(filtered_all.scope_name, "s");
}

#[test]
fn canonical_hash_golden_values() {
    let empty = ResourceScopePayload {
        resource: BTreeMap::new(),
        scope_name: String::new(),
        scope_version: String::new(),
        scope_attrs: BTreeMap::new(),
    };
    let resource_only = ResourceScopePayload {
        resource: BTreeMap::from([
            ("service.name".into(), json!("auth")),
            ("k8s.pod.uid".into(), json!("p1")),
        ]),
        scope_name: String::new(),
        scope_version: String::new(),
        scope_attrs: BTreeMap::new(),
    };
    let scope_and_attrs = ResourceScopePayload {
        resource: BTreeMap::from([
            ("service.name".into(), json!("worker")),
            ("service.version".into(), json!("1.2.3")),
        ]),
        scope_name: "tracer".into(),
        scope_version: "2.0".into(),
        scope_attrs: BTreeMap::from([
            ("library.name".into(), json!("tokio")),
            ("retries".into(), json!(3)),
        ]),
    };

    assert_eq!(empty.canonical_hash(), 0xcd8e_7288_c1c1_b81d);
    assert_eq!(resource_only.canonical_hash(), 0x6df8_dc0a_b42d_963e);
    assert_eq!(scope_and_attrs.canonical_hash(), 0xdb33_5490_c4e5_95f9);
}

mod prop_tests {
    use super::*;
    use proptest::prelude::*;

    fn arb_json_scalar() -> impl Strategy<Value = serde_json::Value> {
        prop_oneof![
            Just(serde_json::Value::Null),
            any::<bool>().prop_map(serde_json::Value::Bool),
            any::<i64>().prop_map(|n| serde_json::json!(n)),
            "[a-zA-Z0-9._:/-]{0,32}".prop_map(serde_json::Value::String),
        ]
    }

    fn arb_resource_scope_payload() -> impl Strategy<Value = ResourceScopePayload> {
        (
            proptest::collection::btree_map("[a-zA-Z0-9._-]{1,16}", arb_json_scalar(), 0..8),
            "[a-zA-Z0-9._-]{0,16}",
            "[0-9.]{0,8}",
            proptest::collection::btree_map("[a-zA-Z0-9._-]{1,16}", arb_json_scalar(), 0..8),
        )
            .prop_map(|(resource, scope_name, scope_version, scope_attrs)| {
                ResourceScopePayload {
                    resource,
                    scope_name,
                    scope_version,
                    scope_attrs,
                }
            })
    }

    proptest! {
        #[test]
        fn canonical_hash_matches_serde_json_xxh3_oneshot(p in arb_resource_scope_payload()) {
            let bytes = serde_json::to_vec(&p).expect("serialize");
            let expected = xxhash_rust::xxh3::xxh3_64(&bytes);
            prop_assert_eq!(p.canonical_hash(), expected);
        }
    }
}
