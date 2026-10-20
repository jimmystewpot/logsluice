#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

use bytes::Bytes;
use logsluice_core::codec::{header::*, plan::*, tail::*, ticket::*};
use logsluice_core::config::{AggregationKind::*, AggregationSpec};
use proptest::prelude::*;

fn spec_example() -> Vec<AggregationSpec> {
    let a = |n: &str, k, f: Option<&str>| AggregationSpec {
        name: n.into(),
        kind: k,
        field: f.map(Into::into),
    };
    vec![
        a("event_count", Count, None),
        a("total_bytes", Sum, Some("network.bytes")),
        a("total_packets", Sum, Some("network.packets")),
        a("first_seen", Min, Some("@timestamp")),
        a("last_seen", Max, Some("@timestamp")),
    ]
}

#[test]
fn spec_example_header_is_112_bytes() {
    let p = AggregationPlan::new(&spec_example()).unwrap();
    assert_eq!(
        p.layout(),
        HeaderLayout {
            banks: 1,
            minmax_slots: 2
        }
    );
    assert_eq!(p.layout().encoded_len(), 112);
    assert_eq!(p.counter_slot(2), Some((0, 2)));
    assert_eq!(p.minmax_slot(4), Some(1));
    assert_eq!(p.counter_slot(3), None);
    assert_eq!(p.minmax_slot(0), None);
    assert_eq!(p.counter_slot(999), None);
    assert_eq!(p.minmax_slot(999), None);
}

#[test]
fn test_schema_bytes_format() {
    let p = AggregationPlan::new(&spec_example()).unwrap();
    let schema = p.schema_bytes();
    // Verify 4 bytes banks LE, 4 bytes minmax_slots LE prefix,
    // followed by each spec formatted as name\0kind\0field\0
    let len = schema.len();
    assert!(len >= 8);
    let banks = u32::from_le_bytes(schema[0..4].try_into().unwrap());
    let minmax = u32::from_le_bytes(schema[4..8].try_into().unwrap());
    assert_eq!(banks, 1);
    assert_eq!(minmax, 2);
    assert_eq!(HeaderLayout::from_schema_prefix(&schema), Some(p.layout()));
    assert_eq!(HeaderLayout::from_schema_prefix(&schema[..7]), None);

    let specs_str = String::from_utf8_lossy(&schema[8..]);
    assert!(specs_str.contains("event_count\0count\0\0"));
    assert!(specs_str.contains("total_bytes\0sum\0network.bytes\0"));
    assert!(specs_str.contains("first_seen\0min\0@timestamp\0"));
    assert!(specs_str.contains("last_seen\0max\0@timestamp\0"));
}

#[test]
fn test_plan_with_zero_and_multiple_banks() {
    let empty_plan = AggregationPlan::new(&[]).unwrap();
    assert_eq!(
        empty_plan.layout(),
        HeaderLayout {
            banks: 0,
            minmax_slots: 0
        }
    );
    assert_eq!(empty_plan.layout().encoded_len(), 16);

    let a = |n: &str, k, f: Option<&str>| AggregationSpec {
        name: n.into(),
        kind: k,
        field: f.map(Into::into),
    };
    let specs = vec![
        a("c1", Count, None),
        a("c2", Count, None),
        a("c3", Count, None),
        a("c4", Count, None),
        a("c5", Count, None),
    ];
    let five_counters = AggregationPlan::new(&specs).unwrap();
    assert_eq!(five_counters.layout().banks, 2);
    assert_eq!(five_counters.counter_slot(0), Some((0, 0)));
    assert_eq!(five_counters.counter_slot(3), Some((0, 3)));
    assert_eq!(five_counters.counter_slot(4), Some((1, 0)));
}

#[test]
fn header_roundtrip_and_corruption() {
    let l = HeaderLayout {
        banks: 1,
        minmax_slots: 2,
    };
    let mut h = NumericHeader::new_zeroed(l, 42);
    h.slice_seq = 3;
    h.total[0] = [10, 20, 30, 0];
    h.emitted[0] = [4, 0, 0, 0];
    h.minmax[1] = (-5, 9);
    let b = h.encode(l);
    assert_eq!(b.len(), 112);
    assert_eq!(NumericHeader::decode(l, &b).unwrap(), h);
    assert!(NumericHeader::decode(l, &b[..111]).is_err());
    let mut extended = b.clone();
    extended.push(0);
    assert!(NumericHeader::decode(l, &extended).is_err());
}

#[test]
fn tail_roundtrip_preserves_missing_vs_empty() {
    let t = ExemplarTail {
        severity: 9,
        body: Some(Bytes::from_static(b"refused")),
        dims: vec![None, Some(Bytes::new()), Some(Bytes::from_static(b"443"))],
        attrs: vec![("env".into(), Bytes::from_static(b"\"prod\""))],
    };
    assert_eq!(ExemplarTail::decode(&t.encode()).unwrap(), t);
    assert!(ExemplarTail::decode(&[2]).is_err()); // unknown version
}

#[test]
fn tail_decoding_error_handling() {
    // Empty bytes
    assert!(ExemplarTail::decode(&[]).is_err());
    // Only 1 byte
    assert!(ExemplarTail::decode(&[1]).is_err());

    // Valid tail encoded
    let valid = ExemplarTail {
        severity: 1,
        body: Some(Bytes::from_static(b"hello")),
        dims: vec![Some(Bytes::from_static(b"dim1"))],
        attrs: vec![("k".into(), Bytes::from_static(b"v"))],
    }
    .encode();

    // Trailing garbage
    let mut trailing = valid.clone();
    trailing.push(0);
    assert!(ExemplarTail::decode(&trailing).is_err());

    // Truncated at various positions
    for cut in 2..valid.len() {
        assert!(ExemplarTail::decode(&valid[..cut]).is_err());
    }

    // Invalid body tag
    let mut invalid_tag = valid.clone();
    invalid_tag[2] = 2; // neither 0 nor 1
    assert!(ExemplarTail::decode(&invalid_tag).is_err());
}

#[test]
fn decode_rejects_huge_dims_count() {
    let mut encoded = ExemplarTail {
        severity: 1,
        body: None,
        dims: vec![],
        attrs: vec![],
    }
    .encode();

    // Find where dims_count is located in encoded:
    // [0]: version (1)
    // [1]: severity (1)
    // [2]: body_tag (0)
    // [3..7]: dims_count (u32 LE)
    assert!(encoded.len() >= 7);
    encoded[3..7].copy_from_slice(&u32::MAX.to_le_bytes());

    let res = ExemplarTail::decode(&encoded);
    assert!(res.is_err());
}

#[test]
fn decode_never_panics_on_mutations() {
    let t = ExemplarTail {
        severity: 5,
        body: Some(Bytes::from_static(b"body")),
        dims: vec![Some(Bytes::from_static(b"dim1"))],
        attrs: vec![("key".into(), Bytes::from_static(b"val"))],
    };
    let valid = t.encode();

    // 1. Truncation test: every prefix length
    for len in 0..=valid.len() {
        let _ = ExemplarTail::decode(&valid[..len]);
    }

    // 2. Single-byte mutation test: flip every byte value
    for i in 0..valid.len() {
        let original = valid[i];
        for b in [0, 1, 0xff, original.wrapping_add(1)] {
            let mut mutated = valid.clone();
            mutated[i] = b;
            let _ = ExemplarTail::decode(&mutated);
        }
    }
}

#[test]
fn ticket_roundtrip() {
    let l = HeaderLayout {
        banks: 1,
        minmax_slots: 2,
    };
    let t = Ticket {
        id: 7,
        window_epoch_ms: 86_400_000,
        reason: EmitReason::Close,
        entries: vec![TicketEntry {
            digest: 0xabc,
            slice_seq: 0,
            delta: vec![[3, 0, 0, 0]],
            minmax: vec![(1, 2), (3, 4)],
        }],
    };
    assert_eq!(Ticket::decode(l, &t.encode(l)).unwrap(), t);
    assert_eq!(EmitReason::LateRevision.as_str(), "late_revision");
}

#[test]
fn test_emit_reasons_strings_and_wire_tags() {
    let reasons = [
        (EmitReason::Close, "close", 0u8),
        (EmitReason::Periodic, "periodic", 1u8),
        (EmitReason::LateRevision, "late_revision", 2u8),
        (EmitReason::Evict, "evict", 3u8),
        (EmitReason::Drain, "drain", 4u8),
    ];
    for (reason, s, tag) in reasons {
        assert_eq!(reason.as_str(), s);
        assert_eq!(reason.to_u8(), tag);
        assert_eq!(EmitReason::from_u8(tag).unwrap(), reason);
    }
    assert!(EmitReason::from_u8(5).is_err());
}

#[test]
fn ticket_decoding_error_handling() {
    let l = HeaderLayout {
        banks: 1,
        minmax_slots: 1,
    };
    let t = Ticket {
        id: 42,
        window_epoch_ms: 1000,
        reason: EmitReason::Drain,
        entries: vec![TicketEntry {
            digest: 0x1234,
            slice_seq: 1,
            delta: vec![[1, 2, 3, 4]],
            minmax: vec![(-10, 10)],
        }],
    };
    let encoded = t.encode(l);

    // Truncated header
    assert!(Ticket::decode(l, &encoded[..20]).is_err());

    // Truncated entry
    assert!(Ticket::decode(l, &encoded[..encoded.len() - 1]).is_err());

    // Trailing bytes
    let mut trailing = encoded.clone();
    trailing.push(0);
    assert!(Ticket::decode(l, &trailing).is_err());

    // Invalid emit reason tag in wire bytes
    let mut bad_reason = encoded.clone();
    bad_reason[16] = 99;
    assert!(Ticket::decode(l, &bad_reason).is_err());
}

proptest! {
    #[test]
    fn prop_test_numeric_header_roundtrip(
        scope_id in any::<u32>(),
        slice_seq in any::<u32>(),
        last_update_s in any::<u32>(),
        flags in any::<u32>(),
        b0 in any::<[u64; 4]>(),
        b1 in any::<[u64; 4]>(),
        e0 in any::<[u64; 4]>(),
        e1 in any::<[u64; 4]>(),
        mm0 in any::<(i64, i64)>(),
        mm1 in any::<(i64, i64)>(),
    ) {
        let layout = HeaderLayout { banks: 2, minmax_slots: 2 };
        let header = NumericHeader {
            resource_scope_id: scope_id,
            slice_seq,
            last_update_s,
            flags,
            total: vec![b0, b1],
            emitted: vec![e0, e1],
            minmax: vec![mm0, mm1],
        };
        let encoded = header.encode(layout);
        prop_assert_eq!(encoded.len(), layout.encoded_len());
        let decoded = NumericHeader::decode(layout, &encoded).unwrap();
        prop_assert_eq!(header, decoded);
    }

    #[test]
    fn prop_test_tail_roundtrip(
        severity in any::<u8>(),
        body in prop::option::of(prop::collection::vec(any::<u8>(), 0..64)),
        dims in prop::collection::vec(prop::option::of(prop::collection::vec(any::<u8>(), 0..32)), 0..5),
        attrs in prop::collection::vec(("[a-z]{1,8}", prop::collection::vec(any::<u8>(), 0..32)), 0..5),
    ) {
        let tail = ExemplarTail {
            severity,
            body: body.map(Bytes::from),
            dims: dims.into_iter().map(|d| d.map(Bytes::from)).collect(),
            attrs: attrs.into_iter().map(|(k, v)| (k, Bytes::from(v))).collect(),
        };
        let encoded = tail.encode();
        let decoded = ExemplarTail::decode(&encoded).unwrap();
        prop_assert_eq!(tail, decoded);
    }
}
