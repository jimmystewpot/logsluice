#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

use logsluice_core::simd::{SimdKernelTable, SimdMode};
use proptest::prelude::*;

fn tables() -> (SimdKernelTable, SimdKernelTable) {
    (
        SimdKernelTable::detect(SimdMode::Auto),
        SimdKernelTable::detect(SimdMode::Scalar),
    )
}

#[test]
fn scalar_backend_named() {
    assert_eq!(
        SimdKernelTable::detect(SimdMode::Scalar).backend_name,
        "scalar"
    );
}

#[test]
fn fingerprint_rules_exact() {
    let (_, s) = tables();
    let mut out = Vec::new();
    (s.fingerprint_log_bytes)(
        b"Connection from 10.1.2.3:5432 refused id=550e8400-e29b-41d4-a716-446655440000 code 0x1F",
        &mut out,
    );
    assert_eq!(
        out,
        b"Connection from <IP>:<NUM> refused id=<UUID> code <HEX>".to_vec()
    );
}

#[test]
fn diff_detects_zero_and_wraps() {
    let (_, s) = tables();
    let mut d = [[0u64; 4]];
    assert!(!(s.diff_counter_banks)(
        &[[5, 0, 0, 0]],
        &[[5, 0, 0, 0]],
        &mut d
    ));
    assert!((s.diff_counter_banks)(
        &[[1, 0, 0, 0]],
        &[[2, 0, 0, 0]],
        &mut d
    ));
    assert_eq!(d[0][0], u64::MAX); // wrapping: signed −1
}

#[test]
fn ipv4_mapped_cidr_mask() {
    let (_, s) = tables();
    let ip = std::net::Ipv4Addr::new(10, 1, 2, 3)
        .to_ipv6_mapped()
        .octets();
    let masked = (s.mask_ip_cidr)(ip, 96 + 24);
    assert_eq!(&masked[12..], &[10, 1, 2, 0]);
}

#[test]
fn long_digit_strings_and_timestamps() {
    let (f, s) = tables();
    let cases: &[&[u8]] = &[
        b"event at 1729155760000 status=200",
        b"1234567890",
        b"123456789012345678901234567890",
        b"prefix 999999999999999999999999999999 suffix",
        b"ts=1729155760000 ip=192.168.1.1 count=100",
    ];

    for &input in cases {
        let mut out_auto = Vec::new();
        let mut out_scalar = Vec::new();
        (f.fingerprint_log_bytes)(input, &mut out_auto);
        (s.fingerprint_log_bytes)(input, &mut out_scalar);
        assert_eq!(out_auto, out_scalar);
    }

    let mut out = Vec::new();
    (s.fingerprint_log_bytes)(b"event at 1729155760000 status=200", &mut out);
    assert_eq!(out, b"event at <NUM> status=<NUM>".to_vec());
}

proptest! {
    #[test]
    fn test_simd_scalar_bit_parity(
        a in prop::collection::vec(any::<[u64;4]>(), 1..8),
        b in prop::collection::vec(any::<[u64;4]>(), 1..8),
        text in prop::collection::vec(any::<u8>(), 0..512),
        vals in prop::collection::vec(any::<i64>(), 1..8),
        prefix in 0u8..=128,
        ip in any::<[u8;16]>(),
        seed in any::<u64>(),
    ) {
        let (f, s) = tables();
        let n = a.len().min(b.len());
        let (mut x, mut y) = (a[..n].to_vec(), a[..n].to_vec());
        (f.accumulate_counter_banks)(&mut x, &b[..n]);
        (s.accumulate_counter_banks)(&mut y, &b[..n]);
        prop_assert_eq!(&x, &y);
        let (mut dx, mut dy) = (vec![[0u64;4]; n], vec![[0u64;4]; n]);
        prop_assert_eq!((f.diff_counter_banks)(&a[..n], &b[..n], &mut dx),
                        (s.diff_counter_banks)(&a[..n], &b[..n], &mut dy));
        prop_assert_eq!(dx, dy);
        let (mut mx, mut my) = (vec![(i64::MAX, i64::MIN); vals.len()], vec![(i64::MAX, i64::MIN); vals.len()]);
        (f.update_minmax_slots)(&mut mx, &vals);
        (s.update_minmax_slots)(&mut my, &vals);
        prop_assert_eq!(mx, my);
        prop_assert_eq!((f.mask_ip_cidr)(ip, prefix), (s.mask_ip_cidr)(ip, prefix));
        let (mut ox, mut oy) = (Vec::new(), Vec::new());
        (f.fingerprint_log_bytes)(&text, &mut ox);
        (s.fingerprint_log_bytes)(&text, &mut oy);
        prop_assert_eq!(ox, oy);
        prop_assert_eq!((f.hash_key_128)(&text, seed), (s.hash_key_128)(&text, seed));
    }
}
