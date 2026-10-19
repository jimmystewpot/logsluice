#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

use logsluice_core::{key::*, window::WindowClock};
use std::time::Duration;

const DAY: i64 = 86_400_000;

#[test]
fn test_window_boundaries() {
    let c = WindowClock::new(Duration::from_secs(86_400), Duration::ZERO).unwrap();
    assert_eq!(c.start_of(DAY - 1), 0);
    assert_eq!(c.start_of(DAY), DAY); // ts == end → next window
    assert_eq!(c.start_of(-1), -DAY); // pre-epoch floors
    assert_eq!(c.end_of(0), DAY);
    let off =
        WindowClock::new(Duration::from_secs(86_400), Duration::from_secs(13 * 3600)).unwrap();
    assert_eq!(off.start_of(13 * 3_600_000), 13 * 3_600_000);
    assert_eq!(off.start_of(13 * 3_600_000 - 1), 13 * 3_600_000 - DAY);
    assert!(WindowClock::new(Duration::ZERO, Duration::ZERO).is_err());
}

#[test]
fn key_layout_and_overflow() {
    let k = StorageKey24 {
        epoch_ms: 1_760_000_000_000,
        digest: 0x0123_4567_89ab_cdef_0011_2233_4455_6677,
    };
    let b = k.to_bytes();
    assert_eq!(&b[..8], &1_760_000_000_000u64.to_be_bytes());
    assert_eq!(StorageKey24::from_bytes(&b).unwrap(), k);
    assert!(StorageKey24::from_bytes(&b[..23]).is_err());
    let o = StorageKey24::overflow(1_760_000_000_000, 42);
    assert!(o.is_overflow() && !k.is_overflow());
    assert_ne!(StorageKey24::overflow(1, 42), StorageKey24::overflow(1, 43));
    let (k1, k2) = (
        StorageKey24 {
            epoch_ms: 1,
            digest: u128::MAX,
        },
        StorageKey24 {
            epoch_ms: 2,
            digest: 0,
        },
    );
    assert!(k1.to_bytes() < k2.to_bytes()); // byte order == (epoch, digest) order
}

#[test]
fn test_missing_dim_differs_from_empty() {
    let kb = KeyBuilder::new(7);
    let a = kb.digest(1, 9, None, &[None]);
    let b = kb.digest(1, 9, None, &[Some(b"")]);
    assert_ne!(a, b);
}

#[test]
fn dims_are_length_prefixed_and_salted() {
    let kb = KeyBuilder::new(7);
    assert_ne!(
        kb.digest(1, 9, None, &[Some(b"ab"), Some(b"c")]),
        kb.digest(1, 9, None, &[Some(b"a"), Some(b"bc")])
    );
    assert_ne!(
        kb.digest(1, 9, Some(b"x"), &[]),
        KeyBuilder::new(8).digest(1, 9, Some(b"x"), &[])
    );
    assert_eq!(
        kb.digest(1, 9, Some(b"x"), &[]),
        KeyBuilder::new(7).digest(1, 9, Some(b"x"), &[])
    );
}
