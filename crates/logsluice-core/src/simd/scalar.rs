//! Scalar implementations of the acceleration kernels.
//!
//! These functions serve as the reference implementations for bit-exact parity testing.

#[inline]
fn is_boundary(b: u8) -> bool {
    !b.is_ascii_alphanumeric()
}

fn try_match_uuid(input: &[u8], pos: usize) -> Option<usize> {
    if pos + 36 > input.len() {
        return None;
    }
    let slice = &input[pos..pos + 36];
    // Pattern: 8 hex - 4 hex - 4 hex - 4 hex - 12 hex
    for &b in &slice[0..8] {
        if !b.is_ascii_hexdigit() {
            return None;
        }
    }
    if slice[8] != b'-' {
        return None;
    }
    for &b in &slice[9..13] {
        if !b.is_ascii_hexdigit() {
            return None;
        }
    }
    if slice[13] != b'-' {
        return None;
    }
    for &b in &slice[14..18] {
        if !b.is_ascii_hexdigit() {
            return None;
        }
    }
    if slice[18] != b'-' {
        return None;
    }
    for &b in &slice[19..23] {
        if !b.is_ascii_hexdigit() {
            return None;
        }
    }
    if slice[23] != b'-' {
        return None;
    }
    for &b in &slice[24..36] {
        if !b.is_ascii_hexdigit() {
            return None;
        }
    }
    // Check after boundary
    if pos + 36 < input.len() && !is_boundary(input[pos + 36]) {
        return None;
    }
    Some(36)
}

fn try_match_ipv4(input: &[u8], pos: usize) -> Option<usize> {
    let slice = &input[pos..];
    let mut offset = 0;
    for i in 0..4 {
        if i > 0 {
            if offset >= slice.len() || slice[offset] != b'.' {
                return None;
            }
            offset += 1;
        }
        let mut val: u32 = 0;
        let mut digit_count = 0;
        while offset < slice.len() && slice[offset].is_ascii_digit() {
            if digit_count < 3 {
                val = val * 10 + (slice[offset] - b'0') as u32;
            }
            digit_count += 1;
            offset += 1;
        }
        if !(1..=3).contains(&digit_count) || val > 255 {
            return None;
        }
    }
    // Check after boundary
    if pos + offset < input.len() && !is_boundary(input[pos + offset]) {
        return None;
    }
    Some(offset)
}

fn try_match_hex(input: &[u8], pos: usize) -> Option<usize> {
    let slice = &input[pos..];
    if slice.len() < 3 {
        return None;
    }
    if slice[0] != b'0' || (slice[1] != b'x' && slice[1] != b'X') {
        return None;
    }
    let mut offset = 2;
    while offset < slice.len() && slice[offset].is_ascii_hexdigit() {
        offset += 1;
    }
    if offset == 2 {
        return None; // No hex digits followed 0x/0X
    }
    // Check after boundary
    if pos + offset < input.len() && !is_boundary(input[pos + offset]) {
        return None;
    }
    Some(offset)
}

fn try_match_num(input: &[u8], pos: usize) -> Option<usize> {
    let slice = &input[pos..];
    let mut offset = 0;
    while offset < slice.len() && slice[offset].is_ascii_digit() {
        offset += 1;
    }
    if offset == 0 {
        return None;
    }
    // Check after boundary
    if pos + offset < input.len() && !is_boundary(input[pos + offset]) {
        return None;
    }
    Some(offset)
}

/// Advances scalar tokenizer by one step at position `pos`.
#[inline]
pub(crate) fn step_scalar(input: &[u8], pos: &mut usize, out: &mut Vec<u8>) {
    let p = *pos;
    if p >= input.len() {
        return;
    }
    if p > 0 && !is_boundary(input[p - 1]) {
        out.push(input[p]);
        *pos += 1;
        return;
    }

    if let Some(len) = try_match_uuid(input, p) {
        out.extend_from_slice(b"<UUID>");
        *pos += len;
        return;
    }

    if let Some(len) = try_match_ipv4(input, p) {
        out.extend_from_slice(b"<IP>");
        *pos += len;
        return;
    }

    if let Some(len) = try_match_hex(input, p) {
        out.extend_from_slice(b"<HEX>");
        *pos += len;
        return;
    }

    if let Some(len) = try_match_num(input, p) {
        out.extend_from_slice(b"<NUM>");
        *pos += len;
        return;
    }

    out.push(input[p]);
    *pos += 1;
}

/// Element-wise wrapping addition of counter banks.
pub(crate) fn accumulate_counter_banks_scalar(dst: &mut [[u64; 4]], src: &[[u64; 4]]) {
    let len = dst.len().min(src.len());
    for i in 0..len {
        for j in 0..4 {
            dst[i][j] = dst[i][j].wrapping_add(src[i][j]);
        }
    }
}

/// Subtracts `b` from `a` element-wise into `dst`, returning `true` if any lane is non-zero.
pub(crate) fn diff_counter_banks_scalar(
    a: &[[u64; 4]],
    b: &[[u64; 4]],
    dst: &mut [[u64; 4]],
) -> bool {
    let len = a.len().min(b.len()).min(dst.len());
    let mut any_non_zero = false;
    for i in 0..len {
        for j in 0..4 {
            let diff = a[i][j].wrapping_sub(b[i][j]);
            dst[i][j] = diff;
            if diff != 0 {
                any_non_zero = true;
            }
        }
    }
    any_non_zero
}

/// Updates min/max slot pairs with incoming values.
pub(crate) fn update_minmax_slots_scalar(slots: &mut [(i64, i64)], vals: &[i64]) {
    let len = slots.len().min(vals.len());
    for i in 0..len {
        slots[i].0 = slots[i].0.min(vals[i]);
        slots[i].1 = slots[i].1.max(vals[i]);
    }
}

/// Masks an IP address with a CIDR prefix (0..=128 bits preserved).
pub(crate) fn mask_ip_cidr_scalar(ip: [u8; 16], prefix: u8) -> [u8; 16] {
    let prefix = (prefix as usize).min(128);
    let mask = crate::simd::CIDR_MASKS[prefix];
    let mut out = [0u8; 16];
    for i in 0..16 {
        out[i] = ip[i] & mask[i];
    }
    out
}

/// Replaces UUIDs, IPv4 addresses, hex literals, and numbers with placeholder tokens.
pub(crate) fn fingerprint_log_bytes_scalar(input: &[u8], out: &mut Vec<u8>) {
    out.reserve(input.len());
    let mut pos = 0;
    while pos < input.len() {
        step_scalar(input, &mut pos, out);
    }
}

/// Scans log bytes in `WIDTH`-byte chunks, copying a chunk only when it cannot
/// start or continue a replaced token.
///
/// AVX2 uses 32 and NEON uses 16. `WIDTH` is a const so this loop inlines into
/// the `#[target_feature]` caller under `codegen-units = 16`. A runtime width
/// would compile the scan in this module's codegen unit, and LLVM will not
/// inline an AVX2 closure into that non-AVX2 function. The chunk stays
/// `[u8; WIDTH]`, so the caller does not `try_into` on every iteration.
///
/// AVX2 and NEON only prefilter chunks. Token replacement (UUID, IPv4, hex,
/// numbers) is scalar so the three backends stay bit-identical. The last byte
/// of a clean chunk is still checked with `is_ascii_hexdigit` because a hex run
/// can straddle the chunk boundary; skipping that check would let the vector
/// path disagree with scalar. `tests/simd_parity.rs` is the gate.
///
/// `chunk_has_marker` reports whether that chunk contains a digit or `'-'`.
#[inline(always)]
pub(crate) fn fingerprint_scan<const WIDTH: usize>(
    input: &[u8],
    out: &mut Vec<u8>,
    mut chunk_has_marker: impl FnMut(&[u8; WIDTH]) -> bool,
) {
    const { assert!(WIDTH > 0, "SIMD chunk width must be non-zero") };
    out.reserve(input.len());
    let mut pos = 0;
    while let Some(chunk) = input.get(pos..).and_then(<[u8]>::first_chunk::<WIDTH>) {
        if !chunk_has_marker(chunk) && !chunk[WIDTH - 1].is_ascii_hexdigit() {
            out.extend_from_slice(chunk);
            pos += WIDTH;
        } else {
            step_scalar(input, &mut pos, out);
        }
    }
    while pos < input.len() {
        step_scalar(input, &mut pos, out);
    }
}

/// Hashes input bytes with 64-bit seed using XXH3-128.
pub(crate) fn hash_key_128_scalar(bytes: &[u8], seed: u64) -> u128 {
    xxhash_rust::xxh3::xxh3_128_with_seed(bytes, seed)
}
