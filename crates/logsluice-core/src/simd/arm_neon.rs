//! ARM NEON-accelerated kernels for aarch64 architectures.

#![cfg(target_arch = "aarch64")]

use core::arch::aarch64::*;

#[target_feature(enable = "neon")]
unsafe fn accumulate_counter_banks_neon_inner(dst: &mut [[u64; 4]], src: &[[u64; 4]]) {
    let len = dst.len().min(src.len());
    for i in 0..len {
        // SAFETY: `dst` and `src` have length at least `len`, so index `i` is in bounds.
        // `dst[i]` and `src[i]` have 4 u64s = two 128-bit vectors.
        // `vld1q_u64` loads 2 u64s. `vaddq_u64` executes wrapping addition. `vst1q_u64` writes 2 u64s.
        unsafe {
            let d_ptr = dst[i].as_mut_ptr();
            let s_ptr = src[i].as_ptr();

            let vd0 = vld1q_u64(d_ptr);
            let vs0 = vld1q_u64(s_ptr);
            let r0 = vaddq_u64(vd0, vs0);
            vst1q_u64(d_ptr, r0);

            let vd1 = vld1q_u64(d_ptr.add(2));
            let vs1 = vld1q_u64(s_ptr.add(2));
            let r1 = vaddq_u64(vd1, vs1);
            vst1q_u64(d_ptr.add(2), r1);
        }
    }
}

/// Element-wise wrapping addition of counter banks using 128-bit ARM NEON vectors.
pub(crate) fn accumulate_counter_banks(dst: &mut [[u64; 4]], src: &[[u64; 4]]) {
    // SAFETY: This kernel table entry is only dispatched after runtime detection of NEON.
    unsafe { accumulate_counter_banks_neon_inner(dst, src) }
}

#[target_feature(enable = "neon")]
unsafe fn diff_counter_banks_neon_inner(
    a: &[[u64; 4]],
    b: &[[u64; 4]],
    dst: &mut [[u64; 4]],
) -> bool {
    let len = a.len().min(b.len()).min(dst.len());
    let (mut acc_or0, mut acc_or1) = (vdupq_n_u64(0), vdupq_n_u64(0));
    for i in 0..len {
        // SAFETY: `a`, `b`, and `dst` have length at least `len`, so index `i` is in bounds.
        // `vld1q_u64` loads 2 u64s. `vsubq_u64` executes wrapping subtraction. `vst1q_u64` writes 2 u64s.
        // `vorrq_u64` accumulates non-zero bits.
        unsafe {
            let a_ptr = a[i].as_ptr();
            let b_ptr = b[i].as_ptr();
            let d_ptr = dst[i].as_mut_ptr();

            let va0 = vld1q_u64(a_ptr);
            let vb0 = vld1q_u64(b_ptr);
            let diff0 = vsubq_u64(va0, vb0);
            vst1q_u64(d_ptr, diff0);
            acc_or0 = vorrq_u64(acc_or0, diff0);

            let va1 = vld1q_u64(a_ptr.add(2));
            let vb1 = vld1q_u64(b_ptr.add(2));
            let diff1 = vsubq_u64(va1, vb1);
            vst1q_u64(d_ptr.add(2), diff1);
            acc_or1 = vorrq_u64(acc_or1, diff1);
        }
    }
    let combined = vorrq_u64(acc_or0, acc_or1);
    let low = vgetq_lane_u64::<0>(combined);
    let high = vgetq_lane_u64::<1>(combined);
    (low | high) != 0
}

/// Subtracts `b` from `a` element-wise into `dst`, returning `true` if any lane is non-zero.
pub(crate) fn diff_counter_banks(a: &[[u64; 4]], b: &[[u64; 4]], dst: &mut [[u64; 4]]) -> bool {
    // SAFETY: This kernel table entry is only dispatched after runtime detection of NEON.
    unsafe { diff_counter_banks_neon_inner(a, b, dst) }
}

#[target_feature(enable = "neon")]
unsafe fn mask_ip_cidr_neon_inner(ip: [u8; 16], prefix: u8) -> [u8; 16] {
    let prefix = (prefix as usize).min(128);
    let mut out = [0u8; 16];
    // SAFETY: `prefix` is clamped to <= 128, so `CIDR_MASKS[prefix]` is in bounds.
    // `vld1q_u8` loads 16 bytes. `vandq_u8` performs bitwise AND. `vst1q_u8` stores 16 bytes.
    unsafe {
        let mask_ptr = crate::simd::CIDR_MASKS[prefix].as_ptr();
        let ip_ptr = ip.as_ptr();
        let v_ip = vld1q_u8(ip_ptr);
        let v_mask = vld1q_u8(mask_ptr);
        let v_res = vandq_u8(v_ip, v_mask);
        vst1q_u8(out.as_mut_ptr(), v_res);
    }
    out
}

/// Masks an IP address with a CIDR prefix using NEON bitwise operations.
pub(crate) fn mask_ip_cidr(ip: [u8; 16], prefix: u8) -> [u8; 16] {
    // SAFETY: This kernel table entry is only dispatched after runtime detection of NEON.
    unsafe { mask_ip_cidr_neon_inner(ip, prefix) }
}

#[target_feature(enable = "neon")]
unsafe fn chunk_contains_digit_or_token_marker_neon(chunk: &[u8; 16]) -> bool {
    // SAFETY: `chunk` has 16 bytes.
    // Loads 16 bytes and tests if any byte is in '0'..='9' or '-'.
    unsafe {
        let v = vld1q_u8(chunk.as_ptr());
        let ge_0 = vcgeq_u8(v, vdupq_n_u8(b'0'));
        let le_9 = vcleq_u8(v, vdupq_n_u8(b'9'));
        let is_digit = vandq_u8(ge_0, le_9);
        let is_dash = vceqq_u8(v, vdupq_n_u8(b'-'));
        let marker = vorrq_u8(is_digit, is_dash);
        let max_val = vmaxvq_u8(marker);
        max_val != 0
    }
}

#[target_feature(enable = "neon")]
unsafe fn fingerprint_log_bytes_neon_inner(input: &[u8], out: &mut Vec<u8>) {
    crate::simd::scalar::fingerprint_scan(input, out, 16, |chunk| {
        let chunk: &[u8; 16] = match chunk.try_into() {
            Ok(c) => c,
            // Wrong width: treat the chunk as marked so the scalar tokenizer
            // consumes it. `fingerprint_scan` only passes `width` bytes.
            Err(_) => return true,
        };
        // SAFETY: NEON is enabled on this function. This closure inherits that
        // feature and only runs while the NEON kernel is executing.
        unsafe { chunk_contains_digit_or_token_marker_neon(chunk) }
    });
}

/// Fingerprints log bytes using NEON-accelerated chunk scanning with scalar fallback.
pub(crate) fn fingerprint_log_bytes(input: &[u8], out: &mut Vec<u8>) {
    // SAFETY: This kernel table entry is only dispatched after runtime detection of NEON.
    unsafe { fingerprint_log_bytes_neon_inner(input, out) }
}
