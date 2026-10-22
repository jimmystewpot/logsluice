//! AVX2-accelerated kernels for x86_64 architectures.

#![cfg(target_arch = "x86_64")]

use core::arch::x86_64::*;

#[target_feature(enable = "avx2")]
unsafe fn accumulate_counter_banks_avx2_inner(dst: &mut [[u64; 4]], src: &[[u64; 4]]) {
    let len = dst.len().min(src.len());
    for i in 0..len {
        // SAFETY: `dst` and `src` have length at least `len`, so index `i` is in bounds.
        // `dst[i]` and `src[i]` are 32 bytes (256-bit). `_mm256_loadu_si256` loads unaligned 256-bit vectors.
        // `_mm256_add_epi64` executes wrapping 64-bit addition for 4 lanes.
        // `_mm256_storeu_si256` writes back to `dst[i]`.
        unsafe {
            let d_ptr = dst[i].as_mut_ptr().cast::<__m256i>();
            let s_ptr = src[i].as_ptr().cast::<__m256i>();
            let vd = _mm256_loadu_si256(d_ptr);
            let vs = _mm256_loadu_si256(s_ptr);
            let res = _mm256_add_epi64(vd, vs);
            _mm256_storeu_si256(d_ptr, res);
        }
    }
}

/// Element-wise wrapping addition of counter banks using 256-bit AVX2 registers.
pub(crate) fn accumulate_counter_banks(dst: &mut [[u64; 4]], src: &[[u64; 4]]) {
    // SAFETY: This kernel table entry is only dispatched after runtime detection of AVX2.
    unsafe { accumulate_counter_banks_avx2_inner(dst, src) }
}

#[target_feature(enable = "avx2")]
unsafe fn diff_counter_banks_avx2_inner(
    a: &[[u64; 4]],
    b: &[[u64; 4]],
    dst: &mut [[u64; 4]],
) -> bool {
    let len = a.len().min(b.len()).min(dst.len());
    let mut acc_or = _mm256_setzero_si256();
    for i in 0..len {
        // SAFETY: `a`, `b`, and `dst` have length at least `len`, so index `i` is in bounds.
        // `_mm256_loadu_si256` loads unaligned 256-bit vectors.
        // `_mm256_sub_epi64` executes wrapping 64-bit subtraction for 4 lanes.
        // `_mm256_storeu_si256` writes back to `dst[i]`.
        // `_mm256_or_si256` accumulates any non-zero bits.
        unsafe {
            let a_ptr = a[i].as_ptr().cast::<__m256i>();
            let b_ptr = b[i].as_ptr().cast::<__m256i>();
            let d_ptr = dst[i].as_mut_ptr().cast::<__m256i>();
            let va = _mm256_loadu_si256(a_ptr);
            let vb = _mm256_loadu_si256(b_ptr);
            let diff = _mm256_sub_epi64(va, vb);
            _mm256_storeu_si256(d_ptr, diff);
            acc_or = _mm256_or_si256(acc_or, diff);
        }
    }
    // `_mm256_testz_si256(acc_or, acc_or)` tests if all bits in `acc_or` are 0.
    // It returns 1 if zero, or 0 if non-zero.
    _mm256_testz_si256(acc_or, acc_or) == 0
}

/// Subtracts `b` from `a` element-wise into `dst`, returning `true` if any lane is non-zero.
pub(crate) fn diff_counter_banks(a: &[[u64; 4]], b: &[[u64; 4]], dst: &mut [[u64; 4]]) -> bool {
    // SAFETY: This kernel table entry is only dispatched after runtime detection of AVX2.
    unsafe { diff_counter_banks_avx2_inner(a, b, dst) }
}

#[target_feature(enable = "avx2")]
unsafe fn mask_ip_cidr_avx2_inner(ip: [u8; 16], prefix: u8) -> [u8; 16] {
    let prefix = (prefix as usize).min(128);
    let mut out = [0u8; 16];
    // SAFETY: `prefix` is clamped to <= 128, so `CIDR_MASKS[prefix]` is in bounds.
    // `_mm_loadu_si128` loads 128-bit (16-byte) vectors unaligned.
    // `_mm_and_si128` performs bitwise AND between the IP and mask.
    // `_mm_storeu_si128` stores 16 bytes into `out`.
    unsafe {
        let mask_ptr = crate::simd::CIDR_MASKS[prefix].as_ptr().cast::<__m128i>();
        let ip_ptr = ip.as_ptr().cast::<__m128i>();
        let v_ip = _mm_loadu_si128(ip_ptr);
        let v_mask = _mm_loadu_si128(mask_ptr);
        let v_res = _mm_and_si128(v_ip, v_mask);
        _mm_storeu_si128(out.as_mut_ptr().cast::<__m128i>(), v_res);
    }
    out
}

/// Masks an IP address with a CIDR prefix using SSE/AVX bitwise operations.
pub(crate) fn mask_ip_cidr(ip: [u8; 16], prefix: u8) -> [u8; 16] {
    // SAFETY: This kernel table entry is only dispatched after runtime detection of AVX2.
    unsafe { mask_ip_cidr_avx2_inner(ip, prefix) }
}

#[target_feature(enable = "avx2")]
unsafe fn chunk_contains_digit_or_token_marker(chunk: &[u8; 32]) -> bool {
    // SAFETY: `chunk` is a 32-byte array.
    // Loads 256 bits unaligned.
    // Checks if any byte is in '0'..='9' (ASCII 48..57) or '-' (ASCII 45).
    unsafe {
        let v = _mm256_loadu_si256(chunk.as_ptr().cast::<__m256i>());
        let gt_47 = _mm256_cmpgt_epi8(v, _mm256_set1_epi8(47));
        let lt_58 = _mm256_cmpgt_epi8(_mm256_set1_epi8(58), v);
        let is_digit = _mm256_and_si256(gt_47, lt_58);
        let is_dash = _mm256_cmpeq_epi8(v, _mm256_set1_epi8(b'-' as i8));
        let marker = _mm256_or_si256(is_digit, is_dash);
        _mm256_movemask_epi8(marker) != 0
    }
}

#[target_feature(enable = "avx2")]
unsafe fn fingerprint_log_bytes_avx2_inner(input: &[u8], out: &mut Vec<u8>) {
    // `fingerprint_scan` is `#[inline(always)]` into this AVX2 function, so the
    // closure inherits `+avx2` and receives `&[u8; 32]` directly.
    crate::simd::scalar::fingerprint_scan::<32>(input, out, |chunk| {
        // SAFETY: AVX2 is enabled on this function. This closure inherits that
        // feature and only runs while the AVX2 kernel is executing.
        unsafe { chunk_contains_digit_or_token_marker(chunk) }
    });
}

/// Fingerprints log bytes using AVX2-accelerated chunk scanning with scalar fallback.
pub(crate) fn fingerprint_log_bytes(input: &[u8], out: &mut Vec<u8>) {
    // SAFETY: This kernel table entry is only dispatched after runtime detection of AVX2.
    unsafe { fingerprint_log_bytes_avx2_inner(input, out) }
}
