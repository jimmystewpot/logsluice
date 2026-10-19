//! SIMD and scalar acceleration kernel table and runtime dispatch.

use serde::{Deserialize, Serialize};

mod arm_neon;
mod scalar;
mod x86_avx2;

/// Precomputed 128-bit bitmasks for IPv6/IPv4-mapped CIDR prefixes (0..=128).
pub(crate) const CIDR_MASKS: [[u8; 16]; 129] = {
    let mut table = [[0u8; 16]; 129];
    let mut prefix = 0;
    while prefix <= 128 {
        let full_bytes = prefix / 8;
        let rem_bits = prefix % 8;
        let mut i = 0;
        while i < full_bytes {
            table[prefix][i] = 0xFF;
            i += 1;
        }
        if full_bytes < 16 && rem_bits > 0 {
            table[prefix][full_bytes] = 0xFFu8 << (8 - rem_bits);
        }
        prefix += 1;
    }
    table
};

/// Desired acceleration mode for windowed aggregate operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SimdMode {
    /// Detect CPU features at runtime and use the fastest available backend (AVX2, NEON, or Scalar).
    #[default]
    Auto,
    /// Force scalar implementation.
    Scalar,
}

/// Function pointer type for counter bank accumulation.
pub type AccumulateCounterBanksFn = fn(&mut [[u64; 4]], &[[u64; 4]]);

/// Function pointer type for counter bank difference.
pub type DiffCounterBanksFn = fn(&[[u64; 4]], &[[u64; 4]], &mut [[u64; 4]]) -> bool;

/// Function pointer type for updating min/max slots.
pub type UpdateMinmaxSlotsFn = fn(&mut [(i64, i64)], &[i64]);

/// Function pointer type for CIDR masking.
pub type MaskIpCidrFn = fn([u8; 16], u8) -> [u8; 16];

/// Function pointer type for log fingerprinting.
pub type FingerprintLogBytesFn = fn(&[u8], &mut Vec<u8>);

/// Function pointer type for 128-bit key hashing.
pub type HashKey128Fn = fn(&[u8], u64) -> u128;

/// Function pointer table of vector acceleration kernels.
#[derive(Clone, Copy, Debug)]
pub struct SimdKernelTable {
    /// Element-wise wrapping addition of 4-wide u64 counter banks.
    pub accumulate_counter_banks: AccumulateCounterBanksFn,
    /// Subtracts `b` from `a` into `dst`, returning `true` if any resulting lane is non-zero.
    pub diff_counter_banks: DiffCounterBanksFn,
    /// Updates min/max slot pairs with incoming values.
    pub update_minmax_slots: UpdateMinmaxSlotsFn,
    /// Masks an IP address keeping the first `prefix` (0..=128) bits.
    pub mask_ip_cidr: MaskIpCidrFn,
    /// Scans log bytes and replaces tokens (UUID, IPv4, hex, number) with placeholders.
    pub fingerprint_log_bytes: FingerprintLogBytesFn,
    /// Hashes input bytes with 64-bit seed using XXH3-128.
    pub hash_key_128: HashKey128Fn,
    /// Human-readable identifier for the active backend ("scalar", "avx2", or "neon").
    pub backend_name: &'static str,
}

impl SimdKernelTable {
    /// Detects CPU features and builds the appropriate kernel table for the requested mode.
    #[must_use]
    pub fn detect(mode: SimdMode) -> Self {
        match mode {
            SimdMode::Scalar => Self::scalar(),
            SimdMode::Auto => {
                #[cfg(target_arch = "x86_64")]
                {
                    if is_x86_feature_detected!("avx2") {
                        return Self {
                            accumulate_counter_banks: x86_avx2::accumulate_counter_banks,
                            diff_counter_banks: x86_avx2::diff_counter_banks,
                            update_minmax_slots: scalar::update_minmax_slots_scalar,
                            mask_ip_cidr: x86_avx2::mask_ip_cidr,
                            fingerprint_log_bytes: x86_avx2::fingerprint_log_bytes,
                            hash_key_128: scalar::hash_key_128_scalar,
                            backend_name: "avx2",
                        };
                    }
                }
                #[cfg(target_arch = "aarch64")]
                {
                    if std::arch::is_aarch64_feature_detected!("neon") {
                        return Self {
                            accumulate_counter_banks: arm_neon::accumulate_counter_banks,
                            diff_counter_banks: arm_neon::diff_counter_banks,
                            update_minmax_slots: scalar::update_minmax_slots_scalar,
                            mask_ip_cidr: arm_neon::mask_ip_cidr,
                            fingerprint_log_bytes: arm_neon::fingerprint_log_bytes,
                            hash_key_128: scalar::hash_key_128_scalar,
                            backend_name: "neon",
                        };
                    }
                }
                Self::scalar()
            }
        }
    }

    /// Constructs the reference scalar kernel table.
    #[must_use]
    pub fn scalar() -> Self {
        Self {
            accumulate_counter_banks: scalar::accumulate_counter_banks_scalar,
            diff_counter_banks: scalar::diff_counter_banks_scalar,
            update_minmax_slots: scalar::update_minmax_slots_scalar,
            mask_ip_cidr: scalar::mask_ip_cidr_scalar,
            fingerprint_log_bytes: scalar::fingerprint_log_bytes_scalar,
            hash_key_128: scalar::hash_key_128_scalar,
            backend_name: "scalar",
        }
    }
}

impl Default for SimdKernelTable {
    fn default() -> Self {
        Self::detect(SimdMode::Auto)
    }
}
