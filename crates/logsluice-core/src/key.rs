//! 24-byte storage key definitions, overflow representation, and salted key builder.

use crate::error::{AggError, AggResult};

/// Top 96-bit mask used to identify overflow keys.
const OVERFLOW_MASK_TOP_96: u128 = !0u128 << 32;

/// A 24-byte composite key representing a window bucket and group digest.
///
/// In storage (e.g. Fjall LSM tree), keys are ordered lexicographically by:
/// 1. `epoch_ms` (big-endian 8 bytes)
/// 2. `digest` (big-endian 16 bytes)
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StorageKey24 {
    /// Window bucket start timestamp in epoch milliseconds.
    pub epoch_ms: u64,
    /// 128-bit hash digest of the grouping dimensions or overflow sentinel.
    pub digest: u128,
}

impl StorageKey24 {
    /// Serializes the key into a 24-byte big-endian array.
    #[inline]
    #[must_use]
    pub const fn to_bytes(&self) -> [u8; 24] {
        let epoch_bytes = self.epoch_ms.to_be_bytes();
        let digest_bytes = self.digest.to_be_bytes();

        let mut bytes = [0u8; 24];
        let mut i = 0;
        while i < 8 {
            bytes[i] = epoch_bytes[i];
            i += 1;
        }
        while i < 24 {
            bytes[i] = digest_bytes[i - 8];
            i += 1;
        }
        bytes
    }

    /// Deserializes a `StorageKey24` from a byte slice.
    ///
    /// # Errors
    /// Returns `AggError::Codec` if `bytes.len() != 24`.
    pub fn from_bytes(bytes: &[u8]) -> AggResult<Self> {
        if bytes.len() != 24 {
            return Err(AggError::Codec(format!(
                "invalid StorageKey24 length: expected 24 bytes, got {}",
                bytes.len()
            )));
        }

        let mut epoch_bytes = [0u8; 8];
        epoch_bytes.copy_from_slice(&bytes[..8]);

        let mut digest_bytes = [0u8; 16];
        digest_bytes.copy_from_slice(&bytes[8..24]);

        Ok(Self {
            epoch_ms: u64::from_be_bytes(epoch_bytes),
            digest: u128::from_be_bytes(digest_bytes),
        })
    }

    /// Creates an overflow sentinel key for a specific resource scope within a window.
    ///
    /// The top 96 bits of `digest` are all set to 1, and the lower 32 bits hold `resource_scope_id`.
    #[inline]
    #[must_use]
    pub const fn overflow(epoch_ms: u64, resource_scope_id: u32) -> Self {
        Self {
            epoch_ms,
            digest: OVERFLOW_MASK_TOP_96 | (resource_scope_id as u128),
        }
    }

    /// Checks if this key is an overflow sentinel key (top 96 bits all set to 1).
    #[inline]
    #[must_use]
    pub const fn is_overflow(&self) -> bool {
        (self.digest & OVERFLOW_MASK_TOP_96) == OVERFLOW_MASK_TOP_96
    }

    /// Returns the resource scope ID if this key is an overflow sentinel.
    #[inline]
    #[must_use]
    pub const fn overflow_resource_scope_id(&self) -> Option<u32> {
        if self.is_overflow() {
            Some(self.digest as u32)
        } else {
            None
        }
    }
}

/// Helper function to encode an optional byte field into an `xxhash_rust::xxh3::Xxh3` stream.
#[inline]
fn encode_field(hasher: &mut xxhash_rust::xxh3::Xxh3, field: Option<&[u8]>) {
    match field {
        None => {
            hasher.update(&[0u8]);
        }
        Some(bytes) => {
            hasher.update(&[1u8]);
            let len = bytes.len() as u32;
            hasher.update(&len.to_le_bytes());
            hasher.update(bytes);
        }
    }
}

/// Salted hasher for producing 128-bit digests across log attributes and dimensions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyBuilder {
    salt: u64,
}

impl KeyBuilder {
    /// Creates a new `KeyBuilder` seeded with `salt`.
    #[inline]
    #[must_use]
    pub const fn new(salt: u64) -> Self {
        Self { salt }
    }

    /// Returns the salt used by this `KeyBuilder`.
    #[inline]
    #[must_use]
    pub const fn salt(&self) -> u64 {
        self.salt
    }

    /// Computes the 128-bit XXH3 digest for the given event dimensions.
    ///
    /// The input stream is length-prefixed and unambiguously separated:
    /// 1. `resource_scope_hash` as little-endian bytes (`u64`);
    /// 2. `severity` byte (`u8`);
    /// 3. `body` encoded with a presence tag (0 = absent, 1 = present), `len: u32` LE, then payload bytes;
    /// 4. Each dimension in `dims` encoded identically to `body`.
    #[must_use]
    pub fn digest(
        &self,
        resource_scope_hash: u64,
        severity: u8,
        body: Option<&[u8]>,
        dims: &[Option<&[u8]>],
    ) -> u128 {
        self.digest_dims(resource_scope_hash, severity, body, dims)
    }

    /// [`KeyBuilder::digest`] over any byte-like dimension slice.
    ///
    /// Lets callers holding `&[Option<Bytes>]` hash in place instead of
    /// collecting a temporary `Vec<Option<&[u8]>>` per event. The encoding is
    /// identical to [`KeyBuilder::digest`].
    #[inline]
    #[must_use]
    pub fn digest_dims<D: AsRef<[u8]>>(
        &self,
        resource_scope_hash: u64,
        severity: u8,
        body: Option<&[u8]>,
        dims: &[Option<D>],
    ) -> u128 {
        let mut hasher = xxhash_rust::xxh3::Xxh3::with_seed(self.salt);
        hasher.update(&resource_scope_hash.to_le_bytes());
        hasher.update(&[severity]);

        encode_field(&mut hasher, body);
        for dim in dims {
            encode_field(&mut hasher, dim.as_ref().map(AsRef::as_ref));
        }

        hasher.digest128()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_key_roundtrip_and_errors() {
        let key = StorageKey24 {
            epoch_ms: 123_456_789,
            digest: 0xfeed_cafe_dead_beef_0123_4567_89ab_cdef,
        };
        let bytes = key.to_bytes();
        let decoded = StorageKey24::from_bytes(&bytes).unwrap();
        assert_eq!(key, decoded);

        assert!(StorageKey24::from_bytes(&bytes[..23]).is_err());
        let mut extended = bytes.to_vec();
        extended.push(0);
        assert!(StorageKey24::from_bytes(&extended).is_err());
    }

    #[test]
    fn test_overflow_key_properties() {
        let epoch = 1_000_000;
        let scope_id = 999;
        let ovf = StorageKey24::overflow(epoch, scope_id);

        assert!(ovf.is_overflow());
        assert_eq!(ovf.overflow_resource_scope_id(), Some(scope_id));

        let non_ovf = StorageKey24 {
            epoch_ms: epoch,
            digest: 999, // top bits are zero
        };
        assert!(!non_ovf.is_overflow());
        assert_eq!(non_ovf.overflow_resource_scope_id(), None);
    }

    #[test]
    fn test_key_builder_salt_and_encoding() {
        let kb1 = KeyBuilder::new(100);
        assert_eq!(kb1.salt(), 100);

        let d1 = kb1.digest(10, 1, Some(b"msg"), &[Some(b"host1")]);
        let d2 = kb1.digest(10, 1, Some(b"msg"), &[Some(b"host1")]);
        assert_eq!(d1, d2);

        let kb2 = KeyBuilder::new(101);
        let d3 = kb2.digest(10, 1, Some(b"msg"), &[Some(b"host1")]);
        assert_ne!(d1, d3);
    }
}
