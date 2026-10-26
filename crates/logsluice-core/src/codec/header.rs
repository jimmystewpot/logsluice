//! Fixed-width numeric aggregation header codec.

use crate::codec::cursor::Cursor;
use crate::codec::plan::HeaderLayout;
use crate::error::{AggError, AggResult};

/// Fixed-width header holding numeric counters and min/max states for an aggregate key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NumericHeader {
    /// Shard-local interned identifier for resource and scope attributes.
    pub resource_scope_id: u32,
    /// Number of slices acknowledged for this key; used in deterministic `slice_id`.
    pub slice_seq: u32,
    /// Seconds since window start when this key was last updated.
    pub last_update_s: u32,
    /// Operational flags (overflow, evicted-and-recreated, etc.).
    pub flags: u32,
    /// Monotonic total counters per bank (each bank has 4 x u64 lanes).
    pub total: Vec<[u64; 4]>,
    /// Monotonic emitted counters per bank (each bank has 4 x u64 lanes).
    pub emitted: Vec<[u64; 4]>,
    /// Running min and max values per slot `(min: i64, max: i64)`.
    pub minmax: Vec<(i64, i64)>,
}

impl NumericHeader {
    /// Creates a new zeroed `NumericHeader` matching `layout` for the given resource scope.
    #[must_use]
    pub fn new_zeroed(layout: HeaderLayout, resource_scope_id: u32) -> Self {
        Self {
            resource_scope_id,
            slice_seq: 0,
            last_update_s: 0,
            flags: 0,
            total: vec![[0u64; 4]; layout.banks],
            emitted: vec![[0u64; 4]; layout.banks],
            minmax: vec![(0i64, 0i64); layout.minmax_slots],
        }
    }

    /// Serializes the numeric header into a byte buffer according to `layout`.
    #[must_use]
    pub fn encode(&self, layout: HeaderLayout) -> Vec<u8> {
        let expected_len = layout.encoded_len();
        let mut buf = Vec::with_capacity(expected_len);

        buf.extend_from_slice(&self.resource_scope_id.to_le_bytes());
        buf.extend_from_slice(&self.slice_seq.to_le_bytes());
        buf.extend_from_slice(&self.last_update_s.to_le_bytes());
        buf.extend_from_slice(&self.flags.to_le_bytes());

        for bank_idx in 0..layout.banks {
            let bank = self.total.get(bank_idx).copied().unwrap_or([0u64; 4]);
            for lane in bank {
                buf.extend_from_slice(&lane.to_le_bytes());
            }
        }

        for bank_idx in 0..layout.banks {
            let bank = self.emitted.get(bank_idx).copied().unwrap_or([0u64; 4]);
            for lane in bank {
                buf.extend_from_slice(&lane.to_le_bytes());
            }
        }

        for slot_idx in 0..layout.minmax_slots {
            let (min_val, max_val) = self.minmax.get(slot_idx).copied().unwrap_or((0i64, 0i64));
            buf.extend_from_slice(&min_val.to_le_bytes());
            buf.extend_from_slice(&max_val.to_le_bytes());
        }

        buf
    }

    /// Deserializes a numeric header from `bytes` according to `layout`.
    ///
    /// # Errors
    /// Returns `AggError::Codec` if `bytes.len() != layout.encoded_len()`.
    pub fn decode(layout: HeaderLayout, bytes: &[u8]) -> AggResult<Self> {
        let expected_len = layout.encoded_len();
        if bytes.len() != expected_len {
            return Err(AggError::Codec(format!(
                "invalid NumericHeader length: expected {expected_len} bytes, got {}",
                bytes.len()
            )));
        }

        let mut cur = Cursor::new(bytes);

        let resource_scope_id = cur.read_u32()?;
        let slice_seq = cur.read_u32()?;
        let last_update_s = cur.read_u32()?;
        let flags = cur.read_u32()?;

        let mut total = Vec::with_capacity(layout.banks);
        for _ in 0..layout.banks {
            total.push(cur.read_bank()?);
        }

        let mut emitted = Vec::with_capacity(layout.banks);
        for _ in 0..layout.banks {
            emitted.push(cur.read_bank()?);
        }

        let mut minmax = Vec::with_capacity(layout.minmax_slots);
        for _ in 0..layout.minmax_slots {
            minmax.push(cur.read_minmax()?);
        }

        Ok(Self {
            resource_scope_id,
            slice_seq,
            last_update_s,
            flags,
            total,
            emitted,
            minmax,
        })
    }
}
