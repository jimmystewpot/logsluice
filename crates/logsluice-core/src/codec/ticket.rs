//! Emission ticket and ticket entry codecs.

use serde::{Deserialize, Serialize};

use crate::codec::cursor::Cursor;
use crate::codec::plan::HeaderLayout;
use crate::error::{AggError, AggResult};

/// Reason for emitting an aggregate slice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EmitReason {
    /// Window closed normally at or after watermark crossed end.
    Close,
    /// Periodic emission during active window lifetime.
    Periodic,
    /// Late events accepted after window closure triggered a revision slice.
    LateRevision,
    /// Cold aggregate key evicted to free cardinality budget.
    Evict,
    /// Graceful process shutdown flushed partial dirty state.
    Drain,
}

impl EmitReason {
    /// Returns the static snake_case string representation of the emit reason.
    #[inline]
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Close => "close",
            Self::Periodic => "periodic",
            Self::LateRevision => "late_revision",
            Self::Evict => "evict",
            Self::Drain => "drain",
        }
    }

    /// Converts the emit reason to a wire tag byte.
    #[inline]
    #[must_use]
    pub const fn to_u8(&self) -> u8 {
        match self {
            Self::Close => 0,
            Self::Periodic => 1,
            Self::LateRevision => 2,
            Self::Evict => 3,
            Self::Drain => 4,
        }
    }

    /// Parses an emit reason from a wire tag byte.
    ///
    /// # Errors
    /// Returns `AggError::Codec` if `val > 4`.
    pub fn from_u8(val: u8) -> AggResult<Self> {
        match val {
            0 => Ok(Self::Close),
            1 => Ok(Self::Periodic),
            2 => Ok(Self::LateRevision),
            3 => Ok(Self::Evict),
            4 => Ok(Self::Drain),
            other => Err(AggError::Codec(format!("unknown EmitReason tag: {other}"))),
        }
    }
}

/// A single key delta snapshot within an emission ticket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TicketEntry {
    /// 128-bit grouping digest.
    pub digest: u128,
    /// Current slice sequence number for this key.
    pub slice_seq: u32,
    /// Unacknowledged delta counters per bank since last emission.
    pub delta: Vec<[u64; 4]>,
    /// Running min/max snapshot per slot.
    pub minmax: Vec<(i64, i64)>,
}

/// Emission intent ticket persisting pending delivery records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ticket {
    /// Monotonic ticket ID.
    pub id: u64,
    /// Window bucket start timestamp in epoch milliseconds.
    pub window_epoch_ms: u64,
    /// Reason triggering this emission.
    pub reason: EmitReason,
    /// Entries to be rendered and emitted.
    pub entries: Vec<TicketEntry>,
}

impl Ticket {
    /// Serializes the ticket into a byte buffer according to `layout`.
    #[must_use]
    pub fn encode(&self, layout: HeaderLayout) -> Vec<u8> {
        let entry_len = 16 + 4 + layout.banks * 32 + layout.minmax_slots * 16;
        let total_capacity = 8 + 8 + 1 + 4 + self.entries.len() * entry_len;
        let mut buf = Vec::with_capacity(total_capacity);

        buf.extend_from_slice(&self.id.to_le_bytes());
        buf.extend_from_slice(&self.window_epoch_ms.to_le_bytes());
        buf.push(self.reason.to_u8());
        let entries_count = u32::try_from(self.entries.len()).unwrap_or(u32::MAX);
        buf.extend_from_slice(&entries_count.to_le_bytes());

        for entry in &self.entries {
            buf.extend_from_slice(&entry.digest.to_le_bytes());
            buf.extend_from_slice(&entry.slice_seq.to_le_bytes());

            for bank_idx in 0..layout.banks {
                let bank = entry.delta.get(bank_idx).copied().unwrap_or([0u64; 4]);
                for lane in bank {
                    buf.extend_from_slice(&lane.to_le_bytes());
                }
            }

            for slot_idx in 0..layout.minmax_slots {
                let (min_val, max_val) =
                    entry.minmax.get(slot_idx).copied().unwrap_or((0i64, 0i64));
                buf.extend_from_slice(&min_val.to_le_bytes());
                buf.extend_from_slice(&max_val.to_le_bytes());
            }
        }

        buf
    }

    /// Deserializes a ticket from `bytes` according to `layout`.
    ///
    /// # Errors
    /// Returns `AggError::Codec` if lengths or tag bytes are invalid.
    pub fn decode(layout: HeaderLayout, bytes: &[u8]) -> AggResult<Self> {
        if bytes.len() < 21 {
            return Err(AggError::Codec(format!(
                "invalid Ticket length: expected at least 21 bytes, got {}",
                bytes.len()
            )));
        }

        let entry_len = 16 + 4 + layout.banks * 32 + layout.minmax_slots * 16;
        let mut cur = Cursor::new(bytes);

        let id = cur.read_u64()?;
        let window_epoch_ms = cur.read_u64()?;
        let reason_tag = cur.read_u8()?;
        let reason = EmitReason::from_u8(reason_tag)?;
        let entries_count = cur.read_u32()? as usize;

        let expected_total_len = 21 + entries_count * entry_len;
        if bytes.len() != expected_total_len {
            return Err(AggError::Codec(format!(
                "invalid Ticket length: expected {expected_total_len} bytes for {entries_count} entries, got {}",
                bytes.len()
            )));
        }

        let mut entries = Vec::with_capacity(entries_count);
        for _ in 0..entries_count {
            let digest = cur.read_u128()?;
            let slice_seq = cur.read_u32()?;

            let mut delta = Vec::with_capacity(layout.banks);
            for _ in 0..layout.banks {
                let l0 = cur.read_u64()?;
                let l1 = cur.read_u64()?;
                let l2 = cur.read_u64()?;
                let l3 = cur.read_u64()?;
                delta.push([l0, l1, l2, l3]);
            }

            let mut minmax = Vec::with_capacity(layout.minmax_slots);
            for _ in 0..layout.minmax_slots {
                let min_val = cur.read_i64()?;
                let max_val = cur.read_i64()?;
                minmax.push((min_val, max_val));
            }

            entries.push(TicketEntry {
                digest,
                slice_seq,
                delta,
                minmax,
            });
        }

        Ok(Self {
            id,
            window_epoch_ms,
            reason,
            entries,
        })
    }
}
