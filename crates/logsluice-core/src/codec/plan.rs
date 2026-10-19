//! Aggregation plan compilation and header layout computation.

use crate::config::{AggregationKind, AggregationSpec};
use crate::error::AggResult;

/// Physical memory and wire layout configuration for numeric aggregation state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HeaderLayout {
    /// Number of 4-lane u64 counter banks (4 counters per bank).
    pub banks: usize,
    /// Number of running min/max slots (each slot holds `(min: i64, max: i64)`).
    pub minmax_slots: usize,
}

impl HeaderLayout {
    /// Returns the exact encoded byte length of a `NumericHeader` using this layout.
    ///
    /// Fixed fields (16 bytes) + counters (64 bytes per bank) + min/max (16 bytes per slot).
    #[inline]
    #[must_use]
    pub const fn encoded_len(&self) -> usize {
        16 + self.banks * 64 + self.minmax_slots * 16
    }

    /// Decodes a `HeaderLayout` from the 8-byte prefix of `schema_bytes`
    /// (`banks` as `u32` LE in `0..4`, `minmax_slots` as `u32` LE in `4..8`).
    #[inline]
    #[must_use]
    pub fn from_schema_prefix(schema_bytes: &[u8]) -> Option<Self> {
        if schema_bytes.len() < 8 {
            return None;
        }
        let mut b_arr = [0u8; 4];
        b_arr.copy_from_slice(&schema_bytes[0..4]);
        let banks = u32::from_le_bytes(b_arr) as usize;

        let mut m_arr = [0u8; 4];
        m_arr.copy_from_slice(&schema_bytes[4..8]);
        let minmax_slots = u32::from_le_bytes(m_arr) as usize;

        Some(Self {
            banks,
            minmax_slots,
        })
    }
}

/// Compiled aggregation plan mapping configured specifications to layout slots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregationPlan {
    specs: Vec<AggregationSpec>,
    layout: HeaderLayout,
    counter_slots: Vec<Option<(usize, usize)>>,
    minmax_slots: Vec<Option<usize>>,
}

impl AggregationPlan {
    /// Compiles an aggregation plan from a slice of configured aggregation specifications.
    ///
    /// - `Count` and `Sum` are packed into counter banks (4 counters per bank) in declaration order.
    /// - `Min` and `Max` aggregations are assigned their own min/max slot in declaration order.
    ///
    /// # Errors
    /// Returns an error if any invariant is violated.
    pub fn new(specs: &[AggregationSpec]) -> AggResult<Self> {
        let mut counter_count = 0usize;
        let mut minmax_count = 0usize;

        let mut counter_slots = Vec::with_capacity(specs.len());
        let mut minmax_slots = Vec::with_capacity(specs.len());

        for spec in specs {
            match spec.kind {
                AggregationKind::Count | AggregationKind::Sum => {
                    let bank = counter_count / 4;
                    let lane = counter_count % 4;
                    counter_slots.push(Some((bank, lane)));
                    minmax_slots.push(None);
                    counter_count += 1;
                }
                AggregationKind::Min | AggregationKind::Max => {
                    counter_slots.push(None);
                    minmax_slots.push(Some(minmax_count));
                    minmax_count += 1;
                }
            }
        }

        let banks = counter_count.div_ceil(4);

        let layout = HeaderLayout {
            banks,
            minmax_slots: minmax_count,
        };

        Ok(Self {
            specs: specs.to_vec(),
            layout,
            counter_slots,
            minmax_slots,
        })
    }

    /// Returns the computed physical header layout for this plan.
    #[inline]
    #[must_use]
    pub const fn layout(&self) -> HeaderLayout {
        self.layout
    }

    /// Returns the configured aggregation specifications in declaration order.
    #[inline]
    #[must_use]
    pub fn specs(&self) -> &[AggregationSpec] {
        &self.specs
    }

    /// Returns the `(bank, lane)` index for the given aggregation spec index,
    /// or `None` if the spec is not a counter (`Count` or `Sum`) or is out of bounds.
    #[inline]
    #[must_use]
    pub fn counter_slot(&self, spec_index: usize) -> Option<(usize, usize)> {
        self.counter_slots.get(spec_index).copied().flatten()
    }

    /// Returns the min/max slot index for the given aggregation spec index,
    /// or `None` if the spec is not a `Min` or `Max` aggregation or is out of bounds.
    #[inline]
    #[must_use]
    pub fn minmax_slot(&self, spec_index: usize) -> Option<usize> {
        self.minmax_slots.get(spec_index).copied().flatten()
    }

    /// Encodes the schema fingerprint bytes for metadata comparison and schema validation.
    ///
    /// Format: `banks` (u32 LE) and `minmax_slots` (u32 LE) prefix, followed by each spec `name\0kind\0field\0`.
    #[must_use]
    pub fn schema_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        let banks_u32 = u32::try_from(self.layout.banks).unwrap_or(u32::MAX);
        let minmax_u32 = u32::try_from(self.layout.minmax_slots).unwrap_or(u32::MAX);

        buf.extend_from_slice(&banks_u32.to_le_bytes());
        buf.extend_from_slice(&minmax_u32.to_le_bytes());

        for spec in &self.specs {
            buf.extend_from_slice(spec.name.as_bytes());
            buf.push(0);
            let kind_str = match spec.kind {
                AggregationKind::Count => "count",
                AggregationKind::Sum => "sum",
                AggregationKind::Min => "min",
                AggregationKind::Max => "max",
            };
            buf.extend_from_slice(kind_str.as_bytes());
            buf.push(0);
            if let Some(ref field) = spec.field {
                buf.extend_from_slice(field.as_bytes());
            }
            buf.push(0);
        }

        buf
    }
}
