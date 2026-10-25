//! Exemplar tail codec for retained log attributes and exemplar dimensions.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::{AggError, AggResult};

impl Default for ExemplarTail {
    /// Same as [`ExemplarTail::missing`].
    #[inline]
    fn default() -> Self {
        Self::missing()
    }
}

/// Tail payload storing exemplar log message body, grouping dimensions, and unmatched attributes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExemplarTail {
    /// Exemplar event severity level.
    pub severity: u8,
    /// Optional exemplar raw log message body.
    pub body: Option<Bytes>,
    /// Optional grouping dimension values in declaration order.
    pub dims: Vec<Option<Bytes>>,
    /// Unmatched or retained key-value log attributes.
    pub attrs: Vec<(String, Bytes)>,
}

impl ExemplarTail {
    /// Tail used when a header row has no exemplar payload.
    ///
    /// Severity is `0` and body, dimensions, and attributes are empty.
    /// Live close and `logsluice drain` both substitute this through
    /// [`crate::emission::SliceRecord::from_parts`] so a missing tail key
    /// cannot drift between the two emitters.
    #[inline]
    #[must_use]
    pub const fn missing() -> Self {
        Self {
            severity: 0,
            body: None,
            dims: Vec::new(),
            attrs: Vec::new(),
        }
    }

    /// Serializes the exemplar tail into a versioned TLV byte buffer.
    ///
    /// Version byte is always `1u8`.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        // Version 1
        buf.push(1u8);
        buf.push(self.severity);

        // Body
        match &self.body {
            None => buf.push(0u8),
            Some(b) => {
                buf.push(1u8);
                let len = u32::try_from(b.len()).unwrap_or(u32::MAX);
                buf.extend_from_slice(&len.to_le_bytes());
                buf.extend_from_slice(b);
            }
        }

        // Dims
        let dims_count = u32::try_from(self.dims.len()).unwrap_or(u32::MAX);
        buf.extend_from_slice(&dims_count.to_le_bytes());
        for dim in &self.dims {
            match dim {
                None => buf.push(0u8),
                Some(d) => {
                    buf.push(1u8);
                    let len = u32::try_from(d.len()).unwrap_or(u32::MAX);
                    buf.extend_from_slice(&len.to_le_bytes());
                    buf.extend_from_slice(d);
                }
            }
        }

        // Attrs
        let attrs_count = u32::try_from(self.attrs.len()).unwrap_or(u32::MAX);
        buf.extend_from_slice(&attrs_count.to_le_bytes());
        for (k, v) in &self.attrs {
            let k_len = u32::try_from(k.len()).unwrap_or(u32::MAX);
            buf.extend_from_slice(&k_len.to_le_bytes());
            buf.extend_from_slice(k.as_bytes());

            let v_len = u32::try_from(v.len()).unwrap_or(u32::MAX);
            buf.extend_from_slice(&v_len.to_le_bytes());
            buf.extend_from_slice(v);
        }

        buf
    }

    /// Deserializes an exemplar tail from `bytes`.
    ///
    /// # Errors
    /// Returns `AggError::Codec` if version is unsupported, bounds check fails, or bytes are corrupted.
    pub fn decode(bytes: &[u8]) -> AggResult<Self> {
        if bytes.len() < 2 {
            return Err(AggError::Codec(format!(
                "invalid ExemplarTail length: expected at least 2 bytes, got {}",
                bytes.len()
            )));
        }

        let version = bytes[0];
        if version != 1 {
            return Err(AggError::Codec(format!(
                "unsupported ExemplarTail version: {version}"
            )));
        }

        let severity = bytes[1];
        let mut offset = 2;

        // Body
        let body_tag = read_u8(bytes, &mut offset)?;
        let body = match body_tag {
            0 => None,
            1 => {
                let len = read_u32_le(bytes, &mut offset)? as usize;
                let data = read_slice(bytes, &mut offset, len)?;
                Some(Bytes::copy_from_slice(data))
            }
            other => {
                return Err(AggError::Codec(format!(
                    "invalid body tag in ExemplarTail: {other}"
                )));
            }
        };

        // Dims
        let dims_count = read_u32_le(bytes, &mut offset)? as usize;
        let remaining_bytes = bytes.len() - offset;
        // Each dim element needs at least 1 byte (tag 0 or tag 1 + len + data)
        if dims_count > remaining_bytes {
            return Err(AggError::Codec(format!(
                "invalid dims_count {dims_count}: exceeds remaining bytes {remaining_bytes}"
            )));
        }
        let mut dims = Vec::with_capacity(dims_count);
        for _ in 0..dims_count {
            let dim_tag = read_u8(bytes, &mut offset)?;
            let dim = match dim_tag {
                0 => None,
                1 => {
                    let len = read_u32_le(bytes, &mut offset)? as usize;
                    let data = read_slice(bytes, &mut offset, len)?;
                    Some(Bytes::copy_from_slice(data))
                }
                other => {
                    return Err(AggError::Codec(format!(
                        "invalid dim tag in ExemplarTail: {other}"
                    )));
                }
            };
            dims.push(dim);
        }

        // Attrs
        let attrs_count = read_u32_le(bytes, &mut offset)? as usize;
        let remaining_bytes = bytes.len() - offset;
        // Each attr needs at least 8 bytes (u32 k_len + u32 v_len)
        if attrs_count > remaining_bytes / 8 {
            return Err(AggError::Codec(format!(
                "invalid attrs_count {attrs_count}: exceeds remaining bytes {remaining_bytes}"
            )));
        }
        let mut attrs = Vec::with_capacity(attrs_count);
        for _ in 0..attrs_count {
            let k_len = read_u32_le(bytes, &mut offset)? as usize;
            let k_bytes = read_slice(bytes, &mut offset, k_len)?;
            let key = String::from_utf8(k_bytes.to_vec())
                .map_err(|e| AggError::Codec(format!("invalid UTF-8 in attr key: {e}")))?;

            let v_len = read_u32_le(bytes, &mut offset)? as usize;
            let v_bytes = read_slice(bytes, &mut offset, v_len)?;
            let val = Bytes::copy_from_slice(v_bytes);

            attrs.push((key, val));
        }

        if offset != bytes.len() {
            return Err(AggError::Codec(format!(
                "trailing bytes in ExemplarTail: consumed {offset}, total {}",
                bytes.len()
            )));
        }

        Ok(Self {
            severity,
            body,
            dims,
            attrs,
        })
    }
}

#[inline]
fn read_u8(bytes: &[u8], offset: &mut usize) -> AggResult<u8> {
    if *offset >= bytes.len() {
        return Err(AggError::Codec("unexpected EOF reading u8".to_string()));
    }
    let val = bytes[*offset];
    *offset += 1;
    Ok(val)
}

#[inline]
fn read_u32_le(bytes: &[u8], offset: &mut usize) -> AggResult<u32> {
    let next = *offset + 4;
    if next > bytes.len() {
        return Err(AggError::Codec("unexpected EOF reading u32".to_string()));
    }
    let slice = &bytes[*offset..next];
    *offset = next;
    let mut arr = [0u8; 4];
    arr.copy_from_slice(slice);
    Ok(u32::from_le_bytes(arr))
}

#[inline]
fn read_slice<'a>(bytes: &'a [u8], offset: &mut usize, len: usize) -> AggResult<&'a [u8]> {
    let next = *offset + len;
    if next > bytes.len() {
        return Err(AggError::Codec(format!(
            "unexpected EOF reading {len} bytes: offset {offset}, total {}",
            bytes.len()
        )));
    }
    let slice = &bytes[*offset..next];
    *offset = next;
    Ok(slice)
}
