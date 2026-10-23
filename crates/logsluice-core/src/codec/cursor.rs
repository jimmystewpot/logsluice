//! Shared little-endian cursor for header, tail, and ticket decoders.
//!
//! The three codecs each had a cursor with the same EOF checks. One `Cursor`
//! keeps those error strings so a short buffer still fails the same way in
//! header, tail, and ticket decoders. The layout of each codec is still owned
//! by that file.

use crate::error::{AggError, AggResult};

/// Offset into one encoded slice.
///
/// Integer reads fail with `unexpected EOF reading u8`, `u32`, `u64`, `u128`,
/// or `i64`. [`Cursor::read_bytes`] keeps the tail reader's length-and-offset
/// message.
pub(crate) struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    /// Starts at the first byte of `bytes`.
    #[inline]
    #[must_use]
    pub(crate) const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    /// Bytes already consumed.
    #[inline]
    #[must_use]
    pub(crate) const fn offset(&self) -> usize {
        self.offset
    }

    /// Bytes not yet consumed.
    #[inline]
    #[must_use]
    pub(crate) const fn remaining(&self) -> usize {
        self.bytes.len() - self.offset
    }

    /// Reads one byte.
    ///
    /// # Errors
    /// Returns `AggError::Codec("unexpected EOF reading u8")` at the end of the slice.
    #[inline]
    pub(crate) fn read_u8(&mut self) -> AggResult<u8> {
        let [byte] = self.read_array("unexpected EOF reading u8")?;
        Ok(byte)
    }

    /// Reads a little-endian `u32`.
    ///
    /// # Errors
    /// Returns `AggError::Codec("unexpected EOF reading u32")` when fewer than 4 bytes remain.
    #[inline]
    pub(crate) fn read_u32(&mut self) -> AggResult<u32> {
        Ok(u32::from_le_bytes(
            self.read_array("unexpected EOF reading u32")?,
        ))
    }

    /// Reads a little-endian `u64`.
    ///
    /// # Errors
    /// Returns `AggError::Codec("unexpected EOF reading u64")` when fewer than 8 bytes remain.
    #[inline]
    pub(crate) fn read_u64(&mut self) -> AggResult<u64> {
        Ok(u64::from_le_bytes(
            self.read_array("unexpected EOF reading u64")?,
        ))
    }

    /// Reads a little-endian `u128`.
    ///
    /// # Errors
    /// Returns `AggError::Codec("unexpected EOF reading u128")` when fewer than 16 bytes remain.
    #[inline]
    pub(crate) fn read_u128(&mut self) -> AggResult<u128> {
        Ok(u128::from_le_bytes(
            self.read_array("unexpected EOF reading u128")?,
        ))
    }

    /// Reads a little-endian `i64`.
    ///
    /// # Errors
    /// Returns `AggError::Codec("unexpected EOF reading i64")` when fewer than 8 bytes remain.
    #[inline]
    pub(crate) fn read_i64(&mut self) -> AggResult<i64> {
        Ok(i64::from_le_bytes(
            self.read_array("unexpected EOF reading i64")?,
        ))
    }

    /// Reads the next `len` bytes without copying them.
    ///
    /// # Errors
    /// Returns `AggError::Codec` with
    /// `unexpected EOF reading {len} bytes: offset {offset}, total {total}`
    /// when the slice ends early. `offset` is the index before this read and
    /// `total` is the full slice length.
    #[inline]
    pub(crate) fn read_bytes(&mut self, len: usize) -> AggResult<&'a [u8]> {
        let offset = self.offset;
        // `get` rejects a wrapping `offset + len` instead of slicing a wrapped
        // range. A wire `u32` cannot wrap `usize` on 64-bit, and a short slice
        // still returns the same EOF text as the old cursor.
        let Some(slice) = self.bytes.get(offset..).and_then(|rem| rem.get(..len)) else {
            return Err(AggError::Codec(format!(
                "unexpected EOF reading {len} bytes: offset {offset}, total {}",
                self.bytes.len()
            )));
        };
        self.offset = offset + len;
        Ok(slice)
    }

    #[inline]
    fn read_array<const N: usize>(&mut self, eof: &'static str) -> AggResult<[u8; N]> {
        let Some(chunk) = self
            .bytes
            .get(self.offset..)
            .and_then(<[u8]>::first_chunk::<N>)
        else {
            return Err(AggError::Codec(eof.to_string()));
        };
        self.offset += N;
        Ok(*chunk)
    }
}

#[cfg(test)]
mod tests {
    #![allow(missing_docs)]

    use super::Cursor;
    use crate::error::AggError;

    fn codec_message(result: crate::error::AggResult<impl Sized>) -> String {
        match result {
            Err(AggError::Codec(message)) => message,
            Ok(_) => panic!("expected a codec error"),
            Err(other) => panic!("expected a codec error, got {other}"),
        }
    }

    #[test]
    fn short_slice_returns_each_integer_eof_string() {
        let message = codec_message(Cursor::new(&[]).read_u8());
        assert_eq!(message, "unexpected EOF reading u8");
        let message = codec_message(Cursor::new(&[1, 2, 3]).read_u32());
        assert_eq!(message, "unexpected EOF reading u32");
        let message = codec_message(Cursor::new(&[0; 7]).read_u64());
        assert_eq!(message, "unexpected EOF reading u64");
        let message = codec_message(Cursor::new(&[0; 15]).read_u128());
        assert_eq!(message, "unexpected EOF reading u128");
        let message = codec_message(Cursor::new(&[0; 7]).read_i64());
        assert_eq!(message, "unexpected EOF reading i64");
    }

    #[test]
    fn failed_read_leaves_the_offset_unchanged() {
        let mut cursor = Cursor::new(&[1, 2, 3]);
        let message = codec_message(cursor.read_u32());
        assert_eq!(message, "unexpected EOF reading u32");
        assert_eq!(cursor.offset(), 0);
        assert_eq!(cursor.read_u8().ok(), Some(1));
        assert_eq!(cursor.offset(), 1);
    }

    #[test]
    fn read_bytes_eof_reports_the_offset_before_the_read() {
        let mut cursor = Cursor::new(&[9, 8, 7]);
        assert_eq!(cursor.read_u8().ok(), Some(9));
        let message = codec_message(cursor.read_bytes(4));
        assert_eq!(message, "unexpected EOF reading 4 bytes: offset 1, total 3");
        assert_eq!(cursor.offset(), 1);
        assert_eq!(cursor.remaining(), 2);
    }
}
