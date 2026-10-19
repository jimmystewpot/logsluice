//! Window clock and time discretization logic.

use std::time::Duration;

use crate::error::{AggError, AggResult};

/// Manages window boundaries and timestamp discretization into tumbling/fixed windows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WindowClock {
    duration_ms: i64,
    offset_ms: i64,
}

impl WindowClock {
    /// Creates a new `WindowClock` with the given window duration and offset.
    ///
    /// # Errors
    /// Returns `AggError::Config` if `duration` is zero or if duration/offset overflow `i64` milliseconds.
    pub fn new(duration: Duration, offset: Duration) -> AggResult<Self> {
        let duration_ms = i64::try_from(duration.as_millis()).map_err(|_| {
            AggError::Config("window duration exceeds maximum supported i64 milliseconds".into())
        })?;

        if duration_ms <= 0 {
            return Err(AggError::Config(
                "window duration must be greater than zero".into(),
            ));
        }

        let raw_offset_ms = i64::try_from(offset.as_millis()).map_err(|_| {
            AggError::Config("window offset exceeds maximum supported i64 milliseconds".into())
        })?;

        let offset_ms = raw_offset_ms.rem_euclid(duration_ms);

        Ok(Self {
            duration_ms,
            offset_ms,
        })
    }

    /// Returns the window duration in milliseconds.
    #[inline]
    #[must_use]
    pub const fn duration_ms(&self) -> i64 {
        self.duration_ms
    }

    /// Returns the normalized window offset in milliseconds (guaranteed `< duration_ms`).
    #[inline]
    #[must_use]
    pub const fn offset_ms(&self) -> i64 {
        self.offset_ms
    }

    /// Computes the window start timestamp in epoch milliseconds containing `ts_ms`.
    ///
    /// Pre-epoch timestamps (negative values) are correctly floored using Euclidean division.
    #[inline]
    #[must_use]
    pub const fn start_of(&self, ts_ms: i64) -> i64 {
        (ts_ms - self.offset_ms).div_euclid(self.duration_ms) * self.duration_ms + self.offset_ms
    }

    /// Computes the window end timestamp (exclusive boundary) for a given window start.
    #[inline]
    #[must_use]
    pub const fn end_of(&self, start_ms: i64) -> i64 {
        start_ms + self.duration_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zero_and_offset_modulo() {
        assert!(WindowClock::new(Duration::ZERO, Duration::ZERO).is_err());

        let clock = WindowClock::new(Duration::from_secs(60), Duration::from_secs(150)).unwrap();
        assert_eq!(clock.duration_ms(), 60_000);
        assert_eq!(clock.offset_ms(), 30_000); // 150_000 % 60_000 = 30_000

        // Window starting at 30_000: spans [30_000, 90_000)
        assert_eq!(clock.start_of(30_000), 30_000);
        assert_eq!(clock.start_of(89_999), 30_000);
        assert_eq!(clock.start_of(90_000), 90_000);
        assert_eq!(clock.start_of(29_999), -30_000);
        assert_eq!(clock.end_of(30_000), 90_000);
    }

    #[test]
    fn test_sub_second_windows() {
        let clock =
            WindowClock::new(Duration::from_millis(500), Duration::from_millis(100)).unwrap();
        assert_eq!(clock.start_of(0), -400); // [-400, 100) contains 0
        assert_eq!(clock.start_of(100), 100);
        assert_eq!(clock.start_of(599), 100);
        assert_eq!(clock.start_of(600), 600);
        assert_eq!(clock.end_of(100), 600);
    }
}
