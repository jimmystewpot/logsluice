//! Deterministic synthetic event generator for replay harness.

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

/// A generated synthetic event with a key ID and timestamp.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GeneratedEvent {
    /// Identifier in range `[0..keys)`.
    pub key_id: usize,
    /// Event timestamp in epoch milliseconds.
    pub event_ms: i64,
}

/// Deterministic event generator seeded for reproducible replay streams.
pub struct EventGenerator {
    rng: StdRng,
    keys: usize,
    total_events: usize,
    generated: usize,
    base_ts_ms: i64,
}

impl EventGenerator {
    /// Creates a new deterministic event generator.
    #[must_use]
    pub fn new(seed: u64, keys: usize, total_events: usize, base_ts_ms: i64) -> Self {
        Self {
            rng: StdRng::seed_from_u64(seed),
            keys: keys.max(1),
            total_events,
            generated: 0,
            base_ts_ms,
        }
    }
}

impl Iterator for EventGenerator {
    type Item = GeneratedEvent;

    fn next(&mut self) -> Option<Self::Item> {
        if self.generated >= self.total_events {
            return None;
        }

        let key_id = self.rng.random_range(0..self.keys);
        // Advance time gradually: 1 ms every 5 events
        let event_ms = self.base_ts_ms + (self.generated as i64) / 5;
        self.generated += 1;

        Some(GeneratedEvent { key_id, event_ms })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.total_events.saturating_sub(self.generated);
        (remaining, Some(remaining))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generator_is_deterministic_and_tracks_size_hint() {
        let mut g1 = EventGenerator::new(42, 10, 12, 1_000);
        let g2 = EventGenerator::new(42, 10, 12, 1_000);

        assert_eq!(g1.size_hint(), (12, Some(12)));
        let v1: Vec<GeneratedEvent> = g1.by_ref().collect();
        let v2: Vec<GeneratedEvent> = g2.collect();

        assert_eq!(v1.len(), 12);
        assert_eq!(v1, v2);
        assert_eq!(g1.size_hint(), (0, Some(0)));
        assert!(v1.iter().all(|e| e.key_id < 10 && e.event_ms >= 1_000));
    }

    #[test]
    fn generator_clamps_zero_keys_without_panicking() {
        let events: Vec<GeneratedEvent> = EventGenerator::new(7, 0, 5, 2_000).collect();
        assert_eq!(events.len(), 5);
        assert!(events.iter().all(|e| e.key_id == 0));
    }
}
