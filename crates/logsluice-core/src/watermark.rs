//! Per-partition and per-source watermark tracking with idle/catch-up semantics.

use std::collections::HashMap;
use std::sync::Arc;

/// Key identifying a partition or logical source.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum SourceKey {
    /// Explicit topic and partition.
    Partition(Arc<str>, i32),
    /// Logical named source.
    Source(Arc<str>),
    /// Fallback default bucket for sources exceeding tracking capacity.
    Default,
}

/// Configuration parameters for watermark computation and source eviction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WatermarkConfig {
    /// Maximum allowed clock skew subtracted from source max event time.
    pub allowed_skew_ms: i64,
    /// Maximum drift into future relative to wall-clock time allowed before quarantine.
    pub max_future_drift_ms: i64,
    /// Inactivity duration after which a partition may be considered idle.
    pub idle_timeout_ms: i64,
    /// Threshold between wall-clock observation and event timestamp for partition to be considered caught-up.
    pub caught_up_threshold_ms: i64,
    /// Grace period after tracker start during which seeded but unseen partitions hold back watermark.
    pub seed_grace_ms: i64,
    /// Maximum number of distinct `SourceKey::Source` keys tracked before mapping to `SourceKey::Default`.
    pub max_tracked_sources: usize,
}

/// Admission decision for an observed event.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Admit {
    /// Event admitted for processing.
    Accept,
    /// Event quarantined due to excessive future drift.
    Quarantine,
}

#[derive(Clone, Debug, Default)]
struct SourceState {
    max_event: Option<i64>,
    last_seen: Option<i64>,
    last_event: Option<i64>,
}

/// Tracks event timestamps across partitions/sources and computes the advancing watermark.
#[derive(Clone, Debug)]
pub struct WatermarkTracker {
    cfg: WatermarkConfig,
    started_ms: i64,
    sources: HashMap<SourceKey, SourceState>,
}

impl WatermarkTracker {
    /// Creates a new `WatermarkTracker`.
    #[must_use]
    pub fn new(cfg: WatermarkConfig, started_ms: i64) -> Self {
        Self {
            cfg,
            started_ms,
            sources: HashMap::new(),
        }
    }

    /// Seeds known source keys before events arrive.
    pub fn seed_known(&mut self, keys: impl IntoIterator<Item = SourceKey>) {
        for key in keys {
            self.sources.entry(key).or_default();
        }
    }

    /// Observes an event for the given key.
    ///
    /// Quarantines events that drift into the future beyond `max_future_drift_ms`.
    /// Caps tracked `SourceKey::Source` items at `max_tracked_sources`, mapping overflows to `SourceKey::Default`.
    pub fn observe(&mut self, key: SourceKey, event_ms: i64, now_ms: i64) -> Admit {
        if event_ms > now_ms.saturating_add(self.cfg.max_future_drift_ms) {
            return Admit::Quarantine;
        }

        let target_key = match key {
            SourceKey::Source(_)
                if !self.sources.contains_key(&key)
                    && self.sources.len() >= self.cfg.max_tracked_sources =>
            {
                SourceKey::Default
            }
            other => other,
        };

        let s = self.sources.entry(target_key).or_default();
        s.max_event = Some(s.max_event.map_or(event_ms, |m| m.max(event_ms)));
        s.last_seen = Some(now_ms);
        s.last_event = Some(event_ms);

        Admit::Accept
    }

    /// Computes the current watermark across all tracked sources.
    ///
    /// Returns `None` if any seeded source has not yet received events and `seed_grace_ms` has not elapsed.
    /// Excludes idle sources that are caught up (`now - last_seen > idle_timeout` AND `last_seen - last_event <= caught_up_threshold`).
    /// If all seen sources are excluded, falls back to `now - allowed_skew - caught_up_threshold`.
    #[must_use]
    pub fn watermark(&self, now_ms: i64) -> Option<i64> {
        // 1. Check seeded sources:
        // If any source has max_event.is_none():
        //   if now_ms - started_ms <= seed_grace_ms: return None (blocking).
        for s in self.sources.values() {
            if s.max_event.is_none()
                && now_ms.saturating_sub(self.started_ms) <= self.cfg.seed_grace_ms
            {
                return None;
            }
        }

        // 2. Check sources with events (max_event.is_some()):
        let mut min_contribution: Option<i64> = None;
        for s in self.sources.values() {
            if let (Some(max_event), Some(last_seen), Some(last_event)) =
                (s.max_event, s.last_seen, s.last_event)
            {
                let is_idle = now_ms.saturating_sub(last_seen) > self.cfg.idle_timeout_ms;
                let is_caught_up =
                    last_seen.saturating_sub(last_event) <= self.cfg.caught_up_threshold_ms;
                let is_excluded = is_idle && is_caught_up;
                if !is_excluded {
                    let contribution = max_event.saturating_sub(self.cfg.allowed_skew_ms);
                    min_contribution =
                        Some(min_contribution.map_or(contribution, |m| m.min(contribution)));
                }
            }
        }

        // 3. If there are active sources, return min(contributions).
        // 4. If every seen source is excluded (or no sources):
        min_contribution.or_else(|| {
            Some(
                now_ms
                    .saturating_sub(self.cfg.allowed_skew_ms)
                    .saturating_sub(self.cfg.caught_up_threshold_ms),
            )
        })
    }

    /// Returns the number of currently tracked sources.
    #[must_use]
    pub fn tracked_len(&self) -> usize {
        self.sources.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_cfg() -> WatermarkConfig {
        WatermarkConfig {
            allowed_skew_ms: 1_000,
            max_future_drift_ms: 5_000,
            idle_timeout_ms: 10_000,
            caught_up_threshold_ms: 2_000,
            seed_grace_ms: 30_000,
            max_tracked_sources: 2,
        }
    }

    #[test]
    fn test_empty_tracker_watermark() {
        let w = WatermarkTracker::new(test_cfg(), 100_000);
        // Empty tracker falls back to all-idle / no-sources formula: now - skew - caught_up
        assert_eq!(w.watermark(100_000), Some(100_000 - 1_000 - 2_000));
        assert_eq!(w.tracked_len(), 0);
    }

    #[test]
    fn test_observe_existing_source_at_capacity() {
        let mut w = WatermarkTracker::new(test_cfg(), 100_000);
        let s1 = SourceKey::Source("s1".into());
        let s2 = SourceKey::Source("s2".into());

        assert_eq!(w.observe(s1.clone(), 90_000, 100_000), Admit::Accept);
        assert_eq!(w.observe(s2.clone(), 91_000, 100_000), Admit::Accept);
        assert_eq!(w.tracked_len(), 2);

        // Observing an existing source when capacity is reached should NOT map to Default
        assert_eq!(w.observe(s1.clone(), 95_000, 100_500), Admit::Accept);
        assert_eq!(w.tracked_len(), 2);

        // Watermark should be min(95_000 - 1_000, 91_000 - 1_000) = 90_000
        assert_eq!(w.watermark(100_500), Some(90_000));
    }

    #[test]
    fn test_seeded_sources_idempotent() {
        let mut w = WatermarkTracker::new(test_cfg(), 100_000);
        let p0 = SourceKey::Partition("t".into(), 0);
        w.seed_known([p0.clone()]);
        w.seed_known([p0.clone()]);
        assert_eq!(w.tracked_len(), 1);

        // Observe an event on p0
        w.observe(p0.clone(), 95_000, 100_000);
        // Re-seeding p0 should not reset its state back to None
        w.seed_known([p0.clone()]);
        assert_eq!(w.watermark(100_000), Some(95_000 - 1_000));
    }
}
