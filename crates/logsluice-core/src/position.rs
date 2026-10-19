//! Source offset tracking for deduplicating replayed inputs.

use std::collections::HashMap;
use std::sync::Arc;

/// Position descriptor for an input stream record.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SourcePosition {
    /// Topic or stream identifier.
    pub topic: Arc<str>,
    /// Partition index.
    pub partition: i32,
    /// Record offset within partition.
    pub offset: i64,
}

/// Tracks highest seen offsets per source partition and records newly advanced offsets as pending.
#[derive(Clone, Debug, Default)]
pub struct OffsetTracker {
    hwm: HashMap<(Arc<str>, i32), i64>,
    pending: HashMap<(Arc<str>, i32), i64>,
}

impl OffsetTracker {
    /// Creates an `OffsetTracker` initialized from persisted high-water marks.
    #[must_use]
    pub fn from_persisted(hwm: HashMap<(Arc<str>, i32), i64>) -> Self {
        Self {
            hwm,
            pending: HashMap::new(),
        }
    }

    /// Returns `true` if `pos.offset` is less than or equal to the high-water mark for its partition.
    #[must_use]
    pub fn is_replay(&self, pos: &SourcePosition) -> bool {
        self.hwm
            .get(&(pos.topic.clone(), pos.partition))
            .is_some_and(|&h| pos.offset <= h)
    }

    /// Observes a source position, advancing the high-water mark and recording it as pending if higher.
    pub fn observe(&mut self, pos: &SourcePosition) {
        let key = (pos.topic.clone(), pos.partition);
        let is_higher = self.hwm.get(&key).is_none_or(|&h| pos.offset > h);
        if is_higher {
            self.hwm.insert(key.clone(), pos.offset);
            self.pending.insert(key, pos.offset);
        }
    }

    /// Drains and returns all pending high-water mark advancements.
    pub fn take_pending(&mut self) -> Vec<((Arc<str>, i32), i64)> {
        let mut list: Vec<((Arc<str>, i32), i64)> =
            std::mem::take(&mut self.pending).into_iter().collect();
        list.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        list
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_tracker() {
        let mut tracker = OffsetTracker::default();
        let pos = SourcePosition {
            topic: Arc::from("orders"),
            partition: 1,
            offset: 0,
        };
        assert!(!tracker.is_replay(&pos));
        assert!(tracker.take_pending().is_empty());

        tracker.observe(&pos);
        assert!(tracker.is_replay(&pos));
        assert_eq!(tracker.take_pending(), vec![((Arc::from("orders"), 1), 0)]);
        assert!(tracker.take_pending().is_empty());
    }

    #[test]
    fn test_multiple_partitions_ordering() {
        let mut tracker = OffsetTracker::default();
        let p0 = SourcePosition {
            topic: Arc::from("topic-b"),
            partition: 0,
            offset: 10,
        };
        let p1 = SourcePosition {
            topic: Arc::from("topic-a"),
            partition: 2,
            offset: 20,
        };
        let p2 = SourcePosition {
            topic: Arc::from("topic-a"),
            partition: 1,
            offset: 30,
        };

        tracker.observe(&p0);
        tracker.observe(&p1);
        tracker.observe(&p2);

        let pending = tracker.take_pending();
        assert_eq!(
            pending,
            vec![
                ((Arc::from("topic-a"), 1), 30),
                ((Arc::from("topic-a"), 2), 20),
                ((Arc::from("topic-b"), 0), 10),
            ]
        );
    }
}
