//! Observability metrics export for shard workers and router state.

use logsluice_core::worker::WorkerStats;
use metrics::{counter, gauge};

use crate::router::RouterStats;

/// Exports worker and router operational metrics to the global metrics registry.
pub fn export(shard: u16, stats: &WorkerStats, router: &RouterStats) {
    let shard_str = shard.to_string();

    counter!("logsluice_events_ingested_total", "shard" => shard_str.clone())
        .absolute(stats.events_ingested_total);
    counter!("logsluice_replayed_events_total", "shard" => shard_str.clone())
        .absolute(stats.replayed_events_total);
    gauge!("logsluice_keys_known", "shard" => shard_str.clone()).set(stats.keys_known as f64);
    counter!("logsluice_keys_overflowed_total", "shard" => shard_str.clone())
        .absolute(stats.keys_overflowed_total);
    counter!("logsluice_keys_evicted_total", "shard" => shard_str.clone())
        .absolute(stats.keys_evicted_total);
    gauge!("logsluice_tickets_inflight", "shard" => shard_str.clone())
        .set(stats.tickets_inflight as f64);
    counter!("logsluice_slices_reemitted_total", "shard" => shard_str.clone())
        .absolute(stats.slices_reemitted_total);
    counter!("logsluice_recovery_failures_total", "shard" => shard_str.clone())
        .absolute(stats.recovery_failures_total);
    counter!("logsluice_forced_closes_total", "shard" => shard_str.clone())
        .absolute(stats.forced_closes_total);
    gauge!("logsluice_disk_bytes", "shard" => shard_str.clone()).set(stats.disk_bytes as f64);
    gauge!("logsluice_resources_table_entries", "shard" => shard_str.clone())
        .set(stats.resources_table_entries as f64);
    gauge!("logsluice_spill_files", "shard" => shard_str.clone()).set(stats.spill_files as f64);
    counter!("logsluice_spill_quarantined_total", "shard" => shard_str.clone())
        .absolute(stats.spill_quarantined_total);
    gauge!("logsluice_store_cmd_queue_depth", "shard" => shard_str.clone())
        .set(stats.store_cmd_queue_depth as f64);
    counter!("logsluice_dropped_events_total", "shard" => shard_str)
        .absolute(stats.dropped_events_total);

    counter!("logsluice_events_routed_total", "route" => "quarantine")
        .absolute(router.quarantined_total);
    counter!("logsluice_events_routed_total", "route" => "late").absolute(router.late_total);
    counter!("logsluice_field_type_errors_total").absolute(router.field_type_errors_total);
    counter!("logsluice_field_coercions_total").absolute(router.field_coercions_total);
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use logsluice_core::worker::WorkerStats;
    use metrics::{
        Counter, CounterFn, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit,
    };

    use super::export;
    use crate::router::RouterStats;

    type CounterKey = (String, Vec<(String, String)>);
    type CounterMap = Arc<Mutex<HashMap<CounterKey, u64>>>;

    struct MapCounter {
        name: String,
        labels: Vec<(String, String)>,
        values: CounterMap,
    }

    impl CounterFn for MapCounter {
        fn increment(&self, value: u64) {
            if let Ok(mut guard) = self.values.lock() {
                *guard
                    .entry((self.name.clone(), self.labels.clone()))
                    .or_insert(0) += value;
            }
        }

        fn absolute(&self, value: u64) {
            if let Ok(mut guard) = self.values.lock() {
                guard.insert((self.name.clone(), self.labels.clone()), value);
            }
        }
    }

    #[derive(Default)]
    struct TestRecorder {
        counters: CounterMap,
    }

    impl Recorder for TestRecorder {
        fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
        fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
        fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

        fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
            let labels = key
                .labels()
                .map(|l| (l.key().to_string(), l.value().to_string()))
                .collect();
            Counter::from_arc(Arc::new(MapCounter {
                name: key.name().to_string(),
                labels,
                values: Arc::clone(&self.counters),
            }))
        }

        fn register_gauge(&self, _key: &Key, _metadata: &Metadata<'_>) -> Gauge {
            Gauge::noop()
        }

        fn register_histogram(&self, _key: &Key, _metadata: &Metadata<'_>) -> Histogram {
            Histogram::noop()
        }
    }

    fn init_test_recorder() -> TestRecorder {
        let recorder = TestRecorder::default();
        recorder.describe_counter("c".into(), None, "".into());
        recorder.describe_gauge("g".into(), None, "".into());
        recorder.describe_histogram("h".into(), None, "".into());
        static META: Metadata<'static> = Metadata::new(module_path!(), metrics::Level::INFO, None);
        let key = Key::from_name("init");
        let _ = recorder.register_gauge(&key, &META);
        let _ = recorder.register_histogram(&key, &META);
        recorder.register_counter(&key, &META).increment(0);
        recorder
    }

    #[test]
    fn exports_recovery_failures_total_counter() {
        let recorder = init_test_recorder();
        let stats = WorkerStats {
            recovery_failures_total: 7,
            ..WorkerStats::default()
        };
        let router = RouterStats::default();

        metrics::with_local_recorder(&recorder, || {
            export(2, &stats, &router);
        });

        let counters = recorder.counters.lock().expect("lock");
        let val = counters.get(&(
            "logsluice_recovery_failures_total".to_string(),
            vec![("shard".to_string(), "2".to_string())],
        ));
        assert_eq!(val, Some(&7));
    }

    #[test]
    fn exports_spill_quarantined_total_counter() {
        let recorder = init_test_recorder();
        let stats = WorkerStats {
            spill_quarantined_total: 3,
            ..WorkerStats::default()
        };
        let router = RouterStats::default();

        metrics::with_local_recorder(&recorder, || {
            export(1, &stats, &router);
        });

        let counters = recorder.counters.lock().expect("lock");
        let val = counters.get(&(
            "logsluice_spill_quarantined_total".to_string(),
            vec![("shard".to_string(), "1".to_string())],
        ));
        assert_eq!(val, Some(&3));
    }
}
