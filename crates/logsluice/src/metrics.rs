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
    counter!("logsluice_forced_closes_total", "shard" => shard_str.clone())
        .absolute(stats.forced_closes_total);
    gauge!("logsluice_disk_bytes", "shard" => shard_str.clone()).set(stats.disk_bytes as f64);
    gauge!("logsluice_resources_table_entries", "shard" => shard_str.clone())
        .set(stats.resources_table_entries as f64);
    gauge!("logsluice_spill_files", "shard" => shard_str.clone()).set(stats.spill_files as f64);
    counter!("logsluice_dropped_events_total", "shard" => shard_str)
        .absolute(stats.dropped_events_total);

    counter!("logsluice_events_routed_total", "route" => "quarantine")
        .absolute(router.quarantined_total);
    counter!("logsluice_events_routed_total", "route" => "late").absolute(router.late_total);
    counter!("logsluice_field_type_errors_total").absolute(router.field_type_errors_total);
    counter!("logsluice_field_coercions_total").absolute(router.field_coercions_total);
}
