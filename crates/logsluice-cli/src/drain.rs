//! Offline window drain tool scanning live shard stores.

#![allow(clippy::print_stdout)]

use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use hashbrown::{HashMap, HashSet};
use logsluice_core::{
    codec::{plan::HeaderLayout, tail::ExemplarTail, ticket::EmitReason},
    emission::{SliceRecord, slice_id},
    error::{AggError, AggResult},
    key::StorageKey24,
    resource::ResourceScopePayload,
    simd::{SimdKernelTable, SimdMode},
    store::{
        meta::PodMeta,
        shard_store::{JoinedRow, ShardStore, StoreOptions},
    },
    window::WindowClock,
    worker::spill::SpillWriter,
};

fn scan_window_rows_map(store: &ShardStore, epoch_ms: u64) -> AggResult<HashMap<u128, JoinedRow>> {
    let mut map = HashMap::new();
    let mut after: Option<StorageKey24> = None;
    loop {
        let rows = store.rows(epoch_ms, after, 500)?;
        let batch_len = rows.len();
        if let Some(last) = rows.last() {
            after = Some(last.key);
        }
        for row in rows {
            map.insert(row.key.digest, row);
        }
        if batch_len < 500 {
            break;
        }
    }
    Ok(map)
}

/// Executes the offline drain command across all shard stores in `data_dir`.
///
/// # Errors
/// Returns [`AggError`] if metadata or shard storage cannot be read.
pub fn run_drain(data_dir: &Path, format: &str) -> AggResult<()> {
    if format != "ndjson" {
        return Err(AggError::Config(format!(
            "unsupported format '{format}'; only 'ndjson' is supported"
        )));
    }

    let meta_path = data_dir.join("pod_meta.json");
    if !meta_path.try_exists()? {
        return Err(AggError::Config(format!(
            "pod_meta.json not found in {}",
            data_dir.display()
        )));
    }

    let meta_bytes = std::fs::read(&meta_path)?;
    let meta: PodMeta =
        serde_json::from_slice(&meta_bytes).map_err(|e| AggError::Codec(e.to_string()))?;

    let layout = HeaderLayout::from_schema_prefix(&meta.codec_schema).unwrap_or(HeaderLayout {
        banks: 1,
        minmax_slots: 2,
    });

    let kernels = SimdKernelTable::detect(SimdMode::Scalar);
    let clock = WindowClock::new(
        Duration::from_millis(meta.window_duration_ms.max(1) as u64),
        Duration::from_millis(meta.window_offset_ms.unsigned_abs()),
    )?;

    let default_res = Arc::new(ResourceScopePayload {
        resource: [(
            "service.name".to_string(),
            serde_json::json!("replay-service"),
        )]
        .into(),
        scope_name: "replay-scope".to_string(),
        scope_version: "1.0.0".to_string(),
        scope_attrs: Default::default(),
    });

    for s in 0..meta.shards {
        let shard_dir = data_dir.join(format!("shard_{s}"));
        if !shard_dir.try_exists()? {
            continue;
        }

        let spill_dir = shard_dir.join("spill");
        let store_opts = StoreOptions {
            shard_dir,
            cache_bytes: 8 << 20,
            hdr_memtable_bytes: 1 << 20,
            tail_memtable_bytes: 1 << 20,
            sync_wal: true,
            layout,
            worker_threads: 2,
            max_write_buffer_bytes: None,
            max_cached_files: None,
        };

        let mut store = ShardStore::open(store_opts, &meta)?;
        let mut resources = HashMap::new();
        for (id, payload, _windows) in store.resources()? {
            resources.insert(id, Arc::new(payload));
        }

        let mut spill_writer = SpillWriter::new(spill_dir);
        let mut spilled_ticket_ids = HashSet::new();
        for (ticket_id, records) in spill_writer.pending()? {
            spilled_ticket_ids.insert(ticket_id);
            for record in records {
                let line =
                    serde_json::to_string(&record).map_err(|e| AggError::Codec(e.to_string()))?;
                println!("{line}");
            }
            std::io::stdout().flush()?;
            spill_writer.remove(ticket_id)?;
        }

        let mut tickets = store.tickets()?;
        tickets.sort_by_key(|t| t.id);
        let mut ticket_window_rows: HashMap<u64, HashMap<u128, JoinedRow>> = HashMap::new();

        for ticket in tickets {
            if spilled_ticket_ids.contains(&ticket.id) {
                store.apply_ack(&ticket, &kernels)?;
                continue;
            }

            let row_map = match ticket_window_rows.entry(ticket.window_epoch_ms) {
                hashbrown::hash_map::Entry::Occupied(e) => e.into_mut(),
                hashbrown::hash_map::Entry::Vacant(e) => {
                    let scanned = scan_window_rows_map(&store, ticket.window_epoch_ms)?;
                    e.insert(scanned)
                }
            };

            let window_start_ms = ticket.window_epoch_ms as i64;
            let window_end_ms = clock.end_of(window_start_ms);

            for entry in &ticket.entries {
                let Some(row) = row_map
                    .get(&entry.digest)
                    .filter(|r| r.header.slice_seq == entry.slice_seq)
                else {
                    continue;
                };

                let res = resources
                    .get(&row.header.resource_scope_id)
                    .cloned()
                    .unwrap_or_else(|| Arc::clone(&default_res));
                let tail = row.tail.clone().unwrap_or_else(|| ExemplarTail {
                    severity: 0,
                    body: None,
                    dims: Vec::new(),
                    attrs: Vec::new(),
                });

                let sid = slice_id(
                    &meta.emitter_id,
                    ticket.window_epoch_ms,
                    entry.digest,
                    entry.slice_seq,
                );

                let record = SliceRecord {
                    window_start_ms,
                    window_end_ms,
                    emit_reason: ticket.reason,
                    emitter_id: meta.emitter_id.clone(),
                    digest: entry.digest,
                    slice_seq: entry.slice_seq,
                    slice_id: sid,
                    resource: res,
                    tail,
                    counters: entry.delta.clone(),
                    minmax: entry.minmax.clone(),
                };

                let line =
                    serde_json::to_string(&record).map_err(|e| AggError::Codec(e.to_string()))?;
                println!("{line}");
            }
            std::io::stdout().flush()?;
            store.apply_ack(&ticket, &kernels)?;
        }

        for epoch_ms in store.live_windows() {
            let mut after = None;
            loop {
                let rows = store.rows(epoch_ms, after, 500)?;
                if rows.is_empty() {
                    break;
                }
                let batch_len = rows.len();
                if let Some(last) = rows.last() {
                    after = Some(last.key);
                }
                for row in rows {
                    let mut delta = vec![[0u64; 4]; row.header.total.len()];
                    let non_zero = (kernels.diff_counter_banks)(
                        &row.header.total,
                        &row.header.emitted,
                        &mut delta,
                    );

                    if non_zero {
                        let res = resources
                            .get(&row.header.resource_scope_id)
                            .cloned()
                            .unwrap_or_else(|| Arc::clone(&default_res));

                        let tail = row.tail.clone().unwrap_or_else(|| ExemplarTail {
                            severity: 0,
                            body: None,
                            dims: Vec::new(),
                            attrs: Vec::new(),
                        });

                        let sid = slice_id(
                            &meta.emitter_id,
                            epoch_ms,
                            row.key.digest,
                            row.header.slice_seq,
                        );
                        let window_start_ms = epoch_ms as i64;
                        let window_end_ms = clock.end_of(window_start_ms);

                        let record = SliceRecord {
                            window_start_ms,
                            window_end_ms,
                            emit_reason: EmitReason::Drain,
                            emitter_id: meta.emitter_id.clone(),
                            digest: row.key.digest,
                            slice_seq: row.header.slice_seq,
                            slice_id: sid,
                            resource: res,
                            tail,
                            counters: delta,
                            minmax: row.header.minmax.clone(),
                        };

                        let line = serde_json::to_string(&record)
                            .map_err(|e| AggError::Codec(e.to_string()))?;
                        println!("{line}");
                    }
                }
                if batch_len < 500 {
                    break;
                }
            }
        }
    }

    Ok(())
}
