//! Offline window drain tool scanning live shard stores.

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::collections::BTreeMap;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use hashbrown::{HashMap, HashSet};
use logsluice_core::{
    codec::{
        plan::HeaderLayout,
        tail::ExemplarTail,
        ticket::{EmitReason, Ticket, TicketEntry},
    },
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

    let mut total_scan_point_gets = 0u64;

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

        let mut tickets_by_window: BTreeMap<u64, Vec<Ticket>> = BTreeMap::new();
        for ticket in tickets {
            tickets_by_window
                .entry(ticket.window_epoch_ms)
                .or_default()
                .push(ticket);
        }

        for (window_epoch_ms, window_tickets) in tickets_by_window {
            let mut pending_by_digest: BTreeMap<u128, Vec<(&Ticket, &TicketEntry)>> =
                BTreeMap::new();
            for ticket in &window_tickets {
                if spilled_ticket_ids.contains(&ticket.id) {
                    continue;
                }
                for entry in &ticket.entries {
                    pending_by_digest
                        .entry(entry.digest)
                        .or_default()
                        .push((ticket, entry));
                }
            }

            if !pending_by_digest.is_empty() {
                let sorted_keys: Vec<StorageKey24> = pending_by_digest
                    .keys()
                    .map(|&digest| StorageKey24 {
                        epoch_ms: window_epoch_ms,
                        digest,
                    })
                    .collect();

                let window_start_ms = window_epoch_ms as i64;
                let window_end_ms = clock.end_of(window_start_ms);

                for chunk in sorted_keys.chunks(500) {
                    let rows = store.rows_for_sorted(chunk)?;
                    let row_map: HashMap<u128, &JoinedRow> =
                        rows.iter().map(|r| (r.key.digest, r)).collect();

                    for key in chunk {
                        let Some(entries_for_digest) = pending_by_digest.get(&key.digest) else {
                            continue;
                        };
                        let Some(&(_, first_entry)) = entries_for_digest.first() else {
                            continue;
                        };
                        let matching_row = row_map.get(&key.digest).copied();
                        // Track `next_seq` in memory per digest during the pre-ack emission phase:
                        // `apply_ack` runs only after all unspilled tickets for the window are emitted,
                        // so `_hdr` has not yet advanced `slice_seq` between consecutive tickets for the
                        // same key. Without advancing `next_seq` locally, a second ticket for the same
                        // key (`entry.slice_seq == header.slice_seq + 1`) would be skipped.
                        let mut next_seq =
                            matching_row.map_or(first_entry.slice_seq, |r| r.header.slice_seq);

                        for &(ticket, entry) in entries_for_digest {
                            if entry.slice_seq != next_seq {
                                continue;
                            }
                            next_seq = next_seq.wrapping_add(1);

                            let (res, tail) = if let Some(row) = matching_row {
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
                                (res, tail)
                            } else {
                                (
                                    Arc::clone(&default_res),
                                    ExemplarTail {
                                        severity: 0,
                                        body: None,
                                        dims: Vec::new(),
                                        attrs: Vec::new(),
                                    },
                                )
                            };

                            let sid = slice_id(
                                &meta.emitter_id,
                                window_epoch_ms,
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

                            let line = serde_json::to_string(&record)
                                .map_err(|e| AggError::Codec(e.to_string()))?;
                            println!("{line}");
                        }
                    }
                }
            }

            std::io::stdout().flush()?;
            for ticket in &window_tickets {
                store.apply_ack(ticket, &kernels)?;
            }
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

        total_scan_point_gets = total_scan_point_gets.saturating_add(store.scan_point_gets());
    }

    // Emit total point-read count on stderr so binary-level integration tests can verify
    // that offline drain replays tickets and scans windows with zero point lookups.
    eprintln!("drain: scan_point_gets={total_scan_point_gets}");

    Ok(())
}
