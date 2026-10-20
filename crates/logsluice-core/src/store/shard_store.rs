//! Shard-level persistence engine wrapping a single `fjall::Database` instance.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fjall::{
    CompressionType, Database, Keyspace, KeyspaceCreateOptions, PersistMode,
    config::{BlockSizePolicy, CompressionPolicy, PinningPolicy},
};
use hashbrown::HashMap;
use serde::{Deserialize, Serialize};

pub use crate::codec::plan::HeaderLayout;
use crate::codec::{
    header::NumericHeader,
    tail::ExemplarTail,
    ticket::{EmitReason, Ticket},
};
use crate::error::{AggError, AggResult};
use crate::key::StorageKey24;
use crate::resource::ResourceScopePayload;
use crate::simd::SimdKernelTable;
use crate::store::meta::PodMeta;

/// Storage engine tuning options per shard.
#[derive(Clone, Debug)]
pub struct StoreOptions {
    /// Directory containing this shard's fjall LSM database files.
    pub shard_dir: PathBuf,
    /// Shared block cache size in bytes across all keyspaces.
    pub cache_bytes: u64,
    /// Memtable capacity limit in bytes for header keyspaces.
    pub hdr_memtable_bytes: u64,
    /// Memtable capacity limit in bytes for exemplar tail keyspaces.
    pub tail_memtable_bytes: u64,
    /// Whether to force `SyncAll` WAL durability on flush commits.
    pub sync_wal: bool,
    /// Active numeric header layout describing banks and min/max slots.
    pub layout: HeaderLayout,
    /// Background compaction and flush worker threads for this shard's fjall database.
    pub worker_threads: usize,
}

/// A single key aggregation delta to be committed into a shard.
#[derive(Clone, Debug)]
pub struct FlushEntry {
    /// 24-byte storage key containing window epoch and 128-bit digest.
    pub key: StorageKey24,
    /// `true` if this key was freshly allocated and does not require read-modify-write.
    pub is_new: bool,
    /// Shard-local interned identifier for resource/scope attributes.
    pub resource_scope_id: u32,
    /// Monotonic delta counter banks to accumulate.
    pub delta: Vec<[u64; 4]>,
    /// Running min/max slot pairs `(min, max)` to merge.
    pub minmax: Vec<(i64, i64)>,
    /// Seconds since window start when this key was updated.
    pub last_update_s: u32,
    /// Optional exemplar log message body and tail attributes.
    pub tail: Option<ExemplarTail>,
}

/// A batch of aggregation entries, offsets, and resource definitions to commit atomically.
#[derive(Clone, Debug)]
pub struct FlushBatch {
    /// Aggregate key entries to update or insert.
    pub entries: Vec<FlushEntry>,
    /// Topic-partition offset markers to record atomically with aggregate state.
    pub offsets: Vec<((Arc<str>, i32), i64)>,
    /// Newly interned resource/scope payloads.
    pub new_resources: Vec<(u32, Arc<ResourceScopePayload>)>,
    /// References associating a resource ID with an active window epoch.
    pub resource_refs: Vec<(u32, u64)>,
}

/// Point-in-time statistics returned after executing an `apply_flush` transaction.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FlushStats {
    /// Number of header reads performed for existing keys.
    pub hdr_reads: u64,
    /// Number of header inserts/updates committed.
    pub hdr_writes: u64,
    /// Number of exemplar tail records committed.
    pub tail_writes: u64,
}

/// Merged aggregate record combining the fixed-width numeric header and optional exemplar tail.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinedRow {
    /// 24-byte composite storage key.
    pub key: StorageKey24,
    /// Deserialized numeric header.
    pub header: NumericHeader,
    /// Optional exemplar tail.
    pub tail: Option<ExemplarTail>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredResource {
    payload: ResourceScopePayload,
    windows: Vec<u64>,
}

/// Shard storage controller managing header, tail, offset, ticket, and metadata keyspaces.
pub struct ShardStore {
    db: Database,
    opts: StoreOptions,
    #[allow(dead_code)]
    meta_ks: Keyspace,
    offsets_ks: Keyspace,
    tickets_ks: Keyspace,
    resources_ks: Keyspace,
    hdr_keyspaces: HashMap<u64, Keyspace>,
    tail_keyspaces: HashMap<u64, Keyspace>,
    fail_drop_after_tail: bool,
}

impl ShardStore {
    /// Opens or recovers a `ShardStore` at `opts.shard_dir`, cross-checking metadata against `meta`.
    ///
    /// # Errors
    /// Returns [`AggError::Config`] if `opts.worker_threads == 0`, [`AggError::MetaMismatch`] if stored
    /// metadata does not match configured shards, salt, codec schema, window duration, or window offset,
    /// or [`AggError::Storage`] if database opening fails.
    pub fn open(opts: StoreOptions, meta: &PodMeta) -> AggResult<Self> {
        if opts.worker_threads == 0 {
            return Err(AggError::Config(
                "storage.worker_threads must be >= 1".to_string(),
            ));
        }

        let db = Database::builder(&opts.shard_dir)
            .cache_size(opts.cache_bytes)
            .worker_threads(opts.worker_threads)
            .open()?;

        let meta_ks = db.keyspace("_meta", KeyspaceCreateOptions::default)?;
        let offsets_ks = db.keyspace("_offsets", KeyspaceCreateOptions::default)?;
        let tickets_ks = db.keyspace("_tickets", KeyspaceCreateOptions::default)?;
        let resources_ks = db.keyspace("_resources", KeyspaceCreateOptions::default)?;

        if let Some(slice) = meta_ks.get(b"pod_meta")? {
            let stored: PodMeta =
                serde_json::from_slice(&slice).map_err(|e| AggError::Codec(e.to_string()))?;
            if stored.shards != meta.shards {
                return Err(AggError::MetaMismatch {
                    field: "shards",
                    persisted: stored.shards.to_string(),
                    configured: meta.shards.to_string(),
                });
            }
            if stored.salt != meta.salt {
                return Err(AggError::MetaMismatch {
                    field: "salt",
                    persisted: stored.salt.to_string(),
                    configured: meta.salt.to_string(),
                });
            }
            if stored.codec_schema != meta.codec_schema {
                return Err(AggError::MetaMismatch {
                    field: "codec_schema",
                    persisted: format!("{:02x?}", stored.codec_schema),
                    configured: format!("{:02x?}", meta.codec_schema),
                });
            }
            if stored.window_duration_ms != meta.window_duration_ms {
                return Err(AggError::MetaMismatch {
                    field: "window_duration_ms",
                    persisted: stored.window_duration_ms.to_string(),
                    configured: meta.window_duration_ms.to_string(),
                });
            }
            if stored.window_offset_ms != meta.window_offset_ms {
                return Err(AggError::MetaMismatch {
                    field: "window_offset_ms",
                    persisted: stored.window_offset_ms.to_string(),
                    configured: meta.window_offset_ms.to_string(),
                });
            }
        } else {
            let data = serde_json::to_vec(meta).map_err(|e| AggError::Codec(e.to_string()))?;
            meta_ks.insert(b"pod_meta", data)?;
        }

        let mut hdr_keyspaces = HashMap::new();
        let mut tail_keyspaces = HashMap::new();

        let keyspace_names = db.list_keyspace_names();
        for name in &keyspace_names {
            if let Some(epoch) = parse_hdr_epoch(name) {
                let mem = opts.hdr_memtable_bytes;
                let ks = db.keyspace(name, || hdr_create_options(mem))?;
                hdr_keyspaces.insert(epoch, ks);
            }
        }

        let mut removed_orphan_tail = false;
        for name in &keyspace_names {
            if let Some(epoch) = parse_tail_epoch(name) {
                let mem = opts.tail_memtable_bytes;
                let ks = db.keyspace(name, || tail_create_options(mem))?;
                if hdr_keyspaces.contains_key(&epoch) {
                    tail_keyspaces.insert(epoch, ks);
                } else {
                    db.delete_keyspace(ks)?;
                    removed_orphan_tail = true;
                }
            }
        }

        if removed_orphan_tail {
            db.persist(PersistMode::SyncAll)?;
        }

        Ok(Self {
            db,
            opts,
            meta_ks,
            offsets_ks,
            tickets_ks,
            resources_ks,
            hdr_keyspaces,
            tail_keyspaces,
            fail_drop_after_tail: false,
        })
    }

    /// Atomically commits a batch of flush entries, consumer offsets, and resource records.
    ///
    /// # Errors
    /// Returns [`AggError::Storage`] or [`AggError::Codec`] if batch encoding or commit fails.
    pub fn apply_flush(&mut self, batch: FlushBatch, k: &SimdKernelTable) -> AggResult<FlushStats> {
        self.apply_flush_ref(&batch, k)
    }

    /// Atomically commits a referenced batch of flush entries, consumer offsets, and resource records.
    ///
    /// # Errors
    /// Returns [`AggError::Storage`] or [`AggError::Codec`] if batch encoding or commit fails.
    pub fn apply_flush_ref(
        &mut self,
        batch: &FlushBatch,
        k: &SimdKernelTable,
    ) -> AggResult<FlushStats> {
        let mut write_batch = self.db.batch();
        let mut stats = FlushStats::default();

        for entry in &batch.entries {
            let hdr_ks = self.get_or_create_hdr_keyspace(entry.key.epoch_ms)?;
            let key_bytes = entry.key.to_bytes();

            if entry.is_new {
                let mut hdr = NumericHeader::new_zeroed(self.opts.layout, entry.resource_scope_id);
                hdr.last_update_s = entry.last_update_s;
                (k.accumulate_counter_banks)(&mut hdr.total, &entry.delta);

                for (slot_idx, &(min_v, max_v)) in entry.minmax.iter().enumerate() {
                    if slot_idx < hdr.minmax.len() {
                        hdr.minmax[slot_idx] = (min_v, max_v);
                    }
                }

                let encoded_hdr = hdr.encode(self.opts.layout);
                write_batch.insert(&hdr_ks, key_bytes, encoded_hdr);
                stats.hdr_writes += 1;

                if let Some(ref tail) = entry.tail {
                    let tail_ks = self.get_or_create_tail_keyspace(entry.key.epoch_ms)?;
                    write_batch.insert(&tail_ks, key_bytes, tail.encode());
                    stats.tail_writes += 1;
                }
            } else {
                stats.hdr_reads += 1;
                let (mut hdr, had_existing_hdr) = match hdr_ks.get(key_bytes)? {
                    Some(slice) => (NumericHeader::decode(self.opts.layout, &slice)?, true),
                    None => {
                        let mut zeroed =
                            NumericHeader::new_zeroed(self.opts.layout, entry.resource_scope_id);
                        for (slot_idx, &(min_v, max_v)) in entry.minmax.iter().enumerate() {
                            if slot_idx < zeroed.minmax.len() {
                                zeroed.minmax[slot_idx] = (min_v, max_v);
                            }
                        }
                        (zeroed, false)
                    }
                };

                hdr.last_update_s = hdr.last_update_s.max(entry.last_update_s);
                (k.accumulate_counter_banks)(&mut hdr.total, &entry.delta);

                for (slot_idx, &(min_v, max_v)) in entry.minmax.iter().enumerate() {
                    if slot_idx < hdr.minmax.len() {
                        let (cur_min, cur_max) = hdr.minmax[slot_idx];
                        hdr.minmax[slot_idx] = (cur_min.min(min_v), cur_max.max(max_v));
                    }
                }

                let encoded_hdr = hdr.encode(self.opts.layout);
                write_batch.insert(&hdr_ks, key_bytes, encoded_hdr);
                stats.hdr_writes += 1;

                if !had_existing_hdr && let Some(ref tail) = entry.tail {
                    let tail_ks = self.get_or_create_tail_keyspace(entry.key.epoch_ms)?;
                    write_batch.insert(&tail_ks, key_bytes, tail.encode());
                    stats.tail_writes += 1;
                }
            }
        }

        for ((topic, partition), offset) in &batch.offsets {
            let key = encode_offset_key(topic, *partition);
            let val = offset.to_le_bytes();
            write_batch.insert(&self.offsets_ks, key, val);
        }

        let mut pending_resources: HashMap<u32, (ResourceScopePayload, BTreeSet<u64>)> =
            HashMap::new();

        for (id, payload) in &batch.new_resources {
            pending_resources.insert(*id, ((**payload).clone(), BTreeSet::new()));
        }

        for &(id, window_epoch) in &batch.resource_refs {
            if let Some((_payload, windows)) = pending_resources.get_mut(&id) {
                windows.insert(window_epoch);
            } else {
                let key = id.to_be_bytes();
                if let Some(slice) = self.resources_ks.get(key)? {
                    let stored: StoredResource = serde_json::from_slice(&slice)
                        .map_err(|e| AggError::Codec(e.to_string()))?;
                    let mut windows: BTreeSet<u64> = stored.windows.into_iter().collect();
                    windows.insert(window_epoch);
                    pending_resources.insert(id, (stored.payload, windows));
                }
            }
        }

        for (id, (payload, windows)) in pending_resources {
            let stored = StoredResource {
                payload,
                windows: windows.into_iter().collect(),
            };
            let val = serde_json::to_vec(&stored).map_err(|e| AggError::Codec(e.to_string()))?;
            write_batch.insert(&self.resources_ks, id.to_be_bytes(), val);
        }

        let mode = if self.opts.sync_wal {
            PersistMode::SyncAll
        } else {
            PersistMode::Buffer
        };

        write_batch.durability(Some(mode)).commit()?;
        Ok(stats)
    }

    /// Looks up a numeric header by storage key.
    ///
    /// # Errors
    /// Returns [`AggError::Storage`] or [`AggError::Codec`].
    pub fn header(&self, key: &StorageKey24) -> AggResult<Option<NumericHeader>> {
        let hdr_ks = match self.get_hdr_keyspace(key.epoch_ms) {
            Some(ks) => ks,
            None => return Ok(None),
        };

        let slice = match hdr_ks.get(key.to_bytes())? {
            Some(s) => s,
            None => return Ok(None),
        };

        let header = NumericHeader::decode(self.opts.layout, &slice)?;
        Ok(Some(header))
    }

    /// Looks up an exemplar tail by storage key.
    ///
    /// # Errors
    /// Returns [`AggError::Storage`] or [`AggError::Codec`].
    pub fn tail(&self, key: &StorageKey24) -> AggResult<Option<ExemplarTail>> {
        let tail_ks = match self.get_tail_keyspace(key.epoch_ms) {
            Some(ks) => ks,
            None => return Ok(None),
        };

        let slice = match tail_ks.get(key.to_bytes())? {
            Some(s) => s,
            None => return Ok(None),
        };

        let tail = ExemplarTail::decode(&slice)?;
        Ok(Some(tail))
    }

    /// Performs a key-ordered merge join of header and tail keyspaces for window `epoch_ms`.
    ///
    /// Scans keys strictly after `after` up to `limit` rows using lockstep forward range iterators.
    ///
    /// # Errors
    /// Returns [`AggError::Storage`] or [`AggError::Codec`].
    pub fn rows(
        &self,
        epoch_ms: u64,
        after: Option<StorageKey24>,
        limit: usize,
    ) -> AggResult<Vec<JoinedRow>> {
        let hdr_ks = match self.get_hdr_keyspace(epoch_ms) {
            Some(ks) => ks,
            None => return Ok(Vec::new()),
        };
        let tail_ks = self.get_tail_keyspace(epoch_ms);

        let mut results = Vec::with_capacity(limit.min(64));

        let iter = match after {
            Some(after_key) => hdr_ks.range((
                std::ops::Bound::Excluded(after_key.to_bytes()),
                std::ops::Bound::Unbounded,
            )),
            None => hdr_ks.iter(),
        };

        let mut tail_iter = tail_ks.as_ref().map(|t_ks| match after {
            Some(after_key) => t_ks.range((
                std::ops::Bound::Excluded(after_key.to_bytes()),
                std::ops::Bound::Unbounded,
            )),
            None => t_ks.iter(),
        });
        let mut next_tail: Option<(fjall::Slice, fjall::Slice)> = None;

        for item in iter {
            if results.len() >= limit {
                break;
            }
            let (k_bytes, v_bytes) = item.into_inner()?;
            let key = StorageKey24::from_bytes(&k_bytes)?;
            let header = NumericHeader::decode(self.opts.layout, &v_bytes)?;

            let mut tail = None;
            if let Some(ref mut t_iter) = tail_iter {
                loop {
                    if next_tail.is_none() {
                        if let Some(t_guard) = t_iter.next() {
                            next_tail = Some(t_guard.into_inner()?);
                        } else {
                            break;
                        }
                    }
                    if let Some((ref tk, _)) = next_tail {
                        match tk.as_ref().cmp(k_bytes.as_ref()) {
                            std::cmp::Ordering::Less => {
                                next_tail = None;
                            }
                            std::cmp::Ordering::Equal => {
                                if let Some((_, tv)) = next_tail.take() {
                                    tail = Some(ExemplarTail::decode(&tv)?);
                                }
                                break;
                            }
                            std::cmp::Ordering::Greater => {
                                break;
                            }
                        }
                    }
                }
            }

            results.push(JoinedRow { key, header, tail });
        }

        Ok(results)
    }

    /// Looks up multiple keys in sorted order, performing merge joins on existing records.
    ///
    /// # Errors
    /// Returns [`AggError::Storage`] or [`AggError::Codec`].
    pub fn rows_for(&self, keys: &[StorageKey24]) -> AggResult<Vec<JoinedRow>> {
        let mut results = Vec::with_capacity(keys.len());
        let mut cached_epoch: Option<(u64, Option<Keyspace>, Option<Keyspace>)> = None;

        for key in keys {
            let needs_refresh = match cached_epoch {
                Some((ep, _, _)) => ep != key.epoch_ms,
                None => true,
            };
            if needs_refresh {
                cached_epoch = Some((
                    key.epoch_ms,
                    self.get_hdr_keyspace(key.epoch_ms),
                    self.get_tail_keyspace(key.epoch_ms),
                ));
            }

            if let Some((_, ref hdr_ks_opt, ref tail_ks_opt)) = cached_epoch {
                let Some(ref hdr_ks) = *hdr_ks_opt else {
                    continue;
                };
                let key_bytes = key.to_bytes();
                let Some(hdr_slice) = hdr_ks.get(key_bytes)? else {
                    continue;
                };
                let header = NumericHeader::decode(self.opts.layout, &hdr_slice)?;
                let tail = match *tail_ks_opt {
                    Some(ref tail_ks) => match tail_ks.get(key_bytes)? {
                        Some(tail_slice) => Some(ExemplarTail::decode(&tail_slice)?),
                        None => None,
                    },
                    None => None,
                };
                results.push(JoinedRow {
                    key: *key,
                    header,
                    tail,
                });
            }
        }
        Ok(results)
    }

    /// Returns all `(digest, resource_scope_id)` pairs indexed for the given window epoch.
    ///
    /// # Errors
    /// Returns [`AggError::Storage`] or [`AggError::Codec`].
    pub fn window_keys(&self, epoch_ms: u64) -> AggResult<Vec<(u128, u32)>> {
        let hdr_ks = match self.get_hdr_keyspace(epoch_ms) {
            Some(ks) => ks,
            None => return Ok(Vec::new()),
        };

        let mut results = Vec::new();
        for item in hdr_ks.iter() {
            let (k_bytes, v_bytes) = item.into_inner()?;
            let key = StorageKey24::from_bytes(&k_bytes)?;
            if v_bytes.len() < 4 {
                return Err(AggError::Codec(
                    "NumericHeader too short for resource_scope_id".into(),
                ));
            }
            let mut arr = [0u8; 4];
            arr.copy_from_slice(&v_bytes[..4]);
            let scope_id = u32::from_le_bytes(arr);
            results.push((key.digest, scope_id));
        }

        Ok(results)
    }

    /// Persists emission intent tickets with `SyncAll` durability.
    ///
    /// # Errors
    /// Returns [`AggError::Storage`].
    pub fn put_tickets(&mut self, tickets: &[Ticket]) -> AggResult<()> {
        let mut batch = self.db.batch();
        for t in tickets {
            let key = t.id.to_be_bytes();
            let val = t.encode(self.opts.layout);
            batch.insert(&self.tickets_ks, key, val);
        }
        batch.durability(Some(PersistMode::SyncAll)).commit()?;
        Ok(())
    }

    /// Acknowledges an emission ticket, incrementing `slice_seq` and updating `emitted` counters.
    ///
    /// When `ticket.reason == EmitReason::Evict`, deletes the key from both header and tail
    /// keyspaces if and only if `stored.total == stored.emitted` after applying the delta,
    /// returning the `(digest, resource_scope_id)` pairs of all keys that were deleted.
    ///
    /// # Errors
    /// Returns [`AggError::Storage`] or [`AggError::Codec`].
    pub fn apply_ack(
        &mut self,
        ticket: &Ticket,
        k: &SimdKernelTable,
    ) -> AggResult<Vec<(u128, u32)>> {
        let mut batch = self.db.batch();
        let epoch_ms = ticket.window_epoch_ms;
        let hdr_ks = self.get_or_create_hdr_keyspace(epoch_ms)?;
        let tail_ks = self.get_tail_keyspace(epoch_ms);
        let mut evicted_keys: Vec<(u128, u32)> = Vec::new();

        for entry in &ticket.entries {
            let key = StorageKey24 {
                epoch_ms,
                digest: entry.digest,
            };
            let key_bytes = key.to_bytes();
            if let Some(slice) = hdr_ks.get(key_bytes)? {
                let mut stored = NumericHeader::decode(self.opts.layout, &slice)?;
                if stored.slice_seq == entry.slice_seq {
                    (k.accumulate_counter_banks)(&mut stored.emitted, &entry.delta);
                    stored.slice_seq = stored.slice_seq.wrapping_add(1);
                    if ticket.reason == EmitReason::Evict && stored.total == stored.emitted {
                        batch.remove(&hdr_ks, key_bytes);
                        if let Some(ref t_ks) = tail_ks {
                            batch.remove(t_ks, key_bytes);
                        }
                        evicted_keys.push((entry.digest, stored.resource_scope_id));
                    } else {
                        let val = stored.encode(self.opts.layout);
                        batch.insert(&hdr_ks, key_bytes, val);
                    }
                }
            }
        }

        batch.remove(&self.tickets_ks, ticket.id.to_be_bytes());
        batch.durability(Some(PersistMode::Buffer)).commit()?;
        Ok(evicted_keys)
    }

    /// Returns all pending emission tickets stored in `_tickets`.
    ///
    /// # Errors
    /// Returns [`AggError::Storage`] or [`AggError::Codec`].
    pub fn tickets(&self) -> AggResult<Vec<Ticket>> {
        let mut result = Vec::new();
        for item in self.tickets_ks.iter() {
            let (_k, v) = item.into_inner()?;
            let t = Ticket::decode(self.opts.layout, &v)?;
            result.push(t);
        }
        Ok(result)
    }

    /// Returns all persisted stream offsets from `_offsets`.
    ///
    /// # Errors
    /// Returns [`AggError::Storage`] or [`AggError::Codec`].
    pub fn offsets(&self) -> AggResult<HashMap<(Arc<str>, i32), i64>> {
        let mut result = HashMap::new();
        for item in self.offsets_ks.iter() {
            let (k, v) = item.into_inner()?;
            if k.len() < 8 || v.len() != 8 {
                continue;
            }
            let mut len_arr = [0u8; 4];
            len_arr.copy_from_slice(&k[0..4]);
            let topic_len = u32::from_le_bytes(len_arr) as usize;
            if k.len() != 4 + topic_len + 4 {
                continue;
            }
            let topic = std::str::from_utf8(&k[4..4 + topic_len])
                .map_err(|e| AggError::Codec(e.to_string()))?;
            let mut part_arr = [0u8; 4];
            part_arr.copy_from_slice(&k[4 + topic_len..4 + topic_len + 4]);
            let partition = i32::from_le_bytes(part_arr);

            let mut off_arr = [0u8; 8];
            off_arr.copy_from_slice(&v[..8]);
            let offset = i64::from_le_bytes(off_arr);

            result.insert((Arc::from(topic), partition), offset);
        }
        Ok(result)
    }

    /// Returns all interned resource scope payloads and referencing windows.
    ///
    /// # Errors
    /// Returns [`AggError::Storage`] or [`AggError::Codec`].
    pub fn resources(&self) -> AggResult<Vec<(u32, ResourceScopePayload, Vec<u64>)>> {
        let mut result = Vec::new();
        for item in self.resources_ks.iter() {
            let (k, v) = item.into_inner()?;
            if k.len() != 4 {
                continue;
            }
            let mut id_arr = [0u8; 4];
            id_arr.copy_from_slice(&k[..4]);
            let id = u32::from_be_bytes(id_arr);
            let stored: StoredResource =
                serde_json::from_slice(&v).map_err(|e| AggError::Codec(e.to_string()))?;
            result.push((id, stored.payload, stored.windows));
        }
        Ok(result)
    }

    /// Deletes the specified resource IDs from `_resources`.
    ///
    /// # Errors
    /// Returns [`AggError::Storage`].
    pub fn delete_resources(&self, ids: &[u32]) -> AggResult<()> {
        let mut batch = self.db.batch();
        for &id in ids {
            batch.remove(&self.resources_ks, id.to_be_bytes());
        }
        batch.durability(Some(PersistMode::Buffer)).commit()?;
        Ok(())
    }

    /// Updates the persisted window references for the specified resources in `_resources`.
    ///
    /// # Errors
    /// Returns [`AggError::Storage`] or [`AggError::Codec`].
    pub fn update_resources(
        &self,
        entries: &[(u32, ResourceScopePayload, Vec<u64>)],
    ) -> AggResult<()> {
        let mut batch = self.db.batch();
        for (id, payload, windows) in entries {
            let stored = StoredResource {
                payload: payload.clone(),
                windows: windows.clone(),
            };
            let val = serde_json::to_vec(&stored).map_err(|e| AggError::Codec(e.to_string()))?;
            batch.insert(&self.resources_ks, id.to_be_bytes(), val);
        }
        batch.durability(Some(PersistMode::Buffer)).commit()?;
        Ok(())
    }

    /// Discovers and returns all window epochs with an active header keyspace.
    #[must_use]
    pub fn live_windows(&self) -> Vec<u64> {
        let mut epochs = Vec::new();
        for name in self.db.list_keyspace_names() {
            if let Some(epoch) = parse_hdr_epoch(&name) {
                epochs.push(epoch);
            }
        }
        epochs.sort_unstable();
        epochs.dedup();
        epochs
    }

    /// Deletes specific keys from both header and tail keyspaces for a window.
    ///
    /// # Errors
    /// Returns [`AggError::Storage`].
    pub fn delete_keys(&mut self, epoch_ms: u64, keys: &[StorageKey24]) -> AggResult<()> {
        let mut batch = self.db.batch();
        let hdr_ks = self.get_hdr_keyspace(epoch_ms);
        let tail_ks = self.get_tail_keyspace(epoch_ms);

        for key in keys {
            let key_bytes = key.to_bytes();
            if let Some(ref ks) = hdr_ks {
                batch.remove(ks, key_bytes);
            }
            if let Some(ref ks) = tail_ks {
                batch.remove(ks, key_bytes);
            }
        }

        let mode = if self.opts.sync_wal {
            PersistMode::SyncAll
        } else {
            PersistMode::Buffer
        };
        batch.durability(Some(mode)).commit()?;
        Ok(())
    }

    /// Drops the tail and header keyspaces for window `epoch_ms`, deleting `_tail` before `_hdr`
    /// so `_hdr` serves as the durable commit marker.
    ///
    /// # Errors
    /// Returns [`AggError::Storage`].
    pub fn drop_window(&mut self, epoch_ms: u64) -> AggResult<()> {
        let hdr_name = format!("w{:020}_hdr", epoch_ms);
        let tail_name = format!("w{:020}_tail", epoch_ms);

        if let Some(ks) = self.tail_keyspaces.remove(&epoch_ms) {
            self.db.delete_keyspace(ks)?;
        } else if self.db.keyspace_exists(&tail_name) {
            let mem = self.opts.tail_memtable_bytes;
            let ks = self.db.keyspace(&tail_name, || tail_create_options(mem))?;
            self.db.delete_keyspace(ks)?;
        }

        if self.fail_drop_after_tail {
            self.fail_drop_after_tail = false;
            return Err(AggError::Io(std::io::Error::other(
                "injected drop_window failure after tail delete",
            )));
        }

        if let Some(ks) = self.hdr_keyspaces.remove(&epoch_ms) {
            self.db.delete_keyspace(ks)?;
        } else if self.db.keyspace_exists(&hdr_name) {
            let mem = self.opts.hdr_memtable_bytes;
            let ks = self.db.keyspace(&hdr_name, || hdr_create_options(mem))?;
            self.db.delete_keyspace(ks)?;
        }

        Ok(())
    }

    /// Returns the sorted list of all keyspace names currently present in the underlying database.
    #[must_use]
    pub fn list_keyspace_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .db
            .list_keyspace_names()
            .into_iter()
            .map(|n| n.to_string())
            .collect();
        names.sort_unstable();
        names
    }

    /// Injects a simulated failure after deleting the tail keyspace in the next `drop_window` call.
    #[doc(hidden)]
    pub fn set_fail_drop_after_tail(&mut self, fail: bool) {
        self.fail_drop_after_tail = fail;
    }

    /// Returns the filesystem directory path for this shard store.
    #[must_use]
    pub fn shard_dir(&self) -> &Path {
        &self.opts.shard_dir
    }

    /// Queries the shard's current disk usage in bytes, combining the LSM database usage via
    /// [`fjall::Database::disk_space`] with any pending spill files in `shard_dir/spill`.
    ///
    /// This accounts for the fjall journal and active keyspace SSTables in O(1) without walking
    /// the LSM directory tree, plus a flat scan of `spill/` if present.
    ///
    /// # Errors
    /// Returns [`AggError::Storage`] if querying database disk space fails.
    pub fn disk_bytes(&self) -> AggResult<u64> {
        Ok(self
            .db
            .disk_space()?
            .saturating_add(crate::worker::spill::spill_dir_bytes(
                &self.opts.shard_dir.join("spill"),
            )))
    }

    fn get_or_create_hdr_keyspace(&mut self, epoch_ms: u64) -> AggResult<Keyspace> {
        if let Some(ks) = self.hdr_keyspaces.get(&epoch_ms) {
            return Ok(ks.clone());
        }
        let name = format!("w{:020}_hdr", epoch_ms);
        let mem = self.opts.hdr_memtable_bytes;
        let ks = self.db.keyspace(&name, || hdr_create_options(mem))?;
        self.hdr_keyspaces.insert(epoch_ms, ks.clone());
        Ok(ks)
    }

    fn get_or_create_tail_keyspace(&mut self, epoch_ms: u64) -> AggResult<Keyspace> {
        if let Some(ks) = self.tail_keyspaces.get(&epoch_ms) {
            return Ok(ks.clone());
        }
        let name = format!("w{:020}_tail", epoch_ms);
        let mem = self.opts.tail_memtable_bytes;
        let ks = self.db.keyspace(&name, || tail_create_options(mem))?;
        self.tail_keyspaces.insert(epoch_ms, ks.clone());
        Ok(ks)
    }

    fn get_hdr_keyspace(&self, epoch_ms: u64) -> Option<Keyspace> {
        if let Some(ks) = self.hdr_keyspaces.get(&epoch_ms) {
            return Some(ks.clone());
        }
        let name = format!("w{:020}_hdr", epoch_ms);
        if self.db.keyspace_exists(&name) {
            let mem = self.opts.hdr_memtable_bytes;
            self.db.keyspace(&name, || hdr_create_options(mem)).ok()
        } else {
            None
        }
    }

    fn get_tail_keyspace(&self, epoch_ms: u64) -> Option<Keyspace> {
        if let Some(ks) = self.tail_keyspaces.get(&epoch_ms) {
            return Some(ks.clone());
        }
        let name = format!("w{:020}_tail", epoch_ms);
        if self.db.keyspace_exists(&name) {
            let mem = self.opts.tail_memtable_bytes;
            self.db.keyspace(&name, || tail_create_options(mem)).ok()
        } else {
            None
        }
    }
}

fn encode_offset_key(topic: &str, partition: i32) -> Vec<u8> {
    let topic_bytes = topic.as_bytes();
    let topic_len = u32::try_from(topic_bytes.len()).unwrap_or(u32::MAX);
    let mut key = Vec::with_capacity(4 + topic_bytes.len() + 4);
    key.extend_from_slice(&topic_len.to_le_bytes());
    key.extend_from_slice(topic_bytes);
    key.extend_from_slice(&partition.to_le_bytes());
    key
}

fn hdr_create_options(max_mem: u64) -> KeyspaceCreateOptions {
    KeyspaceCreateOptions::default()
        .index_block_pinning_policy(PinningPolicy::all(true))
        .filter_block_pinning_policy(PinningPolicy::all(true))
        .expect_point_read_hits(true)
        .data_block_size_policy(BlockSizePolicy::all(4 * 1024))
        .max_memtable_size(max_mem)
}

fn tail_create_options(max_mem: u64) -> KeyspaceCreateOptions {
    KeyspaceCreateOptions::default()
        .data_block_compression_policy(CompressionPolicy::all(CompressionType::Lz4))
        .data_block_size_policy(BlockSizePolicy::all(32 * 1024))
        .max_memtable_size(max_mem)
}

fn parse_tail_epoch(name: &str) -> Option<u64> {
    if name.len() == 26 && name.starts_with('w') && name.ends_with("_tail") {
        name[1..21].parse::<u64>().ok()
    } else {
        None
    }
}

fn parse_hdr_epoch(name: &str) -> Option<u64> {
    if name.len() == 25 && name.starts_with('w') && name.ends_with("_hdr") {
        name[1..21].parse::<u64>().ok()
    } else {
        None
    }
}
