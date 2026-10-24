//! Dedicated per-shard storage worker thread.
//!
//! Executes asynchronous I/O commands (flushes, ticket persistence, acks, range queries, deletions)
//! off the hot ingestion path.

use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::SyncSender;
use std::thread::JoinHandle;

use futures_channel::mpsc::UnboundedReceiver;

use crate::codec::ticket::{EmitReason, Ticket};
use crate::emission::SliceRecord;
use crate::error::{AggError, AggResult};
use crate::key::StorageKey24;
use crate::l1::L1Buffer;
use crate::simd::SimdKernelTable;
use crate::store::shard_store::{FlushBatch, FlushStats, JoinedRow, ShardStore};

/// Command sent to the shard storage thread.
#[allow(clippy::manual_non_exhaustive)]
#[derive(Debug)]
pub enum StoreCommand<F> {
    /// Flush accumulated L1 buffer contents to durable storage.
    Flush(L1Buffer<F>),
    /// Persist emission tickets durably before publishing.
    PersistTickets(Vec<Ticket>),
    /// Acknowledge an emitted ticket and update emitted watermarks.
    Ack(Ticket),
    /// Read joined rows for a window, either by explicit keys or sequential scan.
    Rows {
        /// Target window epoch in milliseconds.
        epoch_ms: u64,
        /// Reason for triggering the row read.
        reason: EmitReason,
        /// Resume key boundary (exclusive) for paged scans.
        after: Option<StorageKey24>,
        /// Optional explicit keys to fetch directly.
        keys: Option<Vec<StorageKey24>>,
        /// Maximum number of rows to return in a single page.
        limit: usize,
        /// Optional ticket ID when reading rows for crash recovery re-emission.
        recovery_ticket: Option<u64>,
    },
    /// Delete expired or emitted keys from header and tail keyspaces.
    DeleteKeys {
        /// Target window epoch in milliseconds.
        epoch_ms: u64,
        /// Storage keys to delete.
        keys: Vec<StorageKey24>,
    },
    /// Drop all data and metadata for an expired window epoch.
    DropWindow {
        /// Target window epoch in milliseconds.
        epoch_ms: u64,
        /// Resource IDs to release if no longer referenced.
        release_resources: Vec<u32>,
    },
    /// Atomically spill unacknowledged slice records for a ticket to disk.
    Spill {
        /// Delivery ticket ID being spilled.
        ticket_id: u64,
        /// Slice records to persist to the spill file.
        records: Vec<SliceRecord>,
    },
    /// Read all pending spilled ticket files from disk for replay.
    ReplaySpill,
    /// Remove a previously spilled ticket file from disk after delivery acknowledgment.
    RemoveSpill {
        /// Delivery ticket ID whose spill file should be removed.
        ticket_id: u64,
    },
    /// Pauses the storage thread for synchronization testing.
    #[doc(hidden)]
    TestPause(std::sync::Arc<std::sync::Barrier>),
    /// Injects a simulated ticket persistence failure on the next `PersistTickets` command.
    #[doc(hidden)]
    FailNextPersist,
    /// Injects a simulated flush failure on the next `Flush` command.
    #[doc(hidden)]
    FailNextFlush,
    /// Injects a simulated row read failure on the next `Rows` command.
    #[doc(hidden)]
    FailNextRows,
    /// Injects a simulated spill write failure on the next `Spill` command.
    #[doc(hidden)]
    FailNextSpill,
    /// Cleanly terminate the storage thread command loop.
    Shutdown,
}

/// Event emitted by the shard storage thread upon command completion or failure.
#[derive(Debug)]
pub enum StoreEvent<F> {
    /// Flush completed and committed durably.
    FlushDone {
        /// Finalizers attached to the flushed L1 buffer.
        finalizers: Vec<F>,
        /// Point-in-time statistics from the flush operation.
        stats: FlushStats,
        /// Current shard database directory size in bytes.
        disk_bytes: u64,
    },
    /// Flush operation failed; returns the error, uncommitted batch, and unacknowledged finalizers.
    FlushFailed {
        /// Storage error encountered during the flush attempt.
        error: AggError,
        /// Uncommitted flush batch so the worker can roll back in-memory state.
        batch: FlushBatch,
        /// Finalizers attached to the failed L1 buffer so upstream can be notified of error.
        finalizers: Vec<F>,
    },
    /// Row scan or key lookup completed.
    Rows {
        /// Target window epoch in milliseconds.
        epoch_ms: u64,
        /// Reason for triggering the row read.
        reason: EmitReason,
        /// Retrieved joined rows.
        rows: Vec<JoinedRow>,
        /// Key cursor for resuming the next scan page, if more rows exist.
        next_after: Option<StorageKey24>,
        /// Optional ticket ID echoed from a crash recovery row lookup.
        recovery_ticket: Option<u64>,
    },
    /// Tickets successfully persisted with `SyncAll` durability.
    TicketsPersisted(Vec<u64>),
    /// Ticket persistence failed; returns the unpersisted ticket IDs and storage error.
    PersistTicketsFailed {
        /// Ticket IDs that failed to persist.
        ticket_ids: Vec<u64>,
        /// Storage error encountered during ticket persistence.
        error: AggError,
    },
    /// Row scan or key lookup failed; returns the window epoch, emission reason, scan parameters, and error.
    RowsFailed {
        /// Target window epoch in milliseconds.
        epoch_ms: u64,
        /// Reason that triggered the row read.
        reason: EmitReason,
        /// Resume key boundary (exclusive) for paged scans.
        after: Option<StorageKey24>,
        /// Optional explicit keys requested for the row read.
        keys: Option<Vec<StorageKey24>>,
        /// Storage error encountered during the row read.
        error: AggError,
        /// Optional ticket ID echoed from a failed crash recovery row lookup.
        recovery_ticket: Option<u64>,
    },
    /// Ticket successfully acknowledged and applied to header state.
    AckApplied {
        /// Acknowledged ticket ID.
        ticket_id: u64,
        /// Keys `(digest, resource_scope_id)` deleted during conditional eviction ack.
        evicted_keys: Vec<(u128, u32)>,
    },
    /// Explicit keys successfully deleted.
    KeysDeleted {
        /// Target window epoch in milliseconds.
        epoch_ms: u64,
        /// Number of keys deleted.
        count: usize,
    },
    /// Window keyspaces successfully dropped.
    WindowDropped(u64),
    /// Disk spill operation (write or removal) completed.
    SpillDone {
        /// Ticket ID whose spill file was written or removed.
        ticket_id: u64,
        /// Updated count of pending `.ndjson` spill files on disk.
        spill_files: usize,
        /// Updated physical disk usage in bytes including the spill directory.
        disk_bytes: u64,
    },
    /// Pending disk spill files loaded for replay.
    SpillReplay {
        /// Pending `(ticket_id, records)` pairs sorted by ticket ID.
        entries: Vec<(u64, Vec<SliceRecord>)>,
        /// Current count of pending `.ndjson` spill files on disk.
        spill_files: usize,
        /// Number of corrupt spill files quarantined during this scan.
        quarantined: u64,
    },
    /// Disk spill operation failed.
    SpillFailed {
        /// Ticket ID associated with the failed spill operation, if applicable.
        ticket_id: Option<u64>,
        /// Error encountered during the spill operation.
        error: AggError,
    },
    /// Storage operation encountered an error.
    Failed(AggError),
}

/// Handle for sending commands to the background shard storage thread.
pub struct StoreHandle<F> {
    tx: SyncSender<StoreCommand<F>>,
    fail_next_flush: Arc<AtomicBool>,
    fail_next_disk_bytes: Arc<AtomicBool>,
    queued: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    scan_point_gets: Arc<AtomicU64>,
}

impl<F> Clone for StoreHandle<F> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            fail_next_flush: Arc::clone(&self.fail_next_flush),
            fail_next_disk_bytes: Arc::clone(&self.fail_next_disk_bytes),
            queued: std::sync::Arc::clone(&self.queued),
            scan_point_gets: Arc::clone(&self.scan_point_gets),
        }
    }
}

impl<F> std::fmt::Debug for StoreHandle<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreHandle").finish()
    }
}

impl<F> StoreHandle<F> {
    /// Sends a storage command to the background shard storage thread.
    ///
    /// # Errors
    /// Returns [`AggError::Channel`] if the worker thread receiver has disconnected.
    pub fn send(&self, cmd: StoreCommand<F>) -> AggResult<()> {
        self.queued
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        self.tx.send(cmd).map_err(|e| {
            self.queued
                .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            AggError::Channel(e.to_string())
        })
    }

    /// Attempts to send a storage command without blocking the calling thread.
    ///
    /// # Errors
    /// Returns `Err(cmd)` with the original command if the bounded queue is full or disconnected.
    #[allow(clippy::result_large_err)]
    pub fn try_send(&self, cmd: StoreCommand<F>) -> Result<(), StoreCommand<F>> {
        self.queued
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        match self.tx.try_send(cmd) {
            Ok(()) => Ok(()),
            Err(
                std::sync::mpsc::TrySendError::Full(cmd)
                | std::sync::mpsc::TrySendError::Disconnected(cmd),
            ) => {
                self.queued
                    .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
                Err(cmd)
            }
        }
    }

    /// Returns the number of commands currently buffered in the storage command queue.
    #[must_use]
    pub fn queue_depth(&self) -> usize {
        self.queued.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Returns the number of random point reads performed by read and scan APIs on the underlying shard.
    ///
    /// AGENTS.md invariant 3 forbids `Keyspace::get` random point reads on window close/drain scans,
    /// and the offline `logsluice drain` over a large PVC backlog turned into per-key random I/O once
    /// (PR #25 review). All keyed reads performed by read/scan APIs go through a counted get choke
    /// point so this counter reflects them; regression tests assert the counter stays 0 for scan paths.
    /// Read-modify-write gets in `apply_flush`/`apply_ack` are deliberately not counted.
    #[must_use]
    pub fn scan_point_gets(&self) -> u64 {
        self.scan_point_gets.load(Ordering::Relaxed)
    }

    /// Arms a one-shot simulated `apply_flush` error on the next `StoreCommand::Flush` processed by this shard.
    #[doc(hidden)]
    pub fn fail_next_flush(&self) {
        self.fail_next_flush.store(true, Ordering::SeqCst);
    }

    /// Arms a one-shot simulated `disk_bytes()` error after the next committed `StoreCommand::Flush` on this shard.
    #[doc(hidden)]
    pub fn fail_next_disk_bytes(&self) {
        self.fail_next_disk_bytes.store(true, Ordering::SeqCst);
    }
}

/// Controller for spawning and managing a per-shard storage worker thread.
pub struct StoreThread;

impl StoreThread {
    /// Spawns a dedicated background thread for shard persistence operations.
    ///
    /// Commands are queued across a bounded channel (capacity 64), and events are published
    /// through an unbounded async futures channel.
    ///
    /// # Errors
    /// Returns [`AggError::Io`] if spawning the thread fails.
    #[allow(clippy::type_complexity)]
    pub fn spawn<F: Send + 'static>(
        store: ShardStore,
        k: SimdKernelTable,
    ) -> AggResult<(
        StoreHandle<F>,
        UnboundedReceiver<StoreEvent<F>>,
        JoinHandle<()>,
    )> {
        let (tx_cmd, rx_cmd) = std::sync::mpsc::sync_channel::<StoreCommand<F>>(64);
        let (tx_event, rx_event) = futures_channel::mpsc::unbounded::<StoreEvent<F>>();
        let shared_fail_next_flush = Arc::new(AtomicBool::new(false));
        let shared_fail_next_disk_bytes = Arc::new(AtomicBool::new(false));
        let thread_fail_next_flush = Arc::clone(&shared_fail_next_flush);
        let thread_fail_next_disk_bytes = Arc::clone(&shared_fail_next_disk_bytes);
        let queued = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let thread_queued = std::sync::Arc::clone(&queued);
        let scan_point_gets = store.scan_point_gets_counter();

        let join_handle = std::thread::Builder::new()
            .name("shard-store-worker".to_string())
            .spawn(move || {
                let spill_writer =
                    crate::worker::spill::SpillWriter::new(store.shard_dir().join("spill"));
                let last_disk_bytes = store.disk_bytes().unwrap_or(0);
                let mut worker = StoreWorker {
                    store,
                    k,
                    spill_writer,
                    fail_next_flush: false,
                    fail_next_persist: false,
                    fail_next_rows: false,
                    fail_next_spill: false,
                    last_disk_bytes,
                    thread_fail_next_flush,
                    thread_fail_next_disk_bytes,
                    tx_event,
                };
                while let Ok(cmd) = rx_cmd.recv() {
                    thread_queued.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
                    if worker.dispatch(cmd).is_break() {
                        break;
                    }
                }
            })
            .map_err(AggError::Io)?;

        Ok((
            StoreHandle {
                tx: tx_cmd,
                fail_next_flush: shared_fail_next_flush,
                fail_next_disk_bytes: shared_fail_next_disk_bytes,
                queued,
                scan_point_gets,
            },
            rx_event,
            join_handle,
        ))
    }
}

/// The thread loop used to own both channel lifetime and every command.
/// `dispatch` is one command so a new `StoreCommand` variant does not grow `spawn`.
///
/// State owned by one shard storage thread.
struct StoreWorker<F> {
    store: ShardStore,
    k: SimdKernelTable,
    spill_writer: crate::worker::spill::SpillWriter,
    fail_next_flush: bool,
    fail_next_persist: bool,
    fail_next_rows: bool,
    fail_next_spill: bool,
    last_disk_bytes: u64,
    thread_fail_next_flush: Arc<AtomicBool>,
    thread_fail_next_disk_bytes: Arc<AtomicBool>,
    tx_event: futures_channel::mpsc::UnboundedSender<StoreEvent<F>>,
}

/// Takes a one-shot flag armed through [`StoreHandle`].
///
/// The relaxed load keeps the production flush path to a shared read; the
/// RMW only runs when a test has armed the flag.
#[inline]
fn take_armed(flag: &AtomicBool) -> bool {
    flag.load(Ordering::Relaxed) && flag.swap(false, Ordering::AcqRel)
}

impl<F> StoreWorker<F> {
    /// Returns [`ControlFlow::Break`] when the command is [`StoreCommand::Shutdown`].
    fn dispatch(&mut self, cmd: StoreCommand<F>) -> ControlFlow<()> {
        let Self {
            store,
            k,
            spill_writer,
            fail_next_flush,
            fail_next_persist,
            fail_next_rows,
            fail_next_spill,
            last_disk_bytes,
            thread_fail_next_flush,
            thread_fail_next_disk_bytes,
            tx_event,
        } = self;
        match cmd {
            StoreCommand::Flush(buf) => {
                let (batch, finalizers) = buf.into_flush();
                let local_fail = std::mem::take(fail_next_flush);
                let handle_fail = take_armed(thread_fail_next_flush);
                let flush_res = if local_fail || handle_fail {
                    Err(AggError::Channel("injected Flush failure".into()))
                } else {
                    store.apply_flush_ref(&batch, k)
                };
                match flush_res {
                    Ok(stats) => {
                        let fail_disk = take_armed(thread_fail_next_disk_bytes);
                        if !fail_disk && let Ok(b) = store.disk_bytes() {
                            *last_disk_bytes = b;
                        }
                        let _ = tx_event.unbounded_send(StoreEvent::FlushDone {
                            finalizers,
                            stats,
                            disk_bytes: *last_disk_bytes,
                        });
                    }
                    Err(e) => {
                        let _ = tx_event.unbounded_send(StoreEvent::FlushFailed {
                            error: e,
                            batch,
                            finalizers,
                        });
                    }
                }
            }
            StoreCommand::PersistTickets(tickets) => {
                let ticket_ids: Vec<u64> = tickets.iter().map(|t| t.id).collect();
                if std::mem::take(fail_next_persist) {
                    let _ = tx_event.unbounded_send(StoreEvent::PersistTicketsFailed {
                        ticket_ids,
                        error: AggError::Channel("injected PersistTickets failure".into()),
                    });
                    return ControlFlow::Continue(());
                }
                match store.put_tickets(&tickets) {
                    Ok(()) => {
                        let _ = tx_event.unbounded_send(StoreEvent::TicketsPersisted(ticket_ids));
                    }
                    Err(error) => {
                        let _ = tx_event
                            .unbounded_send(StoreEvent::PersistTicketsFailed { ticket_ids, error });
                    }
                }
            }
            StoreCommand::Ack(ticket) => {
                let ticket_id = ticket.id;
                match store.apply_ack(&ticket, k) {
                    Ok(evicted_keys) => {
                        let _ = tx_event.unbounded_send(StoreEvent::AckApplied {
                            ticket_id,
                            evicted_keys,
                        });
                    }
                    Err(e) => {
                        let _ = tx_event.unbounded_send(StoreEvent::Failed(e));
                    }
                }
            }
            StoreCommand::Rows {
                epoch_ms,
                reason,
                after,
                keys,
                limit,
                recovery_ticket,
            } => {
                if std::mem::take(fail_next_rows) {
                    let _ = tx_event.unbounded_send(StoreEvent::RowsFailed {
                        epoch_ms,
                        reason,
                        after,
                        keys,
                        error: AggError::Channel("injected Rows failure".into()),
                        recovery_ticket,
                    });
                    return ControlFlow::Continue(());
                }
                if let Some(ref ks) = keys {
                    match store.rows_for_sorted(ks) {
                        Ok(rows) => {
                            let _ = tx_event.unbounded_send(StoreEvent::Rows {
                                epoch_ms,
                                reason,
                                rows,
                                next_after: None,
                                recovery_ticket,
                            });
                        }
                        Err(error) => {
                            let _ = tx_event.unbounded_send(StoreEvent::RowsFailed {
                                epoch_ms,
                                reason,
                                after,
                                keys,
                                error,
                                recovery_ticket,
                            });
                        }
                    }
                } else {
                    match store.rows(epoch_ms, after, limit) {
                        Ok(rows) => {
                            let next_after = if rows.len() == limit {
                                rows.last().map(|r| r.key)
                            } else {
                                None
                            };
                            let _ = tx_event.unbounded_send(StoreEvent::Rows {
                                epoch_ms,
                                reason,
                                rows,
                                next_after,
                                recovery_ticket,
                            });
                        }
                        Err(error) => {
                            let _ = tx_event.unbounded_send(StoreEvent::RowsFailed {
                                epoch_ms,
                                reason,
                                after,
                                keys,
                                error,
                                recovery_ticket,
                            });
                        }
                    }
                }
            }
            StoreCommand::DeleteKeys { epoch_ms, keys } => {
                let count = keys.len();
                match store.delete_keys(epoch_ms, &keys) {
                    Ok(()) => {
                        let _ =
                            tx_event.unbounded_send(StoreEvent::KeysDeleted { epoch_ms, count });
                    }
                    Err(e) => {
                        let _ = tx_event.unbounded_send(StoreEvent::Failed(e));
                    }
                }
            }
            StoreCommand::DropWindow {
                epoch_ms,
                release_resources,
            } => {
                let res = store.drop_window(epoch_ms).and_then(|()| {
                    if release_resources.is_empty() {
                        Ok(())
                    } else {
                        store.delete_resources(&release_resources)
                    }
                });
                match res {
                    Ok(()) => {
                        let _ = tx_event.unbounded_send(StoreEvent::WindowDropped(epoch_ms));
                    }
                    Err(e) => {
                        let _ = tx_event.unbounded_send(StoreEvent::Failed(e));
                    }
                }
            }
            StoreCommand::Spill { ticket_id, records } => {
                if std::mem::take(fail_next_spill) {
                    let _ = tx_event.unbounded_send(StoreEvent::SpillFailed {
                        ticket_id: Some(ticket_id),
                        error: AggError::Channel("injected Spill failure".into()),
                    });
                    return ControlFlow::Continue(());
                }
                match spill_writer.spill(ticket_id, &records) {
                    Ok(()) => {
                        let spill_files = spill_writer.count_files();
                        if let Ok(bytes) = store.disk_bytes() {
                            *last_disk_bytes = bytes;
                        }
                        let _ = tx_event.unbounded_send(StoreEvent::SpillDone {
                            ticket_id,
                            spill_files,
                            disk_bytes: *last_disk_bytes,
                        });
                    }
                    Err(error) => {
                        let _ = tx_event.unbounded_send(StoreEvent::SpillFailed {
                            ticket_id: Some(ticket_id),
                            error,
                        });
                    }
                }
            }
            StoreCommand::ReplaySpill => match spill_writer.pending_with_quarantined() {
                Ok((entries, quarantined)) => {
                    let spill_files = spill_writer.count_files();
                    let _ = tx_event.unbounded_send(StoreEvent::SpillReplay {
                        entries,
                        spill_files,
                        quarantined,
                    });
                }
                Err(error) => {
                    let _ = tx_event.unbounded_send(StoreEvent::SpillFailed {
                        ticket_id: None,
                        error,
                    });
                }
            },
            StoreCommand::RemoveSpill { ticket_id } => match spill_writer.remove(ticket_id) {
                Ok(()) => {
                    let spill_files = spill_writer.count_files();
                    if let Ok(bytes) = store.disk_bytes() {
                        *last_disk_bytes = bytes;
                    }
                    let _ = tx_event.unbounded_send(StoreEvent::SpillDone {
                        ticket_id,
                        spill_files,
                        disk_bytes: *last_disk_bytes,
                    });
                }
                Err(error) => {
                    let _ = tx_event.unbounded_send(StoreEvent::SpillFailed {
                        ticket_id: Some(ticket_id),
                        error,
                    });
                }
            },
            StoreCommand::TestPause(barrier) => {
                barrier.wait();
                barrier.wait();
            }
            StoreCommand::FailNextPersist => {
                *fail_next_persist = true;
            }
            StoreCommand::FailNextFlush => {
                *fail_next_flush = true;
            }
            StoreCommand::FailNextRows => {
                *fail_next_rows = true;
            }
            StoreCommand::FailNextSpill => {
                *fail_next_spill = true;
            }
            StoreCommand::Shutdown => {
                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(())
    }
}
