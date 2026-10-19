//! Dedicated per-shard storage worker thread.
//!
//! Executes asynchronous I/O commands (flushes, ticket persistence, acks, range queries, deletions)
//! off the hot ingestion path.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::SyncSender;
use std::thread::JoinHandle;

use futures_channel::mpsc::UnboundedReceiver;

use crate::codec::ticket::{EmitReason, Ticket};
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
    /// Storage operation encountered an error.
    Failed(AggError),
}

/// Handle for sending commands to the background shard storage thread.
pub struct StoreHandle<F> {
    tx: SyncSender<StoreCommand<F>>,
    fail_next_flush: Arc<AtomicBool>,
    fail_next_disk_bytes: Arc<AtomicBool>,
    queued: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl<F> Clone for StoreHandle<F> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            fail_next_flush: Arc::clone(&self.fail_next_flush),
            fail_next_disk_bytes: Arc::clone(&self.fail_next_disk_bytes),
            queued: std::sync::Arc::clone(&self.queued),
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

static FAIL_NEXT_FLUSH: AtomicBool = AtomicBool::new(false);

/// Controller for spawning and managing a per-shard storage worker thread.
pub struct StoreThread;

impl StoreThread {
    /// Arms a one-shot simulated `apply_flush` error on the next `StoreCommand::Flush` processed.
    #[doc(hidden)]
    pub fn fail_next_flush() {
        FAIL_NEXT_FLUSH.store(true, Ordering::SeqCst);
    }

    /// Spawns a dedicated background thread for shard persistence operations.
    ///
    /// Commands are queued across a bounded channel (capacity 64), and events are published
    /// through an unbounded async futures channel.
    ///
    /// # Errors
    /// Returns [`AggError::Io`] if spawning the thread fails.
    #[allow(clippy::type_complexity)]
    pub fn spawn<F: Send + 'static>(
        mut store: ShardStore,
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

        let join_handle = std::thread::Builder::new()
            .name("shard-store-worker".to_string())
            .spawn(move || {
                let mut fail_next_flush = false;
                let mut fail_next_persist = false;
                let mut fail_next_rows = false;
                let mut last_disk_bytes = store.disk_bytes().unwrap_or(0);
                while let Ok(cmd) = rx_cmd.recv() {
                    thread_queued.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
                    match cmd {
                        StoreCommand::Flush(buf) => {
                            let (batch, finalizers) = buf.into_flush();
                            let local_fail = std::mem::replace(&mut fail_next_flush, false);
                            let handle_fail = thread_fail_next_flush.swap(false, Ordering::SeqCst);
                            let global_fail = FAIL_NEXT_FLUSH.swap(false, Ordering::SeqCst);
                            let flush_res = if local_fail || handle_fail || global_fail {
                                Err(AggError::Channel("injected Flush failure".into()))
                            } else {
                                store.apply_flush_ref(&batch, &k)
                            };
                            match flush_res {
                                Ok(stats) => {
                                    let fail_disk =
                                        thread_fail_next_disk_bytes.swap(false, Ordering::SeqCst);
                                    if !fail_disk && let Ok(b) = store.disk_bytes() {
                                        last_disk_bytes = b;
                                    }
                                    let _ = tx_event.unbounded_send(StoreEvent::FlushDone {
                                        finalizers,
                                        stats,
                                        disk_bytes: last_disk_bytes,
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
                            if fail_next_persist {
                                fail_next_persist = false;
                                let _ = tx_event.unbounded_send(StoreEvent::PersistTicketsFailed {
                                    ticket_ids,
                                    error: AggError::Channel(
                                        "injected PersistTickets failure".into(),
                                    ),
                                });
                                continue;
                            }
                            match store.put_tickets(&tickets) {
                                Ok(()) => {
                                    let _ = tx_event
                                        .unbounded_send(StoreEvent::TicketsPersisted(ticket_ids));
                                }
                                Err(error) => {
                                    let _ =
                                        tx_event.unbounded_send(StoreEvent::PersistTicketsFailed {
                                            ticket_ids,
                                            error,
                                        });
                                }
                            }
                        }
                        StoreCommand::Ack(ticket) => {
                            let ticket_id = ticket.id;
                            match store.apply_ack(&ticket, &k) {
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
                            if fail_next_rows {
                                fail_next_rows = false;
                                let _ = tx_event.unbounded_send(StoreEvent::RowsFailed {
                                    epoch_ms,
                                    reason,
                                    after,
                                    keys,
                                    error: AggError::Channel("injected Rows failure".into()),
                                    recovery_ticket,
                                });
                                continue;
                            }
                            if let Some(ref ks) = keys {
                                match store.rows_for(ks) {
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
                                    let _ = tx_event.unbounded_send(StoreEvent::KeysDeleted {
                                        epoch_ms,
                                        count,
                                    });
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
                            let res = (|| -> AggResult<()> {
                                store.drop_window(epoch_ms)?;
                                if !release_resources.is_empty() {
                                    store.delete_resources(&release_resources)?;
                                }
                                Ok(())
                            })();
                            match res {
                                Ok(()) => {
                                    let _ = tx_event
                                        .unbounded_send(StoreEvent::WindowDropped(epoch_ms));
                                }
                                Err(e) => {
                                    let _ = tx_event.unbounded_send(StoreEvent::Failed(e));
                                }
                            }
                        }
                        StoreCommand::TestPause(barrier) => {
                            barrier.wait();
                            barrier.wait();
                        }
                        StoreCommand::FailNextPersist => {
                            fail_next_persist = true;
                        }
                        StoreCommand::FailNextFlush => {
                            fail_next_flush = true;
                        }
                        StoreCommand::FailNextRows => {
                            fail_next_rows = true;
                        }
                        StoreCommand::Shutdown => {
                            break;
                        }
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
            },
            rx_event,
            join_handle,
        ))
    }
}
