//! Dedicated per-shard storage worker thread.
//!
//! Executes asynchronous I/O commands (flushes, ticket persistence, acks, range queries, deletions)
//! off the hot ingestion path.

use std::sync::mpsc::SyncSender;
use std::thread::JoinHandle;

use futures_channel::mpsc::UnboundedReceiver;

use crate::codec::ticket::{EmitReason, Ticket};
use crate::error::{AggError, AggResult};
use crate::key::StorageKey24;
use crate::l1::L1Buffer;
use crate::simd::SimdKernelTable;
use crate::store::shard_store::{FlushStats, JoinedRow, ShardStore};

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
    /// Flush operation failed; returns the error and unacknowledged finalizers.
    FlushFailed {
        /// Storage error encountered during the flush attempt.
        error: AggError,
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
    },
    /// Tickets successfully persisted with `SyncAll` durability.
    TicketsPersisted(Vec<u64>),
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
}

impl<F> Clone for StoreHandle<F> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
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
        self.tx
            .send(cmd)
            .map_err(|e| AggError::Channel(e.to_string()))
    }
}

static FAIL_NEXT_FLUSH: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Controller for spawning and managing a per-shard storage worker thread.
pub struct StoreThread;

impl StoreThread {
    /// Arms a one-shot simulated `apply_flush` error on the next `StoreCommand::Flush` processed.
    #[doc(hidden)]
    pub fn fail_next_flush() {
        FAIL_NEXT_FLUSH.store(true, std::sync::atomic::Ordering::SeqCst);
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

        let join_handle = std::thread::Builder::new()
            .name("shard-store-worker".to_string())
            .spawn(move || {
                let mut fail_next_flush = false;
                let mut fail_next_persist = false;
                while let Ok(cmd) = rx_cmd.recv() {
                    match cmd {
                        StoreCommand::Flush(buf) => {
                            let (batch, finalizers) = buf.into_flush();
                            let flush_res = if fail_next_flush
                                || FAIL_NEXT_FLUSH.swap(false, std::sync::atomic::Ordering::SeqCst)
                            {
                                fail_next_flush = false;
                                Err(AggError::Channel("injected Flush failure".into()))
                            } else {
                                store.apply_flush(batch, &k)
                            };
                            match flush_res {
                                Ok(stats) => {
                                    let disk_bytes = match store.disk_bytes() {
                                        Ok(b) => b,
                                        Err(e) => {
                                            let _ =
                                                tx_event.unbounded_send(StoreEvent::FlushFailed {
                                                    error: e,
                                                    finalizers,
                                                });
                                            continue;
                                        }
                                    };
                                    let _ = tx_event.unbounded_send(StoreEvent::FlushDone {
                                        finalizers,
                                        stats,
                                        disk_bytes,
                                    });
                                }
                                Err(e) => {
                                    let _ = tx_event.unbounded_send(StoreEvent::FlushFailed {
                                        error: e,
                                        finalizers,
                                    });
                                }
                            }
                        }
                        StoreCommand::PersistTickets(tickets) => {
                            if fail_next_persist {
                                fail_next_persist = false;
                                let _ = tx_event.unbounded_send(StoreEvent::Failed(
                                    AggError::Channel("injected PersistTickets failure".into()),
                                ));
                                continue;
                            }
                            let ticket_ids = tickets.iter().map(|t| t.id).collect();
                            match store.put_tickets(&tickets) {
                                Ok(()) => {
                                    let _ = tx_event
                                        .unbounded_send(StoreEvent::TicketsPersisted(ticket_ids));
                                }
                                Err(e) => {
                                    let _ = tx_event.unbounded_send(StoreEvent::Failed(e));
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
                        } => {
                            if let Some(ref ks) = keys {
                                match store.rows_for(ks) {
                                    Ok(rows) => {
                                        let _ = tx_event.unbounded_send(StoreEvent::Rows {
                                            epoch_ms,
                                            reason,
                                            rows,
                                            next_after: None,
                                        });
                                    }
                                    Err(e) => {
                                        let _ = tx_event.unbounded_send(StoreEvent::Failed(e));
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
                                        });
                                    }
                                    Err(e) => {
                                        let _ = tx_event.unbounded_send(StoreEvent::Failed(e));
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
                        StoreCommand::Shutdown => {
                            break;
                        }
                    }
                }
            })
            .map_err(AggError::Io)?;

        Ok((StoreHandle { tx: tx_cmd }, rx_event, join_handle))
    }
}
