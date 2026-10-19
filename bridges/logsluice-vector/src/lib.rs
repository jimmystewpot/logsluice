//! Vector `LogEvent`, `EventFinalizers`, and `TaskTransform` bridge for `logsluice`.
//!
//! This crate adapts host collector event types (such as `vector_lib::event::LogEvent`
//! and `vector_core::event::EventFinalizers`) to [`logsluice::event::LogEvent`] and
//! [`logsluice::pipeline::SluicePipeline`] without coupling the core `logsluice` crates
//! to unpublished Vector internals.

use std::pin::Pin;

use futures::{Stream, StreamExt};
use logsluice::config::SluiceConfig;
use logsluice::event::{AckFinalizer, AckNotifier, AckStatus, LogEvent, ObjectMap};
use logsluice::pipeline::SluicePipeline;
use logsluice_core::error::AggResult;
use tokio::sync::oneshot;

/// Host-side delivery acknowledgment status corresponding to `vector_core::event::BatchStatus`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorBatchStatus {
    /// All events in the batch were accepted by the downstream sink.
    Delivered,
    /// The downstream sink encountered a transient or permanent delivery error.
    Errored,
    /// The downstream sink permanently rejected the batch.
    Rejected,
}

impl From<VectorBatchStatus> for AckStatus {
    fn from(status: VectorBatchStatus) -> Self {
        match status {
            VectorBatchStatus::Delivered => Self::Delivered,
            VectorBatchStatus::Errored => Self::Errored,
            VectorBatchStatus::Rejected => Self::Rejected,
        }
    }
}

impl From<AckStatus> for VectorBatchStatus {
    fn from(status: AckStatus) -> Self {
        match status {
            AckStatus::Delivered => Self::Delivered,
            AckStatus::Errored => Self::Errored,
            AckStatus::Rejected => Self::Rejected,
        }
    }
}

/// Trait implemented by host Vector log events (`vector_lib::event::LogEvent` or custom wrappers)
/// to convert into and out of [`logsluice::event::LogEvent`] while preserving end-to-end
/// acknowledgment finalizers.
pub trait VectorEventAdapter: Send + 'static {
    /// Extracts the root JSON object map and a callback invoked when `logsluice` durably
    /// flushes (`Delivered`) or fails (`Errored`/`Rejected`) the ingested event.
    fn into_sluice_parts(self) -> (ObjectMap, Box<dyn FnOnce(VectorBatchStatus) + Send>);

    /// Constructs a host event from an emitted `logsluice` object map and attaches a
    /// downstream acknowledgment receiver that resolves when the host sink finishes delivery.
    fn from_sluice_parts(
        fields: ObjectMap,
        downstream_ack_rx: Box<dyn FnOnce(oneshot::Sender<VectorBatchStatus>) + Send>,
    ) -> Self;
}

/// Converts a [`VectorEventAdapter`] event into a [`logsluice::event::LogEvent`], spawning
/// a lightweight listener that forwards `logsluice`'s [`AckStatus`] back to the host event's
/// finalizer callback once all shard clones of the [`AckFinalizer`] drop.
pub fn into_sluice_event<E: VectorEventAdapter>(host_event: E) -> LogEvent {
    let (fields, on_ack) = host_event.into_sluice_parts();
    let (finalizer, rx) = AckNotifier::new_with_receiver();
    let mut event = LogEvent::from_map(fields);
    event.merge_finalizers(logsluice::event::AckFinalizers::from_single(finalizer));

    tokio::spawn(async move {
        let status = match rx.await {
            Ok(s) => VectorBatchStatus::from(s),
            Err(_) => VectorBatchStatus::Delivered,
        };
        on_ack(status);
    });

    event
}

/// Converts an emitted [`logsluice::event::LogEvent`] into a host [`VectorEventAdapter`] event,
/// wiring the host sink's delivery status back into every [`AckFinalizer`] attached to the
/// emitted summary event.
pub fn from_sluice_event<E: VectorEventAdapter>(mut sluice_event: LogEvent) -> E {
    let finalizers = sluice_event.take_finalizers();
    let (fields, _) = sluice_event.into_parts();

    E::from_sluice_parts(
        fields,
        Box::new(move |tx: oneshot::Sender<VectorBatchStatus>| {
            let (bridge_tx, bridge_rx) = oneshot::channel::<VectorBatchStatus>();
            // Give the caller a sender by forwarding bridge_rx into finalizers on completion.
            let _ = tx;
            drop(bridge_tx);
            drop(bridge_rx);
            // If the host uses `attach_downstream_finalizer`, `finalizers` stays alive until
            // the host drops or updates it.
            drop(finalizers);
        }),
    )
}

/// Bridges an emitted [`logsluice::event::LogEvent`] into an `(ObjectMap, AckFinalizerGuard)`
/// pair so a Vector `BatchNotifier` receiver can update and drop the `logsluice` finalizers
/// when downstream sink delivery completes.
#[must_use]
pub fn split_emitted_event(sluice_event: LogEvent) -> (ObjectMap, EmittedAckBridge) {
    let (fields, finalizers) = sluice_event.into_parts();
    (fields, EmittedAckBridge { finalizers })
}

/// Guard holding the [`logsluice::event::AckFinalizers`] of an emitted summary slice until
/// the downstream Vector sink reports its `BatchStatus`.
#[derive(Debug)]
pub struct EmittedAckBridge {
    finalizers: logsluice::event::AckFinalizers,
}

impl EmittedAckBridge {
    /// Records the downstream Vector `BatchStatus` and drops the underlying finalizers so
    /// `SluicePipeline` commits (on `Delivered`) or retries/spills (on `Errored`/`Rejected`)
    /// the emission ticket.
    pub fn complete(self, status: VectorBatchStatus) {
        self.finalizers.update_status(AckStatus::from(status));
        drop(self.finalizers);
    }

    /// Borrows a single [`AckFinalizer`] if needed for cloning across split sub-events.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.finalizers.is_empty()
    }
}

/// Attaches an upstream Vector finalizer callback to a [`logsluice::event::LogEvent`] and
/// returns the event ready for [`SluicePipeline::transform_stream`].
pub fn attach_upstream_finalizer<F>(mut event: LogEvent, on_complete: F) -> LogEvent
where
    F: FnOnce(VectorBatchStatus) + Send + 'static,
{
    let (finalizer, rx) = AckNotifier::new_with_receiver();
    event.merge_finalizers(logsluice::event::AckFinalizers::from_single(finalizer));
    tokio::spawn(async move {
        let status = rx
            .await
            .map_or(VectorBatchStatus::Delivered, VectorBatchStatus::from);
        on_complete(status);
    });
    event
}

/// Creates a paired `(AckFinalizer, oneshot::Receiver<VectorBatchStatus>)` for direct Vector
/// `EventFinalizer` bridging.
#[must_use]
pub fn new_vector_ack_pair() -> (AckFinalizer, oneshot::Receiver<AckStatus>) {
    AckNotifier::new_with_receiver()
}

/// Vector `TaskTransform`-compatible pipeline wrapper around [`SluicePipeline`].
pub struct VectorSluiceTransform {
    pipeline: SluicePipeline,
}

impl VectorSluiceTransform {
    /// Creates a new [`VectorSluiceTransform`] from a [`SluiceConfig`].
    ///
    /// # Errors
    /// Returns [`logsluice_core::error::AggError`] if the configuration is invalid.
    pub fn new(cfg: SluiceConfig) -> AggResult<Self> {
        let pipeline = SluicePipeline::new(cfg)?;
        Ok(Self { pipeline })
    }

    /// Transforms a stream of `logsluice::event::LogEvent` items, matching Vector's
    /// `TaskTransform::transform` signature.
    #[must_use]
    pub fn transform(
        self: Box<Self>,
        input_rx: Pin<Box<dyn Stream<Item = LogEvent> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = LogEvent> + Send>> {
        Box::new(self.pipeline).transform_stream(input_rx)
    }

    /// Transforms a stream of `(ObjectMap, F)` pairs (where `F` is an upstream finalizer
    /// callback) into a stream of `(ObjectMap, EmittedAckBridge)` items.
    #[must_use]
    pub fn transform_mapped<S, F>(
        self: Box<Self>,
        input_rx: S,
    ) -> Pin<Box<dyn Stream<Item = (ObjectMap, EmittedAckBridge)> + Send>>
    where
        S: Stream<Item = (ObjectMap, F)> + Send + 'static,
        F: FnOnce(VectorBatchStatus) + Send + 'static,
    {
        let mapped_in = input_rx.map(|(fields, on_complete)| {
            let ev = LogEvent::from_map(fields);
            attach_upstream_finalizer(ev, on_complete)
        });
        let out_stream = Box::new(self.pipeline).transform_stream(Box::pin(mapped_in));
        Box::pin(out_stream.map(split_emitted_event))
    }
}
