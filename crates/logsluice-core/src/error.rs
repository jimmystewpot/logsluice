//! Error types for windowed aggregate engine.

/// Specialized result type for windowed aggregation operations.
pub type AggResult<T> = Result<T, AggError>;

/// Errors produced by the windowed aggregate engine.
#[derive(Debug, thiserror::Error)]
pub enum AggError {
    /// Configuration validation error.
    #[error("invalid config: {0}")]
    Config(String),

    /// Storage subsystem error from fjall.
    #[error("storage error: {0}")]
    Storage(#[from] fjall::Error),

    /// Standard I/O error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Codec serialization/deserialization error.
    #[error("codec error: {0}")]
    Codec(String),

    /// Worker thread or channel communication error.
    #[error("channel error: {0}")]
    Channel(String),

    /// Metadata mismatch between persisted state and configuration.
    #[error(
        "metadata mismatch for field '{field}': persisted '{persisted}', configured '{configured}'"
    )]
    MetaMismatch {
        /// The field name where mismatch was detected.
        field: &'static str,
        /// The persisted value.
        persisted: String,
        /// The currently configured value.
        configured: String,
    },
}
