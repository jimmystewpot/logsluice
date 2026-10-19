//! Pod-level metadata persistence, initialization, and validation.

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::error::{AggError, AggResult};

fn default_window_duration_ms() -> i64 {
    300_000
}

/// Metadata for a pod running windowed aggregation shards.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PodMeta {
    /// 64-bit random/timestamp salt for salted hashing across shards.
    pub salt: u64,
    /// Identifier of the emitter node or pod.
    pub emitter_id: String,
    /// Total number of shards configured for this pod/cluster.
    pub shards: u16,
    /// Serialized representation or fingerprint of the active codec schema.
    pub codec_schema: Vec<u8>,
    /// Configured window duration in milliseconds.
    #[serde(default = "default_window_duration_ms")]
    pub window_duration_ms: i64,
    /// Configured window alignment offset in milliseconds.
    #[serde(default)]
    pub window_offset_ms: i64,
}

impl PodMeta {
    /// Loads an existing `PodMeta` from `data_dir` or initializes a new one atomically
    /// with the default window duration (300,000 ms) and offset (0 ms).
    ///
    /// # Errors
    /// Returns [`AggError::MetaMismatch`] on metadata divergence, [`AggError::Io`] on filesystem
    /// failure, or [`AggError::Codec`] on JSON deserialization failure.
    pub fn load_or_init(
        data_dir: &Path,
        emitter_id: &str,
        shards: u16,
        codec_schema: &[u8],
    ) -> AggResult<Self> {
        Self::load_or_init_with_window(
            data_dir,
            shards,
            codec_schema,
            Some(emitter_id),
            default_window_duration_ms(),
            0,
        )
    }

    /// Loads an existing `PodMeta` from `data_dir` or initializes a new one atomically,
    /// persisting and validating `window_duration_ms` and `window_offset_ms`.
    ///
    /// # Behavior
    /// - Targets file `data_dir/pod_meta.json`.
    /// - If the file exists: deserializes and verifies that `shards`, `codec_schema`,
    ///   `window_duration_ms`, and `window_offset_ms` match the configured arguments.
    ///   If any differs, returns [`AggError::MetaMismatch`].
    ///   The persisted `emitter_id` takes precedence over `emitter_override`.
    /// - If the file does not exist: generates a deterministic-entropy salt using XXH3 over
    ///   the current nanosecond timestamp and current process ID. Atomically writes to
    ///   `pod_meta.json.tmp`, fsyncs, renames to `pod_meta.json`, and fsyncs `data_dir`.
    ///
    /// # Errors
    /// Returns [`AggError::MetaMismatch`] on metadata divergence, [`AggError::Io`] on filesystem
    /// failure, or [`AggError::Codec`] on JSON deserialization failure.
    pub fn load_or_init_with_window(
        data_dir: &Path,
        shards: u16,
        codec_schema: &[u8],
        emitter_override: Option<&str>,
        window_duration_ms: i64,
        window_offset_ms: i64,
    ) -> AggResult<Self> {
        let meta_path = data_dir.join("pod_meta.json");

        if meta_path.try_exists()? {
            let data = std::fs::read(&meta_path)?;
            let persisted: Self =
                serde_json::from_slice(&data).map_err(|e| AggError::Codec(e.to_string()))?;

            if persisted.shards != shards {
                return Err(AggError::MetaMismatch {
                    field: "shards",
                    persisted: persisted.shards.to_string(),
                    configured: shards.to_string(),
                });
            }

            if persisted.codec_schema != codec_schema {
                return Err(AggError::MetaMismatch {
                    field: "codec_schema",
                    persisted: format!("{:02x?}", persisted.codec_schema),
                    configured: format!("{:02x?}", codec_schema),
                });
            }

            if persisted.window_duration_ms != window_duration_ms {
                return Err(AggError::MetaMismatch {
                    field: "window_duration_ms",
                    persisted: persisted.window_duration_ms.to_string(),
                    configured: window_duration_ms.to_string(),
                });
            }

            if persisted.window_offset_ms != window_offset_ms {
                return Err(AggError::MetaMismatch {
                    field: "window_offset_ms",
                    persisted: persisted.window_offset_ms.to_string(),
                    configured: window_offset_ms.to_string(),
                });
            }

            return Ok(persisted);
        }

        std::fs::create_dir_all(data_dir)?;

        let nanos = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(d) => d.as_nanos(),
            Err(_) => 0,
        };
        let pid = std::process::id();

        let mut seed_bytes = Vec::with_capacity(20);
        seed_bytes.extend_from_slice(&nanos.to_le_bytes());
        seed_bytes.extend_from_slice(&pid.to_le_bytes());
        let salt = xxhash_rust::xxh3::xxh3_64(&seed_bytes);

        let emitter_id = emitter_override.unwrap_or("pod-0").to_string();

        let meta = Self {
            salt,
            emitter_id,
            shards,
            codec_schema: codec_schema.to_vec(),
            window_duration_ms,
            window_offset_ms,
        };

        let tmp_path = data_dir.join("pod_meta.json.tmp");
        let content =
            serde_json::to_vec_pretty(&meta).map_err(|e| AggError::Codec(e.to_string()))?;

        {
            use std::io::Write;
            let mut file = std::fs::File::create(&tmp_path)?;
            file.write_all(&content)?;
            file.sync_all()?;
        }

        std::fs::rename(&tmp_path, &meta_path)?;

        if let Ok(dir_handle) = std::fs::File::open(data_dir) {
            let _ = dir_handle.sync_all();
        }

        Ok(meta)
    }
}
