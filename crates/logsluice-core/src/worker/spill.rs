//! Disk spill writer managing unacknowledged aggregate records during sink outages.
//!
//! Owned and executed on [`crate::store::thread::StoreThread`] (and by offline CLI drain)
//! so [`crate::worker::ShardWorker`] never performs filesystem I/O on the caller thread.

use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use crate::emission::SliceRecord;
use crate::error::{AggError, AggResult};

/// Spill entry consisting of delivery ticket ID and associated slice records.
pub type SpillEntry = (u64, Vec<SliceRecord>);

/// Spill writer handling atomic disk persistence and replay of unacknowledged slices.
#[derive(Clone, Debug)]
pub struct SpillWriter {
    spill_dir: PathBuf,
}

impl SpillWriter {
    /// Creates a new `SpillWriter` managing the `spill/` directory under `dir`.
    ///
    /// If `dir` already ends with `spill`, uses `dir` directly; otherwise uses `dir.join("spill")`.
    #[must_use]
    pub fn new(dir: PathBuf) -> Self {
        let spill_dir = if dir.file_name().is_some_and(|name| name == "spill") {
            dir
        } else {
            dir.join("spill")
        };
        Self { spill_dir }
    }

    /// Returns a reference to the underlying spill directory path.
    #[must_use]
    pub fn spill_dir(&self) -> &Path {
        &self.spill_dir
    }

    /// Atomically persists `records` for `ticket_id` as NDJSON to `spill/{ticket_id:020}.ndjson`.
    ///
    /// Uses write -> fsync -> atomic rename.
    ///
    /// # Errors
    /// Returns [`AggError::Io`] or [`AggError::Codec`] if disk write or serialization fails.
    pub fn spill(&mut self, ticket_id: u64, records: &[SliceRecord]) -> AggResult<()> {
        fs::create_dir_all(&self.spill_dir).map_err(AggError::Io)?;

        let filename = format!("{ticket_id:020}.ndjson");
        let target_path = self.spill_dir.join(&filename);
        let temp_path = self.spill_dir.join(format!(".{ticket_id:020}.ndjson.tmp"));

        let mut file = File::create(&temp_path).map_err(AggError::Io)?;
        for record in records {
            let line = serde_json::to_string(record).map_err(|e| AggError::Codec(e.to_string()))?;
            file.write_all(line.as_bytes()).map_err(AggError::Io)?;
            file.write_all(b"\n").map_err(AggError::Io)?;
        }
        file.sync_all().map_err(AggError::Io)?;
        drop(file);

        fs::rename(&temp_path, &target_path).map_err(AggError::Io)?;

        if let Ok(dir_file) = File::open(&self.spill_dir) {
            let _ = dir_file.sync_all();
        }

        Ok(())
    }

    /// Reads all pending spill files, returning them sorted by ticket ID along with
    /// the count of corrupt spill files quarantined during this scan.
    ///
    /// Corrupt `.ndjson` files are quarantined by renaming them to
    /// `<id>.ndjson.corrupt` and skipped so valid spill files can still be replayed.
    ///
    /// # Errors
    /// Returns [`AggError::Io`] if reading the spill directory, reading a spill file, or
    /// quarantining a corrupt file fails.
    pub fn pending_with_quarantined(&self) -> AggResult<(Vec<SpillEntry>, u64)> {
        if !self.spill_dir.exists() {
            return Ok((Vec::new(), 0));
        }

        let entries = fs::read_dir(&self.spill_dir).map_err(AggError::Io)?;
        let mut results = Vec::new();
        let mut quarantined = 0u64;

        for entry in entries {
            let entry = entry.map_err(AggError::Io)?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }

            let file_name = match path.file_name().and_then(|s| s.to_str()) {
                Some(name) => name,
                None => continue,
            };

            if file_name.starts_with('.') || !file_name.ends_with(".ndjson") {
                continue;
            }

            let ticket_id = path
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<u64>().ok());

            match ticket_id {
                Some(id) => match Self::read_spill_file(&path) {
                    Ok(records) => results.push((id, records)),
                    Err(AggError::Codec(_)) => {
                        let corrupt_path = path.with_extension("ndjson.corrupt");
                        fs::rename(&path, corrupt_path).map_err(AggError::Io)?;
                        quarantined += 1;
                    }
                    Err(err) => return Err(err),
                },
                None => {
                    let corrupt_path = path.with_extension("ndjson.corrupt");
                    fs::rename(&path, corrupt_path).map_err(AggError::Io)?;
                    quarantined += 1;
                }
            }
        }

        results.sort_by_key(|(id, _)| *id);
        Ok((results, quarantined))
    }

    /// Reads all pending spill files, returning them sorted by ticket ID.
    ///
    /// Corrupt `.ndjson` files are quarantined by renaming them to
    /// `<id>.ndjson.corrupt` and skipped so valid spill files can still be replayed.
    ///
    /// # Errors
    /// Returns [`AggError::Io`] if reading the spill directory, reading a spill file, or
    /// quarantining a corrupt file fails.
    pub fn pending(&self) -> AggResult<Vec<SpillEntry>> {
        self.pending_with_quarantined().map(|(entries, _)| entries)
    }

    fn read_spill_file(path: &Path) -> AggResult<Vec<SliceRecord>> {
        let file = File::open(path).map_err(AggError::Io)?;
        let reader = BufReader::new(file);
        let mut records = Vec::new();

        for line in reader.lines() {
            let line = line.map_err(AggError::Io)?;
            if line.trim().is_empty() {
                continue;
            }
            let record: SliceRecord =
                serde_json::from_str(&line).map_err(|e| AggError::Codec(e.to_string()))?;
            records.push(record);
        }

        Ok(records)
    }

    /// Deletes the spill file for `ticket_id` if it exists.
    ///
    /// # Errors
    /// Returns [`AggError::Io`] if deletion fails.
    pub fn remove(&mut self, ticket_id: u64) -> AggResult<()> {
        let filename = format!("{ticket_id:020}.ndjson");
        let target_path = self.spill_dir.join(&filename);
        match fs::remove_file(&target_path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(AggError::Io(e)),
        }
    }

    /// Counts pending `.ndjson` spill files in the spill directory without opening or deserializing them.
    #[must_use]
    pub fn count_files(&self) -> usize {
        let Ok(entries) = fs::read_dir(&self.spill_dir) else {
            return 0;
        };
        let mut count = 0;
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let Some(file_name) = path.file_name().and_then(|s| s.to_str()) else {
                continue;
            };
            if !file_name.starts_with('.') && file_name.ends_with(".ndjson") {
                count += 1;
            }
        }
        count
    }

    /// Returns the total byte size of all regular files currently in the spill directory.
    #[must_use]
    pub fn total_bytes(&self) -> u64 {
        spill_dir_bytes(&self.spill_dir)
    }
}

/// Sums the byte lengths of all regular files in `spill_dir`.
///
/// Returns `0` immediately if `spill_dir` does not exist or cannot be read.
#[must_use]
pub fn spill_dir_bytes(spill_dir: &Path) -> u64 {
    let Ok(entries) = fs::read_dir(spill_dir) else {
        return 0;
    };
    entries
        .flatten()
        .map(|entry| {
            entry
                .metadata()
                .map(|m| if m.is_file() { m.len() } else { 0 })
                .unwrap_or(0)
        })
        .fold(0u64, u64::saturating_add)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::tempdir;

    use crate::codec::tail::ExemplarTail;
    use crate::codec::ticket::EmitReason;
    use crate::resource::ResourceScopePayload;

    fn sample_record(_ticket_id: u64, digest: u128) -> SliceRecord {
        SliceRecord {
            window_start_ms: 0,
            window_end_ms: 60_000,
            emit_reason: EmitReason::Close,
            emitter_id: "emitter-0".to_string(),
            digest,
            slice_seq: 0,
            slice_id: format!("emitter-0:0:{digest:032x}:0"),
            resource: Arc::new(ResourceScopePayload {
                resource: Default::default(),
                scope_name: "test".to_string(),
                scope_version: "1.0".to_string(),
                scope_attrs: Default::default(),
            }),
            tail: ExemplarTail {
                severity: 0,
                body: None,
                dims: Vec::new(),
                attrs: Vec::new(),
            },
            counters: vec![[1, 0, 0, 0]],
            minmax: vec![(10, 20)],
        }
    }

    #[test]
    fn test_spill_write_pending_remove() {
        let tmp = tempdir().expect("tempdir");
        let mut writer = SpillWriter::new(tmp.path().to_path_buf());

        assert_eq!(writer.count_files(), 0);
        assert!(writer.pending().expect("pending").is_empty());

        let r1 = sample_record(42, 100);
        let r2 = sample_record(42, 200);
        writer.spill(42, &[r1.clone(), r2.clone()]).expect("spill");

        let r3 = sample_record(10, 300);
        writer.spill(10, std::slice::from_ref(&r3)).expect("spill");

        assert_eq!(writer.count_files(), 2);
        let pending = writer.pending().expect("pending");
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].0, 10);
        assert_eq!(pending[0].1, vec![r3]);
        assert_eq!(pending[1].0, 42);
        assert_eq!(pending[1].1, vec![r1, r2]);

        writer.remove(10).expect("remove");
        assert_eq!(writer.count_files(), 1);
        let pending = writer.pending().expect("pending");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].0, 42);

        writer.remove(42).expect("remove");
        assert_eq!(writer.count_files(), 0);
        let pending = writer.pending().expect("pending");
        assert!(pending.is_empty());
    }

    #[test]
    fn pending_skips_and_quarantines_corrupt_file() {
        let tmp = tempdir().expect("tempdir");
        let mut writer = SpillWriter::new(tmp.path().to_path_buf());

        let valid_record = sample_record(20, 100);
        writer
            .spill(20, std::slice::from_ref(&valid_record))
            .expect("spill valid");

        let corrupt_orig = writer.spill_dir().join(format!("{:020}.ndjson", 10));
        let corrupt_quarantined = writer
            .spill_dir()
            .join(format!("{:020}.ndjson.corrupt", 10));
        fs::write(&corrupt_orig, b"{broken json\n").expect("write corrupt file");

        let bad_stem_orig = writer.spill_dir().join("bad_stem.ndjson");
        let bad_stem_quarantined = writer.spill_dir().join("bad_stem.ndjson.corrupt");
        fs::write(&bad_stem_orig, b"{\"not\":\"valid_id\"}\n").expect("write bad stem file");

        assert_eq!(writer.count_files(), 3);

        let (first, quarantined) = writer
            .pending_with_quarantined()
            .expect("pending should skip corrupt file instead of failing");
        assert_eq!(quarantined, 2);
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].0, 20);
        assert_eq!(first[0].1, vec![valid_record]);

        assert!(!corrupt_orig.exists());
        assert!(corrupt_quarantined.exists());
        assert!(!bad_stem_orig.exists());
        assert!(bad_stem_quarantined.exists());
        assert_eq!(writer.count_files(), 1);

        let (second, second_quarantined) = writer
            .pending_with_quarantined()
            .expect("second pending call");
        assert_eq!(second_quarantined, 0);
        assert_eq!(second, first);
        assert_eq!(writer.count_files(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn pending_propagates_error_when_quarantine_rename_fails() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempdir().expect("tempdir");
        let writer = SpillWriter::new(tmp.path().to_path_buf());
        fs::create_dir_all(writer.spill_dir()).expect("create spill dir");

        let corrupt_orig = writer.spill_dir().join(format!("{:020}.ndjson", 1));
        fs::write(&corrupt_orig, b"{broken json\n").expect("write corrupt file");

        fs::set_permissions(writer.spill_dir(), fs::Permissions::from_mode(0o555))
            .expect("chmod 0555");

        let res = writer.pending();

        fs::set_permissions(writer.spill_dir(), fs::Permissions::from_mode(0o755))
            .expect("restore chmod 0755");

        assert!(
            matches!(res, Err(AggError::Io(_))),
            "expected AggError::Io when quarantine rename fails, got {res:?}"
        );
        assert!(corrupt_orig.exists());
    }
}
