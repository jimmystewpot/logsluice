#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

use std::process::Command;

use tempfile::TempDir;

fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_logsluice"))
}

#[test]
fn replay_with_crashes_matches_totals() {
    let d = tempfile::tempdir().unwrap();
    let o = bin()
        .args([
            "replay",
            "--events",
            "200000",
            "--keys",
            "5000",
            "--seed",
            "7",
            "--shards",
            "2",
            "--crash-every",
            "25000",
            "--data-dir",
        ])
        .arg(d.path())
        .output()
        .unwrap();
    let s = String::from_utf8_lossy(&o.stdout);
    assert!(o.status.success(), "{s}");
    assert!(s.contains("totals_match=true"));
}

#[test]
fn drain_emits_pending_state() {
    let d = tempfile::tempdir().unwrap();
    assert!(
        bin()
            .args([
                "replay",
                "--events",
                "1000",
                "--keys",
                "10",
                "--seed",
                "1",
                "--shards",
                "1",
                "--stop-before-close",
                "--data-dir"
            ])
            .arg(d.path())
            .status()
            .unwrap()
            .success()
    );
    let o = bin()
        .args(["drain", "--format", "ndjson", "--data-dir"])
        .arg(d.path())
        .output()
        .unwrap();
    let total: u64 = String::from_utf8_lossy(&o.stdout)
        .lines()
        .map(|l| {
            serde_json::from_str::<serde_json::Value>(l).unwrap()["counters"][0][0]
                .as_u64()
                .unwrap()
        })
        .sum();
    assert_eq!(total, 1000);
}

#[test]
fn drain_uses_persisted_window_duration_and_drains_spill_files() {
    let dir = TempDir::new().unwrap();
    let bin = env!("CARGO_BIN_EXE_logsluice");

    // 1. Run replay with custom --window-duration-ms 60000 and --stop-before-close
    let st = Command::new(bin)
        .args([
            "replay",
            "--events",
            "500",
            "--keys",
            "50",
            "--seed",
            "11",
            "--shards",
            "1",
            "--window-duration-ms",
            "60000",
            "--data-dir",
            dir.path().to_str().unwrap(),
            "--stop-before-close",
        ])
        .status()
        .unwrap();
    assert!(st.success());

    // 2. Also write a synthetic spill file in shard_0/spill/ to verify drain emits and removes it
    let spill_dir = dir.path().join("shard_0").join("spill");
    let mut writer = logsluice_core::worker::spill::SpillWriter::new(spill_dir);
    let spilled_record = logsluice_core::emission::SliceRecord {
        window_start_ms: 1_700_000_000_000,
        window_end_ms: 1_700_000_060_000,
        emit_reason: logsluice_core::codec::ticket::EmitReason::Close,
        emitter_id: "test-emitter".to_string(),
        digest: 0x1234,
        slice_seq: 0,
        slice_id: "test-emitter:1700000000000:00000000000000000000000000001234:0".to_string(),
        resource: std::sync::Arc::new(logsluice_core::resource::ResourceScopePayload {
            resource: Default::default(),
            scope_name: String::new(),
            scope_version: String::new(),
            scope_attrs: Default::default(),
        }),
        tail: logsluice_core::codec::tail::ExemplarTail {
            severity: 9,
            body: Some("spilled".into()),
            dims: vec![],
            attrs: vec![],
        },
        counters: vec![[42, 0, 0, 0]],
        minmax: vec![],
    };
    writer
        .spill(999, std::slice::from_ref(&spilled_record))
        .unwrap();

    // 3. Run drain and check both window_end_ms - window_start_ms == 60_000 and the spilled record (+42)
    let out = Command::new(bin)
        .args([
            "drain",
            "--data-dir",
            dir.path().to_str().unwrap(),
            "--format",
            "ndjson",
        ])
        .output()
        .unwrap();
    assert!(out.status.success());

    let stdout = String::from_utf8_lossy(&out.stdout);
    let mut total = 0u64;
    for line in stdout.lines().filter(|l| !l.trim().is_empty()) {
        let v: serde_json::Value = serde_json::from_str(line).unwrap();
        let start = v["window_start_ms"].as_i64().unwrap();
        let end = v["window_end_ms"].as_i64().unwrap();
        assert_eq!(end - start, 60_000);
        total += v["counters"][0][0].as_u64().unwrap();
    }
    assert_eq!(total, 500 + 42);
    assert_eq!(writer.count_files(), 0);
}

#[test]
fn drain_rejects_unsupported_format_and_missing_meta() {
    let dir = TempDir::new().unwrap();

    let bad_fmt = bin()
        .args(["drain", "--data-dir"])
        .arg(dir.path())
        .args(["--format", "parquet"])
        .output()
        .unwrap();
    assert!(!bad_fmt.status.success());
    assert!(
        String::from_utf8_lossy(&bad_fmt.stderr).contains("unsupported format"),
        "expected unsupported format error"
    );

    let missing_meta = bin()
        .args(["drain", "--data-dir"])
        .arg(dir.path())
        .args(["--format", "ndjson"])
        .output()
        .unwrap();
    assert!(!missing_meta.status.success());
    assert!(
        String::from_utf8_lossy(&missing_meta.stderr).contains("pod_meta.json not found"),
        "expected missing pod_meta.json error"
    );
}
