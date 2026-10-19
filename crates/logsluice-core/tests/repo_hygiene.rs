#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

use std::fs;
use std::path::PathBuf;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn workspace_manifests_use_workspace_dependencies_and_sonar_pins() {
    let root = repo_root();
    let required_files = [
        ".github/dependabot.yml",
        ".github/workflows/ci.yml",
        ".github/workflows/sonarcloud.yml",
        ".github/workflows/benchmarks.yml",
        ".github/workflows/release.yml",
        "sonar-project.properties",
        "codecov.yml",
        "deny.toml",
        "README.md",
        "AGENTS.md",
        "LICENSE",
        "CHANGELOG.md",
        "CONTRIBUTING.md",
        "SECURITY.md",
        "docs/architecture/design-specification.md",
        "docs/architecture/tradeoffs-and-benchmarks.md",
        "docs/operations/cli-and-runbook.md",
        "docs/integration/vector.md",
    ];

    for rel in required_files {
        assert!(
            root.join(rel).is_file(),
            "missing required repository file: {rel}"
        );
    }

    let sonar_props = fs::read_to_string(root.join("sonar-project.properties")).unwrap();
    assert!(sonar_props.contains("sonar.projectKey=jimmystewpot_logsluice"));
    assert!(sonar_props.contains("sonar.organization=jimmystewpot"));

    let sonar_wf = fs::read_to_string(root.join(".github/workflows/sonarcloud.yml")).unwrap();
    assert!(sonar_wf.contains("actions/checkout@34e114876b0b11c390a56381ad16ebd13914f8d5"));
    assert!(sonar_wf.contains("actions/cache@0057852bfaa89a56745cba8c7296529d2fc39830"));
    assert!(
        sonar_wf
            .contains("SonarSource/sonarqube-scan-action@d209202bc7d53ff1cc128f7f907dac145c9d6ae9")
    );

    let dependabot = fs::read_to_string(root.join(".github/dependabot.yml")).unwrap();
    assert!(dependabot.contains("package-ecosystem: \"cargo\""));
    assert!(dependabot.contains("package-ecosystem: \"github-actions\""));

    for wf_rel in [
        ".github/workflows/ci.yml",
        ".github/workflows/sonarcloud.yml",
        ".github/workflows/benchmarks.yml",
        ".github/workflows/release.yml",
    ] {
        let wf = fs::read_to_string(root.join(wf_rel)).unwrap();
        assert!(
            wf.contains("runs-on: arc-runner-logsluice"),
            "{wf_rel} must specify runs-on: arc-runner-logsluice"
        );
        for line in wf.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with("runs-on:") {
                assert_eq!(
                    trimmed, "runs-on: arc-runner-logsluice",
                    "{wf_rel} contains non-ARC runner declaration: {trimmed}"
                );
            }
        }
    }

    for crate_dir in [
        "crates/logsluice-core",
        "crates/logsluice",
        "crates/logsluice-cli",
    ] {
        let manifest_path = root.join(crate_dir).join("Cargo.toml");
        if manifest_path.is_file() {
            let content = fs::read_to_string(&manifest_path).unwrap();
            for line in content.lines() {
                let trimmed = line.trim();
                if !trimmed.starts_with("version.workspace") {
                    assert!(
                        !trimmed.contains("version = \""),
                        "{crate_dir}/Cargo.toml must not contain inline version strings: {trimmed}"
                    );
                }
            }
        }
    }
}
