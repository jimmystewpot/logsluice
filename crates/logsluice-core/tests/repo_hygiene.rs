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
        ".github/workflows/coverage.yml",
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
    assert!(
        sonar_props.contains("sonar.rust.lcov.reportPaths=lcov.info")
            && !sonar_props.contains("sonar.coverageReportPaths=lcov.info"),
        "sonar-project.properties must use sonar.rust.lcov.reportPaths=lcov.info (not XML-only sonar.coverageReportPaths)"
    );

    let sonar_wf = fs::read_to_string(root.join(".github/workflows/sonarcloud.yml")).unwrap();
    assert!(sonar_wf.contains("actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1"));
    assert!(sonar_wf.contains("actions/cache@55cc8345863c7cc4c66a329aec7e433d2d1c52a9"));
    assert!(
        sonar_wf
            .contains("SonarSource/sonarqube-scan-action@d209202bc7d53ff1cc128f7f907dac145c9d6ae9")
    );
    assert!(
        sonar_wf.contains("dirmngr"),
        "sonarcloud.yml must install dirmngr so SonarQube GPG key verification succeeds on arc-runner-logsluice"
    );

    let ci_wf = fs::read_to_string(root.join(".github/workflows/ci.yml")).unwrap();
    assert!(
        ci_wf.contains("taiki-e/install-action@cargo-deny"),
        "ci.yml must use host taiki-e/install-action@cargo-deny instead of DinD container action"
    );
    assert!(
        !ci_wf.contains("EmbarkStudios/cargo-deny-action"),
        "ci.yml must not use DinD EmbarkStudios/cargo-deny-action on arc-runner-logsluice"
    );

    let release_wf = fs::read_to_string(root.join(".github/workflows/release.yml")).unwrap();
    assert!(
        release_wf.contains("gcc-aarch64-linux-gnu")
            && release_wf.contains("libc6-dev-arm64-cross")
            && release_wf.contains("build-essential"),
        "release.yml must install build-essential, gcc-aarch64-linux-gnu, and libc6-dev-arm64-cross"
    );
    assert!(
        release_wf.contains("fetch-depth: 0")
            && release_wf.contains("git merge-base --is-ancestor HEAD origin/main"),
        "release.yml must fetch full git history and verify tagged commit is an ancestor of origin/main"
    );
    assert!(
        release_wf.contains("EXPECTED_TAG=\"v${CARGO_VERSION}\""),
        "release.yml must verify pushed tag matches workspace Cargo.toml version"
    );
    assert!(
        release_wf.contains("https://crates.io/api/v1/crates/"),
        "release.yml must check crates.io before publishing so tag re-runs are idempotent"
    );

    let bench_wf = fs::read_to_string(root.join(".github/workflows/benchmarks.yml")).unwrap();
    assert!(
        bench_wf.contains("external-data-json-path: target/bench-data/benchmark-data.json"),
        "benchmarks.yml must configure external-data-json-path so github-action-benchmark does not fail fetching a missing gh-pages branch"
    );
    assert!(
        bench_wf.contains("actions/cache@55cc8345863c7cc4c66a329aec7e433d2d1c52a9"),
        "benchmarks.yml must cache target/bench-data with pinned actions/cache"
    );
    assert!(
        bench_wf.contains("summary-always: true"),
        "benchmarks.yml must enable summary-always for job summary output"
    );
    assert!(
        bench_wf.contains("set -o pipefail"),
        "benchmarks.yml must set -o pipefail when piping cargo bench output to tee"
    );
    assert!(
        bench_wf.contains("rm -rf target/criterion"),
        "benchmarks.yml must remove target/criterion before cargo bench so empty cached base dirs do not corrupt bencher output"
    );

    let dependabot = fs::read_to_string(root.join(".github/dependabot.yml")).unwrap();
    assert!(dependabot.contains("package-ecosystem: \"cargo\""));
    assert!(dependabot.contains("package-ecosystem: \"github-actions\""));

    for wf_rel in [
        ".github/workflows/ci.yml",
        ".github/workflows/coverage.yml",
        ".github/workflows/sonarcloud.yml",
        ".github/workflows/benchmarks.yml",
        ".github/workflows/release.yml",
    ] {
        let wf = fs::read_to_string(root.join(wf_rel)).unwrap();
        assert!(
            wf.contains("runs-on: arc-runner-logsluice"),
            "{wf_rel} must specify runs-on: arc-runner-logsluice"
        );
        assert!(
            wf.contains("build-essential"),
            "{wf_rel} must install build-essential so linker `cc` is available on arc-runner-logsluice"
        );
        if wf_rel != ".github/workflows/release.yml" {
            assert!(
                wf.contains("concurrency:") && wf.contains("cancel-in-progress:"),
                "{wf_rel} must configure concurrency with cancel-in-progress to prevent ARC queue starvation"
            );
        }
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
        let readme_path = root.join(crate_dir).join("README.md");
        let readme = fs::read_to_string(&readme_path).unwrap();
        assert!(
            readme.contains("## The Problem It Solves") && readme.contains("## How It Solves It"),
            "{crate_dir}/README.md must include '## The Problem It Solves' and '## How It Solves It' sections for crates.io discoverability"
        );
        assert!(
            !readme.contains("](../") && !readme.contains("](../../"),
            "{crate_dir}/README.md must use absolute URLs rather than relative parent links so links work on crates.io"
        );
    }
}
