//! US-027 source-ownership regression tests.
//!
//! These tests stay outside production modules and validate that Phase 5 no
//! longer exposes the retired commit-sequence mutex or metric family.

use std::fs;
use std::path::{Path, PathBuf};

const SCAN_DIRS: [&str; 3] = ["src", "tests", "benches"];

fn rust_files_under(dir: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).expect("scan dir readable") {
        let entry = entry.expect("scan entry readable");
        let path = entry.path();
        if path.is_dir() {
            rust_files_under(&path, files);
            continue;
        }
        if path.extension().is_some_and(|ext| ext == "rs") {
            files.push(path);
        }
    }
}

fn rust_source_files() -> Vec<PathBuf> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    for dir in SCAN_DIRS {
        rust_files_under(&root.join(dir), &mut files);
    }
    files
}

#[test]
fn no_retired_sequence_tokens_remain_in_rust_sources() {
    let retired_mutex_token = concat!("commit", "_seq");
    let retired_metric_tokens = [
        concat!("commit", "_seq", "_wait_ns"),
        concat!("record_", "commit", "_seq", "_wait_ns"),
        concat!("reset_", "commit", "_seq", "_wait_ns"),
        concat!("commit", "_seq", "_wait_ns_snapshot"),
    ];
    let mut hits = Vec::new();

    for file in rust_source_files() {
        let source = fs::read_to_string(&file).expect("rust source readable");
        if source.contains(retired_mutex_token)
            || retired_metric_tokens
                .iter()
                .any(|token| source.contains(token))
        {
            hits.push(
                file.strip_prefix(env!("CARGO_MANIFEST_DIR"))
                    .unwrap()
                    .display()
                    .to_string(),
            );
        }
    }

    assert!(
        hits.is_empty(),
        "retired Phase 5 commit-sequence tokens remain in Rust sources: {hits:?}"
    );
}
