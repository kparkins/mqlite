//! Contract 3.8 — citation integrity.
//!
//! Runs scripts/verify_phase_citations.py in --strict mode; fails on any
//! citation drift. Corresponds to docs/STORAGE-CONTRACTS-FROZEN.md Contract 3.8.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    reason = "test and bench targets use assertion-style panics and setup unwraps"
)]

use std::process::Command;

#[test]
fn phase_citations_are_clean() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let script = format!("{}/scripts/verify_phase_citations.py", manifest_dir);
    let out = Command::new("python3")
        .arg(&script)
        .arg("--strict")
        .current_dir(manifest_dir)
        .output()
        .expect("failed to run verify_phase_citations.py");
    assert!(
        out.status.success(),
        "citation drift detected:\nstdout={}\nstderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}
