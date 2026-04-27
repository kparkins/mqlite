//! Phase 3 US-015 public configuration validation tests.

use mqlite::{Client, OpenOptions};

const VALID_THRESHOLD: f64 = 1.0;
const ZERO_THRESHOLD: f64 = 0.0;
const ABOVE_ONE_THRESHOLD: f64 = 1.1;

#[test]
fn delta_bearing_frames_warn_threshold_rejects_invalid_values() {
    let dir = tempfile::tempdir().unwrap();
    let zero_path = dir.path().join("zero.mqlite");
    let high_path = dir.path().join("high.mqlite");

    assert!(
        Client::open_with_options(
            &zero_path,
            OpenOptions::new().delta_bearing_frames_warn_threshold(ZERO_THRESHOLD),
        )
        .is_err(),
        "zero threshold must be rejected"
    );
    assert!(
        Client::open_with_options(
            &high_path,
            OpenOptions::new().delta_bearing_frames_warn_threshold(ABOVE_ONE_THRESHOLD),
        )
        .is_err(),
        "thresholds above one must be rejected"
    );
}

#[test]
fn delta_bearing_frames_warn_threshold_accepts_upper_bound() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("valid.mqlite");

    Client::open_with_options(
        &path,
        OpenOptions::new().delta_bearing_frames_warn_threshold(VALID_THRESHOLD),
    )
    .expect("threshold at upper bound must be valid");
}
