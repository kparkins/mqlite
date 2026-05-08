//! Phase 3 US-015 public configuration validation tests.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    reason = "test and bench targets use assertion-style panics and setup unwraps"
)]

use mqlite::{Client, OpenOptions};

const VALID_THRESHOLD: f64 = 1.0;
const ZERO_THRESHOLD: f64 = 0.0;
const ABOVE_ONE_THRESHOLD: f64 = 1.1;
const NON_FINITE_THRESHOLDS: [f64; 3] = [f64::NAN, f64::INFINITY, f64::NEG_INFINITY];

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

    for (i, threshold) in NON_FINITE_THRESHOLDS.iter().enumerate() {
        let path = dir.path().join(format!("non-finite-{i}.mqlite"));
        assert!(
            Client::open_with_options(
                &path,
                OpenOptions::new().delta_bearing_frames_warn_threshold(*threshold),
            )
            .is_err(),
            "non-finite threshold {threshold:?} must be rejected"
        );
    }
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
