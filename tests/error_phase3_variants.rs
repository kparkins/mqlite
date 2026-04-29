//! Regression coverage for Phase 3 error variants from US-002.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    reason = "test and bench targets use assertion-style panics and setup unwraps"
)]

use mqlite::Error;

#[test]
fn phase3_buffer_pool_eviction_blocked_display_is_stable() {
    let err = Error::BufferPoolEvictionBlocked {
        page: 42,
        reason: "live committed delta head",
    };

    assert_eq!(
        err.to_string(),
        "buffer-pool eviction blocked for page 42: live committed delta head"
    );
    assert_eq!(err.code(), None);
}

#[test]
fn phase3_recovery_pool_exhausted_display_has_operator_guidance() {
    let err = Error::RecoveryPoolExhausted;

    assert_eq!(
        err.to_string(),
        "recovery pool exhausted: logical replay would exceed \
         BufferPool::config.max_pool_bytes; increase max_pool_bytes or perform \
         a forced reconcile on the previous open before closing"
    );
    assert_eq!(err.code(), None);
}

#[test]
fn phase3_engine_fatal_display_describes_reopen_requirement() {
    let err = Error::EngineFatal;

    assert_eq!(
        err.to_string(),
        "engine fatal: post-durable in-memory state could not be repaired; \
         the engine is poisoned, refuses new operations, and must be reopened"
    );
    assert_eq!(err.code(), None);
}
