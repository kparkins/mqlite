//! Test-only probes for Phase 5 US-010 SMO classification and latching.
//!
//! This file intentionally contains the intrusive knobs used by
//! `tests/mwmr_reconcile_latching.rs`; production SMO code only records to or
//! consults this module behind `cfg(any(test, feature = "test-hooks"))`.

use std::collections::VecDeque;
use std::sync::Mutex;

use super::smo_latch::WriteShape;

/// One US-010 probe event.
#[derive(Clone, Debug, PartialEq, Eq)]
#[doc(hidden)]
pub struct Us010ProbeEvent {
    /// Event kind.
    pub kind: &'static str,
    /// Page id for latch events.
    pub page_id: Option<u32>,
    /// Classification phase for shape events.
    pub phase: Option<&'static str>,
    /// Debug rendering of the classified shape.
    pub shape: Option<String>,
    /// Reclassification attempt number.
    pub attempt: Option<u32>,
}

static EVENTS: Mutex<Vec<Us010ProbeEvent>> = Mutex::new(Vec::new());
static CLASSIFICATION_OVERRIDES: Mutex<VecDeque<WriteShape>> = Mutex::new(VecDeque::new());
static FORCE_REVALIDATION_FAILURES: Mutex<u32> = Mutex::new(0);

#[doc(hidden)]
pub fn reset() {
    EVENTS.lock().expect("US-010 events mutex poisoned").clear();
    CLASSIFICATION_OVERRIDES
        .lock()
        .expect("US-010 overrides mutex poisoned")
        .clear();
    *FORCE_REVALIDATION_FAILURES
        .lock()
        .expect("US-010 revalidation mutex poisoned") = 0;
}

#[doc(hidden)]
pub fn drain_events() -> Vec<Us010ProbeEvent> {
    std::mem::take(&mut *EVENTS.lock().expect("US-010 events mutex poisoned"))
}

#[doc(hidden)]
#[allow(
    clippy::panic,
    reason = "test-hook override names are hard-coded by regression tests"
)]
pub fn push_classification_override_names(shapes: &[&str]) {
    let mapped = shapes.iter().map(|shape| match *shape {
        "RootNeutral" => WriteShape::RootNeutral,
        "LeafSplit" => WriteShape::LeafSplit,
        "LeafMerge" => WriteShape::LeafMerge,
        "OverflowChange" => WriteShape::OverflowChange,
        other => panic!("unsupported US-010 WriteShape override: {other}"),
    });
    CLASSIFICATION_OVERRIDES
        .lock()
        .expect("US-010 overrides mutex poisoned")
        .extend(mapped);
}

#[doc(hidden)]
pub fn force_revalidation_failures(count: u32) {
    *FORCE_REVALIDATION_FAILURES
        .lock()
        .expect("US-010 revalidation mutex poisoned") = count;
}

pub(super) fn record_exclusive_acquire(page_id: u32) {
    EVENTS
        .lock()
        .expect("US-010 events mutex poisoned")
        .push(Us010ProbeEvent {
            kind: "exclusive_acquire",
            page_id: Some(page_id),
            phase: None,
            shape: None,
            attempt: None,
        });
}

pub(super) fn record_classification(phase: &'static str, shape: &WriteShape) {
    EVENTS
        .lock()
        .expect("US-010 events mutex poisoned")
        .push(Us010ProbeEvent {
            kind: "classification",
            page_id: None,
            phase: Some(phase),
            shape: Some(format!("{shape:?}")),
            attempt: None,
        });
}

pub(super) fn record_reclassification(attempt: u32) {
    EVENTS
        .lock()
        .expect("US-010 events mutex poisoned")
        .push(Us010ProbeEvent {
            kind: "reclassification",
            page_id: None,
            phase: None,
            shape: None,
            attempt: Some(attempt),
        });
}

pub(super) fn override_classification(_phase: &'static str) -> Option<WriteShape> {
    CLASSIFICATION_OVERRIDES
        .lock()
        .expect("US-010 overrides mutex poisoned")
        .pop_front()
}

pub(super) fn force_revalidation_failure_once() -> bool {
    let mut remaining = FORCE_REVALIDATION_FAILURES
        .lock()
        .expect("US-010 revalidation mutex poisoned");
    if *remaining == 0 {
        return false;
    }
    *remaining -= 1;
    true
}
