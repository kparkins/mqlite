//! Test-only probes for Phase 5 US-010 SMO classification and latching.
//!
//! This file intentionally contains the intrusive knobs used by
//! `tests/mwmr_reconcile_latching.rs`; production SMO code only records to or
//! consults this module behind `cfg(any(test, feature = "test-hooks"))`.

use std::collections::VecDeque;
use std::sync::{Mutex, MutexGuard};

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

fn events() -> MutexGuard<'static, Vec<Us010ProbeEvent>> {
    match EVENTS.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn classification_overrides() -> MutexGuard<'static, VecDeque<WriteShape>> {
    match CLASSIFICATION_OVERRIDES.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn revalidation_failures() -> MutexGuard<'static, u32> {
    match FORCE_REVALIDATION_FAILURES.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[doc(hidden)]
pub fn reset() {
    events().clear();
    classification_overrides().clear();
    *revalidation_failures() = 0;
}

#[doc(hidden)]
#[must_use]
pub fn drain_events() -> Vec<Us010ProbeEvent> {
    std::mem::take(&mut *events())
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
    classification_overrides().extend(mapped);
}

#[doc(hidden)]
pub fn force_revalidation_failures(count: u32) {
    *revalidation_failures() = count;
}

pub(super) fn record_exclusive_acquire(page_id: u32) {
    events().push(Us010ProbeEvent {
        kind: "exclusive_acquire",
        page_id: Some(page_id),
        phase: None,
        shape: None,
        attempt: None,
    });
}

pub(super) fn record_classification(phase: &'static str, shape: &WriteShape) {
    events().push(Us010ProbeEvent {
        kind: "classification",
        page_id: None,
        phase: Some(phase),
        shape: Some(format!("{shape:?}")),
        attempt: None,
    });
}

pub(super) fn record_reclassification(attempt: u32) {
    events().push(Us010ProbeEvent {
        kind: "reclassification",
        page_id: None,
        phase: None,
        shape: None,
        attempt: Some(attempt),
    });
}

pub(super) fn override_classification(_phase: &'static str) -> Option<WriteShape> {
    classification_overrides().pop_front()
}

pub(super) fn force_revalidation_failure_once() -> bool {
    let mut remaining = revalidation_failures();
    if *remaining == 0 {
        return false;
    }
    *remaining -= 1;
    true
}
