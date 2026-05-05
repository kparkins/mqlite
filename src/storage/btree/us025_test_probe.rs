//! Test-only probes for Phase 5 US-025 reader crabbing.
//!
//! Production traversal records here only behind `cfg(any(test,
//! feature = "test-hooks"))`; integration tests drain the events to prove
//! readers acquire a child shared latch before releasing the parent.

use std::sync::Mutex;

/// One reader-crabbing event.
#[derive(Clone, Debug, PartialEq, Eq)]
#[doc(hidden)]
pub struct Us025CrabbingEvent {
    /// Event kind.
    pub kind: &'static str,
    /// Parent page for parent-release events.
    pub parent_page: Option<u32>,
    /// Child page for parent-release events.
    pub child_page: Option<u32>,
    /// Page acquired in shared mode.
    pub page_id: Option<u32>,
    /// B-tree level of the acquired page.
    pub level: Option<u8>,
}

static EVENTS: Mutex<Vec<Us025CrabbingEvent>> = Mutex::new(Vec::new());

#[doc(hidden)]
pub fn reset() {
    EVENTS.lock().expect("US-025 events mutex poisoned").clear();
}

#[doc(hidden)]
pub fn drain_events() -> Vec<Us025CrabbingEvent> {
    std::mem::take(&mut *EVENTS.lock().expect("US-025 events mutex poisoned"))
}

pub(super) fn record_shared_acquire(page_id: u32, level: u8) {
    EVENTS
        .lock()
        .expect("US-025 events mutex poisoned")
        .push(Us025CrabbingEvent {
            kind: "shared_acquire",
            parent_page: None,
            child_page: None,
            page_id: Some(page_id),
            level: Some(level),
        });
}

pub(super) fn record_parent_release_after_child(parent_page: u32, child_page: u32) {
    EVENTS
        .lock()
        .expect("US-025 events mutex poisoned")
        .push(Us025CrabbingEvent {
            kind: "parent_release_after_child",
            parent_page: Some(parent_page),
            child_page: Some(child_page),
            page_id: None,
            level: None,
        });
}
