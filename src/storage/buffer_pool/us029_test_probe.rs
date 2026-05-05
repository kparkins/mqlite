//! US-029 (§10.18) drop-order probe.
//!
//! Records the order in which `LatchedPinnedPage::drop` releases its
//! latch hold and its pin. The probe is `cfg(test)` only; production
//! builds compile without these helpers and without the recording calls
//! in `Drop`.
//!
//! Tests use [`drain_events`] after dropping a `LatchedPinnedPage` to
//! verify the order. The probe is per-thread because `LatchedPinnedPage`
//! is `!Send` — every drop happens on the constructing thread, so a
//! `thread_local!` `RefCell` is sufficient and avoids cross-test
//! interference between concurrent tests.

use std::cell::RefCell;

/// Event tag emitted when the latch hold is released in `Drop`.
pub(super) const EVENT_LATCH_RELEASE: &str = "latch_release";

/// Event tag emitted when the pin is decremented in `Drop` (after the
/// latch has already been released).
pub(super) const EVENT_PIN_RELEASE: &str = "pin_release";

thread_local! {
    static EVENTS: RefCell<Vec<&'static str>> = const { RefCell::new(Vec::new()) };
}

/// Append `event` to this thread's recorded drop sequence.
pub(super) fn record_drop_event(event: &'static str) {
    EVENTS.with(|cell| cell.borrow_mut().push(event));
}

/// Take and clear the current thread's recorded drop sequence.
pub(super) fn drain_events() -> Vec<&'static str> {
    EVENTS.with(|cell| std::mem::take(&mut *cell.borrow_mut()))
}
