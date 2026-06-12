//! F0 test-only failpoint inside the checkpoint spill/relief window.
//!
//! `stage_checkpoint_pre_mutation` has a window of fallible work (journal
//! sync, LSN stamps, the spill-required and relief reconcile passes) whose
//! `?` early-returns surface as recoverable checkpoint failures. The F0
//! regression guard arms this failpoint to fail inside that window and
//! asserts the structural batch's drained deferred-free pages are not
//! leaked. Kept in its own file so intrusive test plumbing stays out of the
//! production checkpoint path.
//!
//! The flag is thread-local: `PagedEngine::checkpoint` runs on the calling
//! thread, so arming it in one test can never fail a checkpoint issued by a
//! concurrently running test.

use std::cell::Cell;

use crate::error::{Error, Result};

thread_local! {
    static SPILL_RELIEF_WINDOW_FAILURE: Cell<bool> = const { Cell::new(false) };
}

/// Arm one injected failure for the next checkpoint this thread runs
/// through the spill/relief window of `stage_checkpoint_pre_mutation`.
#[allow(
    dead_code,
    reason = "armed only by cfg(test) suites; compiled under test-hooks for parity"
)]
pub(crate) fn arm_spill_relief_window_failure() {
    SPILL_RELIEF_WINDOW_FAILURE.with(|armed| armed.set(true));
}

/// Consume the armed flag, failing the caller exactly once.
pub(crate) fn fail_if_armed() -> Result<()> {
    let armed = SPILL_RELIEF_WINDOW_FAILURE.with(|armed| armed.replace(false));
    if armed {
        return Err(Error::Internal(
            "injected checkpoint spill/relief-window failure".into(),
        ));
    }
    Ok(())
}
