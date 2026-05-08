#![cfg(any(test, feature = "test-hooks"))]

//! Test-only probes for Phase 5 US-019 overlap matrix coverage.
//!
//! These helpers live outside `page_latch.rs` so integration tests can exercise
//! private latch behavior without adding intrusive test-only code to the
//! production latch primitive.

use std::sync::{Arc, Barrier};
use std::thread;

use crate::error::{Error, Result, WriteConflictReason};

use super::page_latch::PageLatch;

const UPGRADE_RACE_ATTEMPTS: usize = 64;

/// Race two shared latch holders through `PageLatchShared::upgrade`.
///
/// # Errors
///
/// Returns an error if a worker panics or if any attempt fails to produce
/// exactly one winner and one `UpgradeRace` loser.
pub fn page_latch_upgrade_race_counts() -> Result<(usize, usize)> {
    let mut winners = 0usize;
    let mut upgrade_races = 0usize;

    for _ in 0..UPGRADE_RACE_ATTEMPTS {
        let latch = Arc::new(PageLatch::new());
        let barrier = Arc::new(Barrier::new(2));

        let mut handles = Vec::with_capacity(2);
        for _ in 0..2 {
            let latch = Arc::clone(&latch);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || -> Result<()> {
                let shared = latch.lock_shared();
                barrier.wait();
                let _exclusive = shared.upgrade()?;
                Ok(())
            }));
        }

        let mut attempt_winners = 0usize;
        let mut attempt_races = 0usize;
        for handle in handles {
            match handle.join() {
                Ok(Ok(())) => attempt_winners += 1,
                Ok(Err(Error::WriteConflict {
                    reason: WriteConflictReason::UpgradeRace,
                })) => attempt_races += 1,
                Ok(Err(err)) => return Err(err),
                Err(_) => return Err(Error::Internal("US-019 upgrade worker panicked".into())),
            }
        }

        if attempt_winners != 1 || attempt_races != 1 {
            return Err(Error::Internal(format!(
                "US-019 upgrade race expected one winner and one loser, got \
                 winners={attempt_winners}, upgrade_races={attempt_races}"
            )));
        }

        winners += attempt_winners;
        upgrade_races += attempt_races;
    }

    Ok((winners, upgrade_races))
}
