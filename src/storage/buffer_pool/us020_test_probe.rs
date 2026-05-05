//! US-020 test probes for the page-local latch.
//!
//! These helpers keep starvation and upgrade-race stress scaffolding outside
//! `page_latch.rs` while still allowing the integration matrix to exercise the
//! real latch primitive under the plain release test gate.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Barrier};
use std::thread;
use std::time::Duration;

use crate::error::{Error, Result, WriteConflictReason};

use super::page_latch::PageLatch;

/// Progress counters returned by the US-020 upgrade-race probe.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Us020UpgradeRaceProgress {
    /// Number of successful shared-to-exclusive upgrades.
    pub winners: u64,
    /// Number of losers that returned `WriteConflictReason::UpgradeRace`.
    pub losers: u64,
}

/// Run a bounded reader-pressure probe and return the number of reader cycles.
///
/// # Errors
///
/// Returns an internal error if the exclusive waiter cannot acquire within
/// `timeout_ms` or if a probe thread panics.
#[doc(hidden)]
pub fn us020_writer_preference_bounds_reader_starvation(
    reader_count: usize,
    timeout_ms: u64,
) -> Result<u64> {
    if reader_count == 0 {
        return Err(Error::Internal(
            "US-020 reader-count probe requires at least one reader".into(),
        ));
    }

    let latch = Arc::new(PageLatch::new());
    let stop = Arc::new(AtomicBool::new(false));
    let reader_cycles = Arc::new(AtomicU64::new(0));
    let mut readers = Vec::with_capacity(reader_count);

    for _ in 0..reader_count {
        let reader_latch = Arc::clone(&latch);
        let reader_stop = Arc::clone(&stop);
        let reader_counted = Arc::clone(&reader_cycles);
        readers.push(thread::spawn(move || {
            while !reader_stop.load(Ordering::Acquire) {
                let _shared = reader_latch.lock_shared();
                reader_counted.fetch_add(1, Ordering::AcqRel);
                thread::yield_now();
            }
        }));
    }

    while reader_cycles.load(Ordering::Acquire) < reader_count as u64 {
        thread::yield_now();
    }

    let (tx, rx) = mpsc::channel();
    let writer_latch = Arc::clone(&latch);
    let writer = thread::spawn(move || {
        let _exclusive = writer_latch.lock_exclusive();
        let _ = tx.send(());
    });

    if rx.recv_timeout(Duration::from_millis(timeout_ms)).is_err() {
        stop.store(true, Ordering::Release);
        join_reader_threads(readers)?;
        return Err(Error::Internal(
            "US-020 exclusive latch waiter starved under readers".into(),
        ));
    }

    stop.store(true, Ordering::Release);
    join_reader_threads(readers)?;
    writer
        .join()
        .map_err(|_| Error::Internal("US-020 exclusive waiter panicked".into()))?;
    Ok(reader_cycles.load(Ordering::Acquire))
}

/// Run repeated two-reader upgrade races and return aggregate progress.
///
/// # Errors
///
/// Returns an internal error if any race fails to produce exactly one winner
/// and exactly one `UpgradeRace` loser, or if a probe thread panics.
#[doc(hidden)]
pub fn us020_upgrade_loser_backoff_progress(rounds: usize) -> Result<Us020UpgradeRaceProgress> {
    let mut progress = Us020UpgradeRaceProgress {
        winners: 0,
        losers: 0,
    };

    for _ in 0..rounds {
        let latch = Arc::new(PageLatch::new());
        let barrier = Arc::new(Barrier::new(2));
        let mut handles = Vec::with_capacity(2);

        for _ in 0..2 {
            let worker_latch = Arc::clone(&latch);
            let worker_barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || -> Result<()> {
                let shared = worker_latch.lock_shared();
                worker_barrier.wait();
                let _exclusive = shared.upgrade()?;
                Ok(())
            }));
        }

        let results: Vec<Result<()>> = handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .map_err(|_| Error::Internal("US-020 upgrade worker panicked".into()))?
            })
            .collect();

        let winners = results.iter().filter(|result| result.is_ok()).count() as u64;
        let losers = results
            .iter()
            .filter(|result| {
                matches!(
                    result,
                    Err(Error::WriteConflict {
                        reason: WriteConflictReason::UpgradeRace
                    })
                )
            })
            .count() as u64;

        if winners != 1 || losers != 1 {
            return Err(Error::Internal(format!(
                "US-020 upgrade race expected one winner and one loser, got winners={winners} losers={losers}",
            )));
        }
        progress.winners = progress.winners.saturating_add(winners);
        progress.losers = progress.losers.saturating_add(losers);
    }

    Ok(progress)
}

fn join_reader_threads(readers: Vec<thread::JoinHandle<()>>) -> Result<()> {
    for reader in readers {
        reader
            .join()
            .map_err(|_| Error::Internal("US-020 reader probe panicked".into()))?;
    }
    Ok(())
}
