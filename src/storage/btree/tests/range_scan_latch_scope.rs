//! Test-only probes for Phase 5 US-016 reader latch scope.
//!
//! Production code calls this module only behind `cfg(any(test,
//! feature = "test-hooks"))`. Integration tests use the probes to prove
//! range scans release their shared page latch after copying page bytes and
//! cloning chain snapshots, before row iteration or BSON decode begins.

use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use crate::error::{Error, Result};

/// One observed shared-latch hold around a leaf copy/snapshot step.
#[derive(Clone, Debug)]
#[doc(hidden)]
pub struct Us016ReadLatchSample {
    /// Leaf page id whose shared latch was held.
    pub page_id: u32,
    /// B-tree level, where `0` is a leaf.
    pub level: u8,
    /// Time spent between the read-path copy start and latch release.
    pub hold_duration: Duration,
}

#[doc(hidden)]
pub struct Us016LeafHoldStart {
    page_id: u32,
    level: u8,
    started_at: Instant,
}

struct RangeScanPauseHook {
    ready: Sender<()>,
    release: Receiver<()>,
}

/// RAII guard for a one-shot range-scan pause hook.
#[doc(hidden)]
pub struct Us016RangeScanPauseGuard;

static LATCH_SAMPLES: Mutex<Vec<Us016ReadLatchSample>> = Mutex::new(Vec::new());
static RANGE_SCAN_PAUSE: Mutex<Option<RangeScanPauseHook>> = Mutex::new(None);

fn latch_samples() -> MutexGuard<'static, Vec<Us016ReadLatchSample>> {
    match LATCH_SAMPLES.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn range_scan_pause() -> MutexGuard<'static, Option<RangeScanPauseHook>> {
    match RANGE_SCAN_PAUSE.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[doc(hidden)]
pub fn reset() {
    latch_samples().clear();
    range_scan_pause().take();
}

#[doc(hidden)]
#[must_use]
pub fn drain_latch_samples() -> Vec<Us016ReadLatchSample> {
    std::mem::take(&mut *latch_samples())
}

#[doc(hidden)]
#[must_use]
pub fn install_range_scan_iteration_pause(
    ready: Sender<()>,
    release: Receiver<()>,
) -> Us016RangeScanPauseGuard {
    *range_scan_pause() = Some(RangeScanPauseHook { ready, release });
    Us016RangeScanPauseGuard
}

pub(crate) fn begin_leaf_hold(page_id: u32, level: u8) -> Us016LeafHoldStart {
    Us016LeafHoldStart {
        page_id,
        level,
        started_at: Instant::now(),
    }
}

pub(crate) fn finish_leaf_hold(start: Us016LeafHoldStart) {
    latch_samples().push(Us016ReadLatchSample {
        page_id: start.page_id,
        level: start.level,
        hold_duration: start.started_at.elapsed(),
    });
}

pub(super) fn pause_before_iteration() -> Result<()> {
    let Some(hook) = RANGE_SCAN_PAUSE
        .lock()
        .map_err(|_| Error::Internal("US-016 range-scan pause mutex poisoned".into()))?
        .take()
    else {
        return Ok(());
    };

    hook.ready
        .send(())
        .map_err(|_| Error::Internal("US-016 range-scan pause ready receiver dropped".into()))?;
    hook.release
        .recv()
        .map_err(|_| Error::Internal("US-016 range-scan pause release sender dropped".into()))
}

impl Drop for Us016RangeScanPauseGuard {
    fn drop(&mut self) {
        range_scan_pause().take();
    }
}
