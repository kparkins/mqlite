//! Crash Recovery Testing — 500 cycles, 10 scenarios.
//!
//! Implements Jepsen-style crash injection against the mqlite WAL layer.
//! For each cycle the test:
//!
//!   1. Sets up a fresh database directory with pre-committed "epoch-1" data
//!      in the WAL (5 pages, fill byte derived from the cycle seed).
//!   2. `fork()`s a child process that opens the WAL (triggering recovery of
//!      epoch-1) and then runs a scenario-specific "operation" — writing some
//!      frames to the WAL, or directly to the main file during a simulated
//!      checkpoint.
//!   3. The parent SIGKILLs the child at the scenario's injection point.
//!   4. The parent re-opens the WAL (triggering recovery again).
//!   5. The parent validates all five correctness conditions:
//!
//!      (a) Database opens without error after crash.
//!      (b) WAL replay does not fail (covered by (a) succeeding).
//!      (c) Committed data is present in the main file.
//!      (d) Uncommitted data does not appear (no phantom pages in WAL SHM).
//!      (e) Index pages are absent when the index build was uncommitted.
//!
//! ## Scenarios (10 total, 50 cycles each = 500 total)
//!
//! | # | Name                    | Injection point                                        |
//! |---|-------------------------|--------------------------------------------------------|
//! | 1 | InsertAtFrame0          | Kill before any WAL frame is written for the insert    |
//! | 2 | InsertAtFrame10         | Kill after 10 uncommitted WAL frames                   |
//! | 3 | InsertAtFrame100        | Kill after 100 uncommitted WAL frames                  |
//! | 4 | InsertAtFinalFrame      | Kill AFTER the commit frame (data must survive)        |
//! | 5 | CheckpointAt25Pct       | Kill after 25 % of pages are written to main (chkpt)   |
//! | 6 | CheckpointAt50Pct       | Kill after 50 % of pages are written to main (chkpt)   |
//! | 7 | CheckpointAt75Pct       | Kill after 75 % of pages are written to main (chkpt)   |
//! | 8 | IndexBuildAtStart       | Kill before any index frame is written                 |
//! | 9 | IndexBuildMidway        | Kill after 5 uncommitted index frames                  |
//! |10 | IndexBuildAtEnd         | Kill after all 10 uncommitted index frames (no commit)  |
//!
//! ## Determinism
//!
//! Each cycle is given a `seed` value equal to the cycle index (0–49).  The
//! seed controls the page fill bytes so that failures are reproducible:
//! re-running the test with the same build produces identical WAL contents.

// Unix-only: uses fork()/SIGKILL for crash simulation.
// Test module: visible to the Rust test harness, not exported.
#![cfg(unix)]

use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::error::{Error, Result};
use crate::storage::header::{FileHeader, HEADER_PAGE_SIZE};
use crate::storage::page::PAGE_SIZE_INTERNAL;
use crate::wal::wal_file::WalPageSize;
use crate::wal::{wal_path_for, write_page_to_main, WalManager};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Number of crash-inject cycles per scenario (50 × 10 = 500 total).
const CYCLES_PER_SCENARIO: u32 = 50;

/// Page numbers used for pre-committed epoch-1 data (pages 1–5).
const EPOCH1_START: u32 = 1;
const EPOCH1_END: u32 = 6; // exclusive

/// Page numbers used for epoch-2 operations (insert / checkpoint).
const EPOCH2_START: u32 = 6;
const EPOCH2_END: u32 = 21; // exclusive (pages 6–20)

/// Page numbers used for "index build" operations (pages 100–109).
const INDEX_START: u32 = 100;
const INDEX_END: u32 = 110; // exclusive

/// Total pages written during checkpoint simulation (pages 1–20).
const CHECKPOINT_PAGES: u32 = 20;

/// Fixed WAL salts used across all cycles for reproducibility.
const SALT1: u32 = 0xDEAD_BEEF;
const SALT2: u32 = 0xCAFE_BABE;

// ---------------------------------------------------------------------------
// Seed-derived fill bytes
// ---------------------------------------------------------------------------

/// Fill byte for epoch-1 committed pages (stable, pre-operation data).
fn epoch1_fill(seed: u32) -> u8 {
    ((seed % 200) + 1) as u8
}

/// Fill byte for epoch-2 committed pages (distinct from epoch-1).
fn epoch2_fill(seed: u32) -> u8 {
    (((seed + 100) % 200) + 1) as u8
}

/// Fill byte for uncommitted operation data (never survives recovery).
fn uncommitted_fill(seed: u32) -> u8 {
    (((seed + 50) % 200) + 1) as u8
}

/// Fill byte written to the main file during the simulated checkpoint.
/// Must differ from epoch1_fill / epoch2_fill to be detectable in validation.
const CHECKPOINT_GARBAGE_FILL: u8 = 0xDE;

// ---------------------------------------------------------------------------
// Scenario enum
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
enum Scenario {
    /// Kill before any WAL frame is written for the insert.
    InsertAtFrame0,
    /// Kill after 10 uncommitted WAL frames.
    InsertAtFrame10,
    /// Kill after 100 uncommitted WAL frames.
    InsertAtFrame100,
    /// Kill AFTER the commit frame has been fsynced — committed data must survive.
    InsertAtFinalFrame,
    /// Checkpoint interrupted after 25 % of pages are written to main.
    CheckpointAt25Pct,
    /// Checkpoint interrupted after 50 % of pages are written to main.
    CheckpointAt50Pct,
    /// Checkpoint interrupted after 75 % of pages are written to main.
    CheckpointAt75Pct,
    /// Index build killed before writing any index WAL frame.
    IndexBuildAtStart,
    /// Index build killed after 5 uncommitted index frames.
    IndexBuildMidway,
    /// Index build killed after all 10 uncommitted index frames (never committed).
    IndexBuildAtEnd,
}

const ALL_SCENARIOS: [Scenario; 10] = [
    Scenario::InsertAtFrame0,
    Scenario::InsertAtFrame10,
    Scenario::InsertAtFrame100,
    Scenario::InsertAtFinalFrame,
    Scenario::CheckpointAt25Pct,
    Scenario::CheckpointAt50Pct,
    Scenario::CheckpointAt75Pct,
    Scenario::IndexBuildAtStart,
    Scenario::IndexBuildMidway,
    Scenario::IndexBuildAtEnd,
];

// ---------------------------------------------------------------------------
// Epoch-1 setup: write pre-committed data to the WAL before fork.
// ---------------------------------------------------------------------------

/// Initialise the database files and write five committed epoch-1 pages to the
/// WAL.  The WalManager is **dropped without checkpointing** so that the WAL
/// file remains on disk and recovery will replay it on the next open.
fn setup_epoch1(db_path: &Path, seed: u32) -> Result<()> {
    // Create the main file and pre-allocate space for 250 pages.
    let mut main_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(db_path)
        .map_err(Error::Io)?;
    main_file
        .set_len(250 * PAGE_SIZE_INTERNAL as u64)
        .map_err(Error::Io)?;

    // Write a valid FileHeader at page 0.
    let header = FileHeader::new(1_700_000_000_000, SALT1, SALT2);
    main_file.seek(SeekFrom::Start(0)).map_err(Error::Io)?;
    main_file.write_all(&header.to_bytes()).map_err(Error::Io)?;
    main_file.flush().map_err(Error::Io)?;

    // Open WAL and write epoch-1 committed frames (pages 1–5).
    let mut wal = WalManager::open_or_create(db_path, &header, &mut main_file)?;
    let page_data = vec![epoch1_fill(seed); PAGE_SIZE_INTERNAL as usize];

    for page_no in EPOCH1_START..(EPOCH1_END - 1) {
        wal.append_non_commit(page_no, WalPageSize::Small4k, &page_data)?;
    }
    // The last epoch-1 page carries the commit flag.
    wal.commit(
        EPOCH1_END - 1,
        WalPageSize::Small4k,
        &page_data,
        EPOCH1_END - 1, // db_page_count after this commit
    )?;

    // Drop WalManager WITHOUT checkpointing so the WAL stays on disk.
    drop(wal);
    drop(main_file);
    Ok(())
}

// ---------------------------------------------------------------------------
// Child: scenario-specific operation after fork.
// ---------------------------------------------------------------------------

/// Run the scenario in the child process.
///
/// The child opens the WAL (recovery replays epoch-1 into the main file), then
/// executes the scenario-specific write sequence, sending one byte on `write_fd`
/// after each step so the parent can synchronise the kill point.
///
/// The function never returns — it calls `libc::_exit(0)` when done.
///
/// # Safety
/// Must only be called from the child side of `fork()`.
/// Uses `libc::_exit` to avoid running Rust global destructors.
unsafe fn child_run_scenario(db_path: &Path, scenario: Scenario, seed: u32, write_fd: libc::c_int) {
    // Helper: send one synchronisation byte to the parent.
    macro_rules! step {
        () => {{
            let b: u8 = 1;
            libc::write(write_fd, &b as *const u8 as *const libc::c_void, 1);
        }};
    }

    // Open main file.
    let mut main_file = match OpenOptions::new().read(true).write(true).open(db_path) {
        Ok(f) => f,
        Err(_) => libc::_exit(2),
    };

    // Reconstruct the header with the known fixed salts.
    let header = FileHeader::new(1_700_000_000_000, SALT1, SALT2);

    // Open WAL — this triggers recovery of epoch-1 (writes pages 1–5 to main).
    let mut wal = match WalManager::open_or_create(db_path, &header, &mut main_file) {
        Ok(w) => w,
        Err(_) => libc::_exit(3),
    };

    let uc_fill = uncommitted_fill(seed);
    let e2_fill = epoch2_fill(seed);

    match scenario {
        // ----------------------------------------------------------------
        // Insert scenarios
        // ----------------------------------------------------------------
        Scenario::InsertAtFrame0 => {
            // No frames written — signal once then wait for kill.
            step!();
            std::thread::sleep(std::time::Duration::from_secs(60));
        }

        Scenario::InsertAtFrame10 => {
            let page_data = vec![uc_fill; PAGE_SIZE_INTERNAL as usize];
            for i in 0u32..10 {
                let _ = wal.append_non_commit(EPOCH2_START + i, WalPageSize::Small4k, &page_data);
                step!();
            }
            std::thread::sleep(std::time::Duration::from_secs(60));
        }

        Scenario::InsertAtFrame100 => {
            let page_data = vec![uc_fill; PAGE_SIZE_INTERNAL as usize];
            let span = EPOCH2_END - EPOCH2_START; // 15
            for i in 0u32..100 {
                let page_no = EPOCH2_START + (i % span);
                let _ = wal.append_non_commit(page_no, WalPageSize::Small4k, &page_data);
                step!();
            }
            std::thread::sleep(std::time::Duration::from_secs(60));
        }

        Scenario::InsertAtFinalFrame => {
            // Write 5 non-commit + 1 commit frame.  Signal AFTER the commit.
            let page_data = vec![e2_fill; PAGE_SIZE_INTERNAL as usize];
            for i in 0u32..5 {
                let _ = wal.append_non_commit(EPOCH2_START + i, WalPageSize::Small4k, &page_data);
            }
            let _ = wal.commit(
                EPOCH2_START + 5,
                WalPageSize::Small4k,
                &page_data,
                EPOCH2_START + 5,
            );
            step!(); // signal after commit
            std::thread::sleep(std::time::Duration::from_secs(60));
        }

        // ----------------------------------------------------------------
        // Checkpoint scenarios
        // ----------------------------------------------------------------
        Scenario::CheckpointAt25Pct | Scenario::CheckpointAt50Pct | Scenario::CheckpointAt75Pct => {
            // First, commit a second batch of data (epoch-2, pages 6–20).
            let epoch2_data = vec![e2_fill; PAGE_SIZE_INTERNAL as usize];
            let e2_span = EPOCH2_END - EPOCH2_START; // 15 pages
            for i in 0..(e2_span - 1) {
                let _ = wal.append_non_commit(EPOCH2_START + i, WalPageSize::Small4k, &epoch2_data);
            }
            let _ = wal.commit(
                EPOCH2_START + e2_span - 1,
                WalPageSize::Small4k,
                &epoch2_data,
                EPOCH2_START + e2_span - 1,
            );

            // Simulate the checkpoint: write garbage bytes directly to the
            // main file for each of the 20 pages (1–20).  Send a step signal
            // after each page write so the parent can kill mid-checkpoint.
            let garbage = vec![CHECKPOINT_GARBAGE_FILL; PAGE_SIZE_INTERNAL as usize];
            for page_no in 1..=CHECKPOINT_PAGES {
                let _ = write_page_to_main(
                    &mut main_file,
                    page_no,
                    PAGE_SIZE_INTERNAL as usize,
                    &garbage,
                );
                step!();
            }
            std::thread::sleep(std::time::Duration::from_secs(60));
        }

        // ----------------------------------------------------------------
        // Index build scenarios
        // ----------------------------------------------------------------
        Scenario::IndexBuildAtStart => {
            // No index frames — signal once then wait.
            step!();
            std::thread::sleep(std::time::Duration::from_secs(60));
        }

        Scenario::IndexBuildMidway => {
            let page_data = vec![uc_fill; PAGE_SIZE_INTERNAL as usize];
            for i in 0u32..5 {
                let _ = wal.append_non_commit(INDEX_START + i, WalPageSize::Small4k, &page_data);
                step!();
            }
            std::thread::sleep(std::time::Duration::from_secs(60));
        }

        Scenario::IndexBuildAtEnd => {
            // Write all 10 index frames but DO NOT commit.
            let page_data = vec![uc_fill; PAGE_SIZE_INTERNAL as usize];
            for i in 0u32..(INDEX_END - INDEX_START) {
                let _ = wal.append_non_commit(INDEX_START + i, WalPageSize::Small4k, &page_data);
                step!();
            }
            std::thread::sleep(std::time::Duration::from_secs(60));
        }
    }

    libc::_exit(0);
}

// ---------------------------------------------------------------------------
// Validation helpers
// ---------------------------------------------------------------------------

/// Read one `PAGE_SIZE_INTERNAL`-byte page from the main file.
fn read_main_page(file: &mut std::fs::File, page_no: u32) -> Result<Vec<u8>> {
    let offset = page_no as u64 * PAGE_SIZE_INTERNAL as u64;
    file.seek(SeekFrom::Start(offset)).map_err(Error::Io)?;
    let mut buf = vec![0u8; PAGE_SIZE_INTERNAL as usize];
    file.read_exact(&mut buf).map_err(Error::Io)?;
    Ok(buf)
}

/// Validate all five crash-recovery correctness conditions.
fn validate(
    wal: &WalManager,
    main_file: &mut std::fs::File,
    scenario: Scenario,
    seed: u32,
) -> Result<()> {
    let e1_fill = epoch1_fill(seed);
    let e2_fill = epoch2_fill(seed);

    // ---- Condition (c): committed data is present ----------------------------

    // Epoch-1 pages (1–5) must always be correct after recovery.
    for page_no in EPOCH1_START..EPOCH1_END {
        let page = read_main_page(main_file, page_no)?;
        if page[0] != e1_fill {
            return Err(Error::Internal(format!(
                "condition (c) FAIL: epoch-1 page {} fill={:#04x} want={:#04x} \
                 [scenario {:?} seed {}]",
                page_no, page[0], e1_fill, scenario, seed
            )));
        }
    }

    // InsertAtFinalFrame committed epoch-2 pages (6–11) before crash.
    if matches!(scenario, Scenario::InsertAtFinalFrame) {
        for page_no in EPOCH2_START..(EPOCH2_START + 6) {
            let page = read_main_page(main_file, page_no)?;
            if page[0] != e2_fill {
                return Err(Error::Internal(format!(
                    "condition (c) FAIL: InsertAtFinalFrame page {} fill={:#04x} want={:#04x} \
                     [seed {}]",
                    page_no, page[0], e2_fill, seed
                )));
            }
        }
    }

    // Checkpoint scenarios committed epoch-2 pages (6–20) before crashing.
    if matches!(
        scenario,
        Scenario::CheckpointAt25Pct | Scenario::CheckpointAt50Pct | Scenario::CheckpointAt75Pct
    ) {
        for page_no in EPOCH2_START..EPOCH2_END {
            let page = read_main_page(main_file, page_no)?;
            if page[0] != e2_fill {
                return Err(Error::Internal(format!(
                    "condition (c) FAIL: checkpoint page {} fill={:#04x} want={:#04x} \
                     [scenario {:?} seed {}]",
                    page_no, page[0], e2_fill, scenario, seed
                )));
            }
        }
    }

    // ---- Condition (d): checkpoint garbage is not visible --------------------
    // The partial-checkpoint garbage fill (0xDE) must not survive WAL replay.

    if matches!(
        scenario,
        Scenario::CheckpointAt25Pct | Scenario::CheckpointAt50Pct | Scenario::CheckpointAt75Pct
    ) {
        for page_no in 1..=CHECKPOINT_PAGES {
            let page = read_main_page(main_file, page_no)?;
            if page[0] == CHECKPOINT_GARBAGE_FILL {
                return Err(Error::Internal(format!(
                    "condition (d) FAIL: checkpoint garbage fill {:#04x} found at page {} \
                     after WAL recovery [scenario {:?} seed {}]",
                    CHECKPOINT_GARBAGE_FILL, page_no, scenario, seed
                )));
            }
        }
    }

    // ---- Condition (d): uncommitted WAL frames are not in SHM ---------------

    // Insert-at-frame-10: pages 6–15 were written uncommitted.
    if matches!(scenario, Scenario::InsertAtFrame10) {
        for i in 0u32..10 {
            let page_no = EPOCH2_START + i;
            if wal.shm().lookup(page_no).is_some() {
                return Err(Error::Internal(format!(
                    "condition (d) FAIL: uncommitted page {} in WAL SHM after recovery \
                     [InsertAtFrame10 seed {}]",
                    page_no, seed
                )));
            }
        }
    }

    // Insert-at-frame-100: pages 6–20 were written uncommitted (cycling).
    if matches!(scenario, Scenario::InsertAtFrame100) {
        for page_no in EPOCH2_START..EPOCH2_END {
            if wal.shm().lookup(page_no).is_some() {
                return Err(Error::Internal(format!(
                    "condition (d) FAIL: uncommitted page {} in WAL SHM after recovery \
                     [InsertAtFrame100 seed {}]",
                    page_no, seed
                )));
            }
        }
    }

    // ---- Condition (e): uncommitted index pages are absent from SHM ----------

    let is_index_scenario = matches!(
        scenario,
        Scenario::IndexBuildAtStart | Scenario::IndexBuildMidway | Scenario::IndexBuildAtEnd
    );
    if is_index_scenario {
        for page_no in INDEX_START..INDEX_END {
            if wal.shm().lookup(page_no).is_some() {
                return Err(Error::Internal(format!(
                    "condition (e) FAIL: uncommitted index page {} in WAL SHM after recovery \
                     [scenario {:?} seed {}]",
                    page_no, scenario, seed
                )));
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Run one crash cycle
// ---------------------------------------------------------------------------

/// Run one full crash-recovery cycle.
///
/// Returns `Ok(())` if all five correctness conditions pass.
/// Returns `Err(...)` with a descriptive message on any failure.
fn run_cycle(scenario: Scenario, seed: u32) -> Result<()> {
    let dir = tempfile::tempdir().map_err(Error::Io)?;
    let db_path = dir.path().join("crash.mqlite");

    // Write pre-committed epoch-1 data to the WAL.
    setup_epoch1(&db_path, seed)?;

    // Determine how many pipe signals to read before killing the child.
    let kill_after: u32 = match scenario {
        Scenario::InsertAtFrame0 => 1,
        Scenario::InsertAtFrame10 => 10,
        Scenario::InsertAtFrame100 => 100,
        Scenario::InsertAtFinalFrame => 1,
        Scenario::CheckpointAt25Pct => (CHECKPOINT_PAGES / 4).max(1), // 5
        Scenario::CheckpointAt50Pct => CHECKPOINT_PAGES / 2,          // 10
        Scenario::CheckpointAt75Pct => (CHECKPOINT_PAGES * 3) / 4,    // 15
        Scenario::IndexBuildAtStart => 1,
        Scenario::IndexBuildMidway => 5,
        Scenario::IndexBuildAtEnd => INDEX_END - INDEX_START, // 10
    };

    // ---- Create synchronisation pipe ----------------------------------------
    let mut pipe_fds = [0i32; 2];
    assert_eq!(
        unsafe { libc::pipe(pipe_fds.as_mut_ptr()) },
        0,
        "pipe() failed"
    );
    let (read_fd, write_fd) = (pipe_fds[0], pipe_fds[1]);

    // ---- Fork ---------------------------------------------------------------
    let pid = unsafe { libc::fork() };
    assert!(pid >= 0, "fork() failed");

    if pid == 0 {
        // ===== CHILD =====
        unsafe { libc::close(read_fd) };
        // db_path is in the child's copy of the heap — valid after fork.
        unsafe { child_run_scenario(&db_path, scenario, seed, write_fd) };
        // Never reached (child_run_scenario calls _exit).
        unsafe { libc::_exit(1) };
    }

    // ===== PARENT =====
    unsafe { libc::close(write_fd) };

    // Wait for exactly `kill_after` synchronisation signals from the child.
    let mut buf = 0u8;
    for signal_idx in 0..kill_after {
        let n = unsafe { libc::read(read_fd, &mut buf as *mut u8 as *mut libc::c_void, 1) };
        if n != 1 {
            // Child exited before sending enough signals — cleanup and fail.
            unsafe { libc::kill(pid, libc::SIGKILL) };
            unsafe { libc::waitpid(pid, std::ptr::null_mut(), 0) };
            unsafe { libc::close(read_fd) };
            return Err(Error::Internal(format!(
                "child exited early: got {signal_idx}/{kill_after} signals \
                 [scenario {:?} seed {seed}]",
                scenario
            )));
        }
    }
    unsafe { libc::close(read_fd) };

    // ---- SIGKILL the child --------------------------------------------------
    unsafe { libc::kill(pid, libc::SIGKILL) };
    unsafe { libc::waitpid(pid, std::ptr::null_mut(), 0) };

    // ---- Recover and validate -----------------------------------------------

    // Condition (a) + (b): WalManager::open_or_create must succeed.
    let mut main_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&db_path)
        .map_err(|e| {
            Error::Internal(format!(
                "condition (a) FAIL: cannot reopen main file after crash \
                 [scenario {:?} seed {seed}]: {e}",
                scenario
            ))
        })?;

    let header = FileHeader::new(1_700_000_000_000, SALT1, SALT2);
    let wal = WalManager::open_or_create(&db_path, &header, &mut main_file).map_err(|e| {
        Error::Internal(format!(
            "condition (a)+(b) FAIL: WalManager::open_or_create failed after crash \
             [scenario {:?} seed {seed}]: {e}",
            scenario
        ))
    })?;

    // Conditions (c), (d), (e): inspect recovered state.
    validate(&wal, &mut main_file, scenario, seed)?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Main test: 500 cycles
// ---------------------------------------------------------------------------

/// **Crash recovery gate: 500 cycles must all pass (50 × 10 scenarios).**
///
/// Jepsen-style WAL crash-injection test.  Each cycle forks a child process,
/// SIGKILLs it at a scenario-specific injection point, and validates that
/// `WalManager` recovery satisfies all correctness conditions.
///
/// See module-level documentation for full scenario descriptions.
#[test]
fn crash_recovery_500_cycles() {
    let mut failures: Vec<String> = Vec::new();
    let mut total: u32 = 0;

    for scenario in &ALL_SCENARIOS {
        for cycle in 0..CYCLES_PER_SCENARIO {
            total += 1;
            let seed = cycle;
            if let Err(e) = run_cycle(*scenario, seed) {
                failures.push(format!(
                    "  [cycle {total}/500 | scenario {:?} | seed {seed}] {e}",
                    scenario
                ));
            }
        }
    }

    if !failures.is_empty() {
        // Print WAL state hint in failure output for debugging.
        panic!(
            "CRASH RECOVERY FAILURES — {}/{} cycles failed:\n{}\n\
             Hint: re-run with `RUST_BACKTRACE=1 cargo test crash_recovery` \
             to reproduce a specific failure.",
            failures.len(),
            total,
            failures.join("\n")
        );
    }
}
