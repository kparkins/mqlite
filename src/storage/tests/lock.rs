use super::*;

// ---- NoopFileLock -------------------------------------------------------

#[test]
fn noop_acquire_exclusive_succeeds() {
    let lock = NoopFileLock;
    assert!(!lock.acquire_exclusive(Duration::from_secs(1)).unwrap());
}

#[test]
fn noop_acquire_shared_succeeds() {
    let lock = NoopFileLock;
    assert!(!lock.acquire_shared(Duration::from_secs(1)).unwrap());
}

#[test]
fn noop_release_succeeds() {
    let lock = NoopFileLock;
    lock.release().unwrap();
}

// ---- PosixFileLock (Unix-only) ----------------------------------------

#[cfg(unix)]
mod posix {
    use super::*;
    use std::fs;

    fn temp_db_file() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("test.mqlite");
        // Create an empty file with at least 128 bytes (past our lock region).
        let data = vec![0u8; 256];
        fs::write(&path, &data).expect("write temp file");
        (dir, path)
    }

    #[test]
    fn acquire_exclusive_and_release() {
        let (_dir, path) = temp_db_file();
        let lock = open_file_lock(&path).unwrap();
        // Acquire with no contention — should return false (not contended).
        let contended = lock.acquire_exclusive(Duration::from_secs(1)).unwrap();
        assert!(!contended, "should not be contended on first acquire");
        lock.release().unwrap();
    }

    #[test]
    fn acquire_shared_and_release() {
        let (_dir, path) = temp_db_file();
        let lock = open_file_lock(&path).unwrap();
        let contended = lock.acquire_shared(Duration::from_secs(1)).unwrap();
        assert!(!contended);
        lock.release().unwrap();
    }

    #[test]
    fn release_without_acquire_is_idempotent() {
        let (_dir, path) = temp_db_file();
        let lock = open_file_lock(&path).unwrap();
        // Release without ever acquiring should not error.
        lock.release().unwrap();
    }

    /// Verify that a second exclusive lock attempt from the *same process*
    /// on the *same byte range* via a different fd succeeds immediately.
    ///
    /// POSIX fcntl advisory locks are per-(process, inode, byte-range) —
    /// a second F_SETLK from the same PID upgrades/replaces the first lock
    /// rather than blocking.  Cross-process mutual exclusion must be tested
    /// via a subprocess (see the multi-process integration tests below).
    #[test]
    fn same_process_second_exclusive_lock_replaces_first() {
        let (_dir, path) = temp_db_file();
        let lock1 = open_file_lock(&path).unwrap();
        lock1.acquire_exclusive(Duration::from_secs(1)).unwrap();

        // Second exclusive lock from same PID — should succeed (not block).
        let lock2 = open_file_lock(&path).unwrap();
        let result = lock2.acquire_exclusive(Duration::ZERO);
        assert!(
            result.is_ok(),
            "same-process second exclusive lock should succeed (POSIX semantics): {:?}",
            result
        );
        lock1.release().unwrap();
        lock2.release().unwrap();
    }

    /// Two shared locks from the same process on different fds both succeed.
    #[test]
    fn multiple_shared_locks_from_same_process() {
        let (_dir, path) = temp_db_file();
        let lock1 = open_file_lock(&path).unwrap();
        let lock2 = open_file_lock(&path).unwrap();
        lock1.acquire_shared(Duration::from_secs(1)).unwrap();
        lock2.acquire_shared(Duration::from_secs(1)).unwrap();
        lock1.release().unwrap();
        lock2.release().unwrap();
    }

    /// Zero-timeout acquire on a file locked exclusively by another process
    /// returns `Error::WriterBusy`.
    ///
    /// This test uses `fork()` to create a child process that holds the
    /// exclusive lock, then verifies the parent gets `WriterBusy`.
    #[test]
    #[cfg(unix)]
    fn cross_process_exclusive_lock_returns_writer_busy() {
        let (_dir, path) = temp_db_file();

        // Channel: child writes a byte when ready; parent reads it.
        let (read_fd, write_fd) = {
            let mut fds = [0i32; 2];
            // SAFETY: pipe() is safe; we own the fds.
            assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
            (fds[0], fds[1])
        };

        // SAFETY: fork() is safe here; we call only async-signal-safe
        // functions in the child and exec nothing.
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork() failed");

        if pid == 0 {
            // ---- Child process ----
            // Close the read end.
            unsafe { libc::close(read_fd) };

            // Acquire the exclusive lock.
            let lock = open_file_lock(&path).expect("child: open lock");
            lock.acquire_exclusive(Duration::from_secs(1))
                .expect("child: acquire");

            // Signal parent: write 1 byte.
            let ready: u8 = 1;
            unsafe { libc::write(write_fd, &ready as *const u8 as *const libc::c_void, 1) };

            // Hold the lock while sleeping.
            std::thread::sleep(Duration::from_secs(5));

            // SAFETY: _exit is async-signal-safe.
            unsafe { libc::_exit(0) };
        }

        // ---- Parent process ----
        unsafe { libc::close(write_fd) };

        // Wait for child's ready signal.
        let mut buf = 0u8;
        let n = unsafe { libc::read(read_fd, &mut buf as *mut u8 as *mut libc::c_void, 1) };
        assert_eq!(n, 1, "parent: did not receive child ready signal");
        unsafe { libc::close(read_fd) };

        // Try to acquire exclusive lock — must fail immediately (zero timeout).
        let parent_lock = open_file_lock(&path).expect("parent: open lock");
        let result = parent_lock.acquire_exclusive(Duration::ZERO);
        assert!(
            matches!(result, Err(Error::WriterBusy)),
            "expected WriterBusy, got: {:?}",
            result
        );

        // Kill child — its lock must be released by the kernel.
        unsafe { libc::kill(pid, libc::SIGKILL) };
        unsafe { libc::waitpid(pid, std::ptr::null_mut(), 0) };

        // After child is killed, parent must be able to acquire.
        let acquired = parent_lock
            .acquire_exclusive(Duration::from_secs(2))
            .expect("parent: should acquire after child exits");
        // The lock may show as contended since we were waiting.
        let _ = acquired;
        parent_lock.release().unwrap();
    }

    /// Writer dies (SIGKILL): lock released by OS; next opener acquires cleanly.
    #[test]
    fn lock_released_on_sigkill() {
        let (_dir, path) = temp_db_file();

        let (read_fd, write_fd) = {
            let mut fds = [0i32; 2];
            assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
            (fds[0], fds[1])
        };

        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork() failed");

        if pid == 0 {
            // Child: acquire and signal, then sleep (will be SIGKILLed).
            unsafe { libc::close(read_fd) };
            let lock = open_file_lock(&path).expect("child: open");
            lock.acquire_exclusive(Duration::from_secs(1))
                .expect("child: acquire");
            let ready: u8 = 1;
            unsafe { libc::write(write_fd, &ready as *const u8 as *const libc::c_void, 1) };
            std::thread::sleep(Duration::from_secs(60));
            unsafe { libc::_exit(0) };
        }

        // Parent waits for child ready signal.
        unsafe { libc::close(write_fd) };
        let mut buf = 0u8;
        let n = unsafe { libc::read(read_fd, &mut buf as *mut u8 as *mut libc::c_void, 1) };
        assert_eq!(n, 1);
        unsafe { libc::close(read_fd) };

        // SIGKILL the child.
        unsafe { libc::kill(pid, libc::SIGKILL) };
        unsafe { libc::waitpid(pid, std::ptr::null_mut(), 0) };

        // Parent must now be able to acquire cleanly.
        let lock = open_file_lock(&path).expect("open after kill");
        lock.acquire_exclusive(Duration::from_secs(2))
            .expect("acquire after child SIGKILL");
        lock.release().unwrap();
    }

    /// Verify the lock region constants are within the reserved header area.
    #[test]
    #[allow(
        clippy::assertions_on_constants,
        reason = "this test documents and checks the reserved header lock layout"
    )]
    fn lock_region_is_within_reserved_header_area() {
        // Header reserved area: offsets 76–127 (see storage/header.rs).
        assert!(
            READER_LOCK_OFFSET >= 76,
            "reader lock must be in reserved area"
        );
        assert!(
            READER_LOCK_OFFSET + READER_LOCK_LEN <= 128,
            "reader lock must end before padding"
        );
        assert!(
            WRITER_LOCK_OFFSET >= 76,
            "writer lock must be in reserved area"
        );
        assert!(
            WRITER_LOCK_OFFSET + WRITER_LOCK_LEN <= 128,
            "writer lock must end before padding"
        );
        // Reader and writer regions must not overlap.
        let r_end = READER_LOCK_OFFSET + READER_LOCK_LEN;
        let w_end = WRITER_LOCK_OFFSET + WRITER_LOCK_LEN;
        let no_overlap = r_end <= WRITER_LOCK_OFFSET || w_end <= READER_LOCK_OFFSET;
        assert!(
            no_overlap,
            "reader and writer lock regions must not overlap"
        );
    }
}

// ---- WindowsFileLock (Windows-only) --------------------------------------

#[cfg(windows)]
mod windows {
    use super::*;
    use std::fs;

    /// Create a temporary file with 256 bytes of zeros and return the dir
    /// guard (keeps the dir alive) and the file path.
    fn temp_db_file() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("test.mqlite");
        let data = vec![0u8; 256];
        fs::write(&path, &data).expect("write temp file");
        (dir, path)
    }

    /// Acquiring an exclusive lock on an uncontended file returns `Ok(false)`
    /// (not contended) and release succeeds.
    #[test]
    fn acquire_exclusive_and_release() {
        let (_dir, path) = temp_db_file();
        let lock = open_file_lock(&path).unwrap();
        let contended = lock.acquire_exclusive(Duration::from_secs(1)).unwrap();
        assert!(!contended, "should not be contended on first acquire");
        lock.release().unwrap();
    }

    /// Acquiring a shared lock on an uncontended file returns `Ok(false)` and
    /// release succeeds.
    #[test]
    fn acquire_shared_and_release() {
        let (_dir, path) = temp_db_file();
        let lock = open_file_lock(&path).unwrap();
        let contended = lock.acquire_shared(Duration::from_secs(1)).unwrap();
        assert!(!contended, "should not be contended on first acquire");
        lock.release().unwrap();
    }

    /// write_at / read_exact_at round-trip: bytes written at an offset must be
    /// read back exactly.
    #[test]
    fn write_at_read_exact_at_roundtrip() {
        let (_dir, path) = temp_db_file();
        let lock = open_file_lock(&path).unwrap();
        lock.acquire_exclusive(Duration::from_secs(1)).unwrap();

        let payload: [u8; 8] = [0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE];
        lock.write_at(64, &payload).unwrap();

        let mut buf = [0u8; 8];
        lock.read_exact_at(64, &mut buf).unwrap();
        assert_eq!(buf, payload, "read-back must match what was written");

        lock.release().unwrap();
    }

    /// read_exact_at past the end of the file returns an Io error with kind
    /// `UnexpectedEof`.  This is required by `FilePageSource::read_page` so
    /// that it can zero-fill freshly allocated pages.
    #[test]
    fn read_exact_at_past_eof_returns_unexpected_eof() {
        let (_dir, path) = temp_db_file();
        let lock = open_file_lock(&path).unwrap();

        // The file is 256 bytes; reading 8 bytes at offset 300 must fail with
        // UnexpectedEof.
        let mut buf = [0u8; 8];
        let err = lock
            .read_exact_at(300, &mut buf)
            .expect_err("read past EOF must fail");
        match err {
            Error::Io(ref e) => assert_eq!(
                e.kind(),
                std::io::ErrorKind::UnexpectedEof,
                "expected UnexpectedEof, got {:?}",
                e.kind()
            ),
            other => panic!("expected Error::Io(UnexpectedEof), got {:?}", other),
        }
    }

    /// Re-acquiring the same exclusive lock mode on the same handle must be
    /// idempotent and return `Ok(false)` (not contended, and not an error).
    #[test]
    fn re_acquire_exclusive_is_idempotent() {
        let (_dir, path) = temp_db_file();
        let lock = open_file_lock(&path).unwrap();
        lock.acquire_exclusive(Duration::from_secs(1)).unwrap();

        // Second acquire on the same handle — should return Ok(false) without
        // blocking, not Err.
        let result = lock.acquire_exclusive(Duration::ZERO);
        assert!(
            matches!(result, Ok(false)),
            "re-acquire same exclusive mode must be Ok(false), got {:?}",
            result
        );
        lock.release().unwrap();
    }

    /// Re-acquiring the same shared lock mode on the same handle is idempotent.
    #[test]
    fn re_acquire_shared_is_idempotent() {
        let (_dir, path) = temp_db_file();
        let lock = open_file_lock(&path).unwrap();
        lock.acquire_shared(Duration::from_secs(1)).unwrap();

        let result = lock.acquire_shared(Duration::ZERO);
        assert!(
            matches!(result, Ok(false)),
            "re-acquire same shared mode must be Ok(false), got {:?}",
            result
        );
        lock.release().unwrap();
    }

    /// Two handles on the same file from the **same process**: a second
    /// exclusive acquire must succeed immediately (POSIX parity — same-process
    /// re-acquire never conflicts, regardless of handle identity).
    ///
    /// Mirrors `posix::same_process_second_exclusive_lock_replaces_first`.
    #[test]
    fn same_process_exclusive_then_exclusive_succeeds() {
        let (_dir, path) = temp_db_file();
        let lock1 = open_file_lock(&path).unwrap();
        lock1
            .acquire_exclusive(Duration::from_secs(1))
            .expect("lock1 acquire");

        // Second handle, same process — must succeed immediately (Ok), not
        // block or return WriterBusy.
        let lock2 = open_file_lock(&path).unwrap();
        let result = lock2.acquire_exclusive(Duration::ZERO);
        assert!(
            result.is_ok(),
            "same-process second exclusive acquire must succeed (POSIX parity), got {:?}",
            result
        );

        lock1.release().unwrap();
        lock2.release().unwrap();
    }

    /// Two handles on the same file may both hold shared locks simultaneously.
    ///
    /// Mirrors `posix::multiple_shared_locks_from_same_process`.
    #[test]
    fn same_process_shared_and_shared_both_succeed() {
        let (_dir, path) = temp_db_file();
        let lock1 = open_file_lock(&path).unwrap();
        let lock2 = open_file_lock(&path).unwrap();
        lock1
            .acquire_shared(Duration::from_secs(1))
            .expect("lock1 shared");
        lock2
            .acquire_shared(Duration::from_secs(1))
            .expect("lock2 shared — concurrent shared locks must be allowed");
        lock1.release().unwrap();
        lock2.release().unwrap();
    }

    /// OS lock survives the first handle's drop; a third handle can still
    /// acquire; the OS lock is released only when ALL handles release.
    ///
    /// Scenario:
    ///   A acquires exclusive → lock is taken at OS level (count=1).
    ///   B acquires exclusive → count=2, no additional OS call.
    ///   drop(A)              → count=1, OS lock still held.
    ///   C acquires exclusive → count=2, succeeds (POSIX parity).
    ///   B.release()          → count=1, OS lock still held.
    ///   C.release()          → count=0, OS lock released.
    ///   D acquires exclusive → count=1, fresh OS lock — succeeds.
    #[test]
    fn same_process_locks_survive_first_handle_drop() {
        let (_dir, path) = temp_db_file();

        let lock_a = open_file_lock(&path).unwrap();
        lock_a.acquire_exclusive(Duration::from_secs(1)).unwrap();

        let lock_b = open_file_lock(&path).unwrap();
        lock_b
            .acquire_exclusive(Duration::ZERO)
            .expect("B: same-process acquire must succeed");

        // Drop A — B still holds the mode; the OS lock must remain.
        drop(lock_a);

        // C can still acquire because the process still holds the lock.
        let lock_c = open_file_lock(&path).unwrap();
        lock_c
            .acquire_exclusive(Duration::ZERO)
            .expect("C: same-process acquire after A dropped must succeed");

        // Release B and C — this brings the count to 0; OS lock released.
        lock_b.release().unwrap();
        lock_c.release().unwrap();

        // D must be able to take a fresh OS lock.
        let lock_d = open_file_lock(&path).unwrap();
        lock_d
            .acquire_exclusive(Duration::from_secs(1))
            .expect("D: fresh exclusive acquire after all released must succeed");
        lock_d.release().unwrap();
    }

    /// After dropping all handles the registry entry is gone and a new opener
    /// acquires cleanly — no lock-violation from a stale OS lock.
    #[test]
    fn exclusive_lock_released_on_drop() {
        let (_dir, path) = temp_db_file();
        let lock1 = open_file_lock(&path).unwrap();
        lock1
            .acquire_exclusive(Duration::from_secs(1))
            .expect("lock1 acquire");

        // Drop lock1 — the Drop impl must explicitly call UnlockFileEx through
        // the anchor before the handle is closed.
        drop(lock1);

        let lock2 = open_file_lock(&path).unwrap();
        lock2
            .acquire_exclusive(Duration::from_secs(2))
            .expect("lock2 should acquire after lock1 dropped");
        lock2.release().unwrap();
    }

    // ---- Regression tests for registry-entry-lifetime + acquire-TOCTOU ----

    /// BUG 1 (single handle): one handle, acquire_exclusive → release →
    /// acquire_exclusive must succeed.
    ///
    /// On the buggy code, `release()` removes the registry entry when both
    /// counts hit 0, so the second `acquire_mode` finds no entry and returns
    /// `Err(Internal("registry entry missing in acquire_mode"))`.  POSIX
    /// fcntl allows release-then-reacquire on the same fd, so this must
    /// succeed.
    #[test]
    fn same_handle_reacquire_after_release() {
        let (_dir, path) = temp_db_file();
        let lock = open_file_lock(&path).unwrap();

        lock.acquire_exclusive(Duration::from_secs(1))
            .expect("first acquire");
        lock.release().expect("release");

        // Buggy code returns Err(Internal "registry entry missing").
        lock.acquire_exclusive(Duration::from_secs(1))
            .expect("re-acquire after release must succeed");
        lock.release().expect("final release");
    }

    /// BUG 1 (two handles): handles a + b open; a.acquire_exclusive,
    /// a.release (which on buggy code removes the entry), then
    /// b.acquire_exclusive must succeed even though b is still live.
    ///
    /// On the buggy code the entry is gone after a.release(), so
    /// b.acquire_mode returns `Err(Internal("registry entry missing"))`.
    #[test]
    fn second_handle_acquires_after_first_releases() {
        let (_dir, path) = temp_db_file();
        let a = open_file_lock(&path).unwrap();
        let b = open_file_lock(&path).unwrap();

        a.acquire_exclusive(Duration::from_secs(1))
            .expect("a acquire");
        a.release().expect("a release");

        // b is still alive; on POSIX a different fd in the same process can
        // acquire after the first releases.  Buggy code: Internal error.
        b.acquire_exclusive(Duration::from_secs(1))
            .expect("b acquire after a released must succeed");
        b.release().expect("b release");
    }

    /// BUG 2 (exclusive/exclusive TOCTOU): two same-process handles racing
    /// `acquire_exclusive` at count==0 must BOTH succeed (POSIX parity: a
    /// same-process re-acquire is always uncontended).
    ///
    /// On the buggy code, both threads pass the fast-path check (count==0),
    /// drop the registry mutex, and both call `LockFileEx` exclusive on the
    /// SAME anchor handle.  Win32 rejects exclusive-over-exclusive even on the
    /// same handle (`ERROR_LOCK_VIOLATION`); the loser spins blind in
    /// `win_acquire_with_timeout` (it never re-checks the registry) until the
    /// 5s timeout → spurious `Err(WriterBusy)`.
    ///
    /// Two barriers per iteration: the first synchronises the race start; the
    /// second forces both threads to hold their result before EITHER releases
    /// — otherwise the loser could acquire after the winner's release and mask
    /// the bug.  On the buggy code this fails within the first few iterations;
    /// the 5s timeout only burns on the failing iteration.
    #[test]
    fn same_process_concurrent_exclusive_acquires_both_succeed() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        for iter in 0..300 {
            let (_dir, path) = temp_db_file();

            // Both handles must stay alive for the whole iteration.
            let lock_x = Arc::new(open_file_lock(&path).unwrap());
            let lock_y = Arc::new(open_file_lock(&path).unwrap());

            let start = Arc::new(Barrier::new(2));
            let hold = Arc::new(Barrier::new(2));

            let run = |lock: Arc<AnyFileLock>, start: Arc<Barrier>, hold: Arc<Barrier>| {
                thread::spawn(move || {
                    start.wait();
                    let res = lock.acquire_exclusive(Duration::from_secs(5));
                    // Both threads must hold their result before either releases.
                    hold.wait();
                    if res.is_ok() {
                        let _ = lock.release();
                    }
                    res
                })
            };

            let tx = run(Arc::clone(&lock_x), Arc::clone(&start), Arc::clone(&hold));
            let ty = run(Arc::clone(&lock_y), Arc::clone(&start), Arc::clone(&hold));

            let rx = tx.join().expect("thread x panicked");
            let ry = ty.join().expect("thread y panicked");

            assert!(
                rx.is_ok() && ry.is_ok(),
                "iter {iter}: both same-process exclusive acquires must succeed, \
                 got x={rx:?} y={ry:?}"
            );
        }
    }

    /// BUG 2 corollary (shared/shared OS-lock leak): two same-process handles
    /// racing `acquire_shared` at count==0 must take the OS shared lock AT MOST
    /// ONCE, so that after both release the OS shared lock is fully gone.
    ///
    /// On the buggy code both threads pass the fast-path (count==0), drop the
    /// mutex, and both `LockFileEx` shared succeed on the same anchor (shared
    /// overlap is allowed) → TWO OS shared locks but shared_holders==2.  At
    /// count→0 `release_held` issues only ONE `UnlockFileEx`, leaking one OS
    /// shared lock.
    ///
    /// We make the leak deterministic by keeping a THIRD handle alive across
    /// the whole test, so the registry entry (and its anchor File) is never
    /// removed — Windows therefore does not auto-free the leaked lock.  After
    /// both racing shared handles release, a FOREIGN handle (a separate
    /// `std::fs::File` not in the registry) probing an EXCLUSIVE lock on the
    /// reader range must succeed; a leaked OS shared lock would make it fail.
    #[test]
    fn same_process_concurrent_shared_acquires_no_os_leak() {
        use std::os::windows::io::AsRawHandle;
        use std::sync::{Arc, Barrier};
        use std::thread;

        let (_dir, path) = temp_db_file();

        // Keep the registry entry (and anchor) alive for the whole test so the
        // OS lock leak cannot be masked by entry removal / anchor close.
        let keepalive = open_file_lock(&path).unwrap();

        let lock_x = Arc::new(open_file_lock(&path).unwrap());
        let lock_y = Arc::new(open_file_lock(&path).unwrap());

        let start = Arc::new(Barrier::new(2));
        let hold = Arc::new(Barrier::new(2));

        let run = |lock: Arc<AnyFileLock>, start: Arc<Barrier>, hold: Arc<Barrier>| {
            thread::spawn(move || {
                start.wait();
                lock.acquire_shared(Duration::from_secs(5))
                    .expect("concurrent shared acquire must succeed");
                hold.wait();
            })
        };

        let tx = run(Arc::clone(&lock_x), Arc::clone(&start), Arc::clone(&hold));
        let ty = run(Arc::clone(&lock_y), Arc::clone(&start), Arc::clone(&hold));
        tx.join().expect("thread x panicked");
        ty.join().expect("thread y panicked");

        // Both racing shared handles release.  After this the OS shared lock
        // must be fully gone (taken at most once → unlocked exactly once).
        lock_x.release().expect("x release");
        lock_y.release().expect("y release");

        // Foreign-handle probe: open a SEPARATE std::fs::File (not registered)
        // and attempt an EXCLUSIVE lock on the reader range directly via
        // LockFileEx.  If a shared OS lock leaked, this fails with
        // ERROR_LOCK_VIOLATION (win_try_lock → Ok(false)).
        let probe = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open probe handle");
        let probe_raw = probe.as_raw_handle() as windows_sys::Win32::Foundation::HANDLE;
        let got = win_try_lock(probe_raw, WIN_READER_LOCK_OFFSET, WIN_LOCK_LEN, true)
            .expect("probe lock call");
        assert!(
            got,
            "foreign exclusive probe on reader range must succeed after both \
             shared releases; failure indicates a leaked OS shared lock"
        );
        // Clean up the probe lock.
        win_unlock(probe_raw, WIN_READER_LOCK_OFFSET, WIN_LOCK_LEN).expect("probe unlock");

        drop(keepalive);
    }
}

// ---- BUG-W: same-handle concurrent acquire desyncs the registry count ----

#[cfg(windows)]
mod windows_same_handle_race {
    use super::*;
    use std::fs;

    /// Same helper as `windows::temp_db_file` (private to that sibling mod).
    fn temp_db_file() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("test.mqlite");
        let data = vec![0u8; 256];
        fs::write(&path, &data).expect("write temp file");
        (dir, path)
    }

    /// BUG-W regression: two threads racing `acquire_exclusive` on the SAME
    /// `WindowsFileLock` handle must increment `exclusive_holders` exactly
    /// once, so an explicit `release()` frees the OS writer lock.
    ///
    /// `acquire_exclusive` checks `held.exclusive` and then DROPS the held
    /// mutex before calling `acquire_mode` (check-then-act).  Under a barrier
    /// both threads pass the check while `held.exclusive` is still false.
    /// On the buggy code the per-key acquisition gate serialised them but did
    /// not deduplicate same-HANDLE acquires: the winner took the OS lock and
    /// bumped `exclusive_holders` to 1 (step 5); the loser then won the gate,
    /// hit the step-3 fast path (count > 0) and bumped it AGAIN to 2.
    /// `mark_held` only sets an idempotent bool on the shared handle, so
    /// `release()` decremented exactly once: `exclusive_holders` stuck at 1,
    /// the `== 0` `UnlockFileEx` branch was unreachable, and the OS writer
    /// lock stayed held until the last handle was dropped.  A foreign process
    /// (modelled by a separate, unregistered `File` probing `LockFileEx`
    /// directly) got `ERROR_LOCK_VIOLATION` even after the explicit release.
    /// The fix re-checks the handle's held flag under the gate (step 2b), so
    /// the gate loser returns without touching the count.
    #[test]
    fn same_handle_concurrent_exclusive_release_frees_os_lock() {
        use std::os::windows::io::AsRawHandle;
        use std::sync::{Arc, Barrier};
        use std::thread;

        // The held-check race window spans the whole gate + LockFileEx call,
        // so a start barrier makes it fire essentially every iteration; loop
        // anyway since it is still a race.
        for iter in 0..50 {
            let (_dir, path) = temp_db_file();
            let lock = Arc::new(open_file_lock(&path).unwrap());

            let start = Arc::new(Barrier::new(2));
            let spawn_acquire = |lock: Arc<AnyFileLock>, start: Arc<Barrier>| {
                thread::spawn(move || {
                    start.wait();
                    lock.acquire_exclusive(Duration::from_secs(5))
                })
            };
            let t1 = spawn_acquire(Arc::clone(&lock), Arc::clone(&start));
            let t2 = spawn_acquire(Arc::clone(&lock), Arc::clone(&start));
            t1.join()
                .expect("thread 1 panicked")
                .expect("same-handle concurrent exclusive acquire must succeed");
            t2.join()
                .expect("thread 2 panicked")
                .expect("same-handle concurrent exclusive acquire must succeed");

            // This handle is the only in-process holder; release() must free
            // the OS writer lock.
            lock.release().expect("release");

            // Foreign-process probe: a separate, unregistered File attempts
            // an exclusive LockFileEx on the writer range.  After release()
            // this must succeed.  On the buggy code the double-incremented
            // count keeps the OS lock held -> ERROR_LOCK_VIOLATION -> false.
            let probe = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .expect("open probe handle");
            let probe_raw = probe.as_raw_handle() as windows_sys::Win32::Foundation::HANDLE;
            let got = win_try_lock(probe_raw, WIN_WRITER_LOCK_OFFSET, WIN_LOCK_LEN, true)
                .expect("probe lock call");
            assert!(
                got,
                "iter {iter}: foreign exclusive probe on the writer range must succeed \
                 after release(); failure means the same-handle acquire race \
                 double-incremented exclusive_holders and the OS writer lock leaked"
            );
            win_unlock(probe_raw, WIN_WRITER_LOCK_OFFSET, WIN_LOCK_LEN).expect("probe unlock");
        }
    }
}
