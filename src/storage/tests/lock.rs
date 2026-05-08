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
