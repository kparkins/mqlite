//! Multi-process file locking via OS advisory locks.
//!
//! Provides a [`FileLock`] trait and platform-specific implementations that
//! prevent multiple processes from corrupting the database by writing to the
//! same `.mqlite` file concurrently.
//!
//! This is complementary to the in-process `writer_lock: Mutex<()>` in
//! `DatabaseInner`.  The two work together:
//! - **In-process**: `Mutex<()>` serializes concurrent writers within a
//!   single process (threads).
//! - **Cross-process**: `FileLock` serializes writers across different OS
//!   processes using kernel-enforced advisory locks.
//!
//! ## Locking protocol
//!
//! Advisory locks occupy reserved bytes in the database file header.  The
//! storage engine never reads or writes these bytes, so the kernel-managed
//! lock metadata is safe to place there.
//!
//! | Lock type | Byte range | Lock mode  | Meaning                      |
//! |-----------|------------|------------|------------------------------|
//! | Writer    | 120–127    | Exclusive  | Only one writer per file     |
//! | Reader    | 112–119    | Shared     | Concurrent readers allowed   |
//!
//! Both regions fall within the header's reserved area (offsets 76–127) and
//! are never interpreted by the storage engine.
//!
//! ## WAL integration
//!
//! In the full journal implementation, the writer lock must be acquired
//! **before** appending any frames to the journal and released **after** the
//! commit frame and in-memory index update complete.  In the current (stub)
//! phase, the lock is held for the entire lifetime of the database handle.
//!
//! ## Platform implementations
//!
//! | Platform  | Implementation      | Lock primitive       |
//! |-----------|---------------------|----------------------|
//! | Linux     | [`PosixFileLock`]   | `fcntl(F_SETLK)`     |
//! | macOS     | [`PosixFileLock`]   | `fcntl(F_SETLK)`     |
//! | Windows   | [`WindowsFileLock`] | stub — not yet implemented |
//! | in-memory | [`NoopFileLock`]    | no-op                |
//!
//! ## macOS-specific behaviour
//!
//! **Fork inheritance**: On macOS, `fcntl` advisory locks are inherited across
//! `fork()`, unlike Linux where child processes do not inherit the parent's
//! advisory locks.  Applications that fork after opening a database must
//! re-open the database on a fresh file descriptor in the child process.
//! The inherited lock in the child should be considered invalid.
//!
//! **~10 K system-wide limit**: macOS enforces an advisory lock limit of
//! approximately 10,000 entries system-wide.  At one lock per database handle
//! this is unlikely to be reached.  If it is, `fcntl` returns `ENOLCK` and
//! mqlite propagates the error as [`Error::Io`].
//!
//! **Thread-exit lock release**: On macOS, when a thread that holds an `fcntl`
//! lock exits without releasing it, the lock may not be released until the
//! process exits.  Always release locks explicitly via [`FileLock::release`]
//! before dropping the lock object.
//!
//! ## References
//!
//! - SQLite `os_unix.c`: `unixLock()`, `unixUnlock()` — reference for
//!   macOS-safe locking patterns
//! - POSIX.1-2017 §fcntl: File Locking
//! - [Windows `LockFileEx`](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-lockfileex)

use std::time::Duration;

use crate::error::{Error, Result};

// ---------------------------------------------------------------------------
// Lock region constants
// ---------------------------------------------------------------------------

/// Starting byte of the exclusive writer lock region (within the header
/// reserved area at offsets 76–127).
pub(crate) const WRITER_LOCK_OFFSET: u64 = 120;

/// Length of the writer lock region in bytes.
pub(crate) const WRITER_LOCK_LEN: u64 = 8;

/// Starting byte of the shared reader lock region (within the header reserved
/// area at offsets 76–127).
pub(crate) const READER_LOCK_OFFSET: u64 = 112;

/// Length of the reader lock region in bytes.
pub(crate) const READER_LOCK_LEN: u64 = 8;

/// Sleep interval between retry attempts when the lock is contended.
const LOCK_RETRY_SLEEP: Duration = Duration::from_millis(1);

// ---------------------------------------------------------------------------
// FileLock trait
// ---------------------------------------------------------------------------

/// Platform-agnostic file lock abstraction.
///
/// One `FileLock` is held per open [`Database`](crate::Database)
/// handle.  It serializes writers across OS processes that open the same
/// `.mqlite` file.
///
/// Locks are **advisory**: the kernel will not prevent a non-cooperative
/// process from writing directly to the file.  All mqlite processes must
/// cooperate by acquiring the appropriate lock before writing.
///
/// Dropping the lock releases all held locks automatically (via the underlying
/// file descriptor close).  For explicit release before drop, call
/// [`release`](FileLock::release).
pub(crate) trait FileLock: Send + Sync {
    /// Attempt to acquire an exclusive write lock.
    ///
    /// Spins (with 1 ms sleep between retries) until the lock is acquired or
    /// `timeout` expires.  A `timeout` of [`Duration::ZERO`] makes a single
    /// non-blocking attempt.
    ///
    /// # Returns
    /// - `Ok(true)` — lock acquired; it **was** contended (another holder
    ///   existed and was waited out).
    /// - `Ok(false)` — lock acquired immediately with no contention.
    /// - `Err(Error::WriterBusy)` — lock not acquired within `timeout`.
    /// - `Err(Error::Io(_))` — unexpected OS error.
    fn acquire_exclusive(&self, timeout: Duration) -> Result<bool>;

    /// Attempt to acquire a shared read lock.
    ///
    /// Multiple processes may hold shared locks simultaneously. An exclusive
    /// write lock blocks until all shared locks are released, and vice versa.
    ///
    /// # Returns
    /// - `Ok(true)` — lock acquired; it was contended (waited out an exclusive
    ///   holder).
    /// - `Ok(false)` — lock acquired immediately.
    /// - `Err(Error::WriterBusy)` — lock not acquired within `timeout`.
    /// - `Err(Error::Io(_))` — unexpected OS error.
    fn acquire_shared(&self, timeout: Duration) -> Result<bool>;

    /// Explicitly release all held locks.
    ///
    /// Locks are also released automatically when the `FileLock` is dropped
    /// (the underlying file descriptor is closed).  Call this explicitly when
    /// you need the release to happen before the `FileLock` is dropped.
    #[allow(dead_code)]
    fn release(&self) -> Result<()>;

    /// Write `data` to the backing file at absolute byte offset `offset`.
    ///
    /// **Must use the lock file descriptor**, never open a new fd.
    ///
    /// ## POSIX footgun
    ///
    /// POSIX requires that closing *any* file descriptor for a file releases
    /// *all* advisory locks the process holds on that file (see POSIX.1-2017
    /// §fcntl, File Locking). Therefore any I/O that occurs while the advisory
    /// lock is held **must** go through this method — not through a freshly
    /// opened (and therefore soon-closed) file descriptor.
    fn write_at(&self, offset: u64, data: &[u8]) -> Result<()>;

    /// Read exactly `buf.len()` bytes from the backing file at absolute byte
    /// offset `offset` into `buf`.
    ///
    /// Same POSIX footgun caveat as [`write_at`](FileLock::write_at) — use
    /// this instead of opening a new fd while the lock is held.
    fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> Result<()>;
}

// ---------------------------------------------------------------------------
// NoopFileLock — platforms without native file locking
// ---------------------------------------------------------------------------

/// A no-op [`FileLock`] for platforms that lack a native locking primitive.
///
/// Used as the fallback on targets that are neither Unix nor Windows.
/// All lock operations succeed immediately; I/O operations return an error
/// because there is no backing file to read or write.
pub(crate) struct NoopFileLock;

impl FileLock for NoopFileLock {
    fn acquire_exclusive(&self, _timeout: Duration) -> Result<bool> {
        Ok(false)
    }

    fn acquire_shared(&self, _timeout: Duration) -> Result<bool> {
        Ok(false)
    }

    fn release(&self) -> Result<()> {
        Ok(())
    }

    fn write_at(&self, _offset: u64, _data: &[u8]) -> Result<()> {
        // No backing file on this platform.
        Err(Error::Internal(
            "NoopFileLock::write_at: no backing file on this platform".into(),
        ))
    }

    fn read_exact_at(&self, _offset: u64, _buf: &mut [u8]) -> Result<()> {
        // No backing file on this platform.
        Err(Error::Internal(
            "NoopFileLock::read_exact_at: no backing file on this platform".into(),
        ))
    }
}

// ---------------------------------------------------------------------------
// POSIX implementation (Linux + macOS)
// ---------------------------------------------------------------------------

/// POSIX `fcntl`-based file lock for Linux and macOS.
///
/// Uses `fcntl(F_SETLK)` (non-blocking) in a retry loop to implement busy
/// waiting with a configurable timeout.  This avoids `F_SETLKW` (blocking
/// indefinitely) which cannot be interrupted by a timeout without a separate
/// thread.
///
/// ## macOS notes
///
/// - Advisory locks are inherited across `fork()` on macOS (unlike Linux).
///   Child processes should re-open the database after fork.
/// - System-wide advisory lock limit is ~10 K entries.  `fcntl` returns
///   `ENOLCK` if exceeded; this propagates as [`Error::Io`].
/// - Thread-exit lock release is unreliable on macOS; always call
///   [`release`](FileLock::release) explicitly.
#[cfg(unix)]
pub(crate) struct PosixFileLock {
    /// Dedicated file handle used only for advisory locking.
    ///
    /// Keeping this separate from any I/O file handle ensures the lock
    /// lifetime matches the `PosixFileLock` lifetime (fd closed on drop →
    /// lock released by kernel).
    file: std::fs::File,
}

#[cfg(unix)]
impl PosixFileLock {
    /// Create a `PosixFileLock` from an already-opened file.
    ///
    /// Does **not** acquire any lock — call [`acquire_exclusive`] or
    /// [`acquire_shared`] explicitly.
    pub(crate) fn from_file(file: std::fs::File) -> Self {
        PosixFileLock { file }
    }

    /// Perform a single non-blocking `fcntl(F_SETLK)` call.
    ///
    /// Returns `Ok(true)` when the lock is acquired, `Ok(false)` when
    /// contended (`EAGAIN` / `EACCES`), and `Err` for other OS errors.
    fn try_fcntl(
        &self,
        l_type: libc::c_short,
        offset: libc::off_t,
        len: libc::off_t,
    ) -> std::io::Result<bool> {
        use std::os::unix::io::AsRawFd;

        // Build the flock struct using named-field syntax.
        // The field order in the struct definition differs between Linux and
        // macOS, but Rust named-field initialization is order-independent.
        let fl = libc::flock {
            l_type,
            l_whence: libc::SEEK_SET as libc::c_short,
            l_start: offset,
            l_len: len,
            l_pid: 0,
        };

        let fd = self.file.as_raw_fd();
        // SAFETY: fd is valid for the lifetime of self.file; fl is a local
        // struct on the stack.  F_SETLK is the non-blocking variant.
        let ret = unsafe { libc::fcntl(fd, libc::F_SETLK, &fl as *const libc::flock) };

        if ret == 0 {
            Ok(true) // lock acquired
        } else {
            let err = std::io::Error::last_os_error();
            match err.raw_os_error() {
                // EAGAIN / EACCES: another process holds an incompatible lock.
                Some(libc::EAGAIN) | Some(libc::EACCES) => Ok(false),
                _ => Err(err),
            }
        }
    }

    /// Spin-retry `try_fcntl` until the lock is acquired or `timeout` elapses.
    ///
    /// Returns `Ok(true)` if contention was encountered (i.e. at least one
    /// `EAGAIN`/`EACCES` before acquiring), `Ok(false)` if acquired
    /// immediately, or `Err(WriterBusy)` / `Err(Io(_))` on failure.
    fn acquire_with_timeout(
        &self,
        l_type: libc::c_short,
        offset: u64,
        len: u64,
        timeout: Duration,
    ) -> Result<bool> {
        use std::time::Instant;

        let deadline = Instant::now() + timeout;
        let mut contended = false;

        loop {
            match self.try_fcntl(l_type, offset as libc::off_t, len as libc::off_t) {
                Ok(true) => return Ok(contended),
                Ok(false) => {
                    contended = true;
                    // For zero timeout, fail immediately on first contention.
                    if timeout.is_zero() || Instant::now() >= deadline {
                        return Err(Error::WriterBusy);
                    }
                    std::thread::sleep(LOCK_RETRY_SLEEP);
                }
                Err(e) => return Err(Error::Io(e)),
            }
        }
    }
}

#[cfg(unix)]
impl FileLock for PosixFileLock {
    fn write_at(&self, offset: u64, data: &[u8]) -> Result<()> {
        use std::os::unix::fs::FileExt;
        self.file.write_all_at(data, offset).map_err(Error::Io)
    }

    fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        use std::os::unix::fs::FileExt;
        self.file.read_exact_at(buf, offset).map_err(Error::Io)
    }

    fn acquire_exclusive(&self, timeout: Duration) -> Result<bool> {
        self.acquire_with_timeout(
            libc::F_WRLCK as libc::c_short,
            WRITER_LOCK_OFFSET,
            WRITER_LOCK_LEN,
            timeout,
        )
    }

    fn acquire_shared(&self, timeout: Duration) -> Result<bool> {
        self.acquire_with_timeout(
            libc::F_RDLCK as libc::c_short,
            READER_LOCK_OFFSET,
            READER_LOCK_LEN,
            timeout,
        )
    }

    fn release(&self) -> Result<()> {
        // Unlock the entire lock region (reader + writer) in one call.
        let start = READER_LOCK_OFFSET.min(WRITER_LOCK_OFFSET);
        let end = (READER_LOCK_OFFSET + READER_LOCK_LEN).max(WRITER_LOCK_OFFSET + WRITER_LOCK_LEN);

        self.try_fcntl(
            libc::F_UNLCK as libc::c_short,
            start as libc::off_t,
            (end - start) as libc::off_t,
        )
        .map(|_| ())
        .map_err(Error::Io)
    }
}

// ---------------------------------------------------------------------------
// Windows stub
// ---------------------------------------------------------------------------

/// Windows file lock placeholder.
///
/// The production implementation must use `LockFileEx()` / `UnlockFileEx()`
/// for byte-range locking via the `windows-sys` crate. Until then, every
/// operation returns an explicit error so Windows builds cannot silently run
/// without lock-file I/O.
#[cfg(windows)]
pub(crate) struct WindowsFileLock {
    _marker: std::marker::PhantomData<()>,
}

#[cfg(windows)]
impl WindowsFileLock {
    pub(crate) fn new() -> Self {
        WindowsFileLock {
            _marker: std::marker::PhantomData,
        }
    }
}

#[cfg(windows)]
impl FileLock for WindowsFileLock {
    fn write_at(&self, _offset: u64, _data: &[u8]) -> Result<()> {
        Err(Error::Internal(
            "WindowsFileLock::write_at: not yet implemented".into(),
        ))
    }

    fn read_exact_at(&self, _offset: u64, _buf: &mut [u8]) -> Result<()> {
        Err(Error::Internal(
            "WindowsFileLock::read_exact_at: not yet implemented".into(),
        ))
    }

    fn acquire_exclusive(&self, _timeout: Duration) -> Result<bool> {
        Err(Error::Internal(
            "WindowsFileLock::acquire_exclusive: not yet implemented".into(),
        ))
    }

    fn acquire_shared(&self, _timeout: Duration) -> Result<bool> {
        Err(Error::Internal(
            "WindowsFileLock::acquire_shared: not yet implemented".into(),
        ))
    }

    fn release(&self) -> Result<()> {
        Err(Error::Internal(
            "WindowsFileLock::release: not yet implemented".into(),
        ))
    }
}

// ---------------------------------------------------------------------------
// AnyFileLock — closed-set enum that avoids vtable dispatch
// ---------------------------------------------------------------------------

/// Closed-set enum wrapping all platform [`FileLock`] implementations.
///
/// Replaces `Box<dyn FileLock>` / `Arc<dyn FileLock>` with a concrete type
/// that the compiler can monomorphize, eliminating vtable dispatch on every
/// I/O and locking call.
pub(crate) enum AnyFileLock {
    #[cfg(unix)]
    Posix(PosixFileLock),
    #[cfg(windows)]
    Windows(WindowsFileLock),
    #[cfg(not(any(unix, windows)))]
    Noop(NoopFileLock),
}

impl FileLock for AnyFileLock {
    fn acquire_exclusive(&self, timeout: Duration) -> Result<bool> {
        match self {
            #[cfg(unix)]
            Self::Posix(l) => l.acquire_exclusive(timeout),
            #[cfg(windows)]
            Self::Windows(l) => l.acquire_exclusive(timeout),
            #[cfg(not(any(unix, windows)))]
            Self::Noop(l) => l.acquire_exclusive(timeout),
        }
    }

    fn acquire_shared(&self, timeout: Duration) -> Result<bool> {
        match self {
            #[cfg(unix)]
            Self::Posix(l) => l.acquire_shared(timeout),
            #[cfg(windows)]
            Self::Windows(l) => l.acquire_shared(timeout),
            #[cfg(not(any(unix, windows)))]
            Self::Noop(l) => l.acquire_shared(timeout),
        }
    }

    fn release(&self) -> Result<()> {
        match self {
            #[cfg(unix)]
            Self::Posix(l) => l.release(),
            #[cfg(windows)]
            Self::Windows(l) => l.release(),
            #[cfg(not(any(unix, windows)))]
            Self::Noop(l) => l.release(),
        }
    }

    fn write_at(&self, offset: u64, data: &[u8]) -> Result<()> {
        match self {
            #[cfg(unix)]
            Self::Posix(l) => l.write_at(offset, data),
            #[cfg(windows)]
            Self::Windows(l) => l.write_at(offset, data),
            #[cfg(not(any(unix, windows)))]
            Self::Noop(l) => l.write_at(offset, data),
        }
    }

    fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        match self {
            #[cfg(unix)]
            Self::Posix(l) => l.read_exact_at(offset, buf),
            #[cfg(windows)]
            Self::Windows(l) => l.read_exact_at(offset, buf),
            #[cfg(not(any(unix, windows)))]
            Self::Noop(l) => l.read_exact_at(offset, buf),
        }
    }
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

/// Open an [`AnyFileLock`] for a database file.
///
/// On Unix, opens the path with `O_RDWR` and returns a [`PosixFileLock`]
/// variant.  On Windows, returns the [`WindowsFileLock`] stub variant.
/// For platforms without native locking, returns the [`NoopFileLock`] variant.
///
/// The returned lock **has not yet acquired any lock mode** — call
/// [`FileLock::acquire_exclusive`] or [`FileLock::acquire_shared`] to
/// acquire the desired mode.
///
/// # Errors
///
/// Returns [`Error::Io`] if the file cannot be opened.
pub(crate) fn open_file_lock(path: &std::path::Path) -> Result<AnyFileLock> {
    #[cfg(unix)]
    {
        // Open with O_RDWR so we can acquire both shared and exclusive locks.
        // (F_WRLCK requires the fd to be open for writing.)
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(Error::Io)?;
        Ok(AnyFileLock::Posix(PosixFileLock::from_file(file)))
    }

    #[cfg(windows)]
    {
        let _ = path; // suppress unused warning
        Ok(AnyFileLock::Windows(WindowsFileLock::new()))
    }

    #[cfg(not(any(unix, windows)))]
    {
        let _ = path;
        Ok(AnyFileLock::Noop(NoopFileLock))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
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
}
