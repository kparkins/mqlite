//! POSIX advisory-lock implementation of [`FileLock`](super::FileLock)
//! (Linux + macOS).
//!
//! ## Locking model
//!
//! Cross-process exclusion is delegated entirely to the kernel via
//! `fcntl(F_SETLK)` advisory byte-range locks on a dedicated file descriptor.
//! The writer range ([`WRITER_LOCK_OFFSET`], [`WRITER_LOCK_LEN`]) takes an
//! exclusive (`F_WRLCK`) lock; the reader range ([`READER_LOCK_OFFSET`],
//! [`READER_LOCK_LEN`]) takes a shared (`F_RDLCK`) lock.  Both ranges live in
//! the header's reserved area (offsets 76–127), which the storage engine never
//! interprets, so the kernel-managed lock metadata is safe there.
//!
//! ## Guarantees
//!
//! - **Cross-process**: the kernel enforces that at most one process holds the
//!   exclusive writer range, and that the exclusive range conflicts with any
//!   shared reader range and vice versa.  A process that is `SIGKILL`ed has all
//!   its advisory locks released by the kernel automatically.
//! - **In-process**: POSIX `fcntl` locks are scoped per-(process, inode,
//!   byte-range): a second `F_SETLK` from the same PID never conflicts with the
//!   first — it replaces/upgrades it.  No process-global registry is needed for
//!   POSIX-parity here; the registry exists only on Windows (see
//!   [`super::windows`]).

use std::time::Duration;

use crate::error::{Error, Result};

use super::LOCK_RETRY_SLEEP;

// ---------------------------------------------------------------------------
// Lock region constants
// ---------------------------------------------------------------------------

/// Starting byte of the exclusive writer lock region (within the header
/// reserved area at offsets 76–127). POSIX only: Windows places its lock
/// ranges beyond EOF because Win32 byte-range locks are mandatory.
pub(crate) const WRITER_LOCK_OFFSET: u64 = 120;

/// Length of the writer lock region in bytes.
pub(crate) const WRITER_LOCK_LEN: u64 = 8;

/// Starting byte of the shared reader lock region (within the header reserved
/// area at offsets 76–127). POSIX only — see [`WRITER_LOCK_OFFSET`].
pub(crate) const READER_LOCK_OFFSET: u64 = 112;

/// Length of the reader lock region in bytes.
pub(crate) const READER_LOCK_LEN: u64 = 8;

// ---------------------------------------------------------------------------
// PosixFileLock
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
///   [`release`](super::FileLock::release) explicitly.
pub(crate) struct PosixFileLock {
    /// Dedicated file handle used only for advisory locking.
    ///
    /// Keeping this separate from any I/O file handle ensures the lock
    /// lifetime matches the `PosixFileLock` lifetime (fd closed on drop →
    /// lock released by kernel).
    file: std::fs::File,
}

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

impl super::FileLock for PosixFileLock {
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
