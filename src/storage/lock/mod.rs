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
//! | Platform  | Implementation       | Lock primitive              |
//! |-----------|----------------------|-----------------------------|
//! | Linux     | [`posix::PosixFileLock`]   | `fcntl(F_SETLK)`            |
//! | macOS     | [`posix::PosixFileLock`]   | `fcntl(F_SETLK)`            |
//! | Windows   | [`windows::WindowsFileLock`] | `LockFileEx`/`UnlockFileEx` |
//! | in-memory | [`NoopFileLock`]     | no-op                       |
//!
//! The platform-specific locking model and its cross-process + in-process
//! guarantees are documented in each platform module: see [`posix`] and
//! [`windows`].  This module holds only the platform-agnostic surface — the
//! [`FileLock`] trait, the [`AnyFileLock`] dispatch enum, the [`NoopFileLock`]
//! fallback, and the [`open_file_lock`] factory.
//!
//! ## References
//!
//! - SQLite `os_unix.c`: `unixLock()`, `unixUnlock()` — reference for
//!   macOS-safe locking patterns
//! - POSIX.1-2017 §fcntl: File Locking
//! - [Windows `LockFileEx`](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-lockfileex)

use std::time::Duration;

use crate::error::{Error, Result};

#[cfg(unix)]
mod posix;
#[cfg(windows)]
mod windows;

// Re-export the platform implementation surface into the `lock` module root so
// that the `#[path]`-included test module (`tests/lock.rs`, which uses
// `super::*`) and any in-module callers resolve the platform types, constants,
// and helpers exactly as they did when everything lived in one file.  No new
// public surface is introduced: the re-exports are crate-private and merely
// mirror the items' own crate-private visibility.
#[cfg(unix)]
pub(crate) use posix::PosixFileLock;
#[cfg(unix)]
#[allow(
    unused_imports,
    reason = "constants are referenced only by the unix test module via super::*"
)]
pub(crate) use posix::{READER_LOCK_LEN, READER_LOCK_OFFSET, WRITER_LOCK_LEN, WRITER_LOCK_OFFSET};

#[cfg(windows)]
pub(crate) use windows::WindowsFileLock;
#[cfg(windows)]
#[allow(
    unused_imports,
    reason = "constants and helpers are referenced only by the windows test module via super::*"
)]
pub(crate) use windows::{
    win_try_lock, win_unlock, WIN_LOCK_LEN, WIN_READER_LOCK_OFFSET, WIN_WRITER_LOCK_OFFSET,
};

// ---------------------------------------------------------------------------
// Shared constants
// ---------------------------------------------------------------------------

/// Sleep interval between retry attempts when the lock is contended.
///
/// Referenced by the platform modules via `super::LOCK_RETRY_SLEEP`.
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
// AnyFileLock — closed-set enum that avoids vtable dispatch
// ---------------------------------------------------------------------------

/// Closed-set enum wrapping all platform [`FileLock`] implementations.
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
/// On Unix, opens the path with `O_RDWR` and returns a
/// [`posix::PosixFileLock`] variant.  On Windows, opens the path read+write
/// with the default share mode (`FILE_SHARE_READ | FILE_SHARE_WRITE |
/// FILE_SHARE_DELETE`) so that other handles can still open the same file, and
/// returns a [`windows::WindowsFileLock`] variant.  For platforms without
/// native locking, returns the [`NoopFileLock`] variant.
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
        // Open read+write. The default Rust share mode (READ|WRITE|DELETE)
        // allows other handles/processes to open the same file concurrently,
        // which is required for SWMR operation.
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(Error::Io)?;
        Ok(AnyFileLock::Windows(WindowsFileLock::from_file(file)?))
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
#[path = "../tests/lock.rs"]
mod tests;
