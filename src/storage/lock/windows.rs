//! Windows `LockFileEx`/`UnlockFileEx` implementation of
//! [`FileLock`](super::FileLock).
//!
//! ## Locking model
//!
//! Cross-process exclusion uses Win32 `LockFileEx` byte-range locks taken
//! beyond any reachable file offset ([`WIN_READER_LOCK_OFFSET`],
//! [`WIN_WRITER_LOCK_OFFSET`]).  Win32 byte-range locks are **mandatory** —
//! any overlapping I/O from another handle returns `ERROR_LOCK_VIOLATION` — so
//! the lock ranges are placed near `i64::MAX`, far beyond live file data.
//! Locking beyond EOF is explicitly legal (`LockFileEx` docs §Remarks).
//!
//! ## Guarantees
//!
//! - **Cross-process**: a foreign process attempting to lock an incompatible
//!   range observes `ERROR_LOCK_VIOLATION`, exactly matching POSIX
//!   cross-process exclusion.  Mandatory-lock safety is preserved because the
//!   ranges never overlap file data.
//! - **In-process (POSIX parity)**: POSIX `fcntl` locks are per-(process,
//!   inode), so the same PID can re-acquire a lock held by a different file
//!   descriptor without conflict.  Win32 `LockFileEx` is per-HANDLE, so a
//!   second in-process handle would normally get `ERROR_LOCK_VIOLATION`.  To
//!   match POSIX, all `WindowsFileLock`s in the process share the process-
//!   global [`PROCESS_LOCKS`] registry keyed by file identity; the OS lock for
//!   a mode is taken at most once through a shared *anchor* handle, and
//!   subsequent in-process acquires increment a counter without an OS call.
//!
//! ## Process-global lock registry
//!
//! ### Why per-process semantics?
//!
//! POSIX `fcntl` advisory locks are scoped to (process, inode, byte-range):
//! any `F_SETLK` from the same PID replaces the existing lock rather than
//! conflicting with it.  Consequently, when the same process holds two open
//! handles to the same `.mqlite` file and one tries to acquire a lock already
//! held by the other, the kernel grants the request immediately.
//!
//! Win32 `LockFileEx` locks are scoped to individual kernel file objects
//! (HANDLEs), not to the process.  A second HANDLE from the same process that
//! tries to lock a byte range already locked by a different HANDLE will receive
//! `ERROR_LOCK_VIOLATION`, exactly as if it came from a foreign process.
//!
//! The [`FileLock`](super::FileLock) contract states that `FileLock` is
//! responsible for **cross-process** serialisation only; in-process
//! serialisation is the `writer_lock: Mutex<()>` in `DatabaseInner`.  An mqlite
//! caller may therefore open multiple handles to the same file within one
//! process (e.g. a `Collection` arc keeping the first client alive while a
//! second `Client::open` occurs), and no conflict should arise.
//!
//! ### Design
//!
//! A process-global `PROCESS_LOCKS: OnceLock<Mutex<HashMap<FileKey, …>>>` maps
//! each open database file (identified by its volume serial number + file
//! index) to a `ProcessFileLockState` that records:
//!   - an *anchor* `File` handle whose lifetime is tied to the registry entry,
//!   - a `handle_refcount` of live `WindowsFileLock` handles for the file,
//!   - a per-key *acquisition gate* (`Arc<Mutex<()>>`) that serialises the OS
//!     lock attempt so it happens at most once per mode,
//!   - the count of in-process holders of the exclusive (writer) range, and
//!   - the count of in-process holders of the shared (reader) range.
//!
//! ### Entry lifetime (handle-refcounted)
//!
//! The entry is created (refcount 0 → 1) the first time a handle for the file is
//! constructed in `from_file`, and each additional handle bumps the refcount.
//! The entry — and its anchor and gate — is removed ONLY when the last live
//! handle is dropped (refcount → 0).  Fully releasing all *lock modes* (both
//! counts → 0) does NOT remove the entry while handles remain.  This is what
//! lets a handle release then re-acquire, and lets a sibling handle acquire
//! after the first releases, and survives the two-step `Client::open`
//! (open-then-acquire) even if another in-process client drops its last lock in
//! between.
//!
//! ### Acquisition (gate-serialised, OS lock taken at most once)
//!
//! When any `WindowsFileLock` in the process acquires a lock mode:
//!   1. Under the registry mutex: clone the per-key gate `Arc` and snapshot the
//!      anchor raw HANDLE; drop the registry mutex.
//!   2. Lock the gate (one acquirer per file at a time).  Still under the
//!      gate, re-check THIS handle's `held` flag for the mode: if set, the
//!      handle has already contributed its +1 to the count — return
//!      `Ok(false)` without incrementing.  This is the *same-handle dedup*
//!      invariant: the counts count handles, not acquire calls, and the
//!      per-handle `held` bool is idempotent, so a same-handle race must
//!      never reach the count bump twice (the winner's `mark_held` completes
//!      before the gate is dropped, so the loser observes the flag here).
//!   3. Re-check the per-mode count under the registry mutex.  If it is now
//!      ≥ 1, increment and return `Ok(false)` — no OS call (POSIX parity:
//!      same-process re-acquire is always uncontended).
//!   4. If the count is 0, call `LockFileEx` through the **anchor** handle with
//!      `LOCKFILE_FAIL_IMMEDIATELY`, retry-looping *outside* the registry mutex
//!      (gate held) until acquired or timeout.  On success, re-lock the registry
//!      mutex and increment.
//!
//! Because the recheck + single OS call + count bump all happen under the gate,
//! two same-process threads can never both reach the OS call at count 0: the
//! loser observes count ≥ 1 and reuses the existing OS lock.  This is what
//! guarantees POSIX parity for exclusive/exclusive races (no spurious
//! `WriterBusy`) and prevents a leaked OS shared lock for shared/shared races
//! (the OS lock is taken once, so a single `UnlockFileEx` always balances it).
//!
//! ### Lock order
//!
//! gate → registry mutex → held mutex.  The acquisition gate is taken before the
//! registry mutex; the registry mutex is dropped before locking the gate or the
//! held mutex and is NEVER held across the OS retry/sleep loop (so foreign-
//! process contention on one file never blocks bookkeeping for another file).
//! The held mutex may be taken while the gate is held (the same-handle dedup
//! re-check and `mark_held`) but is always dropped before the registry mutex
//! is (re)acquired.
//!
//! The anchor is a `try_clone()` of the first opener's file stored in the
//! registry for the lifetime of the entry.  Because Win32 byte-range locks
//! belong to the kernel file object, not the HANDLE, a cloned HANDLE shares
//! lock state with the original.  Storing it independently means individual
//! `WindowsFileLock` drops do not release the OS lock prematurely.
//!
//! When the count for a mode reaches 0, `UnlockFileEx` is called through the
//! anchor (exactly once, mirroring the single `LockFileEx`).
//!
//! ### Cross-process behaviour
//!
//! The first in-process holder takes the real OS lock through the anchor.
//! Foreign processes see `ERROR_LOCK_VIOLATION` exactly as before, so cross-
//! process mutual exclusion is unchanged.  The mandatory-lock safety (ranges at
//! `WIN_READER_LOCK_OFFSET` / `WIN_WRITER_LOCK_OFFSET`, far beyond any file
//! data) is also unchanged.

use std::time::Duration;

use crate::error::{Error, Result};

use super::LOCK_RETRY_SLEEP;

// ---------------------------------------------------------------------------
// Lock region constants
// ---------------------------------------------------------------------------

/// Windows lock ranges live beyond any reachable file offset because
/// Win32 byte-range locks are **mandatory**: locking live header bytes would
/// block ordinary page I/O from other handles or processes.
///
/// Locking beyond EOF is explicitly legal on Windows (`LockFileEx` docs §Remarks).
/// We use 8-byte ranges mirroring the POSIX region sizes.
pub(crate) const WIN_READER_LOCK_OFFSET: u64 = 0x7FFF_FFFF_FFFF_FF00;

/// Offset for the exclusive writer lock range on Windows.
///
/// Placed 8 bytes above [`WIN_READER_LOCK_OFFSET`] so the two ranges do not
/// overlap.  Both are far beyond any file data address.
pub(crate) const WIN_WRITER_LOCK_OFFSET: u64 = 0x7FFF_FFFF_FFFF_FF08;

/// Lock range length used for both reader and writer Windows lock regions.
pub(crate) const WIN_LOCK_LEN: u64 = 8;

// ---------------------------------------------------------------------------
// Process-global lock registry
// ---------------------------------------------------------------------------

/// Stable file identity: (volume serial, file index high word, file index low
/// word) as returned by `GetFileInformationByHandle`.
///
/// Two `HANDLE`s opened to the same on-disk file will produce the same key,
/// even if opened from different paths (hard links, etc.).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct FileKey {
    volume_serial: u32,
    file_index_high: u32,
    file_index_low: u32,
}

/// Per-file process-global lock state.
struct ProcessFileLockState {
    /// Anchor handle whose lifetime equals the registry entry.
    ///
    /// All `LockFileEx`/`UnlockFileEx` OS calls go through this handle.
    /// It is a `try_clone()` of the first opener's file, so its kernel file
    /// object is shared and the byte-range locks it holds persist until
    /// `UnlockFileEx` is called explicitly — not until this `File` is dropped.
    anchor: std::fs::File,
    /// Number of live `WindowsFileLock` handles in this process that reference
    /// this file.  Incremented in [`WindowsFileLock::from_file`], decremented
    /// in [`WindowsFileLock`]'s `Drop`.
    ///
    /// INVARIANT: the registry entry's lifetime equals this refcount — the
    /// entry (and its anchor) is removed only when this reaches 0, never merely
    /// because the lock counts hit 0.  An entry therefore survives a lock being
    /// fully released as long as any handle to the file remains live, which is
    /// what makes release-then-reacquire (same handle) and acquire-after-
    /// sibling-release (two handles) and the two-step `Client::open` race
    /// correct.
    handle_refcount: usize,
    /// Per-key acquisition gate.  Held across the same-handle held-flag
    /// re-check + count-recheck + OS lock attempt + count bump in
    /// [`WindowsFileLock::acquire_mode`] so that two same-process threads
    /// never race `LockFileEx` on the shared anchor, and two same-HANDLE
    /// threads never double-increment a per-mode count.
    ///
    /// Lock order: this gate is acquired BEFORE the registry mutex inside the
    /// acquisition path (the gate Arc is first cloned out under the registry
    /// mutex, the registry mutex is dropped, then the gate is locked).  The
    /// registry mutex is never held across the OS retry/sleep loop, so foreign-
    /// process contention on one file never blocks bookkeeping for another.
    gate: std::sync::Arc<std::sync::Mutex<()>>,
    /// Number of in-process `WindowsFileLock` handles that have acquired the
    /// exclusive (writer) lock range.
    exclusive_holders: usize,
    /// Number of in-process `WindowsFileLock` handles that have acquired the
    /// shared (reader) lock range.
    shared_holders: usize,
}

/// Process-global registry of open database file locks.
///
/// Initialised once on first use.  The inner `Mutex` is held only for
/// bookkeeping operations (increment/decrement counts, insert/remove entry);
/// the retry-sleep loop inside `acquire_with_timeout` runs *outside* it.
static PROCESS_LOCKS: std::sync::OnceLock<
    std::sync::Mutex<std::collections::HashMap<FileKey, ProcessFileLockState>>,
> = std::sync::OnceLock::new();

/// Return a reference to the process-global registry, initialising it on first
/// call.
fn process_locks(
) -> &'static std::sync::Mutex<std::collections::HashMap<FileKey, ProcessFileLockState>> {
    PROCESS_LOCKS.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

/// Derive a [`FileKey`] for the given open file handle via
/// `GetFileInformationByHandle`.
///
/// # Errors
///
/// Returns an `io::Error` if the Win32 call fails (e.g. handle is invalid).
fn file_key(file: &std::fs::File) -> std::io::Result<FileKey> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION,
    };

    // SAFETY: BY_HANDLE_FILE_INFORMATION is a plain C struct; zero-init is
    // valid as a starting state before the syscall fills all fields.
    let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
    let handle = file.as_raw_handle() as windows_sys::Win32::Foundation::HANDLE;

    // SAFETY: `handle` is a valid open HANDLE for the lifetime of `file`;
    // `&mut info` is a valid pointer to a zero-initialised struct of the
    // correct type.
    let ok = unsafe { GetFileInformationByHandle(handle, &mut info) };

    if ok != 0 {
        Ok(FileKey {
            volume_serial: info.dwVolumeSerialNumber,
            file_index_high: info.nFileIndexHigh,
            file_index_low: info.nFileIndexLow,
        })
    } else {
        Err(std::io::Error::last_os_error())
    }
}

/// Call `LockFileEx` through `handle` for the given byte range.
///
/// `exclusive` selects `LOCKFILE_EXCLUSIVE_LOCK` (writer) vs shared (reader).
/// Always passes `LOCKFILE_FAIL_IMMEDIATELY`.
///
/// Returns `Ok(true)` when acquired, `Ok(false)` on `ERROR_LOCK_VIOLATION`,
/// `Err` for any other OS error.
pub(crate) fn win_try_lock(
    handle: windows_sys::Win32::Foundation::HANDLE,
    offset: u64,
    len: u64,
    exclusive: bool,
) -> std::io::Result<bool> {
    use windows_sys::Win32::Foundation::ERROR_LOCK_VIOLATION;
    use windows_sys::Win32::Storage::FileSystem::{
        LockFileEx, LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY,
    };
    use windows_sys::Win32::System::IO::OVERLAPPED;

    let mut flags = LOCKFILE_FAIL_IMMEDIATELY;
    if exclusive {
        flags |= LOCKFILE_EXCLUSIVE_LOCK;
    }

    // SAFETY: OVERLAPPED is a C struct; zero-initialisation is the documented
    // way to specify a synchronous byte-range lock (no async I/O completion).
    let mut overlapped: OVERLAPPED = unsafe { std::mem::zeroed() };
    overlapped.Anonymous.Anonymous.Offset = offset as u32;
    overlapped.Anonymous.Anonymous.OffsetHigh = (offset >> 32) as u32;

    // SAFETY:
    // - `handle` is a valid open HANDLE whose lifetime is managed by the
    //   caller (either `self.file` or the anchor in the registry).
    // - `overlapped` is a valid local OVERLAPPED on the stack; its address is
    //   valid for the duration of this synchronous call.
    // - `LOCKFILE_FAIL_IMMEDIATELY` makes the call non-blocking.
    let ok = unsafe {
        LockFileEx(
            handle,
            flags,
            0,                  // reserved, must be zero
            len as u32,         // nNumberOfBytesToLockLow
            (len >> 32) as u32, // nNumberOfBytesToLockHigh
            &mut overlapped,
        )
    };

    if ok != 0 {
        Ok(true)
    } else {
        let err = std::io::Error::last_os_error();
        match err.raw_os_error() {
            Some(c) if c as u32 == ERROR_LOCK_VIOLATION => Ok(false),
            _ => Err(err),
        }
    }
}

/// Call `UnlockFileEx` through `handle` for the given byte range.
pub(crate) fn win_unlock(
    handle: windows_sys::Win32::Foundation::HANDLE,
    offset: u64,
    len: u64,
) -> std::io::Result<()> {
    use windows_sys::Win32::Storage::FileSystem::UnlockFileEx;
    use windows_sys::Win32::System::IO::OVERLAPPED;

    // SAFETY: zero-init is valid for OVERLAPPED used as a plain byte-range
    // specifier (no async I/O).
    let mut overlapped: OVERLAPPED = unsafe { std::mem::zeroed() };
    overlapped.Anonymous.Anonymous.Offset = offset as u32;
    overlapped.Anonymous.Anonymous.OffsetHigh = (offset >> 32) as u32;

    // SAFETY:
    // - `handle` is a valid HANDLE kept alive by the registry anchor.
    // - `overlapped` is a valid local OVERLAPPED.
    // - The range was previously locked by this handle, so unlocking it is
    //   well-defined.
    let ok = unsafe {
        UnlockFileEx(
            handle,
            0,                  // reserved, must be zero
            len as u32,
            (len >> 32) as u32,
            &mut overlapped,
        )
    };

    if ok != 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

/// Spin-retry `win_try_lock` through `handle` until acquired or `timeout`
/// elapses.  Must be called **without** holding the registry mutex so that
/// other threads can manipulate the registry while we sleep.
///
/// Returns `Ok(true)` if the lock was contended before being acquired,
/// `Ok(false)` if acquired on the first attempt, `Err(WriterBusy)` on
/// timeout, or `Err(Io(_))` for unexpected OS errors.
fn win_acquire_with_timeout(
    handle: windows_sys::Win32::Foundation::HANDLE,
    offset: u64,
    len: u64,
    exclusive: bool,
    timeout: Duration,
) -> Result<bool> {
    use std::time::Instant;

    let deadline = Instant::now() + timeout;
    let mut contended = false;

    loop {
        match win_try_lock(handle, offset, len, exclusive) {
            Ok(true) => return Ok(contended),
            Ok(false) => {
                contended = true;
                if timeout.is_zero() || Instant::now() >= deadline {
                    return Err(Error::WriterBusy);
                }
                std::thread::sleep(LOCK_RETRY_SLEEP);
            }
            Err(e) => return Err(Error::Io(e)),
        }
    }
}

/// What a single [`WindowsFileLock`] handle has contributed to the process
/// registry.  Used during `release()` and `Drop` to correctly decrement the
/// right counters.
#[derive(Default)]
struct HeldLocks {
    /// True if this handle incremented `exclusive_holders` in the registry.
    exclusive: bool,
    /// True if this handle incremented `shared_holders` in the registry.
    shared: bool,
}

/// `LockFileEx`/`UnlockFileEx`-based file lock for Windows.
///
/// ## Mandatory-lock rationale
///
/// Win32 byte-range locks are **mandatory**: any read or write I/O on an
/// overlapping range from another handle fails with `ERROR_LOCK_VIOLATION`.
/// Therefore lock ranges must not overlap live file data.  We use offsets near
/// `i64::MAX` ([`WIN_READER_LOCK_OFFSET`], [`WIN_WRITER_LOCK_OFFSET`]) which
/// are unreachable by normal database page I/O.
///
/// ## Per-process semantics (POSIX parity)
///
/// POSIX `fcntl` advisory locks are per-(process, inode, byte-range): a
/// second `F_SETLK` from the same PID never conflicts with the first.  Win32
/// `LockFileEx` is per-HANDLE: a second HANDLE from the same process *would*
/// conflict.  To match POSIX behaviour, all `WindowsFileLock` instances in the
/// same process share a [`PROCESS_LOCKS`] registry keyed by
/// `(volume_serial, file_index_hi, file_index_lo)`.  The OS lock is taken
/// exactly once through a shared **anchor** handle stored in the registry;
/// subsequent in-process acquires simply increment a counter without touching
/// the OS.  Foreign processes still observe `ERROR_LOCK_VIOLATION` as
/// expected.
///
/// ## Drop behaviour
///
/// `Drop` decrements the registry counters for all modes this handle holds.
/// When a counter reaches zero, `UnlockFileEx` is called through the anchor.
/// Explicit unlock before handle close prevents the asynchronous-release race
/// that makes rapid close-then-reopen sequences flaky on Windows.
pub(crate) struct WindowsFileLock {
    /// File handle used for I/O (`seek_read` / `seek_write`).
    ///
    /// Kept separate from the anchor so that I/O can proceed independently of
    /// lock management.  Both handles reference the same on-disk file.
    file: std::fs::File,
    /// Stable identity of `file`, computed once at construction.
    key: FileKey,
    /// What lock modes this particular handle has contributed to the registry.
    held: std::sync::Mutex<HeldLocks>,
}

impl WindowsFileLock {
    /// Create a `WindowsFileLock` from an already-opened file.
    ///
    /// Registers the file in [`PROCESS_LOCKS`] if not already present,
    /// storing an anchor clone for future OS lock calls.
    ///
    /// Does **not** acquire any lock mode — call [`FileLock::acquire_exclusive`]
    /// or [`FileLock::acquire_shared`] explicitly.
    ///
    /// [`FileLock::acquire_exclusive`]: super::FileLock::acquire_exclusive
    /// [`FileLock::acquire_shared`]: super::FileLock::acquire_shared
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if `GetFileInformationByHandle` or `try_clone`
    /// fails.
    pub(crate) fn from_file(file: std::fs::File) -> Result<Self> {
        let key = file_key(&file).map_err(Error::Io)?;

        // Ensure there is a registry entry with an anchor for this file and
        // bump its handle refcount.  We do this eagerly at construction so the
        // anchor lifetime is clear.  Pre-clone the anchor before locking the
        // registry so that we never call try_clone while holding the mutex
        // (avoids holding the lock across a syscall). The clone is cheap and
        // discarded if the entry already exists.
        {
            let anchor_candidate = file.try_clone().map_err(Error::Io)?;
            let mut registry = process_locks()
                .lock()
                .map_err(|_| Error::StatePoisoned { component: "PROCESS_LOCKS" })?;
            let state = registry.entry(key).or_insert_with(|| ProcessFileLockState {
                anchor: anchor_candidate,
                handle_refcount: 0,
                gate: std::sync::Arc::new(std::sync::Mutex::new(())),
                exclusive_holders: 0,
                shared_holders: 0,
            });
            // INVARIANT: this live handle is counted here and decremented
            // exactly once in Drop; the entry persists until the refcount hits
            // 0, independent of the exclusive/shared lock counts.
            state.handle_refcount += 1;
        }

        Ok(WindowsFileLock {
            file,
            key,
            held: std::sync::Mutex::new(HeldLocks::default()),
        })
    }

    /// Acquire a lock mode, using the process registry for POSIX-parity
    /// same-process semantics.
    ///
    /// ## Acquisition gate (OS lock taken at most once per mode)
    ///
    /// INVARIANT: the OS lock for a mode is taken **at most once per file** even
    /// under concurrent same-process acquires.  This path serialises behind a
    /// per-key acquisition gate (`state.gate`, an `Arc<Mutex<()>>` stored in the
    /// registry entry):
    ///
    /// 1. Under the registry mutex: clone the gate `Arc` and snapshot the
    ///    anchor raw HANDLE.  Drop the registry mutex (we never sleep holding
    ///    it).
    /// 2. Lock the gate.  Only one thread per file is in the acquisition
    ///    critical section at a time.  Still under the gate, re-check THIS
    ///    handle's `held` flag for the mode: if set, the handle has already
    ///    contributed its +1 to the count — return `Ok(false)` without
    ///    incrementing (same-handle dedup, see below).
    /// 3. Re-check the per-mode count under the registry mutex.  If it is now
    ///    `> 0`, take the fast path: bump the counter and return `Ok(false)`
    ///    with **no OS call** (POSIX parity: same-process re-acquire is always
    ///    uncontended).
    /// 4. Otherwise call [`win_acquire_with_timeout`] through the anchor with
    ///    the gate held but the registry mutex dropped (so other files' state
    ///    is never blocked while we retry/sleep).  On success, re-lock the
    ///    registry mutex and bump the counter.
    ///
    /// Because the count-recheck, the single OS call, and the count bump all
    /// happen under the gate, two threads can never both reach the OS call at
    /// `count == 0`: the loser observes `count > 0` after winning the gate and
    /// takes the fast path.  This is what guarantees POSIX parity for the
    /// exclusive/exclusive race (no spurious `WriterBusy`) and the single OS
    /// shared lock for the shared/shared race (so one `UnlockFileEx` balances
    /// it).
    ///
    /// ## Same-handle dedup (one count bump per handle per mode)
    ///
    /// INVARIANT: each (handle, mode) contributes **at most one** to the per-mode
    /// count, so a single `release()` always balances the single increment.  The
    /// per-mode counts count *handles*, not acquire calls, while
    /// [`Self::mark_held`]'s bool is idempotent.  Without the held-flag re-check
    /// in step 2, two threads racing acquire on the SAME handle could both
    /// increment (one via step 4, one via the step-3 fast path) while release
    /// decrements once — the count would stick above 0 and `UnlockFileEx` would
    /// never run.  The held-flag re-check closes this: `mark_held` runs before
    /// the gate is dropped, so at most one acquire per (handle, mode) ever
    /// reaches steps 3–5 while the flag is unset.
    ///
    /// ## Lock order
    ///
    /// gate → registry mutex → held mutex.  The gate is always taken before the
    /// registry mutex within this path; the registry mutex is always dropped
    /// before locking the gate or the held mutex; the registry mutex is never
    /// held across the OS retry/sleep loop.  The held mutex may be taken while
    /// the gate is held (the step-2 dedup re-check and `mark_held`) but is
    /// always dropped before the registry mutex is (re)acquired.  This strict
    /// ordering prevents deadlock between the three locks.
    ///
    /// `is_exclusive`: true for the writer range, false for the reader range.
    fn acquire_mode(&self, is_exclusive: bool, timeout: Duration) -> Result<bool> {
        use std::os::windows::io::AsRawHandle;

        // ---- Step 1: clone the acquisition gate + snapshot the anchor ----
        // SAFETY: we transmit only the integer value of the anchor HANDLE; the
        // anchor File is kept alive in the registry (the entry persists while
        // this live handle holds a refcount), so the HANDLE stays valid.
        let (gate, raw) = {
            let registry = process_locks()
                .lock()
                .map_err(|_| Error::StatePoisoned { component: "PROCESS_LOCKS" })?;
            let state = registry.get(&self.key).ok_or_else(|| {
                Error::Internal("WindowsFileLock: registry entry missing in acquire_mode".into())
            })?;
            let raw = state.anchor.as_raw_handle() as windows_sys::Win32::Foundation::HANDLE;
            (std::sync::Arc::clone(&state.gate), raw)
        };

        // ---- Step 2: hold the per-key acquisition gate ----
        // Only one thread per file may be in the recheck + OS attempt + bump
        // critical section at a time.  The registry mutex was dropped above and
        // is never held across the OS retry/sleep loop below.
        let _gate = gate
            .lock()
            .map_err(|_| Error::StatePoisoned { component: "WindowsFileLock::gate" })?;

        // ---- Step 2b: same-handle dedup under the gate ----
        // Two threads racing acquire on the SAME handle can both pass the
        // unsynchronised held-flag fast path in `acquire_exclusive` /
        // `acquire_shared` while the flag is still false.  The gate serialises
        // them, but the per-mode counts cannot deduplicate a handle.  Without
        // this re-check the gate loser would take the step-3 fast path and
        // increment the count a SECOND time for one handle, while
        // `mark_held`'s idempotent bool makes `release_held` decrement only
        // once — the count would stick above 0 and the OS lock would leak
        // until last-handle Drop.  INVARIANT: the winner's `mark_held`
        // completes before it drops the gate, so the loser observes the flag
        // here and returns without touching the count, keeping the count bump
        // at exactly one per (handle, mode).  Lock order is respected: the held
        // mutex is taken after the gate and dropped before the registry mutex
        // is (re)acquired.
        {
            let held = self.held.lock().map_err(|_| Error::StatePoisoned {
                component: "WindowsFileLock::held",
            })?;
            let already_held = if is_exclusive {
                held.exclusive
            } else {
                held.shared
            };
            if already_held {
                // POSIX parity: same-handle re-acquire is idempotent and
                // uncontended.  The count already includes this handle's +1.
                return Ok(false);
            }
        }

        // ---- Step 3: re-check the count under the registry mutex ----
        {
            let mut registry = process_locks()
                .lock()
                .map_err(|_| Error::StatePoisoned { component: "PROCESS_LOCKS" })?;
            if let Some(state) = registry.get_mut(&self.key) {
                let count = if is_exclusive {
                    state.exclusive_holders
                } else {
                    state.shared_holders
                };
                if count > 0 {
                    // Process already holds this mode — POSIX parity: no OS call.
                    // The gate guarantees we observe a consistent count here, so
                    // a concurrent winner's OS lock is reused, not re-taken.
                    if is_exclusive {
                        state.exclusive_holders += 1;
                    } else {
                        state.shared_holders += 1;
                    }
                    drop(registry);
                    self.mark_held(is_exclusive)?;
                    return Ok(false);
                }
            }
            // count == 0 (or entry vanished, handled below): fall through to the
            // OS lock attempt with the registry mutex released.
        }

        // ---- Step 4: take the OS lock exactly once (gate still held) ----
        let (offset, len) = if is_exclusive {
            (WIN_WRITER_LOCK_OFFSET, WIN_LOCK_LEN)
        } else {
            (WIN_READER_LOCK_OFFSET, WIN_LOCK_LEN)
        };

        let contended = win_acquire_with_timeout(raw, offset, len, is_exclusive, timeout)?;

        // ---- Step 5: record success in the registry ----
        {
            let mut registry = process_locks()
                .lock()
                .map_err(|_| Error::StatePoisoned { component: "PROCESS_LOCKS" })?;
            if let Some(state) = registry.get_mut(&self.key) {
                if is_exclusive {
                    state.exclusive_holders += 1;
                } else {
                    state.shared_holders += 1;
                }
            }
        }
        self.mark_held(is_exclusive)?;
        Ok(contended)
    }

    /// Record on this handle that it has contributed `is_exclusive`'s mode to
    /// the registry.  Acquires the handle-level `held` mutex; the registry
    /// mutex and acquisition gate must NOT be held by the caller's *registry*
    /// lock when this runs (lock order gate → registry → held is preserved by
    /// callers that drop the registry guard before calling this).
    fn mark_held(&self, is_exclusive: bool) -> Result<()> {
        let mut held = self.held.lock().map_err(|_| Error::StatePoisoned {
            component: "WindowsFileLock::held",
        })?;
        if is_exclusive {
            held.exclusive = true;
        } else {
            held.shared = true;
        }
        Ok(())
    }

    /// Release one or both lock modes held by this handle, using the process
    /// registry.
    ///
    /// For each mode held: decrement the registry counter; if it reaches 0,
    /// call `UnlockFileEx` through the anchor.  INVARIANT: the registry entry is
    /// kept while live handles remain (`handle_refcount > 0`) — it is removed
    /// only in [`WindowsFileLock`]'s `Drop` when the last handle goes away.
    /// This is what lets a fully-released-but-still-live handle (and any sibling
    /// handle to the same file) re-acquire: the entry — and its anchor + gate —
    /// survive across a full mode release.
    fn release_held(&self, mut held: std::sync::MutexGuard<'_, HeldLocks>) -> Result<()> {
        use std::os::windows::io::AsRawHandle;

        if !held.exclusive && !held.shared {
            return Ok(());
        }

        let release_exclusive = held.exclusive;
        let release_shared = held.shared;
        held.exclusive = false;
        held.shared = false;
        drop(held); // release handle-level lock before touching registry

        let mut registry = process_locks()
            .lock()
            .map_err(|_| Error::StatePoisoned { component: "PROCESS_LOCKS" })?;

        if let Some(state) = registry.get_mut(&self.key) {
            let anchor_raw =
                state.anchor.as_raw_handle() as windows_sys::Win32::Foundation::HANDLE;

            if release_exclusive && state.exclusive_holders > 0 {
                state.exclusive_holders -= 1;
                if state.exclusive_holders == 0 {
                    // Last holder of this mode — release the single OS lock
                    // (taken at most once by acquire_mode) through the anchor.
                    // SAFETY: anchor_raw is valid while state is in the registry.
                    win_unlock(anchor_raw, WIN_WRITER_LOCK_OFFSET, WIN_LOCK_LEN)
                        .map_err(Error::Io)?;
                }
            }
            if release_shared && state.shared_holders > 0 {
                state.shared_holders -= 1;
                if state.shared_holders == 0 {
                    // SAFETY: anchor_raw is valid while state is in the registry.
                    win_unlock(anchor_raw, WIN_READER_LOCK_OFFSET, WIN_LOCK_LEN)
                        .map_err(Error::Io)?;
                }
            }
            // INVARIANT: the entry is NOT removed here even when both counts hit
            // 0 — it survives until the last live handle is dropped (see
            // `Drop`), so a sibling or re-acquiring handle still finds it.
        }
        Ok(())
    }
}

impl Drop for WindowsFileLock {
    /// Best-effort release of all registry-tracked lock modes on drop, then
    /// decrement the entry's handle refcount and remove the entry (anchor +
    /// gate) when this was the last live handle.
    ///
    /// Explicit unlock before the handle is dropped prevents the Windows
    /// asynchronous-lock-release race that makes rapid close-then-reopen
    /// sequences flaky.  Releasing this handle's held locks first preserves the
    /// previous drop ordering (OS unlock through the anchor before the entry —
    /// and thus the anchor — can be removed).
    fn drop(&mut self) {
        // 1. Release this handle's held lock modes (decrements lock counts and
        //    issues UnlockFileEx through the anchor as needed).  If the mutex is
        //    poisoned we skip lock release but still try to drop the refcount.
        if let Ok(held) = self.held.lock() {
            // Errors in drop are ignored — we cannot propagate them.
            let _ = self.release_held(held);
        }

        // 2. Decrement the handle refcount; remove the entry (anchor + gate)
        //    only when no live handles remain for this file.
        if let Ok(mut registry) = process_locks().lock() {
            if let Some(state) = registry.get_mut(&self.key) {
                if state.handle_refcount > 0 {
                    state.handle_refcount -= 1;
                }
                if state.handle_refcount == 0 {
                    registry.remove(&self.key);
                }
            }
        }
    }
}

impl super::FileLock for WindowsFileLock {
    fn acquire_exclusive(&self, timeout: Duration) -> Result<bool> {
        let held = self.held.lock().map_err(|_| Error::StatePoisoned {
            component: "WindowsFileLock::held",
        })?;
        // Same-handle re-acquire is idempotent (POSIX parity).
        if held.exclusive {
            return Ok(false);
        }
        drop(held);
        self.acquire_mode(true, timeout)
    }

    fn acquire_shared(&self, timeout: Duration) -> Result<bool> {
        let held = self.held.lock().map_err(|_| Error::StatePoisoned {
            component: "WindowsFileLock::held",
        })?;
        // Same-handle re-acquire is idempotent.
        if held.shared {
            return Ok(false);
        }
        drop(held);
        self.acquire_mode(false, timeout)
    }

    fn release(&self) -> Result<()> {
        let held = self.held.lock().map_err(|_| Error::StatePoisoned {
            component: "WindowsFileLock::held",
        })?;
        self.release_held(held)
    }

    fn write_at(&self, offset: u64, data: &[u8]) -> Result<()> {
        use std::os::windows::fs::FileExt;

        let mut written = 0usize;
        while written < data.len() {
            let n = self
                .file
                .seek_write(&data[written..], offset + written as u64)
                .map_err(Error::Io)?;
            written += n;
        }
        Ok(())
    }

    fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        use std::os::windows::fs::FileExt;

        let mut read = 0usize;
        while read < buf.len() {
            match self.file.seek_read(&mut buf[read..], offset + read as u64) {
                Ok(0) => {
                    // EOF reached before the buffer was filled.
                    return Err(Error::Io(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "failed to fill whole buffer",
                    )));
                }
                Ok(n) => read += n,
                Err(e) => return Err(Error::Io(e)),
            }
        }
        Ok(())
    }
}
