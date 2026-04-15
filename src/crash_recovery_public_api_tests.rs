//! Crash recovery through the public API — R3.2 (hq-n9ci)
//!
//! Verifies that data inserted via [`Client::open_with_options`] in
//! [`DurabilityMode::FullSync`] mode survives a process crash (SIGKILL) that
//! arrives **after** the fsync for the committed data has completed.
//!
//! ## Test design (MF-5 compliance)
//!
//! The test uses a pipe as the synchronization primitive to ensure the parent
//! does not issue SIGKILL before the child's fsync completes:
//!
//! ```text
//! Parent                           Child
//! ──────────────────────────────────────────────
//! setup_seed_data(db_path)         (not yet running)
//! pipe(read_fd, write_fd)
//! fork()
//!                                  close(read_fd)
//!                                  Client::open_with_options(FullSync)
//!                                  collection.insert_one({...})
//!                                   └─ fsync() called by FullSync path
//!                                  write(write_fd, 1 byte)  ← signal
//!                                  sleep(60)  ← wait to be killed
//! close(write_fd)
//! read(read_fd, 1 byte)  ← wait for signal (MF-5 sync point)
//! kill(child, SIGKILL)   ← crash after fsync
//! waitpid(child)
//! close(read_fd)
//! Client::open_with_options(FullSync)
//! find_one({key: "seed"})  → must exist
//! find_one({key: "child_insert"})  → must exist
//! ```
//!
//! The single pipe read ensures the parent can only SIGKILL after the child
//! has written to the pipe — which happens only after `insert_one` returns,
//! meaning fsync has completed.

// Unix-only: uses fork()/SIGKILL/pipe for crash simulation.
#![cfg(unix)]

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use crate::{
        client::Client,
        doc,
        options::{DurabilityMode, OpenOptions},
    };
    use bson::Document;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /// Build [`OpenOptions`] configured for `FullSync` durability.
    fn fullsync_opts() -> OpenOptions {
        OpenOptions::new().durability(DurabilityMode::FullSync)
    }

    /// Insert a "seed" document via the parent before forking.
    ///
    /// The client is dropped (and the file lock released) so the child can
    /// subsequently open the same file.
    fn setup_seed_data(dir: &TempDir) -> std::path::PathBuf {
        let db_path = dir.path().join("crash_public.mqlite");
        let client = Client::open_with_options(&db_path, fullsync_opts())
            .expect("open seed db");
        let db = client.database("test");
        let col = db.collection::<Document>("items");
        col.insert_one(&doc! { "key": "seed", "value": 1i32 })
            .expect("insert seed");
        // Explicit close / drop releases the advisory file lock.
        drop(client);
        db_path
    }

    // -----------------------------------------------------------------------
    // Main test
    // -----------------------------------------------------------------------

    /// **R3.2 crash recovery through public API.**
    ///
    /// Opens a file-backed database in `FullSync` mode, inserts a document in
    /// a child process, and verifies that both the seed document and the
    /// child's document are visible after re-opening the database following a
    /// SIGKILL of the child.
    ///
    /// The pipe between parent and child is the MF-5 synchronization
    /// primitive: the parent cannot kill the child before fsync completes.
    #[test]
    fn crash_recovery_fullsync_via_public_api() {
        let dir = TempDir::new().expect("tempdir");
        let db_path = setup_seed_data(&dir);

        // ---- Create synchronization pipe ------------------------------------
        let mut pipe_fds = [0i32; 2];
        assert_eq!(
            unsafe { libc::pipe(pipe_fds.as_mut_ptr()) },
            0,
            "pipe() failed"
        );
        let (read_fd, write_fd) = (pipe_fds[0], pipe_fds[1]);

        // ---- Fork -----------------------------------------------------------
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork() failed");

        if pid == 0 {
            // ===== CHILD =====
            // Close the read end — child only writes to the pipe.
            unsafe { libc::close(read_fd) };

            // Open the database with FullSync.  The parent released the lock
            // before forking, so the child can acquire it immediately.
            let client = match Client::open_with_options(&db_path, fullsync_opts()) {
                Ok(c) => c,
                Err(_) => unsafe { libc::_exit(2) },
            };
            let db = client.database("test");
            let col = db.collection::<Document>("items");

            // Insert the child's document.  With FullSync, `insert_one`
            // flushes dirty pages and calls fsync(2) before returning.
            match col.insert_one(&doc! { "key": "child_insert", "value": 2i32 }) {
                Ok(_) => {}
                Err(_) => unsafe { libc::_exit(3) },
            }

            // Signal the parent: fsync is complete, safe to kill.
            let signal_byte: u8 = 1;
            // SAFETY: write_fd is valid; signal_byte is on the stack.
            unsafe {
                libc::write(
                    write_fd,
                    &signal_byte as *const u8 as *const libc::c_void,
                    1,
                )
            };

            // Sleep until killed — simulates a live process after a committed write.
            unsafe { libc::sleep(60) };
            unsafe { libc::_exit(0) };
        }

        // ===== PARENT =====
        // Close the write end — parent only reads from the pipe.
        unsafe { libc::close(write_fd) };

        // Wait for the child's fsync-completion signal (MF-5 sync point).
        let mut buf = 0u8;
        let n = unsafe { libc::read(read_fd, &mut buf as *mut u8 as *mut libc::c_void, 1) };
        unsafe { libc::close(read_fd) };

        if n != 1 {
            // Child exited before signalling — kill it and fail.
            unsafe { libc::kill(pid, libc::SIGKILL) };
            unsafe { libc::waitpid(pid, std::ptr::null_mut(), 0) };
            panic!("child exited before signalling fsync completion");
        }

        // ---- SIGKILL the child (crash after fsync) --------------------------
        unsafe { libc::kill(pid, libc::SIGKILL) };
        unsafe { libc::waitpid(pid, std::ptr::null_mut(), 0) };

        // ---- Recover and validate -------------------------------------------

        // Reopen the database — this is the crash-recovery path.
        let client =
            Client::open_with_options(&db_path, fullsync_opts()).expect("reopen after crash");
        let db = client.database("test");
        let col = db.collection::<Document>("items");

        // Seed document must be present (written before fork).
        let seed = col
            .find_one(doc! { "key": "seed" })
            .expect("find_one seed")
            .expect("seed document must survive crash");
        assert_eq!(
            seed.get_i32("value").ok(),
            Some(1),
            "seed document value must be 1"
        );

        // Child document must be present (written with FullSync → fsync
        // completed before SIGKILL).
        let child_doc = col
            .find_one(doc! { "key": "child_insert" })
            .expect("find_one child_insert")
            .expect("child_insert document must survive crash (FullSync fsync completed before kill)");
        assert_eq!(
            child_doc.get_i32("value").ok(),
            Some(2),
            "child_insert document value must be 2"
        );
    }
}
