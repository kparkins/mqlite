mod tests {
    use super::super::*;
    use crate::error::Error;
    use crate::options::OpenOptions;
    use crate::storage::header::{FileHeader, HEADER_PAGE_SIZE};
    #[cfg(unix)]
    use libc;
    use std::fs;
    use tempfile::TempDir;

    // ---- Symlink rejection -------------------------------------------------

    #[test]
    #[cfg(unix)]
    fn open_symlink_is_rejected() {
        let dir = tempfile::tempdir().expect("tempdir");
        let real_file = dir.path().join("real.mqlite");
        let symlink_path = dir.path().join("link.mqlite");

        fs::write(&real_file, b"").expect("create real file");
        std::os::unix::fs::symlink(&real_file, &symlink_path).expect("create symlink");

        let result = Client::open(&symlink_path);
        assert!(result.is_err(), "expected error opening symlink");
        let err = result.err().unwrap();
        assert!(
            matches!(err, Error::SymlinkRejected { .. }),
            "expected SymlinkRejected"
        );
    }

    #[test]
    #[cfg(unix)]
    fn symlink_rejected_error_code() {
        let dir = tempfile::tempdir().expect("tempdir");
        let real_file = dir.path().join("real.mqlite");
        let symlink_path = dir.path().join("link.mqlite");

        fs::write(&real_file, b"").expect("create real file");
        std::os::unix::fs::symlink(&real_file, &symlink_path).expect("create symlink");

        let result = Client::open(&symlink_path);
        let err = result.err().unwrap();
        assert_eq!(
            err.code(),
            Some(2),
            "SymlinkRejected should have error code BAD_VALUE (2)"
        );
    }

    #[test]
    fn open_new_file_succeeds() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("new.mqlite");
        assert!(!db_path.exists());

        let _client = Client::open(&db_path).expect("should create new database");
    }

    // ---- File permissions --------------------------------------------------

    #[test]
    #[cfg(unix)]
    fn new_database_file_has_0600_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("perms.mqlite");

        Client::open(&db_path).expect("open");

        let meta = fs::metadata(&db_path).expect("metadata");
        let mode = meta.permissions().mode() & 0o777;
        assert_eq!(
            mode, 0o600,
            "database file must have mode 0600, got {:o}",
            mode
        );
    }

    #[test]
    fn open_existing_regular_file_succeeds() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("existing.mqlite");
        fs::write(&db_path, b"").expect("create file");

        let _client = Client::open(&db_path).expect("open existing file");
    }

    // ---- Multi-process file locking ----------------------------------------

    #[test]
    #[cfg(unix)]
    fn in_memory_open_does_not_lock() {
        let _tempdir = TempDir::new().expect("tempdir");
        let _c1 = Client::open(_tempdir.path().join("db1.mqlite")).expect("first open");
        let _tempdir2 = TempDir::new().expect("tempdir");
        let _c2 = Client::open(_tempdir2.path().join("db2.mqlite")).expect("second open");
    }

    #[test]
    #[cfg(unix)]
    fn cross_process_second_writer_gets_writer_busy() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("locked.mqlite");

        let (read_fd, write_fd) = {
            let mut fds = [0i32; 2];
            assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
            (fds[0], fds[1])
        };

        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork() failed");

        if pid == 0 {
            unsafe { libc::close(read_fd) };
            let _client = Client::open(&db_path).expect("child: Client::open");
            let ready: u8 = 1;
            unsafe { libc::write(write_fd, &ready as *const u8 as *const libc::c_void, 1) };
            std::thread::sleep(std::time::Duration::from_secs(5));
            unsafe { libc::_exit(0) };
        }

        unsafe { libc::close(write_fd) };
        let mut buf = 0u8;
        let n = unsafe { libc::read(read_fd, &mut buf as *mut u8 as *mut libc::c_void, 1) };
        assert_eq!(n, 1, "parent: did not receive child ready signal");
        unsafe { libc::close(read_fd) };

        let result = Client::open_with_options(
            &db_path,
            OpenOptions::new().busy_timeout(std::time::Duration::ZERO),
        );
        assert!(
            matches!(result, Err(Error::WriterBusy)),
            "expected WriterBusy, got: {:?}",
            result.err()
        );

        unsafe { libc::kill(pid, libc::SIGKILL) };
        unsafe { libc::waitpid(pid, std::ptr::null_mut(), 0) };
    }

    #[test]
    #[cfg(unix)]
    fn writer_crash_releases_lock_for_next_opener() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("crash.mqlite");

        let (read_fd, write_fd) = {
            let mut fds = [0i32; 2];
            assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
            (fds[0], fds[1])
        };

        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork() failed");

        if pid == 0 {
            unsafe { libc::close(read_fd) };
            let _client = Client::open(&db_path).expect("child: Client::open");
            let ready: u8 = 1;
            unsafe { libc::write(write_fd, &ready as *const u8 as *const libc::c_void, 1) };
            std::thread::sleep(std::time::Duration::from_secs(60));
            unsafe { libc::_exit(0) };
        }

        unsafe { libc::close(write_fd) };
        let mut buf = 0u8;
        let n = unsafe { libc::read(read_fd, &mut buf as *mut u8 as *mut libc::c_void, 1) };
        assert_eq!(n, 1);
        unsafe { libc::close(read_fd) };

        unsafe { libc::kill(pid, libc::SIGKILL) };
        unsafe { libc::waitpid(pid, std::ptr::null_mut(), 0) };

        Client::open(&db_path).expect("should open after writer crash");
    }

    // ---- Header initialization / corruption detection --------------------

    #[test]
    fn new_file_has_valid_header_on_disk() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("init.mqlite");

        Client::open(&db_path).expect("open new database");

        let file_size = std::fs::metadata(&db_path).expect("metadata").len();
        assert!(
            file_size >= HEADER_PAGE_SIZE as u64,
            "header must be written: file size is {file_size} bytes"
        );

        let mut buf = [0u8; HEADER_PAGE_SIZE];
        let mut f = std::fs::File::open(&db_path).expect("open file");
        use std::io::Read;
        f.read_exact(&mut buf).expect("read header");
        let header = FileHeader::from_bytes(&buf).expect("parse header");
        header.validate().expect("validate header");
    }

    #[test]
    fn open_corrupt_file_returns_corrupt_database() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("corrupt.mqlite");

        let garbage = vec![0xDE_u8; HEADER_PAGE_SIZE];
        fs::write(&db_path, &garbage).expect("write garbage");

        let result = Client::open(&db_path);
        assert!(result.is_err(), "expected error opening corrupt file");
        assert!(
            matches!(result.err().unwrap(), Error::CorruptDatabase { .. }),
            "expected CorruptDatabase"
        );
    }

    #[test]
    fn open_bad_magic_returns_corrupt_database_with_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("bad_magic.mqlite");

        let good_header = FileHeader::new_now();
        let mut bytes = good_header.to_bytes();
        bytes[0] = b'X';
        let checksum = FileHeader::compute_checksum(bytes[..64].try_into().expect("64 bytes"));
        bytes[64..68].copy_from_slice(&checksum.to_le_bytes());
        fs::write(&db_path, &bytes).expect("write bad-magic file");

        let result = Client::open(&db_path);
        match result.err().expect("expected an error") {
            Error::CorruptDatabase { path, .. } => {
                assert_eq!(path, db_path, "path must be attached to the error");
            }
            other => panic!("expected CorruptDatabase, got: {:?}", other),
        }
    }

    #[test]
    fn open_truncated_file_returns_corrupt_database() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("truncated.mqlite");

        fs::write(&db_path, b"MQLT").expect("write truncated file");

        let result = Client::open(&db_path);
        assert!(
            matches!(
                result.err().expect("expected error"),
                Error::CorruptDatabase { .. }
            ),
            "expected CorruptDatabase for truncated file"
        );
    }

    #[test]
    fn reopen_after_close_succeeds() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("reopen.mqlite");

        let client = Client::open(&db_path).expect("first open");
        client.close().ok();

        let _c2 = Client::open(&db_path).expect("second open after close");
    }

    // ---- Drop behavior -----------------------------------------------------

    #[test]
    fn drop_releases_file_lock() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("drop_lock.mqlite");

        {
            let _client = Client::open(&db_path).expect("first open");
        }

        let _c2 = Client::open(&db_path).expect("reopen after drop");
    }

    #[test]
    fn drop_does_not_corrupt_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("drop_intact.mqlite");

        Client::open(&db_path).expect("open");

        let mut buf = [0u8; HEADER_PAGE_SIZE];
        let mut f = std::fs::File::open(&db_path).expect("open file");
        use std::io::Read;
        f.read_exact(&mut buf).expect("read header");
        let header = FileHeader::from_bytes(&buf).expect("parse header after drop");
        header.validate().expect("validate header after drop");
    }

    #[test]
    fn tempdir_client_creates_no_files_outside_tempdir() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file_count_before = fs::read_dir(dir.path()).expect("read dir").count();

        {
            let _tempdir = TempDir::new().expect("tempdir");
            let _client = Client::open(_tempdir.path().join("db.mqlite")).expect("open");
        }

        let file_count_after = fs::read_dir(dir.path()).expect("read dir").count();

        assert_eq!(
            file_count_before, file_count_after,
            "tempdir-backed client must not create files outside its own tempdir"
        );
    }

    #[test]
    fn clone_keeps_inner_alive_after_original_drop() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("clone.mqlite");

        let c1 = Client::open(&db_path).expect("open");
        let c2 = c1.clone();
        drop(c1);

        let db = c2.database("test");
        let _: Vec<String> = db.list_collection_names().unwrap_or_default();
    }

    // ---- database() API -------------------------------------------------------

    #[test]
    fn database_returns_handle_with_correct_name() {
        let _tempdir = TempDir::new().expect("tempdir");
        let client = Client::open(_tempdir.path().join("db.mqlite")).expect("open");
        let db = client.database("myapp");
        assert_eq!(db.name(), "myapp");
    }

    #[test]
    fn multiple_databases_are_independent() {
        let _tempdir = TempDir::new().expect("tempdir");
        let client = Client::open(_tempdir.path().join("db.mqlite")).expect("open");
        use bson::doc;
        use serde::{Deserialize, Serialize};
        #[derive(Serialize, Deserialize, Debug)]
        struct Item {
            x: i32,
        }

        let db_a = client.database("alpha");
        let db_b = client.database("beta");

        let col_a = db_a.collection::<Item>("things");
        let col_b = db_b.collection::<Item>("things");

        col_a.insert_one(&Item { x: 1 }).expect("insert into alpha");
        col_b.insert_one(&Item { x: 2 }).expect("insert into beta");

        // alpha.things has x=1, beta.things has x=2
        let a_doc = col_a.find_one(doc! {}).expect("find_one alpha").unwrap();
        let b_doc = col_b.find_one(doc! {}).expect("find_one beta").unwrap();

        assert_eq!(a_doc.x, 1, "alpha collection should have x=1");
        assert_eq!(b_doc.x, 2, "beta collection should have x=2");
    }

    // -----------------------------------------------------------------------
    // SWMR — concurrent reader tests
    // -----------------------------------------------------------------------

    /// Verify that concurrent reads via the public `Client` API do not block
    /// each other: multiple reader threads run simultaneously without
    /// serializing on a single lock.
    #[test]
    fn swmr_concurrent_reads_via_client_do_not_deadlock() {
        use bson::doc;
        use std::sync::Arc;
        use std::thread;

        let _tempdir = TempDir::new().expect("tempdir");
        let client = Arc::new(Client::open(_tempdir.path().join("db.mqlite")).expect("open"));
        let db = client.database("test");
        let col = db.collection::<bson::Document>("data");

        // Seed data.
        for i in 0..50i32 {
            col.insert_one(&doc! { "v": i }).expect("insert");
        }

        // 16 concurrent readers.
        let handles: Vec<_> = (0..16)
            .map(|_| {
                let c = Arc::clone(&client);
                thread::spawn(move || {
                    let db = c.database("test");
                    let col = db.collection::<bson::Document>("data");
                    let docs: Vec<_> = col
                        .find(doc! {})
                        .run()
                        .expect("find")
                        .filter_map(|r| r.ok())
                        .collect();
                    assert_eq!(docs.len(), 50, "all 50 docs must be visible");
                })
            })
            .collect();

        for h in handles {
            h.join().expect("reader panicked");
        }
    }

    /// Verify that concurrent writes via Client all eventually succeed:
    /// writes serialize internally and none are lost.
    #[test]
    fn swmr_concurrent_writes_via_client_all_succeed() {
        use bson::doc;
        use std::sync::Arc;
        use std::thread;

        let _tempdir = TempDir::new().expect("tempdir");
        let client = Arc::new(Client::open(_tempdir.path().join("db.mqlite")).expect("open"));

        // 8 writer threads, each inserts 10 docs.
        let handles: Vec<_> = (0..8u32)
            .map(|w| {
                let c = Arc::clone(&client);
                thread::spawn(move || {
                    let col = c.database("test").collection::<bson::Document>("data");
                    for j in 0..10u32 {
                        col.insert_one(&doc! { "w": w as i32, "j": j as i32 })
                            .expect("insert");
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().expect("writer panicked");
        }

        let count = client
            .database("test")
            .collection::<bson::Document>("data")
            .count_documents(bson::doc! {})
            .expect("count");
        assert_eq!(count, 80, "all 80 documents from 8 writers must be present");
    }

    // -----------------------------------------------------------------------
    // Database::backup — consistent hot copy
    // -----------------------------------------------------------------------

    /// Basic hot backup: insert data, backup, reopen the copy, verify data.
    #[test]
    fn backup_produces_consistent_copy() {
        use bson::doc;

        let dir = tempfile::tempdir().expect("tempdir");
        let src_path = dir.path().join("src.mqlite");
        let dst_path = dir.path().join("dst.mqlite");

        // Seed the source database.
        {
            let client = Client::open(&src_path).expect("open source");
            let col = client
                .database("mydb")
                .collection::<bson::Document>("items");
            for i in 0..100i32 {
                col.insert_one(&doc! { "n": i }).expect("insert");
            }
            // Hot backup while the database is open.
            client.backup(&dst_path).expect("backup");
        }

        // Reopen the backup and verify the document count.
        {
            let client = Client::open(&dst_path).expect("open backup");
            let count = client
                .database("mydb")
                .collection::<bson::Document>("items")
                .count_documents(doc! {})
                .expect("count");
            assert_eq!(count, 100, "backup must contain all 100 documents");
        }
    }

    /// backup() to the same path as the source must return an error.
    #[test]
    fn backup_to_self_returns_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("db.mqlite");
        let client = Client::open(&path).expect("open");
        let result = client.backup(&path);
        assert!(
            result.is_err(),
            "backup to self must fail, got: {:?}",
            result
        );
    }

    /// backup() to a symlink destination must be rejected.
    #[test]
    #[cfg(unix)]
    fn backup_symlink_dest_rejected() {
        let dir = tempfile::tempdir().expect("tempdir");
        let src_path = dir.path().join("src.mqlite");
        let real_dst = dir.path().join("real.mqlite");
        let sym_dst = dir.path().join("link.mqlite");

        fs::write(&real_dst, b"").expect("create real dst");
        std::os::unix::fs::symlink(&real_dst, &sym_dst).expect("create symlink");

        let client = Client::open(&src_path).expect("open source");
        let result = client.backup(&sym_dst);
        assert!(
            matches!(result, Err(Error::SymlinkRejected { .. })),
            "expected SymlinkRejected, got: {:?}",
            result
        );
    }

    /// backup() overwrites an existing destination file.
    #[test]
    fn backup_overwrites_existing_dest() {
        use bson::doc;

        let dir = tempfile::tempdir().expect("tempdir");
        let src_path = dir.path().join("src.mqlite");
        let dst_path = dir.path().join("dst.mqlite");

        // Seed source.
        let client = Client::open(&src_path).expect("open source");
        let col = client.database("db").collection::<bson::Document>("col");
        col.insert_one(&doc! { "x": 1i32 }).expect("insert");

        // First backup.
        client.backup(&dst_path).expect("first backup");
        // Second backup — must overwrite the first without error.
        col.insert_one(&doc! { "x": 2i32 }).expect("insert again");
        client.backup(&dst_path).expect("second backup");

        // Verify both docs are in the second backup.
        let bkup = Client::open(&dst_path).expect("open backup");
        let count = bkup
            .database("db")
            .collection::<bson::Document>("col")
            .count_documents(doc! {})
            .expect("count");
        assert_eq!(count, 2, "second backup must contain both documents");
    }

    /// backup() destination file must have 0600 permissions on Unix.
    #[test]
    #[cfg(unix)]
    fn backup_dest_has_restricted_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().expect("tempdir");
        let src_path = dir.path().join("src.mqlite");
        let dst_path = dir.path().join("dst.mqlite");

        let client = Client::open(&src_path).expect("open source");
        client.backup(&dst_path).expect("backup");

        let meta = fs::metadata(&dst_path).expect("metadata");
        let mode = meta.permissions().mode() & 0o777;
        assert_eq!(
            mode, 0o600,
            "backup file must have mode 0600, got {:o}",
            mode
        );
    }
}

#[cfg(all(test, unix))]
mod crash_recovery_public_api_tests {
    use std::path::{Path, PathBuf};
    use std::process::{Child, Command};
    use std::time::{Duration, Instant};

    use tempfile::TempDir;

    use crate::{
        client::Client,
        doc,
        options::{DurabilityMode, OpenOptions},
    };
    use bson::Document;

    const SUPERVISOR_DIR_ENV: &str = "MQLITE_CRASH_PUBLIC_SUPERVISOR_DIR";
    const CHILD_DB_PATH_ENV: &str = "MQLITE_CRASH_PUBLIC_CHILD_DB_PATH";
    const CHILD_READY_PATH_ENV: &str = "MQLITE_CRASH_PUBLIC_CHILD_READY_PATH";
    const SUPERVISOR_PROCESS_TEST: &str =
        "client::tests::crash_recovery_public_api_tests::crash_recovery_fullsync_supervisor_process";
    const CHILD_PROCESS_TEST: &str =
        "client::tests::crash_recovery_public_api_tests::crash_recovery_fullsync_child_process";
    const CHILD_READY_TIMEOUT: Duration = Duration::from_secs(10);
    const CHILD_READY_POLL: Duration = Duration::from_millis(10);
    const CHILD_SLEEP_AFTER_READY: Duration = Duration::from_secs(60);

    fn fullsync_opts() -> OpenOptions {
        OpenOptions::new().durability(DurabilityMode::FullSync)
    }

    fn setup_seed_data(dir: &Path) -> PathBuf {
        let db_path = dir.join("crash_public.mqlite");
        {
            let client =
                Client::open_with_options(&db_path, fullsync_opts()).expect("open seed db");
            let db = client.database("test");
            let col = db.collection::<Document>("items");
            col.insert_one(&doc! { "key": "seed", "value": 1i32 })
                .expect("insert seed");
        }
        db_path
    }

    fn wait_for_child_ready(child: &mut Child, ready_path: &Path) {
        let deadline = Instant::now() + CHILD_READY_TIMEOUT;
        loop {
            if ready_path.exists() {
                return;
            }
            if let Some(status) = child.try_wait().expect("poll child status") {
                panic!("child exited before signalling fsync completion: {status}");
            }
            if Instant::now() >= deadline {
                child.kill().expect("kill unresponsive child");
                child.wait().expect("wait for killed child");
                panic!("timed out waiting for child fsync completion signal");
            }
            std::thread::sleep(CHILD_READY_POLL);
        }
    }

    #[test]
    fn crash_recovery_fullsync_child_process() {
        let Some(db_path) = std::env::var_os(CHILD_DB_PATH_ENV) else {
            return;
        };
        let ready_path = std::env::var_os(CHILD_READY_PATH_ENV)
            .map(PathBuf::from)
            .expect("child ready path env");
        let client = Client::open_with_options(PathBuf::from(db_path), fullsync_opts())
            .expect("child open client");
        let db = client.database("test");
        let col = db.collection::<Document>("items");
        col.insert_one(&doc! { "key": "child_insert", "value": 2i32 })
            .expect("child insert");
        std::fs::write(ready_path, b"1").expect("write child ready marker");
        std::thread::sleep(CHILD_SLEEP_AFTER_READY);
    }

    #[test]
    fn crash_recovery_fullsync_supervisor_process() {
        let Some(dir) = std::env::var_os(SUPERVISOR_DIR_ENV) else {
            return;
        };
        let dir = PathBuf::from(dir);
        let db_path = setup_seed_data(&dir);
        let ready_path = dir.join("child-ready");
        let current_exe = std::env::current_exe().expect("current test binary");
        let mut child = Command::new(current_exe)
            .arg("--exact")
            .arg(CHILD_PROCESS_TEST)
            .arg("--test-threads=1")
            .env_remove(SUPERVISOR_DIR_ENV)
            .env(CHILD_DB_PATH_ENV, &db_path)
            .env(CHILD_READY_PATH_ENV, &ready_path)
            .spawn()
            .expect("spawn crash child test process");

        wait_for_child_ready(&mut child, &ready_path);
        child.kill().expect("kill child after fsync completion");
        child.wait().expect("wait for killed child");

        let client =
            Client::open_with_options(&db_path, fullsync_opts()).expect("reopen after crash");
        let db = client.database("test");
        let col = db.collection::<Document>("items");

        let seed = col
            .find_one(doc! { "key": "seed" })
            .expect("find_one seed")
            .expect("seed document must survive crash");
        assert_eq!(
            seed.get_i32("value").ok(),
            Some(1),
            "seed document value must be 1"
        );

        let child_doc = col
            .find_one(doc! { "key": "child_insert" })
            .expect("find_one child_insert")
            .expect(
                "child_insert document must survive crash (FullSync fsync completed before kill)",
            );
        assert_eq!(
            child_doc.get_i32("value").ok(),
            Some(2),
            "child_insert document value must be 2"
        );
    }

    #[test]
    fn crash_recovery_fullsync_via_public_api() {
        let dir = TempDir::new().expect("tempdir");
        let current_exe = std::env::current_exe().expect("current test binary");
        let output = Command::new(current_exe)
            .arg("--exact")
            .arg(SUPERVISOR_PROCESS_TEST)
            .arg("--test-threads=1")
            .env(SUPERVISOR_DIR_ENV, dir.path())
            .output()
            .expect("spawn crash supervisor test process");
        assert!(
            output.status.success(),
            "crash supervisor failed: status={:?}\nstdout:\n{}\nstderr:\n{}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[cfg(test)]
mod compat_tests {
    use crate::{
        client::Client,
        doc,
        error::{codes, Error},
        options::ReturnDocument,
        IndexModel, IndexOptions,
    };
    use bson::Document;
    use tempfile::TempDir;

    #[test]
    fn insert_many_ordered_behavioral_contract() {
        let _tempdir = TempDir::new().expect("tempdir");
        let client = Client::open(_tempdir.path().join("db.mqlite")).expect("open");
        let db = client.database("test");
        let col = db.collection::<Document>("items");
        let model = IndexModel::builder()
            .keys(doc! { "x": 1i32 })
            .options(IndexOptions::new().unique(true))
            .build();
        col.create_index(model).unwrap();
        col.insert_one(&doc! { "x": "dup" }).unwrap();
        let docs = vec![
            doc! { "x": "a", "label": "doc0" },
            doc! { "x": "b", "label": "doc1" },
            doc! { "x": "dup", "label": "doc2" },
            doc! { "x": "c", "label": "doc3" },
            doc! { "x": "d", "label": "doc4" },
        ];
        let res = col.insert_many(&docs).ordered(true).run().unwrap();
        assert_eq!(res.inserted_ids.len(), 2);
        assert!(res.inserted_ids.contains_key(&0));
        assert!(res.inserted_ids.contains_key(&1));
        assert_eq!(res.errors.len(), 1);
        assert_eq!(res.errors[0].index, 2);
        assert_eq!(res.errors[0].code, codes::DUPLICATE_KEY);
        assert!(col.find_one(doc! { "x": "c" }).unwrap().is_none());
        assert!(col.find_one(doc! { "x": "d" }).unwrap().is_none());
    }

    #[test]
    fn insert_many_unordered_behavioral_contract() {
        let _tempdir = TempDir::new().expect("tempdir");
        let client = Client::open(_tempdir.path().join("db.mqlite")).expect("open");
        let db = client.database("test");
        let col = db.collection::<Document>("items");
        let model = IndexModel::builder()
            .keys(doc! { "x": 1i32 })
            .options(IndexOptions::new().unique(true))
            .build();
        col.create_index(model).unwrap();
        col.insert_one(&doc! { "x": "dup" }).unwrap();
        let docs = vec![
            doc! { "x": "a", "label": "doc0" },
            doc! { "x": "b", "label": "doc1" },
            doc! { "x": "dup", "label": "doc2" },
            doc! { "x": "c", "label": "doc3" },
            doc! { "x": "d", "label": "doc4" },
        ];
        let res = col.insert_many(&docs).ordered(false).run().unwrap();
        assert_eq!(res.inserted_ids.len(), 4);
        assert!(res.inserted_ids.contains_key(&0));
        assert!(res.inserted_ids.contains_key(&1));
        assert!(!res.inserted_ids.contains_key(&2));
        assert!(res.inserted_ids.contains_key(&3));
        assert!(res.inserted_ids.contains_key(&4));
        assert_eq!(res.errors.len(), 1);
        assert_eq!(res.errors[0].index, 2);
        assert_eq!(res.errors[0].code, codes::DUPLICATE_KEY);
        assert!(col
            .find_one(doc! { "x": "dup", "label": "doc2" })
            .unwrap()
            .is_none());
        assert!(col.find_one(doc! { "x": "a" }).unwrap().is_some());
        assert!(col.find_one(doc! { "x": "b" }).unwrap().is_some());
        assert!(col.find_one(doc! { "x": "c" }).unwrap().is_some());
        assert!(col.find_one(doc! { "x": "d" }).unwrap().is_some());
    }

    #[test]
    fn find_one_and_update_returns_pre_modification() {
        let _tempdir = TempDir::new().expect("tempdir");
        let client = Client::open(_tempdir.path().join("db.mqlite")).expect("open");
        let db = client.database("test");
        let col = db.collection::<Document>("docs");
        col.insert_one(&doc! { "a": 1i32 }).unwrap();
        let returned: Option<Document> = col
            .find_one_and_update(doc! { "a": 1i32 }, doc! { "$set": { "a": 2i32 } })
            .run()
            .unwrap();
        let returned_doc = returned.expect("must return the pre-update document");
        assert_eq!(returned_doc.get_i32("a").unwrap(), 1);
        let db_doc = col
            .find_one(doc! {})
            .unwrap()
            .expect("document must still exist");
        assert_eq!(db_doc.get_i32("a").unwrap(), 2);
    }

    #[test]
    fn find_one_and_update_return_document_after() {
        let _tempdir = TempDir::new().expect("tempdir");
        let client = Client::open(_tempdir.path().join("db.mqlite")).expect("open");
        let db = client.database("test");
        let col = db.collection::<Document>("docs");
        col.insert_one(&doc! { "b": 1i32 }).unwrap();
        let returned: Option<Document> = col
            .find_one_and_update(doc! { "b": 1i32 }, doc! { "$set": { "b": 2i32 } })
            .return_document(ReturnDocument::After)
            .run()
            .unwrap();
        let returned_doc = returned.expect("must return the post-update document");
        assert_eq!(returned_doc.get_i32("b").unwrap(), 2);
    }

    #[test]
    fn upsert_behavioral_contract() {
        let _tempdir = TempDir::new().expect("tempdir");
        let client = Client::open(_tempdir.path().join("db.mqlite")).expect("open");
        let db = client.database("test");
        let col = db.collection::<Document>("users");
        let res = col
            .update_one(
                doc! { "email": "a@b.com" },
                doc! { "$set": { "name": "Alice" } },
            )
            .upsert(true)
            .run()
            .unwrap();
        assert!(res.upserted_id.is_some());
        assert_eq!(res.matched_count, 0);
        assert_eq!(res.modified_count, 0);
        let found = col
            .find_one(doc! { "email": "a@b.com" })
            .unwrap()
            .expect("upserted doc must be findable");
        assert_eq!(found.get_str("email").unwrap(), "a@b.com");
        assert_eq!(found.get_str("name").unwrap(), "Alice");
    }

    #[test]
    fn persistence_round_trip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("round_trip.mqlite");
        let expected_count = 1_000u64;
        let reference_email = "user42@example.com";
        {
            let client = Client::open(&db_path).expect("open new database");
            let db = client.database("app");
            let col = db.collection::<Document>("users");
            let docs: Vec<Document> = (0..expected_count as i32)
                .map(|i| doc! { "email": format!("user{}@example.com", i), "index": i })
                .collect();
            for doc in &docs {
                col.insert_one(doc).expect("insert_one");
            }
            let model = IndexModel::builder()
                .keys(doc! { "email": 1i32 })
                .options(IndexOptions::new().unique(true).name("email_1".to_string()))
                .build();
            col.create_index(model).expect("create email index");
            assert!(col
                .find_one(doc! { "email": reference_email })
                .expect("find_one before close")
                .is_some());
            db.close().expect("close database");
        }
        {
            let client = Client::open(&db_path).expect("reopen database");
            let db = client.database("app");
            let col = db.collection::<Document>("users");
            let count = col.count_documents(doc! {}).expect("count_documents");
            assert_eq!(count, expected_count, "document count must survive reopen");
            let indexes = col.list_indexes().expect("list_indexes");
            let email_idx = indexes.iter().find(|idx| idx.name == "email_1");
            assert!(email_idx.is_some(), "email_1 index must survive reopen");
            assert!(email_idx.unwrap().unique);
            let after_doc = col
                .find_one(doc! { "email": reference_email })
                .expect("find_one after reopen")
                .expect("reference document must be findable after reopen");
            assert_eq!(after_doc.get_str("email").unwrap(), reference_email);
            assert_eq!(after_doc.get_i32("index").unwrap(), 42);
        }
    }

    #[test]
    fn index_vs_scan_consistency_ne() {
        let _tempdir = TempDir::new().expect("tempdir");
        let client = Client::open(_tempdir.path().join("db.mqlite")).expect("open");
        let db = client.database("test");
        let col = db.collection::<Document>("scores");
        for i in 0..10i32 {
            col.insert_one(&doc! { "score": i }).unwrap();
        }
        let model = IndexModel::builder().keys(doc! { "score": 1i32 }).build();
        let idx_name = col.create_index(model).unwrap();
        let filter = doc! { "score": { "$ne": 5i32 } };
        let with_index: Vec<Document> = col
            .find(filter.clone())
            .run()
            .unwrap()
            .collect::<crate::error::Result<_>>()
            .unwrap();
        col.drop_index(&idx_name).unwrap();
        let without_index: Vec<Document> = col
            .find(filter)
            .run()
            .unwrap()
            .collect::<crate::error::Result<_>>()
            .unwrap();
        assert_eq!(with_index.len(), 9);
        assert_eq!(without_index.len(), 9);
        let ids = |docs: &[Document]| -> std::collections::HashSet<Vec<u8>> {
            use crate::keys::encode_key;
            docs.iter()
                .filter_map(|d| d.get("_id"))
                .map(encode_key)
                .collect()
        };
        assert_eq!(ids(&with_index), ids(&without_index));
    }

    #[test]
    fn error_code_duplicate_key() {
        let _tempdir = TempDir::new().expect("tempdir");
        let client = Client::open(_tempdir.path().join("db.mqlite")).expect("open");
        let col = client.database("test").collection::<Document>("u");
        let model = IndexModel::builder()
            .keys(doc! { "email": 1i32 })
            .options(IndexOptions::new().unique(true))
            .build();
        col.create_index(model).unwrap();
        col.insert_one(&doc! { "email": "alice@example.com" })
            .unwrap();
        let err = col
            .insert_one(&doc! { "email": "alice@example.com" })
            .unwrap_err();
        assert!(matches!(err, Error::DuplicateKey { .. }));
        assert_eq!(err.code(), Some(codes::DUPLICATE_KEY));
    }

    #[test]
    fn error_code_unsupported_operator() {
        let _tempdir = TempDir::new().expect("tempdir");
        let client = Client::open(_tempdir.path().join("db.mqlite")).expect("open");
        let col = client.database("test").collection::<Document>("u");
        col.insert_one(&doc! { "x": 1i32 }).unwrap();
        let err = col
            .find(doc! { "$where": "this.x == 1" })
            .run()
            .err()
            .expect("find with $where must return Err");
        assert!(matches!(err, Error::UnsupportedOperator { .. }));
        assert_eq!(err.code(), Some(codes::UNSUPPORTED_OPERATOR));
    }

    #[test]
    fn error_code_unsupported_index_option() {
        let _tempdir = TempDir::new().expect("tempdir");
        let client = Client::open(_tempdir.path().join("db.mqlite")).expect("open");
        let col = client.database("test").collection::<Document>("u");
        let model = IndexModel::builder()
            .keys(doc! { "description": "text" })
            .build();
        let err = col.create_index(model).unwrap_err();
        assert!(matches!(err, Error::UnsupportedIndexOption { .. }));
        assert_eq!(err.code(), Some(codes::CANNOT_CREATE_INDEX));
    }

    #[test]
    fn error_code_document_too_large() {
        let _tempdir = TempDir::new().expect("tempdir");
        let client = Client::open(_tempdir.path().join("db.mqlite")).expect("open");
        let col = client.database("test").collection::<Document>("u");
        let big_doc = doc! { "data": "x".repeat(16 * 1024 * 1024 + 1) };
        let err = col.insert_one(&big_doc).unwrap_err();
        assert!(matches!(err, Error::DocumentTooLarge { .. }));
        assert_eq!(err.code(), Some(codes::DOCUMENT_TOO_LARGE));
    }

    #[test]
    #[cfg(unix)]
    fn error_code_symlink_rejected() {
        let dir = tempfile::tempdir().expect("tempdir");
        let real_file = dir.path().join("real.mqlite");
        let symlink_path = dir.path().join("link.mqlite");
        std::fs::write(&real_file, b"").expect("create real file");
        std::os::unix::fs::symlink(&real_file, &symlink_path).expect("create symlink");
        let err = Client::open(&symlink_path)
            .err()
            .expect("opening symlink must return Err");
        assert!(matches!(err, Error::SymlinkRejected { .. }));
        assert_eq!(err.code(), Some(codes::BAD_VALUE));
    }

    #[test]
    fn collection_not_found_returns_empty() {
        let _tempdir = TempDir::new().expect("tempdir");
        let client = Client::open(_tempdir.path().join("db.mqlite")).expect("open");
        let col = client
            .database("test")
            .collection::<Document>("nonexistent");
        assert_eq!(col.count_documents(doc! {}).unwrap(), 0);
        assert!(col.find_one(doc! {}).unwrap().is_none());
    }
}

#[cfg(test)]
mod journal_atomicity_tests {
    use tempfile::TempDir;

    use crate::{doc, error::Error, Client, Document, IndexModel, IndexOptions, OpenOptions};

    fn open(dir: &TempDir, name: &str) -> Client {
        Client::open_with_options(dir.path().join(name), OpenOptions::new()).expect("open client")
    }

    #[test]
    fn insert_dup_key_leaves_no_zombie_after_reopen() {
        let dir = TempDir::new().expect("tempdir");
        let db_name = "atomicity_zombie.mqlite";
        {
            let client = open(&dir, db_name);
            let col = client.database("t").collection::<Document>("people");
            col.create_index(
                IndexModel::builder()
                    .keys(doc! { "email": 1 })
                    .options(IndexOptions::new().unique(true))
                    .build(),
            )
            .expect("create unique index");
            col.insert_one(&doc! { "_id": 1i32, "email": "a@b.com" })
                .expect("first insert succeeds");
            let err = col
                .insert_one(&doc! { "_id": 2i32, "email": "a@b.com" })
                .unwrap_err();
            assert!(matches!(err, Error::DuplicateKey { .. }));
            assert_eq!(col.count_documents(doc! {}).unwrap(), 1);
            assert!(col.find_one(doc! { "_id": 2i32 }).unwrap().is_none());
        }
        let client = open(&dir, db_name);
        let col = client.database("t").collection::<Document>("people");
        assert_eq!(col.count_documents(doc! {}).unwrap(), 1);
        assert!(col.find_one(doc! { "_id": 2i32 }).unwrap().is_none());
    }

    #[test]
    fn upsert_enforces_unique_secondary_index() {
        let dir = TempDir::new().expect("tempdir");
        let client = open(&dir, "atomicity_upsert.mqlite");
        let col = client.database("t").collection::<Document>("people");
        col.create_index(
            IndexModel::builder()
                .keys(doc! { "email": 1 })
                .options(IndexOptions::new().unique(true))
                .build(),
        )
        .expect("create unique index");
        col.update_one(
            doc! { "_id": 1i32 },
            doc! { "$set": { "email": "x@y.com" } },
        )
        .upsert(true)
        .run()
        .expect("first upsert");
        assert_eq!(col.count_documents(doc! {}).unwrap(), 1);
        let err = col
            .update_one(
                doc! { "_id": 2i32 },
                doc! { "$set": { "email": "x@y.com" } },
            )
            .upsert(true)
            .run()
            .unwrap_err();
        assert!(matches!(err, Error::DuplicateKey { .. }));
        assert_eq!(col.count_documents(doc! {}).unwrap(), 1);
    }

    #[test]
    fn multi_txn_commits_survive_reopen() {
        let dir = TempDir::new().expect("tempdir");
        let db_name = "atomicity_durability.mqlite";
        {
            let client = open(&dir, db_name);
            let col = client.database("t").collection::<Document>("k");
            for i in 0..20i32 {
                col.insert_one(&doc! { "_id": i, "n": i }).unwrap();
            }
        }
        let client = open(&dir, db_name);
        let col = client.database("t").collection::<Document>("k");
        assert_eq!(col.count_documents(doc! {}).unwrap(), 20);
        for i in 0..20i32 {
            assert!(
                col.find_one(doc! { "_id": i }).unwrap().is_some(),
                "doc _id={i} missing after reopen"
            );
        }
    }
}
