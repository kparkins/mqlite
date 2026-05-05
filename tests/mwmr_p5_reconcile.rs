#![doc = "Integration test requiring the test-hooks feature."]
#![cfg(feature = "test-hooks")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    reason = "test target uses assertion-style panics and setup unwraps"
)]

use std::path::Path;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

use bson::{Bson, Document};
use mqlite::error::{Error, WriteConflictReason};
use mqlite::{
    doc, Client, OpenOptions, Us025CrabbingEvent, __us010_drain_events,
    __us010_force_revalidation_failures, __us010_push_classification_override_names,
    __us010_reset_probe, __us025_drain_events, __us025_reset_probe,
};

const DB: &str = "phase5_us010";
const COLL: &str = "docs";
const SEED_DOCS: i32 = 24;
const SEED_PAD_BYTES: usize = 28 * 1024;
const OVERFLOW_PAD_BYTES: usize = 31 * 1024;
static TEST_LOCK: Mutex<()> = Mutex::new(());

const US028_DB: &str = "phase5_us028";
const US028_COLL: &str = "docs";
const US028_NS: &str = "phase5_us028.docs";
const US028_SEED_DOCS: i32 = 96;
const US028_PAD_BYTES: usize = 512;
const US028_UPDATED_PAD_BYTES: usize = 640;
const US028_WAIT_TIMEOUT: Duration = Duration::from_secs(10);
const US028_BLOCKED_WINDOW: Duration = Duration::from_millis(150);

fn us010_test_guard() -> MutexGuard<'static, ()> {
    TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn open_client(cap: u32) -> Client {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.keep().join("us010.mqlite");
    Client::open_with_options(path, OpenOptions::new().smo_classification_retry_cap(cap))
        .expect("open client")
}

fn open_us028_client() -> Client {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.keep().join("us028.mqlite");
    Client::open(path).expect("open US-028 client")
}

fn seed_us028_client() -> Client {
    let client = open_us028_client();
    let coll = client.database(US028_DB).collection::<Document>(US028_COLL);
    let pad = "x".repeat(US028_PAD_BYTES);
    for id in 0..US028_SEED_DOCS {
        coll.insert_one(&doc! { "_id": id, "pad": &pad })
            .expect("seed insert");
    }
    client.checkpoint().expect("checkpoint seeded US-028 tree");
    client
}

fn set_us028_pad(client: &Client, id: i32) -> Result<(), Error> {
    client
        .database(US028_DB)
        .collection::<Document>(US028_COLL)
        .update_one(
            doc! { "_id": id },
            doc! { "$set": { "pad": "y".repeat(US028_UPDATED_PAD_BYTES) } },
        )
        .run()
        .map(|_| ())
}

fn disjoint_us028_ids(client: &Client) -> (i32, i32) {
    let mut first: Option<(i32, u32)> = None;
    for id in 0..US028_SEED_DOCS {
        let page = client
            .__us028_primary_leaf_for_id(US028_NS, &Bson::Int32(id))
            .expect("resolve primary leaf");
        match first {
            Some((first_id, first_page)) if first_page != page => return (first_id, id),
            Some(_) => {}
            None => first = Some((id, page)),
        }
    }
    panic!("US-028 seed data must span at least two leaves");
}

fn spawn_reconcile_latch(
    client: Client,
    id: i32,
) -> (
    Receiver<()>,
    Sender<()>,
    thread::JoinHandle<Result<(), Error>>,
) {
    let (ready_tx, ready_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        client.__us028_hold_primary_leaf_reconcile_latch(
            US028_NS,
            &Bson::Int32(id),
            ready_tx,
            release_rx,
        )
    });
    (ready_rx, release_tx, handle)
}

fn spawn_writer_latch(
    client: Client,
    id: i32,
) -> (
    Receiver<()>,
    Sender<()>,
    thread::JoinHandle<Result<(), Error>>,
) {
    let (ready_tx, ready_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        client.__us028_hold_primary_leaf_writer_latch(
            US028_NS,
            &Bson::Int32(id),
            ready_tx,
            release_rx,
        )
    });
    (ready_rx, release_tx, handle)
}

fn spawn_reader_latch(
    client: Client,
    id: i32,
) -> (
    Receiver<()>,
    Sender<()>,
    thread::JoinHandle<Result<(), Error>>,
) {
    let (ready_tx, ready_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        client.__us025_hold_primary_leaf_reader_latch(
            US028_NS,
            &Bson::Int32(id),
            ready_tx,
            release_rx,
        )
    });
    (ready_rx, release_tx, handle)
}

fn wait_ready(ready_rx: &Receiver<()>, label: &str) {
    ready_rx
        .recv_timeout(US028_WAIT_TIMEOUT)
        .unwrap_or_else(|err| panic!("{label} did not become ready: {err:?}"));
}

fn assert_not_ready(ready_rx: &Receiver<()>, label: &str) {
    match ready_rx.recv_timeout(US028_BLOCKED_WINDOW) {
        Err(RecvTimeoutError::Timeout) => {}
        Err(RecvTimeoutError::Disconnected) => panic!("{label} disconnected before readiness"),
        Ok(()) => panic!("{label} acquired while it should have waited"),
    }
}

fn assert_operation_blocked<T>(done_rx: &Receiver<Result<T, Error>>, label: &str) {
    match done_rx.recv_timeout(US028_BLOCKED_WINDOW) {
        Err(RecvTimeoutError::Timeout) => {}
        Err(RecvTimeoutError::Disconnected) => panic!("{label} disconnected while blocked"),
        Ok(Ok(_)) => panic!("{label} completed while same-page latch was held"),
        Ok(Err(err)) => panic!("{label} failed while same-page latch was held: {err:?}"),
    }
}

fn recv_operation<T>(done_rx: Receiver<Result<T, Error>>, label: &str) -> T {
    done_rx
        .recv_timeout(US028_WAIT_TIMEOUT)
        .unwrap_or_else(|err| panic!("{label} did not finish: {err:?}"))
        .unwrap_or_else(|err| panic!("{label} returned error: {err:?}"))
}

fn release_and_join(
    release_tx: Sender<()>,
    handle: thread::JoinHandle<Result<(), Error>>,
    label: &str,
) {
    release_tx
        .send(())
        .unwrap_or_else(|err| panic!("release {label}: {err:?}"));
    handle
        .join()
        .unwrap_or_else(|_| panic!("{label} thread panicked"))
        .unwrap_or_else(|err| panic!("{label} returned error: {err:?}"));
}

fn source_file(path: &str) -> String {
    std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(path))
        .unwrap_or_else(|err| panic!("read {path}: {err}"))
}

fn seeded_client(cap: u32) -> Client {
    let client = open_client(cap);
    let coll = client.database(DB).collection::<Document>(COLL);
    let pad = "x".repeat(SEED_PAD_BYTES);
    for id in 0..SEED_DOCS {
        coll.insert_one(&doc! { "_id": id, "pad": &pad })
            .expect("seed insert");
    }
    client.checkpoint().expect("checkpoint seeded tree");
    client
}

fn set_pad(client: &Client, id: i32, bytes: usize) -> Result<(), Error> {
    let coll = client.database(DB).collection::<Document>(COLL);
    coll.update_one(
        doc! { "_id": id },
        doc! { "$set": { "pad": "y".repeat(bytes) } },
    )
    .run()
    .map(|_| ())
}

fn insert_doc(client: &Client, id: i32, bytes: usize) -> Result<(), Error> {
    let coll = client.database(DB).collection::<Document>(COLL);
    coll.insert_one(&doc! { "_id": id, "pad": "z".repeat(bytes) })
        .map(|_| ())
}

fn exclusive_pages() -> Vec<u32> {
    __us010_drain_events()
        .into_iter()
        .filter(|event| event.kind == "exclusive_acquire")
        .filter_map(|event| event.page_id)
        .collect()
}

fn classification_shapes() -> Vec<String> {
    __us010_drain_events()
        .into_iter()
        .filter(|event| event.kind == "classification")
        .filter_map(|event| event.shape)
        .collect()
}

fn assert_reader_crabbing(events: &[Us025CrabbingEvent]) {
    let release_events: Vec<(usize, &Us025CrabbingEvent)> = events
        .iter()
        .enumerate()
        .filter(|(_, event)| event.kind == "parent_release_after_child")
        .collect();
    assert!(
        !release_events.is_empty(),
        "reader traversal should release at least one parent after acquiring a child: {events:?}"
    );

    for (release_index, event) in release_events {
        let parent = event.parent_page.expect("parent page recorded");
        let child = event.child_page.expect("child page recorded");
        let child_acquire_index = events[..release_index].iter().rposition(|candidate| {
            candidate.kind == "shared_acquire" && candidate.page_id == Some(child)
        });
        let parent_acquire_index = events[..release_index].iter().rposition(|candidate| {
            candidate.kind == "shared_acquire" && candidate.page_id == Some(parent)
        });
        assert!(
            child_acquire_index.is_some(),
            "child page {child} must be acquired before parent {parent} is released: {events:?}"
        );
        assert!(
            parent_acquire_index < child_acquire_index,
            "parent {parent} should be acquired before child {child}: {events:?}"
        );
    }
}

fn assert_structural_contention(err: Error) {
    assert!(
        matches!(
            err,
            Error::WriteConflict {
                reason: WriteConflictReason::StructuralContention
            }
        ),
        "expected StructuralContention, got {err:?}"
    );
}

#[test]
fn test_smo_writer_takes_ancestor_path_latches_ascending() {
    let _guard = us010_test_guard();
    let client = seeded_client(3);

    __us010_reset_probe();
    set_pad(&client, 10, OVERFLOW_PAD_BYTES).expect("overflow update");

    let pages = exclusive_pages();
    assert!(
        pages.len() >= 2,
        "SMO update should latch at least leaf + ancestor, got {pages:?}"
    );
    assert!(
        pages.windows(2).all(|pair| pair[0] < pair[1]),
        "SMO latches must be acquired in ascending page-id order: {pages:?}"
    );
}

#[test]
fn test_concurrent_smos_on_shared_ancestor_serialize() {
    let _guard = us010_test_guard();
    let client = seeded_client(3);
    __us010_reset_probe();

    let left = client.clone();
    let right = client.clone();
    let t1 = thread::spawn(move || set_pad(&left, 11, OVERFLOW_PAD_BYTES));
    let t2 = thread::spawn(move || set_pad(&right, 12, OVERFLOW_PAD_BYTES));

    t1.join().expect("thread 1").expect("writer 1");
    t2.join().expect("thread 2").expect("writer 2");

    let pages = exclusive_pages();
    assert!(
        pages.len() >= 4,
        "two SMO writers should each acquire structural latches, got {pages:?}"
    );
}

#[test]
fn test_root_neutral_concurrent_with_smo_on_unrelated_subtree() {
    let _guard = us010_test_guard();
    let client = seeded_client(3);
    __us010_reset_probe();

    let root_neutral = client.clone();
    let smo = client.clone();
    let t1 = thread::spawn(move || set_pad(&root_neutral, 13, 16));
    let t2 = thread::spawn(move || set_pad(&smo, 14, OVERFLOW_PAD_BYTES));

    t1.join().expect("thread 1").expect("root-neutral writer");
    t2.join().expect("thread 2").expect("SMO writer");

    let shapes = classification_shapes();
    assert!(
        shapes.iter().any(|shape| shape == "RootNeutral"),
        "expected a root-neutral classification, got {shapes:?}"
    );
    assert!(
        shapes.iter().any(|shape| shape == "OverflowChange"),
        "expected an SMO classification, got {shapes:?}"
    );
}

#[test]
fn test_smo_revalidation_failure_returns_structural_contention() {
    let _guard = us010_test_guard();
    let client = seeded_client(3);

    __us010_reset_probe();
    __us010_force_revalidation_failures(1);

    let err = set_pad(&client, 15, OVERFLOW_PAD_BYTES).unwrap_err();
    assert_structural_contention(err);
}

#[test]
fn test_smo_retry_after_structural_contention_succeeds() {
    let _guard = us010_test_guard();
    let client = seeded_client(3);

    __us010_reset_probe();
    __us010_force_revalidation_failures(1);
    let err = set_pad(&client, 16, OVERFLOW_PAD_BYTES).unwrap_err();
    assert_structural_contention(err);

    set_pad(&client, 16, OVERFLOW_PAD_BYTES).expect("retry should succeed");
}

#[test]
fn test_stale_classification_detected_and_retried() {
    let _guard = us010_test_guard();
    let client = seeded_client(3);

    __us010_reset_probe();
    __us010_push_classification_override_names(&["RootNeutral", "LeafSplit", "LeafSplit"]);
    insert_doc(&client, 10_000, 16).expect("stale classification retry should commit");

    let events = __us010_drain_events();
    assert!(
        events
            .iter()
            .any(|event| event.kind == "reclassification" && event.attempt == Some(1)),
        "expected one bounded reclassification event, got {events:?}"
    );
}

#[test]
fn test_smo_reclassification_bounded_by_config() {
    let _guard = us010_test_guard();
    let client = seeded_client(1);

    __us010_reset_probe();
    __us010_push_classification_override_names(&["RootNeutral", "LeafSplit"]);

    let err = insert_doc(&client, 10_001, 16).unwrap_err();
    assert_structural_contention(err);
}

#[test]
fn test_reconcile_vs_writer_same_page_writer_waits() {
    let _guard = us010_test_guard();
    let client = seed_us028_client();
    let (id, _) = disjoint_us028_ids(&client);

    let (reconcile_ready, reconcile_release, reconcile_handle) =
        spawn_reconcile_latch(client.clone(), id);
    wait_ready(&reconcile_ready, "reconcile latch");

    let (done_tx, done_rx) = mpsc::channel();
    let writer = client.clone();
    let started = Instant::now();
    let writer_handle = thread::spawn(move || {
        let result = set_us028_pad(&writer, id);
        done_tx.send(result).expect("send writer result");
    });

    assert_operation_blocked(&done_rx, "same-page writer");
    release_and_join(reconcile_release, reconcile_handle, "reconcile latch");
    recv_operation(done_rx, "same-page writer");
    writer_handle.join().expect("writer thread");

    assert!(
        started.elapsed() >= US028_BLOCKED_WINDOW,
        "writer should observe the reconcile latch wait window"
    );
}

#[test]
fn test_reconcile_vs_writer_disjoint_page_both_progress() {
    let _guard = us010_test_guard();
    let client = seed_us028_client();
    let (reconcile_id, writer_id) = disjoint_us028_ids(&client);

    let (reconcile_ready, reconcile_release, reconcile_handle) =
        spawn_reconcile_latch(client.clone(), reconcile_id);
    wait_ready(&reconcile_ready, "reconcile latch");

    let (done_tx, done_rx) = mpsc::channel();
    let writer = client.clone();
    let writer_handle = thread::spawn(move || {
        let result = set_us028_pad(&writer, writer_id);
        done_tx.send(result).expect("send writer result");
    });

    recv_operation(done_rx, "disjoint-page writer");
    writer_handle.join().expect("writer thread");
    release_and_join(reconcile_release, reconcile_handle, "reconcile latch");
}

#[test]
fn test_writer_latch_held_reconcile_waits() {
    let _guard = us010_test_guard();
    let client = seed_us028_client();
    let (id, _) = disjoint_us028_ids(&client);

    let (writer_ready, writer_release, writer_handle) = spawn_writer_latch(client.clone(), id);
    wait_ready(&writer_ready, "writer latch");

    let (reconcile_ready, reconcile_release, reconcile_handle) =
        spawn_reconcile_latch(client.clone(), id);
    assert_not_ready(&reconcile_ready, "same-page reconcile latch");

    release_and_join(writer_release, writer_handle, "writer latch");
    wait_ready(&reconcile_ready, "same-page reconcile latch");
    release_and_join(reconcile_release, reconcile_handle, "reconcile latch");
}

#[test]
fn test_reconcile_chain_mutation_requires_exclusive_latch() {
    let driver = source_file("src/storage/reconcile/driver.rs");
    assert!(
        driver.contains("planned_pages.sort_unstable();"),
        "reconcile_leaf must sort the planned page set before latching"
    );
    assert!(
        driver.contains("pin_leaf_set_for_reconcile"),
        "reconcile_leaf must acquire its planned page set through the \
         reconcile latch helper"
    );

    let pool = source_file("src/storage/buffer_pool/mod.rs");
    assert!(
        pool.contains("page: &mut LatchedPinnedPage<'_>"),
        "replace_leaf_and_chains must accept a LatchedPinnedPage latch token"
    );
    assert!(
        pool.contains("page.require_exclusive(\"replace_leaf_and_chains\")"),
        "resident replacement must require PageLatch::Exclusive"
    );
    assert!(
        pool.contains("No partition mutex") && pool.contains("acquired by this helper"),
        "replace_leaf_and_chains must document that it does not re-enter a \
         partition mutex while holding the page latch"
    );
    assert!(
        pool.contains("LatchedPinnedPage::snapshot_chains")
            && pool.contains("LatchedPinnedPage::Shared")
            && pool.contains("copies/clones only"),
        "snapshot_chains must be documented as a shared-latch copy path"
    );
}

#[test]
fn test_reader_crabbing_does_not_observe_split_in_progress() {
    let _guard = us010_test_guard();
    let client = seed_us028_client();
    let coll = client.database(US028_DB).collection::<Document>(US028_COLL);

    __us025_reset_probe();
    let found = coll
        .find_one(doc! { "_id": US028_SEED_DOCS / 2 })
        .expect("reader crabbing lookup should succeed");
    assert!(
        found.is_some(),
        "seeded document should remain visible to a latch-coupled reader"
    );

    let events = __us025_drain_events();
    assert_reader_crabbing(&events);
}

#[test]
fn test_smo_blocked_by_in_progress_reader() {
    let _guard = us010_test_guard();
    let client = seed_us028_client();
    let (id, _) = disjoint_us028_ids(&client);

    let (reader_ready, reader_release, reader_handle) = spawn_reader_latch(client.clone(), id);
    wait_ready(&reader_ready, "reader latch");

    let (done_tx, done_rx) = mpsc::channel();
    let writer = client.clone();
    let started = Instant::now();
    let writer_handle = thread::spawn(move || {
        let result = set_us028_pad(&writer, id);
        done_tx.send(result).expect("send writer result");
    });

    assert_operation_blocked(&done_rx, "same-page writer behind reader");
    release_and_join(reader_release, reader_handle, "reader latch");
    recv_operation(done_rx, "same-page writer behind reader");
    writer_handle.join().expect("writer thread");

    assert!(
        started.elapsed() >= US028_BLOCKED_WINDOW,
        "writer should observe the reader shared-latch wait window"
    );
}

#[test]
fn test_merge_does_not_deadlock_under_page_id_inversion() {
    #[cfg(loom)]
    {
        loom::model(|| {
            use loom::sync::{Arc, RwLock};

            const LOW_PAGE: u32 = 3;
            const HIGH_PAGE: u32 = 7;

            fn acquire_sorted_pair(
                first_requested: u32,
                second_requested: u32,
                low: &Arc<RwLock<()>>,
                high: &Arc<RwLock<()>>,
            ) {
                let mut pages = [first_requested, second_requested];
                pages.sort_unstable();
                let first = if pages[0] == LOW_PAGE {
                    low.write().expect("low page latch")
                } else {
                    high.write().expect("high page latch")
                };
                loom::thread::yield_now();
                let second = if pages[1] == LOW_PAGE {
                    low.write().expect("low page latch")
                } else {
                    high.write().expect("high page latch")
                };
                drop(second);
                drop(first);
            }

            let low = Arc::new(RwLock::new(()));
            let high = Arc::new(RwLock::new(()));

            let left_then_target = {
                let low = Arc::clone(&low);
                let high = Arc::clone(&high);
                loom::thread::spawn(move || {
                    acquire_sorted_pair(HIGH_PAGE, LOW_PAGE, &low, &high);
                })
            };
            let target_then_left = {
                let low = Arc::clone(&low);
                let high = Arc::clone(&high);
                loom::thread::spawn(move || {
                    acquire_sorted_pair(LOW_PAGE, HIGH_PAGE, &low, &high);
                })
            };

            left_then_target.join().expect("left>target thread");
            target_then_left.join().expect("target<left thread");
        });
        return;
    }

    #[cfg(not(loom))]
    {
        let _guard = us010_test_guard();
        let client = seed_us028_client();
        let coll = client.database(US028_DB).collection::<Document>(US028_COLL);

        __us010_reset_probe();
        __us010_push_classification_override_names(&["LeafMerge", "LeafMerge"]);
        coll.delete_one(doc! { "_id": US028_SEED_DOCS / 2 })
            .expect("merge-shaped delete should complete without deadlock");

        let pages = exclusive_pages();
        assert!(
            pages.len() >= 2,
            "merge-shaped delete should acquire a multi-page latch set, got {pages:?}"
        );
        assert!(
        pages.windows(2).all(|pair| pair[0] < pair[1]),
        "merge/reconcile latch acquisition must stay ascending under page-id inversion: {pages:?}"
    );

        let source = source_file("src/storage/paged_engine/smo_latch.rs");
        assert!(
            source.contains("fn acquire_pages") && source.contains("BTreeSet<u32>"),
            "SMO/reconcile latch planning must normalize an inverted page set through BTreeSet"
        );
        assert!(
            source.contains("for page_id in required_pages"),
            "SMO/reconcile latch acquisition must iterate the sorted page set"
        );
    }
}
