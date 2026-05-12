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

use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use bson::{Bson, Document};
use mqlite::error::{Error, WriteConflictReason};
use mqlite::mvcc::{ReadView, Ts};
use mqlite::{
    __us016_drain_latch_samples, __us016_install_range_scan_iteration_pause, __us016_reset_probe,
    __us019_page_latch_upgrade_race_counts, doc, Client, IndexModel, IndexOptions,
    WriteBodyEntryHookGuard,
};
use serial_test::serial;

// Several probes in this target are process-wide test hooks. Serialize the
// integration tests while preserving the writer/reader concurrency inside
// each test body.

const DB: &str = "phase5_us008";
const COLL: &str = "docs";
const US016_DB: &str = "phase5_us016";
const US016_COLL: &str = "docs";
const US016_NS: &str = "phase5_us016.docs";
const US016_DOCS: i32 = 10_000;
const US016_WAIT_TIMEOUT: Duration = Duration::from_secs(10);
const US016_FAST_WRITER_WINDOW: Duration = Duration::from_millis(150);
const US016_MAX_LATCH_HOLD: Duration = Duration::from_millis(10);
const UNIQUE_DB: &str = "phase5_us011";
const UNIQUE_COLL: &str = "docs";
const UNIQUE_NS: &str = "phase5_us011.docs";
const UNIQUE_INDEX: &str = "email_1";
const US019_DB: &str = "phase5_us019";
const US019_COLL: &str = "docs";
const US019_NS: &str = "phase5_us019.docs";
const US019_SEED_DOCS: i32 = 96;
const US019_SNAPSHOT_WRITERS: i32 = 1_000;
const US019_PAD_BYTES: usize = 512;
const US019_WAIT_TIMEOUT: Duration = Duration::from_secs(10);
const US019_BODY_HOLD: Duration = Duration::from_millis(250);
const US019_OVERLAP_NUMERATOR: u128 = 13;
const US019_OVERLAP_DENOMINATOR: u128 = 10;
const US019_RETRY_MARK: i32 = 909;

fn us019_doc(id: i32, mark: i32) -> Document {
    doc! {
        "_id": id,
        "mark": mark,
        "pad": "x".repeat(US019_PAD_BYTES),
    }
}

fn open_us019_collection(name: &str, count: i32) -> (tempfile::TempDir, Client) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(name);
    let client = Client::open(&path).unwrap();
    let db = client.database(US019_DB);
    db.create_collection(US019_COLL).unwrap();
    let coll = db.collection::<Document>(US019_COLL);
    for id in 0..count {
        coll.insert_one(&us019_doc(id, 0)).unwrap();
    }
    client.checkpoint().unwrap();
    (dir, client)
}

fn us019_leaf_for_id(client: &Client, id: i32) -> u32 {
    client
        .__us028_primary_leaf_for_id(US019_NS, &Bson::Int32(id))
        .unwrap()
}

fn disjoint_us019_ids(client: &Client) -> (i32, i32) {
    let mut first: Option<(i32, u32)> = None;
    for id in 0..US019_SEED_DOCS {
        let page = us019_leaf_for_id(client, id);
        match first {
            Some((first_id, first_page)) if first_page != page => return (first_id, id),
            Some(_) => {}
            None => first = Some((id, page)),
        }
    }
    panic!("US-019 seed data must span at least two leaves");
}

fn same_page_us019_ids(client: &Client) -> (i32, i32) {
    let mut seen: Vec<(i32, u32)> = Vec::new();
    for id in 0..US019_SEED_DOCS {
        let page = us019_leaf_for_id(client, id);
        if let Some((existing_id, _)) = seen
            .iter()
            .find(|(_, existing_page)| *existing_page == page)
        {
            return (*existing_id, id);
        }
        seen.push((id, page));
    }
    panic!("US-019 seed data must contain two keys on one leaf");
}

fn set_us019_mark(client: &Client, id: i32, mark: i32) -> mqlite::Result<()> {
    let result = client
        .database(US019_DB)
        .collection::<Document>(US019_COLL)
        .update_one(doc! { "_id": id }, doc! { "$set": { "mark": mark } })
        .run()?;
    assert_eq!(result.matched_count, 1);
    assert_eq!(result.modified_count, 1);
    Ok(())
}

fn insert_us019_mark(client: &Client, id: i32, mark: i32) -> mqlite::Result<()> {
    client
        .database(US019_DB)
        .collection::<Document>(US019_COLL)
        .insert_one(&us019_doc(id, mark))
        .map(|_| ())
}

fn wait_us019_body(hook: &WriteBodyEntryHookGuard, label: &str) {
    hook.wait_until_entered_timeout(US019_WAIT_TIMEOUT)
        .unwrap_or_else(|err| panic!("{label} did not enter write body: {err:?}"));
}

fn release_us019_body(mut hook: WriteBodyEntryHookGuard, label: &str) {
    hook.release()
        .unwrap_or_else(|err| panic!("release {label}: {err:?}"));
}

fn scaled_overlap_limit(baseline: Duration) -> Duration {
    let nanos = baseline.as_nanos() * US019_OVERLAP_NUMERATOR / US019_OVERLAP_DENOMINATOR;
    let clamped = nanos.min(u128::from(u64::MAX));
    Duration::from_nanos(clamped as u64)
}

fn timed_single_hooked_update(client: Client, id: i32, mark: i32) -> Duration {
    let hook = client.__install_write_body_entry_hook(US019_NS);
    let writer = thread::spawn(move || set_us019_mark(&client, id, mark));
    let started = Instant::now();
    wait_us019_body(&hook, "single writer");
    thread::sleep(US019_BODY_HOLD);
    release_us019_body(hook, "single writer");
    writer
        .join()
        .expect("single writer thread panicked")
        .expect("single writer update");
    started.elapsed()
}

fn run_two_hooked_updates(client: Client, ids: (i32, i32), marks: (i32, i32)) -> Duration {
    let hook_a = client.__install_write_body_entry_hook(US019_NS);
    let hook_b = client.__install_write_body_entry_hook(US019_NS);
    let left = client.clone();
    let right = client;
    let started = Instant::now();
    let writer_a = thread::spawn(move || set_us019_mark(&left, ids.0, marks.0));
    let writer_b = thread::spawn(move || set_us019_mark(&right, ids.1, marks.1));

    wait_us019_body(&hook_a, "writer A");
    wait_us019_body(&hook_b, "writer B");
    thread::sleep(US019_BODY_HOLD);
    release_us019_body(hook_a, "writer A");
    release_us019_body(hook_b, "writer B");

    writer_a
        .join()
        .expect("writer A thread panicked")
        .expect("writer A update");
    writer_b
        .join()
        .expect("writer B thread panicked")
        .expect("writer B update");
    started.elapsed()
}

fn run_two_same_key_inserts(
    client: Client,
    key: i32,
    marks: (i32, i32),
) -> Vec<mqlite::Result<()>> {
    let hook_a = client.__install_write_body_entry_hook(US019_NS);
    let hook_b = client.__install_write_body_entry_hook(US019_NS);
    let left = client.clone();
    let right = client;
    let writer_a = thread::spawn(move || insert_us019_mark(&left, key, marks.0));
    let writer_b = thread::spawn(move || insert_us019_mark(&right, key, marks.1));

    wait_us019_body(&hook_a, "same-key writer A");
    wait_us019_body(&hook_b, "same-key writer B");
    release_us019_body(hook_a, "same-key writer A");
    release_us019_body(hook_b, "same-key writer B");

    vec![
        writer_a.join().expect("same-key writer A thread panicked"),
        writer_b.join().expect("same-key writer B thread panicked"),
    ]
}

fn assert_one_same_key_conflict(results: &[mqlite::Result<()>]) {
    let winners = results.iter().filter(|result| result.is_ok()).count();
    let conflicts = results
        .iter()
        .filter(|result| {
            matches!(
                result,
                Err(Error::WriteConflict {
                    reason: WriteConflictReason::SameKeyConflict { .. }
                })
            )
        })
        .count();
    assert_eq!(winners, 1, "exactly one same-key writer should win");
    assert_eq!(
        conflicts, 1,
        "loser must fail with SameKeyConflict: {results:?}"
    );
}

fn unique_email_model() -> IndexModel {
    IndexModel::builder()
        .keys(doc! { "email": 1 })
        .options(IndexOptions::new().unique(true))
        .build()
}

fn open_unique_collection(name: &str) -> (tempfile::TempDir, Client) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(name);
    let client = Client::open(&path).unwrap();
    let db = client.database(UNIQUE_DB);
    db.create_collection(UNIQUE_COLL).unwrap();
    db.collection::<Document>(UNIQUE_COLL)
        .create_index(unique_email_model())
        .unwrap();
    (dir, client)
}

fn open_us016_collection(name: &str, count: i32) -> (tempfile::TempDir, Client) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(name);
    let client = Client::open(&path).unwrap();
    let coll = client.database(US016_DB).collection::<Document>(US016_COLL);
    let docs: Vec<Document> = (0..count)
        .map(|id| doc! { "_id": id, "pad": "reader-latch-scope" })
        .collect();
    coll.insert_many(&docs).run().unwrap();
    (dir, client)
}

fn spawn_us016_range_scan(client: Client) -> thread::JoinHandle<mqlite::Result<usize>> {
    thread::spawn(move || {
        let coll = client.database(US016_DB).collection::<Document>(US016_COLL);
        coll.find(doc! {})
            .limit(0)
            .run()?
            .try_fold(0usize, |count, doc| doc.map(|_| count + 1))
    })
}

fn spawn_us016_writer_latch(
    client: Client,
    id: i32,
) -> (
    Receiver<()>,
    Sender<()>,
    thread::JoinHandle<mqlite::Result<()>>,
) {
    let (ready_tx, ready_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        client.__us028_hold_primary_leaf_writer_latch(
            US016_NS,
            &Bson::Int32(id),
            ready_tx,
            release_rx,
        )
    });
    (ready_rx, release_tx, handle)
}

fn wait_us016_ready(ready_rx: &Receiver<()>, label: &str) {
    ready_rx
        .recv_timeout(US016_WAIT_TIMEOUT)
        .unwrap_or_else(|err| panic!("{label} did not become ready: {err:?}"));
}

fn assert_us016_latch_samples_short() {
    let samples = __us016_drain_latch_samples();
    assert!(
        samples.iter().any(|sample| sample.level == 0),
        "reader scan must record at least one leaf shared-latch hold"
    );
    let slow = samples
        .iter()
        .filter(|sample| sample.hold_duration >= US016_MAX_LATCH_HOLD)
        .collect::<Vec<_>>();
    assert!(
        slow.is_empty(),
        "reader shared-latch holds must be bounded by copy/snapshot work, not scan duration: {slow:?}"
    );
}

fn assert_us016_source_contract() {
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let read_leaf = std::fs::read_to_string(manifest_dir.join("src/storage/btree_store.rs"))
        .expect("read btree_store.rs");
    assert!(
        read_leaf.contains("fn snapshot_leaf")
            && read_leaf.contains("p.data_snapshot()")
            && read_leaf.contains("snap_chains(p)?")
            && read_leaf.contains("LeafPageImage::shared(data)?")
            && read_leaf.contains("pin_shared_for_read(page, PageSize::Large32k)?")
            && read_leaf.contains("self.snapshot_leaf(&guard, |p| p.snapshot_chains(None))"),
        "BufferPoolPageStore::read_leaf must copy bytes, clone chain snapshots, then drop the shared latch"
    );

    let scan = std::fs::read_to_string(manifest_dir.join("src/storage/btree/scan.rs"))
        .expect("read scan.rs");
    assert!(
        scan.contains("let leaf = self.store.read_leaf_guarded(page, &guard)?;")
            && scan.contains("drop(guard);")
            && scan.contains("pause_before_iteration()?;"),
        "reader scan helpers must drop the shared latch before range iteration can pause"
    );
}

fn assert_one_unique_conflict(a: mqlite::Result<()>, b: mqlite::Result<()>) {
    let results = [a, b];
    let winners = results.iter().filter(|result| result.is_ok()).count();
    let conflicts = results
        .iter()
        .filter(|result| {
            matches!(
                result,
                Err(Error::WriteConflict {
                    reason: WriteConflictReason::UniqueConflict { .. }
                })
            )
        })
        .count();
    assert_eq!(
        winners, 1,
        "exactly one unique insert should win: {results:?}"
    );
    assert_eq!(
        conflicts, 1,
        "loser must fail with UniqueConflict, not DuplicateKey or success: {results:?}"
    );
}

#[test]
#[serial(mwmr_namespace_overlap_hooks)]
fn test_disjoint_page_writers_overlap() {
    let (_dir, client) = open_us019_collection("us019-disjoint-overlap.mqlite", US019_SEED_DOCS);
    let ids = disjoint_us019_ids(&client);

    let baseline = timed_single_hooked_update(client.clone(), ids.0, 1);
    let concurrent = run_two_hooked_updates(client.clone(), ids, (2, 3));

    assert!(
        concurrent <= scaled_overlap_limit(baseline),
        "disjoint-page writers should overlap: baseline={baseline:?}, concurrent={concurrent:?}"
    );

    let coll = client.database(US019_DB).collection::<Document>(US019_COLL);
    assert_eq!(
        coll.find_one(doc! { "_id": ids.0 })
            .unwrap()
            .and_then(|doc| doc.get_i32("mark").ok()),
        Some(2)
    );
    assert_eq!(
        coll.find_one(doc! { "_id": ids.1 })
            .unwrap()
            .and_then(|doc| doc.get_i32("mark").ok()),
        Some(3)
    );
}

#[test]
#[serial(mwmr_namespace_overlap_hooks)]
fn test_same_page_writers_serialize_without_corruption() {
    let (_dir, client) = open_us019_collection("us019-same-page.mqlite", US019_SEED_DOCS);
    let ids = same_page_us019_ids(&client);

    run_two_hooked_updates(client.clone(), ids, (11, 12));

    let coll = client.database(US019_DB).collection::<Document>(US019_COLL);
    assert_eq!(
        coll.find_one(doc! { "_id": ids.0 })
            .unwrap()
            .and_then(|doc| doc.get_i32("mark").ok()),
        Some(11)
    );
    assert_eq!(
        coll.find_one(doc! { "_id": ids.1 })
            .unwrap()
            .and_then(|doc| doc.get_i32("mark").ok()),
        Some(12)
    );
}

#[test]
#[serial(mwmr_namespace_overlap_hooks)]
fn test_same_txn_multi_key_insert_across_different_leaves() {
    let (_dir, client) = open_us019_collection("us022-multi-key-insert.mqlite", US019_SEED_DOCS);
    let ids = disjoint_us019_ids(&client);
    assert_ne!(
        us019_leaf_for_id(&client, ids.0),
        us019_leaf_for_id(&client, ids.1),
        "fixture ids must begin on different resident leaves",
    );

    let coll = client.database(US019_DB).collection::<Document>(US019_COLL);
    coll.delete_one(doc! { "_id": ids.0 }).unwrap();
    coll.delete_one(doc! { "_id": ids.1 }).unwrap();

    client
        .__us022_insert_two_docs_one_txn(US019_NS, us019_doc(ids.0, 41), us019_doc(ids.1, 42))
        .expect("same txn insert across different leaves");

    assert_eq!(
        coll.find_one(doc! { "_id": ids.0 })
            .unwrap()
            .and_then(|doc| doc.get_i32("mark").ok()),
        Some(41)
    );
    assert_eq!(
        coll.find_one(doc! { "_id": ids.1 })
            .unwrap()
            .and_then(|doc| doc.get_i32("mark").ok()),
        Some(42)
    );
}

#[test]
#[serial(mwmr_namespace_overlap_hooks)]
fn test_same_key_writers_yield_write_conflict() {
    let (_dir, client) = open_us019_collection("us019-same-key-conflict.mqlite", 0);

    let results = run_two_same_key_inserts(client.clone(), 1, (21, 22));

    assert_one_same_key_conflict(&results);
    assert_eq!(
        client
            .database(US019_DB)
            .collection::<Document>(US019_COLL)
            .count_documents(doc! { "_id": 1 })
            .unwrap(),
        1
    );
}

#[test]
#[serial(mwmr_namespace_overlap_hooks)]
fn test_same_key_retry_after_conflict_succeeds() {
    let (_dir, client) = open_us019_collection("us019-same-key-retry.mqlite", 0);

    let results = run_two_same_key_inserts(client.clone(), 2, (31, 32));
    assert_one_same_key_conflict(&results);

    set_us019_mark(&client, 2, US019_RETRY_MARK).expect("retry after conflict");
    let doc = client
        .database(US019_DB)
        .collection::<Document>(US019_COLL)
        .find_one(doc! { "_id": 2 })
        .unwrap()
        .expect("retried document exists");
    assert_eq!(doc.get_i32("mark").ok(), Some(US019_RETRY_MARK));
}

#[test]
#[serial(mwmr_namespace_overlap_hooks)]
fn test_upgrade_race_returns_upgrade_race() {
    let (winners, upgrade_races) =
        __us019_page_latch_upgrade_race_counts().expect("page latch upgrade race");

    assert_eq!(winners, upgrade_races);
    assert!(winners > 0, "upgrade race probe must execute attempts");
}

#[test]
#[serial(mwmr_namespace_overlap_hooks)]
fn test_long_reader_plus_many_writers_sees_stable_snapshot() {
    let (_dir, client) = open_us019_collection("us019-long-reader.mqlite", US019_SEED_DOCS);
    let coll = client.database(US019_DB).collection::<Document>(US019_COLL);
    let cursor = coll.find(doc! {}).sort(doc! { "_id": 1 }).run().unwrap();

    let writer_client = client;
    let writer = thread::spawn(move || {
        let coll = writer_client
            .database(US019_DB)
            .collection::<Document>(US019_COLL);
        for offset in 0..US019_SNAPSHOT_WRITERS {
            let id = US019_SEED_DOCS + offset;
            coll.insert_one(&us019_doc(id, 1)).unwrap();
        }
    });

    writer.join().expect("many-writer thread panicked");
    let snapshot: Vec<Document> = cursor.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(snapshot.len(), US019_SEED_DOCS as usize);
    assert!(
        snapshot
            .iter()
            .all(|doc| doc.get_i32("mark").ok() == Some(0)),
        "reader cursor must retain the pre-writer snapshot"
    );
    assert_eq!(
        coll.count_documents(doc! {}).unwrap(),
        (US019_SEED_DOCS + US019_SNAPSHOT_WRITERS) as u64
    );
}

#[test]
#[serial(mwmr_namespace_overlap_hooks)]
fn test_reader_does_not_block_writers() {
    let (_dir, client) = open_us016_collection("us019-reader-writer-overlap.mqlite", US016_DOCS);
    __us016_reset_probe();

    let (scan_ready_tx, scan_ready_rx) = mpsc::channel();
    let (scan_release_tx, scan_release_rx) = mpsc::channel();
    let _pause = __us016_install_range_scan_iteration_pause(scan_ready_tx, scan_release_rx);
    let reader = spawn_us016_range_scan(client.clone());

    wait_us016_ready(&scan_ready_rx, "US-019 range scan pause");

    let writer_client = client;
    let started = Instant::now();
    let writer = thread::spawn(move || {
        let coll = writer_client
            .database(US016_DB)
            .collection::<Document>(US016_COLL);
        coll.update_one(doc! { "_id": 0i32 }, doc! { "$set": { "updated": true } })
            .run()
            .map(|_| ())
    });

    writer
        .join()
        .expect("US-019 writer thread panicked")
        .expect("US-019 writer update");
    assert!(
        started.elapsed() < US016_FAST_WRITER_WINDOW,
        "writer must commit while reader is paused between page-copy and iteration"
    );

    scan_release_tx.send(()).expect("release paused scan");
    let count = reader
        .join()
        .expect("US-019 reader thread panicked")
        .expect("US-019 reader scan");
    assert_eq!(count, US016_DOCS as usize);
}

#[test]
#[serial(mwmr_namespace_overlap_hooks)]
fn test_read_view_horizon_pins_old_versions() {
    let (_dir, client) = open_us019_collection("us019-horizon.mqlite", 1);
    let coll = client.database(US019_DB).collection::<Document>(US019_COLL);
    let cursor = coll.find(doc! { "_id": 0 }).run().unwrap();
    let (physical_ms, logical) = client.__published_visible_ts();
    let read_ts = Ts {
        physical_ms,
        logical,
    };
    let registry = client.__read_view_registry().expect("read-view registry");
    let view = ReadView::open(Arc::clone(&registry), read_ts, 19);

    set_us019_mark(&client, 0, 41).expect("first update");
    set_us019_mark(&client, 0, 42).expect("second update");
    client.checkpoint().expect("checkpoint while reader pins");

    assert_eq!(
        registry.oldest_required_ts(),
        read_ts,
        "open read view must pin the oldest-required horizon"
    );
    let snapshot: Vec<Document> = cursor.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0].get_i32("mark").ok(), Some(0));
    assert_eq!(
        coll.find_one(doc! { "_id": 0 })
            .unwrap()
            .and_then(|doc| doc.get_i32("mark").ok()),
        Some(42)
    );

    drop(view);
}

#[test]
#[serial(mwmr_namespace_overlap_hooks)]
fn test_crud_on_existing_leaf_does_not_write_structural_page_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("us008.mqlite");
    let client = Client::open(&path).unwrap();
    let coll = client.database(DB).collection::<Document>(COLL);

    coll.insert_one(&doc! { "_id": 1i32, "name": "seed" })
        .unwrap();

    client.__us008_reset_structural_page_observations();
    coll.insert_one(&doc! { "_id": 2i32, "name": "root-neutral" })
        .unwrap();

    assert_eq!(
        client.__us008_committed_structural_leaf_bytes(),
        0,
        "root-neutral CRUD on an existing leaf must not commit logical row bytes through structural page batches",
    );
    assert_eq!(
        coll.find_one(doc! { "_id": 2i32 })
            .unwrap()
            .and_then(|doc| doc.get_str("name").ok().map(str::to_owned)),
        Some("root-neutral".to_owned()),
        "the inserted row remains visible via the resident delta map",
    );
}

#[test]
#[serial(mwmr_namespace_overlap_hooks)]
fn test_unique_conflict_two_concurrent_inserts_same_prefix() {
    let (_dir, client) = open_unique_collection("us011-two-inserts.mqlite");

    let result_a = client.__us011_install_pending_unique_email(
        UNIQUE_NS,
        UNIQUE_INDEX,
        Bson::Int32(1),
        "race@example.test",
        101,
    );
    let result_b = client.__us011_install_pending_unique_email(
        UNIQUE_NS,
        UNIQUE_INDEX,
        Bson::Int32(2),
        "race@example.test",
        102,
    );
    assert_one_unique_conflict(result_a, result_b);
}

#[test]
#[serial(mwmr_namespace_overlap_hooks)]
fn test_unique_conflict_detected_under_index_latch_not_stage_time() {
    let (_dir, client) = open_unique_collection("us011-install-arbiter.mqlite");

    let result_a = client.__us011_install_pending_unique_email(
        UNIQUE_NS,
        UNIQUE_INDEX,
        Bson::Int32(11),
        "latch@example.test",
        111,
    );
    let result_b = client.__us011_install_pending_unique_email(
        UNIQUE_NS,
        UNIQUE_INDEX,
        Bson::Int32(12),
        "latch@example.test",
        112,
    );
    assert_one_unique_conflict(result_a, result_b);
}

#[test]
#[serial(mwmr_namespace_overlap_hooks)]
fn test_unique_conflict_prefix_spans_sibling_leaf() {
    let (_dir, client) = open_unique_collection("us011-sibling-prefix.mqlite");

    let pages = client
        .__us011_unique_prefix_sibling_pages()
        .expect("sibling prefix probe");

    assert_eq!(
        pages,
        vec![41, 43],
        "unique-prefix range that crosses leaf bounds must include both siblings",
    );
}

#[test]
#[serial(mwmr_namespace_overlap_hooks)]
fn test_concurrent_unique_inserts_across_sibling_leaves_exactly_one_wins() {
    let (_dir, client) = open_unique_collection("us011-sibling-one-wins.mqlite");
    let pages = client
        .__us011_unique_prefix_sibling_pages()
        .expect("sibling prefix probe");
    assert_eq!(pages, vec![41, 43], "probe must exercise sibling planning");

    let result_a = client.__us011_install_pending_unique_email(
        UNIQUE_NS,
        UNIQUE_INDEX,
        Bson::Int32(21),
        "sibling-race@example.test",
        121,
    );
    let result_b = client.__us011_install_pending_unique_email(
        UNIQUE_NS,
        UNIQUE_INDEX,
        Bson::Int32(22),
        "sibling-race@example.test",
        122,
    );
    assert_one_unique_conflict(result_a, result_b);
}

#[test]
#[serial(mwmr_namespace_overlap_hooks)]
fn test_reader_latch_released_before_iteration() {
    let (_dir, client) = open_us016_collection("reader-latch-before-iteration.mqlite", 128);
    __us016_reset_probe();

    let (scan_ready_tx, scan_ready_rx) = mpsc::channel();
    let (scan_release_tx, scan_release_rx) = mpsc::channel();
    let _pause = __us016_install_range_scan_iteration_pause(scan_ready_tx, scan_release_rx);
    let reader = spawn_us016_range_scan(client.clone());

    wait_us016_ready(&scan_ready_rx, "range scan pause");

    let (writer_ready, writer_release, writer_latch) = spawn_us016_writer_latch(client, 0);
    wait_us016_ready(&writer_ready, "writer exclusive latch");
    writer_release.send(()).expect("release writer latch");
    writer_latch
        .join()
        .expect("writer latch thread panicked")
        .expect("writer latch");

    scan_release_tx.send(()).expect("release paused scan");
    let count = reader
        .join()
        .expect("reader thread panicked")
        .expect("reader scan");
    assert_eq!(count, 128);

    assert_us016_latch_samples_short();
    assert_us016_source_contract();
}

#[test]
#[serial(mwmr_namespace_overlap_hooks)]
fn test_long_range_scan_does_not_block_concurrent_writer() {
    let (_dir, client) = open_us016_collection("long-reader-writer-overlap.mqlite", US016_DOCS);
    __us016_reset_probe();

    let (scan_ready_tx, scan_ready_rx) = mpsc::channel();
    let (scan_release_tx, scan_release_rx) = mpsc::channel();
    let _pause = __us016_install_range_scan_iteration_pause(scan_ready_tx, scan_release_rx);
    let reader = spawn_us016_range_scan(client.clone());

    wait_us016_ready(&scan_ready_rx, "long range scan pause");

    let (done_tx, done_rx) = mpsc::channel();
    let writer_client = client;
    let started = Instant::now();
    let writer = thread::spawn(move || {
        let coll = writer_client
            .database(US016_DB)
            .collection::<Document>(US016_COLL);
        let result = coll
            .update_one(doc! { "_id": 0i32 }, doc! { "$set": { "updated": true } })
            .run()
            .map(|_| ());
        done_tx.send(result).expect("send writer result");
    });

    let writer_result = match done_rx.recv_timeout(US016_FAST_WRITER_WINDOW) {
        Ok(result) => result,
        Err(RecvTimeoutError::Timeout) => {
            scan_release_tx.send(()).expect("release paused scan");
            panic!("writer blocked behind paused range-scan iteration");
        }
        Err(RecvTimeoutError::Disconnected) => panic!("writer disconnected before result"),
    };
    writer_result.expect("writer update");
    assert!(
        started.elapsed() < US016_FAST_WRITER_WINDOW,
        "writer wait must be O(page-copy), not O(range-scan duration)"
    );

    scan_release_tx.send(()).expect("release paused scan");
    writer.join().expect("writer thread panicked");
    let count = reader
        .join()
        .expect("reader thread panicked")
        .expect("reader scan");
    assert_eq!(count, US016_DOCS as usize);

    assert_us016_latch_samples_short();
}
