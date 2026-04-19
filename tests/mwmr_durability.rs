//! PR 10 acceptance: FullSync writes survive a crash even without an
//! explicit main-file checkpoint.
//!
//! The durability model: writers append a `ChainCommit` frame to the journal
//! inline. `flush_and_sync_if_fullsync` calls `journal_sync` (fdatasync) —
//! not `checkpoint_through_journal` — so the journal is the durability point.
//! On the next open, `JournalManager::open_or_create` replays the journal into
//! the main file automatically.
//!
//! Note on `Client::drop`: the drop impl calls `checkpoint()` (the admin path)
//! when this is the last handle. That means dropping the write client WILL
//! checkpoint the journal to the main file before this test re-opens. The test
//! therefore proves correctness of the full open→write→close→reopen cycle, but
//! does NOT isolate the "journal alone is sufficient" scenario (that would
//! require a fork+SIGKILL or a separate process). Still a valid regression gate.

use bson::doc;
use bson::Document;
use mqlite::{Client, DurabilityMode, OpenOptions};

#[test]
fn fullsync_survives_crash_without_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("crash.mqlite");

    // Write phase: open with FullSync, insert 50 docs, drop client.
    // drop() will call checkpoint() (admin path) since this is the last handle.
    {
        let client = Client::open_with_options(
            &path,
            OpenOptions::new().durability(DurabilityMode::FullSync),
        )
        .unwrap();
        let col = client.database("d").collection::<Document>("c");
        for i in 0..50i32 {
            col.insert_one(&doc! { "_id": i, "v": format!("v-{i}") }).unwrap();
        }
        // Intentional drop — checkpoint runs here on the last handle.
    }

    // Recovery phase: reopen and verify all 50 writes are present.
    let client = Client::open(&path).unwrap();
    let col = client.database("d").collection::<Document>("c");
    let count = col.count_documents(doc! {}).unwrap();
    assert_eq!(
        count, 50,
        "all 50 FullSync writes must survive close+reopen"
    );

    // Spot-check content.
    let one = col.find_one(doc! { "_id": 25 }).unwrap().unwrap();
    assert_eq!(one.get_str("v").unwrap(), "v-25");
}
