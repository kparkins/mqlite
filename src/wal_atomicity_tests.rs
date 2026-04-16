//! B8 — WAL atomicity regression tests.
//!
//! Verifies that mutation paths wrapped in `BpBackend::with_txn` are atomic:
//! a failure mid-txn must not leave partial state visible after reopen, and
//! the upsert path must enforce unique constraints it previously skipped.

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use crate::{
        doc,
        error::Error,
        Client, Document, IndexModel, IndexOptions, OpenOptions, UpdateOptions,
    };

    fn open(dir: &TempDir, name: &str) -> Client {
        Client::open_with_options(dir.path().join(name), OpenOptions::new())
            .expect("open client")
    }

    /// Insert violating a unique secondary index must leave no zombie document
    /// after the client is closed and reopened.
    ///
    /// Before B6 the insert path ran `btree_insert_doc` (primary) + dirtied
    /// the header, then called `maintain_secondary_on_insert` which would
    /// bail with `DuplicateKey`. Without a txn boundary the primary write
    /// reached the file on the next flush — a zombie.
    #[test]
    fn insert_dup_key_leaves_no_zombie_after_reopen() {
        let dir = TempDir::new().expect("tempdir");
        let db_name = "atomicity_zombie.mqlite";

        {
            let client = open(&dir, db_name);
            let col = client
                .database("t")
                .collection::<Document>("people");
            col.create_index(
                IndexModel::builder()
                    .keys(doc! { "email": 1 })
                    .options(IndexOptions::new().unique(true))
                    .build()
                    .unwrap(),
            )
            .expect("create unique index");
            col.insert_one(&doc! { "_id": 1i32, "email": "a@b.com" })
                .expect("first insert succeeds");

            let err = col
                .insert_one(&doc! { "_id": 2i32, "email": "a@b.com" })
                .unwrap_err();
            assert!(
                matches!(err, Error::DuplicateKey { .. }),
                "expected DuplicateKey, got {err:?}"
            );

            // The failing insert must not be visible in the current session.
            assert_eq!(col.count_documents(doc! {}).unwrap(), 1);
            assert!(col
                .find_one(doc! { "_id": 2i32 })
                .unwrap()
                .is_none());
        }

        // Reopen: the zombie must not resurface from the file.
        let client = open(&dir, db_name);
        let col = client
            .database("t")
            .collection::<Document>("people");
        assert_eq!(col.count_documents(doc! {}).unwrap(), 1);
        assert!(col
            .find_one(doc! { "_id": 2i32 })
            .unwrap()
            .is_none());
    }

    /// Upsert that would duplicate a unique key must fail. Before B6 the
    /// upsert helpers skipped `maintain_secondary_on_insert` entirely, so
    /// the duplicate slipped in silently.
    #[test]
    fn upsert_enforces_unique_secondary_index() {
        let dir = TempDir::new().expect("tempdir");
        let client = open(&dir, "atomicity_upsert.mqlite");
        let col = client
            .database("t")
            .collection::<Document>("people");
        col.create_index(
            IndexModel::builder()
                .keys(doc! { "email": 1 })
                .options(IndexOptions::new().unique(true))
                .build()
                .unwrap(),
        )
        .expect("create unique index");

        // First upsert: no match → insert {_id: 1, email: "x@y.com"}.
        col.update_one_with_options(
            doc! { "_id": 1i32 },
            doc! { "$set": { "email": "x@y.com" } },
            UpdateOptions::new().upsert(true),
        )
        .expect("first upsert");
        assert_eq!(col.count_documents(doc! {}).unwrap(), 1);

        // Second upsert: filter matches nothing (different _id), so the
        // engine inserts a new doc with the duplicate email. This must
        // fail on the unique index.
        let err = col
            .update_one_with_options(
                doc! { "_id": 2i32 },
                doc! { "$set": { "email": "x@y.com" } },
                UpdateOptions::new().upsert(true),
            )
            .unwrap_err();
        assert!(
            matches!(err, Error::DuplicateKey { .. }),
            "expected DuplicateKey from upsert, got {err:?}"
        );
        assert_eq!(col.count_documents(doc! {}).unwrap(), 1);
    }

    /// Sanity: durable writes across multiple committed txns survive close
    /// and reopen — verifies the commit-frame flow end-to-end.
    #[test]
    fn multi_txn_commits_survive_reopen() {
        let dir = TempDir::new().expect("tempdir");
        let db_name = "atomicity_durability.mqlite";

        {
            let client = open(&dir, db_name);
            let col = client
                .database("t")
                .collection::<Document>("k");
            for i in 0..20i32 {
                col.insert_one(&doc! { "_id": i, "n": i }).unwrap();
            }
        }

        let client = open(&dir, db_name);
        let col = client
            .database("t")
            .collection::<Document>("k");
        assert_eq!(col.count_documents(doc! {}).unwrap(), 20);
        for i in 0..20i32 {
            let got = col.find_one(doc! { "_id": i }).unwrap();
            assert!(got.is_some(), "doc _id={i} missing after reopen");
        }
    }
}
