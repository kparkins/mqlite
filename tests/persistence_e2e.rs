//! End-to-end persistence test — R3.1
//!
//! Tests the full open → insert → index → close → reopen → verify cycle
//! across three logical databases within a single mqlite file.
//!
//! Spec: phase1-reconciliation.md §3.1
//!
//! Contract:
//! - Open a file-backed client.
//! - Insert 10 000 documents spread across 3 databases, each with its own
//!   collection (users: 4 000, products: 3 000, events: 3 000).
//! - Create one index per collection (unique where appropriate).
//! - Close the client (triggers WAL checkpoint → single-file state).
//! - Reopen the same path with a fresh `Client::open`.
//! - Assert:
//!   * document counts match for all three collections
//!   * all three indexes are present with the correct metadata
//!   * targeted `find_one` queries using the indexed fields return the
//!     expected documents

use mqlite::{doc, Client, IndexModel, IndexOptions};
use bson::Document;

/// Total document counts per collection.
const USERS_COUNT: i32 = 4_000;
const PRODUCTS_COUNT: i32 = 3_000;
const EVENTS_COUNT: i32 = 3_000;

/// Known documents we'll use to verify indexed queries after reopen.
const REF_USER_EMAIL: &str = "user42@example.com";
const REF_PRODUCT_SKU: &str = "SKU-1337";
const REF_EVENT_KIND: &str = "click";

#[test]
fn end_to_end_persistence_across_three_databases() {
    // ------------------------------------------------------------------
    // Phase 1: write
    // ------------------------------------------------------------------

    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("r3_1.mqlite");

    {
        let client = Client::open(&db_path).expect("open new database");

        // ---- users (4 000 docs, unique email index) -------------------
        {
            let db = client.database("users_db");
            let col = db.collection::<Document>("users");

            let docs: Vec<Document> = (0..USERS_COUNT)
                .map(|i| {
                    doc! {
                        "email": format!("user{}@example.com", i),
                        "seq":   i,
                    }
                })
                .collect();
            col.insert_many(&docs).expect("insert users");

            let model = IndexModel::builder()
                .keys(doc! { "email": 1i32 })
                .options(
                    IndexOptions::new()
                        .unique(true)
                        .name("email_unique".to_string()),
                )
                .build()
                .unwrap();
            col.create_index(model).expect("create users email index");

            // Sanity check before close.
            let pre_close = col
                .find_one(doc! { "email": REF_USER_EMAIL })
                .expect("find_one users pre-close");
            assert!(pre_close.is_some(), "reference user must exist before close");
        }

        // ---- products (3 000 docs, unique sku index) ------------------
        {
            let db = client.database("products_db");
            let col = db.collection::<Document>("products");

            let docs: Vec<Document> = (0..PRODUCTS_COUNT)
                .map(|i| {
                    doc! {
                        "sku":   format!("SKU-{}", i),
                        "price": i as f64 * 0.99,
                        "seq":   i,
                    }
                })
                .collect();
            col.insert_many(&docs).expect("insert products");

            let model = IndexModel::builder()
                .keys(doc! { "sku": 1i32 })
                .options(
                    IndexOptions::new()
                        .unique(true)
                        .name("sku_unique".to_string()),
                )
                .build()
                .unwrap();
            col.create_index(model).expect("create products sku index");

            // Sanity check before close.
            let pre_close = col
                .find_one(doc! { "sku": REF_PRODUCT_SKU })
                .expect("find_one products pre-close");
            assert!(
                pre_close.is_some(),
                "reference product must exist before close"
            );
        }

        // ---- events (3 000 docs, non-unique kind index) ---------------
        {
            let db = client.database("events_db");
            let col = db.collection::<Document>("events");

            // Alternate between two kinds so we exercise a non-unique index.
            let kinds = ["click", "view"];
            let docs: Vec<Document> = (0..EVENTS_COUNT)
                .map(|i| {
                    doc! {
                        "kind": kinds[(i % 2) as usize],
                        "seq":  i,
                    }
                })
                .collect();
            col.insert_many(&docs).expect("insert events");

            let model = IndexModel::builder()
                .keys(doc! { "kind": 1i32 })
                .options(IndexOptions::new().name("kind_idx".to_string()))
                .build()
                .unwrap();
            col.create_index(model).expect("create events kind index");
        }

        // Close triggers WAL checkpoint → all data flushed to the single file.
        client.close().expect("close client");
    }

    // ------------------------------------------------------------------
    // Phase 2: reopen and verify
    // ------------------------------------------------------------------

    {
        let client = Client::open(&db_path).expect("reopen database");

        // ---- users ---------------------------------------------------
        {
            let db = client.database("users_db");
            let col = db.collection::<Document>("users");

            // Document count.
            let count = col
                .count_documents(doc! {})
                .expect("count users after reopen");
            assert_eq!(
                count, USERS_COUNT as u64,
                "users count must survive reopen: expected {}, got {}",
                USERS_COUNT, count
            );

            // Index metadata.
            let indexes = col.list_indexes().expect("list users indexes");
            let email_idx = indexes.iter().find(|i| i.name == "email_unique");
            assert!(
                email_idx.is_some(),
                "email_unique index must survive reopen; found: {:?}",
                indexes.iter().map(|i| &i.name).collect::<Vec<_>>()
            );
            assert!(
                email_idx.unwrap().unique,
                "email_unique index must be unique after reopen"
            );

            // Indexed query result.
            let found: Option<Document> = col
                .find_one(doc! { "email": REF_USER_EMAIL })
                .expect("find_one users after reopen");
            assert!(
                found.is_some(),
                "reference user must be findable after reopen"
            );
            assert_eq!(
                found.unwrap().get_str("email").unwrap(),
                REF_USER_EMAIL,
                "email field must match after reopen"
            );
        }

        // ---- products ------------------------------------------------
        {
            let db = client.database("products_db");
            let col = db.collection::<Document>("products");

            // Document count.
            let count = col
                .count_documents(doc! {})
                .expect("count products after reopen");
            assert_eq!(
                count, PRODUCTS_COUNT as u64,
                "products count must survive reopen: expected {}, got {}",
                PRODUCTS_COUNT, count
            );

            // Index metadata.
            let indexes = col.list_indexes().expect("list products indexes");
            let sku_idx = indexes.iter().find(|i| i.name == "sku_unique");
            assert!(
                sku_idx.is_some(),
                "sku_unique index must survive reopen; found: {:?}",
                indexes.iter().map(|i| &i.name).collect::<Vec<_>>()
            );
            assert!(
                sku_idx.unwrap().unique,
                "sku_unique index must be unique after reopen"
            );

            // Indexed query result.
            let found: Option<Document> = col
                .find_one(doc! { "sku": REF_PRODUCT_SKU })
                .expect("find_one products after reopen");
            assert!(
                found.is_some(),
                "reference product must be findable after reopen"
            );
            let found_doc = found.unwrap();
            assert_eq!(
                found_doc.get_str("sku").unwrap(),
                REF_PRODUCT_SKU,
                "sku field must match after reopen"
            );
            // SKU-1337 → seq = 1337.
            assert_eq!(
                found_doc.get_i32("seq").unwrap(),
                1337,
                "seq field must match after reopen"
            );
        }

        // ---- events --------------------------------------------------
        {
            let db = client.database("events_db");
            let col = db.collection::<Document>("events");

            // Document count.
            let count = col
                .count_documents(doc! {})
                .expect("count events after reopen");
            assert_eq!(
                count, EVENTS_COUNT as u64,
                "events count must survive reopen: expected {}, got {}",
                EVENTS_COUNT, count
            );

            // Index metadata.
            let indexes = col.list_indexes().expect("list events indexes");
            let kind_idx = indexes.iter().find(|i| i.name == "kind_idx");
            assert!(
                kind_idx.is_some(),
                "kind_idx index must survive reopen; found: {:?}",
                indexes.iter().map(|i| &i.name).collect::<Vec<_>>()
            );
            assert!(
                !kind_idx.unwrap().unique,
                "kind_idx index must NOT be unique after reopen"
            );

            // Non-unique indexed query: all "click" events (half of EVENTS_COUNT).
            let expected_clicks = (EVENTS_COUNT / 2) as u64;
            let click_count = col
                .count_documents(doc! { "kind": REF_EVENT_KIND })
                .expect("count click events after reopen");
            assert_eq!(
                click_count, expected_clicks,
                "click event count must survive reopen: expected {}, got {}",
                expected_clicks, click_count
            );

            // Spot-check: find_one returns a doc with the correct kind.
            let found: Option<Document> = col
                .find_one(doc! { "kind": REF_EVENT_KIND })
                .expect("find_one events after reopen");
            assert!(
                found.is_some(),
                "at least one click event must be findable after reopen"
            );
            assert_eq!(
                found.unwrap().get_str("kind").unwrap(),
                REF_EVENT_KIND,
                "kind field must match after reopen"
            );
        }
    }
}
