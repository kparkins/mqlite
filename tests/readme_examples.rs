//! Integration tests that exercise the code examples shown in the README.
//!
//! These tests ensure that the README quick-start examples compile and run
//! correctly. They mirror the examples verbatim (with minor adaptations for
//! in-memory mode) so that any future API breakage is immediately caught in CI.

use mqlite::{doc, Client};
use serde::{Deserialize, Serialize};

/// README example 1: untyped Document access.
///
/// Uses `mqlite::Document` (a re-export of `bson::Document`) so callers do
/// not need a direct `bson` dependency.
#[test]
fn readme_untyped_document_example() {
    let client = Client::open_in_memory().expect("open in-memory client");
    let db = client.database("test");
    let events = db.collection::<mqlite::Document>("events");

    events
        .insert_one(&doc! { "action": "login", "user": "alice" })
        .expect("insert_one");

    let event = events.find_one(doc! { "user": "alice" }).expect("find_one");

    assert!(event.is_some(), "should find the inserted document");
    let event = event.unwrap();
    assert_eq!(
        event.get_str("action").unwrap(),
        "login",
        "action field should match"
    );
    assert_eq!(
        event.get_str("user").unwrap(),
        "alice",
        "user field should match"
    );
}

/// README example 2: typed serde struct.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Config {
    key: String,
    value: String,
}

#[test]
fn readme_typed_struct_example() {
    let client = Client::open_in_memory().expect("open in-memory client");
    let db = client.database("test");
    let configs = db.collection::<Config>("config");

    configs
        .insert_one(&Config {
            key: "theme".into(),
            value: "dark".into(),
        })
        .expect("insert_one");

    let theme = configs.find_one(doc! { "key": "theme" }).expect("find_one");

    assert!(theme.is_some(), "should find the inserted Config");
    let theme = theme.unwrap();
    assert_eq!(theme.key, "theme");
    assert_eq!(theme.value, "dark");
}

/// Verify that cargo add directions work: the macro, open, insert, find are
/// all importable from the crate root without a direct `bson` dependency.
#[test]
fn readme_crate_root_imports() {
    // These types must be accessible at the crate root per the README.
    let _: mqlite::Client;
    let _: mqlite::Database;
    let _doc: mqlite::Document = doc! { "key": "value" };
    let _bson: mqlite::Bson = mqlite::Bson::String("hello".into());
    let _ = mqlite::ObjectId::new();
}

/// Verify `Client::open_in_memory()` — the test-double pattern.
#[test]
fn readme_in_memory_test_double() {
    let client = Client::open_in_memory().expect("open in-memory client");
    let db = client.database("test");
    let col = db.collection::<mqlite::Document>("things");
    col.insert_one(&doc! { "x": 1 }).expect("insert");
    let doc = col.find_one(doc! { "x": 1 }).expect("find_one");
    assert!(doc.is_some());
}
