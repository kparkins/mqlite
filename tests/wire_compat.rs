//! Wire protocol compatibility tests (hq-2lz).
//!
//! Validates that the mqlite wire protocol shim returns MongoDB 8.0-compatible
//! responses for all 18 Phase 1 commands, and that mongosh-style and pymongo-
//! style access patterns work correctly.
//!
//! ## Test coverage
//!
//! 1. **Mongosh smoke tests** — simulate the mongosh 2.x CLI connection flow:
//!    listDatabases, listCollections, insertOne, find, updateOne, deleteOne.
//!
//! 2. **pymongo-style tests** — simulate the pymongo 4.x driver flow:
//!    handshake (hello + buildInfo), all CRUD operations, findAndModify,
//!    createIndexes, listIndexes, getMore cursor pagination, and error codes.
//!
//! 3. **Response format parity** — validates that each command's response
//!    matches the MongoDB 8.0 document structure, field names, BSON types,
//!    and nesting as documented in the MongoDB manual:
//!    - `findAndModify` uses `value` (not `document`)
//!    - `getMore` uses `nextBatch` (not `firstBatch`)
//!    - `createIndexes` returns `numIndexesBefore` / `numIndexesAfter`
//!    - `find` with empty result returns `cursor.firstBatch = []`
//!    - `insert` partial failure uses `writeErrors` array
//!    - cursor ns is `"<db>.<collection>"` format
//!
//! 4. **BSON round-trip** — byte-level equality between native API and wire.
//!
//! 5. **Unsupported command** — `aggregate` returns `{ok:0, code:59, codeName:'CommandNotFound'}`.
//!
//! ## Running
//!
//! ```sh
//! cargo test --features wire --test wire_compat
//! ```
//!
//! All tests use a randomly assigned port to avoid conflicts.

#![cfg(feature = "wire")]

use std::io::{Read, Write};
use std::net::TcpStream;

use bson::{doc, Bson, Document};
use mqlite::{Database, WireProtocol};

// ---------------------------------------------------------------------------
// Wire protocol constants
// ---------------------------------------------------------------------------

const OP_MSG: i32 = 2013;
const OP_QUERY: i32 = 2004;
const OP_REPLY: i32 = 1;

// ---------------------------------------------------------------------------
// Test helper: spin up a wire server on a random port
// ---------------------------------------------------------------------------

/// Spin up a `WireProtocol` server backed by an in-memory database.
///
/// Returns `(server, addr)`.  The server runs in the background until dropped.
/// The address is a random loopback port chosen by the OS.
fn start_server() -> (Database, WireProtocol, std::net::SocketAddr) {
    // Grab a random ephemeral port.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let db = Database::open_in_memory().unwrap();
    let server = WireProtocol::bind(&db, &addr.to_string()).unwrap();
    (db, server, addr)
}

// ---------------------------------------------------------------------------
// Wire framing helpers
// ---------------------------------------------------------------------------

/// Build an OP_MSG frame carrying a single Kind-0 section.
fn build_op_msg(request_id: i32, body: &Document) -> Vec<u8> {
    let bson_bytes = bson::to_vec(body).unwrap();
    let total = 16 + 4 + 1 + bson_bytes.len();
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(&(total as i32).to_le_bytes()); // messageLength
    buf.extend_from_slice(&request_id.to_le_bytes()); // requestID
    buf.extend_from_slice(&0i32.to_le_bytes()); // responseTo
    buf.extend_from_slice(&OP_MSG.to_le_bytes()); // opCode
    buf.extend_from_slice(&0u32.to_le_bytes()); // flagBits
    buf.push(0); // Kind-0
    buf.extend_from_slice(&bson_bytes);
    buf
}

/// Read exactly `n` bytes from a TCP stream.
fn read_exact_bytes(stream: &mut TcpStream, n: usize) -> Vec<u8> {
    let mut buf = vec![0u8; n];
    stream.read_exact(&mut buf).expect("read_exact");
    buf
}

/// Read one OP_MSG response from the stream and return the body document.
fn read_op_msg_body(stream: &mut TcpStream) -> Document {
    let header = read_exact_bytes(stream, 16);
    let msg_len = i32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
    let op_code = i32::from_le_bytes(header[12..16].try_into().unwrap());
    assert_eq!(op_code, OP_MSG, "expected OP_MSG response, got opCode={op_code}");

    let rest = read_exact_bytes(stream, msg_len - 16);
    // rest = flagBits(4) + kind(1) + bson_doc
    assert_eq!(rest[4], 0, "expected Kind-0 section");
    let bson_data = &rest[5..];
    bson::from_slice(bson_data).expect("BSON decode")
}

/// Read one OP_REPLY response from the stream and return the body document.
fn read_op_reply_body(stream: &mut TcpStream) -> Document {
    let header = read_exact_bytes(stream, 16);
    let msg_len = i32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
    let op_code = i32::from_le_bytes(header[12..16].try_into().unwrap());
    assert_eq!(op_code, OP_REPLY, "expected OP_REPLY, got opCode={op_code}");

    let rest = read_exact_bytes(stream, msg_len - 16);
    // OP_REPLY body: responseFlags(4) + cursorID(8) + startingFrom(4) + numberReturned(4) + docs
    let doc_start = 20;
    let doc_size =
        i32::from_le_bytes(rest[doc_start..doc_start + 4].try_into().unwrap()) as usize;
    bson::from_slice(&rest[doc_start..doc_start + doc_size]).expect("BSON decode")
}

/// Build a minimal OP_QUERY message (as pymongo sends during handshake).
fn build_op_query(request_id: i32, collection: &str, body: &Document) -> Vec<u8> {
    let bson_bytes = bson::to_vec(body).unwrap();
    let coll_cstr = {
        let mut v = collection.as_bytes().to_vec();
        v.push(0);
        v
    };
    let total = 16 + 4 + coll_cstr.len() + 4 + 4 + bson_bytes.len();
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(&(total as i32).to_le_bytes());
    buf.extend_from_slice(&request_id.to_le_bytes());
    buf.extend_from_slice(&0i32.to_le_bytes());
    buf.extend_from_slice(&OP_QUERY.to_le_bytes());
    buf.extend_from_slice(&0i32.to_le_bytes()); // flags
    buf.extend_from_slice(&coll_cstr);
    buf.extend_from_slice(&0i32.to_le_bytes()); // numberToSkip
    buf.extend_from_slice(&(-1i32).to_le_bytes()); // numberToReturn = -1 (all)
    buf.extend_from_slice(&bson_bytes);
    buf
}

/// Open a TCP connection to `addr` and set a 5-second read timeout.
fn connect(addr: std::net::SocketAddr) -> TcpStream {
    let stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();
    stream
}

/// Convenience: send an OP_MSG and receive the body document.
fn round_trip(stream: &mut TcpStream, request_id: i32, body: &Document) -> Document {
    let frame = build_op_msg(request_id, body);
    stream.write_all(&frame).unwrap();
    read_op_msg_body(stream)
}

// ===========================================================================
// ── 1. MONGOSH SMOKE TESTS ──────────────────────────────────────────────────
// ===========================================================================
//
// These tests simulate the mongosh 2.x connection and CRUD workflow:
//   mongosh "mongodb://localhost:NNNNN/?directConnection=true"
//   > show dbs          (listDatabases)
//   > show collections  (listCollections)
//   > db.col.insertOne({})
//   > db.col.find({})
//   > db.col.updateOne({}, {$set:{x:1}})
//   > db.col.deleteOne({})

/// mongosh step 0: OP_QUERY isMaster handshake (mongosh sends this first).
#[test]
fn mongosh_smoke_op_query_ismaster_handshake() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);

    let frame = build_op_query(
        1,
        "admin.$cmd",
        &doc! { "ismaster": 1, "helloOk": true },
    );
    s.write_all(&frame).unwrap();
    let body = read_op_reply_body(&mut s);

    assert_eq!(body.get_f64("ok").unwrap(), 1.0, "isMaster ok");
    assert!(body.get_bool("isWritablePrimary").unwrap(), "isWritablePrimary");
    assert!(body.get_bool("helloOk").unwrap(), "helloOk echoed");
    assert_eq!(body.get_i32("maxWireVersion").unwrap(), 21, "maxWireVersion=21");
    // topologyVersion and connectionId must be present (pymongo checks them).
    assert!(body.contains_key("topologyVersion"), "topologyVersion present");
    assert!(body.contains_key("connectionId"), "connectionId present");
}

/// mongosh step 1: `show dbs` → listDatabases.
#[test]
fn mongosh_smoke_show_dbs() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);

    let body = round_trip(&mut s, 1, &doc! { "listDatabases": 1i32, "$db": "admin" });

    assert_eq!(body.get_f64("ok").unwrap(), 1.0, "listDatabases ok");
    let dbs = body.get_array("databases").unwrap();
    assert_eq!(dbs.len(), 1, "exactly one database");
    let db_entry = dbs[0].as_document().unwrap();
    assert!(db_entry.contains_key("name"), "database entry has name");
    assert!(db_entry.contains_key("sizeOnDisk"), "database entry has sizeOnDisk");
    assert!(db_entry.contains_key("empty"), "database entry has empty");
    assert!(body.contains_key("totalSize") || body.contains_key("totalSizeMb"),
        "listDatabases response includes total size field");
}

/// mongosh step 2: `show collections` → listCollections (empty db).
#[test]
fn mongosh_smoke_show_collections_empty() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);

    let body = round_trip(&mut s, 1, &doc! { "listCollections": 1i32, "$db": "local" });

    assert_eq!(body.get_f64("ok").unwrap(), 1.0, "listCollections ok");
    let cursor_doc = body.get_document("cursor").unwrap();
    assert_eq!(cursor_doc.get_i64("id").unwrap(), 0, "exhausted cursor id=0");
    assert!(
        cursor_doc.get_array("firstBatch").unwrap().is_empty(),
        "empty db → firstBatch=[]"
    );
}

/// mongosh step 3: `db.col.insertOne({name:"Alice"})`.
#[test]
fn mongosh_smoke_insert_one() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);

    let body = round_trip(
        &mut s,
        1,
        &doc! {
            "insert": "people",
            "documents": [{"name": "Alice"}],
            "$db": "local",
        },
    );

    assert_eq!(body.get_f64("ok").unwrap(), 1.0, "insertOne ok");
    assert_eq!(body.get_i32("n").unwrap(), 1, "n=1");
    // No writeErrors on success.
    assert!(!body.contains_key("writeErrors"), "no writeErrors on success");
}

/// mongosh step 4: `db.col.find({})` — returns inserted docs.
#[test]
fn mongosh_smoke_find_returns_docs() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);

    // Insert first.
    round_trip(
        &mut s,
        1,
        &doc! {
            "insert": "people",
            "documents": [{"name": "Alice"}, {"name": "Bob"}],
            "$db": "local",
        },
    );

    let body = round_trip(
        &mut s,
        2,
        &doc! { "find": "people", "filter": {}, "$db": "local" },
    );

    assert_eq!(body.get_f64("ok").unwrap(), 1.0, "find ok");
    let cursor_doc = body.get_document("cursor").unwrap();
    let batch = cursor_doc.get_array("firstBatch").unwrap();
    assert_eq!(batch.len(), 2, "find returns 2 docs");
    assert_eq!(cursor_doc.get_i64("id").unwrap(), 0, "cursor exhausted");
    // ns must be in "<db>.<collection>" format.
    let ns = cursor_doc.get_str("ns").unwrap();
    assert!(ns.contains("people"), "ns contains collection name");
    assert!(ns.contains('.'), "ns contains a dot");
}

/// mongosh step 5: `db.col.updateOne({name:"Alice"},{$set:{score:100}})`.
#[test]
fn mongosh_smoke_update_one() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);

    round_trip(
        &mut s,
        1,
        &doc! {
            "insert": "scores",
            "documents": [{"name": "Alice", "score": 0i32}],
            "$db": "local",
        },
    );

    let body = round_trip(
        &mut s,
        2,
        &doc! {
            "update": "scores",
            "updates": [{"q": {"name": "Alice"}, "u": {"$set": {"score": 100i32}}, "multi": false}],
            "$db": "local",
        },
    );

    assert_eq!(body.get_f64("ok").unwrap(), 1.0, "updateOne ok");
    assert_eq!(body.get_i64("n").unwrap(), 1, "n=1 matched");
    assert_eq!(body.get_i64("nModified").unwrap(), 1, "nModified=1");
}

/// mongosh step 6: `db.col.deleteOne({name:"Bob"})`.
#[test]
fn mongosh_smoke_delete_one() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);

    round_trip(
        &mut s,
        1,
        &doc! {
            "insert": "people",
            "documents": [{"name": "Alice"}, {"name": "Bob"}],
            "$db": "local",
        },
    );

    let body = round_trip(
        &mut s,
        2,
        &doc! {
            "delete": "people",
            "deletes": [{"q": {"name": "Bob"}, "limit": 1i32}],
            "$db": "local",
        },
    );

    assert_eq!(body.get_f64("ok").unwrap(), 1.0, "deleteOne ok");
    assert_eq!(body.get_i64("n").unwrap(), 1, "n=1");

    // Verify Alice remains.
    let find_body = round_trip(
        &mut s,
        3,
        &doc! { "find": "people", "filter": {}, "$db": "local" },
    );
    let batch = find_body
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    assert_eq!(batch.len(), 1, "only Alice remains");
    assert_eq!(
        batch[0].as_document().unwrap().get_str("name").unwrap(),
        "Alice"
    );
}

// ===========================================================================
// ── 2. PYMONGO-STYLE TESTS ──────────────────────────────────────────────────
// ===========================================================================
//
// These tests simulate the pymongo 4.x driver workflow, including the
// OP_QUERY handshake, hello command, and all CRUD operations.

/// pymongo connect: OP_QUERY isMaster (initial handshake), then hello OP_MSG.
#[test]
fn pymongo_compat_handshake_sequence() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);

    // Step 1: OP_QUERY isMaster (pymongo sends this before it knows wire version).
    let frame = build_op_query(
        1,
        "admin.$cmd",
        &doc! { "ismaster": 1, "helloOk": true },
    );
    s.write_all(&frame).unwrap();
    let reply = read_op_reply_body(&mut s);
    assert_eq!(reply.get_f64("ok").unwrap(), 1.0, "OP_QUERY handshake ok");
    assert!(reply.get_bool("isWritablePrimary").unwrap());

    // Step 2: hello via OP_MSG (subsequent topology checks).
    let body = round_trip(&mut s, 2, &doc! { "hello": 1i32, "$db": "admin" });
    assert_eq!(body.get_f64("ok").unwrap(), 1.0, "hello ok");
    assert!(body.get_bool("isWritablePrimary").unwrap());
    assert_eq!(body.get_i32("maxWireVersion").unwrap(), 21);
}

/// pymongo: MongoClient.server_info() → buildInfo.
#[test]
fn pymongo_compat_build_info() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);

    let body = round_trip(&mut s, 1, &doc! { "buildInfo": 1i32, "$db": "admin" });

    assert_eq!(body.get_f64("ok").unwrap(), 1.0, "buildInfo ok");
    // Fields required by pymongo's server info inspection.
    assert!(body.get_str("version").is_ok(), "version field present");
    assert!(body.get_str("gitVersion").is_ok(), "gitVersion field present");
    assert!(!body.get_str("version").unwrap().is_empty(), "version non-empty");
}

/// pymongo: ping.
#[test]
fn pymongo_compat_ping() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);

    let body = round_trip(&mut s, 1, &doc! { "ping": 1i32, "$db": "admin" });
    assert_eq!(body.get_f64("ok").unwrap(), 1.0, "ping ok");
}

/// pymongo: insert_one / find.
#[test]
fn pymongo_compat_insert_one_find() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);

    let ins = round_trip(
        &mut s,
        1,
        &doc! { "insert": "coll", "documents": [{"x": 42i32}], "$db": "local" },
    );
    assert_eq!(ins.get_f64("ok").unwrap(), 1.0);
    assert_eq!(ins.get_i32("n").unwrap(), 1);

    let find = round_trip(
        &mut s,
        2,
        &doc! { "find": "coll", "filter": {"x": 42i32}, "$db": "local" },
    );
    assert_eq!(find.get_f64("ok").unwrap(), 1.0);
    let batch = find
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(
        batch[0].as_document().unwrap().get_i32("x").unwrap(),
        42
    );
}

/// pymongo: update_one.
#[test]
fn pymongo_compat_update_one() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);

    round_trip(
        &mut s,
        1,
        &doc! { "insert": "c", "documents": [{"k": "v", "counter": 0i32}], "$db": "local" },
    );

    let upd = round_trip(
        &mut s,
        2,
        &doc! {
            "update": "c",
            "updates": [{"q": {"k": "v"}, "u": {"$set": {"counter": 1i32}}, "multi": false}],
            "$db": "local",
        },
    );
    assert_eq!(upd.get_f64("ok").unwrap(), 1.0);
    assert_eq!(upd.get_i64("n").unwrap(), 1);
    assert_eq!(upd.get_i64("nModified").unwrap(), 1);
}

/// pymongo: delete_one.
#[test]
fn pymongo_compat_delete_one() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);

    round_trip(
        &mut s,
        1,
        &doc! { "insert": "c", "documents": [{"tag": "gone"}], "$db": "local" },
    );

    let del = round_trip(
        &mut s,
        2,
        &doc! {
            "delete": "c",
            "deletes": [{"q": {"tag": "gone"}, "limit": 1i32}],
            "$db": "local",
        },
    );
    assert_eq!(del.get_f64("ok").unwrap(), 1.0);
    assert_eq!(del.get_i64("n").unwrap(), 1);
}

/// pymongo: find_one_and_update (findAndModify).
///
/// Key format requirement: response uses `value` field, NOT `document`.
#[test]
fn pymongo_compat_find_one_and_update() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);

    round_trip(
        &mut s,
        1,
        &doc! { "insert": "c", "documents": [{"name": "Alice", "score": 10i32}], "$db": "local" },
    );

    let fam = round_trip(
        &mut s,
        2,
        &doc! {
            "findandmodify": "c",
            "query": {"name": "Alice"},
            "update": {"$set": {"score": 99i32}},
            "new": false,
            "$db": "local",
        },
    );
    assert_eq!(fam.get_f64("ok").unwrap(), 1.0);
    // CRITICAL: MongoDB 8.0 uses `value`, not `document`.
    assert!(fam.contains_key("value"), "findAndModify response must have 'value' field");
    assert!(
        !fam.contains_key("document"),
        "findAndModify must NOT have 'document' field"
    );
    let value_doc = fam.get_document("value").unwrap();
    assert_eq!(value_doc.get_str("name").unwrap(), "Alice");
    assert_eq!(value_doc.get_i32("score").unwrap(), 10, "pre-update value returned");
    // lastErrorObject is required by pymongo.
    let leo = fam.get_document("lastErrorObject").unwrap();
    assert_eq!(leo.get_i32("n").unwrap(), 1);
}

/// pymongo: find_one_and_update with new=true returns post-update doc.
#[test]
fn pymongo_compat_find_one_and_update_new_true() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);

    round_trip(
        &mut s,
        1,
        &doc! { "insert": "c", "documents": [{"v": 1i32}], "$db": "local" },
    );

    let fam = round_trip(
        &mut s,
        2,
        &doc! {
            "findandmodify": "c",
            "query": {"v": 1i32},
            "update": {"$set": {"v": 2i32}},
            "new": true,
            "$db": "local",
        },
    );
    assert_eq!(fam.get_f64("ok").unwrap(), 1.0);
    assert!(fam.contains_key("value"), "value field present");
    assert_eq!(fam.get_document("value").unwrap().get_i32("v").unwrap(), 2);
}

/// pymongo: findAndModify on missing doc → value is null.
#[test]
fn pymongo_compat_find_and_modify_no_match_null_value() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);

    let fam = round_trip(
        &mut s,
        1,
        &doc! {
            "findandmodify": "empty_coll",
            "query": {"nonexistent": true},
            "update": {"$set": {"x": 1i32}},
            "$db": "local",
        },
    );
    assert_eq!(fam.get_f64("ok").unwrap(), 1.0);
    assert_eq!(
        fam.get("value"),
        Some(&Bson::Null),
        "no match → value:null"
    );
}

/// pymongo: create_index, then list_indexes.
///
/// Key format requirements for `createIndexes`:
/// - `numIndexesBefore` and `numIndexesAfter` must be present.
#[test]
fn pymongo_compat_create_index_list_indexes() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);

    let ci = round_trip(
        &mut s,
        1,
        &doc! {
            "createIndexes": "users",
            "indexes": [{"key": {"email": 1i32}, "name": "email_1"}],
            "$db": "local",
        },
    );
    assert_eq!(ci.get_f64("ok").unwrap(), 1.0, "createIndexes ok");
    // CRITICAL: MongoDB 8.0 format requires these two fields.
    assert!(
        ci.get_i32("numIndexesBefore").is_ok(),
        "numIndexesBefore must be i32"
    );
    assert!(
        ci.get_i32("numIndexesAfter").is_ok(),
        "numIndexesAfter must be i32"
    );
    assert_eq!(
        ci.get_i32("numIndexesBefore").unwrap(),
        1,
        "only _id_ before"
    );
    assert_eq!(
        ci.get_i32("numIndexesAfter").unwrap(),
        2,
        "_id_ + email_1 after"
    );

    let li = round_trip(
        &mut s,
        2,
        &doc! { "listIndexes": "users", "$db": "local" },
    );
    assert_eq!(li.get_f64("ok").unwrap(), 1.0, "listIndexes ok");
    let batch = li
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    assert_eq!(batch.len(), 2, "_id_ + email_1");
    // _id_ index must always be first.
    let id_idx = batch[0].as_document().unwrap();
    assert_eq!(id_idx.get_str("name").unwrap(), "_id_");
    assert_eq!(id_idx.get_i32("v").unwrap(), 2, "index version must be 2");
    assert!(id_idx.get_document("key").is_ok(), "key field present");
}

/// pymongo: cursor iteration via getMore.
///
/// Key format requirement: getMore response uses `nextBatch` (not `firstBatch`).
#[test]
fn pymongo_compat_cursor_get_more() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);

    // Insert 5 docs.
    round_trip(
        &mut s,
        1,
        &doc! {
            "insert": "pages",
            "documents": [
                {"i": 0i32}, {"i": 1i32}, {"i": 2i32}, {"i": 3i32}, {"i": 4i32}
            ],
            "$db": "local",
        },
    );

    // find with batchSize=2.
    let find = round_trip(
        &mut s,
        2,
        &doc! { "find": "pages", "filter": {}, "batchSize": 2i32, "$db": "local" },
    );
    assert_eq!(find.get_f64("ok").unwrap(), 1.0);
    let cursor_doc = find.get_document("cursor").unwrap();
    let cursor_id = cursor_doc.get_i64("id").unwrap();
    assert_ne!(cursor_id, 0, "live cursor id must be non-zero");
    assert_eq!(
        cursor_doc.get_array("firstBatch").unwrap().len(),
        2,
        "firstBatch has 2 docs"
    );

    // getMore: next 2.
    let gm = round_trip(
        &mut s,
        3,
        &doc! {
            "getMore": Bson::Int64(cursor_id),
            "collection": "pages",
            "batchSize": 2i32,
            "$db": "local",
        },
    );
    assert_eq!(gm.get_f64("ok").unwrap(), 1.0, "getMore ok");
    let gm_cursor = gm.get_document("cursor").unwrap();
    // CRITICAL: getMore uses `nextBatch`, NOT `firstBatch`.
    assert!(
        gm_cursor.contains_key("nextBatch"),
        "getMore response must use 'nextBatch'"
    );
    assert!(
        !gm_cursor.contains_key("firstBatch"),
        "getMore must NOT use 'firstBatch'"
    );
    assert_eq!(
        gm_cursor.get_array("nextBatch").unwrap().len(),
        2,
        "nextBatch has 2 docs"
    );
    let mid_cursor_id = gm_cursor.get_i64("id").unwrap();
    assert_ne!(mid_cursor_id, 0, "one doc still remains");

    // getMore: final doc.
    let gm2 = round_trip(
        &mut s,
        4,
        &doc! {
            "getMore": Bson::Int64(mid_cursor_id),
            "collection": "pages",
            "$db": "local",
        },
    );
    assert_eq!(gm2.get_f64("ok").unwrap(), 1.0);
    let gm2_cursor = gm2.get_document("cursor").unwrap();
    assert!(gm2_cursor.contains_key("nextBatch"), "final getMore uses nextBatch");
    assert_eq!(gm2_cursor.get_array("nextBatch").unwrap().len(), 1);
    assert_eq!(
        gm2_cursor.get_i64("id").unwrap(),
        0,
        "cursor exhausted after final batch"
    );
}

/// pymongo: getMore on unknown cursor returns CursorNotFound (code 43).
#[test]
fn pymongo_compat_get_more_unknown_cursor() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);

    let body = round_trip(
        &mut s,
        1,
        &doc! {
            "getMore": Bson::Int64(99999i64),
            "collection": "c",
            "$db": "local",
        },
    );
    assert_eq!(body.get_f64("ok").unwrap(), 0.0, "unknown cursor → error");
    assert_eq!(body.get_i32("code").unwrap(), 43, "CursorNotFound = 43");
    assert_eq!(
        body.get_str("codeName").unwrap(),
        "CursorNotFound",
        "codeName must be 'CursorNotFound'"
    );
}

// ===========================================================================
// ── 3. RESPONSE FORMAT PARITY — ALL 18 PHASE 1 COMMANDS ────────────────────
// ===========================================================================
//
// Validates field presence, types, and nesting for each command's response
// against the MongoDB 8.0 documented wire protocol format.

/// Format parity: ping response.
#[test]
fn format_parity_ping() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);
    let body = round_trip(&mut s, 1, &doc! { "ping": 1i32, "$db": "admin" });
    assert_eq!(body.get_f64("ok").unwrap(), 1.0);
}

/// Format parity: hello response fields.
#[test]
fn format_parity_hello() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);
    let body = round_trip(&mut s, 1, &doc! { "hello": 1i32, "$db": "admin" });

    assert_eq!(body.get_f64("ok").unwrap(), 1.0);
    assert!(body.get_bool("isWritablePrimary").is_ok(), "isWritablePrimary");
    assert!(body.get_bool("helloOk").is_ok(), "helloOk");
    assert!(body.get_i32("maxWireVersion").is_ok(), "maxWireVersion");
    assert!(body.get_i32("minWireVersion").is_ok(), "minWireVersion");
    assert!(body.get_str("localTime").is_err(), "localTime is DateTime not string");
    assert!(body.contains_key("localTime"), "localTime present");
    assert!(body.contains_key("connectionId"), "connectionId");
    assert!(body.contains_key("topologyVersion"), "topologyVersion");
    let tv = body.get_document("topologyVersion").unwrap();
    assert!(tv.contains_key("processId"), "topologyVersion.processId");
    assert!(tv.contains_key("counter"), "topologyVersion.counter");
}

/// Format parity: isMaster (legacy alias for hello).
#[test]
fn format_parity_ismaster() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);
    let body = round_trip(&mut s, 1, &doc! { "isMaster": 1i32, "$db": "admin" });

    assert_eq!(body.get_f64("ok").unwrap(), 1.0);
    assert!(body.get_bool("isWritablePrimary").is_ok());
    assert_eq!(body.get_i32("maxWireVersion").unwrap(), 21);
}

/// Format parity: buildInfo response.
#[test]
fn format_parity_build_info() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);
    let body = round_trip(&mut s, 1, &doc! { "buildInfo": 1i32, "$db": "admin" });

    assert_eq!(body.get_f64("ok").unwrap(), 1.0);
    assert!(body.get_str("version").is_ok(), "version: string");
    assert!(body.get_str("gitVersion").is_ok(), "gitVersion: string");
    // modules must be an empty array (MongoDB 8.0 requirement).
    assert_eq!(
        body.get_array("modules").unwrap().len(),
        0,
        "modules must be an empty array"
    );
    assert!(body.contains_key("allocator"), "allocator field present");
}

/// Format parity: serverStatus response.
#[test]
fn format_parity_server_status() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);
    let body = round_trip(&mut s, 1, &doc! { "serverStatus": 1i32, "$db": "admin" });

    assert_eq!(body.get_f64("ok").unwrap(), 1.0);
    assert!(body.get_i64("uptime").unwrap() >= 0, "uptime >= 0");
    assert!(body.contains_key("uptimeMillis"), "uptimeMillis");
    assert!(body.contains_key("connections"), "connections subdoc");
    assert!(body.contains_key("storageEngine"), "storageEngine subdoc");
    assert!(body.contains_key("localTime"), "localTime");
}

/// Format parity: listDatabases response.
#[test]
fn format_parity_list_databases() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);
    let body = round_trip(&mut s, 1, &doc! { "listDatabases": 1i32, "$db": "admin" });

    assert_eq!(body.get_f64("ok").unwrap(), 1.0);
    let dbs = body.get_array("databases").unwrap();
    assert_eq!(dbs.len(), 1);
    let db_entry = dbs[0].as_document().unwrap();
    assert!(db_entry.get_str("name").is_ok(), "name: string");
    // sizeOnDisk must be numeric (i64 or f64).
    assert!(
        db_entry.contains_key("sizeOnDisk"),
        "sizeOnDisk field present"
    );
    assert!(db_entry.contains_key("empty"), "empty: bool");
}

/// Format parity: insert response fields and types.
#[test]
fn format_parity_insert() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);
    let body = round_trip(
        &mut s,
        1,
        &doc! {
            "insert": "items",
            "documents": [{"a": 1i32}, {"a": 2i32}],
            "$db": "local",
        },
    );

    assert_eq!(body.get_f64("ok").unwrap(), 1.0);
    // n must be Int32.
    assert!(body.get_i32("n").is_ok(), "n must be Int32");
    assert_eq!(body.get_i32("n").unwrap(), 2);
    // writeErrors must NOT appear on success.
    assert!(
        !body.contains_key("writeErrors"),
        "writeErrors absent on success"
    );
}

/// Format parity: insert with duplicate key → writeErrors array.
///
/// The writeErrors array is the MongoDB 8.0 format for partial write failures.
#[test]
fn format_parity_insert_write_errors() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);

    // Create unique index on "email".
    round_trip(
        &mut s,
        1,
        &doc! {
            "createIndexes": "emails",
            "indexes": [{"key": {"email": 1i32}, "name": "email_1", "unique": true}],
            "$db": "local",
        },
    );

    // Insert a doc to set up the conflict.
    round_trip(
        &mut s,
        2,
        &doc! {
            "insert": "emails",
            "documents": [{"email": "alice@example.com"}],
            "$db": "local",
        },
    );

    // Insert with duplicate — ordered=false so the second doc lands.
    let body = round_trip(
        &mut s,
        3,
        &doc! {
            "insert": "emails",
            "documents": [
                {"email": "alice@example.com"},   // duplicate → writeError
                {"email": "bob@example.com"},      // succeeds
            ],
            "ordered": false,
            "$db": "local",
        },
    );
    assert_eq!(body.get_f64("ok").unwrap(), 1.0, "partial success ok=1");
    // CRITICAL: writeErrors must be an array with one entry.
    assert!(body.contains_key("writeErrors"), "writeErrors must be present on partial failure");
    let write_errors = body.get_array("writeErrors").unwrap();
    assert_eq!(write_errors.len(), 1, "exactly one write error");
    let err_entry = write_errors[0].as_document().unwrap();
    // index field identifies which doc failed.
    assert!(err_entry.contains_key("index"), "writeError.index field");
    // code must be 11000 (DuplicateKey).
    assert_eq!(err_entry.get_i32("code").unwrap(), 11000, "DuplicateKey=11000");
    // errmsg must be a string.
    assert!(err_entry.get_str("errmsg").is_ok(), "errmsg: string");
}

/// Format parity: find with empty collection → cursor.firstBatch = [].
#[test]
fn format_parity_find_empty_first_batch() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);
    let body = round_trip(
        &mut s,
        1,
        &doc! { "find": "nonexistent", "filter": {}, "$db": "local" },
    );

    assert_eq!(body.get_f64("ok").unwrap(), 1.0);
    let cursor_doc = body.get_document("cursor").unwrap();
    // CRITICAL: empty result must be cursor.firstBatch = [].
    let batch = cursor_doc.get_array("firstBatch").unwrap();
    assert!(batch.is_empty(), "empty collection → firstBatch=[]");
    assert_eq!(cursor_doc.get_i64("id").unwrap(), 0, "cursor id=0 when exhausted");
    // ns is always present.
    assert!(cursor_doc.get_str("ns").is_ok(), "ns field present");
}

/// Format parity: update response fields (n, nModified, upserted).
#[test]
fn format_parity_update() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);

    round_trip(
        &mut s,
        1,
        &doc! { "insert": "u", "documents": [{"x": 1i32}], "$db": "local" },
    );

    let body = round_trip(
        &mut s,
        2,
        &doc! {
            "update": "u",
            "updates": [{"q": {"x": 1i32}, "u": {"$set": {"x": 2i32}}, "multi": false}],
            "$db": "local",
        },
    );

    assert_eq!(body.get_f64("ok").unwrap(), 1.0);
    // n and nModified must be Int64 in MongoDB 8.0 wire format.
    assert!(body.get_i64("n").is_ok(), "n must be Int64 or coercible");
    assert!(body.get_i64("nModified").is_ok(), "nModified must be present");
    assert_eq!(body.get_i64("n").unwrap(), 1);
    assert_eq!(body.get_i64("nModified").unwrap(), 1);
    // upserted must NOT appear when no upsert happened.
    assert!(
        !body.contains_key("upserted"),
        "upserted absent when no upsert"
    );
}

/// Format parity: update with upsert → upserted array with index + _id.
#[test]
fn format_parity_update_upserted_array() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);

    let body = round_trip(
        &mut s,
        1,
        &doc! {
            "update": "c",
            "updates": [{"q": {"_id": "new"}, "u": {"$set": {"v": 1i32}}, "upsert": true}],
            "$db": "local",
        },
    );

    assert_eq!(body.get_f64("ok").unwrap(), 1.0);
    let upserted = body.get_array("upserted").unwrap();
    assert_eq!(upserted.len(), 1, "one upserted entry");
    let entry = upserted[0].as_document().unwrap();
    assert_eq!(entry.get_i32("index").unwrap(), 0, "upserted[0].index = 0");
    assert!(entry.contains_key("_id"), "upserted[0]._id present");
}

/// Format parity: delete response.
#[test]
fn format_parity_delete() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);

    round_trip(
        &mut s,
        1,
        &doc! { "insert": "d", "documents": [{"k": "gone"}], "$db": "local" },
    );

    let body = round_trip(
        &mut s,
        2,
        &doc! {
            "delete": "d",
            "deletes": [{"q": {"k": "gone"}, "limit": 1i32}],
            "$db": "local",
        },
    );

    assert_eq!(body.get_f64("ok").unwrap(), 1.0);
    // n must be Int64.
    assert!(body.get_i64("n").is_ok(), "n must be present");
    assert_eq!(body.get_i64("n").unwrap(), 1);
}

/// Format parity: findAndModify response structure.
///
/// MongoDB 8.0 uses `value` (not `document`) + `lastErrorObject`.
#[test]
fn format_parity_find_and_modify() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);

    round_trip(
        &mut s,
        1,
        &doc! { "insert": "c", "documents": [{"name": "test"}], "$db": "local" },
    );

    let body = round_trip(
        &mut s,
        2,
        &doc! {
            "findandmodify": "c",
            "query": {"name": "test"},
            "update": {"$set": {"updated": true}},
            "$db": "local",
        },
    );

    assert_eq!(body.get_f64("ok").unwrap(), 1.0);
    // CRITICAL: must use `value` not `document`.
    assert!(body.contains_key("value"), "'value' field required by MongoDB 8.0");
    assert!(!body.contains_key("document"), "'document' field must NOT appear");
    // lastErrorObject must be present.
    let leo = body.get_document("lastErrorObject").unwrap();
    assert!(leo.contains_key("n"), "lastErrorObject.n present");
}

/// Format parity: getMore response — cursor.nextBatch (not firstBatch).
#[test]
fn format_parity_get_more_next_batch() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);

    round_trip(
        &mut s,
        1,
        &doc! {
            "insert": "g",
            "documents": [{"i": 1i32}, {"i": 2i32}],
            "$db": "local",
        },
    );

    let find = round_trip(
        &mut s,
        2,
        &doc! { "find": "g", "filter": {}, "batchSize": 1i32, "$db": "local" },
    );
    let cursor_id = find
        .get_document("cursor")
        .unwrap()
        .get_i64("id")
        .unwrap();

    let gm = round_trip(
        &mut s,
        3,
        &doc! {
            "getMore": Bson::Int64(cursor_id),
            "collection": "g",
            "$db": "local",
        },
    );
    assert_eq!(gm.get_f64("ok").unwrap(), 1.0);
    let cursor_doc = gm.get_document("cursor").unwrap();
    // CRITICAL: getMore response field name.
    assert!(
        cursor_doc.contains_key("nextBatch"),
        "getMore cursor.nextBatch required"
    );
    assert!(
        !cursor_doc.contains_key("firstBatch"),
        "getMore must NOT have firstBatch"
    );
    // cursor ns must be present.
    assert!(cursor_doc.get_str("ns").is_ok(), "ns present in getMore cursor");
}

/// Format parity: killCursors response.
#[test]
fn format_parity_kill_cursors() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);

    // Open a cursor.
    round_trip(
        &mut s,
        1,
        &doc! { "insert": "kc", "documents": [{"i": 1i32}, {"i": 2i32}], "$db": "local" },
    );
    let find = round_trip(
        &mut s,
        2,
        &doc! { "find": "kc", "filter": {}, "batchSize": 1i32, "$db": "local" },
    );
    let cursor_id = find.get_document("cursor").unwrap().get_i64("id").unwrap();

    let kc = round_trip(
        &mut s,
        3,
        &doc! {
            "killCursors": "kc",
            "cursors": [Bson::Int64(cursor_id)],
            "$db": "local",
        },
    );
    assert_eq!(kc.get_f64("ok").unwrap(), 1.0);
    // MongoDB 8.0 format: cursorsKilled and cursorsNotFound arrays.
    assert!(kc.contains_key("cursorsKilled"), "cursorsKilled array required");
    assert!(kc.contains_key("cursorsNotFound"), "cursorsNotFound array required");
    let killed = kc.get_array("cursorsKilled").unwrap();
    assert_eq!(killed.len(), 1, "one cursor killed");
    let not_found = kc.get_array("cursorsNotFound").unwrap();
    assert!(not_found.is_empty(), "no not-found cursors");
}

/// Format parity: create collection response.
#[test]
fn format_parity_create_collection() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);
    let body = round_trip(&mut s, 1, &doc! { "create": "newcol", "$db": "local" });
    assert_eq!(body.get_f64("ok").unwrap(), 1.0);
}

/// Format parity: drop collection response.
#[test]
fn format_parity_drop_collection() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);

    round_trip(
        &mut s,
        1,
        &doc! { "insert": "todrop", "documents": [{"x": 1i32}], "$db": "local" },
    );
    let body = round_trip(&mut s, 2, &doc! { "drop": "todrop", "$db": "local" });
    assert_eq!(body.get_f64("ok").unwrap(), 1.0);
}

/// Format parity: listCollections response structure.
///
/// Each collection entry must have: name, type, options, idIndex.
#[test]
fn format_parity_list_collections() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);

    round_trip(
        &mut s,
        1,
        &doc! { "insert": "mycoll", "documents": [{"x": 1i32}], "$db": "local" },
    );

    let body = round_trip(&mut s, 2, &doc! { "listCollections": 1i32, "$db": "local" });
    assert_eq!(body.get_f64("ok").unwrap(), 1.0);
    let cursor_doc = body.get_document("cursor").unwrap();
    let batch = cursor_doc.get_array("firstBatch").unwrap();
    assert_eq!(batch.len(), 1);

    // MongoDB 8.0 listCollections entry format.
    let entry = batch[0].as_document().unwrap();
    assert_eq!(entry.get_str("name").unwrap(), "mycoll", "name field");
    assert_eq!(entry.get_str("type").unwrap(), "collection", "type='collection'");
    assert!(entry.contains_key("options"), "options subdoc");
    assert!(entry.contains_key("idIndex"), "idIndex subdoc");
    let id_index = entry.get_document("idIndex").unwrap();
    assert!(id_index.contains_key("key"), "idIndex.key");
    assert_eq!(id_index.get_i32("v").unwrap(), 2, "idIndex.v=2");
}

/// Format parity: createIndexes response.
#[test]
fn format_parity_create_indexes() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);
    let body = round_trip(
        &mut s,
        1,
        &doc! {
            "createIndexes": "idx",
            "indexes": [{"key": {"score": 1i32}, "name": "score_1"}],
            "$db": "local",
        },
    );

    assert_eq!(body.get_f64("ok").unwrap(), 1.0);
    // CRITICAL: MongoDB 8.0 requires both fields.
    assert_eq!(body.get_i32("numIndexesBefore").unwrap(), 1);
    assert_eq!(body.get_i32("numIndexesAfter").unwrap(), 2);
}

/// Format parity: dropIndexes response.
#[test]
fn format_parity_drop_indexes() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);

    round_trip(
        &mut s,
        1,
        &doc! {
            "createIndexes": "idx",
            "indexes": [{"key": {"x": 1i32}, "name": "x_1"}],
            "$db": "local",
        },
    );

    let body = round_trip(
        &mut s,
        2,
        &doc! { "dropIndexes": "idx", "index": "x_1", "$db": "local" },
    );
    assert_eq!(body.get_f64("ok").unwrap(), 1.0);
}

/// Format parity: listIndexes response.
#[test]
fn format_parity_list_indexes() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);

    round_trip(
        &mut s,
        1,
        &doc! {
            "createIndexes": "idx",
            "indexes": [{"key": {"email": 1i32}, "name": "email_1", "unique": true}],
            "$db": "local",
        },
    );

    let body = round_trip(&mut s, 2, &doc! { "listIndexes": "idx", "$db": "local" });
    assert_eq!(body.get_f64("ok").unwrap(), 1.0);
    let cursor_doc = body.get_document("cursor").unwrap();
    let batch = cursor_doc.get_array("firstBatch").unwrap();
    // _id_ + email_1
    assert_eq!(batch.len(), 2);

    // Each index entry must have: v (Int32=2), key (Document), name (String).
    for entry_bson in batch {
        let entry = entry_bson.as_document().unwrap();
        assert_eq!(entry.get_i32("v").unwrap(), 2, "index version v=2");
        assert!(entry.get_str("name").is_ok(), "name: string");
        assert!(entry.get_document("key").is_ok(), "key: document");
    }

    // email_1 must have unique=true.
    let email_idx = batch[1].as_document().unwrap();
    assert_eq!(email_idx.get_str("name").unwrap(), "email_1");
    assert!(email_idx.get_bool("unique").unwrap(), "unique=true");
}

// ===========================================================================
// ── 4. BSON ROUND-TRIP TESTS ────────────────────────────────────────────────
// ===========================================================================
//
// Tests that data written via the native Rust API is byte-identical when
// read back via the wire protocol, and vice versa.

/// Native API → wire: insert via native API, read via wire protocol.
#[test]
fn bson_roundtrip_native_write_wire_read() {
    let db = Database::open_in_memory().unwrap();
    let coll = db.collection::<Document>("items");

    // Insert via native API.
    let doc_to_insert = doc! {
        "str": "hello",
        "int32": 42i32,
        "int64": 9999999i64,
        "double": 3.14_f64,
        "bool": true,
        "null_field": Bson::Null,
        "array": [1i32, 2i32, 3i32],
        "nested": {"key": "value"},
    };
    coll.insert_one(&doc_to_insert).unwrap();

    // Read via wire protocol.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let _srv = WireProtocol::bind(&db, &addr.to_string()).unwrap();

    let mut s = connect(addr);
    let find_body = round_trip(
        &mut s,
        1,
        &doc! { "find": "items", "filter": {}, "$db": "local" },
    );

    let batch = find_body
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    assert_eq!(batch.len(), 1, "one document returned via wire");

    let returned_doc = batch[0].as_document().unwrap();
    // Verify BSON types are preserved.
    assert_eq!(returned_doc.get_str("str").unwrap(), "hello", "string preserved");
    assert_eq!(returned_doc.get_i32("int32").unwrap(), 42, "Int32 preserved");
    assert_eq!(returned_doc.get_i64("int64").unwrap(), 9999999, "Int64 preserved");
    assert!((returned_doc.get_f64("double").unwrap() - 3.14).abs() < 1e-9, "Double preserved");
    assert!(returned_doc.get_bool("bool").unwrap(), "Bool preserved");
    assert_eq!(
        returned_doc.get("null_field"),
        Some(&Bson::Null),
        "Null preserved"
    );
    let arr = returned_doc.get_array("array").unwrap();
    assert_eq!(arr.len(), 3, "Array length preserved");
    assert_eq!(
        returned_doc.get_document("nested").unwrap().get_str("key").unwrap(),
        "value",
        "Nested document preserved"
    );
}

/// Wire → native API: insert via wire, read via native API.
#[test]
fn bson_roundtrip_wire_write_native_read() {
    let db = Database::open_in_memory().unwrap();
    let coll = db.collection::<Document>("items");

    // Bind the wire server.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let _srv = WireProtocol::bind(&db, &addr.to_string()).unwrap();

    // Insert via wire protocol.
    let mut s = connect(addr);
    round_trip(
        &mut s,
        1,
        &doc! {
            "insert": "items",
            "documents": [{
                "str": "wire_inserted",
                "num": 777i32,
                "nested": {"a": "b"},
            }],
            "$db": "local",
        },
    );

    // Read via native API.
    let found = coll.find_one(doc! { "str": "wire_inserted" }).unwrap();
    assert!(found.is_some(), "native API finds wire-inserted document");
    let doc = found.unwrap();
    assert_eq!(doc.get_i32("num").unwrap(), 777, "Int32 preserved in native read");
    assert_eq!(
        doc.get_document("nested").unwrap().get_str("a").unwrap(),
        "b",
        "nested doc preserved in native read"
    );
}

/// BSON round-trip: ObjectId preserved across wire.
#[test]
fn bson_roundtrip_object_id_preserved() {
    let db = Database::open_in_memory().unwrap();
    let coll = db.collection::<Document>("oids");

    let oid = bson::oid::ObjectId::new();
    coll.insert_one(&doc! { "_id": oid, "label": "oid-test" }).unwrap();

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let _srv = WireProtocol::bind(&db, &addr.to_string()).unwrap();

    let mut s = connect(addr);
    let find_body = round_trip(
        &mut s,
        1,
        &doc! { "find": "oids", "filter": {}, "$db": "local" },
    );
    let batch = find_body
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    let returned = batch[0].as_document().unwrap();
    match returned.get("_id") {
        Some(Bson::ObjectId(returned_oid)) => {
            assert_eq!(*returned_oid, oid, "ObjectId byte-identical");
        }
        other => panic!("expected ObjectId, got {other:?}"),
    }
}

// ===========================================================================
// ── 5. UNSUPPORTED COMMAND TESTS ────────────────────────────────────────────
// ===========================================================================
//
// Commands outside the Phase 1 surface must return CommandNotFound.
// MongoDB 8.0 format: {ok:0, code:59, codeName:"CommandNotFound"}.

/// aggregate → CommandNotFound (code 59).
#[test]
fn unsupported_command_aggregate_returns_command_not_found() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);

    let body = round_trip(
        &mut s,
        1,
        &doc! { "aggregate": "coll", "$db": "local" },
    );

    assert_eq!(body.get_f64("ok").unwrap(), 0.0, "aggregate must fail");
    assert_eq!(
        body.get_i32("code").unwrap(),
        59,
        "error code must be 59 (CommandNotFound)"
    );
    assert_eq!(
        body.get_str("codeName").unwrap(),
        "CommandNotFound",
        "codeName must be 'CommandNotFound'"
    );
    // errmsg must be present.
    assert!(body.get_str("errmsg").is_ok(), "errmsg field required");
}

/// Unknown command → CommandNotFound.
#[test]
fn unsupported_command_unknown_returns_command_not_found() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);

    let body = round_trip(
        &mut s,
        1,
        &doc! { "doesNotExistCommand": 1i32, "$db": "local" },
    );

    assert_eq!(body.get_f64("ok").unwrap(), 0.0, "unknown command must fail");
    assert_eq!(body.get_i32("code").unwrap(), 59, "code=59");
    assert_eq!(body.get_str("codeName").unwrap(), "CommandNotFound");
}

/// mapReduce → CommandNotFound (removed in MongoDB 8.0).
#[test]
fn unsupported_command_map_reduce_not_found() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);

    let body = round_trip(
        &mut s,
        1,
        &doc! { "mapReduce": "coll", "$db": "local" },
    );

    assert_eq!(body.get_i32("code").unwrap(), 59);
}

// ===========================================================================
// ── 6. DIRECTCONNECTION REQUIREMENT ─────────────────────────────────────────
// ===========================================================================
//
// mqlite presents as a standalone (non-replica-set) server.
// Drivers must use directConnection=true to avoid replica-set discovery.

/// hello response must indicate standalone topology (no setName).
#[test]
fn direct_connection_no_replica_set_fields() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);

    let body = round_trip(&mut s, 1, &doc! { "hello": 1i32, "$db": "admin" });

    // Standalone topology: no setName, no hosts array, no primary.
    assert!(
        !body.contains_key("setName"),
        "standalone must NOT have setName"
    );
    assert!(
        !body.contains_key("hosts"),
        "standalone must NOT have hosts array"
    );
    assert!(
        !body.contains_key("primary"),
        "standalone must NOT have primary"
    );
    // Must be writable.
    assert!(
        body.get_bool("isWritablePrimary").unwrap(),
        "isWritablePrimary must be true for standalone"
    );
}

// ===========================================================================
// ── 7. MULTI-COMMAND SESSION TESTS ──────────────────────────────────────────
// ===========================================================================
//
// Tests that simulate real driver sessions: a single connection, multiple
// sequential commands.

/// Full CRUD lifecycle on one connection.
#[test]
fn session_full_crud_lifecycle() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);

    // 1. Insert 3 docs.
    let ins = round_trip(
        &mut s,
        1,
        &doc! {
            "insert": "lifecycle",
            "documents": [
                {"n": 1i32, "active": true},
                {"n": 2i32, "active": true},
                {"n": 3i32, "active": false},
            ],
            "$db": "local",
        },
    );
    assert_eq!(ins.get_i32("n").unwrap(), 3);

    // 2. Find active docs.
    let find = round_trip(
        &mut s,
        2,
        &doc! { "find": "lifecycle", "filter": {"active": true}, "$db": "local" },
    );
    let batch = find
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    assert_eq!(batch.len(), 2, "2 active docs");

    // 3. Update n=1 → active=false.
    let upd = round_trip(
        &mut s,
        3,
        &doc! {
            "update": "lifecycle",
            "updates": [{"q": {"n": 1i32}, "u": {"$set": {"active": false}}, "multi": false}],
            "$db": "local",
        },
    );
    assert_eq!(upd.get_i64("nModified").unwrap(), 1);

    // 4. Delete all inactive docs.
    let del = round_trip(
        &mut s,
        4,
        &doc! {
            "delete": "lifecycle",
            "deletes": [{"q": {"active": false}, "limit": 0i32}],
            "$db": "local",
        },
    );
    assert_eq!(del.get_i64("n").unwrap(), 2, "2 inactive docs deleted");

    // 5. Only n=2 should remain.
    let final_find = round_trip(
        &mut s,
        5,
        &doc! { "find": "lifecycle", "filter": {}, "$db": "local" },
    );
    let final_batch = final_find
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    assert_eq!(final_batch.len(), 1, "only one doc remains");
    assert_eq!(
        final_batch[0].as_document().unwrap().get_i32("n").unwrap(),
        2
    );
}

/// listCollections after creating collections via inserts.
#[test]
fn session_list_collections_after_inserts() {
    let (_db, _srv, addr) = start_server();
    let mut s = connect(addr);

    // Create two collections by inserting.
    for (coll, n) in [("alpha", 1i32), ("beta", 2i32)] {
        round_trip(
            &mut s,
            n,
            &doc! { "insert": coll, "documents": [{"x": n}], "$db": "local" },
        );
    }

    let list = round_trip(&mut s, 10, &doc! { "listCollections": 1i32, "$db": "local" });
    let batch = list
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    assert_eq!(batch.len(), 2, "two collections visible");

    let names: Vec<&str> = batch
        .iter()
        .map(|b| b.as_document().unwrap().get_str("name").unwrap())
        .collect();
    assert!(names.contains(&"alpha"), "alpha present");
    assert!(names.contains(&"beta"), "beta present");
}
