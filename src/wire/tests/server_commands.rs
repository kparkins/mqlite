use super::*;
use bson::doc;
use tokio::net::{TcpListener, TcpStream as TokioStream};

/// Return an empty per-connection cursor map for use in unit tests that
/// do not exercise cursor-related functionality.
fn dummy_cursors() -> Arc<std::sync::Mutex<ConnectionCursors>> {
    Arc::new(std::sync::Mutex::new(ConnectionCursors::new()))
}

/// Helper: spin up a loopback TCP pair and return (client, server) streams.
async fn loopback_pair() -> (TokioStream, TokioStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let client = TokioStream::connect(addr).await.unwrap();
    let (server, _) = listener.accept().await.unwrap();
    (client, server)
}

// -----------------------------------------------------------------------
// Framing helpers
// -----------------------------------------------------------------------

#[tokio::test]
async fn read_write_round_trip() {
    let (mut client, mut server) = loopback_pair().await;

    let body = doc! { "ok": 1, "ismaster": true };
    let bytes = OpMsg::build_response(1, 99, &body).unwrap();
    write_message(&mut server, &bytes).await.unwrap();

    let msg = read_message(&mut client).await.unwrap();
    assert_eq!(msg.header.request_id, 1);
    assert_eq!(msg.header.response_to, 99);
    let parsed_body = msg.body().unwrap();
    assert_eq!(parsed_body.get_i32("ok").unwrap(), 1);
}

#[tokio::test]
async fn oversized_message_rejected_on_read() {
    let (mut client, mut server) = loopback_pair().await;

    let claimed = (49usize * 1024 * 1024) as i32;
    let header = MsgHeader {
        message_length: claimed,
        request_id: 1,
        response_to: 0,
        op_code: super::super::protocol::OP_MSG,
    };
    server.write_all(&header.to_bytes()).await.unwrap();

    let err = read_message(&mut client).await.unwrap_err();
    match err {
        crate::error::Error::InvalidWireMessage { detail } => {
            assert!(
                detail.contains("exceeds maximum") || detail.contains("48 MiB"),
                "got: {}",
                detail
            );
        }
        _ => panic!("wrong error type: {:?}", err),
    }
}

// -----------------------------------------------------------------------
// Command dispatch (unit tests — no network)
// -----------------------------------------------------------------------

/// Build a minimal OP_MSG request carrying `body`.
fn make_op_msg_request(request_id: i32, body: &Document) -> Vec<u8> {
    let bson_bytes = bson::to_vec(body).unwrap();
    let total = MsgHeader::SIZE + 4 + 1 + bson_bytes.len();
    let header = MsgHeader {
        message_length: total as i32,
        request_id,
        response_to: 0,
        op_code: super::super::protocol::OP_MSG,
    };
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(&header.to_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes()); // flagBits = 0
    buf.push(0); // Kind-0
    buf.extend_from_slice(&bson_bytes);
    buf
}

/// Build a minimal OP_QUERY request.
fn make_op_query_request(request_id: i32, collection: &str, body: &Document) -> Vec<u8> {
    let bson_bytes = bson::to_vec(body).unwrap();
    let coll_bytes = {
        let mut v = collection.as_bytes().to_vec();
        v.push(0); // null terminator
        v
    };
    // header(16) + flags(4) + coll + skip(4) + nret(4) + doc
    let total = 16 + 4 + coll_bytes.len() + 4 + 4 + bson_bytes.len();
    let header = MsgHeader {
        message_length: total as i32,
        request_id,
        response_to: 0,
        op_code: OP_QUERY,
    };
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(&header.to_bytes());
    buf.extend_from_slice(&0i32.to_le_bytes()); // flags
    buf.extend_from_slice(&coll_bytes);
    buf.extend_from_slice(&0i32.to_le_bytes()); // numberToSkip
    buf.extend_from_slice(&(-1i32).to_le_bytes()); // numberToReturn
    buf.extend_from_slice(&bson_bytes);
    buf
}

#[test]
fn dispatch_op_msg_ping() {
    let state = ServerState::default();
    let req_buf = make_op_msg_request(1, &doc! { "ping": 1, "$db": "admin" });
    let msg = OpMsg::parse(&req_buf).unwrap();
    let resp_bytes =
        dispatch_op_msg(&msg, 10, msg.header.request_id, &state, 1, &dummy_cursors()).unwrap();
    let resp = OpMsg::parse(&resp_bytes).unwrap();
    let body = resp.body().unwrap();
    assert_eq!(body.get_f64("ok").unwrap(), 1.0);
}

#[test]
fn dispatch_op_msg_hello() {
    let state = ServerState::default();
    let req_buf = make_op_msg_request(2, &doc! { "hello": 1, "$db": "admin" });
    let msg = OpMsg::parse(&req_buf).unwrap();
    let resp_bytes =
        dispatch_op_msg(&msg, 11, msg.header.request_id, &state, 1, &dummy_cursors()).unwrap();
    let resp = OpMsg::parse(&resp_bytes).unwrap();
    let body = resp.body().unwrap();
    assert_eq!(body.get_f64("ok").unwrap(), 1.0);
    assert!(body.get_bool("isWritablePrimary").unwrap());
    assert!(body.get_bool("helloOk").unwrap());
    assert_eq!(body.get_i32("maxWireVersion").unwrap(), 21);
    assert_eq!(body.get_i32("minWireVersion").unwrap(), 0);
    // connectionId must be present and match the value passed.
    assert_eq!(body.get_i32("connectionId").unwrap(), 1);
    // topologyVersion must be present with processId and counter=0.
    let tv = body.get_document("topologyVersion").unwrap();
    assert!(tv.contains_key("processId"));
    assert_eq!(tv.get_i64("counter").unwrap(), 0);
}

#[test]
fn dispatch_op_query_ismaster() {
    let state = ServerState::default();
    let req_buf = make_op_query_request(3, "admin.$cmd", &doc! { "ismaster": 1, "helloOk": true });
    let resp_bytes = dispatch_op_query(&req_buf, 12, 3, &state, 2).unwrap();

    // Response must be OP_REPLY (opcode 1).
    let header = MsgHeader::parse(&resp_bytes).unwrap();
    assert_eq!(header.op_code, OP_REPLY);
    assert_eq!(header.response_to, 3);

    // Parse the OP_REPLY body.
    // Layout: header(16) + responseFlags(4) + cursorID(8) + startingFrom(4) + numberReturned(4) + doc
    let doc_start = 16 + 4 + 8 + 4 + 4;
    let doc_size =
        i32::from_le_bytes(resp_bytes[doc_start..doc_start + 4].try_into().unwrap()) as usize;
    let raw =
        bson::RawDocumentBuf::from_bytes(resp_bytes[doc_start..doc_start + doc_size].to_vec())
            .unwrap();
    let body = bson::from_slice::<Document>(raw.as_bytes()).unwrap();
    assert_eq!(body.get_f64("ok").unwrap(), 1.0);
    assert!(body.get_bool("isWritablePrimary").unwrap());
    assert!(body.get_bool("helloOk").unwrap());
    // topologyVersion must be present.
    assert!(body.contains_key("topologyVersion"));
    // connectionId must be present.
    assert!(body.contains_key("connectionId"));
}

#[test]
fn dispatch_op_msg_ismaster() {
    let state = ServerState::default();
    let req_buf = make_op_msg_request(3, &doc! { "ismaster": 1, "$db": "admin" });
    let msg = OpMsg::parse(&req_buf).unwrap();
    let resp_bytes =
        dispatch_op_msg(&msg, 12, msg.header.request_id, &state, 3, &dummy_cursors()).unwrap();
    let resp = OpMsg::parse(&resp_bytes).unwrap();
    let body = resp.body().unwrap();
    assert!(body.get_bool("isWritablePrimary").unwrap());
    assert_eq!(body.get_i32("connectionId").unwrap(), 3);
}

#[test]
fn dispatch_op_msg_build_info() {
    let state = ServerState::default();
    let req_buf = make_op_msg_request(4, &doc! { "buildInfo": 1, "$db": "admin" });
    let msg = OpMsg::parse(&req_buf).unwrap();
    let resp_bytes =
        dispatch_op_msg(&msg, 13, msg.header.request_id, &state, 1, &dummy_cursors()).unwrap();
    let resp = OpMsg::parse(&resp_bytes).unwrap();
    let body = resp.body().unwrap();
    assert_eq!(body.get_f64("ok").unwrap(), 1.0);
    assert!(body.get_str("version").is_ok());
    // modules must be an empty array.
    let modules = body.get_array("modules").unwrap();
    assert!(modules.is_empty());
    // allocator field.
    assert_eq!(body.get_str("allocator").unwrap(), "rust");
    // mqlite: true identity marker.
    assert!(body.get_bool("mqlite").unwrap());
}

#[test]
fn dispatch_op_msg_server_status() {
    let state = ServerState::default();
    let req_buf = make_op_msg_request(5, &doc! { "serverStatus": 1, "$db": "admin" });
    let msg = OpMsg::parse(&req_buf).unwrap();
    let resp_bytes =
        dispatch_op_msg(&msg, 14, msg.header.request_id, &state, 1, &dummy_cursors()).unwrap();
    let resp = OpMsg::parse(&resp_bytes).unwrap();
    let body = resp.body().unwrap();
    assert_eq!(body.get_f64("ok").unwrap(), 1.0);
    // uptime must be non-negative.
    assert!(body.get_i64("uptime").unwrap() >= 0);
    // connections sub-document must be present.
    assert!(body.contains_key("connections"));
    // storageEngine sub-document must be present.
    let se = body.get_document("storageEngine").unwrap();
    assert_eq!(se.get_str("name").unwrap(), "mqlite");
}

#[test]
fn dispatch_op_msg_list_databases() {
    // Insert a document so the database is visible in listDatabases.
    let state = ServerState::default();
    let cursors = dummy_cursors();
    let ins_req = make_op_msg_request(
        5,
        &doc! { "insert": "col", "documents": [{"x": 1i32}], "$db": "testdb" },
    );
    let ins_msg = OpMsg::parse(&ins_req).unwrap();
    dispatch_op_msg(&ins_msg, 14, ins_msg.header.request_id, &state, 1, &cursors).unwrap();

    let req_buf = make_op_msg_request(6, &doc! { "listDatabases": 1, "$db": "admin" });
    let msg = OpMsg::parse(&req_buf).unwrap();
    let resp_bytes =
        dispatch_op_msg(&msg, 15, msg.header.request_id, &state, 1, &dummy_cursors()).unwrap();
    let resp = OpMsg::parse(&resp_bytes).unwrap();
    let body = resp.body().unwrap();
    assert_eq!(body.get_f64("ok").unwrap(), 1.0);
    // After the insert, "testdb" must appear.
    let dbs = body.get_array("databases").unwrap();
    assert!(
        !dbs.is_empty(),
        "at least one database must appear after insert"
    );
    let has_testdb = dbs
        .iter()
        .map(|d| d.as_document().unwrap().get_str("name").unwrap())
        .any(|name| name == "testdb");
    assert!(has_testdb, "testdb must appear in listDatabases");
}

#[test]
fn dispatch_op_msg_unknown_command() {
    let state = ServerState::default();
    // Use $db: "admin" (always allowed) to test CommandNotFound, not Unauthorized.
    let req_buf = make_op_msg_request(7, &doc! { "frobnicate": 1, "$db": "admin" });
    let msg = OpMsg::parse(&req_buf).unwrap();
    let resp_bytes =
        dispatch_op_msg(&msg, 16, msg.header.request_id, &state, 1, &dummy_cursors()).unwrap();
    let resp = OpMsg::parse(&resp_bytes).unwrap();
    let body = resp.body().unwrap();
    assert_eq!(body.get_f64("ok").unwrap(), 0.0);
    assert_eq!(body.get_i32("code").unwrap(), 59);
    assert_eq!(body.get_str("codeName").unwrap(), "CommandNotFound");
}

#[test]
fn dispatch_op_msg_end_sessions_returns_ok() {
    // Drivers send endSessions on close; it must be a no-op returning ok:1.
    let state = ServerState::default();
    let req_buf = make_op_msg_request(
        8,
        &doc! {
            "endSessions": [{ "id": bson::Bson::Binary(bson::Binary {
                subtype: bson::spec::BinarySubtype::Uuid,
                bytes: vec![0u8; 16],
            }) }],
            "$db": "admin",
        },
    );
    let msg = OpMsg::parse(&req_buf).unwrap();
    let resp_bytes =
        dispatch_op_msg(&msg, 18, msg.header.request_id, &state, 1, &dummy_cursors()).unwrap();
    let resp = OpMsg::parse(&resp_bytes).unwrap();
    let body = resp.body().unwrap();
    assert_eq!(body.get_f64("ok").unwrap(), 1.0);
    assert!(!body.contains_key("code"), "endSessions must not error");
}

#[test]
fn end_sessions_handler_returns_ok() {
    let body = handle_end_sessions();
    assert_eq!(body.get_f64("ok").unwrap(), 1.0);
}

// -----------------------------------------------------------------------
// $db field routing (multi-database — any $db is accepted)
// -----------------------------------------------------------------------

#[test]
fn dispatch_op_msg_any_db_is_allowed() {
    // Arbitrary $db values must succeed (no Unauthorized for unknown db).
    let state = ServerState::default();
    for db in &["admin", "local", "mydb", "arbitrarydb", "test"] {
        let req_buf = make_op_msg_request(20, &doc! { "ping": 1, "$db": db });
        let msg = OpMsg::parse(&req_buf).unwrap();
        let resp_bytes =
            dispatch_op_msg(&msg, 40, msg.header.request_id, &state, 1, &dummy_cursors()).unwrap();
        let resp = OpMsg::parse(&resp_bytes).unwrap();
        let body = resp.body().unwrap();
        assert_eq!(
            body.get_f64("ok").unwrap(),
            1.0,
            "$db='{}' should succeed but got: {:?}",
            db,
            body
        );
    }
}

#[test]
fn dispatch_op_msg_db_routes_to_correct_namespace() {
    // Documents inserted with $db: "foo" must not appear in $db: "bar".
    let state = ServerState::default();
    let cursors = dummy_cursors();
    handle_insert(
        &doc! { "insert": "things", "documents": [{"x": 1i32}], "$db": "foo" },
        &state,
    );
    // find in same db — must return 1 doc.
    let find_foo = handle_find(
        &doc! { "find": "things", "filter": {}, "$db": "foo" },
        &state,
        &cursors,
    );
    let batch_foo = find_foo
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    assert_eq!(
        batch_foo.len(),
        1,
        "find in same db must return the document"
    );

    // find in different db — must return 0 docs.
    let find_bar = handle_find(
        &doc! { "find": "things", "filter": {}, "$db": "bar" },
        &state,
        &cursors,
    );
    let batch_bar = find_bar
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    assert!(
        batch_bar.is_empty(),
        "find in different db must return no documents"
    );
}

#[test]
fn dispatch_op_query_any_db_is_allowed() {
    // OP_QUERY from any database must succeed (isMaster handshake).
    let state = ServerState::default();
    let req_buf = make_op_query_request(30, "anydb.$cmd", &doc! { "ismaster": 1 });
    let resp_bytes = dispatch_op_query(&req_buf, 50, 30, &state, 1).unwrap();
    // Parse OP_REPLY body.
    let doc_start = 16 + 4 + 8 + 4 + 4;
    let doc_size =
        i32::from_le_bytes(resp_bytes[doc_start..doc_start + 4].try_into().unwrap()) as usize;
    let raw =
        bson::RawDocumentBuf::from_bytes(resp_bytes[doc_start..doc_start + doc_size].to_vec())
            .unwrap();
    let body = bson::from_slice::<Document>(raw.as_bytes()).unwrap();
    // Must succeed — any $db is valid in multi-database mode.
    assert_eq!(
        body.get_f64("ok").unwrap(),
        1.0,
        "OP_QUERY from any db must succeed, got: {:?}",
        body
    );
}

// -----------------------------------------------------------------------
// ConnectionCursors unit tests
// -----------------------------------------------------------------------

#[test]
fn connection_cursors_new_is_empty() {
    let state = ConnectionCursors::new();
    assert_eq!(state.len(), 0);
}

#[test]
fn connection_cursors_store_and_remove() {
    let mut state = ConnectionCursors::new();
    let cursor = crate::Cursor::<bson::Document>::empty();
    let id = state.store(cursor);
    assert_eq!(id, 1, "first cursor should get ID 1");
    assert_eq!(state.len(), 1);

    // Removing an existing cursor returns Some.
    assert!(state.remove(id).is_some());
    assert_eq!(state.len(), 0);

    // Removing again returns None.
    assert!(state.remove(id).is_none());
}

#[test]
fn connection_cursors_sequential_ids() {
    let mut state = ConnectionCursors::new();
    let id1 = state.store(crate::Cursor::<bson::Document>::empty());
    let id2 = state.store(crate::Cursor::<bson::Document>::empty());
    let id3 = state.store(crate::Cursor::<bson::Document>::empty());
    assert_eq!(id1, 1);
    assert_eq!(id2, 2);
    assert_eq!(id3, 3);
}

#[test]
fn connection_cursors_evict_zero_timeout_removes_all() {
    let mut state = ConnectionCursors::new();
    state.store(crate::Cursor::<bson::Document>::empty());
    state.store(crate::Cursor::<bson::Document>::empty());
    assert_eq!(state.len(), 2);

    // Zero timeout: every cursor is "idle".
    let evicted = state.evict_idle(std::time::Duration::from_secs(0));
    assert_eq!(evicted, 2);
    assert_eq!(state.len(), 0);
}

#[test]
fn connection_cursors_evict_long_timeout_keeps_all() {
    let mut state = ConnectionCursors::new();
    state.store(crate::Cursor::<bson::Document>::empty());
    state.store(crate::Cursor::<bson::Document>::empty());

    // Very long timeout: nothing evicted.
    let evicted = state.evict_idle(std::time::Duration::from_secs(3600));
    assert_eq!(evicted, 0);
    assert_eq!(state.len(), 2);
}

#[test]
fn connection_cursors_get_mut_existing_and_missing() {
    let mut state = ConnectionCursors::new();
    let id = state.store(crate::Cursor::<bson::Document>::empty());
    assert!(state.get_mut(id).is_some());
    assert!(state.get_mut(999).is_none());
}

// -----------------------------------------------------------------------
// hello response — spec compliance
// -----------------------------------------------------------------------

#[test]
fn hello_topology_version_fields() {
    // topologyVersion must have a processId (ObjectId) and counter (Int64 = 0).
    let state = ServerState::default();
    let body = handle_hello(&state, 42);

    let tv = body.get_document("topologyVersion").unwrap();
    // processId must be an ObjectId.
    assert!(
        matches!(tv.get("processId"), Some(bson::Bson::ObjectId(_))),
        "processId should be an ObjectId, got: {:?}",
        tv.get("processId")
    );
    assert_eq!(tv.get_i64("counter").unwrap(), 0);
    // connectionId must match the argument.
    assert_eq!(body.get_i32("connectionId").unwrap(), 42);
}

#[test]
fn hello_topology_process_id_stable() {
    // Two calls on the same ServerState must return the same processId.
    let state = ServerState::default();
    let body1 = handle_hello(&state, 1);
    let body2 = handle_hello(&state, 2);
    let pid1 = body1
        .get_document("topologyVersion")
        .unwrap()
        .get("processId")
        .cloned();
    let pid2 = body2
        .get_document("topologyVersion")
        .unwrap()
        .get("processId")
        .cloned();
    assert_eq!(
        pid1, pid2,
        "topology processId should be stable across calls"
    );
}

#[test]
fn hello_connection_ids_unique_per_connection() {
    // Two connections on the same ServerState must get different connectionIds.
    let state = ServerState::default();
    let id1 = state.next_conn_id();
    let id2 = state.next_conn_id();
    assert_ne!(id1, id2);
}

// -----------------------------------------------------------------------
// buildInfo — spec compliance
// -----------------------------------------------------------------------

#[test]
fn build_info_required_fields() {
    let body = handle_build_info();
    assert!(body.get_str("version").is_ok());
    assert!(body.get_str("gitVersion").is_ok());
    assert_eq!(body.get_str("allocator").unwrap(), "rust");
    assert!(body.get_bool("mqlite").unwrap());
    assert!(body.get_array("modules").unwrap().is_empty());
    assert_eq!(body.get_f64("ok").unwrap(), 1.0);
}

// -----------------------------------------------------------------------
// serverStatus — spec compliance
// -----------------------------------------------------------------------

#[test]
fn server_status_required_fields() {
    let state = ServerState::default();
    let body = handle_server_status(&state);
    assert_eq!(body.get_f64("ok").unwrap(), 1.0);
    assert!(body.get_i64("uptime").unwrap() >= 0);
    assert!(body.get_i64("uptimeMillis").unwrap() >= 0);
    assert!(body.contains_key("connections"));
    assert!(body.contains_key("storageEngine"));
    assert!(body.contains_key("localTime"));
}

// -----------------------------------------------------------------------
// listDatabases — spec compliance (multi-database)
// -----------------------------------------------------------------------

#[test]
fn list_databases_empty_when_no_collections() {
    // Empty server — no collections yet — must report no databases.
    let state = ServerState::default();
    let body = handle_list_databases(&state);
    assert_eq!(body.get_f64("ok").unwrap(), 1.0);
    let dbs = body.get_array("databases").unwrap();
    assert!(dbs.is_empty(), "empty server must report no databases");
}

#[test]
fn list_databases_shows_db_after_insert() {
    // After inserting into "mydb", listDatabases must include "mydb".
    let state = ServerState::default();
    handle_insert(
        &doc! { "insert": "col", "documents": [{"x": 1i32}], "$db": "mydb" },
        &state,
    );
    let body = handle_list_databases(&state);
    assert_eq!(body.get_f64("ok").unwrap(), 1.0);
    let dbs = body.get_array("databases").unwrap();
    assert_eq!(dbs.len(), 1);
    let db_doc = dbs[0].as_document().unwrap();
    assert_eq!(db_doc.get_str("name").unwrap(), "mydb");
    assert!(
        db_doc.contains_key("sizeOnDisk"),
        "database entry must have sizeOnDisk"
    );
    assert!(
        db_doc.contains_key("empty"),
        "database entry must have empty"
    );
}

#[test]
fn list_databases_multiple_databases() {
    // Multiple $db namespaces are each reported as a separate database.
    let state = ServerState::default();
    handle_insert(
        &doc! { "insert": "a", "documents": [{"x": 1i32}], "$db": "alpha" },
        &state,
    );
    handle_insert(
        &doc! { "insert": "b", "documents": [{"y": 2i32}], "$db": "beta" },
        &state,
    );
    let body = handle_list_databases(&state);
    let dbs = body.get_array("databases").unwrap();
    assert_eq!(dbs.len(), 2);
    let names: Vec<&str> = dbs
        .iter()
        .map(|d| d.as_document().unwrap().get_str("name").unwrap())
        .collect();
    assert!(names.contains(&"alpha"));
    assert!(names.contains(&"beta"));
}

#[test]
fn list_databases_same_db_different_collections_counted_once() {
    // Two collections in "shared" — should appear as one entry.
    let state = ServerState::default();
    handle_insert(
        &doc! { "insert": "c1", "documents": [{"v": 1i32}], "$db": "shared" },
        &state,
    );
    handle_insert(
        &doc! { "insert": "c2", "documents": [{"v": 2i32}], "$db": "shared" },
        &state,
    );
    let body = handle_list_databases(&state);
    let dbs = body.get_array("databases").unwrap();
    assert_eq!(dbs.len(), 1);
    assert_eq!(
        dbs[0].as_document().unwrap().get_str("name").unwrap(),
        "shared"
    );
}

// -----------------------------------------------------------------------
// Integration: WireProtocol::bind + full TCP round-trip
// -----------------------------------------------------------------------

#[test]
fn wire_protocol_bind_and_ping() {
    // Pick a random port to avoid conflicts.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let _tempdir = tempfile::TempDir::new().expect("tempdir");
    let client = Client::open(_tempdir.path().join("db.mqlite")).expect("open");
    let _server = WireProtocol::bind(&client, &addr.to_string()).unwrap();

    let mut client = std::net::TcpStream::connect(addr).unwrap();
    client
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();

    // Send an OP_MSG ping.
    let body = bson::to_vec(&doc! { "ping": 1, "$db": "admin" }).unwrap();
    let total = (MsgHeader::SIZE + 4 + 1 + body.len()) as i32;
    let header = MsgHeader {
        message_length: total,
        request_id: 42,
        response_to: 0,
        op_code: super::super::protocol::OP_MSG,
    };
    use std::io::{Read, Write};
    client.write_all(&header.to_bytes()).unwrap();
    client.write_all(&0u32.to_le_bytes()).unwrap(); // flagBits
    client.write_all(&[0u8]).unwrap(); // Kind-0
    client.write_all(&body).unwrap();

    // Read response.
    let mut hbuf = [0u8; MsgHeader::SIZE];
    client.read_exact(&mut hbuf).unwrap();
    let resp_header = MsgHeader::parse(&hbuf).unwrap();
    assert_eq!(resp_header.response_to, 42);
    assert_eq!(resp_header.op_code, super::super::protocol::OP_MSG);

    let remaining = resp_header.message_length as usize - MsgHeader::SIZE;
    let mut rest = vec![0u8; remaining];
    client.read_exact(&mut rest).unwrap();

    let mut full = hbuf.to_vec();
    full.extend_from_slice(&rest);
    let resp_msg = OpMsg::parse(&full).unwrap();
    let resp_body = resp_msg.body().unwrap();
    assert_eq!(resp_body.get_f64("ok").unwrap(), 1.0);
}

#[test]
fn wire_protocol_op_query_ismaster_round_trip() {
    // Pick a random port.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let _tempdir = tempfile::TempDir::new().expect("tempdir");
    let client = Client::open(_tempdir.path().join("db.mqlite")).expect("open");
    let _server = WireProtocol::bind(&client, &addr.to_string()).unwrap();

    let mut client = std::net::TcpStream::connect(addr).unwrap();
    client
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();

    // Send an OP_QUERY isMaster (like pymongo does on initial connect).
    let body_doc = doc! { "ismaster": 1, "helloOk": true };
    let body_bson = bson::to_vec(&body_doc).unwrap();
    let coll = b"admin.$cmd\x00";
    let total = (16 + 4 + coll.len() + 4 + 4 + body_bson.len()) as i32;
    let header = MsgHeader {
        message_length: total,
        request_id: 7,
        response_to: 0,
        op_code: OP_QUERY,
    };
    use std::io::{Read, Write};
    client.write_all(&header.to_bytes()).unwrap();
    client.write_all(&0i32.to_le_bytes()).unwrap(); // flags
    client.write_all(coll).unwrap();
    client.write_all(&0i32.to_le_bytes()).unwrap(); // numberToSkip
    client.write_all(&(-1i32).to_le_bytes()).unwrap(); // numberToReturn
    client.write_all(&body_bson).unwrap();

    // Read OP_REPLY response.
    let mut hbuf = [0u8; MsgHeader::SIZE];
    client.read_exact(&mut hbuf).unwrap();
    let resp_header = MsgHeader::parse(&hbuf).unwrap();
    assert_eq!(resp_header.op_code, OP_REPLY);
    assert_eq!(resp_header.response_to, 7);

    // Skip responseFlags(4) + cursorID(8) + startingFrom(4) + numberReturned(4) = 20 bytes
    let remaining = resp_header.message_length as usize - 16;
    let mut rest = vec![0u8; remaining];
    client.read_exact(&mut rest).unwrap();

    // BSON doc starts at offset 20 within rest.
    let doc_start = 20;
    let doc_size = i32::from_le_bytes(rest[doc_start..doc_start + 4].try_into().unwrap()) as usize;
    let raw =
        bson::RawDocumentBuf::from_bytes(rest[doc_start..doc_start + doc_size].to_vec()).unwrap();
    let body = bson::from_slice::<Document>(raw.as_bytes()).unwrap();

    assert_eq!(body.get_f64("ok").unwrap(), 1.0);
    assert!(body.get_bool("isWritablePrimary").unwrap());
    assert!(body.get_bool("helloOk").unwrap());
    assert_eq!(body.get_i32("maxWireVersion").unwrap(), 21);
    // topologyVersion and connectionId must be present in OP_QUERY response too.
    assert!(body.contains_key("topologyVersion"));
    assert!(body.contains_key("connectionId"));
}

// -----------------------------------------------------------------------
// serverStatus — integration via WireProtocol bind
// -----------------------------------------------------------------------

#[test]
fn wire_protocol_server_status_round_trip() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let _tempdir = tempfile::TempDir::new().expect("tempdir");
    let client = Client::open(_tempdir.path().join("db.mqlite")).expect("open");
    let _server = WireProtocol::bind(&client, &addr.to_string()).unwrap();

    let mut client = std::net::TcpStream::connect(addr).unwrap();
    client
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();

    let body_bson = bson::to_vec(&doc! { "serverStatus": 1, "$db": "admin" }).unwrap();
    let total = (MsgHeader::SIZE + 4 + 1 + body_bson.len()) as i32;
    let header = MsgHeader {
        message_length: total,
        request_id: 10,
        response_to: 0,
        op_code: super::super::protocol::OP_MSG,
    };
    use std::io::{Read, Write};
    client.write_all(&header.to_bytes()).unwrap();
    client.write_all(&0u32.to_le_bytes()).unwrap(); // flagBits
    client.write_all(&[0u8]).unwrap(); // Kind-0
    client.write_all(&body_bson).unwrap();

    let mut hbuf = [0u8; MsgHeader::SIZE];
    client.read_exact(&mut hbuf).unwrap();
    let resp_header = MsgHeader::parse(&hbuf).unwrap();
    let remaining = resp_header.message_length as usize - MsgHeader::SIZE;
    let mut rest = vec![0u8; remaining];
    client.read_exact(&mut rest).unwrap();

    let mut full = hbuf.to_vec();
    full.extend_from_slice(&rest);
    let resp_msg = OpMsg::parse(&full).unwrap();
    let resp_body = resp_msg.body().unwrap();
    assert_eq!(resp_body.get_f64("ok").unwrap(), 1.0);
    assert!(resp_body.get_i64("uptime").unwrap() >= 0);
}

// -----------------------------------------------------------------------
// CRUD command handler unit tests
// -----------------------------------------------------------------------

// ---- insert ----

#[test]
fn insert_single_document_returns_n_1() {
    let state = ServerState::default();
    let body = doc! {
        "insert": "users",
        "documents": [{"name": "Alice", "age": 30i32}],
        "$db": "local",
    };
    let result = handle_insert(&body, &state);
    assert_eq!(result.get_f64("ok").unwrap(), 1.0, "{result:?}");
    assert_eq!(result.get_i32("n").unwrap(), 1);
    assert!(!result.contains_key("writeErrors"));
}

#[test]
fn insert_many_documents_ordered() {
    let state = ServerState::default();
    let body = doc! {
        "insert": "items",
        "documents": [
            {"x": 1i32},
            {"x": 2i32},
            {"x": 3i32},
        ],
        "ordered": true,
        "$db": "local",
    };
    let result = handle_insert(&body, &state);
    assert_eq!(result.get_f64("ok").unwrap(), 1.0);
    assert_eq!(result.get_i32("n").unwrap(), 3);
}

#[test]
fn insert_empty_documents_returns_n_0() {
    let state = ServerState::default();
    let body = doc! {
        "insert": "empty",
        "documents": [],
        "$db": "local",
    };
    let result = handle_insert(&body, &state);
    assert_eq!(result.get_f64("ok").unwrap(), 1.0);
    assert_eq!(result.get_i32("n").unwrap(), 0);
}

#[test]
fn insert_collation_returns_bad_value() {
    let state = ServerState::default();
    let body = doc! {
        "insert": "col",
        "documents": [],
        "collation": {"locale": "en"},
        "$db": "local",
    };
    let result = handle_insert(&body, &state);
    assert_eq!(result.get_f64("ok").unwrap(), 0.0);
    assert_eq!(result.get_i32("code").unwrap(), 2); // BadValue
}

/// Insert via Kind-1 document sequence (pymongo bulk path).
#[test]
fn insert_via_doc_sequence_merged_into_body() {
    let state = ServerState::default();
    // Simulate what happens after merge_doc_sequences_into_body:
    // the Kind-1 "documents" section has been merged into the body.
    let body = doc! {
        "insert": "merged",
        "documents": [{"a": 1i32}, {"a": 2i32}],
        "$db": "local",
    };
    let result = handle_insert(&body, &state);
    assert_eq!(result.get_f64("ok").unwrap(), 1.0);
    assert_eq!(result.get_i32("n").unwrap(), 2);
}

// ---- find ----

#[test]
fn find_empty_collection_returns_empty_first_batch() {
    let state = ServerState::default();
    let cursors = dummy_cursors();
    let body = doc! {
        "find": "nonexistent",
        "filter": {},
        "$db": "local",
    };
    let result = handle_find(&body, &state, &cursors);
    assert_eq!(result.get_f64("ok").unwrap(), 1.0, "{result:?}");
    let cursor_doc = result.get_document("cursor").unwrap();
    let first_batch = cursor_doc.get_array("firstBatch").unwrap();
    assert!(
        first_batch.is_empty(),
        "empty collection must return firstBatch=[]"
    );
    assert_eq!(
        cursor_doc.get_i64("id").unwrap(),
        0,
        "cursor id must be 0 when exhausted"
    );
    assert!(cursor_doc.get_str("ns").is_ok(), "ns field must be present");
}

#[test]
fn find_returns_inserted_documents() {
    let state = ServerState::default();
    let cursors = dummy_cursors();
    // Insert 3 docs first.
    let insert_body = doc! {
        "insert": "findtest",
        "documents": [{"v": 1i32}, {"v": 2i32}, {"v": 3i32}],
        "$db": "local",
    };
    let ins_res = handle_insert(&insert_body, &state);
    assert_eq!(ins_res.get_f64("ok").unwrap(), 1.0);

    let find_body = doc! {
        "find": "findtest",
        "filter": {},
        "$db": "local",
    };
    let result = handle_find(&find_body, &state, &cursors);
    assert_eq!(result.get_f64("ok").unwrap(), 1.0);
    let cursor_doc = result.get_document("cursor").unwrap();
    let first_batch = cursor_doc.get_array("firstBatch").unwrap();
    assert_eq!(first_batch.len(), 3);
    // cursor exhausted — no server-side cursor needed
    assert_eq!(cursor_doc.get_i64("id").unwrap(), 0);
}

#[test]
fn find_with_filter_returns_matching_docs() {
    let state = ServerState::default();
    let cursors = dummy_cursors();
    let insert_body = doc! {
        "insert": "filtercoll",
        "documents": [
            {"status": "active", "n": 1i32},
            {"status": "inactive", "n": 2i32},
            {"status": "active", "n": 3i32},
        ],
        "$db": "local",
    };
    handle_insert(&insert_body, &state);

    let find_body = doc! {
        "find": "filtercoll",
        "filter": {"status": "active"},
        "$db": "local",
    };
    let result = handle_find(&find_body, &state, &cursors);
    assert_eq!(result.get_f64("ok").unwrap(), 1.0);
    let cursor_doc = result.get_document("cursor").unwrap();
    assert_eq!(cursor_doc.get_array("firstBatch").unwrap().len(), 2);
}

#[test]
fn find_batch_size_creates_server_side_cursor() {
    let state = ServerState::default();
    let cursors = dummy_cursors();
    // Insert 5 documents.
    let insert_body = doc! {
        "insert": "batchcoll",
        "documents": [
            {"i": 0i32}, {"i": 1i32}, {"i": 2i32},
            {"i": 3i32}, {"i": 4i32},
        ],
        "$db": "local",
    };
    handle_insert(&insert_body, &state);

    // Request only 2 per batch.
    let find_body = doc! {
        "find": "batchcoll",
        "filter": {},
        "batchSize": 2i32,
        "$db": "local",
    };
    let result = handle_find(&find_body, &state, &cursors);
    assert_eq!(result.get_f64("ok").unwrap(), 1.0);
    let cursor_doc = result.get_document("cursor").unwrap();
    let first_batch = cursor_doc.get_array("firstBatch").unwrap();
    assert_eq!(
        first_batch.len(),
        2,
        "firstBatch must have exactly batchSize docs"
    );
    let cursor_id = cursor_doc.get_i64("id").unwrap();
    assert_ne!(
        cursor_id, 0,
        "cursor id must be non-zero when more docs remain"
    );
    // The server-side cursor should be stored.
    assert_eq!(cursors.lock().unwrap().len(), 1);
}

#[test]
fn find_collation_returns_bad_value() {
    let state = ServerState::default();
    let cursors = dummy_cursors();
    let body = doc! {
        "find": "col",
        "filter": {},
        "collation": {"locale": "en"},
        "$db": "local",
    };
    let result = handle_find(&body, &state, &cursors);
    assert_eq!(result.get_f64("ok").unwrap(), 0.0);
    assert_eq!(result.get_i32("code").unwrap(), 2);
}

// ---- update ----

#[test]
fn update_one_modifies_single_document() {
    let state = ServerState::default();
    let cursors = dummy_cursors();
    // Seed.
    handle_insert(
        &doc! { "insert": "updcoll", "documents": [{"k": "a", "v": 1i32}, {"k": "a", "v": 2i32}], "$db": "local" },
        &state,
    );

    let body = doc! {
        "update": "updcoll",
        "updates": [{
            "q": {"k": "a"},
            "u": {"$set": {"v": 99i32}},
            "multi": false,
        }],
        "$db": "local",
    };
    let result = handle_update(&body, &state);
    assert_eq!(result.get_f64("ok").unwrap(), 1.0, "{result:?}");
    assert_eq!(result.get_i64("n").unwrap(), 1);
    assert_eq!(result.get_i64("nModified").unwrap(), 1);

    // Verify only one was modified.
    let find_res = handle_find(
        &doc! { "find": "updcoll", "filter": {"v": 99i32}, "$db": "local" },
        &state,
        &cursors,
    );
    let batch = find_res
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    assert_eq!(batch.len(), 1);
}

#[test]
fn update_many_modifies_all_matching() {
    let state = ServerState::default();
    handle_insert(
        &doc! { "insert": "multcoll", "documents": [{"x": 1i32}, {"x": 1i32}, {"x": 2i32}], "$db": "local" },
        &state,
    );

    let body = doc! {
        "update": "multcoll",
        "updates": [{
            "q": {"x": 1i32},
            "u": {"$set": {"x": 10i32}},
            "multi": true,
        }],
        "$db": "local",
    };
    let result = handle_update(&body, &state);
    assert_eq!(result.get_f64("ok").unwrap(), 1.0);
    assert_eq!(result.get_i64("n").unwrap(), 2);
    assert_eq!(result.get_i64("nModified").unwrap(), 2);
}

#[test]
fn update_with_upsert_inserts_new_document() {
    let state = ServerState::default();
    let body = doc! {
        "update": "upsertcoll",
        "updates": [{
            "q": {"_id": "new-id"},
            "u": {"$set": {"created": true}},
            "upsert": true,
        }],
        "$db": "local",
    };
    let result = handle_update(&body, &state);
    assert_eq!(result.get_f64("ok").unwrap(), 1.0);
    // Upserted array must contain the new document id.
    let upserted = result.get_array("upserted").unwrap();
    assert_eq!(upserted.len(), 1);
    let upsert_entry = upserted[0].as_document().unwrap();
    assert_eq!(upsert_entry.get_i32("index").unwrap(), 0);
    assert!(upsert_entry.contains_key("_id"));
}

#[test]
fn update_collation_returns_bad_value() {
    let state = ServerState::default();
    let body = doc! {
        "update": "col",
        "updates": [{"q": {}, "u": {"$set": {"x": 1i32}}}],
        "collation": {"locale": "en"},
        "$db": "local",
    };
    let result = handle_update(&body, &state);
    assert_eq!(result.get_f64("ok").unwrap(), 0.0);
    assert_eq!(result.get_i32("code").unwrap(), 2);
}

#[test]
fn update_with_array_filters() {
    let state = ServerState::default();
    let cursors = dummy_cursors();
    handle_insert(
        &doc! { "insert": "afcoll", "documents": [{"_id": 1i32, "grades": [95i32, 102i32, 90i32]}], "$db": "local" },
        &state,
    );

    let body = doc! {
        "update": "afcoll",
        "updates": [{
            "q": {"_id": 1i32},
            "u": {"$set": {"grades.$[elem]": 100i32}},
            "arrayFilters": [{"elem": {"$gte": 100i32}}],
        }],
        "$db": "local",
    };
    let result = handle_update(&body, &state);
    assert_eq!(result.get_f64("ok").unwrap(), 1.0, "{result:?}");
    assert_eq!(result.get_i64("nModified").unwrap(), 1);

    let find_res = handle_find(
        &doc! { "find": "afcoll", "filter": {"_id": 1i32}, "$db": "local" },
        &state,
        &cursors,
    );
    let batch = find_res
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    let grades = batch[0].as_document().unwrap().get_array("grades").unwrap();
    assert_eq!(grades[1].as_i32().unwrap(), 100);
    assert_eq!(grades[2].as_i32().unwrap(), 90);
}

#[test]
fn update_with_pipeline_u_field() {
    let state = ServerState::default();
    let cursors = dummy_cursors();
    handle_insert(
        &doc! { "insert": "pipecoll", "documents": [{"_id": 1i32, "price": 4i32, "qty": 3i32}], "$db": "local" },
        &state,
    );

    let body = doc! {
        "update": "pipecoll",
        "updates": [{
            "q": {"_id": 1i32},
            "u": [{"$set": {"total": {"$multiply": ["$price", "$qty"]}}}],
        }],
        "$db": "local",
    };
    let result = handle_update(&body, &state);
    assert_eq!(result.get_f64("ok").unwrap(), 1.0, "{result:?}");
    assert_eq!(result.get_i64("nModified").unwrap(), 1);

    let find_res = handle_find(
        &doc! { "find": "pipecoll", "filter": {"_id": 1i32}, "$db": "local" },
        &state,
        &cursors,
    );
    let batch = find_res
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    assert_eq!(batch[0].as_document().unwrap().get_i32("total").unwrap(), 12);
}

// ---- delete ----

#[test]
fn delete_one_removes_single_document() {
    let state = ServerState::default();
    let cursors = dummy_cursors();
    handle_insert(
        &doc! { "insert": "delcoll", "documents": [{"k": 1i32}, {"k": 1i32}, {"k": 2i32}], "$db": "local" },
        &state,
    );

    let body = doc! {
        "delete": "delcoll",
        "deletes": [{ "q": {"k": 1i32}, "limit": 1i32 }],
        "$db": "local",
    };
    let result = handle_delete(&body, &state);
    assert_eq!(result.get_f64("ok").unwrap(), 1.0, "{result:?}");
    assert_eq!(result.get_i64("n").unwrap(), 1);

    // Two docs with k=1 were inserted; one remains.
    let find_res = handle_find(
        &doc! { "find": "delcoll", "filter": {"k": 1i32}, "$db": "local" },
        &state,
        &cursors,
    );
    let batch = find_res
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    assert_eq!(batch.len(), 1);
}

#[test]
fn delete_many_removes_all_matching() {
    let state = ServerState::default();
    let cursors = dummy_cursors();
    handle_insert(
        &doc! { "insert": "delmanycoll", "documents": [{"t": "x"}, {"t": "x"}, {"t": "y"}], "$db": "local" },
        &state,
    );

    let body = doc! {
        "delete": "delmanycoll",
        "deletes": [{ "q": {"t": "x"}, "limit": 0i32 }],
        "$db": "local",
    };
    let result = handle_delete(&body, &state);
    assert_eq!(result.get_f64("ok").unwrap(), 1.0);
    assert_eq!(result.get_i64("n").unwrap(), 2);

    // Only doc with t=y remains.
    let find_res = handle_find(
        &doc! { "find": "delmanycoll", "filter": {}, "$db": "local" },
        &state,
        &cursors,
    );
    let batch = find_res
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    assert_eq!(batch.len(), 1);
}

#[test]
fn delete_collation_returns_bad_value() {
    let state = ServerState::default();
    let body = doc! {
        "delete": "col",
        "deletes": [{"q": {}, "limit": 1i32}],
        "collation": {"locale": "en"},
        "$db": "local",
    };
    let result = handle_delete(&body, &state);
    assert_eq!(result.get_f64("ok").unwrap(), 0.0);
    assert_eq!(result.get_i32("code").unwrap(), 2);
}

// ---- findAndModify ----

#[test]
fn find_and_modify_update_returns_original_doc() {
    let state = ServerState::default();
    handle_insert(
        &doc! { "insert": "famcoll", "documents": [{"name": "Alice", "score": 10i32}], "$db": "local" },
        &state,
    );

    let body = doc! {
        "findandmodify": "famcoll",
        "query": {"name": "Alice"},
        "update": {"$set": {"score": 99i32}},
        "new": false,
        "$db": "local",
    };
    let result = handle_find_and_modify(&body, &state);
    assert_eq!(result.get_f64("ok").unwrap(), 1.0, "{result:?}");
    // Response must use 'value' not 'document'.
    assert!(
        result.contains_key("value"),
        "response must use 'value' field"
    );
    assert!(
        !result.contains_key("document"),
        "response must NOT use 'document' field"
    );
    let value = result.get_document("value").unwrap();
    assert_eq!(value.get_str("name").unwrap(), "Alice");
    // Original score before update.
    assert_eq!(value.get_i32("score").unwrap(), 10);
    let leo = result.get_document("lastErrorObject").unwrap();
    assert_eq!(leo.get_i32("n").unwrap(), 1);
}

#[test]
fn find_and_modify_update_new_true_returns_updated_doc() {
    let state = ServerState::default();
    handle_insert(
        &doc! { "insert": "famnewcoll", "documents": [{"v": 1i32}], "$db": "local" },
        &state,
    );

    let body = doc! {
        "findandmodify": "famnewcoll",
        "query": {"v": 1i32},
        "update": {"$set": {"v": 2i32}},
        "new": true,
        "$db": "local",
    };
    let result = handle_find_and_modify(&body, &state);
    assert_eq!(result.get_f64("ok").unwrap(), 1.0);
    let value = result.get_document("value").unwrap();
    assert_eq!(value.get_i32("v").unwrap(), 2); // post-update
}

#[test]
fn find_and_modify_with_array_filters() {
    let state = ServerState::default();
    handle_insert(
        &doc! { "insert": "famaf", "documents": [{"_id": 1i32, "grades": [80i32, 90i32]}], "$db": "local" },
        &state,
    );

    let body = doc! {
        "findandmodify": "famaf",
        "query": {"_id": 1i32},
        "update": {"$set": {"grades.$[hi]": 100i32}},
        "arrayFilters": [{"hi": {"$gte": 85i32}}],
        "new": true,
        "$db": "local",
    };
    let result = handle_find_and_modify(&body, &state);
    assert_eq!(result.get_f64("ok").unwrap(), 1.0, "{result:?}");
    let value = result.get_document("value").unwrap();
    let grades = value.get_array("grades").unwrap();
    assert_eq!(grades[0].as_i32().unwrap(), 80);
    assert_eq!(grades[1].as_i32().unwrap(), 100);
}

#[test]
fn find_and_modify_no_match_returns_null_value() {
    let state = ServerState::default();
    let body = doc! {
        "findandmodify": "emptyfamcoll",
        "query": {"nonexistent": true},
        "update": {"$set": {"x": 1i32}},
        "$db": "local",
    };
    let result = handle_find_and_modify(&body, &state);
    assert_eq!(result.get_f64("ok").unwrap(), 1.0);
    assert_eq!(
        result.get("value"),
        Some(&bson::Bson::Null),
        "value must be null when no doc matches"
    );
    let leo = result.get_document("lastErrorObject").unwrap();
    assert_eq!(leo.get_i32("n").unwrap(), 0);
}

#[test]
fn find_and_modify_remove_true_deletes_and_returns_doc() {
    let state = ServerState::default();
    let cursors = dummy_cursors();
    handle_insert(
        &doc! { "insert": "famremcoll", "documents": [{"tag": "del", "val": 42i32}], "$db": "local" },
        &state,
    );

    let body = doc! {
        "findandmodify": "famremcoll",
        "query": {"tag": "del"},
        "remove": true,
        "$db": "local",
    };
    let result = handle_find_and_modify(&body, &state);
    assert_eq!(result.get_f64("ok").unwrap(), 1.0);
    let value = result.get_document("value").unwrap();
    assert_eq!(value.get_i32("val").unwrap(), 42);

    // Verify the document is gone.
    let find_res = handle_find(
        &doc! { "find": "famremcoll", "filter": {}, "$db": "local" },
        &state,
        &cursors,
    );
    let batch = find_res
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    assert!(batch.is_empty());
}

#[test]
fn find_and_modify_collation_returns_bad_value() {
    let state = ServerState::default();
    let body = doc! {
        "findandmodify": "col",
        "query": {},
        "update": {"$set": {"x": 1i32}},
        "collation": {"locale": "en"},
        "$db": "local",
    };
    let result = handle_find_and_modify(&body, &state);
    assert_eq!(result.get_f64("ok").unwrap(), 0.0);
    assert_eq!(result.get_i32("code").unwrap(), 2);
}

// ---- CRUD via full OP_MSG dispatch ----

/// End-to-end dispatch test: insert then find through the wire framing layer.
#[test]
fn dispatch_op_msg_insert_and_find() {
    let state = ServerState::default();
    let cursors = dummy_cursors();

    // Insert
    let insert_req = make_op_msg_request(
        100,
        &doc! { "insert": "disp_coll", "documents": [{"hello": "world"}], "$db": "local" },
    );
    let msg = OpMsg::parse(&insert_req).unwrap();
    let resp_bytes =
        dispatch_op_msg(&msg, 200, msg.header.request_id, &state, 1, &cursors).unwrap();
    let resp = OpMsg::parse(&resp_bytes).unwrap();
    let body = resp.body().unwrap();
    assert_eq!(body.get_f64("ok").unwrap(), 1.0);
    assert_eq!(body.get_i32("n").unwrap(), 1);

    // Find
    let find_req = make_op_msg_request(
        101,
        &doc! { "find": "disp_coll", "filter": {}, "$db": "local" },
    );
    let msg = OpMsg::parse(&find_req).unwrap();
    let resp_bytes =
        dispatch_op_msg(&msg, 201, msg.header.request_id, &state, 1, &cursors).unwrap();
    let resp = OpMsg::parse(&resp_bytes).unwrap();
    let body = resp.body().unwrap();
    assert_eq!(body.get_f64("ok").unwrap(), 1.0);
    let cursor_doc = body.get_document("cursor").unwrap();
    assert_eq!(cursor_doc.get_array("firstBatch").unwrap().len(), 1);
    assert_eq!(cursor_doc.get_i64("id").unwrap(), 0);
    assert!(cursor_doc.get_str("ns").unwrap().contains("disp_coll"));
}

/// Verify merge_doc_sequences_into_body works for the pymongo insert path.
#[test]
fn merge_doc_sequences_merges_kind1_documents() {
    let body = doc! { "insert": "coll", "$db": "local" };
    let docs = vec![doc! { "a": 1i32 }, doc! { "a": 2i32 }];
    let sections = vec![
        Section::Body(body.clone()),
        Section::DocSequence {
            identifier: "documents".to_owned(),
            documents: docs,
        },
    ];
    let merged = merge_doc_sequences_into_body(&body, &sections);
    let arr = merged.get_array("documents").unwrap();
    assert_eq!(arr.len(), 2);
}

/// `get_i64` must coerce Int32, Int64 and Double.
#[test]
fn get_i64_coerces_bson_types() {
    let doc = doc! {
        "int32": 7i32,
        "int64": 100i64,
        "double": 3.0_f64,
    };
    assert_eq!(get_i64(&doc, "int32"), Some(7));
    assert_eq!(get_i64(&doc, "int64"), Some(100));
    assert_eq!(get_i64(&doc, "double"), Some(3));
    assert_eq!(get_i64(&doc, "missing"), None);
}

// -----------------------------------------------------------------------
// getMore
// -----------------------------------------------------------------------

#[test]
fn get_more_paginates_through_cursor() {
    let state = ServerState::default();
    let cursors = dummy_cursors();

    // Insert 5 documents.
    handle_insert(
        &doc! { "insert": "pgcoll", "documents": [
            {"i": 0i32}, {"i": 1i32}, {"i": 2i32}, {"i": 3i32}, {"i": 4i32}
        ], "$db": "local" },
        &state,
    );

    // Find with batchSize=2: get first 2, server-side cursor for the rest.
    let find_res = handle_find(
        &doc! { "find": "pgcoll", "filter": {}, "batchSize": 2i32, "$db": "local" },
        &state,
        &cursors,
    );
    let cursor_doc = find_res.get_document("cursor").unwrap();
    let cursor_id = cursor_doc.get_i64("id").unwrap();
    assert_ne!(cursor_id, 0, "first batch should leave a live cursor");
    assert_eq!(cursor_doc.get_array("firstBatch").unwrap().len(), 2);

    // getMore: next 2.
    let more_res = handle_get_more(
        &doc! { "getMore": bson::Bson::Int64(cursor_id), "collection": "pgcoll", "batchSize": 2i32, "$db": "local" },
        &state,
        &cursors,
    );
    assert_eq!(more_res.get_f64("ok").unwrap(), 1.0, "{more_res:?}");
    let more_cursor = more_res.get_document("cursor").unwrap();
    assert_eq!(more_cursor.get_array("nextBatch").unwrap().len(), 2);
    let mid_id = more_cursor.get_i64("id").unwrap();
    assert_ne!(mid_id, 0, "one doc still remains");

    // getMore: last 1.
    let last_res = handle_get_more(
        &doc! { "getMore": bson::Bson::Int64(mid_id), "collection": "pgcoll", "$db": "local" },
        &state,
        &cursors,
    );
    assert_eq!(last_res.get_f64("ok").unwrap(), 1.0, "{last_res:?}");
    let last_cursor = last_res.get_document("cursor").unwrap();
    assert_eq!(last_cursor.get_array("nextBatch").unwrap().len(), 1);
    // Cursor exhausted: id must be 0.
    assert_eq!(
        last_cursor.get_i64("id").unwrap(),
        0,
        "cursor must be exhausted"
    );
    // Cursor removed from map.
    assert_eq!(cursors.lock().unwrap().len(), 0);
}

#[test]
fn get_more_unknown_cursor_returns_cursor_not_found() {
    let state = ServerState::default();
    let cursors = dummy_cursors();
    let result = handle_get_more(
        &doc! { "getMore": bson::Bson::Int64(9999i64), "collection": "c", "$db": "local" },
        &state,
        &cursors,
    );
    assert_eq!(result.get_f64("ok").unwrap(), 0.0);
    assert_eq!(result.get_i32("code").unwrap(), 43); // CursorNotFound
    assert_eq!(result.get_str("codeName").unwrap(), "CursorNotFound");
}

// -----------------------------------------------------------------------
// killCursors
// -----------------------------------------------------------------------

#[test]
fn kill_cursors_removes_known_cursors() {
    let cursors = dummy_cursors();
    // Store two cursors.
    let id1 = cursors
        .lock()
        .unwrap()
        .store(crate::Cursor::<Document>::empty());
    let id2 = cursors
        .lock()
        .unwrap()
        .store(crate::Cursor::<Document>::empty());
    assert_eq!(cursors.lock().unwrap().len(), 2);

    let result = handle_kill_cursors(
        &doc! { "killCursors": "c", "cursors": [bson::Bson::Int64(id1), bson::Bson::Int64(id2)], "$db": "local" },
        &cursors,
    );
    assert_eq!(result.get_f64("ok").unwrap(), 1.0);
    let killed = result.get_array("cursorsKilled").unwrap();
    assert_eq!(killed.len(), 2);
    let not_found = result.get_array("cursorsNotFound").unwrap();
    assert!(not_found.is_empty());
    assert_eq!(cursors.lock().unwrap().len(), 0);
}

#[test]
fn kill_cursors_reports_not_found_for_missing_ids() {
    let cursors = dummy_cursors();
    let result = handle_kill_cursors(
        &doc! { "killCursors": "c", "cursors": [bson::Bson::Int64(42i64)], "$db": "local" },
        &cursors,
    );
    assert_eq!(result.get_f64("ok").unwrap(), 1.0);
    assert!(result.get_array("cursorsKilled").unwrap().is_empty());
    assert_eq!(result.get_array("cursorsNotFound").unwrap().len(), 1);
}

// -----------------------------------------------------------------------
// create / drop
// -----------------------------------------------------------------------

#[test]
fn create_collection_returns_ok() {
    let state = ServerState::default();
    let result = handle_create(&doc! { "create": "newcoll", "$db": "local" }, &state);
    assert_eq!(result.get_f64("ok").unwrap(), 1.0);
}

#[test]
fn create_collection_is_idempotent() {
    let state = ServerState::default();
    handle_create(&doc! { "create": "idmcoll", "$db": "local" }, &state);
    // Creating again must still return ok:1.
    let result = handle_create(&doc! { "create": "idmcoll", "$db": "local" }, &state);
    assert_eq!(result.get_f64("ok").unwrap(), 1.0);
}

#[test]
fn drop_collection_returns_ok() {
    let state = ServerState::default();
    handle_insert(
        &doc! { "insert": "dropcoll", "documents": [{"x": 1i32}], "$db": "local" },
        &state,
    );
    let result = handle_drop(&doc! { "drop": "dropcoll", "$db": "local" }, &state);
    assert_eq!(result.get_f64("ok").unwrap(), 1.0);
}

#[test]
fn drop_nonexistent_collection_returns_ok() {
    let state = ServerState::default();
    let result = handle_drop(&doc! { "drop": "ghost", "$db": "local" }, &state);
    assert_eq!(result.get_f64("ok").unwrap(), 1.0);
}

// -----------------------------------------------------------------------
// listCollections
// -----------------------------------------------------------------------

#[test]
fn list_collections_empty_db() {
    let state = ServerState::default();
    let result = handle_list_collections(&doc! { "listCollections": 1, "$db": "local" }, &state);
    assert_eq!(result.get_f64("ok").unwrap(), 1.0);
    let cursor_doc = result.get_document("cursor").unwrap();
    assert_eq!(cursor_doc.get_i64("id").unwrap(), 0);
    assert!(cursor_doc.get_array("firstBatch").unwrap().is_empty());
}

#[test]
fn list_collections_after_insert() {
    let state = ServerState::default();
    handle_insert(
        &doc! { "insert": "alpha", "documents": [{"x": 1i32}], "$db": "local" },
        &state,
    );
    handle_insert(
        &doc! { "insert": "beta", "documents": [{"y": 2i32}], "$db": "local" },
        &state,
    );
    let result = handle_list_collections(&doc! { "listCollections": 1, "$db": "local" }, &state);
    assert_eq!(result.get_f64("ok").unwrap(), 1.0);
    let batch = result
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    assert_eq!(batch.len(), 2);
    // Each entry must have name, type, options, idIndex.
    for entry in batch {
        let doc = entry.as_document().unwrap();
        assert!(doc.contains_key("name"));
        assert_eq!(doc.get_str("type").unwrap(), "collection");
        assert!(doc.contains_key("options"));
        assert!(doc.contains_key("idIndex"));
    }
}

#[test]
fn list_collections_name_filter() {
    let state = ServerState::default();
    handle_insert(
        &doc! { "insert": "matchme", "documents": [{"a": 1i32}], "$db": "local" },
        &state,
    );
    handle_insert(
        &doc! { "insert": "other", "documents": [{"a": 2i32}], "$db": "local" },
        &state,
    );
    let result = handle_list_collections(
        &doc! { "listCollections": 1, "filter": {"name": "matchme"}, "$db": "local" },
        &state,
    );
    let batch = result
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(
        batch[0].as_document().unwrap().get_str("name").unwrap(),
        "matchme"
    );
}

// -----------------------------------------------------------------------
// createIndexes / dropIndexes / listIndexes
// -----------------------------------------------------------------------

#[test]
fn create_indexes_returns_num_before_after() {
    let state = ServerState::default();
    let result = handle_create_indexes(
        &doc! {
            "createIndexes": "idxcoll",
            "indexes": [{
                "key": {"email": 1i32},
                "name": "email_1",
            }],
            "$db": "local",
        },
        &state,
    );
    assert_eq!(result.get_f64("ok").unwrap(), 1.0, "{result:?}");
    // Before: only synthetic _id_ (= 1). After: _id_ + email_1 (= 2).
    assert_eq!(result.get_i32("numIndexesBefore").unwrap(), 1);
    assert_eq!(result.get_i32("numIndexesAfter").unwrap(), 2);
}

#[test]
fn create_indexes_unique_flag() {
    let state = ServerState::default();
    handle_create_indexes(
        &doc! {
            "createIndexes": "uniqcoll",
            "indexes": [{"key": {"uid": 1i32}, "name": "uid_1", "unique": true}],
            "$db": "local",
        },
        &state,
    );
    let list_res = handle_list_indexes(&doc! { "listIndexes": "uniqcoll", "$db": "local" }, &state);
    let batch = list_res
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    // _id_ at index 0, uid_1 at index 1.
    let uid_doc = batch[1].as_document().unwrap();
    assert_eq!(uid_doc.get_str("name").unwrap(), "uid_1");
    assert!(uid_doc.get_bool("unique").unwrap());
}

#[test]
fn list_indexes_always_includes_id_index() {
    let state = ServerState::default();
    // Collection with no user-created indexes.
    handle_create(&doc! { "create": "barelidx", "$db": "local" }, &state);
    let result = handle_list_indexes(&doc! { "listIndexes": "barelidx", "$db": "local" }, &state);
    assert_eq!(result.get_f64("ok").unwrap(), 1.0);
    let batch = result
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    assert_eq!(batch.len(), 1, "only _id_ index expected");
    let id_idx = batch[0].as_document().unwrap();
    assert_eq!(id_idx.get_str("name").unwrap(), "_id_");
    assert_eq!(id_idx.get_i32("v").unwrap(), 2);
    let key = id_idx.get_document("key").unwrap();
    assert_eq!(key.get_i32("_id").unwrap(), 1);
}

#[test]
fn drop_indexes_by_name() {
    let state = ServerState::default();
    handle_create_indexes(
        &doc! {
            "createIndexes": "dropbynamecoll",
            "indexes": [{"key": {"score": 1i32}, "name": "score_1"}],
            "$db": "local",
        },
        &state,
    );
    let result = handle_drop_indexes(
        &doc! { "dropIndexes": "dropbynamecoll", "index": "score_1", "$db": "local" },
        &state,
    );
    assert_eq!(result.get_f64("ok").unwrap(), 1.0);
    // Verify the index is gone.
    let list_res = handle_list_indexes(
        &doc! { "listIndexes": "dropbynamecoll", "$db": "local" },
        &state,
    );
    let batch = list_res
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    assert_eq!(batch.len(), 1, "only _id_ should remain");
}

#[test]
fn drop_indexes_star_drops_all_user_indexes() {
    let state = ServerState::default();
    handle_create_indexes(
        &doc! {
            "createIndexes": "staridxcoll",
            "indexes": [
                {"key": {"a": 1i32}, "name": "a_1"},
                {"key": {"b": 1i32}, "name": "b_1"},
            ],
            "$db": "local",
        },
        &state,
    );
    let result = handle_drop_indexes(
        &doc! { "dropIndexes": "staridxcoll", "index": "*", "$db": "local" },
        &state,
    );
    assert_eq!(result.get_f64("ok").unwrap(), 1.0);
    let list_res = handle_list_indexes(
        &doc! { "listIndexes": "staridxcoll", "$db": "local" },
        &state,
    );
    let batch = list_res
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    assert_eq!(batch.len(), 1, "only _id_ should remain after drop *");
}

#[test]
fn drop_indexes_rejects_id_index_by_name() {
    let state = ServerState::default();
    handle_create(
        &doc! { "create": "rejectidnamecoll", "$db": "local" },
        &state,
    );
    let result = handle_drop_indexes(
        &doc! { "dropIndexes": "rejectidnamecoll", "index": "_id_", "$db": "local" },
        &state,
    );
    assert_eq!(result.get_f64("ok").unwrap(), 0.0);
    assert_eq!(result.get_i32("code").unwrap(), 27);
    assert_eq!(result.get_str("codeName").unwrap(), "IndexNotFound");
}

#[test]
fn drop_indexes_rejects_id_index_by_key_pattern() {
    let state = ServerState::default();
    handle_create(
        &doc! { "create": "rejectidkeycoll", "$db": "local" },
        &state,
    );
    let result = handle_drop_indexes(
        &doc! { "dropIndexes": "rejectidkeycoll", "index": {"_id": 1i32}, "$db": "local" },
        &state,
    );
    assert_eq!(result.get_f64("ok").unwrap(), 0.0);
    assert_eq!(result.get_i32("code").unwrap(), 27);
    assert_eq!(result.get_str("codeName").unwrap(), "IndexNotFound");
}

// -----------------------------------------------------------------------
// Full OP_MSG dispatch tests for new commands
// -----------------------------------------------------------------------

#[test]
fn dispatch_op_msg_create_and_list_collections() {
    let state = ServerState::default();
    let cursors = dummy_cursors();

    // Create collection via wire protocol.
    let req = make_op_msg_request(200, &doc! { "create": "wiredcoll", "$db": "local" });
    let msg = OpMsg::parse(&req).unwrap();
    let resp_bytes =
        dispatch_op_msg(&msg, 300, msg.header.request_id, &state, 1, &cursors).unwrap();
    let resp = OpMsg::parse(&resp_bytes).unwrap();
    assert_eq!(resp.body().unwrap().get_f64("ok").unwrap(), 1.0);

    // listCollections should show it.
    let req2 = make_op_msg_request(201, &doc! { "listCollections": 1i32, "$db": "local" });
    let msg2 = OpMsg::parse(&req2).unwrap();
    let resp2_bytes =
        dispatch_op_msg(&msg2, 301, msg2.header.request_id, &state, 1, &cursors).unwrap();
    let resp2 = OpMsg::parse(&resp2_bytes).unwrap();
    let body2 = resp2.body().unwrap();
    assert_eq!(body2.get_f64("ok").unwrap(), 1.0);
    let batch = body2
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(
        batch[0].as_document().unwrap().get_str("name").unwrap(),
        "wiredcoll"
    );
}

#[test]
fn dispatch_op_msg_get_more_pagination() {
    let state = ServerState::default();
    let cursors = dummy_cursors();

    // Insert 3 docs.
    let ins_req = make_op_msg_request(
        210,
        &doc! { "insert": "gm_coll", "documents": [{"i": 1i32}, {"i": 2i32}, {"i": 3i32}], "$db": "local" },
    );
    let ins_msg = OpMsg::parse(&ins_req).unwrap();
    dispatch_op_msg(
        &ins_msg,
        310,
        ins_msg.header.request_id,
        &state,
        1,
        &cursors,
    )
    .unwrap();

    // Find with batchSize=1.
    let find_req = make_op_msg_request(
        211,
        &doc! { "find": "gm_coll", "filter": {}, "batchSize": 1i32, "$db": "local" },
    );
    let find_msg = OpMsg::parse(&find_req).unwrap();
    let find_resp_bytes = dispatch_op_msg(
        &find_msg,
        311,
        find_msg.header.request_id,
        &state,
        1,
        &cursors,
    )
    .unwrap();
    let find_resp = OpMsg::parse(&find_resp_bytes).unwrap();
    let find_body = find_resp.body().unwrap();
    assert_eq!(find_body.get_f64("ok").unwrap(), 1.0);
    let cursor_id = find_body
        .get_document("cursor")
        .unwrap()
        .get_i64("id")
        .unwrap();
    assert_ne!(cursor_id, 0);

    // getMore.
    let gm_req = make_op_msg_request(
        212,
        &doc! { "getMore": bson::Bson::Int64(cursor_id), "collection": "gm_coll", "batchSize": 10i32, "$db": "local" },
    );
    let gm_msg = OpMsg::parse(&gm_req).unwrap();
    let gm_resp_bytes =
        dispatch_op_msg(&gm_msg, 312, gm_msg.header.request_id, &state, 1, &cursors).unwrap();
    let gm_resp = OpMsg::parse(&gm_resp_bytes).unwrap();
    let gm_body = gm_resp.body().unwrap();
    assert_eq!(gm_body.get_f64("ok").unwrap(), 1.0);
    let gm_cursor = gm_body.get_document("cursor").unwrap();
    // nextBatch must exist (not firstBatch).
    assert!(
        gm_cursor.contains_key("nextBatch"),
        "getMore response must use 'nextBatch'"
    );
    assert!(
        !gm_cursor.contains_key("firstBatch"),
        "getMore must NOT use 'firstBatch'"
    );
    // Remaining 2 docs plus cursor exhausted.
    assert_eq!(gm_cursor.get_array("nextBatch").unwrap().len(), 2);
    assert_eq!(
        gm_cursor.get_i64("id").unwrap(),
        0,
        "cursor must be exhausted"
    );
}

#[test]
fn dispatch_op_msg_create_and_list_indexes() {
    let state = ServerState::default();
    let cursors = dummy_cursors();

    // createIndexes.
    let ci_req = make_op_msg_request(
        220,
        &doc! {
            "createIndexes": "idx_test_coll",
            "indexes": [{"key": {"name": 1i32}, "name": "name_1"}],
            "$db": "local",
        },
    );
    let ci_msg = OpMsg::parse(&ci_req).unwrap();
    let ci_resp_bytes =
        dispatch_op_msg(&ci_msg, 320, ci_msg.header.request_id, &state, 1, &cursors).unwrap();
    let ci_resp = OpMsg::parse(&ci_resp_bytes).unwrap();
    let ci_body = ci_resp.body().unwrap();
    assert_eq!(ci_body.get_f64("ok").unwrap(), 1.0);
    assert_eq!(ci_body.get_i32("numIndexesBefore").unwrap(), 1);
    assert_eq!(ci_body.get_i32("numIndexesAfter").unwrap(), 2);

    // listIndexes.
    let li_req = make_op_msg_request(
        221,
        &doc! { "listIndexes": "idx_test_coll", "$db": "local" },
    );
    let li_msg = OpMsg::parse(&li_req).unwrap();
    let li_resp_bytes =
        dispatch_op_msg(&li_msg, 321, li_msg.header.request_id, &state, 1, &cursors).unwrap();
    let li_resp = OpMsg::parse(&li_resp_bytes).unwrap();
    let li_body = li_resp.body().unwrap();
    assert_eq!(li_body.get_f64("ok").unwrap(), 1.0);
    let batch = li_body
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    // _id_ + name_1
    assert_eq!(batch.len(), 2);
    assert_eq!(
        batch[0].as_document().unwrap().get_str("name").unwrap(),
        "_id_"
    );
    assert_eq!(
        batch[1].as_document().unwrap().get_str("name").unwrap(),
        "name_1"
    );
}

#[test]
fn create_indexes_rejects_id_index_by_name() {
    let state = ServerState::default();
    let result = handle_create_indexes(
        &doc! {
            "createIndexes": "nameguardcoll",
            "indexes": [{"key": {"x": 1i32}, "name": "_id_"}],
            "$db": "local",
        },
        &state,
    );
    assert_eq!(result.get_f64("ok").unwrap(), 0.0, "{result:?}");
    assert_eq!(result.get_i32("code").unwrap(), 2);
    assert_eq!(result.get_str("codeName").unwrap(), "BadValue");
}

#[test]
fn create_indexes_rejects_id_index_by_key_pattern() {
    let state = ServerState::default();
    let result = handle_create_indexes(
        &doc! {
            "createIndexes": "keyguardcoll",
            "indexes": [{"key": {"_id": 1i32}, "name": "idx"}],
            "$db": "local",
        },
        &state,
    );
    assert_eq!(result.get_f64("ok").unwrap(), 0.0, "{result:?}");
    assert_eq!(result.get_i32("code").unwrap(), 2);
    assert_eq!(result.get_str("codeName").unwrap(), "BadValue");
}

// T20 — dropIndexes "*" must not touch the synthetic _id_ entry reported
// by listIndexes. Post-Wave-1, the catalog no longer carries `_id_`, so
// the loop at the `*` branch of handle_drop_indexes simply won't see it
// when iterating list_indexes. The listIndexes handler still fabricates
// the `_id_` entry unconditionally.
#[test]
fn drop_indexes_star_does_not_touch_id_index() {
    let state = ServerState::default();
    // Create one user index alongside the implicit _id_ index.
    handle_create_indexes(
        &doc! {
            "createIndexes": "starkeepidcoll",
            "indexes": [{"key": {"x": 1i32}, "name": "x_1"}],
            "$db": "local",
        },
        &state,
    );

    let drop_res = handle_drop_indexes(
        &doc! { "dropIndexes": "starkeepidcoll", "index": "*", "$db": "local" },
        &state,
    );
    assert_eq!(drop_res.get_f64("ok").unwrap(), 1.0, "{drop_res:?}");

    let list_res = handle_list_indexes(
        &doc! { "listIndexes": "starkeepidcoll", "$db": "local" },
        &state,
    );
    let batch = list_res
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();

    // The user index must be gone.
    assert!(
        !batch
            .iter()
            .any(|b| b.as_document().unwrap().get_str("name").unwrap() == "x_1"),
        "user index x_1 should be dropped after '*'",
    );

    // The synthetic `_id_` entry must still appear (fabricated by the
    // wire layer in handle_list_indexes regardless of catalog contents).
    assert_eq!(batch.len(), 1, "only _id_ should remain");
    let id_idx = batch[0].as_document().unwrap();
    assert_eq!(id_idx.get_str("name").unwrap(), "_id_");
    let key = id_idx.get_document("key").unwrap();
    assert_eq!(key.get_i32("_id").unwrap(), 1);
}

// T21 — Lane 1 resolution test. Pre-cleanup, the Buffered backend returned
// `_id_` from list_indexes while the old Memory backend did not — causing a
// `+1` offset in handle_create_indexes to double-count on Buffered.
// Post-cleanup only the Buffered backend exists, and the numbers in
// createIndexes responses must match (before=1, after=2).
#[test]
fn create_indexes_numbers_correct_on_buffered_backend() {
    use crate::client::Client;
    use tempfile::TempDir;

    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("t21.mqlite");
    let client = Client::open(&db_path).expect("open buffered client");

    let state = ServerState::new_with_db(&client);

    let result = handle_create_indexes(
        &doc! {
            "createIndexes": "bufnumcoll",
            "indexes": [{
                "key": {"email": 1i32},
                "name": "email_1",
            }],
            "$db": "local",
        },
        &state,
    );
    assert_eq!(result.get_f64("ok").unwrap(), 1.0, "{result:?}");
    // Before: 0 user indexes + 1 synthetic _id_ = 1.
    assert_eq!(result.get_i32("numIndexesBefore").unwrap(), 1);
    // After: 1 user index + 1 synthetic _id_ = 2.
    assert_eq!(result.get_i32("numIndexesAfter").unwrap(), 2);
}

// -----------------------------------------------------------------------
// count
// -----------------------------------------------------------------------

#[test]
fn count_all_documents_without_query() {
    let state = ServerState::default();
    handle_insert(
        &doc! { "insert": "cntcoll", "documents": [{"x": 1i32}, {"x": 2i32}, {"x": 3i32}], "$db": "local" },
        &state,
    );
    let result = handle_count(&doc! { "count": "cntcoll", "$db": "local" }, &state);
    assert_eq!(result.get_f64("ok").unwrap(), 1.0, "{result:?}");
    assert_eq!(result.get_i64("n").unwrap(), 3);
}

#[test]
fn count_with_query_filters_documents() {
    let state = ServerState::default();
    handle_insert(
        &doc! { "insert": "cntqcoll", "documents": [
            {"s": "a"}, {"s": "b"}, {"s": "a"}
        ], "$db": "local" },
        &state,
    );
    let result = handle_count(
        &doc! { "count": "cntqcoll", "query": {"s": "a"}, "$db": "local" },
        &state,
    );
    assert_eq!(result.get_f64("ok").unwrap(), 1.0);
    assert_eq!(result.get_i64("n").unwrap(), 2);
}

#[test]
fn count_with_skip_and_limit() {
    let state = ServerState::default();
    handle_insert(
        &doc! { "insert": "cntskcoll", "documents": [
            {"i": 0i32}, {"i": 1i32}, {"i": 2i32}, {"i": 3i32}, {"i": 4i32}
        ], "$db": "local" },
        &state,
    );
    // 5 docs, skip 1 -> 4 remaining, limit 2 -> count 2.
    let result = handle_count(
        &doc! { "count": "cntskcoll", "skip": 1i32, "limit": 2i32, "$db": "local" },
        &state,
    );
    assert_eq!(result.get_f64("ok").unwrap(), 1.0);
    assert_eq!(result.get_i64("n").unwrap(), 2);
}

#[test]
fn count_negative_limit_treated_as_absolute() {
    let state = ServerState::default();
    handle_insert(
        &doc! { "insert": "cntneglim", "documents": [
            {"i": 0i32}, {"i": 1i32}, {"i": 2i32}, {"i": 3i32}
        ], "$db": "local" },
        &state,
    );
    // limit -2 must behave like limit 2.
    let result = handle_count(
        &doc! { "count": "cntneglim", "limit": -2i32, "$db": "local" },
        &state,
    );
    assert_eq!(result.get_f64("ok").unwrap(), 1.0);
    assert_eq!(result.get_i64("n").unwrap(), 2);
}

#[test]
fn count_negative_skip_returns_bad_value() {
    let state = ServerState::default();
    let result = handle_count(
        &doc! { "count": "cntcoll", "skip": -1i32, "$db": "local" },
        &state,
    );
    assert_eq!(result.get_f64("ok").unwrap(), 0.0);
    assert_eq!(result.get_i32("code").unwrap(), 2); // BadValue
    assert_eq!(result.get_str("codeName").unwrap(), "BadValue");
}

#[test]
fn count_missing_collection_returns_zero() {
    let state = ServerState::default();
    let result = handle_count(&doc! { "count": "ghostcoll", "$db": "local" }, &state);
    assert_eq!(result.get_f64("ok").unwrap(), 1.0);
    assert_eq!(result.get_i64("n").unwrap(), 0);
}

// -----------------------------------------------------------------------
// distinct
// -----------------------------------------------------------------------

#[test]
fn distinct_basic_returns_unique_values() {
    let state = ServerState::default();
    handle_insert(
        &doc! { "insert": "distcoll", "documents": [
            {"color": "red"}, {"color": "blue"}, {"color": "red"}
        ], "$db": "local" },
        &state,
    );
    let result = handle_distinct(
        &doc! { "distinct": "distcoll", "key": "color", "$db": "local" },
        &state,
    );
    assert_eq!(result.get_f64("ok").unwrap(), 1.0, "{result:?}");
    let values = result.get_array("values").unwrap();
    assert_eq!(values.len(), 2);
    let strs: Vec<&str> = values.iter().filter_map(|v| v.as_str()).collect();
    assert!(strs.contains(&"red"));
    assert!(strs.contains(&"blue"));
}

#[test]
fn distinct_with_query_filter() {
    let state = ServerState::default();
    handle_insert(
        &doc! { "insert": "distqcoll", "documents": [
            {"t": "x", "v": 1i32}, {"t": "y", "v": 2i32}, {"t": "x", "v": 3i32}
        ], "$db": "local" },
        &state,
    );
    let result = handle_distinct(
        &doc! { "distinct": "distqcoll", "key": "v", "query": {"t": "x"}, "$db": "local" },
        &state,
    );
    assert_eq!(result.get_f64("ok").unwrap(), 1.0);
    let values = result.get_array("values").unwrap();
    // Only docs with t=x: v in {1, 3}.
    assert_eq!(values.len(), 2);
}

#[test]
fn distinct_unwraps_array_field_values() {
    let state = ServerState::default();
    handle_insert(
        &doc! { "insert": "distarrcoll", "documents": [
            {"tags": ["a", "b"]}, {"tags": ["b", "c"]}
        ], "$db": "local" },
        &state,
    );
    let result = handle_distinct(
        &doc! { "distinct": "distarrcoll", "key": "tags", "$db": "local" },
        &state,
    );
    assert_eq!(result.get_f64("ok").unwrap(), 1.0);
    let values = result.get_array("values").unwrap();
    let strs: Vec<&str> = values.iter().filter_map(|v| v.as_str()).collect();
    // Array elements are unwound: a, b, c.
    assert_eq!(strs.len(), 3);
    assert!(strs.contains(&"a"));
    assert!(strs.contains(&"b"));
    assert!(strs.contains(&"c"));
}

#[test]
fn distinct_missing_key_returns_bad_value() {
    let state = ServerState::default();
    let result = handle_distinct(&doc! { "distinct": "distcoll", "$db": "local" }, &state);
    assert_eq!(result.get_f64("ok").unwrap(), 0.0);
    assert_eq!(result.get_i32("code").unwrap(), 2); // BadValue
    assert_eq!(result.get_str("codeName").unwrap(), "BadValue");
}

#[test]
fn distinct_missing_collection_returns_empty() {
    let state = ServerState::default();
    let result = handle_distinct(
        &doc! { "distinct": "ghostcoll", "key": "x", "$db": "local" },
        &state,
    );
    assert_eq!(result.get_f64("ok").unwrap(), 1.0);
    assert!(result.get_array("values").unwrap().is_empty());
}

// -----------------------------------------------------------------------
// dropDatabase
// -----------------------------------------------------------------------

#[test]
fn drop_database_drops_only_target_db() {
    let state = ServerState::default();
    // Two collections in "dropme", one in "keepme".
    handle_insert(
        &doc! { "insert": "c1", "documents": [{"x": 1i32}], "$db": "dropme" },
        &state,
    );
    handle_insert(
        &doc! { "insert": "c2", "documents": [{"y": 2i32}], "$db": "dropme" },
        &state,
    );
    handle_insert(
        &doc! { "insert": "survivor", "documents": [{"z": 3i32}], "$db": "keepme" },
        &state,
    );

    let result = handle_drop_database(&doc! { "dropDatabase": 1i32, "$db": "dropme" }, &state);
    assert_eq!(result.get_f64("ok").unwrap(), 1.0, "{result:?}");
    assert_eq!(result.get_str("dropped").unwrap(), "dropme");

    // "dropme" collections are gone.
    let dropme_list =
        handle_list_collections(&doc! { "listCollections": 1i32, "$db": "dropme" }, &state);
    assert!(dropme_list
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap()
        .is_empty());

    // "keepme" survives.
    let keepme_list =
        handle_list_collections(&doc! { "listCollections": 1i32, "$db": "keepme" }, &state);
    let keep_batch = keepme_list
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    assert_eq!(keep_batch.len(), 1);
    assert_eq!(
        keep_batch[0].as_document().unwrap().get_str("name").unwrap(),
        "survivor"
    );
}

#[test]
fn drop_database_empty_db_includes_dropped_field() {
    let state = ServerState::default();
    let result = handle_drop_database(&doc! { "dropDatabase": 1i32, "$db": "neverexisted" }, &state);
    assert_eq!(result.get_f64("ok").unwrap(), 1.0);
    // `dropped` is always present, even with no collections.
    assert_eq!(result.get_str("dropped").unwrap(), "neverexisted");
}

// -----------------------------------------------------------------------
// explain
// -----------------------------------------------------------------------

#[test]
fn explain_find_unindexed_field_reports_collscan() {
    let state = ServerState::default();
    handle_insert(
        &doc! { "insert": "expcoll", "documents": [{"a": 1i32}, {"a": 2i32}], "$db": "local" },
        &state,
    );
    let result = handle_explain(
        &doc! {
            "explain": {"find": "expcoll", "filter": {"a": 1i32}},
            "$db": "local",
        },
        &state,
    );
    assert_eq!(result.get_f64("ok").unwrap(), 1.0, "{result:?}");
    assert_eq!(result.get_str("explainVersion").unwrap(), "1");
    let qp = result.get_document("queryPlanner").unwrap();
    assert!(qp.get_str("namespace").unwrap().contains("expcoll"));
    assert!(!qp.get_bool("indexFilterSet").unwrap());
    assert!(qp.contains_key("parsedQuery"));
    assert!(qp.get_array("rejectedPlans").unwrap().is_empty());
    let winning = qp.get_document("winningPlan").unwrap();
    assert_eq!(winning.get_str("stage").unwrap(), "COLLSCAN");
    // The command echo must be present.
    assert!(result.contains_key("command"));
}

#[test]
fn explain_find_indexed_field_reports_ixscan() {
    let state = ServerState::default();
    handle_insert(
        &doc! { "insert": "expidxcoll", "documents": [
            {"email": "a@x.com"}, {"email": "b@x.com"}
        ], "$db": "local" },
        &state,
    );
    handle_create_indexes(
        &doc! {
            "createIndexes": "expidxcoll",
            "indexes": [{"key": {"email": 1i32}, "name": "email_1"}],
            "$db": "local",
        },
        &state,
    );
    let result = handle_explain(
        &doc! {
            "explain": {"find": "expidxcoll", "filter": {"email": "a@x.com"}},
            "$db": "local",
        },
        &state,
    );
    assert_eq!(result.get_f64("ok").unwrap(), 1.0, "{result:?}");
    let winning = result
        .get_document("queryPlanner")
        .unwrap()
        .get_document("winningPlan")
        .unwrap();
    assert_eq!(winning.get_str("stage").unwrap(), "FETCH");
    let input = winning.get_document("inputStage").unwrap();
    assert_eq!(input.get_str("stage").unwrap(), "IXSCAN");
    assert_eq!(input.get_str("indexName").unwrap(), "email_1");
    let key_pattern = input.get_document("keyPattern").unwrap();
    assert_eq!(key_pattern.get_i32("email").unwrap(), 1);
}

#[test]
fn explain_unsupported_inner_command_returns_command_not_found() {
    let state = ServerState::default();
    let result = handle_explain(
        &doc! {
            "explain": {"aggregate": "coll", "pipeline": []},
            "$db": "local",
        },
        &state,
    );
    assert_eq!(result.get_f64("ok").unwrap(), 0.0);
    assert_eq!(result.get_i32("code").unwrap(), 59); // CommandNotFound
    assert_eq!(result.get_str("codeName").unwrap(), "CommandNotFound");
    // The message must name the unsupported explain target.
    assert!(result.get_str("errmsg").unwrap().contains("aggregate"));
}

#[test]
fn explain_default_verbosity_returns_query_planner_only() {
    let state = ServerState::default();
    handle_insert(
        &doc! { "insert": "expdefcoll", "documents": [{"a": 1i32}], "$db": "local" },
        &state,
    );
    // No verbosity supplied — must default to queryPlanner (no executionStats).
    let result = handle_explain(
        &doc! {
            "explain": {"find": "expdefcoll", "filter": {}},
            "$db": "local",
        },
        &state,
    );
    assert_eq!(result.get_f64("ok").unwrap(), 1.0);
    assert!(result.contains_key("queryPlanner"));
    assert!(
        !result.contains_key("executionStats"),
        "queryPlanner-only response must omit executionStats"
    );
}

// -----------------------------------------------------------------------
// Full OP_MSG dispatch tests for count / distinct / dropDatabase / explain
// -----------------------------------------------------------------------

#[test]
fn dispatch_op_msg_count_and_distinct() {
    let state = ServerState::default();
    let cursors = dummy_cursors();

    // Seed three docs.
    let ins_req = make_op_msg_request(
        400,
        &doc! { "insert": "dcoll", "documents": [
            {"k": "a"}, {"k": "b"}, {"k": "a"}
        ], "$db": "local" },
    );
    let ins_msg = OpMsg::parse(&ins_req).unwrap();
    dispatch_op_msg(&ins_msg, 500, ins_msg.header.request_id, &state, 1, &cursors).unwrap();

    // count via dispatch.
    let cnt_req = make_op_msg_request(401, &doc! { "count": "dcoll", "$db": "local" });
    let cnt_msg = OpMsg::parse(&cnt_req).unwrap();
    let cnt_bytes =
        dispatch_op_msg(&cnt_msg, 501, cnt_msg.header.request_id, &state, 1, &cursors).unwrap();
    let cnt_resp = OpMsg::parse(&cnt_bytes).unwrap();
    let cnt_body = cnt_resp.body().unwrap();
    assert_eq!(cnt_body.get_f64("ok").unwrap(), 1.0);
    assert_eq!(cnt_body.get_i64("n").unwrap(), 3);

    // distinct via dispatch.
    let dst_req =
        make_op_msg_request(402, &doc! { "distinct": "dcoll", "key": "k", "$db": "local" });
    let dst_msg = OpMsg::parse(&dst_req).unwrap();
    let dst_bytes =
        dispatch_op_msg(&dst_msg, 502, dst_msg.header.request_id, &state, 1, &cursors).unwrap();
    let dst_resp = OpMsg::parse(&dst_bytes).unwrap();
    let dst_body = dst_resp.body().unwrap();
    assert_eq!(dst_body.get_f64("ok").unwrap(), 1.0);
    assert_eq!(dst_body.get_array("values").unwrap().len(), 2);
}

#[test]
fn dispatch_op_msg_drop_database_and_explain() {
    let state = ServerState::default();
    let cursors = dummy_cursors();

    // Seed a collection in "ddb".
    let ins_req = make_op_msg_request(
        410,
        &doc! { "insert": "things", "documents": [{"x": 1i32}], "$db": "ddb" },
    );
    let ins_msg = OpMsg::parse(&ins_req).unwrap();
    dispatch_op_msg(&ins_msg, 510, ins_msg.header.request_id, &state, 1, &cursors).unwrap();

    // dropDatabase via dispatch.
    let dd_req = make_op_msg_request(411, &doc! { "dropDatabase": 1i32, "$db": "ddb" });
    let dd_msg = OpMsg::parse(&dd_req).unwrap();
    let dd_bytes =
        dispatch_op_msg(&dd_msg, 511, dd_msg.header.request_id, &state, 1, &cursors).unwrap();
    let dd_resp = OpMsg::parse(&dd_bytes).unwrap();
    let dd_body = dd_resp.body().unwrap();
    assert_eq!(dd_body.get_f64("ok").unwrap(), 1.0);
    assert_eq!(dd_body.get_str("dropped").unwrap(), "ddb");

    // explain via dispatch.
    let ex_req = make_op_msg_request(
        412,
        &doc! { "explain": {"find": "things", "filter": {}}, "$db": "local" },
    );
    let ex_msg = OpMsg::parse(&ex_req).unwrap();
    let ex_bytes =
        dispatch_op_msg(&ex_msg, 512, ex_msg.header.request_id, &state, 1, &cursors).unwrap();
    let ex_resp = OpMsg::parse(&ex_bytes).unwrap();
    let ex_body = ex_resp.body().unwrap();
    assert_eq!(ex_body.get_f64("ok").unwrap(), 1.0);
    assert_eq!(ex_body.get_str("explainVersion").unwrap(), "1");
}

// -----------------------------------------------------------------------
// aggregate
// -----------------------------------------------------------------------

#[test]
fn aggregate_countdocuments_pymongo_shape() {
    // pymongo countDocuments: [{$match: filter}, {$group: {_id: 1, n: {$sum: 1}}}]
    let state = ServerState::default();
    let cursors = dummy_cursors();
    handle_insert(
        &doc! { "insert": "aggcnt", "documents": [
            {"status": "a"}, {"status": "b"}, {"status": "a"}
        ], "$db": "local" },
        &state,
    );
    let body = doc! {
        "aggregate": "aggcnt",
        "pipeline": [
            { "$match": { "status": "a" } },
            { "$group": { "_id": 1i32, "n": { "$sum": 1i32 } } },
        ],
        "cursor": {},
        "$db": "local",
    };
    let result = handle_aggregate(&body, &state, &cursors);
    assert_eq!(result.get_f64("ok").unwrap(), 1.0, "{result:?}");
    let cursor_doc = result.get_document("cursor").unwrap();
    let batch = cursor_doc.get_array("firstBatch").unwrap();
    assert_eq!(batch.len(), 1);
    let group_doc = batch[0].as_document().unwrap();
    assert_eq!(group_doc.get_i32("n").unwrap(), 2);
}

#[test]
fn aggregate_cursor_field_required() {
    let state = ServerState::default();
    let cursors = dummy_cursors();
    let body = doc! {
        "aggregate": "somecoll",
        "pipeline": [],
        // "cursor" field intentionally omitted
        "$db": "local",
    };
    let result = handle_aggregate(&body, &state, &cursors);
    assert_eq!(result.get_f64("ok").unwrap(), 0.0, "{result:?}");
    assert_eq!(result.get_i32("code").unwrap(), 2); // BadValue
    let msg = result.get_str("errmsg").unwrap();
    assert!(
        msg.contains("cursor"),
        "error must mention 'cursor', got: {msg}"
    );
}

#[test]
fn aggregate_getmore_continuation_with_small_batch_size() {
    let state = ServerState::default();
    let cursors = dummy_cursors();
    // Insert 5 docs.
    handle_insert(
        &doc! { "insert": "aggbatch", "documents": [
            {"i": 0i32}, {"i": 1i32}, {"i": 2i32}, {"i": 3i32}, {"i": 4i32}
        ], "$db": "local" },
        &state,
    );
    // Aggregate with batchSize=2: should get firstBatch of 2 and a live cursor.
    let body = doc! {
        "aggregate": "aggbatch",
        "pipeline": [ { "$match": {} } ],
        "cursor": { "batchSize": 2i32 },
        "$db": "local",
    };
    let result = handle_aggregate(&body, &state, &cursors);
    assert_eq!(result.get_f64("ok").unwrap(), 1.0, "{result:?}");
    let cursor_doc = result.get_document("cursor").unwrap();
    assert_eq!(cursor_doc.get_array("firstBatch").unwrap().len(), 2);
    let cursor_id = cursor_doc.get_i64("id").unwrap();
    assert_ne!(cursor_id, 0, "cursor must be live when more docs remain");

    // getMore: drain the rest.
    let more = handle_get_more(
        &doc! { "getMore": bson::Bson::Int64(cursor_id),
                "collection": "aggbatch", "$db": "local" },
        &state,
        &cursors,
    );
    assert_eq!(more.get_f64("ok").unwrap(), 1.0, "{more:?}");
    let more_cursor = more.get_document("cursor").unwrap();
    let next_batch = more_cursor.get_array("nextBatch").unwrap();
    assert_eq!(next_batch.len(), 3);
    assert_eq!(more_cursor.get_i64("id").unwrap(), 0, "cursor exhausted");
}

#[test]
fn aggregate_unknown_stage_error_response() {
    let state = ServerState::default();
    let cursors = dummy_cursors();
    let body = doc! {
        "aggregate": "anycoll",
        "pipeline": [ { "$bogus": {} } ],
        "cursor": {},
        "$db": "local",
    };
    let result = handle_aggregate(&body, &state, &cursors);
    assert_eq!(result.get_f64("ok").unwrap(), 0.0, "{result:?}");
    let msg = result.get_str("errmsg").unwrap();
    assert!(
        msg.contains("$bogus"),
        "error must name unknown stage, got: {msg}"
    );
}

#[test]
fn aggregate_nonexistent_collection_empty_batch() {
    let state = ServerState::default();
    let cursors = dummy_cursors();
    let body = doc! {
        "aggregate": "doesnotexist",
        "pipeline": [ { "$match": { "x": 1i32 } } ],
        "cursor": {},
        "$db": "local",
    };
    let result = handle_aggregate(&body, &state, &cursors);
    assert_eq!(result.get_f64("ok").unwrap(), 1.0, "{result:?}");
    let cursor_doc = result.get_document("cursor").unwrap();
    assert!(
        cursor_doc.get_array("firstBatch").unwrap().is_empty(),
        "nonexistent collection must return empty firstBatch"
    );
    assert_eq!(cursor_doc.get_i64("id").unwrap(), 0);
}

// -----------------------------------------------------------------------
// find / explain with `hint`
// -----------------------------------------------------------------------

/// Seed `hintcoll` in `local` with an `email_1` index, returning the state.
fn seed_hint_collection() -> ServerState {
    let state = ServerState::default();
    handle_insert(
        &doc! { "insert": "hintcoll", "documents": [
            {"email": "a@x.com"}, {"email": "b@x.com"}
        ], "$db": "local" },
        &state,
    );
    handle_create_indexes(
        &doc! {
            "createIndexes": "hintcoll",
            "indexes": [{"key": {"email": 1i32}, "name": "email_1"}],
            "$db": "local",
        },
        &state,
    );
    state
}

#[test]
fn find_with_hint_string_returns_matching_docs() {
    let state = seed_hint_collection();
    let cursors = dummy_cursors();
    let result = handle_find(
        &doc! {
            "find": "hintcoll",
            "filter": {"email": "a@x.com"},
            "hint": "email_1",
            "$db": "local",
        },
        &state,
        &cursors,
    );
    assert_eq!(result.get_f64("ok").unwrap(), 1.0, "{result:?}");
    let first_batch = result
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    assert_eq!(first_batch.len(), 1);
}

#[test]
fn find_with_hint_document_returns_matching_docs() {
    let state = seed_hint_collection();
    let cursors = dummy_cursors();
    let result = handle_find(
        &doc! {
            "find": "hintcoll",
            "filter": {"email": "b@x.com"},
            "hint": {"email": 1i32},
            "$db": "local",
        },
        &state,
        &cursors,
    );
    assert_eq!(result.get_f64("ok").unwrap(), 1.0, "{result:?}");
    let first_batch = result
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    assert_eq!(first_batch.len(), 1);
}

#[test]
fn find_with_bad_hint_returns_bad_value_error() {
    let state = seed_hint_collection();
    let cursors = dummy_cursors();
    let result = handle_find(
        &doc! {
            "find": "hintcoll",
            "filter": {"email": "a@x.com"},
            "hint": "nonexistent_idx",
            "$db": "local",
        },
        &state,
        &cursors,
    );
    assert_eq!(result.get_f64("ok").unwrap(), 0.0, "{result:?}");
    assert_eq!(result.get_i32("code").unwrap(), 2); // BadValue
    assert_eq!(result.get_str("codeName").unwrap(), "BadValue");
    assert!(result
        .get_str("errmsg")
        .unwrap()
        .contains("hint provided does not correspond to an existing index"));
}

#[test]
fn explain_find_with_hint_reports_ixscan() {
    let state = seed_hint_collection();
    let result = handle_explain(
        &doc! {
            "explain": {
                "find": "hintcoll",
                "filter": {},
                "hint": "email_1",
            },
            "$db": "local",
        },
        &state,
    );
    assert_eq!(result.get_f64("ok").unwrap(), 1.0, "{result:?}");
    let winning = result
        .get_document("queryPlanner")
        .unwrap()
        .get_document("winningPlan")
        .unwrap();
    // Empty filter + hint -> unbounded index scan, still an IXSCAN.
    assert_eq!(winning.get_str("stage").unwrap(), "FETCH");
    let input = winning.get_document("inputStage").unwrap();
    assert_eq!(input.get_str("stage").unwrap(), "IXSCAN");
    assert_eq!(input.get_str("indexName").unwrap(), "email_1");
}

#[test]
fn explain_find_with_natural_hint_reports_collscan() {
    let state = seed_hint_collection();
    let result = handle_explain(
        &doc! {
            "explain": {
                "find": "hintcoll",
                "filter": {"email": "a@x.com"},
                "hint": {"$natural": 1i32},
            },
            "$db": "local",
        },
        &state,
    );
    assert_eq!(result.get_f64("ok").unwrap(), 1.0, "{result:?}");
    let winning = result
        .get_document("queryPlanner")
        .unwrap()
        .get_document("winningPlan")
        .unwrap();
    assert_eq!(winning.get_str("stage").unwrap(), "COLLSCAN");
}
