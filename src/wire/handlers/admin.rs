// === Command handlers: admin / status ===

use bson::{doc, DateTime, Document};

use super::super::server::ServerState;

/// `hello` / `isMaster` — driver handshake response.
///
/// Reports server capabilities to the driver.  We advertise:
/// - Standalone mode (`isWritablePrimary: true`, no replica set fields)
/// - `maxWireVersion: 21` (MongoDB 8.0) to prevent driver downgrades
/// - `helloOk: true` — signals to pymongo and mongosh that the server supports
///   the `hello` command, so subsequent topology checks use `hello` via OP_MSG
/// - `topologyVersion` — required by MongoDB 5.0+ drivers; `processId` is
///   generated once at server start, `counter` is always 0 (not a replica set)
/// - `connectionId` — unique per-connection integer identifier
/// - No sessions, no auth, no transactions — strips capabilities mqlite lacks
/// - `mqlite.version` so tooling can detect it is talking to mqlite
pub(super) fn handle_hello(state: &ServerState, connection_id: i32) -> Document {
    doc! {
        // Standalone — no replica set discovery.
        "isWritablePrimary": true,

        // Signals that the server supports `hello` — pymongo 4.x will use
        // `hello` via OP_MSG for all subsequent topology checks instead of
        // retrying with legacy `isMaster` via OP_QUERY.
        "helloOk": true,

        // Topology version — required by MongoDB 5.0+ drivers.
        // processId is a random ObjectId generated once at server start.
        // counter is always 0 (mqlite is not a replica set and never transitions state).
        "topologyVersion": {
            "processId": state.topology_process_id,
            "counter": bson::Bson::Int64(0_i64),
        },

        // Capacity limits (match MongoDB 8.0 defaults).
        "maxBsonObjectSize": 16_777_216i32,
        "maxMessageSizeBytes": 48_000_000i32,
        "maxWriteBatchSize": 100_000i32,

        // Current server time (used by drivers for clock skew detection).
        "localTime": DateTime::now(),

        // Unique identifier for this connection.
        "connectionId": connection_id,

        // Wire protocol version range.
        // minWireVersion: 0  — accept all drivers.
        // maxWireVersion: 21 — MongoDB 8.0; prevents drivers from trying
        //                      to negotiate down to legacy opcodes.
        "minWireVersion": 0i32,
        "maxWireVersion": 21i32,

        // Not a read-only replica.
        "readOnly": false,

        // mqlite identity — lets client code detect it is talking to mqlite.
        "mqlite": {
            "version": env!("CARGO_PKG_VERSION"),
        },

        "ok": 1.0_f64,
    }
}

/// `ping` — basic connectivity check.
///
/// Returns `{ ok: 1 }`.
pub(super) fn handle_ping() -> Document {
    doc! { "ok": 1.0_f64 }
}

/// `buildInfo` — server build metadata.
///
/// Returns mqlite version information in MongoDB buildInfo format.
pub(super) fn handle_build_info() -> Document {
    doc! {
        "version": env!("CARGO_PKG_VERSION"),
        "gitVersion": env!("CARGO_PKG_VERSION"),
        // Empty modules array — mqlite has no enterprise/community modules.
        "modules": [],
        // Memory allocator identity.
        "allocator": "rust",
        // Identify this as mqlite, not MongoDB.
        "mqlite": true,
        "ok": 1.0_f64,
    }
}

/// `serverStatus` — runtime diagnostic statistics.
///
/// Returns uptime, journal file size, connection count, and placeholder buffer pool
/// stats sourced from internal server state.
///
/// Data sources:
/// - **uptime**: elapsed seconds since `WireProtocol::bind()`.
/// - **journal size**: `std::fs::metadata("<db>-journal")?.len()` — best-effort, 0 if absent.
/// - **connections.current**: approximate (counts connections opened, not live ones).
/// - **buffer pool**: placeholder zeros (pool instrumentation not yet implemented).
pub(super) fn handle_server_status(state: &ServerState) -> Document {
    let uptime_secs = state.uptime_secs();
    let journal_size = state.journal_file_size() as i64;
    let total_conns = state.total_connections();

    doc! {
        "host": "mqlite",
        "version": env!("CARGO_PKG_VERSION"),
        "process": "mqlite",
        "pid": bson::Bson::Int64(std::process::id() as i64),
        "uptime": bson::Bson::Int64(uptime_secs),
        "uptimeMillis": bson::Bson::Int64(uptime_secs * 1_000),
        "uptimeEstimate": bson::Bson::Int64(uptime_secs),
        "localTime": DateTime::now(),
        "connections": {
            "current": total_conns,
            "available": 64i32,
            "totalCreated": total_conns,
        },
        "storageEngine": {
            "name": "mqlite",
            "persistent": true,
            "supportsCommittedReads": false,
            "readOnly": false,
        },
        // Journal-based storage stats.
        "mqlite": {
            "journalFileSizeBytes": journal_size,
        },
        // Placeholder buffer pool stats (pool instrumentation not yet implemented).
        "bufferPool": {
            "hits": 0i64,
            "misses": 0i64,
            "pages": 0i64,
        },
        "ok": 1.0_f64,
    }
}

/// `listDatabases` — enumerate available databases.
///
/// Enumerates all unique database namespaces that have at least one collection.
/// The database names are the prefixes of the `"db.collection"` keys stored
/// in the engine.  An empty mqlite instance (no writes yet) returns an empty
/// list, matching the MongoDB 8.0 behaviour where databases only appear after
/// the first write (`use mydb` semantics).
pub(super) fn handle_list_databases(state: &ServerState) -> Document {
    // Collect unique database names from all "db.collection" collection names.
    let all_names = state.database.list_collection_names().unwrap_or_default();
    let mut db_set: std::collections::BTreeSet<String> = all_names
        .into_iter()
        .filter_map(|n| n.split('.').next().map(|db| db.to_owned()))
        .collect();
    // Remove internal engine namespaces that are not user databases.
    db_set.remove("$");

    let size_on_disk = state.journal_file_size() as i64;

    let mut databases: bson::Array = Vec::with_capacity(db_set.len());
    for name in &db_set {
        databases.push(bson::Bson::Document(doc! {
            "name": name,
            "sizeOnDisk": size_on_disk,
            "empty": false,
        }));
    }

    doc! {
        "databases": databases,
        "totalSize": size_on_disk,
        "totalSizeMb": bson::Bson::Int64(size_on_disk / (1024 * 1024)),
        "ok": 1.0_f64,
    }
}

/// Unknown command — returns `CommandNotFound` (error code 59).
pub(super) fn handle_unknown(name: &str) -> Document {
    #[cfg(feature = "tracing")]
    tracing::warn!(
        target: "mqlite",
        operator = name,
        "mqlite::unsupported_op"
    );
    doc! {
        "ok": 0.0_f64,
        "errmsg": format!("no such command: '{}'", name),
        "code": 59i32,
        "codeName": "CommandNotFound",
    }
}
