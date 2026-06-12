// === Command handlers: admin / status ===

use std::collections::BTreeSet;

use bson::{doc, Bson, DateTime, Document};

use super::super::errors::err_from_mqlite;
use super::super::server::ServerState;
use super::extract_db_name;

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
            "counter": Bson::Int64(0_i64),
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
        "pid": Bson::Int64(std::process::id() as i64),
        "uptime": Bson::Int64(uptime_secs),
        "uptimeMillis": Bson::Int64(uptime_secs * 1_000),
        "uptimeEstimate": Bson::Int64(uptime_secs),
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
    let db_set: BTreeSet<String> = all_names
        .into_iter()
        .filter_map(|n| {
            let db = n.split('.').next()?;
            (db != "$").then(|| db.to_owned())
        })
        .collect();

    let size_on_disk = state.journal_file_size() as i64;
    let databases: bson::Array = db_set
        .into_iter()
        .map(|name| {
            Bson::Document(doc! {
                "name": name,
                "sizeOnDisk": size_on_disk,
                "empty": false,
            })
        })
        .collect();

    doc! {
        "databases": databases,
        "totalSize": size_on_disk,
        "totalSizeMb": Bson::Int64(size_on_disk / (1024 * 1024)),
        "ok": 1.0_f64,
    }
}

/// `dropDatabase` — drop every collection belonging to the named database.
///
/// The database is identified by the command's `$db` field.  Every collection
/// whose name is prefixed with `<db>.` is dropped; collections in other
/// databases are untouched.  The `dropped` field is always included, even when
/// the database had no collections (drivers tolerate this).
///
/// Response format (MongoDB 8.0):
/// ```json
/// { "dropped": "<dbName>", "ok": 1.0 }
/// ```
pub(super) fn handle_drop_database(body: &Document, state: &ServerState) -> Document {
    let db_name = extract_db_name(body);

    let all_names = match state.database.list_collection_names() {
        Ok(n) => n,
        Err(e) => return err_from_mqlite(e),
    };

    let db_prefix = format!("{db_name}.");
    for name in all_names {
        if !name.starts_with(&db_prefix) {
            continue;
        }
        if let Err(e) = state.database.drop_collection(&name) {
            return err_from_mqlite(e);
        }
    }

    doc! {
        "dropped": db_name,
        "ok": 1.0_f64,
    }
}

/// `endSessions` — no-op session teardown.
///
/// Drivers send `endSessions` when a client or session pool closes.  mqlite has
/// no session state, so this is a no-op that returns `{ ok: 1 }`.
pub(super) fn handle_end_sessions() -> Document {
    doc! { "ok": 1.0_f64 }
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
