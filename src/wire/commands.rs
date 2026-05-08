//! Command dispatch for the MongoDB wire protocol shim.
//!
//! This module maps incoming OP_MSG command documents to mqlite operations.

/// The set of MongoDB commands supported by the wire protocol shim.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Command {
    // CRUD
    /// Insert one or more documents into a collection.
    Insert,
    /// Query documents in a collection.
    Find,
    /// Update matching documents in a collection.
    Update,
    /// Delete matching documents from a collection.
    Delete,
    /// Atomically find and modify a document.
    FindAndModify,
    /// Fetch the next batch of results from an open cursor.
    GetMore,
    /// Close one or more open server-side cursors.
    KillCursors,

    // Indexes
    /// Create one or more indexes on a collection.
    CreateIndexes,
    /// Remove one or more indexes from a collection.
    DropIndexes,
    /// List indexes for a collection.
    ListIndexes,

    // Collections and Databases
    /// List collections in the current database.
    ListCollections,
    /// Explicitly create a collection.
    Create,
    /// Drop a collection or database.
    Drop,
    /// List all databases on this server.
    ListDatabases,

    // Introspection / handshake
    /// Basic connectivity check.
    Ping,
    /// Driver handshake command (MongoDB 5.0+).
    Hello,
    /// Legacy driver handshake command (deprecated alias for `hello`).
    IsMaster,
    /// Return server build metadata.
    BuildInfo,
    /// Return server runtime statistics.
    ServerStatus,
}

impl Command {
    /// Parse a MongoDB command name (the first key of an OP_MSG command document).
    ///
    /// Returns `None` for unrecognised commands.
    pub fn parse_name(s: &str) -> Option<Command> {
        use Command::*;

        match s.to_lowercase().as_str() {
            "insert" => Some(Insert),
            "find" => Some(Find),
            "update" => Some(Update),
            "delete" => Some(Delete),
            "findandmodify" => Some(FindAndModify),
            "getmore" => Some(GetMore),
            "killcursors" => Some(KillCursors),
            "createindexes" => Some(CreateIndexes),
            "dropindexes" => Some(DropIndexes),
            "listindexes" => Some(ListIndexes),
            "listcollections" => Some(ListCollections),
            "create" => Some(Create),
            "drop" => Some(Drop),
            "listdatabases" => Some(ListDatabases),
            "ping" => Some(Ping),
            "hello" => Some(Hello),
            "ismaster" => Some(IsMaster),
            "buildinfo" => Some(BuildInfo),
            "serverstatus" => Some(ServerStatus),
            _ => None,
        }
    }
}
