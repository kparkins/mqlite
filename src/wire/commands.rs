//! Command dispatch for the MongoDB wire protocol shim.
//!
//! This module maps incoming OP_MSG command documents to mqlite operations.
//!
//! Phase 1 target: 18 commands sufficient for mongosh basic CRUD and pymongo acceptance tests.
//! See api.md for the full list.
//!
//! Phase 1 implementation: tracked in hq-6d0.

/// The set of MongoDB commands supported by the Phase 1 wire protocol shim.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Command {
    // CRUD
    Insert,
    Find,
    Update,
    Delete,
    FindAndModify,
    GetMore,
    KillCursors,

    // Indexes
    CreateIndexes,
    DropIndexes,
    ListIndexes,

    // Collections and Databases
    ListCollections,
    Create,
    Drop,
    ListDatabases,

    // Introspection / handshake
    Ping,
    Hello,
    IsMaster,
    BuildInfo,
    ServerStatus,
}

impl Command {
    /// Parse a command name from the first key of an OP_MSG command document.
    pub fn from_str(s: &str) -> Option<Command> {
        match s.to_lowercase().as_str() {
            "insert" => Some(Command::Insert),
            "find" => Some(Command::Find),
            "update" => Some(Command::Update),
            "delete" => Some(Command::Delete),
            "findandmodify" => Some(Command::FindAndModify),
            "getmore" => Some(Command::GetMore),
            "killcursors" => Some(Command::KillCursors),
            "createindexes" => Some(Command::CreateIndexes),
            "dropindexes" => Some(Command::DropIndexes),
            "listindexes" => Some(Command::ListIndexes),
            "listcollections" => Some(Command::ListCollections),
            "create" => Some(Command::Create),
            "drop" => Some(Command::Drop),
            "listdatabases" => Some(Command::ListDatabases),
            "ping" => Some(Command::Ping),
            "hello" => Some(Command::Hello),
            "ismaster" | "isMaster" => Some(Command::IsMaster),
            "buildinfo" | "buildInfo" => Some(Command::BuildInfo),
            "serverstatus" | "serverStatus" => Some(Command::ServerStatus),
            _ => None,
        }
    }
}
