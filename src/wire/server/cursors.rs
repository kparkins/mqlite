//! Per-connection cursor state and the idle-eviction sweep task.
//!
//! Extracted from [`super`] to keep the file under length budget.

use std::collections::HashMap;
use std::sync::Arc;

/// Cursors not accessed for longer than this duration are evicted.
pub(crate) const CURSOR_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);

/// A buffered cursor stored in the per-connection cursor map.
#[allow(dead_code)] // cursor field used when data commands (getMore, killCursors) are added
struct StoredCursor {
    /// The buffered cursor data.
    cursor: crate::Cursor<bson::Document>,
    /// When this cursor was last accessed (used for idle eviction).
    last_accessed: std::time::Instant,
}

/// Per-connection cursor state.
///
/// Tracks all open server-side cursors for one TCP connection.  When
/// [`ConnectionCursors`] is dropped (i.e., when the connection closes),
/// every stored cursor is released automatically — satisfying the
/// acceptance criterion "connection close releases all associated cursors".
pub(crate) struct ConnectionCursors {
    /// Cursor ID → stored cursor.
    cursors: HashMap<i64, StoredCursor>,
    /// Monotonically increasing cursor ID counter.  Starts at 1; cursor ID 0
    /// is reserved in the MongoDB wire protocol to mean "no cursor".
    next_cursor_id: i64,
}

#[allow(dead_code)] // methods used when data commands (getMore, killCursors) are added
impl ConnectionCursors {
    pub(crate) fn new() -> Self {
        ConnectionCursors {
            cursors: HashMap::new(),
            next_cursor_id: 1,
        }
    }

    /// Store `cursor` and return its assigned cursor ID.
    pub(crate) fn store(&mut self, cursor: crate::Cursor<bson::Document>) -> i64 {
        let id = self.next_cursor_id;
        // Cursor ID 0 is reserved; skip it on overflow.
        self.next_cursor_id = self.next_cursor_id.wrapping_add(1).max(1);
        self.cursors.insert(
            id,
            StoredCursor {
                cursor,
                last_accessed: std::time::Instant::now(),
            },
        );
        id
    }

    /// Remove and return the cursor identified by `id`, if present.
    pub(crate) fn remove(&mut self, id: i64) -> Option<crate::Cursor<bson::Document>> {
        self.cursors.remove(&id).map(|e| e.cursor)
    }

    /// Return a mutable reference to the cursor for `id`, refreshing its
    /// last-accessed timestamp.  Returns `None` if the cursor is not found.
    pub(crate) fn get_mut(&mut self, id: i64) -> Option<&mut crate::Cursor<bson::Document>> {
        self.cursors.get_mut(&id).map(|e| {
            e.last_accessed = std::time::Instant::now();
            &mut e.cursor
        })
    }

    /// Evict cursors that have been idle for longer than `timeout`.
    ///
    /// Returns the number of cursors evicted.
    fn evict_idle(&mut self, timeout: std::time::Duration) -> usize {
        let before = self.cursors.len();
        self.cursors
            .retain(|_, entry| entry.last_accessed.elapsed() < timeout);
        before - self.cursors.len()
    }

    /// Number of currently open cursors.
    fn len(&self) -> usize {
        self.cursors.len()
    }
}

/// Background task: periodically evict idle cursors for one connection.
///
/// Sweeps every 60 seconds using [`CURSOR_IDLE_TIMEOUT`].  Exits when
/// `shutdown_rx` resolves, which happens automatically when the corresponding
/// sender is dropped (i.e., when the connection handler returns).
pub(crate) async fn cursor_sweep_task(
    cursors: Arc<std::sync::Mutex<ConnectionCursors>>,
    mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let mut c = cursors.lock().unwrap_or_else(|e| e.into_inner());
                let _evicted = c.evict_idle(CURSOR_IDLE_TIMEOUT);
                #[cfg(feature = "tracing")]
                if _evicted > 0 {
                    tracing::debug!(
                        target: "mqlite",
                        evicted = _evicted,
                        "mqlite::wire::cursor_evict"
                    );
                }
            }
            _ = &mut shutdown_rx => break,
        }
    }
}
