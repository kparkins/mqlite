//! [`ExplainResult`] — public-API snapshot of a query's execution plan.
//!
//! This is the user-visible projection of the crate-internal
//! [`super::planner::ScanPlan`]. The cursor just holds and returns it;
//! construction lives here so all plan → explain mapping is in one place.

use super::planner::ScanPlan;

/// The result of a cursor's query plan explanation.
///
/// Returned by [`crate::cursor::Cursor::explain`].
///
/// # Example
/// ```no_run
/// # use mqlite::{Client, doc};
/// # use tempfile::TempDir;
/// # fn main() -> mqlite::Result<()> {
/// # let dir = TempDir::new()?;
/// # let client = Client::open(dir.path().join("db.mqlite"))?;
/// # let db = client.database("test");
/// # let col = db.collection::<bson::Document>("orders");
/// let cursor = col.find(doc! { "status": "pending" }).run()?;
/// let plan = cursor.explain()?;
/// println!("Plan: {}", plan.plan);
/// println!("Index used: {:?}", plan.index_used);
/// println!("Docs examined: {}", plan.docs_examined);
/// println!("Full scan: {}", plan.full_scan);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct ExplainResult {
    /// Human-readable description of the query plan (e.g. `"COLLSCAN"` or
    /// `"IXSCAN { email_1 }"`).
    pub plan: String,
    /// The index that was selected for this query, if any.
    ///
    /// `None` means the query engine performed a full collection scan.
    pub index_used: Option<String>,
    /// Total number of documents examined (scanned) to satisfy the query.
    ///
    /// For a full collection scan this equals the collection size at the time
    /// the cursor was created. For an index scan, it is the number of index
    /// entries visited.
    pub docs_examined: u64,
    /// Whether the query required a full collection scan (`true`) or was
    /// satisfied by an index seek (`false`).
    pub full_scan: bool,
}

impl ExplainResult {
    /// Build an [`ExplainResult`] from a [`ScanPlan`] and a post-execution
    /// `docs_examined` count.
    ///
    /// `docs_examined` must be supplied by the executor because the planner
    /// does not run the query — it only selects the access path.
    pub(crate) fn from_plan(plan: &ScanPlan, docs_examined: u64) -> Self {
        let (plan, index_used, full_scan) = match plan {
            ScanPlan::CollScan => ("COLLSCAN".to_owned(), None, true),
            ScanPlan::PrimaryKeyLookup { .. } => {
                ("IDHACK".to_owned(), Some("_id_".to_owned()), false)
            }
            ScanPlan::IndexScan { index_name, .. } => (
                format!("IXSCAN {{ {index_name} }}"),
                Some(index_name.clone()),
                false,
            ),
        };

        Self {
            plan,
            index_used,
            docs_examined,
            full_scan,
        }
    }
}
