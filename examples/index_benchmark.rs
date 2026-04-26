/// Demonstrates how to verify that indexes are being used by comparing
/// `docs_examined` counts between indexed and non-indexed queries.
///
/// The key idea: if an index is used, `docs_examined` should be close to the
/// number of matching documents. Without an index, it equals the full collection size.
use mqlite::{doc, Client, ObjectId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct LogEntry {
    id: Option<ObjectId>,
    level: String,
    service: String,
    message: String,
}

fn main() -> mqlite::Result<()> {
    let db_path = std::env::temp_dir().join("mqlite_index_test.mqlite");
    println!("Database: {}\n", db_path.display());

    let client = Client::open(&db_path)?;
    let db = client.database("logs");

    // --- Create collection and insert data ---
    let logs = db.collection::<LogEntry>("logs");

    // Drop and recreate for a clean run
    let _ = db.drop_collection("logs");

    // Insert 1000 log entries: ~900 "info", ~95 "warn", ~4 "error", ~1 "debug"
    let levels = vec!["info"; 900]
        .into_iter()
        .chain(vec!["warn"; 95])
        .chain(vec!["error"; 4])
        .chain(vec!["debug"; 1]);

    let docs: Vec<LogEntry> = levels
        .enumerate()
        .map(|(i, level)| LogEntry {
            id: None,
            level: level.to_string(),
            service: format!("service-{}", i % 5),
            message: format!("Log message {}", i),
        })
        .collect();

    let insert_result = logs.insert_many(&docs).ordered(false).run()?;
    println!(
        "Inserted {} documents (errors: {})",
        insert_result.inserted_ids.len(),
        insert_result.errors.len()
    );

    let total = logs.estimated_document_count()?;
    println!("Total documents: {}\n", total);

    // --- Create index on `level` field ---
    logs.create_index(
        mqlite::IndexModel::builder()
            .keys(doc! { "level": 1 })
            .build(),
    )?;

    let indexes = logs.list_indexes()?;
    println!(
        "Indexes: {:?}\n",
        indexes.iter().map(|i| &i.name).collect::<Vec<_>>()
    );

    // --- Test 1: Query with index (equality on indexed field) ---
    println!("=== Test 1: Find all 'error' logs (indexed on `level`) ===");
    let cursor = logs.find(doc! { "level": "error" }).run()?;
    let explain = cursor.explain()?;
    println!("  Plan: {}", explain.plan);
    println!("  Index used: {:?}", explain.index_used);
    println!("  Docs examined: {}", explain.docs_examined);
    println!("  Full scan: {}", explain.full_scan);

    let matched: Vec<_> = cursor.collect::<mqlite::Result<Vec<_>>>()?;
    println!("  Matched: {}\n", matched.len());

    // --- Test 2: Query without index (equality on non-indexed field) ---
    println!("=== Test 2: Find all logs for 'service-3' (no index on `service`) ===");
    let cursor = logs.find(doc! { "service": "service-3" }).run()?;
    let explain = cursor.explain()?;
    println!("  Plan: {}", explain.plan);
    println!("  Index used: {:?}", explain.index_used);
    println!("  Docs examined: {}", explain.docs_examined);
    println!("  Full scan: {}", explain.full_scan);

    let matched: Vec<_> = cursor.collect::<mqlite::Result<Vec<_>>>()?;
    println!("  Matched: {}\n", matched.len());

    // --- Test 3: Range query with index ($gte on indexed field) ---
    println!("=== Test 3: Find logs with level >= 'warn' (range on indexed `level`) ===");
    let cursor = logs.find(doc! { "level": { "$gte": "warn" } }).run()?;
    let explain = cursor.explain()?;
    println!("  Plan: {}", explain.plan);
    println!("  Index used: {:?}", explain.index_used);
    println!("  Docs examined: {}", explain.docs_examined);
    println!("  Full scan: {}", explain.full_scan);

    let matched: Vec<_> = cursor.collect::<mqlite::Result<Vec<_>>>()?;
    println!("  Matched: {}\n", matched.len());

    // --- Summary ---
    println!("=== Summary ===");
    println!(
        "Indexed query (level='error'): examined {} docs for {} matches",
        explain.docs_examined,
        matched.len()
    );

    // Clean up
    drop(client);
    let _ = std::fs::remove_file(&db_path);
    println!("\nDone!");

    Ok(())
}
