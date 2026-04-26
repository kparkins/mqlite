use mqlite::{doc, Client, ObjectId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Task {
    id: Option<ObjectId>,
    title: String,
    done: bool,
    priority: u8,
}

fn main() -> mqlite::Result<()> {
    // Use a temp directory so we don't pollute the project root
    let db_path = std::env::temp_dir().join("mqlite_cli_demo.mqlite");
    println!("Database: {}", db_path.display());

    let client = Client::open(&db_path)?;
    let db = client.database("tasks");
    let tasks = db.collection::<Task>("tasks");

    // --- Insert some tasks ---
    println!("\n--- Inserting tasks ---");
    let t1 = Task {
        id: None,
        title: "Write unit tests".into(),
        done: false,
        priority: 3,
    };
    let t2 = Task {
        id: None,
        title: "Review PR".into(),
        done: false,
        priority: 1,
    };
    let t3 = Task {
        id: None,
        title: "Update docs".into(),
        done: true,
        priority: 2,
    };
    let t4 = Task {
        id: None,
        title: "Fix login bug".into(),
        done: false,
        priority: 1,
    };

    tasks.insert_one(&t1)?;
    println!("  ✓ Inserted: {}", t1.title);
    tasks.insert_one(&t2)?;
    println!("  ✓ Inserted: {}", t2.title);
    tasks.insert_one(&t3)?;
    println!("  ✓ Inserted: {}", t3.title);
    tasks.insert_one(&t4)?;
    println!("  ✓ Inserted: {}", t4.title);

    // --- Count all tasks ---
    println!(
        "\n--- Total tasks: {} ---",
        tasks.estimated_document_count()?
    );

    // --- Find all pending (not done) tasks, sorted by priority ---
    println!("\n--- Pending tasks (sorted by priority) ---");
    let cursor = tasks
        .find(doc! { "done": false })
        .sort(doc! { "priority": 1 })
        .run()?;
    for task in cursor.take(10) {
        let t = task?;
        println!(
            "  [{priority}] {title}",
            priority = t.priority,
            title = t.title
        );
    }

    // --- Find one task by filter ---
    println!("\n--- Finding first high-priority task ---");
    if let Some(task) = tasks.find_one(doc! { "priority": 1, "done": false })? {
        println!("  Found: {} (priority={})", task.title, task.priority);
    }

    // --- Update a task ---
    println!("\n--- Marking 'Review PR' as done ---");
    let result = tasks
        .update_one(
            doc! { "title": "Review PR" },
            doc! { "$set": { "done": true } },
        )
        .run()?;
    println!("  Updated {} document(s)", result.modified_count);

    // --- Find all tasks after update ---
    println!("\n--- All tasks after update ---");
    let cursor = tasks
        .find(doc! {})
        .sort(doc! { "priority": 1, "title": 1 })
        .run()?;
    for task in cursor.take(10) {
        let t = task?;
        let status = if t.done { "✓" } else { "○" };
        println!(
            "  {} [{priority}] {}",
            status,
            t.title,
            priority = t.priority
        );
    }

    // --- Delete a task ---
    println!("\n--- Deleting 'Update docs' ---");
    let result = tasks.delete_one(doc! { "title": "Update docs" })?;
    println!("  Deleted {} document(s)", result.deleted_count);

    // --- Final count ---
    println!(
        "\n--- Final count: {} ---",
        tasks.estimated_document_count()?
    );

    // --- List collections ---
    println!("--- Collections: {:?} ---", db.list_collection_names()?);

    // Clean up
    drop(client);
    let _ = std::fs::remove_file(&db_path);
    println!("\nDone! (cleaned up database file)");

    Ok(())
}
