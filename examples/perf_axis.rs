//! Single-axis perf workload for profiling. Opens ONE client and loops the
//! chosen workload until the duration elapses. Avoids the per-iteration
//! TempDir/Client setup overhead that dominates `examples/perf_goal.rs`.
//!
//! Usage:
//!   perf_axis --axis same_ns_single --seconds 15
//!   perf_axis --axis same_ns_batch  --seconds 15
//!   perf_axis --axis multi_ns_single --seconds 15
//!   perf_axis --axis multi_ns_batch  --seconds 15
//!   perf_axis --axis read_find_one  --seconds 15

use std::env;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use bson::{doc, Document};
use mqlite::{Client, DurabilityMode, OpenOptions};
use tempfile::TempDir;

const PAYLOAD_BYTES: usize = 256;
const DURABILITY_INTERVAL_MS: u64 = 100;
const SAME_NS_WRITERS: usize = 4;
const MULTI_NS_WRITERS: usize = 8;
const DOCS_PER_BATCH: usize = 100;
const SEED_DOCS: i32 = 2_000;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut axis: Option<String> = None;
    let mut seconds: u64 = 15;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--axis" => axis = args.next(),
            "--seconds" => {
                seconds = args.next().ok_or("--seconds requires value")?.parse()?;
            }
            other => return Err(format!("unknown arg: {other}").into()),
        }
    }
    let axis = axis.ok_or("--axis required (one of: same_ns_single, same_ns_batch, multi_ns_single, multi_ns_batch, read_find_one)")?;
    let dur = Duration::from_secs(seconds);

    let dir = TempDir::new()?;
    let path = dir.path().join("perf-axis.mqlite");
    let opts = OpenOptions::new().durability(DurabilityMode::Interval(Duration::from_millis(
        DURABILITY_INTERVAL_MS,
    )));
    let client = Client::open_with_options(&path, opts)?;
    let db = client.database("perf_axis");

    match axis.as_str() {
        "same_ns_single" => run_same_ns(&client, dur, false)?,
        "same_ns_batch" => run_same_ns(&client, dur, true)?,
        "multi_ns_single" => run_multi_ns(&client, dur, false)?,
        "multi_ns_batch" => run_multi_ns(&client, dur, true)?,
        "read_find_one" => {
            db.create_collection("reads")?;
            let coll = db.collection::<Document>("reads");
            let payload = "x".repeat(PAYLOAD_BYTES);
            let seed = (0..SEED_DOCS)
                .map(|id| doc! { "_id": id, "payload": payload.as_str() })
                .collect::<Vec<_>>();
            coll.insert_many(&seed).run()?;
            run_read_find_one(&client, dur)?
        }
        other => return Err(format!("unknown --axis: {other}").into()),
    }
    Ok(())
}

fn run_same_ns(
    client: &Client,
    dur: Duration,
    batch: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let db = client.database("perf_axis");
    db.create_collection("same_ns")?;

    let payload = Arc::new("x".repeat(PAYLOAD_BYTES));
    let id_offset = Arc::new(std::sync::atomic::AtomicI64::new(0));
    let stop_at = Instant::now() + dur;
    let barrier = Arc::new(Barrier::new(SAME_NS_WRITERS));

    let mut handles = Vec::with_capacity(SAME_NS_WRITERS);
    for w in 0..SAME_NS_WRITERS {
        let c = client.clone();
        let payload = Arc::clone(&payload);
        let id_offset = Arc::clone(&id_offset);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || -> Result<u64, String> {
            let coll = c.database("perf_axis").collection::<Document>("same_ns");
            barrier.wait();
            let mut count = 0u64;
            while Instant::now() < stop_at {
                if batch {
                    let base = id_offset.fetch_add(
                        DOCS_PER_BATCH as i64 * SAME_NS_WRITERS as i64,
                        std::sync::atomic::Ordering::Relaxed,
                    ) + w as i64 * DOCS_PER_BATCH as i64;
                    let docs: Vec<Document> = (0..DOCS_PER_BATCH)
                        .map(|i| doc! { "_id": base + i as i64, "payload": payload.as_str() })
                        .collect();
                    coll.insert_many(&docs).run().map_err(|e| e.to_string())?;
                    count += DOCS_PER_BATCH as u64;
                } else {
                    let id = id_offset.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    coll.insert_one(&doc! { "_id": id, "payload": payload.as_str() })
                        .map_err(|e| e.to_string())?;
                    count += 1;
                }
            }
            Ok(count)
        }));
    }

    let start = Instant::now();
    let mut total = 0u64;
    for h in handles {
        total += h.join().unwrap().map_err(|s| s.to_string())?;
    }
    let elapsed = start.elapsed().as_secs_f64();
    let mode = if batch { "BATCH" } else { "SINGLE" };
    eprintln!(
        "{{\"axis\":\"same_ns_{}\",\"writers\":{},\"docs\":{},\"elapsed_secs\":{:.6},\"docs_per_second\":{:.2}}}",
        mode.to_lowercase(),
        SAME_NS_WRITERS,
        total,
        elapsed,
        total as f64 / elapsed
    );
    Ok(())
}

fn run_multi_ns(
    client: &Client,
    dur: Duration,
    batch: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let db = client.database("perf_axis");
    for w in 0..MULTI_NS_WRITERS {
        db.create_collection(&format!("multi_ns_{w}"))?;
    }

    let payload = Arc::new("x".repeat(PAYLOAD_BYTES));
    let id_offset = Arc::new(std::sync::atomic::AtomicI64::new(0));
    let stop_at = Instant::now() + dur;
    let barrier = Arc::new(Barrier::new(MULTI_NS_WRITERS));

    let mut handles = Vec::with_capacity(MULTI_NS_WRITERS);
    for w in 0..MULTI_NS_WRITERS {
        let c = client.clone();
        let payload = Arc::clone(&payload);
        let id_offset = Arc::clone(&id_offset);
        let barrier = Arc::clone(&barrier);
        let coll_name = format!("multi_ns_{w}");
        handles.push(thread::spawn(move || -> Result<u64, String> {
            let coll = c.database("perf_axis").collection::<Document>(&coll_name);
            barrier.wait();
            let mut count = 0u64;
            while Instant::now() < stop_at {
                if batch {
                    let base = id_offset.fetch_add(
                        DOCS_PER_BATCH as i64,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    let docs: Vec<Document> = (0..DOCS_PER_BATCH)
                        .map(|i| doc! { "_id": base + i as i64, "payload": payload.as_str() })
                        .collect();
                    coll.insert_many(&docs).run().map_err(|e| e.to_string())?;
                    count += DOCS_PER_BATCH as u64;
                } else {
                    let id = id_offset.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    coll.insert_one(&doc! { "_id": id, "payload": payload.as_str() })
                        .map_err(|e| e.to_string())?;
                    count += 1;
                }
            }
            Ok(count)
        }));
    }

    let start = Instant::now();
    let mut total = 0u64;
    for h in handles {
        total += h.join().unwrap().map_err(|s| s.to_string())?;
    }
    let elapsed = start.elapsed().as_secs_f64();
    let mode = if batch { "BATCH" } else { "SINGLE" };
    eprintln!(
        "{{\"axis\":\"multi_ns_{}\",\"writers\":{},\"docs\":{},\"elapsed_secs\":{:.6},\"docs_per_second\":{:.2}}}",
        mode.to_lowercase(),
        MULTI_NS_WRITERS,
        total,
        elapsed,
        total as f64 / elapsed
    );
    Ok(())
}

fn run_read_find_one(client: &Client, dur: Duration) -> Result<(), Box<dyn std::error::Error>> {
    let coll = client
        .database("perf_axis")
        .collection::<Document>("reads");
    let stop_at = Instant::now() + dur;
    let start = Instant::now();
    let mut count = 0u64;
    let mut id = 0i32;
    while Instant::now() < stop_at {
        let _ = coll.find_one(doc! { "_id": id })?;
        id = (id + 1) % SEED_DOCS;
        count += 1;
    }
    let elapsed = start.elapsed().as_secs_f64();
    eprintln!(
        "{{\"axis\":\"read_find_one\",\"ops\":{},\"elapsed_secs\":{:.6},\"ops_per_second\":{:.2}}}",
        count,
        elapsed,
        count as f64 / elapsed
    );
    Ok(())
}
