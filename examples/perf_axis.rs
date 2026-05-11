//! Single-axis perf workload for profiling. Opens ONE client and loops the
//! chosen workload until the duration elapses. Avoids the per-iteration
//! TempDir/Client setup overhead that dominates `examples/perf_goal.rs`.
//!
//! Usage:
//!   perf_axis --axis same_ns_single      [--writers N] [--seconds S]
//!   perf_axis --axis same_ns_batch       [--writers N] [--seconds S]
//!   perf_axis --axis same_ns_partitioned [--writers N] [--seconds S]
//!   perf_axis --axis multi_ns_single     [--writers N] [--seconds S]
//!   perf_axis --axis multi_ns_batch      [--writers N] [--seconds S]
//!   perf_axis --axis read_find_one       [--seconds S]
//!
//! Default writer counts: same_ns_* = 4, multi_ns_* = 8.

use std::env;
use std::io::Write;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use bson::{doc, Document};
use mqlite::{Client, DurabilityMode, OpenOptions};
use tempfile::TempDir;

const PAYLOAD_BYTES: usize = 256;
const DURABILITY_INTERVAL_MS: u64 = 100;
const DEFAULT_SAME_NS_WRITERS: usize = 4;
const DEFAULT_MULTI_NS_WRITERS: usize = 8;
const DOCS_PER_BATCH: usize = 100;
const SEED_DOCS: i32 = 2_000;
const PARTITIONED_KEYS_PER_WRITER: i64 = 1 << 32;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut axis: Option<String> = None;
    let mut seconds: u64 = 15;
    let mut writers_override: Option<usize> = None;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--axis" => axis = args.next(),
            "--seconds" => {
                seconds = args.next().ok_or("--seconds requires value")?.parse()?;
            }
            "--writers" => {
                let v: usize = args.next().ok_or("--writers requires value")?.parse()?;
                if v == 0 {
                    return Err("--writers must be >= 1".into());
                }
                writers_override = Some(v);
            }
            other => return Err(format!("unknown arg: {other}").into()),
        }
    }
    let axis = axis.ok_or(
        "--axis required (one of: same_ns_single, same_ns_batch, same_ns_partitioned, \
         multi_ns_single, multi_ns_batch, read_find_one)",
    )?;
    let dur = Duration::from_secs(seconds);

    let dir = TempDir::new()?;
    let path = dir.path().join("perf-axis.mqlite");
    let opts = OpenOptions::new().durability(DurabilityMode::Interval(Duration::from_millis(
        DURABILITY_INTERVAL_MS,
    )));
    let client = Client::open_with_options(&path, opts)?;
    let db = client.database("perf_axis");

    match axis.as_str() {
        "same_ns_single" => {
            let writers = writers_override.unwrap_or(DEFAULT_SAME_NS_WRITERS);
            run_same_ns(&client, dur, false, writers, false)?
        }
        "same_ns_batch" => {
            let writers = writers_override.unwrap_or(DEFAULT_SAME_NS_WRITERS);
            run_same_ns(&client, dur, true, writers, false)?
        }
        "same_ns_partitioned" => {
            let writers = writers_override.unwrap_or(DEFAULT_SAME_NS_WRITERS);
            run_same_ns(&client, dur, false, writers, true)?
        }
        "multi_ns_single" => {
            let writers = writers_override.unwrap_or(DEFAULT_MULTI_NS_WRITERS);
            run_multi_ns(&client, dur, false, writers)?
        }
        "multi_ns_batch" => {
            let writers = writers_override.unwrap_or(DEFAULT_MULTI_NS_WRITERS);
            run_multi_ns(&client, dur, true, writers)?
        }
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
        "read_find_one_under_writers" => {
            // PR1 read-coupling AC scenario: 4 writers + 1 reader on
            // same_ns_single. Reader-thread `pin_then_latch` shared-acquire
            // latency is sampled into the buffer-pool histogram (see
            // `src/storage/buffer_pool/metrics_perf.rs`); reader and writer
            // throughput are reported separately.
            let writers = writers_override.unwrap_or(DEFAULT_SAME_NS_WRITERS);
            run_read_find_one_under_writers(&client, dur, writers)?
        }
        other => return Err(format!("unknown --axis: {other}").into()),
    }
    #[cfg(feature = "perf-counters")]
    print_perf_counters();
    // Throughput is already emitted inside the run_* helpers; the workload
    // is what we measure, not the post-workload Client::drop checkpoint.
    // Bypass the drop chain (which would flush a 60K-row checkpoint
    // serially per invocation and dominate the runner's wall time) and
    // exit immediately. The TempDir leak is bounded — the OS reclaims it
    // when the runner finishes.
    std::io::stdout().flush().ok();
    std::process::exit(0);
}

fn run_same_ns(
    client: &Client,
    dur: Duration,
    batch: bool,
    writers: usize,
    partitioned: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let db = client.database("perf_axis");
    db.create_collection("same_ns")?;

    let payload = Arc::new("x".repeat(PAYLOAD_BYTES));
    let id_offset = Arc::new(std::sync::atomic::AtomicI64::new(0));
    let stop_at = Instant::now() + dur;
    let barrier = Arc::new(Barrier::new(writers));

    let mut handles = Vec::with_capacity(writers);
    for w in 0..writers {
        let c = client.clone();
        let payload = Arc::clone(&payload);
        let id_offset = Arc::clone(&id_offset);
        let barrier = Arc::clone(&barrier);
        let writer_idx = w as i64;
        handles.push(thread::spawn(move || -> Result<u64, String> {
            let coll = c.database("perf_axis").collection::<Document>("same_ns");
            barrier.wait();
            let mut count = 0u64;
            // For the partitioned axis each writer reserves a disjoint _id
            // range starting at `writer_idx * PARTITIONED_KEYS_PER_WRITER`.
            let mut local_partitioned_id: i64 = writer_idx * PARTITIONED_KEYS_PER_WRITER;
            while Instant::now() < stop_at {
                if batch {
                    let base = id_offset.fetch_add(
                        DOCS_PER_BATCH as i64 * writers as i64,
                        std::sync::atomic::Ordering::Relaxed,
                    ) + writer_idx * DOCS_PER_BATCH as i64;
                    let docs: Vec<Document> = (0..DOCS_PER_BATCH)
                        .map(|i| doc! { "_id": base + i as i64, "payload": payload.as_str() })
                        .collect();
                    coll.insert_many(&docs).run().map_err(|e| e.to_string())?;
                    count += DOCS_PER_BATCH as u64;
                } else if partitioned {
                    let id = local_partitioned_id;
                    local_partitioned_id += 1;
                    coll.insert_one(&doc! { "_id": id, "payload": payload.as_str() })
                        .map_err(|e| e.to_string())?;
                    count += 1;
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
    let axis_label = if partitioned {
        "partitioned"
    } else if batch {
        "batch"
    } else {
        "single"
    };
    println!(
        "{{\"axis\":\"same_ns_{}\",\"writers\":{},\"docs\":{},\"elapsed_secs\":{:.6},\"docs_per_second\":{:.2}}}",
        axis_label,
        writers,
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
    writers: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let db = client.database("perf_axis");
    for w in 0..writers {
        db.create_collection(&format!("multi_ns_{w}"))?;
    }

    let payload = Arc::new("x".repeat(PAYLOAD_BYTES));
    let id_offset = Arc::new(std::sync::atomic::AtomicI64::new(0));
    let stop_at = Instant::now() + dur;
    let barrier = Arc::new(Barrier::new(writers));

    let mut handles = Vec::with_capacity(writers);
    for w in 0..writers {
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
    println!(
        "{{\"axis\":\"multi_ns_{}\",\"writers\":{},\"docs\":{},\"elapsed_secs\":{:.6},\"docs_per_second\":{:.2}}}",
        mode.to_lowercase(),
        writers,
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
    println!(
        "{{\"axis\":\"read_find_one\",\"writers\":1,\"ops\":{},\"elapsed_secs\":{:.6},\"ops_per_second\":{:.2}}}",
        count,
        elapsed,
        count as f64 / elapsed
    );
    Ok(())
}

/// PR1 read-coupling AC scenario: 4 writers + 1 reader on same_ns
/// `same_ns_single` workload. Reader-thread `pin_then_latch` shared
/// latency is sampled into the buffer-pool histogram (when built with
/// `--features perf-counters`); reader and writer throughput are
/// reported on separate JSON lines.
fn run_read_find_one_under_writers(
    client: &Client,
    dur: Duration,
    writers: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let db = client.database("perf_axis");
    db.create_collection("same_ns")?;

    // Seed the namespace with a small set of documents so the reader's
    // `find_one` always touches a leaf path that exists.
    let payload_seed = "x".repeat(PAYLOAD_BYTES);
    let seed: Vec<Document> = (0..SEED_DOCS)
        .map(|id| doc! { "_id": id, "payload": payload_seed.as_str() })
        .collect();
    db.collection::<Document>("same_ns")
        .insert_many(&seed)
        .run()?;

    // Reset the perf-counter histogram before the workload window so
    // the recorded p50/p99 reflect only this scenario's samples.
    #[cfg(feature = "perf-counters")]
    {
        mqlite::perf_counters::reset_shared_latch_wait_hist();
        mqlite::perf_counters::reset_flip_counters();
    }

    let payload = Arc::new("x".repeat(PAYLOAD_BYTES));
    let id_offset = Arc::new(std::sync::atomic::AtomicI64::new(SEED_DOCS as i64));
    let stop_at = Instant::now() + dur;
    let barrier = Arc::new(Barrier::new(writers + 1));

    let mut writer_handles = Vec::with_capacity(writers);
    for _ in 0..writers {
        let c = client.clone();
        let payload = Arc::clone(&payload);
        let id_offset = Arc::clone(&id_offset);
        let barrier = Arc::clone(&barrier);
        writer_handles.push(thread::spawn(move || -> Result<u64, String> {
            let coll = c.database("perf_axis").collection::<Document>("same_ns");
            barrier.wait();
            let mut count = 0u64;
            while Instant::now() < stop_at {
                let id = id_offset.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                coll.insert_one(&doc! { "_id": id, "payload": payload.as_str() })
                    .map_err(|e| e.to_string())?;
                count += 1;
            }
            Ok(count)
        }));
    }

    // Reader thread (single, on this same client). Its
    // `pin_then_latch(_, _, Shared)` calls inside `find_one` are what
    // the histogram in `metrics_perf::record_shared_latch_wait_ns`
    // samples.
    let reader = {
        let c = client.clone();
        let barrier = Arc::clone(&barrier);
        thread::spawn(move || -> Result<u64, String> {
            let coll = c.database("perf_axis").collection::<Document>("same_ns");
            barrier.wait();
            let mut count = 0u64;
            let mut id = 0i32;
            while Instant::now() < stop_at {
                let _ = coll
                    .find_one(doc! { "_id": id })
                    .map_err(|e| e.to_string())?;
                id = (id + 1) % SEED_DOCS;
                count += 1;
            }
            Ok(count)
        })
    };

    let start = Instant::now();
    let mut writer_total = 0u64;
    for h in writer_handles {
        writer_total += h.join().unwrap().map_err(|s| s.to_string())?;
    }
    let reader_total = reader.join().unwrap().map_err(|s| s.to_string())?;
    let elapsed = start.elapsed().as_secs_f64();

    println!(
        "{{\"axis\":\"read_find_one_under_writers\",\"writers\":{},\"writer_docs\":{},\"reader_ops\":{},\"elapsed_secs\":{:.6},\"writer_docs_per_second\":{:.2},\"reader_ops_per_second\":{:.2}}}",
        writers,
        writer_total,
        reader_total,
        elapsed,
        writer_total as f64 / elapsed,
        reader_total as f64 / elapsed,
    );
    Ok(())
}

/// Emit the PR1 perf-counter values when built with
/// `--features perf-counters`. Called from `main` after every axis
/// completes so the AC harness can scrape:
///   - flip_retry_rate (must be < 0.01 over a 30 s same_ns_single run)
///   - flip_retry_exhausted_count (must be == 0 — engine poison gate)
///   - shared_latch_wait_p50_ns / p99_ns (read-coupling AC)
#[cfg(feature = "perf-counters")]
fn print_perf_counters() {
    use mqlite::perf_counters as pc;
    println!(
        "{{\"perf_counters\":{{\"flip_retry_rate\":{:.6},\"flip_retry_exhausted\":{},\"shared_latch_wait_p50_ns\":{},\"shared_latch_wait_p99_ns\":{}}}}}",
        pc::flip_retry_rate(),
        pc::flip_retry_exhausted_count(),
        pc::shared_latch_wait_p50_ns(),
        pc::shared_latch_wait_p99_ns(),
    );
}
