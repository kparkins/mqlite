//! Public-API throughput probe for the performance-goal evaluator.

use std::env;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use bson::{doc, Document};
use mqlite::{Client, DurabilityMode, IndexModel, OpenOptions};
use tempfile::TempDir;

const DEFAULT_SAME_COLLECTION_WRITERS: usize = 4;
const DEFAULT_MULTI_COLLECTION_WRITERS: usize = 8;
const DEFAULT_WRITE_DOCS_PER_WRITER: usize = 500;
const DEFAULT_WRITE_BATCH_SIZE: usize = 100;
const DEFAULT_READ_SEED_DOCS: i32 = 2_000;
const DEFAULT_READ_OPS: usize = 400;
const KEY_BAND_WIDTH: usize = 1_000_000;
const PAYLOAD_BYTES: usize = 256;
const DURABILITY_INTERVAL_MS: u64 = 100;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let single_same_collection = run_same_collection_write_benchmark(WriteMode::Single)?;
    let batch_same_collection = run_same_collection_write_benchmark(WriteMode::Batch)?;
    let single_multi_collection = run_multi_collection_write_benchmark(WriteMode::Single)?;
    let batch_multi_collection = run_multi_collection_write_benchmark(WriteMode::Batch)?;
    let read = run_read_benchmark()?;
    println!(
        "{{\"write_single_same_collection_4\":{{\"writers\":{},\"docs\":{},\
         \"elapsed_secs\":{:.9},\"docs_per_second\":{:.6}}},\
         \"write_batch_same_collection_4\":{{\"writers\":{},\"docs\":{},\
         \"elapsed_secs\":{:.9},\"docs_per_second\":{:.6}}},\
         \"write_single_multi_collection_8\":{{\"writers\":{},\"docs\":{},\
         \"elapsed_secs\":{:.9},\"docs_per_second\":{:.6}}},\
         \"write_batch_multi_collection_8\":{{\"writers\":{},\"docs\":{},\
         \"elapsed_secs\":{:.9},\"docs_per_second\":{:.6}}},\
         \"read_mixed\":{{\"ops\":{},\"elapsed_secs\":{:.9},\"ops_per_second\":{:.6}}}}}",
        single_same_collection.workers,
        single_same_collection.units,
        single_same_collection.elapsed_secs,
        single_same_collection.units_per_second,
        batch_same_collection.workers,
        batch_same_collection.units,
        batch_same_collection.elapsed_secs,
        batch_same_collection.units_per_second,
        single_multi_collection.workers,
        single_multi_collection.units,
        single_multi_collection.elapsed_secs,
        single_multi_collection.units_per_second,
        batch_multi_collection.workers,
        batch_multi_collection.units,
        batch_multi_collection.elapsed_secs,
        batch_multi_collection.units_per_second,
        read.units,
        read.elapsed_secs,
        read.units_per_second
    );
    Ok(())
}

struct Measurement {
    workers: usize,
    units: usize,
    elapsed_secs: f64,
    units_per_second: f64,
}

struct WriteConfig {
    writers: usize,
    docs_per_writer: usize,
    batch_size: usize,
    distinct_collections: bool,
    mode: WriteMode,
}

#[derive(Clone, Copy)]
enum WriteMode {
    Single,
    Batch,
}

fn run_same_collection_write_benchmark(
    mode: WriteMode,
) -> Result<Measurement, Box<dyn std::error::Error>> {
    let writers = env_usize(
        "MQLITE_PERF_SAME_COLLECTION_WRITERS",
        DEFAULT_SAME_COLLECTION_WRITERS,
    )
    .max(1);
    let docs_per_writer = env_usize(
        "MQLITE_PERF_WRITE_DOCS_PER_WRITER",
        DEFAULT_WRITE_DOCS_PER_WRITER,
    )
    .max(1);
    let batch_size = env_usize("MQLITE_PERF_WRITE_BATCH_SIZE", DEFAULT_WRITE_BATCH_SIZE).max(1);
    run_write_threads(WriteConfig {
        writers,
        docs_per_writer,
        batch_size,
        distinct_collections: false,
        mode,
    })
}

fn run_multi_collection_write_benchmark(
    mode: WriteMode,
) -> Result<Measurement, Box<dyn std::error::Error>> {
    let writers = env_usize(
        "MQLITE_PERF_MULTI_COLLECTION_WRITERS",
        DEFAULT_MULTI_COLLECTION_WRITERS,
    )
    .max(1);
    let docs_per_writer = env_usize(
        "MQLITE_PERF_WRITE_DOCS_PER_WRITER",
        DEFAULT_WRITE_DOCS_PER_WRITER,
    )
    .max(1);
    let batch_size = env_usize("MQLITE_PERF_WRITE_BATCH_SIZE", DEFAULT_WRITE_BATCH_SIZE).max(1);
    run_write_threads(WriteConfig {
        writers,
        docs_per_writer,
        batch_size,
        distinct_collections: true,
        mode,
    })
}

fn run_write_threads(config: WriteConfig) -> Result<Measurement, Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    let client = open_interval_client(&dir)?;
    let db = client.database("perf_goal");
    let collection_names = collection_names(&config);
    for collection_name in setup_collection_names(&config) {
        db.create_collection(&collection_name)?;
        db.collection::<Document>(&collection_name)
            .create_index(IndexModel::builder().keys(doc! { "indexed": 1 }).build())?;
    }

    let payload = "x".repeat(PAYLOAD_BYTES);
    let batch_size = config.batch_size.min(config.docs_per_writer);
    let batches_by_writer = (0..config.writers)
        .map(|writer| {
            let first_id = writer * KEY_BAND_WIDTH;
            build_batches(config.docs_per_writer, batch_size, first_id, &payload)
        })
        .collect::<Vec<_>>();
    let ready_barrier = Arc::new(Barrier::new(config.writers + 1));
    let start_barrier = Arc::new(Barrier::new(config.writers + 1));
    let mode = config.mode;
    let mut handles = Vec::with_capacity(config.writers);

    for (writer, batches) in batches_by_writer.into_iter().enumerate() {
        let client = client.clone();
        let collection_name = collection_names[writer].clone();
        let ready_barrier = Arc::clone(&ready_barrier);
        let start_barrier = Arc::clone(&start_barrier);
        handles.push(thread::spawn(move || -> Result<(), String> {
            let collection = client
                .database("perf_goal")
                .collection::<Document>(&collection_name);
            ready_barrier.wait();
            start_barrier.wait();
            match mode {
                WriteMode::Single => {
                    for batch in &batches {
                        for doc in batch {
                            collection.insert_one(doc).map_err(|err| err.to_string())?;
                        }
                    }
                }
                WriteMode::Batch => {
                    for batch in &batches {
                        collection
                            .insert_many(batch)
                            .run()
                            .map_err(|err| err.to_string())?;
                    }
                }
            }
            Ok(())
        }));
    }

    ready_barrier.wait();
    let start = Instant::now();
    start_barrier.wait();
    for handle in handles {
        match handle.join() {
            Ok(Ok(())) => {}
            Ok(Err(err)) => return Err(err.into()),
            Err(_) => return Err("write benchmark worker panicked".into()),
        }
    }

    Ok(measure(
        config.writers * config.docs_per_writer,
        start.elapsed(),
        config.writers,
    ))
}

fn collection_names(config: &WriteConfig) -> Vec<String> {
    if config.distinct_collections {
        (0..config.writers)
            .map(|writer| format!("write_docs_{writer}"))
            .collect()
    } else {
        vec!["write_docs".to_owned(); config.writers]
    }
}

fn setup_collection_names(config: &WriteConfig) -> Vec<String> {
    if config.distinct_collections {
        collection_names(config)
    } else {
        vec!["write_docs".to_owned()]
    }
}

fn build_batches(
    docs_per_writer: usize,
    batch_size: usize,
    first_id: usize,
    payload: &str,
) -> Vec<Vec<Document>> {
    (0..docs_per_writer)
        .collect::<Vec<_>>()
        .chunks(batch_size)
        .map(|chunk| {
            chunk
                .iter()
                .map(|offset| {
                    let id = (first_id + *offset) as i32;
                    doc! { "_id": id, "indexed": id, "payload": payload }
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

fn run_read_benchmark() -> Result<Measurement, Box<dyn std::error::Error>> {
    let seed_docs = env_i32("MQLITE_PERF_READ_SEED_DOCS", DEFAULT_READ_SEED_DOCS).max(1);
    let ops = env_usize("MQLITE_PERF_READ_OPS", DEFAULT_READ_OPS).max(1);
    let dir = TempDir::new()?;
    let client = open_interval_client(&dir)?;
    let db = client.database("perf_goal");
    db.create_collection("read_docs")?;
    let collection = db.collection::<Document>("read_docs");
    collection.create_index(IndexModel::builder().keys(doc! { "indexed": 1 }).build())?;
    let payload = "x".repeat(PAYLOAD_BYTES);
    let seed_docs_usize = seed_docs as usize;
    let seed_batch = (0..seed_docs)
        .map(|id| doc! { "_id": id, "indexed": id, "group": id % 16, "payload": payload.as_str() })
        .collect::<Vec<_>>();
    collection.insert_many(&seed_batch).run()?;

    let start = Instant::now();
    let mut checksum = 0i64;
    for op in 0..ops {
        let filter = match op % 4 {
            0 => doc! {},
            1 => doc! { "indexed": { "$gte": (op % seed_docs_usize) as i32 } },
            2 => doc! { "group": (op % 16) as i32 },
            _ => doc! { "indexed": (op % seed_docs_usize) as i32 },
        };
        let cursor = collection.find(filter).run()?;
        let mut saw_doc = false;
        for doc in cursor {
            let doc = doc?;
            saw_doc = true;
            if let Ok(id) = doc.get_i32("_id") {
                checksum += i64::from(id);
            }
        }
        if !saw_doc {
            return Err("read benchmark query returned no documents".into());
        }
    }

    if checksum < 0 {
        return Err("read benchmark checksum underflowed".into());
    }

    Ok(measure(ops, start.elapsed(), 1))
}

fn open_interval_client(dir: &TempDir) -> Result<Client, Box<dyn std::error::Error>> {
    let path = dir.path().join("perf-goal.mqlite");
    let opts = OpenOptions::new().durability(DurabilityMode::Interval(Duration::from_millis(
        DURABILITY_INTERVAL_MS,
    )));
    Ok(Client::open_with_options(path, opts)?)
}

fn measure(units: usize, elapsed: Duration, workers: usize) -> Measurement {
    let elapsed_secs = elapsed.as_secs_f64().max(f64::EPSILON);
    Measurement {
        workers,
        units,
        elapsed_secs,
        units_per_second: units as f64 / elapsed_secs,
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_i32(name: &str, default: i32) -> i32 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
