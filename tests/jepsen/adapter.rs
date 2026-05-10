//! Local Jepsen adapter for the embedded mqlite API.
//!
//! The adapter exposes a tiny localhost line protocol so Jepsen can drive the
//! real `mqlite::Client` and `Collection<Document>` API directly.

use mqlite::{
    doc, Bson, Client, Collection, Cursor, Document, Error, IndexModel, IndexOptions,
    ReturnDocument,
};
use std::{
    env,
    error::Error as StdError,
    io::{self, BufRead, BufReader, BufWriter, Write},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    sync::Arc,
    thread,
    time::Duration,
};

const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 27018;
const DB_NAME: &str = "jepsen";
const REGISTER_COLLECTION: &str = "register";
const SET_COLLECTION: &str = "set";
const UNIQUE_INDEX_COLLECTION: &str = "unique_index";
const SECONDARY_INDEX_COLLECTION: &str = "secondary_index";
const READ_YOUR_WRITES_COLLECTION: &str = "read_your_writes";
const DELETE_SET_COLLECTION: &str = "delete_set";
const NAMESPACE_A_COLLECTION: &str = "namespace_a";
const NAMESPACE_B_COLLECTION: &str = "namespace_b";
const COUNT_COLLECTION: &str = "count_consistency";
const INDEX_BUILD_COLLECTION: &str = "index_build";
const DROP_INDEX_COLLECTION: &str = "drop_index";
const COMPOUND_INDEX_COLLECTION: &str = "compound_index";
const MULTIKEY_INDEX_COLLECTION: &str = "multikey_index";
const CLAIM_JOBS_COLLECTION: &str = "claim_jobs";
const LONG_SCAN_COLLECTION: &str = "long_scan";
const BATCH_PREFIX_COLLECTION: &str = "batch_prefix";
const INDEX_KEY_MODULUS: i64 = 8;
const LONG_SCAN_DELAY_MS: u64 = 1;

type BoxError = Box<dyn StdError + Send + Sync + 'static>;
type BoxResult<T> = Result<T, BoxError>;

fn main() {
    if let Err(error) = run() {
        eprintln!("mqlite_jepsen_adapter: {error}");
        std::process::exit(1);
    }
}

fn run() -> BoxResult<()> {
    let config = Config::parse()?;
    let client = Client::open(&config.db_path)?;
    let db = client.database(DB_NAME);
    let state = Arc::new(AdapterState {
        registers: db.collection::<Document>(REGISTER_COLLECTION),
        set: db.collection::<Document>(SET_COLLECTION),
        unique_index: db.collection::<Document>(UNIQUE_INDEX_COLLECTION),
        secondary_index: db.collection::<Document>(SECONDARY_INDEX_COLLECTION),
        read_your_writes: db.collection::<Document>(READ_YOUR_WRITES_COLLECTION),
        delete_set: db.collection::<Document>(DELETE_SET_COLLECTION),
        namespace_a: db.collection::<Document>(NAMESPACE_A_COLLECTION),
        namespace_b: db.collection::<Document>(NAMESPACE_B_COLLECTION),
        count_consistency: db.collection::<Document>(COUNT_COLLECTION),
        index_build: db.collection::<Document>(INDEX_BUILD_COLLECTION),
        drop_index: db.collection::<Document>(DROP_INDEX_COLLECTION),
        compound_index: db.collection::<Document>(COMPOUND_INDEX_COLLECTION),
        multikey_index: db.collection::<Document>(MULTIKEY_INDEX_COLLECTION),
        claim_jobs: db.collection::<Document>(CLAIM_JOBS_COLLECTION),
        long_scan: db.collection::<Document>(LONG_SCAN_COLLECTION),
        batch_prefix: db.collection::<Document>(BATCH_PREFIX_COLLECTION),
    });
    let listener = TcpListener::bind((config.host.as_str(), config.port))?;

    println!(
        "mqlite Jepsen adapter listening on {} with database {}",
        listener.local_addr()?,
        config.db_path.display()
    );

    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                let state = Arc::clone(&state);
                thread::spawn(move || {
                    if let Err(error) = handle_connection(state, stream) {
                        eprintln!("mqlite_jepsen_adapter connection error: {error}");
                    }
                });
            }
            Err(error) => {
                eprintln!("mqlite_jepsen_adapter accept error: {error}");
            }
        }
    }

    Ok(())
}

struct Config {
    host: String,
    port: u16,
    db_path: PathBuf,
}

impl Config {
    fn parse() -> BoxResult<Self> {
        let mut host = DEFAULT_HOST.to_string();
        let mut port = DEFAULT_PORT;
        let mut db_path = None;
        let mut args = env::args().skip(1);

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                "--host" => {
                    host = required_arg(&mut args, "--host")?;
                }
                "--port" => {
                    let raw = required_arg(&mut args, "--port")?;
                    port = raw.parse().map_err(|error| {
                        invalid_input(format!("invalid --port value {raw}: {error}"))
                    })?;
                }
                "--db-path" => {
                    let path = required_arg(&mut args, "--db-path")?;
                    db_path = Some(PathBuf::from(path));
                }
                _ => {
                    return Err(invalid_input(format!("unknown option {arg}")).into());
                }
            }
        }

        let db_path = db_path.ok_or_else(|| invalid_input("--db-path is required"))?;
        Ok(Self {
            host,
            port,
            db_path,
        })
    }
}

fn print_usage() {
    println!(
        "{}",
        concat!(
            "Usage: mqlite_jepsen_adapter ",
            "[--host HOST] [--port PORT] --db-path PATH"
        )
    );
}

fn required_arg<I>(args: &mut I, flag: &str) -> BoxResult<String>
where
    I: Iterator<Item = String>,
{
    match args.next() {
        Some(value) => Ok(value),
        None => Err(invalid_input(format!("{flag} requires a value")).into()),
    }
}

struct AdapterState {
    registers: Collection<Document>,
    set: Collection<Document>,
    unique_index: Collection<Document>,
    secondary_index: Collection<Document>,
    read_your_writes: Collection<Document>,
    delete_set: Collection<Document>,
    namespace_a: Collection<Document>,
    namespace_b: Collection<Document>,
    count_consistency: Collection<Document>,
    index_build: Collection<Document>,
    drop_index: Collection<Document>,
    compound_index: Collection<Document>,
    multikey_index: Collection<Document>,
    claim_jobs: Collection<Document>,
    long_scan: Collection<Document>,
    batch_prefix: Collection<Document>,
}

impl AdapterState {
    fn handle(&self, request: Request) -> BoxResult<Response> {
        match request {
            Request::Ping => Ok(Response::Ok),
            Request::ReadRegister { key } => {
                let value = self.read_register(key)?;
                Ok(Response::Value(value))
            }
            Request::WriteRegister { key, value } => {
                self.write_register(key, value)?;
                Ok(Response::Ok)
            }
            Request::CasRegister { key, old, new } => {
                Ok(Response::Applied(self.cas_register(key, old, new)?))
            }
            Request::AddSet { value } => {
                self.add_set_item(value)?;
                Ok(Response::Ok)
            }
            Request::ReadSet => Ok(Response::Set(self.read_set()?)),
            Request::EnsureUniqueIndex => {
                self.ensure_unique_index()?;
                Ok(Response::Ok)
            }
            Request::UniqueInsert { id, value } => {
                Ok(Response::Applied(self.unique_insert(id, value)?))
            }
            Request::ReadUnique => Ok(Response::Pairs(self.read_unique()?)),
            Request::EnsureSecondaryIndex => {
                self.ensure_secondary_index()?;
                Ok(Response::Ok)
            }
            Request::SecondaryUpsert { id, value } => {
                self.secondary_upsert(id, value)?;
                Ok(Response::Ok)
            }
            Request::SecondaryDelete { id } => {
                self.secondary_delete(id)?;
                Ok(Response::Ok)
            }
            Request::SecondaryIndexRead { value } => {
                Ok(Response::Set(self.secondary_index_read(value)?))
            }
            Request::SecondaryScan => Ok(Response::Pairs(self.secondary_scan()?)),
            Request::ReadYourWrites { id, value } => {
                Ok(Response::Value(self.read_your_writes(id, value)?))
            }
            Request::SeedDeleteSet { count } => {
                self.seed_delete_set(count)?;
                Ok(Response::Ok)
            }
            Request::DeleteSet { id } => {
                self.delete_set_item(id)?;
                Ok(Response::Ok)
            }
            Request::ReadDeleteSet => Ok(Response::Set(self.read_delete_set()?)),
            Request::NamespaceAdd { namespace, value } => {
                self.namespace_add(namespace, value)?;
                Ok(Response::Ok)
            }
            Request::NamespaceRead { namespace } => {
                Ok(Response::Set(self.namespace_read(namespace)?))
            }
            Request::CountUpsert { id, value } => {
                self.count_upsert(id, value)?;
                Ok(Response::Ok)
            }
            Request::CountDelete { id } => {
                self.count_delete(id)?;
                Ok(Response::Ok)
            }
            Request::CountCheck => Ok(Response::Counts(self.count_check()?)),
            Request::SeedIndexBuild { count } => {
                self.seed_index_build(count)?;
                Ok(Response::Ok)
            }
            Request::IndexBuildCreate => {
                self.index_build_create()?;
                Ok(Response::Ok)
            }
            Request::IndexBuildUpsert { id, value } => {
                self.index_build_upsert(id, value)?;
                Ok(Response::Ok)
            }
            Request::IndexBuildDelete { id } => {
                self.index_build_delete(id)?;
                Ok(Response::Ok)
            }
            Request::IndexBuildIndexRead { value } => {
                Ok(Response::Set(self.index_build_index_read(value)?))
            }
            Request::IndexBuildScan => Ok(Response::Pairs(self.index_build_scan()?)),
            Request::SeedDropIndex { count } => {
                self.seed_drop_index(count)?;
                Ok(Response::Ok)
            }
            Request::DropIndexCreate => {
                self.drop_index_create()?;
                Ok(Response::Ok)
            }
            Request::DropIndexDrop => {
                self.drop_index_drop()?;
                Ok(Response::Ok)
            }
            Request::DropIndexUpsert { id, value } => {
                self.drop_index_upsert(id, value)?;
                Ok(Response::Ok)
            }
            Request::DropIndexDelete { id } => {
                self.drop_index_delete(id)?;
                Ok(Response::Ok)
            }
            Request::DropIndexIndexRead { value } => {
                Ok(Response::Set(self.drop_index_index_read(value)?))
            }
            Request::DropIndexScan => Ok(Response::Pairs(self.drop_index_scan()?)),
            Request::EnsureCompoundIndex => {
                self.ensure_compound_index()?;
                Ok(Response::Ok)
            }
            Request::CompoundUpsert { id, a, b } => {
                self.compound_upsert(id, a, b)?;
                Ok(Response::Ok)
            }
            Request::CompoundDelete { id } => {
                self.compound_delete(id)?;
                Ok(Response::Ok)
            }
            Request::CompoundIndexRead { a, b } => {
                Ok(Response::Set(self.compound_index_read(a, b)?))
            }
            Request::CompoundScan => Ok(Response::Triples(self.compound_scan()?)),
            Request::EnsureMultikeyIndex => {
                self.ensure_multikey_index()?;
                Ok(Response::Ok)
            }
            Request::MultikeyUpsert { id, value } => {
                self.multikey_upsert(id, value)?;
                Ok(Response::Ok)
            }
            Request::MultikeyDelete { id } => {
                self.multikey_delete(id)?;
                Ok(Response::Ok)
            }
            Request::MultikeyIndexRead { value } => {
                Ok(Response::Set(self.multikey_index_read(value)?))
            }
            Request::MultikeyScan => Ok(Response::Pairs(self.multikey_scan()?)),
            Request::SeedClaimJobs { count } => {
                self.seed_claim_jobs(count)?;
                Ok(Response::Ok)
            }
            Request::ClaimJob { worker } => Ok(Response::Value(self.claim_job(worker)?)),
            Request::ReadClaims => Ok(Response::Pairs(self.read_claims()?)),
            Request::SeedLongScan { count } => {
                self.seed_long_scan(count)?;
                Ok(Response::Ok)
            }
            Request::LongScanAdvance { epoch } => {
                self.long_scan_advance(epoch)?;
                Ok(Response::Ok)
            }
            Request::LongScanRead => Ok(Response::Set(self.long_scan_read()?)),
            Request::EnsureBatchPrefixIndex => {
                self.ensure_batch_prefix_index()?;
                Ok(Response::Ok)
            }
            Request::WriteBatchPrefix { base } => {
                Ok(Response::Batch(self.write_batch_prefix(base)?))
            }
            Request::ReadBatchPrefix => Ok(Response::Set(self.read_batch_prefix()?)),
        }
    }

    fn read_register(&self, key: i64) -> BoxResult<Option<i64>> {
        let doc = self.registers.find_one(doc! { "_id": key })?;
        match doc {
            Some(doc) => integer_field(&doc, "value"),
            None => Ok(None),
        }
    }

    fn write_register(&self, key: i64, value: i64) -> BoxResult<()> {
        let replacement = doc! { "_id": key, "value": value };

        loop {
            match self
                .registers
                .find_one_and_replace(doc! { "_id": key }, &replacement)
                .upsert(true)
                .run()
            {
                Ok(_) => return Ok(()),
                Err(Error::DuplicateKey { .. }) => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }

    fn cas_register(&self, key: i64, old: Option<i64>, new: i64) -> BoxResult<bool> {
        if let Some(old) = old {
            let updated = self
                .registers
                .find_one_and_update(
                    doc! { "_id": key, "value": old },
                    doc! { "$set": { "value": new } },
                )
                .return_document(ReturnDocument::After)
                .run()?;
            return Ok(updated.is_some());
        }

        match self
            .registers
            .insert_one(&doc! { "_id": key, "value": new })
        {
            Ok(_) => Ok(true),
            Err(Error::DuplicateKey { .. }) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    fn add_set_item(&self, value: i64) -> BoxResult<()> {
        match self.set.insert_one(&doc! { "_id": value }) {
            Ok(_) | Err(Error::DuplicateKey { .. }) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn read_set(&self) -> BoxResult<Vec<i64>> {
        read_ids(&self.set)
    }

    fn ensure_unique_index(&self) -> BoxResult<()> {
        let model = IndexModel::builder()
            .keys(doc! { "u": 1_i32 })
            .options(IndexOptions::new().unique(true).name("u_1"))
            .build();
        self.unique_index.create_index(model)?;
        Ok(())
    }

    fn unique_insert(&self, id: i64, value: i64) -> BoxResult<bool> {
        match self
            .unique_index
            .insert_one(&doc! { "_id": id, "u": value })
        {
            Ok(_) => Ok(true),
            Err(Error::DuplicateKey { .. }) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    fn read_unique(&self) -> BoxResult<Vec<(i64, i64)>> {
        read_pairs(&self.unique_index, "u")
    }

    fn ensure_secondary_index(&self) -> BoxResult<()> {
        let model = IndexModel::builder()
            .keys(doc! { "x": 1_i32 })
            .options(IndexOptions::new().name("x_1"))
            .build();
        self.secondary_index.create_index(model)?;
        Ok(())
    }

    fn secondary_upsert(&self, id: i64, value: i64) -> BoxResult<()> {
        upsert_pair(&self.secondary_index, id, "x", value)
    }

    fn secondary_delete(&self, id: i64) -> BoxResult<()> {
        self.secondary_index.delete_one(doc! { "_id": id })?;
        Ok(())
    }

    fn secondary_index_read(&self, value: i64) -> BoxResult<Vec<i64>> {
        read_ids_by_key(&self.secondary_index, "x", value)
    }

    fn secondary_scan(&self) -> BoxResult<Vec<(i64, i64)>> {
        read_pairs(&self.secondary_index, "x")
    }

    fn read_your_writes(&self, id: i64, value: i64) -> BoxResult<Option<i64>> {
        upsert_pair(&self.read_your_writes, id, "value", value)?;
        let doc = self.read_your_writes.find_one(doc! { "_id": id })?;
        match doc {
            Some(doc) => integer_field(&doc, "value"),
            None => Ok(None),
        }
    }

    fn seed_delete_set(&self, count: i64) -> BoxResult<()> {
        for id in 0..count {
            insert_id(&self.delete_set, id)?;
        }
        Ok(())
    }

    fn delete_set_item(&self, id: i64) -> BoxResult<()> {
        self.delete_set.delete_one(doc! { "_id": id })?;
        Ok(())
    }

    fn read_delete_set(&self) -> BoxResult<Vec<i64>> {
        read_ids(&self.delete_set)
    }

    fn namespace_add(&self, namespace: Namespace, value: i64) -> BoxResult<()> {
        insert_id(self.namespace_collection(namespace), value)
    }

    fn namespace_read(&self, namespace: Namespace) -> BoxResult<Vec<i64>> {
        read_ids(self.namespace_collection(namespace))
    }

    fn namespace_collection(&self, namespace: Namespace) -> &Collection<Document> {
        match namespace {
            Namespace::A => &self.namespace_a,
            Namespace::B => &self.namespace_b,
        }
    }

    fn count_upsert(&self, id: i64, value: i64) -> BoxResult<()> {
        upsert_pair(&self.count_consistency, id, "value", value)
    }

    fn count_delete(&self, id: i64) -> BoxResult<()> {
        self.count_consistency.delete_one(doc! { "_id": id })?;
        Ok(())
    }

    fn count_check(&self) -> BoxResult<(u64, u64)> {
        let exact = self.count_consistency.count_documents(doc! {})?;
        let scan = read_ids(&self.count_consistency)?.len() as u64;
        Ok((exact, scan))
    }

    fn seed_index_build(&self, count: i64) -> BoxResult<()> {
        for id in 0..count {
            upsert_pair(&self.index_build, id, "x", id % INDEX_KEY_MODULUS)?;
        }
        Ok(())
    }

    fn index_build_create(&self) -> BoxResult<()> {
        let model = IndexModel::builder()
            .keys(doc! { "x": 1_i32 })
            .options(IndexOptions::new().name("x_1"))
            .build();
        self.index_build.create_index(model)?;
        Ok(())
    }

    fn index_build_upsert(&self, id: i64, value: i64) -> BoxResult<()> {
        upsert_pair(&self.index_build, id, "x", value)
    }

    fn index_build_delete(&self, id: i64) -> BoxResult<()> {
        self.index_build.delete_one(doc! { "_id": id })?;
        Ok(())
    }

    fn index_build_index_read(&self, value: i64) -> BoxResult<Vec<i64>> {
        read_ids_by_key(&self.index_build, "x", value)
    }

    fn index_build_scan(&self) -> BoxResult<Vec<(i64, i64)>> {
        read_pairs(&self.index_build, "x")
    }

    fn seed_drop_index(&self, count: i64) -> BoxResult<()> {
        for id in 0..count {
            upsert_pair(&self.drop_index, id, "x", id % INDEX_KEY_MODULUS)?;
        }
        self.drop_index_create()
    }

    fn drop_index_create(&self) -> BoxResult<()> {
        let model = IndexModel::builder()
            .keys(doc! { "x": 1_i32 })
            .options(IndexOptions::new().name("x_1"))
            .build();
        self.drop_index.create_index(model)?;
        Ok(())
    }

    fn drop_index_drop(&self) -> BoxResult<()> {
        match self.drop_index.drop_index("x_1") {
            Ok(()) => Ok(()),
            Err(error) if missing_index_error(&error) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn drop_index_upsert(&self, id: i64, value: i64) -> BoxResult<()> {
        upsert_pair(&self.drop_index, id, "x", value)
    }

    fn drop_index_delete(&self, id: i64) -> BoxResult<()> {
        self.drop_index.delete_one(doc! { "_id": id })?;
        Ok(())
    }

    fn drop_index_index_read(&self, value: i64) -> BoxResult<Vec<i64>> {
        read_ids_by_key(&self.drop_index, "x", value)
    }

    fn drop_index_scan(&self) -> BoxResult<Vec<(i64, i64)>> {
        read_pairs(&self.drop_index, "x")
    }

    fn ensure_compound_index(&self) -> BoxResult<()> {
        let model = IndexModel::builder()
            .keys(doc! { "a": 1_i32, "b": 1_i32 })
            .options(IndexOptions::new().name("a_1_b_1"))
            .build();
        self.compound_index.create_index(model)?;
        Ok(())
    }

    fn compound_upsert(&self, id: i64, a: i64, b: i64) -> BoxResult<()> {
        let replacement = doc! { "_id": id, "a": a, "b": b };
        upsert_document(&self.compound_index, id, replacement)
    }

    fn compound_delete(&self, id: i64) -> BoxResult<()> {
        self.compound_index.delete_one(doc! { "_id": id })?;
        Ok(())
    }

    fn compound_index_read(&self, a: i64, b: i64) -> BoxResult<Vec<i64>> {
        read_ids_by_filter(&self.compound_index, doc! { "a": a, "b": b })
    }

    fn compound_scan(&self) -> BoxResult<Vec<(i64, i64, i64)>> {
        read_triples(&self.compound_index, "a", "b")
    }

    fn ensure_multikey_index(&self) -> BoxResult<()> {
        let model = IndexModel::builder()
            .keys(doc! { "tags": 1_i32 })
            .options(IndexOptions::new().name("tags_1"))
            .build();
        self.multikey_index.create_index(model)?;
        Ok(())
    }

    fn multikey_upsert(&self, id: i64, value: i64) -> BoxResult<()> {
        let next = (value + 1).rem_euclid(INDEX_KEY_MODULUS);
        let replacement = doc! {
            "_id": id,
            "tags": [value, next],
            "value": value,
        };
        upsert_document(&self.multikey_index, id, replacement)
    }

    fn multikey_delete(&self, id: i64) -> BoxResult<()> {
        self.multikey_index.delete_one(doc! { "_id": id })?;
        Ok(())
    }

    fn multikey_index_read(&self, value: i64) -> BoxResult<Vec<i64>> {
        read_ids_by_key(&self.multikey_index, "tags", value)
    }

    fn multikey_scan(&self) -> BoxResult<Vec<(i64, i64)>> {
        read_array_pairs(&self.multikey_index, "tags")
    }

    fn seed_claim_jobs(&self, count: i64) -> BoxResult<()> {
        for id in 0..count {
            let replacement = doc! { "_id": id, "claimed": false, "worker": -1_i64 };
            upsert_document(&self.claim_jobs, id, replacement)?;
        }
        Ok(())
    }

    fn claim_job(&self, worker: i64) -> BoxResult<Option<i64>> {
        let updated = self
            .claim_jobs
            .find_one_and_update(
                doc! { "claimed": false },
                doc! { "$set": { "claimed": true, "worker": worker } },
            )
            .return_document(ReturnDocument::After)
            .sort(doc! { "_id": 1_i32 })
            .run()?;
        match updated {
            Some(doc) => integer_field(&doc, "_id"),
            None => Ok(None),
        }
    }

    fn read_claims(&self) -> BoxResult<Vec<(i64, i64)>> {
        read_pairs_by_filter(&self.claim_jobs, doc! { "claimed": true }, "worker")
    }

    fn seed_long_scan(&self, count: i64) -> BoxResult<()> {
        for id in 0..count {
            let replacement = doc! { "_id": id, "epoch": 0_i64 };
            upsert_document(&self.long_scan, id, replacement)?;
        }
        Ok(())
    }

    fn long_scan_advance(&self, epoch: i64) -> BoxResult<()> {
        self.long_scan
            .update_many(doc! {}, doc! { "$set": { "epoch": epoch } })
            .run()?;
        Ok(())
    }

    fn long_scan_read(&self) -> BoxResult<Vec<i64>> {
        let cursor = self
            .long_scan
            .find(doc! {})
            .limit(0)
            .sort(doc! { "_id": 1_i32 })
            .run()?;
        let mut epochs = Vec::new();
        for doc in cursor {
            let doc = doc?;
            let epoch = integer_field(&doc, "epoch")?
                .ok_or_else(|| invalid_data("document is missing epoch"))?;
            epochs.push(epoch);
            thread::sleep(Duration::from_millis(LONG_SCAN_DELAY_MS));
        }
        epochs.sort_unstable();
        epochs.dedup();
        Ok(epochs)
    }

    fn ensure_batch_prefix_index(&self) -> BoxResult<()> {
        let model = IndexModel::builder()
            .keys(doc! { "u": 1_i32 })
            .options(IndexOptions::new().unique(true).name("u_1"))
            .build();
        self.batch_prefix.create_index(model)?;
        Ok(())
    }

    fn write_batch_prefix(&self, base: i64) -> BoxResult<(i64, i64)> {
        let id_base = base * 10;
        let docs = vec![
            doc! { "_id": id_base, "u": id_base },
            doc! { "_id": id_base + 1, "u": id_base + 1 },
            doc! { "_id": id_base + 2, "u": id_base },
            doc! { "_id": id_base + 3, "u": id_base + 3 },
            doc! { "_id": id_base + 4, "u": id_base + 4 },
        ];
        let result = self.batch_prefix.insert_many(&docs).ordered(true).run()?;
        let error_index = result
            .errors
            .first()
            .map(|error| error.index as i64)
            .unwrap_or(-1);
        Ok((result.inserted_ids.len() as i64, error_index))
    }

    fn read_batch_prefix(&self) -> BoxResult<Vec<i64>> {
        read_ids(&self.batch_prefix)
    }
}

#[derive(Clone, Copy)]
enum Namespace {
    A,
    B,
}

enum Request {
    Ping,
    ReadRegister {
        key: i64,
    },
    WriteRegister {
        key: i64,
        value: i64,
    },
    CasRegister {
        key: i64,
        old: Option<i64>,
        new: i64,
    },
    AddSet {
        value: i64,
    },
    ReadSet,
    EnsureUniqueIndex,
    UniqueInsert {
        id: i64,
        value: i64,
    },
    ReadUnique,
    EnsureSecondaryIndex,
    SecondaryUpsert {
        id: i64,
        value: i64,
    },
    SecondaryDelete {
        id: i64,
    },
    SecondaryIndexRead {
        value: i64,
    },
    SecondaryScan,
    ReadYourWrites {
        id: i64,
        value: i64,
    },
    SeedDeleteSet {
        count: i64,
    },
    DeleteSet {
        id: i64,
    },
    ReadDeleteSet,
    NamespaceAdd {
        namespace: Namespace,
        value: i64,
    },
    NamespaceRead {
        namespace: Namespace,
    },
    CountUpsert {
        id: i64,
        value: i64,
    },
    CountDelete {
        id: i64,
    },
    CountCheck,
    SeedIndexBuild {
        count: i64,
    },
    IndexBuildCreate,
    IndexBuildUpsert {
        id: i64,
        value: i64,
    },
    IndexBuildDelete {
        id: i64,
    },
    IndexBuildIndexRead {
        value: i64,
    },
    IndexBuildScan,
    SeedDropIndex {
        count: i64,
    },
    DropIndexCreate,
    DropIndexDrop,
    DropIndexUpsert {
        id: i64,
        value: i64,
    },
    DropIndexDelete {
        id: i64,
    },
    DropIndexIndexRead {
        value: i64,
    },
    DropIndexScan,
    EnsureCompoundIndex,
    CompoundUpsert {
        id: i64,
        a: i64,
        b: i64,
    },
    CompoundDelete {
        id: i64,
    },
    CompoundIndexRead {
        a: i64,
        b: i64,
    },
    CompoundScan,
    EnsureMultikeyIndex,
    MultikeyUpsert {
        id: i64,
        value: i64,
    },
    MultikeyDelete {
        id: i64,
    },
    MultikeyIndexRead {
        value: i64,
    },
    MultikeyScan,
    SeedClaimJobs {
        count: i64,
    },
    ClaimJob {
        worker: i64,
    },
    ReadClaims,
    SeedLongScan {
        count: i64,
    },
    LongScanAdvance {
        epoch: i64,
    },
    LongScanRead,
    EnsureBatchPrefixIndex,
    WriteBatchPrefix {
        base: i64,
    },
    ReadBatchPrefix,
}

enum Response {
    Ok,
    Value(Option<i64>),
    Applied(bool),
    Set(Vec<i64>),
    Pairs(Vec<(i64, i64)>),
    Triples(Vec<(i64, i64, i64)>),
    Counts((u64, u64)),
    Batch((i64, i64)),
    Error(String),
}

fn handle_connection(state: Arc<AdapterState>, stream: TcpStream) -> BoxResult<()> {
    let reader_stream = stream.try_clone()?;
    let mut reader = BufReader::new(reader_stream);
    let mut writer = BufWriter::new(stream);
    let mut line = String::new();

    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }

        let response = match parse_request(line.trim()) {
            Ok(request) => state
                .handle(request)
                .unwrap_or_else(|error| Response::Error(error.to_string())),
            Err(error) => Response::Error(error),
        };
        write_response(&mut writer, response)?;
    }

    Ok(())
}

fn parse_request(line: &str) -> Result<Request, String> {
    let mut tokens = line.split_whitespace();
    let command = next_token(&mut tokens, "command")?;

    match command {
        "ping" => {
            require_end(&mut tokens)?;
            Ok(Request::Ping)
        }
        "read-register" => {
            let key = parse_i64(next_token(&mut tokens, "key")?, "key")?;
            require_end(&mut tokens)?;
            Ok(Request::ReadRegister { key })
        }
        "write-register" => {
            let key = parse_i64(next_token(&mut tokens, "key")?, "key")?;
            let value = parse_i64(next_token(&mut tokens, "value")?, "value")?;
            require_end(&mut tokens)?;
            Ok(Request::WriteRegister { key, value })
        }
        "cas-register" => {
            let key = parse_i64(next_token(&mut tokens, "key")?, "key")?;
            let old_token = next_token(&mut tokens, "old value")?;
            let old = if old_token == "null" {
                None
            } else {
                Some(parse_i64(old_token, "old value")?)
            };
            let new = parse_i64(next_token(&mut tokens, "new value")?, "new value")?;
            require_end(&mut tokens)?;
            Ok(Request::CasRegister { key, old, new })
        }
        "add-set" => {
            let value = parse_i64(next_token(&mut tokens, "value")?, "value")?;
            require_end(&mut tokens)?;
            Ok(Request::AddSet { value })
        }
        "read-set" => {
            require_end(&mut tokens)?;
            Ok(Request::ReadSet)
        }
        "ensure-unique-index" => {
            require_end(&mut tokens)?;
            Ok(Request::EnsureUniqueIndex)
        }
        "unique-insert" => {
            let id = parse_i64(next_token(&mut tokens, "id")?, "id")?;
            let value = parse_i64(next_token(&mut tokens, "value")?, "value")?;
            require_end(&mut tokens)?;
            Ok(Request::UniqueInsert { id, value })
        }
        "read-unique" => {
            require_end(&mut tokens)?;
            Ok(Request::ReadUnique)
        }
        "ensure-secondary-index" => {
            require_end(&mut tokens)?;
            Ok(Request::EnsureSecondaryIndex)
        }
        "secondary-upsert" => {
            let id = parse_i64(next_token(&mut tokens, "id")?, "id")?;
            let value = parse_i64(next_token(&mut tokens, "value")?, "value")?;
            require_end(&mut tokens)?;
            Ok(Request::SecondaryUpsert { id, value })
        }
        "secondary-delete" => {
            let id = parse_i64(next_token(&mut tokens, "id")?, "id")?;
            require_end(&mut tokens)?;
            Ok(Request::SecondaryDelete { id })
        }
        "secondary-index-read" => {
            let value = parse_i64(next_token(&mut tokens, "value")?, "value")?;
            require_end(&mut tokens)?;
            Ok(Request::SecondaryIndexRead { value })
        }
        "secondary-scan" => {
            require_end(&mut tokens)?;
            Ok(Request::SecondaryScan)
        }
        "read-your-writes" => {
            let id = parse_i64(next_token(&mut tokens, "id")?, "id")?;
            let value = parse_i64(next_token(&mut tokens, "value")?, "value")?;
            require_end(&mut tokens)?;
            Ok(Request::ReadYourWrites { id, value })
        }
        "seed-delete-set" => {
            let count = parse_i64(next_token(&mut tokens, "count")?, "count")?;
            require_end(&mut tokens)?;
            Ok(Request::SeedDeleteSet { count })
        }
        "delete-set" => {
            let id = parse_i64(next_token(&mut tokens, "id")?, "id")?;
            require_end(&mut tokens)?;
            Ok(Request::DeleteSet { id })
        }
        "read-delete-set" => {
            require_end(&mut tokens)?;
            Ok(Request::ReadDeleteSet)
        }
        "namespace-add" => {
            let namespace = parse_namespace(next_token(&mut tokens, "namespace")?)?;
            let value = parse_i64(next_token(&mut tokens, "value")?, "value")?;
            require_end(&mut tokens)?;
            Ok(Request::NamespaceAdd { namespace, value })
        }
        "namespace-read" => {
            let namespace = parse_namespace(next_token(&mut tokens, "namespace")?)?;
            require_end(&mut tokens)?;
            Ok(Request::NamespaceRead { namespace })
        }
        "count-upsert" => {
            let id = parse_i64(next_token(&mut tokens, "id")?, "id")?;
            let value = parse_i64(next_token(&mut tokens, "value")?, "value")?;
            require_end(&mut tokens)?;
            Ok(Request::CountUpsert { id, value })
        }
        "count-delete" => {
            let id = parse_i64(next_token(&mut tokens, "id")?, "id")?;
            require_end(&mut tokens)?;
            Ok(Request::CountDelete { id })
        }
        "count-check" => {
            require_end(&mut tokens)?;
            Ok(Request::CountCheck)
        }
        "seed-index-build" => {
            let count = parse_i64(next_token(&mut tokens, "count")?, "count")?;
            require_end(&mut tokens)?;
            Ok(Request::SeedIndexBuild { count })
        }
        "index-build-create" => {
            require_end(&mut tokens)?;
            Ok(Request::IndexBuildCreate)
        }
        "index-build-upsert" => {
            let id = parse_i64(next_token(&mut tokens, "id")?, "id")?;
            let value = parse_i64(next_token(&mut tokens, "value")?, "value")?;
            require_end(&mut tokens)?;
            Ok(Request::IndexBuildUpsert { id, value })
        }
        "index-build-delete" => {
            let id = parse_i64(next_token(&mut tokens, "id")?, "id")?;
            require_end(&mut tokens)?;
            Ok(Request::IndexBuildDelete { id })
        }
        "index-build-index-read" => {
            let value = parse_i64(next_token(&mut tokens, "value")?, "value")?;
            require_end(&mut tokens)?;
            Ok(Request::IndexBuildIndexRead { value })
        }
        "index-build-scan" => {
            require_end(&mut tokens)?;
            Ok(Request::IndexBuildScan)
        }
        "seed-drop-index" => {
            let count = parse_i64(next_token(&mut tokens, "count")?, "count")?;
            require_end(&mut tokens)?;
            Ok(Request::SeedDropIndex { count })
        }
        "drop-index-create" => {
            require_end(&mut tokens)?;
            Ok(Request::DropIndexCreate)
        }
        "drop-index-drop" => {
            require_end(&mut tokens)?;
            Ok(Request::DropIndexDrop)
        }
        "drop-index-upsert" => {
            let id = parse_i64(next_token(&mut tokens, "id")?, "id")?;
            let value = parse_i64(next_token(&mut tokens, "value")?, "value")?;
            require_end(&mut tokens)?;
            Ok(Request::DropIndexUpsert { id, value })
        }
        "drop-index-delete" => {
            let id = parse_i64(next_token(&mut tokens, "id")?, "id")?;
            require_end(&mut tokens)?;
            Ok(Request::DropIndexDelete { id })
        }
        "drop-index-index-read" => {
            let value = parse_i64(next_token(&mut tokens, "value")?, "value")?;
            require_end(&mut tokens)?;
            Ok(Request::DropIndexIndexRead { value })
        }
        "drop-index-scan" => {
            require_end(&mut tokens)?;
            Ok(Request::DropIndexScan)
        }
        "ensure-compound-index" => {
            require_end(&mut tokens)?;
            Ok(Request::EnsureCompoundIndex)
        }
        "compound-upsert" => {
            let id = parse_i64(next_token(&mut tokens, "id")?, "id")?;
            let a = parse_i64(next_token(&mut tokens, "a")?, "a")?;
            let b = parse_i64(next_token(&mut tokens, "b")?, "b")?;
            require_end(&mut tokens)?;
            Ok(Request::CompoundUpsert { id, a, b })
        }
        "compound-delete" => {
            let id = parse_i64(next_token(&mut tokens, "id")?, "id")?;
            require_end(&mut tokens)?;
            Ok(Request::CompoundDelete { id })
        }
        "compound-index-read" => {
            let a = parse_i64(next_token(&mut tokens, "a")?, "a")?;
            let b = parse_i64(next_token(&mut tokens, "b")?, "b")?;
            require_end(&mut tokens)?;
            Ok(Request::CompoundIndexRead { a, b })
        }
        "compound-scan" => {
            require_end(&mut tokens)?;
            Ok(Request::CompoundScan)
        }
        "ensure-multikey-index" => {
            require_end(&mut tokens)?;
            Ok(Request::EnsureMultikeyIndex)
        }
        "multikey-upsert" => {
            let id = parse_i64(next_token(&mut tokens, "id")?, "id")?;
            let value = parse_i64(next_token(&mut tokens, "value")?, "value")?;
            require_end(&mut tokens)?;
            Ok(Request::MultikeyUpsert { id, value })
        }
        "multikey-delete" => {
            let id = parse_i64(next_token(&mut tokens, "id")?, "id")?;
            require_end(&mut tokens)?;
            Ok(Request::MultikeyDelete { id })
        }
        "multikey-index-read" => {
            let value = parse_i64(next_token(&mut tokens, "value")?, "value")?;
            require_end(&mut tokens)?;
            Ok(Request::MultikeyIndexRead { value })
        }
        "multikey-scan" => {
            require_end(&mut tokens)?;
            Ok(Request::MultikeyScan)
        }
        "seed-claim-jobs" => {
            let count = parse_i64(next_token(&mut tokens, "count")?, "count")?;
            require_end(&mut tokens)?;
            Ok(Request::SeedClaimJobs { count })
        }
        "claim-job" => {
            let worker = parse_i64(next_token(&mut tokens, "worker")?, "worker")?;
            require_end(&mut tokens)?;
            Ok(Request::ClaimJob { worker })
        }
        "read-claims" => {
            require_end(&mut tokens)?;
            Ok(Request::ReadClaims)
        }
        "seed-long-scan" => {
            let count = parse_i64(next_token(&mut tokens, "count")?, "count")?;
            require_end(&mut tokens)?;
            Ok(Request::SeedLongScan { count })
        }
        "long-scan-advance" => {
            let epoch = parse_i64(next_token(&mut tokens, "epoch")?, "epoch")?;
            require_end(&mut tokens)?;
            Ok(Request::LongScanAdvance { epoch })
        }
        "long-scan-read" => {
            require_end(&mut tokens)?;
            Ok(Request::LongScanRead)
        }
        "ensure-batch-prefix-index" => {
            require_end(&mut tokens)?;
            Ok(Request::EnsureBatchPrefixIndex)
        }
        "write-batch-prefix" => {
            let base = parse_i64(next_token(&mut tokens, "base")?, "base")?;
            require_end(&mut tokens)?;
            Ok(Request::WriteBatchPrefix { base })
        }
        "read-batch-prefix" => {
            require_end(&mut tokens)?;
            Ok(Request::ReadBatchPrefix)
        }
        _ => Err(format!("unknown command {command}")),
    }
}

fn next_token<'a, I>(tokens: &mut I, name: &str) -> Result<&'a str, String>
where
    I: Iterator<Item = &'a str>,
{
    tokens.next().ok_or_else(|| format!("missing {name}"))
}

fn require_end<'a, I>(tokens: &mut I) -> Result<(), String>
where
    I: Iterator<Item = &'a str>,
{
    match tokens.next() {
        Some(extra) => Err(format!("unexpected token {extra}")),
        None => Ok(()),
    }
}

fn parse_i64(token: &str, name: &str) -> Result<i64, String> {
    token
        .parse()
        .map_err(|error| format!("invalid {name} {token}: {error}"))
}

fn parse_namespace(token: &str) -> Result<Namespace, String> {
    match token {
        "a" => Ok(Namespace::A),
        "b" => Ok(Namespace::B),
        _ => Err(format!("invalid namespace {token}")),
    }
}

fn write_response(writer: &mut impl Write, response: Response) -> io::Result<()> {
    match response {
        Response::Ok => writeln!(writer, "ok")?,
        Response::Value(Some(value)) => writeln!(writer, "value {value}")?,
        Response::Value(None) => writeln!(writer, "value null")?,
        Response::Applied(applied) => writeln!(writer, "applied {applied}")?,
        Response::Set(values) => {
            write!(writer, "set")?;
            for value in values {
                write!(writer, " {value}")?;
            }
            writeln!(writer)?;
        }
        Response::Pairs(values) => {
            write!(writer, "pairs")?;
            for (id, value) in values {
                write!(writer, " {id}:{value}")?;
            }
            writeln!(writer)?;
        }
        Response::Triples(values) => {
            write!(writer, "triples")?;
            for (id, first, second) in values {
                write!(writer, " {id}:{first}:{second}")?;
            }
            writeln!(writer)?;
        }
        Response::Counts((exact, scan)) => writeln!(writer, "counts {exact} {scan}")?,
        Response::Batch((inserted_count, error_index)) => {
            writeln!(writer, "batch {inserted_count} {error_index}")?
        }
        Response::Error(message) => {
            let message = single_line(&message);
            writeln!(writer, "error {message}")?;
        }
    }
    writer.flush()
}

fn insert_id(collection: &Collection<Document>, id: i64) -> BoxResult<()> {
    match collection.insert_one(&doc! { "_id": id }) {
        Ok(_) | Err(Error::DuplicateKey { .. }) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn upsert_pair(
    collection: &Collection<Document>,
    id: i64,
    field: &str,
    value: i64,
) -> BoxResult<()> {
    let mut replacement = doc! { "_id": id };
    replacement.insert(field, value);
    upsert_document(collection, id, replacement)
}

fn upsert_document(
    collection: &Collection<Document>,
    id: i64,
    replacement: Document,
) -> BoxResult<()> {
    loop {
        match collection
            .find_one_and_replace(doc! { "_id": id }, &replacement)
            .upsert(true)
            .run()
        {
            Ok(_) => return Ok(()),
            Err(Error::DuplicateKey { .. }) => continue,
            Err(error) => return Err(error.into()),
        }
    }
}

fn read_ids(collection: &Collection<Document>) -> BoxResult<Vec<i64>> {
    let cursor = collection.find(doc! {}).limit(0).run()?;
    let mut values = Vec::new();

    for doc in cursor {
        let doc = doc?;
        let value =
            integer_field(&doc, "_id")?.ok_or_else(|| invalid_data("document is missing _id"))?;
        values.push(value);
    }

    values.sort_unstable();
    Ok(values)
}

fn read_ids_by_filter(collection: &Collection<Document>, filter: Document) -> BoxResult<Vec<i64>> {
    let cursor = collection.find(filter).limit(0).run()?;
    read_ids_from_cursor(cursor)
}

fn read_ids_by_key(
    collection: &Collection<Document>,
    field: &str,
    value: i64,
) -> BoxResult<Vec<i64>> {
    let mut filter = Document::new();
    filter.insert(field, value);
    let cursor = collection.find(filter).limit(0).run()?;
    read_ids_from_cursor(cursor)
}

fn read_ids_from_cursor(cursor: Cursor<Document>) -> BoxResult<Vec<i64>> {
    let mut ids = Vec::new();

    for doc in cursor {
        let doc = doc?;
        let id =
            integer_field(&doc, "_id")?.ok_or_else(|| invalid_data("document is missing _id"))?;
        ids.push(id);
    }

    ids.sort_unstable();
    Ok(ids)
}

fn read_pairs(collection: &Collection<Document>, field: &str) -> BoxResult<Vec<(i64, i64)>> {
    read_pairs_by_filter(collection, doc! {}, field)
}

fn read_pairs_by_filter(
    collection: &Collection<Document>,
    filter: Document,
    field: &str,
) -> BoxResult<Vec<(i64, i64)>> {
    let cursor = collection.find(filter).limit(0).run()?;
    read_pairs_from_cursor(cursor, field)
}

fn read_pairs_from_cursor(cursor: Cursor<Document>, field: &str) -> BoxResult<Vec<(i64, i64)>> {
    let mut pairs = Vec::new();

    for doc in cursor {
        let doc = doc?;
        let id =
            integer_field(&doc, "_id")?.ok_or_else(|| invalid_data("document is missing _id"))?;
        let value = integer_field(&doc, field)?
            .ok_or_else(|| invalid_data(format!("document is missing {field}")))?;
        pairs.push((id, value));
    }

    pairs.sort_unstable();
    Ok(pairs)
}

fn read_triples(
    collection: &Collection<Document>,
    first_field: &str,
    second_field: &str,
) -> BoxResult<Vec<(i64, i64, i64)>> {
    let cursor = collection.find(doc! {}).limit(0).run()?;
    let mut triples = Vec::new();

    for doc in cursor {
        let doc = doc?;
        let id =
            integer_field(&doc, "_id")?.ok_or_else(|| invalid_data("document is missing _id"))?;
        let first = integer_field(&doc, first_field)?
            .ok_or_else(|| invalid_data(format!("document is missing {first_field}")))?;
        let second = integer_field(&doc, second_field)?
            .ok_or_else(|| invalid_data(format!("document is missing {second_field}")))?;
        triples.push((id, first, second));
    }

    triples.sort_unstable();
    Ok(triples)
}

fn read_array_pairs(collection: &Collection<Document>, field: &str) -> BoxResult<Vec<(i64, i64)>> {
    let cursor = collection.find(doc! {}).limit(0).run()?;
    let mut pairs = Vec::new();

    for doc in cursor {
        let doc = doc?;
        let id =
            integer_field(&doc, "_id")?.ok_or_else(|| invalid_data("document is missing _id"))?;
        match doc.get(field) {
            Some(Bson::Array(values)) => {
                for value in values {
                    pairs.push((id, bson_integer(value, field)?));
                }
            }
            Some(value) => {
                let message = format!("field {field} is not an array: {value:?}");
                return Err(invalid_data(message).into());
            }
            None => {
                return Err(invalid_data(format!("document is missing {field}")).into());
            }
        }
    }

    pairs.sort_unstable();
    Ok(pairs)
}

fn missing_index_error(error: &Error) -> bool {
    matches!(error, Error::Internal(message) if message.contains("not found"))
}

fn bson_integer(value: &Bson, field: &str) -> BoxResult<i64> {
    match value {
        Bson::Int32(value) => Ok(i64::from(*value)),
        Bson::Int64(value) => Ok(*value),
        value => {
            let message = format!("field {field} array value is not an integer: {value:?}");
            Err(invalid_data(message).into())
        }
    }
}

fn integer_field(doc: &Document, field: &str) -> BoxResult<Option<i64>> {
    match doc.get(field) {
        Some(Bson::Int32(value)) => Ok(Some(i64::from(*value))),
        Some(Bson::Int64(value)) => Ok(Some(*value)),
        Some(value) => {
            let message = format!("field {field} is not an integer: {value:?}");
            Err(invalid_data(message).into())
        }
        None => Ok(None),
    }
}

fn invalid_input(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn single_line(message: &str) -> String {
    let mut output = String::new();

    for part in message.split_whitespace() {
        if !output.is_empty() {
            output.push(' ');
        }
        output.push_str(part);
    }

    if output.is_empty() {
        "unknown error".to_string()
    } else {
        output
    }
}
