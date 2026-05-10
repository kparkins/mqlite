# mqlite Jepsen Suite

This directory contains Jepsen tests that are applicable to mqlite's current
architecture: an embedded document store backed by one durable database file.
The suite starts a tiny localhost adapter which uses `mqlite::Client` and
`Collection<Document>` directly; the adapter exists only so Jepsen can issue
concurrent operations from Clojure.

It does not run partition, election, majority, or replica-set tests. mqlite is
not a distributed replica set today, so those nemeses would test behavior the
implementation does not claim to provide.

## Workloads

- `register`: uses Jepsen's `jepsen.tests.linearizable-register` generator and
  Knossos checker against one collection of independent registers. Operations
  are reads, writes, and compare-and-set implemented with embedded mqlite API
  calls.
- `set`: uses Jepsen's `checker/set` against unique acknowledged inserts,
  followed by final reads after recovery. This checks for lost acknowledged
  writes across process restarts.
- `unique-index`: creates a real unique secondary index through
  `Collection::create_index`, races duplicate-key inserts, then checks that no
  duplicate indexed value exists and no acknowledged insert was lost.
- `secondary-index`: creates a real non-unique secondary index, mixes upserts
  and deletes, then compares indexed reads against full collection scans after
  recovery.
- `read-your-writes`: writes a document through the embedded API and
  immediately reads it back through the same adapter process, checking that
  acknowledged writes are visible.
- `delete-set`: preloads documents, races acknowledged deletes, and checks
  after recovery that deleted keys do not reappear.
- `namespace-isolation`: writes disjoint values to two collections at the same
  time and checks that acknowledged writes survive in the right collection only.
- `count-consistency`: mixes upserts and deletes, then compares
  `count_documents({})` with a full collection scan after recovery.
- `index-build`: seeds documents, races `create_index` with upserts/deletes,
  and checks the final indexed reads against full scans.
- `drop-index`: seeds documents, races `drop_index`/`create_index` with
  upserts/deletes, then recreates the index and checks final indexed reads
  against full scans.
- `compound-index`: creates a real `{a: 1, b: 1}` index, mixes upserts and
  deletes, then compares compound indexed reads against full scans.
- `multikey-index`: creates a real array-field index, mixes array upserts and
  deletes, then compares tag indexed reads against full scans.
- `find-and-modify-claim`: preloads unclaimed jobs, races
  `find_one_and_update` claim operations, and checks that no job is claimed by
  more than one acknowledged worker.
- `long-scan-snapshot`: races collection-wide epoch updates with ordered scans
  and checks that each scan observes at most one epoch.
- `write-batch-prefix`: creates a unique index and runs ordered `insert_many`
  batches with a duplicate at index 2, checking that the acknowledged prefix is
  durable and the suffix is not inserted.

All workloads can run with the `restart` nemesis, which repeatedly kills and
restarts the local adapter process against the same database path.

## Running

From the repository root:

```sh
./tests/jepsen/run.sh
```

Useful options:

```sh
./tests/jepsen/run.sh --workload register --time-limit 20 --rate 40
./tests/jepsen/run.sh --workload set --nemesis restart --concurrency 8
./tests/jepsen/run.sh --workload unique-index --time-limit 20
./tests/jepsen/run.sh --workload secondary-index --rate 50
./tests/jepsen/run.sh --workload index-build --time-limit 20
./tests/jepsen/run.sh --workload drop-index --time-limit 20
./tests/jepsen/run.sh --workload find-and-modify-claim --concurrency 16
./tests/jepsen/run.sh --workload write-batch-prefix --nemesis restart
./tests/jepsen/run.sh --workload all --nemesis none
```

Requirements:

- Rust/Cargo, to build `mqlite_jepsen_adapter`
- Java 21 or newer
- `clojure` CLI or `lein`

Jepsen artifacts are written under `tests/jepsen/store/`. mqlite database files
and server logs are written under `target/jepsen/`.
