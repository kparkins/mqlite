# Data Model Design

## Summary

mqlite's data model bridges MongoDB's document semantics with a custom B+ tree storage engine in a single file. Documents are stored as BSON (using the official `bson` crate), with a 16MB maximum document size matching MongoDB 8.0. The storage format uses variable-page B+ trees — 4KB internal nodes for navigation and 32KB leaf pages optimized for large MongoDB documents — with overflow pages for documents exceeding leaf page capacity. Every collection has an automatic `_id` index; single-field, compound, and multikey indexes are supported in Phase 1 to enable the full query operator set ($elemMatch, $all, $size require multikey indexes for non-scan performance). The catalog — mapping collection names to root pages and index metadata — is itself stored as a reserved B+ tree at a fixed location in the file header.

The most consequential data model decisions are: (1) BSON comparison ordering must be encoded into B+ tree keys from day one — MongoDB's type-aware ordering (MinKey < Null < Number < String < Object < Array < ...) is not retrofittable and affects every index scan, (2) variable page sizes require a page allocator that functions as a mini filesystem with free lists per size class, and (3) the WAL uses a page-level redo log that records complete page images, enabling the three-file model (main + WAL + SHM) that collapses to a single file on clean close. ObjectId generation uses MongoDB-compatible format (4-byte timestamp + 5-byte random + 3-byte counter) to ensure wire protocol and driver compatibility.

## Analysis

### Key Considerations

- **BSON is the canonical document format.** Documents are stored as serialized BSON on disk. No intermediate format, no row decomposition. This simplifies the storage layer but means every document access requires BSON deserialization. The `bson` crate handles serialization/deserialization and is battle-tested.
- **Variable page sizes are confirmed (Q7).** 4KB internal pages keep the B+ tree shallow (fewer I/O for traversal), while 32KB leaf pages reduce the number of overflow pages needed for typical MongoDB documents (average 1-5KB). This is more complex than uniform pages but justified by the document size distribution.
- **BSON comparison ordering is foundational.** MongoDB defines a total order across all BSON types: MinKey < Null < Numbers < Symbol < String < Object < Array < BinData < ObjectId < Boolean < Date < Timestamp < RegExp < MaxKey. This ordering must be encoded into every index key. Getting this wrong means every indexed query returns wrong results.
- **Multikey indexes are required for Phase 1.** The operator set includes $elemMatch, $all, and $size. Without multikey indexes, array queries require full collection scans. A document with an array field like `tags: ["a", "b", "c"]` generates three index entries for a single document.
- **The catalog is a critical data structure.** It maps collection names to their data B+ tree root page and lists all indexes (each with their own B+ tree root page). Corruption of the catalog makes the entire database unreadable. It deserves checksumming and potentially redundant storage.
- **WAL operates at page granularity.** The WAL records complete page images (before or after, depending on design). This is simpler than a logical log and enables straightforward crash recovery: replay page writes from WAL to main file.

### Options Explored

#### Option 1: Uniform 4KB Pages with Overflow

- **Description**: All pages (internal and leaf) are 4KB. Documents larger than ~3.5KB (page minus header) are stored across overflow pages linked via pointers.
- **Pros**: Simple page allocator (one size class). Well-understood from SQLite. No variable-size complexity.
- **Cons**: Typical MongoDB documents (1-5KB) frequently spill to overflow, causing 2x I/O. Large documents (100KB+) require chains of 25+ overflow pages, fragmenting reads. Poor locality for document scans.
- **Effort**: Low.

#### Option 2: Uniform 32KB Pages

- **Description**: All pages are 32KB. Internal and leaf nodes use the same page size.
- **Pros**: Most documents fit in a single page. Simple allocator. Good for document scans.
- **Cons**: Internal nodes waste space — a 32KB internal node with 8-byte keys holds ~4000 entries but B+ tree fan-out of 500-1000 is already sufficient with 4KB pages. 32KB pages mean more wasted space for small collections. Higher memory consumption in buffer pool (each cached page = 32KB).
- **Effort**: Low.

#### Option 3: Variable Pages — 4KB Internal / 32KB Leaf (Recommended)

- **Description**: Internal (branch) nodes use 4KB pages for high fan-out with minimal memory. Leaf nodes use 32KB pages to accommodate typical MongoDB documents without overflow. Overflow pages (32KB) handle documents that don't fit in a single leaf.
- **Pros**: Optimized for the actual workload — navigation is cheap (4KB reads), data access is efficient (most docs fit in one 32KB page). Reduces overflow page chains significantly. Better buffer pool utilization (4KB pages for hot internal nodes, 32KB only for accessed leaves).
- **Cons**: Two size classes require a page allocator with separate free lists. Split/merge logic must handle the size transition between internal and leaf levels. More complex to implement correctly.
- **Effort**: Medium-High.

#### Option 4: Log-Structured Merge (LSM) Tree

- **Description**: Use an LSM tree instead of B+ tree. Writes go to an in-memory memtable, periodically flushed to sorted runs on disk. Background compaction merges runs.
- **Pros**: Excellent write throughput. Used by RocksDB, WiredTiger (optionally), LevelDB.
- **Cons**: Read amplification for point queries (check multiple levels). Compaction creates unpredictable latency spikes. Significantly more complex than B+ tree for a single-file embedded database. Space amplification during compaction. Not compatible with the single-file-when-closed model.
- **Effort**: Very High.

### Recommendation

**Option 3: Variable Pages (4KB internal / 32KB leaf).** This is confirmed by the human's Q7 answer. Implementation plan:

1. **Page allocator**: Maintain two free lists (4KB, 32KB) in the file header. Allocate from free list first, extend file if empty. Track free pages via a bitmap or linked list in reserved header pages.
2. **B+ tree**: Standard B+ tree with variable node sizes. Internal nodes (4KB) store keys + child page pointers. Leaf nodes (32KB) store keys + document values. Leaf-to-leaf sibling pointers for range scans.
3. **Overflow**: Documents exceeding ~31KB (leaf page minus header) are stored in overflow page chains. The leaf entry contains a pointer to the first overflow page. Overflow pages are 32KB, linked.
4. **Key encoding**: Implement MongoDB's BSON comparison ordering as a byte-comparable encoding. Each BSON type gets a one-byte type tag prefix, followed by the type-specific encoding (numbers in big-endian, strings as UTF-8 with null terminator, etc.). Compound index keys are concatenated encodings.

## File Format Specification

### File Header (Page 0 — 4KB)

```
Offset  Size   Field
0       4      Magic bytes: "MQLT" (0x4D514C54)
4       4      Format version: uint32 (start at 1)
8       4      Page size internal: uint32 (4096)
12      4      Page size leaf: uint32 (32768)
16      8      Database creation timestamp: uint64 (Unix millis)
24      8      Last checkpoint timestamp: uint64
32      4      Catalog root page: uint32 (page number of catalog B+ tree root)
36      4      Free list head (4KB): uint32 (page number)
40      4      Free list head (32KB): uint32 (page number)
44      4      Total page count: uint32
48      4      Free page count (4KB): uint32
52      4      Free page count (32KB): uint32
56      4      Checksum algorithm: uint32 (1 = CRC32C)
60      4      Header checksum: CRC32C of bytes 0-59
64      4      WAL salt 1: uint32 (random, for WAL file association)
68      4      WAL salt 2: uint32
72      56     Reserved (zero-filled, for future use — encryption metadata, etc.)
128     3968   Unused (padding to 4KB)
```

### Page Format — Internal Node (4KB)

```
Offset  Size   Field
0       1      Page type: 0x01 (internal)
1       1      Level: uint8 (distance from leaf level)
2       2      Key count: uint16
4       4      Page checksum: CRC32C
8       4      Right-most child pointer: uint32 (page number)
12      ...    Key entries: [encoded_key_length(2) | encoded_key(var) | child_page(4)]
```

Fan-out: With average 20-byte encoded keys, a 4KB internal node holds ~150 keys. A 3-level tree (root + 2 internal levels + leaves) can address 150^3 = 3.375 million leaf pages, or ~100GB of data.

### Page Format — Leaf Node (32KB)

```
Offset  Size   Field
0       1      Page type: 0x02 (leaf)
1       1      Flags: bit 0 = has overflow entries
2       2      Entry count: uint16
4       4      Page checksum: CRC32C
8       4      Next leaf page: uint32 (sibling pointer for range scans, 0 = none)
12      4      Prev leaf page: uint32 (sibling pointer)
16      2      Free space offset: uint16 (start of free region)
18      2      Cell pointer array offset: uint16
20      ...    Cell pointer array: [uint16 offset] × entry_count
...     ...    Free space
...     ...    Cell data (grows from end of page toward cell pointer array)
```

Each cell in a leaf page:
```
encoded_key_length(2) | encoded_key(var) | value_type(1) | value_data(var)
```

Value types:
- `0x01`: Inline BSON document (length-prefixed)
- `0x02`: Overflow pointer (page_number: uint32, total_length: uint32)

### Overflow Page (32KB)

```
Offset  Size   Field
0       1      Page type: 0x03 (overflow)
1       3      Reserved
4       4      Page checksum: CRC32C
8       4      Next overflow page: uint32 (0 = last in chain)
12      4      Data length in this page: uint32
16      ...    Raw data (BSON fragment)
```

## BSON Key Encoding

Index keys must be encoded as byte strings that sort correctly using `memcmp`. This is the BSON comparison order:

| Priority | BSON Type | Type Tag | Encoding Notes |
|----------|-----------|----------|----------------|
| 1 | MinKey | 0x00 | Single byte |
| 2 | Null | 0x05 | Single byte |
| 3 | Numbers (Int32, Int64, Double, Decimal128) | 0x10 | Convert to common comparable format. IEEE 754 double with sign-magnitude to unsigned transform. |
| 4 | Symbol | 0x15 | Deprecated, treat as string |
| 5 | String | 0x20 | UTF-8 bytes, null-terminated, with escape for embedded nulls |
| 6 | Object | 0x30 | Recursively encode key-value pairs |
| 7 | Array | 0x40 | Recursively encode elements |
| 8 | BinData | 0x50 | Subtype byte + length + raw bytes |
| 9 | ObjectId | 0x60 | 12 raw bytes (already sort by timestamp) |
| 10 | Boolean | 0x70 | 0x00 = false, 0x01 = true |
| 11 | Date | 0x80 | int64 milliseconds, sign-magnitude transformed |
| 12 | Timestamp | 0x85 | uint64, big-endian |
| 13 | RegExp | 0x90 | pattern + flags, null-terminated |
| 14 | MaxKey | 0xFF | Single byte |

### Compound Index Key Encoding

For compound indexes (e.g., `{ a: 1, b: -1 }`), keys are concatenated:

```
[field_a_encoded] [separator 0x01] [field_b_encoded_inverted]
```

For descending sort direction (-1), the encoded bytes are bitwise inverted (`XOR 0xFF`). This makes `memcmp` sort in reverse order for that field.

### Numeric Comparison

All numeric types (Int32, Int64, Double) must be comparable. The encoding converts all to a 64-bit IEEE 754 double representation with sign-magnitude transformation:

```
if positive: flip the sign bit (0x80 XOR)
if negative: flip all bits (0xFF XOR all bytes)
```

This produces byte-comparable representations where -Infinity < -1.5 < -1 < 0 < 1 < 1.5 < Infinity < NaN.

## Index Architecture

### Auto _id Index

Every collection has an implicit `_id` index. It is:
- Unique
- Cannot be dropped
- Used for point lookups by `_id`
- Backing B+ tree stores `_id` key → document BSON (or overflow pointer)

The `_id` index IS the primary data store for the collection. Documents are stored in `_id` order. Secondary indexes store their key → `_id` value, requiring a second lookup in the primary index to retrieve the full document.

### Secondary Indexes

Secondary indexes are separate B+ trees. Each entry maps:
```
[encoded_secondary_key | encoded_id] → (empty value, presence = match)
```

Including `_id` in the key makes entries unique even for non-unique indexes and enables efficient lookup of the primary document.

### Multikey Indexes

When a field being indexed contains an array, the multikey index creates one entry per array element. For document `{ tags: ["a", "b"] }` with index on `tags`:

```
["a", ObjectId("...")] → ()
["b", ObjectId("...")] → ()
```

The index metadata records that this is a multikey index. The query planner uses this information to correctly handle $elemMatch (must match a single element), $all (all values must match), and $size (count array elements).

### Compound Indexes

A compound index on `{ a: 1, b: -1 }` creates entries with concatenated encoded keys:

```
[encode(a_value) | 0x01 | encode_inverted(b_value) | encode(id)] → ()
```

Supports:
- Prefix queries: `{ a: "x" }` uses the index (prefix match)
- Exact match: `{ a: "x", b: 5 }` uses the index
- Sort: `{ a: 1, b: -1 }` matches index order exactly
- Does NOT support: `{ b: 5 }` alone (no leftmost prefix)

## Catalog Design

The catalog is a reserved B+ tree whose root page is stored in the file header (offset 32). It contains metadata for all collections and indexes.

### Catalog Entry Format

Each entry in the catalog B+ tree:

**Key**: `[type_byte | namespace_bytes]`
- Type 0x01: Collection entry. Key = `0x01 | collection_name`
- Type 0x02: Index entry. Key = `0x02 | collection_name | 0x00 | index_name`

**Value (Collection)**:
```bson
{
    "name": "users",
    "dataRootPage": 4072,        // Root page of _id index (primary data)
    "documentCount": 150000,
    "avgDocSize": 512,
    "createdAt": ISODate("..."),
    "options": {}                // Collection options (future: capped, validator)
}
```

**Value (Index)**:
```bson
{
    "name": "email_1",
    "collection": "users",
    "rootPage": 5100,            // Root page of this index's B+ tree
    "keyPattern": { "email": 1 },
    "unique": false,
    "sparse": false,
    "multikey": false,           // Set to true on first array value indexed
    "entryCount": 150000
}
```

### Catalog Operations

- **Create collection**: Insert collection entry + create `_id` index entry + allocate root pages.
- **Drop collection**: Remove collection entry + remove all index entries + free all data pages (B+ tree traversal to collect pages).
- **Create index**: Insert index entry + allocate root page + scan collection to build index.
- **Drop index**: Remove index entry + free all index pages.
- **List collections**: Scan catalog for type 0x01 entries.
- **List indexes**: Scan catalog for type 0x02 entries with matching collection name prefix.

## WAL Design

### WAL File Format (.mqlite-wal)

```
WAL Header (32 bytes):
  0    4    Magic: "MQWL" (0x4D51574C)
  4    4    Format version: uint32
  8    4    Page size internal: uint32 (must match main file)
  12   4    Page size leaf: uint32 (must match main file)
  16   4    Salt 1: uint32 (must match main file header)
  20   4    Salt 2: uint32 (must match main file header)
  24   4    Checkpoint sequence: uint32
  28   4    Header checksum: CRC32C

WAL Frames (repeated):
  0    4    Page number: uint32
  4    4    Database page count after this commit: uint32 (0 if not a commit frame)
  8    4    Salt 1: uint32
  12   4    Salt 2: uint32
  16   4    Frame checksum: CRC32C (covers header + page data)
  20   N    Page data (4KB or 32KB depending on page number range)
```

### WAL Operations

- **Write**: New/modified pages are appended to the WAL. A commit frame has a non-zero "database page count" field.
- **Read**: Readers check the WAL index (in SHM) first. If a page is in the WAL, use the most recent version. Otherwise, read from the main file.
- **Checkpoint**: Copy committed pages from WAL to main file. Reset WAL to empty. Update main file header.
- **Recovery**: On open, scan the WAL. Replay all committed frames (those with valid commit markers). Discard uncommitted frames at the end.

### Shared Memory File (.mqlite-shm)

The SHM file contains the WAL index — a hash table mapping page numbers to WAL frame offsets. This allows readers to quickly determine if a page is in the WAL without scanning the entire WAL file.

```
SHM Layout:
  0      4    Reader count: uint32
  4      4    Writer lock: uint32 (PID of writer, 0 = unlocked)
  8      24   Reader slots: [snapshot_id(4) | pid(4)] × 64 readers max
  200    ...  WAL index: hash table [page_number(4) → wal_offset(8)]
```

### Clean Close

On `Database::close()` or `Drop`:
1. Checkpoint: copy all WAL pages to main file.
2. Delete WAL file.
3. Delete SHM file.
4. Result: single .mqlite file (per Q6).

If the process crashes, the WAL and SHM files persist. On next open, mqlite detects them, replays the WAL, and resumes normal operation.

## Document Validation

### On Insert

1. **Well-formedness**: BSON must parse without errors (handled by `bson` crate).
2. **Size limit**: Document must not exceed 16MB (16,777,216 bytes) after serialization.
3. **Nesting depth**: Maximum 100 levels (prevent stack overflow during recursive operations).
4. **Field count**: Maximum 10,000 fields per document (prevent memory exhaustion).
5. **Field name**: Maximum 1,024 bytes. No null bytes in field names.
6. **_id field**: If absent, auto-generate an ObjectId. If present, validate uniqueness against the `_id` index. The `_id` value must be a valid BSON type that can be indexed.

### ObjectId Generation

MongoDB-compatible ObjectId format (12 bytes):
```
[timestamp(4)] [random(5)] [counter(3)]
```
- Timestamp: seconds since Unix epoch, big-endian.
- Random: 5 random bytes, generated once per process.
- Counter: 3-byte incrementing counter, initialized randomly.

This ensures ObjectIds are roughly time-ordered (important for the `_id` index locality) and globally unique with high probability.

## Buffer Pool

### Design

The buffer pool caches recently accessed pages in memory. It is a fixed-size pool of page frames, managed with a clock (CLOCK-sweep) eviction algorithm.

```
Buffer Pool:
  [Frame 0: page_number=5, pin_count=0, dirty=false, ref_bit=1]
  [Frame 1: page_number=102, pin_count=2, dirty=true, ref_bit=1]
  [Frame 2: (empty)]
  ...
```

### Operations

- **Pin(page_number)**: Look up page in hash table. If found, increment pin_count, set ref_bit. If not found, evict a victim (pin_count=0, ref_bit=0), read page from disk (or WAL), insert into hash table.
- **Unpin(page_number)**: Decrement pin_count. Caller indicates if page was dirtied.
- **Flush**: Write all dirty pages. Used during checkpoint and close.
- **Eviction**: Clock algorithm sweeps through frames. If ref_bit=1, clear it and advance. If ref_bit=0 and pin_count=0, evict (write if dirty). If all frames are pinned, allocation fails (error, not panic).

### Size Configuration

| Deployment | Buffer Pool Size | 4KB Frames | 32KB Frames |
|------------|-----------------|------------|-------------|
| IoT/Edge | 4 MB | ~500 | ~60 |
| Desktop/CLI | 64 MB (default) | ~8000 | ~1000 |
| Server | 256 MB | ~32000 | ~4000 |

The buffer pool partitions frames by page size: 4KB frames for internal nodes, 32KB frames for leaf/overflow pages. The ratio is configurable but defaults to 25% internal / 75% leaf.

## Constraints Identified

1. **BSON comparison ordering is non-negotiable and non-retrofittable.** The key encoding must be correct from day one. A bug in type ordering means every index is corrupt. Extensive test coverage of edge cases (NaN, -0, Decimal128, mixed numeric types) is mandatory.

2. **Variable page sizes require two free lists.** The page allocator must track 4KB and 32KB free pages separately. Free list corruption means space leaks or worse. The free list should be checksummed.

3. **16MB document limit matches MongoDB.** Documents up to 16MB can span ~500 overflow pages. The overflow chain must be transactional — a crash mid-write of a large document must not leave a partial chain.

4. **Multikey indexes add insertion complexity.** A single document insert on a multikey-indexed field requires N index insertions (N = array length). Updates that modify array fields require removing old entries and adding new ones. This is a significant hot-path cost.

5. **The catalog is a single point of failure.** Catalog corruption makes the database unreadable. Consider: dual-write the catalog to two locations in the file, with consistency check on open.

6. **WAL page images are full pages.** A WAL entry for a 32KB leaf page is 32KB+header. WAL files can grow quickly under write-heavy workloads. The checkpoint threshold must balance WAL size against checkpoint frequency.

7. **ObjectId counter must be thread-safe.** Multiple threads inserting concurrently will call ObjectId generation. The 3-byte counter needs an `AtomicU32` or similar.

8. **CRC32C is for corruption detection, not security.** An attacker can recompute CRC32C. For Phase 1 this is acceptable. Phase 2 encryption-at-rest may add HMAC.

## Open Questions

1. **Should the primary data store use a clustered index (documents stored in _id order) or a heap with separate _id index?** Clustered: range scans on _id are fast, but non-_id queries still need secondary index lookups. Heap: all indexes are equal, but _id lookups require an extra indirection. Recommendation: clustered (matches MongoDB's behavior and optimizes the common case).

2. **What is the free page reclamation strategy?** When documents are deleted, leaf pages may become empty. Options: (a) immediately return pages to free list, (b) batch reclamation during checkpoint, (c) manual VACUUM-style compaction. Recommendation: immediate free list return for simplicity, with VACUUM for file size reduction.

3. **Should overflow pages be the same size as leaf pages (32KB)?** Using 32KB overflow pages wastes space for documents slightly larger than 31KB. Using 4KB overflow pages means long chains for large documents. A hybrid (first overflow = 32KB, then 4KB pages for remainder) adds complexity for minimal gain. Recommendation: uniform 32KB overflow pages.

4. **How are concurrent index builds handled?** Creating an index on an existing collection requires scanning all documents. Should this block writes? MongoDB supports background index builds. For Phase 1, blocking index builds may be acceptable given the embedded use case.

5. **What happens when the file grows beyond available disk?** The page allocator extends the file when the free list is empty. If `ftruncate`/`fallocate` fails due to ENOSPC, the write must fail cleanly without corrupting existing data. The WAL must handle partial writes gracefully.

6. **Should compound index key encoding handle null/missing fields?** When a document lacks a field that's part of a compound index, MongoDB indexes it with a null key. mqlite must match this behavior for compatibility.

## Integration Points

### -> API Layer
- `Collection<T>` CRUD methods translate to B+ tree operations via the catalog
- `Database::open()` initializes the buffer pool, opens/creates the file, reads the catalog
- `Database::checkpoint()` triggers WAL-to-main-file copy
- `IndexModel` from the API maps to catalog index entries

### -> Query Engine
- The query planner reads index metadata from the catalog to select indexes
- Index scans use the B+ tree cursor (start key → end key range)
- Collection scans use leaf-to-leaf sibling pointers for sequential reads
- The key encoding determines whether a query can use an index (key prefix match)

### -> Security
- Page checksums (CRC32C) detect corruption from disk errors and partial writes
- BSON validation limits (depth, size, field count) are enforced at the data model boundary
- File permissions (0600) are set by the storage layer on file creation

### -> Scalability
- Buffer pool size directly affects read performance (cache hit rate)
- WAL checkpoint frequency affects write amplification and recovery time
- Variable page sizes affect memory utilization and I/O patterns
- Free list efficiency affects space amplification after deletes

### -> Wire Protocol
- BSON documents flow through unchanged between wire protocol and storage
- ObjectId generation is shared between native API and wire protocol insert handlers
- Index metadata exposed via listIndexes command reads the catalog directly
