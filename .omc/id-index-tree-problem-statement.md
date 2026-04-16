# Dead `_id_` index tree — problem statement

**Written:** 2026-04-15. Deferred during the WAL-atomicity work (too much format-change risk to bundle in).

## What it is

On `create_collection` (`src/storage/catalog.rs:381-412`) the catalog allocates **two** B+ tree root pages per collection:

1. `data_root_page` — the primary store keyed by `encode_key(_id)` → BSON doc bytes. Every mutator reads/writes this tree.
2. `id_root_page` — a second tree, registered in the catalog as an `IndexEntry { name: "_id_", root_page: id_root_page, key_pattern: { "_id": 1 }, unique: true, ... }`. Its root leaf is initialised via `BTree::create_at` in `paged_engine.rs` (both `create_namespace` at line ~1767 and `create_index` at line ~1613) so the page has a valid header.

The `_id_` tree is modelled on MongoDB's synthetic `_id_` index: every collection reports it via `listIndexes`. The wire layer (`src/wire/server.rs:1944`, `:1735`) fabricates the `_id_` entry from thin air in responses to `listIndexes` — it never reads the `_id_` B+ tree.

## Why it's dead

Nothing ever inserts into it, reads from it, or hands it to the planner:

- `maintain_secondary_on_insert/update/delete` in `src/storage/secondary_index.rs` iterate `catalog.list_indexes(ns)` but the **primary** data tree is already keyed by the encoded `_id`, so the `_id_` entry would duplicate the primary and is (implicitly) skipped by callers — in practice no code path opens it.
- `select_plan` (`src/query/planner/select_plan`) considers the user-created secondary indexes in the catalog, not the `_id_` entry. Point-lookups by `_id` already hit the primary tree directly.
- `drop_namespace` (`paged_engine.rs:1827-1830`) walks `list_indexes` and calls `free_all_pages` on each — including `_id_`. Since the `_id_` tree is always a lone, empty leaf, this free does nothing harmful but wastes one page alloc per collection.

Net effect: every collection consumes one permanently-empty 32 KB leaf page + one catalog `IndexEntry` row. Purely wasted storage and a live source of confusion ("why does listIndexes show `_id_` but nothing maintains it?").

## Why it's risky to remove now

The `_id_` entry is baked into the on-disk catalog. Removing it naively breaks format compatibility with every existing `.mqlite` file:

1. **Catalog schema change.** `create_collection` currently returns `(data_root_page, id_root_page)` and writes the `_id_` `IndexEntry`. Callers and tests (`catalog.rs:812`, `:821`, `:924`) assume the row exists. Tests at `catalog.rs:826-829` hard-assert `get_index("users", "_id_")` returns `Some`. Dropping the row means migrating every existing file and rewriting those assertions.

2. **Page leak on old files.** Existing files have the `_id_` root page allocated. If we stop registering it, `drop_namespace` won't know to free it — permanent leak until a compaction tool exists.

3. **Wire-layer desync.** `src/wire/server.rs` manufactures the `_id_` index in `listIndexes` / index-count output. Those stay — MongoDB drivers expect it — but they currently also compute `numIndexesBefore`/`numIndexesAfter` based on the catalog's index count (`+1` for synthetic `_id_`, e.g. `:1783`, `:1824`). If the catalog stops storing `_id_`, those `+1` offsets flip. Off-by-one bugs in compat tests are near-certain.

4. **No migration path yet.** mqlite doesn't have a file-format version bump / migration story. Whatever fix we pick for `_id_` needs to ride on top of that mechanism once it exists.

## What the fix probably looks like

Path A — **Remove the backing tree, keep the synthetic wire-layer entry.**
- Stop calling `alloc_leaf` for `id_root_page` in `Catalog::create_collection`; return only `data_root_page`.
- Stop inserting the `_id_` `IndexEntry` in the catalog. `listIndexes` in the wire layer already synthesises it.
- `list_indexes` at storage-engine level either (a) keeps not including `_id_` (current behaviour) or (b) explicitly synthesises it for callers that care. Tests at `catalog.rs:826-829`, `:924-939` update.
- `paged_engine.rs` drops the `id_root` initialisation at `:1613`, `:1767`.

Path B — **Actually use the `_id_` tree.**
- Not worth it. Primary data tree already keyed by `_id`. Having a second `_id` index adds write amp with zero read benefit.

Recommend Path A.

## Prerequisites before we can do Path A

1. **File-format version field** in `FileHeader` so `Catalog::open` can branch: new-format files skip the `_id_` `IndexEntry`, old-format files still show it (or trigger a one-shot migration that frees the leaf + removes the row).
2. **Compaction/migration routine** that walks existing files and drops the `_id_` leaf + catalog row. Can be lazy (on first `drop_namespace` for that collection) or eager (bulk at open).
3. **Test inventory.** The greps above turn up ~12 test sites that assert on `_id_`. Classify each: wire-layer (keep, synthetic) vs. storage-layer (update or remove).

## Sizing

Not small. Cross-cuts: `catalog.rs` (schema), `paged_engine.rs` (create/drop), `wire/server.rs` (index counting), `storage/header.rs` (version field), plus a migration pass. Budget a whole session; land behind a format-version bump; coordinate with whoever holds existing `.mqlite` files.

## Why it's deferred

- Not causing correctness bugs today — just a wasted leaf per collection.
- Touching the on-disk catalog schema after the WAL atomicity rewrite adds unnecessary coupling between the two changes. Separate session with its own regression tests.
- Needs the file-format version story landed first. That's a design decision of its own.

## Pointers

- Catalog entry creation: `src/storage/catalog.rs:359-412`.
- B+ tree init during collection create: `src/storage/paged_engine.rs` `create_namespace` (~1755-1770) and `create_index` (~1601-1615).
- Wire-layer synthesis: `src/wire/server.rs:1735`, `:1944`, `:1783`, `:1824`.
- Tests that assume `_id_` is in the catalog: `src/storage/catalog.rs:741-749`, `:812-829`, `:924-945`.
