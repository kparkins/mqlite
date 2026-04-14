#!/usr/bin/env python3
"""
pymongo 4.x curated compatibility test suite for mqlite (hq-2lz).

Tests the pymongo 4.x driver against the mqlite wire protocol, validating:
  - MongoClient connect + ping + buildInfo
  - insert_one, find, update_one, delete_one
  - find_one_and_update (findAndModify)
  - create_index, list_indexes
  - Cursor iteration (getMore)
  - Error codes match MongoDB 8.0
  - directConnection=true required

## Prerequisites

1. pymongo 4.x installed:
       pip install 'pymongo>=4,<5'

2. mqlite wire server running:
       cargo run --features wire --example wire_server
   Or start with a custom port:
       MQLITE_PORT=27018 cargo run --features wire --example wire_server

## Usage

    # Default port 27017:
    python3 tests/pymongo_compat.py

    # Custom port:
    python3 tests/pymongo_compat.py --port 27018

    # Exit code 0 = all tests passed, non-zero = failures.

## CI integration

Start the wire server as a background process, run tests, then kill it:

    cargo run --features wire --example wire_server &
    SERVER_PID=$!
    sleep 1
    python3 tests/pymongo_compat.py
    TEST_RC=$?
    kill $SERVER_PID
    exit $TEST_RC
"""

import sys
import argparse
import time

# ---------------------------------------------------------------------------
# Minimal test runner
# ---------------------------------------------------------------------------

_passed = []
_failed = []


def test(name):
    """Decorator: run a test function, record pass/fail."""
    def decorator(fn):
        def wrapper(client):
            try:
                fn(client)
                _passed.append(name)
                print(f"  ✓ {name}")
            except Exception as exc:
                _failed.append((name, exc))
                print(f"  ✗ {name}: {exc}")
        wrapper._test_name = name
        return wrapper
    return decorator


def assert_eq(actual, expected, msg=""):
    if actual != expected:
        suffix = f": {msg}" if msg else ""
        raise AssertionError(f"expected {expected!r}, got {actual!r}{suffix}")


def assert_true(cond, msg=""):
    if not cond:
        raise AssertionError(msg or "condition is False")


def assert_in(key, mapping, msg=""):
    if key not in mapping:
        raise AssertionError(f"{key!r} not found in {list(mapping.keys())}{': ' + msg if msg else ''}")


def assert_not_in(key, mapping, msg=""):
    if key in mapping:
        raise AssertionError(f"{key!r} unexpectedly found in mapping{': ' + msg if msg else ''}")


# ---------------------------------------------------------------------------
# Test definitions
# ---------------------------------------------------------------------------

ALL_TESTS = []


def register(fn):
    ALL_TESTS.append(fn)
    return fn


# ── Connectivity ──────────────────────────────────────────────────────────────

@register
def test_ping(client):
    """MongoClient.admin.command('ping') returns ok=1."""
    result = client.admin.command("ping")
    assert_eq(result.get("ok"), 1.0, "ping ok")


@register
def test_build_info(client):
    """server_info() / buildInfo returns version string and ok=1."""
    result = client.admin.command("buildInfo")
    assert_eq(result.get("ok"), 1.0, "buildInfo ok")
    assert_true(isinstance(result.get("version"), str), "version is string")
    assert_true(len(result["version"]) > 0, "version non-empty")
    assert_true(isinstance(result.get("gitVersion"), str), "gitVersion is string")
    # modules must be an empty list.
    assert_eq(result.get("modules"), [], "modules must be empty list")


@register
def test_hello(client):
    """hello command returns topology info."""
    result = client.admin.command("hello")
    assert_eq(result.get("ok"), 1.0)
    assert_true(result.get("isWritablePrimary"), "isWritablePrimary")
    assert_eq(result.get("maxWireVersion"), 21, "maxWireVersion=21")
    assert_in("connectionId", result, "connectionId present")
    assert_in("topologyVersion", result, "topologyVersion present")
    # Standalone: no replica set fields.
    assert_not_in("setName", result, "no setName on standalone")
    assert_not_in("hosts", result, "no hosts on standalone")


@register
def test_server_status(client):
    """serverStatus returns required fields."""
    result = client.admin.command("serverStatus")
    assert_eq(result.get("ok"), 1.0)
    assert_true(result.get("uptime", -1) >= 0, "uptime >= 0")
    assert_in("connections", result)
    assert_in("storageEngine", result)


# ── CRUD ──────────────────────────────────────────────────────────────────────

@register
def test_insert_one_and_find(client):
    """insert_one / find round-trip."""
    db = client.local
    coll = db.get_collection("py_insert_find")
    coll.drop()

    result = coll.insert_one({"name": "Alice", "score": 100})
    assert_true(result.inserted_id is not None, "inserted_id is set")

    found = coll.find_one({"name": "Alice"})
    assert_true(found is not None, "document found")
    assert_eq(found["name"], "Alice")
    assert_eq(found["score"], 100)
    assert_eq(found["_id"], result.inserted_id)


@register
def test_insert_many(client):
    """insert_many inserts all documents."""
    db = client.local
    coll = db.get_collection("py_insert_many")
    coll.drop()

    docs = [{"i": i} for i in range(5)]
    result = coll.insert_many(docs)
    assert_eq(len(result.inserted_ids), 5, "5 inserted ids")
    assert_eq(coll.count_documents({}), 5, "5 docs in collection")


@register
def test_find_all(client):
    """find() returns all documents."""
    db = client.local
    coll = db.get_collection("py_find_all")
    coll.drop()

    coll.insert_many([{"n": i} for i in range(3)])
    docs = list(coll.find({}))
    assert_eq(len(docs), 3, "3 docs returned")


@register
def test_find_with_filter(client):
    """find() with filter returns matching docs only."""
    db = client.local
    coll = db.get_collection("py_find_filter")
    coll.drop()

    coll.insert_many([
        {"status": "active", "x": 1},
        {"status": "inactive", "x": 2},
        {"status": "active", "x": 3},
    ])
    docs = list(coll.find({"status": "active"}))
    assert_eq(len(docs), 2, "2 active docs")
    for doc in docs:
        assert_eq(doc["status"], "active")


@register
def test_update_one(client):
    """update_one modifies a single document."""
    db = client.local
    coll = db.get_collection("py_update_one")
    coll.drop()

    coll.insert_many([{"k": "a", "v": 0}, {"k": "a", "v": 0}])
    result = coll.update_one({"k": "a"}, {"$set": {"v": 99}})
    assert_eq(result.matched_count, 1, "matched_count=1")
    assert_eq(result.modified_count, 1, "modified_count=1")

    # Only one doc was modified.
    assert_eq(coll.count_documents({"v": 99}), 1)
    assert_eq(coll.count_documents({"v": 0}), 1)


@register
def test_update_many(client):
    """update_many modifies all matching documents."""
    db = client.local
    coll = db.get_collection("py_update_many")
    coll.drop()

    coll.insert_many([{"x": 1}, {"x": 1}, {"x": 2}])
    result = coll.update_many({"x": 1}, {"$set": {"x": 10}})
    assert_eq(result.matched_count, 2)
    assert_eq(result.modified_count, 2)
    assert_eq(coll.count_documents({"x": 10}), 2)


@register
def test_delete_one(client):
    """delete_one removes a single document."""
    db = client.local
    coll = db.get_collection("py_delete_one")
    coll.drop()

    coll.insert_many([{"t": "x"}, {"t": "x"}, {"t": "y"}])
    result = coll.delete_one({"t": "x"})
    assert_eq(result.deleted_count, 1, "deleted_count=1")
    assert_eq(coll.count_documents({"t": "x"}), 1, "one x remains")
    assert_eq(coll.count_documents({"t": "y"}), 1, "y untouched")


@register
def test_delete_many(client):
    """delete_many removes all matching documents."""
    db = client.local
    coll = db.get_collection("py_delete_many")
    coll.drop()

    coll.insert_many([{"t": "x"}, {"t": "x"}, {"t": "y"}])
    result = coll.delete_many({"t": "x"})
    assert_eq(result.deleted_count, 2)
    assert_eq(coll.count_documents({}), 1, "only y remains")


# ── findAndModify ─────────────────────────────────────────────────────────────

@register
def test_find_one_and_update_returns_pre_update(client):
    """find_one_and_update returns pre-update document by default."""
    db = client.local
    coll = db.get_collection("py_fam_pre")
    coll.drop()

    coll.insert_one({"name": "Alice", "score": 10})
    result = coll.find_one_and_update(
        {"name": "Alice"},
        {"$set": {"score": 99}},
    )
    assert_true(result is not None, "result not None")
    assert_eq(result["name"], "Alice")
    assert_eq(result["score"], 10, "pre-update doc returned")


@register
def test_find_one_and_update_returns_post_update(client):
    """find_one_and_update with return_document=AFTER returns post-update doc."""
    from pymongo import ReturnDocument

    db = client.local
    coll = db.get_collection("py_fam_post")
    coll.drop()

    coll.insert_one({"v": 1})
    result = coll.find_one_and_update(
        {"v": 1},
        {"$set": {"v": 2}},
        return_document=ReturnDocument.AFTER,
    )
    assert_true(result is not None)
    assert_eq(result["v"], 2, "post-update value")


@register
def test_find_one_and_update_no_match_returns_none(client):
    """find_one_and_update returns None when no document matches."""
    db = client.local
    coll = db.get_collection("py_fam_none")
    coll.drop()

    result = coll.find_one_and_update(
        {"nonexistent": True},
        {"$set": {"x": 1}},
    )
    assert_true(result is None, "no match → None")


@register
def test_find_one_and_delete(client):
    """find_one_and_delete removes and returns the document."""
    db = client.local
    coll = db.get_collection("py_fam_del")
    coll.drop()

    coll.insert_one({"tag": "delete_me", "val": 42})
    result = coll.find_one_and_delete({"tag": "delete_me"})
    assert_true(result is not None)
    assert_eq(result["val"], 42)
    assert_eq(coll.count_documents({}), 0, "document deleted")


# ── Indexes ───────────────────────────────────────────────────────────────────

@register
def test_create_index_list_indexes(client):
    """create_index + list_indexes round-trip."""
    db = client.local
    coll = db.get_collection("py_indexes")
    coll.drop()

    # Create a single-field index.
    idx_name = coll.create_index([("email", 1)])
    assert_true(isinstance(idx_name, str), "create_index returns index name")
    assert_true(len(idx_name) > 0, "index name non-empty")

    indexes = list(coll.list_indexes())
    # Must have at least: _id_ + email index.
    names = [idx["name"] for idx in indexes]
    assert_true("_id_" in names, "_id_ index present")
    assert_true(idx_name in names, f"created index {idx_name!r} present")


@register
def test_create_unique_index(client):
    """Unique index enforced on insertion."""
    import pymongo.errors

    db = client.local
    coll = db.get_collection("py_unique_idx")
    coll.drop()

    coll.create_index([("uid", 1)], unique=True)
    coll.insert_one({"uid": "abc"})

    try:
        coll.insert_one({"uid": "abc"})
        raise AssertionError("Expected DuplicateKeyError, got none")
    except pymongo.errors.DuplicateKeyError:
        pass  # Expected


@register
def test_drop_index(client):
    """drop_index removes a user-created index."""
    db = client.local
    coll = db.get_collection("py_drop_idx")
    coll.drop()

    idx_name = coll.create_index([("score", 1)])
    coll.drop_index(idx_name)

    indexes = list(coll.list_indexes())
    names = [idx["name"] for idx in indexes]
    assert_true(idx_name not in names, "dropped index absent")
    assert_true("_id_" in names, "_id_ still present")


# ── Cursor iteration ──────────────────────────────────────────────────────────

@register
def test_cursor_iteration_all_docs(client):
    """Cursor iteration returns all documents (multi-batch)."""
    db = client.local
    coll = db.get_collection("py_cursor_iter")
    coll.drop()

    n = 20
    coll.insert_many([{"i": i} for i in range(n)])

    # batch_size=5 forces multiple getMore round-trips.
    docs = list(coll.find({}, batch_size=5))
    assert_eq(len(docs), n, f"all {n} docs returned")

    # Verify all i values present.
    values = sorted(doc["i"] for doc in docs)
    assert_eq(values, list(range(n)), "all i values present")


@register
def test_cursor_count_documents(client):
    """count_documents returns correct count."""
    db = client.local
    coll = db.get_collection("py_count")
    coll.drop()

    coll.insert_many([{"x": i} for i in range(7)])
    assert_eq(coll.count_documents({}), 7)
    assert_eq(coll.count_documents({"x": {"$lt": 4}}), 4)


# ── Error codes ───────────────────────────────────────────────────────────────

@register
def test_unsupported_command_error_code(client):
    """Unsupported commands return CommandNotFound (code 59)."""
    import pymongo.errors

    try:
        client.admin.command("aggregate")
        raise AssertionError("Expected OperationFailure for unsupported command")
    except pymongo.errors.OperationFailure as exc:
        assert_eq(exc.code, 59, f"expected CommandNotFound=59, got {exc.code}")


@register
def test_duplicate_key_error_code(client):
    """Duplicate key violation returns error code 11000."""
    import pymongo.errors

    db = client.local
    coll = db.get_collection("py_dupkey")
    coll.drop()

    coll.create_index([("uid", 1)], unique=True)
    coll.insert_one({"uid": "dup"})

    try:
        coll.insert_one({"uid": "dup"})
        raise AssertionError("Expected DuplicateKeyError")
    except pymongo.errors.DuplicateKeyError as exc:
        assert_eq(exc.code, 11000, f"expected DuplicateKey=11000, got {exc.code}")


@register
def test_cursor_not_found_error_code(client):
    """Accessing expired/invalid cursor returns CursorNotFound (code 43)."""
    import pymongo.errors

    db = client.local
    coll = db.get_collection("py_cursor_nf")
    coll.drop()

    # We can't easily produce an invalid cursor ID via pymongo,
    # but we can verify via the raw command that code 43 is returned.
    try:
        db.command(
            "getMore",
            collection="py_cursor_nf",
            getMore=99999999,  # invalid cursor ID
        )
        raise AssertionError("Expected OperationFailure for invalid cursor")
    except pymongo.errors.OperationFailure as exc:
        assert_eq(exc.code, 43, f"expected CursorNotFound=43, got {exc.code}")


# ── Collections ───────────────────────────────────────────────────────────────

@register
def test_list_collections(client):
    """list_collection_names returns created collections."""
    db = client.local

    # Use unique names to avoid state from other tests.
    suffix = str(int(time.time()))
    coll_a = db.get_collection(f"py_lc_alpha_{suffix}")
    coll_b = db.get_collection(f"py_lc_beta_{suffix}")
    coll_a.insert_one({"x": 1})
    coll_b.insert_one({"y": 2})

    names = db.list_collection_names()
    assert_true(f"py_lc_alpha_{suffix}" in names, "alpha in list")
    assert_true(f"py_lc_beta_{suffix}" in names, "beta in list")


@register
def test_drop_collection(client):
    """drop() removes the collection from list_collection_names."""
    db = client.local
    suffix = str(int(time.time()))
    coll = db.get_collection(f"py_drop_{suffix}")
    coll.insert_one({"x": 1})

    assert_true(f"py_drop_{suffix}" in db.list_collection_names(), "collection exists before drop")
    coll.drop()
    assert_true(f"py_drop_{suffix}" not in db.list_collection_names(), "collection absent after drop")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_all(port: int) -> int:
    """Run all tests against mqlite on the given port. Returns exit code."""
    try:
        import pymongo
    except ImportError:
        print("ERROR: pymongo not installed. Run: pip install 'pymongo>=4,<5'")
        return 1

    uri = f"mongodb://127.0.0.1:{port}/?directConnection=true"
    print(f"\n{'=' * 65}")
    print(f"mqlite pymongo compatibility tests (hq-2lz)")
    print(f"pymongo version: {pymongo.version}")
    print(f"server: {uri}")
    print(f"{'=' * 65}\n")

    try:
        client = pymongo.MongoClient(
            uri,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
            socketTimeoutMS=10000,
        )
        # Force connection.
        client.admin.command("ping")
        print(f"Connected to mqlite on port {port}\n")
    except pymongo.errors.ServerSelectionTimeoutError as exc:
        print(f"ERROR: Cannot connect to mqlite on port {port}")
        print(f"  Start the server: cargo run --features wire --example wire_server")
        print(f"  Detail: {exc}")
        return 1

    print(f"Running {len(ALL_TESTS)} tests:\n")
    for fn in ALL_TESTS:
        name = fn.__name__
        try:
            fn(client)
            _passed.append(name)
            print(f"  ✓ {name}")
        except Exception as exc:
            _failed.append((name, exc))
            print(f"  ✗ {name}: {exc}")

    client.close()

    print(f"\n{'=' * 65}")
    print(f"Results: {len(_passed)} passed, {len(_failed)} failed")
    if _failed:
        print("\nFailed tests:")
        for name, exc in _failed:
            print(f"  ✗ {name}: {exc}")
    else:
        print("All tests passed ✓")
    print(f"{'=' * 65}\n")

    return 0 if not _failed else 1


def main():
    parser = argparse.ArgumentParser(
        description="pymongo 4.x compatibility test suite for mqlite"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=27017,
        help="mqlite wire server port (default: 27017)",
    )
    args = parser.parse_args()
    sys.exit(run_all(args.port))


if __name__ == "__main__":
    main()
