#!/usr/bin/env bash
# mongosh 2.x smoke test for mqlite (hq-2lz).
#
# Validates that mongosh can connect to the mqlite wire protocol server and
# perform basic CRUD operations matching the MongoDB 8.0 shell experience.
#
# Prerequisites:
#   - mongosh 2.x installed and on PATH
#   - mqlite wire server running (or this script starts one)
#
# Usage:
#   # Start server automatically:
#   ./tests/mongosh_smoke.sh
#
#   # Use an already-running server on a custom port:
#   MQLITE_PORT=27018 MQLITE_NO_START=1 ./tests/mongosh_smoke.sh
#
# Exit codes:
#   0 = all smoke tests passed
#   1 = one or more tests failed
#   2 = prerequisites not met (mongosh not found)

set -euo pipefail

MQLITE_PORT="${MQLITE_PORT:-27017}"
MQLITE_NO_START="${MQLITE_NO_START:-0}"
SERVER_PID=""

# ── Prerequisite check ───────────────────────────────────────────────────────

if ! command -v mongosh &>/dev/null; then
    echo "SKIP: mongosh not found on PATH."
    echo "  Install mongosh 2.x: https://www.mongodb.com/try/download/shell"
    exit 2
fi

MONGOSH_VERSION=$(mongosh --version 2>&1 | head -1)
echo "mongosh: $MONGOSH_VERSION"

# ── Start server if needed ───────────────────────────────────────────────────

if [[ "$MQLITE_NO_START" != "1" ]]; then
    echo "Starting mqlite wire server on port $MQLITE_PORT..."
    MQLITE_PORT="$MQLITE_PORT" cargo run --features wire --example wire_server \
        2>/dev/null &
    SERVER_PID=$!

    # Wait up to 5 seconds for the server to be ready.
    for i in $(seq 1 10); do
        if mongosh --port "$MQLITE_PORT" --directConnection \
                   --quiet --eval "db.adminCommand('ping')" \
                   2>/dev/null | grep -q '"ok"'; then
            break
        fi
        sleep 0.5
    done
fi

URI="mongodb://127.0.0.1:${MQLITE_PORT}/?directConnection=true"

# ── Helper: run a mongosh snippet and check for expected output ──────────────

PASS_COUNT=0
FAIL_COUNT=0

run_test() {
    local name="$1"
    local script="$2"
    local expected="$3"

    local output
    output=$(mongosh "$URI" --quiet --eval "$script" 2>&1)

    if echo "$output" | grep -qF "$expected"; then
        echo "  ✓ $name"
        ((PASS_COUNT++))
    else
        echo "  ✗ $name"
        echo "    Expected to find: $expected"
        echo "    Got: $output"
        ((FAIL_COUNT++))
    fi
}

run_test_no_error() {
    local name="$1"
    local script="$2"

    local output
    if output=$(mongosh "$URI" --quiet --eval "$script" 2>&1); then
        echo "  ✓ $name"
        ((PASS_COUNT++))
    else
        echo "  ✗ $name (error)"
        echo "    Output: $output"
        ((FAIL_COUNT++))
    fi
}

# ── Smoke tests ──────────────────────────────────────────────────────────────

echo ""
echo "================================================================"
echo "mqlite mongosh 2.x smoke test"
echo "URI: $URI"
echo "================================================================"
echo ""

# 1. show dbs (listDatabases)
run_test "show dbs (listDatabases)" \
    "db.adminCommand({listDatabases:1})" \
    '"ok" : 1'

# 2. show collections (listCollections on empty db)
run_test "show collections (listCollections empty)" \
    "db.getSiblingDB('smoke_test').runCommand({listCollections:1})" \
    '"ok" : 1'

# 3. insertOne
run_test "db.coll.insertOne()" \
    "db.getSiblingDB('smoke_test').smoke_coll.insertOne({name:'Alice',score:100})" \
    '"acknowledged" : true'

# 4. find — returns results
run_test "db.coll.find({}) returns docs" \
    "db.getSiblingDB('smoke_test').smoke_coll.find({}).toArray()" \
    'Alice'

# 5. updateOne
run_test "db.coll.updateOne()" \
    "db.getSiblingDB('smoke_test').smoke_coll.updateOne({name:'Alice'},{\$set:{score:999}})" \
    '"modifiedCount" : 1'

# 6. deleteOne
run_test "db.coll.deleteOne()" \
    "db.getSiblingDB('smoke_test').smoke_coll.deleteOne({name:'Alice'})" \
    '"deletedCount" : 1'

# 7. show collections after insert
run_test "show collections after insert" \
    "db.getSiblingDB('smoke_test').runCommand({listCollections:1})" \
    '"name" : "smoke_coll"'

# 8. ping
run_test "ping" \
    "db.adminCommand({ping:1})" \
    '"ok" : 1'

# 9. buildInfo
run_test "buildInfo" \
    "db.adminCommand({buildInfo:1})" \
    '"version"'

# 10. Unsupported command → CommandNotFound (code 59)
run_test "aggregate → CommandNotFound" \
    'try { db.getSiblingDB("smoke_test").smoke_coll.aggregate([]).toArray(); print("NO_ERROR") } catch(e) { print("code:" + e.code) }' \
    'code:59'

# ── Cleanup ───────────────────────────────────────────────────────────────────

if [[ -n "$SERVER_PID" ]]; then
    kill "$SERVER_PID" 2>/dev/null || true
fi

# ── Summary ───────────────────────────────────────────────────────────────────

echo ""
echo "================================================================"
echo "Results: $PASS_COUNT passed, $FAIL_COUNT failed"
if [[ $FAIL_COUNT -eq 0 ]]; then
    echo "All smoke tests passed ✓"
else
    echo "FAILED: $FAIL_COUNT test(s) failed"
fi
echo "================================================================"
echo ""

[[ $FAIL_COUNT -eq 0 ]]
