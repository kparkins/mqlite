#!/usr/bin/env bash
# CI-runnable wire protocol integration test for mqlite (hq-krdx).
#
# Builds and starts the mqlite wire server on a free port, installs pymongo,
# runs the full pymongo compatibility test suite, then tears down the server.
#
# Requirements:
#   - Rust toolchain with cargo (for building the wire server)
#   - Python 3 + pip (for pymongo)
#
# Usage:
#   ./tests/run_wire_ci.sh
#
# Environment variables:
#   MQLITE_PORT           Override port (default: auto-selected free port)
#   MQLITE_START_TIMEOUT  Server startup timeout in seconds (default: 30)
#   MQLITE_TEST_TIMEOUT   Test suite timeout in seconds (default: 120)
#
# Exit codes:
#   0 = all tests passed
#   1 = one or more tests failed
#   2 = setup error (build failure or server startup timeout)

set -euo pipefail

MQLITE_START_TIMEOUT="${MQLITE_START_TIMEOUT:-30}"
MQLITE_TEST_TIMEOUT="${MQLITE_TEST_TIMEOUT:-120}"
SERVER_PID=""
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ── Cleanup on exit (teardown on any failure) ────────────────────────────────

cleanup() {
    local exit_code=$?
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        echo ""
        echo "Stopping wire server (PID $SERVER_PID)..."
        kill "$SERVER_PID" 2>/dev/null || true
        # Wait up to 5 seconds for graceful shutdown, then force-kill.
        local waited=0
        while kill -0 "$SERVER_PID" 2>/dev/null && [[ $waited -lt 10 ]]; do
            sleep 0.5
            waited=$((waited + 1))
        done
        kill -9 "$SERVER_PID" 2>/dev/null || true
        echo "Wire server stopped."
    fi
    exit "$exit_code"
}

trap cleanup EXIT INT TERM

# ── Find a free port ─────────────────────────────────────────────────────────

if [[ -z "${MQLITE_PORT:-}" ]]; then
    MQLITE_PORT=$(python3 - <<'PYEOF'
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(('127.0.0.1', 0))
port = s.getsockname()[1]
s.close()
print(port)
PYEOF
)
fi

echo "================================================================"
echo "mqlite wire protocol CI integration test (hq-krdx)"
echo "Port: $MQLITE_PORT"
echo "Server startup timeout: ${MQLITE_START_TIMEOUT}s"
echo "Test suite timeout: ${MQLITE_TEST_TIMEOUT}s"
echo "================================================================"
echo ""

# ── pip install pymongo ──────────────────────────────────────────────────────

echo "Step 1: Installing pymongo 4.x..."
pip install 'pymongo>=4,<5' --quiet
PYMONGO_VERSION=$(python3 -c "import pymongo; print(pymongo.version)" 2>/dev/null || echo "unknown")
echo "pymongo ${PYMONGO_VERSION} installed ✓"
echo ""

# ── Build wire server ────────────────────────────────────────────────────────

echo "Step 2: Building mqlite wire server..."
cd "$REPO_ROOT"
if ! cargo build --features wire --example wire_server 2>&1; then
    echo "ERROR: cargo build failed"
    exit 2
fi
echo "Build complete ✓"
echo ""

# ── Start wire server ────────────────────────────────────────────────────────

echo "Step 3: Starting wire server on port ${MQLITE_PORT}..."
MQLITE_PORT="$MQLITE_PORT" ./target/debug/examples/wire_server 2>&1 &
SERVER_PID=$!
echo "Server PID: $SERVER_PID"

# Verify the process launched.
sleep 0.2
if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "ERROR: Wire server process exited immediately"
    exit 2
fi

# Poll until the server accepts TCP connections (up to MQLITE_START_TIMEOUT).
echo "Waiting for server to accept connections..."
elapsed=0
ready=0
while [[ $elapsed -lt $MQLITE_START_TIMEOUT ]]; do
    if python3 - <<PYEOF 2>/dev/null
import socket, sys
try:
    s = socket.create_connection(('127.0.0.1', $MQLITE_PORT), timeout=1)
    s.close()
    sys.exit(0)
except Exception:
    sys.exit(1)
PYEOF
    then
        ready=1
        break
    fi

    # Check server hasn't crashed.
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "ERROR: Wire server process exited before accepting connections"
        exit 2
    fi

    sleep 0.5
    elapsed=$((elapsed + 1))
done

if [[ $ready -ne 1 ]]; then
    echo "ERROR: Wire server did not become ready within ${MQLITE_START_TIMEOUT}s"
    exit 2
fi

echo "Server ready ✓"
echo ""

# ── Run pymongo test suite ───────────────────────────────────────────────────

echo "Step 4: Running pymongo compatibility test suite..."
echo ""

# Use `timeout` (Linux/CI) or `gtimeout` (macOS with GNU coreutils).
# If neither is available, run without a timeout limit — CI environments
# always have `timeout`; the job-level timeout-minutes: 10 provides a hard cap.
if command -v timeout &>/dev/null; then
    TIMEOUT_CMD="timeout $MQLITE_TEST_TIMEOUT"
elif command -v gtimeout &>/dev/null; then
    TIMEOUT_CMD="gtimeout $MQLITE_TEST_TIMEOUT"
else
    TIMEOUT_CMD=""
fi

if $TIMEOUT_CMD python3 "$SCRIPT_DIR/pymongo_compat.py" --port "$MQLITE_PORT"; then
    echo ""
    echo "================================================================"
    echo "Wire protocol integration tests: PASSED ✓"
    echo "================================================================"
    exit 0
else
    TEST_RC=$?
    echo ""
    echo "================================================================"
    echo "Wire protocol integration tests: FAILED (exit code ${TEST_RC})"
    echo "================================================================"
    exit "$TEST_RC"
fi
