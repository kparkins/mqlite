#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
JEPSEN_DIR="$SCRIPT_DIR"
ADAPTER_BIN="$REPO_ROOT/target/debug/mqlite_jepsen_adapter"

usage() {
    cat <<'USAGE'
Usage: tests/jepsen/run.sh [jepsen-options]

Runs the mqlite Jepsen suite after building the local embedded API adapter.

Examples:
  tests/jepsen/run.sh
  tests/jepsen/run.sh --workload register --time-limit 20 --rate 40
  tests/jepsen/run.sh --workload set --nemesis restart --concurrency 8
  tests/jepsen/run.sh --workload unique-index --time-limit 20
  tests/jepsen/run.sh --workload secondary-index --rate 50
  tests/jepsen/run.sh --workload index-build --time-limit 20
  tests/jepsen/run.sh --workload drop-index --time-limit 20
  tests/jepsen/run.sh --workload find-and-modify-claim --concurrency 16
  tests/jepsen/run.sh --workload write-batch-prefix --nemesis restart

Requirements:
  - cargo
  - Java 21+
  - clojure CLI or lein
USAGE
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    usage
    exit 0
fi

if ! command -v cargo >/dev/null 2>&1; then
    echo "ERROR: cargo is required to build mqlite_jepsen_adapter" >&2
    exit 2
fi

if ! command -v java >/dev/null 2>&1 || ! java -version >/dev/null 2>&1; then
    if command -v brew >/dev/null 2>&1; then
        OPENJDK_PREFIX="$(brew --prefix openjdk 2>/dev/null || true)"
        if [[ -n "$OPENJDK_PREFIX" && -x "$OPENJDK_PREFIX/bin/java" ]]; then
            export PATH="$OPENJDK_PREFIX/bin:$PATH"
            export JAVA_HOME="$OPENJDK_PREFIX/libexec/openjdk.jdk/Contents/Home"
        fi
    fi
fi

if ! command -v java >/dev/null 2>&1 || ! java -version >/dev/null 2>&1; then
    echo "ERROR: Java 21+ is required to run Jepsen" >&2
    echo "Install a JDK, then rerun tests/jepsen/run.sh" >&2
    exit 2
fi

if command -v clojure >/dev/null 2>&1; then
    RUNNER=(clojure -M:test)
elif command -v lein >/dev/null 2>&1; then
    RUNNER=(lein run)
else
    echo "ERROR: clojure CLI or lein is required to run Jepsen" >&2
    exit 2
fi

echo "Building mqlite Jepsen adapter..."
(cd "$REPO_ROOT" && cargo build --bin mqlite_jepsen_adapter)

mkdir -p "$REPO_ROOT/target/jepsen"

echo "Running Jepsen suite..."
(
    cd "$JEPSEN_DIR"
    "${RUNNER[@]}" \
        --repo-root "$REPO_ROOT" \
        --binary "$ADAPTER_BIN" \
        "$@"
)
