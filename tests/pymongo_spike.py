#!/usr/bin/env python3
"""
pymongo connectivity spike for mqlite (hq-23u).

Validates that pymongo 4.x can connect to the mqlite minimal wire protocol
stub and that the handshake / ping round-trip completes without driver-side
capability errors, unexpected replica-set discovery, or session errors.

Also captures which OP_MSG section kinds pymongo uses for these commands
(Kind 0 only, or also Kind 1) by patching the socket at the lowest level.

Usage:
    # Start the mqlite wire protocol server first:
    cargo run --features wire --example wire_server 2>/dev/null &
    SERVER_PID=$!

    # Then run this script:
    python3 tests/pymongo_spike.py

    kill $SERVER_PID
"""

import socket
import struct
import threading
import time
import sys
from contextlib import contextmanager


# ---------------------------------------------------------------------------
# Minimal OP_MSG framing helpers (pure Python, no pymongo)
# ---------------------------------------------------------------------------

OP_MSG = 2013


def build_op_msg(request_id: int, body_bson: bytes) -> bytes:
    """Build a minimal OP_MSG with a single Kind-0 section."""
    # header (16) + flagBits (4) + kind byte (1) + bson
    total = 16 + 4 + 1 + len(body_bson)
    header = struct.pack("<iiii", total, request_id, 0, OP_MSG)
    flag_bits = struct.pack("<I", 0)
    kind = b"\x00"
    return header + flag_bits + kind + body_bson


def read_op_msg(sock: socket.socket) -> tuple[int, int, int, int, bytes]:
    """
    Read one OP_MSG from the socket.

    Returns (message_length, request_id, response_to, flag_bits, payload)
    where payload is everything after flagBits (sections + optional checksum).
    """
    header = _recvall(sock, 16)
    msg_len, req_id, resp_to, opcode = struct.unpack("<iiii", header)
    assert opcode == OP_MSG, f"Expected OP_MSG (2013), got {opcode}"
    body = _recvall(sock, msg_len - 16)
    flag_bits = struct.unpack("<I", body[:4])[0]
    payload = body[4:]  # sections [+ checksum]
    return msg_len, req_id, resp_to, flag_bits, payload


def _recvall(sock: socket.socket, n: int) -> bytes:
    """Receive exactly n bytes from sock."""
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError(f"Connection closed after {len(buf)}/{n} bytes")
        buf += chunk
    return buf


def analyse_sections(payload: bytes) -> list[dict]:
    """
    Parse OP_MSG sections and return metadata about each.

    Returns a list of dicts with keys:
      - kind: 0 or 1
      - length: bytes consumed (including kind byte)
      - identifier (Kind-1 only)
      - doc_count (Kind-1 only)
    """
    sections = []
    pos = 0
    while pos < len(payload):
        kind = payload[pos]
        pos += 1
        if kind == 0:
            # Kind-0: single BSON document
            doc_size = struct.unpack_from("<i", payload, pos)[0]
            sections.append({"kind": 0, "length": 1 + doc_size})
            pos += doc_size
        elif kind == 1:
            # Kind-1: int32 size + cstring identifier + BSON docs
            size = struct.unpack_from("<i", payload, pos)[0]
            section_data = payload[pos : pos + size]
            null_pos = section_data.index(b"\x00", 4)  # skip the 4-byte size field
            identifier = section_data[4:null_pos].decode("utf-8")
            docs_data = section_data[null_pos + 1 :]
            doc_count = 0
            off = 0
            while off < len(docs_data):
                ds = struct.unpack_from("<i", docs_data, off)[0]
                doc_count += 1
                off += ds
            sections.append(
                {
                    "kind": 1,
                    "length": 1 + size,
                    "identifier": identifier,
                    "doc_count": doc_count,
                }
            )
            pos += size
        else:
            # Unknown kind — stop (might be checksum or garbage)
            break
    return sections


# ---------------------------------------------------------------------------
# Capture proxy: intercepts pymongo's raw bytes
# ---------------------------------------------------------------------------

class CapturingProxy:
    """
    A loopback TCP proxy that:
    1. Accepts a connection from pymongo (client_port)
    2. Forwards traffic to/from the real mqlite server (server_port)
    3. Records all OP_MSG messages in both directions

    This lets us observe exactly what pymongo sends without modifying pymongo.
    """

    def __init__(self, server_host: str, server_port: int):
        self.server_host = server_host
        self.server_port = server_port
        self.captured_requests: list[dict] = []
        self.captured_responses: list[dict] = []
        self._lock = threading.Lock()

        # Bind a proxy listener.
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen(5)
        self._listener.settimeout(5.0)
        self.proxy_port = self._listener.getsockname()[1]
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self):
        # Accept multiple connections (pymongo 4.x creates >=2: one for
        # topology monitoring and one for actual commands).
        while True:
            try:
                client_sock, _ = self._listener.accept()
            except (socket.timeout, OSError):
                return
            server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                server_sock.connect((self.server_host, self.server_port))
            except OSError:
                client_sock.close()
                continue

            # Forward in both directions, capturing traffic.
            threading.Thread(
                target=self._forward,
                args=(client_sock, server_sock, self.captured_requests),
                daemon=True,
            ).start()
            threading.Thread(
                target=self._forward,
                args=(server_sock, client_sock, self.captured_responses),
                daemon=True,
            ).start()

    def _forward(self, src: socket.socket, dst: socket.socket, log: list):
        # No timeout — block until natural EOF or error.  A timeout here would
        # drop idle-but-live connections (e.g., mqlite waiting for the next cmd).
        src.settimeout(None)
        try:
            while True:
                header = _recvall(src, 16)
                if not header:
                    break
                msg_len, req_id, resp_to, opcode = struct.unpack("<iiii", header)
                rest = _recvall(src, msg_len - 16)
                full = header + rest
                # Log section metadata if this is an OP_MSG.
                if opcode == OP_MSG:
                    flag_bits = struct.unpack("<I", rest[:4])[0]
                    payload = rest[4:]
                    checksum_present = bool(flag_bits & 1)
                    analysis_payload = payload[:-4] if checksum_present else payload
                    sections = analyse_sections(analysis_payload)
                    with self._lock:
                        log.append(
                            {
                                "request_id": req_id,
                                "response_to": resp_to,
                                "flag_bits": flag_bits,
                                "checksum_present": checksum_present,
                                "sections": sections,
                            }
                        )
                dst.sendall(full)
        except (ConnectionError, OSError, struct.error):
            pass

    def stop(self):
        self._listener.close()


# ---------------------------------------------------------------------------
# Main spike
# ---------------------------------------------------------------------------


def run_spike(server_port: int = 27017):
    import pymongo
    print(f"\n{'=' * 60}")
    print(f"mqlite pymongo connectivity spike (hq-23u)")
    print(f"pymongo version: {pymongo.version}")
    print(f"{'=' * 60}\n")

    # Start a capturing proxy between pymongo and mqlite.
    proxy = CapturingProxy("127.0.0.1", server_port)
    proxy_uri = f"mongodb://127.0.0.1:{proxy.proxy_port}/?directConnection=true"

    findings = {}

    try:
        print(f"Connecting via proxy on :{proxy.proxy_port} → mqlite :{server_port}")
        client = pymongo.MongoClient(
            proxy_uri,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
            socketTimeoutMS=5000,
        )

        # --- Test 1: ping ---
        print("\n[1] admin.command('ping') ...")
        result = client.admin.command("ping")
        assert result.get("ok") == 1, f"ping returned {result}"
        print(f"    ✓ ping → {result}")
        findings["ping_ok"] = True

        # --- Test 2: hello ---
        print("\n[2] admin.command('hello') ...")
        result = client.admin.command("hello")
        assert result.get("ok") == 1, f"hello returned {result}"
        print(f"    ✓ hello → ok=1")
        print(f"      isWritablePrimary = {result.get('isWritablePrimary')}")
        print(f"      maxWireVersion    = {result.get('maxWireVersion')}")
        print(f"      mqlite.version    = {result.get('mqlite', {}).get('version', 'n/a')}")
        findings["hello_ok"] = True
        findings["hello_response"] = {
            "isWritablePrimary": result.get("isWritablePrimary"),
            "maxWireVersion": result.get("maxWireVersion"),
        }

        # --- Test 3: buildInfo ---
        print("\n[3] admin.command('buildInfo') ...")
        result = client.admin.command("buildInfo")
        assert result.get("ok") == 1, f"buildInfo returned {result}"
        print(f"    ✓ buildInfo → version={result.get('version')}")
        findings["buildInfo_ok"] = True

        # --- Test 4: unknown command → CommandNotFound ---
        print("\n[4] admin.command('aggregate') [should fail with CommandNotFound] ...")
        try:
            client.admin.command("aggregate")
            findings["unknown_cmd_raises"] = False
            print("    ✗ Expected OperationFailure, but no error raised")
        except pymongo.errors.OperationFailure as e:
            if e.code == 59:
                print(f"    ✓ Got CommandNotFound (code 59): {e.details.get('errmsg', '')}")
                findings["unknown_cmd_raises"] = True
            else:
                print(f"    ? Got error code {e.code}: {e}")
                findings["unknown_cmd_raises"] = f"unexpected code {e.code}"

        client.close()

        # --- Wait a moment for proxy to finish capturing ---
        time.sleep(0.3)
        proxy.stop()

        # --- Analyse section kinds ---
        print("\n" + "=" * 60)
        print("Section Kind Analysis (OP_MSG sections from pymongo → mqlite)")
        print("=" * 60)
        kind1_seen = False
        for i, req in enumerate(proxy.captured_requests):
            kinds = [s["kind"] for s in req["sections"]]
            has_kind1 = 1 in kinds
            if has_kind1:
                kind1_seen = True
            checksum = "CRC32C" if req["checksum_present"] else "none"
            print(
                f"  Request {i + 1}: sections={kinds}"
                f"  checksum={checksum}"
                + (" ← KIND-1!" if has_kind1 else "")
            )
            for s in req["sections"]:
                if s["kind"] == 1:
                    print(
                        f"    Kind-1: identifier='{s['identifier']}' "
                        f"doc_count={s['doc_count']}"
                    )

        if not proxy.captured_requests:
            print("  (no requests captured — proxy may have missed traffic)")

        findings["kind1_observed"] = kind1_seen
        findings["total_requests_captured"] = len(proxy.captured_requests)
        findings["checksums_seen"] = any(
            r["checksum_present"] for r in proxy.captured_requests
        )

        print("\n" + "=" * 60)
        print("SPIKE FINDINGS SUMMARY")
        print("=" * 60)
        print(f"  ping:        {'✓' if findings.get('ping_ok') else '✗'}")
        print(f"  hello:       {'✓' if findings.get('hello_ok') else '✗'}")
        print(f"  buildInfo:   {'✓' if findings.get('buildInfo_ok') else '✗'}")
        print(f"  CommandNotFound (code 59): {'✓' if findings.get('unknown_cmd_raises') is True else '✗/partial'}")
        print(f"  Requests captured: {findings.get('total_requests_captured', 0)}")
        print(f"  Kind-1 sections:   {'YES — implementation needed' if kind1_seen else 'NO — Kind-0 only for these commands'}")
        print(f"  CRC32C checksums:  {'YES' if findings.get('checksums_seen') else 'NO'}")
        print()

        # Return findings dict for programmatic use in tests.
        return findings

    except pymongo.errors.ServerSelectionTimeoutError as e:
        proxy.stop()
        print(f"\n✗ Could not connect to mqlite server on port {server_port}")
        print(f"  Start the server first: cargo run --features wire --example wire_server")
        print(f"  Error: {e}")
        sys.exit(1)
    except Exception as e:
        proxy.stop()
        print(f"\n✗ Unexpected error: {type(e).__name__}: {e}")
        raise


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="mqlite pymongo spike test")
    parser.add_argument("--port", type=int, default=27017, help="mqlite server port")
    args = parser.parse_args()

    findings = run_spike(server_port=args.port)

    # Exit non-zero if any critical test failed.
    critical = ["ping_ok", "hello_ok", "buildInfo_ok"]
    failed = [k for k in critical if not findings.get(k)]
    if failed:
        print(f"FAILED: {failed}")
        sys.exit(1)
    print("SPIKE PASSED ✓")
