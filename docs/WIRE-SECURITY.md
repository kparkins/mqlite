# Wire Protocol Security Advisory

> **Applies to:** mqlite with the `wire` feature enabled.
>
> The `wire` feature exposes a TCP listener that implements a subset of the
> MongoDB wire protocol. This advisory describes the security posture of that
> listener and the steps required to use it safely.

## Summary

**mqlite has no authentication, no authorization, and no TLS.**

Anyone who can reach the TCP port has full read/write access to the database.

## Details

### No Authentication

The wire protocol server accepts all connections without requiring credentials.
There is no username/password, no API key, and no certificate-based identity.

This is intentional. mqlite targets local-only use cases (test
doubles, embedded tools, local development) and has no authentication layer.

### Localhost-Only Default

The recommended bind address is `127.0.0.1` (localhost):

```rust
use mqlite::{Client, WireProtocol};

let client = Client::open("myapp.mqlite")?;
let _server = WireProtocol::bind(&client, "127.0.0.1:27017")?;
# Ok::<(), mqlite::Error>(())
```

With this binding, only processes running on the same machine can connect.
Network-adjacent attackers cannot reach the port.

### Why `0.0.0.0` is Dangerous

Binding to `0.0.0.0` (all interfaces) makes the database reachable from any
network interface on the host — including external NICs, VPNs, Docker bridge
networks, etc.

**Do not bind to `0.0.0.0` unless:**
- The host is behind a firewall that blocks the port from all untrusted sources, AND
- You fully understand the exposure and accept the risk.

mqlite logs a warning when `0.0.0.0` is detected:

```
mqlite WARNING: wire protocol server bound to 0.0.0.0:27017 — accessible from
all network interfaces. mqlite has no authentication. Use 127.0.0.1 for
local-only access.
```

### No TLS

mqlite transmits all data — including document contents — in plaintext.

Do not use the wire feature to serve sensitive data over untrusted networks.
Even on a trusted LAN, plaintext transmission is inadvisable for sensitive
workloads.

TLS is not currently supported.

## Safe Deployment Guidelines

### Development / Local Testing (Supported)

```rust
// Safe: localhost only, ephemeral temp-file database
use tempfile::TempDir;
let tempdir = TempDir::new()?;
let client = Client::open(tempdir.path().join("db.mqlite"))?;
let _server = WireProtocol::bind(&client, "127.0.0.1:27017")?;
# Ok::<(), mqlite::Error>(())
```

This is the primary intended use case for mqlite.

### Production (Not Recommended)

If you must expose mqlite in production:

1. **Bind to localhost only** (`127.0.0.1`).
2. **Use a reverse proxy** (e.g., nginx with TLS + mTLS) in front of mqlite.
3. **Restrict access with OS firewall rules** (e.g., `iptables`, `nftables`).
4. **Run mqlite in a network namespace** (Docker/container network policies).
5. **Do not store sensitive data** that would cause harm if exfiltrated.

### CI / Integration Tests (Safe)

Using mqlite with the wire feature in CI is safe when:

- The CI environment does not expose ports to the internet.
- The test database contains only test fixtures (no real user data).

## Threat Model

| Threat | Mitigation |
|--------|------------|
| Local process reads database file | File permissions `0600` (owner only) |
| Local process connects to wire port | None — localhost binding only recommended |
| Network attacker connects to wire port | Firewall / localhost binding |
| Symlink attack on database file | `SymlinkRejected` error prevents open |
| Credential theft | N/A — no credentials exist |
| Data in transit (plaintext) | No mitigation — avoid untrusted networks |

## Reporting Security Issues

Please report security vulnerabilities by opening a GitHub Security Advisory
on the [mqlite repository](https://github.com/kyleparkinson/mqlite/security).

Do not disclose security issues in public GitHub issues.
