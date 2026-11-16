# mongobetween knowledge base

Collected 2026-11-16 by reading the full source (master `ea5a87e`, same as upstream HEAD), the
mongo-go-driver v1.14.0 internals it uses, the libmongoc source, and the upstream GitHub issues and PRs.
Goal: understand how the proxy works before adapting it to our setup. Our setup is a PHP app
(ext-mongodb / libmongoc) in front of an **unsharded Atlas replica set**.

No code was changed while collecting this. **Update:** the changes listed in `07-change-backlog.md` have since
been implemented on `feature/rs-connection-pool`. Documents 01-06 describe the code **as it was before those
changes** (master `ea5a87e`) unless marked otherwise; `05` and `07` have been updated, and the runbook is in
`docs/atlas-poc.md`.

## Files

| File | Content |
|---|---|
| [01-architecture.md](01-architecture.md) | Components, request lifecycle, concurrency model |
| [02-wire-protocol.md](02-wire-protocol.md) | Opcode decoding, handshake interception, fake `isMaster` reply, command classification |
| [03-routing-and-pinning.md](03-routing-and-pinning.md) | Server selection, cursor/transaction pinning, error processing, failover |
| [04-proxy-config-ops.md](04-proxy-config-ops.md) | CLI/URI config, auth model, connection loop, dynamic config, metrics, build/test/CI |
| [05-sharded-vs-unsharded.md](05-sharded-vs-unsharded.md) | **Key doc.** What makes each topology work, PHP client rules, the failure modes we likely hit |
| [06-upstream-history.md](06-upstream-history.md) | Relevant upstream issues/PRs, design history, active forks, alternatives |
| [07-change-backlog.md](07-change-backlog.md) | Candidate modifications, ranked, with the code locations to touch |
| [08-open-follow-ups.md](08-open-follow-ups.md) | Deferred minor findings and parked items after the branch work |

## TL;DR

Current state (after `feature/rs-connection-pool`):

- mongobetween is a **mongos impersonator**. It answers the app's `hello`/`isMaster` locally in the request's
  opcode with `msg: "isdbgrid"`, mirroring the upstream `maxWireVersion` (override with `-max-wire-version`), and
  forwards every other wire message **byte-for-byte** through a Go driver connection pool, one checkout per
  message.
- The app driver never sees the real cluster hosts. SDAM, failover and TLS/auth to Atlas happen inside the Go
  driver (v1.17.10).
- Client `$readPreference` (mode, tags, maxStalenessSeconds) is honoured; the default is primary.
- Proxy-side failures become MongoDB error replies (11602 with retryable labels) instead of closed sockets.
- Optional inbound TLS/mTLS and SCRAM-SHA-256 proxy-only users (`-auth-users-file`); `-require-upstream-tls`.
- It works on an unsharded replica set with a plain `mongodb://<proxy>:<port>` PHP URI (no `mongodb+srv`,
  `replicaSet`, `loadBalanced`). See `05` for the URI rules and `docs/atlas-poc.md` for the runbook.
- Limits: inbound SCRAM-SHA-256 only, all proxy users share the Atlas user's privileges, no exhaust cursors, no
  OP_COMPRESSED, no `speculativeAuthenticate`, Go driver v1.

Pre-change TL;DR (historical, master `ea5a87e`): selection was hard-coded to `Primary` and `$readPreference` was
ignored; `hello` was not intercepted; `maxWireVersion: 8` was hard-coded, which would have broken PHP once
libmongoc raised its minimum wire version to 9.
