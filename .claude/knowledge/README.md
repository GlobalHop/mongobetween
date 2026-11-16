# mongobetween knowledge base

Collected 2026-11-16 by reading the full source (master `ea5a87e`, same as upstream HEAD), the
mongo-go-driver v1.14.0 internals it uses, the libmongoc source, and the upstream GitHub issues and PRs.
Goal: understand how the proxy works before adapting it to our setup. Our setup is a PHP app
(ext-mongodb / libmongoc) in front of an **unsharded Atlas replica set**.

No code was changed while collecting this.

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

## TL;DR

- mongobetween is a **mongos impersonator**. It answers the app's `isMaster` locally with
  `msg: "isdbgrid"` and forwards every other wire message **byte-for-byte** through a Go driver
  connection pool. It checks out one upstream connection per message.
- The app driver therefore never sees the real cluster hosts. It treats the proxy as a single
  always-available mongos. All SDAM, failover and TLS/auth to Atlas happen inside the Go driver.
- **The proxy has three hard assumptions:**
  1. Reads/writes may go to any server (true for mongos). Selection is hard-coded to `Primary`, and on a
     replica set that means *everything goes to the primary*. Client `$readPreference` is ignored.
  2. The client handshake uses legacy `isMaster`. `hello` is not intercepted.
  3. MongoDB is 4.2. `maxWireVersion: 8` is hard-coded.
- **It can work on an unsharded replica set** if the PHP URI is plain
  `mongodb://<proxy>:<port>` (or a unix socket) with no `replicaSet`, no `tls`, no credentials, no
  `serverApi`, no `loadBalanced` and no `directConnection`. The upstream URI holds the Atlas `mongodb+srv` +
  credentials, and its topology must stay replica-set (not direct).
- **Biggest future blocker:** libmongoc is about to raise its minimum wire version to 9 (4.4). The proxy reports
  max 8, so every PHP connection will then be refused. The hard-coded handshake reply must change.
