# 07 — Change backlog (status after the `feature/rs-connection-pool` work)

Originally ranked by importance for **PHP + unsharded Atlas replica set**. Status below reflects the commits in
`c9f11cd..HEAD` on `feature/rs-connection-pool`. The original descriptions are kept as pre-change notes.

## Must-have

1. **Configurable / modern wire version in the fake reply** (was `mongo/ismaster.go:45,60`). **Done.** The reply
   mirrors the upstream `maxWireVersion`; `-max-wire-version` overrides it (0 = mirror, capped at 25 for driver
   v1.17.10). `helloOk`, `topologyVersion`, `compression`, `setName`, `hosts` and `speculativeAuthenticate` stay
   absent. `2473379`, `3e3ea57` (driver upgrade).
2. **Intercept `hello`** and reply in the request's opcode. **Done.** `2473379`.
3. **Error replies instead of closing the socket.** **Done.** Code 11602 plus `RetryableWriteError` /
   `TransientTransactionError` / `UnknownTransactionCommitResult` labels; code 13 for disabled writes;
   `-checkout-timeout` (10s). `eb35d7b`.
4. **Stale transaction pin.** **Done.** `c37b237`.
5. **Deadlines and frame limits.** **Partial.** Checkout deadline (`eb35d7b`) and frame size limits
   (16 B .. 48 MB + 16 KiB) with exhaust stripped (`b22b29d`) are done. Frames are read incrementally, so memory
   tracks bytes received, and a TLS client on a plain port is closed at once (`c88e3e1`). With inbound auth on,
   unauthenticated connections are limited to 64 KiB frames and an idle deadline (`-auth-timeout`, 30s). An idle
   deadline on authenticated client sockets is **not** implemented. `-max-connections` (0 = unlimited) caps
   concurrently open client connections per listener.

## Should-have

6. **Read preference support.** **Done.** Mode, tags and `maxStalenessSeconds` are honoured; default primary.
   Analytics nodes are reachable via tags. `7272f94`, `f5b41b9`. (A `$readPreference` on a write command is
   routed by it, like a direct replica-set connection; this is a known limitation.)
7. **Cursor cache keys and `killCursors` eviction.** **Done.** Pins keyed by full namespace. `6f0b37d`,
   `930de3a`, `61e4e29`.
8. **Graceful shutdown timeout and second signal.** **Done.** `-shutdown-timeout` (30s), second signal kills.
   `d3ccb8b`. The kill path (timeout or second signal) also aborts in-flight upstream round trips
   (`Mongo.CancelInFlight`), `Mongo.Close` cancels before taking its lock, and a second signal exits with status 1
   after a 2 s grace period.
9. **Cluster tag on upstream metrics.** **Done.** `b883655`.

## Nice-to-have / hygiene

10. **Inbound auth / hardening.** **Done** beyond the original ask: optional listener TLS and mTLS
    (`-tls-cert-file`, `-tls-key-file`, `-tls-ca-file`; `9e52db1`), SCRAM-SHA-256 proxy-only users via
    `-auth-users-file` (`3218ca2`, hardening `0126dbf`, raw 28-byte salt fix `9c23f88`), and
    `-require-upstream-tls` (`d3ccb8b`).
11. **`localTime` type, OP_UPDATE/OP_DELETE ZERO bugs, `IsWrite` completeness.** **Not done** (out of scope).
12. **Go driver upgrade and hygiene.** **Partial.** Upgraded to v1.17.10, `go 1.22`, distroless image
    (`3e3ea57`), golangci-lint v2 (`f527cfa`), and the dynamic-config poller is stoppable and race-free
    (`3576989`, `8dc05f8`). **Not done:** driver v2 migration and replacing go-getter.
13. **Test infrastructure.** **Done.** Docker harness with a 3-member replica set and standalones
    (`e17c732`, `c31441b`), replica-set integration tests for failover, read preference and cursors
    (`b76d1c5`, `cd3f860`), PHP ext-mongodb smoke test (`54f78de`). The sharded persona is still untested.

## Still open

- Go driver v2 migration.
- `speculativeAuthenticate` support (one fewer round trip per new client connection).
- Idle/read deadline on authenticated client sockets.
- `IsWrite` completeness for `DisableWrites`, legacy OP_UPDATE/OP_DELETE bugs, go-getter replacement.
- Exhaust cursors and `OP_COMPRESSED` (deliberately unsupported).
- Inbound auth mechanisms other than SCRAM-SHA-256; per-user privileges.

See [../../docs/atlas-poc.md](../../docs/atlas-poc.md) for the runbook.
