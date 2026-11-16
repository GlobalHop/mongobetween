# 01 — Architecture

## Size and layout

About 4.4k lines of Go. Module `github.com/coinbase/mongobetween`, `go 1.18`, `go.mongodb.org/mongo-driver v1.14.0`.

| Path | Role |
|---|---|
| `mongobetween.go` | Entrypoint: parse config, start one `Proxy` per listen address, handle signals |
| `config/config.go` | Flags, `address=uri` pairs, Go driver client options, statsd, SDAM/pool monitors |
| `proxy/proxy.go` | Listener (TCP or unix socket), accept loop, graceful shutdown |
| `proxy/connection.go` | Per-client read loop: read frame → decode → intercept or forward → write reply |
| `proxy/dynamic.go` | Polls a file/URL every 10s for `DisableWrites` / `RedirectTo` rules |
| `mongo/mongo.go` | Wraps the Go driver: `Connect`, `RoundTrip`, `selectServer`, `processError` |
| `mongo/operations.go` | Wire-protocol decoder (OP_MSG, OP_QUERY, OP_REPLY, legacy ops) and metadata extraction |
| `mongo/command.go` | Command classification (`CommandAndCollection`, `IsWrite`, `IsIsMasterDoc`) |
| `mongo/ismaster.go` | The synthetic, hard-coded `isMaster` reply |
| `mongo/cursor_cache.go` | `cursorID-collection` → `driver.Server` (LRU 1M, TTL 24h) |
| `mongo/transaction_cache.go` | base64(lsid) → `driver.Server` (LRU 1M, TTL 120s) |
| `mongo/topology_monitor.go` | Logs topology changes every 5s |
| `lruttl/` | Concurrent LRU + TTL cache used by the two caches |
| `util/statsd.go` | Tagged statsd client, open-connections gauge |

## Request lifecycle

```
PHP (libmongoc) ──TCP/unix──▶ proxy/connection.go handleMessage
   1. readWireMessage: 4-byte length, then body (no max size, no deadline)
   2. mongo.Decode → Operation (metadata only; original bytes kept in msg.Wm)
   3. roundTrip:
        dynamic DisableWrites? → close client conn (no error reply)
        dynamic RedirectTo?    → use the other listener's mongo client
        IsIsMaster?            → mongo.IsMasterResponse(requestID, topology.Kind)  [answered locally]
        else                   → mongo.RoundTrip(msg)
   4. write reply bytes back (skipped for moreToCome / w:0)

mongo.RoundTrip
   selectServer: txn cache (lsid) → cursor cache (cursorID-coll) → SelectServer(Primary + 15ms latency)
   checkoutConnection(server)          ← Go driver pool (already TLS + authenticated to Atlas)
   conn.WriteWireMessage(msg.Wm)       ← client bytes unchanged
   conn.ReadWireMessage()              ← exactly ONE reply frame
   Decode reply → processError (SDAM) → update cursor/txn caches
   return reply bytes unchanged
```

## Key design facts

- **Byte-for-byte forwarding.** Requests and replies are never re-encoded. Request ID and `responseTo` stay
  intact, so the client matches replies normally. `$readPreference`, `lsid`, `txnNumber`, `$clusterTime`
  and read/write concern all pass through untouched.
- **No connection affinity.** Each message checks out a possibly different upstream connection and returns it
  right after. Anything that needs per-connection state breaks: SASL conversations, exhaust streams, and the
  `client` metadata handshake.
- **The topology is read via `unsafe`.** `extractTopology` (`mongo/mongo.go:42-47`) uses reflect + unsafe to
  pull the unexported `Client.deployment` out of the high-level driver client. After that only internal driver
  packages are used (`topology`, `description`, `driver`). This will break on a driver v2 upgrade.
- **Concurrency.** One goroutine per client connection, plus a watcher goroutine. Each client connection is
  strictly serial (request → reply). Upstream concurrency is bounded by the Go driver pool (`maxPoolSize`,
  default 100 per server).
- **SDAM lives only in the Go driver.** One monitor per Atlas member per proxy process. That is a large part of the
  benefit: N app processes no longer each run their own monitoring connections.
