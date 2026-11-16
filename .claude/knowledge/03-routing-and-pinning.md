# 03 — Routing, pinning, error handling (`mongo/mongo.go` + caches)

## Client construction

- `mongo.Connect(ctx, opts)` with options from the upstream URI (`config/config.go:217-254`). SRV, TLS, auth,
  `replicaSet`, pool size, `serverSelectionTimeoutMS`, `socketTimeoutMS` and `waitQueueTimeoutMS` all apply to the
  **upstream** side.
- `-ping` runs `Ping(readpref.Primary())` with a 60s timeout and fatals on failure. Without it, the proxy listens
  immediately and the first requests wait in server selection.
- After connect, the topology is pulled out with reflect+unsafe (`mongo.go:42-47`). The high-level API is unused
  from then on.
- The driver polls SRV records only for Unknown/Sharded kinds. For a replica set, membership comes from hello. That is correct.

## Server selection (`selectServer`, `mongo.go:233-261`)

Order:
1. **Transaction cache**, keyed by lsid. Note: this is checked even when `startTransaction` is true.
2. **Cursor cache**, keyed by `"<cursorID>-<collection>"`.
3. `CompositeSelector{ReadPrefSelector(readpref.Primary()), LatencySelector(15ms)}` on `roundTripCtx`.

Behaviour of `ReadPrefSelector(Primary)` per topology kind (driver `server_selector.go:230-245`):
- **Sharded** → any mongos. The read preference is ignored, and the mongos honours the client's `$readPreference`
  in the command. *This is what the code was written for.*
- **ReplicaSetWithPrimary** → the primary only.
- **ReplicaSetNoPrimary / Unknown** → nothing qualifies, so it blocks until a primary appears or
  `serverSelectionTimeoutMS` (default 30s) passes.

The client's `$readPreference` is never parsed, and neither is any readPreference in the proxy URI.
**On a replica set, every uncached operation goes to the primary.**

## Connection checkout and round trip

- `server.Connection(roundTripCtx)` (`mongo.go:275`). `roundTripCtx` has **no deadline**, so it waits in the pool
  queue unless `waitQueueTimeoutMS`/`timeoutMS` is set upstream (verify). It returns `ErrServerClosed` if the
  server was removed from the topology, and `PoolClearedError` while the pool is paused.
- Write the client bytes, then read **one** reply. A network error is wrapped with the `NetworkError` label.
- **No retries in the proxy.** Any RoundTrip error propagates up, and `proxy/connection.go` **closes the client
  socket** (no MongoDB error doc). The client sees a network error and retries once if retryable reads/writes apply.

## Error processing (`processError`, `mongo.go:318-345`)

Delegates to the driver's `Server.ProcessError` for network errors and for server error replies (the reply is
still forwarded to the client):
- NotPrimary codes (10107, 13435, 10058) and NodeIsRecovering codes (11600, 11602, 13436, 189, 91): mark the server
  Unknown and request an immediate check. The pool is cleared only for shutdown codes (11600, 91).
- Non-timeout network error: mark Unknown, clear the pool, cancel the monitor check.
- The result: after a stepdown, the next selection waits for the new primary, and the client's own retry lands
  there. **Failover works for retryable operations.**

## Cursor pinning (`cursor_cache.go`)

- Key `"<cursorID>-<collection>"` → `driver.Server`. LRU 1M, TTL 24h. `Peek` doesn't refresh the TTL, `Add` does.
  A sweeper runs every second.
- It is added when a reply has a non-zero `cursor.id`. It is removed when a getMore reply returns id 0.
- **Leaks / misses:**
  - `killCursors` never removes entries.
  - Key mismatch for db-level cursors. `aggregate: 1` and `listCollections` are stored with collection `""`, but
    their getMore uses `$cmd.aggregate` / `$cmd.listCollections`. `listIndexes` getMore uses `$cmd.listIndexes.<coll>`.
    A miss falls back to the primary, which is harmless on a replica set.
- After failover, a pinned getMore goes to the old primary (now a secondary), which is the node that has the cursor.
  The result may be 13435 or CursorNotFound. That is unavoidable.

## Transaction pinning (`transaction_cache.go`)

- base64(lsid) → Server. LRU 1M, TTL 120s.
- **Set** on any decoded reply to a `startTransaction:true` statement, even an error reply.
- **Cleared** on any decoded commit/abort reply, *but not on a network error* (early return at `mongo.go:188-191`).
- **Stale-pin bug:** selection checks the cache before looking at `IsStartTransaction`. Drivers pool sessions and
  `withTransaction` retries with the same lsid. So if a commit/abort network-failed during a failover, new
  transactions on that lsid go to the old primary for up to 120s. Atlas maintenance triggers elections routinely,
  so this is a real risk on a replica set.

## Cursor pinning history (#45 → #70 → #73)

- **#45** (`a4bce58`) added server pinning for cursors and transactions (the current design).
- **#70** (`db55bae`) pinned to **connections** for TCP load balancers in front of mongos. Connections stayed
  checked out until the cursor was exhausted. That caused pool exhaustion, concurrent sharing of pinned connections,
  and skipped SDAM error processing.
- **#73** (`4fb1f15`) reverted it. Upstream's stated reason: "mongobetween will start to choke when a bunch of huge,
  slow queries are being thrown at it".
- If our earlier test ran between roughly June and August 2024, we may have hit this regression, and not a
  replica-set issue.
