# 07 — Candidate changes for our use (not started)

Ranked by importance for **PHP + unsharded Atlas replica set**. Nothing here is implemented yet.

## Must-have

1. **Configurable / modern wire version in the fake reply** (`mongo/ismaster.go:45,60`).
   - Expose min/max wire as flags (upstream #28), or mirror the real primary's `maxWireVersion` from
     `topology.Description()`.
   - Prerequisite: re-check that the proxy handles everything the client will then use. Claiming ≥ 9 enables new
     command fields, and ≥ 17 enables more, but these are just OP_MSG fields forwarded as-is. Keep `helloOk`,
     `topologyVersion` and `compression` absent.
   - Without this, the next libmongoc minor release (min wire 9) breaks every PHP connection.
2. **Intercept `hello`** in OP_QUERY and OP_MSG (`command.go:78-92`, `operations.go:210-215, 260-265`), and
   **reply in the same opcode** as the request (an OP_MSG reply for OP_MSG; `ismaster.go:21`). Add `isWritablePrimary`
   when answering `hello`. This makes `serverApi` and app-issued handshakes safe.
3. **Return MongoDB error replies instead of closing the socket** on proxy-side failures (selection timeout,
   pool cleared, server closed). Use a code the driver treats as retryable, e.g. 11602 or 91, plus the
   `RetryableWriteError` label (`proxy/connection.go:50-63, 161-186`; upstream #16/#22).
4. **Fix the stale transaction pin:** skip the cache when `IsStartTransaction` is set, and drop the pin on network or
   NotPrimary errors (`mongo/mongo.go:188-191, 217-225, 239-243`).
5. **Deadlines:** a per-request context timeout for selection/checkout (`mongo.go:73`), plus an idle/read deadline
   and a max frame size (for example `maxMessageSizeBytes`) in `proxy/connection.go:136-159`.

## Should-have

6. **Read preference support** for secondary offload: parse `$readPreference` (mode, tags, maxStalenessSeconds) from
   the OP_MSG and build a `ReadPrefSelector`; keep txn > cursor > readPref > primary. Force the primary for writes and
   for `$out`/`$merge`. Reference: upstream PR #92 and the jitsucom fork.
7. **Fix cursor cache keys** for db-level cursors (`$cmd.aggregate`, `$cmd.listCollections`, `$cmd.listIndexes.<coll>`)
   and remove entries on `killCursors` (`command.go`, `cursor_cache.go`).
8. **Graceful shutdown timeout** and second-SIGTERM handling (`mongobetween.go`, `proxy/proxy.go:132-137`). This is
   important with PHP persistent connections.
9. **Cluster tag on upstream metrics** (`config.go:84`).

## Nice-to-have / hygiene

10. Optional inbound auth or bind-address hardening (the jitsucom fork has `-proxy-auth`).
11. Fix the `localTime` BSON type, the OP_UPDATE/OP_DELETE ZERO field bugs and the incomplete `IsWrite` list.
12. Go driver upgrade. v2 changes internals and would break the `unsafe` `extractTopology`. Go ≥ 1.22, `Dockerfile` to
    match `go.mod`, non-root image, `/tmp` for `-dynamic`, drop or upgrade go-getter.
13. **Test infrastructure:** add a `--replSet` single-node or 3-node replica set to `docker-compose.yml` and CI, test the
    sharded persona, and add a PHP/libmongoc smoke test container.
