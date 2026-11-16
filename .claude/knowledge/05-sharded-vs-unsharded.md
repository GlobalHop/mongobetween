# 05 — Sharded vs unsharded: what is essential

## Mental model

mongobetween always tells the app "I am a mongos". Whether that lie holds depends on what sits upstream:

| Upstream (Go driver topology kind) | Persona shown to the app | Who routes reads/writes |
|---|---|---|
| Sharded (mongos list) | mongos (`isdbgrid`) | mongobetween picks any mongos; **mongos** honours `$readPreference` |
| ReplicaSetWithPrimary / NoPrimary (e.g. Atlas `mongodb+srv` replica set) | mongos (`isdbgrid`) | mongobetween picks **the primary, always** |
| Single (`directConnection=true` / standalone) | standalone (no `msg`) | that one node; the app disables transactions and retryable writes |
| LoadBalanced | mongos (`isdbgrid`) | untested; no `serviceId`, cursor pinning issues (#69/#70) |

## Parts essential for both topologies

1. **Handshake interception** (`command.go:78-92`, `ismaster.go`). It must catch every handshake and heartbeat,
   or the app sees the real topology and either rejects it or bypasses the proxy.
2. **The fake reply must contain no `helloOk`, no `topologyVersion` and no `compression`.** That keeps clients on
   legacy OP_QUERY isMaster, polling monitoring and uncompressed frames, which are the only things the proxy can handle.
3. **`logicalSessionTimeoutMinutes`** in the reply enables sessions, retryable writes and transactions on the client.
4. **Byte-for-byte forwarding plus one checkout per message.** This is the multiplexing core.
5. **Cursor and transaction pinning by server.** This is essential for sharded clusters with several mongos
   (cursors live on one mongos). For a replica set it only matters across a failover.
6. **SDAM error feeding** (`processError`), so failovers are detected quickly.

## Additional essentials for an unsharded replica set

1. **The upstream URI must yield a replica-set topology.** Use `mongodb+srv://…` or `replicaSet=` and never
   `directConnection=true`. Otherwise the persona becomes standalone and transactions stop working.
2. **Read preference is ignored. Everything runs on the primary.** This is correct but gives no secondary offload,
   and analytics-node tags are ignored. To add offload, parse `$readPreference` from OP_MSG and build a selector (see 07).
3. **Elections:** during a primary election, selection blocks for up to the upstream `serverSelectionTimeoutMS`.
   On timeout the proxy closes the client socket, and PHP retries once. Tune the upstream
   `serverSelectionTimeoutMS` against the PHP-FPM request timeout.
4. **Transaction stale-pin bug** (see 03). It matters on replica sets because Atlas maintenance causes elections.
5. **A single pool to the primary** (`maxPoolSize`, default 100). All load funnels into it with no checkout
   deadline, so slow queries cause head-of-line blocking. Size `maxPoolSize`/`minPoolSize`/`waitQueueTimeoutMS`
   deliberately. Atlas load ≈ pods × pool size.

## PHP / libmongoc client rules (required with the current code)

App URI to the proxy: `mongodb://127.0.0.1:<port>/?<only harmless options>` or `mongodb://%2Fpath%2Fto.sock`.

| Option in app URI / driver options | Effect via the proxy | Rule |
|---|---|---|
| `mongodb+srv://` | Needs SRV DNS for the proxy; implies TLS | **Don't** |
| `tls=true` / `ssl=true` | Listener has no TLS; the connection hangs | **Don't** |
| user:password, `authSource`, `authMechanism` | SASL split across pooled connections → "Authentication failed" | **Don't**. Credentials go on the proxy's upstream side |
| `replicaSet=…` | libmongoc removes a MONGOS from a replica-set topology → "No suitable servers found" | **Don't** |
| `serverApi` (Stable API; often in Atlas snippets) | Driver sends OP_MSG `hello`, which is forwarded → real replica-set reply or error 186 "client metadata … may only be sent in the first hello" | **Don't** |
| `loadBalanced=true` | Same `hello` path; also needs `serviceId` | **Don't** |
| `directConnection=true` (app side) | OK-ish, but pointless | Avoid |
| `readPreference=secondary*` | Silently served by the primary | Works, no effect |
| `compressors=` | Never negotiated | No effect |
| exhaust cursors | Corrupts pooled upstream connections | **Don't** |
| App code `executeCommand('admin', ['isMaster'=>1])` | OP_MSG request gets an OP_REPLY reply | Use `ping` |
| Features needing wire > 8 (delete `hint`, snapshot reads, `Client::bulkWrite`, QE, `let`) | Refused client-side | Avoid until `maxWireVersion` is raised |

## Most probable causes of our earlier failure (ranked)

1. The app URI was copied from Atlas (`mongodb+srv`, TLS, credentials, `replicaSet`, or `serverApi`). Each of these
   alone breaks the connection. This matches upstream #30, the only Atlas replica-set report, which was never resolved.
2. The cursor-to-connection pinning regression (#70, live roughly June–August 2024, reverted in #73).
3. Failover behaviour: socket closed instead of an error reply, plus selection blocking during elections (#16).
4. Expectations around reading from secondaries (all reads go to the primary).

## Verify in a proof of concept (not verifiable from code alone)

- Whether libmongoc accepts an OP_REPLY to an OP_MSG isMaster.
- Exact ext-mongodb ↔ libmongoc version and its `WIRE_VERSION_MIN` for our PHP image.
- Go driver v1.14 against an Atlas 8.0 server (officially supports up to 7.0).
- getMore after a failover: 13435 vs success on the old primary.
- Pool checkout blocking with no `waitQueueTimeoutMS`.
- Behaviour under PHP-FPM persistent clients with many workers.
