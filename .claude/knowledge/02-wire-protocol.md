# 02 — Wire protocol layer (`mongo/operations.go`, `mongo/command.go`, `mongo/ismaster.go`)

## Opcodes

`Decode()` (`operations.go:39-76`) extracts metadata only. Forwarding uses the original bytes.

| Opcode | Handling | Notes |
|---|---|---|
| OP_MSG (2013) | Full section parse (kind 0 + kind 1), checksum read but not verified | The only opcode that matters for modern drivers |
| OP_QUERY (2004) | Parsed | `CommandAndCollection` **always returns `Find`** regardless of command (225-227). isMaster only if ns == `admin.$cmd` |
| OP_REPLY (1) | Parsed | Upstream replies to legacy requests; also used for the fake isMaster reply |
| OP_GET_MORE / OP_KILL_CURSORS / OP_INSERT / OP_UPDATE / OP_DELETE | Parsed | Legacy; removed in MongoDB 5.1+/6.0. Bugs: OP_UPDATE decode skips the ZERO field, OP_DELETE encode omits it, legacy writes not marked unacknowledged (the proxy would hang waiting for a reply). Irrelevant for our drivers |
| OP_COMPRESSED (2012) and anything else | `opUnknown`, forwarded raw | No decompression. Compression is never negotiated, so clients won't send it |

Metadata extracted from OP_MSG:
- `CursorID`: `getMore` value in a request, `cursor.id` in a reply (`operations.go:253-258, 405-412`)
- `CommandAndCollection`: first non-Unknown section (433-441)
- `TransactionDetails` (448-478): needs `lsid.id` (binary) **and** `txnNumber` (int64) **and** `autocommit` (bool).
  Retryable writes have no `autocommit`, so they are correctly not treated as transactions.
- `Unacknowledged`: request flag `moreToCome` (w:0)
- `Error()`: `driver.ExtractErrorFromServerResponse` on section 0

**Not extracted:** `$readPreference`, `readConcern`, `$clusterTime`, the `exhaustAllowed` flag, and `moreToCome` on *replies*.

## Handshake interception

`IsIsMasterDoc` (`command.go:78-92`) matches only top-level `isMaster` / `ismaster` with an int32 > 0 or bool true.
- **`hello` is NOT handled anywhere** (no string "hello" in the repo).
- `isMaster: 1.0` (double) and `NumberLong(1)` are not detected, so they are forwarded upstream.
- The request must target `admin`: `admin.$cmd` for OP_QUERY (`operations.go:211`), `$db: "admin"` for OP_MSG (261).

## The fake reply (`ismaster.go:16-67`)

- It is **always encoded as OP_REPLY**, even if the request was OP_MSG. That is a protocol violation, and libmongoc
  likely errors on it (unverified). The driver's own handshake uses OP_QUERY, so this only bites app-issued
  `isMaster` commands. Use `ping` for health checks.
- The body depends on the Go driver topology kind (`client.Description().Kind`):
  - `Single` (upstream direct connection) → standalone persona: no `msg`, `readOnly:false`. The client then disables
    transactions and retryable writes.
  - **Anything else** ("IMPORTANT: if not Single, assumes Sharded!") → **mongos persona**: `msg:"isdbgrid"`,
    `saslSupportedMechs: []`. An Atlas replica set (`ReplicaSetWithPrimary` / `ReplicaSetNoPrimary`) lands here.
- Common fields: `ismaster:true`, `maxBsonObjectSize 16777216`, `maxMessageSizeBytes 48000000`,
  `maxWriteBatchSize 100000`, `logicalSessionTimeoutMinutes 30`, **`maxWireVersion 8`**, `minWireVersion 0`, `ok 1`.
- `localTime` is wrongly encoded as a sub-document `{"$date": ms}` instead of a BSON datetime. Cosmetic.
- **Missing fields:**
  - `helloOk`. Its absence keeps clients on legacy isMaster, which is *good*, because `hello` isn't intercepted.
  - `topologyVersion`. No streaming monitoring, which is good, because exhaust isn't supported.
  - `compression`. No compression, which is good, because OP_COMPRESSED isn't decoded.
  - `isWritablePrimary`, `connectionId`, `speculativeAuthenticate`, `$clusterTime`, `operationTime`, `serviceId`.
  - `setName` / `hosts` / `me` are absent on purpose, so the client never discovers the real members.
- The client's handshake fields (`client`, `compression`, `saslSupportedMechs`, `speculativeAuthenticate`,
  `loadBalanced`, `apiVersion`) are ignored.

### Wire versions

| Server | maxWireVersion |
|---|---|
| 4.2 | 8 (what the proxy claims) |
| 4.4 | 9 |
| 5.0 | 13 |
| 6.0 | 17 |
| 7.0 | 21 |
| 8.0 | 25 |
| 9.0 | 29 |

Features libmongoc refuses or downgrades because it believes the server is 4.2:
- `hint` on delete/findAndModify (needs 9)
- `allowDiskUse` on find, `comment` on all commands (4.4)
- snapshot reads (13)
- `let` (5.0)
- Queryable Encryption (21+)
- client `bulkWrite` / PHP `Client::bulkWrite` (25)

libmongoc minimum wire: 1.28 → 7, 2.1 → 8. NEWS for 2.4 announces → 9 in a future minor release.
**At that point the proxy becomes unusable** until `maxWireVersion` is raised.

## Command classification (`command.go:35-76`)

`CommandAndCollection` scans a **fixed list in fixed order**, not the document's first key:
1. Collection commands (string value): aggregate, count, createIndexes, delete, distinct, drop, dropIndexes, find, findAndModify, insert, listIndexes, mapReduce, update
2. Presence-only: abortTransaction, aggregate, commitTransaction, dropDatabase, isMaster, ismaster, listCollections, listDatabases
3. `getMore` (collection from the `collection` field)
4. `endSessions`
5. Everything else is `Unknown`: hello, ping, killCursors, saslStart/saslContinue, create, collMod, renameCollection, bulkWrite, explain, lowercase `findandmodify`, …

`IsWrite` (40-46) is used only for `DisableWrites`: commitTransaction, createIndexes, delete, drop, dropIndexes,
dropDatabase, findAndModify, insert, update. **These writes leak through:** aggregate `$out`/`$merge`, mapReduce
`out`, create, collMod, renameCollection, bulkWrite, applyOps, user/role commands, and any OP_QUERY command.

## Special treatment summary

- `getMore` / reply `cursor.id` → cursor cache (see 03).
- `startTransaction` / `commitTransaction` / `abortTransaction` → transaction cache (see 03).
- `killCursors` → nothing. The cache entry leaks until the 24h TTL.
- Auth commands → forwarded on arbitrary pooled connections. Broken by design (see 04).
- `moreToCome` request → write upstream, no read, no client reply.
- Exhaust → **not supported**. One reply frame is read, and the remaining frames poison the pooled connection.
