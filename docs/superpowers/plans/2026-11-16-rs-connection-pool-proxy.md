# Replica-Set Connection Pool Proxy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn mongobetween into a transparent connection-pooling proxy that a PHP app (ext-mongodb / libmongoc) can use, over plain TCP on the local network, in front of an **unsharded MongoDB Atlas replica set** (TLS upstream). Sharded/mongos behaviour must keep working.

**Architecture:** Keep the existing design. The proxy still answers the handshake locally as a mongos (`msg: "isdbgrid"`) and forwards every other wire message byte-for-byte through a Go-driver pool. We fix the gaps that break replica sets and modern drivers:
- handshake: `hello`, a reply in the same opcode as the request, the real wire version;
- error replies instead of socket closes;
- read preference;
- cursor and transaction pinning;
- timeouts, frame limits and shutdown.

We add a replica-set test environment and a PHP smoke test that prove it end to end.

**Tech Stack:** Go 1.27 toolchain (`go.mod` `go 1.22`), `go.mongodb.org/mongo-driver v1.17.10` (supports wire 6–25, MongoDB ≤ 8.0), Docker / docker compose (Rancher Desktop locally), `mongo:8.0`, PHP 8.3 + ext-mongodb for the smoke test.

**Spec:** this plan plus the knowledge base in `.claude/knowledge/`. Read `README.md`, `03-routing-and-pinning.md`, `05-sharded-vs-unsharded.md` and `07-change-backlog.md`. The fork evaluation is summarised under "Base and sources" below.

## Decisions (agreed or recommended — confirm before execution)

1. **Base = upstream `coinbase/mongobetween` master `ea5a87e`** (our `master`), not a fork. The forks were evaluated (2026-11-16): none solves `hello`, the wire version or the stale transaction pin, and the most developed one (jitsucom) brings regressions. Only selected commits are cherry-picked (see below).
2. **Go driver v1.17.10 now.** v2 is a separate, later plan: its selectors are internal and it would need ~400 lines vendored. Jonoans' fork `c753821` is the reference map for that migration.
3. **App → proxy:** optional inbound TLS (flag-gated, off by default) and **inbound SCRAM-SHA-256 auth with proxy-only credentials**, which must differ from the Atlas credentials. Clients authenticate to the proxy per client connection. Upstream connections stay authenticated with the Atlas user. Auth uses `github.com/xdg-go/scram` server mode (already in `go.sum`), not jitsucom's hand-rolled SCRAM, which serves only as a behaviour reference.
4. **Proxy → Atlas:** TLS required. A new flag `-require-upstream-tls` makes startup fail if any upstream URI has TLS off; production sets it.
5. **Read preference:** honour the client's `$readPreference` (mode + tags + maxStaleness). The default stays primary.
6. Module path stays `github.com/coinbase/mongobetween` so upstream/fork cherry-picks apply cleanly.

## Base and sources

Remotes to add (fetch only, never merge whole branches):

| Remote | URL | Use |
|---|---|---|
| `crowdsec` | `https://github.com/crowdsecurity/mongobetween.git` | cherry-pick `6c9306f` (= upstream PR #92 read preference, applies cleanly on `ea5a87e`) |
| `drcapulet` | `https://github.com/drcapulet/mongobetween.git` | cherry-pick `7af1b0f` (cluster tag on pool metrics) |
| `jitsucom` | `https://github.com/jitsucom/mongobetween.git` | reference only: frame-size check (`8368320`), `BuildOpMsgResponse` |
| upstream branch `dont-forward-opmsg-error` `c7b33e9` | origin | reference only: fake 11602 error |

When code is copied from jitsucom, keep its NOTICE attribution (Apache-2.0).

## Global Constraints

- Go toolchain 1.27.x locally; `go.mod` directive `go 1.22`; driver `go.mongodb.org/mongo-driver v1.17.10`.
- All tests run with `make test-docker`: compose project `mongobetween-test`, no host ports published (host port 27017 is used by another project's shared MongoDB, so never point tests at host `localhost:27017`).
- Test images: `mongo:8.0` for the standalones `mongo1..3` and the replica set `rs1..rs3` (`rs0`).
- Fake handshake reply must **never** contain `helloOk`, `topologyVersion`, `compression`, `setName`, `hosts` or `speculativeAuthenticate`.
- Default selection when there is no `$readPreference` stays `readpref.Primary()`.
- Secrets: no credential in argv. Use `-password '${VAR}'` / `${VAR}` in URIs (already supported by `expandEnv`). Never log document payloads.
- One logical change per commit; Conventional Commit messages; every task's tests pass under `make test-docker` before commit.
- Work on branch `feature/rs-connection-pool` off `master`.

## Review Focus

1. **Primary stepdown in the middle of PHP traffic** (Atlas maintenance). Expected: retryable reads/writes and `withTransaction` succeed within `serverSelectionTimeoutMS`, with no socket errors. Covered by `TestRSFailoverRetryableWrite` and `TestRSTransactionAfterFailover` (Task 12).
2. **`getMore` on a cursor opened on a secondary for a db-level aggregate (`$cmd.aggregate`).** Expected: the getMore reaches the same secondary, so no CursorNotFound. Covered by `TestRSDbLevelCursorOnSecondary` (Task 12).
3. **App-issued handshake commands** (`executeCommand('admin', ['isMaster'=>1])`, `hello`, `serverApi`). Expected: a valid reply in the request's opcode, and the client stays on the proxy. Covered by the Task 3 tests and the PHP smoke checks `isMasterCommand` / `serverApiPing` (Task 13).
4. **Upstream unavailable** (wrong URI, Atlas paused). Expected: the client gets a MongoDB error `11602` with a retry label, not a dropped socket. Covered by `TestProxyReturnsErrorWhenUpstreamUnavailable` (Task 4).
5. **Garbage or TLS bytes on the plain-TCP port** (misconfigured PHP URI with `tls=true`). Expected: the connection closes quickly, with no huge allocation and no hang. Covered by `TestReadWireMessageRejectsBadSize` (Task 5).

---

## File Structure

| File | Change | Responsibility |
|---|---|---|
| `docker-compose.yml` | modify | standalones + 3-node RS, no host ports, healthchecks, `rs-init` |
| `scripts/test-docker.sh` | create | run the full Go test suite in a `golang:1.27` container on the compose network |
| `Makefile` | modify | `test-docker`, `php-smoke` targets |
| `.github/workflows/workflow.yaml` | modify | run `make test-docker` on `ubuntu-latest` |
| `Dockerfile` | modify | `golang:1.27` build, `distroless/static-debian12:nonroot` runtime |
| `mongo/settings.go` | create | `Settings` struct passed to `Connect` |
| `mongo/handshake.go` (rename of `mongo/ismaster.go`) | modify | hello/isMaster replies, wire version mirroring |
| `mongo/errors.go` | create | `ProxyError`, `ClassifyProxyError`, `ErrorResponse`, `ErrWritesDisabled` |
| `mongo/exhaust.go` | create | `StripExhaustAllowed` |
| `mongo/command.go` | modify | `Hello`, `KillCursors`, `HandshakeCommandOf` |
| `mongo/operations.go` | modify | `CursorNamespace`, `KillCursorIDs`, `ReadPref` |
| `mongo/pinning.go` | create | `pinnedServer` (pure txn/cursor pin lookup) |
| `mongo/mongo.go` | modify | wire Settings, checkout timeout, exhaust strip, pin fixes, read-pref selection |
| `proxy/connection.go` | modify | frame limits, error replies, handshake via `Mongo.HandshakeResponse` |
| `proxy/proxy.go`, `mongobetween.go` | modify | shutdown timeout, signal handling |
| `config/config.go` | modify | new flags, `-require-upstream-tls`, cluster tag |
| `proxy/rs_test.go` | create | replica-set integration tests (skip when `MONGOBETWEEN_RS_URI` is unset) |
| `test/php/Dockerfile`, `test/php/smoke.php`, `scripts/php-smoke.sh` | create | PHP/libmongoc end-to-end smoke test |
| `docs/atlas-poc.md` | create | Atlas proof-of-concept runbook |

---

### Task 1: Docker test harness with a replica set

**Files:**
- Modify: `docker-compose.yml`, `Makefile`
- Create: `scripts/test-docker.sh`, `proxy/rs_test.go`

**Interfaces:**
- Produces: `make test-docker [ARGS="-run X"]`; env `CI=true` and `MONGOBETWEEN_RS_URI=mongodb://rs1:27017,rs2:27017,rs3:27017/?replicaSet=rs0` inside the test container; test helper `rsURI(t *testing.T) string` (skips when unset) in `proxy/rs_test.go`.

- [ ] **Step 1: Create the branch.** `git checkout -b feature/rs-connection-pool master`
- [ ] **Step 2: Rewrite `docker-compose.yml`.**
  - Services `mongo1..3`: `mongo:5.0`, unchanged except **no `ports:`**.
  - Services `rs1..rs3`: `mongo:8.0`, `command: ["--replSet","rs0","--bind_ip_all"]`, healthcheck `mongosh --quiet --eval 'db.adminCommand({ping:1}).ok'`.
  - One-shot `rs-init` (`mongo:8.0`, `restart: "no"`, depends on `rs1..3` healthy). It runs `mongosh --host rs1` to call `rs.initiate({_id:"rs0",members:[{_id:0,host:"rs1:27017"},{_id:1,host:"rs2:27017"},{_id:2,host:"rs3:27017"}]})` if not yet initiated, then loops until `db.hello().isWritablePrimary` is true on some member (timeout 60s, non-zero exit on timeout).
- [ ] **Step 3: Create `scripts/test-docker.sh`** (executable, `set -euo pipefail`):
  1. `docker compose -p mongobetween-test up -d --wait mongo1 mongo2 mongo3 rs1 rs2 rs3`
  2. `docker compose -p mongobetween-test run --rm rs-init`
  3. `docker run --rm --network mongobetween-test_default -e CI=true -e MONGOBETWEEN_RS_URI=... -v "$PWD":/src:ro -v "$(go env GOMODCACHE)":/go/pkg/mod golang:1.27 sh -c 'cp -r /src /work && cd /work && go test -count 1 -race ./... '"${ARGS:-}"`

  Add a Makefile target `test-docker: ; ./scripts/test-docker.sh`.
- [ ] **Step 4: Write the harness test** in `proxy/rs_test.go`:

```go
func rsURI(t *testing.T) string {
	uri := os.Getenv("MONGOBETWEEN_RS_URI")
	if uri == "" {
		t.Skip("MONGOBETWEEN_RS_URI not set")
	}
	return uri
}

func TestRSHarness(t *testing.T) {
	c, err := mongo.Connect(context.Background(), options.Client().ApplyURI(rsURI(t)))
	require.NoError(t, err)
	defer c.Disconnect(context.Background())
	var res bson.M
	require.NoError(t, c.Database("admin").RunCommand(context.Background(), bson.D{{"hello", 1}}).Decode(&res))
	assert.Equal(t, "rs0", res["setName"])
}
```

- [ ] **Step 5: Run.** `make test-docker`. Expected: every package `ok`, and `TestRSHarness` PASS (not SKIP; check with `ARGS="-run TestRSHarness -v"`).
- [ ] **Step 6: Commit.** `test: add docker-based test harness with replica set`

---

### Task 2: Go driver v1.17.10, Go version, image, CI

**Files:**
- Modify: `go.mod`, `go.sum`, `mongo/operations.go` (and any other call sites the compiler flags), `Dockerfile`, `.github/workflows/workflow.yaml`, `docker-compose.yml` (standalones → `mongo:8.0`)

**Interfaces:**
- Produces: the driver at v1.17.10; `topology.SupportedWireVersions` = `[6, 25]`, which Task 3 uses as the cap.

- [ ] **Step 1: Bump.** `go get go.mongodb.org/mongo-driver@v1.17.10`, set `go 1.22` in `go.mod`, `go mod tidy`.
- [ ] **Step 2: Fix compile errors.** `go build ./... && go vet ./...`. The known break is `driver.ExtractErrorFromServerResponse(ctx, doc)`: pass `context.Background()` (`mongo/operations.go` `opMsg.Error`, `opReply.Error`). The reference for this exact diff is mdekkernet `e0a900b`.
- [ ] **Step 3: Switch the standalones `mongo1..3` to `mongo:8.0`.** Run `make test-docker`. Expected: all `ok`.
- [ ] **Step 4: Update the Dockerfile.** Build stage `golang:1.27`, `CGO_ENABLED=0 go build -trimpath -ldflags="-s -w"`. Runtime `gcr.io/distroless/static-debian12:nonroot` (ships CA certs and `/tmp`, runs as non-root). Verify with `docker build -t mongobetween:dev .`; it must succeed.
- [ ] **Step 5: Update CI.** Replace the `tests` job body with `runs-on: ubuntu-latest`, `actions/setup-go` using `GO_VERSION: "1.27"`, and `make test-docker`. Remove the `node` container and the `services:` block. Keep the lint and Salus jobs and bump the lint job's Go version.
- [ ] **Step 6: Commit.** `build: upgrade mongo-driver to v1.17.10, go 1.22, distroless image`

---

### Task 3: Handshake — `hello`, same-opcode reply, real wire version

**Files:**
- Modify: `mongo/command.go:78-92`, `mongo/operations.go:210-215,260-265` (detection), rename `mongo/ismaster.go` → `mongo/handshake.go`, `mongo/mongo.go` (`Connect`), `proxy/connection.go:179-183`, `config/config.go` (flag), all `Connect(...)` call sites in tests
- Create: `mongo/settings.go`
- Test: `mongo/handshake_test.go` (replaces `ismaster_test.go`), `proxy/proxy_test.go`

**Interfaces:**
- Produces:
  - `type Settings struct { Ping bool; MaxWireVersion int32 }` in `mongo/settings.go`. Task 4 adds `CheckoutTimeout time.Duration`.
  - `func Connect(log *zap.Logger, sd *statsd.Client, opts *options.ClientOptions, s Settings) (*Mongo, error)`
  - `const Hello Command = "hello"`
  - `func HandshakeCommandOf(doc bsoncore.Document) Command`: returns `Hello`, `IsMaster` or `Unknown`. The value is truthy when it is int32/int64/double > 0 or bool true. `IsIsMasterDoc(doc)` becomes `HandshakeCommandOf(doc) != Unknown`.
  - `func ReportedMaxWireVersion(topo description.Topology, override int32) int32`
  - `func BuildHandshakeResponse(req Operation, topo description.Topology, maxWireOverride int32) (*Message, error)`
  - `func (m *Mongo) HandshakeResponse(req Operation) (*Message, error)`
  - Flag `-max-wire-version int` (default `0` = mirror upstream).

- [ ] **Step 1: Write failing tests** in `mongo/handshake_test.go`:
  - `TestHandshakeCommandOf`. Table test:

    | Input | Expected |
    |---|---|
    | `{isMaster: int32(1)}` | `IsMaster` |
    | `{ismaster: true}` | `IsMaster` |
    | `{isMaster: int64(1)}` | `IsMaster` |
    | `{isMaster: 1.0}` | `IsMaster` |
    | `{hello: int32(1)}` | `Hello` |
    | `{hello: true}` | `Hello` |
    | `{isMaster: int32(0)}` | `Unknown` |
    | `{hello: false}` | `Unknown` |
    | `{find: "c"}` | `Unknown` |

  - `TestReportedMaxWireVersion`. Use a `description.Topology` with servers built via `description.Server{Kind: description.RSPrimary, WireVersion: &description.VersionRange{Min: 0, Max: 21}}`:

    | Case | Expected |
    |---|---|
    | override 13 | 13 |
    | primary 21 + secondary 25 | 21 (lowest Max among data-bearing members) |
    | no servers / unknown kinds only | 17 |
    | primary 30 | 25 (capped at `topology.SupportedWireVersions.Max`) |

  - `TestHandshakeResponseOpMsgHello`. Build an OP_MSG request `{hello: 1, $db: "admin"}`. Topology kind `ReplicaSetWithPrimary` with a primary at wire 21. Assertions:
    - response `Op.OpCode() == wiremessage.OpMsg`
    - the decoded doc has `isWritablePrimary == true`, `msg == "isdbgrid"`, `maxWireVersion == 21`, `minWireVersion == 0`, `logicalSessionTimeoutMinutes == 30`
    - **absent:** `ismaster`, `helloOk`, `topologyVersion`, `compression`, `setName`, `hosts`
    - `localTime` has BSON type `bsontype.DateTime`
  - `TestHandshakeResponseOpMsgIsMaster`: as above with `{isMaster: 1, $db: "admin"}`. Expect `ismaster == true` and no `isWritablePrimary`.
  - `TestHandshakeResponseOpQuery`: an OP_QUERY `{isMaster: 1}` on `admin.$cmd` gets an `OpReply` response.
  - `TestHandshakeResponseSingle`: topology kind `Single` gives no `msg` field.
- [ ] **Step 2: Run, expect FAIL.** `make test-docker ARGS="-run Handshake ./mongo/"` fails with undefined symbols.
- [ ] **Step 3: Implement.**
  - `ReportedMaxWireVersion`:
    - `override > 0` wins.
    - Otherwise take the minimum `WireVersion.Max` over servers of kind `RSPrimary`, `RSSecondary`, `Mongos` or `Standalone`.
    - Cap the result at `topology.SupportedWireVersions.Max`.
    - Fallback `17` when there are none.
  - Reply opcode follows the request: `*opMsg` → `opMsg{flags: 0, sections: []opMsgSection{&opMsgSectionSingle{msg: doc}}}`; `*opQuery` → the existing `opReply`.
  - `localTime` via `primitive.NewDateTimeFromTime(time.Now())`.
  - `proxy/connection.go` calls `client.HandshakeResponse(msg.Op)`.
  - OP_MSG detection still requires `$db == "admin"`, and OP_QUERY detection still requires `admin.$cmd`. Handshakes on other databases stay forwarded; Task 10 relies on that.
- [ ] **Step 4: Proxy-level test.** In `proxy/proxy_test.go` add `TestProxyAppIssuedHandshakeCommands`. Through the proxy, the Go driver runs `RunCommand(admin, {isMaster:1})` and `RunCommand(admin, {hello:1})`. Both decode without error. `msg` is absent (the upstream is a standalone, so the persona is `Single`), and `maxWireVersion == 25` (mongo:8.0).
- [ ] **Step 5: Run.** `make test-docker`. Expected: all PASS.
- [ ] **Step 6: Commit.** `feat: intercept hello, reply in request opcode, mirror upstream wire version`

---

### Task 4: MongoDB error replies instead of socket close; checkout timeout

**Files:**
- Create: `mongo/errors.go`, `mongo/errors_test.go`
- Modify: `mongo/settings.go`, `mongo/mongo.go:263-282` (checkout), `proxy/connection.go:67-134,161-186`, `config/config.go` (flag `-checkout-timeout`, default `10s`)
- Test: `proxy/proxy_test.go`

**Interfaces:**
- Consumes: `Settings` (Task 3).
- Produces:
  - `var ErrWritesDisabled = errors.New("writes are disabled by mongobetween")`
  - `type ProxyError struct { Code int32; CodeName, Message string; Labels []string }`
  - `func ClassifyProxyError(err error, req Operation) ProxyError`
  - `func ErrorResponse(req Operation, pe ProxyError) (*Message, error)`
  - `Settings.CheckoutTimeout time.Duration`

- [ ] **Step 1: Write failing unit tests** in `mongo/errors_test.go`:
  - `TestClassifyWritesDisabled`: gives `Code 13`, `CodeName "Unauthorized"`, `Labels` empty.
  - `TestClassifyUpstreamErrorPlainWrite`. Use `errors.New("server selection error")` with an OP_MSG `{insert:"c", lsid:{id:<bin>}, txnNumber:int64(1), $db:"test"}`. Expect `Code 11602`, `CodeName "InterruptedDueToReplStateChange"`, `Labels == ["RetryableWriteError"]`.
  - `TestClassifyUpstreamErrorInTransaction`. Request `{insert:"c", lsid, txnNumber, autocommit:false}` gives `Labels == ["TransientTransactionError"]`.
  - `TestClassifyUpstreamErrorCommit`. `{commitTransaction:1, lsid, txnNumber, autocommit:false}` gives `Labels == ["RetryableWriteError","UnknownTransactionCommitResult"]`.
  - `TestErrorResponseOpMsg`. The reply is OP_MSG; the doc has `ok == 0.0`, `code == 11602`, `codeName`, `errmsg` prefixed `"mongobetween: "`, and `errorLabels`.
  - `TestErrorResponseOpQuery`. The reply is OP_REPLY with the same doc fields.
- [ ] **Step 2: Run, expect FAIL.**
- [ ] **Step 3: Implement.**
  - `ClassifyProxyError`: `ErrWritesDisabled` gives 13. Everything else gives 11602, with labels chosen by the transaction/commit rules in Step 1.
  - In `proxy/connection.go`:
    - `roundTrip` returns `mongo.ErrWritesDisabled` for disabled writes.
    - `handleMessage` handles any error from `roundTrip`, except a missing client lookup. For a **non-`moreToCome`** request it writes `ErrorResponse(...)` to the client, logs at warn level (`"Proxy error reply"` with code and command), and **keeps the connection open**. For a `moreToCome` request it logs and continues.
    - Read/decode errors still close the connection.
  - `checkoutConnection`: `ctx, cancel := context.WithTimeout(m.roundTripCtx, s.CheckoutTimeout)` when the timeout is > 0.
- [ ] **Step 4: Proxy tests.**
  - `TestProxyReturnsErrorWhenUpstreamUnavailable`. Start a proxy on port 27024 pointing at `mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=300&connectTimeoutMS=200`. A Go-driver client with `RetryWrites(false)` runs `InsertOne`. Assert the error `errors.As(err, &mongo.WriteException{})` or `mongo.CommandError` with `Code == 11602` and `HasErrorLabel("RetryableWriteError")`. Then a second command on the same client also returns 11602 rather than a network error.
  - Update `TestProxyWithDynamicConfig` so the disabled-writes insert asserts `CommandError.Code == 13`.
- [ ] **Step 5: Run.** `make test-docker`. Expected: all PASS.
- [ ] **Step 6: Commit.** `feat: return retryable MongoDB errors instead of closing client sockets`

---

### Task 5: Frame-size limits and exhaust stripping

**Files:**
- Modify: `proxy/connection.go:136-159`, `mongo/mongo.go` (RoundTrip, before the upstream write)
- Create: `mongo/exhaust.go`, `mongo/exhaust_test.go`, `proxy/connection_test.go`

**Interfaces:**
- Produces:
  - `const MaxWireMessageSize = 48000000 + 16*1024` (in `mongo`)
  - `func StripExhaustAllowed(wm []byte) []byte`: returns `wm` untouched unless it is an OP_MSG with the `ExhaustAllowed` bit set. In that case it returns a **copy** with the bit cleared. If `ChecksumPresent` is also set, the copy drops the trailing 4 checksum bytes, clears that bit and rewrites the length header.

- [ ] **Step 1: Write failing tests.**
  - `TestStripExhaustAllowed`:
    - bit cleared, length unchanged, `Decode` succeeds;
    - the input slice is unchanged;
    - the checksum variant comes back 4 bytes shorter, with both bits cleared and the header length equal to `len(result)`;
    - a non-exhaust message returns the same slice.
  - `TestReadWireMessageRejectsBadSize`. Use `net.Pipe`; the client writes the little-endian size `0x16030100` (start of a TLS ClientHello), then `3`, then `MaxWireMessageSize+1`. Each case: `readWireMessage` returns an error containing `"invalid message size"` within 100ms, and `cap(c.buffer)` stays `< 1024`.
- [ ] **Step 2: Run, expect FAIL.**
- [ ] **Step 3: Implement.** In `readWireMessage`, reject `size < 16 || size > mongo.MaxWireMessageSize` before allocating. In `RoundTrip`, forward `StripExhaustAllowed(msg.Wm)`. If an upstream reply has `MoreToCome` set, log an error and close that pooled connection (`conn.Close()` after marking it via `processError` with a network error) so the pool never reuses it.
- [ ] **Step 4: Run.** `make test-docker`. Expected: all PASS.
- [ ] **Step 5: Commit.** `fix: bound client frame size and never let exhaust replies reach the pool`

---

### Task 6: Cursor pinning by full namespace + killCursors

**Files:**
- Modify: `mongo/operations.go` (`Operation` interface + all op types), `mongo/command.go` (`KillCursors` in `collectionCommands`), `mongo/cursor_cache.go` (parameter renamed `ns`), `mongo/mongo.go:152-213`
- Test: `mongo/operations_test.go`, `mongo/cursor_cache_test.go`

**Interfaces:**
- Produces, on `Operation`:
  - `CursorNamespace() string`:
    - requests: `getMore` and `killCursors` return `$db + "." + <collection>`;
    - responses: `cursor.ns`;
    - everything else: `""`.
  - `KillCursorIDs() []int64`: the `cursors` array of the `killCursors` command (OP_MSG), the IDs of legacy OP_KILL_CURSORS, otherwise `nil`.
  - `const KillCursors Command = "killCursors"`

- [ ] **Step 1: Write failing tests.**

| Input | Assertion |
|---|---|
| OP_MSG `{getMore: int64(5), collection: "$cmd.aggregate", $db: "test"}` | `CursorNamespace() == "test.$cmd.aggregate"`, `CursorID() == (5, true)` |
| OP_MSG reply `{cursor: {id: int64(5), ns: "test.$cmd.aggregate", firstBatch: []}, ok: 1}` | `CursorNamespace() == "test.$cmd.aggregate"` |
| OP_MSG `{killCursors: "coll", cursors: [int64(1), int64(2)], $db: "test"}` | `KillCursorIDs() == []int64{1,2}`, `CursorNamespace() == "test.coll"`, command `KillCursors` |
| OP_MSG `{find: "coll", $db: "test"}` | `CursorNamespace() == ""`, `KillCursorIDs() == nil` |

- [ ] **Step 2: Run, expect FAIL.**
- [ ] **Step 3: Implement in `RoundTrip`.**
  - **Selection key:** `(requestCursorID, op.CursorNamespace())` for getMore. For killCursors, use the first ID in `KillCursorIDs()` with the request namespace.
  - **After the reply:**
    - A non-zero response cursor ID: `cursors.add(id, responseNS or requestNS)`.
    - A zero response ID on a getMore: remove the entry.
    - A `killCursors` request: remove every ID in `KillCursorIDs()`.
- [ ] **Step 4: Run.** `make test-docker`. Expected: all PASS.
- [ ] **Step 5: Commit.** `fix: key cursor pins by full namespace and evict on killCursors`

---

### Task 7: Stale transaction pin fix

**Files:**
- Create: `mongo/pinning.go`, `mongo/pinning_test.go`
- Modify: `mongo/mongo.go:186-191,233-252`

**Interfaces:**
- Produces: `func pinnedServer(txns *transactionCache, cursors *cursorCache, td *TransactionDetails, cursorID int64, ns string) (driver.Server, bool)`, used by `selectServer`.

- [ ] **Step 1: Write failing tests** (use the mock `driver.Server` already in `transaction_cache_test.go`):
  - `TestPinnedServerStartTransactionIgnoresStalePin`: lsid L is pinned to A, and `td{LsID: L, IsStartTransaction: true}` gives `(nil, false)`.
  - `TestPinnedServerContinuesTransaction`: the same pin with `IsStartTransaction: false` gives `(A, true)`.
  - `TestPinnedServerCursor`: no `td`, cursor `(5, "test.c")` pinned to B, gives `(B, true)`.
- [ ] **Step 2: Run, expect FAIL.**
- [ ] **Step 3: Implement.**
  - `selectServer` uses `pinnedServer`.
  - In `RoundTrip`, on a network/round-trip error while `transactionDetails != nil`, call `m.transactions.remove(lsid)`.
  - Starting a transaction overwrites any old pin (`add` already does that).
- [ ] **Step 4: Run.** `make test-docker`. Expected: all PASS.
- [ ] **Step 5: Commit.** `fix: never reuse a stale transaction pin for a new transaction`

---

### Task 8: Read preference (cherry-pick #92 + tags / maxStaleness)

**Files:**
- Cherry-pick: crowdsec `6c9306f` (touches `mongo/mongo.go`, `mongo/operations.go`, `mongo/readpref_test.go`)
- Modify: `extractReadPref` in `mongo/operations.go`, `selectServer` in `mongo/mongo.go`

**Interfaces:**
- Consumes: `pinnedServer` (Task 7). Priority: txn pin > cursor pin > client read preference > primary.
- Produces: `Operation.ReadPref() (*readpref.ReadPref, bool)` (from #92) and `func extractReadPref(doc bsoncore.Document) (*readpref.ReadPref, bool)`, which now honours `tags` and `maxStalenessSeconds`.

- [ ] **Step 1: Cherry-pick.**
  - `git remote add crowdsec https://github.com/crowdsecurity/mongobetween.git && git fetch crowdsec && git cherry-pick 6c9306f`
  - Resolve conflicts with Tasks 6–7. The `selectServer` signature becomes `(..., rp *readpref.ReadPref)` while keeping `pinnedServer` first.
  - Run `make test-docker ARGS="-run ReadPref ./mongo/"`. Expected: PASS.
- [ ] **Step 2: Write failing tests** in `mongo/readpref_test.go`:
  - `TestExtractReadPrefTags`: `{$readPreference: {mode: "secondary", tags: [{nodeType: "ANALYTICS"}, {}]}}` gives mode `SecondaryMode` and `TagSets()` length 2, with the first set `nodeType=ANALYTICS`.
  - `TestExtractReadPrefMaxStaleness`: `{mode: "secondaryPreferred", maxStalenessSeconds: int32(90)}` gives `MaxStaleness() == (90s, true)`. Also accept int64 and double 90.
  - `TestExtractReadPrefPrimaryWithTagsInvalid`: `{mode: "primary", tags: [{a:"b"}]}` gives `(nil, false)`, which falls back to primary.
- [ ] **Step 3: Run, expect FAIL.**
- [ ] **Step 4: Implement** with `readpref.New(mode, readpref.WithTagSets(...), readpref.WithMaxStaleness(...))`. If `readpref.New` returns an error, return `(nil, false)`.
- [ ] **Step 5: Run.** `make test-docker`. Expected: all PASS.
- [ ] **Step 6: Commit.** `feat: honour client read preference incl. tags and maxStalenessSeconds` (keep the cherry-picked commit separate, with its original author).

---

### Task 9: Graceful shutdown, upstream TLS enforcement, metrics tag

**Files:**
- Cherry-pick: drcapulet `7af1b0f` (`config/config.go`)
- Modify: `mongobetween.go:21-90`, `proxy/proxy.go:64-83,132-186`, `config/config.go` (flags `-shutdown-timeout`, default `30s`; `-require-upstream-tls`, default `false`)
- Test: `config/config_test.go`, `proxy/proxy_test.go`

**Interfaces:**
- Produces:
  - `func (p *Proxy) ShutdownWithTimeout(d time.Duration)`: stop accepting, wait up to `d` for client connections to close, then call `Kill()`.
  - `func validateUpstreamTLS(clients []client) error`: errors with `"upstream TLS required but disabled for <address>"` if any `opts.TLSConfig == nil`.

- [ ] **Step 1: Cherry-pick.** `git remote add drcapulet https://github.com/drcapulet/mongobetween.git && git fetch drcapulet && git cherry-pick 7af1b0f`
- [ ] **Step 2: Write failing tests.**
  - `TestValidateUpstreamTLS`:
    - `mongodb://h:27017` gives an error;
    - `mongodb://h:27017/?tls=true` gives nil;
    - `mongodb+srv://` cannot be resolved offline, so build the `ClientOptions` with `SetTLSConfig(&tls.Config{})` and expect nil.
  - `TestProxyShutdownWithTimeoutClosesIdleClients`. A client holds an idle connection (go driver `minPoolSize=1`). `ShutdownWithTimeout(500ms)` returns within 2s, and `p.Run()` has returned.
- [ ] **Step 3: Run, expect FAIL.**
- [ ] **Step 4: Implement.**
  - The signal handler calls `ShutdownWithTimeout(cfg.shutdownTimeout)` on the first SIGINT or SIGTERM. Any second signal calls `Kill()`.
  - After all proxies stop, call `Close()` on each `*mongo.Mongo`.
  - With `-require-upstream-tls`, `parseFlags` returns the `validateUpstreamTLS` error.
- [ ] **Step 5: Run.** `make test-docker`. Expected: all PASS.
- [ ] **Step 6: Commit.** `feat: bounded graceful shutdown and upstream TLS enforcement`

---

### Task 10: Inbound TLS (optional, flag-gated)

**Files:**
- Modify: `config/config.go` (flags), `proxy/proxy.go:104-130` (`listen`)
- Create: `proxy/tls.go`, `proxy/tls_test.go`, `test/certs/gen.sh` (test-only self-signed CA + server cert, generated at test time, never committed)

**Interfaces:**
- Produces:
  - Flags `-tls-cert-file`, `-tls-key-file` (both set means TLS is on, one alone is a startup error), `-tls-ca-file` (optional; when set, client certificates are required and verified, i.e. mTLS).
  - `func loadServerTLSConfig(certFile, keyFile, caFile string) (*tls.Config, error)` with `MinVersion: tls.VersionTLS12`.
  - `NewProxy(...)` gets a `tlsConfig *tls.Config` parameter (nil means plain). The listener is wrapped with `tls.NewListener`.

- [ ] **Step 1: Write failing tests.**
  - `TestLoadServerTLSConfig`:
    - cert without key gives an error;
    - a valid pair gives `MinVersion == tls.VersionTLS12` and `ClientAuth == tls.NoClientCert`;
    - with a CA file, `ClientAuth == tls.RequireAndVerifyClientCert`.
  - `TestProxyTLS`:
    - Proxy on port 27025 with the generated cert. A Go-driver client with `tls=true` and the test CA pings OK.
    - A plain (non-TLS) client fails within 2s; the proxy must not hang.
- [ ] **Step 2: Run, expect FAIL.**
- [ ] **Step 3: Implement.** `test/certs/gen.sh` uses `openssl` to write into a temp dir passed as an argument. The tests call it from `TestMain`, or generate the certs in Go with `crypto/x509`; prefer Go to avoid an openssl dependency in CI.
- [ ] **Step 4: Run.** `make test-docker`. Expected: all PASS.
- [ ] **Step 5: Commit.** `feat: optional TLS on the client listener`

---

### Task 11: Inbound SCRAM-SHA-256 authentication (proxy-only credentials)

**Files:**
- Create: `proxy/auth.go`, `proxy/auth_test.go`
- Modify: `proxy/connection.go` (per-connection auth state, gate), `mongo/handshake.go` (advertise mechs), `config/config.go` (flag), `go.mod` (`github.com/xdg-go/scram` becomes a direct dependency)

**Interfaces:**
- Consumes: `ErrorResponse` / `ProxyError` (Task 4), `BuildHandshakeResponse` (Task 3).
- Produces:
  - Flag `-auth-users-file <path>`. When it is empty, auth is off and behaviour is unchanged.
    - File format: one `username:password` per line. Split on the **first** `:`. `#` comments and blank lines are allowed. A duplicate username is an error.
    - Startup logs a warning if the file mode allows group/other access.
    - The path supports `${VAR}`. Never log passwords.
  - `type Users struct{...}` and `func LoadUsers(path string) (*Users, error)`. Credentials are derived once at startup via `scram.SHA256.NewClient(user, pass, "")` → `GetStoredCredentials(scram.KeyFactors{Salt: <random 16B base64>, Iters: 15000})`.
  - `type authSession struct{ authenticated bool; user string; conv *scram.ServerConversation; convID int32 }`, one per client connection.
  - `BuildHandshakeResponse` gains `saslMechs []string`. When auth is on and the client handshake contains `saslSupportedMechs: "<db>.<user>"`, the reply has `saslSupportedMechs: ["SCRAM-SHA-256"]` (for unknown users too, so user existence doesn't leak). When auth is off, the field stays empty as today.
- Rules:
  - Before authentication, only these are allowed: the handshake, `saslStart`, `saslContinue`, `ping`, `buildInfo`, `endSessions`.
    - Any other command gets `ProxyError{Code: 13, CodeName: "Unauthorized", Message: "command <name> requires authentication"}`. The connection stays open.
  - `saslStart` with a mechanism other than `SCRAM-SHA-256`, and `authenticate`, get code 18 `AuthenticationFailed`.
  - A wrong password or unknown user gets code 18 `"Authentication failed."`. The log line has the username and the remote address only.
  - The `$db` / authSource of the SASL commands is accepted whatever its value. Users are global.
  - SASL replies are OP_MSG `{conversationId, done, payload: <binary>, ok: 1}`. The final `saslContinue` with an empty payload after the server signature returns `done: true`.
  - The `speculativeAuthenticate` field in the client handshake is ignored (not echoed). Clients then fall back to an explicit `saslStart`.

- [ ] **Step 1: Write failing tests** in `proxy/auth_test.go`.
  - `TestLoadUsers`:
    - `"app:pa:ss\n# c\n\nro:x"` gives 2 users, and `app`'s password is `pa:ss` (verified by a successful SCRAM conversation);
    - a duplicate user gives an error;
    - a line without `:` gives an error.
  - `TestProxyAuthSuccess`:
    - The proxy on port 27026 has users-file `app:s3cret`. A Go-driver client with `Auth{Username:"app", Password:"s3cret", AuthMechanism:"SCRAM-SHA-256"}` runs `InsertOne` and `Find` against the standalone `mongo1`: ok.
    - A client **without** an explicit mechanism also succeeds (negotiated through `saslSupportedMechs`).
  - `TestProxyAuthWrongPassword`: `Ping` returns an error with `Code 18`.
  - `TestProxyAuthRequired`: a client with no credentials runs `Find`, gets `CommandError.Code == 13`, then `Ping` succeeds on the same client (connection not dropped).
  - `TestProxyAuthUpstreamCredsNotAccepted`: the proxy's upstream URI user credentials are rejected by the proxy (code 18), which proves the two sets of credentials are separate.
- [ ] **Step 2: Run, expect FAIL.**
- [ ] **Step 3: Implement** with `scram.SHA256.NewServer(lookup)` → `conv.Step(payload)`, one conversation per `saslStart`. A `conversationId` mismatch gives code 18.
- [ ] **Step 4: Run.** `make test-docker`. Expected: all PASS.
- [ ] **Step 5: Commit.** `feat: inbound SCRAM-SHA-256 auth with proxy-only credentials`

---

### Task 12: Replica-set integration tests (failover, secondaries, cursors, transactions)

**Files:**
- Modify: `proxy/rs_test.go`

**Interfaces:**
- Consumes: everything above; `rsURI(t)` (Task 1). Proxy test ports `27030+`.

Helper: `setupRSProxy(t, port int) *Proxy` builds a proxy for `rsURI(t)` with `serverSelectionTimeoutMS=15000`. The client URI is `mongodb://localhost:<port>/?retryWrites=true`.

- [ ] **Step 1: Write the tests.**

| Test | Steps and assertions |
|---|---|
| `TestRSReadPreferenceRouting` | `RunCommand(db "test", {isMaster: 1})` (forwarded, because it is not on `admin`). With the default read preference: `ismaster == true`. With `readpref.Secondary()`: `secondary == true`. With `readpref.Secondary(readpref.WithTags("nodeType","none"))`: error (no suitable server). |
| `TestRSDbLevelCursorOnSecondary` | With `readpref.Secondary()`, `Database("test").Aggregate([{$documents: [10 docs]}], batchSize 2)`: iterating returns 10 docs with no error. |
| `TestRSFailoverRetryableWrite` | Insert a doc. Using a **direct** RS client, `RunCommand(admin, {replSetStepDown: 10, force: true})` (ignore the network error). Immediately `InsertOne` through the proxy: no error within 20s. `CountDocuments == 2`. |
| `TestRSTransactionAfterFailover` | `session.WithTransaction` inserts 1 doc: ok. Step down as above. A second `WithTransaction` on the **same session**: ok within 30s. Both docs exist. |
| `TestRSKillCursorsEvicts` | Find with batchSize 2 on 10 docs, read 1 batch, then `cursor.Close()`. Within 2s the `cursors` count, exposed for the test via `export_test.go` (`func (m *Mongo) CursorCount() int`), drops back to its value before the find. |

- [ ] **Step 2: Run.** `make test-docker ARGS="-run TestRS -v ./proxy/"`. Expected: all PASS. If any fails, fix the owning task's code in a separate `fix:` commit. Do not weaken the assertion.
- [ ] **Step 3: Commit.** `test: replica-set integration tests for failover, read preference, cursors`

---

### Task 13: PHP / libmongoc smoke test

**Files:**
- Create: `test/php/Dockerfile`, `test/php/smoke.php`, `scripts/php-smoke.sh`
- Modify: `docker-compose.yml` (services `mongobetween`, built from `Dockerfile`; `php-smoke` with profile `php`), `Makefile` (`php-smoke` target)

**Interfaces:**
- Consumes: the image from Task 2. The `mongobetween` service command is `-ping -loglevel info :27016=mongodb://rs1:27017,rs2:27017,rs3:27017/?replicaSet=rs0&serverSelectionTimeoutMS=15000`.

- [ ] **Step 1: Write `test/php/Dockerfile`.** Base `php:8.3-cli`. Run `pecl install mongodb-${EXT_MONGODB_VERSION}` and `docker-php-ext-enable mongodb`. `ARG EXT_MONGODB_VERSION` defaults to the version used by our production PHP images; look it up in a Core service's Dockerfile/`composer.lock` and record the source in a comment.
- [ ] **Step 2: Write `test/php/smoke.php`.**
  - It connects with `new MongoDB\Driver\Manager('mongodb://mongobetween:27016/?retryWrites=true')`, which has **no** credentials, no `tls`, no `replicaSet`.
  - It runs each check, prints `PASS <name>` / `FAIL <name>: <message>`, and exits 1 on any failure. Checks:

| Check | What it does |
|---|---|
| `ping` | `executeCommand('admin', ping)` gives `ok == 1` |
| `insertFind` | `BulkWrite` inserts 10 docs; a `Query` with `batchSize 2` returns 10 (exercises getMore) |
| `secondaryRead` | Query with `new ReadPreference('secondary')` succeeds |
| `dbAggregateSecondary` | `executeReadCommand('test', aggregate $documents, batchSize 2)` on a secondary returns all docs |
| `transaction` | `startSession` → `startTransaction` → insert → `commitTransaction`; the doc is visible |
| `isMasterCommand` | `executeCommand('admin', ['isMaster'=>1])` has `msg == 'isdbgrid'` and `maxWireVersion >= 21` |
| `serverApiPing` | A second Manager with `['serverApi' => new ServerApi('1')]` runs `ping`: ok |
| `earlyCursorDestroy` | Query with batchSize 2, read 1 doc, `unset($cursor)`: no exception |
| `authTls` | A second compose service `mongobetween-secure` runs with `-auth-users-file` + `-tls-cert-file/-tls-key-file`. Manager `mongodb://app:<pw>@mongobetween-secure:27017/?tls=true&tlsCAFile=/certs/ca.pem` pings ok. Without credentials, `find` gets code 13. The test certs are generated at script time and the password comes from an env var. |

- [ ] **Step 3: Write `scripts/php-smoke.sh`.** Bring up the RS (reuse the Task 1 steps), then `docker compose -p mongobetween-test --profile php up --build --exit-code-from php-smoke php-smoke`. Add the Makefile target `php-smoke`.
- [ ] **Step 4: Run.** `make php-smoke`. Expected: 9 `PASS` lines, exit 0.
- [ ] **Step 5: Commit.** `test: add PHP ext-mongodb smoke test through the proxy`

---

### Task 14: Atlas PoC runbook and docs

**Files:**
- Create: `docs/atlas-poc.md`
- Modify: `README.md` (new flags, the PHP URI rules, "replica sets supported"), `.claude/knowledge/07-change-backlog.md` (mark the items done with commit SHAs)

- [ ] **Step 1: Write `docs/atlas-poc.md`** in normal prose, with placeholders only, no real hosts or credentials. It covers:
  1. Start the proxy:
     ```
     MONGO_URI='mongodb+srv://<user>:<pass>@<cluster>/?maxPoolSize=50&minPoolSize=5&serverSelectionTimeoutMS=15000&label=atlas' \
       mongobetween -require-upstream-tls -ping -network tcp4 '127.0.0.1:27016=${MONGO_URI}'
     ```
     Bind to a private IP for the LAN.
  2. The PHP URI rules table from `.claude/knowledge/05-sharded-vs-unsharded.md`, updated after Tasks 3, 10 and 11:
     - `serverApi` is now allowed.
     - Credentials are allowed but must be the **proxy** users (`-auth-users-file`), never the Atlas user.
     - `tls=true&tlsCAFile=…` is allowed when the proxy runs with `-tls-cert-file/-tls-key-file`.
     - Show an example users file and how to supply the PHP password from an env var.
  3. Running `smoke.php` against the PoC proxy.
  4. The manual failover check: Atlas UI "Test Failover" while a PHP loop does inserts and reads; expect no exceptions.
  5. The metrics to watch: `pool.checked_out_connections`, `server_selection`, `handle_message{success:false}`, and the "Proxy error reply" logs.
  6. Sizing: Atlas connections ≈ proxies × members × `maxPoolSize`.
- [ ] **Step 2: Update `README.md` and the knowledge backlog.**
- [ ] **Step 3: Commit.** `docs: Atlas PoC runbook and replica-set usage`

---

## Out of scope (later plans)

- Go driver v2 migration (reference: Jonoans `c753821`; vendor `internal/serverselector`).
- `speculativeAuthenticate` support (one fewer round trip per new client connection).
- `IsWrite` completeness for `DisableWrites`, the legacy OP_UPDATE/OP_DELETE ZERO-field bugs, and replacing go-getter.
