# 04 — Proxy, config, operations

## CLI (`config/config.go:119-131`)

```
mongobetween [OPTIONS] address1=uri1 [address2=uri2] ...
  -network tcp|tcp4(default)|tcp6|unix|unixpacket   (global, cannot mix TCP and unix in one process)
  -username / -password                              (applied only if the URI user/password is empty)
  -statsd localhost:8125   -loglevel info   -pretty   -unlink   -ping
  -dynamic <file|url>      -enable-sdam-metrics       -enable-sdam-logging
```

- Positional args are `listenAddress=uri`. One arg may hold several pairs separated by `|` or a newline.
- `${VAR}` (only that exact form) is expanded in every flag and positional arg (`config.go:210-215`).
  **Use it for secrets**: `-password '${MONGO_PASS}'` keeps the literal placeholder in `ps`/argv instead of the value.
- The custom URI param `label=<name>` adds a `cluster` log field and a `cluster:<label>` statsd tag.
  This applies only to proxy-level metrics; upstream/pool metrics are untagged.
- **No Go driver options are overridden** except auth and the pool/server monitors. Everything else comes from the URI.
  Read preference, retryWrites/retryReads and compressors in the upstream URI have **no effect** on proxied
  traffic, because raw bytes bypass the driver operation layer.

## Auth model

- The proxy authenticates **upstream** with its own credentials.
- **There is no inbound auth.** Anyone who can reach the listener gets the proxy user's privileges. The default
  `tcp4` + `:port` binds all interfaces, so bind to `127.0.0.1` or use a unix socket.
- **App credentials break it.** The fake reply has `saslSupportedMechs: []` and no `speculativeAuthenticate`.
  libmongoc then sends `saslStart`/`saslContinue`, and those are forwarded on *different* pooled, already-authenticated
  connections. The result is "Authentication failed". **The PHP URI must have no credentials.**
- **No TLS on the listener** (plain `net.Listen`). If the client sends TLS, the ClientHello bytes are read as a frame
  length, a big buffer is allocated, and the read blocks. **The PHP URI must not use tls / mongodb+srv.**

## Connection loop (`proxy/connection.go`)

- One goroutine per client, serial request/reply. Panics are recovered ("Connection crashed").
- `readWireMessage` (136-159): **no max size** (a frame up to about 2GB is allocated), a negative or under-4 length
  panics (recovered), and **no read/idle deadlines**. The buffer grows to the largest message seen.
- Any proxy-side error (selection timeout, network, DisableWrites, missing redirect target) **closes the client
  socket**. No MongoDB error document is returned.
- Shutdown on SIGINT/SIGTERM: close the listener, then wait for **all** client connections with no timeout.
  - PHP persistent connections keep it hanging.
  - A second SIGINT forces exit; a second SIGTERM does nothing, so Kubernetes ends it with SIGKILL.
  - Driver clients are never `Close()`d.
- If a listener bind fails, or a recovered panic happens in `proxy.run`, the process may exit with code 0
  (`mongobetween.go:29-44`, `proxy.go:84-102`).
- Unix socket: `syscall.Umask(0)` makes the socket 0777.

## Dynamic config (`proxy/dynamic.go`)

```json
{"Clusters": {"<listen address>": {"DisableWrites": true, "RedirectTo": "<another listen address>"}}}
```

- Fetched with hashicorp go-getter. That library has CVEs at v1.5.9.
- The first fetch is synchronous and fatal on failure. After that it polls every 10s (hard-coded) and keeps the
  last good config.
- The config is read on every message, so it applies immediately.
- A blocked write closes the connection. `RedirectTo` only targets listeners in the same process. Cursors and
  transactions break across a redirect, and so does `$clusterTime` if the target is another cluster.
- The `scratch` Docker image has no `/tmp`, so `-dynamic` fails there unless `TMPDIR`/a volume is set.

## Metrics (statsd, prefix `mongobetween.`)

- **Proxy level (cluster tag):** `open_connections`, `connection_opened`, `connection_closed`, `handle_message`
  (tags: op codes, is_master, command, collection, unacknowledged, success). The `collection` tag has high cardinality.
- **Upstream (no cluster tag):** `round_trip`, `request_size`, `response_size` (tag: address), `server_selection`,
  `checkout_connection`, `cursors`, `transactions`, `pool.open_connections`, `pool.checked_out_connections`,
  `pool_event.*`. SDAM counters appear only with `-enable-sdam-metrics`.
- **Logs (zap JSON):** Accept/Close at info level (noisy with FPM churn); "Round trip error"; "Topology changed"
  (5s poll); "Topology changing error"; debug-level per-request lines. No document contents are logged.

## Build / test / CI

- `make build` (`go build -o bin/mongobetween .`), `make test` (`go test -count 1 -race ./...`), `make lint`.
- `Dockerfile`: `golang:1.17` build stage, then `scratch`. That conflicts with `go.mod` `go 1.18`, so use Go ≥ 1.18.
  The image runs as root and has no shell and no /tmp.
- `docker-compose.yml`: **three standalone `mongo:5.0`** containers (27017-27019). No replica set, no sharding,
  no auth, no TLS.
- Tests use those standalones, so **only the `Single` persona is ever tested**. There is no coverage for the
  sharded persona, replica sets, `hello`, auth, TLS, SRV or libmongoc.
- CI: GitHub Actions with `mongo:5` service containers, gotestsum, golangci-lint, Salus.
- Go is not installed on this dev machine (as of 2026-11-16).
