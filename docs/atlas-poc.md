# Running mongobetween in front of an unsharded Atlas replica set (PoC runbook)

This runbook describes how to run `mongobetween` between a PHP application (ext-mongodb / libmongoc) and an
unsharded MongoDB Atlas replica set. All hosts, users and passwords below are placeholders. Never put a real
credential on a command line, in a committed file, or in a log.

## 1. Start the proxy

The upstream connection string (which contains the Atlas user) is passed through an environment variable and
expanded by the proxy, so it never appears in the process arguments:

```
export MONGO_URI='mongodb+srv://<atlas-user>:<atlas-pass>@<cluster-host>/?maxPoolSize=50&minPoolSize=5&serverSelectionTimeoutMS=15000&label=atlas'

mongobetween -require-upstream-tls -ping -network tcp4 '127.0.0.1:27016=${MONGO_URI}'
```

Quote the address argument with single quotes so your shell does not expand `${MONGO_URI}` itself; the proxy
expands it from its own environment. To serve other hosts on the LAN, bind to a private interface address
instead of `127.0.0.1` (for example `10.0.0.5:27016`) and enable TLS and authentication (sections 2 and 3).
Never bind an unauthenticated listener to a routable address.

Recommended options for the upstream URI (the one the proxy uses to talk to Atlas):

| Option | Suggested value | Why |
|---|---|---|
| `maxPoolSize` | `50` (start point) | Upper bound of connections per Atlas member from this proxy. See sizing in section 6. |
| `minPoolSize` | `5` | Keeps warm connections so the first requests after idle do not pay connection setup. |
| `serverSelectionTimeoutMS` | `15000` | How long the proxy waits for a primary during an election before giving up. Keep it below the PHP request timeout. |
| `retryWrites` | leave at the default (`true`) | The Go driver retries once itself. Clients also retry on the retryable error replies the proxy returns. |
| `appName` | e.g. `mongobetween-poc` | Shows up in Atlas logs and the profiler, which helps tell proxy connections apart. |
| `label` | e.g. `atlas` | Used by the proxy to tag its statsd metrics and logs for this cluster. |

Other relevant flags (verified with `mongobetween -h`):

- `-require-upstream-tls` refuses to start unless every upstream is configured for TLS. `mongodb+srv://` implies
  TLS, so this is a safety net against a mistyped URI.
- `-ping` pings the upstream before listening, so a bad URI or blocked network fails at start-up.
- `-checkout-timeout` (default `10s`) bounds how long a request waits for an upstream connection. On expiry the
  client receives an error reply (see section 5) rather than a closed socket.
- `-max-wire-version` (default `0`) controls the `maxWireVersion` in the handshake reply. `0` mirrors the real
  upstream value, which is what you want. The value is capped at 25 because the bundled Go driver (v1.17.10)
  does not know higher ones.
- `-shutdown-timeout` (default `30s`) is how long a SIGTERM/SIGINT waits for clients to disconnect before
  closing the remaining connections. A second signal exits immediately.
- `-statsd` (default `localhost:8125`) is where metrics are sent.

## 2. How the PHP application connects

The proxy presents itself to the client as a single, always-available `mongos`. The application must therefore
use a URI that points at the proxy, not the Atlas URI. Do not copy the connection string from the Atlas UI.

Example, plain connection on a trusted loopback or private network with authentication enabled:

```php
$uri = 'mongodb://app:' . rawurlencode(getenv('MONGOBETWEEN_PASSWORD'))
     . '@127.0.0.1:27016/?retryWrites=true';
$manager = new MongoDB\Driver\Manager($uri);
```

Example with TLS (proxy started with `-tls-cert-file` and `-tls-key-file`):

```php
$uri = 'mongodb://app:' . rawurlencode(getenv('MONGOBETWEEN_PASSWORD'))
     . '@proxy.internal.example:27016/?tls=true&tlsCAFile=/etc/ssl/internal-ca.pem';
```

URI and driver option rules:

| Option in the app URI / driver options | Rule |
|---|---|
| `mongodb+srv://` | Do not use. The proxy has no SRV record, and SRV implies TLS. Use `mongodb://host:port`. |
| `tls=true` / `ssl=true` | Allowed only when the proxy runs with `-tls-cert-file` and `-tls-key-file`; pair it with `tlsCAFile` for a private CA. Otherwise the proxy closes the connection at once and logs `TLS client on plain-text port`. |
| `tlsCertificateKeyFile` | Required only if the proxy runs with `-tls-ca-file` (mutual TLS). |
| user and password | Allowed, and required when the proxy runs with `-auth-users-file`. These must be the **proxy** users from that file, never the Atlas user. Only `SCRAM-SHA-256` is supported; libmongoc picks it automatically when you do not set `authMechanism`. If auth is not enabled on the proxy, omit credentials. |
| `authSource`, `authMechanism` | Normally omit. If set, the mechanism must be `SCRAM-SHA-256`. |
| `replicaSet=...` | Do not use. libmongoc refuses a `mongos` inside a replica-set topology ("No suitable servers found"). |
| `serverApi` (Stable API) | Allowed. The proxy answers the `hello` handshake locally in the same opcode the client used. |
| `loadBalanced=true` | Do not use. |
| `directConnection=true` | Pointless; avoid it. |
| `readPreference=secondary*` (with optional tags and `maxStalenessSeconds`) | Honoured by the proxy and routed to a matching member. If nothing is specified, reads go to the primary. |
| `retryWrites`, `retryReads`, transactions, sessions | Supported. |
| `compressors=` | Has no effect; compression is never negotiated. |
| Exhaust cursors | Not supported. The proxy strips the `exhaustAllowed` flag so these degrade to normal cursors. |
| Application code that runs `isMaster` through `executeCommand` | Works; the proxy replies in the opcode of the request. |

### The proxy users file

Passing `-auth-users-file /path/to/users.txt` turns on inbound authentication. The file contains one
`user:password` entry per line. Only the first `:` splits the line, so passwords may contain colons. Lines
starting with `#` and blank lines are ignored. The file should be mode `0600`.

```
# proxy users (these are NOT Atlas users)
app:<generated-password-for-app>
reporting:<generated-password-for-reporting>
```

Rules and caveats:

- Proxy users must be different from the Atlas user. They exist only inside the proxy.
- Every proxy user acts upstream with the Atlas user's privileges. The proxy authenticates callers but does not
  restrict what an authenticated caller may do.
- Passwords are stored in the file in clear text, so keep it out of version control and out of images; mount it
  as a secret.
- Before authenticating, a client may only run the handshake, `saslStart`/`saslContinue`, `ping`, `buildInfo`
  and `endSessions`. Everything else gets error code 13 (Unauthorized). `logout` is answered locally.
- Supply the PHP password from the environment (`getenv(...)` as above), not from a literal in source code.

## 3. Smoke test through the proxy

The repository's `make php-smoke` runs the 9-check PHP suite (`test/php/smoke.php`, ext-mongodb 2.3.3) against
the docker test harness, not against Atlas, because it expects the harness hostnames. For the PoC, run a short
script of your own with your PoC URI that does at least: a `ping` on `admin`, an insert with a majority write
concern, a `find` that reads the document back, a find with `readPreference=secondaryPreferred`, and (if you
use them) a multi-document transaction.

```php
$manager = new MongoDB\Driver\Manager($uri);
$manager->executeCommand('admin', new MongoDB\Driver\Command(['ping' => 1]));

$bulk = new MongoDB\Driver\BulkWrite();
$bulk->insert(['poc' => true, 'at' => new MongoDB\BSON\UTCDateTime()]);
$manager->executeBulkWrite('poc.items', $bulk,
    ['writeConcern' => new MongoDB\Driver\WriteConcern('majority')]);
$rows = $manager->executeQuery('poc.items', new MongoDB\Driver\Query(['poc' => true]))->toArray();
```

Use a scratch database (`poc`) so the test data is easy to drop.

## 4. Failover test procedure

1. Start the proxy as in section 1 and watch its logs and your statsd dashboard.
2. Start a PHP loop that, once per iteration, inserts a document with `w=majority`, reads it back, and prints
   any exception with its error code and labels. Run it for a few minutes first to establish a baseline.
3. In the Atlas UI choose the cluster's **Test Failover** (Resilience) action.
4. While the election runs (typically under 30 seconds) observe the loop.

Expected result: no uncaught exceptions. Writes issued during the election may be retried by the driver; reads
may stall for as long as the election takes (bounded by `serverSelectionTimeoutMS` upstream) and then proceed.
If the proxy does reply with an error during the window it is the retryable code 11602 with the
`RetryableWriteError` label, which libmongoc retries once. Afterwards the proxy log shows a
"Proxy error reply" line only if the election outlasted the timeouts. Confirm in Atlas that a different member is
now primary and that the loop's last iteration succeeded. Repeat three times.

If you see failures, record the error code, the `Proxy error reply` log line and whether the failure happened
inside or after the election window, and compare `serverSelectionTimeoutMS` and `-checkout-timeout` to the PHP
request timeout.

## 5. What to watch

Metrics (all prefixed `mongobetween.`; the cluster `label` is added as a tag where available):

- `pool.checked_out_connections` close to `maxPoolSize` for a sustained period means requests are queueing for
  connections. Raise `maxPoolSize` or reduce slow queries.
- `pool.open_connections` shows the actual connection count the proxy holds to Atlas.
- `server_selection` (timing, tagged `success:true|false`). A burst of `success:false` is an election or a
  network problem. Rising timings show selection waiting.
- `handle_message` (timing, tagged `success:true|false`). Failures here are requests the proxy could not
  complete; a few during a failover are expected, a steady stream is not.
- `pool_event.connection_check_out_failed` and `pool_event.connection_pool_cleared` rising during a failover is
  normal; outside a failover it indicates trouble.
- `open_connections` (client side), `cursors` and `transactions` gauges show pinned state in memory.

Log lines (JSON, or readable with `-pretty`):

- `Proxy error reply`, a warning with the underlying `error`, the reply `code` and the `command`. Each one means
  the proxy converted an internal failure (selection or checkout timeout, pool cleared, disabled writes) into a
  MongoDB error reply for the client instead of closing the socket.
- `Error handling message` means a message could not even be answered with an error reply.
- `Authentication failed` (with the `user`) for rejected proxy logins.
- `TLS handshake failed` for bad inbound TLS clients, and `TLS client on plain-text port` for a client that
  speaks TLS to a listener started without `-tls-cert-file`/`-tls-key-file` (the connection is closed at once).
- `Shutdown timeout reached, closing remaining connections` and `Force closed connection` during shutdown.

## 6. Sizing

Each proxy process keeps up to `maxPoolSize` connections to **each** replica-set member it can route to (it
also holds a small monitoring connection per member). The number of connections Atlas sees is therefore roughly:

```
Atlas connections  ~  proxies x members x maxPoolSize
```

For example, 3 proxies against a 3-member replica set with `maxPoolSize=50` can open up to 450 connections. In
practice only the primary receives writes and default reads, so it usually carries about one third of that, but
design for the worst case, particularly when clients use secondary read preferences. Compare the figure with the
connection limit of your Atlas tier and leave headroom for other applications and Atlas itself.

## Known limitations

- Inbound authentication supports only SCRAM-SHA-256.
- All proxy users share the privileges of the Atlas user.
- A `$readPreference` on a write command is used for routing, as on a direct replica-set connection (unlike
  through a real `mongos`, which sends writes to the primary).
- Exhaust cursors are not supported (the flag is stripped).
- `OP_COMPRESSED` is not supported (compression is never negotiated).
- `speculativeAuthenticate` is not supported, which costs one extra round trip for each new client connection.
- The proxy uses Go driver v1; moving to v2 is future work.
