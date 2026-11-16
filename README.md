# mongobetween
`mongobetween` is a lightweight MongoDB connection pooler written in Golang. It's primary function is to handle a large number of incoming connections, and multiplex them across a smaller connection pool to one or more MongoDB clusters.

`mongobetween` is used in production at Coinbase. It is currently deployed as a Docker sidecar alongside a Rails application using the [Ruby Mongo driver](https://github.com/mongodb/mongo-ruby-driver), connecting to a number of sharded MongoDB clusters. It was originally designed to connect to `mongos` routers, which are responsible for server selection for read/write preferences. This fork also supports unsharded replica sets (for example MongoDB Atlas): the proxy honours the client's read preference (mode, tags and `maxStalenessSeconds`; primary by default), returns retryable MongoDB error replies during failovers instead of closing sockets, and can mirror the upstream wire version. It has been verified against a docker replica set and a PHP ext-mongodb client, but not yet against production Atlas traffic; see [docs/atlas-poc.md](docs/atlas-poc.md) for the proof-of-concept runbook.

### How it works
`mongobetween` listens for incoming connections from an application, and proxies any queries to the [MongoDB Go driver](https://github.com/mongodb/mongo-go-driver) which is connected to a MongoDB cluster. It also intercepts any `hello` / `isMaster` handshake from the application (in the opcode the client used), and responds with `"I'm a shard router (mongos)"`, without proxying. The reported `maxWireVersion` mirrors the real upstream unless overridden with `-max-wire-version`. This means `mongobetween` appears to the application as an always-available MongoDB shard router, and any MongoDB connection issues or failovers are handled internally by the Go driver.

### Installation
```
go install github.com/coinbase/mongobetween
```

### Usage
```
Usage: mongobetween [OPTIONS] address1=uri1 [address2=uri2] ...
  -auth-timeout duration
    	With -auth-users-file, close a connection that stays unauthenticated and idle this long (0 = never) (default 30s)
  -auth-users-file string
    	File of proxy-only username:password lines enabling inbound SCRAM-SHA-256 authentication (empty = auth off)
  -checkout-timeout duration
    	Maximum time to wait for an upstream connection checkout (0 = unbounded) (default 10s)
  -dynamic string
    	File or URL to query for dynamic configuration
  -enable-sdam-logging
    	Enable SDAM(Server Discovery And Monitoring) logging
  -enable-sdam-metrics
    	Enable SDAM(Server Discovery And Monitoring) metrics
  -loglevel string
    	One of: debug, info, warn, error, dpanic, panic, fatal (default "info")
  -max-wire-version int
    	maxWireVersion reported to clients in handshake replies (0 = mirror upstream)
  -network string
    	One of: tcp, tcp4, tcp6, unix or unixpacket (default "tcp4")
  -password string
    	MongoDB password
  -ping
    	Ping downstream MongoDB before listening
  -pretty
    	Pretty print logging
  -require-upstream-tls
    	Refuse to start unless every upstream cluster is configured with TLS
  -shutdown-timeout duration
    	Maximum time to wait for client connections to close on shutdown before force closing them (default 30s)
  -statsd string
    	Statsd address (default "localhost:8125")
  -tls-ca-file string
    	CA bundle (PEM); when set, client certificates are required and verified (mTLS)
  -tls-cert-file string
    	Server certificate (PEM) enabling TLS on the client listener
  -tls-key-file string
    	Server private key (PEM) enabling TLS on the client listener
  -unlink
    	Unlink existing unix sockets before listening
  -username string
    	MongoDB username
```

TCP socket example:
```
mongobetween ":27016=mongodb+srv://username:password@cluster.mongodb.net/database?maxpoolsize=10&label=cluster0"
```

Unix socket example:
```
mongobetween -network unix "/tmp/mongo.sock=mongodb+srv://username:password@cluster.mongodb.net/database?maxpoolsize=10&label=cluster0"
```

Proxying multiple clusters:
```
mongobetween -network unix \
  "/tmp/mongo1.sock=mongodb+srv://username:password@cluster1.mongodb.net/database?maxpoolsize=10&label=cluster1" \
  "/tmp/mongo2.sock=mongodb+srv://username:password@cluster2.mongodb.net/database?maxpoolsize=10&label=cluster2"
```

The `label` query parameter in the connection URI is used to any tag statsd metrics or logs for that connection.

### Replica sets and PHP

For an unsharded replica set (such as Atlas), keep the Atlas URI on the proxy side only and point the PHP application at the proxy with a plain `mongodb://host:port` URI. Do not use `mongodb+srv`, `replicaSet=` or `loadBalanced` in the application URI. `tls=true` requires the proxy to run with `-tls-cert-file`/`-tls-key-file`, and credentials in the application URI must be proxy users defined in `-auth-users-file` (never the Atlas user; SCRAM-SHA-256 only). The full URI rules, an example users file, the failover test procedure, metrics to watch and the connection sizing formula are in [docs/atlas-poc.md](docs/atlas-poc.md).

Known limitations: only SCRAM-SHA-256 for inbound auth; all proxy users share the upstream user's privileges; a `$readPreference` on a write command is used for routing; exhaust cursors and `OP_COMPRESSED` are not supported; `speculativeAuthenticate` is not supported (one extra round trip per new connection); the Go driver is v1.

### Dynamic configuration

Passing a file or URL as the `-dynamic` argument will allow somewhat dynamic configuration of `mongobetween`. Example supported file format:
```json
{
  "Clusters": {
    ":12345": {
      "DisableWrites": true,
      "RedirectTo": ""
    },
    "/var/tmp/cluster1.sock": {
      "DisableWrites": false,
      "RedirectTo": "/var/tmp/cluster2.sock"
    }
  }
}
```

This will disable writes to the proxy served from address `:12345`, and redirect any traffic sent to `/var/tmp/cluster1.sock` to the proxy running on `/var/tmp/cluster2.sock`. This is useful for minimal-downtime migrations between clusters.

### TODO

Current known missing features:
 - [X] Transaction server pinning
 - [X] Different cursors on separate servers with the same cursor ID value


### Statsd
`mongobetween` supports reporting health metrics to a local statsd sidecar, using the [Datadog Go library](github.com/DataDog/datadog-go). By default it reports to `localhost:8125`. The following metrics are reported:
 - `mongobetween.handle_message` (Timing) - end-to-end time handling an incoming message from the application
 - `mongobetween.round_trip` (Timing) - round trip time sending a request and receiving a response from MongoDB
 - `mongobetween.request_size` (Distribution) - request size to MongoDB
 - `mongobetween.response_size` (Distribution) - response size from MongoDB
 - `mongobetween.open_connections` (Gauge) - number of open connections between the proxy and the application
 - `mongobetween.connection_opened` (Counter) - connection opened with the application
 - `mongobetween.connection_closed` (Counter) - connection closed with the application
 - `mongobetween.cursors` (Gauge) - number of open cursors being tracked (for cursor -> server mapping)
 - `mongobetween.transactions` (Gauge) - number of transactions being tracked (for client sessions -> server mapping)****
 - `mongobetween.server_selection` (Timing) - Go driver server selection timing
 - `mongobetween.checkout_connection` (Timing) - Go driver connection checkout timing
 - `mongobetween.pool.checked_out_connections` (Gauge) - number of connections checked out from the Go driver connection pool
 - `mongobetween.pool.open_connections` (Gauge) - number of open connections from the Go driver to MongoDB
 - `mongobetween.pool_event.connection_closed` (Counter) - Go driver connection closed
 - `mongobetween.pool_event.connection_pool_created` (Counter) - Go driver connection pool created
 - `mongobetween.pool_event.connection_created` (Counter) - Go driver connection created
 - `mongobetween.pool_event.connection_check_out_failed` (Counter) - Go driver connection check out failed
 - `mongobetween.pool_event.connection_checked_out` (Counter) - Go driver connection checked out
 - `mongobetween.pool_event.connection_checked_in` (Counter) - Go driver connection checked in
 - `mongobetween.pool_event.connection_pool_cleared` (Counter) - Go driver connection pool cleared
 - `mongobetween.pool_event.connection_pool_closed` (Counter) - Go driver connection pool closed

### Testing

- `make test` (alias for `make test-docker`) runs the full suite in a docker harness with a replica set and standalones. It sets `CI=true` and publishes no host ports, so it never touches a MongoDB on host port 27017.
- `make test-unit` runs on the host; tests that need a database are skipped.
- `make test-docker-down` removes the harness volumes. Run it once after upgrading from an older checkout that used `mongo:5.0` volumes.
- `make php-smoke` runs a PHP ext-mongodb 2.3.3 smoke test (9 checks, including TLS and authentication) through the proxy.
- `make lint` runs golangci-lint.

### Background
`mongobetween` was built to address a connection storm issue between a high scale Rails app and MongoDB (see [blog post](https://blog.coinbase.com/scaling-connections-with-ruby-and-mongodb-99204dbf8857)). Due to Ruby MRI's global interpreter lock, multi-threaded web applications don't utilize multiple CPU cores. To achieve better CPU utilization, Puma is run with multiple workers (processes), each of which need a separate MongoDB connection pool. This leads to a large number of connections to MongoDB, sometimes exceeding MongoDB's upstream connection limit of 128k connections.

`mongobetween` has reduced connection counts by an order of magnitude, spikes of up to 30k connections are now reduced to around 2k. It has also significantly reduced `ismaster` commands on the cluster, as there's only a single monitor goroutine per `mongobetween` process, instead of a monitor thread for each Ruby process.
