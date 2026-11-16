package mongo

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"sync"
	"time"
	"unsafe"

	"github.com/DataDog/datadog-go/statsd"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/address"
	"go.mongodb.org/mongo-driver/mongo/description"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/x/mongo/driver"
	"go.mongodb.org/mongo-driver/x/mongo/driver/topology"
	"go.uber.org/zap"
)

const pingTimeout = 60 * time.Second
const disconnectTimeout = 10 * time.Second

type Mongo struct {
	log      *zap.Logger
	statsd   *statsd.Client
	opts     *options.ClientOptions
	settings Settings

	mu           sync.RWMutex
	client       *mongo.Client
	topology     *topology.Topology
	cursors      *cursorCache
	transactions *transactionCache

	// ctxMu guards the round trip context. It is separate from mu so that cancelling never waits for the round
	// trips it is meant to abort (they hold mu.RLock).
	ctxMu           sync.Mutex
	roundTripCtx    context.Context
	roundTripCancel func()
	closing         bool
}

func extractTopology(c *mongo.Client) *topology.Topology {
	e := reflect.ValueOf(c).Elem()
	d := e.FieldByName("deployment")
	d = reflect.NewAt(d.Type(), unsafe.Pointer(d.UnsafeAddr())).Elem() // #nosec G103
	return d.Interface().(*topology.Topology)
}

func Connect(log *zap.Logger, sd *statsd.Client, opts *options.ClientOptions, s Settings) (*Mongo, error) {
	// timeout shouldn't be hit if ping == false, as Connect doesn't block the current goroutine
	ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
	defer cancel()

	var err error
	log.Info("Connect")
	c, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, err
	}

	if s.Ping {
		log.Info("Ping")
		err = c.Ping(ctx, readpref.Primary())
		if err != nil {
			return nil, err
		}
		log.Info("Pong")
	}

	t := extractTopology(c)
	go topologyMonitor(log, t)

	rtCtx, rtCancel := context.WithCancel(context.Background())
	m := Mongo{
		log:             log,
		statsd:          sd,
		opts:            opts,
		settings:        s,
		client:          c,
		topology:        t,
		cursors:         newCursorCache(),
		transactions:    newTransactionCache(),
		roundTripCtx:    rtCtx,
		roundTripCancel: rtCancel,
	}
	go m.cacheMonitor()

	return &m, nil
}

// HandshakeResponse answers a client handshake locally, pretending to be a mongos. saslMechs is advertised in
// saslSupportedMechs when the client asks for it; nil when inbound auth is off.
func (m *Mongo) HandshakeResponse(req Operation, saslMechs []string) (*Message, error) {
	return BuildHandshakeResponse(req, m.Description(), m.settings.MaxWireVersion, saslMechs)
}

func (m *Mongo) Description() description.Topology {
	return m.topology.Description()
}

func (m *Mongo) cacheGauge(name string, count float64) {
	_ = m.statsd.Gauge(name, count, []string{}, 1)
}

func (m *Mongo) cacheMonitor() {
	for {
		m.cacheGauge("cursors", float64(m.cursors.count()))
		m.cacheGauge("transactions", float64(m.transactions.count()))
		time.Sleep(1 * time.Second)
	}
}

// Close aborts in-flight round trips, then disconnects the upstream client.
func (m *Mongo) Close() {
	// cancel before taking the write lock: in-flight round trips hold the read lock until they return
	m.ctxMu.Lock()
	m.closing = true
	m.roundTripCancel()
	m.ctxMu.Unlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.client == nil {
		// already closed
		return
	}

	m.log.Info("Disconnect")
	ctx, cancel := context.WithTimeout(context.Background(), disconnectTimeout)
	defer cancel()
	err := m.client.Disconnect(ctx)
	m.client = nil
	if err != nil {
		m.log.Info("Error disconnecting", zap.Error(err))
	}
}

// CancelInFlight aborts every round trip in progress (server selection, checkout and upstream I/O); an aborted
// upstream connection is discarded. Round trips started afterwards are unaffected unless Close has been called.
func (m *Mongo) CancelInFlight() {
	m.ctxMu.Lock()
	defer m.ctxMu.Unlock()
	m.roundTripCancel()
	if !m.closing {
		m.roundTripCtx, m.roundTripCancel = context.WithCancel(context.Background())
	}
}

// requestContext is the context of a new round trip.
func (m *Mongo) requestContext() context.Context {
	m.ctxMu.Lock()
	defer m.ctxMu.Unlock()
	return m.roundTripCtx
}

func (m *Mongo) RoundTrip(msg *Message, tags []string) (_ *Message, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var addr address.Address
	defer func() {
		if err != nil {
			cursorID, _ := msg.Op.CursorID()
			command, collection := msg.Op.CommandAndCollection()
			m.log.Error(
				"Round trip error",
				zap.Error(err),
				zap.Int64("cursor_id", cursorID),
				zap.Int32("op_code", int32(msg.Op.OpCode())),
				zap.String("address", addr.String()),
				zap.String("command", string(command)),
				zap.String("collection", collection),
			)
		}
	}()

	if m.client == nil {
		return nil, errors.New("connection closed")
	}
	ctx := m.requestContext()

	// CursorID is pinned to a server by CursorID-namespace (db.coll) key
	// Transaction is pinned to a server by the issued lsid
	requestCursorID, _ := msg.Op.CursorID()
	requestCommand, _ := msg.Op.CommandAndCollection()
	requestNS := msg.Op.CursorNamespace()
	killCursorIDs := msg.Op.KillCursorIDs()
	lookupCursorID := requestCursorID
	if lookupCursorID == 0 && len(killCursorIDs) > 0 {
		lookupCursorID = killCursorIDs[0]
	}
	transactionDetails := msg.Op.TransactionDetails()
	readPref, err := selectionReadPref(msg.Op)
	if err != nil {
		return nil, err
	}
	server, err := m.selectServer(ctx, lookupCursorID, requestNS, transactionDetails, readPref)
	if err != nil {
		return nil, err
	}

	conn, err := m.checkoutConnection(ctx, server)
	if err != nil {
		return nil, err
	}

	addr = conn.Address()
	tags = append(
		tags,
		fmt.Sprintf("address:%s", conn.Address().String()),
	)

	defer func() {
		err := conn.Close()
		if err != nil {
			m.log.Error("Error closing Mongo connection", zap.Error(err), zap.String("address", addr.String()))
		}
	}()

	// see https://github.com/mongodb/mongo-go-driver/blob/v1.7.2/x/mongo/driver/operation.go#L430-L432
	ep, ok := server.(driver.ErrorProcessor)
	if !ok {
		return nil, errors.New("server ErrorProcessor type assertion failed")
	}

	unacknowledged := msg.Op.Unacknowledged()
	wm, err := m.roundTrip(ctx, conn, StripExhaustAllowed(msg.Wm), unacknowledged, tags)
	if err != nil {
		m.processError(err, ep, addr, conn)
		if transactionDetails != nil {
			// the pinned server may be gone; don't keep routing this lsid to it
			m.transactions.remove(transactionDetails.LsID)
		}
		return nil, err
	}
	if unacknowledged {
		return &Message{}, nil
	}

	if hasMoreToCome(wm) {
		// the upstream will stream further replies we never read; never let the pool reuse this connection
		m.log.Error("Upstream reply has moreToCome set, discarding connection", zap.String("address", addr.String()))
		m.processError(wrapNetworkError(errors.New("unexpected moreToCome reply")), ep, addr, conn)
		if exp, ok := conn.(driver.Expirable); ok {
			if expErr := exp.Expire(); expErr != nil {
				m.log.Error("Error expiring Mongo connection", zap.Error(expErr), zap.String("address", addr.String()))
			}
		}
		return nil, errors.New("upstream sent a streaming (moreToCome) reply")
	}

	op, err := Decode(wm)
	if err != nil {
		return nil, err
	}

	// check if an error is returned in the server response
	opErr := op.Error()
	if opErr != nil {
		// process the error, but don't return it as we still want to forward the response to the client
		m.processError(opErr, ep, addr, conn)
	}

	updateCursorPins(m.cursors, msg.Op, op, server)

	if transactionDetails != nil {
		if transactionDetails.IsStartTransaction {
			m.transactions.add(transactionDetails.LsID, server)
		} else {
			if requestCommand == AbortTransaction || requestCommand == CommitTransaction {
				m.log.Debug("Removing transaction from the cache", zap.String("reqCommand", string(requestCommand)))
				m.transactions.remove(transactionDetails.LsID)
			}
		}
	}

	return &Message{
		Wm: wm,
		Op: op,
	}, nil
}

func (m *Mongo) selectServer(ctx context.Context, requestCursorID int64, ns string, transDetails *TransactionDetails, rp *readpref.ReadPref) (server driver.Server, err error) {
	defer func(start time.Time) {
		_ = m.statsd.Timing("server_selection", time.Since(start), []string{fmt.Sprintf("success:%v", err == nil)}, 1)
	}(time.Now())

	// Transaction pin (continuing statements only), then cursor pin
	if server, ok := pinnedServer(m.transactions, m.cursors, transDetails, requestCursorID, ns); ok {
		m.log.Debug("Using pinned server", zap.Int64("cursor", requestCursorID), zap.String("ns", ns))
		return server, nil
	}

	// Select a server using the provided read preference
	// If no read preference was provided, default to primary
	if rp == nil {
		rp = readpref.Primary()
	}
	selector := description.CompositeSelector([]description.ServerSelector{
		readPrefSelector(rp), // use client's read preference (ignored by sharded clusters)
		description.LatencySelector(15 * time.Millisecond), // default localThreshold for the client
	})
	// A stable replica set that has no member matching the read preference never will in time: fail at once, as a
	// real mongos does, instead of blocking for the whole server selection timeout.
	if unsatisfiableReadPref(m.topology.Description(), rp) {
		return nil, fmt.Errorf("%w: no member matches %s", ErrFailedToSatisfyReadPreference, rp.String())
	}
	server, err = m.topology.SelectServer(ctx, selector)
	if err != nil {
		return nil, mapSelectionError(err, rp)
	}
	return server, nil
}

// unsatisfiableReadPref reports whether rp, a non-primary read preference, matches no member of a replica set that has
// a primary and no member of unknown state, so that waiting for a topology change cannot help. Sharded, single and
// load-balanced topologies never qualify: the selector ignores rp there.
func unsatisfiableReadPref(topo description.Topology, rp *readpref.ReadPref) bool {
	if rp == nil || rp.Mode() == readpref.PrimaryMode || topo.Kind != description.ReplicaSetWithPrimary {
		return false
	}
	for _, s := range topo.Servers {
		if s.Kind == description.Unknown {
			return false
		}
	}
	candidates, err := readPrefSelector(rp).SelectServer(topo, topo.Servers)
	return err == nil && len(candidates) == 0
}

// mapSelectionError marks a server selection timeout for a non-primary read preference as
// ErrFailedToSatisfyReadPreference. Primary selection timeouts (a failover) and cancellations are left as they are.
func mapSelectionError(err error, rp *readpref.ReadPref) error {
	var selErr topology.ServerSelectionError
	if rp == nil || rp.Mode() == readpref.PrimaryMode || !errors.As(err, &selErr) ||
		errors.Is(err, context.Canceled) || errors.Is(err, ErrInvalidReadPreference) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrFailedToSatisfyReadPreference, err)
}

func (m *Mongo) checkoutConnection(ctx context.Context, server driver.Server) (conn driver.Connection, err error) {
	defer func(start time.Time) {
		addr := ""
		if conn != nil {
			addr = conn.Address().String()
		}
		_ = m.statsd.Timing("checkout_connection", time.Since(start), []string{
			fmt.Sprintf("address:%s", addr),
			fmt.Sprintf("success:%v", err == nil),
		}, 1)
	}(time.Now())

	if m.settings.CheckoutTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, m.settings.CheckoutTimeout)
		defer cancel()
	}

	conn, err = server.Connection(ctx)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

// see https://github.com/mongodb/mongo-go-driver/blob/v1.7.2/x/mongo/driver/operation.go#L664-L681
func (m *Mongo) roundTrip(ctx context.Context, conn driver.Connection, req []byte, unacknowledged bool, tags []string) (res []byte, err error) {
	defer func(start time.Time) {
		tags = append(tags, fmt.Sprintf("success:%v", err == nil))

		_ = m.statsd.Distribution("request_size", float64(len(req)), tags, 1)
		if err == nil && !unacknowledged {
			// There is no response size for unacknowledged writes.
			_ = m.statsd.Distribution("response_size", float64(len(res)), tags, 1)
		}

		_ = m.statsd.Timing("round_trip", time.Since(start), tags, 1)
	}(time.Now())

	if err = conn.WriteWireMessage(ctx, req); err != nil {
		return nil, wrapNetworkError(err)
	}

	if unacknowledged {
		return nil, nil
	}

	if res, err = conn.ReadWireMessage(ctx); err != nil {
		return nil, wrapNetworkError(err)
	}

	return res, nil
}

func wrapNetworkError(err error) error {
	labels := []string{driver.NetworkError}
	return driver.Error{Message: err.Error(), Labels: labels, Wrapped: err}
}

// Process the error with the given ErrorProcessor, returning true if processing causes the topology to change
func (m *Mongo) processError(err error, ep driver.ErrorProcessor, addr address.Address, conn driver.Connection) {
	last := m.Description()

	// gather fields for logging
	fields := []zap.Field{
		zap.String("address", addr.String()),
		zap.Error(err),
	}
	if derr, ok := err.(driver.Error); ok {
		fields = append(fields, zap.Int32("error_code", derr.Code))
		fields = append(fields, zap.Strings("error_labels", derr.Labels))
		fields = append(fields, zap.NamedError("error_wrapped", derr.Wrapped))
	}
	if werr, ok := err.(driver.WriteConcernError); ok {
		fields = append(fields, zap.Int64("error_code", werr.Code))
	}

	// process the error
	ep.ProcessError(err, conn)

	// log if the error changed the topology
	if errorChangesTopology(err) {
		desc := m.Description()

		fields = append(fields, topologyChangedFields(&last, &desc)...)
		m.log.Error("Topology changing error", fields...)
	}
}

// see https://github.com/mongodb/mongo-go-driver/blob/v1.7.2/x/mongo/driver/topology/server.go#L432-L505
func errorChangesTopology(err error) bool {
	if cerr, ok := err.(driver.Error); ok && (cerr.NodeIsRecovering() || cerr.NotPrimary()) {
		return true
	}
	if wcerr, ok := err.(driver.WriteConcernError); ok && (wcerr.NodeIsRecovering() || wcerr.NotPrimary()) {
		return true
	}

	wrappedConnErr := unwrapConnectionError(err)
	if wrappedConnErr == nil {
		return false
	}

	// Ignore transient timeout errors.
	if netErr, ok := wrappedConnErr.(net.Error); ok && netErr.Timeout() {
		return false
	}
	if wrappedConnErr == context.Canceled || wrappedConnErr == context.DeadlineExceeded {
		return false
	}

	return true
}

// see https://github.com/mongodb/mongo-go-driver/blob/v1.7.2/x/mongo/driver/topology/server.go#L949-L969
func unwrapConnectionError(err error) error {
	connErr, ok := err.(topology.ConnectionError)
	if ok {
		return connErr.Wrapped
	}

	driverErr, ok := err.(driver.Error)
	if !ok || !driverErr.NetworkError() {
		return nil
	}

	connErr, ok = driverErr.Wrapped.(topology.ConnectionError)
	if ok {
		return connErr.Wrapped
	}

	return nil
}

// CursorCount returns the number of cursors currently pinned to a server (for tests and diagnostics).
func (m *Mongo) CursorCount() int {
	return m.cursors.count()
}

// TransactionCount returns the number of transactions currently pinned to a server (for tests and diagnostics).
func (m *Mongo) TransactionCount() int {
	return m.transactions.count()
}

// selectionReadPref is the read preference a request is routed with. An absent $readPreference, one that is not a
// document, or one with an unknown mode falls back to primary; one that is present but invalid (bad tags or
// maxStalenessSeconds, or a mode that does not allow them) is an ErrInvalidReadPreference. Write commands always use
// primary, as through a real mongos, whatever $readPreference they carry.
func selectionReadPref(op Operation) (*readpref.ReadPref, error) {
	if command, _ := op.CommandAndCollection(); IsWrite(command) {
		return readpref.Primary(), nil
	}
	rp, ok := op.ReadPref()
	if rp == nil {
		if !ok {
			return nil, fmt.Errorf("%w: unsupported $readPreference", ErrInvalidReadPreference)
		}
		return readpref.Primary(), nil
	}
	return rp, nil
}

// readPrefSelector is description.ReadPrefSelector with its errors (e.g. maxStalenessSeconds below 90s or below
// heartbeat + idle write period) marked as ErrInvalidReadPreference. Such an error ends server selection at once.
func readPrefSelector(rp *readpref.ReadPref) description.ServerSelector {
	selector := description.ReadPrefSelector(rp)
	return description.ServerSelectorFunc(func(t description.Topology, candidates []description.Server) ([]description.Server, error) {
		servers, err := selector.SelectServer(t, candidates)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidReadPreference, err)
		}
		return servers, nil
	})
}
