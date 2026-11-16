package mongo_test

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/description"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"

	"github.com/coinbase/mongobetween/mongo"
	"github.com/coinbase/mongobetween/proxy"
)

// blackholeServer answers handshakes as a standalone mongod but never answers any other command, so a round trip
// through it blocks in the upstream read. Every non-handshake request is signalled on requests.
type blackholeServer struct {
	l        net.Listener
	requests chan struct{}

	mu    sync.Mutex
	conns []net.Conn
}

func newBlackholeServer(t *testing.T) *blackholeServer {
	t.Helper()
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	assert.NoError(t, err)
	s := &blackholeServer{l: l, requests: make(chan struct{}, 16)}
	go s.serve()
	t.Cleanup(s.close)
	return s
}

func (s *blackholeServer) addr() string { return s.l.Addr().String() }

func (s *blackholeServer) serve() {
	for {
		c, err := s.l.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.conns = append(s.conns, c)
		s.mu.Unlock()
		go s.handle(c)
	}
}

func (s *blackholeServer) handle(c net.Conn) {
	for {
		var size [4]byte
		if _, err := io.ReadFull(c, size[:]); err != nil {
			return
		}
		wm := make([]byte, binary.LittleEndian.Uint32(size[:]))
		copy(wm, size[:])
		if _, err := io.ReadFull(c, wm[4:]); err != nil {
			return
		}
		op, err := mongo.Decode(wm)
		if err != nil {
			return
		}
		if !op.IsIsMaster() {
			s.requests <- struct{}{}
			continue // never answer
		}
		res, err := mongo.BuildHandshakeResponse(op, description.Topology{Kind: description.Single}, 21, nil)
		if err != nil {
			return
		}
		if _, err := c.Write(res.Wm); err != nil {
			return
		}
	}
}

func (s *blackholeServer) close() {
	_ = s.l.Close()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.conns {
		_ = c.Close()
	}
}

func (s *blackholeServer) awaitRequest(t *testing.T) {
	t.Helper()
	select {
	case <-s.requests:
	case <-time.After(5 * time.Second):
		t.Fatal("upstream did not receive the request")
	}
}

func connectBlackhole(t *testing.T, s *blackholeServer) *mongo.Mongo {
	t.Helper()
	sd, err := statsd.New("localhost:8125")
	assert.NoError(t, err)
	uri := fmt.Sprintf("mongodb://%s/?directConnection=true&serverSelectionTimeoutMS=5000", s.addr())
	m, err := mongo.Connect(zap.NewNop(), sd, options.Client().ApplyURI(uri), mongo.Settings{})
	assert.NoError(t, err)
	// close the server first so a round trip still blocked on it (failed test) cannot block Close
	t.Cleanup(func() {
		s.close()
		m.Close()
	})
	return m
}

func findOpMsg(t *testing.T) *mongo.Message {
	t.Helper()
	find, err := bson.Marshal(bson.D{{Key: "find", Value: "c"}, {Key: "$db", Value: "test"}})
	assert.NoError(t, err)
	return mongo.NewCommandMessage(find)
}

func roundTripAsync(m *mongo.Mongo, msg *mongo.Message) chan error {
	done := make(chan error, 1)
	go func() {
		_, err := m.RoundTrip(msg, nil)
		done <- err
	}()
	return done
}

func assertReturnsPromptly(t *testing.T, done chan error, what string) {
	t.Helper()
	select {
	case err := <-done:
		assert.Error(t, err, what)
	case <-time.After(2 * time.Second):
		t.Fatalf("%s: round trip blocked on the upstream did not return", what)
	}
}

func TestCancelInFlightAbortsBlockedRoundTrip(t *testing.T) {
	s := newBlackholeServer(t)
	m := connectBlackhole(t, s)

	done := roundTripAsync(m, findOpMsg(t))
	s.awaitRequest(t)
	m.CancelInFlight()
	assertReturnsPromptly(t, done, "CancelInFlight")

	// later requests are not pre-cancelled: this one reaches the upstream again
	done = roundTripAsync(m, findOpMsg(t))
	s.awaitRequest(t)
	m.CancelInFlight()
	assertReturnsPromptly(t, done, "second CancelInFlight")
}

func TestCloseAbortsBlockedRoundTrip(t *testing.T) {
	s := newBlackholeServer(t)
	m := connectBlackhole(t, s)

	done := roundTripAsync(m, findOpMsg(t))
	s.awaitRequest(t)

	closed := make(chan struct{})
	go func() {
		m.Close()
		close(closed)
	}()
	assertReturnsPromptly(t, done, "Close")
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return")
	}
}

// Kill (and the shutdown timeout that calls it) must abort upstream reads, or the proxy's Run never returns while a
// request is stuck on an unresponsive upstream.
func TestProxyShutdownTimeoutAbortsBlockedUpstream(t *testing.T) {
	s := newBlackholeServer(t)
	m := connectBlackhole(t, s)

	const port = 27042
	dynamic, err := proxy.NewDynamic("", zap.NewNop())
	assert.NoError(t, err)
	p, err := proxy.NewProxy(zap.NewNop(), nil, "", "tcp4", fmt.Sprintf("127.0.0.1:%d", port), false,
		func(string) *mongo.Mongo { return m }, dynamic, nil, nil)
	assert.NoError(t, err)
	runDone := make(chan error, 1)
	go func() { runDone <- p.Run() }()

	var conn net.Conn
	for i := 0; i < 100; i++ {
		if conn, err = net.Dial("tcp4", fmt.Sprintf("127.0.0.1:%d", port)); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !assert.NoError(t, err) {
		return
	}
	defer func() { _ = conn.Close() }()
	_, err = conn.Write(findOpMsg(t).Wm)
	assert.NoError(t, err)
	s.awaitRequest(t)

	p.ShutdownWithTimeout(100 * time.Millisecond)
	select {
	case err := <-runDone:
		assert.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return: the in-flight upstream read was not aborted")
	}
}
