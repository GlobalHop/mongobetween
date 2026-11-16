package proxy

import (
	"bytes"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/x/mongo/driver/wiremessage"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	mongob "github.com/coinbase/mongobetween/mongo"
)

// captureWriter records everything a statsd client sends.
type captureWriter struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *captureWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buf.Write(p)
	w.buf.WriteByte('\n')
	return len(p), nil
}

func (w *captureWriter) SetWriteTimeout(time.Duration) error { return nil }
func (w *captureWriter) Close() error                        { return nil }

// capturingStatsd returns a statsd client and a function that closes it and returns every metric line it sent.
func capturingStatsd(t *testing.T) (*statsd.Client, func() []string) {
	t.Helper()
	w := &captureWriter{}
	sd, err := statsd.NewWithWriter(w, statsd.WithoutTelemetry())
	assert.NoError(t, err)
	return sd, func() []string {
		_ = sd.Close()
		w.mu.Lock()
		defer w.mu.Unlock()
		var lines []string
		for _, l := range strings.Split(w.buf.String(), "\n") {
			if l != "" {
				lines = append(lines, l)
			}
		}
		return lines
	}
}

func metricLines(lines []string, prefix string) []string {
	var out []string
	for _, l := range lines {
		if strings.HasPrefix(l, prefix) {
			out = append(out, l)
		}
	}
	return out
}

func dialEventually(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp4", addr)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	return conn
}

// closedByPeer reports whether conn is closed by the other side within d (true) or stays open (false).
func closedByPeer(conn net.Conn, d time.Duration) bool {
	_ = conn.SetReadDeadline(time.Now().Add(d))
	_, err := conn.Read(make([]byte, 1))
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return false
	}
	return err != nil
}

func TestProxyMaxConnections(t *testing.T) {
	sd, metrics := capturingStatsd(t)
	core, logs := observer.New(zapcore.InfoLevel)
	dynamic, err := NewDynamic("", zap.NewNop())
	assert.NoError(t, err)
	p, err := NewProxy(zap.New(core), sd, "", "tcp4", "127.0.0.1:0", false,
		func(string) *mongob.Mongo { return nil }, dynamic, nil, nil, WithMaxConnections(1))
	assert.NoError(t, err)

	l, err := net.Listen("tcp4", "127.0.0.1:0")
	assert.NoError(t, err)
	accepted := make(chan struct{})
	go func() {
		p.accept(l)
		close(accepted)
	}()
	addr := l.Addr().String()

	first := dialEventually(t, addr)
	assert.False(t, closedByPeer(first, 200*time.Millisecond), "first connection is within the limit")

	second := dialEventually(t, addr)
	assert.True(t, closedByPeer(second, time.Second), "second connection exceeds -max-connections")
	_ = second.Close()

	// closing the first frees the slot
	_ = first.Close()
	time.Sleep(100 * time.Millisecond)
	third := dialEventually(t, addr)
	assert.False(t, closedByPeer(third, 200*time.Millisecond), "slot is freed when a connection closes")
	_ = third.Close()

	p.Shutdown()
	_ = l.Close()
	select {
	case <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("accept did not return")
	}

	assert.Equal(t, 1, logs.FilterMessage("Max connections reached, closing new connection").Len())
	rejected := metricLines(metrics(), "connection_rejected:")
	if assert.Len(t, rejected, 1) {
		assert.Contains(t, rejected[0], "reason:max_connections")
	}
}

func opMsgBytes(t *testing.T, doc bson.D) []byte {
	t.Helper()
	raw, err := bson.Marshal(doc)
	assert.NoError(t, err)
	idx, wm := wiremessage.AppendHeaderStart(nil, 1, 0, wiremessage.OpMsg)
	wm = wiremessage.AppendMsgFlags(wm, 0)
	wm = wiremessage.AppendMsgSectionType(wm, wiremessage.SingleDocument)
	wm = append(wm, raw...)
	return bsoncore.UpdateLength(wm, idx, int32(len(wm[idx:])))
}

// handleOne sends wm to a connection configured by c, lets it handle one message, drains any reply, and returns
// the handle_message metric lines.
func handleOne(t *testing.T, c *connection, wm []byte) []string {
	t.Helper()
	sd, metrics := capturingStatsd(t)
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	dynamic, err := NewDynamic("", zap.NewNop())
	assert.NoError(t, err)
	c.log, c.statsd, c.conn, c.dynamic = zap.NewNop(), sd, server, dynamic
	c.mongoLookup = func(string) *mongob.Mongo { return nil }

	go func() {
		_, _ = client.Write(wm)
		_, _ = io.Copy(io.Discard, client)
	}()
	_ = c.handleMessage()
	_ = server.Close()
	return metricLines(metrics(), "handle_message:")
}

func TestStatsdTagsBeforeAuthentication(t *testing.T) {
	u := loadTestUsers(t)
	find := opMsgBytes(t, bson.D{{Key: "find", Value: "secret_coll"}, {Key: "$db", Value: "test"}})

	lines := handleOne(t, &connection{users: u}, find)
	if assert.Len(t, lines, 1) {
		assert.Contains(t, lines[0], "command:unauthenticated")
		assert.NotContains(t, lines[0], "collection:")
		assert.NotContains(t, lines[0], "secret_coll")
		assert.NotContains(t, lines[0], "command:find")
	}

	for name, c := range map[string]*connection{
		"authenticated": {users: u, auth: authSession{authenticated: true, user: "app"}},
		"auth off":      {},
	} {
		lines := handleOne(t, c, find)
		if assert.Len(t, lines, 1, name) {
			assert.Contains(t, lines[0], "command:find", name)
			assert.Contains(t, lines[0], "collection:secret_coll", name)
		}
	}
}

func TestProxyErrorReplyMetric(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dynamic.json")
	assert.NoError(t, os.WriteFile(path, []byte(`{"Clusters": {"test-addr": {"DisableWrites": true}}}`), 0o600))
	dynamic, err := NewDynamic(path, zap.NewNop())
	assert.NoError(t, err)
	t.Cleanup(dynamic.Close)

	sd, metrics := capturingStatsd(t)
	core, logs := observer.New(zapcore.InfoLevel)
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	c := &connection{log: zap.New(core), statsd: sd, conn: server, address: "test-addr", dynamic: dynamic,
		mongoLookup: func(string) *mongob.Mongo { return nil }}

	insert := opMsgBytes(t, bson.D{{Key: "insert", Value: "c"}, {Key: "$db", Value: "test"}})
	go func() {
		_, _ = client.Write(insert)
		_, _ = io.Copy(io.Discard, client)
	}()
	assert.NoError(t, c.handleMessage())
	_ = server.Close()

	assert.Equal(t, 1, logs.FilterMessage("Proxy error reply").Len())
	lines := metricLines(metrics(), "proxy_error_reply:")
	if assert.Len(t, lines, 1) {
		assert.Contains(t, lines[0], "|c|")
		assert.Contains(t, lines[0], "code:13")
		assert.Contains(t, lines[0], "command:insert")
	}
}
