package proxy

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func loadTestUsers(t *testing.T) *Users {
	t.Helper()
	u, err := LoadUsers(writeUsersFile(t, authTestUsersFile))
	assert.NoError(t, err)
	return u
}

// writeFrameHeader writes a 16-byte OP_MSG header declaring size, followed by n body bytes.
func writeFrameHeader(conn net.Conn, size uint32, n int) {
	header := make([]byte, 16)
	binary.LittleEndian.PutUint32(header, size)
	binary.LittleEndian.PutUint32(header[12:], 2013)
	_, _ = conn.Write(append(header, make([]byte, n)...))
}

func TestPreAuthFrameSizeCap(t *testing.T) {
	u := loadTestUsers(t)

	t.Run("unauthenticated frame over 64 KiB is refused", func(t *testing.T) {
		client, server := net.Pipe()
		defer func() { _ = client.Close() }()
		defer func() { _ = server.Close() }()
		c := &connection{conn: server, users: u}
		go writeFrameHeader(client, maxPreAuthMessageSize+1, 0)

		done := make(chan error, 1)
		go func() {
			_, err := c.readWireMessage()
			done <- err
		}()
		select {
		case err := <-done:
			assert.True(t, errors.Is(err, errPreAuthFrameTooLarge), "%v", err)
		case <-time.After(100 * time.Millisecond):
			t.Fatal("oversized unauthenticated frame was not refused")
		}
	})

	for name, c := range map[string]*connection{
		"authenticated": {users: u, auth: authSession{authenticated: true, user: "app"}},
		"auth off":      {},
	} {
		t.Run(name+" frame over 64 KiB is read", func(t *testing.T) {
			client, server := net.Pipe()
			defer func() { _ = client.Close() }()
			defer func() { _ = server.Close() }()
			c.conn = server
			const size = maxPreAuthMessageSize + 1
			go writeFrameHeader(client, size, size-16)
			wm, err := c.readWireMessage()
			assert.NoError(t, err)
			assert.Len(t, wm, size)
		})
	}
}

func TestUnauthenticatedIdleConnectionIsClosed(t *testing.T) {
	u := loadTestUsers(t)
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()

	core, logs := observer.New(zapcore.InfoLevel)
	c := &connection{log: zap.New(core), conn: server, users: u, authTimeout: 50 * time.Millisecond}
	done := make(chan struct{})
	go func() {
		c.processMessages()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("idle unauthenticated connection was not closed")
	}
	assert.Equal(t, 1, logs.FilterMessage("Closing idle unauthenticated connection").Len())
	assert.Equal(t, 0, logs.FilterMessage("Error handling message").Len())
}

func TestAuthenticatedIdleConnectionStaysOpen(t *testing.T) {
	u := loadTestUsers(t)
	for name, c := range map[string]*connection{
		"authenticated": {users: u, auth: authSession{authenticated: true, user: "app"}, authTimeout: 50 * time.Millisecond},
		"auth off":      {authTimeout: 50 * time.Millisecond},
	} {
		t.Run(name, func(t *testing.T) {
			client, server := net.Pipe()
			c.log = zap.NewNop()
			c.conn = server
			done := make(chan struct{})
			go func() {
				c.processMessages()
				close(done)
			}()
			select {
			case <-done:
				t.Fatal("connection was closed by the auth timeout")
			case <-time.After(200 * time.Millisecond):
			}
			_ = client.Close()
			<-done
		})
	}
}

// The Go driver's monitoring connections never authenticate; with a heartbeat shorter than -auth-timeout they must
// stay open, and authenticated pooled connections must survive idling past it.
func TestProxyAuthTimeoutKeepsMonitoringAndAuthenticatedConnections(t *testing.T) {
	requireDockerHarness(t)
	const port = 27032
	p := startAuthProxy(t, port, authTestUsersFile, options.Client().ApplyURI(upstreamURI(t)), WithAuthTimeout(time.Second))
	defer p.Kill()

	var closed atomic.Int32
	var changes atomic.Int32
	client := connectAuthClient(t, port, options.Client().
		SetAuth(options.Credential{Username: "app", Password: authTestPassword}).
		SetMaxPoolSize(1).
		SetHeartbeatInterval(500*time.Millisecond).
		SetPoolMonitor(&event.PoolMonitor{Event: func(e *event.PoolEvent) {
			if e.Type == event.ConnectionClosed || e.Type == event.PoolCleared {
				closed.Add(1)
			}
		}}).
		SetServerMonitor(&event.ServerMonitor{ServerHeartbeatFailed: func(*event.ServerHeartbeatFailedEvent) {
			changes.Add(1)
		}}))
	defer func() { _ = client.Disconnect(ctx) }()

	coll := client.Database("test").Collection("test_proxy_auth_timeout")
	_, err := coll.CountDocuments(ctx, bson.D{})
	assert.NoError(t, err)

	time.Sleep(2500 * time.Millisecond)

	_, err = coll.CountDocuments(ctx, bson.D{})
	assert.NoError(t, err)
	assert.Equal(t, int32(0), closed.Load(), "authenticated pooled connection must not be closed")
	assert.Equal(t, int32(0), changes.Load(), "monitoring connection must not be closed")

	// a raw connection that never authenticates is closed after the timeout
	raw, err := net.Dial("tcp4", fmt.Sprintf("localhost:%d", port))
	assert.NoError(t, err)
	defer func() { _ = raw.Close() }()
	_ = raw.SetReadDeadline(time.Now().Add(5 * time.Second))
	start := time.Now()
	_, err = raw.Read(make([]byte, 1))
	assert.Error(t, err)
	var ne net.Error
	assert.False(t, errors.As(err, &ne) && ne.Timeout(), "expected the proxy to close the connection, got %v", err)
	assert.True(t, time.Since(start) < 3*time.Second, "closed after %v", time.Since(start))
}
