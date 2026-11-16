package proxy

import (
	"crypto/tls"
	"encoding/binary"
	"errors"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/coinbase/mongobetween/mongo"
)

func TestReadWireMessageRejectsBadSize(t *testing.T) {
	for _, size := range []uint32{3, 15, mongo.MaxWireMessageSize + 1} {
		client, server := net.Pipe()
		c := &connection{conn: server}
		go func() {
			var b [4]byte
			binary.LittleEndian.PutUint32(b[:], size)
			_, _ = client.Write(b[:])
		}()

		done := make(chan error, 1)
		go func() {
			_, err := c.readWireMessage()
			done <- err
		}()
		select {
		case err := <-done:
			if assert.Error(t, err) {
				assert.Contains(t, err.Error(), "invalid message size")
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("size %#x: readWireMessage did not return", size)
		}
		assert.Less(t, c.buffer.Cap(), 1024)
		_ = client.Close()
		_ = server.Close()
	}
}

// A TLS client on the plain-text port sends a ClientHello (16 03 0x ...), which read as a little-endian int32 is a
// plausible 17-34 MB message size. It must be recognised and closed at once, without allocating that size.
func TestReadWireMessageDetectsTLSClientOnPlainPort(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()

	// a real ClientHello, written by crypto/tls
	go func() {
		_ = tls.Client(client, &tls.Config{ServerName: "proxy.example", MinVersion: tls.VersionTLS12}).Handshake()
	}()

	c := &connection{conn: server}
	done := make(chan error, 1)
	go func() {
		_, err := c.readWireMessage()
		done <- err
	}()
	select {
	case err := <-done:
		assert.True(t, errors.Is(err, errTLSOnPlainPort), "%v", err)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("readWireMessage did not detect the TLS ClientHello within 100ms")
	}
	assert.Less(t, c.buffer.Cap(), 1024)
}

func TestTLSClientOnPlainPortIsLoggedAndClosed(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()

	core, logs := observer.New(zapcore.InfoLevel)
	go func() {
		_ = tls.Client(client, &tls.Config{ServerName: "proxy.example", MinVersion: tls.VersionTLS12}).Handshake()
	}()

	c := &connection{log: zap.New(core), conn: server}
	done := make(chan struct{})
	go func() {
		c.processMessages()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("processMessages did not return")
	}
	assert.Equal(t, 1, logs.FilterMessage("TLS client on plain-text port").Len())
	assert.Equal(t, 0, logs.FilterMessage("Error handling message").Len())
}

// A frame that declares a large size but delivers only a few bytes must not allocate the declared size: memory
// tracks the bytes actually received.
func TestReadWireMessageGrowsBufferIncrementally(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()

	c := &connection{conn: server}
	done := make(chan error, 1)

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	go func() {
		_, err := c.readWireMessage()
		done <- err
	}()

	header := make([]byte, 16)
	binary.LittleEndian.PutUint32(header, 48*1000*1000) // declared 48 MB
	binary.LittleEndian.PutUint32(header[12:], 2013)    // OP_MSG
	_, err := client.Write(append(header, make([]byte, 100)...))
	assert.NoError(t, err)

	select {
	case err := <-done:
		t.Fatalf("readWireMessage returned early: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	_ = client.Close()

	select {
	case err := <-done:
		assert.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("readWireMessage did not return after the client closed")
	}
	runtime.ReadMemStats(&after)

	assert.Less(t, c.buffer.Cap(), 1<<20, "buffer must track received bytes, not the declared size")
	assert.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(8<<20), "declared size must not be allocated")
}

func TestReadWireMessageReadsSplitFrames(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()

	const size = 300 * 1024
	frame := make([]byte, size)
	binary.LittleEndian.PutUint32(frame, size)
	binary.LittleEndian.PutUint32(frame[12:], 2013)
	for i := 16; i < size; i++ {
		frame[i] = byte(i)
	}
	go func() {
		for off := 0; off < size; off += 7000 {
			end := off + 7000
			if end > size {
				end = size
			}
			_, _ = client.Write(frame[off:end])
		}
	}()

	c := &connection{conn: server}
	for i := 0; i < 2; i++ {
		if i == 1 {
			go func() { _, _ = client.Write(frame) }()
		}
		got, err := c.readWireMessage()
		assert.NoError(t, err)
		assert.Equal(t, frame, got)
	}
}
