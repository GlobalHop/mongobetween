package proxy

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/coinbase/mongobetween/mongo"
)

func TestReadWireMessageRejectsBadSize(t *testing.T) {
	for _, size := range []uint32{0x16030100, 3, mongo.MaxWireMessageSize + 1} {
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
		assert.Less(t, cap(c.buffer), 1024)
		_ = client.Close()
		_ = server.Close()
	}
}
