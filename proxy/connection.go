package proxy

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime/debug"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"go.uber.org/zap"

	"github.com/coinbase/mongobetween/mongo"
)

var errClientNotFound = errors.New("mongo client not found for address")

var errTLSOnPlainPort = errors.New("TLS client on plain-text port")

var errPreAuthFrameTooLarge = errors.New("unauthenticated message too large")

// maxPreAuthMessageSize bounds frames read before a connection authenticates (inbound auth on). Handshake and SASL
// messages are a few hundred bytes.
const maxPreAuthMessageSize = 64 * 1024

type connection struct {
	log    *zap.Logger
	statsd *statsd.Client

	address string
	conn    net.Conn
	kill    chan interface{}
	buffer  bytes.Buffer

	// readFirstFrame is set once the first frame of the connection has started arriving.
	readFirstFrame bool

	mongoLookup MongoLookup
	dynamic     *Dynamic

	// users is nil when inbound authentication is off.
	users *Users
	auth  authSession

	// authTimeout is the idle read deadline of an unauthenticated connection (0 = none); authDeadlineSet records
	// that the deadline is armed on conn.
	authTimeout     time.Duration
	authDeadlineSet bool
}

func handleConnection(p *Proxy, log *zap.Logger, conn net.Conn) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("Connection crashed", zap.String("panic", fmt.Sprintf("%v", r)), zap.String("stack", string(debug.Stack())))
		}
	}()

	c := connection{
		log:    log,
		statsd: p.statsd,

		address: p.address,
		conn:    conn,
		kill:    p.kill,

		mongoLookup: p.mongoLookup,
		dynamic:     p.dynamic,
		users:       p.users,
		authTimeout: p.authTimeout,
	}
	c.processMessages()
}

func (c *connection) processMessages() {
	for {
		err := c.handleMessage()
		if err != nil {
			if errors.Is(err, errTLSOnPlainPort) {
				c.log.Warn("TLS client on plain-text port")
				return
			}
			if errors.Is(err, errPreAuthFrameTooLarge) {
				c.log.Warn("Closing unauthenticated connection", zap.Error(err))
				return
			}
			var ne net.Error
			if c.authDeadlineSet && errors.As(err, &ne) && ne.Timeout() {
				c.log.Info("Closing idle unauthenticated connection", zap.Duration("auth_timeout", c.authTimeout))
				return
			}
			if err != io.EOF {
				select {
				case <-c.kill:
					// ignore errors from force shutdown
				default:
					c.log.Error("Error handling message", zap.Error(err))
				}
			}
			return
		}
	}
}

func (c *connection) handleMessage() (err error) {
	var tags []string

	defer func(start time.Time) {
		tags := append(tags, fmt.Sprintf("success:%v", err == nil))
		_ = c.statsd.Timing("handle_message", time.Since(start), tags, 1)
	}(time.Now())

	var wm []byte
	if wm, err = c.readWireMessage(); err != nil {
		return
	}

	var op mongo.Operation
	if op, err = mongo.Decode(wm); err != nil {
		return
	}

	isMaster := op.IsIsMaster()
	command, collection := op.CommandAndCollection()
	unacknowledged := op.Unacknowledged()
	tags = append(
		tags,
		fmt.Sprintf("request_op_code:%v", op.OpCode()),
		fmt.Sprintf("is_master:%v", isMaster),
	)
	if c.unauthenticated() {
		// client-chosen names must not create statsd tag values before the client has authenticated
		tags = append(tags, "command:unauthenticated")
	} else {
		tags = append(tags, fmt.Sprintf("command:%s", string(command)), fmt.Sprintf("collection:%s", collection))
	}
	tags = append(tags, fmt.Sprintf("unacknowledged:%v", unacknowledged))
	c.log.Debug(
		"Request",
		zap.Int32("op_code", int32(op.OpCode())),
		zap.Bool("is_master", isMaster),
		zap.String("command", string(command)),
		zap.String("collection", collection),
		zap.Int("request_size", len(wm)),
	)

	if res, handled, authErr := c.authorize(op); handled {
		if authErr != nil {
			err = authErr
			return
		}
		if res != nil && !unacknowledged {
			_, err = c.conn.Write(res.Wm)
		}
		return
	}

	req := &mongo.Message{
		Wm: wm,
		Op: op,
	}
	var res *mongo.Message
	var rtErr error
	if res, rtErr = c.roundTrip(req, isMaster, tags); rtErr != nil {
		if errors.Is(rtErr, errClientNotFound) {
			return rtErr
		}
		return c.replyWithError(op, rtErr, unacknowledged)
	}

	if unacknowledged {
		c.log.Debug("Unacknowledged request")
		return
	}

	tags = append(
		tags,
		fmt.Sprintf("response_op_code:%v", res.Op.OpCode()),
	)

	if _, err = c.conn.Write(res.Wm); err != nil {
		return
	}

	c.log.Debug(
		"Response",
		zap.Int32("op_code", int32(res.Op.OpCode())),
		zap.Int("response_size", len(res.Wm)),
	)
	return
}

// replyWithError answers a failed request with a MongoDB error document and keeps the connection open.
// Unacknowledged (moreToCome) requests expect no reply, so the error is only logged.
func (c *connection) replyWithError(op mongo.Operation, cause error, unacknowledged bool) error {
	if unacknowledged {
		c.logProxyError(op, mongo.ClassifyProxyError(cause, op), cause)
		return nil
	}
	res, err := c.proxyErrorReply(op, cause)
	if err != nil {
		return err
	}
	_, err = c.conn.Write(res.Wm)
	return err
}

// proxyErrorReply logs cause and renders it as the MongoDB error reply to op.
func (c *connection) proxyErrorReply(op mongo.Operation, cause error) (*mongo.Message, error) {
	pe := mongo.ClassifyProxyError(cause, op)
	c.logProxyError(op, pe, cause)
	return mongo.ErrorResponse(op, pe)
}

// logProxyError logs a proxy-generated error reply and counts it in the proxy_error_reply metric (tagged with the
// error code and command), the signal to watch during failovers: error replies are success:true in handle_message.
func (c *connection) logProxyError(op mongo.Operation, pe mongo.ProxyError, cause error) {
	command, _ := op.CommandAndCollection()
	c.log.Warn("Proxy error reply", zap.Error(cause), zap.Int32("code", pe.Code), zap.String("command", string(command)))

	commandTag := string(command)
	if c.unauthenticated() {
		commandTag = "unauthenticated"
	}
	_ = c.statsd.Incr("proxy_error_reply", []string{
		fmt.Sprintf("code:%d", pe.Code),
		fmt.Sprintf("command:%s", commandTag),
	}, 1)
}

// headerSize is the size of the standard wire message header (messageLength, requestID, responseTo, opCode).
const headerSize = 16

// readWireMessage reads one wire message. The returned slice aliases the connection buffer and is only valid until
// the next call. The body is read incrementally, so the buffer grows with the bytes actually received rather than
// with the size the frame declares.
func (c *connection) readWireMessage() ([]byte, error) {
	var header [headerSize]byte

	if err := c.updateAuthDeadline(); err != nil {
		return nil, err
	}

	if _, err := io.ReadFull(c.conn, header[:4]); err != nil {
		return nil, err
	}
	firstFrame := !c.readFirstFrame
	c.readFirstFrame = true
	size := int(int32(binary.LittleEndian.Uint32(header[:4])))
	prefix := header[:4]

	if firstFrame && looksLikeTLSRecord(prefix) && !c.isTLS() {
		// A TLS ClientHello reads as a plausible 17-34 MB size. Confirm with the rest of the header (a ClientHello is
		// always longer than 16 bytes, and a message whose size starts with these bytes is at least 790 bytes)
		// before refusing.
		if _, err := io.ReadFull(c.conn, header[4:]); err != nil {
			return nil, err
		}
		if looksLikeTLSClientHello(header[:]) {
			return nil, errTLSOnPlainPort
		}
		prefix = header[:]
	}

	if size < headerSize || size > mongo.MaxWireMessageSize {
		return nil, fmt.Errorf("invalid message size %d", size)
	}
	if c.unauthenticated() && size > maxPreAuthMessageSize {
		return nil, fmt.Errorf("%w: %d bytes", errPreAuthFrameTooLarge, size)
	}

	c.buffer.Reset()
	c.buffer.Write(prefix)
	n, err := io.CopyN(&c.buffer, c.conn, int64(size-len(prefix)))
	if err != nil {
		if errors.Is(err, io.EOF) && n > 0 {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return c.buffer.Bytes(), nil
}

// unauthenticated reports whether inbound auth is on and this connection has not authenticated.
func (c *connection) unauthenticated() bool {
	return c.users != nil && !c.auth.authenticated
}

// updateAuthDeadline arms the idle read deadline before every read while the connection is unauthenticated, and
// clears it once after the connection authenticates.
func (c *connection) updateAuthDeadline() error {
	if c.authTimeout <= 0 {
		return nil
	}
	if c.unauthenticated() {
		c.authDeadlineSet = true
		return c.conn.SetReadDeadline(time.Now().Add(c.authTimeout))
	}
	if c.authDeadlineSet {
		c.authDeadlineSet = false
		return c.conn.SetReadDeadline(time.Time{})
	}
	return nil
}

func (c *connection) isTLS() bool {
	_, ok := c.conn.(*tls.Conn)
	return ok
}

// looksLikeTLSRecord reports whether b starts like a TLS handshake record: content type 0x16 and protocol version
// 3.0 (SSL 3.0) to 3.4.
func looksLikeTLSRecord(b []byte) bool {
	return len(b) >= 3 && b[0] == 0x16 && b[1] == 0x03 && b[2] <= 0x04
}

// looksLikeTLSClientHello reports whether the 16 bytes in h are a TLS record header followed by a ClientHello
// (handshake type 1, client version 3.x). For a wire message these bytes are the requestID and responseTo, and a
// client request always has responseTo 0, so byte 9 can never be 0x03.
func looksLikeTLSClientHello(h []byte) bool {
	return len(h) >= 10 && looksLikeTLSRecord(h) && h[5] == 0x01 && h[9] == 0x03
}

func (c *connection) roundTrip(msg *mongo.Message, isMaster bool, tags []string) (*mongo.Message, error) {
	dynamic := c.dynamic.ForAddress(c.address)
	if dynamic.DisableWrites {
		command, _ := msg.Op.CommandAndCollection()
		if mongo.IsWrite(command) {
			return nil, mongo.ErrWritesDisabled
		}
	}

	redirectTo := dynamic.RedirectTo
	if redirectTo == "" {
		redirectTo = c.address
	}
	client := c.mongoLookup(redirectTo)
	if client == nil {
		return nil, fmt.Errorf("%w: %s", errClientNotFound, c.address)
	}

	if isMaster {
		c.log.Debug("Non-proxied handshake response", zap.Int32("request_id", msg.Op.RequestID()))
		return client.HandshakeResponse(msg.Op, c.saslMechs())
	}

	return client.RoundTrip(msg, tags)
}

// saslMechs is advertised in handshake replies: nil when inbound authentication is off.
func (c *connection) saslMechs() []string {
	if c.users == nil {
		return nil
	}
	return c.users.Mechanisms()
}
