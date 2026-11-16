package proxy

import (
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

type connection struct {
	log    *zap.Logger
	statsd *statsd.Client

	address string
	conn    net.Conn
	kill    chan interface{}
	buffer  []byte

	mongoLookup MongoLookup
	dynamic     *Dynamic
}

func handleConnection(log *zap.Logger, sd *statsd.Client, address string, conn net.Conn, mongoLookup MongoLookup, dynamic *Dynamic, kill chan interface{}) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("Connection crashed", zap.String("panic", fmt.Sprintf("%v", r)), zap.String("stack", string(debug.Stack())))
		}
	}()

	c := connection{
		log:    log,
		statsd: sd,

		address: address,
		conn:    conn,
		kill:    kill,

		mongoLookup: mongoLookup,
		dynamic:     dynamic,
	}
	c.processMessages()
}

func (c *connection) processMessages() {
	for {
		err := c.handleMessage()
		if err != nil {
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
		fmt.Sprintf("command:%s", string(command)),
		fmt.Sprintf("collection:%s", collection),
		fmt.Sprintf("unacknowledged:%v", unacknowledged),
	)
	c.log.Debug(
		"Request",
		zap.Int32("op_code", int32(op.OpCode())),
		zap.Bool("is_master", isMaster),
		zap.String("command", string(command)),
		zap.String("collection", collection),
		zap.Int("request_size", len(wm)),
	)

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
	pe := mongo.ClassifyProxyError(cause, op)
	command, _ := op.CommandAndCollection()
	c.log.Warn("Proxy error reply", zap.Error(cause), zap.Int32("code", pe.Code), zap.String("command", string(command)))
	if unacknowledged {
		return nil
	}
	res, err := mongo.ErrorResponse(op, pe)
	if err != nil {
		return err
	}
	_, err = c.conn.Write(res.Wm)
	return err
}

func (c *connection) readWireMessage() ([]byte, error) {
	var sizeBuf [4]byte

	_, err := io.ReadFull(c.conn, sizeBuf[:])
	if err != nil {
		return nil, err
	}

	// read the length as an int32
	size := (int32(sizeBuf[0])) | (int32(sizeBuf[1]) << 8) | (int32(sizeBuf[2]) << 16) | (int32(sizeBuf[3]) << 24)
	if int(size) > cap(c.buffer) {
		c.buffer = make([]byte, 0, size)
	}

	buffer := c.buffer[:size]
	copy(buffer, sizeBuf[:])

	_, err = io.ReadFull(c.conn, buffer[4:])
	if err != nil {
		return nil, err
	}

	return buffer, nil
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
		return client.HandshakeResponse(msg.Op)
	}

	return client.RoundTrip(msg, tags)
}
