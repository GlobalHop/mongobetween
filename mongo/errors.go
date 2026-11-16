package mongo

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/x/mongo/driver/topology"
)

// ErrWritesDisabled is returned when dynamic configuration disables writes for a proxy address.
var ErrWritesDisabled = errors.New("writes are disabled by mongobetween")

// ErrInvalidReadPreference reports a $readPreference that is present but invalid, or rejected by server selection.
var ErrInvalidReadPreference = errors.New("invalid read preference")

// ErrFailedToSatisfyReadPreference reports that no upstream member matches the request's (non-primary) read
// preference, either detected at once on a stable replica set or after a server selection timeout. Like a real mongos
// it is answered with code 133, which drivers do not retry.
var ErrFailedToSatisfyReadPreference = errors.New("no upstream server matches the read preference")

const (
	codeBadValue                        int32 = 2
	codeUnauthorized                    int32 = 13
	codeFailedToSatisfyReadPreference   int32 = 133
	codeInterruptedDueToReplStateChange int32 = 11602

	// Client-visible messages are fixed: errors from the driver carry the upstream topology and hostnames, which
	// stay in the proxy logs.
	msgWritesDisabled        = "writes are disabled"
	msgInvalidReadPreference = "invalid read preference"
	msgNoMatchingServer      = "no upstream server matches the read preference"
	msgUpstreamUnavailable   = "upstream unavailable (no suitable server; failover in progress?)"
	msgUpstreamCancelled     = "request cancelled"
	msgUpstreamError         = "upstream error"

	labelRetryableWrite       = "RetryableWriteError"
	labelTransientTransaction = "TransientTransactionError"
	labelUnknownCommitResult  = "UnknownTransactionCommitResult"
)

// ProxyError is a proxy-side failure rendered as a MongoDB error reply.
type ProxyError struct {
	Code     int32
	CodeName string
	Message  string
	Labels   []string
}

// ClassifyProxyError maps a proxy-side error to the MongoDB error (code and retry labels) a driver understands.
func ClassifyProxyError(err error, req Operation) ProxyError {
	if errors.Is(err, ErrWritesDisabled) {
		return ProxyError{Code: codeUnauthorized, CodeName: "Unauthorized", Message: msgWritesDisabled}
	}
	if errors.Is(err, ErrInvalidReadPreference) {
		return ProxyError{Code: codeBadValue, CodeName: "BadValue", Message: msgInvalidReadPreference}
	}
	if errors.Is(err, ErrFailedToSatisfyReadPreference) {
		return ProxyError{
			Code:     codeFailedToSatisfyReadPreference,
			CodeName: "FailedToSatisfyReadPreference",
			Message:  msgNoMatchingServer,
		}
	}

	pe := ProxyError{
		Code:     codeInterruptedDueToReplStateChange,
		CodeName: "InterruptedDueToReplStateChange",
		Message:  upstreamErrorMessage(err),
		Labels:   []string{labelRetryableWrite},
	}
	if req == nil || req.TransactionDetails() == nil {
		return pe
	}
	if command, _ := req.CommandAndCollection(); command == CommitTransaction {
		pe.Labels = []string{labelRetryableWrite, labelUnknownCommitResult}
	} else {
		pe.Labels = []string{labelTransientTransaction}
	}
	return pe
}

// upstreamErrorMessage picks the fixed client-visible message for an upstream failure.
func upstreamErrorMessage(err error) string {
	var selErr topology.ServerSelectionError
	switch {
	case errors.Is(err, context.Canceled):
		return msgUpstreamCancelled
	case errors.As(err, &selErr), errors.Is(err, context.DeadlineExceeded):
		return msgUpstreamUnavailable
	}
	return msgUpstreamError
}

// ErrorResponse encodes pe as an error reply in the same opcode as req.
func ErrorResponse(req Operation, pe ProxyError) (*Message, error) {
	doc := bson.D{
		{Key: "ok", Value: 0.0},
		{Key: "errmsg", Value: "mongobetween: " + pe.Message},
		{Key: "code", Value: pe.Code},
		{Key: "codeName", Value: pe.CodeName},
	}
	if len(pe.Labels) > 0 {
		doc = append(doc, bson.E{Key: "errorLabels", Value: pe.Labels})
	}
	raw, err := bson.Marshal(doc)
	if err != nil {
		return nil, err
	}
	return encodeDocumentReply(req, raw)
}

// DocumentResponse encodes doc as a locally generated single-document reply to req, in the same opcode as req.
func DocumentResponse(req Operation, doc bsoncore.Document) (*Message, error) {
	return encodeDocumentReply(req, doc)
}

// NewCommandMessage builds a proxy-originated OP_MSG request (flags 0, request ID 0) carrying doc as its body.
func NewCommandMessage(doc bsoncore.Document) *Message {
	op := opMsg{sections: []opMsgSection{&opMsgSectionSingle{msg: doc}}}
	return &Message{Wm: op.Encode(0), Op: &op}
}

// encodeDocumentReply encodes doc as a single-document reply to req, in the same opcode as req.
func encodeDocumentReply(req Operation, doc bsoncore.Document) (*Message, error) {
	responseTo := req.RequestID()
	switch req.(type) {
	case *opMsg:
		reply := opMsg{sections: []opMsgSection{&opMsgSectionSingle{msg: doc}}}
		return &Message{Wm: reply.Encode(responseTo), Op: &reply}, nil
	case *opQuery:
		reply := opReply{
			flags:       resultFlagAwaitCapable,
			numReturned: 1,
			documents:   []bsoncore.Document{doc},
		}
		return &Message{Wm: reply.Encode(responseTo), Op: &reply}, nil
	}
	return nil, fmt.Errorf("unsupported reply opcode: %v", req.OpCode())
}
