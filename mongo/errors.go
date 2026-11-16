package mongo

import (
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
)

// ErrWritesDisabled is returned when dynamic configuration disables writes for a proxy address.
var ErrWritesDisabled = errors.New("writes are disabled by mongobetween")

const (
	codeUnauthorized                    int32 = 13
	codeInterruptedDueToReplStateChange int32 = 11602

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
		return ProxyError{Code: codeUnauthorized, CodeName: "Unauthorized", Message: err.Error()}
	}

	pe := ProxyError{
		Code:     codeInterruptedDueToReplStateChange,
		CodeName: "InterruptedDueToReplStateChange",
		Message:  err.Error(),
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
