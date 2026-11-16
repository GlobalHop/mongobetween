package mongo

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/description"
	"go.mongodb.org/mongo-driver/x/mongo/driver/topology"
	"go.mongodb.org/mongo-driver/x/mongo/driver/wiremessage"
)

func errTestMsg(t *testing.T, doc bson.D) *opMsg {
	t.Helper()
	raw, err := bson.Marshal(doc)
	assert.Nil(t, err)
	return &opMsg{reqID: 5, sections: []opMsgSection{&opMsgSectionSingle{msg: raw}}}
}

func lsidDoc() bson.D {
	return bson.D{{Key: "id", Value: primitive.Binary{Subtype: 4, Data: []byte("0123456789abcdef")}}}
}

func TestClassifyWritesDisabled(t *testing.T) {
	req := errTestMsg(t, bson.D{{Key: "insert", Value: "c"}, {Key: "$db", Value: "test"}})
	pe := ClassifyProxyError(ErrWritesDisabled, req)
	assert.Equal(t, int32(13), pe.Code)
	assert.Equal(t, "Unauthorized", pe.CodeName)
	assert.Empty(t, pe.Labels)
}

func TestClassifyUpstreamErrorPlainWrite(t *testing.T) {
	req := errTestMsg(t, bson.D{
		{Key: "insert", Value: "c"}, {Key: "lsid", Value: lsidDoc()},
		{Key: "txnNumber", Value: int64(1)}, {Key: "$db", Value: "test"},
	})
	pe := ClassifyProxyError(errors.New("server selection error"), req)
	assert.Equal(t, int32(11602), pe.Code)
	assert.Equal(t, "InterruptedDueToReplStateChange", pe.CodeName)
	assert.Equal(t, []string{"RetryableWriteError"}, pe.Labels)
}

func TestClassifyUpstreamErrorInTransaction(t *testing.T) {
	req := errTestMsg(t, bson.D{
		{Key: "insert", Value: "c"}, {Key: "lsid", Value: lsidDoc()},
		{Key: "txnNumber", Value: int64(1)}, {Key: "autocommit", Value: false}, {Key: "$db", Value: "test"},
	})
	pe := ClassifyProxyError(errors.New("boom"), req)
	assert.Equal(t, []string{"TransientTransactionError"}, pe.Labels)
}

func TestClassifyUpstreamErrorCommit(t *testing.T) {
	req := errTestMsg(t, bson.D{
		{Key: "commitTransaction", Value: 1}, {Key: "lsid", Value: lsidDoc()},
		{Key: "txnNumber", Value: int64(1)}, {Key: "autocommit", Value: false}, {Key: "$db", Value: "admin"},
	})
	pe := ClassifyProxyError(errors.New("boom"), req)
	assert.Equal(t, []string{"RetryableWriteError", "UnknownTransactionCommitResult"}, pe.Labels)
}

func testProxyError() ProxyError {
	return ProxyError{Code: 11602, CodeName: "InterruptedDueToReplStateChange", Message: "boom", Labels: []string{"RetryableWriteError"}}
}

func assertErrorDoc(t *testing.T, doc bson.Raw) {
	t.Helper()
	assert.Equal(t, 0.0, doc.Lookup("ok").Double())
	assert.Equal(t, int32(11602), doc.Lookup("code").Int32())
	assert.Equal(t, "InterruptedDueToReplStateChange", doc.Lookup("codeName").StringValue())
	assert.Equal(t, "mongobetween: boom", doc.Lookup("errmsg").StringValue())
	labels, err := doc.Lookup("errorLabels").Array().Values()
	assert.Nil(t, err)
	assert.Equal(t, 1, len(labels))
	assert.Equal(t, "RetryableWriteError", labels[0].StringValue())
}

func TestErrorResponseOpMsg(t *testing.T) {
	req := errTestMsg(t, bson.D{{Key: "insert", Value: "c"}, {Key: "$db", Value: "test"}})
	res, err := ErrorResponse(req, testProxyError())
	assert.Nil(t, err)
	op, err := Decode(res.Wm)
	assert.Nil(t, err)
	assert.Equal(t, wiremessage.OpMsg, op.OpCode())
	assertErrorDoc(t, bson.Raw(op.(*opMsg).sections[0].(*opMsgSectionSingle).msg))
}

func TestErrorResponseOpQuery(t *testing.T) {
	q, err := bson.Marshal(bson.D{{Key: "find", Value: "c"}})
	assert.Nil(t, err)
	req := &opQuery{reqID: 9, fullCollectionName: "test.$cmd", query: q}
	res, err := ErrorResponse(req, testProxyError())
	assert.Nil(t, err)
	op, err := Decode(res.Wm)
	assert.Nil(t, err)
	assert.Equal(t, wiremessage.OpReply, op.OpCode())
	reply := op.(*opReply)
	assert.Equal(t, 1, len(reply.documents))
	assertErrorDoc(t, bson.Raw(reply.documents[0]))
}

func TestClassifyInvalidReadPreference(t *testing.T) {
	req := errTestMsg(t, bson.D{
		{Key: "find", Value: "c"}, {Key: "lsid", Value: lsidDoc()},
		{Key: "txnNumber", Value: int64(1)}, {Key: "autocommit", Value: false}, {Key: "$db", Value: "test"},
	})
	pe := ClassifyProxyError(fmt.Errorf("server selection error: %w", ErrInvalidReadPreference), req)
	assert.Equal(t, int32(2), pe.Code)
	assert.Equal(t, "BadValue", pe.CodeName)
	assert.Empty(t, pe.Labels, "an invalid read preference is not retryable")
}

func TestClassifyFailedToSatisfyReadPreference(t *testing.T) {
	req := errTestMsg(t, bson.D{{Key: "find", Value: "c"}, {Key: "$db", Value: "test"}})
	pe := ClassifyProxyError(fmt.Errorf("%w: %w", ErrFailedToSatisfyReadPreference, errors.New("boom")), req)
	assert.Equal(t, int32(133), pe.Code)
	assert.Equal(t, "FailedToSatisfyReadPreference", pe.CodeName)
	assert.Empty(t, pe.Labels, "not retryable")

	txn := errTestMsg(t, bson.D{
		{Key: "find", Value: "c"}, {Key: "lsid", Value: lsidDoc()},
		{Key: "txnNumber", Value: int64(1)}, {Key: "autocommit", Value: false}, {Key: "$db", Value: "test"},
	})
	assert.Empty(t, ClassifyProxyError(ErrFailedToSatisfyReadPreference, txn).Labels)
}

func TestClassifyMessagesNeverLeakCause(t *testing.T) {
	const leak = "Addr: rs1-shard-00-00.abc.mongodb.net:27017"
	req := errTestMsg(t, bson.D{{Key: "find", Value: "c"}, {Key: "$db", Value: "test"}})
	for name, err := range map[string]error{
		"generic":     errors.New(leak),
		"selection":   topology.ServerSelectionError{Wrapped: context.DeadlineExceeded, Desc: description.Topology{}},
		"cancelled":   fmt.Errorf("%s: %w", leak, context.Canceled),
		"unsatisfied": fmt.Errorf("%w: %s", ErrFailedToSatisfyReadPreference, leak),
		"badvalue":    fmt.Errorf("%w: %s", ErrInvalidReadPreference, leak),
		"writes":      fmt.Errorf("%w: %s", ErrWritesDisabled, leak),
	} {
		pe := ClassifyProxyError(err, req)
		assert.NotEmpty(t, pe.Message, name)
		assert.NotContains(t, pe.Message, "Addr", name)
		assert.NotContains(t, pe.Message, "mongodb.net", name)
		msg, mErr := ErrorResponse(req, pe)
		assert.NoError(t, mErr)
		assert.NotContains(t, string(msg.Wm), "mongodb.net", name)
	}
	pe := ClassifyProxyError(errors.New(leak), req)
	assert.Equal(t, int32(11602), pe.Code)
	assert.Equal(t, []string{"RetryableWriteError"}, pe.Labels)
}
