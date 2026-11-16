package mongo

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
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
