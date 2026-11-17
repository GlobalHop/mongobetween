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
	"go.mongodb.org/mongo-driver/x/mongo/driver"
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

// fakePoolCleared mimics the driver's unexported topology.poolClearedError: retryable, no Unwrap, and the cause
// only visible in the message text.
type fakePoolCleared struct{ cause error }

func (f fakePoolCleared) Error() string {
	return fmt.Sprintf("connection pool for rs1-shard-00-00.abc.mongodb.net:27017 was cleared because another "+
		"operation failed with: %v", f.cause)
}

func (fakePoolCleared) Retryable() bool { return true }

var stateChangeCases = []struct {
	code    int32
	name    string
	message string
}{
	{91, "ShutdownInProgress", "mongobetween: upstream server shutting down (ShutdownInProgress)"},
	{11600, "InterruptedAtShutdown", "mongobetween: upstream server shutting down (InterruptedAtShutdown)"},
	{11602, "InterruptedDueToReplStateChange", "mongobetween: upstream primary changed (InterruptedDueToReplStateChange)"},
	{10107, "NotWritablePrimary", "mongobetween: upstream primary changed (NotWritablePrimary)"},
	{13435, "NotPrimaryNoSecondaryOk", "mongobetween: upstream primary changed (NotPrimaryNoSecondaryOk)"},
	{13436, "NotPrimaryOrSecondary", "mongobetween: upstream primary changed (NotPrimaryOrSecondary)"},
	{189, "PrimarySteppedDown", "mongobetween: upstream primary changed (PrimarySteppedDown)"},
}

func TestClassifyForwardsStateChangeCodes(t *testing.T) {
	read := errTestMsg(t, bson.D{{Key: "getMore", Value: int64(1)}, {Key: "$db", Value: "test"}})
	txn := errTestMsg(t, bson.D{
		{Key: "find", Value: "c"}, {Key: "lsid", Value: lsidDoc()},
		{Key: "txnNumber", Value: int64(1)}, {Key: "autocommit", Value: false}, {Key: "$db", Value: "test"},
	})
	for _, tc := range stateChangeCases {
		srvErr := driver.Error{Code: tc.code, Name: tc.name, Message: "x rs1-shard-00-00.abc.mongodb.net"}
		chains := map[string]error{
			"direct":      srvErr,
			"wrapped":     fmt.Errorf("op failed: %w", srvErr),
			"netwrapped":  driver.Error{Message: srvErr.Error(), Labels: []string{driver.NetworkError}, Wrapped: srvErr},
			"poolCleared": fakePoolCleared{cause: srvErr},
		}
		for kind, err := range chains {
			pe := ClassifyProxyError(err, read)
			label := fmt.Sprintf("%d/%s", tc.code, kind)
			assert.Equal(t, tc.code, pe.Code, label)
			assert.Equal(t, tc.name, pe.CodeName, label)
			assert.Equal(t, tc.message[len("mongobetween: "):], pe.Message, label)
			assert.NotContains(t, pe.Message, "mongodb.net", label)
			assert.Equal(t, []string{"RetryableWriteError"}, pe.Labels, label)
			assert.Equal(t, []string{"TransientTransactionError"}, ClassifyProxyError(err, txn).Labels, label)
		}
	}
}

func TestClassifyUnrelatedServerCodeStaysGeneric(t *testing.T) {
	req := errTestMsg(t, bson.D{{Key: "find", Value: "c"}, {Key: "$db", Value: "test"}})
	for name, err := range map[string]error{
		"code":        driver.Error{Code: 8000, Name: "AtlasError", Message: "x"},
		"poolCleared": fakePoolCleared{cause: driver.Error{Code: 8000, Name: "AtlasError", Message: "x"}},
		"poolNet":     fakePoolCleared{cause: errors.New("EOF")},
		"nonPoolText": errors.New("was cleared because another operation failed with: (ShutdownInProgress) x"),
	} {
		pe := ClassifyProxyError(err, req)
		assert.Equal(t, int32(11602), pe.Code, name)
		assert.Equal(t, "InterruptedDueToReplStateChange", pe.CodeName, name)
		assert.Equal(t, "upstream error", pe.Message, name)
	}
}

func TestClassifySentinelsBeatStateChangeCode(t *testing.T) {
	req := errTestMsg(t, bson.D{{Key: "find", Value: "c"}, {Key: "$db", Value: "test"}})
	err := fmt.Errorf("%w: %w", ErrFailedToSatisfyReadPreference, driver.Error{Code: 91, Name: "ShutdownInProgress"})
	assert.Equal(t, int32(133), ClassifyProxyError(err, req).Code)
}
