package mongo

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/x/mongo/driver"
)

type mockServer struct {
	i int
}

func (m *mockServer) Connection(context.Context) (driver.Connection, error) {
	return nil, nil
}

func (m *mockServer) MinRTT() time.Duration {
	return time.Duration(0)
}

func (m *mockServer) RTT90() time.Duration {
	return time.Duration(0)
}

func (m *mockServer) RTTMonitor() driver.RTTMonitor {
	return nil
}

func TestCount(t *testing.T) {
	cc := newCursorCache()
	cc.add(10, "db.coll", &mockServer{})
	cc.add(12, "db.coll", &mockServer{})
	cc.add(14, "db.coll", &mockServer{})
	assert.Equal(t, 3, cc.count())

	cc.add(10, "db.coll", &mockServer{10})
	assert.Equal(t, 3, cc.count())

	cc.add(10, "db.coll2", &mockServer{10})
	assert.Equal(t, 4, cc.count())

}

func TestPeek(t *testing.T) {
	cc := newCursorCache()
	cc.add(10, "db.coll", &mockServer{4})
	cc.add(12, "db.coll", &mockServer{5})
	cc.add(14, "db.coll", &mockServer{6})

	s, ok := cc.peek(12, "db.coll")
	assert.True(t, ok)
	assert.Equal(t, s.(*mockServer).i, 5)

	_, ok = cc.peek(13, "db.coll")
	assert.False(t, ok)
}

func TestAdd(t *testing.T) {
	cc := newCursorCache()
	cc.add(10, "db.coll", &mockServer{4})
	cc.add(12, "db.coll", &mockServer{5})
	cc.add(14, "db.coll", &mockServer{6})

	_, ok := cc.peek(13, "db.coll")
	assert.False(t, ok)

	cc.add(13, "db.coll", &mockServer{7})
	_, ok = cc.peek(13, "db.coll")
	assert.True(t, ok)
}

func TestRemove(t *testing.T) {
	cc := newCursorCache()
	cc.add(10, "db.coll", &mockServer{4})
	cc.add(12, "db.coll", &mockServer{5})
	cc.add(14, "db.coll", &mockServer{6})
	assert.Equal(t, 3, cc.count())

	_, ok := cc.peek(12, "db.coll")
	assert.True(t, ok)

	cc.remove(12, "db.coll")
	_, ok = cc.peek(12, "db.coll")
	assert.False(t, ok)
	assert.Equal(t, 2, cc.count())

	cc.remove(13, "db.coll")
	assert.Equal(t, 2, cc.count())
}

func TestNamespaceKeyDistinguishesCollections(t *testing.T) {
	c := newCursorCache()
	s := &mockServer{1}
	c.add(5, "test.$cmd.aggregate", s)
	_, ok := c.peek(5, "test.$cmd.aggregate")
	assert.True(t, ok)
	_, ok = c.peek(5, "")
	assert.False(t, ok)
	c.remove(5, "test.$cmd.aggregate")
	assert.Equal(t, 0, c.count())
}

func cursorReply(t *testing.T, id int64, ns, batchKey string) Operation {
	return msgOp(t, bson.D{
		{Key: "cursor", Value: bson.D{{Key: "id", Value: id}, {Key: "ns", Value: ns}, {Key: batchKey, Value: bson.A{}}}},
		{Key: "ok", Value: 1},
	})
}

func getMoreReq(t *testing.T, id int64, db, coll string) Operation {
	return msgOp(t, bson.D{{Key: "getMore", Value: id}, {Key: "collection", Value: coll}, {Key: "$db", Value: db}})
}

func TestUpdateCursorPins(t *testing.T) {
	const id = int64(42)
	server := &mockServer{1}
	legacyFind := &opQuery{fullCollectionName: "test.coll"}
	tests := []struct {
		name        string
		findReq     Operation
		findReply   Operation
		getMore     Operation
		getMoreDone Operation
	}{
		{
			"find", msgOp(t, bson.D{{Key: "find", Value: "coll"}, {Key: "$db", Value: "test"}}),
			cursorReply(t, id, "test.coll", "firstBatch"), getMoreReq(t, id, "test", "coll"),
			cursorReply(t, 0, "test.coll", "nextBatch"),
		},
		{
			"aggregate on collection", msgOp(t, bson.D{{Key: "aggregate", Value: "coll"}, {Key: "$db", Value: "test"}}),
			cursorReply(t, id, "test.coll", "firstBatch"), getMoreReq(t, id, "test", "coll"),
			cursorReply(t, 0, "test.coll", "nextBatch"),
		},
		{
			"aggregate: 1", msgOp(t, bson.D{{Key: "aggregate", Value: int32(1)}, {Key: "$db", Value: "test"}}),
			cursorReply(t, id, "test.$cmd.aggregate", "firstBatch"), getMoreReq(t, id, "test", "$cmd.aggregate"),
			cursorReply(t, 0, "test.$cmd.aggregate", "nextBatch"),
		},
		{
			"listCollections", msgOp(t, bson.D{{Key: "listCollections", Value: int32(1)}, {Key: "$db", Value: "test"}}),
			cursorReply(t, id, "test.$cmd.listCollections", "firstBatch"), getMoreReq(t, id, "test", "$cmd.listCollections"),
			cursorReply(t, 0, "test.$cmd.listCollections", "nextBatch"),
		},
		{
			"listIndexes", msgOp(t, bson.D{{Key: "listIndexes", Value: "coll"}, {Key: "$db", Value: "test"}}),
			cursorReply(t, id, "test.coll", "firstBatch"), getMoreReq(t, id, "test", "coll"),
			cursorReply(t, 0, "test.coll", "nextBatch"),
		},
		{
			"legacy OP_QUERY find", legacyFind,
			&opReply{cursorID: id}, &opGetMore{fullCollectionName: "test.coll", cursorID: id},
			&opReply{cursorID: 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// the reply (or its request, for legacy) and the getMore agree on the namespace
			ns := tt.findReply.CursorNamespace()
			if ns == "" {
				ns = tt.findReq.CursorNamespace()
			}
			assert.Equal(t, ns, tt.getMore.CursorNamespace())

			cache := newCursorCache()
			updateCursorPins(cache, tt.findReq, tt.findReply, server)
			gid, _ := tt.getMore.CursorID()
			got, ok := cache.peek(gid, tt.getMore.CursorNamespace())
			assert.True(t, ok)
			assert.Equal(t, server, got)

			// exhausted getMore evicts
			updateCursorPins(cache, tt.getMore, tt.getMoreDone, server)
			assert.Equal(t, 0, cache.count())

			// killCursors evicts the case's own pin. Legacy OP_KILL_CURSORS carries no namespace by design, so the
			// namespace-keyed pin cannot be evicted by it and the legacy case is skipped here.
			if _, legacy := tt.findReq.(*opQuery); legacy {
				return
			}
			updateCursorPins(cache, tt.findReq, tt.findReply, server)
			ns = tt.getMore.CursorNamespace()
			_, ok = cache.peek(id, ns)
			assert.True(t, ok)
			db, coll, _ := strings.Cut(ns, ".")
			kill := msgOp(t, bson.D{{Key: "killCursors", Value: coll}, {Key: "cursors", Value: bson.A{id}}, {Key: "$db", Value: db}})
			assert.Equal(t, ns, kill.CursorNamespace())
			updateCursorPins(cache, kill, msgOp(t, bson.D{{Key: "ok", Value: 1}}), server)
			_, ok = cache.peek(id, ns)
			assert.False(t, ok)
			assert.Equal(t, 0, cache.count())
		})
	}
}
