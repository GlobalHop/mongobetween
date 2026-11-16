package mongo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/description"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/x/mongo/driver/wiremessage"
)

func TestHandshakeCommandOf(t *testing.T) {
	tests := []struct {
		name     string
		doc      bson.D
		expected Command
	}{
		{"isMaster int32", bson.D{{Key: "isMaster", Value: int32(1)}}, IsMaster},
		{"ismaster bool", bson.D{{Key: "ismaster", Value: true}}, IsMaster},
		{"isMaster int64", bson.D{{Key: "isMaster", Value: int64(1)}}, IsMaster},
		{"isMaster double", bson.D{{Key: "isMaster", Value: 1.0}}, IsMaster},
		{"hello int32", bson.D{{Key: "hello", Value: int32(1)}}, Hello},
		{"hello bool", bson.D{{Key: "hello", Value: true}}, Hello},
		{"isMaster zero", bson.D{{Key: "isMaster", Value: int32(0)}}, Unknown},
		{"hello false", bson.D{{Key: "hello", Value: false}}, Unknown},
		{"find", bson.D{{Key: "find", Value: "c"}}, Unknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, err := bson.Marshal(tt.doc)
			assert.Nil(t, err)
			assert.Equal(t, tt.expected, HandshakeCommandOf(raw))
		})
	}
}

func serverWithWire(kind description.ServerKind, max int32) description.Server {
	return description.Server{Kind: kind, WireVersion: &description.VersionRange{Min: 0, Max: max}}
}

func TestReportedMaxWireVersion(t *testing.T) {
	t.Run("override", func(t *testing.T) {
		topo := description.Topology{Servers: []description.Server{serverWithWire(description.RSPrimary, 21)}}
		assert.Equal(t, int32(13), ReportedMaxWireVersion(topo, 13))
	})
	t.Run("lowest data-bearing", func(t *testing.T) {
		topo := description.Topology{Servers: []description.Server{
			serverWithWire(description.RSPrimary, 21),
			serverWithWire(description.RSSecondary, 25),
		}}
		assert.Equal(t, int32(21), ReportedMaxWireVersion(topo, 0))
	})
	t.Run("none", func(t *testing.T) {
		topo := description.Topology{Servers: []description.Server{serverWithWire(description.ServerKind(description.Unknown), 25)}}
		assert.Equal(t, int32(17), ReportedMaxWireVersion(topo, 0))
		assert.Equal(t, int32(17), ReportedMaxWireVersion(description.Topology{}, 0))
	})
	t.Run("capped", func(t *testing.T) {
		topo := description.Topology{Servers: []description.Server{serverWithWire(description.RSPrimary, 30)}}
		assert.Equal(t, int32(25), ReportedMaxWireVersion(topo, 0))
	})
}

func handshakeMsgRequest(t *testing.T, cmd string) *opMsg {
	t.Helper()
	doc, err := bson.Marshal(bson.D{{Key: cmd, Value: 1}, {Key: "$db", Value: "admin"}})
	assert.Nil(t, err)
	return &opMsg{reqID: 7, sections: []opMsgSection{&opMsgSectionSingle{msg: doc}}}
}

func rsTopology() description.Topology {
	return description.Topology{
		Kind:    description.ReplicaSetWithPrimary,
		Servers: []description.Server{serverWithWire(description.RSPrimary, 21)},
	}
}

func decodeMsgReply(t *testing.T, m *Message) bsoncore.Document {
	t.Helper()
	op, err := Decode(m.Wm)
	assert.Nil(t, err)
	assert.Equal(t, wiremessage.OpMsg, op.OpCode())
	return op.(*opMsg).sections[0].(*opMsgSectionSingle).msg
}

func assertAbsent(t *testing.T, doc bsoncore.Document, keys ...string) {
	t.Helper()
	for _, k := range keys {
		_, err := doc.LookupErr(k)
		assert.Equal(t, bsoncore.ErrElementNotFound, err, k)
	}
}

func TestHandshakeResponseOpMsgHello(t *testing.T) {
	resp, err := BuildHandshakeResponse(handshakeMsgRequest(t, "hello"), rsTopology(), 0)
	assert.Nil(t, err)
	doc := decodeMsgReply(t, resp)

	assert.True(t, doc.Lookup("isWritablePrimary").Boolean())
	assert.Equal(t, "isdbgrid", doc.Lookup("msg").StringValue())
	assert.Equal(t, int32(21), doc.Lookup("maxWireVersion").Int32())
	assert.Equal(t, int32(0), doc.Lookup("minWireVersion").Int32())
	assert.Equal(t, int32(30), doc.Lookup("logicalSessionTimeoutMinutes").Int32())
	assert.Equal(t, bson.TypeDateTime, doc.Lookup("localTime").Type)
	assertAbsent(t, doc, "ismaster", "helloOk", "topologyVersion", "compression", "setName", "hosts", "speculativeAuthenticate")
}

func TestHandshakeResponseOpMsgIsMaster(t *testing.T) {
	resp, err := BuildHandshakeResponse(handshakeMsgRequest(t, "isMaster"), rsTopology(), 0)
	assert.Nil(t, err)
	doc := decodeMsgReply(t, resp)

	assert.True(t, doc.Lookup("ismaster").Boolean())
	assertAbsent(t, doc, "isWritablePrimary", "helloOk", "topologyVersion", "compression", "setName", "hosts")
}

func TestHandshakeResponseOpQuery(t *testing.T) {
	doc, err := bson.Marshal(bson.D{{Key: "isMaster", Value: 1}})
	assert.Nil(t, err)
	req := &opQuery{reqID: 9, fullCollectionName: "admin.$cmd", query: doc}

	resp, err := BuildHandshakeResponse(req, rsTopology(), 0)
	assert.Nil(t, err)
	op, err := Decode(resp.Wm)
	assert.Nil(t, err)
	assert.Equal(t, wiremessage.OpReply, op.OpCode())
	reply := op.(*opReply)
	assert.Equal(t, 1, len(reply.documents))
	assert.True(t, reply.documents[0].Lookup("ismaster").Boolean())
}

func TestHandshakeResponseSingle(t *testing.T) {
	topo := description.Topology{
		Kind:    description.Single,
		Servers: []description.Server{serverWithWire(description.Standalone, 25)},
	}
	resp, err := BuildHandshakeResponse(handshakeMsgRequest(t, "hello"), topo, 0)
	assert.Nil(t, err)
	doc := decodeMsgReply(t, resp)
	assertAbsent(t, doc, "msg")
	assert.Equal(t, int32(25), doc.Lookup("maxWireVersion").Int32())
}
