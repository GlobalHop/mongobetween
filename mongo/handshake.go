package mongo

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/description"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/x/mongo/driver/topology"
	"go.mongodb.org/mongo-driver/x/mongo/driver/wiremessage"
)

// https://github.com/mongodb/mongo/blob/ca57a4d640aee04ef373a50b24e79d85f0bb91a0/src/mongo/client/constants.h#L50
const resultFlagAwaitCapable = wiremessage.ReplyFlag(8)

// fallbackMaxWireVersion is reported when no data-bearing upstream member is known.
const fallbackMaxWireVersion int32 = 17

// ReportedMaxWireVersion returns the maxWireVersion to advertise to clients: the override if set, otherwise the
// lowest Max among data-bearing upstream members, capped at the driver's supported maximum.
func ReportedMaxWireVersion(topo description.Topology, override int32) int32 {
	if override > 0 {
		return override
	}
	var lowest int32
	found := false
	for _, s := range topo.Servers {
		switch s.Kind {
		case description.RSPrimary, description.RSSecondary, description.Mongos, description.Standalone:
		default:
			continue
		}
		if s.WireVersion == nil {
			continue
		}
		if !found || s.WireVersion.Max < lowest {
			lowest = s.WireVersion.Max
			found = true
		}
	}
	if !found {
		return fallbackMaxWireVersion
	}
	if max := topology.SupportedWireVersions.Max; lowest > max {
		return max
	}
	return lowest
}

// BuildHandshakeResponse builds a hard-coded handshake response, emulating a mongos, in the same opcode as the request.
func BuildHandshakeResponse(req Operation, topo description.Topology, maxWireOverride int32) (*Message, error) {
	cmd := Hello
	switch r := req.(type) {
	case *opMsg:
		for _, s := range r.sections {
			if single, ok := s.(*opMsgSectionSingle); ok {
				cmd = HandshakeCommandOf(single.msg)
				break
			}
		}
	case *opQuery:
		cmd = HandshakeCommandOf(r.query)
	}
	if cmd == Unknown {
		return nil, fmt.Errorf("not a handshake request")
	}

	doc, err := handshakeDocument(cmd, topo.Kind, ReportedMaxWireVersion(topo, maxWireOverride))
	if err != nil {
		return nil, err
	}

	return encodeDocumentReply(req, doc)
}

func handshakeDocument(cmd Command, kind description.TopologyKind, maxWire int32) (bsoncore.Document, error) {
	primaryKey := "ismaster"
	if cmd == Hello {
		primaryKey = "isWritablePrimary"
	}
	doc := bson.D{{Key: primaryKey, Value: true}}
	if kind != description.Single {
		// IMPORTANT: if not Single, assumes Sharded (mongos persona)!
		doc = append(doc, bson.E{Key: "msg", Value: "isdbgrid"})
	}
	doc = append(doc,
		bson.E{Key: "maxBsonObjectSize", Value: int32(16777216)},
		bson.E{Key: "maxMessageSizeBytes", Value: int32(48000000)},
		bson.E{Key: "maxWriteBatchSize", Value: int32(100000)},
		bson.E{Key: "localTime", Value: primitive.NewDateTimeFromTime(time.Now())},
		bson.E{Key: "logicalSessionTimeoutMinutes", Value: int32(30)},
		bson.E{Key: "maxWireVersion", Value: maxWire},
		bson.E{Key: "minWireVersion", Value: int32(0)},
		bson.E{Key: "readOnly", Value: false},
		bson.E{Key: "ok", Value: 1.0},
	)
	return bson.Marshal(doc)
}
