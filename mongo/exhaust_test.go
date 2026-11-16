package mongo

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/x/mongo/driver/wiremessage"
)

func opMsgWithFlags(t *testing.T, flags wiremessage.MsgFlag, checksum bool) []byte {
	doc, err := bson.Marshal(bson.D{{Key: "find", Value: "c"}, {Key: "$db", Value: "d"}})
	assert.NoError(t, err)
	wm := NewOpMsg(bsoncore.Document(doc), nil).Wm
	if checksum {
		wm = append(wm, 1, 2, 3, 4)
		binary.LittleEndian.PutUint32(wm[0:4], uint32(len(wm)))
	}
	binary.LittleEndian.PutUint32(wm[16:20], uint32(flags))
	return wm
}

func TestStripExhaustAllowed(t *testing.T) {
	t.Run("clears bit", func(t *testing.T) {
		wm := opMsgWithFlags(t, wiremessage.ExhaustAllowed, false)
		orig := append([]byte(nil), wm...)
		out := StripExhaustAllowed(wm)
		assert.Equal(t, orig, wm)
		assert.Len(t, out, len(wm))
		assert.Equal(t, uint32(0), binary.LittleEndian.Uint32(out[16:20]))
		_, err := Decode(out)
		assert.NoError(t, err)
	})
	t.Run("checksum", func(t *testing.T) {
		wm := opMsgWithFlags(t, wiremessage.ExhaustAllowed|wiremessage.ChecksumPresent, true)
		out := StripExhaustAllowed(wm)
		assert.Len(t, out, len(wm)-4)
		assert.Equal(t, uint32(0), binary.LittleEndian.Uint32(out[16:20]))
		assert.Equal(t, uint32(len(out)), binary.LittleEndian.Uint32(out[0:4]))
		_, err := Decode(out)
		assert.NoError(t, err)
	})
	t.Run("untouched", func(t *testing.T) {
		wm := opMsgWithFlags(t, 0, false)
		out := StripExhaustAllowed(wm)
		assert.Same(t, &wm[0], &out[0])
	})
}

func TestHasMoreToCome(t *testing.T) {
	assert.True(t, hasMoreToCome(opMsgWithFlags(t, wiremessage.MoreToCome, false)))
	assert.False(t, hasMoreToCome(opMsgWithFlags(t, 0, false)))
}
