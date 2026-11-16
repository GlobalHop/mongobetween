package mongo

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/address"
	"go.mongodb.org/mongo-driver/mongo/description"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
)

func TestExtractReadPref_Primary(t *testing.T) {
	doc, err := bson.Marshal(bson.D{
		{Key: "find", Value: "test"},
		{Key: "$db", Value: "testdb"},
		{Key: "$readPreference", Value: bson.D{{Key: "mode", Value: "primary"}}},
	})
	assert.NoError(t, err)

	rp, ok := extractReadPref(doc)
	assert.True(t, ok, "Should successfully extract read preference")
	assert.NotNil(t, rp)
	assert.Equal(t, readpref.PrimaryMode, rp.Mode())
}

func TestExtractReadPref_Secondary(t *testing.T) {
	doc, err := bson.Marshal(bson.D{
		{Key: "find", Value: "test"},
		{Key: "$db", Value: "testdb"},
		{Key: "$readPreference", Value: bson.D{{Key: "mode", Value: "secondary"}}},
	})
	assert.NoError(t, err)

	rp, ok := extractReadPref(doc)
	assert.True(t, ok, "Should successfully extract read preference")
	assert.NotNil(t, rp)
	assert.Equal(t, readpref.SecondaryMode, rp.Mode())
}

func TestExtractReadPref_SecondaryPreferred(t *testing.T) {
	doc, err := bson.Marshal(bson.D{
		{Key: "find", Value: "test"},
		{Key: "$db", Value: "testdb"},
		{Key: "$readPreference", Value: bson.D{{Key: "mode", Value: "secondaryPreferred"}}},
	})
	assert.NoError(t, err)

	rp, ok := extractReadPref(doc)
	assert.True(t, ok, "Should successfully extract read preference")
	assert.NotNil(t, rp)
	assert.Equal(t, readpref.SecondaryPreferredMode, rp.Mode())
}

func TestExtractReadPref_PrimaryPreferred(t *testing.T) {
	doc, err := bson.Marshal(bson.D{
		{Key: "find", Value: "test"},
		{Key: "$db", Value: "testdb"},
		{Key: "$readPreference", Value: bson.D{{Key: "mode", Value: "primaryPreferred"}}},
	})
	assert.NoError(t, err)

	rp, ok := extractReadPref(doc)
	assert.True(t, ok, "Should successfully extract read preference")
	assert.NotNil(t, rp)
	assert.Equal(t, readpref.PrimaryPreferredMode, rp.Mode())
}

func TestExtractReadPref_Nearest(t *testing.T) {
	doc, err := bson.Marshal(bson.D{
		{Key: "find", Value: "test"},
		{Key: "$db", Value: "testdb"},
		{Key: "$readPreference", Value: bson.D{{Key: "mode", Value: "nearest"}}},
	})
	assert.NoError(t, err)

	rp, ok := extractReadPref(doc)
	assert.True(t, ok, "Should successfully extract read preference")
	assert.NotNil(t, rp)
	assert.Equal(t, readpref.NearestMode, rp.Mode())
}

func TestExtractReadPref_NoReadPreference(t *testing.T) {
	doc, err := bson.Marshal(bson.D{
		{Key: "find", Value: "test"},
		{Key: "$db", Value: "testdb"},
	})
	assert.NoError(t, err)

	rp, ok := extractReadPref(doc)
	assert.False(t, ok, "Should return false when no read preference is specified")
	assert.NotNil(t, rp)
	// Should default to primary
	assert.Equal(t, readpref.PrimaryMode, rp.Mode())
}

func TestExtractReadPref_InvalidMode(t *testing.T) {
	doc, err := bson.Marshal(bson.D{
		{Key: "find", Value: "test"},
		{Key: "$db", Value: "testdb"},
		{Key: "$readPreference", Value: bson.D{{Key: "mode", Value: "invalidMode"}}},
	})
	assert.NoError(t, err)

	rp, ok := extractReadPref(doc)
	assert.False(t, ok, "Should return false for invalid read preference mode")
	assert.NotNil(t, rp)
	assert.Equal(t, readpref.PrimaryMode, rp.Mode())
}

func TestExtractReadPref_MalformedDocument(t *testing.T) {
	doc, err := bson.Marshal(bson.D{
		{Key: "find", Value: "test"},
		{Key: "$db", Value: "testdb"},
		{Key: "$readPreference", Value: "notADocument"},
	})
	assert.NoError(t, err)

	rp, ok := extractReadPref(doc)
	assert.False(t, ok, "Should return false for malformed read preference")
	assert.NotNil(t, rp)
	assert.Equal(t, readpref.PrimaryMode, rp.Mode())
}

func TestOpMsg_ReadPref_Primary(t *testing.T) {
	doc, err := bson.Marshal(bson.D{
		{Key: "find", Value: "trainers"},
		{Key: "$db", Value: "test"},
		{Key: "$readPreference", Value: bson.D{{Key: "mode", Value: "primary"}}},
	})
	assert.NoError(t, err)

	op := NewOpMsg(doc, []bsoncore.Document{})

	rp, ok := op.Op.ReadPref()
	assert.True(t, ok)
	assert.Equal(t, readpref.PrimaryMode, rp.Mode())
}

func TestOpMsg_ReadPref_Secondary(t *testing.T) {
	doc, err := bson.Marshal(bson.D{
		{Key: "find", Value: "trainers"},
		{Key: "$db", Value: "test"},
		{Key: "$readPreference", Value: bson.D{{Key: "mode", Value: "secondary"}}},
	})
	assert.NoError(t, err)

	op := NewOpMsg(doc, []bsoncore.Document{})

	rp, ok := op.Op.ReadPref()
	assert.True(t, ok)
	assert.Equal(t, readpref.SecondaryMode, rp.Mode())
}

func TestOpMsg_ReadPref_NoPreference(t *testing.T) {
	doc, err := bson.Marshal(bson.D{
		{Key: "find", Value: "trainers"},
		{Key: "$db", Value: "test"},
	})
	assert.NoError(t, err)

	op := NewOpMsg(doc, []bsoncore.Document{})

	rp, ok := op.Op.ReadPref()
	assert.False(t, ok)
	assert.Equal(t, readpref.PrimaryMode, rp.Mode())
}

func TestOpQuery_ReadPref(t *testing.T) {
	doc, err := bson.Marshal(bson.D{
		{Key: "$query", Value: bson.D{{Key: "name", Value: "test"}}},
		{Key: "$readPreference", Value: bson.D{{Key: "mode", Value: "secondary"}}},
	})
	assert.NoError(t, err)

	op := &opQuery{
		fullCollectionName: "test.trainers",
		query:              doc,
	}

	rp, ok := op.ReadPref()
	assert.True(t, ok)
	assert.Equal(t, readpref.SecondaryMode, rp.Mode())
}

func TestWriteOperations_AlwaysReturnPrimary(t *testing.T) {
	// Test Update
	updateOp := &opUpdate{}
	rp, ok := updateOp.ReadPref()
	assert.False(t, ok)
	assert.Equal(t, readpref.PrimaryMode, rp.Mode())

	// Test Insert
	insertOp := &opInsert{}
	rp, ok = insertOp.ReadPref()
	assert.False(t, ok)
	assert.Equal(t, readpref.PrimaryMode, rp.Mode())

	// Test Delete
	deleteOp := &opDelete{}
	rp, ok = deleteOp.ReadPref()
	assert.False(t, ok)
	assert.Equal(t, readpref.PrimaryMode, rp.Mode())
}

// TestGetMoreOperation_CursorPinning verifies that getMore operations
// don't specify read preferences directly, as they inherit the server
// from the cursor cache. This is the correct behavior since cursors
// must remain pinned to the server where they were created.
func TestGetMoreOperation_CursorPinning(t *testing.T) {
	getMoreOp := &opGetMore{
		cursorID:           12345,
		fullCollectionName: "test.collection",
	}

	// GetMore should not have a read preference since it uses cursor pinning
	rp, ok := getMoreOp.ReadPref()
	assert.False(t, ok, "GetMore should not specify read preference")
	assert.Equal(t, readpref.PrimaryMode, rp.Mode())

	// Verify cursor ID is accessible
	cursorID, cursorOK := getMoreOp.CursorID()
	assert.True(t, cursorOK)
	assert.Equal(t, int64(12345), cursorID)
}

func rpDoc(t *testing.T, rp bson.D) bsoncore.Document {
	t.Helper()
	doc, err := bson.Marshal(bson.D{
		{Key: "find", Value: "test"},
		{Key: "$db", Value: "testdb"},
		{Key: "$readPreference", Value: rp},
	})
	assert.NoError(t, err)
	return doc
}

func TestExtractReadPrefTags(t *testing.T) {
	doc := rpDoc(t, bson.D{
		{Key: "mode", Value: "secondary"},
		{Key: "tags", Value: bson.A{bson.D{{Key: "nodeType", Value: "ANALYTICS"}}, bson.D{}}},
	})

	rp, ok := extractReadPref(doc)
	assert.True(t, ok)
	assert.NotNil(t, rp)
	assert.Equal(t, readpref.SecondaryMode, rp.Mode())
	sets := rp.TagSets()
	assert.Len(t, sets, 2)
	assert.Equal(t, "nodeType", sets[0][0].Name)
	assert.Equal(t, "ANALYTICS", sets[0][0].Value)
	assert.Len(t, sets[1], 0)
}

func TestExtractReadPrefMaxStaleness(t *testing.T) {
	for name, v := range map[string]interface{}{"int32": int32(90), "int64": int64(90), "double": float64(90)} {
		t.Run(name, func(t *testing.T) {
			doc := rpDoc(t, bson.D{{Key: "mode", Value: "secondaryPreferred"}, {Key: "maxStalenessSeconds", Value: v}})
			rp, ok := extractReadPref(doc)
			assert.True(t, ok)
			assert.NotNil(t, rp)
			d, set := rp.MaxStaleness()
			assert.True(t, set)
			assert.Equal(t, 90*time.Second, d)
		})
	}
}

func TestExtractReadPrefPrimaryWithTagsInvalid(t *testing.T) {
	doc := rpDoc(t, bson.D{
		{Key: "mode", Value: "primary"},
		{Key: "tags", Value: bson.A{bson.D{{Key: "a", Value: "b"}}}},
	})
	rp, ok := extractReadPref(doc)
	assert.False(t, ok)
	assert.Nil(t, rp)
}

func TestSelectionReadPref(t *testing.T) {
	msg := func(rp interface{}) Operation {
		d := bson.D{{Key: "find", Value: "c"}, {Key: "$db", Value: "test"}}
		if rp != nil {
			d = append(d, bson.E{Key: "$readPreference", Value: rp})
		}
		doc, err := bson.Marshal(d)
		assert.NoError(t, err)
		return NewOpMsg(doc, nil).Op
	}

	// absent, not a document or an unknown mode: silently primary
	for name, rp := range map[string]interface{}{
		"absent":       nil,
		"not document": "secondary",
		"unknown mode": bson.D{{Key: "mode", Value: "fastest"}},
	} {
		got, err := selectionReadPref(msg(rp))
		assert.NoError(t, err, name)
		if assert.NotNil(t, got, name) {
			assert.Equal(t, readpref.PrimaryMode, got.Mode(), name)
		}
	}

	got, err := selectionReadPref(msg(bson.D{{Key: "mode", Value: "secondary"}}))
	assert.NoError(t, err)
	assert.Equal(t, readpref.SecondaryMode, got.Mode())

	// present but an invalid combination or malformed field
	for name, rp := range map[string]bson.D{
		"primary with tags":         {{Key: "mode", Value: "primary"}, {Key: "tags", Value: bson.A{bson.D{{Key: "a", Value: "b"}}}}},
		"primary with maxStaleness": {{Key: "mode", Value: "primary"}, {Key: "maxStalenessSeconds", Value: int32(120)}},
		"malformed tags":            {{Key: "mode", Value: "secondary"}, {Key: "tags", Value: bson.A{"x"}}},
	} {
		_, err := selectionReadPref(msg(rp))
		assert.True(t, errors.Is(err, ErrInvalidReadPreference), "%s: %v", name, err)
	}
}

func TestReadPrefSelectorRejectsLowMaxStaleness(t *testing.T) {
	rp, err := readpref.New(readpref.SecondaryMode, readpref.WithMaxStaleness(30*time.Second))
	assert.NoError(t, err)
	servers := []description.Server{
		{Addr: address.Address("a:27017"), Kind: description.RSPrimary, WireVersion: &description.VersionRange{Max: 21}, HeartbeatInterval: 10 * time.Second},
		{Addr: address.Address("b:27017"), Kind: description.RSSecondary, WireVersion: &description.VersionRange{Max: 21}, HeartbeatInterval: 10 * time.Second},
	}
	topo := description.Topology{Kind: description.ReplicaSetWithPrimary, Servers: servers}
	_, err = readPrefSelector(rp).SelectServer(topo, servers)
	assert.True(t, errors.Is(err, ErrInvalidReadPreference), "%v", err)

	ok, err := readpref.New(readpref.SecondaryMode, readpref.WithMaxStaleness(120*time.Second))
	assert.NoError(t, err)
	selected, err := readPrefSelector(ok).SelectServer(topo, servers)
	assert.NoError(t, err)
	assert.Len(t, selected, 1)
}

// Writes always go to the primary, as through a real mongos, even when the client attached a $readPreference.
func TestSelectionReadPrefWritesUsePrimary(t *testing.T) {
	for _, cmd := range []bson.E{
		{Key: "insert", Value: "c"},
		{Key: "update", Value: "c"},
		{Key: "delete", Value: "c"},
		{Key: "findAndModify", Value: "c"},
		{Key: "createIndexes", Value: "c"},
	} {
		doc, err := bson.Marshal(bson.D{
			cmd,
			{Key: "$db", Value: "test"},
			{Key: "$readPreference", Value: bson.D{{Key: "mode", Value: "secondary"}}},
		})
		assert.NoError(t, err)
		rp, err := selectionReadPref(NewOpMsg(doc, nil).Op)
		assert.NoError(t, err, cmd.Key)
		if assert.NotNil(t, rp, cmd.Key) {
			assert.Equal(t, readpref.PrimaryMode, rp.Mode(), cmd.Key)
		}
	}

	// reads keep their read preference
	doc, err := bson.Marshal(bson.D{
		{Key: "find", Value: "c"},
		{Key: "$db", Value: "test"},
		{Key: "$readPreference", Value: bson.D{{Key: "mode", Value: "secondary"}}},
	})
	assert.NoError(t, err)
	rp, err := selectionReadPref(NewOpMsg(doc, nil).Op)
	assert.NoError(t, err)
	assert.Equal(t, readpref.SecondaryMode, rp.Mode())
}
