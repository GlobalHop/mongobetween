package proxy

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func rsURI(t *testing.T) string {
	uri := os.Getenv("MONGOBETWEEN_RS_URI")
	if uri == "" {
		t.Skip("MONGOBETWEEN_RS_URI not set")
	}
	return uri
}

func TestRSHarness(t *testing.T) {
	c, err := mongo.Connect(context.Background(), options.Client().ApplyURI(rsURI(t)))
	require.NoError(t, err)
	defer func() { _ = c.Disconnect(context.Background()) }()
	var res bson.M
	require.NoError(t, c.Database("admin").RunCommand(context.Background(), bson.D{{Key: "hello", Value: 1}}).Decode(&res))
	assert.Equal(t, "rs0", res["setName"])
}
