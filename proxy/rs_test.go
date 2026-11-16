package proxy

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.uber.org/zap"

	mongob "github.com/coinbase/mongobetween/mongo"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func rsURI(t *testing.T) string {
	requireDockerHarness(t)
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

// rsDirectClient connects directly to the replica set (bypassing the proxy).
func rsDirectClient(t *testing.T) *mongo.Client {
	t.Helper()
	c, err := mongo.Connect(ctx, options.Client().ApplyURI(rsURI(t)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Disconnect(ctx) })
	return c
}

// waitForPrimary polls hello on the direct client until a writable primary is found (max 30s).
func waitForPrimary(t *testing.T, direct *mongo.Client) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var res bson.M
		c, cancel := context.WithTimeout(ctx, 3*time.Second)
		err := direct.Database("admin").RunCommand(c, bson.D{{Key: "hello", Value: 1}}).Decode(&res)
		cancel()
		if err == nil && res["isWritablePrimary"] == true {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatal("no primary elected within 30s")
}

// stepDown forces the current primary to step down; the resulting network error is ignored.
func stepDown(t *testing.T, direct *mongo.Client) {
	t.Helper()
	c, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_ = direct.Database("admin").RunCommand(c, bson.D{
		{Key: "replSetStepDown", Value: 10},
		{Key: "force", Value: true},
	}).Err()
}

// setupRSProxy starts a proxy in front of the replica set and returns it with its upstream.
func setupRSProxy(t *testing.T, port int) (*Proxy, *mongob.Mongo) {
	t.Helper()
	uri := rsURI(t)
	sep := "?"
	if strings.Contains(uri, "?") {
		sep = "&"
	}
	uri += sep + "serverSelectionTimeoutMS=15000"

	sd, err := statsd.New("localhost:8125")
	require.NoError(t, err)
	upstream, err := mongob.Connect(zap.L(), sd, options.Client().ApplyURI(uri),
		mongob.Settings{CheckoutTimeout: 20 * time.Second})
	require.NoError(t, err)

	dynamic, err := NewDynamic("", zap.L())
	require.NoError(t, err)
	address := fmt.Sprintf(":%d", port)
	proxy, err := NewProxy(zap.L(), sd, "label", "tcp4", address, false,
		func(string) *mongob.Mongo { return upstream }, dynamic, nil, nil)
	require.NoError(t, err)
	go func() { _ = proxy.Run() }()
	t.Cleanup(proxy.Shutdown)
	return proxy, upstream
}

func rsClient(t *testing.T, port int, opts ...*options.ClientOptions) *mongo.Client {
	t.Helper()
	all := append([]*options.ClientOptions{
		options.Client().ApplyURI(fmt.Sprintf("mongodb://localhost:%d/?retryWrites=true", port)),
	}, opts...)
	c, err := mongo.Connect(ctx, all...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Disconnect(ctx) })
	return c
}

func TestRSReadPreferenceRouting(t *testing.T) {
	port := 27030
	waitForPrimary(t, rsDirectClient(t))
	setupRSProxy(t, port)
	client := rsClient(t, port)

	isMaster := func(rp *readpref.ReadPref) (bson.M, error) {
		var res bson.M
		c, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		err := client.Database("test").RunCommand(c, bson.D{{Key: "isMaster", Value: 1}},
			options.RunCmd().SetReadPreference(rp)).Decode(&res)
		return res, err
	}

	res, err := isMaster(readpref.Primary())
	require.NoError(t, err)
	assert.Equal(t, true, res["ismaster"])

	res, err = isMaster(readpref.Secondary())
	require.NoError(t, err)
	assert.Equal(t, true, res["secondary"])

	_, err = isMaster(readpref.Secondary(readpref.WithTags("nodeType", "none")))
	assert.Error(t, err)
}

func TestRSDbLevelCursorOnSecondary(t *testing.T) {
	port := 27031
	waitForPrimary(t, rsDirectClient(t))
	setupRSProxy(t, port)
	client := rsClient(t, port)

	db := client.Database("test", options.Database().SetReadPreference(readpref.Secondary()))
	docs := bson.A{}
	for i := 0; i < 10; i++ {
		docs = append(docs, bson.D{{Key: "i", Value: i}})
	}
	cur, err := db.Aggregate(ctx, mongo.Pipeline{{{Key: "$documents", Value: docs}}},
		options.Aggregate().SetBatchSize(2))
	require.NoError(t, err)
	n := 0
	for cur.Next(ctx) {
		n++
	}
	require.NoError(t, cur.Err())
	assert.Equal(t, 10, n)
}

func TestRSFailoverRetryableWrite(t *testing.T) {
	port := 27032
	direct := rsDirectClient(t)
	waitForPrimary(t, direct)
	setupRSProxy(t, port)
	client := rsClient(t, port)
	coll := client.Database("test").Collection("rs_failover")
	_, err := coll.DeleteMany(ctx, bson.D{})
	require.NoError(t, err)

	_, err = coll.InsertOne(ctx, bson.D{{Key: "n", Value: 1}})
	require.NoError(t, err)

	stepDown(t, direct)

	wctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	_, err = coll.InsertOne(wctx, bson.D{{Key: "n", Value: 2}})
	require.NoError(t, err)

	count, err := coll.CountDocuments(wctx, bson.D{})
	require.NoError(t, err)
	assert.Equal(t, int64(2), count)
}

func TestRSTransactionAfterFailover(t *testing.T) {
	port := 27033
	direct := rsDirectClient(t)
	waitForPrimary(t, direct)
	setupRSProxy(t, port)
	client := rsClient(t, port)
	coll := client.Database("test").Collection("rs_txn_failover")
	_, err := coll.DeleteMany(ctx, bson.D{})
	require.NoError(t, err)

	session, err := client.StartSession()
	require.NoError(t, err)
	defer session.EndSession(ctx)

	insert := func(n int) error {
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		_, err := session.WithTransaction(c, func(sc mongo.SessionContext) (interface{}, error) {
			return coll.InsertOne(sc, bson.D{{Key: "n", Value: n}})
		})
		return err
	}

	require.NoError(t, insert(1))
	stepDown(t, direct)
	require.NoError(t, insert(2))

	count, err := coll.CountDocuments(ctx, bson.D{})
	require.NoError(t, err)
	assert.Equal(t, int64(2), count)
}

func TestRSKillCursorsEvicts(t *testing.T) {
	port := 27034
	waitForPrimary(t, rsDirectClient(t))
	_, upstream := setupRSProxy(t, port)
	client := rsClient(t, port)
	coll := client.Database("test").Collection("rs_killcursors")
	_, err := coll.DeleteMany(ctx, bson.D{})
	require.NoError(t, err)
	var docs []interface{}
	for i := 0; i < 10; i++ {
		docs = append(docs, bson.D{{Key: "i", Value: i}})
	}
	_, err = coll.InsertMany(ctx, docs)
	require.NoError(t, err)

	before := upstream.CursorCount()
	cur, err := coll.Find(ctx, bson.D{}, options.Find().SetBatchSize(2))
	require.NoError(t, err)
	require.True(t, cur.Next(ctx))
	assert.Equal(t, before+1, upstream.CursorCount())

	require.NoError(t, cur.Close(ctx))
	assert.Eventually(t, func() bool { return upstream.CursorCount() == before },
		2*time.Second, 50*time.Millisecond)
}
