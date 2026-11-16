package proxy

import (
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/stretchr/testify/assert"
	"github.com/xdg-go/scram"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/x/mongo/driver"
	"go.mongodb.org/mongo-driver/x/mongo/driver/wiremessage"
	"go.uber.org/zap"

	mongob "github.com/coinbase/mongobetween/mongo"
)

const (
	authProxyPort     = 27026
	authUpstreamPort  = 27028
	upstreamUser      = "mb_upstream"
	upstreamPassword  = "upstream-s3cret"
	authTestPassword  = "s3cret"
	authTestUsersFile = "app:" + authTestPassword + "\n"
)

func writeUsersFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "users")
	assert.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

// scramConverse runs a full client/server SCRAM-SHA-256 conversation against u and reports whether it succeeded.
func scramConverse(t *testing.T, u *Users, user, pass string) bool {
	t.Helper()
	client, err := scram.SHA256.NewClient(user, pass, "")
	assert.NoError(t, err)
	cc := client.NewConversation()
	sc := u.server.NewConversation()

	c1, err := cc.Step("")
	assert.NoError(t, err)
	s1, err := sc.Step(c1)
	if err != nil {
		return false
	}
	c2, err := cc.Step(s1)
	assert.NoError(t, err)
	s2, err := sc.Step(c2)
	if err != nil || !sc.Valid() {
		return false
	}
	_, err = cc.Step(s2)
	return err == nil && cc.Valid()
}

// The salt is sent to clients base64-encoded by the scram library, so the stored value must be the raw salt:
// libmongoc rejects a double-encoded (24/40 byte) salt.
func TestSCRAMSaltIsRaw28Bytes(t *testing.T) {
	u, err := LoadUsers(writeUsersFile(t, "app:pw1"))
	assert.NoError(t, err)
	assert.Len(t, u.creds["app"].Salt, 28)

	decoy, err := u.lookup("nobody")
	assert.NoError(t, err)
	assert.Len(t, decoy.Salt, 28)
}

func TestLoadUsers(t *testing.T) {
	u, err := LoadUsers(writeUsersFile(t, "app:pa:ss\n# c\n\nro:x"))
	assert.NoError(t, err)
	assert.Equal(t, 2, len(u.creds))
	assert.True(t, scramConverse(t, u, "app", "pa:ss"))
	assert.True(t, scramConverse(t, u, "ro", "x"))
	assert.False(t, scramConverse(t, u, "app", "pa"))
	assert.False(t, scramConverse(t, u, "nobody", "x"))
	assert.Equal(t, []string{"SCRAM-SHA-256"}, u.Mechanisms())
	assert.False(t, u.GroupOrOtherAccessible())

	_, err = LoadUsers(writeUsersFile(t, "app:a\napp:b\n"))
	assert.Error(t, err)

	_, err = LoadUsers(writeUsersFile(t, "app:a\nnocolon\n"))
	assert.Error(t, err)

	_, err = LoadUsers(writeUsersFile(t, ":a\n"))
	assert.Error(t, err)

	_, err = LoadUsers(filepath.Join(t.TempDir(), "missing"))
	assert.Error(t, err)

	path := writeUsersFile(t, "app:topsecretvalue\n")
	assert.NoError(t, os.Chmod(path, 0o644))
	u, err = LoadUsers(path)
	assert.NoError(t, err)
	assert.True(t, u.GroupOrOtherAccessible())

	// errors must never echo a password
	_, err = LoadUsers(writeUsersFile(t, "app:topsecretvalue\napp:topsecretvalue\n"))
	assert.Error(t, err)
	assert.NotContains(t, err.Error(), "topsecretvalue")
}

func saslStartDoc(t *testing.T, mech string, payload string, skipEmpty bool) bsoncore.Document {
	t.Helper()
	d := bson.D{
		{Key: "saslStart", Value: int32(1)},
		{Key: "mechanism", Value: mech},
		{Key: "payload", Value: primitive.Binary{Data: []byte(payload)}},
		{Key: "autoAuthorize", Value: int32(1)},
	}
	if skipEmpty {
		d = append(d, bson.E{Key: "options", Value: bson.D{{Key: "skipEmptyExchange", Value: true}}})
	}
	d = append(d, bson.E{Key: "$db", Value: "admin"})
	raw, err := bson.Marshal(d)
	assert.NoError(t, err)
	return raw
}

func saslContinueDoc(t *testing.T, convID int32, payload string) bsoncore.Document {
	t.Helper()
	raw, err := bson.Marshal(bson.D{
		{Key: "saslContinue", Value: int32(1)},
		{Key: "conversationId", Value: convID},
		{Key: "payload", Value: primitive.Binary{Data: []byte(payload)}},
		{Key: "$db", Value: "test"},
	})
	assert.NoError(t, err)
	return raw
}

type saslReplyFields struct {
	ConversationID int32  `bson:"conversationId"`
	Done           bool   `bson:"done"`
	Payload        []byte `bson:"payload"`
	OK             int32  `bson:"ok"`
}

func decodeSASLReply(t *testing.T, doc bsoncore.Document) saslReplyFields {
	t.Helper()
	var r saslReplyFields
	assert.NoError(t, bson.Unmarshal(doc, &r))
	_, _, ok := doc.Lookup("payload").BinaryOK()
	assert.True(t, ok, "payload must be binary")
	return r
}

func authCode(err error) int32 {
	var ae *authError
	if errors.As(err, &ae) {
		return ae.pe.Code
	}
	return 0
}

func TestAuthSessionSASL(t *testing.T) {
	u, err := LoadUsers(writeUsersFile(t, "app:s3cret\n"))
	assert.NoError(t, err)

	for _, skipEmpty := range []bool{true, false} {
		t.Run(fmt.Sprintf("skipEmptyExchange=%v", skipEmpty), func(t *testing.T) {
			client, err := scram.SHA256.NewClient("app", "s3cret", "")
			assert.NoError(t, err)
			cc := client.NewConversation()
			var s authSession

			c1, _ := cc.Step("")
			reply, err := s.saslStart(u, saslStartDoc(t, "SCRAM-SHA-256", c1, skipEmpty))
			assert.NoError(t, err)
			r := decodeSASLReply(t, reply)
			assert.False(t, r.Done)
			assert.Equal(t, int32(1), r.OK)
			assert.False(t, s.authenticated)

			c2, err := cc.Step(string(r.Payload))
			assert.NoError(t, err)
			reply, err = s.saslContinue(u, saslContinueDoc(t, r.ConversationID, c2))
			assert.NoError(t, err)
			r2 := decodeSASLReply(t, reply)
			assert.Equal(t, r.ConversationID, r2.ConversationID)
			_, err = cc.Step(string(r2.Payload))
			assert.NoError(t, err)
			assert.True(t, cc.Valid())

			if skipEmpty {
				assert.True(t, r2.Done)
			} else {
				assert.False(t, r2.Done)
				assert.False(t, s.authenticated)
				reply, err = s.saslContinue(u, saslContinueDoc(t, r.ConversationID, ""))
				assert.NoError(t, err)
				r3 := decodeSASLReply(t, reply)
				assert.True(t, r3.Done)
				assert.Equal(t, 0, len(r3.Payload))
			}
			assert.True(t, s.authenticated)
			assert.Equal(t, "app", s.user)
			assert.Nil(t, s.conv)
		})
	}

	t.Run("wrong password", func(t *testing.T) {
		client, _ := scram.SHA256.NewClient("app", "nope", "")
		cc := client.NewConversation()
		var s authSession
		c1, _ := cc.Step("")
		reply, err := s.saslStart(u, saslStartDoc(t, "SCRAM-SHA-256", c1, true))
		assert.NoError(t, err)
		r := decodeSASLReply(t, reply)
		c2, _ := cc.Step(string(r.Payload))
		_, err = s.saslContinue(u, saslContinueDoc(t, r.ConversationID, c2))
		assert.Equal(t, int32(18), authCode(err))
		assert.False(t, s.authenticated)
		assert.Nil(t, s.conv)
	})

	t.Run("unknown user fails only at proof", func(t *testing.T) {
		client, _ := scram.SHA256.NewClient("ghost", "s3cret", "")
		cc := client.NewConversation()
		var s authSession
		c1, _ := cc.Step("")
		reply, err := s.saslStart(u, saslStartDoc(t, "SCRAM-SHA-256", c1, true))
		assert.NoError(t, err, "unknown users must not be distinguishable at saslStart")
		r := decodeSASLReply(t, reply)
		c2, err := cc.Step(string(r.Payload))
		assert.NoError(t, err)
		_, err = s.saslContinue(u, saslContinueDoc(t, r.ConversationID, c2))
		assert.Equal(t, int32(18), authCode(err))
		assert.False(t, s.authenticated)
	})

	t.Run("conversationId mismatch", func(t *testing.T) {
		client, _ := scram.SHA256.NewClient("app", "s3cret", "")
		cc := client.NewConversation()
		var s authSession
		c1, _ := cc.Step("")
		reply, err := s.saslStart(u, saslStartDoc(t, "SCRAM-SHA-256", c1, true))
		assert.NoError(t, err)
		r := decodeSASLReply(t, reply)
		c2, _ := cc.Step(string(r.Payload))
		_, err = s.saslContinue(u, saslContinueDoc(t, r.ConversationID+1, c2))
		assert.Equal(t, int32(18), authCode(err))
		assert.False(t, s.authenticated)
	})

	t.Run("saslContinue without saslStart", func(t *testing.T) {
		var s authSession
		_, err := s.saslContinue(u, saslContinueDoc(t, 1, "c=biws"))
		assert.Equal(t, int32(18), authCode(err))
	})

	t.Run("unsupported mechanism", func(t *testing.T) {
		var s authSession
		_, err := s.saslStart(u, saslStartDoc(t, "SCRAM-SHA-1", "n,,n=app,r=abc", true))
		assert.Equal(t, int32(18), authCode(err))
		_, err = s.saslStart(u, saslStartDoc(t, "PLAIN", "\x00app\x00s3cret", true))
		assert.Equal(t, int32(18), authCode(err))
	})

	t.Run("malformed client-first", func(t *testing.T) {
		var s authSession
		_, err := s.saslStart(u, saslStartDoc(t, "SCRAM-SHA-256", "garbage", true))
		assert.Equal(t, int32(18), authCode(err))
		// an empty gs2 flag makes the scram library index an empty string
		assert.NotPanics(t, func() {
			_, err = s.saslStart(u, saslStartDoc(t, "SCRAM-SHA-256", ",,n=app,r=abc", true))
		})
		assert.Equal(t, int32(18), authCode(err))
	})

	for _, proofLen := range []int{64, 31, 0} {
		t.Run(fmt.Sprintf("proof of %d bytes", proofLen), func(t *testing.T) {
			client, _ := scram.SHA256.NewClient("app", "s3cret", "")
			cc := client.NewConversation()
			var s authSession
			c1, _ := cc.Step("")
			reply, err := s.saslStart(u, saslStartDoc(t, "SCRAM-SHA-256", c1, true))
			assert.NoError(t, err)
			r := decodeSASLReply(t, reply)
			c2, err := cc.Step(string(r.Payload))
			assert.NoError(t, err)
			// keep channel binding and nonce, swap the proof for one of the wrong length
			forged := c2[:strings.LastIndex(c2, ",p=")] + ",p=" + base64.StdEncoding.EncodeToString(make([]byte, proofLen))
			assert.NotPanics(t, func() {
				_, err = s.saslContinue(u, saslContinueDoc(t, r.ConversationID, forged))
			})
			assert.Equal(t, int32(18), authCode(err))
			assert.False(t, s.authenticated)
			assert.Nil(t, s.conv)
		})
	}
}

func TestSafeStepRecoversPanic(t *testing.T) {
	// a zero ServerConversation has no credential lookup: Step panics on a nil func call
	out, err := safeStep(&scram.ServerConversation{}, "n,,n=app,r=abc")
	assert.Equal(t, "", out)
	assert.True(t, errors.Is(err, errSCRAMPanic), "%v", err)
}

func opMsgOperation(t *testing.T, doc bson.D) mongob.Operation {
	t.Helper()
	raw, err := bson.Marshal(doc)
	assert.NoError(t, err)
	idx, wm := wiremessage.AppendHeaderStart(nil, 1, 0, wiremessage.OpMsg)
	wm = wiremessage.AppendMsgFlags(wm, 0)
	wm = wiremessage.AppendMsgSectionType(wm, wiremessage.SingleDocument)
	wm = append(wm, raw...)
	wm = bsoncore.UpdateLength(wm, idx, int32(len(wm[idx:])))
	op, err := mongob.Decode(wm)
	assert.NoError(t, err)
	return op
}

func replyOK(t *testing.T, res *mongob.Message) float64 {
	t.Helper()
	if !assert.NotNil(t, res) {
		return -1
	}
	op, err := mongob.Decode(res.Wm)
	assert.NoError(t, err)
	assert.Equal(t, wiremessage.OpMsg, op.OpCode())
	doc, ok := mongob.CommandDocument(op)
	assert.True(t, ok)
	return doc.Lookup("ok").Double()
}

func TestLogoutAnsweredLocally(t *testing.T) {
	logout := bson.D{{Key: "logout", Value: int32(1)}, {Key: "$db", Value: "admin"}}

	t.Run("auth off", func(t *testing.T) {
		c := &connection{log: zap.NewNop()}
		res, handled, err := c.authorize(opMsgOperation(t, logout))
		assert.NoError(t, err)
		assert.True(t, handled, "logout must never be forwarded upstream")
		assert.Equal(t, 1.0, replyOK(t, res))

		_, handled, _ = c.authorize(opMsgOperation(t, bson.D{{Key: "find", Value: "c"}, {Key: "$db", Value: "test"}}))
		assert.False(t, handled)
	})

	t.Run("auth on", func(t *testing.T) {
		u, err := LoadUsers(writeUsersFile(t, authTestUsersFile))
		assert.NoError(t, err)
		c := &connection{log: zap.NewNop(), users: u, auth: authSession{authenticated: true, user: "app"}}
		res, handled, err := c.authorize(opMsgOperation(t, logout))
		assert.NoError(t, err)
		assert.True(t, handled)
		assert.Equal(t, 1.0, replyOK(t, res))
		assert.False(t, c.auth.authenticated)
		assert.Equal(t, "", c.auth.user)

		res, handled, err = c.authorize(opMsgOperation(t, bson.D{{Key: "find", Value: "c"}, {Key: "$db", Value: "test"}}))
		assert.NoError(t, err)
		assert.True(t, handled)
		assert.Equal(t, 0.0, replyOK(t, res))
	})
}

var (
	authProxyOnce sync.Once
)

func upstreamURI(t *testing.T) string {
	t.Helper()
	requireDockerHarness(t)
	return "mongodb://mongo1:27017/test"
}

func startAuthProxy(t *testing.T, port int, usersFile string, upstreamOpts *options.ClientOptions, proxyOpts ...Option) *Proxy {
	t.Helper()
	p, _ := startAuthProxyUpstream(t, port, usersFile, upstreamOpts, proxyOpts...)
	return p
}

func startAuthProxyUpstream(t *testing.T, port int, usersFile string, upstreamOpts *options.ClientOptions, proxyOpts ...Option) (*Proxy, *mongob.Mongo) {
	t.Helper()
	requireDockerHarness(t)
	users, err := LoadUsers(writeUsersFile(t, usersFile))
	assert.NoError(t, err)
	sd, err := statsd.New("localhost:8125")
	assert.NoError(t, err)
	dynamic, err := NewDynamic("", zap.L())
	assert.NoError(t, err)

	upstream, err := mongob.Connect(zap.L(), sd, upstreamOpts, mongob.Settings{})
	assert.NoError(t, err)

	p, err := NewProxy(zap.L(), sd, "label", "tcp4", fmt.Sprintf(":%d", port), false,
		func(string) *mongob.Mongo { return upstream }, dynamic, nil, users, proxyOpts...)
	assert.NoError(t, err)
	go func() { _ = p.Run() }()
	return p, upstream
}

// sharedAuthProxy starts (once per test binary) the auth-enabled proxy on authProxyPort with users-file app:s3cret.
func sharedAuthProxy(t *testing.T) {
	t.Helper()
	requireDockerHarness(t)
	authProxyOnce.Do(func() {
		startAuthProxy(t, authProxyPort, authTestUsersFile, options.Client().ApplyURI(upstreamURI(t)))
	})
}

func connectAuthClient(t *testing.T, port int, opts ...*options.ClientOptions) *mongo.Client {
	t.Helper()
	all := append([]*options.ClientOptions{options.Client().
		ApplyURI(fmt.Sprintf("mongodb://localhost:%d/test", port)).
		SetServerSelectionTimeout(3 * time.Second)}, opts...)
	client, err := mongo.Connect(ctx, all...)
	assert.NoError(t, err)
	return client
}

func serverCode(err error) int32 {
	var ce mongo.CommandError
	if errors.As(err, &ce) {
		return ce.Code
	}
	var de driver.Error
	if errors.As(err, &de) {
		return de.Code
	}
	return 0
}

func TestProxyAuthSuccess(t *testing.T) {
	sharedAuthProxy(t)

	creds := []options.Credential{
		{Username: "app", Password: authTestPassword, AuthMechanism: "SCRAM-SHA-256"},
		{Username: "app", Password: authTestPassword}, // negotiated through saslSupportedMechs
	}
	for _, cred := range creds {
		t.Run(fmt.Sprintf("mechanism=%q", cred.AuthMechanism), func(t *testing.T) {
			client := connectAuthClient(t, authProxyPort, options.Client().SetAuth(cred))
			defer func() { _ = client.Disconnect(ctx) }()

			coll := client.Database("test").Collection("test_proxy_auth")
			_, err := coll.InsertOne(ctx, Trainer{"Ash", 10, "Pallet Town"})
			assert.NoError(t, err)

			cur, err := coll.Find(ctx, bson.D{{Key: "name", Value: "Ash"}})
			assert.NoError(t, err)
			var results []Trainer
			assert.NoError(t, cur.All(ctx, &results))
			assert.NotEmpty(t, results)

			_, err = coll.DeleteMany(ctx, bson.D{})
			assert.NoError(t, err)
		})
	}
}

func TestProxyAuthWrongPassword(t *testing.T) {
	sharedAuthProxy(t)

	client := connectAuthClient(t, authProxyPort, options.Client().
		SetAuth(options.Credential{Username: "app", Password: "wrong", AuthMechanism: "SCRAM-SHA-256"}))
	defer func() { _ = client.Disconnect(ctx) }()

	err := client.Ping(ctx, nil)
	assert.Error(t, err)
	assert.Equal(t, int32(18), serverCode(err), "%v", err)
}

func TestProxyAuthRequired(t *testing.T) {
	sharedAuthProxy(t)

	var closed atomic.Int32
	monitor := &event.PoolMonitor{Event: func(e *event.PoolEvent) {
		if e.Type == event.ConnectionClosed {
			closed.Add(1)
		}
	}}
	client := connectAuthClient(t, authProxyPort, options.Client().SetPoolMonitor(monitor).SetMaxPoolSize(1))
	defer func() { _ = client.Disconnect(ctx) }()

	_, err := client.Database("test").Collection("test_proxy_auth").Find(ctx, bson.D{})
	assert.Error(t, err)
	var ce mongo.CommandError
	assert.True(t, errors.As(err, &ce), "%v", err)
	assert.Equal(t, int32(13), ce.Code)

	assert.NoError(t, client.Ping(ctx, nil))
	assert.Equal(t, int32(0), closed.Load(), "connection must stay open after an unauthorized command")
}

func TestProxyAuthUpstreamCredsNotAccepted(t *testing.T) {
	// Create the upstream user directly on mongo1 so the proxy's upstream pool authenticates with it.
	direct, err := mongo.Connect(ctx, options.Client().ApplyURI(upstreamURI(t)))
	assert.NoError(t, err)
	defer func() { _ = direct.Disconnect(ctx) }()
	// least privilege: readWrite on the test database only
	testDB := direct.Database("test")
	dropUpstreamUser := func() { _ = testDB.RunCommand(ctx, bson.D{{Key: "dropUser", Value: upstreamUser}}).Err() }
	dropUpstreamUser()
	defer dropUpstreamUser()
	assert.NoError(t, testDB.RunCommand(ctx, bson.D{
		{Key: "createUser", Value: upstreamUser},
		{Key: "pwd", Value: upstreamPassword},
		{Key: "roles", Value: bson.A{bson.D{{Key: "role", Value: "readWrite"}, {Key: "db", Value: "test"}}}},
		{Key: "mechanisms", Value: bson.A{"SCRAM-SHA-256"}},
	}).Err())

	upstreamOpts := options.Client().ApplyURI(upstreamURI(t)).
		SetAuth(options.Credential{Username: upstreamUser, Password: upstreamPassword, AuthSource: "test"})
	p := startAuthProxy(t, authUpstreamPort, authTestUsersFile, upstreamOpts)
	defer p.Kill()

	// the proxy-only user works, proving the upstream pool is healthy and authenticated
	good := connectAuthClient(t, authUpstreamPort, options.Client().
		SetAuth(options.Credential{Username: "app", Password: authTestPassword}))
	defer func() { _ = good.Disconnect(ctx) }()
	_, err = good.Database("test").Collection("test_proxy_auth").CountDocuments(ctx, bson.D{})
	assert.NoError(t, err)

	// the upstream credentials are not proxy credentials
	bad := connectAuthClient(t, authUpstreamPort, options.Client().
		SetAuth(options.Credential{Username: upstreamUser, Password: upstreamPassword, AuthSource: "test"}))
	defer func() { _ = bad.Disconnect(ctx) }()
	err = bad.Ping(ctx, nil)
	assert.Error(t, err)
	assert.Equal(t, int32(18), serverCode(err), "%v", err)
}

func TestProxyAuthLogout(t *testing.T) {
	sharedAuthProxy(t)

	client := connectAuthClient(t, authProxyPort, options.Client().SetMaxPoolSize(1).
		SetAuth(options.Credential{Username: "app", Password: authTestPassword}))
	defer func() { _ = client.Disconnect(ctx) }()

	coll := client.Database("test").Collection("test_proxy_auth")
	_, err := coll.CountDocuments(ctx, bson.D{})
	assert.NoError(t, err)

	var res bson.M
	assert.NoError(t, client.Database("admin").RunCommand(ctx, bson.D{{Key: "logout", Value: 1}}).Decode(&res))
	assert.Equal(t, 1.0, res["ok"])

	// same (single) pooled connection, now logged out
	_, err = coll.Find(ctx, bson.D{})
	var ce mongo.CommandError
	assert.True(t, errors.As(err, &ce), "%v", err)
	assert.Equal(t, int32(13), ce.Code)
}

func TestProxyLogoutAuthOff(t *testing.T) {
	requireDockerHarness(t)
	const port = 27029
	sd, err := statsd.New("localhost:8125")
	assert.NoError(t, err)
	dynamic, err := NewDynamic("", zap.L())
	assert.NoError(t, err)
	upstream, err := mongob.Connect(zap.L(), sd, options.Client().ApplyURI(upstreamURI(t)), mongob.Settings{})
	assert.NoError(t, err)
	defer upstream.Close()
	p, err := NewProxy(zap.L(), sd, "label", "tcp4", fmt.Sprintf(":%d", port), false,
		func(string) *mongob.Mongo { return upstream }, dynamic, nil, nil)
	assert.NoError(t, err)
	go func() { _ = p.Run() }()
	defer p.Kill()

	client := connectAuthClient(t, port, options.Client().SetMaxPoolSize(1))
	defer func() { _ = client.Disconnect(ctx) }()

	var res bson.M
	assert.NoError(t, client.Database("admin").RunCommand(ctx, bson.D{{Key: "logout", Value: 1}}).Decode(&res))
	assert.Equal(t, 1.0, res["ok"])

	_, err = client.Database("test").Collection("test_proxy_auth").CountDocuments(ctx, bson.D{})
	assert.NoError(t, err)
}

func replyDoc(t *testing.T, res *mongob.Message) bsoncore.Document {
	t.Helper()
	if !assert.NotNil(t, res) {
		return nil
	}
	op, err := mongob.Decode(res.Wm)
	assert.NoError(t, err)
	doc, ok := mongob.CommandDocument(op)
	assert.True(t, ok)
	return doc
}

// With inbound auth off, authentication commands are refused locally: forwarded, they would authenticate (or log
// out) the shared upstream connection.
func TestAuthCommandsRefusedWhenAuthOff(t *testing.T) {
	c := &connection{log: zap.NewNop()}
	for name, doc := range map[string]bson.D{
		"saslStart":    {{Key: "saslStart", Value: int32(1)}, {Key: "mechanism", Value: "SCRAM-SHA-256"}, {Key: "payload", Value: primitive.Binary{Data: []byte("n,,n=app,r=abc")}}, {Key: "$db", Value: "admin"}},
		"saslContinue": {{Key: "saslContinue", Value: int32(1)}, {Key: "conversationId", Value: int32(1)}, {Key: "payload", Value: primitive.Binary{}}, {Key: "$db", Value: "admin"}},
		"authenticate": {{Key: "authenticate", Value: int32(1)}, {Key: "mechanism", Value: "MONGODB-X509"}, {Key: "$db", Value: "$external"}},
	} {
		res, handled, err := c.authorize(opMsgOperation(t, doc))
		assert.NoError(t, err, name)
		assert.True(t, handled, "%s must not be forwarded upstream", name)
		reply := replyDoc(t, res)
		if reply == nil {
			continue
		}
		assert.Equal(t, 0.0, reply.Lookup("ok").Double(), name)
		assert.Equal(t, int32(18), reply.Lookup("code").Int32(), name)
		assert.Contains(t, reply.Lookup("errmsg").StringValue(), "inbound auth disabled on this proxy", name)
	}
}
