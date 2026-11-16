package proxy

import (
	"bufio"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/xdg-go/scram"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"go.uber.org/zap"

	"github.com/coinbase/mongobetween/mongo"
)

const (
	mechanismSCRAMSHA256 = "SCRAM-SHA-256"
	scramIterations      = 15000
	scramSaltBytes       = 16
	decoyKeyBytes        = 32

	codeUnauthorized         int32 = 13
	codeAuthenticationFailed int32 = 18
	authenticationFailedMsg        = "Authentication failed."
)

// Users holds the proxy-only inbound credentials. Only SCRAM-SHA-256 stored credentials (salt, iterations, StoredKey,
// ServerKey) are kept; plaintext passwords are discarded once derived. It is read-only after LoadUsers.
type Users struct {
	creds  map[string]scram.StoredCredentials
	server *scram.Server

	// decoy material answers unknown users with plausible, stable credentials so that a lookup miss is
	// indistinguishable from a wrong password: the conversation fails only at the proof check.
	decoyKey       []byte
	decoyStoredKey []byte
	decoyServerKey []byte

	groupOrOtherAccessible bool
}

// LoadUsers reads a users file of "username:password" lines (split on the first ':'; '#' comments and blank lines
// allowed; duplicate usernames rejected) and derives SCRAM-SHA-256 credentials with a random salt per user.
// Errors never contain passwords.
func LoadUsers(path string) (*Users, error) {
	f, err := os.Open(path) //nolint:gosec // operator supplied path
	if err != nil {
		return nil, fmt.Errorf("opening auth users file: %w", err)
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("reading auth users file: %w", err)
	}

	u, err := parseUsers(f)
	if err != nil {
		return nil, err
	}
	u.groupOrOtherAccessible = info.Mode().Perm()&0o077 != 0
	return u, nil
}

func parseUsers(r io.Reader) (*Users, error) {
	u := &Users{creds: make(map[string]scram.StoredCredentials)}

	scanner := bufio.NewScanner(r)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSuffix(scanner.Text(), "\r")
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		user, pass, ok := strings.Cut(line, ":")
		if !ok {
			return nil, fmt.Errorf("auth users file line %d: expected username:password", lineNo)
		}
		if user == "" {
			return nil, fmt.Errorf("auth users file line %d: empty username", lineNo)
		}
		if pass == "" {
			return nil, fmt.Errorf("auth users file line %d: empty password", lineNo)
		}
		if _, dup := u.creds[user]; dup {
			return nil, fmt.Errorf("auth users file line %d: duplicate user %q", lineNo, user)
		}
		creds, err := deriveCredentials(user, pass)
		if err != nil {
			return nil, fmt.Errorf("auth users file line %d: %w", lineNo, err)
		}
		u.creds[user] = creds
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading auth users file: %w", err)
	}
	if len(u.creds) == 0 {
		return nil, errors.New("auth users file contains no users")
	}

	var err error
	if u.decoyKey, err = randomBytes(decoyKeyBytes); err != nil {
		return nil, err
	}
	if u.decoyStoredKey, err = randomBytes(sha256.Size); err != nil {
		return nil, err
	}
	if u.decoyServerKey, err = randomBytes(sha256.Size); err != nil {
		return nil, err
	}
	if u.server, err = scram.SHA256.NewServer(u.lookup); err != nil {
		return nil, err
	}
	return u, nil
}

// deriveCredentials computes the SCRAM-SHA-256 stored credentials for user/pass. The scram library's error text
// echoes the password, so it is never wrapped.
func deriveCredentials(user, pass string) (scram.StoredCredentials, error) {
	client, err := scram.SHA256.NewClient(user, pass, "")
	if err != nil {
		return scram.StoredCredentials{}, errors.New("username or password rejected by SASLprep")
	}
	salt, err := randomBytes(scramSaltBytes)
	if err != nil {
		return scram.StoredCredentials{}, err
	}
	return client.GetStoredCredentials(scram.KeyFactors{
		Salt:  base64.StdEncoding.EncodeToString(salt),
		Iters: scramIterations,
	}), nil
}

func randomBytes(n int) ([]byte, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return nil, fmt.Errorf("generating random bytes: %w", err)
	}
	return b, nil
}

// lookup is the scram.CredentialLookup. Unknown users get decoy credentials (stable per username, never matching any
// proof) instead of an error, so the server-first message does not reveal whether the user exists.
func (u *Users) lookup(username string) (scram.StoredCredentials, error) {
	if c, ok := u.creds[username]; ok {
		return c, nil
	}
	mac := hmac.New(sha256.New, u.decoyKey)
	_, _ = mac.Write([]byte(username))
	return scram.StoredCredentials{
		KeyFactors: scram.KeyFactors{
			Salt:  base64.StdEncoding.EncodeToString(mac.Sum(nil)[:scramSaltBytes]),
			Iters: scramIterations,
		},
		StoredKey: u.decoyStoredKey,
		ServerKey: u.decoyServerKey,
	}, nil
}

// Mechanisms is the list of SASL mechanisms advertised in handshake replies.
func (u *Users) Mechanisms() []string {
	return []string{mechanismSCRAMSHA256}
}

// Count is the number of configured users.
func (u *Users) Count() int {
	return len(u.creds)
}

// GroupOrOtherAccessible reports whether the users file mode grants any access to group or other.
func (u *Users) GroupOrOtherAccessible() bool {
	return u.groupOrOtherAccessible
}

// authSession is the inbound authentication state of one client connection.
type authSession struct {
	authenticated bool
	user          string
	conv          *scram.ServerConversation
	convID        int32

	// skipEmptyExchange is the saslStart option of the current conversation: when set, done:true is sent together
	// with the server-final message instead of after an extra empty saslContinue.
	skipEmptyExchange bool
}

// authError is a failed or refused authentication step, rendered to the client as pe. user is the client-supplied
// username (possibly empty) for the log line.
type authError struct {
	pe   mongo.ProxyError
	user string

	// recovered is set when the scram library panicked on the client's input.
	recovered bool
}

func (e *authError) Error() string {
	return e.pe.Message
}

func authFailed(user, message string) *authError {
	return &authError{
		pe:   mongo.ProxyError{Code: codeAuthenticationFailed, CodeName: "AuthenticationFailed", Message: message},
		user: user,
	}
}

// stepFailed renders a failed conv.Step. The username is only read when the library did not panic, since a
// panicking conversation may be half-initialised.
func stepFailed(conv *scram.ServerConversation, err error) *authError {
	if errors.Is(err, errSCRAMPanic) {
		ae := authFailed("", authenticationFailedMsg)
		ae.recovered = true
		return ae
	}
	return authFailed(conv.Username(), authenticationFailedMsg)
}

// saslStart begins a new SCRAM-SHA-256 conversation, abandoning any conversation in progress. An existing
// authentication is kept until a new conversation succeeds.
func (s *authSession) saslStart(u *Users, doc bsoncore.Document) (bsoncore.Document, error) {
	s.conv = nil

	if mech, _ := doc.Lookup("mechanism").StringValueOK(); mech != mechanismSCRAMSHA256 {
		return nil, authFailed("", "unsupported mechanism; only "+mechanismSCRAMSHA256+" is supported")
	}
	payload, ok := saslPayload(doc)
	if !ok {
		return nil, authFailed("", authenticationFailedMsg)
	}

	if !validClientFirst(string(payload)) {
		return nil, authFailed("", authenticationFailedMsg)
	}
	conv := u.server.NewConversation()
	out, err := safeStep(conv, string(payload))
	if err != nil {
		return nil, stepFailed(conv, err)
	}
	if authz := conv.AuthzID(); authz != "" && authz != conv.Username() {
		return nil, authFailed(conv.Username(), authenticationFailedMsg)
	}

	s.convID++
	s.conv = conv
	skip, _ := doc.Lookup("options", "skipEmptyExchange").BooleanOK()
	s.skipEmptyExchange = skip
	return saslReply(s.convID, false, []byte(out))
}

// saslContinue advances the current conversation. The client-final step verifies the proof (constant time, inside
// the scram library) and answers with the server signature.
func (s *authSession) saslContinue(u *Users, doc bsoncore.Document) (bsoncore.Document, error) {
	conv := s.conv
	s.conv = nil // every failure abandons the conversation; success paths re-arm it explicitly
	if conv == nil {
		return nil, authFailed("", "no SASL conversation in progress")
	}
	user := conv.Username()

	if id, ok := doc.Lookup("conversationId").AsInt64OK(); !ok || id != int64(s.convID) {
		return nil, authFailed(user, authenticationFailedMsg)
	}
	payload, ok := saslPayload(doc)
	if !ok {
		return nil, authFailed(user, authenticationFailedMsg)
	}

	if conv.Done() {
		// Only a successful conversation is retained after its final step: this is the client's empty
		// acknowledgement of the server signature (no skipEmptyExchange).
		if len(payload) != 0 {
			return nil, authFailed(user, authenticationFailedMsg)
		}
		s.authenticated, s.user = true, user
		return saslReply(s.convID, true, nil)
	}

	if !validClientFinal(string(payload)) {
		return nil, authFailed(user, authenticationFailedMsg)
	}
	out, err := safeStep(conv, string(payload))
	if err != nil {
		return nil, stepFailed(conv, err)
	}
	if !conv.Valid() {
		return nil, authFailed(user, authenticationFailedMsg)
	}
	if s.skipEmptyExchange {
		s.authenticated, s.user = true, user
		return saslReply(s.convID, true, []byte(out))
	}
	s.conv = conv
	return saslReply(s.convID, false, []byte(out))
}

func saslPayload(doc bsoncore.Document) ([]byte, bool) {
	_, data, ok := doc.Lookup("payload").BinaryOK()
	return data, ok
}

func saslReply(convID int32, done bool, payload []byte) (bsoncore.Document, error) {
	if payload == nil {
		payload = []byte{}
	}
	return bson.Marshal(bson.D{
		{Key: "conversationId", Value: convID},
		{Key: "done", Value: done},
		{Key: "payload", Value: primitive.Binary{Subtype: 0x00, Data: payload}},
		{Key: "ok", Value: 1.0},
	})
}

// authorize enforces inbound authentication for op and answers logout. When handled is true the request has been
// answered locally and must not be forwarded; reply is what to send (nil for nothing). A non-nil err closes the
// connection.
func (c *connection) authorize(op mongo.Operation) (reply *mongo.Message, handled bool, err error) {
	if op.IsIsMaster() {
		return nil, false, nil
	}

	doc, isCommand := mongo.CommandDocument(op)
	name := mongo.CommandName(doc)

	// logout never reaches a pooled upstream connection, where it would log out the proxy's own upstream user for
	// every client sharing it. With auth on it ends this connection's proxy session; with auth off it is a no-op.
	if name == "logout" {
		if c.auth.authenticated {
			c.log.Info("Logged out", zap.String("user", c.auth.user))
		}
		c.auth = authSession{convID: c.auth.convID}
		res, err := mongo.DocumentResponse(op, okDocument)
		return res, true, err
	}

	if c.users == nil {
		return nil, false, nil
	}

	// SASL is always handled locally when inbound auth is on, so it never reaches an upstream connection.
	switch name {
	case "saslStart", "saslContinue":
		return c.handleSASL(op, name, doc)
	case "authenticate":
		return c.authFailureReply(op, authFailed("", "authenticate is not supported; use "+mechanismSCRAMSHA256))
	}

	if c.auth.authenticated {
		return nil, false, nil
	}

	switch name {
	case "ping", "buildInfo", "buildinfo":
		return nil, false, nil
	case string(mongo.EndSessions):
		// Acknowledged locally: an unauthenticated client must not end sessions on the shared upstream user.
		res, err := mongo.DocumentResponse(op, okDocument)
		return res, true, err
	}

	if !isCommand || name == "" {
		name = op.OpCode().String()
	}
	c.log.Debug("Unauthenticated command rejected", zap.String("command", name))
	res, err := mongo.ErrorResponse(op, mongo.ProxyError{
		Code:     codeUnauthorized,
		CodeName: "Unauthorized",
		Message:  fmt.Sprintf("command %s requires authentication", name),
	})
	return res, true, err
}

var okDocument = bsoncore.NewDocumentBuilder().AppendDouble("ok", 1).Build()

func (c *connection) handleSASL(op mongo.Operation, name string, doc bsoncore.Document) (*mongo.Message, bool, error) {
	var out bsoncore.Document
	var err error
	if name == "saslStart" {
		out, err = c.auth.saslStart(c.users, doc)
	} else {
		out, err = c.auth.saslContinue(c.users, doc)
	}
	if err != nil {
		var ae *authError
		if errors.As(err, &ae) {
			return c.authFailureReply(op, ae)
		}
		return nil, true, err
	}

	if done, _ := out.Lookup("done").BooleanOK(); done {
		c.log.Info("Authenticated", zap.String("user", c.auth.user))
	}
	res, err := mongo.DocumentResponse(op, out)
	return res, true, err
}

func (c *connection) authFailureReply(op mongo.Operation, ae *authError) (*mongo.Message, bool, error) {
	if ae.recovered {
		c.log.Debug("Recovered SCRAM library panic on client input")
	}
	c.log.Warn("Authentication failed", zap.String("user", ae.user))
	res, err := mongo.ErrorResponse(op, ae.pe)
	return res, true, err
}

// errSCRAMPanic reports a panic recovered from the scram library while processing client input.
var errSCRAMPanic = errors.New("scram: recovered panic")

// safeStep is conv.Step with any library panic turned into errSCRAMPanic. Input is validated before Step (see
// validClientFirst / validClientFinal); this is defence in depth so client bytes can never crash the connection.
func safeStep(conv *scram.ServerConversation, msg string) (out string, err error) {
	defer func() {
		if r := recover(); r != nil {
			out, err = "", errSCRAMPanic
		}
	}()
	return conv.Step(msg)
}

// validClientFirst checks the gs2 header flag: xdg-go/scram v1.1.2 indexes it without a length check. Channel
// binding ("p=") is not supported, so only "n" and "y" are accepted.
func validClientFirst(msg string) bool {
	return strings.HasPrefix(msg, "n,") || strings.HasPrefix(msg, "y,")
}

// validClientFinal checks that the proof (the last field, as the scram library parses it) decodes to exactly one
// SHA-256 digest: xdg-go/scram v1.1.2 XORs it against a 32-byte signature indexed by the proof length.
func validClientFinal(msg string) bool {
	last := msg[strings.LastIndex(msg, ",")+1:]
	encoded, ok := strings.CutPrefix(last, "p=")
	if !ok {
		return false
	}
	proof, err := base64.StdEncoding.DecodeString(encoded)
	return err == nil && len(proof) == sha256.Size
}
