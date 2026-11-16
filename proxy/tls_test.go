package proxy

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"

	mongob "github.com/coinbase/mongobetween/mongo"
)

type testCerts struct {
	caFile, certFile, keyFile string
}

func writePEM(t *testing.T, path, typ string, der []byte) {
	t.Helper()
	assert.NoError(t, os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600))
}

// genTestCerts writes a throwaway CA and a localhost server certificate into a temp dir.
func genTestCerts(t *testing.T) testCerts {
	t.Helper()
	dir := t.TempDir()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	assert.NoError(t, err)
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	assert.NoError(t, err)
	caCert, err := x509.ParseCertificate(caDER)
	assert.NoError(t, err)

	srvKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	assert.NoError(t, err)
	srvTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	srvDER, err := x509.CreateCertificate(rand.Reader, srvTmpl, caCert, &srvKey.PublicKey, caKey)
	assert.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(srvKey)
	assert.NoError(t, err)

	c := testCerts{
		caFile:   filepath.Join(dir, "ca.pem"),
		certFile: filepath.Join(dir, "server.pem"),
		keyFile:  filepath.Join(dir, "server.key"),
	}
	writePEM(t, c.caFile, "CERTIFICATE", caDER)
	writePEM(t, c.certFile, "CERTIFICATE", srvDER)
	writePEM(t, c.keyFile, "EC PRIVATE KEY", keyDER)
	return c
}

func TestLoadServerTLSConfig(t *testing.T) {
	certs := genTestCerts(t)

	_, err := loadServerTLSConfig(certs.certFile, "", "")
	assert.Error(t, err)
	_, err = loadServerTLSConfig("", certs.keyFile, "")
	assert.Error(t, err)

	cfg, err := loadServerTLSConfig(certs.certFile, certs.keyFile, "")
	assert.NoError(t, err)
	assert.Equal(t, uint16(tls.VersionTLS12), cfg.MinVersion)
	assert.Equal(t, tls.NoClientCert, cfg.ClientAuth)

	cfg, err = loadServerTLSConfig(certs.certFile, certs.keyFile, certs.caFile)
	assert.NoError(t, err)
	assert.Equal(t, tls.RequireAndVerifyClientCert, cfg.ClientAuth)
	assert.NotNil(t, cfg.ClientCAs)

	_, err = loadServerTLSConfig(certs.certFile, certs.keyFile, certs.keyFile)
	assert.Error(t, err)
}

func TestProxyTLS(t *testing.T) {
	const port = 27025
	certs := genTestCerts(t)
	tlsCfg, err := loadServerTLSConfig(certs.certFile, certs.keyFile, "")
	assert.NoError(t, err)

	sd, err := statsd.New("localhost:8125")
	assert.NoError(t, err)
	dynamic, err := NewDynamic("", zap.L())
	assert.NoError(t, err)

	uri := "mongodb://localhost:27017/test"
	if os.Getenv("CI") == "true" {
		uri = "mongodb://mongo1:27017/test"
	}
	upstream, err := mongob.Connect(zap.L(), sd, options.Client().ApplyURI(uri), mongob.Settings{})
	assert.NoError(t, err)
	defer upstream.Close()

	proxy, err := NewProxy(zap.L(), sd, "label", "tcp4", ":27025", false,
		func(string) *mongob.Mongo { return upstream }, dynamic, tlsCfg, nil)
	assert.NoError(t, err)
	go func() { _ = proxy.Run() }()
	defer proxy.Kill()

	pool := x509.NewCertPool()
	caPEM, err := os.ReadFile(certs.caFile)
	assert.NoError(t, err)
	assert.True(t, pool.AppendCertsFromPEM(caPEM))

	client := setupClient(t, "localhost", port,
		options.Client().SetTLSConfig(&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}))
	defer func() { _ = client.Disconnect(ctx) }()

	// plain client against the TLS port must fail fast
	start := time.Now()
	plain, err := mongo.Connect(ctx, options.Client().
		ApplyURI(fmt.Sprintf("mongodb://localhost:%d/test", port)).
		SetServerSelectionTimeout(time.Second))
	assert.NoError(t, err)
	defer func() { _ = plain.Disconnect(ctx) }()
	pingCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	assert.Error(t, plain.Ping(pingCtx, nil))
	assert.True(t, time.Since(start) < 2*time.Second)
}
