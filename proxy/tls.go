package proxy

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"time"
)

// tlsHandshakeTimeout bounds how long a freshly accepted client may take to complete the TLS handshake, so a
// plain-text or silent client cannot pin a connection goroutine.
const tlsHandshakeTimeout = 5 * time.Second

// LoadServerTLSConfig builds the TLS config for the client-facing listener. See loadServerTLSConfig.
func LoadServerTLSConfig(certFile, keyFile, caFile string) (*tls.Config, error) {
	return loadServerTLSConfig(certFile, keyFile, caFile)
}

// loadServerTLSConfig builds a server TLS config from a certificate/key pair. When caFile is set, client
// certificates are required and verified against it (mTLS).
func loadServerTLSConfig(certFile, keyFile, caFile string) (*tls.Config, error) {
	if certFile == "" || keyFile == "" {
		return nil, errors.New("both TLS certificate and key files are required")
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("loading TLS key pair: %w", err)
	}

	cfg := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
	}

	if caFile != "" {
		caPEM, err := os.ReadFile(caFile) //nolint:gosec // operator supplied path
		if err != nil {
			return nil, fmt.Errorf("reading TLS CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, errors.New("TLS CA file contains no valid certificates")
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return cfg, nil
}
