package proxy

import (
	"crypto/tls"
	"fmt"
	"net"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"go.uber.org/zap"

	"github.com/coinbase/mongobetween/mongo"
	"github.com/coinbase/mongobetween/util"
)

const restartSleep = 1 * time.Second

type MongoLookup func(address string) *mongo.Mongo

type Proxy struct {
	log    *zap.Logger
	statsd *statsd.Client

	network string
	address string
	unlink  bool

	mongoLookup MongoLookup
	dynamic     *Dynamic
	tlsConfig   *tls.Config
	users       *Users
	authTimeout time.Duration

	// maxConnections caps concurrently open client connections (0 = unlimited).
	maxConnections int

	quit chan interface{}
	kill chan interface{}

	drained     chan interface{}
	drainedOnce sync.Once
}

// Option configures optional Proxy behaviour.
type Option func(*Proxy)

// WithAuthTimeout closes a connection that stays unauthenticated and idle for d (only when inbound authentication
// is on). The deadline is re-armed before every read until the connection authenticates, so unauthenticated
// monitoring connections that send a handshake more often than d stay open. 0 disables it.
func WithAuthTimeout(d time.Duration) Option {
	return func(p *Proxy) { p.authTimeout = d }
}

// WithMaxConnections caps the number of concurrently open client connections; connections accepted beyond it are
// closed at once. 0 means unlimited.
func WithMaxConnections(n int) Option {
	return func(p *Proxy) { p.maxConnections = n }
}

func NewProxy(log *zap.Logger, sd *statsd.Client, label, network, address string, unlink bool, mongoLookup MongoLookup, dynamic *Dynamic, tlsConfig *tls.Config, users *Users, opts ...Option) (*Proxy, error) {
	if label != "" {
		log = log.With(zap.String("cluster", label))

		var err error
		sd, err = util.StatsdWithTags(sd, []string{fmt.Sprintf("cluster:%s", label)})
		if err != nil {
			return nil, err
		}
	}
	p := &Proxy{
		log:    log,
		statsd: sd,

		network: network,
		address: address,
		unlink:  unlink,

		mongoLookup: mongoLookup,
		dynamic:     dynamic,
		tlsConfig:   tlsConfig,
		users:       users,

		quit: make(chan interface{}),
		kill: make(chan interface{}),

		drained: make(chan interface{}),
	}
	for _, opt := range opts {
		opt(p)
	}
	return p, nil
}

func (p *Proxy) Run() error {
	return p.run()
}

func (p *Proxy) Shutdown() {
	defer func() {
		_ = recover() // "close of closed channel" panic if Shutdown() was already called
	}()
	close(p.quit)
}

// ShutdownWithTimeout stops accepting connections, waits up to d for open client connections to close, then
// force closes whatever remains via Kill().
func (p *Proxy) ShutdownWithTimeout(d time.Duration) {
	p.Shutdown()

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-p.drained:
	case <-timer.C:
		p.log.Warn("Shutdown timeout reached, closing remaining connections", zap.Duration("timeout", d))
		p.Kill()
	}
}

func (p *Proxy) Kill() {
	p.Shutdown()

	defer func() {
		_ = recover() // "close of closed channel" panic if Kill() was already called
	}()
	close(p.kill)
}

func (p *Proxy) run() error {
	defer func() {
		if r := recover(); r != nil {
			p.log.Error("Crashed", zap.String("panic", fmt.Sprintf("%v", r)), zap.String("stack", string(debug.Stack())))

			time.Sleep(restartSleep)

			p.log.Info("Restarting", zap.Duration("sleep", restartSleep))
			go func() {
				err := p.run()
				if err != nil {
					p.log.Error("Error restarting", zap.Error(err))
				}
			}()
		}
	}()

	return p.listen()
}

func (p *Proxy) listen() error {
	if strings.Contains(p.network, "unix") {
		oldUmask := syscall.Umask(0)
		defer syscall.Umask(oldUmask)
		if p.unlink {
			_ = syscall.Unlink(p.address)
		}
	}

	l, err := net.Listen(p.network, p.address)
	if err != nil {
		return err
	}
	if p.tlsConfig != nil {
		l = tls.NewListener(l, p.tlsConfig)
	}
	defer func() {
		_ = l.Close()
	}()
	go func() {
		<-p.quit
		err := l.Close()
		if err != nil {
			p.log.Info("Error closing listener", zap.Error(err))
		}
	}()

	p.accept(l)
	return nil
}

func (p *Proxy) accept(l net.Listener) {
	var wg sync.WaitGroup
	defer func() {
		p.log.Info("Waiting for open connections")
		wg.Wait()
		p.drainedOnce.Do(func() { close(p.drained) })
	}()

	opened, closed := util.StatsdBackgroundGauge(p.statsd, "open_connections", []string{})
	var open atomic.Int64

	for {
		c, err := l.Accept()
		if err != nil {
			select {
			case <-p.quit:
				return
			default:
				p.log.Error("Failed to accept incoming connection", zap.Error(err))
				continue
			}
		}

		log := p.log
		remoteAddr := c.RemoteAddr().String()
		if remoteAddr != "" {
			log = p.log.With(zap.String("remote_address", remoteAddr))
		}

		if p.maxConnections > 0 && open.Load() >= int64(p.maxConnections) {
			_ = c.Close()
			log.Warn("Max connections reached, closing new connection", zap.Int("max_connections", p.maxConnections))
			_ = p.statsd.Incr("connection_rejected", []string{"reason:max_connections"}, 1)
			continue
		}

		done := make(chan interface{})

		open.Add(1)
		wg.Add(1)
		opened("connection_opened", []string{})
		go func() {
			defer func() {
				close(done)
				open.Add(-1)
				wg.Done()
				closed("connection_closed", []string{})
			}()
			log.Info("Accept")
			if !handshakeTLS(log, c) {
				_ = c.Close()
				return
			}
			handleConnection(p, log, c)

			_ = c.Close()
			log.Info("Close")
		}()

		go func() {
			select {
			case <-done:
				// closed
			case <-p.kill:
				err := c.Close()
				if err == nil {
					log.Warn("Force closed connection")
				}
			}
		}()
	}
}

// handshakeTLS completes the TLS handshake with a bounded deadline when c is a TLS connection. It returns false
// if the handshake failed. Non-TLS connections always return true.
func handshakeTLS(log *zap.Logger, c net.Conn) bool {
	tc, ok := c.(*tls.Conn)
	if !ok {
		return true
	}
	_ = tc.SetDeadline(time.Now().Add(tlsHandshakeTimeout))
	if err := tc.Handshake(); err != nil {
		log.Warn("TLS handshake failed", zap.Error(err))
		return false
	}
	_ = tc.SetDeadline(time.Time{})
	return true
}
