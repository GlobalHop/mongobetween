package config

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/coinbase/mongobetween/mongo"
	"github.com/coinbase/mongobetween/proxy"
	"github.com/coinbase/mongobetween/util"
)

const usernamePlaceholder = "_"
const defaultStatsdAddress = "localhost:8125"

var validNetworks = []string{"tcp", "tcp4", "tcp6", "unix", "unixpacket"}

var newStatsdClientInit = newStatsdClient

type Config struct {
	network         string
	unlink          bool
	ping            bool
	checkoutTimeout time.Duration
	maxWireVersion  int32
	shutdownTimeout time.Duration
	authTimeout     time.Duration
	pretty          bool
	clients         []client
	level           zapcore.Level
	dynamic         string
	tlsConfig       *tls.Config
	users           *proxy.Users
	statsdaddr      string
	logger          *zap.Logger
	statsd          *statsd.Client
	mongos          []*mongo.Mongo
	dynamicConfig   *proxy.Dynamic
}

type client struct {
	address string
	label   string
	opts    *options.ClientOptions
}

func ParseFlags() *Config {
	config, err := parseFlags()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		flag.Usage()
		os.Exit(2)
	}
	return config
}

func (c *Config) LogLevel() zapcore.Level {
	return c.level
}

func (c *Config) Pretty() bool {
	return c.pretty
}

func (c *Config) Logger() *zap.Logger {
	return c.logger
}

func (c *Config) Statsd() *statsd.Client {
	return c.statsd
}

// ShutdownTimeout is how long a proxy waits for client connections to close before force closing them.
func (c *Config) ShutdownTimeout() time.Duration {
	return c.shutdownTimeout
}

// CloseUpstreams closes every upstream driver client created by Proxies and stops the dynamic config poller.
func (c *Config) CloseUpstreams() {
	for _, m := range c.mongos {
		m.Close()
	}
	if c.dynamicConfig != nil {
		c.dynamicConfig.Close()
	}
}

func (c *Config) Proxies(log *zap.Logger) (proxies []*proxy.Proxy, err error) {
	d, err := proxy.NewDynamic(c.dynamic, log)
	if err != nil {
		return nil, err
	}
	c.dynamicConfig = d

	mongos := make(map[string]*mongo.Mongo)
	for _, client := range c.clients {
		m, err := mongo.Connect(log, c.statsd, client.opts, mongo.Settings{Ping: c.ping, MaxWireVersion: c.maxWireVersion, CheckoutTimeout: c.checkoutTimeout})
		if err != nil {
			return nil, err
		}
		mongos[client.address] = m
		c.mongos = append(c.mongos, m)
	}
	mongoLookup := func(address string) *mongo.Mongo {
		return mongos[address]
	}

	for _, client := range c.clients {
		p, err := proxy.NewProxy(log, c.statsd, client.label, c.network, client.address, c.unlink, mongoLookup, d, c.tlsConfig, c.users,
			proxy.WithAuthTimeout(c.authTimeout))
		if err != nil {
			return nil, err
		}
		proxies = append(proxies, p)
	}
	return
}

func validNetwork(network string) bool {
	for _, n := range validNetworks {
		if n == network {
			return true
		}
	}
	return false
}

func parseFlags() (*Config, error) {
	flag.Usage = func() {
		fmt.Printf("Usage: %s [OPTIONS] address1=uri1 [address2=uri2] ...\n", os.Args[0])
		flag.PrintDefaults()
	}

	var maxWireVersion int
	var checkoutTimeout time.Duration
	var shutdownTimeout time.Duration
	var authTimeout time.Duration
	var requireUpstreamTLS bool
	var unlink, ping, pretty, enableSdamMetrics, enableSdamLogging bool
	var tlsCertFile, tlsKeyFile, tlsCAFile, authUsersFile string
	var network, username, password, stats, loglevel, dynamic string
	flag.StringVar(&network, "network", "tcp4", "One of: tcp, tcp4, tcp6, unix or unixpacket")
	flag.StringVar(&username, "username", "", "MongoDB username")
	flag.StringVar(&password, "password", "", "MongoDB password")
	flag.StringVar(&stats, "statsd", defaultStatsdAddress, "Statsd address")
	flag.BoolVar(&unlink, "unlink", false, "Unlink existing unix sockets before listening")
	flag.IntVar(&maxWireVersion, "max-wire-version", 0, "maxWireVersion reported to clients in handshake replies (0 = mirror upstream)")
	flag.DurationVar(&checkoutTimeout, "checkout-timeout", 10*time.Second, "Maximum time to wait for an upstream connection checkout (0 = unbounded)")
	flag.DurationVar(&shutdownTimeout, "shutdown-timeout", 30*time.Second, "Maximum time to wait for client connections to close on shutdown before force closing them")
	flag.BoolVar(&requireUpstreamTLS, "require-upstream-tls", false, "Refuse to start unless every upstream cluster is configured with TLS")
	flag.StringVar(&tlsCertFile, "tls-cert-file", "", "Server certificate (PEM) enabling TLS on the client listener")
	flag.StringVar(&tlsKeyFile, "tls-key-file", "", "Server private key (PEM) enabling TLS on the client listener")
	flag.StringVar(&tlsCAFile, "tls-ca-file", "", "CA bundle (PEM); when set, client certificates are required and verified (mTLS)")
	flag.StringVar(&authUsersFile, "auth-users-file", "", "File of proxy-only username:password lines enabling inbound SCRAM-SHA-256 authentication (empty = auth off)")
	flag.DurationVar(&authTimeout, "auth-timeout", 30*time.Second, "With -auth-users-file, close a connection that stays unauthenticated and idle this long (0 = never)")
	flag.BoolVar(&ping, "ping", false, "Ping downstream MongoDB before listening")
	flag.BoolVar(&pretty, "pretty", false, "Pretty print logging")
	flag.StringVar(&loglevel, "loglevel", "info", "One of: debug, info, warn, error, dpanic, panic, fatal")
	flag.StringVar(&dynamic, "dynamic", "", "File or URL to query for dynamic configuration")
	flag.BoolVar(&enableSdamMetrics, "enable-sdam-metrics", false, "Enable SDAM(Server Discovery And Monitoring) metrics")
	flag.BoolVar(&enableSdamLogging, "enable-sdam-logging", false, "Enable SDAM(Server Discovery And Monitoring) logging")

	flag.Parse()

	network = expandEnv(network)
	username = expandEnv(username)
	password = expandEnv(password)
	stats = expandEnv(stats)
	loglevel = expandEnv(loglevel)
	dynamic = expandEnv(dynamic)
	tlsCertFile = expandEnv(tlsCertFile)
	tlsKeyFile = expandEnv(tlsKeyFile)
	tlsCAFile = expandEnv(tlsCAFile)
	authUsersFile = expandEnv(authUsersFile)

	var tlsConfig *tls.Config
	if tlsCertFile != "" || tlsKeyFile != "" {
		var err error
		tlsConfig, err = proxy.LoadServerTLSConfig(tlsCertFile, tlsKeyFile, tlsCAFile)
		if err != nil {
			return nil, err
		}
	} else if tlsCAFile != "" {
		return nil, errors.New("-tls-ca-file requires -tls-cert-file and -tls-key-file")
	}

	level := zap.InfoLevel
	if loglevel != "" {
		err := level.Set(loglevel)
		if err != nil {
			return nil, fmt.Errorf("invalid loglevel: %s", loglevel)
		}
	}

	if authTimeout < 0 {
		return nil, errors.New("-auth-timeout must not be negative")
	}

	if !validNetwork(network) {
		return nil, fmt.Errorf("invalid network: %s", network)
	}

	addressMap := make(map[string]string)
	for _, arg := range flag.Args() {
		arg = expandEnv(arg)
		all := strings.FieldsFunc(arg, func(r rune) bool {
			return r == '|' || r == '\n'
		})
		for _, v := range all {
			split := strings.SplitN(v, "=", 2)
			if len(split) != 2 {
				return nil, errors.New("malformed address=uri option")
			}
			if _, ok := addressMap[split[0]]; ok {
				return nil, fmt.Errorf("uri already defined for address: %s", split[0])
			}
			addressMap[split[0]] = split[1]
		}
	}

	if len(addressMap) == 0 {
		return nil, errors.New("missing address=uri(s)")
	}

	loggerClient := newLogger(level, pretty)

	users, err := loadAuthUsers(loggerClient, authUsersFile)
	if err != nil {
		return nil, err
	}

	statsdClient, err := newStatsdClientInit(stats)
	if err != nil {
		return nil, err
	}

	var clients []client
	for address, uri := range addressMap {
		label, opts, err := clientOptions(uri, username, password)
		if err != nil {
			return nil, err
		}
		initMonitoring(label, opts, statsdClient, loggerClient, enableSdamMetrics, enableSdamLogging)
		clients = append(clients, client{
			address: address,
			label:   label,
			opts:    opts,
		})
	}

	if requireUpstreamTLS {
		if err := validateUpstreamTLS(clients); err != nil {
			return nil, err
		}
	}

	return &Config{
		network:         network,
		unlink:          unlink,
		ping:            ping,
		checkoutTimeout: checkoutTimeout,
		maxWireVersion:  int32(maxWireVersion),
		shutdownTimeout: shutdownTimeout,
		authTimeout:     authTimeout,
		pretty:          pretty,
		statsdaddr:      stats,
		clients:         clients,
		level:           level,
		dynamic:         dynamic,
		tlsConfig:       tlsConfig,
		users:           users,
		logger:          loggerClient,
		statsd:          statsdClient,
	}, nil
}

// loadAuthUsers loads the inbound auth users file; an empty path means auth is off (nil users). Passwords are never
// logged.
func loadAuthUsers(log *zap.Logger, path string) (*proxy.Users, error) {
	if path == "" {
		return nil, nil
	}
	users, err := proxy.LoadUsers(path)
	if err != nil {
		return nil, err
	}
	if users.GroupOrOtherAccessible() {
		log.Warn("Auth users file is accessible by group or other; restrict it to the owner (chmod 600)", zap.String("path", path))
	}
	log.Info("Inbound SCRAM-SHA-256 authentication enabled", zap.Int("users", users.Count()))
	return users, nil
}

// validateUpstreamTLS errors if any upstream client has no TLS configuration. The error names only the listen
// address, never the URI, which may carry credentials.
func validateUpstreamTLS(clients []client) error {
	for _, c := range clients {
		if c.opts == nil || c.opts.TLSConfig == nil {
			return fmt.Errorf("upstream TLS required but disabled for %s", c.address)
		}
	}
	return nil
}

func expandEnv(config string) string {
	// more restrictive version of os.ExpandEnv that only replaces exact matches of ${ENV}
	return regexp.MustCompile(`\${(\w+)}`).ReplaceAllStringFunc(config, func(s string) string {
		return os.ExpandEnv(s)
	})
}

func clientOptions(uri, username, password string) (string, *options.ClientOptions, error) {
	uri = uriWorkaround(uri, username)

	cs, err := connstring.Parse(uri)
	if err != nil {
		return "", nil, err
	}

	label := ""
	if len(cs.UnknownOptions["label"]) > 0 {
		label = cs.UnknownOptions["label"][0]
	}

	opts := options.Client()
	opts.ApplyURI(uri)

	if username != "" {
		if opts.Auth == nil {
			opts.SetAuth(options.Credential{Username: username})
		} else if opts.Auth.Username == "" || opts.Auth.Username == usernamePlaceholder {
			opts.Auth.Username = username
		}
	}

	if password != "" {
		if opts.Auth == nil {
			opts.SetAuth(options.Credential{Password: password})
		} else if opts.Auth.Password == "" {
			opts.Auth.Password = password
		}
	}

	if err := opts.Validate(); err != nil {
		return "", nil, err
	}

	return label, opts, nil
}

func initMonitoring(label string, opts *options.ClientOptions, statsd *statsd.Client, logger *zap.Logger, enableSdamMetrics bool, enableSdamLogging bool) *options.ClientOptions {
	// set up monitors for Pool and Server(SDAM)
	opts = opts.SetPoolMonitor(poolMonitor(label, statsd))
	opts = opts.SetServerMonitor(serverMonitoring(logger, statsd, enableSdamMetrics, enableSdamLogging))
	return opts
}

func uriWorkaround(uri, username string) string {
	// Workaround for a feature in the Mongo driver URI parsing where you can't set a URI
	// without setting the username ("error parsing uri: authsource without username is
	// invalid"). This method force-adds a username in the URI, which can be overridden
	// using SetAuth(). This workaround can be removed once the 1.4 driver is released
	// (see https://jira.mongodb.org/browse/GODRIVER-1473).
	if !strings.Contains(uri, "@") && username != "" {
		split := strings.SplitN(uri, "//", 2)
		if len(split) == 2 {
			uri = fmt.Sprintf("%s//%s@%s", split[0], usernamePlaceholder, split[1])
		} else {
			uri = fmt.Sprintf("%s@%s", usernamePlaceholder, split[0])
		}
	}
	return uri
}

func newStatsdClient(statsAddress string) (*statsd.Client, error) {
	return statsd.New(statsAddress, statsd.WithNamespace("mongobetween"))
}

func newLogger(level zapcore.Level, pretty bool) *zap.Logger {
	var c zap.Config
	if pretty {
		c = zap.NewDevelopmentConfig()
		c.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	} else {
		c = zap.NewProductionConfig()
	}

	c.EncoderConfig.MessageKey = "message"
	c.Level.SetLevel(level)

	log, err := c.Build(zap.AddStacktrace(zap.FatalLevel))
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}

	return log
}

func poolMonitor(label string, sd *statsd.Client) *event.PoolMonitor {
	defaultTags := []string{}
	if label != "" {
		defaultTags = append(defaultTags, fmt.Sprintf("cluster:%s", label))
	}

	checkedOut, checkedIn := util.StatsdBackgroundGauge(sd, "pool.checked_out_connections", defaultTags)
	opened, closed := util.StatsdBackgroundGauge(sd, "pool.open_connections", defaultTags)

	return &event.PoolMonitor{
		Event: func(e *event.PoolEvent) {
			snake := strings.ToLower(regexp.MustCompile("([a-z0-9])([A-Z])").ReplaceAllString(e.Type, "${1}_${2}"))
			name := fmt.Sprintf("pool_event.%s", snake)
			tags := []string{
				fmt.Sprintf("address:%s", e.Address),
				fmt.Sprintf("reason:%s", e.Reason),
			}
			tags = append(tags, defaultTags...)

			switch e.Type {
			case event.ConnectionCreated:
				opened(name, tags)
			case event.ConnectionClosed:
				closed(name, tags)
			case event.GetSucceeded:
				checkedOut(name, tags)
			case event.ConnectionReturned:
				checkedIn(name, tags)
			default:
				_ = sd.Incr(name, tags, 1)
			}
		},
	}
}

func serverMonitoring(log *zap.Logger, statsdClient *statsd.Client, enableSdamMetrics bool, enableSdamLogging bool) *event.ServerMonitor {

	return &event.ServerMonitor{
		ServerOpening: func(e *event.ServerOpeningEvent) {
			if enableSdamMetrics {
				_ = statsdClient.Incr("server_opening_event",
					[]string{
						fmt.Sprintf("address:%s", e.Address),
						fmt.Sprintf("topology_id:%s", e.TopologyID.Hex()),
					}, 0)
			}
		},

		ServerClosed: func(e *event.ServerClosedEvent) {
			if enableSdamMetrics {
				_ = statsdClient.Incr("server_closed_event",
					[]string{
						fmt.Sprintf("address:%s", e.Address),
						fmt.Sprintf("topology_id:%s", e.TopologyID.Hex()),
					}, 0)
			}
		},

		ServerDescriptionChanged: func(e *event.ServerDescriptionChangedEvent) {
			if enableSdamMetrics {
				_ = statsdClient.Incr("server_description_changed_event",
					[]string{
						fmt.Sprintf("address:%s", e.Address),
						fmt.Sprintf("topology_id:%s", e.TopologyID.Hex()),
					}, 0)
			}

			if enableSdamLogging {
				var prevDMap map[string]interface{}
				var newDMap map[string]interface{}

				prevDescription, _ := json.Marshal(&e.PreviousDescription)
				_ = json.Unmarshal(prevDescription, &prevDMap)
				newDescription, _ := json.Marshal(e.NewDescription)
				_ = json.Unmarshal(newDescription, &newDMap)

				log.Info("ServerDescriptionChangedEvent detected. ",
					zap.Any("address", e.Address),
					zap.String("topologyId", e.TopologyID.Hex()),
					zap.Any("prevDescription", prevDMap),
					zap.Any("newDescription", newDMap),
				)
			}
		},

		TopologyDescriptionChanged: func(e *event.TopologyDescriptionChangedEvent) {
			if enableSdamMetrics {
				_ = statsdClient.Incr("topology_description_changed_event",
					[]string{
						fmt.Sprintf("topology_id:%s", e.TopologyID.Hex()),
					}, 0)
			}
			if enableSdamLogging {
				var prevDMap map[string]interface{}
				var newDMap map[string]interface{}

				prevDescription, _ := json.Marshal(&e.PreviousDescription)
				_ = json.Unmarshal(prevDescription, &prevDMap)
				newDescription, _ := json.Marshal(e.NewDescription)
				_ = json.Unmarshal(newDescription, &newDMap)

				log.Info("TopologyDescriptionChangedEvent detected. ",
					zap.String("topologyId", e.TopologyID.Hex()),
					zap.Any("prevDescription", prevDMap),
					zap.Any("newDescription", newDMap),
				)
			}
		},

		TopologyOpening: func(e *event.TopologyOpeningEvent) {
			if enableSdamMetrics {
				_ = statsdClient.Incr("topology_opening_event",
					[]string{
						fmt.Sprintf("topology_id:%s", e.TopologyID.Hex()),
					}, 0)
			}
		},

		TopologyClosed: func(e *event.TopologyClosedEvent) {
			if enableSdamMetrics {
				_ = statsdClient.Incr("topology_closed_event",
					[]string{
						fmt.Sprintf("topology_id:%s", e.TopologyID.Hex()),
					}, 0)
			}
		},
	}
}
