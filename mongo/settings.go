package mongo

import "time"

// Settings are per-upstream options that are not part of the driver's client options.
type Settings struct {
	// Ping the upstream before Connect returns.
	Ping bool
	// MaxWireVersion, when > 0, is reported to clients instead of mirroring the upstream.
	MaxWireVersion int32
	// CheckoutTimeout bounds how long a request waits for an upstream connection (0 = unbounded).
	CheckoutTimeout time.Duration
}
