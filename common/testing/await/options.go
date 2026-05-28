package await

import (
	"fmt"
	"time"
)

// Option configures a Require2 / RequireTrue2 call.
type Option func(*config)

// WithTimeout sets the total time the polling loop will run before failing.
// Default: 30s, or TEMPORAL_TEST_TIMEOUT when set. Capped at the parent
// context and the testcontext test timeout cap.
func WithTimeout(d time.Duration) Option {
	return func(c *config) { c.totalTimeout = d }
}

// WithMinPollInterval sets the first wait between attempts after a failure.
// Default: 500ms.
func WithMinPollInterval(d time.Duration) Option {
	return func(c *config) { c.minPollInterval = d }
}

// WithMaxPollInterval caps exponential poll backoff. Default: 2s.
func WithMaxPollInterval(d time.Duration) Option {
	return func(c *config) { c.maxPollInterval = d }
}

// WithAttemptTimeout caps a single attempt. Default: 10s, or
// TEMPORAL_AWAIT_ATTEMPT_TIMEOUT when set. If the attempt exceeds this, its
// context is canceled and the loop proceeds to the next poll. Non-positive
// values are ignored because attempts always have a timeout.
func WithAttemptTimeout(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.attemptTimeout = d
		}
	}
}

// WithMessagef attaches a formatted message to the timeout failure.
func WithMessagef(format string, args ...any) Option {
	return func(c *config) { c.timeoutMsg = fmt.Sprintf(format, args...) }
}
