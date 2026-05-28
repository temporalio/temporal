package await

import (
	"os"
	"time"

	"go.temporal.io/server/common/debug"
)

const (
	totalTimeoutEnvVar    = "TEMPORAL_TEST_TIMEOUT"
	attemptTimeoutEnvVar  = "TEMPORAL_AWAIT_ATTEMPT_TIMEOUT"
	minPollIntervalEnvVar = "TEMPORAL_AWAIT_MIN_POLL_INTERVAL"
	maxPollIntervalEnvVar = "TEMPORAL_AWAIT_MAX_POLL_INTERVAL"
)

type config struct {
	totalTimeout    time.Duration
	minPollInterval time.Duration
	maxPollInterval time.Duration
	attemptTimeout  time.Duration
	timeoutMsg      string
}

func newConfig(opts []Option) config {
	c := config{
		totalTimeout:    envDuration(totalTimeoutEnvVar, 30*time.Second) * debug.TimeoutMultiplier,
		minPollInterval: envDuration(minPollIntervalEnvVar, 500*time.Millisecond),
		maxPollInterval: envDuration(maxPollIntervalEnvVar, 2*time.Second),
		attemptTimeout:  envDuration(attemptTimeoutEnvVar, 10*time.Second) * debug.TimeoutMultiplier,
	}
	for _, o := range opts {
		o(&c)
	}
	return c
}

func legacyConfig(timeout, pollInterval time.Duration, timeoutMsg string) config {
	cfg := newConfig(nil)
	cfg.totalTimeout = timeout
	cfg.minPollInterval = pollInterval
	cfg.maxPollInterval = pollInterval
	cfg.timeoutMsg = timeoutMsg
	return cfg
}

func (c config) nextPollInterval(attempt int) time.Duration {
	if c.minPollInterval <= 0 || attempt <= 1 {
		return c.minPollInterval
	}
	if c.maxPollInterval <= 0 || c.maxPollInterval < c.minPollInterval {
		return c.minPollInterval
	}
	interval := c.minPollInterval
	for i := 1; i < attempt; i++ {
		if interval >= c.maxPollInterval/2 {
			return c.maxPollInterval
		}
		interval *= 2
	}
	if interval > c.maxPollInterval {
		return c.maxPollInterval
	}
	return interval
}

func envDuration(name string, fallback time.Duration) time.Duration {
	if s := os.Getenv(name); s != "" {
		if d, err := time.ParseDuration(s); err == nil && d > 0 {
			return d
		}
	}
	return fallback
}
