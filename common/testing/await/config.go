package await

import (
	"os"
	"time"

	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/testing/testcontext"
)

const attemptTimeoutEnvVar = "TEMPORAL_AWAIT_ATTEMPT_TIMEOUT"

type config struct {
	totalTimeout   time.Duration
	pollInterval   time.Duration
	attemptTimeout time.Duration
	timeoutMsg     string
}

func newConfig() config {
	return config{
		totalTimeout:   testcontext.DefaultTimeout(),
		attemptTimeout: envDuration(attemptTimeoutEnvVar, 10*time.Second) * debug.TimeoutMultiplier,
	}
}

func legacyConfig(pollInterval time.Duration, timeoutMsg string) config {
	cfg := newConfig()
	cfg.pollInterval = pollInterval
	cfg.timeoutMsg = timeoutMsg
	return cfg
}

func nextPollInterval(base time.Duration, attempt int) time.Duration {
	interval := min(base, 2*time.Second)
	if interval <= 0 {
		return interval
	}
	for range attempt - 1 {
		if interval >= time.Second {
			return 2 * time.Second
		}
		interval *= 2
	}
	return min(interval, 2*time.Second)
}

func envDuration(name string, fallback time.Duration) time.Duration {
	if s := os.Getenv(name); s != "" {
		if d, err := time.ParseDuration(s); err == nil && d > 0 {
			return d
		}
	}
	return fallback
}
