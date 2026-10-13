package await

import (
	"os"
	"time"

	"go.temporal.io/server/common/debug"
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
		attemptTimeout: envDuration(attemptTimeoutEnvVar, 10*time.Second) * debug.TimeoutMultiplier,
	}
}

func legacyConfig(timeout, pollInterval time.Duration, timeoutMsg string) config {
	cfg := newConfig()
	cfg.totalTimeout = timeout
	cfg.pollInterval = pollInterval
	cfg.timeoutMsg = timeoutMsg
	return cfg
}

func envDuration(name string, fallback time.Duration) time.Duration {
	if s := os.Getenv(name); s != "" {
		if d, err := time.ParseDuration(s); err == nil && d > 0 {
			return d
		}
	}
	return fallback
}
