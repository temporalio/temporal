package await

import (
	"os"
	"time"

	"go.temporal.io/server/common/debug"
)

const (
	attemptTimeoutEnvVar = "TEMPORAL_AWAIT_ATTEMPT_TIMEOUT"
	minPollInterval      = 500 * time.Millisecond
	maxPollInterval      = 2 * time.Second
)

type config struct {
	totalTimeout   time.Duration
	attemptTimeout time.Duration
	timeoutMsg     string
}

func newConfig() config {
	return config{
		attemptTimeout: envDuration(attemptTimeoutEnvVar, 10*time.Second) * debug.TimeoutMultiplier,
	}
}

func legacyConfig(timeout, _ time.Duration, timeoutMsg string) config {
	cfg := newConfig()
	cfg.totalTimeout = timeout
	cfg.timeoutMsg = timeoutMsg
	return cfg
}

func nextPollInterval(attempt int) time.Duration {
	switch attempt {
	case 1:
		return minPollInterval
	case 2:
		return time.Second
	default:
		return maxPollInterval
	}
}

func envDuration(name string, fallback time.Duration) time.Duration {
	if s := os.Getenv(name); s != "" {
		if d, err := time.ParseDuration(s); err == nil && d > 0 {
			return d
		}
	}
	return fallback
}
