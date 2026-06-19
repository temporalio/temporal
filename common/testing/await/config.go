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
		attemptTimeout: effectiveEnvDuration(attemptTimeoutEnvVar, 10*time.Second),
	}
}

func legacyConfig(timeout, pollInterval time.Duration, timeoutMsg string) config {
	cfg := newConfig()
	cfg.totalTimeout = timeout
	cfg.pollInterval = pollInterval
	cfg.timeoutMsg = timeoutMsg
	return cfg
}

func effectiveEnvDuration(name string, defaultTimeout time.Duration) (timeout time.Duration) {
	defer func() {
		timeout *= debug.TimeoutMultiplier
	}()

	if s := os.Getenv(name); s != "" {
		if d, err := time.ParseDuration(s); err == nil && d > 0 {
			return d
		}
	}
	return defaultTimeout
}
