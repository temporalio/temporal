package await

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConfig_OverrideAttemptTimeout(t *testing.T) {
	t.Setenv(attemptTimeoutEnvVar, "250ms")

	cfg := newConfig()
	require.Equal(t, 250*time.Millisecond, cfg.attemptTimeout)
}

func TestNextPollIntervalCapsAtMaximum(t *testing.T) {
	require.Equal(t, 500*time.Millisecond, nextPollInterval(500*time.Millisecond, 1))
	require.Equal(t, time.Second, nextPollInterval(500*time.Millisecond, 2))
	require.Equal(t, 2*time.Second, nextPollInterval(500*time.Millisecond, 3))
	require.Equal(t, 2*time.Second, nextPollInterval(500*time.Millisecond, 20))
}
