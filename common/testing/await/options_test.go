package await

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWithAttemptTimeoutOption(t *testing.T) {
	t.Setenv(attemptTimeoutEnvVar, "250ms")

	cfg := newConfig([]Option{WithAttemptTimeout(50 * time.Millisecond)})

	require.Equal(t, 50*time.Millisecond, cfg.attemptTimeout)
}

func TestWithAttemptTimeoutOptionIgnoresNonPositiveValues(t *testing.T) {
	t.Setenv(attemptTimeoutEnvVar, "250ms")

	cfg := newConfig([]Option{WithAttemptTimeout(0)})

	require.Equal(t, 250*time.Millisecond, cfg.attemptTimeout)
}

func TestWithPollIntervalOptions(t *testing.T) {
	cfg := newConfig([]Option{
		WithMinPollInterval(25 * time.Millisecond),
		WithMaxPollInterval(time.Second),
	})

	require.Equal(t, 25*time.Millisecond, cfg.minPollInterval)
	require.Equal(t, time.Second, cfg.maxPollInterval)
}
