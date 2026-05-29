package await

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/debug"
)

func TestNewConfigDefaults(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		t.Setenv(totalTimeoutEnvVar, "")
		t.Setenv(attemptTimeoutEnvVar, "")
		t.Setenv(minPollIntervalEnvVar, "")
		t.Setenv(maxPollIntervalEnvVar, "")

		cfg := newConfig(nil)
		require.Equal(t, 30*time.Second*debug.TimeoutMultiplier, cfg.totalTimeout)
		require.Equal(t, 10*time.Second*debug.TimeoutMultiplier, cfg.attemptTimeout)
		require.Equal(t, 500*time.Millisecond, cfg.minPollInterval)
		require.Equal(t, 2*time.Second, cfg.maxPollInterval)
	})

	t.Run("env overrides", func(t *testing.T) {
		t.Setenv(totalTimeoutEnvVar, "90s")
		t.Setenv(attemptTimeoutEnvVar, "10s")
		t.Setenv(minPollIntervalEnvVar, "250ms")
		t.Setenv(maxPollIntervalEnvVar, "2s")

		cfg := newConfig(nil)
		require.Equal(t, 90*time.Second*debug.TimeoutMultiplier, cfg.totalTimeout)
		require.Equal(t, 10*time.Second*debug.TimeoutMultiplier, cfg.attemptTimeout)
		require.Equal(t, 250*time.Millisecond, cfg.minPollInterval)
		require.Equal(t, 2*time.Second, cfg.maxPollInterval)
	})
}

func TestNextPollIntervalBacksOffToCap(t *testing.T) {
	base := 500 * time.Millisecond
	cfg := config{
		minPollInterval: base,
		maxPollInterval: 2 * time.Second,
	}

	require.Equal(t, 500*time.Millisecond, cfg.nextPollInterval(1))
	require.Equal(t, time.Second, cfg.nextPollInterval(2))
	require.Equal(t, 2*time.Second, cfg.nextPollInterval(3))
	require.Equal(t, 2*time.Second, cfg.nextPollInterval(4))
	require.Equal(t, 2*time.Second, cfg.nextPollInterval(20))

	cfg.maxPollInterval = base
	require.Equal(t, base, cfg.nextPollInterval(20))
}
