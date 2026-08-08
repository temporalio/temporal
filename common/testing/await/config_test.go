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

func TestConfig_OverridePostAwaitTimeoutReserve(t *testing.T) {
	t.Setenv(postAwaitTimeoutReserveEnvVar, "750ms")

	require.Equal(t, 750*time.Millisecond, postAwaitTimeoutReserve())
}
