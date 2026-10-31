package await

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/internal/debug"
)

func TestConfig_OverrideAttemptTimeout(t *testing.T) {
	t.Setenv(attemptTimeoutEnvVar, "250ms")

	cfg := newConfig()
	require.Equal(t, 250*time.Millisecond*debug.TimeoutMultiplier, cfg.attemptTimeout)
}
