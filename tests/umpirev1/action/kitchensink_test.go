package action_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/tests/umpirev1/action"
)

// TestKitchensinkMappingsComplete is the exhaustiveness gate: every WorkerCommand action the
// registry can produce must have a kitchensink mapping. It runs with the unit tests, before any
// functional drive, so a WorkerCommand action added without its mapping fails fast.
func TestKitchensinkMappingsComplete(t *testing.T) {
	require.NoError(t, action.ValidateKitchensinkMappings())
}
