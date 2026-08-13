package action_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/tests/umpire1/action"
)

// TestMutationCoverageComplete is the invalid-input exhaustiveness gate: every field of a
// mutation-covered request must be either enumerated by descriptor reflection or a consciously
// deferred kind. Like TestKitchensinkMappingsComplete it runs with the unit tests, before any
// functional drive, so a new request field of an unhandled kind fails fast.
func TestMutationCoverageComplete(t *testing.T) {
	require.NoError(t, action.ValidateMutationCoverage())
}
