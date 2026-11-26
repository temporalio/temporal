package faultinjection

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/testx/failpoint"
)

// TestFailpointErrors checks that failpoints can return the persistence errors by name.
func TestFailpointErrors(t *testing.T) {
	t.Parallel()

	for name, want := range map[string]error{
		"ShardOwnershipLost": &persistence.ShardOwnershipLostError{Msg: "injected"},
		"Timeout":            &persistence.TimeoutError{Msg: "injected"},
		"ConditionFailed":    &persistence.ConditionFailedError{Msg: "injected"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			newError, ok := failpoint.LookupError(name)
			require.True(t, ok)
			require.Equal(t, want, newError("injected"))
		})
	}
}
