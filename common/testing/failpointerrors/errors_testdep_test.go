//go:build test_dep

package failpointerrors_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"
	_ "go.temporal.io/server/common/testing/failpointerrors"
	"go.temporal.io/testx/failpoint"
)

func TestCatalog(t *testing.T) {
	t.Parallel()

	newError, ok := failpoint.LookupError("Unavailable")
	require.True(t, ok)
	err := newError("injected")
	_, isUnavailable := errors.AsType[*serviceerror.Unavailable](err)
	require.True(t, isUnavailable)

	newError, ok = failpoint.LookupError("ShardOwnershipLost")
	require.True(t, ok)
	err = newError("injected")
	require.Equal(t, &persistence.ShardOwnershipLostError{Msg: "injected"}, err)

	require.Subset(t, failpoint.ErrorNames(), []string{
		"Canceled", "ConditionFailed", "DeadlineExceeded", "Internal", "InvalidArgument",
		"NotFound", "ResourceExhausted", "ShardOwnershipLost", "Timeout", "Unavailable",
	})
}
