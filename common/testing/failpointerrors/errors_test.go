package failpointerrors_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
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

	require.Subset(t, failpoint.ErrorNames(), []string{
		"Canceled", "DeadlineExceeded", "Internal", "InvalidArgument", "NotFound", "ResourceExhausted",
		"Unavailable",
	})
}
