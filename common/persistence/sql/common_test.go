package sql

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
)

func TestConvertSQLError(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		err   error
		cause error
	}{
		{name: "canceled", err: fmt.Errorf("lock: %w", context.Canceled), cause: context.Canceled},
		{name: "deadline exceeded", err: fmt.Errorf("query: %w", context.DeadlineExceeded), cause: context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := convertSQLError("Failed to lock task queue", tc.err)
			require.ErrorIs(t, err, tc.cause)
			require.ErrorContains(t, err, "Failed to lock task queue")
			var unavailable *serviceerror.Unavailable
			require.NotErrorAs(t, err, &unavailable)
		})
	}
}

func TestConvertSQLError_WrapsOtherErrorsAsUnavailable(t *testing.T) {
	t.Parallel()

	err := convertSQLError("Failed to lock task queue", errors.New("connection reset"))
	var unavailable *serviceerror.Unavailable
	require.ErrorAs(t, err, &unavailable)
	require.ErrorContains(t, err, "Failed to lock task queue")
	require.ErrorContains(t, err, "connection reset")
}
