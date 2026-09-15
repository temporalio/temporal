package sql

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
)

func TestConvertSQLError_PreservesContextErrors(t *testing.T) {
	t.Parallel()

	canceled := fmt.Errorf("lock: %w", context.Canceled)
	deadline := fmt.Errorf("query: %w", context.DeadlineExceeded)

	err := convertSQLError("Failed to lock task queue", canceled)
	require.ErrorIs(t, err, context.Canceled)
	var unavailable *serviceerror.Unavailable
	require.NotErrorAs(t, err, &unavailable, "canceled must not be wrapped as Unavailable")

	err = convertSQLError("Failed to lock task queue", deadline)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NotErrorAs(t, err, &unavailable, "deadline exceeded must not be wrapped as Unavailable")
}

func TestConvertSQLError_WrapsOtherErrorsAsUnavailable(t *testing.T) {
	t.Parallel()

	err := convertSQLError("Failed to lock task queue", errors.New("connection reset"))
	var unavailable *serviceerror.Unavailable
	require.ErrorAs(t, err, &unavailable)
	require.Contains(t, err.Error(), "Failed to lock task queue")
	require.Contains(t, err.Error(), "connection reset")
}

func TestConvertSQLError_Nil(t *testing.T) {
	t.Parallel()
	err := convertSQLError("Failed to lock task queue", nil)
	require.NoError(t, err)
}
