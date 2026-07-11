package chasm

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewContextWithOperationIntent(t *testing.T) {
	t.Parallel()

	// A bare context carries no intent.
	require.Equal(t, OperationIntentUnspecified, operationIntentFromContext(context.Background()))

	// The exported helper sets the intent that validateAccess reads back from the context,
	// which is how the completion RPC handler requests OperationIntentProgress.
	ctx := NewContextWithOperationIntent(context.Background(), OperationIntentProgress)
	require.Equal(t, OperationIntentProgress, operationIntentFromContext(ctx))
}
