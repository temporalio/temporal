package chasm

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewContextWithOperationIntent(t *testing.T) {
	t.Parallel()

	t.Run("bare context carries no intent", func(t *testing.T) {
		t.Parallel()
		require.Equal(t, OperationIntentUnspecified, operationIntentFromContext(context.Background()))
	})

	t.Run("round-trips the set intent", func(t *testing.T) {
		t.Parallel()
		ctx := NewContextWithOperationIntent(context.Background(), OperationIntentProgress)
		require.Equal(t, OperationIntentProgress, operationIntentFromContext(ctx))
	})
}
