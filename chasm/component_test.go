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

	t.Run("round-trips the intent read back by validateAccess", func(t *testing.T) {
		t.Parallel()
		// This is how the completion RPC handler requests OperationIntentProgress; validateAccess
		// reads it back via operationIntentFromContext. The downstream effect — a progress write to a
		// closed tree failing with errAccessCheckFailed (NotFound) — is covered by the "closed" cases
		// in tree_test.go's validateAccess tests.
		ctx := NewContextWithOperationIntent(context.Background(), OperationIntentProgress)
		require.Equal(t, OperationIntentProgress, operationIntentFromContext(ctx))
	})
}
