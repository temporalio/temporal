package failpoints_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/failpoints"
)

func TestPersistenceKey(t *testing.T) {
	t.Parallel()

	key := failpoints.PersistenceKey("ExecutionStore", "UpdateWorkflowExecution")
	require.Equal(t, "persistence:ExecutionStore.UpdateWorkflowExecution", key.Name())
	require.Empty(t, key.Scope())
}
