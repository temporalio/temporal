package tasktoken

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWorkerTaskID(t *testing.T) {
	t.Parallel()

	require.Equal(t, "workflow/namespace-id/run-id/42", WorkflowWorkerTaskID("namespace-id", "run-id", 42))
	require.Equal(t, "activity/namespace-id/run-id/42", ActivityWorkerTaskID("namespace-id", "run-id", 42))
	require.Equal(t, "query/namespace-id/query-id", QueryWorkerTaskID("namespace-id", "query-id"))
	require.Equal(t, "nexus/namespace-id/nexus-id", NexusWorkerTaskID("namespace-id", "nexus-id"))
}
