package tasktoken

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWorkerTaskID(t *testing.T) {
	t.Parallel()

	require.Equal(t, "v1/workflow/namespace-id/run-id/42", WorkflowWorkerTaskID("namespace-id", "run-id", 42))
	require.Equal(t, "v1/activity/namespace-id/run-id/42", ActivityWorkerTaskID("namespace-id", "run-id", 42))
	require.Equal(t, "v1/query/namespace-id/query-id", QueryWorkerTaskID("namespace-id", "query-id"))
	require.Equal(t, "v1/nexus/namespace-id/nexus-id", NexusWorkerTaskID("namespace-id", "nexus-id"))
}
