package matching

import (
	"testing"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

func TestWorkerTaskID(t *testing.T) {
	t.Parallel()

	task := &persistencespb.TaskInfo{
		NamespaceId:      "namespace-id",
		RunId:            "run-id",
		ScheduledEventId: 42,
	}
	require.Equal(t, "v1/workflow/namespace-id/run-id/42", workflowWorkerTaskID(task))
	require.Equal(t, "v1/activity/namespace-id/run-id/42", activityWorkerTaskID(task))
	require.Equal(t, "v1/query/namespace-id/query-id", queryWorkerTaskID("namespace-id", "query-id"))
	require.Equal(t, "v1/nexus/namespace-id/nexus-id", nexusWorkerTaskID("namespace-id", "nexus-id"))
}
