package logtags

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/log/tag"
)

func TestNexusStage(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		method string
		req    any
		want   []tag.Tag
	}{
		{method: api.HistoryServicePrefix + "StartNexusOperation", want: []tag.Tag{tag.NexusStageHandlerInbound}},
		{method: api.HistoryServicePrefix + "CancelNexusOperation", want: []tag.Tag{tag.NexusStageHandlerInbound}},
		{method: api.NexusServicePrefix + "DispatchByEndpoint", want: []tag.Tag{tag.NexusStageHandlerInbound}},
		{method: api.NexusServicePrefix + "DispatchByNamespaceAndTaskQueue", want: []tag.Tag{tag.NexusStageHandlerInbound}},
		{method: api.WorkflowServicePrefix + "StartWorkflowExecution"},
		// Standalone operation management APIs are client RPCs, not part of a Nexus call's lifecycle.
		{method: api.WorkflowServicePrefix + "StartNexusOperationExecution"},
	} {
		t.Run(tc.method, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, NexusStage(tc.req, tc.method))
		})
	}
}
