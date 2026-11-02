package logtags_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/rpc/interceptor/logtags"
	"go.temporal.io/server/common/tasktoken"
)

func TestNexusStage(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		method string
		req    any
		want   []tag.Tag
	}{
		// Handler Task Delivery
		{method: api.WorkflowServicePrefix + "PollNexusTaskQueue", want: []tag.Tag{tag.NexusStageHandlerTaskDelivery}},
		{method: api.WorkflowServicePrefix + "RespondNexusTaskCompleted", want: []tag.Tag{tag.NexusStageHandlerTaskDelivery}},
		{method: api.WorkflowServicePrefix + "RespondNexusTaskFailed", want: []tag.Tag{tag.NexusStageHandlerTaskDelivery}},
		{method: api.MatchingServicePrefix + "DispatchNexusTask", want: []tag.Tag{tag.NexusStageHandlerTaskDelivery}},
		{method: api.MatchingServicePrefix + "PollNexusTaskQueue", want: []tag.Tag{tag.NexusStageHandlerTaskDelivery}},
		{method: api.MatchingServicePrefix + "RespondNexusTaskCompleted", want: []tag.Tag{tag.NexusStageHandlerTaskDelivery}},
		{method: api.MatchingServicePrefix + "RespondNexusTaskFailed", want: []tag.Tag{tag.NexusStageHandlerTaskDelivery}},
		{
			method: api.MatchingServicePrefix + "CancelOutstandingPoll",
			req:    &matchingservice.CancelOutstandingPollRequest{TaskQueueType: enumspb.TASK_QUEUE_TYPE_NEXUS},
			want:   []tag.Tag{tag.NexusStageHandlerTaskDelivery},
		},
		// Handler Inbound
		{method: api.HistoryServicePrefix + "StartNexusOperation", want: []tag.Tag{tag.NexusStageHandlerInbound}},
		{method: api.HistoryServicePrefix + "CancelNexusOperation", want: []tag.Tag{tag.NexusStageHandlerInbound}},
		{method: api.NexusServicePrefix + "DispatchByEndpoint", want: []tag.Tag{tag.NexusStageHandlerInbound}},
		{method: api.NexusServicePrefix + "DispatchByNamespaceAndTaskQueue", want: []tag.Tag{tag.NexusStageHandlerInbound}},
		// Caller Inbound
		{method: api.HistoryServicePrefix + "CompleteNexusOperation", want: []tag.Tag{tag.NexusStageCallerInbound}},
		{method: api.HistoryServicePrefix + "CompleteNexusOperationChasm", want: []tag.Tag{tag.NexusStageCallerInbound}},
		{method: api.NexusServicePrefix + "CompleteNexusOperation", want: []tag.Tag{tag.NexusStageCallerInbound}},
		// No stage
		{
			method: api.MatchingServicePrefix + "CancelOutstandingPoll",
			req:    &matchingservice.CancelOutstandingPollRequest{TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW},
		},
		{method: api.WorkflowServicePrefix + "StartWorkflowExecution"},
		// Standalone operation APIs are client RPCs, not part of a Nexus call's lifecycle.
		{method: api.WorkflowServicePrefix + "StartNexusOperationExecution"},
	} {
		t.Run(tc.method, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, logtags.NexusStage(tc.req, tc.method))
		})
	}
}

func TestExtractIncludesNexusStage(t *testing.T) {
	t.Parallel()

	wt := logtags.NewWorkflowTags(tasktoken.NewSerializer(), log.NewTestLogger())
	tags := wt.Extract(&historyservice.StartNexusOperationRequest{}, api.HistoryServicePrefix+"StartNexusOperation")
	require.Contains(t, tags, tag.NexusStageHandlerInbound)
}
