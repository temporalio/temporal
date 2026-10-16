package frontend

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testlogger"
	"go.uber.org/mock/gomock"
)

func TestNexusStagePollCancellationFailure(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		taskType enumspb.TaskQueueType
		stage    string
	}{
		{enumspb.TASK_QUEUE_TYPE_NEXUS, "handler-task-delivery"},
		{enumspb.TASK_QUEUE_TYPE_WORKFLOW, ""},
	} {
		t.Run(tc.taskType.String(), func(t *testing.T) {
			t.Parallel()
			logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
			logger.Expect(testlogger.Warn, "Failed to cancel outstanding poller.")
			capture := logger.StartCapture()
			defer logger.StopCapture(capture)
			client := matchingservicemock.NewMockMatchingServiceClient(gomock.NewController(t))
			client.EXPECT().CancelOutstandingPoll(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewInternal("broken"))
			handler := &WorkflowHandler{logger: logger, matchingClient: client}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			require.True(t, handler.cancelOutstandingPoll(ctx, namespace.ID("namespace-id"), tc.taskType, &taskqueuepb.TaskQueue{Name: "queue"}, "poller"))
			records := capture.Snapshot()
			require.Len(t, records, 1)
			tags := map[string]any{}
			for _, tag := range records[0].Tags {
				tags[tag.Key()] = tag.Value()
			}
			if tc.stage == "" {
				require.NotContains(t, tags, "nexus-stage")
			} else {
				require.Equal(t, tc.stage, tags["nexus-stage"])
			}
		})
	}
}
