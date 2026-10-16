package activity

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func TestGrantEagerActivityDispatch(t *testing.T) {
	t.Parallel()

	priority := &commonpb.Priority{PriorityKey: 2, FairnessKey: "fairness-key"}
	request := &workflowservice.StartActivityExecutionRequest{
		Namespace: "namespace",
		TaskQueue: &taskqueuepb.TaskQueue{Name: "activity-task-queue"},
		Priority:  priority,
	}

	testCases := []struct {
		name                    string
		response                *matchingservice.GrantEagerDispatchResponse
		err                     error
		granted                 bool
		noClient                bool
		disableTaskQueueMetrics bool
	}{
		{
			name: "granted",
			response: &matchingservice.GrantEagerDispatchResponse{Items: []*matchingservice.GrantEagerDispatchResponse_Item{
				{GrantedCount: 1},
			}},
			granted: true,
		},
		{
			name: "denied",
			response: &matchingservice.GrantEagerDispatchResponse{Items: []*matchingservice.GrantEagerDispatchResponse_Item{
				{},
			}},
		},
		{
			name: "matching error",
			err:  errors.New("matching unavailable"),
		},
		{
			name:     "missing response item",
			response: &matchingservice.GrantEagerDispatchResponse{},
		},
		{
			name:     "missing client",
			noClient: true,
		},
		{
			name: "task queue metrics omitted",
			response: &matchingservice.GrantEagerDispatchResponse{Items: []*matchingservice.GrantEagerDispatchResponse_Item{
				{GrantedCount: 1},
			}},
			granted:                 true,
			disableTaskQueueMetrics: true,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			metricsHandler := metricstest.NewCaptureHandler()
			capture := metricsHandler.StartCapture()
			t.Cleanup(func() { metricsHandler.StopCapture(capture) })
			h := &handler{
				metricsHandler: metricsHandler,
				config: &Config{
					BreakdownMetricsByTaskQueue: dynamicconfig.GetBoolPropertyFnFilteredByTaskQueue(!test.disableTaskQueueMetrics),
				},
			}
			if test.noClient {
				require.False(t, h.grantEagerActivityDispatch(t.Context(), "namespace-id", request))
				require.Empty(t, capture.SnapshotMetric(metrics.EagerDispatchRequestsSent.Name()))
				return
			}

			controller := gomock.NewController(t)
			matchingClient := matchingservicemock.NewMockMatchingServiceClient(controller)
			matchingClient.EXPECT().GrantEagerDispatch(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, actual *matchingservice.GrantEagerDispatchRequest, _ ...grpc.CallOption) (*matchingservice.GrantEagerDispatchResponse, error) {
					require.Equal(t, "namespace-id", actual.GetNamespaceId())
					require.Equal(t, "activity-task-queue", actual.GetTaskQueuePartition().GetTaskQueue())
					require.Equal(t, enumspb.TASK_QUEUE_TYPE_ACTIVITY, actual.GetTaskQueuePartition().GetTaskQueueType())
					require.Len(t, actual.GetItems(), 1)
					require.EqualValues(t, 1, actual.GetItems()[0].GetCount())
					require.True(t, proto.Equal(priority, actual.GetItems()[0].GetPriority()))
					return test.response, test.err
				},
			)

			h.matchingClient = matchingClient
			require.Equal(t, test.granted, h.grantEagerActivityDispatch(t.Context(), "namespace-id", request))

			requestMetrics := capture.SnapshotMetric(metrics.EagerDispatchRequestsSent.Name())
			require.Len(t, requestMetrics, 1)
			require.Equal(t, int64(1), requestMetrics[0].Value)
			expectedTaskQueue := "activity-task-queue"
			if test.disableTaskQueueMetrics {
				expectedTaskQueue = "__omitted__"
			}
			require.Equal(t, map[string]string{
				metrics.NamespaceTag("").Key:       "namespace",
				metrics.UnsafeTaskQueueTag("").Key: expectedTaskQueue,
				metrics.OperationTag("").Key:       "StartActivityExecution",
			}, requestMetrics[0].Tags)
		})
	}
}
