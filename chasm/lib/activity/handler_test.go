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
	priority := &commonpb.Priority{PriorityKey: 2, FairnessKey: "fairness-key"}
	request := &workflowservice.StartActivityExecutionRequest{
		Namespace: "namespace",
		TaskQueue: &taskqueuepb.TaskQueue{Name: "activity-task-queue"},
		Priority:  priority,
	}

	testCases := []struct {
		name     string
		response *matchingservice.GrantEagerDispatchResponse
		err      error
		granted  bool
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
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			controller := gomock.NewController(t)
			matchingClient := matchingservicemock.NewMockMatchingServiceClient(controller)
			metricsHandler := metricstest.NewCaptureHandler()
			capture := metricsHandler.StartCapture()
			defer metricsHandler.StopCapture(capture)
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

			handler := &handler{
				config: &Config{
					BreakdownMetricsByTaskQueue: dynamicconfig.GetBoolPropertyFnFilteredByTaskQueue(true),
				},
				matchingClient: matchingClient,
				metricsHandler: metricsHandler,
			}
			require.Equal(t, test.granted, handler.grantEagerActivityDispatch(context.Background(), "namespace-id", request))

			recordings := capture.SnapshotMetric(metrics.EagerDispatchRequestsSent.Name())
			require.Len(t, recordings, 1)
			require.EqualValues(t, 1, recordings[0].Value)
			require.Equal(t, "namespace", recordings[0].Tags[metrics.NamespaceTag("").Key])
			require.Equal(t, "activity-task-queue", recordings[0].Tags[metrics.UnsafeTaskQueueTag("").Key])
			require.Equal(t, "StartActivityExecution", recordings[0].Tags[metrics.OperationTag("").Key])
		})
	}

	t.Run("no matching client", func(t *testing.T) {
		metricsHandler := metricstest.NewCaptureHandler()
		capture := metricsHandler.StartCapture()
		defer metricsHandler.StopCapture(capture)
		handler := &handler{metricsHandler: metricsHandler}

		require.False(t, handler.grantEagerActivityDispatch(context.Background(), "namespace-id", request))
		require.Empty(t, capture.SnapshotMetric(metrics.EagerDispatchRequestsSent.Name()))
	})
}
