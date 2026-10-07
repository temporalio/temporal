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

			handler := &handler{matchingClient: matchingClient}
			require.Equal(t, test.granted, handler.grantEagerActivityDispatch(context.Background(), "namespace-id", request))
		})
	}
}

func TestEagerActivityDispatchCheck(t *testing.T) {
	request := &workflowservice.StartActivityExecutionRequest{Namespace: "namespace"}

	t.Run("disabled skips Matching", func(t *testing.T) {
		handler := &handler{config: &Config{
			EnableActivityEagerDispatchCheck: dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		}}
		require.True(t, handler.eagerActivityDispatchAllowed(context.Background(), "namespace-id", request))
	})

	t.Run("enabled falls back when Matching is unavailable", func(t *testing.T) {
		handler := &handler{config: &Config{
			EnableActivityEagerDispatchCheck: dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		}}
		require.False(t, handler.eagerActivityDispatchAllowed(context.Background(), "namespace-id", request))
	})
}
