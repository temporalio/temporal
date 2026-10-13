package eagerdispatch

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

func TestGrant(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := matchingservicemock.NewMockMatchingServiceClient(ctrl)
	items := []*matchingservice.GrantEagerDispatchRequest_Item{
		{Count: 1, Priority: &commonpb.Priority{PriorityKey: 1}},
		{Count: 1, Priority: &commonpb.Priority{PriorityKey: 2}},
	}
	expectedResponse := &matchingservice.GrantEagerDispatchResponse{
		Items: []*matchingservice.GrantEagerDispatchResponse_Item{{GrantedCount: 1}, {GrantedCount: 1}},
	}

	client.EXPECT().GrantEagerDispatch(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *matchingservice.GrantEagerDispatchRequest, _ ...grpc.CallOption) (*matchingservice.GrantEagerDispatchResponse, error) {
			require.Equal(t, "namespace-id", request.GetNamespaceId())
			require.Equal(t, "task-queue", request.GetTaskQueuePartition().GetTaskQueue())
			require.Equal(t, enumspb.TASK_QUEUE_TYPE_ACTIVITY, request.GetTaskQueuePartition().GetTaskQueueType())
			require.Same(t, items[0], request.GetItems()[0])
			require.Same(t, items[1], request.GetItems()[1])
			return expectedResponse, nil
		},
	)

	response, err := Grant(context.Background(), client, "namespace-id", "task-queue", items)
	require.NoError(t, err)
	require.Same(t, expectedResponse, response)
}
