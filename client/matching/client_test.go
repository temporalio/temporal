package matching

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/tqid"
)

func TestGrantEagerDispatchRequestForPartition(t *testing.T) {
	family, err := tqid.NewTaskQueueFamily("namespace-id", "task-queue")
	require.NoError(t, err)
	taskQueue := family.TaskQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	request := &matchingservice.GrantEagerDispatchRequest{
		NamespaceId: "namespace-id",
		Items:       []*matchingservice.GrantEagerDispatchRequest_Item{{Count: 1}},
	}

	rootRequest, err := grantEagerDispatchRequestForPartition(request, taskQueue.RootPartition())
	require.NoError(t, err)
	require.Equal(t, int32(0), rootRequest.GetTaskQueuePartition().GetNormalPartitionId())
	require.NotNil(t, rootRequest.GetTaskQueuePartition().GetPartitionId())
	require.Equal(t, "task-queue", rootRequest.GetTaskQueuePartition().GetTaskQueue())
	require.Equal(t, enumspb.TASK_QUEUE_TYPE_ACTIVITY, rootRequest.GetTaskQueuePartition().GetTaskQueueType())

	childRequest, err := grantEagerDispatchRequestForPartition(request, taskQueue.NormalPartition(3))
	require.NoError(t, err)
	require.Equal(t, int32(3), childRequest.GetTaskQueuePartition().GetNormalPartitionId())
	require.Nil(t, request.GetTaskQueuePartition())

	_, err = grantEagerDispatchRequestForPartition(
		request,
		family.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).StickyPartition("sticky"),
	)
	var invalidArgument *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalidArgument)

	_, err = grantEagerDispatchRequestForPartition(
		request,
		family.TaskQueue(enumspb.TASK_QUEUE_TYPE_NEXUS).WorkerCommandsPartition(),
	)
	require.ErrorAs(t, err, &invalidArgument)
}
