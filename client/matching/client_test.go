package matching

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common/tqid"
)

func TestSetTaskQueuePartition(t *testing.T) {
	family, err := tqid.NewTaskQueueFamily("namespace-id", "task-queue")
	require.NoError(t, err)
	taskQueue := family.TaskQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	partitionProto := &taskqueuespb.TaskQueuePartition{}

	err = setTaskQueuePartition(partitionProto, taskQueue.RootPartition())
	require.NoError(t, err)
	require.Equal(t, int32(0), partitionProto.GetNormalPartitionId())
	require.NotNil(t, partitionProto.GetPartitionId())
	require.Equal(t, "task-queue", partitionProto.GetTaskQueue())
	require.Equal(t, enumspb.TASK_QUEUE_TYPE_ACTIVITY, partitionProto.GetTaskQueueType())

	err = setTaskQueuePartition(partitionProto, taskQueue.NormalPartition(3))
	require.NoError(t, err)
	require.Equal(t, int32(3), partitionProto.GetNormalPartitionId())

	err = setTaskQueuePartition(
		partitionProto,
		family.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).StickyPartition("sticky"),
	)
	var invalidArgument *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalidArgument)

	err = setTaskQueuePartition(
		partitionProto,
		family.TaskQueue(enumspb.TASK_QUEUE_TYPE_NEXUS).WorkerCommandsPartition(),
	)
	require.ErrorAs(t, err, &invalidArgument)
}
