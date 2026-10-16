package matching

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/matchingservice/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
)

func TestGrantEagerDispatchRejectsUnsupportedPartitions(t *testing.T) {
	partitions := map[string]*taskqueuespb.TaskQueuePartition{
		"sticky": {
			TaskQueue:     "task-queue",
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			PartitionId: &taskqueuespb.TaskQueuePartition_StickyName{
				StickyName: "sticky",
			},
		},
		"worker commands": {
			TaskQueue:     "task-queue",
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_NEXUS,
			PartitionId: &taskqueuespb.TaskQueuePartition_WorkerCommands{
				WorkerCommands: &taskqueuespb.WorkerCommandsPartitionId{},
			},
		},
	}

	for name, partition := range partitions {
		t.Run(name, func(t *testing.T) {
			engine := &matchingEngineImpl{}
			_, err := engine.GrantEagerDispatch(context.Background(), &matchingservice.GrantEagerDispatchRequest{
				NamespaceId:        "namespace-id",
				TaskQueuePartition: partition,
			})
			var invalidArgument *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgument)
		})
	}
}
