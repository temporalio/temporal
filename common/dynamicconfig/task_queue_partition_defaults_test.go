package dynamicconfig

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/primitives"
)

func TestDefaultNumTaskQueuePartitions(t *testing.T) {
	require.Equal(t, 12, GlobalDefaultNumTaskQueuePartitions)
	require.Equal(t, []TypedConstrainedValue[int]{
		{
			Constraints: Constraints{
				TaskQueueName: primitives.PerNSWorkerTaskQueue,
			},
			Value: 1,
		},
		{
			Constraints: Constraints{
				TaskQueueName: primitives.AddSearchAttributesActivityTQ,
				Namespace:     primitives.SystemLocalNamespace,
			},
			Value: 1,
		},
		{
			Constraints: Constraints{
				TaskQueueName: primitives.DeleteNamespaceActivityTQ,
				Namespace:     primitives.SystemLocalNamespace,
			},
			Value: 1,
		},
		{
			Constraints: Constraints{
				TaskQueueName: primitives.MigrationActivityTQ,
				Namespace:     primitives.SystemLocalNamespace,
			},
			Value: 1,
		},
		{
			Value: 12,
		},
	}, defaultNumTaskQueuePartitions)

	collection := NewNoopCollection()
	writePartitions := MatchingNumTaskqueueWritePartitions.Get(collection)
	readPartitions := MatchingNumTaskqueueReadPartitions.Get(collection)

	require.Equal(t, 12, writePartitions("default", "hot-queue", enumspb.TASK_QUEUE_TYPE_ACTIVITY))
	require.Equal(t, 12, readPartitions("default", "hot-queue", enumspb.TASK_QUEUE_TYPE_ACTIVITY))
	require.Equal(t, 1, writePartitions("default", primitives.PerNSWorkerTaskQueue, enumspb.TASK_QUEUE_TYPE_ACTIVITY))
	require.Equal(t, 1, readPartitions(primitives.SystemLocalNamespace, primitives.AddSearchAttributesActivityTQ, enumspb.TASK_QUEUE_TYPE_ACTIVITY))
}
