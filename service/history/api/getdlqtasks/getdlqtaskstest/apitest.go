package getdlqtaskstest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonspb "go.temporal.io/server/api/common/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/api/getdlqtasks"
	"go.temporal.io/server/service/history/tasks"
)

// TestInvoke is a library test function intended to be invoked from a persistence test suite. It works by
// enqueueing a task into the DLQ and then calling [getdlqtasks.Invoke] to verify that the right task is returned.
func TestInvoke(t *testing.T, manager persistence.HistoryTaskQueueManager) {
	ctx := context.Background()
	inTask := &tasks.WorkflowTask{
		TaskID: 42,
	}
	sourceCluster := "test-source-cluster-" + t.Name()
	targetCluster := "test-target-cluster-" + t.Name()
	queueType := persistence.QueueTypeHistoryDLQ
	_, err := manager.CreateQueue(ctx, &persistence.CreateQueueRequest{
		QueueKey: persistence.QueueKey{
			QueueType:     queueType,
			Category:      inTask.GetCategory(),
			SourceCluster: sourceCluster,
			TargetCluster: targetCluster,
		},
	})
	require.NoError(t, err)
	_, err = manager.EnqueueTask(ctx, &persistence.EnqueueTaskRequest{
		QueueType:     queueType,
		SourceCluster: sourceCluster,
		TargetCluster: targetCluster,
		Task:          inTask,
		SourceShardID: 1,
	})
	require.NoError(t, err)
	res, err := getdlqtasks.Invoke(
		context.Background(),
		manager,
		tasks.NewDefaultTaskCategoryRegistry(),
		&historyservice.GetDLQTasksRequest{
			DlqKey: &commonspb.HistoryDLQKey{
				TaskCategory:  int32(tasks.CategoryTransfer.ID()),
				SourceCluster: sourceCluster,
				TargetCluster: targetCluster,
			},
			PageSize: 1,
		},
	)
	require.NoError(t, err)
	require.Equal(t, 1, len(res.DlqTasks))
	assert.Equal(t, int64(persistence.FirstQueueMessageID), res.DlqTasks[0].Metadata.MessageId)
	assert.Equal(t, 1, int(res.DlqTasks[0].Payload.ShardId))
	serializer := serialization.NewTaskSerializer()
	outTask, err := serializer.DeserializeTask(tasks.CategoryTransfer, res.DlqTasks[0].Payload.Blob)
	require.NoError(t, err)
	assert.Equal(t, inTask, outTask)
}
