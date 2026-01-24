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
		historyservice.GetDLQTasksRequest_builder{
			DlqKey: commonspb.HistoryDLQKey_builder{
				TaskCategory:  int32(tasks.CategoryTransfer.ID()),
				SourceCluster: sourceCluster,
				TargetCluster: targetCluster,
			}.Build(),
			PageSize: 1,
		}.Build(),
	)
	require.NoError(t, err)
	require.Equal(t, 1, len(res.GetDlqTasks()))
	assert.Equal(t, int64(persistence.FirstQueueMessageID), res.GetDlqTasks()[0].GetMetadata().GetMessageId())
	assert.Equal(t, 1, int(res.GetDlqTasks()[0].GetPayload().GetShardId()))
	serializer := serialization.NewSerializer()
	outTask, err := serializer.DeserializeTask(tasks.CategoryTransfer, res.GetDlqTasks()[0].GetPayload().GetBlob())
	require.NoError(t, err)
	assert.Equal(t, inTask, outTask)
}
