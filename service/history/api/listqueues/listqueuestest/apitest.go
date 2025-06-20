package listqueuestest

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/api/listqueues"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/tasks"
)

// TestInvoke is a library test function intended to be invoked from a persistence test suite. It works by
// enqueueing a task into the DLQ and then calling [getdlqtasks.Invoke] to verify that the right task is returned.
func TestInvoke(t *testing.T, manager persistence.HistoryTaskQueueManager) {
	ctx := context.Background()
	t.Run("HappyPath", func(t *testing.T) {
		inTask := &tasks.WorkflowTask{
			TaskID: 42,
		}
		sourceCluster := "test-source-cluster-" + t.Name()
		targetCluster := "test-target-cluster-" + t.Name()
		queueType := persistence.QueueTypeHistoryDLQ
		var queueKeys []persistence.QueueKey
		for i := 0; i < 3; i++ {
			queueKey := persistence.QueueKey{
				QueueType:     queueType,
				Category:      inTask.GetCategory(),
				SourceCluster: sourceCluster + strconv.Itoa(i),
				TargetCluster: targetCluster + strconv.Itoa(i),
			}
			queueKeys = append(queueKeys, queueKey)
			_, err := manager.CreateQueue(ctx, &persistence.CreateQueueRequest{QueueKey: queueKey})
			require.NoError(t, err)
		}

		var listedQueueNames []string
		var nextPageToken []byte
		for i := int32(1); ; i++ {
			res, err := listqueues.Invoke(
				context.Background(),
				manager,
				&historyservice.ListQueuesRequest{
					QueueType:     int32(queueType),
					PageSize:      i,
					NextPageToken: nextPageToken,
				},
			)
			require.NoError(t, err)
			for _, queue := range res.Queues {
				listedQueueNames = append(listedQueueNames, queue.QueueName)

			}
			if len(res.NextPageToken) == 0 {
				break
			}
			nextPageToken = res.NextPageToken
		}
		for _, queueKey := range queueKeys {
			require.Contains(t, listedQueueNames, queueKey.GetQueueName())
		}
	})
	t.Run("InvalidPageSize", func(t *testing.T) {
		t.Parallel()
		queueType := persistence.QueueTypeHistoryDLQ
		_, err := listqueues.Invoke(
			context.Background(),
			manager,
			&historyservice.ListQueuesRequest{
				QueueType: int32(queueType),
				PageSize:  0,
			},
		)
		require.Error(t, err)
		assert.ErrorIs(t, err, consts.ErrInvalidPageSize)
	})
	t.Run("InvalidNextPageToken", func(t *testing.T) {
		t.Parallel()
		queueType := persistence.QueueTypeHistoryDLQ
		_, err := listqueues.Invoke(
			context.Background(),
			manager,
			&historyservice.ListQueuesRequest{
				QueueType:     int32(queueType),
				PageSize:      1,
				NextPageToken: []byte("invalid_token"),
			},
		)
		require.Error(t, err)
	})
}
