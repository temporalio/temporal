package persistencetest_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/persistencetest"
	"go.temporal.io/server/service/history/tasks"
)

func TestGetQueueKey_Default(t *testing.T) {
	t.Parallel()

	queueKey := persistencetest.GetQueueKey(t)
	require.Equal(t, persistence.QueueTypeHistoryNormal, queueKey.QueueType)
	require.Equal(t, tasks.CategoryTransfer, queueKey.Category)
	require.Equal(t, "src-TestGetQueueKey_Default", queueKey.SourceCluster)
	require.Equal(t, "tgt-TestGetQueueKey_Default", queueKey.TargetCluster)
}

func TestGetQueueKey_WithOptions(t *testing.T) {
	t.Parallel()

	queueKey := persistencetest.GetQueueKey(t,
		persistencetest.WithQueueType(persistence.QueueTypeHistoryDLQ),
		persistencetest.WithCategory(tasks.CategoryTimer),
	)
	require.Equal(t, persistence.QueueTypeHistoryDLQ, queueKey.QueueType)
	require.Equal(t, tasks.CategoryTimer, queueKey.Category)
	require.Equal(t, "src-TestGetQueueKey_WithOptions", queueKey.SourceCluster)
	require.Equal(t, "tgt-TestGetQueueKey_WithOptions", queueKey.TargetCluster)
}
