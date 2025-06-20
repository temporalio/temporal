package persistencetest_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/persistencetest"
	"go.temporal.io/server/service/history/tasks"
)

func TestGetQueueKey_Default(t *testing.T) {
	t.Parallel()

	queueKey := persistencetest.GetQueueKey(t)
	assert.Equal(t, persistence.QueueTypeHistoryNormal, queueKey.QueueType)
	assert.Equal(t, tasks.CategoryTransfer, queueKey.Category)
	assert.Equal(t, "test-source-cluster-TestGetQueueKey_Default", queueKey.SourceCluster)
	assert.Equal(t, "test-target-cluster-TestGetQueueKey_Default", queueKey.TargetCluster)
}

func TestGetQueueKey_WithOptions(t *testing.T) {
	t.Parallel()

	queueKey := persistencetest.GetQueueKey(t,
		persistencetest.WithQueueType(persistence.QueueTypeHistoryDLQ),
		persistencetest.WithCategory(tasks.CategoryTimer),
	)
	assert.Equal(t, persistence.QueueTypeHistoryDLQ, queueKey.QueueType)
	assert.Equal(t, tasks.CategoryTimer, queueKey.Category)
	assert.Equal(t, "test-source-cluster-TestGetQueueKey_WithOptions", queueKey.SourceCluster)
	assert.Equal(t, "test-target-cluster-TestGetQueueKey_WithOptions", queueKey.TargetCluster)
}
