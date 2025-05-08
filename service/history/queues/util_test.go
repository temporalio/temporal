package queues

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
)

func TestIsTaskAcked(t *testing.T) {
	scopes := NewRandomScopes(5)
	exclusiveReaderHighWatermark := scopes[len(scopes)-1].Range.ExclusiveMax.Next()
	persistenceQueueState := ToPersistenceQueueState(&queueState{
		readerScopes: map[int64][]Scope{
			DefaultReaderId: scopes,
		},
		exclusiveReaderHighWatermark: exclusiveReaderHighWatermark,
	})

	workflowKey := definition.NewWorkflowKey(tests.NamespaceID.String(), tests.WorkflowID, tests.RunID)

	testKey := exclusiveReaderHighWatermark
	testTask := tasks.NewFakeTask(
		workflowKey,
		tasks.CategoryTimer,
		testKey.FireTime,
	)
	testTask.SetTaskID(testKey.TaskID)
	assert.False(t, IsTaskAcked(testTask, persistenceQueueState))

	testKey = NewRandomKeyInRange(scopes[rand.Intn(len(scopes))].Range)
	testTask.SetVisibilityTime(testKey.FireTime)
	testTask.SetTaskID(testKey.TaskID)
	assert.False(t, IsTaskAcked(testTask, persistenceQueueState))

	testKey = NewRandomKeyInRange(NewRange(
		scopes[2].Range.ExclusiveMax,
		scopes[3].Range.InclusiveMin,
	))
	testTask.SetVisibilityTime(testKey.FireTime)
	testTask.SetTaskID(testKey.TaskID)
	assert.True(t, IsTaskAcked(testTask, persistenceQueueState))

	testKey = scopes[0].Range.InclusiveMin.Prev()
	testTask.SetVisibilityTime(testKey.FireTime)
	testTask.SetTaskID(testKey.TaskID)
	assert.True(t, IsTaskAcked(testTask, persistenceQueueState))
}
