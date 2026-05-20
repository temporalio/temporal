package queues

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/tasks"
)

func TestGetArchivalTaskTypeTagValue(t *testing.T) {
	assert.Equal(t, "ArchivalTaskArchiveExecution", GetArchivalTaskTypeTagValue(&tasks.ArchiveExecutionTask{}))

	unknownTask := &tasks.CloseExecutionTask{}
	assert.Equal(t, unknownTask.GetType().String(), GetArchivalTaskTypeTagValue(unknownTask))
}

func TestGetArchetypeTag(t *testing.T) {
	registry := chasm.NewRegistry(log.NewTestLogger())

	t.Run("legacy task without HasArchetypeID defaults to workflow", func(t *testing.T) {
		task := &tasks.ActivityTask{}
		tag := getArchetypeTag(task, registry)
		assert.Equal(t, metrics.ArchetypeTag(chasm.WorkflowComponentName), tag)
	})

	t.Run("HasArchetypeID task with unregistered ID defaults to workflow", func(t *testing.T) {
		task := &tasks.ChasmTaskPure{ArchetypeID: 9999}
		tag := getArchetypeTag(task, registry)
		assert.Equal(t, metrics.ArchetypeTag(chasm.WorkflowComponentName), tag)
	})
}
