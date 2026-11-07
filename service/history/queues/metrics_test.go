package queues

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/tasks"
)

type archetypeMetricsTestLibrary struct {
	chasm.UnimplementedLibrary
}

func (archetypeMetricsTestLibrary) Name() string {
	return "testLibrary"
}

func (archetypeMetricsTestLibrary) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*chasm.MockComponent]("testComponent"),
	}
}

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
		require.Equal(t, metrics.ArchetypeTag(chasm.WorkflowArchetype), tag)
	})

	t.Run("HasArchetypeID task with unregistered ID defaults to workflow", func(t *testing.T) {
		task := &tasks.ChasmTaskPure{ArchetypeID: 9999}
		tag := getArchetypeTag(task, registry)
		require.Equal(t, metrics.ArchetypeTag(chasm.WorkflowArchetype), tag)
	})

	t.Run("HasArchetypeID task with registered ID uses fully qualified name", func(t *testing.T) {
		registry := chasm.NewRegistry(log.NewTestLogger())
		require.NoError(t, registry.Register(archetypeMetricsTestLibrary{}))
		task := &tasks.ChasmTaskPure{
			ArchetypeID: chasm.GenerateTypeID("testLibrary.testComponent"),
		}
		tag := getArchetypeTag(task, registry)
		require.Equal(t, metrics.ArchetypeTag("testLibrary.testComponent"), tag)
	})
}
