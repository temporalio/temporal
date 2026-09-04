//go:build test_dep

package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
)

func TestFilterWorkflowMutationTasks(t *testing.T) {
	transferTask := &tasks.WorkflowTask{}
	visibilityTask := &tasks.StartExecutionVisibilityTask{}
	replicationTask := &tasks.HistoryReplicationTask{}
	mutation := &persistence.WorkflowMutation{Tasks: map[tasks.Category][]tasks.Task{
		tasks.CategoryTransfer:    {transferTask},
		tasks.CategoryVisibility:  {visibilityTask},
		tasks.CategoryReplication: {replicationTask},
	}}

	filterWorkflowMutationTasks(mutation, func(task tasks.Task) bool {
		return task.GetCategory() == tasks.CategoryVisibility ||
			task.GetCategory() == tasks.CategoryReplication
	})

	require.NotContains(t, mutation.Tasks, tasks.CategoryTransfer)
	require.Equal(t, []tasks.Task{visibilityTask}, mutation.Tasks[tasks.CategoryVisibility])
	require.Equal(t, []tasks.Task{replicationTask}, mutation.Tasks[tasks.CategoryReplication])
}
