package queues

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/service/history/tasks"
)

func TestGrouperNamespaceID_Key(t *testing.T) {
	g := GrouperNamespaceID{}
	k := g.Key(tasks.NewFakeTask(definition.NewWorkflowKey("nid", "", ""), tasks.CategoryTransfer, time.Now()))
	require.Equal(t, "nid", k)
}

func TestGrouperNamespaceID_Predicate(t *testing.T) {
	g := GrouperNamespaceID{}
	p := g.Predicate([]any{"n1", "n2"})
	require.Equal(t, tasks.NewNamespacePredicate([]string{"n1", "n2"}), p)
}

func TestGrouperStateMachineNamespaceIDAndDestination_Key(t *testing.T) {
	g := GrouperStateMachineNamespaceIDAndDestination{}
	task := &tasks.StateMachineOutboundTask{
		StateMachineTask: tasks.StateMachineTask{
			WorkflowKey: definition.NewWorkflowKey("nid", "", ""),
			Info: &persistencespb.StateMachineTaskInfo{
				Type: "3",
			},
		},
		Destination: "dest",
	}
	k := g.Key(task)
	require.Equal(t, tasks.TaskGroupNamespaceIDAndDestination{
		TaskGroup:   "3",
		NamespaceID: "nid",
		Destination: "dest",
	}, k)
}

func TestGrouperStateMachineNamespaceIDAndDestination_Predicate(t *testing.T) {
	g := GrouperStateMachineNamespaceIDAndDestination{}
	groups := []tasks.TaskGroupNamespaceIDAndDestination{
		{TaskGroup: "1", NamespaceID: "n1", Destination: "d1"},
		{TaskGroup: "2", NamespaceID: "n2", Destination: "d2"},
	}
	untypedGroups := []any{}
	for _, g := range groups {
		untypedGroups = append(untypedGroups, g)
	}
	p := g.Predicate(untypedGroups)

	expected := tasks.NewOutboundTaskPredicate(groups)
	require.Equal(t, expected, p)
}
