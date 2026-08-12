package umpire2

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	historyservice "go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/log"
	umpirefw "go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/fact"
	"go.temporal.io/server/tests/umpire2/model"
)

func startedWorkflowIn(namespaceID, workflowID string) umpirefw.Fact {
	return &fact.WorkflowStarted{
		Request: &historyservice.StartWorkflowExecutionRequest{
			NamespaceId: namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowId: workflowID,
			},
		},
		EntityPath: &umpirefw.EntityPath{
			EntityID:  umpirefw.NewEntityID(model.WorkflowType, workflowID),
			Ancestors: []umpirefw.EntityID{umpirefw.NewEntityID(model.NamespaceType, namespaceID)},
		},
	}
}

func countWorkflows(u *Monitor, namespaceID string) int {
	root := umpirefw.NewEntityID(model.NamespaceType, namespaceID)
	return len(u.ModelState().QueryEntities(model.WorkflowType, 0, &root))
}

func TestMonitor_CheckNamespace_IsScopedAndPurgeable(t *testing.T) {
	ctx := context.Background()
	u, err := NewMonitor(log.NewNoopLogger())
	require.NoError(t, err)

	const nsA, nsB = "ns-a", "ns-b"
	require.NoError(t, u.ModelState().RouteFacts(ctx, []umpirefw.Fact{
		startedWorkflowIn(nsA, "wf-a"),
		startedWorkflowIn(nsB, "wf-b"),
	}))
	require.Equal(t, 1, countWorkflows(u, nsA))
	require.Equal(t, 1, countWorkflows(u, nsB))
	rootA := umpirefw.NewEntityID(model.NamespaceType, nsA)
	entries := u.ModelState().QueryEntities(model.WorkflowType, 0, &rootA)
	require.Len(t, entries, 1)
	workflow, ok := entries[0].Entity.(*model.Workflow)
	require.True(t, ok)
	require.Equal(t, model.WorkflowStarted, workflow.Lifecycle().Current())

	// Checking namespace A must only surface A's stuck workflow, never B's.
	violations := u.CheckNamespace(ctx, nsA)
	require.NotEmpty(t, violations, "expected a violation for the started workflow in namespace A")
	for _, v := range violations {
		tags := fmt.Sprintf("%v", v.Tags)
		require.NotContains(t, tags, "wf-b", "namespace A check leaked into another namespace")
	}

	// Purging A drops only A's data; B is untouched.
	u.PurgeNamespace(nsA)
	require.Equal(t, 0, countWorkflows(u, nsA))
	require.Equal(t, 1, countWorkflows(u, nsB))

	// A re-check of the purged namespace finds nothing.
	require.Empty(t, u.CheckNamespace(ctx, nsA))
}
