package umpire

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	umpirefw "go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/entity"
	"go.temporal.io/server/tests/umpire/fact"
)

// admittedUpdateIn builds a "stuck in admitted" WorkflowUpdate fact rooted at the
// given namespace, which the WorkflowUpdateLossPrevention liveness rule flags at
// teardown. Unlike workflow tasks, updates are not settled by settleWorkflows,
// so the violation survives to CheckNamespace.
func admittedUpdateIn(namespaceID, workflowID, updateID string) umpirefw.Fact {
	return &fact.WorkflowUpdateAdmitted{
		UpdateID:   updateID,
		WorkflowID: workflowID,
		EntityPath: &umpirefw.EntityPath{
			EntityID: umpirefw.NewEntityID(entity.WorkflowUpdateType, updateID),
			Ancestors: []umpirefw.EntityID{
				umpirefw.NewEntityID(entity.NamespaceType, namespaceID),
				umpirefw.NewEntityID(entity.WorkflowType, workflowID),
			},
		},
	}
}

func countUpdates(u *Umpire, namespaceID string) int {
	root := umpirefw.NewEntityID(entity.NamespaceType, namespaceID)
	return len(u.Registry().QueryEntities(entity.WorkflowUpdateType, 0, &root))
}

func TestUmpire_CheckNamespace_IsScopedAndPurgeable(t *testing.T) {
	ctx := context.Background()
	u, err := NewUmpire(log.NewNoopLogger())
	require.NoError(t, err)

	const nsA, nsB = "ns-a", "ns-b"
	require.NoError(t, u.Registry().RouteFacts(ctx, []umpirefw.Fact{
		admittedUpdateIn(nsA, "wf-a", "upd-a"),
		admittedUpdateIn(nsB, "wf-b", "upd-b"),
	}))
	require.Equal(t, 1, countUpdates(u, nsA))
	require.Equal(t, 1, countUpdates(u, nsB))

	// Checking namespace A must only surface A's stuck update, never B's.
	// Several rules may flag the admitted update; assert (rule-agnostically) that
	// none of the reported violations reference namespace B's entities.
	violations := u.CheckNamespace(ctx, nsA)
	require.NotEmpty(t, violations, "expected a violation for the admitted update in namespace A")
	for _, v := range violations {
		tags := fmt.Sprintf("%v", v.Tags)
		require.NotContains(t, tags, "upd-b", "namespace A check leaked into another namespace")
		require.NotContains(t, tags, "wf-b", "namespace A check leaked into another namespace")
	}

	// Purging A drops only A's data; B is untouched.
	u.PurgeNamespace(nsA)
	require.Equal(t, 0, countUpdates(u, nsA))
	require.Equal(t, 1, countUpdates(u, nsB))

	// A re-check of the purged namespace finds nothing.
	require.Empty(t, u.CheckNamespace(ctx, nsA))
}
