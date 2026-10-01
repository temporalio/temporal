package workflow

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestExecutionTypeAndPath verifies that a workflow and the components nested inside it all report
// the same execution identity, and that a nested component is located by its path within that
// execution.
func TestExecutionTypeAndPath(t *testing.T) {
	logger := log.NewTestLogger()
	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(NewLibrary(NewRegistry())))
	require.NoError(t, registry.Register(&callback.Library{}))

	workflowKey := definition.NewWorkflowKey("namespace-id", "workflow-id", "run-id")
	timeSource := clock.NewEventTimeSource()
	nodeBackend := &chasm.MockNodeBackend{
		HandleNextTransitionCount: func() int64 { return 2 },
		HandleGetCurrentVersion:   func() int64 { return 1 },
		HandleGetWorkflowKey:      func() definition.WorkflowKey { return workflowKey },
		HandleNow:                 timeSource.Now,
	}
	root := chasm.NewEmptyTree(
		registry,
		nodeBackend,
		chasm.DefaultPathEncoder,
		logger,
		metrics.NoopMetricsHandler,
	)

	newTestCallback := func(requestID string) *callback.Callback {
		return callback.NewCallback(requestID, timestamppb.Now(), &callbackspb.Callback{
			Variant: &callbackspb.Callback_Nexus_{
				Nexus: &callbackspb.Callback_Nexus{Url: "http://" + requestID},
			},
		})
	}

	mutableCtx := chasm.NewMutableContext(context.Background(), root)
	workflowCallback := newTestCallback("workflow-request-id")
	updateCallback := newTestCallback("update-request-id")

	// A component has no path until it is synced into the tree below.
	require.Nil(t, mutableCtx.Path(workflowCallback))
	update := NewWorkflowUpdate(mutableCtx, "update-id", chasm.NewMSPointer(nodeBackend))
	update.Callbacks = chasm.Map[string, *callback.Callback]{
		updateCallback.RequestId: chasm.NewComponentField(mutableCtx, updateCallback),
	}
	wf := &Workflow{
		MSPointer: chasm.NewMSPointer(nodeBackend),
		Callbacks: chasm.Map[string, *callback.Callback]{
			workflowCallback.RequestId: chasm.NewComponentField(mutableCtx, workflowCallback),
		},
		Updates: chasm.Map[string, *WorkflowUpdate]{
			update.UpdateId: chasm.NewComponentField(mutableCtx, update),
		},
	}
	require.NoError(t, root.SetRootComponent(wf))

	// The workflow archetype is registered with EXECUTION_TYPE_WORKFLOW, so every component of the
	// tree reports the workflow as the execution it belongs to.
	ctx := chasm.NewContext(context.Background(), root)
	require.Equal(t, enumspb.EXECUTION_TYPE_WORKFLOW, ctx.ExecutionInfo().ExecutionType)
	require.Equal(t, chasm.ExecutionKey{
		NamespaceID: "namespace-id",
		BusinessID:  "workflow-id",
		RunID:       "run-id",
	}, ctx.ExecutionKey())

	// The workflow is the root of the execution, so its path within the execution is empty.
	require.Equal(t, []string{}, ctx.Path(wf))
	require.Equal(t, []string{"Callbacks", workflowCallback.RequestId}, ctx.Path(workflowCallback))
	require.Equal(t, []string{"Updates", update.UpdateId}, ctx.Path(update))

	// The callback on the Workflow Update exposes the full path.
	require.Equal(t,
		[]string{"Updates", update.UpdateId, "Callbacks", updateCallback.RequestId},
		ctx.Path(updateCallback),
	)
}
