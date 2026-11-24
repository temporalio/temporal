package workflow

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
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

// TestLifecycleState verifies that the workflow root mirrors the execution status kept in mutable
// state, including transitions made after the component was created.
func TestLifecycleState(t *testing.T) {
	status := enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	nodeBackend := &chasm.MockNodeBackend{
		HandleGetExecutionState: func() *persistencespb.WorkflowExecutionState {
			return &persistencespb.WorkflowExecutionState{Status: status}
		},
	}
	wf := &Workflow{MSPointer: chasm.NewMSPointer(nodeBackend)}

	require.Equal(t, chasm.LifecycleStateRunning, wf.LifecycleState(nil))
}

func TestIsExecutionSkippableWithPersistedNexusOperations(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name          string
		secondStatus  nexusoperationpb.OperationStatus
		corruptPath   []string
		cancellation  bool
		wantSkippable bool
	}{
		{name: "both backing off", secondStatus: nexusoperationpb.OPERATION_STATUS_BACKING_OFF, wantSkippable: true},
		{name: "one scheduled", secondStatus: nexusoperationpb.OPERATION_STATUS_SCHEDULED},
		{name: "unreadable operation", secondStatus: nexusoperationpb.OPERATION_STATUS_BACKING_OFF, corruptPath: []string{"Operations", "2"}},
		{name: "unreadable cancellation", secondStatus: nexusoperationpb.OPERATION_STATUS_BACKING_OFF, cancellation: true, corruptPath: []string{"Operations", "2", "Cancellation"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			logger := log.NewNoopLogger()
			registry := chasm.NewRegistry(logger)
			require.NoError(t, registry.Register(NewLibrary(NewRegistry())))
			require.NoError(t, registry.Register(nexusoperation.NewNilLibrary()))
			backend := &chasm.MockNodeBackend{
				HandleGetCurrentVersion:   func() int64 { return 1 },
				HandleNextTransitionCount: func() int64 { return 1 },
			}
			tree := chasm.NewEmptyTree(registry, backend, chasm.DefaultPathEncoder, logger, metrics.NoopMetricsHandler)
			ctx := chasm.NewMutableContext(t.Context(), tree)
			wf := NewWorkflow(ctx, chasm.NewMSPointer(backend))
			wf.addNexusOperation(ctx, 1, nexusoperation.NewOperation(&nexusoperationpb.OperationState{
				Status: nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
			}))
			second := nexusoperation.NewOperation(&nexusoperationpb.OperationState{Status: test.secondStatus})
			if test.cancellation {
				second.Cancellation = chasm.NewComponentField(ctx, &nexusoperation.Cancellation{
					CancellationState: &nexusoperationpb.CancellationState{RequestId: "cancel-request"},
				})
			}
			wf.addNexusOperation(ctx, 2, second)
			require.NoError(t, tree.SetRootComponent(wf))
			mutation, err := tree.CloseTransaction(t.Context(), true)
			require.NoError(t, err)
			if test.corruptPath != nil {
				var target *persistencespb.ChasmNode
				for encodedPath, node := range mutation.UpdatedNodes {
					path, err := chasm.DefaultPathEncoder.Decode(encodedPath)
					require.NoError(t, err)
					if slices.Equal(path, test.corruptPath) {
						target = node
						break
					}
				}
				require.NotNil(t, target, "missing CHASM node at %v", test.corruptPath)
				target.Data.Data = []byte{0xff}
			}

			reloaded, err := chasm.NewTreeFromDB(
				mutation.UpdatedNodes, registry, backend, chasm.DefaultPathEncoder, logger, metrics.NoopMetricsHandler,
			)
			require.NoError(t, err)
			readCtx := chasm.NewContext(t.Context(), reloaded)
			component, err := reloaded.ComponentByPath(readCtx, nil)
			require.NoError(t, err)
			require.Equal(t, test.wantSkippable, component.(*Workflow).IsExecutionSkippable(readCtx))
		})
	}
}
