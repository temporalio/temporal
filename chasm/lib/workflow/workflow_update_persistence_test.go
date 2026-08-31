package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testlogger"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestWorkflowUpdatePersistenceRoundTrip(t *testing.T) {
	tests := []struct {
		name          string
		finalize      func(*Workflow, chasm.MutableContext) error
		wantStatus    callbackspb.CallbackStatus
		wantRejection string
	}{
		{
			name:       "accepted",
			finalize:   func(*Workflow, chasm.MutableContext) error { return nil },
			wantStatus: callbackspb.CALLBACK_STATUS_STANDBY,
		},
		{
			name: "completed",
			finalize: func(wf *Workflow, ctx chasm.MutableContext) error {
				return wf.ProcessUpdateCallbacks(ctx, "update-id")
			},
			wantStatus: callbackspb.CALLBACK_STATUS_SCHEDULED,
		},
		{
			name: "rejected",
			finalize: func(wf *Workflow, ctx chasm.MutableContext) error {
				return wf.RejectUpdate(ctx, "update-id", &failurepb.Failure{
					Message: "update rejected",
					Source:  "worker",
				})
			},
			wantStatus:    callbackspb.CALLBACK_STATUS_SCHEDULED,
			wantRejection: "update rejected",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			restoredUpdate, restoredContext := roundTripWorkflowUpdate(t, tt.finalize)
			require.Equal(t, "update-id", restoredUpdate.GetUpdateId())
			if tt.wantRejection == "" {
				require.Nil(t, restoredUpdate.GetRejectionFailure())
			} else {
				require.Equal(t, tt.wantRejection, restoredUpdate.GetRejectionFailure().GetMessage())
				require.Equal(t, "worker", restoredUpdate.GetRejectionFailure().GetSource())
			}
			require.Len(t, restoredUpdate.Callbacks, 1)
			for _, callbackField := range restoredUpdate.Callbacks {
				restoredCallback := callbackField.Get(restoredContext)
				require.Equal(t, "http://callback", restoredCallback.GetCallback().GetNexus().GetUrl())
				require.Equal(t, tt.wantStatus, restoredCallback.GetStatus())
			}
		})
	}
}

func roundTripWorkflowUpdate(
	t *testing.T,
	finalize func(*Workflow, chasm.MutableContext) error,
) (*WorkflowUpdate, chasm.Context) {
	t.Helper()

	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(callback.NewNilLibrary()))
	require.NoError(t, registry.Register(NewLibrary(NewRegistry())))

	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Now())
	workflowKey := definition.NewWorkflowKey("namespace-id", "workflow-id", "run-id")
	transitionCount := int64(1)
	newBackend := func() *chasm.MockNodeBackend {
		return &chasm.MockNodeBackend{
			HandleGetCurrentVersion: func() int64 { return 1 },
			HandleNextTransitionCount: func() int64 {
				transitionCount++
				return transitionCount
			},
			HandleCurrentVersionedTransition: func() *persistencespb.VersionedTransition {
				return &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: transitionCount}
			},
			HandleGetWorkflowKey: func() definition.WorkflowKey { return workflowKey },
			HandleIsWorkflow:     func() bool { return true },
			HandleGetExecutionState: func() *persistencespb.WorkflowExecutionState {
				return &persistencespb.WorkflowExecutionState{
					State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
					Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				}
			},
			HandleGetNamespaceEntry: func() *namespace.Namespace {
				return namespace.NewLocalNamespaceForTest(
					&persistencespb.NamespaceInfo{Id: workflowKey.NamespaceID, Name: "namespace"},
					&persistencespb.NamespaceConfig{},
					cluster.TestCurrentClusterName,
				)
			},
		}
	}

	root := chasm.NewEmptyTree(
		registry,
		timeSource,
		newBackend(),
		chasm.DefaultPathEncoder,
		logger,
		metrics.NoopMetricsHandler,
	)
	wf := NewWorkflow(nil, chasm.MSPointer{})
	require.NoError(t, root.SetRootComponent(wf))
	_, err := root.CloseTransaction()
	require.NoError(t, err)

	mutableContext := chasm.NewMutableContext(context.Background(), root)
	rootComponent, err := root.Component(mutableContext, chasm.ComponentRef{})
	require.NoError(t, err)
	wf = rootComponent.(*Workflow)
	require.NoError(t, wf.AddUpdateCompletionCallbacks(
		mutableContext,
		timestamppb.New(timeSource.Now()),
		"update-id",
		"request-id",
		[]*commonpb.Callback{nexusCallback("http://callback")},
		2,
		2,
	))
	require.NoError(t, finalize(wf, mutableContext))
	mutation, err := root.CloseTransaction()
	require.NoError(t, err)
	require.NotEmpty(t, mutation.UpdatedNodes)

	restoredRoot, err := chasm.NewTreeFromDB(
		mutation.UpdatedNodes,
		registry,
		timeSource,
		newBackend(),
		chasm.DefaultPathEncoder,
		logger,
		metrics.NoopMetricsHandler,
	)
	require.NoError(t, err)

	restoredContext := chasm.NewContext(context.Background(), restoredRoot)
	restoredComponent, err := restoredRoot.Component(restoredContext, chasm.ComponentRef{})
	require.NoError(t, err)
	restoredWorkflow := restoredComponent.(*Workflow)
	require.Contains(t, restoredWorkflow.Updates, "update-id")
	return restoredWorkflow.Updates["update-id"].Get(restoredContext), restoredContext
}
