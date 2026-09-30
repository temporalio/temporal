package ndc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	test "go.temporal.io/server/common/testing"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/history/workflow/update"
	"go.uber.org/mock/gomock"
)

// newChasmCallbacksMutableState returns a running workflow whose completion callbacks are held
// by the CHASM tree, on a shard whose aggregate callback limits are far below what the tests
// reapply.
func newChasmCallbacksMutableState(t *testing.T) *workflow.MutableStateImpl {
	t.Helper()

	ctrl := gomock.NewController(t)
	config := tests.NewDynamicConfig()
	config.EnableChasm = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
	config.EnableCHASMCallbacks = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
	config.EnableWorkflowUpdateCallbacks = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
	config.MaxCallbacksPerUpdateID = dynamicconfig.GetIntPropertyFnFilteredByNamespace(1)

	shardCtx := shard.NewTestContext(ctrl, &persistencespb.ShardInfo{ShardId: 1, RangeId: 1}, config)
	t.Cleanup(shardCtx.StopForTest)

	smRegistry := hsm.NewRegistry()
	require.NoError(t, workflow.RegisterStateMachine(smRegistry))
	shardCtx.SetStateMachineRegistry(smRegistry)
	chasmRegistry := chasm.NewRegistry(log.NewTestLogger())
	require.NoError(t, chasmRegistry.Register(chasmworkflow.NewLibrary(chasmworkflow.NewRegistry())))
	shardCtx.SetChasmRegistry(chasmRegistry)

	validatorConfig := test.NewCallbacksValidatorConfig()
	validatorConfig.MaxCallbacksPerExecution = func(string) int { return 1 }
	validatorConfig.TotalCallbacksMaxSize = func(string) int { return 1 }
	shardCtx.SetCallbackValidator(test.NewCallbacksValidator(t, validatorConfig))

	namespaceEntry := tests.GlobalNamespaceEntry
	shardCtx.Resource.NamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(namespaceEntry, nil).AnyTimes()
	shardCtx.Resource.ClusterMetadata.EXPECT().ClusterNameForFailoverVersion(gomock.Any(), gomock.Any()).
		Return(cluster.TestCurrentClusterName).AnyTimes()
	shardCtx.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	shardCtx.Resource.ClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()

	eventsCache := events.NewMockCache(ctrl)
	eventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()
	shardCtx.SetEventsCacheForTesting(eventsCache)

	ms := workflow.NewMutableState(
		shardCtx, eventsCache, shardCtx.GetLogger(), namespaceEntry, tests.WorkflowID, tests.RunID, time.Now().UTC(),
	)
	_, err := ms.AddWorkflowExecutionStartedEvent(
		&commonpb.WorkflowExecution{WorkflowId: tests.WorkflowID, RunId: tests.RunID},
		&historyservice.StartWorkflowExecutionRequest{
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				RequestId:    "req-start",
				WorkflowType: &commonpb.WorkflowType{Name: "test-workflow-type"},
				TaskQueue:    &taskqueuepb.TaskQueue{Name: "test-task-queue"},
			},
		},
	)
	require.NoError(t, err)
	return ms
}

func testNexusCallbacks(n int) []*commonpb.Callback {
	cbs := make([]*commonpb.Callback, n)
	for i := range cbs {
		cbs[i] = &commonpb.Callback{
			Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{Url: "http://localhost/callback"},
			},
		}
	}
	return cbs
}

// Reset and NDC conflict resolution reapply events whose callbacks were validated when first
// attached. Even though they now breach both the execution-wide and per-update limits, they
// must be reapplied as-is rather than failing the reset or stalling replication.
func TestReapplyEvents_KeepsCallbacksOverLimit(t *testing.T) {
	t.Parallel()

	const updateID = "update-id"
	reappliedEvents := []*historypb.HistoryEvent{
		{
			EventId:   10,
			Version:   1,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionOptionsUpdatedEventAttributes{
				WorkflowExecutionOptionsUpdatedEventAttributes: &historypb.WorkflowExecutionOptionsUpdatedEventAttributes{
					AttachedRequestId:           "req-attach",
					AttachedCompletionCallbacks: testNexusCallbacks(3),
				},
			},
		},
		{
			EventId:   11,
			Version:   1,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionUpdateAdmittedEventAttributes{
				WorkflowExecutionUpdateAdmittedEventAttributes: &historypb.WorkflowExecutionUpdateAdmittedEventAttributes{
					Request: &updatepb.Request{
						Meta:                &updatepb.Meta{UpdateId: updateID},
						RequestId:           "req-update",
						CompletionCallbacks: testNexusCallbacks(3),
					},
				},
			},
		},
	}

	for _, tc := range []struct {
		name    string
		isReset bool
	}{
		{name: "Reset", isReset: true},
		{name: "ConflictResolution", isReset: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ms := newChasmCallbacksMutableState(t)

			// Conflict resolution passes the target branch's update registry and dedups against the
			// losing run; reset does neither.
			var targetBranchUpdateRegistry update.Registry
			runIDForDeduplication := ""
			if !tc.isReset {
				targetBranchUpdateRegistry = update.NewRegistry(ms)
				runIDForDeduplication = "losing-run-id"
			}

			applied, err := reapplyEvents(
				context.Background(),
				ms,
				targetBranchUpdateRegistry,
				hsm.NewRegistry(),
				chasmworkflow.NewRegistry(),
				reappliedEvents,
				nil,
				runIDForDeduplication,
				tc.isReset,
				log.NewTestLogger(),
			)
			require.NoError(t, err)
			require.Len(t, applied, len(reappliedEvents))

			wf, ctx, err := ms.ChasmWorkflowComponentReadOnly(context.Background())
			require.NoError(t, err)
			require.Len(t, wf.Callbacks, 3)
			require.Contains(t, wf.Updates, updateID)
			require.Len(t, wf.Updates[updateID].Get(ctx).Callbacks, 3)
			require.Equal(t, int64(6), wf.GetTotalCallbacksCount())
		})
	}
}
