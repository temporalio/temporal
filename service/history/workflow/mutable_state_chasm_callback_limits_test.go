package workflow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	callbacklib "go.temporal.io/server/chasm/lib/callback"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common/callbacks"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	test "go.temporal.io/server/common/testing"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	hsmcallbacks "go.temporal.io/server/service/history/hsm/callbacks"
	"go.temporal.io/server/service/history/hsm/nexusoperations"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func nexusCallbackForTest(url string) *commonpb.Callback {
	return &commonpb.Callback{
		Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: url}},
	}
}

// newChasmCallbackTestMutableState builds a mutable state with a live CHASM tree and the
// cumulative callback count capped at maxCallbacks.
//
// The validator is replaced wholesale rather than overriding a dynamic config getter, because
// configs.NewConfig bakes the validator from the collection it is handed.
func newChasmCallbackTestMutableState(t *testing.T, maxCallbacks int) *MutableStateImpl {
	t.Helper()
	ctrl := gomock.NewController(t)

	cfg := tests.NewDynamicConfig()
	cfg.EnableChasm = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)

	validatorCfg := test.NewCallbacksValidatorConfig()
	validatorCfg.MaxCallbacksPerExecution = func(string) int { return maxCallbacks }
	validator, err := callbacks.NewValidator(validatorCfg)
	require.NoError(t, err)
	cfg.CallbackValidator = validator

	mockShard := shard.NewTestContext(ctrl, &persistencespb.ShardInfo{ShardId: 0, RangeId: 1}, cfg)
	t.Cleanup(mockShard.StopForTest)
	mockShard.SetChasmRegistry(newChasmCallbackTestRegistry(t))

	hsmReg := hsm.NewRegistry()
	require.NoError(t, RegisterStateMachine(hsmReg))
	require.NoError(t, hsmcallbacks.RegisterStateMachine(hsmReg))
	require.NoError(t, nexusoperations.RegisterStateMachines(hsmReg))
	mockShard.SetStateMachineRegistry(hsmReg)

	mockEventsCache := events.NewMockCache(ctrl)
	mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()

	namespaceEntry := tests.LocalNamespaceEntry
	mockShard.Resource.NamespaceCache.EXPECT().
		GetNamespaceByID(namespaceEntry.ID()).Return(namespaceEntry, nil).AnyTimes()
	mockShard.Resource.ClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
	mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().
		Return(cluster.TestCurrentClusterName).AnyTimes()
	mockShard.Resource.ClusterMetadata.EXPECT().
		ClusterNameForFailoverVersion(gomock.Any(), gomock.Any()).
		Return(cluster.TestCurrentClusterName).AnyTimes()

	ms := NewMutableState(
		mockShard, mockEventsCache, log.NewTestLogger(), namespaceEntry,
		tests.WorkflowID, tests.RunID, time.Now().UTC(),
	)
	require.True(t, ms.ChasmEnabled(), "test requires a live CHASM tree")
	ms.EnsureChasmWorkflowComponent(t.Context())
	return ms
}

func newChasmCallbackTestRegistry(t *testing.T) *chasm.Registry {
	t.Helper()
	reg := chasm.NewRegistry(log.NewTestLogger())
	require.NoError(t, reg.Register(chasmworkflow.NewLibrary(chasmworkflow.NewRegistry())))
	require.NoError(t, reg.Register(callbacklib.NewNilLibrary()))
	return reg
}

func optionsUpdatedEvent(cbs ...*commonpb.Callback) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
		EventTime: timestamppb.Now(),
		Attributes: &historypb.HistoryEvent_WorkflowExecutionOptionsUpdatedEventAttributes{
			WorkflowExecutionOptionsUpdatedEventAttributes: &historypb.WorkflowExecutionOptionsUpdatedEventAttributes{
				AttachedRequestId:           "req-1",
				AttachedCompletionCallbacks: cbs,
			},
		},
	}
}

// The write path is where a cumulative callback limit is meaningful: the request has not been
// accepted yet, so rejecting it tells the caller something actionable.
func TestChasmCallbackLimitRejectsOnTheWritePath(t *testing.T) {
	ms := newChasmCallbackTestMutableState(t, 1)

	err := ms.validateChasmCallbackAttachments(ChasmCallbackAttachment{
		Callbacks: []*commonpb.Callback{
			nexusCallbackForTest("http://localhost/cb-1"),
			nexusCallbackForTest("http://localhost/cb-2"),
		},
	})
	require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
	require.ErrorContains(t, err, "cannot attach more than 1 callbacks to an execution")
}

// The regression this stage exists to prevent. Apply functions are also driven by
// MutableStateRebuilder during NDC replication, history import, and reset, where the event has
// already been committed on another cluster. Enforcing there rejects committed history and
// stalls the replication task, and lowering a limit would wedge every execution above it.
//
// Before enforcement moved to the write path this failed with FailedPrecondition.
func TestChasmCallbackLimitIsNotEnforcedWhenApplyingCommittedEvents(t *testing.T) {
	ms := newChasmCallbackTestMutableState(t, 1)

	// Three callbacks against a cap of one: far beyond what the write path would accept.
	event := optionsUpdatedEvent(
		nexusCallbackForTest("http://localhost/cb-1"),
		nexusCallbackForTest("http://localhost/cb-2"),
		nexusCallbackForTest("http://localhost/cb-3"),
	)
	require.NoError(t, ms.ApplyWorkflowExecutionOptionsUpdatedEvent(event))

	wf, ctx, err := ms.ChasmWorkflowComponentReadOnly(t.Context())
	require.NoError(t, err)
	count, size := wf.CallbackTotals(ctx)
	require.Equal(t, 3, count, "every callback on the committed event must be applied")
	require.Positive(t, size)
}

// Re-applying the same event, as happens when a replication task is retried, must not
// double-count the callbacks it already attached.
func TestApplyingTheSameOptionsEventTwiceDoesNotDoubleCount(t *testing.T) {
	ms := newChasmCallbackTestMutableState(t, 100)

	event := optionsUpdatedEvent(
		nexusCallbackForTest("http://localhost/cb-1"),
		nexusCallbackForTest("http://localhost/cb-2"),
	)
	require.NoError(t, ms.ApplyWorkflowExecutionOptionsUpdatedEvent(event))
	require.NoError(t, ms.ApplyWorkflowExecutionOptionsUpdatedEvent(event))

	wf, ctx, err := ms.ChasmWorkflowComponentReadOnly(t.Context())
	require.NoError(t, err)
	count, _ := wf.CallbackTotals(ctx)
	require.Equal(t, 2, count)
}

// Continue-as-new, retry and reset reapply carry forward callbacks that were already accepted.
// Enforcing a since-lowered limit against them would leave the execution unable to proceed.
func TestSuppressCallbackLimitChecksExemptsReapplyPaths(t *testing.T) {
	ms := newChasmCallbackTestMutableState(t, 1)
	cbs := []*commonpb.Callback{
		nexusCallbackForTest("http://localhost/cb-1"),
		nexusCallbackForTest("http://localhost/cb-2"),
	}

	require.Error(t, ms.validateChasmCallbackAttachments(ChasmCallbackAttachment{Callbacks: cbs}))
	ms.SuppressCallbackLimitChecks()
	require.NoError(t, ms.validateChasmCallbackAttachments(ChasmCallbackAttachment{Callbacks: cbs}))
}
