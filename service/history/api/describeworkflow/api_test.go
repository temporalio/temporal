package describeworkflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	chasmworkflowpb "go.temporal.io/server/chasm/lib/workflow/gen/workflowpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	test "go.temporal.io/server/common/testing"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	"go.uber.org/mock/gomock"
)

func TestInvokeMergesRequestIDReferences(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	ctx := t.Context()
	ns := test.NewNamespace(t)
	const updateID = "update-id"
	updateEventType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED
	legacyEventType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED
	chasmCtx := &chasm.MockMutableContext{}
	wf := &chasmworkflow.Workflow{
		Updates: chasm.Map[string, *chasmworkflow.WorkflowUpdate]{
			updateID: chasm.NewComponentField(chasmCtx, &chasmworkflow.WorkflowUpdate{
				UpdateState: &chasmworkflowpb.UpdateState{
					UpdateId: updateID,
					CallbackAttachmentEventReferences: map[string]*chasmworkflowpb.CallbackAttachmentEventReference{
						"chasm-only":        {EventType: updateEventType, EventId: 42},
						"buffered":          {EventType: updateEventType, EventId: common.BufferedEventID},
						"legacy-collision":  {EventType: updateEventType, EventId: 43},
						"signal-collision":  {EventType: updateEventType, EventId: 44},
						"nil-reference":     nil,
						"unspecified-type":  {EventId: 45},
						"zero-event-id":     {EventType: updateEventType},
						"negative-event-id": {EventType: updateEventType, EventId: -1},
					},
				},
			}),
		},
	}
	require.NoError(t, wf.AddIncomingSignalEvent(chasmCtx, "signal-collision", 50))

	ms := historyi.NewMockMutableState(ctrl)
	ms.EXPECT().GetNamespaceEntry().Return(ns).AnyTimes()
	ms.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{}).AnyTimes()
	ms.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RequestIds: map[string]*persistencespb.RequestIDInfo{
			"legacy-collision": {EventType: legacyEventType, EventId: 11},
			"signal-collision": {EventType: legacyEventType, EventId: 12},
		},
	}).AnyTimes()
	ms.EXPECT().GetStartEvent(ctx).Return(&historypb.HistoryEvent{}, nil)
	ms.EXPECT().GetNextEventID().Return(int64(51))
	ms.EXPECT().IsResetRun().Return(false)
	ms.EXPECT().GetPendingActivityInfos().Return(nil)
	ms.EXPECT().GetPendingChildExecutionInfos().Return(nil)
	ms.EXPECT().GetPendingWorkflowTask().Return(nil)
	ms.EXPECT().ChasmEnabled().Return(true).AnyTimes()
	ms.EXPECT().ChasmWorkflowComponentReadOnly(ctx).Return(wf, chasmCtx, nil).AnyTimes()
	registry := hsm.NewRegistry()
	require.NoError(t, workflow.RegisterStateMachine(registry))
	root, err := hsm.NewRoot(registry, workflow.StateMachineType, nil, nil, ms)
	require.NoError(t, err)
	ms.EXPECT().HSM().Return(root).AnyTimes()

	shard := historyi.NewMockShardContext(ctrl)
	shard.EXPECT().GetConfig().Return(tests.NewDynamicConfig()).AnyTimes()
	shard.EXPECT().GetLogger().Return(log.NewNoopLogger()).AnyTimes()
	shard.EXPECT().GetMetricsHandler().Return(metrics.NoopMetricsHandler)

	checker := api.NewMockWorkflowConsistencyChecker(ctrl)
	checker.EXPECT().GetWorkflowLease(ctx, nil, gomock.Any(), gomock.Any()).
		Return(api.NewWorkflowLease(nil, func(err error) { require.NoError(t, err) }, ms), nil)

	response, err := Invoke(ctx, &historyservice.DescribeWorkflowExecutionRequest{
		NamespaceId: ns.ID().String(),
		Request: &workflowservice.DescribeWorkflowExecutionRequest{
			Execution: &commonpb.WorkflowExecution{WorkflowId: "workflow-id", RunId: "run-id"},
		},
	}, shard, checker, nil, nil, nil)
	require.NoError(t, err)
	require.Equal(t, map[string]*workflowpb.RequestIdInfo{
		"chasm-only":       {EventType: updateEventType, EventId: 42},
		"buffered":         {EventType: updateEventType, Buffered: true},
		"legacy-collision": {EventType: legacyEventType, EventId: 11},
		"signal-collision": {EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, EventId: 50},
	}, response.GetWorkflowExtendedInfo().GetRequestIdInfos())
}
