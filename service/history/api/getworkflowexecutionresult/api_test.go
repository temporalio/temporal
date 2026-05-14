package getworkflowexecutionresult

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	historyservice "go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/history/api"
	historyi "go.temporal.io/server/service/history/interfaces"
	historytests "go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

func makeRequest(links []*commonpb.Link) *historyservice.GetWorkflowExecutionResultRequest {
	return &historyservice.GetWorkflowExecutionResultRequest{
		NamespaceId: historytests.NamespaceID.String(),
		Request: &workflowservice.GetWorkflowExecutionResultRequest{
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: historytests.WorkflowID,
			},
			Links: links,
		},
	}
}

func setupBasicMutableState(ctrl *gomock.Controller) *historyi.MockMutableState {
	ms := historyi.NewMockMutableState(ctrl)
	ms.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowId: historytests.WorkflowID,
	}).AnyTimes()
	ms.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: historytests.RunID,
	}).AnyTimes()
	ms.EXPECT().GetNamespaceEntry().Return(historytests.GlobalNamespaceEntry).AnyTimes()
	return ms
}

func setupConsistencyChecker(ctrl *gomock.Controller, lease api.WorkflowLease) api.WorkflowConsistencyChecker {
	checker := api.NewMockWorkflowConsistencyChecker(ctrl)
	checker.EXPECT().GetWorkflowLease(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(lease, nil)
	return checker
}

// TestInvoke_WorkflowCompleted covers all terminal completion types via Invoke.
func TestInvoke_WorkflowCompleted(t *testing.T) {
	t.Parallel()
	details := &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: []byte("cancel-details")}}}
	failure := &failurepb.Failure{Message: "workflow failed"}

	cases := []struct {
		name       string
		event      *historypb.HistoryEvent // the completion event that we want our mocked mutableState to return
		assertResp func(t *testing.T, resp *workflowservice.GetWorkflowExecutionResultResponse)
	}{
		{
			name: "completed",
			event: &historypb.HistoryEvent{
				EventId:   5,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
					WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{
						Result: &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: []byte("result")}}},
					},
				},
			},
			assertResp: func(t *testing.T, r *workflowservice.GetWorkflowExecutionResultResponse) {
				t.Helper()
				require.NotNil(t, r.GetSuccess())
				require.Equal(t, []byte("result"), r.GetSuccess().GetResult().GetPayloads()[0].GetData())
			},
		},
		{
			name: "failed",
			event: &historypb.HistoryEvent{
				EventId:   5,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{
					WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{
						Failure: failure,
					},
				},
			},
			assertResp: func(t *testing.T, r *workflowservice.GetWorkflowExecutionResultResponse) {
				t.Helper()
				protorequire.ProtoEqual(t, failure, r.GetFailure().GetFailure())
			},
		},
		{
			name: "canceled",
			event: &historypb.HistoryEvent{
				EventId:   5,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionCanceledEventAttributes{
					WorkflowExecutionCanceledEventAttributes: &historypb.WorkflowExecutionCanceledEventAttributes{
						Details: details,
					},
				},
			},
			assertResp: func(t *testing.T, r *workflowservice.GetWorkflowExecutionResultResponse) {
				t.Helper()
				f := r.GetFailure().GetFailure()
				require.Equal(t, "workflow execution was canceled", f.GetMessage())
				protorequire.ProtoEqual(t, details, f.GetCanceledFailureInfo().GetDetails())
			},
		},
		{
			name: "terminated",
			event: &historypb.HistoryEvent{
				EventId:   5,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{
					WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{
						Identity: "terminator",
					},
				},
			},
			assertResp: func(t *testing.T, r *workflowservice.GetWorkflowExecutionResultResponse) {
				t.Helper()
				f := r.GetFailure().GetFailure()
				require.Equal(t, "workflow execution was terminated", f.GetMessage())
				require.Equal(t, "terminator", f.GetTerminatedFailureInfo().GetIdentity())
			},
		},
		{
			name: "timed_out",
			event: &historypb.HistoryEvent{
				EventId:   5,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionTimedOutEventAttributes{
					WorkflowExecutionTimedOutEventAttributes: &historypb.WorkflowExecutionTimedOutEventAttributes{},
				},
			},
			assertResp: func(t *testing.T, r *workflowservice.GetWorkflowExecutionResultResponse) {
				t.Helper()
				f := r.GetFailure().GetFailure()
				require.Equal(t, "workflow execution timed out", f.GetMessage())
				require.NotNil(t, f.GetTimeoutFailureInfo())
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			ms := setupBasicMutableState(ctrl)
			ms.EXPECT().IsWorkflowExecutionRunning().Return(false)
			ms.EXPECT().GetCompletionEvent(gomock.Any()).Return(tc.event, nil)

			lease := api.NewWorkflowLease(nil, func(error) {}, ms)
			checker := setupConsistencyChecker(ctrl, lease)

			resp, err := Invoke(context.Background(), makeRequest(nil), nil, checker)
			require.NoError(t, err)
			require.Equal(t, historytests.WorkflowID, resp.Response.GetExecution().GetWorkflowId())
			require.Equal(t, historytests.RunID, resp.Response.GetExecution().GetRunId())

			// Assert we have link to the workflow's completion event.
			require.Len(t, resp.Response.GetLinks(), 1)
			require.Equal(
				t,
				tc.event.GetEventId(),
				resp.Response.GetLinks()[0].GetWorkflowEvent().GetEventRef().GetEventId(),
			)
			tc.assertResp(t, resp.Response)
		})
	}
}

// TestInvoke_WorkflowRunning_NoLinks verifies that a running workflow with no links returns NotCompleted.
func TestInvoke_WorkflowRunning_NoLinks(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	ms := setupBasicMutableState(ctrl)
	ms.EXPECT().IsWorkflowExecutionRunning().Return(true)

	lease := api.NewWorkflowLease(nil, func(error) {}, ms)
	checker := setupConsistencyChecker(ctrl, lease)

	resp, err := Invoke(context.Background(), makeRequest(nil), nil, checker)
	require.NoError(t, err)
	require.Equal(t, historytests.WorkflowID, resp.Response.GetExecution().GetWorkflowId())
	require.Equal(t, historytests.RunID, resp.Response.GetExecution().GetRunId())
	require.NotNil(t, resp.Response.GetNotCompleted())
	require.Empty(t, resp.Response.GetLinks())
}

// TestInvoke_WorkflowRunning_WithLinks verifies that we ask mutableState to attach callbacks and links
// when the workflow is running.
func TestInvoke_WorkflowRunning_WithLinks(t *testing.T) {
	t.Parallel()
	incomingLinks := []*commonpb.Link{
		{
			Variant: &commonpb.Link_WorkflowEvent_{
				WorkflowEvent: &commonpb.Link_WorkflowEvent{
					Namespace:  "caller-ns",
					WorkflowId: "caller-wf",
					RunId:      "caller-run",
				},
			},
		},
	}
	optionsEvent := &historypb.HistoryEvent{
		EventId:   7,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
	}

	ctrl := gomock.NewController(t)
	ms := setupBasicMutableState(ctrl)
	ms.EXPECT().IsWorkflowExecutionRunning().Return(true)

	// Mock out the call to mutableState's internals -- the persistence
	ms.EXPECT().AddWorkflowExecutionOptionsUpdatedEvent(
		nil, false, gomock.Any(), gomock.Any(), incomingLinks, gomock.Any(), nil, nil,
	).Return(optionsEvent, nil)

	wfCtx := historyi.NewMockWorkflowContext(ctrl)
	wfCtx.EXPECT().UpdateWorkflowExecutionAsActive(gomock.Any(), gomock.Any()).Return(nil)

	lease := api.NewWorkflowLease(wfCtx, func(error) {}, ms)
	checker := setupConsistencyChecker(ctrl, lease)

	resp, err := Invoke(context.Background(), makeRequest(incomingLinks), nil, checker)
	require.NoError(t, err)
	require.NotNil(t, resp.Response.GetNotCompleted())

	// Link should point to the event that attaches the callbacks to persistence.
	require.Len(t, resp.Response.GetLinks(), 1)
	require.Equal(
		t,
		optionsEvent.GetEventId(),
		resp.Response.GetLinks()[0].GetWorkflowEvent().GetEventRef().GetEventId(),
	)
}
