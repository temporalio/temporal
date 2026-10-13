package updateworkflow

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow/update"
	"go.uber.org/mock/gomock"
)

func TestRequestIDLinkSelectionForDuplicates(t *testing.T) {
	t.Parallel()

	const (
		updateID                        = "update"
		requestID                       = "duplicate"
		acceptedEventID           int64 = 12
		callbackAttachmentEventID int64 = 15
		admitted                        = enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED
		accepted                        = enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED
		completed                       = enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED
	)
	for _, tc := range []struct {
		name              string
		setCallbacks      bool
		setRequestID      bool
		requestIDEventID  int64
		stageAtCapture    enumspb.UpdateWorkflowExecutionLifecycleStage
		stageAtResponse   enumspb.UpdateWorkflowExecutionLifecycleStage
		rejected          bool
		failed            bool
		wantRequestIDLink bool
	}{
		{name: "admitted with callbacks - requestID link", stageAtResponse: admitted, stageAtCapture: admitted, setCallbacks: true, setRequestID: true, wantRequestIDLink: true},
		{name: "admitted without callbacks - acceptedEvent link", stageAtResponse: admitted, stageAtCapture: admitted, setRequestID: true},
		{name: "accepted with buffered callback attachment - requestID link", stageAtResponse: accepted, stageAtCapture: accepted, setCallbacks: true, setRequestID: true, requestIDEventID: common.BufferedEventID, wantRequestIDLink: true},
		{name: "accepted without callbacks - acceptedEvent link", stageAtResponse: accepted, stageAtCapture: accepted, setRequestID: true},
		{name: "rejected with callbacks - workflow link", stageAtResponse: completed, stageAtCapture: admitted, rejected: true, setCallbacks: true, setRequestID: true, wantRequestIDLink: true},
		{name: "rejected without callbacks - workflow link", stageAtResponse: completed, stageAtCapture: admitted, rejected: true, setRequestID: true},
		{name: "completed after callback attachment - requestID link", stageAtResponse: completed, stageAtCapture: completed, setCallbacks: true, setRequestID: true, requestIDEventID: callbackAttachmentEventID, wantRequestIDLink: true},
		{name: "completed before duplicate arrives - acceptedEvent link", stageAtResponse: completed, stageAtCapture: completed, setCallbacks: true, setRequestID: true},
		{name: "accepted after projected callback attachment - requestID link", stageAtResponse: accepted, stageAtCapture: admitted, setCallbacks: true, setRequestID: true, wantRequestIDLink: true},
		{name: "no callbacks with existing mapping - acceptedEvent link", stageAtResponse: accepted, stageAtCapture: accepted, setRequestID: true, requestIDEventID: callbackAttachmentEventID},
		{name: "accepted after admission without callbacks - acceptedEvent link", stageAtResponse: accepted, stageAtCapture: admitted, setRequestID: true},
		{name: "completed without callbacks - acceptedEvent link", stageAtResponse: completed, stageAtCapture: completed, setRequestID: true},
		{name: "no request ID - acceptedEvent link", stageAtResponse: accepted, stageAtCapture: accepted},
		{name: "handler failure after acceptance with callback attachment - requestID link", stageAtResponse: completed, stageAtCapture: completed, failed: true, setCallbacks: true, setRequestID: true, requestIDEventID: callbackAttachmentEventID, wantRequestIDLink: true},
		{name: "handler failure after acceptance without callbacks - acceptedEvent link", stageAtResponse: completed, stageAtCapture: completed, failed: true, setRequestID: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			ms := historyi.NewMockMutableState(ctrl)
			outcome := &updatepb.Outcome{}
			if tc.failed || tc.rejected {
				outcome.Value = &updatepb.Outcome_Failure{Failure: &failurepb.Failure{Message: "update failed"}}
			}
			upd := update.New(updateID)
			// Mock mutable state appropriately.
			if !tc.rejected && (tc.stageAtResponse == accepted || tc.stageAtResponse == completed) {
				ms.EXPECT().GetCurrentVersion().Return(int64(1))
				ms.EXPECT().VisitUpdates(gomock.Any()).Do(func(visitor func(string, *persistencespb.UpdateInfo)) {
					if tc.stageAtResponse == completed {
						return
					}
					visitor(updateID, &persistencespb.UpdateInfo{
						Value: &persistencespb.UpdateInfo_Acceptance{Acceptance: &persistencespb.UpdateAcceptanceInfo{EventId: acceptedEventID}},
					})
				})
				if tc.stageAtResponse == completed {
					ms.EXPECT().GetUpdateOutcome(gomock.Any(), updateID).Return(outcome, nil)
					ms.EXPECT().GetUpdateAcceptedEventID(gomock.Any(), updateID).Return(acceptedEventID, nil)
				} else {
					ms.EXPECT().IsWorkflowExecutionRunning().Return(true)
				}
				upd = update.NewRegistry(ms).Find(context.Background(), updateID)
			}
			req := createUpdateRequest(updateID)
			req.Request.Namespace = "namespace"
			if tc.setRequestID {
				req.Request.Request.RequestId = requestID
			}
			if tc.setCallbacks {
				req.Request.Request.CompletionCallbacks = []*commonpb.Callback{{}}
			}
			u := &Updater{req: req, upd: upd, wfKey: definition.NewWorkflowKey("namespace-id", "workflow-id", "run-id")}
			// Setup requestIDRefLink TC checks.
			if tc.setRequestID && tc.setCallbacks {
				state := &persistencespb.WorkflowExecutionState{}
				if tc.requestIDEventID != common.EmptyEventID {
					state.RequestIds = map[string]*persistencespb.RequestIDInfo{
						requestID: {EventId: tc.requestIDEventID, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED},
					}
				}
				ms.EXPECT().GetExecutionState().Return(state)
				if tc.requestIDEventID == common.EmptyEventID {
					info := &persistencespb.UpdateInfo{}
					switch tc.stageAtCapture {
					case admitted:
					case completed:
						info.Value = &persistencespb.UpdateInfo_Completion{Completion: &persistencespb.UpdateCompletionInfo{}}
					case accepted:
						info.Value = &persistencespb.UpdateInfo_Acceptance{Acceptance: &persistencespb.UpdateAcceptanceInfo{EventId: acceptedEventID}}
					default:
						require.FailNow(t, "unexpected capture stage", "%v", tc.stageAtCapture)
					}
					ms.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{UpdateInfos: map[string]*persistencespb.UpdateInfo{updateID: info}})
				}
			}

			u.requestIDLink = u.captureRequestIDLink(ms)
			require.Equal(t, tc.wantRequestIDLink, u.requestIDLink != nil)
			link := u.responseLink(&update.Status{Stage: tc.stageAtResponse, Outcome: outcome})

			if tc.stageAtResponse == admitted {
				require.Nil(t, link)
				return
			}
			if tc.rejected {
				workflow := link.GetWorkflow()
				require.NotNil(t, workflow)
				require.Equal(t, "namespace", workflow.GetNamespace())
				require.Equal(t, "workflow-id", workflow.GetWorkflowId())
				require.Equal(t, "run-id", workflow.GetRunId())
				require.Equal(t, "Update rejected", workflow.GetReason())
				return
			}

			require.Equal(t, "namespace", link.GetWorkflowEvent().GetNamespace())
			require.Equal(t, "workflow-id", link.GetWorkflowEvent().GetWorkflowId())
			require.Equal(t, "run-id", link.GetWorkflowEvent().GetRunId())
			if tc.wantRequestIDLink {
				ref := link.GetWorkflowEvent().GetRequestIdRef()
				require.NotNil(t, ref)
				require.Equal(t, requestID, ref.GetRequestId())
				require.Equal(t, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, ref.GetEventType())
			} else {
				ref := link.GetWorkflowEvent().GetEventRef()
				require.NotNil(t, ref)
				require.Equal(t, acceptedEventID, ref.GetEventId())
				require.Equal(t, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED, ref.GetEventType())
			}
		})
	}
}

func TestApplyRequest_RejectsUpdateOnPausedWorkflow(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	ms := historyi.NewMockMutableState(ctrl)
	ms.EXPECT().GetWorkflowKey().Return(definition.NewWorkflowKey("ns-id", "wf-id", "run-id"))
	ms.EXPECT().IsWorkflowExecutionRunning().Return(true)
	ms.EXPECT().IsWorkflowExecutionStatusPaused().Return(true)
	// Required by update.NewRegistry
	ms.EXPECT().GetCurrentVersion().Return(int64(1))
	ms.EXPECT().VisitUpdates(gomock.Any())

	updateReg := update.NewRegistry(ms)

	updater := &Updater{
		req: createUpdateRequest("test-update-id"),
	}

	action, err := updater.ApplyRequest(context.Background(), updateReg, ms)

	require.Nil(t, action)
	require.Error(t, err)
	var failedPrecondition *serviceerror.FailedPrecondition
	require.ErrorAs(t, err, &failedPrecondition)
	require.Contains(t, err.Error(), "Workflow is paused")
}

func createUpdateRequest(updateID string) *historyservice.UpdateWorkflowExecutionRequest {
	return &historyservice.UpdateWorkflowExecutionRequest{
		Request: &workflowservice.UpdateWorkflowExecutionRequest{
			Request: &updatepb.Request{
				Meta: &updatepb.Meta{
					UpdateId: updateID,
				},
			},
		},
	}
}
