package workflow

import (
	"context"
	"time"

	"github.com/google/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	test "go.temporal.io/server/common/testing"
	"go.temporal.io/server/service/history/hsm/callbacks"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func testCompletionCallbacks(n int) []*commonpb.Callback {
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

// enableChasmCallbacks turns on the CHASM callback path and caps the execution at
// maxCallbacks. It returns a fresh mutable state built against the reconfigured shard.
func (s *mutableStateSuite) enableChasmCallbacks(maxCallbacks int) *MutableStateImpl {
	s.mockConfig.EnableChasm = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
	s.mockConfig.EnableCHASMCallbacks = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
	s.mockConfig.EnableWorkflowUpdateCallbacks = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)

	chasmRegistry := chasm.NewRegistry(log.NewTestLogger())
	s.Require().NoError(chasmRegistry.Register(chasmworkflow.NewLibrary(chasmworkflow.NewRegistry())))
	s.mockShard.SetChasmRegistry(chasmRegistry)

	cfg := test.NewCallbacksValidatorConfig()
	cfg.MaxCallbacksPerExecution = func(string) int { return maxCallbacks }
	s.mockShard.SetCallbackValidator(test.NewCallbacksValidator(s.T(), cfg))

	s.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()
	return NewMutableState(
		s.mockShard, s.mockEventsCache, s.logger, s.namespaceEntry, tests.WorkflowID, tests.RunID, time.Now().UTC(),
	)
}

func (s *mutableStateSuite) chasmWorkflowComponent(ms *MutableStateImpl) *chasmworkflow.Workflow {
	wf, _, err := ms.ChasmWorkflowComponentReadOnly(context.Background())
	s.Require().NoError(err)
	return wf
}

func (s *mutableStateSuite) TestUpdatePendingEventIDsMaintainsExistingCHASMStateAcrossGateChanges() {
	ms := s.enableChasmCallbacks(10)

	// Legacy request IDs are finalized without opening a CHASM Workflow root.
	const legacyRequestID = "legacy-request-id"
	ms.executionState.RequestIds = map[string]*persistencespb.RequestIDInfo{
		legacyRequestID: {EventId: common.BufferedEventID},
	}
	s.mockConfig.EnableCHASMCallbacks = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false)
	s.mockConfig.EnableCHASMSignalBacklinks = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false)
	s.Require().NoError(ms.updatePendingEventIDs(nil, map[string]int64{legacyRequestID: 10}))
	s.Equal(int64(10), ms.executionState.RequestIds[legacyRequestID].GetEventId())
	s.mockConfig.EnableCHASMCallbacks = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)

	_, err := ms.AddWorkflowExecutionStartedEvent(
		&commonpb.WorkflowExecution{WorkflowId: tests.WorkflowID, RunId: tests.RunID},
		&historyservice.StartWorkflowExecutionRequest{
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				RequestId:           "start-request-id",
				CompletionCallbacks: testCompletionCallbacks(1),
			},
		},
	)
	s.Require().NoError(err)

	wf, chasmCtx, err := ms.ChasmWorkflowComponent(context.Background())
	s.Require().NoError(err)

	const (
		signalRequestID   = "signal-request-id"
		callbackRequestID = "callback-request-id"
	)
	s.Require().NoError(wf.AddIncomingSignalEvent(chasmCtx, signalRequestID, common.BufferedEventID))
	s.Require().NoError(wf.AddUpdateCompletionCallbacks(
		chasmCtx,
		timestamppb.Now(), chasmworkflow.UpdateCallbackAttachment{EventReference: chasmworkflow.CallbackAttachmentEventReference{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
			EventID:   common.BufferedEventID,
		}, UpdateID: "update-id", RequestID: callbackRequestID, Callbacks: testCompletionCallbacks(1), LegacyEventReference: nil}))

	// Update callback references are finalized independently of the signal-backlink preview.
	s.mockConfig.EnableCHASMSignalBacklinks = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false)
	s.Require().NoError(ms.updatePendingEventIDs(nil, map[string]int64{
		signalRequestID:   11,
		callbackRequestID: 12,
	}))
	s.Equal(common.BufferedEventID, wf.IncomingSignals[signalRequestID].Get(chasmCtx).GetEventId())
	callbackReference, found := wf.LookupCallbackAttachmentEventReference(chasmCtx, "update-id", callbackRequestID)
	s.True(found)
	s.Equal(int64(12), callbackReference.EventID)

	const (
		secondSignalRequestID   = "second-signal-request-id"
		secondCallbackRequestID = "second-callback-request-id"
	)
	s.Require().NoError(wf.AddIncomingSignalEvent(chasmCtx, secondSignalRequestID, common.BufferedEventID))
	s.Require().NoError(wf.AddUpdateCompletionCallbacks(
		chasmCtx,
		timestamppb.Now(), chasmworkflow.UpdateCallbackAttachment{EventReference: chasmworkflow.CallbackAttachmentEventReference{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
			EventID:   common.BufferedEventID,
		}, UpdateID: "update-id", RequestID: secondCallbackRequestID, Callbacks: testCompletionCallbacks(1), LegacyEventReference: nil}))

	// Disabling Update callbacks stops new callback attachment but does not leave an existing
	// buffered reference inconsistent with its history event.
	s.mockConfig.EnableCHASMSignalBacklinks = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
	s.mockConfig.EnableWorkflowUpdateCallbacks = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false)
	s.Require().NoError(ms.updatePendingEventIDs(nil, map[string]int64{
		secondSignalRequestID:   21,
		secondCallbackRequestID: 22,
	}))
	s.Equal(int64(21), wf.IncomingSignals[secondSignalRequestID].Get(chasmCtx).GetEventId())
	callbackReference, found = wf.LookupCallbackAttachmentEventReference(chasmCtx, "update-id", secondCallbackRequestID)
	s.True(found)
	s.Equal(int64(22), callbackReference.EventID)
}

func (s *mutableStateSuite) TestApplyUpdateRequestIDAndCallbacksDoesNotInventAttachmentReference() {
	ms := s.enableChasmCallbacks(10)
	_, err := ms.AddWorkflowExecutionStartedEvent(
		&commonpb.WorkflowExecution{WorkflowId: tests.WorkflowID, RunId: tests.RunID},
		&historyservice.StartWorkflowExecutionRequest{
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				RequestId:           "start-request-id",
				CompletionCallbacks: testCompletionCallbacks(1),
			},
		},
	)
	s.Require().NoError(err)

	wf, chasmCtx, err := ms.ChasmWorkflowComponent(context.Background())
	s.Require().NoError(err)
	const (
		updateID  = "update-id"
		requestID = "request-id"
	)
	completionCallbacks := testCompletionCallbacks(1)
	s.Require().NoError(wf.AddUpdateCompletionCallbacks(
		chasmCtx,
		timestamppb.Now(), chasmworkflow.UpdateCallbackAttachment{EventReference: chasmworkflow.CallbackAttachmentEventReference{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED,
			EventID:   11,
		}, UpdateID: updateID, RequestID: requestID, Callbacks: completionCallbacks, LegacyEventReference: nil}))
	// Simulate pre-reference callback state whose legacy attachment entry is unavailable.
	wf.Updates[updateID].Get(chasmCtx).CallbackAttachmentEventReferences = nil

	s.Require().NoError(ms.applyUpdateRequestIDAndCallbacks(
		&historypb.HistoryEvent{
			EventId:   12,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED,
			EventTime: timestamppb.Now(),
		},
		updateID,
		requestID,
		completionCallbacks,
	))

	_, legacyReferenceFound := ms.executionState.RequestIds[requestID]
	s.False(legacyReferenceFound)
	_, chasmReferenceFound := wf.LookupCallbackAttachmentEventReference(chasmCtx, updateID, requestID)
	s.False(chasmReferenceFound)
}

func (s *mutableStateSuite) TestHasCallbackAttachmentUsesCHASMWithoutLegacyRequestID() {
	ms := s.enableChasmCallbacks(10)
	_, err := ms.AddWorkflowExecutionStartedEvent(
		&commonpb.WorkflowExecution{WorkflowId: tests.WorkflowID, RunId: tests.RunID},
		&historyservice.StartWorkflowExecutionRequest{
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				RequestId:           "start-request-id",
				CompletionCallbacks: testCompletionCallbacks(1),
			},
		},
	)
	s.Require().NoError(err)

	const (
		updateID  = "update-id"
		requestID = "request-id"
	)
	wf, chasmCtx, err := ms.ChasmWorkflowComponent(context.Background())
	s.Require().NoError(err)
	s.Require().NoError(wf.AddUpdateCompletionCallbacks(
		chasmCtx,
		timestamppb.Now(),
		chasmworkflow.UpdateCallbackAttachment{
			UpdateID:  updateID,
			RequestID: requestID,
			Callbacks: testCompletionCallbacks(1),
			EventReference: chasmworkflow.CallbackAttachmentEventReference{
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
				EventID:   12,
			},
		},
	))
	s.NotContains(ms.executionState.RequestIds, requestID)

	attached, err := (MutableStateWithEffects{MutableState: ms}).HasCallbackAttachment(updateID, requestID)
	s.Require().NoError(err)
	s.True(attached)
}

func (s *mutableStateSuite) TestChasmCompletionCallbacks_ValidateCallbackAddition() {
	ms := s.enableChasmCallbacks(2)

	s.NoError(ms.ValidateCallbackAddition(nil, chasmworkflow.CallbackAddition{
		RequestID: "req-1",
		Callbacks: testCompletionCallbacks(2),
	}))

	err := ms.ValidateCallbackAddition(nil, chasmworkflow.CallbackAddition{
		RequestID: "req-1",
		Callbacks: testCompletionCallbacks(3),
	})
	var failedPrecondition *serviceerror.FailedPrecondition
	s.ErrorAs(err, &failedPrecondition)
	s.ErrorContains(err, "cannot attach more than 2 callbacks to an execution")
}

// Validation is left to request handlers, so attaching never checks the limits, and the totals
// track exactly what was attached.
func (s *mutableStateSuite) TestChasmCompletionCallbacks_AttachingDoesNotValidate() {
	ms := s.enableChasmCallbacks(2)

	_, err := ms.AddWorkflowExecutionStartedEvent(
		&commonpb.WorkflowExecution{WorkflowId: tests.WorkflowID, RunId: tests.RunID},
		&historyservice.StartWorkflowExecutionRequest{
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				RequestId:           "req-1",
				CompletionCallbacks: testCompletionCallbacks(3),
			},
		},
	)
	s.NoError(err)

	wf := s.chasmWorkflowComponent(ms)
	s.Len(wf.Callbacks, 3)
	s.Equal(int64(3), wf.GetCallbackMetadata().GetTotalCallbacksCount())
	s.NotZero(wf.GetCallbackMetadata().GetTotalCallbacksSize())
}

// The rebuild path replays events another cluster already committed. Re-checking the limit
// there would stall the replication task instead of protecting anything, so an over-limit
// event must still apply cleanly.
func (s *mutableStateSuite) TestChasmCompletionCallbacks_LimitNotEnforcedOnRebuildPath() {
	ms := s.enableChasmCallbacks(2)

	cbs := testCompletionCallbacks(3)
	startEvent := &historypb.HistoryEvent{
		EventId:   common.FirstEventID,
		EventTime: timestamppb.New(time.Now().UTC()),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
				WorkflowType:        &commonpb.WorkflowType{Name: "test-workflow-type"},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: "test-task-queue"},
				CompletionCallbacks: cbs,
			},
		},
	}

	err := ms.ApplyWorkflowExecutionStartedEvent(
		nil,
		&commonpb.WorkflowExecution{WorkflowId: tests.WorkflowID, RunId: tests.RunID},
		"req-1",
		startEvent,
	)
	s.NoError(err)

	wf := s.chasmWorkflowComponent(ms)
	s.Len(wf.Callbacks, 3)
	s.Equal(int64(3), wf.GetCallbackMetadata().GetTotalCallbacksCount())
}

// lowerCallbackLimits drops the execution-wide limits below anything the tests attach,
// simulating callbacks that were accepted before an operator lowered the limits.
func (s *mutableStateSuite) lowerCallbackLimits() {
	cfg := test.NewCallbacksValidatorConfig()
	cfg.MaxCallbacksPerExecution = func(string) int { return 1 }
	cfg.TotalCallbacksMaxSize = func(string) int { return 1 }
	s.mockShard.SetCallbackValidator(test.NewCallbacksValidator(s.T(), cfg))
}

// startWithCompletionCallbacks starts ms with n completion callbacks and completes its first
// workflow task, leaving it ready to continue-as-new or retry.
func (s *mutableStateSuite) startWithCompletionCallbacks(
	ms *MutableStateImpl,
	n int,
) (startEvent *historypb.HistoryEvent, completedEvent *historypb.HistoryEvent) {
	tq := &taskqueuepb.TaskQueue{Name: "test-task-queue"}
	startEvent, err := ms.AddWorkflowExecutionStartedEvent(
		&commonpb.WorkflowExecution{WorkflowId: tests.WorkflowID, RunId: tests.RunID},
		&historyservice.StartWorkflowExecutionRequest{
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				RequestId:           "req-1",
				WorkflowType:        &commonpb.WorkflowType{Name: "test-workflow-type"},
				TaskQueue:           tq,
				CompletionCallbacks: testCompletionCallbacks(n),
			},
		},
	)
	s.NoError(err)
	s.mockEventsCache.EXPECT().GetEvent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(startEvent, nil).AnyTimes()

	wft, err := ms.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	s.NoError(err)
	_, wft, err = ms.AddWorkflowTaskStartedEvent(wft.ScheduledEventID, "", tq, "", nil, nil, nil, false, nil, 0)
	s.NoError(err)
	completedEvent, err = ms.AddWorkflowTaskCompletedEvent(
		wft,
		&workflowservice.RespondWorkflowTaskCompletedRequest{},
		workflowTaskCompletionLimits,
	)
	s.NoError(err)
	return startEvent, completedEvent
}

// requireCarriedOverCallbacks checks that the new run kept all of the previous run's callbacks,
// and that the lowered limits still reject anything newly attached to it.
func (s *mutableStateSuite) requireCarriedOverCallbacks(newRun historyi.MutableState, want int) {
	wf, _, err := newRun.ChasmWorkflowComponentReadOnly(context.Background())
	s.NoError(err)
	s.Len(wf.Callbacks, want)
	s.Equal(int64(want), wf.GetCallbackMetadata().GetTotalCallbacksCount())

	err = newRun.ValidateCallbackAddition(nil, chasmworkflow.CallbackAddition{
		RequestID: "req-new",
		Callbacks: testCompletionCallbacks(1),
	})
	var failedPrecondition *serviceerror.FailedPrecondition
	s.ErrorAs(err, &failedPrecondition)
}

// Callbacks carried over to a new run were validated when first attached. Re-validating them
// would fail the workflow task completing the continue-as-new, wedging the workflow, so they
// are kept as-is even though the execution is now above the limits.
func (s *mutableStateSuite) TestChasmCompletionCallbacks_ContinueAsNewKeepsCallbacksOverLimit() {
	ms := s.enableChasmCallbacks(10)
	_, completedEvent := s.startWithCompletionCallbacks(ms, 3)
	s.lowerCallbackLimits()

	_, newRun, err := ms.AddContinueAsNewEvent(
		context.Background(),
		completedEvent.GetEventId(),
		"",
		&commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
			WorkflowRunTimeout: ms.GetExecutionInfo().WorkflowRunTimeout,
		},
		nil,
	)
	s.NoError(err)
	s.requireCarriedOverCallbacks(newRun, 3)
}

func (s *mutableStateSuite) TestChasmCompletionCallbacks_RetryKeepsCallbacksOverLimit() {
	ms := s.enableChasmCallbacks(10)
	startEvent, _ := s.startWithCompletionCallbacks(ms, 3)
	s.lowerCallbackLimits()

	newRunID := uuid.NewString()
	newRun, err := NewMutableStateInChain(
		s.mockShard, s.mockEventsCache, s.logger, s.namespaceEntry, tests.WorkflowID, newRunID, time.Now().UTC(), ms,
	)
	s.NoError(err)
	s.NoError(SetupNewWorkflowForRetryOrCron(
		context.Background(),
		ms,
		newRun,
		newRunID,
		startEvent.GetWorkflowExecutionStartedEventAttributes(),
		nil,
		nil,
		&failurepb.Failure{Message: "retryable failure"},
		time.Second,
		enumspb.CONTINUE_AS_NEW_INITIATOR_RETRY,
	))
	s.requireCarriedOverCallbacks(newRun, 3)
}

// Validation must only read the CHASM tree: the update API calls it before deciding whether
// anything will be written at all.
func (s *mutableStateSuite) TestChasmCompletionCallbacks_ValidationIsReadOnly() {
	ms := s.enableChasmCallbacks(2)
	root, ok := ms.chasmTree.(*chasm.Node)
	s.True(ok)

	s.NoError(ms.ValidateCallbackAddition(nil, chasmworkflow.CallbackAddition{
		RequestID: "req-1",
		Callbacks: testCompletionCallbacks(2),
	}))
	err := ms.ValidateCallbackAddition(nil, chasmworkflow.CallbackAddition{
		RequestID: "req-1",
		Callbacks: testCompletionCallbacks(3),
	})
	var failedPrecondition *serviceerror.FailedPrecondition
	s.ErrorAs(err, &failedPrecondition)

	s.False(root.IsDirty())
}

// The aggregate validator only governs callbacks attached to the CHASM tree. An execution whose
// callbacks go to the HSM tree keeps the HSM limit and must not consult it, even when CHASM
// itself is enabled.
func (s *mutableStateSuite) TestHsmCompletionCallbacks_AggregateValidationNotApplied() {
	ms := s.enableChasmCallbacks(1)
	s.mockConfig.EnableCHASMCallbacks = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false)

	s.NoError(ms.ValidateCallbackAddition(nil, chasmworkflow.CallbackAddition{
		RequestID: "req-1",
		Callbacks: testCompletionCallbacks(3),
	}))

	s.startWithCompletionCallbacks(ms, 3)
	s.Equal(3, callbacks.MachineCollection(ms.HSM()).Size())
	wf := s.chasmWorkflowComponent(ms)
	s.Empty(wf.Callbacks)
	s.Zero(wf.GetCallbackMetadata().GetTotalCallbacksCount())
}

// An Update's callbacks are validated when the Update is admitted, before it reaches a worker.
// Once the worker has accepted it, its callbacks are attached as-is: rejecting them then would
// fail the entire workflow task completion, not just the Update. Nor are callbacks reapplied by
// reset or NDC conflict resolution checked again.
func (s *mutableStateSuite) TestChasmUpdateCallbacks_AttachingDoesNotValidate() {
	ms := s.enableChasmCallbacks(10)
	s.startWithCompletionCallbacks(ms, 0)
	_, err := ms.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	s.NoError(err)
	s.lowerCallbackLimits()

	updateRequest := func(updateID string) *updatepb.Request {
		return &updatepb.Request{
			Meta:                &updatepb.Meta{UpdateId: updateID},
			RequestId:           "req-" + updateID,
			CompletionCallbacks: testCompletionCallbacks(3),
		}
	}
	_, err = ms.AddWorkflowExecutionUpdateAcceptedEvent("accepted", "msg-id", 1, updateRequest("accepted"))
	s.NoError(err)
	_, err = ms.AddWorkflowExecutionUpdateAdmittedEvent(updateRequest("reapplied"), enumspb.UPDATE_ADMITTED_EVENT_ORIGIN_REAPPLY)
	s.NoError(err)
	_, err = ms.AddWorkflowExecutionOptionsUpdatedEvent(
		nil, false, "req-attach", testCompletionCallbacks(3), nil, "", nil, nil, false, nil,
	)
	s.NoError(err)

	wf, ctx, err := ms.ChasmWorkflowComponentReadOnly(context.Background())
	s.NoError(err)
	s.Len(wf.Callbacks, 3)
	s.Len(wf.Updates["accepted"].Get(ctx).Callbacks, 3)
	s.Len(wf.Updates["reapplied"].Get(ctx).Callbacks, 3)
	s.Equal(int64(9), wf.GetCallbackMetadata().GetTotalCallbacksCount())
}

// Callbacks of an Update in flight are reserved against the limits until it is accepted. Accepting
// it moves them into the persisted totals, after which the Registry no longer reports them, so
// they are counted exactly once throughout.
func (s *mutableStateSuite) TestChasmUpdateCallbacks_InFlightReservedUntilAccepted() {
	ms := s.enableChasmCallbacks(3)
	s.startWithCompletionCallbacks(ms, 1)
	_, err := ms.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	s.NoError(err)

	inFlight := chasmworkflow.CallbackAddition{UpdateID: "u1", RequestID: "req-u1", Callbacks: testCompletionCallbacks(2)}
	another := chasmworkflow.CallbackAddition{UpdateID: "u2", RequestID: "req-u2", Callbacks: testCompletionCallbacks(1)}
	var failedPrecondition *serviceerror.FailedPrecondition

	// One persisted and two in flight leave no room.
	err = ms.ValidateCallbackAddition([]chasmworkflow.CallbackAddition{inFlight}, another)
	s.ErrorAs(err, &failedPrecondition)
	s.ErrorContains(err, "(3 callbacks already attached)")
	// A retry of the in-flight request is not counted twice.
	s.NoError(ms.ValidateCallbackAddition([]chasmworkflow.CallbackAddition{inFlight}, inFlight))

	_, err = ms.AddWorkflowExecutionUpdateAcceptedEvent(inFlight.UpdateID, "msg-id", 1, &updatepb.Request{
		Meta:                &updatepb.Meta{UpdateId: inFlight.UpdateID},
		RequestId:           inFlight.RequestID,
		CompletionCallbacks: inFlight.Callbacks,
	})
	s.NoError(err)
	wf := s.chasmWorkflowComponent(ms)
	s.Equal(int64(3), wf.GetCallbackMetadata().GetTotalCallbacksCount())

	err = ms.ValidateCallbackAddition(nil, another)
	s.ErrorAs(err, &failedPrecondition)
	s.ErrorContains(err, "(3 callbacks already attached)")
	// Were the Registry still to report it, the persisted copy is recognized and not recounted.
	err = ms.ValidateCallbackAddition([]chasmworkflow.CallbackAddition{inFlight}, another)
	s.ErrorContains(err, "(3 callbacks already attached)")
}
