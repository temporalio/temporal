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
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
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

func (s *mutableStateSuite) TestChasmCompletionCallbacks_ValidateCallbackAddition() {
	ms := s.enableChasmCallbacks(2)

	s.NoError(ms.ValidateCallbackAddition(chasmworkflow.CallbackAddition{
		RequestID: "req-1",
		Callbacks: testCompletionCallbacks(2),
	}))

	err := ms.ValidateCallbackAddition(chasmworkflow.CallbackAddition{
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
	s.Equal(int64(3), wf.GetTotalCallbacksCount())
	s.NotZero(wf.GetTotalCallbacksSize())
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
	s.Equal(int64(3), wf.GetTotalCallbacksCount())
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
	s.Equal(int64(want), wf.GetTotalCallbacksCount())

	err = newRun.ValidateCallbackAddition(chasmworkflow.CallbackAddition{
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

	s.NoError(ms.ValidateCallbackAddition(chasmworkflow.CallbackAddition{
		RequestID: "req-1",
		Callbacks: testCompletionCallbacks(2),
	}))
	err := ms.ValidateCallbackAddition(chasmworkflow.CallbackAddition{
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

	s.NoError(ms.ValidateCallbackAddition(chasmworkflow.CallbackAddition{
		RequestID: "req-1",
		Callbacks: testCompletionCallbacks(3),
	}))

	s.startWithCompletionCallbacks(ms, 3)
	s.Equal(3, callbacks.MachineCollection(ms.HSM()).Size())
	wf := s.chasmWorkflowComponent(ms)
	s.Empty(wf.Callbacks)
	s.Zero(wf.GetTotalCallbacksCount())
}
