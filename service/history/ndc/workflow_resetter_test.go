package ndc

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	clockspb "go.temporal.io/server/api/clock/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.temporal.io/server/service/history/workflow/update"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	workflowResetterSuite struct {
		suite.Suite
		*require.Assertions

		controller         *gomock.Controller
		mockShard          *shard.ContextTest
		mockStateRebuilder *MockStateRebuilder

		mockExecutionMgr *persistence.MockExecutionManager
		mockTransaction  *workflow.MockTransaction

		logger       log.Logger
		namespaceID  namespace.ID
		workflowID   string
		baseRunID    string
		currentRunID string
		resetRunID   string

		workflowResetter *workflowResetterImpl
	}
)

var (
	testIdentity      = "test identity"
	testRequestReason = "test request reason"
)

func TestWorkflowResetterSuite(t *testing.T) {
	s := new(workflowResetterSuite)
	suite.Run(t, s)
}

func (s *workflowResetterSuite) SetupSuite() {
}

func (s *workflowResetterSuite) TearDownSuite() {
}

func (s *workflowResetterSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.logger = log.NewTestLogger()
	s.controller = gomock.NewController(s.T())
	s.mockStateRebuilder = NewMockStateRebuilder(s.controller)

	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)
	s.mockExecutionMgr = s.mockShard.Resource.ExecutionMgr
	s.mockTransaction = workflow.NewMockTransaction(s.controller)

	s.workflowResetter = NewWorkflowResetter(
		s.mockShard,
		wcache.NewHostLevelCache(s.mockShard.GetConfig(), s.mockShard.GetLogger(), metrics.NoopMetricsHandler),
		s.logger,
	)
	s.workflowResetter.stateRebuilder = s.mockStateRebuilder
	s.workflowResetter.transaction = s.mockTransaction

	s.namespaceID = tests.NamespaceID
	s.workflowID = "some random workflow ID"
	s.baseRunID = uuid.NewString()
	s.currentRunID = uuid.NewString()
	s.resetRunID = uuid.NewString()
}

func (s *workflowResetterSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *workflowResetterSuite) TestPersistToDB_CurrentTerminated() {
	currentWorkflow := NewMockWorkflow(s.controller)
	currentReleaseCalled := false
	currentContext := historyi.NewMockWorkflowContext(s.controller)
	currentMutableState := historyi.NewMockMutableState(s.controller)
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetContext().Return(currentContext).AnyTimes()
	currentWorkflow.EXPECT().GetMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()
	currentMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: s.currentRunID,
	}).AnyTimes()

	currentMutableState.EXPECT().GetCurrentVersion().Return(int64(0)).AnyTimes()
	currentMutableState.EXPECT().IsWorkflow().Return(true).AnyTimes()
	currentNewEventsSize := int64(3444)
	currentMutation := &persistence.WorkflowMutation{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			VersionHistories: versionhistory.NewVersionHistories(&historyspb.VersionHistory{
				BranchToken: []byte{1, 2, 3},
				Items: []*historyspb.VersionHistoryItem{
					{EventId: 234, Version: 0},
				},
			}),
		},
	}
	currentEventsSeq := []*persistence.WorkflowEvents{{
		NamespaceID: s.namespaceID.String(),
		WorkflowID:  s.workflowID,
		RunID:       s.currentRunID,
		BranchToken: []byte("some random current branch token"),
		Events: []*historypb.HistoryEvent{{
			EventId: 234,
		}},
	}}

	resetWorkflow := NewMockWorkflow(s.controller)
	resetReleaseCalled := false
	resetContext := historyi.NewMockWorkflowContext(s.controller)
	resetMutableState := historyi.NewMockMutableState(s.controller)
	var tarGetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { resetReleaseCalled = true }
	resetWorkflow.EXPECT().GetContext().Return(resetContext).AnyTimes()
	resetWorkflow.EXPECT().GetMutableState().Return(resetMutableState).AnyTimes()
	resetWorkflow.EXPECT().GetReleaseFn().Return(tarGetReleaseFn).AnyTimes()

	resetMutableState.EXPECT().GetCurrentVersion().Return(int64(0)).AnyTimes()
	resetNewEventsSize := int64(4321)
	resetSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			VersionHistories: versionhistory.NewVersionHistories(&historyspb.VersionHistory{
				BranchToken: []byte{1, 2, 3},
				Items: []*historyspb.VersionHistoryItem{
					{EventId: 123, Version: 0},
				},
			}),
		},
	}
	resetEventsSeq := []*persistence.WorkflowEvents{{
		NamespaceID: s.namespaceID.String(),
		WorkflowID:  s.workflowID,
		RunID:       s.resetRunID,
		BranchToken: []byte("some random reset branch token"),
		Events: []*historypb.HistoryEvent{{
			EventId: 123,
		}},
	}}
	resetMutableState.EXPECT().CloseTransactionAsSnapshot(
		context.Background(),
		historyi.TransactionPolicyActive,
	).Return(resetSnapshot, resetEventsSeq, nil)

	s.mockTransaction.EXPECT().UpdateWorkflowExecution(
		gomock.Any(),
		persistence.UpdateWorkflowModeUpdateCurrent,
		chasm.WorkflowArchetypeID,
		int64(0),
		currentMutation,
		currentEventsSeq,
		new(int64(0)),
		resetSnapshot,
		resetEventsSeq,
		true, // isWorkflow
	).Return(currentNewEventsSize, resetNewEventsSize, nil)

	err := s.workflowResetter.persistToDB(context.Background(), currentWorkflow, currentWorkflow, currentMutation, currentEventsSeq, resetWorkflow)
	s.NoError(err)
	// persistToDB function is not charged of releasing locks
	s.False(currentReleaseCalled)
	s.False(resetReleaseCalled)
}

func (s *workflowResetterSuite) TestPersistToDB_CurrentNotTerminated() {
	currentWorkflow := NewMockWorkflow(s.controller)
	currentReleaseCalled := false
	currentContext := historyi.NewMockWorkflowContext(s.controller)
	currentMutableState := historyi.NewMockMutableState(s.controller)
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetContext().Return(currentContext).AnyTimes()
	currentWorkflow.EXPECT().GetMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()
	currentMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: s.currentRunID,
	}).AnyTimes()

	currentMutation := &persistence.WorkflowMutation{}
	currentEventsSeq := []*persistence.WorkflowEvents{{}}
	currentMutableState.EXPECT().GetCurrentVersion().Return(int64(0)).AnyTimes()
	currentMutableState.EXPECT().IsWorkflow().Return(true).AnyTimes()
	currentMutableState.EXPECT().CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyActive).Return(currentMutation, currentEventsSeq, nil)

	resetWorkflow := NewMockWorkflow(s.controller)
	resetReleaseCalled := false
	resetContext := historyi.NewMockWorkflowContext(s.controller)
	resetMutableState := historyi.NewMockMutableState(s.controller)
	resetMutableState.EXPECT().GetCurrentVersion().Return(int64(0)).AnyTimes()
	var tarGetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { resetReleaseCalled = true }
	resetWorkflow.EXPECT().GetContext().Return(resetContext).AnyTimes()
	resetWorkflow.EXPECT().GetMutableState().Return(resetMutableState).AnyTimes()
	resetWorkflow.EXPECT().GetReleaseFn().Return(tarGetReleaseFn).AnyTimes()

	resetSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{},
	}
	resetEventsSeq := []*persistence.WorkflowEvents{{
		NamespaceID: s.namespaceID.String(),
		WorkflowID:  s.workflowID,
		RunID:       s.resetRunID,
		BranchToken: []byte("some random reset branch token"),
		Events: []*historypb.HistoryEvent{{
			EventId: 123,
		}},
	}}
	resetMutableState.EXPECT().CloseTransactionAsSnapshot(
		context.Background(),
		historyi.TransactionPolicyActive,
	).Return(resetSnapshot, resetEventsSeq, nil)

	s.mockTransaction.EXPECT().UpdateWorkflowExecution(
		gomock.Any(),
		persistence.UpdateWorkflowModeUpdateCurrent,
		chasm.WorkflowArchetypeID,
		int64(0),
		currentMutation,
		currentEventsSeq,
		new(int64(0)),
		resetSnapshot,
		resetEventsSeq,
		true, // isWorkflow
	).Return(int64(0), int64(0), nil)

	err := s.workflowResetter.persistToDB(context.Background(), currentWorkflow, currentWorkflow, nil, nil, resetWorkflow)
	s.NoError(err)
	// persistToDB function is not charged of releasing locks
	s.False(currentReleaseCalled)
	s.False(resetReleaseCalled)
}

func (s *workflowResetterSuite) TestReplayResetWorkflow() {
	ctx := context.Background()
	baseBranchToken := []byte("some random base branch token")
	baseRebuildLastEventID := int64(1233)
	baseRebuildLastEventVersion := int64(12)

	resetBranchToken := []byte("some random reset branch token")
	resetRequestID := uuid.NewString()
	resetStats := RebuildStats{
		HistorySize:          4411,
		ExternalPayloadSize:  1234,
		ExternalPayloadCount: 56,
	}
	resetMutableState := historyi.NewMockMutableState(s.controller)

	s.mockExecutionMgr.EXPECT().ForkHistoryBranch(gomock.Any(), gomock.Any()).Return(
		&persistence.ForkHistoryBranchResponse{NewBranchToken: resetBranchToken}, nil,
	)

	s.mockStateRebuilder.EXPECT().Rebuild(
		ctx,
		gomock.Any(),
		definition.NewWorkflowKey(
			s.namespaceID.String(),
			s.workflowID,
			s.baseRunID,
		),
		baseBranchToken,
		baseRebuildLastEventID,
		new(baseRebuildLastEventVersion),
		definition.NewWorkflowKey(
			s.namespaceID.String(),
			s.workflowID,
			s.resetRunID,
		),
		resetBranchToken,
		resetRequestID,
	).Return(resetMutableState, resetStats, nil)
	resetMutableState.EXPECT().SetBaseWorkflow(
		s.baseRunID,
		baseRebuildLastEventID,
		baseRebuildLastEventVersion,
	)
	resetMutableState.EXPECT().AddHistorySize(resetStats.HistorySize)
	resetMutableState.EXPECT().AddExternalPayloadSize(resetStats.ExternalPayloadSize)
	resetMutableState.EXPECT().AddExternalPayloadCount(resetStats.ExternalPayloadCount)

	resetWorkflow, err := s.workflowResetter.replayResetWorkflow(
		ctx,
		s.namespaceID,
		s.workflowID,
		s.baseRunID,
		baseBranchToken,
		baseRebuildLastEventID,
		baseRebuildLastEventVersion,
		s.resetRunID,
		resetRequestID,
	)
	s.NoError(err)
	s.Equal(resetMutableState, resetWorkflow.GetMutableState())
}

func (s *workflowResetterSuite) TestFailWorkflowTask_NoWorkflowTask() {
	baseRunID := uuid.NewString()
	baseRebuildLastEventID := int64(1234)
	baseRebuildLastEventVersion := int64(5678)
	resetRunID := uuid.NewString()
	resetReason := "some random reset reason"

	mutableState := historyi.NewMockMutableState(s.controller)
	mutableState.EXPECT().GetPendingWorkflowTask().Return(nil).AnyTimes()

	err := s.workflowResetter.failWorkflowTask(
		mutableState,
		baseRunID,
		baseRebuildLastEventID,
		baseRebuildLastEventVersion,
		resetRunID,
		resetReason,
	)
	s.Error(err)
}

func (s *workflowResetterSuite) TestFailWorkflowTask_WorkflowTaskScheduled() {
	baseRunID := uuid.NewString()
	baseRebuildLastEventID := int64(1234)
	baseRebuildLastEventVersion := int64(5678)
	resetRunID := uuid.NewString()
	resetReason := "some random reset reason"

	mutableState := historyi.NewMockMutableState(s.controller)
	workflowTaskSchedule := &historyi.WorkflowTaskInfo{
		ScheduledEventID: baseRebuildLastEventID - 12,
		StartedEventID:   common.EmptyEventID,
		RequestID:        uuid.NewString(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: "random task queue name",
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
	}
	workflowTaskStart := &historyi.WorkflowTaskInfo{
		ScheduledEventID: workflowTaskSchedule.ScheduledEventID,
		StartedEventID:   workflowTaskSchedule.ScheduledEventID + 1,
		RequestID:        workflowTaskSchedule.RequestID,
		TaskQueue:        workflowTaskSchedule.TaskQueue,
	}
	mutableState.EXPECT().GetPendingWorkflowTask().Return(workflowTaskSchedule).AnyTimes()
	mutableState.EXPECT().AddWorkflowTaskStartedEvent(
		workflowTaskSchedule.ScheduledEventID,
		workflowTaskSchedule.RequestID,
		workflowTaskSchedule.TaskQueue,
		consts.IdentityHistoryService,
		nil,
		nil,
		nil,
		true,
		nil,
		gomock.Any(),
	).Return(&historypb.HistoryEvent{}, workflowTaskStart, nil)
	mutableState.EXPECT().AddWorkflowTaskFailedEvent(
		workflowTaskStart,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW,
		failure.NewResetWorkflowFailure(resetReason, nil),
		consts.IdentityHistoryService,
		nil,
		"",
		baseRunID,
		resetRunID,
		baseRebuildLastEventVersion,
	).Return(&historypb.HistoryEvent{}, nil)

	err := s.workflowResetter.failWorkflowTask(
		mutableState,
		baseRunID,
		baseRebuildLastEventID,
		baseRebuildLastEventVersion,
		resetRunID,
		resetReason,
	)
	s.NoError(err)
}

func (s *workflowResetterSuite) TestFailWorkflowTask_WorkflowTaskStarted() {
	baseRunID := uuid.NewString()
	baseRebuildLastEventID := int64(1234)
	baseRebuildLastEventVersion := int64(5678)
	resetRunID := uuid.NewString()
	resetReason := "some random reset reason"

	mutableState := historyi.NewMockMutableState(s.controller)
	workflowTask := &historyi.WorkflowTaskInfo{
		ScheduledEventID: baseRebuildLastEventID - 12,
		StartedEventID:   baseRebuildLastEventID - 10,
		RequestID:        uuid.NewString(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: "random task queue name",
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
	}
	mutableState.EXPECT().GetPendingWorkflowTask().Return(workflowTask).AnyTimes()
	mutableState.EXPECT().AddWorkflowTaskFailedEvent(
		workflowTask,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW,
		failure.NewResetWorkflowFailure(resetReason, nil),
		consts.IdentityHistoryService,
		nil,
		"",
		baseRunID,
		resetRunID,
		baseRebuildLastEventVersion,
	).Return(&historypb.HistoryEvent{}, nil)

	err := s.workflowResetter.failWorkflowTask(
		mutableState,
		baseRunID,
		baseRebuildLastEventID,
		baseRebuildLastEventVersion,
		resetRunID,
		resetReason,
	)
	s.NoError(err)
}

func (s *workflowResetterSuite) TestFailInflightActivity() {
	now := time.Now().UTC()
	terminateReason := "some random termination reason"

	mutableState := historyi.NewMockMutableState(s.controller)

	activity1 := &persistencespb.ActivityInfo{
		Version:              12,
		ScheduledEventId:     123,
		ScheduledTime:        timestamppb.New(now.Add(-10 * time.Second)),
		FirstScheduledTime:   timestamppb.New(now.Add(-10 * time.Second)),
		StartedEventId:       124,
		LastHeartbeatDetails: payloads.EncodeString("some random activity 1 details"),
		StartedIdentity:      "some random activity 1 started identity",
	}
	activity2 := &persistencespb.ActivityInfo{
		Version:            12,
		ScheduledEventId:   456,
		ScheduledTime:      timestamppb.New(now.Add(-10 * time.Second)),
		FirstScheduledTime: timestamppb.New(now.Add(-10 * time.Second)),
		StartedEventId:     common.EmptyEventID,
	}
	mutableState.EXPECT().GetPendingActivityInfos().Return(map[int64]*persistencespb.ActivityInfo{
		activity1.ScheduledEventId: activity1,
		activity2.ScheduledEventId: activity2,
	}).AnyTimes()

	mutableState.EXPECT().AddActivityTaskFailedEvent(
		activity1.ScheduledEventId,
		activity1.StartedEventId,
		failure.NewResetWorkflowFailure(terminateReason, activity1.LastHeartbeatDetails),
		enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE,
		activity1.StartedIdentity,
		activity1.LastWorkerVersionStamp,
	).Return(&historypb.HistoryEvent{}, nil)

	mutableState.EXPECT().UpdateActivity(gomock.Any(), gomock.Any()).Return(nil)

	err := s.workflowResetter.failInflightActivity(now, mutableState, terminateReason)
	s.NoError(err)
}

func (s *workflowResetterSuite) TestGenerateBranchToken() {
	baseBranchToken := []byte("some random base branch token")
	baseNodeID := int64(1234)

	resetBranchToken := []byte("some random reset branch token")

	shardID := s.mockShard.GetShardID()
	s.mockExecutionMgr.EXPECT().ForkHistoryBranch(gomock.Any(), &persistence.ForkHistoryBranchRequest{
		ForkBranchToken: baseBranchToken,
		ForkNodeID:      baseNodeID,
		Info:            persistence.BuildHistoryGarbageCleanupInfo(s.namespaceID.String(), s.workflowID, s.resetRunID),
		ShardID:         shardID,
		NamespaceID:     s.namespaceID.String(),
		NewRunID:        s.resetRunID,
	}).Return(&persistence.ForkHistoryBranchResponse{NewBranchToken: resetBranchToken}, nil)

	newBranchToken, err := s.workflowResetter.forkAndGenerateBranchToken(
		context.Background(), s.namespaceID, s.workflowID, baseBranchToken, baseNodeID, s.resetRunID,
	)
	s.NoError(err)
	s.Equal(resetBranchToken, newBranchToken)
}

func (s *workflowResetterSuite) TestTerminateWorkflow() {
	workflowTask := &historyi.WorkflowTaskInfo{
		Version:          123,
		ScheduledEventID: 1234,
		StartedEventID:   5678,
	}
	wtFailedEventID := int64(666)
	terminateReason := "some random terminate reason"

	mutableState := historyi.NewMockMutableState(s.controller)

	randomEventID := int64(2208)
	mutableState.EXPECT().GetNextEventID().Return(randomEventID).AnyTimes() // This doesn't matter, GetNextEventID is not used if there is started WT.
	mutableState.EXPECT().GetStartedWorkflowTask().Return(workflowTask)
	mutableState.EXPECT().AddWorkflowTaskFailedEvent(
		workflowTask,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_FORCE_CLOSE_COMMAND,
		nil,
		consts.IdentityHistoryService,
		nil,
		"",
		"",
		"",
		int64(0),
	).Return(&historypb.HistoryEvent{EventId: wtFailedEventID}, nil)
	mutableState.EXPECT().FlushBufferedEvents()
	mutableState.EXPECT().AddWorkflowExecutionTerminatedEvent(
		terminateReason,
		nil,
		consts.IdentityResetter,
		false,
		nil,
	).Return(&historypb.HistoryEvent{}, nil)

	err := s.workflowResetter.terminateWorkflow(mutableState, terminateReason)
	s.NoError(err)
}

func (s *workflowResetterSuite) TestReapplyContinueAsNewWorkflowEvents_WithOutContinueAsNewChain() {
	ctx := context.Background()
	baseFirstEventID := int64(124)
	baseNextEventID := int64(456)
	baseBranchToken := []byte("some random base branch token")

	baseEvent1 := &historypb.HistoryEvent{
		EventId:    124,
		EventType:  enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{}},
	}
	baseEvent2 := &historypb.HistoryEvent{
		EventId:    125,
		EventType:  enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{}},
	}
	baseEvent3 := &historypb.HistoryEvent{
		EventId:    126,
		EventType:  enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{}},
	}
	baseEvent4 := &historypb.HistoryEvent{
		EventId:    127,
		EventType:  enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{}},
	}

	baseEvents := []*historypb.HistoryEvent{baseEvent1, baseEvent2, baseEvent3, baseEvent4}
	shardID := s.mockShard.GetShardID()
	s.mockExecutionMgr.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   baseBranchToken,
		MinEventID:    baseFirstEventID,
		MaxEventID:    baseNextEventID,
		PageSize:      defaultPageSize,
		NextPageToken: nil,
		ShardID:       shardID,
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:       []*historypb.History{{Events: baseEvents}},
		NextPageToken: nil,
	}, nil)

	mutableState := historyi.NewMockMutableState(s.controller)
	currentWorkflow := NewMockWorkflow(s.controller)
	smReg := hsm.NewRegistry()
	s.NoError(workflow.RegisterStateMachine(smReg))
	root, err := hsm.NewRoot(smReg, workflow.StateMachineType, nil, make(map[string]*persistencespb.StateMachineMap), nil)
	s.NoError(err)
	mutableState.EXPECT().HSM().Return(root).AnyTimes()

	lastVisitedRunID, err := s.workflowResetter.reapplyContinueAsNewWorkflowEvents(
		ctx,
		mutableState,
		currentWorkflow,
		s.namespaceID,
		s.workflowID,
		s.baseRunID,
		baseBranchToken,
		baseFirstEventID,
		baseNextEventID,
		nil,
		false, // allowResetWithPendingChildren
	)
	s.NoError(err)
	s.Equal(s.baseRunID, lastVisitedRunID)
}

func (s *workflowResetterSuite) TestReapplyContinueAsNewWorkflowEvents_WithContinueAsNewChain() {
	ctx := context.Background()
	baseFirstEventID := int64(124)
	baseNextEventID := int64(456)
	baseBranchToken := []byte("some random base branch token")

	newRunID := uuid.NewString()
	newFirstEventID := common.FirstEventID
	newNextEventID := int64(6)
	newBranchToken := []byte("some random new branch token")

	baseEvent1 := &historypb.HistoryEvent{
		EventId:    124,
		EventType:  enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{}},
	}
	baseEvent2 := &historypb.HistoryEvent{
		EventId:    125,
		EventType:  enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{}},
	}
	baseEvent3 := &historypb.HistoryEvent{
		EventId:    126,
		EventType:  enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{}},
	}
	baseEvent4 := &historypb.HistoryEvent{
		EventId:   127,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{WorkflowExecutionContinuedAsNewEventAttributes: &historypb.WorkflowExecutionContinuedAsNewEventAttributes{
			NewExecutionRunId: newRunID,
		}},
	}

	newEvent1 := &historypb.HistoryEvent{
		EventId:    1,
		EventType:  enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{}},
	}
	newEvent2 := &historypb.HistoryEvent{
		EventId:    2,
		EventType:  enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{}},
	}
	newEvent3 := &historypb.HistoryEvent{
		EventId:    3,
		EventType:  enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{}},
	}
	newEvent4 := &historypb.HistoryEvent{
		EventId:    4,
		EventType:  enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{}},
	}
	newEvent5 := &historypb.HistoryEvent{
		EventId:    5,
		EventType:  enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{}},
	}

	baseEvents := []*historypb.HistoryEvent{baseEvent1, baseEvent2, baseEvent3, baseEvent4}
	shardID := s.mockShard.GetShardID()
	s.mockExecutionMgr.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   baseBranchToken,
		MinEventID:    baseFirstEventID,
		MaxEventID:    baseNextEventID,
		PageSize:      defaultPageSize,
		NextPageToken: nil,
		ShardID:       shardID,
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:       []*historypb.History{{Events: baseEvents}},
		NextPageToken: nil,
	}, nil)

	newEvents := []*historypb.HistoryEvent{newEvent1, newEvent2, newEvent3, newEvent4, newEvent5}
	s.mockExecutionMgr.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   newBranchToken,
		MinEventID:    newFirstEventID,
		MaxEventID:    newNextEventID,
		PageSize:      defaultPageSize,
		NextPageToken: nil,
		ShardID:       shardID,
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:       []*historypb.History{{Events: newEvents}},
		NextPageToken: nil,
	}, nil)

	resetContext := historyi.NewMockWorkflowContext(s.controller)
	resetContext.EXPECT().Lock(gomock.Any(), locks.PriorityHigh).Return(nil)
	resetContext.EXPECT().Unlock()
	resetContext.EXPECT().IsDirty().Return(false).AnyTimes()
	resetMutableState := historyi.NewMockMutableState(s.controller)
	resetContextCacheKey := wcache.Key{
		WorkflowKey: definition.NewWorkflowKey(s.namespaceID.String(), s.workflowID, newRunID),
		ArchetypeID: chasm.WorkflowArchetypeID,
		ShardUUID:   s.mockShard.GetOwner(),
	}
	resetContext.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(resetMutableState, nil)
	resetMutableState.EXPECT().GetNextEventID().Return(newNextEventID).AnyTimes()
	resetMutableState.EXPECT().GetCurrentBranchToken().Return(newBranchToken, nil).AnyTimes()
	err := wcache.PutContextIfNotExist(s.workflowResetter.workflowCache, resetContextCacheKey, resetContext)
	s.NoError(err)

	mutableState := historyi.NewMockMutableState(s.controller)
	mutableState.EXPECT().GetWorkflowKey().Return(definition.WorkflowKey{RunID: "random-run-id"})
	currentWorkflow := NewMockWorkflow(s.controller)
	currentWorkflow.EXPECT().GetMutableState().Return(mutableState)
	smReg := hsm.NewRegistry()
	s.NoError(workflow.RegisterStateMachine(smReg))
	root, err := hsm.NewRoot(smReg, workflow.StateMachineType, nil, make(map[string]*persistencespb.StateMachineMap), nil)
	s.NoError(err)
	mutableState.EXPECT().HSM().Return(root).AnyTimes()

	lastVisitedRunID, err := s.workflowResetter.reapplyContinueAsNewWorkflowEvents(
		ctx,
		mutableState,
		currentWorkflow,
		s.namespaceID,
		s.workflowID,
		s.baseRunID,
		baseBranchToken,
		baseFirstEventID,
		baseNextEventID,
		nil,
		false, // allowResetWithPendingChildren
	)
	s.NoError(err)
	s.Equal(newRunID, lastVisitedRunID)
}

func (s *workflowResetterSuite) TestReapplyWorkflowEvents() {
	firstEventID := common.FirstEventID
	nextEventID := int64(6)
	branchToken := []byte("some random branch token")

	newRunID := uuid.NewString()
	event1 := &historypb.HistoryEvent{
		EventId:    1,
		EventType:  enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{}},
	}
	event2 := &historypb.HistoryEvent{
		EventId:    2,
		EventType:  enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{}},
	}
	event3 := &historypb.HistoryEvent{
		EventId:    3,
		EventType:  enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{}},
	}
	event4 := &historypb.HistoryEvent{
		EventId:    4,
		EventType:  enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{}},
	}
	event5 := &historypb.HistoryEvent{
		EventId:   5,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{WorkflowExecutionContinuedAsNewEventAttributes: &historypb.WorkflowExecutionContinuedAsNewEventAttributes{
			NewExecutionRunId: newRunID,
		}},
	}
	events := []*historypb.HistoryEvent{event1, event2, event3, event4, event5}
	shardID := s.mockShard.GetShardID()
	s.mockExecutionMgr.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      defaultPageSize,
		NextPageToken: nil,
		ShardID:       shardID,
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:       []*historypb.History{{Events: events}},
		NextPageToken: nil,
	}, nil)

	mutableState := historyi.NewMockMutableState(s.controller)
	smReg := hsm.NewRegistry()
	s.NoError(workflow.RegisterStateMachine(smReg))
	root, err := hsm.NewRoot(smReg, workflow.StateMachineType, nil, make(map[string]*persistencespb.StateMachineMap), nil)
	s.NoError(err)
	mutableState.EXPECT().HSM().Return(root).AnyTimes()

	nextRunID, err := s.workflowResetter.reapplyEventsFromBranch(
		context.Background(),
		mutableState,
		firstEventID,
		nextEventID,
		branchToken,
		nil,
		false, // allowResetWithPendingChildren
		map[string]*persistencespb.ResetChildInfo{},
	)
	s.NoError(err)
	s.Equal(newRunID, nextRunID)
}

// TestReapplyEvents_WithPendingChildren tests applying events related to child workflow.
// It asserts that reapplyEvents() function checks mutableState.GetChildExecutionInfo() before applying the event.
func (s *workflowResetterSuite) TestReapplyEvents_WithPendingChildren() {
	testChildClock := &clockspb.VectorClock{ShardId: 1, Clock: 10, ClusterId: 1}
	testInitiatedEventID := int64(123)
	testChildWFType := &commonpb.WorkflowType{Name: "TEST-CHILD-WF-TYPE"}
	testChildWFExecution := &commonpb.WorkflowExecution{
		WorkflowId: uuid.NewString(),
		RunId:      uuid.NewString(),
	}

	testStartEventHeader := &commonpb.Header{}
	startedEvent := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionStartedEventAttributes{ChildWorkflowExecutionStartedEventAttributes: &historypb.ChildWorkflowExecutionStartedEventAttributes{
			InitiatedEventId:  testInitiatedEventID,
			WorkflowExecution: testChildWFExecution,
			WorkflowType:      testChildWFType,
			Header:            testStartEventHeader,
		}},
	}
	testCompletedEventResult := &commonpb.Payloads{}
	completedEvent := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionCompletedEventAttributes{ChildWorkflowExecutionCompletedEventAttributes: &historypb.ChildWorkflowExecutionCompletedEventAttributes{
			InitiatedEventId:  testInitiatedEventID,
			WorkflowExecution: testChildWFExecution,
			Result:            testCompletedEventResult,
		}},
	}
	startFailedEvent := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED,
		Attributes: &historypb.HistoryEvent_StartChildWorkflowExecutionFailedEventAttributes{StartChildWorkflowExecutionFailedEventAttributes: &historypb.StartChildWorkflowExecutionFailedEventAttributes{
			InitiatedEventId: testInitiatedEventID,
			WorkflowId:       testChildWFExecution.WorkflowId,
			Cause:            enumspb.START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_NAMESPACE_NOT_FOUND,
		}},
	}
	childExecutionFailedEvent := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionFailedEventAttributes{ChildWorkflowExecutionFailedEventAttributes: &historypb.ChildWorkflowExecutionFailedEventAttributes{
			InitiatedEventId:  testInitiatedEventID,
			WorkflowExecution: testChildWFExecution,
			Failure:           &failurepb.Failure{},
			RetryState:        enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE,
		}},
	}
	childExecutionCancelledEvent := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionCanceledEventAttributes{ChildWorkflowExecutionCanceledEventAttributes: &historypb.ChildWorkflowExecutionCanceledEventAttributes{
			InitiatedEventId:  testInitiatedEventID,
			WorkflowExecution: testChildWFExecution,
			Details:           &commonpb.Payloads{},
		}},
	}
	childExecutionTimeoutEvent := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionTimedOutEventAttributes{ChildWorkflowExecutionTimedOutEventAttributes: &historypb.ChildWorkflowExecutionTimedOutEventAttributes{
			InitiatedEventId:  testInitiatedEventID,
			WorkflowExecution: testChildWFExecution,
			RetryState:        enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE,
		}},
	}
	childExecutionTerminatedEvent := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionTerminatedEventAttributes{ChildWorkflowExecutionTerminatedEventAttributes: &historypb.ChildWorkflowExecutionTerminatedEventAttributes{
			InitiatedEventId:  testInitiatedEventID,
			WorkflowExecution: testChildWFExecution,
		}},
	}

	testcases := []struct {
		name   string
		events []*historypb.HistoryEvent
	}{
		{name: "apply started event", events: []*historypb.HistoryEvent{startedEvent}},
		{name: "apply completed event", events: []*historypb.HistoryEvent{completedEvent}},
		{name: "apply start failed event", events: []*historypb.HistoryEvent{startFailedEvent}},
		{name: "apply child failed event", events: []*historypb.HistoryEvent{childExecutionFailedEvent}},
		{name: "apply child cancelled event", events: []*historypb.HistoryEvent{childExecutionCancelledEvent}},
		{name: "apply child timeout event", events: []*historypb.HistoryEvent{childExecutionTimeoutEvent}},
		{name: "apply child terminated event", events: []*historypb.HistoryEvent{childExecutionTerminatedEvent}},
	}
	resetcases := []struct {
		name    string
		isReset bool
	}{
		{name: "reset", isReset: true},
		{name: "no reset", isReset: false},
	}
	for _, tcReset := range resetcases {
		mutableState := historyi.NewMockMutableState(s.controller)
		mutableState.EXPECT().GetChildExecutionInfo(testInitiatedEventID).
			Times(len(testcases)). // GetChildExecutionInfo should be called exactly once for each test case.
			Return(&persistencespb.ChildExecutionInfo{Clock: testChildClock}, true)

		// Each of the events must be picked with the correct args exactly once.
		mutableState.EXPECT().AddChildWorkflowExecutionStartedEvent(testChildWFExecution, testChildWFType, testInitiatedEventID, testStartEventHeader, testChildClock).Return(nil, nil).Times(1)
		mutableState.EXPECT().AddChildWorkflowExecutionCompletedEvent(
			testInitiatedEventID,
			testChildWFExecution,
			&historypb.WorkflowExecutionCompletedEventAttributes{Result: testCompletedEventResult},
		).Return(nil, nil).Times(1)
		mutableState.EXPECT().AddStartChildWorkflowExecutionFailedEvent(
			testInitiatedEventID,
			enumspb.START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_NAMESPACE_NOT_FOUND,
			&historypb.StartChildWorkflowExecutionInitiatedEventAttributes{WorkflowId: testChildWFExecution.WorkflowId},
		).Return(nil, nil).Times(1)
		mutableState.EXPECT().AddChildWorkflowExecutionFailedEvent(
			testInitiatedEventID,
			testChildWFExecution,
			&historypb.WorkflowExecutionFailedEventAttributes{Failure: &failurepb.Failure{}, RetryState: enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE},
		).Return(nil, nil).Times(1)
		mutableState.EXPECT().AddChildWorkflowExecutionCanceledEvent(
			testInitiatedEventID,
			testChildWFExecution,
			&historypb.WorkflowExecutionCanceledEventAttributes{Details: &commonpb.Payloads{}},
		).Return(nil, nil).Times(1)
		mutableState.EXPECT().AddChildWorkflowExecutionTimedOutEvent(
			testInitiatedEventID,
			testChildWFExecution,
			&historypb.WorkflowExecutionTimedOutEventAttributes{RetryState: enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE},
		).Return(nil, nil).Times(1)
		mutableState.EXPECT().AddChildWorkflowExecutionTerminatedEvent(
			testInitiatedEventID,
			testChildWFExecution,
		).Return(nil, nil).Times(1)

		for _, tc := range testcases {
			s.Run(tc.name+" "+tcReset.name, func() {
				_, err := reapplyEvents(context.Background(), mutableState, nil, nil, tc.events, nil, "", tcReset.isReset)
				s.NoError(err)
			})
		}
	}
}

// TestReapplyEvents_WithNoPendingChildren asserts that none of the child events are picked when there is no pending child correspondng to the init event ID.
func (s *workflowResetterSuite) TestReapplyEvents_WithNoPendingChildren() {
	testInitiatedEventID := int64(123)
	startedEvent := &historypb.HistoryEvent{
		EventType:  enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionStartedEventAttributes{ChildWorkflowExecutionStartedEventAttributes: &historypb.ChildWorkflowExecutionStartedEventAttributes{InitiatedEventId: testInitiatedEventID}},
	}
	completedEvent := &historypb.HistoryEvent{
		EventType:  enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionCompletedEventAttributes{ChildWorkflowExecutionCompletedEventAttributes: &historypb.ChildWorkflowExecutionCompletedEventAttributes{InitiatedEventId: testInitiatedEventID}},
	}
	startFailedEvent := &historypb.HistoryEvent{
		EventType:  enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED,
		Attributes: &historypb.HistoryEvent_StartChildWorkflowExecutionFailedEventAttributes{StartChildWorkflowExecutionFailedEventAttributes: &historypb.StartChildWorkflowExecutionFailedEventAttributes{InitiatedEventId: testInitiatedEventID}},
	}
	childExecutionFailedEvent := &historypb.HistoryEvent{
		EventType:  enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionFailedEventAttributes{ChildWorkflowExecutionFailedEventAttributes: &historypb.ChildWorkflowExecutionFailedEventAttributes{InitiatedEventId: testInitiatedEventID}},
	}
	childExecutionCancelledEvent := &historypb.HistoryEvent{
		EventType:  enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionCanceledEventAttributes{ChildWorkflowExecutionCanceledEventAttributes: &historypb.ChildWorkflowExecutionCanceledEventAttributes{InitiatedEventId: testInitiatedEventID}},
	}
	childExecutionTimeoutEvent := &historypb.HistoryEvent{
		EventType:  enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionTimedOutEventAttributes{ChildWorkflowExecutionTimedOutEventAttributes: &historypb.ChildWorkflowExecutionTimedOutEventAttributes{InitiatedEventId: testInitiatedEventID}},
	}
	childExecutionTerminatedEvent := &historypb.HistoryEvent{
		EventType:  enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionTerminatedEventAttributes{ChildWorkflowExecutionTerminatedEventAttributes: &historypb.ChildWorkflowExecutionTerminatedEventAttributes{InitiatedEventId: testInitiatedEventID}},
	}

	testCases := []struct {
		name   string
		events []*historypb.HistoryEvent
	}{
		{name: "apply started event", events: []*historypb.HistoryEvent{startedEvent}},
		{name: "apply completed event", events: []*historypb.HistoryEvent{completedEvent}},
		{name: "apply start failed event", events: []*historypb.HistoryEvent{startFailedEvent}},
		{name: "apply child failed event", events: []*historypb.HistoryEvent{childExecutionFailedEvent}},
		{name: "apply child cancelled event", events: []*historypb.HistoryEvent{childExecutionCancelledEvent}},
		{name: "apply child timeout event", events: []*historypb.HistoryEvent{childExecutionTimeoutEvent}},
		{name: "apply child terminated event", events: []*historypb.HistoryEvent{childExecutionTerminatedEvent}},
	}
	resetcases := []struct {
		name    string
		isReset bool
	}{
		{name: "reset", isReset: true},
		{name: "no reset", isReset: false},
	}
	for _, tcReset := range resetcases {
		mutableState := historyi.NewMockMutableState(s.controller)
		// GetChildExecutionInfo should be called exactly once for each test case and none of the Add event methods must be called.
		mutableState.EXPECT().GetChildExecutionInfo(testInitiatedEventID).
			Times(len(testCases)).
			Return(nil, false)

		for _, tc := range testCases {
			s.Run(tc.name+" "+tcReset.name, func() {
				_, err := reapplyEvents(context.Background(), mutableState, nil, nil, tc.events, nil, "", tcReset.isReset)
				s.NoError(err)
			})
		}
	}
}

func (s *workflowResetterSuite) TestReapplyEvents() {

	event1 := &historypb.HistoryEvent{
		EventId:   101,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
			SignalName: "signal-name-1",
			Input:      payloads.EncodeString("signal-input-1"),
			Identity:   "signal-identity-1",
			Header:     &commonpb.Header{Fields: map[string]*commonpb.Payload{"myheader": {Data: []byte("myheader")}}},
			RequestId:  "signal-request-id-1",
		}},
	}
	// This event is not reapplied
	event2 := &historypb.HistoryEvent{
		EventId:   102,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{
			WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{},
		},
	}
	event3 := &historypb.HistoryEvent{
		EventId:   103,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{
			WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
				SignalName: "signal-name-2",
				Input:      payloads.EncodeString("signal-input-2"),
				Identity:   "signal-identity-2",
				RequestId:  "signal-request-id-2",
			},
		},
	}
	event4 := &historypb.HistoryEvent{
		EventId:   104,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionUpdateAdmittedEventAttributes{
			WorkflowExecutionUpdateAdmittedEventAttributes: &historypb.WorkflowExecutionUpdateAdmittedEventAttributes{
				Request: &updatepb.Request{Input: &updatepb.Input{Args: payloads.EncodeString("update-request-payload-1")}},
				Origin:  enumspb.UPDATE_ADMITTED_EVENT_ORIGIN_UNSPECIFIED,
			},
		},
	}
	event5 := &historypb.HistoryEvent{
		EventId:   105,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionUpdateAcceptedEventAttributes{
			WorkflowExecutionUpdateAcceptedEventAttributes: &historypb.WorkflowExecutionUpdateAcceptedEventAttributes{
				AcceptedRequest: &updatepb.Request{Input: &updatepb.Input{Args: payloads.EncodeString("update-request-payload-1")}},
			},
		},
	}
	// This event is not reapplied
	event6 := &historypb.HistoryEvent{
		EventId:   106,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionUpdateCompletedEventAttributes{
			WorkflowExecutionUpdateCompletedEventAttributes: &historypb.WorkflowExecutionUpdateCompletedEventAttributes{},
		},
	}
	event7 := &historypb.HistoryEvent{
		EventId:   107,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCancelRequestedEventAttributes{
			WorkflowExecutionCancelRequestedEventAttributes: &historypb.WorkflowExecutionCancelRequestedEventAttributes{
				Cause:    testRequestReason,
				Identity: testIdentity,
			},
		},
	}
	event8 := &historypb.HistoryEvent{
		EventId:   108,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCancelRequestedEventAttributes{
			WorkflowExecutionCancelRequestedEventAttributes: &historypb.WorkflowExecutionCancelRequestedEventAttributes{
				Cause:    "duplicated cancel cause",
				Identity: "duplicated cancel identity",
			},
		},
	}
	event9 := &historypb.HistoryEvent{
		EventId:   109,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{
			WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{
				Reason:   testRequestReason,
				Details:  payloads.EncodeString("test details"),
				Identity: testIdentity,
			},
		},
	}
	event10 := &historypb.HistoryEvent{
		EventId:   110,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionOptionsUpdatedEventAttributes{
			WorkflowExecutionOptionsUpdatedEventAttributes: &historypb.WorkflowExecutionOptionsUpdatedEventAttributes{
				AttachedRequestId: "test attached request id",
			},
		},
	}
	// This event is not reapplied
	event11 := &historypb.HistoryEvent{
		EventId:   111,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{
			WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{
				Reason:   testRequestReason,
				Details:  payloads.EncodeString("test details"),
				Identity: consts.IdentityHistoryService,
			},
		},
	}
	// This event is not reapplied
	event12 := &historypb.HistoryEvent{
		EventId:   112,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{
			WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{
				Reason:   testRequestReason,
				Details:  payloads.EncodeString("test details"),
				Identity: consts.IdentityResetter,
			},
		},
	}
	events := []*historypb.HistoryEvent{event1, event2, event3, event4, event5, event6, event7, event8, event9, event10, event11, event12}

	testcases := []struct {
		name     string
		isReset  bool
		expected []*historypb.HistoryEvent
	}{
		{
			name:     "reset",
			isReset:  true,
			expected: []*historypb.HistoryEvent{event1, event3, event4, event5, event10},
		},
		{
			name:     "not reset",
			isReset:  false,
			expected: []*historypb.HistoryEvent{event1, event3, event4, event5, event7, event8, event9, event10},
		},
	}

	ms := historyi.NewMockMutableState(s.controller)

	for _, tc := range testcases {
		s.Run(tc.name, func() {
			smReg := hsm.NewRegistry()
			s.NoError(workflow.RegisterStateMachine(smReg))
			root, err := hsm.NewRoot(smReg, workflow.StateMachineType, nil, make(map[string]*persistencespb.StateMachineMap), nil)
			s.NoError(err)
			ms.EXPECT().HSM().Return(root).AnyTimes()

			for _, event := range events {
				expected := slices.ContainsFunc(tc.expected, func(e *historypb.HistoryEvent) bool {
					return e.GetEventId() == event.GetEventId()
				})
				if !expected {
					continue
				}
				switch event.GetEventType() { // nolint:exhaustive
				case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED:
					attr := event.GetWorkflowExecutionOptionsUpdatedEventAttributes()
					ms.EXPECT().AddWorkflowExecutionOptionsUpdatedEvent(
						attr.GetVersioningOverride(),
						attr.GetUnsetVersioningOverride(),
						attr.GetAttachedRequestId(),
						attr.GetAttachedCompletionCallbacks(),
						event.Links,
						attr.GetIdentity(),
						attr.GetPriority(),
						attr.GetTimeSkippingConfig(),
						attr.GetTimeSkippingConfigUpdated(),
						attr.GetWorkflowUpdateOptions(),
					).Return(&historypb.HistoryEvent{}, nil)
				case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
					attr := event.GetWorkflowExecutionSignaledEventAttributes()
					ms.EXPECT().AddWorkflowExecutionSignaled(
						attr.GetSignalName(),
						attr.GetInput(),
						attr.GetIdentity(),
						attr.GetHeader(),
						attr.GetRequestId(),
						event.Links,
					).Return(&historypb.HistoryEvent{}, nil)
				case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED:
					attr := event.GetWorkflowExecutionUpdateAdmittedEventAttributes()
					ms.EXPECT().AddWorkflowExecutionUpdateAdmittedEvent(
						attr.GetRequest(),
						enumspb.UPDATE_ADMITTED_EVENT_ORIGIN_UNSPECIFIED,
					).Return(&historypb.HistoryEvent{}, nil)
				case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED:
					attr := event.GetWorkflowExecutionUpdateAcceptedEventAttributes()
					ms.EXPECT().AddWorkflowExecutionUpdateAdmittedEvent(
						attr.GetAcceptedRequest(),
						enumspb.UPDATE_ADMITTED_EVENT_ORIGIN_REAPPLY,
					).Return(&historypb.HistoryEvent{}, nil)
				case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED:
					if !tc.isReset {
						attr := event.GetWorkflowExecutionCancelRequestedEventAttributes()
						ms.EXPECT().IsCancelRequested().Return(false)
						ms.EXPECT().AddWorkflowExecutionCancelRequestedEvent(
							&historyservice.RequestCancelWorkflowExecutionRequest{
								CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{
									Reason:   attr.GetCause(),
									Identity: attr.GetIdentity(),
									Links:    event.Links,
								},
								ExternalInitiatedEventId:  attr.GetExternalInitiatedEventId(),
								ExternalWorkflowExecution: attr.GetExternalWorkflowExecution(),
							},
						).Return(&historypb.HistoryEvent{}, nil)
					}
				case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
					if !tc.isReset {
						ms.EXPECT().GetStartedWorkflowTask().Return(nil)
						attr := event.GetWorkflowExecutionTerminatedEventAttributes()
						ms.EXPECT().AddWorkflowExecutionTerminatedEvent(
							attr.GetReason(),
							attr.GetDetails(),
							attr.GetIdentity(),
							false,
							event.Links,
						).Return(&historypb.HistoryEvent{}, nil)
					}
				}
			}

			appliedEvents, err := reapplyEvents(context.Background(), ms, nil, smReg, events, nil, "", tc.isReset)
			s.NoError(err)

			s.Equal(tc.expected, appliedEvents)
		})
	}
}

func (s *workflowResetterSuite) TestReapplyEvents_Excludes() {
	event1 := &historypb.HistoryEvent{
		EventId:   101,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
			SignalName: "signal-name-1",
			Input:      payloads.EncodeString("signal-input-1"),
			Identity:   "signal-identity-1",
			Header:     &commonpb.Header{Fields: map[string]*commonpb.Payload{"myheader": {Data: []byte("myheader")}}},
		}},
	}
	event2 := &historypb.HistoryEvent{
		EventId:   102,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionUpdateAdmittedEventAttributes{
			WorkflowExecutionUpdateAdmittedEventAttributes: &historypb.WorkflowExecutionUpdateAdmittedEventAttributes{
				Request: &updatepb.Request{Input: &updatepb.Input{Args: payloads.EncodeString("update-request-payload-1")}},
				Origin:  enumspb.UPDATE_ADMITTED_EVENT_ORIGIN_UNSPECIFIED,
			},
		},
	}
	event3 := &historypb.HistoryEvent{
		EventId:   103,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionUpdateAcceptedEventAttributes{
			WorkflowExecutionUpdateAcceptedEventAttributes: &historypb.WorkflowExecutionUpdateAcceptedEventAttributes{
				AcceptedRequest: &updatepb.Request{Input: &updatepb.Input{Args: payloads.EncodeString("update-request-payload-1")}},
			},
		},
	}
	event4 := &historypb.HistoryEvent{
		EventId:   104,
		EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED,
	}
	event5 := &historypb.HistoryEvent{
		EventId:   105,
		EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED,
	}
	event6 := &historypb.HistoryEvent{
		EventId:   106,
		EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED,
	}
	events := []*historypb.HistoryEvent{event1, event2, event3, event4, event5, event6}

	ms := historyi.NewMockMutableState(s.controller)
	// Assert that none of these following methods are invoked.
	arg := gomock.Any()
	ms.EXPECT().AddWorkflowExecutionSignaled(arg, arg, arg, arg, arg, arg).Times(0)
	ms.EXPECT().AddWorkflowExecutionUpdateAdmittedEvent(arg, arg).Times(0)
	ms.EXPECT().AddHistoryEvent(arg, arg).Times(0)

	smReg := hsm.NewRegistry()
	s.NoError(smReg.RegisterEventDefinition(nexusoperations.StartedEventDefinition{}))
	s.NoError(workflow.RegisterStateMachine(smReg))
	root, err := hsm.NewRoot(smReg, workflow.StateMachineType, nil, make(map[string]*persistencespb.StateMachineMap), nil)
	s.NoError(err)
	ms.EXPECT().HSM().Return(root).AnyTimes()

	excludes := map[enumspb.ResetReapplyExcludeType]struct{}{
		enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL: {},
		enumspb.RESET_REAPPLY_EXCLUDE_TYPE_UPDATE: {},
		enumspb.RESET_REAPPLY_EXCLUDE_TYPE_NEXUS:  {},
	}
	reappliedEvents, err := reapplyEvents(context.Background(), ms, nil, smReg, events, excludes, "", false)
	s.Empty(reappliedEvents)
	s.NoError(err)

	event7 := &historypb.HistoryEvent{
		EventId:   107,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCancelRequestedEventAttributes{
			WorkflowExecutionCancelRequestedEventAttributes: &historypb.WorkflowExecutionCancelRequestedEventAttributes{
				Cause:    testRequestReason,
				Identity: testIdentity,
			},
		},
	}
	event8 := &historypb.HistoryEvent{
		EventId:   108,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{
			WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{
				Reason:   testRequestReason,
				Details:  payloads.EncodeString("test details"),
				Identity: testIdentity,
			},
		},
	}
	events = append(events, event7, event8)
	reappliedEvents, err = reapplyEvents(context.Background(), ms, nil, smReg, events, excludes, "", true)
	s.Empty(reappliedEvents)
	s.NoError(err)
}

func (s *workflowResetterSuite) TestReapplyContinueAsNewWorkflowEvents_ExcludeAllEvents() {
	ctx := context.Background()
	baseFirstEventID := int64(123)
	baseNextEventID := int64(456)
	baseBranchToken := []byte("some random base branch token")
	optionExcludeAllReapplyEvents := map[enumspb.ResetReapplyExcludeType]struct{}{
		enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL:         {},
		enumspb.RESET_REAPPLY_EXCLUDE_TYPE_UPDATE:         {},
		enumspb.RESET_REAPPLY_EXCLUDE_TYPE_NEXUS:          {},
		enumspb.RESET_REAPPLY_EXCLUDE_TYPE_CANCEL_REQUEST: {},
	}

	mutableState := historyi.NewMockMutableState(s.controller)
	currentWorkflow := NewMockWorkflow(s.controller)

	// Assert that we don't read any history events when we are asked to exclude all reapply events.
	s.mockExecutionMgr.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), gomock.Any()).Times(0)
	// Make sure that we don't access the mutable state of the current workflow since there is nothing to update in this case.
	currentWorkflow.EXPECT().GetMutableState().Times(0)

	lastVisitedRunID, err := s.workflowResetter.reapplyContinueAsNewWorkflowEvents(
		ctx,
		mutableState,
		currentWorkflow,
		s.namespaceID,
		s.workflowID,
		s.baseRunID,
		baseBranchToken,
		baseFirstEventID,
		baseNextEventID,
		optionExcludeAllReapplyEvents,
		false, // allowResetWithPendingChildren
	)
	s.NoError(err)
	s.Equal(s.baseRunID, lastVisitedRunID)
}

func (s *workflowResetterSuite) TestPagination() {
	firstEventID := common.FirstEventID
	nextEventID := int64(101)
	branchToken := []byte("some random branch token")

	event1 := &historypb.HistoryEvent{
		EventId:    1,
		EventType:  enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{}},
	}
	event2 := &historypb.HistoryEvent{
		EventId:    2,
		EventType:  enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{}},
	}
	event3 := &historypb.HistoryEvent{
		EventId:    3,
		EventType:  enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{}},
	}
	event4 := &historypb.HistoryEvent{
		EventId:    4,
		EventType:  enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{}},
	}
	event5 := &historypb.HistoryEvent{
		EventId:    5,
		EventType:  enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{}},
	}
	history1 := []*historypb.History{{Events: []*historypb.HistoryEvent{event1, event2, event3}}}
	history2 := []*historypb.History{{Events: []*historypb.HistoryEvent{event4, event5}}}
	history := append(history1, history2...)
	pageToken := []byte("some random token")

	shardID := s.mockShard.GetShardID()
	s.mockExecutionMgr.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      defaultPageSize,
		NextPageToken: nil,
		ShardID:       shardID,
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:       history1,
		NextPageToken: pageToken,
		Size:          12345,
	}, nil)
	s.mockExecutionMgr.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      defaultPageSize,
		NextPageToken: pageToken,
		ShardID:       shardID,
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:       history2,
		NextPageToken: nil,
		Size:          67890,
	}, nil)

	paginationFn := s.workflowResetter.getPaginationFn(context.Background(), firstEventID, nextEventID, branchToken)
	iter := collection.NewPagingIterator(paginationFn)

	var result []*historypb.History
	for iter.HasNext() {
		item, err := iter.Next()
		s.NoError(err)
		result = append(result, item)
	}

	s.Equal(history, result)
}

func (s *workflowResetterSuite) TestWorkflowRestartAfterExecutionTimeout() {
	ctx := context.Background()
	baseBranchToken := []byte("some random base branch token")
	baseRebuildLastEventID := int64(1234)
	baseRebuildLastEventVersion := int64(12)
	resetWorkflowVersion := int64(0)
	resetReason := "some random reset reason"

	resetBranchToken := []byte("some random reset branch token")
	resetRequestID := uuid.NewString()
	resetStats := RebuildStats{
		HistorySize:          4411,
		ExternalPayloadSize:  1234,
		ExternalPayloadCount: 56,
	}
	resetMutableState := historyi.NewMockMutableState(s.controller)
	executionInfos := make(map[int64]*persistencespb.ChildExecutionInfo)

	workflowTaskSchedule := &historyi.WorkflowTaskInfo{
		ScheduledEventID: baseRebuildLastEventID - 12,
		StartedEventID:   common.EmptyEventID,
		RequestID:        uuid.NewString(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: "random task queue name",
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
	}

	s.mockExecutionMgr.EXPECT().ForkHistoryBranch(gomock.Any(), gomock.Any()).Return(
		&persistence.ForkHistoryBranchResponse{NewBranchToken: resetBranchToken}, nil,
	)

	s.mockStateRebuilder.EXPECT().Rebuild(
		ctx,
		gomock.Any(),
		definition.NewWorkflowKey(s.namespaceID.String(), s.workflowID, s.baseRunID),
		baseBranchToken,
		baseRebuildLastEventID,
		new(baseRebuildLastEventVersion),
		definition.NewWorkflowKey(s.namespaceID.String(), s.workflowID, s.resetRunID),
		resetBranchToken,
		resetRequestID,
	).Return(resetMutableState, resetStats, nil)

	resetMutableState.EXPECT().SetBaseWorkflow(s.baseRunID, baseRebuildLastEventID, baseRebuildLastEventVersion)
	resetMutableState.EXPECT().AddHistorySize(resetStats.HistorySize)
	resetMutableState.EXPECT().AddExternalPayloadSize(resetStats.ExternalPayloadSize)
	resetMutableState.EXPECT().AddExternalPayloadCount(resetStats.ExternalPayloadCount)
	resetMutableState.EXPECT().GetCurrentVersion().Return(resetWorkflowVersion).AnyTimes()
	resetMutableState.EXPECT().UpdateCurrentVersion(resetWorkflowVersion, false).Return(nil).AnyTimes()
	resetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId:  resetRequestID,
		Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}).AnyTimes()
	resetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{}).AnyTimes()
	resetMutableState.EXPECT().GetPendingActivityInfos().Return(map[int64]*persistencespb.ActivityInfo{})
	resetMutableState.EXPECT().RefreshExpirationTimeoutTask(ctx).Return(nil).AnyTimes()
	resetMutableState.EXPECT().GetPendingChildExecutionInfos().Return(executionInfos)
	resetMutableState.EXPECT().GetPendingWorkflowTask().Return(workflowTaskSchedule).AnyTimes()
	smReg := hsm.NewRegistry()
	s.NoError(workflow.RegisterStateMachine(smReg))
	root, err := hsm.NewRoot(smReg, workflow.StateMachineType, nil, make(map[string]*persistencespb.StateMachineMap), nil)
	s.NoError(err)
	resetMutableState.EXPECT().HSM().Return(root).AnyTimes()

	workflowTaskStart := &historyi.WorkflowTaskInfo{
		ScheduledEventID: workflowTaskSchedule.ScheduledEventID,
		StartedEventID:   workflowTaskSchedule.ScheduledEventID + 1,
		RequestID:        workflowTaskSchedule.RequestID,
		TaskQueue:        workflowTaskSchedule.TaskQueue,
	}
	resetMutableState.EXPECT().AddWorkflowTaskStartedEvent(
		workflowTaskSchedule.ScheduledEventID,
		workflowTaskSchedule.RequestID,
		workflowTaskSchedule.TaskQueue,
		consts.IdentityHistoryService,
		nil,
		nil,
		nil,
		true,
		nil,
		gomock.Any(),
	).Return(&historypb.HistoryEvent{}, workflowTaskStart, nil)

	resetMutableState.EXPECT().AddWorkflowTaskFailedEvent(
		workflowTaskStart,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW,
		failure.NewResetWorkflowFailure(resetReason, nil),
		consts.IdentityHistoryService,
		nil,
		"",
		s.baseRunID,
		s.resetRunID,
		baseRebuildLastEventVersion,
	).Return(&historypb.HistoryEvent{}, nil)

	resetWorkflow, err := s.workflowResetter.prepareResetWorkflow(
		ctx,
		s.namespaceID,
		s.workflowID,
		s.baseRunID,
		baseBranchToken,
		baseRebuildLastEventID,
		baseRebuildLastEventVersion,
		s.resetRunID,
		resetRequestID,
		resetWorkflowVersion,
		resetReason,
		false, // allowResetWithPendingChildren
	)
	s.NoError(err)
	s.Equal(resetMutableState, resetWorkflow.GetMutableState())
}

func (s *workflowResetterSuite) TestReapplyEvents_WorkflowOptionsUpdated_CompletionCallbackErrors() {
	testCases := []struct {
		name                  string
		requestIDExists       bool
		totalCallbacks        int
		hasVersioningOverride bool
		expectedErrorContains string
	}{
		{
			name:                  "callbacks_exist_with_additional_updates",
			requestIDExists:       true,
			totalCallbacks:        3,
			hasVersioningOverride: true,
			expectedErrorContains: "unable to reapply WorkflowExecutionOptionsUpdated event: 3 completion callbacks are already attached but the event contains additional workflow option updates",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			ms := historyi.NewMockMutableState(s.controller)
			smReg := hsm.NewRegistry()
			s.NoError(workflow.RegisterStateMachine(smReg))
			root, err := hsm.NewRoot(smReg, workflow.StateMachineType, nil, make(map[string]*persistencespb.StateMachineMap), nil)
			s.NoError(err)
			ms.EXPECT().HSM().Return(root).AnyTimes()

			// Create completion callbacks
			callbacks := make([]*commonpb.Callback, tc.totalCallbacks)
			for i := 0; i < tc.totalCallbacks; i++ {
				callbacks[i] = &commonpb.Callback{
					Variant: &commonpb.Callback_Nexus_{
						Nexus: &commonpb.Callback_Nexus{
							Url: "http://example.com",
							Header: map[string]string{
								"test": "value",
							},
						},
					},
				}
			}

			// Create the event
			event := &historypb.HistoryEvent{
				EventId:   101,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionOptionsUpdatedEventAttributes{
					WorkflowExecutionOptionsUpdatedEventAttributes: &historypb.WorkflowExecutionOptionsUpdatedEventAttributes{
						AttachedRequestId:           "test-request-id",
						AttachedCompletionCallbacks: callbacks,
					},
				},
			}

			// Add versioning override if specified
			if tc.hasVersioningOverride {
				event.GetWorkflowExecutionOptionsUpdatedEventAttributes().VersioningOverride = &workflowpb.VersioningOverride{
					Behavior: enumspb.VERSIONING_BEHAVIOR_PINNED,
				}
			}

			ms.EXPECT().HasRequestID("test-request-id").Return(tc.requestIDExists)

			events := []*historypb.HistoryEvent{event}

			// Call reapplyEvents and expect an error
			appliedEvents, err := reapplyEvents(context.Background(), ms, nil, smReg, events, nil, "", true)
			s.Error(err)
			s.Contains(err.Error(), tc.expectedErrorContains)
			s.Empty(appliedEvents)
		})
	}
}

func (s *workflowResetterSuite) TestReapplyEvents_WorkflowOptionsUpdated_CompletionCallbacksSkip() {
	ms := historyi.NewMockMutableState(s.controller)
	smReg := hsm.NewRegistry()
	s.NoError(workflow.RegisterStateMachine(smReg))
	root, err := hsm.NewRoot(smReg, workflow.StateMachineType, nil, make(map[string]*persistencespb.StateMachineMap), nil)
	s.NoError(err)
	ms.EXPECT().HSM().Return(root).AnyTimes()

	// Create completion callbacks
	callbacks := []*commonpb.Callback{
		{
			Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{
					Url: "http://example.com",
					Header: map[string]string{
						"test": "value",
					},
				},
			},
		},
	}

	// Create the event where all callbacks exist but no other updates
	event := &historypb.HistoryEvent{
		EventId:   101,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionOptionsUpdatedEventAttributes{
			WorkflowExecutionOptionsUpdatedEventAttributes: &historypb.WorkflowExecutionOptionsUpdatedEventAttributes{
				AttachedRequestId:           "test-request-id",
				AttachedCompletionCallbacks: callbacks,
				// No VersioningOverride and UnsetVersioningOverride is false
			},
		},
	}

	ms.EXPECT().HasRequestID("test-request-id").Return(true)

	events := []*historypb.HistoryEvent{event}

	// Call reapplyEvents - should skip the event (no error, no applied events)
	appliedEvents, err := reapplyEvents(context.Background(), ms, nil, smReg, events, nil, "", true)
	s.NoError(err)
	s.Empty(appliedEvents) // Event should be skipped
}

// wfResetterErrTest is a sentinel error used by the added tests below.
var wfResetterErrTest = serviceerror.NewUnavailable("wfResetter test error")

func (s *workflowResetterSuite) TestIsTerminatedByResetter() {
	resetterEvent := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{
			WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{
				Identity: consts.IdentityResetter,
			},
		},
	}
	s.True(IsTerminatedByResetter(resetterEvent))

	otherIdentityEvent := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{
			WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{
				Identity: "someone else",
			},
		},
	}
	s.False(IsTerminatedByResetter(otherIdentityEvent))

	nonTerminateEvent := &historypb.HistoryEvent{
		EventType:  enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{}},
	}
	s.False(IsTerminatedByResetter(nonTerminateEvent))
}

func (s *workflowResetterSuite) TestPerformPostResetOperations_Empty() {
	resetMS := historyi.NewMockMutableState(s.controller)
	err := s.workflowResetter.performPostResetOperations(context.Background(), resetMS, nil)
	s.NoError(err)
}

func (s *workflowResetterSuite) TestPerformPostResetOperations_UnknownVariant() {
	resetMS := historyi.NewMockMutableState(s.controller)
	// An operation with no variant set hits the default (no-op) case of the switch.
	ops := []*workflowpb.PostResetOperation{{}}
	err := s.workflowResetter.performPostResetOperations(context.Background(), resetMS, ops)
	s.NoError(err)
}

func (s *workflowResetterSuite) TestPerformPostResetOperations_UpdateWorkflowOptions_Error() {
	resetMS := historyi.NewMockMutableState(s.controller)
	// Returning a versioning info with a nil override forces getOptionsFromMutableState to
	// produce a non-nil empty options struct. The invalid update mask path is what we want;
	// supply an empty mask but a non-nil opts so that MergeAndApply attempts a merge with an
	// invalid field path.
	resetMS.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{}).AnyTimes()
	ops := []*workflowpb.PostResetOperation{
		{
			Variant: &workflowpb.PostResetOperation_UpdateWorkflowOptions_{
				UpdateWorkflowOptions: &workflowpb.PostResetOperation_UpdateWorkflowOptions{
					WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{},
					UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"not_a_real_field"}},
				},
			},
		},
	}
	err := s.workflowResetter.performPostResetOperations(context.Background(), resetMS, ops)
	s.Error(err)
}

func (s *workflowResetterSuite) TestFailInflightActivity_TransientActivity() {
	now := time.Now().UTC()
	terminateReason := "some random termination reason"
	mutableState := historyi.NewMockMutableState(s.controller)
	transientActivity := &persistencespb.ActivityInfo{
		ScheduledEventId: 789,
		StartedEventId:   common.TransientEventID,
	}
	mutableState.EXPECT().GetPendingActivityInfos().Return(map[int64]*persistencespb.ActivityInfo{
		transientActivity.ScheduledEventId: transientActivity,
	})
	err := s.workflowResetter.failInflightActivity(now, mutableState, terminateReason)
	s.Error(err)
}

func (s *workflowResetterSuite) TestFailInflightActivity_UpdateActivityError() {
	now := time.Now().UTC()
	terminateReason := "some random termination reason"
	mutableState := historyi.NewMockMutableState(s.controller)
	notStartedActivity := &persistencespb.ActivityInfo{
		ScheduledEventId: 456,
		StartedEventId:   common.EmptyEventID,
	}
	mutableState.EXPECT().GetPendingActivityInfos().Return(map[int64]*persistencespb.ActivityInfo{
		notStartedActivity.ScheduledEventId: notStartedActivity,
	})
	mutableState.EXPECT().UpdateActivity(notStartedActivity.ScheduledEventId, gomock.Any()).Return(wfResetterErrTest)
	err := s.workflowResetter.failInflightActivity(now, mutableState, terminateReason)
	s.ErrorIs(err, wfResetterErrTest)
}

func (s *workflowResetterSuite) TestFailInflightActivity_AddActivityTaskFailedEventError() {
	now := time.Now().UTC()
	terminateReason := "some random termination reason"
	mutableState := historyi.NewMockMutableState(s.controller)
	startedActivity := &persistencespb.ActivityInfo{
		ScheduledEventId: 123,
		StartedEventId:   124,
		StartedIdentity:  "identity",
	}
	mutableState.EXPECT().GetPendingActivityInfos().Return(map[int64]*persistencespb.ActivityInfo{
		startedActivity.ScheduledEventId: startedActivity,
	})
	mutableState.EXPECT().AddActivityTaskFailedEvent(
		startedActivity.ScheduledEventId,
		startedActivity.StartedEventId,
		gomock.Any(),
		enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE,
		startedActivity.StartedIdentity,
		startedActivity.LastWorkerVersionStamp,
	).Return(nil, wfResetterErrTest)
	err := s.workflowResetter.failInflightActivity(now, mutableState, terminateReason)
	s.ErrorIs(err, wfResetterErrTest)
}

func (s *workflowResetterSuite) TestForkAndGenerateBranchToken_Error() {
	baseBranchToken := []byte("some random base branch token")
	s.mockExecutionMgr.EXPECT().ForkHistoryBranch(gomock.Any(), gomock.Any()).Return(nil, wfResetterErrTest)
	newBranchToken, err := s.workflowResetter.forkAndGenerateBranchToken(
		context.Background(), s.namespaceID, s.workflowID, baseBranchToken, int64(1234), s.resetRunID,
	)
	s.ErrorIs(err, wfResetterErrTest)
	s.Nil(newBranchToken)
}

func (s *workflowResetterSuite) TestFailWorkflowTask_AddWorkflowTaskStartedEventError() {
	baseRunID := uuid.NewString()
	baseRebuildLastEventID := int64(1234)
	baseRebuildLastEventVersion := int64(5678)
	resetRunID := uuid.NewString()
	resetReason := "some random reset reason"

	mutableState := historyi.NewMockMutableState(s.controller)
	workflowTaskSchedule := &historyi.WorkflowTaskInfo{
		ScheduledEventID: baseRebuildLastEventID - 12,
		StartedEventID:   common.EmptyEventID,
		RequestID:        uuid.NewString(),
		TaskQueue:        &taskqueuepb.TaskQueue{Name: "tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
	}
	mutableState.EXPECT().GetPendingWorkflowTask().Return(workflowTaskSchedule)
	mutableState.EXPECT().AddWorkflowTaskStartedEvent(
		workflowTaskSchedule.ScheduledEventID,
		workflowTaskSchedule.RequestID,
		workflowTaskSchedule.TaskQueue,
		consts.IdentityHistoryService,
		nil, nil, nil, true, nil, gomock.Any(),
	).Return(nil, nil, wfResetterErrTest)
	err := s.workflowResetter.failWorkflowTask(
		mutableState, baseRunID, baseRebuildLastEventID, baseRebuildLastEventVersion, resetRunID, resetReason,
	)
	s.ErrorIs(err, wfResetterErrTest)
}

func (s *workflowResetterSuite) TestReplayResetWorkflow_ForkError() {
	s.mockExecutionMgr.EXPECT().ForkHistoryBranch(gomock.Any(), gomock.Any()).Return(nil, wfResetterErrTest)
	resetWorkflow, err := s.workflowResetter.replayResetWorkflow(
		context.Background(), s.namespaceID, s.workflowID, s.baseRunID,
		[]byte("base"), int64(100), int64(1), s.resetRunID, uuid.NewString(),
	)
	s.ErrorIs(err, wfResetterErrTest)
	s.Nil(resetWorkflow)
}

func (s *workflowResetterSuite) TestReplayResetWorkflow_RebuildError() {
	s.mockExecutionMgr.EXPECT().ForkHistoryBranch(gomock.Any(), gomock.Any()).Return(
		&persistence.ForkHistoryBranchResponse{NewBranchToken: []byte("reset")}, nil,
	)
	s.mockStateRebuilder.EXPECT().Rebuild(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
		gomock.Any(), gomock.Any(), gomock.Any(),
	).Return(nil, RebuildStats{}, wfResetterErrTest)
	resetWorkflow, err := s.workflowResetter.replayResetWorkflow(
		context.Background(), s.namespaceID, s.workflowID, s.baseRunID,
		[]byte("base"), int64(100), int64(1), s.resetRunID, uuid.NewString(),
	)
	s.ErrorIs(err, wfResetterErrTest)
	s.Nil(resetWorkflow)
}

func (s *workflowResetterSuite) TestGetPaginationFn_Error() {
	s.mockExecutionMgr.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), gomock.Any()).Return(nil, wfResetterErrTest)
	paginationFn := s.workflowResetter.getPaginationFn(context.Background(), common.FirstEventID, int64(10), []byte("bt"))
	_, _, err := paginationFn(nil)
	s.ErrorIs(err, wfResetterErrTest)
}

func (s *workflowResetterSuite) TestReapplyEventsFromBranch_IteratorError() {
	branchToken := []byte("some random branch token")
	s.mockExecutionMgr.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), gomock.Any()).Return(nil, wfResetterErrTest)
	mutableState := historyi.NewMockMutableState(s.controller)
	nextRunID, err := s.workflowResetter.reapplyEventsFromBranch(
		context.Background(), mutableState, common.FirstEventID, int64(10), branchToken, nil, false,
		map[string]*persistencespb.ResetChildInfo{},
	)
	s.ErrorIs(err, wfResetterErrTest)
	s.Empty(nextRunID)
}

func (s *workflowResetterSuite) TestReapplyContinueAsNewWorkflowEvents_DataLossError() {
	ctx := context.Background()
	baseFirstEventID := int64(124)
	baseNextEventID := int64(456)
	baseBranchToken := []byte("some random base branch token")

	s.mockExecutionMgr.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), gomock.Any()).Return(
		nil, serviceerror.NewDataLoss("data loss"),
	)

	mutableState := historyi.NewMockMutableState(s.controller)
	currentWorkflow := NewMockWorkflow(s.controller)
	lastVisitedRunID, err := s.workflowResetter.reapplyContinueAsNewWorkflowEvents(
		ctx, mutableState, currentWorkflow, s.namespaceID, s.workflowID, s.baseRunID,
		baseBranchToken, baseFirstEventID, baseNextEventID, nil, false,
	)
	var dataLoss *serviceerror.DataLoss
	s.ErrorAs(err, &dataLoss)
	s.Empty(lastVisitedRunID)
}

func (s *workflowResetterSuite) TestReapplyContinueAsNewWorkflowEvents_GetNextEventIDError() {
	ctx := context.Background()
	baseFirstEventID := int64(124)
	baseNextEventID := int64(456)
	baseBranchToken := []byte("some random base branch token")
	newRunID := uuid.NewString()

	baseEvent := &historypb.HistoryEvent{
		EventId:   127,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{WorkflowExecutionContinuedAsNewEventAttributes: &historypb.WorkflowExecutionContinuedAsNewEventAttributes{
			NewExecutionRunId: newRunID,
		}},
	}
	s.mockExecutionMgr.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), gomock.Any()).Return(
		&persistence.ReadHistoryBranchByBatchResponse{
			History:       []*historypb.History{{Events: []*historypb.HistoryEvent{baseEvent}}},
			NextPageToken: nil,
		}, nil,
	)

	// The next run is not the current workflow, so it is loaded via the workflow cache.
	// GetOrCreateWorkflowExecution will load the context; force the LoadMutableState path to error.
	loadedContext := historyi.NewMockWorkflowContext(s.controller)
	loadedContext.EXPECT().Lock(gomock.Any(), locks.PriorityHigh).Return(nil)
	loadedContext.EXPECT().Unlock()
	loadedContext.EXPECT().IsDirty().Return(false).AnyTimes()
	loadedContext.EXPECT().Clear().AnyTimes()
	loadedContext.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(nil, wfResetterErrTest)
	cacheKey := wcache.Key{
		WorkflowKey: definition.NewWorkflowKey(s.namespaceID.String(), s.workflowID, newRunID),
		ArchetypeID: chasm.WorkflowArchetypeID,
		ShardUUID:   s.mockShard.GetOwner(),
	}
	s.NoError(wcache.PutContextIfNotExist(s.workflowResetter.workflowCache, cacheKey, loadedContext))

	mutableState := historyi.NewMockMutableState(s.controller)
	currentWorkflow := NewMockWorkflow(s.controller)
	currentMutableState := historyi.NewMockMutableState(s.controller)
	currentMutableState.EXPECT().GetWorkflowKey().Return(definition.WorkflowKey{RunID: "different-run-id"})
	currentWorkflow.EXPECT().GetMutableState().Return(currentMutableState)
	smReg := hsm.NewRegistry()
	s.NoError(workflow.RegisterStateMachine(smReg))
	root, err := hsm.NewRoot(smReg, workflow.StateMachineType, nil, make(map[string]*persistencespb.StateMachineMap), nil)
	s.NoError(err)
	mutableState.EXPECT().HSM().Return(root).AnyTimes()

	lastVisitedRunID, err := s.workflowResetter.reapplyContinueAsNewWorkflowEvents(
		ctx, mutableState, currentWorkflow, s.namespaceID, s.workflowID, s.baseRunID,
		baseBranchToken, baseFirstEventID, baseNextEventID, nil, false,
	)
	s.ErrorIs(err, wfResetterErrTest)
	s.Empty(lastVisitedRunID)
}

func (s *workflowResetterSuite) TestReapplyContinueAsNewWorkflowEvents_NextRunIsCurrentWorkflow() {
	ctx := context.Background()
	baseFirstEventID := int64(124)
	baseNextEventID := int64(456)
	baseBranchToken := []byte("some random base branch token")
	newRunID := uuid.NewString()
	newBranchToken := []byte("some random new branch token")
	newNextEventID := int64(2)

	baseEvent := &historypb.HistoryEvent{
		EventId:   127,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{WorkflowExecutionContinuedAsNewEventAttributes: &historypb.WorkflowExecutionContinuedAsNewEventAttributes{
			NewExecutionRunId: newRunID,
		}},
	}
	shardID := s.mockShard.GetShardID()
	s.mockExecutionMgr.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   baseBranchToken,
		MinEventID:    baseFirstEventID,
		MaxEventID:    baseNextEventID,
		PageSize:      defaultPageSize,
		NextPageToken: nil,
		ShardID:       shardID,
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:       []*historypb.History{{Events: []*historypb.HistoryEvent{baseEvent}}},
		NextPageToken: nil,
	}, nil)

	// The continue-as-new run is the current workflow. Its context is read directly from
	// currentWorkflow.GetContext(), not the cache.
	newEvent := &historypb.HistoryEvent{
		EventId:    1,
		EventType:  enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{}},
	}
	s.mockExecutionMgr.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   newBranchToken,
		MinEventID:    common.FirstEventID,
		MaxEventID:    newNextEventID,
		PageSize:      defaultPageSize,
		NextPageToken: nil,
		ShardID:       shardID,
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:       []*historypb.History{{Events: []*historypb.HistoryEvent{newEvent}}},
		NextPageToken: nil,
	}, nil)

	mutableState := historyi.NewMockMutableState(s.controller)
	smReg := hsm.NewRegistry()
	s.NoError(workflow.RegisterStateMachine(smReg))
	root, err := hsm.NewRoot(smReg, workflow.StateMachineType, nil, make(map[string]*persistencespb.StateMachineMap), nil)
	s.NoError(err)
	mutableState.EXPECT().HSM().Return(root).AnyTimes()

	currentContext := historyi.NewMockWorkflowContext(s.controller)
	currentMutableState := historyi.NewMockMutableState(s.controller)
	currentMutableState.EXPECT().GetWorkflowKey().Return(definition.WorkflowKey{RunID: newRunID}).AnyTimes()
	currentMutableState.EXPECT().GetNextEventID().Return(newNextEventID)
	currentMutableState.EXPECT().GetCurrentBranchToken().Return(newBranchToken, nil)
	currentContext.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(currentMutableState, nil)
	currentWorkflow := NewMockWorkflow(s.controller)
	currentWorkflow.EXPECT().GetMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().GetContext().Return(currentContext).AnyTimes()

	lastVisitedRunID, err := s.workflowResetter.reapplyContinueAsNewWorkflowEvents(
		ctx, mutableState, currentWorkflow, s.namespaceID, s.workflowID, s.baseRunID,
		baseBranchToken, baseFirstEventID, baseNextEventID, nil, false,
	)
	s.NoError(err)
	s.Equal(newRunID, lastVisitedRunID)
}

func (s *workflowResetterSuite) TestPrepareResetWorkflow_RefreshExpirationError() {
	resetMutableState := s.expectReplayResetWorkflow()
	resetMutableState.EXPECT().RefreshExpirationTimeoutTask(gomock.Any()).Return(wfResetterErrTest)
	_, err := s.callPrepareResetWorkflow(int64(0))
	s.ErrorIs(err, wfResetterErrTest)
}

func (s *workflowResetterSuite) TestPrepareResetWorkflow_VersionMismatch() {
	resetMutableState := s.expectReplayResetWorkflow()
	resetMutableState.EXPECT().RefreshExpirationTimeoutTask(gomock.Any()).Return(nil)
	// resetMutableState current version greater than the supplied reset workflow version.
	resetMutableState.EXPECT().GetCurrentVersion().Return(int64(100)).AnyTimes()
	_, err := s.callPrepareResetWorkflow(int64(0))
	s.Error(err)
	s.Contains(err.Error(), "version mismatch")
}

func (s *workflowResetterSuite) TestPrepareResetWorkflow_UpdateCurrentVersionError() {
	resetMutableState := s.expectReplayResetWorkflow()
	resetMutableState.EXPECT().RefreshExpirationTimeoutTask(gomock.Any()).Return(nil)
	resetMutableState.EXPECT().GetCurrentVersion().Return(int64(0)).AnyTimes()
	resetMutableState.EXPECT().UpdateCurrentVersion(int64(0), false).Return(wfResetterErrTest)
	_, err := s.callPrepareResetWorkflow(int64(0))
	s.ErrorIs(err, wfResetterErrTest)
}

func (s *workflowResetterSuite) TestPrepareResetWorkflow_PendingChildren() {
	resetMutableState := s.expectReplayResetWorkflow()
	resetMutableState.EXPECT().RefreshExpirationTimeoutTask(gomock.Any()).Return(nil)
	resetMutableState.EXPECT().GetCurrentVersion().Return(int64(0)).AnyTimes()
	resetMutableState.EXPECT().UpdateCurrentVersion(int64(0), false).Return(nil)
	resetMutableState.EXPECT().GetPendingChildExecutionInfos().Return(map[int64]*persistencespb.ChildExecutionInfo{
		1: {},
	})
	_, err := s.callPrepareResetWorkflow(int64(0))
	s.Error(err)
	s.Contains(err.Error(), "pending child workflows")
}

func (s *workflowResetterSuite) TestPrepareResetWorkflow_FailWorkflowTaskError() {
	resetMutableState := s.expectReplayResetWorkflow()
	resetMutableState.EXPECT().RefreshExpirationTimeoutTask(gomock.Any()).Return(nil)
	resetMutableState.EXPECT().GetCurrentVersion().Return(int64(0)).AnyTimes()
	resetMutableState.EXPECT().UpdateCurrentVersion(int64(0), false).Return(nil)
	resetMutableState.EXPECT().GetPendingChildExecutionInfos().Return(map[int64]*persistencespb.ChildExecutionInfo{})
	// No pending workflow task -> failWorkflowTask returns an error.
	resetMutableState.EXPECT().GetPendingWorkflowTask().Return(nil)
	_, err := s.callPrepareResetWorkflow(int64(0))
	s.Error(err)
}

func (s *workflowResetterSuite) TestPrepareResetWorkflow_FailInflightActivityError() {
	resetMutableState := s.expectReplayResetWorkflow()
	resetMutableState.EXPECT().RefreshExpirationTimeoutTask(gomock.Any()).Return(nil)
	resetMutableState.EXPECT().GetCurrentVersion().Return(int64(0)).AnyTimes()
	resetMutableState.EXPECT().UpdateCurrentVersion(int64(0), false).Return(nil)
	resetMutableState.EXPECT().GetPendingChildExecutionInfos().Return(map[int64]*persistencespb.ChildExecutionInfo{})
	workflowTask := &historyi.WorkflowTaskInfo{
		ScheduledEventID: 10,
		StartedEventID:   11,
		RequestID:        uuid.NewString(),
		TaskQueue:        &taskqueuepb.TaskQueue{Name: "tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
	}
	resetMutableState.EXPECT().GetPendingWorkflowTask().Return(workflowTask)
	resetMutableState.EXPECT().AddWorkflowTaskFailedEvent(
		workflowTask, enumspb.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW, gomock.Any(),
		consts.IdentityHistoryService, nil, "", s.baseRunID, s.resetRunID, gomock.Any(),
	).Return(&historypb.HistoryEvent{}, nil)
	resetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		StartTime: timestamppb.New(time.Now().UTC()),
	}).AnyTimes()
	// failInflightActivity errors on a transient activity.
	resetMutableState.EXPECT().GetPendingActivityInfos().Return(map[int64]*persistencespb.ActivityInfo{
		5: {ScheduledEventId: 5, StartedEventId: common.TransientEventID},
	})
	_, err := s.callPrepareResetWorkflow(int64(0))
	s.Error(err)
}

// expectReplayResetWorkflow sets up the mocks for the replayResetWorkflow portion of
// prepareResetWorkflow and returns the reset mutable state mock for further expectations.
func (s *workflowResetterSuite) expectReplayResetWorkflow() *historyi.MockMutableState {
	resetMutableState := historyi.NewMockMutableState(s.controller)
	s.mockExecutionMgr.EXPECT().ForkHistoryBranch(gomock.Any(), gomock.Any()).Return(
		&persistence.ForkHistoryBranchResponse{NewBranchToken: []byte("reset")}, nil,
	)
	s.mockStateRebuilder.EXPECT().Rebuild(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
		gomock.Any(), gomock.Any(), gomock.Any(),
	).Return(resetMutableState, RebuildStats{}, nil)
	resetMutableState.EXPECT().SetBaseWorkflow(gomock.Any(), gomock.Any(), gomock.Any())
	resetMutableState.EXPECT().AddHistorySize(gomock.Any())
	resetMutableState.EXPECT().AddExternalPayloadSize(gomock.Any())
	resetMutableState.EXPECT().AddExternalPayloadCount(gomock.Any())
	return resetMutableState
}

func (s *workflowResetterSuite) callPrepareResetWorkflow(resetWorkflowVersion int64) (Workflow, error) {
	return s.workflowResetter.prepareResetWorkflow(
		context.Background(), s.namespaceID, s.workflowID, s.baseRunID,
		[]byte("base"), int64(100), int64(1), s.resetRunID, uuid.NewString(),
		resetWorkflowVersion, "reset reason", false,
	)
}

func (s *workflowResetterSuite) TestPersistToDB_ThreeRuns_ConflictResolve() {
	ctx := context.Background()

	baseWorkflow := NewMockWorkflow(s.controller)
	baseMutableState := historyi.NewMockMutableState(s.controller)
	baseWorkflow.EXPECT().GetMutableState().Return(baseMutableState).AnyTimes()
	baseMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{RunId: s.baseRunID}).AnyTimes()
	baseMutableState.EXPECT().GetCurrentVersion().Return(int64(0)).AnyTimes()
	baseSnapshot := &persistence.WorkflowSnapshot{ExecutionInfo: &persistencespb.WorkflowExecutionInfo{}}
	baseEventsSeq := []*persistence.WorkflowEvents{{}}
	baseMutableState.EXPECT().CloseTransactionAsSnapshot(ctx, historyi.TransactionPolicyActive).Return(baseSnapshot, baseEventsSeq, nil)

	currentWorkflow := NewMockWorkflow(s.controller)
	currentMutableState := historyi.NewMockMutableState(s.controller)
	currentWorkflow.EXPECT().GetMutableState().Return(currentMutableState).AnyTimes()
	currentMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{RunId: s.currentRunID}).AnyTimes()
	currentMutableState.EXPECT().GetCurrentVersion().Return(int64(0)).AnyTimes()
	currentMutableState.EXPECT().IsWorkflow().Return(true).AnyTimes()
	currentMutation := &persistence.WorkflowMutation{}
	currentEventsSeq := []*persistence.WorkflowEvents{{}}
	currentMutableState.EXPECT().CloseTransactionAsMutation(ctx, historyi.TransactionPolicyActive).Return(currentMutation, currentEventsSeq, nil)

	resetWorkflow := NewMockWorkflow(s.controller)
	resetMutableState := historyi.NewMockMutableState(s.controller)
	resetWorkflow.EXPECT().GetMutableState().Return(resetMutableState).AnyTimes()
	resetMutableState.EXPECT().GetCurrentVersion().Return(int64(0)).AnyTimes()
	resetSnapshot := &persistence.WorkflowSnapshot{ExecutionInfo: &persistencespb.WorkflowExecutionInfo{}}
	resetEventsSeq := []*persistence.WorkflowEvents{{}}
	resetMutableState.EXPECT().CloseTransactionAsSnapshot(ctx, historyi.TransactionPolicyActive).Return(resetSnapshot, resetEventsSeq, nil)

	s.mockTransaction.EXPECT().ConflictResolveWorkflowExecution(
		ctx,
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		chasm.WorkflowArchetypeID,
		int64(0),
		baseSnapshot,
		baseEventsSeq,
		util.Ptr(int64(0)),
		resetSnapshot,
		resetEventsSeq,
		util.Ptr(int64(0)),
		currentMutation,
		currentEventsSeq,
		true,
	).Return(int64(0), int64(0), int64(0), nil)

	err := s.workflowResetter.persistToDB(ctx, baseWorkflow, currentWorkflow, nil, nil, resetWorkflow)
	s.NoError(err)
}

func (s *workflowResetterSuite) TestPersistToDB_CloseTransactionAsSnapshotError() {
	ctx := context.Background()
	currentWorkflow := NewMockWorkflow(s.controller)
	currentMutableState := historyi.NewMockMutableState(s.controller)
	currentWorkflow.EXPECT().GetMutableState().Return(currentMutableState).AnyTimes()
	currentMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{RunId: s.currentRunID}).AnyTimes()

	resetWorkflow := NewMockWorkflow(s.controller)
	resetMutableState := historyi.NewMockMutableState(s.controller)
	resetWorkflow.EXPECT().GetMutableState().Return(resetMutableState).AnyTimes()
	resetMutableState.EXPECT().CloseTransactionAsSnapshot(ctx, historyi.TransactionPolicyActive).Return(nil, nil, wfResetterErrTest)

	err := s.workflowResetter.persistToDB(ctx, currentWorkflow, currentWorkflow, nil, nil, resetWorkflow)
	s.ErrorIs(err, wfResetterErrTest)
}

func (s *workflowResetterSuite) TestResetWorkflow_CurrentNotRunning() {
	ctx := context.Background()
	baseBranchToken := []byte("some random base branch token")
	baseRebuildLastEventID := int64(1233)
	baseRebuildLastEventVersion := int64(0)
	baseNextEventID := int64(1235)
	resetReason := "reset reason"

	s.mockShard.Resource.NamespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(tests.LocalNamespaceEntry, nil)

	// base workflow
	baseWorkflow := NewMockWorkflow(s.controller)
	baseMutableState := historyi.NewMockMutableState(s.controller)
	baseWorkflow.EXPECT().GetMutableState().Return(baseMutableState).AnyTimes()
	baseMutableState.EXPECT().UpdateResetRunID(s.resetRunID)
	baseMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId:           s.baseRunID,
		CreateRequestId: "base-create-request-id",
	}).AnyTimes()

	// current workflow - not running
	currentWorkflow := NewMockWorkflow(s.controller)
	currentMutableState := historyi.NewMockMutableState(s.controller)
	currentContext := historyi.NewMockWorkflowContext(s.controller)
	currentWorkflow.EXPECT().GetMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().GetContext().Return(currentContext).AnyTimes()
	currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	currentMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{RunId: s.currentRunID}).AnyTimes()
	currentMutableState.EXPECT().GetCurrentVersion().Return(int64(0)).AnyTimes()
	currentMutableState.EXPECT().IsWorkflow().Return(true).AnyTimes()
	currentMutableState.EXPECT().GetWorkflowKey().Return(definition.WorkflowKey{RunID: s.currentRunID}).AnyTimes()
	currentMutableState.EXPECT().CloseTransactionAsMutation(ctx, historyi.TransactionPolicyActive).Return(&persistence.WorkflowMutation{}, []*persistence.WorkflowEvents{{}}, nil)
	updateRegistry := s.expectAbortRegistry(currentContext)
	_ = updateRegistry

	// reset workflow built via prepareResetWorkflow
	resetMutableState := s.expectReplayResetWorkflow()
	resetMutableState.EXPECT().RefreshExpirationTimeoutTask(gomock.Any()).Return(nil)
	resetMutableState.EXPECT().GetCurrentVersion().Return(int64(0)).AnyTimes()
	resetMutableState.EXPECT().UpdateCurrentVersion(int64(0), false).Return(nil)
	resetMutableState.EXPECT().GetPendingChildExecutionInfos().Return(map[int64]*persistencespb.ChildExecutionInfo{})
	workflowTask := &historyi.WorkflowTaskInfo{
		ScheduledEventID: baseRebuildLastEventID - 12,
		StartedEventID:   baseRebuildLastEventID - 10,
		RequestID:        uuid.NewString(),
		TaskQueue:        &taskqueuepb.TaskQueue{Name: "tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
	}
	resetMutableState.EXPECT().GetPendingWorkflowTask().Return(workflowTask).AnyTimes()
	resetMutableState.EXPECT().AddWorkflowTaskFailedEvent(
		workflowTask, enumspb.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW, gomock.Any(),
		consts.IdentityHistoryService, nil, "", s.baseRunID, s.resetRunID, gomock.Any(),
	).Return(&historypb.HistoryEvent{}, nil)
	resetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId:     s.resetRunID,
		StartTime: timestamppb.New(time.Now().UTC()),
	}).AnyTimes()
	resetMutableState.EXPECT().GetPendingActivityInfos().Return(map[int64]*persistencespb.ActivityInfo{})

	// reapplyContinueAsNewWorkflowEvents reads the base branch (no continue-as-new events).
	smReg := hsm.NewRegistry()
	s.NoError(workflow.RegisterStateMachine(smReg))
	root, err := hsm.NewRoot(smReg, workflow.StateMachineType, nil, make(map[string]*persistencespb.StateMachineMap), nil)
	s.NoError(err)
	resetMutableState.EXPECT().HSM().Return(root).AnyTimes()
	baseEvent := &historypb.HistoryEvent{
		EventId:    baseRebuildLastEventID + 1,
		EventType:  enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{}},
	}
	s.mockExecutionMgr.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), gomock.Any()).Return(
		&persistence.ReadHistoryBranchByBatchResponse{
			History:       []*historypb.History{{Events: []*historypb.HistoryEvent{baseEvent}}},
			NextPageToken: nil,
		}, nil,
	)

	// ScheduleWorkflowTask
	resetMutableState.EXPECT().HasPendingWorkflowTask().Return(true)

	// persistToDB: currentRunID != baseRunID (3-run conflict resolve path).
	baseMutableState.EXPECT().GetCurrentVersion().Return(int64(0)).AnyTimes()
	baseMutableState.EXPECT().CloseTransactionAsSnapshot(ctx, historyi.TransactionPolicyActive).Return(
		&persistence.WorkflowSnapshot{ExecutionInfo: &persistencespb.WorkflowExecutionInfo{}}, []*persistence.WorkflowEvents{{}}, nil,
	)
	resetMutableState.EXPECT().CloseTransactionAsSnapshot(ctx, historyi.TransactionPolicyActive).Return(
		&persistence.WorkflowSnapshot{ExecutionInfo: &persistencespb.WorkflowExecutionInfo{}}, []*persistence.WorkflowEvents{{}}, nil,
	)
	s.mockTransaction.EXPECT().ConflictResolveWorkflowExecution(
		ctx, persistence.ConflictResolveWorkflowModeUpdateCurrent, chasm.WorkflowArchetypeID,
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
		gomock.Any(), gomock.Any(), gomock.Any(), true,
	).Return(int64(0), int64(0), int64(0), nil)

	err = s.workflowResetter.ResetWorkflow(
		ctx, s.namespaceID, s.workflowID, s.baseRunID, baseBranchToken,
		baseRebuildLastEventID, baseRebuildLastEventVersion, baseNextEventID, s.resetRunID,
		baseWorkflow, currentWorkflow, resetReason, nil, nil, false, nil,
	)
	s.NoError(err)
}

func (s *workflowResetterSuite) TestResetWorkflow_CurrentRunning() {
	ctx := context.Background()
	baseBranchToken := []byte("some random base branch token")
	baseRebuildLastEventID := int64(1233)
	baseRebuildLastEventVersion := int64(0)
	baseNextEventID := int64(1235)
	resetReason := "reset reason"

	s.mockShard.Resource.NamespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(tests.LocalNamespaceEntry, nil)

	baseWorkflow := NewMockWorkflow(s.controller)
	baseMutableState := historyi.NewMockMutableState(s.controller)
	baseWorkflow.EXPECT().GetMutableState().Return(baseMutableState).AnyTimes()
	baseMutableState.EXPECT().UpdateResetRunID(s.resetRunID)
	baseMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId:           s.baseRunID,
		CreateRequestId: "base-create-request-id",
	}).AnyTimes()

	// current workflow - running; same run as base so persistToDB takes UpdateWorkflowExecution path.
	currentWorkflow := NewMockWorkflow(s.controller)
	currentMutableState := historyi.NewMockMutableState(s.controller)
	currentContext := historyi.NewMockWorkflowContext(s.controller)
	currentWorkflow.EXPECT().GetMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().GetContext().Return(currentContext).AnyTimes()
	currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	currentMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{RunId: s.baseRunID}).AnyTimes()
	currentMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{}).AnyTimes()
	currentMutableState.EXPECT().GetCurrentVersion().Return(int64(0)).AnyTimes()
	currentMutableState.EXPECT().IsWorkflow().Return(true).AnyTimes()
	currentMutableState.EXPECT().GetWorkflowKey().Return(definition.WorkflowKey{RunID: s.baseRunID}).AnyTimes()

	// terminateWorkflow
	currentMutableState.EXPECT().GetStartedWorkflowTask().Return(nil)
	currentMutableState.EXPECT().AddWorkflowExecutionTerminatedEvent(
		resetReason, nil, consts.IdentityResetter, false, nil,
	).Return(&historypb.HistoryEvent{}, nil)
	// CloseTransactionAsMutation for current (running)
	currentMutation := &persistence.WorkflowMutation{}
	currentEventsSeq := []*persistence.WorkflowEvents{{
		RunID:  s.baseRunID,
		Events: []*historypb.HistoryEvent{{EventId: 1, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED, Attributes: &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{Identity: consts.IdentityResetter}}}},
	}}
	currentMutableState.EXPECT().CloseTransactionAsMutation(ctx, historyi.TransactionPolicyActive).Return(currentMutation, currentEventsSeq, nil)
	s.expectAbortRegistry(currentContext)

	// reset workflow
	resetMutableState := s.expectReplayResetWorkflow()
	resetMutableState.EXPECT().RefreshExpirationTimeoutTask(gomock.Any()).Return(nil)
	resetMutableState.EXPECT().GetCurrentVersion().Return(int64(0)).AnyTimes()
	resetMutableState.EXPECT().UpdateCurrentVersion(int64(0), false).Return(nil)
	resetMutableState.EXPECT().GetPendingChildExecutionInfos().Return(map[int64]*persistencespb.ChildExecutionInfo{})
	workflowTask := &historyi.WorkflowTaskInfo{
		ScheduledEventID: baseRebuildLastEventID - 12,
		StartedEventID:   baseRebuildLastEventID - 10,
		RequestID:        uuid.NewString(),
		TaskQueue:        &taskqueuepb.TaskQueue{Name: "tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
	}
	resetMutableState.EXPECT().GetPendingWorkflowTask().Return(workflowTask).AnyTimes()
	resetMutableState.EXPECT().AddWorkflowTaskFailedEvent(
		workflowTask, enumspb.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW, gomock.Any(),
		consts.IdentityHistoryService, nil, "", s.baseRunID, s.resetRunID, gomock.Any(),
	).Return(&historypb.HistoryEvent{}, nil)
	resetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId:     s.resetRunID,
		StartTime: timestamppb.New(time.Now().UTC()),
	}).AnyTimes()
	resetMutableState.EXPECT().GetPendingActivityInfos().Return(map[int64]*persistencespb.ActivityInfo{})

	smReg := hsm.NewRegistry()
	s.NoError(workflow.RegisterStateMachine(smReg))
	root, err := hsm.NewRoot(smReg, workflow.StateMachineType, nil, make(map[string]*persistencespb.StateMachineMap), nil)
	s.NoError(err)
	resetMutableState.EXPECT().HSM().Return(root).AnyTimes()

	// reapplyContinueAsNewWorkflowEvents reads base branch (no CAN), lastVisitedRunID == baseRunID == currentRunID,
	// so currentWorkflowEventsSeq are reapplied (the terminated event is by resetter -> skipped).
	baseEvent := &historypb.HistoryEvent{
		EventId:    baseRebuildLastEventID + 1,
		EventType:  enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{}},
	}
	s.mockExecutionMgr.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), gomock.Any()).Return(
		&persistence.ReadHistoryBranchByBatchResponse{
			History:       []*historypb.History{{Events: []*historypb.HistoryEvent{baseEvent}}},
			NextPageToken: nil,
		}, nil,
	)

	resetMutableState.EXPECT().HasPendingWorkflowTask().Return(true)

	// persistToDB: currentRunID == baseRunID, currentWorkflowMutation != nil -> UpdateWorkflowExecution path.
	resetMutableState.EXPECT().CloseTransactionAsSnapshot(ctx, historyi.TransactionPolicyActive).Return(
		&persistence.WorkflowSnapshot{ExecutionInfo: &persistencespb.WorkflowExecutionInfo{}}, []*persistence.WorkflowEvents{{}}, nil,
	)
	s.mockTransaction.EXPECT().UpdateWorkflowExecution(
		ctx, persistence.UpdateWorkflowModeUpdateCurrent, chasm.WorkflowArchetypeID,
		int64(0), currentMutation, currentEventsSeq, util.Ptr(int64(0)),
		gomock.Any(), gomock.Any(), true,
	).Return(int64(0), int64(0), nil)

	err = s.workflowResetter.ResetWorkflow(
		ctx, s.namespaceID, s.workflowID, s.baseRunID, baseBranchToken,
		baseRebuildLastEventID, baseRebuildLastEventVersion, baseNextEventID, s.resetRunID,
		baseWorkflow, currentWorkflow, resetReason, nil, nil, false, nil,
	)
	s.NoError(err)
}

func (s *workflowResetterSuite) TestResetWorkflow_GetNamespaceError() {
	s.mockShard.Resource.NamespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(nil, wfResetterErrTest)
	baseWorkflow := NewMockWorkflow(s.controller)
	currentWorkflow := NewMockWorkflow(s.controller)
	err := s.workflowResetter.ResetWorkflow(
		context.Background(), s.namespaceID, s.workflowID, s.baseRunID, []byte("bt"),
		int64(100), int64(0), int64(102), s.resetRunID,
		baseWorkflow, currentWorkflow, "reason", nil, nil, false, nil,
	)
	s.ErrorIs(err, wfResetterErrTest)
}

// wfResetterEmptyUpdateStore is a minimal UpdateStore used to build a real, empty update
// registry so that Abort(AbortReasonWorkflowCompleted) is a safe no-op in ResetWorkflow tests.
type wfResetterEmptyUpdateStore struct{}

func (wfResetterEmptyUpdateStore) VisitUpdates(func(string, *persistencespb.UpdateInfo)) {}
func (wfResetterEmptyUpdateStore) GetUpdateOutcome(context.Context, string) (*updatepb.Outcome, error) {
	return nil, nil
}
func (wfResetterEmptyUpdateStore) GetCurrentVersion() int64         { return 0 }
func (wfResetterEmptyUpdateStore) IsWorkflowExecutionRunning() bool { return false }

// expectAbortRegistry wires up the UpdateRegistry().Abort() call made at the end of ResetWorkflow.
func (s *workflowResetterSuite) expectAbortRegistry(wfContext *historyi.MockWorkflowContext) update.Registry {
	reg := update.NewRegistry(wfResetterEmptyUpdateStore{})
	wfContext.EXPECT().UpdateRegistry(gomock.Any()).Return(reg)
	return reg
}

func (s *workflowResetterSuite) TestReapplyChildEvents_AddEventErrors() {
	initiatedID := int64(123)
	wfExecution := &commonpb.WorkflowExecution{WorkflowId: uuid.NewString(), RunId: uuid.NewString()}

	type tc struct {
		name  string
		event *historypb.HistoryEvent
		setup func(ms *historyi.MockMutableState)
	}
	cases := []tc{
		{
			name: "start child failed",
			event: &historypb.HistoryEvent{
				EventType:  enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED,
				Attributes: &historypb.HistoryEvent_StartChildWorkflowExecutionFailedEventAttributes{StartChildWorkflowExecutionFailedEventAttributes: &historypb.StartChildWorkflowExecutionFailedEventAttributes{InitiatedEventId: initiatedID}},
			},
			setup: func(ms *historyi.MockMutableState) {
				ms.EXPECT().GetChildExecutionInfo(initiatedID).Return(&persistencespb.ChildExecutionInfo{}, true)
				ms.EXPECT().AddStartChildWorkflowExecutionFailedEvent(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, wfResetterErrTest)
			},
		},
		{
			name: "child started",
			event: &historypb.HistoryEvent{
				EventType:  enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
				Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionStartedEventAttributes{ChildWorkflowExecutionStartedEventAttributes: &historypb.ChildWorkflowExecutionStartedEventAttributes{InitiatedEventId: initiatedID, WorkflowExecution: wfExecution}},
			},
			setup: func(ms *historyi.MockMutableState) {
				ms.EXPECT().GetChildExecutionInfo(initiatedID).Return(&persistencespb.ChildExecutionInfo{StartedEventId: common.EmptyEventID}, true)
				ms.EXPECT().AddChildWorkflowExecutionStartedEvent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, wfResetterErrTest)
			},
		},
		{
			name: "child completed",
			event: &historypb.HistoryEvent{
				EventType:  enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
				Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionCompletedEventAttributes{ChildWorkflowExecutionCompletedEventAttributes: &historypb.ChildWorkflowExecutionCompletedEventAttributes{InitiatedEventId: initiatedID, WorkflowExecution: wfExecution}},
			},
			setup: func(ms *historyi.MockMutableState) {
				ms.EXPECT().GetChildExecutionInfo(initiatedID).Return(&persistencespb.ChildExecutionInfo{}, true)
				ms.EXPECT().AddChildWorkflowExecutionCompletedEvent(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, wfResetterErrTest)
			},
		},
		{
			name: "child failed",
			event: &historypb.HistoryEvent{
				EventType:  enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED,
				Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionFailedEventAttributes{ChildWorkflowExecutionFailedEventAttributes: &historypb.ChildWorkflowExecutionFailedEventAttributes{InitiatedEventId: initiatedID, WorkflowExecution: wfExecution}},
			},
			setup: func(ms *historyi.MockMutableState) {
				ms.EXPECT().GetChildExecutionInfo(initiatedID).Return(&persistencespb.ChildExecutionInfo{}, true)
				ms.EXPECT().AddChildWorkflowExecutionFailedEvent(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, wfResetterErrTest)
			},
		},
		{
			name: "child canceled",
			event: &historypb.HistoryEvent{
				EventType:  enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED,
				Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionCanceledEventAttributes{ChildWorkflowExecutionCanceledEventAttributes: &historypb.ChildWorkflowExecutionCanceledEventAttributes{InitiatedEventId: initiatedID, WorkflowExecution: wfExecution}},
			},
			setup: func(ms *historyi.MockMutableState) {
				ms.EXPECT().GetChildExecutionInfo(initiatedID).Return(&persistencespb.ChildExecutionInfo{}, true)
				ms.EXPECT().AddChildWorkflowExecutionCanceledEvent(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, wfResetterErrTest)
			},
		},
		{
			name: "child timed out",
			event: &historypb.HistoryEvent{
				EventType:  enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT,
				Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionTimedOutEventAttributes{ChildWorkflowExecutionTimedOutEventAttributes: &historypb.ChildWorkflowExecutionTimedOutEventAttributes{InitiatedEventId: initiatedID, WorkflowExecution: wfExecution}},
			},
			setup: func(ms *historyi.MockMutableState) {
				ms.EXPECT().GetChildExecutionInfo(initiatedID).Return(&persistencespb.ChildExecutionInfo{}, true)
				ms.EXPECT().AddChildWorkflowExecutionTimedOutEvent(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, wfResetterErrTest)
			},
		},
		{
			name: "child terminated",
			event: &historypb.HistoryEvent{
				EventType:  enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED,
				Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionTerminatedEventAttributes{ChildWorkflowExecutionTerminatedEventAttributes: &historypb.ChildWorkflowExecutionTerminatedEventAttributes{InitiatedEventId: initiatedID, WorkflowExecution: wfExecution}},
			},
			setup: func(ms *historyi.MockMutableState) {
				ms.EXPECT().GetChildExecutionInfo(initiatedID).Return(&persistencespb.ChildExecutionInfo{}, true)
				ms.EXPECT().AddChildWorkflowExecutionTerminatedEvent(gomock.Any(), gomock.Any()).Return(nil, wfResetterErrTest)
			},
		},
	}

	for _, c := range cases {
		s.Run(c.name, func() {
			ms := historyi.NewMockMutableState(s.controller)
			c.setup(ms)
			_, err := reapplyEvents(context.Background(), ms, nil, nil, []*historypb.HistoryEvent{c.event}, nil, "", true)
			s.ErrorIs(err, wfResetterErrTest)
		})
	}
}

func (s *workflowResetterSuite) TestReapplyChildEvents_StartedAlreadyStartedSkipped() {
	initiatedID := int64(123)
	event := &historypb.HistoryEvent{
		EventType:  enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionStartedEventAttributes{ChildWorkflowExecutionStartedEventAttributes: &historypb.ChildWorkflowExecutionStartedEventAttributes{InitiatedEventId: initiatedID}},
	}
	ms := historyi.NewMockMutableState(s.controller)
	// child exists but already has a started event -> skip without adding.
	ms.EXPECT().GetChildExecutionInfo(initiatedID).Return(&persistencespb.ChildExecutionInfo{StartedEventId: 999}, true)
	applied, err := reapplyEvents(context.Background(), ms, nil, nil, []*historypb.HistoryEvent{event}, nil, "", true)
	s.NoError(err)
	s.Empty(applied)
}

func (s *workflowResetterSuite) TestReapplyEvents_Deduplication() {
	runID := uuid.NewString()
	signalEvent := &historypb.HistoryEvent{
		EventId:   101,
		Version:   3,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
			SignalName: "signal-name",
		}},
	}

	ms := historyi.NewMockMutableState(s.controller)
	// First call: not a duplicate -> apply and then record dedup resource.
	ms.EXPECT().IsResourceDuplicated(gomock.Any()).Return(false)
	ms.EXPECT().AddWorkflowExecutionSignaled(
		signalEvent.GetWorkflowExecutionSignaledEventAttributes().GetSignalName(),
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).Return(&historypb.HistoryEvent{}, nil)
	ms.EXPECT().UpdateDuplicatedResource(gomock.Any())

	applied, err := reapplyEvents(context.Background(), ms, nil, nil, []*historypb.HistoryEvent{signalEvent}, nil, runID, false)
	s.NoError(err)
	s.Len(applied, 1)

	// Second call: it is a duplicate -> the event is skipped via continue (no dedup update).
	ms.EXPECT().IsResourceDuplicated(gomock.Any()).Return(true)
	applied, err = reapplyEvents(context.Background(), ms, nil, nil, []*historypb.HistoryEvent{signalEvent}, nil, runID, false)
	s.NoError(err)
	s.Empty(applied)
}

func (s *workflowResetterSuite) TestReapplyEvents_SignalError() {
	event := &historypb.HistoryEvent{
		EventId:   101,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
			SignalName: "signal-name",
		}},
	}
	ms := historyi.NewMockMutableState(s.controller)
	ms.EXPECT().AddWorkflowExecutionSignaled(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, wfResetterErrTest)
	_, err := reapplyEvents(context.Background(), ms, nil, nil, []*historypb.HistoryEvent{event}, nil, "", false)
	s.ErrorIs(err, wfResetterErrTest)
}

func (s *workflowResetterSuite) TestReapplyEvents_UpdateAdmittedError() {
	event := &historypb.HistoryEvent{
		EventId:   102,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionUpdateAdmittedEventAttributes{WorkflowExecutionUpdateAdmittedEventAttributes: &historypb.WorkflowExecutionUpdateAdmittedEventAttributes{
			Request: &updatepb.Request{Input: &updatepb.Input{Args: payloads.EncodeString("x")}},
			Origin:  enumspb.UPDATE_ADMITTED_EVENT_ORIGIN_UNSPECIFIED,
		}},
	}
	ms := historyi.NewMockMutableState(s.controller)
	ms.EXPECT().AddWorkflowExecutionUpdateAdmittedEvent(gomock.Any(), gomock.Any()).Return(nil, wfResetterErrTest)
	_, err := reapplyEvents(context.Background(), ms, nil, nil, []*historypb.HistoryEvent{event}, nil, "", false)
	s.ErrorIs(err, wfResetterErrTest)
}

func (s *workflowResetterSuite) TestReapplyEvents_UpdateAcceptedError() {
	event := &historypb.HistoryEvent{
		EventId:   103,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionUpdateAcceptedEventAttributes{WorkflowExecutionUpdateAcceptedEventAttributes: &historypb.WorkflowExecutionUpdateAcceptedEventAttributes{
			AcceptedRequest: &updatepb.Request{Input: &updatepb.Input{Args: payloads.EncodeString("x")}},
		}},
	}
	ms := historyi.NewMockMutableState(s.controller)
	ms.EXPECT().AddWorkflowExecutionUpdateAdmittedEvent(gomock.Any(), enumspb.UPDATE_ADMITTED_EVENT_ORIGIN_REAPPLY).Return(nil, wfResetterErrTest)
	_, err := reapplyEvents(context.Background(), ms, nil, nil, []*historypb.HistoryEvent{event}, nil, "", false)
	s.ErrorIs(err, wfResetterErrTest)
}

func (s *workflowResetterSuite) TestReapplyEvents_UpdateAcceptedNilRequestSkipped() {
	// An UpdateAccepted event with no accepted request (preceded by an UpdateAdmitted) is skipped.
	event := &historypb.HistoryEvent{
		EventId:   104,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionUpdateAcceptedEventAttributes{WorkflowExecutionUpdateAcceptedEventAttributes: &historypb.WorkflowExecutionUpdateAcceptedEventAttributes{
			AcceptedRequest: nil,
		}},
	}
	ms := historyi.NewMockMutableState(s.controller)
	applied, err := reapplyEvents(context.Background(), ms, nil, nil, []*historypb.HistoryEvent{event}, nil, "", false)
	s.NoError(err)
	s.Empty(applied)
}

func (s *workflowResetterSuite) TestReapplyEvents_CancelRequestedError() {
	event := &historypb.HistoryEvent{
		EventId:   105,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCancelRequestedEventAttributes{WorkflowExecutionCancelRequestedEventAttributes: &historypb.WorkflowExecutionCancelRequestedEventAttributes{
			Cause:    testRequestReason,
			Identity: testIdentity,
		}},
	}
	ms := historyi.NewMockMutableState(s.controller)
	ms.EXPECT().IsCancelRequested().Return(false)
	ms.EXPECT().AddWorkflowExecutionCancelRequestedEvent(gomock.Any()).Return(nil, wfResetterErrTest)
	_, err := reapplyEvents(context.Background(), ms, nil, nil, []*historypb.HistoryEvent{event}, nil, "", false)
	s.ErrorIs(err, wfResetterErrTest)
}

func (s *workflowResetterSuite) TestReapplyEvents_CancelRequestedAlreadyRequested() {
	event := &historypb.HistoryEvent{
		EventId:   106,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCancelRequestedEventAttributes{WorkflowExecutionCancelRequestedEventAttributes: &historypb.WorkflowExecutionCancelRequestedEventAttributes{
			Cause:    testRequestReason,
			Identity: testIdentity,
		}},
	}
	ms := historyi.NewMockMutableState(s.controller)
	ms.EXPECT().IsCancelRequested().Return(true)
	applied, err := reapplyEvents(context.Background(), ms, nil, nil, []*historypb.HistoryEvent{event}, nil, "", false)
	s.NoError(err)
	s.Empty(applied)
}

func (s *workflowResetterSuite) TestFailInflightActivity_NotStartedSuccessInvokesCallback() {
	now := time.Now().UTC()
	mutableState := historyi.NewMockMutableState(s.controller)
	notStarted := &persistencespb.ActivityInfo{
		ScheduledEventId: 456,
		StartedEventId:   common.EmptyEventID,
		ScheduledTime:    timestamppb.New(now.Add(-time.Hour)),
	}
	mutableState.EXPECT().GetPendingActivityInfos().Return(map[int64]*persistencespb.ActivityInfo{
		notStarted.ScheduledEventId: notStarted,
	})
	// Invoke the supplied callback so the ScheduledTime/FirstScheduledTime override code runs.
	mutableState.EXPECT().UpdateActivity(notStarted.ScheduledEventId, gomock.Any()).DoAndReturn(
		func(_ int64, fn func(*persistencespb.ActivityInfo, historyi.MutableState) error) error {
			return fn(notStarted, mutableState)
		},
	)
	err := s.workflowResetter.failInflightActivity(now, mutableState, "reason")
	s.NoError(err)
	s.Equal(timestamppb.New(now), notStarted.ScheduledTime)
	s.Equal(timestamppb.New(now), notStarted.FirstScheduledTime)
}

func (s *workflowResetterSuite) TestReapplyContinueAsNewWorkflowEvents_SecondBranchDataLoss() {
	ctx := context.Background()
	baseFirstEventID := int64(124)
	baseNextEventID := int64(456)
	baseBranchToken := []byte("some random base branch token")
	newRunID := uuid.NewString()
	newBranchToken := []byte("some random new branch token")
	newNextEventID := int64(2)

	baseEvent := &historypb.HistoryEvent{
		EventId:   127,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{WorkflowExecutionContinuedAsNewEventAttributes: &historypb.WorkflowExecutionContinuedAsNewEventAttributes{
			NewExecutionRunId: newRunID,
		}},
	}
	shardID := s.mockShard.GetShardID()
	s.mockExecutionMgr.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   baseBranchToken,
		MinEventID:    baseFirstEventID,
		MaxEventID:    baseNextEventID,
		PageSize:      defaultPageSize,
		NextPageToken: nil,
		ShardID:       shardID,
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:       []*historypb.History{{Events: []*historypb.HistoryEvent{baseEvent}}},
		NextPageToken: nil,
	}, nil)
	// Second branch read returns a data loss error.
	s.mockExecutionMgr.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   newBranchToken,
		MinEventID:    common.FirstEventID,
		MaxEventID:    newNextEventID,
		PageSize:      defaultPageSize,
		NextPageToken: nil,
		ShardID:       shardID,
	}).Return(nil, serviceerror.NewDataLoss("data loss"))

	mutableState := historyi.NewMockMutableState(s.controller)
	smReg := hsm.NewRegistry()
	s.NoError(workflow.RegisterStateMachine(smReg))
	root, err := hsm.NewRoot(smReg, workflow.StateMachineType, nil, make(map[string]*persistencespb.StateMachineMap), nil)
	s.NoError(err)
	mutableState.EXPECT().HSM().Return(root).AnyTimes()

	currentContext := historyi.NewMockWorkflowContext(s.controller)
	currentMutableState := historyi.NewMockMutableState(s.controller)
	currentMutableState.EXPECT().GetWorkflowKey().Return(definition.WorkflowKey{RunID: newRunID}).AnyTimes()
	currentMutableState.EXPECT().GetNextEventID().Return(newNextEventID)
	currentMutableState.EXPECT().GetCurrentBranchToken().Return(newBranchToken, nil)
	currentContext.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(currentMutableState, nil)
	currentWorkflow := NewMockWorkflow(s.controller)
	currentWorkflow.EXPECT().GetMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().GetContext().Return(currentContext).AnyTimes()

	lastVisitedRunID, err := s.workflowResetter.reapplyContinueAsNewWorkflowEvents(
		ctx, mutableState, currentWorkflow, s.namespaceID, s.workflowID, s.baseRunID,
		baseBranchToken, baseFirstEventID, baseNextEventID, nil, false,
	)
	var dataLoss *serviceerror.DataLoss
	s.ErrorAs(err, &dataLoss)
	s.Empty(lastVisitedRunID)
}

func (s *workflowResetterSuite) TestReapplyEvents_WorkflowOptionsUpdated_WithTimeSkippingConfig() {
	timeSkippingConfig := &commonpb.TimeSkippingConfig{Enabled: true}
	event := &historypb.HistoryEvent{
		EventId:   101,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionOptionsUpdatedEventAttributes{
			WorkflowExecutionOptionsUpdatedEventAttributes: &historypb.WorkflowExecutionOptionsUpdatedEventAttributes{
				AttachedRequestId:  "test-request-id",
				TimeSkippingConfig: timeSkippingConfig,
			},
		},
	}
	attr := event.GetWorkflowExecutionOptionsUpdatedEventAttributes()

	ms := historyi.NewMockMutableState(s.controller)
	smReg := hsm.NewRegistry()
	s.NoError(workflow.RegisterStateMachine(smReg))
	root, err := hsm.NewRoot(smReg, workflow.StateMachineType, nil, make(map[string]*persistencespb.StateMachineMap), nil)
	s.NoError(err)
	ms.EXPECT().HSM().Return(root).AnyTimes()
	ms.EXPECT().AddWorkflowExecutionOptionsUpdatedEvent(
		attr.GetVersioningOverride(),
		attr.GetUnsetVersioningOverride(),
		attr.GetAttachedRequestId(),
		attr.GetAttachedCompletionCallbacks(),
		event.Links,
		attr.GetIdentity(),
		attr.GetPriority(),
		timeSkippingConfig,
		attr.GetTimeSkippingConfigUpdated(),
		attr.GetWorkflowUpdateOptions(),
	).Return(&historypb.HistoryEvent{}, nil)

	appliedEvents, err := reapplyEvents(context.Background(), ms, nil, smReg, []*historypb.HistoryEvent{event}, nil, "", true)
	s.NoError(err)
	s.Len(appliedEvents, 1)
}
