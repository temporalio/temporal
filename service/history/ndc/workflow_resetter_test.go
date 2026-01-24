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
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
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
		persistencespb.ShardInfo_builder{
			ShardId: 0,
			RangeId: 1,
		}.Build(),
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
	currentMutableState.EXPECT().GetExecutionState().Return(persistencespb.WorkflowExecutionState_builder{
		RunId: s.currentRunID,
	}.Build()).AnyTimes()

	currentMutableState.EXPECT().GetCurrentVersion().Return(int64(0)).AnyTimes()
	currentMutableState.EXPECT().IsWorkflow().Return(true).AnyTimes()
	currentNewEventsSize := int64(3444)
	currentMutation := &persistence.WorkflowMutation{
		ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
			VersionHistories: versionhistory.NewVersionHistories(historyspb.VersionHistory_builder{
				BranchToken: []byte{1, 2, 3},
				Items: []*historyspb.VersionHistoryItem{
					historyspb.VersionHistoryItem_builder{EventId: 234, Version: 0}.Build(),
				},
			}.Build()),
		}.Build(),
	}
	currentEventsSeq := []*persistence.WorkflowEvents{{
		NamespaceID: s.namespaceID.String(),
		WorkflowID:  s.workflowID,
		RunID:       s.currentRunID,
		BranchToken: []byte("some random current branch token"),
		Events: []*historypb.HistoryEvent{historypb.HistoryEvent_builder{
			EventId: 234,
		}.Build()},
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
		ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
			VersionHistories: versionhistory.NewVersionHistories(historyspb.VersionHistory_builder{
				BranchToken: []byte{1, 2, 3},
				Items: []*historyspb.VersionHistoryItem{
					historyspb.VersionHistoryItem_builder{EventId: 123, Version: 0}.Build(),
				},
			}.Build()),
		}.Build(),
	}
	resetEventsSeq := []*persistence.WorkflowEvents{{
		NamespaceID: s.namespaceID.String(),
		WorkflowID:  s.workflowID,
		RunID:       s.resetRunID,
		BranchToken: []byte("some random reset branch token"),
		Events: []*historypb.HistoryEvent{historypb.HistoryEvent_builder{
			EventId: 123,
		}.Build()},
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
		util.Ptr(int64(0)),
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
	currentMutableState.EXPECT().GetExecutionState().Return(persistencespb.WorkflowExecutionState_builder{
		RunId: s.currentRunID,
	}.Build()).AnyTimes()

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
		Events: []*historypb.HistoryEvent{historypb.HistoryEvent_builder{
			EventId: 123,
		}.Build()},
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
		util.Ptr(int64(0)),
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
		util.Ptr(baseRebuildLastEventVersion),
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
		TaskQueue: taskqueuepb.TaskQueue_builder{
			Name: "random task queue name",
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		}.Build(),
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
		TaskQueue: taskqueuepb.TaskQueue_builder{
			Name: "random task queue name",
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		}.Build(),
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

	activity1 := persistencespb.ActivityInfo_builder{
		Version:              12,
		ScheduledEventId:     123,
		ScheduledTime:        timestamppb.New(now.Add(-10 * time.Second)),
		FirstScheduledTime:   timestamppb.New(now.Add(-10 * time.Second)),
		StartedEventId:       124,
		LastHeartbeatDetails: payloads.EncodeString("some random activity 1 details"),
		StartedIdentity:      "some random activity 1 started identity",
	}.Build()
	activity2 := persistencespb.ActivityInfo_builder{
		Version:            12,
		ScheduledEventId:   456,
		ScheduledTime:      timestamppb.New(now.Add(-10 * time.Second)),
		FirstScheduledTime: timestamppb.New(now.Add(-10 * time.Second)),
		StartedEventId:     common.EmptyEventID,
	}.Build()
	mutableState.EXPECT().GetPendingActivityInfos().Return(map[int64]*persistencespb.ActivityInfo{
		activity1.GetScheduledEventId(): activity1,
		activity2.GetScheduledEventId(): activity2,
	}).AnyTimes()

	mutableState.EXPECT().AddActivityTaskFailedEvent(
		activity1.GetScheduledEventId(),
		activity1.GetStartedEventId(),
		failure.NewResetWorkflowFailure(terminateReason, activity1.GetLastHeartbeatDetails()),
		enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE,
		activity1.GetStartedIdentity(),
		activity1.GetLastWorkerVersionStamp(),
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
	).Return(historypb.HistoryEvent_builder{EventId: wtFailedEventID}.Build(), nil)
	mutableState.EXPECT().FlushBufferedEvents()
	mutableState.EXPECT().AddWorkflowExecutionTerminatedEvent(
		wtFailedEventID,
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

	baseEvent1 := historypb.HistoryEvent_builder{
		EventId:                              124,
		EventType:                            enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{},
	}.Build()
	baseEvent2 := historypb.HistoryEvent_builder{
		EventId:                            125,
		EventType:                          enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{},
	}.Build()
	baseEvent3 := historypb.HistoryEvent_builder{
		EventId:                              126,
		EventType:                            enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{},
	}.Build()
	baseEvent4 := historypb.HistoryEvent_builder{
		EventId:                                127,
		EventType:                              enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
		WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{},
	}.Build()

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
		History:       []*historypb.History{historypb.History_builder{Events: baseEvents}.Build()},
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

	baseEvent1 := historypb.HistoryEvent_builder{
		EventId:                              124,
		EventType:                            enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{},
	}.Build()
	baseEvent2 := historypb.HistoryEvent_builder{
		EventId:                            125,
		EventType:                          enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{},
	}.Build()
	baseEvent3 := historypb.HistoryEvent_builder{
		EventId:                              126,
		EventType:                            enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{},
	}.Build()
	baseEvent4 := historypb.HistoryEvent_builder{
		EventId:   127,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
		WorkflowExecutionContinuedAsNewEventAttributes: historypb.WorkflowExecutionContinuedAsNewEventAttributes_builder{
			NewExecutionRunId: newRunID,
		}.Build(),
	}.Build()

	newEvent1 := historypb.HistoryEvent_builder{
		EventId:                                 1,
		EventType:                               enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{},
	}.Build()
	newEvent2 := historypb.HistoryEvent_builder{
		EventId:                              2,
		EventType:                            enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{},
	}.Build()
	newEvent3 := historypb.HistoryEvent_builder{
		EventId:                            3,
		EventType:                          enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{},
	}.Build()
	newEvent4 := historypb.HistoryEvent_builder{
		EventId:                              4,
		EventType:                            enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{},
	}.Build()
	newEvent5 := historypb.HistoryEvent_builder{
		EventId:                                5,
		EventType:                              enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
		WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{},
	}.Build()

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
		History:       []*historypb.History{historypb.History_builder{Events: baseEvents}.Build()},
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
		History:       []*historypb.History{historypb.History_builder{Events: newEvents}.Build()},
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
	event1 := historypb.HistoryEvent_builder{
		EventId:                                 1,
		EventType:                               enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{},
	}.Build()
	event2 := historypb.HistoryEvent_builder{
		EventId:                              2,
		EventType:                            enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{},
	}.Build()
	event3 := historypb.HistoryEvent_builder{
		EventId:                            3,
		EventType:                          enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{},
	}.Build()
	event4 := historypb.HistoryEvent_builder{
		EventId:                              4,
		EventType:                            enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{},
	}.Build()
	event5 := historypb.HistoryEvent_builder{
		EventId:   5,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
		WorkflowExecutionContinuedAsNewEventAttributes: historypb.WorkflowExecutionContinuedAsNewEventAttributes_builder{
			NewExecutionRunId: newRunID,
		}.Build(),
	}.Build()
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
		History:       []*historypb.History{historypb.History_builder{Events: events}.Build()},
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
	testChildClock := clockspb.VectorClock_builder{ShardId: 1, Clock: 10, ClusterId: 1}.Build()
	testInitiatedEventID := int64(123)
	testChildWFType := commonpb.WorkflowType_builder{Name: "TEST-CHILD-WF-TYPE"}.Build()
	testChildWFExecution := commonpb.WorkflowExecution_builder{
		WorkflowId: uuid.NewString(),
		RunId:      uuid.NewString(),
	}.Build()

	testStartEventHeader := &commonpb.Header{}
	startedEvent := historypb.HistoryEvent_builder{
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
		ChildWorkflowExecutionStartedEventAttributes: historypb.ChildWorkflowExecutionStartedEventAttributes_builder{
			InitiatedEventId:  testInitiatedEventID,
			WorkflowExecution: testChildWFExecution,
			WorkflowType:      testChildWFType,
			Header:            testStartEventHeader,
		}.Build(),
	}.Build()
	testCompletedEventResult := &commonpb.Payloads{}
	completedEvent := historypb.HistoryEvent_builder{
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
		ChildWorkflowExecutionCompletedEventAttributes: historypb.ChildWorkflowExecutionCompletedEventAttributes_builder{
			InitiatedEventId:  testInitiatedEventID,
			WorkflowExecution: testChildWFExecution,
			Result:            testCompletedEventResult,
		}.Build(),
	}.Build()
	startFailedEvent := historypb.HistoryEvent_builder{
		EventType: enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED,
		StartChildWorkflowExecutionFailedEventAttributes: historypb.StartChildWorkflowExecutionFailedEventAttributes_builder{
			InitiatedEventId: testInitiatedEventID,
			WorkflowId:       testChildWFExecution.GetWorkflowId(),
			Cause:            enumspb.START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_NAMESPACE_NOT_FOUND,
		}.Build(),
	}.Build()
	childExecutionFailedEvent := historypb.HistoryEvent_builder{
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED,
		ChildWorkflowExecutionFailedEventAttributes: historypb.ChildWorkflowExecutionFailedEventAttributes_builder{
			InitiatedEventId:  testInitiatedEventID,
			WorkflowExecution: testChildWFExecution,
			Failure:           &failurepb.Failure{},
			RetryState:        enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE,
		}.Build(),
	}.Build()
	childExecutionCancelledEvent := historypb.HistoryEvent_builder{
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED,
		ChildWorkflowExecutionCanceledEventAttributes: historypb.ChildWorkflowExecutionCanceledEventAttributes_builder{
			InitiatedEventId:  testInitiatedEventID,
			WorkflowExecution: testChildWFExecution,
			Details:           &commonpb.Payloads{},
		}.Build(),
	}.Build()
	childExecutionTimeoutEvent := historypb.HistoryEvent_builder{
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT,
		ChildWorkflowExecutionTimedOutEventAttributes: historypb.ChildWorkflowExecutionTimedOutEventAttributes_builder{
			InitiatedEventId:  testInitiatedEventID,
			WorkflowExecution: testChildWFExecution,
			RetryState:        enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE,
		}.Build(),
	}.Build()
	childExecutionTerminatedEvent := historypb.HistoryEvent_builder{
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED,
		ChildWorkflowExecutionTerminatedEventAttributes: historypb.ChildWorkflowExecutionTerminatedEventAttributes_builder{
			InitiatedEventId:  testInitiatedEventID,
			WorkflowExecution: testChildWFExecution,
		}.Build(),
	}.Build()

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
			Return(persistencespb.ChildExecutionInfo_builder{Clock: testChildClock}.Build(), true)

		// Each of the events must be picked with the correct args exactly once.
		mutableState.EXPECT().AddChildWorkflowExecutionStartedEvent(testChildWFExecution, testChildWFType, testInitiatedEventID, testStartEventHeader, testChildClock).Return(nil, nil).Times(1)
		mutableState.EXPECT().AddChildWorkflowExecutionCompletedEvent(
			testInitiatedEventID,
			testChildWFExecution,
			historypb.WorkflowExecutionCompletedEventAttributes_builder{Result: testCompletedEventResult}.Build(),
		).Return(nil, nil).Times(1)
		mutableState.EXPECT().AddStartChildWorkflowExecutionFailedEvent(
			testInitiatedEventID,
			enumspb.START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_NAMESPACE_NOT_FOUND,
			historypb.StartChildWorkflowExecutionInitiatedEventAttributes_builder{WorkflowId: testChildWFExecution.GetWorkflowId()}.Build(),
		).Return(nil, nil).Times(1)
		mutableState.EXPECT().AddChildWorkflowExecutionFailedEvent(
			testInitiatedEventID,
			testChildWFExecution,
			historypb.WorkflowExecutionFailedEventAttributes_builder{Failure: &failurepb.Failure{}, RetryState: enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE}.Build(),
		).Return(nil, nil).Times(1)
		mutableState.EXPECT().AddChildWorkflowExecutionCanceledEvent(
			testInitiatedEventID,
			testChildWFExecution,
			historypb.WorkflowExecutionCanceledEventAttributes_builder{Details: &commonpb.Payloads{}}.Build(),
		).Return(nil, nil).Times(1)
		mutableState.EXPECT().AddChildWorkflowExecutionTimedOutEvent(
			testInitiatedEventID,
			testChildWFExecution,
			historypb.WorkflowExecutionTimedOutEventAttributes_builder{RetryState: enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE}.Build(),
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
	startedEvent := historypb.HistoryEvent_builder{
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
		ChildWorkflowExecutionStartedEventAttributes: historypb.ChildWorkflowExecutionStartedEventAttributes_builder{InitiatedEventId: testInitiatedEventID}.Build(),
	}.Build()
	completedEvent := historypb.HistoryEvent_builder{
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
		ChildWorkflowExecutionCompletedEventAttributes: historypb.ChildWorkflowExecutionCompletedEventAttributes_builder{InitiatedEventId: testInitiatedEventID}.Build(),
	}.Build()
	startFailedEvent := historypb.HistoryEvent_builder{
		EventType: enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED,
		StartChildWorkflowExecutionFailedEventAttributes: historypb.StartChildWorkflowExecutionFailedEventAttributes_builder{InitiatedEventId: testInitiatedEventID}.Build(),
	}.Build()
	childExecutionFailedEvent := historypb.HistoryEvent_builder{
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED,
		ChildWorkflowExecutionFailedEventAttributes: historypb.ChildWorkflowExecutionFailedEventAttributes_builder{InitiatedEventId: testInitiatedEventID}.Build(),
	}.Build()
	childExecutionCancelledEvent := historypb.HistoryEvent_builder{
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED,
		ChildWorkflowExecutionCanceledEventAttributes: historypb.ChildWorkflowExecutionCanceledEventAttributes_builder{InitiatedEventId: testInitiatedEventID}.Build(),
	}.Build()
	childExecutionTimeoutEvent := historypb.HistoryEvent_builder{
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT,
		ChildWorkflowExecutionTimedOutEventAttributes: historypb.ChildWorkflowExecutionTimedOutEventAttributes_builder{InitiatedEventId: testInitiatedEventID}.Build(),
	}.Build()
	childExecutionTerminatedEvent := historypb.HistoryEvent_builder{
		EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED,
		ChildWorkflowExecutionTerminatedEventAttributes: historypb.ChildWorkflowExecutionTerminatedEventAttributes_builder{InitiatedEventId: testInitiatedEventID}.Build(),
	}.Build()

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

	event1 := historypb.HistoryEvent_builder{
		EventId:   101,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		WorkflowExecutionSignaledEventAttributes: historypb.WorkflowExecutionSignaledEventAttributes_builder{
			SignalName: "signal-name-1",
			Input:      payloads.EncodeString("signal-input-1"),
			Identity:   "signal-identity-1",
			Header:     commonpb.Header_builder{Fields: map[string]*commonpb.Payload{"myheader": commonpb.Payload_builder{Data: []byte("myheader")}.Build()}}.Build(),
		}.Build(),
	}.Build()
	// This event is not reapplied
	event2 := historypb.HistoryEvent_builder{
		EventId:                              102,
		EventType:                            enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{},
	}.Build()
	event3 := historypb.HistoryEvent_builder{
		EventId:   103,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		WorkflowExecutionSignaledEventAttributes: historypb.WorkflowExecutionSignaledEventAttributes_builder{
			SignalName: "signal-name-2",
			Input:      payloads.EncodeString("signal-input-2"),
			Identity:   "signal-identity-2",
		}.Build(),
	}.Build()
	event4 := historypb.HistoryEvent_builder{
		EventId:   104,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED,
		WorkflowExecutionUpdateAdmittedEventAttributes: historypb.WorkflowExecutionUpdateAdmittedEventAttributes_builder{
			Request: updatepb.Request_builder{Input: updatepb.Input_builder{Args: payloads.EncodeString("update-request-payload-1")}.Build()}.Build(),
			Origin:  enumspb.UPDATE_ADMITTED_EVENT_ORIGIN_UNSPECIFIED,
		}.Build(),
	}.Build()
	event5 := historypb.HistoryEvent_builder{
		EventId:   105,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED,
		WorkflowExecutionUpdateAcceptedEventAttributes: historypb.WorkflowExecutionUpdateAcceptedEventAttributes_builder{
			AcceptedRequest: updatepb.Request_builder{Input: updatepb.Input_builder{Args: payloads.EncodeString("update-request-payload-1")}.Build()}.Build(),
		}.Build(),
	}.Build()
	// This event is not reapplied
	event6 := historypb.HistoryEvent_builder{
		EventId:   106,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED,
		WorkflowExecutionUpdateCompletedEventAttributes: &historypb.WorkflowExecutionUpdateCompletedEventAttributes{},
	}.Build()
	event7 := historypb.HistoryEvent_builder{
		EventId:   107,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
		WorkflowExecutionCancelRequestedEventAttributes: historypb.WorkflowExecutionCancelRequestedEventAttributes_builder{
			Cause:    testRequestReason,
			Identity: testIdentity,
		}.Build(),
	}.Build()
	event8 := historypb.HistoryEvent_builder{
		EventId:   108,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
		WorkflowExecutionCancelRequestedEventAttributes: historypb.WorkflowExecutionCancelRequestedEventAttributes_builder{
			Cause:    "duplicated cancel cause",
			Identity: "duplicated cancel identity",
		}.Build(),
	}.Build()
	event9 := historypb.HistoryEvent_builder{
		EventId:   109,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
		WorkflowExecutionTerminatedEventAttributes: historypb.WorkflowExecutionTerminatedEventAttributes_builder{
			Reason:   testRequestReason,
			Details:  payloads.EncodeString("test details"),
			Identity: testIdentity,
		}.Build(),
	}.Build()
	event10 := historypb.HistoryEvent_builder{
		EventId:   110,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
		WorkflowExecutionOptionsUpdatedEventAttributes: historypb.WorkflowExecutionOptionsUpdatedEventAttributes_builder{
			AttachedRequestId: "test attached request id",
		}.Build(),
	}.Build()
	// This event is not reapplied
	event11 := historypb.HistoryEvent_builder{
		EventId:   111,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
		WorkflowExecutionTerminatedEventAttributes: historypb.WorkflowExecutionTerminatedEventAttributes_builder{
			Reason:   testRequestReason,
			Details:  payloads.EncodeString("test details"),
			Identity: consts.IdentityHistoryService,
		}.Build(),
	}.Build()
	// This event is not reapplied
	event12 := historypb.HistoryEvent_builder{
		EventId:   112,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
		WorkflowExecutionTerminatedEventAttributes: historypb.WorkflowExecutionTerminatedEventAttributes_builder{
			Reason:   testRequestReason,
			Details:  payloads.EncodeString("test details"),
			Identity: consts.IdentityResetter,
		}.Build(),
	}.Build()
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
						event.GetLinks(),
						attr.GetIdentity(),
						attr.GetPriority(),
					).Return(&historypb.HistoryEvent{}, nil)
				case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
					attr := event.GetWorkflowExecutionSignaledEventAttributes()
					ms.EXPECT().AddWorkflowExecutionSignaled(
						attr.GetSignalName(),
						attr.GetInput(),
						attr.GetIdentity(),
						attr.GetHeader(),
						event.GetLinks(),
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
							historyservice.RequestCancelWorkflowExecutionRequest_builder{
								CancelRequest: workflowservice.RequestCancelWorkflowExecutionRequest_builder{
									Reason:   attr.GetCause(),
									Identity: attr.GetIdentity(),
									Links:    event.GetLinks(),
								}.Build(),
								ExternalInitiatedEventId:  attr.GetExternalInitiatedEventId(),
								ExternalWorkflowExecution: attr.GetExternalWorkflowExecution(),
							}.Build(),
						).Return(&historypb.HistoryEvent{}, nil)
					}
				case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
					if !tc.isReset {
						ms.EXPECT().GetNextEventID().Return(event.GetEventId() + 1)
						ms.EXPECT().GetStartedWorkflowTask().Return(nil)
						attr := event.GetWorkflowExecutionTerminatedEventAttributes()
						ms.EXPECT().AddWorkflowExecutionTerminatedEvent(
							event.GetEventId()+1,
							attr.GetReason(),
							attr.GetDetails(),
							attr.GetIdentity(),
							false,
							event.GetLinks(),
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
	event1 := historypb.HistoryEvent_builder{
		EventId:   101,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		WorkflowExecutionSignaledEventAttributes: historypb.WorkflowExecutionSignaledEventAttributes_builder{
			SignalName: "signal-name-1",
			Input:      payloads.EncodeString("signal-input-1"),
			Identity:   "signal-identity-1",
			Header:     commonpb.Header_builder{Fields: map[string]*commonpb.Payload{"myheader": commonpb.Payload_builder{Data: []byte("myheader")}.Build()}}.Build(),
		}.Build(),
	}.Build()
	event2 := historypb.HistoryEvent_builder{
		EventId:   102,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED,
		WorkflowExecutionUpdateAdmittedEventAttributes: historypb.WorkflowExecutionUpdateAdmittedEventAttributes_builder{
			Request: updatepb.Request_builder{Input: updatepb.Input_builder{Args: payloads.EncodeString("update-request-payload-1")}.Build()}.Build(),
			Origin:  enumspb.UPDATE_ADMITTED_EVENT_ORIGIN_UNSPECIFIED,
		}.Build(),
	}.Build()
	event3 := historypb.HistoryEvent_builder{
		EventId:   103,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED,
		WorkflowExecutionUpdateAcceptedEventAttributes: historypb.WorkflowExecutionUpdateAcceptedEventAttributes_builder{
			AcceptedRequest: updatepb.Request_builder{Input: updatepb.Input_builder{Args: payloads.EncodeString("update-request-payload-1")}.Build()}.Build(),
		}.Build(),
	}.Build()
	event4 := historypb.HistoryEvent_builder{
		EventId:   104,
		EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED,
	}.Build()
	event5 := historypb.HistoryEvent_builder{
		EventId:   105,
		EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED,
	}.Build()
	event6 := historypb.HistoryEvent_builder{
		EventId:   106,
		EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED,
	}.Build()
	events := []*historypb.HistoryEvent{event1, event2, event3, event4, event5, event6}

	ms := historyi.NewMockMutableState(s.controller)
	// Assert that none of these following methods are invoked.
	arg := gomock.Any()
	ms.EXPECT().AddWorkflowExecutionSignaled(arg, arg, arg, arg, arg).Times(0)
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

	event7 := historypb.HistoryEvent_builder{
		EventId:   107,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
		WorkflowExecutionCancelRequestedEventAttributes: historypb.WorkflowExecutionCancelRequestedEventAttributes_builder{
			Cause:    testRequestReason,
			Identity: testIdentity,
		}.Build(),
	}.Build()
	event8 := historypb.HistoryEvent_builder{
		EventId:   108,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
		WorkflowExecutionTerminatedEventAttributes: historypb.WorkflowExecutionTerminatedEventAttributes_builder{
			Reason:   testRequestReason,
			Details:  payloads.EncodeString("test details"),
			Identity: testIdentity,
		}.Build(),
	}.Build()
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

	event1 := historypb.HistoryEvent_builder{
		EventId:                                 1,
		EventType:                               enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{},
	}.Build()
	event2 := historypb.HistoryEvent_builder{
		EventId:                              2,
		EventType:                            enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{},
	}.Build()
	event3 := historypb.HistoryEvent_builder{
		EventId:                            3,
		EventType:                          enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{},
	}.Build()
	event4 := historypb.HistoryEvent_builder{
		EventId:                              4,
		EventType:                            enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{},
	}.Build()
	event5 := historypb.HistoryEvent_builder{
		EventId:                              5,
		EventType:                            enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{},
	}.Build()
	history1 := []*historypb.History{historypb.History_builder{Events: []*historypb.HistoryEvent{event1, event2, event3}}.Build()}
	history2 := []*historypb.History{historypb.History_builder{Events: []*historypb.HistoryEvent{event4, event5}}.Build()}
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
		TaskQueue: taskqueuepb.TaskQueue_builder{
			Name: "random task queue name",
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		}.Build(),
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
		util.Ptr(baseRebuildLastEventVersion),
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
	resetMutableState.EXPECT().GetExecutionState().Return(persistencespb.WorkflowExecutionState_builder{
		RunId:  resetRequestID,
		Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}.Build()).AnyTimes()
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
				callbacks[i] = commonpb.Callback_builder{
					Nexus: commonpb.Callback_Nexus_builder{
						Url: "http://example.com",
						Header: map[string]string{
							"test": "value",
						},
					}.Build(),
				}.Build()
			}

			// Create the event
			event := historypb.HistoryEvent_builder{
				EventId:   101,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
				WorkflowExecutionOptionsUpdatedEventAttributes: historypb.WorkflowExecutionOptionsUpdatedEventAttributes_builder{
					AttachedRequestId:           "test-request-id",
					AttachedCompletionCallbacks: callbacks,
				}.Build(),
			}.Build()

			// Add versioning override if specified
			if tc.hasVersioningOverride {
				event.GetWorkflowExecutionOptionsUpdatedEventAttributes().SetVersioningOverride(workflowpb.VersioningOverride_builder{
					Behavior: enumspb.VERSIONING_BEHAVIOR_PINNED,
				}.Build())
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
		commonpb.Callback_builder{
			Nexus: commonpb.Callback_Nexus_builder{
				Url: "http://example.com",
				Header: map[string]string{
					"test": "value",
				},
			}.Build(),
		}.Build(),
	}

	// Create the event where all callbacks exist but no other updates
	event := historypb.HistoryEvent_builder{
		EventId:   101,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
		WorkflowExecutionOptionsUpdatedEventAttributes: historypb.WorkflowExecutionOptionsUpdatedEventAttributes_builder{
			AttachedRequestId:           "test-request-id",
			AttachedCompletionCallbacks: callbacks,
			// No VersioningOverride and UnsetVersioningOverride is false
		}.Build(),
	}.Build()

	ms.EXPECT().HasRequestID("test-request-id").Return(true)

	events := []*historypb.HistoryEvent{event}

	// Call reapplyEvents - should skip the event (no error, no applied events)
	appliedEvents, err := reapplyEvents(context.Background(), ms, nil, smReg, events, nil, "", true)
	s.NoError(err)
	s.Empty(appliedEvents) // Event should be skipped
}
