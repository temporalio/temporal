// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package ndc

import (
	"context"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
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
	s.baseRunID = uuid.New()
	s.currentRunID = uuid.New()
	s.resetRunID = uuid.New()
}

func (s *workflowResetterSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *workflowResetterSuite) TestPersistToDB_CurrentTerminated() {
	currentWorkflow := NewMockWorkflow(s.controller)
	currentReleaseCalled := false
	currentContext := workflow.NewMockContext(s.controller)
	currentMutableState := workflow.NewMockMutableState(s.controller)
	var currentReleaseFn wcache.ReleaseCacheFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetContext().Return(currentContext).AnyTimes()
	currentWorkflow.EXPECT().GetMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()
	currentMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: s.currentRunID,
	}).AnyTimes()

	currentMutableState.EXPECT().GetCurrentVersion().Return(int64(0)).AnyTimes()
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
	resetContext := workflow.NewMockContext(s.controller)
	resetMutableState := workflow.NewMockMutableState(s.controller)
	var tarGetReleaseFn wcache.ReleaseCacheFunc = func(error) { resetReleaseCalled = true }
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
		workflow.TransactionPolicyActive,
	).Return(resetSnapshot, resetEventsSeq, nil)

	s.mockTransaction.EXPECT().UpdateWorkflowExecution(
		gomock.Any(),
		persistence.UpdateWorkflowModeUpdateCurrent,
		int64(0),
		currentMutation,
		currentEventsSeq,
		util.Ptr(int64(0)),
		resetSnapshot,
		resetEventsSeq,
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
	currentContext := workflow.NewMockContext(s.controller)
	currentMutableState := workflow.NewMockMutableState(s.controller)
	var currentReleaseFn wcache.ReleaseCacheFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetContext().Return(currentContext).AnyTimes()
	currentWorkflow.EXPECT().GetMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()
	currentMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: s.currentRunID,
	}).AnyTimes()

	currentMutation := &persistence.WorkflowMutation{}
	currentEventsSeq := []*persistence.WorkflowEvents{{}}
	currentMutableState.EXPECT().GetCurrentVersion().Return(int64(0)).AnyTimes()
	currentMutableState.EXPECT().CloseTransactionAsMutation(workflow.TransactionPolicyActive).Return(currentMutation, currentEventsSeq, nil)

	resetWorkflow := NewMockWorkflow(s.controller)
	resetReleaseCalled := false
	resetContext := workflow.NewMockContext(s.controller)
	resetMutableState := workflow.NewMockMutableState(s.controller)
	resetMutableState.EXPECT().GetCurrentVersion().Return(int64(0)).AnyTimes()
	var tarGetReleaseFn wcache.ReleaseCacheFunc = func(error) { resetReleaseCalled = true }
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
		workflow.TransactionPolicyActive,
	).Return(resetSnapshot, resetEventsSeq, nil)

	s.mockTransaction.EXPECT().UpdateWorkflowExecution(
		gomock.Any(),
		persistence.UpdateWorkflowModeUpdateCurrent,
		int64(0),
		currentMutation,
		currentEventsSeq,
		util.Ptr(int64(0)),
		resetSnapshot,
		resetEventsSeq,
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
	resetRequestID := uuid.New()
	resetHistorySize := int64(4411)
	resetMutableState := workflow.NewMockMutableState(s.controller)

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
	).Return(resetMutableState, resetHistorySize, nil)
	resetMutableState.EXPECT().SetBaseWorkflow(
		s.baseRunID,
		baseRebuildLastEventID,
		baseRebuildLastEventVersion,
	)
	resetMutableState.EXPECT().AddHistorySize(resetHistorySize)

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
	baseRunID := uuid.New()
	baseRebuildLastEventID := int64(1234)
	baseRebuildLastEventVersion := int64(5678)
	resetRunID := uuid.New()
	resetReason := "some random reset reason"

	mutableState := workflow.NewMockMutableState(s.controller)
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
	baseRunID := uuid.New()
	baseRebuildLastEventID := int64(1234)
	baseRebuildLastEventVersion := int64(5678)
	resetRunID := uuid.New()
	resetReason := "some random reset reason"

	mutableState := workflow.NewMockMutableState(s.controller)
	workflowTaskSchedule := &workflow.WorkflowTaskInfo{
		ScheduledEventID: baseRebuildLastEventID - 12,
		StartedEventID:   common.EmptyEventID,
		RequestID:        uuid.New(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: "random task queue name",
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
	}
	workflowTaskStart := &workflow.WorkflowTaskInfo{
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
		true,
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
	baseRunID := uuid.New()
	baseRebuildLastEventID := int64(1234)
	baseRebuildLastEventVersion := int64(5678)
	resetRunID := uuid.New()
	resetReason := "some random reset reason"

	mutableState := workflow.NewMockMutableState(s.controller)
	workflowTask := &workflow.WorkflowTaskInfo{
		ScheduledEventID: baseRebuildLastEventID - 12,
		StartedEventID:   baseRebuildLastEventID - 10,
		RequestID:        uuid.New(),
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

	mutableState := workflow.NewMockMutableState(s.controller)

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

	mutableState.EXPECT().UpdateActivity(&persistencespb.ActivityInfo{
		Version:            activity2.Version,
		ScheduledEventId:   activity2.ScheduledEventId,
		ScheduledTime:      timestamppb.New(now),
		FirstScheduledTime: timestamppb.New(now),
		StartedEventId:     activity2.StartedEventId,
	}).Return(nil)

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
	workflowTask := &workflow.WorkflowTaskInfo{
		Version:          123,
		ScheduledEventID: 1234,
		StartedEventID:   5678,
	}
	wtFailedEventID := int64(666)
	terminateReason := "some random terminate reason"

	mutableState := workflow.NewMockMutableState(s.controller)

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

	mutableState := workflow.NewMockMutableState(s.controller)
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
	)
	s.NoError(err)
	s.Equal(s.baseRunID, lastVisitedRunID)
}

func (s *workflowResetterSuite) TestReapplyContinueAsNewWorkflowEvents_WithContinueAsNewChain() {
	ctx := context.Background()
	baseFirstEventID := int64(124)
	baseNextEventID := int64(456)
	baseBranchToken := []byte("some random base branch token")

	newRunID := uuid.New()
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

	resetContext := workflow.NewMockContext(s.controller)
	resetContext.EXPECT().Lock(gomock.Any(), locks.PriorityHigh).Return(nil)
	resetContext.EXPECT().Unlock()
	resetContext.EXPECT().IsDirty().Return(false).AnyTimes()
	resetMutableState := workflow.NewMockMutableState(s.controller)
	resetContextCacheKey := wcache.Key{
		WorkflowKey: definition.NewWorkflowKey(s.namespaceID.String(), s.workflowID, newRunID),
		ShardUUID:   s.mockShard.GetOwner(),
	}
	resetContext.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(resetMutableState, nil)
	resetMutableState.EXPECT().GetNextEventID().Return(newNextEventID).AnyTimes()
	resetMutableState.EXPECT().GetCurrentBranchToken().Return(newBranchToken, nil).AnyTimes()
	err := wcache.PutContextIfNotExist(s.workflowResetter.workflowCache, resetContextCacheKey, resetContext)
	s.NoError(err)

	mutableState := workflow.NewMockMutableState(s.controller)
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
	)
	s.NoError(err)
	s.Equal(newRunID, lastVisitedRunID)
}

func (s *workflowResetterSuite) TestReapplyWorkflowEvents() {
	firstEventID := common.FirstEventID
	nextEventID := int64(6)
	branchToken := []byte("some random branch token")

	newRunID := uuid.New()
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

	mutableState := workflow.NewMockMutableState(s.controller)
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
	)
	s.NoError(err)
	s.Equal(newRunID, nextRunID)
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
		EventId:   105,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionUpdateCompletedEventAttributes{
			WorkflowExecutionUpdateCompletedEventAttributes: &historypb.WorkflowExecutionUpdateCompletedEventAttributes{},
		},
	}
	events := []*historypb.HistoryEvent{event1, event2, event3, event4, event5, event6}

	ms := workflow.NewMockMutableState(s.controller)

	for _, event := range events {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
			attr := event.GetWorkflowExecutionSignaledEventAttributes()
			ms.EXPECT().AddWorkflowExecutionSignaled(
				attr.GetSignalName(),
				attr.GetInput(),
				attr.GetIdentity(),
				attr.GetHeader(),
				attr.GetSkipGenerateWorkflowTask(),
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
		}
	}

	smReg := hsm.NewRegistry()
	s.NoError(workflow.RegisterStateMachine(smReg))
	root, err := hsm.NewRoot(smReg, workflow.StateMachineType, nil, make(map[string]*persistencespb.StateMachineMap), nil)
	s.NoError(err)
	ms.EXPECT().HSM().Return(root).AnyTimes()

	_, err = reapplyEvents(context.Background(), ms, nil, smReg, events, nil, "")
	s.NoError(err)
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

	ms := workflow.NewMockMutableState(s.controller)
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
	reappliedEvents, err := reapplyEvents(context.Background(), ms, nil, smReg, events, excludes, "")
	s.Empty(reappliedEvents)
	s.NoError(err)
}

func (s *workflowResetterSuite) TestReapplyContinueAsNewWorkflowEvents_ExcludeAllEvents() {
	ctx := context.Background()
	baseFirstEventID := int64(123)
	baseNextEventID := int64(456)
	baseBranchToken := []byte("some random base branch token")
	optionExcludeAllReapplyEvents := map[enumspb.ResetReapplyExcludeType]struct{}{
		enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL: {},
		enumspb.RESET_REAPPLY_EXCLUDE_TYPE_UPDATE: {},
		enumspb.RESET_REAPPLY_EXCLUDE_TYPE_NEXUS:  {},
	}

	mutableState := workflow.NewMockMutableState(s.controller)
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
	resetRequestID := uuid.New()
	resetHistorySize := int64(4411)
	resetMutableState := workflow.NewMockMutableState(s.controller)
	executionInfos := make(map[int64]*persistencespb.ChildExecutionInfo)

	workflowTaskSchedule := &workflow.WorkflowTaskInfo{
		ScheduledEventID: baseRebuildLastEventID - 12,
		StartedEventID:   common.EmptyEventID,
		RequestID:        uuid.New(),
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
		util.Ptr(baseRebuildLastEventVersion),
		definition.NewWorkflowKey(s.namespaceID.String(), s.workflowID, s.resetRunID),
		resetBranchToken,
		resetRequestID,
	).Return(resetMutableState, resetHistorySize, nil)

	resetMutableState.EXPECT().SetBaseWorkflow(s.baseRunID, baseRebuildLastEventID, baseRebuildLastEventVersion)
	resetMutableState.EXPECT().AddHistorySize(resetHistorySize)
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

	workflowTaskStart := &workflow.WorkflowTaskInfo{
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
		true,
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
	)
	s.NoError(err)
	s.Equal(resetMutableState, resetWorkflow.GetMutableState())
}
