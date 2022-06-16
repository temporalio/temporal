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

package history

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
)

type (
	workflowResetterSuite struct {
		suite.Suite
		*require.Assertions

		controller         *gomock.Controller
		mockShard          *shard.ContextTest
		mockStateRebuilder *MocknDCStateRebuilder

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
	s.mockStateRebuilder = NewMocknDCStateRebuilder(s.controller)

	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId: 0,
				RangeId: 1,
			}},
		tests.NewDynamicConfig(),
	)
	s.mockExecutionMgr = s.mockShard.Resource.ExecutionMgr
	s.mockTransaction = workflow.NewMockTransaction(s.controller)

	s.workflowResetter = newWorkflowResetter(
		s.mockShard,
		workflow.NewCache(s.mockShard),
		s.logger,
	)
	s.workflowResetter.newStateRebuilder = func() nDCStateRebuilder {
		return s.mockStateRebuilder
	}
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
	currentWorkflow := NewMocknDCWorkflow(s.controller)
	currentReleaseCalled := false
	currentContext := workflow.NewMockContext(s.controller)
	currentMutableState := workflow.NewMockMutableState(s.controller)
	var currentReleaseFn workflow.ReleaseCacheFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().getContext().Return(currentContext).AnyTimes()
	currentWorkflow.EXPECT().getMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().getReleaseFn().Return(currentReleaseFn).AnyTimes()

	currentEventsSize := int64(2333)
	currentNewEventsSize := int64(3444)
	currentMutation := &persistence.WorkflowMutation{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{},
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
	currentContext.EXPECT().GetHistorySize().Return(currentEventsSize).AnyTimes()
	currentContext.EXPECT().SetHistorySize(currentEventsSize + currentNewEventsSize)

	resetWorkflow := NewMocknDCWorkflow(s.controller)
	resetReleaseCalled := false
	resetContext := workflow.NewMockContext(s.controller)
	resetMutableState := workflow.NewMockMutableState(s.controller)
	var targetReleaseFn workflow.ReleaseCacheFunc = func(error) { resetReleaseCalled = true }
	resetWorkflow.EXPECT().getContext().Return(resetContext).AnyTimes()
	resetWorkflow.EXPECT().getMutableState().Return(resetMutableState).AnyTimes()
	resetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

	resetEventsSize := int64(1444)
	resetNewEventsSize := int64(4321)
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
		gomock.Any(),
		workflow.TransactionPolicyActive,
	).Return(resetSnapshot, resetEventsSeq, nil)
	resetMutableState.EXPECT().GetNamespaceEntry().Return(namespace.FromPersistentState(&persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info:   &persistencespb.NamespaceInfo{},
			Config: &persistencespb.NamespaceConfig{},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: "active-cluster-name",
			},
		},
		IsGlobalNamespace: false,
	}))
	resetContext.EXPECT().GetHistorySize().Return(resetEventsSize).AnyTimes()
	resetContext.EXPECT().SetHistorySize(resetEventsSize + resetNewEventsSize)

	s.mockTransaction.EXPECT().UpdateWorkflowExecution(
		gomock.Any(),
		persistence.UpdateWorkflowModeUpdateCurrent,
		currentMutation,
		currentEventsSeq,
		resetSnapshot,
		resetEventsSeq,
		"active-cluster-name",
	).Return(currentNewEventsSize, resetNewEventsSize, nil)

	err := s.workflowResetter.persistToDB(context.Background(), currentWorkflow, currentMutation, currentEventsSeq, resetWorkflow)
	s.NoError(err)
	// persistToDB function is not charged of releasing locks
	s.False(currentReleaseCalled)
	s.False(resetReleaseCalled)
}

func (s *workflowResetterSuite) TestPersistToDB_CurrentNotTerminated() {
	currentLastWriteVersion := int64(1234)

	currentWorkflow := NewMocknDCWorkflow(s.controller)
	currentReleaseCalled := false
	currentContext := workflow.NewMockContext(s.controller)
	currentMutableState := workflow.NewMockMutableState(s.controller)
	var currentReleaseFn workflow.ReleaseCacheFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().getContext().Return(currentContext).AnyTimes()
	currentWorkflow.EXPECT().getMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().getReleaseFn().Return(currentReleaseFn).AnyTimes()
	currentMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: s.currentRunID,
	}).AnyTimes()

	currentMutableState.EXPECT().GetLastWriteVersion().Return(currentLastWriteVersion, nil).AnyTimes()

	resetWorkflow := NewMocknDCWorkflow(s.controller)
	resetReleaseCalled := false
	resetContext := workflow.NewMockContext(s.controller)
	resetMutableState := workflow.NewMockMutableState(s.controller)
	var targetReleaseFn workflow.ReleaseCacheFunc = func(error) { resetReleaseCalled = true }
	resetWorkflow.EXPECT().getContext().Return(resetContext).AnyTimes()
	resetWorkflow.EXPECT().getMutableState().Return(resetMutableState).AnyTimes()
	resetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

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
		gomock.Any(),
		workflow.TransactionPolicyActive,
	).Return(resetSnapshot, resetEventsSeq, nil)
	resetContext.EXPECT().GetHistorySize().Return(int64(123)).AnyTimes()
	resetContext.EXPECT().CreateWorkflowExecution(
		gomock.Any(),
		gomock.Any(),
		persistence.CreateWorkflowModeUpdateCurrent,
		s.currentRunID,
		currentLastWriteVersion,
		resetMutableState,
		resetSnapshot,
		resetEventsSeq,
	).Return(nil)

	err := s.workflowResetter.persistToDB(context.Background(), currentWorkflow, nil, nil, resetWorkflow)
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
	baseNodeID := baseRebuildLastEventID + 1

	resetBranchToken := []byte("some random reset branch token")
	resetRequestID := uuid.New()
	resetHistorySize := int64(4411)
	resetMutableState := workflow.NewMockMutableState(s.controller)

	shardID := s.mockShard.GetShardID()
	s.mockExecutionMgr.EXPECT().ForkHistoryBranch(gomock.Any(), &persistence.ForkHistoryBranchRequest{
		ForkBranchToken: baseBranchToken,
		ForkNodeID:      baseNodeID,
		Info:            persistence.BuildHistoryGarbageCleanupInfo(s.namespaceID.String(), s.workflowID, s.resetRunID),
		ShardID:         shardID,
	}).Return(&persistence.ForkHistoryBranchResponse{NewBranchToken: resetBranchToken}, nil)

	s.mockStateRebuilder.EXPECT().rebuild(
		ctx,
		gomock.Any(),
		definition.NewWorkflowKey(
			s.namespaceID.String(),
			s.workflowID,
			s.baseRunID,
		),
		baseBranchToken,
		baseRebuildLastEventID,
		convert.Int64Ptr(baseRebuildLastEventVersion),
		definition.NewWorkflowKey(
			s.namespaceID.String(),
			s.workflowID,
			s.resetRunID,
		),
		resetBranchToken,
		resetRequestID,
	).Return(resetMutableState, resetHistorySize, nil)

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
	s.Equal(resetHistorySize, resetWorkflow.getContext().GetHistorySize())
	s.Equal(resetMutableState, resetWorkflow.getMutableState())
}

func (s *workflowResetterSuite) TestFailWorkflowTask_NoWorkflowTask() {
	baseRunID := uuid.New()
	baseRebuildLastEventID := int64(1234)
	baseRebuildLastEventVersion := int64(5678)
	resetRunID := uuid.New()
	resetReason := "some random reset reason"

	mutableState := workflow.NewMockMutableState(s.controller)
	mutableState.EXPECT().GetPendingWorkflowTask().Return(nil, false).AnyTimes()

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
	mutableState.EXPECT().GetPendingWorkflowTask().Return(workflowTaskSchedule, true).AnyTimes()
	mutableState.EXPECT().AddWorkflowTaskStartedEvent(
		workflowTaskSchedule.ScheduledEventID,
		workflowTaskSchedule.RequestID,
		workflowTaskSchedule.TaskQueue,
		consts.IdentityHistoryService,
	).Return(&historypb.HistoryEvent{}, workflowTaskStart, nil)
	mutableState.EXPECT().AddWorkflowTaskFailedEvent(
		workflowTaskStart.ScheduledEventID,
		workflowTaskStart.StartedEventID,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW,
		failure.NewResetWorkflowFailure(resetReason, nil),
		consts.IdentityHistoryService,
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
	mutableState.EXPECT().GetPendingWorkflowTask().Return(workflowTask, true).AnyTimes()
	mutableState.EXPECT().AddWorkflowTaskFailedEvent(
		workflowTask.ScheduledEventID,
		workflowTask.StartedEventID,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW,
		failure.NewResetWorkflowFailure(resetReason, nil),
		consts.IdentityHistoryService,
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
		ScheduledTime:        timestamp.TimePtr(now.Add(-10 * time.Second)),
		StartedEventId:       124,
		LastHeartbeatDetails: payloads.EncodeString("some random activity 1 details"),
		StartedIdentity:      "some random activity 1 started identity",
	}
	activity2 := &persistencespb.ActivityInfo{
		Version:          12,
		ScheduledEventId: 456,
		ScheduledTime:    timestamp.TimePtr(now.Add(-10 * time.Second)),
		StartedEventId:   common.EmptyEventID,
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
	).Return(&historypb.HistoryEvent{}, nil)

	mutableState.EXPECT().UpdateActivity(&persistencespb.ActivityInfo{
		Version:          activity2.Version,
		ScheduledEventId: activity2.ScheduledEventId,
		ScheduledTime:    timestamp.TimePtr(now),
		StartedEventId:   activity2.StartedEventId,
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
	nextEventID := int64(666)
	terminateReason := "some random terminate reason"

	mutableState := workflow.NewMockMutableState(s.controller)

	mutableState.EXPECT().GetNextEventID().Return(nextEventID).AnyTimes()
	mutableState.EXPECT().GetInFlightWorkflowTask().Return(workflowTask, true)
	mutableState.EXPECT().AddWorkflowTaskFailedEvent(
		workflowTask.ScheduledEventID,
		workflowTask.StartedEventID,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_FORCE_CLOSE_COMMAND,
		nil,
		consts.IdentityHistoryService,
		"",
		"",
		"",
		int64(0),
	).Return(&historypb.HistoryEvent{}, nil)
	mutableState.EXPECT().FlushBufferedEvents()
	mutableState.EXPECT().AddWorkflowExecutionTerminatedEvent(
		nextEventID,
		terminateReason,
		nil,
		consts.IdentityResetter,
		false,
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
		PageSize:      nDCDefaultPageSize,
		NextPageToken: nil,
		ShardID:       shardID,
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:       []*historypb.History{{Events: baseEvents}},
		NextPageToken: nil,
	}, nil)

	mutableState := workflow.NewMockMutableState(s.controller)

	lastVisitedRunID, err := s.workflowResetter.reapplyContinueAsNewWorkflowEvents(
		ctx,
		mutableState,
		s.namespaceID,
		s.workflowID,
		s.baseRunID,
		baseBranchToken,
		baseFirstEventID,
		baseNextEventID,
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
		PageSize:      nDCDefaultPageSize,
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
		PageSize:      nDCDefaultPageSize,
		NextPageToken: nil,
		ShardID:       shardID,
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:       []*historypb.History{{Events: newEvents}},
		NextPageToken: nil,
	}, nil)

	resetContext := workflow.NewMockContext(s.controller)
	resetContext.EXPECT().Lock(gomock.Any(), workflow.CallerTypeAPI).Return(nil)
	resetContext.EXPECT().Unlock(workflow.CallerTypeAPI)
	resetMutableState := workflow.NewMockMutableState(s.controller)
	resetContext.EXPECT().LoadWorkflowExecution(gomock.Any()).Return(resetMutableState, nil)
	resetMutableState.EXPECT().GetNextEventID().Return(newNextEventID).AnyTimes()
	resetMutableState.EXPECT().GetCurrentBranchToken().Return(newBranchToken, nil).AnyTimes()
	resetContextCacheKey := definition.NewWorkflowKey(s.namespaceID.String(), s.workflowID, newRunID)
	_, _ = s.workflowResetter.historyCache.(*workflow.CacheImpl).PutIfNotExist(resetContextCacheKey, resetContext)

	mutableState := workflow.NewMockMutableState(s.controller)

	lastVisitedRunID, err := s.workflowResetter.reapplyContinueAsNewWorkflowEvents(
		ctx,
		mutableState,
		s.namespaceID,
		s.workflowID,
		s.baseRunID,
		baseBranchToken,
		baseFirstEventID,
		baseNextEventID,
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
		PageSize:      nDCDefaultPageSize,
		NextPageToken: nil,
		ShardID:       shardID,
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:       []*historypb.History{{Events: events}},
		NextPageToken: nil,
	}, nil)

	mutableState := workflow.NewMockMutableState(s.controller)

	nextRunID, err := s.workflowResetter.reapplyWorkflowEvents(
		context.Background(),
		mutableState,
		firstEventID,
		nextEventID,
		branchToken,
	)
	s.NoError(err)
	s.Equal(newRunID, nextRunID)
}

func (s *workflowResetterSuite) TestReapplyEvents() {

	event1 := &historypb.HistoryEvent{
		EventId:   101,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
			SignalName: "some random signal name",
			Input:      payloads.EncodeString("some random signal input"),
			Identity:   "some random signal identity",
			Header:     &commonpb.Header{Fields: map[string]*commonpb.Payload{"myheader": {Data: []byte("myheader")}}},
		}},
	}
	event2 := &historypb.HistoryEvent{
		EventId:    102,
		EventType:  enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{}},
	}
	event3 := &historypb.HistoryEvent{
		EventId:   103,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
			SignalName: "another random signal name",
			Input:      payloads.EncodeString("another random signal input"),
			Identity:   "another random signal identity",
		}},
	}
	events := []*historypb.HistoryEvent{event1, event2, event3}

	mutableState := workflow.NewMockMutableState(s.controller)

	for _, event := range events {
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
			attr := event.GetWorkflowExecutionSignaledEventAttributes()
			mutableState.EXPECT().AddWorkflowExecutionSignaled(
				attr.GetSignalName(),
				attr.GetInput(),
				attr.GetIdentity(),
				attr.GetHeader(),
			).Return(&historypb.HistoryEvent{}, nil)
		}
	}

	err := s.workflowResetter.reapplyEvents(mutableState, events)
	s.NoError(err)
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
		PageSize:      nDCDefaultPageSize,
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
		PageSize:      nDCDefaultPageSize,
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
