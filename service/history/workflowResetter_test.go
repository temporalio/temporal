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

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"

	"go.temporal.io/server/api/persistenceblobs/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/loggerimpl"
	"go.temporal.io/server/common/mocks"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
)

type (
	workflowResetterSuite struct {
		suite.Suite
		*require.Assertions

		controller         *gomock.Controller
		mockShard          *shardContextTest
		mockStateRebuilder *MocknDCStateRebuilder

		mockHistoryV2Mgr *mocks.HistoryV2Manager

		logger       log.Logger
		namespaceID  string
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

	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)
	s.controller = gomock.NewController(s.T())
	s.mockStateRebuilder = NewMocknDCStateRebuilder(s.controller)

	s.mockShard = newTestShardContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistenceblobs.ShardInfo{
				ShardId:          0,
				RangeId:          1,
				TransferAckLevel: 0,
			}},
		NewDynamicConfigForTest(),
	)
	s.mockHistoryV2Mgr = s.mockShard.resource.HistoryMgr

	s.workflowResetter = newWorkflowResetter(
		s.mockShard,
		newHistoryCache(s.mockShard),
		s.logger,
	)
	s.workflowResetter.newStateRebuilder = func() nDCStateRebuilder {
		return s.mockStateRebuilder
	}

	s.namespaceID = testNamespaceID
	s.workflowID = "some random workflow ID"
	s.baseRunID = uuid.New()
	s.currentRunID = uuid.New()
	s.resetRunID = uuid.New()
}

func (s *workflowResetterSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.Finish(s.T())
}

func (s *workflowResetterSuite) TestPersistToDB_CurrentTerminated() {
	currentWorkflow := NewMocknDCWorkflow(s.controller)
	currentReleaseCalled := false
	currentContext := NewMockworkflowExecutionContext(s.controller)
	currentMutableState := NewMockmutableState(s.controller)
	var currentReleaseFn releaseWorkflowExecutionFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().getContext().Return(currentContext).AnyTimes()
	currentWorkflow.EXPECT().getMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().getReleaseFn().Return(currentReleaseFn).AnyTimes()

	resetWorkflow := NewMocknDCWorkflow(s.controller)
	resetReleaseCalled := false
	resetContext := NewMockworkflowExecutionContext(s.controller)
	resetMutableState := NewMockmutableState(s.controller)
	var targetReleaseFn releaseWorkflowExecutionFunc = func(error) { resetReleaseCalled = true }
	resetWorkflow.EXPECT().getContext().Return(resetContext).AnyTimes()
	resetWorkflow.EXPECT().getMutableState().Return(resetMutableState).AnyTimes()
	resetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

	currentContext.EXPECT().updateWorkflowExecutionWithNewAsActive(
		gomock.Any(),
		resetContext,
		resetMutableState,
	).Return(nil).Times(1)

	err := s.workflowResetter.persistToDB(true, currentWorkflow, resetWorkflow)
	s.NoError(err)
	// persistToDB function is not charged of releasing locks
	s.False(currentReleaseCalled)
	s.False(resetReleaseCalled)
}

func (s *workflowResetterSuite) TestPersistToDB_CurrentNotTerminated() {
	currentLastWriteVersion := int64(1234)

	currentWorkflow := NewMocknDCWorkflow(s.controller)
	currentReleaseCalled := false
	currentContext := NewMockworkflowExecutionContext(s.controller)
	currentMutableState := NewMockmutableState(s.controller)
	var currentReleaseFn releaseWorkflowExecutionFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().getContext().Return(currentContext).AnyTimes()
	currentWorkflow.EXPECT().getMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().getReleaseFn().Return(currentReleaseFn).AnyTimes()

	currentMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		RunID: s.currentRunID,
	}).AnyTimes()
	currentMutableState.EXPECT().GetLastWriteVersion().Return(currentLastWriteVersion, nil).AnyTimes()

	resetWorkflow := NewMocknDCWorkflow(s.controller)
	resetReleaseCalled := false
	resetContext := NewMockworkflowExecutionContext(s.controller)
	resetMutableState := NewMockmutableState(s.controller)
	var targetReleaseFn releaseWorkflowExecutionFunc = func(error) { resetReleaseCalled = true }
	resetWorkflow.EXPECT().getContext().Return(resetContext).AnyTimes()
	resetWorkflow.EXPECT().getMutableState().Return(resetMutableState).AnyTimes()
	resetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

	resetSnapshot := &persistence.WorkflowSnapshot{}
	resetEventsSeq := []*persistence.WorkflowEvents{{
		NamespaceID: s.namespaceID,
		WorkflowID:  s.workflowID,
		RunID:       s.resetRunID,
		BranchToken: []byte("some random reset branch token"),
		Events: []*historypb.HistoryEvent{{
			EventId: 123,
		}},
	}}
	resetEventsSize := int64(4321)
	resetMutableState.EXPECT().CloseTransactionAsSnapshot(
		gomock.Any(),
		transactionPolicyActive,
	).Return(resetSnapshot, resetEventsSeq, nil).Times(1)
	resetContext.EXPECT().persistFirstWorkflowEvents(resetEventsSeq[0]).Return(resetEventsSize, nil).Times(1)
	resetContext.EXPECT().createWorkflowExecution(
		resetSnapshot,
		resetEventsSize,
		gomock.Any(),
		persistence.CreateWorkflowModeContinueAsNew,
		s.currentRunID,
		currentLastWriteVersion,
	).Return(nil).Times(1)

	err := s.workflowResetter.persistToDB(false, currentWorkflow, resetWorkflow)
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
	resetMutableState := NewMockmutableState(s.controller)

	shardId := s.mockShard.GetShardID()
	s.mockHistoryV2Mgr.On("ForkHistoryBranch", &persistence.ForkHistoryBranchRequest{
		ForkBranchToken: baseBranchToken,
		ForkNodeID:      baseNodeID,
		Info:            persistence.BuildHistoryGarbageCleanupInfo(s.namespaceID, s.workflowID, s.resetRunID),
		ShardID:         &shardId,
	}).Return(&persistence.ForkHistoryBranchResponse{NewBranchToken: resetBranchToken}, nil).Times(1)

	s.mockStateRebuilder.EXPECT().rebuild(
		ctx,
		gomock.Any(),
		definition.NewWorkflowIdentifier(
			s.namespaceID,
			s.workflowID,
			s.baseRunID,
		),
		baseBranchToken,
		baseRebuildLastEventID,
		baseRebuildLastEventVersion,
		definition.NewWorkflowIdentifier(
			s.namespaceID,
			s.workflowID,
			s.resetRunID,
		),
		resetBranchToken,
		resetRequestID,
	).Return(resetMutableState, resetHistorySize, nil).Times(1)

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
	s.Equal(resetHistorySize, resetWorkflow.getContext().getHistorySize())
	s.Equal(resetMutableState, resetWorkflow.getMutableState())
}

func (s *workflowResetterSuite) TestFailInflightActivity() {
	terminateReason := "some random termination reason"

	mutableState := NewMockmutableState(s.controller)

	activity1 := &persistence.ActivityInfo{
		Version:         12,
		ScheduleID:      123,
		StartedID:       124,
		Details:         payloads.EncodeString("some random activity 1 details"),
		StartedIdentity: "some random activity 1 started identity",
	}
	activity2 := &persistence.ActivityInfo{
		Version:    12,
		ScheduleID: 456,
		StartedID:  common.EmptyEventID,
	}
	mutableState.EXPECT().GetPendingActivityInfos().Return(map[int64]*persistence.ActivityInfo{
		activity1.ScheduleID: activity1,
		activity2.ScheduleID: activity2,
	}).AnyTimes()

	mutableState.EXPECT().AddActivityTaskFailedEvent(
		activity1.ScheduleID,
		activity1.StartedID,
		failure.NewResetWorkflowFailure(terminateReason, activity1.Details),
		enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE,
		activity1.StartedIdentity,
	).Return(&historypb.HistoryEvent{}, nil).Times(1)

	err := s.workflowResetter.failInflightActivity(mutableState, terminateReason)
	s.NoError(err)
}

func (s *workflowResetterSuite) TestGenerateBranchToken() {
	baseBranchToken := []byte("some random base branch token")
	baseNodeID := int64(1234)

	resetBranchToken := []byte("some random reset branch token")

	shardId := s.mockShard.GetShardID()
	s.mockHistoryV2Mgr.On("ForkHistoryBranch", &persistence.ForkHistoryBranchRequest{
		ForkBranchToken: baseBranchToken,
		ForkNodeID:      baseNodeID,
		Info:            persistence.BuildHistoryGarbageCleanupInfo(s.namespaceID, s.workflowID, s.resetRunID),
		ShardID:         &shardId,
	}).Return(&persistence.ForkHistoryBranchResponse{NewBranchToken: resetBranchToken}, nil).Times(1)

	newBranchToken, err := s.workflowResetter.generateBranchToken(
		s.namespaceID, s.workflowID, baseBranchToken, baseNodeID, s.resetRunID,
	)
	s.NoError(err)
	s.Equal(resetBranchToken, newBranchToken)
}

func (s *workflowResetterSuite) TestTerminateWorkflow() {
	decision := &decisionInfo{
		Version:    123,
		ScheduleID: 1234,
		StartedID:  5678,
	}
	nextEventID := int64(666)
	terminateReason := "some random terminate reason"

	mutableState := NewMockmutableState(s.controller)

	mutableState.EXPECT().GetNextEventID().Return(nextEventID).AnyTimes()
	mutableState.EXPECT().GetInFlightDecision().Return(decision, true).Times(1)
	mutableState.EXPECT().AddDecisionTaskFailedEvent(
		decision.ScheduleID,
		decision.StartedID,
		enumspb.DECISION_TASK_FAILED_CAUSE_FORCE_CLOSE_DECISION,
		nil,
		identityHistoryService,
		"",
		"",
		"",
		int64(0),
	).Return(&historypb.HistoryEvent{}, nil).Times(1)
	mutableState.EXPECT().FlushBufferedEvents().Return(nil).Times(1)
	mutableState.EXPECT().AddWorkflowExecutionTerminatedEvent(
		nextEventID,
		terminateReason,
		nil,
		identityHistoryService,
	).Return(&historypb.HistoryEvent{}, nil).Times(1)

	err := s.workflowResetter.terminateWorkflow(mutableState, terminateReason)
	s.NoError(err)
}

func (s *workflowResetterSuite) TestReapplyContinueAsNewWorkflowEvents() {
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
		EventType:  enumspb.EVENT_TYPE_DECISION_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &historypb.DecisionTaskScheduledEventAttributes{}},
	}
	baseEvent2 := &historypb.HistoryEvent{
		EventId:    125,
		EventType:  enumspb.EVENT_TYPE_DECISION_TASK_STARTED,
		Attributes: &historypb.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &historypb.DecisionTaskStartedEventAttributes{}},
	}
	baseEvent3 := &historypb.HistoryEvent{
		EventId:    126,
		EventType:  enumspb.EVENT_TYPE_DECISION_TASK_COMPLETED,
		Attributes: &historypb.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &historypb.DecisionTaskCompletedEventAttributes{}},
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
		EventType:  enumspb.EVENT_TYPE_DECISION_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &historypb.DecisionTaskScheduledEventAttributes{}},
	}
	newEvent3 := &historypb.HistoryEvent{
		EventId:    3,
		EventType:  enumspb.EVENT_TYPE_DECISION_TASK_STARTED,
		Attributes: &historypb.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &historypb.DecisionTaskStartedEventAttributes{}},
	}
	newEvent4 := &historypb.HistoryEvent{
		EventId:    4,
		EventType:  enumspb.EVENT_TYPE_DECISION_TASK_COMPLETED,
		Attributes: &historypb.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &historypb.DecisionTaskCompletedEventAttributes{}},
	}
	newEvent5 := &historypb.HistoryEvent{
		EventId:    5,
		EventType:  enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{}},
	}

	baseEvents := []*historypb.HistoryEvent{baseEvent1, baseEvent2, baseEvent3, baseEvent4}
	shardId := s.mockShard.GetShardID()
	s.mockHistoryV2Mgr.On("ReadHistoryBranchByBatch", &persistence.ReadHistoryBranchRequest{
		BranchToken:   baseBranchToken,
		MinEventID:    baseFirstEventID,
		MaxEventID:    baseNextEventID,
		PageSize:      nDCDefaultPageSize,
		NextPageToken: nil,
		ShardID:       &shardId,
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:       []*historypb.History{{Events: baseEvents}},
		NextPageToken: nil,
	}, nil).Once()

	newEvents := []*historypb.HistoryEvent{newEvent1, newEvent2, newEvent3, newEvent4, newEvent5}
	s.mockHistoryV2Mgr.On("ReadHistoryBranchByBatch", &persistence.ReadHistoryBranchRequest{
		BranchToken:   newBranchToken,
		MinEventID:    newFirstEventID,
		MaxEventID:    newNextEventID,
		PageSize:      nDCDefaultPageSize,
		NextPageToken: nil,
		ShardID:       &shardId,
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:       []*historypb.History{{Events: newEvents}},
		NextPageToken: nil,
	}, nil).Once()

	resetContext := NewMockworkflowExecutionContext(s.controller)
	resetContext.EXPECT().lock(gomock.Any()).Return(nil).Times(1)
	resetContext.EXPECT().unlock().Times(1)
	resetMutableState := NewMockmutableState(s.controller)
	resetContext.EXPECT().loadWorkflowExecution().Return(resetMutableState, nil).Times(1)
	resetMutableState.EXPECT().GetNextEventID().Return(newNextEventID).AnyTimes()
	resetMutableState.EXPECT().GetCurrentBranchToken().Return(newBranchToken, nil).AnyTimes()
	resetContextCacheKey := definition.NewWorkflowIdentifier(s.namespaceID, s.workflowID, newRunID)
	_, _ = s.workflowResetter.historyCache.PutIfNotExist(resetContextCacheKey, resetContext)

	mutableState := NewMockmutableState(s.controller)

	err := s.workflowResetter.reapplyContinueAsNewWorkflowEvents(
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
		EventType:  enumspb.EVENT_TYPE_DECISION_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &historypb.DecisionTaskScheduledEventAttributes{}},
	}
	event3 := &historypb.HistoryEvent{
		EventId:    3,
		EventType:  enumspb.EVENT_TYPE_DECISION_TASK_STARTED,
		Attributes: &historypb.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &historypb.DecisionTaskStartedEventAttributes{}},
	}
	event4 := &historypb.HistoryEvent{
		EventId:    4,
		EventType:  enumspb.EVENT_TYPE_DECISION_TASK_COMPLETED,
		Attributes: &historypb.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &historypb.DecisionTaskCompletedEventAttributes{}},
	}
	event5 := &historypb.HistoryEvent{
		EventId:   5,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{WorkflowExecutionContinuedAsNewEventAttributes: &historypb.WorkflowExecutionContinuedAsNewEventAttributes{
			NewExecutionRunId: newRunID,
		}},
	}
	events := []*historypb.HistoryEvent{event1, event2, event3, event4, event5}
	shardId := s.mockShard.GetShardID()
	s.mockHistoryV2Mgr.On("ReadHistoryBranchByBatch", &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      nDCDefaultPageSize,
		NextPageToken: nil,
		ShardID:       &shardId,
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:       []*historypb.History{{Events: events}},
		NextPageToken: nil,
	}, nil).Once()

	mutableState := NewMockmutableState(s.controller)

	nextRunID, err := s.workflowResetter.reapplyWorkflowEvents(
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
		}},
	}
	event2 := &historypb.HistoryEvent{
		EventId:    102,
		EventType:  enumspb.EVENT_TYPE_DECISION_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &historypb.DecisionTaskScheduledEventAttributes{}},
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

	mutableState := NewMockmutableState(s.controller)

	for _, event := range events {
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
			attr := event.GetWorkflowExecutionSignaledEventAttributes()
			mutableState.EXPECT().AddWorkflowExecutionSignaled(
				attr.GetSignalName(),
				attr.GetInput(),
				attr.GetIdentity(),
			).Return(&historypb.HistoryEvent{}, nil).Times(1)
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
		EventType:  enumspb.EVENT_TYPE_DECISION_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &historypb.DecisionTaskScheduledEventAttributes{}},
	}
	event3 := &historypb.HistoryEvent{
		EventId:    3,
		EventType:  enumspb.EVENT_TYPE_DECISION_TASK_STARTED,
		Attributes: &historypb.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &historypb.DecisionTaskStartedEventAttributes{}},
	}
	event4 := &historypb.HistoryEvent{
		EventId:    4,
		EventType:  enumspb.EVENT_TYPE_DECISION_TASK_COMPLETED,
		Attributes: &historypb.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &historypb.DecisionTaskCompletedEventAttributes{}},
	}
	event5 := &historypb.HistoryEvent{
		EventId:    5,
		EventType:  enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{}},
	}
	history1 := []*historypb.History{{[]*historypb.HistoryEvent{event1, event2, event3}}}
	history2 := []*historypb.History{{[]*historypb.HistoryEvent{event4, event5}}}
	history := append(history1, history2...)
	pageToken := []byte("some random token")

	shardId := s.mockShard.GetShardID()
	s.mockHistoryV2Mgr.On("ReadHistoryBranchByBatch", &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      nDCDefaultPageSize,
		NextPageToken: nil,
		ShardID:       &shardId,
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:       history1,
		NextPageToken: pageToken,
		Size:          12345,
	}, nil).Once()
	s.mockHistoryV2Mgr.On("ReadHistoryBranchByBatch", &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      nDCDefaultPageSize,
		NextPageToken: pageToken,
		ShardID:       &shardId,
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:       history2,
		NextPageToken: nil,
		Size:          67890,
	}, nil).Once()

	paginationFn := s.workflowResetter.getPaginationFn(firstEventID, nextEventID, branchToken)
	iter := collection.NewPagingIterator(paginationFn)

	var result []*historypb.History
	for iter.HasNext() {
		item, err := iter.Next()
		s.NoError(err)
		result = append(result, item.(*historypb.History))
	}

	s.Equal(history, result)
}
