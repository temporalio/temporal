// Copyright (c) 2019 Uber Technologies, Inc.
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

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/collection"
	"github.com/uber/cadence/common/definition"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
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
		domainID     string
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
		&persistence.ShardInfo{
			ShardID:          0,
			RangeID:          1,
			TransferAckLevel: 0,
		},
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

	s.domainID = testDomainID
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
	currentWorkflowTerminated := true

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

	err := s.workflowResetter.persistToDB(currentWorkflowTerminated, currentWorkflow, resetWorkflow)
	s.NoError(err)
	// persistToDB function is not charged of releasing locks
	s.False(currentReleaseCalled)
	s.False(resetReleaseCalled)
}

func (s *workflowResetterSuite) TestPersistToDB_CurrentNotTerminated() {
	currentWorkflowTerminated := false
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
		DomainID:    s.domainID,
		WorkflowID:  s.workflowID,
		RunID:       s.resetRunID,
		BranchToken: []byte("some random reset branch token"),
		Events: []*shared.HistoryEvent{{
			EventId: common.Int64Ptr(123),
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

	err := s.workflowResetter.persistToDB(currentWorkflowTerminated, currentWorkflow, resetWorkflow)
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

	s.mockHistoryV2Mgr.On("ForkHistoryBranch", &persistence.ForkHistoryBranchRequest{
		ForkBranchToken: baseBranchToken,
		ForkNodeID:      baseNodeID,
		Info:            persistence.BuildHistoryGarbageCleanupInfo(s.domainID, s.workflowID, s.resetRunID),
		ShardID:         common.IntPtr(s.mockShard.GetShardID()),
	}).Return(&persistence.ForkHistoryBranchResponse{NewBranchToken: resetBranchToken}, nil).Times(1)

	s.mockStateRebuilder.EXPECT().rebuild(
		ctx,
		gomock.Any(),
		definition.NewWorkflowIdentifier(
			s.domainID,
			s.workflowID,
			s.baseRunID,
		),
		baseBranchToken,
		baseRebuildLastEventID,
		baseRebuildLastEventVersion,
		definition.NewWorkflowIdentifier(
			s.domainID,
			s.workflowID,
			s.resetRunID,
		),
		resetBranchToken,
		resetRequestID,
	).Return(resetMutableState, resetHistorySize, nil).Times(1)

	resetWorkflow, err := s.workflowResetter.replayResetWorkflow(
		ctx,
		s.domainID,
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
		Details:         []byte("some random activity 1 details"),
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
		&shared.RespondActivityTaskFailedRequest{
			Reason:   common.StringPtr(terminateReason),
			Details:  activity1.Details,
			Identity: common.StringPtr(activity1.StartedIdentity),
		},
	).Return(&shared.HistoryEvent{}, nil).Times(1)

	err := s.workflowResetter.failInflightActivity(mutableState, terminateReason)
	s.NoError(err)
}

func (s *workflowResetterSuite) TestGenerateBranchToken() {
	baseBranchToken := []byte("some random base branch token")
	baseNodeID := int64(1234)

	resetBranchToken := []byte("some random reset branch token")

	s.mockHistoryV2Mgr.On("ForkHistoryBranch", &persistence.ForkHistoryBranchRequest{
		ForkBranchToken: baseBranchToken,
		ForkNodeID:      baseNodeID,
		Info:            persistence.BuildHistoryGarbageCleanupInfo(s.domainID, s.workflowID, s.resetRunID),
		ShardID:         common.IntPtr(s.mockShard.GetShardID()),
	}).Return(&persistence.ForkHistoryBranchResponse{NewBranchToken: resetBranchToken}, nil).Times(1)

	newBranchToken, err := s.workflowResetter.generateBranchToken(
		s.domainID, s.workflowID, baseBranchToken, baseNodeID, s.resetRunID,
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
		shared.DecisionTaskFailedCauseForceCloseDecision,
		([]byte)(nil),
		identityHistoryService,
		"",
		"",
		"",
		"",
		int64(0),
	).Return(&shared.HistoryEvent{}, nil).Times(1)
	mutableState.EXPECT().FlushBufferedEvents().Return(nil).Times(1)
	mutableState.EXPECT().AddWorkflowExecutionTerminatedEvent(
		nextEventID,
		terminateReason,
		([]byte)(nil),
		identityHistoryService,
	).Return(&shared.HistoryEvent{}, nil).Times(1)

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

	baseEvent1 := &shared.HistoryEvent{
		EventId:                              common.Int64Ptr(124),
		EventType:                            shared.EventTypeDecisionTaskScheduled.Ptr(),
		DecisionTaskScheduledEventAttributes: &shared.DecisionTaskScheduledEventAttributes{},
	}
	baseEvent2 := &shared.HistoryEvent{
		EventId:                            common.Int64Ptr(125),
		EventType:                          shared.EventTypeDecisionTaskStarted.Ptr(),
		DecisionTaskStartedEventAttributes: &shared.DecisionTaskStartedEventAttributes{},
	}
	baseEvent3 := &shared.HistoryEvent{
		EventId:                              common.Int64Ptr(126),
		EventType:                            shared.EventTypeDecisionTaskCompleted.Ptr(),
		DecisionTaskCompletedEventAttributes: &shared.DecisionTaskCompletedEventAttributes{},
	}
	baseEvent4 := &shared.HistoryEvent{
		EventId:   common.Int64Ptr(127),
		EventType: shared.EventTypeWorkflowExecutionContinuedAsNew.Ptr(),
		WorkflowExecutionContinuedAsNewEventAttributes: &shared.WorkflowExecutionContinuedAsNewEventAttributes{
			NewExecutionRunId: common.StringPtr(newRunID),
		},
	}

	newEvent1 := &shared.HistoryEvent{
		EventId:                                 common.Int64Ptr(1),
		EventType:                               shared.EventTypeWorkflowExecutionStarted.Ptr(),
		WorkflowExecutionStartedEventAttributes: &shared.WorkflowExecutionStartedEventAttributes{},
	}
	newEvent2 := &shared.HistoryEvent{
		EventId:                              common.Int64Ptr(2),
		EventType:                            shared.EventTypeDecisionTaskScheduled.Ptr(),
		DecisionTaskScheduledEventAttributes: &shared.DecisionTaskScheduledEventAttributes{},
	}
	newEvent3 := &shared.HistoryEvent{
		EventId:                            common.Int64Ptr(3),
		EventType:                          shared.EventTypeDecisionTaskStarted.Ptr(),
		DecisionTaskStartedEventAttributes: &shared.DecisionTaskStartedEventAttributes{},
	}
	newEvent4 := &shared.HistoryEvent{
		EventId:                              common.Int64Ptr(4),
		EventType:                            shared.EventTypeDecisionTaskCompleted.Ptr(),
		DecisionTaskCompletedEventAttributes: &shared.DecisionTaskCompletedEventAttributes{},
	}
	newEvent5 := &shared.HistoryEvent{
		EventId:                                common.Int64Ptr(5),
		EventType:                              shared.EventTypeWorkflowExecutionFailed.Ptr(),
		WorkflowExecutionFailedEventAttributes: &shared.WorkflowExecutionFailedEventAttributes{},
	}

	baseEvents := []*shared.HistoryEvent{baseEvent1, baseEvent2, baseEvent3, baseEvent4}
	s.mockHistoryV2Mgr.On("ReadHistoryBranchByBatch", &persistence.ReadHistoryBranchRequest{
		BranchToken:   baseBranchToken,
		MinEventID:    baseFirstEventID,
		MaxEventID:    baseNextEventID,
		PageSize:      nDCDefaultPageSize,
		NextPageToken: nil,
		ShardID:       common.IntPtr(s.mockShard.GetShardID()),
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:       []*shared.History{{Events: baseEvents}},
		NextPageToken: nil,
	}, nil).Once()

	newEvents := []*shared.HistoryEvent{newEvent1, newEvent2, newEvent3, newEvent4, newEvent5}
	s.mockHistoryV2Mgr.On("ReadHistoryBranchByBatch", &persistence.ReadHistoryBranchRequest{
		BranchToken:   newBranchToken,
		MinEventID:    newFirstEventID,
		MaxEventID:    newNextEventID,
		PageSize:      nDCDefaultPageSize,
		NextPageToken: nil,
		ShardID:       common.IntPtr(s.mockShard.GetShardID()),
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:       []*shared.History{{Events: newEvents}},
		NextPageToken: nil,
	}, nil).Once()

	resetContext := NewMockworkflowExecutionContext(s.controller)
	resetContext.EXPECT().lock(gomock.Any()).Return(nil).Times(1)
	resetContext.EXPECT().unlock().Times(1)
	resetMutableState := NewMockmutableState(s.controller)
	resetContext.EXPECT().loadWorkflowExecution().Return(resetMutableState, nil).Times(1)
	resetMutableState.EXPECT().GetNextEventID().Return(newNextEventID).AnyTimes()
	resetMutableState.EXPECT().GetCurrentBranchToken().Return(newBranchToken, nil).AnyTimes()
	resetContextCacheKey := definition.NewWorkflowIdentifier(s.domainID, s.workflowID, newRunID)
	_, _ = s.workflowResetter.historyCache.PutIfNotExist(resetContextCacheKey, resetContext)

	mutableState := NewMockmutableState(s.controller)

	err := s.workflowResetter.reapplyContinueAsNewWorkflowEvents(
		ctx,
		mutableState,
		s.domainID,
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
	event1 := &shared.HistoryEvent{
		EventId:                                 common.Int64Ptr(1),
		EventType:                               shared.EventTypeWorkflowExecutionStarted.Ptr(),
		WorkflowExecutionStartedEventAttributes: &shared.WorkflowExecutionStartedEventAttributes{},
	}
	event2 := &shared.HistoryEvent{
		EventId:                              common.Int64Ptr(2),
		EventType:                            shared.EventTypeDecisionTaskScheduled.Ptr(),
		DecisionTaskScheduledEventAttributes: &shared.DecisionTaskScheduledEventAttributes{},
	}
	event3 := &shared.HistoryEvent{
		EventId:                            common.Int64Ptr(3),
		EventType:                          shared.EventTypeDecisionTaskStarted.Ptr(),
		DecisionTaskStartedEventAttributes: &shared.DecisionTaskStartedEventAttributes{},
	}
	event4 := &shared.HistoryEvent{
		EventId:                              common.Int64Ptr(4),
		EventType:                            shared.EventTypeDecisionTaskCompleted.Ptr(),
		DecisionTaskCompletedEventAttributes: &shared.DecisionTaskCompletedEventAttributes{},
	}
	event5 := &shared.HistoryEvent{
		EventId:   common.Int64Ptr(5),
		EventType: shared.EventTypeWorkflowExecutionContinuedAsNew.Ptr(),
		WorkflowExecutionContinuedAsNewEventAttributes: &shared.WorkflowExecutionContinuedAsNewEventAttributes{
			NewExecutionRunId: common.StringPtr(newRunID),
		},
	}
	events := []*shared.HistoryEvent{event1, event2, event3, event4, event5}
	s.mockHistoryV2Mgr.On("ReadHistoryBranchByBatch", &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      nDCDefaultPageSize,
		NextPageToken: nil,
		ShardID:       common.IntPtr(s.mockShard.GetShardID()),
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:       []*shared.History{{Events: events}},
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

	event1 := &shared.HistoryEvent{
		EventId:   common.Int64Ptr(101),
		EventType: shared.EventTypeWorkflowExecutionSignaled.Ptr(),
		WorkflowExecutionSignaledEventAttributes: &shared.WorkflowExecutionSignaledEventAttributes{
			SignalName: common.StringPtr("some random signal name"),
			Input:      []byte("some random signal input"),
			Identity:   common.StringPtr("some random signal identity"),
		},
	}
	event2 := &shared.HistoryEvent{
		EventId:                              common.Int64Ptr(102),
		EventType:                            shared.EventTypeDecisionTaskScheduled.Ptr(),
		DecisionTaskScheduledEventAttributes: &shared.DecisionTaskScheduledEventAttributes{},
	}
	event3 := &shared.HistoryEvent{
		EventId:   common.Int64Ptr(103),
		EventType: shared.EventTypeWorkflowExecutionSignaled.Ptr(),
		WorkflowExecutionSignaledEventAttributes: &shared.WorkflowExecutionSignaledEventAttributes{
			SignalName: common.StringPtr("another random signal name"),
			Input:      []byte("another random signal input"),
			Identity:   common.StringPtr("another random signal identity"),
		},
	}
	events := []*shared.HistoryEvent{event1, event2, event3}

	mutableState := NewMockmutableState(s.controller)

	for _, event := range events {
		if event.GetEventType() == shared.EventTypeWorkflowExecutionSignaled {
			attr := event.GetWorkflowExecutionSignaledEventAttributes()
			mutableState.EXPECT().AddWorkflowExecutionSignaled(
				attr.GetSignalName(),
				attr.GetInput(),
				attr.GetIdentity(),
			).Return(&shared.HistoryEvent{}, nil).Times(1)
		}
	}

	err := s.workflowResetter.reapplyEvents(mutableState, events)
	s.NoError(err)
}

func (s *workflowResetterSuite) TestPagination() {
	firstEventID := common.FirstEventID
	nextEventID := int64(101)
	branchToken := []byte("some random branch token")

	event1 := &shared.HistoryEvent{
		EventId:                                 common.Int64Ptr(1),
		WorkflowExecutionStartedEventAttributes: &shared.WorkflowExecutionStartedEventAttributes{},
	}
	event2 := &shared.HistoryEvent{
		EventId:                              common.Int64Ptr(2),
		DecisionTaskScheduledEventAttributes: &shared.DecisionTaskScheduledEventAttributes{},
	}
	event3 := &shared.HistoryEvent{
		EventId:                            common.Int64Ptr(3),
		DecisionTaskStartedEventAttributes: &shared.DecisionTaskStartedEventAttributes{},
	}
	event4 := &shared.HistoryEvent{
		EventId:                              common.Int64Ptr(4),
		DecisionTaskCompletedEventAttributes: &shared.DecisionTaskCompletedEventAttributes{},
	}
	event5 := &shared.HistoryEvent{
		EventId:                              common.Int64Ptr(5),
		ActivityTaskScheduledEventAttributes: &shared.ActivityTaskScheduledEventAttributes{},
	}
	history1 := []*shared.History{{[]*shared.HistoryEvent{event1, event2, event3}}}
	history2 := []*shared.History{{[]*shared.HistoryEvent{event4, event5}}}
	history := append(history1, history2...)
	pageToken := []byte("some random token")

	s.mockHistoryV2Mgr.On("ReadHistoryBranchByBatch", &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      nDCDefaultPageSize,
		NextPageToken: nil,
		ShardID:       common.IntPtr(s.mockShard.GetShardID()),
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
		ShardID:       common.IntPtr(s.mockShard.GetShardID()),
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:       history2,
		NextPageToken: nil,
		Size:          67890,
	}, nil).Once()

	paginationFn := s.workflowResetter.getPaginationFn(firstEventID, nextEventID, branchToken)
	iter := collection.NewPagingIterator(paginationFn)

	result := []*shared.History{}
	for iter.HasNext() {
		item, err := iter.Next()
		s.NoError(err)
		result = append(result, item.(*shared.History))
	}

	s.Equal(history, result)
}
