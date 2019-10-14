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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/clock"
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

		mockShard           *shardContextImpl
		mockDomainCache     *cache.DomainCacheMock
		mockClusterMetadata *mocks.ClusterMetadata
		mockHistoryV2Mgr    *mocks.HistoryV2Manager
		mockStateRebuilder  *MocknDCStateRebuilder
		logger              log.Logger

		controller         *gomock.Controller
		mockTransactionMgr *MocknDCTransactionMgr

		workflowResetter *workflowResetterImpl

		domainID     string
		workflowID   string
		baseRunID    string
		currentRunID string
		resetRunID   string
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

	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)
	s.controller = gomock.NewController(s.T())
	s.mockTransactionMgr = NewMocknDCTransactionMgr(s.controller)
	s.mockStateRebuilder = NewMocknDCStateRebuilder(s.controller)

	s.mockDomainCache = &cache.DomainCacheMock{}
	s.mockClusterMetadata = &mocks.ClusterMetadata{}
	s.mockHistoryV2Mgr = &mocks.HistoryV2Manager{}

	s.mockShard = &shardContextImpl{
		shardInfo:                 &persistence.ShardInfo{ShardID: 0, RangeID: 1, TransferAckLevel: 0},
		config:                    NewDynamicConfigForTest(),
		historyV2Mgr:              s.mockHistoryV2Mgr,
		domainCache:               s.mockDomainCache,
		clusterMetadata:           s.mockClusterMetadata,
		maxTransferSequenceNumber: 100000,
		logger:                    s.logger,
		timeSource:                clock.NewRealTimeSource(),
	}

	s.workflowResetter = newWorkflowResetter(
		s.mockShard,
		newHistoryCache(s.mockShard),
		s.mockTransactionMgr,
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
	s.mockDomainCache.AssertExpectations(s.T())
	s.mockClusterMetadata.AssertExpectations(s.T())
	s.mockHistoryV2Mgr.AssertExpectations(s.T())
	s.controller.Finish()
}

func (s *workflowResetterSuite) TestPersistToDB_CurrentTerminated() {
	currentWorkflowTerminated := true

	currentWorkflow := NewMocknDCWorkflow(s.controller)
	currentReleaseCalled := false
	currentContext := &mockWorkflowExecutionContext{}
	defer currentContext.AssertExpectations(s.T())
	currentMutableState := &mockMutableState{}
	defer currentMutableState.AssertExpectations(s.T())
	var currentReleaseFn releaseWorkflowExecutionFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().getContext().Return(currentContext).AnyTimes()
	currentWorkflow.EXPECT().getMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().getReleaseFn().Return(currentReleaseFn).AnyTimes()

	resetWorkflow := NewMocknDCWorkflow(s.controller)
	resetReleaseCalled := false
	resetContext := &mockWorkflowExecutionContext{}
	defer resetContext.AssertExpectations(s.T())
	resetMutableState := &mockMutableState{}
	defer resetMutableState.AssertExpectations(s.T())
	var targetReleaseFn releaseWorkflowExecutionFunc = func(error) { resetReleaseCalled = true }
	resetWorkflow.EXPECT().getContext().Return(resetContext).AnyTimes()
	resetWorkflow.EXPECT().getMutableState().Return(resetMutableState).AnyTimes()
	resetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

	currentContext.On("updateWorkflowExecutionWithNewAsActive",
		mock.Anything,
		resetContext,
		resetMutableState,
	).Return(nil).Once()

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
	currentContext := &mockWorkflowExecutionContext{}
	defer currentContext.AssertExpectations(s.T())
	currentMutableState := &mockMutableState{}
	defer currentMutableState.AssertExpectations(s.T())
	var currentReleaseFn releaseWorkflowExecutionFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().getContext().Return(currentContext).AnyTimes()
	currentWorkflow.EXPECT().getMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().getReleaseFn().Return(currentReleaseFn).AnyTimes()

	currentMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		RunID: s.currentRunID,
	})
	currentMutableState.On("GetLastWriteVersion").Return(currentLastWriteVersion, nil)

	resetWorkflow := NewMocknDCWorkflow(s.controller)
	resetReleaseCalled := false
	resetContext := &mockWorkflowExecutionContext{}
	defer resetContext.AssertExpectations(s.T())
	resetMutableState := &mockMutableState{}
	defer resetMutableState.AssertExpectations(s.T())
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
	resetMutableState.On("CloseTransactionAsSnapshot",
		mock.Anything,
		transactionPolicyActive,
	).Return(resetSnapshot, resetEventsSeq, nil).Once()
	resetContext.On("persistFirstWorkflowEvents", resetEventsSeq[0]).Return(resetEventsSize, nil)
	resetContext.On("createWorkflowExecution",
		resetSnapshot,
		resetEventsSize,
		mock.Anything,
		persistence.CreateWorkflowModeContinueAsNew,
		s.currentRunID,
		currentLastWriteVersion,
	).Return(nil).Once()

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
	resetMutableState := &mockMutableState{}
	defer resetMutableState.AssertExpectations(s.T())

	s.mockHistoryV2Mgr.On("ForkHistoryBranch", &persistence.ForkHistoryBranchRequest{
		ForkBranchToken: baseBranchToken,
		ForkNodeID:      baseNodeID,
		Info:            persistence.BuildHistoryGarbageCleanupInfo(s.domainID, s.workflowID, s.resetRunID),
		ShardID:         common.IntPtr(s.mockShard.GetShardID()),
	}).Return(&persistence.ForkHistoryBranchResponse{NewBranchToken: resetBranchToken}, nil).Times(1)

	s.mockHistoryV2Mgr.On("CompleteForkBranch", &persistence.CompleteForkBranchRequest{
		BranchToken: resetBranchToken,
		Success:     true,
		ShardID:     common.IntPtr(s.mockShard.GetShardID()),
	}).Return(nil).Times(1)

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

	mutableState := &mockMutableState{}
	defer mutableState.AssertExpectations(s.T())

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
	mutableState.On("GetPendingActivityInfos").Return(map[int64]*persistence.ActivityInfo{
		activity1.ScheduleID: activity1,
		activity2.ScheduleID: activity2,
	}).Times(1)

	mutableState.On("AddActivityTaskFailedEvent",
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

	s.mockHistoryV2Mgr.On("CompleteForkBranch", &persistence.CompleteForkBranchRequest{
		BranchToken: resetBranchToken,
		Success:     true,
		ShardID:     common.IntPtr(s.mockShard.GetShardID()),
	}).Return(nil).Times(1)

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
	terminateReason := "some random terminate reason"

	mutableState := &mockMutableState{}
	defer mutableState.AssertExpectations(s.T())

	mutableState.On("GetInFlightDecision").Return(decision, true).Times(1)
	mutableState.On("AddDecisionTaskFailedEvent",
		decision.ScheduleID,
		decision.StartedID,
		shared.DecisionTaskFailedCauseForceCloseDecision,
		([]byte)(nil),
		identityHistoryService,
		"",
		"",
		"",
		int64(0),
	).Return(&shared.HistoryEvent{}, nil).Times(1)
	mutableState.On("FlushBufferedEvents").Return(nil).Once()
	mutableState.On("AddWorkflowExecutionTerminatedEvent",
		terminateReason,
		([]byte)(nil),
		identityHistoryService,
	).Return(&shared.HistoryEvent{}, nil)

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

	resetWorkflow := NewMocknDCWorkflow(s.controller)
	resetReleaseCalled := false
	resetMutableState := &mockMutableState{}
	defer resetMutableState.AssertExpectations(s.T())
	var newReleaseFn releaseWorkflowExecutionFunc = func(error) { resetReleaseCalled = true }
	resetWorkflow.EXPECT().getMutableState().Return(resetMutableState).AnyTimes()
	resetWorkflow.EXPECT().getReleaseFn().Return(newReleaseFn).AnyTimes()

	resetMutableState.On("GetNextEventID").Return(newNextEventID)
	resetMutableState.On("GetCurrentBranchToken").Return(newBranchToken, nil)

	s.mockTransactionMgr.EXPECT().loadNDCWorkflow(ctx, s.domainID, s.workflowID, newRunID).Return(resetWorkflow, nil).Times(1)

	mutableState := &mockMutableState{}
	defer mutableState.AssertExpectations(s.T())

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
	s.True(resetReleaseCalled)
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

	mutableState := &mockMutableState{}
	defer mutableState.AssertExpectations(s.T())

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

	mutableState := &mockMutableState{}
	defer mutableState.AssertExpectations(s.T())

	for _, event := range events {
		if event.GetEventType() == shared.EventTypeWorkflowExecutionSignaled {
			attr := event.GetWorkflowExecutionSignaledEventAttributes()
			mutableState.On("AddWorkflowExecutionSignaled",
				attr.GetSignalName(),
				attr.GetInput(),
				attr.GetIdentity(),
			).Return(&shared.HistoryEvent{}, nil).Once()
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
