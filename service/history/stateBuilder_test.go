// Copyright (c) 2017 Uber Technologies, Inc.
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
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
)

type (
	stateBuilderSuite struct {
		suite.Suite
		logger              log.Logger
		mockExecutionMgr    *mocks.ExecutionManager
		mockShardManager    *mocks.ShardManager
		mockClusterMetadata *mocks.ClusterMetadata
		mockProducer        *mocks.KafkaProducer
		mockDomainCache     *cache.DomainCacheMock
		mockMessagingClient messaging.Client
		mockService         service.Service
		mockShard           *shardContextImpl
		mockMutableState    *mockMutableState
		mockClientBean      *client.MockClientBean
		mockEventsCache     *MockEventsCache

		sourceCluster string

		stateBuilder *stateBuilderImpl
	}
)

func TestStateBuilderSuite(t *testing.T) {
	s := new(stateBuilderSuite)
	suite.Run(t, s)
}

func (s *stateBuilderSuite) SetupSuite() {

}

func (s *stateBuilderSuite) TearDownSuite() {

}

func (s *stateBuilderSuite) SetupTest() {
	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)
	s.mockExecutionMgr = &mocks.ExecutionManager{}
	s.mockClusterMetadata = &mocks.ClusterMetadata{}
	s.mockShardManager = &mocks.ShardManager{}
	s.mockProducer = &mocks.KafkaProducer{}
	s.mockMessagingClient = mocks.NewMockMessagingClient(s.mockProducer, nil)
	s.mockDomainCache = &cache.DomainCacheMock{}
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History)
	s.mockClientBean = &client.MockClientBean{}
	s.mockService = service.NewTestService(s.mockClusterMetadata, s.mockMessagingClient, metricsClient, s.mockClientBean, nil, nil, nil)
	s.mockEventsCache = &MockEventsCache{}

	s.mockShard = &shardContextImpl{
		service:                   s.mockService,
		shardInfo:                 &persistence.ShardInfo{ShardID: 0, RangeID: 1, TransferAckLevel: 0},
		transferSequenceNumber:    1,
		executionManager:          s.mockExecutionMgr,
		shardManager:              s.mockShardManager,
		clusterMetadata:           s.mockClusterMetadata,
		maxTransferSequenceNumber: 100000,
		closeCh:                   make(chan int, 100),
		config:                    NewDynamicConfigForTest(),
		logger:                    s.logger,
		domainCache:               s.mockDomainCache,
		eventsCache:               s.mockEventsCache,
		metricsClient:             metrics.NewClient(tally.NoopScope, metrics.History),
		timeSource:                clock.NewRealTimeSource(),
	}
	s.mockMutableState = &mockMutableState{}
	s.mockMutableState.On("GetReplicationState").Return(&persistence.ReplicationState{})
	s.stateBuilder = newStateBuilder(s.mockShard, s.mockMutableState, s.logger)
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	s.mockClusterMetadata.On("IsGlobalDomainEnabled").Return(true)
	s.sourceCluster = "some random source cluster"
}

func (s *stateBuilderSuite) TearDownTest() {
	s.stateBuilder = nil
	s.mockExecutionMgr.AssertExpectations(s.T())
	s.mockShardManager.AssertExpectations(s.T())
	s.mockProducer.AssertExpectations(s.T())
	s.mockDomainCache.AssertExpectations(s.T())
	s.mockClientBean.AssertExpectations(s.T())
	s.mockEventsCache.AssertExpectations(s.T())
}

func (s *stateBuilderSuite) mockUpdateVersion(events ...*shared.HistoryEvent) {
	for _, event := range events {
		s.mockMutableState.On("UpdateReplicationStateVersion", event.GetVersion(), true).Once()
		s.mockClusterMetadata.On("ClusterNameForFailoverVersion", event.GetVersion()).Return(s.sourceCluster).Once()
		s.mockMutableState.On("UpdateReplicationStateLastEventID", event.GetVersion(), event.GetEventId()).Once()
	}
	s.mockMutableState.On("SetHistoryBuilder", newHistoryBuilderFromEvents(events, s.logger)).Once()
	// the timer task and transfer tasks are checked in each individual tests
	s.mockMutableState.On("AddTransferTasks", mock.Anything).Once()
	s.mockMutableState.On("AddTimerTasks", mock.Anything).Once()
}

func (s *stateBuilderSuite) toHistory(events ...*shared.HistoryEvent) []*shared.HistoryEvent {
	return events
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionStarted_NoCronSchedule() {
	s.applyWorkflowExecutionStartedEventTest("")
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionStarted_WithCronSchedule() {
	s.applyWorkflowExecutionStartedEventTest("* * * * *")
}

func (s *stateBuilderSuite) applyWorkflowExecutionStartedEventTest(cronSchedule string) {
	version := int64(1)
	requestID := uuid.New()
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}

	executionInfo := &persistence.WorkflowExecutionInfo{
		WorkflowTimeout: 100,
		CronSchedule:    cronSchedule,
	}

	now := time.Now()
	evenType := shared.EventTypeWorkflowExecutionStarted
	startWorkflowAttribute := &shared.WorkflowExecutionStartedEventAttributes{
		ParentWorkflowDomain: common.StringPtr(testParentDomainName),
	}

	if len(cronSchedule) > 0 {
		startWorkflowAttribute.Initiator = shared.ContinueAsNewInitiatorCronSchedule.Ptr()
		startWorkflowAttribute.FirstDecisionTaskBackoffSeconds = common.Int32Ptr(int32(backoff.GetBackoffForNextSchedule(cronSchedule, now, now).Seconds()))
	}
	event := &shared.HistoryEvent{
		Version:                                 common.Int64Ptr(version),
		EventId:                                 common.Int64Ptr(1),
		Timestamp:                               common.Int64Ptr(now.UnixNano()),
		EventType:                               &evenType,
		WorkflowExecutionStartedEventAttributes: startWorkflowAttribute,
	}

	s.mockMutableState.On("GetStartEvent").Return(event, true)
	s.mockDomainCache.On("GetDomain", testParentDomainName).Return(testGlobalParentDomainEntry, nil).Once()
	s.mockMutableState.On("ReplicateWorkflowExecutionStartedEvent",
		&testParentDomainID, execution, requestID, event).Return(nil).Once()
	s.mockUpdateVersion(event)
	s.mockMutableState.On("GetExecutionInfo").Return(executionInfo)

	s.mockMutableState.On("ClearStickyness").Once()
	s.mockMutableState.On("SetHistoryTree", testRunID).Return(nil)
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)

	expectedTimerTasksLength := 1
	timeout := now.Add(time.Duration(executionInfo.WorkflowTimeout) * time.Second)
	backoffDuration := backoff.GetBackoffForNextSchedule(cronSchedule, now, now)
	if backoffDuration != backoff.NoBackoff {
		expectedTimerTasksLength = 2
		timeout = timeout.Add(backoffDuration)
	}

	s.Equal(expectedTimerTasksLength, len(s.stateBuilder.timerTasks))
	for i := 0; i < expectedTimerTasksLength; i++ {
		switch timerTask := s.stateBuilder.timerTasks[i].(type) {
		case *persistence.WorkflowTimeoutTask:
			s.True(timerTask.VisibilityTimestamp.Equal(timeout))
		case *persistence.WorkflowBackoffTimerTask:
			s.NotEqual(backoff.NoBackoff, backoffDuration)
			s.True(timerTask.VisibilityTimestamp.Equal(now.Add(backoffDuration)))
		default:
			s.FailNow("Unexpected timer task type.")
		}
	}

	s.Equal(1, len(s.stateBuilder.transferTasks))
	s.IsType(&persistence.RecordWorkflowStartedTask{}, s.stateBuilder.transferTasks[0])

	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionTimedOut() {
	version := int64(1)
	requestID := uuid.New()
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}
	retentionDays := int32(1)

	now := time.Now()
	evenType := shared.EventTypeWorkflowExecutionTimedOut
	event := &shared.HistoryEvent{
		Version:                                  common.Int64Ptr(version),
		EventId:                                  common.Int64Ptr(130),
		Timestamp:                                common.Int64Ptr(now.UnixNano()),
		EventType:                                &evenType,
		WorkflowExecutionTimedOutEventAttributes: &shared.WorkflowExecutionTimedOutEventAttributes{},
	}

	s.mockDomainCache.On("GetDomainByID", testDomainID).Return(testGlobalDomainEntry, nil).Once()
	s.mockMutableState.On("ReplicateWorkflowExecutionTimedoutEvent", event.GetEventId(), event).Return(nil).Once()
	s.mockUpdateVersion(event)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{})

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
	s.Equal([]persistence.Task{&persistence.CloseExecutionTask{}}, s.stateBuilder.transferTasks)
	s.Equal(1, len(s.stateBuilder.timerTasks))
	timerTask, ok := s.stateBuilder.timerTasks[0].(*persistence.DeleteHistoryEventTask)
	s.True(ok)
	s.True(timerTask.VisibilityTimestamp.Equal(now.Add(time.Duration(retentionDays) * time.Hour * 24)))

	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionTerminated() {
	version := int64(1)
	requestID := uuid.New()
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}
	retentionDays := int32(1)

	now := time.Now()
	evenType := shared.EventTypeWorkflowExecutionTerminated
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		WorkflowExecutionTerminatedEventAttributes: &shared.WorkflowExecutionTerminatedEventAttributes{},
	}

	s.mockDomainCache.On("GetDomainByID", testDomainID).Return(testGlobalDomainEntry, nil).Once()
	s.mockMutableState.On("ReplicateWorkflowExecutionTerminatedEvent", event.GetEventId(), event).Return(nil).Once()
	s.mockUpdateVersion(event)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{})

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
	s.Equal([]persistence.Task{&persistence.CloseExecutionTask{}}, s.stateBuilder.transferTasks)
	s.Equal(1, len(s.stateBuilder.timerTasks))
	timerTask, ok := s.stateBuilder.timerTasks[0].(*persistence.DeleteHistoryEventTask)
	s.True(ok)
	s.True(timerTask.VisibilityTimestamp.Equal(now.Add(time.Duration(retentionDays) * time.Hour * 24)))

	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionSignaled() {
	version := int64(1)
	requestID := uuid.New()

	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeWorkflowExecutionSignaled
	event := &shared.HistoryEvent{
		Version:                                  common.Int64Ptr(version),
		EventId:                                  common.Int64Ptr(130),
		Timestamp:                                common.Int64Ptr(now.UnixNano()),
		EventType:                                &evenType,
		WorkflowExecutionSignaledEventAttributes: &shared.WorkflowExecutionSignaledEventAttributes{},
	}
	s.mockUpdateVersion(event)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{})
	s.mockMutableState.On("ReplicateWorkflowExecutionSignaled", event).Return(nil).Once()

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionFailed() {
	version := int64(1)
	requestID := uuid.New()
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}
	retentionDays := int32(1)

	now := time.Now()
	evenType := shared.EventTypeWorkflowExecutionFailed
	event := &shared.HistoryEvent{
		Version:                                common.Int64Ptr(version),
		EventId:                                common.Int64Ptr(130),
		Timestamp:                              common.Int64Ptr(now.UnixNano()),
		EventType:                              &evenType,
		WorkflowExecutionFailedEventAttributes: &shared.WorkflowExecutionFailedEventAttributes{},
	}

	s.mockDomainCache.On("GetDomainByID", testDomainID).Return(testGlobalDomainEntry, nil).Once()
	s.mockMutableState.On("ReplicateWorkflowExecutionFailedEvent", event.GetEventId(), event).Return(nil).Once()
	s.mockUpdateVersion(event)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{})

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
	s.Equal([]persistence.Task{&persistence.CloseExecutionTask{}}, s.stateBuilder.transferTasks)
	s.Equal(1, len(s.stateBuilder.timerTasks))
	timerTask, ok := s.stateBuilder.timerTasks[0].(*persistence.DeleteHistoryEventTask)
	s.True(ok)
	s.True(timerTask.VisibilityTimestamp.Equal(now.Add(time.Duration(retentionDays) * time.Hour * 24)))

	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionContinuedAsNew_EventsV2() {
	version := int64(1)
	requestID := uuid.New()
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}
	parentWorkflowID := "some random parent workflow ID"
	parentRunID := uuid.New()
	parentInitiatedEventID := int64(144)
	retentionDays := int32(1)

	now := time.Now()
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeoutSecond := int32(110)
	decisionTimeoutSecond := int32(11)
	newRunID := uuid.New()

	continueAsNewEvent := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: shared.EventTypeWorkflowExecutionContinuedAsNew.Ptr(),
		WorkflowExecutionContinuedAsNewEventAttributes: &shared.WorkflowExecutionContinuedAsNewEventAttributes{
			NewExecutionRunId: common.StringPtr(newRunID),
		},
	}

	newRunStartedEvent := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(1),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: shared.EventTypeWorkflowExecutionStarted.Ptr(),
		WorkflowExecutionStartedEventAttributes: &shared.WorkflowExecutionStartedEventAttributes{
			ParentWorkflowDomain: common.StringPtr(testParentDomainName),
			ParentWorkflowExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(parentWorkflowID),
				RunId:      common.StringPtr(parentRunID),
			},
			ParentInitiatedEventId:              common.Int64Ptr(parentInitiatedEventID),
			ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(workflowTimeoutSecond),
			TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(decisionTimeoutSecond),
			TaskList:                            &shared.TaskList{Name: common.StringPtr(tasklist)},
			WorkflowType:                        &shared.WorkflowType{Name: common.StringPtr(workflowType)},
		},
	}

	newRunSignalEvent := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(2),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: shared.EventTypeWorkflowExecutionSignaled.Ptr(),
		WorkflowExecutionSignaledEventAttributes: &shared.WorkflowExecutionSignaledEventAttributes{
			SignalName: common.StringPtr("some random signal name"),
			Input:      []byte("some random signal input"),
			Identity:   common.StringPtr("some random identity"),
		},
	}

	newRunDecisionAttempt := int64(123)
	newRunDecisionEvent := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(3),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: shared.EventTypeDecisionTaskScheduled.Ptr(),
		DecisionTaskScheduledEventAttributes: &shared.DecisionTaskScheduledEventAttributes{
			TaskList:                   &shared.TaskList{Name: common.StringPtr(tasklist)},
			StartToCloseTimeoutSeconds: common.Int32Ptr(decisionTimeoutSecond),
			Attempt:                    common.Int64Ptr(newRunDecisionAttempt),
		},
	}
	s.mockDomainCache.On("GetDomainByID", testDomainID).Return(testGlobalDomainEntry, nil).Once()
	s.mockDomainCache.On("GetDomain", testParentDomainName).Return(testGlobalParentDomainEntry, nil).Once()
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", continueAsNewEvent.GetVersion()).Return(s.sourceCluster)
	s.mockMutableState.On("ReplicateWorkflowExecutionContinuedAsNewEvent",
		continueAsNewEvent.GetEventId(),
		testDomainID,
		continueAsNewEvent,
	).Return(nil)
	s.mockMutableState.On("GetDomainEntry").Return(testGlobalDomainEntry)
	s.mockUpdateVersion(continueAsNewEvent)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{})

	newRunHistory := &shared.History{Events: []*shared.HistoryEvent{newRunStartedEvent, newRunSignalEvent, newRunDecisionEvent}}
	s.mockMutableState.On("ClearStickyness").Once()
	s.mockEventsCache.On("putEvent", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return().Twice()

	_, _, newRunStateBuilder, err := s.stateBuilder.applyEvents(
		testDomainID, requestID, execution,
		s.toHistory(continueAsNewEvent), newRunHistory.Events, false,
	)
	s.Nil(err)
	expectedNewRunStateBuilder := newMutableStateBuilderWithReplicationState(
		s.mockShard,
		s.mockShard.GetEventsCache(),
		s.logger,
		s.mockMutableState.GetDomainEntry(),
	)
	err = expectedNewRunStateBuilder.ReplicateWorkflowExecutionStartedEvent(
		common.StringPtr(testParentDomainID),
		shared.WorkflowExecution{
			WorkflowId: execution.WorkflowId,
			RunId:      common.StringPtr(newRunID),
		},
		newRunStateBuilder.GetExecutionInfo().CreateRequestID,
		newRunStartedEvent,
	)
	s.Nil(err)
	err = expectedNewRunStateBuilder.ReplicateWorkflowExecutionSignaled(newRunSignalEvent)
	s.Nil(err)
	_, err = expectedNewRunStateBuilder.ReplicateDecisionTaskScheduledEvent(
		newRunDecisionEvent.GetVersion(),
		newRunDecisionEvent.GetEventId(),
		tasklist,
		decisionTimeoutSecond,
		newRunDecisionAttempt,
		newRunDecisionEvent.GetTimestamp(),
		newRunDecisionEvent.GetTimestamp(),
	)
	s.Nil(err)
	expectedNewRunStateBuilder.GetExecutionInfo().LastFirstEventID = newRunStartedEvent.GetEventId()
	expectedNewRunStateBuilder.GetExecutionInfo().NextEventID = newRunDecisionEvent.GetEventId() + 1
	expectedNewRunStateBuilder.GetExecutionInfo().BranchToken = newRunStateBuilder.GetExecutionInfo().BranchToken
	expectedNewRunStateBuilder.SetHistoryBuilder(newHistoryBuilderFromEvents(newRunHistory.Events, s.logger))
	expectedNewRunStateBuilder.UpdateReplicationStateLastEventID(newRunStartedEvent.GetVersion(), newRunDecisionEvent.GetEventId())

	expectedNewRunStateBuilder.nextEventIDInDB = newRunStateBuilder.(*mutableStateBuilder).nextEventIDInDB
	expectedNewRunStateBuilder.insertTransferTasks = newRunStateBuilder.(*mutableStateBuilder).insertTransferTasks
	expectedNewRunStateBuilder.insertTimerTasks = newRunStateBuilder.(*mutableStateBuilder).insertTimerTasks
	expectedNewRunStateBuilder.replicationState.StartVersion = version
	expectedNewRunStateBuilder.replicationState.CurrentVersion = version
	expectedNewRunStateBuilder.replicationState.LastWriteVersion = version
	s.Equal(expectedNewRunStateBuilder, newRunStateBuilder)

	s.Equal([]persistence.Task{&persistence.CloseExecutionTask{}}, s.stateBuilder.transferTasks)
	s.Equal(1, len(s.stateBuilder.timerTasks))
	timerTask, ok := s.stateBuilder.timerTasks[0].(*persistence.DeleteHistoryEventTask)
	s.True(ok)
	s.True(timerTask.VisibilityTimestamp.Equal(now.Add(time.Duration(retentionDays) * time.Hour * 24)))

	s.Equal(1, len(s.stateBuilder.newRunTimerTasks))
	newRunTimerTask, ok := s.stateBuilder.newRunTimerTasks[0].(*persistence.WorkflowTimeoutTask)
	s.True(ok)
	s.True(newRunTimerTask.VisibilityTimestamp.Equal(now.Add(time.Duration(workflowTimeoutSecond) * time.Second)))
	s.Equal([]persistence.Task{
		&persistence.RecordWorkflowStartedTask{},
		&persistence.DecisionTask{
			DomainID:   testDomainID,
			TaskList:   tasklist,
			ScheduleID: newRunDecisionEvent.GetEventId(),
		},
	}, s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionCompleted() {
	version := int64(1)
	requestID := uuid.New()
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}
	retentionDays := int32(1)

	now := time.Now()
	evenType := shared.EventTypeWorkflowExecutionCompleted
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		WorkflowExecutionCompletedEventAttributes: &shared.WorkflowExecutionCompletedEventAttributes{},
	}

	s.mockDomainCache.On("GetDomainByID", testDomainID).Return(testGlobalDomainEntry, nil).Once()
	s.mockMutableState.On("ReplicateWorkflowExecutionCompletedEvent", event.GetEventId(), event).Return(nil).Once()
	s.mockUpdateVersion(event)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{})

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
	s.Equal([]persistence.Task{&persistence.CloseExecutionTask{}}, s.stateBuilder.transferTasks)
	s.Equal(1, len(s.stateBuilder.timerTasks))
	timerTask, ok := s.stateBuilder.timerTasks[0].(*persistence.DeleteHistoryEventTask)
	s.True(ok)
	s.True(timerTask.VisibilityTimestamp.Equal(now.Add(time.Duration(retentionDays) * time.Hour * 24)))

	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionCanceled() {
	version := int64(1)
	requestID := uuid.New()
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}
	retentionDays := int32(1)

	now := time.Now()
	evenType := shared.EventTypeWorkflowExecutionCanceled
	event := &shared.HistoryEvent{
		Version:                                  common.Int64Ptr(version),
		EventId:                                  common.Int64Ptr(130),
		Timestamp:                                common.Int64Ptr(now.UnixNano()),
		EventType:                                &evenType,
		WorkflowExecutionCanceledEventAttributes: &shared.WorkflowExecutionCanceledEventAttributes{},
	}

	s.mockDomainCache.On("GetDomainByID", testDomainID).Return(testGlobalDomainEntry, nil).Once()
	s.mockMutableState.On("ReplicateWorkflowExecutionCanceledEvent", event.GetEventId(), event).Return(nil).Once()
	s.mockUpdateVersion(event)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{})

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
	s.Equal([]persistence.Task{&persistence.CloseExecutionTask{}}, s.stateBuilder.transferTasks)
	s.Equal(1, len(s.stateBuilder.timerTasks))
	timerTask, ok := s.stateBuilder.timerTasks[0].(*persistence.DeleteHistoryEventTask)
	s.True(ok)
	s.True(timerTask.VisibilityTimestamp.Equal(now.Add(time.Duration(retentionDays) * time.Hour * 24)))

	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionCancelRequested() {
	version := int64(1)
	requestID := uuid.New()

	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}
	now := time.Now()
	evenType := shared.EventTypeWorkflowExecutionCancelRequested
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		WorkflowExecutionCancelRequestedEventAttributes: &shared.WorkflowExecutionCancelRequestedEventAttributes{},
	}

	s.mockMutableState.On("ReplicateWorkflowExecutionCancelRequestedEvent", event).Return(nil).Once()
	s.mockUpdateVersion(event)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{})

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeTimerStarted() {
	version := int64(1)
	requestID := uuid.New()

	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}

	now := time.Now()
	timerID := "timer ID"
	timeoutSecond := int64(10)
	evenType := shared.EventTypeTimerStarted
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		TimerStartedEventAttributes: &shared.TimerStartedEventAttributes{
			TimerId:                   common.StringPtr(timerID),
			StartToFireTimeoutSeconds: common.Int64Ptr(timeoutSecond),
		},
	}
	ti := &persistence.TimerInfo{
		Version:    event.GetVersion(),
		TimerID:    timerID,
		ExpiryTime: time.Unix(0, event.GetTimestamp()).Add(time.Duration(timeoutSecond) * time.Second),
		StartedID:  event.GetEventId(),
		TaskID:     TimerTaskStatusNone,
	}
	s.mockMutableState.On("GetPendingTimerInfos").Return(map[string]*persistence.TimerInfo{timerID: ti}).Once()
	s.mockMutableState.On("UpdateUserTimer", ti.TimerID, ti).Once()
	s.mockMutableState.On("ReplicateTimerStartedEvent", event).Return(ti, nil).Once()
	s.mockUpdateVersion(event)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{})

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
	s.Equal(1, len(s.stateBuilder.timerTasks))
	timerTask, ok := s.stateBuilder.timerTasks[0].(*persistence.UserTimerTask)
	s.True(ok)
	s.True(timerTask.VisibilityTimestamp.Equal(ti.ExpiryTime))
	s.Equal(ti.StartedID, timerTask.EventID)

	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeTimerFired() {
	version := int64(1)
	requestID := uuid.New()

	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeTimerFired
	event := &shared.HistoryEvent{
		Version:                   common.Int64Ptr(version),
		EventId:                   common.Int64Ptr(130),
		Timestamp:                 common.Int64Ptr(now.UnixNano()),
		EventType:                 &evenType,
		TimerFiredEventAttributes: &shared.TimerFiredEventAttributes{},
	}

	// this is the remaining timer
	// should create a user timer for this
	timeoutSecond := int32(20)
	ti := &persistence.TimerInfo{
		Version:    version,
		TimerID:    "some random timer ID",
		ExpiryTime: now.Add(time.Duration(timeoutSecond) * time.Second),
		StartedID:  144,
		TaskID:     TimerTaskStatusNone,
	}
	s.mockMutableState.On("GetPendingTimerInfos").Return(map[string]*persistence.TimerInfo{ti.TimerID: ti}).Once()
	s.mockMutableState.On("UpdateUserTimer", ti.TimerID, ti).Once()
	s.mockMutableState.On("ReplicateTimerFiredEvent", event).Return(nil).Once()
	s.mockUpdateVersion(event)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{})

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
	s.Equal(1, len(s.stateBuilder.timerTasks))
	timerTask, ok := s.stateBuilder.timerTasks[0].(*persistence.UserTimerTask)
	s.True(ok)
	s.True(timerTask.VisibilityTimestamp.Equal(ti.ExpiryTime))
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeTimerCanceled() {
	version := int64(1)
	requestID := uuid.New()

	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}

	now := time.Now()

	// this is a timer which already got created and will be used to generate a new concrete timer
	timerID := "timer ID"
	timeoutSecond := int64(10)
	ti := &persistence.TimerInfo{
		Version:    version,
		TimerID:    timerID,
		ExpiryTime: time.Unix(0, now.UnixNano()).Add(time.Duration(timeoutSecond) * time.Second),
		StartedID:  111,
		TaskID:     TimerTaskStatusNone,
	}
	s.mockMutableState.On("GetPendingTimerInfos").Return(map[string]*persistence.TimerInfo{timerID: ti}).Once()
	s.mockMutableState.On("UpdateUserTimer", ti.TimerID, ti).Once()

	evenType := shared.EventTypeTimerCanceled
	event := &shared.HistoryEvent{
		Version:                      common.Int64Ptr(version),
		EventId:                      common.Int64Ptr(130),
		Timestamp:                    common.Int64Ptr(now.UnixNano()),
		EventType:                    &evenType,
		TimerCanceledEventAttributes: &shared.TimerCanceledEventAttributes{},
	}
	s.mockMutableState.On("ReplicateTimerCanceledEvent", event).Return(nil).Once()
	s.mockUpdateVersion(event)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{})

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)

	s.Equal(1, len(s.stateBuilder.timerTasks))
	timerTask, ok := s.stateBuilder.timerTasks[0].(*persistence.UserTimerTask)
	s.True(ok)
	s.True(timerTask.VisibilityTimestamp.Equal(ti.ExpiryTime))
	s.Equal(ti.StartedID, timerTask.EventID)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeStartChildWorkflowExecutionInitiated() {
	version := int64(1)
	requestID := uuid.New()

	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}
	targetWorkflowID := "some random target workflow ID"

	now := time.Now()
	createRequestID := uuid.New()
	evenType := shared.EventTypeStartChildWorkflowExecutionInitiated
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		StartChildWorkflowExecutionInitiatedEventAttributes: &shared.StartChildWorkflowExecutionInitiatedEventAttributes{
			Domain:     common.StringPtr(testTargetDomainName),
			WorkflowId: common.StringPtr(targetWorkflowID),
		},
	}

	ci := &persistence.ChildExecutionInfo{
		Version:               event.GetVersion(),
		InitiatedID:           event.GetEventId(),
		InitiatedEventBatchID: event.GetEventId(),
		StartedID:             common.EmptyEventID,
		CreateRequestID:       createRequestID,
		DomainName:            testTargetDomainName,
	}

	// the create request ID is generated inside, cannot assert equal
	s.mockMutableState.On("ReplicateStartChildWorkflowExecutionInitiatedEvent", event.GetEventId(), event,
		mock.Anything).Return(ci, nil).Once()
	s.mockDomainCache.On("GetDomain", testTargetDomainName).Return(testGlobalTargetDomainEntry, nil).Once()
	s.mockUpdateVersion(event)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{})

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
	s.Equal([]persistence.Task{&persistence.StartChildExecutionTask{
		TargetDomainID:   testTargetDomainID,
		TargetWorkflowID: targetWorkflowID,
		InitiatedID:      event.GetEventId(),
	}}, s.stateBuilder.transferTasks)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeStartChildWorkflowExecutionFailed() {
	version := int64(1)
	requestID := uuid.New()

	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeStartChildWorkflowExecutionFailed
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		StartChildWorkflowExecutionFailedEventAttributes: &shared.StartChildWorkflowExecutionFailedEventAttributes{},
	}
	s.mockMutableState.On("ReplicateStartChildWorkflowExecutionFailedEvent", event).Return(nil).Once()
	s.mockUpdateVersion(event)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{})

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeSignalExternalWorkflowExecutionInitiated() {
	version := int64(1)
	requestID := uuid.New()
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}
	targetWorkflowID := "some random target workflow ID"
	targetRunID := uuid.New()
	childWorkflowOnly := true

	now := time.Now()
	signalRequestID := uuid.New()
	signalName := "some random signal name"
	signalInput := []byte("some random signal input")
	control := []byte("some random control")
	evenType := shared.EventTypeSignalExternalWorkflowExecutionInitiated
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		SignalExternalWorkflowExecutionInitiatedEventAttributes: &shared.SignalExternalWorkflowExecutionInitiatedEventAttributes{
			Domain: common.StringPtr(testTargetDomainName),
			WorkflowExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(targetWorkflowID),
				RunId:      common.StringPtr(targetRunID),
			},
			SignalName:        common.StringPtr(signalName),
			Input:             signalInput,
			ChildWorkflowOnly: common.BoolPtr(childWorkflowOnly),
		},
	}
	si := &persistence.SignalInfo{
		Version:         event.GetVersion(),
		InitiatedID:     event.GetEventId(),
		SignalRequestID: signalRequestID,
		SignalName:      signalName,
		Input:           signalInput,
		Control:         control,
	}

	// the cancellation request ID is generated inside, cannot assert equal
	s.mockMutableState.On("ReplicateSignalExternalWorkflowExecutionInitiatedEvent", event.GetEventId(), event, mock.Anything).Return(si, nil).Once()
	s.mockDomainCache.On("GetDomain", testTargetDomainName).Return(testGlobalTargetDomainEntry, nil).Once()
	s.mockUpdateVersion(event)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{})

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)

	s.Equal([]persistence.Task{&persistence.SignalExecutionTask{
		TargetDomainID:          testTargetDomainID,
		TargetWorkflowID:        targetWorkflowID,
		TargetRunID:             targetRunID,
		TargetChildWorkflowOnly: childWorkflowOnly,
		InitiatedID:             event.GetEventId(),
	}}, s.stateBuilder.transferTasks)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeSignalExternalWorkflowExecutionFailed() {
	version := int64(1)
	requestID := uuid.New()

	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeSignalExternalWorkflowExecutionFailed
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		SignalExternalWorkflowExecutionFailedEventAttributes: &shared.SignalExternalWorkflowExecutionFailedEventAttributes{},
	}
	s.mockMutableState.On("ReplicateSignalExternalWorkflowExecutionFailedEvent", event).Return(nil).Once()
	s.mockUpdateVersion(event)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{})

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeRequestCancelExternalWorkflowExecutionInitiated() {
	version := int64(1)
	requestID := uuid.New()

	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}

	targetWorkflowID := "some random target workflow ID"
	targetRunID := uuid.New()
	childWorkflowOnly := true

	now := time.Now()
	cancellationRequestID := uuid.New()
	control := []byte("some random control")
	evenType := shared.EventTypeRequestCancelExternalWorkflowExecutionInitiated
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		RequestCancelExternalWorkflowExecutionInitiatedEventAttributes: &shared.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
			Domain: common.StringPtr(testTargetDomainName),
			WorkflowExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(targetWorkflowID),
				RunId:      common.StringPtr(targetRunID),
			},
			ChildWorkflowOnly: common.BoolPtr(childWorkflowOnly),
			Control:           control,
		},
	}
	rci := &persistence.RequestCancelInfo{
		Version:         event.GetVersion(),
		InitiatedID:     event.GetEventId(),
		CancelRequestID: cancellationRequestID,
	}

	// the cancellation request ID is generated inside, cannot assert equal
	s.mockMutableState.On("ReplicateRequestCancelExternalWorkflowExecutionInitiatedEvent", event.GetEventId(), event, mock.Anything).Return(rci, nil).Once()
	s.mockDomainCache.On("GetDomain", testTargetDomainName).Return(testGlobalTargetDomainEntry, nil).Once()
	s.mockUpdateVersion(event)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{})

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)

	s.Equal([]persistence.Task{&persistence.CancelExecutionTask{
		TargetDomainID:          testTargetDomainID,
		TargetWorkflowID:        targetWorkflowID,
		TargetRunID:             targetRunID,
		TargetChildWorkflowOnly: childWorkflowOnly,
		InitiatedID:             event.GetEventId(),
	}}, s.stateBuilder.transferTasks)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeRequestCancelExternalWorkflowExecutionFailed() {
	version := int64(1)
	requestID := uuid.New()

	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeRequestCancelExternalWorkflowExecutionFailed
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		RequestCancelActivityTaskFailedEventAttributes: &shared.RequestCancelActivityTaskFailedEventAttributes{},
	}
	s.mockMutableState.On("ReplicateRequestCancelExternalWorkflowExecutionFailedEvent", event).Return(nil).Once()
	s.mockUpdateVersion(event)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{})

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeRequestCancelActivityTaskFailed() {
	version := int64(1)
	requestID := uuid.New()

	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeRequestCancelActivityTaskFailed
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		RequestCancelActivityTaskFailedEventAttributes: &shared.RequestCancelActivityTaskFailedEventAttributes{},
	}
	s.mockUpdateVersion(event)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{})

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeMarkerRecorded() {
	version := int64(1)
	requestID := uuid.New()

	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeMarkerRecorded
	event := &shared.HistoryEvent{
		Version:                       common.Int64Ptr(version),
		EventId:                       common.Int64Ptr(130),
		Timestamp:                     common.Int64Ptr(now.UnixNano()),
		EventType:                     &evenType,
		MarkerRecordedEventAttributes: &shared.MarkerRecordedEventAttributes{},
	}
	s.mockUpdateVersion(event)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{})

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeExternalWorkflowExecutionSignaled() {
	version := int64(1)
	requestID := uuid.New()

	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeExternalWorkflowExecutionSignaled
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		ExternalWorkflowExecutionSignaledEventAttributes: &shared.ExternalWorkflowExecutionSignaledEventAttributes{},
	}
	s.mockMutableState.On("ReplicateExternalWorkflowExecutionSignaled", event).Return(nil).Once()
	s.mockUpdateVersion(event)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{})

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeExternalWorkflowExecutionCancelRequested() {
	version := int64(1)
	requestID := uuid.New()

	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeExternalWorkflowExecutionCancelRequested
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		ExternalWorkflowExecutionCancelRequestedEventAttributes: &shared.ExternalWorkflowExecutionCancelRequestedEventAttributes{},
	}
	s.mockMutableState.On("ReplicateExternalWorkflowExecutionCancelRequested", event).Return(nil).Once()
	s.mockUpdateVersion(event)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{})

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeDecisionTaskTimedOut() {
	version := int64(1)
	requestID := uuid.New()

	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}

	now := time.Now()
	scheduleID := int64(12)
	startedID := int64(28)
	evenType := shared.EventTypeDecisionTaskTimedOut
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		DecisionTaskTimedOutEventAttributes: &shared.DecisionTaskTimedOutEventAttributes{
			ScheduledEventId: common.Int64Ptr(scheduleID),
			StartedEventId:   common.Int64Ptr(startedID),
			TimeoutType:      shared.TimeoutTypeStartToClose.Ptr(),
		},
	}
	s.mockMutableState.On("ReplicateDecisionTaskTimedOutEvent", shared.TimeoutTypeStartToClose).Return(nil).Once()
	tasklist := "some random tasklist"
	newScheduleID := int64(233)
	executionInfo := &persistence.WorkflowExecutionInfo{
		TaskList: tasklist,
	}
	s.mockMutableState.On("GetExecutionInfo").Return(executionInfo)
	s.mockMutableState.On("ReplicateTransientDecisionTaskScheduled").Return(&decisionInfo{
		Version:    version,
		ScheduleID: newScheduleID,
		TaskList:   tasklist,
	}, nil)
	s.mockUpdateVersion(event)

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)

	s.Equal([]persistence.Task{&persistence.DecisionTask{
		DomainID:   testDomainID,
		TaskList:   tasklist,
		ScheduleID: newScheduleID,
	}}, s.stateBuilder.transferTasks)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeDecisionTaskStarted() {
	version := int64(1)
	requestID := uuid.New()

	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}

	now := time.Now()
	tasklist := "some random tasklist"
	timeoutSecond := int32(11)
	scheduleID := int64(111)
	decisionRequestID := uuid.New()
	evenType := shared.EventTypeDecisionTaskStarted
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		DecisionTaskStartedEventAttributes: &shared.DecisionTaskStartedEventAttributes{
			ScheduledEventId: common.Int64Ptr(scheduleID),
			RequestId:        common.StringPtr(decisionRequestID),
		},
	}
	di := &decisionInfo{
		Version:         event.GetVersion(),
		ScheduleID:      scheduleID,
		StartedID:       event.GetEventId(),
		RequestID:       decisionRequestID,
		DecisionTimeout: timeoutSecond,
		TaskList:        tasklist,
		Attempt:         0,
	}
	s.mockMutableState.On("ReplicateDecisionTaskStartedEvent",
		(*decisionInfo)(nil), event.GetVersion(), scheduleID, event.GetEventId(), decisionRequestID, event.GetTimestamp()).Return(di, nil).Once()
	s.mockMutableState.On("UpdateDecision", di).Once()
	s.mockUpdateVersion(event)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{})

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)

	s.Equal(1, len(s.stateBuilder.timerTasks))
	timerTask, ok := s.stateBuilder.timerTasks[0].(*persistence.DecisionTimeoutTask)
	s.True(ok)
	s.True(timerTask.VisibilityTimestamp.Equal(now.Add(time.Duration(timeoutSecond) * time.Second)))
	s.Equal(di.ScheduleID, timerTask.EventID)

	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeDecisionTaskScheduled() {
	version := int64(1)
	requestID := uuid.New()

	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}

	now := time.Now()
	tasklist := "some random tasklist"
	timeoutSecond := int32(11)
	evenType := shared.EventTypeDecisionTaskScheduled
	decisionAttempt := int64(111)
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		DecisionTaskScheduledEventAttributes: &shared.DecisionTaskScheduledEventAttributes{
			TaskList:                   &shared.TaskList{Name: common.StringPtr(tasklist)},
			StartToCloseTimeoutSeconds: common.Int32Ptr(timeoutSecond),
			Attempt:                    common.Int64Ptr(decisionAttempt),
		},
	}
	di := &decisionInfo{
		Version:         event.GetVersion(),
		ScheduleID:      event.GetEventId(),
		StartedID:       common.EmptyEventID,
		RequestID:       emptyUUID,
		DecisionTimeout: timeoutSecond,
		TaskList:        tasklist,
		Attempt:         decisionAttempt,
	}
	executionInfo := &persistence.WorkflowExecutionInfo{
		TaskList: tasklist,
	}
	s.mockMutableState.On("GetExecutionInfo").Return(executionInfo)
	s.mockMutableState.On("ReplicateDecisionTaskScheduledEvent",
		event.GetVersion(), event.GetEventId(), tasklist, timeoutSecond, decisionAttempt, event.GetTimestamp(), event.GetTimestamp(),
	).Return(di, nil).Once()
	s.mockMutableState.On("UpdateDecision", di).Once()
	s.mockUpdateVersion(event)

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)

	s.Equal([]persistence.Task{&persistence.DecisionTask{
		DomainID:   testDomainID,
		TaskList:   tasklist,
		ScheduleID: event.GetEventId(),
	}}, s.stateBuilder.transferTasks)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeDecisionTaskFailed() {
	version := int64(1)
	requestID := uuid.New()

	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}

	now := time.Now()
	scheduleID := int64(12)
	startedID := int64(28)
	evenType := shared.EventTypeDecisionTaskFailed
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		DecisionTaskFailedEventAttributes: &shared.DecisionTaskFailedEventAttributes{
			ScheduledEventId: common.Int64Ptr(scheduleID),
			StartedEventId:   common.Int64Ptr(startedID),
		},
	}
	s.mockMutableState.On("ReplicateDecisionTaskFailedEvent").Return(nil).Once()
	tasklist := "some random tasklist"
	newScheduleID := int64(233)
	executionInfo := &persistence.WorkflowExecutionInfo{
		TaskList: tasklist,
	}
	s.mockMutableState.On("GetExecutionInfo").Return(executionInfo)
	s.mockMutableState.On("ReplicateTransientDecisionTaskScheduled").Return(&decisionInfo{
		Version:    version,
		ScheduleID: newScheduleID,
		TaskList:   tasklist,
	}, nil)
	s.mockUpdateVersion(event)

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)

	s.Equal([]persistence.Task{&persistence.DecisionTask{
		DomainID:   testDomainID,
		TaskList:   tasklist,
		ScheduleID: newScheduleID,
	}}, s.stateBuilder.transferTasks)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeDecisionTaskCompleted() {
	version := int64(1)
	requestID := uuid.New()

	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}

	now := time.Now()
	scheduleID := int64(12)
	startedID := int64(28)
	evenType := shared.EventTypeDecisionTaskCompleted
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		DecisionTaskCompletedEventAttributes: &shared.DecisionTaskCompletedEventAttributes{
			ScheduledEventId: common.Int64Ptr(scheduleID),
			StartedEventId:   common.Int64Ptr(startedID),
		},
	}
	s.mockMutableState.On("ReplicateDecisionTaskCompletedEvent", event).Return(nil).Once()
	s.mockUpdateVersion(event)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{})

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionTimedOut() {
	version := int64(1)
	requestID := uuid.New()

	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeChildWorkflowExecutionTimedOut
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		ChildWorkflowExecutionTimedOutEventAttributes: &shared.ChildWorkflowExecutionTimedOutEventAttributes{},
	}
	s.mockMutableState.On("ReplicateChildWorkflowExecutionTimedOutEvent", event).Return(nil).Once()
	s.mockUpdateVersion(event)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{})

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionTerminated() {
	version := int64(1)
	requestID := uuid.New()

	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeChildWorkflowExecutionTerminated
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		ChildWorkflowExecutionTerminatedEventAttributes: &shared.ChildWorkflowExecutionTerminatedEventAttributes{},
	}
	s.mockMutableState.On("ReplicateChildWorkflowExecutionTerminatedEvent", event).Return(nil).Once()
	s.mockUpdateVersion(event)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{})

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionStarted() {
	version := int64(1)
	requestID := uuid.New()

	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeChildWorkflowExecutionStarted
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		ChildWorkflowExecutionStartedEventAttributes: &shared.ChildWorkflowExecutionStartedEventAttributes{},
	}
	s.mockMutableState.On("ReplicateChildWorkflowExecutionStartedEvent", event).Return(nil).Once()
	s.mockUpdateVersion(event)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{})

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionFailed() {
	version := int64(1)
	requestID := uuid.New()

	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeChildWorkflowExecutionFailed
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		ChildWorkflowExecutionFailedEventAttributes: &shared.ChildWorkflowExecutionFailedEventAttributes{},
	}
	s.mockMutableState.On("ReplicateChildWorkflowExecutionFailedEvent", event).Return(nil).Once()
	s.mockUpdateVersion(event)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{})

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionCompleted() {
	version := int64(1)
	requestID := uuid.New()

	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeChildWorkflowExecutionCompleted
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		ChildWorkflowExecutionCompletedEventAttributes: &shared.ChildWorkflowExecutionCompletedEventAttributes{},
	}
	s.mockMutableState.On("ReplicateChildWorkflowExecutionCompletedEvent", event).Return(nil).Once()
	s.mockUpdateVersion(event)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{})

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionCanceled() {
	version := int64(1)
	requestID := uuid.New()

	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeChildWorkflowExecutionCanceled
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		ChildWorkflowExecutionCanceledEventAttributes: &shared.ChildWorkflowExecutionCanceledEventAttributes{},
	}
	s.mockMutableState.On("ReplicateChildWorkflowExecutionCanceledEvent", event).Return(nil).Once()
	s.mockUpdateVersion(event)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{})

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeCancelTimerFailed() {
	version := int64(1)
	requestID := uuid.New()

	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeCancelTimerFailed
	event := &shared.HistoryEvent{
		Version:                          common.Int64Ptr(version),
		EventId:                          common.Int64Ptr(130),
		Timestamp:                        common.Int64Ptr(now.UnixNano()),
		EventType:                        &evenType,
		CancelTimerFailedEventAttributes: &shared.CancelTimerFailedEventAttributes{},
	}
	s.mockUpdateVersion(event)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{})

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskTimedOut() {
	version := int64(1)
	requestID := uuid.New()

	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeActivityTaskTimedOut
	event := &shared.HistoryEvent{
		Version:                             common.Int64Ptr(version),
		EventId:                             common.Int64Ptr(130),
		Timestamp:                           common.Int64Ptr(now.UnixNano()),
		EventType:                           &evenType,
		ActivityTaskTimedOutEventAttributes: &shared.ActivityTaskTimedOutEventAttributes{},
	}

	// this is the remaining activity
	// should create a activity timer for this
	timeoutSecond := int32(10)
	ai := &persistence.ActivityInfo{
		Version:                  version,
		ScheduleID:               123,
		ScheduledEvent:           nil,
		ScheduledTime:            now,
		StartedID:                124,
		StartedEvent:             nil,
		StartedTime:              now,
		ActivityID:               "some random activity ID",
		ScheduleToStartTimeout:   timeoutSecond,
		ScheduleToCloseTimeout:   timeoutSecond,
		StartToCloseTimeout:      timeoutSecond,
		HeartbeatTimeout:         timeoutSecond,
		CancelRequested:          false,
		CancelRequestID:          common.EmptyEventID,
		LastHeartBeatUpdatedTime: time.Time{},
		TimerTaskStatus:          TimerTaskStatusNone,
		TaskList:                 "some random tasklist",
	}
	s.mockMutableState.On("GetPendingActivityInfos").Return(map[int64]*persistence.ActivityInfo{ai.ScheduleID: ai})
	s.mockMutableState.On("UpdateActivity", ai).Return(nil).Once()
	s.mockMutableState.On("ReplicateActivityTaskTimedOutEvent", event).Return(nil).Once()
	s.mockUpdateVersion(event)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{})

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
	s.Equal(1, len(s.stateBuilder.timerTasks))
	timerTask, ok := s.stateBuilder.timerTasks[0].(*persistence.ActivityTimeoutTask)
	s.True(ok)
	s.True(timerTask.VisibilityTimestamp.Equal(now.Add(time.Duration(timeoutSecond) * time.Second)))
	s.Equal(ai.ScheduleID, timerTask.EventID)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskStarted() {
	version := int64(1)
	requestID := uuid.New()

	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}

	now := time.Now()
	activityID := "activity ID"
	tasklist := "some random tasklist"
	timeoutSecond := int32(10)
	evenType := shared.EventTypeActivityTaskScheduled
	scheduledEvent := &shared.HistoryEvent{
		Version:                              common.Int64Ptr(version),
		EventId:                              common.Int64Ptr(130),
		Timestamp:                            common.Int64Ptr(now.UnixNano()),
		EventType:                            &evenType,
		ActivityTaskScheduledEventAttributes: &shared.ActivityTaskScheduledEventAttributes{},
	}

	evenType = shared.EventTypeActivityTaskStarted
	startedEvent := &shared.HistoryEvent{
		Version:                            common.Int64Ptr(version),
		EventId:                            common.Int64Ptr(scheduledEvent.GetEventId() + 1),
		Timestamp:                          common.Int64Ptr(scheduledEvent.GetTimestamp() + 1000),
		EventType:                          &evenType,
		ActivityTaskStartedEventAttributes: &shared.ActivityTaskStartedEventAttributes{},
	}

	ai := &persistence.ActivityInfo{
		Version:                  version,
		ScheduleID:               scheduledEvent.GetEventId(),
		ScheduledEvent:           scheduledEvent,
		ScheduledTime:            time.Unix(0, scheduledEvent.GetTimestamp()),
		StartedID:                startedEvent.GetEventId(),
		StartedEvent:             startedEvent,
		StartedTime:              time.Unix(0, startedEvent.GetTimestamp()),
		ActivityID:               activityID,
		ScheduleToStartTimeout:   timeoutSecond,
		ScheduleToCloseTimeout:   timeoutSecond,
		StartToCloseTimeout:      timeoutSecond,
		HeartbeatTimeout:         timeoutSecond,
		CancelRequested:          false,
		CancelRequestID:          common.EmptyEventID,
		LastHeartBeatUpdatedTime: time.Time{},
		TimerTaskStatus:          TimerTaskStatusNone,
		TaskList:                 tasklist,
	}
	executionInfo := &persistence.WorkflowExecutionInfo{
		TaskList: tasklist,
	}
	s.mockMutableState.On("GetExecutionInfo").Return(executionInfo)
	s.mockMutableState.On("GetPendingActivityInfos").Return(map[int64]*persistence.ActivityInfo{scheduledEvent.GetEventId(): ai})
	s.mockMutableState.On("UpdateActivity", ai).Return(nil).Once()
	s.mockMutableState.On("ReplicateActivityTaskStartedEvent", startedEvent).Return(nil).Once()
	s.mockUpdateVersion(startedEvent)

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(startedEvent), nil, false)
	s.Nil(err)
	s.Equal(1, len(s.stateBuilder.timerTasks))
	timerTask, ok := s.stateBuilder.timerTasks[0].(*persistence.ActivityTimeoutTask)
	s.True(ok)
	s.True(timerTask.VisibilityTimestamp.Equal(now.Add(time.Duration(timeoutSecond) * time.Second)))
	s.Equal(ai.ScheduleID, timerTask.EventID)
	s.Empty(s.stateBuilder.transferTasks)

	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskScheduled() {
	version := int64(1)
	requestID := uuid.New()

	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}

	now := time.Now()
	activityID := "activity ID"
	tasklist := "some random tasklist"
	timeoutSecond := int32(10)
	evenType := shared.EventTypeActivityTaskScheduled
	event := &shared.HistoryEvent{
		Version:                              common.Int64Ptr(version),
		EventId:                              common.Int64Ptr(130),
		Timestamp:                            common.Int64Ptr(now.UnixNano()),
		EventType:                            &evenType,
		ActivityTaskScheduledEventAttributes: &shared.ActivityTaskScheduledEventAttributes{},
	}

	ai := &persistence.ActivityInfo{
		Version:                  event.GetVersion(),
		ScheduleID:               event.GetEventId(),
		ScheduledEventBatchID:    event.GetEventId(),
		ScheduledEvent:           event,
		ScheduledTime:            time.Unix(0, event.GetTimestamp()),
		StartedID:                common.EmptyEventID,
		StartedTime:              time.Time{},
		ActivityID:               activityID,
		ScheduleToStartTimeout:   timeoutSecond,
		ScheduleToCloseTimeout:   timeoutSecond,
		StartToCloseTimeout:      timeoutSecond,
		HeartbeatTimeout:         timeoutSecond,
		CancelRequested:          false,
		CancelRequestID:          common.EmptyEventID,
		LastHeartBeatUpdatedTime: time.Time{},
		TimerTaskStatus:          TimerTaskStatusNone,
		TaskList:                 tasklist,
	}
	executionInfo := &persistence.WorkflowExecutionInfo{
		TaskList: tasklist,
	}
	s.mockMutableState.On("GetExecutionInfo").Return(executionInfo)
	s.mockMutableState.On("GetPendingActivityInfos").Return(map[int64]*persistence.ActivityInfo{event.GetEventId(): ai})
	s.mockMutableState.On("UpdateActivity", ai).Return(nil).Once()
	s.mockMutableState.On("ReplicateActivityTaskScheduledEvent", event.GetEventId(), event).Return(ai, nil).Once()
	s.mockUpdateVersion(event)

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
	s.Equal(1, len(s.stateBuilder.timerTasks))
	timerTask, ok := s.stateBuilder.timerTasks[0].(*persistence.ActivityTimeoutTask)
	s.True(ok)
	s.True(timerTask.VisibilityTimestamp.Equal(now.Add(time.Duration(timeoutSecond) * time.Second)))
	s.Equal(ai.ScheduleID, timerTask.EventID)
	s.Equal([]persistence.Task{&persistence.ActivityTask{
		DomainID:   testDomainID,
		TaskList:   tasklist,
		ScheduleID: event.GetEventId(),
	}}, s.stateBuilder.transferTasks)

	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskFailed() {
	version := int64(1)
	requestID := uuid.New()

	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeActivityTaskFailed
	event := &shared.HistoryEvent{
		Version:                           common.Int64Ptr(version),
		EventId:                           common.Int64Ptr(130),
		Timestamp:                         common.Int64Ptr(now.UnixNano()),
		EventType:                         &evenType,
		ActivityTaskFailedEventAttributes: &shared.ActivityTaskFailedEventAttributes{},
	}

	// this is the remaining activity
	// should create a activity timer for this
	timeoutSecond := int32(10)
	ai := &persistence.ActivityInfo{
		Version:                  version,
		ScheduleID:               123,
		ScheduledEvent:           nil,
		ScheduledTime:            now,
		StartedID:                124,
		StartedEvent:             nil,
		StartedTime:              now,
		ActivityID:               "some random activity ID",
		ScheduleToStartTimeout:   timeoutSecond,
		ScheduleToCloseTimeout:   timeoutSecond,
		StartToCloseTimeout:      timeoutSecond,
		HeartbeatTimeout:         timeoutSecond,
		CancelRequested:          false,
		CancelRequestID:          common.EmptyEventID,
		LastHeartBeatUpdatedTime: time.Time{},
		TimerTaskStatus:          TimerTaskStatusNone,
		TaskList:                 "some random tasklist",
	}
	s.mockMutableState.On("GetPendingActivityInfos").Return(map[int64]*persistence.ActivityInfo{ai.ScheduleID: ai})
	s.mockMutableState.On("UpdateActivity", ai).Return(nil).Once()
	s.mockMutableState.On("ReplicateActivityTaskFailedEvent", event).Return(nil).Once()
	s.mockUpdateVersion(event)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{})

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
	s.Equal(1, len(s.stateBuilder.timerTasks))
	timerTask, ok := s.stateBuilder.timerTasks[0].(*persistence.ActivityTimeoutTask)
	s.True(ok)
	s.True(timerTask.VisibilityTimestamp.Equal(now.Add(time.Duration(timeoutSecond) * time.Second)))
	s.Equal(ai.ScheduleID, timerTask.EventID)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskCompleted() {
	version := int64(1)
	requestID := uuid.New()

	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeActivityTaskCompleted
	event := &shared.HistoryEvent{
		Version:                              common.Int64Ptr(version),
		EventId:                              common.Int64Ptr(130),
		Timestamp:                            common.Int64Ptr(now.UnixNano()),
		EventType:                            &evenType,
		ActivityTaskCompletedEventAttributes: &shared.ActivityTaskCompletedEventAttributes{},
	}

	// this is the remaining activity
	// should create a activity timer for this
	timeoutSecond := int32(10)
	ai := &persistence.ActivityInfo{
		Version:                  version,
		ScheduleID:               123,
		ScheduledEvent:           nil,
		ScheduledTime:            now,
		StartedID:                124,
		StartedEvent:             nil,
		StartedTime:              now,
		ActivityID:               "some random activity ID",
		ScheduleToStartTimeout:   timeoutSecond,
		ScheduleToCloseTimeout:   timeoutSecond,
		StartToCloseTimeout:      timeoutSecond,
		HeartbeatTimeout:         timeoutSecond,
		CancelRequested:          false,
		CancelRequestID:          common.EmptyEventID,
		LastHeartBeatUpdatedTime: time.Time{},
		TimerTaskStatus:          TimerTaskStatusNone,
		TaskList:                 "some random tasklist",
	}
	s.mockMutableState.On("GetPendingActivityInfos").Return(map[int64]*persistence.ActivityInfo{ai.ScheduleID: ai})
	s.mockMutableState.On("UpdateActivity", ai).Return(nil).Once()
	s.mockMutableState.On("ReplicateActivityTaskCompletedEvent", event).Return(nil).Once()
	s.mockUpdateVersion(event)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{})

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
	s.Equal(1, len(s.stateBuilder.timerTasks))
	timerTask, ok := s.stateBuilder.timerTasks[0].(*persistence.ActivityTimeoutTask)
	s.True(ok)
	s.True(timerTask.VisibilityTimestamp.Equal(now.Add(time.Duration(timeoutSecond) * time.Second)))
	s.Equal(ai.ScheduleID, timerTask.EventID)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskCanceled() {
	version := int64(1)
	requestID := uuid.New()

	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeActivityTaskCanceled
	event := &shared.HistoryEvent{
		Version:                             common.Int64Ptr(version),
		EventId:                             common.Int64Ptr(130),
		Timestamp:                           common.Int64Ptr(now.UnixNano()),
		EventType:                           &evenType,
		ActivityTaskCanceledEventAttributes: &shared.ActivityTaskCanceledEventAttributes{},
	}

	// this is the remaining activity
	// should create a activity timer for this
	timeoutSecond := int32(10)
	ai := &persistence.ActivityInfo{
		Version:                  version,
		ScheduleID:               123,
		ScheduledEvent:           nil,
		ScheduledTime:            now,
		StartedID:                124,
		StartedEvent:             nil,
		StartedTime:              now,
		ActivityID:               "some random activity ID",
		ScheduleToStartTimeout:   timeoutSecond,
		ScheduleToCloseTimeout:   timeoutSecond,
		StartToCloseTimeout:      timeoutSecond,
		HeartbeatTimeout:         timeoutSecond,
		CancelRequested:          false,
		CancelRequestID:          common.EmptyEventID,
		LastHeartBeatUpdatedTime: time.Time{},
		TimerTaskStatus:          TimerTaskStatusNone,
		TaskList:                 "some random tasklist",
	}
	s.mockMutableState.On("GetPendingActivityInfos").Return(map[int64]*persistence.ActivityInfo{ai.ScheduleID: ai})
	s.mockMutableState.On("UpdateActivity", ai).Return(nil).Once()
	s.mockMutableState.On("ReplicateActivityTaskCanceledEvent", event).Return(nil).Once()
	s.mockUpdateVersion(event)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{})

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
	s.Equal(1, len(s.stateBuilder.timerTasks))
	timerTask, ok := s.stateBuilder.timerTasks[0].(*persistence.ActivityTimeoutTask)
	s.True(ok)
	s.True(timerTask.VisibilityTimestamp.Equal(now.Add(time.Duration(timeoutSecond) * time.Second)))
	s.Equal(ai.ScheduleID, timerTask.EventID)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskCancelRequested() {
	version := int64(1)
	requestID := uuid.New()

	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeActivityTaskCancelRequested
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		ActivityTaskCancelRequestedEventAttributes: &shared.ActivityTaskCancelRequestedEventAttributes{},
	}
	s.mockMutableState.On("ReplicateActivityTaskCancelRequestedEvent", event).Return(nil).Once()
	s.mockUpdateVersion(event)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{})

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeUpsertWorkflowSearchAttributes() {
	version := int64(1)
	requestID := uuid.New()

	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeUpsertWorkflowSearchAttributes
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		UpsertWorkflowSearchAttributesEventAttributes: &shared.UpsertWorkflowSearchAttributesEventAttributes{},
	}
	s.mockMutableState.On("ReplicateUpsertWorkflowSearchAttributesEvent", event).Return(nil).Once()
	s.mockUpdateVersion(event)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{})

	s.mockMutableState.On("ClearStickyness").Once()
	_, _, _, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)

	s.Empty(s.stateBuilder.timerTasks)
	s.Equal(1, len(s.stateBuilder.transferTasks))
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEventsNewEventsNotHandled() {
	eventTypes := shared.EventType_Values()
	s.Equal(42, len(eventTypes), "If you see this error, you are adding new event type. "+
		"Before updating the number to make this test pass, please make sure you update stateBuilderImpl.applyEvents method "+
		"to handle the new decision type. Otherwise cross dc will not work on the new event.")
}
