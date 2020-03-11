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

	"github.com/gogo/protobuf/types"
	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"

	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/backoff"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/primitives"
)

type (
	stateBuilderSuite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		mockShard           *shardContextTest
		mockEventsCache     *MockeventsCache
		mockDomainCache     *cache.MockDomainCache
		mockTaskGenerator   *MockmutableStateTaskGenerator
		mockMutableState    *MockmutableState
		mockClusterMetadata *cluster.MockMetadata

		mockTaskGeneratorForNew *MockmutableStateTaskGenerator

		logger log.Logger

		sourceCluster string
		stateBuilder  *stateBuilderImpl
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
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockTaskGenerator = NewMockmutableStateTaskGenerator(s.controller)
	s.mockMutableState = NewMockmutableState(s.controller)
	s.mockTaskGeneratorForNew = NewMockmutableStateTaskGenerator(s.controller)

	s.mockShard = newTestShardContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistenceblobs.ShardInfo{
				ShardID:          0,
				RangeID:          1,
				TransferAckLevel: 0,
			}},
		NewDynamicConfigForTest(),
	)

	s.mockDomainCache = s.mockShard.resource.DomainCache
	s.mockClusterMetadata = s.mockShard.resource.ClusterMetadata
	s.mockEventsCache = s.mockShard.mockEventsCache
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsGlobalDomainEnabled().Return(true).AnyTimes()
	s.mockEventsCache.EXPECT().putEvent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

	s.logger = s.mockShard.GetLogger()

	s.mockMutableState.EXPECT().GetReplicationState().Return(&persistence.ReplicationState{}).AnyTimes()
	s.stateBuilder = newStateBuilder(
		s.mockShard,
		s.logger,
		s.mockMutableState,
		func(mutableState mutableState) mutableStateTaskGenerator {
			if mutableState == s.mockMutableState {
				return s.mockTaskGenerator
			}
			return s.mockTaskGeneratorForNew
		},
	)
	s.sourceCluster = "some random source cluster"
}

func (s *stateBuilderSuite) TearDownTest() {
	s.stateBuilder = nil
	s.controller.Finish()
	s.mockShard.Finish(s.T())
}

func (s *stateBuilderSuite) mockUpdateVersion(events ...*commonproto.HistoryEvent) {
	for _, event := range events {
		s.mockMutableState.EXPECT().UpdateReplicationStateVersion(event.GetVersion(), true).Times(1)
		s.mockMutableState.EXPECT().UpdateReplicationStateLastEventID(event.GetVersion(), event.GetEventId()).Times(1)
	}
	s.mockTaskGenerator.EXPECT().generateActivityTimerTasks(
		s.stateBuilder.unixNanoToTime(events[len(events)-1].GetTimestamp()),
	).Return(nil).Times(1)
	s.mockTaskGenerator.EXPECT().generateUserTimerTasks(
		s.stateBuilder.unixNanoToTime(events[len(events)-1].GetTimestamp()),
	).Return(nil).Times(1)
	s.mockMutableState.EXPECT().SetHistoryBuilder(newHistoryBuilderFromEvents(events, s.logger)).Times(1)
}

func (s *stateBuilderSuite) toHistory(events ...*commonproto.HistoryEvent) []*commonproto.HistoryEvent {
	return events
}

// workflow operations

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionStarted_NoCronSchedule() {
	cronSchedule := ""
	version := int64(1)
	requestID := uuid.New()
	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	executionInfo := &persistence.WorkflowExecutionInfo{
		WorkflowTimeout: 100,
		CronSchedule:    cronSchedule,
	}

	now := time.Now()
	evenType := enums.EventTypeWorkflowExecutionStarted
	startWorkflowAttribute := &commonproto.WorkflowExecutionStartedEventAttributes{
		ParentWorkflowDomain: testParentDomainName,
	}

	event := &commonproto.HistoryEvent{
		Version:    version,
		EventId:    1,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &commonproto.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: startWorkflowAttribute},
	}

	s.mockDomainCache.EXPECT().GetDomain(testParentDomainName).Return(testGlobalParentDomainEntry, nil).Times(1)
	s.mockMutableState.EXPECT().ReplicateWorkflowExecutionStartedEvent(testParentDomainID, execution, requestID, event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	s.mockTaskGenerator.EXPECT().generateRecordWorkflowStartedTasks(
		s.stateBuilder.unixNanoToTime(event.GetTimestamp()),
		event,
	).Return(nil).Times(1)
	s.mockTaskGenerator.EXPECT().generateWorkflowStartTasks(
		s.stateBuilder.unixNanoToTime(event.GetTimestamp()),
		event,
	).Return(nil).Times(1)
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)
	s.mockMutableState.EXPECT().SetHistoryTree([]byte(primitives.MustParseUUID(testRunID))).Return(nil).Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionStarted_WithCronSchedule() {
	cronSchedule := "* * * * *"
	version := int64(1)
	requestID := uuid.New()
	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	executionInfo := &persistence.WorkflowExecutionInfo{
		WorkflowTimeout: 100,
		CronSchedule:    cronSchedule,
	}

	now := time.Now()
	evenType := enums.EventTypeWorkflowExecutionStarted
	startWorkflowAttribute := &commonproto.WorkflowExecutionStartedEventAttributes{
		ParentWorkflowDomain:            testParentDomainName,
		Initiator:                       enums.ContinueAsNewInitiatorCronSchedule,
		FirstDecisionTaskBackoffSeconds: int32(backoff.GetBackoffForNextSchedule(cronSchedule, now, now).Seconds()),
	}

	event := &commonproto.HistoryEvent{
		Version:    version,
		EventId:    1,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &commonproto.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: startWorkflowAttribute},
	}

	s.mockDomainCache.EXPECT().GetDomain(testParentDomainName).Return(testGlobalParentDomainEntry, nil).Times(1)
	s.mockMutableState.EXPECT().ReplicateWorkflowExecutionStartedEvent(testParentDomainID, execution, requestID, event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	s.mockTaskGenerator.EXPECT().generateRecordWorkflowStartedTasks(
		s.stateBuilder.unixNanoToTime(event.GetTimestamp()),
		event,
	).Return(nil).Times(1)
	s.mockTaskGenerator.EXPECT().generateWorkflowStartTasks(
		s.stateBuilder.unixNanoToTime(event.GetTimestamp()),
		event,
	).Return(nil).Times(1)
	s.mockTaskGenerator.EXPECT().generateDelayedDecisionTasks(
		s.stateBuilder.unixNanoToTime(event.GetTimestamp()),
		event,
	).Return(nil).Times(1)
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)
	s.mockMutableState.EXPECT().SetHistoryTree([]byte(primitives.MustParseUUID(testRunID))).Return(nil).Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionTimedOut() {
	version := int64(1)
	requestID := uuid.New()
	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := enums.EventTypeWorkflowExecutionTimedOut
	event := &commonproto.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &commonproto.HistoryEvent_WorkflowExecutionTimedOutEventAttributes{WorkflowExecutionTimedOutEventAttributes: &commonproto.WorkflowExecutionTimedOutEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ReplicateWorkflowExecutionTimedoutEvent(event.GetEventId(), event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockTaskGenerator.EXPECT().generateWorkflowCloseTasks(
		s.stateBuilder.unixNanoToTime(event.GetTimestamp()),
	).Return(nil).Times(1)
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionTerminated() {
	version := int64(1)
	requestID := uuid.New()
	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := enums.EventTypeWorkflowExecutionTerminated
	event := &commonproto.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &commonproto.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{WorkflowExecutionTerminatedEventAttributes: &commonproto.WorkflowExecutionTerminatedEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ReplicateWorkflowExecutionTerminatedEvent(event.GetEventId(), event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockTaskGenerator.EXPECT().generateWorkflowCloseTasks(
		s.stateBuilder.unixNanoToTime(event.GetTimestamp()),
	).Return(nil).Times(1)
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)
	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionFailed() {
	version := int64(1)
	requestID := uuid.New()
	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := enums.EventTypeWorkflowExecutionFailed
	event := &commonproto.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &commonproto.HistoryEvent_WorkflowExecutionFailedEventAttributes{WorkflowExecutionFailedEventAttributes: &commonproto.WorkflowExecutionFailedEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ReplicateWorkflowExecutionFailedEvent(event.GetEventId(), event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockTaskGenerator.EXPECT().generateWorkflowCloseTasks(
		s.stateBuilder.unixNanoToTime(event.GetTimestamp()),
	).Return(nil).Times(1)
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionCompleted() {
	version := int64(1)
	requestID := uuid.New()
	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := enums.EventTypeWorkflowExecutionCompleted
	event := &commonproto.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &commonproto.HistoryEvent_WorkflowExecutionCompletedEventAttributes{WorkflowExecutionCompletedEventAttributes: &commonproto.WorkflowExecutionCompletedEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ReplicateWorkflowExecutionCompletedEvent(event.GetEventId(), event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockTaskGenerator.EXPECT().generateWorkflowCloseTasks(
		s.stateBuilder.unixNanoToTime(event.GetTimestamp()),
	).Return(nil).Times(1)
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionCanceled() {
	version := int64(1)
	requestID := uuid.New()
	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := enums.EventTypeWorkflowExecutionCanceled
	event := &commonproto.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &commonproto.HistoryEvent_WorkflowExecutionCanceledEventAttributes{WorkflowExecutionCanceledEventAttributes: &commonproto.WorkflowExecutionCanceledEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ReplicateWorkflowExecutionCanceledEvent(event.GetEventId(), event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockTaskGenerator.EXPECT().generateWorkflowCloseTasks(
		s.stateBuilder.unixNanoToTime(event.GetTimestamp()),
	).Return(nil).Times(1)
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionContinuedAsNew() {
	version := int64(1)
	requestID := uuid.New()
	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}
	parentWorkflowID := "some random parent workflow ID"
	parentRunID := uuid.New()
	parentInitiatedEventID := int64(144)

	now := time.Now()
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeoutSecond := int32(110)
	decisionTimeoutSecond := int32(11)
	newRunID := uuid.New()

	continueAsNewEvent := &commonproto.HistoryEvent{
		Version:   version,
		EventId:   130,
		Timestamp: now.UnixNano(),
		EventType: enums.EventTypeWorkflowExecutionContinuedAsNew,
		Attributes: &commonproto.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{WorkflowExecutionContinuedAsNewEventAttributes: &commonproto.WorkflowExecutionContinuedAsNewEventAttributes{
			NewExecutionRunId: newRunID,
		}},
	}

	newRunStartedEvent := &commonproto.HistoryEvent{
		Version:   version,
		EventId:   1,
		Timestamp: now.UnixNano(),
		EventType: enums.EventTypeWorkflowExecutionStarted,
		Attributes: &commonproto.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &commonproto.WorkflowExecutionStartedEventAttributes{
			ParentWorkflowDomain: testParentDomainName,
			ParentWorkflowExecution: &commonproto.WorkflowExecution{
				WorkflowId: parentWorkflowID,
				RunId:      parentRunID,
			},
			ParentInitiatedEventId:              parentInitiatedEventID,
			ExecutionStartToCloseTimeoutSeconds: workflowTimeoutSecond,
			TaskStartToCloseTimeoutSeconds:      decisionTimeoutSecond,
			TaskList:                            &commonproto.TaskList{Name: tasklist},
			WorkflowType:                        &commonproto.WorkflowType{Name: workflowType},
		}},
	}

	newRunSignalEvent := &commonproto.HistoryEvent{
		Version:   version,
		EventId:   2,
		Timestamp: now.UnixNano(),
		EventType: enums.EventTypeWorkflowExecutionSignaled,
		Attributes: &commonproto.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &commonproto.WorkflowExecutionSignaledEventAttributes{
			SignalName: "some random signal name",
			Input:      []byte("some random signal input"),
			Identity:   "some random identity",
		}},
	}

	newRunDecisionAttempt := int64(123)
	newRunDecisionEvent := &commonproto.HistoryEvent{
		Version:   version,
		EventId:   3,
		Timestamp: now.UnixNano(),
		EventType: enums.EventTypeDecisionTaskScheduled,
		Attributes: &commonproto.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &commonproto.DecisionTaskScheduledEventAttributes{
			TaskList:                   &commonproto.TaskList{Name: tasklist},
			StartToCloseTimeoutSeconds: decisionTimeoutSecond,
			Attempt:                    newRunDecisionAttempt,
		}},
	}
	newRunEvents := []*commonproto.HistoryEvent{
		newRunStartedEvent, newRunSignalEvent, newRunDecisionEvent,
	}

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(continueAsNewEvent.GetVersion()).Return(s.sourceCluster).AnyTimes()
	s.mockMutableState.EXPECT().ReplicateWorkflowExecutionContinuedAsNewEvent(
		continueAsNewEvent.GetEventId(),
		testDomainID,
		continueAsNewEvent,
	).Return(nil).Times(1)
	s.mockMutableState.EXPECT().GetDomainEntry().Return(testGlobalDomainEntry).AnyTimes()
	s.mockUpdateVersion(continueAsNewEvent)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockTaskGenerator.EXPECT().generateWorkflowCloseTasks(
		s.stateBuilder.unixNanoToTime(continueAsNewEvent.GetTimestamp()),
	).Return(nil).Times(1)
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	// new workflow domain
	s.mockDomainCache.EXPECT().GetDomain(testParentDomainName).Return(testGlobalParentDomainEntry, nil).AnyTimes()
	// task for the new workflow
	s.mockTaskGeneratorForNew.EXPECT().generateRecordWorkflowStartedTasks(
		s.stateBuilder.unixNanoToTime(newRunStartedEvent.GetTimestamp()),
		newRunStartedEvent,
	).Return(nil).Times(1)
	s.mockTaskGeneratorForNew.EXPECT().generateWorkflowStartTasks(
		s.stateBuilder.unixNanoToTime(newRunStartedEvent.GetTimestamp()),
		newRunStartedEvent,
	).Return(nil).Times(1)
	s.mockTaskGeneratorForNew.EXPECT().generateDecisionScheduleTasks(
		s.stateBuilder.unixNanoToTime(newRunDecisionEvent.GetTimestamp()),
		newRunDecisionEvent.GetEventId(),
	).Return(nil).Times(1)
	s.mockTaskGeneratorForNew.EXPECT().generateActivityTimerTasks(
		s.stateBuilder.unixNanoToTime(newRunEvents[len(newRunEvents)-1].GetTimestamp()),
	).Return(nil).Times(1)
	s.mockTaskGeneratorForNew.EXPECT().generateUserTimerTasks(
		s.stateBuilder.unixNanoToTime(newRunEvents[len(newRunEvents)-1].GetTimestamp()),
	).Return(nil).Times(1)

	newRunStateBuilder, err := s.stateBuilder.applyEvents(
		testDomainID, requestID, execution, s.toHistory(continueAsNewEvent), newRunEvents, false,
	)
	s.Nil(err)
	s.NotNil(newRunStateBuilder)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionContinuedAsNew_EmptyNewRunHistory() {
	version := int64(1)
	requestID := uuid.New()
	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	newRunID := uuid.New()

	continueAsNewEvent := &commonproto.HistoryEvent{
		Version:   version,
		EventId:   130,
		Timestamp: now.UnixNano(),
		EventType: enums.EventTypeWorkflowExecutionContinuedAsNew,
		Attributes: &commonproto.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{WorkflowExecutionContinuedAsNewEventAttributes: &commonproto.WorkflowExecutionContinuedAsNewEventAttributes{
			NewExecutionRunId: newRunID,
		}},
	}

	s.mockMutableState.EXPECT().ReplicateWorkflowExecutionContinuedAsNewEvent(
		continueAsNewEvent.GetEventId(),
		testDomainID,
		continueAsNewEvent,
	).Return(nil).Times(1)
	s.mockMutableState.EXPECT().GetDomainEntry().Return(testGlobalDomainEntry).AnyTimes()
	s.mockUpdateVersion(continueAsNewEvent)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockTaskGenerator.EXPECT().generateWorkflowCloseTasks(
		s.stateBuilder.unixNanoToTime(continueAsNewEvent.GetTimestamp()),
	).Return(nil).Times(1)
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	// new workflow domain
	s.mockDomainCache.EXPECT().GetDomain(testParentDomainName).Return(testGlobalParentDomainEntry, nil).AnyTimes()
	newRunStateBuilder, err := s.stateBuilder.applyEvents(
		testDomainID, requestID, execution, s.toHistory(continueAsNewEvent), nil, false,
	)
	s.Nil(err)
	s.Nil(newRunStateBuilder)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionSignaled() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := enums.EventTypeWorkflowExecutionSignaled
	event := &commonproto.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &commonproto.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &commonproto.WorkflowExecutionSignaledEventAttributes{}},
	}
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockMutableState.EXPECT().ReplicateWorkflowExecutionSignaled(event).Return(nil).Times(1)
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionCancelRequested() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}
	now := time.Now()
	evenType := enums.EventTypeWorkflowExecutionCancelRequested
	event := &commonproto.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &commonproto.HistoryEvent_WorkflowExecutionCancelRequestedEventAttributes{WorkflowExecutionCancelRequestedEventAttributes: &commonproto.WorkflowExecutionCancelRequestedEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ReplicateWorkflowExecutionCancelRequestedEvent(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeUpsertWorkflowSearchAttributes() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := enums.EventTypeUpsertWorkflowSearchAttributes
	event := &commonproto.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &commonproto.HistoryEvent_UpsertWorkflowSearchAttributesEventAttributes{UpsertWorkflowSearchAttributesEventAttributes: &commonproto.UpsertWorkflowSearchAttributesEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateUpsertWorkflowSearchAttributesEvent(event).Return().Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockTaskGenerator.EXPECT().generateWorkflowSearchAttrTasks(
		s.stateBuilder.unixNanoToTime(event.GetTimestamp()),
	).Return(nil).Times(1)
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeMarkerRecorded() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := enums.EventTypeMarkerRecorded
	event := &commonproto.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &commonproto.HistoryEvent_MarkerRecordedEventAttributes{MarkerRecordedEventAttributes: &commonproto.MarkerRecordedEventAttributes{}},
	}
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

// decision operations

func (s *stateBuilderSuite) TestApplyEvents_EventTypeDecisionTaskScheduled() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	tasklist := "some random tasklist"
	timeoutSecond := int32(11)
	evenType := enums.EventTypeDecisionTaskScheduled
	decisionAttempt := int64(111)
	event := &commonproto.HistoryEvent{
		Version:   version,
		EventId:   130,
		Timestamp: now.UnixNano(),
		EventType: evenType,
		Attributes: &commonproto.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &commonproto.DecisionTaskScheduledEventAttributes{
			TaskList:                   &commonproto.TaskList{Name: tasklist},
			StartToCloseTimeoutSeconds: timeoutSecond,
			Attempt:                    decisionAttempt,
		}},
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
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	s.mockMutableState.EXPECT().ReplicateDecisionTaskScheduledEvent(
		event.GetVersion(), event.GetEventId(), tasklist, timeoutSecond, decisionAttempt, event.GetTimestamp(), event.GetTimestamp(),
	).Return(di, nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().generateDecisionScheduleTasks(
		s.stateBuilder.unixNanoToTime(event.GetTimestamp()),
		di.ScheduleID,
	).Return(nil).Times(1)
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}
func (s *stateBuilderSuite) TestApplyEvents_EventTypeDecisionTaskStarted() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	tasklist := "some random tasklist"
	timeoutSecond := int32(11)
	scheduleID := int64(111)
	decisionRequestID := uuid.New()
	evenType := enums.EventTypeDecisionTaskStarted
	event := &commonproto.HistoryEvent{
		Version:   version,
		EventId:   130,
		Timestamp: now.UnixNano(),
		EventType: evenType,
		Attributes: &commonproto.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &commonproto.DecisionTaskStartedEventAttributes{
			ScheduledEventId: scheduleID,
			RequestId:        decisionRequestID,
		}},
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
	s.mockMutableState.EXPECT().ReplicateDecisionTaskStartedEvent(
		(*decisionInfo)(nil), event.GetVersion(), scheduleID, event.GetEventId(), decisionRequestID, event.GetTimestamp(),
	).Return(di, nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockTaskGenerator.EXPECT().generateDecisionStartTasks(
		s.stateBuilder.unixNanoToTime(event.GetTimestamp()),
		di.ScheduleID,
	).Return(nil).Times(1)
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeDecisionTaskTimedOut() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	scheduleID := int64(12)
	startedID := int64(28)
	evenType := enums.EventTypeDecisionTaskTimedOut
	event := &commonproto.HistoryEvent{
		Version:   version,
		EventId:   130,
		Timestamp: now.UnixNano(),
		EventType: evenType,
		Attributes: &commonproto.HistoryEvent_DecisionTaskTimedOutEventAttributes{DecisionTaskTimedOutEventAttributes: &commonproto.DecisionTaskTimedOutEventAttributes{
			ScheduledEventId: scheduleID,
			StartedEventId:   startedID,
			TimeoutType:      enums.TimeoutTypeStartToClose,
		}},
	}
	s.mockMutableState.EXPECT().ReplicateDecisionTaskTimedOutEvent(enums.TimeoutTypeStartToClose).Return(nil).Times(1)
	tasklist := "some random tasklist"
	newScheduleID := int64(233)
	executionInfo := &persistence.WorkflowExecutionInfo{
		TaskList: tasklist,
	}
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	s.mockMutableState.EXPECT().ReplicateTransientDecisionTaskScheduled().Return(&decisionInfo{
		Version:    version,
		ScheduleID: newScheduleID,
		TaskList:   tasklist,
	}, nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().generateDecisionScheduleTasks(
		s.stateBuilder.unixNanoToTime(event.GetTimestamp()),
		newScheduleID,
	).Return(nil).Times(1)
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeDecisionTaskFailed() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	scheduleID := int64(12)
	startedID := int64(28)
	evenType := enums.EventTypeDecisionTaskFailed
	event := &commonproto.HistoryEvent{
		Version:   version,
		EventId:   130,
		Timestamp: now.UnixNano(),
		EventType: evenType,
		Attributes: &commonproto.HistoryEvent_DecisionTaskFailedEventAttributes{DecisionTaskFailedEventAttributes: &commonproto.DecisionTaskFailedEventAttributes{
			ScheduledEventId: scheduleID,
			StartedEventId:   startedID,
		}},
	}
	s.mockMutableState.EXPECT().ReplicateDecisionTaskFailedEvent().Return(nil).Times(1)
	tasklist := "some random tasklist"
	newScheduleID := int64(233)
	executionInfo := &persistence.WorkflowExecutionInfo{
		TaskList: tasklist,
	}
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	s.mockMutableState.EXPECT().ReplicateTransientDecisionTaskScheduled().Return(&decisionInfo{
		Version:    version,
		ScheduleID: newScheduleID,
		TaskList:   tasklist,
	}, nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().generateDecisionScheduleTasks(
		s.stateBuilder.unixNanoToTime(event.GetTimestamp()),
		newScheduleID,
	).Return(nil).Times(1)
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeDecisionTaskCompleted() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	scheduleID := int64(12)
	startedID := int64(28)
	evenType := enums.EventTypeDecisionTaskCompleted
	event := &commonproto.HistoryEvent{
		Version:   version,
		EventId:   130,
		Timestamp: now.UnixNano(),
		EventType: evenType,
		Attributes: &commonproto.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &commonproto.DecisionTaskCompletedEventAttributes{
			ScheduledEventId: scheduleID,
			StartedEventId:   startedID,
		}},
	}
	s.mockMutableState.EXPECT().ReplicateDecisionTaskCompletedEvent(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

// user timer operations

func (s *stateBuilderSuite) TestApplyEvents_EventTypeTimerStarted() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	timerID := "timer ID"
	timeoutSecond := int64(10)
	evenType := enums.EventTypeTimerStarted
	event := &commonproto.HistoryEvent{
		Version:   version,
		EventId:   130,
		Timestamp: now.UnixNano(),
		EventType: evenType,
		Attributes: &commonproto.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &commonproto.TimerStartedEventAttributes{
			TimerId:                   timerID,
			StartToFireTimeoutSeconds: timeoutSecond,
		}},
	}
	expiryTime, _ := types.TimestampProto(time.Unix(0, event.GetTimestamp()).Add(time.Duration(timeoutSecond) * time.Second))
	ti := &persistenceblobs.TimerInfo{
		Version:    event.GetVersion(),
		TimerID:    timerID,
		ExpiryTime: expiryTime,
		StartedID:  event.GetEventId(),
		TaskStatus: timerTaskStatusNone,
	}
	s.mockMutableState.EXPECT().ReplicateTimerStartedEvent(event).Return(ti, nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeTimerFired() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := enums.EventTypeTimerFired
	event := &commonproto.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &commonproto.HistoryEvent_TimerFiredEventAttributes{TimerFiredEventAttributes: &commonproto.TimerFiredEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ReplicateTimerFiredEvent(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeCancelTimerFailed() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := enums.EventTypeCancelTimerFailed
	event := &commonproto.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &commonproto.HistoryEvent_CancelTimerFailedEventAttributes{CancelTimerFailedEventAttributes: &commonproto.CancelTimerFailedEventAttributes{}},
	}
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)
	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeTimerCanceled() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()

	evenType := enums.EventTypeTimerCanceled
	event := &commonproto.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &commonproto.HistoryEvent_TimerCanceledEventAttributes{TimerCanceledEventAttributes: &commonproto.TimerCanceledEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateTimerCanceledEvent(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

// activity operations

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskScheduled() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	activityID := "activity ID"
	tasklist := "some random tasklist"
	timeoutSecond := int32(10)
	evenType := enums.EventTypeActivityTaskScheduled
	event := &commonproto.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &commonproto.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &commonproto.ActivityTaskScheduledEventAttributes{}},
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
		TimerTaskStatus:          timerTaskStatusNone,
		TaskList:                 tasklist,
	}
	executionInfo := &persistence.WorkflowExecutionInfo{
		TaskList: tasklist,
	}
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	s.mockMutableState.EXPECT().ReplicateActivityTaskScheduledEvent(event.GetEventId(), event).Return(ai, nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().generateActivityTransferTasks(
		s.stateBuilder.unixNanoToTime(event.GetTimestamp()),
		event,
	).Return(nil).Times(1)
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskStarted() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	tasklist := "some random tasklist"
	evenType := enums.EventTypeActivityTaskScheduled
	scheduledEvent := &commonproto.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &commonproto.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &commonproto.ActivityTaskScheduledEventAttributes{}},
	}

	evenType = enums.EventTypeActivityTaskStarted
	startedEvent := &commonproto.HistoryEvent{
		Version:    version,
		EventId:    scheduledEvent.GetEventId() + 1,
		Timestamp:  scheduledEvent.GetTimestamp() + 1000,
		EventType:  evenType,
		Attributes: &commonproto.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &commonproto.ActivityTaskStartedEventAttributes{}},
	}

	executionInfo := &persistence.WorkflowExecutionInfo{
		TaskList: tasklist,
	}
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	s.mockMutableState.EXPECT().ReplicateActivityTaskStartedEvent(startedEvent).Return(nil).Times(1)
	s.mockUpdateVersion(startedEvent)
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(startedEvent), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskTimedOut() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := enums.EventTypeActivityTaskTimedOut
	event := &commonproto.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &commonproto.HistoryEvent_ActivityTaskTimedOutEventAttributes{ActivityTaskTimedOutEventAttributes: &commonproto.ActivityTaskTimedOutEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ReplicateActivityTaskTimedOutEvent(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	//	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskFailed() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := enums.EventTypeActivityTaskFailed
	event := &commonproto.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &commonproto.HistoryEvent_ActivityTaskFailedEventAttributes{ActivityTaskFailedEventAttributes: &commonproto.ActivityTaskFailedEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ReplicateActivityTaskFailedEvent(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskCompleted() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := enums.EventTypeActivityTaskCompleted
	event := &commonproto.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &commonproto.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &commonproto.ActivityTaskCompletedEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ReplicateActivityTaskCompletedEvent(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskCancelRequested() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := enums.EventTypeActivityTaskCancelRequested
	event := &commonproto.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &commonproto.HistoryEvent_ActivityTaskCancelRequestedEventAttributes{ActivityTaskCancelRequestedEventAttributes: &commonproto.ActivityTaskCancelRequestedEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateActivityTaskCancelRequestedEvent(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeRequestCancelActivityTaskFailed() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := enums.EventTypeRequestCancelActivityTaskFailed
	event := &commonproto.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &commonproto.HistoryEvent_RequestCancelActivityTaskFailedEventAttributes{RequestCancelActivityTaskFailedEventAttributes: &commonproto.RequestCancelActivityTaskFailedEventAttributes{}},
	}
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskCanceled() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := enums.EventTypeActivityTaskCanceled
	event := &commonproto.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &commonproto.HistoryEvent_ActivityTaskCanceledEventAttributes{ActivityTaskCanceledEventAttributes: &commonproto.ActivityTaskCanceledEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ReplicateActivityTaskCanceledEvent(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

// child workflow operations

func (s *stateBuilderSuite) TestApplyEvents_EventTypeStartChildWorkflowExecutionInitiated() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}
	targetWorkflowID := "some random target workflow ID"

	now := time.Now()
	createRequestID := uuid.New()
	evenType := enums.EventTypeStartChildWorkflowExecutionInitiated
	event := &commonproto.HistoryEvent{
		Version:   version,
		EventId:   130,
		Timestamp: now.UnixNano(),
		EventType: evenType,
		Attributes: &commonproto.HistoryEvent_StartChildWorkflowExecutionInitiatedEventAttributes{StartChildWorkflowExecutionInitiatedEventAttributes: &commonproto.StartChildWorkflowExecutionInitiatedEventAttributes{
			Domain:     testTargetDomainName,
			WorkflowId: targetWorkflowID,
		}},
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
	s.mockMutableState.EXPECT().ReplicateStartChildWorkflowExecutionInitiatedEvent(
		event.GetEventId(), event, gomock.Any(),
	).Return(ci, nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockTaskGenerator.EXPECT().generateChildWorkflowTasks(
		s.stateBuilder.unixNanoToTime(event.GetTimestamp()),
		event,
	).Return(nil).Times(1)
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeStartChildWorkflowExecutionFailed() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := enums.EventTypeStartChildWorkflowExecutionFailed
	event := &commonproto.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &commonproto.HistoryEvent_StartChildWorkflowExecutionFailedEventAttributes{StartChildWorkflowExecutionFailedEventAttributes: &commonproto.StartChildWorkflowExecutionFailedEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateStartChildWorkflowExecutionFailedEvent(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionStarted() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := enums.EventTypeChildWorkflowExecutionStarted
	event := &commonproto.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &commonproto.HistoryEvent_ChildWorkflowExecutionStartedEventAttributes{ChildWorkflowExecutionStartedEventAttributes: &commonproto.ChildWorkflowExecutionStartedEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateChildWorkflowExecutionStartedEvent(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionTimedOut() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := enums.EventTypeChildWorkflowExecutionTimedOut
	event := &commonproto.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &commonproto.HistoryEvent_ChildWorkflowExecutionTimedOutEventAttributes{ChildWorkflowExecutionTimedOutEventAttributes: &commonproto.ChildWorkflowExecutionTimedOutEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateChildWorkflowExecutionTimedOutEvent(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionTerminated() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := enums.EventTypeChildWorkflowExecutionTerminated
	event := &commonproto.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &commonproto.HistoryEvent_ChildWorkflowExecutionTerminatedEventAttributes{ChildWorkflowExecutionTerminatedEventAttributes: &commonproto.ChildWorkflowExecutionTerminatedEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateChildWorkflowExecutionTerminatedEvent(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionFailed() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := enums.EventTypeChildWorkflowExecutionFailed
	event := &commonproto.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &commonproto.HistoryEvent_ChildWorkflowExecutionFailedEventAttributes{ChildWorkflowExecutionFailedEventAttributes: &commonproto.ChildWorkflowExecutionFailedEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateChildWorkflowExecutionFailedEvent(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionCompleted() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := enums.EventTypeChildWorkflowExecutionCompleted
	event := &commonproto.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &commonproto.HistoryEvent_ChildWorkflowExecutionCompletedEventAttributes{ChildWorkflowExecutionCompletedEventAttributes: &commonproto.ChildWorkflowExecutionCompletedEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateChildWorkflowExecutionCompletedEvent(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

// cancel external workflow operations

func (s *stateBuilderSuite) TestApplyEvents_EventTypeRequestCancelExternalWorkflowExecutionInitiated() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	targetWorkflowID := "some random target workflow ID"
	targetRunID := uuid.New()
	childWorkflowOnly := true

	now := time.Now()
	cancellationRequestID := uuid.New()
	control := []byte("some random control")
	evenType := enums.EventTypeRequestCancelExternalWorkflowExecutionInitiated
	event := &commonproto.HistoryEvent{
		Version:   version,
		EventId:   130,
		Timestamp: now.UnixNano(),
		EventType: evenType,
		Attributes: &commonproto.HistoryEvent_RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{RequestCancelExternalWorkflowExecutionInitiatedEventAttributes: &commonproto.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
			Domain: testTargetDomainName,
			WorkflowExecution: &commonproto.WorkflowExecution{
				WorkflowId: targetWorkflowID,
				RunId:      targetRunID,
			},
			ChildWorkflowOnly: childWorkflowOnly,
			Control:           control,
		}},
	}
	rci := &persistenceblobs.RequestCancelInfo{
		Version:         event.GetVersion(),
		InitiatedID:     event.GetEventId(),
		CancelRequestID: cancellationRequestID,
	}

	// the cancellation request ID is generated inside, cannot assert equal
	s.mockMutableState.EXPECT().ReplicateRequestCancelExternalWorkflowExecutionInitiatedEvent(
		event.GetEventId(), event, gomock.Any(),
	).Return(rci, nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockTaskGenerator.EXPECT().generateRequestCancelExternalTasks(
		s.stateBuilder.unixNanoToTime(event.GetTimestamp()),
		event,
	).Return(nil).Times(1)
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeRequestCancelExternalWorkflowExecutionFailed() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := enums.EventTypeRequestCancelExternalWorkflowExecutionFailed
	event := &commonproto.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &commonproto.HistoryEvent_RequestCancelExternalWorkflowExecutionFailedEventAttributes{RequestCancelExternalWorkflowExecutionFailedEventAttributes: &commonproto.RequestCancelExternalWorkflowExecutionFailedEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateRequestCancelExternalWorkflowExecutionFailedEvent(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeExternalWorkflowExecutionCancelRequested() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := enums.EventTypeExternalWorkflowExecutionCancelRequested
	event := &commonproto.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &commonproto.HistoryEvent_ExternalWorkflowExecutionCancelRequestedEventAttributes{ExternalWorkflowExecutionCancelRequestedEventAttributes: &commonproto.ExternalWorkflowExecutionCancelRequestedEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateExternalWorkflowExecutionCancelRequested(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionCanceled() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := enums.EventTypeChildWorkflowExecutionCanceled
	event := &commonproto.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &commonproto.HistoryEvent_ChildWorkflowExecutionCanceledEventAttributes{ChildWorkflowExecutionCanceledEventAttributes: &commonproto.ChildWorkflowExecutionCanceledEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateChildWorkflowExecutionCanceledEvent(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

// signal external workflow operations

func (s *stateBuilderSuite) TestApplyEvents_EventTypeSignalExternalWorkflowExecutionInitiated() {
	version := int64(1)
	requestID := uuid.New()
	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}
	targetWorkflowID := "some random target workflow ID"
	targetRunID := uuid.New()
	childWorkflowOnly := true

	now := time.Now()
	signalRequestID := uuid.New()
	signalName := "some random signal name"
	signalInput := []byte("some random signal input")
	control := []byte("some random control")
	evenType := enums.EventTypeSignalExternalWorkflowExecutionInitiated
	event := &commonproto.HistoryEvent{
		Version:   version,
		EventId:   130,
		Timestamp: now.UnixNano(),
		EventType: evenType,
		Attributes: &commonproto.HistoryEvent_SignalExternalWorkflowExecutionInitiatedEventAttributes{SignalExternalWorkflowExecutionInitiatedEventAttributes: &commonproto.SignalExternalWorkflowExecutionInitiatedEventAttributes{
			Domain: testTargetDomainName,
			WorkflowExecution: &commonproto.WorkflowExecution{
				WorkflowId: targetWorkflowID,
				RunId:      targetRunID,
			},
			SignalName:        signalName,
			Input:             signalInput,
			ChildWorkflowOnly: childWorkflowOnly,
		}},
	}
	si := &persistenceblobs.SignalInfo{
		Version:     event.GetVersion(),
		InitiatedID: event.GetEventId(),
		RequestID:   signalRequestID,
		Name:        signalName,
		Input:       signalInput,
		Control:     control,
	}

	// the cancellation request ID is generated inside, cannot assert equal
	s.mockMutableState.EXPECT().ReplicateSignalExternalWorkflowExecutionInitiatedEvent(
		event.GetEventId(), event, gomock.Any(),
	).Return(si, nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockTaskGenerator.EXPECT().generateSignalExternalTasks(
		s.stateBuilder.unixNanoToTime(event.GetTimestamp()),
		event,
	).Return(nil).Times(1)
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeSignalExternalWorkflowExecutionFailed() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := enums.EventTypeSignalExternalWorkflowExecutionFailed
	event := &commonproto.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &commonproto.HistoryEvent_SignalExternalWorkflowExecutionFailedEventAttributes{SignalExternalWorkflowExecutionFailedEventAttributes: &commonproto.SignalExternalWorkflowExecutionFailedEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateSignalExternalWorkflowExecutionFailedEvent(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeExternalWorkflowExecutionSignaled() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := enums.EventTypeExternalWorkflowExecutionSignaled
	event := &commonproto.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &commonproto.HistoryEvent_ExternalWorkflowExecutionSignaledEventAttributes{ExternalWorkflowExecutionSignaledEventAttributes: &commonproto.ExternalWorkflowExecutionSignaledEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateExternalWorkflowExecutionSignaled(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testDomainID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEventsNewEventsNotHandled() {
	eventTypes := enums.EventType_value
	s.Equal(42, len(eventTypes), "If you see this error, you are adding new event type. "+
		"Before updating the number to make this test pass, please make sure you update stateBuilderImpl.applyEvents method "+
		"to handle the new decision type. Otherwise cross dc will not work on the new event.")
}
