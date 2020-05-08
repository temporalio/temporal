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
	"testing"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/temporal-proto/common"
	eventpb "go.temporal.io/temporal-proto/event"
	executionpb "go.temporal.io/temporal-proto/execution"
	tasklistpb "go.temporal.io/temporal-proto/tasklist"

	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/backoff"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/payloads"
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
		mockNamespaceCache  *cache.MockNamespaceCache
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
				ShardId:          0,
				RangeId:          1,
				TransferAckLevel: 0,
			}},
		NewDynamicConfigForTest(),
	)

	s.mockNamespaceCache = s.mockShard.resource.NamespaceCache
	s.mockClusterMetadata = s.mockShard.resource.ClusterMetadata
	s.mockEventsCache = s.mockShard.mockEventsCache
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
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

func (s *stateBuilderSuite) mockUpdateVersion(events ...*eventpb.HistoryEvent) {
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

func (s *stateBuilderSuite) toHistory(events ...*eventpb.HistoryEvent) []*eventpb.HistoryEvent {
	return events
}

// workflow operations

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionStarted_NoCronSchedule() {
	cronSchedule := ""
	version := int64(1)
	requestID := uuid.New()
	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	executionInfo := &persistence.WorkflowExecutionInfo{
		WorkflowRunTimeout: 100,
		CronSchedule:       cronSchedule,
	}

	now := time.Now()
	evenType := eventpb.EventType_WorkflowExecutionStarted
	startWorkflowAttribute := &eventpb.WorkflowExecutionStartedEventAttributes{
		ParentWorkflowNamespace: testParentNamespace,
	}

	event := &eventpb.HistoryEvent{
		Version:    version,
		EventId:    1,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &eventpb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: startWorkflowAttribute},
	}

	s.mockNamespaceCache.EXPECT().GetNamespace(testParentNamespace).Return(testGlobalParentNamespaceEntry, nil).Times(1)
	s.mockMutableState.EXPECT().ReplicateWorkflowExecutionStartedEvent(testParentNamespaceID, execution, requestID, event).Return(nil).Times(1)
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

	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionStarted_WithCronSchedule() {
	cronSchedule := "* * * * *"
	version := int64(1)
	requestID := uuid.New()
	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	executionInfo := &persistence.WorkflowExecutionInfo{
		WorkflowRunTimeout: 100,
		CronSchedule:       cronSchedule,
	}

	now := time.Now()
	evenType := eventpb.EventType_WorkflowExecutionStarted
	startWorkflowAttribute := &eventpb.WorkflowExecutionStartedEventAttributes{
		ParentWorkflowNamespace:         testParentNamespace,
		Initiator:                       commonpb.ContinueAsNewInitiator_CronSchedule,
		FirstDecisionTaskBackoffSeconds: int32(backoff.GetBackoffForNextSchedule(cronSchedule, now, now).Seconds()),
	}

	event := &eventpb.HistoryEvent{
		Version:    version,
		EventId:    1,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &eventpb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: startWorkflowAttribute},
	}

	s.mockNamespaceCache.EXPECT().GetNamespace(testParentNamespace).Return(testGlobalParentNamespaceEntry, nil).Times(1)
	s.mockMutableState.EXPECT().ReplicateWorkflowExecutionStartedEvent(testParentNamespaceID, execution, requestID, event).Return(nil).Times(1)
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

	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionTimedOut() {
	version := int64(1)
	requestID := uuid.New()
	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := eventpb.EventType_WorkflowExecutionTimedOut
	event := &eventpb.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &eventpb.HistoryEvent_WorkflowExecutionTimedOutEventAttributes{WorkflowExecutionTimedOutEventAttributes: &eventpb.WorkflowExecutionTimedOutEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ReplicateWorkflowExecutionTimedoutEvent(event.GetEventId(), event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockTaskGenerator.EXPECT().generateWorkflowCloseTasks(
		s.stateBuilder.unixNanoToTime(event.GetTimestamp()),
	).Return(nil).Times(1)
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionTerminated() {
	version := int64(1)
	requestID := uuid.New()
	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := eventpb.EventType_WorkflowExecutionTerminated
	event := &eventpb.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &eventpb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{WorkflowExecutionTerminatedEventAttributes: &eventpb.WorkflowExecutionTerminatedEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ReplicateWorkflowExecutionTerminatedEvent(event.GetEventId(), event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockTaskGenerator.EXPECT().generateWorkflowCloseTasks(
		s.stateBuilder.unixNanoToTime(event.GetTimestamp()),
	).Return(nil).Times(1)
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)
	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionFailed() {
	version := int64(1)
	requestID := uuid.New()
	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := eventpb.EventType_WorkflowExecutionFailed
	event := &eventpb.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &eventpb.HistoryEvent_WorkflowExecutionFailedEventAttributes{WorkflowExecutionFailedEventAttributes: &eventpb.WorkflowExecutionFailedEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ReplicateWorkflowExecutionFailedEvent(event.GetEventId(), event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockTaskGenerator.EXPECT().generateWorkflowCloseTasks(
		s.stateBuilder.unixNanoToTime(event.GetTimestamp()),
	).Return(nil).Times(1)
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionCompleted() {
	version := int64(1)
	requestID := uuid.New()
	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := eventpb.EventType_WorkflowExecutionCompleted
	event := &eventpb.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &eventpb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{WorkflowExecutionCompletedEventAttributes: &eventpb.WorkflowExecutionCompletedEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ReplicateWorkflowExecutionCompletedEvent(event.GetEventId(), event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockTaskGenerator.EXPECT().generateWorkflowCloseTasks(
		s.stateBuilder.unixNanoToTime(event.GetTimestamp()),
	).Return(nil).Times(1)
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionCanceled() {
	version := int64(1)
	requestID := uuid.New()
	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := eventpb.EventType_WorkflowExecutionCanceled
	event := &eventpb.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &eventpb.HistoryEvent_WorkflowExecutionCanceledEventAttributes{WorkflowExecutionCanceledEventAttributes: &eventpb.WorkflowExecutionCanceledEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ReplicateWorkflowExecutionCanceledEvent(event.GetEventId(), event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockTaskGenerator.EXPECT().generateWorkflowCloseTasks(
		s.stateBuilder.unixNanoToTime(event.GetTimestamp()),
	).Return(nil).Times(1)
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionContinuedAsNew() {
	version := int64(1)
	requestID := uuid.New()
	execution := executionpb.WorkflowExecution{
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
	taskTimeoutSeconds := int32(11)
	newRunID := uuid.New()

	continueAsNewEvent := &eventpb.HistoryEvent{
		Version:   version,
		EventId:   130,
		Timestamp: now.UnixNano(),
		EventType: eventpb.EventType_WorkflowExecutionContinuedAsNew,
		Attributes: &eventpb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{WorkflowExecutionContinuedAsNewEventAttributes: &eventpb.WorkflowExecutionContinuedAsNewEventAttributes{
			NewExecutionRunId: newRunID,
		}},
	}

	newRunStartedEvent := &eventpb.HistoryEvent{
		Version:   version,
		EventId:   1,
		Timestamp: now.UnixNano(),
		EventType: eventpb.EventType_WorkflowExecutionStarted,
		Attributes: &eventpb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &eventpb.WorkflowExecutionStartedEventAttributes{
			ParentWorkflowNamespace: testParentNamespace,
			ParentWorkflowExecution: &executionpb.WorkflowExecution{
				WorkflowId: parentWorkflowID,
				RunId:      parentRunID,
			},
			ParentInitiatedEventId:          parentInitiatedEventID,
			WorkflowExecutionTimeoutSeconds: workflowTimeoutSecond,
			WorkflowTaskTimeoutSeconds:      taskTimeoutSeconds,
			TaskList:                        &tasklistpb.TaskList{Name: tasklist},
			WorkflowType:                    &commonpb.WorkflowType{Name: workflowType},
		}},
	}

	newRunSignalEvent := &eventpb.HistoryEvent{
		Version:   version,
		EventId:   2,
		Timestamp: now.UnixNano(),
		EventType: eventpb.EventType_WorkflowExecutionSignaled,
		Attributes: &eventpb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &eventpb.WorkflowExecutionSignaledEventAttributes{
			SignalName: "some random signal name",
			Input:      payloads.EncodeString("some random signal input"),
			Identity:   "some random identity",
		}},
	}

	newRunDecisionAttempt := int64(123)
	newRunDecisionEvent := &eventpb.HistoryEvent{
		Version:   version,
		EventId:   3,
		Timestamp: now.UnixNano(),
		EventType: eventpb.EventType_DecisionTaskScheduled,
		Attributes: &eventpb.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &eventpb.DecisionTaskScheduledEventAttributes{
			TaskList:                   &tasklistpb.TaskList{Name: tasklist},
			StartToCloseTimeoutSeconds: taskTimeoutSeconds,
			Attempt:                    newRunDecisionAttempt,
		}},
	}
	newRunEvents := []*eventpb.HistoryEvent{
		newRunStartedEvent, newRunSignalEvent, newRunDecisionEvent,
	}

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(continueAsNewEvent.GetVersion()).Return(s.sourceCluster).AnyTimes()
	s.mockMutableState.EXPECT().ReplicateWorkflowExecutionContinuedAsNewEvent(
		continueAsNewEvent.GetEventId(),
		testNamespaceID,
		continueAsNewEvent,
	).Return(nil).Times(1)
	s.mockMutableState.EXPECT().GetNamespaceEntry().Return(testGlobalNamespaceEntry).AnyTimes()
	s.mockUpdateVersion(continueAsNewEvent)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockTaskGenerator.EXPECT().generateWorkflowCloseTasks(
		s.stateBuilder.unixNanoToTime(continueAsNewEvent.GetTimestamp()),
	).Return(nil).Times(1)
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	// new workflow namespace
	s.mockNamespaceCache.EXPECT().GetNamespace(testParentNamespace).Return(testGlobalParentNamespaceEntry, nil).AnyTimes()
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
		testNamespaceID, requestID, execution, s.toHistory(continueAsNewEvent), newRunEvents, false,
	)
	s.Nil(err)
	s.NotNil(newRunStateBuilder)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionContinuedAsNew_EmptyNewRunHistory() {
	version := int64(1)
	requestID := uuid.New()
	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	newRunID := uuid.New()

	continueAsNewEvent := &eventpb.HistoryEvent{
		Version:   version,
		EventId:   130,
		Timestamp: now.UnixNano(),
		EventType: eventpb.EventType_WorkflowExecutionContinuedAsNew,
		Attributes: &eventpb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{WorkflowExecutionContinuedAsNewEventAttributes: &eventpb.WorkflowExecutionContinuedAsNewEventAttributes{
			NewExecutionRunId: newRunID,
		}},
	}

	s.mockMutableState.EXPECT().ReplicateWorkflowExecutionContinuedAsNewEvent(
		continueAsNewEvent.GetEventId(),
		testNamespaceID,
		continueAsNewEvent,
	).Return(nil).Times(1)
	s.mockMutableState.EXPECT().GetNamespaceEntry().Return(testGlobalNamespaceEntry).AnyTimes()
	s.mockUpdateVersion(continueAsNewEvent)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockTaskGenerator.EXPECT().generateWorkflowCloseTasks(
		s.stateBuilder.unixNanoToTime(continueAsNewEvent.GetTimestamp()),
	).Return(nil).Times(1)
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	// new workflow namespace
	s.mockNamespaceCache.EXPECT().GetNamespace(testParentNamespace).Return(testGlobalParentNamespaceEntry, nil).AnyTimes()
	newRunStateBuilder, err := s.stateBuilder.applyEvents(
		testNamespaceID, requestID, execution, s.toHistory(continueAsNewEvent), nil, false,
	)
	s.Nil(err)
	s.Nil(newRunStateBuilder)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionSignaled() {
	version := int64(1)
	requestID := uuid.New()

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := eventpb.EventType_WorkflowExecutionSignaled
	event := &eventpb.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &eventpb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &eventpb.WorkflowExecutionSignaledEventAttributes{}},
	}
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockMutableState.EXPECT().ReplicateWorkflowExecutionSignaled(event).Return(nil).Times(1)
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionCancelRequested() {
	version := int64(1)
	requestID := uuid.New()

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}
	now := time.Now()
	evenType := eventpb.EventType_WorkflowExecutionCancelRequested
	event := &eventpb.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &eventpb.HistoryEvent_WorkflowExecutionCancelRequestedEventAttributes{WorkflowExecutionCancelRequestedEventAttributes: &eventpb.WorkflowExecutionCancelRequestedEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ReplicateWorkflowExecutionCancelRequestedEvent(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeUpsertWorkflowSearchAttributes() {
	version := int64(1)
	requestID := uuid.New()

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := eventpb.EventType_UpsertWorkflowSearchAttributes
	event := &eventpb.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &eventpb.HistoryEvent_UpsertWorkflowSearchAttributesEventAttributes{UpsertWorkflowSearchAttributesEventAttributes: &eventpb.UpsertWorkflowSearchAttributesEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateUpsertWorkflowSearchAttributesEvent(event).Return().Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockTaskGenerator.EXPECT().generateWorkflowSearchAttrTasks(
		s.stateBuilder.unixNanoToTime(event.GetTimestamp()),
	).Return(nil).Times(1)
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeMarkerRecorded() {
	version := int64(1)
	requestID := uuid.New()

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := eventpb.EventType_MarkerRecorded
	event := &eventpb.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &eventpb.HistoryEvent_MarkerRecordedEventAttributes{MarkerRecordedEventAttributes: &eventpb.MarkerRecordedEventAttributes{}},
	}
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

// decision operations

func (s *stateBuilderSuite) TestApplyEvents_EventTypeDecisionTaskScheduled() {
	version := int64(1)
	requestID := uuid.New()

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	tasklist := "some random tasklist"
	timeoutSecond := int32(11)
	evenType := eventpb.EventType_DecisionTaskScheduled
	decisionAttempt := int64(111)
	event := &eventpb.HistoryEvent{
		Version:   version,
		EventId:   130,
		Timestamp: now.UnixNano(),
		EventType: evenType,
		Attributes: &eventpb.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &eventpb.DecisionTaskScheduledEventAttributes{
			TaskList:                   &tasklistpb.TaskList{Name: tasklist},
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

	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}
func (s *stateBuilderSuite) TestApplyEvents_EventTypeDecisionTaskStarted() {
	version := int64(1)
	requestID := uuid.New()

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	tasklist := "some random tasklist"
	timeoutSecond := int32(11)
	scheduleID := int64(111)
	decisionRequestID := uuid.New()
	evenType := eventpb.EventType_DecisionTaskStarted
	event := &eventpb.HistoryEvent{
		Version:   version,
		EventId:   130,
		Timestamp: now.UnixNano(),
		EventType: evenType,
		Attributes: &eventpb.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &eventpb.DecisionTaskStartedEventAttributes{
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

	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeDecisionTaskTimedOut() {
	version := int64(1)
	requestID := uuid.New()

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	scheduleID := int64(12)
	startedID := int64(28)
	evenType := eventpb.EventType_DecisionTaskTimedOut
	event := &eventpb.HistoryEvent{
		Version:   version,
		EventId:   130,
		Timestamp: now.UnixNano(),
		EventType: evenType,
		Attributes: &eventpb.HistoryEvent_DecisionTaskTimedOutEventAttributes{DecisionTaskTimedOutEventAttributes: &eventpb.DecisionTaskTimedOutEventAttributes{
			ScheduledEventId: scheduleID,
			StartedEventId:   startedID,
			TimeoutType:      eventpb.TimeoutType_StartToClose,
		}},
	}
	s.mockMutableState.EXPECT().ReplicateDecisionTaskTimedOutEvent(eventpb.TimeoutType_StartToClose).Return(nil).Times(1)
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

	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeDecisionTaskFailed() {
	version := int64(1)
	requestID := uuid.New()

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	scheduleID := int64(12)
	startedID := int64(28)
	evenType := eventpb.EventType_DecisionTaskFailed
	event := &eventpb.HistoryEvent{
		Version:   version,
		EventId:   130,
		Timestamp: now.UnixNano(),
		EventType: evenType,
		Attributes: &eventpb.HistoryEvent_DecisionTaskFailedEventAttributes{DecisionTaskFailedEventAttributes: &eventpb.DecisionTaskFailedEventAttributes{
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

	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeDecisionTaskCompleted() {
	version := int64(1)
	requestID := uuid.New()

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	scheduleID := int64(12)
	startedID := int64(28)
	evenType := eventpb.EventType_DecisionTaskCompleted
	event := &eventpb.HistoryEvent{
		Version:   version,
		EventId:   130,
		Timestamp: now.UnixNano(),
		EventType: evenType,
		Attributes: &eventpb.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &eventpb.DecisionTaskCompletedEventAttributes{
			ScheduledEventId: scheduleID,
			StartedEventId:   startedID,
		}},
	}
	s.mockMutableState.EXPECT().ReplicateDecisionTaskCompletedEvent(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

// user timer operations

func (s *stateBuilderSuite) TestApplyEvents_EventTypeTimerStarted() {
	version := int64(1)
	requestID := uuid.New()

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	timerID := "timer ID"
	timeoutSecond := int64(10)
	evenType := eventpb.EventType_TimerStarted
	event := &eventpb.HistoryEvent{
		Version:   version,
		EventId:   130,
		Timestamp: now.UnixNano(),
		EventType: evenType,
		Attributes: &eventpb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &eventpb.TimerStartedEventAttributes{
			TimerId:                   timerID,
			StartToFireTimeoutSeconds: timeoutSecond,
		}},
	}
	expiryTime, _ := types.TimestampProto(time.Unix(0, event.GetTimestamp()).Add(time.Duration(timeoutSecond) * time.Second))
	ti := &persistenceblobs.TimerInfo{
		Version:    event.GetVersion(),
		TimerId:    timerID,
		ExpiryTime: expiryTime,
		StartedId:  event.GetEventId(),
		TaskStatus: timerTaskStatusNone,
	}
	s.mockMutableState.EXPECT().ReplicateTimerStartedEvent(event).Return(ti, nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeTimerFired() {
	version := int64(1)
	requestID := uuid.New()

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := eventpb.EventType_TimerFired
	event := &eventpb.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &eventpb.HistoryEvent_TimerFiredEventAttributes{TimerFiredEventAttributes: &eventpb.TimerFiredEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ReplicateTimerFiredEvent(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeCancelTimerFailed() {
	version := int64(1)
	requestID := uuid.New()

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := eventpb.EventType_CancelTimerFailed
	event := &eventpb.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &eventpb.HistoryEvent_CancelTimerFailedEventAttributes{CancelTimerFailedEventAttributes: &eventpb.CancelTimerFailedEventAttributes{}},
	}
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)
	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeTimerCanceled() {
	version := int64(1)
	requestID := uuid.New()

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()

	evenType := eventpb.EventType_TimerCanceled
	event := &eventpb.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &eventpb.HistoryEvent_TimerCanceledEventAttributes{TimerCanceledEventAttributes: &eventpb.TimerCanceledEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateTimerCanceledEvent(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

// activity operations

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskScheduled() {
	version := int64(1)
	requestID := uuid.New()

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	activityID := "activity ID"
	tasklist := "some random tasklist"
	timeoutSecond := int32(10)
	evenType := eventpb.EventType_ActivityTaskScheduled
	event := &eventpb.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &eventpb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &eventpb.ActivityTaskScheduledEventAttributes{}},
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

	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskStarted() {
	version := int64(1)
	requestID := uuid.New()

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	tasklist := "some random tasklist"
	evenType := eventpb.EventType_ActivityTaskScheduled
	scheduledEvent := &eventpb.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &eventpb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &eventpb.ActivityTaskScheduledEventAttributes{}},
	}

	evenType = eventpb.EventType_ActivityTaskStarted
	startedEvent := &eventpb.HistoryEvent{
		Version:    version,
		EventId:    scheduledEvent.GetEventId() + 1,
		Timestamp:  scheduledEvent.GetTimestamp() + 1000,
		EventType:  evenType,
		Attributes: &eventpb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &eventpb.ActivityTaskStartedEventAttributes{}},
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

	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(startedEvent), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskTimedOut() {
	version := int64(1)
	requestID := uuid.New()

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := eventpb.EventType_ActivityTaskTimedOut
	event := &eventpb.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &eventpb.HistoryEvent_ActivityTaskTimedOutEventAttributes{ActivityTaskTimedOutEventAttributes: &eventpb.ActivityTaskTimedOutEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ReplicateActivityTaskTimedOutEvent(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	//	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskFailed() {
	version := int64(1)
	requestID := uuid.New()

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := eventpb.EventType_ActivityTaskFailed
	event := &eventpb.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &eventpb.HistoryEvent_ActivityTaskFailedEventAttributes{ActivityTaskFailedEventAttributes: &eventpb.ActivityTaskFailedEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ReplicateActivityTaskFailedEvent(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskCompleted() {
	version := int64(1)
	requestID := uuid.New()

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := eventpb.EventType_ActivityTaskCompleted
	event := &eventpb.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &eventpb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &eventpb.ActivityTaskCompletedEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ReplicateActivityTaskCompletedEvent(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskCancelRequested() {
	version := int64(1)
	requestID := uuid.New()

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := eventpb.EventType_ActivityTaskCancelRequested
	event := &eventpb.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &eventpb.HistoryEvent_ActivityTaskCancelRequestedEventAttributes{ActivityTaskCancelRequestedEventAttributes: &eventpb.ActivityTaskCancelRequestedEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateActivityTaskCancelRequestedEvent(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskCanceled() {
	version := int64(1)
	requestID := uuid.New()

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := eventpb.EventType_ActivityTaskCanceled
	event := &eventpb.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &eventpb.HistoryEvent_ActivityTaskCanceledEventAttributes{ActivityTaskCanceledEventAttributes: &eventpb.ActivityTaskCanceledEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ReplicateActivityTaskCanceledEvent(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

// child workflow operations

func (s *stateBuilderSuite) TestApplyEvents_EventTypeStartChildWorkflowExecutionInitiated() {
	version := int64(1)
	requestID := uuid.New()

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}
	targetWorkflowID := "some random target workflow ID"

	now := time.Now()
	createRequestID := uuid.New()
	evenType := eventpb.EventType_StartChildWorkflowExecutionInitiated
	event := &eventpb.HistoryEvent{
		Version:   version,
		EventId:   130,
		Timestamp: now.UnixNano(),
		EventType: evenType,
		Attributes: &eventpb.HistoryEvent_StartChildWorkflowExecutionInitiatedEventAttributes{StartChildWorkflowExecutionInitiatedEventAttributes: &eventpb.StartChildWorkflowExecutionInitiatedEventAttributes{
			Namespace:  testTargetNamespace,
			WorkflowId: targetWorkflowID,
		}},
	}

	ci := &persistence.ChildExecutionInfo{
		Version:               event.GetVersion(),
		InitiatedID:           event.GetEventId(),
		InitiatedEventBatchID: event.GetEventId(),
		StartedID:             common.EmptyEventID,
		CreateRequestID:       createRequestID,
		Namespace:             testTargetNamespace,
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

	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeStartChildWorkflowExecutionFailed() {
	version := int64(1)
	requestID := uuid.New()

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := eventpb.EventType_StartChildWorkflowExecutionFailed
	event := &eventpb.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &eventpb.HistoryEvent_StartChildWorkflowExecutionFailedEventAttributes{StartChildWorkflowExecutionFailedEventAttributes: &eventpb.StartChildWorkflowExecutionFailedEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateStartChildWorkflowExecutionFailedEvent(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionStarted() {
	version := int64(1)
	requestID := uuid.New()

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := eventpb.EventType_ChildWorkflowExecutionStarted
	event := &eventpb.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &eventpb.HistoryEvent_ChildWorkflowExecutionStartedEventAttributes{ChildWorkflowExecutionStartedEventAttributes: &eventpb.ChildWorkflowExecutionStartedEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateChildWorkflowExecutionStartedEvent(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionTimedOut() {
	version := int64(1)
	requestID := uuid.New()

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := eventpb.EventType_ChildWorkflowExecutionTimedOut
	event := &eventpb.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &eventpb.HistoryEvent_ChildWorkflowExecutionTimedOutEventAttributes{ChildWorkflowExecutionTimedOutEventAttributes: &eventpb.ChildWorkflowExecutionTimedOutEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateChildWorkflowExecutionTimedOutEvent(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionTerminated() {
	version := int64(1)
	requestID := uuid.New()

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := eventpb.EventType_ChildWorkflowExecutionTerminated
	event := &eventpb.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &eventpb.HistoryEvent_ChildWorkflowExecutionTerminatedEventAttributes{ChildWorkflowExecutionTerminatedEventAttributes: &eventpb.ChildWorkflowExecutionTerminatedEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateChildWorkflowExecutionTerminatedEvent(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionFailed() {
	version := int64(1)
	requestID := uuid.New()

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := eventpb.EventType_ChildWorkflowExecutionFailed
	event := &eventpb.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &eventpb.HistoryEvent_ChildWorkflowExecutionFailedEventAttributes{ChildWorkflowExecutionFailedEventAttributes: &eventpb.ChildWorkflowExecutionFailedEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateChildWorkflowExecutionFailedEvent(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionCompleted() {
	version := int64(1)
	requestID := uuid.New()

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := eventpb.EventType_ChildWorkflowExecutionCompleted
	event := &eventpb.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &eventpb.HistoryEvent_ChildWorkflowExecutionCompletedEventAttributes{ChildWorkflowExecutionCompletedEventAttributes: &eventpb.ChildWorkflowExecutionCompletedEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateChildWorkflowExecutionCompletedEvent(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

// cancel external workflow operations

func (s *stateBuilderSuite) TestApplyEvents_EventTypeRequestCancelExternalWorkflowExecutionInitiated() {
	version := int64(1)
	requestID := uuid.New()

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	targetWorkflowID := "some random target workflow ID"
	targetRunID := uuid.New()
	childWorkflowOnly := true

	now := time.Now()
	cancellationRequestID := uuid.New()
	control := "some random control"
	evenType := eventpb.EventType_RequestCancelExternalWorkflowExecutionInitiated
	event := &eventpb.HistoryEvent{
		Version:   version,
		EventId:   130,
		Timestamp: now.UnixNano(),
		EventType: evenType,
		Attributes: &eventpb.HistoryEvent_RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{RequestCancelExternalWorkflowExecutionInitiatedEventAttributes: &eventpb.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
			Namespace: testTargetNamespace,
			WorkflowExecution: &executionpb.WorkflowExecution{
				WorkflowId: targetWorkflowID,
				RunId:      targetRunID,
			},
			ChildWorkflowOnly: childWorkflowOnly,
			Control:           control,
		}},
	}
	rci := &persistenceblobs.RequestCancelInfo{
		Version:         event.GetVersion(),
		InitiatedId:     event.GetEventId(),
		CancelRequestId: cancellationRequestID,
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

	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeRequestCancelExternalWorkflowExecutionFailed() {
	version := int64(1)
	requestID := uuid.New()

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := eventpb.EventType_RequestCancelExternalWorkflowExecutionFailed
	event := &eventpb.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &eventpb.HistoryEvent_RequestCancelExternalWorkflowExecutionFailedEventAttributes{RequestCancelExternalWorkflowExecutionFailedEventAttributes: &eventpb.RequestCancelExternalWorkflowExecutionFailedEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateRequestCancelExternalWorkflowExecutionFailedEvent(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeExternalWorkflowExecutionCancelRequested() {
	version := int64(1)
	requestID := uuid.New()

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := eventpb.EventType_ExternalWorkflowExecutionCancelRequested
	event := &eventpb.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &eventpb.HistoryEvent_ExternalWorkflowExecutionCancelRequestedEventAttributes{ExternalWorkflowExecutionCancelRequestedEventAttributes: &eventpb.ExternalWorkflowExecutionCancelRequestedEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateExternalWorkflowExecutionCancelRequested(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionCanceled() {
	version := int64(1)
	requestID := uuid.New()

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := eventpb.EventType_ChildWorkflowExecutionCanceled
	event := &eventpb.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &eventpb.HistoryEvent_ChildWorkflowExecutionCanceledEventAttributes{ChildWorkflowExecutionCanceledEventAttributes: &eventpb.ChildWorkflowExecutionCanceledEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateChildWorkflowExecutionCanceledEvent(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

// signal external workflow operations

func (s *stateBuilderSuite) TestApplyEvents_EventTypeSignalExternalWorkflowExecutionInitiated() {
	version := int64(1)
	requestID := uuid.New()
	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}
	targetWorkflowID := "some random target workflow ID"
	targetRunID := uuid.New()
	childWorkflowOnly := true

	now := time.Now()
	signalRequestID := uuid.New()
	signalName := "some random signal name"
	signalInput := payloads.EncodeString("some random signal input")
	control := "some random control"
	evenType := eventpb.EventType_SignalExternalWorkflowExecutionInitiated
	event := &eventpb.HistoryEvent{
		Version:   version,
		EventId:   130,
		Timestamp: now.UnixNano(),
		EventType: evenType,
		Attributes: &eventpb.HistoryEvent_SignalExternalWorkflowExecutionInitiatedEventAttributes{SignalExternalWorkflowExecutionInitiatedEventAttributes: &eventpb.SignalExternalWorkflowExecutionInitiatedEventAttributes{
			Namespace: testTargetNamespace,
			WorkflowExecution: &executionpb.WorkflowExecution{
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
		InitiatedId: event.GetEventId(),
		RequestId:   signalRequestID,
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

	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeSignalExternalWorkflowExecutionFailed() {
	version := int64(1)
	requestID := uuid.New()

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := eventpb.EventType_SignalExternalWorkflowExecutionFailed
	event := &eventpb.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &eventpb.HistoryEvent_SignalExternalWorkflowExecutionFailedEventAttributes{SignalExternalWorkflowExecutionFailedEventAttributes: &eventpb.SignalExternalWorkflowExecutionFailedEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateSignalExternalWorkflowExecutionFailedEvent(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeExternalWorkflowExecutionSignaled() {
	version := int64(1)
	requestID := uuid.New()

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
	}

	now := time.Now()
	evenType := eventpb.EventType_ExternalWorkflowExecutionSignaled
	event := &eventpb.HistoryEvent{
		Version:    version,
		EventId:    130,
		Timestamp:  now.UnixNano(),
		EventType:  evenType,
		Attributes: &eventpb.HistoryEvent_ExternalWorkflowExecutionSignaledEventAttributes{ExternalWorkflowExecutionSignaledEventAttributes: &eventpb.ExternalWorkflowExecutionSignaledEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateExternalWorkflowExecutionSignaled(event).Return(nil).Times(1)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).AnyTimes()
	s.mockMutableState.EXPECT().ClearStickyness().Times(1)

	_, err := s.stateBuilder.applyEvents(testNamespaceID, requestID, execution, s.toHistory(event), nil, false)
	s.Nil(err)
}

func (s *stateBuilderSuite) TestApplyEventsNewEventsNotHandled() {
	eventTypes := eventpb.EventType_value
	s.Equal(42, len(eventTypes), "If you see this error, you are adding new event type. "+
		"Before updating the number to make this test pass, please make sure you update stateBuilderImpl.applyEvents method "+
		"to handle the new decision type. Otherwise cross dc will not work on the new event.")
}
