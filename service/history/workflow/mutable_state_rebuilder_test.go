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

package workflow

import (
	"math/rand"
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

	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
)

type (
	stateBuilderSuite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		mockShard           *shard.ContextTest
		mockEventsCache     *events.MockCache
		mockNamespaceCache  *namespace.MockRegistry
		mockTaskGenerator   *MockTaskGenerator
		mockMutableState    *MockMutableState
		mockClusterMetadata *cluster.MockMetadata

		mockTaskGeneratorForNew *MockTaskGenerator

		logger log.Logger

		sourceCluster  string
		executionInfo  *persistencespb.WorkflowExecutionInfo
		stateRebuilder *MutableStateRebuilderImpl
	}

	testTaskGeneratorProvider struct {
		mockMutableState *MockMutableState

		mockTaskGenerator       *MockTaskGenerator
		mockTaskGeneratorForNew *MockTaskGenerator
	}
)

func TestStateBuilderSuite(t *testing.T) {
	s := new(stateBuilderSuite)
	suite.Run(t, s)
}

func (s *stateBuilderSuite) SetupSuite() {
	rand.Seed(time.Now().UnixNano())
}

func (s *stateBuilderSuite) TearDownSuite() {

}

func (s *stateBuilderSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockTaskGenerator = NewMockTaskGenerator(s.controller)
	s.mockMutableState = NewMockMutableState(s.controller)
	s.mockTaskGeneratorForNew = NewMockTaskGenerator(s.controller)

	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId: 0,
				RangeId: 1,
			}},
		tests.NewDynamicConfig(),
	)

	s.mockNamespaceCache = s.mockShard.Resource.NamespaceCache
	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata
	s.mockEventsCache = s.mockShard.MockEventsCache
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()

	s.logger = s.mockShard.GetLogger()
	s.executionInfo = &persistencespb.WorkflowExecutionInfo{
		VersionHistories: versionhistory.NewVersionHistories(&historyspb.VersionHistory{}),
	}
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(s.executionInfo).AnyTimes()

	taskGeneratorProvider = &testTaskGeneratorProvider{
		mockMutableState:        s.mockMutableState,
		mockTaskGenerator:       s.mockTaskGenerator,
		mockTaskGeneratorForNew: s.mockTaskGeneratorForNew,
	}
	s.stateRebuilder = NewMutableStateRebuilder(
		s.mockShard,
		s.logger,
		s.mockMutableState,
	)
	s.sourceCluster = "some random source cluster"
}

func (s *stateBuilderSuite) TearDownTest() {
	s.stateRebuilder = nil
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *stateBuilderSuite) mockUpdateVersion(events ...*historypb.HistoryEvent) {
	for _, event := range events {
		s.mockMutableState.EXPECT().UpdateCurrentVersion(event.GetVersion(), true)
	}
	s.mockTaskGenerator.EXPECT().GenerateActivityTimerTasks(
		timestamp.TimeValue(events[len(events)-1].GetEventTime()),
	).Return(nil)
	s.mockTaskGenerator.EXPECT().GenerateUserTimerTasks(
		timestamp.TimeValue(events[len(events)-1].GetEventTime()),
	).Return(nil)
	s.mockMutableState.EXPECT().SetHistoryBuilder(NewImmutableHistoryBuilder(events))
}

func (s *stateBuilderSuite) toHistory(events ...*historypb.HistoryEvent) []*historypb.HistoryEvent {
	return events
}

// workflow operations

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionStarted_NoCronSchedule() {
	cronSchedule := ""
	version := int64(1)
	requestID := uuid.New()
	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	s.executionInfo.WorkflowRunTimeout = timestamp.DurationFromSeconds(100)
	s.executionInfo.CronSchedule = cronSchedule

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
	startWorkflowAttribute := &historypb.WorkflowExecutionStartedEventAttributes{
		ParentWorkflowNamespace:   tests.ParentNamespace.String(),
		ParentWorkflowNamespaceId: tests.ParentNamespaceID.String(),
	}

	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    1,
		EventTime:  &now,
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: startWorkflowAttribute},
	}

	s.mockMutableState.EXPECT().ReplicateWorkflowExecutionStartedEvent(nil, execution, requestID, event).Return(nil)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateRecordWorkflowStartedTasks(
		timestamp.TimeValue(event.GetEventTime()),
		event,
	).Return(nil)
	s.mockTaskGenerator.EXPECT().GenerateWorkflowStartTasks(
		timestamp.TimeValue(event.GetEventTime()),
		event,
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyness()
	s.mockMutableState.EXPECT().SetHistoryTree(tests.RunID).Return(nil)

	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(event), nil)
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionStarted_WithCronSchedule() {
	cronSchedule := "* * * * *"
	version := int64(1)
	requestID := uuid.New()
	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	s.executionInfo.WorkflowRunTimeout = timestamp.DurationFromSeconds(100)
	s.executionInfo.CronSchedule = cronSchedule

	now := time.Now().UTC()
	eventType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
	startWorkflowAttribute := &historypb.WorkflowExecutionStartedEventAttributes{
		ParentWorkflowNamespace:   tests.ParentNamespace.String(),
		ParentWorkflowNamespaceId: tests.ParentNamespaceID.String(),
		Initiator:                 enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE,
		FirstWorkflowTaskBackoff:  timestamp.DurationPtr(backoff.GetBackoffForNextSchedule(cronSchedule, now, now)),
	}

	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    1,
		EventTime:  &now,
		EventType:  eventType,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: startWorkflowAttribute},
	}

	s.mockMutableState.EXPECT().ReplicateWorkflowExecutionStartedEvent(nil, execution, requestID, event).Return(nil)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateRecordWorkflowStartedTasks(
		timestamp.TimeValue(event.GetEventTime()),
		event,
	).Return(nil)
	s.mockTaskGenerator.EXPECT().GenerateWorkflowStartTasks(
		timestamp.TimeValue(event.GetEventTime()),
		event,
	).Return(nil)
	s.mockTaskGenerator.EXPECT().GenerateDelayedWorkflowTasks(
		timestamp.TimeValue(event.GetEventTime()),
		event,
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyness()
	s.mockMutableState.EXPECT().SetHistoryTree(tests.RunID).Return(nil)

	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(event), nil)
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionTimedOut() {
	version := int64(1)
	requestID := uuid.New()
	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  &now,
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionTimedOutEventAttributes{WorkflowExecutionTimedOutEventAttributes: &historypb.WorkflowExecutionTimedOutEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ReplicateWorkflowExecutionTimedoutEvent(event.GetEventId(), event).Return(nil)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		timestamp.TimeValue(event.GetEventTime()),
		false,
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyness()

	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(event), nil)
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionTerminated() {
	version := int64(1)
	requestID := uuid.New()
	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  &now,
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ReplicateWorkflowExecutionTerminatedEvent(event.GetEventId(), event).Return(nil)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		timestamp.TimeValue(event.GetEventTime()),
		false,
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyness()
	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(event), nil)
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionFailed() {
	version := int64(1)
	requestID := uuid.New()
	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  &now,
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ReplicateWorkflowExecutionFailedEvent(event.GetEventId(), event).Return(nil)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		timestamp.TimeValue(event.GetEventTime()),
		false,
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyness()

	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(event), nil)
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionCompleted() {
	version := int64(1)
	requestID := uuid.New()
	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  &now,
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ReplicateWorkflowExecutionCompletedEvent(event.GetEventId(), event).Return(nil)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		timestamp.TimeValue(event.GetEventTime()),
		false,
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyness()

	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(event), nil)
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionCanceled() {
	version := int64(1)
	requestID := uuid.New()
	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  &now,
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCanceledEventAttributes{WorkflowExecutionCanceledEventAttributes: &historypb.WorkflowExecutionCanceledEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ReplicateWorkflowExecutionCanceledEvent(event.GetEventId(), event).Return(nil)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		timestamp.TimeValue(event.GetEventTime()),
		false,
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyness()

	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(event), nil)
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionContinuedAsNew() {
	version := int64(1)
	requestID := uuid.New()
	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}
	parentWorkflowID := "some random parent workflow ID"
	parentRunID := uuid.New()
	parentInitiatedEventID := int64(144)

	now := time.Now().UTC()
	taskqueue := "some random taskqueue"
	workflowType := "some random workflow type"
	workflowTimeoutSecond := time.Duration(110) * time.Second
	taskTimeout := time.Duration(11) * time.Second
	newRunID := uuid.New()

	continueAsNewEvent := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: &now,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{WorkflowExecutionContinuedAsNewEventAttributes: &historypb.WorkflowExecutionContinuedAsNewEventAttributes{
			NewExecutionRunId: newRunID,
		}},
	}

	newRunStartedEvent := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   1,
		EventTime: &now,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
			ParentWorkflowNamespace:   tests.ParentNamespace.String(),
			ParentWorkflowNamespaceId: tests.ParentNamespaceID.String(),
			ParentWorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: parentWorkflowID,
				RunId:      parentRunID,
			},
			ParentInitiatedEventId:   parentInitiatedEventID,
			WorkflowExecutionTimeout: &workflowTimeoutSecond,
			WorkflowTaskTimeout:      &taskTimeout,
			TaskQueue:                &taskqueuepb.TaskQueue{Name: taskqueue},
			WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
		}},
	}

	newRunSignalEvent := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   2,
		EventTime: &now,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
			SignalName: "some random signal name",
			Input:      payloads.EncodeString("some random signal input"),
			Identity:   "some random identity",
		}},
	}

	newRunWorkflowTaskAttempt := int32(123)
	newRunWorkflowTaskEvent := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   3,
		EventTime: &now,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskqueue},
			StartToCloseTimeout: &taskTimeout,
			Attempt:             newRunWorkflowTaskAttempt,
		}},
	}
	newRunEvents := []*historypb.HistoryEvent{
		newRunStartedEvent, newRunSignalEvent, newRunWorkflowTaskEvent,
	}

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, continueAsNewEvent.GetVersion()).Return(s.sourceCluster).AnyTimes()
	s.mockMutableState.EXPECT().ReplicateWorkflowExecutionContinuedAsNewEvent(
		continueAsNewEvent.GetEventId(),
		continueAsNewEvent,
	).Return(nil)
	s.mockMutableState.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry).AnyTimes()
	s.mockUpdateVersion(continueAsNewEvent)
	s.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		timestamp.TimeValue(continueAsNewEvent.GetEventTime()),
		false,
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyness()

	// new workflow namespace
	s.mockNamespaceCache.EXPECT().GetNamespace(tests.ParentNamespace).Return(tests.GlobalParentNamespaceEntry, nil).AnyTimes()
	// task for the new workflow
	s.mockTaskGeneratorForNew.EXPECT().GenerateRecordWorkflowStartedTasks(
		timestamp.TimeValue(newRunStartedEvent.GetEventTime()),
		newRunStartedEvent,
	).Return(nil)
	s.mockTaskGeneratorForNew.EXPECT().GenerateWorkflowStartTasks(
		timestamp.TimeValue(newRunStartedEvent.GetEventTime()),
		newRunStartedEvent,
	).Return(nil)
	s.mockTaskGeneratorForNew.EXPECT().GenerateScheduleWorkflowTaskTasks(
		timestamp.TimeValue(newRunWorkflowTaskEvent.GetEventTime()),
		newRunWorkflowTaskEvent.GetEventId(),
	).Return(nil)
	s.mockTaskGeneratorForNew.EXPECT().GenerateActivityTimerTasks(
		timestamp.TimeValue(newRunEvents[len(newRunEvents)-1].GetEventTime()),
	).Return(nil)
	s.mockTaskGeneratorForNew.EXPECT().GenerateUserTimerTasks(
		timestamp.TimeValue(newRunEvents[len(newRunEvents)-1].GetEventTime()),
	).Return(nil)

	newRunStateBuilder, err := s.stateRebuilder.ApplyEvents(
		tests.NamespaceID, requestID, execution, s.toHistory(continueAsNewEvent), newRunEvents,
	)
	s.Nil(err)
	s.NotNil(newRunStateBuilder)
	s.Equal(continueAsNewEvent.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionContinuedAsNew_EmptyNewRunHistory() {
	version := int64(1)
	requestID := uuid.New()
	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	newRunID := uuid.New()

	continueAsNewEvent := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: &now,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{WorkflowExecutionContinuedAsNewEventAttributes: &historypb.WorkflowExecutionContinuedAsNewEventAttributes{
			NewExecutionRunId: newRunID,
		}},
	}

	s.mockMutableState.EXPECT().ReplicateWorkflowExecutionContinuedAsNewEvent(
		continueAsNewEvent.GetEventId(),
		continueAsNewEvent,
	).Return(nil)
	s.mockMutableState.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry).AnyTimes()
	s.mockUpdateVersion(continueAsNewEvent)
	s.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		timestamp.TimeValue(continueAsNewEvent.GetEventTime()),
		false,
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyness()

	// new workflow namespace
	s.mockNamespaceCache.EXPECT().GetNamespace(tests.ParentNamespace).Return(tests.GlobalParentNamespaceEntry, nil).AnyTimes()
	newRunStateBuilder, err := s.stateRebuilder.ApplyEvents(
		tests.NamespaceID, requestID, execution, s.toHistory(continueAsNewEvent), nil,
	)
	s.Nil(err)
	s.Nil(newRunStateBuilder)
	s.Equal(continueAsNewEvent.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionSignaled() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  &now,
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{}},
	}
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ReplicateWorkflowExecutionSignaled(event).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyness()

	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(event), nil)
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionCancelRequested() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}
	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  &now,
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCancelRequestedEventAttributes{WorkflowExecutionCancelRequestedEventAttributes: &historypb.WorkflowExecutionCancelRequestedEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ReplicateWorkflowExecutionCancelRequestedEvent(event).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyness()

	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(event), nil)
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeUpsertWorkflowSearchAttributes() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  &now,
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_UpsertWorkflowSearchAttributesEventAttributes{UpsertWorkflowSearchAttributesEventAttributes: &historypb.UpsertWorkflowSearchAttributesEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateUpsertWorkflowSearchAttributesEvent(event).Return()
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateWorkflowSearchAttrTasks(
		timestamp.TimeValue(event.GetEventTime()),
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyness()

	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(event), nil)
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeMarkerRecorded() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_MARKER_RECORDED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  &now,
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_MarkerRecordedEventAttributes{MarkerRecordedEventAttributes: &historypb.MarkerRecordedEventAttributes{}},
	}
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyness()

	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(event), nil)
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

// workflow task operations
func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowTaskScheduled() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	taskqueue := &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, Name: "some random taskqueue"}
	timeout := time.Duration(11) * time.Second
	evenType := enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
	workflowTaskAttempt := int32(111)
	event := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: &now,
		EventType: evenType,
		Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
			TaskQueue:           taskqueue,
			StartToCloseTimeout: &timeout,
			Attempt:             workflowTaskAttempt,
		}},
	}
	wt := &WorkflowTaskInfo{
		Version:             event.GetVersion(),
		ScheduledEventID:    event.GetEventId(),
		StartedEventID:      common.EmptyEventID,
		RequestID:           emptyUUID,
		WorkflowTaskTimeout: &timeout,
		TaskQueue:           taskqueue,
		Attempt:             workflowTaskAttempt,
	}
	s.executionInfo.TaskQueue = taskqueue.GetName()
	s.mockMutableState.EXPECT().ReplicateWorkflowTaskScheduledEvent(
		event.GetVersion(), event.GetEventId(), taskqueue, &timeout, workflowTaskAttempt, event.GetEventTime(), event.GetEventTime(),
	).Return(wt, nil)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateScheduleWorkflowTaskTasks(
		timestamp.TimeValue(event.GetEventTime()),
		wt.ScheduledEventID,
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyness()

	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(event), nil)
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}
func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowTaskStarted() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	taskqueue := &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, Name: "some random taskqueue"}
	timeout := time.Second * 11
	scheduledEventID := int64(111)
	workflowTaskRequestID := uuid.New()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED
	event := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: &now,
		EventType: evenType,
		Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
			ScheduledEventId: scheduledEventID,
			RequestId:        workflowTaskRequestID,
		}},
	}
	wt := &WorkflowTaskInfo{
		Version:             event.GetVersion(),
		ScheduledEventID:    scheduledEventID,
		StartedEventID:      event.GetEventId(),
		RequestID:           workflowTaskRequestID,
		WorkflowTaskTimeout: &timeout,
		TaskQueue:           taskqueue,
		Attempt:             1,
	}
	s.mockMutableState.EXPECT().ReplicateWorkflowTaskStartedEvent(
		(*WorkflowTaskInfo)(nil), event.GetVersion(), scheduledEventID, event.GetEventId(), workflowTaskRequestID, timestamp.TimeValue(event.GetEventTime()),
	).Return(wt, nil)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateStartWorkflowTaskTasks(
		timestamp.TimeValue(event.GetEventTime()),
		wt.ScheduledEventID,
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyness()

	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(event), nil)
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowTaskTimedOut() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	scheduledEventID := int64(12)
	startedEventID := int64(28)
	evenType := enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT
	event := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: &now,
		EventType: evenType,
		Attributes: &historypb.HistoryEvent_WorkflowTaskTimedOutEventAttributes{WorkflowTaskTimedOutEventAttributes: &historypb.WorkflowTaskTimedOutEventAttributes{
			ScheduledEventId: scheduledEventID,
			StartedEventId:   startedEventID,
			TimeoutType:      enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		}},
	}
	s.mockMutableState.EXPECT().ReplicateWorkflowTaskTimedOutEvent(enumspb.TIMEOUT_TYPE_START_TO_CLOSE).Return(nil)
	taskqueue := &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, Name: "some random taskqueue"}
	newScheduledEventID := int64(233)
	s.executionInfo.TaskQueue = taskqueue.GetName()
	s.mockMutableState.EXPECT().ReplicateTransientWorkflowTaskScheduled().Return(&WorkflowTaskInfo{
		Version:          version,
		ScheduledEventID: newScheduledEventID,
		TaskQueue:        taskqueue,
	}, nil)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateScheduleWorkflowTaskTasks(
		timestamp.TimeValue(event.GetEventTime()),
		newScheduledEventID,
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyness()

	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(event), nil)
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowTaskFailed() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	scheduledEventID := int64(12)
	startedEventID := int64(28)
	evenType := enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED
	event := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: &now,
		EventType: evenType,
		Attributes: &historypb.HistoryEvent_WorkflowTaskFailedEventAttributes{WorkflowTaskFailedEventAttributes: &historypb.WorkflowTaskFailedEventAttributes{
			ScheduledEventId: scheduledEventID,
			StartedEventId:   startedEventID,
		}},
	}
	s.mockMutableState.EXPECT().ReplicateWorkflowTaskFailedEvent().Return(nil)
	taskqueue := &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, Name: "some random taskqueue"}
	newScheduledEventID := int64(233)
	s.executionInfo.TaskQueue = taskqueue.GetName()
	s.mockMutableState.EXPECT().ReplicateTransientWorkflowTaskScheduled().Return(&WorkflowTaskInfo{
		Version:          version,
		ScheduledEventID: newScheduledEventID,
		TaskQueue:        taskqueue,
	}, nil)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateScheduleWorkflowTaskTasks(
		timestamp.TimeValue(event.GetEventTime()),
		newScheduledEventID,
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyness()

	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(event), nil)
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowTaskCompleted() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	scheduledEventID := int64(12)
	startedEventID := int64(28)
	evenType := enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED
	event := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: &now,
		EventType: evenType,
		Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
			ScheduledEventId: scheduledEventID,
			StartedEventId:   startedEventID,
		}},
	}
	s.mockMutableState.EXPECT().ReplicateWorkflowTaskCompletedEvent(event).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyness()

	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(event), nil)
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

// user timer operations

func (s *stateBuilderSuite) TestApplyEvents_EventTypeTimerStarted() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	timerID := "timer ID"
	timeoutSecond := time.Duration(10) * time.Second
	evenType := enumspb.EVENT_TYPE_TIMER_STARTED
	event := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: &now,
		EventType: evenType,
		Attributes: &historypb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
			TimerId:            timerID,
			StartToFireTimeout: &timeoutSecond,
		}},
	}
	expiryTime := timestamp.TimeValue(event.GetEventTime()).Add(timeoutSecond)
	ti := &persistencespb.TimerInfo{
		Version:        event.GetVersion(),
		TimerId:        timerID,
		ExpiryTime:     &expiryTime,
		StartedEventId: event.GetEventId(),
		TaskStatus:     TimerTaskStatusNone,
	}
	s.mockMutableState.EXPECT().ReplicateTimerStartedEvent(event).Return(ti, nil)
	s.mockUpdateVersion(event)
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyness()

	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(event), nil)
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeTimerFired() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_TIMER_FIRED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  &now,
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_TimerFiredEventAttributes{TimerFiredEventAttributes: &historypb.TimerFiredEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ReplicateTimerFiredEvent(event).Return(nil)
	s.mockUpdateVersion(event)
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyness()

	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(event), nil)
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeTimerCanceled() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()

	evenType := enumspb.EVENT_TYPE_TIMER_CANCELED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  &now,
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_TimerCanceledEventAttributes{TimerCanceledEventAttributes: &historypb.TimerCanceledEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateTimerCanceledEvent(event).Return(nil)
	s.mockUpdateVersion(event)
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyness()

	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(event), nil)
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

// activity operations

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskScheduled() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	activityID := "activity ID"
	taskqueue := "some random taskqueue"
	timeoutSecond := 10 * time.Second
	evenType := enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  &now,
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{}},
	}

	ai := &persistencespb.ActivityInfo{
		Version:                 event.GetVersion(),
		ScheduledEventId:        event.GetEventId(),
		ScheduledEventBatchId:   event.GetEventId(),
		ScheduledTime:           event.GetEventTime(),
		StartedEventId:          common.EmptyEventID,
		StartedTime:             timestamp.TimePtr(time.Time{}),
		ActivityId:              activityID,
		ScheduleToStartTimeout:  &timeoutSecond,
		ScheduleToCloseTimeout:  &timeoutSecond,
		StartToCloseTimeout:     &timeoutSecond,
		HeartbeatTimeout:        &timeoutSecond,
		CancelRequested:         false,
		CancelRequestId:         common.EmptyEventID,
		LastHeartbeatUpdateTime: timestamp.TimePtr(time.Time{}),
		TimerTaskStatus:         TimerTaskStatusNone,
		TaskQueue:               taskqueue,
	}
	s.executionInfo.TaskQueue = taskqueue
	s.mockMutableState.EXPECT().ReplicateActivityTaskScheduledEvent(event.GetEventId(), event).Return(ai, nil)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateActivityTasks(
		timestamp.TimeValue(event.GetEventTime()),
		event,
	).Return(nil)
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyness()

	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(event), nil)
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskStarted() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	taskqueue := "some random taskqueue"
	evenType := enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
	scheduledEvent := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  &now,
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{}},
	}

	evenType = enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED
	startedEvent := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    scheduledEvent.GetEventId() + 1,
		EventTime:  timestamp.TimePtr(timestamp.TimeValue(scheduledEvent.GetEventTime()).Add(1000 * time.Nanosecond)),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{}},
	}

	s.executionInfo.TaskQueue = taskqueue
	s.mockMutableState.EXPECT().ReplicateActivityTaskStartedEvent(startedEvent).Return(nil)
	s.mockUpdateVersion(startedEvent)
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyness()

	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(startedEvent), nil)
	s.Nil(err)
	s.Equal(startedEvent.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskTimedOut() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  &now,
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ActivityTaskTimedOutEventAttributes{ActivityTaskTimedOutEventAttributes: &historypb.ActivityTaskTimedOutEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ReplicateActivityTaskTimedOutEvent(event).Return(nil)
	s.mockUpdateVersion(event)
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	//	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyness()

	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(event), nil)
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskFailed() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  &now,
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ActivityTaskFailedEventAttributes{ActivityTaskFailedEventAttributes: &historypb.ActivityTaskFailedEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ReplicateActivityTaskFailedEvent(event).Return(nil)
	s.mockUpdateVersion(event)
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyness()

	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(event), nil)
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskCompleted() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  &now,
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ReplicateActivityTaskCompletedEvent(event).Return(nil)
	s.mockUpdateVersion(event)
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyness()

	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(event), nil)
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskCancelRequested() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  &now,
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ActivityTaskCancelRequestedEventAttributes{ActivityTaskCancelRequestedEventAttributes: &historypb.ActivityTaskCancelRequestedEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateActivityTaskCancelRequestedEvent(event).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyness()

	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(event), nil)
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskCanceled() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  &now,
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ActivityTaskCanceledEventAttributes{ActivityTaskCanceledEventAttributes: &historypb.ActivityTaskCanceledEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ReplicateActivityTaskCanceledEvent(event).Return(nil)
	s.mockUpdateVersion(event)
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyness()

	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(event), nil)
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

// child workflow operations

func (s *stateBuilderSuite) TestApplyEvents_EventTypeStartChildWorkflowExecutionInitiated() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}
	targetWorkflowID := "some random target workflow ID"

	now := time.Now().UTC()
	createRequestID := uuid.New()
	evenType := enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED
	event := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: &now,
		EventType: evenType,
		Attributes: &historypb.HistoryEvent_StartChildWorkflowExecutionInitiatedEventAttributes{StartChildWorkflowExecutionInitiatedEventAttributes: &historypb.StartChildWorkflowExecutionInitiatedEventAttributes{
			Namespace:   tests.TargetNamespace.String(),
			NamespaceId: tests.TargetNamespaceID.String(),
			WorkflowId:  targetWorkflowID,
		}},
	}

	ci := &persistencespb.ChildExecutionInfo{
		Version:               event.GetVersion(),
		InitiatedEventId:      event.GetEventId(),
		InitiatedEventBatchId: event.GetEventId(),
		StartedEventId:        common.EmptyEventID,
		CreateRequestId:       createRequestID,
		Namespace:             tests.TargetNamespace.String(),
		NamespaceId:           tests.TargetNamespaceID.String(),
	}

	// the create request ID is generated inside, cannot assert equal
	s.mockMutableState.EXPECT().ReplicateStartChildWorkflowExecutionInitiatedEvent(
		event.GetEventId(), event, gomock.Any(),
	).Return(ci, nil)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateChildWorkflowTasks(
		timestamp.TimeValue(event.GetEventTime()),
		event,
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyness()

	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(event), nil)
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeStartChildWorkflowExecutionFailed() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  &now,
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_StartChildWorkflowExecutionFailedEventAttributes{StartChildWorkflowExecutionFailedEventAttributes: &historypb.StartChildWorkflowExecutionFailedEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateStartChildWorkflowExecutionFailedEvent(event).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyness()

	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(event), nil)
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionStarted() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  &now,
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionStartedEventAttributes{ChildWorkflowExecutionStartedEventAttributes: &historypb.ChildWorkflowExecutionStartedEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateChildWorkflowExecutionStartedEvent(event, nil).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyness()

	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(event), nil)
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionTimedOut() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  &now,
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionTimedOutEventAttributes{ChildWorkflowExecutionTimedOutEventAttributes: &historypb.ChildWorkflowExecutionTimedOutEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateChildWorkflowExecutionTimedOutEvent(event).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyness()

	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(event), nil)
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionTerminated() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  &now,
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionTerminatedEventAttributes{ChildWorkflowExecutionTerminatedEventAttributes: &historypb.ChildWorkflowExecutionTerminatedEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateChildWorkflowExecutionTerminatedEvent(event).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyness()

	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(event), nil)
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionFailed() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  &now,
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionFailedEventAttributes{ChildWorkflowExecutionFailedEventAttributes: &historypb.ChildWorkflowExecutionFailedEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateChildWorkflowExecutionFailedEvent(event).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyness()

	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(event), nil)
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionCompleted() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  &now,
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionCompletedEventAttributes{ChildWorkflowExecutionCompletedEventAttributes: &historypb.ChildWorkflowExecutionCompletedEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateChildWorkflowExecutionCompletedEvent(event).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyness()

	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(event), nil)
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

// cancel external workflow operations

func (s *stateBuilderSuite) TestApplyEvents_EventTypeRequestCancelExternalWorkflowExecutionInitiated() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	targetWorkflowID := "some random target workflow ID"
	targetRunID := uuid.New()
	childWorkflowOnly := true

	now := time.Now().UTC()
	cancellationRequestID := uuid.New()
	control := "some random control"
	evenType := enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED
	event := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: &now,
		EventType: evenType,
		Attributes: &historypb.HistoryEvent_RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{RequestCancelExternalWorkflowExecutionInitiatedEventAttributes: &historypb.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
			Namespace:   tests.TargetNamespace.String(),
			NamespaceId: tests.TargetNamespaceID.String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: targetWorkflowID,
				RunId:      targetRunID,
			},
			ChildWorkflowOnly: childWorkflowOnly,
			Control:           control,
		}},
	}
	rci := &persistencespb.RequestCancelInfo{
		Version:          event.GetVersion(),
		InitiatedEventId: event.GetEventId(),
		CancelRequestId:  cancellationRequestID,
	}

	// the cancellation request ID is generated inside, cannot assert equal
	s.mockMutableState.EXPECT().ReplicateRequestCancelExternalWorkflowExecutionInitiatedEvent(
		event.GetEventId(), event, gomock.Any(),
	).Return(rci, nil)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateRequestCancelExternalTasks(
		timestamp.TimeValue(event.GetEventTime()),
		event,
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyness()

	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(event), nil)
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeRequestCancelExternalWorkflowExecutionFailed() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  &now,
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_RequestCancelExternalWorkflowExecutionFailedEventAttributes{RequestCancelExternalWorkflowExecutionFailedEventAttributes: &historypb.RequestCancelExternalWorkflowExecutionFailedEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateRequestCancelExternalWorkflowExecutionFailedEvent(event).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyness()

	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(event), nil)
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeExternalWorkflowExecutionCancelRequested() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  &now,
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ExternalWorkflowExecutionCancelRequestedEventAttributes{ExternalWorkflowExecutionCancelRequestedEventAttributes: &historypb.ExternalWorkflowExecutionCancelRequestedEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateExternalWorkflowExecutionCancelRequested(event).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyness()

	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(event), nil)
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionCanceled() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  &now,
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionCanceledEventAttributes{ChildWorkflowExecutionCanceledEventAttributes: &historypb.ChildWorkflowExecutionCanceledEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateChildWorkflowExecutionCanceledEvent(event).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyness()

	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(event), nil)
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

// signal external workflow operations

func (s *stateBuilderSuite) TestApplyEvents_EventTypeSignalExternalWorkflowExecutionInitiated() {
	version := int64(1)
	requestID := uuid.New()
	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}
	targetWorkflowID := "some random target workflow ID"
	targetRunID := uuid.New()
	childWorkflowOnly := true

	now := time.Now().UTC()
	signalRequestID := uuid.New()
	signalName := "some random signal name"
	signalInput := payloads.EncodeString("some random signal input")
	signalHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{"signal header key": payload.EncodeString("signal header value")},
	}
	control := "some random control"
	evenType := enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED
	event := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: &now,
		EventType: evenType,
		Attributes: &historypb.HistoryEvent_SignalExternalWorkflowExecutionInitiatedEventAttributes{SignalExternalWorkflowExecutionInitiatedEventAttributes: &historypb.SignalExternalWorkflowExecutionInitiatedEventAttributes{
			Namespace:   tests.TargetNamespace.String(),
			NamespaceId: tests.TargetNamespaceID.String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: targetWorkflowID,
				RunId:      targetRunID,
			},
			SignalName:        signalName,
			Input:             signalInput,
			ChildWorkflowOnly: childWorkflowOnly,
			Header:            signalHeader,
			Control:           control,
		}},
	}
	si := &persistencespb.SignalInfo{
		Version:          event.GetVersion(),
		InitiatedEventId: event.GetEventId(),
		RequestId:        signalRequestID,
	}

	// the cancellation request ID is generated inside, cannot assert equal
	s.mockMutableState.EXPECT().ReplicateSignalExternalWorkflowExecutionInitiatedEvent(
		event.GetEventId(), event, gomock.Any(),
	).Return(si, nil)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateSignalExternalTasks(
		timestamp.TimeValue(event.GetEventTime()),
		event,
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyness()

	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(event), nil)
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeSignalExternalWorkflowExecutionFailed() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  &now,
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_SignalExternalWorkflowExecutionFailedEventAttributes{SignalExternalWorkflowExecutionFailedEventAttributes: &historypb.SignalExternalWorkflowExecutionFailedEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateSignalExternalWorkflowExecutionFailedEvent(event).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyness()

	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(event), nil)
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeExternalWorkflowExecutionSignaled() {
	version := int64(1)
	requestID := uuid.New()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  &now,
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ExternalWorkflowExecutionSignaledEventAttributes{ExternalWorkflowExecutionSignaledEventAttributes: &historypb.ExternalWorkflowExecutionSignaledEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ReplicateExternalWorkflowExecutionSignaled(event).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyness()

	_, err := s.stateRebuilder.ApplyEvents(tests.NamespaceID, requestID, execution, s.toHistory(event), nil)
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEventsNewEventsNotHandled() {
	eventTypes := enumspb.EventType_value
	s.Equal(44, len(eventTypes), "If you see this error, you are adding new event type. "+
		"Before updating the number to make this test pass, please make sure you update stateBuilderImpl.ApplyEvents method "+
		"to handle the new command type. Otherwise cross dc will not work on the new event.")
}

func (p *testTaskGeneratorProvider) NewTaskGenerator(
	_ shard.Context,
	mutableState MutableState,
) TaskGenerator {
	if mutableState == p.mockMutableState {
		return p.mockTaskGenerator
	}
	return p.mockTaskGeneratorForNew
}
