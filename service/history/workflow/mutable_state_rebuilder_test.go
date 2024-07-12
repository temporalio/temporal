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
	"context"
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
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/testing/protomock"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/historybuilder"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
)

type (
	stateBuilderSuite struct {
		suite.Suite
		*require.Assertions

		controller           *gomock.Controller
		mockShard            *shard.ContextTest
		mockEventsCache      *events.MockCache
		mockNamespaceCache   *namespace.MockRegistry
		mockTaskGenerator    *MockTaskGenerator
		mockMutableState     *MockMutableState
		mockClusterMetadata  *cluster.MockMetadata
		stateMachineRegistry *hsm.Registry

		logger log.Logger

		sourceCluster  string
		executionInfo  *persistencespb.WorkflowExecutionInfo
		stateRebuilder *MutableStateRebuilderImpl
	}

	testTaskGeneratorProvider struct {
		mockMutableState  *MockMutableState
		mockTaskGenerator *MockTaskGenerator
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
	s.mockTaskGenerator = NewMockTaskGenerator(s.controller)
	s.mockMutableState = NewMockMutableState(s.controller)

	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)

	reg := hsm.NewRegistry()
	s.NoError(RegisterStateMachine(reg))
	s.NoError(nexusoperations.RegisterStateMachines(reg))
	s.NoError(nexusoperations.RegisterEventDefinitions(reg))
	s.NoError(nexusoperations.RegisterTaskSerializers(reg))
	s.mockShard.SetStateMachineRegistry(reg)
	s.stateMachineRegistry = reg

	root, err := hsm.NewRoot(reg, StateMachineType, s.mockMutableState, make(map[string]*persistencespb.StateMachineMap), s.mockMutableState)
	s.NoError(err)
	s.mockMutableState.EXPECT().HSM().Return(root).AnyTimes()

	s.mockNamespaceCache = s.mockShard.Resource.NamespaceCache
	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata
	s.mockEventsCache = s.mockShard.MockEventsCache
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()

	s.logger = s.mockShard.GetLogger()
	s.executionInfo = &persistencespb.WorkflowExecutionInfo{
		VersionHistories:                 versionhistory.NewVersionHistories(&historyspb.VersionHistory{}),
		FirstExecutionRunId:              uuid.New(),
		WorkflowExecutionTimerTaskStatus: TimerTaskStatusCreated,
	}
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(s.executionInfo).AnyTimes()
	s.mockMutableState.EXPECT().GetCurrentVersion().Return(int64(1)).AnyTimes()
	s.mockMutableState.EXPECT().NextTransitionCount().Return(int64(2)).AnyTimes()

	taskGeneratorProvider = &testTaskGeneratorProvider{
		mockMutableState:  s.mockMutableState,
		mockTaskGenerator: s.mockTaskGenerator,
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
	s.mockTaskGenerator.EXPECT().GenerateActivityTimerTasks().Return(nil)
	s.mockTaskGenerator.EXPECT().GenerateUserTimerTasks().Return(nil)
	s.mockTaskGenerator.EXPECT().GenerateDirtySubStateMachineTasks(s.stateMachineRegistry).Return(nil).AnyTimes()
	s.mockMutableState.EXPECT().SetHistoryBuilder(historybuilder.NewImmutable(events))
}

func (s *stateBuilderSuite) toHistory(eventss ...*historypb.HistoryEvent) [][]*historypb.HistoryEvent {
	return [][]*historypb.HistoryEvent{eventss}
}

// workflow operations

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionStarted_NoCronSchedule() {
	cronSchedule := ""
	version := int64(1)
	requestID := uuid.New()
	execution := &commonpb.WorkflowExecution{
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
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: startWorkflowAttribute},
	}

	s.mockMutableState.EXPECT().ApplyWorkflowExecutionStartedEvent(nil, execution, requestID, protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateRecordWorkflowStartedTasks(
		protomock.Eq(event),
	).Return(nil)
	s.mockTaskGenerator.EXPECT().GenerateWorkflowStartTasks(
		protomock.Eq(event),
	).Return(int32(TimerTaskStatusCreated), nil)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()
	s.mockMutableState.EXPECT().SetHistoryTree(nil, timestamp.DurationFromSeconds(100), tests.RunID).Return(nil)

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionStarted_WithCronSchedule() {
	cronSchedule := "* * * * *"
	version := int64(1)
	requestID := uuid.New()
	execution := &commonpb.WorkflowExecution{
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
		FirstWorkflowTaskBackoff:  durationpb.New(backoff.GetBackoffForNextSchedule(cronSchedule, now, now)),
	}

	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    1,
		EventTime:  timestamppb.New(now),
		EventType:  eventType,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: startWorkflowAttribute},
	}

	s.mockMutableState.EXPECT().ApplyWorkflowExecutionStartedEvent(nil, protomock.Eq(execution), requestID, protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateRecordWorkflowStartedTasks(
		protomock.Eq(event),
	).Return(nil)
	s.mockTaskGenerator.EXPECT().GenerateWorkflowStartTasks(
		protomock.Eq(event),
	).Return(int32(TimerTaskStatusCreated), nil)
	s.mockTaskGenerator.EXPECT().GenerateDelayedWorkflowTasks(
		protomock.Eq(event),
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()
	s.mockMutableState.EXPECT().SetHistoryTree(nil, timestamp.DurationFromSeconds(100), tests.RunID).Return(nil)

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionTimedOut() {
	version := int64(1)
	requestID := uuid.New()
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionTimedOutEventAttributes{WorkflowExecutionTimedOutEventAttributes: &historypb.WorkflowExecutionTimedOutEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ApplyWorkflowExecutionTimedoutEvent(event.GetEventId(), protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		now,
		false,
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionTimedOut_WithNewRunHistory() {
	version := int64(1)
	requestID := uuid.New()
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}
	newRunID := uuid.New()

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT
	event := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionTimedOutEventAttributes{
			WorkflowExecutionTimedOutEventAttributes: &historypb.WorkflowExecutionTimedOutEventAttributes{
				NewExecutionRunId: newRunID,
			},
		},
	}

	newRunStartedEvent := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   1,
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionTimeout:        durationpb.New(100 * time.Second),
			WorkflowRunTimeout:              durationpb.New(100 * time.Second),
			WorkflowTaskTimeout:             durationpb.New(10 * time.Second),
			TaskQueue:                       &taskqueuepb.TaskQueue{Name: "some random taskqueue"},
			WorkflowType:                    &commonpb.WorkflowType{Name: "some random workflow type"},
			FirstWorkflowTaskBackoff:        durationpb.New(10 * time.Second),
			FirstExecutionRunId:             s.mockMutableState.GetExecutionInfo().FirstExecutionRunId,
			WorkflowExecutionExpirationTime: timestamppb.New(now.Add(100 * time.Second)),
			ContinuedExecutionRunId:         execution.RunId,
		}},
	}
	newRunEvents := []*historypb.HistoryEvent{newRunStartedEvent}

	s.mockMutableState.EXPECT().ApplyWorkflowExecutionTimedoutEvent(event.GetEventId(), protomock.Eq(event)).Return(nil)
	s.mockMutableState.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		CreateRequestId: uuid.New(),
		State:           enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		Status:          enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
	})
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		now,
		false,
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	newRunStateBuilder, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), newRunEvents, newRunID)
	s.Nil(err)
	s.NotNil(newRunStateBuilder)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)

	newRunTasks := newRunStateBuilder.PopTasks()
	s.Len(newRunTasks[tasks.CategoryTimer], 1)      // backoffTimer
	s.Len(newRunTasks[tasks.CategoryVisibility], 1) // recordWorkflowStarted
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionTerminated() {
	version := int64(1)
	requestID := uuid.New()
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ApplyWorkflowExecutionTerminatedEvent(event.GetEventId(), protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		now,
		false,
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()
	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionTerminated_WithNewRunHistory() {
	version := int64(1)
	requestID := uuid.New()
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{}},
	}

	newRunStartedEvent := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   1,
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionTimeout:        durationpb.New(100 * time.Second),
			WorkflowRunTimeout:              durationpb.New(10 * time.Second),
			WorkflowTaskTimeout:             durationpb.New(10 * time.Second),
			TaskQueue:                       &taskqueuepb.TaskQueue{Name: "some random taskqueue"},
			WorkflowType:                    &commonpb.WorkflowType{Name: "some random workflow type"},
			FirstWorkflowTaskBackoff:        durationpb.New(10 * time.Second),
			FirstExecutionRunId:             uuid.New(),
			WorkflowExecutionExpirationTime: timestamppb.New(now.Add(100 * time.Second)),
		}},
	}
	newRunEvents := []*historypb.HistoryEvent{newRunStartedEvent}

	s.mockMutableState.EXPECT().ApplyWorkflowExecutionTerminatedEvent(event.GetEventId(), protomock.Eq(event)).Return(nil)
	s.mockMutableState.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		CreateRequestId: uuid.New(),
		State:           enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		Status:          enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
	})
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		now,
		false,
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	newRunStateBuilder, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), newRunEvents, uuid.New())
	s.Nil(err)
	s.NotNil(newRunStateBuilder)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)

	newRunTasks := newRunStateBuilder.PopTasks()
	s.Len(newRunTasks[tasks.CategoryTimer], 3)      // backoff timer, runTimeout timer, executionTimeout timer
	s.Len(newRunTasks[tasks.CategoryVisibility], 1) // recordWorkflowStarted
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionFailed() {
	version := int64(1)
	requestID := uuid.New()
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ApplyWorkflowExecutionFailedEvent(event.GetEventId(), protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		now,
		false,
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionFailed_WithNewRunHistory() {
	version := int64(1)
	requestID := uuid.New()
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}
	newRunID := uuid.New()

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED
	event := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{
			WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{
				NewExecutionRunId: newRunID,
			},
		},
	}

	newRunStartedEvent := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   1,
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionTimeout:        durationpb.New(100 * time.Second),
			WorkflowRunTimeout:              durationpb.New(10 * time.Second),
			WorkflowTaskTimeout:             durationpb.New(10 * time.Second),
			TaskQueue:                       &taskqueuepb.TaskQueue{Name: "some random taskqueue"},
			WorkflowType:                    &commonpb.WorkflowType{Name: "some random workflow type"},
			FirstWorkflowTaskBackoff:        durationpb.New(10 * time.Second),
			FirstExecutionRunId:             s.mockMutableState.GetExecutionInfo().FirstExecutionRunId,
			WorkflowExecutionExpirationTime: timestamppb.New(now.Add(100 * time.Second)),
			ContinuedExecutionRunId:         execution.RunId,
		}},
	}
	newRunEvents := []*historypb.HistoryEvent{newRunStartedEvent}

	s.mockMutableState.EXPECT().ApplyWorkflowExecutionFailedEvent(event.GetEventId(), protomock.Eq(event)).Return(nil)
	s.mockMutableState.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		CreateRequestId: uuid.New(),
		State:           enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		Status:          enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
	})
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		now,
		false,
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	newRunStateBuilder, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), newRunEvents, newRunID)
	s.Nil(err)
	s.NotNil(newRunStateBuilder)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)

	newRunTasks := newRunStateBuilder.PopTasks()
	s.Len(newRunTasks[tasks.CategoryTimer], 2)      // backoffTimer, runTimeout timer
	s.Len(newRunTasks[tasks.CategoryVisibility], 1) // recordWorkflowStarted
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionCompleted() {
	version := int64(1)
	requestID := uuid.New()
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ApplyWorkflowExecutionCompletedEvent(event.GetEventId(), event).Return(nil)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		now,
		false,
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionCompleted_WithNewRunHistory() {
	version := int64(1)
	requestID := uuid.New()
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}
	newRunID := uuid.New()

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
	event := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
			WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{
				NewExecutionRunId: newRunID,
			},
		},
	}

	newRunStartedEvent := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   1,
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionTimeout:        durationpb.New(100 * time.Second),
			WorkflowRunTimeout:              durationpb.New(100 * time.Second),
			WorkflowTaskTimeout:             durationpb.New(10 * time.Second),
			TaskQueue:                       &taskqueuepb.TaskQueue{Name: "some random taskqueue"},
			WorkflowType:                    &commonpb.WorkflowType{Name: "some random workflow type"},
			FirstExecutionRunId:             s.mockMutableState.GetExecutionInfo().FirstExecutionRunId,
			WorkflowExecutionExpirationTime: timestamppb.New(now.Add(100 * time.Second)),
			ContinuedExecutionRunId:         execution.RunId,
		}},
	}
	newRunEvents := []*historypb.HistoryEvent{newRunStartedEvent}

	s.mockMutableState.EXPECT().ApplyWorkflowExecutionCompletedEvent(event.GetEventId(), event).Return(nil)
	s.mockMutableState.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		CreateRequestId: uuid.New(),
		State:           enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		Status:          enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	})
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		now,
		false,
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	newRunStateBuilder, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), newRunEvents, newRunID)
	s.Nil(err)
	s.NotNil(newRunStateBuilder)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)

	newRunTasks := newRunStateBuilder.PopTasks()
	s.Len(newRunTasks[tasks.CategoryTimer], 0)
	s.Len(newRunTasks[tasks.CategoryVisibility], 1) // recordWorkflowStarted
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionCanceled() {
	version := int64(1)
	requestID := uuid.New()
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCanceledEventAttributes{WorkflowExecutionCanceledEventAttributes: &historypb.WorkflowExecutionCanceledEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ApplyWorkflowExecutionCanceledEvent(event.GetEventId(), event).Return(nil)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		now,
		false,
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionContinuedAsNew() {
	version := int64(1)
	requestID := uuid.New()
	execution := &commonpb.WorkflowExecution{
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
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{WorkflowExecutionContinuedAsNewEventAttributes: &historypb.WorkflowExecutionContinuedAsNewEventAttributes{
			NewExecutionRunId: newRunID,
		}},
	}

	newRunStartedEvent := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   1,
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
			ParentWorkflowNamespace:   tests.ParentNamespace.String(),
			ParentWorkflowNamespaceId: tests.ParentNamespaceID.String(),
			ParentWorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: parentWorkflowID,
				RunId:      parentRunID,
			},
			ParentInitiatedEventId:          parentInitiatedEventID,
			WorkflowExecutionTimeout:        durationpb.New(workflowTimeoutSecond),
			WorkflowTaskTimeout:             durationpb.New(taskTimeout),
			TaskQueue:                       &taskqueuepb.TaskQueue{Name: taskqueue},
			WorkflowType:                    &commonpb.WorkflowType{Name: workflowType},
			WorkflowExecutionExpirationTime: timestamppb.New(now.Add(workflowTimeoutSecond)),
			FirstExecutionRunId:             s.mockMutableState.GetExecutionInfo().FirstExecutionRunId,
			ContinuedExecutionRunId:         execution.RunId,
		}},
	}

	newRunSignalEvent := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   2,
		EventTime: timestamppb.New(now),
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
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskqueue},
			StartToCloseTimeout: durationpb.New(taskTimeout),
			Attempt:             newRunWorkflowTaskAttempt,
		}},
	}
	newRunEvents := []*historypb.HistoryEvent{
		newRunStartedEvent, newRunSignalEvent, newRunWorkflowTaskEvent,
	}

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, continueAsNewEvent.GetVersion()).Return(s.sourceCluster).AnyTimes()
	s.mockMutableState.EXPECT().ApplyWorkflowExecutionContinuedAsNewEvent(
		continueAsNewEvent.GetEventId(),
		protomock.Eq(continueAsNewEvent),
	).Return(nil)
	s.mockMutableState.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry).AnyTimes()
	s.mockMutableState.EXPECT().GetWorkflowKey().Return(definition.NewWorkflowKey(tests.GlobalNamespaceEntry.ID().String(), execution.WorkflowId, execution.RunId)).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		CreateRequestId: uuid.New(),
		State:           enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		Status:          enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
	})
	s.mockUpdateVersion(continueAsNewEvent)
	s.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		now,
		false,
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	// new workflow namespace
	s.mockNamespaceCache.EXPECT().GetNamespace(tests.ParentNamespace).Return(tests.GlobalParentNamespaceEntry, nil).AnyTimes()

	newRunStateBuilder, err := s.stateRebuilder.ApplyEvents(
		context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(continueAsNewEvent), newRunEvents, "",
	)
	s.Nil(err)
	s.NotNil(newRunStateBuilder)
	s.Equal(continueAsNewEvent.TaskId, s.executionInfo.LastEventTaskId)

	newRunTasks := newRunStateBuilder.PopTasks()
	s.Empty(newRunTasks[tasks.CategoryTimer])
	s.Len(newRunTasks[tasks.CategoryVisibility], 1) // recordWorkflowStarted
	s.Len(newRunTasks[tasks.CategoryTransfer], 1)   // workflow task
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionContinuedAsNew_EmptyNewRunHistory() {
	version := int64(1)
	requestID := uuid.New()
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	newRunID := uuid.New()

	continueAsNewEvent := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{WorkflowExecutionContinuedAsNewEventAttributes: &historypb.WorkflowExecutionContinuedAsNewEventAttributes{
			NewExecutionRunId: newRunID,
		}},
	}

	s.mockMutableState.EXPECT().ApplyWorkflowExecutionContinuedAsNewEvent(
		continueAsNewEvent.GetEventId(),
		protomock.Eq(continueAsNewEvent),
	).Return(nil)
	s.mockMutableState.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry).AnyTimes()
	s.mockUpdateVersion(continueAsNewEvent)
	s.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		now,
		false,
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	// new workflow namespace
	s.mockNamespaceCache.EXPECT().GetNamespace(tests.ParentNamespace).Return(tests.GlobalParentNamespaceEntry, nil).AnyTimes()
	newRunStateBuilder, err := s.stateRebuilder.ApplyEvents(
		context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(continueAsNewEvent), nil, "",
	)
	s.Nil(err)
	s.Nil(newRunStateBuilder)
	s.Equal(continueAsNewEvent.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionSignaled() {
	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{}},
	}
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ApplyWorkflowExecutionSignaled(protomock.Eq(event)).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionCancelRequested() {
	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}
	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCancelRequestedEventAttributes{WorkflowExecutionCancelRequestedEventAttributes: &historypb.WorkflowExecutionCancelRequestedEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ApplyWorkflowExecutionCancelRequestedEvent(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeUpsertWorkflowSearchAttributes() {
	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES
	event := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		Attributes: &historypb.HistoryEvent_UpsertWorkflowSearchAttributesEventAttributes{
			UpsertWorkflowSearchAttributesEventAttributes: &historypb.UpsertWorkflowSearchAttributesEventAttributes{},
		},
	}
	s.mockMutableState.EXPECT().ApplyUpsertWorkflowSearchAttributesEvent(protomock.Eq(event)).Return()
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateUpsertVisibilityTask().Return(nil)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowPropertiesModified() {
	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_PROPERTIES_MODIFIED
	event := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		Attributes: &historypb.HistoryEvent_WorkflowPropertiesModifiedEventAttributes{
			WorkflowPropertiesModifiedEventAttributes: &historypb.WorkflowPropertiesModifiedEventAttributes{},
		},
	}
	s.mockMutableState.EXPECT().ApplyWorkflowPropertiesModifiedEvent(protomock.Eq(event)).Return()
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateUpsertVisibilityTask().Return(nil)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeMarkerRecorded() {
	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_MARKER_RECORDED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_MarkerRecordedEventAttributes{MarkerRecordedEventAttributes: &historypb.MarkerRecordedEventAttributes{}},
	}
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

// workflow task operations
func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowTaskScheduled() {
	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
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
		EventTime: timestamppb.New(now),
		EventType: evenType,
		Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
			TaskQueue:           taskqueue,
			StartToCloseTimeout: durationpb.New(timeout),
			Attempt:             workflowTaskAttempt,
		}},
	}
	wt := &WorkflowTaskInfo{
		Version:             event.GetVersion(),
		ScheduledEventID:    event.GetEventId(),
		StartedEventID:      common.EmptyEventID,
		RequestID:           emptyUUID,
		WorkflowTaskTimeout: timeout,
		TaskQueue:           taskqueue,
		Attempt:             workflowTaskAttempt,
		Type:                enumsspb.WORKFLOW_TASK_TYPE_NORMAL,
	}
	s.executionInfo.TaskQueue = taskqueue.GetName()
	s.mockMutableState.EXPECT().ApplyWorkflowTaskScheduledEvent(
		event.GetVersion(), event.GetEventId(), taskqueue, durationpb.New(timeout), workflowTaskAttempt, event.GetEventTime(), event.GetEventTime(), enumsspb.WORKFLOW_TASK_TYPE_NORMAL,
	).Return(wt, nil)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateScheduleWorkflowTaskTasks(
		wt.ScheduledEventID,
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}
func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowTaskStarted() {
	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
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
		EventTime: timestamppb.New(now),
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
		WorkflowTaskTimeout: timeout,
		TaskQueue:           taskqueue,
		Attempt:             1,
	}
	s.mockMutableState.EXPECT().ApplyWorkflowTaskStartedEvent(
		(*WorkflowTaskInfo)(nil), event.GetVersion(), scheduledEventID, event.GetEventId(), workflowTaskRequestID, timestamp.TimeValue(event.GetEventTime()),
		false, gomock.Any(), nil, int64(0),
	).Return(wt, nil)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateStartWorkflowTaskTasks(
		wt.ScheduledEventID,
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowTaskTimedOut() {
	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
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
		EventTime: timestamppb.New(now),
		EventType: evenType,
		Attributes: &historypb.HistoryEvent_WorkflowTaskTimedOutEventAttributes{WorkflowTaskTimedOutEventAttributes: &historypb.WorkflowTaskTimedOutEventAttributes{
			ScheduledEventId: scheduledEventID,
			StartedEventId:   startedEventID,
			TimeoutType:      enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		}},
	}
	s.mockMutableState.EXPECT().ApplyWorkflowTaskTimedOutEvent(enumspb.TIMEOUT_TYPE_START_TO_CLOSE).Return(nil)
	taskqueue := &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, Name: "some random taskqueue"}
	newScheduledEventID := int64(233)
	s.executionInfo.TaskQueue = taskqueue.GetName()
	s.mockMutableState.EXPECT().ApplyTransientWorkflowTaskScheduled().Return(&WorkflowTaskInfo{
		Version:          version,
		ScheduledEventID: newScheduledEventID,
		TaskQueue:        taskqueue,
	}, nil)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateScheduleWorkflowTaskTasks(
		newScheduledEventID,
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowTaskFailed() {
	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
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
		EventTime: timestamppb.New(now),
		EventType: evenType,
		Attributes: &historypb.HistoryEvent_WorkflowTaskFailedEventAttributes{WorkflowTaskFailedEventAttributes: &historypb.WorkflowTaskFailedEventAttributes{
			ScheduledEventId: scheduledEventID,
			StartedEventId:   startedEventID,
		}},
	}
	s.mockMutableState.EXPECT().ApplyWorkflowTaskFailedEvent().Return(nil)
	taskqueue := &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, Name: "some random taskqueue"}
	newScheduledEventID := int64(233)
	s.executionInfo.TaskQueue = taskqueue.GetName()
	s.mockMutableState.EXPECT().ApplyTransientWorkflowTaskScheduled().Return(&WorkflowTaskInfo{
		Version:          version,
		ScheduledEventID: newScheduledEventID,
		TaskQueue:        taskqueue,
	}, nil)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateScheduleWorkflowTaskTasks(
		newScheduledEventID,
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowTaskCompleted() {
	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
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
		EventTime: timestamppb.New(now),
		EventType: evenType,
		Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
			ScheduledEventId: scheduledEventID,
			StartedEventId:   startedEventID,
		}},
	}
	s.mockMutableState.EXPECT().ApplyWorkflowTaskCompletedEvent(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

// user timer operations

func (s *stateBuilderSuite) TestApplyEvents_EventTypeTimerStarted() {
	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
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
		EventTime: timestamppb.New(now),
		EventType: evenType,
		Attributes: &historypb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
			TimerId:            timerID,
			StartToFireTimeout: durationpb.New(timeoutSecond),
		}},
	}
	expiryTime := timestamp.TimeValue(event.GetEventTime()).Add(timeoutSecond)
	ti := &persistencespb.TimerInfo{
		Version:        event.GetVersion(),
		TimerId:        timerID,
		ExpiryTime:     timestamppb.New(expiryTime),
		StartedEventId: event.GetEventId(),
		TaskStatus:     TimerTaskStatusNone,
	}
	s.mockMutableState.EXPECT().ApplyTimerStartedEvent(protomock.Eq(event)).Return(ti, nil)
	s.mockUpdateVersion(event)
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeTimerFired() {
	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_TIMER_FIRED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_TimerFiredEventAttributes{TimerFiredEventAttributes: &historypb.TimerFiredEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ApplyTimerFiredEvent(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeTimerCanceled() {
	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()

	evenType := enumspb.EVENT_TYPE_TIMER_CANCELED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_TimerCanceledEventAttributes{TimerCanceledEventAttributes: &historypb.TimerCanceledEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ApplyTimerCanceledEvent(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

// activity operations

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskScheduled() {
	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
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
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{}},
	}

	ai := &persistencespb.ActivityInfo{
		Version:                 event.GetVersion(),
		ScheduledEventId:        event.GetEventId(),
		ScheduledEventBatchId:   event.GetEventId(),
		ScheduledTime:           event.GetEventTime(),
		StartedEventId:          common.EmptyEventID,
		StartedTime:             nil,
		ActivityId:              activityID,
		ScheduleToStartTimeout:  durationpb.New(timeoutSecond),
		ScheduleToCloseTimeout:  durationpb.New(timeoutSecond),
		StartToCloseTimeout:     durationpb.New(timeoutSecond),
		HeartbeatTimeout:        durationpb.New(timeoutSecond),
		CancelRequested:         false,
		CancelRequestId:         common.EmptyEventID,
		LastHeartbeatUpdateTime: nil,
		TimerTaskStatus:         TimerTaskStatusNone,
		TaskQueue:               taskqueue,
	}
	s.executionInfo.TaskQueue = taskqueue
	s.mockMutableState.EXPECT().ApplyActivityTaskScheduledEvent(event.GetEventId(), protomock.Eq(event)).Return(ai, nil)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateActivityTasks(
		event.GetEventId(),
	).Return(nil)
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskStarted() {
	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
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
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{}},
	}

	evenType = enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED
	startedEvent := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    scheduledEvent.GetEventId() + 1,
		EventTime:  timestamppb.New(timestamp.TimeValue(scheduledEvent.GetEventTime()).Add(1000 * time.Nanosecond)),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{}},
	}

	s.executionInfo.TaskQueue = taskqueue
	s.mockMutableState.EXPECT().ApplyActivityTaskStartedEvent(protomock.Eq(startedEvent)).Return(nil)
	s.mockUpdateVersion(startedEvent)
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(startedEvent), nil, "")
	s.Nil(err)
	s.Equal(startedEvent.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskTimedOut() {
	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ActivityTaskTimedOutEventAttributes{ActivityTaskTimedOutEventAttributes: &historypb.ActivityTaskTimedOutEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ApplyActivityTaskTimedOutEvent(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	//	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskFailed() {
	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ActivityTaskFailedEventAttributes{ActivityTaskFailedEventAttributes: &historypb.ActivityTaskFailedEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ApplyActivityTaskFailedEvent(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskCompleted() {
	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ApplyActivityTaskCompletedEvent(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskCancelRequested() {
	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ActivityTaskCancelRequestedEventAttributes{ActivityTaskCancelRequestedEventAttributes: &historypb.ActivityTaskCancelRequestedEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ApplyActivityTaskCancelRequestedEvent(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskCanceled() {
	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ActivityTaskCanceledEventAttributes{ActivityTaskCanceledEventAttributes: &historypb.ActivityTaskCanceledEventAttributes{}},
	}

	s.mockMutableState.EXPECT().ApplyActivityTaskCanceledEvent(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

// child workflow operations

func (s *stateBuilderSuite) TestApplyEvents_EventTypeStartChildWorkflowExecutionInitiated() {
	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
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
		EventTime: timestamppb.New(now),
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
	s.mockMutableState.EXPECT().ApplyStartChildWorkflowExecutionInitiatedEvent(
		event.GetEventId(), protomock.Eq(event), gomock.Any(),
	).Return(ci, nil)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateChildWorkflowTasks(
		protomock.Eq(event),
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeStartChildWorkflowExecutionFailed() {
	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_StartChildWorkflowExecutionFailedEventAttributes{StartChildWorkflowExecutionFailedEventAttributes: &historypb.StartChildWorkflowExecutionFailedEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ApplyStartChildWorkflowExecutionFailedEvent(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionStarted() {
	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionStartedEventAttributes{ChildWorkflowExecutionStartedEventAttributes: &historypb.ChildWorkflowExecutionStartedEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ApplyChildWorkflowExecutionStartedEvent(protomock.Eq(event), nil).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionTimedOut() {
	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionTimedOutEventAttributes{ChildWorkflowExecutionTimedOutEventAttributes: &historypb.ChildWorkflowExecutionTimedOutEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ApplyChildWorkflowExecutionTimedOutEvent(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionTerminated() {
	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionTerminatedEventAttributes{ChildWorkflowExecutionTerminatedEventAttributes: &historypb.ChildWorkflowExecutionTerminatedEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ApplyChildWorkflowExecutionTerminatedEvent(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionFailed() {
	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionFailedEventAttributes{ChildWorkflowExecutionFailedEventAttributes: &historypb.ChildWorkflowExecutionFailedEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ApplyChildWorkflowExecutionFailedEvent(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionCompleted() {
	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionCompletedEventAttributes{ChildWorkflowExecutionCompletedEventAttributes: &historypb.ChildWorkflowExecutionCompletedEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ApplyChildWorkflowExecutionCompletedEvent(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

// cancel external workflow operations

func (s *stateBuilderSuite) TestApplyEvents_EventTypeRequestCancelExternalWorkflowExecutionInitiated() {
	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
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
		EventTime: timestamppb.New(now),
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
	s.mockMutableState.EXPECT().ApplyRequestCancelExternalWorkflowExecutionInitiatedEvent(
		event.GetEventId(), protomock.Eq(event), gomock.Any(),
	).Return(rci, nil)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateRequestCancelExternalTasks(
		protomock.Eq(event),
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeRequestCancelExternalWorkflowExecutionFailed() {
	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_RequestCancelExternalWorkflowExecutionFailedEventAttributes{RequestCancelExternalWorkflowExecutionFailedEventAttributes: &historypb.RequestCancelExternalWorkflowExecutionFailedEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ApplyRequestCancelExternalWorkflowExecutionFailedEvent(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeExternalWorkflowExecutionCancelRequested() {
	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ExternalWorkflowExecutionCancelRequestedEventAttributes{ExternalWorkflowExecutionCancelRequestedEventAttributes: &historypb.ExternalWorkflowExecutionCancelRequestedEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ApplyExternalWorkflowExecutionCancelRequested(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionCanceled() {
	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionCanceledEventAttributes{ChildWorkflowExecutionCanceledEventAttributes: &historypb.ChildWorkflowExecutionCanceledEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ApplyChildWorkflowExecutionCanceledEvent(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

// signal external workflow operations

func (s *stateBuilderSuite) TestApplyEvents_EventTypeSignalExternalWorkflowExecutionInitiated() {
	version := int64(1)
	requestID := uuid.New()
	execution := &commonpb.WorkflowExecution{
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
		EventTime: timestamppb.New(now),
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
	s.mockMutableState.EXPECT().ApplySignalExternalWorkflowExecutionInitiatedEvent(
		event.GetEventId(), protomock.Eq(event), gomock.Any(),
	).Return(si, nil)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateSignalExternalTasks(
		protomock.Eq(event),
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeSignalExternalWorkflowExecutionFailed() {
	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_SignalExternalWorkflowExecutionFailedEventAttributes{SignalExternalWorkflowExecutionFailedEventAttributes: &historypb.SignalExternalWorkflowExecutionFailedEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ApplySignalExternalWorkflowExecutionFailedEvent(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeExternalWorkflowExecutionSignaled() {
	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ExternalWorkflowExecutionSignaledEventAttributes{ExternalWorkflowExecutionSignaledEventAttributes: &historypb.ExternalWorkflowExecutionSignaledEventAttributes{}},
	}
	s.mockMutableState.EXPECT().ApplyExternalWorkflowExecutionSignaled(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionUpdateAccepted() {
	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED
	event := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionUpdateAcceptedEventAttributes{
			WorkflowExecutionUpdateAcceptedEventAttributes: &historypb.WorkflowExecutionUpdateAcceptedEventAttributes{
				ProtocolInstanceId: s.T().Name(),
			},
		},
	}
	s.mockMutableState.EXPECT().ApplyWorkflowExecutionUpdateAcceptedEvent(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.NoError(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionUpdateCompleted() {
	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED
	event := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionUpdateCompletedEventAttributes{
			WorkflowExecutionUpdateCompletedEventAttributes: &historypb.WorkflowExecutionUpdateCompletedEventAttributes{},
		},
	}
	s.mockMutableState.EXPECT().ApplyWorkflowExecutionUpdateCompletedEvent(protomock.Eq(event), event.EventId).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.NoError(err)
	s.Equal(event.TaskId, s.executionInfo.LastEventTaskId)
}

func (s *stateBuilderSuite) TestApplyEvents_HSMRegistry() {
	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	event := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   5,
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
		Attributes: &historypb.HistoryEvent_NexusOperationScheduledEventAttributes{
			NexusOperationScheduledEventAttributes: &historypb.NexusOperationScheduledEventAttributes{
				EndpointId:                   "endpoint-id",
				Endpoint:                     "endpoint",
				Service:                      "service",
				Operation:                    "operation",
				WorkflowTaskCompletedEventId: 4,
				RequestId:                    "request-id",
			},
		},
	}
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()
	s.mockUpdateVersion(event)

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.NoError(err)
	// Verify the event was applied.
	sm, err := nexusoperations.MachineCollection(s.mockMutableState.HSM()).Data("5")
	s.NoError(err)
	s.Equal(enumsspb.NEXUS_OPERATION_STATE_SCHEDULED, sm.State())
}

func (p *testTaskGeneratorProvider) NewTaskGenerator(
	shardContext shard.Context,
	mutableState MutableState,
) TaskGenerator {
	if mutableState == p.mockMutableState {
		return p.mockTaskGenerator
	}

	return NewTaskGenerator(
		shardContext.GetNamespaceRegistry(),
		mutableState,
		shardContext.GetConfig(),
		shardContext.GetArchivalMetadata(),
	)
}
