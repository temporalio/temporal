package workflow

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
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
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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
		mockMutableState     *historyi.MockMutableState
		mockClusterMetadata  *cluster.MockMetadata
		stateMachineRegistry *hsm.Registry

		logger log.Logger

		sourceCluster  string
		executionInfo  *persistencespb.WorkflowExecutionInfo
		stateRebuilder *MutableStateRebuilderImpl
	}

	testTaskGeneratorProvider struct {
		mockMutableState  *historyi.MockMutableState
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
	s.mockMutableState = historyi.NewMockMutableState(s.controller)

	s.mockShard = shard.NewTestContext(
		s.controller,
		persistencespb.ShardInfo_builder{
			ShardId: 0,
			RangeId: 1,
		}.Build(),
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
	s.mockMutableState.EXPECT().IsTransitionHistoryEnabled().Return(false).AnyTimes()

	s.mockNamespaceCache = s.mockShard.Resource.NamespaceCache
	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata
	s.mockEventsCache = s.mockShard.MockEventsCache
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()

	s.logger = s.mockShard.GetLogger()
	s.executionInfo = persistencespb.WorkflowExecutionInfo_builder{
		VersionHistories:                 versionhistory.NewVersionHistories(&historyspb.VersionHistory{}),
		FirstExecutionRunId:              uuid.NewString(),
		WorkflowExecutionTimerTaskStatus: TimerTaskStatusCreated,
	}.Build()
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
	requestID := uuid.NewString()
	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	s.executionInfo.SetWorkflowRunTimeout(timestamp.DurationFromSeconds(100))
	s.executionInfo.SetCronSchedule(cronSchedule)

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
	startWorkflowAttribute := historypb.WorkflowExecutionStartedEventAttributes_builder{
		ParentWorkflowNamespace:   tests.ParentNamespace.String(),
		ParentWorkflowNamespaceId: tests.ParentNamespaceID.String(),
	}.Build()

	event := historypb.HistoryEvent_builder{
		TaskId:                                  rand.Int63(),
		Version:                                 version,
		EventId:                                 1,
		EventTime:                               timestamppb.New(now),
		EventType:                               evenType,
		WorkflowExecutionStartedEventAttributes: proto.ValueOrDefault(startWorkflowAttribute),
	}.Build()

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
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionStarted_WithCronSchedule() {
	cronSchedule := "* * * * *"
	version := int64(1)
	requestID := uuid.NewString()
	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	s.executionInfo.SetWorkflowRunTimeout(timestamp.DurationFromSeconds(100))
	s.executionInfo.SetCronSchedule(cronSchedule)

	now := time.Now().UTC()
	eventType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
	startWorkflowAttribute := historypb.WorkflowExecutionStartedEventAttributes_builder{
		ParentWorkflowNamespace:   tests.ParentNamespace.String(),
		ParentWorkflowNamespaceId: tests.ParentNamespaceID.String(),
		Initiator:                 enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE,
		FirstWorkflowTaskBackoff:  durationpb.New(backoff.GetBackoffForNextSchedule(cronSchedule, now, now)),
	}.Build()

	event := historypb.HistoryEvent_builder{
		TaskId:                                  rand.Int63(),
		Version:                                 version,
		EventId:                                 1,
		EventTime:                               timestamppb.New(now),
		EventType:                               eventType,
		WorkflowExecutionStartedEventAttributes: proto.ValueOrDefault(startWorkflowAttribute),
	}.Build()

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
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionTimedOut() {
	version := int64(1)
	requestID := uuid.NewString()
	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT
	event := historypb.HistoryEvent_builder{
		TaskId:                                   rand.Int63(),
		Version:                                  version,
		EventId:                                  130,
		EventTime:                                timestamppb.New(now),
		EventType:                                evenType,
		WorkflowExecutionTimedOutEventAttributes: &historypb.WorkflowExecutionTimedOutEventAttributes{},
	}.Build()

	s.mockMutableState.EXPECT().ApplyWorkflowExecutionTimedoutEvent(event.GetEventId(), protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		now,
		false,
		false, // skipCloseTransferTask
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionTimedOut_WithNewRunHistory() {
	version := int64(1)
	requestID := uuid.NewString()
	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()
	newRunID := uuid.NewString()

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT
	event := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		WorkflowExecutionTimedOutEventAttributes: historypb.WorkflowExecutionTimedOutEventAttributes_builder{
			NewExecutionRunId: newRunID,
		}.Build(),
	}.Build()

	newRunStartedEvent := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   1,
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		WorkflowExecutionStartedEventAttributes: historypb.WorkflowExecutionStartedEventAttributes_builder{
			WorkflowExecutionTimeout:        durationpb.New(100 * time.Second),
			WorkflowRunTimeout:              durationpb.New(100 * time.Second),
			WorkflowTaskTimeout:             durationpb.New(10 * time.Second),
			TaskQueue:                       taskqueuepb.TaskQueue_builder{Name: "some random taskqueue"}.Build(),
			WorkflowType:                    commonpb.WorkflowType_builder{Name: "some random workflow type"}.Build(),
			FirstWorkflowTaskBackoff:        durationpb.New(10 * time.Second),
			FirstExecutionRunId:             s.mockMutableState.GetExecutionInfo().GetFirstExecutionRunId(),
			WorkflowExecutionExpirationTime: timestamppb.New(now.Add(100 * time.Second)),
			ContinuedExecutionRunId:         execution.GetRunId(),
		}.Build(),
	}.Build()
	newRunEvents := []*historypb.HistoryEvent{newRunStartedEvent}

	s.mockMutableState.EXPECT().ApplyWorkflowExecutionTimedoutEvent(event.GetEventId(), protomock.Eq(event)).Return(nil)
	s.mockMutableState.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionState().Return(persistencespb.WorkflowExecutionState_builder{
		CreateRequestId: uuid.NewString(),
		State:           enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		Status:          enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
	}.Build())
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		now,
		false,
		false, // skipCloseTransferTask
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	newRunStateBuilder, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), newRunEvents, newRunID)
	s.Nil(err)
	s.NotNil(newRunStateBuilder)
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())

	newRunTasks := newRunStateBuilder.PopTasks()
	s.Len(newRunTasks[tasks.CategoryTimer], 1)      // backoffTimer
	s.Len(newRunTasks[tasks.CategoryVisibility], 1) // recordWorkflowStarted
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionTerminated() {
	version := int64(1)
	requestID := uuid.NewString()
	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED
	event := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{},
	}.Build()

	s.mockMutableState.EXPECT().ApplyWorkflowExecutionTerminatedEvent(event.GetEventId(), protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		now,
		false,
		false, // skipCloseTransferTask
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()
	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionTerminated_WithNewRunHistory() {
	version := int64(1)
	requestID := uuid.NewString()
	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED
	event := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{},
	}.Build()

	newRunStartedEvent := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   1,
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		WorkflowExecutionStartedEventAttributes: historypb.WorkflowExecutionStartedEventAttributes_builder{
			WorkflowExecutionTimeout:        durationpb.New(100 * time.Second),
			WorkflowRunTimeout:              durationpb.New(10 * time.Second),
			WorkflowTaskTimeout:             durationpb.New(10 * time.Second),
			TaskQueue:                       taskqueuepb.TaskQueue_builder{Name: "some random taskqueue"}.Build(),
			WorkflowType:                    commonpb.WorkflowType_builder{Name: "some random workflow type"}.Build(),
			FirstWorkflowTaskBackoff:        durationpb.New(10 * time.Second),
			FirstExecutionRunId:             uuid.NewString(),
			WorkflowExecutionExpirationTime: timestamppb.New(now.Add(100 * time.Second)),
		}.Build(),
	}.Build()
	newRunEvents := []*historypb.HistoryEvent{newRunStartedEvent}

	s.mockMutableState.EXPECT().ApplyWorkflowExecutionTerminatedEvent(event.GetEventId(), protomock.Eq(event)).Return(nil)
	s.mockMutableState.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionState().Return(persistencespb.WorkflowExecutionState_builder{
		CreateRequestId: uuid.NewString(),
		State:           enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		Status:          enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
	}.Build())
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		now,
		false,
		false, // skipCloseTransferTask
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	newRunStateBuilder, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), newRunEvents, uuid.NewString())
	s.Nil(err)
	s.NotNil(newRunStateBuilder)
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())

	newRunTasks := newRunStateBuilder.PopTasks()
	s.Len(newRunTasks[tasks.CategoryTimer], 3)      // backoff timer, runTimeout timer, executionTimeout timer
	s.Len(newRunTasks[tasks.CategoryVisibility], 1) // recordWorkflowStarted
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionFailed() {
	version := int64(1)
	requestID := uuid.NewString()
	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED
	event := historypb.HistoryEvent_builder{
		TaskId:                                 rand.Int63(),
		Version:                                version,
		EventId:                                130,
		EventTime:                              timestamppb.New(now),
		EventType:                              evenType,
		WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{},
	}.Build()

	s.mockMutableState.EXPECT().ApplyWorkflowExecutionFailedEvent(event.GetEventId(), protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		now,
		false,
		false, // skipCloseTransferTask
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionFailed_WithNewRunHistory() {
	version := int64(1)
	requestID := uuid.NewString()
	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()
	newRunID := uuid.NewString()

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED
	event := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		WorkflowExecutionFailedEventAttributes: historypb.WorkflowExecutionFailedEventAttributes_builder{
			NewExecutionRunId: newRunID,
		}.Build(),
	}.Build()

	newRunStartedEvent := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   1,
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		WorkflowExecutionStartedEventAttributes: historypb.WorkflowExecutionStartedEventAttributes_builder{
			WorkflowExecutionTimeout:        durationpb.New(100 * time.Second),
			WorkflowRunTimeout:              durationpb.New(10 * time.Second),
			WorkflowTaskTimeout:             durationpb.New(10 * time.Second),
			TaskQueue:                       taskqueuepb.TaskQueue_builder{Name: "some random taskqueue"}.Build(),
			WorkflowType:                    commonpb.WorkflowType_builder{Name: "some random workflow type"}.Build(),
			FirstWorkflowTaskBackoff:        durationpb.New(10 * time.Second),
			FirstExecutionRunId:             s.mockMutableState.GetExecutionInfo().GetFirstExecutionRunId(),
			WorkflowExecutionExpirationTime: timestamppb.New(now.Add(100 * time.Second)),
			ContinuedExecutionRunId:         execution.GetRunId(),
		}.Build(),
	}.Build()
	newRunEvents := []*historypb.HistoryEvent{newRunStartedEvent}

	s.mockMutableState.EXPECT().ApplyWorkflowExecutionFailedEvent(event.GetEventId(), protomock.Eq(event)).Return(nil)
	s.mockMutableState.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionState().Return(persistencespb.WorkflowExecutionState_builder{
		CreateRequestId: uuid.NewString(),
		State:           enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		Status:          enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
	}.Build())
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		now,
		false,
		false, // skipCloseTransferTask
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	newRunStateBuilder, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), newRunEvents, newRunID)
	s.Nil(err)
	s.NotNil(newRunStateBuilder)
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())

	newRunTasks := newRunStateBuilder.PopTasks()
	s.Len(newRunTasks[tasks.CategoryTimer], 2)      // backoffTimer, runTimeout timer
	s.Len(newRunTasks[tasks.CategoryVisibility], 1) // recordWorkflowStarted
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionCompleted() {
	version := int64(1)
	requestID := uuid.NewString()
	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
	event := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{},
	}.Build()

	s.mockMutableState.EXPECT().ApplyWorkflowExecutionCompletedEvent(event.GetEventId(), event).Return(nil)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		now,
		false,
		false, // skipCloseTransferTask
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionCompleted_WithNewRunHistory() {
	version := int64(1)
	requestID := uuid.NewString()
	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()
	newRunID := uuid.NewString()

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
	event := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		WorkflowExecutionCompletedEventAttributes: historypb.WorkflowExecutionCompletedEventAttributes_builder{
			NewExecutionRunId: newRunID,
		}.Build(),
	}.Build()

	newRunStartedEvent := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   1,
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		WorkflowExecutionStartedEventAttributes: historypb.WorkflowExecutionStartedEventAttributes_builder{
			WorkflowExecutionTimeout:        durationpb.New(100 * time.Second),
			WorkflowRunTimeout:              durationpb.New(100 * time.Second),
			WorkflowTaskTimeout:             durationpb.New(10 * time.Second),
			TaskQueue:                       taskqueuepb.TaskQueue_builder{Name: "some random taskqueue"}.Build(),
			WorkflowType:                    commonpb.WorkflowType_builder{Name: "some random workflow type"}.Build(),
			FirstExecutionRunId:             s.mockMutableState.GetExecutionInfo().GetFirstExecutionRunId(),
			WorkflowExecutionExpirationTime: timestamppb.New(now.Add(100 * time.Second)),
			ContinuedExecutionRunId:         execution.GetRunId(),
		}.Build(),
	}.Build()
	newRunEvents := []*historypb.HistoryEvent{newRunStartedEvent}

	s.mockMutableState.EXPECT().ApplyWorkflowExecutionCompletedEvent(event.GetEventId(), event).Return(nil)
	s.mockMutableState.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionState().Return(persistencespb.WorkflowExecutionState_builder{
		CreateRequestId: uuid.NewString(),
		State:           enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		Status:          enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	}.Build())
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		now,
		false,
		false, // skipCloseTransferTask
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	newRunStateBuilder, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), newRunEvents, newRunID)
	s.Nil(err)
	s.NotNil(newRunStateBuilder)
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())

	newRunTasks := newRunStateBuilder.PopTasks()
	s.Len(newRunTasks[tasks.CategoryTimer], 0)
	s.Len(newRunTasks[tasks.CategoryVisibility], 1) // recordWorkflowStarted
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionCanceled() {
	version := int64(1)
	requestID := uuid.NewString()
	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED
	event := historypb.HistoryEvent_builder{
		TaskId:                                   rand.Int63(),
		Version:                                  version,
		EventId:                                  130,
		EventTime:                                timestamppb.New(now),
		EventType:                                evenType,
		WorkflowExecutionCanceledEventAttributes: &historypb.WorkflowExecutionCanceledEventAttributes{},
	}.Build()

	s.mockMutableState.EXPECT().ApplyWorkflowExecutionCanceledEvent(event.GetEventId(), event).Return(nil)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		now,
		false,
		false, // skipCloseTransferTask
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionContinuedAsNew() {
	version := int64(1)
	requestID := uuid.NewString()
	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()
	parentWorkflowID := "some random parent workflow ID"
	parentRunID := uuid.NewString()
	parentInitiatedEventID := int64(144)

	now := time.Now().UTC()
	taskqueue := "some random taskqueue"
	workflowType := "some random workflow type"
	workflowTimeoutSecond := time.Duration(110) * time.Second
	taskTimeout := time.Duration(11) * time.Second
	newRunID := uuid.NewString()

	continueAsNewEvent := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
		WorkflowExecutionContinuedAsNewEventAttributes: historypb.WorkflowExecutionContinuedAsNewEventAttributes_builder{
			NewExecutionRunId: newRunID,
		}.Build(),
	}.Build()

	newRunStartedEvent := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   1,
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		WorkflowExecutionStartedEventAttributes: historypb.WorkflowExecutionStartedEventAttributes_builder{
			ParentWorkflowNamespace:   tests.ParentNamespace.String(),
			ParentWorkflowNamespaceId: tests.ParentNamespaceID.String(),
			ParentWorkflowExecution: commonpb.WorkflowExecution_builder{
				WorkflowId: parentWorkflowID,
				RunId:      parentRunID,
			}.Build(),
			ParentInitiatedEventId:          parentInitiatedEventID,
			WorkflowExecutionTimeout:        durationpb.New(workflowTimeoutSecond),
			WorkflowTaskTimeout:             durationpb.New(taskTimeout),
			TaskQueue:                       taskqueuepb.TaskQueue_builder{Name: taskqueue}.Build(),
			WorkflowType:                    commonpb.WorkflowType_builder{Name: workflowType}.Build(),
			WorkflowExecutionExpirationTime: timestamppb.New(now.Add(workflowTimeoutSecond)),
			FirstExecutionRunId:             s.mockMutableState.GetExecutionInfo().GetFirstExecutionRunId(),
			ContinuedExecutionRunId:         execution.GetRunId(),
		}.Build(),
	}.Build()

	newRunSignalEvent := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   2,
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		WorkflowExecutionSignaledEventAttributes: historypb.WorkflowExecutionSignaledEventAttributes_builder{
			SignalName: "some random signal name",
			Input:      payloads.EncodeString("some random signal input"),
			Identity:   "some random identity",
		}.Build(),
	}.Build()

	newRunWorkflowTaskAttempt := int32(123)
	newRunWorkflowTaskEvent := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   3,
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		WorkflowTaskScheduledEventAttributes: historypb.WorkflowTaskScheduledEventAttributes_builder{
			TaskQueue:           taskqueuepb.TaskQueue_builder{Name: taskqueue}.Build(),
			StartToCloseTimeout: durationpb.New(taskTimeout),
			Attempt:             newRunWorkflowTaskAttempt,
		}.Build(),
	}.Build()
	newRunEvents := []*historypb.HistoryEvent{
		newRunStartedEvent, newRunSignalEvent, newRunWorkflowTaskEvent,
	}

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, continueAsNewEvent.GetVersion()).Return(s.sourceCluster).AnyTimes()
	s.mockMutableState.EXPECT().ApplyWorkflowExecutionContinuedAsNewEvent(
		continueAsNewEvent.GetEventId(),
		protomock.Eq(continueAsNewEvent),
	).Return(nil)
	s.mockMutableState.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry).AnyTimes()
	s.mockMutableState.EXPECT().GetWorkflowKey().Return(definition.NewWorkflowKey(tests.GlobalNamespaceEntry.ID().String(), execution.GetWorkflowId(), execution.GetRunId())).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionState().Return(persistencespb.WorkflowExecutionState_builder{
		CreateRequestId: uuid.NewString(),
		State:           enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		Status:          enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
	}.Build())
	s.mockUpdateVersion(continueAsNewEvent)
	s.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		now,
		false,
		false, // skipCloseTransferTask
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	// new workflow namespace
	s.mockNamespaceCache.EXPECT().GetNamespace(tests.ParentNamespace).Return(tests.GlobalParentNamespaceEntry, nil).AnyTimes()

	newRunStateBuilder, err := s.stateRebuilder.ApplyEvents(
		context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(continueAsNewEvent), newRunEvents, "",
	)
	s.Nil(err)
	s.NotNil(newRunStateBuilder)
	s.Equal(continueAsNewEvent.GetTaskId(), s.executionInfo.GetLastRunningClock())

	newRunTasks := newRunStateBuilder.PopTasks()
	s.Empty(newRunTasks[tasks.CategoryTimer])
	s.Len(newRunTasks[tasks.CategoryVisibility], 1) // recordWorkflowStarted
	s.Len(newRunTasks[tasks.CategoryTransfer], 1)   // workflow task
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionContinuedAsNew_EmptyNewRunHistory() {
	version := int64(1)
	requestID := uuid.NewString()
	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()
	newRunID := uuid.NewString()

	continueAsNewEvent := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
		WorkflowExecutionContinuedAsNewEventAttributes: historypb.WorkflowExecutionContinuedAsNewEventAttributes_builder{
			NewExecutionRunId: newRunID,
		}.Build(),
	}.Build()

	s.mockMutableState.EXPECT().ApplyWorkflowExecutionContinuedAsNewEvent(
		continueAsNewEvent.GetEventId(),
		protomock.Eq(continueAsNewEvent),
	).Return(nil)
	s.mockMutableState.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry).AnyTimes()
	s.mockUpdateVersion(continueAsNewEvent)
	s.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		now,
		false,
		false, // skipCloseTransferTask
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	// new workflow namespace
	s.mockNamespaceCache.EXPECT().GetNamespace(tests.ParentNamespace).Return(tests.GlobalParentNamespaceEntry, nil).AnyTimes()
	newRunStateBuilder, err := s.stateRebuilder.ApplyEvents(
		context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(continueAsNewEvent), nil, "",
	)
	s.Nil(err)
	s.Nil(newRunStateBuilder)
	s.Equal(continueAsNewEvent.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionSignaled() {
	version := int64(1)
	requestID := uuid.NewString()

	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
	event := historypb.HistoryEvent_builder{
		TaskId:                                   rand.Int63(),
		Version:                                  version,
		EventId:                                  130,
		EventTime:                                timestamppb.New(now),
		EventType:                                evenType,
		WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{},
	}.Build()
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ApplyWorkflowExecutionSignaled(protomock.Eq(event)).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionCancelRequested() {
	version := int64(1)
	requestID := uuid.NewString()

	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()
	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED
	event := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		WorkflowExecutionCancelRequestedEventAttributes: &historypb.WorkflowExecutionCancelRequestedEventAttributes{},
	}.Build()

	s.mockMutableState.EXPECT().ApplyWorkflowExecutionCancelRequestedEvent(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeUpsertWorkflowSearchAttributes() {
	version := int64(1)
	requestID := uuid.NewString()

	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES
	event := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		UpsertWorkflowSearchAttributesEventAttributes: &historypb.UpsertWorkflowSearchAttributesEventAttributes{},
	}.Build()
	s.mockMutableState.EXPECT().ApplyUpsertWorkflowSearchAttributesEvent(protomock.Eq(event)).Return()
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateUpsertVisibilityTask().Return(nil)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowPropertiesModified() {
	version := int64(1)
	requestID := uuid.NewString()

	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_PROPERTIES_MODIFIED
	event := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		WorkflowPropertiesModifiedEventAttributes: &historypb.WorkflowPropertiesModifiedEventAttributes{},
	}.Build()
	s.mockMutableState.EXPECT().ApplyWorkflowPropertiesModifiedEvent(protomock.Eq(event)).Return()
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateUpsertVisibilityTask().Return(nil)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeMarkerRecorded() {
	version := int64(1)
	requestID := uuid.NewString()

	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_MARKER_RECORDED
	event := historypb.HistoryEvent_builder{
		TaskId:                        rand.Int63(),
		Version:                       version,
		EventId:                       130,
		EventTime:                     timestamppb.New(now),
		EventType:                     evenType,
		MarkerRecordedEventAttributes: &historypb.MarkerRecordedEventAttributes{},
	}.Build()
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

// workflow task operations
func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowTaskScheduled() {
	version := int64(1)
	requestID := uuid.NewString()

	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()
	taskqueue := taskqueuepb.TaskQueue_builder{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, Name: "some random taskqueue"}.Build()
	timeout := time.Duration(11) * time.Second
	evenType := enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
	workflowTaskAttempt := int32(111)
	event := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		WorkflowTaskScheduledEventAttributes: historypb.WorkflowTaskScheduledEventAttributes_builder{
			TaskQueue:           taskqueue,
			StartToCloseTimeout: durationpb.New(timeout),
			Attempt:             workflowTaskAttempt,
		}.Build(),
	}.Build()
	wt := &historyi.WorkflowTaskInfo{
		Version:             event.GetVersion(),
		ScheduledEventID:    event.GetEventId(),
		StartedEventID:      common.EmptyEventID,
		RequestID:           emptyUUID,
		WorkflowTaskTimeout: timeout,
		TaskQueue:           taskqueue,
		Attempt:             workflowTaskAttempt,
		Type:                enumsspb.WORKFLOW_TASK_TYPE_NORMAL,
	}
	s.executionInfo.SetTaskQueue(taskqueue.GetName())
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
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}
func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowTaskStarted() {
	version := int64(1)
	requestID := uuid.NewString()

	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()
	taskqueue := taskqueuepb.TaskQueue_builder{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, Name: "some random taskqueue"}.Build()
	timeout := time.Second * 11
	scheduledEventID := int64(111)
	workflowTaskRequestID := uuid.NewString()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED
	event := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		WorkflowTaskStartedEventAttributes: historypb.WorkflowTaskStartedEventAttributes_builder{
			ScheduledEventId: scheduledEventID,
			RequestId:        workflowTaskRequestID,
		}.Build(),
	}.Build()
	wt := &historyi.WorkflowTaskInfo{
		Version:             event.GetVersion(),
		ScheduledEventID:    scheduledEventID,
		StartedEventID:      event.GetEventId(),
		RequestID:           workflowTaskRequestID,
		WorkflowTaskTimeout: timeout,
		TaskQueue:           taskqueue,
		Attempt:             1,
	}
	s.mockMutableState.EXPECT().ApplyWorkflowTaskStartedEvent(
		(*historyi.WorkflowTaskInfo)(nil), event.GetVersion(), scheduledEventID, event.GetEventId(), workflowTaskRequestID, timestamp.TimeValue(event.GetEventTime()),
		false, gomock.Any(), nil, int64(0), nil,
	).Return(wt, nil)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateStartWorkflowTaskTasks(
		wt.ScheduledEventID,
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowTaskTimedOut() {
	version := int64(1)
	requestID := uuid.NewString()

	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()
	scheduledEventID := int64(12)
	startedEventID := int64(28)
	evenType := enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT
	event := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		WorkflowTaskTimedOutEventAttributes: historypb.WorkflowTaskTimedOutEventAttributes_builder{
			ScheduledEventId: scheduledEventID,
			StartedEventId:   startedEventID,
			TimeoutType:      enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		}.Build(),
	}.Build()
	s.mockMutableState.EXPECT().ApplyWorkflowTaskTimedOutEvent(enumspb.TIMEOUT_TYPE_START_TO_CLOSE).Return(nil)
	taskqueue := taskqueuepb.TaskQueue_builder{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, Name: "some random taskqueue"}.Build()
	newScheduledEventID := int64(233)
	s.executionInfo.SetTaskQueue(taskqueue.GetName())
	s.mockMutableState.EXPECT().ApplyTransientWorkflowTaskScheduled().Return(&historyi.WorkflowTaskInfo{
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
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowTaskFailed() {
	version := int64(1)
	requestID := uuid.NewString()

	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()
	scheduledEventID := int64(12)
	startedEventID := int64(28)
	evenType := enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED
	event := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		WorkflowTaskFailedEventAttributes: historypb.WorkflowTaskFailedEventAttributes_builder{
			ScheduledEventId: scheduledEventID,
			StartedEventId:   startedEventID,
		}.Build(),
	}.Build()
	s.mockMutableState.EXPECT().ApplyWorkflowTaskFailedEvent().Return(nil)
	taskqueue := taskqueuepb.TaskQueue_builder{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, Name: "some random taskqueue"}.Build()
	newScheduledEventID := int64(233)
	s.executionInfo.SetTaskQueue(taskqueue.GetName())
	s.mockMutableState.EXPECT().ApplyTransientWorkflowTaskScheduled().Return(&historyi.WorkflowTaskInfo{
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
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowTaskCompleted() {
	version := int64(1)
	requestID := uuid.NewString()

	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()
	scheduledEventID := int64(12)
	startedEventID := int64(28)
	evenType := enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED
	event := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		WorkflowTaskCompletedEventAttributes: historypb.WorkflowTaskCompletedEventAttributes_builder{
			ScheduledEventId: scheduledEventID,
			StartedEventId:   startedEventID,
		}.Build(),
	}.Build()
	s.mockMutableState.EXPECT().ApplyWorkflowTaskCompletedEvent(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

// user timer operations

func (s *stateBuilderSuite) TestApplyEvents_EventTypeTimerStarted() {
	version := int64(1)
	requestID := uuid.NewString()

	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()
	timerID := "timer ID"
	timeoutSecond := time.Duration(10) * time.Second
	evenType := enumspb.EVENT_TYPE_TIMER_STARTED
	event := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		TimerStartedEventAttributes: historypb.TimerStartedEventAttributes_builder{
			TimerId:            timerID,
			StartToFireTimeout: durationpb.New(timeoutSecond),
		}.Build(),
	}.Build()
	expiryTime := timestamp.TimeValue(event.GetEventTime()).Add(timeoutSecond)
	ti := persistencespb.TimerInfo_builder{
		Version:        event.GetVersion(),
		TimerId:        timerID,
		ExpiryTime:     timestamppb.New(expiryTime),
		StartedEventId: event.GetEventId(),
		TaskStatus:     TimerTaskStatusNone,
	}.Build()
	s.mockMutableState.EXPECT().ApplyTimerStartedEvent(protomock.Eq(event)).Return(ti, nil)
	s.mockUpdateVersion(event)
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeTimerFired() {
	version := int64(1)
	requestID := uuid.NewString()

	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_TIMER_FIRED
	event := historypb.HistoryEvent_builder{
		TaskId:                    rand.Int63(),
		Version:                   version,
		EventId:                   130,
		EventTime:                 timestamppb.New(now),
		EventType:                 evenType,
		TimerFiredEventAttributes: &historypb.TimerFiredEventAttributes{},
	}.Build()

	s.mockMutableState.EXPECT().ApplyTimerFiredEvent(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeTimerCanceled() {
	version := int64(1)
	requestID := uuid.NewString()

	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()

	evenType := enumspb.EVENT_TYPE_TIMER_CANCELED
	event := historypb.HistoryEvent_builder{
		TaskId:                       rand.Int63(),
		Version:                      version,
		EventId:                      130,
		EventTime:                    timestamppb.New(now),
		EventType:                    evenType,
		TimerCanceledEventAttributes: &historypb.TimerCanceledEventAttributes{},
	}.Build()
	s.mockMutableState.EXPECT().ApplyTimerCanceledEvent(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

// activity operations

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskScheduled() {
	version := int64(1)
	requestID := uuid.NewString()

	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()
	activityID := "activity ID"
	taskqueue := "some random taskqueue"
	timeoutSecond := 10 * time.Second
	evenType := enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
	event := historypb.HistoryEvent_builder{
		TaskId:                               rand.Int63(),
		Version:                              version,
		EventId:                              130,
		EventTime:                            timestamppb.New(now),
		EventType:                            evenType,
		ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{},
	}.Build()

	ai := persistencespb.ActivityInfo_builder{
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
	}.Build()
	s.executionInfo.SetTaskQueue(taskqueue)
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
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskStarted() {
	version := int64(1)
	requestID := uuid.NewString()

	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()
	taskqueue := "some random taskqueue"
	evenType := enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
	scheduledEvent := historypb.HistoryEvent_builder{
		TaskId:                               rand.Int63(),
		Version:                              version,
		EventId:                              130,
		EventTime:                            timestamppb.New(now),
		EventType:                            evenType,
		ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{},
	}.Build()

	evenType = enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED
	startedEvent := historypb.HistoryEvent_builder{
		TaskId:                             rand.Int63(),
		Version:                            version,
		EventId:                            scheduledEvent.GetEventId() + 1,
		EventTime:                          timestamppb.New(timestamp.TimeValue(scheduledEvent.GetEventTime()).Add(1000 * time.Nanosecond)),
		EventType:                          evenType,
		ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{},
	}.Build()

	s.executionInfo.SetTaskQueue(taskqueue)
	s.mockMutableState.EXPECT().ApplyActivityTaskStartedEvent(protomock.Eq(startedEvent)).Return(nil)
	s.mockUpdateVersion(startedEvent)
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(startedEvent), nil, "")
	s.Nil(err)
	s.Equal(startedEvent.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskTimedOut() {
	version := int64(1)
	requestID := uuid.NewString()

	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT
	event := historypb.HistoryEvent_builder{
		TaskId:                              rand.Int63(),
		Version:                             version,
		EventId:                             130,
		EventTime:                           timestamppb.New(now),
		EventType:                           evenType,
		ActivityTaskTimedOutEventAttributes: &historypb.ActivityTaskTimedOutEventAttributes{},
	}.Build()

	s.mockMutableState.EXPECT().ApplyActivityTaskTimedOutEvent(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	//	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskFailed() {
	version := int64(1)
	requestID := uuid.NewString()

	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED
	event := historypb.HistoryEvent_builder{
		TaskId:                            rand.Int63(),
		Version:                           version,
		EventId:                           130,
		EventTime:                         timestamppb.New(now),
		EventType:                         evenType,
		ActivityTaskFailedEventAttributes: &historypb.ActivityTaskFailedEventAttributes{},
	}.Build()

	s.mockMutableState.EXPECT().ApplyActivityTaskFailedEvent(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskCompleted() {
	version := int64(1)
	requestID := uuid.NewString()

	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED
	event := historypb.HistoryEvent_builder{
		TaskId:                               rand.Int63(),
		Version:                              version,
		EventId:                              130,
		EventTime:                            timestamppb.New(now),
		EventType:                            evenType,
		ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{},
	}.Build()

	s.mockMutableState.EXPECT().ApplyActivityTaskCompletedEvent(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskCancelRequested() {
	version := int64(1)
	requestID := uuid.NewString()

	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED
	event := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		ActivityTaskCancelRequestedEventAttributes: &historypb.ActivityTaskCancelRequestedEventAttributes{},
	}.Build()
	s.mockMutableState.EXPECT().ApplyActivityTaskCancelRequestedEvent(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskCanceled() {
	version := int64(1)
	requestID := uuid.NewString()

	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED
	event := historypb.HistoryEvent_builder{
		TaskId:                              rand.Int63(),
		Version:                             version,
		EventId:                             130,
		EventTime:                           timestamppb.New(now),
		EventType:                           evenType,
		ActivityTaskCanceledEventAttributes: &historypb.ActivityTaskCanceledEventAttributes{},
	}.Build()

	s.mockMutableState.EXPECT().ApplyActivityTaskCanceledEvent(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

// child workflow operations

func (s *stateBuilderSuite) TestApplyEvents_EventTypeStartChildWorkflowExecutionInitiated() {
	version := int64(1)
	requestID := uuid.NewString()

	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()
	targetWorkflowID := "some random target workflow ID"

	now := time.Now().UTC()
	createRequestID := uuid.NewString()
	evenType := enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED
	event := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		StartChildWorkflowExecutionInitiatedEventAttributes: historypb.StartChildWorkflowExecutionInitiatedEventAttributes_builder{
			Namespace:   tests.TargetNamespace.String(),
			NamespaceId: tests.TargetNamespaceID.String(),
			WorkflowId:  targetWorkflowID,
		}.Build(),
	}.Build()

	ci := persistencespb.ChildExecutionInfo_builder{
		Version:               event.GetVersion(),
		InitiatedEventId:      event.GetEventId(),
		InitiatedEventBatchId: event.GetEventId(),
		StartedEventId:        common.EmptyEventID,
		CreateRequestId:       createRequestID,
		Namespace:             tests.TargetNamespace.String(),
		NamespaceId:           tests.TargetNamespaceID.String(),
	}.Build()

	// the create request ID is generated inside, cannot assert equal
	s.mockMutableState.EXPECT().ApplyStartChildWorkflowExecutionInitiatedEvent(
		event.GetEventId(), protomock.Eq(event),
	).Return(ci, nil)
	s.mockUpdateVersion(event)
	s.mockTaskGenerator.EXPECT().GenerateChildWorkflowTasks(
		event.GetEventId(),
	).Return(nil)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeStartChildWorkflowExecutionFailed() {
	version := int64(1)
	requestID := uuid.NewString()

	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED
	event := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		StartChildWorkflowExecutionFailedEventAttributes: &historypb.StartChildWorkflowExecutionFailedEventAttributes{},
	}.Build()
	s.mockMutableState.EXPECT().ApplyStartChildWorkflowExecutionFailedEvent(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionStarted() {
	version := int64(1)
	requestID := uuid.NewString()

	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED
	event := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		ChildWorkflowExecutionStartedEventAttributes: &historypb.ChildWorkflowExecutionStartedEventAttributes{},
	}.Build()
	s.mockMutableState.EXPECT().ApplyChildWorkflowExecutionStartedEvent(protomock.Eq(event), nil).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionTimedOut() {
	version := int64(1)
	requestID := uuid.NewString()

	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT
	event := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		ChildWorkflowExecutionTimedOutEventAttributes: &historypb.ChildWorkflowExecutionTimedOutEventAttributes{},
	}.Build()
	s.mockMutableState.EXPECT().ApplyChildWorkflowExecutionTimedOutEvent(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionTerminated() {
	version := int64(1)
	requestID := uuid.NewString()

	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED
	event := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		ChildWorkflowExecutionTerminatedEventAttributes: &historypb.ChildWorkflowExecutionTerminatedEventAttributes{},
	}.Build()
	s.mockMutableState.EXPECT().ApplyChildWorkflowExecutionTerminatedEvent(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionFailed() {
	version := int64(1)
	requestID := uuid.NewString()

	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED
	event := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		ChildWorkflowExecutionFailedEventAttributes: &historypb.ChildWorkflowExecutionFailedEventAttributes{},
	}.Build()
	s.mockMutableState.EXPECT().ApplyChildWorkflowExecutionFailedEvent(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionCompleted() {
	version := int64(1)
	requestID := uuid.NewString()

	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED
	event := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		ChildWorkflowExecutionCompletedEventAttributes: &historypb.ChildWorkflowExecutionCompletedEventAttributes{},
	}.Build()
	s.mockMutableState.EXPECT().ApplyChildWorkflowExecutionCompletedEvent(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

// cancel external workflow operations

func (s *stateBuilderSuite) TestApplyEvents_EventTypeRequestCancelExternalWorkflowExecutionInitiated() {
	version := int64(1)
	requestID := uuid.NewString()

	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	targetWorkflowID := "some random target workflow ID"
	targetRunID := uuid.NewString()
	childWorkflowOnly := true

	now := time.Now().UTC()
	cancellationRequestID := uuid.NewString()
	control := "some random control"
	evenType := enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED
	event := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		RequestCancelExternalWorkflowExecutionInitiatedEventAttributes: historypb.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes_builder{
			Namespace:   tests.TargetNamespace.String(),
			NamespaceId: tests.TargetNamespaceID.String(),
			WorkflowExecution: commonpb.WorkflowExecution_builder{
				WorkflowId: targetWorkflowID,
				RunId:      targetRunID,
			}.Build(),
			ChildWorkflowOnly: childWorkflowOnly,
			Control:           control,
		}.Build(),
	}.Build()
	rci := persistencespb.RequestCancelInfo_builder{
		Version:          event.GetVersion(),
		InitiatedEventId: event.GetEventId(),
		CancelRequestId:  cancellationRequestID,
	}.Build()

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
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeRequestCancelExternalWorkflowExecutionFailed() {
	version := int64(1)
	requestID := uuid.NewString()

	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED
	event := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		RequestCancelExternalWorkflowExecutionFailedEventAttributes: &historypb.RequestCancelExternalWorkflowExecutionFailedEventAttributes{},
	}.Build()
	s.mockMutableState.EXPECT().ApplyRequestCancelExternalWorkflowExecutionFailedEvent(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeExternalWorkflowExecutionCancelRequested() {
	version := int64(1)
	requestID := uuid.NewString()

	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED
	event := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		ExternalWorkflowExecutionCancelRequestedEventAttributes: &historypb.ExternalWorkflowExecutionCancelRequestedEventAttributes{},
	}.Build()
	s.mockMutableState.EXPECT().ApplyExternalWorkflowExecutionCancelRequested(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionCanceled() {
	version := int64(1)
	requestID := uuid.NewString()

	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED
	event := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		ChildWorkflowExecutionCanceledEventAttributes: &historypb.ChildWorkflowExecutionCanceledEventAttributes{},
	}.Build()
	s.mockMutableState.EXPECT().ApplyChildWorkflowExecutionCanceledEvent(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

// signal external workflow operations

func (s *stateBuilderSuite) TestApplyEvents_EventTypeSignalExternalWorkflowExecutionInitiated() {
	version := int64(1)
	requestID := uuid.NewString()
	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()
	targetWorkflowID := "some random target workflow ID"
	targetRunID := uuid.NewString()
	childWorkflowOnly := true

	now := time.Now().UTC()
	signalRequestID := uuid.NewString()
	signalName := "some random signal name"
	signalInput := payloads.EncodeString("some random signal input")
	signalHeader := commonpb.Header_builder{
		Fields: map[string]*commonpb.Payload{"signal header key": payload.EncodeString("signal header value")},
	}.Build()
	control := "some random control"
	evenType := enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED
	event := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		SignalExternalWorkflowExecutionInitiatedEventAttributes: historypb.SignalExternalWorkflowExecutionInitiatedEventAttributes_builder{
			Namespace:   tests.TargetNamespace.String(),
			NamespaceId: tests.TargetNamespaceID.String(),
			WorkflowExecution: commonpb.WorkflowExecution_builder{
				WorkflowId: targetWorkflowID,
				RunId:      targetRunID,
			}.Build(),
			SignalName:        signalName,
			Input:             signalInput,
			ChildWorkflowOnly: childWorkflowOnly,
			Header:            signalHeader,
			Control:           control,
		}.Build(),
	}.Build()
	si := persistencespb.SignalInfo_builder{
		Version:          event.GetVersion(),
		InitiatedEventId: event.GetEventId(),
		RequestId:        signalRequestID,
	}.Build()

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
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeSignalExternalWorkflowExecutionFailed() {
	version := int64(1)
	requestID := uuid.NewString()

	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED
	event := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		SignalExternalWorkflowExecutionFailedEventAttributes: &historypb.SignalExternalWorkflowExecutionFailedEventAttributes{},
	}.Build()
	s.mockMutableState.EXPECT().ApplySignalExternalWorkflowExecutionFailedEvent(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeExternalWorkflowExecutionSignaled() {
	version := int64(1)
	requestID := uuid.NewString()

	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED
	event := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		ExternalWorkflowExecutionSignaledEventAttributes: &historypb.ExternalWorkflowExecutionSignaledEventAttributes{},
	}.Build()
	s.mockMutableState.EXPECT().ApplyExternalWorkflowExecutionSignaled(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.Nil(err)
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionUpdateAccepted() {
	version := int64(1)
	requestID := uuid.NewString()

	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED
	event := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		WorkflowExecutionUpdateAcceptedEventAttributes: historypb.WorkflowExecutionUpdateAcceptedEventAttributes_builder{
			ProtocolInstanceId: s.T().Name(),
		}.Build(),
	}.Build()
	s.mockMutableState.EXPECT().ApplyWorkflowExecutionUpdateAcceptedEvent(protomock.Eq(event)).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.NoError(err)
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionUpdateCompleted() {
	version := int64(1)
	requestID := uuid.NewString()

	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED
	event := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		WorkflowExecutionUpdateCompletedEventAttributes: &historypb.WorkflowExecutionUpdateCompletedEventAttributes{},
	}.Build()
	s.mockMutableState.EXPECT().ApplyWorkflowExecutionUpdateCompletedEvent(protomock.Eq(event), event.GetEventId()).Return(nil)
	s.mockUpdateVersion(event)
	s.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := s.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, s.toHistory(event), nil, "")
	s.NoError(err)
	s.Equal(event.GetTaskId(), s.executionInfo.GetLastRunningClock())
}

func (s *stateBuilderSuite) TestApplyEvents_HSMRegistry() {
	version := int64(1)
	requestID := uuid.NewString()

	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}.Build()

	now := time.Now().UTC()
	event := historypb.HistoryEvent_builder{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   5,
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
		NexusOperationScheduledEventAttributes: historypb.NexusOperationScheduledEventAttributes_builder{
			EndpointId:                   "endpoint-id",
			Endpoint:                     "endpoint",
			Service:                      "service",
			Operation:                    "operation",
			WorkflowTaskCompletedEventId: 4,
			RequestId:                    "request-id",
		}.Build(),
	}.Build()
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
	shardContext historyi.ShardContext,
	mutableState historyi.MutableState,
) TaskGenerator {
	if mutableState == p.mockMutableState {
		return p.mockTaskGenerator
	}

	return NewTaskGenerator(
		shardContext.GetNamespaceRegistry(),
		mutableState,
		shardContext.GetConfig(),
		shardContext.GetArchivalMetadata(),
		shardContext.GetLogger(),
	)
}
