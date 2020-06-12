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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	commonpb "go.temporal.io/temporal-proto/common/v1"
	enumspb "go.temporal.io/temporal-proto/enums/v1"
	tasklistpb "go.temporal.io/temporal-proto/tasklist/v1"
	"go.temporal.io/temporal-proto/workflowservice/v1"

	enumsgenpb "github.com/temporalio/temporal/.gen/proto/enums/v1"
	"github.com/temporalio/temporal/.gen/proto/historyservice/v1"
	"github.com/temporalio/temporal/.gen/proto/matchingservice/v1"
	"github.com/temporalio/temporal/.gen/proto/matchingservicemock/v1"
	"github.com/temporalio/temporal/.gen/proto/persistenceblobs/v1"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/clock"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/definition"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/mocks"
	"github.com/temporalio/temporal/common/persistence"
)

type (
	timerQueueActiveTaskExecutorSuite struct {
		suite.Suite
		*require.Assertions

		controller               *gomock.Controller
		mockShard                *shardContextTest
		mockTxProcessor          *MocktransferQueueProcessor
		mockReplicationProcessor *MockReplicatorQueueProcessor
		mockTimerProcessor       *MocktimerQueueProcessor
		mockNamespaceCache       *cache.MockNamespaceCache
		mockMatchingClient       *matchingservicemock.MockMatchingServiceClient
		mockClusterMetadata      *cluster.MockMetadata

		mockHistoryEngine *historyEngineImpl
		mockExecutionMgr  *mocks.ExecutionManager
		mockHistoryV2Mgr  *mocks.HistoryV2Manager

		logger                       log.Logger
		namespaceID                  string
		namespaceEntry               *cache.NamespaceCacheEntry
		version                      int64
		now                          time.Time
		timeSource                   *clock.EventTimeSource
		timerQueueActiveTaskExecutor *timerQueueActiveTaskExecutor
	}
)

func TestTimerQueueActiveTaskExecutorSuite(t *testing.T) {
	s := new(timerQueueActiveTaskExecutorSuite)
	suite.Run(t, s)
}

func (s *timerQueueActiveTaskExecutorSuite) SetupSuite() {

}

func (s *timerQueueActiveTaskExecutorSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.namespaceID = testNamespaceID
	s.namespaceEntry = testGlobalNamespaceEntry
	s.version = s.namespaceEntry.GetFailoverVersion()
	s.now = time.Now()
	s.timeSource = clock.NewEventTimeSource().Update(s.now)

	s.controller = gomock.NewController(s.T())
	s.mockTxProcessor = NewMocktransferQueueProcessor(s.controller)
	s.mockReplicationProcessor = NewMockReplicatorQueueProcessor(s.controller)
	s.mockTimerProcessor = NewMocktimerQueueProcessor(s.controller)
	s.mockTxProcessor.EXPECT().NotifyNewTask(gomock.Any(), gomock.Any()).AnyTimes()
	s.mockReplicationProcessor.EXPECT().notifyNewTask().AnyTimes()
	s.mockTimerProcessor.EXPECT().NotifyNewTimers(gomock.Any(), gomock.Any()).AnyTimes()

	config := NewDynamicConfigForTest()
	s.mockShard = newTestShardContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistenceblobs.ShardInfo{
				RangeId:          1,
				TransferAckLevel: 0,
			}},
		config,
	)
	s.mockShard.eventsCache = newEventsCache(s.mockShard)
	s.mockShard.resource.TimeSource = s.timeSource

	s.mockNamespaceCache = s.mockShard.resource.NamespaceCache
	s.mockMatchingClient = s.mockShard.resource.MatchingClient
	s.mockExecutionMgr = s.mockShard.resource.ExecutionMgr
	s.mockHistoryV2Mgr = s.mockShard.resource.HistoryMgr
	s.mockClusterMetadata = s.mockShard.resource.ClusterMetadata
	// ack manager will use the namespace information
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(testGlobalNamespaceEntry, nil).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.version).Return(s.mockClusterMetadata.GetCurrentClusterName()).AnyTimes()

	s.logger = s.mockShard.GetLogger()

	historyCache := newHistoryCache(s.mockShard)
	h := &historyEngineImpl{
		currentClusterName:   s.mockShard.GetService().GetClusterMetadata().GetCurrentClusterName(),
		shard:                s.mockShard,
		clusterMetadata:      s.mockClusterMetadata,
		executionManager:     s.mockExecutionMgr,
		historyCache:         historyCache,
		logger:               s.logger,
		tokenSerializer:      common.NewProtoTaskTokenSerializer(),
		metricsClient:        s.mockShard.GetMetricsClient(),
		historyEventNotifier: newHistoryEventNotifier(clock.NewRealTimeSource(), metrics.NewClient(tally.NoopScope, metrics.History), func(string) int { return 0 }),
		txProcessor:          s.mockTxProcessor,
		replicatorProcessor:  s.mockReplicationProcessor,
		timerProcessor:       s.mockTimerProcessor,
	}
	s.mockShard.SetEngine(h)
	s.mockHistoryEngine = h

	s.timerQueueActiveTaskExecutor = newTimerQueueActiveTaskExecutor(
		s.mockShard,
		h,
		newTimerQueueActiveProcessor(
			s.mockShard,
			h,
			s.mockMatchingClient,
			newTaskAllocator(s.mockShard),
			nil,
			s.logger,
		),
		s.logger,
		s.mockShard.GetMetricsClient(),
		config,
	).(*timerQueueActiveTaskExecutor)
}

func (s *timerQueueActiveTaskExecutorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.Finish(s.T())
}

func (s *timerQueueActiveTaskExecutorSuite) TestProcessUserTimerTimeout_Fire() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(
		s.mockShard,
		s.mockShard.GetEventsCache(),
		s.logger,
		s.version,
		execution.GetRunId(),
	)
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:               &commonpb.WorkflowType{Name: workflowType},
				TaskList:                   &tasklistpb.TaskList{Name: taskListName},
				WorkflowRunTimeoutSeconds:  2,
				WorkflowTaskTimeoutSeconds: 1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	timerID := "timer"
	timerTimeout := 2 * time.Second
	event, _ = addTimerStartedEvent(mutableState, event.GetEventId(), timerID, int64(timerTimeout.Seconds()))

	timerSequence := newTimerSequence(s.timeSource, mutableState)
	mutableState.insertTimerTasks = nil
	modified, err := timerSequence.createNextUserTimer()
	s.NoError(err)
	s.True(modified)
	task := mutableState.insertTimerTasks[0]
	protoTaskTime, err := types.TimestampProto(task.(*persistence.UserTimerTask).GetVisibilityTimestamp())
	s.NoError(err)
	timerTask := &persistenceblobs.TimerTaskInfo{
		Version:             s.version,
		NamespaceId:         s.namespaceID,
		WorkflowId:          execution.GetWorkflowId(),
		RunId:               execution.GetRunId(),
		TaskId:              int64(100),
		TaskType:            enumsgenpb.TASK_TYPE_USER_TIMER,
		TimeoutType:         int32(enumspb.TIMEOUT_TYPE_START_TO_CLOSE),
		VisibilityTimestamp: protoTaskTime,
		EventId:             event.EventId,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	s.timeSource.Update(s.now.Add(2 * timerTimeout))
	err = s.timerQueueActiveTaskExecutor.execute(timerTask, true)
	s.NoError(err)

	_, ok := s.getMutableStateFromCache(s.namespaceID, execution.GetWorkflowId(), execution.GetRunId()).GetUserTimerInfo(timerID)
	s.False(ok)
}

func (s *timerQueueActiveTaskExecutorSuite) TestProcessUserTimerTimeout_Noop() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(
		s.mockShard,
		s.mockShard.GetEventsCache(),
		s.logger,
		s.version,
		execution.GetRunId(),
	)
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                    &commonpb.WorkflowType{Name: workflowType},
				TaskList:                        &tasklistpb.TaskList{Name: taskListName},
				WorkflowExecutionTimeoutSeconds: 2,
				WorkflowTaskTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	timerID := "timer"
	timerTimeout := 2 * time.Second
	event, _ = addTimerStartedEvent(mutableState, event.GetEventId(), timerID, int64(timerTimeout.Seconds()))

	timerSequence := newTimerSequence(s.timeSource, mutableState)
	mutableState.insertTimerTasks = nil
	modified, err := timerSequence.createNextUserTimer()
	s.NoError(err)
	s.True(modified)
	task := mutableState.insertTimerTasks[0]
	protoTaskTime, err := types.TimestampProto(task.(*persistence.UserTimerTask).GetVisibilityTimestamp())
	s.NoError(err)
	timerTask := &persistenceblobs.TimerTaskInfo{
		Version:             s.version,
		NamespaceId:         s.namespaceID,
		WorkflowId:          execution.GetWorkflowId(),
		RunId:               execution.GetRunId(),
		TaskId:              int64(100),
		TaskType:            enumsgenpb.TASK_TYPE_USER_TIMER,
		TimeoutType:         int32(enumspb.TIMEOUT_TYPE_START_TO_CLOSE),
		VisibilityTimestamp: protoTaskTime,
		EventId:             event.EventId,
	}

	event = addTimerFiredEvent(mutableState, timerID)

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.timeSource.Update(s.now.Add(2 * timerTimeout))
	err = s.timerQueueActiveTaskExecutor.execute(timerTask, true)
	s.NoError(err)
}

func (s *timerQueueActiveTaskExecutorSuite) TestProcessActivityTimeout_NoRetryPolicy_Fire() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                    &commonpb.WorkflowType{Name: workflowType},
				TaskList:                        &tasklistpb.TaskList{Name: taskListName},
				WorkflowExecutionTimeoutSeconds: 2,
				WorkflowTaskTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	tasklist := "tasklist"
	activityID := "activity"
	activityType := "activity type"
	timerTimeout := 2 * time.Second
	scheduledEvent, _ := addActivityTaskScheduledEvent(
		mutableState,
		event.GetEventId(),
		activityID,
		activityType,
		tasklist,
		nil,
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
	)

	timerSequence := newTimerSequence(s.timeSource, mutableState)
	mutableState.insertTimerTasks = nil
	modified, err := timerSequence.createNextActivityTimer()
	s.NoError(err)
	s.True(modified)
	task := mutableState.insertTimerTasks[0]
	protoTaskTime, err := types.TimestampProto(task.(*persistence.ActivityTimeoutTask).GetVisibilityTimestamp())
	s.NoError(err)
	timerTask := &persistenceblobs.TimerTaskInfo{
		Version:             s.version,
		NamespaceId:         s.namespaceID,
		WorkflowId:          execution.GetWorkflowId(),
		RunId:               execution.GetRunId(),
		TaskId:              int64(100),
		TaskType:            enumsgenpb.TASK_TYPE_ACTIVITY_TIMEOUT,
		TimeoutType:         int32(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE),
		VisibilityTimestamp: protoTaskTime,
		EventId:             di.ScheduleID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	s.timeSource.Update(s.now.Add(2 * timerTimeout))
	err = s.timerQueueActiveTaskExecutor.execute(timerTask, true)
	s.NoError(err)

	_, ok := s.getMutableStateFromCache(s.namespaceID, execution.GetWorkflowId(), execution.GetRunId()).GetActivityInfo(scheduledEvent.GetEventId())
	s.False(ok)
}

func (s *timerQueueActiveTaskExecutorSuite) TestProcessActivityTimeout_NoRetryPolicy_Noop() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                    &commonpb.WorkflowType{Name: workflowType},
				TaskList:                        &tasklistpb.TaskList{Name: taskListName},
				WorkflowExecutionTimeoutSeconds: 2,
				WorkflowTaskTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	identity := "identity"
	tasklist := "tasklist"
	activityID := "activity"
	activityType := "activity type"
	timerTimeout := 2 * time.Second
	scheduledEvent, _ := addActivityTaskScheduledEvent(
		mutableState,
		event.GetEventId(),
		activityID,
		activityType,
		tasklist,
		nil,
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
	)
	startedEvent := addActivityTaskStartedEvent(mutableState, scheduledEvent.GetEventId(), identity)

	timerSequence := newTimerSequence(s.timeSource, mutableState)
	mutableState.insertTimerTasks = nil
	modified, err := timerSequence.createNextActivityTimer()
	s.NoError(err)
	s.True(modified)
	task := mutableState.insertTimerTasks[0]
	protoTaskTime, err := types.TimestampProto(task.(*persistence.ActivityTimeoutTask).GetVisibilityTimestamp())
	s.NoError(err)
	timerTask := &persistenceblobs.TimerTaskInfo{
		Version:             s.version,
		NamespaceId:         s.namespaceID,
		WorkflowId:          execution.GetWorkflowId(),
		RunId:               execution.GetRunId(),
		TaskId:              int64(100),
		TaskType:            enumsgenpb.TASK_TYPE_ACTIVITY_TIMEOUT,
		TimeoutType:         int32(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE),
		VisibilityTimestamp: protoTaskTime,
		EventId:             di.ScheduleID,
	}

	completeEvent := addActivityTaskCompletedEvent(mutableState, scheduledEvent.GetEventId(), startedEvent.GetEventId(), nil, identity)

	persistenceMutableState := s.createPersistenceMutableState(mutableState, completeEvent.GetEventId(), completeEvent.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.timeSource.Update(s.now.Add(2 * timerTimeout))
	err = s.timerQueueActiveTaskExecutor.execute(timerTask, true)
	s.NoError(err)
}

func (s *timerQueueActiveTaskExecutorSuite) TestProcessActivityTimeout_RetryPolicy_Retry() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                    &commonpb.WorkflowType{Name: workflowType},
				TaskList:                        &tasklistpb.TaskList{Name: taskListName},
				WorkflowRunTimeoutSeconds:       2,
				WorkflowExecutionTimeoutSeconds: 999,
				WorkflowTaskTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	identity := "identity"
	tasklist := "tasklist"
	activityID := "activity"
	activityType := "activity type"
	timerTimeout := 2 * time.Second
	scheduledEvent, _ := addActivityTaskScheduledEventWithRetry(
		mutableState,
		event.GetEventId(),
		activityID,
		activityType,
		tasklist,
		nil,
		999,
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		&commonpb.RetryPolicy{
			InitialIntervalInSeconds: 1,
			BackoffCoefficient:       1.2,
			MaximumIntervalInSeconds: 5,
			MaximumAttempts:          5,
			NonRetryableErrorTypes:   []string{"（╯' - ')╯ ┻━┻ "},
		},
	)
	startedEvent := addActivityTaskStartedEvent(mutableState, scheduledEvent.GetEventId(), identity)
	s.Nil(startedEvent)

	timerSequence := newTimerSequence(s.timeSource, mutableState)
	mutableState.insertTimerTasks = nil
	modified, err := timerSequence.createNextActivityTimer()
	s.NoError(err)
	s.True(modified)
	task := mutableState.insertTimerTasks[0]
	protoTaskTime, err := types.TimestampProto(task.(*persistence.ActivityTimeoutTask).GetVisibilityTimestamp())
	s.NoError(err)
	timerTask := &persistenceblobs.TimerTaskInfo{
		Version:             s.version,
		NamespaceId:         s.namespaceID,
		WorkflowId:          execution.GetWorkflowId(),
		RunId:               execution.GetRunId(),
		TaskId:              int64(100),
		TaskType:            enumsgenpb.TASK_TYPE_ACTIVITY_TIMEOUT,
		TimeoutType:         int32(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE),
		VisibilityTimestamp: protoTaskTime,
		EventId:             di.ScheduleID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	s.timeSource.Update(s.now.Add(2 * timerTimeout))
	err = s.timerQueueActiveTaskExecutor.execute(timerTask, true)
	s.NoError(err)

	activityInfo, ok := s.getMutableStateFromCache(s.namespaceID, execution.GetWorkflowId(), execution.GetRunId()).GetActivityInfo(scheduledEvent.GetEventId())
	s.True(ok)
	s.Equal(scheduledEvent.GetEventId(), activityInfo.ScheduleID)
	s.Equal(common.EmptyEventID, activityInfo.StartedID)
	// only a schedule to start timer will be created, apart from the retry timer
	s.Equal(int32(timerTaskStatusCreatedScheduleToStart), activityInfo.TimerTaskStatus)
}

func (s *timerQueueActiveTaskExecutorSuite) TestProcessActivityTimeout_RetryPolicy_Fire() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                    &commonpb.WorkflowType{Name: workflowType},
				TaskList:                        &tasklistpb.TaskList{Name: taskListName},
				WorkflowExecutionTimeoutSeconds: 2,
				WorkflowTaskTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	tasklist := "tasklist"
	activityID := "activity"
	activityType := "activity type"
	timerTimeout := 2 * time.Second
	scheduledEvent, _ := addActivityTaskScheduledEventWithRetry(
		mutableState,
		event.GetEventId(),
		activityID,
		activityType,
		tasklist,
		nil,
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		&commonpb.RetryPolicy{
			InitialIntervalInSeconds: 1,
			BackoffCoefficient:       1.2,
			MaximumIntervalInSeconds: 5,
			MaximumAttempts:          5,
			NonRetryableErrorTypes:   []string{"（╯' - ')╯ ┻━┻ "},
		},
	)

	timerSequence := newTimerSequence(s.timeSource, mutableState)
	mutableState.insertTimerTasks = nil
	modified, err := timerSequence.createNextActivityTimer()
	s.NoError(err)
	s.True(modified)
	task := mutableState.insertTimerTasks[0]
	protoTaskTime, err := types.TimestampProto(task.(*persistence.ActivityTimeoutTask).GetVisibilityTimestamp())
	s.NoError(err)
	timerTask := &persistenceblobs.TimerTaskInfo{
		Version:             s.version,
		NamespaceId:         s.namespaceID,
		WorkflowId:          execution.GetWorkflowId(),
		RunId:               execution.GetRunId(),
		TaskId:              int64(100),
		TaskType:            enumsgenpb.TASK_TYPE_ACTIVITY_TIMEOUT,
		TimeoutType:         int32(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE),
		VisibilityTimestamp: protoTaskTime,
		EventId:             di.ScheduleID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	s.timeSource.Update(s.now.Add(2 * timerTimeout))
	err = s.timerQueueActiveTaskExecutor.execute(timerTask, true)
	s.NoError(err)

	_, ok := s.getMutableStateFromCache(s.namespaceID, execution.GetWorkflowId(), execution.GetRunId()).GetActivityInfo(scheduledEvent.GetEventId())
	s.False(ok)
}

func (s *timerQueueActiveTaskExecutorSuite) TestProcessActivityTimeout_RetryPolicy_Noop() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                    &commonpb.WorkflowType{Name: workflowType},
				TaskList:                        &tasklistpb.TaskList{Name: taskListName},
				WorkflowExecutionTimeoutSeconds: 2,
				WorkflowTaskTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	identity := "identity"
	tasklist := "tasklist"
	activityID := "activity"
	activityType := "activity type"
	timerTimeout := 2 * time.Second
	scheduledEvent, _ := addActivityTaskScheduledEventWithRetry(
		mutableState,
		event.GetEventId(),
		activityID,
		activityType,
		tasklist,
		nil,
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		&commonpb.RetryPolicy{
			InitialIntervalInSeconds: 1,
			BackoffCoefficient:       1.2,
			MaximumIntervalInSeconds: 5,
			MaximumAttempts:          5,
			NonRetryableErrorTypes:   []string{"（╯' - ')╯ ┻━┻ "},
		},
	)
	startedEvent := addActivityTaskStartedEvent(mutableState, scheduledEvent.GetEventId(), identity)
	s.Nil(startedEvent)

	timerSequence := newTimerSequence(s.timeSource, mutableState)
	mutableState.insertTimerTasks = nil
	modified, err := timerSequence.createNextActivityTimer()
	s.NoError(err)
	s.True(modified)
	task := mutableState.insertTimerTasks[0]
	protoTaskTime, err := types.TimestampProto(task.(*persistence.ActivityTimeoutTask).GetVisibilityTimestamp())
	s.NoError(err)
	timerTask := &persistenceblobs.TimerTaskInfo{
		Version:             s.version,
		NamespaceId:         s.namespaceID,
		WorkflowId:          execution.GetWorkflowId(),
		RunId:               execution.GetRunId(),
		TaskId:              int64(100),
		TaskType:            enumsgenpb.TASK_TYPE_ACTIVITY_TIMEOUT,
		TimeoutType:         int32(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE),
		VisibilityTimestamp: protoTaskTime,
		EventId:             di.ScheduleID,
	}

	completeEvent := addActivityTaskCompletedEvent(mutableState, scheduledEvent.GetEventId(), common.TransientEventID, nil, identity)

	persistenceMutableState := s.createPersistenceMutableState(mutableState, completeEvent.GetEventId(), completeEvent.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.timeSource.Update(s.now.Add(2 * timerTimeout))
	err = s.timerQueueActiveTaskExecutor.execute(timerTask, true)
	s.NoError(err)
}

func (s *timerQueueActiveTaskExecutorSuite) TestProcessActivityTimeout_Heartbeat_Noop() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                    &commonpb.WorkflowType{Name: workflowType},
				TaskList:                        &tasklistpb.TaskList{Name: taskListName},
				WorkflowExecutionTimeoutSeconds: 2,
				WorkflowTaskTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	identity := "identity"
	tasklist := "tasklist"
	activityID := "activity"
	activityType := "activity type"
	timerTimeout := 2 * time.Second
	heartbeatTimerTimeout := time.Second
	scheduledEvent, _ := addActivityTaskScheduledEventWithRetry(
		mutableState,
		event.GetEventId(),
		activityID,
		activityType,
		tasklist,
		nil,
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		int32(heartbeatTimerTimeout.Seconds()),
		&commonpb.RetryPolicy{
			InitialIntervalInSeconds: 1,
			BackoffCoefficient:       1.2,
			MaximumIntervalInSeconds: 5,
			MaximumAttempts:          5,
			NonRetryableErrorTypes:   []string{"（╯' - ')╯ ┻━┻ "},
		},
	)
	startedEvent := addActivityTaskStartedEvent(mutableState, scheduledEvent.GetEventId(), identity)
	s.Nil(startedEvent)

	timerSequence := newTimerSequence(s.timeSource, mutableState)
	mutableState.insertTimerTasks = nil
	modified, err := timerSequence.createNextActivityTimer()
	s.NoError(err)
	s.True(modified)
	task := mutableState.insertTimerTasks[0]
	s.Equal(int(timerTypeHeartbeat), task.(*persistence.ActivityTimeoutTask).TimeoutType)
	protoTaskTime, err := types.TimestampProto(task.(*persistence.ActivityTimeoutTask).GetVisibilityTimestamp().Add(-time.Second))
	s.NoError(err)
	timerTask := &persistenceblobs.TimerTaskInfo{
		Version:             s.version,
		NamespaceId:         s.namespaceID,
		WorkflowId:          execution.GetWorkflowId(),
		RunId:               execution.GetRunId(),
		TaskId:              int64(100),
		TaskType:            enumsgenpb.TASK_TYPE_ACTIVITY_TIMEOUT,
		TimeoutType:         int32(enumspb.TIMEOUT_TYPE_HEARTBEAT),
		VisibilityTimestamp: protoTaskTime,
		EventId:             scheduledEvent.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	err = s.timerQueueActiveTaskExecutor.execute(timerTask, true)
	s.NoError(err)
}

func (s *timerQueueActiveTaskExecutorSuite) TestDecisionTimeout_Fire() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                    &commonpb.WorkflowType{Name: workflowType},
				TaskList:                        &tasklistpb.TaskList{Name: taskListName},
				WorkflowExecutionTimeoutSeconds: 2,
				WorkflowTaskTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	startedEvent := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())

	protoTime, err := types.TimestampProto(s.now)
	s.NoError(err)
	timerTask := &persistenceblobs.TimerTaskInfo{
		Version:             s.version,
		NamespaceId:         s.namespaceID,
		WorkflowId:          execution.GetWorkflowId(),
		RunId:               execution.GetRunId(),
		TaskId:              int64(100),
		TaskType:            enumsgenpb.TASK_TYPE_DECISION_TIMEOUT,
		TimeoutType:         int32(enumspb.TIMEOUT_TYPE_START_TO_CLOSE),
		VisibilityTimestamp: protoTime,
		EventId:             di.ScheduleID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, startedEvent.GetEventId(), startedEvent.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	err = s.timerQueueActiveTaskExecutor.execute(timerTask, true)
	s.NoError(err)

	decisionInfo, ok := s.getMutableStateFromCache(s.namespaceID, execution.GetWorkflowId(), execution.GetRunId()).GetPendingDecision()
	s.True(ok)
	s.True(decisionInfo.ScheduleID != common.EmptyEventID)
	s.Equal(common.EmptyEventID, decisionInfo.StartedID)
	s.Equal(int64(1), decisionInfo.Attempt)
}

func (s *timerQueueActiveTaskExecutorSuite) TestDecisionTimeout_Noop() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                    &commonpb.WorkflowType{Name: workflowType},
				TaskList:                        &tasklistpb.TaskList{Name: taskListName},
				WorkflowExecutionTimeoutSeconds: 2,
				WorkflowTaskTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	startedEvent := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())

	protoTime, err := types.TimestampProto(s.now)
	s.NoError(err)
	timerTask := &persistenceblobs.TimerTaskInfo{
		Version:             s.version,
		NamespaceId:         s.namespaceID,
		WorkflowId:          execution.GetWorkflowId(),
		RunId:               execution.GetRunId(),
		TaskId:              int64(100),
		TaskType:            enumsgenpb.TASK_TYPE_DECISION_TIMEOUT,
		TimeoutType:         int32(enumspb.TIMEOUT_TYPE_START_TO_CLOSE),
		VisibilityTimestamp: protoTime,
		EventId:             di.ScheduleID - 1,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, startedEvent.GetEventId(), startedEvent.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil).Once()

	err = s.timerQueueActiveTaskExecutor.execute(timerTask, true)
	s.NoError(err)
}

func (s *timerQueueActiveTaskExecutorSuite) TestWorkflowBackoffTimer_Fire() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	event, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                    &commonpb.WorkflowType{Name: workflowType},
				TaskList:                        &tasklistpb.TaskList{Name: taskListName},
				WorkflowExecutionTimeoutSeconds: 2,
				WorkflowTaskTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	protoTaskTime, err := types.TimestampProto(s.now)
	s.NoError(err)
	timerTask := &persistenceblobs.TimerTaskInfo{
		Version:             s.version,
		NamespaceId:         s.namespaceID,
		WorkflowId:          execution.GetWorkflowId(),
		RunId:               execution.GetRunId(),
		TaskId:              int64(100),
		TaskType:            enumsgenpb.TASK_TYPE_WORKFLOW_BACKOFF_TIMER,
		TimeoutType:         persistence.WorkflowBackoffTimeoutTypeRetry,
		VisibilityTimestamp: protoTaskTime,
		EventId:             0,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	err = s.timerQueueActiveTaskExecutor.execute(timerTask, true)
	s.NoError(err)

	decisionInfo, ok := s.getMutableStateFromCache(s.namespaceID, execution.GetWorkflowId(), execution.GetRunId()).GetPendingDecision()
	s.True(ok)
	s.True(decisionInfo.ScheduleID != common.EmptyEventID)
	s.Equal(common.EmptyEventID, decisionInfo.StartedID)
	s.Equal(int64(0), decisionInfo.Attempt)
}

func (s *timerQueueActiveTaskExecutorSuite) TestWorkflowBackoffTimer_Noop() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                    &commonpb.WorkflowType{Name: workflowType},
				TaskList:                        &tasklistpb.TaskList{Name: taskListName},
				WorkflowExecutionTimeoutSeconds: 2,
				WorkflowTaskTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	protoTaskTime, err := types.TimestampProto(s.now)
	s.NoError(err)
	timerTask := &persistenceblobs.TimerTaskInfo{
		Version:             s.version,
		NamespaceId:         s.namespaceID,
		WorkflowId:          execution.GetWorkflowId(),
		RunId:               execution.GetRunId(),
		TaskId:              int64(100),
		TaskType:            enumsgenpb.TASK_TYPE_WORKFLOW_BACKOFF_TIMER,
		TimeoutType:         persistence.WorkflowBackoffTimeoutTypeRetry,
		VisibilityTimestamp: protoTaskTime,
		EventId:             0,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil).Once()

	err = s.timerQueueActiveTaskExecutor.execute(timerTask, true)
	s.NoError(err)
}

func (s *timerQueueActiveTaskExecutorSuite) TestActivityRetryTimer_Fire() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                    &commonpb.WorkflowType{Name: workflowType},
				TaskList:                        &tasklistpb.TaskList{Name: taskListName},
				WorkflowExecutionTimeoutSeconds: 2,
				WorkflowTaskTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	tasklist := "tasklist"
	activityID := "activity"
	activityType := "activity type"
	timerTimeout := 2 * time.Second
	scheduledEvent, activityInfo := addActivityTaskScheduledEventWithRetry(
		mutableState,
		event.GetEventId(),
		activityID,
		activityType,
		tasklist,
		nil,
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		&commonpb.RetryPolicy{
			InitialIntervalInSeconds: 1,
			BackoffCoefficient:       1.2,
			MaximumIntervalInSeconds: 5,
			MaximumAttempts:          5,
			NonRetryableErrorTypes:   []string{"（╯' - ')╯ ┻━┻ "},
		},
	)
	activityInfo.Attempt = 1

	protoTaskTime, err := types.TimestampProto(s.now)
	s.NoError(err)
	timerTask := &persistenceblobs.TimerTaskInfo{
		Version:             s.version,
		NamespaceId:         s.namespaceID,
		WorkflowId:          execution.GetWorkflowId(),
		RunId:               execution.GetRunId(),
		TaskId:              int64(100),
		TaskType:            enumsgenpb.TASK_TYPE_ACTIVITY_RETRY_TIMER,
		TimeoutType:         0,
		VisibilityTimestamp: protoTaskTime,
		EventId:             activityInfo.ScheduleID,
		ScheduleAttempt:     int64(activityInfo.Attempt),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.EXPECT().AddActivityTask(
		gomock.Any(),
		&matchingservice.AddActivityTaskRequest{
			NamespaceId:       activityInfo.NamespaceID,
			SourceNamespaceId: activityInfo.NamespaceID,
			Execution:         &execution,
			TaskList: &tasklistpb.TaskList{
				Name: activityInfo.TaskList,
			},
			ScheduleId:                    activityInfo.ScheduleID,
			ScheduleToStartTimeoutSeconds: activityInfo.ScheduleToStartTimeout,
		},
	).Return(&matchingservice.AddActivityTaskResponse{}, nil).Times(1)

	err = s.timerQueueActiveTaskExecutor.execute(timerTask, true)
	s.NoError(err)
}

func (s *timerQueueActiveTaskExecutorSuite) TestActivityRetryTimer_Noop() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                    &commonpb.WorkflowType{Name: workflowType},
				TaskList:                        &tasklistpb.TaskList{Name: taskListName},
				WorkflowExecutionTimeoutSeconds: 2,
				WorkflowTaskTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	identity := "identity"
	tasklist := "tasklist"
	activityID := "activity"
	activityType := "activity type"
	timerTimeout := 2 * time.Second
	scheduledEvent, activityInfo := addActivityTaskScheduledEventWithRetry(
		mutableState,
		event.GetEventId(),
		activityID,
		activityType,
		tasklist,
		nil,
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		&commonpb.RetryPolicy{
			InitialIntervalInSeconds: 1,
			BackoffCoefficient:       1.2,
			MaximumIntervalInSeconds: 5,
			MaximumAttempts:          5,
			NonRetryableErrorTypes:   []string{"（╯' - ')╯ ┻━┻ "},
		},
	)
	startedEvent := addActivityTaskStartedEvent(mutableState, scheduledEvent.GetEventId(), identity)
	s.Nil(startedEvent)

	protoTaskTime, err := types.TimestampProto(s.now)
	s.NoError(err)
	timerTask := &persistenceblobs.TimerTaskInfo{
		Version:             s.version,
		NamespaceId:         s.namespaceID,
		WorkflowId:          execution.GetWorkflowId(),
		RunId:               execution.GetRunId(),
		TaskId:              int64(100),
		TaskType:            enumsgenpb.TASK_TYPE_ACTIVITY_RETRY_TIMER,
		TimeoutType:         0,
		VisibilityTimestamp: protoTaskTime,
		EventId:             activityInfo.ScheduleID,
		ScheduleAttempt:     int64(activityInfo.Attempt),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	err = s.timerQueueActiveTaskExecutor.execute(timerTask, true)
	s.NoError(err)
}

func (s *timerQueueActiveTaskExecutorSuite) TestWorkflowTimeout_Fire() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                    &commonpb.WorkflowType{Name: workflowType},
				TaskList:                        &tasklistpb.TaskList{Name: taskListName},
				WorkflowExecutionTimeoutSeconds: 2,
				WorkflowTaskTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	startEvent := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = startEvent.GetEventId()
	completionEvent := addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	protoTaskTime, err := types.TimestampProto(s.now)
	s.NoError(err)
	timerTask := &persistenceblobs.TimerTaskInfo{
		Version:             s.version,
		NamespaceId:         s.namespaceID,
		WorkflowId:          execution.GetWorkflowId(),
		RunId:               execution.GetRunId(),
		TaskId:              int64(100),
		TaskType:            enumsgenpb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT,
		TimeoutType:         int32(enumspb.TIMEOUT_TYPE_START_TO_CLOSE),
		VisibilityTimestamp: protoTaskTime,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, completionEvent.GetEventId(), completionEvent.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	err = s.timerQueueActiveTaskExecutor.execute(timerTask, true)
	s.NoError(err)

	running := s.getMutableStateFromCache(s.namespaceID, execution.GetWorkflowId(), execution.GetRunId()).IsWorkflowExecutionRunning()
	s.False(running)
}

func (s *timerQueueActiveTaskExecutorSuite) TestWorkflowTimeout_ContinueAsNew_Retry() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                    &commonpb.WorkflowType{Name: workflowType},
				TaskList:                        &tasklistpb.TaskList{Name: taskListName},
				WorkflowExecutionTimeoutSeconds: 2,
				WorkflowTaskTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)
	// need to override the workflow retry policy
	executionInfo := mutableState.executionInfo
	executionInfo.HasRetryPolicy = true
	executionInfo.WorkflowExpirationTime = s.now.Add(1000 * time.Second)
	executionInfo.MaximumAttempts = 10
	executionInfo.InitialInterval = 1
	executionInfo.MaximumInterval = 1
	executionInfo.BackoffCoefficient = 1

	di := addDecisionTaskScheduledEvent(mutableState)
	startEvent := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = startEvent.GetEventId()
	completionEvent := addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	protoTaskTime, err := types.TimestampProto(s.now)
	s.NoError(err)
	timerTask := &persistenceblobs.TimerTaskInfo{
		Version:             s.version,
		NamespaceId:         s.namespaceID,
		WorkflowId:          execution.GetWorkflowId(),
		RunId:               execution.GetRunId(),
		TaskId:              int64(100),
		TaskType:            enumsgenpb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT,
		TimeoutType:         int32(enumspb.TIMEOUT_TYPE_START_TO_CLOSE),
		VisibilityTimestamp: protoTaskTime,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, completionEvent.GetEventId(), completionEvent.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	// one for current workflow, one for new
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Times(2)
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	err = s.timerQueueActiveTaskExecutor.execute(timerTask, true)
	s.NoError(err)

	state, status := s.getMutableStateFromCache(s.namespaceID, execution.GetWorkflowId(), execution.GetRunId()).GetWorkflowStateStatus()
	s.Equal(enumsgenpb.WORKFLOW_EXECUTION_STATE_COMPLETED, state)
	s.EqualValues(enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW, status)
}

func (s *timerQueueActiveTaskExecutorSuite) TestWorkflowTimeout_ContinueAsNew_Cron() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                    &commonpb.WorkflowType{Name: workflowType},
				TaskList:                        &tasklistpb.TaskList{Name: taskListName},
				WorkflowExecutionTimeoutSeconds: 2,
				WorkflowTaskTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)
	executionInfo := mutableState.executionInfo
	executionInfo.StartTimestamp = s.now
	executionInfo.CronSchedule = "* * * * *"

	di := addDecisionTaskScheduledEvent(mutableState)
	startEvent := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = startEvent.GetEventId()
	completionEvent := addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	protoTaskTime, err := types.TimestampProto(s.now)
	s.NoError(err)
	timerTask := &persistenceblobs.TimerTaskInfo{
		Version:             s.version,
		NamespaceId:         s.namespaceID,
		WorkflowId:          execution.GetWorkflowId(),
		RunId:               execution.GetRunId(),
		TaskId:              int64(100),
		TaskType:            enumsgenpb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT,
		TimeoutType:         int32(enumspb.TIMEOUT_TYPE_START_TO_CLOSE),
		VisibilityTimestamp: protoTaskTime,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, completionEvent.GetEventId(), completionEvent.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	// one for current workflow, one for new
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Times(2)
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	err = s.timerQueueActiveTaskExecutor.execute(timerTask, true)
	s.NoError(err)

	state, status := s.getMutableStateFromCache(s.namespaceID, execution.GetWorkflowId(), execution.GetRunId()).GetWorkflowStateStatus()
	s.Equal(enumsgenpb.WORKFLOW_EXECUTION_STATE_COMPLETED, state)
	s.EqualValues(enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW, status)
}

func (s *timerQueueActiveTaskExecutorSuite) createPersistenceMutableState(
	ms mutableState,
	lastEventID int64,
	lastEventVersion int64,
) *persistence.WorkflowMutableState {

	if ms.GetReplicationState() != nil {
		ms.UpdateReplicationStateLastEventID(lastEventVersion, lastEventID)
	} else if ms.GetVersionHistories() != nil {
		currentVersionHistory, err := ms.GetVersionHistories().GetCurrentVersionHistory()
		s.NoError(err)
		err = currentVersionHistory.AddOrUpdateItem(persistence.NewVersionHistoryItem(
			lastEventID, lastEventVersion,
		))
		s.NoError(err)
	}

	return createMutableState(ms)
}

func (s *timerQueueActiveTaskExecutorSuite) getMutableStateFromCache(
	namespaceID string,
	workflowID string,
	runID string,
) mutableState {

	return s.mockHistoryEngine.historyCache.Get(
		definition.NewWorkflowIdentifier(namespaceID, workflowID, runID),
	).(*workflowExecutionContextImpl).mutableState
}
