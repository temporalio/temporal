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

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/mocks"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/shard"
)

type (
	timerQueueActiveTaskExecutorSuite struct {
		suite.Suite
		*require.Assertions

		controller               *gomock.Controller
		mockShard                *shard.ContextTest
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
	s.now = time.Now().UTC()
	s.timeSource = clock.NewEventTimeSource().Update(s.now)

	s.controller = gomock.NewController(s.T())
	s.mockTxProcessor = NewMocktransferQueueProcessor(s.controller)
	s.mockReplicationProcessor = NewMockReplicatorQueueProcessor(s.controller)
	s.mockTimerProcessor = NewMocktimerQueueProcessor(s.controller)
	s.mockTxProcessor.EXPECT().NotifyNewTask(gomock.Any(), gomock.Any()).AnyTimes()
	s.mockReplicationProcessor.EXPECT().notifyNewTask().AnyTimes()
	s.mockTimerProcessor.EXPECT().NotifyNewTimers(gomock.Any(), gomock.Any()).AnyTimes()

	config := configs.NewDynamicConfigForTest()
	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId:          1,
				RangeId:          1,
				TransferAckLevel: 0,
			}},
		config,
	)
	s.mockShard.EventsCache = events.NewEventsCache(
		convert.Int32Ptr(s.mockShard.GetShardID()),
		s.mockShard.GetConfig().EventsCacheInitialSize(),
		s.mockShard.GetConfig().EventsCacheMaxSize(),
		s.mockShard.GetConfig().EventsCacheTTL(),
		s.mockShard.GetHistoryManager(),
		false,
		s.mockShard.GetLogger(),
		s.mockShard.GetMetricsClient(),
	)

	s.mockShard.Resource.TimeSource = s.timeSource

	s.mockNamespaceCache = s.mockShard.Resource.NamespaceCache
	s.mockMatchingClient = s.mockShard.Resource.MatchingClient
	s.mockExecutionMgr = s.mockShard.Resource.ExecutionMgr
	s.mockHistoryV2Mgr = s.mockShard.Resource.HistoryMgr
	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata
	// ack manager will use the namespace information
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(testGlobalNamespaceEntry, nil).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.version).Return(s.mockClusterMetadata.GetCurrentClusterName()).AnyTimes()

	s.logger = s.mockShard.GetLogger()

	historyCache := newHistoryCache(s.mockShard)
	h := &historyEngineImpl{
		currentClusterName:  s.mockShard.GetService().GetClusterMetadata().GetCurrentClusterName(),
		shard:               s.mockShard,
		clusterMetadata:     s.mockClusterMetadata,
		executionManager:    s.mockExecutionMgr,
		historyCache:        historyCache,
		logger:              s.logger,
		tokenSerializer:     common.NewProtoTaskTokenSerializer(),
		metricsClient:       s.mockShard.GetMetricsClient(),
		eventNotifier:       events.NewNotifier(clock.NewRealTimeSource(), metrics.NewClient(tally.NoopScope, metrics.History), func(string, string) int32 { return 1 }),
		txProcessor:         s.mockTxProcessor,
		replicatorProcessor: s.mockReplicationProcessor,
		timerProcessor:      s.mockTimerProcessor,
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
	taskQueueName := "some random task queue"

	mutableState := newMutableStateBuilderWithVersionHistoriesForTest(
		s.mockShard,
		s.mockShard.GetEventsCache(),
		s.logger,
		s.version,
		execution.GetRunId(),
	)
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  timestamp.DurationPtr(2 * time.Second),
				WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	timerID := "timer"
	timerTimeout := 2 * time.Second
	event, _ = addTimerStartedEvent(mutableState, event.GetEventId(), timerID, timerTimeout)

	timerSequence := newTimerSequence(s.timeSource, mutableState)
	mutableState.insertTimerTasks = nil
	modified, err := timerSequence.createNextUserTimer()
	s.NoError(err)
	s.True(modified)
	task := mutableState.insertTimerTasks[0]
	protoTaskTime := task.(*persistence.UserTimerTask).GetVisibilityTimestamp()
	s.NoError(err)
	timerTask := &persistencespb.TimerTaskInfo{
		ScheduleAttempt: 1,
		Version:         s.version,
		NamespaceId:     s.namespaceID,
		WorkflowId:      execution.GetWorkflowId(),
		RunId:           execution.GetRunId(),
		TaskId:          int64(100),
		TaskType:        enumsspb.TASK_TYPE_USER_TIMER,
		TimeoutType:     enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		VisibilityTime:  &protoTaskTime,
		EventId:         event.EventId,
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
	taskQueueName := "some random task queue"

	mutableState := newMutableStateBuilderWithVersionHistoriesForTest(
		s.mockShard,
		s.mockShard.GetEventsCache(),
		s.logger,
		s.version,
		execution.GetRunId(),
	)
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: timestamp.DurationPtr(2 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	timerID := "timer"
	timerTimeout := 2 * time.Second
	event, _ = addTimerStartedEvent(mutableState, event.GetEventId(), timerID, timerTimeout)

	timerSequence := newTimerSequence(s.timeSource, mutableState)
	mutableState.insertTimerTasks = nil
	modified, err := timerSequence.createNextUserTimer()
	s.NoError(err)
	s.True(modified)
	task := mutableState.insertTimerTasks[0]
	protoTaskTime := task.(*persistence.UserTimerTask).GetVisibilityTimestamp()
	s.NoError(err)
	timerTask := &persistencespb.TimerTaskInfo{
		ScheduleAttempt: 1,
		Version:         s.version,
		NamespaceId:     s.namespaceID,
		WorkflowId:      execution.GetWorkflowId(),
		RunId:           execution.GetRunId(),
		TaskId:          int64(100),
		TaskType:        enumsspb.TASK_TYPE_USER_TIMER,
		TimeoutType:     enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		VisibilityTime:  &protoTaskTime,
		EventId:         event.EventId,
	}

	event = addTimerFiredEvent(mutableState, timerID)
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

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
	taskQueueName := "some random task queue"

	mutableState := newMutableStateBuilderWithVersionHistoriesForTest(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: timestamp.DurationPtr(2 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	taskqueue := "taskqueue"
	activityID := "activity"
	activityType := "activity type"
	timerTimeout := 2 * time.Second
	scheduledEvent, _ := addActivityTaskScheduledEvent(
		mutableState,
		event.GetEventId(),
		activityID,
		activityType,
		taskqueue,
		nil,
		timerTimeout,
		timerTimeout,
		timerTimeout,
		timerTimeout,
	)

	timerSequence := newTimerSequence(s.timeSource, mutableState)
	mutableState.insertTimerTasks = nil
	modified, err := timerSequence.createNextActivityTimer()
	s.NoError(err)
	s.True(modified)
	task := mutableState.insertTimerTasks[0]
	protoTaskTime := task.(*persistence.ActivityTimeoutTask).GetVisibilityTimestamp()
	s.NoError(err)
	timerTask := &persistencespb.TimerTaskInfo{
		ScheduleAttempt: 1,
		Version:         s.version,
		NamespaceId:     s.namespaceID,
		WorkflowId:      execution.GetWorkflowId(),
		RunId:           execution.GetRunId(),
		TaskId:          int64(100),
		TaskType:        enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
		TimeoutType:     enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		VisibilityTime:  &protoTaskTime,
		EventId:         di.ScheduleID,
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
	taskQueueName := "some random task queue"

	mutableState := newMutableStateBuilderWithVersionHistoriesForTest(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: timestamp.DurationPtr(2 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	identity := "identity"
	taskqueue := "taskqueue"
	activityID := "activity"
	activityType := "activity type"
	timerTimeout := 2 * time.Second
	scheduledEvent, _ := addActivityTaskScheduledEvent(
		mutableState,
		event.GetEventId(),
		activityID,
		activityType,
		taskqueue,
		nil,
		timerTimeout,
		timerTimeout,
		timerTimeout,
		timerTimeout,
	)
	startedEvent := addActivityTaskStartedEvent(mutableState, scheduledEvent.GetEventId(), identity)

	timerSequence := newTimerSequence(s.timeSource, mutableState)
	mutableState.insertTimerTasks = nil
	modified, err := timerSequence.createNextActivityTimer()
	s.NoError(err)
	s.True(modified)
	task := mutableState.insertTimerTasks[0]
	protoTaskTime := task.(*persistence.ActivityTimeoutTask).GetVisibilityTimestamp()
	s.NoError(err)
	timerTask := &persistencespb.TimerTaskInfo{
		ScheduleAttempt: 1,
		Version:         s.version,
		NamespaceId:     s.namespaceID,
		WorkflowId:      execution.GetWorkflowId(),
		RunId:           execution.GetRunId(),
		TaskId:          int64(100),
		TaskType:        enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
		TimeoutType:     enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		VisibilityTime:  &protoTaskTime,
		EventId:         di.ScheduleID,
	}

	completeEvent := addActivityTaskCompletedEvent(mutableState, scheduledEvent.GetEventId(), startedEvent.GetEventId(), nil, identity)
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

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
	taskQueueName := "some random task queue"

	mutableState := newMutableStateBuilderWithVersionHistoriesForTest(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:       timestamp.DurationPtr(2 * time.Second),
				WorkflowExecutionTimeout: timestamp.DurationPtr(999 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	identity := "identity"
	taskqueue := "taskqueue"
	activityID := "activity"
	activityType := "activity type"
	timerTimeout := 2 * time.Second
	scheduledEvent, _ := addActivityTaskScheduledEventWithRetry(
		mutableState,
		event.GetEventId(),
		activityID,
		activityType,
		taskqueue,
		nil,
		999*time.Second,
		timerTimeout,
		timerTimeout,
		timerTimeout,
		&commonpb.RetryPolicy{
			InitialInterval:        timestamp.DurationPtr(1 * time.Second),
			BackoffCoefficient:     1.2,
			MaximumInterval:        timestamp.DurationPtr(5 * time.Second),
			MaximumAttempts:        5,
			NonRetryableErrorTypes: []string{"（╯' - ')╯ ┻━┻ "},
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
	protoTaskTime := task.(*persistence.ActivityTimeoutTask).GetVisibilityTimestamp()
	s.NoError(err)
	timerTask := &persistencespb.TimerTaskInfo{
		ScheduleAttempt: 1,
		Version:         s.version,
		NamespaceId:     s.namespaceID,
		WorkflowId:      execution.GetWorkflowId(),
		RunId:           execution.GetRunId(),
		TaskId:          int64(100),
		TaskType:        enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
		TimeoutType:     enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		VisibilityTime:  &protoTaskTime,
		EventId:         di.ScheduleID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	s.timeSource.Update(s.now.Add(2 * timerTimeout))
	err = s.timerQueueActiveTaskExecutor.execute(timerTask, true)
	s.NoError(err)

	activityInfo, ok := s.getMutableStateFromCache(s.namespaceID, execution.GetWorkflowId(), execution.GetRunId()).GetActivityInfo(scheduledEvent.GetEventId())
	s.True(ok)
	s.Equal(scheduledEvent.GetEventId(), activityInfo.ScheduleId)
	s.Equal(common.EmptyEventID, activityInfo.StartedId)
	// only a schedule to start timer will be created, apart from the retry timer
	s.Equal(int32(timerTaskStatusCreatedScheduleToStart), activityInfo.TimerTaskStatus)
}

func (s *timerQueueActiveTaskExecutorSuite) TestProcessActivityTimeout_RetryPolicy_Fire() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := newMutableStateBuilderWithVersionHistoriesForTest(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: timestamp.DurationPtr(2 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	taskqueue := "taskqueue"
	activityID := "activity"
	activityType := "activity type"
	timerTimeout := 2 * time.Second
	scheduledEvent, _ := addActivityTaskScheduledEventWithRetry(
		mutableState,
		event.GetEventId(),
		activityID,
		activityType,
		taskqueue,
		nil,
		timerTimeout,
		timerTimeout,
		timerTimeout,
		timerTimeout,
		&commonpb.RetryPolicy{
			InitialInterval:        timestamp.DurationPtr(1 * time.Second),
			BackoffCoefficient:     1.2,
			MaximumInterval:        timestamp.DurationPtr(5 * time.Second),
			MaximumAttempts:        5,
			NonRetryableErrorTypes: []string{"（╯' - ')╯ ┻━┻ "},
		},
	)

	timerSequence := newTimerSequence(s.timeSource, mutableState)
	mutableState.insertTimerTasks = nil
	modified, err := timerSequence.createNextActivityTimer()
	s.NoError(err)
	s.True(modified)
	task := mutableState.insertTimerTasks[0]
	protoTaskTime := task.(*persistence.ActivityTimeoutTask).GetVisibilityTimestamp()
	s.NoError(err)
	timerTask := &persistencespb.TimerTaskInfo{
		ScheduleAttempt: 1,
		Version:         s.version,
		NamespaceId:     s.namespaceID,
		WorkflowId:      execution.GetWorkflowId(),
		RunId:           execution.GetRunId(),
		TaskId:          int64(100),
		TaskType:        enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
		TimeoutType:     enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		VisibilityTime:  &protoTaskTime,
		EventId:         di.ScheduleID,
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
	taskQueueName := "some random task queue"

	mutableState := newMutableStateBuilderWithVersionHistoriesForTest(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: timestamp.DurationPtr(2 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	identity := "identity"
	taskqueue := "taskqueue"
	activityID := "activity"
	activityType := "activity type"
	timerTimeout := 2 * time.Second
	scheduledEvent, _ := addActivityTaskScheduledEventWithRetry(
		mutableState,
		event.GetEventId(),
		activityID,
		activityType,
		taskqueue,
		nil,
		timerTimeout,
		timerTimeout,
		timerTimeout,
		timerTimeout,
		&commonpb.RetryPolicy{
			InitialInterval:        timestamp.DurationPtr(1 * time.Second),
			BackoffCoefficient:     1.2,
			MaximumInterval:        timestamp.DurationPtr(5 * time.Second),
			MaximumAttempts:        5,
			NonRetryableErrorTypes: []string{"（╯' - ')╯ ┻━┻ "},
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
	protoTaskTime := task.(*persistence.ActivityTimeoutTask).GetVisibilityTimestamp()
	s.NoError(err)
	timerTask := &persistencespb.TimerTaskInfo{
		ScheduleAttempt: 1,
		Version:         s.version,
		NamespaceId:     s.namespaceID,
		WorkflowId:      execution.GetWorkflowId(),
		RunId:           execution.GetRunId(),
		TaskId:          int64(100),
		TaskType:        enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
		TimeoutType:     enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		VisibilityTime:  &protoTaskTime,
		EventId:         di.ScheduleID,
	}

	completeEvent := addActivityTaskCompletedEvent(mutableState, scheduledEvent.GetEventId(), common.TransientEventID, nil, identity)
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

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
	taskQueueName := "some random task queue"

	mutableState := newMutableStateBuilderWithVersionHistoriesForTest(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: timestamp.DurationPtr(2 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	identity := "identity"
	taskqueue := "taskqueue"
	activityID := "activity"
	activityType := "activity type"
	timerTimeout := 2 * time.Second
	heartbeatTimerTimeout := time.Second
	scheduledEvent, _ := addActivityTaskScheduledEventWithRetry(
		mutableState,
		event.GetEventId(),
		activityID,
		activityType,
		taskqueue,
		nil,
		timerTimeout,
		timerTimeout,
		timerTimeout,
		heartbeatTimerTimeout,
		&commonpb.RetryPolicy{
			InitialInterval:        timestamp.DurationPtr(1 * time.Second),
			BackoffCoefficient:     1.2,
			MaximumInterval:        timestamp.DurationPtr(5 * time.Second),
			MaximumAttempts:        5,
			NonRetryableErrorTypes: []string{"（╯' - ')╯ ┻━┻ "},
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
	s.Equal(enumspb.TIMEOUT_TYPE_HEARTBEAT, task.(*persistence.ActivityTimeoutTask).TimeoutType)
	s.NoError(err)
	timerTask := &persistencespb.TimerTaskInfo{
		ScheduleAttempt: 1,
		Version:         s.version,
		NamespaceId:     s.namespaceID,
		WorkflowId:      execution.GetWorkflowId(),
		RunId:           execution.GetRunId(),
		TaskId:          int64(100),
		TaskType:        enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
		TimeoutType:     enumspb.TIMEOUT_TYPE_HEARTBEAT,
		VisibilityTime:  &time.Time{},
		EventId:         scheduledEvent.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	err = s.timerQueueActiveTaskExecutor.execute(timerTask, true)
	s.NoError(err)
}

func (s *timerQueueActiveTaskExecutorSuite) TestWorkflowTaskTimeout_Fire() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := newMutableStateBuilderWithVersionHistoriesForTest(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: timestamp.DurationPtr(2 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	di := addWorkflowTaskScheduledEvent(mutableState)
	startedEvent := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())

	protoTime := s.now
	s.NoError(err)
	timerTask := &persistencespb.TimerTaskInfo{
		ScheduleAttempt: 1,
		Version:         s.version,
		NamespaceId:     s.namespaceID,
		WorkflowId:      execution.GetWorkflowId(),
		RunId:           execution.GetRunId(),
		TaskId:          int64(100),
		TaskType:        enumsspb.TASK_TYPE_WORKFLOW_TASK_TIMEOUT,
		TimeoutType:     enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		VisibilityTime:  &protoTime,
		EventId:         di.ScheduleID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, startedEvent.GetEventId(), startedEvent.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	err = s.timerQueueActiveTaskExecutor.execute(timerTask, true)
	s.NoError(err)

	workflowTask, ok := s.getMutableStateFromCache(s.namespaceID, execution.GetWorkflowId(), execution.GetRunId()).GetPendingWorkflowTask()
	s.True(ok)
	s.True(workflowTask.ScheduleID != common.EmptyEventID)
	s.Equal(common.EmptyEventID, workflowTask.StartedID)
	s.Equal(int32(2), workflowTask.Attempt)
}

func (s *timerQueueActiveTaskExecutorSuite) TestWorkflowTaskTimeout_Noop() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := newMutableStateBuilderWithVersionHistoriesForTest(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: timestamp.DurationPtr(2 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	di := addWorkflowTaskScheduledEvent(mutableState)
	startedEvent := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())

	protoTime := s.now
	s.NoError(err)
	timerTask := &persistencespb.TimerTaskInfo{
		ScheduleAttempt: 1,
		Version:         s.version,
		NamespaceId:     s.namespaceID,
		WorkflowId:      execution.GetWorkflowId(),
		RunId:           execution.GetRunId(),
		TaskId:          int64(100),
		TaskType:        enumsspb.TASK_TYPE_WORKFLOW_TASK_TIMEOUT,
		TimeoutType:     enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		VisibilityTime:  &protoTime,
		EventId:         di.ScheduleID - 1,
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
	taskQueueName := "some random task queue"

	mutableState := newMutableStateBuilderWithVersionHistoriesForTest(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	event, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: timestamp.DurationPtr(2 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	protoTaskTime := s.now
	s.NoError(err)
	timerTask := &persistencespb.TimerTaskInfo{
		ScheduleAttempt:     1,
		Version:             s.version,
		NamespaceId:         s.namespaceID,
		WorkflowId:          execution.GetWorkflowId(),
		RunId:               execution.GetRunId(),
		TaskId:              int64(100),
		TaskType:            enumsspb.TASK_TYPE_WORKFLOW_BACKOFF_TIMER,
		WorkflowBackoffType: enumsspb.WORKFLOW_BACKOFF_TYPE_RETRY,
		VisibilityTime:      &protoTaskTime,
		EventId:             0,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	err = s.timerQueueActiveTaskExecutor.execute(timerTask, true)
	s.NoError(err)

	workflowTask, ok := s.getMutableStateFromCache(s.namespaceID, execution.GetWorkflowId(), execution.GetRunId()).GetPendingWorkflowTask()
	s.True(ok)
	s.True(workflowTask.ScheduleID != common.EmptyEventID)
	s.Equal(common.EmptyEventID, workflowTask.StartedID)
	s.Equal(int32(1), workflowTask.Attempt)
}

func (s *timerQueueActiveTaskExecutorSuite) TestWorkflowBackoffTimer_Noop() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := newMutableStateBuilderWithVersionHistoriesForTest(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: timestamp.DurationPtr(2 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	protoTaskTime := s.now
	s.NoError(err)
	timerTask := &persistencespb.TimerTaskInfo{
		ScheduleAttempt:     1,
		Version:             s.version,
		NamespaceId:         s.namespaceID,
		WorkflowId:          execution.GetWorkflowId(),
		RunId:               execution.GetRunId(),
		TaskId:              int64(100),
		TaskType:            enumsspb.TASK_TYPE_WORKFLOW_BACKOFF_TIMER,
		WorkflowBackoffType: enumsspb.WORKFLOW_BACKOFF_TYPE_RETRY,
		VisibilityTime:      &protoTaskTime,
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
	taskQueueName := "some random task queue"

	mutableState := newMutableStateBuilderWithVersionHistoriesForTest(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType: &commonpb.WorkflowType{Name: workflowType},
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: taskQueueName,
					Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
				},
				WorkflowExecutionTimeout: timestamp.DurationPtr(2 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	taskqueue := "taskqueue"
	activityID := "activity"
	activityType := "activity type"
	timerTimeout := 2 * time.Second
	scheduledEvent, activityInfo := addActivityTaskScheduledEventWithRetry(
		mutableState,
		event.GetEventId(),
		activityID,
		activityType,
		taskqueue,
		nil,
		timerTimeout,
		timerTimeout,
		timerTimeout,
		timerTimeout,
		&commonpb.RetryPolicy{
			InitialInterval:        timestamp.DurationPtr(1 * time.Second),
			BackoffCoefficient:     1.2,
			MaximumInterval:        timestamp.DurationPtr(5 * time.Second),
			MaximumAttempts:        5,
			NonRetryableErrorTypes: []string{"（╯' - ')╯ ┻━┻ "},
		},
	)
	activityInfo.Attempt = 1

	protoTaskTime := s.now
	s.NoError(err)
	timerTask := &persistencespb.TimerTaskInfo{
		Version:         s.version,
		NamespaceId:     s.namespaceID,
		WorkflowId:      execution.GetWorkflowId(),
		RunId:           execution.GetRunId(),
		TaskId:          int64(100),
		TaskType:        enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER,
		TimeoutType:     enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		VisibilityTime:  &protoTaskTime,
		EventId:         activityInfo.ScheduleId,
		ScheduleAttempt: activityInfo.Attempt,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.EXPECT().AddActivityTask(
		gomock.Any(),
		&matchingservice.AddActivityTaskRequest{
			NamespaceId:       activityInfo.NamespaceId,
			SourceNamespaceId: activityInfo.NamespaceId,
			Execution:         &execution,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: activityInfo.TaskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			ScheduleId:             activityInfo.ScheduleId,
			ScheduleToStartTimeout: activityInfo.ScheduleToStartTimeout,
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
	taskQueueName := "some random task queue"

	mutableState := newMutableStateBuilderWithVersionHistoriesForTest(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: timestamp.DurationPtr(2 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	identity := "identity"
	taskqueue := "taskqueue"
	activityID := "activity"
	activityType := "activity type"
	timerTimeout := 2 * time.Second
	scheduledEvent, activityInfo := addActivityTaskScheduledEventWithRetry(
		mutableState,
		event.GetEventId(),
		activityID,
		activityType,
		taskqueue,
		nil,
		timerTimeout,
		timerTimeout,
		timerTimeout,
		timerTimeout,
		&commonpb.RetryPolicy{
			InitialInterval:        timestamp.DurationPtr(1 * time.Second),
			BackoffCoefficient:     1.2,
			MaximumInterval:        timestamp.DurationPtr(5 * time.Second),
			MaximumAttempts:        5,
			NonRetryableErrorTypes: []string{"（╯' - ')╯ ┻━┻ "},
		},
	)
	startedEvent := addActivityTaskStartedEvent(mutableState, scheduledEvent.GetEventId(), identity)
	s.Nil(startedEvent)

	protoTaskTime := s.now
	s.NoError(err)
	timerTask := &persistencespb.TimerTaskInfo{
		Version:         s.version,
		NamespaceId:     s.namespaceID,
		WorkflowId:      execution.GetWorkflowId(),
		RunId:           execution.GetRunId(),
		TaskId:          int64(100),
		TaskType:        enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER,
		TimeoutType:     enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		VisibilityTime:  &protoTaskTime,
		EventId:         activityInfo.ScheduleId,
		ScheduleAttempt: activityInfo.Attempt,
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
	taskQueueName := "some random task queue"

	mutableState := newMutableStateBuilderWithVersionHistoriesForTest(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: timestamp.DurationPtr(2 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
			},
			WorkflowExecutionExpirationTime: timestamp.TimePtr(s.now.Add(10 * time.Second)),
		},
	)
	s.Nil(err)

	di := addWorkflowTaskScheduledEvent(mutableState)
	startEvent := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = startEvent.GetEventId()
	completionEvent := addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	protoTaskTime := s.now
	s.NoError(err)
	timerTask := &persistencespb.TimerTaskInfo{
		ScheduleAttempt: 1,
		Version:         s.version,
		NamespaceId:     s.namespaceID,
		WorkflowId:      execution.GetWorkflowId(),
		RunId:           execution.GetRunId(),
		TaskId:          int64(100),
		TaskType:        enumsspb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT,
		TimeoutType:     enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		VisibilityTime:  &protoTaskTime,
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
	taskQueueName := "some random task queue"

	mutableState := newMutableStateBuilderWithVersionHistoriesForTest(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: timestamp.DurationPtr(2 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
			},
			WorkflowExecutionExpirationTime: timestamp.TimePtr(s.now.Add(10 * time.Second)),
		},
	)
	s.Nil(err)
	// need to override the workflow retry policy
	executionInfo := mutableState.executionInfo
	executionInfo.HasRetryPolicy = true
	executionInfo.RetryExpirationTime = timestamp.TimeNowPtrUtcAddSeconds(1000)
	executionInfo.RetryMaximumAttempts = 10
	executionInfo.RetryInitialInterval = timestamp.DurationFromSeconds(1)
	executionInfo.RetryMaximumInterval = timestamp.DurationFromSeconds(1)
	executionInfo.RetryBackoffCoefficient = 1

	di := addWorkflowTaskScheduledEvent(mutableState)
	startEvent := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = startEvent.GetEventId()
	completionEvent := addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	protoTaskTime := s.now
	s.NoError(err)
	timerTask := &persistencespb.TimerTaskInfo{
		ScheduleAttempt: 1,
		Version:         s.version,
		NamespaceId:     s.namespaceID,
		WorkflowId:      execution.GetWorkflowId(),
		RunId:           execution.GetRunId(),
		TaskId:          int64(100),
		TaskType:        enumsspb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT,
		TimeoutType:     enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		VisibilityTime:  &protoTaskTime,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, completionEvent.GetEventId(), completionEvent.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	// one for current workflow, one for new
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Times(2)
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	err = s.timerQueueActiveTaskExecutor.execute(timerTask, true)
	s.NoError(err)

	state, status := s.getMutableStateFromCache(s.namespaceID, execution.GetWorkflowId(), execution.GetRunId()).GetWorkflowStateStatus()
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, state)
	s.EqualValues(enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW, status)
}

func (s *timerQueueActiveTaskExecutorSuite) TestWorkflowTimeout_ContinueAsNew_Cron() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := newMutableStateBuilderWithVersionHistoriesForTest(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  timestamp.DurationPtr(2 * time.Second),
				WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
			},
			WorkflowExecutionExpirationTime: timestamp.TimePtr(s.now.Add(10 * time.Second)),
		},
	)
	s.Nil(err)
	executionInfo := mutableState.executionInfo
	executionInfo.StartTime = &s.now
	executionInfo.CronSchedule = "* * * * *"

	di := addWorkflowTaskScheduledEvent(mutableState)
	startEvent := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = startEvent.GetEventId()
	completionEvent := addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	protoTaskTime := s.now
	s.NoError(err)
	timerTask := &persistencespb.TimerTaskInfo{
		ScheduleAttempt: 1,
		Version:         s.version,
		NamespaceId:     s.namespaceID,
		WorkflowId:      execution.GetWorkflowId(),
		RunId:           execution.GetRunId(),
		TaskId:          int64(100),
		TaskType:        enumsspb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT,
		TimeoutType:     enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		VisibilityTime:  &protoTaskTime,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, completionEvent.GetEventId(), completionEvent.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	// one for current workflow, one for new
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Times(2)
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	err = s.timerQueueActiveTaskExecutor.execute(timerTask, true)
	s.NoError(err)

	state, status := s.getMutableStateFromCache(s.namespaceID, execution.GetWorkflowId(), execution.GetRunId()).GetWorkflowStateStatus()
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, state)
	s.EqualValues(enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW, status)
}

func (s *timerQueueActiveTaskExecutorSuite) TestWorkflowTimeout_WorkflowExpired() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := newMutableStateBuilderWithVersionHistoriesForTest(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  timestamp.DurationPtr(2 * time.Second),
				WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
			},
			WorkflowExecutionExpirationTime: timestamp.TimePtr(s.now.Add(-1 * time.Second)),
		},
	)
	s.Nil(err)
	executionInfo := mutableState.executionInfo
	executionInfo.StartTime = &s.now
	executionInfo.CronSchedule = "* * * * *"

	di := addWorkflowTaskScheduledEvent(mutableState)
	startEvent := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = startEvent.GetEventId()
	completionEvent := addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	protoTaskTime := s.now
	s.NoError(err)
	timerTask := &persistencespb.TimerTaskInfo{
		Version:        s.version,
		NamespaceId:    s.namespaceID,
		WorkflowId:     execution.GetWorkflowId(),
		RunId:          execution.GetRunId(),
		TaskId:         int64(100),
		TaskType:       enumsspb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT,
		TimeoutType:    enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		VisibilityTime: &protoTaskTime,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, completionEvent.GetEventId(), completionEvent.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Times(1)
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	err = s.timerQueueActiveTaskExecutor.execute(timerTask, true)
	s.NoError(err)

	state, status := s.getMutableStateFromCache(s.namespaceID, execution.GetWorkflowId(), execution.GetRunId()).GetWorkflowStateStatus()
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, state)
	s.EqualValues(enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, status)
}

func (s *timerQueueActiveTaskExecutorSuite) createPersistenceMutableState(
	ms mutableState,
	lastEventID int64,
	lastEventVersion int64,
) *persistencespb.WorkflowMutableState {

	if ms.GetExecutionInfo().GetVersionHistories() != nil {
		currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(ms.GetExecutionInfo().GetVersionHistories())
		s.NoError(err)
		err = versionhistory.AddOrUpdateItem(currentVersionHistory, versionhistory.NewItem(
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
