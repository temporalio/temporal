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
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

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
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/vclock"
	"go.temporal.io/server/service/history/workflow"
)

type (
	timerQueueActiveTaskExecutorSuite struct {
		suite.Suite
		*require.Assertions

		controller              *gomock.Controller
		mockShard               *shard.ContextTest
		mockTxProcessor         *queues.MockQueue
		mockTimerProcessor      *queues.MockQueue
		mockVisibilityProcessor *queues.MockQueue
		mockNamespaceCache      *namespace.MockRegistry
		mockMatchingClient      *matchingservicemock.MockMatchingServiceClient
		mockClusterMetadata     *cluster.MockMetadata

		mockHistoryEngine *historyEngineImpl
		mockDeleteManager *workflow.MockDeleteManager
		mockExecutionMgr  *persistence.MockExecutionManager

		workflowCache                workflow.Cache
		logger                       log.Logger
		namespaceID                  namespace.ID
		namespaceEntry               *namespace.Namespace
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

	s.namespaceID = tests.NamespaceID
	s.namespaceEntry = tests.GlobalNamespaceEntry
	s.version = s.namespaceEntry.FailoverVersion()
	s.now = time.Now().UTC()
	s.timeSource = clock.NewEventTimeSource().Update(s.now)

	s.controller = gomock.NewController(s.T())
	s.mockTxProcessor = queues.NewMockQueue(s.controller)
	s.mockTimerProcessor = queues.NewMockQueue(s.controller)
	s.mockVisibilityProcessor = queues.NewMockQueue(s.controller)
	s.mockTxProcessor.EXPECT().Category().Return(tasks.CategoryTransfer).AnyTimes()
	s.mockTimerProcessor.EXPECT().Category().Return(tasks.CategoryTimer).AnyTimes()
	s.mockVisibilityProcessor.EXPECT().Category().Return(tasks.CategoryVisibility).AnyTimes()
	s.mockTxProcessor.EXPECT().NotifyNewTasks(gomock.Any(), gomock.Any()).AnyTimes()
	s.mockTimerProcessor.EXPECT().NotifyNewTasks(gomock.Any(), gomock.Any()).AnyTimes()
	s.mockVisibilityProcessor.EXPECT().NotifyNewTasks(gomock.Any(), gomock.Any()).AnyTimes()

	config := tests.NewDynamicConfig()
	s.mockShard = shard.NewTestContextWithTimeSource(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId: 1,
				RangeId: 1,
			},
		},
		config,
		s.timeSource,
	)
	s.mockShard.SetEventsCacheForTesting(events.NewEventsCache(
		s.mockShard.GetShardID(),
		s.mockShard.GetConfig().EventsCacheInitialSize(),
		s.mockShard.GetConfig().EventsCacheMaxSize(),
		s.mockShard.GetConfig().EventsCacheTTL(),
		s.mockShard.GetExecutionManager(),
		false,
		s.mockShard.GetLogger(),
		s.mockShard.GetMetricsClient(),
	))

	s.mockNamespaceCache = s.mockShard.Resource.NamespaceCache
	s.mockMatchingClient = s.mockShard.Resource.MatchingClient
	s.mockExecutionMgr = s.mockShard.Resource.ExecutionMgr
	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata
	// ack manager will use the namespace information
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceName(gomock.Any()).Return(tests.Namespace, nil).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetClusterID().Return(cluster.TestCurrentClusterInitialFailoverVersion).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.IsGlobalNamespace(), s.version).Return(s.mockClusterMetadata.GetCurrentClusterName()).AnyTimes()
	s.workflowCache = workflow.NewCache(s.mockShard)
	s.logger = s.mockShard.GetLogger()

	s.mockDeleteManager = workflow.NewMockDeleteManager(s.controller)
	h := &historyEngineImpl{
		currentClusterName: s.mockShard.Resource.GetClusterMetadata().GetCurrentClusterName(),
		shard:              s.mockShard,
		clusterMetadata:    s.mockClusterMetadata,
		executionManager:   s.mockExecutionMgr,
		logger:             s.logger,
		tokenSerializer:    common.NewProtoTaskTokenSerializer(),
		metricsClient:      s.mockShard.GetMetricsClient(),
		eventNotifier:      events.NewNotifier(clock.NewRealTimeSource(), metrics.NoopClient, func(namespace.ID, string) int32 { return 1 }),
		queueProcessors: map[tasks.Category]queues.Queue{
			s.mockTxProcessor.Category():         s.mockTxProcessor,
			s.mockTimerProcessor.Category():      s.mockTimerProcessor,
			s.mockVisibilityProcessor.Category(): s.mockVisibilityProcessor,
		},
	}
	s.mockShard.SetEngineForTesting(h)
	s.mockHistoryEngine = h

	s.timerQueueActiveTaskExecutor = newTimerQueueActiveTaskExecutor(
		s.mockShard,
		s.workflowCache,
		s.mockDeleteManager,
		newTimerQueueActiveProcessor(
			s.mockShard,
			s.workflowCache,
			nil,
			s.mockDeleteManager,
			s.mockMatchingClient,
			newTaskAllocator(s.mockShard),
			s.mockShard.Resource.ClientBean,
			quotas.NewDefaultOutgoingRateLimiter(
				func() float64 { return float64(config.TimerProcessorMaxPollRPS()) },
			),
			s.logger,
			metrics.NoopMetricsHandler,
			false,
		),
		s.logger,
		metrics.NoopMetricsHandler,
		config,
		s.mockShard.Resource.GetMatchingClient(),
	).(*timerQueueActiveTaskExecutor)
}

func (s *timerQueueActiveTaskExecutorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *timerQueueActiveTaskExecutorSuite) TestProcessUserTimerTimeout_Fire() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(
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
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  timestamp.DurationPtr(200 * time.Second),
				WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	timerID := "timer"
	timerTimeout := 2 * time.Second
	event, _ = addTimerStartedEvent(mutableState, event.GetEventId(), timerID, timerTimeout)

	timerSequence := workflow.NewTimerSequence(mutableState)
	mutableState.InsertTasks[tasks.CategoryTimer] = nil
	modified, err := timerSequence.CreateNextUserTimer()
	s.NoError(err)
	s.True(modified)
	task := mutableState.InsertTasks[tasks.CategoryTimer][0]

	timerTask := &tasks.UserTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		TaskID:              int64(100),
		VisibilityTimestamp: task.(*tasks.UserTimerTask).VisibilityTimestamp,
		EventID:             event.EventId,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	s.timeSource.Update(s.now.Add(2 * timerTimeout))
	_, err = s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
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

	mutableState := workflow.TestGlobalMutableState(
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
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  timestamp.DurationPtr(200 * time.Second),
				WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	timerID := "timer"
	timerTimeout := 2 * time.Second
	event, _ = addTimerStartedEvent(mutableState, event.GetEventId(), timerID, timerTimeout)

	timerSequence := workflow.NewTimerSequence(mutableState)
	mutableState.InsertTasks[tasks.CategoryTimer] = nil
	modified, err := timerSequence.CreateNextUserTimer()
	s.NoError(err)
	s.True(modified)
	task := mutableState.InsertTasks[tasks.CategoryTimer][0]

	timerTask := &tasks.UserTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		TaskID:              int64(100),
		VisibilityTimestamp: task.(*tasks.UserTimerTask).VisibilityTimestamp,
		EventID:             event.EventId,
	}

	event = addTimerFiredEvent(mutableState, timerID)
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.timeSource.Update(s.now.Add(2 * timerTimeout))
	_, err = s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(err)
}

func (s *timerQueueActiveTaskExecutorSuite) TestProcessActivityTimeout_NoRetryPolicy_Fire() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: timestamp.DurationPtr(200 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

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

	timerSequence := workflow.NewTimerSequence(mutableState)
	mutableState.InsertTasks[tasks.CategoryTimer] = nil
	modified, err := timerSequence.CreateNextActivityTimer()
	s.NoError(err)
	s.True(modified)
	task := mutableState.InsertTasks[tasks.CategoryTimer][0]

	timerTask := &tasks.ActivityTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Attempt:             1,
		Version:             s.version,
		TaskID:              int64(100),
		TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		VisibilityTimestamp: task.(*tasks.ActivityTimeoutTask).VisibilityTimestamp,
		EventID:             wt.ScheduledEventID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	s.timeSource.Update(s.now.Add(2 * timerTimeout))
	_, err = s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
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

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: timestamp.DurationPtr(200 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

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

	timerSequence := workflow.NewTimerSequence(mutableState)
	mutableState.InsertTasks[tasks.CategoryTimer] = nil
	modified, err := timerSequence.CreateNextActivityTimer()
	s.NoError(err)
	s.True(modified)
	task := mutableState.InsertTasks[tasks.CategoryTimer][0]

	timerTask := &tasks.ActivityTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Attempt:             1,
		Version:             s.version,
		TaskID:              int64(100),
		TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		VisibilityTimestamp: task.(*tasks.ActivityTimeoutTask).VisibilityTimestamp,
		EventID:             wt.ScheduledEventID,
	}

	completeEvent := addActivityTaskCompletedEvent(mutableState, scheduledEvent.GetEventId(), startedEvent.GetEventId(), nil, identity)
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, completeEvent.GetEventId(), completeEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.timeSource.Update(s.now.Add(2 * timerTimeout))
	_, err = s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(err)
}

func (s *timerQueueActiveTaskExecutorSuite) TestProcessActivityTimeout_RetryPolicy_Retry() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  timestamp.DurationPtr(200 * time.Second),
				WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

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

	timerSequence := workflow.NewTimerSequence(mutableState)
	mutableState.InsertTasks[tasks.CategoryTimer] = nil
	modified, err := timerSequence.CreateNextActivityTimer()
	s.NoError(err)
	s.True(modified)
	task := mutableState.InsertTasks[tasks.CategoryTimer][0]

	timerTask := &tasks.ActivityTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Attempt:             1,
		Version:             s.version,
		TaskID:              int64(100),
		TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		VisibilityTimestamp: task.(*tasks.ActivityTimeoutTask).VisibilityTimestamp,
		EventID:             wt.ScheduledEventID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	s.timeSource.Update(s.now.Add(2 * timerTimeout))
	_, err = s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(err)

	activityInfo, ok := s.getMutableStateFromCache(s.namespaceID, execution.GetWorkflowId(), execution.GetRunId()).GetActivityInfo(scheduledEvent.GetEventId())
	s.True(ok)
	s.Equal(scheduledEvent.GetEventId(), activityInfo.ScheduledEventId)
	s.Equal(common.EmptyEventID, activityInfo.StartedEventId)
	// only a schedule to start timer will be created, apart from the retry timer
	s.Equal(int32(workflow.TimerTaskStatusCreatedScheduleToStart), activityInfo.TimerTaskStatus)
}

func (s *timerQueueActiveTaskExecutorSuite) TestProcessActivityTimeout_RetryPolicy_Fire() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  timestamp.DurationPtr(200 * time.Second),
				WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

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

	timerSequence := workflow.NewTimerSequence(mutableState)
	mutableState.InsertTasks[tasks.CategoryTimer] = nil
	modified, err := timerSequence.CreateNextActivityTimer()
	s.NoError(err)
	s.True(modified)
	task := mutableState.InsertTasks[tasks.CategoryTimer][0]

	timerTask := &tasks.ActivityTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Attempt:             1,
		Version:             s.version,
		TaskID:              int64(100),
		TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		VisibilityTimestamp: task.(*tasks.ActivityTimeoutTask).VisibilityTimestamp,
		EventID:             wt.ScheduledEventID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	s.timeSource.Update(s.now.Add(2 * timerTimeout))
	_, err = s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
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

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  timestamp.DurationPtr(200 * time.Second),
				WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

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

	timerSequence := workflow.NewTimerSequence(mutableState)
	mutableState.InsertTasks[tasks.CategoryTimer] = nil
	modified, err := timerSequence.CreateNextActivityTimer()
	s.NoError(err)
	s.True(modified)
	task := mutableState.InsertTasks[tasks.CategoryTimer][0]

	timerTask := &tasks.ActivityTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Attempt:             1,
		Version:             s.version,
		TaskID:              int64(100),
		TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		VisibilityTimestamp: task.(*tasks.ActivityTimeoutTask).VisibilityTimestamp,
		EventID:             wt.ScheduledEventID,
	}

	completeEvent := addActivityTaskCompletedEvent(mutableState, scheduledEvent.GetEventId(), common.TransientEventID, nil, identity)
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, completeEvent.GetEventId(), completeEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.timeSource.Update(s.now.Add(2 * timerTimeout))
	_, err = s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(err)
}

func (s *timerQueueActiveTaskExecutorSuite) TestProcessActivityTimeout_Heartbeat_Noop() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  timestamp.DurationPtr(200 * time.Second),
				WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

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

	timerSequence := workflow.NewTimerSequence(mutableState)
	mutableState.InsertTasks[tasks.CategoryTimer] = nil
	modified, err := timerSequence.CreateNextActivityTimer()
	s.NoError(err)
	s.True(modified)
	task := mutableState.InsertTasks[tasks.CategoryTimer][0]
	s.Equal(enumspb.TIMEOUT_TYPE_HEARTBEAT, task.(*tasks.ActivityTimeoutTask).TimeoutType)

	timerTask := &tasks.ActivityTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Attempt:             1,
		Version:             s.version,
		TaskID:              int64(100),
		TimeoutType:         enumspb.TIMEOUT_TYPE_HEARTBEAT,
		VisibilityTimestamp: time.Time{},
		EventID:             scheduledEvent.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	_, err = s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(err)
}

func (s *timerQueueActiveTaskExecutorSuite) TestWorkflowTaskTimeout_Fire() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  timestamp.DurationPtr(200 * time.Second),
				WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	startedEvent := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())

	timerTask := &tasks.WorkflowTaskTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		ScheduleAttempt:     1,
		Version:             s.version,
		TaskID:              int64(100),
		TimeoutType:         enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		VisibilityTimestamp: s.now,
		EventID:             wt.ScheduledEventID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, startedEvent.GetEventId(), startedEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err = s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(err)

	workflowTask, ok := s.getMutableStateFromCache(s.namespaceID, execution.GetWorkflowId(), execution.GetRunId()).GetPendingWorkflowTask()
	s.True(ok)
	s.True(workflowTask.ScheduledEventID != common.EmptyEventID)
	s.Equal(common.EmptyEventID, workflowTask.StartedEventID)
	s.Equal(int32(2), workflowTask.Attempt)
}

func (s *timerQueueActiveTaskExecutorSuite) TestWorkflowTaskTimeout_Noop() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  timestamp.DurationPtr(200 * time.Second),
				WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	startedEvent := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())

	timerTask := &tasks.WorkflowTaskTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		ScheduleAttempt:     1,
		Version:             s.version,
		TaskID:              int64(100),
		TimeoutType:         enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		VisibilityTimestamp: s.now,
		EventID:             wt.ScheduledEventID - 1,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, startedEvent.GetEventId(), startedEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	_, err = s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(err)
}

func (s *timerQueueActiveTaskExecutorSuite) TestWorkflowBackoffTimer_Fire() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	event, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  timestamp.DurationPtr(200 * time.Second),
				WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	timerTask := &tasks.WorkflowBackoffTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		TaskID:              int64(100),
		WorkflowBackoffType: enumsspb.WORKFLOW_BACKOFF_TYPE_RETRY,
		VisibilityTimestamp: s.now,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err = s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(err)

	workflowTask, ok := s.getMutableStateFromCache(s.namespaceID, execution.GetWorkflowId(), execution.GetRunId()).GetPendingWorkflowTask()
	s.True(ok)
	s.True(workflowTask.ScheduledEventID != common.EmptyEventID)
	s.Equal(common.EmptyEventID, workflowTask.StartedEventID)
	s.Equal(int32(1), workflowTask.Attempt)
}

func (s *timerQueueActiveTaskExecutorSuite) TestWorkflowBackoffTimer_Noop() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  timestamp.DurationPtr(200 * time.Second),
				WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	timerTask := &tasks.WorkflowBackoffTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		TaskID:              int64(100),
		WorkflowBackoffType: enumsspb.WORKFLOW_BACKOFF_TYPE_RETRY,
		VisibilityTimestamp: s.now,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	_, err = s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(err)
}

func (s *timerQueueActiveTaskExecutorSuite) TestActivityRetryTimer_Fire() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType: &commonpb.WorkflowType{Name: workflowType},
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: taskQueueName,
					Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
				},
				WorkflowRunTimeout:  timestamp.DurationPtr(200 * time.Second),
				WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

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

	timerTask := &tasks.ActivityRetryTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		TaskID:              int64(100),
		VisibilityTimestamp: s.now,
		EventID:             activityInfo.ScheduledEventId,
		Attempt:             activityInfo.Attempt,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
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
			ScheduledEventId:       activityInfo.ScheduledEventId,
			ScheduleToStartTimeout: activityInfo.ScheduleToStartTimeout,
			Clock:                  vclock.NewVectorClock(s.mockClusterMetadata.GetClusterID(), s.mockShard.GetShardID(), timerTask.TaskID),
		},
		gomock.Any(),
	).Return(&matchingservice.AddActivityTaskResponse{}, nil)

	_, err = s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(err)
}

func (s *timerQueueActiveTaskExecutorSuite) TestActivityRetryTimer_Noop() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  timestamp.DurationPtr(200 * time.Second),
				WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

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

	timerTask := &tasks.ActivityRetryTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		TaskID:              int64(100),
		VisibilityTimestamp: s.now,
		EventID:             activityInfo.ScheduledEventId,
		Attempt:             activityInfo.Attempt,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	_, err = s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(err)
}

func (s *timerQueueActiveTaskExecutorSuite) TestWorkflowTimeout_Fire() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  timestamp.DurationPtr(200 * time.Second),
				WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
			},
			WorkflowExecutionExpirationTime: timestamp.TimePtr(s.now.Add(10 * time.Second)),
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	startEvent := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = startEvent.GetEventId()
	completionEvent := addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	timerTask := &tasks.WorkflowTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		TaskID:              int64(100),
		VisibilityTimestamp: s.now,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, completionEvent.GetEventId(), completionEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err = s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(err)

	running := s.getMutableStateFromCache(s.namespaceID, execution.GetWorkflowId(), execution.GetRunId()).IsWorkflowExecutionRunning()
	s.False(running)
}

func (s *timerQueueActiveTaskExecutorSuite) TestWorkflowTimeout_Retry() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  timestamp.DurationPtr(200 * time.Second),
				WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
			},
			WorkflowExecutionExpirationTime: timestamp.TimePtr(s.now.Add(10 * time.Second)),
		},
	)
	s.Nil(err)
	// need to override the workflow retry policy
	executionInfo := mutableState.GetExecutionInfo()
	executionInfo.HasRetryPolicy = true
	executionInfo.WorkflowExecutionExpirationTime = timestamp.TimeNowPtrUtcAddSeconds(1000)
	executionInfo.RetryMaximumAttempts = 10
	executionInfo.RetryInitialInterval = timestamp.DurationFromSeconds(1)
	executionInfo.RetryMaximumInterval = timestamp.DurationFromSeconds(1)
	executionInfo.RetryBackoffCoefficient = 1

	wt := addWorkflowTaskScheduledEvent(mutableState)
	startEvent := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = startEvent.GetEventId()
	completionEvent := addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	timerTask := &tasks.WorkflowTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		TaskID:              int64(100),
		VisibilityTimestamp: s.now,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, completionEvent.GetEventId(), completionEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	// one for current workflow, one for new
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err = s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(err)

	state, status := s.getMutableStateFromCache(s.namespaceID, execution.GetWorkflowId(), execution.GetRunId()).GetWorkflowStateStatus()
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, state)
	s.EqualValues(enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, status)
}

func (s *timerQueueActiveTaskExecutorSuite) TestWorkflowTimeout_Cron() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  timestamp.DurationPtr(200 * time.Second),
				WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
			},
			WorkflowExecutionExpirationTime: timestamp.TimePtr(s.now.Add(10 * time.Second)),
		},
	)
	s.Nil(err)
	executionInfo := mutableState.GetExecutionInfo()
	executionInfo.StartTime = &s.now
	executionInfo.CronSchedule = "* * * * *"

	wt := addWorkflowTaskScheduledEvent(mutableState)
	startEvent := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = startEvent.GetEventId()
	completionEvent := addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	timerTask := &tasks.WorkflowTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		TaskID:              int64(100),
		VisibilityTimestamp: s.now,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, completionEvent.GetEventId(), completionEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	// one for current workflow, one for new
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err = s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(err)

	state, status := s.getMutableStateFromCache(s.namespaceID, execution.GetWorkflowId(), execution.GetRunId()).GetWorkflowStateStatus()
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, state)
	s.EqualValues(enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, status)
}

func (s *timerQueueActiveTaskExecutorSuite) TestWorkflowTimeout_WorkflowExpired() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  timestamp.DurationPtr(200 * time.Second),
				WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
			},
			WorkflowExecutionExpirationTime: timestamp.TimePtr(s.now.Add(-1 * time.Second)),
		},
	)
	s.Nil(err)
	executionInfo := mutableState.GetExecutionInfo()
	executionInfo.StartTime = &s.now
	executionInfo.CronSchedule = "* * * * *"

	wt := addWorkflowTaskScheduledEvent(mutableState)
	startEvent := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = startEvent.GetEventId()
	completionEvent := addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	timerTask := &tasks.WorkflowTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		TaskID:              int64(100),
		VisibilityTimestamp: s.now,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, completionEvent.GetEventId(), completionEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err = s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(err)

	state, status := s.getMutableStateFromCache(s.namespaceID, execution.GetWorkflowId(), execution.GetRunId()).GetWorkflowStateStatus()
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, state)
	s.EqualValues(enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, status)
}

func (s *timerQueueActiveTaskExecutorSuite) createPersistenceMutableState(
	ms workflow.MutableState,
	lastEventID int64,
	lastEventVersion int64,
) *persistencespb.WorkflowMutableState {
	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(ms.GetExecutionInfo().GetVersionHistories())
	s.NoError(err)
	err = versionhistory.AddOrUpdateVersionHistoryItem(currentVersionHistory, versionhistory.NewVersionHistoryItem(
		lastEventID, lastEventVersion,
	))
	s.NoError(err)
	return workflow.TestCloneToProto(ms)
}

func (s *timerQueueActiveTaskExecutorSuite) getMutableStateFromCache(
	namespaceID namespace.ID,
	workflowID string,
	runID string,
) workflow.MutableState {
	return s.workflowCache.(*workflow.CacheImpl).Get(
		definition.NewWorkflowKey(namespaceID.String(), workflowID, runID),
	).(*workflow.ContextImpl).MutableState
}

func (s *timerQueueActiveTaskExecutorSuite) newTaskExecutable(
	task tasks.Task,
) queues.Executable {
	return queues.NewExecutable(task, nil, s.timerQueueActiveTaskExecutor, nil, nil, s.mockShard.GetTimeSource(), nil, nil, queues.QueueTypeActiveTimer, nil)
}
