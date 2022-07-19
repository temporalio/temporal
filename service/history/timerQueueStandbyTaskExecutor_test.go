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

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
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
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/vclock"
	"go.temporal.io/server/service/history/workflow"
)

type (
	timerQueueStandbyTaskExecutorSuite struct {
		suite.Suite
		*require.Assertions

		controller             *gomock.Controller
		mockExecutionMgr       *persistence.MockExecutionManager
		mockShard              *shard.ContextTest
		mockTxProcessor        *queues.MockQueue
		mockTimerProcessor     *queues.MockQueue
		mockNamespaceCache     *namespace.MockRegistry
		mockClusterMetadata    *cluster.MockMetadata
		mockAdminClient        *adminservicemock.MockAdminServiceClient
		mockNDCHistoryResender *xdc.MockNDCHistoryResender
		mockDeleteManager      *workflow.MockDeleteManager
		mockMatchingClient     *matchingservicemock.MockMatchingServiceClient

		workflowCache        workflow.Cache
		logger               log.Logger
		namespaceID          namespace.ID
		namespaceEntry       *namespace.Namespace
		version              int64
		clusterName          string
		now                  time.Time
		timeSource           *clock.EventTimeSource
		fetchHistoryDuration time.Duration
		discardDuration      time.Duration

		timerQueueStandbyTaskExecutor *timerQueueStandbyTaskExecutor
	}
)

func TestTimerQueueStandbyTaskExecutorSuite(t *testing.T) {
	s := new(timerQueueStandbyTaskExecutorSuite)
	suite.Run(t, s)
}

func (s *timerQueueStandbyTaskExecutorSuite) SetupSuite() {
}

func (s *timerQueueStandbyTaskExecutorSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	config := tests.NewDynamicConfig()
	s.namespaceEntry = tests.GlobalStandbyNamespaceEntry
	s.namespaceID = s.namespaceEntry.ID()
	s.version = s.namespaceEntry.FailoverVersion()
	s.clusterName = cluster.TestAlternativeClusterName
	s.now = time.Now().UTC()
	s.timeSource = clock.NewEventTimeSource().Update(s.now)
	s.fetchHistoryDuration = config.StandbyTaskMissingEventsResendDelay() +
		(config.StandbyTaskMissingEventsDiscardDelay()-config.StandbyTaskMissingEventsResendDelay())/2
	s.discardDuration = config.StandbyTaskMissingEventsDiscardDelay() * 2

	s.controller = gomock.NewController(s.T())
	s.mockNDCHistoryResender = xdc.NewMockNDCHistoryResender(s.controller)
	s.mockTxProcessor = queues.NewMockQueue(s.controller)
	s.mockTimerProcessor = queues.NewMockQueue(s.controller)
	s.mockTxProcessor.EXPECT().Category().Return(tasks.CategoryTransfer).AnyTimes()
	s.mockTimerProcessor.EXPECT().Category().Return(tasks.CategoryTimer).AnyTimes()
	s.mockTxProcessor.EXPECT().NotifyNewTasks(gomock.Any(), gomock.Any()).AnyTimes()
	s.mockTimerProcessor.EXPECT().NotifyNewTasks(gomock.Any(), gomock.Any()).AnyTimes()

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

	// ack manager will use the namespace information
	s.mockNamespaceCache = s.mockShard.Resource.NamespaceCache
	s.mockExecutionMgr = s.mockShard.Resource.ExecutionMgr
	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata
	s.mockAdminClient = s.mockShard.Resource.RemoteAdminClient
	s.mockMatchingClient = s.mockShard.Resource.MatchingClient
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(s.namespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceName(gomock.Any()).Return(s.namespaceEntry.Name(), nil).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetClusterID().Return(cluster.TestCurrentClusterInitialFailoverVersion).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.IsGlobalNamespace(), s.version).Return(s.clusterName).AnyTimes()
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
		eventNotifier:      events.NewNotifier(s.timeSource, metrics.NoopClient, func(namespace.ID, string) int32 { return 1 }),
		queueProcessors: map[tasks.Category]queues.Queue{
			s.mockTxProcessor.Category():    s.mockTxProcessor,
			s.mockTimerProcessor.Category(): s.mockTimerProcessor,
		},
	}
	s.mockShard.SetEngineForTesting(h)

	s.timerQueueStandbyTaskExecutor = newTimerQueueStandbyTaskExecutor(
		s.mockShard,
		s.workflowCache,
		s.mockDeleteManager,
		s.mockNDCHistoryResender,
		s.mockMatchingClient,
		s.logger,
		metrics.NoopMetricsHandler,
		s.clusterName,
		config,
	).(*timerQueueStandbyTaskExecutor)
}

func (s *timerQueueStandbyTaskExecutorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessUserTimerTimeout_Pending() {
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
	nextEventID := event.GetEventId()

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

	s.mockShard.SetCurrentTime(s.clusterName, s.now)
	_, err = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, s.now.Add(s.fetchHistoryDuration))
	s.mockAdminClient.EXPECT().RefreshWorkflowTasks(gomock.Any(), &adminservice.RefreshWorkflowTasksRequest{
		Namespace:   s.namespaceEntry.Name().String(),
		NamespaceId: s.namespaceEntry.ID().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: timerTask.WorkflowID,
			RunId:      timerTask.RunID,
		},
	}).Return(&adminservice.RefreshWorkflowTasksResponse{}, nil)
	s.mockNDCHistoryResender.EXPECT().SendSingleWorkflowHistory(
		s.clusterName,
		namespace.ID(timerTask.NamespaceID),
		timerTask.WorkflowID,
		timerTask.RunID,
		nextEventID,
		s.version,
		int64(0),
		int64(0),
	).Return(nil)
	_, err = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, s.now.Add(s.discardDuration))
	_, err = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskDiscarded, err)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessUserTimerTimeout_Success() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard,
		s.mockShard.GetEventsCache(),
		s.logger,
		s.version,
		execution.GetRunId())
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

	s.mockShard.SetCurrentTime(s.clusterName, s.now)
	_, err = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Nil(err)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessUserTimerTimeout_Multiple() {
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

	timerID1 := "timer-1"
	timerTimeout1 := 2 * time.Second
	event, _ = addTimerStartedEvent(mutableState, event.GetEventId(), timerID1, timerTimeout1)

	timerID2 := "timer-2"
	timerTimeout2 := 50 * time.Second
	_, _ = addTimerStartedEvent(mutableState, event.GetEventId(), timerID2, timerTimeout2)

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

	event = addTimerFiredEvent(mutableState, timerID1)
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, s.now)
	_, err = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Nil(err)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessActivityTimeout_Pending() {
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
	scheduledEvent, _ := addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID, activityType, taskqueue, nil,
		timerTimeout, timerTimeout, timerTimeout, timerTimeout)
	nextEventID := scheduledEvent.GetEventId()

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
		EventID:             event.EventId,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, s.now)
	_, err = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, s.now.Add(s.fetchHistoryDuration))
	s.mockAdminClient.EXPECT().RefreshWorkflowTasks(gomock.Any(), &adminservice.RefreshWorkflowTasksRequest{
		Namespace:   s.namespaceEntry.Name().String(),
		NamespaceId: s.namespaceEntry.ID().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: timerTask.WorkflowID,
			RunId:      timerTask.RunID,
		},
	}).Return(&adminservice.RefreshWorkflowTasksResponse{}, nil)
	s.mockNDCHistoryResender.EXPECT().SendSingleWorkflowHistory(
		s.clusterName,
		namespace.ID(timerTask.NamespaceID),
		timerTask.WorkflowID,
		timerTask.RunID,
		nextEventID,
		s.version,
		int64(0),
		int64(0),
	).Return(nil)
	_, err = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, s.now.Add(s.discardDuration))
	_, err = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskDiscarded, err)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessActivityTimeout_Success() {
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
	scheduledEvent, _ := addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID, activityType, taskqueue, nil,
		timerTimeout, timerTimeout, timerTimeout, timerTimeout)
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
		EventID:             event.GetEventId(),
	}

	completeEvent := addActivityTaskCompletedEvent(mutableState, scheduledEvent.GetEventId(), startedEvent.GetEventId(), nil, identity)
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, completeEvent.GetEventId(), completeEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, s.now)
	_, err = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Nil(err)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessActivityTimeout_Heartbeat_Noop() {
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
	scheduledEvent, _ := addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID, activityType, taskqueue, nil,
		timerTimeout, timerTimeout, timerTimeout, heartbeatTimerTimeout)
	startedEvent := addActivityTaskStartedEvent(mutableState, scheduledEvent.GetEventId(), identity)
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

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
		VisibilityTimestamp: time.Unix(946684800, 0).Add(-100 * time.Second), // see pendingActivityTimerHeartbeats from mutable state
		EventID:             scheduledEvent.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, startedEvent.GetEventId(), startedEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, s.now)
	_, err = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Nil(err)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessActivityTimeout_Multiple_CanUpdate() {
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
	activityID1 := "activity 1"
	activityType1 := "activity type 1"
	timerTimeout1 := 2 * time.Second
	scheduledEvent1, _ := addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID1, activityType1, taskqueue, nil,
		timerTimeout1, timerTimeout1, timerTimeout1, timerTimeout1)
	startedEvent1 := addActivityTaskStartedEvent(mutableState, scheduledEvent1.GetEventId(), identity)

	activityID2 := "activity 2"
	activityType2 := "activity type 2"
	timerTimeout2 := 20 * time.Second
	scheduledEvent2, _ := addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID2, activityType2, taskqueue, nil,
		timerTimeout2, timerTimeout2, timerTimeout2, 10*time.Second)
	addActivityTaskStartedEvent(mutableState, scheduledEvent2.GetEventId(), identity)
	activityInfo2 := mutableState.GetPendingActivityInfos()[scheduledEvent2.GetEventId()]
	activityInfo2.TimerTaskStatus |= workflow.TimerTaskStatusCreatedHeartbeat
	activityInfo2.LastHeartbeatUpdateTime = timestamp.TimePtr(time.Now().UTC())

	timerSequence := workflow.NewTimerSequence(mutableState)
	mutableState.InsertTasks[tasks.CategoryTimer] = nil
	modified, err := timerSequence.CreateNextActivityTimer()
	s.NoError(err)
	s.True(modified)

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
		VisibilityTimestamp: activityInfo2.LastHeartbeatUpdateTime.Add(-5 * time.Second),
		EventID:             scheduledEvent2.GetEventId(),
	}

	completeEvent1 := addActivityTaskCompletedEvent(mutableState, scheduledEvent1.GetEventId(), startedEvent1.GetEventId(), nil, identity)
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, completeEvent1.GetEventId(), completeEvent1.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, input *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
			s.Equal(1, len(input.UpdateWorkflowMutation.Tasks[tasks.CategoryTimer]))
			s.Equal(1, len(input.UpdateWorkflowMutation.UpsertActivityInfos))
			mutableState.GetExecutionInfo().LastUpdateTime = input.UpdateWorkflowMutation.ExecutionInfo.LastUpdateTime
			input.RangeID = 0
			input.UpdateWorkflowMutation.ExecutionInfo.LastEventTaskId = 0
			input.UpdateWorkflowMutation.ExecutionInfo.LastFirstEventTxnId = 0
			input.UpdateWorkflowMutation.ExecutionInfo.StateTransitionCount = 0
			mutableState.GetExecutionInfo().LastEventTaskId = 0
			mutableState.GetExecutionInfo().LastFirstEventTxnId = 0
			mutableState.GetExecutionInfo().StateTransitionCount = 0
			mutableState.GetExecutionInfo().WorkflowTaskOriginalScheduledTime = input.UpdateWorkflowMutation.ExecutionInfo.WorkflowTaskOriginalScheduledTime
			mutableState.GetExecutionInfo().ExecutionStats = &persistencespb.ExecutionStats{}

			s.Equal(&persistence.UpdateWorkflowExecutionRequest{
				ShardID: s.mockShard.GetShardID(),
				UpdateWorkflowMutation: persistence.WorkflowMutation{
					ExecutionInfo:             mutableState.GetExecutionInfo(),
					ExecutionState:            mutableState.GetExecutionState(),
					NextEventID:               mutableState.GetNextEventID(),
					Tasks:                     input.UpdateWorkflowMutation.Tasks,
					Condition:                 mutableState.GetNextEventID(),
					UpsertActivityInfos:       input.UpdateWorkflowMutation.UpsertActivityInfos,
					DeleteActivityInfos:       map[int64]struct{}{},
					UpsertTimerInfos:          map[string]*persistencespb.TimerInfo{},
					DeleteTimerInfos:          map[string]struct{}{},
					UpsertChildExecutionInfos: map[int64]*persistencespb.ChildExecutionInfo{},
					DeleteChildExecutionInfos: map[int64]struct{}{},
					UpsertRequestCancelInfos:  map[int64]*persistencespb.RequestCancelInfo{},
					DeleteRequestCancelInfos:  map[int64]struct{}{},
					UpsertSignalInfos:         map[int64]*persistencespb.SignalInfo{},
					DeleteSignalInfos:         map[int64]struct{}{},
					UpsertSignalRequestedIDs:  map[string]struct{}{},
					DeleteSignalRequestedIDs:  map[string]struct{}{},
					NewBufferedEvents:         nil,
					ClearBufferedEvents:       false,
				},
				UpdateWorkflowEvents: []*persistence.WorkflowEvents{},
			}, input)
			return tests.UpdateWorkflowExecutionResponse, nil
		})

	s.mockShard.SetCurrentTime(s.clusterName, s.now)
	_, err = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Nil(err)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessWorkflowTaskTimeout_Pending() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_ = mutableState.UpdateCurrentVersion(s.version, false)
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
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()
	nextEventID := startedEvent.GetEventId()

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

	s.mockShard.SetCurrentTime(s.clusterName, s.now)
	_, err = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, s.now.Add(s.fetchHistoryDuration))
	s.mockAdminClient.EXPECT().RefreshWorkflowTasks(gomock.Any(), &adminservice.RefreshWorkflowTasksRequest{
		Namespace:   s.namespaceEntry.Name().String(),
		NamespaceId: s.namespaceEntry.ID().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: timerTask.WorkflowID,
			RunId:      timerTask.RunID,
		},
	}).Return(&adminservice.RefreshWorkflowTasksResponse{}, nil)
	s.mockNDCHistoryResender.EXPECT().SendSingleWorkflowHistory(
		s.clusterName,
		namespace.ID(timerTask.NamespaceID),
		timerTask.WorkflowID,
		timerTask.RunID,
		nextEventID,
		s.version,
		int64(0),
		int64(0),
	).Return(nil)
	_, err = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, s.now.Add(s.discardDuration))
	_, err = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskDiscarded, err)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessWorkflowTaskTimeout_ScheduleToStartTimer() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}

	workflowTaskScheduledEventID := int64(16384)

	timerTask := &tasks.WorkflowTaskTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		ScheduleAttempt:     1,
		Version:             s.version,
		TaskID:              int64(100),
		TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		VisibilityTimestamp: s.now,
		EventID:             workflowTaskScheduledEventID,
	}

	s.mockShard.SetCurrentTime(s.clusterName, s.now)
	_, err := s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(nil, err)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessWorkflowTaskTimeout_Success() {
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
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

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

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, s.now)
	_, err = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Nil(err)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessWorkflowBackoffTimer_Pending() {
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

	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()
	nextEventID := event.GetEventId()

	timerTask := &tasks.WorkflowBackoffTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		TaskID:              int64(100),
		VisibilityTimestamp: s.now,
		WorkflowBackoffType: enumsspb.WORKFLOW_BACKOFF_TYPE_RETRY,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, s.now)
	_, err = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, time.Now().UTC().Add(s.fetchHistoryDuration))
	s.mockAdminClient.EXPECT().RefreshWorkflowTasks(gomock.Any(), &adminservice.RefreshWorkflowTasksRequest{
		Namespace:   s.namespaceEntry.Name().String(),
		NamespaceId: s.namespaceEntry.ID().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: timerTask.WorkflowID,
			RunId:      timerTask.RunID,
		},
	}).Return(&adminservice.RefreshWorkflowTasksResponse{}, nil)
	s.mockNDCHistoryResender.EXPECT().SendSingleWorkflowHistory(
		s.clusterName,
		namespace.ID(timerTask.NamespaceID),
		timerTask.WorkflowID,
		timerTask.RunID,
		nextEventID,
		s.version,
		int64(0),
		int64(0),
	).Return(nil)
	_, err = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, time.Now().UTC().Add(s.discardDuration))
	_, err = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskDiscarded, err)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessWorkflowBackoffTimer_Success() {
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
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	timerTask := &tasks.WorkflowBackoffTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		TaskID:              int64(100),
		VisibilityTimestamp: s.now,
		WorkflowBackoffType: enumsspb.WORKFLOW_BACKOFF_TYPE_CRON,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, wt.ScheduledEventID, wt.Version)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, s.now)
	_, err = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Nil(err)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessWorkflowTimeout_Pending() {
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
	startEvent := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = startEvent.GetEventId()
	completionEvent := addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()
	nextEventID := completionEvent.GetEventId()

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

	s.mockShard.SetCurrentTime(s.clusterName, s.now)
	_, err = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, s.now.Add(s.fetchHistoryDuration))
	s.mockAdminClient.EXPECT().RefreshWorkflowTasks(gomock.Any(), &adminservice.RefreshWorkflowTasksRequest{
		Namespace:   s.namespaceEntry.Name().String(),
		NamespaceId: s.namespaceEntry.ID().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: timerTask.WorkflowID,
			RunId:      timerTask.RunID,
		},
	}).Return(&adminservice.RefreshWorkflowTasksResponse{}, nil)
	s.mockNDCHistoryResender.EXPECT().SendSingleWorkflowHistory(
		s.clusterName,
		namespace.ID(timerTask.NamespaceID),
		timerTask.WorkflowID,
		timerTask.RunID,
		nextEventID,
		s.version,
		int64(0),
		int64(0),
	).Return(nil)
	_, err = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, s.now.Add(s.discardDuration))
	_, err = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskDiscarded, err)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessWorkflowTimeout_Success() {
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
	event = addCompleteWorkflowEvent(mutableState, event.GetEventId(), nil)
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

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

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, s.now)
	_, err = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Nil(err)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessRetryTimeout() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	startEvent, err := mutableState.AddWorkflowExecutionStartedEvent(
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
	persistenceMutableState := s.createPersistenceMutableState(mutableState, startEvent.GetEventId(), startEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil).AnyTimes()
	timerTask := &tasks.ActivityRetryTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Attempt:             1,
		Version:             s.version,
		TaskID:              int64(100),
		VisibilityTimestamp: s.now,
		EventID:             int64(16384),
	}

	s.mockShard.SetCurrentTime(s.clusterName, s.now)
	_, err = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Nil(err)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessActivityRetryTimer_Noop() {
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
	scheduledEvent, _ := addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID, activityType, taskqueue, nil,
		timerTimeout, timerTimeout, timerTimeout, timerTimeout)
	startedEvent := addActivityTaskStartedEvent(mutableState, scheduledEvent.GetEventId(), identity)

	timerSequence := workflow.NewTimerSequence(mutableState)
	mutableState.InsertTasks[tasks.CategoryTimer] = nil
	modified, err := timerSequence.CreateNextActivityTimer()
	s.NoError(err)
	s.True(modified)
	task := mutableState.InsertTasks[tasks.CategoryTimer][0]

	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, startedEvent.GetEventId(), startedEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockShard.SetCurrentTime(s.clusterName, s.now)

	timerTask := &tasks.ActivityRetryTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Attempt:             2,
		Version:             s.version,
		TaskID:              int64(100),
		VisibilityTimestamp: task.(*tasks.ActivityTimeoutTask).VisibilityTimestamp,
		EventID:             scheduledEvent.GetEventId(),
	}
	_, err = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Nil(err)

	timerTask = &tasks.ActivityRetryTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Attempt:             2,
		Version:             s.version - 1,
		TaskID:              int64(100),
		VisibilityTimestamp: task.(*tasks.ActivityTimeoutTask).VisibilityTimestamp,
		EventID:             scheduledEvent.GetEventId(),
	}
	_, err = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Nil(err)

	timerTask = &tasks.ActivityRetryTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Attempt:             0,
		Version:             s.version,
		TaskID:              int64(100),
		VisibilityTimestamp: task.(*tasks.ActivityTimeoutTask).VisibilityTimestamp,
		EventID:             scheduledEvent.GetEventId(),
	}
	_, err = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Nil(err)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessActivityRetryTimer_ActivityCompleted() {
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
	scheduledEvent, _ := addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID, activityType, taskqueue, nil,
		timerTimeout, timerTimeout, timerTimeout, timerTimeout)
	startedEvent := addActivityTaskStartedEvent(mutableState, scheduledEvent.GetEventId(), identity)

	timerSequence := workflow.NewTimerSequence(mutableState)
	mutableState.InsertTasks[tasks.CategoryTimer] = nil
	modified, err := timerSequence.CreateNextActivityTimer()
	s.NoError(err)
	s.True(modified)
	task := mutableState.InsertTasks[tasks.CategoryTimer][0]

	completeEvent := addActivityTaskCompletedEvent(mutableState, scheduledEvent.GetEventId(), startedEvent.GetEventId(), nil, identity)
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, completeEvent.GetEventId(), completeEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, s.now)
	timerTask := &tasks.ActivityRetryTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Attempt:             2,
		Version:             s.version,
		TaskID:              int64(100),
		VisibilityTimestamp: task.(*tasks.ActivityTimeoutTask).VisibilityTimestamp,
		EventID:             scheduledEvent.GetEventId(),
	}
	_, err = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Nil(err)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessActivityRetryTimer_Pending() {
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
	scheduledEvent, _ := addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID, activityType, taskqueue, nil,
		timerTimeout, timerTimeout, timerTimeout, timerTimeout)

	timerSequence := workflow.NewTimerSequence(mutableState)
	mutableState.InsertTasks[tasks.CategoryTimer] = nil
	modified, err := timerSequence.CreateNextActivityTimer()
	s.NoError(err)
	s.True(modified)
	task := mutableState.InsertTasks[tasks.CategoryTimer][0]

	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	timerTask := &tasks.ActivityRetryTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Attempt:             2,
		Version:             s.version,
		TaskID:              int64(100),
		VisibilityTimestamp: task.(*tasks.ActivityTimeoutTask).VisibilityTimestamp,
		EventID:             scheduledEvent.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	// no-op post action
	s.mockShard.SetCurrentTime(s.clusterName, s.now)
	_, err = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskRetry, err)

	// resend history post action
	s.mockShard.SetCurrentTime(s.clusterName, s.now.Add(s.fetchHistoryDuration))
	s.mockAdminClient.EXPECT().RefreshWorkflowTasks(gomock.Any(), &adminservice.RefreshWorkflowTasksRequest{
		NamespaceId: s.namespaceEntry.ID().String(),
		Namespace:   s.namespaceEntry.Name().String(),
		Execution:   &execution,
	}).Return(&adminservice.RefreshWorkflowTasksResponse{}, nil)
	s.mockNDCHistoryResender.EXPECT().SendSingleWorkflowHistory(
		s.clusterName,
		s.namespaceID,
		execution.WorkflowId,
		execution.RunId,
		scheduledEvent.GetEventId(),
		s.version,
		int64(0),
		int64(0),
	).Return(nil)
	_, err = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskRetry, err)

	// push to matching post action
	s.mockShard.SetCurrentTime(s.clusterName, s.now.Add(s.discardDuration))
	s.mockMatchingClient.EXPECT().AddActivityTask(
		gomock.Any(),
		&matchingservice.AddActivityTaskRequest{
			NamespaceId:       s.namespaceID.String(),
			SourceNamespaceId: s.namespaceID.String(),
			Execution:         &execution,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskqueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			ScheduledEventId:       scheduledEvent.EventId,
			ScheduleToStartTimeout: &timerTimeout,
			Clock:                  vclock.NewVectorClock(s.mockClusterMetadata.GetClusterID(), s.mockShard.GetShardID(), timerTask.TaskID),
		},
		gomock.Any(),
	).Return(&matchingservice.AddActivityTaskResponse{}, nil)

	_, err = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Nil(err)
}

func (s *timerQueueStandbyTaskExecutorSuite) createPersistenceMutableState(
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

func (s *timerQueueStandbyTaskExecutorSuite) newTaskExecutable(
	task tasks.Task,
) queues.Executable {
	return queues.NewExecutable(task, nil, s.timerQueueStandbyTaskExecutor, nil, nil, s.mockShard.GetTimeSource(), nil, nil, queues.QueueTypeStandbyTimer, nil)
}
