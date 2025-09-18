package history

import (
	"context"
	"testing"
	"time"

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
	"go.temporal.io/server/chasm"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/telemetry"
	pm "go.temporal.io/server/common/testing/protomock"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/components/dummy"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/deletemanager"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/vclock"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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
		mockArchivalProcessor   *queues.MockQueue
		mockNamespaceCache      *namespace.MockRegistry
		mockMatchingClient      *matchingservicemock.MockMatchingServiceClient
		mockClusterMetadata     *cluster.MockMetadata
		mockChasmEngine         *chasm.MockEngine

		mockHistoryEngine *historyEngineImpl
		mockDeleteManager *deletemanager.MockDeleteManager
		mockExecutionMgr  *persistence.MockExecutionManager

		config                       *configs.Config
		workflowCache                wcache.Cache
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
	s.mockArchivalProcessor = queues.NewMockQueue(s.controller)
	s.mockChasmEngine = chasm.NewMockEngine(s.controller)
	s.mockTxProcessor.EXPECT().Category().Return(tasks.CategoryTransfer).AnyTimes()
	s.mockTimerProcessor.EXPECT().Category().Return(tasks.CategoryTimer).AnyTimes()
	s.mockVisibilityProcessor.EXPECT().Category().Return(tasks.CategoryVisibility).AnyTimes()
	s.mockArchivalProcessor.EXPECT().Category().Return(tasks.CategoryArchival).AnyTimes()
	s.mockTxProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	s.mockTimerProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	s.mockVisibilityProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	s.mockArchivalProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()

	s.config = tests.NewDynamicConfig()
	s.mockShard = shard.NewTestContextWithTimeSource(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 1,
			RangeId: 1,
		},
		s.config,
		s.timeSource,
	)

	reg := hsm.NewRegistry()
	err := workflow.RegisterStateMachine(reg)
	s.NoError(err)
	s.mockShard.SetStateMachineRegistry(reg)

	s.mockShard.SetEventsCacheForTesting(events.NewHostLevelEventsCache(
		s.mockShard.GetExecutionManager(),
		s.mockShard.GetConfig(),
		s.mockShard.GetMetricsHandler(),
		s.mockShard.GetLogger(),
		false,
	))

	s.mockNamespaceCache = s.mockShard.Resource.NamespaceCache
	s.mockMatchingClient = s.mockShard.Resource.MatchingClient
	s.mockExecutionMgr = s.mockShard.Resource.ExecutionMgr
	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata
	// ack manager will use the namespace information
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceName(gomock.Any()).Return(tests.Namespace, nil).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetClusterID().Return(tests.Version).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsVersionFromSameCluster(tests.Version, tests.Version).Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.IsGlobalNamespace(), s.version).Return(s.mockClusterMetadata.GetCurrentClusterName()).AnyTimes()
	s.workflowCache = wcache.NewHostLevelCache(s.mockShard.GetConfig(), s.mockShard.GetLogger(), metrics.NoopMetricsHandler)
	s.logger = s.mockShard.GetLogger()

	s.mockDeleteManager = deletemanager.NewMockDeleteManager(s.controller)
	h := &historyEngineImpl{
		currentClusterName: s.mockShard.Resource.GetClusterMetadata().GetCurrentClusterName(),
		shardContext:       s.mockShard,
		clusterMetadata:    s.mockClusterMetadata,
		executionManager:   s.mockExecutionMgr,
		logger:             s.logger,
		tokenSerializer:    tasktoken.NewSerializer(),
		metricsHandler:     s.mockShard.GetMetricsHandler(),
		eventNotifier:      events.NewNotifier(clock.NewRealTimeSource(), metrics.NoopMetricsHandler, func(namespace.ID, string) int32 { return 1 }),
		queueProcessors: map[tasks.Category]queues.Queue{
			s.mockTxProcessor.Category():         s.mockTxProcessor,
			s.mockTimerProcessor.Category():      s.mockTimerProcessor,
			s.mockVisibilityProcessor.Category(): s.mockVisibilityProcessor,
			s.mockArchivalProcessor.Category():   s.mockArchivalProcessor,
		},
	}
	s.mockShard.SetEngineForTesting(h)
	s.mockHistoryEngine = h

	s.timerQueueActiveTaskExecutor = newTimerQueueActiveTaskExecutor(
		s.mockShard,
		s.workflowCache,
		s.mockDeleteManager,
		s.logger,
		metrics.NoopMetricsHandler,
		s.config,
		s.mockShard.Resource.GetMatchingClient(),
		s.mockChasmEngine,
	).(*timerQueueActiveTaskExecutor)
}

func (s *timerQueueActiveTaskExecutorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *timerQueueActiveTaskExecutorSuite) TestProcessUserTimerTimeout_Fire() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowKey := definition.NewWorkflowKey(
		s.namespaceID.String(),
		execution.GetWorkflowId(),
		execution.GetRunId(),
	)
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(
		s.mockShard,
		s.mockShard.GetEventsCache(),
		s.logger,
		s.version,
		execution.GetWorkflowId(),
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
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

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
		WorkflowKey:         workflowKey,
		TaskID:              s.mustGenerateTaskID(),
		VisibilityTimestamp: task.(*tasks.UserTimerTask).VisibilityTimestamp,
		EventID:             event.EventId,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())

	for _, currentTime := range []time.Time{
		s.now.Add(-timerTimeout),
		s.now.Add(2 * timerTimeout),
	} {
		getWorkflowExecutionResponse := &persistence.GetWorkflowExecutionResponse{State: common.CloneProto(persistenceMutableState)}
		s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(getWorkflowExecutionResponse, nil)
		s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

		s.timeSource.Update(currentTime)
		resp := s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
		s.NoError(resp.ExecutionErr)

		_, ok := s.getMutableStateFromCache(workflowKey).GetUserTimerInfo(timerID)
		s.False(ok)

		s.clearMutableStateFromCache(workflowKey)
	}
}

func (s *timerQueueActiveTaskExecutorSuite) TestProcessUserTimerTimeout_Noop() {
	execution := &commonpb.WorkflowExecution{
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
		execution.GetWorkflowId(),
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
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

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
		TaskID:              s.mustGenerateTaskID(),
		VisibilityTimestamp: task.(*tasks.UserTimerTask).VisibilityTimestamp,
		EventID:             event.EventId,
	}

	event = addTimerFiredEvent(mutableState, timerID)
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.timeSource.Update(s.now.Add(2 * timerTimeout))
	resp := s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.ErrorIs(resp.ExecutionErr, errNoTimerFired)
}

func (s *timerQueueActiveTaskExecutorSuite) TestProcessUserTimerTimeout_WfClosed() {
	execution := &commonpb.WorkflowExecution{
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
		execution.GetWorkflowId(),
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
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

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
		TaskID:              s.mustGenerateTaskID(),
		VisibilityTimestamp: task.(*tasks.UserTimerTask).VisibilityTimestamp,
		EventID:             event.EventId,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	persistenceMutableState.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.timeSource.Update(s.now.Add(2 * timerTimeout))
	resp := s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.ErrorIs(resp.ExecutionErr, consts.ErrWorkflowCompleted)
}

func (s *timerQueueActiveTaskExecutorSuite) TestProcessUserTimerTimeout_NoTimerAndWfClosed() {
	execution := &commonpb.WorkflowExecution{
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
		execution.GetWorkflowId(),
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
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	timerTask := &tasks.UserTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		TaskID:              s.mustGenerateTaskID(),
		VisibilityTimestamp: time.Now(),
		EventID:             event.EventId,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	persistenceMutableState.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	resp := s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.ErrorIs(resp.ExecutionErr, errNoTimerFired)
}

func (s *timerQueueActiveTaskExecutorSuite) TestProcessActivityTimeout_NoRetryPolicy_Fire() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowKey := definition.NewWorkflowKey(
		s.namespaceID.String(),
		execution.GetWorkflowId(),
		execution.GetRunId(),
	)
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(200 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

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
		WorkflowKey:         workflowKey,
		Attempt:             1,
		TaskID:              s.mustGenerateTaskID(),
		TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		VisibilityTimestamp: task.(*tasks.ActivityTimeoutTask).VisibilityTimestamp,
		EventID:             scheduledEvent.EventId,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())

	for _, currentTime := range []time.Time{
		s.now.Add(-timerTimeout),
		s.now.Add(2 * timerTimeout),
	} {
		getWorkflowExecutionResponse := &persistence.GetWorkflowExecutionResponse{State: common.CloneProto(persistenceMutableState)}
		s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(getWorkflowExecutionResponse, nil)
		s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

		s.timeSource.Update(currentTime)
		resp := s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
		s.NoError(resp.ExecutionErr)

		_, ok := s.getMutableStateFromCache(workflowKey).GetActivityInfo(scheduledEvent.GetEventId())
		s.False(ok)

		s.clearMutableStateFromCache(workflowKey)
	}
}

func (s *timerQueueActiveTaskExecutorSuite) TestProcessActivityTimeout_NoRetryPolicy_Noop() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(200 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

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
		TaskID:              s.mustGenerateTaskID(),
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
	resp := s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(resp.ExecutionErr)
}

func (s *timerQueueActiveTaskExecutorSuite) TestProcessActivityTimeout_RetryPolicy_Retry() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowKey := definition.NewWorkflowKey(
		s.namespaceID.String(),
		execution.GetWorkflowId(),
		execution.GetRunId(),
	)
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

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
			InitialInterval:        durationpb.New(1 * time.Second),
			BackoffCoefficient:     1.2,
			MaximumInterval:        durationpb.New(5 * time.Second),
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
		WorkflowKey:         workflowKey,
		Attempt:             1,
		TaskID:              s.mustGenerateTaskID(),
		TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		VisibilityTimestamp: task.(*tasks.ActivityTimeoutTask).VisibilityTimestamp,
		EventID:             scheduledEvent.EventId,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	s.timeSource.Update(s.now.Add(2 * timerTimeout))
	resp := s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(resp.ExecutionErr)

	activityInfo, ok := s.getMutableStateFromCache(workflowKey).GetActivityInfo(scheduledEvent.GetEventId())
	s.True(ok)
	s.Equal(scheduledEvent.GetEventId(), activityInfo.ScheduledEventId)
	s.Equal(common.EmptyEventID, activityInfo.StartedEventId)
	// only a schedule to start timer will be created, apart from the retry timer
	s.Equal(int32(workflow.TimerTaskStatusCreatedScheduleToStart), activityInfo.TimerTaskStatus)
}

func (s *timerQueueActiveTaskExecutorSuite) TestProcessActivityTimeout_RetryPolicy_RetryTimeout() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

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
			InitialInterval:        durationpb.New(1 * time.Second),
			BackoffCoefficient:     1.2,
			MaximumInterval:        durationpb.New(5 * time.Second),
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
		TaskID:              s.mustGenerateTaskID(),
		TimeoutType:         enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		VisibilityTimestamp: task.(*tasks.ActivityTimeoutTask).VisibilityTimestamp,
		EventID:             scheduledEvent.EventId,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			ctx context.Context,
			request *persistence.UpdateWorkflowExecutionRequest,
		) (*persistence.UpdateWorkflowExecutionResponse, error) {
			s.Len(request.UpdateWorkflowEvents, 1)
			// activityStarted (since activity has retry policy), activityTimedOut, workflowTaskScheduled
			s.Len(request.UpdateWorkflowEvents[0].Events, 3)

			timeoutEvent := request.UpdateWorkflowEvents[0].Events[1]
			s.Equal(enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT, timeoutEvent.GetEventType())
			timeoutAttributes := timeoutEvent.GetActivityTaskTimedOutEventAttributes()
			s.Equal(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE, timeoutAttributes.Failure.GetTimeoutFailureInfo().GetTimeoutType())
			s.Contains(timeoutAttributes.Failure.Message, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE.String())
			s.Equal(enumspb.RETRY_STATE_TIMEOUT, timeoutAttributes.RetryState)
			return tests.UpdateWorkflowExecutionResponse, nil
		})

	s.timeSource.Update(s.now.Add(2 * timerTimeout))
	resp := s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(resp.ExecutionErr)
}

func (s *timerQueueActiveTaskExecutorSuite) TestProcessActivityTimeout_RetryPolicy_Fire() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowKey := definition.NewWorkflowKey(
		s.namespaceID.String(),
		execution.GetWorkflowId(),
		execution.GetRunId(),
	)
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

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
			InitialInterval:        durationpb.New(1 * time.Second),
			BackoffCoefficient:     1.2,
			MaximumInterval:        durationpb.New(5 * time.Second),
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
		WorkflowKey:         workflowKey,
		Attempt:             1,
		TaskID:              s.mustGenerateTaskID(),
		TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		VisibilityTimestamp: task.(*tasks.ActivityTimeoutTask).VisibilityTimestamp,
		EventID:             scheduledEvent.EventId,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	s.timeSource.Update(s.now.Add(2 * timerTimeout))
	resp := s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(resp.ExecutionErr)

	_, ok := s.getMutableStateFromCache(workflowKey).GetActivityInfo(scheduledEvent.GetEventId())
	s.False(ok)
}

func (s *timerQueueActiveTaskExecutorSuite) TestProcessActivityTimeout_RetryPolicy_Noop() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

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
			InitialInterval:        durationpb.New(1 * time.Second),
			BackoffCoefficient:     1.2,
			MaximumInterval:        durationpb.New(5 * time.Second),
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
		TaskID:              s.mustGenerateTaskID(),
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
	resp := s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(resp.ExecutionErr)
}

func (s *timerQueueActiveTaskExecutorSuite) TestProcessActivityTimeout_Heartbeat_Noop() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

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
			InitialInterval:        durationpb.New(1 * time.Second),
			BackoffCoefficient:     1.2,
			MaximumInterval:        durationpb.New(5 * time.Second),
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
		TaskID:              s.mustGenerateTaskID(),
		TimeoutType:         enumspb.TIMEOUT_TYPE_HEARTBEAT,
		VisibilityTimestamp: time.Time{},
		EventID:             scheduledEvent.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	resp := s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(resp.ExecutionErr)
}

func (s *timerQueueActiveTaskExecutorSuite) TestWorkflowTaskTimeout_Fire() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowKey := definition.NewWorkflowKey(
		s.namespaceID.String(),
		execution.GetWorkflowId(),
		execution.GetRunId(),
	)
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	startedEvent := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())

	timerTask := &tasks.WorkflowTaskTimeoutTask{
		WorkflowKey:         workflowKey,
		ScheduleAttempt:     1,
		Version:             s.version,
		TaskID:              s.mustGenerateTaskID(),
		TimeoutType:         enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		VisibilityTimestamp: s.now,
		EventID:             wt.ScheduledEventID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, startedEvent.GetEventId(), startedEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	resp := s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(resp.ExecutionErr)

	workflowTask := s.getMutableStateFromCache(workflowKey).GetPendingWorkflowTask()
	s.NotNil(workflowTask)
	s.True(workflowTask.ScheduledEventID != common.EmptyEventID)
	s.Equal(common.EmptyEventID, workflowTask.StartedEventID)
	s.Equal(int32(2), workflowTask.Attempt)
}

func (s *timerQueueActiveTaskExecutorSuite) TestWorkflowTaskTimeout_Noop() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
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
		TaskID:              s.mustGenerateTaskID(),
		TimeoutType:         enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		VisibilityTimestamp: s.now,
		EventID:             wt.ScheduledEventID - 1,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, startedEvent.GetEventId(), startedEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	resp := s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(resp.ExecutionErr)
}

func (s *timerQueueActiveTaskExecutorSuite) TestWorkflowTaskTimeout_StampMismatch() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	startedEvent := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())

	// Create timer task with original stamp
	timerTask := &tasks.WorkflowTaskTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		ScheduleAttempt:     1,
		Version:             s.version,
		TaskID:              s.mustGenerateTaskID(),
		TimeoutType:         enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		VisibilityTimestamp: s.now,
		EventID:             wt.ScheduledEventID,
		Stamp:               wt.Stamp,
	}

	// Modify the workflow task stamp in mutable state to create mismatch
	mutableState.GetExecutionInfo().WorkflowTaskStamp = wt.Stamp + 1

	persistenceMutableState := s.createPersistenceMutableState(mutableState, startedEvent.GetEventId(), startedEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	resp := s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.ErrorIs(resp.ExecutionErr, consts.ErrStaleReference)
}

func (s *timerQueueActiveTaskExecutorSuite) TestWorkflowBackoffTimer_Fire() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowKey := definition.NewWorkflowKey(
		s.namespaceID.String(),
		execution.GetWorkflowId(),
		execution.GetRunId(),
	)
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetWorkflowId(), execution.GetRunId())
	event, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	timerTask := &tasks.WorkflowBackoffTimerTask{
		WorkflowKey:         workflowKey,
		Version:             s.version,
		TaskID:              s.mustGenerateTaskID(),
		WorkflowBackoffType: enumsspb.WORKFLOW_BACKOFF_TYPE_RETRY,
		VisibilityTimestamp: s.now,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	resp := s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(resp.ExecutionErr)

	workflowTask := s.getMutableStateFromCache(workflowKey).GetPendingWorkflowTask()
	s.NotNil(workflowTask)
	s.True(workflowTask.ScheduledEventID != common.EmptyEventID)
	s.Equal(common.EmptyEventID, workflowTask.StartedEventID)
	s.Equal(int32(1), workflowTask.Attempt)
}

func (s *timerQueueActiveTaskExecutorSuite) TestWorkflowBackoffTimer_Noop() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	timerTask := &tasks.WorkflowBackoffTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		TaskID:              s.mustGenerateTaskID(),
		WorkflowBackoffType: enumsspb.WORKFLOW_BACKOFF_TYPE_RETRY,
		VisibilityTimestamp: s.now,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	resp := s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.ErrorIs(resp.ExecutionErr, errNoTimerFired)
}

func (s *timerQueueActiveTaskExecutorSuite) TestActivityRetryTimer_Fire() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetWorkflowId(), execution.GetRunId())
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
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	activityID := "activity"
	activityType := "activity type"
	timerTimeout := 2 * time.Second
	scheduledEvent, activityInfo := addActivityTaskScheduledEventWithRetry(
		mutableState,
		event.GetEventId(),
		activityID,
		activityType,
		taskQueueName,
		nil,
		timerTimeout,
		timerTimeout,
		timerTimeout,
		timerTimeout,
		&commonpb.RetryPolicy{
			InitialInterval:        durationpb.New(1 * time.Second),
			BackoffCoefficient:     1.2,
			MaximumInterval:        durationpb.New(5 * time.Second),
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
		TaskID:              s.mustGenerateTaskID(),
		VisibilityTimestamp: s.now,
		EventID:             activityInfo.ScheduledEventId,
		Attempt:             activityInfo.Attempt,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.EXPECT().AddActivityTask(
		gomock.Any(),
		pm.Eq(&matchingservice.AddActivityTaskRequest{
			NamespaceId: s.namespaceID.String(),
			Execution:   execution,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: activityInfo.TaskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			ScheduledEventId:       activityInfo.ScheduledEventId,
			ScheduleToStartTimeout: activityInfo.ScheduleToStartTimeout,
			Clock:                  vclock.NewVectorClock(s.mockClusterMetadata.GetClusterID(), s.mockShard.GetShardID(), timerTask.TaskID),
			VersionDirective:       worker_versioning.MakeUseAssignmentRulesDirective(),
		}),
		gomock.Any(),
	).Return(&matchingservice.AddActivityTaskResponse{}, nil)

	resp := s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(resp.ExecutionErr)
}

func (s *timerQueueActiveTaskExecutorSuite) TestActivityRetryTimer_Noop() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

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
			InitialInterval:        durationpb.New(1 * time.Second),
			BackoffCoefficient:     1.2,
			MaximumInterval:        durationpb.New(5 * time.Second),
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
		TaskID:              s.mustGenerateTaskID(),
		VisibilityTimestamp: s.now,
		EventID:             activityInfo.ScheduledEventId,
		Attempt:             activityInfo.Attempt,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	resp := s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.ErrorIs(resp.ExecutionErr, consts.ErrActivityTaskNotFound)
}

func (s *timerQueueActiveTaskExecutorSuite) TestWorkflowRunTimeout_Fire() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowKey := definition.NewWorkflowKey(
		s.namespaceID.String(),
		execution.GetWorkflowId(),
		execution.GetRunId(),
	)
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	expirationTime := 10 * time.Second

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
			WorkflowExecutionExpirationTime: timestamppb.New(s.now.Add(expirationTime)),
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	startEvent := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = startEvent.GetEventId()
	completionEvent := addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	timerTask := &tasks.WorkflowRunTimeoutTask{
		WorkflowKey:         workflowKey,
		Version:             s.version,
		TaskID:              s.mustGenerateTaskID(),
		VisibilityTimestamp: s.now.Add(expirationTime),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, completionEvent.GetEventId(), completionEvent.GetVersion())

	for _, currrentTime := range []time.Time{
		s.now.Add(expirationTime - 1*time.Second),
		s.now.Add(expirationTime + 1*time.Second),
	} {
		getWorkflowExecutionResponse := &persistence.GetWorkflowExecutionResponse{State: common.CloneProto(persistenceMutableState)}
		s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(getWorkflowExecutionResponse, nil)
		s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

		s.timeSource.Update(currrentTime)
		resp := s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
		s.NoError(resp.ExecutionErr)

		running := s.getMutableStateFromCache(workflowKey).IsWorkflowExecutionRunning()
		s.False(running)

		s.clearMutableStateFromCache(workflowKey)
	}
}

func (s *timerQueueActiveTaskExecutorSuite) TestWorkflowRunTimeout_Retry() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowKey := definition.NewWorkflowKey(
		s.namespaceID.String(),
		execution.GetWorkflowId(),
		execution.GetRunId(),
	)
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	executionRunTimeout := time.Duration(10 * time.Second)
	workflowRunTimeout := time.Duration(200 * time.Second)

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  durationpb.New(workflowRunTimeout),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
			WorkflowExecutionExpirationTime: timestamppb.New(s.now.Add(executionRunTimeout)),
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
	completionEvent := addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	timerTask := &tasks.WorkflowRunTimeoutTask{
		WorkflowKey:         workflowKey,
		Version:             s.version,
		TaskID:              s.mustGenerateTaskID(),
		VisibilityTimestamp: s.now,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, completionEvent.GetEventId(), completionEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	// one for current workflow, one for new
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	// move
	s.timeSource.Advance(workflowRunTimeout + 1*time.Second)
	resp := s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(resp.ExecutionErr)

	state, status := s.getMutableStateFromCache(workflowKey).GetWorkflowStateStatus()
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, state)
	s.EqualValues(enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, status)
}

func (s *timerQueueActiveTaskExecutorSuite) TestWorkflowRunTimeout_Cron() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowKey := definition.NewWorkflowKey(
		s.namespaceID.String(),
		execution.GetWorkflowId(),
		execution.GetRunId(),
	)
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	expirationTime := time.Duration(10 * time.Second)

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
			WorkflowExecutionExpirationTime: timestamppb.New(s.now.Add(expirationTime)),
		},
	)
	s.Nil(err)
	mutableState.GetExecutionState().StartTime = timestamppb.New(s.now)
	mutableState.GetExecutionInfo().CronSchedule = "* * * * *"

	wt := addWorkflowTaskScheduledEvent(mutableState)
	startEvent := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = startEvent.GetEventId()
	completionEvent := addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	timerTask := &tasks.WorkflowRunTimeoutTask{
		WorkflowKey:         workflowKey,
		Version:             s.version,
		TaskID:              s.mustGenerateTaskID(),
		VisibilityTimestamp: s.now,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, completionEvent.GetEventId(), completionEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	// one for current workflow, one for new
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	// advance timer past run expiration time
	s.timeSource.Advance(expirationTime + 1*time.Second)
	resp := s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(resp.ExecutionErr)

	state, status := s.getMutableStateFromCache(workflowKey).GetWorkflowStateStatus()
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, state)
	s.EqualValues(enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, status)
}

func (s *timerQueueActiveTaskExecutorSuite) TestWorkflowRunTimeout_WorkflowExpired() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowKey := definition.NewWorkflowKey(
		s.namespaceID.String(),
		execution.GetWorkflowId(),
		execution.GetRunId(),
	)
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
			WorkflowExecutionExpirationTime: timestamppb.New(s.now.Add(-1 * time.Second)),
		},
	)
	s.Nil(err)
	mutableState.GetExecutionState().StartTime = timestamppb.New(s.now)
	mutableState.GetExecutionInfo().CronSchedule = "* * * * *"

	wt := addWorkflowTaskScheduledEvent(mutableState)
	startEvent := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = startEvent.GetEventId()
	completionEvent := addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	timerTask := &tasks.WorkflowRunTimeoutTask{
		WorkflowKey:         workflowKey,
		Version:             s.version,
		TaskID:              s.mustGenerateTaskID(),
		VisibilityTimestamp: s.now,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, completionEvent.GetEventId(), completionEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	resp := s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(resp.ExecutionErr)

	state, status := s.getMutableStateFromCache(workflowKey).GetWorkflowStateStatus()
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, state)
	s.EqualValues(enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, status)
}

func (s *timerQueueActiveTaskExecutorSuite) TestWorkflowExecutionTimeout_Fire() {
	firstRunID := uuid.New()
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowKey := definition.NewWorkflowKey(
		s.namespaceID.String(),
		execution.GetWorkflowId(),
		execution.GetRunId(),
	)
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	var mutableState historyi.MutableState
	mutableState = workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetWorkflowId(), execution.GetRunId())
	startedEvent, err := mutableState.AddWorkflowExecutionStartedEventWithOptions(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
			WorkflowExecutionExpirationTime: timestamppb.New(s.now.Add(10 * time.Second)),
		},
		nil,
		uuid.New(),
		firstRunID,
	)
	s.Nil(err)

	timerTask := &tasks.WorkflowExecutionTimeoutTask{
		NamespaceID:         s.namespaceID.String(),
		WorkflowID:          execution.GetWorkflowId(),
		FirstRunID:          firstRunID,
		VisibilityTimestamp: s.now.Add(10 * time.Second),
		TaskID:              s.mustGenerateTaskID(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, startedEvent.GetEventId(), startedEvent.GetVersion())

	for _, currentTime := range []time.Time{
		s.now.Add(5 * time.Second),
		s.now.Add(15 * time.Second),
	} {
		getWorkflowExecutionResponse := &persistence.GetWorkflowExecutionResponse{State: common.CloneProto(persistenceMutableState)}
		persistenceExecutionState := getWorkflowExecutionResponse.State.ExecutionState
		s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
			ShardID:     s.mockShard.GetShardID(),
			NamespaceID: s.namespaceID.String(),
			WorkflowID:  execution.GetWorkflowId(),
		}).Return(&persistence.GetCurrentExecutionResponse{
			StartRequestID: persistenceExecutionState.CreateRequestId,
			RunID:          persistenceExecutionState.RunId,
			State:          persistenceExecutionState.State,
			Status:         persistenceExecutionState.Status,
		}, nil).Times(1)
		s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(getWorkflowExecutionResponse, nil)
		s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

		s.timeSource.Update(currentTime)

		resp := s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
		s.NoError(resp.ExecutionErr)

		mutableState = s.getMutableStateFromCache(workflowKey)
		s.False(mutableState.IsWorkflowExecutionRunning())
		state, status := mutableState.GetWorkflowStateStatus()
		s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, state)
		s.EqualValues(enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, status)

		s.clearMutableStateFromCache(workflowKey)
	}
}

func (s *timerQueueActiveTaskExecutorSuite) TestWorkflowExecutionTimeout_Noop() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetWorkflowId(), execution.GetRunId())
	startedEvent, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
			WorkflowExecutionExpirationTime: timestamppb.New(s.now.Add(10 * time.Second)),
		},
	)
	s.Nil(err)

	timerTask := &tasks.WorkflowExecutionTimeoutTask{
		NamespaceID:         s.namespaceID.String(),
		WorkflowID:          execution.GetWorkflowId(),
		FirstRunID:          uuid.New(), // does not match the firsrt runID of the execution
		VisibilityTimestamp: s.now,
		TaskID:              s.mustGenerateTaskID(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, startedEvent.GetEventId(), startedEvent.GetVersion())
	persistenceExecutionState := persistenceMutableState.ExecutionState
	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: s.namespaceID.String(),
		WorkflowID:  execution.GetWorkflowId(),
	}).Return(&persistence.GetCurrentExecutionResponse{
		StartRequestID: persistenceExecutionState.CreateRequestId,
		RunID:          persistenceExecutionState.RunId,
		State:          persistenceExecutionState.State,
		Status:         persistenceExecutionState.Status,
	}, nil).Times(1)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	resp := s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(resp.ExecutionErr)
}

func (s *timerQueueActiveTaskExecutorSuite) TestExecuteChasmPureTimerTask_ZombieWorkflow() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: uuid.New(),
		RunId:      uuid.New(),
	}

	// Start the workflow with an event, and then push it into zombie state.
	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetWorkflowId(), execution.GetRunId())
	startedEvent, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId:  s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{},
		},
	)
	s.Nil(err)
	mutableState.GetExecutionState().State = enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE

	// Add a valid timer task.
	timerTask := &tasks.ChasmTaskPure{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		VisibilityTimestamp: s.now,
		TaskID:              s.mustGenerateTaskID(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, startedEvent.GetEventId(), startedEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	// Execution should fail due to zombie.
	resp := s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.ErrorIs(resp.ExecutionErr, consts.ErrWorkflowZombie)
}

func (s *timerQueueActiveTaskExecutorSuite) TestExecuteStateMachineTimerTask_ZombieWorkflow() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetWorkflowId(), execution.GetRunId())
	startedEvent, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId:  s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{},
		},
	)
	s.Nil(err)
	mutableState.GetExecutionState().State = enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE

	timerTask := &tasks.StateMachineTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		VisibilityTimestamp: s.now,
		TaskID:              s.mustGenerateTaskID(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, startedEvent.GetEventId(), startedEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	resp := s.timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.ErrorIs(resp.ExecutionErr, consts.ErrWorkflowZombie)
}

func (s *timerQueueActiveTaskExecutorSuite) TestExecuteChasmSideEffectTimerTask_ExecutesTask() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowKey.WorkflowID,
		RunId:      tests.WorkflowKey.RunID,
	}

	// Mock the CHASM tree.
	chasmTree := historyi.NewMockChasmTree(s.controller)
	chasmTree.EXPECT().ExecuteSideEffectTask(
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
	).Times(1).Return(nil)

	// Mock mutable state.
	ms := historyi.NewMockMutableState(s.controller)
	info := &persistencespb.WorkflowExecutionInfo{}
	ms.EXPECT().GetCurrentVersion().Return(int64(2)).AnyTimes()
	ms.EXPECT().NextTransitionCount().Return(int64(0)).AnyTimes() // emulate transition history disabled.
	ms.EXPECT().GetNextEventID().Return(int64(2)).AnyTimes()
	ms.EXPECT().GetExecutionInfo().Return(info).AnyTimes()
	ms.EXPECT().GetWorkflowKey().Return(tests.WorkflowKey).AnyTimes()
	ms.EXPECT().GetExecutionState().Return(
		&persistencespb.WorkflowExecutionState{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING},
	).AnyTimes()
	ms.EXPECT().ChasmTree().Return(chasmTree).AnyTimes()

	// Add a valid timer task.
	timerTask := &tasks.ChasmTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		VisibilityTimestamp: s.now,
		TaskID:              s.mustGenerateTaskID(),
		Info: &persistencespb.ChasmTaskInfo{
			ComponentInitialVersionedTransition:    &persistencespb.VersionedTransition{},
			ComponentLastUpdateVersionedTransition: &persistencespb.VersionedTransition{},
			Path:                                   []string{},
			Type:                                   "Testlib.TestSideEffectTask",
			Data: &commonpb.DataBlob{
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			},
		},
	}

	wfCtx := historyi.NewMockWorkflowContext(s.controller)
	wfCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(ms, nil)

	mockCache := wcache.NewMockCache(s.controller)
	mockCache.EXPECT().GetOrCreateChasmEntity(
		gomock.Any(), s.mockShard, gomock.Any(), execution, chasm.ArchetypeAny, locks.PriorityLow,
	).Return(wfCtx, wcache.NoopReleaseFn, nil)

	//nolint:revive // unchecked-type-assertion
	timerQueueActiveTaskExecutor := newTimerQueueActiveTaskExecutor(
		s.mockShard,
		mockCache,
		s.mockDeleteManager,
		s.mockShard.GetLogger(),
		metrics.NoopMetricsHandler,
		s.config,
		s.mockShard.Resource.GetMatchingClient(),
		s.mockChasmEngine,
	).(*timerQueueActiveTaskExecutor)

	// Execution should succeed.
	resp := timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NotNil(resp)
	s.Nil(resp.ExecutionErr)
}

func (s *timerQueueActiveTaskExecutorSuite) TestExecuteChasmPureTimerTask_ExecutesAllPureTimers() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowKey.WorkflowID,
		RunId:      tests.WorkflowKey.RunID,
	}

	// Mock the CHASM tree and execute interface.
	mockEach := chasm.NewMockNodePureTask(s.controller)
	mockEach.EXPECT().ExecutePureTask(gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
	chasmTree := historyi.NewMockChasmTree(s.controller)
	chasmTree.EXPECT().EachPureTask(gomock.Any(), gomock.Any()).
		Times(1).Do(
		func(_ time.Time, callback func(executor chasm.NodePureTask, taskAttributes chasm.TaskAttributes, task any) (bool, error)) error {
			_, err := callback(mockEach, chasm.TaskAttributes{}, nil)
			return err
		})

	// Mock mutable state.
	ms := historyi.NewMockMutableState(s.controller)
	info := &persistencespb.WorkflowExecutionInfo{}
	ms.EXPECT().GetCurrentVersion().Return(int64(2)).AnyTimes()
	ms.EXPECT().NextTransitionCount().Return(int64(0)).AnyTimes() // emulate transition history disabled.
	ms.EXPECT().GetNextEventID().Return(int64(2)).AnyTimes()
	ms.EXPECT().GetExecutionInfo().Return(info).AnyTimes()
	ms.EXPECT().GetWorkflowKey().Return(tests.WorkflowKey).AnyTimes()
	ms.EXPECT().GetExecutionState().Return(
		&persistencespb.WorkflowExecutionState{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING},
	).AnyTimes()
	ms.EXPECT().ChasmTree().Return(chasmTree).AnyTimes()

	// Add a valid timer task.
	timerTask := &tasks.ChasmTaskPure{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		VisibilityTimestamp: s.now,
		TaskID:              s.mustGenerateTaskID(),
	}

	wfCtx := historyi.NewMockWorkflowContext(s.controller)
	wfCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(ms, nil)
	wfCtx.EXPECT().UpdateWorkflowExecutionAsActive(gomock.Any(), gomock.Any())

	mockCache := wcache.NewMockCache(s.controller)
	mockCache.EXPECT().GetOrCreateChasmEntity(
		gomock.Any(), s.mockShard, gomock.Any(), execution, chasm.ArchetypeAny, locks.PriorityLow,
	).Return(wfCtx, wcache.NoopReleaseFn, nil)

	//nolint:revive // unchecked-type-assertion
	timerQueueActiveTaskExecutor := newTimerQueueActiveTaskExecutor(
		s.mockShard,
		mockCache,
		s.mockDeleteManager,
		s.mockShard.GetLogger(),
		metrics.NoopMetricsHandler,
		s.config,
		s.mockShard.Resource.GetMatchingClient(),
		s.mockChasmEngine,
	).(*timerQueueActiveTaskExecutor)

	// Execution should succeed.
	resp := timerQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NotNil(resp)
	s.Nil(resp.ExecutionErr)
}

func (s *timerQueueActiveTaskExecutorSuite) TestExecuteStateMachineTimerTask_ExecutesAllAvailableTimers() {
	numInvocations := 0

	reg := s.mockShard.StateMachineRegistry()
	s.NoError(dummy.RegisterStateMachine(reg))
	s.NoError(dummy.RegisterTaskSerializers(reg))
	s.NoError(dummy.RegisterExecutor(
		reg,
		dummy.TaskExecutorOptions{
			ImmediateExecutor: nil,
			TimerExecutor: func(env hsm.Environment, node *hsm.Node, task dummy.TimerTask) error {
				numInvocations++
				return nil
			},
		},
	))

	we := &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}

	ms := historyi.NewMockMutableState(s.controller)
	info := &persistencespb.WorkflowExecutionInfo{}
	root, err := hsm.NewRoot(
		reg,
		workflow.StateMachineType,
		ms,
		make(map[string]*persistencespb.StateMachineMap),
		ms,
	)
	s.NoError(err)
	ms.EXPECT().GetCurrentVersion().Return(int64(2)).AnyTimes()
	ms.EXPECT().NextTransitionCount().Return(int64(0)).AnyTimes() // emulate transition history disabled.
	ms.EXPECT().GetNextEventID().Return(int64(2)).AnyTimes()
	ms.EXPECT().GetExecutionInfo().Return(info).AnyTimes()
	ms.EXPECT().GetWorkflowKey().Return(tests.WorkflowKey).AnyTimes()
	ms.EXPECT().GetExecutionState().Return(
		&persistencespb.WorkflowExecutionState{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING},
	).AnyTimes()
	ms.EXPECT().HSM().Return(root).AnyTimes()

	_, err = dummy.MachineCollection(root).Add("dummy", dummy.NewDummy())
	s.NoError(err)

	// Track some tasks.

	// Invalid reference, should be dropped.
	invalidTask := &persistencespb.StateMachineTaskInfo{
		Ref: &persistencespb.StateMachineRef{
			MutableStateVersionedTransition: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 1,
			},
			MachineInitialVersionedTransition: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 2,
			},
		},
		Type: dummy.TaskTypeTimer,
	}
	validTask := &persistencespb.StateMachineTaskInfo{
		Ref: &persistencespb.StateMachineRef{
			Path: []*persistencespb.StateMachineKey{
				{Type: dummy.StateMachineType, Id: "dummy"},
			},
			MutableStateVersionedTransition: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 2,
			},
			MachineInitialVersionedTransition: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 2,
			},
		},
		Type: dummy.TaskTypeTimer,
	}

	// Past deadline, should get executed.
	workflow.TrackStateMachineTimer(ms, s.now.Add(-time.Hour), invalidTask)
	workflow.TrackStateMachineTimer(ms, s.now.Add(-time.Hour), validTask)
	workflow.TrackStateMachineTimer(ms, s.now.Add(-time.Minute), validTask)
	// Future deadline, new task should be scheduled.
	futureDeadline := s.now.Add(time.Hour)
	workflow.TrackStateMachineTimer(ms, futureDeadline, validTask)

	wfCtx := historyi.NewMockWorkflowContext(s.controller)
	wfCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(ms, nil)
	wfCtx.EXPECT().UpdateWorkflowExecutionAsActive(gomock.Any(), gomock.Any())

	mockCache := wcache.NewMockCache(s.controller)
	mockCache.EXPECT().GetOrCreateChasmEntity(
		gomock.Any(), s.mockShard, tests.NamespaceID, we, chasmworkflow.Archetype, locks.PriorityLow,
	).Return(wfCtx, wcache.NoopReleaseFn, nil)

	task := &tasks.StateMachineTimerTask{
		WorkflowKey:         tests.WorkflowKey,
		VisibilityTimestamp: s.now,
		Version:             2,
	}

	// change now to a value earilier than task's visibility timestamp to test the case where system wall clock go backwards.
	s.timeSource.Update(s.now.Add(-30 * time.Minute))

	//nolint:revive // unchecked-type-assertion
	timerQueueActiveTaskExecutor := newTimerQueueActiveTaskExecutor(
		s.mockShard,
		mockCache,
		s.mockDeleteManager,
		s.mockShard.GetLogger(),
		metrics.NoopMetricsHandler,
		s.config,
		s.mockShard.Resource.GetMatchingClient(),
		s.mockChasmEngine,
	).(*timerQueueActiveTaskExecutor)

	err = timerQueueActiveTaskExecutor.executeStateMachineTimerTask(context.Background(), task)
	s.NoError(err)
	s.Equal(2, numInvocations) // two valid tasks within the deadline.
	s.Equal(1, len(info.StateMachineTimers))
	s.Equal(futureDeadline, info.StateMachineTimers[0].Deadline.AsTime())
}

func (s *timerQueueActiveTaskExecutorSuite) createPersistenceMutableState(
	ms historyi.MutableState,
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
	workflowKey definition.WorkflowKey,
) historyi.MutableState {
	key := wcache.Key{
		WorkflowKey: workflowKey,
		ShardUUID:   s.mockShard.GetOwner(),
	}
	return wcache.GetMutableState(s.workflowCache, key)
}

func (s *timerQueueActiveTaskExecutorSuite) clearMutableStateFromCache(
	workflowKey definition.WorkflowKey,
) {
	wcache.ClearMutableState(s.workflowCache, wcache.Key{
		WorkflowKey: workflowKey,
		ShardUUID:   s.mockShard.GetOwner(),
	})
}

func (s *timerQueueActiveTaskExecutorSuite) newTaskExecutable(
	task tasks.Task,
) queues.Executable {
	return queues.NewExecutable(
		queues.DefaultReaderId,
		task,
		s.timerQueueActiveTaskExecutor,
		nil,
		nil,
		queues.NewNoopPriorityAssigner(),
		s.mockShard.GetTimeSource(),
		s.mockNamespaceCache,
		s.mockClusterMetadata,
		nil,
		metrics.NoopMetricsHandler,
		telemetry.NoopTracer,
	)
}

func (s *timerQueueActiveTaskExecutorSuite) mustGenerateTaskID() int64 {
	taskID, err := s.mockShard.GenerateTaskID()
	s.NoError(err)
	return taskID
}

func (s *timerQueueActiveTaskExecutorSuite) TestProcessSingleActivityTimeoutTask() {
	ms := historyi.NewMockMutableState(s.controller)

	testCases := []struct {
		name                         string
		ai                           *persistencespb.ActivityInfo
		timerSequenceID              workflow.TimerSequenceID
		expectRetryActivity          bool
		retryState                   enumspb.RetryState
		retryError                   error
		expectAddTimedTask           bool
		expectedUpdateMutableState   bool
		expectedScheduleWorkflowTask bool
	}{
		{
			name: "Retry Policy Not Set",
			timerSequenceID: workflow.TimerSequenceID{
				Attempt: 1,
			},
			ai: &persistencespb.ActivityInfo{
				Attempt: 1,
				Stamp:   1,
			},
			expectRetryActivity:          true,
			retryState:                   enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET,
			retryError:                   nil,
			expectAddTimedTask:           true,
			expectedUpdateMutableState:   true,
			expectedScheduleWorkflowTask: true,
		},
		{
			name: "Retry State Timeout",
			timerSequenceID: workflow.TimerSequenceID{
				Attempt: 1,
			},
			ai: &persistencespb.ActivityInfo{
				Attempt: 1,
				Stamp:   1,
			},
			expectRetryActivity:          true,
			retryState:                   enumspb.RETRY_STATE_TIMEOUT,
			retryError:                   nil,
			expectAddTimedTask:           true,
			expectedUpdateMutableState:   true,
			expectedScheduleWorkflowTask: true,
		},
		{
			name: "Retry State In Progress",
			timerSequenceID: workflow.TimerSequenceID{
				Attempt: 1,
			},
			ai: &persistencespb.ActivityInfo{
				Attempt: 1,
			},
			expectRetryActivity:          true,
			retryState:                   enumspb.RETRY_STATE_IN_PROGRESS,
			retryError:                   nil,
			expectAddTimedTask:           false,
			expectedUpdateMutableState:   true,
			expectedScheduleWorkflowTask: false,
		},
		{
			name: "Attempt dont match",
			timerSequenceID: workflow.TimerSequenceID{
				Attempt: 1,
			},
			ai: &persistencespb.ActivityInfo{
				Attempt: 2,
			},
			expectRetryActivity:          false,
			expectAddTimedTask:           false,
			expectedUpdateMutableState:   false,
			expectedScheduleWorkflowTask: false,
		},
	}
	info := &persistencespb.WorkflowExecutionInfo{}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			if tc.expectRetryActivity {
				ms.EXPECT().RetryActivity(gomock.Any(), gomock.Any()).Return(tc.retryState, tc.retryError)
				ms.EXPECT().GetWorkflowType().Return(&commonpb.WorkflowType{Name: "test-workflow-type"}).AnyTimes()
			}

			if tc.expectAddTimedTask {
				ms.EXPECT().GetExecutionInfo().Return(info).AnyTimes()
				ms.EXPECT().AddActivityTaskTimedOutEvent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
			}
			ms.EXPECT().GetEffectiveVersioningBehavior().Return(enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED).AnyTimes()
			ms.EXPECT().GetNamespaceEntry().Return(tests.LocalNamespaceEntry).AnyTimes()

			result, err := s.timerQueueActiveTaskExecutor.processSingleActivityTimeoutTask(
				ms, tc.timerSequenceID, tc.ai)

			s.NoError(err)
			s.Equal(tc.expectedScheduleWorkflowTask, result.shouldScheduleWorkflowTask, "scheduleWorkflowTask")
			s.Equal(tc.expectedUpdateMutableState, result.shouldUpdateMutableState, "updateMutableState")
		})
	}
}
