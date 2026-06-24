package history

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/temporalproto"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/protorequire"
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

// TODO: remove all SetCurrentTime usage in this test suite
// after clusterName & getCurrentTime() method are deprecated
// from timerQueueStandbyTaskExecutor

type (
	timerQueueStandbyTaskExecutorSuite struct {
		suite.Suite
		*require.Assertions
		protorequire.ProtoAssertions

		controller          *gomock.Controller
		mockExecutionMgr    *persistence.MockExecutionManager
		mockShard           *shard.ContextTest
		mockTxProcessor     *queues.MockQueue
		mockTimerProcessor  *queues.MockQueue
		mockNamespaceCache  *namespace.MockRegistry
		mockClusterMetadata *cluster.MockMetadata
		mockAdminClient     *adminservicemock.MockAdminServiceClient
		mockDeleteManager   *deletemanager.MockDeleteManager
		mockMatchingClient  *matchingservicemock.MockMatchingServiceClient
		mockChasmEngine     *chasm.MockEngine

		config               *configs.Config
		workflowCache        wcache.Cache
		logger               log.Logger
		namespaceID          namespace.ID
		namespaceEntry       *namespace.Namespace
		version              int64
		clusterName          string
		now                  time.Time
		timeSource           *clock.EventTimeSource
		fetchHistoryDuration time.Duration
		discardDuration      time.Duration
		chasmDiscardDuration time.Duration
		clientBean           *client.MockBean

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
	s.ProtoAssertions = protorequire.New(s.T())

	s.config = tests.NewDynamicConfig()
	s.config.EnableWorkflowTaskStampIncrementOnFailure = func() bool { return true }
	s.namespaceEntry = tests.GlobalStandbyNamespaceEntry
	s.namespaceID = s.namespaceEntry.ID()
	s.version = s.namespaceEntry.FailoverVersion(namespace.EmptyBusinessID)
	s.clusterName = cluster.TestAlternativeClusterName
	s.now = time.Now().UTC()
	s.timeSource = clock.NewEventTimeSource().Update(s.now)
	s.fetchHistoryDuration = time.Minute * 12
	s.discardDuration = time.Minute * 30
	s.chasmDiscardDuration = s.config.ChasmStandbyTaskDiscardDelay("")

	s.controller = gomock.NewController(s.T())
	s.mockTxProcessor = queues.NewMockQueue(s.controller)
	s.mockTimerProcessor = queues.NewMockQueue(s.controller)
	s.mockTxProcessor.EXPECT().Category().Return(tasks.CategoryTransfer).AnyTimes()
	s.mockTimerProcessor.EXPECT().Category().Return(tasks.CategoryTimer).AnyTimes()
	s.mockTxProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	s.mockTimerProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	s.clientBean = client.NewMockBean(s.controller)
	s.mockChasmEngine = chasm.NewMockEngine(s.controller)

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

	// ack manager will use the namespace information
	s.mockNamespaceCache = s.mockShard.Resource.NamespaceCache
	s.mockExecutionMgr = s.mockShard.Resource.ExecutionMgr
	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata
	s.mockAdminClient = s.mockShard.Resource.RemoteAdminClient
	s.mockMatchingClient = s.mockShard.Resource.MatchingClient
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(s.namespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceName(gomock.Any()).Return(s.namespaceEntry.Name(), nil).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetClusterID().Return(tests.Version).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsVersionFromSameCluster(tests.Version, tests.Version).Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.IsGlobalNamespace(), s.version).Return(s.clusterName).AnyTimes()
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
		eventNotifier:      events.NewNotifier(s.timeSource, metrics.NoopMetricsHandler, func(namespace.ID, string) int32 { return 1 }),
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
		s.mockMatchingClient,
		s.mockChasmEngine,
		s.logger,
		metrics.NoopMetricsHandler,
		s.clusterName,
		s.config,
		s.clientBean,
	).(*timerQueueStandbyTaskExecutor)
}

func (s *timerQueueStandbyTaskExecutorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessUserTimerTimeout_Pending() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
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
	s.NoError(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.NewString())
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
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.timeSource.Update(s.now.Add(-time.Second))
	s.mockShard.SetCurrentTime(s.clusterName, s.now.Add(-time.Second))
	resp := s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskRetry, resp.ExecutionErr)

	s.timeSource.Update(s.now.Add(2 * timerTimeout))
	s.mockShard.SetCurrentTime(s.clusterName, s.now.Add(2*timerTimeout))
	resp = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskRetry, resp.ExecutionErr)

	s.timeSource.Update(s.now.Add(s.fetchHistoryDuration))
	s.mockShard.SetCurrentTime(s.clusterName, s.now.Add(s.fetchHistoryDuration))
	resp = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskRetry, resp.ExecutionErr)

	s.timeSource.Update(s.now.Add(s.discardDuration))
	s.mockShard.SetCurrentTime(s.clusterName, s.now.Add(s.discardDuration))
	resp = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskDiscarded, resp.ExecutionErr)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessUserTimerTimeout_Success() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard,
		s.mockShard.GetEventsCache(),
		s.logger,
		s.version,
		execution.GetWorkflowId(),
		execution.GetRunId())
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
	s.NoError(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.NewString())
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

	s.mockShard.SetCurrentTime(s.clusterName, s.now)
	resp := s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(resp.ExecutionErr)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessUserTimerTimeout_Multiple() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
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
	s.NoError(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.NewString())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

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
		TaskID:              s.mustGenerateTaskID(),
		VisibilityTimestamp: task.(*tasks.UserTimerTask).VisibilityTimestamp,
		EventID:             event.EventId,
	}

	event = addTimerFiredEvent(mutableState, timerID1)
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, s.now)
	resp := s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(resp.ExecutionErr)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessActivityTimeout_Pending() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
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
	s.NoError(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.NewString())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

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
		EventID:             event.EventId,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.timeSource.Update(s.now.Add(-time.Second))
	s.mockShard.SetCurrentTime(s.clusterName, s.now.Add(-time.Second))
	resp := s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskRetry, resp.ExecutionErr)

	s.timeSource.Update(s.now.Add(2 * timerTimeout))
	s.mockShard.SetCurrentTime(s.clusterName, s.now.Add(2*timerTimeout))
	resp = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskRetry, resp.ExecutionErr)

	s.timeSource.Update(s.now.Add(s.fetchHistoryDuration))
	s.mockShard.SetCurrentTime(s.clusterName, s.now.Add(s.fetchHistoryDuration))
	resp = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskRetry, resp.ExecutionErr)

	s.timeSource.Update(s.now.Add(s.discardDuration))
	s.mockShard.SetCurrentTime(s.clusterName, s.now.Add(s.discardDuration))
	resp = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskDiscarded, resp.ExecutionErr)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessActivityTimeout_Success() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
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
	s.NoError(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.NewString())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

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
		TaskID:              s.mustGenerateTaskID(),
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
	resp := s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(resp.ExecutionErr)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessActivityTimeout_Heartbeat_Noop() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
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
	s.NoError(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.NewString())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

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
		TaskID:              s.mustGenerateTaskID(),
		TimeoutType:         enumspb.TIMEOUT_TYPE_HEARTBEAT,
		VisibilityTimestamp: time.Unix(946684800, 0).Add(-100 * time.Second), // see pendingActivityTimerHeartbeats from mutable state
		EventID:             scheduledEvent.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, startedEvent.GetEventId(), startedEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, s.now)
	resp := s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(resp.ExecutionErr)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessActivityTimeout_Multiple_CanUpdate() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
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
	s.NoError(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.NewString())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

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
	activityInfo2.LastHeartbeatUpdateTime = timestamppb.New(time.Now().UTC())

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
		TaskID:              s.mustGenerateTaskID(),
		TimeoutType:         enumspb.TIMEOUT_TYPE_HEARTBEAT,
		VisibilityTimestamp: activityInfo2.LastHeartbeatUpdateTime.AsTime().Add(-5 * time.Second),
		EventID:             scheduledEvent2.GetEventId(),
	}

	completeEvent1 := addActivityTaskCompletedEvent(mutableState, scheduledEvent1.GetEventId(), startedEvent1.GetEventId(), nil, identity)
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, completeEvent1.GetEventId(), completeEvent1.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, input *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
			s.Len(input.UpdateWorkflowMutation.Tasks[tasks.CategoryTimer], 1)
			s.Len(input.UpdateWorkflowMutation.UpsertActivityInfos, 1)
			mutableState.GetExecutionInfo().LastUpdateTime = input.UpdateWorkflowMutation.ExecutionInfo.LastUpdateTime
			input.RangeID = 0
			input.UpdateWorkflowMutation.ExecutionInfo.LastRunningClock = 0
			input.UpdateWorkflowMutation.ExecutionInfo.LastFirstEventTxnId = 0
			input.UpdateWorkflowMutation.ExecutionInfo.StateTransitionCount = 0
			mutableState.GetExecutionInfo().LastRunningClock = 0
			mutableState.GetExecutionInfo().LastFirstEventTxnId = 0
			mutableState.GetExecutionInfo().StateTransitionCount = 0
			mutableState.GetExecutionInfo().WorkflowTaskOriginalScheduledTime = input.UpdateWorkflowMutation.ExecutionInfo.WorkflowTaskOriginalScheduledTime
			mutableState.GetExecutionInfo().ExecutionStats = &persistencespb.ExecutionStats{}

			s.True(temporalproto.DeepEqual(&persistence.UpdateWorkflowExecutionRequest{
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
			}, input))
			return tests.UpdateWorkflowExecutionResponse, nil
		})

	s.mockShard.SetCurrentTime(s.clusterName, s.now)
	resp := s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(resp.ExecutionErr)
}

// TestProcessUserTimerTimeout_StandbyFiresUnderSkip: without virtual time, the
// task's wall fire time arrives before MS's virtual deadline so standby acks
// silently. With virtual time, standby sees the timer as expired and retries
// until the fired event replicates.
func (s *timerQueueStandbyTaskExecutorSuite) TestProcessUserTimerTimeout_StandbyFiresUnderSkip() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
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
				WorkflowRunTimeout:  durationpb.New(72 * time.Hour),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
		},
	)
	s.NoError(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.NewString())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	const timerDur = 30 * time.Minute
	timerID := "t1"
	event, _ = addTimerStartedEvent(mutableState, event.GetEventId(), timerID, timerDur)

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	// Attach time-skipping state on the persistence proto. On reload, the wrapper
	// makes ms.Now() return s.now + 1h, so the persisted virtual expiry (~s.now+30min)
	// is already past in the virtual frame.
	persistenceMutableState.ExecutionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
		AccumulatedSkippedDuration: durationpb.New(30 * time.Minute),
	}

	// VisibilityTimestamp = virtual_deadline - skip = wall_deadline ≈ s.now - 30min.
	// Dispatchable (≤ wall_now = s.now); mirrors what AddTasks would have produced
	// if TSI had been present at scheduling time.
	timerTask := &tasks.UserTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		TaskID:              s.mustGenerateTaskID(),
		VisibilityTimestamp: s.now,
		EventID:             event.EventId,
	}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockShard.SetCurrentTime(s.clusterName, s.now)
	resp := s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskRetry, resp.ExecutionErr,
		"standby user-timer branch must see virtual expiry as elapsed under accumulated skip and retry until the fired event replicates")
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessActivityTimeout_StandbyFiresUnderSkip() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
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
				WorkflowRunTimeout:  durationpb.New(72 * time.Hour),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
		},
	)
	s.NoError(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.NewString())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	const activityTimeout = 30 * time.Minute
	scheduledEvent, _ := addActivityTaskScheduledEvent(
		mutableState,
		event.GetEventId(),
		"activity", "activity-type", "taskqueue", nil,
		activityTimeout, activityTimeout, activityTimeout, activityTimeout,
	)

	persistenceMutableState := s.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	persistenceMutableState.ExecutionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
		AccumulatedSkippedDuration: durationpb.New(time.Hour),
	}

	timerTask := &tasks.ActivityTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Attempt:             1,
		TaskID:              s.mustGenerateTaskID(),
		TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		VisibilityTimestamp: s.now.Add(-30 * time.Minute),
		EventID:             scheduledEvent.GetEventId(),
	}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	// Same windowing trick as the user-timer test — keep standby time inside the
	// NoOp window so NoOp(&struct{}{}) → ErrTaskRetry rather than discard.
	s.mockShard.SetCurrentTime(s.clusterName, s.now.Add(-29*time.Minute))
	resp := s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskRetry, resp.ExecutionErr,
		"standby activity-timer branch must see virtual deadline as elapsed under accumulated skip — referenceTime must come from mutableState.Now() (virtual)")
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessActivityTimeout_StandbyHeartbeat_DedupUnderSkip() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
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
				WorkflowRunTimeout:  durationpb.New(72 * time.Hour),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
		},
	)
	s.NoError(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.NewString())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	// Heartbeat strictly shorter than the other timeouts so it sorts first in
	// LoadAndSortActivityTimers — paired with TimerTaskStatusCreatedHeartbeat
	// below, this makes CreateNextActivityTimer a no-op (TimerCreated=true),
	// so the dedup branch is the only source of modification.
	const (
		longTimeout         = 24 * time.Hour
		heartbeatTimeoutDur = 12 * time.Hour
	)
	scheduledEvent, _ := addActivityTaskScheduledEvent(
		mutableState,
		event.GetEventId(),
		"activity", "activity-type", "taskqueue", nil,
		longTimeout, longTimeout, longTimeout, heartbeatTimeoutDur,
	)
	addActivityTaskStartedEvent(mutableState, scheduledEvent.GetEventId(), "identity")

	// Flip the heartbeat-created bit so NewMutableStateFromDB populates
	// pendingActivityTimerHeartbeats[id] with the year-2000 sentinel. Also set
	// LastHeartbeatUpdateTime so the timer-sequence treats the heartbeat as a
	// real ongoing tracker.
	activityInfo := mutableState.GetPendingActivityInfos()[scheduledEvent.GetEventId()]
	activityInfo.TimerTaskStatus |= workflow.TimerTaskStatusCreatedHeartbeat
	activityInfo.LastHeartbeatUpdateTime = timestamppb.New(s.now)

	persistenceMutableState := s.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	persistenceMutableState.ExecutionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
		AccumulatedSkippedDuration: durationpb.New(time.Hour),
	}

	heartbeatYear2000 := time.Unix(946684800, 0).UTC()
	timerTask := &tasks.ActivityTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Attempt:             1,
		TaskID:              s.mustGenerateTaskID(),
		TimeoutType:         enumspb.TIMEOUT_TYPE_HEARTBEAT,
		VisibilityTimestamp: heartbeatYear2000.Add(-30 * time.Minute),
		EventID:             scheduledEvent.GetEventId(),
	}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	// Dedup branch must fire → bit cleared → CreateNextActivityTimer regenerates
	// → UpdateWorkflowExecutionAsPassive routes through persistence.UpdateWorkflowExecution.
	// Without ToRealTime the gate would skip and this expectation would go unmet.
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(tests.UpdateWorkflowExecutionResponse, nil)

	s.mockShard.SetCurrentTime(s.clusterName, s.now)
	resp := s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(resp.ExecutionErr,
		"standby heartbeat-dedup gate must fire under accumulated skip via ToRealTime(heartbeatTimeoutVis)")
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessWorkflowTaskTimeout_Pending() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetWorkflowId(), execution.GetRunId())
	_ = mutableState.UpdateCurrentVersion(s.version, false)
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
	s.NoError(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	startedEvent := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.NewString())
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
		TaskID:              s.mustGenerateTaskID(),
		TimeoutType:         enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		VisibilityTimestamp: s.now,
		EventID:             wt.ScheduledEventID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, startedEvent.GetEventId(), startedEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, s.now)
	resp := s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskRetry, resp.ExecutionErr)

	s.mockShard.SetCurrentTime(s.clusterName, s.now.Add(s.fetchHistoryDuration))
	resp = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskRetry, resp.ExecutionErr)

	s.mockShard.SetCurrentTime(s.clusterName, s.now.Add(s.discardDuration))
	resp = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskDiscarded, resp.ExecutionErr)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessWorkflowTaskTimeout_ScheduleToStartTimer() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
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
		TaskID:              s.mustGenerateTaskID(),
		TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		VisibilityTimestamp: s.now,
		EventID:             workflowTaskScheduledEventID,
	}

	s.mockShard.SetCurrentTime(s.clusterName, s.now)
	resp := s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(resp.ExecutionErr)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessWorkflowTaskTimeout_Success() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
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
	s.NoError(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.NewString())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")
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
		TaskID:              s.mustGenerateTaskID(),
		TimeoutType:         enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		VisibilityTimestamp: s.now,
		EventID:             wt.ScheduledEventID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, s.now)
	resp := s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(resp.ExecutionErr)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessWorkflowTaskTimeout_AttemptMismatch() {
	// This test verifies that when a workflow task fails and is rescheduled with a new attempt,
	// the old timer task (with old attempt and event id) is skipped without calling UpdateWorkflowExecution.
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
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
	s.NoError(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.NewString())
	wt.StartedEventID = event.GetEventId()

	// We must manually update the version history here.
	// The logic for scheduling transient workflow task below will use the version history to determine
	// if there's failover and if workflow task attempt needs to be reset.
	vh, err := versionhistory.GetCurrentVersionHistory(mutableState.GetExecutionInfo().VersionHistories)
	s.NoError(err)
	err = versionhistory.AddOrUpdateVersionHistoryItem(vh, versionhistory.NewVersionHistoryItem(
		event.GetEventId(), event.GetVersion(),
	))
	s.NoError(err)

	event, err = mutableState.AddWorkflowTaskFailedEvent(
		wt,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_NON_DETERMINISTIC_ERROR,
		failure.NewServerFailure("some random workflow task failure details", false),
		"some random workflow task failure identity",
		nil,
		"",
		"",
		"",
		0,
	)
	s.NoError(err)

	wt2, err := mutableState.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_TRANSIENT)
	s.NoError(err)
	s.Equal(int32(2), wt2.Attempt)

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
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId()+1, event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, s.now)
	resp := s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(resp.ExecutionErr)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessWorkflowTaskTimeout_StampMismatch() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
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
	s.NoError(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	startedEvent := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.NewString())

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
	s.mockShard.SetCurrentTime(s.clusterName, s.now)

	resp := s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.ErrorIs(resp.ExecutionErr, consts.ErrStaleReference)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessWorkflowBackoffTimer_Pending() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
	}
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
	s.NoError(err)

	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	timerTask := &tasks.WorkflowBackoffTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		TaskID:              s.mustGenerateTaskID(),
		VisibilityTimestamp: s.now,
		WorkflowBackoffType: enumsspb.WORKFLOW_BACKOFF_TYPE_RETRY,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, s.now)
	resp := s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskRetry, resp.ExecutionErr)

	s.mockShard.SetCurrentTime(s.clusterName, time.Now().UTC().Add(s.fetchHistoryDuration))
	resp = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskRetry, resp.ExecutionErr)

	s.mockShard.SetCurrentTime(s.clusterName, time.Now().UTC().Add(s.discardDuration))
	resp = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskDiscarded, resp.ExecutionErr)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessWorkflowBackoffTimer_Success() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
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
	s.NoError(err)

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
		TaskID:              s.mustGenerateTaskID(),
		VisibilityTimestamp: s.now,
		WorkflowBackoffType: enumsspb.WORKFLOW_BACKOFF_TYPE_CRON,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, wt.ScheduledEventID, wt.Version)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, s.now)
	resp := s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(resp.ExecutionErr)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessWorkflowRunTimeout_Pending() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	workflowRunTimeout := 200 * time.Second

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
		},
	)
	s.NoError(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	startEvent := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.NewString())
	wt.StartedEventID = startEvent.GetEventId()
	completionEvent := addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	startTime := mutableState.GetExecutionState().GetStartTime().AsTime()
	timerTask := &tasks.WorkflowRunTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		TaskID:              s.mustGenerateTaskID(),
		VisibilityTimestamp: startTime.Add(workflowRunTimeout),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, completionEvent.GetEventId(), completionEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.timeSource.Update(s.now.Add(-time.Second))
	s.mockShard.SetCurrentTime(s.clusterName, s.now.Add(-time.Second))
	resp := s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskRetry, resp.ExecutionErr)

	s.timeSource.Update(s.now)
	s.mockShard.SetCurrentTime(s.clusterName, s.now.Add(2*workflowRunTimeout))
	resp = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskRetry, resp.ExecutionErr)

	s.timeSource.Update(s.now.Add(s.fetchHistoryDuration))
	s.mockShard.SetCurrentTime(s.clusterName, s.now.Add(s.fetchHistoryDuration))
	resp = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskRetry, resp.ExecutionErr)

	s.timeSource.Update(s.now.Add(s.discardDuration))
	s.mockShard.SetCurrentTime(s.clusterName, s.now.Add(s.discardDuration))
	resp = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskDiscarded, resp.ExecutionErr)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessWorkflowRunTimeout_Success() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
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
	s.NoError(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.NewString())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")
	event = addCompleteWorkflowEvent(mutableState, event.GetEventId(), nil)
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	timerTask := &tasks.WorkflowRunTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		TaskID:              s.mustGenerateTaskID(),
		VisibilityTimestamp: s.now,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, s.now)
	resp := s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(resp.ExecutionErr)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessWorkflowExecutionTimeout_Pending() {
	firstRunID := uuid.NewString()
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	workflowRunTimeout := 200 * time.Second

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetWorkflowId(), execution.GetRunId())
	startedEvent, err := mutableState.AddWorkflowExecutionStartedEventWithOptions(
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
		},
		nil,
		uuid.NewString(),
		firstRunID,
	)
	s.NoError(err)

	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	timerTask := &tasks.WorkflowExecutionTimeoutTask{
		NamespaceID:         s.namespaceID.String(),
		WorkflowID:          execution.GetWorkflowId(),
		FirstRunID:          firstRunID,
		VisibilityTimestamp: s.now.Add(workflowRunTimeout),
		TaskID:              s.mustGenerateTaskID(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, startedEvent.GetEventId(), startedEvent.GetVersion())
	persistenceExecutionState := persistenceMutableState.ExecutionState
	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: s.namespaceID.String(),
		WorkflowID:  execution.GetWorkflowId(),
		ArchetypeID: chasm.WorkflowArchetypeID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		StartRequestID: persistenceExecutionState.CreateRequestId,
		RunID:          persistenceExecutionState.RunId,
		State:          persistenceExecutionState.State,
		Status:         persistenceExecutionState.Status,
	}, nil).Times(4)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.timeSource.Update(s.now.Add(-time.Second))
	s.mockShard.SetCurrentTime(s.clusterName, s.now.Add(-time.Second))
	resp := s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskRetry, resp.ExecutionErr)

	s.timeSource.Update(s.now.Add(2 * workflowRunTimeout))
	s.mockShard.SetCurrentTime(s.clusterName, s.now.Add(2*workflowRunTimeout))
	resp = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskRetry, resp.ExecutionErr)

	s.timeSource.Update(s.now.Add(s.fetchHistoryDuration))
	s.mockShard.SetCurrentTime(s.clusterName, s.now.Add(s.fetchHistoryDuration))
	resp = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskRetry, resp.ExecutionErr)

	s.timeSource.Update(s.now.Add(s.discardDuration))
	s.mockShard.SetCurrentTime(s.clusterName, s.now.Add(s.discardDuration))
	resp = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskDiscarded, resp.ExecutionErr)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessWorkflowExecutionTimeout_Success() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
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
		},
	)
	s.NoError(err)

	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	timerTask := &tasks.WorkflowExecutionTimeoutTask{
		NamespaceID:         s.namespaceID.String(),
		WorkflowID:          execution.GetWorkflowId(),
		FirstRunID:          uuid.NewString(), // does not match the firsrt runID of the execution
		VisibilityTimestamp: s.now,
		TaskID:              s.mustGenerateTaskID(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, startedEvent.GetEventId(), startedEvent.GetVersion())
	persistenceExecutionState := persistenceMutableState.ExecutionState
	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: s.namespaceID.String(),
		WorkflowID:  execution.GetWorkflowId(),
		ArchetypeID: chasm.WorkflowArchetypeID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		StartRequestID: persistenceExecutionState.CreateRequestId,
		RunID:          persistenceExecutionState.RunId,
		State:          persistenceExecutionState.State,
		Status:         persistenceExecutionState.Status,
	}, nil).Times(1)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, s.now)
	resp := s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(resp.ExecutionErr)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessRetryTimeout() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetWorkflowId(), execution.GetRunId())
	startEvent, err := mutableState.AddWorkflowExecutionStartedEvent(
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
	s.NoError(err)
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
		TaskID:              s.mustGenerateTaskID(),
		VisibilityTimestamp: s.now,
		EventID:             int64(16384),
	}

	s.mockShard.SetCurrentTime(s.clusterName, s.now)
	resp := s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(resp.ExecutionErr)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessActivityRetryTimer_Noop() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
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
	s.NoError(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.NewString())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

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
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil).AnyTimes()
	s.mockShard.SetCurrentTime(s.clusterName, s.now)

	timerTask := &tasks.ActivityRetryTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Attempt:             2,
		Version:             s.version,
		TaskID:              s.mustGenerateTaskID(),
		VisibilityTimestamp: task.(*tasks.ActivityTimeoutTask).VisibilityTimestamp,
		EventID:             scheduledEvent.GetEventId(),
	}
	resp := s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(resp.ExecutionErr)

	timerTask = &tasks.ActivityRetryTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Attempt:             2,
		Version:             s.version - 1,
		TaskID:              s.mustGenerateTaskID(),
		VisibilityTimestamp: task.(*tasks.ActivityTimeoutTask).VisibilityTimestamp,
		EventID:             scheduledEvent.GetEventId(),
	}
	resp = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskVersionMismatch, resp.ExecutionErr)

	timerTask = &tasks.ActivityRetryTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Attempt:             0,
		Version:             s.version,
		TaskID:              s.mustGenerateTaskID(),
		VisibilityTimestamp: task.(*tasks.ActivityTimeoutTask).VisibilityTimestamp,
		EventID:             scheduledEvent.GetEventId(),
	}
	resp = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(resp.ExecutionErr)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessActivityRetryTimer_ActivityCompleted() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
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
	s.NoError(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.NewString())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

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
		TaskID:              s.mustGenerateTaskID(),
		VisibilityTimestamp: task.(*tasks.ActivityTimeoutTask).VisibilityTimestamp,
		EventID:             scheduledEvent.GetEventId(),
	}
	resp := s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(resp.ExecutionErr)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestProcessActivityRetryTimer_Pending() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
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
	s.NoError(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.NewString())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	activityID := "activity"
	activityType := "activity type"
	timerTimeout := 2 * time.Second
	scheduledEvent, _ := addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID, activityType, taskQueueName, nil,
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
		TaskID:              s.mustGenerateTaskID(),
		VisibilityTimestamp: task.(*tasks.ActivityTimeoutTask).VisibilityTimestamp,
		EventID:             scheduledEvent.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	// no-op post action
	s.mockShard.SetCurrentTime(s.clusterName, s.now)
	resp := s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskRetry, resp.ExecutionErr)

	// resend history post action
	s.mockShard.SetCurrentTime(s.clusterName, s.now.Add(s.fetchHistoryDuration))

	resp = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.Equal(consts.ErrTaskRetry, resp.ExecutionErr)

	// push to matching post action
	s.mockShard.SetCurrentTime(s.clusterName, s.now.Add(s.discardDuration))
	s.mockMatchingClient.EXPECT().AddActivityTask(
		gomock.Any(),
		&matchingservice.AddActivityTaskRequest{
			NamespaceId: s.namespaceID.String(),
			Execution:   execution,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueueName,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			ScheduledEventId:       scheduledEvent.EventId,
			ScheduleToStartTimeout: durationpb.New(timerTimeout),
			Clock:                  vclock.NewVectorClock(s.mockClusterMetadata.GetClusterID(), s.mockShard.GetShardID(), timerTask.TaskID),
			VersionDirective:       worker_versioning.MakeUseAssignmentRulesDirective(),
		},
		gomock.Any(),
	).Return(&matchingservice.AddActivityTaskResponse{}, nil)

	resp = s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NoError(resp.ExecutionErr)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestExecuteStateMachineTimerTask_ExecutesAllAvailableTimers() {
	reg := s.mockShard.StateMachineRegistry()
	s.NoError(dummy.RegisterStateMachine(reg))
	s.NoError(dummy.RegisterTaskSerializers(reg))

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
	ms.EXPECT().GetNextEventID().Return(int64(2))
	ms.EXPECT().GetExecutionInfo().Return(info).AnyTimes()
	ms.EXPECT().GetWorkflowKey().Return(tests.WorkflowKey).AnyTimes()
	ms.EXPECT().GetExecutionState().Return(
		&persistencespb.WorkflowExecutionState{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING},
	).AnyTimes()
	ms.EXPECT().HSM().Return(root).AnyTimes()
	ms.EXPECT().Now().Return(s.mockShard.GetTimeSource().Now()).AnyTimes()
	ms.EXPECT().ToRealTime(gomock.Any()).DoAndReturn(func(t time.Time) time.Time { return t }).AnyTimes()

	_, err = dummy.MachineCollection(root).Add("dummy", dummy.NewDummy())
	s.NoError(err)

	dummyRoot, err := root.Child([]hsm.Key{
		{Type: dummy.StateMachineType, ID: "dummy"},
	})
	s.NoError(err)
	err = hsm.MachineTransition(dummyRoot, func(sm *dummy.Dummy) (hsm.TransitionOutput, error) {
		return dummy.Transition0.Apply(sm, dummy.Event0{})
	})
	s.NoError(err)
	err = hsm.MachineTransition(dummyRoot, func(sm *dummy.Dummy) (hsm.TransitionOutput, error) {
		return dummy.Transition0.Apply(sm, dummy.Event0{})
	})
	s.NoError(err)

	// Track some tasks.

	// Invalid reference, should be dropped.
	invalidTask := &persistencespb.StateMachineTaskInfo{
		Ref: &persistencespb.StateMachineRef{
			MutableStateVersionedTransition: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 2,
			},
			MachineInitialVersionedTransition: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 1,
			},
		},
		Type: dummy.TaskTypeTimer,
	}
	staleTask := &persistencespb.StateMachineTaskInfo{
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
			MachineLastUpdateVersionedTransition: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 2,
			},
			MachineTransitionCount: 1,
		},
		Type: dummy.TaskTypeTimer,
	}

	// Past deadline, should get executed.
	workflow.TrackStateMachineTimer(ms, s.mockShard.GetTimeSource().Now().Add(-time.Hour), invalidTask)
	workflow.TrackStateMachineTimer(ms, s.mockShard.GetTimeSource().Now().Add(-time.Hour), staleTask)
	workflow.TrackStateMachineTimer(ms, s.mockShard.GetTimeSource().Now().Add(-time.Minute), staleTask)
	// Future deadline, new task should be scheduled.
	futureDeadline := s.mockShard.GetTimeSource().Now().Add(time.Hour)
	workflow.TrackStateMachineTimer(ms, futureDeadline, staleTask)

	wfCtx := historyi.NewMockWorkflowContext(s.controller)
	wfCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(ms, nil)
	wfCtx.EXPECT().UpdateWorkflowExecutionAsPassive(gomock.Any(), gomock.Any())

	mockCache := wcache.NewMockCache(s.controller)
	mockCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(), s.mockShard, tests.NamespaceID, we, chasm.WorkflowArchetypeID, locks.PriorityLow,
	).Return(wfCtx, wcache.NoopReleaseFn, nil)

	task := &tasks.StateMachineTimerTask{
		WorkflowKey: tests.WorkflowKey,
		Version:     2,
	}

	//nolint:revive // unchecked-type-assertion
	timerQueueStandbyTaskExecutor := newTimerQueueStandbyTaskExecutor(
		s.mockShard,
		mockCache,
		s.mockDeleteManager,
		s.mockMatchingClient,
		s.mockChasmEngine,
		s.logger,
		metrics.NoopMetricsHandler,
		s.clusterName,
		s.config,
		s.clientBean,
	).(*timerQueueStandbyTaskExecutor)

	err = timerQueueStandbyTaskExecutor.executeStateMachineTimerTask(context.Background(), task)
	s.NoError(err)
	s.Len(info.StateMachineTimers, 1)
	s.Equal(futureDeadline, info.StateMachineTimers[0].Deadline.AsTime())
}

func (s *timerQueueStandbyTaskExecutorSuite) TestExecuteStateMachineTimerTask_ValidConcurrentTaskIsKept() {
	reg := s.mockShard.StateMachineRegistry()
	s.NoError(dummy.RegisterStateMachine(reg))
	s.NoError(dummy.RegisterTaskSerializers(reg))

	we := &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}

	ms := historyi.NewMockMutableState(s.controller)
	info := &persistencespb.WorkflowExecutionInfo{
		VersionHistories: &historyspb.VersionHistories{
			CurrentVersionHistoryIndex: 0,
			Histories: []*historyspb.VersionHistory{
				{
					Items: []*historyspb.VersionHistoryItem{
						{EventId: 1, Version: 2},
					},
				},
			},
		},
	}

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
	ms.EXPECT().GetNextEventID().Return(int64(2))
	ms.EXPECT().GetExecutionInfo().Return(info).AnyTimes()
	ms.EXPECT().GetWorkflowKey().Return(tests.WorkflowKey).AnyTimes()
	ms.EXPECT().GetExecutionState().Return(
		&persistencespb.WorkflowExecutionState{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING},
	).AnyTimes()
	ms.EXPECT().HSM().Return(root).AnyTimes()
	ms.EXPECT().Now().Return(s.mockShard.GetTimeSource().Now()).AnyTimes()
	ms.EXPECT().ToRealTime(gomock.Any()).DoAndReturn(func(t time.Time) time.Time { return t }).AnyTimes()

	_, err = dummy.MachineCollection(root).Add("dummy", dummy.NewDummy())
	s.NoError(err)

	dummyRoot, err := root.Child([]hsm.Key{
		{Type: dummy.StateMachineType, ID: "dummy"},
	})
	s.NoError(err)
	err = hsm.MachineTransition(dummyRoot, func(sm *dummy.Dummy) (hsm.TransitionOutput, error) {
		return dummy.Transition0.Apply(sm, dummy.Event0{})
	})
	s.NoError(err)
	err = hsm.MachineTransition(dummyRoot, func(sm *dummy.Dummy) (hsm.TransitionOutput, error) {
		return dummy.Transition0.Apply(sm, dummy.Event0{})
	})
	s.NoError(err)

	// Track a task with a past deadline. Should get executed.
	workflow.TrackStateMachineTimer(ms, s.mockShard.GetTimeSource().Now().Add(-time.Hour), &persistencespb.StateMachineTaskInfo{
		Ref: &persistencespb.StateMachineRef{
			MutableStateVersionedTransition: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 2,
			},
			MachineInitialVersionedTransition: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 0,
			},
			MachineLastUpdateVersionedTransition: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 2,
			},
		},
		Type: dummy.TaskTypeTimer,
	})

	wfCtx := historyi.NewMockWorkflowContext(s.controller)
	wfCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(ms, nil)

	mockCache := wcache.NewMockCache(s.controller)
	mockCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(), s.mockShard, tests.NamespaceID, we, chasm.WorkflowArchetypeID, locks.PriorityLow,
	).Return(wfCtx, wcache.NoopReleaseFn, nil)

	task := &tasks.StateMachineTimerTask{
		WorkflowKey: tests.WorkflowKey,
		Version:     2,
	}

	//nolint:revive // unchecked-type-assertion
	timerQueueStandbyTaskExecutor := newTimerQueueStandbyTaskExecutor(
		s.mockShard,
		mockCache,
		s.mockDeleteManager,
		s.mockMatchingClient,
		s.mockChasmEngine,
		s.logger,
		metrics.NoopMetricsHandler,
		s.clusterName,
		s.config,
		s.clientBean,
	).(*timerQueueStandbyTaskExecutor)

	err = timerQueueStandbyTaskExecutor.executeStateMachineTimerTask(context.Background(), task)
	s.ErrorIs(err, consts.ErrTaskRetry)
	s.Len(info.StateMachineTimers, 1)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestExecuteStateMachineTimerTask_StaleStateMachine() {
	reg := s.mockShard.StateMachineRegistry()
	s.NoError(dummy.RegisterStateMachine(reg))
	s.NoError(dummy.RegisterTaskSerializers(reg))

	we := &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}

	ms := historyi.NewMockMutableState(s.controller)
	info := &persistencespb.WorkflowExecutionInfo{
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 2, TransitionCount: 2},
		},
		VersionHistories: &historyspb.VersionHistories{
			CurrentVersionHistoryIndex: 0,
			Histories: []*historyspb.VersionHistory{
				{
					Items: []*historyspb.VersionHistoryItem{
						{EventId: 1, Version: 2},
					},
				},
			},
		},
	}
	root, err := hsm.NewRoot(
		reg,
		workflow.StateMachineType,
		ms,
		make(map[string]*persistencespb.StateMachineMap),
		ms,
	)
	s.NoError(err)

	ms.EXPECT().GetCurrentVersion().Return(int64(1)).AnyTimes()
	ms.EXPECT().NextTransitionCount().Return(int64(0)).AnyTimes()
	ms.EXPECT().GetNextEventID().Return(int64(2))
	ms.EXPECT().GetExecutionInfo().Return(info).AnyTimes()
	ms.EXPECT().GetWorkflowKey().Return(tests.WorkflowKey).AnyTimes()
	ms.EXPECT().GetExecutionState().Return(
		&persistencespb.WorkflowExecutionState{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING},
	).AnyTimes()
	ms.EXPECT().HSM().Return(root).AnyTimes()
	ms.EXPECT().Now().Return(s.mockShard.GetTimeSource().Now()).AnyTimes()
	ms.EXPECT().ToRealTime(gomock.Any()).DoAndReturn(func(t time.Time) time.Time { return t }).AnyTimes()

	_, err = dummy.MachineCollection(root).Add("dummy", dummy.NewDummy())
	s.NoError(err)

	// Track some tasks.

	validTask := &persistencespb.StateMachineTaskInfo{
		Ref: &persistencespb.StateMachineRef{
			Path: []*persistencespb.StateMachineKey{
				{Type: dummy.StateMachineType, Id: "dummy"},
			},
			MutableStateVersionedTransition: nil,
			MachineInitialVersionedTransition: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 1,
				TransitionCount:          0,
			},
			MachineLastUpdateVersionedTransition: nil,
			MachineTransitionCount:               0,
		},
		Type: dummy.TaskTypeTimer,
	}

	// Past deadline, still valid task
	workflow.TrackStateMachineTimer(ms, s.mockShard.GetTimeSource().Now().Add(-time.Hour), validTask)
	// Future deadline, new task should be scheduled.
	workflow.TrackStateMachineTimer(ms, s.mockShard.GetTimeSource().Now().Add(time.Hour), validTask)

	wfCtx := historyi.NewMockWorkflowContext(s.controller)
	wfCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(ms, nil)

	mockCache := wcache.NewMockCache(s.controller)
	mockCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(), s.mockShard, tests.NamespaceID, we, chasm.WorkflowArchetypeID, locks.PriorityLow,
	).Return(wfCtx, wcache.NoopReleaseFn, nil)

	task := &tasks.StateMachineTimerTask{
		WorkflowKey: tests.WorkflowKey,
		Version:     2,
	}

	//nolint:revive // unchecked-type-assertion
	timerQueueStandbyTaskExecutor := newTimerQueueStandbyTaskExecutor(
		s.mockShard,
		mockCache,
		s.mockDeleteManager,
		s.mockMatchingClient,
		s.mockChasmEngine,
		s.logger,
		metrics.NoopMetricsHandler,
		s.clusterName,
		s.config,
		s.clientBean,
	).(*timerQueueStandbyTaskExecutor)

	err = timerQueueStandbyTaskExecutor.executeStateMachineTimerTask(context.Background(), task)
	s.ErrorIs(err, consts.ErrTaskRetry)
	s.Len(info.StateMachineTimers, 2)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestExecuteStateMachineTimerTask_ZombieWorkflow() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
	}

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetWorkflowId(), execution.GetRunId())
	startedEvent, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId:  s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{},
		},
	)
	s.NoError(err)
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

	resp := s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.ErrorIs(resp.ExecutionErr, consts.ErrWorkflowZombie)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestExecuteChasmSideEffectTimerTask_ExecutesTask() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowKey.WorkflowID,
		RunId:      tests.WorkflowKey.RunID,
	}

	// Mock the CHASM tree.
	chasmTree := historyi.NewMockChasmTree(s.controller)
	expectValidate := func(isTaskInTree bool, isValidByComponent bool, err error) {
		chasmTree.EXPECT().ValidateSideEffectTask(
			gomock.Any(),
			gomock.Any(),
		).Times(1).Return(isTaskInTree, isValidByComponent, err)
	}

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
			ArchetypeId: tests.ArchetypeID,
		},
	}

	wfCtx := historyi.NewMockWorkflowContext(s.controller)
	wfCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(ms, nil).AnyTimes()

	mockCache := wcache.NewMockCache(s.controller)
	mockCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(), s.mockShard, gomock.Any(), execution, tests.ArchetypeID, locks.PriorityLow,
	).Return(wfCtx, wcache.NoopReleaseFn, nil).AnyTimes()

	//nolint:revive // unchecked-type-assertion
	timerQueueStandbyTaskExecutor := newTimerQueueStandbyTaskExecutor(
		s.mockShard,
		mockCache,
		s.mockDeleteManager,
		s.mockMatchingClient,
		s.mockChasmEngine,
		s.logger,
		metrics.NoopMetricsHandler,
		s.clusterName,
		s.config,
		s.clientBean,
	).(*timerQueueStandbyTaskExecutor)

	// Task in tree and valid by component — retry.
	expectValidate(true, true, nil)
	resp := timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NotNil(resp)
	s.ErrorIs(consts.ErrTaskRetry, resp.ExecutionErr)

	// Task in tree but component says invalid (e.g. code-deployment) — still retry.
	expectValidate(true, false, nil)
	resp = timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NotNil(resp)
	s.ErrorIs(consts.ErrTaskRetry, resp.ExecutionErr)

	// Task not in tree — replication removed it, drop the physical task.
	expectValidate(false, false, nil)
	resp = timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NotNil(resp)
	s.NoError(resp.ExecutionErr)

	// Validation error — propagate.
	expectedErr := errors.New("validation error")
	expectValidate(false, false, expectedErr)
	resp = timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NotNil(resp)
	s.ErrorIs(expectedErr, resp.ExecutionErr)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestExecuteChasmPureTimerTask_ValidatesAllPureTimers() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowKey.WorkflowID,
		RunId:      tests.WorkflowKey.RunID,
	}

	// Mock the CHASM tree and execute interface.
	chasmTree := historyi.NewMockChasmTree(s.controller)
	expectEachPureTask := func(err error) {
		chasmTree.EXPECT().EachPureTask(gomock.Any(), gomock.Any()).
			Times(1).Return(err)
	}

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
	ms.EXPECT().Now().Return(s.mockShard.GetTimeSource().Now()).AnyTimes()

	// Add a valid timer task.
	timerTask := &tasks.ChasmTaskPure{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		VisibilityTimestamp: s.now,
		TaskID:              s.mustGenerateTaskID(),
		ArchetypeID:         tests.ArchetypeID,
	}

	wfCtx := historyi.NewMockWorkflowContext(s.controller)
	wfCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(ms, nil).AnyTimes()

	mockCache := wcache.NewMockCache(s.controller)
	mockCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(), s.mockShard, gomock.Any(), execution, tests.ArchetypeID, locks.PriorityLow,
	).Return(wfCtx, wcache.NoopReleaseFn, nil).AnyTimes()

	//nolint:revive // unchecked-type-assertion
	timerQueueStandbyTaskExecutor := newTimerQueueStandbyTaskExecutor(
		s.mockShard,
		mockCache,
		s.mockDeleteManager,
		s.mockMatchingClient,
		s.mockChasmEngine,
		s.logger,
		metrics.NoopMetricsHandler,
		s.clusterName,
		s.config,
		s.clientBean,
	).(*timerQueueStandbyTaskExecutor)

	// No tasks found — EachPureTask completed without invoking the callback.
	expectEachPureTask(nil)
	resp := timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NotNil(resp)
	s.NoError(resp.ExecutionErr)

	// Tasks should retry.
	expectEachPureTask(consts.ErrTaskRetry)
	resp = timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NotNil(resp)
	s.ErrorIs(consts.ErrTaskRetry, resp.ExecutionErr)

	// Validation failed.
	expectedErr := errors.New("validation error")
	expectEachPureTask(expectedErr)
	resp = timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
	s.NotNil(resp)
	s.ErrorIs(expectedErr, resp.ExecutionErr)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestExecuteTimeSkippingTimerTask() {
	// makeTimeSkippingMS builds a running mutable state, snapshots it to a persistence proto,
	// and returns the persistence proto plus the workflow key. The caller can mutate the returned
	// ExecutionInfo (e.g. set TimeSkippingInfo) before programming GetWorkflowExecution.
	makeTimeSkippingMS := func() (*persistencespb.WorkflowMutableState, definition.WorkflowKey) {
		execution := &commonpb.WorkflowExecution{
			WorkflowId: "ts-bound-wf-" + uuid.NewString(),
			RunId:      uuid.NewString(),
		}
		workflowKey := definition.NewWorkflowKey(s.namespaceID.String(), execution.GetWorkflowId(), execution.GetRunId())

		mutableState := workflow.TestGlobalMutableState(
			s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetWorkflowId(), execution.GetRunId())
		event, err := mutableState.AddWorkflowExecutionStartedEvent(
			execution,
			&historyservice.StartWorkflowExecutionRequest{
				Attempt:     1,
				NamespaceId: s.namespaceID.String(),
				StartRequest: &workflowservice.StartWorkflowExecutionRequest{
					WorkflowType:        &commonpb.WorkflowType{Name: "test-wf-type"},
					TaskQueue:           &taskqueuepb.TaskQueue{Name: "test-tq"},
					WorkflowRunTimeout:  durationpb.New(200 * time.Second),
					WorkflowTaskTimeout: durationpb.New(1 * time.Second),
				},
			},
		)
		s.NoError(err)

		pms := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
		return pms, workflowKey
	}

	// makeTimeSkippingPendingMS builds an MS that puts the standby's action function on
	// the "still waiting" path: fast-forward matches the task's source event and HasReached=false.
	makeTimeSkippingPendingMS := func() (*persistencespb.WorkflowMutableState, definition.WorkflowKey) {
		pms, workflowKey := makeTimeSkippingMS()
		pms.ExecutionInfo.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{
			Config: &commonpb.TimeSkippingConfig{
				Enabled:     true,
				FastForward: durationpb.New(time.Hour),
			},
			FastForwardInfo: &persistencespb.FastForwardInfo{
				TargetTime:    timestamppb.New(s.now.Add(time.Hour)),
				SourceEventId: 1,
			},
		}
		return pms, workflowKey
	}

	s.Run("Wait", func() {
		pms, workflowKey := makeTimeSkippingPendingMS()

		timerTask := &tasks.TimeSkippingTimerTask{
			WorkflowKey:         workflowKey,
			TaskID:              s.mustGenerateTaskID(),
			VisibilityTimestamp: s.now,
			EventID:             1,
		}
		s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).
			Return(&persistence.GetWorkflowExecutionResponse{State: pms}, nil)

		s.mockShard.SetCurrentTime(s.clusterName, s.now)
		resp := s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
		s.Equal(consts.ErrTaskRetry, resp.ExecutionErr)
	})

	s.Run("Ack", func() {
		// HasReached=true: active side already replicated the disable transition,
		// so the standby's action function returns nil and the task is acked.
		pms, workflowKey := makeTimeSkippingPendingMS()
		pms.ExecutionInfo.TimeSkippingInfo.FastForwardInfo.HasReached = true

		timerTask := &tasks.TimeSkippingTimerTask{
			WorkflowKey:         workflowKey,
			TaskID:              s.mustGenerateTaskID(),
			VisibilityTimestamp: s.now.Add(time.Hour),
			EventID:             1,
		}
		s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).
			Return(&persistence.GetWorkflowExecutionResponse{State: pms}, nil)

		s.mockShard.SetCurrentTime(s.clusterName, s.now)
		resp := s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
		s.NoError(resp.ExecutionErr)
	})

	s.Run("Discard", func() {
		pms, workflowKey := makeTimeSkippingPendingMS()

		timerTask := &tasks.TimeSkippingTimerTask{
			WorkflowKey:         workflowKey,
			TaskID:              s.mustGenerateTaskID(),
			VisibilityTimestamp: s.now,
			EventID:             1,
		}
		s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).
			Return(&persistence.GetWorkflowExecutionResponse{State: pms}, nil)

		// Past VisibilityTime + discardDelay: ErrTaskDiscarded.
		s.mockShard.SetCurrentTime(s.clusterName, s.now.Add(s.discardDuration))
		resp := s.timerQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(timerTask))
		s.Equal(consts.ErrTaskDiscarded, resp.ExecutionErr)
	})
}

func (s *timerQueueStandbyTaskExecutorSuite) createPersistenceMutableState(
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
	return workflow.TestCloneToProto(context.Background(), ms)
}

func (s *timerQueueStandbyTaskExecutorSuite) newTaskExecutable(
	task tasks.Task,
) queues.Executable {
	return queues.NewExecutable(
		queues.DefaultReaderId,
		task,
		s.timerQueueStandbyTaskExecutor,
		nil,
		nil,
		queues.NewNoopPriorityAssigner(),
		s.mockShard.GetTimeSource(),
		s.mockNamespaceCache,
		s.mockClusterMetadata,
		s.mockShard.ChasmRegistry(),
		queues.GetTaskTypeTagValue,
		nil,
		metrics.NoopMetricsHandler,
		telemetry.NoopTracer,
	)
}

func (s *timerQueueStandbyTaskExecutorSuite) TestExecuteChasmSideEffectTimerTask_Discard() {
	setupDiscard := func(lib chasm.Library, taskName string, treeMockFn func(*historyi.MockChasmTree)) (*timerQueueStandbyTaskExecutor, *tasks.ChasmTask) {
		execution := &commonpb.WorkflowExecution{
			WorkflowId: tests.WorkflowKey.WorkflowID,
			RunId:      tests.WorkflowKey.RunID,
		}

		registry := chasm.NewRegistry(s.logger)
		s.NoError(registry.Register(lib))
		s.mockShard.SetChasmRegistry(registry)
		typeID := chasm.GenerateTypeID(chasm.FullyQualifiedName(lib.Name(), taskName))

		chasmTree := historyi.NewMockChasmTree(s.controller)
		chasmTree.EXPECT().ValidateSideEffectTask(gomock.Any(), gomock.Any()).Return(true, true, nil).Times(1)
		treeMockFn(chasmTree)

		ms := historyi.NewMockMutableState(s.controller)
		ms.EXPECT().GetCurrentVersion().Return(int64(2)).AnyTimes()
		ms.EXPECT().NextTransitionCount().Return(int64(0)).AnyTimes()
		ms.EXPECT().GetNextEventID().Return(int64(2)).AnyTimes()
		ms.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{}).AnyTimes()
		ms.EXPECT().GetWorkflowKey().Return(tests.WorkflowKey).AnyTimes()
		ms.EXPECT().GetExecutionState().Return(
			&persistencespb.WorkflowExecutionState{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING},
		).AnyTimes()
		ms.EXPECT().ChasmTree().Return(chasmTree).AnyTimes()

		timerTask := &tasks.ChasmTask{
			WorkflowKey: definition.NewWorkflowKey(
				s.namespaceID.String(),
				execution.GetWorkflowId(),
				execution.GetRunId(),
			),
			VisibilityTimestamp: s.now,
			TaskID:              s.mustGenerateTaskID(),
			Info: &persistencespb.ChasmTaskInfo{
				ArchetypeId: tests.ArchetypeID,
				TypeId:      typeID,
			},
		}

		wfCtx := historyi.NewMockWorkflowContext(s.controller)
		wfCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(ms, nil).AnyTimes()

		mockCache := wcache.NewMockCache(s.controller)
		mockCache.EXPECT().GetOrCreateChasmExecution(
			gomock.Any(), s.mockShard, gomock.Any(), execution, tests.ArchetypeID, locks.PriorityLow,
		).Return(wfCtx, wcache.NoopReleaseFn, nil).AnyTimes()

		//nolint:revive // unchecked-type-assertion
		executor := newTimerQueueStandbyTaskExecutor(
			s.mockShard,
			mockCache,
			s.mockDeleteManager,
			s.mockMatchingClient,
			s.mockChasmEngine,
			s.logger,
			metrics.NoopMetricsHandler,
			s.clusterName,
			s.config,
			s.clientBean,
		).(*timerQueueStandbyTaskExecutor)

		// Advance the standby cluster's time past the CHASM discard delay.
		s.mockShard.SetCurrentTime(s.clusterName, s.now.Add(s.chasmDiscardDuration+time.Second))

		return executor, timerTask
	}

	s.Run("WithHandler", func() {
		executor, task := setupDiscard(&discardableTaskTestLibrary{}, "discard_task", func(tree *historyi.MockChasmTree) {
			tree.EXPECT().ExecuteSideEffectDiscardTask(
				gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
			).Return(nil).Times(1)
		})
		resp := executor.Execute(context.Background(), s.newTaskExecutable(task))
		s.NotNil(resp)
		s.NoError(resp.ExecutionErr)
	})

	s.Run("WithoutHandler", func() {
		executor, task := setupDiscard(&nonDiscardableTaskTestLibrary{}, "non_discard_task", func(tree *historyi.MockChasmTree) {
			tree.EXPECT().ExecuteSideEffectDiscardTask(
				gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
			).Return(chasm.ErrTaskDiscarded).Times(1)
		})
		resp := executor.Execute(context.Background(), s.newTaskExecutable(task))
		s.NotNil(resp)
		s.ErrorIs(resp.ExecutionErr, consts.ErrTaskDiscarded)
	})
}

func (s *timerQueueStandbyTaskExecutorSuite) mustGenerateTaskID() int64 {
	taskID, err := s.mockShard.GenerateTaskID()
	s.NoError(err)
	return taskID
}
