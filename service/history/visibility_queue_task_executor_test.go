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
	persistencespb "go.temporal.io/server/api/persistence/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/protomock"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	visibilityQueueTaskExecutorSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller
		mockShard  *shard.ContextTest

		mockVisibilityMgr *manager.MockVisibilityManager
		mockExecutionMgr  *persistence.MockExecutionManager

		workflowCache               wcache.Cache
		logger                      log.Logger
		namespaceID                 namespace.ID
		namespace                   namespace.Name
		version                     int64
		now                         time.Time
		timeSource                  *clock.EventTimeSource
		visibilityQueueTaskExecutor queues.Executor

		enableCloseWorkflowCleanup bool
	}
)

func TestVisibilityQueueTaskExecutorSuite(t *testing.T) {
	s := new(visibilityQueueTaskExecutorSuite)
	suite.Run(t, s)
}

func (s *visibilityQueueTaskExecutorSuite) SetupSuite() {
}

func (s *visibilityQueueTaskExecutorSuite) TearDownSuite() {
}

func (s *visibilityQueueTaskExecutorSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.namespaceID = tests.NamespaceID
	s.namespace = tests.Namespace
	s.version = tests.GlobalNamespaceEntry.FailoverVersion()
	s.now = time.Now().UTC()
	s.timeSource = clock.NewEventTimeSource().Update(s.now)

	s.controller = gomock.NewController(s.T())

	config := tests.NewDynamicConfig()
	config.EnableChasm = dynamicconfig.GetBoolPropertyFn(true)
	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 1,
			RangeId: 1,
		},
		config,
	)

	reg := hsm.NewRegistry()
	err := workflow.RegisterStateMachine(reg)
	s.NoError(err)
	s.mockShard.SetStateMachineRegistry(reg)

	chasmRegistry := s.mockShard.ChasmRegistry()
	err = chasmRegistry.Register(&chasm.CoreLibrary{})
	s.NoError(err)

	s.mockShard.SetEventsCacheForTesting(events.NewHostLevelEventsCache(
		s.mockShard.GetExecutionManager(),
		s.mockShard.GetConfig(),
		s.mockShard.GetMetricsHandler(),
		s.mockShard.GetLogger(),
		false,
	))
	s.mockShard.Resource.TimeSource = s.timeSource

	s.mockExecutionMgr = s.mockShard.Resource.ExecutionMgr
	s.mockVisibilityMgr = manager.NewMockVisibilityManager(s.controller)

	mockNamespaceCache := s.mockShard.Resource.NamespaceCache
	mockNamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	mockNamespaceCache.EXPECT().GetNamespace(tests.Namespace).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	mockNamespaceCache.EXPECT().GetNamespaceName(tests.NamespaceID).Return(tests.Namespace, nil).AnyTimes()

	mockClusterMetadata := s.mockShard.Resource.ClusterMetadata
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	mockClusterMetadata.EXPECT().GetClusterID().Return(tests.Version).AnyTimes()
	mockClusterMetadata.EXPECT().IsVersionFromSameCluster(tests.Version, tests.Version).Return(true).AnyTimes()
	mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, s.version).Return(mockClusterMetadata.GetCurrentClusterName()).AnyTimes()

	s.workflowCache = wcache.NewHostLevelCache(s.mockShard.GetConfig(), s.mockShard.GetLogger(), metrics.NoopMetricsHandler)
	s.logger = s.mockShard.GetLogger()

	h := &historyEngineImpl{
		currentClusterName: s.mockShard.Resource.GetClusterMetadata().GetCurrentClusterName(),
		shardContext:       s.mockShard,
		clusterMetadata:    mockClusterMetadata,
		executionManager:   s.mockExecutionMgr,
		logger:             s.logger,
		tokenSerializer:    tasktoken.NewSerializer(),
		metricsHandler:     s.mockShard.GetMetricsHandler(),
		eventNotifier:      events.NewNotifier(clock.NewRealTimeSource(), metrics.NoopMetricsHandler, func(namespace.ID, string) int32 { return 1 }),
	}
	s.mockShard.SetEngineForTesting(h)

	s.enableCloseWorkflowCleanup = false
	s.visibilityQueueTaskExecutor = newVisibilityQueueTaskExecutor(
		s.mockShard,
		s.workflowCache,
		s.mockVisibilityMgr,
		s.logger,
		metrics.NoopMetricsHandler,
		config.VisibilityProcessorEnsureCloseBeforeDelete,
		func(_ string) bool { return s.enableCloseWorkflowCleanup },
		config.VisibilityProcessorRelocateAttributesMinBlobSize,
	)
}

func (s *visibilityQueueTaskExecutorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *visibilityQueueTaskExecutorSuite) TestProcessCloseExecution() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	parentNamespaceID := "some random parent namespace ID"
	parentInitiatedID := int64(3222)
	parentInitiatedVersion := int64(1234)
	parentNamespace := "some random parent namespace Name"
	parentExecution := &commonpb.WorkflowExecution{
		WorkflowId: "some random parent workflow ID",
		RunId:      uuid.New(),
	}
	rootExecution := &commonpb.WorkflowExecution{
		WorkflowId: "some random root workflow ID",
		RunId:      uuid.New(),
	}

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
			ParentExecutionInfo: &workflowspb.ParentExecutionInfo{
				NamespaceId:      parentNamespaceID,
				Namespace:        parentNamespace,
				Execution:        parentExecution,
				InitiatedId:      parentInitiatedID,
				InitiatedVersion: parentInitiatedVersion,
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := int64(59)
	event = addCompleteWorkflowEvent(mutableState, event.GetEventId(), nil)

	visibilityTask := &tasks.CloseExecutionVisibilityTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		VisibilityTimestamp: time.Now().UTC(),
		Version:             s.version,
		TaskID:              taskID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockVisibilityMgr.EXPECT().RecordWorkflowExecutionClosed(
		gomock.Any(),
		s.createRecordWorkflowExecutionClosedRequest(
			s.namespace,
			visibilityTask,
			mutableState,
			taskQueueName,
			parentExecution,
			rootExecution,
			map[string]any{
				searchattribute.BuildIds: []string{worker_versioning.UnversionedSearchAttribute},
			},
		),
	).Return(nil)

	resp := s.visibilityQueueTaskExecutor.Execute(context.Background(), s.newTaskExecutable(visibilityTask))
	s.Nil(resp.ExecutionErr)
}

func (s *visibilityQueueTaskExecutorSuite) TestProcessCloseExecutionWithWorkflowClosedCleanup() {
	s.enableCloseWorkflowCleanup = true

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	parentNamespaceID := "some random parent namespace ID"
	parentInitiatedID := int64(3222)
	parentInitiatedVersion := int64(1234)
	parentNamespace := "some random parent namespace Name"
	parentExecution := &commonpb.WorkflowExecution{
		WorkflowId: "some random parent workflow ID",
		RunId:      uuid.New(),
	}
	rootExecution := &commonpb.WorkflowExecution{
		WorkflowId: "some random root workflow ID",
		RunId:      uuid.New(),
	}

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
			ParentExecutionInfo: &workflowspb.ParentExecutionInfo{
				NamespaceId:      parentNamespaceID,
				Namespace:        parentNamespace,
				Execution:        parentExecution,
				InitiatedId:      parentInitiatedID,
				InitiatedVersion: parentInitiatedVersion,
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := int64(59)
	event = addCompleteWorkflowEvent(mutableState, event.GetEventId(), nil)

	visibilityTask := &tasks.CloseExecutionVisibilityTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		VisibilityTimestamp: time.Now().UTC(),
		Version:             s.version,
		TaskID:              taskID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)
	s.mockVisibilityMgr.EXPECT().RecordWorkflowExecutionClosed(
		gomock.Any(),
		s.createRecordWorkflowExecutionClosedRequest(
			s.namespace,
			visibilityTask,
			mutableState,
			taskQueueName,
			parentExecution,
			rootExecution,
			map[string]any{
				searchattribute.BuildIds: []string{worker_versioning.UnversionedSearchAttribute},
			},
		),
	).Return(nil)

	resp := s.visibilityQueueTaskExecutor.Execute(context.Background(), s.newTaskExecutable(visibilityTask))
	s.Nil(resp.ExecutionErr)
}

func (s *visibilityQueueTaskExecutorSuite) TestProcessRecordWorkflowStartedTask() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"
	cronSchedule := "@every 5s"
	backoff := 5 * time.Second

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetWorkflowId(), execution.GetRunId())

	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
				CronSchedule:             cronSchedule,
			},
			FirstWorkflowTaskBackoff: durationpb.New(backoff),
		},
	)
	s.Nil(err)

	taskID := int64(59)
	wt := addWorkflowTaskScheduledEvent(mutableState)

	visibilityTask := &tasks.StartExecutionVisibilityTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		VisibilityTimestamp: time.Now().UTC(),
		Version:             s.version,
		TaskID:              taskID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, wt.ScheduledEventID, wt.Version)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockVisibilityMgr.EXPECT().RecordWorkflowExecutionStarted(
		gomock.Any(),
		s.createRecordWorkflowExecutionStartedRequest(s.namespace, visibilityTask, mutableState, taskQueueName),
	).Return(nil)

	resp := s.visibilityQueueTaskExecutor.Execute(context.Background(), s.newTaskExecutable(visibilityTask))
	s.Nil(resp.ExecutionErr)
}

func (s *visibilityQueueTaskExecutorSuite) TestProcessUpsertWorkflowSearchAttributes() {
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
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	s.NoError(err)

	taskID := int64(59)
	wt := addWorkflowTaskScheduledEvent(mutableState)

	visibilityTask := &tasks.UpsertExecutionVisibilityTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		TaskID: taskID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, wt.ScheduledEventID, wt.Version)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockVisibilityMgr.EXPECT().UpsertWorkflowExecution(
		gomock.Any(),
		s.createUpsertWorkflowRequest(s.namespace, visibilityTask, mutableState, taskQueueName),
	).Return(nil)

	resp := s.visibilityQueueTaskExecutor.Execute(context.Background(), s.newTaskExecutable(visibilityTask))
	s.Nil(resp.ExecutionErr)
}

func (s *visibilityQueueTaskExecutorSuite) TestProcessModifyWorkflowProperties() {
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
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	s.NoError(err)

	taskID := int64(59)
	wt := addWorkflowTaskScheduledEvent(mutableState)

	visibilityTask := &tasks.UpsertExecutionVisibilityTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		TaskID: taskID,
	}

	persistenceMutableState := s.createPersistenceMutableState(
		mutableState,
		wt.ScheduledEventID,
		wt.Version,
	)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(
		gomock.Any(),
		gomock.Any(),
	).Return(
		&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState},
		nil,
	)
	s.mockVisibilityMgr.EXPECT().UpsertWorkflowExecution(
		gomock.Any(),
		s.createUpsertWorkflowRequest(s.namespace, visibilityTask, mutableState, taskQueueName),
	).Return(nil)

	resp := s.visibilityQueueTaskExecutor.Execute(
		context.Background(),
		s.newTaskExecutable(visibilityTask),
	)
	s.Nil(resp.ExecutionErr)
}

func (s *visibilityQueueTaskExecutorSuite) TestProcessDeleteExecution() {
	s.T().SkipNow()
	workflowKey := definition.WorkflowKey{
		NamespaceID: s.namespaceID.String(),
	}
	s.Run("TaskID=0", func() {
		s.mockVisibilityMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any())
		err := s.execute(&tasks.DeleteExecutionVisibilityTask{
			WorkflowKey:                    workflowKey,
			CloseExecutionVisibilityTaskID: 0,
		})
		s.Assert().NoError(err)
	})
	s.Run("WorkflowCloseTime=1970-01-01T00:00:00Z", func() {
		s.mockVisibilityMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any())
		err := s.execute(&tasks.DeleteExecutionVisibilityTask{
			WorkflowKey: workflowKey,
			CloseTime:   time.Unix(0, 0).UTC(),
		})
		s.Assert().NoError(err)
	})
	s.Run("MultiCursorQueue", func() {
		const highWatermark int64 = 5
		s.NoError(s.mockShard.SetQueueState(tasks.CategoryVisibility, 1, &persistencespb.QueueState{
			ReaderStates: nil,
			ExclusiveReaderHighWatermark: &persistencespb.TaskKey{
				TaskId:   highWatermark,
				FireTime: timestamppb.New(tasks.DefaultFireTime),
			},
		}))
		s.Run("NotAcked", func() {
			err := s.execute(&tasks.DeleteExecutionVisibilityTask{
				WorkflowKey:                    workflowKey,
				CloseExecutionVisibilityTaskID: highWatermark + 1,
			})
			s.ErrorIs(err, consts.ErrDependencyTaskNotCompleted)
		})
		s.Run("Acked", func() {
			s.mockVisibilityMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any())
			err := s.execute(&tasks.DeleteExecutionVisibilityTask{
				WorkflowKey:                    workflowKey,
				CloseExecutionVisibilityTaskID: highWatermark - 1,
			})
			s.NoError(err)
		})
	})
}

func (s *visibilityQueueTaskExecutorSuite) TestProcessChasmTask_InvalidTask() {
	key := definition.NewWorkflowKey(
		s.namespaceID.String(),
		"some random ID",
		uuid.New(),
	)
	mutableState := s.buildChasmMutableState(key, "archetype", 5)

	// Case 1: invalid task with lower transition count than the state
	visibilityTask := s.buildChasmVisTask(key, 3)

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: mutableState}, nil)

	resp := s.visibilityQueueTaskExecutor.Execute(context.Background(), s.newTaskExecutable(visibilityTask))
	s.Nil(resp.ExecutionErr)

	// Case 2: invalid task with a different initial versioned transition
	componentInitVT := mutableState.ChasmNodes["Visibility"].Metadata.InitialVersionedTransition
	visibilityTask = s.buildChasmVisTask(key, 5)
	visibilityTask.Info.ComponentInitialVersionedTransition = &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: componentInitVT.NamespaceFailoverVersion + 101,
		TransitionCount:          componentInitVT.TransitionCount,
	}

	resp = s.visibilityQueueTaskExecutor.Execute(context.Background(), s.newTaskExecutable(visibilityTask))
	s.Nil(resp.ExecutionErr)
}

func (s *visibilityQueueTaskExecutorSuite) TestProcessChasmTask_RunningExecution() {
	key := definition.NewWorkflowKey(
		s.namespaceID.String(),
		"some random ID",
		uuid.New(),
	)
	archetype := "archetype"
	mutableState := s.buildChasmMutableState(key, archetype, 5)

	visibilityTask := s.buildChasmVisTask(key, 5)

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: mutableState}, nil)
	s.mockVisibilityMgr.EXPECT().UpsertWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, request *manager.UpsertWorkflowExecutionRequest) error {
			v, ok := request.SearchAttributes.IndexedFields[searchattribute.TemporalNamespaceDivision]
			s.True(ok)

			var actualArchetype string
			err := payload.Decode(v, &actualArchetype)
			s.NoError(err)
			s.Equal(archetype, actualArchetype)
			return nil
		},
	)

	resp := s.visibilityQueueTaskExecutor.Execute(context.Background(), s.newTaskExecutable(visibilityTask))
	s.Nil(resp.ExecutionErr)
}

func (s *visibilityQueueTaskExecutorSuite) TestProcessChasmTask_ClosedExecution() {
	key := definition.NewWorkflowKey(
		s.namespaceID.String(),
		"some random ID",
		uuid.New(),
	)
	archetype := "archetype"

	mutableState := s.buildChasmMutableState(key, archetype, 5)

	closeTime := s.now.Add(5 * time.Minute)
	mutableState.ExecutionInfo.CloseTime = timestamppb.New(closeTime)
	mutableState.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	mutableState.ExecutionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED

	visibilityTask := s.buildChasmVisTask(key, 5)

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: mutableState}, nil)
	s.mockVisibilityMgr.EXPECT().RecordWorkflowExecutionClosed(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, request *manager.RecordWorkflowExecutionClosedRequest) error {
			s.True(closeTime.Equal(request.CloseTime))
			s.NotEmpty(request.ExecutionDuration)
			s.Zero(request.HistoryLength)
			s.Zero(request.HistorySizeBytes)
			s.NotEmpty(request.StateTransitionCount)

			v, ok := request.SearchAttributes.IndexedFields[searchattribute.TemporalNamespaceDivision]
			s.True(ok)

			var actualArchetype string
			err := payload.Decode(v, &actualArchetype)
			s.NoError(err)
			s.Equal(archetype, actualArchetype)
			return nil
		},
	)

	resp := s.visibilityQueueTaskExecutor.Execute(context.Background(), s.newTaskExecutable(visibilityTask))
	s.Nil(resp.ExecutionErr)
}

func (s *visibilityQueueTaskExecutorSuite) buildChasmMutableState(
	key definition.WorkflowKey,
	archetype string,
	visComponentTransitionCount int64,
) *persistencespb.WorkflowMutableState {
	executionInfo := &persistencespb.WorkflowExecutionInfo{
		NamespaceId:    s.namespaceID.String(),
		WorkflowId:     key.WorkflowID,
		LastUpdateTime: timestamp.TimeNowPtrUtc(),
		StartTime:      timestamppb.Now(),
		ExecutionTime:  timestamppb.Now(),
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: s.version, TransitionCount: 1},
		},
		StateTransitionCount: 10,
	}
	executionState := &persistencespb.WorkflowExecutionState{
		RunId:     key.RunID,
		State:     enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		StartTime: timestamppb.Now(),
	}

	visibilityComponentData := &persistencespb.ChasmVisibilityData{
		TransitionCount: visComponentTransitionCount,
	}
	data, err := visibilityComponentData.Marshal()
	s.NoError(err)

	chasmNodes := map[string]*persistencespb.ChasmNode{
		"": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{NamespaceFailoverVersion: s.version, TransitionCount: 1},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: s.version, TransitionCount: 1},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						Type: archetype,
					},
				},
			},
			Data: &commonpb.DataBlob{Data: []byte("some-random-data")},
		},
		"Visibility": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{NamespaceFailoverVersion: s.version, TransitionCount: 1},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: s.version, TransitionCount: 1},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						Type: "core.vis",
					},
				},
			},
			Data: &commonpb.DataBlob{
				Data:         data,
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			},
		},
	}
	return &persistencespb.WorkflowMutableState{
		ExecutionInfo:  executionInfo,
		ExecutionState: executionState,
		ChasmNodes:     chasmNodes,
		NextEventId:    common.FirstEventID,
	}
}

func (s *visibilityQueueTaskExecutorSuite) buildChasmVisTask(
	key definition.WorkflowKey,
	taskTransitionCount int64,
) *tasks.ChasmTask {
	visTaskData := &persistencespb.ChasmVisibilityTaskData{
		TransitionCount: taskTransitionCount,
	}
	data, err := visTaskData.Marshal()
	s.NoError(err)

	return &tasks.ChasmTask{
		WorkflowKey:         key,
		VisibilityTimestamp: time.Now().UTC(),
		TaskID:              int64(59),
		Category:            tasks.CategoryVisibility,
		Info: &persistencespb.ChasmTaskInfo{
			ComponentInitialVersionedTransition:    &persistencespb.VersionedTransition{NamespaceFailoverVersion: s.version, TransitionCount: 1},
			ComponentLastUpdateVersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: s.version, TransitionCount: 1},
			Path:                                   "Visibility",
			Type:                                   "core.visTask",
			Data: &commonpb.DataBlob{
				Data:         data,
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			},
		},
	}
}

func (s *visibilityQueueTaskExecutorSuite) execute(task tasks.Task) error {
	return s.visibilityQueueTaskExecutor.Execute(context.Background(), s.newTaskExecutable(task)).ExecutionErr
}

func (s *visibilityQueueTaskExecutorSuite) createVisibilityRequestBase(
	namespaceName namespace.Name,
	task tasks.Task,
	mutableState historyi.MutableState,
	taskQueueName string,
	parentExecution *commonpb.WorkflowExecution,
	rootExecution *commonpb.WorkflowExecution,
	searchAttributes map[string]any,
) *manager.VisibilityRequestBase {
	encodedSearchAttributes, err := searchattribute.Encode(
		searchAttributes,
		&searchattribute.NameTypeMap{},
	)
	s.NoError(err)

	execution := &commonpb.WorkflowExecution{
		WorkflowId: task.GetWorkflowID(),
		RunId:      task.GetRunID(),
	}
	executionInfo := mutableState.GetExecutionInfo()

	if rootExecution == nil {
		if parentExecution != nil {
			rootExecution = parentExecution
		} else {
			rootExecution = &commonpb.WorkflowExecution{
				WorkflowId: execution.WorkflowId,
				RunId:      execution.RunId,
			}
		}
	}

	return &manager.VisibilityRequestBase{
		NamespaceID:      namespace.ID(task.GetNamespaceID()),
		Namespace:        namespaceName,
		Execution:        execution,
		WorkflowTypeName: executionInfo.WorkflowTypeName,
		StartTime:        timestamp.TimeValue(mutableState.GetExecutionState().GetStartTime()),
		Status:           mutableState.GetExecutionState().GetStatus(),
		ExecutionTime:    timestamp.TimeValue(executionInfo.GetExecutionTime()),
		TaskID:           task.GetTaskID(),
		ShardID:          s.mockShard.GetShardID(),
		TaskQueue:        taskQueueName,
		ParentExecution:  parentExecution,
		RootExecution:    rootExecution,
		SearchAttributes: encodedSearchAttributes,
	}
}

func (s *visibilityQueueTaskExecutorSuite) createRecordWorkflowExecutionStartedRequest(
	namespaceName namespace.Name,
	task *tasks.StartExecutionVisibilityTask,
	mutableState historyi.MutableState,
	taskQueueName string,
) gomock.Matcher {
	return protomock.Eq(&manager.RecordWorkflowExecutionStartedRequest{
		VisibilityRequestBase: s.createVisibilityRequestBase(
			namespaceName,
			task,
			mutableState,
			taskQueueName,
			nil,
			nil,
			nil,
		),
	})
}

func (s *visibilityQueueTaskExecutorSuite) createUpsertWorkflowRequest(
	namespaceName namespace.Name,
	task *tasks.UpsertExecutionVisibilityTask,
	mutableState historyi.MutableState,
	taskQueueName string,
) gomock.Matcher {
	return protomock.Eq(&manager.UpsertWorkflowExecutionRequest{
		VisibilityRequestBase: s.createVisibilityRequestBase(
			namespaceName,
			task,
			mutableState,
			taskQueueName,
			nil,
			nil,
			nil,
		),
	})
}

func (s *visibilityQueueTaskExecutorSuite) createRecordWorkflowExecutionClosedRequest(
	namespaceName namespace.Name,
	task *tasks.CloseExecutionVisibilityTask,
	mutableState historyi.MutableState,
	taskQueueName string,
	parentExecution *commonpb.WorkflowExecution,
	rootExecution *commonpb.WorkflowExecution,
	searchAttributes map[string]any,
) gomock.Matcher {
	executionInfo := mutableState.GetExecutionInfo()
	return protomock.Eq(&manager.RecordWorkflowExecutionClosedRequest{
		VisibilityRequestBase: s.createVisibilityRequestBase(
			namespaceName,
			task,
			mutableState,
			taskQueueName,
			parentExecution,
			rootExecution,
			searchAttributes,
		),
		CloseTime:            timestamp.TimeValue(executionInfo.GetCloseTime()),
		HistoryLength:        mutableState.GetNextEventID() - 1,
		HistorySizeBytes:     executionInfo.GetExecutionStats().GetHistorySize(),
		StateTransitionCount: executionInfo.GetStateTransitionCount(),
	})
}

func (s *visibilityQueueTaskExecutorSuite) createPersistenceMutableState(
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

func (s *visibilityQueueTaskExecutorSuite) newTaskExecutable(
	task tasks.Task,
) queues.Executable {
	return queues.NewExecutable(
		queues.DefaultReaderId,
		task,
		s.visibilityQueueTaskExecutor,
		nil,
		nil,
		queues.NewNoopPriorityAssigner(),
		s.mockShard.GetTimeSource(),
		s.mockShard.GetNamespaceRegistry(),
		s.mockShard.GetClusterMetadata(),
		nil,
		metrics.NoopMetricsHandler,
		telemetry.NoopTracer,
	)
}

func (s *visibilityQueueTaskExecutorSuite) TestCopyMapPayload() {
	var input map[string]*commonpb.Payload
	s.Nil(copyMapPayload(input))

	key := "key"
	val := payload.EncodeBytes([]byte{'1', '2', '3'})
	input = map[string]*commonpb.Payload{
		key: val,
	}
	result := copyMapPayload(input)
	s.Equal(input, result)
	result[key].GetData()[0] = '0'
	s.Equal(byte('1'), val.GetData()[0])
}
