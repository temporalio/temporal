package history

import (
	"context"
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/mockapi/workflowservicemock/v1"
	"go.temporal.io/server/common/testing/protomock"
	"go.temporal.io/server/service/history/consts"
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
)

// TODO: remove all SetCurrentTime usage in this test suite
// after clusterName & getCurrentTime() method are deprecated
// from transferQueueStandbyTaskExecutor

type (
	transferQueueStandbyTaskExecutorSuite struct {
		suite.Suite
		*require.Assertions

		controller            *gomock.Controller
		mockShard             *shard.ContextTest
		mockNamespaceCache    *namespace.MockRegistry
		mockClusterMetadata   *cluster.MockMetadata
		mockFrontendClient    *workflowservicemock.MockWorkflowServiceClient
		mockHistoryClient     *historyservicemock.MockHistoryServiceClient
		mockMatchingClient    *matchingservicemock.MockMatchingServiceClient
		mockRemoteAdminClient *adminservicemock.MockAdminServiceClient
		mockChasmEngine       chasm.Engine

		mockExecutionMgr     *persistence.MockExecutionManager
		mockArchivalMetadata archiver.MetadataMock
		mockArchiverProvider *provider.MockArchiverProvider

		workflowCache             wcache.Cache
		logger                    log.Logger
		namespaceID               namespace.ID
		namespaceEntry            *namespace.Namespace
		version                   int64
		clusterName               string
		now                       time.Time
		timeSource                *clock.EventTimeSource
		localVerificationDuration time.Duration
		fetchHistoryDuration      time.Duration
		discardDuration           time.Duration

		transferQueueStandbyTaskExecutor *transferQueueStandbyTaskExecutor
		mockSearchAttributesProvider     *searchattribute.MockProvider
		mockVisibilityManager            *manager.MockVisibilityManager
		clientBean                       *client.MockBean
	}
)

func TestTransferQueueStandbyTaskExecutorSuite(t *testing.T) {
	s := new(transferQueueStandbyTaskExecutorSuite)
	suite.Run(t, s)
}

func (s *transferQueueStandbyTaskExecutorSuite) SetupSuite() {
}

func (s *transferQueueStandbyTaskExecutorSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	config := tests.NewDynamicConfig()

	s.namespaceEntry = tests.GlobalStandbyNamespaceEntry
	s.namespaceID = s.namespaceEntry.ID()
	s.version = s.namespaceEntry.FailoverVersion()
	s.now = time.Now().UTC()
	s.timeSource = clock.NewEventTimeSource().Update(s.now)
	s.localVerificationDuration = time.Minute * 10
	s.fetchHistoryDuration = time.Minute * 12
	s.discardDuration = time.Minute * 30

	s.controller = gomock.NewController(s.T())
	s.mockShard = shard.NewTestContextWithTimeSource(
		s.controller,
		&persistencespb.ShardInfo{
			RangeId: 1,
		},
		config,
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

	s.mockHistoryClient = s.mockShard.Resource.HistoryClient
	s.mockMatchingClient = s.mockShard.Resource.MatchingClient
	s.mockExecutionMgr = s.mockShard.Resource.ExecutionMgr
	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata
	s.mockArchivalMetadata = s.mockShard.Resource.ArchivalMetadata
	s.mockArchiverProvider = s.mockShard.Resource.ArchiverProvider
	s.mockNamespaceCache = s.mockShard.Resource.NamespaceCache
	s.mockRemoteAdminClient = s.mockShard.Resource.RemoteAdminClient
	s.mockSearchAttributesProvider = s.mockShard.Resource.SearchAttributesProvider
	s.mockVisibilityManager = s.mockShard.Resource.VisibilityManager
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(tests.Namespace).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.TargetNamespaceID).Return(tests.GlobalTargetNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(tests.TargetNamespace).Return(tests.GlobalTargetNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.ParentNamespaceID).Return(tests.GlobalParentNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(tests.ParentNamespace).Return(tests.GlobalParentNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.ChildNamespaceID).Return(tests.GlobalChildNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(tests.ChildNamespace).Return(tests.GlobalChildNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.StandbyNamespaceID).Return(tests.GlobalStandbyNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(tests.StandbyNamespace).Return(tests.GlobalStandbyNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceName(tests.StandbyNamespaceID).Return(tests.StandbyNamespace, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.StandbyWithVisibilityArchivalNamespaceID).
		Return(tests.GlobalStandbyWithVisibilityArchivalNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(tests.StandbyWithVisibilityArchivalNamespace).
		Return(tests.GlobalStandbyWithVisibilityArchivalNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceName(tests.StandbyWithVisibilityArchivalNamespaceID).
		Return(tests.StandbyWithVisibilityArchivalNamespace, nil).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetClusterID().Return(cluster.TestCurrentClusterInitialFailoverVersion).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.IsGlobalNamespace(), s.version).Return(s.clusterName).AnyTimes()
	s.clientBean = client.NewMockBean(s.controller)
	s.workflowCache = wcache.NewHostLevelCache(s.mockShard.GetConfig(), s.mockShard.GetLogger(), metrics.NoopMetricsHandler)
	s.logger = s.mockShard.GetLogger()
	s.mockFrontendClient = s.mockShard.Resource.FrontendClient
	s.mockChasmEngine = chasm.NewMockEngine(s.controller)

	s.mockArchivalMetadata.SetHistoryEnabledByDefault()
	s.mockArchivalMetadata.SetVisibilityEnabledByDefault()

	h := &historyEngineImpl{
		currentClusterName: s.mockShard.Resource.GetClusterMetadata().GetCurrentClusterName(),
		shardContext:       s.mockShard,
		clusterMetadata:    s.mockClusterMetadata,
		executionManager:   s.mockExecutionMgr,
		logger:             s.logger,
		tokenSerializer:    tasktoken.NewSerializer(),
		metricsHandler:     s.mockShard.GetMetricsHandler(),
	}
	s.mockShard.SetEngineForTesting(h)
	s.clusterName = cluster.TestAlternativeClusterName

	s.transferQueueStandbyTaskExecutor = newTransferQueueStandbyTaskExecutor(
		s.mockShard,
		s.workflowCache,
		s.logger,
		metrics.NoopMetricsHandler,
		s.clusterName,
		s.mockShard.Resource.HistoryClient,
		s.mockShard.Resource.MatchingClient,
		s.mockVisibilityManager,
		s.mockChasmEngine,
		s.clientBean,
	).(*transferQueueStandbyTaskExecutor)
}

func (s *transferQueueStandbyTaskExecutorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessActivityTask_Pending() {
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
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	s.NoError(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.NewString())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := s.mustGenerateTaskID()
	activityID := "activity-1"
	activityType := "some random activity type"
	event, _ = addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID, activityType, taskQueueName, &commonpb.Payloads{}, 1*time.Second, 1*time.Second, 1*time.Second, 1*time.Second)

	now := time.Now().UTC()
	transferTask := &tasks.ActivityTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		VisibilityTimestamp: now,
		TaskID:              taskID,
		TaskQueue:           taskQueueName,
		ScheduledEventID:    event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	// no-op post action
	s.mockShard.SetCurrentTime(s.clusterName, now)
	resp := s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(consts.ErrTaskRetry, resp.ExecutionErr)

	// resend history post action
	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.fetchHistoryDuration))
	resp = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(consts.ErrTaskRetry, resp.ExecutionErr)

	// push to matching post action
	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.discardDuration))
	s.mockMatchingClient.EXPECT().AddActivityTask(gomock.Any(), gomock.Any(), gomock.Any()).Return(&matchingservice.AddActivityTaskResponse{}, nil)
	resp = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Nil(resp.ExecutionErr)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestExecuteChasmSideEffectTransferTask_ExecutesTask() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowKey.WorkflowID,
		RunId:      tests.WorkflowKey.RunID,
	}

	// Mock the CHASM tree.
	chasmTree := historyi.NewMockChasmTree(s.controller)
	expectValidate := func(isValid bool, err error) {
		chasmTree.EXPECT().ValidateSideEffectTask(
			gomock.Any(),
			gomock.Any(),
		).Times(1).Return(isValid, err)
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

	// Add a valid transfer task.
	transferTask := &tasks.ChasmTask{
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
		gomock.Any(), s.mockShard, gomock.Any(), execution, tests.ArchetypeID, gomock.Any(),
	).Return(wfCtx, wcache.NoopReleaseFn, nil).AnyTimes()

	//nolint:revive // unchecked-type-assertion
	transferQueueStandbyTaskExecutor := newTransferQueueStandbyTaskExecutor(
		s.mockShard,
		mockCache,
		s.logger,
		metrics.NoopMetricsHandler,
		s.clusterName,
		s.mockShard.Resource.HistoryClient,
		s.mockShard.Resource.MatchingClient,
		s.mockVisibilityManager,
		s.mockChasmEngine,
		s.clientBean,
	).(*transferQueueStandbyTaskExecutor)

	// Validation succeeds, task should retry.
	expectValidate(true, nil)
	resp := transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.NotNil(resp)
	s.ErrorIs(consts.ErrTaskRetry, resp.ExecutionErr)

	// Validation succeeds but task is invalid.
	expectValidate(false, nil)
	resp = transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.NotNil(resp)
	s.NoError(resp.ExecutionErr)

	// Validation fails, processing should fail.
	expectedErr := errors.New("validation error")
	expectValidate(false, expectedErr)
	resp = transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.NotNil(resp)
	s.ErrorIs(expectedErr, resp.ExecutionErr)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessActivityTask_Success() {
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
				WorkflowType: &commonpb.WorkflowType{Name: workflowType},
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: taskQueueName,
					Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
				},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	s.NoError(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.NewString())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := s.mustGenerateTaskID()
	activityID := "activity-1"
	activityType := "some random activity type"
	event, _ = addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID, activityType, taskQueueName, &commonpb.Payloads{}, 1*time.Second, 1*time.Second, 1*time.Second, 1*time.Second)

	now := time.Now().UTC()
	transferTask := &tasks.ActivityTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		VisibilityTimestamp: now,
		TaskID:              taskID,
		TaskQueue:           taskQueueName,
		ScheduledEventID:    event.GetEventId(),
	}

	event = addActivityTaskStartedEvent(mutableState, event.GetEventId(), "")
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, now)
	resp := s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Nil(resp.ExecutionErr)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessActivityTask_Paused() {
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
				WorkflowType: &commonpb.WorkflowType{Name: workflowType},
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: taskQueueName,
					Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
				},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	s.NoError(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.NewString())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := s.mustGenerateTaskID()
	activityID := "activity-1"
	activityType := "some random activity type"
	event, ai := addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID, activityType, taskQueueName, &commonpb.Payloads{}, 1*time.Second, 1*time.Second, 1*time.Second, 1*time.Second)

	// Set the activity as paused without starting it
	ai.Paused = true

	now := time.Now().UTC()
	transferTask := &tasks.ActivityTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		VisibilityTimestamp: now,
		TaskID:              taskID,
		TaskQueue:           taskQueueName,
		ScheduledEventID:    event.GetEventId(),
		Stamp:               ai.Stamp, // Ensure stamp matches
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	resp := s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.NoError(resp.ExecutionErr)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessWorkflowTask_Pending() {
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
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	s.NoError(err)

	taskID := s.mustGenerateTaskID()
	wt := addWorkflowTaskScheduledEvent(mutableState)

	now := time.Now().UTC()
	transferTask := &tasks.WorkflowTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		VisibilityTimestamp: now,
		TaskID:              taskID,
		TaskQueue:           taskQueueName,
		ScheduledEventID:    wt.ScheduledEventID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, wt.ScheduledEventID, wt.Version)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	// no-op post action
	s.mockShard.SetCurrentTime(s.clusterName, now)
	resp := s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(consts.ErrTaskRetry, resp.ExecutionErr)

	// resend history post action
	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.fetchHistoryDuration))
	resp = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(consts.ErrTaskRetry, resp.ExecutionErr)

	// push to matching post action
	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.discardDuration))
	s.mockMatchingClient.EXPECT().AddWorkflowTask(gomock.Any(), gomock.Any(), gomock.Any()).Return(&matchingservice.AddWorkflowTaskResponse{}, nil)
	resp = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Nil(resp.ExecutionErr)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessWorkflowTask_Success_FirstWorkflowTask() {
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
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	s.NoError(err)

	taskID := s.mustGenerateTaskID()
	wt := addWorkflowTaskScheduledEvent(mutableState)

	now := time.Now().UTC()
	transferTask := &tasks.WorkflowTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		VisibilityTimestamp: now,
		TaskID:              taskID,
		TaskQueue:           taskQueueName,
		ScheduledEventID:    wt.ScheduledEventID,
	}

	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.NewString())
	wt.StartedEventID = event.GetEventId()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, now)
	resp := s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Nil(resp.ExecutionErr)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessWorkflowTask_Success_NonFirstWorkflowTask() {
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
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	s.NoError(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.NewString())
	wt.StartedEventID = event.GetEventId()
	addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := s.mustGenerateTaskID()
	wt = addWorkflowTaskScheduledEvent(mutableState)

	now := time.Now().UTC()
	transferTask := &tasks.WorkflowTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		VisibilityTimestamp: now,
		TaskID:              taskID,
		TaskQueue:           taskQueueName,
		ScheduledEventID:    wt.ScheduledEventID,
	}

	event = addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.NewString())
	wt.StartedEventID = event.GetEventId()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, now)
	resp := s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Nil(resp.ExecutionErr)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessWorkflowTask_StampMismatch() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"
	now := time.Now().UTC().Add(-10 * time.Second)

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

	wt := addWorkflowTaskScheduledEvent(mutableState)

	transferTask := &tasks.WorkflowTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		TaskID:              s.mustGenerateTaskID(),
		TaskQueue:           taskQueueName,
		ScheduledEventID:    wt.ScheduledEventID,
		VisibilityTimestamp: now,
		Stamp:               wt.Stamp,
	}

	// Modify the workflow task stamp in mutable state to create mismatch
	mutableState.GetExecutionInfo().WorkflowTaskStamp = wt.Stamp + 1

	persistenceMutableState := s.createPersistenceMutableState(mutableState, wt.ScheduledEventID, s.version)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockShard.SetCurrentTime(s.clusterName, now)

	resp := s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.ErrorIs(resp.ExecutionErr, consts.ErrStaleReference)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessCloseExecution() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	parentNamespaceID := "some random parent namespace ID"
	parentInitiatedID := int64(3222)
	parentInitiatedVersion := int64(1234)
	parentNamespace := "some random parent namespace Name"
	parentExecution := &commonpb.WorkflowExecution{
		WorkflowId: "some random parent workflow ID",
		RunId:      uuid.NewString(),
	}
	parentClock := vclock.NewVectorClock(rand.Int63(), rand.Int31(), rand.Int63())

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
				Clock:            parentClock,
			},
		},
	)
	s.NoError(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.NewString())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := s.mustGenerateTaskID()
	event = addCompleteWorkflowEvent(mutableState, event.GetEventId(), nil)

	now := time.Now().UTC()
	transferTask := &tasks.CloseExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		VisibilityTimestamp: now,
		TaskID:              taskID,
	}

	expectedVerificationRequest := protomock.Eq(&historyservice.VerifyChildExecutionCompletionRecordedRequest{
		NamespaceId:            parentNamespaceID,
		ParentExecution:        parentExecution,
		ChildExecution:         execution,
		ParentInitiatedId:      parentInitiatedID,
		ParentInitiatedVersion: parentInitiatedVersion,
		Clock:                  parentClock,
	})

	expectedVerificationWithResendParentRequest := protomock.Eq(&historyservice.VerifyChildExecutionCompletionRecordedRequest{
		NamespaceId:            parentNamespaceID,
		ParentExecution:        parentExecution,
		ChildExecution:         execution,
		ParentInitiatedId:      parentInitiatedID,
		ParentInitiatedVersion: parentInitiatedVersion,
		Clock:                  parentClock,
		ResendParent:           true,
	})

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(parentNamespaceID)).Return(tests.GlobalParentNamespaceEntry, nil).AnyTimes()
	s.clientBean.EXPECT().GetRemoteAdminClient(tests.GlobalChildNamespaceEntry.ActiveClusterName(parentExecution.WorkflowId)).Return(s.mockRemoteAdminClient, nil).AnyTimes()
	s.mockRemoteAdminClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&adminservice.DescribeMutableStateRequest{
		Namespace:       tests.ParentNamespace.String(),
		Execution:       parentExecution,
		SkipForceReload: true,
		Archetype:       chasm.WorkflowArchetype,
	})).Return(nil, serviceerror.NewInternal("some error")).AnyTimes()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewDisabledArchvialConfig()).AnyTimes()

	s.mockShard.SetCurrentTime(s.clusterName, now)
	s.mockHistoryClient.EXPECT().VerifyChildExecutionCompletionRecorded(gomock.Any(), expectedVerificationRequest).Return(nil, nil)
	resp := s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Nil(resp.ExecutionErr)

	s.mockHistoryClient.EXPECT().VerifyChildExecutionCompletionRecorded(gomock.Any(), expectedVerificationRequest).Return(nil, consts.ErrWorkflowExecutionNotFound)
	resp = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(consts.ErrTaskRetry, resp.ExecutionErr)

	s.mockHistoryClient.EXPECT().VerifyChildExecutionCompletionRecorded(gomock.Any(), expectedVerificationRequest).Return(nil, consts.ErrWorkflowNotReady)
	resp = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(consts.ErrTaskRetry, resp.ExecutionErr)

	s.mockHistoryClient.EXPECT().VerifyChildExecutionCompletionRecorded(gomock.Any(), expectedVerificationRequest).Return(nil, serviceerror.NewUnimplemented("not implemented"))
	resp = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Nil(resp.ExecutionErr)

	s.mockHistoryClient.EXPECT().VerifyChildExecutionCompletionRecorded(gomock.Any(), expectedVerificationRequest).Return(nil, consts.ErrResourceExhaustedBusyWorkflow)
	resp = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	var verificationErr *verificationErr
	s.True(errors.As(resp.ExecutionErr, &verificationErr))
	var resourceExhaustedErr *serviceerror.ResourceExhausted
	s.True(errors.As(resp.ExecutionErr, &resourceExhaustedErr))

	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.localVerificationDuration))
	s.mockHistoryClient.EXPECT().VerifyChildExecutionCompletionRecorded(gomock.Any(), expectedVerificationWithResendParentRequest).Return(nil, consts.ErrWorkflowNotReady)
	resp = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(consts.ErrTaskRetry, resp.ExecutionErr)

	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.fetchHistoryDuration))
	s.mockHistoryClient.EXPECT().VerifyChildExecutionCompletionRecorded(gomock.Any(), expectedVerificationWithResendParentRequest).Return(nil, consts.ErrWorkflowNotReady)
	resp = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(consts.ErrTaskRetry, resp.ExecutionErr)

	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.discardDuration))
	s.mockHistoryClient.EXPECT().VerifyChildExecutionCompletionRecorded(gomock.Any(), expectedVerificationWithResendParentRequest).Return(nil, consts.ErrWorkflowNotReady)
	resp = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(consts.ErrTaskDiscarded, resp.ExecutionErr)

	randomErr := errors.New("some random error")
	s.mockHistoryClient.EXPECT().VerifyChildExecutionCompletionRecorded(gomock.Any(), expectedVerificationWithResendParentRequest).Return(nil, randomErr)
	resp = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.ErrorAs(resp.ExecutionErr, &verificationErr)
	s.Equal(randomErr, verificationErr.Unwrap())
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessCancelExecution_Pending() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	targetExecution := &commonpb.WorkflowExecution{
		WorkflowId: "some random target workflow ID",
		RunId:      uuid.NewString(),
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
		},
	)
	s.NoError(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.NewString())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := s.mustGenerateTaskID()
	event, _ = addRequestCancelInitiatedEvent(mutableState, event.GetEventId(), uuid.NewString(), tests.TargetNamespace, tests.TargetNamespaceID, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), true)

	now := time.Now().UTC()
	transferTask := &tasks.CancelExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		VisibilityTimestamp: now,
		TaskID:              taskID,
		InitiatedEventID:    event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, now)
	resp := s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(consts.ErrTaskRetry, resp.ExecutionErr)

	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.fetchHistoryDuration))
	resp = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(consts.ErrTaskRetry, resp.ExecutionErr)

	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.discardDuration))
	resp = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(consts.ErrTaskDiscarded, resp.ExecutionErr)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessCancelExecution_Success() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	targetExecution := &commonpb.WorkflowExecution{
		WorkflowId: "some random target workflow ID",
		RunId:      uuid.NewString(),
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
		},
	)
	s.NoError(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.NewString())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := s.mustGenerateTaskID()
	event, _ = addRequestCancelInitiatedEvent(mutableState, event.GetEventId(), uuid.NewString(), tests.TargetNamespace, tests.TargetNamespaceID, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), false)

	now := time.Now().UTC()
	transferTask := &tasks.CancelExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		VisibilityTimestamp: now,
		TaskID:              taskID,
		InitiatedEventID:    event.GetEventId(),
	}

	event = addCancelRequestedEvent(mutableState, event.GetEventId(), tests.TargetNamespace, tests.TargetNamespaceID, targetExecution.GetWorkflowId(), targetExecution.GetRunId())
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, now)
	resp := s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Nil(resp.ExecutionErr)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessSignalExecution_Pending() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	targetExecution := &commonpb.WorkflowExecution{
		WorkflowId: "some random target workflow ID",
		RunId:      uuid.NewString(),
	}
	signalName := "some random signal name"

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

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.NewString())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := s.mustGenerateTaskID()
	event, _ = addRequestSignalInitiatedEvent(mutableState, event.GetEventId(), uuid.NewString(),
		tests.TargetNamespace, tests.TargetNamespaceID, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), false, signalName, nil, "", nil)

	now := time.Now().UTC()
	transferTask := &tasks.SignalExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		VisibilityTimestamp: now,
		TaskID:              taskID,
		InitiatedEventID:    event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, now)
	resp := s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(consts.ErrTaskRetry, resp.ExecutionErr)

	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.fetchHistoryDuration))
	resp = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(consts.ErrTaskRetry, resp.ExecutionErr)

	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.discardDuration))
	resp = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(consts.ErrTaskDiscarded, resp.ExecutionErr)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessSignalExecution_Success() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	targetExecution := &commonpb.WorkflowExecution{
		WorkflowId: "some random target workflow ID",
		RunId:      uuid.NewString(),
	}
	signalName := "some random signal name"

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

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.NewString())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := s.mustGenerateTaskID()
	event, _ = addRequestSignalInitiatedEvent(mutableState, event.GetEventId(), uuid.NewString(),
		tests.TargetNamespace, tests.TargetNamespaceID, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), false, signalName, nil, "", nil)

	now := time.Now().UTC()
	transferTask := &tasks.SignalExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		VisibilityTimestamp: now,
		TaskID:              taskID,
		InitiatedEventID:    event.GetEventId(),
	}

	event = addSignaledEvent(mutableState, event.GetEventId(), tests.TargetNamespace, tests.TargetNamespaceID, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), "")
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, now)
	resp := s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Nil(resp.ExecutionErr)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessStartChildExecution_Pending() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	childWorkflowID := "some random child workflow ID"
	childWorkflowType := "some random child workflow type"
	childTaskQueueName := "some random child task queue"

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

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.NewString())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := s.mustGenerateTaskID()
	event, _ = addStartChildWorkflowExecutionInitiatedEvent(mutableState, event.GetEventId(),
		tests.ChildNamespace, tests.ChildNamespaceID, childWorkflowID, childWorkflowType, childTaskQueueName, nil, 1*time.Second, 1*time.Second, 1*time.Second, enumspb.PARENT_CLOSE_POLICY_ABANDON)

	now := time.Now().UTC()
	transferTask := &tasks.StartChildExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		VisibilityTimestamp: now,
		TaskID:              taskID,
		InitiatedEventID:    event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, now)
	resp := s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(consts.ErrTaskRetry, resp.ExecutionErr)

	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.fetchHistoryDuration))
	resp = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(consts.ErrTaskRetry, resp.ExecutionErr)

	event = addChildWorkflowExecutionStartedEvent(mutableState, event.GetEventId(), childWorkflowID, uuid.NewString(), childWorkflowType, nil)
	mutableState.FlushBufferedEvents()

	// clear the cache
	s.transferQueueStandbyTaskExecutor.cache = wcache.NewHostLevelCache(s.mockShard.GetConfig(), s.mockShard.GetLogger(), metrics.NoopMetricsHandler)
	persistenceMutableState = s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockHistoryClient.EXPECT().VerifyFirstWorkflowTaskScheduled(gomock.Any(), gomock.Any()).Return(nil, nil)
	resp = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Nil(resp.ExecutionErr)

	s.mockHistoryClient.EXPECT().VerifyFirstWorkflowTaskScheduled(gomock.Any(), gomock.Any()).Return(nil, consts.ErrWorkflowNotReady)
	resp = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(consts.ErrTaskRetry, resp.ExecutionErr)

	s.mockHistoryClient.EXPECT().VerifyFirstWorkflowTaskScheduled(gomock.Any(), gomock.Any()).Return(nil, consts.ErrWorkflowExecutionNotFound)
	resp = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(consts.ErrTaskRetry, resp.ExecutionErr)

	s.mockHistoryClient.EXPECT().VerifyFirstWorkflowTaskScheduled(gomock.Any(), gomock.Any()).Return(nil, &serviceerror.Unimplemented{})
	resp = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Nil(resp.ExecutionErr)

	s.mockHistoryClient.EXPECT().VerifyFirstWorkflowTaskScheduled(gomock.Any(), gomock.Any()).Return(nil, consts.ErrResourceExhaustedBusyWorkflow)
	resp = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	var verificationErr *verificationErr
	s.True(errors.As(resp.ExecutionErr, &verificationErr))
	var resourceExhaustedErr *serviceerror.ResourceExhausted
	s.True(errors.As(resp.ExecutionErr, &resourceExhaustedErr))

	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.discardDuration))
	s.mockHistoryClient.EXPECT().VerifyFirstWorkflowTaskScheduled(gomock.Any(), gomock.Any()).Return(nil, &serviceerror.WorkflowNotReady{})
	resp = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(consts.ErrTaskDiscarded, resp.ExecutionErr)

	randomErr := errors.New("some random error")
	s.mockHistoryClient.EXPECT().VerifyFirstWorkflowTaskScheduled(gomock.Any(), gomock.Any()).Return(nil, randomErr)
	resp = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.ErrorAs(resp.ExecutionErr, &verificationErr)
	s.Equal(randomErr, verificationErr.Unwrap())

	s.mockHistoryClient.EXPECT().VerifyFirstWorkflowTaskScheduled(gomock.Any(), gomock.Any()).Return(nil, nil)
	resp = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Nil(resp.ExecutionErr)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessStartChildExecution_Success() {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	childWorkflowID := "some random child workflow ID"
	childWorkflowType := "some random child workflow type"
	childTaskQueueName := "some random child task queue"

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
			ContinueAsNewInitiator: enumspb.CONTINUE_AS_NEW_INITIATOR_UNSPECIFIED,
		},
	)
	s.NoError(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.NewString())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&s.Suite, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := s.mustGenerateTaskID()
	event, childInfo := addStartChildWorkflowExecutionInitiatedEvent(mutableState, event.GetEventId(),
		tests.ChildNamespace, tests.ChildNamespaceID, childWorkflowID, childWorkflowType, childTaskQueueName, nil, 1*time.Second, 1*time.Second, 1*time.Second, enumspb.PARENT_CLOSE_POLICY_ABANDON)

	now := time.Now().UTC()
	transferTask := &tasks.StartChildExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		VisibilityTimestamp: now,
		TaskID:              taskID,
		InitiatedEventID:    event.GetEventId(),
	}
	event = addChildWorkflowExecutionStartedEvent(mutableState, event.GetEventId(), childWorkflowID, uuid.NewString(), childWorkflowType, nil)
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()
	childInfo.StartedEventId = event.GetEventId()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().VerifyFirstWorkflowTaskScheduled(gomock.Any(), gomock.Any()).Return(nil, nil)

	s.mockShard.SetCurrentTime(s.clusterName, now)
	resp := s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Nil(resp.ExecutionErr)

	// workflow closed && child started && parent close policy is abandon
	event, err = mutableState.AddTimeoutWorkflowEvent(
		mutableState.GetNextEventID(),
		enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET,
		uuid.NewString(),
	)
	s.NoError(err)

	s.transferQueueStandbyTaskExecutor.cache = wcache.NewHostLevelCache(s.mockShard.GetConfig(), s.mockShard.GetLogger(), metrics.NoopMetricsHandler)
	persistenceMutableState = s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().VerifyFirstWorkflowTaskScheduled(gomock.Any(), gomock.Any()).Return(nil, nil)

	s.mockShard.SetCurrentTime(s.clusterName, now)
	resp = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Nil(resp.ExecutionErr)
}

func (s *transferQueueStandbyTaskExecutorSuite) createPersistenceMutableState(
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

func (s *transferQueueStandbyTaskExecutorSuite) newTaskExecutable(
	task tasks.Task,
) queues.Executable {
	return queues.NewExecutable(
		queues.DefaultReaderId,
		task,
		s.transferQueueStandbyTaskExecutor,
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

func (s *transferQueueStandbyTaskExecutorSuite) mustGenerateTaskID() int64 {
	taskID, err := s.mockShard.GenerateTaskID()
	s.NoError(err)
	return taskID
}
