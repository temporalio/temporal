package history

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
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
	engine3Suite struct {
		suite.Suite
		*require.Assertions

		controller              *gomock.Controller
		mockShard               *shard.ContextTest
		mockTxProcessor         *queues.MockQueue
		mockTimerProcessor      *queues.MockQueue
		mockVisibilityProcessor *queues.MockQueue
		mockEventsCache         *events.MockCache
		mockNamespaceCache      *namespace.MockRegistry
		mockClusterMetadata     *cluster.MockMetadata
		workflowCache           wcache.Cache
		historyEngine           *historyEngineImpl
		mockExecutionMgr        *persistence.MockExecutionManager
		mockVisibilityManager   *manager.MockVisibilityManager

		config *configs.Config
		logger log.Logger
	}
)

func TestEngine3Suite(t *testing.T) {
	s := new(engine3Suite)
	suite.Run(t, s)
}

func (s *engine3Suite) SetupSuite() {
	s.config = tests.NewDynamicConfig()
}

func (s *engine3Suite) TearDownSuite() {
}

func (s *engine3Suite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())

	s.mockTxProcessor = queues.NewMockQueue(s.controller)
	s.mockTimerProcessor = queues.NewMockQueue(s.controller)
	s.mockVisibilityProcessor = queues.NewMockQueue(s.controller)
	s.mockTxProcessor.EXPECT().Category().Return(tasks.CategoryTransfer).AnyTimes()
	s.mockTimerProcessor.EXPECT().Category().Return(tasks.CategoryTimer).AnyTimes()
	s.mockVisibilityProcessor.EXPECT().Category().Return(tasks.CategoryVisibility).AnyTimes()
	s.mockTxProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	s.mockTimerProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	s.mockVisibilityProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()

	s.mockShard = shard.NewTestContext(
		s.controller,
		persistencespb.ShardInfo_builder{
			ShardId: 1,
			RangeId: 1,
		}.Build(),
		s.config,
	)

	reg := hsm.NewRegistry()
	err := workflow.RegisterStateMachine(reg)
	s.NoError(err)
	s.mockShard.SetStateMachineRegistry(reg)

	s.mockExecutionMgr = s.mockShard.Resource.ExecutionMgr
	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata
	s.mockNamespaceCache = s.mockShard.Resource.NamespaceCache
	s.mockEventsCache = s.mockShard.MockEventsCache
	s.mockVisibilityManager = s.mockShard.Resource.VisibilityManager

	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(false, common.EmptyVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()
	s.workflowCache = wcache.NewHostLevelCache(s.mockShard.GetConfig(), s.mockShard.GetLogger(), metrics.NoopMetricsHandler)
	s.mockVisibilityManager.EXPECT().GetIndexName().Return("").AnyTimes()
	s.mockVisibilityManager.EXPECT().
		ValidateCustomSearchAttributes(gomock.Any()).
		DoAndReturn(
			func(searchAttributes map[string]any) (map[string]any, error) {
				return searchAttributes, nil
			},
		).
		AnyTimes()
	s.logger = s.mockShard.GetLogger()

	h := &historyEngineImpl{
		currentClusterName: s.mockShard.GetClusterMetadata().GetCurrentClusterName(),
		shardContext:       s.mockShard,
		clusterMetadata:    s.mockClusterMetadata,
		executionManager:   s.mockExecutionMgr,
		logger:             s.logger,
		throttledLogger:    s.logger,
		metricsHandler:     metrics.NoopMetricsHandler,
		tokenSerializer:    tasktoken.NewSerializer(),
		config:             s.config,
		timeSource:         s.mockShard.GetTimeSource(),
		eventNotifier:      events.NewNotifier(clock.NewRealTimeSource(), metrics.NoopMetricsHandler, func(namespace.ID, string) int32 { return 1 }),
		queueProcessors: map[tasks.Category]queues.Queue{
			s.mockTxProcessor.Category():         s.mockTxProcessor,
			s.mockTimerProcessor.Category():      s.mockTimerProcessor,
			s.mockVisibilityProcessor.Category(): s.mockVisibilityProcessor,
		},
		workflowConsistencyChecker: api.NewWorkflowConsistencyChecker(s.mockShard, s.workflowCache),
		persistenceVisibilityMgr:   s.mockVisibilityManager,
	}
	s.mockShard.SetEngineForTesting(h)

	s.historyEngine = h
}

func (s *engine3Suite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *engine3Suite) TestRecordWorkflowTaskStartedSuccessStickyEnabled() {
	fakeHistory := []*historypb.HistoryEvent{
		historypb.HistoryEvent_builder{
			EventId:   int64(1),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		}.Build(),
		historypb.HistoryEvent_builder{
			EventId:   int64(2),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			WorkflowExecutionStartedEventAttributes: historypb.WorkflowExecutionStartedEventAttributes_builder{
				SearchAttributes: commonpb.SearchAttributes_builder{
					IndexedFields: map[string]*commonpb.Payload{
						"Keyword01":             payload.EncodeString("random-keyword"),
						"TemporalChangeVersion": payload.EncodeString("random-data"),
					},
				}.Build(),
			}.Build(),
		}.Build(),
		historypb.HistoryEvent_builder{
			EventId:   int64(3),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		}.Build(),
	}

	s.mockExecutionMgr.EXPECT().ReadHistoryBranch(gomock.Any(), gomock.Any()).Return(&persistence.ReadHistoryBranchResponse{
		HistoryEvents: fakeHistory,
		NextPageToken: []byte{},
		Size:          1,
	}, nil)

	testNamespaceEntry := namespace.NewLocalNamespaceForTest(
		persistencespb.NamespaceInfo_builder{Id: tests.NamespaceID.String(), Name: tests.Namespace.String()}.Build(), persistencespb.NamespaceConfig_builder{Retention: timestamp.DurationFromDays(1)}.Build(), "active",
	)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()

	s.mockShard.Resource.SearchAttributesProvider.EXPECT().GetSearchAttributes(gomock.Any(), false).Return(searchattribute.TestNameTypeMap(), nil)
	s.mockShard.Resource.SearchAttributesMapperProvider.EXPECT().GetMapper(tests.Namespace).
		Return(&searchattribute.TestMapper{Namespace: tests.Namespace.String()}, nil).AnyTimes()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution_builder{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}.Build()
	tl := "testTaskQueue"
	stickyTl := "stickyTaskQueue"
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(s.historyEngine.shardContext, s.mockEventsCache, tests.LocalNamespaceEntry,
		we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	executionInfo := ms.GetExecutionInfo()
	executionInfo.SetLastUpdateTime(timestamp.TimeNowPtrUtc())
	executionInfo.SetStickyTaskQueue(stickyTl)

	addWorkflowExecutionStartedEvent(ms, we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)

	wfMs := workflow.TestCloneToProto(context.Background(), ms)

	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	request := historyservice.RecordWorkflowTaskStartedRequest_builder{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: we,
		ScheduledEventId:  2,
		RequestId:         "reqId",
		PollRequest: workflowservice.PollWorkflowTaskQueueRequest_builder{
			TaskQueue: taskqueuepb.TaskQueue_builder{
				Name: stickyTl,
			}.Build(),
			Identity: identity,
		}.Build(),
	}.Build()

	expectedResponse := &historyservice.RecordWorkflowTaskStartedResponseWithRawHistory{}
	expectedResponse.SetWorkflowType(ms.GetWorkflowType())
	executionInfo = ms.GetExecutionInfo()
	if executionInfo.GetLastCompletedWorkflowTaskStartedEventId() != common.EmptyEventID {
		expectedResponse.SetPreviousStartedEventId(executionInfo.GetLastCompletedWorkflowTaskStartedEventId())
	}
	expectedResponse.SetScheduledEventId(wt.ScheduledEventID)
	expectedResponse.SetScheduledTime(timestamppb.New(wt.ScheduledTime))
	expectedResponse.SetStartedEventId(wt.ScheduledEventID + 1)
	expectedResponse.SetStickyExecutionEnabled(true)
	expectedResponse.SetNextEventId(ms.GetNextEventID() + 1)
	expectedResponse.SetAttempt(wt.Attempt)
	expectedResponse.SetWorkflowExecutionTaskQueue(taskqueuepb.TaskQueue_builder{
		Name: executionInfo.GetTaskQueue(),
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}.Build())
	expectedResponse.BranchToken, _ = ms.GetCurrentBranchToken()
	expectedResponse.SetHistory(historypb.History_builder{Events: fakeHistory}.Build())
	expectedResponse.SetNextPageToken(nil)

	response, err := s.historyEngine.RecordWorkflowTaskStarted(context.Background(), request)
	s.Nil(err)
	s.NotNil(response)
	s.True(response.GetStartedTime().AsTime().After(expectedResponse.GetScheduledTime().AsTime()))
	expectedResponse.SetStartedTime(response.GetStartedTime())
	s.Equal(expectedResponse, response)
}

func (s *engine3Suite) TestRecordWorkflowTaskStartedSuccessStickyEnabled_WithInternalRawHistory() {
	s.config.SendRawHistoryBetweenInternalServices = func() bool { return true }
	fakeHistory := historypb.History_builder{
		Events: []*historypb.HistoryEvent{
			historypb.HistoryEvent_builder{
				EventId:   int64(1),
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			}.Build(),
			historypb.HistoryEvent_builder{
				EventId:   int64(2),
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				WorkflowExecutionStartedEventAttributes: historypb.WorkflowExecutionStartedEventAttributes_builder{
					SearchAttributes: commonpb.SearchAttributes_builder{
						IndexedFields: map[string]*commonpb.Payload{
							"Keyword01":             payload.EncodeString("random-keyword"),
							"TemporalChangeVersion": payload.EncodeString("random-data"),
						},
					}.Build(),
				}.Build(),
			}.Build(),
			historypb.HistoryEvent_builder{
				EventId:   int64(3),
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
			}.Build(),
		},
	}.Build()
	historyBlob, err := fakeHistory.Marshal()
	s.NoError(err)

	s.mockExecutionMgr.EXPECT().ReadRawHistoryBranch(gomock.Any(), gomock.Any()).Return(&persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*commonpb.DataBlob{
			commonpb.DataBlob_builder{
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				Data:         historyBlob,
			}.Build(),
		},
		NextPageToken: []byte{},
		Size:          1,
	}, nil)

	testNamespaceEntry := namespace.NewLocalNamespaceForTest(
		persistencespb.NamespaceInfo_builder{Id: tests.NamespaceID.String()}.Build(), persistencespb.NamespaceConfig_builder{Retention: timestamp.DurationFromDays(1)}.Build(), "active",
	)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution_builder{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}.Build()
	tl := "testTaskQueue"
	stickyTl := "stickyTaskQueue"
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(s.historyEngine.shardContext, s.mockEventsCache, tests.LocalNamespaceEntry,
		we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	executionInfo := ms.GetExecutionInfo()
	executionInfo.SetLastUpdateTime(timestamp.TimeNowPtrUtc())
	executionInfo.SetStickyTaskQueue(stickyTl)

	addWorkflowExecutionStartedEvent(ms, we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)

	wfMs := workflow.TestCloneToProto(context.Background(), ms)

	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	request := historyservice.RecordWorkflowTaskStartedRequest_builder{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: we,
		ScheduledEventId:  2,
		RequestId:         "reqId",
		PollRequest: workflowservice.PollWorkflowTaskQueueRequest_builder{
			TaskQueue: taskqueuepb.TaskQueue_builder{
				Name: stickyTl,
			}.Build(),
			Identity: identity,
		}.Build(),
	}.Build()

	expectedResponse := &historyservice.RecordWorkflowTaskStartedResponseWithRawHistory{}
	expectedResponse.SetWorkflowType(ms.GetWorkflowType())
	executionInfo = ms.GetExecutionInfo()
	if executionInfo.GetLastCompletedWorkflowTaskStartedEventId() != common.EmptyEventID {
		expectedResponse.SetPreviousStartedEventId(executionInfo.GetLastCompletedWorkflowTaskStartedEventId())
	}
	expectedResponse.SetScheduledEventId(wt.ScheduledEventID)
	expectedResponse.SetScheduledTime(timestamppb.New(wt.ScheduledTime))
	expectedResponse.SetStartedEventId(wt.ScheduledEventID + 1)
	expectedResponse.SetStickyExecutionEnabled(true)
	expectedResponse.SetNextEventId(ms.GetNextEventID() + 1)
	expectedResponse.SetAttempt(wt.Attempt)
	expectedResponse.SetWorkflowExecutionTaskQueue(taskqueuepb.TaskQueue_builder{
		Name: executionInfo.GetTaskQueue(),
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}.Build())
	expectedResponse.BranchToken, _ = ms.GetCurrentBranchToken()
	expectedResponse.SetRawHistory([][]byte{historyBlob})
	expectedResponse.SetNextPageToken(nil)

	response, err := s.historyEngine.RecordWorkflowTaskStarted(context.Background(), request)
	s.Nil(err)
	s.NotNil(response)
	s.True(response.GetStartedTime().AsTime().After(expectedResponse.GetScheduledTime().AsTime()))
	expectedResponse.SetStartedTime(response.GetStartedTime())
	s.Equal(expectedResponse, response)
}

func (s *engine3Suite) TestStartWorkflowExecution_BrandNew() {
	testNamespaceEntry := namespace.NewLocalNamespaceForTest(
		persistencespb.NamespaceInfo_builder{Id: tests.NamespaceID.String()}.Build(), persistencespb.NamespaceConfig_builder{Retention: timestamp.DurationFromDays(1)}.Build(), "active",
	)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()

	namespaceID := tests.NamespaceID
	workflowID := "workflowID"
	workflowType := "workflowType"
	taskQueue := "testTaskQueue"
	identity := "testIdentity"

	s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.CreateWorkflowExecutionResponse, nil)

	requestID := uuid.NewString()
	resp, err := s.historyEngine.StartWorkflowExecution(context.Background(), historyservice.StartWorkflowExecutionRequest_builder{
		Attempt:     1,
		NamespaceId: namespaceID.String(),
		StartRequest: workflowservice.StartWorkflowExecutionRequest_builder{
			Namespace:                namespaceID.String(),
			WorkflowId:               workflowID,
			WorkflowType:             commonpb.WorkflowType_builder{Name: workflowType}.Build(),
			TaskQueue:                taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build(),
			WorkflowExecutionTimeout: durationpb.New(1 * time.Second),
			WorkflowTaskTimeout:      durationpb.New(2 * time.Second),
			Identity:                 identity,
			RequestId:                requestID,
		}.Build(),
	}.Build())
	s.Nil(err)
	s.NotNil(resp.GetRunId())
}

func (s *engine3Suite) TestSignalWithStartWorkflowExecution_JustSignal() {
	testNamespaceEntry := namespace.NewLocalNamespaceForTest(
		persistencespb.NamespaceInfo_builder{Id: tests.NamespaceID.String()}.Build(), persistencespb.NamespaceConfig_builder{Retention: timestamp.DurationFromDays(1)}.Build(), "active",
	)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()
	workflowID := "wId"
	sRequest := historyservice.SignalWithStartWorkflowExecutionRequest_builder{
		SignalWithStartRequest: workflowservice.SignalWithStartWorkflowExecutionRequest_builder{
			WorkflowId: workflowID,
		}.Build(),
	}.Build()
	_, err := s.historyEngine.SignalWithStartWorkflowExecution(context.Background(), sRequest)
	s.EqualError(err, "Missing namespace UUID.")

	namespaceID := tests.NamespaceID

	workflowType := "workflowType"
	runID := tests.RunID
	taskQueue := "testTaskQueue"
	identity := "testIdentity"
	signalName := "my signal name"
	input := payloads.EncodeString("test input")
	requestID := uuid.NewString()
	sRequest = historyservice.SignalWithStartWorkflowExecutionRequest_builder{
		NamespaceId: namespaceID.String(),
		SignalWithStartRequest: workflowservice.SignalWithStartWorkflowExecutionRequest_builder{
			Namespace:    namespaceID.String(),
			WorkflowId:   workflowID,
			WorkflowType: commonpb.WorkflowType_builder{Name: workflowType}.Build(),
			TaskQueue:    taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build(),
			Identity:     identity,
			SignalName:   signalName,
			Input:        input,
			RequestId:    requestID,
		}.Build(),
	}.Build()

	ms := workflow.TestLocalMutableState(s.historyEngine.shardContext, s.mockEventsCache, tests.LocalNamespaceEntry,
		workflowID, runID, log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, commonpb.WorkflowExecution_builder{
		WorkflowId: workflowID,
		RunId:      runID,
	}.Build(), "wType", "testTaskQueue", payloads.EncodeString("input"), 25*time.Second, 20*time.Second, 200*time.Second, identity)
	_ = addWorkflowTaskScheduledEvent(ms)
	wfMs := workflow.TestCloneToProto(context.Background(), ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: runID}

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(gceResponse, nil)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	resp, err := s.historyEngine.SignalWithStartWorkflowExecution(context.Background(), sRequest)
	s.Nil(err)
	s.Equal(runID, resp.GetRunId())
}

func (s *engine3Suite) TestSignalWithStartWorkflowExecution_WorkflowNotExist() {
	testNamespaceEntry := namespace.NewLocalNamespaceForTest(
		persistencespb.NamespaceInfo_builder{Id: tests.NamespaceID.String()}.Build(), persistencespb.NamespaceConfig_builder{Retention: timestamp.DurationFromDays(1)}.Build(), "active",
	)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()

	workflowID := "wId"
	sRequest := historyservice.SignalWithStartWorkflowExecutionRequest_builder{
		SignalWithStartRequest: workflowservice.SignalWithStartWorkflowExecutionRequest_builder{
			WorkflowId: workflowID,
		}.Build(),
	}.Build()
	_, err := s.historyEngine.SignalWithStartWorkflowExecution(context.Background(), sRequest)
	s.EqualError(err, "Missing namespace UUID.")

	namespaceID := tests.NamespaceID
	workflowType := "workflowType"
	taskQueue := "testTaskQueue"
	identity := "testIdentity"
	signalName := "my signal name"
	input := payloads.EncodeString("test input")
	requestID := uuid.NewString()
	sRequest = historyservice.SignalWithStartWorkflowExecutionRequest_builder{
		NamespaceId: namespaceID.String(),
		SignalWithStartRequest: workflowservice.SignalWithStartWorkflowExecutionRequest_builder{
			Namespace:                namespaceID.String(),
			WorkflowId:               workflowID,
			WorkflowType:             commonpb.WorkflowType_builder{Name: workflowType}.Build(),
			TaskQueue:                taskqueuepb.TaskQueue_builder{Name: taskQueue}.Build(),
			WorkflowExecutionTimeout: durationpb.New(1 * time.Second),
			WorkflowTaskTimeout:      durationpb.New(2 * time.Second),
			Identity:                 identity,
			SignalName:               signalName,
			Input:                    input,
			RequestId:                requestID,
		}.Build(),
	}.Build()

	notExistErr := serviceerror.NewNotFound("Workflow not exist")

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(nil, notExistErr)
	s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.CreateWorkflowExecutionResponse, nil)

	resp, err := s.historyEngine.SignalWithStartWorkflowExecution(context.Background(), sRequest)
	s.Nil(err)
	s.NotNil(resp.GetRunId())
}
