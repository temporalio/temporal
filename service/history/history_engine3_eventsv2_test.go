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
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

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
		&persistencespb.ShardInfo{
			ShardId: 1,
			RangeId: 1,
		},
		s.config,
	)

	reg := hsm.NewRegistry()
	err := workflow.RegisterStateMachine(reg)
	s.NoError(err)
	s.mockShard.SetStateMachineRegistry(reg)

	s.mockShard.Resource.ShardMgr.EXPECT().AssertShardOwnership(gomock.Any(), gomock.Any()).AnyTimes()

	s.mockExecutionMgr = s.mockShard.Resource.ExecutionMgr
	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata
	s.mockNamespaceCache = s.mockShard.Resource.NamespaceCache
	s.mockEventsCache = s.mockShard.MockEventsCache
	s.mockVisibilityManager = s.mockShard.Resource.VisibilityManager

	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(false, common.EmptyVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()
	s.workflowCache = wcache.NewHostLevelCache(s.mockShard.GetConfig(), metrics.NoopMetricsHandler)
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
		tokenSerializer:    common.NewProtoTaskTokenSerializer(),
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
		{
			EventId:   int64(1),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		},
		{
			EventId:   int64(2),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
				WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
					SearchAttributes: &commonpb.SearchAttributes{
						IndexedFields: map[string]*commonpb.Payload{
							"CustomKeywordField":    payload.EncodeString("random-keyword"),
							"TemporalChangeVersion": payload.EncodeString("random-data"),
						},
					},
				},
			},
		},
		{
			EventId:   int64(3),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		},
	}

	s.mockExecutionMgr.EXPECT().ReadHistoryBranch(gomock.Any(), gomock.Any()).Return(&persistence.ReadHistoryBranchResponse{
		HistoryEvents: fakeHistory,
		NextPageToken: []byte{},
		Size:          1,
	}, nil)

	testNamespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: tests.NamespaceID.String()}, &persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)}, "",
	)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()

	s.mockNamespaceCache.EXPECT().GetNamespaceName(gomock.Any()).Return(tests.Namespace, nil)
	s.mockShard.Resource.SearchAttributesProvider.EXPECT().GetSearchAttributes(gomock.Any(), false).Return(searchattribute.TestNameTypeMap, nil)
	s.mockShard.Resource.SearchAttributesMapperProvider.EXPECT().GetMapper(tests.Namespace).
		Return(&searchattribute.TestMapper{Namespace: tests.Namespace.String()}, nil).AnyTimes()

	namespaceID := tests.NamespaceID
	we := &commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	stickyTl := "stickyTaskQueue"
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(s.historyEngine.shardContext, s.mockEventsCache, tests.LocalNamespaceEntry,
		we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	executionInfo := ms.GetExecutionInfo()
	executionInfo.LastUpdateTime = timestamp.TimeNowPtrUtc()
	executionInfo.StickyTaskQueue = stickyTl

	addWorkflowExecutionStartedEvent(ms, we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)

	wfMs := workflow.TestCloneToProto(ms)

	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	request := historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: we,
		ScheduledEventId:  2,
		RequestId:         "reqId",
		PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: stickyTl,
			},
			Identity: identity,
		},
	}

	expectedResponse := historyservice.RecordWorkflowTaskStartedResponse{}
	expectedResponse.WorkflowType = ms.GetWorkflowType()
	executionInfo = ms.GetExecutionInfo()
	if executionInfo.LastWorkflowTaskStartedEventId != common.EmptyEventID {
		expectedResponse.PreviousStartedEventId = executionInfo.LastWorkflowTaskStartedEventId
	}
	expectedResponse.ScheduledEventId = wt.ScheduledEventID
	expectedResponse.ScheduledTime = timestamppb.New(wt.ScheduledTime)
	expectedResponse.StartedEventId = wt.ScheduledEventID + 1
	expectedResponse.StickyExecutionEnabled = true
	expectedResponse.NextEventId = ms.GetNextEventID() + 1
	expectedResponse.Attempt = wt.Attempt
	expectedResponse.WorkflowExecutionTaskQueue = &taskqueuepb.TaskQueue{
		Name: executionInfo.TaskQueue,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}
	expectedResponse.BranchToken, _ = ms.GetCurrentBranchToken()
	expectedResponse.History = &historypb.History{Events: fakeHistory}
	expectedResponse.NextPageToken = nil

	response, err := s.historyEngine.RecordWorkflowTaskStarted(context.Background(), &request)
	s.Nil(err)
	s.NotNil(response)
	s.True(response.StartedTime.AsTime().After(expectedResponse.ScheduledTime.AsTime()))
	expectedResponse.StartedTime = response.StartedTime
	s.Equal(&expectedResponse, response)
}

func (s *engine3Suite) TestStartWorkflowExecution_BrandNew() {
	testNamespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: tests.NamespaceID.String()}, &persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)}, "",
	)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()

	namespaceID := tests.NamespaceID
	workflowID := "workflowID"
	workflowType := "workflowType"
	taskQueue := "testTaskQueue"
	identity := "testIdentity"

	s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.CreateWorkflowExecutionResponse, nil)

	requestID := uuid.New()
	resp, err := s.historyEngine.StartWorkflowExecution(context.Background(), &historyservice.StartWorkflowExecutionRequest{
		Attempt:     1,
		NamespaceId: namespaceID.String(),
		StartRequest: &workflowservice.StartWorkflowExecutionRequest{
			Namespace:                namespaceID.String(),
			WorkflowId:               workflowID,
			WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
			TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueue},
			WorkflowExecutionTimeout: durationpb.New(1 * time.Second),
			WorkflowTaskTimeout:      durationpb.New(2 * time.Second),
			Identity:                 identity,
			RequestId:                requestID,
		},
	})
	s.Nil(err)
	s.NotNil(resp.RunId)
}

func (s *engine3Suite) TestSignalWithStartWorkflowExecution_JustSignal() {
	testNamespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: tests.NamespaceID.String()}, &persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)}, "",
	)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()

	sRequest := &historyservice.SignalWithStartWorkflowExecutionRequest{}
	_, err := s.historyEngine.SignalWithStartWorkflowExecution(context.Background(), sRequest)
	s.EqualError(err, "Missing namespace UUID.")

	namespaceID := tests.NamespaceID
	workflowID := "wId"
	workflowType := "workflowType"
	runID := tests.RunID
	taskQueue := "testTaskQueue"
	identity := "testIdentity"
	signalName := "my signal name"
	input := payloads.EncodeString("test input")
	requestID := uuid.New()
	sRequest = &historyservice.SignalWithStartWorkflowExecutionRequest{
		NamespaceId: namespaceID.String(),
		SignalWithStartRequest: &workflowservice.SignalWithStartWorkflowExecutionRequest{
			Namespace:    namespaceID.String(),
			WorkflowId:   workflowID,
			WorkflowType: &commonpb.WorkflowType{Name: workflowType},
			TaskQueue:    &taskqueuepb.TaskQueue{Name: taskQueue},
			Identity:     identity,
			SignalName:   signalName,
			Input:        input,
			RequestId:    requestID,
		},
	}

	ms := workflow.TestLocalMutableState(s.historyEngine.shardContext, s.mockEventsCache, tests.LocalNamespaceEntry,
		workflowID, runID, log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}, "wType", "testTaskQueue", payloads.EncodeString("input"), 25*time.Second, 20*time.Second, 200*time.Second, identity)
	_ = addWorkflowTaskScheduledEvent(ms)
	wfMs := workflow.TestCloneToProto(ms)
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
		&persistencespb.NamespaceInfo{Id: tests.NamespaceID.String()}, &persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)}, "",
	)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()

	sRequest := &historyservice.SignalWithStartWorkflowExecutionRequest{}
	_, err := s.historyEngine.SignalWithStartWorkflowExecution(context.Background(), sRequest)
	s.EqualError(err, "Missing namespace UUID.")

	namespaceID := tests.NamespaceID
	workflowID := "wId"
	workflowType := "workflowType"
	taskQueue := "testTaskQueue"
	identity := "testIdentity"
	signalName := "my signal name"
	input := payloads.EncodeString("test input")
	requestID := uuid.New()
	sRequest = &historyservice.SignalWithStartWorkflowExecutionRequest{
		NamespaceId: namespaceID.String(),
		SignalWithStartRequest: &workflowservice.SignalWithStartWorkflowExecutionRequest{
			Namespace:                namespaceID.String(),
			WorkflowId:               workflowID,
			WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
			TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueue},
			WorkflowExecutionTimeout: durationpb.New(1 * time.Second),
			WorkflowTaskTimeout:      durationpb.New(2 * time.Second),
			Identity:                 identity,
			SignalName:               signalName,
			Input:                    input,
			RequestId:                requestID,
		},
	}

	notExistErr := serviceerror.NewNotFound("Workflow not exist")

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(nil, notExistErr)
	s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.CreateWorkflowExecutionResponse, nil)

	resp, err := s.historyEngine.SignalWithStartWorkflowExecution(context.Background(), sRequest)
	s.Nil(err)
	s.NotNil(resp.GetRunId())
}
