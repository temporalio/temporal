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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/persistenceblobs/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/loggerimpl"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/mocks"
	"go.temporal.io/server/common/payloads"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
)

type (
	engine3Suite struct {
		suite.Suite
		*require.Assertions

		controller               *gomock.Controller
		mockShard                *shardContextTest
		mockTxProcessor          *MocktransferQueueProcessor
		mockReplicationProcessor *MockReplicatorQueueProcessor
		mockTimerProcessor       *MocktimerQueueProcessor
		mockEventsCache          *MockeventsCache
		mockNamespaceCache       *cache.MockNamespaceCache
		mockClusterMetadata      *cluster.MockMetadata

		historyEngine    *historyEngineImpl
		mockExecutionMgr *mocks.ExecutionManager
		mockHistoryV2Mgr *mocks.HistoryV2Manager

		config *Config
		logger log.Logger
	}
)

func TestEngine3Suite(t *testing.T) {
	s := new(engine3Suite)
	suite.Run(t, s)
}

func (s *engine3Suite) SetupSuite() {
	s.config = NewDynamicConfigForTest()
}

func (s *engine3Suite) TearDownSuite() {
}

func (s *engine3Suite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockTxProcessor = NewMocktransferQueueProcessor(s.controller)
	s.mockReplicationProcessor = NewMockReplicatorQueueProcessor(s.controller)
	s.mockTimerProcessor = NewMocktimerQueueProcessor(s.controller)
	s.mockTxProcessor.EXPECT().NotifyNewTask(gomock.Any(), gomock.Any()).AnyTimes()
	s.mockReplicationProcessor.EXPECT().notifyNewTask().AnyTimes()
	s.mockTimerProcessor.EXPECT().NotifyNewTimers(gomock.Any(), gomock.Any()).AnyTimes()

	s.mockShard = newTestShardContext(
		s.controller,
		&p.ShardInfoWithFailover{ShardInfo: &persistenceblobs.ShardInfo{
			ShardId:          1,
			RangeId:          1,
			TransferAckLevel: 0,
		}},
		s.config,
	)

	s.mockExecutionMgr = s.mockShard.resource.ExecutionMgr
	s.mockHistoryV2Mgr = s.mockShard.resource.HistoryMgr
	s.mockClusterMetadata = s.mockShard.resource.ClusterMetadata
	s.mockNamespaceCache = s.mockShard.resource.NamespaceCache
	s.mockEventsCache = s.mockShard.mockEventsCache

	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(common.EmptyVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockEventsCache.EXPECT().putEvent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

	s.logger = s.mockShard.GetLogger()

	historyCache := newHistoryCache(s.mockShard)
	h := &historyEngineImpl{
		currentClusterName:   s.mockShard.GetClusterMetadata().GetCurrentClusterName(),
		shard:                s.mockShard,
		clusterMetadata:      s.mockClusterMetadata,
		executionManager:     s.mockExecutionMgr,
		historyV2Mgr:         s.mockHistoryV2Mgr,
		historyCache:         historyCache,
		logger:               s.logger,
		throttledLogger:      s.logger,
		metricsClient:        metrics.NewClient(tally.NoopScope, metrics.History),
		tokenSerializer:      common.NewProtoTaskTokenSerializer(),
		config:               s.config,
		timeSource:           s.mockShard.GetTimeSource(),
		historyEventNotifier: newHistoryEventNotifier(clock.NewRealTimeSource(), metrics.NewClient(tally.NoopScope, metrics.History), func(string, string) int { return 1 }),
		txProcessor:          s.mockTxProcessor,
		replicatorProcessor:  s.mockReplicationProcessor,
		timerProcessor:       s.mockTimerProcessor,
	}
	s.mockShard.SetEngine(h)
	h.workflowTaskHandler = newWorkflowTaskHandlerCallback(h)

	s.historyEngine = h
}

func (s *engine3Suite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.Finish(s.T())
}

func (s *engine3Suite) TestRecordWorkflowTaskStartedSuccessStickyEnabled() {
	testNamespaceEntry := cache.NewLocalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Id: testNamespaceID}, &persistenceblobs.NamespaceConfig{Retention: timestamp.DurationFromDays(1)}, "", nil,
	)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()

	namespaceID := testNamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskQueue"
	stickyTl := "stickyTaskQueue"
	identity := "testIdentity"

	msBuilder := newMutableStateBuilderWithEventV2(s.historyEngine.shard, s.mockEventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	executionInfo := msBuilder.GetExecutionInfo()
	executionInfo.LastUpdatedTimestamp = time.Now()
	executionInfo.StickyTaskQueue = stickyTl

	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	di := addWorkflowTaskScheduledEvent(msBuilder)

	ms := createMutableState(msBuilder)

	gwmsResponse := &p.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{
		MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{},
	}, nil).Once()

	request := historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:       namespaceID,
		WorkflowExecution: &we,
		ScheduleId:        2,
		TaskId:            100,
		RequestId:         "reqId",
		PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: stickyTl,
			},
			Identity: identity,
		},
	}

	expectedResponse := historyservice.RecordWorkflowTaskStartedResponse{}
	expectedResponse.WorkflowType = msBuilder.GetWorkflowType()
	executionInfo = msBuilder.GetExecutionInfo()
	if executionInfo.LastProcessedEvent != common.EmptyEventID {
		expectedResponse.PreviousStartedEventId = executionInfo.LastProcessedEvent
	}
	expectedResponse.ScheduledEventId = di.ScheduleID
	expectedResponse.StartedEventId = di.ScheduleID + 1
	expectedResponse.StickyExecutionEnabled = true
	expectedResponse.NextEventId = msBuilder.GetNextEventID() + 1
	expectedResponse.Attempt = di.Attempt
	expectedResponse.WorkflowExecutionTaskQueue = &taskqueuepb.TaskQueue{
		Name: executionInfo.TaskQueue,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}
	expectedResponse.BranchToken = msBuilder.GetExecutionInfo().BranchToken

	response, err := s.historyEngine.RecordWorkflowTaskStarted(context.Background(), &request)
	s.Nil(err)
	s.NotNil(response)
	expectedResponse.StartedTime = response.StartedTime
	expectedResponse.ScheduledTime = &time.Time{}
	expectedResponse.Queries = make(map[string]*querypb.WorkflowQuery)
	s.Equal(&expectedResponse, response)
}

func (s *engine3Suite) TestStartWorkflowExecution_BrandNew() {
	testNamespaceEntry := cache.NewLocalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Id: testNamespaceID}, &persistenceblobs.NamespaceConfig{Retention: timestamp.DurationFromDays(1)}, "", nil,
	)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()

	namespaceID := testNamespaceID
	workflowID := "workflowID"
	workflowType := "workflowType"
	taskQueue := "testTaskQueue"
	identity := "testIdentity"

	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(&p.CreateWorkflowExecutionResponse{}, nil).Once()

	requestID := uuid.New()
	resp, err := s.historyEngine.StartWorkflowExecution(context.Background(), &historyservice.StartWorkflowExecutionRequest{
		Attempt:     1,
		NamespaceId: namespaceID,
		StartRequest: &workflowservice.StartWorkflowExecutionRequest{
			Namespace:                namespaceID,
			WorkflowId:               workflowID,
			WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
			TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueue},
			WorkflowExecutionTimeout: timestamp.DurationPtr(1 * time.Second),
			WorkflowTaskTimeout:      timestamp.DurationPtr(2 * time.Second),
			Identity:                 identity,
			RequestId:                requestID,
		},
	})
	s.Nil(err)
	s.NotNil(resp.RunId)
}

func (s *engine3Suite) TestSignalWithStartWorkflowExecution_JustSignal() {
	testNamespaceEntry := cache.NewLocalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Id: testNamespaceID}, &persistenceblobs.NamespaceConfig{Retention: timestamp.DurationFromDays(1)}, "", nil,
	)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()

	sRequest := &historyservice.SignalWithStartWorkflowExecutionRequest{}
	_, err := s.historyEngine.SignalWithStartWorkflowExecution(context.Background(), sRequest)
	s.EqualError(err, "Missing namespace UUID.")

	namespaceID := testNamespaceID
	workflowID := "wId"
	runID := testRunID
	identity := "testIdentity"
	signalName := "my signal name"
	input := payloads.EncodeString("test input")
	sRequest = &historyservice.SignalWithStartWorkflowExecutionRequest{
		NamespaceId: namespaceID,
		SignalWithStartRequest: &workflowservice.SignalWithStartWorkflowExecutionRequest{
			Namespace:  namespaceID,
			WorkflowId: workflowID,
			Identity:   identity,
			SignalName: signalName,
			Input:      input,
		},
	}

	msBuilder := newMutableStateBuilderWithEventV2(s.historyEngine.shard, s.mockEventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), runID)
	ms := createMutableState(msBuilder)
	gwmsResponse := &p.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &p.GetCurrentExecutionResponse{RunID: runID}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{
		MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{},
	}, nil).Once()

	resp, err := s.historyEngine.SignalWithStartWorkflowExecution(context.Background(), sRequest)
	s.Nil(err)
	s.Equal(runID, resp.GetRunId())
}

func (s *engine3Suite) TestSignalWithStartWorkflowExecution_WorkflowNotExist() {
	testNamespaceEntry := cache.NewLocalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Id: testNamespaceID}, &persistenceblobs.NamespaceConfig{Retention: timestamp.DurationFromDays(1)}, "", nil,
	)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()

	sRequest := &historyservice.SignalWithStartWorkflowExecutionRequest{}
	_, err := s.historyEngine.SignalWithStartWorkflowExecution(context.Background(), sRequest)
	s.EqualError(err, "Missing namespace UUID.")

	namespaceID := testNamespaceID
	workflowID := "wId"
	workflowType := "workflowType"
	taskQueue := "testTaskQueue"
	identity := "testIdentity"
	signalName := "my signal name"
	input := payloads.EncodeString("test input")
	requestID := uuid.New()
	sRequest = &historyservice.SignalWithStartWorkflowExecutionRequest{
		NamespaceId: namespaceID,
		SignalWithStartRequest: &workflowservice.SignalWithStartWorkflowExecutionRequest{
			Namespace:                namespaceID,
			WorkflowId:               workflowID,
			WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
			TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueue},
			WorkflowExecutionTimeout: timestamp.DurationPtr(1 * time.Second),
			WorkflowTaskTimeout:      timestamp.DurationPtr(2 * time.Second),
			Identity:                 identity,
			SignalName:               signalName,
			Input:                    input,
			RequestId:                requestID,
		},
	}

	notExistErr := serviceerror.NewNotFound("Workflow not exist")

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(nil, notExistErr).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(&p.CreateWorkflowExecutionResponse{}, nil).Once()

	resp, err := s.historyEngine.SignalWithStartWorkflowExecution(context.Background(), sRequest)
	s.Nil(err)
	s.NotNil(resp.GetRunId())
}
