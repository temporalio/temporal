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
	querypb "go.temporal.io/api/query/v1"
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
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
)

type (
	engine3Suite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		mockShard           *shard.ContextTest
		mockTxProcessor     *MocktransferQueueProcessor
		mockTimerProcessor  *MocktimerQueueProcessor
		mockEventsCache     *events.MockCache
		mockNamespaceCache  *namespace.MockRegistry
		mockClusterMetadata *cluster.MockMetadata

		historyEngine    *historyEngineImpl
		mockExecutionMgr *persistence.MockExecutionManager

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
	s.mockTxProcessor = NewMocktransferQueueProcessor(s.controller)
	s.mockTimerProcessor = NewMocktimerQueueProcessor(s.controller)
	s.mockTxProcessor.EXPECT().NotifyNewTask(gomock.Any(), gomock.Any()).AnyTimes()
	s.mockTimerProcessor.EXPECT().NotifyNewTimers(gomock.Any(), gomock.Any()).AnyTimes()

	s.mockShard = shard.NewTestContext(
		s.controller,
		&p.ShardInfoWithFailover{ShardInfo: &persistencespb.ShardInfo{
			ShardId:          1,
			RangeId:          1,
			TransferAckLevel: 0,
		}},
		s.config,
	)

	s.mockExecutionMgr = s.mockShard.Resource.ExecutionMgr
	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata
	s.mockNamespaceCache = s.mockShard.Resource.NamespaceCache
	s.mockEventsCache = s.mockShard.MockEventsCache

	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(false, common.EmptyVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()

	s.logger = s.mockShard.GetLogger()

	historyCache := workflow.NewCache(s.mockShard)
	h := &historyEngineImpl{
		currentClusterName: s.mockShard.GetClusterMetadata().GetCurrentClusterName(),
		shard:              s.mockShard,
		clusterMetadata:    s.mockClusterMetadata,
		executionManager:   s.mockExecutionMgr,
		historyCache:       historyCache,
		logger:             s.logger,
		throttledLogger:    s.logger,
		metricsClient:      metrics.NewNoopMetricsClient(),
		tokenSerializer:    common.NewProtoTaskTokenSerializer(),
		config:             s.config,
		timeSource:         s.mockShard.GetTimeSource(),
		eventNotifier:      events.NewNotifier(clock.NewRealTimeSource(), metrics.NewNoopMetricsClient(), func(namespace.ID, string) int32 { return 1 }),
		txProcessor:        s.mockTxProcessor,
		timerProcessor:     s.mockTimerProcessor,
	}
	s.mockShard.SetEngineForTesting(h)
	h.workflowTaskHandler = newWorkflowTaskHandlerCallback(h)

	s.historyEngine = h
}

func (s *engine3Suite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *engine3Suite) TestRecordWorkflowTaskStartedSuccessStickyEnabled() {
	testNamespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: tests.NamespaceID.String()}, &persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)}, "",
	)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	stickyTl := "stickyTaskQueue"
	identity := "testIdentity"

	msBuilder := workflow.TestLocalMutableState(s.historyEngine.shard, s.mockEventsCache, tests.LocalNamespaceEntry,
		log.NewTestLogger(), we.GetRunId())
	executionInfo := msBuilder.GetExecutionInfo()
	executionInfo.LastUpdateTime = timestamp.TimeNowPtrUtc()
	executionInfo.StickyTaskQueue = stickyTl

	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	di := addWorkflowTaskScheduledEvent(msBuilder)

	ms := workflow.TestCloneToProto(msBuilder)

	gwmsResponse := &p.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	request := historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:       namespaceID.String(),
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
	if executionInfo.LastWorkflowTaskStartId != common.EmptyEventID {
		expectedResponse.PreviousStartedEventId = executionInfo.LastWorkflowTaskStartId
	}
	expectedResponse.ScheduledEventId = di.ScheduleID
	expectedResponse.ScheduledTime = di.ScheduledTime
	expectedResponse.StartedEventId = di.ScheduleID + 1
	expectedResponse.StickyExecutionEnabled = true
	expectedResponse.NextEventId = msBuilder.GetNextEventID() + 1
	expectedResponse.Attempt = di.Attempt
	expectedResponse.WorkflowExecutionTaskQueue = &taskqueuepb.TaskQueue{
		Name: executionInfo.TaskQueue,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}
	expectedResponse.BranchToken, _ = msBuilder.GetCurrentBranchToken()

	response, err := s.historyEngine.RecordWorkflowTaskStarted(context.Background(), &request)
	s.Nil(err)
	s.NotNil(response)
	s.True(response.StartedTime.After(*expectedResponse.ScheduledTime))
	expectedResponse.StartedTime = response.StartedTime
	expectedResponse.Queries = make(map[string]*querypb.WorkflowQuery)
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

	s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any()).Return(tests.CreateWorkflowExecutionResponse, nil)

	requestID := uuid.New()
	resp, err := s.historyEngine.StartWorkflowExecution(context.Background(), &historyservice.StartWorkflowExecutionRequest{
		Attempt:     1,
		NamespaceId: namespaceID.String(),
		StartRequest: &workflowservice.StartWorkflowExecutionRequest{
			Namespace:                namespaceID.String(),
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
	runID := tests.RunID
	identity := "testIdentity"
	signalName := "my signal name"
	input := payloads.EncodeString("test input")
	sRequest = &historyservice.SignalWithStartWorkflowExecutionRequest{
		NamespaceId: namespaceID.String(),
		SignalWithStartRequest: &workflowservice.SignalWithStartWorkflowExecutionRequest{
			Namespace:  namespaceID.String(),
			WorkflowId: workflowID,
			Identity:   identity,
			SignalName: signalName,
			Input:      input,
		},
	}

	msBuilder := workflow.TestLocalMutableState(s.historyEngine.shard, s.mockEventsCache, tests.LocalNamespaceEntry,
		log.NewTestLogger(), runID)
	ms := workflow.TestCloneToProto(msBuilder)
	gwmsResponse := &p.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &p.GetCurrentExecutionResponse{RunID: runID}

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any()).Return(gceResponse, nil)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

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
			WorkflowExecutionTimeout: timestamp.DurationPtr(1 * time.Second),
			WorkflowTaskTimeout:      timestamp.DurationPtr(2 * time.Second),
			Identity:                 identity,
			SignalName:               signalName,
			Input:                    input,
			RequestId:                requestID,
		},
	}

	notExistErr := serviceerror.NewNotFound("Workflow not exist")

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any()).Return(nil, notExistErr)
	s.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any()).Return(tests.CreateWorkflowExecutionResponse, nil)

	resp, err := s.historyEngine.SignalWithStartWorkflowExecution(context.Background(), sRequest)
	s.Nil(err)
	s.NotNil(resp.GetRunId())
}
