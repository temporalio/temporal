// Copyright (c) 2017 Uber Technologies, Inc.
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
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/serviceerror"
	"go.temporal.io/temporal-proto/workflowservice"

	"github.com/temporalio/temporal/.gen/proto/historyservice"
	pblobs "github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/clock"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/loggerimpl"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/mocks"
	p "github.com/temporalio/temporal/common/persistence"
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
		mockDomainCache          *cache.MockDomainCache
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
		&p.ShardInfoWithFailover{ShardInfo: &pblobs.ShardInfo{
			ShardID:          0,
			RangeID:          1,
			TransferAckLevel: 0,
		}},
		s.config,
	)

	s.mockExecutionMgr = s.mockShard.resource.ExecutionMgr
	s.mockHistoryV2Mgr = s.mockShard.resource.HistoryMgr
	s.mockClusterMetadata = s.mockShard.resource.ClusterMetadata
	s.mockDomainCache = s.mockShard.resource.DomainCache
	s.mockEventsCache = s.mockShard.mockEventsCache

	s.mockClusterMetadata.EXPECT().IsGlobalDomainEnabled().Return(false).AnyTimes()
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
		historyEventNotifier: newHistoryEventNotifier(clock.NewRealTimeSource(), metrics.NewClient(tally.NoopScope, metrics.History), func(string) int { return 0 }),
		txProcessor:          s.mockTxProcessor,
		replicatorProcessor:  s.mockReplicationProcessor,
		timerProcessor:       s.mockTimerProcessor,
	}
	s.mockShard.SetEngine(h)
	h.decisionHandler = newDecisionHandler(h)

	s.historyEngine = h
}

func (s *engine3Suite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.Finish(s.T())
}

func (s *engine3Suite) TestRecordDecisionTaskStartedSuccessStickyEnabled() {
	testDomainEntry := cache.NewLocalDomainCacheEntryForTest(
		&p.DomainInfo{ID: testDomainID}, &p.DomainConfig{Retention: 1}, "", nil,
	)
	s.mockDomainCache.EXPECT().GetDomainByID(gomock.Any()).Return(testDomainEntry, nil).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomain(gomock.Any()).Return(testDomainEntry, nil).AnyTimes()

	domainID := testDomainID
	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	stickyTl := "stickyTaskList"
	identity := "testIdentity"

	msBuilder := newMutableStateBuilderWithEventV2(s.historyEngine.shard, s.mockEventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	executionInfo := msBuilder.GetExecutionInfo()
	executionInfo.LastUpdatedTimestamp = time.Now()
	executionInfo.StickyTaskList = stickyTl

	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)

	ms := createMutableState(msBuilder)

	gwmsResponse := &p.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{
		MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{},
	}, nil).Once()

	request := historyservice.RecordDecisionTaskStartedRequest{
		DomainUUID:        domainID,
		WorkflowExecution: &we,
		ScheduleId:        2,
		TaskId:            100,
		RequestId:         "reqId",
		PollRequest: &workflowservice.PollForDecisionTaskRequest{
			TaskList: &commonproto.TaskList{
				Name: stickyTl,
			},
			Identity: identity,
		},
	}

	expectedResponse := historyservice.RecordDecisionTaskStartedResponse{}
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
	expectedResponse.WorkflowExecutionTaskList = &commonproto.TaskList{
		Name: executionInfo.TaskList,
		Kind: enums.TaskListKindNormal,
	}
	expectedResponse.BranchToken = msBuilder.GetExecutionInfo().BranchToken

	response, err := s.historyEngine.RecordDecisionTaskStarted(context.Background(), &request)
	s.Nil(err)
	s.NotNil(response)
	expectedResponse.StartedTimestamp = response.StartedTimestamp
	expectedResponse.ScheduledTimestamp = 0
	expectedResponse.Queries = make(map[string]*commonproto.WorkflowQuery)
	s.Equal(&expectedResponse, response)
}

func (s *engine3Suite) TestStartWorkflowExecution_BrandNew() {
	testDomainEntry := cache.NewLocalDomainCacheEntryForTest(
		&p.DomainInfo{ID: testDomainID}, &p.DomainConfig{Retention: 1}, "", nil,
	)
	s.mockDomainCache.EXPECT().GetDomainByID(gomock.Any()).Return(testDomainEntry, nil).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomain(gomock.Any()).Return(testDomainEntry, nil).AnyTimes()

	domainID := testDomainID
	workflowID := "workflowID"
	workflowType := "workflowType"
	taskList := "testTaskList"
	identity := "testIdentity"

	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(&p.CreateWorkflowExecutionResponse{}, nil).Once()

	requestID := uuid.New()
	resp, err := s.historyEngine.StartWorkflowExecution(context.Background(), &historyservice.StartWorkflowExecutionRequest{
		DomainUUID: domainID,
		StartRequest: &workflowservice.StartWorkflowExecutionRequest{
			Domain:                              domainID,
			WorkflowId:                          workflowID,
			WorkflowType:                        &commonproto.WorkflowType{Name: workflowType},
			TaskList:                            &commonproto.TaskList{Name: taskList},
			ExecutionStartToCloseTimeoutSeconds: 1,
			TaskStartToCloseTimeoutSeconds:      2,
			Identity:                            identity,
			RequestId:                           requestID,
		},
	})
	s.Nil(err)
	s.NotNil(resp.RunId)
}

func (s *engine3Suite) TestSignalWithStartWorkflowExecution_JustSignal() {
	testDomainEntry := cache.NewLocalDomainCacheEntryForTest(
		&p.DomainInfo{ID: testDomainID}, &p.DomainConfig{Retention: 1}, "", nil,
	)
	s.mockDomainCache.EXPECT().GetDomainByID(gomock.Any()).Return(testDomainEntry, nil).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomain(gomock.Any()).Return(testDomainEntry, nil).AnyTimes()

	sRequest := &historyservice.SignalWithStartWorkflowExecutionRequest{}
	_, err := s.historyEngine.SignalWithStartWorkflowExecution(context.Background(), sRequest)
	s.EqualError(err, "Missing domain UUID.")

	domainID := testDomainID
	workflowID := "wId"
	runID := testRunID
	identity := "testIdentity"
	signalName := "my signal name"
	input := []byte("test input")
	sRequest = &historyservice.SignalWithStartWorkflowExecutionRequest{
		DomainUUID: domainID,
		SignalWithStartRequest: &workflowservice.SignalWithStartWorkflowExecutionRequest{
			Domain:     domainID,
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
	testDomainEntry := cache.NewLocalDomainCacheEntryForTest(
		&p.DomainInfo{ID: testDomainID}, &p.DomainConfig{Retention: 1}, "", nil,
	)
	s.mockDomainCache.EXPECT().GetDomainByID(gomock.Any()).Return(testDomainEntry, nil).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomain(gomock.Any()).Return(testDomainEntry, nil).AnyTimes()

	sRequest := &historyservice.SignalWithStartWorkflowExecutionRequest{}
	_, err := s.historyEngine.SignalWithStartWorkflowExecution(context.Background(), sRequest)
	s.EqualError(err, "Missing domain UUID.")

	domainID := testDomainID
	workflowID := "wId"
	workflowType := "workflowType"
	taskList := "testTaskList"
	identity := "testIdentity"
	signalName := "my signal name"
	input := []byte("test input")
	requestID := uuid.New()
	sRequest = &historyservice.SignalWithStartWorkflowExecutionRequest{
		DomainUUID: domainID,
		SignalWithStartRequest: &workflowservice.SignalWithStartWorkflowExecutionRequest{
			Domain:                              domainID,
			WorkflowId:                          workflowID,
			WorkflowType:                        &commonproto.WorkflowType{Name: workflowType},
			TaskList:                            &commonproto.TaskList{Name: taskList},
			ExecutionStartToCloseTimeoutSeconds: 1,
			TaskStartToCloseTimeoutSeconds:      2,
			Identity:                            identity,
			SignalName:                          signalName,
			Input:                               input,
			RequestId:                           requestID,
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
