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

	h "github.com/uber/cadence/.gen/go/history"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/history/config"
	"github.com/uber/cadence/service/history/constants"
	"github.com/uber/cadence/service/history/events"
	"github.com/uber/cadence/service/history/execution"
	"github.com/uber/cadence/service/history/queue"
	"github.com/uber/cadence/service/history/shard"
	test "github.com/uber/cadence/service/history/testing"
)

type (
	engine3Suite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		mockShard           *shard.TestContext
		mockTxProcessor     *queue.MockProcessor
		mockTimerProcessor  *queue.MockProcessor
		mockEventsCache     *events.MockCache
		mockDomainCache     *cache.MockDomainCache
		mockClusterMetadata *cluster.MockMetadata

		historyEngine    *historyEngineImpl
		mockExecutionMgr *mocks.ExecutionManager
		mockHistoryV2Mgr *mocks.HistoryV2Manager

		config *config.Config
		logger log.Logger
	}
)

func TestEngine3Suite(t *testing.T) {
	s := new(engine3Suite)
	suite.Run(t, s)
}

func (s *engine3Suite) SetupSuite() {
	s.config = config.NewForTest()
}

func (s *engine3Suite) TearDownSuite() {
}

func (s *engine3Suite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockTxProcessor = queue.NewMockProcessor(s.controller)
	s.mockTimerProcessor = queue.NewMockProcessor(s.controller)
	s.mockTxProcessor.EXPECT().NotifyNewTask(gomock.Any(), gomock.Any()).AnyTimes()
	s.mockTimerProcessor.EXPECT().NotifyNewTask(gomock.Any(), gomock.Any()).AnyTimes()

	s.mockShard = shard.NewTestContext(
		s.controller,
		&p.ShardInfo{
			ShardID:          0,
			RangeID:          1,
			TransferAckLevel: 0,
		},
		s.config,
	)

	s.mockExecutionMgr = s.mockShard.Resource.ExecutionMgr
	s.mockHistoryV2Mgr = s.mockShard.Resource.HistoryMgr
	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata
	s.mockDomainCache = s.mockShard.Resource.DomainCache
	s.mockEventsCache = s.mockShard.MockEventsCache

	s.mockClusterMetadata.EXPECT().IsGlobalDomainEnabled().Return(false).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(common.EmptyVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

	s.logger = s.mockShard.GetLogger()

	h := &historyEngineImpl{
		currentClusterName:   s.mockShard.GetClusterMetadata().GetCurrentClusterName(),
		shard:                s.mockShard,
		clusterMetadata:      s.mockClusterMetadata,
		executionManager:     s.mockExecutionMgr,
		historyV2Mgr:         s.mockHistoryV2Mgr,
		executionCache:       execution.NewCache(s.mockShard),
		logger:               s.logger,
		throttledLogger:      s.logger,
		metricsClient:        metrics.NewClient(tally.NoopScope, metrics.History),
		tokenSerializer:      common.NewJSONTaskTokenSerializer(),
		config:               s.config,
		timeSource:           s.mockShard.GetTimeSource(),
		historyEventNotifier: events.NewNotifier(clock.NewRealTimeSource(), metrics.NewClient(tally.NoopScope, metrics.History), func(string) int { return 0 }),
		txProcessor:          s.mockTxProcessor,
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
		&p.DomainInfo{ID: constants.TestDomainID}, &p.DomainConfig{Retention: 1}, "", nil,
	)
	s.mockDomainCache.EXPECT().GetDomainByID(gomock.Any()).Return(testDomainEntry, nil).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomain(gomock.Any()).Return(testDomainEntry, nil).AnyTimes()

	domainID := constants.TestDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(constants.TestRunID),
	}
	tl := "testTaskList"
	stickyTl := "stickyTaskList"
	identity := "testIdentity"

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.historyEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	executionInfo := msBuilder.GetExecutionInfo()
	executionInfo.LastUpdatedTimestamp = time.Now()
	executionInfo.StickyTaskList = stickyTl

	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)

	ms := execution.CreatePersistenceMutableState(msBuilder)

	gwmsResponse := &p.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{
		MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{},
	}, nil).Once()

	request := h.RecordDecisionTaskStartedRequest{
		DomainUUID:        common.StringPtr(domainID),
		WorkflowExecution: &we,
		ScheduleId:        common.Int64Ptr(2),
		TaskId:            common.Int64Ptr(100),
		RequestId:         common.StringPtr("reqId"),
		PollRequest: &workflow.PollForDecisionTaskRequest{
			TaskList: &workflow.TaskList{
				Name: common.StringPtr(stickyTl),
			},
			Identity: common.StringPtr(identity),
		},
	}

	expectedResponse := h.RecordDecisionTaskStartedResponse{}
	expectedResponse.WorkflowType = msBuilder.GetWorkflowType()
	executionInfo = msBuilder.GetExecutionInfo()
	if executionInfo.LastProcessedEvent != common.EmptyEventID {
		expectedResponse.PreviousStartedEventId = common.Int64Ptr(executionInfo.LastProcessedEvent)
	}
	expectedResponse.ScheduledEventId = common.Int64Ptr(di.ScheduleID)
	expectedResponse.StartedEventId = common.Int64Ptr(di.ScheduleID + 1)
	expectedResponse.StickyExecutionEnabled = common.BoolPtr(true)
	expectedResponse.NextEventId = common.Int64Ptr(msBuilder.GetNextEventID() + 1)
	expectedResponse.Attempt = common.Int64Ptr(di.Attempt)
	expectedResponse.WorkflowExecutionTaskList = common.TaskListPtr(workflow.TaskList{
		Name: &executionInfo.TaskList,
		Kind: common.TaskListKindPtr(workflow.TaskListKindNormal),
	})
	expectedResponse.BranchToken = msBuilder.GetExecutionInfo().BranchToken

	response, err := s.historyEngine.RecordDecisionTaskStarted(context.Background(), &request)
	s.Nil(err)
	s.NotNil(response)
	expectedResponse.StartedTimestamp = response.StartedTimestamp
	expectedResponse.ScheduledTimestamp = common.Int64Ptr(0)
	expectedResponse.Queries = make(map[string]*workflow.WorkflowQuery)
	s.Equal(&expectedResponse, response)
}

func (s *engine3Suite) TestStartWorkflowExecution_BrandNew() {
	testDomainEntry := cache.NewLocalDomainCacheEntryForTest(
		&p.DomainInfo{ID: constants.TestDomainID}, &p.DomainConfig{Retention: 1}, "", nil,
	)
	s.mockDomainCache.EXPECT().GetDomainByID(gomock.Any()).Return(testDomainEntry, nil).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomain(gomock.Any()).Return(testDomainEntry, nil).AnyTimes()

	domainID := constants.TestDomainID
	workflowID := "workflowID"
	workflowType := "workflowType"
	taskList := "testTaskList"
	identity := "testIdentity"

	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(&p.CreateWorkflowExecutionResponse{}, nil).Once()

	requestID := uuid.New()
	resp, err := s.historyEngine.StartWorkflowExecution(context.Background(), &h.StartWorkflowExecutionRequest{
		DomainUUID: common.StringPtr(domainID),
		StartRequest: &workflow.StartWorkflowExecutionRequest{
			Domain:                              common.StringPtr(domainID),
			WorkflowId:                          common.StringPtr(workflowID),
			WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
			TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskList)},
			ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(1),
			TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(2),
			Identity:                            common.StringPtr(identity),
			RequestId:                           common.StringPtr(requestID),
		},
	})
	s.Nil(err)
	s.NotNil(resp.RunId)
}

func (s *engine3Suite) TestSignalWithStartWorkflowExecution_JustSignal() {
	testDomainEntry := cache.NewLocalDomainCacheEntryForTest(
		&p.DomainInfo{ID: constants.TestDomainID}, &p.DomainConfig{Retention: 1}, "", nil,
	)
	s.mockDomainCache.EXPECT().GetDomainByID(gomock.Any()).Return(testDomainEntry, nil).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomain(gomock.Any()).Return(testDomainEntry, nil).AnyTimes()

	sRequest := &h.SignalWithStartWorkflowExecutionRequest{}
	_, err := s.historyEngine.SignalWithStartWorkflowExecution(context.Background(), sRequest)
	s.EqualError(err, "BadRequestError{Message: Missing domain UUID.}")

	domainID := constants.TestDomainID
	workflowID := "wId"
	runID := constants.TestRunID
	identity := "testIdentity"
	signalName := "my signal name"
	input := []byte("test input")
	sRequest = &h.SignalWithStartWorkflowExecutionRequest{
		DomainUUID: common.StringPtr(domainID),
		SignalWithStartRequest: &workflow.SignalWithStartWorkflowExecutionRequest{
			Domain:     common.StringPtr(domainID),
			WorkflowId: common.StringPtr(workflowID),
			Identity:   common.StringPtr(identity),
			SignalName: common.StringPtr(signalName),
			Input:      input,
		},
	}

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.historyEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		runID,
		constants.TestLocalDomainEntry,
	)
	ms := execution.CreatePersistenceMutableState(msBuilder)
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
		&p.DomainInfo{ID: constants.TestDomainID}, &p.DomainConfig{Retention: 1}, "", nil,
	)
	s.mockDomainCache.EXPECT().GetDomainByID(gomock.Any()).Return(testDomainEntry, nil).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomain(gomock.Any()).Return(testDomainEntry, nil).AnyTimes()

	sRequest := &h.SignalWithStartWorkflowExecutionRequest{}
	_, err := s.historyEngine.SignalWithStartWorkflowExecution(context.Background(), sRequest)
	s.EqualError(err, "BadRequestError{Message: Missing domain UUID.}")

	domainID := constants.TestDomainID
	workflowID := "wId"
	workflowType := "workflowType"
	taskList := "testTaskList"
	identity := "testIdentity"
	signalName := "my signal name"
	input := []byte("test input")
	requestID := uuid.New()
	sRequest = &h.SignalWithStartWorkflowExecutionRequest{
		DomainUUID: common.StringPtr(domainID),
		SignalWithStartRequest: &workflow.SignalWithStartWorkflowExecutionRequest{
			Domain:                              common.StringPtr(domainID),
			WorkflowId:                          common.StringPtr(workflowID),
			WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
			TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskList)},
			ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(1),
			TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(2),
			Identity:                            common.StringPtr(identity),
			SignalName:                          common.StringPtr(signalName),
			Input:                               input,
			RequestId:                           common.StringPtr(requestID),
		},
	}

	notExistErr := &workflow.EntityNotExistsError{Message: "Workflow not exist"}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(nil, notExistErr).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(&p.CreateWorkflowExecutionResponse{}, nil).Once()

	resp, err := s.historyEngine.SignalWithStartWorkflowExecution(context.Background(), sRequest)
	s.Nil(err)
	s.NotNil(resp.GetRunId())
}
