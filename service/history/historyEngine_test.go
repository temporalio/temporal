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
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	"github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/history/historyservicetest"
	"github.com/uber/cadence/.gen/go/matching/matchingservicetest"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
	"github.com/uber/cadence/service/worker/archiver"
)

type (
	engineSuite struct {
		suite.Suite
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		controller               *gomock.Controller
		mockHistoryEngine        *historyEngineImpl
		mockArchivalClient       *archiver.ClientMock
		mockMatchingClient       *matchingservicetest.MockClient
		mockHistoryClient        *historyservicetest.MockClient
		mockDomainCache          *cache.DomainCacheMock
		mockVisibilityMgr        *mocks.VisibilityManager
		mockExecutionMgr         *mocks.ExecutionManager
		mockHistoryV2Mgr         *mocks.HistoryV2Manager
		mockShardManager         *mocks.ShardManager
		mockClusterMetadata      *mocks.ClusterMetadata
		mockProducer             *mocks.KafkaProducer
		mockClientBean           *client.MockClientBean
		mockEventsCache          *MockEventsCache
		mockMessagingClient      messaging.Client
		mockService              service.Service
		mockMetricClient         metrics.Client
		mockTxProcessor          *MockTransferQueueProcessor
		mockReplicationProcessor *MockReplicatorQueueProcessor
		mockTimerProcessor       *MockTimerQueueProcessor

		shardClosedCh chan int
		eventsCache   eventsCache
		config        *Config
		logger        log.Logger
	}
)

var testVersion = int64(1234)
var testDomainID = "deadbeef-0123-4567-890a-bcdef0123456"
var testDomainName = "some random domain name"
var testParentDomainID = "deadbeef-0123-4567-890a-bcdef0123457"
var testParentDomainName = "some random parent domain name"
var testTargetDomainID = "deadbeef-0123-4567-890a-bcdef0123458"
var testTargetDomainName = "some random target domain name"
var testChildDomainID = "deadbeef-0123-4567-890a-bcdef0123459"
var testChildDomainName = "some random child domain name"
var testRunID = "0d00698f-08e1-4d36-a3e2-3bf109f5d2d6"

var testLocalDomainEntry = cache.NewLocalDomainCacheEntryForTest(
	&p.DomainInfo{ID: testDomainID, Name: testDomainName},
	&p.DomainConfig{Retention: 1},
	cluster.TestCurrentClusterName,
	nil,
)

var testGlobalDomainEntry = cache.NewGlobalDomainCacheEntryForTest(
	&persistence.DomainInfo{ID: testDomainID, Name: testDomainName},
	&p.DomainConfig{Retention: 1},
	&persistence.DomainReplicationConfig{
		ActiveClusterName: cluster.TestCurrentClusterName,
		Clusters: []*persistence.ClusterReplicationConfig{
			{ClusterName: cluster.TestCurrentClusterName},
			{ClusterName: cluster.TestAlternativeClusterName},
		},
	},
	testVersion,
	nil,
)

var testLocalParentDomainEntry = cache.NewLocalDomainCacheEntryForTest(
	&persistence.DomainInfo{ID: testParentDomainID, Name: testParentDomainName},
	&p.DomainConfig{Retention: 1},
	cluster.TestCurrentClusterName,
	nil,
)

var testGlobalParentDomainEntry = cache.NewGlobalDomainCacheEntryForTest(
	&persistence.DomainInfo{ID: testParentDomainID, Name: testParentDomainName},
	&p.DomainConfig{Retention: 1},
	&persistence.DomainReplicationConfig{
		ActiveClusterName: cluster.TestCurrentClusterName,
		Clusters: []*persistence.ClusterReplicationConfig{
			{ClusterName: cluster.TestCurrentClusterName},
			{ClusterName: cluster.TestAlternativeClusterName},
		},
	},
	testVersion,
	nil,
)

var testLocalTargetDomainEntry = cache.NewLocalDomainCacheEntryForTest(
	&persistence.DomainInfo{ID: testTargetDomainID, Name: testTargetDomainName},
	&p.DomainConfig{Retention: 1},
	cluster.TestCurrentClusterName,
	nil,
)

var testGlobalTargetDomainEntry = cache.NewGlobalDomainCacheEntryForTest(
	&persistence.DomainInfo{ID: testTargetDomainID, Name: testTargetDomainName},
	&p.DomainConfig{Retention: 1},
	&persistence.DomainReplicationConfig{
		ActiveClusterName: cluster.TestCurrentClusterName,
		Clusters: []*persistence.ClusterReplicationConfig{
			{ClusterName: cluster.TestCurrentClusterName},
			{ClusterName: cluster.TestAlternativeClusterName},
		},
	},
	testVersion,
	nil,
)

var testLocalChildDomainEntry = cache.NewLocalDomainCacheEntryForTest(
	&persistence.DomainInfo{ID: testChildDomainID, Name: testChildDomainName},
	&p.DomainConfig{Retention: 1},
	cluster.TestCurrentClusterName,
	nil,
)

var testGlobalChildDomainEntry = cache.NewGlobalDomainCacheEntryForTest(
	&persistence.DomainInfo{ID: testChildDomainID, Name: testChildDomainName},
	&p.DomainConfig{Retention: 1},
	&persistence.DomainReplicationConfig{
		ActiveClusterName: cluster.TestCurrentClusterName,
		Clusters: []*persistence.ClusterReplicationConfig{
			{ClusterName: cluster.TestCurrentClusterName},
			{ClusterName: cluster.TestAlternativeClusterName},
		},
	},
	testVersion,
	nil,
)

func TestEngineSuite(t *testing.T) {
	s := new(engineSuite)
	suite.Run(t, s)
}

func (s *engineSuite) SetupSuite() {

	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)
	s.config = NewDynamicConfigForTest()
}

func (s *engineSuite) TearDownSuite() {

}

func (s *engineSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())

	shardID := 10
	s.controller = gomock.NewController(s.T())
	s.mockArchivalClient = &archiver.ClientMock{}
	s.mockMatchingClient = matchingservicetest.NewMockClient(s.controller)
	s.mockHistoryClient = historyservicetest.NewMockClient(s.controller)
	s.mockDomainCache = &cache.DomainCacheMock{}
	s.mockVisibilityMgr = &mocks.VisibilityManager{}
	s.mockExecutionMgr = &mocks.ExecutionManager{}
	s.mockHistoryV2Mgr = &mocks.HistoryV2Manager{}
	s.mockShardManager = &mocks.ShardManager{}
	s.mockClusterMetadata = &mocks.ClusterMetadata{}
	s.mockProducer = &mocks.KafkaProducer{}
	s.shardClosedCh = make(chan int, 100)
	s.mockMetricClient = metrics.NewClient(tally.NoopScope, metrics.History)
	s.mockMessagingClient = mocks.NewMockMessagingClient(s.mockProducer, nil)
	s.mockClientBean = &client.MockClientBean{}
	s.mockService = service.NewTestService(
		s.mockClusterMetadata,
		s.mockMessagingClient,
		s.mockMetricClient,
		s.mockClientBean,
		nil,
		nil,
		nil)
	s.mockEventsCache = &MockEventsCache{}

	historyEventNotifier := newHistoryEventNotifier(
		clock.NewRealTimeSource(),
		s.mockMetricClient,
		func(workflowID string) int {
			return len(workflowID)
		},
	)
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History)
	mockShard := &shardContextImpl{
		service:                   s.mockService,
		clusterMetadata:           s.mockClusterMetadata,
		shardInfo:                 &persistence.ShardInfo{ShardID: shardID, RangeID: 1, TransferAckLevel: 0},
		transferSequenceNumber:    1,
		executionManager:          s.mockExecutionMgr,
		historyV2Mgr:              s.mockHistoryV2Mgr,
		domainCache:               s.mockDomainCache,
		shardManager:              s.mockShardManager,
		maxTransferSequenceNumber: 100000,
		closeCh:                   s.shardClosedCh,
		config:                    s.config,
		logger:                    s.logger,
		throttledLogger:           s.logger,
		metricsClient:             metricsClient,
		timeSource:                clock.NewRealTimeSource(),
	}
	s.eventsCache = newEventsCache(mockShard)
	mockShard.eventsCache = s.eventsCache
	s.mockClusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", common.EmptyVersion).Return(cluster.TestCurrentClusterName)
	s.mockTxProcessor = &MockTransferQueueProcessor{}
	s.mockTxProcessor.On("NotifyNewTask", mock.Anything, mock.Anything).Maybe()
	s.mockReplicationProcessor = NewMockReplicatorQueueProcessor(s.controller)
	s.mockReplicationProcessor.EXPECT().notifyNewTask().AnyTimes()
	s.mockTimerProcessor = &MockTimerQueueProcessor{}
	s.mockTimerProcessor.On("NotifyNewTimers", mock.Anything, mock.Anything).Maybe()
	s.mockDomainCache.On("GetDomainByID", testDomainID).Return(testLocalDomainEntry, nil).Maybe()
	s.mockDomainCache.On("GetDomain", testDomainName).Return(testLocalDomainEntry, nil).Maybe()

	historyCache := newHistoryCache(mockShard)
	// this is used by shard context, not relevant to this test, so we do not care how many times "GetCurrentClusterName" os called
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", common.EmptyVersion).Return(cluster.TestCurrentClusterName)
	s.mockClusterMetadata.On("GetAllClusterInfo").Return(cluster.TestSingleDCClusterInfo)
	s.mockClusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	h := &historyEngineImpl{
		currentClusterName:   s.mockService.GetClusterMetadata().GetCurrentClusterName(),
		shard:                mockShard,
		clusterMetadata:      s.mockClusterMetadata,
		executionManager:     s.mockExecutionMgr,
		historyV2Mgr:         s.mockHistoryV2Mgr,
		historyCache:         historyCache,
		logger:               s.logger,
		metricsClient:        metricsClient,
		tokenSerializer:      common.NewJSONTaskTokenSerializer(),
		historyEventNotifier: historyEventNotifier,
		config:               NewDynamicConfigForTest(),
		archivalClient:       s.mockArchivalClient,
		txProcessor:          s.mockTxProcessor,
		replicatorProcessor:  s.mockReplicationProcessor,
		timerProcessor:       s.mockTimerProcessor,
	}
	mockShard.SetEngine(h)
	h.decisionHandler = newDecisionHandler(h)

	h.historyEventNotifier.Start()

	s.mockHistoryEngine = h
}

func (s *engineSuite) TearDownTest() {
	s.controller.Finish()
	s.mockHistoryEngine.historyEventNotifier.Stop()
	s.mockExecutionMgr.AssertExpectations(s.T())
	s.mockHistoryV2Mgr.AssertExpectations(s.T())
	s.mockShardManager.AssertExpectations(s.T())
	s.mockVisibilityMgr.AssertExpectations(s.T())
	s.mockProducer.AssertExpectations(s.T())
	s.mockClientBean.AssertExpectations(s.T())
	s.mockArchivalClient.AssertExpectations(s.T())
	s.mockTxProcessor.AssertExpectations(s.T())
	s.mockTimerProcessor.AssertExpectations(s.T())
}

func (s *engineSuite) TestGetMutableStateSync() {
	ctx := context.Background()

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("test-get-workflow-execution-event-id"),
		RunId:      common.StringPtr(testRunID),
	}
	tasklist := "testTaskList"
	identity := "testIdentity"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), execution.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, execution, "wType", tasklist, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tasklist, identity)
	ms := createMutableState(msBuilder)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	// right now the next event ID is 4
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gweResponse, nil).Once()

	// test get the next event ID instantly
	response, err := s.mockHistoryEngine.GetMutableState(ctx, &history.GetMutableStateRequest{
		DomainUUID: common.StringPtr(testDomainID),
		Execution:  &execution,
	})
	s.Nil(err)
	s.Equal(int64(4), response.GetNextEventId())
}

func (s *engineSuite) TestGetMutableState_IntestRunID() {
	ctx := context.Background()

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("test-get-workflow-execution-event-id"),
		RunId:      common.StringPtr("run-id-not-valid-uuid"),
	}

	_, err := s.mockHistoryEngine.GetMutableState(ctx, &history.GetMutableStateRequest{
		DomainUUID: common.StringPtr(testDomainID),
		Execution:  &execution,
	})
	s.Equal(errRunIDNotValid, err)
}

func (s *engineSuite) TestGetMutableState_EmptyRunID() {
	ctx := context.Background()

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("test-get-workflow-execution-event-id"),
	}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(nil, &workflow.EntityNotExistsError{}).Once()

	_, err := s.mockHistoryEngine.GetMutableState(ctx, &history.GetMutableStateRequest{
		DomainUUID: common.StringPtr(testDomainID),
		Execution:  &execution,
	})
	s.Equal(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestGetMutableStateLongPoll() {
	ctx := context.Background()

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("test-get-workflow-execution-event-id"),
		RunId:      common.StringPtr(testRunID),
	}
	tasklist := "testTaskList"
	identity := "testIdentity"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), execution.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, execution, "wType", tasklist, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tasklist, identity)
	ms := createMutableState(msBuilder)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	// right now the next event ID is 4
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gweResponse, nil).Once()

	// test long poll on next event ID change
	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(1)
	asycWorkflowUpdate := func(delay time.Duration) {
		taskToken, _ := json.Marshal(&common.TaskToken{
			WorkflowID: *execution.WorkflowId,
			RunID:      *execution.RunId,
			ScheduleID: 2,
		})
		s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
		s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

		timer := time.NewTimer(delay)

		<-timer.C
		_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
			DomainUUID: common.StringPtr(testDomainID),
			CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
				TaskToken: taskToken,
				Identity:  &identity,
			},
		})
		s.Nil(err)
		waitGroup.Done()
		// right now the next event ID is 5
	}

	// return immediately, since the expected next event ID appears
	response, err := s.mockHistoryEngine.GetMutableState(ctx, &history.GetMutableStateRequest{
		DomainUUID:          common.StringPtr(testDomainID),
		Execution:           &execution,
		ExpectedNextEventId: common.Int64Ptr(3),
	})
	s.Nil(err)
	s.Equal(int64(4), *response.NextEventId)

	// long poll, new event happen before long poll timeout
	go asycWorkflowUpdate(time.Second * 2)
	start := time.Now()
	pollResponse, err := s.mockHistoryEngine.PollMutableState(ctx, &history.PollMutableStateRequest{
		DomainUUID:          common.StringPtr(testDomainID),
		Execution:           &execution,
		ExpectedNextEventId: common.Int64Ptr(4),
	})
	s.True(time.Now().After(start.Add(time.Second * 1)))
	s.Nil(err)
	s.Equal(int64(5), pollResponse.GetNextEventId())
	waitGroup.Wait()
}

func (s *engineSuite) TestGetMutableStateLongPoll_CurrentBranchChanged() {
	ctx := context.Background()

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("test-get-workflow-execution-event-id"),
		RunId:      common.StringPtr(testRunID),
	}
	tasklist := "testTaskList"
	identity := "testIdentity"

	msBuilder := newMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		execution.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, execution, "wType", tasklist, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tasklist, identity)
	ms := createMutableState(msBuilder)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	// right now the next event ID is 4
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gweResponse, nil).Once()

	// test long poll on next event ID change
	asyncBranchTokenUpdate := func(delay time.Duration) {
		timer := time.NewTimer(delay)
		<-timer.C
		newExecution := &workflow.WorkflowExecution{
			WorkflowId: execution.WorkflowId,
			RunId:      execution.RunId,
		}
		s.mockHistoryEngine.historyEventNotifier.NotifyNewHistoryEvent(newHistoryEventNotification(
			"testDomainID",
			newExecution,
			int64(1),
			int64(4),
			int64(1),
			[]byte{1},
			persistence.WorkflowStateCreated,
			persistence.WorkflowCloseStatusNone))
	}

	// return immediately, since the expected next event ID appears
	response0, err := s.mockHistoryEngine.GetMutableState(ctx, &history.GetMutableStateRequest{
		DomainUUID:          common.StringPtr(testDomainID),
		Execution:           &execution,
		ExpectedNextEventId: common.Int64Ptr(3),
	})
	s.Nil(err)
	s.Equal(int64(4), response0.GetNextEventId())

	// long poll, new event happen before long poll timeout
	go asyncBranchTokenUpdate(time.Second * 2)
	start := time.Now()
	response1, err := s.mockHistoryEngine.GetMutableState(ctx, &history.GetMutableStateRequest{
		DomainUUID:          common.StringPtr(testDomainID),
		Execution:           &execution,
		ExpectedNextEventId: common.Int64Ptr(10),
	})
	s.True(time.Now().After(start.Add(time.Second * 1)))
	s.Nil(err)
	s.Equal(response0.GetCurrentBranchToken(), response1.GetCurrentBranchToken())
}

func (s *engineSuite) TestGetMutableStateLongPollTimeout() {
	ctx := context.Background()

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("test-get-workflow-execution-event-id"),
		RunId:      common.StringPtr(testRunID),
	}
	tasklist := "testTaskList"
	identity := "testIdentity"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), execution.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, execution, "wType", tasklist, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tasklist, identity)
	ms := createMutableState(msBuilder)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	// right now the next event ID is 4
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gweResponse, nil).Once()

	// long poll, no event happen after long poll timeout
	response, err := s.mockHistoryEngine.GetMutableState(ctx, &history.GetMutableStateRequest{
		DomainUUID:          common.StringPtr(testDomainID),
		Execution:           &execution,
		ExpectedNextEventId: common.Int64Ptr(4),
	})
	s.Nil(err)
	s.Equal(int64(4), response.GetNextEventId())
}

func (s *engineSuite) TestQueryWorkflow() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("test-get-workflow-execution-event-id"),
		RunId:      common.StringPtr(testRunID),
	}
	tasklist := "testTaskList"
	identity := "testIdentity"
	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache, loggerimpl.NewDevelopmentForTest(s.Suite), execution.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, execution, "wType", tasklist, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tasklist, identity)
	ms := createMutableState(msBuilder)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gweResponse, nil).Once()

	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(1)
	asyncQueryUpdate := func(delay time.Duration, answer []byte) {
		<-time.After(delay)
		builder := s.getBuilder(testDomainID, execution)
		s.NotNil(builder)
		queryRegistry := builder.GetQueryRegistry()
		buffered := queryRegistry.getBufferedSnapshot()
		for _, id := range buffered {
			changed, err := queryRegistry.recordEvent(id, queryEventStart, nil)
			s.True(changed)
			s.NoError(err)
		}
		started := queryRegistry.getStartedSnapshot()
		for _, id := range started {
			changed, err := queryRegistry.recordEvent(id, queryEventPersistenceConditionSatisfied, nil)
			s.False(changed)
			s.NoError(err)
			resultType := workflow.QueryResultTypeAnswered
			changed, err = queryRegistry.recordEvent(id, queryEventRecordResult, &workflow.WorkflowQueryResult{
				ResultType: &resultType,
				Answer:     answer,
			})
			s.True(changed)
			s.NoError(err)
			querySnapshot, err := queryRegistry.getQuerySnapshot(id)
			s.NoError(err)
			s.Equal(queryStateCompleted, querySnapshot.state)
		}
		waitGroup.Done()
	}
	request := &history.QueryWorkflowRequest{
		DomainUUID: common.StringPtr(testDomainID),
		Execution:  &execution,
		Query:      &workflow.WorkflowQuery{},
	}

	// time out because query is not completed in time
	resp, err := s.mockHistoryEngine.QueryWorkflow(context.Background(), request)
	s.Nil(resp)
	s.Equal(ErrQueryTimeout, err)

	go asyncQueryUpdate(time.Second*2, []byte{1, 2, 3})
	start := time.Now()
	resp, err = s.mockHistoryEngine.QueryWorkflow(context.Background(), request)
	s.True(time.Now().After(start.Add(time.Second)))
	s.NoError(err)
	s.Equal([]byte{1, 2, 3}, resp.GetQueryResult())
	waitGroup.Wait()
}

func (s *engineSuite) TestRespondDecisionTaskCompletedInvalidToken() {

	invalidToken, _ := json.Marshal("bad token")
	identity := "testIdentity"

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        invalidToken,
			Decisions:        nil,
			ExecutionContext: nil,
			Identity:         &identity,
		},
	})

	s.NotNil(err)
	s.IsType(&workflow.BadRequestError{}, err)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedIfNoExecution() {

	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      testRunID,
		ScheduleID: 2,
	})
	identity := "testIdentity"

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, &workflow.EntityNotExistsError{}).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedIfGetExecutionFailed() {

	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      testRunID,
		ScheduleID: 2,
	})
	identity := "testIdentity"

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, errors.New("FAILED")).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondDecisionTaskCompletedUpdateExecutionFailed() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"

	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 2,
	})
	identity := "testIdentity"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, errors.New("FAILED")).Once()
	s.mockShardManager.On("UpdateShard", mock.Anything).Return(nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.NotNil(err)
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondDecisionTaskCompletedIfTaskCompleted() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 2,
	})
	identity := "testIdentity"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	startedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, *startedEvent.EventId, nil, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedIfTaskNotStarted() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 2,
	})
	identity := "testIdentity"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	addDecisionTaskScheduledEvent(msBuilder)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken: taskToken,
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedConflictOnUpdate() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	identity := "testIdentity"
	executionContext := []byte("context")
	activity1ID := "activity1"
	activity1Type := "activity_type1"
	activity1Input := []byte("input1")
	activity1Result := []byte("activity1_result")
	activity2ID := "activity2"
	activity2Type := "activity_type2"
	activity2Input := []byte("input2")
	activity2Result := []byte("activity2_result")
	activity3ID := "activity3"
	activity3Type := "activity_type3"
	activity3Input := []byte("input3")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di1 := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent1 := addDecisionTaskStartedEvent(msBuilder, di1.ScheduleID, tl, identity)
	decisionCompletedEvent1 := addDecisionTaskCompletedEvent(msBuilder, di1.ScheduleID,
		*decisionStartedEvent1.EventId, nil, identity)
	activity1ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent1.EventId,
		activity1ID, activity1Type, tl, activity1Input, 100, 10, 5)
	activity2ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent1.EventId,
		activity2ID, activity2Type, tl, activity2Input, 100, 10, 5)
	activity1StartedEvent := addActivityTaskStartedEvent(msBuilder, *activity1ScheduledEvent.EventId, identity)
	activity2StartedEvent := addActivityTaskStartedEvent(msBuilder, *activity2ScheduledEvent.EventId, identity)
	addActivityTaskCompletedEvent(msBuilder, *activity1ScheduledEvent.EventId,
		*activity1StartedEvent.EventId, activity1Result, identity)
	di2 := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent2 := addDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)

	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      we.GetRunId(),
		ScheduleID: di2.ScheduleID,
	})

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
		ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
			ActivityId:                    common.StringPtr(activity3ID),
			ActivityType:                  &workflow.ActivityType{Name: common.StringPtr(activity3Type)},
			TaskList:                      &workflow.TaskList{Name: &tl},
			Input:                         activity3Input,
			ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(10),
			StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
			HeartbeatTimeoutSeconds:       common.Int32Ptr(5),
		},
	}}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	addActivityTaskCompletedEvent(msBuilder, *activity2ScheduledEvent.EventId,
		*activity2StartedEvent.EventId, activity2Result, identity)

	ms2 := createMutableState(msBuilder)
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: ms2}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}},
		&persistence.ConditionFailedError{}).Once()

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse2, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	s.Equal(int64(16), ms2.ExecutionInfo.NextEventID)
	s.Equal(*decisionStartedEvent2.EventId, ms2.ExecutionInfo.LastProcessedEvent)
	s.Equal(executionContext, ms2.ExecutionInfo.ExecutionContext)

	executionBuilder := s.getBuilder(testDomainID, we)
	activity3Attributes := s.getActivityScheduledEvent(executionBuilder, 13).ActivityTaskScheduledEventAttributes
	s.Equal(activity3ID, *activity3Attributes.ActivityId)
	s.Equal(activity3Type, *activity3Attributes.ActivityType.Name)
	s.Equal(int64(12), *activity3Attributes.DecisionTaskCompletedEventId)
	s.Equal(tl, *activity3Attributes.TaskList.Name)
	s.Equal(activity3Input, activity3Attributes.Input)
	s.Equal(int32(100), *activity3Attributes.ScheduleToCloseTimeoutSeconds)
	s.Equal(int32(10), *activity3Attributes.ScheduleToStartTimeoutSeconds)
	s.Equal(int32(50), *activity3Attributes.StartToCloseTimeoutSeconds)
	s.Equal(int32(5), *activity3Attributes.HeartbeatTimeoutSeconds)

	di, ok := executionBuilder.GetDecisionInfo(15)
	s.True(ok)
	s.Equal(int32(100), di.DecisionTimeout)
}

func (s *engineSuite) TestValidateSignalRequest() {
	workflowType := "testType"
	input := []byte("input")
	startRequest := &workflow.StartWorkflowExecutionRequest{
		WorkflowId:                          common.StringPtr("ID"),
		WorkflowType:                        &workflow.WorkflowType{Name: &workflowType},
		TaskList:                            &workflow.TaskList{Name: common.StringPtr("taskptr")},
		Input:                               input,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(10),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(10),
		Identity:                            common.StringPtr("identity"),
	}
	err := validateStartWorkflowExecutionRequest(startRequest, 999)
	s.Error(err, "startRequest doesn't have request id, it should error out")
}

func (s *engineSuite) TestRespondDecisionTaskCompletedMaxAttemptsExceeded() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 2,
	})
	identity := "testIdentity"
	executionContext := []byte("context")
	input := []byte("input")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
		ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
			ActivityId:                    common.StringPtr("activity1"),
			ActivityType:                  &workflow.ActivityType{Name: common.StringPtr("activity_type1")},
			TaskList:                      &workflow.TaskList{Name: &tl},
			Input:                         input,
			ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(10),
			StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
			HeartbeatTimeoutSeconds:       common.Int32Ptr(5),
		},
	}}

	for i := 0; i < conditionalRetryCount; i++ {
		ms := createMutableState(msBuilder)
		gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

		s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
		s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
		s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}},
			&persistence.ConditionFailedError{}).Once()
	}

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         &identity,
		},
	})
	s.NotNil(err)
	s.Equal(ErrMaxAttemptsExceeded, err)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedCompleteWorkflowFailed() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	identity := "testIdentity"
	executionContext := []byte("context")
	activity1ID := "activity1"
	activity1Type := "activity_type1"
	activity1Input := []byte("input1")
	activity1Result := []byte("activity1_result")
	activity2ID := "activity2"
	activity2Type := "activity_type2"
	activity2Input := []byte("input2")
	activity2Result := []byte("activity2_result")
	workflowResult := []byte("workflow result")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 25, 200, identity)
	di1 := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent1 := addDecisionTaskStartedEvent(msBuilder, di1.ScheduleID, tl, identity)
	decisionCompletedEvent1 := addDecisionTaskCompletedEvent(msBuilder, di1.ScheduleID,
		*decisionStartedEvent1.EventId, nil, identity)
	activity1ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent1.EventId,
		activity1ID, activity1Type, tl, activity1Input, 100, 10, 5)
	activity2ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent1.EventId,
		activity2ID, activity2Type, tl, activity2Input, 100, 10, 5)
	activity1StartedEvent := addActivityTaskStartedEvent(msBuilder, *activity1ScheduledEvent.EventId, identity)
	activity2StartedEvent := addActivityTaskStartedEvent(msBuilder, *activity2ScheduledEvent.EventId, identity)
	addActivityTaskCompletedEvent(msBuilder, *activity1ScheduledEvent.EventId,
		*activity1StartedEvent.EventId, activity1Result, identity)
	di2 := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)
	addActivityTaskCompletedEvent(msBuilder, *activity2ScheduledEvent.EventId,
		*activity2StartedEvent.EventId, activity2Result, identity)

	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: di2.ScheduleID,
	})

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
		CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
			Result: workflowResult,
		},
	}}

	for i := 0; i < 2; i++ {
		ms := createMutableState(msBuilder)
		gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
		s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	}

	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(testDomainID, we)
	s.Equal(int64(15), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(*decisionStartedEvent1.EventId, executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Empty(executionBuilder.GetExecutionInfo().ExecutionContext)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.True(executionBuilder.HasPendingDecision())
	di3, ok := executionBuilder.GetDecisionInfo(executionBuilder.GetExecutionInfo().NextEventID - 1)
	s.True(ok)
	s.Equal(executionBuilder.GetExecutionInfo().NextEventID-1, di3.ScheduleID)
	s.Equal(int64(0), di3.Attempt)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedFailWorkflowFailed() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	identity := "testIdentity"
	executionContext := []byte("context")
	activity1ID := "activity1"
	activity1Type := "activity_type1"
	activity1Input := []byte("input1")
	activity1Result := []byte("activity1_result")
	activity2ID := "activity2"
	activity2Type := "activity_type2"
	activity2Input := []byte("input2")
	activity2Result := []byte("activity2_result")
	reason := "workflow fail reason"
	details := []byte("workflow fail details")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 25, 200, identity)
	di1 := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent1 := addDecisionTaskStartedEvent(msBuilder, di1.ScheduleID, tl, identity)
	decisionCompletedEvent1 := addDecisionTaskCompletedEvent(msBuilder, di1.ScheduleID,
		*decisionStartedEvent1.EventId, nil, identity)
	activity1ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent1.EventId, activity1ID,
		activity1Type, tl, activity1Input, 100, 10, 5)
	activity2ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent1.EventId, activity2ID,
		activity2Type, tl, activity2Input, 100, 10, 5)
	activity1StartedEvent := addActivityTaskStartedEvent(msBuilder, *activity1ScheduledEvent.EventId, identity)
	activity2StartedEvent := addActivityTaskStartedEvent(msBuilder, *activity2ScheduledEvent.EventId, identity)
	addActivityTaskCompletedEvent(msBuilder, *activity1ScheduledEvent.EventId,
		*activity1StartedEvent.EventId, activity1Result, identity)
	di2 := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)
	addActivityTaskCompletedEvent(msBuilder, *activity2ScheduledEvent.EventId,
		*activity2StartedEvent.EventId, activity2Result, identity)

	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: di2.ScheduleID,
	})

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeFailWorkflowExecution),
		FailWorkflowExecutionDecisionAttributes: &workflow.FailWorkflowExecutionDecisionAttributes{
			Reason:  &reason,
			Details: details,
		},
	}}

	for i := 0; i < 2; i++ {
		ms := createMutableState(msBuilder)
		gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
		s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	}

	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(testDomainID, we)
	s.Equal(int64(15), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(*decisionStartedEvent1.EventId, executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Empty(executionBuilder.GetExecutionInfo().ExecutionContext)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.True(executionBuilder.HasPendingDecision())
	di3, ok := executionBuilder.GetDecisionInfo(executionBuilder.GetExecutionInfo().NextEventID - 1)
	s.True(ok)
	s.Equal(executionBuilder.GetExecutionInfo().NextEventID-1, di3.ScheduleID)
	s.Equal(int64(0), di3.Attempt)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedBadDecisionAttributes() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	identity := "testIdentity"
	executionContext := []byte("context")
	activity1ID := "activity1"
	activity1Type := "activity_type1"
	activity1Input := []byte("input1")
	activity1Result := []byte("activity1_result")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 25, 200, identity)
	di1 := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent1 := addDecisionTaskStartedEvent(msBuilder, di1.ScheduleID, tl, identity)
	decisionCompletedEvent1 := addDecisionTaskCompletedEvent(msBuilder, di1.ScheduleID,
		*decisionStartedEvent1.EventId, nil, identity)
	activity1ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent1.EventId, activity1ID,
		activity1Type, tl, activity1Input, 100, 10, 5)
	activity1StartedEvent := addActivityTaskStartedEvent(msBuilder, *activity1ScheduledEvent.EventId, identity)
	addActivityTaskCompletedEvent(msBuilder, *activity1ScheduledEvent.EventId,
		*activity1StartedEvent.EventId, activity1Result, identity)
	di2 := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)

	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: di2.ScheduleID,
	})

	// Decision with nil attributes
	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
	}}

	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: createMutableState(msBuilder)}
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: createMutableState(msBuilder)}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse1, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse2, nil).Once()

	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{
		MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil,
	).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         &identity,
		},
	})
	s.Nil(err)
}

// This test unit tests the activity schedule timeout validation logic of HistoryEngine's RespondDecisionTaskComplete function.
// An scheduled activity decision has 3 timeouts: ScheduleToClose, ScheduleToStart and StartToClose.
// This test verifies that when either ScheduleToClose or ScheduleToStart and StartToClose are specified,
// HistoryEngine's validateActivityScheduleAttribute will deduce the missing timeout and fill it in
// instead of returning a BadRequest error and only when all three are missing should a BadRequest be returned.
func (s *engineSuite) TestRespondDecisionTaskCompletedSingleActivityScheduledAttribute() {
	workflowTimeout := int32(100)
	testIterationVariables := []struct {
		scheduleToClose         *int32
		scheduleToStart         *int32
		startToClose            *int32
		heartbeat               *int32
		expectedScheduleToClose int32
		expectedScheduleToStart int32
		expectedStartToClose    int32
		expectDecisionFail      bool
	}{
		// No ScheduleToClose timeout, will use ScheduleToStart + StartToClose
		{nil, common.Int32Ptr(3), common.Int32Ptr(7), nil,
			3 + 7, 3, 7, false},
		// Has ScheduleToClose timeout but not ScheduleToStart or StartToClose,
		// will use ScheduleToClose for ScheduleToStart and StartToClose
		{common.Int32Ptr(7), nil, nil, nil,
			7, 7, 7, false},
		// No ScheduleToClose timeout, ScheduleToStart or StartToClose, expect error return
		{nil, nil, nil, nil,
			0, 0, 0, true},
		// Negative ScheduleToClose, expect error return
		{common.Int32Ptr(-1), nil, nil, nil,
			0, 0, 0, true},
		// Negative ScheduleToStart, expect error return
		{nil, common.Int32Ptr(-1), nil, nil,
			0, 0, 0, true},
		// Negative StartToClose, expect error return
		{nil, nil, common.Int32Ptr(-1), nil,
			0, 0, 0, true},
		// Negative HeartBeat, expect error return
		{nil, nil, nil, common.Int32Ptr(-1),
			0, 0, 0, true},
		// Use workflow timeout
		{common.Int32Ptr(workflowTimeout), nil, nil, nil,
			workflowTimeout, workflowTimeout, workflowTimeout, false},
		// Timeout larger than workflow timeout
		{common.Int32Ptr(workflowTimeout + 1), nil, nil, nil,
			workflowTimeout, workflowTimeout, workflowTimeout, false},
		{nil, common.Int32Ptr(workflowTimeout + 1), nil, nil,
			0, 0, 0, true},
		{nil, nil, common.Int32Ptr(workflowTimeout + 1), nil,
			0, 0, 0, true},
		{nil, nil, nil, common.Int32Ptr(workflowTimeout + 1),
			0, 0, 0, true},
		// No ScheduleToClose timeout, will use ScheduleToStart + StartToClose, but exceed limit
		{nil, common.Int32Ptr(workflowTimeout), common.Int32Ptr(10), nil,
			workflowTimeout, workflowTimeout, 10, false},
	}

	for _, iVar := range testIterationVariables {

		we := workflow.WorkflowExecution{
			WorkflowId: common.StringPtr("wId"),
			RunId:      common.StringPtr(testRunID),
		}
		tl := "testTaskList"
		taskToken, _ := json.Marshal(&common.TaskToken{
			WorkflowID: "wId",
			RunID:      we.GetRunId(),
			ScheduleID: 2,
		})
		identity := "testIdentity"
		executionContext := []byte("context")
		input := []byte("input")

		msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
			loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
		addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), workflowTimeout, 200, identity)
		di := addDecisionTaskScheduledEvent(msBuilder)
		addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

		decisions := []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
			ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
				ActivityId:                    common.StringPtr("activity1"),
				ActivityType:                  &workflow.ActivityType{Name: common.StringPtr("activity_type1")},
				TaskList:                      &workflow.TaskList{Name: &tl},
				Input:                         input,
				ScheduleToCloseTimeoutSeconds: iVar.scheduleToClose,
				ScheduleToStartTimeoutSeconds: iVar.scheduleToStart,
				StartToCloseTimeoutSeconds:    iVar.startToClose,
				HeartbeatTimeoutSeconds:       iVar.heartbeat,
			},
		}}

		gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: createMutableState(msBuilder)}
		s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse1, nil).Once()
		if iVar.expectDecisionFail {
			gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: createMutableState(msBuilder)}
			s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse2, nil).Once()
		}

		s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
		s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

		_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
			DomainUUID: common.StringPtr(testDomainID),
			CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
				TaskToken:        taskToken,
				Decisions:        decisions,
				ExecutionContext: executionContext,
				Identity:         &identity,
			},
		})

		s.Nil(err, s.printHistory(msBuilder))
		executionBuilder := s.getBuilder(testDomainID, we)
		if !iVar.expectDecisionFail {
			s.Equal(int64(6), executionBuilder.GetExecutionInfo().NextEventID)
			s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
			s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
			s.False(executionBuilder.HasPendingDecision())

			activity1Attributes := s.getActivityScheduledEvent(executionBuilder, int64(5)).ActivityTaskScheduledEventAttributes
			s.Equal(iVar.expectedScheduleToClose, activity1Attributes.GetScheduleToCloseTimeoutSeconds())
			s.Equal(iVar.expectedScheduleToStart, activity1Attributes.GetScheduleToStartTimeoutSeconds())
			s.Equal(iVar.expectedStartToClose, activity1Attributes.GetStartToCloseTimeoutSeconds())
		} else {
			s.Equal(int64(5), executionBuilder.GetExecutionInfo().NextEventID)
			s.Equal(common.EmptyEventID, executionBuilder.GetExecutionInfo().LastProcessedEvent)
			s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
			s.True(executionBuilder.HasPendingDecision())
		}
		s.TearDownTest()
		s.SetupTest()
	}
}

func (s *engineSuite) TestRespondDecisionTaskCompletedBadBinary() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      we.GetRunId(),
		ScheduleID: 2,
	})
	identity := "testIdentity"
	executionContext := []byte("context")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	var decisions []*workflow.Decision

	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: createMutableState(msBuilder)}
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: createMutableState(msBuilder)}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse1, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse2, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()
	s.mockDomainCache.ExpectedCalls = nil
	s.mockDomainCache.On("GetDomainByID", mock.Anything).Return(
		cache.NewLocalDomainCacheEntryForTest(
			&persistence.DomainInfo{ID: testDomainID, Name: testDomainName},
			&p.DomainConfig{
				Retention: 1,
				BadBinaries: workflow.BadBinaries{
					Binaries: map[string]*workflow.BadBinaryInfo{
						"test-bad-binary": {},
					},
				},
			},
			cluster.TestCurrentClusterName,
			nil,
		),
		nil,
	)

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         &identity,
			BinaryChecksum:   common.StringPtr("test-bad-binary"),
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(testDomainID, we)
	s.Equal(int64(5), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(common.EmptyEventID, executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Empty(executionBuilder.GetExecutionInfo().ExecutionContext)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.True(executionBuilder.HasPendingDecision())
}

func (s *engineSuite) TestRespondDecisionTaskCompletedSingleActivityScheduledDecision() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      we.GetRunId(),
		ScheduleID: 2,
	})
	identity := "testIdentity"
	executionContext := []byte("context")
	input := []byte("input")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
		ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
			ActivityId:                    common.StringPtr("activity1"),
			ActivityType:                  &workflow.ActivityType{Name: common.StringPtr("activity_type1")},
			TaskList:                      &workflow.TaskList{Name: &tl},
			Input:                         input,
			ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(10),
			StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
			HeartbeatTimeoutSeconds:       common.Int32Ptr(5),
		},
	}}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(testDomainID, we)
	s.Equal(int64(6), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(executionContext, executionBuilder.GetExecutionInfo().ExecutionContext)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecision())

	activity1Attributes := s.getActivityScheduledEvent(executionBuilder, int64(5)).ActivityTaskScheduledEventAttributes
	s.Equal("activity1", *activity1Attributes.ActivityId)
	s.Equal("activity_type1", *activity1Attributes.ActivityType.Name)
	s.Equal(int64(4), *activity1Attributes.DecisionTaskCompletedEventId)
	s.Equal(tl, *activity1Attributes.TaskList.Name)
	s.Equal(input, activity1Attributes.Input)
	s.Equal(int32(100), *activity1Attributes.ScheduleToCloseTimeoutSeconds)
	s.Equal(int32(10), *activity1Attributes.ScheduleToStartTimeoutSeconds)
	s.Equal(int32(50), *activity1Attributes.StartToCloseTimeoutSeconds)
	s.Equal(int32(5), *activity1Attributes.HeartbeatTimeoutSeconds)
}

func (s *engineSuite) TestRespondDecisionTaskCompleted_DecisionHeartbeatTimeout() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 2,
	})
	identity := "testIdentity"
	executionContext := []byte("context")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	msBuilder.executionInfo.DecisionOriginalScheduledTimestamp = time.Now().Add(-time.Hour).UnixNano()

	decisions := []*workflow.Decision{}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	s.mockClusterMetadata.On("IsArchivalEnabled").Return(true)
	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			ForceCreateNewDecisionTask: common.BoolPtr(true),
			TaskToken:                  taskToken,
			Decisions:                  decisions,
			ExecutionContext:           executionContext,
			Identity:                   &identity,
		},
	})
	s.Error(err, "decision heartbeat timeout")
}

func (s *engineSuite) TestRespondDecisionTaskCompleted_DecisionHeartbeatNotTimeout() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 2,
	})
	identity := "testIdentity"
	executionContext := []byte("context")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	msBuilder.executionInfo.DecisionOriginalScheduledTimestamp = time.Now().Add(-time.Minute).UnixNano()

	decisions := []*workflow.Decision{}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	s.mockClusterMetadata.On("IsArchivalEnabled").Return(true)
	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			ForceCreateNewDecisionTask: common.BoolPtr(true),
			TaskToken:                  taskToken,
			Decisions:                  decisions,
			ExecutionContext:           executionContext,
			Identity:                   &identity,
		},
	})
	s.Nil(err)
}

func (s *engineSuite) TestRespondDecisionTaskCompleted_DecisionHeartbeatNotTimeout_ZeroOrignalScheduledTime() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 2,
	})
	identity := "testIdentity"
	executionContext := []byte("context")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	msBuilder.executionInfo.DecisionOriginalScheduledTimestamp = 0

	decisions := []*workflow.Decision{}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	s.mockClusterMetadata.On("IsArchivalEnabled").Return(true)
	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			ForceCreateNewDecisionTask: common.BoolPtr(true),
			TaskToken:                  taskToken,
			Decisions:                  decisions,
			ExecutionContext:           executionContext,
			Identity:                   &identity,
		},
	})
	s.Nil(err)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedCompleteWorkflowSuccess() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 2,
	})
	identity := "testIdentity"
	executionContext := []byte("context")
	workflowResult := []byte("success")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
		CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
			Result: workflowResult,
		},
	}}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	s.mockClusterMetadata.On("IsArchivalEnabled").Return(true)
	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(testDomainID, we)
	s.Equal(int64(6), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(executionContext, executionBuilder.GetExecutionInfo().ExecutionContext)
	s.Equal(persistence.WorkflowStateCompleted, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecision())
}

func (s *engineSuite) TestRespondDecisionTaskCompletedFailWorkflowSuccess() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 2,
	})
	identity := "testIdentity"
	executionContext := []byte("context")
	details := []byte("fail workflow details")
	reason := "fail workflow reason"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeFailWorkflowExecution),
		FailWorkflowExecutionDecisionAttributes: &workflow.FailWorkflowExecutionDecisionAttributes{
			Reason:  &reason,
			Details: details,
		},
	}}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()
	s.mockDomainCache.ExpectedCalls = nil
	s.mockDomainCache.On("GetDomainByID", mock.Anything).Return(
		cache.NewLocalDomainCacheEntryForTest(
			&persistence.DomainInfo{ID: testDomainID, Name: testDomainName},
			&p.DomainConfig{
				Retention:                1,
				HistoryArchivalStatus:    workflow.ArchivalStatusEnabled,
				VisibilityArchivalStatus: workflow.ArchivalStatusEnabled,
			},
			cluster.TestCurrentClusterName,
			nil,
		),
		nil,
	)
	s.mockClusterMetadata.On("IsArchivalEnabled").Return(true)
	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(testDomainID, we)
	s.Equal(int64(6), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(executionContext, executionBuilder.GetExecutionInfo().ExecutionContext)
	s.Equal(persistence.WorkflowStateCompleted, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecision())
}

func (s *engineSuite) TestRespondDecisionTaskCompletedSignalExternalWorkflowSuccess() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 2,
	})
	identity := "testIdentity"
	executionContext := []byte("context")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeSignalExternalWorkflowExecution),
		SignalExternalWorkflowExecutionDecisionAttributes: &workflow.SignalExternalWorkflowExecutionDecisionAttributes{
			Domain: common.StringPtr(testDomainName),
			Execution: &workflow.WorkflowExecution{
				WorkflowId: we.WorkflowId,
				RunId:      we.RunId,
			},
			SignalName: common.StringPtr("signal"),
			Input:      []byte("test input"),
		},
	}}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(testDomainID, we)
	s.Equal(int64(6), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(executionContext, executionBuilder.GetExecutionInfo().ExecutionContext)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedStartChildWorkflowWithAbandonPolicy() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 2,
	})
	identity := "testIdentity"
	executionContext := []byte("context")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	abandon := workflow.ParentClosePolicyAbandon
	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeStartChildWorkflowExecution),
		StartChildWorkflowExecutionDecisionAttributes: &workflow.StartChildWorkflowExecutionDecisionAttributes{
			Domain:     common.StringPtr(testDomainName),
			WorkflowId: common.StringPtr("child-workflow-id"),
			WorkflowType: &workflow.WorkflowType{
				Name: common.StringPtr("child-workflow-type"),
			},
			ParentClosePolicy: &abandon,
		},
	}}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(testDomainID, we)
	s.Equal(int64(6), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(executionContext, executionBuilder.GetExecutionInfo().ExecutionContext)
	s.Equal(int(1), len(executionBuilder.GetPendingChildExecutionInfos()))
	var childID int64
	for c := range executionBuilder.GetPendingChildExecutionInfos() {
		childID = c
		break
	}
	s.Equal("child-workflow-id", executionBuilder.GetPendingChildExecutionInfos()[childID].StartedWorkflowID)
	s.Equal(workflow.ParentClosePolicyAbandon, executionBuilder.GetPendingChildExecutionInfos()[childID].ParentClosePolicy)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedStartChildWorkflowWithTerminatePolicy() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 2,
	})
	identity := "testIdentity"
	executionContext := []byte("context")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	terminate := workflow.ParentClosePolicyTerminate
	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeStartChildWorkflowExecution),
		StartChildWorkflowExecutionDecisionAttributes: &workflow.StartChildWorkflowExecutionDecisionAttributes{
			Domain:     common.StringPtr(testDomainName),
			WorkflowId: common.StringPtr("child-workflow-id"),
			WorkflowType: &workflow.WorkflowType{
				Name: common.StringPtr("child-workflow-type"),
			},
			ParentClosePolicy: &terminate,
		},
	}}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(testDomainID, we)
	s.Equal(int64(6), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(executionContext, executionBuilder.GetExecutionInfo().ExecutionContext)
	s.Equal(int(1), len(executionBuilder.GetPendingChildExecutionInfos()))
	var childID int64
	for c := range executionBuilder.GetPendingChildExecutionInfos() {
		childID = c
		break
	}
	s.Equal("child-workflow-id", executionBuilder.GetPendingChildExecutionInfos()[childID].StartedWorkflowID)
	s.Equal(workflow.ParentClosePolicyTerminate, executionBuilder.GetPendingChildExecutionInfos()[childID].ParentClosePolicy)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedSignalExternalWorkflowFailed() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("invalid run id"),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: we.GetWorkflowId(),
		RunID:      we.GetRunId(),
		ScheduleID: 2,
	})
	identity := "testIdentity"
	executionContext := []byte("context")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeSignalExternalWorkflowExecution),
		SignalExternalWorkflowExecutionDecisionAttributes: &workflow.SignalExternalWorkflowExecutionDecisionAttributes{
			Domain: common.StringPtr(testDomainID),
			Execution: &workflow.WorkflowExecution{
				WorkflowId: we.WorkflowId,
				RunId:      we.RunId,
			},
			SignalName: common.StringPtr("signal"),
			Input:      []byte("test input"),
		},
	}}

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         &identity,
		},
	})

	s.EqualError(err, "BadRequestError{Message: RunID is not valid UUID.}")
}

func (s *engineSuite) TestRespondDecisionTaskCompletedSignalExternalWorkflowFailed_UnKnownDomain() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 2,
	})
	identity := "testIdentity"
	executionContext := []byte("context")
	foreignDomain := "unknown domain"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeSignalExternalWorkflowExecution),
		SignalExternalWorkflowExecutionDecisionAttributes: &workflow.SignalExternalWorkflowExecutionDecisionAttributes{
			Domain: common.StringPtr(foreignDomain),
			Execution: &workflow.WorkflowExecution{
				WorkflowId: we.WorkflowId,
				RunId:      we.RunId,
			},
			SignalName: common.StringPtr("signal"),
			Input:      []byte("test input"),
		},
	}}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockDomainCache.On("GetDomain", foreignDomain).Return(
		nil, errors.New("get foreign domain error"),
	)

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         &identity,
		},
	})

	s.NotNil(err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedInvalidToken() {

	invalidToken, _ := json.Marshal("bad token")
	identity := "testIdentity"

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: invalidToken,
			Result:    nil,
			Identity:  &identity,
		},
	})

	s.NotNil(err)
	s.IsType(&workflow.BadRequestError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedIfNoExecution() {

	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      testRunID,
		ScheduleID: 2,
	})
	identity := "testIdentity"

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, &workflow.EntityNotExistsError{}).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedIfNoRunID() {

	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		ScheduleID: 2,
	})
	identity := "testIdentity"

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(nil, &workflow.EntityNotExistsError{}).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedIfGetExecutionFailed() {

	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      testRunID,
		ScheduleID: 2,
	})
	identity := "testIdentity"

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, errors.New("FAILED")).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondActivityTaskCompletedIfNoAIdProvided() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tasklist := "testTaskList"
	identity := "testIdentity"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		ScheduleID: common.EmptyEventID,
	})

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), testRunID)
	addWorkflowExecutionStartedEvent(msBuilder, execution, "wType", tasklist, []byte("input"), 100, 200, identity)
	addDecisionTaskScheduledEvent(msBuilder)
	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: testRunID}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.EqualError(err, "BadRequestError{Message: Neither ActivityID nor ScheduleID is provided}")
}

func (s *engineSuite) TestRespondActivityTaskCompletedIfNotFound() {

	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		ScheduleID: common.EmptyEventID,
		ActivityID: "aid",
	})
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tasklist := "testTaskList"
	identity := "testIdentity"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), testRunID)
	addWorkflowExecutionStartedEvent(msBuilder, execution, "wType", tasklist, []byte("input"), 100, 200, identity)
	addDecisionTaskScheduledEvent(msBuilder)
	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: testRunID}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.Error(err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedUpdateExecutionFailed() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	activityResult := []byte("activity result")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 5)
	addActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, errors.New("FAILED")).Once()
	s.mockShardManager.On("UpdateShard", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result:    activityResult,
			Identity:  &identity,
		},
	})
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondActivityTaskCompletedIfTaskCompleted() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	activityResult := []byte("activity result")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 5)
	activityStartedEvent := addActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, identity)
	addActivityTaskCompletedEvent(msBuilder, *activityScheduledEvent.EventId, *activityStartedEvent.EventId,
		activityResult, identity)
	addDecisionTaskScheduledEvent(msBuilder)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result:    activityResult,
			Identity:  &identity,
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedIfTaskNotStarted() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	activityResult := []byte("activity result")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 5)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result:    activityResult,
			Identity:  &identity,
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedConflictOnUpdate() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activity1ID := "activity1"
	activity1Type := "activity_type1"
	activity1Input := []byte("input1")
	activity1Result := []byte("activity1_result")
	activity2ID := "activity2"
	activity2Type := "activity_type2"
	activity2Input := []byte("input2")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent1 := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent1 := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent1.EventId, nil, identity)
	activity1ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent1.EventId, activity1ID,
		activity1Type, tl, activity1Input, 100, 10, 5)
	activity2ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent1.EventId, activity2ID,
		activity2Type, tl, activity2Input, 100, 10, 5)
	addActivityTaskStartedEvent(msBuilder, *activity1ScheduledEvent.EventId, identity)
	addActivityTaskStartedEvent(msBuilder, *activity2ScheduledEvent.EventId, identity)

	ms1 := createMutableState(msBuilder)
	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: ms1}

	ms2 := createMutableState(msBuilder)
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: ms2}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse1, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, &persistence.ConditionFailedError{}).Once()

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse2, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result:    activity1Result,
			Identity:  &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(testDomainID, we)
	s.Equal(int64(11), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)

	s.True(executionBuilder.HasPendingDecision())
	di, ok := executionBuilder.GetDecisionInfo(int64(10))
	s.True(ok)
	s.Equal(int32(100), di.DecisionTimeout)
	s.Equal(int64(10), di.ScheduleID)
	s.Equal(common.EmptyEventID, di.StartedID)
}

func (s *engineSuite) TestRespondActivityTaskCompletedMaxAttemptsExceeded() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	activityResult := []byte("activity result")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 5)
	addActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, identity)

	for i := 0; i < conditionalRetryCount; i++ {
		ms := createMutableState(msBuilder)
		gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

		s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
		s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
		s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, &persistence.ConditionFailedError{}).Once()
	}

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result:    activityResult,
			Identity:  &identity,
		},
	})
	s.Equal(ErrMaxAttemptsExceeded, err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedSuccess() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	activityResult := []byte("activity result")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 5)
	addActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result:    activityResult,
			Identity:  &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(testDomainID, we)
	s.Equal(int64(9), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)

	s.True(executionBuilder.HasPendingDecision())
	di, ok := executionBuilder.GetDecisionInfo(int64(8))
	s.True(ok)
	s.Equal(int32(100), di.DecisionTimeout)
	s.Equal(int64(8), di.ScheduleID)
	s.Equal(common.EmptyEventID, di.StartedID)
}

func (s *engineSuite) TestRespondActivityTaskCompletedByIdSuccess() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"

	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	activityResult := []byte("activity result")
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		ScheduleID: common.EmptyEventID,
		ActivityID: activityID,
	})

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	decisionScheduledEvent := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, decisionScheduledEvent.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 5)
	addActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: *we.RunId}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result:    activityResult,
			Identity:  &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(testDomainID, we)
	s.Equal(int64(9), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)

	s.True(executionBuilder.HasPendingDecision())
	di, ok := executionBuilder.GetDecisionInfo(int64(8))
	s.True(ok)
	s.Equal(int32(100), di.DecisionTimeout)
	s.Equal(int64(8), di.ScheduleID)
	s.Equal(common.EmptyEventID, di.StartedID)
}

func (s *engineSuite) TestRespondActivityTaskFailedInvalidToken() {

	invalidToken, _ := json.Marshal("bad token")
	identity := "testIdentity"

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		FailedRequest: &workflow.RespondActivityTaskFailedRequest{
			TaskToken: invalidToken,
			Identity:  &identity,
		},
	})

	s.NotNil(err)
	s.IsType(&workflow.BadRequestError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskFailedIfNoExecution() {

	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      testRunID,
		ScheduleID: 2,
	})
	identity := "testIdentity"

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil,
		&workflow.EntityNotExistsError{}).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		FailedRequest: &workflow.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskFailedIfNoRunID() {

	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		ScheduleID: 2,
	})
	identity := "testIdentity"

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(nil,
		&workflow.EntityNotExistsError{}).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		FailedRequest: &workflow.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskFailedIfGetExecutionFailed() {

	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      testRunID,
		ScheduleID: 2,
	})
	identity := "testIdentity"

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil,
		errors.New("FAILED")).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		FailedRequest: &workflow.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondActivityTaskFailededIfNoAIdProvided() {

	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		ScheduleID: common.EmptyEventID,
	})
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tasklist := "testTaskList"
	identity := "testIdentity"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), testRunID)
	addWorkflowExecutionStartedEvent(msBuilder, execution, "wType", tasklist, []byte("input"), 100, 200, identity)
	addDecisionTaskScheduledEvent(msBuilder)
	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: testRunID}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		FailedRequest: &workflow.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.EqualError(err, "BadRequestError{Message: Neither ActivityID nor ScheduleID is provided}")
}

func (s *engineSuite) TestRespondActivityTaskFailededIfNotFound() {

	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		ScheduleID: common.EmptyEventID,
		ActivityID: "aid",
	})
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tasklist := "testTaskList"
	identity := "testIdentity"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), testRunID)
	addWorkflowExecutionStartedEvent(msBuilder, execution, "wType", tasklist, []byte("input"), 100, 200, identity)
	addDecisionTaskScheduledEvent(msBuilder)
	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: testRunID}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		FailedRequest: &workflow.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.Error(err)
}

func (s *engineSuite) TestRespondActivityTaskFailedUpdateExecutionFailed() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 5)
	addActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, errors.New("FAILED")).Once()
	s.mockShardManager.On("UpdateShard", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		FailedRequest: &workflow.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondActivityTaskFailedIfTaskCompleted() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	failReason := "fail reason"
	details := []byte("fail details")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 5)
	activityStartedEvent := addActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, identity)
	addActivityTaskFailedEvent(msBuilder, *activityScheduledEvent.EventId, *activityStartedEvent.EventId,
		failReason, details, identity)
	addDecisionTaskScheduledEvent(msBuilder)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		FailedRequest: &workflow.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Reason:    &failReason,
			Details:   details,
			Identity:  &identity,
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskFailedIfTaskNotStarted() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 5)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		FailedRequest: &workflow.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskFailedConflictOnUpdate() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activity1ID := "activity1"
	activity1Type := "activity_type1"
	activity1Input := []byte("input1")
	failReason := "fail reason"
	details := []byte("fail details.")
	activity2ID := "activity2"
	activity2Type := "activity_type2"
	activity2Input := []byte("input2")
	activity2Result := []byte("activity2_result")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 25, 25, identity)
	di1 := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent1 := addDecisionTaskStartedEvent(msBuilder, di1.ScheduleID, tl, identity)
	decisionCompletedEvent1 := addDecisionTaskCompletedEvent(msBuilder, di1.ScheduleID,
		*decisionStartedEvent1.EventId, nil, identity)
	activity1ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent1.EventId, activity1ID,
		activity1Type, tl, activity1Input, 100, 10, 5)
	activity2ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent1.EventId, activity2ID,
		activity2Type, tl, activity2Input, 100, 10, 5)
	addActivityTaskStartedEvent(msBuilder, *activity1ScheduledEvent.EventId, identity)
	activity2StartedEvent := addActivityTaskStartedEvent(msBuilder, *activity2ScheduledEvent.EventId, identity)

	ms1 := createMutableState(msBuilder)
	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: ms1}

	addActivityTaskCompletedEvent(msBuilder, *activity2ScheduledEvent.EventId,
		*activity2StartedEvent.EventId, activity2Result, identity)
	addDecisionTaskScheduledEvent(msBuilder)

	ms2 := createMutableState(msBuilder)
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: ms2}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse1, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, &persistence.ConditionFailedError{}).Once()

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse2, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		FailedRequest: &workflow.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Reason:    &failReason,
			Details:   details,
			Identity:  &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(testDomainID, we)
	s.Equal(int64(12), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)

	s.True(executionBuilder.HasPendingDecision())
	di, ok := executionBuilder.GetDecisionInfo(int64(10))
	s.True(ok)
	s.Equal(int32(25), di.DecisionTimeout)
	s.Equal(int64(10), di.ScheduleID)
	s.Equal(common.EmptyEventID, di.StartedID)
}

func (s *engineSuite) TestRespondActivityTaskFailedMaxAttemptsExceeded() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 5)
	addActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, identity)

	for i := 0; i < conditionalRetryCount; i++ {
		ms := createMutableState(msBuilder)
		gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

		s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
		s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
		s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, &persistence.ConditionFailedError{}).Once()
	}

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		FailedRequest: &workflow.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.Equal(ErrMaxAttemptsExceeded, err)
}

func (s *engineSuite) TestRespondActivityTaskFailedSuccess() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	failReason := "failed"
	failDetails := []byte("fail details.")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 5)
	addActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		FailedRequest: &workflow.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Reason:    &failReason,
			Details:   failDetails,
			Identity:  &identity,
		},
	})
	s.Nil(err)
	executionBuilder := s.getBuilder(testDomainID, we)
	s.Equal(int64(9), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)

	s.True(executionBuilder.HasPendingDecision())
	di, ok := executionBuilder.GetDecisionInfo(int64(8))
	s.True(ok)
	s.Equal(int32(100), di.DecisionTimeout)
	s.Equal(int64(8), di.ScheduleID)
	s.Equal(common.EmptyEventID, di.StartedID)
}

func (s *engineSuite) TestRespondActivityTaskFailedByIDSuccess() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"

	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	failReason := "failed"
	failDetails := []byte("fail details.")
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		ScheduleID: common.EmptyEventID,
		ActivityID: activityID,
	})

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	decisionScheduledEvent := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, decisionScheduledEvent.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 5)
	addActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: *we.RunId}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		FailedRequest: &workflow.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Reason:    &failReason,
			Details:   failDetails,
			Identity:  &identity,
		},
	})
	s.Nil(err)
	executionBuilder := s.getBuilder(testDomainID, we)
	s.Equal(int64(9), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)

	s.True(executionBuilder.HasPendingDecision())
	di, ok := executionBuilder.GetDecisionInfo(int64(8))
	s.True(ok)
	s.Equal(int32(100), di.DecisionTimeout)
	s.Equal(int64(8), di.ScheduleID)
	s.Equal(common.EmptyEventID, di.StartedID)
}

func (s *engineSuite) TestRecordActivityTaskHeartBeatSuccess_NoTimer() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 0)
	addActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, identity)

	// No HeartBeat timer running.
	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	detais := []byte("details")

	_, err := s.mockHistoryEngine.RecordActivityTaskHeartbeat(context.Background(), &history.RecordActivityTaskHeartbeatRequest{
		DomainUUID: common.StringPtr(testDomainID),
		HeartbeatRequest: &workflow.RecordActivityTaskHeartbeatRequest{
			TaskToken: taskToken,
			Identity:  &identity,
			Details:   detais,
		},
	})
	s.Nil(err)
}

func (s *engineSuite) TestRecordActivityTaskHeartBeatSuccess_TimerRunning() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1)
	addActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	// HeartBeat timer running.
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	detais := []byte("details")

	_, err := s.mockHistoryEngine.RecordActivityTaskHeartbeat(context.Background(), &history.RecordActivityTaskHeartbeatRequest{
		DomainUUID: common.StringPtr(testDomainID),
		HeartbeatRequest: &workflow.RecordActivityTaskHeartbeatRequest{
			TaskToken: taskToken,
			Identity:  &identity,
			Details:   detais,
		},
	})
	s.Nil(err)
	executionBuilder := s.getBuilder(testDomainID, we)
	s.Equal(int64(7), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecision())
}

func (s *engineSuite) TestRecordActivityTaskHeartBeatByIDSuccess() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: common.EmptyEventID,
		ActivityID: activityID,
	})

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 0)
	addActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, identity)

	// No HeartBeat timer running.
	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	detais := []byte("details")

	_, err := s.mockHistoryEngine.RecordActivityTaskHeartbeat(context.Background(), &history.RecordActivityTaskHeartbeatRequest{
		DomainUUID: common.StringPtr(testDomainID),
		HeartbeatRequest: &workflow.RecordActivityTaskHeartbeatRequest{
			TaskToken: taskToken,
			Identity:  &identity,
			Details:   detais,
		},
	})
	s.Nil(err)
}

func (s *engineSuite) TestRespondActivityTaskCanceled_Scheduled() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCanceled(context.Background(), &history.RespondActivityTaskCanceledRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CancelRequest: &workflow.RespondActivityTaskCanceledRequest{
			TaskToken: taskToken,
			Identity:  &identity,
			Details:   []byte("details"),
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCanceled_Started() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1)
	addActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, identity)
	_, _, err := msBuilder.AddActivityTaskCancelRequestedEvent(*decisionCompletedEvent.EventId, activityID, identity)
	s.Nil(err)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	err = s.mockHistoryEngine.RespondActivityTaskCanceled(context.Background(), &history.RespondActivityTaskCanceledRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CancelRequest: &workflow.RespondActivityTaskCanceledRequest{
			TaskToken: taskToken,
			Identity:  &identity,
			Details:   []byte("details"),
		},
	})
	s.Nil(err)
	executionBuilder := s.getBuilder(testDomainID, we)
	s.Equal(int64(10), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)

	s.True(executionBuilder.HasPendingDecision())
	di, ok := executionBuilder.GetDecisionInfo(int64(9))
	s.True(ok)
	s.Equal(int32(100), di.DecisionTimeout)
	s.Equal(int64(9), di.ScheduleID)
	s.Equal(common.EmptyEventID, di.StartedID)
}

func (s *engineSuite) TestRespondActivityTaskCanceledByID_Started() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		ScheduleID: common.EmptyEventID,
		ActivityID: activityID,
	})

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	decisionScheduledEvent := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, decisionScheduledEvent.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1)
	addActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, identity)
	_, _, err := msBuilder.AddActivityTaskCancelRequestedEvent(*decisionCompletedEvent.EventId, activityID, identity)
	s.Nil(err)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: *we.RunId}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	err = s.mockHistoryEngine.RespondActivityTaskCanceled(context.Background(), &history.RespondActivityTaskCanceledRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CancelRequest: &workflow.RespondActivityTaskCanceledRequest{
			TaskToken: taskToken,
			Identity:  &identity,
			Details:   []byte("details"),
		},
	})
	s.Nil(err)
	executionBuilder := s.getBuilder(testDomainID, we)
	s.Equal(int64(10), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)

	s.True(executionBuilder.HasPendingDecision())
	di, ok := executionBuilder.GetDecisionInfo(int64(9))
	s.True(ok)
	s.Equal(int32(100), di.DecisionTimeout)
	s.Equal(int64(9), di.ScheduleID)
	s.Equal(common.EmptyEventID, di.StartedID)
}

func (s *engineSuite) TestRespondActivityTaskCanceledIfNoRunID() {

	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		ScheduleID: 2,
	})
	identity := "testIdentity"

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(nil, &workflow.EntityNotExistsError{}).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCanceled(context.Background(), &history.RespondActivityTaskCanceledRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CancelRequest: &workflow.RespondActivityTaskCanceledRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCanceledIfNoAIdProvided() {

	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		ScheduleID: common.EmptyEventID,
	})
	identity := "testIdentity"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), testRunID)
	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: testRunID}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCanceled(context.Background(), &history.RespondActivityTaskCanceledRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CancelRequest: &workflow.RespondActivityTaskCanceledRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.EqualError(err, "BadRequestError{Message: Neither ActivityID nor ScheduleID is provided}")
}

func (s *engineSuite) TestRespondActivityTaskCanceledIfNotFound() {

	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		ScheduleID: common.EmptyEventID,
		ActivityID: "aid",
	})
	identity := "testIdentity"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), testRunID)
	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: testRunID}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCanceled(context.Background(), &history.RespondActivityTaskCanceledRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CancelRequest: &workflow.RespondActivityTaskCanceledRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.Error(err)
}

func (s *engineSuite) TestRequestCancel_RespondDecisionTaskCompleted_NotScheduled() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 2,
	})
	identity := "testIdentity"
	activityID := "activity1_id"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeRequestCancelActivityTask),
		RequestCancelActivityTaskDecisionAttributes: &workflow.RequestCancelActivityTaskDecisionAttributes{
			ActivityId: common.StringPtr(activityID),
		},
	}}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)
	executionBuilder := s.getBuilder(testDomainID, we)
	s.Equal(int64(7), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecision())
}

func (s *engineSuite) TestRequestCancel_RespondDecisionTaskCompleted_Scheduled() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 6,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1)
	di2 := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeRequestCancelActivityTask),
		RequestCancelActivityTaskDecisionAttributes: &workflow.RequestCancelActivityTaskDecisionAttributes{
			ActivityId: common.StringPtr(activityID),
		},
	}}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(testDomainID, we)
	s.Equal(int64(12), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(7), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.True(executionBuilder.HasPendingDecision())
	di2, ok := executionBuilder.GetDecisionInfo(executionBuilder.GetExecutionInfo().NextEventID - 1)
	s.True(ok)
	s.Equal(executionBuilder.GetExecutionInfo().NextEventID-1, di2.ScheduleID)
	s.Equal(int64(0), di2.Attempt)
}

func (s *engineSuite) TestRequestCancel_RespondDecisionTaskCompleted_Started() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 7,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 0)
	addActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, identity)
	di2 := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeRequestCancelActivityTask),
		RequestCancelActivityTaskDecisionAttributes: &workflow.RequestCancelActivityTaskDecisionAttributes{
			ActivityId: common.StringPtr(activityID),
		},
	}}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(testDomainID, we)
	s.Equal(int64(11), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(8), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecision())
}

func (s *engineSuite) TestRequestCancel_RespondDecisionTaskCompleted_Completed() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 6,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	workflowResult := []byte("workflow result")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 0)
	di2 := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)

	decisions := []*workflow.Decision{
		{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeRequestCancelActivityTask),
			RequestCancelActivityTaskDecisionAttributes: &workflow.RequestCancelActivityTaskDecisionAttributes{
				ActivityId: common.StringPtr(activityID),
			},
		},
		{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: workflowResult,
			},
		},
	}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(testDomainID, we)
	s.Equal(int64(11), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(7), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateCompleted, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecision())
}

func (s *engineSuite) TestRequestCancel_RespondDecisionTaskCompleted_NoHeartBeat() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 7,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 0)
	addActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, identity)
	di2 := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeRequestCancelActivityTask),
		RequestCancelActivityTaskDecisionAttributes: &workflow.RequestCancelActivityTaskDecisionAttributes{
			ActivityId: common.StringPtr(activityID),
		},
	}}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(testDomainID, we)
	s.Equal(int64(11), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(8), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecision())

	// Try recording activity heartbeat
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	activityTaskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      we.GetRunId(),
		ScheduleID: 5,
	})

	hbResponse, err := s.mockHistoryEngine.RecordActivityTaskHeartbeat(context.Background(), &history.RecordActivityTaskHeartbeatRequest{
		DomainUUID: common.StringPtr(testDomainID),
		HeartbeatRequest: &workflow.RecordActivityTaskHeartbeatRequest{
			TaskToken: activityTaskToken,
			Identity:  &identity,
			Details:   []byte("details"),
		},
	})
	s.Nil(err)
	s.NotNil(hbResponse)
	s.True(*hbResponse.CancelRequested)

	// Try cancelling the request.
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	err = s.mockHistoryEngine.RespondActivityTaskCanceled(context.Background(), &history.RespondActivityTaskCanceledRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CancelRequest: &workflow.RespondActivityTaskCanceledRequest{
			TaskToken: activityTaskToken,
			Identity:  &identity,
			Details:   []byte("details"),
		},
	})
	s.Nil(err)

	executionBuilder = s.getBuilder(testDomainID, we)
	s.Equal(int64(13), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(8), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.True(executionBuilder.HasPendingDecision())
}

func (s *engineSuite) TestRequestCancel_RespondDecisionTaskCompleted_Success() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 7,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1)
	addActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, identity)
	di2 := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeRequestCancelActivityTask),
		RequestCancelActivityTaskDecisionAttributes: &workflow.RequestCancelActivityTaskDecisionAttributes{
			ActivityId: common.StringPtr(activityID),
		},
	}}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(testDomainID, we)
	s.Equal(int64(11), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(8), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecision())

	// Try recording activity heartbeat
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	activityTaskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      we.GetRunId(),
		ScheduleID: 5,
	})

	hbResponse, err := s.mockHistoryEngine.RecordActivityTaskHeartbeat(context.Background(), &history.RecordActivityTaskHeartbeatRequest{
		DomainUUID: common.StringPtr(testDomainID),
		HeartbeatRequest: &workflow.RecordActivityTaskHeartbeatRequest{
			TaskToken: activityTaskToken,
			Identity:  &identity,
			Details:   []byte("details"),
		},
	})
	s.Nil(err)
	s.NotNil(hbResponse)
	s.True(*hbResponse.CancelRequested)

	// Try cancelling the request.
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	err = s.mockHistoryEngine.RespondActivityTaskCanceled(context.Background(), &history.RespondActivityTaskCanceledRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CancelRequest: &workflow.RespondActivityTaskCanceledRequest{
			TaskToken: activityTaskToken,
			Identity:  &identity,
			Details:   []byte("details"),
		},
	})
	s.Nil(err)

	executionBuilder = s.getBuilder(testDomainID, we)
	s.Equal(int64(13), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(8), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.True(executionBuilder.HasPendingDecision())
}

func (s *engineSuite) TestStarTimer_DuplicateTimerID() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 2,
	})
	identity := "testIdentity"
	timerID := "t1"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())

	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeStartTimer),
		StartTimerDecisionAttributes: &workflow.StartTimerDecisionAttributes{
			TimerId:                   common.StringPtr(timerID),
			StartToFireTimeoutSeconds: common.Int64Ptr(1),
		},
	}}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(testDomainID, we)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)

	// Try to add the same timer ID again.
	di2 := addDecisionTaskScheduledEvent(executionBuilder)
	addDecisionTaskStartedEvent(executionBuilder, di2.ScheduleID, tl, identity)
	taskToken2, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: di2.ScheduleID,
	})

	ms2 := createMutableState(executionBuilder)
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: ms2}

	decisionFailedEvent := false
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse2, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Run(func(arguments mock.Arguments) {
		req := arguments.Get(0).(*persistence.AppendHistoryNodesRequest)
		decTaskIndex := len(req.Events) - 1
		if decTaskIndex >= 0 && *req.Events[decTaskIndex].EventType == workflow.EventTypeDecisionTaskFailed {
			decisionFailedEvent = true
		}
	}).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err = s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken2,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)

	s.True(decisionFailedEvent)
	executionBuilder = s.getBuilder(testDomainID, we)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.True(executionBuilder.HasPendingDecision())
	di3, ok := executionBuilder.GetDecisionInfo(executionBuilder.GetExecutionInfo().NextEventID)
	s.True(ok, "DI.ScheduleID: %v, ScheduleID: %v, StartedID: %v", di2.ScheduleID,
		executionBuilder.GetExecutionInfo().DecisionScheduleID, executionBuilder.GetExecutionInfo().DecisionStartedID)
	s.Equal(executionBuilder.GetExecutionInfo().NextEventID, di3.ScheduleID)
	s.Equal(int64(1), di3.Attempt)
}

func (s *engineSuite) TestUserTimer_RespondDecisionTaskCompleted() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 6,
	})
	identity := "testIdentity"
	timerID := "t1"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	// Verify cancel timer with a start event.
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	addTimerStartedEvent(msBuilder, *decisionCompletedEvent.EventId, timerID, 10)
	di2 := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCancelTimer),
		CancelTimerDecisionAttributes: &workflow.CancelTimerDecisionAttributes{
			TimerId: common.StringPtr(timerID),
		},
	}}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(testDomainID, we)
	s.Equal(int64(10), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(7), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecision())
}

func (s *engineSuite) TestCancelTimer_RespondDecisionTaskCompleted_NoStartTimer() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 2,
	})
	identity := "testIdentity"
	timerID := "t1"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	// Verify cancel timer with a start event.
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCancelTimer),
		CancelTimerDecisionAttributes: &workflow.CancelTimerDecisionAttributes{
			TimerId: common.StringPtr(timerID),
		},
	}}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(testDomainID, we)
	s.Equal(int64(6), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecision())
}

func (s *engineSuite) TestCancelTimer_RespondDecisionTaskCompleted_TimerFired() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 6,
	})
	identity := "testIdentity"
	timerID := "t1"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	// Verify cancel timer with a start event.
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	addTimerStartedEvent(msBuilder, *decisionCompletedEvent.EventId, timerID, 10)
	di2 := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)
	addTimerFiredEvent(msBuilder, di2.ScheduleID, timerID)
	_, _, err := msBuilder.CloseTransactionAsMutation(time.Now(), transactionPolicyActive)
	s.Nil(err)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.True(len(gwmsResponse.State.BufferedEvents) > 0)

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCancelTimer),
		CancelTimerDecisionAttributes: &workflow.CancelTimerDecisionAttributes{
			TimerId: common.StringPtr(timerID),
		},
	}}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.MatchedBy(func(input *persistence.UpdateWorkflowExecutionRequest) bool {
		// need to check whether the buffered events are cleared
		s.True(input.UpdateWorkflowMutation.ClearBufferedEvents)
		return true
	})).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err = s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(testDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(testDomainID, we)
	s.Equal(int64(10), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(7), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecision())
	s.False(executionBuilder.HasBufferedEvents())
}

func (s *engineSuite) TestSignalWorkflowExecution() {
	signalRequest := &history.SignalWorkflowExecutionRequest{}
	err := s.mockHistoryEngine.SignalWorkflowExecution(context.Background(), signalRequest)
	s.EqualError(err, "BadRequestError{Message: Missing domain UUID.}")

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tasklist := "testTaskList"
	identity := "testIdentity"
	signalName := "my signal name"
	input := []byte("test input")
	signalRequest = &history.SignalWorkflowExecutionRequest{
		DomainUUID: common.StringPtr(testDomainID),
		SignalRequest: &workflow.SignalWorkflowExecutionRequest{
			Domain:            common.StringPtr(testDomainID),
			WorkflowExecution: &we,
			Identity:          common.StringPtr(identity),
			SignalName:        common.StringPtr(signalName),
			Input:             input,
		},
	}

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tasklist, []byte("input"), 100, 200, identity)
	addDecisionTaskScheduledEvent(msBuilder)
	ms := createMutableState(msBuilder)
	ms.ExecutionInfo.DomainID = testDomainID
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	err = s.mockHistoryEngine.SignalWorkflowExecution(context.Background(), signalRequest)
	s.Nil(err)
}

// Test signal decision by adding request ID
func (s *engineSuite) TestSignalWorkflowExecution_DuplicateRequest() {
	signalRequest := &history.SignalWorkflowExecutionRequest{}
	err := s.mockHistoryEngine.SignalWorkflowExecution(context.Background(), signalRequest)
	s.EqualError(err, "BadRequestError{Message: Missing domain UUID.}")

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId2"),
		RunId:      common.StringPtr(testRunID),
	}
	tasklist := "testTaskList"
	identity := "testIdentity"
	signalName := "my signal name 2"
	input := []byte("test input 2")
	requestID := uuid.New()
	signalRequest = &history.SignalWorkflowExecutionRequest{
		DomainUUID: common.StringPtr(testDomainID),
		SignalRequest: &workflow.SignalWorkflowExecutionRequest{
			Domain:            common.StringPtr(testDomainID),
			WorkflowExecution: &we,
			Identity:          common.StringPtr(identity),
			SignalName:        common.StringPtr(signalName),
			Input:             input,
			RequestId:         common.StringPtr(requestID),
		},
	}

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tasklist, []byte("input"), 100, 200, identity)
	addDecisionTaskScheduledEvent(msBuilder)
	ms := createMutableState(msBuilder)
	// assume duplicate request id
	ms.SignalRequestedIDs = make(map[string]struct{})
	ms.SignalRequestedIDs[requestID] = struct{}{}
	ms.ExecutionInfo.DomainID = testDomainID
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	err = s.mockHistoryEngine.SignalWorkflowExecution(context.Background(), signalRequest)
	s.Nil(err)
}

func (s *engineSuite) TestSignalWorkflowExecution_Failed() {
	signalRequest := &history.SignalWorkflowExecutionRequest{}
	err := s.mockHistoryEngine.SignalWorkflowExecution(context.Background(), signalRequest)
	s.EqualError(err, "BadRequestError{Message: Missing domain UUID.}")

	we := &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tasklist := "testTaskList"
	identity := "testIdentity"
	signalName := "my signal name"
	input := []byte("test input")
	signalRequest = &history.SignalWorkflowExecutionRequest{
		DomainUUID: common.StringPtr(testDomainID),
		SignalRequest: &workflow.SignalWorkflowExecutionRequest{
			Domain:            common.StringPtr(testDomainID),
			WorkflowExecution: we,
			Identity:          common.StringPtr(identity),
			SignalName:        common.StringPtr(signalName),
			Input:             input,
		},
	}

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, *we, "wType", tasklist, []byte("input"), 100, 200, identity)
	addDecisionTaskScheduledEvent(msBuilder)
	ms := createMutableState(msBuilder)
	ms.ExecutionInfo.State = persistence.WorkflowStateCompleted
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err = s.mockHistoryEngine.SignalWorkflowExecution(context.Background(), signalRequest)
	s.EqualError(err, "EntityNotExistsError{Message: workflow execution already completed}")
}

func (s *engineSuite) TestRemoveSignalMutableState() {
	removeRequest := &history.RemoveSignalMutableStateRequest{}
	err := s.mockHistoryEngine.RemoveSignalMutableState(context.Background(), removeRequest)
	s.EqualError(err, "BadRequestError{Message: Missing domain UUID.}")

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tasklist := "testTaskList"
	identity := "testIdentity"
	requestID := uuid.New()
	removeRequest = &history.RemoveSignalMutableStateRequest{
		DomainUUID:        common.StringPtr(testDomainID),
		WorkflowExecution: &execution,
		RequestId:         common.StringPtr(requestID),
	}

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), testRunID)
	addWorkflowExecutionStartedEvent(msBuilder, execution, "wType", tasklist, []byte("input"), 100, 200, identity)
	addDecisionTaskScheduledEvent(msBuilder)
	ms := createMutableState(msBuilder)
	ms.ExecutionInfo.DomainID = testDomainID
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	err = s.mockHistoryEngine.RemoveSignalMutableState(context.Background(), removeRequest)
	s.Nil(err)
}

func (s *engineSuite) getBuilder(testDomainID string, we workflow.WorkflowExecution) mutableState {
	context, release, err := s.mockHistoryEngine.historyCache.getOrCreateWorkflowExecutionForBackground(testDomainID, we)
	if err != nil {
		return nil
	}
	defer release(nil)

	return context.(*workflowExecutionContextImpl).msBuilder
}

func (s *engineSuite) getActivityScheduledEvent(msBuilder mutableState,
	scheduleID int64) *workflow.HistoryEvent {
	event, _ := msBuilder.GetActivityScheduledEvent(scheduleID)
	return event
}

func (s *engineSuite) printHistory(builder mutableState) string {
	return builder.GetHistoryBuilder().GetHistory().String()
}

func addWorkflowExecutionStartedEventWithParent(builder mutableState, workflowExecution workflow.WorkflowExecution,
	workflowType, taskList string, input []byte, executionStartToCloseTimeout, taskStartToCloseTimeout int32,
	parentInfo *history.ParentExecutionInfo, identity string) *workflow.HistoryEvent {

	startRequest := &workflow.StartWorkflowExecutionRequest{
		WorkflowId:                          common.StringPtr(*workflowExecution.WorkflowId),
		WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
		TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskList)},
		Input:                               input,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(executionStartToCloseTimeout),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(taskStartToCloseTimeout),
		Identity:                            common.StringPtr(identity),
	}

	event, _ := builder.AddWorkflowExecutionStartedEvent(
		workflowExecution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID:          common.StringPtr(testDomainID),
			StartRequest:        startRequest,
			ParentExecutionInfo: parentInfo,
		},
	)

	return event
}

func addWorkflowExecutionStartedEvent(builder mutableState, workflowExecution workflow.WorkflowExecution,
	workflowType, taskList string, input []byte, executionStartToCloseTimeout, taskStartToCloseTimeout int32,
	identity string) *workflow.HistoryEvent {
	return addWorkflowExecutionStartedEventWithParent(builder, workflowExecution, workflowType, taskList, input,
		executionStartToCloseTimeout, taskStartToCloseTimeout, nil, identity)
}

func addDecisionTaskScheduledEvent(builder mutableState) *decisionInfo {
	di, _ := builder.AddDecisionTaskScheduledEvent(false)
	return di
}

func addDecisionTaskStartedEvent(builder mutableState, scheduleID int64, taskList,
	identity string) *workflow.HistoryEvent {
	return addDecisionTaskStartedEventWithRequestID(builder, scheduleID, testRunID, taskList, identity)
}

func addDecisionTaskStartedEventWithRequestID(builder mutableState, scheduleID int64, requestID string,
	taskList, identity string) *workflow.HistoryEvent {
	event, _, _ := builder.AddDecisionTaskStartedEvent(scheduleID, requestID, &workflow.PollForDecisionTaskRequest{
		TaskList: &workflow.TaskList{Name: common.StringPtr(taskList)},
		Identity: common.StringPtr(identity),
	})

	return event
}

func addDecisionTaskCompletedEvent(builder mutableState, scheduleID, startedID int64, context []byte,
	identity string) *workflow.HistoryEvent {
	event, _ := builder.AddDecisionTaskCompletedEvent(scheduleID, startedID, &workflow.RespondDecisionTaskCompletedRequest{
		ExecutionContext: context,
		Identity:         common.StringPtr(identity),
	}, defaultHistoryMaxAutoResetPoints)

	builder.FlushBufferedEvents()

	return event
}

func addActivityTaskScheduledEvent(builder mutableState, decisionCompletedID int64, activityID, activityType,
	taskList string, input []byte, timeout, queueTimeout, heartbeatTimeout int32) (*workflow.HistoryEvent,
	*persistence.ActivityInfo) {

	event, ai, _ := builder.AddActivityTaskScheduledEvent(decisionCompletedID, &workflow.ScheduleActivityTaskDecisionAttributes{
		ActivityId:                    common.StringPtr(activityID),
		ActivityType:                  &workflow.ActivityType{Name: common.StringPtr(activityType)},
		TaskList:                      &workflow.TaskList{Name: common.StringPtr(taskList)},
		Input:                         input,
		ScheduleToCloseTimeoutSeconds: common.Int32Ptr(timeout),
		ScheduleToStartTimeoutSeconds: common.Int32Ptr(queueTimeout),
		HeartbeatTimeoutSeconds:       common.Int32Ptr(heartbeatTimeout),
		StartToCloseTimeoutSeconds:    common.Int32Ptr(1),
	})

	return event, ai
}

func addActivityTaskStartedEvent(builder mutableState, scheduleID int64, identity string) *workflow.HistoryEvent {
	ai, _ := builder.GetActivityInfo(scheduleID)
	event, _ := builder.AddActivityTaskStartedEvent(ai, scheduleID, testRunID, identity)
	return event
}

func addActivityTaskCompletedEvent(builder mutableState, scheduleID, startedID int64, result []byte,
	identity string) *workflow.HistoryEvent {
	event, _ := builder.AddActivityTaskCompletedEvent(scheduleID, startedID, &workflow.RespondActivityTaskCompletedRequest{
		Result:   result,
		Identity: common.StringPtr(identity),
	})

	return event
}

func addActivityTaskFailedEvent(builder mutableState, scheduleID, startedID int64, reason string, details []byte,
	identity string) *workflow.HistoryEvent {
	event, _ := builder.AddActivityTaskFailedEvent(scheduleID, startedID, &workflow.RespondActivityTaskFailedRequest{
		Reason:   common.StringPtr(reason),
		Details:  details,
		Identity: common.StringPtr(identity),
	})

	return event
}

func addTimerStartedEvent(builder mutableState, decisionCompletedEventID int64, timerID string,
	timeOut int64) (*workflow.HistoryEvent, *persistence.TimerInfo) {
	event, ti, _ := builder.AddTimerStartedEvent(decisionCompletedEventID,
		&workflow.StartTimerDecisionAttributes{
			TimerId:                   common.StringPtr(timerID),
			StartToFireTimeoutSeconds: common.Int64Ptr(timeOut),
		})
	return event, ti
}

func addTimerFiredEvent(builder mutableState, scheduleID int64, timerID string) *workflow.HistoryEvent {
	event, _ := builder.AddTimerFiredEvent(scheduleID, timerID)
	return event
}

func addRequestCancelInitiatedEvent(builder mutableState, decisionCompletedEventID int64,
	cancelRequestID, domain, workflowID, runID string) (*workflow.HistoryEvent, *persistence.RequestCancelInfo) {
	event, rci, _ := builder.AddRequestCancelExternalWorkflowExecutionInitiatedEvent(decisionCompletedEventID,
		cancelRequestID, &workflow.RequestCancelExternalWorkflowExecutionDecisionAttributes{
			Domain:     common.StringPtr(domain),
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		})

	return event, rci
}

func addCancelRequestedEvent(builder mutableState, initiatedID int64, domain, workflowID, runID string) *workflow.HistoryEvent {
	event, _ := builder.AddExternalWorkflowExecutionCancelRequested(initiatedID, domain, workflowID, runID)
	return event
}

func addRequestSignalInitiatedEvent(builder mutableState, decisionCompletedEventID int64,
	signalRequestID, domain, workflowID, runID, signalName string, input, control []byte) (*workflow.HistoryEvent, *persistence.SignalInfo) {
	event, si, _ := builder.AddSignalExternalWorkflowExecutionInitiatedEvent(decisionCompletedEventID, signalRequestID,
		&workflow.SignalExternalWorkflowExecutionDecisionAttributes{
			Domain: common.StringPtr(domain),
			Execution: &workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowID),
				RunId:      common.StringPtr(runID),
			},
			SignalName: common.StringPtr(signalName),
			Input:      input,
			Control:    control,
		})

	return event, si
}

func addSignaledEvent(builder mutableState, initiatedID int64, domain, workflowID, runID string, control []byte) *workflow.HistoryEvent {
	event, _ := builder.AddExternalWorkflowExecutionSignaled(initiatedID, domain, workflowID, runID, control)
	return event
}

func addStartChildWorkflowExecutionInitiatedEvent(builder mutableState, decisionCompletedID int64,
	createRequestID, domain, workflowID, workflowType, tasklist string, input []byte,
	executionStartToCloseTimeout, taskStartToCloseTimeout int32) (*workflow.HistoryEvent,
	*persistence.ChildExecutionInfo) {

	event, cei, _ := builder.AddStartChildWorkflowExecutionInitiatedEvent(decisionCompletedID, createRequestID,
		&workflow.StartChildWorkflowExecutionDecisionAttributes{
			Domain:                              common.StringPtr(domain),
			WorkflowId:                          common.StringPtr(workflowID),
			WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
			TaskList:                            &workflow.TaskList{Name: common.StringPtr(tasklist)},
			Input:                               input,
			ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(executionStartToCloseTimeout),
			TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(taskStartToCloseTimeout),
			Control:                             nil,
		})
	return event, cei
}

func addChildWorkflowExecutionStartedEvent(builder mutableState, initiatedID int64, domain, workflowID, runID string,
	workflowType string) *workflow.HistoryEvent {
	event, _ := builder.AddChildWorkflowExecutionStartedEvent(
		common.StringPtr(domain),
		&workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
		&workflow.WorkflowType{Name: common.StringPtr(workflowType)},
		initiatedID,
		&workflow.Header{},
	)
	return event
}

func addChildWorkflowExecutionCompletedEvent(builder mutableState, initiatedID int64, childExecution *workflow.WorkflowExecution,
	attributes *workflow.WorkflowExecutionCompletedEventAttributes) *workflow.HistoryEvent {
	event, _ := builder.AddChildWorkflowExecutionCompletedEvent(initiatedID, childExecution, attributes)
	return event
}

func addCompleteWorkflowEvent(builder mutableState, decisionCompletedEventID int64,
	result []byte) *workflow.HistoryEvent {
	event, _ := builder.AddCompletedWorkflowEvent(decisionCompletedEventID, &workflow.CompleteWorkflowExecutionDecisionAttributes{
		Result: result,
	})

	return event
}

func newMutableStateBuilderWithEventV2(shard ShardContext, eventsCache eventsCache,
	logger log.Logger, runID string) *mutableStateBuilder {

	msBuilder := newMutableStateBuilder(shard, eventsCache, logger, testLocalDomainEntry)
	_ = msBuilder.SetHistoryTree(runID)

	return msBuilder
}

func newMutableStateBuilderWithReplicationStateWithEventV2(shard ShardContext, eventsCache eventsCache,
	logger log.Logger, version int64, runID string) *mutableStateBuilder {

	msBuilder := newMutableStateBuilderWithReplicationState(shard, eventsCache, logger, testGlobalDomainEntry)
	msBuilder.GetReplicationState().StartVersion = version
	msBuilder.UpdateCurrentVersion(version, true)
	_ = msBuilder.SetHistoryTree(runID)

	return msBuilder
}

func createMutableState(ms mutableState) *persistence.WorkflowMutableState {
	builder := ms.(*mutableStateBuilder)
	builder.FlushBufferedEvents()
	info := copyWorkflowExecutionInfo(builder.executionInfo)
	stats := &persistence.ExecutionStats{}
	activityInfos := make(map[int64]*persistence.ActivityInfo)
	for id, info := range builder.pendingActivityInfoIDs {
		activityInfos[id] = copyActivityInfo(info)
	}
	timerInfos := make(map[string]*persistence.TimerInfo)
	for id, info := range builder.pendingTimerInfoIDs {
		timerInfos[id] = copyTimerInfo(info)
	}
	cancellationInfos := make(map[int64]*persistence.RequestCancelInfo)
	for id, info := range builder.pendingRequestCancelInfoIDs {
		cancellationInfos[id] = copyCancellationInfo(info)
	}
	signalInfos := make(map[int64]*persistence.SignalInfo)
	for id, info := range builder.pendingSignalInfoIDs {
		signalInfos[id] = copySignalInfo(info)
	}
	childInfos := make(map[int64]*persistence.ChildExecutionInfo)
	for id, info := range builder.pendingChildExecutionInfoIDs {
		childInfos[id] = copyChildInfo(info)
	}

	builder.FlushBufferedEvents()
	var bufferedEvents []*workflow.HistoryEvent
	if len(builder.bufferedEvents) > 0 {
		bufferedEvents = append(bufferedEvents, builder.bufferedEvents...)
	}
	if len(builder.updateBufferedEvents) > 0 {
		bufferedEvents = append(bufferedEvents, builder.updateBufferedEvents...)
	}
	var replicationState *persistence.ReplicationState
	if builder.replicationState != nil {
		replicationState = copyReplicationState(builder.replicationState)
	}

	return &persistence.WorkflowMutableState{
		ExecutionInfo:       info,
		ExecutionStats:      stats,
		ActivityInfos:       activityInfos,
		TimerInfos:          timerInfos,
		BufferedEvents:      bufferedEvents,
		SignalInfos:         signalInfos,
		RequestCancelInfos:  cancellationInfos,
		ChildExecutionInfos: childInfos,
		ReplicationState:    replicationState,
	}
}

func copyWorkflowExecutionInfo(sourceInfo *persistence.WorkflowExecutionInfo) *persistence.WorkflowExecutionInfo {
	return &persistence.WorkflowExecutionInfo{
		DomainID:                           sourceInfo.DomainID,
		WorkflowID:                         sourceInfo.WorkflowID,
		RunID:                              sourceInfo.RunID,
		ParentDomainID:                     sourceInfo.ParentDomainID,
		ParentWorkflowID:                   sourceInfo.ParentWorkflowID,
		ParentRunID:                        sourceInfo.ParentRunID,
		InitiatedID:                        sourceInfo.InitiatedID,
		CompletionEventBatchID:             sourceInfo.CompletionEventBatchID,
		CompletionEvent:                    sourceInfo.CompletionEvent,
		TaskList:                           sourceInfo.TaskList,
		StickyTaskList:                     sourceInfo.StickyTaskList,
		StickyScheduleToStartTimeout:       sourceInfo.StickyScheduleToStartTimeout,
		WorkflowTypeName:                   sourceInfo.WorkflowTypeName,
		WorkflowTimeout:                    sourceInfo.WorkflowTimeout,
		DecisionTimeoutValue:               sourceInfo.DecisionTimeoutValue,
		ExecutionContext:                   sourceInfo.ExecutionContext,
		State:                              sourceInfo.State,
		CloseStatus:                        sourceInfo.CloseStatus,
		LastFirstEventID:                   sourceInfo.LastFirstEventID,
		LastEventTaskID:                    sourceInfo.LastEventTaskID,
		NextEventID:                        sourceInfo.NextEventID,
		LastProcessedEvent:                 sourceInfo.LastProcessedEvent,
		StartTimestamp:                     sourceInfo.StartTimestamp,
		LastUpdatedTimestamp:               sourceInfo.LastUpdatedTimestamp,
		CreateRequestID:                    sourceInfo.CreateRequestID,
		SignalCount:                        sourceInfo.SignalCount,
		DecisionVersion:                    sourceInfo.DecisionVersion,
		DecisionScheduleID:                 sourceInfo.DecisionScheduleID,
		DecisionStartedID:                  sourceInfo.DecisionStartedID,
		DecisionRequestID:                  sourceInfo.DecisionRequestID,
		DecisionTimeout:                    sourceInfo.DecisionTimeout,
		DecisionAttempt:                    sourceInfo.DecisionAttempt,
		DecisionStartedTimestamp:           sourceInfo.DecisionStartedTimestamp,
		DecisionOriginalScheduledTimestamp: sourceInfo.DecisionOriginalScheduledTimestamp,
		CancelRequested:                    sourceInfo.CancelRequested,
		CancelRequestID:                    sourceInfo.CancelRequestID,
		CronSchedule:                       sourceInfo.CronSchedule,
		ClientLibraryVersion:               sourceInfo.ClientLibraryVersion,
		ClientFeatureVersion:               sourceInfo.ClientFeatureVersion,
		ClientImpl:                         sourceInfo.ClientImpl,
		AutoResetPoints:                    sourceInfo.AutoResetPoints,
		Memo:                               sourceInfo.Memo,
		SearchAttributes:                   sourceInfo.SearchAttributes,
		Attempt:                            sourceInfo.Attempt,
		HasRetryPolicy:                     sourceInfo.HasRetryPolicy,
		InitialInterval:                    sourceInfo.InitialInterval,
		BackoffCoefficient:                 sourceInfo.BackoffCoefficient,
		MaximumInterval:                    sourceInfo.MaximumInterval,
		ExpirationTime:                     sourceInfo.ExpirationTime,
		MaximumAttempts:                    sourceInfo.MaximumAttempts,
		NonRetriableErrors:                 sourceInfo.NonRetriableErrors,
		BranchToken:                        sourceInfo.BranchToken,
		ExpirationSeconds:                  sourceInfo.ExpirationSeconds,
	}
}

func copyActivityInfo(sourceInfo *persistence.ActivityInfo) *persistence.ActivityInfo {
	details := make([]byte, len(sourceInfo.Details))
	copy(details, sourceInfo.Details)

	var scheduledEvent *workflow.HistoryEvent
	var startedEvent *workflow.HistoryEvent
	if sourceInfo.ScheduledEvent != nil {
		scheduledEvent = &workflow.HistoryEvent{}
		wv, err := sourceInfo.ScheduledEvent.ToWire()
		if err != nil {
			panic(err)
		}
		err = scheduledEvent.FromWire(wv)
		if err != nil {
			panic(err)
		}
	}
	if sourceInfo.StartedEvent != nil {
		startedEvent = &workflow.HistoryEvent{}
		wv, err := sourceInfo.StartedEvent.ToWire()
		if err != nil {
			panic(err)
		}
		err = startedEvent.FromWire(wv)
		if err != nil {
			panic(err)
		}
	}

	return &persistence.ActivityInfo{
		Version:                  sourceInfo.Version,
		ScheduleID:               sourceInfo.ScheduleID,
		ScheduledEventBatchID:    sourceInfo.ScheduledEventBatchID,
		ScheduledEvent:           scheduledEvent,
		StartedID:                sourceInfo.StartedID,
		StartedEvent:             startedEvent,
		ActivityID:               sourceInfo.ActivityID,
		RequestID:                sourceInfo.RequestID,
		Details:                  details,
		ScheduledTime:            sourceInfo.ScheduledTime,
		StartedTime:              sourceInfo.StartedTime,
		ScheduleToStartTimeout:   sourceInfo.ScheduleToStartTimeout,
		ScheduleToCloseTimeout:   sourceInfo.ScheduleToCloseTimeout,
		StartToCloseTimeout:      sourceInfo.StartToCloseTimeout,
		HeartbeatTimeout:         sourceInfo.HeartbeatTimeout,
		LastHeartBeatUpdatedTime: sourceInfo.LastHeartBeatUpdatedTime,
		CancelRequested:          sourceInfo.CancelRequested,
		CancelRequestID:          sourceInfo.CancelRequestID,
		TimerTaskStatus:          sourceInfo.TimerTaskStatus,
		Attempt:                  sourceInfo.Attempt,
		DomainID:                 sourceInfo.DomainID,
		StartedIdentity:          sourceInfo.StartedIdentity,
		TaskList:                 sourceInfo.TaskList,
		HasRetryPolicy:           sourceInfo.HasRetryPolicy,
		InitialInterval:          sourceInfo.InitialInterval,
		BackoffCoefficient:       sourceInfo.BackoffCoefficient,
		MaximumInterval:          sourceInfo.MaximumInterval,
		ExpirationTime:           sourceInfo.ExpirationTime,
		MaximumAttempts:          sourceInfo.MaximumAttempts,
		NonRetriableErrors:       sourceInfo.NonRetriableErrors,
		LastFailureReason:        sourceInfo.LastFailureReason,
		LastWorkerIdentity:       sourceInfo.LastWorkerIdentity,
		LastFailureDetails:       sourceInfo.LastFailureDetails,
		//// Not written to database - This is used only for deduping heartbeat timer creation
		// LastHeartbeatTimeoutVisibility: sourceInfo.LastHeartbeatTimeoutVisibility,
	}
}

func copyTimerInfo(sourceInfo *persistence.TimerInfo) *persistence.TimerInfo {
	return &persistence.TimerInfo{
		Version:    sourceInfo.Version,
		TimerID:    sourceInfo.TimerID,
		StartedID:  sourceInfo.StartedID,
		ExpiryTime: sourceInfo.ExpiryTime,
		TaskID:     sourceInfo.TaskID,
	}
}

func copyCancellationInfo(sourceInfo *persistence.RequestCancelInfo) *persistence.RequestCancelInfo {
	return &persistence.RequestCancelInfo{
		Version:         sourceInfo.Version,
		InitiatedID:     sourceInfo.InitiatedID,
		CancelRequestID: sourceInfo.CancelRequestID,
	}
}

func copySignalInfo(sourceInfo *persistence.SignalInfo) *persistence.SignalInfo {
	result := &persistence.SignalInfo{
		Version:         sourceInfo.Version,
		InitiatedID:     sourceInfo.InitiatedID,
		SignalRequestID: sourceInfo.SignalRequestID,
		SignalName:      sourceInfo.SignalName,
	}
	result.Input = make([]byte, len(sourceInfo.Input))
	copy(result.Input, sourceInfo.Input)
	result.Control = make([]byte, len(sourceInfo.Control))
	copy(result.Control, sourceInfo.Control)
	return result
}

func copyChildInfo(sourceInfo *persistence.ChildExecutionInfo) *persistence.ChildExecutionInfo {
	result := &persistence.ChildExecutionInfo{
		Version:               sourceInfo.Version,
		InitiatedID:           sourceInfo.InitiatedID,
		InitiatedEventBatchID: sourceInfo.InitiatedEventBatchID,
		StartedID:             sourceInfo.StartedID,
		StartedWorkflowID:     sourceInfo.StartedWorkflowID,
		StartedRunID:          sourceInfo.StartedRunID,
		CreateRequestID:       sourceInfo.CreateRequestID,
		DomainName:            sourceInfo.DomainName,
		WorkflowTypeName:      sourceInfo.WorkflowTypeName,
		ParentClosePolicy:     sourceInfo.ParentClosePolicy,
	}

	if sourceInfo.InitiatedEvent != nil {
		result.InitiatedEvent = &workflow.HistoryEvent{}
		wv, err := sourceInfo.InitiatedEvent.ToWire()
		if err != nil {
			panic(err)
		}
		err = result.InitiatedEvent.FromWire(wv)
		if err != nil {
			panic(err)
		}
	}
	if sourceInfo.StartedEvent != nil {
		result.StartedEvent = &workflow.HistoryEvent{}
		wv, err := sourceInfo.StartedEvent.ToWire()
		if err != nil {
			panic(err)
		}
		err = result.StartedEvent.FromWire(wv)
		if err != nil {
			panic(err)
		}
	}
	return result
}

func copyReplicationState(source *persistence.ReplicationState) *persistence.ReplicationState {
	var lastReplicationInfo map[string]*persistence.ReplicationInfo
	if source.LastReplicationInfo != nil {
		lastReplicationInfo = map[string]*persistence.ReplicationInfo{}
		for k, v := range source.LastReplicationInfo {
			lastReplicationInfo[k] = &persistence.ReplicationInfo{
				Version:     v.Version,
				LastEventID: v.LastEventID,
			}
		}
	}

	return &persistence.ReplicationState{
		CurrentVersion:      source.CurrentVersion,
		StartVersion:        source.StartVersion,
		LastWriteVersion:    source.LastWriteVersion,
		LastWriteEventID:    source.LastWriteEventID,
		LastReplicationInfo: lastReplicationInfo,
	}
}
