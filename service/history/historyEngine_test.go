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
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/serviceerror"
	"go.temporal.io/temporal-proto/workflowservice"

	"github.com/temporalio/temporal/.gen/proto/historyservice"
	"github.com/temporalio/temporal/.gen/proto/historyservicemock"
	"github.com/temporalio/temporal/.gen/proto/matchingservice"
	"github.com/temporalio/temporal/.gen/proto/matchingservicemock"
	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/.gen/proto/replication"
	"github.com/temporalio/temporal/.gen/proto/token"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/clock"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/headers"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/loggerimpl"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/mocks"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/primitives"
	cconfig "github.com/temporalio/temporal/common/service/config"
	"github.com/temporalio/temporal/common/service/dynamicconfig"
)

type (
	engineSuite struct {
		suite.Suite
		*require.Assertions

		controller               *gomock.Controller
		mockShard                *shardContextTest
		mockTxProcessor          *MocktransferQueueProcessor
		mockReplicationProcessor *MockReplicatorQueueProcessor
		mockTimerProcessor       *MocktimerQueueProcessor
		mockDomainCache          *cache.MockDomainCache
		mockMatchingClient       *matchingservicemock.MockMatchingServiceClient
		mockHistoryClient        *historyservicemock.MockHistoryServiceClient
		mockClusterMetadata      *cluster.MockMetadata

		mockHistoryEngine *historyEngineImpl
		mockExecutionMgr  *mocks.ExecutionManager
		mockHistoryV2Mgr  *mocks.HistoryV2Manager
		mockShardManager  *mocks.ShardManager

		eventsCache eventsCache
		config      *Config
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
var testWorkflowID = "random-workflow-id"
var testRunID = "0d00698f-08e1-4d36-a3e2-3bf109f5d2d6"

var testLocalDomainEntry = cache.NewLocalDomainCacheEntryForTest(
	&persistence.DomainInfo{ID: testDomainID, Name: testDomainName},
	&persistence.DomainConfig{Retention: 1},
	cluster.TestCurrentClusterName,
	nil,
)

var testGlobalDomainEntry = cache.NewGlobalDomainCacheEntryForTest(
	&persistence.DomainInfo{ID: testDomainID, Name: testDomainName},
	&persistence.DomainConfig{
		Retention:                1,
		VisibilityArchivalStatus: enums.ArchivalStatusEnabled,
		VisibilityArchivalURI:    "test:///visibility/archival",
	},
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

var testGlobalParentDomainEntry = cache.NewGlobalDomainCacheEntryForTest(
	&persistence.DomainInfo{ID: testParentDomainID, Name: testParentDomainName},
	&persistence.DomainConfig{Retention: 1},
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

var testGlobalTargetDomainEntry = cache.NewGlobalDomainCacheEntryForTest(
	&persistence.DomainInfo{ID: testTargetDomainID, Name: testTargetDomainName},
	&persistence.DomainConfig{Retention: 1},
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

var testGlobalChildDomainEntry = cache.NewGlobalDomainCacheEntryForTest(
	&persistence.DomainInfo{ID: testChildDomainID, Name: testChildDomainName},
	&persistence.DomainConfig{Retention: 1},
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

func NewDynamicConfigForTest() *Config {
	dc := dynamicconfig.NewNopCollection()
	config := NewConfig(dc, 1, cconfig.StoreTypeCassandra, false)
	// reduce the duration of long poll to increase test speed
	config.LongPollExpirationInterval = dc.GetDurationPropertyFilteredByDomain(dynamicconfig.HistoryLongPollExpirationInterval, 10*time.Second)
	config.EnableConsistentQueryByDomain = dc.GetBoolPropertyFnWithDomainFilter(dynamicconfig.EnableConsistentQueryByDomain, true)
	return config
}

func TestEngineSuite(t *testing.T) {
	s := new(engineSuite)
	suite.Run(t, s)
}

func (s *engineSuite) SetupSuite() {
	s.config = NewDynamicConfigForTest()
}

func (s *engineSuite) TearDownSuite() {
}

func (s *engineSuite) SetupTest() {
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
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistenceblobs.ShardInfo{
				RangeID:          1,
				TransferAckLevel: 0,
			}},
		s.config,
	)
	s.eventsCache = newEventsCache(s.mockShard)
	s.mockShard.eventsCache = s.eventsCache

	s.mockMatchingClient = s.mockShard.resource.MatchingClient
	s.mockHistoryClient = s.mockShard.resource.HistoryClient
	s.mockExecutionMgr = s.mockShard.resource.ExecutionMgr
	s.mockHistoryV2Mgr = s.mockShard.resource.HistoryMgr
	s.mockShardManager = s.mockShard.resource.ShardMgr
	s.mockClusterMetadata = s.mockShard.resource.ClusterMetadata
	s.mockDomainCache = s.mockShard.resource.DomainCache
	s.mockClusterMetadata.EXPECT().IsGlobalDomainEnabled().Return(false).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestSingleDCClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(common.EmptyVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomainByID(testDomainID).Return(testLocalDomainEntry, nil).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomain(testDomainName).Return(testLocalDomainEntry, nil).AnyTimes()

	historyEventNotifier := newHistoryEventNotifier(
		clock.NewRealTimeSource(),
		s.mockShard.resource.MetricsClient,
		func(workflowID string) int {
			return len(workflowID)
		},
	)

	historyCache := newHistoryCache(s.mockShard)
	h := &historyEngineImpl{
		currentClusterName:   s.mockShard.GetClusterMetadata().GetCurrentClusterName(),
		shard:                s.mockShard,
		clusterMetadata:      s.mockClusterMetadata,
		executionManager:     s.mockExecutionMgr,
		historyV2Mgr:         s.mockHistoryV2Mgr,
		historyCache:         historyCache,
		logger:               s.mockShard.GetLogger(),
		metricsClient:        s.mockShard.GetMetricsClient(),
		tokenSerializer:      common.NewProtoTaskTokenSerializer(),
		historyEventNotifier: historyEventNotifier,
		config:               NewDynamicConfigForTest(),
		txProcessor:          s.mockTxProcessor,
		replicatorProcessor:  s.mockReplicationProcessor,
		timerProcessor:       s.mockTimerProcessor,
		versionChecker:       headers.NewVersionChecker(),
	}
	s.mockShard.SetEngine(h)
	h.decisionHandler = newDecisionHandler(h)

	h.historyEventNotifier.Start()

	s.mockHistoryEngine = h
}

func (s *engineSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.Finish(s.T())
	s.mockHistoryEngine.historyEventNotifier.Stop()
}

func (s *engineSuite) TestGetMutableStateSync() {
	ctx := context.Background()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "test-get-workflow-execution-event-id",
		RunId:      testRunID,
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
	response, err := s.mockHistoryEngine.GetMutableState(ctx, &historyservice.GetMutableStateRequest{
		DomainUUID: testDomainID,
		Execution:  &execution,
	})
	s.Nil(err)
	s.Equal(int64(4), response.GetNextEventId())
}

func (s *engineSuite) TestGetMutableState_IntestRunID() {
	ctx := context.Background()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "test-get-workflow-execution-event-id",
		RunId:      "run-id-not-valid-uuid",
	}

	_, err := s.mockHistoryEngine.GetMutableState(ctx, &historyservice.GetMutableStateRequest{
		DomainUUID: testDomainID,
		Execution:  &execution,
	})
	s.Equal(errRunIDNotValid, err)
}

func (s *engineSuite) TestGetMutableState_EmptyRunID() {
	ctx := context.Background()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "test-get-workflow-execution-event-id",
	}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(nil, serviceerror.NewNotFound("")).Once()

	_, err := s.mockHistoryEngine.GetMutableState(ctx, &historyservice.GetMutableStateRequest{
		DomainUUID: testDomainID,
		Execution:  &execution,
	})
	s.Equal(&serviceerror.NotFound{}, err)
}

func (s *engineSuite) TestGetMutableStateLongPoll() {
	ctx := context.Background()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "test-get-workflow-execution-event-id",
		RunId:      testRunID,
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
		tt := &token.Task{
			WorkflowId: execution.WorkflowId,
			RunId:      primitives.MustParseUUID(execution.RunId),
			ScheduleId: 2,
		}
		taskToken, _ := tt.Marshal()
		s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
		s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

		timer := time.NewTimer(delay)

		<-timer.C
		_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &historyservice.RespondDecisionTaskCompletedRequest{
			DomainUUID: testDomainID,
			CompleteRequest: &workflowservice.RespondDecisionTaskCompletedRequest{
				TaskToken: taskToken,
				Identity:  identity,
			},
		})
		s.Nil(err)
		waitGroup.Done()
		// right now the next event ID is 5
	}

	// return immediately, since the expected next event ID appears
	response, err := s.mockHistoryEngine.GetMutableState(ctx, &historyservice.GetMutableStateRequest{
		DomainUUID:          testDomainID,
		Execution:           &execution,
		ExpectedNextEventId: 3,
	})
	s.Nil(err)
	s.Equal(int64(4), response.NextEventId)

	// long poll, new event happen before long poll timeout
	go asycWorkflowUpdate(time.Second * 2)
	start := time.Now()
	pollResponse, err := s.mockHistoryEngine.PollMutableState(ctx, &historyservice.PollMutableStateRequest{
		DomainUUID:          testDomainID,
		Execution:           &execution,
		ExpectedNextEventId: 4,
	})
	s.True(time.Now().After(start.Add(time.Second * 1)))
	s.Nil(err)
	s.Equal(int64(5), pollResponse.GetNextEventId())
	waitGroup.Wait()
}

func (s *engineSuite) TestGetMutableStateLongPoll_CurrentBranchChanged() {
	ctx := context.Background()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "test-get-workflow-execution-event-id",
		RunId:      testRunID,
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
		newExecution := &commonproto.WorkflowExecution{
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
			persistence.WorkflowCloseStatusRunning))
	}

	// return immediately, since the expected next event ID appears
	response0, err := s.mockHistoryEngine.GetMutableState(ctx, &historyservice.GetMutableStateRequest{
		DomainUUID:          testDomainID,
		Execution:           &execution,
		ExpectedNextEventId: 3,
	})
	s.Nil(err)
	s.Equal(int64(4), response0.GetNextEventId())

	// long poll, new event happen before long poll timeout
	go asyncBranchTokenUpdate(time.Second * 2)
	start := time.Now()
	response1, err := s.mockHistoryEngine.GetMutableState(ctx, &historyservice.GetMutableStateRequest{
		DomainUUID:          testDomainID,
		Execution:           &execution,
		ExpectedNextEventId: 10,
	})
	s.True(time.Now().After(start.Add(time.Second * 1)))
	s.Nil(err)
	s.Equal(response0.GetCurrentBranchToken(), response1.GetCurrentBranchToken())
}

func (s *engineSuite) TestGetMutableStateLongPollTimeout() {
	ctx := context.Background()

	execution := commonproto.WorkflowExecution{
		WorkflowId: "test-get-workflow-execution-event-id",
		RunId:      testRunID,
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
	response, err := s.mockHistoryEngine.GetMutableState(ctx, &historyservice.GetMutableStateRequest{
		DomainUUID:          testDomainID,
		Execution:           &execution,
		ExpectedNextEventId: 4,
	})
	s.Nil(err)
	s.Equal(int64(4), response.GetNextEventId())
}

func (s *engineSuite) TestQueryWorkflow_RejectBasedOnNotEnabled() {
	s.mockHistoryEngine.config.EnableConsistentQueryByDomain = dynamicconfig.GetBoolPropertyFnFilteredByDomain(false)
	request := &historyservice.QueryWorkflowRequest{
		DomainUUID: testDomainID,
		Request: &workflowservice.QueryWorkflowRequest{
			QueryConsistencyLevel: enums.QueryConsistencyLevelStrong,
		},
	}
	resp, err := s.mockHistoryEngine.QueryWorkflow(context.Background(), request)
	s.Nil(resp)
	s.Equal(ErrConsistentQueryNotEnabled, err)

	s.mockHistoryEngine.config.EnableConsistentQueryByDomain = dynamicconfig.GetBoolPropertyFnFilteredByDomain(true)
	s.mockHistoryEngine.config.EnableConsistentQuery = dynamicconfig.GetBoolPropertyFn(false)
	resp, err = s.mockHistoryEngine.QueryWorkflow(context.Background(), request)
	s.Nil(resp)
	s.Equal(ErrConsistentQueryNotEnabled, err)
}

func (s *engineSuite) TestQueryWorkflow_RejectBasedOnCompleted() {
	execution := commonproto.WorkflowExecution{
		WorkflowId: "TestQueryWorkflow_RejectBasedOnCompleted",
		RunId:      testRunID,
	}
	tasklist := "testTaskList"
	identity := "testIdentity"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache, loggerimpl.NewDevelopmentForTest(s.Suite), execution.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, execution, "wType", tasklist, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	event := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tasklist, identity)
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, di.StartedID, nil, "some random identity")
	addCompleteWorkflowEvent(msBuilder, event.GetEventId(), nil)
	ms := createMutableState(msBuilder)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gweResponse, nil).Once()

	request := &historyservice.QueryWorkflowRequest{
		DomainUUID: testDomainID,
		Request: &workflowservice.QueryWorkflowRequest{
			Execution:            &execution,
			Query:                &commonproto.WorkflowQuery{},
			QueryRejectCondition: enums.QueryRejectConditionNotOpen,
		},
	}
	resp, err := s.mockHistoryEngine.QueryWorkflow(context.Background(), request)
	s.NoError(err)
	s.Nil(resp.GetResponse().QueryResult)
	s.NotNil(resp.GetResponse().QueryRejected)
	s.EqualValues(enums.WorkflowExecutionCloseStatusCompleted, resp.GetResponse().GetQueryRejected().CloseStatus)
}

func (s *engineSuite) TestQueryWorkflow_RejectBasedOnFailed() {
	execution := commonproto.WorkflowExecution{
		WorkflowId: "TestQueryWorkflow_RejectBasedOnFailed",
		RunId:      testRunID,
	}
	tasklist := "testTaskList"
	identity := "testIdentity"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache, loggerimpl.NewDevelopmentForTest(s.Suite), execution.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, execution, "wType", tasklist, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	event := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tasklist, identity)
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, di.StartedID, nil, "some random identity")
	addFailWorkflowEvent(msBuilder, event.GetEventId(), "failure reason", []byte{1, 2, 3})
	ms := createMutableState(msBuilder)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gweResponse, nil).Once()

	request := &historyservice.QueryWorkflowRequest{
		DomainUUID: testDomainID,
		Request: &workflowservice.QueryWorkflowRequest{
			Execution:            &execution,
			Query:                &commonproto.WorkflowQuery{},
			QueryRejectCondition: enums.QueryRejectConditionNotOpen,
		},
	}
	resp, err := s.mockHistoryEngine.QueryWorkflow(context.Background(), request)
	s.NoError(err)
	s.Nil(resp.GetResponse().QueryResult)
	s.NotNil(resp.GetResponse().QueryRejected)
	s.EqualValues(enums.WorkflowExecutionCloseStatusFailed, resp.GetResponse().GetQueryRejected().CloseStatus)

	request = &historyservice.QueryWorkflowRequest{
		DomainUUID: testDomainID,
		Request: &workflowservice.QueryWorkflowRequest{
			Execution:            &execution,
			Query:                &commonproto.WorkflowQuery{},
			QueryRejectCondition: enums.QueryRejectConditionNotCompletedCleanly,
		},
	}
	resp, err = s.mockHistoryEngine.QueryWorkflow(context.Background(), request)
	s.NoError(err)
	s.Nil(resp.GetResponse().QueryResult)
	s.NotNil(resp.GetResponse().QueryRejected)
	s.EqualValues(enums.WorkflowExecutionCloseStatusFailed, resp.GetResponse().GetQueryRejected().CloseStatus)
}

func (s *engineSuite) TestQueryWorkflow_FirstDecisionNotCompleted() {
	execution := commonproto.WorkflowExecution{
		WorkflowId: "TestQueryWorkflow_FirstDecisionNotCompleted",
		RunId:      testRunID,
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

	request := &historyservice.QueryWorkflowRequest{
		DomainUUID: testDomainID,
		Request: &workflowservice.QueryWorkflowRequest{
			Execution: &execution,
			Query:     &commonproto.WorkflowQuery{},
		},
	}
	resp, err := s.mockHistoryEngine.QueryWorkflow(context.Background(), request)
	s.Equal(ErrQueryWorkflowBeforeFirstDecision, err)
	s.Nil(resp)
}

func (s *engineSuite) TestQueryWorkflow_DirectlyThroughMatching() {
	execution := commonproto.WorkflowExecution{
		WorkflowId: "TestQueryWorkflow_DirectlyThroughMatching",
		RunId:      testRunID,
	}
	tasklist := "testTaskList"
	identity := "testIdentity"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache, loggerimpl.NewDevelopmentForTest(s.Suite), execution.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, execution, "wType", tasklist, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	startedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tasklist, identity)
	addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, startedEvent.EventId, nil, identity)
	di = addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tasklist, identity)

	ms := createMutableState(msBuilder)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gweResponse, nil).Once()
	s.mockMatchingClient.EXPECT().QueryWorkflow(gomock.Any(), gomock.Any()).Return(&matchingservice.QueryWorkflowResponse{QueryResult: []byte{1, 2, 3}}, nil)
	s.mockHistoryEngine.matchingClient = s.mockMatchingClient
	request := &historyservice.QueryWorkflowRequest{
		DomainUUID: testDomainID,
		Request: &workflowservice.QueryWorkflowRequest{
			Execution: &execution,
			Query:     &commonproto.WorkflowQuery{},
			// since workflow is open this filter does not reject query
			QueryRejectCondition:  enums.QueryRejectConditionNotOpen,
			QueryConsistencyLevel: enums.QueryConsistencyLevelEventual,
		},
	}
	resp, err := s.mockHistoryEngine.QueryWorkflow(context.Background(), request)
	s.NoError(err)
	s.NotNil(resp.GetResponse().QueryResult)
	s.Nil(resp.GetResponse().QueryRejected)
	s.Equal([]byte{1, 2, 3}, resp.GetResponse().GetQueryResult())
}

func (s *engineSuite) TestQueryWorkflow_DecisionTaskDispatch_Timeout() {
	execution := commonproto.WorkflowExecution{
		WorkflowId: "TestQueryWorkflow_DecisionTaskDispatch_Timeout",
		RunId:      testRunID,
	}
	tasklist := "testTaskList"
	identity := "testIdentity"
	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache, loggerimpl.NewDevelopmentForTest(s.Suite), execution.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, execution, "wType", tasklist, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	startedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tasklist, identity)
	addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, startedEvent.EventId, nil, identity)
	di = addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tasklist, identity)

	ms := createMutableState(msBuilder)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gweResponse, nil).Once()
	request := &historyservice.QueryWorkflowRequest{
		DomainUUID: testDomainID,
		Request: &workflowservice.QueryWorkflowRequest{
			Execution: &execution,
			Query:     &commonproto.WorkflowQuery{},
			// since workflow is open this filter does not reject query
			QueryRejectCondition:  enums.QueryRejectConditionNotOpen,
			QueryConsistencyLevel: enums.QueryConsistencyLevelStrong,
		},
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
		defer cancel()
		resp, err := s.mockHistoryEngine.QueryWorkflow(ctx, request)
		s.Error(err)
		s.Nil(resp)
		wg.Done()
	}()

	<-time.After(time.Second)
	builder := s.getBuilder(testDomainID, execution)
	s.NotNil(builder)
	qr := builder.GetQueryRegistry()
	s.True(qr.hasBufferedQuery())
	s.False(qr.hasCompletedQuery())
	s.False(qr.hasUnblockedQuery())
	s.False(qr.hasFailedQuery())
	wg.Wait()
	s.False(qr.hasBufferedQuery())
	s.False(qr.hasCompletedQuery())
	s.False(qr.hasUnblockedQuery())
	s.False(qr.hasFailedQuery())
}

func (s *engineSuite) TestQueryWorkflow_ConsistentQueryBufferFull() {
	execution := commonproto.WorkflowExecution{
		WorkflowId: "TestQueryWorkflow_ConsistentQueryBufferFull",
		RunId:      testRunID,
	}
	tasklist := "testTaskList"
	identity := "testIdentity"
	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache, loggerimpl.NewDevelopmentForTest(s.Suite), execution.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, execution, "wType", tasklist, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	startedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tasklist, identity)
	addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, startedEvent.EventId, nil, identity)
	di = addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tasklist, identity)

	ms := createMutableState(msBuilder)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gweResponse, nil).Once()

	// buffer query so that when history.QueryWorkflow is called buffer is already full
	ctx, release, err := s.mockHistoryEngine.historyCache.getOrCreateWorkflowExecutionForBackground(testDomainID, execution)
	s.NoError(err)
	loadedMS, err := ctx.loadWorkflowExecution()
	s.NoError(err)
	qr := newQueryRegistry()
	qr.bufferQuery(&commonproto.WorkflowQuery{})
	loadedMS.(*mutableStateBuilder).queryRegistry = qr
	release(nil)

	request := &historyservice.QueryWorkflowRequest{
		DomainUUID: testDomainID,
		Request: &workflowservice.QueryWorkflowRequest{
			Execution:             &execution,
			Query:                 &commonproto.WorkflowQuery{},
			QueryConsistencyLevel: enums.QueryConsistencyLevelStrong,
		},
	}
	resp, err := s.mockHistoryEngine.QueryWorkflow(context.Background(), request)
	s.Nil(resp)
	s.Equal(ErrConsistentQueryBufferExceeded, err)
}

func (s *engineSuite) TestQueryWorkflow_DecisionTaskDispatch_Complete() {
	execution := commonproto.WorkflowExecution{
		WorkflowId: "TestQueryWorkflow_DecisionTaskDispatch_Complete",
		RunId:      testRunID,
	}
	tasklist := "testTaskList"
	identity := "testIdentity"
	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache, loggerimpl.NewDevelopmentForTest(s.Suite), execution.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, execution, "wType", tasklist, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	startedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tasklist, identity)
	addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, startedEvent.EventId, nil, identity)
	di = addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tasklist, identity)

	ms := createMutableState(msBuilder)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gweResponse, nil).Once()

	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(1)
	asyncQueryUpdate := func(delay time.Duration, answer []byte) {
		defer waitGroup.Done()
		<-time.After(delay)
		builder := s.getBuilder(testDomainID, execution)
		s.NotNil(builder)
		qr := builder.GetQueryRegistry()
		buffered := qr.getBufferedIDs()
		for _, id := range buffered {
			resultType := enums.QueryResultTypeAnswered
			completedTerminationState := &queryTerminationState{
				queryTerminationType: queryTerminationTypeCompleted,
				queryResult: &commonproto.WorkflowQueryResult{
					ResultType: resultType,
					Answer:     answer,
				},
			}
			err := qr.setTerminationState(id, completedTerminationState)
			s.NoError(err)
			state, err := qr.getTerminationState(id)
			s.NoError(err)
			s.Equal(queryTerminationTypeCompleted, state.queryTerminationType)
		}
	}

	request := &historyservice.QueryWorkflowRequest{
		DomainUUID: testDomainID,
		Request: &workflowservice.QueryWorkflowRequest{
			Execution:             &execution,
			Query:                 &commonproto.WorkflowQuery{},
			QueryConsistencyLevel: enums.QueryConsistencyLevelStrong,
		},
	}
	go asyncQueryUpdate(time.Second*2, []byte{1, 2, 3})
	start := time.Now()
	resp, err := s.mockHistoryEngine.QueryWorkflow(context.Background(), request)
	s.True(time.Now().After(start.Add(time.Second)))
	s.NoError(err)
	s.Equal([]byte{1, 2, 3}, resp.GetResponse().GetQueryResult())
	builder := s.getBuilder(testDomainID, execution)
	s.NotNil(builder)
	qr := builder.GetQueryRegistry()
	s.False(qr.hasBufferedQuery())
	s.False(qr.hasCompletedQuery())
	waitGroup.Wait()
}

func (s *engineSuite) TestQueryWorkflow_DecisionTaskDispatch_Unblocked() {
	execution := commonproto.WorkflowExecution{
		WorkflowId: "TestQueryWorkflow_DecisionTaskDispatch_Unblocked",
		RunId:      testRunID,
	}
	tasklist := "testTaskList"
	identity := "testIdentity"
	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache, loggerimpl.NewDevelopmentForTest(s.Suite), execution.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, execution, "wType", tasklist, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	startedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tasklist, identity)
	addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, startedEvent.EventId, nil, identity)
	di = addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tasklist, identity)

	ms := createMutableState(msBuilder)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gweResponse, nil).Once()
	s.mockMatchingClient.EXPECT().QueryWorkflow(gomock.Any(), gomock.Any()).Return(&matchingservice.QueryWorkflowResponse{QueryResult: []byte{1, 2, 3}}, nil)
	s.mockHistoryEngine.matchingClient = s.mockMatchingClient
	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(1)
	asyncQueryUpdate := func(delay time.Duration, answer []byte) {
		defer waitGroup.Done()
		<-time.After(delay)
		builder := s.getBuilder(testDomainID, execution)
		s.NotNil(builder)
		qr := builder.GetQueryRegistry()
		buffered := qr.getBufferedIDs()
		for _, id := range buffered {
			s.NoError(qr.setTerminationState(id, &queryTerminationState{queryTerminationType: queryTerminationTypeUnblocked}))
			state, err := qr.getTerminationState(id)
			s.NoError(err)
			s.Equal(queryTerminationTypeUnblocked, state.queryTerminationType)
		}
	}

	request := &historyservice.QueryWorkflowRequest{
		DomainUUID: testDomainID,
		Request: &workflowservice.QueryWorkflowRequest{
			Execution:             &execution,
			Query:                 &commonproto.WorkflowQuery{},
			QueryConsistencyLevel: enums.QueryConsistencyLevelStrong,
		},
	}
	go asyncQueryUpdate(time.Second*2, []byte{1, 2, 3})
	start := time.Now()
	resp, err := s.mockHistoryEngine.QueryWorkflow(context.Background(), request)
	s.True(time.Now().After(start.Add(time.Second)))
	s.NoError(err)
	s.Equal([]byte{1, 2, 3}, resp.GetResponse().GetQueryResult())
	builder := s.getBuilder(testDomainID, execution)
	s.NotNil(builder)
	qr := builder.GetQueryRegistry()
	s.False(qr.hasBufferedQuery())
	s.False(qr.hasCompletedQuery())
	s.False(qr.hasUnblockedQuery())
	waitGroup.Wait()
}

func (s *engineSuite) TestRespondDecisionTaskCompletedInvalidToken() {

	invalidToken, _ := json.Marshal("bad token")
	identity := "testIdentity"

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &historyservice.RespondDecisionTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondDecisionTaskCompletedRequest{
			TaskToken:        invalidToken,
			Decisions:        nil,
			ExecutionContext: nil,
			Identity:         identity,
		},
	})

	s.NotNil(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedIfNoExecution() {

	tt := &token.Task{
		WorkflowId: "wId",
		RunId:      primitives.MustParseUUID(testRunID),
		ScheduleId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, serviceerror.NewNotFound("")).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &historyservice.RespondDecisionTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondDecisionTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedIfGetExecutionFailed() {

	tt := &token.Task{
		WorkflowId: "wId",
		RunId:      primitives.MustParseUUID(testRunID),
		ScheduleId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, errors.New("FAILED")).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &historyservice.RespondDecisionTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondDecisionTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondDecisionTaskCompletedUpdateExecutionFailed() {

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"

	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, errors.New("FAILED")).Once()
	s.mockShardManager.On("UpdateShard", mock.Anything).Return(nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &historyservice.RespondDecisionTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondDecisionTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.NotNil(err)
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondDecisionTaskCompletedIfTaskCompleted() {

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	startedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, startedEvent.EventId, nil, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &historyservice.RespondDecisionTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondDecisionTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedIfTaskNotStarted() {

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	addDecisionTaskScheduledEvent(msBuilder)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &historyservice.RespondDecisionTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondDecisionTaskCompletedRequest{
			TaskToken: taskToken,
		},
	})
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedConflictOnUpdate() {

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
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
		decisionStartedEvent1.EventId, nil, identity)
	activity1ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent1.EventId,
		activity1ID, activity1Type, tl, activity1Input, 100, 10, 5)
	activity2ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent1.EventId,
		activity2ID, activity2Type, tl, activity2Input, 100, 10, 5)
	activity1StartedEvent := addActivityTaskStartedEvent(msBuilder, activity1ScheduledEvent.EventId, identity)
	activity2StartedEvent := addActivityTaskStartedEvent(msBuilder, activity2ScheduledEvent.EventId, identity)
	addActivityTaskCompletedEvent(msBuilder, activity1ScheduledEvent.EventId,
		activity1StartedEvent.EventId, activity1Result, identity)
	di2 := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent2 := addDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)

	tt := &token.Task{
		WorkflowId: "wId",
		RunId:      primitives.MustParseUUID(we.GetRunId()),
		ScheduleId: di2.ScheduleID,
	}
	taskToken, _ := tt.Marshal()

	decisions := []*commonproto.Decision{{
		DecisionType: enums.DecisionTypeScheduleActivityTask,
		Attributes: &commonproto.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &commonproto.ScheduleActivityTaskDecisionAttributes{
			ActivityId:                    activity3ID,
			ActivityType:                  &commonproto.ActivityType{Name: activity3Type},
			TaskList:                      &commonproto.TaskList{Name: tl},
			Input:                         activity3Input,
			ScheduleToCloseTimeoutSeconds: 100,
			ScheduleToStartTimeoutSeconds: 10,
			StartToCloseTimeoutSeconds:    50,
			HeartbeatTimeoutSeconds:       5,
		}},
	}}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	addActivityTaskCompletedEvent(msBuilder, activity2ScheduledEvent.EventId,
		activity2StartedEvent.EventId, activity2Result, identity)

	ms2 := createMutableState(msBuilder)
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: ms2}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}},
		&persistence.ConditionFailedError{}).Once()

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse2, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &historyservice.RespondDecisionTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	s.Equal(int64(16), ms2.ExecutionInfo.NextEventID)
	s.Equal(decisionStartedEvent2.EventId, ms2.ExecutionInfo.LastProcessedEvent)
	s.Equal(executionContext, ms2.ExecutionInfo.ExecutionContext)

	executionBuilder := s.getBuilder(testDomainID, we)
	activity3Attributes := s.getActivityScheduledEvent(executionBuilder, 13).GetActivityTaskScheduledEventAttributes()
	s.Equal(activity3ID, activity3Attributes.ActivityId)
	s.Equal(activity3Type, activity3Attributes.ActivityType.Name)
	s.Equal(int64(12), activity3Attributes.DecisionTaskCompletedEventId)
	s.Equal(tl, activity3Attributes.TaskList.Name)
	s.Equal(activity3Input, activity3Attributes.Input)
	s.Equal(int32(100), activity3Attributes.ScheduleToCloseTimeoutSeconds)
	s.Equal(int32(10), activity3Attributes.ScheduleToStartTimeoutSeconds)
	s.Equal(int32(50), activity3Attributes.StartToCloseTimeoutSeconds)
	s.Equal(int32(5), activity3Attributes.HeartbeatTimeoutSeconds)

	di, ok := executionBuilder.GetDecisionInfo(15)
	s.True(ok)
	s.Equal(int32(100), di.DecisionTimeout)
}

func (s *engineSuite) TestValidateSignalRequest() {
	workflowType := "testType"
	input := []byte("input")
	startRequest := &workflowservice.StartWorkflowExecutionRequest{
		WorkflowId:                          "ID",
		WorkflowType:                        &commonproto.WorkflowType{Name: workflowType},
		TaskList:                            &commonproto.TaskList{Name: "taskptr"},
		Input:                               input,
		ExecutionStartToCloseTimeoutSeconds: 10,
		TaskStartToCloseTimeoutSeconds:      10,
		Identity:                            "identity",
	}
	err := validateStartWorkflowExecutionRequest(startRequest, 999)
	s.Error(err, "startRequest doesn't have request id, it should error out")
}

func (s *engineSuite) TestRespondDecisionTaskCompletedMaxAttemptsExceeded() {

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	executionContext := []byte("context")
	input := []byte("input")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	decisions := []*commonproto.Decision{{
		DecisionType: enums.DecisionTypeScheduleActivityTask,
		Attributes: &commonproto.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &commonproto.ScheduleActivityTaskDecisionAttributes{
			ActivityId:                    "activity1",
			ActivityType:                  &commonproto.ActivityType{Name: "activity_type1"},
			TaskList:                      &commonproto.TaskList{Name: tl},
			Input:                         input,
			ScheduleToCloseTimeoutSeconds: 100,
			ScheduleToStartTimeoutSeconds: 10,
			StartToCloseTimeoutSeconds:    50,
			HeartbeatTimeoutSeconds:       5,
		}},
	}}

	for i := 0; i < conditionalRetryCount; i++ {
		ms := createMutableState(msBuilder)
		gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

		s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
		s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
		s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}},
			&persistence.ConditionFailedError{}).Once()
	}

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &historyservice.RespondDecisionTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         identity,
		},
	})
	s.NotNil(err)
	s.Equal(ErrMaxAttemptsExceeded, err)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedCompleteWorkflowFailed() {

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
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
		decisionStartedEvent1.EventId, nil, identity)
	activity1ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent1.EventId,
		activity1ID, activity1Type, tl, activity1Input, 100, 10, 5)
	activity2ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent1.EventId,
		activity2ID, activity2Type, tl, activity2Input, 100, 10, 5)
	activity1StartedEvent := addActivityTaskStartedEvent(msBuilder, activity1ScheduledEvent.EventId, identity)
	activity2StartedEvent := addActivityTaskStartedEvent(msBuilder, activity2ScheduledEvent.EventId, identity)
	addActivityTaskCompletedEvent(msBuilder, activity1ScheduledEvent.EventId,
		activity1StartedEvent.EventId, activity1Result, identity)
	di2 := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)
	addActivityTaskCompletedEvent(msBuilder, activity2ScheduledEvent.EventId,
		activity2StartedEvent.EventId, activity2Result, identity)

	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: di2.ScheduleID,
	}
	taskToken, _ := tt.Marshal()

	decisions := []*commonproto.Decision{{
		DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
		Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
			Result: workflowResult,
		}},
	}}

	for i := 0; i < 2; i++ {
		ms := createMutableState(msBuilder)
		gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
		s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	}

	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &historyservice.RespondDecisionTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(testDomainID, we)
	s.Equal(int64(15), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(decisionStartedEvent1.EventId, executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Empty(executionBuilder.GetExecutionInfo().ExecutionContext)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.True(executionBuilder.HasPendingDecision())
	di3, ok := executionBuilder.GetDecisionInfo(executionBuilder.GetExecutionInfo().NextEventID - 1)
	s.True(ok)
	s.Equal(executionBuilder.GetExecutionInfo().NextEventID-1, di3.ScheduleID)
	s.Equal(int64(0), di3.Attempt)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedFailWorkflowFailed() {

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
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
		decisionStartedEvent1.EventId, nil, identity)
	activity1ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent1.EventId, activity1ID,
		activity1Type, tl, activity1Input, 100, 10, 5)
	activity2ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent1.EventId, activity2ID,
		activity2Type, tl, activity2Input, 100, 10, 5)
	activity1StartedEvent := addActivityTaskStartedEvent(msBuilder, activity1ScheduledEvent.EventId, identity)
	activity2StartedEvent := addActivityTaskStartedEvent(msBuilder, activity2ScheduledEvent.EventId, identity)
	addActivityTaskCompletedEvent(msBuilder, activity1ScheduledEvent.EventId,
		activity1StartedEvent.EventId, activity1Result, identity)
	di2 := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)
	addActivityTaskCompletedEvent(msBuilder, activity2ScheduledEvent.EventId,
		activity2StartedEvent.EventId, activity2Result, identity)

	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: di2.ScheduleID,
	}
	taskToken, _ := tt.Marshal()

	decisions := []*commonproto.Decision{{
		DecisionType: enums.DecisionTypeFailWorkflowExecution,
		Attributes: &commonproto.Decision_FailWorkflowExecutionDecisionAttributes{FailWorkflowExecutionDecisionAttributes: &commonproto.FailWorkflowExecutionDecisionAttributes{
			Reason:  reason,
			Details: details,
		}},
	}}

	for i := 0; i < 2; i++ {
		ms := createMutableState(msBuilder)
		gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
		s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	}

	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &historyservice.RespondDecisionTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(testDomainID, we)
	s.Equal(int64(15), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(decisionStartedEvent1.EventId, executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Empty(executionBuilder.GetExecutionInfo().ExecutionContext)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.True(executionBuilder.HasPendingDecision())
	di3, ok := executionBuilder.GetDecisionInfo(executionBuilder.GetExecutionInfo().NextEventID - 1)
	s.True(ok)
	s.Equal(executionBuilder.GetExecutionInfo().NextEventID-1, di3.ScheduleID)
	s.Equal(int64(0), di3.Attempt)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedBadDecisionAttributes() {

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
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
		decisionStartedEvent1.EventId, nil, identity)
	activity1ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent1.EventId, activity1ID,
		activity1Type, tl, activity1Input, 100, 10, 5)
	activity1StartedEvent := addActivityTaskStartedEvent(msBuilder, activity1ScheduledEvent.EventId, identity)
	addActivityTaskCompletedEvent(msBuilder, activity1ScheduledEvent.EventId,
		activity1StartedEvent.EventId, activity1Result, identity)
	di2 := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)

	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: di2.ScheduleID,
	}
	taskToken, _ := tt.Marshal()

	// Decision with nil attributes
	decisions := []*commonproto.Decision{{
		DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
	}}

	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: createMutableState(msBuilder)}
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: createMutableState(msBuilder)}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse1, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse2, nil).Once()

	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{
		MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil,
	).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &historyservice.RespondDecisionTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         identity,
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
		scheduleToClose         int32
		scheduleToStart         int32
		startToClose            int32
		heartbeat               int32
		expectedScheduleToClose int32
		expectedScheduleToStart int32
		expectedStartToClose    int32
		expectDecisionFail      bool
	}{
		// No ScheduleToClose timeout, will use ScheduleToStart + StartToClose
		{0, 3, 7, 0,
			3 + 7, 3, 7, false},
		// Has ScheduleToClose timeout but not ScheduleToStart or StartToClose,
		// will use ScheduleToClose for ScheduleToStart and StartToClose
		{7, 0, 0, 0,
			7, 7, 7, false},
		// No ScheduleToClose timeout, ScheduleToStart or StartToClose, expect error return
		{0, 0, 0, 0,
			0, 0, 0, true},
		// Negative ScheduleToClose, expect error return
		{-1, 0, 0, 0,
			0, 0, 0, true},
		// Negative ScheduleToStart, expect error return
		{0, -1, 0, 0,
			0, 0, 0, true},
		// Negative StartToClose, expect error return
		{0, 0, -1, 0,
			0, 0, 0, true},
		// Negative HeartBeat, expect error return
		{0, 0, 0, -1,
			0, 0, 0, true},
		// Use workflow timeout
		{workflowTimeout, 0, 0, 0,
			workflowTimeout, workflowTimeout, workflowTimeout, false},
		// Timeout larger than workflow timeout
		{workflowTimeout + 1, 0, 0, 0,
			workflowTimeout, workflowTimeout, workflowTimeout, false},
		{0, workflowTimeout + 1, 0, 0,
			0, 0, 0, true},
		{0, 0, workflowTimeout + 1, 0,
			0, 0, 0, true},
		{0, 0, 0, workflowTimeout + 1,
			0, 0, 0, true},
		// No ScheduleToClose timeout, will use ScheduleToStart + StartToClose, but exceed limit
		{0, workflowTimeout, 10, 0,
			workflowTimeout, workflowTimeout, 10, false},
	}

	for _, iVar := range testIterationVariables {
		we := commonproto.WorkflowExecution{
			WorkflowId: "wId",
			RunId:      testRunID,
		}
		tl := "testTaskList"
		tt := &token.Task{
			WorkflowId: "wId",
			RunId:      primitives.MustParseUUID(we.GetRunId()),
			ScheduleId: 2,
		}
		taskToken, _ := tt.Marshal()
		identity := "testIdentity"
		executionContext := []byte("context")
		input := []byte("input")

		msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
			loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
		addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), workflowTimeout, 200, identity)
		di := addDecisionTaskScheduledEvent(msBuilder)
		addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

		decisions := []*commonproto.Decision{{
			DecisionType: enums.DecisionTypeScheduleActivityTask,
			Attributes: &commonproto.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &commonproto.ScheduleActivityTaskDecisionAttributes{
				ActivityId:                    "activity1",
				ActivityType:                  &commonproto.ActivityType{Name: "activity_type1"},
				TaskList:                      &commonproto.TaskList{Name: tl},
				Input:                         input,
				ScheduleToCloseTimeoutSeconds: iVar.scheduleToClose,
				ScheduleToStartTimeoutSeconds: iVar.scheduleToStart,
				StartToCloseTimeoutSeconds:    iVar.startToClose,
				HeartbeatTimeoutSeconds:       iVar.heartbeat,
			}},
		}}

		gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: createMutableState(msBuilder)}
		s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse1, nil).Once()
		if iVar.expectDecisionFail {
			gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: createMutableState(msBuilder)}
			s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse2, nil).Once()
		}

		s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
		s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

		_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &historyservice.RespondDecisionTaskCompletedRequest{
			DomainUUID: testDomainID,
			CompleteRequest: &workflowservice.RespondDecisionTaskCompletedRequest{
				TaskToken:        taskToken,
				Decisions:        decisions,
				ExecutionContext: executionContext,
				Identity:         identity,
			},
		})

		s.Nil(err, s.printHistory(msBuilder))
		executionBuilder := s.getBuilder(testDomainID, we)
		if !iVar.expectDecisionFail {
			s.Equal(int64(6), executionBuilder.GetExecutionInfo().NextEventID)
			s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
			s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
			s.False(executionBuilder.HasPendingDecision())

			activity1Attributes := s.getActivityScheduledEvent(executionBuilder, int64(5)).GetActivityTaskScheduledEventAttributes()
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
	domainID := uuid.New()
	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: "wId",
		RunId:      primitives.MustParseUUID(we.GetRunId()),
		ScheduleId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	executionContext := []byte("context")
	domainEntry := cache.NewLocalDomainCacheEntryForTest(
		&persistence.DomainInfo{ID: domainID, Name: testDomainName},
		&persistence.DomainConfig{
			Retention: 2,
			BadBinaries: commonproto.BadBinaries{
				Binaries: map[string]*commonproto.BadBinaryInfo{
					"test-bad-binary": {},
				},
			},
		},
		cluster.TestCurrentClusterName,
		nil,
	)

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	msBuilder.domainEntry = domainEntry
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	var decisions []*commonproto.Decision

	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: createMutableState(msBuilder)}
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: createMutableState(msBuilder)}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse1, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse2, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()
	s.mockDomainCache.EXPECT().GetDomainByID(domainID).Return(domainEntry, nil).AnyTimes()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &historyservice.RespondDecisionTaskCompletedRequest{
		DomainUUID: domainID,
		CompleteRequest: &workflowservice.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         identity,
			BinaryChecksum:   "test-bad-binary",
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(5), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(common.EmptyEventID, executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Empty(executionBuilder.GetExecutionInfo().ExecutionContext)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.True(executionBuilder.HasPendingDecision())
}

func (s *engineSuite) TestRespondDecisionTaskCompletedSingleActivityScheduledDecision() {

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: "wId",
		RunId:      primitives.MustParseUUID(we.GetRunId()),
		ScheduleId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	executionContext := []byte("context")
	input := []byte("input")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	decisions := []*commonproto.Decision{{
		DecisionType: enums.DecisionTypeScheduleActivityTask,
		Attributes: &commonproto.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &commonproto.ScheduleActivityTaskDecisionAttributes{
			ActivityId:                    "activity1",
			ActivityType:                  &commonproto.ActivityType{Name: "activity_type1"},
			TaskList:                      &commonproto.TaskList{Name: tl},
			Input:                         input,
			ScheduleToCloseTimeoutSeconds: 100,
			ScheduleToStartTimeoutSeconds: 10,
			StartToCloseTimeoutSeconds:    50,
			HeartbeatTimeoutSeconds:       5,
		}},
	}}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &historyservice.RespondDecisionTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(testDomainID, we)
	s.Equal(int64(6), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(executionContext, executionBuilder.GetExecutionInfo().ExecutionContext)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecision())

	activity1Attributes := s.getActivityScheduledEvent(executionBuilder, int64(5)).GetActivityTaskScheduledEventAttributes()
	s.Equal("activity1", activity1Attributes.ActivityId)
	s.Equal("activity_type1", activity1Attributes.ActivityType.Name)
	s.Equal(int64(4), activity1Attributes.DecisionTaskCompletedEventId)
	s.Equal(tl, activity1Attributes.TaskList.Name)
	s.Equal(input, activity1Attributes.Input)
	s.Equal(int32(100), activity1Attributes.ScheduleToCloseTimeoutSeconds)
	s.Equal(int32(10), activity1Attributes.ScheduleToStartTimeoutSeconds)
	s.Equal(int32(50), activity1Attributes.StartToCloseTimeoutSeconds)
	s.Equal(int32(5), activity1Attributes.HeartbeatTimeoutSeconds)
}

func (s *engineSuite) TestRespondDecisionTaskCompleted_DecisionHeartbeatTimeout() {

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	executionContext := []byte("context")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	msBuilder.executionInfo.DecisionOriginalScheduledTimestamp = time.Now().Add(-time.Hour).UnixNano()

	decisions := []*commonproto.Decision{}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &historyservice.RespondDecisionTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondDecisionTaskCompletedRequest{
			ForceCreateNewDecisionTask: true,
			TaskToken:                  taskToken,
			Decisions:                  decisions,
			ExecutionContext:           executionContext,
			Identity:                   identity,
		},
	})
	s.Error(err, "decision heartbeat timeout")
}

func (s *engineSuite) TestRespondDecisionTaskCompleted_DecisionHeartbeatNotTimeout() {

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	executionContext := []byte("context")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	msBuilder.executionInfo.DecisionOriginalScheduledTimestamp = time.Now().Add(-time.Minute).UnixNano()

	decisions := []*commonproto.Decision{}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &historyservice.RespondDecisionTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondDecisionTaskCompletedRequest{
			ForceCreateNewDecisionTask: true,
			TaskToken:                  taskToken,
			Decisions:                  decisions,
			ExecutionContext:           executionContext,
			Identity:                   identity,
		},
	})
	s.Nil(err)
}

func (s *engineSuite) TestRespondDecisionTaskCompleted_DecisionHeartbeatNotTimeout_ZeroOrignalScheduledTime() {

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	executionContext := []byte("context")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	msBuilder.executionInfo.DecisionOriginalScheduledTimestamp = 0

	decisions := []*commonproto.Decision{}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &historyservice.RespondDecisionTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondDecisionTaskCompletedRequest{
			ForceCreateNewDecisionTask: true,
			TaskToken:                  taskToken,
			Decisions:                  decisions,
			ExecutionContext:           executionContext,
			Identity:                   identity,
		},
	})
	s.Nil(err)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedCompleteWorkflowSuccess() {

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	executionContext := []byte("context")
	workflowResult := []byte("success")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	decisions := []*commonproto.Decision{{
		DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
		Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
			Result: workflowResult,
		}},
	}}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &historyservice.RespondDecisionTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         identity,
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

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	executionContext := []byte("context")
	details := []byte("fail workflow details")
	reason := "fail workflow reason"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	decisions := []*commonproto.Decision{{
		DecisionType: enums.DecisionTypeFailWorkflowExecution,
		Attributes: &commonproto.Decision_FailWorkflowExecutionDecisionAttributes{FailWorkflowExecutionDecisionAttributes: &commonproto.FailWorkflowExecutionDecisionAttributes{
			Reason:  reason,
			Details: details,
		}},
	}}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &historyservice.RespondDecisionTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         identity,
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

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	executionContext := []byte("context")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	decisions := []*commonproto.Decision{{
		DecisionType: enums.DecisionTypeSignalExternalWorkflowExecution,
		Attributes: &commonproto.Decision_SignalExternalWorkflowExecutionDecisionAttributes{SignalExternalWorkflowExecutionDecisionAttributes: &commonproto.SignalExternalWorkflowExecutionDecisionAttributes{
			Domain: testDomainName,
			Execution: &commonproto.WorkflowExecution{
				WorkflowId: we.WorkflowId,
				RunId:      we.RunId,
			},
			SignalName: "signal",
			Input:      []byte("test input"),
		}},
	}}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &historyservice.RespondDecisionTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(testDomainID, we)
	s.Equal(int64(6), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(executionContext, executionBuilder.GetExecutionInfo().ExecutionContext)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedStartChildWorkflowWithAbandonPolicy() {

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	executionContext := []byte("context")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	abandon := enums.ParentClosePolicyAbandon
	decisions := []*commonproto.Decision{{
		DecisionType: enums.DecisionTypeStartChildWorkflowExecution,
		Attributes: &commonproto.Decision_StartChildWorkflowExecutionDecisionAttributes{StartChildWorkflowExecutionDecisionAttributes: &commonproto.StartChildWorkflowExecutionDecisionAttributes{
			Domain:     testDomainName,
			WorkflowId: "child-workflow-id",
			WorkflowType: &commonproto.WorkflowType{
				Name: "child-workflow-type",
			},
			ParentClosePolicy: abandon,
		}},
	}}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &historyservice.RespondDecisionTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         identity,
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
	s.Equal(enums.ParentClosePolicyAbandon, enums.ParentClosePolicy(executionBuilder.GetPendingChildExecutionInfos()[childID].ParentClosePolicy))
}

func (s *engineSuite) TestRespondDecisionTaskCompletedStartChildWorkflowWithTerminatePolicy() {

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	executionContext := []byte("context")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	terminate := enums.ParentClosePolicyTerminate
	decisions := []*commonproto.Decision{{
		DecisionType: enums.DecisionTypeStartChildWorkflowExecution,
		Attributes: &commonproto.Decision_StartChildWorkflowExecutionDecisionAttributes{StartChildWorkflowExecutionDecisionAttributes: &commonproto.StartChildWorkflowExecutionDecisionAttributes{
			Domain:     testDomainName,
			WorkflowId: "child-workflow-id",
			WorkflowType: &commonproto.WorkflowType{
				Name: "child-workflow-type",
			},
			ParentClosePolicy: terminate,
		}},
	}}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &historyservice.RespondDecisionTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         identity,
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
	s.Equal(enums.ParentClosePolicyTerminate, enums.ParentClosePolicy(executionBuilder.GetPendingChildExecutionInfos()[childID].ParentClosePolicy))
}

// RunID Invalid is no longer possible form this scope.
/*func (s *engineSuite) TestRespondDecisionTaskCompletedSignalExternalWorkflowFailed() {

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      "invalid run id",
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.GetWorkflowId(),
		RunId:      primitives.MustParseUUID(we.GetRunId()),
		ScheduleId: 2,
	}
                                  taskToken, _  := tt.Marshal()
	identity := "testIdentity"
	executionContext := []byte("context")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), testRunID)
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	decisions := []*commonproto.Decision{{
		DecisionType: enums.DecisionTypeSignalExternalWorkflowExecution,
		Attributes: &commonproto.Decision_SignalExternalWorkflowExecutionDecisionAttributes{SignalExternalWorkflowExecutionDecisionAttributes: &commonproto.SignalExternalWorkflowExecutionDecisionAttributes{
			Domain: testDomainID,
			Execution: &commonproto.WorkflowExecution{
				WorkflowId: we.WorkflowId,
				RunId:      we.RunId,
			},
			SignalName: "signal",
			Input:      []byte("test input"),
		}},
	}}

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &historyservice.RespondDecisionTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondDecisionTaskCompletedRequest{
			Task:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         identity,
		},
	})

	s.EqualError(err, "RunID is not valid UUID.")
}*/

func (s *engineSuite) TestRespondDecisionTaskCompletedSignalExternalWorkflowFailed_UnKnownDomain() {

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	executionContext := []byte("context")
	foreignDomain := "unknown domain"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	decisions := []*commonproto.Decision{{
		DecisionType: enums.DecisionTypeSignalExternalWorkflowExecution,
		Attributes: &commonproto.Decision_SignalExternalWorkflowExecutionDecisionAttributes{SignalExternalWorkflowExecutionDecisionAttributes: &commonproto.SignalExternalWorkflowExecutionDecisionAttributes{
			Domain: foreignDomain,
			Execution: &commonproto.WorkflowExecution{
				WorkflowId: we.WorkflowId,
				RunId:      we.RunId,
			},
			SignalName: "signal",
			Input:      []byte("test input"),
		}},
	}}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockDomainCache.EXPECT().GetDomain(foreignDomain).Return(
		nil, errors.New("get foreign domain error"),
	).Times(1)

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &historyservice.RespondDecisionTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         identity,
		},
	})

	s.NotNil(err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedInvalidToken() {

	invalidToken, _ := json.Marshal("bad token")
	identity := "testIdentity"

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: invalidToken,
			Result:    nil,
			Identity:  identity,
		},
	})

	s.NotNil(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedIfNoExecution() {

	tt := &token.Task{
		WorkflowId: "wId",
		RunId:      primitives.MustParseUUID(testRunID),
		ScheduleId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, serviceerror.NewNotFound("")).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedIfNoRunID() {

	tt := &token.Task{
		WorkflowId: "wId",
		ScheduleId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(nil, serviceerror.NewNotFound("")).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedIfGetExecutionFailed() {

	tt := &token.Task{
		WorkflowId: "wId",
		RunId:      primitives.MustParseUUID(testRunID),
		ScheduleId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, errors.New("FAILED")).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondActivityTaskCompletedIfNoAIdProvided() {

	execution := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tasklist := "testTaskList"
	identity := "testIdentity"
	tt := &token.Task{
		WorkflowId: "wId",
		ScheduleId: common.EmptyEventID,
	}
	taskToken, _ := tt.Marshal()

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), testRunID)
	addWorkflowExecutionStartedEvent(msBuilder, execution, "wType", tasklist, []byte("input"), 100, 200, identity)
	addDecisionTaskScheduledEvent(msBuilder)
	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: testRunID}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.EqualError(err, "Neither ActivityID nor ScheduleID is provided")
}

func (s *engineSuite) TestRespondActivityTaskCompletedIfNotFound() {

	tt := &token.Task{
		WorkflowId: "wId",
		ScheduleId: common.EmptyEventID,
		ActivityId: "aid",
	}
	taskToken, _ := tt.Marshal()
	execution := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
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

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.Error(err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedUpdateExecutionFailed() {

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 5,
	}
	taskToken, _ := tt.Marshal()
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
		decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 5)
	addActivityTaskStartedEvent(msBuilder, activityScheduledEvent.EventId, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, errors.New("FAILED")).Once()
	s.mockShardManager.On("UpdateShard", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result:    activityResult,
			Identity:  identity,
		},
	})
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondActivityTaskCompletedIfTaskCompleted() {

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 5,
	}
	taskToken, _ := tt.Marshal()
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
		decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 5)
	activityStartedEvent := addActivityTaskStartedEvent(msBuilder, activityScheduledEvent.EventId, identity)
	addActivityTaskCompletedEvent(msBuilder, activityScheduledEvent.EventId, activityStartedEvent.EventId,
		activityResult, identity)
	addDecisionTaskScheduledEvent(msBuilder)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result:    activityResult,
			Identity:  identity,
		},
	})
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedIfTaskNotStarted() {

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 5,
	}
	taskToken, _ := tt.Marshal()
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
		decisionStartedEvent.EventId, nil, identity)
	addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 5)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result:    activityResult,
			Identity:  identity,
		},
	})
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedConflictOnUpdate() {

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 5,
	}
	taskToken, _ := tt.Marshal()
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
		decisionStartedEvent1.EventId, nil, identity)
	activity1ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent1.EventId, activity1ID,
		activity1Type, tl, activity1Input, 100, 10, 5)
	activity2ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent1.EventId, activity2ID,
		activity2Type, tl, activity2Input, 100, 10, 5)
	addActivityTaskStartedEvent(msBuilder, activity1ScheduledEvent.EventId, identity)
	addActivityTaskStartedEvent(msBuilder, activity2ScheduledEvent.EventId, identity)

	ms1 := createMutableState(msBuilder)
	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: ms1}

	ms2 := createMutableState(msBuilder)
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: ms2}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse1, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, &persistence.ConditionFailedError{}).Once()

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse2, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result:    activity1Result,
			Identity:  identity,
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

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 5,
	}
	taskToken, _ := tt.Marshal()
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
		decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 5)
	addActivityTaskStartedEvent(msBuilder, activityScheduledEvent.EventId, identity)

	for i := 0; i < conditionalRetryCount; i++ {
		ms := createMutableState(msBuilder)
		gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

		s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
		s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
		s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, &persistence.ConditionFailedError{}).Once()
	}

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result:    activityResult,
			Identity:  identity,
		},
	})
	s.Equal(ErrMaxAttemptsExceeded, err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedSuccess() {

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 5,
	}
	taskToken, _ := tt.Marshal()
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
		decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 5)
	addActivityTaskStartedEvent(msBuilder, activityScheduledEvent.EventId, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result:    activityResult,
			Identity:  identity,
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

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"

	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	activityResult := []byte("activity result")
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		ScheduleId: common.EmptyEventID,
		ActivityId: activityID,
	}
	taskToken, _ := tt.Marshal()

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	decisionScheduledEvent := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, decisionScheduledEvent.ScheduleID,
		decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 5)
	addActivityTaskStartedEvent(msBuilder, activityScheduledEvent.EventId, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: we.RunId}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result:    activityResult,
			Identity:  identity,
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

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &historyservice.RespondActivityTaskFailedRequest{
		DomainUUID: testDomainID,
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: invalidToken,
			Identity:  identity,
		},
	})

	s.NotNil(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *engineSuite) TestRespondActivityTaskFailedIfNoExecution() {

	tt := &token.Task{
		WorkflowId: "wId",
		RunId:      primitives.MustParseUUID(testRunID),
		ScheduleId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil,
		serviceerror.NewNotFound("")).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &historyservice.RespondActivityTaskFailedRequest{
		DomainUUID: testDomainID,
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *engineSuite) TestRespondActivityTaskFailedIfNoRunID() {

	tt := &token.Task{
		WorkflowId: "wId",
		ScheduleId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(nil,
		serviceerror.NewNotFound("")).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &historyservice.RespondActivityTaskFailedRequest{
		DomainUUID: testDomainID,
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *engineSuite) TestRespondActivityTaskFailedIfGetExecutionFailed() {

	tt := &token.Task{
		WorkflowId: "wId",
		RunId:      primitives.MustParseUUID(testRunID),
		ScheduleId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil,
		errors.New("FAILED")).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &historyservice.RespondActivityTaskFailedRequest{
		DomainUUID: testDomainID,
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondActivityTaskFailededIfNoAIdProvided() {

	tt := &token.Task{
		WorkflowId: "wId",
		ScheduleId: common.EmptyEventID,
	}
	taskToken, _ := tt.Marshal()
	execution := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
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

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &historyservice.RespondActivityTaskFailedRequest{
		DomainUUID: testDomainID,
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.EqualError(err, "Neither ActivityID nor ScheduleID is provided")
}

func (s *engineSuite) TestRespondActivityTaskFailededIfNotFound() {

	tt := &token.Task{
		WorkflowId: "wId",
		ScheduleId: common.EmptyEventID,
		ActivityId: "aid",
	}
	taskToken, _ := tt.Marshal()
	execution := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
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

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &historyservice.RespondActivityTaskFailedRequest{
		DomainUUID: testDomainID,
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.Error(err)
}

func (s *engineSuite) TestRespondActivityTaskFailedUpdateExecutionFailed() {

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 5,
	}
	taskToken, _ := tt.Marshal()
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
		decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 5)
	addActivityTaskStartedEvent(msBuilder, activityScheduledEvent.EventId, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, errors.New("FAILED")).Once()
	s.mockShardManager.On("UpdateShard", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &historyservice.RespondActivityTaskFailedRequest{
		DomainUUID: testDomainID,
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondActivityTaskFailedIfTaskCompleted() {

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 5,
	}
	taskToken, _ := tt.Marshal()
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
		decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 5)
	activityStartedEvent := addActivityTaskStartedEvent(msBuilder, activityScheduledEvent.EventId, identity)
	addActivityTaskFailedEvent(msBuilder, activityScheduledEvent.EventId, activityStartedEvent.EventId,
		failReason, details, identity)
	addDecisionTaskScheduledEvent(msBuilder)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &historyservice.RespondActivityTaskFailedRequest{
		DomainUUID: testDomainID,
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Reason:    failReason,
			Details:   details,
			Identity:  identity,
		},
	})
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *engineSuite) TestRespondActivityTaskFailedIfTaskNotStarted() {

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 5,
	}
	taskToken, _ := tt.Marshal()
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
		decisionStartedEvent.EventId, nil, identity)
	addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 5)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &historyservice.RespondActivityTaskFailedRequest{
		DomainUUID: testDomainID,
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *engineSuite) TestRespondActivityTaskFailedConflictOnUpdate() {

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 5,
	}
	taskToken, _ := tt.Marshal()
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
		decisionStartedEvent1.EventId, nil, identity)
	activity1ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent1.EventId, activity1ID,
		activity1Type, tl, activity1Input, 100, 10, 5)
	activity2ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent1.EventId, activity2ID,
		activity2Type, tl, activity2Input, 100, 10, 5)
	addActivityTaskStartedEvent(msBuilder, activity1ScheduledEvent.EventId, identity)
	activity2StartedEvent := addActivityTaskStartedEvent(msBuilder, activity2ScheduledEvent.EventId, identity)

	ms1 := createMutableState(msBuilder)
	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: ms1}

	addActivityTaskCompletedEvent(msBuilder, activity2ScheduledEvent.EventId,
		activity2StartedEvent.EventId, activity2Result, identity)
	addDecisionTaskScheduledEvent(msBuilder)

	ms2 := createMutableState(msBuilder)
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: ms2}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse1, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, &persistence.ConditionFailedError{}).Once()

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse2, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &historyservice.RespondActivityTaskFailedRequest{
		DomainUUID: testDomainID,
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Reason:    failReason,
			Details:   details,
			Identity:  identity,
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

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 5,
	}
	taskToken, _ := tt.Marshal()
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
		decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 5)
	addActivityTaskStartedEvent(msBuilder, activityScheduledEvent.EventId, identity)

	for i := 0; i < conditionalRetryCount; i++ {
		ms := createMutableState(msBuilder)
		gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

		s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
		s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
		s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, &persistence.ConditionFailedError{}).Once()
	}

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &historyservice.RespondActivityTaskFailedRequest{
		DomainUUID: testDomainID,
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.Equal(ErrMaxAttemptsExceeded, err)
}

func (s *engineSuite) TestRespondActivityTaskFailedSuccess() {

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 5,
	}
	taskToken, _ := tt.Marshal()
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
		decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 5)
	addActivityTaskStartedEvent(msBuilder, activityScheduledEvent.EventId, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &historyservice.RespondActivityTaskFailedRequest{
		DomainUUID: testDomainID,
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Reason:    failReason,
			Details:   failDetails,
			Identity:  identity,
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

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"

	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	failReason := "failed"
	failDetails := []byte("fail details.")
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		ScheduleId: common.EmptyEventID,
		ActivityId: activityID,
	}
	taskToken, _ := tt.Marshal()

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	decisionScheduledEvent := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, decisionScheduledEvent.ScheduleID,
		decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 5)
	addActivityTaskStartedEvent(msBuilder, activityScheduledEvent.EventId, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: we.RunId}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &historyservice.RespondActivityTaskFailedRequest{
		DomainUUID: testDomainID,
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Reason:    failReason,
			Details:   failDetails,
			Identity:  identity,
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

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 5,
	}
	taskToken, _ := tt.Marshal()
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
		decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 0)
	addActivityTaskStartedEvent(msBuilder, activityScheduledEvent.EventId, identity)

	// No HeartBeat timer running.
	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	detais := []byte("details")

	_, err := s.mockHistoryEngine.RecordActivityTaskHeartbeat(context.Background(), &historyservice.RecordActivityTaskHeartbeatRequest{
		DomainUUID: testDomainID,
		HeartbeatRequest: &workflowservice.RecordActivityTaskHeartbeatRequest{
			TaskToken: taskToken,
			Identity:  identity,
			Details:   detais,
		},
	})
	s.Nil(err)
}

func (s *engineSuite) TestRecordActivityTaskHeartBeatSuccess_TimerRunning() {

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 5,
	}
	taskToken, _ := tt.Marshal()
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
		decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1)
	addActivityTaskStartedEvent(msBuilder, activityScheduledEvent.EventId, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	// HeartBeat timer running.
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	detais := []byte("details")

	_, err := s.mockHistoryEngine.RecordActivityTaskHeartbeat(context.Background(), &historyservice.RecordActivityTaskHeartbeatRequest{
		DomainUUID: testDomainID,
		HeartbeatRequest: &workflowservice.RecordActivityTaskHeartbeatRequest{
			TaskToken: taskToken,
			Identity:  identity,
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

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: common.EmptyEventID,
		ActivityId: activityID,
	}
	taskToken, _ := tt.Marshal()

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 0)
	addActivityTaskStartedEvent(msBuilder, activityScheduledEvent.EventId, identity)

	// No HeartBeat timer running.
	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	detais := []byte("details")

	_, err := s.mockHistoryEngine.RecordActivityTaskHeartbeat(context.Background(), &historyservice.RecordActivityTaskHeartbeatRequest{
		DomainUUID: testDomainID,
		HeartbeatRequest: &workflowservice.RecordActivityTaskHeartbeatRequest{
			TaskToken: taskToken,
			Identity:  identity,
			Details:   detais,
		},
	})
	s.Nil(err)
}

func (s *engineSuite) TestRespondActivityTaskCanceled_Scheduled() {

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 5,
	}
	taskToken, _ := tt.Marshal()
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
		decisionStartedEvent.EventId, nil, identity)
	addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCanceled(context.Background(), &historyservice.RespondActivityTaskCanceledRequest{
		DomainUUID: testDomainID,
		CancelRequest: &workflowservice.RespondActivityTaskCanceledRequest{
			TaskToken: taskToken,
			Identity:  identity,
			Details:   []byte("details"),
		},
	})
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCanceled_Started() {

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 5,
	}
	taskToken, _ := tt.Marshal()
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
		decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1)
	addActivityTaskStartedEvent(msBuilder, activityScheduledEvent.EventId, identity)
	_, _, err := msBuilder.AddActivityTaskCancelRequestedEvent(decisionCompletedEvent.EventId, activityID, identity)
	s.Nil(err)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	err = s.mockHistoryEngine.RespondActivityTaskCanceled(context.Background(), &historyservice.RespondActivityTaskCanceledRequest{
		DomainUUID: testDomainID,
		CancelRequest: &workflowservice.RespondActivityTaskCanceledRequest{
			TaskToken: taskToken,
			Identity:  identity,
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

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		ScheduleId: common.EmptyEventID,
		ActivityId: activityID,
	}
	taskToken, _ := tt.Marshal()

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	decisionScheduledEvent := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, decisionScheduledEvent.ScheduleID,
		decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1)
	addActivityTaskStartedEvent(msBuilder, activityScheduledEvent.EventId, identity)
	_, _, err := msBuilder.AddActivityTaskCancelRequestedEvent(decisionCompletedEvent.EventId, activityID, identity)
	s.Nil(err)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: we.RunId}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	err = s.mockHistoryEngine.RespondActivityTaskCanceled(context.Background(), &historyservice.RespondActivityTaskCanceledRequest{
		DomainUUID: testDomainID,
		CancelRequest: &workflowservice.RespondActivityTaskCanceledRequest{
			TaskToken: taskToken,
			Identity:  identity,
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

	tt := &token.Task{
		WorkflowId: "wId",
		ScheduleId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(nil, serviceerror.NewNotFound("")).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCanceled(context.Background(), &historyservice.RespondActivityTaskCanceledRequest{
		DomainUUID: testDomainID,
		CancelRequest: &workflowservice.RespondActivityTaskCanceledRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCanceledIfNoAIdProvided() {

	tt := &token.Task{
		WorkflowId: "wId",
		ScheduleId: common.EmptyEventID,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), testRunID)
	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: testRunID}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCanceled(context.Background(), &historyservice.RespondActivityTaskCanceledRequest{
		DomainUUID: testDomainID,
		CancelRequest: &workflowservice.RespondActivityTaskCanceledRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.EqualError(err, "Neither ActivityID nor ScheduleID is provided")
}

func (s *engineSuite) TestRespondActivityTaskCanceledIfNotFound() {

	tt := &token.Task{
		WorkflowId: "wId",
		ScheduleId: common.EmptyEventID,
		ActivityId: "aid",
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), testRunID)
	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: testRunID}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCanceled(context.Background(), &historyservice.RespondActivityTaskCanceledRequest{
		DomainUUID: testDomainID,
		CancelRequest: &workflowservice.RespondActivityTaskCanceledRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	s.Error(err)
}

func (s *engineSuite) TestRequestCancel_RespondDecisionTaskCompleted_NotScheduled() {

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	decisions := []*commonproto.Decision{{
		DecisionType: enums.DecisionTypeRequestCancelActivityTask,
		Attributes: &commonproto.Decision_RequestCancelActivityTaskDecisionAttributes{RequestCancelActivityTaskDecisionAttributes: &commonproto.RequestCancelActivityTaskDecisionAttributes{
			ActivityId: activityID,
		}},
	}}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &historyservice.RespondDecisionTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         identity,
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

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 6,
	}
	taskToken, _ := tt.Marshal()
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
		decisionStartedEvent.EventId, nil, identity)
	addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1)
	di2 := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	decisions := []*commonproto.Decision{{
		DecisionType: enums.DecisionTypeRequestCancelActivityTask,
		Attributes: &commonproto.Decision_RequestCancelActivityTaskDecisionAttributes{RequestCancelActivityTaskDecisionAttributes: &commonproto.RequestCancelActivityTaskDecisionAttributes{
			ActivityId: activityID,
		}},
	}}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &historyservice.RespondDecisionTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         identity,
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

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 7,
	}
	taskToken, _ := tt.Marshal()
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
		decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 0)
	addActivityTaskStartedEvent(msBuilder, activityScheduledEvent.EventId, identity)
	di2 := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	decisions := []*commonproto.Decision{{
		DecisionType: enums.DecisionTypeRequestCancelActivityTask,
		Attributes: &commonproto.Decision_RequestCancelActivityTaskDecisionAttributes{RequestCancelActivityTaskDecisionAttributes: &commonproto.RequestCancelActivityTaskDecisionAttributes{
			ActivityId: activityID,
		}},
	}}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &historyservice.RespondDecisionTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         identity,
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

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 6,
	}
	taskToken, _ := tt.Marshal()
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
		decisionStartedEvent.EventId, nil, identity)
	addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 0)
	di2 := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)

	decisions := []*commonproto.Decision{
		{
			DecisionType: enums.DecisionTypeRequestCancelActivityTask,
			Attributes: &commonproto.Decision_RequestCancelActivityTaskDecisionAttributes{RequestCancelActivityTaskDecisionAttributes: &commonproto.RequestCancelActivityTaskDecisionAttributes{
				ActivityId: activityID,
			}},
		},
		{
			DecisionType: enums.DecisionTypeCompleteWorkflowExecution,
			Attributes: &commonproto.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &commonproto.CompleteWorkflowExecutionDecisionAttributes{
				Result: workflowResult,
			}},
		},
	}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &historyservice.RespondDecisionTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         identity,
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

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 7,
	}
	taskToken, _ := tt.Marshal()
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
		decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 0)
	addActivityTaskStartedEvent(msBuilder, activityScheduledEvent.EventId, identity)
	di2 := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	decisions := []*commonproto.Decision{{
		DecisionType: enums.DecisionTypeRequestCancelActivityTask,
		Attributes: &commonproto.Decision_RequestCancelActivityTaskDecisionAttributes{RequestCancelActivityTaskDecisionAttributes: &commonproto.RequestCancelActivityTaskDecisionAttributes{
			ActivityId: activityID,
		}},
	}}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &historyservice.RespondDecisionTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         identity,
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(testDomainID, we)
	s.Equal(int64(11), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(8), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecision())

	// Try recording activity heartbeat
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	att := &token.Task{
		WorkflowId: "wId",
		RunId:      primitives.MustParseUUID(we.GetRunId()),
		ScheduleId: 5,
	}
	activityTaskToken, _ := att.Marshal()

	hbResponse, err := s.mockHistoryEngine.RecordActivityTaskHeartbeat(context.Background(), &historyservice.RecordActivityTaskHeartbeatRequest{
		DomainUUID: testDomainID,
		HeartbeatRequest: &workflowservice.RecordActivityTaskHeartbeatRequest{
			TaskToken: activityTaskToken,
			Identity:  identity,
			Details:   []byte("details"),
		},
	})
	s.Nil(err)
	s.NotNil(hbResponse)
	s.True(hbResponse.CancelRequested)

	// Try cancelling the request.
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	err = s.mockHistoryEngine.RespondActivityTaskCanceled(context.Background(), &historyservice.RespondActivityTaskCanceledRequest{
		DomainUUID: testDomainID,
		CancelRequest: &workflowservice.RespondActivityTaskCanceledRequest{
			TaskToken: activityTaskToken,
			Identity:  identity,
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

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 7,
	}
	taskToken, _ := tt.Marshal()
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
		decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1)
	addActivityTaskStartedEvent(msBuilder, activityScheduledEvent.EventId, identity)
	di2 := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	decisions := []*commonproto.Decision{{
		DecisionType: enums.DecisionTypeRequestCancelActivityTask,
		Attributes: &commonproto.Decision_RequestCancelActivityTaskDecisionAttributes{RequestCancelActivityTaskDecisionAttributes: &commonproto.RequestCancelActivityTaskDecisionAttributes{
			ActivityId: activityID,
		}},
	}}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &historyservice.RespondDecisionTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         identity,
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(testDomainID, we)
	s.Equal(int64(11), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(8), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecision())

	// Try recording activity heartbeat
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	att := &token.Task{
		WorkflowId: "wId",
		RunId:      primitives.MustParseUUID(we.GetRunId()),
		ScheduleId: 5,
	}
	activityTaskToken, _ := att.Marshal()

	hbResponse, err := s.mockHistoryEngine.RecordActivityTaskHeartbeat(context.Background(), &historyservice.RecordActivityTaskHeartbeatRequest{
		DomainUUID: testDomainID,
		HeartbeatRequest: &workflowservice.RecordActivityTaskHeartbeatRequest{
			TaskToken: activityTaskToken,
			Identity:  identity,
			Details:   []byte("details"),
		},
	})
	s.Nil(err)
	s.NotNil(hbResponse)
	s.True(hbResponse.CancelRequested)

	// Try cancelling the request.
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	err = s.mockHistoryEngine.RespondActivityTaskCanceled(context.Background(), &historyservice.RespondActivityTaskCanceledRequest{
		DomainUUID: testDomainID,
		CancelRequest: &workflowservice.RespondActivityTaskCanceledRequest{
			TaskToken: activityTaskToken,
			Identity:  identity,
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

func (s *engineSuite) TestRequestCancel_RespondDecisionTaskCompleted_SuccessWithQueries() {
	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 7,
	}
	taskToken, _ := tt.Marshal()
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
		decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1)
	addActivityTaskStartedEvent(msBuilder, activityScheduledEvent.EventId, identity)
	di2 := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	decisions := []*commonproto.Decision{{
		DecisionType: enums.DecisionTypeRequestCancelActivityTask,
		Attributes: &commonproto.Decision_RequestCancelActivityTaskDecisionAttributes{RequestCancelActivityTaskDecisionAttributes: &commonproto.RequestCancelActivityTaskDecisionAttributes{
			ActivityId: activityID,
		}},
	}}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	// load mutable state such that it already exists in memory when respond decision task is called
	// this enables us to set query registry on it
	ctx, release, err := s.mockHistoryEngine.historyCache.getOrCreateWorkflowExecutionForBackground(testDomainID, we)
	s.NoError(err)
	loadedMS, err := ctx.loadWorkflowExecution()
	s.NoError(err)
	qr := newQueryRegistry()
	id1, _ := qr.bufferQuery(&commonproto.WorkflowQuery{})
	id2, _ := qr.bufferQuery(&commonproto.WorkflowQuery{})
	id3, _ := qr.bufferQuery(&commonproto.WorkflowQuery{})
	loadedMS.(*mutableStateBuilder).queryRegistry = qr
	release(nil)
	result1 := &commonproto.WorkflowQueryResult{
		ResultType: enums.QueryResultTypeAnswered,
		Answer:     []byte{1, 2, 3},
	}
	result2 := &commonproto.WorkflowQueryResult{
		ResultType:   enums.QueryResultTypeFailed,
		ErrorMessage: "error reason",
	}
	queryResults := map[string]*commonproto.WorkflowQueryResult{
		id1: result1,
		id2: result2,
	}
	_, err = s.mockHistoryEngine.RespondDecisionTaskCompleted(s.constructCallContext(headers.GoWorkerConsistentQueryVersion), &historyservice.RespondDecisionTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         identity,
			QueryResults:     queryResults,
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(testDomainID, we)
	s.Equal(int64(11), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(8), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecision())
	s.Len(qr.getCompletedIDs(), 2)
	completed1, err := qr.getTerminationState(id1)
	s.NoError(err)
	s.EqualValues(completed1.queryResult, result1)
	s.Equal(queryTerminationTypeCompleted, completed1.queryTerminationType)
	completed2, err := qr.getTerminationState(id2)
	s.NoError(err)
	s.EqualValues(completed2.queryResult, result2)
	s.Equal(queryTerminationTypeCompleted, completed2.queryTerminationType)
	s.Len(qr.getBufferedIDs(), 0)
	s.Len(qr.getFailedIDs(), 0)
	s.Len(qr.getUnblockedIDs(), 1)
	unblocked1, err := qr.getTerminationState(id3)
	s.NoError(err)
	s.Nil(unblocked1.queryResult)
	s.Equal(queryTerminationTypeUnblocked, unblocked1.queryTerminationType)

	// Try recording activity heartbeat
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	att := &token.Task{
		WorkflowId: "wId",
		RunId:      primitives.MustParseUUID(we.GetRunId()),
		ScheduleId: 5,
	}
	activityTaskToken, _ := att.Marshal()

	hbResponse, err := s.mockHistoryEngine.RecordActivityTaskHeartbeat(context.Background(), &historyservice.RecordActivityTaskHeartbeatRequest{
		DomainUUID: testDomainID,
		HeartbeatRequest: &workflowservice.RecordActivityTaskHeartbeatRequest{
			TaskToken: activityTaskToken,
			Identity:  identity,
			Details:   []byte("details"),
		},
	})
	s.Nil(err)
	s.NotNil(hbResponse)
	s.True(hbResponse.CancelRequested)

	// Try cancelling the request.
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	err = s.mockHistoryEngine.RespondActivityTaskCanceled(context.Background(), &historyservice.RespondActivityTaskCanceledRequest{
		DomainUUID: testDomainID,
		CancelRequest: &workflowservice.RespondActivityTaskCanceledRequest{
			TaskToken: activityTaskToken,
			Identity:  identity,
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

func (s *engineSuite) TestRequestCancel_RespondDecisionTaskCompleted_SuccessWithConsistentQueriesUnsupported() {
	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 7,
	}
	taskToken, _ := tt.Marshal()
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
		decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1)
	addActivityTaskStartedEvent(msBuilder, activityScheduledEvent.EventId, identity)
	di2 := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	decisions := []*commonproto.Decision{{
		DecisionType: enums.DecisionTypeRequestCancelActivityTask,
		Attributes: &commonproto.Decision_RequestCancelActivityTaskDecisionAttributes{RequestCancelActivityTaskDecisionAttributes: &commonproto.RequestCancelActivityTaskDecisionAttributes{
			ActivityId: activityID,
		}},
	}}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	// load mutable state such that it already exists in memory when respond decision task is called
	// this enables us to set query registry on it
	ctx, release, err := s.mockHistoryEngine.historyCache.getOrCreateWorkflowExecutionForBackground(testDomainID, we)
	s.NoError(err)
	loadedMS, err := ctx.loadWorkflowExecution()
	s.NoError(err)
	qr := newQueryRegistry()
	qr.bufferQuery(&commonproto.WorkflowQuery{})
	qr.bufferQuery(&commonproto.WorkflowQuery{})
	qr.bufferQuery(&commonproto.WorkflowQuery{})
	loadedMS.(*mutableStateBuilder).queryRegistry = qr
	release(nil)
	_, err = s.mockHistoryEngine.RespondDecisionTaskCompleted(s.constructCallContext("0.0.0"), &historyservice.RespondDecisionTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         identity,
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(testDomainID, we)
	s.Equal(int64(11), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(8), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecision())
	s.Len(qr.getBufferedIDs(), 0)
	s.Len(qr.getCompletedIDs(), 0)
	s.Len(qr.getUnblockedIDs(), 0)
	s.Len(qr.getFailedIDs(), 3)
}

func (s *engineSuite) constructCallContext(featureVersion string) context.Context {
	return headers.SetVersionsForTests(context.Background(), headers.SupportedGoSDKVersion, headers.GoSDK, featureVersion)
}

func (s *engineSuite) TestStarTimer_DuplicateTimerID() {

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	timerID := "t1"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())

	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	decisions := []*commonproto.Decision{{
		DecisionType: enums.DecisionTypeStartTimer,
		Attributes: &commonproto.Decision_StartTimerDecisionAttributes{StartTimerDecisionAttributes: &commonproto.StartTimerDecisionAttributes{
			TimerId:                   timerID,
			StartToFireTimeoutSeconds: 1,
		}},
	}}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &historyservice.RespondDecisionTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         identity,
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(testDomainID, we)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)

	// Try to add the same timer ID again.
	di2 := addDecisionTaskScheduledEvent(executionBuilder)
	addDecisionTaskStartedEvent(executionBuilder, di2.ScheduleID, tl, identity)
	tt2 := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: di2.ScheduleID,
	}
	taskToken2, _ := tt2.Marshal()

	ms2 := createMutableState(executionBuilder)
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: ms2}

	decisionFailedEvent := false
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse2, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Run(func(arguments mock.Arguments) {
		req := arguments.Get(0).(*persistence.AppendHistoryNodesRequest)
		decTaskIndex := len(req.Events) - 1
		if decTaskIndex >= 0 && req.Events[decTaskIndex].EventType == enums.EventTypeDecisionTaskFailed {
			decisionFailedEvent = true
		}
	}).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err = s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &historyservice.RespondDecisionTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken2,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         identity,
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

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 6,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	timerID := "t1"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	// Verify cancel timer with a start event.
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		decisionStartedEvent.EventId, nil, identity)
	addTimerStartedEvent(msBuilder, decisionCompletedEvent.EventId, timerID, 10)
	di2 := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	decisions := []*commonproto.Decision{{
		DecisionType: enums.DecisionTypeCancelTimer,
		Attributes: &commonproto.Decision_CancelTimerDecisionAttributes{CancelTimerDecisionAttributes: &commonproto.CancelTimerDecisionAttributes{
			TimerId: timerID,
		}},
	}}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &historyservice.RespondDecisionTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         identity,
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

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 2,
	}
	taskToken, _ := tt.Marshal()
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

	decisions := []*commonproto.Decision{{
		DecisionType: enums.DecisionTypeCancelTimer,
		Attributes: &commonproto.Decision_CancelTimerDecisionAttributes{CancelTimerDecisionAttributes: &commonproto.CancelTimerDecisionAttributes{
			TimerId: timerID,
		}},
	}}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &historyservice.RespondDecisionTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         identity,
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

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	tt := &token.Task{
		WorkflowId: we.WorkflowId,
		RunId:      primitives.MustParseUUID(we.RunId),
		ScheduleId: 6,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	timerID := "t1"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	// Verify cancel timer with a start event.
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		decisionStartedEvent.EventId, nil, identity)
	addTimerStartedEvent(msBuilder, decisionCompletedEvent.EventId, timerID, 10)
	di2 := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)
	addTimerFiredEvent(msBuilder, timerID)
	_, _, err := msBuilder.CloseTransactionAsMutation(time.Now(), transactionPolicyActive)
	s.Nil(err)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.True(len(gwmsResponse.State.BufferedEvents) > 0)

	decisions := []*commonproto.Decision{{
		DecisionType: enums.DecisionTypeCancelTimer,
		Attributes: &commonproto.Decision_CancelTimerDecisionAttributes{CancelTimerDecisionAttributes: &commonproto.CancelTimerDecisionAttributes{
			TimerId: timerID,
		}},
	}}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.MatchedBy(func(input *persistence.UpdateWorkflowExecutionRequest) bool {
		// need to check whether the buffered events are cleared
		s.True(input.UpdateWorkflowMutation.ClearBufferedEvents)
		return true
	})).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err = s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &historyservice.RespondDecisionTaskCompletedRequest{
		DomainUUID: testDomainID,
		CompleteRequest: &workflowservice.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         identity,
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
	signalRequest := &historyservice.SignalWorkflowExecutionRequest{}
	err := s.mockHistoryEngine.SignalWorkflowExecution(context.Background(), signalRequest)
	s.EqualError(err, "Missing domain UUID.")

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tasklist := "testTaskList"
	identity := "testIdentity"
	signalName := "my signal name"
	input := []byte("test input")
	signalRequest = &historyservice.SignalWorkflowExecutionRequest{
		DomainUUID: testDomainID,
		SignalRequest: &workflowservice.SignalWorkflowExecutionRequest{
			Domain:            testDomainID,
			WorkflowExecution: &we,
			Identity:          identity,
			SignalName:        signalName,
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
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	err = s.mockHistoryEngine.SignalWorkflowExecution(context.Background(), signalRequest)
	s.Nil(err)
}

// Test signal decision by adding request ID
func (s *engineSuite) TestSignalWorkflowExecution_DuplicateRequest() {
	signalRequest := &historyservice.SignalWorkflowExecutionRequest{}
	err := s.mockHistoryEngine.SignalWorkflowExecution(context.Background(), signalRequest)
	s.EqualError(err, "Missing domain UUID.")

	we := commonproto.WorkflowExecution{
		WorkflowId: "wId2",
		RunId:      testRunID,
	}
	tasklist := "testTaskList"
	identity := "testIdentity"
	signalName := "my signal name 2"
	input := []byte("test input 2")
	requestID := uuid.New()
	signalRequest = &historyservice.SignalWorkflowExecutionRequest{
		DomainUUID: testDomainID,
		SignalRequest: &workflowservice.SignalWorkflowExecutionRequest{
			Domain:            testDomainID,
			WorkflowExecution: &we,
			Identity:          identity,
			SignalName:        signalName,
			Input:             input,
			RequestId:         requestID,
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
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	err = s.mockHistoryEngine.SignalWorkflowExecution(context.Background(), signalRequest)
	s.Nil(err)
}

func (s *engineSuite) TestSignalWorkflowExecution_Failed() {
	signalRequest := &historyservice.SignalWorkflowExecutionRequest{}
	err := s.mockHistoryEngine.SignalWorkflowExecution(context.Background(), signalRequest)
	s.EqualError(err, "Missing domain UUID.")

	we := &commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tasklist := "testTaskList"
	identity := "testIdentity"
	signalName := "my signal name"
	input := []byte("test input")
	signalRequest = &historyservice.SignalWorkflowExecutionRequest{
		DomainUUID: testDomainID,
		SignalRequest: &workflowservice.SignalWorkflowExecutionRequest{
			Domain:            testDomainID,
			WorkflowExecution: we,
			Identity:          identity,
			SignalName:        signalName,
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
	s.EqualError(err, "workflow execution already completed")
}

func (s *engineSuite) TestRemoveSignalMutableState() {
	removeRequest := &historyservice.RemoveSignalMutableStateRequest{}
	err := s.mockHistoryEngine.RemoveSignalMutableState(context.Background(), removeRequest)
	s.EqualError(err, "Missing domain UUID.")

	execution := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tasklist := "testTaskList"
	identity := "testIdentity"
	requestID := uuid.New()
	removeRequest = &historyservice.RemoveSignalMutableStateRequest{
		DomainUUID:        testDomainID,
		WorkflowExecution: &execution,
		RequestId:         requestID,
	}

	msBuilder := newMutableStateBuilderWithEventV2(s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), testRunID)
	addWorkflowExecutionStartedEvent(msBuilder, execution, "wType", tasklist, []byte("input"), 100, 200, identity)
	addDecisionTaskScheduledEvent(msBuilder)
	ms := createMutableState(msBuilder)
	ms.ExecutionInfo.DomainID = testDomainID
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	err = s.mockHistoryEngine.RemoveSignalMutableState(context.Background(), removeRequest)
	s.Nil(err)
}

func (s *engineSuite) getBuilder(testDomainID string, we commonproto.WorkflowExecution) mutableState {
	context, release, err := s.mockHistoryEngine.historyCache.getOrCreateWorkflowExecutionForBackground(testDomainID, we)
	if err != nil {
		return nil
	}
	defer release(nil)

	return context.(*workflowExecutionContextImpl).mutableState
}

func (s *engineSuite) getActivityScheduledEvent(msBuilder mutableState,
	scheduleID int64) *commonproto.HistoryEvent {
	event, _ := msBuilder.GetActivityScheduledEvent(scheduleID)
	return event
}

func (s *engineSuite) printHistory(builder mutableState) string {
	return builder.GetHistoryBuilder().GetHistory().String()
}

func addWorkflowExecutionStartedEventWithParent(builder mutableState, workflowExecution commonproto.WorkflowExecution,
	workflowType, taskList string, input []byte, executionStartToCloseTimeout, taskStartToCloseTimeout int32,
	parentInfo *commonproto.ParentExecutionInfo, identity string) *commonproto.HistoryEvent {

	startRequest := &workflowservice.StartWorkflowExecutionRequest{
		WorkflowId:                          workflowExecution.WorkflowId,
		WorkflowType:                        &commonproto.WorkflowType{Name: workflowType},
		TaskList:                            &commonproto.TaskList{Name: taskList},
		Input:                               input,
		ExecutionStartToCloseTimeoutSeconds: executionStartToCloseTimeout,
		TaskStartToCloseTimeoutSeconds:      taskStartToCloseTimeout,
		Identity:                            identity,
	}

	event, _ := builder.AddWorkflowExecutionStartedEvent(
		workflowExecution,
		&historyservice.StartWorkflowExecutionRequest{
			DomainUUID:          testDomainID,
			StartRequest:        startRequest,
			ParentExecutionInfo: parentInfo,
		},
	)

	return event
}

func addWorkflowExecutionStartedEvent(builder mutableState, workflowExecution commonproto.WorkflowExecution,
	workflowType, taskList string, input []byte, executionStartToCloseTimeout, taskStartToCloseTimeout int32,
	identity string) *commonproto.HistoryEvent {
	return addWorkflowExecutionStartedEventWithParent(builder, workflowExecution, workflowType, taskList, input,
		executionStartToCloseTimeout, taskStartToCloseTimeout, nil, identity)
}

func addDecisionTaskScheduledEvent(builder mutableState) *decisionInfo {
	di, _ := builder.AddDecisionTaskScheduledEvent(false)
	return di
}

func addDecisionTaskStartedEvent(builder mutableState, scheduleID int64, taskList,
	identity string) *commonproto.HistoryEvent {
	return addDecisionTaskStartedEventWithRequestID(builder, scheduleID, testRunID, taskList, identity)
}

func addDecisionTaskStartedEventWithRequestID(builder mutableState, scheduleID int64, requestID string,
	taskList, identity string) *commonproto.HistoryEvent {
	event, _, _ := builder.AddDecisionTaskStartedEvent(scheduleID, requestID, &workflowservice.PollForDecisionTaskRequest{
		TaskList: &commonproto.TaskList{Name: taskList},
		Identity: identity,
	})

	return event
}

func addDecisionTaskCompletedEvent(builder mutableState, scheduleID, startedID int64, context []byte,
	identity string) *commonproto.HistoryEvent {
	event, _ := builder.AddDecisionTaskCompletedEvent(scheduleID, startedID, &workflowservice.RespondDecisionTaskCompletedRequest{
		ExecutionContext: context,
		Identity:         identity,
	}, defaultHistoryMaxAutoResetPoints)

	builder.FlushBufferedEvents() // nolint:errcheck

	return event
}

func addActivityTaskScheduledEvent(builder mutableState, decisionCompletedID int64, activityID, activityType,
	taskList string, input []byte, timeout, queueTimeout, heartbeatTimeout int32) (*commonproto.HistoryEvent,
	*persistence.ActivityInfo) {

	event, ai, _ := builder.AddActivityTaskScheduledEvent(decisionCompletedID, &commonproto.ScheduleActivityTaskDecisionAttributes{
		ActivityId:                    activityID,
		ActivityType:                  &commonproto.ActivityType{Name: activityType},
		TaskList:                      &commonproto.TaskList{Name: taskList},
		Input:                         input,
		ScheduleToCloseTimeoutSeconds: timeout,
		ScheduleToStartTimeoutSeconds: queueTimeout,
		HeartbeatTimeoutSeconds:       heartbeatTimeout,
		StartToCloseTimeoutSeconds:    1,
	})

	return event, ai
}

func addActivityTaskScheduledEventWithRetry(
	builder mutableState,
	decisionCompletedID int64,
	activityID, activityType,
	taskList string,
	input []byte,
	scheduleToCloseTimeout int32,
	scheduleToStartTimeout int32,
	startToCloseTimeout int32,
	heartbeatTimeout int32,
	retryPolicy *commonproto.RetryPolicy,
) (*commonproto.HistoryEvent, *persistence.ActivityInfo) {

	event, ai, _ := builder.AddActivityTaskScheduledEvent(decisionCompletedID, &commonproto.ScheduleActivityTaskDecisionAttributes{
		ActivityId:                    activityID,
		ActivityType:                  &commonproto.ActivityType{Name: activityType},
		TaskList:                      &commonproto.TaskList{Name: taskList},
		Input:                         input,
		ScheduleToCloseTimeoutSeconds: scheduleToCloseTimeout,
		ScheduleToStartTimeoutSeconds: scheduleToStartTimeout,
		StartToCloseTimeoutSeconds:    startToCloseTimeout,
		HeartbeatTimeoutSeconds:       heartbeatTimeout,
		RetryPolicy:                   retryPolicy,
	})

	return event, ai
}

func addActivityTaskStartedEvent(builder mutableState, scheduleID int64, identity string) *commonproto.HistoryEvent {
	ai, _ := builder.GetActivityInfo(scheduleID)
	event, _ := builder.AddActivityTaskStartedEvent(ai, scheduleID, testRunID, identity)
	return event
}

func addActivityTaskCompletedEvent(builder mutableState, scheduleID, startedID int64, result []byte,
	identity string) *commonproto.HistoryEvent {
	event, _ := builder.AddActivityTaskCompletedEvent(scheduleID, startedID, &workflowservice.RespondActivityTaskCompletedRequest{
		Result:   result,
		Identity: identity,
	})

	return event
}

func addActivityTaskFailedEvent(builder mutableState, scheduleID, startedID int64, reason string, details []byte,
	identity string) *commonproto.HistoryEvent {
	event, _ := builder.AddActivityTaskFailedEvent(scheduleID, startedID, &workflowservice.RespondActivityTaskFailedRequest{
		Reason:   reason,
		Details:  details,
		Identity: identity,
	})

	return event
}

func addTimerStartedEvent(builder mutableState, decisionCompletedEventID int64, timerID string,
	timeOut int64) (*commonproto.HistoryEvent, *persistenceblobs.TimerInfo) {
	event, ti, _ := builder.AddTimerStartedEvent(decisionCompletedEventID,
		&commonproto.StartTimerDecisionAttributes{
			TimerId:                   timerID,
			StartToFireTimeoutSeconds: timeOut,
		})
	return event, ti
}

func addTimerFiredEvent(mutableState mutableState, timerID string) *commonproto.HistoryEvent {
	event, _ := mutableState.AddTimerFiredEvent(timerID)
	return event
}

func addRequestCancelInitiatedEvent(builder mutableState, decisionCompletedEventID int64,
	cancelRequestID, domain, workflowID, runID string) (*commonproto.HistoryEvent, *persistenceblobs.RequestCancelInfo) {
	event, rci, _ := builder.AddRequestCancelExternalWorkflowExecutionInitiatedEvent(decisionCompletedEventID,
		cancelRequestID, &commonproto.RequestCancelExternalWorkflowExecutionDecisionAttributes{
			Domain:     domain,
			WorkflowId: workflowID,
			RunId:      runID,
		})

	return event, rci
}

func addCancelRequestedEvent(builder mutableState, initiatedID int64, domain, workflowID, runID string) *commonproto.HistoryEvent {
	event, _ := builder.AddExternalWorkflowExecutionCancelRequested(initiatedID, domain, workflowID, runID)
	return event
}

func addRequestSignalInitiatedEvent(builder mutableState, decisionCompletedEventID int64,
	signalRequestID, domain, workflowID, runID, signalName string, input, control []byte) (*commonproto.HistoryEvent, *persistenceblobs.SignalInfo) {
	event, si, _ := builder.AddSignalExternalWorkflowExecutionInitiatedEvent(decisionCompletedEventID, signalRequestID,
		&commonproto.SignalExternalWorkflowExecutionDecisionAttributes{
			Domain: domain,
			Execution: &commonproto.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			SignalName: signalName,
			Input:      input,
			Control:    control,
		})

	return event, si
}

func addSignaledEvent(builder mutableState, initiatedID int64, domain, workflowID, runID string, control []byte) *commonproto.HistoryEvent {
	event, _ := builder.AddExternalWorkflowExecutionSignaled(initiatedID, domain, workflowID, runID, control)
	return event
}

func addStartChildWorkflowExecutionInitiatedEvent(builder mutableState, decisionCompletedID int64,
	createRequestID, domain, workflowID, workflowType, tasklist string, input []byte,
	executionStartToCloseTimeout, taskStartToCloseTimeout int32) (*commonproto.HistoryEvent,
	*persistence.ChildExecutionInfo) {

	event, cei, _ := builder.AddStartChildWorkflowExecutionInitiatedEvent(decisionCompletedID, createRequestID,
		&commonproto.StartChildWorkflowExecutionDecisionAttributes{
			Domain:                              domain,
			WorkflowId:                          workflowID,
			WorkflowType:                        &commonproto.WorkflowType{Name: workflowType},
			TaskList:                            &commonproto.TaskList{Name: tasklist},
			Input:                               input,
			ExecutionStartToCloseTimeoutSeconds: executionStartToCloseTimeout,
			TaskStartToCloseTimeoutSeconds:      taskStartToCloseTimeout,
			Control:                             nil,
		})
	return event, cei
}

func addChildWorkflowExecutionStartedEvent(builder mutableState, initiatedID int64, domain, workflowID, runID string,
	workflowType string) *commonproto.HistoryEvent {
	event, _ := builder.AddChildWorkflowExecutionStartedEvent(
		domain,
		&commonproto.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		&commonproto.WorkflowType{Name: workflowType},
		initiatedID,
		&commonproto.Header{},
	)
	return event
}

func addChildWorkflowExecutionCompletedEvent(builder mutableState, initiatedID int64, childExecution *commonproto.WorkflowExecution,
	attributes *commonproto.WorkflowExecutionCompletedEventAttributes) *commonproto.HistoryEvent {
	event, _ := builder.AddChildWorkflowExecutionCompletedEvent(initiatedID, childExecution, attributes)
	return event
}

func addCompleteWorkflowEvent(builder mutableState, decisionCompletedEventID int64,
	result []byte) *commonproto.HistoryEvent {
	event, _ := builder.AddCompletedWorkflowEvent(decisionCompletedEventID, &commonproto.CompleteWorkflowExecutionDecisionAttributes{
		Result: result,
	})
	return event
}

func addFailWorkflowEvent(
	builder mutableState,
	decisionCompletedEventID int64,
	reason string,
	details []byte,
) *commonproto.HistoryEvent {
	event, _ := builder.AddFailWorkflowEvent(decisionCompletedEventID, &commonproto.FailWorkflowExecutionDecisionAttributes{
		Reason:  reason,
		Details: details,
	})
	return event
}

func newMutableStateBuilderWithEventV2(shard ShardContext, eventsCache eventsCache,
	logger log.Logger, runID string) *mutableStateBuilder {

	msBuilder := newMutableStateBuilder(shard, eventsCache, logger, testLocalDomainEntry)
	_ = msBuilder.SetHistoryTree(primitives.MustParseUUID(runID))

	return msBuilder
}

func newMutableStateBuilderWithReplicationStateWithEventV2(shard ShardContext, eventsCache eventsCache,
	logger log.Logger, version int64, runID string) *mutableStateBuilder {

	msBuilder := newMutableStateBuilderWithReplicationState(shard, eventsCache, logger, testGlobalDomainEntry)
	msBuilder.GetReplicationState().StartVersion = version
	err := msBuilder.UpdateCurrentVersion(version, true)
	if err != nil {
		logger.Error("update current version error", tag.Error(err))
	}
	_ = msBuilder.SetHistoryTree(primitives.MustParseUUID(runID))

	return msBuilder
}

func createMutableState(ms mutableState) *persistence.WorkflowMutableState {
	builder := ms.(*mutableStateBuilder)
	builder.FlushBufferedEvents() // nolint:errcheck
	info := copyWorkflowExecutionInfo(builder.executionInfo)
	stats := &persistence.ExecutionStats{}
	activityInfos := make(map[int64]*persistence.ActivityInfo)
	for id, info := range builder.pendingActivityInfoIDs {
		activityInfos[id] = copyActivityInfo(info)
	}
	timerInfos := make(map[string]*persistenceblobs.TimerInfo)
	for id, info := range builder.pendingTimerInfoIDs {
		timerInfos[id] = copyTimerInfo(info)
	}
	cancellationInfos := make(map[int64]*persistenceblobs.RequestCancelInfo)
	for id, info := range builder.pendingRequestCancelInfoIDs {
		cancellationInfos[id] = copyCancellationInfo(info)
	}
	signalInfos := make(map[int64]*persistenceblobs.SignalInfo)
	for id, info := range builder.pendingSignalInfoIDs {
		signalInfos[id] = copySignalInfo(info)
	}
	childInfos := make(map[int64]*persistence.ChildExecutionInfo)
	for id, info := range builder.pendingChildExecutionInfoIDs {
		childInfos[id] = copyChildInfo(info)
	}

	builder.FlushBufferedEvents() // nolint:errcheck
	var bufferedEvents []*commonproto.HistoryEvent
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
		DecisionStartToCloseTimeout:        sourceInfo.DecisionStartToCloseTimeout,
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

func copyHistoryEvent(source *commonproto.HistoryEvent) *commonproto.HistoryEvent {
	if source == nil {
		return nil
	}

	bytes, err := source.Marshal()
	if err != nil {
		panic(err)
	}

	result := &commonproto.HistoryEvent{}
	err = result.Unmarshal(bytes)
	if err != nil {
		panic(err)
	}
	return result
}

func copyActivityInfo(sourceInfo *persistence.ActivityInfo) *persistence.ActivityInfo {
	details := make([]byte, len(sourceInfo.Details))
	copy(details, sourceInfo.Details)

	return &persistence.ActivityInfo{
		Version:                  sourceInfo.Version,
		ScheduleID:               sourceInfo.ScheduleID,
		ScheduledEventBatchID:    sourceInfo.ScheduledEventBatchID,
		ScheduledEvent:           copyHistoryEvent(sourceInfo.ScheduledEvent),
		StartedID:                sourceInfo.StartedID,
		StartedEvent:             copyHistoryEvent(sourceInfo.StartedEvent),
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
		// // Not written to database - This is used only for deduping heartbeat timer creation
		LastHeartbeatTimeoutVisibility: sourceInfo.LastHeartbeatTimeoutVisibility,
	}
}

func copyTimerInfo(sourceInfo *persistenceblobs.TimerInfo) *persistenceblobs.TimerInfo {
	return &persistenceblobs.TimerInfo{
		Version:    sourceInfo.Version,
		TimerID:    sourceInfo.TimerID,
		StartedID:  sourceInfo.StartedID,
		ExpiryTime: sourceInfo.ExpiryTime,
		TaskStatus: sourceInfo.TaskStatus,
	}
}

func copyCancellationInfo(sourceInfo *persistenceblobs.RequestCancelInfo) *persistenceblobs.RequestCancelInfo {
	return &persistenceblobs.RequestCancelInfo{
		Version:         sourceInfo.Version,
		InitiatedID:     sourceInfo.InitiatedID,
		CancelRequestID: sourceInfo.CancelRequestID,
	}
}

func copySignalInfo(sourceInfo *persistenceblobs.SignalInfo) *persistenceblobs.SignalInfo {
	result := &persistenceblobs.SignalInfo{
		Version:     sourceInfo.Version,
		InitiatedID: sourceInfo.InitiatedID,
		RequestID:   sourceInfo.RequestID,
		Name:        sourceInfo.Name,
	}
	result.Input = make([]byte, len(sourceInfo.Input))
	copy(result.Input, sourceInfo.Input)
	result.Control = make([]byte, len(sourceInfo.Control))
	copy(result.Control, sourceInfo.Control)
	return result
}

func copyChildInfo(sourceInfo *persistence.ChildExecutionInfo) *persistence.ChildExecutionInfo {
	return &persistence.ChildExecutionInfo{
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
		InitiatedEvent:        copyHistoryEvent(sourceInfo.InitiatedEvent),
		StartedEvent:          copyHistoryEvent(sourceInfo.StartedEvent),
	}
}

func copyReplicationState(source *persistence.ReplicationState) *persistence.ReplicationState {
	var lastReplicationInfo map[string]*replication.ReplicationInfo
	if source.LastReplicationInfo != nil {
		lastReplicationInfo = map[string]*replication.ReplicationInfo{}
		for k, v := range source.LastReplicationInfo {
			lastReplicationInfo[k] = &replication.ReplicationInfo{
				Version:     v.Version,
				LastEventId: v.LastEventId,
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
