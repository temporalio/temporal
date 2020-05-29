// Copyright (c) 2017-2020 Uber Technologies Inc.
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
	"go.uber.org/yarpc/api/encoding"
	"go.uber.org/yarpc/api/transport"

	"github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/history/historyservicetest"
	"github.com/uber/cadence/.gen/go/matching/matchingservicetest"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	cc "github.com/uber/cadence/common/client"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"github.com/uber/cadence/service/history/config"
	"github.com/uber/cadence/service/history/constants"
	"github.com/uber/cadence/service/history/events"
	"github.com/uber/cadence/service/history/execution"
	"github.com/uber/cadence/service/history/ndc"
	"github.com/uber/cadence/service/history/query"
	"github.com/uber/cadence/service/history/queue"
	"github.com/uber/cadence/service/history/reset"
	"github.com/uber/cadence/service/history/shard"
	test "github.com/uber/cadence/service/history/testing"
)

type (
	engineSuite struct {
		suite.Suite
		*require.Assertions

		controller               *gomock.Controller
		mockShard                *shard.TestContext
		mockTxProcessor          *queue.MockTransferQueueProcessor
		mockReplicationProcessor *MockReplicatorQueueProcessor
		mockTimerProcessor       *MocktimerQueueProcessor
		mockDomainCache          *cache.MockDomainCache
		mockMatchingClient       *matchingservicetest.MockClient
		mockHistoryClient        *historyservicetest.MockClient
		mockClusterMetadata      *cluster.MockMetadata
		mockEventsReapplier      *ndc.MockEventsReapplier
		mockWorkflowResetter     *reset.MockWorkflowResetter

		mockHistoryEngine *historyEngineImpl
		mockExecutionMgr  *mocks.ExecutionManager
		mockHistoryV2Mgr  *mocks.HistoryV2Manager
		mockShardManager  *mocks.ShardManager

		eventsCache events.Cache
		config      *config.Config
	}
)

func TestEngineSuite(t *testing.T) {
	s := new(engineSuite)
	suite.Run(t, s)
}

func (s *engineSuite) SetupSuite() {
	s.config = config.NewForTest()
}

func (s *engineSuite) TearDownSuite() {
}

func (s *engineSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockTxProcessor = queue.NewMockTransferQueueProcessor(s.controller)
	s.mockReplicationProcessor = NewMockReplicatorQueueProcessor(s.controller)
	s.mockTimerProcessor = NewMocktimerQueueProcessor(s.controller)
	s.mockEventsReapplier = ndc.NewMockEventsReapplier(s.controller)
	s.mockWorkflowResetter = reset.NewMockWorkflowResetter(s.controller)
	s.mockTxProcessor.EXPECT().NotifyNewTask(gomock.Any(), gomock.Any()).AnyTimes()
	s.mockReplicationProcessor.EXPECT().notifyNewTask().AnyTimes()
	s.mockTimerProcessor.EXPECT().NotifyNewTimers(gomock.Any(), gomock.Any()).AnyTimes()

	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistence.ShardInfo{
			RangeID:          1,
			TransferAckLevel: 0,
		},
		s.config,
	)
	s.eventsCache = events.NewCache(
		s.mockShard.GetShardID(),
		s.mockShard.GetHistoryManager(),
		s.config,
		s.mockShard.GetLogger(),
		s.mockShard.GetMetricsClient(),
	)
	s.mockShard.SetEventsCache(s.eventsCache)

	s.mockMatchingClient = s.mockShard.Resource.MatchingClient
	s.mockHistoryClient = s.mockShard.Resource.HistoryClient
	s.mockExecutionMgr = s.mockShard.Resource.ExecutionMgr
	s.mockHistoryV2Mgr = s.mockShard.Resource.HistoryMgr
	s.mockShardManager = s.mockShard.Resource.ShardMgr
	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata
	s.mockDomainCache = s.mockShard.Resource.DomainCache
	s.mockClusterMetadata.EXPECT().IsGlobalDomainEnabled().Return(false).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestSingleDCClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(common.EmptyVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomainByID(constants.TestDomainID).Return(constants.TestLocalDomainEntry, nil).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomain(constants.TestDomainName).Return(constants.TestLocalDomainEntry, nil).AnyTimes()

	historyEventNotifier := events.NewNotifier(
		clock.NewRealTimeSource(),
		s.mockShard.Resource.MetricsClient,
		func(workflowID string) int {
			return len(workflowID)
		},
	)

	h := &historyEngineImpl{
		currentClusterName:   s.mockShard.GetClusterMetadata().GetCurrentClusterName(),
		shard:                s.mockShard,
		clusterMetadata:      s.mockClusterMetadata,
		executionManager:     s.mockExecutionMgr,
		historyV2Mgr:         s.mockHistoryV2Mgr,
		executionCache:       execution.NewCache(s.mockShard),
		logger:               s.mockShard.GetLogger(),
		metricsClient:        s.mockShard.GetMetricsClient(),
		tokenSerializer:      common.NewJSONTaskTokenSerializer(),
		historyEventNotifier: historyEventNotifier,
		config:               config.NewForTest(),
		txProcessor:          s.mockTxProcessor,
		replicatorProcessor:  s.mockReplicationProcessor,
		timerProcessor:       s.mockTimerProcessor,
		clientChecker:        cc.NewVersionChecker(),
		eventsReapplier:      s.mockEventsReapplier,
		workflowResetter:     s.mockWorkflowResetter,
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

	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("test-get-workflow-execution-event-id"),
		RunId:      common.StringPtr(constants.TestRunID),
	}
	tasklist := "testTaskList"
	identity := "testIdentity"

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		workflowExecution.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, workflowExecution, "wType", tasklist, []byte("input"), 100, 200, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tasklist, identity)
	ms := execution.CreatePersistenceMutableState(msBuilder)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	// right now the next event ID is 4
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gweResponse, nil).Once()

	// test get the next event ID instantly
	response, err := s.mockHistoryEngine.GetMutableState(ctx, &history.GetMutableStateRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
		Execution:  &workflowExecution,
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
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
		DomainUUID: common.StringPtr(constants.TestDomainID),
		Execution:  &execution,
	})
	s.Equal(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestGetMutableStateLongPoll() {
	ctx := context.Background()

	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("test-get-workflow-execution-event-id"),
		RunId:      common.StringPtr(constants.TestRunID),
	}
	tasklist := "testTaskList"
	identity := "testIdentity"

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		workflowExecution.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, workflowExecution, "wType", tasklist, []byte("input"), 100, 200, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tasklist, identity)
	ms := execution.CreatePersistenceMutableState(msBuilder)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	// right now the next event ID is 4
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gweResponse, nil).Once()

	// test long poll on next event ID change
	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(1)
	asycWorkflowUpdate := func(delay time.Duration) {
		taskToken, _ := json.Marshal(&common.TaskToken{
			WorkflowID: *workflowExecution.WorkflowId,
			RunID:      *workflowExecution.RunId,
			ScheduleID: 2,
		})
		s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
		s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

		timer := time.NewTimer(delay)

		<-timer.C
		_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
			DomainUUID: common.StringPtr(constants.TestDomainID),
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
		DomainUUID:          common.StringPtr(constants.TestDomainID),
		Execution:           &workflowExecution,
		ExpectedNextEventId: common.Int64Ptr(3),
	})
	s.Nil(err)
	s.Equal(int64(4), *response.NextEventId)

	// long poll, new event happen before long poll timeout
	go asycWorkflowUpdate(time.Second * 2)
	start := time.Now()
	pollResponse, err := s.mockHistoryEngine.PollMutableState(ctx, &history.PollMutableStateRequest{
		DomainUUID:          common.StringPtr(constants.TestDomainID),
		Execution:           &workflowExecution,
		ExpectedNextEventId: common.Int64Ptr(4),
	})
	s.True(time.Now().After(start.Add(time.Second * 1)))
	s.Nil(err)
	s.Equal(int64(5), pollResponse.GetNextEventId())
	waitGroup.Wait()
}

func (s *engineSuite) TestGetMutableStateLongPoll_CurrentBranchChanged() {
	ctx := context.Background()

	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("test-get-workflow-execution-event-id"),
		RunId:      common.StringPtr(constants.TestRunID),
	}
	tasklist := "testTaskList"
	identity := "testIdentity"

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		workflowExecution.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, workflowExecution, "wType", tasklist, []byte("input"), 100, 200, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tasklist, identity)
	ms := execution.CreatePersistenceMutableState(msBuilder)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	// right now the next event ID is 4
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gweResponse, nil).Once()

	// test long poll on next event ID change
	asyncBranchTokenUpdate := func(delay time.Duration) {
		timer := time.NewTimer(delay)
		<-timer.C
		newExecution := &workflow.WorkflowExecution{
			WorkflowId: workflowExecution.WorkflowId,
			RunId:      workflowExecution.RunId,
		}
		s.mockHistoryEngine.historyEventNotifier.NotifyNewHistoryEvent(events.NewNotification(
			"constants.TestDomainID",
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
		DomainUUID:          common.StringPtr(constants.TestDomainID),
		Execution:           &workflowExecution,
		ExpectedNextEventId: common.Int64Ptr(3),
	})
	s.Nil(err)
	s.Equal(int64(4), response0.GetNextEventId())

	// long poll, new event happen before long poll timeout
	go asyncBranchTokenUpdate(time.Second * 2)
	start := time.Now()
	response1, err := s.mockHistoryEngine.GetMutableState(ctx, &history.GetMutableStateRequest{
		DomainUUID:          common.StringPtr(constants.TestDomainID),
		Execution:           &workflowExecution,
		ExpectedNextEventId: common.Int64Ptr(10),
	})
	s.True(time.Now().After(start.Add(time.Second * 1)))
	s.Nil(err)
	s.Equal(response0.GetCurrentBranchToken(), response1.GetCurrentBranchToken())
}

func (s *engineSuite) TestGetMutableStateLongPollTimeout() {
	ctx := context.Background()

	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("test-get-workflow-execution-event-id"),
		RunId:      common.StringPtr(constants.TestRunID),
	}
	tasklist := "testTaskList"
	identity := "testIdentity"

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		workflowExecution.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, workflowExecution, "wType", tasklist, []byte("input"), 100, 200, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tasklist, identity)
	ms := execution.CreatePersistenceMutableState(msBuilder)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	// right now the next event ID is 4
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gweResponse, nil).Once()

	// long poll, no event happen after long poll timeout
	response, err := s.mockHistoryEngine.GetMutableState(ctx, &history.GetMutableStateRequest{
		DomainUUID:          common.StringPtr(constants.TestDomainID),
		Execution:           &workflowExecution,
		ExpectedNextEventId: common.Int64Ptr(4),
	})
	s.Nil(err)
	s.Equal(int64(4), response.GetNextEventId())
}

func (s *engineSuite) TestQueryWorkflow_RejectBasedOnNotEnabled() {
	s.mockHistoryEngine.config.EnableConsistentQueryByDomain = dynamicconfig.GetBoolPropertyFnFilteredByDomain(false)
	request := &history.QueryWorkflowRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
		Request: &workflow.QueryWorkflowRequest{
			QueryConsistencyLevel: common.QueryConsistencyLevelPtr(workflow.QueryConsistencyLevelStrong),
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
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("TestQueryWorkflow_RejectBasedOnCompleted"),
		RunId:      common.StringPtr(constants.TestRunID),
	}
	tasklist := "testTaskList"
	identity := "testIdentity"

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		workflowExecution.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, workflowExecution, "wType", tasklist, []byte("input"), 100, 200, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	event := test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tasklist, identity)
	di.StartedID = event.GetEventId()
	event = test.AddDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, di.StartedID, nil, "some random identity")
	test.AddCompleteWorkflowEvent(msBuilder, event.GetEventId(), nil)
	ms := execution.CreatePersistenceMutableState(msBuilder)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gweResponse, nil).Once()

	request := &history.QueryWorkflowRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
		Request: &workflow.QueryWorkflowRequest{
			Execution:            &workflowExecution,
			Query:                &workflow.WorkflowQuery{},
			QueryRejectCondition: common.QueryRejectConditionPtr(workflow.QueryRejectConditionNotOpen),
		},
	}
	resp, err := s.mockHistoryEngine.QueryWorkflow(context.Background(), request)
	s.NoError(err)
	s.Nil(resp.GetResponse().QueryResult)
	s.NotNil(resp.GetResponse().QueryRejected)
	s.True(resp.GetResponse().GetQueryRejected().CloseStatus.Equals(workflow.WorkflowExecutionCloseStatusCompleted))
}

func (s *engineSuite) TestQueryWorkflow_RejectBasedOnFailed() {
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("TestQueryWorkflow_RejectBasedOnFailed"),
		RunId:      common.StringPtr(constants.TestRunID),
	}
	tasklist := "testTaskList"
	identity := "testIdentity"

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		workflowExecution.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, workflowExecution, "wType", tasklist, []byte("input"), 100, 200, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	event := test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tasklist, identity)
	di.StartedID = event.GetEventId()
	event = test.AddDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, di.StartedID, nil, "some random identity")
	test.AddFailWorkflowEvent(msBuilder, event.GetEventId(), "failure reason", []byte{1, 2, 3})
	ms := execution.CreatePersistenceMutableState(msBuilder)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gweResponse, nil).Once()

	request := &history.QueryWorkflowRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
		Request: &workflow.QueryWorkflowRequest{
			Execution:            &workflowExecution,
			Query:                &workflow.WorkflowQuery{},
			QueryRejectCondition: common.QueryRejectConditionPtr(workflow.QueryRejectConditionNotOpen),
		},
	}
	resp, err := s.mockHistoryEngine.QueryWorkflow(context.Background(), request)
	s.NoError(err)
	s.Nil(resp.GetResponse().QueryResult)
	s.NotNil(resp.GetResponse().QueryRejected)
	s.True(resp.GetResponse().GetQueryRejected().CloseStatus.Equals(workflow.WorkflowExecutionCloseStatusFailed))

	request = &history.QueryWorkflowRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
		Request: &workflow.QueryWorkflowRequest{
			Execution:            &workflowExecution,
			Query:                &workflow.WorkflowQuery{},
			QueryRejectCondition: common.QueryRejectConditionPtr(workflow.QueryRejectConditionNotCompletedCleanly),
		},
	}
	resp, err = s.mockHistoryEngine.QueryWorkflow(context.Background(), request)
	s.NoError(err)
	s.Nil(resp.GetResponse().QueryResult)
	s.NotNil(resp.GetResponse().QueryRejected)
	s.True(resp.GetResponse().GetQueryRejected().CloseStatus.Equals(workflow.WorkflowExecutionCloseStatusFailed))
}

func (s *engineSuite) TestQueryWorkflow_FirstDecisionNotCompleted() {
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("TestQueryWorkflow_FirstDecisionNotCompleted"),
		RunId:      common.StringPtr(constants.TestRunID),
	}
	tasklist := "testTaskList"
	identity := "testIdentity"

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		workflowExecution.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, workflowExecution, "wType", tasklist, []byte("input"), 100, 200, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tasklist, identity)
	ms := execution.CreatePersistenceMutableState(msBuilder)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gweResponse, nil).Once()

	request := &history.QueryWorkflowRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
		Request: &workflow.QueryWorkflowRequest{
			Execution: &workflowExecution,
			Query:     &workflow.WorkflowQuery{},
		},
	}
	resp, err := s.mockHistoryEngine.QueryWorkflow(context.Background(), request)
	s.Equal(ErrQueryWorkflowBeforeFirstDecision, err)
	s.Nil(resp)
}

func (s *engineSuite) TestQueryWorkflow_DirectlyThroughMatching() {
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("TestQueryWorkflow_DirectlyThroughMatching"),
		RunId:      common.StringPtr(constants.TestRunID),
	}
	tasklist := "testTaskList"
	identity := "testIdentity"

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		workflowExecution.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, workflowExecution, "wType", tasklist, []byte("input"), 100, 200, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	startedEvent := test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tasklist, identity)
	test.AddDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, *startedEvent.EventId, nil, identity)
	di = test.AddDecisionTaskScheduledEvent(msBuilder)
	test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tasklist, identity)

	ms := execution.CreatePersistenceMutableState(msBuilder)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gweResponse, nil).Once()
	s.mockMatchingClient.EXPECT().QueryWorkflow(gomock.Any(), gomock.Any()).Return(&workflow.QueryWorkflowResponse{QueryResult: []byte{1, 2, 3}}, nil)
	s.mockHistoryEngine.matchingClient = s.mockMatchingClient
	request := &history.QueryWorkflowRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
		Request: &workflow.QueryWorkflowRequest{
			Execution: &workflowExecution,
			Query:     &workflow.WorkflowQuery{},
			// since workflow is open this filter does not reject query
			QueryRejectCondition:  common.QueryRejectConditionPtr(workflow.QueryRejectConditionNotOpen),
			QueryConsistencyLevel: common.QueryConsistencyLevelPtr(workflow.QueryConsistencyLevelEventual),
		},
	}
	resp, err := s.mockHistoryEngine.QueryWorkflow(context.Background(), request)
	s.NoError(err)
	s.NotNil(resp.GetResponse().QueryResult)
	s.Nil(resp.GetResponse().QueryRejected)
	s.Equal([]byte{1, 2, 3}, resp.GetResponse().GetQueryResult())
}

func (s *engineSuite) TestQueryWorkflow_DecisionTaskDispatch_Timeout() {
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("TestQueryWorkflow_DecisionTaskDispatch_Timeout"),
		RunId:      common.StringPtr(constants.TestRunID),
	}
	tasklist := "testTaskList"
	identity := "testIdentity"
	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		workflowExecution.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, workflowExecution, "wType", tasklist, []byte("input"), 100, 200, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	startedEvent := test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tasklist, identity)
	test.AddDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, *startedEvent.EventId, nil, identity)
	di = test.AddDecisionTaskScheduledEvent(msBuilder)
	test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tasklist, identity)

	ms := execution.CreatePersistenceMutableState(msBuilder)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gweResponse, nil).Once()
	request := &history.QueryWorkflowRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
		Request: &workflow.QueryWorkflowRequest{
			Execution: &workflowExecution,
			Query:     &workflow.WorkflowQuery{},
			// since workflow is open this filter does not reject query
			QueryRejectCondition:  common.QueryRejectConditionPtr(workflow.QueryRejectConditionNotOpen),
			QueryConsistencyLevel: common.QueryConsistencyLevelPtr(workflow.QueryConsistencyLevelStrong),
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
	builder := s.getBuilder(constants.TestDomainID, workflowExecution)
	s.NotNil(builder)
	qr := builder.GetQueryRegistry()
	s.True(qr.HasBufferedQuery())
	s.False(qr.HasCompletedQuery())
	s.False(qr.HasUnblockedQuery())
	s.False(qr.HasFailedQuery())
	wg.Wait()
	s.False(qr.HasBufferedQuery())
	s.False(qr.HasCompletedQuery())
	s.False(qr.HasUnblockedQuery())
	s.False(qr.HasFailedQuery())
}

func (s *engineSuite) TestQueryWorkflow_ConsistentQueryBufferFull() {
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("TestQueryWorkflow_ConsistentQueryBufferFull"),
		RunId:      common.StringPtr(constants.TestRunID),
	}
	tasklist := "testTaskList"
	identity := "testIdentity"
	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		workflowExecution.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, workflowExecution, "wType", tasklist, []byte("input"), 100, 200, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	startedEvent := test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tasklist, identity)
	test.AddDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, *startedEvent.EventId, nil, identity)
	di = test.AddDecisionTaskScheduledEvent(msBuilder)
	test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tasklist, identity)

	ms := execution.CreatePersistenceMutableState(msBuilder)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gweResponse, nil).Once()

	// buffer query so that when history.QueryWorkflow is called buffer is already full
	ctx, release, err := s.mockHistoryEngine.executionCache.GetOrCreateWorkflowExecutionForBackground(constants.TestDomainID, workflowExecution)
	s.NoError(err)
	loadedMS, err := ctx.LoadWorkflowExecution()
	s.NoError(err)
	qr := query.NewRegistry()
	qr.BufferQuery(&workflow.WorkflowQuery{})
	loadedMS.SetQueryRegistry(qr)
	release(nil)

	request := &history.QueryWorkflowRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
		Request: &workflow.QueryWorkflowRequest{
			Execution:             &workflowExecution,
			Query:                 &workflow.WorkflowQuery{},
			QueryConsistencyLevel: common.QueryConsistencyLevelPtr(workflow.QueryConsistencyLevelStrong),
		},
	}
	resp, err := s.mockHistoryEngine.QueryWorkflow(context.Background(), request)
	s.Nil(resp)
	s.Equal(ErrConsistentQueryBufferExceeded, err)
}

func (s *engineSuite) TestQueryWorkflow_DecisionTaskDispatch_Complete() {
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("TestQueryWorkflow_DecisionTaskDispatch_Complete"),
		RunId:      common.StringPtr(constants.TestRunID),
	}
	tasklist := "testTaskList"
	identity := "testIdentity"
	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		workflowExecution.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, workflowExecution, "wType", tasklist, []byte("input"), 100, 200, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	startedEvent := test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tasklist, identity)
	test.AddDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, *startedEvent.EventId, nil, identity)
	di = test.AddDecisionTaskScheduledEvent(msBuilder)
	test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tasklist, identity)

	ms := execution.CreatePersistenceMutableState(msBuilder)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gweResponse, nil).Once()

	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(1)
	asyncQueryUpdate := func(delay time.Duration, answer []byte) {
		defer waitGroup.Done()
		<-time.After(delay)
		builder := s.getBuilder(constants.TestDomainID, workflowExecution)
		s.NotNil(builder)
		qr := builder.GetQueryRegistry()
		buffered := qr.GetBufferedIDs()
		for _, id := range buffered {
			resultType := workflow.QueryResultTypeAnswered
			completedTerminationState := &query.TerminationState{
				TerminationType: query.TerminationTypeCompleted,
				QueryResult: &workflow.WorkflowQueryResult{
					ResultType: &resultType,
					Answer:     answer,
				},
			}
			err := qr.SetTerminationState(id, completedTerminationState)
			s.NoError(err)
			state, err := qr.GetTerminationState(id)
			s.NoError(err)
			s.Equal(query.TerminationTypeCompleted, state.TerminationType)
		}
	}

	request := &history.QueryWorkflowRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
		Request: &workflow.QueryWorkflowRequest{
			Execution:             &workflowExecution,
			Query:                 &workflow.WorkflowQuery{},
			QueryConsistencyLevel: common.QueryConsistencyLevelPtr(workflow.QueryConsistencyLevelStrong),
		},
	}
	go asyncQueryUpdate(time.Second*2, []byte{1, 2, 3})
	start := time.Now()
	resp, err := s.mockHistoryEngine.QueryWorkflow(context.Background(), request)
	s.True(time.Now().After(start.Add(time.Second)))
	s.NoError(err)
	s.Equal([]byte{1, 2, 3}, resp.GetResponse().GetQueryResult())
	builder := s.getBuilder(constants.TestDomainID, workflowExecution)
	s.NotNil(builder)
	qr := builder.GetQueryRegistry()
	s.False(qr.HasBufferedQuery())
	s.False(qr.HasCompletedQuery())
	waitGroup.Wait()
}

func (s *engineSuite) TestQueryWorkflow_DecisionTaskDispatch_Unblocked() {
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("TestQueryWorkflow_DecisionTaskDispatch_Unblocked"),
		RunId:      common.StringPtr(constants.TestRunID),
	}
	tasklist := "testTaskList"
	identity := "testIdentity"
	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		workflowExecution.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, workflowExecution, "wType", tasklist, []byte("input"), 100, 200, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	startedEvent := test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tasklist, identity)
	test.AddDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, *startedEvent.EventId, nil, identity)
	di = test.AddDecisionTaskScheduledEvent(msBuilder)
	test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tasklist, identity)

	ms := execution.CreatePersistenceMutableState(msBuilder)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gweResponse, nil).Once()
	s.mockMatchingClient.EXPECT().QueryWorkflow(gomock.Any(), gomock.Any()).Return(&workflow.QueryWorkflowResponse{QueryResult: []byte{1, 2, 3}}, nil)
	s.mockHistoryEngine.matchingClient = s.mockMatchingClient
	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(1)
	asyncQueryUpdate := func(delay time.Duration, answer []byte) {
		defer waitGroup.Done()
		<-time.After(delay)
		builder := s.getBuilder(constants.TestDomainID, workflowExecution)
		s.NotNil(builder)
		qr := builder.GetQueryRegistry()
		buffered := qr.GetBufferedIDs()
		for _, id := range buffered {
			s.NoError(qr.SetTerminationState(id, &query.TerminationState{TerminationType: query.TerminationTypeUnblocked}))
			state, err := qr.GetTerminationState(id)
			s.NoError(err)
			s.Equal(query.TerminationTypeUnblocked, state.TerminationType)
		}
	}

	request := &history.QueryWorkflowRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
		Request: &workflow.QueryWorkflowRequest{
			Execution:             &workflowExecution,
			Query:                 &workflow.WorkflowQuery{},
			QueryConsistencyLevel: common.QueryConsistencyLevelPtr(workflow.QueryConsistencyLevelStrong),
		},
	}
	go asyncQueryUpdate(time.Second*2, []byte{1, 2, 3})
	start := time.Now()
	resp, err := s.mockHistoryEngine.QueryWorkflow(context.Background(), request)
	s.True(time.Now().After(start.Add(time.Second)))
	s.NoError(err)
	s.Equal([]byte{1, 2, 3}, resp.GetResponse().GetQueryResult())
	builder := s.getBuilder(constants.TestDomainID, workflowExecution)
	s.NotNil(builder)
	qr := builder.GetQueryRegistry()
	s.False(qr.HasBufferedQuery())
	s.False(qr.HasCompletedQuery())
	s.False(qr.HasUnblockedQuery())
	waitGroup.Wait()
}

func (s *engineSuite) TestRespondDecisionTaskCompletedInvalidToken() {

	invalidToken, _ := json.Marshal("bad token")
	identity := "testIdentity"

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
		RunID:      constants.TestRunID,
		ScheduleID: 2,
	})
	identity := "testIdentity"

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, &workflow.EntityNotExistsError{}).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
		RunID:      constants.TestRunID,
		ScheduleID: 2,
	})
	identity := "testIdentity"

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, errors.New("FAILED")).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
		RunId:      common.StringPtr(constants.TestRunID),
	}
	tl := "testTaskList"

	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 2,
	})
	identity := "testIdentity"

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, errors.New("FAILED")).Once()
	s.mockShardManager.On("UpdateShard", mock.Anything).Return(nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
		RunId:      common.StringPtr(constants.TestRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 2,
	})
	identity := "testIdentity"

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	startedEvent := test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	test.AddDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, *startedEvent.EventId, nil, identity)

	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
		RunId:      common.StringPtr(constants.TestRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 2,
	})
	identity := "testIdentity"

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	test.AddDecisionTaskScheduledEvent(msBuilder)

	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
		RunId:      common.StringPtr(constants.TestRunID),
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

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di1 := test.AddDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent1 := test.AddDecisionTaskStartedEvent(msBuilder, di1.ScheduleID, tl, identity)
	decisionCompletedEvent1 := test.AddDecisionTaskCompletedEvent(msBuilder, di1.ScheduleID,
		*decisionStartedEvent1.EventId, nil, identity)
	activity1ScheduledEvent, _ := test.AddActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent1.EventId,
		activity1ID, activity1Type, tl, activity1Input, 100, 10, 1, 5)
	activity2ScheduledEvent, _ := test.AddActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent1.EventId,
		activity2ID, activity2Type, tl, activity2Input, 100, 10, 1, 5)
	activity1StartedEvent := test.AddActivityTaskStartedEvent(msBuilder, *activity1ScheduledEvent.EventId, identity)
	activity2StartedEvent := test.AddActivityTaskStartedEvent(msBuilder, *activity2ScheduledEvent.EventId, identity)
	test.AddActivityTaskCompletedEvent(msBuilder, *activity1ScheduledEvent.EventId,
		*activity1StartedEvent.EventId, activity1Result, identity)
	di2 := test.AddDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent2 := test.AddDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)

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

	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	test.AddActivityTaskCompletedEvent(msBuilder, *activity2ScheduledEvent.EventId,
		*activity2StartedEvent.EventId, activity2Result, identity)

	ms2 := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: ms2}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}},
		&persistence.ConditionFailedError{}).Once()

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse2, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
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

	executionBuilder := s.getBuilder(constants.TestDomainID, we)
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
		RunId:      common.StringPtr(constants.TestRunID),
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

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

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
		ms := execution.CreatePersistenceMutableState(msBuilder)
		gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

		s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
		s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
		s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}},
			&persistence.ConditionFailedError{}).Once()
	}

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
		RunId:      common.StringPtr(constants.TestRunID),
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

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 25, 200, identity)
	di1 := test.AddDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent1 := test.AddDecisionTaskStartedEvent(msBuilder, di1.ScheduleID, tl, identity)
	decisionCompletedEvent1 := test.AddDecisionTaskCompletedEvent(msBuilder, di1.ScheduleID,
		*decisionStartedEvent1.EventId, nil, identity)
	activity1ScheduledEvent, _ := test.AddActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent1.EventId,
		activity1ID, activity1Type, tl, activity1Input, 100, 10, 1, 5)
	activity2ScheduledEvent, _ := test.AddActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent1.EventId,
		activity2ID, activity2Type, tl, activity2Input, 100, 10, 1, 5)
	activity1StartedEvent := test.AddActivityTaskStartedEvent(msBuilder, *activity1ScheduledEvent.EventId, identity)
	activity2StartedEvent := test.AddActivityTaskStartedEvent(msBuilder, *activity2ScheduledEvent.EventId, identity)
	test.AddActivityTaskCompletedEvent(msBuilder, *activity1ScheduledEvent.EventId,
		*activity1StartedEvent.EventId, activity1Result, identity)
	di2 := test.AddDecisionTaskScheduledEvent(msBuilder)
	test.AddDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)
	test.AddActivityTaskCompletedEvent(msBuilder, *activity2ScheduledEvent.EventId,
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
		ms := execution.CreatePersistenceMutableState(msBuilder)
		gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
		s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	}

	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(constants.TestDomainID, we)
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
		RunId:      common.StringPtr(constants.TestRunID),
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

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 25, 200, identity)
	di1 := test.AddDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent1 := test.AddDecisionTaskStartedEvent(msBuilder, di1.ScheduleID, tl, identity)
	decisionCompletedEvent1 := test.AddDecisionTaskCompletedEvent(msBuilder, di1.ScheduleID,
		*decisionStartedEvent1.EventId, nil, identity)
	activity1ScheduledEvent, _ := test.AddActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent1.EventId, activity1ID,
		activity1Type, tl, activity1Input, 100, 10, 1, 5)
	activity2ScheduledEvent, _ := test.AddActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent1.EventId, activity2ID,
		activity2Type, tl, activity2Input, 100, 10, 1, 5)
	activity1StartedEvent := test.AddActivityTaskStartedEvent(msBuilder, *activity1ScheduledEvent.EventId, identity)
	activity2StartedEvent := test.AddActivityTaskStartedEvent(msBuilder, *activity2ScheduledEvent.EventId, identity)
	test.AddActivityTaskCompletedEvent(msBuilder, *activity1ScheduledEvent.EventId,
		*activity1StartedEvent.EventId, activity1Result, identity)
	di2 := test.AddDecisionTaskScheduledEvent(msBuilder)
	test.AddDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)
	test.AddActivityTaskCompletedEvent(msBuilder, *activity2ScheduledEvent.EventId,
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
		ms := execution.CreatePersistenceMutableState(msBuilder)
		gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
		s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	}

	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(constants.TestDomainID, we)
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
		RunId:      common.StringPtr(constants.TestRunID),
	}
	tl := "testTaskList"
	identity := "testIdentity"
	executionContext := []byte("context")
	activity1ID := "activity1"
	activity1Type := "activity_type1"
	activity1Input := []byte("input1")
	activity1Result := []byte("activity1_result")

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 25, 200, identity)
	di1 := test.AddDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent1 := test.AddDecisionTaskStartedEvent(msBuilder, di1.ScheduleID, tl, identity)
	decisionCompletedEvent1 := test.AddDecisionTaskCompletedEvent(msBuilder, di1.ScheduleID,
		*decisionStartedEvent1.EventId, nil, identity)
	activity1ScheduledEvent, _ := test.AddActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent1.EventId, activity1ID,
		activity1Type, tl, activity1Input, 100, 10, 1, 5)
	activity1StartedEvent := test.AddActivityTaskStartedEvent(msBuilder, *activity1ScheduledEvent.EventId, identity)
	test.AddActivityTaskCompletedEvent(msBuilder, *activity1ScheduledEvent.EventId,
		*activity1StartedEvent.EventId, activity1Result, identity)
	di2 := test.AddDecisionTaskScheduledEvent(msBuilder)
	test.AddDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)

	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: di2.ScheduleID,
	})

	// Decision with nil attributes
	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
	}}

	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: execution.CreatePersistenceMutableState(msBuilder)}
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: execution.CreatePersistenceMutableState(msBuilder)}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse1, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse2, nil).Once()

	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{
		MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil,
	).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
			RunId:      common.StringPtr(constants.TestRunID),
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

		msBuilder := execution.NewMutableStateBuilderWithEventV2(
			s.mockHistoryEngine.shard,
			loggerimpl.NewDevelopmentForTest(s.Suite),
			we.GetRunId(),
			constants.TestLocalDomainEntry,
		)
		test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), workflowTimeout, 200, identity)
		di := test.AddDecisionTaskScheduledEvent(msBuilder)
		test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

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

		gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: execution.CreatePersistenceMutableState(msBuilder)}
		s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse1, nil).Once()
		if iVar.expectDecisionFail {
			gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: execution.CreatePersistenceMutableState(msBuilder)}
			s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse2, nil).Once()
		}

		s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
		s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

		_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
			DomainUUID: common.StringPtr(constants.TestDomainID),
			CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
				TaskToken:        taskToken,
				Decisions:        decisions,
				ExecutionContext: executionContext,
				Identity:         &identity,
			},
		})

		s.Nil(err, s.printHistory(msBuilder))
		executionBuilder := s.getBuilder(constants.TestDomainID, we)
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
	domainID := uuid.New()
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(constants.TestRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      we.GetRunId(),
		ScheduleID: 2,
	})
	identity := "testIdentity"
	executionContext := []byte("context")
	domainEntry := cache.NewLocalDomainCacheEntryForTest(
		&persistence.DomainInfo{ID: domainID, Name: constants.TestDomainName},
		&p.DomainConfig{
			Retention: 2,
			BadBinaries: workflow.BadBinaries{
				Binaries: map[string]*workflow.BadBinaryInfo{
					"test-bad-binary": {},
				},
			},
		},
		cluster.TestCurrentClusterName,
		nil,
	)

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		domainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	var decisions []*workflow.Decision

	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: execution.CreatePersistenceMutableState(msBuilder)}
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: execution.CreatePersistenceMutableState(msBuilder)}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse1, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse2, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()
	s.mockDomainCache.EXPECT().GetDomainByID(domainID).Return(domainEntry, nil).AnyTimes()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         &identity,
			BinaryChecksum:   common.StringPtr("test-bad-binary"),
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

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(constants.TestRunID),
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

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

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

	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(constants.TestDomainID, we)
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
		RunId:      common.StringPtr(constants.TestRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 2,
	})
	identity := "testIdentity"
	executionContext := []byte("context")

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	msBuilder.GetExecutionInfo().DecisionOriginalScheduledTimestamp = time.Now().Add(-time.Hour).UnixNano()

	decisions := []*workflow.Decision{}

	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
		RunId:      common.StringPtr(constants.TestRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 2,
	})
	identity := "testIdentity"
	executionContext := []byte("context")

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	msBuilder.GetExecutionInfo().DecisionOriginalScheduledTimestamp = time.Now().Add(-time.Minute).UnixNano()

	decisions := []*workflow.Decision{}

	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
		RunId:      common.StringPtr(constants.TestRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 2,
	})
	identity := "testIdentity"
	executionContext := []byte("context")

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	msBuilder.GetExecutionInfo().DecisionOriginalScheduledTimestamp = 0

	decisions := []*workflow.Decision{}

	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
		RunId:      common.StringPtr(constants.TestRunID),
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

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
		CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
			Result: workflowResult,
		},
	}}

	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(constants.TestDomainID, we)
	s.Equal(int64(6), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(executionContext, executionBuilder.GetExecutionInfo().ExecutionContext)
	s.Equal(persistence.WorkflowStateCompleted, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecision())
}

func (s *engineSuite) TestRespondDecisionTaskCompletedFailWorkflowSuccess() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(constants.TestRunID),
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

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeFailWorkflowExecution),
		FailWorkflowExecutionDecisionAttributes: &workflow.FailWorkflowExecutionDecisionAttributes{
			Reason:  &reason,
			Details: details,
		},
	}}

	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(constants.TestDomainID, we)
	s.Equal(int64(6), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(executionContext, executionBuilder.GetExecutionInfo().ExecutionContext)
	s.Equal(persistence.WorkflowStateCompleted, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecision())
}

func (s *engineSuite) TestRespondDecisionTaskCompletedSignalExternalWorkflowSuccess() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(constants.TestRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 2,
	})
	identity := "testIdentity"
	executionContext := []byte("context")

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeSignalExternalWorkflowExecution),
		SignalExternalWorkflowExecutionDecisionAttributes: &workflow.SignalExternalWorkflowExecutionDecisionAttributes{
			Domain: common.StringPtr(constants.TestDomainName),
			Execution: &workflow.WorkflowExecution{
				WorkflowId: we.WorkflowId,
				RunId:      we.RunId,
			},
			SignalName: common.StringPtr("signal"),
			Input:      []byte("test input"),
		},
	}}

	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(constants.TestDomainID, we)
	s.Equal(int64(6), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(executionContext, executionBuilder.GetExecutionInfo().ExecutionContext)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedStartChildWorkflowWithAbandonPolicy() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(constants.TestRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 2,
	})
	identity := "testIdentity"
	executionContext := []byte("context")

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	abandon := workflow.ParentClosePolicyAbandon
	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeStartChildWorkflowExecution),
		StartChildWorkflowExecutionDecisionAttributes: &workflow.StartChildWorkflowExecutionDecisionAttributes{
			Domain:     common.StringPtr(constants.TestDomainName),
			WorkflowId: common.StringPtr("child-workflow-id"),
			WorkflowType: &workflow.WorkflowType{
				Name: common.StringPtr("child-workflow-type"),
			},
			ParentClosePolicy: &abandon,
		},
	}}

	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(constants.TestDomainID, we)
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
		RunId:      common.StringPtr(constants.TestRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 2,
	})
	identity := "testIdentity"
	executionContext := []byte("context")

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	terminate := workflow.ParentClosePolicyTerminate
	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeStartChildWorkflowExecution),
		StartChildWorkflowExecutionDecisionAttributes: &workflow.StartChildWorkflowExecutionDecisionAttributes{
			Domain:     common.StringPtr(constants.TestDomainName),
			WorkflowId: common.StringPtr("child-workflow-id"),
			WorkflowType: &workflow.WorkflowType{
				Name: common.StringPtr("child-workflow-type"),
			},
			ParentClosePolicy: &terminate,
		},
	}}

	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(constants.TestDomainID, we)
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

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeSignalExternalWorkflowExecution),
		SignalExternalWorkflowExecutionDecisionAttributes: &workflow.SignalExternalWorkflowExecutionDecisionAttributes{
			Domain: common.StringPtr(constants.TestDomainID),
			Execution: &workflow.WorkflowExecution{
				WorkflowId: we.WorkflowId,
				RunId:      we.RunId,
			},
			SignalName: common.StringPtr("signal"),
			Input:      []byte("test input"),
		},
	}}

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
		RunId:      common.StringPtr(constants.TestRunID),
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

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

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

	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockDomainCache.EXPECT().GetDomain(foreignDomain).Return(
		nil, errors.New("get foreign domain error"),
	).Times(1)

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
		RunID:      constants.TestRunID,
		ScheduleID: 2,
	})
	identity := "testIdentity"

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, &workflow.EntityNotExistsError{}).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
		RunID:      constants.TestRunID,
		ScheduleID: 2,
	})
	identity := "testIdentity"

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, errors.New("FAILED")).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
		CompleteRequest: &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondActivityTaskCompletedIfNoAIdProvided() {

	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(constants.TestRunID),
	}
	tasklist := "testTaskList"
	identity := "testIdentity"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		ScheduleID: common.EmptyEventID,
	})

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		constants.TestRunID,
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, workflowExecution, "wType", tasklist, []byte("input"), 100, 200, identity)
	test.AddDecisionTaskScheduledEvent(msBuilder)
	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: constants.TestRunID}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(constants.TestRunID),
	}
	tasklist := "testTaskList"
	identity := "testIdentity"

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		constants.TestRunID,
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, workflowExecution, "wType", tasklist, []byte("input"), 100, 200, identity)
	test.AddDecisionTaskScheduledEvent(msBuilder)
	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: constants.TestRunID}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
		RunId:      common.StringPtr(constants.TestRunID),
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

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := test.AddDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := test.AddActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1, 5)
	test.AddActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, identity)

	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, errors.New("FAILED")).Once()
	s.mockShardManager.On("UpdateShard", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
		RunId:      common.StringPtr(constants.TestRunID),
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

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := test.AddDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := test.AddActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1, 5)
	activityStartedEvent := test.AddActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, identity)
	test.AddActivityTaskCompletedEvent(msBuilder, *activityScheduledEvent.EventId, *activityStartedEvent.EventId,
		activityResult, identity)
	test.AddDecisionTaskScheduledEvent(msBuilder)

	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
		RunId:      common.StringPtr(constants.TestRunID),
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

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := test.AddDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	test.AddActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1, 5)

	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
		RunId:      common.StringPtr(constants.TestRunID),
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

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent1 := test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent1 := test.AddDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent1.EventId, nil, identity)
	activity1ScheduledEvent, _ := test.AddActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent1.EventId, activity1ID,
		activity1Type, tl, activity1Input, 100, 10, 1, 5)
	activity2ScheduledEvent, _ := test.AddActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent1.EventId, activity2ID,
		activity2Type, tl, activity2Input, 100, 10, 1, 5)
	test.AddActivityTaskStartedEvent(msBuilder, *activity1ScheduledEvent.EventId, identity)
	test.AddActivityTaskStartedEvent(msBuilder, *activity2ScheduledEvent.EventId, identity)

	ms1 := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: ms1}

	ms2 := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: ms2}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse1, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, &persistence.ConditionFailedError{}).Once()

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse2, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
		CompleteRequest: &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result:    activity1Result,
			Identity:  &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(constants.TestDomainID, we)
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
		RunId:      common.StringPtr(constants.TestRunID),
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

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := test.AddDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := test.AddActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1, 5)
	test.AddActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, identity)

	for i := 0; i < conditionalRetryCount; i++ {
		ms := execution.CreatePersistenceMutableState(msBuilder)
		gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

		s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
		s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
		s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, &persistence.ConditionFailedError{}).Once()
	}

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
		RunId:      common.StringPtr(constants.TestRunID),
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

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := test.AddDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := test.AddActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1, 5)
	test.AddActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, identity)

	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
		CompleteRequest: &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result:    activityResult,
			Identity:  &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(constants.TestDomainID, we)
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
		RunId:      common.StringPtr(constants.TestRunID),
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

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	decisionScheduledEvent := test.AddDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := test.AddDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent.ScheduleID, tl, identity)
	decisionCompletedEvent := test.AddDecisionTaskCompletedEvent(msBuilder, decisionScheduledEvent.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := test.AddActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1, 5)
	test.AddActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, identity)

	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: *we.RunId}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
		CompleteRequest: &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result:    activityResult,
			Identity:  &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(constants.TestDomainID, we)
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
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
		RunID:      constants.TestRunID,
		ScheduleID: 2,
	})
	identity := "testIdentity"

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil,
		&workflow.EntityNotExistsError{}).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
		RunID:      constants.TestRunID,
		ScheduleID: 2,
	})
	identity := "testIdentity"

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil,
		errors.New("FAILED")).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(constants.TestRunID),
	}
	tasklist := "testTaskList"
	identity := "testIdentity"

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		constants.TestRunID,
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, workflowExecution, "wType", tasklist, []byte("input"), 100, 200, identity)
	test.AddDecisionTaskScheduledEvent(msBuilder)
	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: constants.TestRunID}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(constants.TestRunID),
	}
	tasklist := "testTaskList"
	identity := "testIdentity"

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		constants.TestRunID,
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, workflowExecution, "wType", tasklist, []byte("input"), 100, 200, identity)
	test.AddDecisionTaskScheduledEvent(msBuilder)
	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: constants.TestRunID}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
		RunId:      common.StringPtr(constants.TestRunID),
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

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := test.AddDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := test.AddActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1, 5)
	test.AddActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, identity)

	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, errors.New("FAILED")).Once()
	s.mockShardManager.On("UpdateShard", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
		RunId:      common.StringPtr(constants.TestRunID),
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

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := test.AddDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := test.AddActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1, 5)
	activityStartedEvent := test.AddActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, identity)
	test.AddActivityTaskFailedEvent(msBuilder, *activityScheduledEvent.EventId, *activityStartedEvent.EventId,
		failReason, details, identity)
	test.AddDecisionTaskScheduledEvent(msBuilder)

	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
		RunId:      common.StringPtr(constants.TestRunID),
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

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := test.AddDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	test.AddActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1, 5)

	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
		RunId:      common.StringPtr(constants.TestRunID),
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

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 25, 25, identity)
	di1 := test.AddDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent1 := test.AddDecisionTaskStartedEvent(msBuilder, di1.ScheduleID, tl, identity)
	decisionCompletedEvent1 := test.AddDecisionTaskCompletedEvent(msBuilder, di1.ScheduleID,
		*decisionStartedEvent1.EventId, nil, identity)
	activity1ScheduledEvent, _ := test.AddActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent1.EventId, activity1ID,
		activity1Type, tl, activity1Input, 100, 10, 1, 5)
	activity2ScheduledEvent, _ := test.AddActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent1.EventId, activity2ID,
		activity2Type, tl, activity2Input, 100, 10, 1, 5)
	test.AddActivityTaskStartedEvent(msBuilder, *activity1ScheduledEvent.EventId, identity)
	activity2StartedEvent := test.AddActivityTaskStartedEvent(msBuilder, *activity2ScheduledEvent.EventId, identity)

	ms1 := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: ms1}

	test.AddActivityTaskCompletedEvent(msBuilder, *activity2ScheduledEvent.EventId,
		*activity2StartedEvent.EventId, activity2Result, identity)
	test.AddDecisionTaskScheduledEvent(msBuilder)

	ms2 := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: ms2}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse1, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, &persistence.ConditionFailedError{}).Once()

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse2, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
		FailedRequest: &workflow.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Reason:    &failReason,
			Details:   details,
			Identity:  &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(constants.TestDomainID, we)
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
		RunId:      common.StringPtr(constants.TestRunID),
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

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := test.AddDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := test.AddActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1, 5)
	test.AddActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, identity)

	for i := 0; i < conditionalRetryCount; i++ {
		ms := execution.CreatePersistenceMutableState(msBuilder)
		gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

		s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
		s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
		s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, &persistence.ConditionFailedError{}).Once()
	}

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
		RunId:      common.StringPtr(constants.TestRunID),
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

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := test.AddDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := test.AddActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1, 5)
	test.AddActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, identity)

	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
		FailedRequest: &workflow.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Reason:    &failReason,
			Details:   failDetails,
			Identity:  &identity,
		},
	})
	s.Nil(err)
	executionBuilder := s.getBuilder(constants.TestDomainID, we)
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
		RunId:      common.StringPtr(constants.TestRunID),
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

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	decisionScheduledEvent := test.AddDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := test.AddDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent.ScheduleID, tl, identity)
	decisionCompletedEvent := test.AddDecisionTaskCompletedEvent(msBuilder, decisionScheduledEvent.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := test.AddActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1, 5)
	test.AddActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, identity)

	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: *we.RunId}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
		FailedRequest: &workflow.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Reason:    &failReason,
			Details:   failDetails,
			Identity:  &identity,
		},
	})
	s.Nil(err)
	executionBuilder := s.getBuilder(constants.TestDomainID, we)
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
		RunId:      common.StringPtr(constants.TestRunID),
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

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := test.AddDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := test.AddActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1, 0)
	test.AddActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, identity)

	// No HeartBeat timer running.
	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	detais := []byte("details")

	_, err := s.mockHistoryEngine.RecordActivityTaskHeartbeat(context.Background(), &history.RecordActivityTaskHeartbeatRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
		RunId:      common.StringPtr(constants.TestRunID),
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

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := test.AddDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := test.AddActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1, 1)
	test.AddActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, identity)

	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	// HeartBeat timer running.
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	detais := []byte("details")

	_, err := s.mockHistoryEngine.RecordActivityTaskHeartbeat(context.Background(), &history.RecordActivityTaskHeartbeatRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
		HeartbeatRequest: &workflow.RecordActivityTaskHeartbeatRequest{
			TaskToken: taskToken,
			Identity:  &identity,
			Details:   detais,
		},
	})
	s.Nil(err)
	executionBuilder := s.getBuilder(constants.TestDomainID, we)
	s.Equal(int64(7), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecision())
}

func (s *engineSuite) TestRecordActivityTaskHeartBeatByIDSuccess() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(constants.TestRunID),
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

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := test.AddDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := test.AddActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1, 0)
	test.AddActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, identity)

	// No HeartBeat timer running.
	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	detais := []byte("details")

	_, err := s.mockHistoryEngine.RecordActivityTaskHeartbeat(context.Background(), &history.RecordActivityTaskHeartbeatRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
		RunId:      common.StringPtr(constants.TestRunID),
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

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := test.AddDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	test.AddActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1, 1)

	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCanceled(context.Background(), &history.RespondActivityTaskCanceledRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
		RunId:      common.StringPtr(constants.TestRunID),
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

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := test.AddDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := test.AddActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1, 1)
	test.AddActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, identity)
	_, _, err := msBuilder.AddActivityTaskCancelRequestedEvent(*decisionCompletedEvent.EventId, activityID, identity)
	s.Nil(err)

	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	err = s.mockHistoryEngine.RespondActivityTaskCanceled(context.Background(), &history.RespondActivityTaskCanceledRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
		CancelRequest: &workflow.RespondActivityTaskCanceledRequest{
			TaskToken: taskToken,
			Identity:  &identity,
			Details:   []byte("details"),
		},
	})
	s.Nil(err)
	executionBuilder := s.getBuilder(constants.TestDomainID, we)
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
		RunId:      common.StringPtr(constants.TestRunID),
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

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	decisionScheduledEvent := test.AddDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := test.AddDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent.ScheduleID, tl, identity)
	decisionCompletedEvent := test.AddDecisionTaskCompletedEvent(msBuilder, decisionScheduledEvent.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := test.AddActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1, 1)
	test.AddActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, identity)
	_, _, err := msBuilder.AddActivityTaskCancelRequestedEvent(*decisionCompletedEvent.EventId, activityID, identity)
	s.Nil(err)

	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: *we.RunId}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	err = s.mockHistoryEngine.RespondActivityTaskCanceled(context.Background(), &history.RespondActivityTaskCanceledRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
		CancelRequest: &workflow.RespondActivityTaskCanceledRequest{
			TaskToken: taskToken,
			Identity:  &identity,
			Details:   []byte("details"),
		},
	})
	s.Nil(err)
	executionBuilder := s.getBuilder(constants.TestDomainID, we)
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
		DomainUUID: common.StringPtr(constants.TestDomainID),
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

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		constants.TestRunID,
		constants.TestLocalDomainEntry,
	)
	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: constants.TestRunID}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCanceled(context.Background(), &history.RespondActivityTaskCanceledRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
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

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		constants.TestRunID,
		constants.TestLocalDomainEntry,
	)
	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: constants.TestRunID}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCanceled(context.Background(), &history.RespondActivityTaskCanceledRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
		RunId:      common.StringPtr(constants.TestRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 2,
	})
	identity := "testIdentity"
	activityID := "activity1_id"

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeRequestCancelActivityTask),
		RequestCancelActivityTaskDecisionAttributes: &workflow.RequestCancelActivityTaskDecisionAttributes{
			ActivityId: common.StringPtr(activityID),
		},
	}}

	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)
	executionBuilder := s.getBuilder(constants.TestDomainID, we)
	s.Equal(int64(7), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecision())
}

func (s *engineSuite) TestRequestCancel_RespondDecisionTaskCompleted_Scheduled() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(constants.TestRunID),
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

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := test.AddDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	test.AddActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1, 1)
	di2 := test.AddDecisionTaskScheduledEvent(msBuilder)
	test.AddDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)

	ms := execution.CreatePersistenceMutableState(msBuilder)
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
		DomainUUID: common.StringPtr(constants.TestDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(constants.TestDomainID, we)
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
		RunId:      common.StringPtr(constants.TestRunID),
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

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := test.AddDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := test.AddActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1, 0)
	test.AddActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, identity)
	di2 := test.AddDecisionTaskScheduledEvent(msBuilder)
	test.AddDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)

	ms := execution.CreatePersistenceMutableState(msBuilder)
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
		DomainUUID: common.StringPtr(constants.TestDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(constants.TestDomainID, we)
	s.Equal(int64(11), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(8), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecision())
}

func (s *engineSuite) TestRequestCancel_RespondDecisionTaskCompleted_Completed() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(constants.TestRunID),
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

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := test.AddDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	test.AddActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1, 0)
	di2 := test.AddDecisionTaskScheduledEvent(msBuilder)
	test.AddDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)

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

	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(constants.TestDomainID, we)
	s.Equal(int64(11), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(7), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateCompleted, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecision())
}

func (s *engineSuite) TestRequestCancel_RespondDecisionTaskCompleted_NoHeartBeat() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(constants.TestRunID),
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

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := test.AddDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := test.AddActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1, 0)
	test.AddActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, identity)
	di2 := test.AddDecisionTaskScheduledEvent(msBuilder)
	test.AddDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)

	ms := execution.CreatePersistenceMutableState(msBuilder)
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
		DomainUUID: common.StringPtr(constants.TestDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(constants.TestDomainID, we)
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
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
		DomainUUID: common.StringPtr(constants.TestDomainID),
		CancelRequest: &workflow.RespondActivityTaskCanceledRequest{
			TaskToken: activityTaskToken,
			Identity:  &identity,
			Details:   []byte("details"),
		},
	})
	s.Nil(err)

	executionBuilder = s.getBuilder(constants.TestDomainID, we)
	s.Equal(int64(13), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(8), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.True(executionBuilder.HasPendingDecision())
}

func (s *engineSuite) TestRequestCancel_RespondDecisionTaskCompleted_Success() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(constants.TestRunID),
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

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := test.AddDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := test.AddActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1, 1)
	test.AddActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, identity)
	di2 := test.AddDecisionTaskScheduledEvent(msBuilder)
	test.AddDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)

	ms := execution.CreatePersistenceMutableState(msBuilder)
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
		DomainUUID: common.StringPtr(constants.TestDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(constants.TestDomainID, we)
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
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
		DomainUUID: common.StringPtr(constants.TestDomainID),
		CancelRequest: &workflow.RespondActivityTaskCanceledRequest{
			TaskToken: activityTaskToken,
			Identity:  &identity,
			Details:   []byte("details"),
		},
	})
	s.Nil(err)

	executionBuilder = s.getBuilder(constants.TestDomainID, we)
	s.Equal(int64(13), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(8), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.True(executionBuilder.HasPendingDecision())
}

func (s *engineSuite) TestRequestCancel_RespondDecisionTaskCompleted_SuccessWithQueries() {
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(constants.TestRunID),
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

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := test.AddDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := test.AddActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1, 1)
	test.AddActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, identity)
	di2 := test.AddDecisionTaskScheduledEvent(msBuilder)
	test.AddDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)

	ms := execution.CreatePersistenceMutableState(msBuilder)
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

	// load mutable state such that it already exists in memory when respond decision task is called
	// this enables us to set query registry on it
	ctx, release, err := s.mockHistoryEngine.executionCache.GetOrCreateWorkflowExecutionForBackground(constants.TestDomainID, we)
	s.NoError(err)
	loadedMS, err := ctx.LoadWorkflowExecution()
	s.NoError(err)
	qr := query.NewRegistry()
	id1, _ := qr.BufferQuery(&workflow.WorkflowQuery{})
	id2, _ := qr.BufferQuery(&workflow.WorkflowQuery{})
	id3, _ := qr.BufferQuery(&workflow.WorkflowQuery{})
	loadedMS.SetQueryRegistry(qr)
	release(nil)
	result1 := &workflow.WorkflowQueryResult{
		ResultType: common.QueryResultTypePtr(workflow.QueryResultTypeAnswered),
		Answer:     []byte{1, 2, 3},
	}
	result2 := &workflow.WorkflowQueryResult{
		ResultType:   common.QueryResultTypePtr(workflow.QueryResultTypeFailed),
		ErrorMessage: common.StringPtr("error reason"),
	}
	queryResults := map[string]*workflow.WorkflowQueryResult{
		id1: result1,
		id2: result2,
	}
	_, err = s.mockHistoryEngine.RespondDecisionTaskCompleted(s.constructCallContext(cc.GoWorkerConsistentQueryVersion), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
			QueryResults:     queryResults,
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(constants.TestDomainID, we)
	s.Equal(int64(11), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(8), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecision())
	s.Len(qr.GetCompletedIDs(), 2)
	completed1, err := qr.GetTerminationState(id1)
	s.NoError(err)
	s.True(result1.Equals(completed1.QueryResult))
	s.Equal(query.TerminationTypeCompleted, completed1.TerminationType)
	completed2, err := qr.GetTerminationState(id2)
	s.NoError(err)
	s.True(result2.Equals(completed2.QueryResult))
	s.Equal(query.TerminationTypeCompleted, completed2.TerminationType)
	s.Len(qr.GetBufferedIDs(), 0)
	s.Len(qr.GetFailedIDs(), 0)
	s.Len(qr.GetUnblockedIDs(), 1)
	unblocked1, err := qr.GetTerminationState(id3)
	s.NoError(err)
	s.Nil(unblocked1.QueryResult)
	s.Equal(query.TerminationTypeUnblocked, unblocked1.TerminationType)

	// Try recording activity heartbeat
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	activityTaskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      we.GetRunId(),
		ScheduleID: 5,
	})

	hbResponse, err := s.mockHistoryEngine.RecordActivityTaskHeartbeat(context.Background(), &history.RecordActivityTaskHeartbeatRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
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
		DomainUUID: common.StringPtr(constants.TestDomainID),
		CancelRequest: &workflow.RespondActivityTaskCanceledRequest{
			TaskToken: activityTaskToken,
			Identity:  &identity,
			Details:   []byte("details"),
		},
	})
	s.Nil(err)

	executionBuilder = s.getBuilder(constants.TestDomainID, we)
	s.Equal(int64(13), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(8), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.True(executionBuilder.HasPendingDecision())
}

func (s *engineSuite) TestRequestCancel_RespondDecisionTaskCompleted_SuccessWithConsistentQueriesUnsupported() {
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(constants.TestRunID),
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

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := test.AddDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := test.AddActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1, 1)
	test.AddActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, identity)
	di2 := test.AddDecisionTaskScheduledEvent(msBuilder)
	test.AddDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)

	ms := execution.CreatePersistenceMutableState(msBuilder)
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

	// load mutable state such that it already exists in memory when respond decision task is called
	// this enables us to set query registry on it
	ctx, release, err := s.mockHistoryEngine.executionCache.GetOrCreateWorkflowExecutionForBackground(constants.TestDomainID, we)
	s.NoError(err)
	loadedMS, err := ctx.LoadWorkflowExecution()
	s.NoError(err)
	qr := query.NewRegistry()
	qr.BufferQuery(&workflow.WorkflowQuery{})
	qr.BufferQuery(&workflow.WorkflowQuery{})
	qr.BufferQuery(&workflow.WorkflowQuery{})
	loadedMS.SetQueryRegistry(qr)
	release(nil)
	_, err = s.mockHistoryEngine.RespondDecisionTaskCompleted(s.constructCallContext("0.0.0"), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(constants.TestDomainID, we)
	s.Equal(int64(11), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(8), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecision())
	s.Len(qr.GetBufferedIDs(), 0)
	s.Len(qr.GetCompletedIDs(), 0)
	s.Len(qr.GetUnblockedIDs(), 0)
	s.Len(qr.GetFailedIDs(), 3)
}

func (s *engineSuite) constructCallContext(featureVersion string) context.Context {
	ctx := context.Background()
	ctx, call := encoding.NewInboundCall(ctx)
	err := call.ReadFromRequest(&transport.Request{
		Headers: transport.NewHeaders().With(common.ClientImplHeaderName, cc.GoSDK).With(common.FeatureVersionHeaderName, featureVersion),
	})
	s.NoError(err)
	return ctx
}

func (s *engineSuite) TestStarTimer_DuplicateTimerID() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(constants.TestRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 2,
	})
	identity := "testIdentity"
	timerID := "t1"

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)

	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	ms := execution.CreatePersistenceMutableState(msBuilder)
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
		DomainUUID: common.StringPtr(constants.TestDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(constants.TestDomainID, we)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)

	// Try to add the same timer ID again.
	di2 := test.AddDecisionTaskScheduledEvent(executionBuilder)
	test.AddDecisionTaskStartedEvent(executionBuilder, di2.ScheduleID, tl, identity)
	taskToken2, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: di2.ScheduleID,
	})

	ms2 := execution.CreatePersistenceMutableState(executionBuilder)
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
		DomainUUID: common.StringPtr(constants.TestDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken2,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)

	s.True(decisionFailedEvent)
	executionBuilder = s.getBuilder(constants.TestDomainID, we)
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
		RunId:      common.StringPtr(constants.TestRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 6,
	})
	identity := "testIdentity"
	timerID := "t1"

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	// Verify cancel timer with a start event.
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := test.AddDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	test.AddTimerStartedEvent(msBuilder, *decisionCompletedEvent.EventId, timerID, 10)
	di2 := test.AddDecisionTaskScheduledEvent(msBuilder)
	test.AddDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)

	ms := execution.CreatePersistenceMutableState(msBuilder)
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
		DomainUUID: common.StringPtr(constants.TestDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(constants.TestDomainID, we)
	s.Equal(int64(10), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(7), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecision())
}

func (s *engineSuite) TestCancelTimer_RespondDecisionTaskCompleted_NoStartTimer() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(constants.TestRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 2,
	})
	identity := "testIdentity"
	timerID := "t1"

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	// Verify cancel timer with a start event.
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	ms := execution.CreatePersistenceMutableState(msBuilder)
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
		DomainUUID: common.StringPtr(constants.TestDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(constants.TestDomainID, we)
	s.Equal(int64(6), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecision())
}

func (s *engineSuite) TestCancelTimer_RespondDecisionTaskCompleted_TimerFired() {

	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(constants.TestRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 6,
	})
	identity := "testIdentity"
	timerID := "t1"

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	// Verify cancel timer with a start event.
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := test.AddDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := test.AddDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := test.AddDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	test.AddTimerStartedEvent(msBuilder, *decisionCompletedEvent.EventId, timerID, 10)
	di2 := test.AddDecisionTaskScheduledEvent(msBuilder)
	test.AddDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)
	test.AddTimerFiredEvent(msBuilder, timerID)
	_, _, err := msBuilder.CloseTransactionAsMutation(time.Now(), execution.TransactionPolicyActive)
	s.Nil(err)

	ms := execution.CreatePersistenceMutableState(msBuilder)
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
		DomainUUID: common.StringPtr(constants.TestDomainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(constants.TestDomainID, we)
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
		RunId:      common.StringPtr(constants.TestRunID),
	}
	tasklist := "testTaskList"
	identity := "testIdentity"
	signalName := "my signal name"
	input := []byte("test input")
	signalRequest = &history.SignalWorkflowExecutionRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
		SignalRequest: &workflow.SignalWorkflowExecutionRequest{
			Domain:            common.StringPtr(constants.TestDomainID),
			WorkflowExecution: &we,
			Identity:          common.StringPtr(identity),
			SignalName:        common.StringPtr(signalName),
			Input:             input,
		},
	}

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tasklist, []byte("input"), 100, 200, identity)
	test.AddDecisionTaskScheduledEvent(msBuilder)
	ms := execution.CreatePersistenceMutableState(msBuilder)
	ms.ExecutionInfo.DomainID = constants.TestDomainID
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
		RunId:      common.StringPtr(constants.TestRunID),
	}
	tasklist := "testTaskList"
	identity := "testIdentity"
	signalName := "my signal name 2"
	input := []byte("test input 2")
	requestID := uuid.New()
	signalRequest = &history.SignalWorkflowExecutionRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
		SignalRequest: &workflow.SignalWorkflowExecutionRequest{
			Domain:            common.StringPtr(constants.TestDomainID),
			WorkflowExecution: &we,
			Identity:          common.StringPtr(identity),
			SignalName:        common.StringPtr(signalName),
			Input:             input,
			RequestId:         common.StringPtr(requestID),
		},
	}

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, we, "wType", tasklist, []byte("input"), 100, 200, identity)
	test.AddDecisionTaskScheduledEvent(msBuilder)
	ms := execution.CreatePersistenceMutableState(msBuilder)
	// assume duplicate request id
	ms.SignalRequestedIDs = make(map[string]struct{})
	ms.SignalRequestedIDs[requestID] = struct{}{}
	ms.ExecutionInfo.DomainID = constants.TestDomainID
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
		RunId:      common.StringPtr(constants.TestRunID),
	}
	tasklist := "testTaskList"
	identity := "testIdentity"
	signalName := "my signal name"
	input := []byte("test input")
	signalRequest = &history.SignalWorkflowExecutionRequest{
		DomainUUID: common.StringPtr(constants.TestDomainID),
		SignalRequest: &workflow.SignalWorkflowExecutionRequest{
			Domain:            common.StringPtr(constants.TestDomainID),
			WorkflowExecution: we,
			Identity:          common.StringPtr(identity),
			SignalName:        common.StringPtr(signalName),
			Input:             input,
		},
	}

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		we.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, *we, "wType", tasklist, []byte("input"), 100, 200, identity)
	test.AddDecisionTaskScheduledEvent(msBuilder)
	ms := execution.CreatePersistenceMutableState(msBuilder)
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

	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(constants.TestRunID),
	}
	tasklist := "testTaskList"
	identity := "testIdentity"
	requestID := uuid.New()
	removeRequest = &history.RemoveSignalMutableStateRequest{
		DomainUUID:        common.StringPtr(constants.TestDomainID),
		WorkflowExecution: &workflowExecution,
		RequestId:         common.StringPtr(requestID),
	}

	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		constants.TestRunID,
		constants.TestLocalDomainEntry,
	)
	test.AddWorkflowExecutionStartedEvent(msBuilder, workflowExecution, "wType", tasklist, []byte("input"), 100, 200, identity)
	test.AddDecisionTaskScheduledEvent(msBuilder)
	ms := execution.CreatePersistenceMutableState(msBuilder)
	ms.ExecutionInfo.DomainID = constants.TestDomainID
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	err = s.mockHistoryEngine.RemoveSignalMutableState(context.Background(), removeRequest)
	s.Nil(err)
}

func (s *engineSuite) TestReapplyEvents_ReturnSuccess() {
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("test-reapply"),
		RunId:      common.StringPtr(constants.TestRunID),
	}
	history := []*workflow.HistoryEvent{
		{
			EventId:   common.Int64Ptr(1),
			EventType: common.EventTypePtr(workflow.EventTypeWorkflowExecutionSignaled),
			Version:   common.Int64Ptr(1),
		},
	}
	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		workflowExecution.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: constants.TestRunID}
	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockEventsReapplier.EXPECT().ReapplyEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1)

	err := s.mockHistoryEngine.ReapplyEvents(
		context.Background(),
		constants.TestDomainID,
		*workflowExecution.WorkflowId,
		*workflowExecution.RunId,
		history,
	)
	s.NoError(err)
}

func (s *engineSuite) TestReapplyEvents_IgnoreSameVersionEvents() {
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("test-reapply-same-version"),
		RunId:      common.StringPtr(constants.TestRunID),
	}
	history := []*workflow.HistoryEvent{
		{
			EventId:   common.Int64Ptr(1),
			EventType: common.EventTypePtr(workflow.EventTypeTimerStarted),
			Version:   common.Int64Ptr(common.EmptyVersion),
		},
	}
	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		workflowExecution.GetRunId(),
		constants.TestLocalDomainEntry,
	)

	ms := execution.CreatePersistenceMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: constants.TestRunID}
	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockEventsReapplier.EXPECT().ReapplyEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	err := s.mockHistoryEngine.ReapplyEvents(
		context.Background(),
		constants.TestDomainID,
		*workflowExecution.WorkflowId,
		*workflowExecution.RunId,
		history,
	)
	s.NoError(err)
}

func (s *engineSuite) TestReapplyEvents_ResetWorkflow() {
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("test-reapply-reset-workflow"),
		RunId:      common.StringPtr(constants.TestRunID),
	}
	history := []*workflow.HistoryEvent{
		{
			EventId:   common.Int64Ptr(1),
			EventType: common.EventTypePtr(workflow.EventTypeWorkflowExecutionSignaled),
			Version:   common.Int64Ptr(100),
		},
	}
	msBuilder := execution.NewMutableStateBuilderWithEventV2(
		s.mockHistoryEngine.shard,
		loggerimpl.NewDevelopmentForTest(s.Suite),
		workflowExecution.GetRunId(),
		constants.TestLocalDomainEntry,
	)
	ms := execution.CreatePersistenceMutableState(msBuilder)
	ms.ExecutionInfo.State = persistence.WorkflowStateCompleted
	ms.ExecutionInfo.LastProcessedEvent = 1
	token, err := msBuilder.GetCurrentBranchToken()
	s.NoError(err)
	item := persistence.NewVersionHistoryItem(1, 1)
	versionHistory := persistence.NewVersionHistory(token, []*persistence.VersionHistoryItem{item})
	ms.VersionHistories = persistence.NewVersionHistories(versionHistory)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: constants.TestRunID}
	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockEventsReapplier.EXPECT().ReapplyEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	s.mockWorkflowResetter.EXPECT().ResetWorkflow(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).Return(nil).Times(1)
	err = s.mockHistoryEngine.ReapplyEvents(
		context.Background(),
		constants.TestDomainID,
		*workflowExecution.WorkflowId,
		*workflowExecution.RunId,
		history,
	)
	s.NoError(err)
}

func (s *engineSuite) getBuilder(testDomainID string, we workflow.WorkflowExecution) execution.MutableState {
	context, release, err := s.mockHistoryEngine.executionCache.GetOrCreateWorkflowExecutionForBackground(testDomainID, we)
	if err != nil {
		return nil
	}
	defer release(nil)

	return context.GetWorkflowExecution()
}

func (s *engineSuite) getActivityScheduledEvent(msBuilder execution.MutableState,
	scheduleID int64) *workflow.HistoryEvent {
	event, _ := msBuilder.GetActivityScheduledEvent(scheduleID)
	return event
}

func (s *engineSuite) printHistory(builder execution.MutableState) string {
	return builder.GetHistoryBuilder().GetHistory().String()
}
