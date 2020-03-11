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
	"errors"
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
	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/.gen/proto/token"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/clock"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/loggerimpl"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/mocks"
	p "github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/primitives"
)

type (
	engine2Suite struct {
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

func TestEngine2Suite(t *testing.T) {
	s := new(engine2Suite)
	suite.Run(t, s)
}

func (s *engine2Suite) SetupSuite() {
	s.config = NewDynamicConfigForTest()
}

func (s *engine2Suite) TearDownSuite() {
}

func (s *engine2Suite) SetupTest() {
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
		&p.ShardInfoWithFailover{
			ShardInfo: &persistenceblobs.ShardInfo{
				ShardID:          0,
				RangeID:          1,
				TransferAckLevel: 0,
			}},
		s.config,
	)

	s.mockDomainCache = s.mockShard.resource.DomainCache
	s.mockExecutionMgr = s.mockShard.resource.ExecutionMgr
	s.mockHistoryV2Mgr = s.mockShard.resource.HistoryMgr
	s.mockClusterMetadata = s.mockShard.resource.ClusterMetadata
	s.mockEventsCache = s.mockShard.mockEventsCache
	s.mockDomainCache.EXPECT().GetDomainByID(gomock.Any()).Return(cache.NewLocalDomainCacheEntryForTest(
		&p.DomainInfo{ID: testDomainID}, &p.DomainConfig{}, "", nil,
	), nil).AnyTimes()
	s.mockEventsCache.EXPECT().putEvent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

	s.mockClusterMetadata.EXPECT().IsGlobalDomainEnabled().Return(false).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(common.EmptyVersion).Return(cluster.TestCurrentClusterName).AnyTimes()

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

func (s *engine2Suite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.Finish(s.T())
}

func (s *engine2Suite) TestRecordDecisionTaskStartedSuccessStickyExpired() {
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
	expectedResponse.StickyExecutionEnabled = false
	expectedResponse.NextEventId = msBuilder.GetNextEventID() + 1
	expectedResponse.Attempt = di.Attempt
	expectedResponse.WorkflowExecutionTaskList = &commonproto.TaskList{
		Name: executionInfo.TaskList,
		Kind: enums.TaskListKindNormal,
	}
	expectedResponse.BranchToken, _ = msBuilder.GetCurrentBranchToken()

	response, err := s.historyEngine.RecordDecisionTaskStarted(context.Background(), &request)
	s.Nil(err)
	s.NotNil(response)
	expectedResponse.StartedTimestamp = response.StartedTimestamp
	expectedResponse.ScheduledTimestamp = 0
	expectedResponse.Queries = make(map[string]*commonproto.WorkflowQuery)
	s.Equal(&expectedResponse, response)
}

func (s *engine2Suite) TestRecordDecisionTaskStartedSuccessStickyEnabled() {
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
	currentBranchTokken, err := msBuilder.GetCurrentBranchToken()
	s.NoError(err)
	expectedResponse.BranchToken = currentBranchTokken

	response, err := s.historyEngine.RecordDecisionTaskStarted(context.Background(), &request)
	s.Nil(err)
	s.NotNil(response)
	expectedResponse.StartedTimestamp = response.StartedTimestamp
	expectedResponse.ScheduledTimestamp = 0
	expectedResponse.Queries = make(map[string]*commonproto.WorkflowQuery)
	s.Equal(&expectedResponse, response)
}

func (s *engine2Suite) TestRecordDecisionTaskStartedIfNoExecution() {
	domainID := testDomainID
	workflowExecution := &commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}

	identity := "testIdentity"
	tl := "testTaskList"

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, serviceerror.NewNotFound("")).Once()

	response, err := s.historyEngine.RecordDecisionTaskStarted(context.Background(), &historyservice.RecordDecisionTaskStartedRequest{
		DomainUUID:        domainID,
		WorkflowExecution: workflowExecution,
		ScheduleId:        2,
		TaskId:            100,
		RequestId:         "reqId",
		PollRequest: &workflowservice.PollForDecisionTaskRequest{
			TaskList: &commonproto.TaskList{
				Name: tl,
			},
			Identity: identity,
		},
	})
	s.Nil(response)
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *engine2Suite) TestRecordDecisionTaskStartedIfGetExecutionFailed() {
	domainID := testDomainID
	workflowExecution := &commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}

	identity := "testIdentity"
	tl := "testTaskList"

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, errors.New("FAILED")).Once()

	response, err := s.historyEngine.RecordDecisionTaskStarted(context.Background(), &historyservice.RecordDecisionTaskStartedRequest{
		DomainUUID:        domainID,
		WorkflowExecution: workflowExecution,
		ScheduleId:        2,
		TaskId:            100,
		RequestId:         "reqId",
		PollRequest: &workflowservice.PollForDecisionTaskRequest{
			TaskList: &commonproto.TaskList{
				Name: tl,
			},
			Identity: identity,
		},
	})
	s.Nil(response)
	s.NotNil(err)
	s.EqualError(err, "FAILED")
}

func (s *engine2Suite) TestRecordDecisionTaskStartedIfTaskAlreadyStarted() {
	domainID := testDomainID
	workflowExecution := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}

	identity := "testIdentity"
	tl := "testTaskList"

	msBuilder := s.createExecutionStartedState(workflowExecution, tl, identity, true)
	ms := createMutableState(msBuilder)
	gwmsResponse := &p.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	response, err := s.historyEngine.RecordDecisionTaskStarted(context.Background(), &historyservice.RecordDecisionTaskStartedRequest{
		DomainUUID:        domainID,
		WorkflowExecution: &workflowExecution,
		ScheduleId:        2,
		TaskId:            100,
		RequestId:         "reqId",
		PollRequest: &workflowservice.PollForDecisionTaskRequest{
			TaskList: &commonproto.TaskList{
				Name: tl,
			},
			Identity: identity,
		},
	})
	s.Nil(response)
	s.NotNil(err)
	s.IsType(&serviceerror.EventAlreadyStarted{}, err)
	s.logger.Error("RecordDecisionTaskStarted failed with", tag.Error(err))
}

func (s *engine2Suite) TestRecordDecisionTaskStartedIfTaskAlreadyCompleted() {
	domainID := testDomainID
	workflowExecution := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}

	identity := "testIdentity"
	tl := "testTaskList"

	msBuilder := s.createExecutionStartedState(workflowExecution, tl, identity, true)
	addDecisionTaskCompletedEvent(msBuilder, int64(2), int64(3), nil, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &p.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	response, err := s.historyEngine.RecordDecisionTaskStarted(context.Background(), &historyservice.RecordDecisionTaskStartedRequest{
		DomainUUID:        domainID,
		WorkflowExecution: &workflowExecution,
		ScheduleId:        2,
		TaskId:            100,
		RequestId:         "reqId",
		PollRequest: &workflowservice.PollForDecisionTaskRequest{
			TaskList: &commonproto.TaskList{
				Name: tl,
			},
			Identity: identity,
		},
	})
	s.Nil(response)
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
	s.logger.Error("RecordDecisionTaskStarted failed with", tag.Error(err))
}

func (s *engine2Suite) TestRecordDecisionTaskStartedConflictOnUpdate() {
	domainID := testDomainID
	workflowExecution := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}

	identity := "testIdentity"
	tl := "testTaskList"

	msBuilder := s.createExecutionStartedState(workflowExecution, tl, identity, false)

	ms := createMutableState(msBuilder)
	gwmsResponse := &p.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil, &p.ConditionFailedError{}).Once()

	ms2 := createMutableState(msBuilder)
	gwmsResponse2 := &p.GetWorkflowExecutionResponse{State: ms2}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse2, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{
		MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{},
	}, nil).Once()

	response, err := s.historyEngine.RecordDecisionTaskStarted(context.Background(), &historyservice.RecordDecisionTaskStartedRequest{
		DomainUUID:        domainID,
		WorkflowExecution: &workflowExecution,
		ScheduleId:        2,
		TaskId:            100,
		RequestId:         "reqId",
		PollRequest: &workflowservice.PollForDecisionTaskRequest{
			TaskList: &commonproto.TaskList{
				Name: tl,
			},
			Identity: identity,
		},
	})
	s.Nil(err)
	s.NotNil(response)
	s.Equal("wType", response.WorkflowType.Name)
	s.True(response.PreviousStartedEventId == 0)
	s.Equal(int64(3), response.StartedEventId)
}

func (s *engine2Suite) TestRecordDecisionTaskRetrySameRequest() {
	domainID := testDomainID
	workflowExecution := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}

	tl := "testTaskList"
	identity := "testIdentity"
	requestID := "testRecordDecisionTaskRetrySameRequestID"

	msBuilder := s.createExecutionStartedState(workflowExecution, tl, identity, false)
	ms := createMutableState(msBuilder)
	gwmsResponse := &p.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil, &p.ConditionFailedError{}).Once()

	startedEventID := addDecisionTaskStartedEventWithRequestID(msBuilder, int64(2), requestID, tl, identity)
	ms2 := createMutableState(msBuilder)
	gwmsResponse2 := &p.GetWorkflowExecutionResponse{State: ms2}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse2, nil).Once()

	response, err := s.historyEngine.RecordDecisionTaskStarted(context.Background(), &historyservice.RecordDecisionTaskStartedRequest{
		DomainUUID:        domainID,
		WorkflowExecution: &workflowExecution,
		ScheduleId:        2,
		TaskId:            100,
		RequestId:         requestID,
		PollRequest: &workflowservice.PollForDecisionTaskRequest{
			TaskList: &commonproto.TaskList{
				Name: tl,
			},
			Identity: identity,
		},
	})

	s.Nil(err)
	s.NotNil(response)
	s.Equal("wType", response.WorkflowType.Name)
	s.True(response.PreviousStartedEventId == 0)
	s.Equal(startedEventID.EventId, response.StartedEventId)
}

func (s *engine2Suite) TestRecordDecisionTaskRetryDifferentRequest() {
	domainID := testDomainID
	workflowExecution := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}

	tl := "testTaskList"
	identity := "testIdentity"
	requestID := "testRecordDecisionTaskRetrySameRequestID"

	msBuilder := s.createExecutionStartedState(workflowExecution, tl, identity, false)
	ms := createMutableState(msBuilder)
	gwmsResponse := &p.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil, &p.ConditionFailedError{}).Once()

	// Add event.
	addDecisionTaskStartedEventWithRequestID(msBuilder, int64(2), "some_other_req", tl, identity)
	ms2 := createMutableState(msBuilder)
	gwmsResponse2 := &p.GetWorkflowExecutionResponse{State: ms2}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse2, nil).Once()

	response, err := s.historyEngine.RecordDecisionTaskStarted(context.Background(), &historyservice.RecordDecisionTaskStartedRequest{
		DomainUUID:        domainID,
		WorkflowExecution: &workflowExecution,
		ScheduleId:        2,
		TaskId:            100,
		RequestId:         requestID,
		PollRequest: &workflowservice.PollForDecisionTaskRequest{
			TaskList: &commonproto.TaskList{
				Name: tl,
			},
			Identity: identity,
		},
	})

	s.Nil(response)
	s.NotNil(err)
	s.IsType(&serviceerror.EventAlreadyStarted{}, err)
	s.logger.Info("Failed with error", tag.Error(err))
}

func (s *engine2Suite) TestRecordDecisionTaskStartedMaxAttemptsExceeded() {
	domainID := testDomainID
	workflowExecution := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}

	tl := "testTaskList"
	identity := "testIdentity"

	msBuilder := s.createExecutionStartedState(workflowExecution, tl, identity, false)
	for i := 0; i < conditionalRetryCount; i++ {
		ms := createMutableState(msBuilder)
		gwmsResponse := &p.GetWorkflowExecutionResponse{State: ms}

		s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	}

	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Times(
		conditionalRetryCount)
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil,
		&p.ConditionFailedError{}).Times(conditionalRetryCount)

	response, err := s.historyEngine.RecordDecisionTaskStarted(context.Background(), &historyservice.RecordDecisionTaskStartedRequest{
		DomainUUID:        domainID,
		WorkflowExecution: &workflowExecution,
		ScheduleId:        2,
		TaskId:            100,
		RequestId:         "reqId",
		PollRequest: &workflowservice.PollForDecisionTaskRequest{
			TaskList: &commonproto.TaskList{
				Name: tl,
			},
			Identity: identity,
		},
	})

	s.NotNil(err)
	s.Nil(response)
	s.Equal(ErrMaxAttemptsExceeded, err)
}

func (s *engine2Suite) TestRecordDecisionTaskSuccess() {
	domainID := testDomainID
	workflowExecution := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}

	tl := "testTaskList"
	identity := "testIdentity"

	msBuilder := s.createExecutionStartedState(workflowExecution, tl, identity, false)
	ms := createMutableState(msBuilder)
	gwmsResponse := &p.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{
		MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{},
	}, nil).Once()

	// load mutable state such that it already exists in memory when respond decision task is called
	// this enables us to set query registry on it
	ctx, release, err := s.historyEngine.historyCache.getOrCreateWorkflowExecutionForBackground(testDomainID, workflowExecution)
	s.NoError(err)
	loadedMS, err := ctx.loadWorkflowExecution()
	s.NoError(err)
	qr := newQueryRegistry()
	id1, _ := qr.bufferQuery(&commonproto.WorkflowQuery{})
	id2, _ := qr.bufferQuery(&commonproto.WorkflowQuery{})
	id3, _ := qr.bufferQuery(&commonproto.WorkflowQuery{})
	loadedMS.(*mutableStateBuilder).queryRegistry = qr
	release(nil)

	response, err := s.historyEngine.RecordDecisionTaskStarted(context.Background(), &historyservice.RecordDecisionTaskStartedRequest{
		DomainUUID:        domainID,
		WorkflowExecution: &workflowExecution,
		ScheduleId:        2,
		TaskId:            100,
		RequestId:         "reqId",
		PollRequest: &workflowservice.PollForDecisionTaskRequest{
			TaskList: &commonproto.TaskList{
				Name: tl,
			},
			Identity: identity,
		},
	})

	s.Nil(err)
	s.NotNil(response)
	s.Equal("wType", response.WorkflowType.Name)
	s.True(response.PreviousStartedEventId == 0)
	s.Equal(int64(3), response.StartedEventId)
	expectedQueryMap := map[string]*commonproto.WorkflowQuery{
		id1: {},
		id2: {},
		id3: {},
	}
	s.Equal(expectedQueryMap, response.Queries)
}

func (s *engine2Suite) TestRecordActivityTaskStartedIfNoExecution() {
	domainID := testDomainID
	workflowExecution := &commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}

	identity := "testIdentity"
	tl := "testTaskList"

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, serviceerror.NewNotFound("")).Once()

	response, err := s.historyEngine.RecordActivityTaskStarted(context.Background(), &historyservice.RecordActivityTaskStartedRequest{
		DomainUUID:        domainID,
		WorkflowExecution: workflowExecution,
		ScheduleId:        5,
		TaskId:            100,
		RequestId:         "reqId",
		PollRequest: &workflowservice.PollForActivityTaskRequest{
			TaskList: &commonproto.TaskList{
				Name: tl,
			},
			Identity: identity,
		},
	})
	if err != nil {
		s.logger.Error("Unexpected Error", tag.Error(err))
	}
	s.Nil(response)
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *engine2Suite) TestRecordActivityTaskStartedSuccess() {
	domainID := testDomainID
	workflowExecution := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}

	identity := "testIdentity"
	tl := "testTaskList"

	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	msBuilder := s.createExecutionStartedState(workflowExecution, tl, identity, true)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, int64(2), int64(3), nil, identity)
	scheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 5)

	ms1 := createMutableState(msBuilder)
	gwmsResponse1 := &p.GetWorkflowExecutionResponse{State: ms1}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse1, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{
		MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{},
	}, nil).Once()

	s.mockEventsCache.EXPECT().getEvent(
		domainID, workflowExecution.GetWorkflowId(), workflowExecution.GetRunId(),
		decisionCompletedEvent.GetEventId(), scheduledEvent.GetEventId(), gomock.Any(),
	).Return(scheduledEvent, nil)
	response, err := s.historyEngine.RecordActivityTaskStarted(context.Background(), &historyservice.RecordActivityTaskStartedRequest{
		DomainUUID:        domainID,
		WorkflowExecution: &workflowExecution,
		ScheduleId:        5,
		TaskId:            100,
		RequestId:         "reqId",
		PollRequest: &workflowservice.PollForActivityTaskRequest{
			TaskList: &commonproto.TaskList{
				Name: tl,
			},
			Identity: identity,
		},
	})
	s.Nil(err)
	s.NotNil(response)
	s.Equal(scheduledEvent, response.ScheduledEvent)
}

func (s *engine2Suite) TestRequestCancelWorkflowExecutionSuccess() {
	domainID := testDomainID
	workflowExecution := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}

	identity := "testIdentity"
	tl := "testTaskList"

	msBuilder := s.createExecutionStartedState(workflowExecution, tl, identity, false)
	ms1 := createMutableState(msBuilder)
	gwmsResponse1 := &p.GetWorkflowExecutionResponse{State: ms1}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse1, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{
		MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{},
	}, nil).Once()

	err := s.historyEngine.RequestCancelWorkflowExecution(context.Background(), &historyservice.RequestCancelWorkflowExecutionRequest{
		DomainUUID: domainID,
		CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{
			WorkflowExecution: &commonproto.WorkflowExecution{
				WorkflowId: workflowExecution.WorkflowId,
				RunId:      workflowExecution.RunId,
			},
			Identity: "identity",
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(domainID, workflowExecution)
	s.Equal(int64(4), executionBuilder.GetNextEventID())
}

func (s *engine2Suite) TestRequestCancelWorkflowExecutionFail() {
	domainID := testDomainID
	workflowExecution := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}

	identity := "testIdentity"
	tl := "testTaskList"

	msBuilder := s.createExecutionStartedState(workflowExecution, tl, identity, false)
	msBuilder.GetExecutionInfo().State = p.WorkflowStateCompleted
	ms1 := createMutableState(msBuilder)
	gwmsResponse1 := &p.GetWorkflowExecutionResponse{State: ms1}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse1, nil).Once()

	err := s.historyEngine.RequestCancelWorkflowExecution(context.Background(), &historyservice.RequestCancelWorkflowExecutionRequest{
		DomainUUID: domainID,
		CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{
			WorkflowExecution: &commonproto.WorkflowExecution{
				WorkflowId: workflowExecution.WorkflowId,
				RunId:      workflowExecution.RunId,
			},
			Identity: "identity",
		},
	})
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *engine2Suite) createExecutionStartedState(we commonproto.WorkflowExecution, tl, identity string,
	startDecision bool) mutableState {
	msBuilder := newMutableStateBuilderWithEventV2(s.historyEngine.shard, s.mockEventsCache,
		s.logger, we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	if startDecision {
		addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	}
	_ = msBuilder.SetHistoryTree(primitives.MustParseUUID(we.GetRunId()))

	return msBuilder
}

func (s *engine2Suite) printHistory(builder mutableState) string {
	return builder.GetHistoryBuilder().GetHistory().String()
}

func (s *engine2Suite) TestRespondDecisionTaskCompletedRecordMarkerDecision() {
	domainID := testDomainID
	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskList"
	taskToken := &token.Task{
		WorkflowId: "wId",
		RunId:      primitives.MustParseUUID(we.GetRunId()),
		ScheduleId: 2,
	}
	serializedTaskToken, _ := taskToken.Marshal()
	identity := "testIdentity"
	markerDetails := []byte("marker details")
	markerName := "marker name"

	msBuilder := newMutableStateBuilderWithEventV2(s.historyEngine.shard, s.mockEventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	decisions := []*commonproto.Decision{{
		DecisionType: enums.DecisionTypeRecordMarker,
		Attributes: &commonproto.Decision_RecordMarkerDecisionAttributes{RecordMarkerDecisionAttributes: &commonproto.RecordMarkerDecisionAttributes{
			MarkerName: markerName,
			Details:    markerDetails,
		}},
	}}

	ms := createMutableState(msBuilder)
	gwmsResponse := &p.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{
		MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{},
	}, nil).Once()

	_, err := s.historyEngine.RespondDecisionTaskCompleted(context.Background(), &historyservice.RespondDecisionTaskCompletedRequest{
		DomainUUID: domainID,
		CompleteRequest: &workflowservice.RespondDecisionTaskCompletedRequest{
			TaskToken:        serializedTaskToken,
			Decisions:        decisions,
			ExecutionContext: nil,
			Identity:         identity,
		},
	})
	s.Nil(err)
	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(6), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(p.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecision())
}

func (s *engine2Suite) TestStartWorkflowExecution_BrandNew() {
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

func (s *engine2Suite) TestStartWorkflowExecution_StillRunning_Dedup() {
	domainID := testDomainID
	workflowID := "workflowID"
	runID := "runID"
	workflowType := "workflowType"
	taskList := "testTaskList"
	identity := "testIdentity"
	requestID := "requestID"
	lastWriteVersion := common.EmptyVersion

	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(nil, &p.WorkflowExecutionAlreadyStartedError{
		Msg:              "random message",
		StartRequestID:   requestID,
		RunID:            runID,
		State:            p.WorkflowStateRunning,
		CloseStatus:      p.WorkflowCloseStatusRunning,
		LastWriteVersion: lastWriteVersion,
	}).Once()

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
	s.Equal(runID, resp.GetRunId())
}

func (s *engine2Suite) TestStartWorkflowExecution_StillRunning_NonDeDup() {
	domainID := testDomainID
	workflowID := "workflowID"
	runID := "runID"
	workflowType := "workflowType"
	taskList := "testTaskList"
	identity := "testIdentity"
	lastWriteVersion := common.EmptyVersion

	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(nil, &p.WorkflowExecutionAlreadyStartedError{
		Msg:              "random message",
		StartRequestID:   "oldRequestID",
		RunID:            runID,
		State:            p.WorkflowStateRunning,
		CloseStatus:      p.WorkflowCloseStatusRunning,
		LastWriteVersion: lastWriteVersion,
	}).Once()

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
			RequestId:                           "newRequestID",
		},
	})
	if _, ok := err.(*serviceerror.WorkflowExecutionAlreadyStarted); !ok {
		s.Fail("return err is not *serviceerror.WorkflowExecutionAlreadyStarted")
	}
	s.Nil(resp)
}

func (s *engine2Suite) TestStartWorkflowExecution_NotRunning_PrevSuccess() {
	domainID := testDomainID
	workflowID := "workflowID"
	runID := "runID"
	workflowType := "workflowType"
	taskList := "testTaskList"
	identity := "testIdentity"
	lastWriteVersion := common.EmptyVersion

	options := []enums.WorkflowIdReusePolicy{
		enums.WorkflowIdReusePolicyAllowDuplicateFailedOnly,
		enums.WorkflowIdReusePolicyAllowDuplicate,
		enums.WorkflowIdReusePolicyRejectDuplicate,
	}

	expecedErrs := []bool{true, false, true}

	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Times(len(expecedErrs))
	s.mockExecutionMgr.On(
		"CreateWorkflowExecution",
		mock.MatchedBy(func(request *p.CreateWorkflowExecutionRequest) bool {
			return request.Mode == p.CreateWorkflowModeBrandNew
		}),
	).Return(nil, &p.WorkflowExecutionAlreadyStartedError{
		Msg:              "random message",
		StartRequestID:   "oldRequestID",
		RunID:            runID,
		State:            p.WorkflowStateCompleted,
		CloseStatus:      p.WorkflowCloseStatusCompleted,
		LastWriteVersion: lastWriteVersion,
	}).Times(len(expecedErrs))

	for index, option := range options {
		if !expecedErrs[index] {
			s.mockExecutionMgr.On(
				"CreateWorkflowExecution",
				mock.MatchedBy(func(request *p.CreateWorkflowExecutionRequest) bool {
					return request.Mode == p.CreateWorkflowModeWorkflowIDReuse &&
						request.PreviousRunID == runID &&
						request.PreviousLastWriteVersion == lastWriteVersion
				}),
			).Return(&p.CreateWorkflowExecutionResponse{}, nil).Once()
		}

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
				RequestId:                           "newRequestID",
				WorkflowIdReusePolicy:               option,
			},
		})

		if expecedErrs[index] {
			if _, ok := err.(*serviceerror.WorkflowExecutionAlreadyStarted); !ok {
				s.Fail("return err is not *serviceerror.WorkflowExecutionAlreadyStarted")
			}
			s.Nil(resp)
		} else {
			s.Nil(err)
			s.NotNil(resp)
		}
	}
}

func (s *engine2Suite) TestStartWorkflowExecution_NotRunning_PrevFail() {
	domainID := testDomainID
	workflowID := "workflowID"
	workflowType := "workflowType"
	taskList := "testTaskList"
	identity := "testIdentity"
	lastWriteVersion := common.EmptyVersion

	options := []enums.WorkflowIdReusePolicy{
		enums.WorkflowIdReusePolicyAllowDuplicateFailedOnly,
		enums.WorkflowIdReusePolicyAllowDuplicate,
		enums.WorkflowIdReusePolicyRejectDuplicate,
	}

	expecedErrs := []bool{false, false, true}

	closeStates := []int{
		p.WorkflowCloseStatusFailed,
		p.WorkflowCloseStatusCanceled,
		p.WorkflowCloseStatusTerminated,
		p.WorkflowCloseStatusTimedOut,
	}
	runIDs := []string{"1", "2", "3", "4"}

	for i, closeState := range closeStates {

		s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Times(len(expecedErrs))
		s.mockExecutionMgr.On(
			"CreateWorkflowExecution",
			mock.MatchedBy(func(request *p.CreateWorkflowExecutionRequest) bool {
				return request.Mode == p.CreateWorkflowModeBrandNew
			}),
		).Return(nil, &p.WorkflowExecutionAlreadyStartedError{
			Msg:              "random message",
			StartRequestID:   "oldRequestID",
			RunID:            runIDs[i],
			State:            p.WorkflowStateCompleted,
			CloseStatus:      enums.WorkflowExecutionCloseStatus(closeState),
			LastWriteVersion: lastWriteVersion,
		}).Times(len(expecedErrs))

		for j, option := range options {

			if !expecedErrs[j] {
				s.mockExecutionMgr.On(
					"CreateWorkflowExecution",
					mock.MatchedBy(func(request *p.CreateWorkflowExecutionRequest) bool {
						return request.Mode == p.CreateWorkflowModeWorkflowIDReuse &&
							request.PreviousRunID == runIDs[i] &&
							request.PreviousLastWriteVersion == lastWriteVersion
					}),
				).Return(&p.CreateWorkflowExecutionResponse{}, nil).Once()
			}

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
					RequestId:                           "newRequestID",
					WorkflowIdReusePolicy:               option,
				},
			})

			if expecedErrs[j] {
				if _, ok := err.(*serviceerror.WorkflowExecutionAlreadyStarted); !ok {
					s.Fail("return err is not *serviceerror.WorkflowExecutionAlreadyStarted")
				}
				s.Nil(resp)
			} else {
				s.Nil(err)
				s.NotNil(resp)
			}
		}
	}
}

func (s *engine2Suite) TestSignalWithStartWorkflowExecution_JustSignal() {
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

func (s *engine2Suite) TestSignalWithStartWorkflowExecution_WorkflowNotExist() {
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

func (s *engine2Suite) TestSignalWithStartWorkflowExecution_CreateTimeout() {
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
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(nil, &p.TimeoutError{}).Once()

	resp, err := s.historyEngine.SignalWithStartWorkflowExecution(context.Background(), sRequest)
	s.True(p.IsTimeoutError(err))
	s.NotNil(resp.GetRunId())
}

func (s *engine2Suite) TestSignalWithStartWorkflowExecution_WorkflowNotRunning() {
	sRequest := &historyservice.SignalWithStartWorkflowExecutionRequest{}
	_, err := s.historyEngine.SignalWithStartWorkflowExecution(context.Background(), sRequest)
	s.EqualError(err, "Missing domain UUID.")

	domainID := testDomainID
	workflowID := "wId"
	runID := testRunID
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
			Input:                               input,
			ExecutionStartToCloseTimeoutSeconds: 1,
			TaskStartToCloseTimeoutSeconds:      2,
			Identity:                            identity,
			RequestId:                           requestID,
			WorkflowIdReusePolicy:               enums.WorkflowIdReusePolicyAllowDuplicate,
			SignalName:                          signalName,
			SignalInput:                         nil,
			Control:                             nil,
			RetryPolicy:                         nil,
			CronSchedule:                        "",
			Memo:                                nil,
			SearchAttributes:                    nil,
			Header:                              nil,
		},
	}

	msBuilder := newMutableStateBuilderWithEventV2(s.historyEngine.shard, s.mockEventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), runID)
	ms := createMutableState(msBuilder)
	ms.ExecutionInfo.State = p.WorkflowStateCompleted
	gwmsResponse := &p.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &p.GetCurrentExecutionResponse{RunID: runID}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(&p.CreateWorkflowExecutionResponse{}, nil).Once()

	resp, err := s.historyEngine.SignalWithStartWorkflowExecution(context.Background(), sRequest)
	s.Nil(err)
	s.NotNil(resp.GetRunId())
	s.NotEqual(runID, resp.GetRunId())
}

func (s *engine2Suite) TestSignalWithStartWorkflowExecution_Start_DuplicateRequests() {
	domainID := testDomainID
	workflowID := "wId"
	runID := testRunID
	workflowType := "workflowType"
	taskList := "testTaskList"
	identity := "testIdentity"
	signalName := "my signal name"
	input := []byte("test input")
	requestID := "testRequestID"
	sRequest := &historyservice.SignalWithStartWorkflowExecutionRequest{
		DomainUUID: domainID,
		SignalWithStartRequest: &workflowservice.SignalWithStartWorkflowExecutionRequest{
			Domain:                              domainID,
			WorkflowId:                          workflowID,
			WorkflowType:                        &commonproto.WorkflowType{Name: workflowType},
			TaskList:                            &commonproto.TaskList{Name: taskList},
			Input:                               input,
			ExecutionStartToCloseTimeoutSeconds: 1,
			TaskStartToCloseTimeoutSeconds:      2,
			Identity:                            identity,
			RequestId:                           requestID,
			WorkflowIdReusePolicy:               enums.WorkflowIdReusePolicyAllowDuplicate,
			SignalName:                          signalName,
			SignalInput:                         nil,
			Control:                             nil,
			RetryPolicy:                         nil,
			CronSchedule:                        "",
			Memo:                                nil,
			SearchAttributes:                    nil,
			Header:                              nil,
		},
	}

	msBuilder := newMutableStateBuilderWithEventV2(s.historyEngine.shard, s.mockEventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), runID)
	ms := createMutableState(msBuilder)
	ms.ExecutionInfo.State = p.WorkflowStateCompleted
	gwmsResponse := &p.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &p.GetCurrentExecutionResponse{RunID: runID}
	workflowAlreadyStartedErr := &p.WorkflowExecutionAlreadyStartedError{
		Msg:              "random message",
		StartRequestID:   requestID, // use same requestID
		RunID:            runID,
		State:            p.WorkflowStateRunning,
		CloseStatus:      p.WorkflowCloseStatusRunning,
		LastWriteVersion: common.EmptyVersion,
	}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(nil, workflowAlreadyStartedErr).Once()

	resp, err := s.historyEngine.SignalWithStartWorkflowExecution(context.Background(), sRequest)
	s.Nil(err)
	s.NotNil(resp.GetRunId())
	s.Equal(runID, resp.GetRunId())
}

func (s *engine2Suite) TestSignalWithStartWorkflowExecution_Start_WorkflowAlreadyStarted() {
	domainID := testDomainID
	workflowID := "wId"
	runID := testRunID
	workflowType := "workflowType"
	taskList := "testTaskList"
	identity := "testIdentity"
	signalName := "my signal name"
	input := []byte("test input")
	requestID := "testRequestID"
	sRequest := &historyservice.SignalWithStartWorkflowExecutionRequest{
		DomainUUID: domainID,
		SignalWithStartRequest: &workflowservice.SignalWithStartWorkflowExecutionRequest{
			Domain:                              domainID,
			WorkflowId:                          workflowID,
			WorkflowType:                        &commonproto.WorkflowType{Name: workflowType},
			TaskList:                            &commonproto.TaskList{Name: taskList},
			Input:                               input,
			ExecutionStartToCloseTimeoutSeconds: 1,
			TaskStartToCloseTimeoutSeconds:      2,
			Identity:                            identity,
			RequestId:                           requestID,
			WorkflowIdReusePolicy:               enums.WorkflowIdReusePolicyAllowDuplicate,
			SignalName:                          signalName,
			SignalInput:                         nil,
			Control:                             nil,
			RetryPolicy:                         nil,
			CronSchedule:                        "",
			Memo:                                nil,
			SearchAttributes:                    nil,
			Header:                              nil,
		},
	}

	msBuilder := newMutableStateBuilderWithEventV2(s.historyEngine.shard, s.mockEventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), runID)
	ms := createMutableState(msBuilder)
	ms.ExecutionInfo.State = p.WorkflowStateCompleted
	gwmsResponse := &p.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &p.GetCurrentExecutionResponse{RunID: runID}
	workflowAlreadyStartedErr := &p.WorkflowExecutionAlreadyStartedError{
		Msg:              "random message",
		StartRequestID:   "new request ID",
		RunID:            runID,
		State:            p.WorkflowStateRunning,
		CloseStatus:      p.WorkflowCloseStatusRunning,
		LastWriteVersion: common.EmptyVersion,
	}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(nil, workflowAlreadyStartedErr).Once()

	resp, err := s.historyEngine.SignalWithStartWorkflowExecution(context.Background(), sRequest)
	s.Nil(resp)
	s.NotNil(err)
}

func (s *engine2Suite) getBuilder(domainID string, we commonproto.WorkflowExecution) mutableState {
	weContext, release, err := s.historyEngine.historyCache.getOrCreateWorkflowExecutionForBackground(domainID, we)
	if err != nil {
		return nil
	}
	defer release(nil)

	return weContext.(*workflowExecutionContextImpl).mutableState
}
