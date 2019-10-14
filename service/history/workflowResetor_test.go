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
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	h "github.com/uber/cadence/.gen/go/history"
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
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
	"github.com/uber/cadence/service/worker/archiver"
	"go.uber.org/cadence/.gen/go/shared"
)

type (
	resetorSuite struct {
		suite.Suite
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		historyEngine            *historyEngineImpl
		controller               *gomock.Controller
		mockMatchingClient       *matchingservicetest.MockClient
		mockHistoryClient        *historyservicetest.MockClient
		mockVisibilityMgr        *mocks.VisibilityManager
		mockExecutionMgr         *mocks.ExecutionManager
		mockHistoryV2Mgr         *mocks.HistoryV2Manager
		mockShardManager         *mocks.ShardManager
		mockClusterMetadata      *mocks.ClusterMetadata
		mockProducer             *mocks.KafkaProducer
		mockMessagingClient      messaging.Client
		mockService              service.Service
		mockDomainCache          *cache.DomainCacheMock
		mockArchivalClient       *archiver.ClientMock
		mockClientBean           *client.MockClientBean
		mockEventsCache          *MockEventsCache
		resetor                  workflowResetor
		mockTxProcessor          *MockTransferQueueProcessor
		mockReplicationProcessor *MockReplicatorQueueProcessor
		mockTimerProcessor       *MockTimerQueueProcessor

		shardClosedCh chan int
		config        *Config
		logger        log.Logger
		shardID       int
	}
)

func TestWorkflowResetorSuite(t *testing.T) {
	s := new(resetorSuite)
	suite.Run(t, s)
}

func (s *resetorSuite) SetupSuite() {

	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)
	s.config = NewDynamicConfigForTest()
}

func (s *resetorSuite) TearDownSuite() {
}

func (s *resetorSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())

	shardID := 10
	s.shardID = shardID
	s.controller = gomock.NewController(s.T())
	s.mockMatchingClient = matchingservicetest.NewMockClient(s.controller)
	s.mockHistoryClient = historyservicetest.NewMockClient(s.controller)
	s.mockVisibilityMgr = &mocks.VisibilityManager{}
	s.mockExecutionMgr = &mocks.ExecutionManager{}
	s.mockHistoryV2Mgr = &mocks.HistoryV2Manager{}
	s.mockShardManager = &mocks.ShardManager{}
	s.mockClusterMetadata = &mocks.ClusterMetadata{}
	s.mockProducer = &mocks.KafkaProducer{}
	s.shardClosedCh = make(chan int, 100)
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History)
	s.mockMessagingClient = mocks.NewMockMessagingClient(s.mockProducer, nil)
	s.mockClientBean = &client.MockClientBean{}
	s.mockService = service.NewTestService(s.mockClusterMetadata, s.mockMessagingClient, metricsClient, s.mockClientBean, nil, nil, nil)
	s.mockDomainCache = &cache.DomainCacheMock{}
	s.mockArchivalClient = &archiver.ClientMock{}
	s.mockEventsCache = &MockEventsCache{}

	mockShard := &shardContextImpl{
		service:                   s.mockService,
		shardInfo:                 &p.ShardInfo{ShardID: shardID, RangeID: 1, TransferAckLevel: 0},
		shardID:                   shardID,
		transferSequenceNumber:    1,
		executionManager:          s.mockExecutionMgr,
		historyV2Mgr:              s.mockHistoryV2Mgr,
		domainCache:               s.mockDomainCache,
		eventsCache:               s.mockEventsCache,
		clusterMetadata:           s.mockClusterMetadata,
		shardManager:              s.mockShardManager,
		maxTransferSequenceNumber: 100000,
		closeCh:                   s.shardClosedCh,
		config:                    s.config,
		logger:                    s.logger,
		metricsClient:             metrics.NewClient(tally.NoopScope, metrics.History),
		standbyClusterCurrentTime: map[string]time.Time{},
		timeSource:                clock.NewRealTimeSource(),
	}
	s.mockClusterMetadata.On("IsGlobalDomainEnabled").Return(true)
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", common.EmptyVersion).Return(cluster.TestCurrentClusterName)
	s.mockTxProcessor = &MockTransferQueueProcessor{}
	s.mockTxProcessor.On("NotifyNewTask", mock.Anything, mock.Anything).Maybe()
	s.mockReplicationProcessor = NewMockReplicatorQueueProcessor(s.controller)
	s.mockReplicationProcessor.EXPECT().notifyNewTask().AnyTimes()
	s.mockTimerProcessor = &MockTimerQueueProcessor{}
	s.mockTimerProcessor.On("NotifyNewTimers", mock.Anything, mock.Anything).Maybe()

	historyCache := newHistoryCache(mockShard)
	h := &historyEngineImpl{
		currentClusterName:   mockShard.GetService().GetClusterMetadata().GetCurrentClusterName(),
		shard:                mockShard,
		clusterMetadata:      s.mockClusterMetadata,
		executionManager:     s.mockExecutionMgr,
		historyV2Mgr:         s.mockHistoryV2Mgr,
		historyCache:         historyCache,
		logger:               s.logger,
		metricsClient:        metrics.NewClient(tally.NoopScope, metrics.History),
		tokenSerializer:      common.NewJSONTaskTokenSerializer(),
		config:               s.config,
		archivalClient:       s.mockArchivalClient,
		historyEventNotifier: newHistoryEventNotifier(clock.NewRealTimeSource(), metrics.NewClient(tally.NoopScope, metrics.History), func(string) int { return 0 }),
		txProcessor:          s.mockTxProcessor,
		replicatorProcessor:  s.mockReplicationProcessor,
		timerProcessor:       s.mockTimerProcessor,
	}
	mockShard.SetEngine(h)
	s.resetor = newWorkflowResetor(h)
	h.resetor = s.resetor
	s.historyEngine = h
}

func (s *resetorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockHistoryV2Mgr.AssertExpectations(s.T())
	s.mockShardManager.AssertExpectations(s.T())
	s.mockVisibilityMgr.AssertExpectations(s.T())
	s.mockProducer.AssertExpectations(s.T())
	s.mockClientBean.AssertExpectations(s.T())
	s.mockArchivalClient.AssertExpectations(s.T())
	s.mockEventsCache.AssertExpectations(s.T())
	s.mockTxProcessor.AssertExpectations(s.T())
	s.mockTimerProcessor.AssertExpectations(s.T())
}

func (s *resetorSuite) TestResetWorkflowExecution_NoReplication() {
	testDomainEntry := cache.NewLocalDomainCacheEntryForTest(
		&p.DomainInfo{ID: testDomainID}, &p.DomainConfig{Retention: 1}, "", nil,
	)
	s.mockDomainCache.On("GetDomainByID", mock.Anything).Return(testDomainEntry, nil)
	s.mockDomainCache.On("GetDomain", mock.Anything).Return(testDomainEntry, nil)
	s.mockEventsCache.On("putEvent", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return().Once()

	request := &h.ResetWorkflowExecutionRequest{}
	domainID := testDomainID
	request.DomainUUID = &domainID
	request.ResetRequest = &workflow.ResetWorkflowExecutionRequest{}

	wid := "wId"
	wfType := "wfType"
	taskListName := "taskList"
	forkRunID := uuid.New().String()
	currRunID := uuid.New().String()
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(wid),
		RunId:      common.StringPtr(forkRunID),
	}
	request.ResetRequest = &workflow.ResetWorkflowExecutionRequest{
		Domain:                common.StringPtr("testDomainName"),
		WorkflowExecution:     &we,
		Reason:                common.StringPtr("test reset"),
		DecisionFinishEventId: common.Int64Ptr(29),
		RequestId:             common.StringPtr(uuid.New().String()),
	}

	forkGwmsRequest := &p.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(wid),
			RunId:      common.StringPtr(forkRunID),
		},
	}

	timerFiredID := "timerID0"
	timerUnfiredID1 := "timerID1"
	timerUnfiredID2 := "timerID2"
	timerAfterReset := "timerID3"
	actIDCompleted1 := "actID0"
	actIDCompleted2 := "actID1"
	actIDStarted1 := "actID2"
	actIDNotStarted := "actID3"
	actIDStarted2 := "actID4"
	signalName1 :=
		"sig1"
	signalName2 := "sig2"
	forkBranchToken := []byte("forkBranchToken")
	forkExeInfo := &p.WorkflowExecutionInfo{
		DomainID:           domainID,
		WorkflowID:         wid,
		WorkflowTypeName:   wfType,
		TaskList:           taskListName,
		RunID:              forkRunID,
		BranchToken:        forkBranchToken,
		NextEventID:        34,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
	}
	forkGwmsResponse := &p.GetWorkflowExecutionResponse{State: &p.WorkflowMutableState{
		ExecutionInfo:  forkExeInfo,
		ExecutionStats: &p.ExecutionStats{},
	}}

	currGwmsRequest := &p.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(wid),
			RunId:      common.StringPtr(currRunID),
		},
	}
	currExeInfo := &p.WorkflowExecutionInfo{
		DomainID:           domainID,
		WorkflowID:         wid,
		WorkflowTypeName:   wfType,
		TaskList:           taskListName,
		RunID:              currRunID,
		NextEventID:        common.FirstEventID,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
	}
	compareCurrExeInfo := copyWorkflowExecutionInfo(currExeInfo)
	currGwmsResponse := &p.GetWorkflowExecutionResponse{State: &p.WorkflowMutableState{
		ExecutionInfo:  currExeInfo,
		ExecutionStats: &p.ExecutionStats{},
	}}

	gcurResponse := &p.GetCurrentExecutionResponse{
		RunID: currRunID,
	}

	readHistoryReq := &p.ReadHistoryBranchRequest{
		BranchToken:   forkBranchToken,
		MinEventID:    common.FirstEventID,
		MaxEventID:    int64(34),
		PageSize:      defaultHistoryPageSize,
		NextPageToken: nil,
		ShardID:       common.IntPtr(s.shardID),
	}

	taskList := &workflow.TaskList{
		Name: common.StringPtr(taskListName),
	}
	readHistoryResp := &p.ReadHistoryBranchByBatchResponse{
		NextPageToken:    nil,
		Size:             1000,
		LastFirstEventID: int64(31),
		History: []*workflow.History{
			&workflow.History{
				Events: []*workflow.HistoryEvent{
					&workflow.HistoryEvent{
						EventId:   common.Int64Ptr(1),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeWorkflowExecutionStarted),
						WorkflowExecutionStartedEventAttributes: &workflow.WorkflowExecutionStartedEventAttributes{
							WorkflowType: &workflow.WorkflowType{
								Name: common.StringPtr(wfType),
							},
							TaskList:                            taskList,
							Input:                               []byte("testInput"),
							ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
							TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(200),
						},
					},
					{
						EventId:   common.Int64Ptr(2),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskScheduled),
						DecisionTaskScheduledEventAttributes: &workflow.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: common.Int32Ptr(100),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(3),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskStarted),
						DecisionTaskStartedEventAttributes: &workflow.DecisionTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(2),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(4),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskCompleted),
						DecisionTaskCompletedEventAttributes: &workflow.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: common.Int64Ptr(2),
							StartedEventId:   common.Int64Ptr(3),
						},
					},
					{
						EventId:   common.Int64Ptr(5),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeMarkerRecorded),
						MarkerRecordedEventAttributes: &workflow.MarkerRecordedEventAttributes{
							MarkerName:                   common.StringPtr("Version"),
							Details:                      []byte("details"),
							DecisionTaskCompletedEventId: common.Int64Ptr(4),
						},
					},
					{
						EventId:   common.Int64Ptr(6),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskScheduled),
						ActivityTaskScheduledEventAttributes: &workflow.ActivityTaskScheduledEventAttributes{
							ActivityId: common.StringPtr(actIDCompleted1),
							ActivityType: &workflow.ActivityType{
								Name: common.StringPtr("actType0"),
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1000),
							ScheduleToStartTimeoutSeconds: common.Int32Ptr(2000),
							StartToCloseTimeoutSeconds:    common.Int32Ptr(3000),
							HeartbeatTimeoutSeconds:       common.Int32Ptr(4000),
							DecisionTaskCompletedEventId:  common.Int64Ptr(4),
						},
					},
					{
						EventId:   common.Int64Ptr(7),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeTimerStarted),
						TimerStartedEventAttributes: &workflow.TimerStartedEventAttributes{
							TimerId:                      common.StringPtr(timerFiredID),
							StartToFireTimeoutSeconds:    common.Int64Ptr(2),
							DecisionTaskCompletedEventId: common.Int64Ptr(4),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(8),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskStarted),
						ActivityTaskStartedEventAttributes: &workflow.ActivityTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(6),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(9),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskCompleted),
						ActivityTaskCompletedEventAttributes: &workflow.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: common.Int64Ptr(6),
							StartedEventId:   common.Int64Ptr(8),
						},
					},
					{
						EventId:   common.Int64Ptr(10),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskScheduled),
						DecisionTaskScheduledEventAttributes: &workflow.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: common.Int32Ptr(100),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(11),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskStarted),
						DecisionTaskStartedEventAttributes: &workflow.DecisionTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(10),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(12),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskCompleted),
						DecisionTaskCompletedEventAttributes: &workflow.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: common.Int64Ptr(10),
							StartedEventId:   common.Int64Ptr(11),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(13),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeTimerFired),
						TimerFiredEventAttributes: &workflow.TimerFiredEventAttributes{
							TimerId: common.StringPtr(timerFiredID),
						},
					},
					{
						EventId:   common.Int64Ptr(14),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskScheduled),
						DecisionTaskScheduledEventAttributes: &workflow.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: common.Int32Ptr(100),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(15),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskStarted),
						DecisionTaskStartedEventAttributes: &workflow.DecisionTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(14),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(16),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskCompleted),
						DecisionTaskCompletedEventAttributes: &workflow.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: common.Int64Ptr(14),
							StartedEventId:   common.Int64Ptr(15),
						},
					},
					{
						EventId:   common.Int64Ptr(17),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskScheduled),
						ActivityTaskScheduledEventAttributes: &workflow.ActivityTaskScheduledEventAttributes{
							ActivityId: common.StringPtr(actIDStarted1),
							ActivityType: &workflow.ActivityType{
								Name: common.StringPtr("actType1"),
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1000),
							ScheduleToStartTimeoutSeconds: common.Int32Ptr(2000),
							StartToCloseTimeoutSeconds:    common.Int32Ptr(3000),
							HeartbeatTimeoutSeconds:       common.Int32Ptr(4000),
							DecisionTaskCompletedEventId:  common.Int64Ptr(16),
							RetryPolicy: &workflow.RetryPolicy{
								InitialIntervalInSeconds:    common.Int32Ptr(1),
								BackoffCoefficient:          common.Float64Ptr(0.2),
								MaximumAttempts:             common.Int32Ptr(10),
								MaximumIntervalInSeconds:    common.Int32Ptr(1000),
								ExpirationIntervalInSeconds: common.Int32Ptr(math.MaxInt32),
							},
						},
					},
					{
						EventId:   common.Int64Ptr(18),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskScheduled),
						ActivityTaskScheduledEventAttributes: &workflow.ActivityTaskScheduledEventAttributes{
							ActivityId: common.StringPtr(actIDNotStarted),
							ActivityType: &workflow.ActivityType{
								Name: common.StringPtr("actType2"),
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1000),
							ScheduleToStartTimeoutSeconds: common.Int32Ptr(2000),
							StartToCloseTimeoutSeconds:    common.Int32Ptr(3000),
							HeartbeatTimeoutSeconds:       common.Int32Ptr(4000),
							DecisionTaskCompletedEventId:  common.Int64Ptr(16),
						},
					},
					{
						EventId:   common.Int64Ptr(19),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeTimerStarted),
						TimerStartedEventAttributes: &workflow.TimerStartedEventAttributes{
							TimerId:                      common.StringPtr(timerUnfiredID1),
							StartToFireTimeoutSeconds:    common.Int64Ptr(4),
							DecisionTaskCompletedEventId: common.Int64Ptr(16),
						},
					},
					{
						EventId:   common.Int64Ptr(20),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeTimerStarted),
						TimerStartedEventAttributes: &workflow.TimerStartedEventAttributes{
							TimerId:                      common.StringPtr(timerUnfiredID2),
							StartToFireTimeoutSeconds:    common.Int64Ptr(8),
							DecisionTaskCompletedEventId: common.Int64Ptr(16),
						},
					},
					{
						EventId:   common.Int64Ptr(21),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskScheduled),
						ActivityTaskScheduledEventAttributes: &workflow.ActivityTaskScheduledEventAttributes{
							ActivityId: common.StringPtr(actIDCompleted2),
							ActivityType: &workflow.ActivityType{
								Name: common.StringPtr("actType2"),
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1000),
							ScheduleToStartTimeoutSeconds: common.Int32Ptr(2000),
							StartToCloseTimeoutSeconds:    common.Int32Ptr(3000),
							HeartbeatTimeoutSeconds:       common.Int32Ptr(4000),
							DecisionTaskCompletedEventId:  common.Int64Ptr(16),
						},
					},
					{
						EventId:   common.Int64Ptr(22),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskScheduled),
						ActivityTaskScheduledEventAttributes: &workflow.ActivityTaskScheduledEventAttributes{
							ActivityId: common.StringPtr(actIDStarted2),
							ActivityType: &workflow.ActivityType{
								Name: common.StringPtr("actType2"),
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1000),
							ScheduleToStartTimeoutSeconds: common.Int32Ptr(2000),
							StartToCloseTimeoutSeconds:    common.Int32Ptr(3000),
							HeartbeatTimeoutSeconds:       common.Int32Ptr(4000),
							DecisionTaskCompletedEventId:  common.Int64Ptr(16),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(23),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskStarted),
						ActivityTaskStartedEventAttributes: &workflow.ActivityTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(21),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(24),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskStarted),
						ActivityTaskStartedEventAttributes: &workflow.ActivityTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(17),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(25),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskStarted),
						ActivityTaskStartedEventAttributes: &workflow.ActivityTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(22),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(26),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskCompleted),
						ActivityTaskCompletedEventAttributes: &workflow.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: common.Int64Ptr(21),
							StartedEventId:   common.Int64Ptr(23),
						},
					},
					{
						EventId:   common.Int64Ptr(27),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskScheduled),
						DecisionTaskScheduledEventAttributes: &workflow.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: common.Int32Ptr(100),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(28),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskStarted),
						DecisionTaskStartedEventAttributes: &workflow.DecisionTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(27),
						},
					},
				},
			},
			/////////////// reset point/////////////
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(29),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskCompleted),
						DecisionTaskCompletedEventAttributes: &workflow.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: common.Int64Ptr(27),
							StartedEventId:   common.Int64Ptr(28),
						},
					},
					{
						EventId:   common.Int64Ptr(30),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeTimerStarted),
						TimerStartedEventAttributes: &workflow.TimerStartedEventAttributes{
							TimerId:                      common.StringPtr(timerAfterReset),
							StartToFireTimeoutSeconds:    common.Int64Ptr(4),
							DecisionTaskCompletedEventId: common.Int64Ptr(29),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(31),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskStarted),
						ActivityTaskStartedEventAttributes: &workflow.ActivityTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(18),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(32),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeWorkflowExecutionSignaled),
						WorkflowExecutionSignaledEventAttributes: &workflow.WorkflowExecutionSignaledEventAttributes{
							SignalName: common.StringPtr(signalName1),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(33),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeWorkflowExecutionSignaled),
						WorkflowExecutionSignaledEventAttributes: &workflow.WorkflowExecutionSignaledEventAttributes{
							SignalName: common.StringPtr(signalName2),
						},
					},
				},
			},
		},
	}

	eid := int64(0)
	for _, be := range readHistoryResp.History {
		for _, e := range be.Events {
			eid++
			if e.GetEventId() != eid {
				s.Fail(fmt.Sprintf("inconintous eventID: %v, %v", eid, e.GetEventId()))
			}
			e.Timestamp = common.Int64Ptr(1000)
		}
	}

	newBranchToken := []byte("newBranch")
	forkResp := &p.ForkHistoryBranchResponse{
		NewBranchToken: newBranchToken,
	}

	completeReq := &p.CompleteForkBranchRequest{
		BranchToken: newBranchToken,
		Success:     true,
		ShardID:     common.IntPtr(s.shardID),
	}
	completeReqErr := &p.CompleteForkBranchRequest{
		BranchToken: newBranchToken,
		Success:     true,
		ShardID:     common.IntPtr(s.shardID),
	}

	appendV2Resp := &p.AppendHistoryNodesResponse{
		Size: 200,
	}

	s.mockExecutionMgr.On("GetWorkflowExecution", forkGwmsRequest).Return(forkGwmsResponse, nil).Once()
	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gcurResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", currGwmsRequest).Return(currGwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("ReadHistoryBranchByBatch", readHistoryReq).Return(readHistoryResp, nil).Once()
	s.mockHistoryV2Mgr.On("ForkHistoryBranch", mock.Anything).Return(forkResp, nil).Once()
	s.mockHistoryV2Mgr.On("CompleteForkBranch", completeReq).Return(nil).Once()
	s.mockHistoryV2Mgr.On("CompleteForkBranch", completeReqErr).Return(nil).Maybe()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(appendV2Resp, nil).Times(2)
	s.mockExecutionMgr.On("ResetWorkflowExecution", mock.Anything).Return(nil).Once()
	s.mockEventsCache.On("putEvent", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return().Once()
	s.mockClusterMetadata.On("TestResetWorkflowExecution_NoReplication")
	response, err := s.historyEngine.ResetWorkflowExecution(context.Background(), request)
	s.Nil(err)
	s.NotNil(response.RunId)

	// verify historyEvent: 5 events to append
	// 1. decisionFailed :29
	// 2. activityFailed :30
	// 3. signal 1 :31
	// 4. signal 2 :32
	// 5. decisionTaskScheduled :33
	calls := s.mockHistoryV2Mgr.Calls
	s.Equal(5, len(calls))
	appendCall := calls[3]
	s.Equal("AppendHistoryNodes", appendCall.Method)
	appendReq, ok := appendCall.Arguments[0].(*p.AppendHistoryNodesRequest)
	s.Equal(true, ok)
	s.Equal(newBranchToken, appendReq.BranchToken)
	s.Equal(false, appendReq.IsNewBranch)
	s.Equal(6, len(appendReq.Events))
	s.Equal(workflow.EventTypeDecisionTaskFailed, appendReq.Events[0].GetEventType())
	s.Equal(workflow.EventTypeActivityTaskFailed, appendReq.Events[1].GetEventType())
	s.Equal(workflow.EventTypeActivityTaskFailed, appendReq.Events[2].GetEventType())
	s.Equal(workflow.EventTypeWorkflowExecutionSignaled, appendReq.Events[3].GetEventType())
	s.Equal(workflow.EventTypeWorkflowExecutionSignaled, appendReq.Events[4].GetEventType())
	s.Equal(workflow.EventTypeDecisionTaskScheduled, appendReq.Events[5].GetEventType())

	s.Equal(int64(29), appendReq.Events[0].GetEventId())
	s.Equal(int64(30), appendReq.Events[1].GetEventId())
	s.Equal(int64(31), appendReq.Events[2].GetEventId())
	s.Equal(int64(32), appendReq.Events[3].GetEventId())
	s.Equal(int64(33), appendReq.Events[4].GetEventId())
	s.Equal(int64(34), appendReq.Events[5].GetEventId())

	// verify executionManager request
	calls = s.mockExecutionMgr.Calls
	s.Equal(4, len(calls))
	resetCall := calls[3]
	s.Equal("ResetWorkflowExecution", resetCall.Method)
	resetReq, ok := resetCall.Arguments[0].(*p.ResetWorkflowExecutionRequest)
	s.True(resetReq.CurrentWorkflowMutation.ExecutionInfo.LastEventTaskID > 0)
	resetReq.CurrentWorkflowMutation.ExecutionInfo.LastEventTaskID = 0
	s.Equal(true, ok)
	s.Equal(true, resetReq.CurrentWorkflowMutation != nil)
	compareCurrExeInfo.State = p.WorkflowStateCompleted
	compareCurrExeInfo.CloseStatus = p.WorkflowCloseStatusTerminated
	compareCurrExeInfo.NextEventID = 2
	compareCurrExeInfo.CompletionEventBatchID = 1
	s.Equal(compareCurrExeInfo, resetReq.CurrentWorkflowMutation.ExecutionInfo)
	s.Equal(1, len(resetReq.CurrentWorkflowMutation.TransferTasks))
	s.Equal(1, len(resetReq.CurrentWorkflowMutation.TimerTasks))
	s.Equal(p.TransferTaskTypeCloseExecution, resetReq.CurrentWorkflowMutation.TransferTasks[0].GetType())
	s.Equal(p.TaskTypeDeleteHistoryEvent, resetReq.CurrentWorkflowMutation.TimerTasks[0].GetType())
	s.Equal(int64(200), resetReq.CurrentWorkflowMutation.ExecutionStats.HistorySize)

	s.Equal("wfType", resetReq.NewWorkflowSnapshot.ExecutionInfo.WorkflowTypeName)
	s.True(len(resetReq.NewWorkflowSnapshot.ExecutionInfo.RunID) > 0)
	s.Equal([]byte(newBranchToken), resetReq.NewWorkflowSnapshot.ExecutionInfo.BranchToken)
	// 35 = resetEventID(29) + 6 in a batch
	s.Equal(int64(34), resetReq.NewWorkflowSnapshot.ExecutionInfo.DecisionScheduleID)
	s.Equal(int64(35), resetReq.NewWorkflowSnapshot.ExecutionInfo.NextEventID)

	// one activity task, one decision task and one record workflow started task
	s.Equal(3, len(resetReq.NewWorkflowSnapshot.TransferTasks))
	s.Equal(p.TransferTaskTypeActivityTask, resetReq.NewWorkflowSnapshot.TransferTasks[0].GetType())
	s.Equal(p.TransferTaskTypeDecisionTask, resetReq.NewWorkflowSnapshot.TransferTasks[1].GetType())
	s.Equal(p.TransferTaskTypeRecordWorkflowStarted, resetReq.NewWorkflowSnapshot.TransferTasks[2].GetType())

	// WF timeout task, user timer, activity timeout timer, activity retry timer
	s.Equal(3, len(resetReq.NewWorkflowSnapshot.TimerTasks))
	s.Equal(p.TaskTypeWorkflowTimeout, resetReq.NewWorkflowSnapshot.TimerTasks[0].GetType())
	s.Equal(p.TaskTypeUserTimer, resetReq.NewWorkflowSnapshot.TimerTasks[1].GetType())
	s.Equal(p.TaskTypeActivityTimeout, resetReq.NewWorkflowSnapshot.TimerTasks[2].GetType())

	s.Equal(2, len(resetReq.NewWorkflowSnapshot.TimerInfos))
	s.assertTimerIDs([]string{timerUnfiredID1, timerUnfiredID2}, resetReq.NewWorkflowSnapshot.TimerInfos)

	s.Equal(1, len(resetReq.NewWorkflowSnapshot.ActivityInfos))
	s.assertActivityIDs([]string{actIDNotStarted}, resetReq.NewWorkflowSnapshot.ActivityInfos)

	s.Nil(resetReq.NewWorkflowSnapshot.ReplicationTasks)
	s.Nil(resetReq.NewWorkflowSnapshot.ReplicationState)
	s.Equal(0, len(resetReq.NewWorkflowSnapshot.RequestCancelInfos))

	// not supported feature
	s.Empty(resetReq.NewWorkflowSnapshot.ChildExecutionInfos)
	s.Empty(resetReq.NewWorkflowSnapshot.SignalInfos)
	s.Empty(resetReq.NewWorkflowSnapshot.SignalRequestedIDs)
}

func (s *resetorSuite) assertTimerIDs(ids []string, timers []*p.TimerInfo) {
	m := map[string]bool{}
	for _, s := range ids {
		m[s] = true
	}

	for _, t := range timers {
		delete(m, t.TimerID)
	}

	s.Equal(0, len(m))
}

func (s *resetorSuite) assertActivityIDs(ids []string, timers []*p.ActivityInfo) {
	m := map[string]bool{}
	for _, s := range ids {
		m[s] = true
	}

	for _, t := range timers {
		delete(m, t.ActivityID)
	}

	s.Equal(0, len(m))
}

func (s *resetorSuite) TestResetWorkflowExecution_NoReplication_WithRequestCancel() {
	testDomainEntry := cache.NewLocalDomainCacheEntryForTest(
		&p.DomainInfo{ID: testDomainID}, &p.DomainConfig{Retention: 1}, "", nil,
	)
	s.mockDomainCache.On("GetDomainByID", mock.Anything).Return(testDomainEntry, nil)
	s.mockDomainCache.On("GetDomain", mock.Anything).Return(testDomainEntry, nil)
	s.mockEventsCache.On("putEvent", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return().Once()

	request := &h.ResetWorkflowExecutionRequest{}
	domainID := testDomainID
	request.DomainUUID = &domainID
	request.ResetRequest = &workflow.ResetWorkflowExecutionRequest{}

	wid := "wId"
	wfType := "wfType"
	taskListName := "taskList"
	forkRunID := uuid.New().String()
	currRunID := uuid.New().String()
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(wid),
		RunId:      common.StringPtr(forkRunID),
	}
	request.ResetRequest = &workflow.ResetWorkflowExecutionRequest{
		Domain:                common.StringPtr("testDomainName"),
		WorkflowExecution:     &we,
		Reason:                common.StringPtr("test reset"),
		DecisionFinishEventId: common.Int64Ptr(30),
		RequestId:             common.StringPtr(uuid.New().String()),
	}

	forkGwmsRequest := &p.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(wid),
			RunId:      common.StringPtr(forkRunID),
		},
	}

	timerFiredID := "timerID0"
	timerUnfiredID1 := "timerID1"
	timerUnfiredID2 := "timerID2"
	timerAfterReset := "timerID3"
	actIDCompleted1 := "actID0"
	actIDCompleted2 := "actID1"
	actIDStartedRetry := "actID2"
	actIDNotStarted := "actID3"
	actIDStartedNoRetry := "actID4"
	signalName1 := "sig1"
	signalName2 := "sig2"
	cancelWE := &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("cancel-wfid"),
		RunId:      common.StringPtr(uuid.New().String()),
	}
	forkBranchToken := []byte("forkBranchToken")
	forkExeInfo := &p.WorkflowExecutionInfo{
		DomainID:           domainID,
		WorkflowID:         wid,
		WorkflowTypeName:   wfType,
		TaskList:           taskListName,
		RunID:              forkRunID,
		BranchToken:        forkBranchToken,
		NextEventID:        35,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
	}
	forkGwmsResponse := &p.GetWorkflowExecutionResponse{State: &p.WorkflowMutableState{
		ExecutionInfo:  forkExeInfo,
		ExecutionStats: &p.ExecutionStats{},
	}}

	currGwmsRequest := &p.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(wid),
			RunId:      common.StringPtr(currRunID),
		},
	}
	currExeInfo := &p.WorkflowExecutionInfo{
		DomainID:           domainID,
		WorkflowID:         wid,
		WorkflowTypeName:   wfType,
		TaskList:           taskListName,
		RunID:              currRunID,
		NextEventID:        common.FirstEventID,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
	}
	currGwmsResponse := &p.GetWorkflowExecutionResponse{State: &p.WorkflowMutableState{
		ExecutionInfo:  currExeInfo,
		ExecutionStats: &p.ExecutionStats{},
	}}

	gcurResponse := &p.GetCurrentExecutionResponse{
		RunID: currRunID,
	}

	readHistoryReq := &p.ReadHistoryBranchRequest{
		BranchToken:   forkBranchToken,
		MinEventID:    common.FirstEventID,
		MaxEventID:    int64(35),
		PageSize:      defaultHistoryPageSize,
		NextPageToken: nil,
		ShardID:       common.IntPtr(s.shardID),
	}

	taskList := &workflow.TaskList{
		Name: common.StringPtr(taskListName),
	}
	readHistoryResp := &p.ReadHistoryBranchByBatchResponse{
		NextPageToken:    nil,
		Size:             1000,
		LastFirstEventID: int64(31),
		History: []*workflow.History{
			&workflow.History{
				Events: []*workflow.HistoryEvent{
					&workflow.HistoryEvent{
						EventId:   common.Int64Ptr(1),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeWorkflowExecutionStarted),
						WorkflowExecutionStartedEventAttributes: &workflow.WorkflowExecutionStartedEventAttributes{
							WorkflowType: &workflow.WorkflowType{
								Name: common.StringPtr(wfType),
							},
							TaskList:                            taskList,
							Input:                               []byte("testInput"),
							ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
							TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(200),
						},
					},
					{
						EventId:   common.Int64Ptr(2),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskScheduled),
						DecisionTaskScheduledEventAttributes: &workflow.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: common.Int32Ptr(100),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(3),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskStarted),
						DecisionTaskStartedEventAttributes: &workflow.DecisionTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(2),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(4),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskCompleted),
						DecisionTaskCompletedEventAttributes: &workflow.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: common.Int64Ptr(2),
							StartedEventId:   common.Int64Ptr(3),
						},
					},
					{
						EventId:   common.Int64Ptr(5),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeMarkerRecorded),
						MarkerRecordedEventAttributes: &workflow.MarkerRecordedEventAttributes{
							MarkerName:                   common.StringPtr("Version"),
							Details:                      []byte("details"),
							DecisionTaskCompletedEventId: common.Int64Ptr(4),
						},
					},
					{
						EventId:   common.Int64Ptr(6),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskScheduled),
						ActivityTaskScheduledEventAttributes: &workflow.ActivityTaskScheduledEventAttributes{
							ActivityId: common.StringPtr(actIDCompleted1),
							ActivityType: &workflow.ActivityType{
								Name: common.StringPtr("actType0"),
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1000),
							ScheduleToStartTimeoutSeconds: common.Int32Ptr(2000),
							StartToCloseTimeoutSeconds:    common.Int32Ptr(3000),
							HeartbeatTimeoutSeconds:       common.Int32Ptr(4000),
							DecisionTaskCompletedEventId:  common.Int64Ptr(4),
						},
					},
					{
						EventId:   common.Int64Ptr(7),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeTimerStarted),
						TimerStartedEventAttributes: &workflow.TimerStartedEventAttributes{
							TimerId:                      common.StringPtr(timerFiredID),
							StartToFireTimeoutSeconds:    common.Int64Ptr(2),
							DecisionTaskCompletedEventId: common.Int64Ptr(4),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(8),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskStarted),
						ActivityTaskStartedEventAttributes: &workflow.ActivityTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(6),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(9),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskCompleted),
						ActivityTaskCompletedEventAttributes: &workflow.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: common.Int64Ptr(6),
							StartedEventId:   common.Int64Ptr(8),
						},
					},
					{
						EventId:   common.Int64Ptr(10),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskScheduled),
						DecisionTaskScheduledEventAttributes: &workflow.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: common.Int32Ptr(100),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(11),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskStarted),
						DecisionTaskStartedEventAttributes: &workflow.DecisionTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(10),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(12),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskCompleted),
						DecisionTaskCompletedEventAttributes: &workflow.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: common.Int64Ptr(10),
							StartedEventId:   common.Int64Ptr(11),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(13),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeTimerFired),
						TimerFiredEventAttributes: &workflow.TimerFiredEventAttributes{
							TimerId: common.StringPtr(timerFiredID),
						},
					},
					{
						EventId:   common.Int64Ptr(14),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskScheduled),
						DecisionTaskScheduledEventAttributes: &workflow.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: common.Int32Ptr(100),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(15),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskStarted),
						DecisionTaskStartedEventAttributes: &workflow.DecisionTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(14),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(16),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskCompleted),
						DecisionTaskCompletedEventAttributes: &workflow.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: common.Int64Ptr(14),
							StartedEventId:   common.Int64Ptr(15),
						},
					},
					{
						EventId:   common.Int64Ptr(17),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskScheduled),
						ActivityTaskScheduledEventAttributes: &workflow.ActivityTaskScheduledEventAttributes{
							ActivityId: common.StringPtr(actIDStartedRetry),
							ActivityType: &workflow.ActivityType{
								Name: common.StringPtr("actType1"),
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1000),
							ScheduleToStartTimeoutSeconds: common.Int32Ptr(2000),
							StartToCloseTimeoutSeconds:    common.Int32Ptr(3000),
							HeartbeatTimeoutSeconds:       common.Int32Ptr(4000),
							DecisionTaskCompletedEventId:  common.Int64Ptr(16),
							RetryPolicy: &workflow.RetryPolicy{
								InitialIntervalInSeconds:    common.Int32Ptr(1),
								BackoffCoefficient:          common.Float64Ptr(0.2),
								MaximumAttempts:             common.Int32Ptr(10),
								MaximumIntervalInSeconds:    common.Int32Ptr(1000),
								ExpirationIntervalInSeconds: common.Int32Ptr(math.MaxInt32),
							},
						},
					},
					{
						EventId:   common.Int64Ptr(18),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskScheduled),
						ActivityTaskScheduledEventAttributes: &workflow.ActivityTaskScheduledEventAttributes{
							ActivityId: common.StringPtr(actIDNotStarted),
							ActivityType: &workflow.ActivityType{
								Name: common.StringPtr("actType2"),
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1000),
							ScheduleToStartTimeoutSeconds: common.Int32Ptr(2000),
							StartToCloseTimeoutSeconds:    common.Int32Ptr(3000),
							HeartbeatTimeoutSeconds:       common.Int32Ptr(4000),
							DecisionTaskCompletedEventId:  common.Int64Ptr(16),
						},
					},
					{
						EventId:   common.Int64Ptr(19),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeTimerStarted),
						TimerStartedEventAttributes: &workflow.TimerStartedEventAttributes{
							TimerId:                      common.StringPtr(timerUnfiredID1),
							StartToFireTimeoutSeconds:    common.Int64Ptr(4),
							DecisionTaskCompletedEventId: common.Int64Ptr(16),
						},
					},
					{
						EventId:   common.Int64Ptr(20),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeTimerStarted),
						TimerStartedEventAttributes: &workflow.TimerStartedEventAttributes{
							TimerId:                      common.StringPtr(timerUnfiredID2),
							StartToFireTimeoutSeconds:    common.Int64Ptr(8),
							DecisionTaskCompletedEventId: common.Int64Ptr(16),
						},
					},
					{
						EventId:   common.Int64Ptr(21),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskScheduled),
						ActivityTaskScheduledEventAttributes: &workflow.ActivityTaskScheduledEventAttributes{
							ActivityId: common.StringPtr(actIDCompleted2),
							ActivityType: &workflow.ActivityType{
								Name: common.StringPtr("actType2"),
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1000),
							ScheduleToStartTimeoutSeconds: common.Int32Ptr(2000),
							StartToCloseTimeoutSeconds:    common.Int32Ptr(3000),
							HeartbeatTimeoutSeconds:       common.Int32Ptr(4000),
							DecisionTaskCompletedEventId:  common.Int64Ptr(16),
						},
					},
					{
						EventId:   common.Int64Ptr(22),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskScheduled),
						ActivityTaskScheduledEventAttributes: &workflow.ActivityTaskScheduledEventAttributes{
							ActivityId: common.StringPtr(actIDStartedNoRetry),
							ActivityType: &workflow.ActivityType{
								Name: common.StringPtr("actType2"),
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1000),
							ScheduleToStartTimeoutSeconds: common.Int32Ptr(2000),
							StartToCloseTimeoutSeconds:    common.Int32Ptr(3000),
							HeartbeatTimeoutSeconds:       common.Int32Ptr(4000),
							DecisionTaskCompletedEventId:  common.Int64Ptr(16),
						},
					},
					{
						EventId:   common.Int64Ptr(23),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeRequestCancelExternalWorkflowExecutionInitiated),
						RequestCancelExternalWorkflowExecutionInitiatedEventAttributes: &workflow.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
							Domain:                       common.StringPtr("any-domain-name"),
							WorkflowExecution:            cancelWE,
							DecisionTaskCompletedEventId: common.Int64Ptr(16),
							ChildWorkflowOnly:            common.BoolPtr(true),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(24),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskStarted),
						ActivityTaskStartedEventAttributes: &workflow.ActivityTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(21),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(25),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskStarted),
						ActivityTaskStartedEventAttributes: &workflow.ActivityTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(17),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(26),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskStarted),
						ActivityTaskStartedEventAttributes: &workflow.ActivityTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(22),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(27),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskCompleted),
						ActivityTaskCompletedEventAttributes: &workflow.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: common.Int64Ptr(21),
							StartedEventId:   common.Int64Ptr(24),
						},
					},
					{
						EventId:   common.Int64Ptr(28),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskScheduled),
						DecisionTaskScheduledEventAttributes: &workflow.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: common.Int32Ptr(100),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(29),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskStarted),
						DecisionTaskStartedEventAttributes: &workflow.DecisionTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(28),
						},
					},
				},
			},
			/////////////// reset point/////////////
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(30),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskCompleted),
						DecisionTaskCompletedEventAttributes: &workflow.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: common.Int64Ptr(28),
							StartedEventId:   common.Int64Ptr(29),
						},
					},
					{
						EventId:   common.Int64Ptr(31),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeTimerStarted),
						TimerStartedEventAttributes: &workflow.TimerStartedEventAttributes{
							TimerId:                      common.StringPtr(timerAfterReset),
							StartToFireTimeoutSeconds:    common.Int64Ptr(4),
							DecisionTaskCompletedEventId: common.Int64Ptr(30),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(32),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskStarted),
						ActivityTaskStartedEventAttributes: &workflow.ActivityTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(18),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(33),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeWorkflowExecutionSignaled),
						WorkflowExecutionSignaledEventAttributes: &workflow.WorkflowExecutionSignaledEventAttributes{
							SignalName: common.StringPtr(signalName1),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(34),
						Version:   common.Int64Ptr(common.EmptyVersion),
						EventType: common.EventTypePtr(workflow.EventTypeWorkflowExecutionSignaled),
						WorkflowExecutionSignaledEventAttributes: &workflow.WorkflowExecutionSignaledEventAttributes{
							SignalName: common.StringPtr(signalName2),
						},
					},
				},
			},
		},
	}

	eid := int64(0)
	for _, be := range readHistoryResp.History {
		for _, e := range be.Events {
			eid++
			if e.GetEventId() != eid {
				s.Fail(fmt.Sprintf("inconintous eventID: %v, %v", eid, e.GetEventId()))
			}
			e.Timestamp = common.Int64Ptr(1000)
		}
	}

	s.mockExecutionMgr.On("GetWorkflowExecution", forkGwmsRequest).Return(forkGwmsResponse, nil).Once()
	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gcurResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", currGwmsRequest).Return(currGwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("ReadHistoryBranchByBatch", readHistoryReq).Return(readHistoryResp, nil).Once()
	s.mockHistoryV2Mgr.On("CompleteForkBranch", mock.Anything).Return(nil).Maybe()
	s.mockEventsCache.On("putEvent", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return().Once()

	_, err := s.historyEngine.ResetWorkflowExecution(context.Background(), request)
	s.EqualError(err, "BadRequestError{Message: it is not allowed resetting to a point that workflow has pending request cancel.}")
}

func (s *resetorSuite) TestResetWorkflowExecution_Replication_WithTerminatingCurrent() {
	domainName := "testDomainName"
	beforeResetVersion := int64(100)
	afterResetVersion := int64(101)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", beforeResetVersion).Return("standby")
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", afterResetVersion).Return("active")

	testDomainEntry := cache.NewGlobalDomainCacheEntryForTest(
		&p.DomainInfo{ID: testDomainID},
		&p.DomainConfig{Retention: 1},
		&p.DomainReplicationConfig{
			ActiveClusterName: "active",
			Clusters: []*p.ClusterReplicationConfig{
				{
					ClusterName: "active",
				}, {
					ClusterName: "standby",
				},
			},
		},
		afterResetVersion,
		cluster.GetTestClusterMetadata(true, true),
	)
	// override domain cache
	s.mockDomainCache.On("GetDomainByID", mock.Anything).Return(testDomainEntry, nil)
	s.mockDomainCache.On("GetDomain", mock.Anything).Return(testDomainEntry, nil)
	s.mockEventsCache.On("putEvent", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return().Once()

	request := &h.ResetWorkflowExecutionRequest{}
	domainID := testDomainID
	request.DomainUUID = &domainID
	request.ResetRequest = &workflow.ResetWorkflowExecutionRequest{}

	wid := "wId"
	wfType := "wfType"
	taskListName := "taskList"
	forkRunID := uuid.New().String()
	currRunID := uuid.New().String()
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(wid),
		RunId:      common.StringPtr(forkRunID),
	}
	request.ResetRequest = &workflow.ResetWorkflowExecutionRequest{
		Domain:                common.StringPtr(domainName),
		WorkflowExecution:     &we,
		Reason:                common.StringPtr("test reset"),
		DecisionFinishEventId: common.Int64Ptr(30),
		RequestId:             common.StringPtr(uuid.New().String()),
	}

	forkGwmsRequest := &p.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(wid),
			RunId:      common.StringPtr(forkRunID),
		},
	}

	timerFiredID := "timerID0"
	timerUnfiredID1 := "timerID1"
	timerUnfiredID2 := "timerID2"
	timerAfterReset := "timerID3"
	actIDCompleted1 := "actID0"
	actIDCompleted2 := "actID1"
	actIDRetry := "actID2"          // not started, will reschedule
	actIDNotStarted := "actID3"     // not started, will reschedule
	actIDStartedNoRetry := "actID4" // started, will fail
	signalName1 := "sig1"
	signalName2 := "sig2"
	signalName3 := "sig3"
	signalName4 := "sig4"

	forkBranchToken := []byte("forkBranchToken")
	forkExeInfo := &p.WorkflowExecutionInfo{
		DomainID:           domainID,
		WorkflowID:         wid,
		WorkflowTypeName:   wfType,
		TaskList:           taskListName,
		RunID:              forkRunID,
		BranchToken:        forkBranchToken,
		NextEventID:        35,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
	}

	forkRepState := &p.ReplicationState{
		CurrentVersion:      beforeResetVersion,
		StartVersion:        beforeResetVersion,
		LastWriteEventID:    common.EmptyEventID,
		LastWriteVersion:    common.EmptyVersion,
		LastReplicationInfo: map[string]*p.ReplicationInfo{},
	}
	forkGwmsResponse := &p.GetWorkflowExecutionResponse{State: &p.WorkflowMutableState{
		ExecutionInfo:    forkExeInfo,
		ExecutionStats:   &p.ExecutionStats{},
		ReplicationState: forkRepState,
	}}

	currGwmsRequest := &p.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(wid),
			RunId:      common.StringPtr(currRunID),
		},
	}
	currExeInfo := &p.WorkflowExecutionInfo{
		DomainID:           domainID,
		WorkflowID:         wid,
		WorkflowTypeName:   wfType,
		TaskList:           taskListName,
		RunID:              currRunID,
		NextEventID:        common.FirstEventID,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
	}
	compareCurrExeInfo := copyWorkflowExecutionInfo(currExeInfo)
	currGwmsResponse := &p.GetWorkflowExecutionResponse{State: &p.WorkflowMutableState{
		ExecutionInfo:    currExeInfo,
		ExecutionStats:   &p.ExecutionStats{},
		ReplicationState: forkRepState,
	}}

	gcurResponse := &p.GetCurrentExecutionResponse{
		RunID: currRunID,
	}

	readHistoryReq := &p.ReadHistoryBranchRequest{
		BranchToken:   forkBranchToken,
		MinEventID:    common.FirstEventID,
		MaxEventID:    int64(35),
		PageSize:      defaultHistoryPageSize,
		NextPageToken: nil,
		ShardID:       common.IntPtr(s.shardID),
	}

	taskList := &workflow.TaskList{
		Name: common.StringPtr(taskListName),
	}
	readHistoryResp := &p.ReadHistoryBranchByBatchResponse{
		NextPageToken:    nil,
		Size:             1000,
		LastFirstEventID: int64(31),
		History: []*workflow.History{
			&workflow.History{
				Events: []*workflow.HistoryEvent{
					&workflow.HistoryEvent{
						EventId:   common.Int64Ptr(1),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeWorkflowExecutionStarted),
						WorkflowExecutionStartedEventAttributes: &workflow.WorkflowExecutionStartedEventAttributes{
							WorkflowType: &workflow.WorkflowType{
								Name: common.StringPtr(wfType),
							},
							TaskList:                            taskList,
							Input:                               []byte("testInput"),
							ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
							TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(200),
						},
					},
					{
						EventId:   common.Int64Ptr(2),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskScheduled),
						DecisionTaskScheduledEventAttributes: &workflow.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: common.Int32Ptr(100),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(3),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskStarted),
						DecisionTaskStartedEventAttributes: &workflow.DecisionTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(2),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(4),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskCompleted),
						DecisionTaskCompletedEventAttributes: &workflow.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: common.Int64Ptr(2),
							StartedEventId:   common.Int64Ptr(3),
						},
					},
					{
						EventId:   common.Int64Ptr(5),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeMarkerRecorded),
						MarkerRecordedEventAttributes: &workflow.MarkerRecordedEventAttributes{
							MarkerName:                   common.StringPtr("Version"),
							Details:                      []byte("details"),
							DecisionTaskCompletedEventId: common.Int64Ptr(4),
						},
					},
					{
						EventId:   common.Int64Ptr(6),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskScheduled),
						ActivityTaskScheduledEventAttributes: &workflow.ActivityTaskScheduledEventAttributes{
							ActivityId: common.StringPtr(actIDCompleted1),
							ActivityType: &workflow.ActivityType{
								Name: common.StringPtr("actType0"),
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1000),
							ScheduleToStartTimeoutSeconds: common.Int32Ptr(2000),
							StartToCloseTimeoutSeconds:    common.Int32Ptr(3000),
							HeartbeatTimeoutSeconds:       common.Int32Ptr(4000),
							DecisionTaskCompletedEventId:  common.Int64Ptr(4),
						},
					},
					{
						EventId:   common.Int64Ptr(7),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeTimerStarted),
						TimerStartedEventAttributes: &workflow.TimerStartedEventAttributes{
							TimerId:                      common.StringPtr(timerFiredID),
							StartToFireTimeoutSeconds:    common.Int64Ptr(2),
							DecisionTaskCompletedEventId: common.Int64Ptr(4),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(8),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskStarted),
						ActivityTaskStartedEventAttributes: &workflow.ActivityTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(6),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(9),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskCompleted),
						ActivityTaskCompletedEventAttributes: &workflow.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: common.Int64Ptr(6),
							StartedEventId:   common.Int64Ptr(8),
						},
					},
					{
						EventId:   common.Int64Ptr(10),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskScheduled),
						DecisionTaskScheduledEventAttributes: &workflow.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: common.Int32Ptr(100),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(11),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskStarted),
						DecisionTaskStartedEventAttributes: &workflow.DecisionTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(10),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(12),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskCompleted),
						DecisionTaskCompletedEventAttributes: &workflow.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: common.Int64Ptr(10),
							StartedEventId:   common.Int64Ptr(11),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(13),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeTimerFired),
						TimerFiredEventAttributes: &workflow.TimerFiredEventAttributes{
							TimerId: common.StringPtr(timerFiredID),
						},
					},
					{
						EventId:   common.Int64Ptr(14),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskScheduled),
						DecisionTaskScheduledEventAttributes: &workflow.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: common.Int32Ptr(100),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(15),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskStarted),
						DecisionTaskStartedEventAttributes: &workflow.DecisionTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(14),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(16),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskCompleted),
						DecisionTaskCompletedEventAttributes: &workflow.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: common.Int64Ptr(14),
							StartedEventId:   common.Int64Ptr(15),
						},
					},
					{
						EventId:   common.Int64Ptr(17),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskScheduled),
						ActivityTaskScheduledEventAttributes: &workflow.ActivityTaskScheduledEventAttributes{
							ActivityId: common.StringPtr(actIDRetry),
							ActivityType: &workflow.ActivityType{
								Name: common.StringPtr("actType1"),
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1000),
							ScheduleToStartTimeoutSeconds: common.Int32Ptr(2000),
							StartToCloseTimeoutSeconds:    common.Int32Ptr(3000),
							HeartbeatTimeoutSeconds:       common.Int32Ptr(4000),
							DecisionTaskCompletedEventId:  common.Int64Ptr(16),
							RetryPolicy: &workflow.RetryPolicy{
								InitialIntervalInSeconds:    common.Int32Ptr(1),
								BackoffCoefficient:          common.Float64Ptr(0.2),
								MaximumAttempts:             common.Int32Ptr(10),
								MaximumIntervalInSeconds:    common.Int32Ptr(1000),
								ExpirationIntervalInSeconds: common.Int32Ptr(math.MaxInt32),
							},
						},
					},
					{
						EventId:   common.Int64Ptr(18),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskScheduled),
						ActivityTaskScheduledEventAttributes: &workflow.ActivityTaskScheduledEventAttributes{
							ActivityId: common.StringPtr(actIDNotStarted),
							ActivityType: &workflow.ActivityType{
								Name: common.StringPtr("actType2"),
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1000),
							ScheduleToStartTimeoutSeconds: common.Int32Ptr(2000),
							StartToCloseTimeoutSeconds:    common.Int32Ptr(3000),
							HeartbeatTimeoutSeconds:       common.Int32Ptr(4000),
							DecisionTaskCompletedEventId:  common.Int64Ptr(16),
						},
					},
					{
						EventId:   common.Int64Ptr(19),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeTimerStarted),
						TimerStartedEventAttributes: &workflow.TimerStartedEventAttributes{
							TimerId:                      common.StringPtr(timerUnfiredID1),
							StartToFireTimeoutSeconds:    common.Int64Ptr(4),
							DecisionTaskCompletedEventId: common.Int64Ptr(16),
						},
					},
					{
						EventId:   common.Int64Ptr(20),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeTimerStarted),
						TimerStartedEventAttributes: &workflow.TimerStartedEventAttributes{
							TimerId:                      common.StringPtr(timerUnfiredID2),
							StartToFireTimeoutSeconds:    common.Int64Ptr(8),
							DecisionTaskCompletedEventId: common.Int64Ptr(16),
						},
					},
					{
						EventId:   common.Int64Ptr(21),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskScheduled),
						ActivityTaskScheduledEventAttributes: &workflow.ActivityTaskScheduledEventAttributes{
							ActivityId: common.StringPtr(actIDCompleted2),
							ActivityType: &workflow.ActivityType{
								Name: common.StringPtr("actType2"),
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1000),
							ScheduleToStartTimeoutSeconds: common.Int32Ptr(2000),
							StartToCloseTimeoutSeconds:    common.Int32Ptr(3000),
							HeartbeatTimeoutSeconds:       common.Int32Ptr(4000),
							DecisionTaskCompletedEventId:  common.Int64Ptr(16),
						},
					},
					{
						EventId:   common.Int64Ptr(22),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskScheduled),
						ActivityTaskScheduledEventAttributes: &workflow.ActivityTaskScheduledEventAttributes{
							ActivityId: common.StringPtr(actIDStartedNoRetry),
							ActivityType: &workflow.ActivityType{
								Name: common.StringPtr("actType2"),
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1000),
							ScheduleToStartTimeoutSeconds: common.Int32Ptr(2000),
							StartToCloseTimeoutSeconds:    common.Int32Ptr(3000),
							HeartbeatTimeoutSeconds:       common.Int32Ptr(4000),
							DecisionTaskCompletedEventId:  common.Int64Ptr(16),
						},
					},
					{
						EventId:   common.Int64Ptr(23),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeWorkflowExecutionSignaled),
						WorkflowExecutionSignaledEventAttributes: &workflow.WorkflowExecutionSignaledEventAttributes{
							SignalName: common.StringPtr(signalName3),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(24),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskStarted),
						ActivityTaskStartedEventAttributes: &workflow.ActivityTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(21),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(25),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeWorkflowExecutionSignaled),
						WorkflowExecutionSignaledEventAttributes: &workflow.WorkflowExecutionSignaledEventAttributes{
							SignalName: common.StringPtr(signalName4),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(26),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskStarted),
						ActivityTaskStartedEventAttributes: &workflow.ActivityTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(22),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(27),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskCompleted),
						ActivityTaskCompletedEventAttributes: &workflow.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: common.Int64Ptr(21),
							StartedEventId:   common.Int64Ptr(24),
						},
					},
					{
						EventId:   common.Int64Ptr(28),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskScheduled),
						DecisionTaskScheduledEventAttributes: &workflow.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: common.Int32Ptr(100),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(29),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskStarted),
						DecisionTaskStartedEventAttributes: &workflow.DecisionTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(28),
						},
					},
				},
			},
			/////////////// reset point/////////////
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(30),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskCompleted),
						DecisionTaskCompletedEventAttributes: &workflow.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: common.Int64Ptr(28),
							StartedEventId:   common.Int64Ptr(29),
						},
					},
					{
						EventId:   common.Int64Ptr(31),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeTimerStarted),
						TimerStartedEventAttributes: &workflow.TimerStartedEventAttributes{
							TimerId:                      common.StringPtr(timerAfterReset),
							StartToFireTimeoutSeconds:    common.Int64Ptr(4),
							DecisionTaskCompletedEventId: common.Int64Ptr(30),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(32),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskStarted),
						ActivityTaskStartedEventAttributes: &workflow.ActivityTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(18),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(33),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeWorkflowExecutionSignaled),
						WorkflowExecutionSignaledEventAttributes: &workflow.WorkflowExecutionSignaledEventAttributes{
							SignalName: common.StringPtr(signalName1),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(34),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeWorkflowExecutionSignaled),
						WorkflowExecutionSignaledEventAttributes: &workflow.WorkflowExecutionSignaledEventAttributes{
							SignalName: common.StringPtr(signalName2),
						},
					},
				},
			},
		},
	}

	eid := int64(0)
	for _, be := range readHistoryResp.History {
		for _, e := range be.Events {
			eid++
			if e.GetEventId() != eid {
				s.Fail(fmt.Sprintf("inconintous eventID: %v, %v", eid, e.GetEventId()))
			}
			e.Timestamp = common.Int64Ptr(1000)
		}
	}

	newBranchToken := []byte("newBranch")
	forkResp := &p.ForkHistoryBranchResponse{
		NewBranchToken: newBranchToken,
	}

	completeReq := &p.CompleteForkBranchRequest{
		BranchToken: newBranchToken,
		Success:     true,
		ShardID:     common.IntPtr(s.shardID),
	}
	completeReqErr := &p.CompleteForkBranchRequest{
		BranchToken: newBranchToken,
		Success:     true,
		ShardID:     common.IntPtr(s.shardID),
	}

	appendV2Resp := &p.AppendHistoryNodesResponse{
		Size: 200,
	}

	s.mockExecutionMgr.On("GetWorkflowExecution", forkGwmsRequest).Return(forkGwmsResponse, nil).Once()
	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gcurResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", currGwmsRequest).Return(currGwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("ReadHistoryBranchByBatch", readHistoryReq).Return(readHistoryResp, nil).Once()
	s.mockHistoryV2Mgr.On("ForkHistoryBranch", mock.Anything).Return(forkResp, nil).Once()
	s.mockHistoryV2Mgr.On("CompleteForkBranch", completeReq).Return(nil).Once()
	s.mockHistoryV2Mgr.On("CompleteForkBranch", completeReqErr).Return(nil).Maybe()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(appendV2Resp, nil).Times(2)
	s.mockExecutionMgr.On("ResetWorkflowExecution", mock.Anything).Return(nil).Once()
	s.mockEventsCache.On("putEvent", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return().Once()

	response, err := s.historyEngine.ResetWorkflowExecution(context.Background(), request)
	s.Nil(err)
	s.NotNil(response.RunId)

	// verify historyEvent: 5 events to append
	// 1. decisionFailed
	// 2. activityFailed
	// 3. signal 1
	// 4. signal 2
	// 5. decisionTaskScheduled
	calls := s.mockHistoryV2Mgr.Calls
	s.Equal(5, len(calls))
	appendCall := calls[3]
	s.Equal("AppendHistoryNodes", appendCall.Method)
	appendReq, ok := appendCall.Arguments[0].(*p.AppendHistoryNodesRequest)
	s.Equal(true, ok)
	s.Equal(newBranchToken, appendReq.BranchToken)
	s.Equal(false, appendReq.IsNewBranch)
	s.Equal(5, len(appendReq.Events))
	s.Equal(workflow.EventTypeDecisionTaskFailed, appendReq.Events[0].GetEventType())
	s.Equal(workflow.EventTypeActivityTaskFailed, appendReq.Events[1].GetEventType())
	s.Equal(workflow.EventTypeWorkflowExecutionSignaled, appendReq.Events[2].GetEventType())
	s.Equal(workflow.EventTypeWorkflowExecutionSignaled, appendReq.Events[3].GetEventType())
	s.Equal(workflow.EventTypeDecisionTaskScheduled, appendReq.Events[4].GetEventType())

	s.Equal(int64(30), appendReq.Events[0].GetEventId())
	s.Equal(int64(31), appendReq.Events[1].GetEventId())
	s.Equal(int64(32), appendReq.Events[2].GetEventId())
	s.Equal(int64(33), appendReq.Events[3].GetEventId())
	s.Equal(int64(34), appendReq.Events[4].GetEventId())

	// verify executionManager request
	calls = s.mockExecutionMgr.Calls
	s.Equal(4, len(calls))
	resetCall := calls[3]
	s.Equal("ResetWorkflowExecution", resetCall.Method)
	resetReq, ok := resetCall.Arguments[0].(*p.ResetWorkflowExecutionRequest)
	s.True(resetReq.CurrentWorkflowMutation.ExecutionInfo.LastEventTaskID > 0)
	resetReq.CurrentWorkflowMutation.ExecutionInfo.LastEventTaskID = 0
	s.Equal(true, ok)
	s.Equal(true, resetReq.CurrentWorkflowMutation != nil)
	compareCurrExeInfo.State = p.WorkflowStateCompleted
	compareCurrExeInfo.CloseStatus = p.WorkflowCloseStatusTerminated
	compareCurrExeInfo.NextEventID = 2
	compareCurrExeInfo.LastFirstEventID = 1
	compareCurrExeInfo.CompletionEventBatchID = 1
	s.Equal(compareCurrExeInfo, resetReq.CurrentWorkflowMutation.ExecutionInfo)
	s.Equal(1, len(resetReq.CurrentWorkflowMutation.TransferTasks))
	s.Equal(1, len(resetReq.CurrentWorkflowMutation.TimerTasks))
	s.Equal(p.TransferTaskTypeCloseExecution, resetReq.CurrentWorkflowMutation.TransferTasks[0].GetType())
	s.Equal(p.TaskTypeDeleteHistoryEvent, resetReq.CurrentWorkflowMutation.TimerTasks[0].GetType())
	s.Equal(int64(200), resetReq.CurrentWorkflowMutation.ExecutionStats.HistorySize)

	s.Equal("wfType", resetReq.NewWorkflowSnapshot.ExecutionInfo.WorkflowTypeName)
	s.True(len(resetReq.NewWorkflowSnapshot.ExecutionInfo.RunID) > 0)
	s.Equal([]byte(newBranchToken), resetReq.NewWorkflowSnapshot.ExecutionInfo.BranchToken)

	s.Equal(int64(34), resetReq.NewWorkflowSnapshot.ExecutionInfo.DecisionScheduleID)
	s.Equal(int64(35), resetReq.NewWorkflowSnapshot.ExecutionInfo.NextEventID)

	s.Equal(4, len(resetReq.NewWorkflowSnapshot.TransferTasks))
	s.Equal(p.TransferTaskTypeActivityTask, resetReq.NewWorkflowSnapshot.TransferTasks[0].GetType())
	s.Equal(p.TransferTaskTypeActivityTask, resetReq.NewWorkflowSnapshot.TransferTasks[1].GetType())
	s.Equal(p.TransferTaskTypeDecisionTask, resetReq.NewWorkflowSnapshot.TransferTasks[2].GetType())
	s.Equal(p.TransferTaskTypeRecordWorkflowStarted, resetReq.NewWorkflowSnapshot.TransferTasks[3].GetType())

	// WF timeout task, user timer, activity timeout timer, activity retry timer
	s.Equal(3, len(resetReq.NewWorkflowSnapshot.TimerTasks))
	s.Equal(p.TaskTypeWorkflowTimeout, resetReq.NewWorkflowSnapshot.TimerTasks[0].GetType())
	s.Equal(p.TaskTypeUserTimer, resetReq.NewWorkflowSnapshot.TimerTasks[1].GetType())
	s.Equal(p.TaskTypeActivityTimeout, resetReq.NewWorkflowSnapshot.TimerTasks[2].GetType())

	s.Equal(2, len(resetReq.NewWorkflowSnapshot.TimerInfos))
	s.assertTimerIDs([]string{timerUnfiredID1, timerUnfiredID2}, resetReq.NewWorkflowSnapshot.TimerInfos)

	s.Equal(2, len(resetReq.NewWorkflowSnapshot.ActivityInfos))
	s.assertActivityIDs([]string{actIDRetry, actIDNotStarted}, resetReq.NewWorkflowSnapshot.ActivityInfos)

	s.Equal(1, len(resetReq.NewWorkflowSnapshot.ReplicationTasks))
	s.Equal(p.ReplicationTaskTypeHistory, resetReq.NewWorkflowSnapshot.ReplicationTasks[0].GetType())
	s.Equal(1, len(resetReq.CurrentWorkflowMutation.ReplicationTasks))
	s.Equal(p.ReplicationTaskTypeHistory, resetReq.CurrentWorkflowMutation.ReplicationTasks[0].GetType())

	compareRepState := copyReplicationState(forkRepState)
	compareRepState.StartVersion = beforeResetVersion
	compareRepState.CurrentVersion = afterResetVersion
	compareRepState.LastWriteEventID = 34
	compareRepState.LastWriteVersion = afterResetVersion
	compareRepState.LastReplicationInfo = map[string]*p.ReplicationInfo{
		"standby": &p.ReplicationInfo{
			LastEventID: 29,
			Version:     beforeResetVersion,
		},
	}
	s.Equal(compareRepState, resetReq.NewWorkflowSnapshot.ReplicationState)

	// not supported feature
	s.Empty(resetReq.NewWorkflowSnapshot.ChildExecutionInfos)
	s.Empty(resetReq.NewWorkflowSnapshot.SignalInfos)
	s.Empty(resetReq.NewWorkflowSnapshot.SignalRequestedIDs)
	s.Equal(0, len(resetReq.NewWorkflowSnapshot.RequestCancelInfos))
}

func (s *resetorSuite) TestResetWorkflowExecution_Replication_NotActive() {
	domainName := "testDomainName"
	beforeResetVersion := int64(100)
	afterResetVersion := int64(101)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", beforeResetVersion).Return("active")
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", afterResetVersion).Return("standby")

	testDomainEntry := cache.NewGlobalDomainCacheEntryForTest(
		&p.DomainInfo{ID: testDomainID},
		&p.DomainConfig{Retention: 1},
		&p.DomainReplicationConfig{
			ActiveClusterName: "active",
			Clusters: []*p.ClusterReplicationConfig{
				{
					ClusterName: "active",
				}, {
					ClusterName: "standby",
				},
			},
		},
		afterResetVersion,
		cluster.GetTestClusterMetadata(true, true),
	)
	// override domain cache
	s.mockDomainCache.On("GetDomainByID", mock.Anything).Return(testDomainEntry, nil)
	s.mockDomainCache.On("GetDomain", mock.Anything).Return(testDomainEntry, nil)
	s.mockEventsCache.On("putEvent", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return().Once()

	request := &h.ResetWorkflowExecutionRequest{}
	domainID := testDomainID
	request.DomainUUID = &domainID
	request.ResetRequest = &workflow.ResetWorkflowExecutionRequest{}

	wid := "wId"
	wfType := "wfType"
	taskListName := "taskList"
	forkRunID := uuid.New().String()
	currRunID := uuid.New().String()
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(wid),
		RunId:      common.StringPtr(forkRunID),
	}
	request.ResetRequest = &workflow.ResetWorkflowExecutionRequest{
		Domain:                common.StringPtr(domainName),
		WorkflowExecution:     &we,
		Reason:                common.StringPtr("test reset"),
		DecisionFinishEventId: common.Int64Ptr(30),
		RequestId:             common.StringPtr(uuid.New().String()),
	}

	forkGwmsRequest := &p.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(wid),
			RunId:      common.StringPtr(forkRunID),
		},
	}

	timerFiredID := "timerID0"
	timerUnfiredID1 := "timerID1"
	timerUnfiredID2 := "timerID2"
	timerAfterReset := "timerID3"
	actIDCompleted1 := "actID0"
	actIDCompleted2 := "actID1"
	actIDRetry := "actID2"          // not started, will reschedule
	actIDNotStarted := "actID3"     // not started, will reschedule
	actIDStartedNoRetry := "actID4" // started, will fail
	signalName1 := "sig1"
	signalName2 := "sig2"
	signalName3 := "sig3"
	signalName4 := "sig4"

	forkBranchToken := []byte("forkBranchToken")
	forkExeInfo := &p.WorkflowExecutionInfo{
		DomainID:           domainID,
		WorkflowID:         wid,
		WorkflowTypeName:   wfType,
		TaskList:           taskListName,
		RunID:              forkRunID,
		BranchToken:        forkBranchToken,
		NextEventID:        35,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
	}

	forkRepState := &p.ReplicationState{
		CurrentVersion:      beforeResetVersion,
		StartVersion:        beforeResetVersion,
		LastWriteEventID:    common.EmptyEventID,
		LastWriteVersion:    common.EmptyVersion,
		LastReplicationInfo: map[string]*p.ReplicationInfo{},
	}
	forkGwmsResponse := &p.GetWorkflowExecutionResponse{State: &p.WorkflowMutableState{
		ExecutionInfo:    forkExeInfo,
		ExecutionStats:   &p.ExecutionStats{},
		ReplicationState: forkRepState,
	}}

	currGwmsRequest := &p.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(wid),
			RunId:      common.StringPtr(currRunID),
		},
	}
	currExeInfo := &p.WorkflowExecutionInfo{
		DomainID:           domainID,
		WorkflowID:         wid,
		WorkflowTypeName:   wfType,
		TaskList:           taskListName,
		RunID:              currRunID,
		NextEventID:        common.FirstEventID,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
	}
	currGwmsResponse := &p.GetWorkflowExecutionResponse{State: &p.WorkflowMutableState{
		ExecutionInfo:    currExeInfo,
		ExecutionStats:   &p.ExecutionStats{},
		ReplicationState: forkRepState,
	}}

	gcurResponse := &p.GetCurrentExecutionResponse{
		RunID: currRunID,
	}

	readHistoryReq := &p.ReadHistoryBranchRequest{
		BranchToken:   forkBranchToken,
		MinEventID:    common.FirstEventID,
		MaxEventID:    int64(35),
		PageSize:      defaultHistoryPageSize,
		NextPageToken: nil,
		ShardID:       common.IntPtr(s.shardID),
	}

	taskList := &workflow.TaskList{
		Name: common.StringPtr(taskListName),
	}
	readHistoryResp := &p.ReadHistoryBranchByBatchResponse{
		NextPageToken:    nil,
		Size:             1000,
		LastFirstEventID: int64(31),
		History: []*workflow.History{
			&workflow.History{
				Events: []*workflow.HistoryEvent{
					&workflow.HistoryEvent{
						EventId:   common.Int64Ptr(1),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeWorkflowExecutionStarted),
						WorkflowExecutionStartedEventAttributes: &workflow.WorkflowExecutionStartedEventAttributes{
							WorkflowType: &workflow.WorkflowType{
								Name: common.StringPtr(wfType),
							},
							TaskList:                            taskList,
							Input:                               []byte("testInput"),
							ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
							TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(200),
						},
					},
					{
						EventId:   common.Int64Ptr(2),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskScheduled),
						DecisionTaskScheduledEventAttributes: &workflow.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: common.Int32Ptr(100),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(3),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskStarted),
						DecisionTaskStartedEventAttributes: &workflow.DecisionTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(2),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(4),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskCompleted),
						DecisionTaskCompletedEventAttributes: &workflow.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: common.Int64Ptr(2),
							StartedEventId:   common.Int64Ptr(3),
						},
					},
					{
						EventId:   common.Int64Ptr(5),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeMarkerRecorded),
						MarkerRecordedEventAttributes: &workflow.MarkerRecordedEventAttributes{
							MarkerName:                   common.StringPtr("Version"),
							Details:                      []byte("details"),
							DecisionTaskCompletedEventId: common.Int64Ptr(4),
						},
					},
					{
						EventId:   common.Int64Ptr(6),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskScheduled),
						ActivityTaskScheduledEventAttributes: &workflow.ActivityTaskScheduledEventAttributes{
							ActivityId: common.StringPtr(actIDCompleted1),
							ActivityType: &workflow.ActivityType{
								Name: common.StringPtr("actType0"),
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1000),
							ScheduleToStartTimeoutSeconds: common.Int32Ptr(2000),
							StartToCloseTimeoutSeconds:    common.Int32Ptr(3000),
							HeartbeatTimeoutSeconds:       common.Int32Ptr(4000),
							DecisionTaskCompletedEventId:  common.Int64Ptr(4),
						},
					},
					{
						EventId:   common.Int64Ptr(7),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeTimerStarted),
						TimerStartedEventAttributes: &workflow.TimerStartedEventAttributes{
							TimerId:                      common.StringPtr(timerFiredID),
							StartToFireTimeoutSeconds:    common.Int64Ptr(2),
							DecisionTaskCompletedEventId: common.Int64Ptr(4),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(8),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskStarted),
						ActivityTaskStartedEventAttributes: &workflow.ActivityTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(6),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(9),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskCompleted),
						ActivityTaskCompletedEventAttributes: &workflow.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: common.Int64Ptr(6),
							StartedEventId:   common.Int64Ptr(8),
						},
					},
					{
						EventId:   common.Int64Ptr(10),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskScheduled),
						DecisionTaskScheduledEventAttributes: &workflow.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: common.Int32Ptr(100),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(11),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskStarted),
						DecisionTaskStartedEventAttributes: &workflow.DecisionTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(10),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(12),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskCompleted),
						DecisionTaskCompletedEventAttributes: &workflow.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: common.Int64Ptr(10),
							StartedEventId:   common.Int64Ptr(11),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(13),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeTimerFired),
						TimerFiredEventAttributes: &workflow.TimerFiredEventAttributes{
							TimerId: common.StringPtr(timerFiredID),
						},
					},
					{
						EventId:   common.Int64Ptr(14),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskScheduled),
						DecisionTaskScheduledEventAttributes: &workflow.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: common.Int32Ptr(100),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(15),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskStarted),
						DecisionTaskStartedEventAttributes: &workflow.DecisionTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(14),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(16),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskCompleted),
						DecisionTaskCompletedEventAttributes: &workflow.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: common.Int64Ptr(14),
							StartedEventId:   common.Int64Ptr(15),
						},
					},
					{
						EventId:   common.Int64Ptr(17),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskScheduled),
						ActivityTaskScheduledEventAttributes: &workflow.ActivityTaskScheduledEventAttributes{
							ActivityId: common.StringPtr(actIDRetry),
							ActivityType: &workflow.ActivityType{
								Name: common.StringPtr("actType1"),
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1000),
							ScheduleToStartTimeoutSeconds: common.Int32Ptr(2000),
							StartToCloseTimeoutSeconds:    common.Int32Ptr(3000),
							HeartbeatTimeoutSeconds:       common.Int32Ptr(4000),
							DecisionTaskCompletedEventId:  common.Int64Ptr(16),
							RetryPolicy: &workflow.RetryPolicy{
								InitialIntervalInSeconds:    common.Int32Ptr(1),
								BackoffCoefficient:          common.Float64Ptr(0.2),
								MaximumAttempts:             common.Int32Ptr(10),
								MaximumIntervalInSeconds:    common.Int32Ptr(1000),
								ExpirationIntervalInSeconds: common.Int32Ptr(math.MaxInt32),
							},
						},
					},
					{
						EventId:   common.Int64Ptr(18),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskScheduled),
						ActivityTaskScheduledEventAttributes: &workflow.ActivityTaskScheduledEventAttributes{
							ActivityId: common.StringPtr(actIDNotStarted),
							ActivityType: &workflow.ActivityType{
								Name: common.StringPtr("actType2"),
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1000),
							ScheduleToStartTimeoutSeconds: common.Int32Ptr(2000),
							StartToCloseTimeoutSeconds:    common.Int32Ptr(3000),
							HeartbeatTimeoutSeconds:       common.Int32Ptr(4000),
							DecisionTaskCompletedEventId:  common.Int64Ptr(16),
						},
					},
					{
						EventId:   common.Int64Ptr(19),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeTimerStarted),
						TimerStartedEventAttributes: &workflow.TimerStartedEventAttributes{
							TimerId:                      common.StringPtr(timerUnfiredID1),
							StartToFireTimeoutSeconds:    common.Int64Ptr(4),
							DecisionTaskCompletedEventId: common.Int64Ptr(16),
						},
					},
					{
						EventId:   common.Int64Ptr(20),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeTimerStarted),
						TimerStartedEventAttributes: &workflow.TimerStartedEventAttributes{
							TimerId:                      common.StringPtr(timerUnfiredID2),
							StartToFireTimeoutSeconds:    common.Int64Ptr(8),
							DecisionTaskCompletedEventId: common.Int64Ptr(16),
						},
					},
					{
						EventId:   common.Int64Ptr(21),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskScheduled),
						ActivityTaskScheduledEventAttributes: &workflow.ActivityTaskScheduledEventAttributes{
							ActivityId: common.StringPtr(actIDCompleted2),
							ActivityType: &workflow.ActivityType{
								Name: common.StringPtr("actType2"),
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1000),
							ScheduleToStartTimeoutSeconds: common.Int32Ptr(2000),
							StartToCloseTimeoutSeconds:    common.Int32Ptr(3000),
							HeartbeatTimeoutSeconds:       common.Int32Ptr(4000),
							DecisionTaskCompletedEventId:  common.Int64Ptr(16),
						},
					},
					{
						EventId:   common.Int64Ptr(22),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskScheduled),
						ActivityTaskScheduledEventAttributes: &workflow.ActivityTaskScheduledEventAttributes{
							ActivityId: common.StringPtr(actIDStartedNoRetry),
							ActivityType: &workflow.ActivityType{
								Name: common.StringPtr("actType2"),
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1000),
							ScheduleToStartTimeoutSeconds: common.Int32Ptr(2000),
							StartToCloseTimeoutSeconds:    common.Int32Ptr(3000),
							HeartbeatTimeoutSeconds:       common.Int32Ptr(4000),
							DecisionTaskCompletedEventId:  common.Int64Ptr(16),
						},
					},
					{
						EventId:   common.Int64Ptr(23),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeWorkflowExecutionSignaled),
						WorkflowExecutionSignaledEventAttributes: &workflow.WorkflowExecutionSignaledEventAttributes{
							SignalName: common.StringPtr(signalName3),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(24),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskStarted),
						ActivityTaskStartedEventAttributes: &workflow.ActivityTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(21),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(25),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeWorkflowExecutionSignaled),
						WorkflowExecutionSignaledEventAttributes: &workflow.WorkflowExecutionSignaledEventAttributes{
							SignalName: common.StringPtr(signalName4),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(26),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskStarted),
						ActivityTaskStartedEventAttributes: &workflow.ActivityTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(22),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(27),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskCompleted),
						ActivityTaskCompletedEventAttributes: &workflow.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: common.Int64Ptr(21),
							StartedEventId:   common.Int64Ptr(24),
						},
					},
					{
						EventId:   common.Int64Ptr(28),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskScheduled),
						DecisionTaskScheduledEventAttributes: &workflow.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: common.Int32Ptr(100),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(29),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskStarted),
						DecisionTaskStartedEventAttributes: &workflow.DecisionTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(28),
						},
					},
				},
			},
			/////////////// reset point/////////////
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(30),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskCompleted),
						DecisionTaskCompletedEventAttributes: &workflow.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: common.Int64Ptr(28),
							StartedEventId:   common.Int64Ptr(29),
						},
					},
					{
						EventId:   common.Int64Ptr(31),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeTimerStarted),
						TimerStartedEventAttributes: &workflow.TimerStartedEventAttributes{
							TimerId:                      common.StringPtr(timerAfterReset),
							StartToFireTimeoutSeconds:    common.Int64Ptr(4),
							DecisionTaskCompletedEventId: common.Int64Ptr(30),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(32),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskStarted),
						ActivityTaskStartedEventAttributes: &workflow.ActivityTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(18),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(33),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeWorkflowExecutionSignaled),
						WorkflowExecutionSignaledEventAttributes: &workflow.WorkflowExecutionSignaledEventAttributes{
							SignalName: common.StringPtr(signalName1),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(34),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeWorkflowExecutionSignaled),
						WorkflowExecutionSignaledEventAttributes: &workflow.WorkflowExecutionSignaledEventAttributes{
							SignalName: common.StringPtr(signalName2),
						},
					},
				},
			},
		},
	}

	eid := int64(0)
	for _, be := range readHistoryResp.History {
		for _, e := range be.Events {
			eid++
			if e.GetEventId() != eid {
				s.Fail(fmt.Sprintf("inconintous eventID: %v, %v", eid, e.GetEventId()))
			}
			e.Timestamp = common.Int64Ptr(1000)
		}
	}

	newBranchToken := []byte("newBranch")
	forkResp := &p.ForkHistoryBranchResponse{
		NewBranchToken: newBranchToken,
	}

	completeReqErr := &p.CompleteForkBranchRequest{
		BranchToken: newBranchToken,
		Success:     true,
		ShardID:     common.IntPtr(s.shardID),
	}

	s.mockExecutionMgr.On("GetWorkflowExecution", forkGwmsRequest).Return(forkGwmsResponse, nil).Once()
	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gcurResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", currGwmsRequest).Return(currGwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("ReadHistoryBranchByBatch", readHistoryReq).Return(readHistoryResp, nil).Once()
	s.mockHistoryV2Mgr.On("ForkHistoryBranch", mock.Anything).Return(forkResp, nil).Once()
	s.mockHistoryV2Mgr.On("CompleteForkBranch", completeReqErr).Return(nil).Once()
	s.mockEventsCache.On("putEvent", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return().Once()

	_, err := s.historyEngine.ResetWorkflowExecution(context.Background(), request)
	s.IsType(&shared.DomainNotActiveError{}, err)
}

func (s *resetorSuite) TestResetWorkflowExecution_Replication_NoTerminatingCurrent() {
	domainName := "testDomainName"
	beforeResetVersion := int64(100)
	afterResetVersion := int64(101)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", beforeResetVersion).Return("standby")
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", afterResetVersion).Return("active")

	testDomainEntry := cache.NewGlobalDomainCacheEntryForTest(
		&p.DomainInfo{ID: testDomainID},
		&p.DomainConfig{Retention: 1},
		&p.DomainReplicationConfig{
			ActiveClusterName: "active",
			Clusters: []*p.ClusterReplicationConfig{
				{
					ClusterName: "active",
				}, {
					ClusterName: "standby",
				},
			},
		},
		afterResetVersion,
		cluster.GetTestClusterMetadata(true, true),
	)
	// override domain cache
	s.mockDomainCache.On("GetDomainByID", mock.Anything).Return(testDomainEntry, nil)
	s.mockDomainCache.On("GetDomain", mock.Anything).Return(testDomainEntry, nil)
	s.mockEventsCache.On("putEvent", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return().Once()

	request := &h.ResetWorkflowExecutionRequest{}
	domainID := testDomainID
	request.DomainUUID = &domainID
	request.ResetRequest = &workflow.ResetWorkflowExecutionRequest{}

	wid := "wId"
	wfType := "wfType"
	taskListName := "taskList"
	forkRunID := uuid.New().String()
	currRunID := uuid.New().String()
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(wid),
		RunId:      common.StringPtr(forkRunID),
	}
	request.ResetRequest = &workflow.ResetWorkflowExecutionRequest{
		Domain:                common.StringPtr(domainName),
		WorkflowExecution:     &we,
		Reason:                common.StringPtr("test reset"),
		DecisionFinishEventId: common.Int64Ptr(30),
		RequestId:             common.StringPtr(uuid.New().String()),
	}

	forkGwmsRequest := &p.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(wid),
			RunId:      common.StringPtr(forkRunID),
		},
	}

	timerFiredID := "timerID0"
	timerUnfiredID1 := "timerID1"
	timerUnfiredID2 := "timerID2"
	timerAfterReset := "timerID3"
	actIDCompleted1 := "actID0"
	actIDCompleted2 := "actID1"
	actIDRetry := "actID2"          // not started, will reschedule
	actIDNotStarted := "actID3"     // not started, will reschedule
	actIDStartedNoRetry := "actID4" // started, will fail
	signalName1 := "sig1"
	signalName2 := "sig2"
	signalName3 := "sig3"
	signalName4 := "sig4"

	forkBranchToken := []byte("forkBranchToken")
	forkExeInfo := &p.WorkflowExecutionInfo{
		DomainID:           domainID,
		WorkflowID:         wid,
		WorkflowTypeName:   wfType,
		TaskList:           taskListName,
		RunID:              forkRunID,
		BranchToken:        forkBranchToken,
		NextEventID:        35,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
	}

	forkRepState := &p.ReplicationState{
		CurrentVersion:      beforeResetVersion,
		StartVersion:        beforeResetVersion,
		LastWriteEventID:    common.EmptyEventID,
		LastWriteVersion:    common.EmptyVersion,
		LastReplicationInfo: map[string]*p.ReplicationInfo{},
	}
	forkGwmsResponse := &p.GetWorkflowExecutionResponse{State: &p.WorkflowMutableState{
		ExecutionInfo:    forkExeInfo,
		ExecutionStats:   &p.ExecutionStats{},
		ReplicationState: forkRepState,
	}}

	currGwmsRequest := &p.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(wid),
			RunId:      common.StringPtr(currRunID),
		},
	}
	currExeInfo := &p.WorkflowExecutionInfo{
		DomainID:           domainID,
		WorkflowID:         wid,
		WorkflowTypeName:   wfType,
		TaskList:           taskListName,
		RunID:              currRunID,
		NextEventID:        common.FirstEventID,
		State:              p.WorkflowStateCompleted,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
	}
	compareCurrExeInfo := copyWorkflowExecutionInfo(currExeInfo)
	currGwmsResponse := &p.GetWorkflowExecutionResponse{State: &p.WorkflowMutableState{
		ExecutionInfo:    currExeInfo,
		ExecutionStats:   &p.ExecutionStats{},
		ReplicationState: forkRepState,
	}}

	gcurResponse := &p.GetCurrentExecutionResponse{
		RunID: currRunID,
	}

	readHistoryReq := &p.ReadHistoryBranchRequest{
		BranchToken:   forkBranchToken,
		MinEventID:    common.FirstEventID,
		MaxEventID:    int64(35),
		PageSize:      defaultHistoryPageSize,
		NextPageToken: nil,
		ShardID:       common.IntPtr(s.shardID),
	}

	taskList := &workflow.TaskList{
		Name: common.StringPtr(taskListName),
	}
	readHistoryResp := &p.ReadHistoryBranchByBatchResponse{
		NextPageToken:    nil,
		Size:             1000,
		LastFirstEventID: int64(31),
		History: []*workflow.History{
			&workflow.History{
				Events: []*workflow.HistoryEvent{
					&workflow.HistoryEvent{
						EventId:   common.Int64Ptr(1),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeWorkflowExecutionStarted),
						WorkflowExecutionStartedEventAttributes: &workflow.WorkflowExecutionStartedEventAttributes{
							WorkflowType: &workflow.WorkflowType{
								Name: common.StringPtr(wfType),
							},
							TaskList:                            taskList,
							Input:                               []byte("testInput"),
							ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
							TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(200),
						},
					},
					{
						EventId:   common.Int64Ptr(2),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskScheduled),
						DecisionTaskScheduledEventAttributes: &workflow.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: common.Int32Ptr(100),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(3),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskStarted),
						DecisionTaskStartedEventAttributes: &workflow.DecisionTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(2),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(4),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskCompleted),
						DecisionTaskCompletedEventAttributes: &workflow.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: common.Int64Ptr(2),
							StartedEventId:   common.Int64Ptr(3),
						},
					},
					{
						EventId:   common.Int64Ptr(5),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeMarkerRecorded),
						MarkerRecordedEventAttributes: &workflow.MarkerRecordedEventAttributes{
							MarkerName:                   common.StringPtr("Version"),
							Details:                      []byte("details"),
							DecisionTaskCompletedEventId: common.Int64Ptr(4),
						},
					},
					{
						EventId:   common.Int64Ptr(6),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskScheduled),
						ActivityTaskScheduledEventAttributes: &workflow.ActivityTaskScheduledEventAttributes{
							ActivityId: common.StringPtr(actIDCompleted1),
							ActivityType: &workflow.ActivityType{
								Name: common.StringPtr("actType0"),
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1000),
							ScheduleToStartTimeoutSeconds: common.Int32Ptr(2000),
							StartToCloseTimeoutSeconds:    common.Int32Ptr(3000),
							HeartbeatTimeoutSeconds:       common.Int32Ptr(4000),
							DecisionTaskCompletedEventId:  common.Int64Ptr(4),
						},
					},
					{
						EventId:   common.Int64Ptr(7),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeTimerStarted),
						TimerStartedEventAttributes: &workflow.TimerStartedEventAttributes{
							TimerId:                      common.StringPtr(timerFiredID),
							StartToFireTimeoutSeconds:    common.Int64Ptr(2),
							DecisionTaskCompletedEventId: common.Int64Ptr(4),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(8),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskStarted),
						ActivityTaskStartedEventAttributes: &workflow.ActivityTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(6),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(9),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskCompleted),
						ActivityTaskCompletedEventAttributes: &workflow.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: common.Int64Ptr(6),
							StartedEventId:   common.Int64Ptr(8),
						},
					},
					{
						EventId:   common.Int64Ptr(10),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskScheduled),
						DecisionTaskScheduledEventAttributes: &workflow.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: common.Int32Ptr(100),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(11),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskStarted),
						DecisionTaskStartedEventAttributes: &workflow.DecisionTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(10),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(12),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskCompleted),
						DecisionTaskCompletedEventAttributes: &workflow.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: common.Int64Ptr(10),
							StartedEventId:   common.Int64Ptr(11),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(13),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeTimerFired),
						TimerFiredEventAttributes: &workflow.TimerFiredEventAttributes{
							TimerId: common.StringPtr(timerFiredID),
						},
					},
					{
						EventId:   common.Int64Ptr(14),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskScheduled),
						DecisionTaskScheduledEventAttributes: &workflow.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: common.Int32Ptr(100),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(15),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskStarted),
						DecisionTaskStartedEventAttributes: &workflow.DecisionTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(14),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(16),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskCompleted),
						DecisionTaskCompletedEventAttributes: &workflow.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: common.Int64Ptr(14),
							StartedEventId:   common.Int64Ptr(15),
						},
					},
					{
						EventId:   common.Int64Ptr(17),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskScheduled),
						ActivityTaskScheduledEventAttributes: &workflow.ActivityTaskScheduledEventAttributes{
							ActivityId: common.StringPtr(actIDRetry),
							ActivityType: &workflow.ActivityType{
								Name: common.StringPtr("actType1"),
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1000),
							ScheduleToStartTimeoutSeconds: common.Int32Ptr(2000),
							StartToCloseTimeoutSeconds:    common.Int32Ptr(3000),
							HeartbeatTimeoutSeconds:       common.Int32Ptr(4000),
							DecisionTaskCompletedEventId:  common.Int64Ptr(16),
							RetryPolicy: &workflow.RetryPolicy{
								InitialIntervalInSeconds:    common.Int32Ptr(1),
								BackoffCoefficient:          common.Float64Ptr(0.2),
								MaximumAttempts:             common.Int32Ptr(10),
								MaximumIntervalInSeconds:    common.Int32Ptr(1000),
								ExpirationIntervalInSeconds: common.Int32Ptr(math.MaxInt32),
							},
						},
					},
					{
						EventId:   common.Int64Ptr(18),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskScheduled),
						ActivityTaskScheduledEventAttributes: &workflow.ActivityTaskScheduledEventAttributes{
							ActivityId: common.StringPtr(actIDNotStarted),
							ActivityType: &workflow.ActivityType{
								Name: common.StringPtr("actType2"),
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1000),
							ScheduleToStartTimeoutSeconds: common.Int32Ptr(2000),
							StartToCloseTimeoutSeconds:    common.Int32Ptr(3000),
							HeartbeatTimeoutSeconds:       common.Int32Ptr(4000),
							DecisionTaskCompletedEventId:  common.Int64Ptr(16),
						},
					},
					{
						EventId:   common.Int64Ptr(19),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeTimerStarted),
						TimerStartedEventAttributes: &workflow.TimerStartedEventAttributes{
							TimerId:                      common.StringPtr(timerUnfiredID1),
							StartToFireTimeoutSeconds:    common.Int64Ptr(4),
							DecisionTaskCompletedEventId: common.Int64Ptr(16),
						},
					},
					{
						EventId:   common.Int64Ptr(20),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeTimerStarted),
						TimerStartedEventAttributes: &workflow.TimerStartedEventAttributes{
							TimerId:                      common.StringPtr(timerUnfiredID2),
							StartToFireTimeoutSeconds:    common.Int64Ptr(8),
							DecisionTaskCompletedEventId: common.Int64Ptr(16),
						},
					},
					{
						EventId:   common.Int64Ptr(21),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskScheduled),
						ActivityTaskScheduledEventAttributes: &workflow.ActivityTaskScheduledEventAttributes{
							ActivityId: common.StringPtr(actIDCompleted2),
							ActivityType: &workflow.ActivityType{
								Name: common.StringPtr("actType2"),
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1000),
							ScheduleToStartTimeoutSeconds: common.Int32Ptr(2000),
							StartToCloseTimeoutSeconds:    common.Int32Ptr(3000),
							HeartbeatTimeoutSeconds:       common.Int32Ptr(4000),
							DecisionTaskCompletedEventId:  common.Int64Ptr(16),
						},
					},
					{
						EventId:   common.Int64Ptr(22),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskScheduled),
						ActivityTaskScheduledEventAttributes: &workflow.ActivityTaskScheduledEventAttributes{
							ActivityId: common.StringPtr(actIDStartedNoRetry),
							ActivityType: &workflow.ActivityType{
								Name: common.StringPtr("actType2"),
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1000),
							ScheduleToStartTimeoutSeconds: common.Int32Ptr(2000),
							StartToCloseTimeoutSeconds:    common.Int32Ptr(3000),
							HeartbeatTimeoutSeconds:       common.Int32Ptr(4000),
							DecisionTaskCompletedEventId:  common.Int64Ptr(16),
						},
					},
					{
						EventId:   common.Int64Ptr(23),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeWorkflowExecutionSignaled),
						WorkflowExecutionSignaledEventAttributes: &workflow.WorkflowExecutionSignaledEventAttributes{
							SignalName: common.StringPtr(signalName3),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(24),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskStarted),
						ActivityTaskStartedEventAttributes: &workflow.ActivityTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(21),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(25),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeWorkflowExecutionSignaled),
						WorkflowExecutionSignaledEventAttributes: &workflow.WorkflowExecutionSignaledEventAttributes{
							SignalName: common.StringPtr(signalName4),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(26),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskStarted),
						ActivityTaskStartedEventAttributes: &workflow.ActivityTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(22),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(27),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskCompleted),
						ActivityTaskCompletedEventAttributes: &workflow.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: common.Int64Ptr(21),
							StartedEventId:   common.Int64Ptr(24),
						},
					},
					{
						EventId:   common.Int64Ptr(28),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskScheduled),
						DecisionTaskScheduledEventAttributes: &workflow.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: common.Int32Ptr(100),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(29),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskStarted),
						DecisionTaskStartedEventAttributes: &workflow.DecisionTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(28),
						},
					},
				},
			},
			/////////////// reset point/////////////
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(30),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskCompleted),
						DecisionTaskCompletedEventAttributes: &workflow.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: common.Int64Ptr(28),
							StartedEventId:   common.Int64Ptr(29),
						},
					},
					{
						EventId:   common.Int64Ptr(31),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeTimerStarted),
						TimerStartedEventAttributes: &workflow.TimerStartedEventAttributes{
							TimerId:                      common.StringPtr(timerAfterReset),
							StartToFireTimeoutSeconds:    common.Int64Ptr(4),
							DecisionTaskCompletedEventId: common.Int64Ptr(30),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(32),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskStarted),
						ActivityTaskStartedEventAttributes: &workflow.ActivityTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(18),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(33),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeWorkflowExecutionSignaled),
						WorkflowExecutionSignaledEventAttributes: &workflow.WorkflowExecutionSignaledEventAttributes{
							SignalName: common.StringPtr(signalName1),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(34),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeWorkflowExecutionSignaled),
						WorkflowExecutionSignaledEventAttributes: &workflow.WorkflowExecutionSignaledEventAttributes{
							SignalName: common.StringPtr(signalName2),
						},
					},
				},
			},
		},
	}

	eid := int64(0)
	for _, be := range readHistoryResp.History {
		for _, e := range be.Events {
			eid++
			if e.GetEventId() != eid {
				s.Fail(fmt.Sprintf("inconintous eventID: %v, %v", eid, e.GetEventId()))
			}
			e.Timestamp = common.Int64Ptr(1000)
		}
	}

	newBranchToken := []byte("newBranch")
	forkResp := &p.ForkHistoryBranchResponse{
		NewBranchToken: newBranchToken,
	}

	completeReq := &p.CompleteForkBranchRequest{
		BranchToken: newBranchToken,
		Success:     true,
		ShardID:     common.IntPtr(s.shardID),
	}
	completeReqErr := &p.CompleteForkBranchRequest{
		BranchToken: newBranchToken,
		Success:     true,
		ShardID:     common.IntPtr(s.shardID),
	}

	appendV2Resp := &p.AppendHistoryNodesResponse{
		Size: 200,
	}

	s.mockExecutionMgr.On("GetWorkflowExecution", forkGwmsRequest).Return(forkGwmsResponse, nil).Once()
	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gcurResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", currGwmsRequest).Return(currGwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("ReadHistoryBranchByBatch", readHistoryReq).Return(readHistoryResp, nil).Once()
	s.mockHistoryV2Mgr.On("ForkHistoryBranch", mock.Anything).Return(forkResp, nil).Once()
	s.mockHistoryV2Mgr.On("CompleteForkBranch", completeReq).Return(nil).Once()
	s.mockHistoryV2Mgr.On("CompleteForkBranch", completeReqErr).Return(nil).Maybe()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(appendV2Resp, nil).Once()
	s.mockExecutionMgr.On("ResetWorkflowExecution", mock.Anything).Return(nil).Once()
	response, err := s.historyEngine.ResetWorkflowExecution(context.Background(), request)
	s.Nil(err)
	s.NotNil(response.RunId)

	// verify historyEvent: 5 events to append
	// 1. decisionFailed
	// 2. activityFailed
	// 3. signal 1
	// 4. signal 2
	// 5. decisionTaskScheduled
	calls := s.mockHistoryV2Mgr.Calls
	s.Equal(4, len(calls))
	appendCall := calls[2]
	s.Equal("AppendHistoryNodes", appendCall.Method)
	appendReq, ok := appendCall.Arguments[0].(*p.AppendHistoryNodesRequest)
	s.Equal(true, ok)
	s.Equal(newBranchToken, appendReq.BranchToken)
	s.Equal(false, appendReq.IsNewBranch)
	s.Equal(5, len(appendReq.Events))
	s.Equal(workflow.EventTypeDecisionTaskFailed, appendReq.Events[0].GetEventType())
	s.Equal(workflow.EventTypeActivityTaskFailed, appendReq.Events[1].GetEventType())
	s.Equal(workflow.EventTypeWorkflowExecutionSignaled, appendReq.Events[2].GetEventType())
	s.Equal(workflow.EventTypeWorkflowExecutionSignaled, appendReq.Events[3].GetEventType())
	s.Equal(workflow.EventTypeDecisionTaskScheduled, appendReq.Events[4].GetEventType())

	s.Equal(int64(30), appendReq.Events[0].GetEventId())
	s.Equal(int64(31), appendReq.Events[1].GetEventId())
	s.Equal(int64(32), appendReq.Events[2].GetEventId())
	s.Equal(int64(33), appendReq.Events[3].GetEventId())
	s.Equal(int64(34), appendReq.Events[4].GetEventId())

	// verify executionManager request
	calls = s.mockExecutionMgr.Calls
	s.Equal(4, len(calls))
	resetCall := calls[3]
	s.Equal("ResetWorkflowExecution", resetCall.Method)
	resetReq, ok := resetCall.Arguments[0].(*p.ResetWorkflowExecutionRequest)
	s.Equal(true, ok)
	s.Equal(false, resetReq.CurrentWorkflowMutation != nil)
	s.Equal(compareCurrExeInfo.RunID, resetReq.CurrentRunID)
	s.Equal(compareCurrExeInfo.NextEventID, resetReq.CurrentRunNextEventID)

	s.Equal("wfType", resetReq.NewWorkflowSnapshot.ExecutionInfo.WorkflowTypeName)
	s.True(len(resetReq.NewWorkflowSnapshot.ExecutionInfo.RunID) > 0)
	s.Equal([]byte(newBranchToken), resetReq.NewWorkflowSnapshot.ExecutionInfo.BranchToken)

	s.Equal(int64(34), resetReq.NewWorkflowSnapshot.ExecutionInfo.DecisionScheduleID)
	s.Equal(int64(35), resetReq.NewWorkflowSnapshot.ExecutionInfo.NextEventID)

	s.Equal(4, len(resetReq.NewWorkflowSnapshot.TransferTasks))
	s.Equal(p.TransferTaskTypeActivityTask, resetReq.NewWorkflowSnapshot.TransferTasks[0].GetType())
	s.Equal(p.TransferTaskTypeActivityTask, resetReq.NewWorkflowSnapshot.TransferTasks[1].GetType())
	s.Equal(p.TransferTaskTypeDecisionTask, resetReq.NewWorkflowSnapshot.TransferTasks[2].GetType())
	s.Equal(p.TransferTaskTypeRecordWorkflowStarted, resetReq.NewWorkflowSnapshot.TransferTasks[3].GetType())

	// WF timeout task, user timer, activity timeout timer, activity retry timer
	s.Equal(3, len(resetReq.NewWorkflowSnapshot.TimerTasks))
	s.Equal(p.TaskTypeWorkflowTimeout, resetReq.NewWorkflowSnapshot.TimerTasks[0].GetType())
	s.Equal(p.TaskTypeUserTimer, resetReq.NewWorkflowSnapshot.TimerTasks[1].GetType())
	s.Equal(p.TaskTypeActivityTimeout, resetReq.NewWorkflowSnapshot.TimerTasks[2].GetType())

	s.Equal(2, len(resetReq.NewWorkflowSnapshot.TimerInfos))
	s.assertTimerIDs([]string{timerUnfiredID1, timerUnfiredID2}, resetReq.NewWorkflowSnapshot.TimerInfos)

	s.Equal(2, len(resetReq.NewWorkflowSnapshot.ActivityInfos))
	s.assertActivityIDs([]string{actIDRetry, actIDNotStarted}, resetReq.NewWorkflowSnapshot.ActivityInfos)

	s.Equal(1, len(resetReq.NewWorkflowSnapshot.ReplicationTasks))
	s.Equal(p.ReplicationTaskTypeHistory, resetReq.NewWorkflowSnapshot.ReplicationTasks[0].GetType())

	compareRepState := copyReplicationState(forkRepState)
	compareRepState.StartVersion = beforeResetVersion
	compareRepState.CurrentVersion = afterResetVersion
	compareRepState.LastWriteEventID = 34
	compareRepState.LastWriteVersion = afterResetVersion
	compareRepState.LastReplicationInfo = map[string]*p.ReplicationInfo{
		"standby": &p.ReplicationInfo{
			LastEventID: 29,
			Version:     beforeResetVersion,
		},
	}
	s.Equal(compareRepState, resetReq.NewWorkflowSnapshot.ReplicationState)

	// not supported feature
	s.Empty(resetReq.NewWorkflowSnapshot.ChildExecutionInfos)
	s.Empty(resetReq.NewWorkflowSnapshot.SignalInfos)
	s.Empty(resetReq.NewWorkflowSnapshot.SignalRequestedIDs)
	s.Equal(0, len(resetReq.NewWorkflowSnapshot.RequestCancelInfos))
}

func (s *resetorSuite) TestApplyReset() {
	domainID := testDomainID
	beforeResetVersion := int64(100)
	afterResetVersion := int64(101)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", beforeResetVersion).Return(cluster.TestAlternativeClusterName)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", afterResetVersion).Return(cluster.TestCurrentClusterName)

	testDomainEntry := cache.NewGlobalDomainCacheEntryForTest(
		&p.DomainInfo{ID: testDomainID},
		&p.DomainConfig{Retention: 1},
		&p.DomainReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []*p.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		afterResetVersion,
		cluster.GetTestClusterMetadata(true, true),
	)
	// override domain cache
	s.mockDomainCache.On("GetDomainByID", mock.Anything).Return(testDomainEntry, nil)
	s.mockDomainCache.On("GetDomain", mock.Anything).Return(testDomainEntry, nil)
	s.mockEventsCache.On("putEvent", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return().Once()

	wid := "wId"
	wfType := "wfType"
	taskListName := "taskList"
	forkRunID := uuid.New().String()
	currRunID := uuid.New().String()
	newRunID := uuid.New().String()
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(wid),
		RunId:      common.StringPtr(newRunID),
	}

	forkGwmsRequest := &p.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(wid),
			RunId:      common.StringPtr(forkRunID),
		},
	}

	timerFiredID := "timerID0"
	timerUnfiredID1 := "timerID1"
	timerUnfiredID2 := "timerID2"
	actIDCompleted1 := "actID0"
	actIDCompleted2 := "actID1"
	actIDRetry := "actID2"          // not started, will reschedule
	actIDNotStarted := "actID3"     // not started, will reschedule
	actIDStartedNoRetry := "actID4" // started, will fail
	signalName1 := "sig1"
	signalName2 := "sig2"
	signalName3 := "sig3"
	signalName4 := "sig4"

	forkBranchToken := []byte("forkBranchToken")
	forkExeInfo := &p.WorkflowExecutionInfo{
		DomainID:           domainID,
		WorkflowID:         wid,
		WorkflowTypeName:   wfType,
		TaskList:           taskListName,
		RunID:              forkRunID,
		BranchToken:        forkBranchToken,
		NextEventID:        35,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
	}

	forkRepState := &p.ReplicationState{
		CurrentVersion:      beforeResetVersion,
		StartVersion:        beforeResetVersion,
		LastWriteEventID:    common.EmptyEventID,
		LastWriteVersion:    common.EmptyVersion,
		LastReplicationInfo: map[string]*p.ReplicationInfo{},
	}
	forkGwmsResponse := &p.GetWorkflowExecutionResponse{State: &p.WorkflowMutableState{
		ExecutionInfo:    forkExeInfo,
		ExecutionStats:   &p.ExecutionStats{},
		ReplicationState: forkRepState,
	}}

	currGwmsRequest := &p.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(wid),
			RunId:      common.StringPtr(currRunID),
		},
	}
	currExeInfo := &p.WorkflowExecutionInfo{
		DomainID:           domainID,
		WorkflowID:         wid,
		WorkflowTypeName:   wfType,
		TaskList:           taskListName,
		RunID:              currRunID,
		NextEventID:        common.FirstEventID,
		State:              p.WorkflowStateCompleted,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
	}
	compareCurrExeInfo := copyWorkflowExecutionInfo(currExeInfo)
	currGwmsResponse := &p.GetWorkflowExecutionResponse{State: &p.WorkflowMutableState{
		ExecutionInfo:    currExeInfo,
		ExecutionStats:   &p.ExecutionStats{},
		ReplicationState: forkRepState,
	}}

	readHistoryReq := &p.ReadHistoryBranchRequest{
		BranchToken:   forkBranchToken,
		MinEventID:    common.FirstEventID,
		MaxEventID:    int64(30),
		PageSize:      defaultHistoryPageSize,
		NextPageToken: nil,
		ShardID:       common.IntPtr(s.shardID),
	}

	taskList := &workflow.TaskList{
		Name: common.StringPtr(taskListName),
	}

	readHistoryResp := &p.ReadHistoryBranchByBatchResponse{
		NextPageToken:    nil,
		Size:             1000,
		LastFirstEventID: int64(31),
		History: []*workflow.History{
			&workflow.History{
				Events: []*workflow.HistoryEvent{
					&workflow.HistoryEvent{
						EventId:   common.Int64Ptr(1),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeWorkflowExecutionStarted),
						WorkflowExecutionStartedEventAttributes: &workflow.WorkflowExecutionStartedEventAttributes{
							WorkflowType: &workflow.WorkflowType{
								Name: common.StringPtr(wfType),
							},
							TaskList:                            taskList,
							Input:                               []byte("testInput"),
							ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
							TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(200),
						},
					},
					{
						EventId:   common.Int64Ptr(2),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskScheduled),
						DecisionTaskScheduledEventAttributes: &workflow.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: common.Int32Ptr(100),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(3),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskStarted),
						DecisionTaskStartedEventAttributes: &workflow.DecisionTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(2),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(4),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskCompleted),
						DecisionTaskCompletedEventAttributes: &workflow.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: common.Int64Ptr(2),
							StartedEventId:   common.Int64Ptr(3),
						},
					},
					{
						EventId:   common.Int64Ptr(5),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeMarkerRecorded),
						MarkerRecordedEventAttributes: &workflow.MarkerRecordedEventAttributes{
							MarkerName:                   common.StringPtr("Version"),
							Details:                      []byte("details"),
							DecisionTaskCompletedEventId: common.Int64Ptr(4),
						},
					},
					{
						EventId:   common.Int64Ptr(6),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskScheduled),
						ActivityTaskScheduledEventAttributes: &workflow.ActivityTaskScheduledEventAttributes{
							ActivityId: common.StringPtr(actIDCompleted1),
							ActivityType: &workflow.ActivityType{
								Name: common.StringPtr("actType0"),
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1000),
							ScheduleToStartTimeoutSeconds: common.Int32Ptr(2000),
							StartToCloseTimeoutSeconds:    common.Int32Ptr(3000),
							HeartbeatTimeoutSeconds:       common.Int32Ptr(4000),
							DecisionTaskCompletedEventId:  common.Int64Ptr(4),
						},
					},
					{
						EventId:   common.Int64Ptr(7),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeTimerStarted),
						TimerStartedEventAttributes: &workflow.TimerStartedEventAttributes{
							TimerId:                      common.StringPtr(timerFiredID),
							StartToFireTimeoutSeconds:    common.Int64Ptr(2),
							DecisionTaskCompletedEventId: common.Int64Ptr(4),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(8),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskStarted),
						ActivityTaskStartedEventAttributes: &workflow.ActivityTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(6),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(9),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskCompleted),
						ActivityTaskCompletedEventAttributes: &workflow.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: common.Int64Ptr(6),
							StartedEventId:   common.Int64Ptr(8),
						},
					},
					{
						EventId:   common.Int64Ptr(10),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskScheduled),
						DecisionTaskScheduledEventAttributes: &workflow.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: common.Int32Ptr(100),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(11),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskStarted),
						DecisionTaskStartedEventAttributes: &workflow.DecisionTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(10),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(12),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskCompleted),
						DecisionTaskCompletedEventAttributes: &workflow.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: common.Int64Ptr(10),
							StartedEventId:   common.Int64Ptr(11),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(13),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeTimerFired),
						TimerFiredEventAttributes: &workflow.TimerFiredEventAttributes{
							TimerId: common.StringPtr(timerFiredID),
						},
					},
					{
						EventId:   common.Int64Ptr(14),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskScheduled),
						DecisionTaskScheduledEventAttributes: &workflow.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: common.Int32Ptr(100),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(15),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskStarted),
						DecisionTaskStartedEventAttributes: &workflow.DecisionTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(14),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(16),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskCompleted),
						DecisionTaskCompletedEventAttributes: &workflow.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: common.Int64Ptr(14),
							StartedEventId:   common.Int64Ptr(15),
						},
					},
					{
						EventId:   common.Int64Ptr(17),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskScheduled),
						ActivityTaskScheduledEventAttributes: &workflow.ActivityTaskScheduledEventAttributes{
							ActivityId: common.StringPtr(actIDRetry),
							ActivityType: &workflow.ActivityType{
								Name: common.StringPtr("actType1"),
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1000),
							ScheduleToStartTimeoutSeconds: common.Int32Ptr(2000),
							StartToCloseTimeoutSeconds:    common.Int32Ptr(3000),
							HeartbeatTimeoutSeconds:       common.Int32Ptr(4000),
							DecisionTaskCompletedEventId:  common.Int64Ptr(16),
							RetryPolicy: &workflow.RetryPolicy{
								InitialIntervalInSeconds:    common.Int32Ptr(1),
								BackoffCoefficient:          common.Float64Ptr(0.2),
								MaximumAttempts:             common.Int32Ptr(10),
								MaximumIntervalInSeconds:    common.Int32Ptr(1000),
								ExpirationIntervalInSeconds: common.Int32Ptr(math.MaxInt32),
							},
						},
					},
					{
						EventId:   common.Int64Ptr(18),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskScheduled),
						ActivityTaskScheduledEventAttributes: &workflow.ActivityTaskScheduledEventAttributes{
							ActivityId: common.StringPtr(actIDNotStarted),
							ActivityType: &workflow.ActivityType{
								Name: common.StringPtr("actType2"),
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1000),
							ScheduleToStartTimeoutSeconds: common.Int32Ptr(2000),
							StartToCloseTimeoutSeconds:    common.Int32Ptr(3000),
							HeartbeatTimeoutSeconds:       common.Int32Ptr(4000),
							DecisionTaskCompletedEventId:  common.Int64Ptr(16),
						},
					},
					{
						EventId:   common.Int64Ptr(19),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeTimerStarted),
						TimerStartedEventAttributes: &workflow.TimerStartedEventAttributes{
							TimerId:                      common.StringPtr(timerUnfiredID1),
							StartToFireTimeoutSeconds:    common.Int64Ptr(4),
							DecisionTaskCompletedEventId: common.Int64Ptr(16),
						},
					},
					{
						EventId:   common.Int64Ptr(20),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeTimerStarted),
						TimerStartedEventAttributes: &workflow.TimerStartedEventAttributes{
							TimerId:                      common.StringPtr(timerUnfiredID2),
							StartToFireTimeoutSeconds:    common.Int64Ptr(8),
							DecisionTaskCompletedEventId: common.Int64Ptr(16),
						},
					},
					{
						EventId:   common.Int64Ptr(21),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskScheduled),
						ActivityTaskScheduledEventAttributes: &workflow.ActivityTaskScheduledEventAttributes{
							ActivityId: common.StringPtr(actIDCompleted2),
							ActivityType: &workflow.ActivityType{
								Name: common.StringPtr("actType2"),
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1000),
							ScheduleToStartTimeoutSeconds: common.Int32Ptr(2000),
							StartToCloseTimeoutSeconds:    common.Int32Ptr(3000),
							HeartbeatTimeoutSeconds:       common.Int32Ptr(4000),
							DecisionTaskCompletedEventId:  common.Int64Ptr(16),
						},
					},
					{
						EventId:   common.Int64Ptr(22),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskScheduled),
						ActivityTaskScheduledEventAttributes: &workflow.ActivityTaskScheduledEventAttributes{
							ActivityId: common.StringPtr(actIDStartedNoRetry),
							ActivityType: &workflow.ActivityType{
								Name: common.StringPtr("actType2"),
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1000),
							ScheduleToStartTimeoutSeconds: common.Int32Ptr(2000),
							StartToCloseTimeoutSeconds:    common.Int32Ptr(3000),
							HeartbeatTimeoutSeconds:       common.Int32Ptr(4000),
							DecisionTaskCompletedEventId:  common.Int64Ptr(16),
						},
					},
					{
						EventId:   common.Int64Ptr(23),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeWorkflowExecutionSignaled),
						WorkflowExecutionSignaledEventAttributes: &workflow.WorkflowExecutionSignaledEventAttributes{
							SignalName: common.StringPtr(signalName3),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(24),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskStarted),
						ActivityTaskStartedEventAttributes: &workflow.ActivityTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(21),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(25),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeWorkflowExecutionSignaled),
						WorkflowExecutionSignaledEventAttributes: &workflow.WorkflowExecutionSignaledEventAttributes{
							SignalName: common.StringPtr(signalName4),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(26),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskStarted),
						ActivityTaskStartedEventAttributes: &workflow.ActivityTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(22),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(27),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeActivityTaskCompleted),
						ActivityTaskCompletedEventAttributes: &workflow.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: common.Int64Ptr(21),
							StartedEventId:   common.Int64Ptr(24),
						},
					},
					{
						EventId:   common.Int64Ptr(28),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskScheduled),
						DecisionTaskScheduledEventAttributes: &workflow.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: common.Int32Ptr(100),
						},
					},
				},
			},
			{
				Events: []*workflow.HistoryEvent{
					{
						EventId:   common.Int64Ptr(29),
						Version:   common.Int64Ptr(beforeResetVersion),
						EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskStarted),
						DecisionTaskStartedEventAttributes: &workflow.DecisionTaskStartedEventAttributes{
							ScheduledEventId: common.Int64Ptr(28),
						},
					},
				},
			},
			/////////////// reset point/////////////
		},
	}

	eid := int64(0)
	for _, be := range readHistoryResp.History {
		for _, e := range be.Events {
			eid++
			if e.GetEventId() != eid {
				s.Fail(fmt.Sprintf("inconintous eventID: %v, %v", eid, e.GetEventId()))
			}
			e.Timestamp = common.Int64Ptr(1000)
		}
	}

	newBranchToken := []byte("newBranch")
	forkReq := &p.ForkHistoryBranchRequest{
		ForkBranchToken: forkBranchToken,
		ForkNodeID:      30,
		Info:            p.BuildHistoryGarbageCleanupInfo(domainID, wid, newRunID),
		ShardID:         common.IntPtr(s.shardID),
	}
	forkResp := &p.ForkHistoryBranchResponse{
		NewBranchToken: newBranchToken,
	}

	completeReq := &p.CompleteForkBranchRequest{
		BranchToken: newBranchToken,
		Success:     true,
		ShardID:     common.IntPtr(s.shardID),
	}
	completeReqErr := &p.CompleteForkBranchRequest{
		BranchToken: newBranchToken,
		Success:     true,
		ShardID:     common.IntPtr(s.shardID),
	}

	historyAfterReset := &workflow.History{
		Events: []*workflow.HistoryEvent{
			&workflow.HistoryEvent{
				EventId:   common.Int64Ptr(30),
				Version:   common.Int64Ptr(afterResetVersion),
				EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskFailed),
				DecisionTaskFailedEventAttributes: &workflow.DecisionTaskFailedEventAttributes{
					ScheduledEventId: common.Int64Ptr(int64(28)),
					StartedEventId:   common.Int64Ptr(int64(29)),
					Cause:            common.DecisionTaskFailedCausePtr(workflow.DecisionTaskFailedCauseResetWorkflow),
					Details:          nil,
					Identity:         common.StringPtr(identityHistoryService),
					Reason:           common.StringPtr("resetWFtest"),
					BaseRunId:        common.StringPtr(forkRunID),
					NewRunId:         common.StringPtr(newRunID),
					ForkEventVersion: common.Int64Ptr(beforeResetVersion),
				},
			},
			{
				EventId:   common.Int64Ptr(31),
				Version:   common.Int64Ptr(afterResetVersion),
				EventType: common.EventTypePtr(workflow.EventTypeActivityTaskFailed),
				ActivityTaskFailedEventAttributes: &workflow.ActivityTaskFailedEventAttributes{
					Reason:           common.StringPtr("resetWF"),
					ScheduledEventId: common.Int64Ptr(22),
					StartedEventId:   common.Int64Ptr(26),
					Identity:         common.StringPtr(identityHistoryService),
				},
			},
			{
				EventId:   common.Int64Ptr(32),
				Version:   common.Int64Ptr(afterResetVersion),
				EventType: common.EventTypePtr(workflow.EventTypeWorkflowExecutionSignaled),
				WorkflowExecutionSignaledEventAttributes: &workflow.WorkflowExecutionSignaledEventAttributes{
					SignalName: common.StringPtr(signalName1),
				},
			},
			{
				EventId:   common.Int64Ptr(33),
				Version:   common.Int64Ptr(afterResetVersion),
				EventType: common.EventTypePtr(workflow.EventTypeWorkflowExecutionSignaled),
				WorkflowExecutionSignaledEventAttributes: &workflow.WorkflowExecutionSignaledEventAttributes{
					SignalName: common.StringPtr(signalName2),
				},
			},
			{
				EventId:   common.Int64Ptr(34),
				Version:   common.Int64Ptr(afterResetVersion),
				EventType: common.EventTypePtr(workflow.EventTypeDecisionTaskScheduled),
				DecisionTaskScheduledEventAttributes: &workflow.DecisionTaskScheduledEventAttributes{
					TaskList:                   taskList,
					StartToCloseTimeoutSeconds: common.Int32Ptr(100),
				},
			},
		},
	}

	appendV2Resp := &p.AppendHistoryNodesResponse{
		Size: 200,
	}

	request := &h.ReplicateEventsRequest{
		SourceCluster:     common.StringPtr("standby"),
		DomainUUID:        common.StringPtr(domainID),
		WorkflowExecution: &we,
		FirstEventId:      common.Int64Ptr(30),
		NextEventId:       common.Int64Ptr(35),
		History:           historyAfterReset,
	}

	s.mockExecutionMgr.On("GetWorkflowExecution", forkGwmsRequest).Return(forkGwmsResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", currGwmsRequest).Return(currGwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("ReadHistoryBranchByBatch", readHistoryReq).Return(readHistoryResp, nil).Once()
	s.mockHistoryV2Mgr.On("ForkHistoryBranch", forkReq).Return(forkResp, nil).Once()
	s.mockHistoryV2Mgr.On("CompleteForkBranch", completeReq).Return(nil).Once()
	s.mockHistoryV2Mgr.On("CompleteForkBranch", completeReqErr).Return(nil).Maybe()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(appendV2Resp, nil).Once()
	s.mockExecutionMgr.On("ResetWorkflowExecution", mock.Anything).Return(nil).Once()
	err := s.resetor.ApplyResetEvent(context.Background(), request, domainID, wid, currRunID)
	s.Nil(err)

	// verify historyEvent: 5 events to append
	// 1. decisionFailed
	// 2. activityFailed
	// 3. signal 1
	// 4. signal 2
	// 5. decisionTaskScheduled
	calls := s.mockHistoryV2Mgr.Calls
	s.Equal(4, len(calls))
	appendCall := calls[2]
	s.Equal("AppendHistoryNodes", appendCall.Method)
	appendReq, ok := appendCall.Arguments[0].(*p.AppendHistoryNodesRequest)
	s.Equal(true, ok)
	s.Equal(newBranchToken, appendReq.BranchToken)
	s.Equal(false, appendReq.IsNewBranch)
	s.Equal(5, len(appendReq.Events))
	s.Equal(workflow.EventTypeDecisionTaskFailed, appendReq.Events[0].GetEventType())
	s.Equal(workflow.EventTypeActivityTaskFailed, appendReq.Events[1].GetEventType())
	s.Equal(workflow.EventTypeWorkflowExecutionSignaled, appendReq.Events[2].GetEventType())
	s.Equal(workflow.EventTypeWorkflowExecutionSignaled, appendReq.Events[3].GetEventType())
	s.Equal(workflow.EventTypeDecisionTaskScheduled, appendReq.Events[4].GetEventType())

	s.Equal(int64(30), appendReq.Events[0].GetEventId())
	s.Equal(int64(31), appendReq.Events[1].GetEventId())
	s.Equal(int64(32), appendReq.Events[2].GetEventId())
	s.Equal(int64(33), appendReq.Events[3].GetEventId())
	s.Equal(int64(34), appendReq.Events[4].GetEventId())

	s.Equal(common.EncodingType(s.config.EventEncodingType(domainID)), appendReq.Encoding)

	// verify executionManager request
	calls = s.mockExecutionMgr.Calls
	s.Equal(3, len(calls))
	resetCall := calls[2]
	s.Equal("ResetWorkflowExecution", resetCall.Method)
	resetReq, ok := resetCall.Arguments[0].(*p.ResetWorkflowExecutionRequest)
	s.Equal(true, ok)
	s.Equal(false, resetReq.CurrentWorkflowMutation != nil)
	s.Equal(compareCurrExeInfo.RunID, resetReq.CurrentRunID)
	s.Equal(compareCurrExeInfo.NextEventID, resetReq.CurrentRunNextEventID)

	s.Equal("wfType", resetReq.NewWorkflowSnapshot.ExecutionInfo.WorkflowTypeName)
	s.True(len(resetReq.NewWorkflowSnapshot.ExecutionInfo.RunID) > 0)
	s.Equal([]byte(newBranchToken), resetReq.NewWorkflowSnapshot.ExecutionInfo.BranchToken)

	s.Equal(int64(34), resetReq.NewWorkflowSnapshot.ExecutionInfo.DecisionScheduleID)
	s.Equal(int64(35), resetReq.NewWorkflowSnapshot.ExecutionInfo.NextEventID)

	s.Equal(3, len(resetReq.NewWorkflowSnapshot.TransferTasks))
	s.Equal(p.TransferTaskTypeActivityTask, resetReq.NewWorkflowSnapshot.TransferTasks[0].GetType())
	s.Equal(p.TransferTaskTypeActivityTask, resetReq.NewWorkflowSnapshot.TransferTasks[1].GetType())
	s.Equal(p.TransferTaskTypeDecisionTask, resetReq.NewWorkflowSnapshot.TransferTasks[2].GetType())

	// WF timeout task, user timer, activity timeout timer, activity retry timer
	s.Equal(3, len(resetReq.NewWorkflowSnapshot.TimerTasks))
	s.Equal(p.TaskTypeWorkflowTimeout, resetReq.NewWorkflowSnapshot.TimerTasks[0].GetType())
	s.Equal(p.TaskTypeUserTimer, resetReq.NewWorkflowSnapshot.TimerTasks[1].GetType())
	s.Equal(p.TaskTypeActivityTimeout, resetReq.NewWorkflowSnapshot.TimerTasks[2].GetType())

	s.Equal(2, len(resetReq.NewWorkflowSnapshot.TimerInfos))
	s.assertTimerIDs([]string{timerUnfiredID1, timerUnfiredID2}, resetReq.NewWorkflowSnapshot.TimerInfos)

	s.Equal(2, len(resetReq.NewWorkflowSnapshot.ActivityInfos))
	s.assertActivityIDs([]string{actIDRetry, actIDNotStarted}, resetReq.NewWorkflowSnapshot.ActivityInfos)

	compareRepState := copyReplicationState(forkRepState)
	compareRepState.StartVersion = beforeResetVersion
	compareRepState.CurrentVersion = afterResetVersion
	compareRepState.LastWriteEventID = 34
	compareRepState.LastWriteVersion = afterResetVersion
	compareRepState.LastReplicationInfo = map[string]*p.ReplicationInfo{
		"standby": &p.ReplicationInfo{
			LastEventID: 29,
			Version:     beforeResetVersion,
		},
	}
	s.Equal(compareRepState, resetReq.NewWorkflowSnapshot.ReplicationState)

	s.Equal(0, len(resetReq.NewWorkflowSnapshot.ReplicationTasks))
	// not supported feature
	s.Empty(resetReq.NewWorkflowSnapshot.ChildExecutionInfos)
	s.Empty(resetReq.NewWorkflowSnapshot.SignalInfos)
	s.Empty(resetReq.NewWorkflowSnapshot.SignalRequestedIDs)
	s.Equal(0, len(resetReq.NewWorkflowSnapshot.RequestCancelInfos))
}

func TestFindAutoResetPoint(t *testing.T) {
	timeSource := clock.NewRealTimeSource()

	// case 1: nil
	_, pt := FindAutoResetPoint(timeSource, nil, nil)
	assert.Nil(t, pt)

	// case 2: empty
	_, pt = FindAutoResetPoint(timeSource, &workflow.BadBinaries{}, &workflow.ResetPoints{})
	assert.Nil(t, pt)

	pt0 := &workflow.ResetPointInfo{
		BinaryChecksum: common.StringPtr("abc"),
		Resettable:     common.BoolPtr(true),
	}
	pt1 := &workflow.ResetPointInfo{
		BinaryChecksum: common.StringPtr("def"),
		Resettable:     common.BoolPtr(true),
	}
	pt3 := &workflow.ResetPointInfo{
		BinaryChecksum: common.StringPtr("ghi"),
		Resettable:     common.BoolPtr(false),
	}

	expiredNowNano := time.Now().UnixNano() - int64(time.Hour)
	notExpiredNowNano := time.Now().UnixNano() + int64(time.Hour)
	pt4 := &workflow.ResetPointInfo{
		BinaryChecksum:   common.StringPtr("expired"),
		Resettable:       common.BoolPtr(true),
		ExpiringTimeNano: common.Int64Ptr(expiredNowNano),
	}

	pt5 := &workflow.ResetPointInfo{
		BinaryChecksum:   common.StringPtr("notExpired"),
		Resettable:       common.BoolPtr(true),
		ExpiringTimeNano: common.Int64Ptr(notExpiredNowNano),
	}

	// case 3: two intersection
	_, pt = FindAutoResetPoint(timeSource, &workflow.BadBinaries{
		Binaries: map[string]*workflow.BadBinaryInfo{
			"abc": {},
			"def": {},
		},
	}, &workflow.ResetPoints{
		Points: []*workflow.ResetPointInfo{
			pt0, pt1, pt3,
		},
	})
	assert.Equal(t, pt.String(), pt0.String())

	// case 4: one intersection
	_, pt = FindAutoResetPoint(timeSource, &workflow.BadBinaries{
		Binaries: map[string]*workflow.BadBinaryInfo{
			"none":    {},
			"def":     {},
			"expired": {},
		},
	}, &workflow.ResetPoints{
		Points: []*workflow.ResetPointInfo{
			pt4, pt0, pt1, pt3,
		},
	})
	assert.Equal(t, pt.String(), pt1.String())

	// case 4: no intersection
	_, pt = FindAutoResetPoint(timeSource, &workflow.BadBinaries{
		Binaries: map[string]*workflow.BadBinaryInfo{
			"none1": {},
			"none2": {},
		},
	}, &workflow.ResetPoints{
		Points: []*workflow.ResetPointInfo{
			pt0, pt1, pt3,
		},
	})
	assert.Nil(t, pt)

	// case 5: not resettable
	_, pt = FindAutoResetPoint(timeSource, &workflow.BadBinaries{
		Binaries: map[string]*workflow.BadBinaryInfo{
			"none1": {},
			"ghi":   {},
		},
	}, &workflow.ResetPoints{
		Points: []*workflow.ResetPointInfo{
			pt0, pt1, pt3,
		},
	})
	assert.Nil(t, pt)

	// case 6: one intersection of expired
	_, pt = FindAutoResetPoint(timeSource, &workflow.BadBinaries{
		Binaries: map[string]*workflow.BadBinaryInfo{
			"none":    {},
			"expired": {},
		},
	}, &workflow.ResetPoints{
		Points: []*workflow.ResetPointInfo{
			pt0, pt1, pt3, pt4, pt5,
		},
	})
	assert.Nil(t, pt)

	// case 7: one intersection of not expired
	_, pt = FindAutoResetPoint(timeSource, &workflow.BadBinaries{
		Binaries: map[string]*workflow.BadBinaryInfo{
			"none":       {},
			"notExpired": {},
		},
	}, &workflow.ResetPoints{
		Points: []*workflow.ResetPointInfo{
			pt0, pt1, pt3, pt4, pt5,
		},
	})
	assert.Equal(t, pt.String(), pt5.String())
}
