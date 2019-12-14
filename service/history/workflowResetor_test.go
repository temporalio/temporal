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
	"go.uber.org/cadence/.gen/go/shared"

	h "github.com/uber/cadence/.gen/go/history"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/mocks"
	p "github.com/uber/cadence/common/persistence"
)

type (
	resetorSuite struct {
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

		mockExecutionMgr *mocks.ExecutionManager
		mockHistoryV2Mgr *mocks.HistoryV2Manager

		config  *Config
		logger  log.Logger
		shardID int

		historyEngine *historyEngineImpl
		resetor       workflowResetor
	}
)

func TestWorkflowResetorSuite(t *testing.T) {
	s := new(resetorSuite)
	suite.Run(t, s)
}

func (s *resetorSuite) SetupSuite() {
	s.config = NewDynamicConfigForTest()
}

func (s *resetorSuite) TearDownSuite() {
}

func (s *resetorSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	shardID := 10
	s.shardID = shardID

	s.controller = gomock.NewController(s.T())
	s.mockTxProcessor = NewMocktransferQueueProcessor(s.controller)
	s.mockReplicationProcessor = NewMockReplicatorQueueProcessor(s.controller)
	s.mockTimerProcessor = NewMocktimerQueueProcessor(s.controller)
	s.mockTxProcessor.EXPECT().NotifyNewTask(gomock.Any(), gomock.Any()).AnyTimes()
	s.mockReplicationProcessor.EXPECT().notifyNewTask().AnyTimes()
	s.mockTimerProcessor.EXPECT().NotifyNewTimers(gomock.Any(), gomock.Any()).AnyTimes()

	s.mockShard = newTestShardContext(
		s.controller,
		&p.ShardInfo{
			ShardID:          shardID,
			RangeID:          1,
			TransferAckLevel: 0,
		},
		s.config,
	)

	s.mockExecutionMgr = s.mockShard.resource.ExecutionMgr
	s.mockHistoryV2Mgr = s.mockShard.resource.HistoryMgr
	s.mockDomainCache = s.mockShard.resource.DomainCache
	s.mockClusterMetadata = s.mockShard.resource.ClusterMetadata
	s.mockEventsCache = s.mockShard.mockEventsCache
	s.mockClusterMetadata.EXPECT().IsGlobalDomainEnabled().Return(true).AnyTimes()
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
		metricsClient:        s.mockShard.GetMetricsClient(),
		tokenSerializer:      common.NewJSONTaskTokenSerializer(),
		config:               s.config,
		historyEventNotifier: newHistoryEventNotifier(clock.NewRealTimeSource(), s.mockShard.GetMetricsClient(), func(string) int { return 0 }),
		txProcessor:          s.mockTxProcessor,
		replicatorProcessor:  s.mockReplicationProcessor,
		timerProcessor:       s.mockTimerProcessor,
	}
	s.mockShard.SetEngine(h)
	s.resetor = newWorkflowResetor(h)
	h.resetor = s.resetor
	s.historyEngine = h
}

func (s *resetorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.Finish(s.T())
}

func (s *resetorSuite) TestResetWorkflowExecution_NoReplication() {
	testDomainEntry := cache.NewLocalDomainCacheEntryForTest(
		&p.DomainInfo{ID: testDomainID}, &p.DomainConfig{Retention: 1}, "", nil,
	)
	s.mockDomainCache.EXPECT().GetDomainByID(gomock.Any()).Return(testDomainEntry, nil).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomain(gomock.Any()).Return(testDomainEntry, nil).AnyTimes()

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

	appendV2Resp := &p.AppendHistoryNodesResponse{
		Size: 200,
	}

	s.mockExecutionMgr.On("GetWorkflowExecution", forkGwmsRequest).Return(forkGwmsResponse, nil).Once()
	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gcurResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", currGwmsRequest).Return(currGwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("ReadHistoryBranchByBatch", readHistoryReq).Return(readHistoryResp, nil).Once()
	s.mockHistoryV2Mgr.On("ForkHistoryBranch", mock.Anything).Return(forkResp, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(appendV2Resp, nil).Times(2)
	s.mockExecutionMgr.On("ResetWorkflowExecution", mock.Anything).Return(nil).Once()
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
	s.Equal(4, len(calls))
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
	s.mockDomainCache.EXPECT().GetDomainByID(gomock.Any()).Return(testDomainEntry, nil).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomain(gomock.Any()).Return(testDomainEntry, nil).AnyTimes()

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

	_, err := s.historyEngine.ResetWorkflowExecution(context.Background(), request)
	s.EqualError(err, "BadRequestError{Message: it is not allowed resetting to a point that workflow has pending request cancel.}")
}

func (s *resetorSuite) TestResetWorkflowExecution_Replication_WithTerminatingCurrent() {
	domainName := "testDomainName"
	beforeResetVersion := int64(100)
	afterResetVersion := int64(101)
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(beforeResetVersion).Return("standby").AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(afterResetVersion).Return("active").AnyTimes()

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
	s.mockDomainCache.EXPECT().GetDomainByID(gomock.Any()).Return(testDomainEntry, nil).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomain(gomock.Any()).Return(testDomainEntry, nil).AnyTimes()

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

	appendV2Resp := &p.AppendHistoryNodesResponse{
		Size: 200,
	}

	s.mockExecutionMgr.On("GetWorkflowExecution", forkGwmsRequest).Return(forkGwmsResponse, nil).Once()
	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gcurResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", currGwmsRequest).Return(currGwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("ReadHistoryBranchByBatch", readHistoryReq).Return(readHistoryResp, nil).Once()
	s.mockHistoryV2Mgr.On("ForkHistoryBranch", mock.Anything).Return(forkResp, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(appendV2Resp, nil).Times(2)
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
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(beforeResetVersion).Return("active").AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(afterResetVersion).Return("standby").AnyTimes()

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
	s.mockDomainCache.EXPECT().GetDomainByID(gomock.Any()).Return(testDomainEntry, nil).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomain(gomock.Any()).Return(testDomainEntry, nil).AnyTimes()

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

	s.mockExecutionMgr.On("GetWorkflowExecution", forkGwmsRequest).Return(forkGwmsResponse, nil).Once()
	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gcurResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", currGwmsRequest).Return(currGwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("ReadHistoryBranchByBatch", readHistoryReq).Return(readHistoryResp, nil).Once()
	s.mockHistoryV2Mgr.On("ForkHistoryBranch", mock.Anything).Return(forkResp, nil).Once()

	_, err := s.historyEngine.ResetWorkflowExecution(context.Background(), request)
	s.IsType(&shared.DomainNotActiveError{}, err)
}

func (s *resetorSuite) TestResetWorkflowExecution_Replication_NoTerminatingCurrent() {
	domainName := "testDomainName"
	beforeResetVersion := int64(100)
	afterResetVersion := int64(101)
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(beforeResetVersion).Return("standby").AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(afterResetVersion).Return("active").AnyTimes()

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
	s.mockDomainCache.EXPECT().GetDomainByID(gomock.Any()).Return(testDomainEntry, nil).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomain(gomock.Any()).Return(testDomainEntry, nil).AnyTimes()

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

	appendV2Resp := &p.AppendHistoryNodesResponse{
		Size: 200,
	}

	s.mockExecutionMgr.On("GetWorkflowExecution", forkGwmsRequest).Return(forkGwmsResponse, nil).Once()
	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gcurResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", currGwmsRequest).Return(currGwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("ReadHistoryBranchByBatch", readHistoryReq).Return(readHistoryResp, nil).Once()
	s.mockHistoryV2Mgr.On("ForkHistoryBranch", mock.Anything).Return(forkResp, nil).Once()
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
	s.Equal(3, len(calls))
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
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(beforeResetVersion).Return(cluster.TestAlternativeClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(afterResetVersion).Return(cluster.TestCurrentClusterName).AnyTimes()

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
	s.mockDomainCache.EXPECT().GetDomainByID(gomock.Any()).Return(testDomainEntry, nil).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomain(gomock.Any()).Return(testDomainEntry, nil).AnyTimes()

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
	s.Equal(3, len(calls))
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
