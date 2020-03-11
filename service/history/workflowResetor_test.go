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
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/serviceerror"
	"go.temporal.io/temporal-proto/workflowservice"

	"github.com/temporalio/temporal/.gen/proto/historyservice"
	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/.gen/proto/replication"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/clock"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/mocks"
	"github.com/temporalio/temporal/common/persistence"
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
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistenceblobs.ShardInfo{
				ShardID:          int32(shardID),
				RangeID:          1,
				TransferAckLevel: 0,
			}},
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
		tokenSerializer:      common.NewProtoTaskTokenSerializer(),
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
		&persistence.DomainInfo{ID: testDomainID}, &persistence.DomainConfig{Retention: 1}, "", nil,
	)
	s.mockDomainCache.EXPECT().GetDomainByID(gomock.Any()).Return(testDomainEntry, nil).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomain(gomock.Any()).Return(testDomainEntry, nil).AnyTimes()

	request := &historyservice.ResetWorkflowExecutionRequest{}
	domainID := testDomainID
	request.DomainUUID = domainID
	request.ResetRequest = &workflowservice.ResetWorkflowExecutionRequest{}

	wid := "wId"
	wfType := "wfType"
	taskListName := "taskList"
	forkRunID := uuid.New().String()
	currRunID := uuid.New().String()
	we := commonproto.WorkflowExecution{
		WorkflowId: wid,
		RunId:      forkRunID,
	}
	request.ResetRequest = &workflowservice.ResetWorkflowExecutionRequest{
		Domain:                "testDomainName",
		WorkflowExecution:     &we,
		Reason:                "test reset",
		DecisionFinishEventId: 29,
		RequestId:             uuid.New().String(),
	}

	forkGwmsRequest := &persistence.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: commonproto.WorkflowExecution{
			WorkflowId: wid,
			RunId:      forkRunID,
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
	forkExeInfo := &persistence.WorkflowExecutionInfo{
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
	forkGwmsResponse := &persistence.GetWorkflowExecutionResponse{State: &persistence.WorkflowMutableState{
		ExecutionInfo:  forkExeInfo,
		ExecutionStats: &persistence.ExecutionStats{},
	}}

	currGwmsRequest := &persistence.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: commonproto.WorkflowExecution{
			WorkflowId: wid,
			RunId:      currRunID,
		},
	}
	currExeInfo := &persistence.WorkflowExecutionInfo{
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
	currGwmsResponse := &persistence.GetWorkflowExecutionResponse{State: &persistence.WorkflowMutableState{
		ExecutionInfo:  currExeInfo,
		ExecutionStats: &persistence.ExecutionStats{},
	}}

	gcurResponse := &persistence.GetCurrentExecutionResponse{
		RunID: currRunID,
	}

	readHistoryReq := &persistence.ReadHistoryBranchRequest{
		BranchToken:   forkBranchToken,
		MinEventID:    common.FirstEventID,
		MaxEventID:    int64(34),
		PageSize:      defaultHistoryPageSize,
		NextPageToken: nil,
		ShardID:       &s.shardID,
	}

	taskList := &commonproto.TaskList{
		Name: taskListName,
	}
	readHistoryResp := &persistence.ReadHistoryBranchByBatchResponse{
		NextPageToken:    nil,
		Size:             1000,
		LastFirstEventID: int64(31),
		History: []*commonproto.History{
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   1,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeWorkflowExecutionStarted,
						Attributes: &commonproto.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &commonproto.WorkflowExecutionStartedEventAttributes{
							WorkflowType: &commonproto.WorkflowType{
								Name: wfType,
							},
							TaskList:                            taskList,
							Input:                               []byte("testInput"),
							ExecutionStartToCloseTimeoutSeconds: 100,
							TaskStartToCloseTimeoutSeconds:      200,
						}},
					},
					{
						EventId:   2,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeDecisionTaskScheduled,
						Attributes: &commonproto.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &commonproto.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   3,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeDecisionTaskStarted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &commonproto.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 2,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   4,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeDecisionTaskCompleted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &commonproto.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 2,
							StartedEventId:   3,
						}},
					},
					{
						EventId:   5,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeMarkerRecorded,
						Attributes: &commonproto.HistoryEvent_MarkerRecordedEventAttributes{MarkerRecordedEventAttributes: &commonproto.MarkerRecordedEventAttributes{
							MarkerName:                   "Version",
							Details:                      []byte("details"),
							DecisionTaskCompletedEventId: 4,
						}},
					},
					{
						EventId:   6,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeActivityTaskScheduled,
						Attributes: &commonproto.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &commonproto.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDCompleted1,
							ActivityType: &commonproto.ActivityType{
								Name: "actType0",
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: 1000,
							ScheduleToStartTimeoutSeconds: 2000,
							StartToCloseTimeoutSeconds:    3000,
							HeartbeatTimeoutSeconds:       4000,
							DecisionTaskCompletedEventId:  4,
						}},
					},
					{
						EventId:   7,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeTimerStarted,
						Attributes: &commonproto.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &commonproto.TimerStartedEventAttributes{
							TimerId:                      timerFiredID,
							StartToFireTimeoutSeconds:    2,
							DecisionTaskCompletedEventId: 4,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   8,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeActivityTaskStarted,
						Attributes: &commonproto.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &commonproto.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 6,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   9,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeActivityTaskCompleted,
						Attributes: &commonproto.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &commonproto.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: 6,
							StartedEventId:   8,
						}},
					},
					{
						EventId:   10,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeDecisionTaskScheduled,
						Attributes: &commonproto.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &commonproto.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   11,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeDecisionTaskStarted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &commonproto.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 10,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   12,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeDecisionTaskCompleted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &commonproto.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 10,
							StartedEventId:   11,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   13,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeTimerFired,
						Attributes: &commonproto.HistoryEvent_TimerFiredEventAttributes{TimerFiredEventAttributes: &commonproto.TimerFiredEventAttributes{
							TimerId: timerFiredID,
						}},
					},
					{
						EventId:   14,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeDecisionTaskScheduled,
						Attributes: &commonproto.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &commonproto.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   15,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeDecisionTaskStarted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &commonproto.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 14,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   16,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeDecisionTaskCompleted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &commonproto.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 14,
							StartedEventId:   15,
						}},
					},
					{
						EventId:   17,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeActivityTaskScheduled,
						Attributes: &commonproto.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &commonproto.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDStarted1,
							ActivityType: &commonproto.ActivityType{
								Name: "actType1",
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: 1000,
							ScheduleToStartTimeoutSeconds: 2000,
							StartToCloseTimeoutSeconds:    3000,
							HeartbeatTimeoutSeconds:       4000,
							DecisionTaskCompletedEventId:  16,
							RetryPolicy: &commonproto.RetryPolicy{
								InitialIntervalInSeconds:    1,
								BackoffCoefficient:          0.2,
								MaximumAttempts:             10,
								MaximumIntervalInSeconds:    1000,
								ExpirationIntervalInSeconds: math.MaxInt32,
							},
						}},
					},
					{
						EventId:   18,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeActivityTaskScheduled,
						Attributes: &commonproto.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &commonproto.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDNotStarted,
							ActivityType: &commonproto.ActivityType{
								Name: "actType2",
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: 1000,
							ScheduleToStartTimeoutSeconds: 2000,
							StartToCloseTimeoutSeconds:    3000,
							HeartbeatTimeoutSeconds:       4000,
							DecisionTaskCompletedEventId:  16,
						}},
					},
					{
						EventId:   19,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeTimerStarted,
						Attributes: &commonproto.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &commonproto.TimerStartedEventAttributes{
							TimerId:                      timerUnfiredID1,
							StartToFireTimeoutSeconds:    4,
							DecisionTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   20,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeTimerStarted,
						Attributes: &commonproto.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &commonproto.TimerStartedEventAttributes{
							TimerId:                      timerUnfiredID2,
							StartToFireTimeoutSeconds:    8,
							DecisionTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   21,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeActivityTaskScheduled,
						Attributes: &commonproto.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &commonproto.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDCompleted2,
							ActivityType: &commonproto.ActivityType{
								Name: "actType2",
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: 1000,
							ScheduleToStartTimeoutSeconds: 2000,
							StartToCloseTimeoutSeconds:    3000,
							HeartbeatTimeoutSeconds:       4000,
							DecisionTaskCompletedEventId:  16,
						}},
					},
					{
						EventId:   22,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeActivityTaskScheduled,
						Attributes: &commonproto.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &commonproto.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDStarted2,
							ActivityType: &commonproto.ActivityType{
								Name: "actType2",
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: 1000,
							ScheduleToStartTimeoutSeconds: 2000,
							StartToCloseTimeoutSeconds:    3000,
							HeartbeatTimeoutSeconds:       4000,
							DecisionTaskCompletedEventId:  16,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   23,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeActivityTaskStarted,
						Attributes: &commonproto.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &commonproto.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 21,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   24,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeActivityTaskStarted,
						Attributes: &commonproto.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &commonproto.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 17,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   25,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeActivityTaskStarted,
						Attributes: &commonproto.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &commonproto.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 22,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   26,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeActivityTaskCompleted,
						Attributes: &commonproto.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &commonproto.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: 21,
							StartedEventId:   23,
						}},
					},
					{
						EventId:   27,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeDecisionTaskScheduled,
						Attributes: &commonproto.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &commonproto.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   28,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeDecisionTaskStarted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &commonproto.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 27,
						}},
					},
				},
			},
			/////////////// reset point/////////////
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   29,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeDecisionTaskCompleted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &commonproto.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 27,
							StartedEventId:   28,
						}},
					},
					{
						EventId:   30,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeTimerStarted,
						Attributes: &commonproto.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &commonproto.TimerStartedEventAttributes{
							TimerId:                      timerAfterReset,
							StartToFireTimeoutSeconds:    4,
							DecisionTaskCompletedEventId: 29,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   31,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeActivityTaskStarted,
						Attributes: &commonproto.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &commonproto.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 18,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   32,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeWorkflowExecutionSignaled,
						Attributes: &commonproto.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &commonproto.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName1,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   33,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeWorkflowExecutionSignaled,
						Attributes: &commonproto.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &commonproto.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName2,
						}},
					},
				},
			},
		},
	}

	eid := int64(0)
	timestamp := int64(1000)
	for _, be := range readHistoryResp.History {
		for _, e := range be.Events {
			eid++
			if e.GetEventId() != eid {
				s.Fail(fmt.Sprintf("inconintous eventID: %v, %v", eid, e.GetEventId()))
			}
			e.Timestamp = timestamp
		}
	}

	newBranchToken := []byte("newBranch")
	forkResp := &persistence.ForkHistoryBranchResponse{
		NewBranchToken: newBranchToken,
	}

	appendV2Resp := &persistence.AppendHistoryNodesResponse{
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
	appendReq, ok := appendCall.Arguments[0].(*persistence.AppendHistoryNodesRequest)
	s.Equal(true, ok)
	s.Equal(newBranchToken, appendReq.BranchToken)
	s.Equal(false, appendReq.IsNewBranch)
	s.Equal(6, len(appendReq.Events))
	s.Equal(enums.EventTypeDecisionTaskFailed, enums.EventType(appendReq.Events[0].GetEventType()))
	s.Equal(enums.EventTypeActivityTaskFailed, enums.EventType(appendReq.Events[1].GetEventType()))
	s.Equal(enums.EventTypeActivityTaskFailed, enums.EventType(appendReq.Events[2].GetEventType()))
	s.Equal(enums.EventTypeWorkflowExecutionSignaled, enums.EventType(appendReq.Events[3].GetEventType()))
	s.Equal(enums.EventTypeWorkflowExecutionSignaled, enums.EventType(appendReq.Events[4].GetEventType()))
	s.Equal(enums.EventTypeDecisionTaskScheduled, enums.EventType(appendReq.Events[5].GetEventType()))

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
	resetReq, ok := resetCall.Arguments[0].(*persistence.ResetWorkflowExecutionRequest)
	s.True(resetReq.CurrentWorkflowMutation.ExecutionInfo.LastEventTaskID > 0)
	resetReq.CurrentWorkflowMutation.ExecutionInfo.LastEventTaskID = 0
	s.Equal(true, ok)
	s.Equal(true, resetReq.CurrentWorkflowMutation != nil)
	compareCurrExeInfo.State = persistence.WorkflowStateCompleted
	compareCurrExeInfo.CloseStatus = persistence.WorkflowCloseStatusTerminated
	compareCurrExeInfo.NextEventID = 2
	compareCurrExeInfo.CompletionEventBatchID = 1
	s.Equal(compareCurrExeInfo, resetReq.CurrentWorkflowMutation.ExecutionInfo)
	s.Equal(1, len(resetReq.CurrentWorkflowMutation.TransferTasks))
	s.Equal(1, len(resetReq.CurrentWorkflowMutation.TimerTasks))
	s.Equal(persistence.TransferTaskTypeCloseExecution, resetReq.CurrentWorkflowMutation.TransferTasks[0].GetType())
	s.Equal(persistence.TaskTypeDeleteHistoryEvent, resetReq.CurrentWorkflowMutation.TimerTasks[0].GetType())
	s.Equal(int64(200), resetReq.CurrentWorkflowMutation.ExecutionStats.HistorySize)

	s.Equal("wfType", resetReq.NewWorkflowSnapshot.ExecutionInfo.WorkflowTypeName)
	s.True(len(resetReq.NewWorkflowSnapshot.ExecutionInfo.RunID) > 0)
	s.Equal([]byte(newBranchToken), resetReq.NewWorkflowSnapshot.ExecutionInfo.BranchToken)
	// 35 = resetEventID(29) + 6 in a batch
	s.Equal(int64(34), resetReq.NewWorkflowSnapshot.ExecutionInfo.DecisionScheduleID)
	s.Equal(int64(35), resetReq.NewWorkflowSnapshot.ExecutionInfo.NextEventID)

	// one activity task, one decision task and one record workflow started task
	s.Equal(3, len(resetReq.NewWorkflowSnapshot.TransferTasks))
	s.Equal(persistence.TransferTaskTypeActivityTask, resetReq.NewWorkflowSnapshot.TransferTasks[0].GetType())
	s.Equal(persistence.TransferTaskTypeDecisionTask, resetReq.NewWorkflowSnapshot.TransferTasks[1].GetType())
	s.Equal(persistence.TransferTaskTypeRecordWorkflowStarted, resetReq.NewWorkflowSnapshot.TransferTasks[2].GetType())

	// WF timeout task, user timer, activity timeout timer, activity retry timer
	s.Equal(3, len(resetReq.NewWorkflowSnapshot.TimerTasks))
	s.Equal(persistence.TaskTypeWorkflowTimeout, resetReq.NewWorkflowSnapshot.TimerTasks[0].GetType())
	s.Equal(persistence.TaskTypeUserTimer, resetReq.NewWorkflowSnapshot.TimerTasks[1].GetType())
	s.Equal(persistence.TaskTypeActivityTimeout, resetReq.NewWorkflowSnapshot.TimerTasks[2].GetType())

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

func (s *resetorSuite) assertTimerIDs(ids []string, timers []*persistenceblobs.TimerInfo) {
	m := map[string]bool{}
	for _, s := range ids {
		m[s] = true
	}

	for _, t := range timers {
		delete(m, t.TimerID)
	}

	s.Equal(0, len(m))
}

func (s *resetorSuite) assertActivityIDs(ids []string, timers []*persistence.ActivityInfo) {
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
		&persistence.DomainInfo{ID: testDomainID}, &persistence.DomainConfig{Retention: 1}, "", nil,
	)
	s.mockDomainCache.EXPECT().GetDomainByID(gomock.Any()).Return(testDomainEntry, nil).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomain(gomock.Any()).Return(testDomainEntry, nil).AnyTimes()

	request := &historyservice.ResetWorkflowExecutionRequest{}
	domainID := testDomainID
	request.DomainUUID = domainID
	request.ResetRequest = &workflowservice.ResetWorkflowExecutionRequest{}

	wid := "wId"
	wfType := "wfType"
	taskListName := "taskList"
	forkRunID := uuid.New().String()
	currRunID := uuid.New().String()
	we := commonproto.WorkflowExecution{
		WorkflowId: wid,
		RunId:      forkRunID,
	}
	request.ResetRequest = &workflowservice.ResetWorkflowExecutionRequest{
		Domain:                "testDomainName",
		WorkflowExecution:     &we,
		Reason:                "test reset",
		DecisionFinishEventId: 30,
		RequestId:             uuid.New().String(),
	}

	forkGwmsRequest := &persistence.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: commonproto.WorkflowExecution{
			WorkflowId: wid,
			RunId:      forkRunID,
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
	cancelWE := &commonproto.WorkflowExecution{
		WorkflowId: "cancel-wfid",
		RunId:      uuid.New().String(),
	}
	forkBranchToken := []byte("forkBranchToken")
	forkExeInfo := &persistence.WorkflowExecutionInfo{
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
	forkGwmsResponse := &persistence.GetWorkflowExecutionResponse{State: &persistence.WorkflowMutableState{
		ExecutionInfo:  forkExeInfo,
		ExecutionStats: &persistence.ExecutionStats{},
	}}

	currGwmsRequest := &persistence.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: commonproto.WorkflowExecution{
			WorkflowId: wid,
			RunId:      currRunID,
		},
	}
	currExeInfo := &persistence.WorkflowExecutionInfo{
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
	currGwmsResponse := &persistence.GetWorkflowExecutionResponse{State: &persistence.WorkflowMutableState{
		ExecutionInfo:  currExeInfo,
		ExecutionStats: &persistence.ExecutionStats{},
	}}

	gcurResponse := &persistence.GetCurrentExecutionResponse{
		RunID: currRunID,
	}

	readHistoryReq := &persistence.ReadHistoryBranchRequest{
		BranchToken:   forkBranchToken,
		MinEventID:    common.FirstEventID,
		MaxEventID:    int64(35),
		PageSize:      defaultHistoryPageSize,
		NextPageToken: nil,
		ShardID:       &s.shardID,
	}

	taskList := &commonproto.TaskList{
		Name: taskListName,
	}
	readHistoryResp := &persistence.ReadHistoryBranchByBatchResponse{
		NextPageToken:    nil,
		Size:             1000,
		LastFirstEventID: int64(31),
		History: []*commonproto.History{
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   1,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeWorkflowExecutionStarted,
						Attributes: &commonproto.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &commonproto.WorkflowExecutionStartedEventAttributes{
							WorkflowType: &commonproto.WorkflowType{
								Name: wfType,
							},
							TaskList:                            taskList,
							Input:                               []byte("testInput"),
							ExecutionStartToCloseTimeoutSeconds: 100,
							TaskStartToCloseTimeoutSeconds:      200,
						}},
					},
					{
						EventId:   2,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeDecisionTaskScheduled,
						Attributes: &commonproto.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &commonproto.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   3,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeDecisionTaskStarted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &commonproto.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 2,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   4,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeDecisionTaskCompleted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &commonproto.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 2,
							StartedEventId:   3,
						}},
					},
					{
						EventId:   5,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeMarkerRecorded,
						Attributes: &commonproto.HistoryEvent_MarkerRecordedEventAttributes{MarkerRecordedEventAttributes: &commonproto.MarkerRecordedEventAttributes{
							MarkerName:                   "Version",
							Details:                      []byte("details"),
							DecisionTaskCompletedEventId: 4,
						}},
					},
					{
						EventId:   6,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeActivityTaskScheduled,
						Attributes: &commonproto.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &commonproto.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDCompleted1,
							ActivityType: &commonproto.ActivityType{
								Name: "actType0",
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: 1000,
							ScheduleToStartTimeoutSeconds: 2000,
							StartToCloseTimeoutSeconds:    3000,
							HeartbeatTimeoutSeconds:       4000,
							DecisionTaskCompletedEventId:  4,
						}},
					},
					{
						EventId:   7,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeTimerStarted,
						Attributes: &commonproto.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &commonproto.TimerStartedEventAttributes{
							TimerId:                      timerFiredID,
							StartToFireTimeoutSeconds:    2,
							DecisionTaskCompletedEventId: 4,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   8,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeActivityTaskStarted,
						Attributes: &commonproto.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &commonproto.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 6,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   9,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeActivityTaskCompleted,
						Attributes: &commonproto.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &commonproto.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: 6,
							StartedEventId:   8,
						}},
					},
					{
						EventId:   10,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeDecisionTaskScheduled,
						Attributes: &commonproto.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &commonproto.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   11,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeDecisionTaskStarted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &commonproto.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 10,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   12,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeDecisionTaskCompleted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &commonproto.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 10,
							StartedEventId:   11,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   13,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeTimerFired,
						Attributes: &commonproto.HistoryEvent_TimerFiredEventAttributes{TimerFiredEventAttributes: &commonproto.TimerFiredEventAttributes{
							TimerId: timerFiredID,
						}},
					},
					{
						EventId:   14,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeDecisionTaskScheduled,
						Attributes: &commonproto.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &commonproto.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   15,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeDecisionTaskStarted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &commonproto.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 14,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   16,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeDecisionTaskCompleted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &commonproto.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 14,
							StartedEventId:   15,
						}},
					},
					{
						EventId:   17,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeActivityTaskScheduled,
						Attributes: &commonproto.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &commonproto.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDStartedRetry,
							ActivityType: &commonproto.ActivityType{
								Name: "actType1",
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: 1000,
							ScheduleToStartTimeoutSeconds: 2000,
							StartToCloseTimeoutSeconds:    3000,
							HeartbeatTimeoutSeconds:       4000,
							DecisionTaskCompletedEventId:  16,
							RetryPolicy: &commonproto.RetryPolicy{
								InitialIntervalInSeconds:    1,
								BackoffCoefficient:          0.2,
								MaximumAttempts:             10,
								MaximumIntervalInSeconds:    1000,
								ExpirationIntervalInSeconds: math.MaxInt32,
							},
						}},
					},
					{
						EventId:   18,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeActivityTaskScheduled,
						Attributes: &commonproto.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &commonproto.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDNotStarted,
							ActivityType: &commonproto.ActivityType{
								Name: "actType2",
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: 1000,
							ScheduleToStartTimeoutSeconds: 2000,
							StartToCloseTimeoutSeconds:    3000,
							HeartbeatTimeoutSeconds:       4000,
							DecisionTaskCompletedEventId:  16,
						}},
					},
					{
						EventId:   19,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeTimerStarted,
						Attributes: &commonproto.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &commonproto.TimerStartedEventAttributes{
							TimerId:                      timerUnfiredID1,
							StartToFireTimeoutSeconds:    4,
							DecisionTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   20,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeTimerStarted,
						Attributes: &commonproto.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &commonproto.TimerStartedEventAttributes{
							TimerId:                      timerUnfiredID2,
							StartToFireTimeoutSeconds:    8,
							DecisionTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   21,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeActivityTaskScheduled,
						Attributes: &commonproto.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &commonproto.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDCompleted2,
							ActivityType: &commonproto.ActivityType{
								Name: "actType2",
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: 1000,
							ScheduleToStartTimeoutSeconds: 2000,
							StartToCloseTimeoutSeconds:    3000,
							HeartbeatTimeoutSeconds:       4000,
							DecisionTaskCompletedEventId:  16,
						}},
					},
					{
						EventId:   22,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeActivityTaskScheduled,
						Attributes: &commonproto.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &commonproto.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDStartedNoRetry,
							ActivityType: &commonproto.ActivityType{
								Name: "actType2",
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: 1000,
							ScheduleToStartTimeoutSeconds: 2000,
							StartToCloseTimeoutSeconds:    3000,
							HeartbeatTimeoutSeconds:       4000,
							DecisionTaskCompletedEventId:  16,
						}},
					},
					{
						EventId:   23,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeRequestCancelExternalWorkflowExecutionInitiated,
						Attributes: &commonproto.HistoryEvent_RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{RequestCancelExternalWorkflowExecutionInitiatedEventAttributes: &commonproto.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
							Domain:                       "any-domain-name",
							WorkflowExecution:            cancelWE,
							DecisionTaskCompletedEventId: 16,
							ChildWorkflowOnly:            true,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   24,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeActivityTaskStarted,
						Attributes: &commonproto.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &commonproto.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 21,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   25,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeActivityTaskStarted,
						Attributes: &commonproto.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &commonproto.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 17,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   26,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeActivityTaskStarted,
						Attributes: &commonproto.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &commonproto.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 22,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   27,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeActivityTaskCompleted,
						Attributes: &commonproto.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &commonproto.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: 21,
							StartedEventId:   24,
						}},
					},
					{
						EventId:   28,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeDecisionTaskScheduled,
						Attributes: &commonproto.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &commonproto.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   29,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeDecisionTaskStarted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &commonproto.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 28,
						}},
					},
				},
			},
			/////////////// reset point/////////////
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   30,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeDecisionTaskCompleted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &commonproto.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 28,
							StartedEventId:   29,
						}},
					},
					{
						EventId:   31,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeTimerStarted,
						Attributes: &commonproto.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &commonproto.TimerStartedEventAttributes{
							TimerId:                      timerAfterReset,
							StartToFireTimeoutSeconds:    4,
							DecisionTaskCompletedEventId: 30,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   32,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeActivityTaskStarted,
						Attributes: &commonproto.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &commonproto.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 18,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   33,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeWorkflowExecutionSignaled,
						Attributes: &commonproto.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &commonproto.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName1,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   34,
						Version:   common.EmptyVersion,
						EventType: enums.EventTypeWorkflowExecutionSignaled,
						Attributes: &commonproto.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &commonproto.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName2,
						}},
					},
				},
			},
		},
	}

	eid := int64(0)
	timestamp := int64(1000)
	for _, be := range readHistoryResp.History {
		for _, e := range be.Events {
			eid++
			if e.GetEventId() != eid {
				s.Fail(fmt.Sprintf("inconintous eventID: %v, %v", eid, e.GetEventId()))
			}
			e.Timestamp = timestamp
		}
	}

	s.mockExecutionMgr.On("GetWorkflowExecution", forkGwmsRequest).Return(forkGwmsResponse, nil).Once()
	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gcurResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", currGwmsRequest).Return(currGwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("ReadHistoryBranchByBatch", readHistoryReq).Return(readHistoryResp, nil).Once()

	_, err := s.historyEngine.ResetWorkflowExecution(context.Background(), request)
	s.EqualError(err, "it is not allowed resetting to a point that workflow has pending request cancel.")
}

func (s *resetorSuite) TestResetWorkflowExecution_Replication_WithTerminatingCurrent() {
	domainName := "testDomainName"
	beforeResetVersion := int64(100)
	afterResetVersion := int64(101)
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(beforeResetVersion).Return("standby").AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(afterResetVersion).Return("active").AnyTimes()

	testDomainEntry := cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{ID: testDomainID},
		&persistence.DomainConfig{Retention: 1},
		&persistence.DomainReplicationConfig{
			ActiveClusterName: "active",
			Clusters: []*persistence.ClusterReplicationConfig{
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

	request := &historyservice.ResetWorkflowExecutionRequest{}
	domainID := testDomainID
	request.DomainUUID = domainID
	request.ResetRequest = &workflowservice.ResetWorkflowExecutionRequest{}

	wid := "wId"
	wfType := "wfType"
	taskListName := "taskList"
	forkRunID := uuid.New().String()
	currRunID := uuid.New().String()
	we := commonproto.WorkflowExecution{
		WorkflowId: wid,
		RunId:      forkRunID,
	}
	request.ResetRequest = &workflowservice.ResetWorkflowExecutionRequest{
		Domain:                domainName,
		WorkflowExecution:     &we,
		Reason:                "test reset",
		DecisionFinishEventId: 30,
		RequestId:             uuid.New().String(),
	}

	forkGwmsRequest := &persistence.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: commonproto.WorkflowExecution{
			WorkflowId: wid,
			RunId:      forkRunID,
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
	forkExeInfo := &persistence.WorkflowExecutionInfo{
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

	forkRepState := &persistence.ReplicationState{
		CurrentVersion:      beforeResetVersion,
		StartVersion:        beforeResetVersion,
		LastWriteEventID:    common.EmptyEventID,
		LastWriteVersion:    common.EmptyVersion,
		LastReplicationInfo: map[string]*replication.ReplicationInfo{},
	}
	forkGwmsResponse := &persistence.GetWorkflowExecutionResponse{State: &persistence.WorkflowMutableState{
		ExecutionInfo:    forkExeInfo,
		ExecutionStats:   &persistence.ExecutionStats{},
		ReplicationState: forkRepState,
	}}

	currGwmsRequest := &persistence.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: commonproto.WorkflowExecution{
			WorkflowId: wid,
			RunId:      currRunID,
		},
	}
	currExeInfo := &persistence.WorkflowExecutionInfo{
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
	currGwmsResponse := &persistence.GetWorkflowExecutionResponse{State: &persistence.WorkflowMutableState{
		ExecutionInfo:    currExeInfo,
		ExecutionStats:   &persistence.ExecutionStats{},
		ReplicationState: forkRepState,
	}}

	gcurResponse := &persistence.GetCurrentExecutionResponse{
		RunID: currRunID,
	}

	readHistoryReq := &persistence.ReadHistoryBranchRequest{
		BranchToken:   forkBranchToken,
		MinEventID:    common.FirstEventID,
		MaxEventID:    int64(35),
		PageSize:      defaultHistoryPageSize,
		NextPageToken: nil,
		ShardID:       &s.shardID,
	}

	taskList := &commonproto.TaskList{
		Name: taskListName,
	}
	readHistoryResp := &persistence.ReadHistoryBranchByBatchResponse{
		NextPageToken:    nil,
		Size:             1000,
		LastFirstEventID: int64(31),
		History: []*commonproto.History{
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   1,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeWorkflowExecutionStarted,
						Attributes: &commonproto.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &commonproto.WorkflowExecutionStartedEventAttributes{
							WorkflowType: &commonproto.WorkflowType{
								Name: wfType,
							},
							TaskList:                            taskList,
							Input:                               []byte("testInput"),
							ExecutionStartToCloseTimeoutSeconds: 100,
							TaskStartToCloseTimeoutSeconds:      200,
						}},
					},
					{
						EventId:   2,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskScheduled,
						Attributes: &commonproto.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &commonproto.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   3,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskStarted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &commonproto.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 2,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   4,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskCompleted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &commonproto.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 2,
							StartedEventId:   3,
						}},
					},
					{
						EventId:   5,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeMarkerRecorded,
						Attributes: &commonproto.HistoryEvent_MarkerRecordedEventAttributes{MarkerRecordedEventAttributes: &commonproto.MarkerRecordedEventAttributes{
							MarkerName:                   "Version",
							Details:                      []byte("details"),
							DecisionTaskCompletedEventId: 4,
						}},
					},
					{
						EventId:   6,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskScheduled,
						Attributes: &commonproto.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &commonproto.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDCompleted1,
							ActivityType: &commonproto.ActivityType{
								Name: "actType0",
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: 1000,
							ScheduleToStartTimeoutSeconds: 2000,
							StartToCloseTimeoutSeconds:    3000,
							HeartbeatTimeoutSeconds:       4000,
							DecisionTaskCompletedEventId:  4,
						}},
					},
					{
						EventId:   7,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeTimerStarted,
						Attributes: &commonproto.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &commonproto.TimerStartedEventAttributes{
							TimerId:                      timerFiredID,
							StartToFireTimeoutSeconds:    2,
							DecisionTaskCompletedEventId: 4,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   8,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskStarted,
						Attributes: &commonproto.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &commonproto.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 6,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   9,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskCompleted,
						Attributes: &commonproto.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &commonproto.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: 6,
							StartedEventId:   8,
						}},
					},
					{
						EventId:   10,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskScheduled,
						Attributes: &commonproto.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &commonproto.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   11,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskStarted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &commonproto.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 10,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   12,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskCompleted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &commonproto.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 10,
							StartedEventId:   11,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   13,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeTimerFired,
						Attributes: &commonproto.HistoryEvent_TimerFiredEventAttributes{TimerFiredEventAttributes: &commonproto.TimerFiredEventAttributes{
							TimerId: timerFiredID,
						}},
					},
					{
						EventId:   14,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskScheduled,
						Attributes: &commonproto.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &commonproto.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   15,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskStarted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &commonproto.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 14,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   16,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskCompleted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &commonproto.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 14,
							StartedEventId:   15,
						}},
					},
					{
						EventId:   17,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskScheduled,
						Attributes: &commonproto.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &commonproto.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDRetry,
							ActivityType: &commonproto.ActivityType{
								Name: "actType1",
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: 1000,
							ScheduleToStartTimeoutSeconds: 2000,
							StartToCloseTimeoutSeconds:    3000,
							HeartbeatTimeoutSeconds:       4000,
							DecisionTaskCompletedEventId:  16,
							RetryPolicy: &commonproto.RetryPolicy{
								InitialIntervalInSeconds:    1,
								BackoffCoefficient:          0.2,
								MaximumAttempts:             10,
								MaximumIntervalInSeconds:    1000,
								ExpirationIntervalInSeconds: math.MaxInt32,
							},
						}},
					},
					{
						EventId:   18,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskScheduled,
						Attributes: &commonproto.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &commonproto.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDNotStarted,
							ActivityType: &commonproto.ActivityType{
								Name: "actType2",
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: 1000,
							ScheduleToStartTimeoutSeconds: 2000,
							StartToCloseTimeoutSeconds:    3000,
							HeartbeatTimeoutSeconds:       4000,
							DecisionTaskCompletedEventId:  16,
						}},
					},
					{
						EventId:   19,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeTimerStarted,
						Attributes: &commonproto.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &commonproto.TimerStartedEventAttributes{
							TimerId:                      timerUnfiredID1,
							StartToFireTimeoutSeconds:    4,
							DecisionTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   20,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeTimerStarted,
						Attributes: &commonproto.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &commonproto.TimerStartedEventAttributes{
							TimerId:                      timerUnfiredID2,
							StartToFireTimeoutSeconds:    8,
							DecisionTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   21,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskScheduled,
						Attributes: &commonproto.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &commonproto.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDCompleted2,
							ActivityType: &commonproto.ActivityType{
								Name: "actType2",
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: 1000,
							ScheduleToStartTimeoutSeconds: 2000,
							StartToCloseTimeoutSeconds:    3000,
							HeartbeatTimeoutSeconds:       4000,
							DecisionTaskCompletedEventId:  16,
						}},
					},
					{
						EventId:   22,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskScheduled,
						Attributes: &commonproto.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &commonproto.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDStartedNoRetry,
							ActivityType: &commonproto.ActivityType{
								Name: "actType2",
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: 1000,
							ScheduleToStartTimeoutSeconds: 2000,
							StartToCloseTimeoutSeconds:    3000,
							HeartbeatTimeoutSeconds:       4000,
							DecisionTaskCompletedEventId:  16,
						}},
					},
					{
						EventId:   23,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeWorkflowExecutionSignaled,
						Attributes: &commonproto.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &commonproto.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName3,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   24,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskStarted,
						Attributes: &commonproto.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &commonproto.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 21,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   25,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeWorkflowExecutionSignaled,
						Attributes: &commonproto.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &commonproto.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName4,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   26,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskStarted,
						Attributes: &commonproto.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &commonproto.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 22,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   27,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskCompleted,
						Attributes: &commonproto.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &commonproto.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: 21,
							StartedEventId:   24,
						}},
					},
					{
						EventId:   28,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskScheduled,
						Attributes: &commonproto.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &commonproto.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   29,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskStarted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &commonproto.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 28,
						}},
					},
				},
			},
			/////////////// reset point/////////////
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   30,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskCompleted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &commonproto.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 28,
							StartedEventId:   29,
						}},
					},
					{
						EventId:   31,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeTimerStarted,
						Attributes: &commonproto.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &commonproto.TimerStartedEventAttributes{
							TimerId:                      timerAfterReset,
							StartToFireTimeoutSeconds:    4,
							DecisionTaskCompletedEventId: 30,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   32,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskStarted,
						Attributes: &commonproto.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &commonproto.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 18,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   33,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeWorkflowExecutionSignaled,
						Attributes: &commonproto.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &commonproto.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName1,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   34,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeWorkflowExecutionSignaled,
						Attributes: &commonproto.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &commonproto.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName2,
						}},
					},
				},
			},
		},
	}

	eid := int64(0)
	timestamp := int64(1000)
	for _, be := range readHistoryResp.History {
		for _, e := range be.Events {
			eid++
			if e.GetEventId() != eid {
				s.Fail(fmt.Sprintf("inconintous eventID: %v, %v", eid, e.GetEventId()))
			}
			e.Timestamp = timestamp
		}
	}

	newBranchToken := []byte("newBranch")
	forkResp := &persistence.ForkHistoryBranchResponse{
		NewBranchToken: newBranchToken,
	}

	appendV2Resp := &persistence.AppendHistoryNodesResponse{
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
	appendReq, ok := appendCall.Arguments[0].(*persistence.AppendHistoryNodesRequest)
	s.Equal(true, ok)
	s.Equal(newBranchToken, appendReq.BranchToken)
	s.Equal(false, appendReq.IsNewBranch)
	s.Equal(5, len(appendReq.Events))
	s.Equal(enums.EventTypeDecisionTaskFailed, enums.EventType(appendReq.Events[0].GetEventType()))
	s.Equal(enums.EventTypeActivityTaskFailed, enums.EventType(appendReq.Events[1].GetEventType()))
	s.Equal(enums.EventTypeWorkflowExecutionSignaled, enums.EventType(appendReq.Events[2].GetEventType()))
	s.Equal(enums.EventTypeWorkflowExecutionSignaled, enums.EventType(appendReq.Events[3].GetEventType()))
	s.Equal(enums.EventTypeDecisionTaskScheduled, enums.EventType(appendReq.Events[4].GetEventType()))

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
	resetReq, ok := resetCall.Arguments[0].(*persistence.ResetWorkflowExecutionRequest)
	s.True(resetReq.CurrentWorkflowMutation.ExecutionInfo.LastEventTaskID > 0)
	resetReq.CurrentWorkflowMutation.ExecutionInfo.LastEventTaskID = 0
	s.Equal(true, ok)
	s.Equal(true, resetReq.CurrentWorkflowMutation != nil)
	compareCurrExeInfo.State = persistence.WorkflowStateCompleted
	compareCurrExeInfo.CloseStatus = persistence.WorkflowCloseStatusTerminated
	compareCurrExeInfo.NextEventID = 2
	compareCurrExeInfo.LastFirstEventID = 1
	compareCurrExeInfo.CompletionEventBatchID = 1
	s.Equal(compareCurrExeInfo, resetReq.CurrentWorkflowMutation.ExecutionInfo)
	s.Equal(1, len(resetReq.CurrentWorkflowMutation.TransferTasks))
	s.Equal(1, len(resetReq.CurrentWorkflowMutation.TimerTasks))
	s.Equal(persistence.TransferTaskTypeCloseExecution, resetReq.CurrentWorkflowMutation.TransferTasks[0].GetType())
	s.Equal(persistence.TaskTypeDeleteHistoryEvent, resetReq.CurrentWorkflowMutation.TimerTasks[0].GetType())
	s.Equal(int64(200), resetReq.CurrentWorkflowMutation.ExecutionStats.HistorySize)

	s.Equal("wfType", resetReq.NewWorkflowSnapshot.ExecutionInfo.WorkflowTypeName)
	s.True(len(resetReq.NewWorkflowSnapshot.ExecutionInfo.RunID) > 0)
	s.Equal([]byte(newBranchToken), resetReq.NewWorkflowSnapshot.ExecutionInfo.BranchToken)

	s.Equal(int64(34), resetReq.NewWorkflowSnapshot.ExecutionInfo.DecisionScheduleID)
	s.Equal(int64(35), resetReq.NewWorkflowSnapshot.ExecutionInfo.NextEventID)

	s.Equal(4, len(resetReq.NewWorkflowSnapshot.TransferTasks))
	s.Equal(persistence.TransferTaskTypeActivityTask, resetReq.NewWorkflowSnapshot.TransferTasks[0].GetType())
	s.Equal(persistence.TransferTaskTypeActivityTask, resetReq.NewWorkflowSnapshot.TransferTasks[1].GetType())
	s.Equal(persistence.TransferTaskTypeDecisionTask, resetReq.NewWorkflowSnapshot.TransferTasks[2].GetType())
	s.Equal(persistence.TransferTaskTypeRecordWorkflowStarted, resetReq.NewWorkflowSnapshot.TransferTasks[3].GetType())

	// WF timeout task, user timer, activity timeout timer, activity retry timer
	s.Equal(3, len(resetReq.NewWorkflowSnapshot.TimerTasks))
	s.Equal(persistence.TaskTypeWorkflowTimeout, resetReq.NewWorkflowSnapshot.TimerTasks[0].GetType())
	s.Equal(persistence.TaskTypeUserTimer, resetReq.NewWorkflowSnapshot.TimerTasks[1].GetType())
	s.Equal(persistence.TaskTypeActivityTimeout, resetReq.NewWorkflowSnapshot.TimerTasks[2].GetType())

	s.Equal(2, len(resetReq.NewWorkflowSnapshot.TimerInfos))
	s.assertTimerIDs([]string{timerUnfiredID1, timerUnfiredID2}, resetReq.NewWorkflowSnapshot.TimerInfos)

	s.Equal(2, len(resetReq.NewWorkflowSnapshot.ActivityInfos))
	s.assertActivityIDs([]string{actIDRetry, actIDNotStarted}, resetReq.NewWorkflowSnapshot.ActivityInfos)

	s.Equal(1, len(resetReq.NewWorkflowSnapshot.ReplicationTasks))
	s.Equal(persistence.ReplicationTaskTypeHistory, resetReq.NewWorkflowSnapshot.ReplicationTasks[0].GetType())
	s.Equal(1, len(resetReq.CurrentWorkflowMutation.ReplicationTasks))
	s.Equal(persistence.ReplicationTaskTypeHistory, resetReq.CurrentWorkflowMutation.ReplicationTasks[0].GetType())

	compareRepState := copyReplicationState(forkRepState)
	compareRepState.StartVersion = beforeResetVersion
	compareRepState.CurrentVersion = afterResetVersion
	compareRepState.LastWriteEventID = 34
	compareRepState.LastWriteVersion = afterResetVersion
	compareRepState.LastReplicationInfo = map[string]*replication.ReplicationInfo{
		"standby": {
			LastEventId: 29,
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
		&persistence.DomainInfo{ID: testDomainID},
		&persistence.DomainConfig{Retention: 1},
		&persistence.DomainReplicationConfig{
			ActiveClusterName: "active",
			Clusters: []*persistence.ClusterReplicationConfig{
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

	request := &historyservice.ResetWorkflowExecutionRequest{}
	domainID := testDomainID
	request.DomainUUID = domainID
	request.ResetRequest = &workflowservice.ResetWorkflowExecutionRequest{}

	wid := "wId"
	wfType := "wfType"
	taskListName := "taskList"
	forkRunID := uuid.New().String()
	currRunID := uuid.New().String()
	we := commonproto.WorkflowExecution{
		WorkflowId: wid,
		RunId:      forkRunID,
	}
	request.ResetRequest = &workflowservice.ResetWorkflowExecutionRequest{
		Domain:                domainName,
		WorkflowExecution:     &we,
		Reason:                "test reset",
		DecisionFinishEventId: 30,
		RequestId:             uuid.New().String(),
	}

	forkGwmsRequest := &persistence.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: commonproto.WorkflowExecution{
			WorkflowId: wid,
			RunId:      forkRunID,
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
	forkExeInfo := &persistence.WorkflowExecutionInfo{
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

	forkRepState := &persistence.ReplicationState{
		CurrentVersion:      beforeResetVersion,
		StartVersion:        beforeResetVersion,
		LastWriteEventID:    common.EmptyEventID,
		LastWriteVersion:    common.EmptyVersion,
		LastReplicationInfo: map[string]*replication.ReplicationInfo{},
	}
	forkGwmsResponse := &persistence.GetWorkflowExecutionResponse{State: &persistence.WorkflowMutableState{
		ExecutionInfo:    forkExeInfo,
		ExecutionStats:   &persistence.ExecutionStats{},
		ReplicationState: forkRepState,
	}}

	currGwmsRequest := &persistence.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: commonproto.WorkflowExecution{
			WorkflowId: wid,
			RunId:      currRunID,
		},
	}
	currExeInfo := &persistence.WorkflowExecutionInfo{
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
	currGwmsResponse := &persistence.GetWorkflowExecutionResponse{State: &persistence.WorkflowMutableState{
		ExecutionInfo:    currExeInfo,
		ExecutionStats:   &persistence.ExecutionStats{},
		ReplicationState: forkRepState,
	}}

	gcurResponse := &persistence.GetCurrentExecutionResponse{
		RunID: currRunID,
	}

	readHistoryReq := &persistence.ReadHistoryBranchRequest{
		BranchToken:   forkBranchToken,
		MinEventID:    common.FirstEventID,
		MaxEventID:    int64(35),
		PageSize:      defaultHistoryPageSize,
		NextPageToken: nil,
		ShardID:       &s.shardID,
	}

	taskList := &commonproto.TaskList{
		Name: taskListName,
	}
	readHistoryResp := &persistence.ReadHistoryBranchByBatchResponse{
		NextPageToken:    nil,
		Size:             1000,
		LastFirstEventID: int64(31),
		History: []*commonproto.History{
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   1,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeWorkflowExecutionStarted,
						Attributes: &commonproto.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &commonproto.WorkflowExecutionStartedEventAttributes{
							WorkflowType: &commonproto.WorkflowType{
								Name: wfType,
							},
							TaskList:                            taskList,
							Input:                               []byte("testInput"),
							ExecutionStartToCloseTimeoutSeconds: 100,
							TaskStartToCloseTimeoutSeconds:      200,
						}},
					},
					{
						EventId:   2,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskScheduled,
						Attributes: &commonproto.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &commonproto.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   3,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskStarted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &commonproto.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 2,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   4,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskCompleted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &commonproto.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 2,
							StartedEventId:   3,
						}},
					},
					{
						EventId:   5,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeMarkerRecorded,
						Attributes: &commonproto.HistoryEvent_MarkerRecordedEventAttributes{MarkerRecordedEventAttributes: &commonproto.MarkerRecordedEventAttributes{
							MarkerName:                   "Version",
							Details:                      []byte("details"),
							DecisionTaskCompletedEventId: 4,
						}},
					},
					{
						EventId:   6,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskScheduled,
						Attributes: &commonproto.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &commonproto.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDCompleted1,
							ActivityType: &commonproto.ActivityType{
								Name: "actType0",
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: 1000,
							ScheduleToStartTimeoutSeconds: 2000,
							StartToCloseTimeoutSeconds:    3000,
							HeartbeatTimeoutSeconds:       4000,
							DecisionTaskCompletedEventId:  4,
						}},
					},
					{
						EventId:   7,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeTimerStarted,
						Attributes: &commonproto.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &commonproto.TimerStartedEventAttributes{
							TimerId:                      timerFiredID,
							StartToFireTimeoutSeconds:    2,
							DecisionTaskCompletedEventId: 4,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   8,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskStarted,
						Attributes: &commonproto.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &commonproto.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 6,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   9,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskCompleted,
						Attributes: &commonproto.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &commonproto.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: 6,
							StartedEventId:   8,
						}},
					},
					{
						EventId:   10,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskScheduled,
						Attributes: &commonproto.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &commonproto.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   11,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskStarted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &commonproto.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 10,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   12,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskCompleted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &commonproto.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 10,
							StartedEventId:   11,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   13,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeTimerFired,
						Attributes: &commonproto.HistoryEvent_TimerFiredEventAttributes{TimerFiredEventAttributes: &commonproto.TimerFiredEventAttributes{
							TimerId: timerFiredID,
						}},
					},
					{
						EventId:   14,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskScheduled,
						Attributes: &commonproto.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &commonproto.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   15,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskStarted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &commonproto.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 14,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   16,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskCompleted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &commonproto.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 14,
							StartedEventId:   15,
						}},
					},
					{
						EventId:   17,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskScheduled,
						Attributes: &commonproto.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &commonproto.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDRetry,
							ActivityType: &commonproto.ActivityType{
								Name: "actType1",
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: 1000,
							ScheduleToStartTimeoutSeconds: 2000,
							StartToCloseTimeoutSeconds:    3000,
							HeartbeatTimeoutSeconds:       4000,
							DecisionTaskCompletedEventId:  16,
							RetryPolicy: &commonproto.RetryPolicy{
								InitialIntervalInSeconds:    1,
								BackoffCoefficient:          0.2,
								MaximumAttempts:             10,
								MaximumIntervalInSeconds:    1000,
								ExpirationIntervalInSeconds: math.MaxInt32,
							},
						}},
					},
					{
						EventId:   18,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskScheduled,
						Attributes: &commonproto.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &commonproto.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDNotStarted,
							ActivityType: &commonproto.ActivityType{
								Name: "actType2",
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: 1000,
							ScheduleToStartTimeoutSeconds: 2000,
							StartToCloseTimeoutSeconds:    3000,
							HeartbeatTimeoutSeconds:       4000,
							DecisionTaskCompletedEventId:  16,
						}},
					},
					{
						EventId:   19,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeTimerStarted,
						Attributes: &commonproto.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &commonproto.TimerStartedEventAttributes{
							TimerId:                      timerUnfiredID1,
							StartToFireTimeoutSeconds:    4,
							DecisionTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   20,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeTimerStarted,
						Attributes: &commonproto.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &commonproto.TimerStartedEventAttributes{
							TimerId:                      timerUnfiredID2,
							StartToFireTimeoutSeconds:    8,
							DecisionTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   21,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskScheduled,
						Attributes: &commonproto.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &commonproto.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDCompleted2,
							ActivityType: &commonproto.ActivityType{
								Name: "actType2",
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: 1000,
							ScheduleToStartTimeoutSeconds: 2000,
							StartToCloseTimeoutSeconds:    3000,
							HeartbeatTimeoutSeconds:       4000,
							DecisionTaskCompletedEventId:  16,
						}},
					},
					{
						EventId:   22,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskScheduled,
						Attributes: &commonproto.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &commonproto.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDStartedNoRetry,
							ActivityType: &commonproto.ActivityType{
								Name: "actType2",
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: 1000,
							ScheduleToStartTimeoutSeconds: 2000,
							StartToCloseTimeoutSeconds:    3000,
							HeartbeatTimeoutSeconds:       4000,
							DecisionTaskCompletedEventId:  16,
						}},
					},
					{
						EventId:   23,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeWorkflowExecutionSignaled,
						Attributes: &commonproto.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &commonproto.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName3,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   24,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskStarted,
						Attributes: &commonproto.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &commonproto.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 21,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   25,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeWorkflowExecutionSignaled,
						Attributes: &commonproto.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &commonproto.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName4,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   26,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskStarted,
						Attributes: &commonproto.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &commonproto.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 22,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   27,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskCompleted,
						Attributes: &commonproto.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &commonproto.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: 21,
							StartedEventId:   24,
						}},
					},
					{
						EventId:   28,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskScheduled,
						Attributes: &commonproto.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &commonproto.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   29,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskStarted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &commonproto.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 28,
						}},
					},
				},
			},
			/////////////// reset point/////////////
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   30,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskCompleted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &commonproto.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 28,
							StartedEventId:   29,
						}},
					},
					{
						EventId:   31,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeTimerStarted,
						Attributes: &commonproto.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &commonproto.TimerStartedEventAttributes{
							TimerId:                      timerAfterReset,
							StartToFireTimeoutSeconds:    4,
							DecisionTaskCompletedEventId: 30,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   32,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskStarted,
						Attributes: &commonproto.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &commonproto.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 18,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   33,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeWorkflowExecutionSignaled,
						Attributes: &commonproto.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &commonproto.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName1,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   34,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeWorkflowExecutionSignaled,
						Attributes: &commonproto.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &commonproto.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName2,
						}},
					},
				},
			},
		},
	}

	eid := int64(0)
	timestamp := int64(1000)
	for _, be := range readHistoryResp.History {
		for _, e := range be.Events {
			eid++
			if e.GetEventId() != eid {
				s.Fail(fmt.Sprintf("inconintous eventID: %v, %v", eid, e.GetEventId()))
			}
			e.Timestamp = timestamp
		}
	}

	newBranchToken := []byte("newBranch")
	forkResp := &persistence.ForkHistoryBranchResponse{
		NewBranchToken: newBranchToken,
	}

	s.mockExecutionMgr.On("GetWorkflowExecution", forkGwmsRequest).Return(forkGwmsResponse, nil).Once()
	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gcurResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", currGwmsRequest).Return(currGwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("ReadHistoryBranchByBatch", readHistoryReq).Return(readHistoryResp, nil).Once()
	s.mockHistoryV2Mgr.On("ForkHistoryBranch", mock.Anything).Return(forkResp, nil).Once()

	_, err := s.historyEngine.ResetWorkflowExecution(context.Background(), request)
	s.IsType(&serviceerror.DomainNotActive{}, err)
}

func (s *resetorSuite) TestResetWorkflowExecution_Replication_NoTerminatingCurrent() {
	domainName := "testDomainName"
	beforeResetVersion := int64(100)
	afterResetVersion := int64(101)
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(beforeResetVersion).Return("standby").AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(afterResetVersion).Return("active").AnyTimes()

	testDomainEntry := cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{ID: testDomainID},
		&persistence.DomainConfig{Retention: 1},
		&persistence.DomainReplicationConfig{
			ActiveClusterName: "active",
			Clusters: []*persistence.ClusterReplicationConfig{
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

	request := &historyservice.ResetWorkflowExecutionRequest{}
	domainID := testDomainID
	request.DomainUUID = domainID
	request.ResetRequest = &workflowservice.ResetWorkflowExecutionRequest{}

	wid := "wId"
	wfType := "wfType"
	taskListName := "taskList"
	forkRunID := uuid.New().String()
	currRunID := uuid.New().String()
	we := commonproto.WorkflowExecution{
		WorkflowId: wid,
		RunId:      forkRunID,
	}
	request.ResetRequest = &workflowservice.ResetWorkflowExecutionRequest{
		Domain:                domainName,
		WorkflowExecution:     &we,
		Reason:                "test reset",
		DecisionFinishEventId: 30,
		RequestId:             uuid.New().String(),
	}

	forkGwmsRequest := &persistence.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: commonproto.WorkflowExecution{
			WorkflowId: wid,
			RunId:      forkRunID,
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
	forkExeInfo := &persistence.WorkflowExecutionInfo{
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

	forkRepState := &persistence.ReplicationState{
		CurrentVersion:      beforeResetVersion,
		StartVersion:        beforeResetVersion,
		LastWriteEventID:    common.EmptyEventID,
		LastWriteVersion:    common.EmptyVersion,
		LastReplicationInfo: map[string]*replication.ReplicationInfo{},
	}
	forkGwmsResponse := &persistence.GetWorkflowExecutionResponse{State: &persistence.WorkflowMutableState{
		ExecutionInfo:    forkExeInfo,
		ExecutionStats:   &persistence.ExecutionStats{},
		ReplicationState: forkRepState,
	}}

	currGwmsRequest := &persistence.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: commonproto.WorkflowExecution{
			WorkflowId: wid,
			RunId:      currRunID,
		},
	}
	currExeInfo := &persistence.WorkflowExecutionInfo{
		DomainID:           domainID,
		WorkflowID:         wid,
		WorkflowTypeName:   wfType,
		TaskList:           taskListName,
		RunID:              currRunID,
		NextEventID:        common.FirstEventID,
		State:              persistence.WorkflowStateCompleted,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
	}
	compareCurrExeInfo := copyWorkflowExecutionInfo(currExeInfo)
	currGwmsResponse := &persistence.GetWorkflowExecutionResponse{State: &persistence.WorkflowMutableState{
		ExecutionInfo:    currExeInfo,
		ExecutionStats:   &persistence.ExecutionStats{},
		ReplicationState: forkRepState,
	}}

	gcurResponse := &persistence.GetCurrentExecutionResponse{
		RunID: currRunID,
	}

	readHistoryReq := &persistence.ReadHistoryBranchRequest{
		BranchToken:   forkBranchToken,
		MinEventID:    common.FirstEventID,
		MaxEventID:    int64(35),
		PageSize:      defaultHistoryPageSize,
		NextPageToken: nil,
		ShardID:       &s.shardID,
	}

	taskList := &commonproto.TaskList{
		Name: taskListName,
	}
	readHistoryResp := &persistence.ReadHistoryBranchByBatchResponse{
		NextPageToken:    nil,
		Size:             1000,
		LastFirstEventID: int64(31),
		History: []*commonproto.History{
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   1,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeWorkflowExecutionStarted,
						Attributes: &commonproto.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &commonproto.WorkflowExecutionStartedEventAttributes{
							WorkflowType: &commonproto.WorkflowType{
								Name: wfType,
							},
							TaskList:                            taskList,
							Input:                               []byte("testInput"),
							ExecutionStartToCloseTimeoutSeconds: 100,
							TaskStartToCloseTimeoutSeconds:      200,
						}},
					},
					{
						EventId:   2,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskScheduled,
						Attributes: &commonproto.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &commonproto.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   3,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskStarted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &commonproto.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 2,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   4,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskCompleted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &commonproto.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 2,
							StartedEventId:   3,
						}},
					},
					{
						EventId:   5,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeMarkerRecorded,
						Attributes: &commonproto.HistoryEvent_MarkerRecordedEventAttributes{MarkerRecordedEventAttributes: &commonproto.MarkerRecordedEventAttributes{
							MarkerName:                   "Version",
							Details:                      []byte("details"),
							DecisionTaskCompletedEventId: 4,
						}},
					},
					{
						EventId:   6,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskScheduled,
						Attributes: &commonproto.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &commonproto.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDCompleted1,
							ActivityType: &commonproto.ActivityType{
								Name: "actType0",
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: 1000,
							ScheduleToStartTimeoutSeconds: 2000,
							StartToCloseTimeoutSeconds:    3000,
							HeartbeatTimeoutSeconds:       4000,
							DecisionTaskCompletedEventId:  4,
						}},
					},
					{
						EventId:   7,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeTimerStarted,
						Attributes: &commonproto.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &commonproto.TimerStartedEventAttributes{
							TimerId:                      timerFiredID,
							StartToFireTimeoutSeconds:    2,
							DecisionTaskCompletedEventId: 4,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   8,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskStarted,
						Attributes: &commonproto.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &commonproto.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 6,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   9,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskCompleted,
						Attributes: &commonproto.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &commonproto.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: 6,
							StartedEventId:   8,
						}},
					},
					{
						EventId:   10,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskScheduled,
						Attributes: &commonproto.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &commonproto.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   11,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskStarted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &commonproto.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 10,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   12,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskCompleted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &commonproto.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 10,
							StartedEventId:   11,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   13,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeTimerFired,
						Attributes: &commonproto.HistoryEvent_TimerFiredEventAttributes{TimerFiredEventAttributes: &commonproto.TimerFiredEventAttributes{
							TimerId: timerFiredID,
						}},
					},
					{
						EventId:   14,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskScheduled,
						Attributes: &commonproto.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &commonproto.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   15,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskStarted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &commonproto.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 14,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   16,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskCompleted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &commonproto.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 14,
							StartedEventId:   15,
						}},
					},
					{
						EventId:   17,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskScheduled,
						Attributes: &commonproto.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &commonproto.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDRetry,
							ActivityType: &commonproto.ActivityType{
								Name: "actType1",
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: 1000,
							ScheduleToStartTimeoutSeconds: 2000,
							StartToCloseTimeoutSeconds:    3000,
							HeartbeatTimeoutSeconds:       4000,
							DecisionTaskCompletedEventId:  16,
							RetryPolicy: &commonproto.RetryPolicy{
								InitialIntervalInSeconds:    1,
								BackoffCoefficient:          0.2,
								MaximumAttempts:             10,
								MaximumIntervalInSeconds:    1000,
								ExpirationIntervalInSeconds: math.MaxInt32,
							},
						}},
					},
					{
						EventId:   18,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskScheduled,
						Attributes: &commonproto.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &commonproto.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDNotStarted,
							ActivityType: &commonproto.ActivityType{
								Name: "actType2",
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: 1000,
							ScheduleToStartTimeoutSeconds: 2000,
							StartToCloseTimeoutSeconds:    3000,
							HeartbeatTimeoutSeconds:       4000,
							DecisionTaskCompletedEventId:  16,
						}},
					},
					{
						EventId:   19,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeTimerStarted,
						Attributes: &commonproto.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &commonproto.TimerStartedEventAttributes{
							TimerId:                      timerUnfiredID1,
							StartToFireTimeoutSeconds:    4,
							DecisionTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   20,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeTimerStarted,
						Attributes: &commonproto.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &commonproto.TimerStartedEventAttributes{
							TimerId:                      timerUnfiredID2,
							StartToFireTimeoutSeconds:    8,
							DecisionTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   21,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskScheduled,
						Attributes: &commonproto.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &commonproto.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDCompleted2,
							ActivityType: &commonproto.ActivityType{
								Name: "actType2",
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: 1000,
							ScheduleToStartTimeoutSeconds: 2000,
							StartToCloseTimeoutSeconds:    3000,
							HeartbeatTimeoutSeconds:       4000,
							DecisionTaskCompletedEventId:  16,
						}},
					},
					{
						EventId:   22,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskScheduled,
						Attributes: &commonproto.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &commonproto.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDStartedNoRetry,
							ActivityType: &commonproto.ActivityType{
								Name: "actType2",
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: 1000,
							ScheduleToStartTimeoutSeconds: 2000,
							StartToCloseTimeoutSeconds:    3000,
							HeartbeatTimeoutSeconds:       4000,
							DecisionTaskCompletedEventId:  16,
						}},
					},
					{
						EventId:   23,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeWorkflowExecutionSignaled,
						Attributes: &commonproto.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &commonproto.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName3,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   24,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskStarted,
						Attributes: &commonproto.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &commonproto.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 21,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   25,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeWorkflowExecutionSignaled,
						Attributes: &commonproto.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &commonproto.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName4,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   26,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskStarted,
						Attributes: &commonproto.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &commonproto.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 22,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   27,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskCompleted,
						Attributes: &commonproto.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &commonproto.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: 21,
							StartedEventId:   24,
						}},
					},
					{
						EventId:   28,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskScheduled,
						Attributes: &commonproto.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &commonproto.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   29,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskStarted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &commonproto.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 28,
						}},
					},
				},
			},
			/////////////// reset point/////////////
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   30,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskCompleted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &commonproto.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 28,
							StartedEventId:   29,
						}},
					},
					{
						EventId:   31,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeTimerStarted,
						Attributes: &commonproto.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &commonproto.TimerStartedEventAttributes{
							TimerId:                      timerAfterReset,
							StartToFireTimeoutSeconds:    4,
							DecisionTaskCompletedEventId: 30,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   32,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskStarted,
						Attributes: &commonproto.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &commonproto.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 18,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   33,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeWorkflowExecutionSignaled,
						Attributes: &commonproto.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &commonproto.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName1,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   34,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeWorkflowExecutionSignaled,
						Attributes: &commonproto.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &commonproto.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName2,
						}},
					},
				},
			},
		},
	}

	eid := int64(0)
	timestamp := int64(1000)
	for _, be := range readHistoryResp.History {
		for _, e := range be.Events {
			eid++
			if e.GetEventId() != eid {
				s.Fail(fmt.Sprintf("inconintous eventID: %v, %v", eid, e.GetEventId()))
			}
			e.Timestamp = timestamp
		}
	}

	newBranchToken := []byte("newBranch")
	forkResp := &persistence.ForkHistoryBranchResponse{
		NewBranchToken: newBranchToken,
	}

	appendV2Resp := &persistence.AppendHistoryNodesResponse{
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
	appendReq, ok := appendCall.Arguments[0].(*persistence.AppendHistoryNodesRequest)
	s.Equal(true, ok)
	s.Equal(newBranchToken, appendReq.BranchToken)
	s.Equal(false, appendReq.IsNewBranch)
	s.Equal(5, len(appendReq.Events))
	s.Equal(enums.EventTypeDecisionTaskFailed, enums.EventType(appendReq.Events[0].GetEventType()))
	s.Equal(enums.EventTypeActivityTaskFailed, enums.EventType(appendReq.Events[1].GetEventType()))
	s.Equal(enums.EventTypeWorkflowExecutionSignaled, enums.EventType(appendReq.Events[2].GetEventType()))
	s.Equal(enums.EventTypeWorkflowExecutionSignaled, enums.EventType(appendReq.Events[3].GetEventType()))
	s.Equal(enums.EventTypeDecisionTaskScheduled, enums.EventType(appendReq.Events[4].GetEventType()))

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
	resetReq, ok := resetCall.Arguments[0].(*persistence.ResetWorkflowExecutionRequest)
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
	s.Equal(persistence.TransferTaskTypeActivityTask, resetReq.NewWorkflowSnapshot.TransferTasks[0].GetType())
	s.Equal(persistence.TransferTaskTypeActivityTask, resetReq.NewWorkflowSnapshot.TransferTasks[1].GetType())
	s.Equal(persistence.TransferTaskTypeDecisionTask, resetReq.NewWorkflowSnapshot.TransferTasks[2].GetType())
	s.Equal(persistence.TransferTaskTypeRecordWorkflowStarted, resetReq.NewWorkflowSnapshot.TransferTasks[3].GetType())

	// WF timeout task, user timer, activity timeout timer, activity retry timer
	s.Equal(3, len(resetReq.NewWorkflowSnapshot.TimerTasks))
	s.Equal(persistence.TaskTypeWorkflowTimeout, resetReq.NewWorkflowSnapshot.TimerTasks[0].GetType())
	s.Equal(persistence.TaskTypeUserTimer, resetReq.NewWorkflowSnapshot.TimerTasks[1].GetType())
	s.Equal(persistence.TaskTypeActivityTimeout, resetReq.NewWorkflowSnapshot.TimerTasks[2].GetType())

	s.Equal(2, len(resetReq.NewWorkflowSnapshot.TimerInfos))
	s.assertTimerIDs([]string{timerUnfiredID1, timerUnfiredID2}, resetReq.NewWorkflowSnapshot.TimerInfos)

	s.Equal(2, len(resetReq.NewWorkflowSnapshot.ActivityInfos))
	s.assertActivityIDs([]string{actIDRetry, actIDNotStarted}, resetReq.NewWorkflowSnapshot.ActivityInfos)

	s.Equal(1, len(resetReq.NewWorkflowSnapshot.ReplicationTasks))
	s.Equal(persistence.ReplicationTaskTypeHistory, resetReq.NewWorkflowSnapshot.ReplicationTasks[0].GetType())

	compareRepState := copyReplicationState(forkRepState)
	compareRepState.StartVersion = beforeResetVersion
	compareRepState.CurrentVersion = afterResetVersion
	compareRepState.LastWriteEventID = 34
	compareRepState.LastWriteVersion = afterResetVersion
	compareRepState.LastReplicationInfo = map[string]*replication.ReplicationInfo{
		"standby": &replication.ReplicationInfo{
			LastEventId: 29,
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
		&persistence.DomainInfo{ID: testDomainID},
		&persistence.DomainConfig{Retention: 1},
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
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
	we := commonproto.WorkflowExecution{
		WorkflowId: wid,
		RunId:      newRunID,
	}

	forkGwmsRequest := &persistence.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: commonproto.WorkflowExecution{
			WorkflowId: wid,
			RunId:      forkRunID,
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
	forkExeInfo := &persistence.WorkflowExecutionInfo{
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

	forkRepState := &persistence.ReplicationState{
		CurrentVersion:      beforeResetVersion,
		StartVersion:        beforeResetVersion,
		LastWriteEventID:    common.EmptyEventID,
		LastWriteVersion:    common.EmptyVersion,
		LastReplicationInfo: map[string]*replication.ReplicationInfo{},
	}
	forkGwmsResponse := &persistence.GetWorkflowExecutionResponse{State: &persistence.WorkflowMutableState{
		ExecutionInfo:    forkExeInfo,
		ExecutionStats:   &persistence.ExecutionStats{},
		ReplicationState: forkRepState,
	}}

	currGwmsRequest := &persistence.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: commonproto.WorkflowExecution{
			WorkflowId: wid,
			RunId:      currRunID,
		},
	}
	currExeInfo := &persistence.WorkflowExecutionInfo{
		DomainID:           domainID,
		WorkflowID:         wid,
		WorkflowTypeName:   wfType,
		TaskList:           taskListName,
		RunID:              currRunID,
		NextEventID:        common.FirstEventID,
		State:              persistence.WorkflowStateCompleted,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
	}
	compareCurrExeInfo := copyWorkflowExecutionInfo(currExeInfo)
	currGwmsResponse := &persistence.GetWorkflowExecutionResponse{State: &persistence.WorkflowMutableState{
		ExecutionInfo:    currExeInfo,
		ExecutionStats:   &persistence.ExecutionStats{},
		ReplicationState: forkRepState,
	}}

	readHistoryReq := &persistence.ReadHistoryBranchRequest{
		BranchToken:   forkBranchToken,
		MinEventID:    common.FirstEventID,
		MaxEventID:    int64(30),
		PageSize:      defaultHistoryPageSize,
		NextPageToken: nil,
		ShardID:       &s.shardID,
	}

	taskList := &commonproto.TaskList{
		Name: taskListName,
	}

	readHistoryResp := &persistence.ReadHistoryBranchByBatchResponse{
		NextPageToken:    nil,
		Size:             1000,
		LastFirstEventID: int64(31),
		History: []*commonproto.History{
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   1,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeWorkflowExecutionStarted,
						Attributes: &commonproto.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &commonproto.WorkflowExecutionStartedEventAttributes{
							WorkflowType: &commonproto.WorkflowType{
								Name: wfType,
							},
							TaskList:                            taskList,
							Input:                               []byte("testInput"),
							ExecutionStartToCloseTimeoutSeconds: 100,
							TaskStartToCloseTimeoutSeconds:      200,
						}},
					},
					{
						EventId:   2,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskScheduled,
						Attributes: &commonproto.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &commonproto.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   3,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskStarted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &commonproto.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 2,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   4,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskCompleted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &commonproto.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 2,
							StartedEventId:   3,
						}},
					},
					{
						EventId:   5,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeMarkerRecorded,
						Attributes: &commonproto.HistoryEvent_MarkerRecordedEventAttributes{MarkerRecordedEventAttributes: &commonproto.MarkerRecordedEventAttributes{
							MarkerName:                   "Version",
							Details:                      []byte("details"),
							DecisionTaskCompletedEventId: 4,
						}},
					},
					{
						EventId:   6,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskScheduled,
						Attributes: &commonproto.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &commonproto.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDCompleted1,
							ActivityType: &commonproto.ActivityType{
								Name: "actType0",
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: 1000,
							ScheduleToStartTimeoutSeconds: 2000,
							StartToCloseTimeoutSeconds:    3000,
							HeartbeatTimeoutSeconds:       4000,
							DecisionTaskCompletedEventId:  4,
						}},
					},
					{
						EventId:   7,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeTimerStarted,
						Attributes: &commonproto.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &commonproto.TimerStartedEventAttributes{
							TimerId:                      timerFiredID,
							StartToFireTimeoutSeconds:    2,
							DecisionTaskCompletedEventId: 4,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   8,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskStarted,
						Attributes: &commonproto.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &commonproto.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 6,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   9,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskCompleted,
						Attributes: &commonproto.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &commonproto.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: 6,
							StartedEventId:   8,
						}},
					},
					{
						EventId:   10,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskScheduled,
						Attributes: &commonproto.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &commonproto.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   11,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskStarted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &commonproto.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 10,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   12,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskCompleted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &commonproto.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 10,
							StartedEventId:   11,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   13,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeTimerFired,
						Attributes: &commonproto.HistoryEvent_TimerFiredEventAttributes{TimerFiredEventAttributes: &commonproto.TimerFiredEventAttributes{
							TimerId: timerFiredID,
						}},
					},
					{
						EventId:   14,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskScheduled,
						Attributes: &commonproto.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &commonproto.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   15,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskStarted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &commonproto.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 14,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   16,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskCompleted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &commonproto.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 14,
							StartedEventId:   15,
						}},
					},
					{
						EventId:   17,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskScheduled,
						Attributes: &commonproto.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &commonproto.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDRetry,
							ActivityType: &commonproto.ActivityType{
								Name: "actType1",
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: 1000,
							ScheduleToStartTimeoutSeconds: 2000,
							StartToCloseTimeoutSeconds:    3000,
							HeartbeatTimeoutSeconds:       4000,
							DecisionTaskCompletedEventId:  16,
							RetryPolicy: &commonproto.RetryPolicy{
								InitialIntervalInSeconds:    1,
								BackoffCoefficient:          0.2,
								MaximumAttempts:             10,
								MaximumIntervalInSeconds:    1000,
								ExpirationIntervalInSeconds: math.MaxInt32,
							},
						}},
					},
					{
						EventId:   18,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskScheduled,
						Attributes: &commonproto.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &commonproto.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDNotStarted,
							ActivityType: &commonproto.ActivityType{
								Name: "actType2",
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: 1000,
							ScheduleToStartTimeoutSeconds: 2000,
							StartToCloseTimeoutSeconds:    3000,
							HeartbeatTimeoutSeconds:       4000,
							DecisionTaskCompletedEventId:  16,
						}},
					},
					{
						EventId:   19,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeTimerStarted,
						Attributes: &commonproto.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &commonproto.TimerStartedEventAttributes{
							TimerId:                      timerUnfiredID1,
							StartToFireTimeoutSeconds:    4,
							DecisionTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   20,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeTimerStarted,
						Attributes: &commonproto.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &commonproto.TimerStartedEventAttributes{
							TimerId:                      timerUnfiredID2,
							StartToFireTimeoutSeconds:    8,
							DecisionTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   21,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskScheduled,
						Attributes: &commonproto.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &commonproto.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDCompleted2,
							ActivityType: &commonproto.ActivityType{
								Name: "actType2",
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: 1000,
							ScheduleToStartTimeoutSeconds: 2000,
							StartToCloseTimeoutSeconds:    3000,
							HeartbeatTimeoutSeconds:       4000,
							DecisionTaskCompletedEventId:  16,
						}},
					},
					{
						EventId:   22,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskScheduled,
						Attributes: &commonproto.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &commonproto.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDStartedNoRetry,
							ActivityType: &commonproto.ActivityType{
								Name: "actType2",
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: 1000,
							ScheduleToStartTimeoutSeconds: 2000,
							StartToCloseTimeoutSeconds:    3000,
							HeartbeatTimeoutSeconds:       4000,
							DecisionTaskCompletedEventId:  16,
						}},
					},
					{
						EventId:   23,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeWorkflowExecutionSignaled,
						Attributes: &commonproto.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &commonproto.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName3,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   24,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskStarted,
						Attributes: &commonproto.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &commonproto.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 21,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   25,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeWorkflowExecutionSignaled,
						Attributes: &commonproto.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &commonproto.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName4,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   26,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskStarted,
						Attributes: &commonproto.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &commonproto.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 22,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   27,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeActivityTaskCompleted,
						Attributes: &commonproto.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &commonproto.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: 21,
							StartedEventId:   24,
						}},
					},
					{
						EventId:   28,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskScheduled,
						Attributes: &commonproto.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &commonproto.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*commonproto.HistoryEvent{
					{
						EventId:   29,
						Version:   beforeResetVersion,
						EventType: enums.EventTypeDecisionTaskStarted,
						Attributes: &commonproto.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &commonproto.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 28,
						}},
					},
				},
			},
			/////////////// reset point/////////////
		},
	}

	eid := int64(0)
	timestamp := int64(1000)
	for _, be := range readHistoryResp.History {
		for _, e := range be.Events {
			eid++
			if e.GetEventId() != eid {
				s.Fail(fmt.Sprintf("inconintous eventID: %v, %v", eid, e.GetEventId()))
			}
			e.Timestamp = timestamp
		}
	}

	newBranchToken := []byte("newBranch")
	forkReq := &persistence.ForkHistoryBranchRequest{
		ForkBranchToken: forkBranchToken,
		ForkNodeID:      30,
		Info:            persistence.BuildHistoryGarbageCleanupInfo(domainID, wid, newRunID),
		ShardID:         &s.shardID,
	}
	forkResp := &persistence.ForkHistoryBranchResponse{
		NewBranchToken: newBranchToken,
	}

	historyAfterReset := &commonproto.History{
		Events: []*commonproto.HistoryEvent{
			&commonproto.HistoryEvent{
				EventId:   30,
				Version:   afterResetVersion,
				EventType: enums.EventTypeDecisionTaskFailed,
				Attributes: &commonproto.HistoryEvent_DecisionTaskFailedEventAttributes{DecisionTaskFailedEventAttributes: &commonproto.DecisionTaskFailedEventAttributes{
					ScheduledEventId: int64(28),
					StartedEventId:   int64(29),
					Cause:            enums.DecisionTaskFailedCauseResetWorkflow,
					Details:          nil,
					Identity:         identityHistoryService,
					Reason:           "resetWFtest",
					BaseRunId:        forkRunID,
					NewRunId:         newRunID,
					ForkEventVersion: beforeResetVersion,
				}},
			},
			{
				EventId:   31,
				Version:   afterResetVersion,
				EventType: enums.EventTypeActivityTaskFailed,
				Attributes: &commonproto.HistoryEvent_ActivityTaskFailedEventAttributes{ActivityTaskFailedEventAttributes: &commonproto.ActivityTaskFailedEventAttributes{
					Reason:           "resetWF",
					ScheduledEventId: 22,
					StartedEventId:   26,
					Identity:         identityHistoryService,
				}},
			},
			{
				EventId:   32,
				Version:   afterResetVersion,
				EventType: enums.EventTypeWorkflowExecutionSignaled,
				Attributes: &commonproto.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &commonproto.WorkflowExecutionSignaledEventAttributes{
					SignalName: signalName1,
				}},
			},
			{
				EventId:   33,
				Version:   afterResetVersion,
				EventType: enums.EventTypeWorkflowExecutionSignaled,
				Attributes: &commonproto.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &commonproto.WorkflowExecutionSignaledEventAttributes{
					SignalName: signalName2,
				}},
			},
			{
				EventId:   34,
				Version:   afterResetVersion,
				EventType: enums.EventTypeDecisionTaskScheduled,
				Attributes: &commonproto.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &commonproto.DecisionTaskScheduledEventAttributes{
					TaskList:                   taskList,
					StartToCloseTimeoutSeconds: 100,
				}},
			},
		},
	}

	appendV2Resp := &persistence.AppendHistoryNodesResponse{
		Size: 200,
	}

	request := &historyservice.ReplicateEventsRequest{
		SourceCluster:     "standby",
		DomainUUID:        domainID,
		WorkflowExecution: &we,
		FirstEventId:      30,
		NextEventId:       35,
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
	appendReq, ok := appendCall.Arguments[0].(*persistence.AppendHistoryNodesRequest)
	s.Equal(true, ok)
	s.Equal(newBranchToken, appendReq.BranchToken)
	s.Equal(false, appendReq.IsNewBranch)
	s.Equal(5, len(appendReq.Events))
	s.Equal(enums.EventTypeDecisionTaskFailed, enums.EventType(appendReq.Events[0].GetEventType()))
	s.Equal(enums.EventTypeActivityTaskFailed, enums.EventType(appendReq.Events[1].GetEventType()))
	s.Equal(enums.EventTypeWorkflowExecutionSignaled, enums.EventType(appendReq.Events[2].GetEventType()))
	s.Equal(enums.EventTypeWorkflowExecutionSignaled, enums.EventType(appendReq.Events[3].GetEventType()))
	s.Equal(enums.EventTypeDecisionTaskScheduled, enums.EventType(appendReq.Events[4].GetEventType()))

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
	resetReq, ok := resetCall.Arguments[0].(*persistence.ResetWorkflowExecutionRequest)
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
	s.Equal(persistence.TransferTaskTypeActivityTask, resetReq.NewWorkflowSnapshot.TransferTasks[0].GetType())
	s.Equal(persistence.TransferTaskTypeActivityTask, resetReq.NewWorkflowSnapshot.TransferTasks[1].GetType())
	s.Equal(persistence.TransferTaskTypeDecisionTask, resetReq.NewWorkflowSnapshot.TransferTasks[2].GetType())

	// WF timeout task, user timer, activity timeout timer, activity retry timer
	s.Equal(3, len(resetReq.NewWorkflowSnapshot.TimerTasks))
	s.Equal(persistence.TaskTypeWorkflowTimeout, resetReq.NewWorkflowSnapshot.TimerTasks[0].GetType())
	s.Equal(persistence.TaskTypeUserTimer, resetReq.NewWorkflowSnapshot.TimerTasks[1].GetType())
	s.Equal(persistence.TaskTypeActivityTimeout, resetReq.NewWorkflowSnapshot.TimerTasks[2].GetType())

	s.Equal(2, len(resetReq.NewWorkflowSnapshot.TimerInfos))
	s.assertTimerIDs([]string{timerUnfiredID1, timerUnfiredID2}, resetReq.NewWorkflowSnapshot.TimerInfos)

	s.Equal(2, len(resetReq.NewWorkflowSnapshot.ActivityInfos))
	s.assertActivityIDs([]string{actIDRetry, actIDNotStarted}, resetReq.NewWorkflowSnapshot.ActivityInfos)

	compareRepState := copyReplicationState(forkRepState)
	compareRepState.StartVersion = beforeResetVersion
	compareRepState.CurrentVersion = afterResetVersion
	compareRepState.LastWriteEventID = 34
	compareRepState.LastWriteVersion = afterResetVersion
	compareRepState.LastReplicationInfo = map[string]*replication.ReplicationInfo{
		"standby": &replication.ReplicationInfo{
			LastEventId: 29,
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
	_, pt = FindAutoResetPoint(timeSource, &commonproto.BadBinaries{}, &commonproto.ResetPoints{})
	assert.Nil(t, pt)

	pt0 := &commonproto.ResetPointInfo{
		BinaryChecksum: "abc",
		Resettable:     true,
	}
	pt1 := &commonproto.ResetPointInfo{
		BinaryChecksum: "def",
		Resettable:     true,
	}
	pt3 := &commonproto.ResetPointInfo{
		BinaryChecksum: "ghi",
		Resettable:     false,
	}

	expiredNowNano := time.Now().UnixNano() - int64(time.Hour)
	notExpiredNowNano := time.Now().UnixNano() + int64(time.Hour)
	pt4 := &commonproto.ResetPointInfo{
		BinaryChecksum:   "expired",
		Resettable:       true,
		ExpiringTimeNano: expiredNowNano,
	}

	pt5 := &commonproto.ResetPointInfo{
		BinaryChecksum:   "notExpired",
		Resettable:       true,
		ExpiringTimeNano: notExpiredNowNano,
	}

	// case 3: two intersection
	_, pt = FindAutoResetPoint(timeSource, &commonproto.BadBinaries{
		Binaries: map[string]*commonproto.BadBinaryInfo{
			"abc": {},
			"def": {},
		},
	}, &commonproto.ResetPoints{
		Points: []*commonproto.ResetPointInfo{
			pt0, pt1, pt3,
		},
	})
	assert.Equal(t, pt.String(), pt0.String())

	// case 4: one intersection
	_, pt = FindAutoResetPoint(timeSource, &commonproto.BadBinaries{
		Binaries: map[string]*commonproto.BadBinaryInfo{
			"none":    {},
			"def":     {},
			"expired": {},
		},
	}, &commonproto.ResetPoints{
		Points: []*commonproto.ResetPointInfo{
			pt4, pt0, pt1, pt3,
		},
	})
	assert.Equal(t, pt.String(), pt1.String())

	// case 4: no intersection
	_, pt = FindAutoResetPoint(timeSource, &commonproto.BadBinaries{
		Binaries: map[string]*commonproto.BadBinaryInfo{
			"none1": {},
			"none2": {},
		},
	}, &commonproto.ResetPoints{
		Points: []*commonproto.ResetPointInfo{
			pt0, pt1, pt3,
		},
	})
	assert.Nil(t, pt)

	// case 5: not resettable
	_, pt = FindAutoResetPoint(timeSource, &commonproto.BadBinaries{
		Binaries: map[string]*commonproto.BadBinaryInfo{
			"none1": {},
			"ghi":   {},
		},
	}, &commonproto.ResetPoints{
		Points: []*commonproto.ResetPointInfo{
			pt0, pt1, pt3,
		},
	})
	assert.Nil(t, pt)

	// case 6: one intersection of expired
	_, pt = FindAutoResetPoint(timeSource, &commonproto.BadBinaries{
		Binaries: map[string]*commonproto.BadBinaryInfo{
			"none":    {},
			"expired": {},
		},
	}, &commonproto.ResetPoints{
		Points: []*commonproto.ResetPointInfo{
			pt0, pt1, pt3, pt4, pt5,
		},
	})
	assert.Nil(t, pt)

	// case 7: one intersection of not expired
	_, pt = FindAutoResetPoint(timeSource, &commonproto.BadBinaries{
		Binaries: map[string]*commonproto.BadBinaryInfo{
			"none":       {},
			"notExpired": {},
		},
	}, &commonproto.ResetPoints{
		Points: []*commonproto.ResetPointInfo{
			pt0, pt1, pt3, pt4, pt5,
		},
	})
	assert.Equal(t, pt.String(), pt5.String())
}
