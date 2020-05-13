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
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commongenpb "github.com/temporalio/temporal/.gen/proto/common"
	executiongenpb "github.com/temporalio/temporal/.gen/proto/execution"
	"github.com/temporalio/temporal/.gen/proto/historyservice"
	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	replicationgenpb "github.com/temporalio/temporal/.gen/proto/replication"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/clock"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/mocks"
	"github.com/temporalio/temporal/common/payloads"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/primitives"
	commonpb "go.temporal.io/temporal-proto/common"
	eventpb "go.temporal.io/temporal-proto/event"
	executionpb "go.temporal.io/temporal-proto/execution"
	namespacepb "go.temporal.io/temporal-proto/namespace"
	"go.temporal.io/temporal-proto/serviceerror"
	tasklistpb "go.temporal.io/temporal-proto/tasklist"
	"go.temporal.io/temporal-proto/workflowservice"
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
		mockNamespaceCache       *cache.MockNamespaceCache
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
				ShardId:          int32(shardID),
				RangeId:          1,
				TransferAckLevel: 0,
			}},
		s.config,
	)

	s.mockExecutionMgr = s.mockShard.resource.ExecutionMgr
	s.mockHistoryV2Mgr = s.mockShard.resource.HistoryMgr
	s.mockNamespaceCache = s.mockShard.resource.NamespaceCache
	s.mockClusterMetadata = s.mockShard.resource.ClusterMetadata
	s.mockEventsCache = s.mockShard.mockEventsCache
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
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
	testNamespaceEntry := cache.NewLocalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Id: testNamespaceID}, &persistenceblobs.NamespaceConfig{RetentionDays: 1}, "", nil,
	)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()

	request := &historyservice.ResetWorkflowExecutionRequest{}
	namespaceID := testNamespaceID
	request.NamespaceId = namespaceID
	request.ResetRequest = &workflowservice.ResetWorkflowExecutionRequest{}

	wid := "wId"
	wfType := "wfType"
	taskListName := "taskList"
	forkRunID := uuid.New().String()
	currRunID := uuid.New().String()
	we := executionpb.WorkflowExecution{
		WorkflowId: wid,
		RunId:      forkRunID,
	}
	request.ResetRequest = &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:             "testNamespace",
		WorkflowExecution:     &we,
		Reason:                "test reset",
		DecisionFinishEventId: 29,
		RequestId:             uuid.New().String(),
	}

	forkGwmsRequest := &persistence.GetWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		Execution: executionpb.WorkflowExecution{
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
		NamespaceID:        namespaceID,
		WorkflowID:         wid,
		WorkflowTypeName:   wfType,
		TaskList:           taskListName,
		RunID:              forkRunID,
		BranchToken:        forkBranchToken,
		NextEventID:        34,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
		State:              executiongenpb.WorkflowExecutionState_WorkflowExecutionState_Created,
	}
	forkGwmsResponse := &persistence.GetWorkflowExecutionResponse{State: &persistence.WorkflowMutableState{
		ExecutionInfo:  forkExeInfo,
		ExecutionStats: &persistence.ExecutionStats{},
	}}

	currGwmsRequest := &persistence.GetWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		Execution: executionpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      currRunID,
		},
	}
	currExeInfo := &persistence.WorkflowExecutionInfo{
		NamespaceID:        namespaceID,
		WorkflowID:         wid,
		WorkflowTypeName:   wfType,
		TaskList:           taskListName,
		RunID:              currRunID,
		NextEventID:        common.FirstEventID,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
		State:              executiongenpb.WorkflowExecutionState_WorkflowExecutionState_Created,
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

	taskList := &tasklistpb.TaskList{
		Name: taskListName,
	}
	readHistoryResp := &persistence.ReadHistoryBranchByBatchResponse{
		NextPageToken:    nil,
		Size:             1000,
		LastFirstEventID: int64(31),
		History: []*eventpb.History{
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   1,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_WorkflowExecutionStarted,
						Attributes: &eventpb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &eventpb.WorkflowExecutionStartedEventAttributes{
							WorkflowType: &commonpb.WorkflowType{
								Name: wfType,
							},
							TaskList:                        taskList,
							Input:                           payloads.EncodeString("testInput"),
							WorkflowExecutionTimeoutSeconds: 100,
							WorkflowRunTimeoutSeconds:       50,
							WorkflowTaskTimeoutSeconds:      200,
						}},
					},
					{
						EventId:   2,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_DecisionTaskScheduled,
						Attributes: &eventpb.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &eventpb.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   3,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_DecisionTaskStarted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &eventpb.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 2,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   4,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_DecisionTaskCompleted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &eventpb.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 2,
							StartedEventId:   3,
						}},
					},
					{
						EventId:   5,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_MarkerRecorded,
						Attributes: &eventpb.HistoryEvent_MarkerRecordedEventAttributes{MarkerRecordedEventAttributes: &eventpb.MarkerRecordedEventAttributes{
							MarkerName:                   "Version",
							Details:                      payloads.EncodeString("details"),
							DecisionTaskCompletedEventId: 4,
						}},
					},
					{
						EventId:   6,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_ActivityTaskScheduled,
						Attributes: &eventpb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &eventpb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDCompleted1,
							ActivityType: &commonpb.ActivityType{
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
						EventType: eventpb.EventType_TimerStarted,
						Attributes: &eventpb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &eventpb.TimerStartedEventAttributes{
							TimerId:                      timerFiredID,
							StartToFireTimeoutSeconds:    2,
							DecisionTaskCompletedEventId: 4,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   8,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_ActivityTaskStarted,
						Attributes: &eventpb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &eventpb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 6,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   9,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_ActivityTaskCompleted,
						Attributes: &eventpb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &eventpb.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: 6,
							StartedEventId:   8,
						}},
					},
					{
						EventId:   10,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_DecisionTaskScheduled,
						Attributes: &eventpb.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &eventpb.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   11,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_DecisionTaskStarted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &eventpb.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 10,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   12,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_DecisionTaskCompleted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &eventpb.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 10,
							StartedEventId:   11,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   13,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_TimerFired,
						Attributes: &eventpb.HistoryEvent_TimerFiredEventAttributes{TimerFiredEventAttributes: &eventpb.TimerFiredEventAttributes{
							TimerId: timerFiredID,
						}},
					},
					{
						EventId:   14,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_DecisionTaskScheduled,
						Attributes: &eventpb.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &eventpb.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   15,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_DecisionTaskStarted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &eventpb.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 14,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   16,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_DecisionTaskCompleted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &eventpb.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 14,
							StartedEventId:   15,
						}},
					},
					{
						EventId:   17,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_ActivityTaskScheduled,
						Attributes: &eventpb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &eventpb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDStarted1,
							ActivityType: &commonpb.ActivityType{
								Name: "actType1",
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: 1000,
							ScheduleToStartTimeoutSeconds: 2000,
							StartToCloseTimeoutSeconds:    3000,
							HeartbeatTimeoutSeconds:       4000,
							DecisionTaskCompletedEventId:  16,
							RetryPolicy: &commonpb.RetryPolicy{
								InitialIntervalInSeconds: 1,
								BackoffCoefficient:       0.2,
								MaximumAttempts:          10,
								MaximumIntervalInSeconds: 1000,
							},
						}},
					},
					{
						EventId:   18,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_ActivityTaskScheduled,
						Attributes: &eventpb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &eventpb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDNotStarted,
							ActivityType: &commonpb.ActivityType{
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
						EventType: eventpb.EventType_TimerStarted,
						Attributes: &eventpb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &eventpb.TimerStartedEventAttributes{
							TimerId:                      timerUnfiredID1,
							StartToFireTimeoutSeconds:    4,
							DecisionTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   20,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_TimerStarted,
						Attributes: &eventpb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &eventpb.TimerStartedEventAttributes{
							TimerId:                      timerUnfiredID2,
							StartToFireTimeoutSeconds:    8,
							DecisionTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   21,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_ActivityTaskScheduled,
						Attributes: &eventpb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &eventpb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDCompleted2,
							ActivityType: &commonpb.ActivityType{
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
						EventType: eventpb.EventType_ActivityTaskScheduled,
						Attributes: &eventpb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &eventpb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDStarted2,
							ActivityType: &commonpb.ActivityType{
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
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   23,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_ActivityTaskStarted,
						Attributes: &eventpb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &eventpb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 21,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   24,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_ActivityTaskStarted,
						Attributes: &eventpb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &eventpb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 17,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   25,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_ActivityTaskStarted,
						Attributes: &eventpb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &eventpb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 22,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   26,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_ActivityTaskCompleted,
						Attributes: &eventpb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &eventpb.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: 21,
							StartedEventId:   23,
						}},
					},
					{
						EventId:   27,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_DecisionTaskScheduled,
						Attributes: &eventpb.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &eventpb.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   28,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_DecisionTaskStarted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &eventpb.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 27,
						}},
					},
				},
			},
			/////////////// reset point/////////////
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   29,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_DecisionTaskCompleted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &eventpb.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 27,
							StartedEventId:   28,
						}},
					},
					{
						EventId:   30,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_TimerStarted,
						Attributes: &eventpb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &eventpb.TimerStartedEventAttributes{
							TimerId:                      timerAfterReset,
							StartToFireTimeoutSeconds:    4,
							DecisionTaskCompletedEventId: 29,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   31,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_ActivityTaskStarted,
						Attributes: &eventpb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &eventpb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 18,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   32,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_WorkflowExecutionSignaled,
						Attributes: &eventpb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &eventpb.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName1,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   33,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_WorkflowExecutionSignaled,
						Attributes: &eventpb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &eventpb.WorkflowExecutionSignaledEventAttributes{
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
	s.Equal(eventpb.EventType_DecisionTaskFailed, eventpb.EventType(appendReq.Events[0].GetEventType()))
	s.Equal(eventpb.EventType_ActivityTaskFailed, eventpb.EventType(appendReq.Events[1].GetEventType()))
	s.Equal(eventpb.EventType_ActivityTaskFailed, eventpb.EventType(appendReq.Events[2].GetEventType()))
	s.Equal(eventpb.EventType_WorkflowExecutionSignaled, eventpb.EventType(appendReq.Events[3].GetEventType()))
	s.Equal(eventpb.EventType_WorkflowExecutionSignaled, eventpb.EventType(appendReq.Events[4].GetEventType()))
	s.Equal(eventpb.EventType_DecisionTaskScheduled, eventpb.EventType(appendReq.Events[5].GetEventType()))

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
	compareCurrExeInfo.State = executiongenpb.WorkflowExecutionState_WorkflowExecutionState_Completed
	compareCurrExeInfo.Status = executionpb.WorkflowExecutionStatus_Terminated
	compareCurrExeInfo.NextEventID = 2
	compareCurrExeInfo.CompletionEventBatchID = 1
	s.Equal(compareCurrExeInfo, resetReq.CurrentWorkflowMutation.ExecutionInfo)
	s.Equal(1, len(resetReq.CurrentWorkflowMutation.TransferTasks))
	s.Equal(1, len(resetReq.CurrentWorkflowMutation.TimerTasks))
	s.Equal(commongenpb.TaskType_TransferCloseExecution, resetReq.CurrentWorkflowMutation.TransferTasks[0].GetType())
	s.Equal(commongenpb.TaskType_DeleteHistoryEvent, resetReq.CurrentWorkflowMutation.TimerTasks[0].GetType())
	s.Equal(int64(200), resetReq.CurrentWorkflowMutation.ExecutionStats.HistorySize)

	s.Equal("wfType", resetReq.NewWorkflowSnapshot.ExecutionInfo.WorkflowTypeName)
	s.True(len(resetReq.NewWorkflowSnapshot.ExecutionInfo.RunID) > 0)
	s.Equal([]byte(newBranchToken), resetReq.NewWorkflowSnapshot.ExecutionInfo.BranchToken)
	// 35 = resetEventID(29) + 6 in a batch
	s.Equal(int64(34), resetReq.NewWorkflowSnapshot.ExecutionInfo.DecisionScheduleID)
	s.Equal(int64(35), resetReq.NewWorkflowSnapshot.ExecutionInfo.NextEventID)

	// one activity task, one decision task and one record workflow started task
	s.Equal(3, len(resetReq.NewWorkflowSnapshot.TransferTasks))
	s.Equal(commongenpb.TaskType_TransferActivityTask, resetReq.NewWorkflowSnapshot.TransferTasks[0].GetType())
	s.Equal(commongenpb.TaskType_TransferDecisionTask, resetReq.NewWorkflowSnapshot.TransferTasks[1].GetType())
	s.Equal(commongenpb.TaskType_TransferRecordWorkflowStarted, resetReq.NewWorkflowSnapshot.TransferTasks[2].GetType())

	// WF timeout task, user timer, activity timeout timer, activity retry timer
	s.Equal(3, len(resetReq.NewWorkflowSnapshot.TimerTasks))
	s.Equal(commongenpb.TaskType_WorkflowRunTimeout, resetReq.NewWorkflowSnapshot.TimerTasks[0].GetType())
	s.Equal(commongenpb.TaskType_UserTimer, resetReq.NewWorkflowSnapshot.TimerTasks[1].GetType())
	s.Equal(commongenpb.TaskType_ActivityTimeout, resetReq.NewWorkflowSnapshot.TimerTasks[2].GetType())

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
		delete(m, t.GetTimerId())
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
	testNamespaceEntry := cache.NewLocalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Id: testNamespaceID}, &persistenceblobs.NamespaceConfig{RetentionDays: 1}, "", nil,
	)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()

	request := &historyservice.ResetWorkflowExecutionRequest{}
	namespaceID := testNamespaceID
	request.NamespaceId = namespaceID
	request.ResetRequest = &workflowservice.ResetWorkflowExecutionRequest{}

	wid := "wId"
	wfType := "wfType"
	taskListName := "taskList"
	forkRunID := uuid.New().String()
	currRunID := uuid.New().String()
	we := executionpb.WorkflowExecution{
		WorkflowId: wid,
		RunId:      forkRunID,
	}
	request.ResetRequest = &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:             "testNamespace",
		WorkflowExecution:     &we,
		Reason:                "test reset",
		DecisionFinishEventId: 30,
		RequestId:             uuid.New().String(),
	}

	forkGwmsRequest := &persistence.GetWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		Execution: executionpb.WorkflowExecution{
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
	cancelWE := &executionpb.WorkflowExecution{
		WorkflowId: "cancel-wfid",
		RunId:      uuid.New().String(),
	}
	forkBranchToken := []byte("forkBranchToken")
	forkExeInfo := &persistence.WorkflowExecutionInfo{
		NamespaceID:        namespaceID,
		WorkflowID:         wid,
		WorkflowTypeName:   wfType,
		TaskList:           taskListName,
		RunID:              forkRunID,
		BranchToken:        forkBranchToken,
		NextEventID:        35,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
		State:              executiongenpb.WorkflowExecutionState_WorkflowExecutionState_Created,
	}
	forkGwmsResponse := &persistence.GetWorkflowExecutionResponse{State: &persistence.WorkflowMutableState{
		ExecutionInfo:  forkExeInfo,
		ExecutionStats: &persistence.ExecutionStats{},
	}}

	currGwmsRequest := &persistence.GetWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		Execution: executionpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      currRunID,
		},
	}
	currExeInfo := &persistence.WorkflowExecutionInfo{
		NamespaceID:        namespaceID,
		WorkflowID:         wid,
		WorkflowTypeName:   wfType,
		TaskList:           taskListName,
		RunID:              currRunID,
		NextEventID:        common.FirstEventID,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
		State:              executiongenpb.WorkflowExecutionState_WorkflowExecutionState_Created,
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

	taskList := &tasklistpb.TaskList{
		Name: taskListName,
	}
	readHistoryResp := &persistence.ReadHistoryBranchByBatchResponse{
		NextPageToken:    nil,
		Size:             1000,
		LastFirstEventID: int64(31),
		History: []*eventpb.History{
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   1,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_WorkflowExecutionStarted,
						Attributes: &eventpb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &eventpb.WorkflowExecutionStartedEventAttributes{
							WorkflowType: &commonpb.WorkflowType{
								Name: wfType,
							},
							TaskList:                        taskList,
							Input:                           payloads.EncodeString("testInput"),
							WorkflowExecutionTimeoutSeconds: 100,
							WorkflowTaskTimeoutSeconds:      200,
						}},
					},
					{
						EventId:   2,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_DecisionTaskScheduled,
						Attributes: &eventpb.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &eventpb.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   3,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_DecisionTaskStarted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &eventpb.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 2,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   4,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_DecisionTaskCompleted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &eventpb.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 2,
							StartedEventId:   3,
						}},
					},
					{
						EventId:   5,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_MarkerRecorded,
						Attributes: &eventpb.HistoryEvent_MarkerRecordedEventAttributes{MarkerRecordedEventAttributes: &eventpb.MarkerRecordedEventAttributes{
							MarkerName:                   "Version",
							Details:                      payloads.EncodeString("details"),
							DecisionTaskCompletedEventId: 4,
						}},
					},
					{
						EventId:   6,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_ActivityTaskScheduled,
						Attributes: &eventpb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &eventpb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDCompleted1,
							ActivityType: &commonpb.ActivityType{
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
						EventType: eventpb.EventType_TimerStarted,
						Attributes: &eventpb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &eventpb.TimerStartedEventAttributes{
							TimerId:                      timerFiredID,
							StartToFireTimeoutSeconds:    2,
							DecisionTaskCompletedEventId: 4,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   8,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_ActivityTaskStarted,
						Attributes: &eventpb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &eventpb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 6,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   9,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_ActivityTaskCompleted,
						Attributes: &eventpb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &eventpb.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: 6,
							StartedEventId:   8,
						}},
					},
					{
						EventId:   10,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_DecisionTaskScheduled,
						Attributes: &eventpb.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &eventpb.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   11,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_DecisionTaskStarted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &eventpb.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 10,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   12,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_DecisionTaskCompleted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &eventpb.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 10,
							StartedEventId:   11,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   13,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_TimerFired,
						Attributes: &eventpb.HistoryEvent_TimerFiredEventAttributes{TimerFiredEventAttributes: &eventpb.TimerFiredEventAttributes{
							TimerId: timerFiredID,
						}},
					},
					{
						EventId:   14,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_DecisionTaskScheduled,
						Attributes: &eventpb.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &eventpb.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   15,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_DecisionTaskStarted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &eventpb.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 14,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   16,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_DecisionTaskCompleted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &eventpb.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 14,
							StartedEventId:   15,
						}},
					},
					{
						EventId:   17,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_ActivityTaskScheduled,
						Attributes: &eventpb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &eventpb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDStartedRetry,
							ActivityType: &commonpb.ActivityType{
								Name: "actType1",
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: 1000,
							ScheduleToStartTimeoutSeconds: 2000,
							StartToCloseTimeoutSeconds:    3000,
							HeartbeatTimeoutSeconds:       4000,
							DecisionTaskCompletedEventId:  16,
							RetryPolicy: &commonpb.RetryPolicy{
								InitialIntervalInSeconds: 1,
								BackoffCoefficient:       0.2,
								MaximumAttempts:          10,
								MaximumIntervalInSeconds: 1000,
							},
						}},
					},
					{
						EventId:   18,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_ActivityTaskScheduled,
						Attributes: &eventpb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &eventpb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDNotStarted,
							ActivityType: &commonpb.ActivityType{
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
						EventType: eventpb.EventType_TimerStarted,
						Attributes: &eventpb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &eventpb.TimerStartedEventAttributes{
							TimerId:                      timerUnfiredID1,
							StartToFireTimeoutSeconds:    4,
							DecisionTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   20,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_TimerStarted,
						Attributes: &eventpb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &eventpb.TimerStartedEventAttributes{
							TimerId:                      timerUnfiredID2,
							StartToFireTimeoutSeconds:    8,
							DecisionTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   21,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_ActivityTaskScheduled,
						Attributes: &eventpb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &eventpb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDCompleted2,
							ActivityType: &commonpb.ActivityType{
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
						EventType: eventpb.EventType_ActivityTaskScheduled,
						Attributes: &eventpb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &eventpb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDStartedNoRetry,
							ActivityType: &commonpb.ActivityType{
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
						EventType: eventpb.EventType_RequestCancelExternalWorkflowExecutionInitiated,
						Attributes: &eventpb.HistoryEvent_RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{RequestCancelExternalWorkflowExecutionInitiatedEventAttributes: &eventpb.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
							Namespace:                    "any-namespace",
							WorkflowExecution:            cancelWE,
							DecisionTaskCompletedEventId: 16,
							ChildWorkflowOnly:            true,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   24,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_ActivityTaskStarted,
						Attributes: &eventpb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &eventpb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 21,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   25,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_ActivityTaskStarted,
						Attributes: &eventpb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &eventpb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 17,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   26,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_ActivityTaskStarted,
						Attributes: &eventpb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &eventpb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 22,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   27,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_ActivityTaskCompleted,
						Attributes: &eventpb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &eventpb.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: 21,
							StartedEventId:   24,
						}},
					},
					{
						EventId:   28,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_DecisionTaskScheduled,
						Attributes: &eventpb.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &eventpb.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   29,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_DecisionTaskStarted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &eventpb.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 28,
						}},
					},
				},
			},
			/////////////// reset point/////////////
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   30,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_DecisionTaskCompleted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &eventpb.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 28,
							StartedEventId:   29,
						}},
					},
					{
						EventId:   31,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_TimerStarted,
						Attributes: &eventpb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &eventpb.TimerStartedEventAttributes{
							TimerId:                      timerAfterReset,
							StartToFireTimeoutSeconds:    4,
							DecisionTaskCompletedEventId: 30,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   32,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_ActivityTaskStarted,
						Attributes: &eventpb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &eventpb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 18,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   33,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_WorkflowExecutionSignaled,
						Attributes: &eventpb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &eventpb.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName1,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   34,
						Version:   common.EmptyVersion,
						EventType: eventpb.EventType_WorkflowExecutionSignaled,
						Attributes: &eventpb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &eventpb.WorkflowExecutionSignaledEventAttributes{
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
	namespace := "testNamespace"
	beforeResetVersion := int64(100)
	afterResetVersion := int64(101)
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(beforeResetVersion).Return("standby").AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(afterResetVersion).Return("active").AnyTimes()

	testNamespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Id: testNamespaceID},
		&persistenceblobs.NamespaceConfig{RetentionDays: 1},
		&persistenceblobs.NamespaceReplicationConfig{
			ActiveClusterName: "active",
			Clusters: []string{
				"active", "standby",
			},
		},
		afterResetVersion,
		cluster.GetTestClusterMetadata(true, true),
	)
	// override namespace cache
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()

	request := &historyservice.ResetWorkflowExecutionRequest{}
	namespaceID := testNamespaceID
	request.NamespaceId = namespaceID
	request.ResetRequest = &workflowservice.ResetWorkflowExecutionRequest{}

	wid := "wId"
	wfType := "wfType"
	taskListName := "taskList"
	forkRunID := uuid.New().String()
	currRunID := uuid.New().String()
	we := executionpb.WorkflowExecution{
		WorkflowId: wid,
		RunId:      forkRunID,
	}
	request.ResetRequest = &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:             namespace,
		WorkflowExecution:     &we,
		Reason:                "test reset",
		DecisionFinishEventId: 30,
		RequestId:             uuid.New().String(),
	}

	forkGwmsRequest := &persistence.GetWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		Execution: executionpb.WorkflowExecution{
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
		NamespaceID:        namespaceID,
		WorkflowID:         wid,
		WorkflowTypeName:   wfType,
		TaskList:           taskListName,
		RunID:              forkRunID,
		BranchToken:        forkBranchToken,
		NextEventID:        35,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
		State:              executiongenpb.WorkflowExecutionState_WorkflowExecutionState_Created,
	}

	forkRepState := &persistence.ReplicationState{
		CurrentVersion:      beforeResetVersion,
		StartVersion:        beforeResetVersion,
		LastWriteEventID:    common.EmptyEventID,
		LastWriteVersion:    common.EmptyVersion,
		LastReplicationInfo: map[string]*replicationgenpb.ReplicationInfo{},
	}
	forkGwmsResponse := &persistence.GetWorkflowExecutionResponse{State: &persistence.WorkflowMutableState{
		ExecutionInfo:    forkExeInfo,
		ExecutionStats:   &persistence.ExecutionStats{},
		ReplicationState: forkRepState,
	}}

	currGwmsRequest := &persistence.GetWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		Execution: executionpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      currRunID,
		},
	}
	currExeInfo := &persistence.WorkflowExecutionInfo{
		NamespaceID:        namespaceID,
		WorkflowID:         wid,
		WorkflowTypeName:   wfType,
		TaskList:           taskListName,
		RunID:              currRunID,
		NextEventID:        common.FirstEventID,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
		State:              executiongenpb.WorkflowExecutionState_WorkflowExecutionState_Created,
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

	taskList := &tasklistpb.TaskList{
		Name: taskListName,
	}
	readHistoryResp := &persistence.ReadHistoryBranchByBatchResponse{
		NextPageToken:    nil,
		Size:             1000,
		LastFirstEventID: int64(31),
		History: []*eventpb.History{
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   1,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_WorkflowExecutionStarted,
						Attributes: &eventpb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &eventpb.WorkflowExecutionStartedEventAttributes{
							WorkflowType: &commonpb.WorkflowType{
								Name: wfType,
							},
							TaskList:                        taskList,
							Input:                           payloads.EncodeString("testInput"),
							WorkflowExecutionTimeoutSeconds: 100,
							WorkflowTaskTimeoutSeconds:      200,
						}},
					},
					{
						EventId:   2,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskScheduled,
						Attributes: &eventpb.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &eventpb.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   3,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskStarted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &eventpb.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 2,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   4,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskCompleted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &eventpb.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 2,
							StartedEventId:   3,
						}},
					},
					{
						EventId:   5,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_MarkerRecorded,
						Attributes: &eventpb.HistoryEvent_MarkerRecordedEventAttributes{MarkerRecordedEventAttributes: &eventpb.MarkerRecordedEventAttributes{
							MarkerName:                   "Version",
							Details:                      payloads.EncodeString("details"),
							DecisionTaskCompletedEventId: 4,
						}},
					},
					{
						EventId:   6,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_ActivityTaskScheduled,
						Attributes: &eventpb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &eventpb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDCompleted1,
							ActivityType: &commonpb.ActivityType{
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
						EventType: eventpb.EventType_TimerStarted,
						Attributes: &eventpb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &eventpb.TimerStartedEventAttributes{
							TimerId:                      timerFiredID,
							StartToFireTimeoutSeconds:    2,
							DecisionTaskCompletedEventId: 4,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   8,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_ActivityTaskStarted,
						Attributes: &eventpb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &eventpb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 6,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   9,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_ActivityTaskCompleted,
						Attributes: &eventpb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &eventpb.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: 6,
							StartedEventId:   8,
						}},
					},
					{
						EventId:   10,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskScheduled,
						Attributes: &eventpb.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &eventpb.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   11,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskStarted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &eventpb.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 10,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   12,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskCompleted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &eventpb.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 10,
							StartedEventId:   11,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   13,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_TimerFired,
						Attributes: &eventpb.HistoryEvent_TimerFiredEventAttributes{TimerFiredEventAttributes: &eventpb.TimerFiredEventAttributes{
							TimerId: timerFiredID,
						}},
					},
					{
						EventId:   14,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskScheduled,
						Attributes: &eventpb.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &eventpb.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   15,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskStarted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &eventpb.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 14,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   16,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskCompleted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &eventpb.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 14,
							StartedEventId:   15,
						}},
					},
					{
						EventId:   17,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_ActivityTaskScheduled,
						Attributes: &eventpb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &eventpb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDRetry,
							ActivityType: &commonpb.ActivityType{
								Name: "actType1",
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: 1000,
							ScheduleToStartTimeoutSeconds: 2000,
							StartToCloseTimeoutSeconds:    3000,
							HeartbeatTimeoutSeconds:       4000,
							DecisionTaskCompletedEventId:  16,
							RetryPolicy: &commonpb.RetryPolicy{
								InitialIntervalInSeconds: 1,
								BackoffCoefficient:       0.2,
								MaximumAttempts:          10,
								MaximumIntervalInSeconds: 1000,
							},
						}},
					},
					{
						EventId:   18,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_ActivityTaskScheduled,
						Attributes: &eventpb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &eventpb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDNotStarted,
							ActivityType: &commonpb.ActivityType{
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
						EventType: eventpb.EventType_TimerStarted,
						Attributes: &eventpb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &eventpb.TimerStartedEventAttributes{
							TimerId:                      timerUnfiredID1,
							StartToFireTimeoutSeconds:    4,
							DecisionTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   20,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_TimerStarted,
						Attributes: &eventpb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &eventpb.TimerStartedEventAttributes{
							TimerId:                      timerUnfiredID2,
							StartToFireTimeoutSeconds:    8,
							DecisionTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   21,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_ActivityTaskScheduled,
						Attributes: &eventpb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &eventpb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDCompleted2,
							ActivityType: &commonpb.ActivityType{
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
						EventType: eventpb.EventType_ActivityTaskScheduled,
						Attributes: &eventpb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &eventpb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDStartedNoRetry,
							ActivityType: &commonpb.ActivityType{
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
						EventType: eventpb.EventType_WorkflowExecutionSignaled,
						Attributes: &eventpb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &eventpb.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName3,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   24,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_ActivityTaskStarted,
						Attributes: &eventpb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &eventpb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 21,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   25,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_WorkflowExecutionSignaled,
						Attributes: &eventpb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &eventpb.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName4,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   26,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_ActivityTaskStarted,
						Attributes: &eventpb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &eventpb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 22,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   27,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_ActivityTaskCompleted,
						Attributes: &eventpb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &eventpb.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: 21,
							StartedEventId:   24,
						}},
					},
					{
						EventId:   28,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskScheduled,
						Attributes: &eventpb.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &eventpb.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   29,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskStarted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &eventpb.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 28,
						}},
					},
				},
			},
			/////////////// reset point/////////////
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   30,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskCompleted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &eventpb.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 28,
							StartedEventId:   29,
						}},
					},
					{
						EventId:   31,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_TimerStarted,
						Attributes: &eventpb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &eventpb.TimerStartedEventAttributes{
							TimerId:                      timerAfterReset,
							StartToFireTimeoutSeconds:    4,
							DecisionTaskCompletedEventId: 30,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   32,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_ActivityTaskStarted,
						Attributes: &eventpb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &eventpb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 18,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   33,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_WorkflowExecutionSignaled,
						Attributes: &eventpb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &eventpb.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName1,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   34,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_WorkflowExecutionSignaled,
						Attributes: &eventpb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &eventpb.WorkflowExecutionSignaledEventAttributes{
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
	s.Equal(eventpb.EventType_DecisionTaskFailed, eventpb.EventType(appendReq.Events[0].GetEventType()))
	s.Equal(eventpb.EventType_ActivityTaskFailed, eventpb.EventType(appendReq.Events[1].GetEventType()))
	s.Equal(eventpb.EventType_WorkflowExecutionSignaled, eventpb.EventType(appendReq.Events[2].GetEventType()))
	s.Equal(eventpb.EventType_WorkflowExecutionSignaled, eventpb.EventType(appendReq.Events[3].GetEventType()))
	s.Equal(eventpb.EventType_DecisionTaskScheduled, eventpb.EventType(appendReq.Events[4].GetEventType()))

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
	compareCurrExeInfo.State = executiongenpb.WorkflowExecutionState_WorkflowExecutionState_Completed
	compareCurrExeInfo.Status = executionpb.WorkflowExecutionStatus_Terminated
	compareCurrExeInfo.NextEventID = 2
	compareCurrExeInfo.LastFirstEventID = 1
	compareCurrExeInfo.CompletionEventBatchID = 1
	s.Equal(compareCurrExeInfo, resetReq.CurrentWorkflowMutation.ExecutionInfo)
	s.Equal(1, len(resetReq.CurrentWorkflowMutation.TransferTasks))
	s.Equal(1, len(resetReq.CurrentWorkflowMutation.TimerTasks))
	s.Equal(commongenpb.TaskType_TransferCloseExecution, resetReq.CurrentWorkflowMutation.TransferTasks[0].GetType())
	s.Equal(commongenpb.TaskType_DeleteHistoryEvent, resetReq.CurrentWorkflowMutation.TimerTasks[0].GetType())
	s.Equal(int64(200), resetReq.CurrentWorkflowMutation.ExecutionStats.HistorySize)

	s.Equal("wfType", resetReq.NewWorkflowSnapshot.ExecutionInfo.WorkflowTypeName)
	s.True(len(resetReq.NewWorkflowSnapshot.ExecutionInfo.RunID) > 0)
	s.Equal([]byte(newBranchToken), resetReq.NewWorkflowSnapshot.ExecutionInfo.BranchToken)

	s.Equal(int64(34), resetReq.NewWorkflowSnapshot.ExecutionInfo.DecisionScheduleID)
	s.Equal(int64(35), resetReq.NewWorkflowSnapshot.ExecutionInfo.NextEventID)

	s.Equal(4, len(resetReq.NewWorkflowSnapshot.TransferTasks))
	s.Equal(commongenpb.TaskType_TransferActivityTask, resetReq.NewWorkflowSnapshot.TransferTasks[0].GetType())
	s.Equal(commongenpb.TaskType_TransferActivityTask, resetReq.NewWorkflowSnapshot.TransferTasks[1].GetType())
	s.Equal(commongenpb.TaskType_TransferDecisionTask, resetReq.NewWorkflowSnapshot.TransferTasks[2].GetType())
	s.Equal(commongenpb.TaskType_TransferRecordWorkflowStarted, resetReq.NewWorkflowSnapshot.TransferTasks[3].GetType())

	// WF timeout task, user timer, activity timeout timer, activity retry timer
	s.Equal(3, len(resetReq.NewWorkflowSnapshot.TimerTasks))
	s.Equal(commongenpb.TaskType_WorkflowRunTimeout, resetReq.NewWorkflowSnapshot.TimerTasks[0].GetType())
	s.Equal(commongenpb.TaskType_UserTimer, resetReq.NewWorkflowSnapshot.TimerTasks[1].GetType())
	s.Equal(commongenpb.TaskType_ActivityTimeout, resetReq.NewWorkflowSnapshot.TimerTasks[2].GetType())

	s.Equal(2, len(resetReq.NewWorkflowSnapshot.TimerInfos))
	s.assertTimerIDs([]string{timerUnfiredID1, timerUnfiredID2}, resetReq.NewWorkflowSnapshot.TimerInfos)

	s.Equal(2, len(resetReq.NewWorkflowSnapshot.ActivityInfos))
	s.assertActivityIDs([]string{actIDRetry, actIDNotStarted}, resetReq.NewWorkflowSnapshot.ActivityInfos)

	s.Equal(1, len(resetReq.NewWorkflowSnapshot.ReplicationTasks))
	s.Equal(commongenpb.TaskType_ReplicationHistory, resetReq.NewWorkflowSnapshot.ReplicationTasks[0].GetType())
	s.Equal(1, len(resetReq.CurrentWorkflowMutation.ReplicationTasks))
	s.Equal(commongenpb.TaskType_ReplicationHistory, resetReq.CurrentWorkflowMutation.ReplicationTasks[0].GetType())

	compareRepState := copyReplicationState(forkRepState)
	compareRepState.StartVersion = beforeResetVersion
	compareRepState.CurrentVersion = afterResetVersion
	compareRepState.LastWriteEventID = 34
	compareRepState.LastWriteVersion = afterResetVersion
	compareRepState.LastReplicationInfo = map[string]*replicationgenpb.ReplicationInfo{
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
	namespace := "testNamespace"
	beforeResetVersion := int64(100)
	afterResetVersion := int64(101)
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(beforeResetVersion).Return("active").AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(afterResetVersion).Return("standby").AnyTimes()

	testNamespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Id: testNamespaceID},
		&persistenceblobs.NamespaceConfig{RetentionDays: 1},
		&persistenceblobs.NamespaceReplicationConfig{
			ActiveClusterName: "active",
			Clusters: []string{
				"active", "standby",
			},
		},
		afterResetVersion,
		cluster.GetTestClusterMetadata(true, true),
	)
	// override namespace cache
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()

	request := &historyservice.ResetWorkflowExecutionRequest{}
	namespaceID := testNamespaceID
	request.NamespaceId = namespaceID
	request.ResetRequest = &workflowservice.ResetWorkflowExecutionRequest{}

	wid := "wId"
	wfType := "wfType"
	taskListName := "taskList"
	forkRunID := uuid.New().String()
	currRunID := uuid.New().String()
	we := executionpb.WorkflowExecution{
		WorkflowId: wid,
		RunId:      forkRunID,
	}
	request.ResetRequest = &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:             namespace,
		WorkflowExecution:     &we,
		Reason:                "test reset",
		DecisionFinishEventId: 30,
		RequestId:             uuid.New().String(),
	}

	forkGwmsRequest := &persistence.GetWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		Execution: executionpb.WorkflowExecution{
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
		NamespaceID:        namespaceID,
		WorkflowID:         wid,
		WorkflowTypeName:   wfType,
		TaskList:           taskListName,
		RunID:              forkRunID,
		BranchToken:        forkBranchToken,
		NextEventID:        35,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
		State:              executiongenpb.WorkflowExecutionState_WorkflowExecutionState_Created,
	}

	forkRepState := &persistence.ReplicationState{
		CurrentVersion:      beforeResetVersion,
		StartVersion:        beforeResetVersion,
		LastWriteEventID:    common.EmptyEventID,
		LastWriteVersion:    common.EmptyVersion,
		LastReplicationInfo: map[string]*replicationgenpb.ReplicationInfo{},
	}
	forkGwmsResponse := &persistence.GetWorkflowExecutionResponse{State: &persistence.WorkflowMutableState{
		ExecutionInfo:    forkExeInfo,
		ExecutionStats:   &persistence.ExecutionStats{},
		ReplicationState: forkRepState,
	}}

	currGwmsRequest := &persistence.GetWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		Execution: executionpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      currRunID,
		},
	}
	currExeInfo := &persistence.WorkflowExecutionInfo{
		NamespaceID:        namespaceID,
		WorkflowID:         wid,
		WorkflowTypeName:   wfType,
		TaskList:           taskListName,
		RunID:              currRunID,
		NextEventID:        common.FirstEventID,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
		State:              executiongenpb.WorkflowExecutionState_WorkflowExecutionState_Created,
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

	taskList := &tasklistpb.TaskList{
		Name: taskListName,
	}
	readHistoryResp := &persistence.ReadHistoryBranchByBatchResponse{
		NextPageToken:    nil,
		Size:             1000,
		LastFirstEventID: int64(31),
		History: []*eventpb.History{
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   1,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_WorkflowExecutionStarted,
						Attributes: &eventpb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &eventpb.WorkflowExecutionStartedEventAttributes{
							WorkflowType: &commonpb.WorkflowType{
								Name: wfType,
							},
							TaskList:                   taskList,
							Input:                      payloads.EncodeString("testInput"),
							WorkflowRunTimeoutSeconds:  100,
							WorkflowTaskTimeoutSeconds: 200,
						}},
					},
					{
						EventId:   2,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskScheduled,
						Attributes: &eventpb.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &eventpb.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   3,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskStarted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &eventpb.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 2,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   4,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskCompleted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &eventpb.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 2,
							StartedEventId:   3,
						}},
					},
					{
						EventId:   5,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_MarkerRecorded,
						Attributes: &eventpb.HistoryEvent_MarkerRecordedEventAttributes{MarkerRecordedEventAttributes: &eventpb.MarkerRecordedEventAttributes{
							MarkerName:                   "Version",
							Details:                      payloads.EncodeString("details"),
							DecisionTaskCompletedEventId: 4,
						}},
					},
					{
						EventId:   6,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_ActivityTaskScheduled,
						Attributes: &eventpb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &eventpb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDCompleted1,
							ActivityType: &commonpb.ActivityType{
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
						EventType: eventpb.EventType_TimerStarted,
						Attributes: &eventpb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &eventpb.TimerStartedEventAttributes{
							TimerId:                      timerFiredID,
							StartToFireTimeoutSeconds:    2,
							DecisionTaskCompletedEventId: 4,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   8,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_ActivityTaskStarted,
						Attributes: &eventpb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &eventpb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 6,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   9,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_ActivityTaskCompleted,
						Attributes: &eventpb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &eventpb.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: 6,
							StartedEventId:   8,
						}},
					},
					{
						EventId:   10,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskScheduled,
						Attributes: &eventpb.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &eventpb.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   11,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskStarted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &eventpb.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 10,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   12,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskCompleted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &eventpb.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 10,
							StartedEventId:   11,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   13,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_TimerFired,
						Attributes: &eventpb.HistoryEvent_TimerFiredEventAttributes{TimerFiredEventAttributes: &eventpb.TimerFiredEventAttributes{
							TimerId: timerFiredID,
						}},
					},
					{
						EventId:   14,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskScheduled,
						Attributes: &eventpb.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &eventpb.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   15,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskStarted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &eventpb.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 14,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   16,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskCompleted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &eventpb.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 14,
							StartedEventId:   15,
						}},
					},
					{
						EventId:   17,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_ActivityTaskScheduled,
						Attributes: &eventpb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &eventpb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDRetry,
							ActivityType: &commonpb.ActivityType{
								Name: "actType1",
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: 1000,
							ScheduleToStartTimeoutSeconds: 2000,
							StartToCloseTimeoutSeconds:    3000,
							HeartbeatTimeoutSeconds:       4000,
							DecisionTaskCompletedEventId:  16,
							RetryPolicy: &commonpb.RetryPolicy{
								InitialIntervalInSeconds: 1,
								BackoffCoefficient:       0.2,
								MaximumAttempts:          10,
								MaximumIntervalInSeconds: 1000,
							},
						}},
					},
					{
						EventId:   18,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_ActivityTaskScheduled,
						Attributes: &eventpb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &eventpb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDNotStarted,
							ActivityType: &commonpb.ActivityType{
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
						EventType: eventpb.EventType_TimerStarted,
						Attributes: &eventpb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &eventpb.TimerStartedEventAttributes{
							TimerId:                      timerUnfiredID1,
							StartToFireTimeoutSeconds:    4,
							DecisionTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   20,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_TimerStarted,
						Attributes: &eventpb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &eventpb.TimerStartedEventAttributes{
							TimerId:                      timerUnfiredID2,
							StartToFireTimeoutSeconds:    8,
							DecisionTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   21,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_ActivityTaskScheduled,
						Attributes: &eventpb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &eventpb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDCompleted2,
							ActivityType: &commonpb.ActivityType{
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
						EventType: eventpb.EventType_ActivityTaskScheduled,
						Attributes: &eventpb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &eventpb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDStartedNoRetry,
							ActivityType: &commonpb.ActivityType{
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
						EventType: eventpb.EventType_WorkflowExecutionSignaled,
						Attributes: &eventpb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &eventpb.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName3,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   24,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_ActivityTaskStarted,
						Attributes: &eventpb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &eventpb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 21,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   25,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_WorkflowExecutionSignaled,
						Attributes: &eventpb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &eventpb.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName4,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   26,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_ActivityTaskStarted,
						Attributes: &eventpb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &eventpb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 22,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   27,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_ActivityTaskCompleted,
						Attributes: &eventpb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &eventpb.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: 21,
							StartedEventId:   24,
						}},
					},
					{
						EventId:   28,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskScheduled,
						Attributes: &eventpb.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &eventpb.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   29,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskStarted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &eventpb.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 28,
						}},
					},
				},
			},
			/////////////// reset point/////////////
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   30,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskCompleted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &eventpb.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 28,
							StartedEventId:   29,
						}},
					},
					{
						EventId:   31,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_TimerStarted,
						Attributes: &eventpb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &eventpb.TimerStartedEventAttributes{
							TimerId:                      timerAfterReset,
							StartToFireTimeoutSeconds:    4,
							DecisionTaskCompletedEventId: 30,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   32,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_ActivityTaskStarted,
						Attributes: &eventpb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &eventpb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 18,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   33,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_WorkflowExecutionSignaled,
						Attributes: &eventpb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &eventpb.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName1,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   34,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_WorkflowExecutionSignaled,
						Attributes: &eventpb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &eventpb.WorkflowExecutionSignaledEventAttributes{
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
	s.IsType(&serviceerror.NamespaceNotActive{}, err)
}

func (s *resetorSuite) TestResetWorkflowExecution_Replication_NoTerminatingCurrent() {
	namespace := "testNamespace"
	beforeResetVersion := int64(100)
	afterResetVersion := int64(101)
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(beforeResetVersion).Return("standby").AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(afterResetVersion).Return("active").AnyTimes()

	testNamespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Id: testNamespaceID},
		&persistenceblobs.NamespaceConfig{RetentionDays: 1},
		&persistenceblobs.NamespaceReplicationConfig{
			ActiveClusterName: "active",
			Clusters: []string{
				"active", "standby",
			},
		},
		afterResetVersion,
		cluster.GetTestClusterMetadata(true, true),
	)
	// override namespace cache
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()

	request := &historyservice.ResetWorkflowExecutionRequest{}
	namespaceID := testNamespaceID
	request.NamespaceId = namespaceID
	request.ResetRequest = &workflowservice.ResetWorkflowExecutionRequest{}

	wid := "wId"
	wfType := "wfType"
	taskListName := "taskList"
	forkRunID := uuid.New().String()
	currRunID := uuid.New().String()
	we := executionpb.WorkflowExecution{
		WorkflowId: wid,
		RunId:      forkRunID,
	}
	request.ResetRequest = &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:             namespace,
		WorkflowExecution:     &we,
		Reason:                "test reset",
		DecisionFinishEventId: 30,
		RequestId:             uuid.New().String(),
	}

	forkGwmsRequest := &persistence.GetWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		Execution: executionpb.WorkflowExecution{
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
		NamespaceID:        namespaceID,
		WorkflowID:         wid,
		WorkflowTypeName:   wfType,
		TaskList:           taskListName,
		RunID:              forkRunID,
		BranchToken:        forkBranchToken,
		NextEventID:        35,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
		State:              executiongenpb.WorkflowExecutionState_WorkflowExecutionState_Created,
	}

	forkRepState := &persistence.ReplicationState{
		CurrentVersion:      beforeResetVersion,
		StartVersion:        beforeResetVersion,
		LastWriteEventID:    common.EmptyEventID,
		LastWriteVersion:    common.EmptyVersion,
		LastReplicationInfo: map[string]*replicationgenpb.ReplicationInfo{},
	}
	forkGwmsResponse := &persistence.GetWorkflowExecutionResponse{State: &persistence.WorkflowMutableState{
		ExecutionInfo:    forkExeInfo,
		ExecutionStats:   &persistence.ExecutionStats{},
		ReplicationState: forkRepState,
	}}

	currGwmsRequest := &persistence.GetWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		Execution: executionpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      currRunID,
		},
	}
	currExeInfo := &persistence.WorkflowExecutionInfo{
		NamespaceID:        namespaceID,
		WorkflowID:         wid,
		WorkflowTypeName:   wfType,
		TaskList:           taskListName,
		RunID:              currRunID,
		NextEventID:        common.FirstEventID,
		State:              executiongenpb.WorkflowExecutionState_WorkflowExecutionState_Completed,
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

	taskList := &tasklistpb.TaskList{
		Name: taskListName,
	}
	readHistoryResp := &persistence.ReadHistoryBranchByBatchResponse{
		NextPageToken:    nil,
		Size:             1000,
		LastFirstEventID: int64(31),
		History: []*eventpb.History{
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   1,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_WorkflowExecutionStarted,
						Attributes: &eventpb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &eventpb.WorkflowExecutionStartedEventAttributes{
							WorkflowType: &commonpb.WorkflowType{
								Name: wfType,
							},
							TaskList:                   taskList,
							Input:                      payloads.EncodeString("testInput"),
							WorkflowRunTimeoutSeconds:  100,
							WorkflowTaskTimeoutSeconds: 200,
						}},
					},
					{
						EventId:   2,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskScheduled,
						Attributes: &eventpb.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &eventpb.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   3,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskStarted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &eventpb.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 2,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   4,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskCompleted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &eventpb.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 2,
							StartedEventId:   3,
						}},
					},
					{
						EventId:   5,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_MarkerRecorded,
						Attributes: &eventpb.HistoryEvent_MarkerRecordedEventAttributes{MarkerRecordedEventAttributes: &eventpb.MarkerRecordedEventAttributes{
							MarkerName:                   "Version",
							Details:                      payloads.EncodeString("details"),
							DecisionTaskCompletedEventId: 4,
						}},
					},
					{
						EventId:   6,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_ActivityTaskScheduled,
						Attributes: &eventpb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &eventpb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDCompleted1,
							ActivityType: &commonpb.ActivityType{
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
						EventType: eventpb.EventType_TimerStarted,
						Attributes: &eventpb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &eventpb.TimerStartedEventAttributes{
							TimerId:                      timerFiredID,
							StartToFireTimeoutSeconds:    2,
							DecisionTaskCompletedEventId: 4,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   8,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_ActivityTaskStarted,
						Attributes: &eventpb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &eventpb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 6,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   9,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_ActivityTaskCompleted,
						Attributes: &eventpb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &eventpb.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: 6,
							StartedEventId:   8,
						}},
					},
					{
						EventId:   10,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskScheduled,
						Attributes: &eventpb.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &eventpb.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   11,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskStarted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &eventpb.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 10,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   12,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskCompleted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &eventpb.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 10,
							StartedEventId:   11,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   13,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_TimerFired,
						Attributes: &eventpb.HistoryEvent_TimerFiredEventAttributes{TimerFiredEventAttributes: &eventpb.TimerFiredEventAttributes{
							TimerId: timerFiredID,
						}},
					},
					{
						EventId:   14,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskScheduled,
						Attributes: &eventpb.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &eventpb.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   15,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskStarted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &eventpb.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 14,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   16,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskCompleted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &eventpb.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 14,
							StartedEventId:   15,
						}},
					},
					{
						EventId:   17,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_ActivityTaskScheduled,
						Attributes: &eventpb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &eventpb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDRetry,
							ActivityType: &commonpb.ActivityType{
								Name: "actType1",
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: 1000,
							ScheduleToStartTimeoutSeconds: 2000,
							StartToCloseTimeoutSeconds:    3000,
							HeartbeatTimeoutSeconds:       4000,
							DecisionTaskCompletedEventId:  16,
							RetryPolicy: &commonpb.RetryPolicy{
								InitialIntervalInSeconds: 1,
								BackoffCoefficient:       0.2,
								MaximumAttempts:          10,
								MaximumIntervalInSeconds: 1000,
							},
						}},
					},
					{
						EventId:   18,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_ActivityTaskScheduled,
						Attributes: &eventpb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &eventpb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDNotStarted,
							ActivityType: &commonpb.ActivityType{
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
						EventType: eventpb.EventType_TimerStarted,
						Attributes: &eventpb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &eventpb.TimerStartedEventAttributes{
							TimerId:                      timerUnfiredID1,
							StartToFireTimeoutSeconds:    4,
							DecisionTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   20,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_TimerStarted,
						Attributes: &eventpb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &eventpb.TimerStartedEventAttributes{
							TimerId:                      timerUnfiredID2,
							StartToFireTimeoutSeconds:    8,
							DecisionTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   21,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_ActivityTaskScheduled,
						Attributes: &eventpb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &eventpb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDCompleted2,
							ActivityType: &commonpb.ActivityType{
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
						EventType: eventpb.EventType_ActivityTaskScheduled,
						Attributes: &eventpb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &eventpb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDStartedNoRetry,
							ActivityType: &commonpb.ActivityType{
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
						EventType: eventpb.EventType_WorkflowExecutionSignaled,
						Attributes: &eventpb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &eventpb.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName3,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   24,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_ActivityTaskStarted,
						Attributes: &eventpb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &eventpb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 21,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   25,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_WorkflowExecutionSignaled,
						Attributes: &eventpb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &eventpb.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName4,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   26,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_ActivityTaskStarted,
						Attributes: &eventpb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &eventpb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 22,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   27,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_ActivityTaskCompleted,
						Attributes: &eventpb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &eventpb.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: 21,
							StartedEventId:   24,
						}},
					},
					{
						EventId:   28,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskScheduled,
						Attributes: &eventpb.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &eventpb.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   29,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskStarted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &eventpb.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 28,
						}},
					},
				},
			},
			/////////////// reset point/////////////
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   30,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskCompleted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &eventpb.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 28,
							StartedEventId:   29,
						}},
					},
					{
						EventId:   31,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_TimerStarted,
						Attributes: &eventpb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &eventpb.TimerStartedEventAttributes{
							TimerId:                      timerAfterReset,
							StartToFireTimeoutSeconds:    4,
							DecisionTaskCompletedEventId: 30,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   32,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_ActivityTaskStarted,
						Attributes: &eventpb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &eventpb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 18,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   33,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_WorkflowExecutionSignaled,
						Attributes: &eventpb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &eventpb.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName1,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   34,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_WorkflowExecutionSignaled,
						Attributes: &eventpb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &eventpb.WorkflowExecutionSignaledEventAttributes{
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
	s.Equal(eventpb.EventType_DecisionTaskFailed, eventpb.EventType(appendReq.Events[0].GetEventType()))
	s.Equal(eventpb.EventType_ActivityTaskFailed, eventpb.EventType(appendReq.Events[1].GetEventType()))
	s.Equal(eventpb.EventType_WorkflowExecutionSignaled, eventpb.EventType(appendReq.Events[2].GetEventType()))
	s.Equal(eventpb.EventType_WorkflowExecutionSignaled, eventpb.EventType(appendReq.Events[3].GetEventType()))
	s.Equal(eventpb.EventType_DecisionTaskScheduled, eventpb.EventType(appendReq.Events[4].GetEventType()))

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
	s.Equal(commongenpb.TaskType_TransferActivityTask, resetReq.NewWorkflowSnapshot.TransferTasks[0].GetType())
	s.Equal(commongenpb.TaskType_TransferActivityTask, resetReq.NewWorkflowSnapshot.TransferTasks[1].GetType())
	s.Equal(commongenpb.TaskType_TransferDecisionTask, resetReq.NewWorkflowSnapshot.TransferTasks[2].GetType())
	s.Equal(commongenpb.TaskType_TransferRecordWorkflowStarted, resetReq.NewWorkflowSnapshot.TransferTasks[3].GetType())

	// WF timeout task, user timer, activity timeout timer, activity retry timer
	s.Equal(3, len(resetReq.NewWorkflowSnapshot.TimerTasks))
	s.Equal(commongenpb.TaskType_WorkflowRunTimeout, resetReq.NewWorkflowSnapshot.TimerTasks[0].GetType())
	s.Equal(commongenpb.TaskType_UserTimer, resetReq.NewWorkflowSnapshot.TimerTasks[1].GetType())
	s.Equal(commongenpb.TaskType_ActivityTimeout, resetReq.NewWorkflowSnapshot.TimerTasks[2].GetType())

	s.Equal(2, len(resetReq.NewWorkflowSnapshot.TimerInfos))
	s.assertTimerIDs([]string{timerUnfiredID1, timerUnfiredID2}, resetReq.NewWorkflowSnapshot.TimerInfos)

	s.Equal(2, len(resetReq.NewWorkflowSnapshot.ActivityInfos))
	s.assertActivityIDs([]string{actIDRetry, actIDNotStarted}, resetReq.NewWorkflowSnapshot.ActivityInfos)

	s.Equal(1, len(resetReq.NewWorkflowSnapshot.ReplicationTasks))
	s.Equal(commongenpb.TaskType_ReplicationHistory, resetReq.NewWorkflowSnapshot.ReplicationTasks[0].GetType())

	compareRepState := copyReplicationState(forkRepState)
	compareRepState.StartVersion = beforeResetVersion
	compareRepState.CurrentVersion = afterResetVersion
	compareRepState.LastWriteEventID = 34
	compareRepState.LastWriteVersion = afterResetVersion
	compareRepState.LastReplicationInfo = map[string]*replicationgenpb.ReplicationInfo{
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

func (s *resetorSuite) TestApplyReset() {
	namespaceID := testNamespaceID
	beforeResetVersion := int64(100)
	afterResetVersion := int64(101)
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(beforeResetVersion).Return(cluster.TestAlternativeClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(afterResetVersion).Return(cluster.TestCurrentClusterName).AnyTimes()

	testNamespaceEntry := cache.NewGlobalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Id: testNamespaceID},
		&persistenceblobs.NamespaceConfig{RetentionDays: 1},
		&persistenceblobs.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		afterResetVersion,
		cluster.GetTestClusterMetadata(true, true),
	)
	// override namespace cache
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()

	wid := "wId"
	wfType := "wfType"
	taskListName := "taskList"
	forkRunID := uuid.New().String()
	currRunID := uuid.New().String()
	newRunID := uuid.New().String()
	we := executionpb.WorkflowExecution{
		WorkflowId: wid,
		RunId:      newRunID,
	}

	forkGwmsRequest := &persistence.GetWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		Execution: executionpb.WorkflowExecution{
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
		NamespaceID:        namespaceID,
		WorkflowID:         wid,
		WorkflowTypeName:   wfType,
		TaskList:           taskListName,
		RunID:              forkRunID,
		BranchToken:        forkBranchToken,
		NextEventID:        35,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
		State:              executiongenpb.WorkflowExecutionState_WorkflowExecutionState_Created,
	}

	forkRepState := &persistence.ReplicationState{
		CurrentVersion:      beforeResetVersion,
		StartVersion:        beforeResetVersion,
		LastWriteEventID:    common.EmptyEventID,
		LastWriteVersion:    common.EmptyVersion,
		LastReplicationInfo: map[string]*replicationgenpb.ReplicationInfo{},
	}
	forkGwmsResponse := &persistence.GetWorkflowExecutionResponse{State: &persistence.WorkflowMutableState{
		ExecutionInfo:    forkExeInfo,
		ExecutionStats:   &persistence.ExecutionStats{},
		ReplicationState: forkRepState,
	}}

	currGwmsRequest := &persistence.GetWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		Execution: executionpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      currRunID,
		},
	}
	currExeInfo := &persistence.WorkflowExecutionInfo{
		NamespaceID:        namespaceID,
		WorkflowID:         wid,
		WorkflowTypeName:   wfType,
		TaskList:           taskListName,
		RunID:              currRunID,
		NextEventID:        common.FirstEventID,
		State:              executiongenpb.WorkflowExecutionState_WorkflowExecutionState_Completed,
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

	taskList := &tasklistpb.TaskList{
		Name: taskListName,
	}

	readHistoryResp := &persistence.ReadHistoryBranchByBatchResponse{
		NextPageToken:    nil,
		Size:             1000,
		LastFirstEventID: int64(31),
		History: []*eventpb.History{
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   1,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_WorkflowExecutionStarted,
						Attributes: &eventpb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &eventpb.WorkflowExecutionStartedEventAttributes{
							WorkflowType: &commonpb.WorkflowType{
								Name: wfType,
							},
							TaskList:                   taskList,
							Input:                      payloads.EncodeString("testInput"),
							WorkflowRunTimeoutSeconds:  100,
							WorkflowTaskTimeoutSeconds: 200,
						}},
					},
					{
						EventId:   2,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskScheduled,
						Attributes: &eventpb.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &eventpb.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   3,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskStarted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &eventpb.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 2,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   4,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskCompleted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &eventpb.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 2,
							StartedEventId:   3,
						}},
					},
					{
						EventId:   5,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_MarkerRecorded,
						Attributes: &eventpb.HistoryEvent_MarkerRecordedEventAttributes{MarkerRecordedEventAttributes: &eventpb.MarkerRecordedEventAttributes{
							MarkerName:                   "Version",
							Details:                      payloads.EncodeString("details"),
							DecisionTaskCompletedEventId: 4,
						}},
					},
					{
						EventId:   6,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_ActivityTaskScheduled,
						Attributes: &eventpb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &eventpb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDCompleted1,
							ActivityType: &commonpb.ActivityType{
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
						EventType: eventpb.EventType_TimerStarted,
						Attributes: &eventpb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &eventpb.TimerStartedEventAttributes{
							TimerId:                      timerFiredID,
							StartToFireTimeoutSeconds:    2,
							DecisionTaskCompletedEventId: 4,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   8,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_ActivityTaskStarted,
						Attributes: &eventpb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &eventpb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 6,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   9,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_ActivityTaskCompleted,
						Attributes: &eventpb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &eventpb.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: 6,
							StartedEventId:   8,
						}},
					},
					{
						EventId:   10,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskScheduled,
						Attributes: &eventpb.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &eventpb.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   11,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskStarted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &eventpb.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 10,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   12,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskCompleted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &eventpb.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 10,
							StartedEventId:   11,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   13,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_TimerFired,
						Attributes: &eventpb.HistoryEvent_TimerFiredEventAttributes{TimerFiredEventAttributes: &eventpb.TimerFiredEventAttributes{
							TimerId: timerFiredID,
						}},
					},
					{
						EventId:   14,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskScheduled,
						Attributes: &eventpb.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &eventpb.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   15,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskStarted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &eventpb.DecisionTaskStartedEventAttributes{
							ScheduledEventId: 14,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   16,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskCompleted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &eventpb.DecisionTaskCompletedEventAttributes{
							ScheduledEventId: 14,
							StartedEventId:   15,
						}},
					},
					{
						EventId:   17,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_ActivityTaskScheduled,
						Attributes: &eventpb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &eventpb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDRetry,
							ActivityType: &commonpb.ActivityType{
								Name: "actType1",
							},
							TaskList:                      taskList,
							ScheduleToCloseTimeoutSeconds: 1000,
							ScheduleToStartTimeoutSeconds: 2000,
							StartToCloseTimeoutSeconds:    3000,
							HeartbeatTimeoutSeconds:       4000,
							DecisionTaskCompletedEventId:  16,
							RetryPolicy: &commonpb.RetryPolicy{
								InitialIntervalInSeconds: 1,
								BackoffCoefficient:       0.2,
								MaximumAttempts:          10,
								MaximumIntervalInSeconds: 1000,
							},
						}},
					},
					{
						EventId:   18,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_ActivityTaskScheduled,
						Attributes: &eventpb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &eventpb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDNotStarted,
							ActivityType: &commonpb.ActivityType{
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
						EventType: eventpb.EventType_TimerStarted,
						Attributes: &eventpb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &eventpb.TimerStartedEventAttributes{
							TimerId:                      timerUnfiredID1,
							StartToFireTimeoutSeconds:    4,
							DecisionTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   20,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_TimerStarted,
						Attributes: &eventpb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &eventpb.TimerStartedEventAttributes{
							TimerId:                      timerUnfiredID2,
							StartToFireTimeoutSeconds:    8,
							DecisionTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   21,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_ActivityTaskScheduled,
						Attributes: &eventpb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &eventpb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDCompleted2,
							ActivityType: &commonpb.ActivityType{
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
						EventType: eventpb.EventType_ActivityTaskScheduled,
						Attributes: &eventpb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &eventpb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDStartedNoRetry,
							ActivityType: &commonpb.ActivityType{
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
						EventType: eventpb.EventType_WorkflowExecutionSignaled,
						Attributes: &eventpb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &eventpb.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName3,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   24,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_ActivityTaskStarted,
						Attributes: &eventpb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &eventpb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 21,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   25,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_WorkflowExecutionSignaled,
						Attributes: &eventpb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &eventpb.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName4,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   26,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_ActivityTaskStarted,
						Attributes: &eventpb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &eventpb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 22,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   27,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_ActivityTaskCompleted,
						Attributes: &eventpb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &eventpb.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: 21,
							StartedEventId:   24,
						}},
					},
					{
						EventId:   28,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskScheduled,
						Attributes: &eventpb.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &eventpb.DecisionTaskScheduledEventAttributes{
							TaskList:                   taskList,
							StartToCloseTimeoutSeconds: 100,
						}},
					},
				},
			},
			{
				Events: []*eventpb.HistoryEvent{
					{
						EventId:   29,
						Version:   beforeResetVersion,
						EventType: eventpb.EventType_DecisionTaskStarted,
						Attributes: &eventpb.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &eventpb.DecisionTaskStartedEventAttributes{
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
		Info:            persistence.BuildHistoryGarbageCleanupInfo(namespaceID, wid, newRunID),
		ShardID:         &s.shardID,
	}
	forkResp := &persistence.ForkHistoryBranchResponse{
		NewBranchToken: newBranchToken,
	}

	historyAfterReset := &eventpb.History{
		Events: []*eventpb.HistoryEvent{
			{
				EventId:   30,
				Version:   afterResetVersion,
				EventType: eventpb.EventType_DecisionTaskFailed,
				Attributes: &eventpb.HistoryEvent_DecisionTaskFailedEventAttributes{DecisionTaskFailedEventAttributes: &eventpb.DecisionTaskFailedEventAttributes{
					ScheduledEventId: int64(28),
					StartedEventId:   int64(29),
					Cause:            eventpb.DecisionTaskFailedCause_ResetWorkflow,
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
				EventType: eventpb.EventType_ActivityTaskFailed,
				Attributes: &eventpb.HistoryEvent_ActivityTaskFailedEventAttributes{ActivityTaskFailedEventAttributes: &eventpb.ActivityTaskFailedEventAttributes{
					Reason:           "resetWF",
					ScheduledEventId: 22,
					StartedEventId:   26,
					Identity:         identityHistoryService,
				}},
			},
			{
				EventId:   32,
				Version:   afterResetVersion,
				EventType: eventpb.EventType_WorkflowExecutionSignaled,
				Attributes: &eventpb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &eventpb.WorkflowExecutionSignaledEventAttributes{
					SignalName: signalName1,
				}},
			},
			{
				EventId:   33,
				Version:   afterResetVersion,
				EventType: eventpb.EventType_WorkflowExecutionSignaled,
				Attributes: &eventpb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &eventpb.WorkflowExecutionSignaledEventAttributes{
					SignalName: signalName2,
				}},
			},
			{
				EventId:   34,
				Version:   afterResetVersion,
				EventType: eventpb.EventType_DecisionTaskScheduled,
				Attributes: &eventpb.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &eventpb.DecisionTaskScheduledEventAttributes{
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
		NamespaceId:       namespaceID,
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
	err := s.resetor.ApplyResetEvent(context.Background(), request, namespaceID, wid, currRunID)
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
	s.Equal(eventpb.EventType_DecisionTaskFailed, eventpb.EventType(appendReq.Events[0].GetEventType()))
	s.Equal(eventpb.EventType_ActivityTaskFailed, eventpb.EventType(appendReq.Events[1].GetEventType()))
	s.Equal(eventpb.EventType_WorkflowExecutionSignaled, eventpb.EventType(appendReq.Events[2].GetEventType()))
	s.Equal(eventpb.EventType_WorkflowExecutionSignaled, eventpb.EventType(appendReq.Events[3].GetEventType()))
	s.Equal(eventpb.EventType_DecisionTaskScheduled, eventpb.EventType(appendReq.Events[4].GetEventType()))

	s.Equal(int64(30), appendReq.Events[0].GetEventId())
	s.Equal(int64(31), appendReq.Events[1].GetEventId())
	s.Equal(int64(32), appendReq.Events[2].GetEventId())
	s.Equal(int64(33), appendReq.Events[3].GetEventId())
	s.Equal(int64(34), appendReq.Events[4].GetEventId())

	s.Equal(common.EncodingType(s.config.EventEncodingType(namespaceID)), appendReq.Encoding)

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
	s.Equal(commongenpb.TaskType_TransferActivityTask, resetReq.NewWorkflowSnapshot.TransferTasks[0].GetType())
	s.Equal(commongenpb.TaskType_TransferActivityTask, resetReq.NewWorkflowSnapshot.TransferTasks[1].GetType())
	s.Equal(commongenpb.TaskType_TransferDecisionTask, resetReq.NewWorkflowSnapshot.TransferTasks[2].GetType())

	// WF timeout task, user timer, activity timeout timer, activity retry timer
	s.Equal(3, len(resetReq.NewWorkflowSnapshot.TimerTasks))
	s.Equal(commongenpb.TaskType_WorkflowRunTimeout, resetReq.NewWorkflowSnapshot.TimerTasks[0].GetType())
	s.Equal(commongenpb.TaskType_UserTimer, resetReq.NewWorkflowSnapshot.TimerTasks[1].GetType())
	s.Equal(commongenpb.TaskType_ActivityTimeout, resetReq.NewWorkflowSnapshot.TimerTasks[2].GetType())

	s.Equal(2, len(resetReq.NewWorkflowSnapshot.TimerInfos))
	s.assertTimerIDs([]string{timerUnfiredID1, timerUnfiredID2}, resetReq.NewWorkflowSnapshot.TimerInfos)

	s.Equal(2, len(resetReq.NewWorkflowSnapshot.ActivityInfos))
	s.assertActivityIDs([]string{actIDRetry, actIDNotStarted}, resetReq.NewWorkflowSnapshot.ActivityInfos)

	compareRepState := copyReplicationState(forkRepState)
	compareRepState.StartVersion = beforeResetVersion
	compareRepState.CurrentVersion = afterResetVersion
	compareRepState.LastWriteEventID = 34
	compareRepState.LastWriteVersion = afterResetVersion
	compareRepState.LastReplicationInfo = map[string]*replicationgenpb.ReplicationInfo{
		"standby": {
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
	_, pt = FindAutoResetPoint(timeSource, &namespacepb.BadBinaries{}, &executionpb.ResetPoints{})
	assert.Nil(t, pt)

	pt0 := &executionpb.ResetPointInfo{
		BinaryChecksum: "abc",
		Resettable:     true,
	}
	pt1 := &executionpb.ResetPointInfo{
		BinaryChecksum: "def",
		Resettable:     true,
	}
	pt3 := &executionpb.ResetPointInfo{
		BinaryChecksum: "ghi",
		Resettable:     false,
	}

	expiredNowNano := time.Now().UnixNano() - int64(time.Hour)
	notExpiredNowNano := time.Now().UnixNano() + int64(time.Hour)
	pt4 := &executionpb.ResetPointInfo{
		BinaryChecksum:   "expired",
		Resettable:       true,
		ExpiringTimeNano: expiredNowNano,
	}

	pt5 := &executionpb.ResetPointInfo{
		BinaryChecksum:   "notExpired",
		Resettable:       true,
		ExpiringTimeNano: notExpiredNowNano,
	}

	// case 3: two intersection
	_, pt = FindAutoResetPoint(timeSource, &namespacepb.BadBinaries{
		Binaries: map[string]*namespacepb.BadBinaryInfo{
			"abc": {},
			"def": {},
		},
	}, &executionpb.ResetPoints{
		Points: []*executionpb.ResetPointInfo{
			pt0, pt1, pt3,
		},
	})
	assert.Equal(t, pt.String(), pt0.String())

	// case 4: one intersection
	_, pt = FindAutoResetPoint(timeSource, &namespacepb.BadBinaries{
		Binaries: map[string]*namespacepb.BadBinaryInfo{
			"none":    {},
			"def":     {},
			"expired": {},
		},
	}, &executionpb.ResetPoints{
		Points: []*executionpb.ResetPointInfo{
			pt4, pt0, pt1, pt3,
		},
	})
	assert.Equal(t, pt.String(), pt1.String())

	// case 4: no intersection
	_, pt = FindAutoResetPoint(timeSource, &namespacepb.BadBinaries{
		Binaries: map[string]*namespacepb.BadBinaryInfo{
			"none1": {},
			"none2": {},
		},
	}, &executionpb.ResetPoints{
		Points: []*executionpb.ResetPointInfo{
			pt0, pt1, pt3,
		},
	})
	assert.Nil(t, pt)

	// case 5: not resettable
	_, pt = FindAutoResetPoint(timeSource, &namespacepb.BadBinaries{
		Binaries: map[string]*namespacepb.BadBinaryInfo{
			"none1": {},
			"ghi":   {},
		},
	}, &executionpb.ResetPoints{
		Points: []*executionpb.ResetPointInfo{
			pt0, pt1, pt3,
		},
	})
	assert.Nil(t, pt)

	// case 6: one intersection of expired
	_, pt = FindAutoResetPoint(timeSource, &namespacepb.BadBinaries{
		Binaries: map[string]*namespacepb.BadBinaryInfo{
			"none":    {},
			"expired": {},
		},
	}, &executionpb.ResetPoints{
		Points: []*executionpb.ResetPointInfo{
			pt0, pt1, pt3, pt4, pt5,
		},
	})
	assert.Nil(t, pt)

	// case 7: one intersection of not expired
	_, pt = FindAutoResetPoint(timeSource, &namespacepb.BadBinaries{
		Binaries: map[string]*namespacepb.BadBinaryInfo{
			"none":       {},
			"notExpired": {},
		},
	}, &executionpb.ResetPoints{
		Points: []*executionpb.ResetPointInfo{
			pt0, pt1, pt3, pt4, pt5,
		},
	})
	assert.Equal(t, pt.String(), pt5.String())
}
