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
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/persistenceblobs/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/mocks"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
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
		historyEventNotifier: newHistoryEventNotifier(clock.NewRealTimeSource(), s.mockShard.GetMetricsClient(), func(string, string) int { return shardID }),
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
		&persistenceblobs.NamespaceInfo{Id: testNamespaceID}, &persistenceblobs.NamespaceConfig{Retention: timestamp.DurationFromDays(1)}, "", nil,
	)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()

	request := &historyservice.ResetWorkflowExecutionRequest{}
	namespaceID := testNamespaceID
	request.NamespaceId = namespaceID
	request.ResetRequest = &workflowservice.ResetWorkflowExecutionRequest{}

	wid := "wId"
	wfType := "wfType"
	taskQueueName := "taskQueue"
	forkRunID := uuid.New().String()
	currRunID := uuid.New().String()
	we := commonpb.WorkflowExecution{
		WorkflowId: wid,
		RunId:      forkRunID,
	}
	request.ResetRequest = &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 "testNamespace",
		WorkflowExecution:         &we,
		Reason:                    "test reset",
		WorkflowTaskFinishEventId: 29,
		RequestId:                 uuid.New().String(),
	}

	forkGwmsRequest := &persistence.GetWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		Execution: commonpb.WorkflowExecution{
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
		NamespaceID:            namespaceID,
		WorkflowID:             wid,
		WorkflowTypeName:       wfType,
		TaskQueue:              taskQueueName,
		RunID:                  forkRunID,
		BranchToken:            forkBranchToken,
		NextEventID:            34,
		WorkflowTaskVersion:    common.EmptyVersion,
		WorkflowTaskScheduleID: common.EmptyEventID,
		WorkflowTaskStartedID:  common.EmptyEventID,
		State:                  enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
	}
	forkGwmsResponse := &persistence.GetWorkflowExecutionResponse{State: &persistence.WorkflowMutableState{
		ExecutionInfo:  forkExeInfo,
		ExecutionStats: &persistenceblobs.ExecutionStats{},
	}}

	currGwmsRequest := &persistence.GetWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		Execution: commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      currRunID,
		},
	}
	currExeInfo := &persistence.WorkflowExecutionInfo{
		NamespaceID:            namespaceID,
		WorkflowID:             wid,
		WorkflowTypeName:       wfType,
		TaskQueue:              taskQueueName,
		RunID:                  currRunID,
		NextEventID:            common.FirstEventID,
		WorkflowTaskVersion:    common.EmptyVersion,
		WorkflowTaskScheduleID: common.EmptyEventID,
		WorkflowTaskStartedID:  common.EmptyEventID,
		State:                  enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
	}
	compareCurrExeInfo := copyWorkflowExecutionInfo(currExeInfo)
	currGwmsResponse := &persistence.GetWorkflowExecutionResponse{State: &persistence.WorkflowMutableState{
		ExecutionInfo:  currExeInfo,
		ExecutionStats: &persistenceblobs.ExecutionStats{},
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

	taskQueue := &taskqueuepb.TaskQueue{
		Name: taskQueueName,
	}
	readHistoryResp := &persistence.ReadHistoryBranchByBatchResponse{
		NextPageToken:    nil,
		Size:             1000,
		LastFirstEventID: int64(31),
		History: []*historypb.History{
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   1,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
						Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
							WorkflowType: &commonpb.WorkflowType{
								Name: wfType,
							},
							TaskQueue:                taskQueue,
							Input:                    payloads.EncodeString("testInput"),
							WorkflowExecutionTimeout: timestamp.DurationPtr(100 * time.Second),
							WorkflowRunTimeout:       timestamp.DurationPtr(50 * time.Second),
							WorkflowTaskTimeout:      timestamp.DurationPtr(200 * time.Second),
						}},
					},
					{
						EventId:   2,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
							TaskQueue:           taskQueue,
							StartToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   3,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
							ScheduledEventId: 2,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   4,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
							ScheduledEventId: 2,
							StartedEventId:   3,
						}},
					},
					{
						EventId:   5,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_MARKER_RECORDED,
						Attributes: &historypb.HistoryEvent_MarkerRecordedEventAttributes{MarkerRecordedEventAttributes: &historypb.MarkerRecordedEventAttributes{
							MarkerName: "Version",
							Details: map[string]*commonpb.Payloads{
								"change-id": payloads.EncodeString("32283"),
								"version":   payloads.EncodeInt(22),
							},
							WorkflowTaskCompletedEventId: 4,
						}},
					},
					{
						EventId:   6,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDCompleted1,
							ActivityType: &commonpb.ActivityType{
								Name: "actType0",
							},
							TaskQueue:                    taskQueue,
							ScheduleToCloseTimeout:       timestamp.DurationPtr(1000 * time.Second),
							ScheduleToStartTimeout:       timestamp.DurationPtr(2000 * time.Second),
							StartToCloseTimeout:          timestamp.DurationPtr(3000 * time.Second),
							HeartbeatTimeout:             timestamp.DurationPtr(4000 * time.Second),
							WorkflowTaskCompletedEventId: 4,
						}},
					},
					{
						EventId:   7,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_TIMER_STARTED,
						Attributes: &historypb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
							TimerId:                      timerFiredID,
							StartToFireTimeout:           timestamp.DurationPtr(2 * time.Second),
							WorkflowTaskCompletedEventId: 4,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   8,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 6,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   9,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
						Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: 6,
							StartedEventId:   8,
						}},
					},
					{
						EventId:   10,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
							TaskQueue:           taskQueue,
							StartToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   11,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
							ScheduledEventId: 10,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   12,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
							ScheduledEventId: 10,
							StartedEventId:   11,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   13,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_TIMER_FIRED,
						Attributes: &historypb.HistoryEvent_TimerFiredEventAttributes{TimerFiredEventAttributes: &historypb.TimerFiredEventAttributes{
							TimerId: timerFiredID,
						}},
					},
					{
						EventId:   14,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
							TaskQueue:           taskQueue,
							StartToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   15,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
							ScheduledEventId: 14,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   16,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
							ScheduledEventId: 14,
							StartedEventId:   15,
						}},
					},
					{
						EventId:   17,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDStarted1,
							ActivityType: &commonpb.ActivityType{
								Name: "actType1",
							},
							TaskQueue:                    taskQueue,
							ScheduleToCloseTimeout:       timestamp.DurationPtr(1000 * time.Second),
							ScheduleToStartTimeout:       timestamp.DurationPtr(2000 * time.Second),
							StartToCloseTimeout:          timestamp.DurationPtr(3000 * time.Second),
							HeartbeatTimeout:             timestamp.DurationPtr(4000 * time.Second),
							WorkflowTaskCompletedEventId: 16,
							RetryPolicy: &commonpb.RetryPolicy{
								InitialInterval:    timestamp.DurationPtr(1 * time.Second),
								BackoffCoefficient: 0.2,
								MaximumAttempts:    10,
								MaximumInterval:    timestamp.DurationPtr(1000 * time.Second),
							},
						}},
					},
					{
						EventId:   18,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDNotStarted,
							ActivityType: &commonpb.ActivityType{
								Name: "actType2",
							},
							TaskQueue:                    taskQueue,
							ScheduleToCloseTimeout:       timestamp.DurationPtr(1000 * time.Second),
							ScheduleToStartTimeout:       timestamp.DurationPtr(2000 * time.Second),
							StartToCloseTimeout:          timestamp.DurationPtr(3000 * time.Second),
							HeartbeatTimeout:             timestamp.DurationPtr(4000 * time.Second),
							WorkflowTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   19,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_TIMER_STARTED,
						Attributes: &historypb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
							TimerId:                      timerUnfiredID1,
							StartToFireTimeout:           timestamp.DurationPtr(4 * time.Second),
							WorkflowTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   20,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_TIMER_STARTED,
						Attributes: &historypb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
							TimerId:                      timerUnfiredID2,
							StartToFireTimeout:           timestamp.DurationPtr(8 * time.Second),
							WorkflowTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   21,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDCompleted2,
							ActivityType: &commonpb.ActivityType{
								Name: "actType2",
							},
							TaskQueue:                    taskQueue,
							ScheduleToCloseTimeout:       timestamp.DurationPtr(1000 * time.Second),
							ScheduleToStartTimeout:       timestamp.DurationPtr(2000 * time.Second),
							StartToCloseTimeout:          timestamp.DurationPtr(3000 * time.Second),
							HeartbeatTimeout:             timestamp.DurationPtr(4000 * time.Second),
							WorkflowTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   22,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDStarted2,
							ActivityType: &commonpb.ActivityType{
								Name: "actType2",
							},
							TaskQueue:                    taskQueue,
							ScheduleToCloseTimeout:       timestamp.DurationPtr(1000 * time.Second),
							ScheduleToStartTimeout:       timestamp.DurationPtr(2000 * time.Second),
							StartToCloseTimeout:          timestamp.DurationPtr(3000 * time.Second),
							HeartbeatTimeout:             timestamp.DurationPtr(4000 * time.Second),
							WorkflowTaskCompletedEventId: 16,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   23,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 21,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   24,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 17,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   25,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 22,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   26,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
						Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: 21,
							StartedEventId:   23,
						}},
					},
					{
						EventId:   27,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
							TaskQueue:           taskQueue,
							StartToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   28,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
							ScheduledEventId: 27,
						}},
					},
				},
			},
			// ///////////// reset point/////////////
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   29,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
							ScheduledEventId: 27,
							StartedEventId:   28,
						}},
					},
					{
						EventId:   30,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_TIMER_STARTED,
						Attributes: &historypb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
							TimerId:                      timerAfterReset,
							StartToFireTimeout:           timestamp.DurationPtr(4 * time.Second),
							WorkflowTaskCompletedEventId: 29,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   31,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 18,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   32,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
						Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName1,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   33,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
						Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName2,
						}},
					},
				},
			},
		},
	}

	eid := int64(0)
	eventTime := time.Unix(0, 1000).UTC()
	for _, be := range readHistoryResp.History {
		for _, e := range be.Events {
			eid++
			if e.GetEventId() != eid {
				s.Fail(fmt.Sprintf("inconintous eventID: %v, %v", eid, e.GetEventId()))
			}
			e.EventTime = &eventTime
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
	// 1. workflowTaskFailed :29
	// 2. activityTaskFailed :30
	// 3. signal 1 :31
	// 4. signal 2 :32
	// 5. workflowTaskScheduled :33
	calls := s.mockHistoryV2Mgr.Calls
	s.Equal(4, len(calls))
	appendCall := calls[3]
	s.Equal("AppendHistoryNodes", appendCall.Method)
	appendReq, ok := appendCall.Arguments[0].(*persistence.AppendHistoryNodesRequest)
	s.Equal(true, ok)
	s.Equal(newBranchToken, appendReq.BranchToken)
	s.Equal(false, appendReq.IsNewBranch)
	s.Equal(6, len(appendReq.Events))
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED, enumspb.EventType(appendReq.Events[0].GetEventType()))
	s.Equal(enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED, enumspb.EventType(appendReq.Events[1].GetEventType()))
	s.Equal(enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED, enumspb.EventType(appendReq.Events[2].GetEventType()))
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, enumspb.EventType(appendReq.Events[3].GetEventType()))
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, enumspb.EventType(appendReq.Events[4].GetEventType()))
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, enumspb.EventType(appendReq.Events[5].GetEventType()))

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
	compareCurrExeInfo.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	compareCurrExeInfo.Status = enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED
	compareCurrExeInfo.NextEventID = 2
	compareCurrExeInfo.CompletionEventBatchID = 1
	s.Equal(compareCurrExeInfo, resetReq.CurrentWorkflowMutation.ExecutionInfo)
	s.Equal(1, len(resetReq.CurrentWorkflowMutation.TransferTasks))
	s.Equal(1, len(resetReq.CurrentWorkflowMutation.TimerTasks))
	s.Equal(enumsspb.TASK_TYPE_TRANSFER_CLOSE_EXECUTION, resetReq.CurrentWorkflowMutation.TransferTasks[0].GetType())
	s.Equal(enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT, resetReq.CurrentWorkflowMutation.TimerTasks[0].GetType())
	s.Equal(int64(200), resetReq.CurrentWorkflowMutation.ExecutionStats.HistorySize)

	s.Equal("wfType", resetReq.NewWorkflowSnapshot.ExecutionInfo.WorkflowTypeName)
	s.True(len(resetReq.NewWorkflowSnapshot.ExecutionInfo.RunID) > 0)
	s.Equal([]byte(newBranchToken), resetReq.NewWorkflowSnapshot.ExecutionInfo.BranchToken)
	// 35 = resetEventID(29) + 6 in a batch
	s.Equal(int64(34), resetReq.NewWorkflowSnapshot.ExecutionInfo.WorkflowTaskScheduleID)
	s.Equal(int64(35), resetReq.NewWorkflowSnapshot.ExecutionInfo.NextEventID)

	// one activity task, one workflow task and one record workflow started task
	s.Equal(3, len(resetReq.NewWorkflowSnapshot.TransferTasks))
	s.Equal(enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK, resetReq.NewWorkflowSnapshot.TransferTasks[0].GetType())
	s.Equal(enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK, resetReq.NewWorkflowSnapshot.TransferTasks[1].GetType())
	s.Equal(enumsspb.TASK_TYPE_TRANSFER_RECORD_WORKFLOW_STARTED, resetReq.NewWorkflowSnapshot.TransferTasks[2].GetType())

	// WF timeout task, user timer, activity timeout timer, activity retry timer
	s.Equal(3, len(resetReq.NewWorkflowSnapshot.TimerTasks))
	s.Equal(enumsspb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT, resetReq.NewWorkflowSnapshot.TimerTasks[0].GetType())
	s.Equal(enumsspb.TASK_TYPE_USER_TIMER, resetReq.NewWorkflowSnapshot.TimerTasks[1].GetType())
	s.Equal(enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT, resetReq.NewWorkflowSnapshot.TimerTasks[2].GetType())

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

func (s *resetorSuite) assertActivityIDs(ids []string, timers []*persistenceblobs.ActivityInfo) {
	m := map[string]bool{}
	for _, s := range ids {
		m[s] = true
	}

	for _, t := range timers {
		delete(m, t.ActivityId)
	}

	s.Equal(0, len(m))
}

func (s *resetorSuite) TestResetWorkflowExecution_NoReplication_WithRequestCancel() {
	testNamespaceEntry := cache.NewLocalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Id: testNamespaceID}, &persistenceblobs.NamespaceConfig{Retention: timestamp.DurationFromDays(1)}, "", nil,
	)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()

	request := &historyservice.ResetWorkflowExecutionRequest{}
	namespaceID := testNamespaceID
	request.NamespaceId = namespaceID
	request.ResetRequest = &workflowservice.ResetWorkflowExecutionRequest{}

	wid := "wId"
	wfType := "wfType"
	taskQueueName := "taskQueue"
	forkRunID := uuid.New().String()
	currRunID := uuid.New().String()
	we := commonpb.WorkflowExecution{
		WorkflowId: wid,
		RunId:      forkRunID,
	}
	request.ResetRequest = &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 "testNamespace",
		WorkflowExecution:         &we,
		Reason:                    "test reset",
		WorkflowTaskFinishEventId: 30,
		RequestId:                 uuid.New().String(),
	}

	forkGwmsRequest := &persistence.GetWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		Execution: commonpb.WorkflowExecution{
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
	cancelWE := &commonpb.WorkflowExecution{
		WorkflowId: "cancel-wfid",
		RunId:      uuid.New().String(),
	}
	forkBranchToken := []byte("forkBranchToken")
	forkExeInfo := &persistence.WorkflowExecutionInfo{
		NamespaceID:            namespaceID,
		WorkflowID:             wid,
		WorkflowTypeName:       wfType,
		TaskQueue:              taskQueueName,
		RunID:                  forkRunID,
		BranchToken:            forkBranchToken,
		NextEventID:            35,
		WorkflowTaskVersion:    common.EmptyVersion,
		WorkflowTaskScheduleID: common.EmptyEventID,
		WorkflowTaskStartedID:  common.EmptyEventID,
		State:                  enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
	}
	forkGwmsResponse := &persistence.GetWorkflowExecutionResponse{State: &persistence.WorkflowMutableState{
		ExecutionInfo:  forkExeInfo,
		ExecutionStats: &persistenceblobs.ExecutionStats{},
	}}

	currGwmsRequest := &persistence.GetWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		Execution: commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      currRunID,
		},
	}
	currExeInfo := &persistence.WorkflowExecutionInfo{
		NamespaceID:            namespaceID,
		WorkflowID:             wid,
		WorkflowTypeName:       wfType,
		TaskQueue:              taskQueueName,
		RunID:                  currRunID,
		NextEventID:            common.FirstEventID,
		WorkflowTaskVersion:    common.EmptyVersion,
		WorkflowTaskScheduleID: common.EmptyEventID,
		WorkflowTaskStartedID:  common.EmptyEventID,
		State:                  enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
	}
	currGwmsResponse := &persistence.GetWorkflowExecutionResponse{State: &persistence.WorkflowMutableState{
		ExecutionInfo:  currExeInfo,
		ExecutionStats: &persistenceblobs.ExecutionStats{},
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

	taskQueue := &taskqueuepb.TaskQueue{
		Name: taskQueueName,
	}
	readHistoryResp := &persistence.ReadHistoryBranchByBatchResponse{
		NextPageToken:    nil,
		Size:             1000,
		LastFirstEventID: int64(31),
		History: []*historypb.History{
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   1,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
						Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
							WorkflowType: &commonpb.WorkflowType{
								Name: wfType,
							},
							TaskQueue:                taskQueue,
							Input:                    payloads.EncodeString("testInput"),
							WorkflowExecutionTimeout: timestamp.DurationPtr(100 * time.Second),
							WorkflowTaskTimeout:      timestamp.DurationPtr(200 * time.Second),
						}},
					},
					{
						EventId:   2,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
							TaskQueue:           taskQueue,
							StartToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   3,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
							ScheduledEventId: 2,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   4,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
							ScheduledEventId: 2,
							StartedEventId:   3,
						}},
					},
					{
						EventId:   5,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_MARKER_RECORDED,
						Attributes: &historypb.HistoryEvent_MarkerRecordedEventAttributes{MarkerRecordedEventAttributes: &historypb.MarkerRecordedEventAttributes{
							MarkerName: "Version",
							Details: map[string]*commonpb.Payloads{
								"change-id": payloads.EncodeString("32283"),
								"version":   payloads.EncodeInt(22),
							},
							WorkflowTaskCompletedEventId: 4,
						}},
					},
					{
						EventId:   6,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDCompleted1,
							ActivityType: &commonpb.ActivityType{
								Name: "actType0",
							},
							TaskQueue:                    taskQueue,
							ScheduleToCloseTimeout:       timestamp.DurationPtr(1000 * time.Second),
							ScheduleToStartTimeout:       timestamp.DurationPtr(2000 * time.Second),
							StartToCloseTimeout:          timestamp.DurationPtr(3000 * time.Second),
							HeartbeatTimeout:             timestamp.DurationPtr(4000 * time.Second),
							WorkflowTaskCompletedEventId: 4,
						}},
					},
					{
						EventId:   7,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_TIMER_STARTED,
						Attributes: &historypb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
							TimerId:                      timerFiredID,
							StartToFireTimeout:           timestamp.DurationPtr(2 * time.Second),
							WorkflowTaskCompletedEventId: 4,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   8,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 6,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   9,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
						Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: 6,
							StartedEventId:   8,
						}},
					},
					{
						EventId:   10,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
							TaskQueue:           taskQueue,
							StartToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   11,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
							ScheduledEventId: 10,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   12,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
							ScheduledEventId: 10,
							StartedEventId:   11,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   13,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_TIMER_FIRED,
						Attributes: &historypb.HistoryEvent_TimerFiredEventAttributes{TimerFiredEventAttributes: &historypb.TimerFiredEventAttributes{
							TimerId: timerFiredID,
						}},
					},
					{
						EventId:   14,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
							TaskQueue:           taskQueue,
							StartToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   15,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
							ScheduledEventId: 14,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   16,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
							ScheduledEventId: 14,
							StartedEventId:   15,
						}},
					},
					{
						EventId:   17,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDStartedRetry,
							ActivityType: &commonpb.ActivityType{
								Name: "actType1",
							},
							TaskQueue:                    taskQueue,
							ScheduleToCloseTimeout:       timestamp.DurationPtr(1000 * time.Second),
							ScheduleToStartTimeout:       timestamp.DurationPtr(2000 * time.Second),
							StartToCloseTimeout:          timestamp.DurationPtr(3000 * time.Second),
							HeartbeatTimeout:             timestamp.DurationPtr(4000 * time.Second),
							WorkflowTaskCompletedEventId: 16,
							RetryPolicy: &commonpb.RetryPolicy{
								InitialInterval:    timestamp.DurationPtr(1 * time.Second),
								BackoffCoefficient: 0.2,
								MaximumAttempts:    10,
								MaximumInterval:    timestamp.DurationPtr(1000 * time.Second),
							},
						}},
					},
					{
						EventId:   18,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDNotStarted,
							ActivityType: &commonpb.ActivityType{
								Name: "actType2",
							},
							TaskQueue:                    taskQueue,
							ScheduleToCloseTimeout:       timestamp.DurationPtr(1000 * time.Second),
							ScheduleToStartTimeout:       timestamp.DurationPtr(2000 * time.Second),
							StartToCloseTimeout:          timestamp.DurationPtr(3000 * time.Second),
							HeartbeatTimeout:             timestamp.DurationPtr(4000 * time.Second),
							WorkflowTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   19,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_TIMER_STARTED,
						Attributes: &historypb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
							TimerId:                      timerUnfiredID1,
							StartToFireTimeout:           timestamp.DurationPtr(4 * time.Second),
							WorkflowTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   20,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_TIMER_STARTED,
						Attributes: &historypb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
							TimerId:                      timerUnfiredID2,
							StartToFireTimeout:           timestamp.DurationPtr(8 * time.Second),
							WorkflowTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   21,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDCompleted2,
							ActivityType: &commonpb.ActivityType{
								Name: "actType2",
							},
							TaskQueue:                    taskQueue,
							ScheduleToCloseTimeout:       timestamp.DurationPtr(1000 * time.Second),
							ScheduleToStartTimeout:       timestamp.DurationPtr(2000 * time.Second),
							StartToCloseTimeout:          timestamp.DurationPtr(3000 * time.Second),
							HeartbeatTimeout:             timestamp.DurationPtr(4000 * time.Second),
							WorkflowTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   22,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDStartedNoRetry,
							ActivityType: &commonpb.ActivityType{
								Name: "actType2",
							},
							TaskQueue:                    taskQueue,
							ScheduleToCloseTimeout:       timestamp.DurationPtr(1000 * time.Second),
							ScheduleToStartTimeout:       timestamp.DurationPtr(2000 * time.Second),
							StartToCloseTimeout:          timestamp.DurationPtr(3000 * time.Second),
							HeartbeatTimeout:             timestamp.DurationPtr(4000 * time.Second),
							WorkflowTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   23,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
						Attributes: &historypb.HistoryEvent_RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{RequestCancelExternalWorkflowExecutionInitiatedEventAttributes: &historypb.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
							Namespace:                    "any-namespace",
							WorkflowExecution:            cancelWE,
							WorkflowTaskCompletedEventId: 16,
							ChildWorkflowOnly:            true,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   24,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 21,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   25,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 17,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   26,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 22,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   27,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
						Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: 21,
							StartedEventId:   24,
						}},
					},
					{
						EventId:   28,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
							TaskQueue:           taskQueue,
							StartToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   29,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
							ScheduledEventId: 28,
						}},
					},
				},
			},
			// ///////////// reset point/////////////
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   30,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
							ScheduledEventId: 28,
							StartedEventId:   29,
						}},
					},
					{
						EventId:   31,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_TIMER_STARTED,
						Attributes: &historypb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
							TimerId:                      timerAfterReset,
							StartToFireTimeout:           timestamp.DurationPtr(4 * time.Second),
							WorkflowTaskCompletedEventId: 30,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   32,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 18,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   33,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
						Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName1,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   34,
						Version:   common.EmptyVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
						Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName2,
						}},
					},
				},
			},
		},
	}

	eid := int64(0)
	eventTime := time.Unix(0, 1000).UTC()
	for _, be := range readHistoryResp.History {
		for _, e := range be.Events {
			eid++
			if e.GetEventId() != eid {
				s.Fail(fmt.Sprintf("inconintous eventID: %v, %v", eid, e.GetEventId()))
			}
			e.EventTime = &eventTime
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
		&persistenceblobs.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
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
	taskQueueName := "taskQueue"
	forkRunID := uuid.New().String()
	currRunID := uuid.New().String()
	we := commonpb.WorkflowExecution{
		WorkflowId: wid,
		RunId:      forkRunID,
	}
	request.ResetRequest = &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 namespace,
		WorkflowExecution:         &we,
		Reason:                    "test reset",
		WorkflowTaskFinishEventId: 30,
		RequestId:                 uuid.New().String(),
	}

	forkGwmsRequest := &persistence.GetWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		Execution: commonpb.WorkflowExecution{
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
		NamespaceID:            namespaceID,
		WorkflowID:             wid,
		WorkflowTypeName:       wfType,
		TaskQueue:              taskQueueName,
		RunID:                  forkRunID,
		BranchToken:            forkBranchToken,
		NextEventID:            35,
		WorkflowTaskVersion:    common.EmptyVersion,
		WorkflowTaskScheduleID: common.EmptyEventID,
		WorkflowTaskStartedID:  common.EmptyEventID,
		State:                  enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
	}

	forkRepState := &persistenceblobs.ReplicationState{
		CurrentVersion:      beforeResetVersion,
		StartVersion:        beforeResetVersion,
		LastWriteEventId:    common.EmptyEventID,
		LastWriteVersion:    common.EmptyVersion,
		LastReplicationInfo: map[string]*replicationspb.ReplicationInfo{},
	}
	forkGwmsResponse := &persistence.GetWorkflowExecutionResponse{State: &persistence.WorkflowMutableState{
		ExecutionInfo:    forkExeInfo,
		ExecutionStats:   &persistenceblobs.ExecutionStats{},
		ReplicationState: forkRepState,
	}}

	currGwmsRequest := &persistence.GetWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		Execution: commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      currRunID,
		},
	}
	currExeInfo := &persistence.WorkflowExecutionInfo{
		NamespaceID:            namespaceID,
		WorkflowID:             wid,
		WorkflowTypeName:       wfType,
		TaskQueue:              taskQueueName,
		RunID:                  currRunID,
		NextEventID:            common.FirstEventID,
		WorkflowTaskVersion:    common.EmptyVersion,
		WorkflowTaskScheduleID: common.EmptyEventID,
		WorkflowTaskStartedID:  common.EmptyEventID,
		State:                  enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
	}
	compareCurrExeInfo := copyWorkflowExecutionInfo(currExeInfo)
	currGwmsResponse := &persistence.GetWorkflowExecutionResponse{State: &persistence.WorkflowMutableState{
		ExecutionInfo:    currExeInfo,
		ExecutionStats:   &persistenceblobs.ExecutionStats{},
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

	taskQueue := &taskqueuepb.TaskQueue{
		Name: taskQueueName,
	}
	readHistoryResp := &persistence.ReadHistoryBranchByBatchResponse{
		NextPageToken:    nil,
		Size:             1000,
		LastFirstEventID: int64(31),
		History: []*historypb.History{
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   1,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
						Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
							WorkflowType: &commonpb.WorkflowType{
								Name: wfType,
							},
							TaskQueue:                taskQueue,
							Input:                    payloads.EncodeString("testInput"),
							WorkflowExecutionTimeout: timestamp.DurationPtr(100 * time.Second),
							WorkflowTaskTimeout:      timestamp.DurationPtr(200 * time.Second),
						}},
					},
					{
						EventId:   2,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
							TaskQueue:           taskQueue,
							StartToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   3,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
							ScheduledEventId: 2,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   4,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
							ScheduledEventId: 2,
							StartedEventId:   3,
						}},
					},
					{
						EventId:   5,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_MARKER_RECORDED,
						Attributes: &historypb.HistoryEvent_MarkerRecordedEventAttributes{MarkerRecordedEventAttributes: &historypb.MarkerRecordedEventAttributes{
							MarkerName: "Version",
							Details: map[string]*commonpb.Payloads{
								"change-id": payloads.EncodeString("32283"),
								"version":   payloads.EncodeInt(22),
							},
							WorkflowTaskCompletedEventId: 4,
						}},
					},
					{
						EventId:   6,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDCompleted1,
							ActivityType: &commonpb.ActivityType{
								Name: "actType0",
							},
							TaskQueue:                    taskQueue,
							ScheduleToCloseTimeout:       timestamp.DurationPtr(1000 * time.Second),
							ScheduleToStartTimeout:       timestamp.DurationPtr(2000 * time.Second),
							StartToCloseTimeout:          timestamp.DurationPtr(3000 * time.Second),
							HeartbeatTimeout:             timestamp.DurationPtr(4000 * time.Second),
							WorkflowTaskCompletedEventId: 4,
						}},
					},
					{
						EventId:   7,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_TIMER_STARTED,
						Attributes: &historypb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
							TimerId:                      timerFiredID,
							StartToFireTimeout:           timestamp.DurationPtr(2 * time.Second),
							WorkflowTaskCompletedEventId: 4,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   8,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 6,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   9,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
						Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: 6,
							StartedEventId:   8,
						}},
					},
					{
						EventId:   10,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
							TaskQueue:           taskQueue,
							StartToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   11,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
							ScheduledEventId: 10,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   12,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
							ScheduledEventId: 10,
							StartedEventId:   11,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   13,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_TIMER_FIRED,
						Attributes: &historypb.HistoryEvent_TimerFiredEventAttributes{TimerFiredEventAttributes: &historypb.TimerFiredEventAttributes{
							TimerId: timerFiredID,
						}},
					},
					{
						EventId:   14,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
							TaskQueue:           taskQueue,
							StartToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   15,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
							ScheduledEventId: 14,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   16,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
							ScheduledEventId: 14,
							StartedEventId:   15,
						}},
					},
					{
						EventId:   17,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDRetry,
							ActivityType: &commonpb.ActivityType{
								Name: "actType1",
							},
							TaskQueue:                    taskQueue,
							ScheduleToCloseTimeout:       timestamp.DurationPtr(1000 * time.Second),
							ScheduleToStartTimeout:       timestamp.DurationPtr(2000 * time.Second),
							StartToCloseTimeout:          timestamp.DurationPtr(3000 * time.Second),
							HeartbeatTimeout:             timestamp.DurationPtr(4000 * time.Second),
							WorkflowTaskCompletedEventId: 16,
							RetryPolicy: &commonpb.RetryPolicy{
								InitialInterval:    timestamp.DurationPtr(1 * time.Second),
								BackoffCoefficient: 0.2,
								MaximumAttempts:    10,
								MaximumInterval:    timestamp.DurationPtr(1000 * time.Second),
							},
						}},
					},
					{
						EventId:   18,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDNotStarted,
							ActivityType: &commonpb.ActivityType{
								Name: "actType2",
							},
							TaskQueue:                    taskQueue,
							ScheduleToCloseTimeout:       timestamp.DurationPtr(1000 * time.Second),
							ScheduleToStartTimeout:       timestamp.DurationPtr(2000 * time.Second),
							StartToCloseTimeout:          timestamp.DurationPtr(3000 * time.Second),
							HeartbeatTimeout:             timestamp.DurationPtr(4000 * time.Second),
							WorkflowTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   19,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_TIMER_STARTED,
						Attributes: &historypb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
							TimerId:                      timerUnfiredID1,
							StartToFireTimeout:           timestamp.DurationPtr(4 * time.Second),
							WorkflowTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   20,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_TIMER_STARTED,
						Attributes: &historypb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
							TimerId:                      timerUnfiredID2,
							StartToFireTimeout:           timestamp.DurationPtr(8 * time.Second),
							WorkflowTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   21,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDCompleted2,
							ActivityType: &commonpb.ActivityType{
								Name: "actType2",
							},
							TaskQueue:                    taskQueue,
							ScheduleToCloseTimeout:       timestamp.DurationPtr(1000 * time.Second),
							ScheduleToStartTimeout:       timestamp.DurationPtr(2000 * time.Second),
							StartToCloseTimeout:          timestamp.DurationPtr(3000 * time.Second),
							HeartbeatTimeout:             timestamp.DurationPtr(4000 * time.Second),
							WorkflowTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   22,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDStartedNoRetry,
							ActivityType: &commonpb.ActivityType{
								Name: "actType2",
							},
							TaskQueue:                    taskQueue,
							ScheduleToCloseTimeout:       timestamp.DurationPtr(1000 * time.Second),
							ScheduleToStartTimeout:       timestamp.DurationPtr(2000 * time.Second),
							StartToCloseTimeout:          timestamp.DurationPtr(3000 * time.Second),
							HeartbeatTimeout:             timestamp.DurationPtr(4000 * time.Second),
							WorkflowTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   23,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
						Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName3,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   24,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 21,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   25,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
						Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName4,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   26,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 22,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   27,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
						Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: 21,
							StartedEventId:   24,
						}},
					},
					{
						EventId:   28,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
							TaskQueue:           taskQueue,
							StartToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   29,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
							ScheduledEventId: 28,
						}},
					},
				},
			},
			// ///////////// reset point/////////////
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   30,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
							ScheduledEventId: 28,
							StartedEventId:   29,
						}},
					},
					{
						EventId:   31,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_TIMER_STARTED,
						Attributes: &historypb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
							TimerId:                      timerAfterReset,
							StartToFireTimeout:           timestamp.DurationPtr(4 * time.Second),
							WorkflowTaskCompletedEventId: 30,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   32,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 18,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   33,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
						Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName1,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   34,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
						Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName2,
						}},
					},
				},
			},
		},
	}

	eid := int64(0)
	eventTime := time.Unix(0, 1000).UTC()
	for _, be := range readHistoryResp.History {
		for _, e := range be.Events {
			eid++
			if e.GetEventId() != eid {
				s.Fail(fmt.Sprintf("inconintous eventID: %v, %v", eid, e.GetEventId()))
			}
			e.EventTime = &eventTime
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
	// 1. workflowTaskFailed
	// 2. activityTaskFailed
	// 3. signal 1
	// 4. signal 2
	// 5. workflowTaskScheduled
	calls := s.mockHistoryV2Mgr.Calls
	s.Equal(4, len(calls))
	appendCall := calls[3]
	s.Equal("AppendHistoryNodes", appendCall.Method)
	appendReq, ok := appendCall.Arguments[0].(*persistence.AppendHistoryNodesRequest)
	s.Equal(true, ok)
	s.Equal(newBranchToken, appendReq.BranchToken)
	s.Equal(false, appendReq.IsNewBranch)
	s.Equal(5, len(appendReq.Events))
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED, enumspb.EventType(appendReq.Events[0].GetEventType()))
	s.Equal(enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED, enumspb.EventType(appendReq.Events[1].GetEventType()))
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, enumspb.EventType(appendReq.Events[2].GetEventType()))
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, enumspb.EventType(appendReq.Events[3].GetEventType()))
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, enumspb.EventType(appendReq.Events[4].GetEventType()))

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
	compareCurrExeInfo.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	compareCurrExeInfo.Status = enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED
	compareCurrExeInfo.NextEventID = 2
	compareCurrExeInfo.LastFirstEventID = 1
	compareCurrExeInfo.CompletionEventBatchID = 1
	s.Equal(compareCurrExeInfo, resetReq.CurrentWorkflowMutation.ExecutionInfo)
	s.Equal(1, len(resetReq.CurrentWorkflowMutation.TransferTasks))
	s.Equal(1, len(resetReq.CurrentWorkflowMutation.TimerTasks))
	s.Equal(enumsspb.TASK_TYPE_TRANSFER_CLOSE_EXECUTION, resetReq.CurrentWorkflowMutation.TransferTasks[0].GetType())
	s.Equal(enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT, resetReq.CurrentWorkflowMutation.TimerTasks[0].GetType())
	s.Equal(int64(200), resetReq.CurrentWorkflowMutation.ExecutionStats.HistorySize)

	s.Equal("wfType", resetReq.NewWorkflowSnapshot.ExecutionInfo.WorkflowTypeName)
	s.True(len(resetReq.NewWorkflowSnapshot.ExecutionInfo.RunID) > 0)
	s.Equal([]byte(newBranchToken), resetReq.NewWorkflowSnapshot.ExecutionInfo.BranchToken)

	s.Equal(int64(34), resetReq.NewWorkflowSnapshot.ExecutionInfo.WorkflowTaskScheduleID)
	s.Equal(int64(35), resetReq.NewWorkflowSnapshot.ExecutionInfo.NextEventID)

	s.Equal(4, len(resetReq.NewWorkflowSnapshot.TransferTasks))
	s.Equal(enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK, resetReq.NewWorkflowSnapshot.TransferTasks[0].GetType())
	s.Equal(enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK, resetReq.NewWorkflowSnapshot.TransferTasks[1].GetType())
	s.Equal(enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK, resetReq.NewWorkflowSnapshot.TransferTasks[2].GetType())
	s.Equal(enumsspb.TASK_TYPE_TRANSFER_RECORD_WORKFLOW_STARTED, resetReq.NewWorkflowSnapshot.TransferTasks[3].GetType())

	// WF timeout task, user timer, activity timeout timer, activity retry timer
	s.Equal(3, len(resetReq.NewWorkflowSnapshot.TimerTasks))
	s.Equal(enumsspb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT, resetReq.NewWorkflowSnapshot.TimerTasks[0].GetType())
	s.Equal(enumsspb.TASK_TYPE_USER_TIMER, resetReq.NewWorkflowSnapshot.TimerTasks[1].GetType())
	s.Equal(enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT, resetReq.NewWorkflowSnapshot.TimerTasks[2].GetType())

	s.Equal(2, len(resetReq.NewWorkflowSnapshot.TimerInfos))
	s.assertTimerIDs([]string{timerUnfiredID1, timerUnfiredID2}, resetReq.NewWorkflowSnapshot.TimerInfos)

	s.Equal(2, len(resetReq.NewWorkflowSnapshot.ActivityInfos))
	s.assertActivityIDs([]string{actIDRetry, actIDNotStarted}, resetReq.NewWorkflowSnapshot.ActivityInfos)

	s.Equal(1, len(resetReq.NewWorkflowSnapshot.ReplicationTasks))
	s.Equal(enumsspb.TASK_TYPE_REPLICATION_HISTORY, resetReq.NewWorkflowSnapshot.ReplicationTasks[0].GetType())
	s.Equal(1, len(resetReq.CurrentWorkflowMutation.ReplicationTasks))
	s.Equal(enumsspb.TASK_TYPE_REPLICATION_HISTORY, resetReq.CurrentWorkflowMutation.ReplicationTasks[0].GetType())

	compareRepState := copyReplicationState(forkRepState)
	compareRepState.StartVersion = beforeResetVersion
	compareRepState.CurrentVersion = afterResetVersion
	compareRepState.LastWriteEventId = 34
	compareRepState.LastWriteVersion = afterResetVersion
	compareRepState.LastReplicationInfo = map[string]*replicationspb.ReplicationInfo{
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
		&persistenceblobs.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
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
	taskQueueName := "taskQueue"
	forkRunID := uuid.New().String()
	currRunID := uuid.New().String()
	we := commonpb.WorkflowExecution{
		WorkflowId: wid,
		RunId:      forkRunID,
	}
	request.ResetRequest = &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 namespace,
		WorkflowExecution:         &we,
		Reason:                    "test reset",
		WorkflowTaskFinishEventId: 30,
		RequestId:                 uuid.New().String(),
	}

	forkGwmsRequest := &persistence.GetWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		Execution: commonpb.WorkflowExecution{
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
		NamespaceID:            namespaceID,
		WorkflowID:             wid,
		WorkflowTypeName:       wfType,
		TaskQueue:              taskQueueName,
		RunID:                  forkRunID,
		BranchToken:            forkBranchToken,
		NextEventID:            35,
		WorkflowTaskVersion:    common.EmptyVersion,
		WorkflowTaskScheduleID: common.EmptyEventID,
		WorkflowTaskStartedID:  common.EmptyEventID,
		State:                  enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
	}

	forkRepState := &persistenceblobs.ReplicationState{
		CurrentVersion:      beforeResetVersion,
		StartVersion:        beforeResetVersion,
		LastWriteEventId:    common.EmptyEventID,
		LastWriteVersion:    common.EmptyVersion,
		LastReplicationInfo: map[string]*replicationspb.ReplicationInfo{},
	}
	forkGwmsResponse := &persistence.GetWorkflowExecutionResponse{State: &persistence.WorkflowMutableState{
		ExecutionInfo:    forkExeInfo,
		ExecutionStats:   &persistenceblobs.ExecutionStats{},
		ReplicationState: forkRepState,
	}}

	currGwmsRequest := &persistence.GetWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		Execution: commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      currRunID,
		},
	}
	currExeInfo := &persistence.WorkflowExecutionInfo{
		NamespaceID:            namespaceID,
		WorkflowID:             wid,
		WorkflowTypeName:       wfType,
		TaskQueue:              taskQueueName,
		RunID:                  currRunID,
		NextEventID:            common.FirstEventID,
		WorkflowTaskVersion:    common.EmptyVersion,
		WorkflowTaskScheduleID: common.EmptyEventID,
		WorkflowTaskStartedID:  common.EmptyEventID,
		State:                  enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
	}
	currGwmsResponse := &persistence.GetWorkflowExecutionResponse{State: &persistence.WorkflowMutableState{
		ExecutionInfo:    currExeInfo,
		ExecutionStats:   &persistenceblobs.ExecutionStats{},
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

	taskQueue := &taskqueuepb.TaskQueue{
		Name: taskQueueName,
	}
	readHistoryResp := &persistence.ReadHistoryBranchByBatchResponse{
		NextPageToken:    nil,
		Size:             1000,
		LastFirstEventID: int64(31),
		History: []*historypb.History{
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   1,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
						Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
							WorkflowType: &commonpb.WorkflowType{
								Name: wfType,
							},
							TaskQueue:           taskQueue,
							Input:               payloads.EncodeString("testInput"),
							WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
							WorkflowTaskTimeout: timestamp.DurationPtr(200 * time.Second),
						}},
					},
					{
						EventId:   2,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
							TaskQueue:           taskQueue,
							StartToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   3,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
							ScheduledEventId: 2,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   4,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
							ScheduledEventId: 2,
							StartedEventId:   3,
						}},
					},
					{
						EventId:   5,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_MARKER_RECORDED,
						Attributes: &historypb.HistoryEvent_MarkerRecordedEventAttributes{MarkerRecordedEventAttributes: &historypb.MarkerRecordedEventAttributes{
							MarkerName: "Version",
							Details: map[string]*commonpb.Payloads{
								"change-id": payloads.EncodeString("32283"),
								"version":   payloads.EncodeInt(22),
							},
							WorkflowTaskCompletedEventId: 4,
						}},
					},
					{
						EventId:   6,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDCompleted1,
							ActivityType: &commonpb.ActivityType{
								Name: "actType0",
							},
							TaskQueue:                    taskQueue,
							ScheduleToCloseTimeout:       timestamp.DurationPtr(1000 * time.Second),
							ScheduleToStartTimeout:       timestamp.DurationPtr(2000 * time.Second),
							StartToCloseTimeout:          timestamp.DurationPtr(3000 * time.Second),
							HeartbeatTimeout:             timestamp.DurationPtr(4000 * time.Second),
							WorkflowTaskCompletedEventId: 4,
						}},
					},
					{
						EventId:   7,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_TIMER_STARTED,
						Attributes: &historypb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
							TimerId:                      timerFiredID,
							StartToFireTimeout:           timestamp.DurationPtr(2 * time.Second),
							WorkflowTaskCompletedEventId: 4,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   8,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 6,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   9,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
						Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: 6,
							StartedEventId:   8,
						}},
					},
					{
						EventId:   10,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
							TaskQueue:           taskQueue,
							StartToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   11,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
							ScheduledEventId: 10,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   12,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
							ScheduledEventId: 10,
							StartedEventId:   11,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   13,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_TIMER_FIRED,
						Attributes: &historypb.HistoryEvent_TimerFiredEventAttributes{TimerFiredEventAttributes: &historypb.TimerFiredEventAttributes{
							TimerId: timerFiredID,
						}},
					},
					{
						EventId:   14,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
							TaskQueue:           taskQueue,
							StartToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   15,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
							ScheduledEventId: 14,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   16,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
							ScheduledEventId: 14,
							StartedEventId:   15,
						}},
					},
					{
						EventId:   17,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDRetry,
							ActivityType: &commonpb.ActivityType{
								Name: "actType1",
							},
							TaskQueue:                    taskQueue,
							ScheduleToCloseTimeout:       timestamp.DurationPtr(1000 * time.Second),
							ScheduleToStartTimeout:       timestamp.DurationPtr(2000 * time.Second),
							StartToCloseTimeout:          timestamp.DurationPtr(3000 * time.Second),
							HeartbeatTimeout:             timestamp.DurationPtr(4000 * time.Second),
							WorkflowTaskCompletedEventId: 16,
							RetryPolicy: &commonpb.RetryPolicy{
								InitialInterval:    timestamp.DurationPtr(1 * time.Second),
								BackoffCoefficient: 0.2,
								MaximumAttempts:    10,
								MaximumInterval:    timestamp.DurationPtr(1000 * time.Second),
							},
						}},
					},
					{
						EventId:   18,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDNotStarted,
							ActivityType: &commonpb.ActivityType{
								Name: "actType2",
							},
							TaskQueue:                    taskQueue,
							ScheduleToCloseTimeout:       timestamp.DurationPtr(1000 * time.Second),
							ScheduleToStartTimeout:       timestamp.DurationPtr(2000 * time.Second),
							StartToCloseTimeout:          timestamp.DurationPtr(3000 * time.Second),
							HeartbeatTimeout:             timestamp.DurationPtr(4000 * time.Second),
							WorkflowTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   19,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_TIMER_STARTED,
						Attributes: &historypb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
							TimerId:                      timerUnfiredID1,
							StartToFireTimeout:           timestamp.DurationPtr(4 * time.Second),
							WorkflowTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   20,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_TIMER_STARTED,
						Attributes: &historypb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
							TimerId:                      timerUnfiredID2,
							StartToFireTimeout:           timestamp.DurationPtr(8 * time.Second),
							WorkflowTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   21,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDCompleted2,
							ActivityType: &commonpb.ActivityType{
								Name: "actType2",
							},
							TaskQueue:                    taskQueue,
							ScheduleToCloseTimeout:       timestamp.DurationPtr(1000 * time.Second),
							ScheduleToStartTimeout:       timestamp.DurationPtr(2000 * time.Second),
							StartToCloseTimeout:          timestamp.DurationPtr(3000 * time.Second),
							HeartbeatTimeout:             timestamp.DurationPtr(4000 * time.Second),
							WorkflowTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   22,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDStartedNoRetry,
							ActivityType: &commonpb.ActivityType{
								Name: "actType2",
							},
							TaskQueue:                    taskQueue,
							ScheduleToCloseTimeout:       timestamp.DurationPtr(1000 * time.Second),
							ScheduleToStartTimeout:       timestamp.DurationPtr(2000 * time.Second),
							StartToCloseTimeout:          timestamp.DurationPtr(3000 * time.Second),
							HeartbeatTimeout:             timestamp.DurationPtr(4000 * time.Second),
							WorkflowTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   23,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
						Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName3,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   24,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 21,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   25,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
						Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName4,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   26,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 22,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   27,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
						Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: 21,
							StartedEventId:   24,
						}},
					},
					{
						EventId:   28,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
							TaskQueue:           taskQueue,
							StartToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   29,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
							ScheduledEventId: 28,
						}},
					},
				},
			},
			// ///////////// reset point/////////////
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   30,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
							ScheduledEventId: 28,
							StartedEventId:   29,
						}},
					},
					{
						EventId:   31,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_TIMER_STARTED,
						Attributes: &historypb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
							TimerId:                      timerAfterReset,
							StartToFireTimeout:           timestamp.DurationPtr(4 * time.Second),
							WorkflowTaskCompletedEventId: 30,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   32,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 18,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   33,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
						Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName1,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   34,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
						Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName2,
						}},
					},
				},
			},
		},
	}

	eid := int64(0)
	eventTime := time.Unix(0, 1000).UTC()
	for _, be := range readHistoryResp.History {
		for _, e := range be.Events {
			eid++
			if e.GetEventId() != eid {
				s.Fail(fmt.Sprintf("inconintous eventID: %v, %v", eid, e.GetEventId()))
			}
			e.EventTime = &eventTime
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
		&persistenceblobs.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
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
	taskQueueName := "taskQueue"
	forkRunID := uuid.New().String()
	currRunID := uuid.New().String()
	we := commonpb.WorkflowExecution{
		WorkflowId: wid,
		RunId:      forkRunID,
	}
	request.ResetRequest = &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 namespace,
		WorkflowExecution:         &we,
		Reason:                    "test reset",
		WorkflowTaskFinishEventId: 30,
		RequestId:                 uuid.New().String(),
	}

	forkGwmsRequest := &persistence.GetWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		Execution: commonpb.WorkflowExecution{
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
		NamespaceID:            namespaceID,
		WorkflowID:             wid,
		WorkflowTypeName:       wfType,
		TaskQueue:              taskQueueName,
		RunID:                  forkRunID,
		BranchToken:            forkBranchToken,
		NextEventID:            35,
		WorkflowTaskVersion:    common.EmptyVersion,
		WorkflowTaskScheduleID: common.EmptyEventID,
		WorkflowTaskStartedID:  common.EmptyEventID,
		State:                  enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
	}

	forkRepState := &persistenceblobs.ReplicationState{
		CurrentVersion:      beforeResetVersion,
		StartVersion:        beforeResetVersion,
		LastWriteEventId:    common.EmptyEventID,
		LastWriteVersion:    common.EmptyVersion,
		LastReplicationInfo: map[string]*replicationspb.ReplicationInfo{},
	}
	forkGwmsResponse := &persistence.GetWorkflowExecutionResponse{State: &persistence.WorkflowMutableState{
		ExecutionInfo:    forkExeInfo,
		ExecutionStats:   &persistenceblobs.ExecutionStats{},
		ReplicationState: forkRepState,
	}}

	currGwmsRequest := &persistence.GetWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		Execution: commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      currRunID,
		},
	}
	currExeInfo := &persistence.WorkflowExecutionInfo{
		NamespaceID:            namespaceID,
		WorkflowID:             wid,
		WorkflowTypeName:       wfType,
		TaskQueue:              taskQueueName,
		RunID:                  currRunID,
		NextEventID:            common.FirstEventID,
		State:                  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		WorkflowTaskVersion:    common.EmptyVersion,
		WorkflowTaskScheduleID: common.EmptyEventID,
		WorkflowTaskStartedID:  common.EmptyEventID,
	}
	compareCurrExeInfo := copyWorkflowExecutionInfo(currExeInfo)
	currGwmsResponse := &persistence.GetWorkflowExecutionResponse{State: &persistence.WorkflowMutableState{
		ExecutionInfo:    currExeInfo,
		ExecutionStats:   &persistenceblobs.ExecutionStats{},
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

	taskQueue := &taskqueuepb.TaskQueue{
		Name: taskQueueName,
	}
	readHistoryResp := &persistence.ReadHistoryBranchByBatchResponse{
		NextPageToken:    nil,
		Size:             1000,
		LastFirstEventID: int64(31),
		History: []*historypb.History{
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   1,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
						Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
							WorkflowType: &commonpb.WorkflowType{
								Name: wfType,
							},
							TaskQueue:           taskQueue,
							Input:               payloads.EncodeString("testInput"),
							WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
							WorkflowTaskTimeout: timestamp.DurationPtr(200 * time.Second),
						}},
					},
					{
						EventId:   2,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
							TaskQueue:           taskQueue,
							StartToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   3,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
							ScheduledEventId: 2,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   4,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
							ScheduledEventId: 2,
							StartedEventId:   3,
						}},
					},
					{
						EventId:   5,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_MARKER_RECORDED,
						Attributes: &historypb.HistoryEvent_MarkerRecordedEventAttributes{MarkerRecordedEventAttributes: &historypb.MarkerRecordedEventAttributes{
							MarkerName: "Version",
							Details: map[string]*commonpb.Payloads{
								"change-id": payloads.EncodeString("32283"),
								"version":   payloads.EncodeInt(22),
							},
							WorkflowTaskCompletedEventId: 4,
						}},
					},
					{
						EventId:   6,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDCompleted1,
							ActivityType: &commonpb.ActivityType{
								Name: "actType0",
							},
							TaskQueue:                    taskQueue,
							ScheduleToCloseTimeout:       timestamp.DurationPtr(1000 * time.Second),
							ScheduleToStartTimeout:       timestamp.DurationPtr(2000 * time.Second),
							StartToCloseTimeout:          timestamp.DurationPtr(3000 * time.Second),
							HeartbeatTimeout:             timestamp.DurationPtr(4000 * time.Second),
							WorkflowTaskCompletedEventId: 4,
						}},
					},
					{
						EventId:   7,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_TIMER_STARTED,
						Attributes: &historypb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
							TimerId:                      timerFiredID,
							StartToFireTimeout:           timestamp.DurationPtr(2 * time.Second),
							WorkflowTaskCompletedEventId: 4,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   8,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 6,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   9,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
						Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: 6,
							StartedEventId:   8,
						}},
					},
					{
						EventId:   10,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
							TaskQueue:           taskQueue,
							StartToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   11,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
							ScheduledEventId: 10,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   12,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
							ScheduledEventId: 10,
							StartedEventId:   11,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   13,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_TIMER_FIRED,
						Attributes: &historypb.HistoryEvent_TimerFiredEventAttributes{TimerFiredEventAttributes: &historypb.TimerFiredEventAttributes{
							TimerId: timerFiredID,
						}},
					},
					{
						EventId:   14,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
							TaskQueue:           taskQueue,
							StartToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   15,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
							ScheduledEventId: 14,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   16,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
							ScheduledEventId: 14,
							StartedEventId:   15,
						}},
					},
					{
						EventId:   17,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDRetry,
							ActivityType: &commonpb.ActivityType{
								Name: "actType1",
							},
							TaskQueue:                    taskQueue,
							ScheduleToCloseTimeout:       timestamp.DurationPtr(1000 * time.Second),
							ScheduleToStartTimeout:       timestamp.DurationPtr(2000 * time.Second),
							StartToCloseTimeout:          timestamp.DurationPtr(3000 * time.Second),
							HeartbeatTimeout:             timestamp.DurationPtr(4000 * time.Second),
							WorkflowTaskCompletedEventId: 16,
							RetryPolicy: &commonpb.RetryPolicy{
								InitialInterval:    timestamp.DurationPtr(1 * time.Second),
								BackoffCoefficient: 0.2,
								MaximumAttempts:    10,
								MaximumInterval:    timestamp.DurationPtr(1000 * time.Second),
							},
						}},
					},
					{
						EventId:   18,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDNotStarted,
							ActivityType: &commonpb.ActivityType{
								Name: "actType2",
							},
							TaskQueue:                    taskQueue,
							ScheduleToCloseTimeout:       timestamp.DurationPtr(1000 * time.Second),
							ScheduleToStartTimeout:       timestamp.DurationPtr(2000 * time.Second),
							StartToCloseTimeout:          timestamp.DurationPtr(3000 * time.Second),
							HeartbeatTimeout:             timestamp.DurationPtr(4000 * time.Second),
							WorkflowTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   19,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_TIMER_STARTED,
						Attributes: &historypb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
							TimerId:                      timerUnfiredID1,
							StartToFireTimeout:           timestamp.DurationPtr(4 * time.Second),
							WorkflowTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   20,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_TIMER_STARTED,
						Attributes: &historypb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
							TimerId:                      timerUnfiredID2,
							StartToFireTimeout:           timestamp.DurationPtr(8 * time.Second),
							WorkflowTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   21,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDCompleted2,
							ActivityType: &commonpb.ActivityType{
								Name: "actType2",
							},
							TaskQueue:                    taskQueue,
							ScheduleToCloseTimeout:       timestamp.DurationPtr(1000 * time.Second),
							ScheduleToStartTimeout:       timestamp.DurationPtr(2000 * time.Second),
							StartToCloseTimeout:          timestamp.DurationPtr(3000 * time.Second),
							HeartbeatTimeout:             timestamp.DurationPtr(4000 * time.Second),
							WorkflowTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   22,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDStartedNoRetry,
							ActivityType: &commonpb.ActivityType{
								Name: "actType2",
							},
							TaskQueue:                    taskQueue,
							ScheduleToCloseTimeout:       timestamp.DurationPtr(1000 * time.Second),
							ScheduleToStartTimeout:       timestamp.DurationPtr(2000 * time.Second),
							StartToCloseTimeout:          timestamp.DurationPtr(3000 * time.Second),
							HeartbeatTimeout:             timestamp.DurationPtr(4000 * time.Second),
							WorkflowTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   23,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
						Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName3,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   24,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 21,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   25,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
						Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName4,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   26,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 22,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   27,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
						Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: 21,
							StartedEventId:   24,
						}},
					},
					{
						EventId:   28,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
							TaskQueue:           taskQueue,
							StartToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   29,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
							ScheduledEventId: 28,
						}},
					},
				},
			},
			// ///////////// reset point/////////////
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   30,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
							ScheduledEventId: 28,
							StartedEventId:   29,
						}},
					},
					{
						EventId:   31,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_TIMER_STARTED,
						Attributes: &historypb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
							TimerId:                      timerAfterReset,
							StartToFireTimeout:           timestamp.DurationPtr(4 * time.Second),
							WorkflowTaskCompletedEventId: 30,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   32,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 18,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   33,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
						Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName1,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   34,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
						Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName2,
						}},
					},
				},
			},
		},
	}

	eid := int64(0)
	eventTime := time.Unix(0, 1000).UTC()
	for _, be := range readHistoryResp.History {
		for _, e := range be.Events {
			eid++
			if e.GetEventId() != eid {
				s.Fail(fmt.Sprintf("inconintous eventID: %v, %v", eid, e.GetEventId()))
			}
			e.EventTime = &eventTime
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
	// 1. workflowTaskFailed
	// 2. activityTaskFailed
	// 3. signal 1
	// 4. signal 2
	// 5. workflowTaskScheduled
	calls := s.mockHistoryV2Mgr.Calls
	s.Equal(3, len(calls))
	appendCall := calls[2]
	s.Equal("AppendHistoryNodes", appendCall.Method)
	appendReq, ok := appendCall.Arguments[0].(*persistence.AppendHistoryNodesRequest)
	s.Equal(true, ok)
	s.Equal(newBranchToken, appendReq.BranchToken)
	s.Equal(false, appendReq.IsNewBranch)
	s.Equal(5, len(appendReq.Events))
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED, enumspb.EventType(appendReq.Events[0].GetEventType()))
	s.Equal(enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED, enumspb.EventType(appendReq.Events[1].GetEventType()))
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, enumspb.EventType(appendReq.Events[2].GetEventType()))
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, enumspb.EventType(appendReq.Events[3].GetEventType()))
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, enumspb.EventType(appendReq.Events[4].GetEventType()))

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

	s.Equal(int64(34), resetReq.NewWorkflowSnapshot.ExecutionInfo.WorkflowTaskScheduleID)
	s.Equal(int64(35), resetReq.NewWorkflowSnapshot.ExecutionInfo.NextEventID)

	s.Equal(4, len(resetReq.NewWorkflowSnapshot.TransferTasks))
	s.Equal(enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK, resetReq.NewWorkflowSnapshot.TransferTasks[0].GetType())
	s.Equal(enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK, resetReq.NewWorkflowSnapshot.TransferTasks[1].GetType())
	s.Equal(enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK, resetReq.NewWorkflowSnapshot.TransferTasks[2].GetType())
	s.Equal(enumsspb.TASK_TYPE_TRANSFER_RECORD_WORKFLOW_STARTED, resetReq.NewWorkflowSnapshot.TransferTasks[3].GetType())

	// WF timeout task, user timer, activity timeout timer, activity retry timer
	s.Equal(3, len(resetReq.NewWorkflowSnapshot.TimerTasks))
	s.Equal(enumsspb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT, resetReq.NewWorkflowSnapshot.TimerTasks[0].GetType())
	s.Equal(enumsspb.TASK_TYPE_USER_TIMER, resetReq.NewWorkflowSnapshot.TimerTasks[1].GetType())
	s.Equal(enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT, resetReq.NewWorkflowSnapshot.TimerTasks[2].GetType())

	s.Equal(2, len(resetReq.NewWorkflowSnapshot.TimerInfos))
	s.assertTimerIDs([]string{timerUnfiredID1, timerUnfiredID2}, resetReq.NewWorkflowSnapshot.TimerInfos)

	s.Equal(2, len(resetReq.NewWorkflowSnapshot.ActivityInfos))
	s.assertActivityIDs([]string{actIDRetry, actIDNotStarted}, resetReq.NewWorkflowSnapshot.ActivityInfos)

	s.Equal(1, len(resetReq.NewWorkflowSnapshot.ReplicationTasks))
	s.Equal(enumsspb.TASK_TYPE_REPLICATION_HISTORY, resetReq.NewWorkflowSnapshot.ReplicationTasks[0].GetType())

	compareRepState := copyReplicationState(forkRepState)
	compareRepState.StartVersion = beforeResetVersion
	compareRepState.CurrentVersion = afterResetVersion
	compareRepState.LastWriteEventId = 34
	compareRepState.LastWriteVersion = afterResetVersion
	compareRepState.LastReplicationInfo = map[string]*replicationspb.ReplicationInfo{
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
		&persistenceblobs.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
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
	taskQueueName := "taskQueue"
	forkRunID := uuid.New().String()
	currRunID := uuid.New().String()
	newRunID := uuid.New().String()
	we := commonpb.WorkflowExecution{
		WorkflowId: wid,
		RunId:      newRunID,
	}

	forkGwmsRequest := &persistence.GetWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		Execution: commonpb.WorkflowExecution{
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
		NamespaceID:            namespaceID,
		WorkflowID:             wid,
		WorkflowTypeName:       wfType,
		TaskQueue:              taskQueueName,
		RunID:                  forkRunID,
		BranchToken:            forkBranchToken,
		NextEventID:            35,
		WorkflowTaskVersion:    common.EmptyVersion,
		WorkflowTaskScheduleID: common.EmptyEventID,
		WorkflowTaskStartedID:  common.EmptyEventID,
		State:                  enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
	}

	forkRepState := &persistenceblobs.ReplicationState{
		CurrentVersion:      beforeResetVersion,
		StartVersion:        beforeResetVersion,
		LastWriteEventId:    common.EmptyEventID,
		LastWriteVersion:    common.EmptyVersion,
		LastReplicationInfo: map[string]*replicationspb.ReplicationInfo{},
	}
	forkGwmsResponse := &persistence.GetWorkflowExecutionResponse{State: &persistence.WorkflowMutableState{
		ExecutionInfo:    forkExeInfo,
		ExecutionStats:   &persistenceblobs.ExecutionStats{},
		ReplicationState: forkRepState,
	}}

	currGwmsRequest := &persistence.GetWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		Execution: commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      currRunID,
		},
	}
	currExeInfo := &persistence.WorkflowExecutionInfo{
		NamespaceID:            namespaceID,
		WorkflowID:             wid,
		WorkflowTypeName:       wfType,
		TaskQueue:              taskQueueName,
		RunID:                  currRunID,
		NextEventID:            common.FirstEventID,
		State:                  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		WorkflowTaskVersion:    common.EmptyVersion,
		WorkflowTaskScheduleID: common.EmptyEventID,
		WorkflowTaskStartedID:  common.EmptyEventID,
	}
	compareCurrExeInfo := copyWorkflowExecutionInfo(currExeInfo)
	currGwmsResponse := &persistence.GetWorkflowExecutionResponse{State: &persistence.WorkflowMutableState{
		ExecutionInfo:    currExeInfo,
		ExecutionStats:   &persistenceblobs.ExecutionStats{},
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

	taskQueue := &taskqueuepb.TaskQueue{
		Name: taskQueueName,
	}

	readHistoryResp := &persistence.ReadHistoryBranchByBatchResponse{
		NextPageToken:    nil,
		Size:             1000,
		LastFirstEventID: int64(31),
		History: []*historypb.History{
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   1,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
						Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
							WorkflowType: &commonpb.WorkflowType{
								Name: wfType,
							},
							TaskQueue:           taskQueue,
							Input:               payloads.EncodeString("testInput"),
							WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
							WorkflowTaskTimeout: timestamp.DurationPtr(200 * time.Second),
						}},
					},
					{
						EventId:   2,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
							TaskQueue:           taskQueue,
							StartToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   3,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
							ScheduledEventId: 2,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   4,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
							ScheduledEventId: 2,
							StartedEventId:   3,
						}},
					},
					{
						EventId:   5,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_MARKER_RECORDED,
						Attributes: &historypb.HistoryEvent_MarkerRecordedEventAttributes{MarkerRecordedEventAttributes: &historypb.MarkerRecordedEventAttributes{
							MarkerName: "Version",
							Details: map[string]*commonpb.Payloads{
								"change-id": payloads.EncodeString("32283"),
								"version":   payloads.EncodeInt(22),
							},
							WorkflowTaskCompletedEventId: 4,
						}},
					},
					{
						EventId:   6,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDCompleted1,
							ActivityType: &commonpb.ActivityType{
								Name: "actType0",
							},
							TaskQueue:                    taskQueue,
							ScheduleToCloseTimeout:       timestamp.DurationPtr(1000 * time.Second),
							ScheduleToStartTimeout:       timestamp.DurationPtr(2000 * time.Second),
							StartToCloseTimeout:          timestamp.DurationPtr(3000 * time.Second),
							HeartbeatTimeout:             timestamp.DurationPtr(4000 * time.Second),
							WorkflowTaskCompletedEventId: 4,
						}},
					},
					{
						EventId:   7,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_TIMER_STARTED,
						Attributes: &historypb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
							TimerId:                      timerFiredID,
							StartToFireTimeout:           timestamp.DurationPtr(2 * time.Second),
							WorkflowTaskCompletedEventId: 4,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   8,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 6,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   9,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
						Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: 6,
							StartedEventId:   8,
						}},
					},
					{
						EventId:   10,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
							TaskQueue:           taskQueue,
							StartToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   11,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
							ScheduledEventId: 10,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   12,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
							ScheduledEventId: 10,
							StartedEventId:   11,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   13,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_TIMER_FIRED,
						Attributes: &historypb.HistoryEvent_TimerFiredEventAttributes{TimerFiredEventAttributes: &historypb.TimerFiredEventAttributes{
							TimerId: timerFiredID,
						}},
					},
					{
						EventId:   14,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
							TaskQueue:           taskQueue,
							StartToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   15,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
							ScheduledEventId: 14,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   16,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
							ScheduledEventId: 14,
							StartedEventId:   15,
						}},
					},
					{
						EventId:   17,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDRetry,
							ActivityType: &commonpb.ActivityType{
								Name: "actType1",
							},
							TaskQueue:                    taskQueue,
							ScheduleToCloseTimeout:       timestamp.DurationPtr(1000 * time.Second),
							ScheduleToStartTimeout:       timestamp.DurationPtr(2000 * time.Second),
							StartToCloseTimeout:          timestamp.DurationPtr(3000 * time.Second),
							HeartbeatTimeout:             timestamp.DurationPtr(4000 * time.Second),
							WorkflowTaskCompletedEventId: 16,
							RetryPolicy: &commonpb.RetryPolicy{
								InitialInterval:    timestamp.DurationPtr(1 * time.Second),
								BackoffCoefficient: 0.2,
								MaximumAttempts:    10,
								MaximumInterval:    timestamp.DurationPtr(1000 * time.Second),
							},
						}},
					},
					{
						EventId:   18,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDNotStarted,
							ActivityType: &commonpb.ActivityType{
								Name: "actType2",
							},
							TaskQueue:                    taskQueue,
							ScheduleToCloseTimeout:       timestamp.DurationPtr(1000 * time.Second),
							ScheduleToStartTimeout:       timestamp.DurationPtr(2000 * time.Second),
							StartToCloseTimeout:          timestamp.DurationPtr(3000 * time.Second),
							HeartbeatTimeout:             timestamp.DurationPtr(4000 * time.Second),
							WorkflowTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   19,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_TIMER_STARTED,
						Attributes: &historypb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
							TimerId:                      timerUnfiredID1,
							StartToFireTimeout:           timestamp.DurationPtr(4 * time.Second),
							WorkflowTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   20,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_TIMER_STARTED,
						Attributes: &historypb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
							TimerId:                      timerUnfiredID2,
							StartToFireTimeout:           timestamp.DurationPtr(8 * time.Second),
							WorkflowTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   21,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDCompleted2,
							ActivityType: &commonpb.ActivityType{
								Name: "actType2",
							},
							TaskQueue:                    taskQueue,
							ScheduleToCloseTimeout:       timestamp.DurationPtr(1000 * time.Second),
							ScheduleToStartTimeout:       timestamp.DurationPtr(2000 * time.Second),
							StartToCloseTimeout:          timestamp.DurationPtr(3000 * time.Second),
							HeartbeatTimeout:             timestamp.DurationPtr(4000 * time.Second),
							WorkflowTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   22,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
							ActivityId: actIDStartedNoRetry,
							ActivityType: &commonpb.ActivityType{
								Name: "actType2",
							},
							TaskQueue:                    taskQueue,
							ScheduleToCloseTimeout:       timestamp.DurationPtr(1000 * time.Second),
							ScheduleToStartTimeout:       timestamp.DurationPtr(2000 * time.Second),
							StartToCloseTimeout:          timestamp.DurationPtr(3000 * time.Second),
							HeartbeatTimeout:             timestamp.DurationPtr(4000 * time.Second),
							WorkflowTaskCompletedEventId: 16,
						}},
					},
					{
						EventId:   23,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
						Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName3,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   24,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 21,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   25,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
						Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
							SignalName: signalName4,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   26,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
							ScheduledEventId: 22,
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   27,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
						Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
							ScheduledEventId: 21,
							StartedEventId:   24,
						}},
					},
					{
						EventId:   28,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
							TaskQueue:           taskQueue,
							StartToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
						}},
					},
				},
			},
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   29,
						Version:   beforeResetVersion,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
						Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
							ScheduledEventId: 28,
						}},
					},
				},
			},
			// ///////////// reset point/////////////
		},
	}

	eid := int64(0)
	eventTime := time.Unix(0, 1000).UTC()
	for _, be := range readHistoryResp.History {
		for _, e := range be.Events {
			eid++
			if e.GetEventId() != eid {
				s.Fail(fmt.Sprintf("inconintous eventID: %v, %v", eid, e.GetEventId()))
			}
			e.EventTime = &eventTime
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

	historyAfterReset := &historypb.History{
		Events: []*historypb.HistoryEvent{
			{
				EventId:   30,
				Version:   afterResetVersion,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskFailedEventAttributes{WorkflowTaskFailedEventAttributes: &historypb.WorkflowTaskFailedEventAttributes{
					ScheduledEventId: int64(28),
					StartedEventId:   int64(29),
					Cause:            enumspb.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW,
					Identity:         identityHistoryService,
					Failure:          failure.NewResetWorkflowFailure("resetWFtest", nil),
					BaseRunId:        forkRunID,
					NewRunId:         newRunID,
					ForkEventVersion: beforeResetVersion,
				}},
			},
			{
				EventId:   31,
				Version:   afterResetVersion,
				EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED,
				Attributes: &historypb.HistoryEvent_ActivityTaskFailedEventAttributes{ActivityTaskFailedEventAttributes: &historypb.ActivityTaskFailedEventAttributes{
					Failure:          failure.NewResetWorkflowFailure("resetWF", nil),
					ScheduledEventId: 22,
					StartedEventId:   26,
					Identity:         identityHistoryService,
				}},
			},
			{
				EventId:   32,
				Version:   afterResetVersion,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
					SignalName: signalName1,
				}},
			},
			{
				EventId:   33,
				Version:   afterResetVersion,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
					SignalName: signalName2,
				}},
			},
			{
				EventId:   34,
				Version:   afterResetVersion,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
					TaskQueue:           taskQueue,
					StartToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
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
	// 1. workflowTaskFailed
	// 2. activityTaskFailed
	// 3. signal 1
	// 4. signal 2
	// 5. workflowTaskScheduled
	calls := s.mockHistoryV2Mgr.Calls
	s.Equal(3, len(calls))
	appendCall := calls[2]
	s.Equal("AppendHistoryNodes", appendCall.Method)
	appendReq, ok := appendCall.Arguments[0].(*persistence.AppendHistoryNodesRequest)
	s.Equal(true, ok)
	s.Equal(newBranchToken, appendReq.BranchToken)
	s.Equal(false, appendReq.IsNewBranch)
	s.Equal(5, len(appendReq.Events))
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED, enumspb.EventType(appendReq.Events[0].GetEventType()))
	s.Equal(enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED, enumspb.EventType(appendReq.Events[1].GetEventType()))
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, enumspb.EventType(appendReq.Events[2].GetEventType()))
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, enumspb.EventType(appendReq.Events[3].GetEventType()))
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, enumspb.EventType(appendReq.Events[4].GetEventType()))

	s.Equal(int64(30), appendReq.Events[0].GetEventId())
	s.Equal(int64(31), appendReq.Events[1].GetEventId())
	s.Equal(int64(32), appendReq.Events[2].GetEventId())
	s.Equal(int64(33), appendReq.Events[3].GetEventId())
	s.Equal(int64(34), appendReq.Events[4].GetEventId())

	s.Equal(enumspb.EncodingType(enumspb.EncodingType_value[s.config.EventEncodingType(namespaceID)]), appendReq.Encoding)

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

	s.Equal(int64(34), resetReq.NewWorkflowSnapshot.ExecutionInfo.WorkflowTaskScheduleID)
	s.Equal(int64(35), resetReq.NewWorkflowSnapshot.ExecutionInfo.NextEventID)

	s.Equal(3, len(resetReq.NewWorkflowSnapshot.TransferTasks))
	s.Equal(enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK, resetReq.NewWorkflowSnapshot.TransferTasks[0].GetType())
	s.Equal(enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK, resetReq.NewWorkflowSnapshot.TransferTasks[1].GetType())
	s.Equal(enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK, resetReq.NewWorkflowSnapshot.TransferTasks[2].GetType())

	// WF timeout task, user timer, activity timeout timer, activity retry timer
	s.Equal(3, len(resetReq.NewWorkflowSnapshot.TimerTasks))
	s.Equal(enumsspb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT, resetReq.NewWorkflowSnapshot.TimerTasks[0].GetType())
	s.Equal(enumsspb.TASK_TYPE_USER_TIMER, resetReq.NewWorkflowSnapshot.TimerTasks[1].GetType())
	s.Equal(enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT, resetReq.NewWorkflowSnapshot.TimerTasks[2].GetType())

	s.Equal(2, len(resetReq.NewWorkflowSnapshot.TimerInfos))
	s.assertTimerIDs([]string{timerUnfiredID1, timerUnfiredID2}, resetReq.NewWorkflowSnapshot.TimerInfos)

	s.Equal(2, len(resetReq.NewWorkflowSnapshot.ActivityInfos))
	s.assertActivityIDs([]string{actIDRetry, actIDNotStarted}, resetReq.NewWorkflowSnapshot.ActivityInfos)

	compareRepState := copyReplicationState(forkRepState)
	compareRepState.StartVersion = beforeResetVersion
	compareRepState.CurrentVersion = afterResetVersion
	compareRepState.LastWriteEventId = 34
	compareRepState.LastWriteVersion = afterResetVersion
	compareRepState.LastReplicationInfo = map[string]*replicationspb.ReplicationInfo{
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
	_, pt = FindAutoResetPoint(timeSource, &namespacepb.BadBinaries{}, &workflowpb.ResetPoints{})
	assert.Nil(t, pt)

	pt0 := &workflowpb.ResetPointInfo{
		BinaryChecksum: "abc",
		Resettable:     true,
	}
	pt1 := &workflowpb.ResetPointInfo{
		BinaryChecksum: "def",
		Resettable:     true,
	}
	pt3 := &workflowpb.ResetPointInfo{
		BinaryChecksum: "ghi",
		Resettable:     false,
	}

	expiredNow := time.Now().UTC().Add(-1 * time.Hour)
	notExpiredNow := time.Now().UTC().Add(time.Hour)
	pt4 := &workflowpb.ResetPointInfo{
		BinaryChecksum: "expired",
		Resettable:     true,
		ExpireTime:     &expiredNow,
	}

	pt5 := &workflowpb.ResetPointInfo{
		BinaryChecksum: "notExpired",
		Resettable:     true,
		ExpireTime:     &notExpiredNow,
	}

	// case 3: two intersection
	_, pt = FindAutoResetPoint(timeSource, &namespacepb.BadBinaries{
		Binaries: map[string]*namespacepb.BadBinaryInfo{
			"abc": {},
			"def": {},
		},
	}, &workflowpb.ResetPoints{
		Points: []*workflowpb.ResetPointInfo{
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
	}, &workflowpb.ResetPoints{
		Points: []*workflowpb.ResetPointInfo{
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
	}, &workflowpb.ResetPoints{
		Points: []*workflowpb.ResetPointInfo{
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
	}, &workflowpb.ResetPoints{
		Points: []*workflowpb.ResetPointInfo{
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
	}, &workflowpb.ResetPoints{
		Points: []*workflowpb.ResetPointInfo{
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
	}, &workflowpb.ResetPoints{
		Points: []*workflowpb.ResetPointInfo{
			pt0, pt1, pt3, pt4, pt5,
		},
	})
	assert.Equal(t, pt.String(), pt5.String())
}
