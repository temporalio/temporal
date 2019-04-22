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
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"github.com/uber/cadence/.gen/go/history"
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
	"github.com/uber/cadence/common/service"
	"github.com/uber/cadence/common/xdc"
)

type (
	timerQueueStandbyProcessorSuite struct {
		suite.Suite

		mockShardManager *mocks.ShardManager
		logger           log.Logger

		mockHistoryEngine       *historyEngineImpl
		mockMetadataMgr         *mocks.MetadataManager
		mockVisibilityMgr       *mocks.VisibilityManager
		mockExecutionMgr        *mocks.ExecutionManager
		mockHistoryMgr          *mocks.HistoryManager
		mockShard               ShardContext
		mockClusterMetadata     *mocks.ClusterMetadata
		mockMessagingClient     messaging.Client
		mocktimerQueueAckMgr    *MockTimerQueueAckMgr
		mockService             service.Service
		mockClientBean          *client.MockClientBean
		mockHistoryRereplicator *xdc.MockHistoryRereplicator
		clusterName             string

		timerQueueStandbyProcessor *timerQueueStandbyProcessorImpl
	}
)

func TestTimerQueueStandbyProcessorSuite(t *testing.T) {
	s := new(timerQueueStandbyProcessorSuite)
	suite.Run(t, s)
}

func (s *timerQueueStandbyProcessorSuite) SetupSuite() {

}

func (s *timerQueueStandbyProcessorSuite) SetupTest() {
	shardID := 0
	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)
	s.mockShardManager = &mocks.ShardManager{}
	s.mockExecutionMgr = &mocks.ExecutionManager{}
	s.mockHistoryMgr = &mocks.HistoryManager{}
	s.mockVisibilityMgr = &mocks.VisibilityManager{}
	s.mockMetadataMgr = &mocks.MetadataManager{}
	s.mockClusterMetadata = &mocks.ClusterMetadata{}
	s.mockHistoryRereplicator = &xdc.MockHistoryRereplicator{}
	// ack manager will use the domain information
	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: "domainID"},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestAlternativeClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestAlternativeClusterName},
				},
			},
			IsGlobalDomain: true,
			TableVersion:   persistence.DomainTableVersionV1,
		},
		nil,
	)
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	s.mockClusterMetadata.On("IsGlobalDomainEnabled").Return(true)
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History)
	s.mockClientBean = &client.MockClientBean{}
	s.mockService = service.NewTestService(s.mockClusterMetadata, s.mockMessagingClient, metricsClient, s.mockClientBean)

	config := NewDynamicConfigForTest()
	shardContext := &shardContextImpl{
		service:                   s.mockService,
		shardInfo:                 &persistence.ShardInfo{ShardID: shardID, RangeID: 1, TransferAckLevel: 0},
		transferSequenceNumber:    1,
		executionManager:          s.mockExecutionMgr,
		shardManager:              s.mockShardManager,
		historyMgr:                s.mockHistoryMgr,
		maxTransferSequenceNumber: 100000,
		closeCh:                   make(chan int, 100),
		config:                    config,
		logger:                    s.logger,
		domainCache:               cache.NewDomainCache(s.mockMetadataMgr, s.mockClusterMetadata, metricsClient, s.logger),
		metricsClient:             metrics.NewClient(tally.NoopScope, metrics.History),
		timerMaxReadLevelMap:      make(map[string]time.Time),
		standbyClusterCurrentTime: make(map[string]time.Time),
	}
	shardContext.eventsCache = newEventsCache(shardContext)
	s.mockShard = shardContext

	historyCache := newHistoryCache(s.mockShard)
	h := &historyEngineImpl{
		currentClusterName: s.mockShard.GetService().GetClusterMetadata().GetCurrentClusterName(),
		shard:              s.mockShard,
		historyMgr:         s.mockHistoryMgr,
		executionManager:   s.mockExecutionMgr,
		historyCache:       historyCache,
		logger:             s.logger,
		tokenSerializer:    common.NewJSONTaskTokenSerializer(),
		metricsClient:      s.mockShard.GetMetricsClient(),
	}
	s.mockHistoryEngine = h
	s.clusterName = cluster.TestAlternativeClusterName
	s.timerQueueStandbyProcessor = newTimerQueueStandbyProcessor(s.mockShard, h, s.clusterName, newTaskAllocator(s.mockShard), s.mockHistoryRereplicator, s.logger)
	s.mocktimerQueueAckMgr = &MockTimerQueueAckMgr{}
	s.timerQueueStandbyProcessor.timerQueueAckMgr = s.mocktimerQueueAckMgr
}

func (s *timerQueueStandbyProcessorSuite) TearDownTest() {
	s.mockShardManager.AssertExpectations(s.T())
	s.mockExecutionMgr.AssertExpectations(s.T())
	s.mockHistoryMgr.AssertExpectations(s.T())
	s.mockVisibilityMgr.AssertExpectations(s.T())
	s.mocktimerQueueAckMgr.AssertExpectations(s.T())
	s.mockHistoryRereplicator.AssertExpectations(s.T())
	s.mockClientBean.AssertExpectations(s.T())
}

func (s *timerQueueStandbyProcessorSuite) TestProcessExpiredUserTimer_Pending() {
	domainID := "some random domain ID"
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	version := int64(4096)
	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(
		s.mockClusterMetadata.GetCurrentClusterName(),
		s.mockShard,
		s.mockShard.GetEventsCache(),
		s.logger,
		version,
		execution.GetRunId(),
	)
	msBuilder.AddWorkflowExecutionStartedEvent(
		execution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)

	di := addDecisionTaskScheduledEvent(msBuilder)
	event := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, di.StartedID, nil, "some random identity")

	timerID := "timer"
	timerTimeout := 2 * time.Second
	event, timerInfo := addTimerStartedEvent(msBuilder, event.GetEventId(), timerID, int64(timerTimeout.Seconds()))
	nextEventID := event.GetEventId() + 1

	tBuilder := newTimerBuilder(s.mockShard.GetConfig(), s.logger, clock.NewRealTimeSource())
	tBuilder.AddUserTimer(timerInfo, msBuilder)
	timerTask := &persistence.TimerTaskInfo{
		Version:             version,
		DomainID:            domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		TaskID:              int64(100),
		TaskType:            persistence.TaskTypeUserTimer,
		TimeoutType:         int(workflow.TimeoutTypeStartToClose),
		VisibilityTimestamp: tBuilder.GetUserTimerTaskIfNeeded(msBuilder).(*persistence.UserTimerTask).GetVisibilityTimestamp(),
		EventID:             di.ScheduleID,
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	_, err := s.timerQueueStandbyProcessor.process(timerTask, true)
	s.Equal(ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, time.Now().Add(3*s.mockShard.GetConfig().StandbyClusterDelay()))
	s.mockHistoryRereplicator.On("SendMultiWorkflowHistory",
		timerTask.DomainID, timerTask.WorkflowID,
		timerTask.RunID, nextEventID,
		timerTask.RunID, common.EndEventID,
	).Return(nil).Once()
	_, err = s.timerQueueStandbyProcessor.process(timerTask, true)
	s.Equal(ErrTaskDiscarded, err)
}

func (s *timerQueueStandbyProcessorSuite) TestProcessExpiredUserTimer_Success() {
	domainID := "some random domain ID"
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	version := int64(4096)
	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(),
		s.mockShard, s.mockShard.GetEventsCache(), s.logger, version, execution.GetRunId())
	msBuilder.AddWorkflowExecutionStartedEvent(
		execution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)

	di := addDecisionTaskScheduledEvent(msBuilder)
	event := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, di.StartedID, nil, "some random identity")

	timerID := "timer"
	timerTimeout := 2 * time.Second
	event, timerInfo := addTimerStartedEvent(msBuilder, event.GetEventId(), timerID, int64(timerTimeout.Seconds()))

	tBuilder := newTimerBuilder(s.mockShard.GetConfig(), s.logger, clock.NewRealTimeSource())
	tBuilder.AddUserTimer(timerInfo, msBuilder)
	timerTask := &persistence.TimerTaskInfo{
		Version:             version,
		DomainID:            domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		TaskID:              int64(100),
		TaskType:            persistence.TaskTypeUserTimer,
		TimeoutType:         int(workflow.TimeoutTypeStartToClose),
		VisibilityTimestamp: tBuilder.GetUserTimerTaskIfNeeded(msBuilder).(*persistence.UserTimerTask).GetVisibilityTimestamp(),
		EventID:             di.ScheduleID,
	}

	addTimerFiredEvent(msBuilder, event.GetEventId(), timerID)

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil).Once()

	_, err := s.timerQueueStandbyProcessor.process(timerTask, true)
	s.Nil(err)
}

func (s *timerQueueStandbyProcessorSuite) TestProcessExpiredUserTimer_Multiple() {
	domainID := "some random domain ID"
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	version := int64(4096)
	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(),
		s.mockShard, s.mockShard.GetEventsCache(), s.logger, version, execution.GetRunId())
	msBuilder.AddWorkflowExecutionStartedEvent(
		execution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)

	di := addDecisionTaskScheduledEvent(msBuilder)
	event := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, di.StartedID, nil, "some random identity")

	timerID1 := "timer-1"
	timerTimeout1 := 2 * time.Second
	timerEvent1, timerInfo1 := addTimerStartedEvent(msBuilder, event.GetEventId(), timerID1, int64(timerTimeout1.Seconds()))

	timerID2 := "timer-2"
	timerTimeout2 := 50 * time.Second
	_, timerInfo2 := addTimerStartedEvent(msBuilder, event.GetEventId(), timerID2, int64(timerTimeout2.Seconds()))

	tBuilder := newTimerBuilder(s.mockShard.GetConfig(), s.logger, clock.NewRealTimeSource())
	tBuilder.AddUserTimer(timerInfo1, msBuilder)
	tBuilder.AddUserTimer(timerInfo2, msBuilder)

	timerTask := &persistence.TimerTaskInfo{
		Version:             version,
		DomainID:            domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		TaskID:              int64(100),
		TaskType:            persistence.TaskTypeUserTimer,
		TimeoutType:         int(workflow.TimeoutTypeStartToClose),
		VisibilityTimestamp: tBuilder.GetUserTimerTaskIfNeeded(msBuilder).(*persistence.UserTimerTask).GetVisibilityTimestamp(),
		EventID:             di.ScheduleID,
	}

	addTimerFiredEvent(msBuilder, timerEvent1.GetEventId(), timerID1)

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil).Once()

	_, err := s.timerQueueStandbyProcessor.process(timerTask, true)
	s.Nil(err)
}

func (s *timerQueueStandbyProcessorSuite) TestProcessActivityTimeout_Pending() {
	domainID := "some random domain ID"
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	version := int64(4096)
	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(),
		s.mockShard, s.mockShard.GetEventsCache(), s.logger, version, execution.GetRunId())
	msBuilder.AddWorkflowExecutionStartedEvent(
		execution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)

	di := addDecisionTaskScheduledEvent(msBuilder)
	event := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, di.StartedID, nil, "some random identity")

	tasklist := "tasklist"
	activityID := "activity"
	activityType := "activity type"
	timerTimeout := 2 * time.Second
	scheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, event.GetEventId(), activityID, activityType, tasklist, []byte(nil),
		int32(timerTimeout.Seconds()), int32(timerTimeout.Seconds()), int32(timerTimeout.Seconds()))
	nextEventID := scheduledEvent.GetEventId() + 1

	tBuilder := newTimerBuilder(s.mockShard.GetConfig(), s.logger, clock.NewRealTimeSource())

	timerTask := &persistence.TimerTaskInfo{
		Version:             version,
		DomainID:            domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		TaskID:              int64(100),
		TaskType:            persistence.TaskTypeActivityTimeout,
		TimeoutType:         int(workflow.TimeoutTypeScheduleToClose),
		VisibilityTimestamp: tBuilder.GetActivityTimerTaskIfNeeded(msBuilder).(*persistence.ActivityTimeoutTask).GetVisibilityTimestamp(),
		EventID:             di.ScheduleID,
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil).Once()

	_, err := s.timerQueueStandbyProcessor.process(timerTask, true)
	s.Equal(ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, time.Now().Add(3*s.mockShard.GetConfig().StandbyClusterDelay()))
	s.mockHistoryRereplicator.On("SendMultiWorkflowHistory",
		timerTask.DomainID, timerTask.WorkflowID,
		timerTask.RunID, nextEventID,
		timerTask.RunID, common.EndEventID,
	).Return(nil).Once()
	_, err = s.timerQueueStandbyProcessor.process(timerTask, true)
	s.Equal(ErrTaskDiscarded, err)
}

func (s *timerQueueStandbyProcessorSuite) TestProcessActivityTimeout_Success() {
	domainID := "some random domain ID"
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	version := int64(4096)
	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(),
		s.mockShard, s.mockShard.GetEventsCache(), s.logger, version, execution.GetRunId())
	msBuilder.AddWorkflowExecutionStartedEvent(
		execution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)

	di := addDecisionTaskScheduledEvent(msBuilder)
	event := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, di.StartedID, nil, "some random identity")

	identity := "identity"
	tasklist := "tasklist"
	activityID := "activity"
	activityType := "activity type"
	timerTimeout := 2 * time.Second
	scheduleEvent, timerInfo := addActivityTaskScheduledEvent(msBuilder, event.GetEventId(), activityID, activityType, tasklist, []byte(nil),
		int32(timerTimeout.Seconds()), int32(timerTimeout.Seconds()), int32(timerTimeout.Seconds()))
	startedEvent := addActivityTaskStartedEvent(msBuilder, scheduleEvent.GetEventId(), tasklist, identity)

	tBuilder := newTimerBuilder(s.mockShard.GetConfig(), s.logger, clock.NewRealTimeSource())
	tBuilder.AddStartToCloseActivityTimeout(timerInfo)

	timerTask := &persistence.TimerTaskInfo{
		Version:             version,
		DomainID:            domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		TaskID:              int64(100),
		TaskType:            persistence.TaskTypeActivityTimeout,
		TimeoutType:         int(workflow.TimeoutTypeScheduleToClose),
		VisibilityTimestamp: tBuilder.GetActivityTimerTaskIfNeeded(msBuilder).(*persistence.ActivityTimeoutTask).GetVisibilityTimestamp(),
		EventID:             di.ScheduleID,
	}

	completeEvent := addActivityTaskCompletedEvent(msBuilder, scheduleEvent.GetEventId(), startedEvent.GetEventId(), []byte(nil), identity)
	msBuilder.UpdateReplicationStateLastEventID(s.mockClusterMetadata.GetCurrentClusterName(), completeEvent.GetVersion(), completeEvent.GetEventId())

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil).Once()
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", version).Return(s.mockClusterMetadata.GetCurrentClusterName())

	_, err := s.timerQueueStandbyProcessor.process(timerTask, true)
	s.Nil(err)
}

func (s *timerQueueStandbyProcessorSuite) TestProcessActivityTimeout_Multiple_CanUpdate() {
	domainID := "some random domain ID"
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	version := int64(4096)
	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(),
		s.mockShard, s.mockShard.GetEventsCache(), s.logger, version, execution.GetRunId())
	msBuilder.AddWorkflowExecutionStartedEvent(
		execution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)

	di := addDecisionTaskScheduledEvent(msBuilder)
	event := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, di.StartedID, nil, "some random identity")

	identity := "identity"
	tasklist := "tasklist"
	activityID1 := "activity 1"
	activityType1 := "activity type 1"
	timerTimeout1 := 2 * time.Second
	scheduleEvent1, timerInfo1 := addActivityTaskScheduledEvent(msBuilder, event.GetEventId(), activityID1, activityType1, tasklist, []byte(nil),
		int32(timerTimeout1.Seconds()), int32(timerTimeout1.Seconds()), int32(timerTimeout1.Seconds()))
	startedEvent1 := addActivityTaskStartedEvent(msBuilder, scheduleEvent1.GetEventId(), tasklist, identity)

	activityID2 := "activity 2"
	activityType2 := "activity type 2"
	timerTimeout2 := 20 * time.Second
	scheduleEvent2, timerInfo2 := addActivityTaskScheduledEvent(msBuilder, event.GetEventId(), activityID2, activityType2, tasklist, []byte(nil),
		int32(timerTimeout2.Seconds()), int32(timerTimeout2.Seconds()), int32(timerTimeout2.Seconds()))
	addActivityTaskStartedEvent(msBuilder, scheduleEvent2.GetEventId(), tasklist, identity)
	activityInfo2 := msBuilder.pendingActivityInfoIDs[scheduleEvent2.GetEventId()]
	activityInfo2.TimerTaskStatus |= TimerTaskStatusCreatedHeartbeat
	activityInfo2.LastHeartBeatUpdatedTime = time.Now()

	tBuilder := newTimerBuilder(s.mockShard.GetConfig(), s.logger, clock.NewRealTimeSource())
	tBuilder.AddStartToCloseActivityTimeout(timerInfo1)
	tBuilder.AddScheduleToCloseActivityTimeout(timerInfo2)

	timerTask := &persistence.TimerTaskInfo{
		Version:             version,
		DomainID:            domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		TaskID:              int64(100),
		TaskType:            persistence.TaskTypeActivityTimeout,
		TimeoutType:         int(workflow.TimeoutTypeHeartbeat),
		VisibilityTimestamp: activityInfo2.LastHeartBeatUpdatedTime.Add(-5 * time.Second),
		EventID:             scheduleEvent2.GetEventId(),
	}

	completeEvent1 := addActivityTaskCompletedEvent(msBuilder, scheduleEvent1.GetEventId(), startedEvent1.GetEventId(), []byte(nil), identity)
	msBuilder.UpdateReplicationStateLastEventID(s.mockClusterMetadata.GetCurrentClusterName(), completeEvent1.GetVersion(), completeEvent1.GetEventId())

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil).Once()
	// make the version match the cluster name in standby cluster, so standby cluster can do update on mutable state
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", version).Return(s.clusterName)
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.MatchedBy(func(input *persistence.UpdateWorkflowExecutionRequest) bool {
		s.Equal(1, len(input.TimerTasks))
		s.Equal(1, len(input.UpsertActivityInfos))
		msBuilder.executionInfo.LastUpdatedTimestamp = input.ExecutionInfo.LastUpdatedTimestamp
		input.RangeID = 0
		input.ExecutionInfo.LastEventTaskID = 0
		msBuilder.executionInfo.LastEventTaskID = 0
		s.Equal(&persistence.UpdateWorkflowExecutionRequest{
			ExecutionInfo:                 msBuilder.executionInfo,
			ReplicationState:              msBuilder.replicationState,
			TransferTasks:                 nil,
			ReplicationTasks:              nil,
			TimerTasks:                    input.TimerTasks,
			Condition:                     msBuilder.GetNextEventID(),
			DeleteTimerTask:               nil,
			UpsertActivityInfos:           input.UpsertActivityInfos,
			DeleteActivityInfos:           []int64{},
			UpserTimerInfos:               []*persistence.TimerInfo{},
			DeleteTimerInfos:              []string{},
			UpsertChildExecutionInfos:     []*persistence.ChildExecutionInfo{},
			DeleteChildExecutionInfo:      nil,
			UpsertRequestCancelInfos:      []*persistence.RequestCancelInfo{},
			DeleteRequestCancelInfo:       nil,
			UpsertSignalInfos:             []*persistence.SignalInfo{},
			DeleteSignalInfo:              nil,
			UpsertSignalRequestedIDs:      []string{},
			DeleteSignalRequestedID:       "",
			NewBufferedEvents:             nil,
			ClearBufferedEvents:           false,
			NewBufferedReplicationTask:    nil,
			DeleteBufferedReplicationTask: nil,
			ContinueAsNew:                 nil,
			FinishExecution:               false,
			FinishedExecutionTTL:          0,
			Encoding:                      common.EncodingType(s.mockShard.GetConfig().EventEncodingType(domainID)),
		}, input)
		return true
	})).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.timerQueueStandbyProcessor.process(timerTask, true)
	s.Nil(err)
}

func (s *timerQueueStandbyProcessorSuite) TestProcessDecisionTimeout_Pending() {
	domainID := "some random domain ID"
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	version := int64(4096)
	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(),
		s.mockShard, s.mockShard.GetEventsCache(), s.logger, version, execution.GetRunId())
	msBuilder.AddWorkflowExecutionStartedEvent(
		execution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)

	di := addDecisionTaskScheduledEvent(msBuilder)
	startedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, taskListName, uuid.New())
	nextEventID := startedEvent.GetEventId() + 1

	timerTask := &persistence.TimerTaskInfo{
		Version:             version,
		DomainID:            domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		TaskID:              int64(100),
		TaskType:            persistence.TaskTypeDecisionTimeout,
		TimeoutType:         int(workflow.TimeoutTypeStartToClose),
		VisibilityTimestamp: time.Now(),
		EventID:             di.ScheduleID,
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil).Once()

	_, err := s.timerQueueStandbyProcessor.process(timerTask, true)
	s.Equal(ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, time.Now().Add(3*s.mockShard.GetConfig().StandbyClusterDelay()))
	s.mockHistoryRereplicator.On("SendMultiWorkflowHistory",
		timerTask.DomainID, timerTask.WorkflowID,
		timerTask.RunID, nextEventID,
		timerTask.RunID, common.EndEventID,
	).Return(nil).Once()
	_, err = s.timerQueueStandbyProcessor.process(timerTask, true)
	s.Equal(ErrTaskDiscarded, err)
}

func (s *timerQueueStandbyProcessorSuite) TestProcessDecisionTimeout_Success() {
	domainID := "some random domain ID"
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	version := int64(4096)
	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(),
		s.mockShard, s.mockShard.GetEventsCache(), s.logger, version, execution.GetRunId())
	msBuilder.AddWorkflowExecutionStartedEvent(
		execution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)

	di := addDecisionTaskScheduledEvent(msBuilder)
	event := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, di.StartedID, nil, "some random identity")

	timerTask := &persistence.TimerTaskInfo{
		Version:             version,
		DomainID:            domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		TaskID:              int64(100),
		TaskType:            persistence.TaskTypeDecisionTimeout,
		TimeoutType:         int(workflow.TimeoutTypeStartToClose),
		VisibilityTimestamp: time.Now(),
		EventID:             di.ScheduleID,
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil).Once()

	_, err := s.timerQueueStandbyProcessor.process(timerTask, true)
	s.Nil(err)
}

func (s *timerQueueStandbyProcessorSuite) TestProcessWorkflowBackoffTimer_Pending() {
	domainID := "some random domain ID"
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	version := int64(4096)
	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(),
		s.mockShard, s.mockShard.GetEventsCache(), s.logger, version, execution.GetRunId())
	event := msBuilder.AddWorkflowExecutionStartedEvent(
		execution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)
	nextEventID := event.GetEventId() + 1

	timerTask := &persistence.TimerTaskInfo{
		Version:             version,
		DomainID:            domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		TaskID:              int64(100),
		TaskType:            persistence.TaskTypeWorkflowBackoffTimer,
		VisibilityTimestamp: time.Now(),
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil).Once()

	_, err := s.timerQueueStandbyProcessor.process(timerTask, true)
	s.Equal(ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, time.Now().Add(3*s.mockShard.GetConfig().StandbyClusterDelay()))
	s.mockHistoryRereplicator.On("SendMultiWorkflowHistory",
		timerTask.DomainID, timerTask.WorkflowID,
		timerTask.RunID, nextEventID,
		timerTask.RunID, common.EndEventID,
	).Return(nil).Once()
	_, err = s.timerQueueStandbyProcessor.process(timerTask, true)
	s.Equal(ErrTaskDiscarded, err)
}

func (s *timerQueueStandbyProcessorSuite) TestProcessWorkflowBackoffTimer_Success() {
	domainID := "some random domain ID"
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	version := int64(4096)
	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(),
		s.mockShard, s.mockShard.GetEventsCache(), s.logger, version, execution.GetRunId())
	msBuilder.AddWorkflowExecutionStartedEvent(
		execution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)

	addDecisionTaskScheduledEvent(msBuilder)

	timerTask := &persistence.TimerTaskInfo{
		Version:             version,
		DomainID:            domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		TaskID:              int64(100),
		TaskType:            persistence.TaskTypeWorkflowBackoffTimer,
		VisibilityTimestamp: time.Now(),
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil).Once()

	_, err := s.timerQueueStandbyProcessor.process(timerTask, true)
	s.Nil(err)
}

func (s *timerQueueStandbyProcessorSuite) TestProcessWorkflowTimeout_Pending() {
	domainID := "some random domain ID"
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	version := int64(4096)
	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(),
		s.mockShard, s.mockShard.GetEventsCache(), s.logger, version, execution.GetRunId())
	msBuilder.AddWorkflowExecutionStartedEvent(
		execution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)

	di := addDecisionTaskScheduledEvent(msBuilder)
	startEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = startEvent.GetEventId()
	completionEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, di.StartedID, nil, "some random identity")
	nextEventID := completionEvent.GetEventId() + 1

	timerTask := &persistence.TimerTaskInfo{
		Version:             version,
		DomainID:            domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		TaskID:              int64(100),
		TaskType:            persistence.TaskTypeWorkflowTimeout,
		TimeoutType:         int(workflow.TimeoutTypeStartToClose),
		VisibilityTimestamp: time.Now(),
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil).Once()

	_, err := s.timerQueueStandbyProcessor.process(timerTask, true)
	s.Equal(ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, time.Now().Add(3*s.mockShard.GetConfig().StandbyClusterDelay()))
	s.mockHistoryRereplicator.On("SendMultiWorkflowHistory",
		timerTask.DomainID, timerTask.WorkflowID,
		timerTask.RunID, nextEventID,
		timerTask.RunID, common.EndEventID,
	).Return(nil).Once()
	_, err = s.timerQueueStandbyProcessor.process(timerTask, true)
	s.Equal(ErrTaskDiscarded, err)
}

func (s *timerQueueStandbyProcessorSuite) TestProcessWorkflowTimeout_Success() {
	domainID := "some random domain ID"
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	version := int64(4096)
	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(),
		s.mockShard, s.mockShard.GetEventsCache(), s.logger, version, execution.GetRunId())
	msBuilder.AddWorkflowExecutionStartedEvent(
		execution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)

	di := addDecisionTaskScheduledEvent(msBuilder)
	event := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, di.StartedID, nil, "some random identity")
	addCompleteWorkflowEvent(msBuilder, event.GetEventId(), nil)

	timerTask := &persistence.TimerTaskInfo{
		Version:             version,
		DomainID:            domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		TaskID:              int64(100),
		TaskType:            persistence.TaskTypeWorkflowTimeout,
		TimeoutType:         int(workflow.TimeoutTypeStartToClose),
		VisibilityTimestamp: time.Now(),
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil).Once()

	_, err := s.timerQueueStandbyProcessor.process(timerTask, true)
	s.Nil(err)
}

func (s *timerQueueStandbyProcessorSuite) TestProcessRetryTimeout() {
	domainID := "some random domain ID"
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	version := int64(4096)
	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(),
		s.mockShard, s.mockShard.GetEventsCache(), s.logger, version, execution.GetRunId())
	msBuilder.AddWorkflowExecutionStartedEvent(
		execution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)

	timerTask := &persistence.TimerTaskInfo{
		Version:             version,
		DomainID:            domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		TaskID:              int64(100),
		TaskType:            persistence.TaskTypeActivityRetryTimer,
		TimeoutType:         int(workflow.TimeoutTypeStartToClose),
		VisibilityTimestamp: time.Now(),
	}

	_, err := s.timerQueueStandbyProcessor.process(timerTask, true)
	s.Nil(err)
}
