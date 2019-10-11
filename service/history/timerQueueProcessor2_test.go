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
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/matching/matchingservicetest"
	"github.com/uber/cadence/.gen/go/shared"
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
)

type (
	timerQueueProcessor2Suite struct {
		suite.Suite
		mockShardManager *mocks.ShardManager
		shardClosedCh    chan int
		config           *Config
		logger           log.Logger

		controller               *gomock.Controller
		mockHistoryEngine        *historyEngineImpl
		mockMatchingClient       *matchingservicetest.MockClient
		mockDomainCache          *cache.DomainCacheMock
		mockVisibilityMgr        *mocks.VisibilityManager
		mockExecutionMgr         *mocks.ExecutionManager
		mockHistoryV2Mgr         *mocks.HistoryV2Manager
		mockShard                ShardContext
		mockClusterMetadata      *mocks.ClusterMetadata
		mockProducer             *mocks.KafkaProducer
		mockClientBean           *client.MockClientBean
		mockMessagingClient      messaging.Client
		mockService              service.Service
		mockEventsCache          *MockEventsCache
		mockTxProcessor          *MockTransferQueueProcessor
		mockReplicationProcessor *MockReplicatorQueueProcessor
		mockTimerProcessor       *MockTimerQueueProcessor

		domainID                  string
		domainEntry               *cache.DomainCacheEntry
		clusterName               string
		timerQueueActiveProcessor *timerQueueActiveProcessorImpl
	}
)

func TestTimerQueueProcessor2Suite(t *testing.T) {
	s := new(timerQueueProcessor2Suite)
	suite.Run(t, s)
}

func (s *timerQueueProcessor2Suite) SetupSuite() {

	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)

	s.config = NewDynamicConfigForTest()
}

func (s *timerQueueProcessor2Suite) SetupTest() {
	shardID := 0
	s.controller = gomock.NewController(s.T())
	s.mockMatchingClient = matchingservicetest.NewMockClient(s.controller)
	s.mockExecutionMgr = &mocks.ExecutionManager{}
	s.mockShardManager = &mocks.ShardManager{}
	s.mockHistoryV2Mgr = &mocks.HistoryV2Manager{}
	s.mockVisibilityMgr = &mocks.VisibilityManager{}
	s.mockDomainCache = &cache.DomainCacheMock{}
	s.mockClusterMetadata = &mocks.ClusterMetadata{}
	// ack manager will use the domain information
	s.mockDomainCache.On("GetDomainByID", mock.Anything).Return(
		testLocalDomainEntry,
		nil,
	)
	s.mockProducer = &mocks.KafkaProducer{}
	s.shardClosedCh = make(chan int, 100)
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History)
	s.mockMessagingClient = mocks.NewMockMessagingClient(s.mockProducer, nil)
	s.mockClientBean = &client.MockClientBean{}
	s.mockService = service.NewTestService(s.mockClusterMetadata, s.mockMessagingClient, metricsClient, s.mockClientBean, nil, nil, nil)
	s.mockEventsCache = &MockEventsCache{}

	s.mockShard = &shardContextImpl{
		service: s.mockService,
		shardInfo: &persistence.ShardInfo{
			ShardID:                 shardID,
			RangeID:                 1,
			TransferAckLevel:        0,
			ClusterTransferAckLevel: make(map[string]int64),
			ClusterTimerAckLevel:    make(map[string]time.Time),
			TransferFailoverLevels:  make(map[string]persistence.TransferFailoverLevel),
			TimerFailoverLevels:     make(map[string]persistence.TimerFailoverLevel)},
		transferSequenceNumber:    1,
		executionManager:          s.mockExecutionMgr,
		shardManager:              s.mockShardManager,
		clusterMetadata:           s.mockClusterMetadata,
		historyV2Mgr:              s.mockHistoryV2Mgr,
		maxTransferSequenceNumber: 100000,
		closeCh:                   s.shardClosedCh,
		config:                    s.config,
		logger:                    s.logger,
		domainCache:               s.mockDomainCache,
		eventsCache:               s.mockEventsCache,
		metricsClient:             metricsClient,
		timerMaxReadLevelMap:      make(map[string]time.Time),
		timeSource:                clock.NewRealTimeSource(),
	}

	historyCache := newHistoryCache(s.mockShard)
	// this is used by shard context, not relevent to this test, so we do not care how many times "GetCurrentClusterName" os called
	s.mockClusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", common.EmptyVersion).Return(cluster.TestCurrentClusterName)
	s.mockTxProcessor = &MockTransferQueueProcessor{}
	s.mockTxProcessor.On("NotifyNewTask", mock.Anything, mock.Anything).Maybe()
	s.mockReplicationProcessor = NewMockReplicatorQueueProcessor(s.controller)
	s.mockReplicationProcessor.EXPECT().notifyNewTask().AnyTimes()
	s.mockTimerProcessor = &MockTimerQueueProcessor{}
	s.mockTimerProcessor.On("NotifyNewTimers", mock.Anything, mock.Anything).Maybe()
	s.mockClusterMetadata.On("IsArchivalEnabled").Return(false)

	h := &historyEngineImpl{
		currentClusterName:   s.mockShard.GetService().GetClusterMetadata().GetCurrentClusterName(),
		shard:                s.mockShard,
		clusterMetadata:      s.mockClusterMetadata,
		historyV2Mgr:         s.mockHistoryV2Mgr,
		executionManager:     s.mockExecutionMgr,
		historyCache:         historyCache,
		logger:               s.logger,
		tokenSerializer:      common.NewJSONTaskTokenSerializer(),
		metricsClient:        s.mockShard.GetMetricsClient(),
		historyEventNotifier: newHistoryEventNotifier(clock.NewRealTimeSource(), metrics.NewClient(tally.NoopScope, metrics.History), func(string) int { return 0 }),
		txProcessor:          s.mockTxProcessor,
		replicatorProcessor:  s.mockReplicationProcessor,
		timerProcessor:       s.mockTimerProcessor,
	}
	s.mockShard.SetEngine(h)
	s.mockHistoryEngine = h
	s.clusterName = cluster.TestCurrentClusterName
	s.timerQueueActiveProcessor = newTimerQueueActiveProcessor(
		s.mockShard,
		h,
		s.mockMatchingClient,
		newTaskAllocator(s.mockShard),
		s.logger,
	)

	s.domainID = testDomainActiveID
	s.domainEntry = cache.NewLocalDomainCacheEntryForTest(&persistence.DomainInfo{ID: s.domainID}, &persistence.DomainConfig{}, "", nil)
}

func (s *timerQueueProcessor2Suite) TearDownTest() {
	s.controller.Finish()
	s.mockShardManager.AssertExpectations(s.T())
	s.mockExecutionMgr.AssertExpectations(s.T())
	s.mockHistoryV2Mgr.AssertExpectations(s.T())
	s.mockVisibilityMgr.AssertExpectations(s.T())
	s.mockProducer.AssertExpectations(s.T())
	s.mockClientBean.AssertExpectations(s.T())
	s.mockEventsCache.AssertExpectations(s.T())
	s.mockTxProcessor.AssertExpectations(s.T())
	s.mockTimerProcessor.AssertExpectations(s.T())
}

func (s *timerQueueProcessor2Suite) startProcessor() {
	s.timerQueueActiveProcessor.Start()
}

func (s *timerQueueProcessor2Suite) stopProcessor() {
	s.timerQueueActiveProcessor.Stop()
}

func (s *timerQueueProcessor2Suite) TestTimerUpdateTimesOut() {
	we := workflow.WorkflowExecution{WorkflowId: common.StringPtr("timer-update-timesout-test"),
		RunId: common.StringPtr(testRunID)}

	taskList := "user-timer-update-times-out"

	builder := newMutableStateBuilderWithEventV2(s.mockShard, s.mockEventsCache, s.logger, we.GetRunId())
	s.mockEventsCache.On("putEvent", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything).Return().Once()
	startRequest := &workflow.StartWorkflowExecutionRequest{
		WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr("wType")},
		TaskList:                            common.TaskListPtr(workflow.TaskList{Name: common.StringPtr(taskList)}),
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
	}
	_, _ = builder.AddWorkflowExecutionStartedEvent(
		we,
		&history.StartWorkflowExecutionRequest{
			DomainUUID:   common.StringPtr(s.domainID),
			StartRequest: startRequest,
		},
	)

	di := addDecisionTaskScheduledEvent(builder)
	addDecisionTaskStartedEvent(builder, di.ScheduleID, taskList, uuid.New())

	waitCh := make(chan struct{})

	mockTS := &mockTimeSource{currTime: time.Now()}

	taskID := int64(100)
	timerTask := &persistence.TimerTaskInfo{
		DomainID:   s.domainID,
		WorkflowID: "wid",
		RunID:      testRunID,
		TaskID:     taskID,
		TaskType:   persistence.TaskTypeDecisionTimeout, TimeoutType: int(workflow.TimeoutTypeStartToClose),
		VisibilityTimestamp: mockTS.Now(),
		EventID:             di.ScheduleID}
	timerIndexResponse := &persistence.GetTimerIndexTasksResponse{Timers: []*persistence.TimerTaskInfo{timerTask}}

	s.mockExecutionMgr.On("GetTimerIndexTasks", mock.Anything).Return(timerIndexResponse, nil).Once()

	for i := 0; i < 2; i++ {
		ms := createMutableState(builder)
		wfResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
		s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()
	}

	s.mockExecutionMgr.On("GetTimerIndexTasks", mock.Anything).Return(
		&persistence.GetTimerIndexTasksResponse{Timers: []*persistence.TimerTaskInfo{}}, nil)

	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, errors.New("FAILED")).Once()
	s.mockShardManager.On("UpdateShard", mock.Anything).Return(nil)

	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Run(func(arguments mock.Arguments) {
		// Done.
		waitCh <- struct{}{}
	}).Once()

	// Start timer Processor.
	s.startProcessor()

	s.timerQueueActiveProcessor.notifyNewTimers(
		[]persistence.Task{&persistence.DecisionTimeoutTask{
			VisibilityTimestamp: timerTask.VisibilityTimestamp,
			EventID:             timerTask.EventID,
		}})

	<-waitCh
	s.stopProcessor()
}

func (s *timerQueueProcessor2Suite) TestWorkflowTimeout() {
	we := workflow.WorkflowExecution{WorkflowId: common.StringPtr("workflow-timesout-test"),
		RunId: common.StringPtr(testRunID)}
	taskList := "task-workflow-times-out"

	builder := newMutableStateBuilderWithEventV2(s.mockShard, s.mockEventsCache, s.logger, we.GetRunId())
	s.mockEventsCache.On("putEvent", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything).Return().Once()
	startRequest := &workflow.StartWorkflowExecutionRequest{
		WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr("wType")},
		TaskList:                            common.TaskListPtr(workflow.TaskList{Name: common.StringPtr(taskList)}),
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(1),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
	}
	_, _ = builder.AddWorkflowExecutionStartedEvent(
		we,
		&history.StartWorkflowExecutionRequest{
			DomainUUID:   common.StringPtr(s.domainID),
			StartRequest: startRequest,
		},
	)

	di := addDecisionTaskScheduledEvent(builder)
	addDecisionTaskStartedEvent(builder, di.ScheduleID, taskList, uuid.New())

	waitCh := make(chan struct{})

	mockTS := &mockTimeSource{currTime: time.Now()}

	taskID := int64(100)
	timerTask := &persistence.TimerTaskInfo{
		DomainID:            s.domainID,
		WorkflowID:          "wid",
		RunID:               testRunID,
		TaskID:              taskID,
		TaskType:            persistence.TaskTypeWorkflowTimeout,
		VisibilityTimestamp: mockTS.Now(),
		EventID:             di.ScheduleID}
	timerIndexResponse := &persistence.GetTimerIndexTasksResponse{Timers: []*persistence.TimerTaskInfo{timerTask}}

	ms := createMutableState(builder)
	wfResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()

	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Run(func(arguments mock.Arguments) {
		// Done.
		waitCh <- struct{}{}
	}).Once()
	s.mockEventsCache.On("putEvent", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return().Once()
	s.mockEventsCache.On("getEvent", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&workflow.HistoryEvent{}, nil).Once()

	// Start timer Processor.
	emptyResponse := &persistence.GetTimerIndexTasksResponse{Timers: []*persistence.TimerTaskInfo{}}
	s.mockExecutionMgr.On("GetTimerIndexTasks", mock.Anything).Return(emptyResponse, nil).Run(func(arguments mock.Arguments) {
		waitCh <- struct{}{}
	}).Once()
	s.mockExecutionMgr.On("GetTimerIndexTasks", mock.Anything).Return(emptyResponse, nil).Run(func(arguments mock.Arguments) {
		waitCh <- struct{}{}
	}).Once() // for lookAheadTask
	s.startProcessor()
	<-waitCh
	<-waitCh

	s.mockExecutionMgr.On("GetTimerIndexTasks", mock.Anything).Return(timerIndexResponse, nil).Once()
	s.mockExecutionMgr.On("GetTimerIndexTasks", mock.Anything).Return(emptyResponse, nil) // for lookAheadTask
	s.timerQueueActiveProcessor.notifyNewTimers(
		[]persistence.Task{&persistence.WorkflowTimeoutTask{
			VisibilityTimestamp: timerTask.VisibilityTimestamp,
		}})

	<-waitCh
	s.stopProcessor()
}

func (s *timerQueueProcessor2Suite) TestWorkflowTimeout_Cron() {
	we := workflow.WorkflowExecution{WorkflowId: common.StringPtr("workflow-timesout-test"),
		RunId: common.StringPtr(testRunID)}
	taskList := "task-workflow-times-out"
	schedule := "@every 30s"

	builder := newMutableStateBuilderWithEventV2(s.mockShard, s.mockEventsCache, s.logger, we.GetRunId())
	s.mockEventsCache.On("putEvent", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything).Return().Once()
	startRequest := &workflow.StartWorkflowExecutionRequest{
		WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr("wType")},
		TaskList:                            common.TaskListPtr(workflow.TaskList{Name: common.StringPtr(taskList)}),
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(1),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		CronSchedule:                        &schedule,
	}
	_, _ = builder.AddWorkflowExecutionStartedEvent(
		we,
		&history.StartWorkflowExecutionRequest{
			DomainUUID:   common.StringPtr(s.domainID),
			StartRequest: startRequest,
		},
	)

	di := addDecisionTaskScheduledEvent(builder)
	addDecisionTaskStartedEvent(builder, di.ScheduleID, taskList, uuid.New())

	waitCh := make(chan struct{})

	mockTS := &mockTimeSource{currTime: time.Now()}

	taskID := int64(100)
	timerTask := &persistence.TimerTaskInfo{
		DomainID:            s.domainID,
		WorkflowID:          "wid",
		RunID:               testRunID,
		TaskID:              taskID,
		TaskType:            persistence.TaskTypeWorkflowTimeout,
		VisibilityTimestamp: mockTS.Now(),
		EventID:             di.ScheduleID}
	timerIndexResponse := &persistence.GetTimerIndexTasksResponse{Timers: []*persistence.TimerTaskInfo{timerTask}}

	ms := createMutableState(builder)
	ms.ExecutionInfo.StartTimestamp = time.Now()
	wfResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Times(2)
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Run(func(arguments mock.Arguments) {
		// Done.
		waitCh <- struct{}{}
	}).Once()
	s.mockEventsCache.On("putEvent", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return().Times(2)
	startedEvent := &workflow.HistoryEvent{
		WorkflowExecutionStartedEventAttributes: &shared.WorkflowExecutionStartedEventAttributes{
			ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(0),
			TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(0),
		},
	}
	s.mockEventsCache.On("getEvent", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(startedEvent, nil).Times(4)

	// Start timer Processor.
	emptyResponse := &persistence.GetTimerIndexTasksResponse{Timers: []*persistence.TimerTaskInfo{}}
	s.mockExecutionMgr.On("GetTimerIndexTasks", mock.Anything).Return(emptyResponse, nil).Run(func(arguments mock.Arguments) {
		waitCh <- struct{}{}
	}).Once()
	s.mockExecutionMgr.On("GetTimerIndexTasks", mock.Anything).Return(emptyResponse, nil).Run(func(arguments mock.Arguments) {
		waitCh <- struct{}{}
	}).Once() // for lookAheadTask
	s.startProcessor()
	<-waitCh
	<-waitCh
	s.mockExecutionMgr.On("GetTimerIndexTasks", mock.Anything).Return(timerIndexResponse, nil).Once()
	s.mockExecutionMgr.On("GetTimerIndexTasks", mock.Anything).Return(emptyResponse, nil) // for lookAheadTask
	s.timerQueueActiveProcessor.notifyNewTimers(
		[]persistence.Task{&persistence.WorkflowTimeoutTask{
			VisibilityTimestamp: timerTask.VisibilityTimestamp,
		}})

	<-waitCh
	s.stopProcessor()
}
