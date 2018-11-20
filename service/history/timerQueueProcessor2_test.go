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
	"os"
	"testing"
	"time"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	"github.com/uber-go/tally"
	"github.com/uber/cadence/.gen/go/history"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
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
		logger           bark.Logger

		mockHistoryEngine   *historyEngineImpl
		mockMatchingClient  *mocks.MatchingClient
		mockMetadataMgr     *mocks.MetadataManager
		mockVisibilityMgr   *mocks.VisibilityManager
		mockExecutionMgr    *mocks.ExecutionManager
		mockHistoryMgr      *mocks.HistoryManager
		mockShard           ShardContext
		mockClusterMetadata *mocks.ClusterMetadata
		mockProducer        *mocks.KafkaProducer
		mockMessagingClient messaging.Client
		mockService         service.Service
	}
)

func TestTimerQueueProcessor2Suite(t *testing.T) {
	s := new(timerQueueProcessor2Suite)
	suite.Run(t, s)
}

func (s *timerQueueProcessor2Suite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}

	log2 := log.New()
	log2.Level = log.DebugLevel
	s.logger = bark.NewLoggerFromLogrus(log2)

	s.config = NewDynamicConfigForTest()
}

func (s *timerQueueProcessor2Suite) SetupTest() {
	shardID := 0
	s.mockMatchingClient = &mocks.MatchingClient{}
	s.mockExecutionMgr = &mocks.ExecutionManager{}
	s.mockShardManager = &mocks.ShardManager{}
	s.mockHistoryMgr = &mocks.HistoryManager{}
	s.mockVisibilityMgr = &mocks.VisibilityManager{}
	s.mockMetadataMgr = &mocks.MetadataManager{}
	s.mockClusterMetadata = &mocks.ClusterMetadata{}
	// ack manager will use the domain information
	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: "domainID"},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	s.mockProducer = &mocks.KafkaProducer{}
	s.shardClosedCh = make(chan int, 100)
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History)
	s.mockMessagingClient = mocks.NewMockMessagingClient(s.mockProducer, nil)
	s.mockService = service.NewTestService(s.mockClusterMetadata, s.mockMessagingClient, metricsClient, s.logger)

	domainCache := cache.NewDomainCache(s.mockMetadataMgr, s.mockClusterMetadata, metricsClient, s.logger)
	s.mockShard = &shardContextImpl{
		service:                   s.mockService,
		shardInfo:                 &persistence.ShardInfo{ShardID: shardID, RangeID: 1, TransferAckLevel: 0},
		transferSequenceNumber:    1,
		executionManager:          s.mockExecutionMgr,
		shardManager:              s.mockShardManager,
		historyMgr:                s.mockHistoryMgr,
		maxTransferSequenceNumber: 100000,
		closeCh:                   s.shardClosedCh,
		config:                    s.config,
		logger:                    s.logger,
		domainCache:               domainCache,
		metricsClient:             metrics.NewClient(tally.NoopScope, metrics.History),
		timerMaxReadLevelMap:      make(map[string]time.Time),
	}

	historyCache := newHistoryCache(s.mockShard)
	// this is used by shard context, not relevent to this test, so we do not care how many times "GetCurrentClusterName" os called
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	s.mockClusterMetadata.On("GetAllClusterFailoverVersions").Return(cluster.TestAllClusterFailoverVersions)
	s.mockClusterMetadata.On("IsGlobalDomainEnabled").Return(false)
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
	h.txProcessor = newTransferQueueProcessor(s.mockShard, h, s.mockVisibilityMgr, s.mockProducer, s.mockMatchingClient, &mocks.HistoryClient{}, s.logger)
	h.timerProcessor = newTimerQueueProcessor(s.mockShard, h, s.mockMatchingClient, s.logger)
	s.mockHistoryEngine = h
}

func (s *timerQueueProcessor2Suite) TearDownTest() {
	s.mockShardManager.AssertExpectations(s.T())
	s.mockMatchingClient.AssertExpectations(s.T())
	s.mockExecutionMgr.AssertExpectations(s.T())
	s.mockHistoryMgr.AssertExpectations(s.T())
	s.mockVisibilityMgr.AssertExpectations(s.T())
	s.mockClusterMetadata.AssertExpectations(s.T())
	s.mockProducer.AssertExpectations(s.T())
}

func (s *timerQueueProcessor2Suite) TestTimerUpdateTimesOut() {
	domainID := testDomainActiveID
	we := workflow.WorkflowExecution{WorkflowId: common.StringPtr("timer-update-timesout-test"),
		RunId: common.StringPtr(validRunID)}

	taskList := "user-timer-update-times-out"

	builder := newMutableStateBuilder(cluster.TestCurrentClusterName, s.config, s.logger)
	startRequest := &workflow.StartWorkflowExecutionRequest{
		WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr("wType")},
		TaskList:                            common.TaskListPtr(workflow.TaskList{Name: common.StringPtr(taskList)}),
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
	}
	builder.AddWorkflowExecutionStartedEvent(we, &history.StartWorkflowExecutionRequest{
		DomainUUID:   common.StringPtr(domainID),
		StartRequest: startRequest,
	})

	di := addDecisionTaskScheduledEvent(builder)
	addDecisionTaskStartedEvent(builder, di.ScheduleID, taskList, uuid.New())

	waitCh := make(chan struct{})

	mockTS := &mockTimeSource{currTime: time.Now()}

	taskID := int64(100)
	timerTask := &persistence.TimerTaskInfo{
		DomainID:   domainID,
		WorkflowID: "wid",
		RunID:      validRunID,
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

	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(&p.AppendHistoryEventsResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, errors.New("FAILED")).Once()
	s.mockShardManager.On("UpdateShard", mock.Anything).Return(nil)

	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(&p.AppendHistoryEventsResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Run(func(arguments mock.Arguments) {
		// Done.
		waitCh <- struct{}{}
	}).Once()

	// Start timer Processor.
	s.mockHistoryEngine.timerProcessor.(*timerQueueProcessorImpl).activeTimerProcessor.Start()

	s.mockHistoryEngine.timerProcessor.NotifyNewTimers(
		cluster.TestCurrentClusterName,
		s.mockShard.GetCurrentTime(cluster.TestCurrentClusterName),
		[]persistence.Task{&persistence.DecisionTimeoutTask{
			VisibilityTimestamp: timerTask.VisibilityTimestamp,
			EventID:             timerTask.EventID,
		}})

	<-waitCh
	s.mockHistoryEngine.timerProcessor.(*timerQueueProcessorImpl).activeTimerProcessor.Stop()
}

func (s *timerQueueProcessor2Suite) TestWorkflowTimeout() {
	domainID := testDomainActiveID
	we := workflow.WorkflowExecution{WorkflowId: common.StringPtr("workflow-timesout-test"),
		RunId: common.StringPtr(validRunID)}
	taskList := "task-workflow-times-out"

	builder := newMutableStateBuilder(cluster.TestCurrentClusterName, s.config, s.logger)
	startRequest := &workflow.StartWorkflowExecutionRequest{
		WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr("wType")},
		TaskList:                            common.TaskListPtr(workflow.TaskList{Name: common.StringPtr(taskList)}),
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(1),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
	}
	builder.AddWorkflowExecutionStartedEvent(we, &history.StartWorkflowExecutionRequest{
		DomainUUID:   common.StringPtr(domainID),
		StartRequest: startRequest,
	})

	di := addDecisionTaskScheduledEvent(builder)
	addDecisionTaskStartedEvent(builder, di.ScheduleID, taskList, uuid.New())

	waitCh := make(chan struct{})

	mockTS := &mockTimeSource{currTime: time.Now()}

	taskID := int64(100)
	timerTask := &persistence.TimerTaskInfo{
		DomainID:            domainID,
		WorkflowID:          "wid",
		RunID:               validRunID,
		TaskID:              taskID,
		TaskType:            persistence.TaskTypeWorkflowTimeout,
		VisibilityTimestamp: mockTS.Now(),
		EventID:             di.ScheduleID}
	timerIndexResponse := &persistence.GetTimerIndexTasksResponse{Timers: []*persistence.TimerTaskInfo{timerTask}}

	ms := createMutableState(builder)
	wfResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()

	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(&p.AppendHistoryEventsResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Run(func(arguments mock.Arguments) {
		// Done.
		waitCh <- struct{}{}
	}).Once()

	// Start timer Processor.
	emptyResponse := &persistence.GetTimerIndexTasksResponse{Timers: []*persistence.TimerTaskInfo{}}
	s.mockExecutionMgr.On("GetTimerIndexTasks", mock.Anything).Return(emptyResponse, nil).Run(func(arguments mock.Arguments) {
		waitCh <- struct{}{}
	}).Once()
	s.mockExecutionMgr.On("GetTimerIndexTasks", mock.Anything).Return(emptyResponse, nil).Run(func(arguments mock.Arguments) {
		waitCh <- struct{}{}
	}).Once() // for lookAheadTask
	s.mockHistoryEngine.timerProcessor.(*timerQueueProcessorImpl).activeTimerProcessor.Start()
	<-waitCh
	<-waitCh

	s.mockExecutionMgr.On("GetTimerIndexTasks", mock.Anything).Return(timerIndexResponse, nil).Once()
	s.mockExecutionMgr.On("GetTimerIndexTasks", mock.Anything).Return(emptyResponse, nil) // for lookAheadTask
	s.mockHistoryEngine.timerProcessor.NotifyNewTimers(
		cluster.TestCurrentClusterName,
		s.mockShard.GetCurrentTime(cluster.TestCurrentClusterName),
		[]persistence.Task{&persistence.WorkflowTimeoutTask{
			VisibilityTimestamp: timerTask.VisibilityTimestamp,
		}})

	<-waitCh
	s.mockHistoryEngine.timerProcessor.(*timerQueueProcessorImpl).activeTimerProcessor.Stop()
}
