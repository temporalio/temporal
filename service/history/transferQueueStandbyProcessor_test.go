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

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/matching/matchingservicetest"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/archiver/provider"
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
	transferQueueStandbyProcessorSuite struct {
		suite.Suite

		controller               *gomock.Controller
		mockShardManager         *mocks.ShardManager
		mockHistoryEngine        *historyEngineImpl
		mockDomainCache          *cache.DomainCacheMock
		mockVisibilityMgr        *mocks.VisibilityManager
		mockMatchingClient       *matchingservicetest.MockClient
		mockExecutionMgr         *mocks.ExecutionManager
		mockShard                ShardContext
		mockClusterMetadata      *mocks.ClusterMetadata
		mockProducer             *mocks.KafkaProducer
		mockClientBean           *client.MockClientBean
		mockMessagingClient      messaging.Client
		mockQueueAckMgr          *MockQueueAckMgr
		mockService              service.Service
		mockHistoryRereplicator  *xdc.MockHistoryRereplicator
		logger                   log.Logger
		mockTxProcessor          *MockTransferQueueProcessor
		mockReplicationProcessor *MockReplicatorQueueProcessor
		mockTimerProcessor       *MockTimerQueueProcessor
		mockArchivalMetadata     *archiver.MockArchivalMetadata
		mockArchiverProvider     *provider.MockArchiverProvider

		domainID                      string
		domainEntry                   *cache.DomainCacheEntry
		clusterName                   string
		transferQueueStandbyProcessor *transferQueueStandbyProcessorImpl
	}
)

func TestTransferQueueStandbyProcessorSuite(t *testing.T) {
	s := new(transferQueueStandbyProcessorSuite)
	suite.Run(t, s)
}

func (s *transferQueueStandbyProcessorSuite) SetupSuite() {

}

func (s *transferQueueStandbyProcessorSuite) SetupTest() {
	shardID := 0
	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)
	s.controller = gomock.NewController(s.T())
	s.mockShardManager = &mocks.ShardManager{}
	s.mockExecutionMgr = &mocks.ExecutionManager{}
	s.mockVisibilityMgr = &mocks.VisibilityManager{}
	s.mockMatchingClient = matchingservicetest.NewMockClient(s.controller)
	s.mockDomainCache = &cache.DomainCacheMock{}
	s.mockClusterMetadata = &mocks.ClusterMetadata{}
	s.mockHistoryRereplicator = &xdc.MockHistoryRereplicator{}
	s.mockDomainCache.On("GetDomainByID", testDomainID).Return(testGlobalDomainEntry, nil)
	s.mockDomainCache.On("GetDomain", testDomainName).Return(testGlobalDomainEntry, nil)
	s.mockDomainCache.On("GetDomainByID", testTargetDomainID).Return(testGlobalTargetDomainEntry, nil)
	s.mockDomainCache.On("GetDomain", testTargetDomainName).Return(testGlobalTargetDomainEntry, nil)
	s.mockDomainCache.On("GetDomainByID", testParentDomainID).Return(testGlobalParentDomainEntry, nil).Maybe()
	s.mockDomainCache.On("GetDomain", testParentDomainName).Return(testGlobalParentDomainEntry, nil).Maybe()
	s.mockDomainCache.On("GetDomainByID", testChildDomainID).Return(testGlobalChildDomainEntry, nil).Maybe()
	s.mockDomainCache.On("GetDomain", testChildDomainName).Return(testGlobalChildDomainEntry, nil).Maybe()
	s.mockProducer = &mocks.KafkaProducer{}
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History)
	s.mockMessagingClient = mocks.NewMockMessagingClient(s.mockProducer, nil)
	s.mockClientBean = &client.MockClientBean{}
	s.mockArchivalMetadata = &archiver.MockArchivalMetadata{}
	s.mockArchiverProvider = &provider.MockArchiverProvider{}
	s.mockService = service.NewTestService(
		s.mockClusterMetadata,
		s.mockMessagingClient,
		metricsClient,
		s.mockClientBean,
		s.mockArchivalMetadata,
		s.mockArchiverProvider,
		nil,
	)

	config := NewDynamicConfigForTest()
	shardContext := &shardContextImpl{
		service:                   s.mockService,
		clusterMetadata:           s.mockClusterMetadata,
		shardInfo:                 &persistence.ShardInfo{ShardID: shardID, RangeID: 1, TransferAckLevel: 0},
		transferSequenceNumber:    1,
		executionManager:          s.mockExecutionMgr,
		shardManager:              s.mockShardManager,
		maxTransferSequenceNumber: 100000,
		closeCh:                   make(chan int, 100),
		config:                    config,
		logger:                    s.logger,
		domainCache:               s.mockDomainCache,
		metricsClient:             metricsClient,
		standbyClusterCurrentTime: make(map[string]time.Time),
		timerMaxReadLevelMap:      make(map[string]time.Time),
		timeSource:                clock.NewRealTimeSource(),
	}
	shardContext.eventsCache = newEventsCache(shardContext)
	s.mockShard = shardContext
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	s.mockClusterMetadata.On("GetAllClusterInfo").Return(cluster.TestAllClusterInfo)
	s.mockClusterMetadata.On("IsGlobalDomainEnabled").Return(true)
	s.mockTxProcessor = &MockTransferQueueProcessor{}
	s.mockTxProcessor.On("NotifyNewTask", mock.Anything, mock.Anything).Maybe()
	s.mockReplicationProcessor = NewMockReplicatorQueueProcessor(s.controller)
	s.mockReplicationProcessor.EXPECT().notifyNewTask().AnyTimes()
	s.mockTimerProcessor = &MockTimerQueueProcessor{}
	s.mockTimerProcessor.On("NotifyNewTimers", mock.Anything, mock.Anything).Maybe()

	historyCache := newHistoryCache(s.mockShard)
	h := &historyEngineImpl{
		currentClusterName:   s.mockShard.GetService().GetClusterMetadata().GetCurrentClusterName(),
		shard:                s.mockShard,
		clusterMetadata:      s.mockClusterMetadata,
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
	s.clusterName = cluster.TestAlternativeClusterName
	s.transferQueueStandbyProcessor = newTransferQueueStandbyProcessor(
		s.clusterName,
		s.mockShard,
		h,
		s.mockVisibilityMgr,
		s.mockMatchingClient,
		newTaskAllocator(s.mockShard),
		s.mockHistoryRereplicator,
		s.logger,
	)
	s.mockQueueAckMgr = &MockQueueAckMgr{}
	s.transferQueueStandbyProcessor.queueAckMgr = s.mockQueueAckMgr

	s.domainID = testDomainID
	s.domainEntry = cache.NewLocalDomainCacheEntryForTest(&persistence.DomainInfo{ID: s.domainID}, &persistence.DomainConfig{}, "", nil)
}

func (s *transferQueueStandbyProcessorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShardManager.AssertExpectations(s.T())
	s.mockExecutionMgr.AssertExpectations(s.T())
	s.mockVisibilityMgr.AssertExpectations(s.T())
	s.mockProducer.AssertExpectations(s.T())
	s.mockClientBean.AssertExpectations(s.T())
	s.mockHistoryRereplicator.AssertExpectations(s.T())
	s.mockTxProcessor.AssertExpectations(s.T())
	s.mockTimerProcessor.AssertExpectations(s.T())
	s.mockArchivalMetadata.AssertExpectations(s.T())
	s.mockArchiverProvider.AssertExpectations(s.T())
}

func (s *transferQueueStandbyProcessorSuite) TestProcessActivityTask_Pending() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	version := int64(4096)
	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, version, execution.GetRunId())
	_, err := msBuilder.AddWorkflowExecutionStartedEvent(
		execution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(s.domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(msBuilder)
	event := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, di.StartedID, nil, "some random identity")

	taskID := int64(59)
	activityID := "activity-1"
	activityType := "some random activity type"
	event, _ = addActivityTaskScheduledEvent(msBuilder, event.GetEventId(), activityID, activityType, taskListName, []byte{}, 1, 1, 1)

	s.mockShard.SetCurrentTime(s.clusterName, time.Now())
	transferTask := &persistence.TransferTaskInfo{
		Version:             version,
		DomainID:            s.domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		VisibilityTimestamp: time.Now(),
		TaskID:              taskID,
		TaskList:            taskListName,
		TaskType:            persistence.TransferTaskTypeActivityTask,
		ScheduleID:          event.GetEventId(),
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	_, err = s.transferQueueStandbyProcessor.process(transferTask, true)
	s.Equal(ErrTaskRetry, err)
}

func (s *transferQueueStandbyProcessorSuite) TestProcessActivityTask_Pending_PushToMatching() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	version := int64(4096)
	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, version, execution.GetRunId())
	_, err := msBuilder.AddWorkflowExecutionStartedEvent(
		execution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(s.domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(msBuilder)
	event := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, di.StartedID, nil, "some random identity")

	taskID := int64(59)
	activityID := "activity-1"
	activityType := "some random activity type"
	event, _ = addActivityTaskScheduledEvent(msBuilder, event.GetEventId(), activityID, activityType, taskListName, []byte{}, 1, 1, 1)

	s.mockShard.SetCurrentTime(s.clusterName, time.Now().Add(5*s.mockShard.GetConfig().StandbyClusterDelay()))
	transferTask := &persistence.TransferTaskInfo{
		Version:             version,
		DomainID:            s.domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		VisibilityTimestamp: time.Now(),
		TaskID:              taskID,
		TaskList:            taskListName,
		TaskType:            persistence.TransferTaskTypeActivityTask,
		ScheduleID:          event.GetEventId(),
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.EXPECT().AddActivityTask(gomock.Any(), gomock.Any()).Return(nil).Times(1)

	_, err = s.transferQueueStandbyProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueStandbyProcessorSuite) TestProcessActivityTask_Success() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	version := int64(4096)
	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, version, execution.GetRunId())
	_, err := msBuilder.AddWorkflowExecutionStartedEvent(
		execution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(s.domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(msBuilder)
	event := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, di.StartedID, nil, "some random identity")

	taskID := int64(59)
	activityID := "activity-1"
	activityType := "some random activity type"
	event, _ = addActivityTaskScheduledEvent(msBuilder, event.GetEventId(), activityID, activityType, taskListName, []byte{}, 1, 1, 1)

	transferTask := &persistence.TransferTaskInfo{
		Version:             version,
		DomainID:            s.domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		VisibilityTimestamp: time.Now(),
		TaskID:              taskID,
		TaskList:            taskListName,
		TaskType:            persistence.TransferTaskTypeActivityTask,
		ScheduleID:          event.GetEventId(),
	}

	addActivityTaskStartedEvent(msBuilder, event.GetEventId(), "")

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	_, err = s.transferQueueStandbyProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueStandbyProcessorSuite) TestProcessDecisionTask_Pending() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	version := int64(4096)
	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, version, execution.GetRunId())
	_, err := msBuilder.AddWorkflowExecutionStartedEvent(
		execution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(s.domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)
	s.Nil(err)

	taskID := int64(59)
	di := addDecisionTaskScheduledEvent(msBuilder)

	s.mockShard.SetCurrentTime(s.clusterName, time.Now())
	transferTask := &persistence.TransferTaskInfo{
		Version:             version,
		DomainID:            s.domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		VisibilityTimestamp: time.Now(),
		TaskID:              taskID,
		TaskList:            taskListName,
		TaskType:            persistence.TransferTaskTypeDecisionTask,
		ScheduleID:          di.ScheduleID,
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	_, err = s.transferQueueStandbyProcessor.process(transferTask, true)
	s.Equal(ErrTaskRetry, err)
}

func (s *transferQueueStandbyProcessorSuite) TestProcessDecisionTask_Pending_PushToMatching() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	version := int64(4096)
	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, version, execution.GetRunId())
	_, err := msBuilder.AddWorkflowExecutionStartedEvent(
		execution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(s.domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)
	s.Nil(err)

	taskID := int64(59)
	di := addDecisionTaskScheduledEvent(msBuilder)

	s.mockShard.SetCurrentTime(s.clusterName, time.Now().Add(5*s.mockShard.GetConfig().StandbyClusterDelay()))
	transferTask := &persistence.TransferTaskInfo{
		Version:             version,
		DomainID:            s.domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		VisibilityTimestamp: time.Now(),
		TaskID:              taskID,
		TaskList:            taskListName,
		TaskType:            persistence.TransferTaskTypeDecisionTask,
		ScheduleID:          di.ScheduleID,
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.EXPECT().AddDecisionTask(gomock.Any(), gomock.Any()).Return(nil).Times(1)

	_, err = s.transferQueueStandbyProcessor.process(transferTask, true)
	s.Nil(nil, err)
}

func (s *transferQueueStandbyProcessorSuite) TestProcessDecisionTask_Success_FirstDecision() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	version := int64(4096)
	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, version, execution.GetRunId())
	_, err := msBuilder.AddWorkflowExecutionStartedEvent(
		execution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(s.domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)
	s.Nil(err)

	taskID := int64(59)
	di := addDecisionTaskScheduledEvent(msBuilder)

	transferTask := &persistence.TransferTaskInfo{
		Version:             version,
		DomainID:            s.domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		VisibilityTimestamp: time.Now(),
		TaskID:              taskID,
		TaskList:            taskListName,
		TaskType:            persistence.TransferTaskTypeDecisionTask,
		ScheduleID:          di.ScheduleID,
	}

	event := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	_, err = s.transferQueueStandbyProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueStandbyProcessorSuite) TestProcessDecisionTask_Success_NonFirstDecision() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	version := int64(4096)
	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, version, execution.GetRunId())
	_, err := msBuilder.AddWorkflowExecutionStartedEvent(
		execution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(s.domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(msBuilder)
	event := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, di.StartedID, nil, "some random identity")

	taskID := int64(59)
	di = addDecisionTaskScheduledEvent(msBuilder)

	transferTask := &persistence.TransferTaskInfo{
		Version:             version,
		DomainID:            s.domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		VisibilityTimestamp: time.Now(),
		TaskID:              taskID,
		TaskList:            taskListName,
		TaskType:            persistence.TransferTaskTypeDecisionTask,
		ScheduleID:          di.ScheduleID,
	}

	event = addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	_, err = s.transferQueueStandbyProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueStandbyProcessorSuite) TestProcessCloseExecution() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	version := int64(4096)
	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, version, execution.GetRunId())
	_, err := msBuilder.AddWorkflowExecutionStartedEvent(
		execution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(s.domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(msBuilder)
	event := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, di.StartedID, nil, "some random identity")

	taskID := int64(59)
	event = addCompleteWorkflowEvent(msBuilder, event.GetEventId(), nil)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", version).Return(s.clusterName)
	msBuilder.UpdateReplicationStateLastEventID(version, event.GetEventId())

	transferTask := &persistence.TransferTaskInfo{
		Version:             version,
		DomainID:            s.domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		VisibilityTimestamp: time.Now(),
		TaskID:              taskID,
		TaskList:            taskListName,
		TaskType:            persistence.TransferTaskTypeCloseExecution,
		ScheduleID:          event.GetEventId(),
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockVisibilityMgr.On("RecordWorkflowExecutionClosed", mock.Anything).Return(nil).Once()

	_, err = s.transferQueueStandbyProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueStandbyProcessorSuite) TestProcessCancelExecution_Pending() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	targetExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random target workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}

	version := int64(4096)
	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, version, execution.GetRunId())
	_, err := msBuilder.AddWorkflowExecutionStartedEvent(
		execution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(s.domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(msBuilder)
	event := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, di.StartedID, nil, "some random identity")

	taskID := int64(59)
	event, _ = addRequestCancelInitiatedEvent(msBuilder, event.GetEventId(), uuid.New(), testTargetDomainName, targetExecution.GetWorkflowId(), targetExecution.GetRunId())
	nextEventID := event.GetEventId() + 1

	transferTask := &persistence.TransferTaskInfo{
		Version:             version,
		DomainID:            s.domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		VisibilityTimestamp: time.Now(),
		TargetDomainID:      testTargetDomainID,
		TargetWorkflowID:    targetExecution.GetWorkflowId(),
		TargetRunID:         targetExecution.GetRunId(),
		TaskID:              taskID,
		TaskList:            taskListName,
		TaskType:            persistence.TransferTaskTypeCancelExecution,
		ScheduleID:          event.GetEventId(),
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	_, err = s.transferQueueStandbyProcessor.process(transferTask, true)
	s.Equal(ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, time.Now().Add(5*s.mockShard.GetConfig().StandbyClusterDelay()))
	s.mockHistoryRereplicator.On("SendMultiWorkflowHistory",
		transferTask.DomainID, transferTask.WorkflowID,
		transferTask.RunID, nextEventID,
		transferTask.RunID, common.EndEventID,
	).Return(nil).Once()
	_, err = s.transferQueueStandbyProcessor.process(transferTask, true)
	s.Equal(ErrTaskDiscarded, err)
}

func (s *transferQueueStandbyProcessorSuite) TestProcessCancelExecution_Success() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	targetExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random target workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}

	version := int64(4096)
	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, version, execution.GetRunId())
	_, err := msBuilder.AddWorkflowExecutionStartedEvent(
		execution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(s.domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(msBuilder)
	event := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, di.StartedID, nil, "some random identity")

	taskID := int64(59)
	event, _ = addRequestCancelInitiatedEvent(msBuilder, event.GetEventId(), uuid.New(), testTargetDomainName, targetExecution.GetWorkflowId(), targetExecution.GetRunId())

	transferTask := &persistence.TransferTaskInfo{
		Version:             version,
		DomainID:            s.domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		VisibilityTimestamp: time.Now(),
		TargetDomainID:      testTargetDomainID,
		TargetWorkflowID:    targetExecution.GetWorkflowId(),
		TargetRunID:         targetExecution.GetRunId(),
		TaskID:              taskID,
		TaskList:            taskListName,
		TaskType:            persistence.TransferTaskTypeCancelExecution,
		ScheduleID:          event.GetEventId(),
	}

	addCancelRequestedEvent(msBuilder, event.GetEventId(), testTargetDomainID, targetExecution.GetWorkflowId(), targetExecution.GetRunId())

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	_, err = s.transferQueueStandbyProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueStandbyProcessorSuite) TestProcessSignalExecution_Pending() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	targetExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random target workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	signalName := "some random signal name"

	version := int64(4096)
	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, version, execution.GetRunId())
	_, err := msBuilder.AddWorkflowExecutionStartedEvent(
		execution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(s.domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(msBuilder)
	event := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, di.StartedID, nil, "some random identity")

	taskID := int64(59)
	event, _ = addRequestSignalInitiatedEvent(msBuilder, event.GetEventId(), uuid.New(),
		testTargetDomainName, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), signalName, nil, nil)
	nextEventID := event.GetEventId() + 1

	transferTask := &persistence.TransferTaskInfo{
		Version:             version,
		DomainID:            s.domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		VisibilityTimestamp: time.Now(),
		TargetDomainID:      testTargetDomainID,
		TargetWorkflowID:    targetExecution.GetWorkflowId(),
		TargetRunID:         targetExecution.GetRunId(),
		TaskID:              taskID,
		TaskList:            taskListName,
		TaskType:            persistence.TransferTaskTypeSignalExecution,
		ScheduleID:          event.GetEventId(),
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	_, err = s.transferQueueStandbyProcessor.process(transferTask, true)
	s.Equal(ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, time.Now().Add(5*s.mockShard.GetConfig().StandbyClusterDelay()))
	s.mockHistoryRereplicator.On("SendMultiWorkflowHistory",
		transferTask.DomainID, transferTask.WorkflowID,
		transferTask.RunID, nextEventID,
		transferTask.RunID, common.EndEventID,
	).Return(nil).Once()
	_, err = s.transferQueueStandbyProcessor.process(transferTask, true)
	s.Equal(ErrTaskDiscarded, err)
}

func (s *transferQueueStandbyProcessorSuite) TestProcessSignalExecution_Success() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	targetExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random target workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	signalName := "some random signal name"

	version := int64(4096)
	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, version, execution.GetRunId())
	_, err := msBuilder.AddWorkflowExecutionStartedEvent(
		execution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(s.domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(msBuilder)
	event := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, di.StartedID, nil, "some random identity")

	taskID := int64(59)
	event, _ = addRequestSignalInitiatedEvent(msBuilder, event.GetEventId(), uuid.New(),
		testTargetDomainName, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), signalName, nil, nil)

	transferTask := &persistence.TransferTaskInfo{
		Version:             version,
		DomainID:            s.domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		VisibilityTimestamp: time.Now(),
		TargetDomainID:      testTargetDomainID,
		TargetWorkflowID:    targetExecution.GetWorkflowId(),
		TargetRunID:         targetExecution.GetRunId(),
		TaskID:              taskID,
		TaskList:            taskListName,
		TaskType:            persistence.TransferTaskTypeSignalExecution,
		ScheduleID:          event.GetEventId(),
	}

	addSignaledEvent(msBuilder, event.GetEventId(), testTargetDomainName, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), nil)

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	_, err = s.transferQueueStandbyProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueStandbyProcessorSuite) TestProcessStartChildExecution_Pending() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	childWorkflowID := "some random child workflow ID"
	childWorkflowType := "some random child workflow type"
	childTaskListName := "some random child task list"

	version := int64(4096)
	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, version, execution.GetRunId())
	_, err := msBuilder.AddWorkflowExecutionStartedEvent(
		execution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(s.domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(msBuilder)
	event := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, di.StartedID, nil, "some random identity")

	taskID := int64(59)
	event, _ = addStartChildWorkflowExecutionInitiatedEvent(msBuilder, event.GetEventId(), uuid.New(),
		testChildDomainName, childWorkflowID, childWorkflowType, childTaskListName, nil, 1, 1)
	nextEventID := event.GetEventId() + 1

	transferTask := &persistence.TransferTaskInfo{
		Version:             version,
		DomainID:            s.domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		VisibilityTimestamp: time.Now(),
		TargetDomainID:      testChildDomainID,
		TargetWorkflowID:    childWorkflowID,
		TargetRunID:         "",
		TaskID:              taskID,
		TaskList:            taskListName,
		TaskType:            persistence.TransferTaskTypeStartChildExecution,
		ScheduleID:          event.GetEventId(),
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	_, err = s.transferQueueStandbyProcessor.process(transferTask, true)
	s.Equal(ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, time.Now().Add(5*s.mockShard.GetConfig().StandbyClusterDelay()))
	s.mockHistoryRereplicator.On("SendMultiWorkflowHistory",
		transferTask.DomainID, transferTask.WorkflowID,
		transferTask.RunID, nextEventID,
		transferTask.RunID, common.EndEventID,
	).Return(nil).Once()
	_, err = s.transferQueueStandbyProcessor.process(transferTask, true)
	s.Equal(ErrTaskDiscarded, err)
}

func (s *transferQueueStandbyProcessorSuite) TestProcessStartChildExecution_Success() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	childWorkflowID := "some random child workflow ID"
	childWorkflowType := "some random child workflow type"
	childTaskListName := "some random child task list"

	version := int64(4096)
	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, version, execution.GetRunId())
	_, err := msBuilder.AddWorkflowExecutionStartedEvent(
		execution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(s.domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(msBuilder)
	event := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, di.StartedID, nil, "some random identity")

	taskID := int64(59)
	event, childInfo := addStartChildWorkflowExecutionInitiatedEvent(msBuilder, event.GetEventId(), uuid.New(),
		testChildDomainName, childWorkflowID, childWorkflowType, childTaskListName, nil, 1, 1)

	transferTask := &persistence.TransferTaskInfo{
		Version:             version,
		DomainID:            s.domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		VisibilityTimestamp: time.Now(),
		TargetDomainID:      testChildDomainID,
		TargetWorkflowID:    childWorkflowID,
		TargetRunID:         "",
		TaskID:              taskID,
		TaskList:            taskListName,
		TaskType:            persistence.TransferTaskTypeStartChildExecution,
		ScheduleID:          event.GetEventId(),
	}

	event = addChildWorkflowExecutionStartedEvent(msBuilder, event.GetEventId(), testChildDomainName, childWorkflowID, uuid.New(), childWorkflowType)
	childInfo.StartedID = event.GetEventId()

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	_, err = s.transferQueueStandbyProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueStandbyProcessorSuite) TestProcessRecordWorkflowStartedTask() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	version := int64(4096)
	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, version, execution.GetRunId())
	event, err := msBuilder.AddWorkflowExecutionStartedEvent(
		execution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(s.domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)
	s.Nil(err)

	taskID := int64(59)

	di := addDecisionTaskScheduledEvent(msBuilder)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", version).Return(s.clusterName)
	msBuilder.UpdateReplicationStateLastEventID(di.Version, di.ScheduleID)

	transferTask := &persistence.TransferTaskInfo{
		Version:             version,
		DomainID:            s.domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		VisibilityTimestamp: time.Now(),
		TaskID:              taskID,
		TaskList:            taskListName,
		TaskType:            persistence.TransferTaskTypeRecordWorkflowStarted,
		ScheduleID:          event.GetEventId(),
	}

	persistenceMutableState := createMutableState(msBuilder)
	executionInfo := msBuilder.GetExecutionInfo()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockVisibilityMgr.On("RecordWorkflowExecutionStarted", &persistence.RecordWorkflowExecutionStartedRequest{
		DomainUUID: testDomainID,
		Domain:     testDomainName,
		Execution: workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(executionInfo.WorkflowID),
			RunId:      common.StringPtr(executionInfo.RunID),
		},
		WorkflowTypeName: executionInfo.WorkflowTypeName,
		StartTimestamp:   executionInfo.StartTimestamp.UnixNano(),
		WorkflowTimeout:  int64(executionInfo.WorkflowTimeout),
		TaskID:           taskID,
	}).Return(nil).Once()
	_, err = s.transferQueueStandbyProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueStandbyProcessorSuite) TestProcessUpsertWorkflowSearchAttributesTask() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	version := int64(4096)
	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, version, execution.GetRunId())
	event, err := msBuilder.AddWorkflowExecutionStartedEvent(
		execution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(s.domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)
	s.Nil(err)

	taskID := int64(59)

	di := addDecisionTaskScheduledEvent(msBuilder)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", version).Return(s.clusterName)
	msBuilder.UpdateReplicationStateLastEventID(di.Version, di.ScheduleID)

	transferTask := &persistence.TransferTaskInfo{
		Version:             version,
		DomainID:            s.domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		VisibilityTimestamp: time.Now(),
		TaskID:              taskID,
		TaskList:            taskListName,
		TaskType:            persistence.TransferTaskTypeUpsertWorkflowSearchAttributes,
		ScheduleID:          event.GetEventId(),
	}

	persistenceMutableState := createMutableState(msBuilder)
	executionInfo := msBuilder.GetExecutionInfo()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockVisibilityMgr.On("UpsertWorkflowExecution", &persistence.UpsertWorkflowExecutionRequest{
		DomainUUID: testDomainID,
		Domain:     testDomainName,
		Execution: workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(executionInfo.WorkflowID),
			RunId:      common.StringPtr(executionInfo.RunID),
		},
		WorkflowTypeName: executionInfo.WorkflowTypeName,
		StartTimestamp:   executionInfo.StartTimestamp.UnixNano(),
		WorkflowTimeout:  int64(executionInfo.WorkflowTimeout),
		TaskID:           taskID,
	}).Return(nil).Once()
	_, err = s.transferQueueStandbyProcessor.process(transferTask, true)
	s.Nil(err)
}
