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
	"github.com/uber/cadence/.gen/go/history/historyservicetest"
	"github.com/uber/cadence/.gen/go/matching"
	"github.com/uber/cadence/.gen/go/matching/matchingservicetest"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/archiver/provider"
	"github.com/uber/cadence/common/backoff"
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
	"github.com/uber/cadence/service/worker/parentclosepolicy"
)

type (
	transferQueueActiveProcessorSuite struct {
		suite.Suite

		controller                  *gomock.Controller
		mockShardManager            *mocks.ShardManager
		mockHistoryEngine           *historyEngineImpl
		mockDomainCache             *cache.DomainCacheMock
		mockVisibilityMgr           *mocks.VisibilityManager
		mockExecutionMgr            *mocks.ExecutionManager
		mockHistoryV2Mgr            *mocks.HistoryV2Manager
		mockMatchingClient          *matchingservicetest.MockClient
		mockHistoryClient           *historyservicetest.MockClient
		mockShard                   ShardContext
		mockClusterMetadata         *mocks.ClusterMetadata
		mockProducer                *mocks.KafkaProducer
		mockMessagingClient         messaging.Client
		mockQueueAckMgr             *MockQueueAckMgr
		mockClientBean              *client.MockClientBean
		mockService                 service.Service
		logger                      log.Logger
		mockTxProcessor             *MockTransferQueueProcessor
		mockReplicationProcessor    *MockReplicatorQueueProcessor
		mockTimerProcessor          *MockTimerQueueProcessor
		mockArchivalMetadata        *archiver.MockArchivalMetadata
		mockArchiverProvider        *provider.MockArchiverProvider
		mockParentClosePolicyClient *parentclosepolicy.ClientMock

		domainID                     string
		domainEntry                  *cache.DomainCacheEntry
		version                      int64
		transferQueueActiveProcessor *transferQueueActiveProcessorImpl
	}
)

func TestTransferQueueActiveProcessorSuite(t *testing.T) {
	s := new(transferQueueActiveProcessorSuite)
	suite.Run(t, s)
}

func (s *transferQueueActiveProcessorSuite) SetupSuite() {

}

func (s *transferQueueActiveProcessorSuite) TearDownSuite() {

}

func (s *transferQueueActiveProcessorSuite) SetupTest() {
	shardID := 0
	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)
	s.controller = gomock.NewController(s.T())
	s.mockShardManager = &mocks.ShardManager{}
	s.mockExecutionMgr = &mocks.ExecutionManager{}
	s.mockHistoryV2Mgr = &mocks.HistoryV2Manager{}
	s.mockVisibilityMgr = &mocks.VisibilityManager{}
	s.mockMatchingClient = matchingservicetest.NewMockClient(s.controller)
	s.mockHistoryClient = historyservicetest.NewMockClient(s.controller)
	s.mockDomainCache = &cache.DomainCacheMock{}
	s.mockClusterMetadata = &mocks.ClusterMetadata{}
	s.version = testGlobalDomainEntry.GetFailoverVersion()
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

	shardContext := &shardContextImpl{
		service:                   s.mockService,
		shardInfo:                 &persistence.ShardInfo{ShardID: shardID, RangeID: 1, TransferAckLevel: 0},
		transferSequenceNumber:    1,
		executionManager:          s.mockExecutionMgr,
		shardManager:              s.mockShardManager,
		historyV2Mgr:              s.mockHistoryV2Mgr,
		clusterMetadata:           s.mockClusterMetadata,
		maxTransferSequenceNumber: 100000,
		closeCh:                   make(chan int, 100),
		config:                    NewDynamicConfigForTest(),
		logger:                    s.logger,
		domainCache:               s.mockDomainCache,
		metricsClient:             metricsClient,
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
	s.mockQueueAckMgr = &MockQueueAckMgr{}
	s.transferQueueActiveProcessor = newTransferQueueActiveProcessor(
		s.mockShard,
		h,
		s.mockVisibilityMgr,
		s.mockMatchingClient,
		s.mockHistoryClient,
		newTaskAllocator(s.mockShard),
		s.logger,
	)
	s.transferQueueActiveProcessor.queueAckMgr = s.mockQueueAckMgr
	s.transferQueueActiveProcessor.queueProcessorBase.ackMgr = s.mockQueueAckMgr

	s.mockParentClosePolicyClient = &parentclosepolicy.ClientMock{}
	s.transferQueueActiveProcessor.parentClosePolicyClient = s.mockParentClosePolicyClient

	s.domainID = testDomainID
	s.domainEntry = cache.NewLocalDomainCacheEntryForTest(&persistence.DomainInfo{ID: s.domainID}, &persistence.DomainConfig{}, "", nil)
}

func (s *transferQueueActiveProcessorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShardManager.AssertExpectations(s.T())
	s.mockExecutionMgr.AssertExpectations(s.T())
	s.mockHistoryV2Mgr.AssertExpectations(s.T())
	s.mockVisibilityMgr.AssertExpectations(s.T())
	s.mockProducer.AssertExpectations(s.T())
	s.mockQueueAckMgr.AssertExpectations(s.T())
	s.mockClientBean.AssertExpectations(s.T())
	s.mockTxProcessor.AssertExpectations(s.T())
	s.mockTimerProcessor.AssertExpectations(s.T())
	s.mockArchivalMetadata.AssertExpectations(s.T())
	s.mockArchiverProvider.AssertExpectations(s.T())
}

func (s *transferQueueActiveProcessorSuite) TestProcessActivityTask_Success() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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
	event, ai := addActivityTaskScheduledEvent(msBuilder, event.GetEventId(), activityID, activityType, taskListName, []byte{}, 1, 1, 1)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", s.version).Return(s.mockClusterMetadata.GetCurrentClusterName())
	msBuilder.UpdateReplicationStateLastEventID(s.version, event.GetEventId())

	transferTask := &persistence.TransferTaskInfo{
		Version:        s.version,
		DomainID:       s.domainID,
		TargetDomainID: testTargetDomainID,
		WorkflowID:     execution.GetWorkflowId(),
		RunID:          execution.GetRunId(),
		TaskID:         taskID,
		TaskList:       taskListName,
		TaskType:       persistence.TransferTaskTypeActivityTask,
		ScheduleID:     event.GetEventId(),
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.EXPECT().AddActivityTask(nil, s.createAddActivityTaskRequest(transferTask, ai)).Return(nil).Times(1)

	_, err = s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessActivityTask_Duplication() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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
	event, ai := addActivityTaskScheduledEvent(msBuilder, event.GetEventId(), activityID, activityType, taskListName, []byte{}, 1, 1, 1)

	transferTask := &persistence.TransferTaskInfo{
		Version:        s.version,
		DomainID:       s.domainID,
		TargetDomainID: testTargetDomainID,
		WorkflowID:     execution.GetWorkflowId(),
		RunID:          execution.GetRunId(),
		TaskID:         taskID,
		TaskList:       taskListName,
		TaskType:       persistence.TransferTaskTypeActivityTask,
		ScheduleID:     event.GetEventId(),
	}

	event = addActivityTaskStartedEvent(msBuilder, event.GetEventId(), "")
	ai.StartedID = event.GetEventId()
	event = addActivityTaskCompletedEvent(msBuilder, ai.ScheduleID, ai.StartedID, nil, "")
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", s.version).Return(s.mockClusterMetadata.GetCurrentClusterName())
	msBuilder.UpdateReplicationStateLastEventID(s.version, event.GetEventId())

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	_, err = s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessDecisionTask_FirstDecision() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", s.version).Return(s.mockClusterMetadata.GetCurrentClusterName())
	msBuilder.UpdateReplicationStateLastEventID(s.version, di.ScheduleID)

	transferTask := &persistence.TransferTaskInfo{
		Version:    s.version,
		DomainID:   s.domainID,
		WorkflowID: execution.GetWorkflowId(),
		RunID:      execution.GetRunId(),
		TaskID:     taskID,
		TaskList:   taskListName,
		TaskType:   persistence.TransferTaskTypeDecisionTask,
		ScheduleID: di.ScheduleID,
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.EXPECT().AddDecisionTask(nil, s.createAddDecisionTaskRequest(transferTask, msBuilder)).Return(nil).Times(1)

	_, err = s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessDecisionTask_NonFirstDecision() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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

	// make another round of decision
	taskID := int64(59)
	di = addDecisionTaskScheduledEvent(msBuilder)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", s.version).Return(s.mockClusterMetadata.GetCurrentClusterName())
	msBuilder.UpdateReplicationStateLastEventID(s.version, di.ScheduleID)

	transferTask := &persistence.TransferTaskInfo{
		Version:    s.version,
		DomainID:   s.domainID,
		WorkflowID: execution.GetWorkflowId(),
		RunID:      execution.GetRunId(),
		TaskID:     taskID,
		TaskList:   taskListName,
		TaskType:   persistence.TransferTaskTypeDecisionTask,
		ScheduleID: di.ScheduleID,
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.EXPECT().AddDecisionTask(nil, s.createAddDecisionTaskRequest(transferTask, msBuilder)).Return(nil).Times(1)

	_, err = s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessDecisionTask_Sticky_NonFirstDecision() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"
	stickyTaskListName := "some random sticky task list"
	stickyTaskListTimeout := int32(233)

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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
	// set the sticky tasklist attr
	executionInfo := msBuilder.GetExecutionInfo()
	executionInfo.StickyTaskList = stickyTaskListName
	executionInfo.StickyScheduleToStartTimeout = stickyTaskListTimeout

	// make another round of decision
	taskID := int64(59)
	di = addDecisionTaskScheduledEvent(msBuilder)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", s.version).Return(s.mockClusterMetadata.GetCurrentClusterName())
	msBuilder.UpdateReplicationStateLastEventID(s.version, di.ScheduleID)

	transferTask := &persistence.TransferTaskInfo{
		Version:    s.version,
		DomainID:   s.domainID,
		WorkflowID: execution.GetWorkflowId(),
		RunID:      execution.GetRunId(),
		TaskID:     taskID,
		TaskList:   stickyTaskListName,
		TaskType:   persistence.TransferTaskTypeDecisionTask,
		ScheduleID: di.ScheduleID,
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.EXPECT().AddDecisionTask(nil, s.createAddDecisionTaskRequest(transferTask, msBuilder)).Return(nil).Times(1)

	_, err = s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessDecisionTask_DecisionNotSticky_MutableStateSticky() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"
	stickyTaskListName := "some random sticky task list"
	stickyTaskListTimeout := int32(233)

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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
	// set the sticky tasklist attr
	executionInfo := msBuilder.GetExecutionInfo()
	executionInfo.StickyTaskList = stickyTaskListName
	executionInfo.StickyScheduleToStartTimeout = stickyTaskListTimeout

	// make another round of decision
	taskID := int64(59)
	di = addDecisionTaskScheduledEvent(msBuilder)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", s.version).Return(s.mockClusterMetadata.GetCurrentClusterName())
	msBuilder.UpdateReplicationStateLastEventID(s.version, di.ScheduleID)

	transferTask := &persistence.TransferTaskInfo{
		Version:    s.version,
		DomainID:   s.domainID,
		WorkflowID: execution.GetWorkflowId(),
		RunID:      execution.GetRunId(),
		TaskID:     taskID,
		TaskList:   taskListName,
		TaskType:   persistence.TransferTaskTypeDecisionTask,
		ScheduleID: di.ScheduleID,
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.EXPECT().AddDecisionTask(nil, s.createAddDecisionTaskRequest(transferTask, msBuilder)).Return(nil).Times(1)

	_, err = s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessDecisionTask_Duplication() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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

	taskID := int64(4096)
	di := addDecisionTaskScheduledEvent(msBuilder)
	event := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, di.StartedID, nil, "some random identity")
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", s.version).Return(s.mockClusterMetadata.GetCurrentClusterName())
	msBuilder.UpdateReplicationStateLastEventID(s.version, event.GetEventId())

	transferTask := &persistence.TransferTaskInfo{
		Version:    s.version,
		DomainID:   s.domainID,
		WorkflowID: execution.GetWorkflowId(),
		RunID:      execution.GetRunId(),
		TaskID:     taskID,
		TaskList:   taskListName,
		TaskType:   persistence.TransferTaskTypeDecisionTask,
		ScheduleID: di.ScheduleID,
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	_, err = s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessCloseExecution_HasParent() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	parentDomainID := "some random parent domain ID"
	parentInitiatedID := int64(3222)
	parentDomainName := "some random parent domain Name"
	parentExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random parent workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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
			ParentExecutionInfo: &history.ParentExecutionInfo{
				DomainUUID:  common.StringPtr(parentDomainID),
				Domain:      common.StringPtr(parentDomainName),
				Execution:   &parentExecution,
				InitiatedId: common.Int64Ptr(parentInitiatedID),
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
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", s.version).Return(s.mockClusterMetadata.GetCurrentClusterName())
	msBuilder.UpdateReplicationStateLastEventID(s.version, event.GetEventId())

	transferTask := &persistence.TransferTaskInfo{
		Version:    s.version,
		DomainID:   s.domainID,
		WorkflowID: execution.GetWorkflowId(),
		RunID:      execution.GetRunId(),
		TaskID:     taskID,
		TaskList:   taskListName,
		TaskType:   persistence.TransferTaskTypeCloseExecution,
		ScheduleID: event.GetEventId(),
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().RecordChildExecutionCompleted(nil, &history.RecordChildExecutionCompletedRequest{
		DomainUUID:         common.StringPtr(parentDomainID),
		WorkflowExecution:  &parentExecution,
		InitiatedId:        common.Int64Ptr(parentInitiatedID),
		CompletedExecution: &execution,
		CompletionEvent:    event,
	}).Return(nil).Times(1)
	s.mockVisibilityMgr.On("RecordWorkflowExecutionClosed", mock.Anything).Return(nil).Once()

	_, err = s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessCloseExecution_NoParent() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", s.version).Return(s.mockClusterMetadata.GetCurrentClusterName())
	msBuilder.UpdateReplicationStateLastEventID(s.version, event.GetEventId())

	transferTask := &persistence.TransferTaskInfo{
		Version:    s.version,
		DomainID:   s.domainID,
		WorkflowID: execution.GetWorkflowId(),
		RunID:      execution.GetRunId(),
		TaskID:     taskID,
		TaskList:   taskListName,
		TaskType:   persistence.TransferTaskTypeCloseExecution,
		ScheduleID: event.GetEventId(),
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockVisibilityMgr.On("RecordWorkflowExecutionClosed", mock.Anything).Return(nil).Once()

	_, err = s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessCloseExecution_NoParent_HasFewChildren() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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

	dt := workflow.DecisionTypeStartChildWorkflowExecution
	parentClosePolicy1 := workflow.ParentClosePolicyAbandon
	parentClosePolicy2 := workflow.ParentClosePolicyTerminate
	parentClosePolicy3 := workflow.ParentClosePolicyRequestCancel

	event, _ = msBuilder.AddDecisionTaskCompletedEvent(di.ScheduleID, di.StartedID, &workflow.RespondDecisionTaskCompletedRequest{
		ExecutionContext: nil,
		Identity:         common.StringPtr("some random identity"),
		Decisions: []*workflow.Decision{
			&workflow.Decision{
				DecisionType: &dt,
				StartChildWorkflowExecutionDecisionAttributes: &workflow.StartChildWorkflowExecutionDecisionAttributes{
					WorkflowId: common.StringPtr("child workflow1"),
					WorkflowType: &workflow.WorkflowType{
						Name: common.StringPtr("child workflow type"),
					},
					TaskList:          &workflow.TaskList{Name: common.StringPtr(taskListName)},
					Input:             []byte("random input"),
					ParentClosePolicy: &parentClosePolicy1,
				},
			},
			&workflow.Decision{
				DecisionType: &dt,
				StartChildWorkflowExecutionDecisionAttributes: &workflow.StartChildWorkflowExecutionDecisionAttributes{
					WorkflowId: common.StringPtr("child workflow2"),
					WorkflowType: &workflow.WorkflowType{
						Name: common.StringPtr("child workflow type"),
					},
					TaskList:          &workflow.TaskList{Name: common.StringPtr(taskListName)},
					Input:             []byte("random input"),
					ParentClosePolicy: &parentClosePolicy2,
				},
			},
			&workflow.Decision{
				DecisionType: &dt,
				StartChildWorkflowExecutionDecisionAttributes: &workflow.StartChildWorkflowExecutionDecisionAttributes{
					WorkflowId: common.StringPtr("child workflow3"),
					WorkflowType: &workflow.WorkflowType{
						Name: common.StringPtr("child workflow type"),
					},
					TaskList:          &workflow.TaskList{Name: common.StringPtr(taskListName)},
					Input:             []byte("random input"),
					ParentClosePolicy: &parentClosePolicy3,
				},
			},
		},
	}, defaultHistoryMaxAutoResetPoints)

	_, _, err = msBuilder.AddStartChildWorkflowExecutionInitiatedEvent(event.GetEventId(), uuid.New(), &workflow.StartChildWorkflowExecutionDecisionAttributes{
		WorkflowId: common.StringPtr("child workflow1"),
		WorkflowType: &workflow.WorkflowType{
			Name: common.StringPtr("child workflow type"),
		},
		TaskList:          &workflow.TaskList{Name: common.StringPtr(taskListName)},
		Input:             []byte("random input"),
		ParentClosePolicy: &parentClosePolicy1,
	})
	s.Nil(err)
	_, _, err = msBuilder.AddStartChildWorkflowExecutionInitiatedEvent(event.GetEventId(), uuid.New(), &workflow.StartChildWorkflowExecutionDecisionAttributes{
		WorkflowId: common.StringPtr("child workflow2"),
		WorkflowType: &workflow.WorkflowType{
			Name: common.StringPtr("child workflow type"),
		},
		TaskList:          &workflow.TaskList{Name: common.StringPtr(taskListName)},
		Input:             []byte("random input"),
		ParentClosePolicy: &parentClosePolicy2,
	})
	s.Nil(err)
	_, _, err = msBuilder.AddStartChildWorkflowExecutionInitiatedEvent(event.GetEventId(), uuid.New(), &workflow.StartChildWorkflowExecutionDecisionAttributes{
		WorkflowId: common.StringPtr("child workflow3"),
		WorkflowType: &workflow.WorkflowType{
			Name: common.StringPtr("child workflow type"),
		},
		TaskList:          &workflow.TaskList{Name: common.StringPtr(taskListName)},
		Input:             []byte("random input"),
		ParentClosePolicy: &parentClosePolicy3,
	})
	s.Nil(err)

	msBuilder.FlushBufferedEvents()

	taskID := int64(59)
	event = addCompleteWorkflowEvent(msBuilder, event.GetEventId(), nil)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", s.version).Return(s.mockClusterMetadata.GetCurrentClusterName())
	msBuilder.UpdateReplicationStateLastEventID(s.version, event.GetEventId())

	transferTask := &persistence.TransferTaskInfo{
		Version:    s.version,
		DomainID:   s.domainID,
		WorkflowID: execution.GetWorkflowId(),
		RunID:      execution.GetRunId(),
		TaskID:     taskID,
		TaskList:   taskListName,
		TaskType:   persistence.TransferTaskTypeCloseExecution,
		ScheduleID: event.GetEventId(),
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockVisibilityMgr.On("RecordWorkflowExecutionClosed", mock.Anything).Return(nil).Once()
	s.mockHistoryClient.EXPECT().RequestCancelWorkflowExecution(nil, gomock.Any()).Return(nil).Times(1)
	s.mockHistoryClient.EXPECT().TerminateWorkflowExecution(nil, gomock.Any()).Return(nil).Times(1)

	_, err = s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessCloseExecution_NoParent_HasManyChildren() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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

	dt := workflow.DecisionTypeStartChildWorkflowExecution
	parentClosePolicy := workflow.ParentClosePolicyTerminate
	decisions := []*workflow.Decision{}
	for i := 0; i < 10; i++ {
		decisions = append(decisions, &workflow.Decision{
			DecisionType: &dt,
			StartChildWorkflowExecutionDecisionAttributes: &workflow.StartChildWorkflowExecutionDecisionAttributes{
				WorkflowId: common.StringPtr("child workflow" + string(i)),
				WorkflowType: &workflow.WorkflowType{
					Name: common.StringPtr("child workflow type"),
				},
				TaskList:          &workflow.TaskList{Name: common.StringPtr(taskListName)},
				Input:             []byte("random input"),
				ParentClosePolicy: &parentClosePolicy,
			},
		})
	}

	event, _ = msBuilder.AddDecisionTaskCompletedEvent(di.ScheduleID, di.StartedID, &workflow.RespondDecisionTaskCompletedRequest{
		ExecutionContext: nil,
		Identity:         common.StringPtr("some random identity"),
		Decisions:        decisions,
	}, defaultHistoryMaxAutoResetPoints)

	for i := 0; i < 10; i++ {
		_, _, err = msBuilder.AddStartChildWorkflowExecutionInitiatedEvent(event.GetEventId(), uuid.New(), &workflow.StartChildWorkflowExecutionDecisionAttributes{
			WorkflowId: common.StringPtr("child workflow" + string(i)),
			WorkflowType: &workflow.WorkflowType{
				Name: common.StringPtr("child workflow type"),
			},
			TaskList:          &workflow.TaskList{Name: common.StringPtr(taskListName)},
			Input:             []byte("random input"),
			ParentClosePolicy: &parentClosePolicy,
		})
		s.Nil(err)
	}

	msBuilder.FlushBufferedEvents()

	taskID := int64(59)
	event = addCompleteWorkflowEvent(msBuilder, event.GetEventId(), nil)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", s.version).Return(s.mockClusterMetadata.GetCurrentClusterName())
	msBuilder.UpdateReplicationStateLastEventID(s.version, event.GetEventId())

	transferTask := &persistence.TransferTaskInfo{
		Version:    s.version,
		DomainID:   s.domainID,
		WorkflowID: execution.GetWorkflowId(),
		RunID:      execution.GetRunId(),
		TaskID:     taskID,
		TaskList:   taskListName,
		TaskType:   persistence.TransferTaskTypeCloseExecution,
		ScheduleID: event.GetEventId(),
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockVisibilityMgr.On("RecordWorkflowExecutionClosed", mock.Anything).Return(nil).Once()
	s.mockParentClosePolicyClient.On("SendParentClosePolicyRequest", mock.Anything).Return(nil).Times(1)

	_, err = s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessCancelExecution_Success() {

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

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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
	event, rci := addRequestCancelInitiatedEvent(msBuilder, event.GetEventId(), uuid.New(), testTargetDomainName, targetExecution.GetWorkflowId(), targetExecution.GetRunId())

	transferTask := &persistence.TransferTaskInfo{
		Version:          s.version,
		DomainID:         s.domainID,
		WorkflowID:       execution.GetWorkflowId(),
		RunID:            execution.GetRunId(),
		TargetDomainID:   testTargetDomainID,
		TargetWorkflowID: targetExecution.GetWorkflowId(),
		TargetRunID:      targetExecution.GetRunId(),
		TaskID:           taskID,
		TaskList:         taskListName,
		TaskType:         persistence.TransferTaskTypeCancelExecution,
		ScheduleID:       event.GetEventId(),
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().RequestCancelWorkflowExecution(nil, s.createRequetCancelWorkflowExecutionRequest(transferTask, rci)).Return(nil).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", s.version).Return(cluster.TestCurrentClusterName)

	_, err = s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessCancelExecution_Failure() {

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

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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
	event, rci := addRequestCancelInitiatedEvent(msBuilder, event.GetEventId(), uuid.New(), testTargetDomainName, targetExecution.GetWorkflowId(), targetExecution.GetRunId())
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", s.version).Return(s.mockClusterMetadata.GetCurrentClusterName())
	msBuilder.UpdateReplicationStateLastEventID(s.version, event.GetEventId())

	transferTask := &persistence.TransferTaskInfo{
		Version:          s.version,
		DomainID:         s.domainID,
		WorkflowID:       execution.GetWorkflowId(),
		RunID:            execution.GetRunId(),
		TargetDomainID:   testTargetDomainID,
		TargetWorkflowID: targetExecution.GetWorkflowId(),
		TargetRunID:      targetExecution.GetRunId(),
		TaskID:           taskID,
		TaskList:         taskListName,
		TaskType:         persistence.TransferTaskTypeCancelExecution,
		ScheduleID:       event.GetEventId(),
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().RequestCancelWorkflowExecution(nil, s.createRequetCancelWorkflowExecutionRequest(transferTask, rci)).Return(&workflow.EntityNotExistsError{}).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", s.version).Return(cluster.TestCurrentClusterName)

	_, err = s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessCancelExecution_Duplication() {

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

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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
		Version:          s.version,
		DomainID:         s.domainID,
		WorkflowID:       execution.GetWorkflowId(),
		RunID:            execution.GetRunId(),
		TargetDomainID:   testTargetDomainID,
		TargetWorkflowID: targetExecution.GetWorkflowId(),
		TargetRunID:      targetExecution.GetRunId(),
		TaskID:           taskID,
		TaskList:         taskListName,
		TaskType:         persistence.TransferTaskTypeCancelExecution,
		ScheduleID:       event.GetEventId(),
	}

	event = addCancelRequestedEvent(msBuilder, event.GetEventId(), testTargetDomainID, targetExecution.GetWorkflowId(), targetExecution.GetRunId())
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", s.version).Return(s.mockClusterMetadata.GetCurrentClusterName())
	msBuilder.UpdateReplicationStateLastEventID(s.version, event.GetEventId())

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	_, err = s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessSignalExecution_Success() {

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
	signalInput := []byte("some random signal input")
	signalControl := []byte("some random signal control")

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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
	event, si := addRequestSignalInitiatedEvent(msBuilder, event.GetEventId(), uuid.New(),
		testTargetDomainName, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), signalName, signalInput, signalControl)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", s.version).Return(s.mockClusterMetadata.GetCurrentClusterName())
	msBuilder.UpdateReplicationStateLastEventID(s.version, event.GetEventId())

	transferTask := &persistence.TransferTaskInfo{
		Version:          s.version,
		DomainID:         s.domainID,
		WorkflowID:       execution.GetWorkflowId(),
		RunID:            execution.GetRunId(),
		TargetDomainID:   testTargetDomainID,
		TargetWorkflowID: targetExecution.GetWorkflowId(),
		TargetRunID:      targetExecution.GetRunId(),
		TaskID:           taskID,
		TaskList:         taskListName,
		TaskType:         persistence.TransferTaskTypeSignalExecution,
		ScheduleID:       event.GetEventId(),
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().SignalWorkflowExecution(nil, s.createSignalWorkflowExecutionRequest(transferTask, si)).Return(nil).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", s.version).Return(cluster.TestCurrentClusterName)

	s.mockHistoryClient.EXPECT().RemoveSignalMutableState(nil, &history.RemoveSignalMutableStateRequest{
		DomainUUID: common.StringPtr(transferTask.TargetDomainID),
		WorkflowExecution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(transferTask.TargetWorkflowID),
			RunId:      common.StringPtr(transferTask.TargetRunID),
		},
		RequestId: common.StringPtr(si.SignalRequestID),
	}).Return(nil).Times(1)

	_, err = s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessSignalExecution_Failure() {

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
	signalInput := []byte("some random signal input")
	signalControl := []byte("some random signal control")

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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
	event, si := addRequestSignalInitiatedEvent(msBuilder, event.GetEventId(), uuid.New(),
		testTargetDomainName, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), signalName, signalInput, signalControl)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", s.version).Return(s.mockClusterMetadata.GetCurrentClusterName())
	msBuilder.UpdateReplicationStateLastEventID(s.version, event.GetEventId())

	transferTask := &persistence.TransferTaskInfo{
		Version:          s.version,
		DomainID:         s.domainID,
		WorkflowID:       execution.GetWorkflowId(),
		RunID:            execution.GetRunId(),
		TargetDomainID:   testTargetDomainID,
		TargetWorkflowID: targetExecution.GetWorkflowId(),
		TargetRunID:      targetExecution.GetRunId(),
		TaskID:           taskID,
		TaskList:         taskListName,
		TaskType:         persistence.TransferTaskTypeSignalExecution,
		ScheduleID:       event.GetEventId(),
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().SignalWorkflowExecution(nil, s.createSignalWorkflowExecutionRequest(transferTask, si)).Return(&workflow.EntityNotExistsError{}).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", s.version).Return(cluster.TestCurrentClusterName)

	_, err = s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessSignalExecution_Duplication() {

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
	signalInput := []byte("some random signal input")
	signalControl := []byte("some random signal control")

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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
		testTargetDomainName, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), signalName, signalInput, signalControl)

	transferTask := &persistence.TransferTaskInfo{
		Version:          s.version,
		DomainID:         s.domainID,
		WorkflowID:       execution.GetWorkflowId(),
		RunID:            execution.GetRunId(),
		TargetDomainID:   testTargetDomainID,
		TargetWorkflowID: targetExecution.GetWorkflowId(),
		TargetRunID:      targetExecution.GetRunId(),
		TaskID:           taskID,
		TaskList:         taskListName,
		TaskType:         persistence.TransferTaskTypeSignalExecution,
		ScheduleID:       event.GetEventId(),
	}

	event = addSignaledEvent(msBuilder, event.GetEventId(), testTargetDomainName, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), nil)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", s.version).Return(s.mockClusterMetadata.GetCurrentClusterName())
	msBuilder.UpdateReplicationStateLastEventID(s.version, event.GetEventId())

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	_, err = s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessStartChildExecution_Success() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	childWorkflowID := "some random child workflow ID"
	childRunID := uuid.New()
	childWorkflowType := "some random child workflow type"
	childTaskListName := "some random child task list"

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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

	event, ci := addStartChildWorkflowExecutionInitiatedEvent(msBuilder, event.GetEventId(), uuid.New(),
		testChildDomainName, childWorkflowID, childWorkflowType, childTaskListName, nil, 1, 1)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", s.version).Return(s.mockClusterMetadata.GetCurrentClusterName())
	msBuilder.UpdateReplicationStateLastEventID(s.version, event.GetEventId())

	transferTask := &persistence.TransferTaskInfo{
		Version:          s.version,
		DomainID:         s.domainID,
		WorkflowID:       execution.GetWorkflowId(),
		RunID:            execution.GetRunId(),
		TargetDomainID:   testChildDomainID,
		TargetWorkflowID: childWorkflowID,
		TargetRunID:      "",
		TaskID:           taskID,
		TaskList:         taskListName,
		TaskType:         persistence.TransferTaskTypeStartChildExecution,
		ScheduleID:       event.GetEventId(),
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().StartWorkflowExecution(nil, s.createChildWorkflowExecutionRequest(
		transferTask,
		msBuilder,
		ci,
		testDomainName,
		testChildDomainName,
	)).Return(&workflow.StartWorkflowExecutionResponse{RunId: common.StringPtr(childRunID)}, nil).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", s.version).Return(cluster.TestCurrentClusterName)
	s.mockHistoryClient.EXPECT().ScheduleDecisionTask(nil, &history.ScheduleDecisionTaskRequest{
		DomainUUID: common.StringPtr(testChildDomainID),
		WorkflowExecution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(childWorkflowID),
			RunId:      common.StringPtr(childRunID),
		},
		IsFirstDecision: common.BoolPtr(true),
	}).Return(nil).Times(1)

	_, err = s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessStartChildExecution_Failure() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	childWorkflowID := "some random child workflow ID"
	childWorkflowType := "some random child workflow type"
	childTaskListName := "some random child task list"

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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

	event, ci := addStartChildWorkflowExecutionInitiatedEvent(msBuilder, event.GetEventId(), uuid.New(),
		testChildDomainName, childWorkflowID, childWorkflowType, childTaskListName, nil, 1, 1)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", s.version).Return(s.mockClusterMetadata.GetCurrentClusterName())
	msBuilder.UpdateReplicationStateLastEventID(s.version, event.GetEventId())

	transferTask := &persistence.TransferTaskInfo{
		Version:          s.version,
		DomainID:         s.domainID,
		WorkflowID:       execution.GetWorkflowId(),
		RunID:            execution.GetRunId(),
		TargetDomainID:   testChildDomainID,
		TargetWorkflowID: childWorkflowID,
		TargetRunID:      "",
		TaskID:           taskID,
		TaskList:         taskListName,
		TaskType:         persistence.TransferTaskTypeStartChildExecution,
		ScheduleID:       event.GetEventId(),
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().StartWorkflowExecution(nil, s.createChildWorkflowExecutionRequest(
		transferTask,
		msBuilder,
		ci,
		testDomainName,
		testChildDomainName,
	)).Return(nil, &workflow.WorkflowExecutionAlreadyStartedError{}).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", s.version).Return(cluster.TestCurrentClusterName)

	_, err = s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessStartChildExecution_Success_Dup() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	childWorkflowID := "some random child workflow ID"
	childRunID := uuid.New()
	childWorkflowType := "some random child workflow type"
	childTaskListName := "some random child task list"

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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

	event, ci := addStartChildWorkflowExecutionInitiatedEvent(msBuilder, event.GetEventId(), uuid.New(),
		testChildDomainName, childWorkflowID, childWorkflowType, childTaskListName, nil, 1, 1)

	transferTask := &persistence.TransferTaskInfo{
		Version:          s.version,
		DomainID:         s.domainID,
		WorkflowID:       execution.GetWorkflowId(),
		RunID:            execution.GetRunId(),
		TargetDomainID:   testChildDomainID,
		TargetWorkflowID: childWorkflowID,
		TargetRunID:      "",
		TaskID:           taskID,
		TaskList:         taskListName,
		TaskType:         persistence.TransferTaskTypeStartChildExecution,
		ScheduleID:       event.GetEventId(),
	}

	event = addChildWorkflowExecutionStartedEvent(msBuilder, event.GetEventId(), testChildDomainID, childWorkflowID, childRunID, childWorkflowType)
	ci.StartedID = event.GetEventId()
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", s.version).Return(s.mockClusterMetadata.GetCurrentClusterName())
	msBuilder.UpdateReplicationStateLastEventID(s.version, event.GetEventId())

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().ScheduleDecisionTask(nil, &history.ScheduleDecisionTaskRequest{
		DomainUUID: common.StringPtr(testChildDomainID),
		WorkflowExecution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(childWorkflowID),
			RunId:      common.StringPtr(childRunID),
		},
		IsFirstDecision: common.BoolPtr(true),
	}).Return(nil).Times(1)

	_, err = s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessStartChildExecution_Duplication() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	childExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random child workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	childWorkflowType := "some random child workflow type"
	childTaskListName := "some random child task list"

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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

	event, ci := addStartChildWorkflowExecutionInitiatedEvent(msBuilder, event.GetEventId(), uuid.New(),
		testChildDomainName, childExecution.GetWorkflowId(), childWorkflowType, childTaskListName, nil, 1, 1)

	transferTask := &persistence.TransferTaskInfo{
		Version:          s.version,
		DomainID:         s.domainID,
		WorkflowID:       execution.GetWorkflowId(),
		RunID:            execution.GetRunId(),
		TargetDomainID:   testChildDomainID,
		TargetWorkflowID: childExecution.GetWorkflowId(),
		TargetRunID:      "",
		TaskID:           taskID,
		TaskList:         taskListName,
		TaskType:         persistence.TransferTaskTypeStartChildExecution,
		ScheduleID:       event.GetEventId(),
	}

	event = addChildWorkflowExecutionStartedEvent(msBuilder, event.GetEventId(), testChildDomainID, childExecution.GetWorkflowId(), childExecution.GetRunId(), childWorkflowType)
	ci.StartedID = event.GetEventId()
	event = addChildWorkflowExecutionCompletedEvent(msBuilder, ci.InitiatedID, &childExecution, &workflow.WorkflowExecutionCompletedEventAttributes{
		Result:                       []byte("some random child workflow execution result"),
		DecisionTaskCompletedEventId: common.Int64Ptr(transferTask.ScheduleID),
	})
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", s.version).Return(s.mockClusterMetadata.GetCurrentClusterName())
	msBuilder.UpdateReplicationStateLastEventID(s.version, event.GetEventId())

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	_, err = s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessRecordWorkflowStartedTask() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"
	cronSchedule := "@every 5s"
	backoffSeconds := int32(5)

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())

	event, err := msBuilder.AddWorkflowExecutionStartedEvent(
		execution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(s.domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
				CronSchedule:                        common.StringPtr(cronSchedule),
			},
			FirstDecisionTaskBackoffSeconds: common.Int32Ptr(backoffSeconds),
		},
	)
	s.Nil(err)

	taskID := int64(59)
	di := addDecisionTaskScheduledEvent(msBuilder)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", s.version).Return(s.mockClusterMetadata.GetCurrentClusterName())
	msBuilder.UpdateReplicationStateLastEventID(di.Version, di.ScheduleID)

	transferTask := &persistence.TransferTaskInfo{
		Version:    s.version,
		DomainID:   s.domainID,
		WorkflowID: execution.GetWorkflowId(),
		RunID:      execution.GetRunId(),
		TaskID:     taskID,
		TaskList:   taskListName,
		TaskType:   persistence.TransferTaskTypeRecordWorkflowStarted,
		ScheduleID: event.GetEventId(),
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockVisibilityMgr.On("RecordWorkflowExecutionStarted", s.createRecordWorkflowExecutionStartedRequest(transferTask, msBuilder, backoffSeconds)).Once().Return(nil)

	_, err = s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessUpsertWorkflowSearchAttributes() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())

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
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", s.version).Return(s.mockClusterMetadata.GetCurrentClusterName())
	msBuilder.UpdateReplicationStateLastEventID(di.Version, di.ScheduleID)

	transferTask := &persistence.TransferTaskInfo{
		Version:    s.version,
		DomainID:   s.domainID,
		WorkflowID: execution.GetWorkflowId(),
		RunID:      execution.GetRunId(),
		TaskID:     taskID,
		TaskList:   taskListName,
		TaskType:   persistence.TransferTaskTypeUpsertWorkflowSearchAttributes,
		ScheduleID: event.GetEventId(),
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockVisibilityMgr.On("UpsertWorkflowExecution", s.createUpsertWorkflowSearchAttributesRequest(transferTask, msBuilder)).Once().Return(nil)

	_, err = s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestCopySearchAttributes() {
	var input map[string][]byte
	s.Nil(copySearchAttributes(input))

	key := "key"
	val := []byte{'1', '2', '3'}
	input = map[string][]byte{
		key: val,
	}
	result := copySearchAttributes(input)
	s.Equal(input, result)
	result[key][0] = '0'
	s.Equal(byte('1'), val[0])
}

func (s *transferQueueActiveProcessorSuite) createAddActivityTaskRequest(task *persistence.TransferTaskInfo,
	ai *persistence.ActivityInfo) *matching.AddActivityTaskRequest {
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(task.WorkflowID),
		RunId:      common.StringPtr(task.RunID),
	}
	taskList := &workflow.TaskList{Name: &task.TaskList}

	return &matching.AddActivityTaskRequest{
		DomainUUID:                    common.StringPtr(task.TargetDomainID),
		SourceDomainUUID:              common.StringPtr(task.DomainID),
		Execution:                     &execution,
		TaskList:                      taskList,
		ScheduleId:                    &task.ScheduleID,
		ScheduleToStartTimeoutSeconds: common.Int32Ptr(ai.ScheduleToStartTimeout),
	}
}

func (s *transferQueueActiveProcessorSuite) createAddDecisionTaskRequest(task *persistence.TransferTaskInfo,
	msBuilder mutableState) *matching.AddDecisionTaskRequest {
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(task.WorkflowID),
		RunId:      common.StringPtr(task.RunID),
	}
	taskList := &workflow.TaskList{Name: &task.TaskList}
	executionInfo := msBuilder.GetExecutionInfo()
	timeout := executionInfo.WorkflowTimeout
	if msBuilder.GetExecutionInfo().TaskList != task.TaskList {
		taskList.Kind = common.TaskListKindPtr(workflow.TaskListKindSticky)
		timeout = executionInfo.StickyScheduleToStartTimeout
	}

	return &matching.AddDecisionTaskRequest{
		DomainUUID:                    common.StringPtr(task.DomainID),
		Execution:                     &execution,
		TaskList:                      taskList,
		ScheduleId:                    common.Int64Ptr(task.ScheduleID),
		ScheduleToStartTimeoutSeconds: common.Int32Ptr(timeout),
	}
}

func (s *transferQueueActiveProcessorSuite) createRecordWorkflowExecutionStartedRequest(task *persistence.TransferTaskInfo,
	msBuilder mutableState, backoffSeconds int32) *persistence.RecordWorkflowExecutionStartedRequest {
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(task.WorkflowID),
		RunId:      common.StringPtr(task.RunID),
	}
	executionInfo := msBuilder.GetExecutionInfo()
	executionTimestamp := executionInfo.StartTimestamp.Add(time.Duration(backoffSeconds) * time.Second)
	domainEntry, err := s.mockDomainCache.GetDomainByID(task.DomainID)
	s.NoError(err)

	return &persistence.RecordWorkflowExecutionStartedRequest{
		DomainUUID:         domainEntry.GetInfo().ID,
		Domain:             domainEntry.GetInfo().Name,
		Execution:          execution,
		WorkflowTypeName:   executionInfo.WorkflowTypeName,
		StartTimestamp:     executionInfo.StartTimestamp.UnixNano(),
		ExecutionTimestamp: executionTimestamp.UnixNano(),
		WorkflowTimeout:    int64(executionInfo.WorkflowTimeout),
		TaskID:             task.TaskID,
	}
}

func (s *transferQueueActiveProcessorSuite) createRequetCancelWorkflowExecutionRequest(task *persistence.TransferTaskInfo,
	rci *persistence.RequestCancelInfo) *history.RequestCancelWorkflowExecutionRequest {
	sourceExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(task.WorkflowID),
		RunId:      common.StringPtr(task.RunID),
	}
	targetExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(task.TargetWorkflowID),
		RunId:      common.StringPtr(task.TargetRunID),
	}

	return &history.RequestCancelWorkflowExecutionRequest{
		DomainUUID: common.StringPtr(task.TargetDomainID),
		CancelRequest: &workflow.RequestCancelWorkflowExecutionRequest{
			Domain:            common.StringPtr(task.TargetDomainID),
			WorkflowExecution: &targetExecution,
			Identity:          common.StringPtr(identityHistoryService),
			// Use the same request ID to dedupe RequestCancelWorkflowExecution calls
			RequestId: common.StringPtr(rci.CancelRequestID),
		},
		ExternalInitiatedEventId:  common.Int64Ptr(task.ScheduleID),
		ExternalWorkflowExecution: &sourceExecution,
		ChildWorkflowOnly:         common.BoolPtr(task.TargetChildWorkflowOnly),
	}
}

func (s *transferQueueActiveProcessorSuite) createSignalWorkflowExecutionRequest(task *persistence.TransferTaskInfo,
	si *persistence.SignalInfo) *history.SignalWorkflowExecutionRequest {

	sourceExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(task.WorkflowID),
		RunId:      common.StringPtr(task.RunID),
	}
	targetExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(task.TargetWorkflowID),
		RunId:      common.StringPtr(task.TargetRunID),
	}

	return &history.SignalWorkflowExecutionRequest{
		DomainUUID: common.StringPtr(task.TargetDomainID),
		SignalRequest: &workflow.SignalWorkflowExecutionRequest{
			Domain:            common.StringPtr(task.TargetDomainID),
			WorkflowExecution: &targetExecution,
			Identity:          common.StringPtr(identityHistoryService),
			SignalName:        common.StringPtr(si.SignalName),
			Input:             si.Input,
			RequestId:         common.StringPtr(si.SignalRequestID),
			Control:           si.Control,
		},
		ExternalWorkflowExecution: &sourceExecution,
		ChildWorkflowOnly:         common.BoolPtr(task.TargetChildWorkflowOnly),
	}
}

func (s *transferQueueActiveProcessorSuite) createChildWorkflowExecutionRequest(task *persistence.TransferTaskInfo,
	msBuilder mutableState, ci *persistence.ChildExecutionInfo, testDomainName string, targetDomainName string) *history.StartWorkflowExecutionRequest {

	event, err := msBuilder.GetChildExecutionInitiatedEvent(task.ScheduleID)
	if err != nil {
		s.Fail("Cannot find corresponding child workflow info with scheduled ID %v.", task.ScheduleID)
	}
	attributes := event.StartChildWorkflowExecutionInitiatedEventAttributes
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(task.WorkflowID),
		RunId:      common.StringPtr(task.RunID),
	}
	now := time.Now()
	return &history.StartWorkflowExecutionRequest{
		DomainUUID: common.StringPtr(task.TargetDomainID),
		StartRequest: &workflow.StartWorkflowExecutionRequest{
			Domain:                              common.StringPtr(targetDomainName),
			WorkflowId:                          attributes.WorkflowId,
			WorkflowType:                        attributes.WorkflowType,
			TaskList:                            attributes.TaskList,
			Input:                               attributes.Input,
			ExecutionStartToCloseTimeoutSeconds: attributes.ExecutionStartToCloseTimeoutSeconds,
			TaskStartToCloseTimeoutSeconds:      attributes.TaskStartToCloseTimeoutSeconds,
			// Use the same request ID to dedupe StartWorkflowExecution calls
			RequestId:             common.StringPtr(ci.CreateRequestID),
			WorkflowIdReusePolicy: attributes.WorkflowIdReusePolicy,
		},
		ParentExecutionInfo: &history.ParentExecutionInfo{
			DomainUUID:  common.StringPtr(task.DomainID),
			Domain:      common.StringPtr(testDomainName),
			Execution:   &execution,
			InitiatedId: common.Int64Ptr(task.ScheduleID),
		},
		FirstDecisionTaskBackoffSeconds: common.Int32Ptr(backoff.GetBackoffForNextScheduleInSeconds(attributes.GetCronSchedule(), now, now)),
	}
}

func (s *transferQueueActiveProcessorSuite) createUpsertWorkflowSearchAttributesRequest(
	task *persistence.TransferTaskInfo,
	msBuilder mutableState,
) *persistence.UpsertWorkflowExecutionRequest {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(task.WorkflowID),
		RunId:      common.StringPtr(task.RunID),
	}
	executionInfo := msBuilder.GetExecutionInfo()
	domainEntry, err := s.mockDomainCache.GetDomainByID(task.DomainID)
	s.NoError(err)
	return &persistence.UpsertWorkflowExecutionRequest{
		DomainUUID:       domainEntry.GetInfo().ID,
		Domain:           domainEntry.GetInfo().Name,
		Execution:        execution,
		WorkflowTypeName: executionInfo.WorkflowTypeName,
		StartTimestamp:   executionInfo.StartTimestamp.UnixNano(),
		WorkflowTimeout:  int64(executionInfo.WorkflowTimeout),
		TaskID:           task.TaskID,
	}
}
