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
	"github.com/uber/cadence/.gen/go/matching"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/cron"
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
	transferQueueActiveProcessorSuite struct {
		suite.Suite

		mockShardManager *mocks.ShardManager
		logger           log.Logger

		mockHistoryEngine          *historyEngineImpl
		mockMetadataMgr            *mocks.MetadataManager
		mockVisibilityMgr          *mocks.VisibilityManager
		mockExecutionMgr           *mocks.ExecutionManager
		mockHistoryMgr             *mocks.HistoryManager
		mockHistoryV2Mgr           *mocks.HistoryV2Manager
		mockMatchingClient         *mocks.MatchingClient
		mockHistoryClient          *mocks.HistoryClient
		mockShard                  ShardContext
		mockClusterMetadata        *mocks.ClusterMetadata
		mockProducer               *mocks.KafkaProducer
		mockMessagingClient        messaging.Client
		mockQueueAckMgr            *MockQueueAckMgr
		mockClientBean             *client.MockClientBean
		mockTransferQueueProcessor *MockTransferQueueProcessor
		mockTimerQueueProcessor    *MockTimerQueueProcessor
		mockService                service.Service

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
	s.mockShardManager = &mocks.ShardManager{}
	s.mockExecutionMgr = &mocks.ExecutionManager{}
	s.mockHistoryMgr = &mocks.HistoryManager{}
	s.mockHistoryV2Mgr = &mocks.HistoryV2Manager{}
	s.mockVisibilityMgr = &mocks.VisibilityManager{}
	s.mockMatchingClient = &mocks.MatchingClient{}
	s.mockHistoryClient = &mocks.HistoryClient{}
	s.mockMetadataMgr = &mocks.MetadataManager{}
	s.mockTransferQueueProcessor = &MockTransferQueueProcessor{}
	s.mockTimerQueueProcessor = &MockTimerQueueProcessor{}
	s.mockClusterMetadata = &mocks.ClusterMetadata{}
	s.version = int64(4096)
	// ack manager will use the domain information
	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(&persistence.GetDomainResponse{
		Info:           &persistence.DomainInfo{ID: validDomainID},
		Config:         &persistence.DomainConfig{Retention: 1},
		IsGlobalDomain: true,
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			// Clusters attr is not used.
		},
		FailoverVersion: s.version,
		TableVersion:    persistence.DomainTableVersionV1,
	}, nil)
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	s.mockClusterMetadata.On("IsGlobalDomainEnabled").Return(true)
	s.mockProducer = &mocks.KafkaProducer{}
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History)
	s.mockMessagingClient = mocks.NewMockMessagingClient(s.mockProducer, nil)
	s.mockClientBean = &client.MockClientBean{}
	s.mockService = service.NewTestService(s.mockClusterMetadata, s.mockMessagingClient, metricsClient, s.mockClientBean)

	shardContext := &shardContextImpl{
		service:                   s.mockService,
		shardInfo:                 &persistence.ShardInfo{ShardID: shardID, RangeID: 1, TransferAckLevel: 0},
		transferSequenceNumber:    1,
		executionManager:          s.mockExecutionMgr,
		shardManager:              s.mockShardManager,
		historyMgr:                s.mockHistoryMgr,
		historyV2Mgr:              s.mockHistoryV2Mgr,
		maxTransferSequenceNumber: 100000,
		closeCh:                   make(chan int, 100),
		config:                    NewDynamicConfigForTest(),
		logger:                    s.logger,
		domainCache:               cache.NewDomainCache(s.mockMetadataMgr, s.mockClusterMetadata, metricsClient, s.logger),
		metricsClient:             metrics.NewClient(tally.NoopScope, metrics.History),
		timerMaxReadLevelMap:      make(map[string]time.Time),
	}
	shardContext.eventsCache = newEventsCache(shardContext)
	s.mockShard = shardContext

	historyCache := newHistoryCache(s.mockShard)
	h := &historyEngineImpl{
		currentClusterName: s.mockShard.GetService().GetClusterMetadata().GetCurrentClusterName(),
		shard:              s.mockShard,
		historyMgr:         s.mockHistoryMgr,
		historyV2Mgr:       s.mockHistoryV2Mgr,
		executionManager:   s.mockExecutionMgr,
		historyCache:       historyCache,
		logger:             s.logger,
		tokenSerializer:    common.NewJSONTaskTokenSerializer(),
		metricsClient:      s.mockShard.GetMetricsClient(),
		txProcessor:        s.mockTransferQueueProcessor,
		timerProcessor:     s.mockTimerQueueProcessor,
	}
	s.mockHistoryEngine = h
	s.transferQueueActiveProcessor = newTransferQueueActiveProcessor(s.mockShard, h, s.mockVisibilityMgr, s.mockMatchingClient, s.mockHistoryClient, newTaskAllocator(s.mockShard), s.logger)
	s.mockQueueAckMgr = &MockQueueAckMgr{}
	s.transferQueueActiveProcessor.queueAckMgr = s.mockQueueAckMgr
	s.transferQueueActiveProcessor.queueProcessorBase.ackMgr = s.mockQueueAckMgr
}

func (s *transferQueueActiveProcessorSuite) TearDownTest() {
	s.mockShardManager.AssertExpectations(s.T())
	s.mockExecutionMgr.AssertExpectations(s.T())
	s.mockHistoryMgr.AssertExpectations(s.T())
	s.mockHistoryV2Mgr.AssertExpectations(s.T())
	s.mockMatchingClient.AssertExpectations(s.T())
	s.mockHistoryClient.AssertExpectations(s.T())
	s.mockVisibilityMgr.AssertExpectations(s.T())
	s.mockProducer.AssertExpectations(s.T())
	s.mockTransferQueueProcessor.AssertExpectations(s.T())
	s.mockTimerQueueProcessor.AssertExpectations(s.T())
	s.mockQueueAckMgr.AssertExpectations(s.T())
	s.mockClientBean.AssertExpectations(s.T())
}

func (s *transferQueueActiveProcessorSuite) TestProcessActivityTask_Success() {
	domainID := "some random domain ID"
	targetDomainID := "some random target domain ID"
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(),
		s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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

	taskID := int64(59)
	activityID := "activity-1"
	activityType := "some random activity type"
	event, ai := addActivityTaskScheduledEvent(msBuilder, event.GetEventId(), activityID, activityType, taskListName, []byte{}, 1, 1, 1)
	msBuilder.UpdateReplicationStateLastEventID(s.mockClusterMetadata.GetCurrentClusterName(), s.version, event.GetEventId())

	transferTask := &persistence.TransferTaskInfo{
		Version:        s.version,
		DomainID:       domainID,
		TargetDomainID: targetDomainID,
		WorkflowID:     execution.GetWorkflowId(),
		RunID:          execution.GetRunId(),
		TaskID:         taskID,
		TaskList:       taskListName,
		TaskType:       persistence.TransferTaskTypeActivityTask,
		ScheduleID:     event.GetEventId(),
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.On("AddActivityTask", nil, s.createAddActivityTaskRequest(transferTask, ai)).Once().Return(nil)

	_, err := s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessActivityTask_Duplication() {
	domainID := "some random domain ID"
	targetDomainID := "some random target domain ID"
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(),
		s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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

	taskID := int64(59)
	activityID := "activity-1"
	activityType := "some random activity type"
	event, ai := addActivityTaskScheduledEvent(msBuilder, event.GetEventId(), activityID, activityType, taskListName, []byte{}, 1, 1, 1)

	transferTask := &persistence.TransferTaskInfo{
		Version:        s.version,
		DomainID:       domainID,
		TargetDomainID: targetDomainID,
		WorkflowID:     execution.GetWorkflowId(),
		RunID:          execution.GetRunId(),
		TaskID:         taskID,
		TaskList:       taskListName,
		TaskType:       persistence.TransferTaskTypeActivityTask,
		ScheduleID:     event.GetEventId(),
	}

	event = addActivityTaskStartedEvent(msBuilder, event.GetEventId(), taskListName, "")
	ai.StartedID = event.GetEventId()
	event = addActivityTaskCompletedEvent(msBuilder, ai.ScheduleID, ai.StartedID, nil, "")
	msBuilder.UpdateReplicationStateLastEventID(s.mockClusterMetadata.GetCurrentClusterName(), s.version, event.GetEventId())

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	_, err := s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessDecisionTask_FirstDecision() {
	domainID := "some random domain ID"
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(),
		s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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

	taskID := int64(59)
	di := addDecisionTaskScheduledEvent(msBuilder)
	msBuilder.UpdateReplicationStateLastEventID(s.mockClusterMetadata.GetCurrentClusterName(), s.version, di.ScheduleID)

	transferTask := &persistence.TransferTaskInfo{
		Version:    s.version,
		DomainID:   domainID,
		WorkflowID: execution.GetWorkflowId(),
		RunID:      execution.GetRunId(),
		TaskID:     taskID,
		TaskList:   taskListName,
		TaskType:   persistence.TransferTaskTypeDecisionTask,
		ScheduleID: di.ScheduleID,
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.On("AddDecisionTask", nil, s.createAddDecisionTaskRequest(transferTask, msBuilder)).Once().Return(nil)

	_, err := s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessDecisionTask_NonFirstDecision() {
	domainID := "some random domain ID"
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(),
		s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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

	// make another round of decision
	taskID := int64(59)
	di = addDecisionTaskScheduledEvent(msBuilder)
	msBuilder.UpdateReplicationStateLastEventID(s.mockClusterMetadata.GetCurrentClusterName(), s.version, di.ScheduleID)

	transferTask := &persistence.TransferTaskInfo{
		Version:    s.version,
		DomainID:   domainID,
		WorkflowID: execution.GetWorkflowId(),
		RunID:      execution.GetRunId(),
		TaskID:     taskID,
		TaskList:   taskListName,
		TaskType:   persistence.TransferTaskTypeDecisionTask,
		ScheduleID: di.ScheduleID,
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.On("AddDecisionTask", nil, s.createAddDecisionTaskRequest(transferTask, msBuilder)).Once().Return(nil)

	_, err := s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessDecisionTask_Sticky_NonFirstDecision() {
	domainID := "some random domain ID"
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"
	stickyTaskListName := "some random sticky task list"
	stickyTaskListTimeout := int32(233)

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(),
		s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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
	// set the sticky tasklist attr
	executionInfo := msBuilder.GetExecutionInfo()
	executionInfo.StickyTaskList = stickyTaskListName
	executionInfo.StickyScheduleToStartTimeout = stickyTaskListTimeout

	// make another round of decision
	taskID := int64(59)
	di = addDecisionTaskScheduledEvent(msBuilder)
	msBuilder.UpdateReplicationStateLastEventID(s.mockClusterMetadata.GetCurrentClusterName(), s.version, di.ScheduleID)

	transferTask := &persistence.TransferTaskInfo{
		Version:    s.version,
		DomainID:   domainID,
		WorkflowID: execution.GetWorkflowId(),
		RunID:      execution.GetRunId(),
		TaskID:     taskID,
		TaskList:   stickyTaskListName,
		TaskType:   persistence.TransferTaskTypeDecisionTask,
		ScheduleID: di.ScheduleID,
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.On("AddDecisionTask", nil, s.createAddDecisionTaskRequest(transferTask, msBuilder)).Once().Return(nil)

	_, err := s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessDecisionTask_DecisionNotSticky_MutableStateSticky() {
	domainID := "some random domain ID"
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"
	stickyTaskListName := "some random sticky task list"
	stickyTaskListTimeout := int32(233)

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(),
		s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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
	// set the sticky tasklist attr
	executionInfo := msBuilder.GetExecutionInfo()
	executionInfo.StickyTaskList = stickyTaskListName
	executionInfo.StickyScheduleToStartTimeout = stickyTaskListTimeout

	// make another round of decision
	taskID := int64(59)
	di = addDecisionTaskScheduledEvent(msBuilder)
	msBuilder.UpdateReplicationStateLastEventID(s.mockClusterMetadata.GetCurrentClusterName(), s.version, di.ScheduleID)

	transferTask := &persistence.TransferTaskInfo{
		Version:    s.version,
		DomainID:   domainID,
		WorkflowID: execution.GetWorkflowId(),
		RunID:      execution.GetRunId(),
		TaskID:     taskID,
		TaskList:   taskListName,
		TaskType:   persistence.TransferTaskTypeDecisionTask,
		ScheduleID: di.ScheduleID,
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.On("AddDecisionTask", nil, s.createAddDecisionTaskRequest(transferTask, msBuilder)).Once().Return(nil)

	_, err := s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessDecisionTask_Duplication() {
	domainID := "some random domain ID"
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(),
		s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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

	taskID := int64(4096)
	di := addDecisionTaskScheduledEvent(msBuilder)
	event := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, di.StartedID, nil, "some random identity")
	msBuilder.UpdateReplicationStateLastEventID(s.mockClusterMetadata.GetCurrentClusterName(), s.version, event.GetEventId())

	transferTask := &persistence.TransferTaskInfo{
		Version:    s.version,
		DomainID:   domainID,
		WorkflowID: execution.GetWorkflowId(),
		RunID:      execution.GetRunId(),
		TaskID:     taskID,
		TaskList:   taskListName,
		TaskType:   persistence.TransferTaskTypeDecisionTask,
		ScheduleID: di.ScheduleID,
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	_, err := s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessCloseExecution_HasParent() {
	domainID := "some random domain ID"
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

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(),
		s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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
			ParentExecutionInfo: &history.ParentExecutionInfo{
				DomainUUID:  common.StringPtr(parentDomainID),
				Domain:      common.StringPtr(parentDomainName),
				Execution:   &parentExecution,
				InitiatedId: common.Int64Ptr(parentInitiatedID),
			},
		},
	)

	di := addDecisionTaskScheduledEvent(msBuilder)
	event := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, di.StartedID, nil, "some random identity")

	taskID := int64(59)
	event = addCompleteWorkflowEvent(msBuilder, event.GetEventId(), nil)
	msBuilder.UpdateReplicationStateLastEventID(s.mockClusterMetadata.GetCurrentClusterName(), s.version, event.GetEventId())

	transferTask := &persistence.TransferTaskInfo{
		Version:    s.version,
		DomainID:   domainID,
		WorkflowID: execution.GetWorkflowId(),
		RunID:      execution.GetRunId(),
		TaskID:     taskID,
		TaskList:   taskListName,
		TaskType:   persistence.TransferTaskTypeCloseExecution,
		ScheduleID: event.GetEventId(),
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.On("RecordChildExecutionCompleted", nil, &history.RecordChildExecutionCompletedRequest{
		DomainUUID:         common.StringPtr(parentDomainID),
		WorkflowExecution:  &parentExecution,
		InitiatedId:        common.Int64Ptr(parentInitiatedID),
		CompletedExecution: &execution,
		CompletionEvent:    event,
	}).Return(nil).Once()
	s.mockVisibilityMgr.On("RecordWorkflowExecutionClosed", mock.Anything).Return(nil).Once()

	_, err := s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessCloseExecution_NoParent() {
	domainID := "some random domain ID"
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(),
		s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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

	taskID := int64(59)
	event = addCompleteWorkflowEvent(msBuilder, event.GetEventId(), nil)
	msBuilder.UpdateReplicationStateLastEventID(s.mockClusterMetadata.GetCurrentClusterName(), s.version, event.GetEventId())

	transferTask := &persistence.TransferTaskInfo{
		Version:    s.version,
		DomainID:   domainID,
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

	_, err := s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessCancelExecution_Success() {
	domainID := "some random domain ID"
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	targetDomainID := "some random target domain ID"
	targetExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random target workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(),
		s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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

	taskID := int64(59)
	event, rci := addRequestCancelInitiatedEvent(msBuilder, event.GetEventId(), uuid.New(), targetDomainID, targetExecution.GetWorkflowId(), targetExecution.GetRunId())

	transferTask := &persistence.TransferTaskInfo{
		Version:          s.version,
		DomainID:         domainID,
		WorkflowID:       execution.GetWorkflowId(),
		RunID:            execution.GetRunId(),
		TargetDomainID:   targetDomainID,
		TargetWorkflowID: targetExecution.GetWorkflowId(),
		TargetRunID:      targetExecution.GetRunId(),
		TaskID:           taskID,
		TaskList:         taskListName,
		TaskType:         persistence.TransferTaskTypeCancelExecution,
		ScheduleID:       event.GetEventId(),
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.On("RequestCancelWorkflowExecution", nil, s.createRequetCancelWorkflowExecutionRequest(transferTask, rci)).Return(nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", s.version).Return(cluster.TestCurrentClusterName)
	s.mockTimerQueueProcessor.On("NotifyNewTimers", cluster.TestCurrentClusterName, mock.Anything, mock.Anything).Once()

	_, err := s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessCancelExecution_Failure() {
	domainID := "some random domain ID"
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	targetDomainID := "some random target domain ID"
	targetExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random target workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(),
		s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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

	taskID := int64(59)
	event, rci := addRequestCancelInitiatedEvent(msBuilder, event.GetEventId(), uuid.New(), targetDomainID, targetExecution.GetWorkflowId(), targetExecution.GetRunId())
	msBuilder.UpdateReplicationStateLastEventID(s.mockClusterMetadata.GetCurrentClusterName(), s.version, event.GetEventId())

	transferTask := &persistence.TransferTaskInfo{
		Version:          s.version,
		DomainID:         domainID,
		WorkflowID:       execution.GetWorkflowId(),
		RunID:            execution.GetRunId(),
		TargetDomainID:   targetDomainID,
		TargetWorkflowID: targetExecution.GetWorkflowId(),
		TargetRunID:      targetExecution.GetRunId(),
		TaskID:           taskID,
		TaskList:         taskListName,
		TaskType:         persistence.TransferTaskTypeCancelExecution,
		ScheduleID:       event.GetEventId(),
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.On("RequestCancelWorkflowExecution", nil, s.createRequetCancelWorkflowExecutionRequest(transferTask, rci)).Return(&workflow.EntityNotExistsError{}).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", s.version).Return(cluster.TestCurrentClusterName)
	s.mockTimerQueueProcessor.On("NotifyNewTimers", cluster.TestCurrentClusterName, mock.Anything, mock.Anything).Once()

	_, err := s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessCancelExecution_Duplication() {
	domainID := "some random domain ID"
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	targetDomainID := "some random target domain ID"
	targetExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random target workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(),
		s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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

	taskID := int64(59)
	event, _ = addRequestCancelInitiatedEvent(msBuilder, event.GetEventId(), uuid.New(), targetDomainID, targetExecution.GetWorkflowId(), targetExecution.GetRunId())

	transferTask := &persistence.TransferTaskInfo{
		Version:          s.version,
		DomainID:         domainID,
		WorkflowID:       execution.GetWorkflowId(),
		RunID:            execution.GetRunId(),
		TargetDomainID:   targetDomainID,
		TargetWorkflowID: targetExecution.GetWorkflowId(),
		TargetRunID:      targetExecution.GetRunId(),
		TaskID:           taskID,
		TaskList:         taskListName,
		TaskType:         persistence.TransferTaskTypeCancelExecution,
		ScheduleID:       event.GetEventId(),
	}

	event = addCancelRequestedEvent(msBuilder, event.GetEventId(), targetDomainID, targetExecution.GetWorkflowId(), targetExecution.GetRunId())
	msBuilder.UpdateReplicationStateLastEventID(s.mockClusterMetadata.GetCurrentClusterName(), s.version, event.GetEventId())

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	_, err := s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessSignalExecution_Success() {
	domainID := "some random domain ID"
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	targetDomainID := "some random target domain ID"
	targetExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random target workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	signalName := "some random signal name"
	signalInput := []byte("some random signal input")
	signalControl := []byte("some random signal control")

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(),
		s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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

	taskID := int64(59)
	event, si := addRequestSignalInitiatedEvent(msBuilder, event.GetEventId(), uuid.New(),
		targetDomainID, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), signalName, signalInput, signalControl)
	msBuilder.UpdateReplicationStateLastEventID(s.mockClusterMetadata.GetCurrentClusterName(), s.version, event.GetEventId())

	transferTask := &persistence.TransferTaskInfo{
		Version:          s.version,
		DomainID:         domainID,
		WorkflowID:       execution.GetWorkflowId(),
		RunID:            execution.GetRunId(),
		TargetDomainID:   targetDomainID,
		TargetWorkflowID: targetExecution.GetWorkflowId(),
		TargetRunID:      targetExecution.GetRunId(),
		TaskID:           taskID,
		TaskList:         taskListName,
		TaskType:         persistence.TransferTaskTypeSignalExecution,
		ScheduleID:       event.GetEventId(),
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.On("SignalWorkflowExecution", nil, s.createSignalWorkflowExecutionRequest(transferTask, si)).Return(nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", s.version).Return(cluster.TestCurrentClusterName)
	s.mockTimerQueueProcessor.On("NotifyNewTimers", cluster.TestCurrentClusterName, mock.Anything, mock.Anything).Once()
	s.mockHistoryClient.On("RemoveSignalMutableState", nil, &history.RemoveSignalMutableStateRequest{
		DomainUUID: common.StringPtr(transferTask.TargetDomainID),
		WorkflowExecution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(transferTask.TargetWorkflowID),
			RunId:      common.StringPtr(transferTask.TargetRunID),
		},
		RequestId: common.StringPtr(si.SignalRequestID),
	}).Return(nil).Once()

	_, err := s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessSignalExecution_Failure() {
	domainID := "some random domain ID"
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	targetDomainID := "some random target domain ID"
	targetExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random target workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	signalName := "some random signal name"
	signalInput := []byte("some random signal input")
	signalControl := []byte("some random signal control")

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(),
		s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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

	taskID := int64(59)
	event, si := addRequestSignalInitiatedEvent(msBuilder, event.GetEventId(), uuid.New(),
		targetDomainID, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), signalName, signalInput, signalControl)
	msBuilder.UpdateReplicationStateLastEventID(s.mockClusterMetadata.GetCurrentClusterName(), s.version, event.GetEventId())

	transferTask := &persistence.TransferTaskInfo{
		Version:          s.version,
		DomainID:         domainID,
		WorkflowID:       execution.GetWorkflowId(),
		RunID:            execution.GetRunId(),
		TargetDomainID:   targetDomainID,
		TargetWorkflowID: targetExecution.GetWorkflowId(),
		TargetRunID:      targetExecution.GetRunId(),
		TaskID:           taskID,
		TaskList:         taskListName,
		TaskType:         persistence.TransferTaskTypeSignalExecution,
		ScheduleID:       event.GetEventId(),
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.On("SignalWorkflowExecution", nil, s.createSignalWorkflowExecutionRequest(transferTask, si)).Return(&workflow.EntityNotExistsError{}).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", s.version).Return(cluster.TestCurrentClusterName)
	s.mockTimerQueueProcessor.On("NotifyNewTimers", cluster.TestCurrentClusterName, mock.Anything, mock.Anything).Once()

	_, err := s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessSignalExecution_Duplication() {
	domainID := "some random domain ID"
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	targetDomainID := "some random target domain ID"
	targetExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random target workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	signalName := "some random signal name"
	signalInput := []byte("some random signal input")
	signalControl := []byte("some random signal control")

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(),
		s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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

	taskID := int64(59)
	event, _ = addRequestSignalInitiatedEvent(msBuilder, event.GetEventId(), uuid.New(),
		targetDomainID, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), signalName, signalInput, signalControl)

	transferTask := &persistence.TransferTaskInfo{
		Version:          s.version,
		DomainID:         domainID,
		WorkflowID:       execution.GetWorkflowId(),
		RunID:            execution.GetRunId(),
		TargetDomainID:   targetDomainID,
		TargetWorkflowID: targetExecution.GetWorkflowId(),
		TargetRunID:      targetExecution.GetRunId(),
		TaskID:           taskID,
		TaskList:         taskListName,
		TaskType:         persistence.TransferTaskTypeSignalExecution,
		ScheduleID:       event.GetEventId(),
	}

	event = addSignaledEvent(msBuilder, event.GetEventId(), targetDomainID, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), nil)
	msBuilder.UpdateReplicationStateLastEventID(s.mockClusterMetadata.GetCurrentClusterName(), s.version, event.GetEventId())

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	_, err := s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessStartChildExecution_Success() {
	domainID := "some random domain ID"
	domainName := "some random domain Name"
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	childDomainID := "some random child domain ID"
	childDomainName := "some random child domain Name"
	childWorkflowID := "some random child workflow ID"
	childRunID := uuid.New()
	childWorkflowType := "some random child workflow type"
	childTaskListName := "some random child task list"

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(),
		s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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

	taskID := int64(59)
	event, ci := addStartChildWorkflowExecutionInitiatedEvent(msBuilder, event.GetEventId(), uuid.New(),
		childDomainID, childWorkflowID, childWorkflowType, childTaskListName, nil, 1, 1)
	msBuilder.UpdateReplicationStateLastEventID(s.mockClusterMetadata.GetCurrentClusterName(), s.version, event.GetEventId())

	transferTask := &persistence.TransferTaskInfo{
		Version:          s.version,
		DomainID:         domainID,
		WorkflowID:       execution.GetWorkflowId(),
		RunID:            execution.GetRunId(),
		TargetDomainID:   childDomainID,
		TargetWorkflowID: childWorkflowID,
		TargetRunID:      "",
		TaskID:           taskID,
		TaskList:         taskListName,
		TaskType:         persistence.TransferTaskTypeStartChildExecution,
		ScheduleID:       event.GetEventId(),
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockMetadataMgr.ExpectedCalls = nil
	s.mockMetadataMgr.On("GetDomain", &persistence.GetDomainRequest{ID: domainID}).Return(&persistence.GetDomainResponse{
		Info:              &persistence.DomainInfo{Name: domainName},
		Config:            &persistence.DomainConfig{},
		ReplicationConfig: &persistence.DomainReplicationConfig{},
		FailoverVersion:   s.version,
		TableVersion:      persistence.DomainTableVersionV1,
	}, nil).Once()
	s.mockMetadataMgr.On("GetDomain", &persistence.GetDomainRequest{ID: childDomainID}).Return(&persistence.GetDomainResponse{
		Info:              &persistence.DomainInfo{Name: childDomainName},
		Config:            &persistence.DomainConfig{},
		ReplicationConfig: &persistence.DomainReplicationConfig{},
		TableVersion:      persistence.DomainTableVersionV1,
	}, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.On("StartWorkflowExecution", nil, s.createChildWorkflowExecutionRequest(
		transferTask,
		msBuilder,
		ci,
		domainName,
		childDomainName,
	)).Return(&workflow.StartWorkflowExecutionResponse{RunId: common.StringPtr(childRunID)}, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", s.version).Return(cluster.TestCurrentClusterName)
	s.mockHistoryClient.On("ScheduleDecisionTask", nil, &history.ScheduleDecisionTaskRequest{
		DomainUUID: common.StringPtr(childDomainID),
		WorkflowExecution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(childWorkflowID),
			RunId:      common.StringPtr(childRunID),
		},
		IsFirstDecision: common.BoolPtr(true),
	}).Return(nil).Once()
	s.mockTimerQueueProcessor.On("NotifyNewTimers", cluster.TestCurrentClusterName, mock.Anything, mock.Anything).Once()

	_, err := s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessStartChildExecution_Failure() {
	domainID := "some random domain ID"
	domainName := "some random domain Name"
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	childDomainID := "some random child domain ID"
	childDomainName := "some random child domain Name"
	childWorkflowID := "some random child workflow ID"
	childWorkflowType := "some random child workflow type"
	childTaskListName := "some random child task list"

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(),
		s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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

	taskID := int64(59)
	event, ci := addStartChildWorkflowExecutionInitiatedEvent(msBuilder, event.GetEventId(), uuid.New(),
		childDomainID, childWorkflowID, childWorkflowType, childTaskListName, nil, 1, 1)
	msBuilder.UpdateReplicationStateLastEventID(s.mockClusterMetadata.GetCurrentClusterName(), s.version, event.GetEventId())

	transferTask := &persistence.TransferTaskInfo{
		Version:          s.version,
		DomainID:         domainID,
		WorkflowID:       execution.GetWorkflowId(),
		RunID:            execution.GetRunId(),
		TargetDomainID:   childDomainID,
		TargetWorkflowID: childWorkflowID,
		TargetRunID:      "",
		TaskID:           taskID,
		TaskList:         taskListName,
		TaskType:         persistence.TransferTaskTypeStartChildExecution,
		ScheduleID:       event.GetEventId(),
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockMetadataMgr.ExpectedCalls = nil
	s.mockMetadataMgr.On("GetDomain", &persistence.GetDomainRequest{ID: domainID}).Return(&persistence.GetDomainResponse{
		Info:              &persistence.DomainInfo{Name: domainName},
		Config:            &persistence.DomainConfig{},
		ReplicationConfig: &persistence.DomainReplicationConfig{},
		FailoverVersion:   s.version,
		TableVersion:      persistence.DomainTableVersionV1,
	}, nil).Once()
	s.mockMetadataMgr.On("GetDomain", &persistence.GetDomainRequest{ID: childDomainID}).Return(&persistence.GetDomainResponse{
		Info:              &persistence.DomainInfo{Name: childDomainName},
		Config:            &persistence.DomainConfig{},
		ReplicationConfig: &persistence.DomainReplicationConfig{},
		TableVersion:      persistence.DomainTableVersionV1,
	}, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.On("StartWorkflowExecution", nil, s.createChildWorkflowExecutionRequest(
		transferTask,
		msBuilder,
		ci,
		domainName,
		childDomainName,
	)).Return(nil, &workflow.WorkflowExecutionAlreadyStartedError{}).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", s.version).Return(cluster.TestCurrentClusterName)
	s.mockTimerQueueProcessor.On("NotifyNewTimers", cluster.TestCurrentClusterName, mock.Anything, mock.Anything).Once()

	_, err := s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessStartChildExecution_Success_Dup() {
	domainID := "some random domain ID"
	domainName := "some random domain Name"
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	childDomainID := "some random child domain ID"
	childDomainName := "some random child domain Name"
	childWorkflowID := "some random child workflow ID"
	childRunID := uuid.New()
	childWorkflowType := "some random child workflow type"
	childTaskListName := "some random child task list"

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(),
		s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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

	taskID := int64(59)
	event, ci := addStartChildWorkflowExecutionInitiatedEvent(msBuilder, event.GetEventId(), uuid.New(),
		childDomainID, childWorkflowID, childWorkflowType, childTaskListName, nil, 1, 1)

	transferTask := &persistence.TransferTaskInfo{
		Version:          s.version,
		DomainID:         domainID,
		WorkflowID:       execution.GetWorkflowId(),
		RunID:            execution.GetRunId(),
		TargetDomainID:   childDomainID,
		TargetWorkflowID: childWorkflowID,
		TargetRunID:      "",
		TaskID:           taskID,
		TaskList:         taskListName,
		TaskType:         persistence.TransferTaskTypeStartChildExecution,
		ScheduleID:       event.GetEventId(),
	}

	event = addChildWorkflowExecutionStartedEvent(msBuilder, event.GetEventId(), childDomainID, childWorkflowID, childRunID, childWorkflowType)
	ci.StartedID = event.GetEventId()
	msBuilder.UpdateReplicationStateLastEventID(s.mockClusterMetadata.GetCurrentClusterName(), s.version, event.GetEventId())

	persistenceMutableState := createMutableState(msBuilder)
	s.mockMetadataMgr.ExpectedCalls = nil
	s.mockMetadataMgr.On("GetDomain", &persistence.GetDomainRequest{ID: domainID}).Return(&persistence.GetDomainResponse{
		Info:              &persistence.DomainInfo{Name: domainName},
		Config:            &persistence.DomainConfig{},
		ReplicationConfig: &persistence.DomainReplicationConfig{},
		FailoverVersion:   s.version,
		TableVersion:      persistence.DomainTableVersionV1,
	}, nil).Once()
	s.mockMetadataMgr.On("GetDomain", &persistence.GetDomainRequest{ID: childDomainID}).Return(&persistence.GetDomainResponse{
		Info:              &persistence.DomainInfo{Name: childDomainName},
		Config:            &persistence.DomainConfig{},
		ReplicationConfig: &persistence.DomainReplicationConfig{},
		TableVersion:      persistence.DomainTableVersionV1,
	}, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.On("ScheduleDecisionTask", nil, &history.ScheduleDecisionTaskRequest{
		DomainUUID: common.StringPtr(childDomainID),
		WorkflowExecution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(childWorkflowID),
			RunId:      common.StringPtr(childRunID),
		},
		IsFirstDecision: common.BoolPtr(true),
	}).Return(nil).Once()

	_, err := s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessStartChildExecution_Duplication() {
	domainID := "some random domain ID"
	domainName := "some random domain Name"
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	childDomainID := "some random child domain ID"
	childDomainName := "some random child domain Name"
	childExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random child workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	childWorkflowType := "some random child workflow type"
	childTaskListName := "some random child task list"

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(),
		s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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

	taskID := int64(59)
	event, ci := addStartChildWorkflowExecutionInitiatedEvent(msBuilder, event.GetEventId(), uuid.New(),
		childDomainID, childExecution.GetWorkflowId(), childWorkflowType, childTaskListName, nil, 1, 1)

	transferTask := &persistence.TransferTaskInfo{
		Version:          s.version,
		DomainID:         domainID,
		WorkflowID:       execution.GetWorkflowId(),
		RunID:            execution.GetRunId(),
		TargetDomainID:   childDomainID,
		TargetWorkflowID: childExecution.GetWorkflowId(),
		TargetRunID:      "",
		TaskID:           taskID,
		TaskList:         taskListName,
		TaskType:         persistence.TransferTaskTypeStartChildExecution,
		ScheduleID:       event.GetEventId(),
	}

	event = addChildWorkflowExecutionStartedEvent(msBuilder, event.GetEventId(), childDomainID, childExecution.GetWorkflowId(), childExecution.GetRunId(), childWorkflowType)
	ci.StartedID = event.GetEventId()
	event = addChildWorkflowExecutionCompletedEvent(msBuilder, ci.InitiatedID, &childExecution, &workflow.WorkflowExecutionCompletedEventAttributes{
		Result:                       []byte("some random child workflow execution result"),
		DecisionTaskCompletedEventId: common.Int64Ptr(transferTask.ScheduleID),
	})
	msBuilder.UpdateReplicationStateLastEventID(s.mockClusterMetadata.GetCurrentClusterName(), s.version, event.GetEventId())

	persistenceMutableState := createMutableState(msBuilder)
	s.mockMetadataMgr.ExpectedCalls = nil
	s.mockMetadataMgr.On("GetDomain", &persistence.GetDomainRequest{ID: domainID}).Return(&persistence.GetDomainResponse{
		Info:              &persistence.DomainInfo{Name: domainName},
		Config:            &persistence.DomainConfig{},
		ReplicationConfig: &persistence.DomainReplicationConfig{},
		FailoverVersion:   s.version,
		TableVersion:      persistence.DomainTableVersionV1,
	}, nil).Once()
	s.mockMetadataMgr.On("GetDomain", &persistence.GetDomainRequest{ID: childDomainID}).Return(&persistence.GetDomainResponse{
		Info:              &persistence.DomainInfo{Name: childDomainName},
		Config:            &persistence.DomainConfig{},
		ReplicationConfig: &persistence.DomainReplicationConfig{},
		TableVersion:      persistence.DomainTableVersionV1,
	}, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	_, err := s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveProcessorSuite) TestProcessRecordWorkflowStartedTask() {
	domainID := "some random domain ID"
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"
	cronSchedule := "@every 5s"
	backoffSeconds := int32(5)

	msBuilder := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(),
		s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())

	event := msBuilder.AddWorkflowExecutionStartedEvent(
		execution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(domainID),
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

	taskID := int64(59)
	msBuilder.UpdateReplicationStateLastEventID(s.mockClusterMetadata.GetCurrentClusterName(), s.version, event.GetEventId())

	msBuilder.UpdateReplicationStateVersion(s.version+1, false)
	di := addDecisionTaskScheduledEvent(msBuilder)
	msBuilder.UpdateReplicationStateLastEventID(s.mockClusterMetadata.GetCurrentClusterName(), di.Version, di.ScheduleID)

	transferTask := &persistence.TransferTaskInfo{
		Version:    s.version,
		DomainID:   domainID,
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

	_, err := s.transferQueueActiveProcessor.process(transferTask, true)
	s.Nil(err)
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

	return &persistence.RecordWorkflowExecutionStartedRequest{
		DomainUUID:         task.DomainID,
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
	msBuilder mutableState, ci *persistence.ChildExecutionInfo, domainName string, targetDomainName string) *history.StartWorkflowExecutionRequest {

	event, ok := msBuilder.GetChildExecutionInitiatedEvent(task.ScheduleID)
	if !ok {
		s.Fail("Cannot find corresponding child workflow info with scheduled ID %v.", task.ScheduleID)
	}
	attributes := event.StartChildWorkflowExecutionInitiatedEventAttributes
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(task.WorkflowID),
		RunId:      common.StringPtr(task.RunID),
	}

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
			ChildPolicy:           attributes.ChildPolicy,
		},
		ParentExecutionInfo: &history.ParentExecutionInfo{
			DomainUUID:  common.StringPtr(task.DomainID),
			Domain:      common.StringPtr(domainName),
			Execution:   &execution,
			InitiatedId: common.Int64Ptr(task.ScheduleID),
		},
		FirstDecisionTaskBackoffSeconds: common.Int32Ptr(cron.GetBackoffForNextScheduleInSeconds(attributes.GetCronSchedule(), time.Now())),
	}
}
