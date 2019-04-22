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

		mockShardManager *mocks.ShardManager
		logger           log.Logger

		mockHistoryEngine       *historyEngineImpl
		mockMetadataMgr         *mocks.MetadataManager
		mockVisibilityMgr       *mocks.VisibilityManager
		mockMatchingClient      *mocks.MatchingClient
		mockExecutionMgr        *mocks.ExecutionManager
		mockHistoryMgr          *mocks.HistoryManager
		mockShard               ShardContext
		mockClusterMetadata     *mocks.ClusterMetadata
		mockProducer            *mocks.KafkaProducer
		mockClientBean          *client.MockClientBean
		mockMessagingClient     messaging.Client
		mockQueueAckMgr         *MockQueueAckMgr
		mockService             service.Service
		mockHistoryRereplicator *xdc.MockHistoryRereplicator
		clusterName             string

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
	s.mockShardManager = &mocks.ShardManager{}
	s.mockExecutionMgr = &mocks.ExecutionManager{}
	s.mockHistoryMgr = &mocks.HistoryManager{}
	s.mockVisibilityMgr = &mocks.VisibilityManager{}
	s.mockMatchingClient = &mocks.MatchingClient{}
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
	s.mockProducer = &mocks.KafkaProducer{}
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History)
	s.mockMessagingClient = mocks.NewMockMessagingClient(s.mockProducer, nil)
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
		standbyClusterCurrentTime: make(map[string]time.Time),
		timerMaxReadLevelMap:      make(map[string]time.Time),
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
	s.transferQueueStandbyProcessor = newTransferQueueStandbyProcessor(
		s.clusterName, s.mockShard, h, s.mockVisibilityMgr, s.mockMatchingClient,
		newTaskAllocator(s.mockShard), s.mockHistoryRereplicator, s.logger,
	)
	s.mockQueueAckMgr = &MockQueueAckMgr{}
	s.transferQueueStandbyProcessor.queueAckMgr = s.mockQueueAckMgr
}

func (s *transferQueueStandbyProcessorSuite) TearDownTest() {
	s.mockShardManager.AssertExpectations(s.T())
	s.mockExecutionMgr.AssertExpectations(s.T())
	s.mockHistoryMgr.AssertExpectations(s.T())
	s.mockVisibilityMgr.AssertExpectations(s.T())
	s.mockProducer.AssertExpectations(s.T())
	s.mockClientBean.AssertExpectations(s.T())
	s.mockHistoryRereplicator.AssertExpectations(s.T())
}

func (s *transferQueueStandbyProcessorSuite) TestProcessActivityTask_Pending() {
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

	taskID := int64(59)
	activityID := "activity-1"
	activityType := "some random activity type"
	event, _ = addActivityTaskScheduledEvent(msBuilder, event.GetEventId(), activityID, activityType, taskListName, []byte{}, 1, 1, 1)

	s.mockShard.SetCurrentTime(s.clusterName, time.Now())
	transferTask := &persistence.TransferTaskInfo{
		Version:             version,
		DomainID:            domainID,
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

	_, err := s.transferQueueStandbyProcessor.process(transferTask, true)
	s.Equal(ErrTaskRetry, err)
}

func (s *transferQueueStandbyProcessorSuite) TestProcessActivityTask_Pending_PushToMatching() {
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

	taskID := int64(59)
	activityID := "activity-1"
	activityType := "some random activity type"
	event, _ = addActivityTaskScheduledEvent(msBuilder, event.GetEventId(), activityID, activityType, taskListName, []byte{}, 1, 1, 1)

	s.mockShard.SetCurrentTime(s.clusterName, time.Now().Add(3*s.mockShard.GetConfig().StandbyClusterDelay()))
	transferTask := &persistence.TransferTaskInfo{
		Version:             version,
		DomainID:            domainID,
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
	s.mockMatchingClient.On("AddActivityTask", mock.Anything, mock.Anything).Return(nil).Once()

	_, err := s.transferQueueStandbyProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueStandbyProcessorSuite) TestProcessActivityTask_Success() {
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

	taskID := int64(59)
	activityID := "activity-1"
	activityType := "some random activity type"
	event, _ = addActivityTaskScheduledEvent(msBuilder, event.GetEventId(), activityID, activityType, taskListName, []byte{}, 1, 1, 1)

	transferTask := &persistence.TransferTaskInfo{
		Version:             version,
		DomainID:            domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		VisibilityTimestamp: time.Now(),
		TaskID:              taskID,
		TaskList:            taskListName,
		TaskType:            persistence.TransferTaskTypeActivityTask,
		ScheduleID:          event.GetEventId(),
	}

	addActivityTaskStartedEvent(msBuilder, event.GetEventId(), taskListName, "")

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	_, err := s.transferQueueStandbyProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueStandbyProcessorSuite) TestProcessDecisionTask_Pending() {
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

	taskID := int64(59)
	di := addDecisionTaskScheduledEvent(msBuilder)

	s.mockShard.SetCurrentTime(s.clusterName, time.Now())
	transferTask := &persistence.TransferTaskInfo{
		Version:             version,
		DomainID:            domainID,
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
	_, err := s.transferQueueStandbyProcessor.process(transferTask, true)
	s.Equal(ErrTaskRetry, err)
}

func (s *transferQueueStandbyProcessorSuite) TestProcessDecisionTask_Pending_PushToMatching() {
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

	taskID := int64(59)
	di := addDecisionTaskScheduledEvent(msBuilder)

	s.mockShard.SetCurrentTime(s.clusterName, time.Now().Add(3*s.mockShard.GetConfig().StandbyClusterDelay()))
	transferTask := &persistence.TransferTaskInfo{
		Version:             version,
		DomainID:            domainID,
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
	s.mockMatchingClient.On("AddDecisionTask", mock.Anything, mock.Anything).Return(nil).Once()

	_, err := s.transferQueueStandbyProcessor.process(transferTask, true)
	s.Nil(nil, err)
}

func (s *transferQueueStandbyProcessorSuite) TestProcessDecisionTask_Success_FirstDecision() {
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

	taskID := int64(59)
	di := addDecisionTaskScheduledEvent(msBuilder)

	transferTask := &persistence.TransferTaskInfo{
		Version:             version,
		DomainID:            domainID,
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
	_, err := s.transferQueueStandbyProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueStandbyProcessorSuite) TestProcessDecisionTask_Success_NonFirstDecision() {
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

	taskID := int64(59)
	di = addDecisionTaskScheduledEvent(msBuilder)

	transferTask := &persistence.TransferTaskInfo{
		Version:             version,
		DomainID:            domainID,
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

	_, err := s.transferQueueStandbyProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueStandbyProcessorSuite) TestProcessCloseExecution() {
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

	taskID := int64(59)
	event = addCompleteWorkflowEvent(msBuilder, event.GetEventId(), nil)
	msBuilder.UpdateReplicationStateLastEventID(s.mockClusterMetadata.GetCurrentClusterName(), version, event.GetEventId())

	transferTask := &persistence.TransferTaskInfo{
		Version:             version,
		DomainID:            domainID,
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

	_, err := s.transferQueueStandbyProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueStandbyProcessorSuite) TestProcessCancelExecution_Pending() {
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

	taskID := int64(59)
	event, _ = addRequestCancelInitiatedEvent(msBuilder, event.GetEventId(), uuid.New(), targetDomainID, targetExecution.GetWorkflowId(), targetExecution.GetRunId())
	nextEventID := event.GetEventId() + 1

	transferTask := &persistence.TransferTaskInfo{
		Version:             version,
		DomainID:            domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		VisibilityTimestamp: time.Now(),
		TargetDomainID:      targetDomainID,
		TargetWorkflowID:    targetExecution.GetWorkflowId(),
		TargetRunID:         targetExecution.GetRunId(),
		TaskID:              taskID,
		TaskList:            taskListName,
		TaskType:            persistence.TransferTaskTypeCancelExecution,
		ScheduleID:          event.GetEventId(),
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	_, err := s.transferQueueStandbyProcessor.process(transferTask, true)
	s.Equal(ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, time.Now().Add(3*s.mockShard.GetConfig().StandbyClusterDelay()))
	s.mockHistoryRereplicator.On("SendMultiWorkflowHistory",
		transferTask.DomainID, transferTask.WorkflowID,
		transferTask.RunID, nextEventID,
		transferTask.RunID, common.EndEventID,
	).Return(nil).Once()
	_, err = s.transferQueueStandbyProcessor.process(transferTask, true)
	s.Equal(ErrTaskDiscarded, err)
}

func (s *transferQueueStandbyProcessorSuite) TestProcessCancelExecution_Success() {
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

	taskID := int64(59)
	event, _ = addRequestCancelInitiatedEvent(msBuilder, event.GetEventId(), uuid.New(), targetDomainID, targetExecution.GetWorkflowId(), targetExecution.GetRunId())

	transferTask := &persistence.TransferTaskInfo{
		Version:             version,
		DomainID:            domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		VisibilityTimestamp: time.Now(),
		TargetDomainID:      targetDomainID,
		TargetWorkflowID:    targetExecution.GetWorkflowId(),
		TargetRunID:         targetExecution.GetRunId(),
		TaskID:              taskID,
		TaskList:            taskListName,
		TaskType:            persistence.TransferTaskTypeCancelExecution,
		ScheduleID:          event.GetEventId(),
	}

	addCancelRequestedEvent(msBuilder, event.GetEventId(), targetDomainID, targetExecution.GetWorkflowId(), targetExecution.GetRunId())

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	_, err := s.transferQueueStandbyProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueStandbyProcessorSuite) TestProcessSignalExecution_Pending() {
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

	taskID := int64(59)
	event, _ = addRequestSignalInitiatedEvent(msBuilder, event.GetEventId(), uuid.New(),
		targetDomainID, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), signalName, nil, nil)
	nextEventID := event.GetEventId() + 1

	transferTask := &persistence.TransferTaskInfo{
		Version:             version,
		DomainID:            domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		VisibilityTimestamp: time.Now(),
		TargetDomainID:      targetDomainID,
		TargetWorkflowID:    targetExecution.GetWorkflowId(),
		TargetRunID:         targetExecution.GetRunId(),
		TaskID:              taskID,
		TaskList:            taskListName,
		TaskType:            persistence.TransferTaskTypeSignalExecution,
		ScheduleID:          event.GetEventId(),
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	_, err := s.transferQueueStandbyProcessor.process(transferTask, true)
	s.Equal(ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, time.Now().Add(3*s.mockShard.GetConfig().StandbyClusterDelay()))
	s.mockHistoryRereplicator.On("SendMultiWorkflowHistory",
		transferTask.DomainID, transferTask.WorkflowID,
		transferTask.RunID, nextEventID,
		transferTask.RunID, common.EndEventID,
	).Return(nil).Once()
	_, err = s.transferQueueStandbyProcessor.process(transferTask, true)
	s.Equal(ErrTaskDiscarded, err)
}

func (s *transferQueueStandbyProcessorSuite) TestProcessSignalExecution_Success() {
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

	taskID := int64(59)
	event, _ = addRequestSignalInitiatedEvent(msBuilder, event.GetEventId(), uuid.New(),
		targetDomainID, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), signalName, nil, nil)

	transferTask := &persistence.TransferTaskInfo{
		Version:             version,
		DomainID:            domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		VisibilityTimestamp: time.Now(),
		TargetDomainID:      targetDomainID,
		TargetWorkflowID:    targetExecution.GetWorkflowId(),
		TargetRunID:         targetExecution.GetRunId(),
		TaskID:              taskID,
		TaskList:            taskListName,
		TaskType:            persistence.TransferTaskTypeSignalExecution,
		ScheduleID:          event.GetEventId(),
	}

	addSignaledEvent(msBuilder, event.GetEventId(), targetDomainID, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), nil)

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	_, err := s.transferQueueStandbyProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueStandbyProcessorSuite) TestProcessStartChildExecution_Pending() {
	domainID := "some random domain ID"
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	childDomainID := "some random child domain ID"
	childWorkflowID := "some random child workflow ID"
	childWorkflowType := "some random child workflow type"
	childTaskListName := "some random child task list"

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

	taskID := int64(59)
	event, _ = addStartChildWorkflowExecutionInitiatedEvent(msBuilder, event.GetEventId(), uuid.New(),
		childDomainID, childWorkflowID, childWorkflowType, childTaskListName, nil, 1, 1)
	nextEventID := event.GetEventId() + 1

	transferTask := &persistence.TransferTaskInfo{
		Version:             version,
		DomainID:            domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		VisibilityTimestamp: time.Now(),
		TargetDomainID:      childDomainID,
		TargetWorkflowID:    childWorkflowID,
		TargetRunID:         "",
		TaskID:              taskID,
		TaskList:            taskListName,
		TaskType:            persistence.TransferTaskTypeStartChildExecution,
		ScheduleID:          event.GetEventId(),
	}

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	_, err := s.transferQueueStandbyProcessor.process(transferTask, true)
	s.Equal(ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, time.Now().Add(3*s.mockShard.GetConfig().StandbyClusterDelay()))
	s.mockHistoryRereplicator.On("SendMultiWorkflowHistory",
		transferTask.DomainID, transferTask.WorkflowID,
		transferTask.RunID, nextEventID,
		transferTask.RunID, common.EndEventID,
	).Return(nil).Once()
	_, err = s.transferQueueStandbyProcessor.process(transferTask, true)
	s.Equal(ErrTaskDiscarded, err)
}

func (s *transferQueueStandbyProcessorSuite) TestProcessStartChildExecution_Success() {
	domainID := "some random domain ID"
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	childDomainID := "some random child domain ID"
	childWorkflowID := "some random child workflow ID"
	childWorkflowType := "some random child workflow type"
	childTaskListName := "some random child task list"

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

	taskID := int64(59)
	event, childInfo := addStartChildWorkflowExecutionInitiatedEvent(msBuilder, event.GetEventId(), uuid.New(),
		childDomainID, childWorkflowID, childWorkflowType, childTaskListName, nil, 1, 1)

	transferTask := &persistence.TransferTaskInfo{
		Version:             version,
		DomainID:            domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		VisibilityTimestamp: time.Now(),
		TargetDomainID:      childDomainID,
		TargetWorkflowID:    childWorkflowID,
		TargetRunID:         "",
		TaskID:              taskID,
		TaskList:            taskListName,
		TaskType:            persistence.TransferTaskTypeStartChildExecution,
		ScheduleID:          event.GetEventId(),
	}

	event = addChildWorkflowExecutionStartedEvent(msBuilder, event.GetEventId(), childDomainID, childWorkflowID, uuid.New(), childWorkflowType)
	childInfo.StartedID = event.GetEventId()

	persistenceMutableState := createMutableState(msBuilder)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	_, err := s.transferQueueStandbyProcessor.process(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueStandbyProcessorSuite) TestProcessRecordWorkflowStartedTask() {
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

	taskID := int64(59)
	msBuilder.UpdateReplicationStateLastEventID(s.mockClusterMetadata.GetCurrentClusterName(), version, event.GetEventId())

	msBuilder.UpdateReplicationStateVersion(version+1, false)
	di := addDecisionTaskScheduledEvent(msBuilder)
	msBuilder.UpdateReplicationStateLastEventID(s.mockClusterMetadata.GetCurrentClusterName(), di.Version, di.ScheduleID)

	transferTask := &persistence.TransferTaskInfo{
		Version:             version,
		DomainID:            domainID,
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
		DomainUUID: executionInfo.DomainID,
		Execution: workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(executionInfo.WorkflowID),
			RunId:      common.StringPtr(executionInfo.RunID),
		},
		WorkflowTypeName: executionInfo.WorkflowTypeName,
		StartTimestamp:   executionInfo.StartTimestamp.UnixNano(),
		WorkflowTimeout:  int64(executionInfo.WorkflowTimeout),
		TaskID:           taskID,
	}).Return(nil).Once()
	_, err := s.transferQueueStandbyProcessor.process(transferTask, true)
	s.Nil(err)
}
