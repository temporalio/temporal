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
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	"github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/matching/matchingservicetest"
	workflow "github.com/uber/cadence/.gen/go/shared"
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
		*require.Assertions

		controller               *gomock.Controller
		mockTxProcessor          *MocktransferQueueProcessor
		mockReplicationProcessor *MockReplicatorQueueProcessor
		mockTimerProcessor       *MocktimerQueueProcessor
		mockDomainCache          *cache.MockDomainCache

		mockShardManager        *mocks.ShardManager
		mockHistoryEngine       *historyEngineImpl
		mockVisibilityMgr       *mocks.VisibilityManager
		mockMatchingClient      *matchingservicetest.MockClient
		mockExecutionMgr        *mocks.ExecutionManager
		mockShard               ShardContext
		mockClusterMetadata     *mocks.ClusterMetadata
		mockProducer            *mocks.KafkaProducer
		mockMessagingClient     messaging.Client
		mockQueueAckMgr         *MockQueueAckMgr
		mockService             service.Service
		mockHistoryRereplicator *xdc.MockHistoryRereplicator
		mockNDCHistoryResender  *xdc.MockNDCHistoryResender
		logger                  log.Logger

		mockArchivalMetadata *archiver.MockArchivalMetadata
		mockArchiverProvider *provider.MockArchiverProvider

		domainID             string
		domainEntry          *cache.DomainCacheEntry
		version              int64
		clusterName          string
		now                  time.Time
		timeSource           *clock.EventTimeSource
		fetchHistoryDuration time.Duration
		discardDuration      time.Duration

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
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockTxProcessor = NewMocktransferQueueProcessor(s.controller)
	s.mockReplicationProcessor = NewMockReplicatorQueueProcessor(s.controller)
	s.mockTimerProcessor = NewMocktimerQueueProcessor(s.controller)
	s.mockDomainCache = cache.NewMockDomainCache(s.controller)
	s.mockTxProcessor.EXPECT().NotifyNewTask(gomock.Any(), gomock.Any()).AnyTimes()
	s.mockReplicationProcessor.EXPECT().notifyNewTask().AnyTimes()
	s.mockTimerProcessor.EXPECT().NotifyNewTimers(gomock.Any(), gomock.Any()).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomainByID(testDomainID).Return(testGlobalDomainEntry, nil).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomain(testDomainName).Return(testGlobalDomainEntry, nil).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomainByID(testTargetDomainID).Return(testGlobalTargetDomainEntry, nil).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomain(testTargetDomainName).Return(testGlobalTargetDomainEntry, nil).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomainByID(testParentDomainID).Return(testGlobalParentDomainEntry, nil).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomain(testParentDomainName).Return(testGlobalParentDomainEntry, nil).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomainByID(testChildDomainID).Return(testGlobalChildDomainEntry, nil).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomain(testChildDomainName).Return(testGlobalChildDomainEntry, nil).AnyTimes()

	config := NewDynamicConfigForTest()
	s.domainID = testDomainID
	s.domainEntry = testGlobalDomainEntry
	s.version = s.domainEntry.GetFailoverVersion()
	s.now = time.Now()
	s.timeSource = clock.NewEventTimeSource().Update(s.now)
	s.fetchHistoryDuration = config.StandbyTaskMissingEventsResendDelay() +
		(config.StandbyTaskMissingEventsDiscardDelay()-config.StandbyTaskMissingEventsResendDelay())/2
	s.discardDuration = config.StandbyTaskMissingEventsDiscardDelay() * 2

	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)
	s.mockShardManager = &mocks.ShardManager{}
	s.mockExecutionMgr = &mocks.ExecutionManager{}
	s.mockVisibilityMgr = &mocks.VisibilityManager{}
	s.mockMatchingClient = matchingservicetest.NewMockClient(s.controller)

	s.mockClusterMetadata = &mocks.ClusterMetadata{}
	s.mockHistoryRereplicator = &xdc.MockHistoryRereplicator{}
	s.mockNDCHistoryResender = &xdc.MockNDCHistoryResender{}
	s.mockProducer = &mocks.KafkaProducer{}
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History)
	s.mockMessagingClient = mocks.NewMockMessagingClient(s.mockProducer, nil)
	s.mockArchivalMetadata = &archiver.MockArchivalMetadata{}
	s.mockArchiverProvider = &provider.MockArchiverProvider{}
	s.mockService = service.NewTestService(
		s.mockClusterMetadata,
		s.mockMessagingClient,
		metricsClient,
		nil,
		s.mockArchivalMetadata,
		s.mockArchiverProvider,
		nil,
	)

	shardContext := &shardContextImpl{
		service:                   s.mockService,
		clusterMetadata:           s.mockClusterMetadata,
		shardInfo:                 &persistence.ShardInfo{RangeID: 1, TransferAckLevel: 0},
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
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", s.version).Return(s.clusterName)

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
		s.mockNDCHistoryResender,
		s.logger,
	)
	s.mockQueueAckMgr = &MockQueueAckMgr{}
	s.transferQueueStandbyProcessor.queueAckMgr = s.mockQueueAckMgr
}

func (s *transferQueueStandbyProcessorSuite) TearDownTest() {
	s.mockShardManager.AssertExpectations(s.T())
	s.mockExecutionMgr.AssertExpectations(s.T())
	s.mockVisibilityMgr.AssertExpectations(s.T())
	s.mockProducer.AssertExpectations(s.T())
	s.mockHistoryRereplicator.AssertExpectations(s.T())
	s.mockArchivalMetadata.AssertExpectations(s.T())
	s.mockArchiverProvider.AssertExpectations(s.T())
	s.controller.Finish()
}

func (s *transferQueueStandbyProcessorSuite) TestProcessActivityTask_Pending() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
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

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, nil, "some random identity")

	taskID := int64(59)
	activityID := "activity-1"
	activityType := "some random activity type"
	event, _ = addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID, activityType, taskListName, []byte{}, 1, 1, 1)

	now := time.Now()
	transferTask := &persistence.TransferTaskInfo{
		Version:             s.version,
		DomainID:            s.domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		VisibilityTimestamp: now,
		TaskID:              taskID,
		TaskList:            taskListName,
		TaskType:            persistence.TransferTaskTypeActivityTask,
		ScheduleID:          event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, now)
	_, err = s.transferQueueStandbyProcessor.process(newTaskInfo(nil, transferTask, s.logger))
	s.Equal(ErrTaskRetry, err)
}

func (s *transferQueueStandbyProcessorSuite) TestProcessActivityTask_Pending_PushToMatching() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
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

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, nil, "some random identity")

	taskID := int64(59)
	activityID := "activity-1"
	activityType := "some random activity type"
	event, _ = addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID, activityType, taskListName, []byte{}, 1, 1, 1)

	now := time.Now()
	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.fetchHistoryDuration))
	transferTask := &persistence.TransferTaskInfo{
		Version:             s.version,
		DomainID:            s.domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		VisibilityTimestamp: now,
		TaskID:              taskID,
		TaskList:            taskListName,
		TaskType:            persistence.TransferTaskTypeActivityTask,
		ScheduleID:          event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.EXPECT().AddActivityTask(gomock.Any(), gomock.Any()).Return(nil).Times(1)

	s.mockShard.SetCurrentTime(s.clusterName, now)
	_, err = s.transferQueueStandbyProcessor.process(newTaskInfo(nil, transferTask, s.logger))
	s.Nil(err)
}

func (s *transferQueueStandbyProcessorSuite) TestProcessActivityTask_Success() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
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

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, nil, "some random identity")

	taskID := int64(59)
	activityID := "activity-1"
	activityType := "some random activity type"
	event, _ = addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID, activityType, taskListName, []byte{}, 1, 1, 1)

	now := time.Now()
	transferTask := &persistence.TransferTaskInfo{
		Version:             s.version,
		DomainID:            s.domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		VisibilityTimestamp: now,
		TaskID:              taskID,
		TaskList:            taskListName,
		TaskType:            persistence.TransferTaskTypeActivityTask,
		ScheduleID:          event.GetEventId(),
	}

	event = addActivityTaskStartedEvent(mutableState, event.GetEventId(), "")

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, now)
	_, err = s.transferQueueStandbyProcessor.process(newTaskInfo(nil, transferTask, s.logger))
	s.Nil(err)
}

func (s *transferQueueStandbyProcessorSuite) TestProcessDecisionTask_Pending() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
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
	di := addDecisionTaskScheduledEvent(mutableState)

	now := time.Now()
	transferTask := &persistence.TransferTaskInfo{
		Version:             s.version,
		DomainID:            s.domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		VisibilityTimestamp: now,
		TaskID:              taskID,
		TaskList:            taskListName,
		TaskType:            persistence.TransferTaskTypeDecisionTask,
		ScheduleID:          di.ScheduleID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, di.ScheduleID, di.Version)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, now)
	_, err = s.transferQueueStandbyProcessor.process(newTaskInfo(nil, transferTask, s.logger))
	s.Equal(ErrTaskRetry, err)
}

func (s *transferQueueStandbyProcessorSuite) TestProcessDecisionTask_Pending_PushToMatching() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
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
	di := addDecisionTaskScheduledEvent(mutableState)

	now := time.Now()
	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.fetchHistoryDuration))
	transferTask := &persistence.TransferTaskInfo{
		Version:             s.version,
		DomainID:            s.domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		VisibilityTimestamp: now,
		TaskID:              taskID,
		TaskList:            taskListName,
		TaskType:            persistence.TransferTaskTypeDecisionTask,
		ScheduleID:          di.ScheduleID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, di.ScheduleID, di.Version)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.EXPECT().AddDecisionTask(gomock.Any(), gomock.Any()).Return(nil).Times(1)

	s.mockShard.SetCurrentTime(s.clusterName, now)
	_, err = s.transferQueueStandbyProcessor.process(newTaskInfo(nil, transferTask, s.logger))
	s.Nil(nil, err)
}

func (s *transferQueueStandbyProcessorSuite) TestProcessDecisionTask_Success_FirstDecision() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
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
	di := addDecisionTaskScheduledEvent(mutableState)

	now := time.Now()
	transferTask := &persistence.TransferTaskInfo{
		Version:             s.version,
		DomainID:            s.domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		VisibilityTimestamp: now,
		TaskID:              taskID,
		TaskList:            taskListName,
		TaskType:            persistence.TransferTaskTypeDecisionTask,
		ScheduleID:          di.ScheduleID,
	}

	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, now)
	_, err = s.transferQueueStandbyProcessor.process(newTaskInfo(nil, transferTask, s.logger))
	s.Nil(err)
}

func (s *transferQueueStandbyProcessorSuite) TestProcessDecisionTask_Success_NonFirstDecision() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
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

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, nil, "some random identity")

	taskID := int64(59)
	di = addDecisionTaskScheduledEvent(mutableState)

	now := time.Now()
	transferTask := &persistence.TransferTaskInfo{
		Version:             s.version,
		DomainID:            s.domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		VisibilityTimestamp: now,
		TaskID:              taskID,
		TaskList:            taskListName,
		TaskType:            persistence.TransferTaskTypeDecisionTask,
		ScheduleID:          di.ScheduleID,
	}

	event = addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, now)
	_, err = s.transferQueueStandbyProcessor.process(newTaskInfo(nil, transferTask, s.logger))
	s.Nil(err)
}

func (s *transferQueueStandbyProcessorSuite) TestProcessCloseExecution() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
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

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, nil, "some random identity")

	taskID := int64(59)
	event = addCompleteWorkflowEvent(mutableState, event.GetEventId(), nil)

	now := time.Now()
	transferTask := &persistence.TransferTaskInfo{
		Version:             s.version,
		DomainID:            s.domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		VisibilityTimestamp: now,
		TaskID:              taskID,
		TaskList:            taskListName,
		TaskType:            persistence.TransferTaskTypeCloseExecution,
		ScheduleID:          event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockVisibilityMgr.On("RecordWorkflowExecutionClosed", mock.Anything).Return(nil).Once()
	s.mockArchivalMetadata.On("GetVisibilityConfig").Return(archiver.NewDisabledArchvialConfig())

	s.mockShard.SetCurrentTime(s.clusterName, now)
	_, err = s.transferQueueStandbyProcessor.process(newTaskInfo(nil, transferTask, s.logger))
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

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
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

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, nil, "some random identity")

	taskID := int64(59)
	event, _ = addRequestCancelInitiatedEvent(mutableState, event.GetEventId(), uuid.New(), testTargetDomainName, targetExecution.GetWorkflowId(), targetExecution.GetRunId())
	nextEventID := event.GetEventId() + 1

	now := time.Now()
	transferTask := &persistence.TransferTaskInfo{
		Version:             s.version,
		DomainID:            s.domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		VisibilityTimestamp: now,
		TargetDomainID:      testTargetDomainID,
		TargetWorkflowID:    targetExecution.GetWorkflowId(),
		TargetRunID:         targetExecution.GetRunId(),
		TaskID:              taskID,
		TaskList:            taskListName,
		TaskType:            persistence.TransferTaskTypeCancelExecution,
		ScheduleID:          event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, now)
	_, err = s.transferQueueStandbyProcessor.process(newTaskInfo(nil, transferTask, s.logger))
	s.Equal(ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.fetchHistoryDuration))
	s.mockHistoryRereplicator.On("SendMultiWorkflowHistory",
		transferTask.DomainID, transferTask.WorkflowID,
		transferTask.RunID, nextEventID,
		transferTask.RunID, common.EndEventID,
	).Return(nil).Once()
	_, err = s.transferQueueStandbyProcessor.process(newTaskInfo(nil, transferTask, s.logger))
	s.Equal(ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.discardDuration))
	_, err = s.transferQueueStandbyProcessor.process(newTaskInfo(nil, transferTask, s.logger))
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

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
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

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, nil, "some random identity")

	taskID := int64(59)
	event, _ = addRequestCancelInitiatedEvent(mutableState, event.GetEventId(), uuid.New(), testTargetDomainName, targetExecution.GetWorkflowId(), targetExecution.GetRunId())

	now := time.Now()
	transferTask := &persistence.TransferTaskInfo{
		Version:             s.version,
		DomainID:            s.domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		VisibilityTimestamp: now,
		TargetDomainID:      testTargetDomainID,
		TargetWorkflowID:    targetExecution.GetWorkflowId(),
		TargetRunID:         targetExecution.GetRunId(),
		TaskID:              taskID,
		TaskList:            taskListName,
		TaskType:            persistence.TransferTaskTypeCancelExecution,
		ScheduleID:          event.GetEventId(),
	}

	event = addCancelRequestedEvent(mutableState, event.GetEventId(), testTargetDomainID, targetExecution.GetWorkflowId(), targetExecution.GetRunId())

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, now)
	_, err = s.transferQueueStandbyProcessor.process(newTaskInfo(nil, transferTask, s.logger))
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

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
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

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, nil, "some random identity")

	taskID := int64(59)
	event, _ = addRequestSignalInitiatedEvent(mutableState, event.GetEventId(), uuid.New(),
		testTargetDomainName, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), signalName, nil, nil)
	nextEventID := event.GetEventId() + 1

	now := time.Now()
	transferTask := &persistence.TransferTaskInfo{
		Version:             s.version,
		DomainID:            s.domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		VisibilityTimestamp: now,
		TargetDomainID:      testTargetDomainID,
		TargetWorkflowID:    targetExecution.GetWorkflowId(),
		TargetRunID:         targetExecution.GetRunId(),
		TaskID:              taskID,
		TaskList:            taskListName,
		TaskType:            persistence.TransferTaskTypeSignalExecution,
		ScheduleID:          event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, now)
	_, err = s.transferQueueStandbyProcessor.process(newTaskInfo(nil, transferTask, s.logger))
	s.Equal(ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.fetchHistoryDuration))
	s.mockHistoryRereplicator.On("SendMultiWorkflowHistory",
		transferTask.DomainID, transferTask.WorkflowID,
		transferTask.RunID, nextEventID,
		transferTask.RunID, common.EndEventID,
	).Return(nil).Once()
	_, err = s.transferQueueStandbyProcessor.process(newTaskInfo(nil, transferTask, s.logger))
	s.Equal(ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.discardDuration))
	_, err = s.transferQueueStandbyProcessor.process(newTaskInfo(nil, transferTask, s.logger))
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

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
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

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, nil, "some random identity")

	taskID := int64(59)
	event, _ = addRequestSignalInitiatedEvent(mutableState, event.GetEventId(), uuid.New(),
		testTargetDomainName, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), signalName, nil, nil)

	now := time.Now()
	transferTask := &persistence.TransferTaskInfo{
		Version:             s.version,
		DomainID:            s.domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		VisibilityTimestamp: now,
		TargetDomainID:      testTargetDomainID,
		TargetWorkflowID:    targetExecution.GetWorkflowId(),
		TargetRunID:         targetExecution.GetRunId(),
		TaskID:              taskID,
		TaskList:            taskListName,
		TaskType:            persistence.TransferTaskTypeSignalExecution,
		ScheduleID:          event.GetEventId(),
	}

	event = addSignaledEvent(mutableState, event.GetEventId(), testTargetDomainName, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), nil)

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, now)
	_, err = s.transferQueueStandbyProcessor.process(newTaskInfo(nil, transferTask, s.logger))
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

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
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

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, nil, "some random identity")

	taskID := int64(59)
	event, _ = addStartChildWorkflowExecutionInitiatedEvent(mutableState, event.GetEventId(), uuid.New(),
		testChildDomainName, childWorkflowID, childWorkflowType, childTaskListName, nil, 1, 1)
	nextEventID := event.GetEventId() + 1

	now := time.Now()
	transferTask := &persistence.TransferTaskInfo{
		Version:             s.version,
		DomainID:            s.domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		VisibilityTimestamp: now,
		TargetDomainID:      testChildDomainID,
		TargetWorkflowID:    childWorkflowID,
		TargetRunID:         "",
		TaskID:              taskID,
		TaskList:            taskListName,
		TaskType:            persistence.TransferTaskTypeStartChildExecution,
		ScheduleID:          event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, now)
	_, err = s.transferQueueStandbyProcessor.process(newTaskInfo(nil, transferTask, s.logger))
	s.Equal(ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.fetchHistoryDuration))
	s.mockHistoryRereplicator.On("SendMultiWorkflowHistory",
		transferTask.DomainID, transferTask.WorkflowID,
		transferTask.RunID, nextEventID,
		transferTask.RunID, common.EndEventID,
	).Return(nil).Once()
	_, err = s.transferQueueStandbyProcessor.process(newTaskInfo(nil, transferTask, s.logger))
	s.Equal(ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.discardDuration))
	_, err = s.transferQueueStandbyProcessor.process(newTaskInfo(nil, transferTask, s.logger))
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

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
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

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, nil, "some random identity")

	taskID := int64(59)
	event, childInfo := addStartChildWorkflowExecutionInitiatedEvent(mutableState, event.GetEventId(), uuid.New(),
		testChildDomainName, childWorkflowID, childWorkflowType, childTaskListName, nil, 1, 1)

	now := time.Now()
	transferTask := &persistence.TransferTaskInfo{
		Version:             s.version,
		DomainID:            s.domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		VisibilityTimestamp: now,
		TargetDomainID:      testChildDomainID,
		TargetWorkflowID:    childWorkflowID,
		TargetRunID:         "",
		TaskID:              taskID,
		TaskList:            taskListName,
		TaskType:            persistence.TransferTaskTypeStartChildExecution,
		ScheduleID:          event.GetEventId(),
	}

	event = addChildWorkflowExecutionStartedEvent(mutableState, event.GetEventId(), testChildDomainName, childWorkflowID, uuid.New(), childWorkflowType)
	childInfo.StartedID = event.GetEventId()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, now)
	_, err = s.transferQueueStandbyProcessor.process(newTaskInfo(nil, transferTask, s.logger))
	s.Nil(err)
}

func (s *transferQueueStandbyProcessorSuite) TestProcessRecordWorkflowStartedTask() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	event, err := mutableState.AddWorkflowExecutionStartedEvent(
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

	di := addDecisionTaskScheduledEvent(mutableState)

	now := time.Now()
	transferTask := &persistence.TransferTaskInfo{
		Version:             s.version,
		DomainID:            s.domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		VisibilityTimestamp: now,
		TaskID:              taskID,
		TaskList:            taskListName,
		TaskType:            persistence.TransferTaskTypeRecordWorkflowStarted,
		ScheduleID:          event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, di.ScheduleID, di.Version)
	executionInfo := mutableState.GetExecutionInfo()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockVisibilityMgr.On("RecordWorkflowExecutionStarted", &persistence.RecordWorkflowExecutionStartedRequest{
		DomainUUID: testDomainID,
		Domain:     testDomainName,
		Execution: workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(executionInfo.WorkflowID),
			RunId:      common.StringPtr(executionInfo.RunID),
		},
		WorkflowTypeName: executionInfo.WorkflowTypeName,
		StartTimestamp:   event.GetTimestamp(),
		WorkflowTimeout:  int64(executionInfo.WorkflowTimeout),
		TaskID:           taskID,
	}).Return(nil).Once()

	s.mockShard.SetCurrentTime(s.clusterName, now)
	_, err = s.transferQueueStandbyProcessor.process(newTaskInfo(nil, transferTask, s.logger))
	s.Nil(err)
}

func (s *transferQueueStandbyProcessorSuite) TestProcessUpsertWorkflowSearchAttributesTask() {

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	event, err := mutableState.AddWorkflowExecutionStartedEvent(
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

	di := addDecisionTaskScheduledEvent(mutableState)

	now := time.Now()
	transferTask := &persistence.TransferTaskInfo{
		Version:             s.version,
		DomainID:            s.domainID,
		WorkflowID:          execution.GetWorkflowId(),
		RunID:               execution.GetRunId(),
		VisibilityTimestamp: now,
		TaskID:              taskID,
		TaskList:            taskListName,
		TaskType:            persistence.TransferTaskTypeUpsertWorkflowSearchAttributes,
		ScheduleID:          event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, di.ScheduleID, di.Version)
	executionInfo := mutableState.GetExecutionInfo()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockVisibilityMgr.On("UpsertWorkflowExecution", &persistence.UpsertWorkflowExecutionRequest{
		DomainUUID: testDomainID,
		Domain:     testDomainName,
		Execution: workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(executionInfo.WorkflowID),
			RunId:      common.StringPtr(executionInfo.RunID),
		},
		WorkflowTypeName: executionInfo.WorkflowTypeName,
		StartTimestamp:   event.GetTimestamp(),
		WorkflowTimeout:  int64(executionInfo.WorkflowTimeout),
		TaskID:           taskID,
	}).Return(nil).Once()

	s.mockShard.SetCurrentTime(s.clusterName, now)
	_, err = s.transferQueueStandbyProcessor.process(newTaskInfo(nil, transferTask, s.logger))
	s.Nil(err)
}

func (s *transferQueueStandbyProcessorSuite) createPersistenceMutableState(
	ms mutableState,
	lastEventID int64,
	lastEventVersion int64,
) *persistence.WorkflowMutableState {

	if ms.GetReplicationState() != nil {
		ms.UpdateReplicationStateLastEventID(lastEventVersion, lastEventID)
	} else if ms.GetVersionHistories() != nil {
		currentVersionHistory, err := ms.GetVersionHistories().GetCurrentVersionHistory()
		s.NoError(err)
		err = currentVersionHistory.AddOrUpdateItem(persistence.NewVersionHistoryItem(
			lastEventID, lastEventVersion,
		))
		s.NoError(err)
	}

	return createMutableState(ms)
}
