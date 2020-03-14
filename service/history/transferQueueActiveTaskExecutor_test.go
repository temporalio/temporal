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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/serviceerror"
	"go.temporal.io/temporal-proto/workflowservice"

	"github.com/temporalio/temporal/.gen/proto/historyservice"
	"github.com/temporalio/temporal/.gen/proto/historyservicemock"
	"github.com/temporalio/temporal/.gen/proto/matchingservice"
	"github.com/temporalio/temporal/.gen/proto/matchingservicemock"
	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/archiver"
	"github.com/temporalio/temporal/common/archiver/provider"
	"github.com/temporalio/temporal/common/backoff"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/clock"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/mocks"
	"github.com/temporalio/temporal/common/persistence"
	p "github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/primitives"
	dc "github.com/temporalio/temporal/common/service/dynamicconfig"
	warchiver "github.com/temporalio/temporal/service/worker/archiver"
	"github.com/temporalio/temporal/service/worker/parentclosepolicy"
)

type (
	transferQueueActiveTaskExecutorSuite struct {
		suite.Suite
		*require.Assertions

		controller               *gomock.Controller
		mockShard                *shardContextTest
		mockTxProcessor          *MocktransferQueueProcessor
		mockReplicationProcessor *MockReplicatorQueueProcessor
		mockTimerProcessor       *MocktimerQueueProcessor
		mockDomainCache          *cache.MockDomainCache
		mockMatchingClient       *matchingservicemock.MockMatchingServiceClient
		mockHistoryClient        *historyservicemock.MockHistoryServiceClient
		mockClusterMetadata      *cluster.MockMetadata

		mockVisibilityMgr           *mocks.VisibilityManager
		mockExecutionMgr            *mocks.ExecutionManager
		mockHistoryV2Mgr            *mocks.HistoryV2Manager
		mockQueueAckMgr             *MockQueueAckMgr
		mockArchivalClient          *warchiver.ClientMock
		mockArchivalMetadata        *archiver.MockArchivalMetadata
		mockArchiverProvider        *provider.MockArchiverProvider
		mockParentClosePolicyClient *parentclosepolicy.ClientMock

		logger                          log.Logger
		domainID                        string
		domainName                      string
		domainEntry                     *cache.DomainCacheEntry
		targetDomainID                  string
		targetDomainName                string
		targetDomainEntry               *cache.DomainCacheEntry
		childDomainID                   string
		childDomainName                 string
		childDomainEntry                *cache.DomainCacheEntry
		version                         int64
		now                             time.Time
		timeSource                      *clock.EventTimeSource
		transferQueueActiveTaskExecutor *transferQueueActiveTaskExecutor
	}
)

func TestTransferQueueActiveTaskExecutorSuite(t *testing.T) {
	s := new(transferQueueActiveTaskExecutorSuite)
	suite.Run(t, s)
}

func (s *transferQueueActiveTaskExecutorSuite) SetupSuite() {

}

func (s *transferQueueActiveTaskExecutorSuite) TearDownSuite() {

}

func (s *transferQueueActiveTaskExecutorSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.domainID = testDomainID
	s.domainName = testDomainName
	s.domainEntry = testGlobalDomainEntry
	s.targetDomainID = testTargetDomainID
	s.targetDomainName = testTargetDomainName
	s.targetDomainEntry = testGlobalTargetDomainEntry
	s.childDomainID = testChildDomainID
	s.childDomainName = testChildDomainName
	s.childDomainEntry = testGlobalChildDomainEntry
	s.version = s.domainEntry.GetFailoverVersion()
	s.now = time.Now()
	s.timeSource = clock.NewEventTimeSource().Update(s.now)

	s.controller = gomock.NewController(s.T())
	s.mockTxProcessor = NewMocktransferQueueProcessor(s.controller)
	s.mockReplicationProcessor = NewMockReplicatorQueueProcessor(s.controller)
	s.mockTimerProcessor = NewMocktimerQueueProcessor(s.controller)
	s.mockTxProcessor.EXPECT().NotifyNewTask(gomock.Any(), gomock.Any()).AnyTimes()
	s.mockReplicationProcessor.EXPECT().notifyNewTask().AnyTimes()
	s.mockTimerProcessor.EXPECT().NotifyNewTimers(gomock.Any(), gomock.Any()).AnyTimes()

	config := NewDynamicConfigForTest()
	s.mockShard = newTestShardContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistenceblobs.ShardInfo{
				ShardID:          0,
				RangeID:          1,
				TransferAckLevel: 0,
			}},
		NewDynamicConfigForTest(),
	)
	s.mockShard.eventsCache = newEventsCache(s.mockShard)
	s.mockShard.resource.TimeSource = s.timeSource

	s.mockParentClosePolicyClient = &parentclosepolicy.ClientMock{}
	s.mockArchivalClient = &warchiver.ClientMock{}
	s.mockMatchingClient = s.mockShard.resource.MatchingClient
	s.mockHistoryClient = s.mockShard.resource.HistoryClient
	s.mockExecutionMgr = s.mockShard.resource.ExecutionMgr
	s.mockHistoryV2Mgr = s.mockShard.resource.HistoryMgr
	s.mockVisibilityMgr = s.mockShard.resource.VisibilityMgr
	s.mockClusterMetadata = s.mockShard.resource.ClusterMetadata
	s.mockArchivalMetadata = s.mockShard.resource.ArchivalMetadata
	s.mockArchiverProvider = s.mockShard.resource.ArchiverProvider
	s.mockDomainCache = s.mockShard.resource.DomainCache
	s.mockDomainCache.EXPECT().GetDomainByID(testDomainID).Return(testGlobalDomainEntry, nil).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomain(testDomainName).Return(testGlobalDomainEntry, nil).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomainByID(testTargetDomainID).Return(testGlobalTargetDomainEntry, nil).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomain(testTargetDomainName).Return(testGlobalTargetDomainEntry, nil).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomainByID(testParentDomainID).Return(testGlobalParentDomainEntry, nil).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomain(testParentDomainName).Return(testGlobalParentDomainEntry, nil).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomainByID(testChildDomainID).Return(testGlobalChildDomainEntry, nil).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomain(testChildDomainName).Return(testGlobalChildDomainEntry, nil).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsGlobalDomainEnabled().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.version).Return(s.mockClusterMetadata.GetCurrentClusterName()).AnyTimes()

	s.logger = s.mockShard.GetLogger()

	historyCache := newHistoryCache(s.mockShard)
	h := &historyEngineImpl{
		currentClusterName:   s.mockShard.GetService().GetClusterMetadata().GetCurrentClusterName(),
		shard:                s.mockShard,
		clusterMetadata:      s.mockClusterMetadata,
		historyV2Mgr:         s.mockHistoryV2Mgr,
		executionManager:     s.mockExecutionMgr,
		historyCache:         historyCache,
		logger:               s.logger,
		tokenSerializer:      common.NewProtoTaskTokenSerializer(),
		metricsClient:        s.mockShard.GetMetricsClient(),
		historyEventNotifier: newHistoryEventNotifier(clock.NewRealTimeSource(), metrics.NewClient(tally.NoopScope, metrics.History), func(string) int { return 0 }),
		txProcessor:          s.mockTxProcessor,
		replicatorProcessor:  s.mockReplicationProcessor,
		timerProcessor:       s.mockTimerProcessor,
		archivalClient:       s.mockArchivalClient,
	}
	s.mockShard.SetEngine(h)

	s.mockQueueAckMgr = &MockQueueAckMgr{}
	s.transferQueueActiveTaskExecutor = newTransferQueueActiveTaskExecutor(
		s.mockShard,
		h,
		s.logger,
		s.mockShard.GetMetricsClient(),
		config,
	).(*transferQueueActiveTaskExecutor)
	s.transferQueueActiveTaskExecutor.parentClosePolicyClient = s.mockParentClosePolicyClient
}

func (s *transferQueueActiveTaskExecutorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.Finish(s.T())
	s.mockQueueAckMgr.AssertExpectations(s.T())
	s.mockArchivalClient.AssertExpectations(s.T())
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessActivityTask_Success() {

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			DomainUUID: s.domainID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonproto.WorkflowType{Name: workflowType},
				TaskList:                            &commonproto.TaskList{Name: taskListName},
				ExecutionStartToCloseTimeoutSeconds: 2,
				TaskStartToCloseTimeoutSeconds:      1,
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
	event, ai := addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID, activityType, taskListName, []byte{}, 1, 1, 1)

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:        s.version,
		DomainID:       s.GetDomainIDBytes(),
		TargetDomainID: primitives.MustParseUUID(testTargetDomainID),
		WorkflowID:     execution.GetWorkflowId(),
		RunID:          primitives.MustParseUUID(execution.GetRunId()),
		TaskID:         taskID,
		TaskList:       taskListName,
		TaskType:       persistence.TransferTaskTypeActivityTask,
		ScheduleID:     event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.EXPECT().AddActivityTask(gomock.Any(), s.createAddActivityTaskRequest(transferTask, ai)).Return(&matchingservice.AddActivityTaskResponse{}, nil).Times(1)

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) GetDomainIDBytes() primitives.UUID {
	return primitives.MustParseUUID(s.domainID)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessActivityTask_Duplication() {

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			DomainUUID: s.domainID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonproto.WorkflowType{Name: workflowType},
				TaskList:                            &commonproto.TaskList{Name: taskListName},
				ExecutionStartToCloseTimeoutSeconds: 2,
				TaskStartToCloseTimeoutSeconds:      1,
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
	event, ai := addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID, activityType, taskListName, []byte{}, 1, 1, 1)

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:        s.version,
		DomainID:       s.GetDomainIDBytes(),
		TargetDomainID: primitives.MustParseUUID(s.targetDomainID),
		WorkflowID:     execution.GetWorkflowId(),
		RunID:          primitives.MustParseUUID(execution.GetRunId()),
		TaskID:         taskID,
		TaskList:       taskListName,
		TaskType:       persistence.TransferTaskTypeActivityTask,
		ScheduleID:     event.GetEventId(),
	}

	event = addActivityTaskStartedEvent(mutableState, event.GetEventId(), "")
	ai.StartedID = event.GetEventId()
	event = addActivityTaskCompletedEvent(mutableState, ai.ScheduleID, ai.StartedID, nil, "")

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessDecisionTask_FirstDecision() {

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			DomainUUID: s.domainID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonproto.WorkflowType{Name: workflowType},
				TaskList:                            &commonproto.TaskList{Name: taskListName},
				ExecutionStartToCloseTimeoutSeconds: 2,
				TaskStartToCloseTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	taskID := int64(59)
	di := addDecisionTaskScheduledEvent(mutableState)

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:    s.version,
		DomainID:   s.GetDomainIDBytes(),
		WorkflowID: execution.GetWorkflowId(),
		RunID:      primitives.MustParseUUID(execution.GetRunId()),
		TaskID:     taskID,
		TaskList:   taskListName,
		TaskType:   persistence.TransferTaskTypeDecisionTask,
		ScheduleID: di.ScheduleID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, di.ScheduleID, di.Version)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.EXPECT().AddDecisionTask(gomock.Any(), s.createAddDecisionTaskRequest(transferTask, mutableState)).Return(&matchingservice.AddDecisionTaskResponse{}, nil).Times(1)

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessDecisionTask_NonFirstDecision() {

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			DomainUUID: s.domainID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonproto.WorkflowType{Name: workflowType},
				TaskList:                            &commonproto.TaskList{Name: taskListName},
				ExecutionStartToCloseTimeoutSeconds: 2,
				TaskStartToCloseTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, nil, "some random identity")
	s.NotNil(event)

	// make another round of decision
	taskID := int64(59)
	di = addDecisionTaskScheduledEvent(mutableState)

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:    s.version,
		DomainID:   s.GetDomainIDBytes(),
		WorkflowID: execution.GetWorkflowId(),
		RunID:      primitives.MustParseUUID(execution.GetRunId()),
		TaskID:     taskID,
		TaskList:   taskListName,
		TaskType:   persistence.TransferTaskTypeDecisionTask,
		ScheduleID: di.ScheduleID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, di.ScheduleID, di.Version)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.EXPECT().AddDecisionTask(gomock.Any(), s.createAddDecisionTaskRequest(transferTask, mutableState)).Return(&matchingservice.AddDecisionTaskResponse{}, nil).Times(1)

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessDecisionTask_Sticky_NonFirstDecision() {

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"
	stickyTaskListName := "some random sticky task list"
	stickyTaskListTimeout := int32(233)

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			DomainUUID: s.domainID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonproto.WorkflowType{Name: workflowType},
				TaskList:                            &commonproto.TaskList{Name: taskListName},
				ExecutionStartToCloseTimeoutSeconds: 2,
				TaskStartToCloseTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, nil, "some random identity")
	s.NotNil(event)
	// set the sticky tasklist attr
	executionInfo := mutableState.GetExecutionInfo()
	executionInfo.StickyTaskList = stickyTaskListName
	executionInfo.StickyScheduleToStartTimeout = stickyTaskListTimeout

	// make another round of decision
	taskID := int64(59)
	di = addDecisionTaskScheduledEvent(mutableState)

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:    s.version,
		DomainID:   s.GetDomainIDBytes(),
		WorkflowID: execution.GetWorkflowId(),
		RunID:      primitives.MustParseUUID(execution.GetRunId()),
		TaskID:     taskID,
		TaskList:   stickyTaskListName,
		TaskType:   persistence.TransferTaskTypeDecisionTask,
		ScheduleID: di.ScheduleID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, di.ScheduleID, di.Version)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.EXPECT().AddDecisionTask(gomock.Any(), s.createAddDecisionTaskRequest(transferTask, mutableState)).Return(&matchingservice.AddDecisionTaskResponse{}, nil).Times(1)

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessDecisionTask_DecisionNotSticky_MutableStateSticky() {

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"
	stickyTaskListName := "some random sticky task list"
	stickyTaskListTimeout := int32(233)

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			DomainUUID: s.domainID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonproto.WorkflowType{Name: workflowType},
				TaskList:                            &commonproto.TaskList{Name: taskListName},
				ExecutionStartToCloseTimeoutSeconds: 2,
				TaskStartToCloseTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, nil, "some random identity")
	s.NotNil(event)
	// set the sticky tasklist attr
	executionInfo := mutableState.GetExecutionInfo()
	executionInfo.StickyTaskList = stickyTaskListName
	executionInfo.StickyScheduleToStartTimeout = stickyTaskListTimeout

	// make another round of decision
	taskID := int64(59)
	di = addDecisionTaskScheduledEvent(mutableState)

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:    s.version,
		DomainID:   s.GetDomainIDBytes(),
		WorkflowID: execution.GetWorkflowId(),
		RunID:      primitives.MustParseUUID(execution.GetRunId()),
		TaskID:     taskID,
		TaskList:   taskListName,
		TaskType:   persistence.TransferTaskTypeDecisionTask,
		ScheduleID: di.ScheduleID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, di.ScheduleID, di.Version)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.EXPECT().AddDecisionTask(gomock.Any(), s.createAddDecisionTaskRequest(transferTask, mutableState)).Return(&matchingservice.AddDecisionTaskResponse{}, nil).Times(1)

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessDecisionTask_Duplication() {

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			DomainUUID: s.domainID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonproto.WorkflowType{Name: workflowType},
				TaskList:                            &commonproto.TaskList{Name: taskListName},
				ExecutionStartToCloseTimeoutSeconds: 2,
				TaskStartToCloseTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	taskID := int64(4096)
	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, nil, "some random identity")

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:    s.version,
		DomainID:   s.GetDomainIDBytes(),
		WorkflowID: execution.GetWorkflowId(),
		RunID:      primitives.MustParseUUID(execution.GetRunId()),
		TaskID:     taskID,
		TaskList:   taskListName,
		TaskType:   persistence.TransferTaskTypeDecisionTask,
		ScheduleID: di.ScheduleID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessCloseExecution_HasParent() {

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	parentDomainID := "some random parent domain ID"
	parentInitiatedID := int64(3222)
	parentDomainName := "some random parent domain Name"
	parentExecution := &commonproto.WorkflowExecution{
		WorkflowId: "some random parent workflow ID",
		RunId:      uuid.New(),
	}

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			DomainUUID: s.domainID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonproto.WorkflowType{Name: workflowType},
				TaskList:                            &commonproto.TaskList{Name: taskListName},
				ExecutionStartToCloseTimeoutSeconds: 2,
				TaskStartToCloseTimeoutSeconds:      1,
			},
			ParentExecutionInfo: &commonproto.ParentExecutionInfo{
				DomainUUID:  parentDomainID,
				Domain:      parentDomainName,
				Execution:   parentExecution,
				InitiatedId: parentInitiatedID,
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

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:    s.version,
		DomainID:   s.GetDomainIDBytes(),
		WorkflowID: execution.GetWorkflowId(),
		RunID:      primitives.MustParseUUID(execution.GetRunId()),
		TaskID:     taskID,
		TaskList:   taskListName,
		TaskType:   persistence.TransferTaskTypeCloseExecution,
		ScheduleID: event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().RecordChildExecutionCompleted(gomock.Any(), &historyservice.RecordChildExecutionCompletedRequest{
		DomainUUID:         parentDomainID,
		WorkflowExecution:  parentExecution,
		InitiatedId:        parentInitiatedID,
		CompletedExecution: &execution,
		CompletionEvent:    event,
	}).Return(nil, nil).Times(1)
	s.mockVisibilityMgr.On("RecordWorkflowExecutionClosed", mock.Anything).Return(nil).Once()
	s.mockArchivalMetadata.On("GetVisibilityConfig").Return(archiver.NewDisabledArchvialConfig())

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessCloseExecution_NoParent() {

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			DomainUUID: s.domainID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonproto.WorkflowType{Name: workflowType},
				TaskList:                            &commonproto.TaskList{Name: taskListName},
				ExecutionStartToCloseTimeoutSeconds: 2,
				TaskStartToCloseTimeoutSeconds:      1,
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

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:    s.version,
		DomainID:   s.GetDomainIDBytes(),
		WorkflowID: execution.GetWorkflowId(),
		RunID:      primitives.MustParseUUID(execution.GetRunId()),
		TaskID:     taskID,
		TaskList:   taskListName,
		TaskType:   persistence.TransferTaskTypeCloseExecution,
		ScheduleID: event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockVisibilityMgr.On("RecordWorkflowExecutionClosed", mock.Anything).Return(nil).Once()
	s.mockArchivalMetadata.On("GetVisibilityConfig").Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "random URI"))
	s.mockArchivalClient.On("Archive", mock.Anything, mock.Anything).Return(nil, nil).Once()

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessCloseExecution_NoParent_HasFewChildren() {

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			DomainUUID: s.domainID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonproto.WorkflowType{Name: workflowType},
				TaskList:                            &commonproto.TaskList{Name: taskListName},
				ExecutionStartToCloseTimeoutSeconds: 2,
				TaskStartToCloseTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()

	dt := enums.DecisionTypeStartChildWorkflowExecution
	parentClosePolicy1 := enums.ParentClosePolicyAbandon
	parentClosePolicy2 := enums.ParentClosePolicyTerminate
	parentClosePolicy3 := enums.ParentClosePolicyRequestCancel

	event, _ = mutableState.AddDecisionTaskCompletedEvent(di.ScheduleID, di.StartedID, &workflowservice.RespondDecisionTaskCompletedRequest{
		ExecutionContext: nil,
		Identity:         "some random identity",
		Decisions: []*commonproto.Decision{
			{
				DecisionType: dt,
				Attributes: &commonproto.Decision_StartChildWorkflowExecutionDecisionAttributes{StartChildWorkflowExecutionDecisionAttributes: &commonproto.StartChildWorkflowExecutionDecisionAttributes{
					WorkflowId: "child workflow1",
					WorkflowType: &commonproto.WorkflowType{
						Name: "child workflow type",
					},
					TaskList:          &commonproto.TaskList{Name: taskListName},
					Input:             []byte("random input"),
					ParentClosePolicy: parentClosePolicy1,
				}},
			},
			{
				DecisionType: dt,
				Attributes: &commonproto.Decision_StartChildWorkflowExecutionDecisionAttributes{StartChildWorkflowExecutionDecisionAttributes: &commonproto.StartChildWorkflowExecutionDecisionAttributes{
					WorkflowId: "child workflow2",
					WorkflowType: &commonproto.WorkflowType{
						Name: "child workflow type",
					},
					TaskList:          &commonproto.TaskList{Name: taskListName},
					Input:             []byte("random input"),
					ParentClosePolicy: parentClosePolicy2,
				}},
			},
			{
				DecisionType: dt,
				Attributes: &commonproto.Decision_StartChildWorkflowExecutionDecisionAttributes{StartChildWorkflowExecutionDecisionAttributes: &commonproto.StartChildWorkflowExecutionDecisionAttributes{
					WorkflowId: "child workflow3",
					WorkflowType: &commonproto.WorkflowType{
						Name: "child workflow type",
					},
					TaskList:          &commonproto.TaskList{Name: taskListName},
					Input:             []byte("random input"),
					ParentClosePolicy: parentClosePolicy3,
				}},
			},
		},
	}, defaultHistoryMaxAutoResetPoints)

	_, _, err = mutableState.AddStartChildWorkflowExecutionInitiatedEvent(event.GetEventId(), uuid.New(), &commonproto.StartChildWorkflowExecutionDecisionAttributes{
		WorkflowId: "child workflow1",
		WorkflowType: &commonproto.WorkflowType{
			Name: "child workflow type",
		},
		TaskList:          &commonproto.TaskList{Name: taskListName},
		Input:             []byte("random input"),
		ParentClosePolicy: parentClosePolicy1,
	})
	s.Nil(err)
	_, _, err = mutableState.AddStartChildWorkflowExecutionInitiatedEvent(event.GetEventId(), uuid.New(), &commonproto.StartChildWorkflowExecutionDecisionAttributes{
		WorkflowId: "child workflow2",
		WorkflowType: &commonproto.WorkflowType{
			Name: "child workflow type",
		},
		TaskList:          &commonproto.TaskList{Name: taskListName},
		Input:             []byte("random input"),
		ParentClosePolicy: parentClosePolicy2,
	})
	s.Nil(err)
	_, _, err = mutableState.AddStartChildWorkflowExecutionInitiatedEvent(event.GetEventId(), uuid.New(), &commonproto.StartChildWorkflowExecutionDecisionAttributes{
		WorkflowId: "child workflow3",
		WorkflowType: &commonproto.WorkflowType{
			Name: "child workflow type",
		},
		TaskList:          &commonproto.TaskList{Name: taskListName},
		Input:             []byte("random input"),
		ParentClosePolicy: parentClosePolicy3,
	})
	s.Nil(err)

	s.NoError(mutableState.FlushBufferedEvents())

	taskID := int64(59)
	event = addCompleteWorkflowEvent(mutableState, event.GetEventId(), nil)

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:    s.version,
		DomainID:   s.GetDomainIDBytes(),
		WorkflowID: execution.GetWorkflowId(),
		RunID:      primitives.MustParseUUID(execution.GetRunId()),
		TaskID:     taskID,
		TaskList:   taskListName,
		TaskType:   persistence.TransferTaskTypeCloseExecution,
		ScheduleID: event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockVisibilityMgr.On("RecordWorkflowExecutionClosed", mock.Anything).Return(nil).Once()
	s.mockArchivalMetadata.On("GetVisibilityConfig").Return(archiver.NewDisabledArchvialConfig())
	s.mockHistoryClient.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)
	s.mockHistoryClient.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessCloseExecution_NoParent_HasManyChildren() {

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			DomainUUID: s.domainID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonproto.WorkflowType{Name: workflowType},
				TaskList:                            &commonproto.TaskList{Name: taskListName},
				ExecutionStartToCloseTimeoutSeconds: 2,
				TaskStartToCloseTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()

	dt := enums.DecisionTypeStartChildWorkflowExecution
	parentClosePolicy := enums.ParentClosePolicyTerminate
	var decisions []*commonproto.Decision
	for i := 0; i < 10; i++ {
		decisions = append(decisions, &commonproto.Decision{
			DecisionType: dt,
			Attributes: &commonproto.Decision_StartChildWorkflowExecutionDecisionAttributes{StartChildWorkflowExecutionDecisionAttributes: &commonproto.StartChildWorkflowExecutionDecisionAttributes{
				WorkflowId: "child workflow" + string(i),
				WorkflowType: &commonproto.WorkflowType{
					Name: "child workflow type",
				},
				TaskList:          &commonproto.TaskList{Name: taskListName},
				Input:             []byte("random input"),
				ParentClosePolicy: parentClosePolicy,
			}},
		})
	}

	event, _ = mutableState.AddDecisionTaskCompletedEvent(di.ScheduleID, di.StartedID, &workflowservice.RespondDecisionTaskCompletedRequest{
		ExecutionContext: nil,
		Identity:         "some random identity",
		Decisions:        decisions,
	}, defaultHistoryMaxAutoResetPoints)

	for i := 0; i < 10; i++ {
		_, _, err = mutableState.AddStartChildWorkflowExecutionInitiatedEvent(event.GetEventId(), uuid.New(), &commonproto.StartChildWorkflowExecutionDecisionAttributes{
			WorkflowId: "child workflow" + string(i),
			WorkflowType: &commonproto.WorkflowType{
				Name: "child workflow type",
			},
			TaskList:          &commonproto.TaskList{Name: taskListName},
			Input:             []byte("random input"),
			ParentClosePolicy: parentClosePolicy,
		})
		s.Nil(err)
	}

	s.NoError(mutableState.FlushBufferedEvents())

	taskID := int64(59)
	event = addCompleteWorkflowEvent(mutableState, event.GetEventId(), nil)

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:    s.version,
		DomainID:   s.GetDomainIDBytes(),
		WorkflowID: execution.GetWorkflowId(),
		RunID:      primitives.MustParseUUID(execution.GetRunId()),
		TaskID:     taskID,
		TaskList:   taskListName,
		TaskType:   persistence.TransferTaskTypeCloseExecution,
		ScheduleID: event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockVisibilityMgr.On("RecordWorkflowExecutionClosed", mock.Anything).Return(nil).Once()
	s.mockArchivalMetadata.On("GetVisibilityConfig").Return(archiver.NewDisabledArchvialConfig())
	s.mockParentClosePolicyClient.On("SendParentClosePolicyRequest", mock.Anything).Return(nil).Times(1)

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessCloseExecution_NoParent_HasManyAbandonedChildren() {

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			DomainUUID: s.domainID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonproto.WorkflowType{Name: workflowType},
				TaskList:                            &commonproto.TaskList{Name: taskListName},
				ExecutionStartToCloseTimeoutSeconds: 2,
				TaskStartToCloseTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()

	dt := enums.DecisionTypeStartChildWorkflowExecution
	parentClosePolicy := enums.ParentClosePolicyAbandon
	var decisions []*commonproto.Decision
	for i := 0; i < 10; i++ {
		decisions = append(decisions, &commonproto.Decision{
			DecisionType: dt,
			Attributes: &commonproto.Decision_StartChildWorkflowExecutionDecisionAttributes{StartChildWorkflowExecutionDecisionAttributes: &commonproto.StartChildWorkflowExecutionDecisionAttributes{
				WorkflowId: "child workflow" + string(i),
				WorkflowType: &commonproto.WorkflowType{
					Name: "child workflow type",
				},
				TaskList:          &commonproto.TaskList{Name: taskListName},
				Input:             []byte("random input"),
				ParentClosePolicy: parentClosePolicy,
			}},
		})
	}

	event, _ = mutableState.AddDecisionTaskCompletedEvent(di.ScheduleID, di.StartedID, &workflowservice.RespondDecisionTaskCompletedRequest{
		ExecutionContext: nil,
		Identity:         "some random identity",
		Decisions:        decisions,
	}, defaultHistoryMaxAutoResetPoints)

	for i := 0; i < 10; i++ {
		_, _, err = mutableState.AddStartChildWorkflowExecutionInitiatedEvent(event.GetEventId(), uuid.New(), &commonproto.StartChildWorkflowExecutionDecisionAttributes{
			WorkflowId: "child workflow" + string(i),
			WorkflowType: &commonproto.WorkflowType{
				Name: "child workflow type",
			},
			TaskList:          &commonproto.TaskList{Name: taskListName},
			Input:             []byte("random input"),
			ParentClosePolicy: parentClosePolicy,
		})
		s.Nil(err)
	}

	s.NoError(mutableState.FlushBufferedEvents())

	taskID := int64(59)
	event = addCompleteWorkflowEvent(mutableState, event.GetEventId(), nil)

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:    s.version,
		DomainID:   s.GetDomainIDBytes(),
		WorkflowID: execution.GetWorkflowId(),
		RunID:      primitives.MustParseUUID(execution.GetRunId()),
		TaskID:     taskID,
		TaskList:   taskListName,
		TaskType:   persistence.TransferTaskTypeCloseExecution,
		ScheduleID: event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockVisibilityMgr.On("RecordWorkflowExecutionClosed", mock.Anything).Return(nil).Once()
	s.mockArchivalMetadata.On("GetVisibilityConfig").Return(archiver.NewDisabledArchvialConfig())

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessCancelExecution_Success() {

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	targetExecution := commonproto.WorkflowExecution{
		WorkflowId: "some random target workflow ID",
		RunId:      uuid.New(),
	}

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			DomainUUID: s.domainID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonproto.WorkflowType{Name: workflowType},
				TaskList:                            &commonproto.TaskList{Name: taskListName},
				ExecutionStartToCloseTimeoutSeconds: 2,
				TaskStartToCloseTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, nil, "some random identity")

	taskID := int64(59)
	event, rci := addRequestCancelInitiatedEvent(mutableState, event.GetEventId(), uuid.New(), testTargetDomainName, targetExecution.GetWorkflowId(), targetExecution.GetRunId())

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:          s.version,
		DomainID:         s.GetDomainIDBytes(),
		WorkflowID:       execution.GetWorkflowId(),
		RunID:            primitives.MustParseUUID(execution.GetRunId()),
		TargetDomainID:   primitives.MustParseUUID(s.targetDomainID),
		TargetWorkflowID: targetExecution.GetWorkflowId(),
		TargetRunID:      primitives.MustParseUUID(targetExecution.GetRunId()),
		TaskID:           taskID,
		TaskList:         taskListName,
		TaskType:         persistence.TransferTaskTypeCancelExecution,
		ScheduleID:       event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), s.createRequestCancelWorkflowExecutionRequest(s.targetDomainName, transferTask, rci)).Return(nil, nil).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.version).Return(cluster.TestCurrentClusterName).AnyTimes()

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessCancelExecution_Failure() {

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	targetExecution := commonproto.WorkflowExecution{
		WorkflowId: "some random target workflow ID",
		RunId:      uuid.New(),
	}

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			DomainUUID: s.domainID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonproto.WorkflowType{Name: workflowType},
				TaskList:                            &commonproto.TaskList{Name: taskListName},
				ExecutionStartToCloseTimeoutSeconds: 2,
				TaskStartToCloseTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, nil, "some random identity")

	taskID := int64(59)
	event, rci := addRequestCancelInitiatedEvent(mutableState, event.GetEventId(), uuid.New(), testTargetDomainName, targetExecution.GetWorkflowId(), targetExecution.GetRunId())

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:          s.version,
		DomainID:         s.GetDomainIDBytes(),
		WorkflowID:       execution.GetWorkflowId(),
		RunID:            primitives.MustParseUUID(execution.GetRunId()),
		TargetDomainID:   primitives.MustParseUUID(s.targetDomainID),
		TargetWorkflowID: targetExecution.GetWorkflowId(),
		TargetRunID:      primitives.MustParseUUID(targetExecution.GetRunId()),
		TaskID:           taskID,
		TaskList:         taskListName,
		TaskType:         persistence.TransferTaskTypeCancelExecution,
		ScheduleID:       event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), s.createRequestCancelWorkflowExecutionRequest(s.targetDomainName, transferTask, rci)).Return(nil, serviceerror.NewNotFound("")).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.version).Return(cluster.TestCurrentClusterName).AnyTimes()

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessCancelExecution_Duplication() {

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	targetExecution := commonproto.WorkflowExecution{
		WorkflowId: "some random target workflow ID",
		RunId:      uuid.New(),
	}

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			DomainUUID: s.domainID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonproto.WorkflowType{Name: workflowType},
				TaskList:                            &commonproto.TaskList{Name: taskListName},
				ExecutionStartToCloseTimeoutSeconds: 2,
				TaskStartToCloseTimeoutSeconds:      1,
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

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:          s.version,
		DomainID:         s.GetDomainIDBytes(),
		WorkflowID:       execution.GetWorkflowId(),
		RunID:            primitives.MustParseUUID(execution.GetRunId()),
		TargetDomainID:   primitives.MustParseUUID(s.targetDomainID),
		TargetWorkflowID: targetExecution.GetWorkflowId(),
		TargetRunID:      primitives.MustParseUUID(targetExecution.GetRunId()),
		TaskID:           taskID,
		TaskList:         taskListName,
		TaskType:         persistence.TransferTaskTypeCancelExecution,
		ScheduleID:       event.GetEventId(),
	}

	event = addCancelRequestedEvent(mutableState, event.GetEventId(), testTargetDomainID, targetExecution.GetWorkflowId(), targetExecution.GetRunId())

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessSignalExecution_Success() {

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	targetExecution := commonproto.WorkflowExecution{
		WorkflowId: "some random target workflow ID",
		RunId:      uuid.New(),
	}
	signalName := "some random signal name"
	signalInput := []byte("some random signal input")
	signalControl := []byte("some random signal control")

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			DomainUUID: s.domainID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonproto.WorkflowType{Name: workflowType},
				TaskList:                            &commonproto.TaskList{Name: taskListName},
				ExecutionStartToCloseTimeoutSeconds: 2,
				TaskStartToCloseTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, nil, "some random identity")

	taskID := int64(59)
	event, si := addRequestSignalInitiatedEvent(mutableState, event.GetEventId(), uuid.New(),
		testTargetDomainName, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), signalName, signalInput, signalControl)

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:          s.version,
		DomainID:         s.GetDomainIDBytes(),
		WorkflowID:       execution.GetWorkflowId(),
		RunID:            primitives.MustParseUUID(execution.GetRunId()),
		TargetDomainID:   primitives.MustParseUUID(s.targetDomainID),
		TargetWorkflowID: targetExecution.GetWorkflowId(),
		TargetRunID:      primitives.MustParseUUID(targetExecution.GetRunId()),
		TaskID:           taskID,
		TaskList:         taskListName,
		TaskType:         persistence.TransferTaskTypeSignalExecution,
		ScheduleID:       event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().SignalWorkflowExecution(gomock.Any(), s.createSignalWorkflowExecutionRequest(s.targetDomainName, transferTask, si)).Return(nil, nil).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.version).Return(cluster.TestCurrentClusterName).AnyTimes()

	s.mockHistoryClient.EXPECT().RemoveSignalMutableState(gomock.Any(), &historyservice.RemoveSignalMutableStateRequest{
		DomainUUID: primitives.UUID(transferTask.TargetDomainID).String(),
		WorkflowExecution: &commonproto.WorkflowExecution{
			WorkflowId: transferTask.TargetWorkflowID,
			RunId:      primitives.UUID(transferTask.TargetRunID).String(),
		},
		RequestId: si.RequestID,
	}).Return(nil, nil).Times(1)

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessSignalExecution_Failure() {

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	targetExecution := commonproto.WorkflowExecution{
		WorkflowId: "some random target workflow ID",
		RunId:      uuid.New(),
	}
	signalName := "some random signal name"
	signalInput := []byte("some random signal input")
	signalControl := []byte("some random signal control")

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			DomainUUID: s.domainID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonproto.WorkflowType{Name: workflowType},
				TaskList:                            &commonproto.TaskList{Name: taskListName},
				ExecutionStartToCloseTimeoutSeconds: 2,
				TaskStartToCloseTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, nil, "some random identity")

	taskID := int64(59)
	event, si := addRequestSignalInitiatedEvent(mutableState, event.GetEventId(), uuid.New(),
		testTargetDomainName, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), signalName, signalInput, signalControl)

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:          s.version,
		DomainID:         s.GetDomainIDBytes(),
		WorkflowID:       execution.GetWorkflowId(),
		RunID:            primitives.MustParseUUID(execution.GetRunId()),
		TargetDomainID:   primitives.MustParseUUID(s.targetDomainID),
		TargetWorkflowID: targetExecution.GetWorkflowId(),
		TargetRunID:      primitives.MustParseUUID(targetExecution.GetRunId()),
		TaskID:           taskID,
		TaskList:         taskListName,
		TaskType:         persistence.TransferTaskTypeSignalExecution,
		ScheduleID:       event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().SignalWorkflowExecution(gomock.Any(), s.createSignalWorkflowExecutionRequest(s.targetDomainName, transferTask, si)).Return(nil, serviceerror.NewNotFound("")).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.version).Return(cluster.TestCurrentClusterName).AnyTimes()

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessSignalExecution_Duplication() {

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	targetExecution := commonproto.WorkflowExecution{
		WorkflowId: "some random target workflow ID",
		RunId:      uuid.New(),
	}
	signalName := "some random signal name"
	signalInput := []byte("some random signal input")
	signalControl := []byte("some random signal control")

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			DomainUUID: s.domainID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonproto.WorkflowType{Name: workflowType},
				TaskList:                            &commonproto.TaskList{Name: taskListName},
				ExecutionStartToCloseTimeoutSeconds: 2,
				TaskStartToCloseTimeoutSeconds:      1,
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
		testTargetDomainName, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), signalName, signalInput, signalControl)

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:          s.version,
		DomainID:         s.GetDomainIDBytes(),
		WorkflowID:       execution.GetWorkflowId(),
		RunID:            primitives.MustParseUUID(execution.GetRunId()),
		TargetDomainID:   primitives.MustParseUUID(s.targetDomainID),
		TargetWorkflowID: targetExecution.GetWorkflowId(),
		TargetRunID:      primitives.MustParseUUID(targetExecution.GetRunId()),
		TaskID:           taskID,
		TaskList:         taskListName,
		TaskType:         persistence.TransferTaskTypeSignalExecution,
		ScheduleID:       event.GetEventId(),
	}

	event = addSignaledEvent(mutableState, event.GetEventId(), testTargetDomainName, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), nil)

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessStartChildExecution_Success() {

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	childWorkflowID := "some random child workflow ID"
	childRunID := uuid.New()
	childWorkflowType := "some random child workflow type"
	childTaskListName := "some random child task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			DomainUUID: s.domainID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonproto.WorkflowType{Name: workflowType},
				TaskList:                            &commonproto.TaskList{Name: taskListName},
				ExecutionStartToCloseTimeoutSeconds: 2,
				TaskStartToCloseTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, nil, "some random identity")

	taskID := int64(59)

	event, ci := addStartChildWorkflowExecutionInitiatedEvent(mutableState, event.GetEventId(), uuid.New(),
		s.childDomainName, childWorkflowID, childWorkflowType, childTaskListName, nil, 1, 1)

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:          s.version,
		DomainID:         s.GetDomainIDBytes(),
		WorkflowID:       execution.GetWorkflowId(),
		RunID:            primitives.MustParseUUID(execution.GetRunId()),
		TargetDomainID:   primitives.MustParseUUID(testChildDomainID),
		TargetWorkflowID: childWorkflowID,
		TargetRunID:      nil,
		TaskID:           taskID,
		TaskList:         taskListName,
		TaskType:         persistence.TransferTaskTypeStartChildExecution,
		ScheduleID:       event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().StartWorkflowExecution(gomock.Any(), s.createChildWorkflowExecutionRequest(
		s.domainName,
		s.childDomainName,
		transferTask,
		mutableState,
		ci,
	)).Return(&historyservice.StartWorkflowExecutionResponse{RunId: childRunID}, nil).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.version).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockHistoryClient.EXPECT().ScheduleDecisionTask(gomock.Any(), &historyservice.ScheduleDecisionTaskRequest{
		DomainUUID: testChildDomainID,
		WorkflowExecution: &commonproto.WorkflowExecution{
			WorkflowId: childWorkflowID,
			RunId:      childRunID,
		},
		IsFirstDecision: true,
	}).Return(nil, nil).Times(1)

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessStartChildExecution_Failure() {

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	childWorkflowID := "some random child workflow ID"
	childWorkflowType := "some random child workflow type"
	childTaskListName := "some random child task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			DomainUUID: s.domainID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonproto.WorkflowType{Name: workflowType},
				TaskList:                            &commonproto.TaskList{Name: taskListName},
				ExecutionStartToCloseTimeoutSeconds: 2,
				TaskStartToCloseTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, nil, "some random identity")

	taskID := int64(59)

	event, ci := addStartChildWorkflowExecutionInitiatedEvent(
		mutableState,
		event.GetEventId(),
		uuid.New(),
		s.childDomainName,
		childWorkflowID,
		childWorkflowType,
		childTaskListName,
		nil,
		1,
		1,
	)

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:          s.version,
		DomainID:         s.GetDomainIDBytes(),
		WorkflowID:       execution.GetWorkflowId(),
		RunID:            primitives.MustParseUUID(execution.GetRunId()),
		TargetDomainID:   primitives.MustParseUUID(testChildDomainID),
		TargetWorkflowID: childWorkflowID,
		TargetRunID:      nil,
		TaskID:           taskID,
		TaskList:         taskListName,
		TaskType:         persistence.TransferTaskTypeStartChildExecution,
		ScheduleID:       event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().StartWorkflowExecution(gomock.Any(), s.createChildWorkflowExecutionRequest(
		s.domainName,
		s.childDomainName,
		transferTask,
		mutableState,
		ci,
	)).Return(nil, serviceerror.NewWorkflowExecutionAlreadyStarted("msg", "", "")).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.version).Return(cluster.TestCurrentClusterName).AnyTimes()

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessStartChildExecution_Success_Dup() {

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	childWorkflowID := "some random child workflow ID"
	childRunID := uuid.New()
	childWorkflowType := "some random child workflow type"
	childTaskListName := "some random child task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			DomainUUID: s.domainID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonproto.WorkflowType{Name: workflowType},
				TaskList:                            &commonproto.TaskList{Name: taskListName},
				ExecutionStartToCloseTimeoutSeconds: 2,
				TaskStartToCloseTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, nil, "some random identity")

	taskID := int64(59)

	event, ci := addStartChildWorkflowExecutionInitiatedEvent(
		mutableState,
		event.GetEventId(),
		uuid.New(),
		s.childDomainName,
		childWorkflowID,
		childWorkflowType,
		childTaskListName,
		nil,
		1,
		1,
	)

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:          s.version,
		DomainID:         s.GetDomainIDBytes(),
		WorkflowID:       execution.GetWorkflowId(),
		RunID:            primitives.MustParseUUID(execution.GetRunId()),
		TargetDomainID:   primitives.MustParseUUID(testChildDomainID),
		TargetWorkflowID: childWorkflowID,
		TargetRunID:      nil,
		TaskID:           taskID,
		TaskList:         taskListName,
		TaskType:         persistence.TransferTaskTypeStartChildExecution,
		ScheduleID:       event.GetEventId(),
	}

	event = addChildWorkflowExecutionStartedEvent(mutableState, event.GetEventId(), testChildDomainID, childWorkflowID, childRunID, childWorkflowType)
	ci.StartedID = event.GetEventId()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().ScheduleDecisionTask(gomock.Any(), &historyservice.ScheduleDecisionTaskRequest{
		DomainUUID: testChildDomainID,
		WorkflowExecution: &commonproto.WorkflowExecution{
			WorkflowId: childWorkflowID,
			RunId:      childRunID,
		},
		IsFirstDecision: true,
	}).Return(nil, nil).Times(1)

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessStartChildExecution_Duplication() {

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	childExecution := commonproto.WorkflowExecution{
		WorkflowId: "some random child workflow ID",
		RunId:      uuid.New(),
	}
	childWorkflowType := "some random child workflow type"
	childTaskListName := "some random child task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			DomainUUID: s.domainID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonproto.WorkflowType{Name: workflowType},
				TaskList:                            &commonproto.TaskList{Name: taskListName},
				ExecutionStartToCloseTimeoutSeconds: 2,
				TaskStartToCloseTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, nil, "some random identity")

	taskID := int64(59)

	event, ci := addStartChildWorkflowExecutionInitiatedEvent(
		mutableState,
		event.GetEventId(),
		uuid.New(),
		s.childDomainName,
		childExecution.GetWorkflowId(),
		childWorkflowType,
		childTaskListName,
		nil,
		1,
		1,
	)

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:          s.version,
		DomainID:         s.GetDomainIDBytes(),
		WorkflowID:       execution.GetWorkflowId(),
		RunID:            primitives.MustParseUUID(execution.GetRunId()),
		TargetDomainID:   primitives.MustParseUUID(testChildDomainID),
		TargetWorkflowID: childExecution.GetWorkflowId(),
		TargetRunID:      nil,
		TaskID:           taskID,
		TaskList:         taskListName,
		TaskType:         persistence.TransferTaskTypeStartChildExecution,
		ScheduleID:       event.GetEventId(),
	}

	event = addChildWorkflowExecutionStartedEvent(mutableState, event.GetEventId(), testChildDomainID, childExecution.GetWorkflowId(), childExecution.GetRunId(), childWorkflowType)
	ci.StartedID = event.GetEventId()
	event = addChildWorkflowExecutionCompletedEvent(mutableState, ci.InitiatedID, &childExecution, &commonproto.WorkflowExecutionCompletedEventAttributes{
		Result:                       []byte("some random child workflow execution result"),
		DecisionTaskCompletedEventId: transferTask.ScheduleID,
	})

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessRecordWorkflowStartedTask() {

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"
	cronSchedule := "@every 5s"
	backoffSeconds := int32(5)

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())

	event, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			DomainUUID: s.domainID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonproto.WorkflowType{Name: workflowType},
				TaskList:                            &commonproto.TaskList{Name: taskListName},
				ExecutionStartToCloseTimeoutSeconds: 2,
				TaskStartToCloseTimeoutSeconds:      1,
				CronSchedule:                        cronSchedule,
			},
			FirstDecisionTaskBackoffSeconds: backoffSeconds,
		},
	)
	s.Nil(err)

	taskID := int64(59)
	di := addDecisionTaskScheduledEvent(mutableState)

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:    s.version,
		DomainID:   s.GetDomainIDBytes(),
		WorkflowID: execution.GetWorkflowId(),
		RunID:      primitives.MustParseUUID(execution.GetRunId()),
		TaskID:     taskID,
		TaskList:   taskListName,
		TaskType:   persistence.TransferTaskTypeRecordWorkflowStarted,
		ScheduleID: event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, di.ScheduleID, di.Version)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockVisibilityMgr.On("RecordWorkflowExecutionStarted", s.createRecordWorkflowExecutionStartedRequest(s.domainName, event, transferTask, mutableState, backoffSeconds)).Once().Return(nil)

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessUpsertWorkflowSearchAttributes() {

	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())

	event, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			DomainUUID: s.domainID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonproto.WorkflowType{Name: workflowType},
				TaskList:                            &commonproto.TaskList{Name: taskListName},
				ExecutionStartToCloseTimeoutSeconds: 2,
				TaskStartToCloseTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	taskID := int64(59)
	di := addDecisionTaskScheduledEvent(mutableState)

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:    s.version,
		DomainID:   s.GetDomainIDBytes(),
		WorkflowID: execution.GetWorkflowId(),
		RunID:      primitives.MustParseUUID(execution.GetRunId()),
		TaskID:     taskID,
		TaskList:   taskListName,
		TaskType:   persistence.TransferTaskTypeUpsertWorkflowSearchAttributes,
		ScheduleID: event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, di.ScheduleID, di.Version)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockVisibilityMgr.On("UpsertWorkflowExecution", s.createUpsertWorkflowSearchAttributesRequest(s.domainName, event, transferTask, mutableState)).Once().Return(nil)

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestCopySearchAttributes() {
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

func (s *transferQueueActiveTaskExecutorSuite) createAddActivityTaskRequest(
	task *persistenceblobs.TransferTaskInfo,
	ai *persistence.ActivityInfo,
) *matchingservice.AddActivityTaskRequest {
	return &matchingservice.AddActivityTaskRequest{
		DomainUUID:       primitives.UUID(task.TargetDomainID).String(),
		SourceDomainUUID: primitives.UUID(task.DomainID).String(),
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: task.WorkflowID,
			RunId:      primitives.UUID(task.RunID).String(),
		},
		TaskList:                      &commonproto.TaskList{Name: task.TaskList},
		ScheduleId:                    task.ScheduleID,
		ScheduleToStartTimeoutSeconds: ai.ScheduleToStartTimeout,
	}
}

func (s *transferQueueActiveTaskExecutorSuite) createAddDecisionTaskRequest(
	task *persistenceblobs.TransferTaskInfo,
	mutableState mutableState,
) *matchingservice.AddDecisionTaskRequest {

	execution := commonproto.WorkflowExecution{
		WorkflowId: task.WorkflowID,
		RunId:      primitives.UUID(task.RunID).String(),
	}
	taskList := &commonproto.TaskList{Name: task.TaskList}
	executionInfo := mutableState.GetExecutionInfo()
	timeout := executionInfo.WorkflowTimeout
	if mutableState.GetExecutionInfo().TaskList != task.TaskList {
		taskList.Kind = enums.TaskListKindSticky
		timeout = executionInfo.StickyScheduleToStartTimeout
	}

	return &matchingservice.AddDecisionTaskRequest{
		DomainUUID:                    primitives.UUID(task.DomainID).String(),
		Execution:                     &execution,
		TaskList:                      taskList,
		ScheduleId:                    task.ScheduleID,
		ScheduleToStartTimeoutSeconds: timeout,
	}
}

func (s *transferQueueActiveTaskExecutorSuite) createRecordWorkflowExecutionStartedRequest(
	domainName string,
	startEvent *commonproto.HistoryEvent,
	task *persistenceblobs.TransferTaskInfo,
	mutableState mutableState,
	backoffSeconds int32,
) *persistence.RecordWorkflowExecutionStartedRequest {
	execution := &commonproto.WorkflowExecution{
		WorkflowId: task.WorkflowID,
		RunId:      primitives.UUID(task.RunID).String(),
	}
	executionInfo := mutableState.GetExecutionInfo()
	executionTimestamp := time.Unix(0, startEvent.GetTimestamp()).Add(time.Duration(backoffSeconds) * time.Second)

	return &persistence.RecordWorkflowExecutionStartedRequest{
		Domain:             domainName,
		DomainUUID:         primitives.UUID(task.DomainID).String(),
		Execution:          *execution,
		WorkflowTypeName:   executionInfo.WorkflowTypeName,
		StartTimestamp:     startEvent.GetTimestamp(),
		ExecutionTimestamp: executionTimestamp.UnixNano(),
		WorkflowTimeout:    int64(executionInfo.WorkflowTimeout),
		TaskID:             task.TaskID,
	}
}

func (s *transferQueueActiveTaskExecutorSuite) createRequestCancelWorkflowExecutionRequest(
	targetDomainName string,
	task *persistenceblobs.TransferTaskInfo,
	rci *persistenceblobs.RequestCancelInfo,
) *historyservice.RequestCancelWorkflowExecutionRequest {

	sourceExecution := commonproto.WorkflowExecution{
		WorkflowId: task.WorkflowID,
		RunId:      primitives.UUID(task.RunID).String(),
	}
	targetExecution := commonproto.WorkflowExecution{
		WorkflowId: task.TargetWorkflowID,
		RunId:      primitives.UUID(task.TargetRunID).String(),
	}

	return &historyservice.RequestCancelWorkflowExecutionRequest{
		DomainUUID: primitives.UUID(task.TargetDomainID).String(),
		CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{
			Domain:            targetDomainName,
			WorkflowExecution: &targetExecution,
			Identity:          identityHistoryService,
			// Use the same request ID to dedupe RequestCancelWorkflowExecution calls
			RequestId: rci.CancelRequestID,
		},
		ExternalInitiatedEventId:  task.ScheduleID,
		ExternalWorkflowExecution: &sourceExecution,
		ChildWorkflowOnly:         task.TargetChildWorkflowOnly,
	}
}

func (s *transferQueueActiveTaskExecutorSuite) createSignalWorkflowExecutionRequest(
	targetDomainName string,
	task *persistenceblobs.TransferTaskInfo,
	si *persistenceblobs.SignalInfo,
) *historyservice.SignalWorkflowExecutionRequest {

	sourceExecution := commonproto.WorkflowExecution{
		WorkflowId: task.WorkflowID,
		RunId:      primitives.UUID(task.RunID).String(),
	}
	targetExecution := commonproto.WorkflowExecution{
		WorkflowId: task.TargetWorkflowID,
		RunId:      primitives.UUID(task.TargetRunID).String(),
	}

	return &historyservice.SignalWorkflowExecutionRequest{
		DomainUUID: primitives.UUID(task.TargetDomainID).String(),
		SignalRequest: &workflowservice.SignalWorkflowExecutionRequest{
			Domain:            targetDomainName,
			WorkflowExecution: &targetExecution,
			Identity:          identityHistoryService,
			SignalName:        si.Name,
			Input:             si.Input,
			RequestId:         si.RequestID,
			Control:           si.Control,
		},
		ExternalWorkflowExecution: &sourceExecution,
		ChildWorkflowOnly:         task.TargetChildWorkflowOnly,
	}
}

func (s *transferQueueActiveTaskExecutorSuite) createChildWorkflowExecutionRequest(
	domainName string,
	childDomainName string,
	task *persistenceblobs.TransferTaskInfo,
	mutableState mutableState,
	ci *persistence.ChildExecutionInfo,
) *historyservice.StartWorkflowExecutionRequest {

	event, err := mutableState.GetChildExecutionInitiatedEvent(task.ScheduleID)
	s.NoError(err)
	attributes := event.GetStartChildWorkflowExecutionInitiatedEventAttributes()
	execution := commonproto.WorkflowExecution{
		WorkflowId: task.WorkflowID,
		RunId:      primitives.UUID(task.RunID).String(),
	}
	now := time.Now()
	return &historyservice.StartWorkflowExecutionRequest{
		DomainUUID: primitives.UUID(task.TargetDomainID).String(),
		StartRequest: &workflowservice.StartWorkflowExecutionRequest{
			Domain:                              childDomainName,
			WorkflowId:                          attributes.WorkflowId,
			WorkflowType:                        attributes.WorkflowType,
			TaskList:                            attributes.TaskList,
			Input:                               attributes.Input,
			ExecutionStartToCloseTimeoutSeconds: attributes.ExecutionStartToCloseTimeoutSeconds,
			TaskStartToCloseTimeoutSeconds:      attributes.TaskStartToCloseTimeoutSeconds,
			// Use the same request ID to dedupe StartWorkflowExecution calls
			RequestId:             ci.CreateRequestID,
			WorkflowIdReusePolicy: attributes.WorkflowIdReusePolicy,
		},
		ParentExecutionInfo: &commonproto.ParentExecutionInfo{
			DomainUUID:  primitives.UUID(task.DomainID).String(),
			Domain:      testDomainName,
			Execution:   &execution,
			InitiatedId: task.ScheduleID,
		},
		FirstDecisionTaskBackoffSeconds: backoff.GetBackoffForNextScheduleInSeconds(attributes.GetCronSchedule(), now, now),
	}
}

func (s *transferQueueActiveTaskExecutorSuite) createUpsertWorkflowSearchAttributesRequest(
	domainName string,
	startEvent *commonproto.HistoryEvent,
	task *persistenceblobs.TransferTaskInfo,
	mutableState mutableState,
) *persistence.UpsertWorkflowExecutionRequest {

	execution := &commonproto.WorkflowExecution{
		WorkflowId: task.WorkflowID,
		RunId:      primitives.UUID(task.RunID).String(),
	}
	executionInfo := mutableState.GetExecutionInfo()

	return &persistence.UpsertWorkflowExecutionRequest{
		Domain:           domainName,
		DomainUUID:       primitives.UUID(task.DomainID).String(),
		Execution:        *execution,
		WorkflowTypeName: executionInfo.WorkflowTypeName,
		StartTimestamp:   startEvent.GetTimestamp(),
		WorkflowTimeout:  int64(executionInfo.WorkflowTimeout),
		TaskID:           task.TaskID,
	}
}

func (s *transferQueueActiveTaskExecutorSuite) createPersistenceMutableState(
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
