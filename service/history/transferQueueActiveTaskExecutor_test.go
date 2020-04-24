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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	commonpb "go.temporal.io/temporal-proto/common"
	decisionpb "go.temporal.io/temporal-proto/decision"
	eventpb "go.temporal.io/temporal-proto/event"
	executionpb "go.temporal.io/temporal-proto/execution"
	"go.temporal.io/temporal-proto/serviceerror"
	tasklistpb "go.temporal.io/temporal-proto/tasklist"
	"go.temporal.io/temporal-proto/workflowservice"

	executiongenpb "github.com/temporalio/temporal/.gen/proto/execution"
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
	"github.com/temporalio/temporal/common/payload"
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
		mockNamespaceCache       *cache.MockNamespaceCache
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
		namespaceID                     string
		namespace                       string
		namespaceEntry                  *cache.NamespaceCacheEntry
		targetNamespaceID               string
		targetNamespace                 string
		targetNamespaceEntry            *cache.NamespaceCacheEntry
		childNamespaceID                string
		childNamespace                  string
		childNamespaceEntry             *cache.NamespaceCacheEntry
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

	s.namespaceID = testNamespaceID
	s.namespace = testNamespace
	s.namespaceEntry = testGlobalNamespaceEntry
	s.targetNamespaceID = testTargetNamespaceID
	s.targetNamespace = testTargetNamespace
	s.targetNamespaceEntry = testGlobalTargetNamespaceEntry
	s.childNamespaceID = testChildNamespaceID
	s.childNamespace = testChildNamespace
	s.childNamespaceEntry = testGlobalChildNamespaceEntry
	s.version = s.namespaceEntry.GetFailoverVersion()
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
				ShardId:          0,
				RangeId:          1,
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
	s.mockNamespaceCache = s.mockShard.resource.NamespaceCache
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(testNamespaceID).Return(testGlobalNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(testNamespace).Return(testGlobalNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(testTargetNamespaceID).Return(testGlobalTargetNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(testTargetNamespace).Return(testGlobalTargetNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(testParentNamespaceID).Return(testGlobalParentNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(testParentNamespace).Return(testGlobalParentNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(testChildNamespaceID).Return(testGlobalChildNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(testChildNamespace).Return(testGlobalChildNamespaceEntry, nil).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
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

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonpb.WorkflowType{Name: workflowType},
				TaskList:                            &tasklistpb.TaskList{Name: taskListName},
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
	event, ai := addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID, activityType, taskListName, &commonpb.Payload{}, 1, 1, 1, 1)

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:           s.version,
		NamespaceId:       s.GetNamespaceIDBytes(),
		TargetNamespaceId: primitives.MustParseUUID(testTargetNamespaceID),
		WorkflowId:        execution.GetWorkflowId(),
		RunId:             primitives.MustParseUUID(execution.GetRunId()),
		TaskId:            taskID,
		TaskList:          taskListName,
		TaskType:          persistence.TransferTaskTypeActivityTask,
		ScheduleId:        event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.EXPECT().AddActivityTask(gomock.Any(), s.createAddActivityTaskRequest(transferTask, ai)).Return(&matchingservice.AddActivityTaskResponse{}, nil).Times(1)

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) GetNamespaceIDBytes() primitives.UUID {
	return primitives.MustParseUUID(s.namespaceID)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessActivityTask_Duplication() {

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonpb.WorkflowType{Name: workflowType},
				TaskList:                            &tasklistpb.TaskList{Name: taskListName},
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
	event, ai := addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID, activityType, taskListName, &commonpb.Payload{}, 1, 1, 1, 1)

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:           s.version,
		NamespaceId:       s.GetNamespaceIDBytes(),
		TargetNamespaceId: primitives.MustParseUUID(s.targetNamespaceID),
		WorkflowId:        execution.GetWorkflowId(),
		RunId:             primitives.MustParseUUID(execution.GetRunId()),
		TaskId:            taskID,
		TaskList:          taskListName,
		TaskType:          persistence.TransferTaskTypeActivityTask,
		ScheduleId:        event.GetEventId(),
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

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonpb.WorkflowType{Name: workflowType},
				TaskList:                            &tasklistpb.TaskList{Name: taskListName},
				ExecutionStartToCloseTimeoutSeconds: 2,
				TaskStartToCloseTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	taskID := int64(59)
	di := addDecisionTaskScheduledEvent(mutableState)

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:     s.version,
		NamespaceId: s.GetNamespaceIDBytes(),
		WorkflowId:  execution.GetWorkflowId(),
		RunId:       primitives.MustParseUUID(execution.GetRunId()),
		TaskId:      taskID,
		TaskList:    taskListName,
		TaskType:    persistence.TransferTaskTypeDecisionTask,
		ScheduleId:  di.ScheduleID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, di.ScheduleID, di.Version)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.EXPECT().AddDecisionTask(gomock.Any(), s.createAddDecisionTaskRequest(transferTask, mutableState)).Return(&matchingservice.AddDecisionTaskResponse{}, nil).Times(1)

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessDecisionTask_NonFirstDecision() {

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonpb.WorkflowType{Name: workflowType},
				TaskList:                            &tasklistpb.TaskList{Name: taskListName},
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
		Version:     s.version,
		NamespaceId: s.GetNamespaceIDBytes(),
		WorkflowId:  execution.GetWorkflowId(),
		RunId:       primitives.MustParseUUID(execution.GetRunId()),
		TaskId:      taskID,
		TaskList:    taskListName,
		TaskType:    persistence.TransferTaskTypeDecisionTask,
		ScheduleId:  di.ScheduleID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, di.ScheduleID, di.Version)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.EXPECT().AddDecisionTask(gomock.Any(), s.createAddDecisionTaskRequest(transferTask, mutableState)).Return(&matchingservice.AddDecisionTaskResponse{}, nil).Times(1)

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessDecisionTask_Sticky_NonFirstDecision() {

	execution := executionpb.WorkflowExecution{
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
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonpb.WorkflowType{Name: workflowType},
				TaskList:                            &tasklistpb.TaskList{Name: taskListName},
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
		Version:     s.version,
		NamespaceId: s.GetNamespaceIDBytes(),
		WorkflowId:  execution.GetWorkflowId(),
		RunId:       primitives.MustParseUUID(execution.GetRunId()),
		TaskId:      taskID,
		TaskList:    stickyTaskListName,
		TaskType:    persistence.TransferTaskTypeDecisionTask,
		ScheduleId:  di.ScheduleID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, di.ScheduleID, di.Version)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.EXPECT().AddDecisionTask(gomock.Any(), s.createAddDecisionTaskRequest(transferTask, mutableState)).Return(&matchingservice.AddDecisionTaskResponse{}, nil).Times(1)

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessDecisionTask_DecisionNotSticky_MutableStateSticky() {

	execution := executionpb.WorkflowExecution{
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
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonpb.WorkflowType{Name: workflowType},
				TaskList:                            &tasklistpb.TaskList{Name: taskListName},
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
		Version:     s.version,
		NamespaceId: s.GetNamespaceIDBytes(),
		WorkflowId:  execution.GetWorkflowId(),
		RunId:       primitives.MustParseUUID(execution.GetRunId()),
		TaskId:      taskID,
		TaskList:    taskListName,
		TaskType:    persistence.TransferTaskTypeDecisionTask,
		ScheduleId:  di.ScheduleID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, di.ScheduleID, di.Version)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.EXPECT().AddDecisionTask(gomock.Any(), s.createAddDecisionTaskRequest(transferTask, mutableState)).Return(&matchingservice.AddDecisionTaskResponse{}, nil).Times(1)

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessDecisionTask_Duplication() {

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonpb.WorkflowType{Name: workflowType},
				TaskList:                            &tasklistpb.TaskList{Name: taskListName},
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
		Version:     s.version,
		NamespaceId: s.GetNamespaceIDBytes(),
		WorkflowId:  execution.GetWorkflowId(),
		RunId:       primitives.MustParseUUID(execution.GetRunId()),
		TaskId:      taskID,
		TaskList:    taskListName,
		TaskType:    persistence.TransferTaskTypeDecisionTask,
		ScheduleId:  di.ScheduleID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessCloseExecution_HasParent() {

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	parentNamespaceID := "some random parent namespace ID"
	parentInitiatedID := int64(3222)
	parentNamespace := "some random parent namespace Name"
	parentExecution := &executionpb.WorkflowExecution{
		WorkflowId: "some random parent workflow ID",
		RunId:      uuid.New(),
	}

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonpb.WorkflowType{Name: workflowType},
				TaskList:                            &tasklistpb.TaskList{Name: taskListName},
				ExecutionStartToCloseTimeoutSeconds: 2,
				TaskStartToCloseTimeoutSeconds:      1,
			},
			ParentExecutionInfo: &executiongenpb.ParentExecutionInfo{
				NamespaceId: parentNamespaceID,
				Namespace:   parentNamespace,
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
		Version:     s.version,
		NamespaceId: s.GetNamespaceIDBytes(),
		WorkflowId:  execution.GetWorkflowId(),
		RunId:       primitives.MustParseUUID(execution.GetRunId()),
		TaskId:      taskID,
		TaskList:    taskListName,
		TaskType:    persistence.TransferTaskTypeCloseExecution,
		ScheduleId:  event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().RecordChildExecutionCompleted(gomock.Any(), &historyservice.RecordChildExecutionCompletedRequest{
		NamespaceId:        parentNamespaceID,
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

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonpb.WorkflowType{Name: workflowType},
				TaskList:                            &tasklistpb.TaskList{Name: taskListName},
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
		Version:     s.version,
		NamespaceId: s.GetNamespaceIDBytes(),
		WorkflowId:  execution.GetWorkflowId(),
		RunId:       primitives.MustParseUUID(execution.GetRunId()),
		TaskId:      taskID,
		TaskList:    taskListName,
		TaskType:    persistence.TransferTaskTypeCloseExecution,
		ScheduleId:  event.GetEventId(),
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

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonpb.WorkflowType{Name: workflowType},
				TaskList:                            &tasklistpb.TaskList{Name: taskListName},
				ExecutionStartToCloseTimeoutSeconds: 2,
				TaskStartToCloseTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()

	dt := decisionpb.DecisionType_StartChildWorkflowExecution
	parentClosePolicy1 := commonpb.ParentClosePolicy_Abandon
	parentClosePolicy2 := commonpb.ParentClosePolicy_Terminate
	parentClosePolicy3 := commonpb.ParentClosePolicy_RequestCancel

	event, _ = mutableState.AddDecisionTaskCompletedEvent(di.ScheduleID, di.StartedID, &workflowservice.RespondDecisionTaskCompletedRequest{
		ExecutionContext: nil,
		Identity:         "some random identity",
		Decisions: []*decisionpb.Decision{
			{
				DecisionType: dt,
				Attributes: &decisionpb.Decision_StartChildWorkflowExecutionDecisionAttributes{StartChildWorkflowExecutionDecisionAttributes: &decisionpb.StartChildWorkflowExecutionDecisionAttributes{
					WorkflowId: "child workflow1",
					WorkflowType: &commonpb.WorkflowType{
						Name: "child workflow type",
					},
					TaskList:          &tasklistpb.TaskList{Name: taskListName},
					Input:             payload.EncodeString("random input"),
					ParentClosePolicy: parentClosePolicy1,
				}},
			},
			{
				DecisionType: dt,
				Attributes: &decisionpb.Decision_StartChildWorkflowExecutionDecisionAttributes{StartChildWorkflowExecutionDecisionAttributes: &decisionpb.StartChildWorkflowExecutionDecisionAttributes{
					WorkflowId: "child workflow2",
					WorkflowType: &commonpb.WorkflowType{
						Name: "child workflow type",
					},
					TaskList:          &tasklistpb.TaskList{Name: taskListName},
					Input:             payload.EncodeString("random input"),
					ParentClosePolicy: parentClosePolicy2,
				}},
			},
			{
				DecisionType: dt,
				Attributes: &decisionpb.Decision_StartChildWorkflowExecutionDecisionAttributes{StartChildWorkflowExecutionDecisionAttributes: &decisionpb.StartChildWorkflowExecutionDecisionAttributes{
					WorkflowId: "child workflow3",
					WorkflowType: &commonpb.WorkflowType{
						Name: "child workflow type",
					},
					TaskList:          &tasklistpb.TaskList{Name: taskListName},
					Input:             payload.EncodeString("random input"),
					ParentClosePolicy: parentClosePolicy3,
				}},
			},
		},
	}, defaultHistoryMaxAutoResetPoints)

	_, _, err = mutableState.AddStartChildWorkflowExecutionInitiatedEvent(event.GetEventId(), uuid.New(), &decisionpb.StartChildWorkflowExecutionDecisionAttributes{
		WorkflowId: "child workflow1",
		WorkflowType: &commonpb.WorkflowType{
			Name: "child workflow type",
		},
		TaskList:          &tasklistpb.TaskList{Name: taskListName},
		Input:             payload.EncodeString("random input"),
		ParentClosePolicy: parentClosePolicy1,
	})
	s.Nil(err)
	_, _, err = mutableState.AddStartChildWorkflowExecutionInitiatedEvent(event.GetEventId(), uuid.New(), &decisionpb.StartChildWorkflowExecutionDecisionAttributes{
		WorkflowId: "child workflow2",
		WorkflowType: &commonpb.WorkflowType{
			Name: "child workflow type",
		},
		TaskList:          &tasklistpb.TaskList{Name: taskListName},
		Input:             payload.EncodeString("random input"),
		ParentClosePolicy: parentClosePolicy2,
	})
	s.Nil(err)
	_, _, err = mutableState.AddStartChildWorkflowExecutionInitiatedEvent(event.GetEventId(), uuid.New(), &decisionpb.StartChildWorkflowExecutionDecisionAttributes{
		WorkflowId: "child workflow3",
		WorkflowType: &commonpb.WorkflowType{
			Name: "child workflow type",
		},
		TaskList:          &tasklistpb.TaskList{Name: taskListName},
		Input:             payload.EncodeString("random input"),
		ParentClosePolicy: parentClosePolicy3,
	})
	s.Nil(err)

	s.NoError(mutableState.FlushBufferedEvents())

	taskID := int64(59)
	event = addCompleteWorkflowEvent(mutableState, event.GetEventId(), nil)

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:     s.version,
		NamespaceId: s.GetNamespaceIDBytes(),
		WorkflowId:  execution.GetWorkflowId(),
		RunId:       primitives.MustParseUUID(execution.GetRunId()),
		TaskId:      taskID,
		TaskList:    taskListName,
		TaskType:    persistence.TransferTaskTypeCloseExecution,
		ScheduleId:  event.GetEventId(),
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

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonpb.WorkflowType{Name: workflowType},
				TaskList:                            &tasklistpb.TaskList{Name: taskListName},
				ExecutionStartToCloseTimeoutSeconds: 2,
				TaskStartToCloseTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()

	dt := decisionpb.DecisionType_StartChildWorkflowExecution
	parentClosePolicy := commonpb.ParentClosePolicy_Terminate
	var decisions []*decisionpb.Decision
	for i := 0; i < 10; i++ {
		decisions = append(decisions, &decisionpb.Decision{
			DecisionType: dt,
			Attributes: &decisionpb.Decision_StartChildWorkflowExecutionDecisionAttributes{StartChildWorkflowExecutionDecisionAttributes: &decisionpb.StartChildWorkflowExecutionDecisionAttributes{
				WorkflowId: "child workflow" + string(i),
				WorkflowType: &commonpb.WorkflowType{
					Name: "child workflow type",
				},
				TaskList:          &tasklistpb.TaskList{Name: taskListName},
				Input:             payload.EncodeString("random input"),
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
		_, _, err = mutableState.AddStartChildWorkflowExecutionInitiatedEvent(event.GetEventId(), uuid.New(), &decisionpb.StartChildWorkflowExecutionDecisionAttributes{
			WorkflowId: "child workflow" + string(i),
			WorkflowType: &commonpb.WorkflowType{
				Name: "child workflow type",
			},
			TaskList:          &tasklistpb.TaskList{Name: taskListName},
			Input:             payload.EncodeString("random input"),
			ParentClosePolicy: parentClosePolicy,
		})
		s.Nil(err)
	}

	s.NoError(mutableState.FlushBufferedEvents())

	taskID := int64(59)
	event = addCompleteWorkflowEvent(mutableState, event.GetEventId(), nil)

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:     s.version,
		NamespaceId: s.GetNamespaceIDBytes(),
		WorkflowId:  execution.GetWorkflowId(),
		RunId:       primitives.MustParseUUID(execution.GetRunId()),
		TaskId:      taskID,
		TaskList:    taskListName,
		TaskType:    persistence.TransferTaskTypeCloseExecution,
		ScheduleId:  event.GetEventId(),
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

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonpb.WorkflowType{Name: workflowType},
				TaskList:                            &tasklistpb.TaskList{Name: taskListName},
				ExecutionStartToCloseTimeoutSeconds: 2,
				TaskStartToCloseTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()

	dt := decisionpb.DecisionType_StartChildWorkflowExecution
	parentClosePolicy := commonpb.ParentClosePolicy_Abandon
	var decisions []*decisionpb.Decision
	for i := 0; i < 10; i++ {
		decisions = append(decisions, &decisionpb.Decision{
			DecisionType: dt,
			Attributes: &decisionpb.Decision_StartChildWorkflowExecutionDecisionAttributes{StartChildWorkflowExecutionDecisionAttributes: &decisionpb.StartChildWorkflowExecutionDecisionAttributes{
				WorkflowId: "child workflow" + string(i),
				WorkflowType: &commonpb.WorkflowType{
					Name: "child workflow type",
				},
				TaskList:          &tasklistpb.TaskList{Name: taskListName},
				Input:             payload.EncodeString("random input"),
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
		_, _, err = mutableState.AddStartChildWorkflowExecutionInitiatedEvent(event.GetEventId(), uuid.New(), &decisionpb.StartChildWorkflowExecutionDecisionAttributes{
			WorkflowId: "child workflow" + string(i),
			WorkflowType: &commonpb.WorkflowType{
				Name: "child workflow type",
			},
			TaskList:          &tasklistpb.TaskList{Name: taskListName},
			Input:             payload.EncodeString("random input"),
			ParentClosePolicy: parentClosePolicy,
		})
		s.Nil(err)
	}

	s.NoError(mutableState.FlushBufferedEvents())

	taskID := int64(59)
	event = addCompleteWorkflowEvent(mutableState, event.GetEventId(), nil)

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:     s.version,
		NamespaceId: s.GetNamespaceIDBytes(),
		WorkflowId:  execution.GetWorkflowId(),
		RunId:       primitives.MustParseUUID(execution.GetRunId()),
		TaskId:      taskID,
		TaskList:    taskListName,
		TaskType:    persistence.TransferTaskTypeCloseExecution,
		ScheduleId:  event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockVisibilityMgr.On("RecordWorkflowExecutionClosed", mock.Anything).Return(nil).Once()
	s.mockArchivalMetadata.On("GetVisibilityConfig").Return(archiver.NewDisabledArchvialConfig())

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessCancelExecution_Success() {

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	targetExecution := executionpb.WorkflowExecution{
		WorkflowId: "some random target workflow ID",
		RunId:      uuid.New(),
	}

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonpb.WorkflowType{Name: workflowType},
				TaskList:                            &tasklistpb.TaskList{Name: taskListName},
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
	event, rci := addRequestCancelInitiatedEvent(mutableState, event.GetEventId(), uuid.New(), testTargetNamespace, targetExecution.GetWorkflowId(), targetExecution.GetRunId())

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:           s.version,
		NamespaceId:       s.GetNamespaceIDBytes(),
		WorkflowId:        execution.GetWorkflowId(),
		RunId:             primitives.MustParseUUID(execution.GetRunId()),
		TargetNamespaceId: primitives.MustParseUUID(s.targetNamespaceID),
		TargetWorkflowId:  targetExecution.GetWorkflowId(),
		TargetRunId:       primitives.MustParseUUID(targetExecution.GetRunId()),
		TaskId:            taskID,
		TaskList:          taskListName,
		TaskType:          persistence.TransferTaskTypeCancelExecution,
		ScheduleId:        event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), s.createRequestCancelWorkflowExecutionRequest(s.targetNamespace, transferTask, rci)).Return(nil, nil).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.version).Return(cluster.TestCurrentClusterName).AnyTimes()

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessCancelExecution_Failure() {

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	targetExecution := executionpb.WorkflowExecution{
		WorkflowId: "some random target workflow ID",
		RunId:      uuid.New(),
	}

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonpb.WorkflowType{Name: workflowType},
				TaskList:                            &tasklistpb.TaskList{Name: taskListName},
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
	event, rci := addRequestCancelInitiatedEvent(mutableState, event.GetEventId(), uuid.New(), testTargetNamespace, targetExecution.GetWorkflowId(), targetExecution.GetRunId())

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:           s.version,
		NamespaceId:       s.GetNamespaceIDBytes(),
		WorkflowId:        execution.GetWorkflowId(),
		RunId:             primitives.MustParseUUID(execution.GetRunId()),
		TargetNamespaceId: primitives.MustParseUUID(s.targetNamespaceID),
		TargetWorkflowId:  targetExecution.GetWorkflowId(),
		TargetRunId:       primitives.MustParseUUID(targetExecution.GetRunId()),
		TaskId:            taskID,
		TaskList:          taskListName,
		TaskType:          persistence.TransferTaskTypeCancelExecution,
		ScheduleId:        event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), s.createRequestCancelWorkflowExecutionRequest(s.targetNamespace, transferTask, rci)).Return(nil, serviceerror.NewNotFound("")).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.version).Return(cluster.TestCurrentClusterName).AnyTimes()

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessCancelExecution_Duplication() {

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	targetExecution := executionpb.WorkflowExecution{
		WorkflowId: "some random target workflow ID",
		RunId:      uuid.New(),
	}

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonpb.WorkflowType{Name: workflowType},
				TaskList:                            &tasklistpb.TaskList{Name: taskListName},
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
	event, _ = addRequestCancelInitiatedEvent(mutableState, event.GetEventId(), uuid.New(), testTargetNamespace, targetExecution.GetWorkflowId(), targetExecution.GetRunId())

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:           s.version,
		NamespaceId:       s.GetNamespaceIDBytes(),
		WorkflowId:        execution.GetWorkflowId(),
		RunId:             primitives.MustParseUUID(execution.GetRunId()),
		TargetNamespaceId: primitives.MustParseUUID(s.targetNamespaceID),
		TargetWorkflowId:  targetExecution.GetWorkflowId(),
		TargetRunId:       primitives.MustParseUUID(targetExecution.GetRunId()),
		TaskId:            taskID,
		TaskList:          taskListName,
		TaskType:          persistence.TransferTaskTypeCancelExecution,
		ScheduleId:        event.GetEventId(),
	}

	event = addCancelRequestedEvent(mutableState, event.GetEventId(), testTargetNamespaceID, targetExecution.GetWorkflowId(), targetExecution.GetRunId())

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessSignalExecution_Success() {

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	targetExecution := executionpb.WorkflowExecution{
		WorkflowId: "some random target workflow ID",
		RunId:      uuid.New(),
	}
	signalName := "some random signal name"
	signalInput := payload.EncodeString("some random signal input")
	signalControl := []byte("some random signal control")

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonpb.WorkflowType{Name: workflowType},
				TaskList:                            &tasklistpb.TaskList{Name: taskListName},
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
		testTargetNamespace, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), signalName, signalInput, signalControl)

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:           s.version,
		NamespaceId:       s.GetNamespaceIDBytes(),
		WorkflowId:        execution.GetWorkflowId(),
		RunId:             primitives.MustParseUUID(execution.GetRunId()),
		TargetNamespaceId: primitives.MustParseUUID(s.targetNamespaceID),
		TargetWorkflowId:  targetExecution.GetWorkflowId(),
		TargetRunId:       primitives.MustParseUUID(targetExecution.GetRunId()),
		TaskId:            taskID,
		TaskList:          taskListName,
		TaskType:          persistence.TransferTaskTypeSignalExecution,
		ScheduleId:        event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().SignalWorkflowExecution(gomock.Any(), s.createSignalWorkflowExecutionRequest(s.targetNamespace, transferTask, si)).Return(nil, nil).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.version).Return(cluster.TestCurrentClusterName).AnyTimes()

	s.mockHistoryClient.EXPECT().RemoveSignalMutableState(gomock.Any(), &historyservice.RemoveSignalMutableStateRequest{
		NamespaceId: primitives.UUID(transferTask.GetTargetNamespaceId()).String(),
		WorkflowExecution: &executionpb.WorkflowExecution{
			WorkflowId: transferTask.GetTargetWorkflowId(),
			RunId:      primitives.UUID(transferTask.GetTargetRunId()).String(),
		},
		RequestId: si.GetRequestId(),
	}).Return(nil, nil).Times(1)

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessSignalExecution_Failure() {

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	targetExecution := executionpb.WorkflowExecution{
		WorkflowId: "some random target workflow ID",
		RunId:      uuid.New(),
	}
	signalName := "some random signal name"
	signalInput := payload.EncodeString("some random signal input")
	signalControl := []byte("some random signal control")

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonpb.WorkflowType{Name: workflowType},
				TaskList:                            &tasklistpb.TaskList{Name: taskListName},
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
		testTargetNamespace, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), signalName, signalInput, signalControl)

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:           s.version,
		NamespaceId:       s.GetNamespaceIDBytes(),
		WorkflowId:        execution.GetWorkflowId(),
		RunId:             primitives.MustParseUUID(execution.GetRunId()),
		TargetNamespaceId: primitives.MustParseUUID(s.targetNamespaceID),
		TargetWorkflowId:  targetExecution.GetWorkflowId(),
		TargetRunId:       primitives.MustParseUUID(targetExecution.GetRunId()),
		TaskId:            taskID,
		TaskList:          taskListName,
		TaskType:          persistence.TransferTaskTypeSignalExecution,
		ScheduleId:        event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().SignalWorkflowExecution(gomock.Any(), s.createSignalWorkflowExecutionRequest(s.targetNamespace, transferTask, si)).Return(nil, serviceerror.NewNotFound("")).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.version).Return(cluster.TestCurrentClusterName).AnyTimes()

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessSignalExecution_Duplication() {

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	targetExecution := executionpb.WorkflowExecution{
		WorkflowId: "some random target workflow ID",
		RunId:      uuid.New(),
	}
	signalName := "some random signal name"
	signalInput := payload.EncodeString("some random signal input")
	signalControl := []byte("some random signal control")

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonpb.WorkflowType{Name: workflowType},
				TaskList:                            &tasklistpb.TaskList{Name: taskListName},
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
		testTargetNamespace, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), signalName, signalInput, signalControl)

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:           s.version,
		NamespaceId:       s.GetNamespaceIDBytes(),
		WorkflowId:        execution.GetWorkflowId(),
		RunId:             primitives.MustParseUUID(execution.GetRunId()),
		TargetNamespaceId: primitives.MustParseUUID(s.targetNamespaceID),
		TargetWorkflowId:  targetExecution.GetWorkflowId(),
		TargetRunId:       primitives.MustParseUUID(targetExecution.GetRunId()),
		TaskId:            taskID,
		TaskList:          taskListName,
		TaskType:          persistence.TransferTaskTypeSignalExecution,
		ScheduleId:        event.GetEventId(),
	}

	event = addSignaledEvent(mutableState, event.GetEventId(), testTargetNamespace, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), nil)

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessStartChildExecution_Success() {

	execution := executionpb.WorkflowExecution{
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
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonpb.WorkflowType{Name: workflowType},
				TaskList:                            &tasklistpb.TaskList{Name: taskListName},
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
		s.childNamespace, childWorkflowID, childWorkflowType, childTaskListName, nil, 1, 1)

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:           s.version,
		NamespaceId:       s.GetNamespaceIDBytes(),
		WorkflowId:        execution.GetWorkflowId(),
		RunId:             primitives.MustParseUUID(execution.GetRunId()),
		TargetNamespaceId: primitives.MustParseUUID(testChildNamespaceID),
		TargetWorkflowId:  childWorkflowID,
		TargetRunId:       nil,
		TaskId:            taskID,
		TaskList:          taskListName,
		TaskType:          persistence.TransferTaskTypeStartChildExecution,
		ScheduleId:        event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().StartWorkflowExecution(gomock.Any(), s.createChildWorkflowExecutionRequest(
		s.namespace,
		s.childNamespace,
		transferTask,
		mutableState,
		ci,
	)).Return(&historyservice.StartWorkflowExecutionResponse{RunId: childRunID}, nil).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.version).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockHistoryClient.EXPECT().ScheduleDecisionTask(gomock.Any(), &historyservice.ScheduleDecisionTaskRequest{
		NamespaceId: testChildNamespaceID,
		WorkflowExecution: &executionpb.WorkflowExecution{
			WorkflowId: childWorkflowID,
			RunId:      childRunID,
		},
		IsFirstDecision: true,
	}).Return(nil, nil).Times(1)

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessStartChildExecution_Failure() {

	execution := executionpb.WorkflowExecution{
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
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonpb.WorkflowType{Name: workflowType},
				TaskList:                            &tasklistpb.TaskList{Name: taskListName},
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
		s.childNamespace,
		childWorkflowID,
		childWorkflowType,
		childTaskListName,
		nil,
		1,
		1,
	)

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:           s.version,
		NamespaceId:       s.GetNamespaceIDBytes(),
		WorkflowId:        execution.GetWorkflowId(),
		RunId:             primitives.MustParseUUID(execution.GetRunId()),
		TargetNamespaceId: primitives.MustParseUUID(testChildNamespaceID),
		TargetWorkflowId:  childWorkflowID,
		TargetRunId:       nil,
		TaskId:            taskID,
		TaskList:          taskListName,
		TaskType:          persistence.TransferTaskTypeStartChildExecution,
		ScheduleId:        event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().StartWorkflowExecution(gomock.Any(), s.createChildWorkflowExecutionRequest(
		s.namespace,
		s.childNamespace,
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

	execution := executionpb.WorkflowExecution{
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
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonpb.WorkflowType{Name: workflowType},
				TaskList:                            &tasklistpb.TaskList{Name: taskListName},
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
		s.childNamespace,
		childWorkflowID,
		childWorkflowType,
		childTaskListName,
		nil,
		1,
		1,
	)

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:           s.version,
		NamespaceId:       s.GetNamespaceIDBytes(),
		WorkflowId:        execution.GetWorkflowId(),
		RunId:             primitives.MustParseUUID(execution.GetRunId()),
		TargetNamespaceId: primitives.MustParseUUID(testChildNamespaceID),
		TargetWorkflowId:  childWorkflowID,
		TargetRunId:       nil,
		TaskId:            taskID,
		TaskList:          taskListName,
		TaskType:          persistence.TransferTaskTypeStartChildExecution,
		ScheduleId:        event.GetEventId(),
	}

	event = addChildWorkflowExecutionStartedEvent(mutableState, event.GetEventId(), testChildNamespaceID, childWorkflowID, childRunID, childWorkflowType)
	ci.StartedID = event.GetEventId()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().ScheduleDecisionTask(gomock.Any(), &historyservice.ScheduleDecisionTaskRequest{
		NamespaceId: testChildNamespaceID,
		WorkflowExecution: &executionpb.WorkflowExecution{
			WorkflowId: childWorkflowID,
			RunId:      childRunID,
		},
		IsFirstDecision: true,
	}).Return(nil, nil).Times(1)

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessStartChildExecution_Duplication() {

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	childExecution := executionpb.WorkflowExecution{
		WorkflowId: "some random child workflow ID",
		RunId:      uuid.New(),
	}
	childWorkflowType := "some random child workflow type"
	childTaskListName := "some random child task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonpb.WorkflowType{Name: workflowType},
				TaskList:                            &tasklistpb.TaskList{Name: taskListName},
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
		s.childNamespace,
		childExecution.GetWorkflowId(),
		childWorkflowType,
		childTaskListName,
		nil,
		1,
		1,
	)

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:           s.version,
		NamespaceId:       s.GetNamespaceIDBytes(),
		WorkflowId:        execution.GetWorkflowId(),
		RunId:             primitives.MustParseUUID(execution.GetRunId()),
		TargetNamespaceId: primitives.MustParseUUID(testChildNamespaceID),
		TargetWorkflowId:  childExecution.GetWorkflowId(),
		TargetRunId:       nil,
		TaskId:            taskID,
		TaskList:          taskListName,
		TaskType:          persistence.TransferTaskTypeStartChildExecution,
		ScheduleId:        event.GetEventId(),
	}

	event = addChildWorkflowExecutionStartedEvent(mutableState, event.GetEventId(), testChildNamespaceID, childExecution.GetWorkflowId(), childExecution.GetRunId(), childWorkflowType)
	ci.StartedID = event.GetEventId()
	event = addChildWorkflowExecutionCompletedEvent(mutableState, ci.InitiatedID, &childExecution, &eventpb.WorkflowExecutionCompletedEventAttributes{
		Result:                       payload.EncodeString("some random child workflow execution result"),
		DecisionTaskCompletedEventId: transferTask.GetScheduleId(),
	})

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessRecordWorkflowStartedTask() {

	execution := executionpb.WorkflowExecution{
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
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonpb.WorkflowType{Name: workflowType},
				TaskList:                            &tasklistpb.TaskList{Name: taskListName},
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
		Version:     s.version,
		NamespaceId: s.GetNamespaceIDBytes(),
		WorkflowId:  execution.GetWorkflowId(),
		RunId:       primitives.MustParseUUID(execution.GetRunId()),
		TaskId:      taskID,
		TaskList:    taskListName,
		TaskType:    persistence.TransferTaskTypeRecordWorkflowStarted,
		ScheduleId:  event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, di.ScheduleID, di.Version)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockVisibilityMgr.On("RecordWorkflowExecutionStarted", s.createRecordWorkflowExecutionStartedRequest(s.namespace, event, transferTask, mutableState, backoffSeconds)).Once().Return(nil)

	err = s.transferQueueActiveTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessUpsertWorkflowSearchAttributes() {

	execution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())

	event, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                        &commonpb.WorkflowType{Name: workflowType},
				TaskList:                            &tasklistpb.TaskList{Name: taskListName},
				ExecutionStartToCloseTimeoutSeconds: 2,
				TaskStartToCloseTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	taskID := int64(59)
	di := addDecisionTaskScheduledEvent(mutableState)

	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:     s.version,
		NamespaceId: s.GetNamespaceIDBytes(),
		WorkflowId:  execution.GetWorkflowId(),
		RunId:       primitives.MustParseUUID(execution.GetRunId()),
		TaskId:      taskID,
		TaskList:    taskListName,
		TaskType:    persistence.TransferTaskTypeUpsertWorkflowSearchAttributes,
		ScheduleId:  event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, di.ScheduleID, di.Version)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockVisibilityMgr.On("UpsertWorkflowExecution", s.createUpsertWorkflowSearchAttributesRequest(s.namespace, event, transferTask, mutableState)).Once().Return(nil)

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
		NamespaceId:       primitives.UUID(task.GetTargetNamespaceId()).String(),
		SourceNamespaceId: primitives.UUID(task.GetNamespaceId()).String(),
		Execution: &executionpb.WorkflowExecution{
			WorkflowId: task.GetWorkflowId(),
			RunId:      primitives.UUID(task.GetRunId()).String(),
		},
		TaskList:                      &tasklistpb.TaskList{Name: task.TaskList},
		ScheduleId:                    task.GetScheduleId(),
		ScheduleToStartTimeoutSeconds: ai.ScheduleToStartTimeout,
	}
}

func (s *transferQueueActiveTaskExecutorSuite) createAddDecisionTaskRequest(
	task *persistenceblobs.TransferTaskInfo,
	mutableState mutableState,
) *matchingservice.AddDecisionTaskRequest {

	execution := executionpb.WorkflowExecution{
		WorkflowId: task.GetWorkflowId(),
		RunId:      primitives.UUID(task.GetRunId()).String(),
	}
	taskList := &tasklistpb.TaskList{Name: task.TaskList}
	executionInfo := mutableState.GetExecutionInfo()
	timeout := executionInfo.WorkflowTimeout
	if mutableState.GetExecutionInfo().TaskList != task.TaskList {
		taskList.Kind = tasklistpb.TaskListKind_Sticky
		timeout = executionInfo.StickyScheduleToStartTimeout
	}

	return &matchingservice.AddDecisionTaskRequest{
		NamespaceId:                   primitives.UUID(task.GetNamespaceId()).String(),
		Execution:                     &execution,
		TaskList:                      taskList,
		ScheduleId:                    task.GetScheduleId(),
		ScheduleToStartTimeoutSeconds: timeout,
	}
}

func (s *transferQueueActiveTaskExecutorSuite) createRecordWorkflowExecutionStartedRequest(
	namespace string,
	startEvent *eventpb.HistoryEvent,
	task *persistenceblobs.TransferTaskInfo,
	mutableState mutableState,
	backoffSeconds int32,
) *persistence.RecordWorkflowExecutionStartedRequest {
	execution := &executionpb.WorkflowExecution{
		WorkflowId: task.GetWorkflowId(),
		RunId:      primitives.UUID(task.GetRunId()).String(),
	}
	executionInfo := mutableState.GetExecutionInfo()
	executionTimestamp := time.Unix(0, startEvent.GetTimestamp()).Add(time.Duration(backoffSeconds) * time.Second)

	return &persistence.RecordWorkflowExecutionStartedRequest{
		Namespace:          namespace,
		NamespaceID:        primitives.UUID(task.GetNamespaceId()).String(),
		Execution:          *execution,
		WorkflowTypeName:   executionInfo.WorkflowTypeName,
		StartTimestamp:     startEvent.GetTimestamp(),
		ExecutionTimestamp: executionTimestamp.UnixNano(),
		WorkflowTimeout:    int64(executionInfo.WorkflowTimeout),
		TaskID:             task.GetTaskId(),
		TaskList:           task.TaskList,
	}
}

func (s *transferQueueActiveTaskExecutorSuite) createRequestCancelWorkflowExecutionRequest(
	targetNamespace string,
	task *persistenceblobs.TransferTaskInfo,
	rci *persistenceblobs.RequestCancelInfo,
) *historyservice.RequestCancelWorkflowExecutionRequest {

	sourceExecution := executionpb.WorkflowExecution{
		WorkflowId: task.GetWorkflowId(),
		RunId:      primitives.UUID(task.GetRunId()).String(),
	}
	targetExecution := executionpb.WorkflowExecution{
		WorkflowId: task.GetTargetWorkflowId(),
		RunId:      primitives.UUID(task.GetTargetRunId()).String(),
	}

	return &historyservice.RequestCancelWorkflowExecutionRequest{
		NamespaceId: primitives.UUID(task.GetTargetNamespaceId()).String(),
		CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{
			Namespace:         targetNamespace,
			WorkflowExecution: &targetExecution,
			Identity:          identityHistoryService,
			// Use the same request ID to dedupe RequestCancelWorkflowExecution calls
			RequestId: rci.GetCancelRequestId(),
		},
		ExternalInitiatedEventId:  task.GetScheduleId(),
		ExternalWorkflowExecution: &sourceExecution,
		ChildWorkflowOnly:         task.TargetChildWorkflowOnly,
	}
}

func (s *transferQueueActiveTaskExecutorSuite) createSignalWorkflowExecutionRequest(
	targetNamespace string,
	task *persistenceblobs.TransferTaskInfo,
	si *persistenceblobs.SignalInfo,
) *historyservice.SignalWorkflowExecutionRequest {

	sourceExecution := executionpb.WorkflowExecution{
		WorkflowId: task.GetWorkflowId(),
		RunId:      primitives.UUID(task.GetRunId()).String(),
	}
	targetExecution := executionpb.WorkflowExecution{
		WorkflowId: task.GetTargetWorkflowId(),
		RunId:      primitives.UUID(task.GetTargetRunId()).String(),
	}

	return &historyservice.SignalWorkflowExecutionRequest{
		NamespaceId: primitives.UUID(task.GetTargetNamespaceId()).String(),
		SignalRequest: &workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         targetNamespace,
			WorkflowExecution: &targetExecution,
			Identity:          identityHistoryService,
			SignalName:        si.Name,
			Input:             si.Input,
			RequestId:         si.GetRequestId(),
			Control:           si.Control,
		},
		ExternalWorkflowExecution: &sourceExecution,
		ChildWorkflowOnly:         task.TargetChildWorkflowOnly,
	}
}

func (s *transferQueueActiveTaskExecutorSuite) createChildWorkflowExecutionRequest(
	namespace string,
	childNamespace string,
	task *persistenceblobs.TransferTaskInfo,
	mutableState mutableState,
	ci *persistence.ChildExecutionInfo,
) *historyservice.StartWorkflowExecutionRequest {

	event, err := mutableState.GetChildExecutionInitiatedEvent(task.GetScheduleId())
	s.NoError(err)
	attributes := event.GetStartChildWorkflowExecutionInitiatedEventAttributes()
	execution := executionpb.WorkflowExecution{
		WorkflowId: task.GetWorkflowId(),
		RunId:      primitives.UUID(task.GetRunId()).String(),
	}
	now := time.Now()
	return &historyservice.StartWorkflowExecutionRequest{
		NamespaceId: primitives.UUID(task.GetTargetNamespaceId()).String(),
		StartRequest: &workflowservice.StartWorkflowExecutionRequest{
			Namespace:                           childNamespace,
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
		ParentExecutionInfo: &executiongenpb.ParentExecutionInfo{
			NamespaceId: primitives.UUID(task.GetNamespaceId()).String(),
			Namespace:   testNamespace,
			Execution:   &execution,
			InitiatedId: task.GetScheduleId(),
		},
		FirstDecisionTaskBackoffSeconds: backoff.GetBackoffForNextScheduleInSeconds(attributes.GetCronSchedule(), now, now),
	}
}

func (s *transferQueueActiveTaskExecutorSuite) createUpsertWorkflowSearchAttributesRequest(
	namespace string,
	startEvent *eventpb.HistoryEvent,
	task *persistenceblobs.TransferTaskInfo,
	mutableState mutableState,
) *persistence.UpsertWorkflowExecutionRequest {

	execution := &executionpb.WorkflowExecution{
		WorkflowId: task.GetWorkflowId(),
		RunId:      primitives.UUID(task.GetRunId()).String(),
	}
	executionInfo := mutableState.GetExecutionInfo()

	return &persistence.UpsertWorkflowExecutionRequest{
		Namespace:        namespace,
		NamespaceID:      primitives.UUID(task.GetNamespaceId()).String(),
		Execution:        *execution,
		WorkflowTypeName: executionInfo.WorkflowTypeName,
		StartTimestamp:   startEvent.GetTimestamp(),
		WorkflowTimeout:  int64(executionInfo.WorkflowTimeout),
		TaskID:           task.GetTaskId(),
		TaskList:         task.TaskList,
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
