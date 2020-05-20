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

	"github.com/gogo/protobuf/types"
	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/temporal-proto/common"
	tasklistpb "go.temporal.io/temporal-proto/tasklist"
	"go.temporal.io/temporal-proto/workflowservice"

	commongenpb "github.com/temporalio/temporal/.gen/proto/common"
	"github.com/temporalio/temporal/.gen/proto/historyservice"
	"github.com/temporalio/temporal/.gen/proto/matchingservice"
	"github.com/temporalio/temporal/.gen/proto/matchingservicemock"
	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/archiver"
	"github.com/temporalio/temporal/common/archiver/provider"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/clock"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/mocks"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/xdc"
)

type (
	transferQueueStandbyTaskExecutorSuite struct {
		suite.Suite
		*require.Assertions

		controller             *gomock.Controller
		mockShard              *shardContextTest
		mockNamespaceCache     *cache.MockNamespaceCache
		mockClusterMetadata    *cluster.MockMetadata
		mockNDCHistoryResender *xdc.MockNDCHistoryResender
		mockMatchingClient     *matchingservicemock.MockMatchingServiceClient

		mockVisibilityMgr       *mocks.VisibilityManager
		mockExecutionMgr        *mocks.ExecutionManager
		mockArchivalMetadata    *archiver.MockArchivalMetadata
		mockArchiverProvider    *provider.MockArchiverProvider
		mockHistoryRereplicator *xdc.MockHistoryRereplicator

		logger               log.Logger
		namespaceID          string
		namespaceEntry       *cache.NamespaceCacheEntry
		version              int64
		clusterName          string
		now                  time.Time
		timeSource           *clock.EventTimeSource
		fetchHistoryDuration time.Duration
		discardDuration      time.Duration

		transferQueueStandbyTaskExecutor *transferQueueStandbyTaskExecutor
	}
)

func TestTransferQueueStandbyTaskExecutorSuite(t *testing.T) {
	s := new(transferQueueStandbyTaskExecutorSuite)
	suite.Run(t, s)
}

func (s *transferQueueStandbyTaskExecutorSuite) SetupSuite() {

}

func (s *transferQueueStandbyTaskExecutorSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	config := NewDynamicConfigForTest()
	s.namespaceID = testNamespaceID
	s.namespaceEntry = testGlobalNamespaceEntry
	s.version = s.namespaceEntry.GetFailoverVersion()
	s.now = time.Now()
	s.timeSource = clock.NewEventTimeSource().Update(s.now)
	s.fetchHistoryDuration = config.StandbyTaskMissingEventsResendDelay() +
		(config.StandbyTaskMissingEventsDiscardDelay()-config.StandbyTaskMissingEventsResendDelay())/2
	s.discardDuration = config.StandbyTaskMissingEventsDiscardDelay() * 2

	s.controller = gomock.NewController(s.T())
	s.mockNDCHistoryResender = xdc.NewMockNDCHistoryResender(s.controller)

	s.mockShard = newTestShardContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistenceblobs.ShardInfo{
				RangeId:          1,
				TransferAckLevel: 0,
			}},
		config,
	)
	s.mockShard.eventsCache = newEventsCache(s.mockShard)
	s.mockShard.resource.TimeSource = s.timeSource

	s.mockHistoryRereplicator = &xdc.MockHistoryRereplicator{}
	s.mockMatchingClient = s.mockShard.resource.MatchingClient
	s.mockExecutionMgr = s.mockShard.resource.ExecutionMgr
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
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.version).Return(s.clusterName).AnyTimes()

	s.logger = s.mockShard.GetLogger()

	historyCache := newHistoryCache(s.mockShard)
	h := &historyEngineImpl{
		currentClusterName: s.mockShard.GetService().GetClusterMetadata().GetCurrentClusterName(),
		shard:              s.mockShard,
		clusterMetadata:    s.mockClusterMetadata,
		executionManager:   s.mockExecutionMgr,
		historyCache:       historyCache,
		logger:             s.logger,
		tokenSerializer:    common.NewProtoTaskTokenSerializer(),
		metricsClient:      s.mockShard.GetMetricsClient(),
	}
	s.mockShard.SetEngine(h)
	s.clusterName = cluster.TestAlternativeClusterName
	s.transferQueueStandbyTaskExecutor = newTransferQueueStandbyTaskExecutor(
		s.mockShard,
		h,
		s.mockHistoryRereplicator,
		s.mockNDCHistoryResender,
		s.logger,
		s.mockShard.GetMetricsClient(),
		s.clusterName,
		config,
	).(*transferQueueStandbyTaskExecutor)
}

func (s *transferQueueStandbyTaskExecutorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.Finish(s.T())
	s.mockHistoryRereplicator.AssertExpectations(s.T())
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessActivityTask_Pending() {

	execution := commonpb.WorkflowExecution{
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
				WorkflowType:                    &commonpb.WorkflowType{Name: workflowType},
				TaskList:                        &tasklistpb.TaskList{Name: taskListName},
				WorkflowExecutionTimeoutSeconds: 2,
				WorkflowTaskTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	taskID := int64(59)
	activityID := "activity-1"
	activityType := "some random activity type"
	event, _ = addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID, activityType, taskListName, &commonpb.Payloads{}, 1, 1, 1, 1)

	now := types.TimestampNow()
	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:             s.version,
		NamespaceId:         s.namespaceID,
		WorkflowId:          execution.GetWorkflowId(),
		RunId:               execution.GetRunId(),
		VisibilityTimestamp: now,
		TaskId:              taskID,
		TaskList:            taskListName,
		TaskType:            commongenpb.TaskType_TransferActivityTask,
		ScheduleId:          event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, time.Unix(now.Seconds, int64(now.Nanos)).UTC())
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Equal(ErrTaskRetry, err)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessActivityTask_Pending_PushToMatching() {

	execution := commonpb.WorkflowExecution{
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
				WorkflowType:                    &commonpb.WorkflowType{Name: workflowType},
				TaskList:                        &tasklistpb.TaskList{Name: taskListName},
				WorkflowExecutionTimeoutSeconds: 2,
				WorkflowTaskTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	taskID := int64(59)
	activityID := "activity-1"
	activityType := "some random activity type"
	event, _ = addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID, activityType, taskListName, &commonpb.Payloads{}, 1, 1, 1, 1)

	now := types.TimestampNow()
	s.mockShard.SetCurrentTime(s.clusterName, time.Unix(now.Seconds, int64(now.Nanos)).UTC().Add(s.fetchHistoryDuration))
	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:             s.version,
		NamespaceId:         s.namespaceID,
		WorkflowId:          execution.GetWorkflowId(),
		RunId:               execution.GetRunId(),
		VisibilityTimestamp: now,
		TaskId:              taskID,
		TaskList:            taskListName,
		TaskType:            commongenpb.TaskType_TransferActivityTask,
		ScheduleId:          event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.EXPECT().AddActivityTask(gomock.Any(), gomock.Any()).Return(&matchingservice.AddActivityTaskResponse{}, nil).Times(1)

	s.mockShard.SetCurrentTime(s.clusterName, time.Unix(now.Seconds, int64(now.Nanos)).UTC())
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessActivityTask_Success() {

	execution := commonpb.WorkflowExecution{
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
				WorkflowType:                    &commonpb.WorkflowType{Name: workflowType},
				TaskList:                        &tasklistpb.TaskList{Name: taskListName},
				WorkflowExecutionTimeoutSeconds: 2,
				WorkflowTaskTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	taskID := int64(59)
	activityID := "activity-1"
	activityType := "some random activity type"
	event, _ = addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID, activityType, taskListName, &commonpb.Payloads{}, 1, 1, 1, 1)

	now := types.TimestampNow()
	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:             s.version,
		NamespaceId:         s.namespaceID,
		WorkflowId:          execution.GetWorkflowId(),
		RunId:               execution.GetRunId(),
		VisibilityTimestamp: now,
		TaskId:              taskID,
		TaskList:            taskListName,
		TaskType:            commongenpb.TaskType_TransferActivityTask,
		ScheduleId:          event.GetEventId(),
	}

	event = addActivityTaskStartedEvent(mutableState, event.GetEventId(), "")

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, time.Unix(now.Seconds, int64(now.Nanos)).UTC())
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessDecisionTask_Pending() {

	execution := commonpb.WorkflowExecution{
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
				WorkflowType:                    &commonpb.WorkflowType{Name: workflowType},
				TaskList:                        &tasklistpb.TaskList{Name: taskListName},
				WorkflowExecutionTimeoutSeconds: 2,
				WorkflowTaskTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	taskID := int64(59)
	di := addDecisionTaskScheduledEvent(mutableState)

	now := types.TimestampNow()
	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:             s.version,
		NamespaceId:         s.namespaceID,
		WorkflowId:          execution.GetWorkflowId(),
		RunId:               execution.GetRunId(),
		VisibilityTimestamp: now,
		TaskId:              taskID,
		TaskList:            taskListName,
		TaskType:            commongenpb.TaskType_TransferDecisionTask,
		ScheduleId:          di.ScheduleID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, di.ScheduleID, di.Version)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, time.Unix(now.Seconds, int64(now.Nanos)).UTC())
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Equal(ErrTaskRetry, err)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessDecisionTask_Pending_PushToMatching() {

	execution := commonpb.WorkflowExecution{
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
				WorkflowType:                    &commonpb.WorkflowType{Name: workflowType},
				TaskList:                        &tasklistpb.TaskList{Name: taskListName},
				WorkflowExecutionTimeoutSeconds: 2,
				WorkflowTaskTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	taskID := int64(59)
	di := addDecisionTaskScheduledEvent(mutableState)

	now := types.TimestampNow()
	s.mockShard.SetCurrentTime(s.clusterName, time.Unix(now.Seconds, int64(now.Nanos)).UTC().Add(s.fetchHistoryDuration))
	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:             s.version,
		NamespaceId:         s.namespaceID,
		WorkflowId:          execution.GetWorkflowId(),
		RunId:               execution.GetRunId(),
		VisibilityTimestamp: now,
		TaskId:              taskID,
		TaskList:            taskListName,
		TaskType:            commongenpb.TaskType_TransferDecisionTask,
		ScheduleId:          di.ScheduleID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, di.ScheduleID, di.Version)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.EXPECT().AddDecisionTask(gomock.Any(), gomock.Any()).Return(&matchingservice.AddDecisionTaskResponse{}, nil).Times(1)

	s.mockShard.SetCurrentTime(s.clusterName, time.Unix(now.Seconds, int64(now.Nanos)).UTC())
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessDecisionTask_Success_FirstDecision() {

	execution := commonpb.WorkflowExecution{
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
				WorkflowType:                    &commonpb.WorkflowType{Name: workflowType},
				TaskList:                        &tasklistpb.TaskList{Name: taskListName},
				WorkflowExecutionTimeoutSeconds: 2,
				WorkflowTaskTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	taskID := int64(59)
	di := addDecisionTaskScheduledEvent(mutableState)

	now := types.TimestampNow()
	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:             s.version,
		NamespaceId:         s.namespaceID,
		WorkflowId:          execution.GetWorkflowId(),
		RunId:               execution.GetRunId(),
		VisibilityTimestamp: now,
		TaskId:              taskID,
		TaskList:            taskListName,
		TaskType:            commongenpb.TaskType_TransferDecisionTask,
		ScheduleId:          di.ScheduleID,
	}

	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, time.Unix(now.Seconds, int64(now.Nanos)).UTC())
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessDecisionTask_Success_NonFirstDecision() {

	execution := commonpb.WorkflowExecution{
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
				WorkflowType:                    &commonpb.WorkflowType{Name: workflowType},
				TaskList:                        &tasklistpb.TaskList{Name: taskListName},
				WorkflowExecutionTimeoutSeconds: 2,
				WorkflowTaskTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	taskID := int64(59)
	di = addDecisionTaskScheduledEvent(mutableState)

	now := types.TimestampNow()
	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:             s.version,
		NamespaceId:         s.namespaceID,
		WorkflowId:          execution.GetWorkflowId(),
		RunId:               execution.GetRunId(),
		VisibilityTimestamp: now,
		TaskId:              taskID,
		TaskList:            taskListName,
		TaskType:            commongenpb.TaskType_TransferDecisionTask,
		ScheduleId:          di.ScheduleID,
	}

	event = addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, time.Unix(now.Seconds, int64(now.Nanos)).UTC())
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessCloseExecution() {

	execution := commonpb.WorkflowExecution{
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
				WorkflowType:                    &commonpb.WorkflowType{Name: workflowType},
				TaskList:                        &tasklistpb.TaskList{Name: taskListName},
				WorkflowExecutionTimeoutSeconds: 2,
				WorkflowTaskTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	taskID := int64(59)
	event = addCompleteWorkflowEvent(mutableState, event.GetEventId(), nil)

	now := types.TimestampNow()
	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:             s.version,
		NamespaceId:         s.namespaceID,
		WorkflowId:          execution.GetWorkflowId(),
		RunId:               execution.GetRunId(),
		VisibilityTimestamp: now,
		TaskId:              taskID,
		TaskList:            taskListName,
		TaskType:            commongenpb.TaskType_TransferCloseExecution,
		ScheduleId:          event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockVisibilityMgr.On("RecordWorkflowExecutionClosed", mock.Anything).Return(nil).Once()
	s.mockArchivalMetadata.On("GetVisibilityConfig").Return(archiver.NewDisabledArchvialConfig())

	s.mockShard.SetCurrentTime(s.clusterName, time.Unix(now.Seconds, int64(now.Nanos)).UTC())
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessCancelExecution_Pending() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	targetExecution := commonpb.WorkflowExecution{
		WorkflowId: "some random target workflow ID",
		RunId:      uuid.New(),
	}

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                    &commonpb.WorkflowType{Name: workflowType},
				TaskList:                        &tasklistpb.TaskList{Name: taskListName},
				WorkflowExecutionTimeoutSeconds: 2,
				WorkflowTaskTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	taskID := int64(59)
	event, _ = addRequestCancelInitiatedEvent(mutableState, event.GetEventId(), uuid.New(), testTargetNamespace, targetExecution.GetWorkflowId(), targetExecution.GetRunId())
	nextEventID := event.GetEventId() + 1

	now := types.TimestampNow()
	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:             s.version,
		NamespaceId:         s.namespaceID,
		WorkflowId:          execution.GetWorkflowId(),
		RunId:               execution.GetRunId(),
		VisibilityTimestamp: now,
		TargetNamespaceId:   testTargetNamespaceID,
		TargetWorkflowId:    targetExecution.GetWorkflowId(),
		TargetRunId:         targetExecution.GetRunId(),
		TaskId:              taskID,
		TaskList:            taskListName,
		TaskType:            commongenpb.TaskType_TransferCancelExecution,
		ScheduleId:          event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, time.Unix(now.Seconds, int64(now.Nanos)).UTC())
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Equal(ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, time.Unix(now.Seconds, int64(now.Nanos)).UTC().Add(s.fetchHistoryDuration))
	s.mockHistoryRereplicator.On("SendMultiWorkflowHistory",
		transferTask.GetNamespaceId(), transferTask.GetWorkflowId(),
		transferTask.GetRunId(), nextEventID,
		transferTask.GetRunId(), common.EndEventID,
	).Return(nil).Once()
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Equal(ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, time.Unix(now.Seconds, int64(now.Nanos)).UTC().Add(s.discardDuration))
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Equal(ErrTaskDiscarded, err)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessCancelExecution_Success() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	targetExecution := commonpb.WorkflowExecution{
		WorkflowId: "some random target workflow ID",
		RunId:      uuid.New(),
	}

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                    &commonpb.WorkflowType{Name: workflowType},
				TaskList:                        &tasklistpb.TaskList{Name: taskListName},
				WorkflowExecutionTimeoutSeconds: 2,
				WorkflowTaskTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	taskID := int64(59)
	event, _ = addRequestCancelInitiatedEvent(mutableState, event.GetEventId(), uuid.New(), testTargetNamespace, targetExecution.GetWorkflowId(), targetExecution.GetRunId())

	now := types.TimestampNow()
	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:             s.version,
		NamespaceId:         s.namespaceID,
		WorkflowId:          execution.GetWorkflowId(),
		RunId:               execution.GetRunId(),
		VisibilityTimestamp: now,
		TargetNamespaceId:   testTargetNamespaceID,
		TargetWorkflowId:    targetExecution.GetWorkflowId(),
		TargetRunId:         targetExecution.GetRunId(),
		TaskId:              taskID,
		TaskList:            taskListName,
		TaskType:            commongenpb.TaskType_TransferCancelExecution,
		ScheduleId:          event.GetEventId(),
	}

	event = addCancelRequestedEvent(mutableState, event.GetEventId(), testTargetNamespaceID, targetExecution.GetWorkflowId(), targetExecution.GetRunId())

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, time.Unix(now.Seconds, int64(now.Nanos)).UTC())
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessSignalExecution_Pending() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	targetExecution := commonpb.WorkflowExecution{
		WorkflowId: "some random target workflow ID",
		RunId:      uuid.New(),
	}
	signalName := "some random signal name"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                    &commonpb.WorkflowType{Name: workflowType},
				TaskList:                        &tasklistpb.TaskList{Name: taskListName},
				WorkflowExecutionTimeoutSeconds: 2,
				WorkflowTaskTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	taskID := int64(59)
	event, _ = addRequestSignalInitiatedEvent(mutableState, event.GetEventId(), uuid.New(),
		testTargetNamespace, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), signalName, nil, "")
	nextEventID := event.GetEventId() + 1

	now := types.TimestampNow()
	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:             s.version,
		NamespaceId:         s.namespaceID,
		WorkflowId:          execution.GetWorkflowId(),
		RunId:               execution.GetRunId(),
		VisibilityTimestamp: now,
		TargetNamespaceId:   testTargetNamespaceID,
		TargetWorkflowId:    targetExecution.GetWorkflowId(),
		TargetRunId:         targetExecution.GetRunId(),
		TaskId:              taskID,
		TaskList:            taskListName,
		TaskType:            commongenpb.TaskType_TransferSignalExecution,
		ScheduleId:          event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, time.Unix(now.Seconds, int64(now.Nanos)).UTC())
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Equal(ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, time.Unix(now.Seconds, int64(now.Nanos)).UTC().Add(s.fetchHistoryDuration))
	s.mockHistoryRereplicator.On("SendMultiWorkflowHistory",
		transferTask.GetNamespaceId(), transferTask.GetWorkflowId(),
		transferTask.GetRunId(), nextEventID,
		transferTask.GetRunId(), common.EndEventID,
	).Return(nil).Once()
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Equal(ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, time.Unix(now.Seconds, int64(now.Nanos)).UTC().Add(s.discardDuration))
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Equal(ErrTaskDiscarded, err)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessSignalExecution_Success() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	targetExecution := commonpb.WorkflowExecution{
		WorkflowId: "some random target workflow ID",
		RunId:      uuid.New(),
	}
	signalName := "some random signal name"

	mutableState := newMutableStateBuilderWithReplicationStateWithEventV2(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:                    &commonpb.WorkflowType{Name: workflowType},
				TaskList:                        &tasklistpb.TaskList{Name: taskListName},
				WorkflowExecutionTimeoutSeconds: 2,
				WorkflowTaskTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	taskID := int64(59)
	event, _ = addRequestSignalInitiatedEvent(mutableState, event.GetEventId(), uuid.New(),
		testTargetNamespace, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), signalName, nil, "")

	now := types.TimestampNow()
	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:             s.version,
		NamespaceId:         s.namespaceID,
		WorkflowId:          execution.GetWorkflowId(),
		RunId:               execution.GetRunId(),
		VisibilityTimestamp: now,
		TargetNamespaceId:   testTargetNamespaceID,
		TargetWorkflowId:    targetExecution.GetWorkflowId(),
		TargetRunId:         targetExecution.GetRunId(),
		TaskId:              taskID,
		TaskList:            taskListName,
		TaskType:            commongenpb.TaskType_TransferSignalExecution,
		ScheduleId:          event.GetEventId(),
	}

	event = addSignaledEvent(mutableState, event.GetEventId(), testTargetNamespace, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), "")

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, time.Unix(now.Seconds, int64(now.Nanos)).UTC())
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessStartChildExecution_Pending() {

	execution := commonpb.WorkflowExecution{
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
				WorkflowType:                    &commonpb.WorkflowType{Name: workflowType},
				TaskList:                        &tasklistpb.TaskList{Name: taskListName},
				WorkflowExecutionTimeoutSeconds: 2,
				WorkflowTaskTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	taskID := int64(59)
	event, _ = addStartChildWorkflowExecutionInitiatedEvent(mutableState, event.GetEventId(), uuid.New(),
		testChildNamespace, childWorkflowID, childWorkflowType, childTaskListName, nil, 1, 1, 1)
	nextEventID := event.GetEventId() + 1

	now := types.TimestampNow()
	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:             s.version,
		NamespaceId:         s.namespaceID,
		WorkflowId:          execution.GetWorkflowId(),
		RunId:               execution.GetRunId(),
		VisibilityTimestamp: now,
		TargetNamespaceId:   testChildNamespaceID,
		TargetWorkflowId:    childWorkflowID,
		TargetRunId:         "",
		TaskId:              taskID,
		TaskList:            taskListName,
		TaskType:            commongenpb.TaskType_TransferStartChildExecution,
		ScheduleId:          event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, time.Unix(now.Seconds, int64(now.Nanos)).UTC())
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Equal(ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, time.Unix(now.Seconds, int64(now.Nanos)).UTC().Add(s.fetchHistoryDuration))
	s.mockHistoryRereplicator.On("SendMultiWorkflowHistory",
		transferTask.GetNamespaceId(), transferTask.GetWorkflowId(),
		transferTask.GetRunId(), nextEventID,
		transferTask.GetRunId(), common.EndEventID,
	).Return(nil).Once()
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Equal(ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, time.Unix(now.Seconds, int64(now.Nanos)).UTC().Add(s.discardDuration))
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Equal(ErrTaskDiscarded, err)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessStartChildExecution_Success() {

	execution := commonpb.WorkflowExecution{
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
				WorkflowType:                    &commonpb.WorkflowType{Name: workflowType},
				TaskList:                        &tasklistpb.TaskList{Name: taskListName},
				WorkflowExecutionTimeoutSeconds: 2,
				WorkflowTaskTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	di := addDecisionTaskScheduledEvent(mutableState)
	event := addDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	taskID := int64(59)
	event, childInfo := addStartChildWorkflowExecutionInitiatedEvent(mutableState, event.GetEventId(), uuid.New(),
		testChildNamespace, childWorkflowID, childWorkflowType, childTaskListName, nil, 1, 1, 1)

	now := types.TimestampNow()
	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:             s.version,
		NamespaceId:         s.namespaceID,
		WorkflowId:          execution.GetWorkflowId(),
		RunId:               execution.GetRunId(),
		VisibilityTimestamp: now,
		TargetNamespaceId:   testChildNamespaceID,
		TargetWorkflowId:    childWorkflowID,
		TargetRunId:         "",
		TaskId:              taskID,
		TaskList:            taskListName,
		TaskType:            commongenpb.TaskType_TransferStartChildExecution,
		ScheduleId:          event.GetEventId(),
	}

	event = addChildWorkflowExecutionStartedEvent(mutableState, event.GetEventId(), testChildNamespace, childWorkflowID, uuid.New(), childWorkflowType)
	childInfo.StartedID = event.GetEventId()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, time.Unix(now.Seconds, int64(now.Nanos)).UTC())
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessRecordWorkflowStartedTask() {

	execution := commonpb.WorkflowExecution{
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
				WorkflowType:                    &commonpb.WorkflowType{Name: workflowType},
				TaskList:                        &tasklistpb.TaskList{Name: taskListName},
				WorkflowExecutionTimeoutSeconds: 2,
				WorkflowTaskTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	taskID := int64(59)

	di := addDecisionTaskScheduledEvent(mutableState)

	now := types.TimestampNow()
	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:             s.version,
		NamespaceId:         s.namespaceID,
		WorkflowId:          execution.GetWorkflowId(),
		RunId:               execution.GetRunId(),
		VisibilityTimestamp: now,
		TaskId:              taskID,
		TaskList:            taskListName,
		TaskType:            commongenpb.TaskType_TransferRecordWorkflowStarted,
		ScheduleId:          event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, di.ScheduleID, di.Version)
	executionInfo := mutableState.GetExecutionInfo()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockVisibilityMgr.On("RecordWorkflowExecutionStarted", &persistence.RecordWorkflowExecutionStartedRequest{
		NamespaceID: testNamespaceID,
		Namespace:   testNamespace,
		Execution: commonpb.WorkflowExecution{
			WorkflowId: executionInfo.WorkflowID,
			RunId:      executionInfo.RunID,
		},
		WorkflowTypeName: executionInfo.WorkflowTypeName,
		StartTimestamp:   event.GetTimestamp(),
		RunTimeout:       int64(executionInfo.WorkflowRunTimeout),
		TaskID:           taskID,
		TaskList:         taskListName,
	}).Return(nil).Once()

	s.mockShard.SetCurrentTime(s.clusterName, time.Unix(now.Seconds, int64(now.Nanos)).UTC())
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessUpsertWorkflowSearchAttributesTask() {

	execution := commonpb.WorkflowExecution{
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
				WorkflowType:                    &commonpb.WorkflowType{Name: workflowType},
				TaskList:                        &tasklistpb.TaskList{Name: taskListName},
				WorkflowExecutionTimeoutSeconds: 2,
				WorkflowTaskTimeoutSeconds:      1,
			},
		},
	)
	s.Nil(err)

	taskID := int64(59)

	di := addDecisionTaskScheduledEvent(mutableState)

	now := types.TimestampNow()
	transferTask := &persistenceblobs.TransferTaskInfo{
		Version:             s.version,
		NamespaceId:         s.namespaceID,
		WorkflowId:          execution.GetWorkflowId(),
		RunId:               execution.GetRunId(),
		VisibilityTimestamp: now,
		TaskId:              taskID,
		TaskList:            taskListName,
		TaskType:            commongenpb.TaskType_TransferUpsertWorkflowSearchAttributes,
		ScheduleId:          event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, di.ScheduleID, di.Version)
	executionInfo := mutableState.GetExecutionInfo()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockVisibilityMgr.On("UpsertWorkflowExecution", &persistence.UpsertWorkflowExecutionRequest{
		NamespaceID: testNamespaceID,
		Namespace:   testNamespace,
		Execution: commonpb.WorkflowExecution{
			WorkflowId: executionInfo.WorkflowID,
			RunId:      executionInfo.RunID,
		},
		WorkflowTypeName: executionInfo.WorkflowTypeName,
		StartTimestamp:   event.GetTimestamp(),
		WorkflowTimeout:  int64(executionInfo.WorkflowRunTimeout),
		TaskID:           taskID,
		TaskList:         taskListName,
	}).Return(nil).Once()

	s.mockShard.SetCurrentTime(s.clusterName, time.Unix(now.Seconds, int64(now.Nanos)).UTC())
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueStandbyTaskExecutorSuite) createPersistenceMutableState(
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
