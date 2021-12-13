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
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
)

type (
	visibilityQueueTaskExecutorSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller
		mockShard  *shard.ContextTest

		mockVisibilityMgr *manager.MockVisibilityManager
		mockExecutionMgr  *persistence.MockExecutionManager

		logger                      log.Logger
		namespaceID                 namespace.ID
		namespace                   namespace.Name
		version                     int64
		now                         time.Time
		timeSource                  *clock.EventTimeSource
		visibilityQueueTaskExecutor *visibilityQueueTaskExecutor
	}
)

func TestVisibilityQueueTaskExecutorSuite(t *testing.T) {
	s := new(visibilityQueueTaskExecutorSuite)
	suite.Run(t, s)
}

func (s *visibilityQueueTaskExecutorSuite) SetupSuite() {

}

func (s *visibilityQueueTaskExecutorSuite) TearDownSuite() {

}

func (s *visibilityQueueTaskExecutorSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.namespaceID = tests.NamespaceID
	s.namespace = tests.Namespace
	s.version = tests.GlobalNamespaceEntry.FailoverVersion()
	s.now = time.Now().UTC()
	s.timeSource = clock.NewEventTimeSource().Update(s.now)

	s.controller = gomock.NewController(s.T())

	config := tests.NewDynamicConfig()
	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId:          1,
				RangeId:          1,
				TransferAckLevel: 0,
			}},
		config,
	)
	s.mockShard.SetEventsCacheForTesting(events.NewEventsCache(
		s.mockShard.GetShardID(),
		s.mockShard.GetConfig().EventsCacheInitialSize(),
		s.mockShard.GetConfig().EventsCacheMaxSize(),
		s.mockShard.GetConfig().EventsCacheTTL(),
		s.mockShard.GetExecutionManager(),
		false,
		s.mockShard.GetLogger(),
		s.mockShard.GetMetricsClient(),
	))
	s.mockShard.Resource.TimeSource = s.timeSource

	s.mockExecutionMgr = s.mockShard.Resource.ExecutionMgr
	s.mockVisibilityMgr = manager.NewMockVisibilityManager(s.controller)

	mockNamespaceCache := s.mockShard.Resource.NamespaceCache
	mockNamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	mockNamespaceCache.EXPECT().GetNamespace(tests.Namespace).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	mockNamespaceCache.EXPECT().GetNamespaceByID(tests.TargetNamespaceID).Return(tests.GlobalTargetNamespaceEntry, nil).AnyTimes()
	mockNamespaceCache.EXPECT().GetNamespace(tests.TargetNamespace).Return(tests.GlobalTargetNamespaceEntry, nil).AnyTimes()
	mockNamespaceCache.EXPECT().GetNamespaceByID(tests.ParentNamespaceID).Return(tests.GlobalParentNamespaceEntry, nil).AnyTimes()
	mockNamespaceCache.EXPECT().GetNamespace(tests.ParentNamespace).Return(tests.GlobalParentNamespaceEntry, nil).AnyTimes()
	mockNamespaceCache.EXPECT().GetNamespaceByID(tests.ChildNamespaceID).Return(tests.GlobalChildNamespaceEntry, nil).AnyTimes()
	mockNamespaceCache.EXPECT().GetNamespace(tests.ChildNamespace).Return(tests.GlobalChildNamespaceEntry, nil).AnyTimes()

	mockClusterMetadata := s.mockShard.Resource.ClusterMetadata
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, s.version).Return(mockClusterMetadata.GetCurrentClusterName()).AnyTimes()

	s.logger = s.mockShard.GetLogger()

	historyCache := workflow.NewCache(s.mockShard)
	h := &historyEngineImpl{
		currentClusterName: s.mockShard.Resource.GetClusterMetadata().GetCurrentClusterName(),
		shard:              s.mockShard,
		clusterMetadata:    mockClusterMetadata,
		executionManager:   s.mockExecutionMgr,
		historyCache:       historyCache,
		logger:             s.logger,
		tokenSerializer:    common.NewProtoTaskTokenSerializer(),
		metricsClient:      s.mockShard.GetMetricsClient(),
		eventNotifier:      events.NewNotifier(clock.NewRealTimeSource(), metrics.NewNoopMetricsClient(), func(namespace.ID, string) int32 { return 1 }),
	}
	s.mockShard.SetEngineForTesting(h)

	s.visibilityQueueTaskExecutor = newVisibilityQueueTaskExecutor(
		s.mockShard,
		h,
		s.mockVisibilityMgr,
		s.logger,
		s.mockShard.GetMetricsClient(),
		config,
		s.mockShard.Resource.GetMatchingClient(),
	)
}

func (s *visibilityQueueTaskExecutorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *visibilityQueueTaskExecutorSuite) TestProcessCloseExecution() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	parentNamespaceID := "some random parent namespace ID"
	parentInitiatedID := int64(3222)
	parentNamespace := "some random parent namespace Name"
	parentExecution := &commonpb.WorkflowExecution{
		WorkflowId: "some random parent workflow ID",
		RunId:      uuid.New(),
	}

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: timestamp.DurationPtr(2 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
			},
			ParentExecutionInfo: &workflowspb.ParentExecutionInfo{
				NamespaceId: parentNamespaceID,
				Namespace:   parentNamespace,
				Execution:   parentExecution,
				InitiatedId: parentInitiatedID,
			},
		},
	)
	s.Nil(err)

	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	taskID := int64(59)
	event = addCompleteWorkflowEvent(mutableState, event.GetEventId(), nil)

	visibilityTask := &tasks.CloseExecutionVisibilityTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		VisibilityTimestamp: time.Now().UTC(),
		Version:             s.version,
		TaskID:              taskID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockVisibilityMgr.EXPECT().RecordWorkflowExecutionClosed(gomock.Any()).Return(nil)

	err = s.visibilityQueueTaskExecutor.execute(context.Background(), visibilityTask, true)
	s.Nil(err)
}

func (s *visibilityQueueTaskExecutorSuite) TestProcessRecordWorkflowStartedTask() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"
	cronSchedule := "@every 5s"
	backoff := 5 * time.Second

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())

	event, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: timestamp.DurationPtr(2 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
				CronSchedule:             cronSchedule,
			},
			FirstWorkflowTaskBackoff: &backoff,
		},
	)
	s.Nil(err)

	taskID := int64(59)
	di := addWorkflowTaskScheduledEvent(mutableState)

	visibilityTask := &tasks.StartExecutionVisibilityTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		VisibilityTimestamp: time.Now().UTC(),
		Version:             s.version,
		TaskID:              taskID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, di.ScheduleID, di.Version)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockVisibilityMgr.EXPECT().RecordWorkflowExecutionStarted(s.createRecordWorkflowExecutionStartedRequest(s.namespace, event, visibilityTask, mutableState, backoff, taskQueueName)).Return(nil)

	err = s.visibilityQueueTaskExecutor.execute(context.Background(), visibilityTask, true)
	s.Nil(err)
}

func (s *visibilityQueueTaskExecutorSuite) TestProcessUpsertWorkflowSearchAttributes() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())

	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: timestamp.DurationPtr(2 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.NoError(err)

	taskID := int64(59)
	di := addWorkflowTaskScheduledEvent(mutableState)

	visibilityTask := &tasks.UpsertExecutionVisibilityTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version: s.version,
		TaskID:  taskID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, di.ScheduleID, di.Version)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockVisibilityMgr.EXPECT().UpsertWorkflowExecution(s.createUpsertWorkflowSearchAttributesRequest(s.namespace, visibilityTask, mutableState, taskQueueName)).Return(nil)

	err = s.visibilityQueueTaskExecutor.execute(context.Background(), visibilityTask, true)
	s.NoError(err)
}

func (s *visibilityQueueTaskExecutorSuite) createRecordWorkflowExecutionStartedRequest(
	namespaceName namespace.Name,
	startEvent *historypb.HistoryEvent,
	task *tasks.StartExecutionVisibilityTask,
	mutableState workflow.MutableState,
	backoffSeconds time.Duration,
	taskQueueName string,
) *manager.RecordWorkflowExecutionStartedRequest {

	execution := &commonpb.WorkflowExecution{
		WorkflowId: task.WorkflowID,
		RunId:      task.RunID,
	}
	executionInfo := mutableState.GetExecutionInfo()
	executionTimestamp := timestamp.TimeValue(startEvent.GetEventTime()).Add(backoffSeconds)

	return &manager.RecordWorkflowExecutionStartedRequest{
		VisibilityRequestBase: &manager.VisibilityRequestBase{
			Namespace:        namespaceName,
			NamespaceID:      namespace.ID(task.NamespaceID),
			Execution:        *execution,
			WorkflowTypeName: executionInfo.WorkflowTypeName,
			StartTime:        timestamp.TimeValue(startEvent.GetEventTime()),
			ExecutionTime:    executionTimestamp,
			TaskID:           task.TaskID,
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			ShardID:          s.mockShard.GetShardID(),
			TaskQueue:        taskQueueName,
		},
	}
}

func (s *visibilityQueueTaskExecutorSuite) createUpsertWorkflowSearchAttributesRequest(
	namespaceName namespace.Name,
	task *tasks.UpsertExecutionVisibilityTask,
	mutableState workflow.MutableState,
	taskQueueName string,
) *manager.UpsertWorkflowExecutionRequest {

	execution := &commonpb.WorkflowExecution{
		WorkflowId: task.WorkflowID,
		RunId:      task.RunID,
	}
	executionInfo := mutableState.GetExecutionInfo()

	return &manager.UpsertWorkflowExecutionRequest{
		VisibilityRequestBase: &manager.VisibilityRequestBase{
			Namespace:        namespaceName,
			NamespaceID:      namespace.ID(task.NamespaceID),
			Execution:        *execution,
			WorkflowTypeName: executionInfo.WorkflowTypeName,
			StartTime:        timestamp.TimeValue(executionInfo.GetStartTime()),
			ExecutionTime:    timestamp.TimeValue(executionInfo.GetExecutionTime()),
			TaskID:           task.TaskID,
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			TaskQueue:        taskQueueName,
			ShardID:          s.mockShard.GetShardID(),
		},
	}
}

func (s *visibilityQueueTaskExecutorSuite) createPersistenceMutableState(
	ms workflow.MutableState,
	lastEventID int64,
	lastEventVersion int64,
) *persistencespb.WorkflowMutableState {

	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(ms.GetExecutionInfo().GetVersionHistories())
	s.NoError(err)
	err = versionhistory.AddOrUpdateVersionHistoryItem(currentVersionHistory, versionhistory.NewVersionHistoryItem(
		lastEventID, lastEventVersion,
	))
	s.NoError(err)
	return workflow.TestCloneToProto(ms)
}
