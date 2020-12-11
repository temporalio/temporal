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
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/mocks"
	"go.temporal.io/server/common/persistence"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	dc "go.temporal.io/server/common/service/dynamicconfig"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/shard"
)

type (
	visibilityQueueTaskExecutorSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller
		mockShard  *shard.ContextTest

		mockVisibilityMgr *mocks.VisibilityManager
		mockExecutionMgr  *mocks.ExecutionManager

		logger                      log.Logger
		namespaceID                 string
		namespace                   string
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

	s.namespaceID = testNamespaceID
	s.namespace = testNamespace
	s.version = testGlobalNamespaceEntry.GetFailoverVersion()
	s.now = time.Now().UTC()
	s.timeSource = clock.NewEventTimeSource().Update(s.now)

	s.controller = gomock.NewController(s.T())

	config := NewDynamicConfigForTest()
	config.DisableKafkaForVisibility = dc.GetBoolPropertyFn(true)
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
	s.mockShard.EventsCache = events.NewEventsCache(
		convert.Int32Ptr(s.mockShard.GetShardID()),
		s.mockShard.GetConfig().EventsCacheInitialSize(),
		s.mockShard.GetConfig().EventsCacheMaxSize(),
		s.mockShard.GetConfig().EventsCacheTTL(),
		s.mockShard.GetHistoryManager(),
		false,
		s.mockShard.GetLogger(),
		s.mockShard.GetMetricsClient(),
	)
	s.mockShard.Resource.TimeSource = s.timeSource

	s.mockExecutionMgr = s.mockShard.Resource.ExecutionMgr
	s.mockVisibilityMgr = s.mockShard.Resource.VisibilityMgr

	mockNamespaceCache := s.mockShard.Resource.NamespaceCache
	mockNamespaceCache.EXPECT().GetNamespaceByID(testNamespaceID).Return(testGlobalNamespaceEntry, nil).AnyTimes()
	mockNamespaceCache.EXPECT().GetNamespace(testNamespace).Return(testGlobalNamespaceEntry, nil).AnyTimes()
	mockNamespaceCache.EXPECT().GetNamespaceByID(testTargetNamespaceID).Return(testGlobalTargetNamespaceEntry, nil).AnyTimes()
	mockNamespaceCache.EXPECT().GetNamespace(testTargetNamespace).Return(testGlobalTargetNamespaceEntry, nil).AnyTimes()
	mockNamespaceCache.EXPECT().GetNamespaceByID(testParentNamespaceID).Return(testGlobalParentNamespaceEntry, nil).AnyTimes()
	mockNamespaceCache.EXPECT().GetNamespace(testParentNamespace).Return(testGlobalParentNamespaceEntry, nil).AnyTimes()
	mockNamespaceCache.EXPECT().GetNamespaceByID(testChildNamespaceID).Return(testGlobalChildNamespaceEntry, nil).AnyTimes()
	mockNamespaceCache.EXPECT().GetNamespace(testChildNamespace).Return(testGlobalChildNamespaceEntry, nil).AnyTimes()

	mockClusterMetadata := s.mockShard.Resource.ClusterMetadata
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.version).Return(mockClusterMetadata.GetCurrentClusterName()).AnyTimes()

	s.logger = s.mockShard.GetLogger()

	historyCache := newHistoryCache(s.mockShard)
	h := &historyEngineImpl{
		currentClusterName: s.mockShard.GetService().GetClusterMetadata().GetCurrentClusterName(),
		shard:              s.mockShard,
		clusterMetadata:    mockClusterMetadata,
		executionManager:   s.mockExecutionMgr,
		historyCache:       historyCache,
		logger:             s.logger,
		tokenSerializer:    common.NewProtoTaskTokenSerializer(),
		metricsClient:      s.mockShard.GetMetricsClient(),
		eventNotifier:      events.NewNotifier(clock.NewRealTimeSource(), metrics.NewClient(tally.NoopScope, metrics.History), func(string, string) int32 { return 1 }),
	}
	s.mockShard.SetEngine(h)

	s.visibilityQueueTaskExecutor = newVisibilityQueueTaskExecutor(
		s.mockShard,
		h,
		s.logger,
		s.mockShard.GetMetricsClient(),
		config,
	)
}

func (s *visibilityQueueTaskExecutorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.Finish(s.T())
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

	mutableState := newMutableStateBuilderWithVersionHistoriesForTest(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID,
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

	visibilityTask := &persistencespb.VisibilityTaskInfo{
		Version:     s.version,
		NamespaceId: s.namespaceID,
		WorkflowId:  execution.GetWorkflowId(),
		RunId:       execution.GetRunId(),
		TaskId:      taskID,
		TaskType:    enumsspb.TASK_TYPE_VISIBILITY_CLOSE_EXECUTION,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockVisibilityMgr.On("RecordWorkflowExecutionClosedV2", mock.Anything).Return(nil).Once()

	err = s.visibilityQueueTaskExecutor.execute(visibilityTask, true)
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

	mutableState := newMutableStateBuilderWithVersionHistoriesForTest(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())

	event, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID,
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

	visibiltyTask := &persistencespb.VisibilityTaskInfo{
		Version:     s.version,
		NamespaceId: s.namespaceID,
		WorkflowId:  execution.GetWorkflowId(),
		RunId:       execution.GetRunId(),
		TaskId:      taskID,
		TaskType:    enumsspb.TASK_TYPE_VISIBILITY_START_EXECUTION,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, di.ScheduleID, di.Version)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockVisibilityMgr.On("RecordWorkflowExecutionStartedV2", s.createRecordWorkflowExecutionStartedRequest(s.namespace, event, visibiltyTask, mutableState, backoff, taskQueueName)).Once().Return(nil)

	err = s.visibilityQueueTaskExecutor.execute(visibiltyTask, true)
	s.Nil(err)
}

func (s *visibilityQueueTaskExecutorSuite) TestProcessUpsertWorkflowSearchAttributes() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := newMutableStateBuilderWithVersionHistoriesForTest(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())

	event, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: timestamp.DurationPtr(2 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	taskID := int64(59)
	di := addWorkflowTaskScheduledEvent(mutableState)

	visibilityTask := &persistencespb.VisibilityTaskInfo{
		Version:     s.version,
		NamespaceId: s.namespaceID,
		WorkflowId:  execution.GetWorkflowId(),
		RunId:       execution.GetRunId(),
		TaskId:      taskID,
		TaskType:    enumsspb.TASK_TYPE_VISIBILITY_UPSERT_EXECUTION,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, di.ScheduleID, di.Version)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockVisibilityMgr.On("UpsertWorkflowExecutionV2", s.createUpsertWorkflowSearchAttributesRequest(s.namespace, event, visibilityTask, mutableState, taskQueueName)).Once().Return(nil)

	err = s.visibilityQueueTaskExecutor.execute(visibilityTask, true)
	s.Nil(err)
}

func (s *visibilityQueueTaskExecutorSuite) createRecordWorkflowExecutionStartedRequest(
	namespace string,
	startEvent *historypb.HistoryEvent,
	task *persistencespb.VisibilityTaskInfo,
	mutableState mutableState,
	backoffSeconds time.Duration,
	taskQueueName string,
) *persistence.RecordWorkflowExecutionStartedRequest {

	execution := &commonpb.WorkflowExecution{
		WorkflowId: task.GetWorkflowId(),
		RunId:      task.GetRunId(),
	}
	executionInfo := mutableState.GetExecutionInfo()
	executionTimestamp := timestamp.TimeValue(startEvent.GetEventTime()).Add(backoffSeconds)

	return &persistence.RecordWorkflowExecutionStartedRequest{
		VisibilityRequestBase: &p.VisibilityRequestBase{
			Namespace:          namespace,
			NamespaceID:        task.GetNamespaceId(),
			Execution:          *execution,
			WorkflowTypeName:   executionInfo.WorkflowTypeName,
			StartTimestamp:     timestamp.TimeValue(startEvent.GetEventTime()).UnixNano(),
			ExecutionTimestamp: executionTimestamp.UnixNano(),
			TaskID:             task.GetTaskId(),
			Status:             enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			ShardID:            s.mockShard.GetShardID(),
			TaskQueue:          taskQueueName,
		},
		RunTimeout: int64(timestamp.DurationValue(executionInfo.WorkflowRunTimeout).Round(time.Second).Seconds()),
	}
}

func (s *visibilityQueueTaskExecutorSuite) createUpsertWorkflowSearchAttributesRequest(
	namespace string,
	startEvent *historypb.HistoryEvent,
	task *persistencespb.VisibilityTaskInfo,
	mutableState mutableState,
	taskQueueName string,
) *persistence.UpsertWorkflowExecutionRequest {

	execution := &commonpb.WorkflowExecution{
		WorkflowId: task.GetWorkflowId(),
		RunId:      task.GetRunId(),
	}
	executionInfo := mutableState.GetExecutionInfo()

	return &persistence.UpsertWorkflowExecutionRequest{
		VisibilityRequestBase: &p.VisibilityRequestBase{
			Namespace:        namespace,
			NamespaceID:      task.GetNamespaceId(),
			Execution:        *execution,
			WorkflowTypeName: executionInfo.WorkflowTypeName,
			StartTimestamp:   timestamp.TimeValue(startEvent.GetEventTime()).UnixNano(),
			TaskID:           task.GetTaskId(),
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			TaskQueue:        taskQueueName,
			ShardID:          s.mockShard.GetShardID(),
		},
		WorkflowTimeout: int64(timestamp.DurationValue(executionInfo.WorkflowRunTimeout).Round(time.Second).Seconds()),
	}
}

func (s *visibilityQueueTaskExecutorSuite) createPersistenceMutableState(
	ms mutableState,
	lastEventID int64,
	lastEventVersion int64,
) *persistencespb.WorkflowMutableState {

	if ms.GetExecutionInfo().GetVersionHistories() != nil {
		currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(ms.GetExecutionInfo().GetVersionHistories())
		s.NoError(err)
		err = versionhistory.AddOrUpdateVersionHistoryItem(currentVersionHistory, versionhistory.NewVersionHistoryItem(
			lastEventID, lastEventVersion,
		))
		s.NoError(err)
	}

	return createMutableState(ms)
}
