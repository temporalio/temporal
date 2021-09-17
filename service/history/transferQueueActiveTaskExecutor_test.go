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
	"github.com/uber-go/tally"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/common/persistence/visibility"

	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/history/consts"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/convert"
	dc "go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	warchiver "go.temporal.io/server/service/worker/archiver"
	"go.temporal.io/server/service/worker/parentclosepolicy"
)

type (
	transferQueueActiveTaskExecutorSuite struct {
		suite.Suite
		*require.Assertions

		controller                   *gomock.Controller
		mockShard                    *shard.ContextTest
		mockTxProcessor              *MocktransferQueueProcessor
		mockReplicationProcessor     *MockReplicatorQueueProcessor
		mockTimerProcessor           *MocktimerQueueProcessor
		mockNamespaceCache           *namespace.MockCache
		mockMatchingClient           *matchingservicemock.MockMatchingServiceClient
		mockHistoryClient            *historyservicemock.MockHistoryServiceClient
		mockClusterMetadata          *cluster.MockMetadata
		mockSearchAttributesProvider *searchattribute.MockProvider

		mockExecutionMgr            *persistence.MockExecutionManager
		mockArchivalClient          *warchiver.MockClient
		mockArchivalMetadata        *archiver.MockArchivalMetadata
		mockArchiverProvider        *provider.MockArchiverProvider
		mockParentClosePolicyClient *parentclosepolicy.MockClient

		logger                          log.Logger
		namespaceID                     string
		namespace                       string
		namespaceEntry                  *namespace.CacheEntry
		targetNamespaceID               string
		targetNamespace                 string
		targetNamespaceEntry            *namespace.CacheEntry
		childNamespaceID                string
		childNamespace                  string
		childNamespaceEntry             *namespace.CacheEntry
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

	s.namespaceID = tests.NamespaceID
	s.namespace = tests.Namespace
	s.namespaceEntry = tests.GlobalNamespaceEntry
	s.targetNamespaceID = tests.TargetNamespaceID
	s.targetNamespace = tests.TargetNamespace
	s.targetNamespaceEntry = tests.GlobalTargetNamespaceEntry
	s.childNamespaceID = tests.ChildNamespaceID
	s.childNamespace = tests.ChildNamespace
	s.childNamespaceEntry = tests.GlobalChildNamespaceEntry
	s.version = s.namespaceEntry.GetFailoverVersion()
	s.now = time.Now().UTC()
	s.timeSource = clock.NewEventTimeSource().Update(s.now)

	s.controller = gomock.NewController(s.T())
	s.mockTxProcessor = NewMocktransferQueueProcessor(s.controller)
	s.mockReplicationProcessor = NewMockReplicatorQueueProcessor(s.controller)
	s.mockTimerProcessor = NewMocktimerQueueProcessor(s.controller)
	s.mockTxProcessor.EXPECT().NotifyNewTask(gomock.Any(), gomock.Any()).AnyTimes()
	s.mockReplicationProcessor.EXPECT().notifyNewTask().AnyTimes()
	s.mockTimerProcessor.EXPECT().NotifyNewTimers(gomock.Any(), gomock.Any()).AnyTimes()

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
	s.mockShard.EventsCache = events.NewEventsCache(
		s.mockShard.GetShardID(),
		s.mockShard.GetConfig().EventsCacheInitialSize(),
		s.mockShard.GetConfig().EventsCacheMaxSize(),
		s.mockShard.GetConfig().EventsCacheTTL(),
		s.mockShard.GetExecutionManager(),
		false,
		s.mockShard.GetLogger(),
		s.mockShard.GetMetricsClient(),
	)
	s.mockShard.Resource.TimeSource = s.timeSource

	s.mockParentClosePolicyClient = parentclosepolicy.NewMockClient(s.controller)
	s.mockArchivalClient = warchiver.NewMockClient(s.controller)
	s.mockMatchingClient = s.mockShard.Resource.MatchingClient
	s.mockHistoryClient = s.mockShard.Resource.HistoryClient
	s.mockExecutionMgr = s.mockShard.Resource.ExecutionMgr
	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata
	s.mockSearchAttributesProvider = s.mockShard.Resource.SearchAttributesProvider
	s.mockArchivalMetadata = s.mockShard.Resource.ArchivalMetadata
	s.mockArchiverProvider = s.mockShard.Resource.ArchiverProvider
	s.mockNamespaceCache = s.mockShard.Resource.NamespaceCache
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(tests.Namespace).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.TargetNamespaceID).Return(tests.GlobalTargetNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(tests.TargetNamespace).Return(tests.GlobalTargetNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.ParentNamespaceID).Return(tests.GlobalParentNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(tests.ParentNamespace).Return(tests.GlobalParentNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.ChildNamespaceID).Return(tests.GlobalChildNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(tests.ChildNamespace).Return(tests.GlobalChildNamespaceEntry, nil).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.version).Return(s.mockClusterMetadata.GetCurrentClusterName()).AnyTimes()

	s.logger = s.mockShard.GetLogger()

	historyCache := workflow.NewCache(s.mockShard)
	h := &historyEngineImpl{
		currentClusterName: s.mockShard.GetService().GetClusterMetadata().GetCurrentClusterName(),
		shard:              s.mockShard,
		clusterMetadata:    s.mockClusterMetadata,
		executionManager:   s.mockExecutionMgr,
		historyCache:       historyCache,
		logger:             s.logger,
		tokenSerializer:    common.NewProtoTaskTokenSerializer(),
		metricsClient:      s.mockShard.GetMetricsClient(),
		eventNotifier:      events.NewNotifier(clock.NewRealTimeSource(), metrics.NewClient(tally.NoopScope, metrics.History), func(string, string) int32 { return 1 }),
		txProcessor:        s.mockTxProcessor,
		timerProcessor:     s.mockTimerProcessor,
		archivalClient:     s.mockArchivalClient,
	}
	s.mockShard.SetEngine(h)

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
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessActivityTask_Success() {

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
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType: &commonpb.WorkflowType{Name: workflowType},
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: taskQueueName,
					Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
				},
				WorkflowExecutionTimeout: timestamp.DurationPtr(2 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	taskID := int64(59)
	activityID := "activity-1"
	activityType := "some random activity type"
	event, ai := addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID, activityType, taskQueueName, &commonpb.Payloads{}, 1*time.Second, 1*time.Second, 1*time.Second, 1*time.Second)

	transferTask := &persistencespb.TransferTaskInfo{
		Version:           s.version,
		NamespaceId:       s.namespaceID,
		TargetNamespaceId: tests.TargetNamespaceID,
		WorkflowId:        execution.GetWorkflowId(),
		RunId:             execution.GetRunId(),
		TaskId:            taskID,
		TaskQueue:         taskQueueName,
		TaskType:          enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK,
		ScheduleId:        event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.EXPECT().AddActivityTask(gomock.Any(), s.createAddActivityTaskRequest(transferTask, ai), gomock.Any()).Return(&matchingservice.AddActivityTaskResponse{}, nil)

	err = s.transferQueueActiveTaskExecutor.execute(context.Background(), transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessActivityTask_Duplication() {

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

	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	taskID := int64(59)
	activityID := "activity-1"
	activityType := "some random activity type"
	event, ai := addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID, activityType, taskQueueName, &commonpb.Payloads{}, 1*time.Second, 1*time.Second, 1*time.Second, 1*time.Second)

	transferTask := &persistencespb.TransferTaskInfo{
		Version:           s.version,
		NamespaceId:       s.namespaceID,
		TargetNamespaceId: s.targetNamespaceID,
		WorkflowId:        execution.GetWorkflowId(),
		RunId:             execution.GetRunId(),
		TaskId:            taskID,
		TaskQueue:         taskQueueName,
		TaskType:          enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK,
		ScheduleId:        event.GetEventId(),
	}

	event = addActivityTaskStartedEvent(mutableState, event.GetEventId(), "")
	ai.StartedId = event.GetEventId()
	event = addActivityTaskCompletedEvent(mutableState, ai.ScheduleId, ai.StartedId, nil, "")
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	err = s.transferQueueActiveTaskExecutor.execute(context.Background(), transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessWorkflowTask_FirstWorkflowTask() {

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
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType: &commonpb.WorkflowType{Name: workflowType},
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: taskQueueName,
					Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
				},
				WorkflowExecutionTimeout: timestamp.DurationPtr(2 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	taskID := int64(59)
	di := addWorkflowTaskScheduledEvent(mutableState)

	transferTask := &persistencespb.TransferTaskInfo{
		Version:     s.version,
		NamespaceId: s.namespaceID,
		WorkflowId:  execution.GetWorkflowId(),
		RunId:       execution.GetRunId(),
		TaskId:      taskID,
		TaskQueue:   taskQueueName,
		TaskType:    enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK,
		ScheduleId:  di.ScheduleID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, di.ScheduleID, di.Version)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.EXPECT().AddWorkflowTask(gomock.Any(), s.createAddWorkflowTaskRequest(transferTask, mutableState), gomock.Any()).Return(&matchingservice.AddWorkflowTaskResponse{}, nil)

	err = s.transferQueueActiveTaskExecutor.execute(context.Background(), transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessWorkflowTask_NonFirstWorkflowTask() {

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
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType: &commonpb.WorkflowType{Name: workflowType},
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: taskQueueName,
					Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
				},
				WorkflowExecutionTimeout: timestamp.DurationPtr(2 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")
	s.NotNil(event)

	// make another round of workflow task
	taskID := int64(59)
	di = addWorkflowTaskScheduledEvent(mutableState)

	transferTask := &persistencespb.TransferTaskInfo{
		Version:     s.version,
		NamespaceId: s.namespaceID,
		WorkflowId:  execution.GetWorkflowId(),
		RunId:       execution.GetRunId(),
		TaskId:      taskID,
		TaskQueue:   taskQueueName,
		TaskType:    enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK,
		ScheduleId:  di.ScheduleID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, di.ScheduleID, di.Version)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.EXPECT().AddWorkflowTask(gomock.Any(), s.createAddWorkflowTaskRequest(transferTask, mutableState), gomock.Any()).Return(&matchingservice.AddWorkflowTaskResponse{}, nil)

	err = s.transferQueueActiveTaskExecutor.execute(context.Background(), transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessWorkflowTask_Sticky_NonFirstWorkflowTask() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"
	stickyTaskQueueName := "some random sticky task queue"
	stickyTaskQueueTimeout := timestamp.DurationFromSeconds(233)

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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
		},
	)
	s.Nil(err)

	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")
	s.NotNil(event)
	// set the sticky taskqueue attr
	executionInfo := mutableState.GetExecutionInfo()
	executionInfo.StickyTaskQueue = stickyTaskQueueName
	executionInfo.StickyScheduleToStartTimeout = stickyTaskQueueTimeout

	// make another round of workflow task
	taskID := int64(59)
	di = addWorkflowTaskScheduledEvent(mutableState)

	transferTask := &persistencespb.TransferTaskInfo{
		Version:     s.version,
		NamespaceId: s.namespaceID,
		WorkflowId:  execution.GetWorkflowId(),
		RunId:       execution.GetRunId(),
		TaskId:      taskID,
		TaskQueue:   stickyTaskQueueName,
		TaskType:    enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK,
		ScheduleId:  di.ScheduleID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, di.ScheduleID, di.Version)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.EXPECT().AddWorkflowTask(gomock.Any(), s.createAddWorkflowTaskRequest(transferTask, mutableState), gomock.Any()).Return(&matchingservice.AddWorkflowTaskResponse{}, nil)

	err = s.transferQueueActiveTaskExecutor.execute(context.Background(), transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessWorkflowTask_WorkflowTaskNotSticky_MutableStateSticky() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"
	stickyTaskQueueName := "some random sticky task queue"
	stickyTaskQueueTimeout := timestamp.DurationFromSeconds(233)

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType: &commonpb.WorkflowType{Name: workflowType},
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: taskQueueName,
					Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
				},
				WorkflowExecutionTimeout: timestamp.DurationPtr(2 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")
	s.NotNil(event)
	// set the sticky taskqueue attr
	executionInfo := mutableState.GetExecutionInfo()
	executionInfo.StickyTaskQueue = stickyTaskQueueName
	executionInfo.StickyScheduleToStartTimeout = stickyTaskQueueTimeout

	// make another round of workflow task
	taskID := int64(59)
	di = addWorkflowTaskScheduledEvent(mutableState)

	transferTask := &persistencespb.TransferTaskInfo{
		Version:     s.version,
		NamespaceId: s.namespaceID,
		WorkflowId:  execution.GetWorkflowId(),
		RunId:       execution.GetRunId(),
		TaskId:      taskID,
		TaskQueue:   taskQueueName,
		TaskType:    enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK,
		ScheduleId:  di.ScheduleID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, di.ScheduleID, di.Version)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.EXPECT().AddWorkflowTask(gomock.Any(), s.createAddWorkflowTaskRequest(transferTask, mutableState), gomock.Any()).Return(&matchingservice.AddWorkflowTaskResponse{}, nil)

	err = s.transferQueueActiveTaskExecutor.execute(context.Background(), transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessWorkflowTask_Duplication() {

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

	taskID := int64(4096)
	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	transferTask := &persistencespb.TransferTaskInfo{
		Version:     s.version,
		NamespaceId: s.namespaceID,
		WorkflowId:  execution.GetWorkflowId(),
		RunId:       execution.GetRunId(),
		TaskId:      taskID,
		TaskQueue:   taskQueueName,
		TaskType:    enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK,
		ScheduleId:  di.ScheduleID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	err = s.transferQueueActiveTaskExecutor.execute(context.Background(), transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessCloseExecution_HasParent() {

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

	transferTask := &persistencespb.TransferTaskInfo{
		Version:     s.version,
		NamespaceId: s.namespaceID,
		WorkflowId:  execution.GetWorkflowId(),
		RunId:       execution.GetRunId(),
		TaskId:      taskID,
		TaskQueue:   taskQueueName,
		TaskType:    enumsspb.TASK_TYPE_TRANSFER_CLOSE_EXECUTION,
		ScheduleId:  event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().RecordChildExecutionCompleted(gomock.Any(), &historyservice.RecordChildExecutionCompletedRequest{
		NamespaceId:        parentNamespaceID,
		WorkflowExecution:  parentExecution,
		InitiatedId:        parentInitiatedID,
		CompletedExecution: &execution,
		CompletionEvent:    event,
	}).Return(nil, nil)
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewDisabledArchvialConfig())

	err = s.transferQueueActiveTaskExecutor.execute(context.Background(), transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessCloseExecution_NoParent() {

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

	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	taskID := int64(59)
	event = addCompleteWorkflowEvent(mutableState, event.GetEventId(), nil)

	transferTask := &persistencespb.TransferTaskInfo{
		Version:     s.version,
		NamespaceId: s.namespaceID,
		WorkflowId:  execution.GetWorkflowId(),
		RunId:       execution.GetRunId(),
		TaskId:      taskID,
		TaskQueue:   taskQueueName,
		TaskType:    enumsspb.TASK_TYPE_TRANSFER_CLOSE_EXECUTION,
		ScheduleId:  event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "random URI"))
	s.mockArchivalClient.EXPECT().Archive(gomock.Any(), gomock.Any()).Return(nil, nil)
	s.mockSearchAttributesProvider.EXPECT().GetSearchAttributes(gomock.Any(), false)

	err = s.transferQueueActiveTaskExecutor.execute(context.Background(), transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessCloseExecution_NoParent_HasFewChildren() {

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

	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()

	dt := enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION
	parentClosePolicy1 := enumspb.PARENT_CLOSE_POLICY_ABANDON
	parentClosePolicy2 := enumspb.PARENT_CLOSE_POLICY_TERMINATE
	parentClosePolicy3 := enumspb.PARENT_CLOSE_POLICY_REQUEST_CANCEL

	event, _ = mutableState.AddWorkflowTaskCompletedEvent(di.ScheduleID, di.StartedID, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity: "some random identity",
		Commands: []*commandpb.Command{
			{
				CommandType: dt,
				Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
					WorkflowId: "child workflow1",
					WorkflowType: &commonpb.WorkflowType{
						Name: "child workflow type",
					},
					TaskQueue:         &taskqueuepb.TaskQueue{Name: taskQueueName},
					Input:             payloads.EncodeString("random input"),
					ParentClosePolicy: parentClosePolicy1,
				}},
			},
			{
				CommandType: dt,
				Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
					WorkflowId: "child workflow2",
					WorkflowType: &commonpb.WorkflowType{
						Name: "child workflow type",
					},
					TaskQueue:         &taskqueuepb.TaskQueue{Name: taskQueueName},
					Input:             payloads.EncodeString("random input"),
					ParentClosePolicy: parentClosePolicy2,
				}},
			},
			{
				CommandType: dt,
				Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
					WorkflowId: "child workflow3",
					WorkflowType: &commonpb.WorkflowType{
						Name: "child workflow type",
					},
					TaskQueue:         &taskqueuepb.TaskQueue{Name: taskQueueName},
					Input:             payloads.EncodeString("random input"),
					ParentClosePolicy: parentClosePolicy3,
				}},
			},
		},
	}, configs.DefaultHistoryMaxAutoResetPoints)

	_, _, err = mutableState.AddStartChildWorkflowExecutionInitiatedEvent(event.GetEventId(), uuid.New(), &commandpb.StartChildWorkflowExecutionCommandAttributes{
		WorkflowId: "child workflow1",
		WorkflowType: &commonpb.WorkflowType{
			Name: "child workflow type",
		},
		TaskQueue:         &taskqueuepb.TaskQueue{Name: taskQueueName},
		Input:             payloads.EncodeString("random input"),
		ParentClosePolicy: parentClosePolicy1,
	})
	s.Nil(err)
	_, _, err = mutableState.AddStartChildWorkflowExecutionInitiatedEvent(event.GetEventId(), uuid.New(), &commandpb.StartChildWorkflowExecutionCommandAttributes{
		WorkflowId: "child workflow2",
		WorkflowType: &commonpb.WorkflowType{
			Name: "child workflow type",
		},
		TaskQueue:         &taskqueuepb.TaskQueue{Name: taskQueueName},
		Input:             payloads.EncodeString("random input"),
		ParentClosePolicy: parentClosePolicy2,
	})
	s.Nil(err)
	_, _, err = mutableState.AddStartChildWorkflowExecutionInitiatedEvent(event.GetEventId(), uuid.New(), &commandpb.StartChildWorkflowExecutionCommandAttributes{
		WorkflowId: "child workflow3",
		WorkflowType: &commonpb.WorkflowType{
			Name: "child workflow type",
		},
		TaskQueue:         &taskqueuepb.TaskQueue{Name: taskQueueName},
		Input:             payloads.EncodeString("random input"),
		ParentClosePolicy: parentClosePolicy3,
	})
	s.Nil(err)

	mutableState.FlushBufferedEvents()

	taskID := int64(59)
	event = addCompleteWorkflowEvent(mutableState, event.GetEventId(), nil)

	transferTask := &persistencespb.TransferTaskInfo{
		Version:     s.version,
		NamespaceId: s.namespaceID,
		WorkflowId:  execution.GetWorkflowId(),
		RunId:       execution.GetRunId(),
		TaskId:      taskID,
		TaskQueue:   taskQueueName,
		TaskType:    enumsspb.TASK_TYPE_TRANSFER_CLOSE_EXECUTION,
		ScheduleId:  event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewDisabledArchvialConfig())
	s.mockHistoryClient.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, nil)
	s.mockHistoryClient.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, nil)

	err = s.transferQueueActiveTaskExecutor.execute(context.Background(), transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessCloseExecution_NoParent_HasManyChildren() {

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

	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()

	dt := enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION
	parentClosePolicy := enumspb.PARENT_CLOSE_POLICY_TERMINATE
	var commands []*commandpb.Command
	for i := 0; i < 10; i++ {
		commands = append(commands, &commandpb.Command{
			CommandType: dt,
			Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
				WorkflowId: "child workflow" + convert.IntToString(i),
				WorkflowType: &commonpb.WorkflowType{
					Name: "child workflow type",
				},
				TaskQueue:         &taskqueuepb.TaskQueue{Name: taskQueueName},
				Input:             payloads.EncodeString("random input"),
				ParentClosePolicy: parentClosePolicy,
			}},
		})
	}

	event, _ = mutableState.AddWorkflowTaskCompletedEvent(di.ScheduleID, di.StartedID, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity: "some random identity",
		Commands: commands,
	}, configs.DefaultHistoryMaxAutoResetPoints)

	for i := 0; i < 10; i++ {
		_, _, err = mutableState.AddStartChildWorkflowExecutionInitiatedEvent(event.GetEventId(), uuid.New(), &commandpb.StartChildWorkflowExecutionCommandAttributes{
			WorkflowId: "child workflow" + convert.IntToString(i),
			WorkflowType: &commonpb.WorkflowType{
				Name: "child workflow type",
			},
			TaskQueue:         &taskqueuepb.TaskQueue{Name: taskQueueName},
			Input:             payloads.EncodeString("random input"),
			ParentClosePolicy: parentClosePolicy,
		})
		s.Nil(err)
	}

	mutableState.FlushBufferedEvents()

	taskID := int64(59)
	event = addCompleteWorkflowEvent(mutableState, event.GetEventId(), nil)

	transferTask := &persistencespb.TransferTaskInfo{
		Version:     s.version,
		NamespaceId: s.namespaceID,
		WorkflowId:  execution.GetWorkflowId(),
		RunId:       execution.GetRunId(),
		TaskId:      taskID,
		TaskQueue:   taskQueueName,
		TaskType:    enumsspb.TASK_TYPE_TRANSFER_CLOSE_EXECUTION,
		ScheduleId:  event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewDisabledArchvialConfig())
	s.mockParentClosePolicyClient.EXPECT().SendParentClosePolicyRequest(gomock.Any()).Return(nil)

	err = s.transferQueueActiveTaskExecutor.execute(context.Background(), transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessCloseExecution_NoParent_HasManyAbandonedChildren() {

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

	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()

	dt := enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION
	parentClosePolicy := enumspb.PARENT_CLOSE_POLICY_ABANDON
	var commands []*commandpb.Command
	for i := 0; i < 10; i++ {
		commands = append(commands, &commandpb.Command{
			CommandType: dt,
			Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
				WorkflowId: "child workflow" + convert.IntToString(i),
				WorkflowType: &commonpb.WorkflowType{
					Name: "child workflow type",
				},
				TaskQueue:         &taskqueuepb.TaskQueue{Name: taskQueueName},
				Input:             payloads.EncodeString("random input"),
				ParentClosePolicy: parentClosePolicy,
			}},
		})
	}

	event, _ = mutableState.AddWorkflowTaskCompletedEvent(di.ScheduleID, di.StartedID, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity: "some random identity",
		Commands: commands,
	}, configs.DefaultHistoryMaxAutoResetPoints)

	for i := 0; i < 10; i++ {
		_, _, err = mutableState.AddStartChildWorkflowExecutionInitiatedEvent(event.GetEventId(), uuid.New(), &commandpb.StartChildWorkflowExecutionCommandAttributes{
			WorkflowId: "child workflow" + convert.IntToString(i),
			WorkflowType: &commonpb.WorkflowType{
				Name: "child workflow type",
			},
			TaskQueue:         &taskqueuepb.TaskQueue{Name: taskQueueName},
			Input:             payloads.EncodeString("random input"),
			ParentClosePolicy: parentClosePolicy,
		})
		s.Nil(err)
	}

	mutableState.FlushBufferedEvents()

	taskID := int64(59)
	event = addCompleteWorkflowEvent(mutableState, event.GetEventId(), nil)

	transferTask := &persistencespb.TransferTaskInfo{
		Version:     s.version,
		NamespaceId: s.namespaceID,
		WorkflowId:  execution.GetWorkflowId(),
		RunId:       execution.GetRunId(),
		TaskId:      taskID,
		TaskQueue:   taskQueueName,
		TaskType:    enumsspb.TASK_TYPE_TRANSFER_CLOSE_EXECUTION,
		ScheduleId:  event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewDisabledArchvialConfig())

	err = s.transferQueueActiveTaskExecutor.execute(context.Background(), transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessCancelExecution_Success() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	targetExecution := commonpb.WorkflowExecution{
		WorkflowId: "some random target workflow ID",
		RunId:      uuid.New(),
	}

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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
		},
	)
	s.Nil(err)

	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	taskID := int64(59)
	event, rci := addRequestCancelInitiatedEvent(mutableState, event.GetEventId(), uuid.New(), tests.TargetNamespace, targetExecution.GetWorkflowId(), targetExecution.GetRunId())

	transferTask := &persistencespb.TransferTaskInfo{
		Version:           s.version,
		NamespaceId:       s.namespaceID,
		WorkflowId:        execution.GetWorkflowId(),
		RunId:             execution.GetRunId(),
		TargetNamespaceId: s.targetNamespaceID,
		TargetWorkflowId:  targetExecution.GetWorkflowId(),
		TargetRunId:       targetExecution.GetRunId(),
		TaskId:            taskID,
		TaskQueue:         taskQueueName,
		TaskType:          enumsspb.TASK_TYPE_TRANSFER_CANCEL_EXECUTION,
		ScheduleId:        event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), s.createRequestCancelWorkflowExecutionRequest(s.targetNamespace, transferTask, rci)).Return(nil, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.version).Return(cluster.TestCurrentClusterName).AnyTimes()

	err = s.transferQueueActiveTaskExecutor.execute(context.Background(), transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessCancelExecution_Failure() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	targetExecution := commonpb.WorkflowExecution{
		WorkflowId: "some random target workflow ID",
		RunId:      uuid.New(),
	}

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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
		},
	)
	s.Nil(err)

	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	taskID := int64(59)
	event, rci := addRequestCancelInitiatedEvent(mutableState, event.GetEventId(), uuid.New(), tests.TargetNamespace, targetExecution.GetWorkflowId(), targetExecution.GetRunId())

	transferTask := &persistencespb.TransferTaskInfo{
		Version:           s.version,
		NamespaceId:       s.namespaceID,
		WorkflowId:        execution.GetWorkflowId(),
		RunId:             execution.GetRunId(),
		TargetNamespaceId: s.targetNamespaceID,
		TargetWorkflowId:  targetExecution.GetWorkflowId(),
		TargetRunId:       targetExecution.GetRunId(),
		TaskId:            taskID,
		TaskQueue:         taskQueueName,
		TaskType:          enumsspb.TASK_TYPE_TRANSFER_CANCEL_EXECUTION,
		ScheduleId:        event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), s.createRequestCancelWorkflowExecutionRequest(s.targetNamespace, transferTask, rci)).Return(nil, serviceerror.NewNotFound(""))
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.version).Return(cluster.TestCurrentClusterName).AnyTimes()

	err = s.transferQueueActiveTaskExecutor.execute(context.Background(), transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessCancelExecution_Duplication() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	targetExecution := commonpb.WorkflowExecution{
		WorkflowId: "some random target workflow ID",
		RunId:      uuid.New(),
	}

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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
		},
	)
	s.Nil(err)

	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	taskID := int64(59)
	event, _ = addRequestCancelInitiatedEvent(mutableState, event.GetEventId(), uuid.New(), tests.TargetNamespace, targetExecution.GetWorkflowId(), targetExecution.GetRunId())

	transferTask := &persistencespb.TransferTaskInfo{
		Version:           s.version,
		NamespaceId:       s.namespaceID,
		WorkflowId:        execution.GetWorkflowId(),
		RunId:             execution.GetRunId(),
		TargetNamespaceId: s.targetNamespaceID,
		TargetWorkflowId:  targetExecution.GetWorkflowId(),
		TargetRunId:       targetExecution.GetRunId(),
		TaskId:            taskID,
		TaskQueue:         taskQueueName,
		TaskType:          enumsspb.TASK_TYPE_TRANSFER_CANCEL_EXECUTION,
		ScheduleId:        event.GetEventId(),
	}

	event = addCancelRequestedEvent(mutableState, event.GetEventId(), tests.TargetNamespaceID, targetExecution.GetWorkflowId(), targetExecution.GetRunId())
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	err = s.transferQueueActiveTaskExecutor.execute(context.Background(), transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessSignalExecution_Success() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	targetExecution := commonpb.WorkflowExecution{
		WorkflowId: "some random target workflow ID",
		RunId:      uuid.New(),
	}
	signalName := "some random signal name"
	signalInput := payloads.EncodeString("some random signal input")
	signalControl := "some random signal control"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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
		},
	)
	s.Nil(err)

	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	taskID := int64(59)
	event, si := addRequestSignalInitiatedEvent(mutableState, event.GetEventId(), uuid.New(),
		tests.TargetNamespace, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), signalName, signalInput, signalControl)

	transferTask := &persistencespb.TransferTaskInfo{
		Version:           s.version,
		NamespaceId:       s.namespaceID,
		WorkflowId:        execution.GetWorkflowId(),
		RunId:             execution.GetRunId(),
		TargetNamespaceId: s.targetNamespaceID,
		TargetWorkflowId:  targetExecution.GetWorkflowId(),
		TargetRunId:       targetExecution.GetRunId(),
		TaskId:            taskID,
		TaskQueue:         taskQueueName,
		TaskType:          enumsspb.TASK_TYPE_TRANSFER_SIGNAL_EXECUTION,
		ScheduleId:        event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().SignalWorkflowExecution(gomock.Any(), s.createSignalWorkflowExecutionRequest(s.targetNamespace, transferTask, si)).Return(nil, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.version).Return(cluster.TestCurrentClusterName).AnyTimes()

	s.mockHistoryClient.EXPECT().RemoveSignalMutableState(gomock.Any(), &historyservice.RemoveSignalMutableStateRequest{
		NamespaceId: transferTask.GetTargetNamespaceId(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: transferTask.GetTargetWorkflowId(),
			RunId:      transferTask.GetTargetRunId(),
		},
		RequestId: si.GetRequestId(),
	}).Return(nil, nil)

	err = s.transferQueueActiveTaskExecutor.execute(context.Background(), transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessSignalExecution_Failure() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	targetExecution := commonpb.WorkflowExecution{
		WorkflowId: "some random target workflow ID",
		RunId:      uuid.New(),
	}
	signalName := "some random signal name"
	signalInput := payloads.EncodeString("some random signal input")
	signalControl := "some random signal control"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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
		},
	)
	s.Nil(err)

	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	taskID := int64(59)
	event, si := addRequestSignalInitiatedEvent(mutableState, event.GetEventId(), uuid.New(),
		tests.TargetNamespace, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), signalName, signalInput, signalControl)

	transferTask := &persistencespb.TransferTaskInfo{
		Version:           s.version,
		NamespaceId:       s.namespaceID,
		WorkflowId:        execution.GetWorkflowId(),
		RunId:             execution.GetRunId(),
		TargetNamespaceId: s.targetNamespaceID,
		TargetWorkflowId:  targetExecution.GetWorkflowId(),
		TargetRunId:       targetExecution.GetRunId(),
		TaskId:            taskID,
		TaskQueue:         taskQueueName,
		TaskType:          enumsspb.TASK_TYPE_TRANSFER_SIGNAL_EXECUTION,
		ScheduleId:        event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().SignalWorkflowExecution(gomock.Any(), s.createSignalWorkflowExecutionRequest(s.targetNamespace, transferTask, si)).Return(nil, serviceerror.NewNotFound(""))
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.version).Return(cluster.TestCurrentClusterName).AnyTimes()

	err = s.transferQueueActiveTaskExecutor.execute(context.Background(), transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessSignalExecution_Duplication() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	targetExecution := commonpb.WorkflowExecution{
		WorkflowId: "some random target workflow ID",
		RunId:      uuid.New(),
	}
	signalName := "some random signal name"
	signalInput := payloads.EncodeString("some random signal input")
	signalControl := "some random signal control"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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
		},
	)
	s.Nil(err)

	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	taskID := int64(59)
	event, _ = addRequestSignalInitiatedEvent(mutableState, event.GetEventId(), uuid.New(),
		tests.TargetNamespace, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), signalName, signalInput, signalControl)

	transferTask := &persistencespb.TransferTaskInfo{
		Version:           s.version,
		NamespaceId:       s.namespaceID,
		WorkflowId:        execution.GetWorkflowId(),
		RunId:             execution.GetRunId(),
		TargetNamespaceId: s.targetNamespaceID,
		TargetWorkflowId:  targetExecution.GetWorkflowId(),
		TargetRunId:       targetExecution.GetRunId(),
		TaskId:            taskID,
		TaskQueue:         taskQueueName,
		TaskType:          enumsspb.TASK_TYPE_TRANSFER_SIGNAL_EXECUTION,
		ScheduleId:        event.GetEventId(),
	}

	event = addSignaledEvent(mutableState, event.GetEventId(), tests.TargetNamespace, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), "")
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	err = s.transferQueueActiveTaskExecutor.execute(context.Background(), transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessStartChildExecution_Success() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	childWorkflowID := "some random child workflow ID"
	childRunID := uuid.New()
	childWorkflowType := "some random child workflow type"
	childTaskQueueName := "some random child task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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
		},
	)
	s.Nil(err)

	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	taskID := int64(59)

	event, ci := addStartChildWorkflowExecutionInitiatedEvent(mutableState, event.GetEventId(), uuid.New(),
		s.childNamespace, childWorkflowID, childWorkflowType, childTaskQueueName, nil, 1*time.Second, 1*time.Second, 1*time.Second)

	transferTask := &persistencespb.TransferTaskInfo{
		Version:           s.version,
		NamespaceId:       s.namespaceID,
		WorkflowId:        execution.GetWorkflowId(),
		RunId:             execution.GetRunId(),
		TargetNamespaceId: tests.ChildNamespaceID,
		TargetWorkflowId:  childWorkflowID,
		TargetRunId:       "",
		TaskId:            taskID,
		TaskQueue:         taskQueueName,
		TaskType:          enumsspb.TASK_TYPE_TRANSFER_START_CHILD_EXECUTION,
		ScheduleId:        event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().StartWorkflowExecution(gomock.Any(), s.createChildWorkflowExecutionRequest(
		s.namespace,
		s.childNamespace,
		transferTask,
		mutableState,
		ci,
	)).Return(&historyservice.StartWorkflowExecutionResponse{RunId: childRunID}, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.version).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockHistoryClient.EXPECT().ScheduleWorkflowTask(gomock.Any(), &historyservice.ScheduleWorkflowTaskRequest{
		NamespaceId: tests.ChildNamespaceID,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: childWorkflowID,
			RunId:      childRunID,
		},
		IsFirstWorkflowTask: true,
	}).Return(nil, nil)

	err = s.transferQueueActiveTaskExecutor.execute(context.Background(), transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessStartChildExecution_Failure() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	childWorkflowID := "some random child workflow ID"
	childWorkflowType := "some random child workflow type"
	childTaskQueueName := "some random child task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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
			ContinueAsNewInitiator: enumspb.CONTINUE_AS_NEW_INITIATOR_UNSPECIFIED,
		},
	)
	s.Nil(err)

	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	taskID := int64(59)

	event, ci := addStartChildWorkflowExecutionInitiatedEvent(
		mutableState,
		event.GetEventId(),
		uuid.New(),
		s.childNamespace,
		childWorkflowID,
		childWorkflowType,
		childTaskQueueName,
		nil,
		1*time.Second,
		1*time.Second,
		1*time.Second,
	)

	transferTask := &persistencespb.TransferTaskInfo{
		Version:           s.version,
		NamespaceId:       s.namespaceID,
		WorkflowId:        execution.GetWorkflowId(),
		RunId:             execution.GetRunId(),
		TargetNamespaceId: tests.ChildNamespaceID,
		TargetWorkflowId:  childWorkflowID,
		TargetRunId:       "",
		TaskId:            taskID,
		TaskQueue:         taskQueueName,
		TaskType:          enumsspb.TASK_TYPE_TRANSFER_START_CHILD_EXECUTION,
		ScheduleId:        event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().StartWorkflowExecution(gomock.Any(), s.createChildWorkflowExecutionRequest(
		s.namespace,
		s.childNamespace,
		transferTask,
		mutableState,
		ci,
	)).Return(nil, serviceerror.NewWorkflowExecutionAlreadyStarted("msg", "", ""))
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.version).Return(cluster.TestCurrentClusterName).AnyTimes()

	err = s.transferQueueActiveTaskExecutor.execute(context.Background(), transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessStartChildExecution_Success_Dup() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	childWorkflowID := "some random child workflow ID"
	childRunID := uuid.New()
	childWorkflowType := "some random child workflow type"
	childTaskQueueName := "some random child task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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
		},
	)
	s.Nil(err)

	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	taskID := int64(59)

	event, ci := addStartChildWorkflowExecutionInitiatedEvent(
		mutableState,
		event.GetEventId(),
		uuid.New(),
		s.childNamespace,
		childWorkflowID,
		childWorkflowType,
		childTaskQueueName,
		nil,
		1*time.Second,
		1*time.Second,
		1*time.Second,
	)

	transferTask := &persistencespb.TransferTaskInfo{
		Version:           s.version,
		NamespaceId:       s.namespaceID,
		WorkflowId:        execution.GetWorkflowId(),
		RunId:             execution.GetRunId(),
		TargetNamespaceId: tests.ChildNamespaceID,
		TargetWorkflowId:  childWorkflowID,
		TargetRunId:       "",
		TaskId:            taskID,
		TaskQueue:         taskQueueName,
		TaskType:          enumsspb.TASK_TYPE_TRANSFER_START_CHILD_EXECUTION,
		ScheduleId:        event.GetEventId(),
	}

	event = addChildWorkflowExecutionStartedEvent(mutableState, event.GetEventId(), tests.ChildNamespaceID, childWorkflowID, childRunID, childWorkflowType)
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()
	ci.StartedId = event.GetEventId()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().ScheduleWorkflowTask(gomock.Any(), &historyservice.ScheduleWorkflowTaskRequest{
		NamespaceId: tests.ChildNamespaceID,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: childWorkflowID,
			RunId:      childRunID,
		},
		IsFirstWorkflowTask: true,
	}).Return(nil, nil)

	err = s.transferQueueActiveTaskExecutor.execute(context.Background(), transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessStartChildExecution_Duplication() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	childExecution := commonpb.WorkflowExecution{
		WorkflowId: "some random child workflow ID",
		RunId:      uuid.New(),
	}
	childWorkflowType := "some random child workflow type"
	childTaskQueueName := "some random child task queue"

	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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
		},
	)
	s.Nil(err)

	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	taskID := int64(59)

	event, ci := addStartChildWorkflowExecutionInitiatedEvent(
		mutableState,
		event.GetEventId(),
		uuid.New(),
		s.childNamespace,
		childExecution.GetWorkflowId(),
		childWorkflowType,
		childTaskQueueName,
		nil,
		1*time.Second,
		1*time.Second,
		1*time.Second,
	)

	transferTask := &persistencespb.TransferTaskInfo{
		Version:           s.version,
		NamespaceId:       s.namespaceID,
		WorkflowId:        execution.GetWorkflowId(),
		RunId:             execution.GetRunId(),
		TargetNamespaceId: tests.ChildNamespaceID,
		TargetWorkflowId:  childExecution.GetWorkflowId(),
		TargetRunId:       "",
		TaskId:            taskID,
		TaskQueue:         taskQueueName,
		TaskType:          enumsspb.TASK_TYPE_TRANSFER_START_CHILD_EXECUTION,
		ScheduleId:        event.GetEventId(),
	}

	event = addChildWorkflowExecutionStartedEvent(mutableState, event.GetEventId(), tests.ChildNamespaceID, childExecution.GetWorkflowId(), childExecution.GetRunId(), childWorkflowType)
	ci.StartedId = event.GetEventId()
	event = addChildWorkflowExecutionCompletedEvent(mutableState, ci.InitiatedId, &childExecution, &historypb.WorkflowExecutionCompletedEventAttributes{
		Result:                       payloads.EncodeString("some random child workflow execution result"),
		WorkflowTaskCompletedEventId: transferTask.GetScheduleId(),
	})
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	err = s.transferQueueActiveTaskExecutor.execute(context.Background(), transferTask, true)
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestCopySearchAttributes() {
	var input map[string]*commonpb.Payload
	s.Nil(copySearchAttributes(input))

	key := "key"
	val := payload.EncodeBytes([]byte{'1', '2', '3'})
	input = map[string]*commonpb.Payload{
		key: val,
	}
	result := copySearchAttributes(input)
	s.Equal(input, result)
	result[key].GetData()[0] = '0'
	s.Equal(byte('1'), val.GetData()[0])
}

func (s *transferQueueActiveTaskExecutorSuite) createAddActivityTaskRequest(
	task *persistencespb.TransferTaskInfo,
	ai *persistencespb.ActivityInfo,
) *matchingservice.AddActivityTaskRequest {
	return &matchingservice.AddActivityTaskRequest{
		NamespaceId:       task.GetTargetNamespaceId(),
		SourceNamespaceId: task.GetNamespaceId(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: task.GetWorkflowId(),
			RunId:      task.GetRunId(),
		},
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: task.TaskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		ScheduleId:             task.GetScheduleId(),
		ScheduleToStartTimeout: ai.ScheduleToStartTimeout,
	}
}

func (s *transferQueueActiveTaskExecutorSuite) createAddWorkflowTaskRequest(
	task *persistencespb.TransferTaskInfo,
	mutableState workflow.MutableState,
) *matchingservice.AddWorkflowTaskRequest {

	execution := commonpb.WorkflowExecution{
		WorkflowId: task.GetWorkflowId(),
		RunId:      task.GetRunId(),
	}
	taskQueue := &taskqueuepb.TaskQueue{
		Name: task.TaskQueue,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}
	executionInfo := mutableState.GetExecutionInfo()
	timeout := timestamp.DurationValue(executionInfo.WorkflowRunTimeout)
	if mutableState.GetExecutionInfo().TaskQueue != task.TaskQueue {
		taskQueue.Kind = enumspb.TASK_QUEUE_KIND_STICKY
		timeout = timestamp.DurationValue(executionInfo.StickyScheduleToStartTimeout)
	}

	return &matchingservice.AddWorkflowTaskRequest{
		NamespaceId:            task.GetNamespaceId(),
		Execution:              &execution,
		TaskQueue:              taskQueue,
		ScheduleId:             task.GetScheduleId(),
		ScheduleToStartTimeout: &timeout,
	}
}

func (s *transferQueueActiveTaskExecutorSuite) createRecordWorkflowExecutionStartedRequest(
	namespace string,
	startEvent *historypb.HistoryEvent,
	task *persistencespb.TransferTaskInfo,
	mutableState workflow.MutableState,
	backoffSeconds time.Duration,
) *visibility.RecordWorkflowExecutionStartedRequest {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: task.GetWorkflowId(),
		RunId:      task.GetRunId(),
	}
	executionInfo := mutableState.GetExecutionInfo()
	executionTimestamp := timestamp.TimeValue(startEvent.GetEventTime()).Add(backoffSeconds)

	return &visibility.RecordWorkflowExecutionStartedRequest{
		VisibilityRequestBase: &visibility.VisibilityRequestBase{
			Namespace:            namespace,
			NamespaceID:          task.GetNamespaceId(),
			Execution:            *execution,
			WorkflowTypeName:     executionInfo.WorkflowTypeName,
			StartTime:            timestamp.TimeValue(startEvent.GetEventTime()),
			ExecutionTime:        executionTimestamp,
			StateTransitionCount: executionInfo.StateTransitionCount,
			TaskID:               task.GetTaskId(),
			TaskQueue:            task.TaskQueue,
		},
	}
}

func (s *transferQueueActiveTaskExecutorSuite) createRequestCancelWorkflowExecutionRequest(
	targetNamespace string,
	task *persistencespb.TransferTaskInfo,
	rci *persistencespb.RequestCancelInfo,
) *historyservice.RequestCancelWorkflowExecutionRequest {

	sourceExecution := commonpb.WorkflowExecution{
		WorkflowId: task.GetWorkflowId(),
		RunId:      task.GetRunId(),
	}
	targetExecution := commonpb.WorkflowExecution{
		WorkflowId: task.GetTargetWorkflowId(),
		RunId:      task.GetTargetRunId(),
	}

	return &historyservice.RequestCancelWorkflowExecutionRequest{
		NamespaceId: task.GetTargetNamespaceId(),
		CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{
			Namespace:         targetNamespace,
			WorkflowExecution: &targetExecution,
			Identity:          consts.IdentityHistoryService,
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
	task *persistencespb.TransferTaskInfo,
	si *persistencespb.SignalInfo,
) *historyservice.SignalWorkflowExecutionRequest {

	sourceExecution := commonpb.WorkflowExecution{
		WorkflowId: task.GetWorkflowId(),
		RunId:      task.GetRunId(),
	}
	targetExecution := commonpb.WorkflowExecution{
		WorkflowId: task.GetTargetWorkflowId(),
		RunId:      task.GetTargetRunId(),
	}

	return &historyservice.SignalWorkflowExecutionRequest{
		NamespaceId: task.GetTargetNamespaceId(),
		SignalRequest: &workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         targetNamespace,
			WorkflowExecution: &targetExecution,
			Identity:          consts.IdentityHistoryService,
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
	task *persistencespb.TransferTaskInfo,
	mutableState workflow.MutableState,
	ci *persistencespb.ChildExecutionInfo,
) *historyservice.StartWorkflowExecutionRequest {

	event, err := mutableState.GetChildExecutionInitiatedEvent(task.GetScheduleId())
	s.NoError(err)
	attributes := event.GetStartChildWorkflowExecutionInitiatedEventAttributes()
	execution := commonpb.WorkflowExecution{
		WorkflowId: task.GetWorkflowId(),
		RunId:      task.GetRunId(),
	}
	now := s.timeSource.Now().UTC()
	return &historyservice.StartWorkflowExecutionRequest{
		Attempt:     1,
		NamespaceId: task.GetTargetNamespaceId(),
		StartRequest: &workflowservice.StartWorkflowExecutionRequest{
			Namespace:                childNamespace,
			WorkflowId:               attributes.WorkflowId,
			WorkflowType:             attributes.WorkflowType,
			TaskQueue:                attributes.TaskQueue,
			Input:                    attributes.Input,
			WorkflowExecutionTimeout: attributes.WorkflowExecutionTimeout,
			WorkflowRunTimeout:       attributes.WorkflowRunTimeout,
			WorkflowTaskTimeout:      attributes.WorkflowTaskTimeout,
			// Use the same request ID to dedupe StartWorkflowExecution calls
			RequestId:             ci.CreateRequestId,
			WorkflowIdReusePolicy: attributes.WorkflowIdReusePolicy,
		},
		ParentExecutionInfo: &workflowspb.ParentExecutionInfo{
			NamespaceId: task.GetNamespaceId(),
			Namespace:   tests.Namespace,
			Execution:   &execution,
			InitiatedId: task.GetScheduleId(),
		},
		FirstWorkflowTaskBackoff:        backoff.GetBackoffForNextScheduleNonNegative(attributes.GetCronSchedule(), now, now),
		ContinueAsNewInitiator:          enumspb.CONTINUE_AS_NEW_INITIATOR_UNSPECIFIED,
		WorkflowExecutionExpirationTime: timestamp.TimePtr(now.Add(*attributes.WorkflowExecutionTimeout).Round(time.Millisecond)),
	}
}

func (s *transferQueueActiveTaskExecutorSuite) createUpsertWorkflowSearchAttributesRequest(
	namespace string,
	startEvent *historypb.HistoryEvent,
	task *persistencespb.TransferTaskInfo,
	mutableState workflow.MutableState,
) *visibility.UpsertWorkflowExecutionRequest {

	execution := &commonpb.WorkflowExecution{
		WorkflowId: task.GetWorkflowId(),
		RunId:      task.GetRunId(),
	}
	executionInfo := mutableState.GetExecutionInfo()

	return &visibility.UpsertWorkflowExecutionRequest{
		VisibilityRequestBase: &visibility.VisibilityRequestBase{
			Namespace:        namespace,
			NamespaceID:      task.GetNamespaceId(),
			Execution:        *execution,
			WorkflowTypeName: executionInfo.WorkflowTypeName,
			StartTime:        timestamp.TimeValue(startEvent.GetEventTime()),
			TaskID:           task.GetTaskId(),
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			TaskQueue:        task.TaskQueue,
		},
	}
}

func (s *transferQueueActiveTaskExecutorSuite) createPersistenceMutableState(
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
