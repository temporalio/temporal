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
	"math/rand"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

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
	"go.temporal.io/server/common/definition"
	dc "go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/vclock"
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
		mockTxProcessor              *queues.MockQueue
		mockTimerProcessor           *queues.MockQueue
		mockNamespaceCache           *namespace.MockRegistry
		mockMatchingClient           *matchingservicemock.MockMatchingServiceClient
		mockHistoryClient            *historyservicemock.MockHistoryServiceClient
		mockClusterMetadata          *cluster.MockMetadata
		mockSearchAttributesProvider *searchattribute.MockProvider

		mockExecutionMgr            *persistence.MockExecutionManager
		mockArchivalClient          *warchiver.MockClient
		mockArchivalMetadata        *archiver.MockArchivalMetadata
		mockArchiverProvider        *provider.MockArchiverProvider
		mockParentClosePolicyClient *parentclosepolicy.MockClient

		workflowCache                   workflow.Cache
		logger                          log.Logger
		namespaceID                     namespace.ID
		namespace                       namespace.Name
		namespaceEntry                  *namespace.Namespace
		targetNamespaceID               namespace.ID
		targetNamespace                 namespace.Name
		targetNamespaceEntry            *namespace.Namespace
		childNamespaceID                namespace.ID
		childNamespace                  namespace.Name
		childNamespaceEntry             *namespace.Namespace
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
	s.version = s.namespaceEntry.FailoverVersion()
	s.now = time.Now().UTC()
	s.timeSource = clock.NewEventTimeSource().Update(s.now)

	s.controller = gomock.NewController(s.T())
	s.mockTxProcessor = queues.NewMockQueue(s.controller)
	s.mockTimerProcessor = queues.NewMockQueue(s.controller)
	s.mockTxProcessor.EXPECT().Category().Return(tasks.CategoryTransfer).AnyTimes()
	s.mockTimerProcessor.EXPECT().Category().Return(tasks.CategoryTimer).AnyTimes()
	s.mockTxProcessor.EXPECT().NotifyNewTasks(gomock.Any(), gomock.Any()).AnyTimes()
	s.mockTimerProcessor.EXPECT().NotifyNewTasks(gomock.Any(), gomock.Any()).AnyTimes()

	config := tests.NewDynamicConfig()
	s.mockShard = shard.NewTestContextWithTimeSource(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId: 1,
				RangeId: 1,
			},
		},
		config,
		s.timeSource,
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
	s.mockNamespaceCache.EXPECT().GetNamespaceName(tests.NamespaceID).Return(tests.Namespace, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.TargetNamespaceID).Return(tests.GlobalTargetNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(tests.TargetNamespace).Return(tests.GlobalTargetNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.ParentNamespaceID).Return(tests.GlobalParentNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(tests.ParentNamespace).Return(tests.GlobalParentNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.ChildNamespaceID).Return(tests.GlobalChildNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(tests.ChildNamespace).Return(tests.GlobalChildNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.MissedNamespaceID).Return(nil, serviceerror.NewNamespaceNotFound(tests.MissedNamespaceID.String())).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetClusterID().Return(cluster.TestCurrentClusterInitialFailoverVersion).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.IsGlobalNamespace(), s.version).Return(s.mockClusterMetadata.GetCurrentClusterName()).AnyTimes()

	s.workflowCache = workflow.NewCache(s.mockShard)
	s.logger = s.mockShard.GetLogger()

	h := &historyEngineImpl{
		currentClusterName: s.mockShard.Resource.GetClusterMetadata().GetCurrentClusterName(),
		shard:              s.mockShard,
		clusterMetadata:    s.mockClusterMetadata,
		executionManager:   s.mockExecutionMgr,
		logger:             s.logger,
		tokenSerializer:    common.NewProtoTaskTokenSerializer(),
		metricsClient:      s.mockShard.GetMetricsClient(),
		eventNotifier:      events.NewNotifier(clock.NewRealTimeSource(), metrics.NoopClient, func(namespace.ID, string) int32 { return 1 }),
		queueProcessors: map[tasks.Category]queues.Queue{
			s.mockTxProcessor.Category():    s.mockTxProcessor,
			s.mockTimerProcessor.Category(): s.mockTimerProcessor,
		},
	}
	s.mockShard.SetEngineForTesting(h)

	s.transferQueueActiveTaskExecutor = newTransferQueueActiveTaskExecutor(
		s.mockShard,
		s.workflowCache,
		s.mockArchivalClient,
		h.sdkClientFactory,
		s.logger,
		metrics.NoopMetricsHandler,
		config,
		s.mockShard.Resource.MatchingClient,
	).(*transferQueueActiveTaskExecutor)
	s.transferQueueActiveTaskExecutor.parentClosePolicyClient = s.mockParentClosePolicyClient
}

func (s *transferQueueActiveTaskExecutorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
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
			NamespaceId: s.namespaceID.String(),
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

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := int64(59)
	activityID := "activity-1"
	activityType := "some random activity type"
	event, ai := addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID, activityType, taskQueueName, &commonpb.Payloads{}, 1*time.Second, 1*time.Second, 1*time.Second, 1*time.Second)

	transferTask := &tasks.ActivityTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		TaskID:              taskID,
		TaskQueue:           taskQueueName,
		ScheduledEventID:    event.GetEventId(),
		VisibilityTimestamp: time.Now().UTC(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.EXPECT().AddActivityTask(gomock.Any(), s.createAddActivityTaskRequest(transferTask, ai), gomock.Any()).Return(&matchingservice.AddActivityTaskResponse{}, nil)

	_, err = s.transferQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
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
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: timestamp.DurationPtr(2 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := int64(59)
	activityID := "activity-1"
	activityType := "some random activity type"
	event, ai := addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID, activityType, taskQueueName, &commonpb.Payloads{}, 1*time.Second, 1*time.Second, 1*time.Second, 1*time.Second)

	transferTask := &tasks.ActivityTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		TaskID:              taskID,
		TaskQueue:           taskQueueName,
		ScheduledEventID:    event.GetEventId(),
		VisibilityTimestamp: time.Now().UTC(),
	}

	event = addActivityTaskStartedEvent(mutableState, event.GetEventId(), "")
	ai.StartedEventId = event.GetEventId()
	event = addActivityTaskCompletedEvent(mutableState, ai.ScheduledEventId, ai.StartedEventId, nil, "")
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	_, err = s.transferQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
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
			NamespaceId: s.namespaceID.String(),
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
	wt := addWorkflowTaskScheduledEvent(mutableState)

	transferTask := &tasks.WorkflowTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		TaskID:              taskID,
		TaskQueue:           taskQueueName,
		ScheduledEventID:    wt.ScheduledEventID,
		VisibilityTimestamp: time.Now().UTC(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, wt.ScheduledEventID, wt.Version)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.EXPECT().AddWorkflowTask(gomock.Any(), s.createAddWorkflowTaskRequest(transferTask, mutableState), gomock.Any()).Return(&matchingservice.AddWorkflowTaskResponse{}, nil)

	_, err = s.transferQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
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
			NamespaceId: s.namespaceID.String(),
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

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")
	s.NotNil(event)

	// make another round of workflow task
	taskID := int64(59)
	wt = addWorkflowTaskScheduledEvent(mutableState)

	transferTask := &tasks.WorkflowTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		TaskID:              taskID,
		TaskQueue:           taskQueueName,
		ScheduledEventID:    wt.ScheduledEventID,
		VisibilityTimestamp: time.Now().UTC(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, wt.ScheduledEventID, wt.Version)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.EXPECT().AddWorkflowTask(gomock.Any(), s.createAddWorkflowTaskRequest(transferTask, mutableState), gomock.Any()).Return(&matchingservice.AddWorkflowTaskResponse{}, nil)

	_, err = s.transferQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
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
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: timestamp.DurationPtr(2 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")
	s.NotNil(event)
	// set the sticky taskqueue attr
	executionInfo := mutableState.GetExecutionInfo()
	executionInfo.StickyTaskQueue = stickyTaskQueueName
	executionInfo.StickyScheduleToStartTimeout = stickyTaskQueueTimeout

	// make another round of workflow task
	taskID := int64(59)
	wt = addWorkflowTaskScheduledEvent(mutableState)

	transferTask := &tasks.WorkflowTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		TaskID:              taskID,
		TaskQueue:           stickyTaskQueueName,
		ScheduledEventID:    wt.ScheduledEventID,
		VisibilityTimestamp: time.Now().UTC(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, wt.ScheduledEventID, wt.Version)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.EXPECT().AddWorkflowTask(gomock.Any(), s.createAddWorkflowTaskRequest(transferTask, mutableState), gomock.Any()).Return(&matchingservice.AddWorkflowTaskResponse{}, nil)

	_, err = s.transferQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
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
			NamespaceId: s.namespaceID.String(),
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

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")
	s.NotNil(event)
	// set the sticky taskqueue attr
	executionInfo := mutableState.GetExecutionInfo()
	executionInfo.StickyTaskQueue = stickyTaskQueueName
	executionInfo.StickyScheduleToStartTimeout = stickyTaskQueueTimeout

	// make another round of workflow task
	taskID := int64(59)
	wt = addWorkflowTaskScheduledEvent(mutableState)

	transferTask := &tasks.WorkflowTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		TaskID:              taskID,
		TaskQueue:           taskQueueName,
		ScheduledEventID:    wt.ScheduledEventID,
		VisibilityTimestamp: time.Now().UTC(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, wt.ScheduledEventID, wt.Version)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.EXPECT().AddWorkflowTask(gomock.Any(), s.createAddWorkflowTaskRequest(transferTask, mutableState), gomock.Any()).Return(&matchingservice.AddWorkflowTaskResponse{}, nil)

	_, err = s.transferQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
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
			NamespaceId: s.namespaceID.String(),
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
	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	transferTask := &tasks.WorkflowTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		TaskID:              taskID,
		TaskQueue:           taskQueueName,
		ScheduledEventID:    wt.ScheduledEventID,
		VisibilityTimestamp: time.Now().UTC(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	_, err = s.transferQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
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
	parentInitiatedVersion := int64(1234)
	parentNamespace := "some random parent namespace Name"
	parentExecution := &commonpb.WorkflowExecution{
		WorkflowId: "some random parent workflow ID",
		RunId:      uuid.New(),
	}
	parentClock := vclock.NewVectorClock(rand.Int63(), rand.Int31(), rand.Int63())

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
				NamespaceId:      parentNamespaceID,
				Namespace:        parentNamespace,
				Execution:        parentExecution,
				InitiatedId:      parentInitiatedID,
				InitiatedVersion: parentInitiatedVersion,
				Clock:            parentClock,
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := int64(59)
	event = addCompleteWorkflowEvent(mutableState, event.GetEventId(), nil)

	transferTask := &tasks.CloseExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		TaskID:              taskID,
		VisibilityTimestamp: time.Now().UTC(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().RecordChildExecutionCompleted(gomock.Any(), &historyservice.RecordChildExecutionCompletedRequest{
		NamespaceId:            parentNamespaceID,
		WorkflowExecution:      parentExecution,
		ParentInitiatedId:      parentInitiatedID,
		ParentInitiatedVersion: parentInitiatedVersion,
		Clock:                  parentClock,
		CompletedExecution:     &execution,
		CompletionEvent:        event,
	}).Return(nil, nil)
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewDisabledArchvialConfig())

	_, err = s.transferQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
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
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: timestamp.DurationPtr(2 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := int64(59)
	event = addCompleteWorkflowEvent(mutableState, event.GetEventId(), nil)

	transferTask := &tasks.CloseExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		TaskID:              taskID,
		VisibilityTimestamp: time.Now().UTC(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "random URI"))
	s.mockArchivalClient.EXPECT().Archive(gomock.Any(), gomock.Any()).Return(nil, nil)
	s.mockSearchAttributesProvider.EXPECT().GetSearchAttributes(gomock.Any(), false)

	_, err = s.transferQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessCloseExecution_NoParent_HasFewChildren() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	s.mockNamespaceCache.EXPECT().GetNamespace(namespace.Name("child namespace1")).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(namespace.Name("child namespace2")).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(namespace.Name("child namespace3")).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()

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
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()

	commandType := enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION
	parentClosePolicy1 := enumspb.PARENT_CLOSE_POLICY_ABANDON
	parentClosePolicy2 := enumspb.PARENT_CLOSE_POLICY_TERMINATE
	parentClosePolicy3 := enumspb.PARENT_CLOSE_POLICY_REQUEST_CANCEL

	event, _ = mutableState.AddWorkflowTaskCompletedEvent(wt.ScheduledEventID, wt.StartedEventID, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity: "some random identity",
		Commands: []*commandpb.Command{
			{
				CommandType: commandType,
				Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
					Namespace:  "child namespace1",
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
				CommandType: commandType,
				Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
					Namespace:  "child namespace2",
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
				CommandType: commandType,
				Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
					Namespace:  "child namespace3",
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
		Namespace:  "child namespace1",
		WorkflowId: "child workflow1",
		WorkflowType: &commonpb.WorkflowType{
			Name: "child workflow type",
		},
		TaskQueue:         &taskqueuepb.TaskQueue{Name: taskQueueName},
		Input:             payloads.EncodeString("random input"),
		ParentClosePolicy: parentClosePolicy1,
	}, "child namespace1-ID")
	s.Nil(err)
	_, _, err = mutableState.AddStartChildWorkflowExecutionInitiatedEvent(event.GetEventId(), uuid.New(), &commandpb.StartChildWorkflowExecutionCommandAttributes{
		Namespace:  "child namespace2",
		WorkflowId: "child workflow2",
		WorkflowType: &commonpb.WorkflowType{
			Name: "child workflow type",
		},
		TaskQueue:         &taskqueuepb.TaskQueue{Name: taskQueueName},
		Input:             payloads.EncodeString("random input"),
		ParentClosePolicy: parentClosePolicy2,
	}, "child namespace2-ID")
	s.Nil(err)
	_, _, err = mutableState.AddStartChildWorkflowExecutionInitiatedEvent(event.GetEventId(), uuid.New(), &commandpb.StartChildWorkflowExecutionCommandAttributes{
		Namespace:  "child namespace3",
		WorkflowId: "child workflow3",
		WorkflowType: &commonpb.WorkflowType{
			Name: "child workflow type",
		},
		TaskQueue:         &taskqueuepb.TaskQueue{Name: taskQueueName},
		Input:             payloads.EncodeString("random input"),
		ParentClosePolicy: parentClosePolicy3,
	}, "child namespace3-ID")
	s.Nil(err)

	mutableState.FlushBufferedEvents()

	taskID := int64(59)
	event = addCompleteWorkflowEvent(mutableState, event.GetEventId(), nil)

	transferTask := &tasks.CloseExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		TaskID:              taskID,
		VisibilityTimestamp: time.Now().UTC(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewDisabledArchvialConfig())
	s.mockHistoryClient.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *historyservice.RequestCancelWorkflowExecutionRequest, _ ...grpc.CallOption) (*historyservice.RequestCancelWorkflowExecutionResponse, error) {
			s.True(request.GetChildWorkflowOnly())
			s.Equal(execution.GetWorkflowId(), request.GetExternalWorkflowExecution().GetWorkflowId())
			s.Equal(execution.GetRunId(), request.GetExternalWorkflowExecution().GetRunId())
			return nil, nil
		},
	)
	s.mockHistoryClient.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *historyservice.TerminateWorkflowExecutionRequest, _ ...grpc.CallOption) (*historyservice.TerminateWorkflowExecutionResponse, error) {
			s.True(request.GetChildWorkflowOnly())
			s.Equal(execution.GetWorkflowId(), request.GetExternalWorkflowExecution().GetWorkflowId())
			s.Equal(execution.GetRunId(), request.GetExternalWorkflowExecution().GetRunId())
			return nil, nil
		},
	)

	_, err = s.transferQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
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
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: timestamp.DurationPtr(2 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()

	commandType := enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION
	parentClosePolicy := enumspb.PARENT_CLOSE_POLICY_TERMINATE
	var commands []*commandpb.Command
	for i := 0; i < 10; i++ {
		commands = append(commands, &commandpb.Command{
			CommandType: commandType,
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

	event, _ = mutableState.AddWorkflowTaskCompletedEvent(wt.ScheduledEventID, wt.StartedEventID, &workflowservice.RespondWorkflowTaskCompletedRequest{
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
		}, "child namespace1-ID")
		s.Nil(err)
	}

	mutableState.FlushBufferedEvents()

	taskID := int64(59)
	event = addCompleteWorkflowEvent(mutableState, event.GetEventId(), nil)

	transferTask := &tasks.CloseExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		TaskID:              taskID,
		VisibilityTimestamp: time.Now().UTC(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewDisabledArchvialConfig())
	s.mockParentClosePolicyClient.EXPECT().SendParentClosePolicyRequest(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request parentclosepolicy.Request) error {
			s.Equal(execution, request.ParentExecution)
			return nil
		},
	)

	_, err = s.transferQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
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
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: timestamp.DurationPtr(2 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()

	commandType := enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION
	parentClosePolicy := enumspb.PARENT_CLOSE_POLICY_ABANDON
	var commands []*commandpb.Command
	for i := 0; i < 10; i++ {
		commands = append(commands, &commandpb.Command{
			CommandType: commandType,
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

	event, _ = mutableState.AddWorkflowTaskCompletedEvent(wt.ScheduledEventID, wt.StartedEventID, &workflowservice.RespondWorkflowTaskCompletedRequest{
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
		}, "child namespace1-ID")
		s.Nil(err)
	}

	mutableState.FlushBufferedEvents()

	taskID := int64(59)
	event = addCompleteWorkflowEvent(mutableState, event.GetEventId(), nil)

	transferTask := &tasks.CloseExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		TaskID:              taskID,
		VisibilityTimestamp: time.Now().UTC(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewDisabledArchvialConfig())

	_, err = s.transferQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessCloseExecution_NoParent_ChildInDeletedNamespace() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	s.mockNamespaceCache.EXPECT().GetNamespace(namespace.Name("child namespace1")).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()

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

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()

	event, _ = mutableState.AddWorkflowTaskCompletedEvent(wt.ScheduledEventID, wt.StartedEventID, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity: "some random identity",
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
					Namespace:  "child namespace1",
					WorkflowId: "child workflow1",
					WorkflowType: &commonpb.WorkflowType{
						Name: "child workflow type",
					},
					TaskQueue:         &taskqueuepb.TaskQueue{Name: taskQueueName},
					Input:             payloads.EncodeString("random input"),
					ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_TERMINATE,
				}},
			},
			{
				CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
					Namespace:  "child namespace1",
					WorkflowId: "child workflow2",
					WorkflowType: &commonpb.WorkflowType{
						Name: "child workflow type",
					},
					TaskQueue:         &taskqueuepb.TaskQueue{Name: taskQueueName},
					Input:             payloads.EncodeString("random input"),
					ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
				}},
			},
		},
	}, configs.DefaultHistoryMaxAutoResetPoints)

	_, _, err = mutableState.AddStartChildWorkflowExecutionInitiatedEvent(event.GetEventId(), uuid.New(), &commandpb.StartChildWorkflowExecutionCommandAttributes{
		Namespace:  "child namespace1",
		WorkflowId: "child workflow1",
		WorkflowType: &commonpb.WorkflowType{
			Name: "child workflow type",
		},
		TaskQueue:         &taskqueuepb.TaskQueue{Name: taskQueueName},
		Input:             payloads.EncodeString("random input"),
		ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_TERMINATE,
	}, "child namespace1-ID")
	s.NoError(err)

	_, _, err = mutableState.AddStartChildWorkflowExecutionInitiatedEvent(event.GetEventId(), uuid.New(), &commandpb.StartChildWorkflowExecutionCommandAttributes{
		Namespace:  "child namespace1",
		WorkflowId: "child workflow2",
		WorkflowType: &commonpb.WorkflowType{
			Name: "child workflow type",
		},
		TaskQueue:         &taskqueuepb.TaskQueue{Name: taskQueueName},
		Input:             payloads.EncodeString("random input"),
		ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
	}, "child namespace2-ID")
	s.NoError(err)

	mutableState.FlushBufferedEvents()

	taskID := int64(22)
	event = addCompleteWorkflowEvent(mutableState, event.GetEventId(), nil)

	transferTask := &tasks.CloseExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		TaskID:              taskID,
		VisibilityTimestamp: time.Now().UTC(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewDisabledArchvialConfig())

	s.mockHistoryClient.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *historyservice.TerminateWorkflowExecutionRequest, _ ...grpc.CallOption) (*historyservice.TerminateWorkflowExecutionResponse, error) {
			s.True(request.GetChildWorkflowOnly())
			s.Equal(execution.GetWorkflowId(), request.GetExternalWorkflowExecution().GetWorkflowId())
			s.Equal(execution.GetRunId(), request.GetExternalWorkflowExecution().GetRunId())
			return nil, serviceerror.NewNamespaceNotFound("child namespace1")
		},
	)

	s.mockHistoryClient.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *historyservice.RequestCancelWorkflowExecutionRequest, _ ...grpc.CallOption) (*historyservice.RequestCancelWorkflowExecutionResponse, error) {
			s.True(request.GetChildWorkflowOnly())
			s.Equal(execution.GetWorkflowId(), request.GetExternalWorkflowExecution().GetWorkflowId())
			s.Equal(execution.GetRunId(), request.GetExternalWorkflowExecution().GetRunId())
			return nil, serviceerror.NewNamespaceNotFound("child namespace1")
		},
	)

	_, err = s.transferQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.NoError(err)
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
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: timestamp.DurationPtr(2 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := int64(59)
	event, rci := addRequestCancelInitiatedEvent(mutableState, event.GetEventId(), uuid.New(), tests.TargetNamespace, tests.TargetNamespaceID, targetExecution.GetWorkflowId(), targetExecution.GetRunId())
	attributes := event.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes()

	transferTask := &tasks.CancelExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:                 s.version,
		TargetNamespaceID:       s.targetNamespaceID.String(),
		TargetWorkflowID:        targetExecution.GetWorkflowId(),
		TargetRunID:             targetExecution.GetRunId(),
		TaskID:                  taskID,
		TargetChildWorkflowOnly: true,
		InitiatedEventID:        event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), s.createRequestCancelWorkflowExecutionRequest(s.targetNamespace, transferTask, rci, attributes)).Return(nil, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.IsGlobalNamespace(), s.version).Return(cluster.TestCurrentClusterName).AnyTimes()

	_, err = s.transferQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
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
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: timestamp.DurationPtr(2 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := int64(59)
	event, rci := addRequestCancelInitiatedEvent(mutableState, event.GetEventId(), uuid.New(), tests.TargetNamespace, tests.TargetNamespaceID, targetExecution.GetWorkflowId(), targetExecution.GetRunId())
	attributes := event.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes()

	transferTask := &tasks.CancelExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:                 s.version,
		TargetNamespaceID:       s.targetNamespaceID.String(),
		TargetWorkflowID:        targetExecution.GetWorkflowId(),
		TargetRunID:             targetExecution.GetRunId(),
		TaskID:                  taskID,
		TargetChildWorkflowOnly: true,
		InitiatedEventID:        event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), s.createRequestCancelWorkflowExecutionRequest(s.targetNamespace, transferTask, rci, attributes)).Return(nil, serviceerror.NewNotFound(""))
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(gomock.Any(), s.version).Return(cluster.TestCurrentClusterName).AnyTimes()

	_, err = s.transferQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessCancelExecution_Failure_TargetNamespaceNotFound() {
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
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: timestamp.DurationPtr(2 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := int64(59)
	event, _ = addRequestCancelInitiatedEvent(mutableState, event.GetEventId(), uuid.New(), tests.TargetNamespace, tests.TargetNamespaceID, targetExecution.GetWorkflowId(), targetExecution.GetRunId())

	transferTask := &tasks.CancelExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:                 s.version,
		TargetNamespaceID:       tests.MissedNamespaceID.String(),
		TargetWorkflowID:        targetExecution.GetWorkflowId(),
		TargetRunID:             targetExecution.GetRunId(),
		TaskID:                  taskID,
		TargetChildWorkflowOnly: true,
		InitiatedEventID:        event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(gomock.Any(), s.version).Return(cluster.TestCurrentClusterName).AnyTimes()

	_, err = s.transferQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
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
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: timestamp.DurationPtr(2 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := int64(59)
	event, _ = addRequestCancelInitiatedEvent(mutableState, event.GetEventId(), uuid.New(), tests.TargetNamespace, tests.TargetNamespaceID, targetExecution.GetWorkflowId(), targetExecution.GetRunId())

	transferTask := &tasks.CancelExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:                 s.version,
		TargetNamespaceID:       s.targetNamespaceID.String(),
		TargetWorkflowID:        targetExecution.GetWorkflowId(),
		TargetRunID:             targetExecution.GetRunId(),
		TaskID:                  taskID,
		TargetChildWorkflowOnly: true,
		InitiatedEventID:        event.GetEventId(),
	}

	event = addCancelRequestedEvent(mutableState, event.GetEventId(), tests.TargetNamespace, tests.TargetNamespaceID, targetExecution.GetWorkflowId(), targetExecution.GetRunId())
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	_, err = s.transferQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
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
	signalHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{"signal header key": payload.EncodeString("signal header value")},
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
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := int64(59)
	event, si := addRequestSignalInitiatedEvent(mutableState, event.GetEventId(), uuid.New(),
		tests.TargetNamespace, tests.TargetNamespaceID, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), signalName, signalInput,
		signalControl, signalHeader)
	attributes := event.GetSignalExternalWorkflowExecutionInitiatedEventAttributes()

	transferTask := &tasks.SignalExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:                 s.version,
		TargetNamespaceID:       s.targetNamespaceID.String(),
		TargetWorkflowID:        targetExecution.GetWorkflowId(),
		TargetRunID:             targetExecution.GetRunId(),
		TaskID:                  taskID,
		TargetChildWorkflowOnly: true,
		InitiatedEventID:        event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().SignalWorkflowExecution(gomock.Any(), s.createSignalWorkflowExecutionRequest(s.targetNamespace, transferTask, si, attributes)).Return(nil, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.IsGlobalNamespace(), s.version).Return(cluster.TestCurrentClusterName).AnyTimes()

	s.mockHistoryClient.EXPECT().RemoveSignalMutableState(gomock.Any(), &historyservice.RemoveSignalMutableStateRequest{
		NamespaceId: transferTask.TargetNamespaceID,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: transferTask.TargetWorkflowID,
			RunId:      transferTask.TargetRunID,
		},
		RequestId: si.GetRequestId(),
	}).Return(nil, nil)

	_, err = s.transferQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
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
	signalHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{"signal header key": payload.EncodeString("signal header value")},
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
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := int64(59)
	event, si := addRequestSignalInitiatedEvent(mutableState, event.GetEventId(), uuid.New(),
		tests.TargetNamespace, tests.TargetNamespaceID, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), signalName, signalInput,
		signalControl, signalHeader)
	attributes := event.GetSignalExternalWorkflowExecutionInitiatedEventAttributes()

	transferTask := &tasks.SignalExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:                 s.version,
		TargetNamespaceID:       s.targetNamespaceID.String(),
		TargetWorkflowID:        targetExecution.GetWorkflowId(),
		TargetRunID:             targetExecution.GetRunId(),
		TaskID:                  taskID,
		TargetChildWorkflowOnly: true,
		InitiatedEventID:        event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().SignalWorkflowExecution(gomock.Any(), s.createSignalWorkflowExecutionRequest(s.targetNamespace, transferTask, si, attributes)).Return(nil, serviceerror.NewNotFound(""))
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.IsGlobalNamespace(), s.version).Return(cluster.TestCurrentClusterName).AnyTimes()

	_, err = s.transferQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessSignalExecution_Failure_TargetNamespaceNotFound() {
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
	signalHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{"signal header key": payload.EncodeString("signal header value")},
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
		},
	)
	s.NoError(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := int64(59)
	event, _ = addRequestSignalInitiatedEvent(mutableState, event.GetEventId(), uuid.New(),
		tests.TargetNamespace, tests.TargetNamespaceID, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), signalName, signalInput,
		signalControl, signalHeader)

	transferTask := &tasks.SignalExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:                 s.version,
		TargetNamespaceID:       tests.MissedNamespaceID.String(),
		TargetWorkflowID:        targetExecution.GetWorkflowId(),
		TargetRunID:             targetExecution.GetRunId(),
		TaskID:                  taskID,
		TargetChildWorkflowOnly: true,
		InitiatedEventID:        event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.IsGlobalNamespace(), s.version).Return(cluster.TestCurrentClusterName).AnyTimes()

	_, err = s.transferQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
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
	signalHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{"signal header key": payload.EncodeString("signal header value")},
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
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := int64(59)
	event, _ = addRequestSignalInitiatedEvent(mutableState, event.GetEventId(), uuid.New(),
		tests.TargetNamespace, tests.TargetNamespaceID, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), signalName, signalInput,
		signalControl, signalHeader)

	transferTask := &tasks.SignalExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:                 s.version,
		TargetNamespaceID:       s.targetNamespaceID.String(),
		TargetWorkflowID:        targetExecution.GetWorkflowId(),
		TargetRunID:             targetExecution.GetRunId(),
		TaskID:                  taskID,
		TargetChildWorkflowOnly: true,
		InitiatedEventID:        event.GetEventId(),
	}

	event = addSignaledEvent(mutableState, event.GetEventId(), tests.TargetNamespace, tests.TargetNamespaceID, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), "")
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	_, err = s.transferQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
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
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: timestamp.DurationPtr(2 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := int64(59)

	event, ci := addStartChildWorkflowExecutionInitiatedEvent(
		mutableState,
		event.GetEventId(),
		uuid.New(),
		s.childNamespace,
		s.childNamespaceID,
		childWorkflowID,
		childWorkflowType,
		childTaskQueueName,
		nil,
		1*time.Second,
		1*time.Second,
		1*time.Second,
		enumspb.PARENT_CLOSE_POLICY_TERMINATE,
	)

	transferTask := &tasks.StartChildExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		TargetNamespaceID:   tests.ChildNamespaceID.String(),
		TargetWorkflowID:    childWorkflowID,
		TaskID:              taskID,
		InitiatedEventID:    event.GetEventId(),
		VisibilityTimestamp: time.Now().UTC(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().StartWorkflowExecution(gomock.Any(), s.createChildWorkflowExecutionRequest(
		s.namespace,
		s.childNamespace,
		transferTask,
		mutableState,
		ci,
	)).Return(&historyservice.StartWorkflowExecutionResponse{RunId: childRunID}, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.IsGlobalNamespace(), s.version).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockHistoryClient.EXPECT().ScheduleWorkflowTask(gomock.Any(), &historyservice.ScheduleWorkflowTaskRequest{
		NamespaceId: tests.ChildNamespaceID.String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: childWorkflowID,
			RunId:      childRunID,
		},
		IsFirstWorkflowTask: true,
	}).Return(nil, nil)

	_, err = s.transferQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
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
			NamespaceId: s.namespaceID.String(),
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

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := int64(59)

	event, ci := addStartChildWorkflowExecutionInitiatedEvent(
		mutableState,
		event.GetEventId(),
		uuid.New(),
		s.childNamespace,
		s.childNamespaceID,
		childWorkflowID,
		childWorkflowType,
		childTaskQueueName,
		nil,
		1*time.Second,
		1*time.Second,
		1*time.Second,
		enumspb.PARENT_CLOSE_POLICY_TERMINATE,
	)

	transferTask := &tasks.StartChildExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		TargetNamespaceID:   tests.ChildNamespaceID.String(),
		TargetWorkflowID:    childWorkflowID,
		TaskID:              taskID,
		InitiatedEventID:    event.GetEventId(),
		VisibilityTimestamp: time.Now().UTC(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().StartWorkflowExecution(gomock.Any(), s.createChildWorkflowExecutionRequest(
		s.namespace,
		s.childNamespace,
		transferTask,
		mutableState,
		ci,
	)).Return(nil, serviceerror.NewWorkflowExecutionAlreadyStarted("msg", "", ""))
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.IsGlobalNamespace(), s.version).Return(cluster.TestCurrentClusterName).AnyTimes()

	_, err = s.transferQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessStartChildExecution_Failure_TargetNamespaceNotFound() {
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
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: timestamp.DurationPtr(2 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
			},
			ContinueAsNewInitiator: enumspb.CONTINUE_AS_NEW_INITIATOR_UNSPECIFIED,
		},
	)
	s.NoError(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := int64(59)

	event, _ = addStartChildWorkflowExecutionInitiatedEvent(
		mutableState,
		event.GetEventId(),
		uuid.New(),
		s.namespace,
		s.namespaceID,
		childWorkflowID,
		childWorkflowType,
		childTaskQueueName,
		nil,
		1*time.Second,
		1*time.Second,
		1*time.Second,
		enumspb.PARENT_CLOSE_POLICY_TERMINATE,
	)

	transferTask := &tasks.StartChildExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		TargetNamespaceID:   tests.MissedNamespaceID.String(),
		TargetWorkflowID:    childWorkflowID,
		TaskID:              taskID,
		InitiatedEventID:    event.GetEventId(),
		VisibilityTimestamp: time.Now().UTC(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.IsGlobalNamespace(), s.version).Return(cluster.TestCurrentClusterName).AnyTimes()

	_, err = s.transferQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.NoError(err)
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
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: timestamp.DurationPtr(2 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := int64(59)

	event, ci := addStartChildWorkflowExecutionInitiatedEvent(
		mutableState,
		event.GetEventId(),
		uuid.New(),
		s.childNamespace,
		s.childNamespaceID,
		childWorkflowID,
		childWorkflowType,
		childTaskQueueName,
		nil,
		1*time.Second,
		1*time.Second,
		1*time.Second,
		enumspb.PARENT_CLOSE_POLICY_TERMINATE,
	)

	transferTask := &tasks.StartChildExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		TargetNamespaceID:   tests.ChildNamespaceID.String(),
		TargetWorkflowID:    childWorkflowID,
		TaskID:              taskID,
		InitiatedEventID:    event.GetEventId(),
		VisibilityTimestamp: time.Now().UTC(),
	}
	childClock := vclock.NewVectorClock(rand.Int63(), rand.Int31(), rand.Int63())
	event = addChildWorkflowExecutionStartedEvent(mutableState, event.GetEventId(), childWorkflowID, childRunID, childWorkflowType, childClock)
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()
	ci.StartedEventId = event.GetEventId()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().ScheduleWorkflowTask(gomock.Any(), &historyservice.ScheduleWorkflowTaskRequest{
		NamespaceId: tests.ChildNamespaceID.String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: childWorkflowID,
			RunId:      childRunID,
		},
		IsFirstWorkflowTask: true,
		Clock:               childClock,
	}).Return(nil, nil)

	_, err = s.transferQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
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
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: timestamp.DurationPtr(2 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := int64(59)

	event, ci := addStartChildWorkflowExecutionInitiatedEvent(
		mutableState,
		event.GetEventId(),
		uuid.New(),
		s.childNamespace,
		s.childNamespaceID,
		childExecution.GetWorkflowId(),
		childWorkflowType,
		childTaskQueueName,
		nil,
		1*time.Second,
		1*time.Second,
		1*time.Second,
		enumspb.PARENT_CLOSE_POLICY_TERMINATE,
	)

	transferTask := &tasks.StartChildExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		TargetNamespaceID:   tests.ChildNamespaceID.String(),
		TargetWorkflowID:    childExecution.GetWorkflowId(),
		TaskID:              taskID,
		InitiatedEventID:    event.GetEventId(),
		VisibilityTimestamp: time.Now().UTC(),
	}
	childClock := vclock.NewVectorClock(rand.Int63(), rand.Int31(), rand.Int63())
	event = addChildWorkflowExecutionStartedEvent(mutableState, event.GetEventId(), childExecution.GetWorkflowId(), childExecution.GetRunId(), childWorkflowType, childClock)
	ci.StartedEventId = event.GetEventId()
	event = addChildWorkflowExecutionCompletedEvent(mutableState, ci.InitiatedEventId, &childExecution, &historypb.WorkflowExecutionCompletedEventAttributes{
		Result:                       payloads.EncodeString("some random child workflow execution result"),
		WorkflowTaskCompletedEventId: transferTask.InitiatedEventID,
	})
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	_, err = s.transferQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Nil(err)
}

func (s *transferQueueActiveTaskExecutorSuite) TestProcessorStartChildExecution_ChildStarted_ParentClosed() {
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
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: timestamp.DurationPtr(2 * time.Second),
				WorkflowTaskTimeout:      timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := int64(59)

	event, ci := addStartChildWorkflowExecutionInitiatedEvent(
		mutableState,
		event.GetEventId(),
		uuid.New(),
		s.childNamespace,
		s.childNamespaceID,
		childExecution.GetWorkflowId(),
		childWorkflowType,
		childTaskQueueName,
		nil,
		1*time.Second,
		1*time.Second,
		1*time.Second,
		enumspb.PARENT_CLOSE_POLICY_ABANDON,
	)

	transferTask := &tasks.StartChildExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		TargetNamespaceID:   tests.ChildNamespaceID.String(),
		TargetWorkflowID:    childExecution.GetWorkflowId(),
		TaskID:              taskID,
		InitiatedEventID:    event.GetEventId(),
		VisibilityTimestamp: time.Now().UTC(),
	}
	childClock := vclock.NewVectorClock(rand.Int63(), rand.Int31(), rand.Int63())
	event = addChildWorkflowExecutionStartedEvent(mutableState, event.GetEventId(), childExecution.GetWorkflowId(), childExecution.GetRunId(), childWorkflowType, childClock)
	ci.StartedEventId = event.GetEventId()
	wt = addWorkflowTaskScheduledEvent(mutableState)
	event = addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, "some random identity")
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")
	event = addCompleteWorkflowEvent(mutableState, event.EventId, nil)
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().ScheduleWorkflowTask(gomock.Any(), &historyservice.ScheduleWorkflowTaskRequest{
		NamespaceId: s.childNamespaceID.String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: childExecution.WorkflowId,
			RunId:      childExecution.RunId,
		},
		IsFirstWorkflowTask: true,
		Clock:               childClock,
	}).Return(&historyservice.ScheduleWorkflowTaskResponse{}, nil).Times(1)

	_, err = s.transferQueueActiveTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
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
	task *tasks.ActivityTask,
	ai *persistencespb.ActivityInfo,
) *matchingservice.AddActivityTaskRequest {
	return &matchingservice.AddActivityTaskRequest{
		NamespaceId:       task.NamespaceID,
		SourceNamespaceId: task.NamespaceID,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: task.WorkflowID,
			RunId:      task.RunID,
		},
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: task.TaskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		ScheduledEventId:       task.ScheduledEventID,
		ScheduleToStartTimeout: ai.ScheduleToStartTimeout,
		Clock:                  vclock.NewVectorClock(s.mockClusterMetadata.GetClusterID(), s.mockShard.GetShardID(), task.TaskID),
	}
}

func (s *transferQueueActiveTaskExecutorSuite) createAddWorkflowTaskRequest(
	task *tasks.WorkflowTask,
	mutableState workflow.MutableState,
) *matchingservice.AddWorkflowTaskRequest {
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
		NamespaceId: task.NamespaceID,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: task.WorkflowID,
			RunId:      task.RunID,
		},
		TaskQueue:              taskQueue,
		ScheduledEventId:       task.ScheduledEventID,
		ScheduleToStartTimeout: &timeout,
		Clock:                  vclock.NewVectorClock(s.mockClusterMetadata.GetClusterID(), s.mockShard.GetShardID(), task.TaskID),
	}
}

func (s *transferQueueActiveTaskExecutorSuite) createRequestCancelWorkflowExecutionRequest(
	targetNamespace namespace.Name,
	task *tasks.CancelExecutionTask,
	rci *persistencespb.RequestCancelInfo,
	attributes *historypb.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes,
) *historyservice.RequestCancelWorkflowExecutionRequest {
	sourceExecution := commonpb.WorkflowExecution{
		WorkflowId: task.WorkflowID,
		RunId:      task.RunID,
	}
	targetExecution := commonpb.WorkflowExecution{
		WorkflowId: task.TargetWorkflowID,
		RunId:      task.TargetRunID,
	}

	return &historyservice.RequestCancelWorkflowExecutionRequest{
		NamespaceId: task.TargetNamespaceID,
		CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{
			Namespace:         targetNamespace.String(),
			WorkflowExecution: &targetExecution,
			Identity:          consts.IdentityHistoryService,
			// Use the same request ID to dedupe RequestCancelWorkflowExecution calls
			RequestId: rci.GetCancelRequestId(),
			Reason:    attributes.Reason,
		},
		ExternalInitiatedEventId:  task.InitiatedEventID,
		ExternalWorkflowExecution: &sourceExecution,
		ChildWorkflowOnly:         task.TargetChildWorkflowOnly,
	}
}

func (s *transferQueueActiveTaskExecutorSuite) createSignalWorkflowExecutionRequest(
	targetNamespace namespace.Name,
	task *tasks.SignalExecutionTask,
	si *persistencespb.SignalInfo,
	attributes *historypb.SignalExternalWorkflowExecutionInitiatedEventAttributes,
) *historyservice.SignalWorkflowExecutionRequest {
	sourceExecution := commonpb.WorkflowExecution{
		WorkflowId: task.WorkflowID,
		RunId:      task.RunID,
	}
	targetExecution := commonpb.WorkflowExecution{
		WorkflowId: task.TargetWorkflowID,
		RunId:      task.TargetRunID,
	}

	return &historyservice.SignalWorkflowExecutionRequest{
		NamespaceId: task.TargetNamespaceID,
		SignalRequest: &workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         targetNamespace.String(),
			WorkflowExecution: &targetExecution,
			Identity:          consts.IdentityHistoryService,
			SignalName:        attributes.SignalName,
			Input:             attributes.Input,
			RequestId:         si.GetRequestId(),
			Control:           attributes.Control,
			Header:            attributes.Header,
		},
		ExternalWorkflowExecution: &sourceExecution,
		ChildWorkflowOnly:         task.TargetChildWorkflowOnly,
	}
}

func (s *transferQueueActiveTaskExecutorSuite) createChildWorkflowExecutionRequest(
	namespace namespace.Name,
	childNamespace namespace.Name,
	task *tasks.StartChildExecutionTask,
	mutableState workflow.MutableState,
	ci *persistencespb.ChildExecutionInfo,
) *historyservice.StartWorkflowExecutionRequest {
	event, err := mutableState.GetChildExecutionInitiatedEvent(context.Background(), task.InitiatedEventID)
	s.NoError(err)
	attributes := event.GetStartChildWorkflowExecutionInitiatedEventAttributes()
	execution := commonpb.WorkflowExecution{
		WorkflowId: task.WorkflowID,
		RunId:      task.RunID,
	}
	now := s.timeSource.Now().UTC()
	return &historyservice.StartWorkflowExecutionRequest{
		Attempt:     1,
		NamespaceId: task.TargetNamespaceID,
		StartRequest: &workflowservice.StartWorkflowExecutionRequest{
			Namespace:                childNamespace.String(),
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
			NamespaceId:      task.NamespaceID,
			Namespace:        tests.Namespace.String(),
			Execution:        &execution,
			InitiatedId:      task.InitiatedEventID,
			InitiatedVersion: task.Version,
			Clock:            vclock.NewVectorClock(s.mockClusterMetadata.GetClusterID(), s.mockShard.GetShardID(), task.TaskID),
		},
		FirstWorkflowTaskBackoff:        backoff.GetBackoffForNextScheduleNonNegative(attributes.GetCronSchedule(), now, now),
		ContinueAsNewInitiator:          enumspb.CONTINUE_AS_NEW_INITIATOR_UNSPECIFIED,
		WorkflowExecutionExpirationTime: timestamp.TimePtr(now.Add(*attributes.WorkflowExecutionTimeout).Round(time.Millisecond)),
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

func (s *transferQueueActiveTaskExecutorSuite) newTaskExecutable(
	task tasks.Task,
) queues.Executable {
	return queues.NewExecutable(task, nil, s.transferQueueActiveTaskExecutor, nil, nil, s.mockShard.GetTimeSource(), nil, nil, queues.QueueTypeActiveTransfer, nil)
}
