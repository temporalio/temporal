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
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/vclock"
	"go.temporal.io/server/service/history/workflow"
	warchiver "go.temporal.io/server/service/worker/archiver"
)

type (
	transferQueueStandbyTaskExecutorSuite struct {
		suite.Suite
		*require.Assertions

		controller             *gomock.Controller
		mockShard              *shard.ContextTest
		mockNamespaceCache     *namespace.MockRegistry
		mockClusterMetadata    *cluster.MockMetadata
		mockAdminClient        *adminservicemock.MockAdminServiceClient
		mockNDCHistoryResender *xdc.MockNDCHistoryResender
		mockHistoryClient      *historyservicemock.MockHistoryServiceClient
		mockMatchingClient     *matchingservicemock.MockMatchingServiceClient

		mockExecutionMgr     *persistence.MockExecutionManager
		mockArchivalClient   *warchiver.MockClient
		mockArchivalMetadata *archiver.MockArchivalMetadata
		mockArchiverProvider *provider.MockArchiverProvider

		workflowCache        workflow.Cache
		logger               log.Logger
		namespaceID          namespace.ID
		namespaceEntry       *namespace.Namespace
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

	config := tests.NewDynamicConfig()

	s.namespaceEntry = tests.GlobalStandbyNamespaceEntry
	s.namespaceID = s.namespaceEntry.ID()
	s.version = s.namespaceEntry.FailoverVersion()
	s.now = time.Now().UTC()
	s.timeSource = clock.NewEventTimeSource().Update(s.now)
	s.fetchHistoryDuration = config.StandbyTaskMissingEventsResendDelay() +
		(config.StandbyTaskMissingEventsDiscardDelay()-config.StandbyTaskMissingEventsResendDelay())/2
	s.discardDuration = config.StandbyTaskMissingEventsDiscardDelay() * 2

	s.controller = gomock.NewController(s.T())
	s.mockNDCHistoryResender = xdc.NewMockNDCHistoryResender(s.controller)
	s.mockArchivalClient = warchiver.NewMockClient(s.controller)
	s.mockShard = shard.NewTestContextWithTimeSource(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistencespb.ShardInfo{
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

	s.mockHistoryClient = s.mockShard.Resource.HistoryClient
	s.mockMatchingClient = s.mockShard.Resource.MatchingClient
	s.mockExecutionMgr = s.mockShard.Resource.ExecutionMgr
	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata
	s.mockArchivalMetadata = s.mockShard.Resource.ArchivalMetadata
	s.mockArchiverProvider = s.mockShard.Resource.ArchiverProvider
	s.mockNamespaceCache = s.mockShard.Resource.NamespaceCache
	s.mockAdminClient = s.mockShard.Resource.RemoteAdminClient
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(tests.Namespace).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.TargetNamespaceID).Return(tests.GlobalTargetNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(tests.TargetNamespace).Return(tests.GlobalTargetNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.ParentNamespaceID).Return(tests.GlobalParentNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(tests.ParentNamespace).Return(tests.GlobalParentNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.ChildNamespaceID).Return(tests.GlobalChildNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(tests.ChildNamespace).Return(tests.GlobalChildNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.StandbyNamespaceID).Return(tests.GlobalStandbyNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(tests.StandbyNamespace).Return(tests.GlobalStandbyNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceName(tests.StandbyNamespaceID).Return(tests.StandbyNamespace, nil).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetClusterID().Return(cluster.TestCurrentClusterInitialFailoverVersion).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.IsGlobalNamespace(), s.version).Return(s.clusterName).AnyTimes()

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
	}
	s.mockShard.SetEngineForTesting(h)
	s.clusterName = cluster.TestAlternativeClusterName

	s.transferQueueStandbyTaskExecutor = newTransferQueueStandbyTaskExecutor(
		s.mockShard,
		s.workflowCache,
		s.mockArchivalClient,
		s.mockNDCHistoryResender,
		s.logger,
		metrics.NoopMetricsHandler,
		s.clusterName,
		s.mockShard.Resource.GetMatchingClient(),
	).(*transferQueueStandbyTaskExecutor)
}

func (s *transferQueueStandbyTaskExecutorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessActivityTask_Pending() {
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
	event, _ = addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID, activityType, taskQueueName, &commonpb.Payloads{}, 1*time.Second, 1*time.Second, 1*time.Second, 1*time.Second)

	now := time.Now().UTC()
	transferTask := &tasks.ActivityTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		VisibilityTimestamp: now,
		TaskID:              taskID,
		TaskQueue:           taskQueueName,
		ScheduledEventID:    event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	// no-op post action
	s.mockShard.SetCurrentTime(s.clusterName, now)
	_, err = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(consts.ErrTaskRetry, err)

	// resend history post action
	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.fetchHistoryDuration))
	s.mockAdminClient.EXPECT().RefreshWorkflowTasks(gomock.Any(), &adminservice.RefreshWorkflowTasksRequest{
		Namespace:   s.namespaceEntry.Name().String(),
		NamespaceId: s.namespaceEntry.ID().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: transferTask.WorkflowID,
			RunId:      transferTask.RunID,
		},
	}).Return(&adminservice.RefreshWorkflowTasksResponse{}, nil)
	s.mockNDCHistoryResender.EXPECT().SendSingleWorkflowHistory(
		s.clusterName,
		namespace.ID(transferTask.NamespaceID),
		transferTask.WorkflowID,
		transferTask.RunID,
		event.GetEventId(),
		s.version,
		int64(0),
		int64(0),
	).Return(nil)
	_, err = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(consts.ErrTaskRetry, err)

	// push to matching post action
	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.discardDuration))
	s.mockMatchingClient.EXPECT().AddActivityTask(gomock.Any(), gomock.Any(), gomock.Any()).Return(&matchingservice.AddActivityTaskResponse{}, nil)
	_, err = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Nil(err)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessActivityTask_Success() {
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
	event, _ = addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID, activityType, taskQueueName, &commonpb.Payloads{}, 1*time.Second, 1*time.Second, 1*time.Second, 1*time.Second)

	now := time.Now().UTC()
	transferTask := &tasks.ActivityTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		VisibilityTimestamp: now,
		TaskID:              taskID,
		TaskQueue:           taskQueueName,
		ScheduledEventID:    event.GetEventId(),
	}

	event = addActivityTaskStartedEvent(mutableState, event.GetEventId(), "")
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, now)
	_, err = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Nil(err)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessWorkflowTask_Pending() {
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

	taskID := int64(59)
	wt := addWorkflowTaskScheduledEvent(mutableState)

	now := time.Now().UTC()
	transferTask := &tasks.WorkflowTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		VisibilityTimestamp: now,
		TaskID:              taskID,
		TaskQueue:           taskQueueName,
		ScheduledEventID:    wt.ScheduledEventID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, wt.ScheduledEventID, wt.Version)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	// no-op post action
	s.mockShard.SetCurrentTime(s.clusterName, now)
	_, err = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(consts.ErrTaskRetry, err)

	// resend history post action
	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.fetchHistoryDuration))
	s.mockAdminClient.EXPECT().RefreshWorkflowTasks(gomock.Any(), &adminservice.RefreshWorkflowTasksRequest{
		Namespace:   s.namespaceEntry.Name().String(),
		NamespaceId: s.namespaceEntry.ID().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: transferTask.WorkflowID,
			RunId:      transferTask.RunID,
		},
	}).Return(&adminservice.RefreshWorkflowTasksResponse{}, nil)
	s.mockNDCHistoryResender.EXPECT().SendSingleWorkflowHistory(
		s.clusterName,
		namespace.ID(transferTask.NamespaceID),
		transferTask.WorkflowID,
		transferTask.RunID,
		wt.ScheduledEventID,
		s.version,
		int64(0),
		int64(0),
	).Return(nil)
	_, err = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(consts.ErrTaskRetry, err)

	// push to matching post action
	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.discardDuration))
	s.mockMatchingClient.EXPECT().AddWorkflowTask(gomock.Any(), gomock.Any(), gomock.Any()).Return(&matchingservice.AddWorkflowTaskResponse{}, nil)
	_, err = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Nil(err)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessWorkflowTask_Success_FirstWorkflowTask() {
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

	taskID := int64(59)
	wt := addWorkflowTaskScheduledEvent(mutableState)

	now := time.Now().UTC()
	transferTask := &tasks.WorkflowTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		VisibilityTimestamp: now,
		TaskID:              taskID,
		TaskQueue:           taskQueueName,
		ScheduledEventID:    wt.ScheduledEventID,
	}

	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, now)
	_, err = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Nil(err)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessWorkflowTask_Success_NonFirstWorkflowTask() {
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
	addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := int64(59)
	wt = addWorkflowTaskScheduledEvent(mutableState)

	now := time.Now().UTC()
	transferTask := &tasks.WorkflowTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		VisibilityTimestamp: now,
		TaskID:              taskID,
		TaskQueue:           taskQueueName,
		ScheduledEventID:    wt.ScheduledEventID,
	}

	event = addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, now)
	_, err = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Nil(err)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessCloseExecution() {
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

	now := time.Now().UTC()
	transferTask := &tasks.CloseExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		VisibilityTimestamp: now,
		TaskID:              taskID,
	}

	expectedVerificationRequest := &historyservice.VerifyChildExecutionCompletionRecordedRequest{
		NamespaceId:            parentNamespaceID,
		ParentExecution:        parentExecution,
		ChildExecution:         &execution,
		ParentInitiatedId:      parentInitiatedID,
		ParentInitiatedVersion: parentInitiatedVersion,
		Clock:                  parentClock,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewDisabledArchvialConfig()).AnyTimes()

	s.mockShard.SetCurrentTime(s.clusterName, now)
	s.mockHistoryClient.EXPECT().VerifyChildExecutionCompletionRecorded(gomock.Any(), expectedVerificationRequest).Return(nil, nil)
	_, err = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Nil(err)

	s.mockHistoryClient.EXPECT().VerifyChildExecutionCompletionRecorded(gomock.Any(), expectedVerificationRequest).Return(nil, consts.ErrWorkflowExecutionNotFound)
	_, err = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(consts.ErrTaskRetry, err)

	s.mockHistoryClient.EXPECT().VerifyChildExecutionCompletionRecorded(gomock.Any(), expectedVerificationRequest).Return(nil, consts.ErrWorkflowNotReady)
	_, err = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(consts.ErrTaskRetry, err)

	s.mockHistoryClient.EXPECT().VerifyChildExecutionCompletionRecorded(gomock.Any(), expectedVerificationRequest).Return(nil, serviceerror.NewUnimplemented("not implemented"))
	_, err = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Nil(err)

	s.mockHistoryClient.EXPECT().VerifyChildExecutionCompletionRecorded(gomock.Any(), expectedVerificationRequest).Return(nil, errors.New("some random error"))
	_, err = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(errVerificationFailed, err)

	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.fetchHistoryDuration))
	s.mockHistoryClient.EXPECT().VerifyChildExecutionCompletionRecorded(gomock.Any(), expectedVerificationRequest).Return(nil, consts.ErrWorkflowNotReady)
	_, err = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(consts.ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.discardDuration))
	s.mockHistoryClient.EXPECT().VerifyChildExecutionCompletionRecorded(gomock.Any(), expectedVerificationRequest).Return(nil, consts.ErrWorkflowNotReady)
	_, err = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(consts.ErrTaskDiscarded, err)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessCancelExecution_Pending() {
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
	nextEventID := event.GetEventId()

	now := time.Now().UTC()
	transferTask := &tasks.CancelExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:                 s.version,
		VisibilityTimestamp:     now,
		TargetNamespaceID:       tests.TargetNamespaceID.String(),
		TargetWorkflowID:        targetExecution.GetWorkflowId(),
		TargetRunID:             targetExecution.GetRunId(),
		TargetChildWorkflowOnly: true,
		TaskID:                  taskID,
		InitiatedEventID:        event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, now)
	_, err = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(consts.ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.fetchHistoryDuration))
	s.mockAdminClient.EXPECT().RefreshWorkflowTasks(gomock.Any(), &adminservice.RefreshWorkflowTasksRequest{
		Namespace:   s.namespaceEntry.Name().String(),
		NamespaceId: s.namespaceEntry.ID().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: transferTask.WorkflowID,
			RunId:      transferTask.RunID,
		},
	}).Return(&adminservice.RefreshWorkflowTasksResponse{}, nil)
	s.mockNDCHistoryResender.EXPECT().SendSingleWorkflowHistory(
		s.clusterName,
		namespace.ID(transferTask.NamespaceID),
		transferTask.WorkflowID,
		transferTask.RunID,
		nextEventID,
		s.version,
		int64(0),
		int64(0),
	).Return(nil)
	_, err = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(consts.ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.discardDuration))
	_, err = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(consts.ErrTaskDiscarded, err)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessCancelExecution_Success() {
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

	now := time.Now().UTC()
	transferTask := &tasks.CancelExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		VisibilityTimestamp: now,
		TargetNamespaceID:   tests.TargetNamespaceID.String(),
		TargetWorkflowID:    targetExecution.GetWorkflowId(),
		TargetRunID:         targetExecution.GetRunId(),
		TaskID:              taskID,
		InitiatedEventID:    event.GetEventId(),
	}

	event = addCancelRequestedEvent(mutableState, event.GetEventId(), tests.TargetNamespace, tests.TargetNamespaceID, targetExecution.GetWorkflowId(), targetExecution.GetRunId())
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, now)
	_, err = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Nil(err)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessSignalExecution_Pending() {
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
		tests.TargetNamespace, tests.TargetNamespaceID, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), signalName, nil, "", nil)
	nextEventID := event.GetEventId()

	now := time.Now().UTC()
	transferTask := &tasks.SignalExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		VisibilityTimestamp: now,
		TargetNamespaceID:   tests.TargetNamespaceID.String(),
		TargetWorkflowID:    targetExecution.GetWorkflowId(),
		TargetRunID:         targetExecution.GetRunId(),
		TaskID:              taskID,
		InitiatedEventID:    event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, now)
	_, err = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(consts.ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.fetchHistoryDuration))
	s.mockAdminClient.EXPECT().RefreshWorkflowTasks(gomock.Any(), &adminservice.RefreshWorkflowTasksRequest{
		Namespace:   s.namespaceEntry.Name().String(),
		NamespaceId: s.namespaceEntry.ID().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: transferTask.WorkflowID,
			RunId:      transferTask.RunID,
		},
	}).Return(&adminservice.RefreshWorkflowTasksResponse{}, nil)
	s.mockNDCHistoryResender.EXPECT().SendSingleWorkflowHistory(
		s.clusterName,
		namespace.ID(transferTask.NamespaceID),
		transferTask.WorkflowID,
		transferTask.RunID,
		nextEventID,
		s.version,
		int64(0),
		int64(0),
	).Return(nil)
	_, err = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(consts.ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.discardDuration))
	_, err = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(consts.ErrTaskDiscarded, err)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessSignalExecution_Success() {
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
		tests.TargetNamespace, tests.TargetNamespaceID, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), signalName, nil, "", nil)

	now := time.Now().UTC()
	transferTask := &tasks.SignalExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		VisibilityTimestamp: now,
		TargetNamespaceID:   tests.TargetNamespaceID.String(),
		TargetWorkflowID:    targetExecution.GetWorkflowId(),
		TargetRunID:         targetExecution.GetRunId(),
		TaskID:              taskID,
		InitiatedEventID:    event.GetEventId(),
	}

	event = addSignaledEvent(mutableState, event.GetEventId(), tests.TargetNamespace, tests.TargetNamespaceID, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), "")
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, now)
	_, err = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Nil(err)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessStartChildExecution_Pending() {
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
		},
	)
	s.Nil(err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := int64(59)
	event, _ = addStartChildWorkflowExecutionInitiatedEvent(mutableState, event.GetEventId(), uuid.New(),
		tests.ChildNamespace, tests.ChildNamespaceID, childWorkflowID, childWorkflowType, childTaskQueueName, nil, 1*time.Second, 1*time.Second, 1*time.Second, enumspb.PARENT_CLOSE_POLICY_ABANDON)
	nextEventID := event.GetEventId()

	now := time.Now().UTC()
	transferTask := &tasks.StartChildExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		VisibilityTimestamp: now,
		TargetNamespaceID:   tests.ChildNamespaceID.String(),
		TargetWorkflowID:    childWorkflowID,
		TaskID:              taskID,
		InitiatedEventID:    event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, now)
	_, err = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(consts.ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.fetchHistoryDuration))
	s.mockAdminClient.EXPECT().RefreshWorkflowTasks(gomock.Any(), &adminservice.RefreshWorkflowTasksRequest{
		Namespace:   s.namespaceEntry.Name().String(),
		NamespaceId: s.namespaceEntry.ID().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: transferTask.WorkflowID,
			RunId:      transferTask.RunID,
		},
	}).Return(&adminservice.RefreshWorkflowTasksResponse{}, nil)
	s.mockNDCHistoryResender.EXPECT().SendSingleWorkflowHistory(
		s.clusterName,
		namespace.ID(transferTask.NamespaceID),
		transferTask.WorkflowID,
		transferTask.RunID,
		nextEventID,
		s.version,
		int64(0),
		int64(0),
	).Return(nil)
	_, err = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(consts.ErrTaskRetry, err)

	event = addChildWorkflowExecutionStartedEvent(mutableState, event.GetEventId(), childWorkflowID, uuid.New(), childWorkflowType, nil)
	mutableState.FlushBufferedEvents()

	// clear the cache
	s.transferQueueStandbyTaskExecutor.cache = workflow.NewCache(s.mockShard)
	persistenceMutableState = s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.fetchHistoryDuration))
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockHistoryClient.EXPECT().VerifyFirstWorkflowTaskScheduled(gomock.Any(), gomock.Any()).Return(nil, nil)
	_, err = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Nil(err)

	s.mockHistoryClient.EXPECT().VerifyFirstWorkflowTaskScheduled(gomock.Any(), gomock.Any()).Return(nil, consts.ErrWorkflowNotReady)
	_, err = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(consts.ErrTaskRetry, err)

	s.mockHistoryClient.EXPECT().VerifyFirstWorkflowTaskScheduled(gomock.Any(), gomock.Any()).Return(nil, consts.ErrWorkflowExecutionNotFound)
	_, err = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(consts.ErrTaskRetry, err)

	s.mockHistoryClient.EXPECT().VerifyFirstWorkflowTaskScheduled(gomock.Any(), gomock.Any()).Return(nil, &serviceerror.Unimplemented{})
	_, err = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Nil(err)

	s.mockHistoryClient.EXPECT().VerifyFirstWorkflowTaskScheduled(gomock.Any(), gomock.Any()).Return(nil, errors.New("some random error"))
	_, err = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(errVerificationFailed, err)

	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.discardDuration))
	s.mockHistoryClient.EXPECT().VerifyFirstWorkflowTaskScheduled(gomock.Any(), gomock.Any()).Return(nil, &serviceerror.WorkflowNotReady{})
	_, err = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Equal(consts.ErrTaskDiscarded, err)

	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.discardDuration))
	s.mockHistoryClient.EXPECT().VerifyFirstWorkflowTaskScheduled(gomock.Any(), gomock.Any()).Return(nil, nil)
	_, err = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Nil(err)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessStartChildExecution_Success() {
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
	event, childInfo := addStartChildWorkflowExecutionInitiatedEvent(mutableState, event.GetEventId(), uuid.New(),
		tests.ChildNamespace, tests.ChildNamespaceID, childWorkflowID, childWorkflowType, childTaskQueueName, nil, 1*time.Second, 1*time.Second, 1*time.Second, enumspb.PARENT_CLOSE_POLICY_ABANDON)

	now := time.Now().UTC()
	transferTask := &tasks.StartChildExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             s.version,
		VisibilityTimestamp: now,
		TargetNamespaceID:   tests.ChildNamespaceID.String(),
		TargetWorkflowID:    childWorkflowID,
		TaskID:              taskID,
		InitiatedEventID:    event.GetEventId(),
	}
	event = addChildWorkflowExecutionStartedEvent(mutableState, event.GetEventId(), childWorkflowID, uuid.New(), childWorkflowType, nil)
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()
	childInfo.StartedEventId = event.GetEventId()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().VerifyFirstWorkflowTaskScheduled(gomock.Any(), gomock.Any()).Return(nil, nil)

	s.mockShard.SetCurrentTime(s.clusterName, now)
	_, err = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Nil(err)

	// workflow closed && child started && parent close policy is abandon
	event, err = mutableState.AddTimeoutWorkflowEvent(
		mutableState.GetNextEventID(),
		enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET,
		uuid.New(),
	)
	s.NoError(err)

	s.transferQueueStandbyTaskExecutor.cache = workflow.NewCache(s.mockShard)
	persistenceMutableState = s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryClient.EXPECT().VerifyFirstWorkflowTaskScheduled(gomock.Any(), gomock.Any()).Return(nil, nil)

	s.mockShard.SetCurrentTime(s.clusterName, now)
	_, err = s.transferQueueStandbyTaskExecutor.Execute(context.Background(), s.newTaskExecutable(transferTask))
	s.Nil(err)
}

func (s *transferQueueStandbyTaskExecutorSuite) createPersistenceMutableState(
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

func (s *transferQueueStandbyTaskExecutorSuite) newTaskExecutable(
	task tasks.Task,
) queues.Executable {
	return queues.NewExecutable(task, nil, s.transferQueueStandbyTaskExecutor, nil, nil, s.mockShard.GetTimeSource(), nil, nil, queues.QueueTypeStandbyTransfer, nil)
}
