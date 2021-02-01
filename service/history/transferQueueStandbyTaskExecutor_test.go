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
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/mocks"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	dc "go.temporal.io/server/common/service/dynamicconfig"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/shard"
)

type (
	transferQueueStandbyTaskExecutorSuite struct {
		suite.Suite
		*require.Assertions

		controller             *gomock.Controller
		mockShard              *shard.ContextTest
		mockNamespaceCache     *cache.MockNamespaceCache
		mockClusterMetadata    *cluster.MockMetadata
		mockAdminClient        *adminservicemock.MockAdminServiceClient
		mockNDCHistoryResender *xdc.MockNDCHistoryResender
		mockMatchingClient     *matchingservicemock.MockMatchingServiceClient

		mockVisibilityMgr    *mocks.VisibilityManager
		mockExecutionMgr     *persistence.MockExecutionManager
		mockArchivalMetadata *archiver.MockArchivalMetadata
		mockArchiverProvider *provider.MockArchiverProvider

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

	config := configs.NewDynamicConfigForTest()
	config.VisibilityQueue = dc.GetStringPropertyFn(common.VisibilityQueueInternal)

	s.namespaceID = testNamespaceID
	s.namespaceEntry = testGlobalNamespaceEntry
	s.version = s.namespaceEntry.GetFailoverVersion()
	s.now = time.Now().UTC()
	s.timeSource = clock.NewEventTimeSource().Update(s.now)
	s.fetchHistoryDuration = config.StandbyTaskMissingEventsResendDelay() +
		(config.StandbyTaskMissingEventsDiscardDelay()-config.StandbyTaskMissingEventsResendDelay())/2
	s.discardDuration = config.StandbyTaskMissingEventsDiscardDelay() * 2

	s.controller = gomock.NewController(s.T())
	s.mockNDCHistoryResender = xdc.NewMockNDCHistoryResender(s.controller)

	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistencespb.ShardInfo{
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

	s.mockMatchingClient = s.mockShard.Resource.MatchingClient
	s.mockExecutionMgr = s.mockShard.Resource.ExecutionMgr
	s.mockVisibilityMgr = s.mockShard.Resource.VisibilityMgr
	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata
	s.mockArchivalMetadata = s.mockShard.Resource.ArchivalMetadata
	s.mockArchiverProvider = s.mockShard.Resource.ArchiverProvider
	s.mockNamespaceCache = s.mockShard.Resource.NamespaceCache
	s.mockAdminClient = s.mockShard.Resource.RemoteAdminClient
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
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessActivityTask_Pending() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

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
	event, _ = addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID, activityType, taskQueueName, &commonpb.Payloads{}, 1*time.Second, 1*time.Second, 1*time.Second, 1*time.Second)

	now := timestamp.TimeNowPtrUtc()
	transferTask := &persistencespb.TransferTaskInfo{
		Version:        s.version,
		NamespaceId:    s.namespaceID,
		WorkflowId:     execution.GetWorkflowId(),
		RunId:          execution.GetRunId(),
		VisibilityTime: now,
		TaskId:         taskID,
		TaskQueue:      taskQueueName,
		TaskType:       enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK,
		ScheduleId:     event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, *now)
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Equal(ErrTaskRetry, err)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessActivityTask_Pending_PushToMatching() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

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
	event, _ = addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID, activityType, taskQueueName, &commonpb.Payloads{}, 1*time.Second, 1*time.Second, 1*time.Second, 1*time.Second)

	now := timestamp.TimeNowPtrUtc()
	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.fetchHistoryDuration))
	transferTask := &persistencespb.TransferTaskInfo{
		Version:        s.version,
		NamespaceId:    s.namespaceID,
		WorkflowId:     execution.GetWorkflowId(),
		RunId:          execution.GetRunId(),
		VisibilityTime: now,
		TaskId:         taskID,
		TaskQueue:      taskQueueName,
		TaskType:       enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK,
		ScheduleId:     event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.EXPECT().AddActivityTask(gomock.Any(), gomock.Any()).Return(&matchingservice.AddActivityTaskResponse{}, nil).Times(1)

	s.mockShard.SetCurrentTime(s.clusterName, *now)
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessActivityTask_Success() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := newMutableStateBuilderWithVersionHistoriesForTest(s.mockShard, s.mockShard.GetEventsCache(), s.logger, s.version, execution.GetRunId())
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
	event, _ = addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID, activityType, taskQueueName, &commonpb.Payloads{}, 1*time.Second, 1*time.Second, 1*time.Second, 1*time.Second)

	now := timestamp.TimeNowPtrUtc()
	transferTask := &persistencespb.TransferTaskInfo{
		Version:        s.version,
		NamespaceId:    s.namespaceID,
		WorkflowId:     execution.GetWorkflowId(),
		RunId:          execution.GetRunId(),
		VisibilityTime: now,
		TaskId:         taskID,
		TaskQueue:      taskQueueName,
		TaskType:       enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK,
		ScheduleId:     event.GetEventId(),
	}

	event = addActivityTaskStartedEvent(mutableState, event.GetEventId(), "")
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, *now)
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessWorkflowTask_Pending() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

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
		},
	)
	s.Nil(err)

	taskID := int64(59)
	di := addWorkflowTaskScheduledEvent(mutableState)

	now := timestamp.TimeNowPtrUtc()
	transferTask := &persistencespb.TransferTaskInfo{
		Version:        s.version,
		NamespaceId:    s.namespaceID,
		WorkflowId:     execution.GetWorkflowId(),
		RunId:          execution.GetRunId(),
		VisibilityTime: now,
		TaskId:         taskID,
		TaskQueue:      taskQueueName,
		TaskType:       enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK,
		ScheduleId:     di.ScheduleID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, di.ScheduleID, di.Version)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, *now)
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Equal(ErrTaskRetry, err)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessWorkflowTask_Pending_PushToMatching() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

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
		},
	)
	s.Nil(err)

	taskID := int64(59)
	di := addWorkflowTaskScheduledEvent(mutableState)

	now := timestamp.TimeNowPtrUtc()
	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.fetchHistoryDuration))
	transferTask := &persistencespb.TransferTaskInfo{
		Version:        s.version,
		NamespaceId:    s.namespaceID,
		WorkflowId:     execution.GetWorkflowId(),
		RunId:          execution.GetRunId(),
		VisibilityTime: now,
		TaskId:         taskID,
		TaskQueue:      taskQueueName,
		TaskType:       enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK,
		ScheduleId:     di.ScheduleID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, di.ScheduleID, di.Version)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.EXPECT().AddWorkflowTask(gomock.Any(), gomock.Any()).Return(&matchingservice.AddWorkflowTaskResponse{}, nil).Times(1)

	s.mockShard.SetCurrentTime(s.clusterName, *now)
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessWorkflowTask_Success_FirstWorkflowTask() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

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
		},
	)
	s.Nil(err)

	taskID := int64(59)
	di := addWorkflowTaskScheduledEvent(mutableState)

	now := timestamp.TimeNowPtrUtc()
	transferTask := &persistencespb.TransferTaskInfo{
		Version:        s.version,
		NamespaceId:    s.namespaceID,
		WorkflowId:     execution.GetWorkflowId(),
		RunId:          execution.GetRunId(),
		VisibilityTime: now,
		TaskId:         taskID,
		TaskQueue:      taskQueueName,
		TaskType:       enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK,
		ScheduleId:     di.ScheduleID,
	}

	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, *now)
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessWorkflowTask_Success_NonFirstWorkflowTask() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

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
		},
	)
	s.Nil(err)

	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()
	addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	taskID := int64(59)
	di = addWorkflowTaskScheduledEvent(mutableState)

	now := timestamp.TimeNowPtrUtc()
	transferTask := &persistencespb.TransferTaskInfo{
		Version:        s.version,
		NamespaceId:    s.namespaceID,
		WorkflowId:     execution.GetWorkflowId(),
		RunId:          execution.GetRunId(),
		VisibilityTime: now,
		TaskId:         taskID,
		TaskQueue:      taskQueueName,
		TaskType:       enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK,
		ScheduleId:     di.ScheduleID,
	}

	event = addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, *now)
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessCloseExecution() {

	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

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
		},
	)
	s.Nil(err)

	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	taskID := int64(59)
	event = addCompleteWorkflowEvent(mutableState, event.GetEventId(), nil)

	now := timestamp.TimeNowPtrUtc()
	transferTask := &persistencespb.TransferTaskInfo{
		Version:        s.version,
		NamespaceId:    s.namespaceID,
		WorkflowId:     execution.GetWorkflowId(),
		RunId:          execution.GetRunId(),
		VisibilityTime: now,
		TaskId:         taskID,
		TaskQueue:      taskQueueName,
		TaskType:       enumsspb.TASK_TYPE_TRANSFER_CLOSE_EXECUTION,
		ScheduleId:     event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockArchivalMetadata.On("GetVisibilityConfig").Return(archiver.NewDisabledArchvialConfig())

	s.mockShard.SetCurrentTime(s.clusterName, *now)
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Nil(err)
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
		},
	)
	s.Nil(err)

	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	taskID := int64(59)
	event, _ = addRequestCancelInitiatedEvent(mutableState, event.GetEventId(), uuid.New(), testTargetNamespace, targetExecution.GetWorkflowId(), targetExecution.GetRunId())
	nextEventID := event.GetEventId()

	now := timestamp.TimeNowPtrUtc()
	transferTask := &persistencespb.TransferTaskInfo{
		Version:           s.version,
		NamespaceId:       s.namespaceID,
		WorkflowId:        execution.GetWorkflowId(),
		RunId:             execution.GetRunId(),
		VisibilityTime:    now,
		TargetNamespaceId: testTargetNamespaceID,
		TargetWorkflowId:  targetExecution.GetWorkflowId(),
		TargetRunId:       targetExecution.GetRunId(),
		TaskId:            taskID,
		TaskQueue:         taskQueueName,
		TaskType:          enumsspb.TASK_TYPE_TRANSFER_CANCEL_EXECUTION,
		ScheduleId:        event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, *now)
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Equal(ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.fetchHistoryDuration))
	s.mockAdminClient.EXPECT().RefreshWorkflowTasks(gomock.Any(), &adminservice.RefreshWorkflowTasksRequest{
		Namespace: s.namespaceEntry.GetInfo().Name,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: transferTask.GetWorkflowId(),
			RunId:      transferTask.GetRunId(),
		},
	}).Return(&adminservice.RefreshWorkflowTasksResponse{}, nil).Times(1)
	s.mockNDCHistoryResender.EXPECT().SendSingleWorkflowHistory(
		transferTask.GetNamespaceId(),
		transferTask.GetWorkflowId(),
		transferTask.GetRunId(),
		nextEventID,
		s.version,
		int64(0),
		int64(0),
	).Return(nil).Times(1)
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Equal(ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.discardDuration))
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Equal(ErrTaskDiscarded, err)
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
		},
	)
	s.Nil(err)

	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	taskID := int64(59)
	event, _ = addRequestCancelInitiatedEvent(mutableState, event.GetEventId(), uuid.New(), testTargetNamespace, targetExecution.GetWorkflowId(), targetExecution.GetRunId())

	now := timestamp.TimeNowPtrUtc()
	transferTask := &persistencespb.TransferTaskInfo{
		Version:           s.version,
		NamespaceId:       s.namespaceID,
		WorkflowId:        execution.GetWorkflowId(),
		RunId:             execution.GetRunId(),
		VisibilityTime:    now,
		TargetNamespaceId: testTargetNamespaceID,
		TargetWorkflowId:  targetExecution.GetWorkflowId(),
		TargetRunId:       targetExecution.GetRunId(),
		TaskId:            taskID,
		TaskQueue:         taskQueueName,
		TaskType:          enumsspb.TASK_TYPE_TRANSFER_CANCEL_EXECUTION,
		ScheduleId:        event.GetEventId(),
	}

	event = addCancelRequestedEvent(mutableState, event.GetEventId(), testTargetNamespaceID, targetExecution.GetWorkflowId(), targetExecution.GetRunId())
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, *now)
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
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
		},
	)
	s.Nil(err)

	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	taskID := int64(59)
	event, _ = addRequestSignalInitiatedEvent(mutableState, event.GetEventId(), uuid.New(),
		testTargetNamespace, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), signalName, nil, "")
	nextEventID := event.GetEventId()

	now := timestamp.TimeNowPtrUtc()
	transferTask := &persistencespb.TransferTaskInfo{
		Version:           s.version,
		NamespaceId:       s.namespaceID,
		WorkflowId:        execution.GetWorkflowId(),
		RunId:             execution.GetRunId(),
		VisibilityTime:    now,
		TargetNamespaceId: testTargetNamespaceID,
		TargetWorkflowId:  targetExecution.GetWorkflowId(),
		TargetRunId:       targetExecution.GetRunId(),
		TaskId:            taskID,
		TaskQueue:         taskQueueName,
		TaskType:          enumsspb.TASK_TYPE_TRANSFER_SIGNAL_EXECUTION,
		ScheduleId:        event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, *now)
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Equal(ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.fetchHistoryDuration))
	s.mockAdminClient.EXPECT().RefreshWorkflowTasks(gomock.Any(), &adminservice.RefreshWorkflowTasksRequest{
		Namespace: s.namespaceEntry.GetInfo().Name,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: transferTask.GetWorkflowId(),
			RunId:      transferTask.GetRunId(),
		},
	}).Return(&adminservice.RefreshWorkflowTasksResponse{}, nil).Times(1)
	s.mockNDCHistoryResender.EXPECT().SendSingleWorkflowHistory(
		transferTask.GetNamespaceId(),
		transferTask.GetWorkflowId(),
		transferTask.GetRunId(),
		nextEventID,
		s.version,
		int64(0),
		int64(0),
	).Return(nil).Times(1)
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Equal(ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.discardDuration))
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Equal(ErrTaskDiscarded, err)
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
		},
	)
	s.Nil(err)

	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	taskID := int64(59)
	event, _ = addRequestSignalInitiatedEvent(mutableState, event.GetEventId(), uuid.New(),
		testTargetNamespace, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), signalName, nil, "")

	now := timestamp.TimeNowPtrUtc()
	transferTask := &persistencespb.TransferTaskInfo{
		Version:           s.version,
		NamespaceId:       s.namespaceID,
		WorkflowId:        execution.GetWorkflowId(),
		RunId:             execution.GetRunId(),
		VisibilityTime:    now,
		TargetNamespaceId: testTargetNamespaceID,
		TargetWorkflowId:  targetExecution.GetWorkflowId(),
		TargetRunId:       targetExecution.GetRunId(),
		TaskId:            taskID,
		TaskQueue:         taskQueueName,
		TaskType:          enumsspb.TASK_TYPE_TRANSFER_SIGNAL_EXECUTION,
		ScheduleId:        event.GetEventId(),
	}

	event = addSignaledEvent(mutableState, event.GetEventId(), testTargetNamespace, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), "")
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, *now)
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
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
		},
	)
	s.Nil(err)

	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	taskID := int64(59)
	event, _ = addStartChildWorkflowExecutionInitiatedEvent(mutableState, event.GetEventId(), uuid.New(),
		testChildNamespace, childWorkflowID, childWorkflowType, childTaskQueueName, nil, 1*time.Second, 1*time.Second, 1*time.Second)
	nextEventID := event.GetEventId()

	now := timestamp.TimeNowPtrUtc()
	transferTask := &persistencespb.TransferTaskInfo{
		Version:           s.version,
		NamespaceId:       s.namespaceID,
		WorkflowId:        execution.GetWorkflowId(),
		RunId:             execution.GetRunId(),
		VisibilityTime:    now,
		TargetNamespaceId: testChildNamespaceID,
		TargetWorkflowId:  childWorkflowID,
		TargetRunId:       "",
		TaskId:            taskID,
		TaskQueue:         taskQueueName,
		TaskType:          enumsspb.TASK_TYPE_TRANSFER_START_CHILD_EXECUTION,
		ScheduleId:        event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, *now)
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Equal(ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.fetchHistoryDuration))
	s.mockAdminClient.EXPECT().RefreshWorkflowTasks(gomock.Any(), &adminservice.RefreshWorkflowTasksRequest{
		Namespace: s.namespaceEntry.GetInfo().Name,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: transferTask.GetWorkflowId(),
			RunId:      transferTask.GetRunId(),
		},
	}).Return(&adminservice.RefreshWorkflowTasksResponse{}, nil).Times(1)
	s.mockNDCHistoryResender.EXPECT().SendSingleWorkflowHistory(
		transferTask.GetNamespaceId(),
		transferTask.GetWorkflowId(),
		transferTask.GetRunId(),
		nextEventID,
		s.version,
		int64(0),
		int64(0),
	).Return(nil).Times(1)
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Equal(ErrTaskRetry, err)

	s.mockShard.SetCurrentTime(s.clusterName, now.Add(s.discardDuration))
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Equal(ErrTaskDiscarded, err)
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
			ContinueAsNewInitiator: enumspb.CONTINUE_AS_NEW_INITIATOR_UNSPECIFIED,
		},
	)
	s.Nil(err)

	di := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, di.ScheduleID, taskQueueName, uuid.New())
	di.StartedID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, "some random identity")

	taskID := int64(59)
	event, childInfo := addStartChildWorkflowExecutionInitiatedEvent(mutableState, event.GetEventId(), uuid.New(),
		testChildNamespace, childWorkflowID, childWorkflowType, childTaskQueueName, nil, 1*time.Second, 1*time.Second, 1*time.Second)

	now := timestamp.TimeNowPtrUtc()
	transferTask := &persistencespb.TransferTaskInfo{
		Version:           s.version,
		NamespaceId:       s.namespaceID,
		WorkflowId:        execution.GetWorkflowId(),
		RunId:             execution.GetRunId(),
		VisibilityTime:    now,
		TargetNamespaceId: testChildNamespaceID,
		TargetWorkflowId:  childWorkflowID,
		TargetRunId:       "",
		TaskId:            taskID,
		TaskQueue:         taskQueueName,
		TaskType:          enumsspb.TASK_TYPE_TRANSFER_START_CHILD_EXECUTION,
		ScheduleId:        event.GetEventId(),
	}

	event = addChildWorkflowExecutionStartedEvent(mutableState, event.GetEventId(), testChildNamespace, childWorkflowID, uuid.New(), childWorkflowType)
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()
	childInfo.StartedId = event.GetEventId()

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.mockShard.SetCurrentTime(s.clusterName, *now)
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessRecordWorkflowStartedTask() {

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

	now := timestamp.TimeNowPtrUtc()
	transferTask := &persistencespb.TransferTaskInfo{
		Version:        s.version,
		NamespaceId:    s.namespaceID,
		WorkflowId:     execution.GetWorkflowId(),
		RunId:          execution.GetRunId(),
		VisibilityTime: now,
		TaskId:         taskID,
		TaskQueue:      taskQueueName,
		TaskType:       enumsspb.TASK_TYPE_TRANSFER_RECORD_WORKFLOW_STARTED,
		ScheduleId:     event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, di.ScheduleID, di.Version)
	executionInfo := mutableState.GetExecutionInfo()
	executionState := mutableState.GetExecutionState()
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockVisibilityMgr.On("RecordWorkflowExecutionStarted", &persistence.RecordWorkflowExecutionStartedRequest{
		VisibilityRequestBase: &persistence.VisibilityRequestBase{
			NamespaceID: testNamespaceID,
			Namespace:   testNamespace,
			Execution: commonpb.WorkflowExecution{
				WorkflowId: executionInfo.WorkflowId,
				RunId:      executionState.RunId,
			},
			WorkflowTypeName: executionInfo.WorkflowTypeName,
			StartTimestamp:   timestamp.TimeValue(event.GetEventTime()).UnixNano(),
			TaskID:           taskID,
			TaskQueue:        taskQueueName,
		},
		RunTimeout: int64(timestamp.DurationValue(executionInfo.WorkflowRunTimeout).Round(time.Second).Seconds()),
	}).Return(nil).Once()

	s.mockShard.SetCurrentTime(s.clusterName, *now)
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueStandbyTaskExecutorSuite) TestProcessUpsertWorkflowSearchAttributesTask() {

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

	now := timestamp.TimeNowPtrUtc()
	transferTask := &persistencespb.TransferTaskInfo{
		Version:        s.version,
		NamespaceId:    s.namespaceID,
		WorkflowId:     execution.GetWorkflowId(),
		RunId:          execution.GetRunId(),
		VisibilityTime: now,
		TaskId:         taskID,
		TaskQueue:      taskQueueName,
		TaskType:       enumsspb.TASK_TYPE_TRANSFER_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES,
		ScheduleId:     event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, di.ScheduleID, di.Version)
	executionInfo := mutableState.GetExecutionInfo()
	executionState := mutableState.GetExecutionState()
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockVisibilityMgr.On("UpsertWorkflowExecution", &persistence.UpsertWorkflowExecutionRequest{
		VisibilityRequestBase: &persistence.VisibilityRequestBase{
			NamespaceID: testNamespaceID,
			Namespace:   testNamespace,
			Execution: commonpb.WorkflowExecution{
				WorkflowId: executionInfo.WorkflowId,
				RunId:      executionState.RunId,
			},
			WorkflowTypeName: executionInfo.WorkflowTypeName,
			StartTimestamp:   timestamp.TimeValue(event.GetEventTime()).UnixNano(),
			TaskID:           taskID,
			TaskQueue:        taskQueueName,
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		WorkflowTimeout: int64(timestamp.DurationValue(executionInfo.WorkflowRunTimeout).Round(time.Second).Seconds()),
	}).Return(nil).Once()

	s.mockShard.SetCurrentTime(s.clusterName, *now)
	err = s.transferQueueStandbyTaskExecutor.execute(transferTask, true)
	s.Nil(err)
}

func (s *transferQueueStandbyTaskExecutorSuite) createPersistenceMutableState(
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
