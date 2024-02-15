// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"google.golang.org/protobuf/types/known/durationpb"
)

type taskExecutorSuite struct {
	suite.Suite
	*require.Assertions

	namespaceID    namespace.ID
	namespaceEntry *namespace.Namespace
	controller     *gomock.Controller
	mockShard      *shard.ContextTest
	workflowCache  wcache.Cache
	now            time.Time
	version        int64
	timeSource     *clock.EventTimeSource
}

func TestTaskExecutor(t *testing.T) {
	s := new(taskExecutorSuite)
	suite.Run(t, s)
}

func (s *taskExecutorSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.namespaceID = tests.NamespaceID
	s.namespaceEntry = tests.GlobalNamespaceEntry
	s.now = time.Now().UTC()
	s.timeSource = clock.NewEventTimeSource().Update(s.now)
	s.controller = gomock.NewController(s.T())
	config := tests.NewDynamicConfig()
	s.version = s.namespaceEntry.FailoverVersion()

	s.mockShard = shard.NewTestContextWithTimeSource(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 1,
			RangeId: 1,
		},
		config,
		s.timeSource,
	)
	s.mockShard.SetEventsCacheForTesting(events.NewHostLevelEventsCache(
		s.mockShard.GetExecutionManager(),
		s.mockShard.GetConfig(),
		s.mockShard.GetMetricsHandler(),
		s.mockShard.GetLogger(),
		false,
	))
	s.workflowCache = wcache.NewHostLevelCache(s.mockShard.GetConfig())

	mockClusterMetadata := s.mockShard.Resource.ClusterMetadata
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.IsGlobalNamespace(), s.version).Return(mockClusterMetadata.GetCurrentClusterName()).AnyTimes()
	mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	mockClusterMetadata.EXPECT().GetClusterID().Return(tests.Version).AnyTimes()
	mockClusterMetadata.EXPECT().IsVersionFromSameCluster(tests.Version, tests.Version).Return(true).AnyTimes()

	mockTimerProcessor := queues.NewMockQueue(s.controller)
	mockTimerProcessor.EXPECT().Category().Return(tasks.CategoryTimer).AnyTimes()
	mockTimerProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()

	h := &historyEngineImpl{
		currentClusterName: s.mockShard.Resource.GetClusterMetadata().GetCurrentClusterName(),
		shardContext:       s.mockShard,
		clusterMetadata:    mockClusterMetadata,
		executionManager:   s.mockShard.GetExecutionManager(),
		logger:             s.mockShard.GetLogger(),
		tokenSerializer:    common.NewProtoTaskTokenSerializer(),
		metricsHandler:     s.mockShard.GetMetricsHandler(),
		eventNotifier:      events.NewNotifier(clock.NewRealTimeSource(), metrics.NoopMetricsHandler, func(namespace.ID, string) int32 { return 1 }),
		queueProcessors: map[tasks.Category]queues.Queue{
			mockTimerProcessor.Category(): mockTimerProcessor,
		},
	}
	s.mockShard.SetEngineForTesting(h)
}

func (s *taskExecutorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *taskExecutorSuite) TestProcessCallbackTask_TaskState() {
	type testcase struct {
		name             string
		mutateCallback   func(*persistencespb.CallbackInfo)
		assertOutcome    func(error)
		expectedGetCalls int
	}
	cases := []testcase{
		{
			name: "stale-task-version",
			mutateCallback: func(cb *persistencespb.CallbackInfo) {
				cb.NamespaceFailoverVersion++
			},
			assertOutcome: func(err error) {
				s.ErrorIs(err, queues.ErrStaleTask)
			},
			expectedGetCalls: 1,
		},
		{
			name: "stale-callback-version",
			mutateCallback: func(cb *persistencespb.CallbackInfo) {
				cb.NamespaceFailoverVersion--
			},
			assertOutcome: func(err error) {
				s.ErrorAs(err, new(queues.StaleStateError))
			},
			expectedGetCalls: 2,
		},
		{
			name: "stale-task-transitions",
			mutateCallback: func(cb *persistencespb.CallbackInfo) {
				cb.TransitionCount++
			},
			assertOutcome: func(err error) {
				s.ErrorIs(err, queues.ErrStaleTask)
			},
			expectedGetCalls: 1,
		},
		{
			name: "stale-callback-transitions",
			mutateCallback: func(cb *persistencespb.CallbackInfo) {
				cb.TransitionCount--
			},
			assertOutcome: func(err error) {
				s.ErrorAs(err, new(queues.StaleStateError))
			},
			expectedGetCalls: 2,
		},
	}
	for _, tc := range cases {
		tc := tc
		s.T().Run(tc.name, func(t *testing.T) {
			mutableState := s.prepareMutableStateWithCompletedWFT()
			event, err := mutableState.AddCompletedWorkflowEvent(mutableState.GetNextEventID(), &commandpb.CompleteWorkflowExecutionCommandAttributes{}, "")
			s.NoError(err)
			for _, cb := range mutableState.GetExecutionInfo().Callbacks {
				tc.mutateCallback(cb)
			}
			task := mutableState.PopTasks()[tasks.CategoryCallback][0]
			s.sealMutableState(mutableState, event, tc.expectedGetCalls)
			exec := taskExecutor{
				shardContext:   s.mockShard,
				cache:          s.workflowCache,
				metricsHandler: s.mockShard.GetMetricsHandler(),
				logger:         s.mockShard.GetLogger(),
			}
			_, _, _, err = exec.getValidatedMutableStateForTask(context.Background(), task, func(ms workflow.MutableState) error {
				_, err := exec.validateCallbackTask(ms, task.(*tasks.CallbackTask))
				return err
			})
			tc.assertOutcome(err)
		})
	}
}

func (s *taskExecutorSuite) prepareMutableStateWithCompletedWFT() workflow.MutableState {
	s.mockShard.Resource.NamespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(s.namespaceEntry, nil).AnyTimes()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.mockShard.GetLogger(), s.namespaceEntry.FailoverVersion(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType: &commonpb.WorkflowType{Name: "irrelevant"},
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: "irrelevant",
					Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
				},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
				CompletionCallbacks: []*commonpb.Callback{
					{
						Variant: &commonpb.Callback_Nexus_{
							Nexus: &commonpb.Callback_Nexus{
								Url: "http://destination/path",
							},
						},
					},
				},
			},
		},
	)
	s.NoError(err)
	wt := addWorkflowTaskScheduledEvent(mutableState)
	taskQueueName := "irrelevant"
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	_, err = mutableState.AddWorkflowTaskCompletedEvent(wt, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity: "some random identity",
	}, defaultWorkflowTaskCompletionLimits)
	s.NoError(err)

	return mutableState
}

func (s *taskExecutorSuite) sealMutableState(mutableState workflow.MutableState, lastEvent *historypb.HistoryEvent, expectedGetCalls int) {
	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(mutableState.GetExecutionInfo().GetVersionHistories())
	s.NoError(err)
	err = versionhistory.AddOrUpdateVersionHistoryItem(currentVersionHistory, versionhistory.NewVersionHistoryItem(
		lastEvent.EventId, lastEvent.Version,
	))
	s.NoError(err)
	persistenceMutableState := workflow.TestCloneToProto(mutableState)
	s.mockShard.Resource.ExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil).Times(expectedGetCalls)
}
