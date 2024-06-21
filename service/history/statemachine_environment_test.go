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
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/durationpb"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/history/workflow/cache"
)

type taskExecutorTestContext struct {
	t              *testing.T
	namespaceID    namespace.ID
	namespaceEntry *namespace.Namespace
	controller     *gomock.Controller
	mockShard      *shard.ContextTest
	workflowCache  cache.Cache
	now            time.Time
	version        int64
	timeSource     *clock.EventTimeSource
}

func newStateMachineEnvTestContext(t *testing.T, enableTransitionHistory bool) *taskExecutorTestContext {
	s := taskExecutorTestContext{}
	s.t = t
	s.namespaceID = tests.NamespaceID
	s.namespaceEntry = tests.GlobalNamespaceEntry
	s.now = time.Now().UTC()
	s.timeSource = clock.NewEventTimeSource().Update(s.now)
	s.controller = gomock.NewController(t)
	config := tests.NewDynamicConfig()
	config.EnableTransitionHistory = func() bool { return enableTransitionHistory }
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
	reg := hsm.NewRegistry()
	require.NoError(t, workflow.RegisterStateMachine(reg))
	require.NoError(t, callbacks.RegisterStateMachine(reg))
	require.NoError(t, callbacks.RegisterTaskSerializers(reg))
	s.mockShard.SetStateMachineRegistry(reg)
	s.workflowCache = cache.NewHostLevelCache(s.mockShard.GetConfig(), s.mockShard.GetMetricsHandler())

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
	return &s
}

func (s *taskExecutorTestContext) TearDown() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func TestValidateStateMachineRef(t *testing.T) {
	cases := []struct {
		name                    string
		enableTransitionHistory bool
		mutateRef               func(*hsm.Ref)
		assertOutcome           func(*testing.T, error)
	}{
		{
			name:                    "TaskGenerationStale",
			enableTransitionHistory: true,
			mutateRef: func(ref *hsm.Ref) {
				ref.TaskID = 1
			},
			assertOutcome: func(t *testing.T, err error) {
				require.ErrorIs(t, err, consts.ErrStaleReference)
			},
		},
		{
			name:                    "WithTransitionHistory/StalenessCheckFailure",
			enableTransitionHistory: true,
			mutateRef: func(ref *hsm.Ref) {
				ref.StateMachineRef.MutableStateNamespaceFailoverVersion++
			},
			assertOutcome: func(t *testing.T, err error) {
				require.ErrorIs(t, err, consts.ErrStaleState)
			},
		},
		{
			name:                    "WithoutTransitionHistory/CanBeStale/MachineStalenessCheckFailure",
			enableTransitionHistory: false,
			mutateRef: func(ref *hsm.Ref) {
				ref.StateMachineRef.MachineInitialNamespaceFailoverVersion++
			},
			assertOutcome: func(t *testing.T, err error) {
				require.ErrorIs(t, err, consts.ErrStaleState)
			},
		},
		{
			name:                    "WithoutTransitionHistory/CannotBeStale/MachineStalenessCheckFailure",
			enableTransitionHistory: false,
			mutateRef: func(ref *hsm.Ref) {
				ref.StateMachineRef.MachineInitialNamespaceFailoverVersion++
				ref.TaskID = tasks.MaximumKey.TaskID
			},
			assertOutcome: func(t *testing.T, err error) {
				require.ErrorIs(t, err, consts.ErrStaleReference)
			},
		},
		{
			name:                    "WithTransitionHistory/NodeNotFound",
			enableTransitionHistory: true,
			mutateRef: func(ref *hsm.Ref) {
				ref.StateMachineRef.Path[0].Id = "not-found"
			},
			assertOutcome: func(t *testing.T, err error) {
				require.ErrorIs(t, err, consts.ErrStaleReference)
			},
		},
		{
			name:                    "WithoutTransitionHistory/CanBeStale/NodeNotFound",
			enableTransitionHistory: false,
			mutateRef: func(ref *hsm.Ref) {
				ref.StateMachineRef.Path[0].Id = "not-found"
			},
			assertOutcome: func(t *testing.T, err error) {
				require.ErrorIs(t, err, consts.ErrStaleState)
			},
		},
		{
			name:                    "WithoutTransitionHistory/CannotBeStale/NodeNotFound",
			enableTransitionHistory: false,
			mutateRef: func(ref *hsm.Ref) {
				ref.StateMachineRef.Path[0].Id = "not-found"
				ref.TaskID = tasks.MaximumKey.TaskID
			},
			assertOutcome: func(t *testing.T, err error) {
				require.ErrorIs(t, err, consts.ErrStaleReference)
			},
		},
		{
			name:                    "WithTransitionHistory/MachineLastUpdateTransitionInequality",
			enableTransitionHistory: true,
			mutateRef: func(ref *hsm.Ref) {
				ref.StateMachineRef.MachineLastUpdateMutableStateTransitionCount++
			},
			assertOutcome: func(t *testing.T, err error) {
				require.ErrorIs(t, err, consts.ErrStaleReference)
			},
		},
		{
			name:                    "WithoutTransitionHistory/MachineTransitionInequality",
			enableTransitionHistory: false,
			mutateRef: func(ref *hsm.Ref) {
				ref.StateMachineRef.MachineTransitionCount++
			},
			assertOutcome: func(t *testing.T, err error) {
				require.ErrorIs(t, err, consts.ErrStaleReference)
			},
		},
		{
			name:                    "WithTransitionHistory/Valid",
			enableTransitionHistory: true,
			mutateRef: func(ref *hsm.Ref) {
			},
			assertOutcome: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
		{
			name:                    "WithoutTransitionHistory/Valid",
			enableTransitionHistory: false,
			mutateRef: func(ref *hsm.Ref) {
			},
			assertOutcome: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := newStateMachineEnvTestContext(t, tc.enableTransitionHistory)
			mutableState := s.prepareMutableStateWithTriggeredNexusCompletionCallback()
			snapshot, _, err := mutableState.CloseTransactionAsMutation(workflow.TransactionPolicyActive)
			require.NoError(t, err)
			task := snapshot.Tasks[tasks.CategoryOutbound][0]
			exec := stateMachineEnvironment{
				shardContext:   s.mockShard,
				cache:          s.workflowCache,
				metricsHandler: s.mockShard.GetMetricsHandler(),
				logger:         s.mockShard.GetLogger(),
			}

			cbt := task.(*tasks.StateMachineOutboundTask)
			ref := hsm.Ref{
				WorkflowKey:     taskWorkflowKey(task),
				StateMachineRef: cbt.Info.Ref,
			}
			tc.mutateRef(&ref)
			err = exec.validateStateMachineRef(mutableState, ref, true)
			tc.assertOutcome(t, err)
		})
	}
}

func TestAccess(t *testing.T) {
	cases := []struct {
		name                string
		accessType          hsm.AccessType
		workflowState       enumsspb.WorkflowExecutionState
		expectedSetRequests int
		accessor            func(*hsm.Node) error
		assertOutcome       func(*testing.T, error)
	}{
		{
			name:                "read success",
			accessType:          hsm.AccessRead,
			workflowState:       enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			expectedSetRequests: 0,
			accessor: func(n *hsm.Node) error {
				return nil
			},
			assertOutcome: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
		{
			name:                "read failure",
			accessType:          hsm.AccessRead,
			workflowState:       enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			expectedSetRequests: 0,
			accessor: func(n *hsm.Node) error {
				return fmt.Errorf("test read error")
			},
			assertOutcome: func(t *testing.T, err error) {
				require.ErrorContains(t, err, "test read error")
			},
		},
		{
			name:                "write success",
			accessType:          hsm.AccessWrite,
			workflowState:       enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			expectedSetRequests: 1,
			accessor: func(n *hsm.Node) error {
				return nil
			},
			assertOutcome: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
		{
			name:                "write error",
			accessType:          hsm.AccessWrite,
			workflowState:       enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			expectedSetRequests: 0,
			accessor: func(n *hsm.Node) error {
				return fmt.Errorf("test write error")
			},
			assertOutcome: func(t *testing.T, err error) {
				require.ErrorContains(t, err, "test write error")
			},
		},
		{
			name:                "write zombie",
			accessType:          hsm.AccessWrite,
			workflowState:       enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
			expectedSetRequests: 0,
			accessor: func(n *hsm.Node) error {
				return fmt.Errorf("accessor should not be called")
			},
			assertOutcome: func(t *testing.T, err error) {
				require.ErrorIs(t, err, consts.ErrWorkflowZombie)
			},
		},
		{
			name:                "read zombie",
			accessType:          hsm.AccessRead,
			workflowState:       enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
			expectedSetRequests: 0,
			accessor: func(n *hsm.Node) error {
				return nil
			},
			assertOutcome: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
		// TODO: test write success on open workflow updates instead of sets execution when we have machines that support that.
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newStateMachineEnvTestContext(t, true)
			mutableState := s.prepareMutableStateWithTriggeredNexusCompletionCallback()
			mutableState.GetExecutionState().State = tc.workflowState
			snapshot, _, err := mutableState.CloseTransactionAsMutation(workflow.TransactionPolicyActive)
			require.NoError(t, err)
			persistenceMutableState := workflow.TestCloneToProto(mutableState)
			em := s.mockShard.GetExecutionManager().(*persistence.MockExecutionManager)
			em.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
			em.EXPECT().SetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.SetWorkflowExecutionResponse{}, nil).Times(tc.expectedSetRequests)

			exec := stateMachineEnvironment{
				shardContext:   s.mockShard,
				cache:          s.workflowCache,
				metricsHandler: s.mockShard.GetMetricsHandler(),
				logger:         s.mockShard.GetLogger(),
			}

			task := snapshot.Tasks[tasks.CategoryOutbound][0]
			cbt := task.(*tasks.StateMachineOutboundTask)
			ref := hsm.Ref{
				WorkflowKey:     taskWorkflowKey(task),
				StateMachineRef: cbt.Info.Ref,
			}
			err = exec.Access(context.Background(), ref, tc.accessType, tc.accessor)
			tc.assertOutcome(t, err)
		})
	}
}

func (s *taskExecutorTestContext) prepareMutableStateWithReadyNexusCompletionCallback() *workflow.MutableStateImpl {
	s.mockShard.Resource.NamespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(s.namespaceEntry, nil).AnyTimes()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
	}
	mutableState := workflow.TestGlobalMutableState(s.mockShard, s.mockShard.GetEventsCache(), s.mockShard.GetLogger(), s.namespaceEntry.FailoverVersion(), execution.GetWorkflowId(), execution.GetRunId())
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
	require.NoError(s.t, err)
	return mutableState
}

func (s *taskExecutorTestContext) prepareMutableStateWithTriggeredNexusCompletionCallback() *workflow.MutableStateImpl {
	mutableState := s.prepareMutableStateWithReadyNexusCompletionCallback()
	wt := addWorkflowTaskScheduledEvent(mutableState)
	taskQueueName := "irrelevant"
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.NewString())
	wt.StartedEventID = event.GetEventId()
	_, err := mutableState.AddWorkflowTaskCompletedEvent(wt, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity: "some random identity",
	}, defaultWorkflowTaskCompletionLimits)
	require.NoError(s.t, err)
	_, err = mutableState.AddCompletedWorkflowEvent(mutableState.GetNextEventID(), &commandpb.CompleteWorkflowExecutionCommandAttributes{}, "")
	require.NoError(s.t, err)

	return mutableState
}

func TestGetCurrentWorkflowExecutionContext(t *testing.T) {
	namespaceID := tests.NamespaceID
	workflowID := tests.WorkflowID

	testCases := []struct {
		name              string
		currentRunRunning bool
		currentRunChanged bool
	}{
		{
			name:              "current run running",
			currentRunRunning: true,
			currentRunChanged: false,
		},
		{
			name:              "current run closed, no new run",
			currentRunRunning: false,
			currentRunChanged: false,
		},
		{
			name:              "current run closed, with new run",
			currentRunRunning: false,
			currentRunChanged: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			controller := gomock.NewController(t)

			currentRunID := uuid.NewString()

			mockShard := shard.NewTestContext(
				controller,
				&persistencespb.ShardInfo{
					ShardId: 1,
					RangeId: 1,
				},
				tests.NewDynamicConfig(),
			)

			mockMutableState := workflow.NewMockMutableState(controller)
			mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(tc.currentRunRunning).Times(1)

			mockWorkflowContext := workflow.NewMockContext(controller)
			mockWorkflowContext.EXPECT().LoadMutableState(gomock.Any(), mockShard).Return(mockMutableState, nil).Times(1)
			mockWorkflowContext.EXPECT().GetWorkflowKey().Return(definition.NewWorkflowKey(namespaceID.String(), workflowID, currentRunID)).AnyTimes()

			mockWorkflowCache := cache.NewMockCache(controller)
			mockWorkflowCache.EXPECT().GetOrCreateCurrentWorkflowExecution(
				gomock.Any(),
				mockShard,
				namespaceID,
				workflowID,
				workflow.LockPriorityLow,
			).Return(cache.NoopReleaseFn, nil).AnyTimes()
			mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
				gomock.Any(),
				mockShard,
				namespaceID,
				&commonpb.WorkflowExecution{
					WorkflowId: workflowID,
					RunId:      currentRunID,
				},
				workflow.LockPriorityLow,
			).Return(mockWorkflowContext, cache.NoopReleaseFn, nil).Times(1)

			mockExecutionManager := mockShard.Resource.ExecutionMgr
			mockExecutionManager.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
				ShardID:     mockShard.GetShardID(),
				NamespaceID: namespaceID.String(),
				WorkflowID:  workflowID,
			}).Return(&persistence.GetCurrentExecutionResponse{
				RunID: currentRunID,
			}, nil).Times(1)

			if !tc.currentRunRunning {
				if tc.currentRunChanged {
					currentRunID = uuid.NewString()
				}

				mockExecutionManager.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
					ShardID:     mockShard.GetShardID(),
					NamespaceID: namespaceID.String(),
					WorkflowID:  workflowID,
				}).Return(&persistence.GetCurrentExecutionResponse{
					RunID: currentRunID,
				}, nil).Times(1)
			}

			workflowContext, release, err := getCurrentWorkflowExecutionContext(
				context.Background(),
				mockShard,
				mockWorkflowCache,
				namespaceID.String(),
				workflowID,
				workflow.LockPriorityLow,
			)
			if tc.currentRunChanged {
				require.Error(t, err)
				require.Nil(t, workflowContext)
				require.Nil(t, release)
			} else {
				require.NoError(t, err)
				require.NotNil(t, workflowContext)
				require.Equal(t, currentRunID, workflowContext.GetWorkflowKey().RunID)
				release(nil)
			}
		})
	}
}
