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
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/deletemanager"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

type (
	timerQueueTaskExecutorBaseSuite struct {
		suite.Suite
		*require.Assertions

		controller        *gomock.Controller
		mockDeleteManager *deletemanager.MockDeleteManager
		mockCache         *wcache.MockCache

		testShardContext           *shard.ContextTest
		timerQueueTaskExecutorBase *timerQueueTaskExecutorBase
	}
)

func TestTimerQueueTaskExecutorBaseSuite(t *testing.T) {
	s := new(timerQueueTaskExecutorBaseSuite)
	suite.Run(t, s)
}

func (s *timerQueueTaskExecutorBaseSuite) SetupSuite() {
}

func (s *timerQueueTaskExecutorBaseSuite) TearDownSuite() {
}

func (s *timerQueueTaskExecutorBaseSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockDeleteManager = deletemanager.NewMockDeleteManager(s.controller)
	s.mockCache = wcache.NewMockCache(s.controller)

	config := tests.NewDynamicConfig()
	s.testShardContext = shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
		},
		config,
	)
	s.testShardContext.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.testShardContext.Resource.ClusterMetadata.EXPECT().GetClusterID().Return(cluster.TestCurrentClusterInitialFailoverVersion).AnyTimes()

	s.timerQueueTaskExecutorBase = newTimerQueueTaskExecutorBase(
		s.testShardContext,
		s.mockCache,
		s.mockDeleteManager,
		s.testShardContext.Resource.MatchingClient,
		s.testShardContext.GetLogger(),
		metrics.NoopMetricsHandler,
		config,
	)
}

func (s *timerQueueTaskExecutorBaseSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *timerQueueTaskExecutorBaseSuite) Test_executeDeleteHistoryEventTask_NoErr() {
	task := &tasks.DeleteHistoryEventTask{
		WorkflowKey: definition.NewWorkflowKey(
			tests.NamespaceID.String(),
			tests.WorkflowID,
			tests.RunID,
		),
		Version:             123,
		TaskID:              12345,
		VisibilityTimestamp: time.Now().UTC(),
	}
	we := &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}

	mockWeCtx := workflow.NewMockContext(s.controller)
	mockMutableState := workflow.NewMockMutableState(s.controller)

	s.mockCache.EXPECT().GetOrCreateWorkflowExecution(gomock.Any(), s.testShardContext, tests.NamespaceID, we, workflow.LockPriorityLow).Return(mockWeCtx, wcache.NoopReleaseFn, nil)

	mockWeCtx.EXPECT().LoadMutableState(gomock.Any(), s.testShardContext).Return(mockMutableState, nil)
	mockMutableState.EXPECT().GetCloseVersion().Return(int64(1), nil)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{})
	mockMutableState.EXPECT().GetNextEventID().Return(int64(2))
	mockMutableState.EXPECT().GetNamespaceEntry().Return(tests.LocalNamespaceEntry)
	s.testShardContext.Resource.ClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false)
	mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{State: enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED})

	stage := tasks.DeleteWorkflowExecutionStageNone
	s.mockDeleteManager.EXPECT().DeleteWorkflowExecutionByRetention(
		gomock.Any(),
		tests.NamespaceID,
		we,
		mockWeCtx,
		mockMutableState,
		&stage,
	).Return(nil)

	err := s.timerQueueTaskExecutorBase.executeDeleteHistoryEventTask(
		context.Background(),
		task)
	s.NoError(err)
}

func (s *timerQueueTaskExecutorBaseSuite) TestArchiveHistory_DeleteFailed() {
	task := &tasks.DeleteHistoryEventTask{
		WorkflowKey: definition.NewWorkflowKey(
			tests.NamespaceID.String(),
			tests.WorkflowID,
			tests.RunID,
		),
		Version:             123,
		TaskID:              12345,
		VisibilityTimestamp: time.Now().UTC(),
	}
	we := &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}

	mockWeCtx := workflow.NewMockContext(s.controller)
	mockMutableState := workflow.NewMockMutableState(s.controller)

	s.mockCache.EXPECT().GetOrCreateWorkflowExecution(gomock.Any(), s.testShardContext, tests.NamespaceID, we, workflow.LockPriorityLow).Return(mockWeCtx, wcache.NoopReleaseFn, nil)

	mockWeCtx.EXPECT().LoadMutableState(gomock.Any(), s.testShardContext).Return(mockMutableState, nil)
	mockMutableState.EXPECT().GetCloseVersion().Return(int64(1), nil)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{})
	mockMutableState.EXPECT().GetNextEventID().Return(int64(2))
	mockMutableState.EXPECT().GetNamespaceEntry().Return(tests.LocalNamespaceEntry)
	s.testShardContext.Resource.ClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false)
	mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{State: enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED})

	stage := tasks.DeleteWorkflowExecutionStageNone
	s.mockDeleteManager.EXPECT().DeleteWorkflowExecutionByRetention(
		gomock.Any(),
		tests.NamespaceID,
		we,
		mockWeCtx,
		mockMutableState,
		&stage,
	).Return(serviceerror.NewInternal("test error"))

	err := s.timerQueueTaskExecutorBase.executeDeleteHistoryEventTask(
		context.Background(),
		task)
	s.Error(err)
}

func (s *timerQueueTaskExecutorBaseSuite) TestIsValidExecutionTimeoutTask() {

	testCases := []struct {
		name            string
		firstRunIDMatch bool
		workflowRunning bool
		isValid         bool
	}{
		{
			name:            "different chain",
			firstRunIDMatch: false,
			isValid:         false,
		},
		{
			name:            "same chain, workflow running",
			firstRunIDMatch: true,
			workflowRunning: true,
			isValid:         true,
		},
		{
			name:            "same chain, workflow completed",
			firstRunIDMatch: true,
			workflowRunning: false,
			isValid:         false,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			timerTask := &tasks.WorkflowExecutionTimeoutTask{
				NamespaceID:         tests.NamespaceID.String(),
				WorkflowID:          tests.WorkflowID,
				FirstRunID:          uuid.New(),
				VisibilityTimestamp: s.testShardContext.GetTimeSource().Now(),
				TaskID:              100,
			}
			mutableStateFirstRunID := timerTask.FirstRunID
			if !tc.firstRunIDMatch {
				mutableStateFirstRunID = uuid.New()
			}

			mockMutableState := workflow.NewMockMutableState(s.controller)
			mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
				FirstExecutionRunId: mutableStateFirstRunID,
			}).AnyTimes()
			mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(tc.workflowRunning).AnyTimes()

			isValid := s.timerQueueTaskExecutorBase.isValidExecutionTimeoutTask(mockMutableState, timerTask)
			s.Equal(tc.isValid, isValid)
		})
	}
}

func (s *timerQueueTaskExecutorBaseSuite) TestExecuteStateMachineTimerTask_PerformsStalenessCheck() {
	we := &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}

	ms := workflow.NewMockMutableState(s.controller)
	ms.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 2, MaxTransitionCount: 1},
		},
	}).AnyTimes()

	mockWeCtx := workflow.NewMockContext(s.controller)
	s.mockCache.EXPECT().GetOrCreateWorkflowExecution(gomock.Any(), s.testShardContext, tests.NamespaceID, we, workflow.LockPriorityLow).Return(mockWeCtx, wcache.NoopReleaseFn, nil)
	mockWeCtx.EXPECT().LoadMutableState(gomock.Any(), s.testShardContext).Return(ms, nil)

	task := &tasks.StateMachineTimerTask{
		WorkflowKey:                 tests.WorkflowKey,
		Version:                     1,
		MutableStateTransitionCount: 1,
	}
	err := s.timerQueueTaskExecutorBase.executeStateMachineTimerTask(context.Background(), task, func(node *hsm.Node, task hsm.Task) error {
		return nil
	})
	s.ErrorIs(err, consts.ErrStaleReference)
}

func (s *timerQueueTaskExecutorBaseSuite) TestExecuteStateMachineTimerTask_ExecutesAllAvailableTimers() {
	reg := hsm.NewRegistry()
	s.NoError(workflow.RegisterStateMachine(reg))
	s.NoError(callbacks.RegisterTaskSerializers(reg))
	s.NoError(callbacks.RegisterStateMachine(reg))
	s.testShardContext.SetStateMachineRegistry(reg)

	we := &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}

	ms := workflow.NewMockMutableState(s.controller)
	info := &persistencespb.WorkflowExecutionInfo{
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 2, MaxTransitionCount: 1},
		},
	}
	root, err := hsm.NewRoot(reg, workflow.StateMachineType.ID, ms, make(map[int32]*persistencespb.StateMachineMap), ms)
	s.NoError(err)
	ms.EXPECT().GetExecutionInfo().Return(info).AnyTimes()
	ms.EXPECT().GetWorkflowKey().Return(tests.WorkflowKey).AnyTimes()
	ms.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{Status: enums.WORKFLOW_EXECUTION_STATUS_RUNNING}).AnyTimes()
	ms.EXPECT().HSM().Return(root).AnyTimes()
	ms.EXPECT().AddTasks(gomock.Any())

	_, err = callbacks.MachineCollection(root).Add("callback", callbacks.NewCallback(timestamppb.Now(), callbacks.NewWorkflowClosedTrigger(), nil))
	s.NoError(err)

	// Track some tasks.

	// Invalid reference, should be dropped.
	invalidTask := &persistencespb.StateMachineTaskInfo{
		Ref: &persistencespb.StateMachineRef{
			MutableStateNamespaceFailoverVersion: 1,
			MutableStateTransitionCount:          1,
		},
		Type: callbacks.TaskTypeBackoff.ID,
	}
	validTask := &persistencespb.StateMachineTaskInfo{
		Ref: &persistencespb.StateMachineRef{
			Path: []*persistencespb.StateMachineKey{
				{Type: callbacks.StateMachineType.ID, Id: "callback"},
			},
			MutableStateNamespaceFailoverVersion: 2,
			MutableStateTransitionCount:          1,
		},
		Type: callbacks.TaskTypeBackoff.ID,
	}

	// Past deadline, should get executed.
	workflow.TrackStateMachineTimer(ms, s.testShardContext.GetTimeSource().Now().Add(-time.Hour), invalidTask)
	workflow.TrackStateMachineTimer(ms, s.testShardContext.GetTimeSource().Now().Add(-time.Hour), validTask)
	workflow.TrackStateMachineTimer(ms, s.testShardContext.GetTimeSource().Now().Add(-time.Minute), validTask)
	// Future deadline, new task should be scheduled.
	workflow.TrackStateMachineTimer(ms, s.testShardContext.GetTimeSource().Now().Add(time.Hour), validTask)

	wfCtx := workflow.NewMockContext(s.controller)
	s.mockCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(), s.testShardContext, tests.NamespaceID, we, workflow.LockPriorityLow,
	).Return(wfCtx, wcache.NoopReleaseFn, nil)
	wfCtx.EXPECT().LoadMutableState(gomock.Any(), s.testShardContext).Return(ms, nil)
	wfCtx.EXPECT().UpdateWorkflowExecutionAsActive(gomock.Any(), gomock.Any())

	task := &tasks.StateMachineTimerTask{
		WorkflowKey:                 tests.WorkflowKey,
		Version:                     2,
		MutableStateTransitionCount: 1,
	}

	numInvocations := 0
	err = s.timerQueueTaskExecutorBase.executeStateMachineTimerTask(context.Background(), task, func(node *hsm.Node, task hsm.Task) error {
		numInvocations++
		return nil
	})
	s.NoError(err)
	s.Equal(2, numInvocations) // two valid tasks within the deadline.
	s.Equal(1, len(info.StateMachineTimers))
	s.True(info.StateMachineTimers[0].Scheduled)
}
