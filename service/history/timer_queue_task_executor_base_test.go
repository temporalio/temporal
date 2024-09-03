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
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/deletemanager"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"google.golang.org/protobuf/types/known/timestamppb"
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
		true, // isActive (irelevant for test)
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

	s.mockCache.EXPECT().GetOrCreateWorkflowExecution(gomock.Any(), s.testShardContext, tests.NamespaceID, we, locks.PriorityLow).Return(mockWeCtx, wcache.NoopReleaseFn, nil)

	mockWeCtx.EXPECT().LoadMutableState(gomock.Any(), s.testShardContext).Return(mockMutableState, nil)
	mockMutableState.EXPECT().GetWorkflowKey().Return(task.WorkflowKey).AnyTimes()
	mockMutableState.EXPECT().GetCloseVersion().Return(int64(1), nil)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{}).AnyTimes()
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

	s.mockCache.EXPECT().GetOrCreateWorkflowExecution(gomock.Any(), s.testShardContext, tests.NamespaceID, we, locks.PriorityLow).Return(mockWeCtx, wcache.NoopReleaseFn, nil)

	mockWeCtx.EXPECT().LoadMutableState(gomock.Any(), s.testShardContext).Return(mockMutableState, nil)
	mockMutableState.EXPECT().GetWorkflowKey().Return(task.WorkflowKey).AnyTimes()
	mockMutableState.EXPECT().GetCloseVersion().Return(int64(1), nil)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{}).AnyTimes()
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

			isValid := s.timerQueueTaskExecutorBase.isValidWorkflowExecutionTimeoutTask(mockMutableState, timerTask)
			s.Equal(tc.isValid, isValid)
		})
	}
}

func (s *timerQueueTaskExecutorBaseSuite) TestIsValidExecutionTimeouts() {

	timeNow := s.testShardContext.GetTimeSource().Now()
	timeBefore := timeNow.Add(time.Duration(-15) * time.Second)
	timeAfter := timeNow.Add(time.Duration(15) * time.Second)

	timerTask := &tasks.WorkflowExecutionTimeoutTask{
		NamespaceID: tests.NamespaceID.String(),
		WorkflowID:  tests.WorkflowID,
		FirstRunID:  uuid.New(),
		TaskID:      100,
	}
	mockMutableState := workflow.NewMockMutableState(s.controller)
	mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()

	testCases := []struct {
		name           string
		expirationTime time.Time
		isValid        bool
	}{
		{
			name:           "expiration set before now",
			expirationTime: timeBefore,
			isValid:        true,
		},
		{
			name:           "expiration set after now",
			expirationTime: timeAfter,
			isValid:        false,
		},
	}

	for _, tc := range testCases {
		mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
			FirstExecutionRunId:             timerTask.FirstRunID,
			WorkflowExecutionExpirationTime: timestamppb.New(tc.expirationTime),
		})
		isValid := s.timerQueueTaskExecutorBase.isValidWorkflowExecutionTimeoutTask(mockMutableState, timerTask)
		s.Equal(tc.isValid, isValid)
	}
}
