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
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
)

type (
	timerQueueTaskExecutorBaseSuite struct {
		suite.Suite
		*require.Assertions

		controller        *gomock.Controller
		mockDeleteManager *workflow.MockDeleteManager
		mockCache         *workflow.MockCache

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
	s.mockDeleteManager = workflow.NewMockDeleteManager(s.controller)
	s.mockCache = workflow.NewMockCache(s.controller)

	config := tests.NewDynamicConfig()
	s.testShardContext = shard.NewTestContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId: 0,
				RangeId: 1,
			},
		},
		config,
	)
	s.testShardContext.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	s.timerQueueTaskExecutorBase = newTimerQueueTaskExecutorBase(
		s.testShardContext,
		s.mockCache,
		s.mockDeleteManager,
		nil,
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
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}

	mockWeCtx := workflow.NewMockContext(s.controller)
	mockMutableState := workflow.NewMockMutableState(s.controller)

	s.mockCache.EXPECT().GetOrCreateWorkflowExecution(gomock.Any(), tests.NamespaceID, we, workflow.CallerTypeTask).Return(mockWeCtx, workflow.NoopReleaseFn, nil)

	mockWeCtx.EXPECT().LoadWorkflowExecution(gomock.Any()).Return(mockMutableState, nil)
	mockMutableState.EXPECT().GetLastWriteVersion().Return(int64(1), nil)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{})
	mockMutableState.EXPECT().GetNextEventID().Return(int64(2))
	mockMutableState.EXPECT().GetNamespaceEntry().Return(tests.LocalNamespaceEntry)
	s.testShardContext.Resource.ClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false)
	mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{State: enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED})

	s.mockDeleteManager.EXPECT().DeleteWorkflowExecutionByRetention(
		gomock.Any(),
		tests.NamespaceID,
		we,
		mockWeCtx,
		mockMutableState,
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
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}

	mockWeCtx := workflow.NewMockContext(s.controller)
	mockMutableState := workflow.NewMockMutableState(s.controller)

	s.mockCache.EXPECT().GetOrCreateWorkflowExecution(gomock.Any(), tests.NamespaceID, we, workflow.CallerTypeTask).Return(mockWeCtx, workflow.NoopReleaseFn, nil)

	mockWeCtx.EXPECT().LoadWorkflowExecution(gomock.Any()).Return(mockMutableState, nil)
	mockMutableState.EXPECT().GetLastWriteVersion().Return(int64(1), nil)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{})
	mockMutableState.EXPECT().GetNextEventID().Return(int64(2))
	mockMutableState.EXPECT().GetNamespaceEntry().Return(tests.LocalNamespaceEntry)
	s.testShardContext.Resource.ClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false)
	mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{State: enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED})

	s.mockDeleteManager.EXPECT().DeleteWorkflowExecutionByRetention(
		gomock.Any(),
		tests.NamespaceID,
		we,
		mockWeCtx,
		mockMutableState,
	).Return(serviceerror.NewInternal("test error"))

	err := s.timerQueueTaskExecutorBase.executeDeleteHistoryEventTask(
		context.Background(),
		task)
	s.Error(err)
}
