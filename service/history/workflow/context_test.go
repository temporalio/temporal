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

package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	contextSuite struct {
		suite.Suite
		*require.Assertions

		mockShard *shard.ContextTest

		workflowContext *ContextImpl
	}
)

func TestContextSuite(t *testing.T) {
	suite.Run(t, new(contextSuite))
}

func (s *contextSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	configs := tests.NewDynamicConfig()

	controller := gomock.NewController(s.T())
	s.mockShard = shard.NewTestContext(
		controller,
		&persistencespb.ShardInfo{ShardId: 1},
		configs,
	)
	mockEngine := shard.NewMockEngine(controller)
	mockEngine.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	mockEngine.EXPECT().NotifyNewHistoryEvent(gomock.Any()).AnyTimes()
	s.mockShard.SetEngineForTesting(mockEngine)
	s.NoError(RegisterStateMachine(s.mockShard.StateMachineRegistry()))
	mockClusterMetadata := s.mockShard.Resource.ClusterMetadata
	mockNamespaceCache := s.mockShard.Resource.NamespaceCache
	mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false).AnyTimes()
	mockClusterMetadata.EXPECT().GetClusterID().Return(cluster.TestCurrentClusterInitialFailoverVersion).AnyTimes()
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestSingleDCClusterInfo).AnyTimes()
	mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(false, common.EmptyVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	mockNamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.LocalNamespaceEntry, nil).AnyTimes()
	mockNamespaceCache.EXPECT().GetNamespace(tests.Namespace).Return(tests.LocalNamespaceEntry, nil).AnyTimes()

	s.workflowContext = NewContext(
		configs,
		tests.WorkflowKey,
		log.NewNoopLogger(),
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
	)
}

func (s *contextSuite) TestMergeReplicationTasks_NoNewRun() {
	currentWorkflowMutation := &persistence.WorkflowMutation{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
	}

	err := s.workflowContext.mergeUpdateWithNewReplicationTasks(
		currentWorkflowMutation,
		nil, // no new run
	)
	s.NoError(err)
	s.Empty(currentWorkflowMutation.Tasks)
}

func (s *contextSuite) TestMergeReplicationTasks_LocalNamespace() {
	currentWorkflowMutation := &persistence.WorkflowMutation{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		},
		// no replication tasks
	}
	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		},
	}

	err := s.workflowContext.mergeUpdateWithNewReplicationTasks(
		currentWorkflowMutation,
		newWorkflowSnapshot,
	)
	s.NoError(err)
	s.Empty(currentWorkflowMutation.Tasks) // verify no change to tasks
	s.Empty(newWorkflowSnapshot.Tasks)     // verify no change to tasks
}

func (s *contextSuite) TestMergeReplicationTasks_SingleReplicationTask() {
	currentWorkflowMutation := &persistence.WorkflowMutation{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		},
		Tasks: map[tasks.Category][]tasks.Task{
			tasks.CategoryReplication: {
				&tasks.HistoryReplicationTask{
					WorkflowKey:         tests.WorkflowKey,
					VisibilityTimestamp: time.Now(),
					FirstEventID:        5,
					NextEventID:         10,
					Version:             tests.Version,
				},
				&tasks.HistoryReplicationTask{
					WorkflowKey:         tests.WorkflowKey,
					VisibilityTimestamp: time.Now(),
					FirstEventID:        10,
					NextEventID:         20,
					Version:             tests.Version,
				},
			},
		},
	}

	newRunID := uuid.New()
	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		},
		Tasks: map[tasks.Category][]tasks.Task{
			tasks.CategoryReplication: {
				&tasks.HistoryReplicationTask{
					WorkflowKey: definition.NewWorkflowKey(
						string(tests.NamespaceID),
						tests.WorkflowID,
						newRunID,
					),
					VisibilityTimestamp: time.Now(),
					FirstEventID:        1,
					NextEventID:         3,
					Version:             tests.Version,
				},
			},
		},
	}

	err := s.workflowContext.mergeUpdateWithNewReplicationTasks(
		currentWorkflowMutation,
		newWorkflowSnapshot,
	)
	s.NoError(err)
	s.Len(currentWorkflowMutation.Tasks[tasks.CategoryReplication], 2)
	s.Empty(newWorkflowSnapshot.Tasks[tasks.CategoryReplication]) // verify no change to tasks

	mergedReplicationTasks := currentWorkflowMutation.Tasks[tasks.CategoryReplication]
	s.Empty(mergedReplicationTasks[0].(*tasks.HistoryReplicationTask).NewRunID)
	s.Equal(newRunID, mergedReplicationTasks[1].(*tasks.HistoryReplicationTask).NewRunID)
}

func (s *contextSuite) TestMergeReplicationTasks_MultipleReplicationTasks() {
	// The case can happen when importing a workflow:
	// current workflow will be terminated and imported workflow can contain multiple replication tasks
	// This case is not supported right now
	// NOTE: ^ should be the case and both current and new runs should have replication tasks. However, the
	// actual implementation in WorkflowImporter will close the transaction of the new run with Passive
	// policy resulting in 0 replication tasks.
	// However the implementation of mergeUpdateWithNewReplicationTasks should still handle this case and not error out.

	currentWorkflowMutation := &persistence.WorkflowMutation{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		},
		Tasks: map[tasks.Category][]tasks.Task{
			tasks.CategoryReplication: {
				&tasks.HistoryReplicationTask{
					WorkflowKey:         tests.WorkflowKey,
					VisibilityTimestamp: time.Now(),
					FirstEventID:        9,
					NextEventID:         10,
					Version:             tests.Version,
				},
			},
		},
	}

	newRunID := uuid.New()
	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		},
		Tasks: map[tasks.Category][]tasks.Task{
			tasks.CategoryReplication: {
				&tasks.HistoryReplicationTask{
					WorkflowKey: definition.NewWorkflowKey(
						string(tests.NamespaceID),
						tests.WorkflowID,
						newRunID,
					),
					VisibilityTimestamp: time.Now(),
					FirstEventID:        1,
					NextEventID:         3,
					Version:             tests.Version,
				},
				&tasks.HistoryReplicationTask{
					WorkflowKey: definition.NewWorkflowKey(
						string(tests.NamespaceID),
						tests.WorkflowID,
						newRunID,
					),
					VisibilityTimestamp: time.Now(),
					FirstEventID:        3,
					NextEventID:         6,
					Version:             tests.Version,
				},
				&tasks.HistoryReplicationTask{
					WorkflowKey: definition.NewWorkflowKey(
						string(tests.NamespaceID),
						tests.WorkflowID,
						newRunID,
					),
					VisibilityTimestamp: time.Now(),
					FirstEventID:        6,
					NextEventID:         10,
					Version:             tests.Version,
				},
			},
		},
	}

	err := s.workflowContext.mergeUpdateWithNewReplicationTasks(
		currentWorkflowMutation,
		newWorkflowSnapshot,
	)
	s.NoError(err)
	s.Len(currentWorkflowMutation.Tasks[tasks.CategoryReplication], 1) // verify no change to tasks
	s.Len(newWorkflowSnapshot.Tasks[tasks.CategoryReplication], 3)     // verify no change to tasks
}

func (s *contextSuite) TestMergeReplicationTasks_CurrentRunRunning() {
	// The case can happen when suppressing a current running workflow to be zombie
	// and creating a new workflow at the same time

	currentWorkflowMutation := &persistence.WorkflowMutation{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		},
		Tasks: map[tasks.Category][]tasks.Task{},
	}

	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		},
		Tasks: map[tasks.Category][]tasks.Task{},
	}

	err := s.workflowContext.mergeUpdateWithNewReplicationTasks(
		currentWorkflowMutation,
		newWorkflowSnapshot,
	)
	s.NoError(err)
	s.Empty(currentWorkflowMutation.Tasks) // verify no change to tasks
	s.Empty(newWorkflowSnapshot.Tasks)     // verify no change to tasks
}

func (s *contextSuite) TestMergeReplicationTasks_OnlyCurrentRunHasReplicationTasks() {
	// The case can happen when importing a workflow (via replication task)
	// current workflow may be terminated and the imported workflow since it's received via replication task
	// will not generate replication tasks again.

	currentWorkflowMutation := &persistence.WorkflowMutation{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		},
		Tasks: map[tasks.Category][]tasks.Task{
			tasks.CategoryReplication: {
				&tasks.HistoryReplicationTask{
					WorkflowKey:         tests.WorkflowKey,
					VisibilityTimestamp: time.Now(),
					FirstEventID:        5,
					NextEventID:         6,
					Version:             tests.Version,
				},
			},
		},
	}

	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		},
		Tasks: map[tasks.Category][]tasks.Task{},
	}

	err := s.workflowContext.mergeUpdateWithNewReplicationTasks(
		currentWorkflowMutation,
		newWorkflowSnapshot,
	)
	s.NoError(err)
	s.Len(currentWorkflowMutation.Tasks[tasks.CategoryReplication], 1) // verify no change to tasks
	s.Empty(newWorkflowSnapshot.Tasks)                                 // verify no change to tasks
}

func (s *contextSuite) TestMergeReplicationTasks_OnlyNewRunHasReplicationTasks() {
	// TODO: check if this case can happen or not.

	currentWorkflowMutation := &persistence.WorkflowMutation{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		},
		Tasks: map[tasks.Category][]tasks.Task{},
	}

	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		},
		Tasks: map[tasks.Category][]tasks.Task{
			tasks.CategoryReplication: {
				&tasks.HistoryReplicationTask{
					WorkflowKey:         tests.WorkflowKey,
					VisibilityTimestamp: time.Now(),
					FirstEventID:        5,
					NextEventID:         6,
					Version:             tests.Version,
				},
			},
		},
	}

	err := s.workflowContext.mergeUpdateWithNewReplicationTasks(
		currentWorkflowMutation,
		newWorkflowSnapshot,
	)
	s.NoError(err)
	s.Empty(currentWorkflowMutation.Tasks)                         // verify no change to tasks
	s.Len(newWorkflowSnapshot.Tasks[tasks.CategoryReplication], 1) // verify no change to tasks
}

func (s *contextSuite) TestRefreshTask() {
	now := time.Now()

	baseMutableState := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId:        tests.NamespaceID.String(),
			WorkflowId:         tests.WorkflowID,
			WorkflowRunTimeout: timestamp.DurationFromSeconds(200),
			StartTime:          timestamppb.New(now),
			ExecutionTime:      timestamppb.New(now),
			VersionHistories: &historyspb.VersionHistories{
				Histories: []*historyspb.VersionHistory{
					{
						BranchToken: []byte("token#1"),
						Items: []*historyspb.VersionHistoryItem{
							{EventId: 1, Version: common.EmptyVersion},
						},
					},
				},
			},
			TransitionHistory: []*persistencespb.VersionedTransition{
				{
					NamespaceFailoverVersion: common.EmptyVersion,
					TransitionCount:          1,
				},
			},
			ExecutionStats: &persistencespb.ExecutionStats{
				HistorySize: 128,
			},
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		NextEventId: 2,
	}

	testCases := []struct {
		name                  string
		persistedMutableState func() *persistencespb.WorkflowMutableState
		setupMock             func(mockShard *shard.ContextTest)
	}{
		{
			name: "open workflow",
			persistedMutableState: func() *persistencespb.WorkflowMutableState {
				return common.CloneProto(baseMutableState)
			},
			setupMock: func(mockShard *shard.ContextTest) {
				mockShard.MockEventsCache.EXPECT().GetEvent(
					gomock.Any(),
					mockShard.GetShardID(),
					gomock.Any(),
					common.FirstEventID,
					gomock.Any(),
				).Return(&historypb.HistoryEvent{
					EventId:    1,
					EventTime:  timestamppb.New(now),
					EventType:  enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
					Version:    common.EmptyVersion,
					Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{},
				}, nil).Times(2)
				mockShard.Resource.ExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
						s.Equal(persistence.UpdateWorkflowModeUpdateCurrent, request.Mode)
						s.NotEmpty(request.UpdateWorkflowMutation.Tasks)
						s.Empty(request.UpdateWorkflowEvents)
						return &persistence.UpdateWorkflowExecutionResponse{
							UpdateMutableStateStats: persistence.MutableStateStatistics{
								HistoryStatistics: &persistence.HistoryStatistics{},
							},
						}, nil
					}).Times(1)
			},
		},
		{
			name: "completed workflow",
			persistedMutableState: func() *persistencespb.WorkflowMutableState {
				base := common.CloneProto(baseMutableState)
				base.ExecutionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
				base.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
				base.NextEventId = 3
				base.ExecutionInfo.VersionHistories.Histories[0].Items[0].EventId = 2
				base.ExecutionInfo.TransitionHistory[0].TransitionCount = 2
				base.ExecutionInfo.CloseTime = timestamppb.New(now.Add(time.Second))
				return base
			},
			setupMock: func(mockShard *shard.ContextTest) {
				mockShard.Resource.ExecutionMgr.EXPECT().SetWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, request *persistence.SetWorkflowExecutionRequest) (*persistence.SetWorkflowExecutionResponse, error) {
						s.NotEmpty(request.SetWorkflowSnapshot.Tasks)
						return &persistence.SetWorkflowExecutionResponse{}, nil
					}).Times(1)
			},
		},
		{
			name: "zombie workflow",
			persistedMutableState: func() *persistencespb.WorkflowMutableState {
				base := common.CloneProto(baseMutableState)
				base.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE
				return base
			},
			setupMock: func(mockShard *shard.ContextTest) {
				mockShard.MockEventsCache.EXPECT().GetEvent(
					gomock.Any(),
					mockShard.GetShardID(),
					gomock.Any(),
					common.FirstEventID,
					gomock.Any(),
				).Return(&historypb.HistoryEvent{
					EventId:    1,
					EventTime:  timestamppb.New(now),
					EventType:  enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
					Version:    common.EmptyVersion,
					Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{},
				}, nil).Times(2)
				mockShard.Resource.ExecutionMgr.EXPECT().SetWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, request *persistence.SetWorkflowExecutionRequest) (*persistence.SetWorkflowExecutionResponse, error) {
						s.NotEmpty(request.SetWorkflowSnapshot.Tasks)
						return &persistence.SetWorkflowExecutionResponse{}, nil
					}).Times(1)
			},
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			var err error
			s.workflowContext.MutableState, err = NewMutableStateFromDB(
				s.mockShard,
				s.mockShard.MockEventsCache,
				s.mockShard.GetLogger(),
				tests.LocalNamespaceEntry,
				tc.persistedMutableState(),
				1,
			)
			s.NoError(err)

			tc.setupMock(s.mockShard)

			err = s.workflowContext.RefreshTasks(context.Background(), s.mockShard)
			s.NoError(err)
		})
	}
}
