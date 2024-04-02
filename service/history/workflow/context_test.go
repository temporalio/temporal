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
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
)

type (
	contextSuite struct {
		suite.Suite
		*require.Assertions

		workflowContext *ContextImpl
	}
)

func TestContextSuite(t *testing.T) {
	suite.Run(t, new(contextSuite))
}

func (s *contextSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	configs := tests.NewDynamicConfig()

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
