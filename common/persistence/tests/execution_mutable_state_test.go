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

package tests

import (
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
)

type (
	executionMutableStateSuite struct {
		suite.Suite
		*require.Assertions

		shardID     int32
		rangeID     int64
		namespaceID string
		workflowID  string
		runID       string

		shardManager     p.ShardManager
		executionManager p.ExecutionManager
		logger           log.Logger
	}
)

func newExecutionMutableStateSuite(
	t *testing.T,
	shardStore p.ShardStore,
	executionStore p.ExecutionStore,
	logger log.Logger,
) *executionMutableStateSuite {
	return &executionMutableStateSuite{
		Assertions:   require.New(t),
		shardManager: p.NewShardManager(shardStore),
		executionManager: p.NewExecutionManager(
			executionStore,
			logger,
			dynamicconfig.GetIntPropertyFn(4*1024*1024),
		),
		logger: logger,
	}
}

func (s *executionMutableStateSuite) SetupSuite() {
	rand.Seed(time.Now().UnixNano())
}

func (s *executionMutableStateSuite) TearDownSuite() {

}

func (s *executionMutableStateSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.shardID = rand.Int31()
	resp, err := s.shardManager.GetOrCreateShard(&p.GetOrCreateShardRequest{
		ShardID:         s.shardID,
		CreateIfMissing: true,
		InitialShardInfo: &persistencespb.ShardInfo{
			ShardId: s.shardID,
			RangeId: 1,
		},
	})
	s.NoError(err)
	s.rangeID = resp.ShardInfo.RangeId

	s.namespaceID = uuid.New().String()
	s.workflowID = uuid.New().String()
	s.runID = uuid.New().String()
}

func (s *executionMutableStateSuite) TearDownTest() {

}

func (s *executionMutableStateSuite) TestCreate_BrandNew() {
	_ = randomSnapshot(
		s.namespaceID,
		s.workflowID,
		s.runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)
}

func (s *executionMutableStateSuite) TestCreate_BrandNew_CurrentConflict() {
	lastWriteVersion := rand.Int63()
	newSnapshot := s.createWorkflow(
		lastWriteVersion,
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		int64(1),
	)

	_, err := s.executionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.CreateWorkflowModeBrandNew,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *newSnapshot,
		NewWorkflowEvents:   nil,
	})
	if err, ok := err.(*p.CurrentWorkflowConditionFailedError); ok {
		err.Msg = ""
	}
	s.Equal(&p.CurrentWorkflowConditionFailedError{
		Msg:              "",
		RequestID:        newSnapshot.ExecutionState.CreateRequestId,
		RunID:            newSnapshot.ExecutionState.RunId,
		State:            newSnapshot.ExecutionState.State,
		Status:           newSnapshot.ExecutionState.Status,
		LastWriteVersion: lastWriteVersion,
	}, err)
}

func (s *executionMutableStateSuite) TestCreate_Reuse() {
	prevLastWriteVersion := rand.Int63()
	prevSnapshot := s.createWorkflow(
		prevLastWriteVersion,
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		int64(1),
	)

	newSnapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		uuid.New().String(),
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)

	_, err := s.executionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.CreateWorkflowModeWorkflowIDReuse,

		PreviousRunID:            prevSnapshot.ExecutionState.RunId,
		PreviousLastWriteVersion: prevLastWriteVersion,

		NewWorkflowSnapshot: *newSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)
}

func (s *executionMutableStateSuite) TestCreate_Reuse_CurrentConflict() {
	lastWriteVersion := rand.Int63()
	newSnapshot := s.createWorkflow(
		lastWriteVersion,
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		int64(1),
	)

	_, err := s.executionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.CreateWorkflowModeWorkflowIDReuse,

		PreviousRunID:            uuid.New().String(),
		PreviousLastWriteVersion: rand.Int63(),

		NewWorkflowSnapshot: *newSnapshot,
		NewWorkflowEvents:   nil,
	})
	if err, ok := err.(*p.CurrentWorkflowConditionFailedError); ok {
		err.Msg = ""
	}
	s.Equal(&p.CurrentWorkflowConditionFailedError{
		Msg:              "",
		RequestID:        newSnapshot.ExecutionState.CreateRequestId,
		RunID:            newSnapshot.ExecutionState.RunId,
		State:            newSnapshot.ExecutionState.State,
		Status:           newSnapshot.ExecutionState.Status,
		LastWriteVersion: lastWriteVersion,
	}, err)
}

func (s *executionMutableStateSuite) TestCreate_Zombie() {
	prevLastWriteVersion := rand.Int63()
	_ = s.createWorkflow(
		prevLastWriteVersion,
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		int64(1),
	)

	newSnapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		uuid.New().String(),
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)

	_, err := s.executionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.CreateWorkflowModeZombie,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *newSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)
}

func (s *executionMutableStateSuite) TestCreate_Conflict() {
	lastWriteVersion := rand.Int63()
	newSnapshot := s.createWorkflow(
		lastWriteVersion,
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		int64(1),
	)

	_, err := s.executionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.CreateWorkflowModeWorkflowIDReuse,

		PreviousRunID:            newSnapshot.ExecutionState.RunId,
		PreviousLastWriteVersion: lastWriteVersion,

		NewWorkflowSnapshot: *newSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.IsType(&p.WorkflowConditionFailedError{}, err)
}

func (s *executionMutableStateSuite) TestUpdate_NotZombie() {
	currentSnapshot := s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)

	currentMutation := randomMutation(
		s.namespaceID,
		s.workflowID,
		s.runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		currentSnapshot.DBRecordVersion+1,
	)
	_, err := s.executionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.UpdateWorkflowModeUpdateCurrent,

		UpdateWorkflowMutation: *currentMutation,
		UpdateWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)
}

func (s *executionMutableStateSuite) TestUpdate_NotZombie_CurrentConflict() {
	_ = s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)

	currentMutation := randomMutation(
		s.namespaceID,
		s.workflowID,
		uuid.New().String(),
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	_, err := s.executionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.UpdateWorkflowModeUpdateCurrent,

		UpdateWorkflowMutation: *currentMutation,
		UpdateWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,
	})
	s.IsType(&p.CurrentWorkflowConditionFailedError{}, err)
}

func (s *executionMutableStateSuite) TestUpdate_NotZombie_Conflict() {
	_ = s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)

	currentMutation := randomMutation(
		s.namespaceID,
		s.workflowID,
		s.runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	_, err := s.executionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.UpdateWorkflowModeUpdateCurrent,

		UpdateWorkflowMutation: *currentMutation,
		UpdateWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,
	})
	s.IsType(&p.WorkflowConditionFailedError{}, err)
}

func (s *executionMutableStateSuite) TestUpdate_NotZombie_WithNew() {
	currentSnapshot := s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)

	currentMutation := randomMutation(
		s.namespaceID,
		s.workflowID,
		s.runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		currentSnapshot.DBRecordVersion+1,
	)
	newSnapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		uuid.New().String(),
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)
	_, err := s.executionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.UpdateWorkflowModeUpdateCurrent,

		UpdateWorkflowMutation: *currentMutation,
		UpdateWorkflowEvents:   nil,

		NewWorkflowSnapshot: newSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)
}

func (s *executionMutableStateSuite) TestUpdate_Zombie() {
	_ = s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)
	runID := uuid.New().String()
	zombieSnapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)
	_, err := s.executionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.CreateWorkflowModeZombie,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *zombieSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	zombieMutation := randomMutation(
		s.namespaceID,
		s.workflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		zombieSnapshot.DBRecordVersion+1,
	)
	_, err = s.executionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.UpdateWorkflowModeBypassCurrent,

		UpdateWorkflowMutation: *zombieMutation,
		UpdateWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)
}

func (s *executionMutableStateSuite) TestUpdate_Zombie_CurrentConflict() {
	currentSnapshot := s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)

	currentMutation := randomMutation(
		s.namespaceID,
		s.workflowID,
		s.runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		currentSnapshot.DBRecordVersion+1,
	)
	_, err := s.executionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.UpdateWorkflowModeBypassCurrent,

		UpdateWorkflowMutation: *currentMutation,
		UpdateWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,
	})
	s.IsType(&p.CurrentWorkflowConditionFailedError{}, err)
}

func (s *executionMutableStateSuite) TestUpdate_Zombie_Conflict() {
	_ = s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)
	runID := uuid.New().String()
	zombieSnapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)
	_, err := s.executionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.CreateWorkflowModeZombie,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *zombieSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	zombieMutation := randomMutation(
		s.namespaceID,
		s.workflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	_, err = s.executionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.UpdateWorkflowModeBypassCurrent,

		UpdateWorkflowMutation: *zombieMutation,
		UpdateWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,
	})
	s.IsType(&p.WorkflowConditionFailedError{}, err)
}

func (s *executionMutableStateSuite) TestUpdate_Zombie_WithNew() {
	_ = s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)
	runID := uuid.New().String()
	zombieSnapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)
	_, err := s.executionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.CreateWorkflowModeZombie,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *zombieSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	zombieMutation := randomMutation(
		s.namespaceID,
		s.workflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		zombieSnapshot.DBRecordVersion+1,
	)
	newZombieSnapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		uuid.New().String(),
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)
	_, err = s.executionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.UpdateWorkflowModeBypassCurrent,

		UpdateWorkflowMutation: *zombieMutation,
		UpdateWorkflowEvents:   nil,

		NewWorkflowSnapshot: newZombieSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)
}

func (s *executionMutableStateSuite) TestConflictResolve_SuppressCurrent() {
	currentSnapshot := s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)

	runID := uuid.New().String()
	resetSnapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)
	_, err := s.executionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.CreateWorkflowModeZombie,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *resetSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	resetSnapshot = randomSnapshot(
		s.namespaceID,
		s.workflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		resetSnapshot.DBRecordVersion+1,
	)
	currentMutation := randomMutation(
		s.namespaceID,
		s.workflowID,
		s.runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		currentSnapshot.DBRecordVersion+1,
	)
	_, err = s.executionManager.ConflictResolveWorkflowExecution(&p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.ConflictResolveWorkflowModeUpdateCurrent,

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: currentMutation,
		CurrentWorkflowEvents:   nil,
	})
	s.NoError(err)
}

func (s *executionMutableStateSuite) TestConflictResolve_SuppressCurrent_CurrentConflict() {
	_ = s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)

	runID := uuid.New().String()
	resetSnapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)
	_, err := s.executionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.CreateWorkflowModeZombie,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *resetSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	resetSnapshot = randomSnapshot(
		s.namespaceID,
		s.workflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		resetSnapshot.DBRecordVersion+1,
	)
	currentMutation := randomMutation(
		s.namespaceID,
		s.workflowID,
		uuid.New().String(),
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	_, err = s.executionManager.ConflictResolveWorkflowExecution(&p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.ConflictResolveWorkflowModeUpdateCurrent,

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: currentMutation,
		CurrentWorkflowEvents:   nil,
	})
	s.IsType(&p.CurrentWorkflowConditionFailedError{}, err)
}

func (s *executionMutableStateSuite) TestConflictResolve_SuppressCurrent_Conflict_Case1() {
	_ = s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)

	runID := uuid.New().String()
	resetSnapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)
	_, err := s.executionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.CreateWorkflowModeZombie,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *resetSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	resetSnapshot = randomSnapshot(
		s.namespaceID,
		s.workflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		resetSnapshot.DBRecordVersion+1,
	)
	currentMutation := randomMutation(
		s.namespaceID,
		s.workflowID,
		s.runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	_, err = s.executionManager.ConflictResolveWorkflowExecution(&p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.ConflictResolveWorkflowModeUpdateCurrent,

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: currentMutation,
		CurrentWorkflowEvents:   nil,
	})
	s.IsType(&p.WorkflowConditionFailedError{}, err)
}

func (s *executionMutableStateSuite) TestConflictResolve_SuppressCurrent_Conflict_Case2() {
	currentSnapshot := s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)

	runID := uuid.New().String()
	resetSnapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)
	_, err := s.executionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.CreateWorkflowModeZombie,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *resetSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	resetSnapshot = randomSnapshot(
		s.namespaceID,
		s.workflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	currentMutation := randomMutation(
		s.namespaceID,
		s.workflowID,
		s.runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		currentSnapshot.DBRecordVersion+1,
	)
	_, err = s.executionManager.ConflictResolveWorkflowExecution(&p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.ConflictResolveWorkflowModeUpdateCurrent,

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: currentMutation,
		CurrentWorkflowEvents:   nil,
	})
	s.IsType(&p.WorkflowConditionFailedError{}, err)
}

func (s *executionMutableStateSuite) TestConflictResolve_SuppressCurrent_WithNew() {
	currentSnapshot := s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)

	runID := uuid.New().String()
	resetSnapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)
	_, err := s.executionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.CreateWorkflowModeZombie,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *resetSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	resetSnapshot = randomSnapshot(
		s.namespaceID,
		s.workflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		resetSnapshot.DBRecordVersion+1,
	)
	newSnapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		uuid.New().String(),
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)
	currentMutation := randomMutation(
		s.namespaceID,
		s.workflowID,
		s.runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		currentSnapshot.DBRecordVersion+1,
	)
	_, err = s.executionManager.ConflictResolveWorkflowExecution(&p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.ConflictResolveWorkflowModeUpdateCurrent,

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   nil,

		NewWorkflowSnapshot: newSnapshot,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: currentMutation,
		CurrentWorkflowEvents:   nil,
	})
	s.NoError(err)
}

func (s *executionMutableStateSuite) TestConflictResolve_ResetCurrent() {
	currentSnapshot := s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)

	currentSnapshot = randomSnapshot(
		s.namespaceID,
		s.workflowID,
		s.runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		currentSnapshot.DBRecordVersion+1,
	)
	_, err := s.executionManager.ConflictResolveWorkflowExecution(&p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.ConflictResolveWorkflowModeUpdateCurrent,

		ResetWorkflowSnapshot: *currentSnapshot,
		ResetWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: nil,
		CurrentWorkflowEvents:   nil,
	})
	s.NoError(err)
}

func (s *executionMutableStateSuite) TestConflictResolve_ResetCurrent_CurrentConflict() {
	_ = s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)
	runID := uuid.New().String()
	dbVersion := rand.Int63()
	currentSnapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		dbVersion,
	)
	_, err := s.executionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.CreateWorkflowModeZombie,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *currentSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	currentSnapshot = randomSnapshot(
		s.namespaceID,
		s.workflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		dbVersion+1,
	)
	_, err = s.executionManager.ConflictResolveWorkflowExecution(&p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.ConflictResolveWorkflowModeUpdateCurrent,

		ResetWorkflowSnapshot: *currentSnapshot,
		ResetWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: nil,
		CurrentWorkflowEvents:   nil,
	})
	s.IsType(&p.CurrentWorkflowConditionFailedError{}, err)
}

func (s *executionMutableStateSuite) TestConflictResolve_ResetCurrent_Conflict() {
	currentSnapshot := s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)

	currentSnapshot = randomSnapshot(
		s.namespaceID,
		s.workflowID,
		s.runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	_, err := s.executionManager.ConflictResolveWorkflowExecution(&p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.ConflictResolveWorkflowModeUpdateCurrent,

		ResetWorkflowSnapshot: *currentSnapshot,
		ResetWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: nil,
		CurrentWorkflowEvents:   nil,
	})
	s.IsType(&p.WorkflowConditionFailedError{}, err)
}

func (s *executionMutableStateSuite) TestConflictResolve_ResetCurrent_WithNew() {
	currentSnapshot := s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)

	currentSnapshot = randomSnapshot(
		s.namespaceID,
		s.workflowID,
		s.runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		currentSnapshot.DBRecordVersion+1,
	)
	newSnapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		uuid.New().String(),
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)
	_, err := s.executionManager.ConflictResolveWorkflowExecution(&p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.ConflictResolveWorkflowModeUpdateCurrent,

		ResetWorkflowSnapshot: *currentSnapshot,
		ResetWorkflowEvents:   nil,

		NewWorkflowSnapshot: newSnapshot,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: nil,
		CurrentWorkflowEvents:   nil,
	})
	s.NoError(err)
}

func (s *executionMutableStateSuite) TestConflictResolve_Zombie() {
	_ = s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)
	runID := uuid.New().String()
	dbVersion := rand.Int63()
	zombieSnapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		dbVersion,
	)
	_, err := s.executionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.CreateWorkflowModeZombie,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *zombieSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	zombieSnapshot = randomSnapshot(
		s.namespaceID,
		s.workflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		dbVersion+1,
	)
	_, err = s.executionManager.ConflictResolveWorkflowExecution(&p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.ConflictResolveWorkflowModeBypassCurrent,

		ResetWorkflowSnapshot: *zombieSnapshot,
		ResetWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: nil,
		CurrentWorkflowEvents:   nil,
	})
	s.NoError(err)
}

func (s *executionMutableStateSuite) TestConflictResolve_Zombie_CurrentConflict() {
	dbVersion := rand.Int63()
	currentSnapshot := s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		dbVersion,
	)

	currentSnapshot = randomSnapshot(
		s.namespaceID,
		s.workflowID,
		s.runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		dbVersion+1,
	)
	_, err := s.executionManager.ConflictResolveWorkflowExecution(&p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.ConflictResolveWorkflowModeBypassCurrent,

		ResetWorkflowSnapshot: *currentSnapshot,
		ResetWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: nil,
		CurrentWorkflowEvents:   nil,
	})
	s.IsType(&p.CurrentWorkflowConditionFailedError{}, err)
}

func (s *executionMutableStateSuite) TestConflictResolve_Zombie_Conflict() {
	_ = s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)
	runID := uuid.New().String()
	zombieSnapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	_, err := s.executionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.CreateWorkflowModeZombie,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *zombieSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	zombieSnapshot = randomSnapshot(
		s.namespaceID,
		s.workflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	_, err = s.executionManager.ConflictResolveWorkflowExecution(&p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.ConflictResolveWorkflowModeBypassCurrent,

		ResetWorkflowSnapshot: *zombieSnapshot,
		ResetWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: nil,
		CurrentWorkflowEvents:   nil,
	})
	s.IsType(&p.WorkflowConditionFailedError{}, err)
}

func (s *executionMutableStateSuite) TestConflictResolve_Zombie_WithNew() {
	_ = s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)
	runID := uuid.New().String()
	dbVersion := rand.Int63()
	zombieSnapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		dbVersion,
	)
	_, err := s.executionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.CreateWorkflowModeZombie,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *zombieSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	zombieSnapshot = randomSnapshot(
		s.namespaceID,
		s.workflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		dbVersion+1,
	)
	newZombieSnapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		uuid.New().String(),
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)
	_, err = s.executionManager.ConflictResolveWorkflowExecution(&p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.ConflictResolveWorkflowModeBypassCurrent,

		ResetWorkflowSnapshot: *zombieSnapshot,
		ResetWorkflowEvents:   nil,

		NewWorkflowSnapshot: newZombieSnapshot,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: nil,
		CurrentWorkflowEvents:   nil,
	})
	s.NoError(err)
}

func (s *executionMutableStateSuite) TestDeleteCurrent_IsCurrent() {
	_ = s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)

	err := s.executionManager.DeleteCurrentWorkflowExecution(&p.DeleteCurrentWorkflowExecutionRequest{
		ShardID:     s.shardID,
		NamespaceID: s.namespaceID,
		WorkflowID:  s.workflowID,
		RunID:       s.runID,
	})
	s.NoError(err)

	_, err = s.executionManager.GetCurrentExecution(&p.GetCurrentExecutionRequest{
		ShardID:     s.shardID,
		NamespaceID: s.namespaceID,
		WorkflowID:  s.workflowID,
	})
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *executionMutableStateSuite) TestDeleteCurrent_NotCurrent() {
	err := s.executionManager.DeleteCurrentWorkflowExecution(&p.DeleteCurrentWorkflowExecutionRequest{
		ShardID:     s.shardID,
		NamespaceID: s.namespaceID,
		WorkflowID:  s.workflowID,
		RunID:       s.runID,
	})
	s.NoError(err)

	_, err = s.executionManager.GetCurrentExecution(&p.GetCurrentExecutionRequest{
		ShardID:     s.shardID,
		NamespaceID: s.namespaceID,
		WorkflowID:  s.workflowID,
	})
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *executionMutableStateSuite) TestDelete_Exists() {
	_ = s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)

	err := s.executionManager.DeleteWorkflowExecution(&p.DeleteWorkflowExecutionRequest{
		ShardID:     s.shardID,
		NamespaceID: s.namespaceID,
		WorkflowID:  s.workflowID,
		RunID:       s.runID,
	})
	s.NoError(err)

	_, err = s.executionManager.GetWorkflowExecution(&p.GetWorkflowExecutionRequest{
		ShardID:     s.shardID,
		NamespaceID: s.namespaceID,
		WorkflowID:  s.workflowID,
		RunID:       s.runID,
	})
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *executionMutableStateSuite) TestDelete_NotExists() {
	err := s.executionManager.DeleteWorkflowExecution(&p.DeleteWorkflowExecutionRequest{
		ShardID:     s.shardID,
		NamespaceID: s.namespaceID,
		WorkflowID:  s.workflowID,
		RunID:       s.runID,
	})
	s.NoError(err)

	_, err = s.executionManager.GetWorkflowExecution(&p.GetWorkflowExecutionRequest{
		ShardID:     s.shardID,
		NamespaceID: s.namespaceID,
		WorkflowID:  s.workflowID,
		RunID:       s.runID,
	})
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *executionMutableStateSuite) createWorkflow(
	lastWriteVersion int64,
	state enumsspb.WorkflowExecutionState,
	status enumspb.WorkflowExecutionStatus,
	dbRecordVersion int64,
) *p.WorkflowSnapshot {
	snapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		s.runID,
		lastWriteVersion,
		state,
		status,
		dbRecordVersion,
	)
	_, err := s.executionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.CreateWorkflowModeBrandNew,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *snapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)
	return snapshot
}
