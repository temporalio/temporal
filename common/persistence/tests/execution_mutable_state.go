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

	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
)

type (
	ExecutionMutableStateSuite struct {
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

func NewExecutionMutableStateSuite(
	t *testing.T,
	shardStore p.ShardStore,
	executionStore p.ExecutionStore,
	logger log.Logger,
) *ExecutionMutableStateSuite {
	return &ExecutionMutableStateSuite{
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

func (s *ExecutionMutableStateSuite) SetupSuite() {
	rand.Seed(time.Now().UnixNano())
}

func (s *ExecutionMutableStateSuite) TearDownSuite() {

}

func (s *ExecutionMutableStateSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.shardID = 1 + rand.Int31n(16)
	resp, err := s.shardManager.GetOrCreateShard(&p.GetOrCreateShardRequest{
		ShardID: s.shardID,
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

func (s *ExecutionMutableStateSuite) TearDownTest() {

}

func (s *ExecutionMutableStateSuite) TestCreate_BrandNew() {
	newSnapshot := s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	s.assertEqualWithDB(newSnapshot)
}

func (s *ExecutionMutableStateSuite) TestCreate_BrandNew_CurrentConflict() {
	lastWriteVersion := rand.Int63()
	newSnapshot := s.createWorkflow(
		lastWriteVersion,
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		rand.Int63(),
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

	s.assertEqualWithDB(newSnapshot)
}

func (s *ExecutionMutableStateSuite) TestCreate_Reuse() {
	prevLastWriteVersion := rand.Int63()
	prevSnapshot := s.createWorkflow(
		prevLastWriteVersion,
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		rand.Int63(),
	)

	newSnapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		uuid.New().String(),
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
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

	s.assertEqualWithDB(newSnapshot)
}

func (s *ExecutionMutableStateSuite) TestCreate_Reuse_CurrentConflict() {
	prevLastWriteVersion := rand.Int63()
	prevSnapshot := s.createWorkflow(
		prevLastWriteVersion,
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		rand.Int63(),
	)

	_, err := s.executionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.CreateWorkflowModeWorkflowIDReuse,

		PreviousRunID:            uuid.New().String(),
		PreviousLastWriteVersion: rand.Int63(),

		NewWorkflowSnapshot: *prevSnapshot,
		NewWorkflowEvents:   nil,
	})
	if err, ok := err.(*p.CurrentWorkflowConditionFailedError); ok {
		err.Msg = ""
	}
	s.Equal(&p.CurrentWorkflowConditionFailedError{
		Msg:              "",
		RequestID:        prevSnapshot.ExecutionState.CreateRequestId,
		RunID:            prevSnapshot.ExecutionState.RunId,
		State:            prevSnapshot.ExecutionState.State,
		Status:           prevSnapshot.ExecutionState.Status,
		LastWriteVersion: prevLastWriteVersion,
	}, err)

	s.assertEqualWithDB(prevSnapshot)
}

func (s *ExecutionMutableStateSuite) TestCreate_Zombie() {
	prevLastWriteVersion := rand.Int63()
	_ = s.createWorkflow(
		prevLastWriteVersion,
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		rand.Int63(),
	)

	newSnapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		uuid.New().String(),
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

		NewWorkflowSnapshot: *newSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	s.assertEqualWithDB(newSnapshot)
}

func (s *ExecutionMutableStateSuite) TestCreate_Conflict() {
	lastWriteVersion := rand.Int63()
	newSnapshot := s.createWorkflow(
		lastWriteVersion,
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		rand.Int63(),
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

func (s *ExecutionMutableStateSuite) TestUpdate_NotZombie() {
	currentSnapshot := s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
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

	s.assertEqualWithDB(currentSnapshot, currentMutation)
}

func (s *ExecutionMutableStateSuite) TestUpdate_NotZombie_CurrentConflict() {
	_ = s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
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

	s.assertMissingFromDB(
		currentMutation.ExecutionInfo.NamespaceId,
		currentMutation.ExecutionInfo.WorkflowId,
		currentMutation.ExecutionState.RunId,
	)
}

func (s *ExecutionMutableStateSuite) TestUpdate_NotZombie_Conflict() {
	currentSnapshot := s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
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

	s.assertEqualWithDB(currentSnapshot)
}

func (s *ExecutionMutableStateSuite) TestUpdate_NotZombie_WithNew() {
	currentSnapshot := s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
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
		rand.Int63(),
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

	s.assertEqualWithDB(currentSnapshot, currentMutation)
	s.assertEqualWithDB(newSnapshot)
}

func (s *ExecutionMutableStateSuite) TestUpdate_Zombie() {
	_ = s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
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

	s.assertEqualWithDB(zombieSnapshot, zombieMutation)
}

func (s *ExecutionMutableStateSuite) TestUpdate_Zombie_CurrentConflict() {
	currentSnapshot := s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
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

	s.assertEqualWithDB(currentSnapshot)
}

func (s *ExecutionMutableStateSuite) TestUpdate_Zombie_Conflict() {
	_ = s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
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

	s.assertEqualWithDB(zombieSnapshot)
}

func (s *ExecutionMutableStateSuite) TestUpdate_Zombie_WithNew() {
	_ = s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
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
		rand.Int63(),
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

	s.assertEqualWithDB(zombieSnapshot, zombieMutation)
	s.assertEqualWithDB(newZombieSnapshot)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_SuppressCurrent() {
	currentSnapshot := s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	runID := uuid.New().String()
	baseSnapshot := randomSnapshot(
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

		NewWorkflowSnapshot: *baseSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	resetSnapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		baseSnapshot.DBRecordVersion+1,
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

	s.assertEqualWithDB(resetSnapshot)
	s.assertEqualWithDB(currentSnapshot, currentMutation)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_SuppressCurrent_CurrentConflict() {
	currentSnapshot := s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	runID := uuid.New().String()
	baseSnapshot := randomSnapshot(
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

		NewWorkflowSnapshot: *baseSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	resetSnapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		baseSnapshot.DBRecordVersion+1,
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

	s.assertEqualWithDB(baseSnapshot)
	s.assertEqualWithDB(currentSnapshot)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_SuppressCurrent_Conflict_Case1() {
	currentSnapshot := s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	runID := uuid.New().String()
	baseSnapshot := randomSnapshot(
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

		NewWorkflowSnapshot: *baseSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	resetSnapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		baseSnapshot.DBRecordVersion+1,
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

	s.assertEqualWithDB(baseSnapshot)
	s.assertEqualWithDB(currentSnapshot)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_SuppressCurrent_Conflict_Case2() {
	currentSnapshot := s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	runID := uuid.New().String()
	baseSnapshot := randomSnapshot(
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

		NewWorkflowSnapshot: *baseSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	resetSnapshot := randomSnapshot(
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

	s.assertEqualWithDB(baseSnapshot)
	s.assertEqualWithDB(currentSnapshot)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_SuppressCurrent_WithNew() {
	currentSnapshot := s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	runID := uuid.New().String()
	baseSnapshot := randomSnapshot(
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

		NewWorkflowSnapshot: *baseSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	resetSnapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		baseSnapshot.DBRecordVersion+1,
	)
	newSnapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		uuid.New().String(),
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

		NewWorkflowSnapshot: newSnapshot,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: currentMutation,
		CurrentWorkflowEvents:   nil,
	})
	s.NoError(err)

	s.assertEqualWithDB(resetSnapshot)
	s.assertEqualWithDB(newSnapshot)
	s.assertEqualWithDB(currentSnapshot, currentMutation)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_ResetCurrent() {
	baseSnapshot := s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	resetSnapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		s.runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		baseSnapshot.DBRecordVersion+1,
	)
	_, err := s.executionManager.ConflictResolveWorkflowExecution(&p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.ConflictResolveWorkflowModeUpdateCurrent,

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: nil,
		CurrentWorkflowEvents:   nil,
	})
	s.NoError(err)

	s.assertEqualWithDB(resetSnapshot)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_ResetCurrent_CurrentConflict() {
	_ = s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	runID := uuid.New().String()
	baseSnapshot := randomSnapshot(
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

		NewWorkflowSnapshot: *baseSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	resetSnapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		baseSnapshot.DBRecordVersion+1,
	)
	_, err = s.executionManager.ConflictResolveWorkflowExecution(&p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.ConflictResolveWorkflowModeUpdateCurrent,

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: nil,
		CurrentWorkflowEvents:   nil,
	})
	s.IsType(&p.CurrentWorkflowConditionFailedError{}, err)

	s.assertEqualWithDB(baseSnapshot)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_ResetCurrent_Conflict() {
	baseSnapshot := s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	resetSnapshot := randomSnapshot(
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

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: nil,
		CurrentWorkflowEvents:   nil,
	})
	s.IsType(&p.WorkflowConditionFailedError{}, err)

	s.assertEqualWithDB(baseSnapshot)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_ResetCurrent_WithNew() {
	baseSnapshot := s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	resetSnapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		s.runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		baseSnapshot.DBRecordVersion+1,
	)
	newSnapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		uuid.New().String(),
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	_, err := s.executionManager.ConflictResolveWorkflowExecution(&p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.ConflictResolveWorkflowModeUpdateCurrent,

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   nil,

		NewWorkflowSnapshot: newSnapshot,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: nil,
		CurrentWorkflowEvents:   nil,
	})
	s.NoError(err)

	s.assertEqualWithDB(resetSnapshot)
	s.assertEqualWithDB(newSnapshot)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_Zombie() {
	_ = s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	runID := uuid.New().String()
	baseSnapshot := randomSnapshot(
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

		NewWorkflowSnapshot: *baseSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	resetSnapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		baseSnapshot.DBRecordVersion+1,
	)
	_, err = s.executionManager.ConflictResolveWorkflowExecution(&p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.ConflictResolveWorkflowModeBypassCurrent,

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: nil,
		CurrentWorkflowEvents:   nil,
	})
	s.NoError(err)

	s.assertEqualWithDB(resetSnapshot)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_Zombie_CurrentConflict() {
	baseSnapshot := s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	resetSnapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		s.runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		baseSnapshot.DBRecordVersion+1,
	)
	_, err := s.executionManager.ConflictResolveWorkflowExecution(&p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.ConflictResolveWorkflowModeBypassCurrent,

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: nil,
		CurrentWorkflowEvents:   nil,
	})
	s.IsType(&p.CurrentWorkflowConditionFailedError{}, err)

	s.assertEqualWithDB(baseSnapshot)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_Zombie_Conflict() {
	_ = s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	runID := uuid.New().String()
	baseSnapshot := randomSnapshot(
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

		NewWorkflowSnapshot: *baseSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	resetSnapshot := randomSnapshot(
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

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: nil,
		CurrentWorkflowEvents:   nil,
	})
	s.IsType(&p.WorkflowConditionFailedError{}, err)

	s.assertEqualWithDB(baseSnapshot)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_Zombie_WithNew() {
	_ = s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	runID := uuid.New().String()
	baseSnapshot := randomSnapshot(
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

		NewWorkflowSnapshot: *baseSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	resetSnapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		baseSnapshot.DBRecordVersion+1,
	)
	newSnapshot := randomSnapshot(
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
		Mode:    p.ConflictResolveWorkflowModeBypassCurrent,

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   nil,

		NewWorkflowSnapshot: newSnapshot,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: nil,
		CurrentWorkflowEvents:   nil,
	})
	s.NoError(err)

	s.assertEqualWithDB(resetSnapshot)
	s.assertEqualWithDB(newSnapshot)
}

func (s *ExecutionMutableStateSuite) TestDeleteCurrent_IsCurrent() {
	_ = s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
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

func (s *ExecutionMutableStateSuite) TestDeleteCurrent_NotCurrent() {
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

func (s *ExecutionMutableStateSuite) TestDelete_Exists() {
	_ = s.createWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	err := s.executionManager.DeleteWorkflowExecution(&p.DeleteWorkflowExecutionRequest{
		ShardID:     s.shardID,
		NamespaceID: s.namespaceID,
		WorkflowID:  s.workflowID,
		RunID:       s.runID,
	})
	s.NoError(err)

	s.assertMissingFromDB(s.namespaceID, s.workflowID, s.runID)
}

func (s *ExecutionMutableStateSuite) TestDelete_NotExists() {
	err := s.executionManager.DeleteWorkflowExecution(&p.DeleteWorkflowExecutionRequest{
		ShardID:     s.shardID,
		NamespaceID: s.namespaceID,
		WorkflowID:  s.workflowID,
		RunID:       s.runID,
	})
	s.NoError(err)

	s.assertMissingFromDB(s.namespaceID, s.workflowID, s.runID)
}

func (s *ExecutionMutableStateSuite) createWorkflow(
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

func (s *ExecutionMutableStateSuite) assertMissingFromDB(
	namespaceID string,
	workflowID string,
	runID string,
) {
	_, err := s.executionManager.GetWorkflowExecution(&p.GetWorkflowExecutionRequest{
		ShardID:     s.shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	})
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *ExecutionMutableStateSuite) assertEqualWithDB(
	snapshot *p.WorkflowSnapshot,
	mutations ...*p.WorkflowMutation,
) {
	resp, err := s.executionManager.GetWorkflowExecution(&p.GetWorkflowExecutionRequest{
		ShardID:     s.shardID,
		NamespaceID: snapshot.ExecutionInfo.NamespaceId,
		WorkflowID:  snapshot.ExecutionInfo.WorkflowId,
		RunID:       snapshot.ExecutionState.RunId,
	})
	s.NoError(err)

	actualMutableState := resp.State
	actualDBRecordVersion := resp.DBRecordVersion

	expectedMutableState, expectedDBRecordVersion := s.accumulate(snapshot, mutations...)

	// need to special handling signal request IDs ...
	// since ^ is slice
	s.Equal(
		convert.StringSliceToSet(expectedMutableState.SignalRequestedIds),
		convert.StringSliceToSet(actualMutableState.SignalRequestedIds),
	)
	actualMutableState.SignalRequestedIds = expectedMutableState.SignalRequestedIds

	s.Equal(expectedDBRecordVersion, actualDBRecordVersion)
	s.Equal(expectedMutableState, actualMutableState)
}

func (s *ExecutionMutableStateSuite) accumulate(
	snapshot *p.WorkflowSnapshot,
	mutations ...*p.WorkflowMutation,
) (*persistencespb.WorkflowMutableState, int64) {
	mutableState := &persistencespb.WorkflowMutableState{
		ExecutionInfo:  snapshot.ExecutionInfo,
		ExecutionState: snapshot.ExecutionState,

		NextEventId: snapshot.NextEventID,

		ActivityInfos:       snapshot.ActivityInfos,
		TimerInfos:          snapshot.TimerInfos,
		ChildExecutionInfos: snapshot.ChildExecutionInfos,
		RequestCancelInfos:  snapshot.RequestCancelInfos,
		SignalInfos:         snapshot.SignalInfos,
		SignalRequestedIds:  convert.StringSetToSlice(snapshot.SignalRequestedIDs),
	}
	dbRecordVersion := snapshot.DBRecordVersion

	for _, mutation := range mutations {
		s.Equal(dbRecordVersion, mutation.DBRecordVersion-1)
		dbRecordVersion = mutation.DBRecordVersion

		mutableState.ExecutionInfo = mutation.ExecutionInfo
		mutableState.ExecutionState = mutation.ExecutionState

		mutableState.NextEventId = mutation.NextEventID

		// activity infos
		for key, info := range mutation.UpsertActivityInfos {
			mutableState.ActivityInfos[key] = info
		}
		for key := range mutation.DeleteActivityInfos {
			delete(mutableState.ActivityInfos, key)
		}

		// timer infos
		for key, info := range mutation.UpsertTimerInfos {
			mutableState.TimerInfos[key] = info
		}
		for key := range mutation.DeleteTimerInfos {
			delete(mutableState.TimerInfos, key)
		}

		// child workflow infos
		for key, info := range mutation.UpsertChildExecutionInfos {
			mutableState.ChildExecutionInfos[key] = info
		}
		for key := range mutation.DeleteChildExecutionInfos {
			delete(mutableState.ChildExecutionInfos, key)
		}

		// request cancel infos
		for key, info := range mutation.UpsertRequestCancelInfos {
			mutableState.RequestCancelInfos[key] = info
		}
		for key := range mutation.DeleteRequestCancelInfos {
			delete(mutableState.RequestCancelInfos, key)
		}

		// signal infos
		for key, info := range mutation.UpsertSignalInfos {
			mutableState.SignalInfos[key] = info
		}
		for key := range mutation.DeleteSignalInfos {
			delete(mutableState.SignalInfos, key)
		}

		// signal request IDs
		signalRequestIDs := convert.StringSliceToSet(mutableState.SignalRequestedIds)
		for key, info := range mutation.UpsertSignalRequestedIDs {
			signalRequestIDs[key] = info
		}
		for key := range mutation.DeleteSignalRequestedIDs {
			delete(signalRequestIDs, key)
		}
		mutableState.SignalRequestedIds = convert.StringSetToSlice(signalRequestIDs)

		// buffered events
		if mutation.ClearBufferedEvents {
			mutableState.BufferedEvents = nil
		} else if mutation.NewBufferedEvents != nil {
			mutableState.BufferedEvents = append(mutableState.BufferedEvents, mutation.NewBufferedEvents...)
		}
	}

	// need to serialize & deserialize to get rid of timezone information ...
	bytes, err := proto.Marshal(mutableState)
	s.NoError(err)
	mutableState = &persistencespb.WorkflowMutableState{}
	err = proto.Unmarshal(bytes, mutableState)
	s.NoError(err)

	// make equal test easier
	if mutableState.ActivityInfos == nil {
		mutableState.ActivityInfos = make(map[int64]*persistencespb.ActivityInfo)
	}
	if mutableState.TimerInfos == nil {
		mutableState.TimerInfos = make(map[string]*persistencespb.TimerInfo)
	}
	if mutableState.ChildExecutionInfos == nil {
		mutableState.ChildExecutionInfos = make(map[int64]*persistencespb.ChildExecutionInfo)
	}
	if mutableState.RequestCancelInfos == nil {
		mutableState.RequestCancelInfos = make(map[int64]*persistencespb.RequestCancelInfo)
	}
	if mutableState.SignalInfos == nil {
		mutableState.SignalInfos = make(map[int64]*persistencespb.SignalInfo)
	}
	if mutableState.SignalRequestedIds == nil {
		mutableState.SignalRequestedIds = make([]string, 0)
	}
	if mutableState.BufferedEvents == nil {
		mutableState.BufferedEvents = make([]*historypb.HistoryEvent, 0)
	}

	return mutableState, dbRecordVersion
}
