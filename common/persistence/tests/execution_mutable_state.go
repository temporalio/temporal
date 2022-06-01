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
	"context"
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
	"go.temporal.io/server/common/persistence/serialization"
)

const (
	maxShards = 16
)

type (
	ExecutionMutableStateSuite struct {
		suite.Suite
		*require.Assertions

		ShardID     int32
		RangeID     int64
		NamespaceID string
		WorkflowID  string
		RunID       string

		ShardManager     p.ShardManager
		ExecutionManager p.ExecutionManager
		Logger           log.Logger

		Ctx    context.Context
		Cancel context.CancelFunc
	}
)

func NewExecutionMutableStateSuite(
	t *testing.T,
	shardStore p.ShardStore,
	executionStore p.ExecutionStore,
	serializer serialization.Serializer,
	logger log.Logger,
) *ExecutionMutableStateSuite {
	return &ExecutionMutableStateSuite{
		Assertions: require.New(t),
		ShardManager: p.NewShardManager(
			shardStore,
			serializer,
		),
		ExecutionManager: p.NewExecutionManager(
			executionStore,
			serializer,
			logger,
			dynamicconfig.GetIntPropertyFn(4*1024*1024),
		),
		Logger: logger,
	}
}

func (s *ExecutionMutableStateSuite) SetupSuite() {
	rand.Seed(time.Now().UnixNano())
}

func (s *ExecutionMutableStateSuite) TearDownSuite() {

}

func (s *ExecutionMutableStateSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.Ctx, s.Cancel = context.WithTimeout(context.Background(), time.Second*30)

	s.ShardID = 1 + rand.Int31n(maxShards)
	resp, err := s.ShardManager.GetOrCreateShard(s.Ctx, &p.GetOrCreateShardRequest{
		ShardID: s.ShardID,
		InitialShardInfo: &persistencespb.ShardInfo{
			ShardId: s.ShardID,
			RangeId: 1,
		},
	})
	s.NoError(err)
	previousRangeID := resp.ShardInfo.RangeId
	resp.ShardInfo.RangeId += 1
	err = s.ShardManager.UpdateShard(s.Ctx, &p.UpdateShardRequest{
		ShardInfo:       resp.ShardInfo,
		PreviousRangeID: previousRangeID,
	})
	s.NoError(err)
	s.RangeID = resp.ShardInfo.RangeId

	s.NamespaceID = uuid.New().String()
	s.WorkflowID = uuid.New().String()
	s.RunID = uuid.New().String()
}

func (s *ExecutionMutableStateSuite) TearDownTest() {
	s.Cancel()
}

func (s *ExecutionMutableStateSuite) TestCreate_BrandNew() {
	newSnapshot := s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	s.AssertEqualWithDB(newSnapshot)
}

func (s *ExecutionMutableStateSuite) TestCreate_BrandNew_CurrentConflict() {
	lastWriteVersion := rand.Int63()
	newSnapshot := s.CreateWorkflow(
		lastWriteVersion,
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		rand.Int63(),
	)

	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
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

	s.AssertEqualWithDB(newSnapshot)
}

func (s *ExecutionMutableStateSuite) TestCreate_Reuse() {
	prevLastWriteVersion := rand.Int63()
	prevSnapshot := s.CreateWorkflow(
		prevLastWriteVersion,
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		rand.Int63(),
	)

	newSnapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		uuid.New().String(),
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeUpdateCurrent,

		PreviousRunID:            prevSnapshot.ExecutionState.RunId,
		PreviousLastWriteVersion: prevLastWriteVersion,

		NewWorkflowSnapshot: *newSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	s.AssertEqualWithDB(newSnapshot)
}

func (s *ExecutionMutableStateSuite) TestCreate_Reuse_CurrentConflict() {
	prevLastWriteVersion := rand.Int63()
	prevSnapshot := s.CreateWorkflow(
		prevLastWriteVersion,
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		rand.Int63(),
	)

	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeUpdateCurrent,

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

	s.AssertEqualWithDB(prevSnapshot)
}

func (s *ExecutionMutableStateSuite) TestCreate_Zombie() {
	prevLastWriteVersion := rand.Int63()
	_ = s.CreateWorkflow(
		prevLastWriteVersion,
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		rand.Int63(),
	)

	newSnapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		uuid.New().String(),
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeBypassCurrent,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *newSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	s.AssertEqualWithDB(newSnapshot)
}

func (s *ExecutionMutableStateSuite) TestCreate_Conflict() {
	lastWriteVersion := rand.Int63()
	newSnapshot := s.CreateWorkflow(
		lastWriteVersion,
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		rand.Int63(),
	)

	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeUpdateCurrent,

		PreviousRunID:            newSnapshot.ExecutionState.RunId,
		PreviousLastWriteVersion: lastWriteVersion,

		NewWorkflowSnapshot: *newSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.IsType(&p.WorkflowConditionFailedError{}, err)
}

func (s *ExecutionMutableStateSuite) TestCreate_ClosedWorkflow_BrandNew() {
	newSnapshot := s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		rand.Int63(),
	)

	s.AssertEqualWithDB(newSnapshot)
}

func (s *ExecutionMutableStateSuite) TestCreate_ClosedWorkflow_Bypass() {
	prevLastWriteVersion := rand.Int63()
	_ = s.CreateWorkflow(
		prevLastWriteVersion,
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	newSnapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		uuid.New().String(),
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		rand.Int63(),
	)

	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeBypassCurrent,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *newSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	s.AssertEqualWithDB(newSnapshot)
}

func (s *ExecutionMutableStateSuite) TestCreate_ClosedWorkflow_UpdateCurrent() {
	prevLastWriteVersion := rand.Int63()
	prevSnapshot := s.CreateWorkflow(
		prevLastWriteVersion,
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		rand.Int63(),
	)

	newSnapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		uuid.New().String(),
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		rand.Int63(),
	)

	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeUpdateCurrent,

		PreviousRunID:            prevSnapshot.ExecutionState.RunId,
		PreviousLastWriteVersion: prevLastWriteVersion,

		NewWorkflowSnapshot: *newSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	s.AssertEqualWithDB(newSnapshot)
}

func (s *ExecutionMutableStateSuite) TestUpdate_NotZombie() {
	currentSnapshot := s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	currentMutation := RandomMutation(
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		currentSnapshot.DBRecordVersion+1,
	)
	_, err := s.ExecutionManager.UpdateWorkflowExecution(s.Ctx, &p.UpdateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.UpdateWorkflowModeUpdateCurrent,

		UpdateWorkflowMutation: *currentMutation,
		UpdateWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	s.AssertEqualWithDB(currentSnapshot, currentMutation)
}

func (s *ExecutionMutableStateSuite) TestUpdate_NotZombie_CurrentConflict() {
	_ = s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	currentMutation := RandomMutation(
		s.NamespaceID,
		s.WorkflowID,
		uuid.New().String(),
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	_, err := s.ExecutionManager.UpdateWorkflowExecution(s.Ctx, &p.UpdateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.UpdateWorkflowModeUpdateCurrent,

		UpdateWorkflowMutation: *currentMutation,
		UpdateWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,
	})
	s.IsType(&p.CurrentWorkflowConditionFailedError{}, err)

	s.AssertMissingFromDB(
		currentMutation.ExecutionInfo.NamespaceId,
		currentMutation.ExecutionInfo.WorkflowId,
		currentMutation.ExecutionState.RunId,
	)
}

func (s *ExecutionMutableStateSuite) TestUpdate_NotZombie_Conflict() {
	currentSnapshot := s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	currentMutation := RandomMutation(
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	_, err := s.ExecutionManager.UpdateWorkflowExecution(s.Ctx, &p.UpdateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.UpdateWorkflowModeUpdateCurrent,

		UpdateWorkflowMutation: *currentMutation,
		UpdateWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,
	})
	s.IsType(&p.WorkflowConditionFailedError{}, err)

	s.AssertEqualWithDB(currentSnapshot)
}

func (s *ExecutionMutableStateSuite) TestUpdate_NotZombie_WithNew() {
	currentSnapshot := s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	currentMutation := RandomMutation(
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		currentSnapshot.DBRecordVersion+1,
	)
	newSnapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		uuid.New().String(),
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	_, err := s.ExecutionManager.UpdateWorkflowExecution(s.Ctx, &p.UpdateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.UpdateWorkflowModeUpdateCurrent,

		UpdateWorkflowMutation: *currentMutation,
		UpdateWorkflowEvents:   nil,

		NewWorkflowSnapshot: newSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	s.AssertEqualWithDB(currentSnapshot, currentMutation)
	s.AssertEqualWithDB(newSnapshot)
}

func (s *ExecutionMutableStateSuite) TestUpdate_Zombie() {
	_ = s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	runID := uuid.New().String()
	zombieSnapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeBypassCurrent,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *zombieSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	zombieMutation := RandomMutation(
		s.NamespaceID,
		s.WorkflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		zombieSnapshot.DBRecordVersion+1,
	)
	_, err = s.ExecutionManager.UpdateWorkflowExecution(s.Ctx, &p.UpdateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.UpdateWorkflowModeBypassCurrent,

		UpdateWorkflowMutation: *zombieMutation,
		UpdateWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	s.AssertEqualWithDB(zombieSnapshot, zombieMutation)
}

func (s *ExecutionMutableStateSuite) TestUpdate_Zombie_CurrentConflict() {
	currentSnapshot := s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	currentMutation := RandomMutation(
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		currentSnapshot.DBRecordVersion+1,
	)
	_, err := s.ExecutionManager.UpdateWorkflowExecution(s.Ctx, &p.UpdateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.UpdateWorkflowModeBypassCurrent,

		UpdateWorkflowMutation: *currentMutation,
		UpdateWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,
	})
	s.IsType(&p.CurrentWorkflowConditionFailedError{}, err)

	s.AssertEqualWithDB(currentSnapshot)
}

func (s *ExecutionMutableStateSuite) TestUpdate_Zombie_Conflict() {
	_ = s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	runID := uuid.New().String()
	zombieSnapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeBypassCurrent,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *zombieSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	zombieMutation := RandomMutation(
		s.NamespaceID,
		s.WorkflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	_, err = s.ExecutionManager.UpdateWorkflowExecution(s.Ctx, &p.UpdateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.UpdateWorkflowModeBypassCurrent,

		UpdateWorkflowMutation: *zombieMutation,
		UpdateWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,
	})
	s.IsType(&p.WorkflowConditionFailedError{}, err)

	s.AssertEqualWithDB(zombieSnapshot)
}

func (s *ExecutionMutableStateSuite) TestUpdate_Zombie_WithNew() {
	_ = s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	runID := uuid.New().String()
	zombieSnapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeBypassCurrent,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *zombieSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	zombieMutation := RandomMutation(
		s.NamespaceID,
		s.WorkflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		zombieSnapshot.DBRecordVersion+1,
	)
	newZombieSnapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		uuid.New().String(),
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	_, err = s.ExecutionManager.UpdateWorkflowExecution(s.Ctx, &p.UpdateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.UpdateWorkflowModeBypassCurrent,

		UpdateWorkflowMutation: *zombieMutation,
		UpdateWorkflowEvents:   nil,

		NewWorkflowSnapshot: newZombieSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	s.AssertEqualWithDB(zombieSnapshot, zombieMutation)
	s.AssertEqualWithDB(newZombieSnapshot)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_SuppressCurrent() {
	currentSnapshot := s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	runID := uuid.New().String()
	baseSnapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeBypassCurrent,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *baseSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	resetSnapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		baseSnapshot.DBRecordVersion+1,
	)
	currentMutation := RandomMutation(
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		currentSnapshot.DBRecordVersion+1,
	)
	_, err = s.ExecutionManager.ConflictResolveWorkflowExecution(s.Ctx, &p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.ConflictResolveWorkflowModeUpdateCurrent,

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: currentMutation,
		CurrentWorkflowEvents:   nil,
	})
	s.NoError(err)

	s.AssertEqualWithDB(resetSnapshot)
	s.AssertEqualWithDB(currentSnapshot, currentMutation)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_SuppressCurrent_CurrentConflict() {
	currentSnapshot := s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	runID := uuid.New().String()
	baseSnapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeBypassCurrent,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *baseSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	resetSnapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		baseSnapshot.DBRecordVersion+1,
	)
	currentMutation := RandomMutation(
		s.NamespaceID,
		s.WorkflowID,
		uuid.New().String(),
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	_, err = s.ExecutionManager.ConflictResolveWorkflowExecution(s.Ctx, &p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.ConflictResolveWorkflowModeUpdateCurrent,

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: currentMutation,
		CurrentWorkflowEvents:   nil,
	})
	s.IsType(&p.CurrentWorkflowConditionFailedError{}, err)

	s.AssertEqualWithDB(baseSnapshot)
	s.AssertEqualWithDB(currentSnapshot)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_SuppressCurrent_Conflict_Case1() {
	currentSnapshot := s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	runID := uuid.New().String()
	baseSnapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeBypassCurrent,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *baseSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	resetSnapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		baseSnapshot.DBRecordVersion+1,
	)
	currentMutation := RandomMutation(
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	_, err = s.ExecutionManager.ConflictResolveWorkflowExecution(s.Ctx, &p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.ConflictResolveWorkflowModeUpdateCurrent,

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: currentMutation,
		CurrentWorkflowEvents:   nil,
	})
	s.IsType(&p.WorkflowConditionFailedError{}, err)

	s.AssertEqualWithDB(baseSnapshot)
	s.AssertEqualWithDB(currentSnapshot)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_SuppressCurrent_Conflict_Case2() {
	currentSnapshot := s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	runID := uuid.New().String()
	baseSnapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeBypassCurrent,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *baseSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	resetSnapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	currentMutation := RandomMutation(
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		currentSnapshot.DBRecordVersion+1,
	)
	_, err = s.ExecutionManager.ConflictResolveWorkflowExecution(s.Ctx, &p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.ConflictResolveWorkflowModeUpdateCurrent,

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: currentMutation,
		CurrentWorkflowEvents:   nil,
	})
	s.IsType(&p.WorkflowConditionFailedError{}, err)

	s.AssertEqualWithDB(baseSnapshot)
	s.AssertEqualWithDB(currentSnapshot)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_SuppressCurrent_WithNew() {
	currentSnapshot := s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	runID := uuid.New().String()
	baseSnapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeBypassCurrent,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *baseSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	resetSnapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		baseSnapshot.DBRecordVersion+1,
	)
	newSnapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		uuid.New().String(),
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	currentMutation := RandomMutation(
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		currentSnapshot.DBRecordVersion+1,
	)
	_, err = s.ExecutionManager.ConflictResolveWorkflowExecution(s.Ctx, &p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.ConflictResolveWorkflowModeUpdateCurrent,

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   nil,

		NewWorkflowSnapshot: newSnapshot,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: currentMutation,
		CurrentWorkflowEvents:   nil,
	})
	s.NoError(err)

	s.AssertEqualWithDB(resetSnapshot)
	s.AssertEqualWithDB(newSnapshot)
	s.AssertEqualWithDB(currentSnapshot, currentMutation)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_ResetCurrent() {
	baseSnapshot := s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	resetSnapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		baseSnapshot.DBRecordVersion+1,
	)
	_, err := s.ExecutionManager.ConflictResolveWorkflowExecution(s.Ctx, &p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.ConflictResolveWorkflowModeUpdateCurrent,

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: nil,
		CurrentWorkflowEvents:   nil,
	})
	s.NoError(err)

	s.AssertEqualWithDB(resetSnapshot)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_ResetCurrent_CurrentConflict() {
	_ = s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	runID := uuid.New().String()
	baseSnapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeBypassCurrent,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *baseSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	resetSnapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		baseSnapshot.DBRecordVersion+1,
	)
	_, err = s.ExecutionManager.ConflictResolveWorkflowExecution(s.Ctx, &p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.ConflictResolveWorkflowModeUpdateCurrent,

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: nil,
		CurrentWorkflowEvents:   nil,
	})
	s.IsType(&p.CurrentWorkflowConditionFailedError{}, err)

	s.AssertEqualWithDB(baseSnapshot)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_ResetCurrent_Conflict() {
	baseSnapshot := s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	resetSnapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	_, err := s.ExecutionManager.ConflictResolveWorkflowExecution(s.Ctx, &p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.ConflictResolveWorkflowModeUpdateCurrent,

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: nil,
		CurrentWorkflowEvents:   nil,
	})
	s.IsType(&p.WorkflowConditionFailedError{}, err)

	s.AssertEqualWithDB(baseSnapshot)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_ResetCurrent_WithNew() {
	baseSnapshot := s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	resetSnapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		baseSnapshot.DBRecordVersion+1,
	)
	newSnapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		uuid.New().String(),
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	_, err := s.ExecutionManager.ConflictResolveWorkflowExecution(s.Ctx, &p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.ConflictResolveWorkflowModeUpdateCurrent,

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   nil,

		NewWorkflowSnapshot: newSnapshot,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: nil,
		CurrentWorkflowEvents:   nil,
	})
	s.NoError(err)

	s.AssertEqualWithDB(resetSnapshot)
	s.AssertEqualWithDB(newSnapshot)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_Zombie() {
	_ = s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	runID := uuid.New().String()
	baseSnapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeBypassCurrent,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *baseSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	resetSnapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		baseSnapshot.DBRecordVersion+1,
	)
	_, err = s.ExecutionManager.ConflictResolveWorkflowExecution(s.Ctx, &p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.ConflictResolveWorkflowModeBypassCurrent,

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: nil,
		CurrentWorkflowEvents:   nil,
	})
	s.NoError(err)

	s.AssertEqualWithDB(resetSnapshot)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_Zombie_CurrentConflict() {
	baseSnapshot := s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	resetSnapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		baseSnapshot.DBRecordVersion+1,
	)
	_, err := s.ExecutionManager.ConflictResolveWorkflowExecution(s.Ctx, &p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.ConflictResolveWorkflowModeBypassCurrent,

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: nil,
		CurrentWorkflowEvents:   nil,
	})
	s.IsType(&p.CurrentWorkflowConditionFailedError{}, err)

	s.AssertEqualWithDB(baseSnapshot)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_Zombie_Conflict() {
	_ = s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	runID := uuid.New().String()
	baseSnapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeBypassCurrent,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *baseSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	resetSnapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	_, err = s.ExecutionManager.ConflictResolveWorkflowExecution(s.Ctx, &p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.ConflictResolveWorkflowModeBypassCurrent,

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   nil,

		NewWorkflowSnapshot: nil,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: nil,
		CurrentWorkflowEvents:   nil,
	})
	s.IsType(&p.WorkflowConditionFailedError{}, err)

	s.AssertEqualWithDB(baseSnapshot)
}

func (s *ExecutionMutableStateSuite) TestConflictResolve_Zombie_WithNew() {
	_ = s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	runID := uuid.New().String()
	baseSnapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeBypassCurrent,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *baseSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	resetSnapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		baseSnapshot.DBRecordVersion+1,
	)
	newSnapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		uuid.New().String(),
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	_, err = s.ExecutionManager.ConflictResolveWorkflowExecution(s.Ctx, &p.ConflictResolveWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.ConflictResolveWorkflowModeBypassCurrent,

		ResetWorkflowSnapshot: *resetSnapshot,
		ResetWorkflowEvents:   nil,

		NewWorkflowSnapshot: newSnapshot,
		NewWorkflowEvents:   nil,

		CurrentWorkflowMutation: nil,
		CurrentWorkflowEvents:   nil,
	})
	s.NoError(err)

	s.AssertEqualWithDB(resetSnapshot)
	s.AssertEqualWithDB(newSnapshot)
}

func (s *ExecutionMutableStateSuite) TestSet_NotExists() {
	setSnapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	_, err := s.ExecutionManager.SetWorkflowExecution(s.Ctx, &p.SetWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,

		SetWorkflowSnapshot: *setSnapshot,
	})
	s.IsType(&p.ConditionFailedError{}, err)

	s.AssertMissingFromDB(s.NamespaceID, s.WorkflowID, s.RunID)
}

func (s *ExecutionMutableStateSuite) TestSet_Conflict() {
	snapshot := s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	setSnapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)
	_, err := s.ExecutionManager.SetWorkflowExecution(s.Ctx, &p.SetWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,

		SetWorkflowSnapshot: *setSnapshot,
	})
	s.IsType(&p.WorkflowConditionFailedError{}, err)

	s.AssertEqualWithDB(snapshot)
}

func (s *ExecutionMutableStateSuite) TestSet() {
	snapshot := s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	setSnapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		snapshot.DBRecordVersion+1,
	)
	_, err := s.ExecutionManager.SetWorkflowExecution(s.Ctx, &p.SetWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,

		SetWorkflowSnapshot: *setSnapshot,
	})
	s.NoError(err)

	s.AssertEqualWithDB(setSnapshot)
}

func (s *ExecutionMutableStateSuite) TestDeleteCurrent_IsCurrent() {
	newSnapshot := s.CreateWorkflow(
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	err := s.ExecutionManager.DeleteCurrentWorkflowExecution(s.Ctx, &p.DeleteCurrentWorkflowExecutionRequest{
		ShardID:     s.ShardID,
		NamespaceID: s.NamespaceID,
		WorkflowID:  s.WorkflowID,
		RunID:       s.RunID,
	})
	s.NoError(err)

	_, err = s.ExecutionManager.GetCurrentExecution(s.Ctx, &p.GetCurrentExecutionRequest{
		ShardID:     s.ShardID,
		NamespaceID: s.NamespaceID,
		WorkflowID:  s.WorkflowID,
	})
	s.IsType(&serviceerror.NotFound{}, err)

	s.AssertEqualWithDB(newSnapshot)
}

func (s *ExecutionMutableStateSuite) TestDeleteCurrent_NotCurrent() {
	newSnapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeBypassCurrent,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *newSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	err = s.ExecutionManager.DeleteCurrentWorkflowExecution(s.Ctx, &p.DeleteCurrentWorkflowExecutionRequest{
		ShardID:     s.ShardID,
		NamespaceID: s.NamespaceID,
		WorkflowID:  s.WorkflowID,
		RunID:       s.RunID,
	})
	s.NoError(err)

	_, err = s.ExecutionManager.GetCurrentExecution(s.Ctx, &p.GetCurrentExecutionRequest{
		ShardID:     s.ShardID,
		NamespaceID: s.NamespaceID,
		WorkflowID:  s.WorkflowID,
	})
	s.IsType(&serviceerror.NotFound{}, err)

	s.AssertEqualWithDB(newSnapshot)
}

func (s *ExecutionMutableStateSuite) TestDelete_Exists() {
	newSnapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rand.Int63(),
	)

	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeBypassCurrent,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *newSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	err = s.ExecutionManager.DeleteWorkflowExecution(s.Ctx, &p.DeleteWorkflowExecutionRequest{
		ShardID:     s.ShardID,
		NamespaceID: s.NamespaceID,
		WorkflowID:  s.WorkflowID,
		RunID:       s.RunID,
	})
	s.NoError(err)

	s.AssertMissingFromDB(s.NamespaceID, s.WorkflowID, s.RunID)
}

func (s *ExecutionMutableStateSuite) TestDelete_NotExists() {
	err := s.ExecutionManager.DeleteWorkflowExecution(s.Ctx, &p.DeleteWorkflowExecutionRequest{
		ShardID:     s.ShardID,
		NamespaceID: s.NamespaceID,
		WorkflowID:  s.WorkflowID,
		RunID:       s.RunID,
	})
	s.NoError(err)

	s.AssertMissingFromDB(s.NamespaceID, s.WorkflowID, s.RunID)
}

func (s *ExecutionMutableStateSuite) CreateWorkflow(
	lastWriteVersion int64,
	state enumsspb.WorkflowExecutionState,
	status enumspb.WorkflowExecutionStatus,
	dbRecordVersion int64,
) *p.WorkflowSnapshot {
	snapshot := RandomSnapshot(
		s.NamespaceID,
		s.WorkflowID,
		s.RunID,
		lastWriteVersion,
		state,
		status,
		dbRecordVersion,
	)
	_, err := s.ExecutionManager.CreateWorkflowExecution(s.Ctx, &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardID,
		RangeID: s.RangeID,
		Mode:    p.CreateWorkflowModeBrandNew,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *snapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)
	return snapshot
}

func (s *ExecutionMutableStateSuite) AssertMissingFromDB(
	namespaceID string,
	workflowID string,
	runID string,
) {
	_, err := s.ExecutionManager.GetWorkflowExecution(s.Ctx, &p.GetWorkflowExecutionRequest{
		ShardID:     s.ShardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	})
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *ExecutionMutableStateSuite) AssertEqualWithDB(
	snapshot *p.WorkflowSnapshot,
	mutations ...*p.WorkflowMutation,
) {
	resp, err := s.ExecutionManager.GetWorkflowExecution(s.Ctx, &p.GetWorkflowExecutionRequest{
		ShardID:     s.ShardID,
		NamespaceID: snapshot.ExecutionInfo.NamespaceId,
		WorkflowID:  snapshot.ExecutionInfo.WorkflowId,
		RunID:       snapshot.ExecutionState.RunId,
	})
	s.NoError(err)

	actualMutableState := resp.State
	actualDBRecordVersion := resp.DBRecordVersion

	expectedMutableState, expectedDBRecordVersion := s.Accumulate(snapshot, mutations...)

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

func (s *ExecutionMutableStateSuite) Accumulate(
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
