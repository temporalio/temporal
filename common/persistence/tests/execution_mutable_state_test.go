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
	snapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		s.runID,
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
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
}

func (s *executionMutableStateSuite) TestCreate_BrandNew_CurrentConflict() {
	lastWriteVersion := rand.Int63()
	snapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		s.runID,
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

		NewWorkflowSnapshot: *snapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	_, err = s.executionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.CreateWorkflowModeBrandNew,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *snapshot,
		NewWorkflowEvents:   nil,
	})
	if err, ok := err.(*p.CurrentWorkflowConditionFailedError); ok {
		err.Msg = ""
	}
	s.Equal(&p.CurrentWorkflowConditionFailedError{
		Msg:              "",
		RequestID:        snapshot.ExecutionState.CreateRequestId,
		RunID:            snapshot.ExecutionState.RunId,
		State:            snapshot.ExecutionState.State,
		Status:           snapshot.ExecutionState.Status,
		LastWriteVersion: lastWriteVersion,
	}, err)
}

func (s *executionMutableStateSuite) TestCreate_Reuse() {
	prevLastWriteVersion := rand.Int63()
	prevSnapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		s.runID,
		prevLastWriteVersion,
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

		NewWorkflowSnapshot: *prevSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	snapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		uuid.New().String(),
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)

	_, err = s.executionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.CreateWorkflowModeWorkflowIDReuse,

		PreviousRunID:            prevSnapshot.ExecutionState.RunId,
		PreviousLastWriteVersion: prevLastWriteVersion,

		NewWorkflowSnapshot: *snapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)
}

func (s *executionMutableStateSuite) TestCreate_Reuse_CurrentConflict() {
	lastWriteVersion := rand.Int63()
	snapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		s.runID,
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

		NewWorkflowSnapshot: *snapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	_, err = s.executionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.CreateWorkflowModeWorkflowIDReuse,

		PreviousRunID:            uuid.New().String(),
		PreviousLastWriteVersion: rand.Int63(),

		NewWorkflowSnapshot: *snapshot,
		NewWorkflowEvents:   nil,
	})
	if err, ok := err.(*p.CurrentWorkflowConditionFailedError); ok {
		err.Msg = ""
	}
	s.Equal(&p.CurrentWorkflowConditionFailedError{
		Msg:              "",
		RequestID:        snapshot.ExecutionState.CreateRequestId,
		RunID:            snapshot.ExecutionState.RunId,
		State:            snapshot.ExecutionState.State,
		Status:           snapshot.ExecutionState.Status,
		LastWriteVersion: lastWriteVersion,
	}, err)
}

func (s *executionMutableStateSuite) TestCreate_Zombie() {
	prevLastWriteVersion := rand.Int63()
	prevSnapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		s.runID,
		prevLastWriteVersion,
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

		NewWorkflowSnapshot: *prevSnapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	snapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		uuid.New().String(),
		rand.Int63(),
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		int64(1),
	)

	_, err = s.executionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.CreateWorkflowModeZombie,

		PreviousRunID:            "",
		PreviousLastWriteVersion: 0,

		NewWorkflowSnapshot: *snapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)
}

func (s *executionMutableStateSuite) TestCreate_Conflict() {
	lastWriteVersion := rand.Int63()
	snapshot := randomSnapshot(
		s.namespaceID,
		s.workflowID,
		s.runID,
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

		NewWorkflowSnapshot: *snapshot,
		NewWorkflowEvents:   nil,
	})
	s.NoError(err)

	_, err = s.executionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		ShardID: s.shardID,
		RangeID: s.rangeID,
		Mode:    p.CreateWorkflowModeWorkflowIDReuse,

		PreviousRunID:            snapshot.ExecutionState.RunId,
		PreviousLastWriteVersion: lastWriteVersion,

		NewWorkflowSnapshot: *snapshot,
		NewWorkflowEvents:   nil,
	})
	s.IsType(&p.WorkflowConditionFailedError{}, err)
}

func (s *executionMutableStateSuite) TestUpdate() {}

func (s *executionMutableStateSuite) TestConflictResolve() {}

func (s *executionMutableStateSuite) TestDeleteCurrent() {}

func (s *executionMutableStateSuite) TestDelete() {}
