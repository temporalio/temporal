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

package persistencetests

import (
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/tasks"
)

type (
	// ExecutionManagerSuite contains matching persistence tests
	ExecutionManagerSuite struct {
		TestBase
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
	}
)

var testWorkflowChecksum = &persistencespb.Checksum{
	Version: 22,
	Flavor:  enumsspb.CHECKSUM_FLAVOR_IEEE_CRC32_OVER_PROTO3_BINARY,
	Value:   []byte("test-checksum"),
}

// SetupSuite implementation
func (s *ExecutionManagerSuite) SetupSuite() {
}

// TearDownSuite implementation
func (s *ExecutionManagerSuite) TearDownSuite() {
	s.TearDownWorkflowStore()
}

// SetupTest implementation
func (s *ExecutionManagerSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
	s.ClearTasks()
}

func (s *ExecutionManagerSuite) newRandomChecksum() *persistencespb.Checksum {
	return &persistencespb.Checksum{
		Flavor:  enumsspb.CHECKSUM_FLAVOR_IEEE_CRC32_OVER_PROTO3_BINARY,
		Version: 22,
		Value:   uuid.NewRandom(),
	}
}

func (s *ExecutionManagerSuite) assertChecksumsEqual(expected *persistencespb.Checksum, actual *persistencespb.Checksum) {
	if actual.GetFlavor() != enumsspb.CHECKSUM_FLAVOR_IEEE_CRC32_OVER_PROTO3_BINARY {
		// not all stores support checksum persistence today
		// if its not supported, assert that everything is zero'd out
		expected = nil
	}
	s.Equal(expected, actual)
}

// TestCreateWorkflowExecutionDeDup test
func (s *ExecutionManagerSuite) TestCreateWorkflowExecutionDeDup() {
	namespaceID := uuid.New()
	workflowID := "create-workflow-test-dedup"
	runID := "3969fae6-6b75-4c2a-b74b-4054edd296a6"
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}
	taskqueue := "some random taskqueue"
	workflowType := "some random workflow type"
	workflowTimeout := timestamp.DurationFromSeconds(10)
	workflowTaskTimeout := timestamp.DurationFromSeconds(14)
	lastProcessedEventID := int64(0)
	nextEventID := int64(3)
	csum := s.newRandomChecksum()

	req := &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardInfo.GetShardId(),
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				NamespaceId:                namespaceID,
				WorkflowId:                 workflowID,
				TaskQueue:                  taskqueue,
				WorkflowTypeName:           workflowType,
				WorkflowRunTimeout:         workflowTimeout,
				DefaultWorkflowTaskTimeout: workflowTaskTimeout,
				LastFirstEventId:           common.FirstEventID,
				LastWorkflowTaskStartId:    lastProcessedEventID,
				ExecutionStats:             &persistencespb.ExecutionStats{},
				StartTime:                  timestamp.TimeNowPtrUtc(),
				LastUpdateTime:             timestamp.TimeNowPtrUtc(),
			},
			NextEventID: nextEventID,
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId:           runID,
				CreateRequestId: uuid.New(),
				State:           enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
				Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
			Checksum: csum,
		},
		RangeID: s.ShardInfo.GetRangeId(),
		Mode:    p.CreateWorkflowModeBrandNew,
	}

	_, err := s.ExecutionManager.CreateWorkflowExecution(req)
	s.Nil(err)
	info, err := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.Nil(err)
	s.assertChecksumsEqual(csum, info.Checksum)
	updatedInfo := copyWorkflowExecutionInfo(info.ExecutionInfo)
	updatedState := copyWorkflowExecutionState(info.ExecutionState)
	updatedState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	updatedState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	_, err = s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		ShardID: s.ShardInfo.GetShardId(),
		UpdateWorkflowMutation: p.WorkflowMutation{
			ExecutionInfo:  updatedInfo,
			ExecutionState: updatedState,
			NextEventID:    info.NextEventId,
			Condition:      nextEventID,
		},
		RangeID: s.ShardInfo.GetRangeId(),
		Mode:    p.UpdateWorkflowModeUpdateCurrent,
	})
	s.NoError(err)

	req.Mode = p.CreateWorkflowModeWorkflowIDReuse
	req.PreviousRunID = runID
	req.PreviousLastWriteVersion = common.EmptyVersion
	_, err = s.ExecutionManager.CreateWorkflowExecution(req)
	s.Error(err)
	s.IsType(&p.WorkflowConditionFailedError{}, err)
}

// TestCreateWorkflowExecutionStateStatus test
func (s *ExecutionManagerSuite) TestCreateWorkflowExecutionStateStatus() {
	namespaceID := uuid.New()
	invalidStatuses := []enumspb.WorkflowExecutionStatus{
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
		enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
	}
	taskqueue := "some random taskqueue"
	workflowType := "some random workflow type"
	workflowTimeout := timestamp.DurationFromSeconds(10)
	workflowTaskTimeout := timestamp.DurationFromSeconds(14)
	lastProcessedEventID := int64(0)
	nextEventID := int64(3)
	csum := s.newRandomChecksum()

	req := &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardInfo.GetShardId(),
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				NamespaceId:                namespaceID,
				TaskQueue:                  taskqueue,
				WorkflowTypeName:           workflowType,
				WorkflowRunTimeout:         workflowTimeout,
				DefaultWorkflowTaskTimeout: workflowTaskTimeout,
				LastFirstEventId:           common.FirstEventID,
				LastWorkflowTaskStartId:    lastProcessedEventID,
				ExecutionStats:             &persistencespb.ExecutionStats{},
				StartTime:                  timestamp.TimeNowPtrUtc(),
				LastUpdateTime:             timestamp.TimeNowPtrUtc(),
			},
			NextEventID: nextEventID,
			ExecutionState: &persistencespb.WorkflowExecutionState{
				CreateRequestId: uuid.New(),
			},
			Checksum: csum,
		},
		RangeID: s.ShardInfo.GetRangeId(),
		Mode:    p.CreateWorkflowModeBrandNew,
	}

	workflowExecutionStatusCreated := commonpb.WorkflowExecution{
		WorkflowId: "create-workflow-test-state-created",
		RunId:      uuid.New(),
	}
	req.NewWorkflowSnapshot.ExecutionInfo.WorkflowId = workflowExecutionStatusCreated.GetWorkflowId()
	req.NewWorkflowSnapshot.ExecutionState.RunId = workflowExecutionStatusCreated.GetRunId()
	req.NewWorkflowSnapshot.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_CREATED
	for _, invalidStatus := range invalidStatuses {
		req.NewWorkflowSnapshot.ExecutionState.Status = invalidStatus
		_, err := s.ExecutionManager.CreateWorkflowExecution(req)
		s.IsType(&serviceerror.Internal{}, err)
	}
	req.NewWorkflowSnapshot.ExecutionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	_, err := s.ExecutionManager.CreateWorkflowExecution(req)
	s.Nil(err)
	info, err := s.GetWorkflowMutableState(namespaceID, workflowExecutionStatusCreated)
	s.Nil(err)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_CREATED, info.ExecutionState.State)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.ExecutionState.Status)
	s.assertChecksumsEqual(csum, info.Checksum)

	workflowExecutionStatusRunning := commonpb.WorkflowExecution{
		WorkflowId: "create-workflow-test-state-running",
		RunId:      uuid.New(),
	}
	req.NewWorkflowSnapshot.ExecutionInfo.WorkflowId = workflowExecutionStatusRunning.GetWorkflowId()
	req.NewWorkflowSnapshot.ExecutionState.RunId = workflowExecutionStatusRunning.GetRunId()
	req.NewWorkflowSnapshot.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING
	for _, invalidStatus := range invalidStatuses {
		req.NewWorkflowSnapshot.ExecutionState.Status = invalidStatus
		_, err := s.ExecutionManager.CreateWorkflowExecution(req)
		s.IsType(&serviceerror.Internal{}, err)
	}
	req.NewWorkflowSnapshot.ExecutionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	_, err = s.ExecutionManager.CreateWorkflowExecution(req)
	s.Nil(err)
	info, err = s.GetWorkflowMutableState(namespaceID, workflowExecutionStatusRunning)
	s.Nil(err)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, info.ExecutionState.State)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.ExecutionState.Status)
	s.assertChecksumsEqual(csum, info.Checksum)

	// for zombie workflow creation, we must use existing workflow ID which got created
	// since we do not allow creation of zombie workflow without current record
	workflowExecutionStatusZombie := commonpb.WorkflowExecution{
		WorkflowId: workflowExecutionStatusRunning.WorkflowId,
		RunId:      uuid.New(),
	}
	req.Mode = p.CreateWorkflowModeZombie
	req.NewWorkflowSnapshot.ExecutionInfo.WorkflowId = workflowExecutionStatusZombie.GetWorkflowId()
	req.NewWorkflowSnapshot.ExecutionState.RunId = workflowExecutionStatusZombie.GetRunId()
	req.NewWorkflowSnapshot.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE
	for _, invalidStatus := range invalidStatuses {
		req.NewWorkflowSnapshot.ExecutionState.Status = invalidStatus
		_, err := s.ExecutionManager.CreateWorkflowExecution(req)
		s.IsType(&serviceerror.Internal{}, err)
	}
	req.NewWorkflowSnapshot.ExecutionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	_, err = s.ExecutionManager.CreateWorkflowExecution(req)
	s.Nil(err)
	info, err = s.GetWorkflowMutableState(namespaceID, workflowExecutionStatusZombie)
	s.Nil(err)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE, info.ExecutionState.State)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.ExecutionState.Status)
	s.assertChecksumsEqual(csum, info.Checksum)
}

// TestCreateWorkflowExecutionWithZombieState test
func (s *ExecutionManagerSuite) TestCreateWorkflowExecutionWithZombieState() {
	namespaceID := uuid.New()
	workflowID := "create-workflow-test-with-zombie-state"
	workflowExecutionZombie1 := commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      uuid.New(),
	}
	taskqueue := "some random taskqueue"
	workflowType := "some random workflow type"
	workflowTimeout := timestamp.DurationFromSeconds(10)
	workflowTaskTimeout := timestamp.DurationFromSeconds(14)
	lastProcessedEventID := int64(0)
	nextEventID := int64(3)
	csum := s.newRandomChecksum()

	req := &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardInfo.GetShardId(),
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				NamespaceId:                namespaceID,
				WorkflowId:                 workflowID,
				TaskQueue:                  taskqueue,
				WorkflowTypeName:           workflowType,
				WorkflowRunTimeout:         workflowTimeout,
				DefaultWorkflowTaskTimeout: workflowTaskTimeout,
				LastWorkflowTaskStartId:    lastProcessedEventID,
				ExecutionStats:             &persistencespb.ExecutionStats{},
				StartTime:                  timestamp.TimeNowPtrUtc(),
				LastUpdateTime:             timestamp.TimeNowPtrUtc(),
			},
			NextEventID: nextEventID,
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId:           workflowExecutionZombie1.GetRunId(),
				CreateRequestId: uuid.New(),
				State:           enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
				Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
			Checksum: csum,
		},
		RangeID: s.ShardInfo.GetRangeId(),
		Mode:    p.CreateWorkflowModeZombie,
	}
	_, err := s.ExecutionManager.CreateWorkflowExecution(req)
	s.Nil(err) // allow creating a zombie workflow if no current running workflow
	_, err = s.GetCurrentWorkflowRunID(namespaceID, workflowID)
	s.IsType(&serviceerror.NotFound{}, err) // no current workflow

	workflowExecutionRunning := commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      uuid.New(),
	}
	req.NewWorkflowSnapshot.ExecutionState.RunId = workflowExecutionRunning.GetRunId()
	req.Mode = p.CreateWorkflowModeBrandNew
	req.NewWorkflowSnapshot.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING
	req.NewWorkflowSnapshot.ExecutionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	_, err = s.ExecutionManager.CreateWorkflowExecution(req)
	s.Nil(err)
	currentRunID, err := s.GetCurrentWorkflowRunID(namespaceID, workflowID)
	s.Nil(err)
	s.Equal(workflowExecutionRunning.GetRunId(), currentRunID)

	workflowExecutionZombie := commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      uuid.New(),
	}
	req.NewWorkflowSnapshot.ExecutionState.RunId = workflowExecutionZombie.GetRunId()
	req.Mode = p.CreateWorkflowModeZombie
	req.NewWorkflowSnapshot.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE
	req.NewWorkflowSnapshot.ExecutionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	_, err = s.ExecutionManager.CreateWorkflowExecution(req)
	s.Nil(err)
	// current run ID is still the prev running run ID
	currentRunID, err = s.GetCurrentWorkflowRunID(namespaceID, workflowExecutionRunning.GetWorkflowId())
	s.Nil(err)
	s.Equal(workflowExecutionRunning.GetRunId(), currentRunID)
	info, err := s.GetWorkflowMutableState(namespaceID, workflowExecutionZombie)
	s.Nil(err)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE, info.ExecutionState.State)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.ExecutionState.Status)
	s.assertChecksumsEqual(csum, info.Checksum)
}

// TestUpdateWorkflowExecutionStateStatus test
func (s *ExecutionManagerSuite) TestUpdateWorkflowExecutionStateStatus() {
	namespaceID := uuid.New()
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "update-workflow-test-state",
		RunId:      uuid.New(),
	}
	statuses := []enumspb.WorkflowExecutionStatus{
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
		enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
	}
	taskqueue := "some random taskqueue"
	workflowType := "some random workflow type"
	workflowTimeout := timestamp.DurationFromSeconds(10)
	workflowTaskTimeout := timestamp.DurationFromSeconds(14)
	lastProcessedEventID := int64(0)
	nextEventID := int64(3)
	csum := s.newRandomChecksum()

	req := &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardInfo.GetShardId(),
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				NamespaceId:                namespaceID,
				WorkflowId:                 workflowExecution.GetWorkflowId(),
				TaskQueue:                  taskqueue,
				WorkflowTypeName:           workflowType,
				WorkflowRunTimeout:         workflowTimeout,
				DefaultWorkflowTaskTimeout: workflowTaskTimeout,
				LastFirstEventId:           common.FirstEventID,
				LastWorkflowTaskStartId:    lastProcessedEventID,
				ExecutionStats:             &persistencespb.ExecutionStats{},
				StartTime:                  timestamp.TimeNowPtrUtc(),
				LastUpdateTime:             timestamp.TimeNowPtrUtc(),
			},
			NextEventID: nextEventID,
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId:           workflowExecution.GetRunId(),
				CreateRequestId: uuid.New(),
			},
			Checksum: csum,
		},
		RangeID: s.ShardInfo.GetRangeId(),
		Mode:    p.CreateWorkflowModeBrandNew,
	}

	req.NewWorkflowSnapshot.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_CREATED
	req.NewWorkflowSnapshot.ExecutionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	_, err := s.ExecutionManager.CreateWorkflowExecution(req)
	s.Nil(err)
	state, err := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.Nil(err)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_CREATED, state.ExecutionState.State)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, state.ExecutionState.Status)
	s.assertChecksumsEqual(csum, state.Checksum)

	csum = s.newRandomChecksum() // update the checksum to new value
	updatedInfo := copyWorkflowExecutionInfo(state.ExecutionInfo)
	updatedState := copyWorkflowExecutionState(state.ExecutionState)
	updatedState.State = enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING
	updatedState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	_, err = s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		ShardID: s.ShardInfo.GetShardId(),
		UpdateWorkflowMutation: p.WorkflowMutation{
			ExecutionInfo:  updatedInfo,
			ExecutionState: updatedState,
			NextEventID:    state.NextEventId,
			Condition:      nextEventID,
			Checksum:       csum,
		},
		RangeID: s.ShardInfo.GetRangeId(),
		Mode:    p.UpdateWorkflowModeUpdateCurrent,
	})
	s.NoError(err)
	state, err = s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.Nil(err)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, state.ExecutionState.State)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, state.ExecutionState.Status)
	s.assertChecksumsEqual(csum, state.Checksum)

	updatedInfo = copyWorkflowExecutionInfo(state.ExecutionInfo)
	updatedState = copyWorkflowExecutionState(state.ExecutionState)
	updatedState.State = enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING
	for _, status := range statuses {
		updatedState.Status = status
		_, err = s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
			ShardID: s.ShardInfo.GetShardId(),
			UpdateWorkflowMutation: p.WorkflowMutation{
				ExecutionInfo:  updatedInfo,
				ExecutionState: updatedState,
				NextEventID:    state.NextEventId,
				Condition:      nextEventID,
			},
			RangeID: s.ShardInfo.GetRangeId(),
			Mode:    p.UpdateWorkflowModeUpdateCurrent,
		})
		s.IsType(&serviceerror.Internal{}, err)
	}

	updatedInfo = copyWorkflowExecutionInfo(state.ExecutionInfo)
	updatedState = copyWorkflowExecutionState(state.ExecutionState)
	updatedState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	updatedState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	_, err = s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		ShardID: s.ShardInfo.GetShardId(),
		UpdateWorkflowMutation: p.WorkflowMutation{
			ExecutionInfo:  updatedInfo,
			ExecutionState: updatedState,
			NextEventID:    state.NextEventId,
			Condition:      nextEventID,
		},
		RangeID: s.ShardInfo.GetRangeId(),
		Mode:    p.UpdateWorkflowModeUpdateCurrent,
	})
	s.IsType(&serviceerror.Internal{}, err)

	for _, status := range statuses {
		updatedState.Status = status
		_, err = s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
			ShardID: s.ShardInfo.GetShardId(),
			UpdateWorkflowMutation: p.WorkflowMutation{
				ExecutionInfo:  updatedInfo,
				ExecutionState: updatedState,
				NextEventID:    state.NextEventId,
				Condition:      nextEventID,
			},
			RangeID: s.ShardInfo.GetRangeId(),
			Mode:    p.UpdateWorkflowModeUpdateCurrent,
		})
		s.Nil(err)
		state, err = s.GetWorkflowMutableState(namespaceID, workflowExecution)
		s.Nil(err)
		s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, state.ExecutionState.State)
		s.EqualValues(status, state.ExecutionState.Status)
	}

	// create a new workflow with same namespace ID & workflow ID
	// to enable update workflow with zombie status
	workflowExecutionRunning := commonpb.WorkflowExecution{
		WorkflowId: workflowExecution.WorkflowId,
		RunId:      uuid.New(),
	}
	req.NewWorkflowSnapshot.ExecutionInfo.WorkflowId = workflowExecutionRunning.GetWorkflowId()
	req.NewWorkflowSnapshot.ExecutionState.RunId = workflowExecutionRunning.GetRunId()
	req.Mode = p.CreateWorkflowModeWorkflowIDReuse
	req.PreviousRunID = workflowExecution.GetRunId()
	req.PreviousLastWriteVersion = common.EmptyVersion
	req.NewWorkflowSnapshot.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING
	req.NewWorkflowSnapshot.ExecutionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	_, err = s.ExecutionManager.CreateWorkflowExecution(req)
	s.Nil(err)

	updatedInfo = copyWorkflowExecutionInfo(state.ExecutionInfo)
	updatedState = copyWorkflowExecutionState(state.ExecutionState)
	updatedState.State = enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE
	updatedState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	_, err = s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		ShardID: s.ShardInfo.GetShardId(),
		UpdateWorkflowMutation: p.WorkflowMutation{
			ExecutionInfo:  updatedInfo,
			ExecutionState: updatedState,
			NextEventID:    state.NextEventId,
			Condition:      nextEventID,
		},
		RangeID: s.ShardInfo.GetRangeId(),
		Mode:    p.UpdateWorkflowModeBypassCurrent,
	})
	s.NoError(err)
	state, err = s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.Nil(err)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE, state.ExecutionState.State)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, state.ExecutionState.Status)

	updatedInfo = copyWorkflowExecutionInfo(state.ExecutionInfo)
	updatedState = copyWorkflowExecutionState(state.ExecutionState)
	updatedState.State = enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE
	for _, status := range statuses {
		updatedState.Status = status
		_, err = s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
			ShardID: s.ShardInfo.GetShardId(),
			UpdateWorkflowMutation: p.WorkflowMutation{
				ExecutionInfo:  updatedInfo,
				ExecutionState: updatedState,
				NextEventID:    state.NextEventId,
				Condition:      nextEventID,
			},
			RangeID: s.ShardInfo.GetRangeId(),
			Mode:    p.UpdateWorkflowModeBypassCurrent,
		})
		s.IsType(&serviceerror.Internal{}, err)
	}
}

// TestUpdateWorkflowExecutionWithZombieState test
func (s *ExecutionManagerSuite) TestUpdateWorkflowExecutionWithZombieState() {
	namespaceID := uuid.New()
	workflowID := "create-workflow-test-with-zombie-state"
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      uuid.New(),
	}
	taskqueue := "some random taskqueue"
	workflowType := "some random workflow type"
	workflowTimeout := timestamp.DurationFromSeconds(10)
	workflowTaskTimeout := timestamp.DurationFromSeconds(14)
	lastProcessedEventID := int64(0)
	nextEventID := int64(3)
	csum := s.newRandomChecksum()

	// create and update a workflow to make it completed
	req := &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardInfo.GetShardId(),
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				NamespaceId:                namespaceID,
				WorkflowId:                 workflowExecution.GetWorkflowId(),
				TaskQueue:                  taskqueue,
				WorkflowTypeName:           workflowType,
				WorkflowRunTimeout:         workflowTimeout,
				DefaultWorkflowTaskTimeout: workflowTaskTimeout,
				LastWorkflowTaskStartId:    lastProcessedEventID,
				ExecutionStats:             &persistencespb.ExecutionStats{},
				StartTime:                  timestamp.TimeNowPtrUtc(),
				LastUpdateTime:             timestamp.TimeNowPtrUtc(),
			},
			NextEventID: nextEventID,
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId:           workflowExecution.GetRunId(),
				CreateRequestId: uuid.New(),
				State:           enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
				Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
			Checksum: csum,
		},
		RangeID: s.ShardInfo.GetRangeId(),
		Mode:    p.CreateWorkflowModeBrandNew,
	}
	_, err := s.ExecutionManager.CreateWorkflowExecution(req)
	s.Nil(err)
	currentRunID, err := s.GetCurrentWorkflowRunID(namespaceID, workflowID)
	s.Nil(err)
	s.Equal(workflowExecution.GetRunId(), currentRunID)

	info, err := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.Nil(err)
	s.assertChecksumsEqual(csum, info.Checksum)

	// try to turn current workflow into zombie state, this should end with an error
	updatedInfo := copyWorkflowExecutionInfo(info.ExecutionInfo)
	updatedState := copyWorkflowExecutionState(info.ExecutionState)
	updatedState.State = enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE
	updatedState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	_, err = s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		ShardID: s.ShardInfo.GetShardId(),
		UpdateWorkflowMutation: p.WorkflowMutation{
			ExecutionInfo:  updatedInfo,
			ExecutionState: updatedState,
			NextEventID:    info.NextEventId,
			Condition:      nextEventID,
			Checksum:       csum,
		},
		RangeID: s.ShardInfo.GetRangeId(),
		Mode:    p.UpdateWorkflowModeBypassCurrent,
	})
	s.NotNil(err)

	updatedInfo = copyWorkflowExecutionInfo(info.ExecutionInfo)
	updatedState = copyWorkflowExecutionState(info.ExecutionState)
	updatedState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	updatedState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	_, err = s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		ShardID: s.ShardInfo.GetShardId(),
		UpdateWorkflowMutation: p.WorkflowMutation{
			ExecutionInfo:  updatedInfo,
			ExecutionState: updatedState,
			NextEventID:    info.NextEventId,
			Condition:      nextEventID,
		},
		RangeID: s.ShardInfo.GetRangeId(),
		Mode:    p.UpdateWorkflowModeUpdateCurrent,
	})
	s.NoError(err)

	// create a new workflow with same namespace ID & workflow ID
	workflowExecutionRunning := commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      uuid.New(),
	}
	csum = nil
	req.NewWorkflowSnapshot.ExecutionInfo.WorkflowId = workflowExecutionRunning.GetWorkflowId()
	req.NewWorkflowSnapshot.ExecutionState.RunId = workflowExecutionRunning.GetRunId()
	req.Mode = p.CreateWorkflowModeWorkflowIDReuse
	req.PreviousRunID = workflowExecution.GetRunId()
	req.PreviousLastWriteVersion = common.EmptyVersion
	req.NewWorkflowSnapshot.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING
	req.NewWorkflowSnapshot.ExecutionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	req.NewWorkflowSnapshot.Checksum = csum
	_, err = s.ExecutionManager.CreateWorkflowExecution(req)
	s.Nil(err)
	currentRunID, err = s.GetCurrentWorkflowRunID(namespaceID, workflowID)
	s.Nil(err)
	s.Equal(workflowExecutionRunning.GetRunId(), currentRunID)

	// get the workflow to be turned into a zombie
	info, err = s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.Nil(err)
	s.assertChecksumsEqual(csum, info.Checksum)
	updatedInfo = copyWorkflowExecutionInfo(info.ExecutionInfo)
	updatedState = copyWorkflowExecutionState(info.ExecutionState)
	updatedState.State = enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE
	updatedState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	_, err = s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		ShardID: s.ShardInfo.GetShardId(),
		UpdateWorkflowMutation: p.WorkflowMutation{
			ExecutionInfo:  updatedInfo,
			ExecutionState: updatedState,
			NextEventID:    info.NextEventId,
			Condition:      nextEventID,
			Checksum:       csum,
		},
		RangeID: s.ShardInfo.GetRangeId(),
		Mode:    p.UpdateWorkflowModeBypassCurrent,
	})
	s.NoError(err)
	info, err = s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.Nil(err)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE, info.ExecutionState.State)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.ExecutionState.Status)
	s.assertChecksumsEqual(csum, info.Checksum)
	// check current run ID is un touched
	currentRunID, err = s.GetCurrentWorkflowRunID(namespaceID, workflowID)
	s.Nil(err)
	s.Equal(workflowExecutionRunning.GetRunId(), currentRunID)
}

// TestCreateWorkflowExecutionBrandNew test
func (s *ExecutionManagerSuite) TestCreateWorkflowExecutionBrandNew() {
	namespaceID := uuid.New()
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "create-workflow-test-brand-new",
		RunId:      uuid.New(),
	}
	taskqueue := "some random taskqueue"
	workflowType := "some random workflow type"
	workflowTimeout := timestamp.DurationFromSeconds(10)
	workflowTaskTimeout := timestamp.DurationFromSeconds(14)
	lastProcessedEventID := int64(0)
	nextEventID := int64(3)

	req := &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardInfo.GetShardId(),
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				NamespaceId:                namespaceID,
				WorkflowId:                 workflowExecution.GetWorkflowId(),
				TaskQueue:                  taskqueue,
				WorkflowTypeName:           workflowType,
				WorkflowRunTimeout:         workflowTimeout,
				DefaultWorkflowTaskTimeout: workflowTaskTimeout,
				LastFirstEventId:           common.FirstEventID,
				LastWorkflowTaskStartId:    lastProcessedEventID,
				ExecutionStats:             &persistencespb.ExecutionStats{},
				StartTime:                  timestamp.TimeNowPtrUtc(),
				LastUpdateTime:             timestamp.TimeNowPtrUtc(),
			},
			NextEventID: nextEventID,
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId:           workflowExecution.GetRunId(),
				CreateRequestId: uuid.New(),
				State:           enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
				Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
		},
		RangeID: s.ShardInfo.GetRangeId(),
		Mode:    p.CreateWorkflowModeBrandNew,
	}

	_, err := s.ExecutionManager.CreateWorkflowExecution(req)
	s.Nil(err)
	_, err = s.ExecutionManager.CreateWorkflowExecution(req)
	s.NotNil(err)
	alreadyStartedErr, ok := err.(*p.CurrentWorkflowConditionFailedError)
	s.True(ok, "err is not CurrentWorkflowConditionFailedError")
	s.Equal(req.NewWorkflowSnapshot.ExecutionState.CreateRequestId, alreadyStartedErr.RequestID)
	s.Equal(workflowExecution.GetRunId(), alreadyStartedErr.RunID)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, alreadyStartedErr.State)
}

// TestUpsertWorkflowActivity test
func (s *ExecutionManagerSuite) TestUpsertWorkflowActivity() {
	namespaceID := uuid.New()
	workflowID := "create-workflow-test-with-upsert-activity"
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      uuid.New(),
	}
	taskqueue := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := timestamp.DurationFromSeconds(10)
	workflowTaskTimeout := timestamp.DurationFromSeconds(14)
	lastProcessedEventID := int64(0)
	nextEventID := int64(3)
	csum := s.newRandomChecksum()

	// create and update a workflow to make it completed
	req := &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardInfo.GetShardId(),
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				NamespaceId:                namespaceID,
				WorkflowId:                 workflowExecution.GetWorkflowId(),
				TaskQueue:                  taskqueue,
				WorkflowTypeName:           workflowType,
				WorkflowRunTimeout:         workflowTimeout,
				DefaultWorkflowTaskTimeout: workflowTaskTimeout,
				LastFirstEventId:           common.FirstEventID,
				LastWorkflowTaskStartId:    lastProcessedEventID,
				ExecutionStats:             &persistencespb.ExecutionStats{},
				StartTime:                  timestamp.TimeNowPtrUtc(),
				LastUpdateTime:             timestamp.TimeNowPtrUtc(),
			},
			NextEventID: nextEventID,
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId:           workflowExecution.GetRunId(),
				CreateRequestId: uuid.New(),
				State:           enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
				Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
		},
		RangeID: s.ShardInfo.GetRangeId(),
		Mode:    p.CreateWorkflowModeBrandNew,
	}
	_, err := s.ExecutionManager.CreateWorkflowExecution(req)
	s.Nil(err)
	currentRunID, err := s.GetCurrentWorkflowRunID(namespaceID, workflowID)
	s.Nil(err)
	s.Equal(workflowExecution.GetRunId(), currentRunID)

	info, err := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.Nil(err)
	s.assertChecksumsEqual(csum, info.Checksum)
	s.Equal(0, len(info.ActivityInfos))

	// insert a new activity
	updatedInfo := copyWorkflowExecutionInfo(info.ExecutionInfo)
	updatedState := copyWorkflowExecutionState(info.ExecutionState)
	_, err = s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		ShardID: s.ShardInfo.GetShardId(),
		UpdateWorkflowMutation: p.WorkflowMutation{
			ExecutionInfo:  updatedInfo,
			ExecutionState: updatedState,
			NextEventID:    info.NextEventId,
			Condition:      nextEventID,
			Checksum:       csum,
			UpsertActivityInfos: map[int64]*persistencespb.ActivityInfo{
				100: {
					Version:    0,
					ScheduleId: 100,
					TaskQueue:  "test-activity-tasktlist-1",
				},
			},
		},
		RangeID: s.ShardInfo.RangeId,
		Mode:    p.UpdateWorkflowModeUpdateCurrent,
	})
	s.Nil(err)

	info2, err := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.Nil(err)
	s.Equal(1, len(info2.ActivityInfos))
	s.Equal("test-activity-tasktlist-1", info2.ActivityInfos[100].TaskQueue)

	// upsert the previous activity
	_, err = s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		ShardID: s.ShardInfo.GetShardId(),
		UpdateWorkflowMutation: p.WorkflowMutation{
			ExecutionInfo:  updatedInfo,
			ExecutionState: updatedState,
			NextEventID:    info.NextEventId,
			Condition:      nextEventID,
			Checksum:       csum,
			UpsertActivityInfos: map[int64]*persistencespb.ActivityInfo{
				100: {
					Version:    0,
					ScheduleId: 100,
					TaskQueue:  "test-activity-tasktlist-2",
				},
			},
		},
		RangeID: s.ShardInfo.RangeId,
		Mode:    p.UpdateWorkflowModeUpdateCurrent,
	})
	info3, err := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.Nil(err)
	s.Equal(1, len(info3.ActivityInfos))
	s.Equal("test-activity-tasktlist-2", info3.ActivityInfos[100].TaskQueue)
}

// TestCreateWorkflowExecutionRunIDReuseWithoutReplication test
func (s *ExecutionManagerSuite) TestCreateWorkflowExecutionRunIDReuseWithoutReplication() {
	namespaceID := uuid.New()
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "create-workflow-test-run-id-reuse-without-replication",
		RunId:      uuid.New(),
	}
	taskqueue := "some random taskqueue"
	workflowType := "some random workflow type"
	workflowTimeout := timestamp.DurationFromSeconds(10)
	workflowTaskTimeout := timestamp.DurationFromSeconds(14)
	lastProcessedEventID := int64(0)
	nextEventID := int64(3)
	workflowTaskScheduleID := int64(2)

	task0, err0 := s.CreateWorkflowExecution(namespaceID, workflowExecution, taskqueue, workflowType, workflowTimeout, workflowTaskTimeout, nextEventID, lastProcessedEventID, workflowTaskScheduleID, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err1)
	s.assertChecksumsEqual(testWorkflowChecksum, state0.Checksum)
	info0 := state0.ExecutionInfo
	closeInfo := copyWorkflowExecutionInfo(info0)
	closeState := copyWorkflowExecutionState(state0.ExecutionState)
	closeState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	closeState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	closeInfo.LastWorkflowTaskStartId = int64(2)

	err2 := s.UpdateWorkflowExecution(closeInfo, closeState, int64(5), nil, nil, nextEventID,
		nil, nil, nil, nil, nil)
	s.NoError(err2)

	newExecution := commonpb.WorkflowExecution{
		WorkflowId: workflowExecution.GetWorkflowId(),
		RunId:      uuid.New(),
	}
	// this create should work since we are relying the business logic in history engine
	// to check whether the existing running workflow has finished
	_, err3 := s.ExecutionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardInfo.GetShardId(),
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				NamespaceId:                namespaceID,
				WorkflowId:                 newExecution.GetWorkflowId(),
				TaskQueue:                  taskqueue,
				WorkflowTypeName:           workflowType,
				WorkflowRunTimeout:         workflowTimeout,
				DefaultWorkflowTaskTimeout: workflowTaskTimeout,
				LastFirstEventId:           common.FirstEventID,
				LastWorkflowTaskStartId:    lastProcessedEventID,
				ExecutionStats:             &persistencespb.ExecutionStats{},
				StartTime:                  timestamp.TimeNowPtrUtc(),
				LastUpdateTime:             timestamp.TimeNowPtrUtc(),
			},
			NextEventID: nextEventID,
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId:           newExecution.GetRunId(),
				CreateRequestId: uuid.New(),
				State:           enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
				Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
		},
		RangeID:                  s.ShardInfo.GetRangeId(),
		Mode:                     p.CreateWorkflowModeWorkflowIDReuse,
		PreviousRunID:            workflowExecution.GetRunId(),
		PreviousLastWriteVersion: common.EmptyVersion,
	})
	s.NoError(err3)
}

// TestCreateWorkflowExecutionConcurrentCreate test
func (s *ExecutionManagerSuite) TestCreateWorkflowExecutionConcurrentCreate() {
	namespaceID := uuid.New()
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "create-workflow-test-concurrent-create",
		RunId:      uuid.New(),
	}
	taskqueue := "some random taskqueue"
	workflowType := "some random workflow type"
	workflowTimeout := timestamp.DurationFromSeconds(10)
	workflowTaskTimeout := timestamp.DurationFromSeconds(14)
	lastProcessedEventID := int64(0)
	nextEventID := int64(3)
	workflowTaskScheduleID := int64(2)

	task0, err0 := s.CreateWorkflowExecution(namespaceID, workflowExecution, taskqueue, workflowType, workflowTimeout, workflowTaskTimeout, nextEventID, lastProcessedEventID, workflowTaskScheduleID, nil)
	s.Nil(err0, "No error expected.")
	s.NotNil(task0, "Expected non empty task identifier.")

	times := 2
	var wg sync.WaitGroup
	wg.Add(times)
	var numOfErr int32
	var lastError error
	for i := 0; i < times; i++ {
		go func() {
			newExecution := commonpb.WorkflowExecution{
				WorkflowId: workflowExecution.GetWorkflowId(),
				RunId:      uuid.New(),
			}

			state0, err1 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
			s.NoError(err1)
			info0 := state0.ExecutionInfo
			continueAsNewInfo := copyWorkflowExecutionInfo(info0)
			continueAsNewState := copyWorkflowExecutionState(state0.ExecutionState)
			continueAsNewState.State = enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING
			continueAsNewNextEventID := int64(5)
			continueAsNewInfo.LastWorkflowTaskStartId = int64(2)

			err2 := s.ContinueAsNewExecution(continueAsNewInfo, continueAsNewState, continueAsNewNextEventID, state0.NextEventId, newExecution, int64(3), int64(2), nil)
			if err2 != nil {
				errCount := atomic.AddInt32(&numOfErr, 1)
				if errCount > 1 {
					lastError = err2
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
	if lastError != nil {
		s.Fail("Last error is not nil", "Last error: %v", lastError.Error())
	}
	s.Equal(int32(1), atomic.LoadInt32(&numOfErr))
}

// TestPersistenceStartWorkflow test
func (s *ExecutionManagerSuite) TestPersistenceStartWorkflow() {
	namespaceID := "2d7994bf-9de8-459d-9c81-e723daedb246"
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "start-workflow-test",
		RunId:      "7f9fe8a0-9237-11e6-ae22-56b6b6499611",
	}
	workflowKey := definition.NewWorkflowKey(
		namespaceID,
		workflowExecution.WorkflowId,
		workflowExecution.RunId,
	)
	task0, err0 := s.CreateWorkflowExecution(namespaceID, workflowExecution, "queue1", "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	task1, err1 := s.CreateWorkflowExecution(namespaceID, workflowExecution, "queue1", "wType1", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(14), 3, 0, 2, nil)
	s.Error(err1, "Expected workflow creation to fail.")
	startedErr, ok := err1.(*p.CurrentWorkflowConditionFailedError)
	s.True(ok, fmt.Sprintf("Expected CurrentWorkflowConditionFailedError, but actual is %v", err1))
	s.Equal(workflowExecution.GetRunId(), startedErr.RunID, startedErr.Msg)

	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, startedErr.State, startedErr.Msg)
	s.Equal(common.EmptyVersion, startedErr.LastWriteVersion, startedErr.Msg)
	s.Empty(task1, "Expected empty task identifier.")

	response, err2 := s.ExecutionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardInfo.GetShardId(),
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				NamespaceId:                namespaceID,
				WorkflowId:                 workflowExecution.GetWorkflowId(),
				TaskQueue:                  "queue1",
				WorkflowTypeName:           "workflow_type_test",
				WorkflowRunTimeout:         timestamp.DurationFromSeconds(20),
				DefaultWorkflowTaskTimeout: timestamp.DurationFromSeconds(13),
				LastFirstEventId:           common.FirstEventID,
				LastWorkflowTaskStartId:    0,
				WorkflowTaskScheduleId:     int64(2),
				WorkflowTaskStartedId:      common.EmptyEventID,
				WorkflowTaskTimeout:        timestamp.DurationFromSeconds(1),
				ExecutionStats:             &persistencespb.ExecutionStats{},
				StartTime:                  timestamp.TimeNowPtrUtc(),
				LastUpdateTime:             timestamp.TimeNowPtrUtc(),
			},
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId:           workflowExecution.GetRunId(),
				CreateRequestId: uuid.New(),
				State:           enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
				Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
			NextEventID: int64(3),
			TransferTasks: []tasks.Task{
				&tasks.WorkflowTask{
					WorkflowKey: workflowKey,
					TaskID:      s.GetNextSequenceNumber(),
					TaskQueue:   "queue1",
					ScheduleID:  int64(2),
				},
			},
			TimerTasks: nil,
		},
		RangeID: s.ShardInfo.GetRangeId() - 1,
	})

	s.Error(err2, "Expected workflow creation to fail.")
	s.Nil(response)
	s.IsType(&p.ShardOwnershipLostError{}, err2)
}

// TestGetWorkflow test
func (s *ExecutionManagerSuite) TestGetWorkflow() {
	testResetPoints := workflowpb.ResetPoints{
		Points: []*workflowpb.ResetPointInfo{
			{
				BinaryChecksum:               "test-binary-checksum",
				RunId:                        "test-runID",
				FirstWorkflowTaskCompletedId: 123,
				CreateTime:                   timestamp.TimePtr(time.Date(2020, 8, 22, 0, 0, 0, 0, time.UTC)),
				Resettable:                   true,
				ExpireTime:                   timestamp.TimePtr(time.Date(2020, 8, 22, 0, 0, 0, 0, time.UTC)),
			},
		},
	}
	testSearchAttrKey := "env"
	testSearchAttrVal := payload.EncodeString("test")
	testSearchAttr := map[string]*commonpb.Payload{
		testSearchAttrKey: testSearchAttrVal,
	}

	testMemoKey := "memoKey"
	testMemoVal := payload.EncodeString("memoVal")
	testMemo := map[string]*commonpb.Payload{
		testMemoKey: testMemoVal,
	}

	csum := s.newRandomChecksum()

	createReq := &p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardInfo.GetShardId(),
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				NamespaceId:                     uuid.New(),
				WorkflowId:                      "get-workflow-test",
				FirstExecutionRunId:             uuid.New(),
				ParentNamespaceId:               uuid.New(),
				ParentWorkflowId:                "get-workflow-test-parent",
				ParentRunId:                     uuid.New(),
				InitiatedId:                     rand.Int63(),
				TaskQueue:                       "get-wf-test-taskqueue",
				WorkflowTypeName:                "code.uber.internal/test/workflow",
				WorkflowRunTimeout:              timestamp.DurationFromSeconds(int64(rand.Int31())),
				DefaultWorkflowTaskTimeout:      timestamp.DurationFromSeconds(int64(rand.Int31())),
				LastFirstEventId:                common.FirstEventID,
				LastWorkflowTaskStartId:         int64(rand.Int31()),
				SignalCount:                     rand.Int63(),
				WorkflowTaskVersion:             int64(rand.Int31()),
				WorkflowTaskScheduleId:          int64(rand.Int31()),
				WorkflowTaskStartedId:           int64(rand.Int31()),
				WorkflowTaskTimeout:             timestamp.DurationFromSeconds(int64(rand.Int31())),
				Attempt:                         rand.Int31(),
				HasRetryPolicy:                  true,
				RetryInitialInterval:            timestamp.DurationFromSeconds(int64(rand.Int31())),
				RetryBackoffCoefficient:         7.78,
				RetryMaximumInterval:            timestamp.DurationFromSeconds(int64(rand.Int31())),
				RetryMaximumAttempts:            rand.Int31(),
				RetryNonRetryableErrorTypes:     []string{"badRequestError", "accessDeniedError"},
				CronSchedule:                    "* * * * *",
				AutoResetPoints:                 &testResetPoints,
				SearchAttributes:                testSearchAttr,
				Memo:                            testMemo,
				WorkflowRunExpirationTime:       timestamp.TimeNowPtrUtc(),
				WorkflowExecutionExpirationTime: timestamp.TimeNowPtrUtc(),
				ExecutionStats: &persistencespb.ExecutionStats{
					HistorySize: int64(rand.Int31()),
				},
				StartTime:      timestamp.TimeNowPtrUtc(),
				LastUpdateTime: timestamp.TimeNowPtrUtc(),
			},
			NextEventID: rand.Int63(),
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId:           uuid.New(),
				CreateRequestId: uuid.New(),
				State:           enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
				Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
			Checksum: csum,
		},
		Mode: p.CreateWorkflowModeBrandNew,
	}

	createResp, err := s.ExecutionManager.CreateWorkflowExecution(createReq)
	s.NoError(err)
	s.NotNil(createResp, "Expected non empty task identifier.")

	state, err := s.GetWorkflowMutableState(createReq.NewWorkflowSnapshot.ExecutionInfo.NamespaceId,
		commonpb.WorkflowExecution{
			WorkflowId: createReq.NewWorkflowSnapshot.ExecutionInfo.WorkflowId,
			RunId:      createReq.NewWorkflowSnapshot.ExecutionState.RunId,
		})
	s.NoError(err)
	info := state.ExecutionInfo
	s.NotNil(info, "Valid Workflow response expected.")
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionState.CreateRequestId, state.ExecutionState.CreateRequestId)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.NamespaceId, info.NamespaceId)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.WorkflowId, info.WorkflowId)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionState.RunId, state.ExecutionState.GetRunId())
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.FirstExecutionRunId, info.FirstExecutionRunId)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.ParentNamespaceId, info.ParentNamespaceId)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.ParentWorkflowId, info.ParentWorkflowId)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.ParentRunId, info.ParentRunId)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.InitiatedId, info.InitiatedId)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.TaskQueue, info.TaskQueue)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.WorkflowTypeName, info.WorkflowTypeName)
	s.EqualValues(*createReq.NewWorkflowSnapshot.ExecutionInfo.WorkflowRunTimeout, *info.WorkflowRunTimeout)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.DefaultWorkflowTaskTimeout, info.DefaultWorkflowTaskTimeout)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, state.ExecutionState.State)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, state.ExecutionState.Status)
	s.Equal(createReq.NewWorkflowSnapshot.NextEventID, state.NextEventId)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.LastWorkflowTaskStartId, info.LastWorkflowTaskStartId)
	s.True(s.validateTimeRange(timestamp.TimeValue(info.LastUpdateTime), time.Hour))
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.WorkflowTaskVersion, info.WorkflowTaskVersion)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.WorkflowTaskScheduleId, info.WorkflowTaskScheduleId)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.WorkflowTaskStartedId, info.WorkflowTaskStartedId)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.WorkflowTaskTimeout, info.WorkflowTaskTimeout)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.SignalCount, info.SignalCount)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.Attempt, info.Attempt)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.HasRetryPolicy, info.HasRetryPolicy)
	s.EqualValues(createReq.NewWorkflowSnapshot.ExecutionInfo.RetryInitialInterval, info.RetryInitialInterval)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.RetryBackoffCoefficient, info.RetryBackoffCoefficient)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.RetryMaximumAttempts, info.RetryMaximumAttempts)
	s.EqualValues(createReq.NewWorkflowSnapshot.ExecutionInfo.RetryMaximumInterval, info.RetryMaximumInterval)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.CronSchedule, info.CronSchedule)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.RetryNonRetryableErrorTypes, info.RetryNonRetryableErrorTypes)
	s.Equal(testResetPoints.String(), info.AutoResetPoints.String())
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.ExecutionStats.HistorySize, state.ExecutionInfo.ExecutionStats.HistorySize)
	s.EqualTimes(*createReq.NewWorkflowSnapshot.ExecutionInfo.WorkflowRunExpirationTime, *info.WorkflowRunExpirationTime)
	s.EqualTimes(*createReq.NewWorkflowSnapshot.ExecutionInfo.WorkflowExecutionExpirationTime, *info.WorkflowExecutionExpirationTime)
	saVal, ok := info.SearchAttributes[testSearchAttrKey]
	s.True(ok)
	s.True(proto.Equal(testSearchAttrVal, saVal))
	memoVal, ok := info.Memo[testMemoKey]
	s.True(ok)
	s.True(proto.Equal(testMemoVal, memoVal))

	s.assertChecksumsEqual(csum, state.Checksum)
}

// TestUpdateWorkflow test
func (s *ExecutionManagerSuite) TestUpdateWorkflow() {
	namespaceID := "b0a8571c-0257-40ea-afcd-3a14eae181c0"
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "update-workflow-test",
		RunId:      "5ba5e531-e46b-48d9-b4b3-859919839553",
	}
	task0, err0 := s.CreateWorkflowExecution(namespaceID, workflowExecution, "queue1", "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")
	s.Equal(namespaceID, info0.NamespaceId)
	s.Equal("update-workflow-test", info0.WorkflowId)
	s.Equal("5ba5e531-e46b-48d9-b4b3-859919839553", state0.ExecutionState.GetRunId())
	s.Equal("queue1", info0.TaskQueue)
	s.Equal("wType", info0.WorkflowTypeName)
	s.EqualValues(int64(20), info0.WorkflowRunTimeout.Seconds())
	s.EqualValues(int64(13), info0.DefaultWorkflowTaskTimeout.Seconds())
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, state0.ExecutionState.State)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, state0.ExecutionState.Status)
	s.Equal(int64(1), info0.LastFirstEventId)
	s.Equal(int64(3), state0.NextEventId)
	s.Equal(int64(0), info0.LastWorkflowTaskStartId)
	s.True(s.validateTimeRange(*info0.LastUpdateTime, time.Hour))
	s.Equal(int64(0), info0.WorkflowTaskVersion)
	s.Equal(int64(2), info0.WorkflowTaskScheduleId)
	s.Equal(common.EmptyEventID, info0.WorkflowTaskStartedId)
	s.EqualValues(1, int64(info0.WorkflowTaskTimeout.Seconds()))
	s.Equal(int32(0), info0.WorkflowTaskAttempt)
	s.Nil(info0.WorkflowTaskStartedTime)
	s.Nil(info0.WorkflowTaskScheduledTime)
	s.Nil(info0.WorkflowTaskOriginalScheduledTime)
	s.Empty(info0.StickyTaskQueue)
	s.EqualValues(int64(0), timestamp.DurationValue(info0.StickyScheduleToStartTimeout))
	s.Equal(int64(0), info0.SignalCount)
	s.True(reflect.DeepEqual(info0.AutoResetPoints, &workflowpb.ResetPoints{}))
	s.True(len(info0.SearchAttributes) == 0)
	s.True(len(info0.Memo) == 0)
	s.assertChecksumsEqual(testWorkflowChecksum, state0.Checksum)

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedState := copyWorkflowExecutionState(state0.ExecutionState)
	updatedInfo.LastFirstEventId = int64(3)
	updatedInfo.LastWorkflowTaskStartId = int64(2)
	updatedInfo.WorkflowTaskVersion = int64(666)
	updatedInfo.WorkflowTaskAttempt = 123
	updatedInfo.WorkflowTaskStartedTime = timestamp.UnixOrZeroTimePtr(321)
	updatedInfo.WorkflowTaskScheduledTime = timestamp.UnixOrZeroTimePtr(654)
	updatedInfo.WorkflowTaskOriginalScheduledTime = timestamp.UnixOrZeroTimePtr(655)
	updatedInfo.StickyTaskQueue = "random sticky taskqueue"
	updatedInfo.StickyScheduleToStartTimeout = timestamp.DurationFromSeconds(876)
	updatedInfo.SignalCount = 9
	updatedInfo.RetryInitialInterval = timestamp.DurationFromSeconds(math.MaxInt32)
	updatedInfo.RetryBackoffCoefficient = 4.45
	updatedInfo.RetryMaximumInterval = timestamp.DurationFromSeconds(math.MaxInt32)
	updatedInfo.RetryMaximumAttempts = math.MaxInt32
	updatedInfo.WorkflowRunExpirationTime = timestamp.TimeNowPtrUtc()
	updatedInfo.WorkflowExecutionExpirationTime = timestamp.TimeNowPtrUtc()
	updatedInfo.RetryNonRetryableErrorTypes = []string{"accessDenied", "badRequest"}
	searchAttrKey := "env"
	searchAttrVal := payload.EncodeBytes([]byte("test"))
	updatedInfo.SearchAttributes = map[string]*commonpb.Payload{searchAttrKey: searchAttrVal}

	memoKey := "memoKey"
	memoVal := payload.EncodeBytes([]byte("memoVal"))
	updatedInfo.Memo = map[string]*commonpb.Payload{memoKey: memoVal}
	updatedInfo.ExecutionStats.HistorySize = math.MaxInt64

	err2 := s.UpdateWorkflowExecution(updatedInfo, updatedState, int64(5), []int64{int64(4)}, nil, int64(3), nil, nil, nil, nil, nil)
	s.NoError(err2)

	state1, err3 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err3)
	info1 := state1.ExecutionInfo
	s.NotNil(info1, "Valid Workflow info expected.")
	s.Equal(namespaceID, info1.NamespaceId)
	s.Equal("update-workflow-test", info1.WorkflowId)
	s.Equal("5ba5e531-e46b-48d9-b4b3-859919839553", state1.ExecutionState.GetRunId())
	s.Equal("queue1", info1.TaskQueue)
	s.Equal("wType", info1.WorkflowTypeName)
	s.EqualValues(int64(20), info1.WorkflowRunTimeout.Seconds())
	s.EqualValues(13, int64(info1.DefaultWorkflowTaskTimeout.Seconds()))
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, state1.ExecutionState.State)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, state1.ExecutionState.Status)
	s.Equal(int64(3), info1.LastFirstEventId)
	s.Equal(int64(5), state1.NextEventId)
	s.Equal(int64(2), info1.LastWorkflowTaskStartId)
	s.True(s.validateTimeRange(*info1.LastUpdateTime, time.Hour))
	s.Equal(int64(666), info1.WorkflowTaskVersion)
	s.Equal(int64(2), info1.WorkflowTaskScheduleId)
	s.Equal(common.EmptyEventID, info1.WorkflowTaskStartedId)
	s.EqualValues(1, int64(info1.WorkflowTaskTimeout.Seconds()))
	s.Equal(int32(123), info1.WorkflowTaskAttempt)
	s.Equal(int64(321), info1.WorkflowTaskStartedTime.UnixNano())
	s.Equal(int64(654), info1.WorkflowTaskScheduledTime.UnixNano())
	s.Equal(int64(655), info1.WorkflowTaskOriginalScheduledTime.UnixNano())
	s.Equal(updatedInfo.StickyTaskQueue, info1.StickyTaskQueue)
	s.EqualValues(timestamp.DurationValue(updatedInfo.StickyScheduleToStartTimeout), timestamp.DurationValue(info1.StickyScheduleToStartTimeout))
	s.Equal(updatedInfo.SignalCount, info1.SignalCount)
	s.EqualValues(updatedInfo.ExecutionStats.HistorySize, info1.ExecutionStats.HistorySize)
	s.EqualValues(updatedInfo.RetryInitialInterval, info1.RetryInitialInterval)
	s.Equal(updatedInfo.RetryBackoffCoefficient, info1.RetryBackoffCoefficient)
	s.Equal(updatedInfo.RetryMaximumInterval, info1.RetryMaximumInterval)
	s.Equal(updatedInfo.RetryMaximumAttempts, info1.RetryMaximumAttempts)
	s.Equal(updatedInfo.RetryNonRetryableErrorTypes, info1.RetryNonRetryableErrorTypes)
	s.EqualTimes(*updatedInfo.WorkflowRunExpirationTime, *info1.WorkflowRunExpirationTime)
	s.EqualTimes(*updatedInfo.WorkflowExecutionExpirationTime, *info1.WorkflowExecutionExpirationTime)
	searchAttrVal1, ok := info1.SearchAttributes[searchAttrKey]
	s.True(ok)
	s.Equal(searchAttrVal, searchAttrVal1)
	memoVal1, ok := info1.Memo[memoKey]
	s.True(ok)
	s.Equal(memoVal, memoVal1)
	s.assertChecksumsEqual(testWorkflowChecksum, state1.Checksum)

	failedUpdateInfo := copyWorkflowExecutionInfo(updatedInfo)
	failedUpdateState := copyWorkflowExecutionState(updatedState)
	err4 := s.UpdateWorkflowExecution(failedUpdateInfo, failedUpdateState, state0.NextEventId, []int64{int64(5)}, nil, int64(3), nil, nil, nil, nil, nil)
	s.Error(err4, "expected non nil error.")
	s.IsType(&p.WorkflowConditionFailedError{}, err4)

	state2, err4 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err4)
	info2 := state2.ExecutionInfo
	s.NotNil(info2, "Valid Workflow info expected.")
	s.Equal(namespaceID, info2.NamespaceId)
	s.Equal("update-workflow-test", info2.WorkflowId)
	s.Equal("5ba5e531-e46b-48d9-b4b3-859919839553", state2.ExecutionState.GetRunId())
	s.Equal("queue1", info2.TaskQueue)
	s.Equal("wType", info2.WorkflowTypeName)
	s.EqualValues(int64(20), info2.WorkflowRunTimeout.Seconds())
	s.EqualValues(13, int64(info2.DefaultWorkflowTaskTimeout.Seconds()))
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, state2.ExecutionState.State)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, state2.ExecutionState.Status)
	s.Equal(int64(5), state2.NextEventId)
	s.Equal(int64(2), info2.LastWorkflowTaskStartId)
	s.True(s.validateTimeRange(*info2.LastUpdateTime, time.Hour))
	s.Equal(int64(666), info2.WorkflowTaskVersion)
	s.Equal(int64(2), info2.WorkflowTaskScheduleId)
	s.Equal(common.EmptyEventID, info2.WorkflowTaskStartedId)
	s.EqualValues(1, int64(info2.WorkflowTaskTimeout.Seconds()))
	s.Equal(int32(123), info2.WorkflowTaskAttempt)
	s.Equal(int64(321), info2.WorkflowTaskStartedTime.UnixNano())
	s.Equal(int64(654), info2.WorkflowTaskScheduledTime.UnixNano())
	s.Equal(int64(655), info2.WorkflowTaskOriginalScheduledTime.UnixNano())
	s.Equal(updatedInfo.SignalCount, info2.SignalCount)
	s.EqualValues(updatedInfo.ExecutionStats.HistorySize, info2.ExecutionStats.HistorySize)
	s.EqualValues(updatedInfo.RetryInitialInterval, info2.RetryInitialInterval)
	s.Equal(updatedInfo.RetryBackoffCoefficient, info2.RetryBackoffCoefficient)
	s.Equal(updatedInfo.RetryMaximumInterval, info2.RetryMaximumInterval)
	s.Equal(updatedInfo.RetryMaximumAttempts, info2.RetryMaximumAttempts)
	s.Equal(updatedInfo.RetryNonRetryableErrorTypes, info2.RetryNonRetryableErrorTypes)
	s.EqualTimes(*updatedInfo.WorkflowRunExpirationTime, *info2.WorkflowRunExpirationTime)
	s.EqualTimes(*updatedInfo.WorkflowExecutionExpirationTime, *info2.WorkflowExecutionExpirationTime)
	searchAttrVal2, ok := info2.SearchAttributes[searchAttrKey]
	s.True(ok)
	s.Equal(searchAttrVal, searchAttrVal2)
	memoVal2, ok := info1.Memo[memoKey]
	s.True(ok)
	s.Equal(memoVal, memoVal2)
	s.assertChecksumsEqual(testWorkflowChecksum, state2.Checksum)

	err5 := s.UpdateWorkflowExecutionWithRangeID(failedUpdateInfo, failedUpdateState, state0.NextEventId, []int64{int64(5)}, nil, int64(12345), int64(5), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	s.Error(err5, "expected non nil error.")
	s.IsType(&p.ShardOwnershipLostError{}, err5)

	state3, err6 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err6)
	info3 := state3.ExecutionInfo
	s.NotNil(info3, "Valid Workflow info expected.")
	s.Equal(namespaceID, info3.NamespaceId)
	s.Equal("update-workflow-test", info3.WorkflowId)
	s.Equal("5ba5e531-e46b-48d9-b4b3-859919839553", state3.ExecutionState.GetRunId())
	s.Equal("queue1", info3.TaskQueue)
	s.Equal("wType", info3.WorkflowTypeName)
	s.EqualValues(int64(20), info3.WorkflowRunTimeout.Seconds())
	s.EqualValues(13, int64(info3.DefaultWorkflowTaskTimeout.Seconds()))
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, state3.ExecutionState.State)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, state3.ExecutionState.Status)
	s.Equal(int64(5), state3.NextEventId)
	s.Equal(int64(2), info3.LastWorkflowTaskStartId)
	s.True(s.validateTimeRange(*info3.LastUpdateTime, time.Hour))
	s.Equal(int64(666), info3.WorkflowTaskVersion)
	s.Equal(int64(2), info3.WorkflowTaskScheduleId)
	s.Equal(common.EmptyEventID, info3.WorkflowTaskStartedId)
	s.EqualValues(1, int64(info3.WorkflowTaskTimeout.Seconds()))
	s.Equal(int32(123), info3.WorkflowTaskAttempt)
	s.Equal(int64(321), info3.WorkflowTaskStartedTime.UnixNano())
	s.Equal(int64(654), info3.WorkflowTaskScheduledTime.UnixNano())
	s.Equal(int64(655), info3.WorkflowTaskOriginalScheduledTime.UnixNano())
	s.Equal(updatedInfo.SignalCount, info3.SignalCount)
	s.EqualValues(updatedInfo.ExecutionStats.HistorySize, info3.ExecutionStats.HistorySize)
	s.EqualValues(updatedInfo.RetryInitialInterval, info3.RetryInitialInterval)
	s.Equal(updatedInfo.RetryBackoffCoefficient, info3.RetryBackoffCoefficient)
	s.Equal(updatedInfo.RetryMaximumInterval, info3.RetryMaximumInterval)
	s.Equal(updatedInfo.RetryMaximumAttempts, info3.RetryMaximumAttempts)
	s.Equal(updatedInfo.RetryNonRetryableErrorTypes, info3.RetryNonRetryableErrorTypes)
	s.EqualTimes(*updatedInfo.WorkflowRunExpirationTime, *info3.WorkflowRunExpirationTime)
	s.EqualTimes(*updatedInfo.WorkflowExecutionExpirationTime, *info3.WorkflowExecutionExpirationTime)
	searchAttrVal3, ok := info3.SearchAttributes[searchAttrKey]
	s.True(ok)
	s.Equal(searchAttrVal, searchAttrVal3)
	memoVal3, ok := info1.Memo[memoKey]
	s.True(ok)
	s.Equal(memoVal, memoVal3)
	s.assertChecksumsEqual(testWorkflowChecksum, state3.Checksum)

	// update with incorrect rangeID and condition(next_event_id)
	err7 := s.UpdateWorkflowExecutionWithRangeID(failedUpdateInfo, failedUpdateState, state0.NextEventId, []int64{int64(5)}, nil, int64(12345), int64(3), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	s.Error(err7, "expected non nil error.")
	s.IsType(&p.ShardOwnershipLostError{}, err7)

	state4, err8 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err8)
	info4 := state4.ExecutionInfo
	s.NotNil(info4, "Valid Workflow info expected.")
	s.Equal(namespaceID, info4.NamespaceId)
	s.Equal("update-workflow-test", info4.WorkflowId)
	s.Equal("5ba5e531-e46b-48d9-b4b3-859919839553", state4.ExecutionState.GetRunId())
	s.Equal("queue1", info4.TaskQueue)
	s.Equal("wType", info4.WorkflowTypeName)
	s.EqualValues(int64(20), info4.WorkflowRunTimeout.Seconds())
	s.EqualValues(int64(13), info4.DefaultWorkflowTaskTimeout.Seconds())
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, state4.ExecutionState.State)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, state4.ExecutionState.Status)
	s.Equal(int64(5), state4.NextEventId)
	s.Equal(int64(2), info4.LastWorkflowTaskStartId)
	s.True(s.validateTimeRange(*info4.LastUpdateTime, time.Hour))
	s.Equal(int64(666), info4.WorkflowTaskVersion)
	s.Equal(int64(2), info4.WorkflowTaskScheduleId)
	s.Equal(common.EmptyEventID, info4.WorkflowTaskStartedId)
	s.EqualValues(1, int64(info4.WorkflowTaskTimeout.Seconds()))
	s.Equal(int32(123), info4.WorkflowTaskAttempt)
	s.Equal(int64(321), info4.WorkflowTaskStartedTime.UnixNano())
	s.Equal(updatedInfo.SignalCount, info4.SignalCount)
	s.EqualValues(updatedInfo.ExecutionStats.HistorySize, state4.ExecutionInfo.ExecutionStats.HistorySize)
	s.EqualValues(updatedInfo.RetryInitialInterval, info4.RetryInitialInterval)
	s.Equal(updatedInfo.RetryBackoffCoefficient, info4.RetryBackoffCoefficient)
	s.Equal(updatedInfo.RetryMaximumInterval, info4.RetryMaximumInterval)
	s.Equal(updatedInfo.RetryMaximumAttempts, info4.RetryMaximumAttempts)
	s.Equal(updatedInfo.RetryNonRetryableErrorTypes, info4.RetryNonRetryableErrorTypes)
	s.EqualTimes(*updatedInfo.WorkflowRunExpirationTime, *info4.WorkflowRunExpirationTime)
	s.EqualTimes(*updatedInfo.WorkflowExecutionExpirationTime, *info4.WorkflowExecutionExpirationTime)
	searchAttrVal4, ok := info4.SearchAttributes[searchAttrKey]
	s.True(ok)
	s.Equal(searchAttrVal, searchAttrVal4)
	memoVal4, ok := info1.Memo[memoKey]
	s.True(ok)
	s.Equal(memoVal, memoVal4)
	s.assertChecksumsEqual(testWorkflowChecksum, state4.Checksum)

}

// TestDeleteWorkflow test
func (s *ExecutionManagerSuite) TestDeleteWorkflow() {
	namespaceID := "1d4abb23-b87b-457b-96ef-43aba0b9c44f"
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "delete-workflow-test",
		RunId:      "4e0917f2-9361-4a14-b16f-1fafe09b287a",
	}
	task0, err0 := s.CreateWorkflowExecution(namespaceID, workflowExecution, "queue1", "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")
	s.Equal(namespaceID, info0.NamespaceId)
	s.Equal("delete-workflow-test", info0.WorkflowId)
	s.Equal("4e0917f2-9361-4a14-b16f-1fafe09b287a", state0.ExecutionState.GetRunId())
	s.Equal("queue1", info0.TaskQueue)
	s.Equal("wType", info0.WorkflowTypeName)
	s.EqualValues(int64(20), info0.WorkflowRunTimeout.Seconds())
	s.EqualValues(13, int64(info0.DefaultWorkflowTaskTimeout.Seconds()))
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, state0.ExecutionState.State)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, state0.ExecutionState.Status)
	s.Equal(int64(3), state0.NextEventId)
	s.Equal(int64(0), info0.LastWorkflowTaskStartId)
	s.True(s.validateTimeRange(*info0.LastUpdateTime, time.Hour))
	s.Equal(int64(2), info0.WorkflowTaskScheduleId)
	s.Equal(common.EmptyEventID, info0.WorkflowTaskStartedId)
	s.EqualValues(1, int64(info0.WorkflowTaskTimeout.Seconds()))

	err4 := s.DeleteCurrentWorkflowExecution(info0, state0.ExecutionState)
	s.NoError(err4)
	err4 = s.DeleteWorkflowExecution(info0, state0.ExecutionState)
	s.NoError(err4)

	_, err3 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.Error(err3, "expected non nil error.")
	s.IsType(&serviceerror.NotFound{}, err3)

	err5 := s.DeleteWorkflowExecution(info0, state0.ExecutionState)
	s.NoError(err5)
}

// TestDeleteCurrentWorkflow test
func (s *ExecutionManagerSuite) TestDeleteCurrentWorkflow() {
	if s.ExecutionManager.GetName() != "cassandra" {
		// "this test is only applicable for cassandra (uses TTL based deletes)"
		return
	}
	namespaceID := "54d15308-e20e-4b91-a00f-a518a3892790"
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "delete-current-workflow-test",
		RunId:      "6cae4054-6ba7-46d3-8755-e3c2db6f74ea",
	}

	task0, err0 := s.CreateWorkflowExecution(namespaceID, workflowExecution, "queue1", "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	runID0, err1 := s.GetCurrentWorkflowRunID(namespaceID, workflowExecution.GetWorkflowId())
	s.NoError(err1)
	s.Equal(workflowExecution.GetRunId(), runID0)

	info0, err2 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err2)

	updatedInfo1 := copyWorkflowExecutionInfo(info0.ExecutionInfo)
	updatedState1 := copyWorkflowExecutionState(info0.ExecutionState)
	updatedInfo1.LastWorkflowTaskStartId = int64(2)
	err3 := s.UpdateWorkflowExecutionAndFinish(updatedInfo1, updatedState1, int64(6), int64(3))
	s.NoError(err3)

	runID4, err4 := s.GetCurrentWorkflowRunID(namespaceID, workflowExecution.GetWorkflowId())
	s.NoError(err4)
	s.Equal(workflowExecution.GetRunId(), runID4)

	fakeInfo := &persistencespb.WorkflowExecutionInfo{
		NamespaceId: info0.ExecutionInfo.NamespaceId,
		WorkflowId:  info0.ExecutionInfo.WorkflowId,
	}

	// test wrong run id with conditional delete
	s.NoError(s.DeleteCurrentWorkflowExecution(fakeInfo, &persistencespb.WorkflowExecutionState{RunId: uuid.New()}))

	runID5, err5 := s.GetCurrentWorkflowRunID(namespaceID, workflowExecution.GetWorkflowId())
	s.NoError(err5)
	s.Equal(workflowExecution.GetRunId(), runID5)

	// simulate a timer_task deleting execution after retention
	s.NoError(s.DeleteCurrentWorkflowExecution(info0.ExecutionInfo, info0.ExecutionState))

	runID0, err1 = s.GetCurrentWorkflowRunID(namespaceID, workflowExecution.GetWorkflowId())
	s.Error(err1)
	s.Empty(runID0)
	_, ok := err1.(*serviceerror.NotFound)
	s.True(ok)

	// execution record should still be there
	_, err2 = s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err2)
}

// TestUpdateDeleteWorkflow mocks the timer behavoir to clean up workflow.
func (s *ExecutionManagerSuite) TestUpdateDeleteWorkflow() {
	finishedCurrentExecutionRetentionTTL := int32(2)
	namespaceID := "54d15308-e20e-4b91-a00f-a518a3892790"
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "update-delete-workflow-test",
		RunId:      "6cae4054-6ba7-46d3-8755-e3c2db6f74ea",
	}

	task0, err0 := s.CreateWorkflowExecution(namespaceID, workflowExecution, "queue1", "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	runID0, err1 := s.GetCurrentWorkflowRunID(namespaceID, workflowExecution.GetWorkflowId())
	s.NoError(err1)
	s.Equal(workflowExecution.GetRunId(), runID0)

	info0, err2 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err2)

	updatedInfo1 := copyWorkflowExecutionInfo(info0.ExecutionInfo)
	updatedState1 := copyWorkflowExecutionState(info0.ExecutionState)
	updatedInfo1.LastWorkflowTaskStartId = int64(2)
	err3 := s.UpdateWorkflowExecutionAndFinish(updatedInfo1, updatedState1, int64(6), int64(3))
	s.NoError(err3)

	runID4, err4 := s.GetCurrentWorkflowRunID(namespaceID, workflowExecution.GetWorkflowId())
	s.NoError(err4)
	s.Equal(workflowExecution.GetRunId(), runID4)

	// simulate a timer_task deleting execution after retention
	err5 := s.DeleteCurrentWorkflowExecution(info0.ExecutionInfo, info0.ExecutionState)
	s.NoError(err5)
	err6 := s.DeleteWorkflowExecution(info0.ExecutionInfo, info0.ExecutionState)
	s.NoError(err6)

	time.Sleep(time.Duration(finishedCurrentExecutionRetentionTTL*2) * time.Second)

	runID0, err1 = s.GetCurrentWorkflowRunID(namespaceID, workflowExecution.GetWorkflowId())
	s.Error(err1)
	s.Empty(runID0)
	_, ok := err1.(*serviceerror.NotFound)
	// execution record should still be there
	_, err2 = s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.Error(err2)
	_, ok = err2.(*serviceerror.NotFound)
	s.True(ok)
}

// TestCleanupCorruptedWorkflow test
func (s *ExecutionManagerSuite) TestCleanupCorruptedWorkflow() {
	namespaceID := "54d15308-e20e-4b91-a00f-a518a3892790"
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "cleanup-corrupted-workflow-test",
		RunId:      "6cae4054-6ba7-46d3-8755-e3c2db6f74ea",
	}

	task0, err0 := s.CreateWorkflowExecution(namespaceID, workflowExecution, "queue1", "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	runID0, err1 := s.GetCurrentWorkflowRunID(namespaceID, workflowExecution.GetWorkflowId())
	s.NoError(err1)
	s.Equal(workflowExecution.GetRunId(), runID0)

	info0, err2 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err2)

	// deleting current record and verify
	err3 := s.DeleteCurrentWorkflowExecution(info0.ExecutionInfo, info0.ExecutionState)
	s.NoError(err3)
	runID0, err4 := s.GetCurrentWorkflowRunID(namespaceID, workflowExecution.GetWorkflowId())
	s.Error(err4)
	s.Empty(runID0)
	_, ok := err4.(*serviceerror.NotFound)
	s.True(ok)

	// we should still be able to load with runID
	info1, err5 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err5)
	s.Equal(info0, info1)

	// mark it as corrupted
	info0.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_CORRUPTED
	_, err6 := s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		ShardID: s.ShardInfo.GetShardId(),
		UpdateWorkflowMutation: p.WorkflowMutation{
			ExecutionInfo:  info0.ExecutionInfo,
			ExecutionState: info0.ExecutionState,
			NextEventID:    info0.NextEventId,
			Condition:      info0.NextEventId,
			Checksum:       testWorkflowChecksum,
		},
		RangeID: s.ShardInfo.GetRangeId(),
		Mode:    p.UpdateWorkflowModeBypassCurrent,
	})
	s.NoError(err6)

	// we should still be able to load with runID
	info2, err7 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err7)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_CORRUPTED, info2.ExecutionState.State)
	info2.ExecutionState.State = info1.ExecutionState.State
	info2.ExecutionInfo.LastUpdateTime = info1.ExecutionInfo.LastUpdateTime
	s.Equal(info2, info1)

	// delete the run
	err8 := s.DeleteCurrentWorkflowExecution(info0.ExecutionInfo, info0.ExecutionState)
	s.NoError(err8)
	err8 = s.DeleteWorkflowExecution(info0.ExecutionInfo, info0.ExecutionState)
	s.NoError(err8)

	// execution record should be gone
	_, err9 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.Error(err9)
	_, ok = err9.(*serviceerror.NotFound)
	s.True(ok)
}

// TestGetCurrentWorkflow test
func (s *ExecutionManagerSuite) TestGetCurrentWorkflow() {
	namespaceID := "54d15308-e20e-4b91-a00f-a518a3892790"
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "get-current-workflow-test",
		RunId:      "6cae4054-6ba7-46d3-8755-e3c2db6f74ea",
	}

	task0, err0 := s.CreateWorkflowExecution(namespaceID, workflowExecution, "queue1", "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	response, err := s.ExecutionManager.GetCurrentExecution(&p.GetCurrentExecutionRequest{
		ShardID:     s.ShardInfo.GetShardId(),
		NamespaceID: namespaceID,
		WorkflowID:  workflowExecution.GetWorkflowId(),
	})
	s.NoError(err)
	s.Equal(workflowExecution.GetRunId(), response.RunID)

	info0, err2 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err2)

	updatedInfo1 := copyWorkflowExecutionInfo(info0.ExecutionInfo)
	updatedState1 := copyWorkflowExecutionState(info0.ExecutionState)
	updatedInfo1.LastWorkflowTaskStartId = int64(2)
	err3 := s.UpdateWorkflowExecutionAndFinish(updatedInfo1, updatedState1, int64(6), int64(3))
	s.NoError(err3)

	runID4, err4 := s.GetCurrentWorkflowRunID(namespaceID, workflowExecution.GetWorkflowId())
	s.NoError(err4)
	s.Equal(workflowExecution.GetRunId(), runID4)

	workflowExecution2 := commonpb.WorkflowExecution{
		WorkflowId: "get-current-workflow-test",
		RunId:      "c3ff4bc6-de18-4643-83b2-037a33f45322",
	}

	task1, err5 := s.CreateWorkflowExecution(namespaceID, workflowExecution2, "queue1", "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, nil)
	s.Error(err5, "Error expected.")
	s.Empty(task1, "Expected empty task identifier.")
}

// TestTransferTasksThroughUpdate test
func (s *ExecutionManagerSuite) TestTransferTasksThroughUpdate() {
	namespaceID := "b785a8ba-bd7d-4760-bb05-41b115f3e10a"
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "get-transfer-tasks-through-update-test",
		RunId:      "30a9fa1f-0db1-4d7a-8c34-aa82c5dad3aa",
	}
	workflowKey := definition.NewWorkflowKey(
		namespaceID,
		workflowExecution.WorkflowId,
		workflowExecution.RunId,
	)

	task0, err0 := s.CreateWorkflowExecution(namespaceID, workflowExecution, "queue1", "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	tasks1, err1 := s.GetTransferTasks(1, false)
	s.NoError(err1)
	s.NotNil(tasks1, "expected valid list of tasks.")
	s.Equal(1, len(tasks1), "Expected 1 workflow task.")
	task1 := tasks1[0]
	s.Equal(&tasks.WorkflowTask{
		WorkflowKey:         workflowKey,
		VisibilityTimestamp: task1.GetVisibilityTime(),
		TaskID:              task1.GetTaskID(),
		TaskQueue:           "queue1",
		ScheduleID:          2,
		Version:             0,
	}, task1)

	err3 := s.CompleteTransferTask(task1.GetTaskID())
	s.NoError(err3)

	state0, err11 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err11)
	info0 := state0.ExecutionInfo
	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedState0 := copyWorkflowExecutionState(state0.ExecutionState)
	updatedInfo.LastWorkflowTaskStartId = int64(2)
	err2 := s.UpdateWorkflowExecution(updatedInfo, updatedState0, int64(5), nil, []int64{int64(4)}, int64(3), nil, nil, nil, nil, nil)
	s.NoError(err2)

	tasks2, err1 := s.GetTransferTasks(1, false)
	s.NoError(err1)
	s.NotNil(tasks2, "expected valid list of tasks.")
	s.Equal(1, len(tasks2), "Expected 1 workflow task.")
	task2 := tasks2[0]
	s.Equal(&tasks.ActivityTask{
		WorkflowKey:         workflowKey,
		TargetNamespaceID:   namespaceID,
		VisibilityTimestamp: task2.GetVisibilityTime(),
		TaskID:              task2.GetTaskID(),
		TaskQueue:           "queue1",
		ScheduleID:          4,
		Version:             0,
	}, task2)

	err4 := s.CompleteTransferTask(task2.GetTaskID())
	s.NoError(err4)

	state1, _ := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	info1 := state1.ExecutionInfo
	updatedInfo1 := copyWorkflowExecutionInfo(info1)
	updatedState1 := copyWorkflowExecutionState(state1.ExecutionState)
	updatedInfo1.LastWorkflowTaskStartId = int64(2)
	err5 := s.UpdateWorkflowExecutionAndFinish(updatedInfo1, updatedState1, int64(6), int64(5))
	s.NoError(err5)

	newExecution := commonpb.WorkflowExecution{
		WorkflowId: workflowExecution.GetWorkflowId(),
		RunId:      "2a038c8f-b575-4151-8d2c-d443e999ab5a",
	}
	runID6, err6 := s.GetCurrentWorkflowRunID(namespaceID, newExecution.GetWorkflowId())
	s.NoError(err6)
	s.Equal(workflowExecution.GetRunId(), runID6)

	tasks3, err7 := s.GetTransferTasks(1, false)
	s.NoError(err7)
	s.NotNil(tasks3, "expected valid list of tasks.")
	s.Equal(1, len(tasks3), "Expected 1 workflow task.")
	task3 := tasks3[0]
	s.Equal(&tasks.CloseExecutionTask{
		WorkflowKey:         workflowKey,
		VisibilityTimestamp: task3.GetVisibilityTime(),
		TaskID:              task3.GetTaskID(),
		Version:             0,
	}, task3)

	err8 := s.CompleteTransferTask(task3.GetTaskID())
	s.NoError(err8)

	_, err9 := s.CreateWorkflowExecution(namespaceID, newExecution, "queue1", "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, nil)
	s.Error(err9, "createWFExecution (brand_new) must fail when there is a previous instance of workflow state already in DB")

	err10 := s.DeleteCurrentWorkflowExecution(info1, state1.ExecutionState)
	s.NoError(err10)
	err10 = s.DeleteWorkflowExecution(info1, state1.ExecutionState)
	s.NoError(err10)
}

// TestCancelTransferTaskTasks test
func (s *ExecutionManagerSuite) TestCancelTransferTaskTasks() {
	namespaceID := "aeac8287-527b-4b35-80a9-667cb47e7c6d"
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "cancel-workflow-test",
		RunId:      "db20f7e2-1a1e-40d9-9278-d8b886738e05",
	}
	workflowKey := definition.NewWorkflowKey(
		namespaceID,
		workflowExecution.WorkflowId,
		workflowExecution.RunId,
	)

	task0, err := s.CreateWorkflowExecution(namespaceID, workflowExecution, "queue1", "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, nil)
	s.NoError(err)
	s.NotNil(task0, "Expected non empty task identifier.")

	taskD, err := s.GetTransferTasks(1, false)
	s.Equal(1, len(taskD), "Expected 1 workflow task.")
	err = s.CompleteTransferTask(taskD[0].GetTaskID())
	s.NoError(err)

	// Lookup is time-sensitive, hence retry
	var deleteCheck []tasks.Task
	for i := 0; i < 3; i++ {
		deleteCheck, err = s.GetTransferTasks(1, false)
		if len(deleteCheck) == 0 {
			break
		}
		time.Sleep(time.Second * time.Duration(i))
	}
	s.NoError(err)
	s.Equal(0, len(deleteCheck), "Expected no workflow task.")

	state1, err := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err)
	s.NotNil(state1.ExecutionInfo, "Valid Workflow info expected.")
	updatedInfo1 := copyWorkflowExecutionInfo(state1.ExecutionInfo)
	updatedState1 := copyWorkflowExecutionState(state1.ExecutionState)

	targetNamespaceID := "f2bfaab6-7e8b-4fac-9a62-17da8d37becb"
	targetWorkflowID := "target-workflow-cancellation-id-1"
	targetRunID := "0d00698f-08e1-4d36-a3e2-3bf109f5d2d6"
	targetChildWorkflowOnly := false
	transferTasks := []tasks.Task{&tasks.CancelExecutionTask{
		WorkflowKey:             workflowKey,
		TaskID:                  s.GetNextSequenceNumber(),
		TargetNamespaceID:       targetNamespaceID,
		TargetWorkflowID:        targetWorkflowID,
		TargetRunID:             targetRunID,
		TargetChildWorkflowOnly: targetChildWorkflowOnly,
		InitiatedID:             1,
	}}
	err = s.UpdateWorkflowExecutionWithTransferTasks(updatedInfo1, updatedState1, state1.NextEventId, int64(3), transferTasks, nil)
	s.NoError(err)

	tasks1, err := s.GetTransferTasks(1, false)
	s.NoError(err)
	s.NotNil(tasks1, "expected valid list of tasks.")
	s.Equal(1, len(tasks1), "Expected 1 cancel task.")
	task1 := tasks1[0]
	s.Equal(&tasks.CancelExecutionTask{
		WorkflowKey:             workflowKey,
		VisibilityTimestamp:     task1.GetVisibilityTime(),
		TaskID:                  task1.GetTaskID(),
		TargetNamespaceID:       targetNamespaceID,
		TargetWorkflowID:        targetWorkflowID,
		TargetRunID:             targetRunID,
		TargetChildWorkflowOnly: targetChildWorkflowOnly,
		InitiatedID:             1,
		Version:                 0,
	}, task1)

	err = s.CompleteTransferTask(task1.GetTaskID())
	s.NoError(err)

	targetNamespaceID = "f2bfaab6-7e8b-4fac-9a62-17da8d37becb"
	targetWorkflowID = "target-workflow-cancellation-id-2"
	targetRunID = ""
	targetChildWorkflowOnly = true
	transferTasks = []tasks.Task{&tasks.CancelExecutionTask{
		WorkflowKey:             workflowKey,
		TaskID:                  s.GetNextSequenceNumber(),
		TargetNamespaceID:       targetNamespaceID,
		TargetWorkflowID:        targetWorkflowID,
		TargetRunID:             targetRunID,
		TargetChildWorkflowOnly: targetChildWorkflowOnly,
		InitiatedID:             3,
	}}

	state2, err := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err)
	info2 := state2.ExecutionInfo
	s.NotNil(info2, "Valid Workflow info expected.")
	updatedInfo2 := copyWorkflowExecutionInfo(info2)
	updatedState2 := copyWorkflowExecutionState(state2.ExecutionState)

	err = s.UpdateWorkflowExecutionWithTransferTasks(updatedInfo2, updatedState2, state2.NextEventId, int64(3), transferTasks, nil)
	s.NoError(err)

	tasks2, err := s.GetTransferTasks(1, false)
	s.NoError(err)
	s.NotNil(tasks2, "expected valid list of tasks.")
	s.Equal(1, len(tasks2), "Expected 1 cancel task.")
	task2 := tasks2[0]
	s.Equal(&tasks.CancelExecutionTask{
		WorkflowKey:             workflowKey,
		VisibilityTimestamp:     task2.GetVisibilityTime(),
		TaskID:                  task2.GetTaskID(),
		TargetNamespaceID:       targetNamespaceID,
		TargetWorkflowID:        targetWorkflowID,
		TargetRunID:             targetRunID,
		TargetChildWorkflowOnly: targetChildWorkflowOnly,
		InitiatedID:             3,
		Version:                 0,
	}, task2)

	err = s.CompleteTransferTask(task2.GetTaskID())
	s.NoError(err)
}

func (s *ExecutionManagerSuite) validateTransferTaskHighLevel(task1 *persistencespb.TransferTaskInfo, taskType enumsspb.TaskType, namespaceID string, workflowExecution commonpb.WorkflowExecution) {
	s.EqualValues(taskType, task1.TaskType)
	s.Equal(namespaceID, task1.GetNamespaceId())
	s.Equal(workflowExecution.GetWorkflowId(), task1.GetWorkflowId())
	s.Equal(workflowExecution.GetRunId(), task1.GetRunId())
}

func (s *ExecutionManagerSuite) validateTransferTaskTargetInfo(task2 *persistencespb.TransferTaskInfo, targetNamespaceID string, targetWorkflowID string, targetRunID string) {
	s.Equal(targetNamespaceID, task2.GetTargetNamespaceId())
	s.Equal(targetWorkflowID, task2.GetTargetWorkflowId())
	s.Equal(targetRunID, task2.GetTargetRunId())
}

// TestSignalTransferTaskTasks test
func (s *ExecutionManagerSuite) TestSignalTransferTaskTasks() {
	namespaceID := "aeac8287-527b-4b35-80a9-667cb47e7c6d"
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "signal-workflow-test",
		RunId:      "db20f7e2-1a1e-40d9-9278-d8b886738e05",
	}
	workflowKey := definition.NewWorkflowKey(
		namespaceID,
		workflowExecution.WorkflowId,
		workflowExecution.RunId,
	)

	task0, err := s.CreateWorkflowExecution(namespaceID, workflowExecution, "queue1", "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, nil)
	s.NoError(err)
	s.NotNil(task0, "Expected non empty task identifier.")

	taskD, err := s.GetTransferTasks(1, false)
	s.Equal(1, len(taskD), "Expected 1 workflow task.")
	err = s.CompleteTransferTask(taskD[0].GetTaskID())
	s.NoError(err)

	state1, err := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err)
	info1 := state1.ExecutionInfo
	s.NotNil(info1, "Valid Workflow info expected.")
	updatedInfo1 := copyWorkflowExecutionInfo(info1)
	updatedState1 := copyWorkflowExecutionState(state1.ExecutionState)

	targetNamespaceID := "f2bfaab6-7e8b-4fac-9a62-17da8d37becb"
	targetWorkflowID := "target-workflow-signal-id-1"
	targetRunID := "0d00698f-08e1-4d36-a3e2-3bf109f5d2d6"
	targetChildWorkflowOnly := false
	transferTasks := []tasks.Task{&tasks.SignalExecutionTask{
		WorkflowKey:             workflowKey,
		TaskID:                  s.GetNextSequenceNumber(),
		TargetNamespaceID:       targetNamespaceID,
		TargetWorkflowID:        targetWorkflowID,
		TargetRunID:             targetRunID,
		TargetChildWorkflowOnly: targetChildWorkflowOnly,
		InitiatedID:             1,
	}}
	err = s.UpdateWorkflowExecutionWithTransferTasks(updatedInfo1, updatedState1, state1.NextEventId, int64(3), transferTasks, nil)
	s.NoError(err)

	tasks1, err := s.GetTransferTasks(1, false)
	s.NoError(err)
	s.NotNil(tasks1, "expected valid list of tasks.")
	s.Equal(1, len(tasks1), "Expected 1 cancel task.")
	task1 := tasks1[0]
	s.Equal(&tasks.SignalExecutionTask{
		WorkflowKey:             workflowKey,
		VisibilityTimestamp:     task1.GetVisibilityTime(),
		TaskID:                  task1.GetTaskID(),
		TargetNamespaceID:       targetNamespaceID,
		TargetWorkflowID:        targetWorkflowID,
		TargetRunID:             targetRunID,
		TargetChildWorkflowOnly: targetChildWorkflowOnly,
		InitiatedID:             1,
		Version:                 0,
	}, task1)

	err = s.CompleteTransferTask(task1.GetTaskID())
	s.NoError(err)

	targetNamespaceID = "f2bfaab6-7e8b-4fac-9a62-17da8d37becb"
	targetWorkflowID = "target-workflow-signal-id-2"
	targetRunID = ""
	targetChildWorkflowOnly = true
	transferTasks = []tasks.Task{&tasks.SignalExecutionTask{
		WorkflowKey:             workflowKey,
		TaskID:                  s.GetNextSequenceNumber(),
		TargetNamespaceID:       targetNamespaceID,
		TargetWorkflowID:        targetWorkflowID,
		TargetRunID:             targetRunID,
		TargetChildWorkflowOnly: targetChildWorkflowOnly,
		InitiatedID:             3,
	}}

	state2, err := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err)
	info2 := state2.ExecutionInfo
	s.NotNil(info2, "Valid Workflow info expected.")
	updatedInfo2 := copyWorkflowExecutionInfo(info2)
	updatedState2 := copyWorkflowExecutionState(state2.ExecutionState)

	err = s.UpdateWorkflowExecutionWithTransferTasks(updatedInfo2, updatedState2, state2.NextEventId, int64(3), transferTasks, nil)
	s.NoError(err)

	tasks2, err := s.GetTransferTasks(1, false)
	s.NoError(err)
	s.NotNil(tasks2, "expected valid list of tasks.")
	s.Equal(1, len(tasks2), "Expected 1 cancel task.")
	task2 := tasks2[0]
	s.Equal(&tasks.SignalExecutionTask{
		WorkflowKey:             workflowKey,
		VisibilityTimestamp:     task2.GetVisibilityTime(),
		TaskID:                  task2.GetTaskID(),
		TargetNamespaceID:       targetNamespaceID,
		TargetWorkflowID:        targetWorkflowID,
		TargetRunID:             targetRunID,
		TargetChildWorkflowOnly: targetChildWorkflowOnly,
		InitiatedID:             3,
		Version:                 0,
	}, task2)

	err = s.CompleteTransferTask(task2.GetTaskID())
	s.NoError(err)
}

// TestReplicationTasks test
func (s *ExecutionManagerSuite) TestReplicationTasks() {
	namespaceID := "2466d7de-6602-4ad8-b939-fb8f8c36c711"
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "get-replication-tasks-test",
		RunId:      "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
	}
	workflowKey := definition.NewWorkflowKey(
		namespaceID,
		workflowExecution.WorkflowId,
		workflowExecution.RunId,
	)

	task0, err := s.CreateWorkflowExecution(namespaceID, workflowExecution, "queue1", "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, nil)
	s.NoError(err)
	s.NotNil(task0, "Expected non empty task identifier.")
	taskD, err := s.GetTransferTasks(1, false)
	s.Equal(1, len(taskD), "Expected 1 workflow task.")
	err = s.CompleteTransferTask(taskD[0].GetTaskID())
	s.NoError(err)

	state1, err := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err)
	info1 := state1.ExecutionInfo
	s.NotNil(info1, "Valid Workflow info expected.")
	updatedInfo1 := copyWorkflowExecutionInfo(info1)
	updatedState1 := copyWorkflowExecutionState(state1.ExecutionState)

	replicationTasks := []tasks.Task{
		&tasks.HistoryReplicationTask{
			WorkflowKey:  workflowKey,
			TaskID:       s.GetNextSequenceNumber(),
			FirstEventID: int64(1),
			NextEventID:  int64(3),
			Version:      123,
		},
		&tasks.HistoryReplicationTask{
			WorkflowKey:  workflowKey,
			TaskID:       s.GetNextSequenceNumber(),
			FirstEventID: int64(1),
			NextEventID:  int64(3),
			Version:      456,
		},
		&tasks.SyncActivityTask{
			WorkflowKey: workflowKey,
			TaskID:      s.GetNextSequenceNumber(),
			Version:     789,
			ScheduledID: 99,
		},
	}
	err = s.UpdateWorklowStateAndReplication(updatedInfo1, updatedState1, state1.NextEventId, int64(3), replicationTasks)
	s.NoError(err)

	respTasks, err := s.GetReplicationTasks(1, true)
	s.NoError(err)
	s.Equal(len(replicationTasks), len(respTasks))

	for index := range replicationTasks {
		s.Equal(replicationTasks[index].GetTaskID(), respTasks[index].GetTaskID())
		s.Equal(replicationTasks[index].GetVersion(), respTasks[index].GetVersion())
		switch expected := replicationTasks[index].(type) {
		case *tasks.HistoryReplicationTask:
			s.Equal(expected.FirstEventID, respTasks[index].(*tasks.HistoryReplicationTask).FirstEventID)
			s.Equal(expected.NextEventID, respTasks[index].(*tasks.HistoryReplicationTask).NextEventID)
			s.Equal(expected.BranchToken, respTasks[index].(*tasks.HistoryReplicationTask).BranchToken)
			s.Equal(expected.NewRunBranchToken, respTasks[index].(*tasks.HistoryReplicationTask).NewRunBranchToken)

		case *tasks.SyncActivityTask:
			s.Equal(expected.ScheduledID, respTasks[index].(*tasks.SyncActivityTask).ScheduledID)
		}
		err = s.CompleteReplicationTask(respTasks[index].GetTaskID())
		s.NoError(err)
	}
}

// TestTransferTasksComplete test
func (s *ExecutionManagerSuite) TestTransferTasksComplete() {
	namespaceID := "8bfb47be-5b57-4d55-9109-5fb35e20b1d7"
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "get-transfer-tasks-test-complete",
		RunId:      "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
	}
	workflowKey := definition.NewWorkflowKey(
		namespaceID,
		workflowExecution.WorkflowId,
		workflowExecution.RunId,
	)
	taskqueue := "some random taskqueue"

	task0, err0 := s.CreateWorkflowExecution(namespaceID, workflowExecution, taskqueue, "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	tasks1, err1 := s.GetTransferTasks(1, false)
	s.NoError(err1)
	s.NotNil(tasks1, "expected valid list of tasks.")
	s.Equal(1, len(tasks1), "Expected 1 workflow task.")
	task1 := tasks1[0]
	scheduleId := int64(2)
	s.Equal(&tasks.WorkflowTask{
		WorkflowKey:         workflowKey,
		VisibilityTimestamp: task1.GetVisibilityTime(),
		TaskID:              task1.GetTaskID(),
		TaskQueue:           taskqueue,
		ScheduleID:          scheduleId,
		Version:             0,
	}, task1)
	err3 := s.CompleteTransferTask(task1.GetTaskID())
	s.NoError(err3)

	state0, err1 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedState := copyWorkflowExecutionState(state0.ExecutionState)
	updatedInfo.LastWorkflowTaskStartId = int64(2)
	scheduleID := int64(123)
	targetNamespaceID := "8bfb47be-5b57-4d66-9109-5fb35e20b1d0"
	targetWorkflowID := "some random target namespace ID"
	targetRunID := uuid.New()
	currentTransferID := s.GetTransferReadLevel()
	now := time.Now().UTC()

	taskSlice := []tasks.Task{
		&tasks.ActivityTask{workflowKey, now, currentTransferID + 10001, namespaceID, taskqueue, scheduleID, 111},
		&tasks.WorkflowTask{workflowKey, now, currentTransferID + 10002, taskqueue, scheduleID, 222},
		&tasks.CloseExecutionTask{workflowKey, now, currentTransferID + 10003, 333},
		&tasks.CancelExecutionTask{workflowKey, now, currentTransferID + 10004, targetNamespaceID, targetWorkflowID, targetRunID, true, scheduleID, 444},
		&tasks.SignalExecutionTask{workflowKey, now, currentTransferID + 10005, targetNamespaceID, targetWorkflowID, targetRunID, true, scheduleID, 555},
		&tasks.StartChildExecutionTask{workflowKey, now, currentTransferID + 10006, targetNamespaceID, targetWorkflowID, scheduleID, 666},
	}
	err2 := s.UpdateWorklowStateAndReplication(updatedInfo, updatedState, int64(6), int64(3), taskSlice)
	s.NoError(err2)

	txTasks, err1 := s.GetTransferTasks(1, true) // use page size one to force pagination
	s.NoError(err1)
	s.NotNil(txTasks, "expected valid list of tasks.")
	s.Equal(len(taskSlice), len(txTasks))
	for index := range taskSlice {
		t := txTasks[index].GetVisibilityTime()
		s.True(timeComparatorGo(taskSlice[index].GetVisibilityTime(), t, TimePrecision))
	}
	s.IsType(&tasks.ActivityTask{}, txTasks[0])
	s.IsType(&tasks.WorkflowTask{}, txTasks[1])
	s.IsType(&tasks.CloseExecutionTask{}, txTasks[2])
	s.IsType(&tasks.CancelExecutionTask{}, txTasks[3])
	s.IsType(&tasks.SignalExecutionTask{}, txTasks[4])
	s.IsType(&tasks.StartChildExecutionTask{}, txTasks[5])
	s.Equal(int64(111), txTasks[0].GetVersion())
	s.Equal(int64(222), txTasks[1].GetVersion())
	s.Equal(int64(333), txTasks[2].GetVersion())
	s.Equal(int64(444), txTasks[3].GetVersion())
	s.Equal(int64(555), txTasks[4].GetVersion())
	s.Equal(int64(666), txTasks[5].GetVersion())

	err2 = s.CompleteTransferTask(txTasks[0].GetTaskID())
	s.NoError(err2)

	err2 = s.CompleteTransferTask(txTasks[1].GetTaskID())
	s.NoError(err2)

	err2 = s.CompleteTransferTask(txTasks[2].GetTaskID())
	s.NoError(err2)

	err2 = s.CompleteTransferTask(txTasks[3].GetTaskID())
	s.NoError(err2)

	err2 = s.CompleteTransferTask(txTasks[4].GetTaskID())
	s.NoError(err2)

	err2 = s.CompleteTransferTask(txTasks[5].GetTaskID())
	s.NoError(err2)

	txTasks, err2 = s.GetTransferTasks(100, false)
	s.NoError(err2)
	s.Empty(txTasks, "expected empty task queue.")
}

// TestTransferTasksRangeComplete test
func (s *ExecutionManagerSuite) TestTransferTasksRangeComplete() {
	namespaceID := "8bfb47be-5b57-4d55-9109-5fb35e20b1d8"
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "get-transfer-tasks-test-range-complete",
		RunId:      "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
	}
	workflowKey := definition.NewWorkflowKey(
		namespaceID,
		workflowExecution.WorkflowId,
		workflowExecution.RunId,
	)
	taskqueue := "some random taskqueue"

	task0, err0 := s.CreateWorkflowExecution(namespaceID, workflowExecution, taskqueue, "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	tasks1, err1 := s.GetTransferTasks(1, false)
	s.NoError(err1)
	s.NotNil(tasks1, "expected valid list of tasks.")
	s.Equal(1, len(tasks1), "Expected 1 workflow task.")
	task1 := tasks1[0]
	s.Equal(&tasks.WorkflowTask{
		WorkflowKey:         workflowKey,
		VisibilityTimestamp: task1.GetVisibilityTime(),
		TaskID:              task1.GetTaskID(),
		TaskQueue:           taskqueue,
		ScheduleID:          2,
		Version:             0,
	}, task1)

	err3 := s.CompleteTransferTask(task1.GetTaskID())
	s.NoError(err3)

	state0, err1 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedState := copyWorkflowExecutionState(state0.ExecutionState)
	updatedInfo.LastWorkflowTaskStartId = int64(2)
	scheduleID := int64(123)
	targetNamespaceID := "8bfb47be-5b57-4d66-9109-5fb35e20b1d0"
	targetWorkflowID := "some random target namespace ID"
	targetRunID := uuid.New()
	currentTransferID := s.GetTransferReadLevel()
	now := time.Now().UTC()
	taskSlice := []tasks.Task{
		&tasks.ActivityTask{workflowKey, now, currentTransferID + 10001, namespaceID, taskqueue, scheduleID, 111},
		&tasks.WorkflowTask{workflowKey, now, currentTransferID + 10002, taskqueue, scheduleID, 222},
		&tasks.CloseExecutionTask{workflowKey, now, currentTransferID + 10003, 333},
		&tasks.CancelExecutionTask{workflowKey, now, currentTransferID + 10004, targetNamespaceID, targetWorkflowID, targetRunID, true, scheduleID, 444},
		&tasks.SignalExecutionTask{workflowKey, now, currentTransferID + 10005, targetNamespaceID, targetWorkflowID, targetRunID, true, scheduleID, 555},
		&tasks.StartChildExecutionTask{workflowKey, now, currentTransferID + 10006, targetNamespaceID, targetWorkflowID, scheduleID, 666},
	}
	err2 := s.UpdateWorklowStateAndReplication(updatedInfo, updatedState, int64(6), int64(3), taskSlice)
	s.NoError(err2)

	txTasks, err1 := s.GetTransferTasks(2, true) // use page size one to force pagination
	s.NoError(err1)
	s.NotNil(txTasks, "expected valid list of tasks.")
	s.Equal(len(taskSlice), len(txTasks))
	for index := range taskSlice {
		t := txTasks[index].GetVisibilityTime()
		s.True(timeComparatorGo(taskSlice[index].GetVisibilityTime(), t, TimePrecision))
	}
	s.IsType(&tasks.ActivityTask{}, txTasks[0])
	s.IsType(&tasks.WorkflowTask{}, txTasks[1])
	s.IsType(&tasks.CloseExecutionTask{}, txTasks[2])
	s.IsType(&tasks.CancelExecutionTask{}, txTasks[3])
	s.IsType(&tasks.SignalExecutionTask{}, txTasks[4])
	s.IsType(&tasks.StartChildExecutionTask{}, txTasks[5])
	s.Equal(int64(111), txTasks[0].GetVersion())
	s.Equal(int64(222), txTasks[1].GetVersion())
	s.Equal(int64(333), txTasks[2].GetVersion())
	s.Equal(int64(444), txTasks[3].GetVersion())
	s.Equal(int64(555), txTasks[4].GetVersion())
	s.Equal(int64(666), txTasks[5].GetVersion())
	s.Equal(currentTransferID+10001, txTasks[0].GetTaskID())
	s.Equal(currentTransferID+10002, txTasks[1].GetTaskID())
	s.Equal(currentTransferID+10003, txTasks[2].GetTaskID())
	s.Equal(currentTransferID+10004, txTasks[3].GetTaskID())
	s.Equal(currentTransferID+10005, txTasks[4].GetTaskID())
	s.Equal(currentTransferID+10006, txTasks[5].GetTaskID())

	err2 = s.RangeCompleteTransferTask(txTasks[0].GetTaskID()-1, txTasks[5].GetTaskID())
	s.NoError(err2)

	txTasks, err2 = s.GetTransferTasks(100, false)
	s.NoError(err2)
	s.Empty(txTasks, "expected empty task queue.")
}

// TestTimerTasksComplete test
func (s *ExecutionManagerSuite) TestTimerTasksComplete() {
	namespaceID := "8bfb47be-5b57-4d66-9109-5fb35e20b1d7"
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "get-timer-tasks-test-complete",
		RunId:      "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
	}
	workflowKey := definition.NewWorkflowKey(
		namespaceID,
		workflowExecution.WorkflowId,
		workflowExecution.RunId,
	)

	now := time.Now().UTC()
	initialTasks := []tasks.Task{&tasks.WorkflowTaskTimeoutTask{workflowKey, now.Add(1 * time.Second), 1, 2, 3, enumspb.TIMEOUT_TYPE_START_TO_CLOSE, 11}}

	task0, err0 := s.CreateWorkflowExecution(namespaceID, workflowExecution, "taskQueue", "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, initialTasks)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedState := copyWorkflowExecutionState(state0.ExecutionState)
	updatedInfo.LastWorkflowTaskStartId = int64(2)
	taskSlice := []tasks.Task{
		&tasks.WorkflowTimeoutTask{workflowKey, now.Add(2 * time.Second), 2, 12},
		&tasks.DeleteHistoryEventTask{workflowKey, now.Add(2 * time.Second), 3, 13},
		&tasks.ActivityTimeoutTask{workflowKey, now.Add(3 * time.Second), 4, enumspb.TIMEOUT_TYPE_START_TO_CLOSE, 7, 0, 14},
		&tasks.UserTimerTask{workflowKey, now.Add(3 * time.Second), 5, 7, 15},
	}
	err2 := s.UpdateWorkflowExecution(updatedInfo, updatedState, int64(5), []int64{int64(4)}, nil, int64(3), taskSlice, nil, nil, nil, nil)
	s.NoError(err2)

	// try the following a couple of times to give cassandra time to catch up
	var timerTasks []tasks.Task
	for i := 0; i < 3; i++ {
		timerTasks, err1 = s.GetTimerTasks(1, true) // use page size one to force pagination
		s.NoError(err1)
		s.NotNil(timerTasks, "expected valid list of tasks.")
		if len(taskSlice)+len(initialTasks) == len(timerTasks) {
			break
		}
		time.Sleep(time.Second * time.Duration(i))
	}
	s.Equal(len(taskSlice)+len(initialTasks), len(timerTasks))
	s.IsType(&tasks.WorkflowTaskTimeoutTask{}, timerTasks[0])
	s.IsType(&tasks.WorkflowTimeoutTask{}, timerTasks[1])
	s.IsType(&tasks.DeleteHistoryEventTask{}, timerTasks[2])
	s.IsType(&tasks.ActivityTimeoutTask{}, timerTasks[3])
	s.IsType(&tasks.UserTimerTask{}, timerTasks[4])
	s.Equal(int64(11), timerTasks[0].GetVersion())
	s.Equal(int64(12), timerTasks[1].GetVersion())
	s.Equal(int64(13), timerTasks[2].GetVersion())
	s.Equal(int64(14), timerTasks[3].GetVersion())
	s.Equal(int64(15), timerTasks[4].GetVersion())

	visTimer0 := timerTasks[0].GetVisibilityTime()
	visTimer4 := timerTasks[4].GetVisibilityTime().Add(1 * time.Second)
	err2 = s.RangeCompleteTimerTask(visTimer0, visTimer4)
	s.NoError(err2)

	timerTasks2, err2 := s.GetTimerTasks(100, false)
	s.NoError(err2)
	s.Empty(timerTasks2, "expected empty task queue.")
}

// TestTimerTasksRangeComplete test
func (s *ExecutionManagerSuite) TestTimerTasksRangeComplete() {
	namespaceID := "8bfb47be-5b57-4d66-9109-5fb35e20b1d7"
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "get-timer-tasks-test-range-complete",
		RunId:      "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
	}
	workflowKey := definition.NewWorkflowKey(
		namespaceID,
		workflowExecution.WorkflowId,
		workflowExecution.RunId,
	)

	task0, err0 := s.CreateWorkflowExecution(namespaceID, workflowExecution, "taskQueue", "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedState := copyWorkflowExecutionState(state0.ExecutionState)
	updatedInfo.LastWorkflowTaskStartId = int64(2)
	taskSlice := []tasks.Task{
		&tasks.WorkflowTaskTimeoutTask{workflowKey, time.Now().UTC(), 1, 2, 3, enumspb.TIMEOUT_TYPE_START_TO_CLOSE, 11},
		&tasks.WorkflowTimeoutTask{workflowKey, time.Now().UTC(), 2, 12},
		&tasks.DeleteHistoryEventTask{workflowKey, time.Now().UTC(), 3, 13},
		&tasks.ActivityTimeoutTask{workflowKey, time.Now().UTC(), 4, enumspb.TIMEOUT_TYPE_START_TO_CLOSE, 7, 0, 14},
		&tasks.UserTimerTask{workflowKey, time.Now().UTC(), 5, 7, 15},
	}
	err2 := s.UpdateWorkflowExecution(updatedInfo, updatedState, int64(5), []int64{int64(4)}, nil, int64(3), taskSlice, nil, nil, nil, nil)
	s.NoError(err2)

	var timerTasks []tasks.Task
	// Try a couple of times to avoid flakiness
	for i := 0; i < 3; i++ {
		timerTasks, err1 = s.GetTimerTasks(1, true) // use page size one to force pagination
		if len(taskSlice) == len(timerTasks) {
			break
		}
		time.Sleep(time.Second * time.Duration(i))
	}

	s.NoError(err1)
	s.NotNil(timerTasks, "expected valid list of tasks.")
	s.Equal(len(taskSlice), len(timerTasks))
	s.IsType(&tasks.WorkflowTaskTimeoutTask{}, timerTasks[0])
	s.IsType(&tasks.WorkflowTimeoutTask{}, timerTasks[1])
	s.IsType(&tasks.DeleteHistoryEventTask{}, timerTasks[2])
	s.IsType(&tasks.ActivityTimeoutTask{}, timerTasks[3])
	s.IsType(&tasks.UserTimerTask{}, timerTasks[4])
	s.Equal(int64(11), timerTasks[0].GetVersion())
	s.Equal(int64(12), timerTasks[1].GetVersion())
	s.Equal(int64(13), timerTasks[2].GetVersion())
	s.Equal(int64(14), timerTasks[3].GetVersion())
	s.Equal(int64(15), timerTasks[4].GetVersion())

	err2 = s.UpdateWorkflowExecution(updatedInfo, updatedState, int64(5), nil, nil, int64(5), nil, nil, nil, nil, nil)
	s.NoError(err2)

	err2 = s.CompleteTimerTask(timerTasks[0].GetVisibilityTime(), timerTasks[0].GetTaskID())
	s.NoError(err2)

	err2 = s.CompleteTimerTask(timerTasks[1].GetVisibilityTime(), timerTasks[1].GetTaskID())
	s.NoError(err2)

	err2 = s.CompleteTimerTask(timerTasks[2].GetVisibilityTime(), timerTasks[2].GetTaskID())
	s.NoError(err2)

	err2 = s.CompleteTimerTask(timerTasks[3].GetVisibilityTime(), timerTasks[3].GetTaskID())
	s.NoError(err2)

	err2 = s.CompleteTimerTask(timerTasks[4].GetVisibilityTime(), timerTasks[4].GetTaskID())
	s.NoError(err2)

	timerTasks2, err2 := s.GetTimerTasks(100, false)
	s.NoError(err2)
	s.Empty(timerTasks2, "expected empty task queue.")
}

// TestWorkflowMutableStateActivities test
func (s *ExecutionManagerSuite) TestWorkflowMutableStateActivities() {
	namespaceID := "7fcf0aa9-e121-4292-bdad-0a75181b4aa3"
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "test-workflow-mutable-test",
		RunId:      "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
	}

	task0, err0 := s.CreateWorkflowExecution(namespaceID, workflowExecution, "taskQueue", "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedState := copyWorkflowExecutionState(state0.ExecutionState)
	updatedInfo.LastWorkflowTaskStartId = int64(2)
	currentTime := time.Now().UTC()

	activityInfos := []*persistencespb.ActivityInfo{{
		Version:                     7789,
		ScheduleId:                  1,
		ScheduledEventBatchId:       1,
		ScheduledTime:               &currentTime,
		ActivityId:                  uuid.New(),
		RequestId:                   uuid.New(),
		LastHeartbeatDetails:        payloads.EncodeString(uuid.New()),
		StartedId:                   2,
		StartedTime:                 &currentTime,
		ScheduleToCloseTimeout:      timestamp.DurationFromSeconds(1),
		ScheduleToStartTimeout:      timestamp.DurationFromSeconds(2),
		StartToCloseTimeout:         timestamp.DurationFromSeconds(3),
		HeartbeatTimeout:            timestamp.DurationFromSeconds(4),
		LastHeartbeatUpdateTime:     &currentTime,
		TimerTaskStatus:             1,
		CancelRequested:             true,
		CancelRequestId:             math.MaxInt64,
		Attempt:                     math.MaxInt32,
		NamespaceId:                 namespaceID,
		StartedIdentity:             uuid.New(),
		TaskQueue:                   uuid.New(),
		HasRetryPolicy:              true,
		RetryInitialInterval:        timestamp.DurationPtr(math.MaxInt32 * time.Second),
		RetryMaximumInterval:        timestamp.DurationPtr(math.MaxInt32 * time.Second),
		RetryMaximumAttempts:        math.MaxInt32,
		RetryBackoffCoefficient:     5.55,
		RetryExpirationTime:         &currentTime,
		RetryNonRetryableErrorTypes: []string{"accessDenied", "badRequest"},
		RetryLastWorkerIdentity:     uuid.New(),
		RetryLastFailure:            failure.NewServerFailure("some random error", false),
	}}
	err2 := s.UpdateWorkflowExecution(updatedInfo, updatedState, int64(5), []int64{int64(4)}, nil, int64(3), nil, activityInfos, nil, nil, nil)
	s.NoError(err2)

	state, err1 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err1)
	s.NotNil(state, "expected valid state.")
	s.Equal(1, len(state.ActivityInfos))

	ai, ok := state.ActivityInfos[1]
	s.True(ok)
	s.NotNil(ai)
	s.Equal(int64(7789), ai.Version)
	s.Equal(int64(1), ai.ScheduleId)
	s.Equal(int64(1), ai.ScheduledEventBatchId)
	s.EqualTimes(currentTime, *ai.ScheduledTime)
	s.Equal(activityInfos[0].ActivityId, ai.ActivityId)
	s.Equal(activityInfos[0].RequestId, ai.RequestId)
	s.Equal(activityInfos[0].LastHeartbeatDetails, ai.LastHeartbeatDetails)
	s.Equal(int64(2), ai.StartedId)
	s.EqualTimes(currentTime, *ai.StartedTime)
	s.EqualValues(*timestamp.DurationFromSeconds(1), *ai.ScheduleToCloseTimeout)
	s.EqualValues(*timestamp.DurationFromSeconds(2), *ai.ScheduleToStartTimeout)
	s.EqualValues(*timestamp.DurationFromSeconds(3), *ai.StartToCloseTimeout)
	s.EqualValues(*timestamp.DurationFromSeconds(4), *ai.HeartbeatTimeout)
	s.EqualTimes(currentTime, *ai.LastHeartbeatUpdateTime)
	s.Equal(int32(1), ai.TimerTaskStatus)
	s.Equal(activityInfos[0].CancelRequested, ai.CancelRequested)
	s.Equal(activityInfos[0].CancelRequestId, ai.CancelRequestId)
	s.Equal(activityInfos[0].Attempt, ai.Attempt)
	s.Equal(activityInfos[0].NamespaceId, ai.NamespaceId)
	s.Equal(activityInfos[0].StartedIdentity, ai.StartedIdentity)
	s.Equal(activityInfos[0].TaskQueue, ai.TaskQueue)
	s.Equal(activityInfos[0].HasRetryPolicy, ai.HasRetryPolicy)
	s.EqualValues(activityInfos[0].RetryInitialInterval, ai.RetryInitialInterval)
	s.Equal(activityInfos[0].RetryMaximumInterval, ai.RetryMaximumInterval)
	s.Equal(activityInfos[0].RetryMaximumAttempts, ai.RetryMaximumAttempts)
	s.Equal(activityInfos[0].RetryBackoffCoefficient, ai.RetryBackoffCoefficient)
	s.EqualTimes(*activityInfos[0].RetryExpirationTime, *ai.RetryExpirationTime)
	s.Equal(activityInfos[0].RetryNonRetryableErrorTypes, ai.RetryNonRetryableErrorTypes)
	s.Equal(activityInfos[0].RetryLastFailure, ai.RetryLastFailure)
	s.Equal(activityInfos[0].RetryLastWorkerIdentity, ai.RetryLastWorkerIdentity)

	err2 = s.UpdateWorkflowExecution(updatedInfo, updatedState, int64(5), nil, nil, int64(5), nil, nil, []int64{1}, nil, nil)
	s.NoError(err2)

	state, err2 = s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err2)
	s.NotNil(state, "expected valid state.")
	s.Equal(0, len(state.ActivityInfos))
}

// TestWorkflowMutableStateTimers test
func (s *ExecutionManagerSuite) TestWorkflowMutableStateTimers() {
	namespaceID := "025d178a-709b-4c07-8dd7-86dbf9bd2e06"
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "test-workflow-mutable-timers-test",
		RunId:      "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
	}

	task0, err0 := s.CreateWorkflowExecution(namespaceID, workflowExecution, "taskQueue", "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedState := copyWorkflowExecutionState(state0.ExecutionState)
	updatedInfo.LastWorkflowTaskStartId = int64(2)
	currentTime := timestamp.TimePtr(time.Date(2078, 8, 22, 12, 59, 59, 999999, time.UTC))
	timerID := "id_1"
	timerInfos := []*persistencespb.TimerInfo{{
		Version:    3345,
		TimerId:    timerID,
		ExpiryTime: currentTime,
		TaskStatus: 2,
		StartedId:  5,
	}}
	err2 := s.UpdateWorkflowExecution(updatedInfo, updatedState, int64(5), []int64{int64(4)}, nil, int64(3), nil, nil, nil, timerInfos, nil)
	s.NoError(err2)

	state, err1 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err1)
	s.NotNil(state, "expected valid state.")
	s.Equal(1, len(state.TimerInfos))
	s.Equal(int64(3345), state.TimerInfos[timerID].Version)
	s.Equal(timerID, state.TimerInfos[timerID].GetTimerId())
	s.Equal(currentTime, state.TimerInfos[timerID].ExpiryTime)
	s.Equal(int64(2), state.TimerInfos[timerID].TaskStatus)
	s.Equal(int64(5), state.TimerInfos[timerID].GetStartedId())

	err2 = s.UpdateWorkflowExecution(updatedInfo, updatedState, int64(5), nil, nil, int64(5), nil, nil, nil, nil, []string{timerID})
	s.NoError(err2)

	state, err2 = s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err2)
	s.NotNil(state, "expected valid state.")
	s.Equal(0, len(state.TimerInfos))
}

// TestWorkflowMutableStateChildExecutions test
func (s *ExecutionManagerSuite) TestWorkflowMutableStateChildExecutions() {
	namespaceID := "88236cd2-c439-4cec-9957-2748ce3be074"
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "test-workflow-mutable-child-executions-parent-test",
		RunId:      "c63dba1e-929c-4fbf-8ec5-4533b16269a9",
	}

	parentNamespaceID := "6036ded3-e541-42c9-8f69-3d9354dad081"
	parentExecution := commonpb.WorkflowExecution{
		WorkflowId: "test-workflow-mutable-child-executions-child-test",
		RunId:      "73e89362-25ec-4305-bcb8-d9448b90856c",
	}

	task0, err0 := s.CreateChildWorkflowExecution(namespaceID, workflowExecution, parentNamespaceID, parentExecution, 1, "taskQueue", "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")
	s.Equal(parentNamespaceID, info0.ParentNamespaceId)
	s.Equal(parentExecution.GetWorkflowId(), info0.ParentWorkflowId)
	s.Equal(parentExecution.GetRunId(), info0.ParentRunId)
	s.Equal(int64(1), info0.InitiatedId)

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedState := copyWorkflowExecutionState(state0.ExecutionState)
	updatedInfo.LastWorkflowTaskStartId = int64(2)
	createRequestID := uuid.New()
	childExecutionInfos := []*persistencespb.ChildExecutionInfo{{
		Version:           1234,
		InitiatedId:       1,
		StartedId:         2,
		CreateRequestId:   createRequestID,
		ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_TERMINATE,
	}}
	err2 := s.UpsertChildExecutionsState(updatedInfo, updatedState, int64(5), int64(3), childExecutionInfos)
	s.NoError(err2)

	state, err1 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err1)
	s.NotNil(state, "expected valid state.")
	s.Equal(1, len(state.ChildExecutionInfos))
	ci, ok := state.ChildExecutionInfos[1]
	s.True(ok)
	s.NotNil(ci)
	s.Equal(int64(1234), ci.Version)
	s.Equal(int64(1), ci.InitiatedId)
	s.Equal(enumspb.PARENT_CLOSE_POLICY_TERMINATE, ci.ParentClosePolicy)
	s.Equal(int64(2), ci.StartedId)
	s.Equal(createRequestID, ci.CreateRequestId)

	err2 = s.DeleteChildExecutionsState(updatedInfo, updatedState, int64(5), int64(5), int64(1))
	s.NoError(err2)

	state, err2 = s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err2)
	s.NotNil(state, "expected valid state.")
	s.Equal(0, len(state.ChildExecutionInfos))
}

// TestWorkflowMutableStateRequestCancel test
func (s *ExecutionManagerSuite) TestWorkflowMutableStateRequestCancel() {
	namespaceID := "568b8d19-cf64-4aac-be1b-f8a3edbc1fa9"
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "test-workflow-mutable-request-cancel-test",
		RunId:      "87f96253-b925-426e-90db-aa4ee89b5aca",
	}

	task0, err0 := s.CreateWorkflowExecution(namespaceID, workflowExecution, "taskQueue", "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedState := copyWorkflowExecutionState(state0.ExecutionState)
	updatedInfo.LastWorkflowTaskStartId = int64(2)
	requestCancelInfo := &persistencespb.RequestCancelInfo{
		Version:               456,
		InitiatedId:           2,
		InitiatedEventBatchId: 1,
		CancelRequestId:       uuid.New(),
	}
	err2 := s.UpsertRequestCancelState(updatedInfo, updatedState, int64(5), int64(3), []*persistencespb.RequestCancelInfo{requestCancelInfo})
	s.NoError(err2)

	state, err1 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err1)
	s.NotNil(state, "expected valid state.")
	s.Equal(1, len(state.RequestCancelInfos))
	ri, ok := state.RequestCancelInfos[requestCancelInfo.GetInitiatedId()]
	s.True(ok)
	s.NotNil(ri)
	s.Equal(requestCancelInfo, ri)

	err2 = s.DeleteCancelState(updatedInfo, updatedState, int64(5), int64(5), requestCancelInfo.GetInitiatedId())
	s.NoError(err2)

	state, err2 = s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err2)
	s.NotNil(state, "expected valid state.")
	s.Equal(0, len(state.RequestCancelInfos))
}

// TestWorkflowMutableStateSignalInfo test
func (s *ExecutionManagerSuite) TestWorkflowMutableStateSignalInfo() {
	namespaceID := uuid.New()
	runID := uuid.New()
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "test-workflow-mutable-signal-info-test",
		RunId:      runID,
	}

	task0, err0 := s.CreateWorkflowExecution(namespaceID, workflowExecution, "taskQueue", "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedState := copyWorkflowExecutionState(state0.ExecutionState)
	updatedInfo.LastWorkflowTaskStartId = int64(2)
	signalInfo := &persistencespb.SignalInfo{
		Version:               123,
		InitiatedId:           2,
		InitiatedEventBatchId: 1,
		RequestId:             uuid.New(),
	}
	err2 := s.UpsertSignalInfoState(updatedInfo, updatedState, int64(5), int64(3), []*persistencespb.SignalInfo{signalInfo})
	s.NoError(err2)

	state, err1 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err1)
	s.NotNil(state, "expected valid state.")
	s.Equal(1, len(state.SignalInfos))
	si, ok := state.SignalInfos[signalInfo.GetInitiatedId()]
	s.True(ok)
	s.NotNil(si)
	s.Equal(signalInfo, si)

	err2 = s.DeleteSignalState(updatedInfo, updatedState, int64(5), int64(5), signalInfo.GetInitiatedId())
	s.NoError(err2)

	state, err2 = s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err2)
	s.NotNil(state, "expected valid state.")
	s.Equal(0, len(state.SignalInfos))
}

// TestWorkflowMutableStateSignalRequested test
func (s *ExecutionManagerSuite) TestWorkflowMutableStateSignalRequested() {
	namespaceID := uuid.New()
	runID := uuid.New()
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "test-workflow-mutable-signal-requested-test",
		RunId:      runID,
	}

	task0, err0 := s.CreateWorkflowExecution(namespaceID, workflowExecution, "taskQueue", "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedState := copyWorkflowExecutionState(state0.ExecutionState)
	updatedInfo.LastWorkflowTaskStartId = int64(2)
	signalRequestedID := uuid.New()
	signalsRequested := []string{signalRequestedID}
	err2 := s.UpsertSignalsRequestedState(updatedInfo, updatedState, int64(5), int64(3), signalsRequested)
	s.NoError(err2)

	state, err1 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err1)
	s.NotNil(state, "expected valid state.")
	s.Equal(1, len(state.SignalRequestedIds))
	s.Equal(signalRequestedID, state.SignalRequestedIds[0])

	err2 = s.DeleteSignalsRequestedState(updatedInfo, updatedState, int64(5), int64(5), []string{signalRequestedID, uuid.New()})
	s.NoError(err2)

	state, err2 = s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err2)
	s.NotNil(state, "expected valid state.")
	s.Equal(0, len(state.SignalRequestedIds))
}

// TestWorkflowMutableStateInfo test
func (s *ExecutionManagerSuite) TestWorkflowMutableStateInfo() {
	namespaceID := "9ed8818b-3090-4160-9f21-c6b70e64d2dd"
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "test-workflow-mutable-state-test",
		RunId:      "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
	}

	task0, err0 := s.CreateWorkflowExecution(namespaceID, workflowExecution, "taskQueue", "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedState := copyWorkflowExecutionState(state0.ExecutionState)
	updatedNextEventID := int64(5)
	updatedInfo.LastWorkflowTaskStartId = int64(2)

	err2 := s.UpdateWorkflowExecution(updatedInfo, updatedState, updatedNextEventID, []int64{int64(4)}, nil, int64(3), nil, nil, nil, nil, nil)
	s.NoError(err2)

	state, err1 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err1)
	s.NotNil(state, "expected valid state.")
	s.NotNil(state.ExecutionInfo, "expected valid MS Info state.")
	s.Equal(updatedNextEventID, state.NextEventId)
	s.Equal(updatedState.State, state.ExecutionState.State)
}

// TestContinueAsNew test
func (s *ExecutionManagerSuite) TestContinueAsNew() {
	namespaceID := "c1c0bb55-04e6-4a9c-89d0-1be7b96459f8"
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "continue-as-new-workflow-test",
		RunId:      "551c88d2-d9e6-404f-8131-9eec14f36643",
	}

	_, err0 := s.CreateWorkflowExecution(namespaceID, workflowExecution, "queue1", "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, nil)
	s.NoError(err0)

	state0, err1 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	continueAsNewInfo := copyWorkflowExecutionInfo(info0)
	continueAsNewState := copyWorkflowExecutionState(state0.ExecutionState)
	continueAsNewState.State = enumsspb.WORKFLOW_EXECUTION_STATE_CREATED
	continueAsNewState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	continueAsNewNextEventID := int64(5)
	continueAsNewInfo.LastWorkflowTaskStartId = int64(2)

	newWorkflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "continue-as-new-workflow-test",
		RunId:      "64c7e15a-3fd7-4182-9c6f-6f25a4fa2614",
	}

	testResetPoints := workflowpb.ResetPoints{
		Points: []*workflowpb.ResetPointInfo{
			{
				BinaryChecksum:               "test-binary-checksum",
				RunId:                        "test-runID",
				FirstWorkflowTaskCompletedId: 123,
				CreateTime:                   timestamp.TimePtr(time.Date(2020, 8, 22, 0, 0, 0, 0, time.UTC)),
				Resettable:                   true,
				ExpireTime:                   timestamp.TimePtr(time.Date(2020, 8, 22, 0, 0, 0, 0, time.UTC)),
			},
		},
	}

	err2 := s.ContinueAsNewExecution(continueAsNewInfo, continueAsNewState, continueAsNewNextEventID, state0.NextEventId, newWorkflowExecution, int64(3), int64(2), &testResetPoints)
	s.NoError(err2)

	prevExecutionState, err3 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err3)
	prevExecutionInfo := prevExecutionState.ExecutionInfo
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, prevExecutionState.ExecutionState.State)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW, prevExecutionState.ExecutionState.Status)
	s.Equal(int64(5), prevExecutionState.NextEventId)
	s.Equal(int64(2), prevExecutionInfo.LastWorkflowTaskStartId)
	s.True(reflect.DeepEqual(prevExecutionInfo.AutoResetPoints, &workflowpb.ResetPoints{}))

	newExecutionState, err4 := s.GetWorkflowMutableState(namespaceID, newWorkflowExecution)
	s.NoError(err4)
	newExecutionInfo := newExecutionState.ExecutionInfo
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_CREATED, newExecutionState.ExecutionState.State)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, newExecutionState.ExecutionState.Status)
	s.Equal(int64(3), newExecutionState.NextEventId)
	s.Equal(common.EmptyEventID, newExecutionInfo.LastWorkflowTaskStartId)
	s.Equal(int64(2), newExecutionInfo.WorkflowTaskScheduleId)
	s.Equal(testResetPoints.String(), newExecutionInfo.AutoResetPoints.String())

	newRunID, err5 := s.GetCurrentWorkflowRunID(namespaceID, workflowExecution.GetWorkflowId())
	s.NoError(err5)
	s.Equal(newWorkflowExecution.GetRunId(), newRunID)
}

// TestReplicationTransferTaskTasks test
func (s *ExecutionManagerSuite) TestReplicationTransferTaskTasks() {
	s.ClearReplicationQueue()
	namespaceID := "2466d7de-6602-4ad8-b939-fb8f8c36c711"
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "replication-transfer-task-test",
		RunId:      "dcde9d85-5d7a-43c7-8b18-cb2cae0e29e0",
	}
	workflowKey := definition.NewWorkflowKey(
		namespaceID,
		workflowExecution.WorkflowId,
		workflowExecution.RunId,
	)

	task0, err := s.CreateWorkflowExecution(namespaceID, workflowExecution, "queue1", "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, nil)
	s.NoError(err)
	s.NotNil(task0, "Expected non empty task identifier.")

	taskD, err := s.GetTransferTasks(1, false)
	s.Equal(1, len(taskD), "Expected 1 workflow task.")
	err = s.CompleteTransferTask(taskD[0].GetTaskID())
	s.NoError(err)

	state1, err := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err)
	info1 := state1.ExecutionInfo
	s.NotNil(info1, "Valid Workflow info expected.")
	updatedInfo1 := copyWorkflowExecutionInfo(info1)
	updatedState1 := copyWorkflowExecutionState(state1.ExecutionState)

	replicationTasks := []tasks.Task{&tasks.HistoryReplicationTask{
		WorkflowKey:  workflowKey,
		TaskID:       s.GetNextSequenceNumber(),
		FirstEventID: int64(1),
		NextEventID:  int64(3),
		Version:      int64(9),
	}}
	err = s.UpdateWorklowStateAndReplication(updatedInfo1, updatedState1, state1.NextEventId, int64(3), replicationTasks)
	s.NoError(err)

	tasks1, err := s.GetReplicationTasks(1, false)
	s.NoError(err)
	s.NotNil(tasks1, "expected valid list of tasks.")
	s.Equal(1, len(tasks1), "Expected 1 replication task.")
	task1 := tasks1[0]
	s.Equal(&tasks.HistoryReplicationTask{
		WorkflowKey:         workflowKey,
		VisibilityTimestamp: task1.GetVisibilityTime(),
		TaskID:              task1.GetTaskID(),
		FirstEventID:        1,
		NextEventID:         3,
		Version:             9,
		BranchToken:         nil,
		NewRunBranchToken:   nil,
	}, task1)

	err = s.CompleteTransferTask(task1.GetTaskID())
	s.NoError(err)
	tasks2, err := s.GetReplicationTasks(1, false)
	s.NoError(err)
	s.Equal(0, len(tasks2))
}

// TestReplicationTransferTaskRangeComplete test
func (s *ExecutionManagerSuite) TestReplicationTransferTaskRangeComplete() {
	s.ClearReplicationQueue()
	namespaceID := uuid.New()
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "replication-transfer-task--range-complete-test",
		RunId:      uuid.New(),
	}
	workflowKey := definition.NewWorkflowKey(
		namespaceID,
		workflowExecution.WorkflowId,
		workflowExecution.RunId,
	)

	task0, err := s.CreateWorkflowExecution(namespaceID, workflowExecution, "queue1", "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, nil)
	s.NoError(err)
	s.NotNil(task0, "Expected non empty task identifier.")

	taskD, err := s.GetTransferTasks(1, false)
	s.Equal(1, len(taskD), "Expected 1 workflow task.")
	err = s.CompleteTransferTask(taskD[0].GetTaskID())
	s.NoError(err)

	state1, err := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err)
	info1 := state1.ExecutionInfo
	s.NotNil(info1, "Valid Workflow info expected.")
	updatedInfo1 := copyWorkflowExecutionInfo(info1)
	updatedState1 := copyWorkflowExecutionState(state1.ExecutionState)

	replicationTasks := []tasks.Task{
		&tasks.HistoryReplicationTask{
			WorkflowKey:  workflowKey,
			TaskID:       s.GetNextSequenceNumber(),
			FirstEventID: int64(1),
			NextEventID:  int64(3),
			Version:      int64(9),
		},
		&tasks.HistoryReplicationTask{
			WorkflowKey:  workflowKey,
			TaskID:       s.GetNextSequenceNumber(),
			FirstEventID: int64(4),
			NextEventID:  int64(5),
			Version:      int64(9),
		},
	}
	err = s.UpdateWorklowStateAndReplication(
		updatedInfo1,
		updatedState1,
		state1.NextEventId,
		int64(3),
		replicationTasks,
	)
	s.NoError(err)

	tasks1, err := s.GetReplicationTasks(1, false)
	s.NoError(err)
	s.NotNil(tasks1, "expected valid list of tasks.")
	s.Equal(1, len(tasks1), "Expected 1 replication task.")
	task1 := tasks1[0]
	s.Equal(&tasks.HistoryReplicationTask{
		WorkflowKey:         workflowKey,
		VisibilityTimestamp: task1.GetVisibilityTime(),
		TaskID:              task1.GetTaskID(),
		FirstEventID:        1,
		NextEventID:         3,
		Version:             9,
		BranchToken:         nil,
		NewRunBranchToken:   nil,
	}, task1)

	err = s.RangeCompleteReplicationTask(task1.GetTaskID())
	s.NoError(err)
	tasks2, err := s.GetReplicationTasks(1, false)
	s.NoError(err)
	s.NotNil(tasks2, "expected valid list of tasks.")
	task2 := tasks2[0]
	s.Equal(&tasks.HistoryReplicationTask{
		WorkflowKey:         workflowKey,
		VisibilityTimestamp: task2.GetVisibilityTime(),
		TaskID:              task2.GetTaskID(),
		FirstEventID:        4,
		NextEventID:         5,
		Version:             9,
		BranchToken:         nil,
		NewRunBranchToken:   nil,
	}, task2)
	err = s.CompleteReplicationTask(task2.GetTaskID())
	s.NoError(err)
	tasks3, err := s.GetReplicationTasks(1, false)
	s.NoError(err)
	s.Equal(0, len(tasks3))
}

// TestCreateGetShardBackfill test
func (s *ExecutionManagerSuite) TestCreateGetShardBackfill() {
	shardID := int32(4)
	rangeID := int64(59)

	// test create && get
	currentReplicationAck := int64(27)
	currentClusterTransferAck := int64(21)
	currentClusterTimerAck := timestampConvertor(time.Now().UTC().Add(-10 * time.Second))
	shardInfo := &persistencespb.ShardInfo{
		ShardId:                 shardID,
		Owner:                   "some random owner",
		RangeId:                 rangeID,
		StolenSinceRenew:        12,
		UpdateTime:              timestampConvertor(time.Now().UTC()),
		ReplicationAckLevel:     currentReplicationAck,
		TransferAckLevel:        currentClusterTransferAck,
		TimerAckLevelTime:       currentClusterTimerAck,
		ClusterReplicationLevel: map[string]int64{},
		ReplicationDlqAckLevel:  map[string]int64{},
	}
	request := &p.GetOrCreateShardRequest{
		ShardID:          shardID,
		InitialShardInfo: shardInfo,
	}
	resp, err := s.ShardMgr.GetOrCreateShard(request)
	s.NoError(err)

	shardInfo.ClusterTransferAckLevel = map[string]int64{
		s.ClusterMetadata.GetCurrentClusterName(): currentClusterTransferAck,
	}
	shardInfo.ClusterTimerAckLevel = map[string]*time.Time{
		s.ClusterMetadata.GetCurrentClusterName(): currentClusterTimerAck,
	}
	resp, err = s.ShardMgr.GetOrCreateShard(&p.GetOrCreateShardRequest{ShardID: shardID})
	s.NoError(err)
	s.True(timeComparatorGoPtr(shardInfo.UpdateTime, resp.ShardInfo.UpdateTime, TimePrecision))
	s.True(timeComparatorGoPtr(shardInfo.ClusterTimerAckLevel[cluster.TestCurrentClusterName], resp.ShardInfo.ClusterTimerAckLevel[cluster.TestCurrentClusterName], TimePrecision))
	s.True(timeComparatorGoPtr(shardInfo.ClusterTimerAckLevel[cluster.TestAlternativeClusterName], resp.ShardInfo.ClusterTimerAckLevel[cluster.TestAlternativeClusterName], TimePrecision))
	s.Equal(shardInfo.TimerAckLevelTime.UnixNano(), resp.ShardInfo.TimerAckLevelTime.UnixNano())
	resp.ShardInfo.TimerAckLevelTime = shardInfo.TimerAckLevelTime
	resp.ShardInfo.UpdateTime = shardInfo.UpdateTime
	resp.ShardInfo.ClusterTimerAckLevel = shardInfo.ClusterTimerAckLevel
	s.Equal(shardInfo, resp.ShardInfo)
}

// TestCreateGetUpdateGetShard test
func (s *ExecutionManagerSuite) TestCreateGetUpdateGetShard() {
	shardID := int32(8)
	rangeID := int64(59)

	// test create && get
	currentReplicationAck := int64(27)
	currentClusterTransferAck := int64(21)
	alternativeClusterTransferAck := int64(32)
	currentClusterTimerAck := timestampConvertor(time.Now().UTC().Add(-10 * time.Second))
	alternativeClusterTimerAck := timestampConvertor(time.Now().UTC().Add(-20 * time.Second))
	namespaceNotificationVersion := int64(8192)
	shardInfo := &persistencespb.ShardInfo{
		ShardId:             shardID,
		Owner:               "some random owner",
		RangeId:             rangeID,
		StolenSinceRenew:    12,
		UpdateTime:          timestampConvertor(time.Now().UTC()),
		ReplicationAckLevel: currentReplicationAck,
		TransferAckLevel:    currentClusterTransferAck,
		TimerAckLevelTime:   currentClusterTimerAck,
		ClusterTransferAckLevel: map[string]int64{
			cluster.TestCurrentClusterName:     currentClusterTransferAck,
			cluster.TestAlternativeClusterName: alternativeClusterTransferAck,
		},
		ClusterTimerAckLevel: map[string]*time.Time{
			cluster.TestCurrentClusterName:     currentClusterTimerAck,
			cluster.TestAlternativeClusterName: alternativeClusterTimerAck,
		},
		NamespaceNotificationVersion: namespaceNotificationVersion,
		ClusterReplicationLevel:      map[string]int64{},
		ReplicationDlqAckLevel:       map[string]int64{},
	}
	createRequest := &p.GetOrCreateShardRequest{
		ShardID:          shardID,
		InitialShardInfo: shardInfo,
	}
	resp, err := s.ShardMgr.GetOrCreateShard(createRequest)
	s.NoError(err)
	s.True(timeComparatorGoPtr(shardInfo.UpdateTime, resp.ShardInfo.UpdateTime, TimePrecision))
	s.True(timeComparatorGoPtr(shardInfo.ClusterTimerAckLevel[cluster.TestCurrentClusterName], resp.ShardInfo.ClusterTimerAckLevel[cluster.TestCurrentClusterName], TimePrecision))
	s.True(timeComparatorGoPtr(shardInfo.ClusterTimerAckLevel[cluster.TestAlternativeClusterName], resp.ShardInfo.ClusterTimerAckLevel[cluster.TestAlternativeClusterName], TimePrecision))
	s.Equal(shardInfo.TimerAckLevelTime.UnixNano(), resp.ShardInfo.TimerAckLevelTime.UnixNano())
	resp.ShardInfo.TimerAckLevelTime = shardInfo.TimerAckLevelTime
	resp.ShardInfo.UpdateTime = shardInfo.UpdateTime
	resp.ShardInfo.ClusterTimerAckLevel = shardInfo.ClusterTimerAckLevel
	s.Equal(shardInfo, resp.ShardInfo)

	// test update && get
	currentReplicationAck = int64(270)
	currentClusterTransferAck = int64(210)
	alternativeClusterTransferAck = int64(320)
	currentClusterTimerAck = timestampConvertor(time.Now().UTC().Add(-100 * time.Second))
	alternativeClusterTimerAck = timestampConvertor(time.Now().UTC().Add(-200 * time.Second))
	namespaceNotificationVersion = int64(16384)
	shardInfo = &persistencespb.ShardInfo{
		ShardId:             shardID,
		Owner:               "some random owner",
		RangeId:             int64(28),
		StolenSinceRenew:    4,
		UpdateTime:          timestampConvertor(time.Now().UTC()),
		ReplicationAckLevel: currentReplicationAck,
		TransferAckLevel:    currentClusterTransferAck,
		TimerAckLevelTime:   currentClusterTimerAck,
		ClusterTransferAckLevel: map[string]int64{
			cluster.TestCurrentClusterName:     currentClusterTransferAck,
			cluster.TestAlternativeClusterName: alternativeClusterTransferAck,
		},
		ClusterTimerAckLevel: map[string]*time.Time{
			cluster.TestCurrentClusterName:     currentClusterTimerAck,
			cluster.TestAlternativeClusterName: alternativeClusterTimerAck,
		},
		NamespaceNotificationVersion: namespaceNotificationVersion,
		ClusterReplicationLevel:      map[string]int64{cluster.TestAlternativeClusterName: 12345},
		ReplicationDlqAckLevel:       map[string]int64{},
	}
	updateRequest := &p.UpdateShardRequest{
		ShardInfo:       shardInfo,
		PreviousRangeID: rangeID,
	}
	s.Nil(s.ShardMgr.UpdateShard(updateRequest))

	resp, err = s.ShardMgr.GetOrCreateShard(&p.GetOrCreateShardRequest{ShardID: shardID})
	s.NoError(err)
	s.True(timeComparatorGoPtr(shardInfo.UpdateTime, resp.ShardInfo.UpdateTime, TimePrecision))
	s.True(timeComparatorGoPtr(shardInfo.ClusterTimerAckLevel[cluster.TestCurrentClusterName], resp.ShardInfo.ClusterTimerAckLevel[cluster.TestCurrentClusterName], TimePrecision))
	s.True(timeComparatorGoPtr(shardInfo.ClusterTimerAckLevel[cluster.TestAlternativeClusterName], resp.ShardInfo.ClusterTimerAckLevel[cluster.TestAlternativeClusterName], TimePrecision))
	s.Equal(shardInfo.TimerAckLevelTime.UnixNano(), resp.ShardInfo.TimerAckLevelTime.UnixNano())
	resp.ShardInfo.UpdateTime = shardInfo.UpdateTime
	resp.ShardInfo.TimerAckLevelTime = shardInfo.TimerAckLevelTime
	resp.ShardInfo.ClusterTimerAckLevel = shardInfo.ClusterTimerAckLevel
	s.Equal(shardInfo, resp.ShardInfo)
}

// TestReplicationDLQ test
func (s *ExecutionManagerSuite) TestReplicationDLQ() {
	sourceCluster := "test"
	taskInfo := &persistencespb.ReplicationTaskInfo{
		NamespaceId: uuid.New(),
		WorkflowId:  uuid.New(),
		RunId:       uuid.New(),
		TaskId:      0,
		TaskType:    1,
	}
	err := s.PutReplicationTaskToDLQ(sourceCluster, taskInfo)
	s.NoError(err)
	resp, err := s.GetReplicationTasksFromDLQ(sourceCluster, -1, 0, 1, nil)
	s.NoError(err)
	s.Len(resp.Tasks, 1)
	err = s.DeleteReplicationTaskFromDLQ(sourceCluster, 0)
	s.NoError(err)
	resp, err = s.GetReplicationTasksFromDLQ(sourceCluster, -1, 0, 1, nil)
	s.NoError(err)
	s.Len(resp.Tasks, 0)

	taskInfo1 := &persistencespb.ReplicationTaskInfo{
		NamespaceId: uuid.New(),
		WorkflowId:  uuid.New(),
		RunId:       uuid.New(),
		TaskId:      1,
		TaskType:    2,
	}
	taskInfo2 := &persistencespb.ReplicationTaskInfo{
		NamespaceId: uuid.New(),
		WorkflowId:  uuid.New(),
		RunId:       uuid.New(),
		TaskId:      2,
		TaskType:    1,
	}
	err = s.PutReplicationTaskToDLQ(sourceCluster, taskInfo1)
	s.NoError(err)
	err = s.PutReplicationTaskToDLQ(sourceCluster, taskInfo2)
	s.NoError(err)
	resp, err = s.GetReplicationTasksFromDLQ(sourceCluster, 0, 2, 2, nil)
	s.NoError(err)
	s.Len(resp.Tasks, 2)
	err = s.RangeDeleteReplicationTaskFromDLQ(sourceCluster, 0, 2)
	s.NoError(err)
	resp, err = s.GetReplicationTasksFromDLQ(sourceCluster, 0, 2, 2, nil)
	s.NoError(err)
	s.Len(resp.Tasks, 0)
}

func copyWorkflowExecutionInfo(sourceInfo *persistencespb.WorkflowExecutionInfo) *persistencespb.WorkflowExecutionInfo {
	return &persistencespb.WorkflowExecutionInfo{
		NamespaceId:                sourceInfo.NamespaceId,
		WorkflowId:                 sourceInfo.WorkflowId,
		ParentNamespaceId:          sourceInfo.ParentNamespaceId,
		ParentWorkflowId:           sourceInfo.ParentWorkflowId,
		ParentRunId:                sourceInfo.ParentRunId,
		InitiatedId:                sourceInfo.InitiatedId,
		TaskQueue:                  sourceInfo.TaskQueue,
		WorkflowTypeName:           sourceInfo.WorkflowTypeName,
		WorkflowRunTimeout:         sourceInfo.WorkflowRunTimeout,
		DefaultWorkflowTaskTimeout: sourceInfo.DefaultWorkflowTaskTimeout,
		LastFirstEventId:           sourceInfo.LastFirstEventId,
		LastWorkflowTaskStartId:    sourceInfo.LastWorkflowTaskStartId,
		WorkflowTaskVersion:        sourceInfo.WorkflowTaskVersion,
		WorkflowTaskScheduleId:     sourceInfo.WorkflowTaskScheduleId,
		WorkflowTaskStartedId:      sourceInfo.WorkflowTaskStartedId,
		WorkflowTaskRequestId:      sourceInfo.WorkflowTaskRequestId,
		WorkflowTaskTimeout:        sourceInfo.WorkflowTaskTimeout,
		AutoResetPoints:            sourceInfo.AutoResetPoints,
		ExecutionStats: &persistencespb.ExecutionStats{
			HistorySize: sourceInfo.ExecutionStats.HistorySize,
		},
		StartTime:      sourceInfo.StartTime,
		LastUpdateTime: sourceInfo.LastUpdateTime,
	}
}

func copyWorkflowExecutionState(sourceState *persistencespb.WorkflowExecutionState) *persistencespb.WorkflowExecutionState {
	return &persistencespb.WorkflowExecutionState{
		RunId:           sourceState.RunId,
		State:           sourceState.State,
		Status:          sourceState.Status,
		CreateRequestId: sourceState.CreateRequestId,
	}
}

// Note: cassandra only provide millisecond precision timestamp
// ref: https://docs.datastax.com/en/cql/3.3/cql/cql_reference/timestamp_type_r.html
// so to use equal function, we need to do conversion, getting rid of sub milliseconds
func timestampConvertor(t time.Time) *time.Time {
	r := t.Round(time.Millisecond)
	return &r
}

func timeComparatorGoPtr(t1, t2 *time.Time, timeTolerance time.Duration) bool {
	return timeComparatorGo(timestamp.TimeValue(t1), timestamp.TimeValue(t2), timeTolerance)
}

func timeComparatorGo(t1, t2 time.Time, timeTolerance time.Duration) bool {
	diff := t2.Sub(t1)
	if diff.Nanoseconds() <= timeTolerance.Nanoseconds() {
		return true
	}
	return false
}
