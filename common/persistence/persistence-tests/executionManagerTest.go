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
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/persistenceblobs/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/checksum"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
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

var testWorkflowChecksum = checksum.Checksum{
	Version: 22,
	Flavor:  checksum.FlavorIEEECRC32OverProto3Binary,
	Value:   []byte("test-checksum"),
}

// SetupSuite implementation
func (s *ExecutionManagerSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}
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

func (s *ExecutionManagerSuite) newRandomChecksum() checksum.Checksum {
	return checksum.Checksum{
		Flavor:  checksum.FlavorIEEECRC32OverProto3Binary,
		Version: 22,
		Value:   []byte(uuid.NewRandom()),
	}
}

func (s *ExecutionManagerSuite) assertChecksumsEqual(expected checksum.Checksum, actual checksum.Checksum) {
	if !actual.Flavor.IsValid() {
		// not all stores support checksum persistence today
		// if its not supported, assert that everything is zero'd out
		expected = checksum.Checksum{}
	}
	s.EqualValues(expected, actual)
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
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &p.WorkflowExecutionInfo{
				CreateRequestID:            uuid.New(),
				NamespaceID:                namespaceID,
				WorkflowID:                 workflowID,
				RunID:                      runID,
				TaskQueue:                  taskqueue,
				WorkflowTypeName:           workflowType,
				WorkflowRunTimeout:         workflowTimeout,
				DefaultWorkflowTaskTimeout: workflowTaskTimeout,
				LastFirstEventID:           common.FirstEventID,
				NextEventID:                nextEventID,
				LastProcessedEvent:         lastProcessedEventID,
				State:                      enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
				Status:                     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
			ExecutionStats: &persistenceblobs.ExecutionStats{},
			Checksum:       csum,
		},
		RangeID: s.ShardInfo.GetRangeId(),
		Mode:    p.CreateWorkflowModeBrandNew,
	}

	_, err := s.ExecutionManager.CreateWorkflowExecution(req)
	s.Nil(err)
	info, err := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.Nil(err)
	s.assertChecksumsEqual(csum, info.Checksum)
	updatedInfo := copyWorkflowExecutionInfo(info.ExecutionInfo)
	updatedStats := copyExecutionStats(info.ExecutionStats)
	updatedInfo.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	updatedInfo.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	_, err = s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		UpdateWorkflowMutation: p.WorkflowMutation{
			ExecutionInfo:  updatedInfo,
			ExecutionStats: updatedStats,
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
	s.IsType(&p.WorkflowExecutionAlreadyStartedError{}, err)
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
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &p.WorkflowExecutionInfo{
				CreateRequestID:            uuid.New(),
				NamespaceID:                namespaceID,
				TaskQueue:                  taskqueue,
				WorkflowTypeName:           workflowType,
				WorkflowRunTimeout:         workflowTimeout,
				DefaultWorkflowTaskTimeout: workflowTaskTimeout,
				LastFirstEventID:           common.FirstEventID,
				NextEventID:                nextEventID,
				LastProcessedEvent:         lastProcessedEventID,
			},
			ExecutionStats: &persistenceblobs.ExecutionStats{},
			Checksum:       csum,
		},
		RangeID: s.ShardInfo.GetRangeId(),
		Mode:    p.CreateWorkflowModeBrandNew,
	}

	workflowExecutionStatusCreated := commonpb.WorkflowExecution{
		WorkflowId: "create-workflow-test-state-created",
		RunId:      uuid.New(),
	}
	req.NewWorkflowSnapshot.ExecutionInfo.WorkflowID = workflowExecutionStatusCreated.GetWorkflowId()
	req.NewWorkflowSnapshot.ExecutionInfo.RunID = workflowExecutionStatusCreated.GetRunId()
	req.NewWorkflowSnapshot.ExecutionInfo.State = enumsspb.WORKFLOW_EXECUTION_STATE_CREATED
	for _, invalidStatus := range invalidStatuses {
		req.NewWorkflowSnapshot.ExecutionInfo.Status = invalidStatus
		_, err := s.ExecutionManager.CreateWorkflowExecution(req)
		s.IsType(&serviceerror.Internal{}, err)
	}
	req.NewWorkflowSnapshot.ExecutionInfo.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	_, err := s.ExecutionManager.CreateWorkflowExecution(req)
	s.Nil(err)
	info, err := s.GetWorkflowExecutionInfo(namespaceID, workflowExecutionStatusCreated)
	s.Nil(err)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_CREATED, info.ExecutionInfo.State)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.ExecutionInfo.Status)
	s.assertChecksumsEqual(csum, info.Checksum)

	workflowExecutionStatusRunning := commonpb.WorkflowExecution{
		WorkflowId: "create-workflow-test-state-running",
		RunId:      uuid.New(),
	}
	req.NewWorkflowSnapshot.ExecutionInfo.WorkflowID = workflowExecutionStatusRunning.GetWorkflowId()
	req.NewWorkflowSnapshot.ExecutionInfo.RunID = workflowExecutionStatusRunning.GetRunId()
	req.NewWorkflowSnapshot.ExecutionInfo.State = enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING
	for _, invalidStatus := range invalidStatuses {
		req.NewWorkflowSnapshot.ExecutionInfo.Status = invalidStatus
		_, err := s.ExecutionManager.CreateWorkflowExecution(req)
		s.IsType(&serviceerror.Internal{}, err)
	}
	req.NewWorkflowSnapshot.ExecutionInfo.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	_, err = s.ExecutionManager.CreateWorkflowExecution(req)
	s.Nil(err)
	info, err = s.GetWorkflowExecutionInfo(namespaceID, workflowExecutionStatusRunning)
	s.Nil(err)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, info.ExecutionInfo.State)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.ExecutionInfo.Status)
	s.assertChecksumsEqual(csum, info.Checksum)

	workflowExecutionStatusCompleted := commonpb.WorkflowExecution{
		WorkflowId: "create-workflow-test-state-completed",
		RunId:      uuid.New(),
	}
	req.NewWorkflowSnapshot.ExecutionInfo.WorkflowID = workflowExecutionStatusCompleted.GetWorkflowId()
	req.NewWorkflowSnapshot.ExecutionInfo.RunID = workflowExecutionStatusCompleted.GetRunId()
	req.NewWorkflowSnapshot.ExecutionInfo.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	for _, invalidStatus := range invalidStatuses {
		req.NewWorkflowSnapshot.ExecutionInfo.Status = invalidStatus
		_, err := s.ExecutionManager.CreateWorkflowExecution(req)
		s.IsType(&serviceerror.Internal{}, err)
	}
	req.NewWorkflowSnapshot.ExecutionInfo.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	_, err = s.ExecutionManager.CreateWorkflowExecution(req)
	s.IsType(&serviceerror.Internal{}, err)

	// for zombie workflow creation, we must use existing workflow ID which got created
	// since we do not allow creation of zombie workflow without current record
	workflowExecutionStatusZombie := commonpb.WorkflowExecution{
		WorkflowId: workflowExecutionStatusRunning.WorkflowId,
		RunId:      uuid.New(),
	}
	req.Mode = p.CreateWorkflowModeZombie
	req.NewWorkflowSnapshot.ExecutionInfo.WorkflowID = workflowExecutionStatusZombie.GetWorkflowId()
	req.NewWorkflowSnapshot.ExecutionInfo.RunID = workflowExecutionStatusZombie.GetRunId()
	req.NewWorkflowSnapshot.ExecutionInfo.State = enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE
	for _, invalidStatus := range invalidStatuses {
		req.NewWorkflowSnapshot.ExecutionInfo.Status = invalidStatus
		_, err := s.ExecutionManager.CreateWorkflowExecution(req)
		s.IsType(&serviceerror.Internal{}, err)
	}
	req.NewWorkflowSnapshot.ExecutionInfo.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	_, err = s.ExecutionManager.CreateWorkflowExecution(req)
	s.Nil(err)
	info, err = s.GetWorkflowExecutionInfo(namespaceID, workflowExecutionStatusZombie)
	s.Nil(err)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE, info.ExecutionInfo.State)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.ExecutionInfo.Status)
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
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &p.WorkflowExecutionInfo{
				CreateRequestID:            uuid.New(),
				NamespaceID:                namespaceID,
				WorkflowID:                 workflowID,
				RunID:                      workflowExecutionZombie1.GetRunId(),
				TaskQueue:                  taskqueue,
				WorkflowTypeName:           workflowType,
				WorkflowRunTimeout:         workflowTimeout,
				DefaultWorkflowTaskTimeout: workflowTaskTimeout,
				NextEventID:                nextEventID,
				LastProcessedEvent:         lastProcessedEventID,
				State:                      enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
				Status:                     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
			ExecutionStats: &persistenceblobs.ExecutionStats{},
			Checksum:       csum,
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
	req.NewWorkflowSnapshot.ExecutionInfo.RunID = workflowExecutionRunning.GetRunId()
	req.Mode = p.CreateWorkflowModeBrandNew
	req.NewWorkflowSnapshot.ExecutionInfo.State = enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING
	req.NewWorkflowSnapshot.ExecutionInfo.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	_, err = s.ExecutionManager.CreateWorkflowExecution(req)
	s.Nil(err)
	currentRunID, err := s.GetCurrentWorkflowRunID(namespaceID, workflowID)
	s.Nil(err)
	s.Equal(workflowExecutionRunning.GetRunId(), currentRunID)

	workflowExecutionZombie := commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      uuid.New(),
	}
	req.NewWorkflowSnapshot.ExecutionInfo.RunID = workflowExecutionZombie.GetRunId()
	req.Mode = p.CreateWorkflowModeZombie
	req.NewWorkflowSnapshot.ExecutionInfo.State = enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE
	req.NewWorkflowSnapshot.ExecutionInfo.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	_, err = s.ExecutionManager.CreateWorkflowExecution(req)
	s.Nil(err)
	// current run ID is still the prev running run ID
	currentRunID, err = s.GetCurrentWorkflowRunID(namespaceID, workflowExecutionRunning.GetWorkflowId())
	s.Nil(err)
	s.Equal(workflowExecutionRunning.GetRunId(), currentRunID)
	info, err := s.GetWorkflowExecutionInfo(namespaceID, workflowExecutionZombie)
	s.Nil(err)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE, info.ExecutionInfo.State)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.ExecutionInfo.Status)
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
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &p.WorkflowExecutionInfo{
				CreateRequestID:            uuid.New(),
				NamespaceID:                namespaceID,
				WorkflowID:                 workflowExecution.GetWorkflowId(),
				RunID:                      workflowExecution.GetRunId(),
				TaskQueue:                  taskqueue,
				WorkflowTypeName:           workflowType,
				WorkflowRunTimeout:         workflowTimeout,
				DefaultWorkflowTaskTimeout: workflowTaskTimeout,
				LastFirstEventID:           common.FirstEventID,
				NextEventID:                nextEventID,
				LastProcessedEvent:         lastProcessedEventID,
			},
			ExecutionStats: &persistenceblobs.ExecutionStats{},
			Checksum:       csum,
		},
		RangeID: s.ShardInfo.GetRangeId(),
		Mode:    p.CreateWorkflowModeBrandNew,
	}

	req.NewWorkflowSnapshot.ExecutionInfo.State = enumsspb.WORKFLOW_EXECUTION_STATE_CREATED
	req.NewWorkflowSnapshot.ExecutionInfo.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	_, err := s.ExecutionManager.CreateWorkflowExecution(req)
	s.Nil(err)
	info, err := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.Nil(err)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_CREATED, info.ExecutionInfo.State)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.ExecutionInfo.Status)
	s.assertChecksumsEqual(csum, info.Checksum)

	csum = s.newRandomChecksum() // update the checksum to new value
	updatedInfo := copyWorkflowExecutionInfo(info.ExecutionInfo)
	updatedStats := copyExecutionStats(info.ExecutionStats)
	updatedInfo.State = enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING
	updatedInfo.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	_, err = s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		UpdateWorkflowMutation: p.WorkflowMutation{
			ExecutionInfo:  updatedInfo,
			ExecutionStats: updatedStats,
			Condition:      nextEventID,
			Checksum:       csum,
		},
		RangeID: s.ShardInfo.GetRangeId(),
		Mode:    p.UpdateWorkflowModeUpdateCurrent,
	})
	s.NoError(err)
	info, err = s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.Nil(err)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, info.ExecutionInfo.State)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.ExecutionInfo.Status)
	s.assertChecksumsEqual(csum, info.Checksum)

	updatedInfo = copyWorkflowExecutionInfo(info.ExecutionInfo)
	updatedStats = copyExecutionStats(info.ExecutionStats)
	updatedInfo.State = enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING
	for _, status := range statuses {
		updatedInfo.Status = status
		_, err = s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
			UpdateWorkflowMutation: p.WorkflowMutation{
				ExecutionInfo:  updatedInfo,
				ExecutionStats: updatedStats,
				Condition:      nextEventID,
			},
			RangeID: s.ShardInfo.GetRangeId(),
			Mode:    p.UpdateWorkflowModeUpdateCurrent,
		})
		s.IsType(&serviceerror.Internal{}, err)
	}

	updatedInfo = copyWorkflowExecutionInfo(info.ExecutionInfo)
	updatedStats = copyExecutionStats(info.ExecutionStats)
	updatedInfo.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	updatedInfo.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	_, err = s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		UpdateWorkflowMutation: p.WorkflowMutation{
			ExecutionInfo:  updatedInfo,
			ExecutionStats: updatedStats,
			Condition:      nextEventID,
		},
		RangeID: s.ShardInfo.GetRangeId(),
		Mode:    p.UpdateWorkflowModeUpdateCurrent,
	})
	s.IsType(&serviceerror.Internal{}, err)

	for _, status := range statuses {
		updatedInfo.Status = status
		_, err = s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
			UpdateWorkflowMutation: p.WorkflowMutation{
				ExecutionInfo:  updatedInfo,
				ExecutionStats: updatedStats,
				Condition:      nextEventID,
			},
			RangeID: s.ShardInfo.GetRangeId(),
			Mode:    p.UpdateWorkflowModeUpdateCurrent,
		})
		s.Nil(err)
		info, err = s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
		s.Nil(err)
		s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, info.ExecutionInfo.State)
		s.EqualValues(status, info.ExecutionInfo.Status)
	}

	// create a new workflow with same namespace ID & workflow ID
	// to enable update workflow with zombie status
	workflowExecutionRunning := commonpb.WorkflowExecution{
		WorkflowId: workflowExecution.WorkflowId,
		RunId:      uuid.New(),
	}
	req.NewWorkflowSnapshot.ExecutionInfo.WorkflowID = workflowExecutionRunning.GetWorkflowId()
	req.NewWorkflowSnapshot.ExecutionInfo.RunID = workflowExecutionRunning.GetRunId()
	req.Mode = p.CreateWorkflowModeWorkflowIDReuse
	req.PreviousRunID = workflowExecution.GetRunId()
	req.PreviousLastWriteVersion = common.EmptyVersion
	req.NewWorkflowSnapshot.ExecutionInfo.State = enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING
	req.NewWorkflowSnapshot.ExecutionInfo.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	_, err = s.ExecutionManager.CreateWorkflowExecution(req)
	s.Nil(err)

	updatedInfo = copyWorkflowExecutionInfo(info.ExecutionInfo)
	updatedStats = copyExecutionStats(info.ExecutionStats)
	updatedInfo.State = enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE
	updatedInfo.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	_, err = s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		UpdateWorkflowMutation: p.WorkflowMutation{
			ExecutionInfo:  updatedInfo,
			ExecutionStats: updatedStats,
			Condition:      nextEventID,
		},
		RangeID: s.ShardInfo.GetRangeId(),
		Mode:    p.UpdateWorkflowModeBypassCurrent,
	})
	s.NoError(err)
	info, err = s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.Nil(err)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE, info.ExecutionInfo.State)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.ExecutionInfo.Status)

	updatedInfo = copyWorkflowExecutionInfo(info.ExecutionInfo)
	updatedStats = copyExecutionStats(info.ExecutionStats)
	updatedInfo.State = enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE
	for _, status := range statuses {
		updatedInfo.Status = status
		_, err = s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
			UpdateWorkflowMutation: p.WorkflowMutation{
				ExecutionInfo:  updatedInfo,
				ExecutionStats: updatedStats,
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
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &p.WorkflowExecutionInfo{
				CreateRequestID:            uuid.New(),
				NamespaceID:                namespaceID,
				WorkflowID:                 workflowExecution.GetWorkflowId(),
				RunID:                      workflowExecution.GetRunId(),
				TaskQueue:                  taskqueue,
				WorkflowTypeName:           workflowType,
				WorkflowRunTimeout:         workflowTimeout,
				DefaultWorkflowTaskTimeout: workflowTaskTimeout,
				NextEventID:                nextEventID,
				LastProcessedEvent:         lastProcessedEventID,
				State:                      enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
				Status:                     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
			ExecutionStats: &persistenceblobs.ExecutionStats{},
			Checksum:       csum,
		},
		RangeID: s.ShardInfo.GetRangeId(),
		Mode:    p.CreateWorkflowModeBrandNew,
	}
	_, err := s.ExecutionManager.CreateWorkflowExecution(req)
	s.Nil(err)
	currentRunID, err := s.GetCurrentWorkflowRunID(namespaceID, workflowID)
	s.Nil(err)
	s.Equal(workflowExecution.GetRunId(), currentRunID)

	info, err := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.Nil(err)
	s.assertChecksumsEqual(csum, info.Checksum)

	// try to turn current workflow into zombie state, this should end with an error
	updatedInfo := copyWorkflowExecutionInfo(info.ExecutionInfo)
	updateStats := copyExecutionStats(info.ExecutionStats)
	updatedInfo.State = enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE
	updatedInfo.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	_, err = s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		UpdateWorkflowMutation: p.WorkflowMutation{
			ExecutionInfo:  updatedInfo,
			ExecutionStats: updateStats,
			Condition:      nextEventID,
			Checksum:       csum,
		},
		RangeID: s.ShardInfo.GetRangeId(),
		Mode:    p.UpdateWorkflowModeBypassCurrent,
	})
	s.NotNil(err)

	updatedInfo = copyWorkflowExecutionInfo(info.ExecutionInfo)
	updateStats = copyExecutionStats(info.ExecutionStats)
	updatedInfo.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	updatedInfo.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	_, err = s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		UpdateWorkflowMutation: p.WorkflowMutation{
			ExecutionInfo:  updatedInfo,
			ExecutionStats: updateStats,
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
	csum = checksum.Checksum{} // set checksum to nil
	req.NewWorkflowSnapshot.ExecutionInfo.WorkflowID = workflowExecutionRunning.GetWorkflowId()
	req.NewWorkflowSnapshot.ExecutionInfo.RunID = workflowExecutionRunning.GetRunId()
	req.Mode = p.CreateWorkflowModeWorkflowIDReuse
	req.PreviousRunID = workflowExecution.GetRunId()
	req.PreviousLastWriteVersion = common.EmptyVersion
	req.NewWorkflowSnapshot.ExecutionInfo.State = enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING
	req.NewWorkflowSnapshot.ExecutionInfo.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	req.NewWorkflowSnapshot.Checksum = csum
	_, err = s.ExecutionManager.CreateWorkflowExecution(req)
	s.Nil(err)
	currentRunID, err = s.GetCurrentWorkflowRunID(namespaceID, workflowID)
	s.Nil(err)
	s.Equal(workflowExecutionRunning.GetRunId(), currentRunID)

	// get the workflow to be turned into a zombie
	info, err = s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.Nil(err)
	s.assertChecksumsEqual(csum, info.Checksum)
	updatedInfo = copyWorkflowExecutionInfo(info.ExecutionInfo)
	updateStats = copyExecutionStats(info.ExecutionStats)
	updatedInfo.State = enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE
	updatedInfo.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	_, err = s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		UpdateWorkflowMutation: p.WorkflowMutation{
			ExecutionInfo:  updatedInfo,
			ExecutionStats: updateStats,
			Condition:      nextEventID,
			Checksum:       csum,
		},
		RangeID: s.ShardInfo.GetRangeId(),
		Mode:    p.UpdateWorkflowModeBypassCurrent,
	})
	s.NoError(err)
	info, err = s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.Nil(err)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE, info.ExecutionInfo.State)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.ExecutionInfo.Status)
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
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &p.WorkflowExecutionInfo{
				CreateRequestID:            uuid.New(),
				NamespaceID:                namespaceID,
				WorkflowID:                 workflowExecution.GetWorkflowId(),
				RunID:                      workflowExecution.GetRunId(),
				TaskQueue:                  taskqueue,
				WorkflowTypeName:           workflowType,
				WorkflowRunTimeout:         workflowTimeout,
				DefaultWorkflowTaskTimeout: workflowTaskTimeout,
				State:                      enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
				Status:                     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				LastFirstEventID:           common.FirstEventID,
				NextEventID:                nextEventID,
				LastProcessedEvent:         lastProcessedEventID,
			},
			ExecutionStats: &persistenceblobs.ExecutionStats{},
		},
		RangeID: s.ShardInfo.GetRangeId(),
		Mode:    p.CreateWorkflowModeBrandNew,
	}

	_, err := s.ExecutionManager.CreateWorkflowExecution(req)
	s.Nil(err)
	_, err = s.ExecutionManager.CreateWorkflowExecution(req)
	s.NotNil(err)
	alreadyStartedErr, ok := err.(*p.WorkflowExecutionAlreadyStartedError)
	s.True(ok, "err is not WorkflowExecutionAlreadyStartedError")
	s.Equal(req.NewWorkflowSnapshot.ExecutionInfo.CreateRequestID, alreadyStartedErr.StartRequestID)
	s.Equal(workflowExecution.GetRunId(), alreadyStartedErr.RunID)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, alreadyStartedErr.Status)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, alreadyStartedErr.State)
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

	state0, err1 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err1)
	s.assertChecksumsEqual(testWorkflowChecksum, state0.Checksum)
	info0 := state0.ExecutionInfo
	closeInfo := copyWorkflowExecutionInfo(info0)
	closeInfo.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	closeInfo.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	closeInfo.NextEventID = int64(5)
	closeInfo.LastProcessedEvent = int64(2)

	err2 := s.UpdateWorkflowExecution(closeInfo, state0.ExecutionStats, nil, nil, nil, nextEventID,
		nil, nil, nil, nil, nil)
	s.NoError(err2)

	newExecution := commonpb.WorkflowExecution{
		WorkflowId: workflowExecution.GetWorkflowId(),
		RunId:      uuid.New(),
	}
	// this create should work since we are relying the business logic in history engine
	// to check whether the existing running workflow has finished
	_, err3 := s.ExecutionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &p.WorkflowExecutionInfo{
				CreateRequestID:            uuid.New(),
				NamespaceID:                namespaceID,
				WorkflowID:                 newExecution.GetWorkflowId(),
				RunID:                      newExecution.GetRunId(),
				TaskQueue:                  taskqueue,
				WorkflowTypeName:           workflowType,
				WorkflowRunTimeout:         workflowTimeout,
				DefaultWorkflowTaskTimeout: workflowTaskTimeout,
				State:                      enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
				Status:                     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				LastFirstEventID:           common.FirstEventID,
				NextEventID:                nextEventID,
				LastProcessedEvent:         lastProcessedEventID,
			},
			ExecutionStats: &persistenceblobs.ExecutionStats{},
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

			state0, err1 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
			s.NoError(err1)
			info0 := state0.ExecutionInfo
			continueAsNewInfo := copyWorkflowExecutionInfo(info0)
			continueAsNewInfo.State = enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING
			continueAsNewInfo.NextEventID = int64(5)
			continueAsNewInfo.LastProcessedEvent = int64(2)

			err2 := s.ContinueAsNewExecution(continueAsNewInfo, state0.ExecutionStats, info0.NextEventID, newExecution, int64(3), int64(2), nil)
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
		s.Fail("More than one error: %v", lastError.Error())
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
	task0, err0 := s.CreateWorkflowExecution(namespaceID, workflowExecution, "queue1", "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	task1, err1 := s.CreateWorkflowExecution(namespaceID, workflowExecution, "queue1", "wType1", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(14), 3, 0, 2, nil)
	s.Error(err1, "Expected workflow creation to fail.")
	log.Infof("Unable to start workflow execution: %v", err1)
	startedErr, ok := err1.(*p.WorkflowExecutionAlreadyStartedError)
	s.True(ok, fmt.Sprintf("Expected WorkflowExecutionAlreadyStartedError, but actual is %v", err1))
	s.Equal(workflowExecution.GetRunId(), startedErr.RunID, startedErr.Msg)

	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, startedErr.State, startedErr.Msg)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, startedErr.Status, startedErr.Msg)
	s.Equal(common.EmptyVersion, startedErr.LastWriteVersion, startedErr.Msg)
	s.Empty(task1, "Expected empty task identifier.")

	response, err2 := s.ExecutionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &p.WorkflowExecutionInfo{
				CreateRequestID:            uuid.New(),
				NamespaceID:                namespaceID,
				WorkflowID:                 workflowExecution.GetWorkflowId(),
				RunID:                      workflowExecution.GetRunId(),
				TaskQueue:                  "queue1",
				WorkflowTypeName:           "workflow_type_test",
				WorkflowRunTimeout:         timestamp.DurationFromSeconds(20),
				DefaultWorkflowTaskTimeout: timestamp.DurationFromSeconds(13),
				State:                      enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
				Status:                     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				LastFirstEventID:           common.FirstEventID,
				NextEventID:                int64(3),
				LastProcessedEvent:         0,
				WorkflowTaskScheduleID:     int64(2),
				WorkflowTaskStartedID:      common.EmptyEventID,
				WorkflowTaskTimeout:        timestamp.DurationFromSeconds(1),
			},
			ExecutionStats: &persistenceblobs.ExecutionStats{},
			TransferTasks: []p.Task{
				&p.WorkflowTask{
					TaskID:      s.GetNextSequenceNumber(),
					NamespaceID: namespaceID,
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
	log.Infof("Unable to start workflow execution: %v", err2)
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
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &p.WorkflowExecutionInfo{
				CreateRequestID:            uuid.New(),
				NamespaceID:                uuid.New(),
				WorkflowID:                 "get-workflow-test",
				RunID:                      uuid.New(),
				FirstExecutionRunID:        uuid.New(),
				ParentNamespaceID:          uuid.New(),
				ParentWorkflowID:           "get-workflow-test-parent",
				ParentRunID:                uuid.New(),
				InitiatedID:                rand.Int63(),
				TaskQueue:                  "get-wf-test-taskqueue",
				WorkflowTypeName:           "code.uber.internal/test/workflow",
				WorkflowRunTimeout:         timestamp.DurationFromSeconds(int64(rand.Int31())),
				DefaultWorkflowTaskTimeout: timestamp.DurationFromSeconds(int64(rand.Int31())),
				State:                      enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
				Status:                     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				LastFirstEventID:           common.FirstEventID,
				NextEventID:                rand.Int63(),
				LastProcessedEvent:         int64(rand.Int31()),
				SignalCount:                rand.Int63(),
				WorkflowTaskVersion:        int64(rand.Int31()),
				WorkflowTaskScheduleID:     int64(rand.Int31()),
				WorkflowTaskStartedID:      int64(rand.Int31()),
				WorkflowTaskTimeout:        timestamp.DurationFromSeconds(int64(rand.Int31())),
				Attempt:                    rand.Int31(),
				HasRetryPolicy:             true,
				InitialInterval:            timestamp.DurationFromSeconds(int64(rand.Int31())),
				BackoffCoefficient:         7.78,
				MaximumInterval:            timestamp.DurationFromSeconds(int64(rand.Int31())),
				WorkflowExpirationTime:     timestamp.TimeNowPtrUtc(),
				MaximumAttempts:            rand.Int31(),
				NonRetryableErrorTypes:     []string{"badRequestError", "accessDeniedError"},
				CronSchedule:               "* * * * *",
				AutoResetPoints:            &testResetPoints,
				SearchAttributes:           testSearchAttr,
				Memo:                       testMemo,
			},
			ExecutionStats: &persistenceblobs.ExecutionStats{
				HistorySize: int64(rand.Int31()),
			},
			Checksum: csum,
		},
		Mode: p.CreateWorkflowModeBrandNew,
	}

	createResp, err := s.ExecutionManager.CreateWorkflowExecution(createReq)
	s.NoError(err)
	s.NotNil(createResp, "Expected non empty task identifier.")

	state, err := s.GetWorkflowExecutionInfo(createReq.NewWorkflowSnapshot.ExecutionInfo.NamespaceID,
		commonpb.WorkflowExecution{
			WorkflowId: createReq.NewWorkflowSnapshot.ExecutionInfo.WorkflowID,
			RunId:      createReq.NewWorkflowSnapshot.ExecutionInfo.RunID,
		})
	s.NoError(err)
	info := state.ExecutionInfo
	s.NotNil(info, "Valid Workflow response expected.")
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.CreateRequestID, info.CreateRequestID)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.NamespaceID, info.NamespaceID)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.WorkflowID, info.WorkflowID)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.RunID, info.RunID)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.FirstExecutionRunID, info.FirstExecutionRunID)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.ParentNamespaceID, info.ParentNamespaceID)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.ParentWorkflowID, info.ParentWorkflowID)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.ParentRunID, info.ParentRunID)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.InitiatedID, info.InitiatedID)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.TaskQueue, info.TaskQueue)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.WorkflowTypeName, info.WorkflowTypeName)
	s.EqualValues(*createReq.NewWorkflowSnapshot.ExecutionInfo.WorkflowRunTimeout, *info.WorkflowRunTimeout)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.DefaultWorkflowTaskTimeout, info.DefaultWorkflowTaskTimeout)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, info.State)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.Status)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.NextEventID, info.NextEventID)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.LastProcessedEvent, info.LastProcessedEvent)
	s.True(s.validateTimeRange(timestamp.TimeValue(info.LastUpdatedTimestamp), time.Hour))
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.WorkflowTaskVersion, info.WorkflowTaskVersion)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.WorkflowTaskScheduleID, info.WorkflowTaskScheduleID)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.WorkflowTaskStartedID, info.WorkflowTaskStartedID)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.WorkflowTaskTimeout, info.WorkflowTaskTimeout)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.SignalCount, info.SignalCount)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.Attempt, info.Attempt)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.HasRetryPolicy, info.HasRetryPolicy)
	s.EqualValues(createReq.NewWorkflowSnapshot.ExecutionInfo.InitialInterval, info.InitialInterval)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.BackoffCoefficient, info.BackoffCoefficient)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.MaximumAttempts, info.MaximumAttempts)
	s.EqualValues(createReq.NewWorkflowSnapshot.ExecutionInfo.MaximumInterval, info.MaximumInterval)
	s.EqualTimes(*createReq.NewWorkflowSnapshot.ExecutionInfo.WorkflowExpirationTime, *info.WorkflowExpirationTime)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.CronSchedule, info.CronSchedule)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.NonRetryableErrorTypes, info.NonRetryableErrorTypes)
	s.Equal(testResetPoints.String(), info.AutoResetPoints.String())
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionStats.HistorySize, state.ExecutionStats.HistorySize)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionStats.HistorySize, state.ExecutionInfo.ExecutionStats.HistorySize)
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

	state0, err1 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")
	s.Equal(namespaceID, info0.NamespaceID)
	s.Equal("update-workflow-test", info0.WorkflowID)
	s.Equal("5ba5e531-e46b-48d9-b4b3-859919839553", info0.RunID)
	s.Equal("queue1", info0.TaskQueue)
	s.Equal("wType", info0.WorkflowTypeName)
	s.EqualValues(int64(20), info0.WorkflowRunTimeout.Seconds())
	s.EqualValues(int64(13), info0.DefaultWorkflowTaskTimeout.Seconds())
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, info0.State)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info0.Status)
	s.Equal(int64(1), info0.LastFirstEventID)
	s.Equal(int64(3), info0.NextEventID)
	s.Equal(int64(0), info0.LastProcessedEvent)
	s.True(s.validateTimeRange(*info0.LastUpdatedTimestamp, time.Hour))
	s.Equal(int64(0), info0.WorkflowTaskVersion)
	s.Equal(int64(2), info0.WorkflowTaskScheduleID)
	s.Equal(common.EmptyEventID, info0.WorkflowTaskStartedID)
	s.EqualValues(1, int64(info0.WorkflowTaskTimeout.Seconds()))
	s.Equal(int32(0), info0.WorkflowTaskAttempt)
	s.Nil(info0.WorkflowTaskStartedTimestamp)
	s.Nil(info0.WorkflowTaskScheduledTimestamp)
	s.Nil(info0.WorkflowTaskOriginalScheduledTimestamp)
	s.Empty(info0.StickyTaskQueue)
	s.EqualValues(int64(0), timestamp.DurationValue(info0.StickyScheduleToStartTimeout))
	s.Empty(info0.ClientLibraryVersion)
	s.Empty(info0.ClientFeatureVersion)
	s.Empty(info0.ClientImpl)
	s.Equal(int64(0), info0.SignalCount)
	s.True(reflect.DeepEqual(info0.AutoResetPoints, &workflowpb.ResetPoints{}))
	s.True(len(info0.SearchAttributes) == 0)
	s.True(len(info0.Memo) == 0)
	s.assertChecksumsEqual(testWorkflowChecksum, state0.Checksum)

	log.Infof("Workflow execution last updated: %v", info0.LastUpdatedTimestamp)

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedStats := state0.ExecutionStats
	updatedInfo.LastFirstEventID = int64(3)
	updatedInfo.NextEventID = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)
	updatedInfo.WorkflowTaskVersion = int64(666)
	updatedInfo.WorkflowTaskAttempt = 123
	updatedInfo.WorkflowTaskStartedTimestamp = timestamp.UnixOrZeroTimePtr(321)
	updatedInfo.WorkflowTaskScheduledTimestamp = timestamp.UnixOrZeroTimePtr(654)
	updatedInfo.WorkflowTaskOriginalScheduledTimestamp = timestamp.UnixOrZeroTimePtr(655)
	updatedInfo.StickyTaskQueue = "random sticky taskqueue"
	updatedInfo.StickyScheduleToStartTimeout = timestamp.DurationFromSeconds(876)
	updatedInfo.ClientLibraryVersion = "random client library version"
	updatedInfo.ClientFeatureVersion = "random client feature version"
	updatedInfo.ClientImpl = "random client impl"
	updatedInfo.SignalCount = 9
	updatedInfo.InitialInterval = timestamp.DurationFromSeconds(math.MaxInt32)
	updatedInfo.BackoffCoefficient = 4.45
	updatedInfo.MaximumInterval = timestamp.DurationFromSeconds(math.MaxInt32)
	updatedInfo.MaximumAttempts = math.MaxInt32
	updatedInfo.WorkflowExpirationTime = timestamp.TimeNowPtrUtc()
	updatedInfo.NonRetryableErrorTypes = []string{"accessDenied", "badRequest"}
	searchAttrKey := "env"
	searchAttrVal := payload.EncodeBytes([]byte("test"))
	updatedInfo.SearchAttributes = map[string]*commonpb.Payload{searchAttrKey: searchAttrVal}

	memoKey := "memoKey"
	memoVal := payload.EncodeBytes([]byte("memoVal"))
	updatedInfo.Memo = map[string]*commonpb.Payload{memoKey: memoVal}
	updatedStats.HistorySize = math.MaxInt64

	err2 := s.UpdateWorkflowExecution(updatedInfo, updatedStats, nil, []int64{int64(4)}, nil, int64(3), nil, nil, nil, nil, nil)
	s.NoError(err2)

	state1, err3 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err3)
	info1 := state1.ExecutionInfo
	s.NotNil(info1, "Valid Workflow info expected.")
	s.Equal(namespaceID, info1.NamespaceID)
	s.Equal("update-workflow-test", info1.WorkflowID)
	s.Equal("5ba5e531-e46b-48d9-b4b3-859919839553", info1.RunID)
	s.Equal("queue1", info1.TaskQueue)
	s.Equal("wType", info1.WorkflowTypeName)
	s.EqualValues(int64(20), info1.WorkflowRunTimeout.Seconds())
	s.EqualValues(13, int64(info1.DefaultWorkflowTaskTimeout.Seconds()))
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, info1.State)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info1.Status)
	s.Equal(int64(3), info1.LastFirstEventID)
	s.Equal(int64(5), info1.NextEventID)
	s.Equal(int64(2), info1.LastProcessedEvent)
	s.True(s.validateTimeRange(*info1.LastUpdatedTimestamp, time.Hour))
	s.Equal(int64(666), info1.WorkflowTaskVersion)
	s.Equal(int64(2), info1.WorkflowTaskScheduleID)
	s.Equal(common.EmptyEventID, info1.WorkflowTaskStartedID)
	s.EqualValues(1, int64(info1.WorkflowTaskTimeout.Seconds()))
	s.Equal(int32(123), info1.WorkflowTaskAttempt)
	s.Equal(int64(321), info1.WorkflowTaskStartedTimestamp.UnixNano())
	s.Equal(int64(654), info1.WorkflowTaskScheduledTimestamp.UnixNano())
	s.Equal(int64(655), info1.WorkflowTaskOriginalScheduledTimestamp.UnixNano())
	s.Equal(updatedInfo.StickyTaskQueue, info1.StickyTaskQueue)
	s.EqualValues(timestamp.DurationValue(updatedInfo.StickyScheduleToStartTimeout), timestamp.DurationValue(info1.StickyScheduleToStartTimeout))
	s.Equal(updatedInfo.ClientLibraryVersion, info1.ClientLibraryVersion)
	s.Equal(updatedInfo.ClientFeatureVersion, info1.ClientFeatureVersion)
	s.Equal(updatedInfo.ClientImpl, info1.ClientImpl)
	s.Equal(updatedInfo.SignalCount, info1.SignalCount)
	s.EqualValues(updatedStats.HistorySize, state1.ExecutionStats.HistorySize)
	s.EqualValues(updatedInfo.InitialInterval, info1.InitialInterval)
	s.Equal(updatedInfo.BackoffCoefficient, info1.BackoffCoefficient)
	s.Equal(updatedInfo.MaximumInterval, info1.MaximumInterval)
	s.Equal(updatedInfo.MaximumAttempts, info1.MaximumAttempts)
	s.EqualTimes(*updatedInfo.WorkflowExpirationTime, *info1.WorkflowExpirationTime)
	s.Equal(updatedInfo.NonRetryableErrorTypes, info1.NonRetryableErrorTypes)
	searchAttrVal1, ok := info1.SearchAttributes[searchAttrKey]
	s.True(ok)
	s.Equal(searchAttrVal, searchAttrVal1)
	memoVal1, ok := info1.Memo[memoKey]
	s.True(ok)
	s.Equal(memoVal, memoVal1)
	s.assertChecksumsEqual(testWorkflowChecksum, state1.Checksum)

	log.Infof("Workflow execution last updated: %v", info1.LastUpdatedTimestamp)

	failedUpdateInfo := copyWorkflowExecutionInfo(updatedInfo)
	failedUpdateStats := copyExecutionStats(updatedStats)
	err4 := s.UpdateWorkflowExecution(failedUpdateInfo, failedUpdateStats, nil, []int64{int64(5)}, nil, int64(3), nil, nil, nil, nil, nil)
	s.Error(err4, "expected non nil error.")
	s.IsType(&p.ConditionFailedError{}, err4)
	log.Errorf("Conditional update failed with error: %v", err4)

	state2, err4 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err4)
	info2 := state2.ExecutionInfo
	s.NotNil(info2, "Valid Workflow info expected.")
	s.Equal(namespaceID, info2.NamespaceID)
	s.Equal("update-workflow-test", info2.WorkflowID)
	s.Equal("5ba5e531-e46b-48d9-b4b3-859919839553", info2.RunID)
	s.Equal("queue1", info2.TaskQueue)
	s.Equal("wType", info2.WorkflowTypeName)
	s.EqualValues(int64(20), info2.WorkflowRunTimeout.Seconds())
	s.EqualValues(13, int64(info2.DefaultWorkflowTaskTimeout.Seconds()))
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, info2.State)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info2.Status)
	s.Equal(int64(5), info2.NextEventID)
	s.Equal(int64(2), info2.LastProcessedEvent)
	s.True(s.validateTimeRange(*info2.LastUpdatedTimestamp, time.Hour))
	s.Equal(int64(666), info2.WorkflowTaskVersion)
	s.Equal(int64(2), info2.WorkflowTaskScheduleID)
	s.Equal(common.EmptyEventID, info2.WorkflowTaskStartedID)
	s.EqualValues(1, int64(info2.WorkflowTaskTimeout.Seconds()))
	s.Equal(int32(123), info2.WorkflowTaskAttempt)
	s.Equal(int64(321), info2.WorkflowTaskStartedTimestamp.UnixNano())
	s.Equal(int64(654), info2.WorkflowTaskScheduledTimestamp.UnixNano())
	s.Equal(int64(655), info2.WorkflowTaskOriginalScheduledTimestamp.UnixNano())
	s.Equal(updatedInfo.SignalCount, info2.SignalCount)
	s.EqualValues(updatedStats.HistorySize, state2.ExecutionStats.HistorySize)
	s.EqualValues(updatedInfo.InitialInterval, info2.InitialInterval)
	s.Equal(updatedInfo.BackoffCoefficient, info2.BackoffCoefficient)
	s.Equal(updatedInfo.MaximumInterval, info2.MaximumInterval)
	s.Equal(updatedInfo.MaximumAttempts, info2.MaximumAttempts)
	s.EqualTimes(*updatedInfo.WorkflowExpirationTime, *info2.WorkflowExpirationTime)
	s.Equal(updatedInfo.NonRetryableErrorTypes, info2.NonRetryableErrorTypes)
	searchAttrVal2, ok := info2.SearchAttributes[searchAttrKey]
	s.True(ok)
	s.Equal(searchAttrVal, searchAttrVal2)
	memoVal2, ok := info1.Memo[memoKey]
	s.True(ok)
	s.Equal(memoVal, memoVal2)
	s.assertChecksumsEqual(testWorkflowChecksum, state2.Checksum)
	log.Infof("Workflow execution last updated: %v", info2.LastUpdatedTimestamp)

	err5 := s.UpdateWorkflowExecutionWithRangeID(failedUpdateInfo, failedUpdateStats, nil, []int64{int64(5)}, nil, int64(12345), int64(5), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, "")
	s.Error(err5, "expected non nil error.")
	s.IsType(&p.ShardOwnershipLostError{}, err5)
	log.Errorf("Conditional update failed with error: %v", err5)

	state3, err6 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err6)
	info3 := state3.ExecutionInfo
	s.NotNil(info3, "Valid Workflow info expected.")
	s.Equal(namespaceID, info3.NamespaceID)
	s.Equal("update-workflow-test", info3.WorkflowID)
	s.Equal("5ba5e531-e46b-48d9-b4b3-859919839553", info3.RunID)
	s.Equal("queue1", info3.TaskQueue)
	s.Equal("wType", info3.WorkflowTypeName)
	s.EqualValues(int64(20), info3.WorkflowRunTimeout.Seconds())
	s.EqualValues(13, int64(info3.DefaultWorkflowTaskTimeout.Seconds()))
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, info3.State)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info3.Status)
	s.Equal(int64(5), info3.NextEventID)
	s.Equal(int64(2), info3.LastProcessedEvent)
	s.True(s.validateTimeRange(*info3.LastUpdatedTimestamp, time.Hour))
	s.Equal(int64(666), info3.WorkflowTaskVersion)
	s.Equal(int64(2), info3.WorkflowTaskScheduleID)
	s.Equal(common.EmptyEventID, info3.WorkflowTaskStartedID)
	s.EqualValues(1, int64(info3.WorkflowTaskTimeout.Seconds()))
	s.Equal(int32(123), info3.WorkflowTaskAttempt)
	s.Equal(int64(321), info3.WorkflowTaskStartedTimestamp.UnixNano())
	s.Equal(int64(654), info3.WorkflowTaskScheduledTimestamp.UnixNano())
	s.Equal(int64(655), info3.WorkflowTaskOriginalScheduledTimestamp.UnixNano())
	s.Equal(updatedInfo.SignalCount, info3.SignalCount)
	s.EqualValues(updatedStats.HistorySize, state3.ExecutionStats.HistorySize)
	s.EqualValues(updatedInfo.InitialInterval, info3.InitialInterval)
	s.Equal(updatedInfo.BackoffCoefficient, info3.BackoffCoefficient)
	s.Equal(updatedInfo.MaximumInterval, info3.MaximumInterval)
	s.Equal(updatedInfo.MaximumAttempts, info3.MaximumAttempts)
	s.EqualTimes(*updatedInfo.WorkflowExpirationTime, *info3.WorkflowExpirationTime)
	s.Equal(updatedInfo.NonRetryableErrorTypes, info3.NonRetryableErrorTypes)
	searchAttrVal3, ok := info3.SearchAttributes[searchAttrKey]
	s.True(ok)
	s.Equal(searchAttrVal, searchAttrVal3)
	memoVal3, ok := info1.Memo[memoKey]
	s.True(ok)
	s.Equal(memoVal, memoVal3)
	s.assertChecksumsEqual(testWorkflowChecksum, state3.Checksum)

	log.Infof("Workflow execution last updated: %v", info3.LastUpdatedTimestamp)

	// update with incorrect rangeID and condition(next_event_id)
	err7 := s.UpdateWorkflowExecutionWithRangeID(failedUpdateInfo, failedUpdateStats, nil, []int64{int64(5)}, nil, int64(12345), int64(3), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, "")
	s.Error(err7, "expected non nil error.")
	s.IsType(&p.ShardOwnershipLostError{}, err7)
	log.Errorf("Conditional update failed with error: %v", err7)

	state4, err8 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err8)
	info4 := state4.ExecutionInfo
	s.NotNil(info4, "Valid Workflow info expected.")
	s.Equal(namespaceID, info4.NamespaceID)
	s.Equal("update-workflow-test", info4.WorkflowID)
	s.Equal("5ba5e531-e46b-48d9-b4b3-859919839553", info4.RunID)
	s.Equal("queue1", info4.TaskQueue)
	s.Equal("wType", info4.WorkflowTypeName)
	s.EqualValues(int64(20), info4.WorkflowRunTimeout.Seconds())
	s.EqualValues(int64(13), info4.DefaultWorkflowTaskTimeout.Seconds())
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, info4.State)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info4.Status)
	s.Equal(int64(5), info4.NextEventID)
	s.Equal(int64(2), info4.LastProcessedEvent)
	s.True(s.validateTimeRange(*info4.LastUpdatedTimestamp, time.Hour))
	s.Equal(int64(666), info4.WorkflowTaskVersion)
	s.Equal(int64(2), info4.WorkflowTaskScheduleID)
	s.Equal(common.EmptyEventID, info4.WorkflowTaskStartedID)
	s.EqualValues(1, int64(info4.WorkflowTaskTimeout.Seconds()))
	s.Equal(int32(123), info4.WorkflowTaskAttempt)
	s.Equal(int64(321), info4.WorkflowTaskStartedTimestamp.UnixNano())
	s.Equal(updatedInfo.SignalCount, info4.SignalCount)
	s.EqualValues(updatedStats.HistorySize, state4.ExecutionStats.HistorySize)
	s.EqualValues(updatedInfo.InitialInterval, info4.InitialInterval)
	s.Equal(updatedInfo.BackoffCoefficient, info4.BackoffCoefficient)
	s.Equal(updatedInfo.MaximumInterval, info4.MaximumInterval)
	s.Equal(updatedInfo.MaximumAttempts, info4.MaximumAttempts)
	s.EqualTimes(*updatedInfo.WorkflowExpirationTime, *info4.WorkflowExpirationTime)
	s.Equal(updatedInfo.NonRetryableErrorTypes, info4.NonRetryableErrorTypes)
	searchAttrVal4, ok := info4.SearchAttributes[searchAttrKey]
	s.True(ok)
	s.Equal(searchAttrVal, searchAttrVal4)
	memoVal4, ok := info1.Memo[memoKey]
	s.True(ok)
	s.Equal(memoVal, memoVal4)
	s.assertChecksumsEqual(testWorkflowChecksum, state4.Checksum)

	log.Infof("Workflow execution last updated: %v", info4.LastUpdatedTimestamp)
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

	state0, err1 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")
	s.Equal(namespaceID, info0.NamespaceID)
	s.Equal("delete-workflow-test", info0.WorkflowID)
	s.Equal("4e0917f2-9361-4a14-b16f-1fafe09b287a", info0.RunID)
	s.Equal("queue1", info0.TaskQueue)
	s.Equal("wType", info0.WorkflowTypeName)
	s.EqualValues(int64(20), info0.WorkflowRunTimeout.Seconds())
	s.EqualValues(13, int64(info0.DefaultWorkflowTaskTimeout.Seconds()))
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, info0.State)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info0.Status)
	s.Equal(int64(3), info0.NextEventID)
	s.Equal(int64(0), info0.LastProcessedEvent)
	s.True(s.validateTimeRange(*info0.LastUpdatedTimestamp, time.Hour))
	s.Equal(int64(2), info0.WorkflowTaskScheduleID)
	s.Equal(common.EmptyEventID, info0.WorkflowTaskStartedID)
	s.EqualValues(1, int64(info0.WorkflowTaskTimeout.Seconds()))

	log.Infof("Workflow execution last updated: %v", info0.LastUpdatedTimestamp)

	err4 := s.DeleteWorkflowExecution(info0)
	s.NoError(err4)

	_, err3 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.Error(err3, "expected non nil error.")
	s.IsType(&serviceerror.NotFound{}, err3)

	err5 := s.DeleteWorkflowExecution(info0)
	s.NoError(err5)
}

// TestDeleteCurrentWorkflow test
func (s *ExecutionManagerSuite) TestDeleteCurrentWorkflow() {
	if s.ExecutionManager.GetName() != "cassandra" {
		s.T().Skip("this test is only applicable for cassandra (uses TTL based deletes)")
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

	info0, err2 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err2)

	updatedInfo1 := copyWorkflowExecutionInfo(info0.ExecutionInfo)
	updatedStats1 := copyExecutionStats(info0.ExecutionStats)
	updatedInfo1.NextEventID = int64(6)
	updatedInfo1.LastProcessedEvent = int64(2)
	err3 := s.UpdateWorkflowExecutionAndFinish(updatedInfo1, updatedStats1, int64(3))
	s.NoError(err3)

	runID4, err4 := s.GetCurrentWorkflowRunID(namespaceID, workflowExecution.GetWorkflowId())
	s.NoError(err4)
	s.Equal(workflowExecution.GetRunId(), runID4)

	fakeInfo := &p.WorkflowExecutionInfo{
		NamespaceID: info0.ExecutionInfo.NamespaceID,
		WorkflowID:  info0.ExecutionInfo.WorkflowID,
		RunID:       uuid.New(),
	}

	// test wrong run id with conditional delete
	s.NoError(s.DeleteCurrentWorkflowExecution(fakeInfo))

	runID5, err5 := s.GetCurrentWorkflowRunID(namespaceID, workflowExecution.GetWorkflowId())
	s.NoError(err5)
	s.Equal(workflowExecution.GetRunId(), runID5)

	// simulate a timer_task deleting execution after retention
	s.NoError(s.DeleteCurrentWorkflowExecution(info0.ExecutionInfo))

	runID0, err1 = s.GetCurrentWorkflowRunID(namespaceID, workflowExecution.GetWorkflowId())
	s.Error(err1)
	s.Empty(runID0)
	_, ok := err1.(*serviceerror.NotFound)
	s.True(ok)

	// execution record should still be there
	_, err2 = s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
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

	info0, err2 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err2)

	updatedInfo1 := copyWorkflowExecutionInfo(info0.ExecutionInfo)
	updatedStats1 := copyExecutionStats(info0.ExecutionStats)
	updatedInfo1.NextEventID = int64(6)
	updatedInfo1.LastProcessedEvent = int64(2)
	err3 := s.UpdateWorkflowExecutionAndFinish(updatedInfo1, updatedStats1, int64(3))
	s.NoError(err3)

	runID4, err4 := s.GetCurrentWorkflowRunID(namespaceID, workflowExecution.GetWorkflowId())
	s.NoError(err4)
	s.Equal(workflowExecution.GetRunId(), runID4)

	// simulate a timer_task deleting execution after retention
	err5 := s.DeleteCurrentWorkflowExecution(info0.ExecutionInfo)
	s.NoError(err5)
	err6 := s.DeleteWorkflowExecution(info0.ExecutionInfo)
	s.NoError(err6)

	time.Sleep(time.Duration(finishedCurrentExecutionRetentionTTL*2) * time.Second)

	runID0, err1 = s.GetCurrentWorkflowRunID(namespaceID, workflowExecution.GetWorkflowId())
	s.Error(err1)
	s.Empty(runID0)
	_, ok := err1.(*serviceerror.NotFound)
	// execution record should still be there
	_, err2 = s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
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

	info0, err2 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err2)

	// deleting current record and verify
	err3 := s.DeleteCurrentWorkflowExecution(info0.ExecutionInfo)
	s.NoError(err3)
	runID0, err4 := s.GetCurrentWorkflowRunID(namespaceID, workflowExecution.GetWorkflowId())
	s.Error(err4)
	s.Empty(runID0)
	_, ok := err4.(*serviceerror.NotFound)
	s.True(ok)

	// we should still be able to load with runID
	info1, err5 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err5)
	s.Equal(info0, info1)

	// mark it as corrupted
	info0.ExecutionInfo.State = enumsspb.WORKFLOW_EXECUTION_STATE_CORRUPTED
	_, err6 := s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		UpdateWorkflowMutation: p.WorkflowMutation{
			ExecutionInfo:  info0.ExecutionInfo,
			ExecutionStats: info0.ExecutionStats,
			Condition:      info0.ExecutionInfo.NextEventID,
			Checksum:       testWorkflowChecksum,
		},
		RangeID: s.ShardInfo.GetRangeId(),
		Mode:    p.UpdateWorkflowModeBypassCurrent,
	})
	s.NoError(err6)

	// we should still be able to load with runID
	info2, err7 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err7)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_CORRUPTED, info2.ExecutionInfo.State)
	info2.ExecutionInfo.State = info1.ExecutionInfo.State
	info2.ExecutionInfo.LastUpdatedTimestamp = info1.ExecutionInfo.LastUpdatedTimestamp
	s.Equal(info2, info1)

	// delete the run
	err8 := s.DeleteWorkflowExecution(info0.ExecutionInfo)
	s.NoError(err8)

	// execution record should be gone
	_, err9 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
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
		NamespaceID: namespaceID,
		WorkflowID:  workflowExecution.GetWorkflowId(),
	})
	s.NoError(err)
	s.Equal(workflowExecution.GetRunId(), response.RunID)
	s.Equal(common.EmptyVersion, response.LastWriteVersion)

	info0, err2 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err2)

	updatedInfo1 := copyWorkflowExecutionInfo(info0.ExecutionInfo)
	updatedStats1 := copyExecutionStats(info0.ExecutionStats)
	updatedInfo1.NextEventID = int64(6)
	updatedInfo1.LastProcessedEvent = int64(2)
	err3 := s.UpdateWorkflowExecutionAndFinish(updatedInfo1, updatedStats1, int64(3))
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

	task0, err0 := s.CreateWorkflowExecution(namespaceID, workflowExecution, "queue1", "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	tasks1, err1 := s.GetTransferTasks(1, false)
	s.NoError(err1)
	s.NotNil(tasks1, "expected valid list of tasks.")
	s.Equal(1, len(tasks1), "Expected 1 workflow task.")
	task1 := tasks1[0]
	s.validateTransferTaskHighLevel(task1, enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK, namespaceID, workflowExecution)
	s.Equal("queue1", task1.TaskQueue)
	s.Equal(int64(2), task1.GetScheduleId())
	s.EqualValues("", task1.GetTargetRunId())

	err3 := s.CompleteTransferTask(task1.GetTaskId())
	s.NoError(err3)

	state0, err11 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err11)
	info0 := state0.ExecutionInfo
	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedStats0 := copyExecutionStats(state0.ExecutionStats)
	updatedInfo.NextEventID = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)
	err2 := s.UpdateWorkflowExecution(updatedInfo, updatedStats0, nil, nil, []int64{int64(4)}, int64(3), nil, nil, nil, nil, nil)
	s.NoError(err2)

	tasks2, err1 := s.GetTransferTasks(1, false)
	s.NoError(err1)
	s.NotNil(tasks2, "expected valid list of tasks.")
	s.Equal(1, len(tasks2), "Expected 1 workflow task.")
	task2 := tasks2[0]
	s.validateTransferTaskHighLevel(task2, enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK, namespaceID, workflowExecution)
	s.Equal("queue1", task2.TaskQueue)
	s.Equal(int64(4), task2.GetScheduleId())
	s.EqualValues("", task2.GetTargetRunId())

	err4 := s.CompleteTransferTask(task2.GetTaskId())
	s.NoError(err4)

	state1, _ := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	info1 := state1.ExecutionInfo
	updatedInfo1 := copyWorkflowExecutionInfo(info1)
	updatedStats1 := copyExecutionStats(state1.ExecutionStats)
	updatedInfo1.NextEventID = int64(6)
	updatedInfo1.LastProcessedEvent = int64(2)
	err5 := s.UpdateWorkflowExecutionAndFinish(updatedInfo1, updatedStats1, int64(5))
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
	s.validateTransferTaskHighLevel(task3, enumsspb.TASK_TYPE_TRANSFER_CLOSE_EXECUTION, namespaceID, workflowExecution)
	s.EqualValues("", task3.GetTargetRunId())

	err8 := s.CompleteTransferTask(task3.GetTaskId())
	s.NoError(err8)

	_, err9 := s.CreateWorkflowExecution(namespaceID, newExecution, "queue1", "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, nil)
	s.Error(err9, "createWFExecution (brand_new) must fail when there is a previous instance of workflow state already in DB")

	err10 := s.DeleteWorkflowExecution(info1)
	s.NoError(err10)
}

// TestCancelTransferTaskTasks test
func (s *ExecutionManagerSuite) TestCancelTransferTaskTasks() {
	namespaceID := "aeac8287-527b-4b35-80a9-667cb47e7c6d"
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "cancel-workflow-test",
		RunId:      "db20f7e2-1a1e-40d9-9278-d8b886738e05",
	}

	task0, err := s.CreateWorkflowExecution(namespaceID, workflowExecution, "queue1", "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, nil)
	s.NoError(err)
	s.NotNil(task0, "Expected non empty task identifier.")

	taskD, err := s.GetTransferTasks(1, false)
	s.Equal(1, len(taskD), "Expected 1 workflow task.")
	err = s.CompleteTransferTask(taskD[0].GetTaskId())
	s.NoError(err)

	deleteCheck, err := s.GetTransferTasks(1, false)
	s.Equal(0, len(deleteCheck), "Expected no workflow task.")

	state1, err := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err)
	info1 := state1.ExecutionInfo
	s.NotNil(info1, "Valid Workflow info expected.")
	updatedInfo1 := copyWorkflowExecutionInfo(info1)
	updatedStats1 := copyExecutionStats(state1.ExecutionStats)

	targetNamespaceID := "f2bfaab6-7e8b-4fac-9a62-17da8d37becb"
	targetWorkflowID := "target-workflow-cancellation-id-1"
	targetRunID := "0d00698f-08e1-4d36-a3e2-3bf109f5d2d6"
	targetChildWorkflowOnly := false
	transferTasks := []p.Task{&p.CancelExecutionTask{
		TaskID:                  s.GetNextSequenceNumber(),
		TargetNamespaceID:       targetNamespaceID,
		TargetWorkflowID:        targetWorkflowID,
		TargetRunID:             targetRunID,
		TargetChildWorkflowOnly: targetChildWorkflowOnly,
		InitiatedID:             1,
	}}
	err = s.UpdateWorkflowExecutionWithTransferTasks(updatedInfo1, updatedStats1, int64(3), transferTasks, nil)
	s.NoError(err)

	tasks1, err := s.GetTransferTasks(1, false)
	s.NoError(err)
	s.NotNil(tasks1, "expected valid list of tasks.")
	s.Equal(1, len(tasks1), "Expected 1 cancel task.")
	task1 := tasks1[0]
	s.validateTransferTaskHighLevel(task1, enumsspb.TASK_TYPE_TRANSFER_CANCEL_EXECUTION, namespaceID, workflowExecution)
	s.validateTransferTaskTargetInfo(task1, targetNamespaceID, targetWorkflowID, targetRunID)
	s.Equal(targetChildWorkflowOnly, task1.TargetChildWorkflowOnly)

	err = s.CompleteTransferTask(task1.GetTaskId())
	s.NoError(err)

	targetNamespaceID = "f2bfaab6-7e8b-4fac-9a62-17da8d37becb"
	targetWorkflowID = "target-workflow-cancellation-id-2"
	targetRunID = ""
	targetChildWorkflowOnly = true
	transferTasks = []p.Task{&p.CancelExecutionTask{
		TaskID:                  s.GetNextSequenceNumber(),
		TargetNamespaceID:       targetNamespaceID,
		TargetWorkflowID:        targetWorkflowID,
		TargetRunID:             targetRunID,
		TargetChildWorkflowOnly: targetChildWorkflowOnly,
		InitiatedID:             3,
	}}

	state2, err := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err)
	info2 := state2.ExecutionInfo
	s.NotNil(info2, "Valid Workflow info expected.")
	updatedInfo2 := copyWorkflowExecutionInfo(info2)
	updatedStats2 := copyExecutionStats(state2.ExecutionStats)

	err = s.UpdateWorkflowExecutionWithTransferTasks(updatedInfo2, updatedStats2, int64(3), transferTasks, nil)
	s.NoError(err)

	tasks2, err := s.GetTransferTasks(1, false)
	s.NoError(err)
	s.NotNil(tasks2, "expected valid list of tasks.")
	s.Equal(1, len(tasks2), "Expected 1 cancel task.")
	task2 := tasks2[0]
	s.validateTransferTaskHighLevel(task2, enumsspb.TASK_TYPE_TRANSFER_CANCEL_EXECUTION, namespaceID, workflowExecution)
	s.validateTransferTaskTargetInfo(task2, targetNamespaceID, targetWorkflowID, targetRunID)
	s.Equal(targetChildWorkflowOnly, task2.TargetChildWorkflowOnly)

	err = s.CompleteTransferTask(task2.GetTaskId())
	s.NoError(err)
}

func (s *ExecutionManagerSuite) validateTransferTaskHighLevel(task1 *persistenceblobs.TransferTaskInfo, taskType enumsspb.TaskType, namespaceID string, workflowExecution commonpb.WorkflowExecution) {
	s.EqualValues(taskType, task1.TaskType)
	s.Equal(namespaceID, task1.GetNamespaceId())
	s.Equal(workflowExecution.GetWorkflowId(), task1.GetWorkflowId())
	s.Equal(workflowExecution.GetRunId(), task1.GetRunId())
}

func (s *ExecutionManagerSuite) validateTransferTaskTargetInfo(task2 *persistenceblobs.TransferTaskInfo, targetNamespaceID string, targetWorkflowID string, targetRunID string) {
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

	task0, err := s.CreateWorkflowExecution(namespaceID, workflowExecution, "queue1", "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, nil)
	s.NoError(err)
	s.NotNil(task0, "Expected non empty task identifier.")

	taskD, err := s.GetTransferTasks(1, false)
	s.Equal(1, len(taskD), "Expected 1 workflow task.")
	err = s.CompleteTransferTask(taskD[0].GetTaskId())
	s.NoError(err)

	state1, err := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err)
	info1 := state1.ExecutionInfo
	s.NotNil(info1, "Valid Workflow info expected.")
	updatedInfo1 := copyWorkflowExecutionInfo(info1)
	updatedStats1 := copyExecutionStats(state1.ExecutionStats)

	targetNamespaceID := "f2bfaab6-7e8b-4fac-9a62-17da8d37becb"
	targetWorkflowID := "target-workflow-signal-id-1"
	targetRunID := "0d00698f-08e1-4d36-a3e2-3bf109f5d2d6"
	targetChildWorkflowOnly := false
	transferTasks := []p.Task{&p.SignalExecutionTask{
		TaskID:                  s.GetNextSequenceNumber(),
		TargetNamespaceID:       targetNamespaceID,
		TargetWorkflowID:        targetWorkflowID,
		TargetRunID:             targetRunID,
		TargetChildWorkflowOnly: targetChildWorkflowOnly,
		InitiatedID:             1,
	}}
	err = s.UpdateWorkflowExecutionWithTransferTasks(updatedInfo1, updatedStats1, int64(3), transferTasks, nil)
	s.NoError(err)

	tasks1, err := s.GetTransferTasks(1, false)
	s.NoError(err)
	s.NotNil(tasks1, "expected valid list of tasks.")
	s.Equal(1, len(tasks1), "Expected 1 cancel task.")
	task1 := tasks1[0]
	s.validateTransferTaskHighLevel(task1, enumsspb.TASK_TYPE_TRANSFER_SIGNAL_EXECUTION, namespaceID, workflowExecution)
	s.validateTransferTaskTargetInfo(task1, targetNamespaceID, targetWorkflowID, targetRunID)
	s.Equal(targetChildWorkflowOnly, task1.TargetChildWorkflowOnly)

	err = s.CompleteTransferTask(task1.GetTaskId())
	s.NoError(err)

	targetNamespaceID = "f2bfaab6-7e8b-4fac-9a62-17da8d37becb"
	targetWorkflowID = "target-workflow-signal-id-2"
	targetRunID = ""
	targetChildWorkflowOnly = true
	transferTasks = []p.Task{&p.SignalExecutionTask{
		TaskID:                  s.GetNextSequenceNumber(),
		TargetNamespaceID:       targetNamespaceID,
		TargetWorkflowID:        targetWorkflowID,
		TargetRunID:             targetRunID,
		TargetChildWorkflowOnly: targetChildWorkflowOnly,
		InitiatedID:             3,
	}}

	state2, err := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err)
	info2 := state2.ExecutionInfo
	s.NotNil(info2, "Valid Workflow info expected.")
	updatedInfo2 := copyWorkflowExecutionInfo(info2)
	updatedStats2 := copyExecutionStats(state2.ExecutionStats)

	err = s.UpdateWorkflowExecutionWithTransferTasks(updatedInfo2, updatedStats2, int64(3), transferTasks, nil)
	s.NoError(err)

	tasks2, err := s.GetTransferTasks(1, false)
	s.NoError(err)
	s.NotNil(tasks2, "expected valid list of tasks.")
	s.Equal(1, len(tasks2), "Expected 1 cancel task.")
	task2 := tasks2[0]
	s.validateTransferTaskHighLevel(task2, enumsspb.TASK_TYPE_TRANSFER_SIGNAL_EXECUTION, namespaceID, workflowExecution)
	s.validateTransferTaskTargetInfo(task2, targetNamespaceID, targetWorkflowID, targetRunID)
	s.Equal(targetChildWorkflowOnly, task2.TargetChildWorkflowOnly)

	err = s.CompleteTransferTask(task2.GetTaskId())
	s.NoError(err)
}

// TestReplicationTasks test
func (s *ExecutionManagerSuite) TestReplicationTasks() {
	namespaceID := "2466d7de-6602-4ad8-b939-fb8f8c36c711"
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "get-replication-tasks-test",
		RunId:      "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
	}

	task0, err := s.CreateWorkflowExecution(namespaceID, workflowExecution, "queue1", "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, nil)
	s.NoError(err)
	s.NotNil(task0, "Expected non empty task identifier.")
	taskD, err := s.GetTransferTasks(1, false)
	s.Equal(1, len(taskD), "Expected 1 workflow task.")
	err = s.CompleteTransferTask(taskD[0].GetTaskId())
	s.NoError(err)

	state1, err := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err)
	info1 := state1.ExecutionInfo
	s.NotNil(info1, "Valid Workflow info expected.")
	updatedInfo1 := copyWorkflowExecutionInfo(info1)
	updatedStats1 := copyExecutionStats(state1.ExecutionStats)

	replicationTasks := []p.Task{
		&p.HistoryReplicationTask{
			TaskID:       s.GetNextSequenceNumber(),
			FirstEventID: int64(1),
			NextEventID:  int64(3),
			Version:      123,
		},
		&p.HistoryReplicationTask{
			TaskID:       s.GetNextSequenceNumber(),
			FirstEventID: int64(1),
			NextEventID:  int64(3),
			Version:      456,
		},
		&p.SyncActivityTask{
			TaskID:      s.GetNextSequenceNumber(),
			Version:     789,
			ScheduledID: 99,
		},
	}
	err = s.UpdateWorklowStateAndReplication(updatedInfo1, updatedStats1, nil, int64(3), replicationTasks)
	s.NoError(err)

	respTasks, err := s.GetReplicationTasks(1, true)
	s.NoError(err)
	s.Equal(len(replicationTasks), len(respTasks))

	for index := range replicationTasks {
		s.Equal(replicationTasks[index].GetTaskID(), respTasks[index].GetTaskId())
		s.Equal(replicationTasks[index].GetType(), respTasks[index].GetTaskType())
		s.Equal(replicationTasks[index].GetVersion(), respTasks[index].GetVersion())
		switch replicationTasks[index].GetType() {
		case enumsspb.TASK_TYPE_REPLICATION_HISTORY:
			expected := replicationTasks[index].(*p.HistoryReplicationTask)
			s.Equal(expected.FirstEventID, respTasks[index].GetFirstEventId())
			s.Equal(expected.NextEventID, respTasks[index].GetNextEventId())
			s.Equal(expected.BranchToken, respTasks[index].BranchToken)
			s.Equal(expected.NewRunBranchToken, respTasks[index].NewRunBranchToken)

		case enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY:
			expected := replicationTasks[index].(*p.SyncActivityTask)
			s.Equal(expected.ScheduledID, respTasks[index].GetScheduledId())
		}
		err = s.CompleteReplicationTask(respTasks[index].GetTaskId())
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
	taskqueue := "some random taskqueue"

	task0, err0 := s.CreateWorkflowExecution(namespaceID, workflowExecution, taskqueue, "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	tasks1, err1 := s.GetTransferTasks(1, false)
	s.NoError(err1)
	s.NotNil(tasks1, "expected valid list of tasks.")
	s.Equal(1, len(tasks1), "Expected 1 workflow task.")
	task1 := tasks1[0]
	taskType := enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK
	scheduleId := int64(2)
	targetWorkflowId := p.TransferTaskTransferTargetWorkflowID
	targetRunId := ""
	s.validateTransferTaskHighLevel(task1, taskType, namespaceID, workflowExecution)
	s.validateTransferTaskTargetInfo(task1, task1.GetTargetNamespaceId(), targetWorkflowId, targetRunId)
	s.Equal(taskqueue, task1.TaskQueue)
	s.Equal(scheduleId, task1.GetScheduleId())
	err3 := s.CompleteTransferTask(task1.GetTaskId())
	s.NoError(err3)

	state0, err1 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedStats := copyExecutionStats(state0.ExecutionStats)
	updatedInfo.NextEventID = int64(6)
	updatedInfo.LastProcessedEvent = int64(2)
	scheduleID := int64(123)
	targetNamespaceID := "8bfb47be-5b57-4d66-9109-5fb35e20b1d0"
	targetWorkflowID := "some random target namespace ID"
	targetRunID := uuid.New()
	currentTransferID := s.GetTransferReadLevel()
	now := time.Now().UTC()
	tasks := []p.Task{
		&p.ActivityTask{now, currentTransferID + 10001, namespaceID, taskqueue, scheduleID, 111},
		&p.WorkflowTask{now, currentTransferID + 10002, namespaceID, taskqueue, scheduleID, 222, false},
		&p.CloseExecutionTask{now, currentTransferID + 10003, 333},
		&p.CancelExecutionTask{now, currentTransferID + 10004, targetNamespaceID, targetWorkflowID, targetRunID, true, scheduleID, 444},
		&p.SignalExecutionTask{now, currentTransferID + 10005, targetNamespaceID, targetWorkflowID, targetRunID, true, scheduleID, 555},
		&p.StartChildExecutionTask{now, currentTransferID + 10006, targetNamespaceID, targetWorkflowID, scheduleID, 666},
	}
	err2 := s.UpdateWorklowStateAndReplication(updatedInfo, updatedStats, nil, int64(3), tasks)
	s.NoError(err2)

	txTasks, err1 := s.GetTransferTasks(1, true) // use page size one to force pagination
	s.NoError(err1)
	s.NotNil(txTasks, "expected valid list of tasks.")
	s.Equal(len(tasks), len(txTasks))
	for index := range tasks {
		t := txTasks[index].VisibilityTime
		s.True(timeComparatorGo(tasks[index].GetVisibilityTimestamp(), *t, TimePrecision))
	}
	s.EqualValues(enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK, txTasks[0].TaskType)
	s.EqualValues(enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK, txTasks[1].TaskType)
	s.EqualValues(enumsspb.TASK_TYPE_TRANSFER_CLOSE_EXECUTION, txTasks[2].TaskType)
	s.EqualValues(enumsspb.TASK_TYPE_TRANSFER_CANCEL_EXECUTION, txTasks[3].TaskType)
	s.EqualValues(enumsspb.TASK_TYPE_TRANSFER_SIGNAL_EXECUTION, txTasks[4].TaskType)
	s.EqualValues(enumsspb.TASK_TYPE_TRANSFER_START_CHILD_EXECUTION, txTasks[5].TaskType)
	s.Equal(int64(111), txTasks[0].Version)
	s.Equal(int64(222), txTasks[1].Version)
	s.Equal(int64(333), txTasks[2].Version)
	s.Equal(int64(444), txTasks[3].Version)
	s.Equal(int64(555), txTasks[4].Version)
	s.Equal(int64(666), txTasks[5].Version)

	err2 = s.CompleteTransferTask(txTasks[0].GetTaskId())
	s.NoError(err2)

	err2 = s.CompleteTransferTask(txTasks[1].GetTaskId())
	s.NoError(err2)

	err2 = s.CompleteTransferTask(txTasks[2].GetTaskId())
	s.NoError(err2)

	err2 = s.CompleteTransferTask(txTasks[3].GetTaskId())
	s.NoError(err2)

	err2 = s.CompleteTransferTask(txTasks[4].GetTaskId())
	s.NoError(err2)

	err2 = s.CompleteTransferTask(txTasks[5].GetTaskId())
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
	taskqueue := "some random taskqueue"

	task0, err0 := s.CreateWorkflowExecution(namespaceID, workflowExecution, taskqueue, "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	tasks1, err1 := s.GetTransferTasks(1, false)
	s.NoError(err1)
	s.NotNil(tasks1, "expected valid list of tasks.")
	s.Equal(1, len(tasks1), "Expected 1 workflow task.")
	task1 := tasks1[0]
	s.validateTransferTaskHighLevel(task1, enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK, namespaceID, workflowExecution)
	s.validateTransferTaskTargetInfo(task1, task1.GetNamespaceId(), p.TransferTaskTransferTargetWorkflowID, "")
	s.Equal(taskqueue, task1.TaskQueue)
	s.Equal(int64(2), task1.GetScheduleId())

	err3 := s.CompleteTransferTask(task1.GetTaskId())
	s.NoError(err3)

	state0, err1 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedStats := copyExecutionStats(state0.ExecutionStats)
	updatedInfo.NextEventID = int64(6)
	updatedInfo.LastProcessedEvent = int64(2)
	scheduleID := int64(123)
	targetNamespaceID := "8bfb47be-5b57-4d66-9109-5fb35e20b1d0"
	targetWorkflowID := "some random target namespace ID"
	targetRunID := uuid.New()
	currentTransferID := s.GetTransferReadLevel()
	now := time.Now().UTC()
	tasks := []p.Task{
		&p.ActivityTask{now, currentTransferID + 10001, namespaceID, taskqueue, scheduleID, 111},
		&p.WorkflowTask{now, currentTransferID + 10002, namespaceID, taskqueue, scheduleID, 222, false},
		&p.CloseExecutionTask{now, currentTransferID + 10003, 333},
		&p.CancelExecutionTask{now, currentTransferID + 10004, targetNamespaceID, targetWorkflowID, targetRunID, true, scheduleID, 444},
		&p.SignalExecutionTask{now, currentTransferID + 10005, targetNamespaceID, targetWorkflowID, targetRunID, true, scheduleID, 555},
		&p.StartChildExecutionTask{now, currentTransferID + 10006, targetNamespaceID, targetWorkflowID, scheduleID, 666},
	}
	err2 := s.UpdateWorklowStateAndReplication(updatedInfo, updatedStats, nil, int64(3), tasks)
	s.NoError(err2)

	txTasks, err1 := s.GetTransferTasks(2, true) // use page size one to force pagination
	s.NoError(err1)
	s.NotNil(txTasks, "expected valid list of tasks.")
	s.Equal(len(tasks), len(txTasks))
	for index := range tasks {
		t := txTasks[index].VisibilityTime
		s.True(timeComparatorGo(tasks[index].GetVisibilityTimestamp(), *t, TimePrecision))
	}
	s.EqualValues(enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK, txTasks[0].TaskType)
	s.EqualValues(enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK, txTasks[1].TaskType)
	s.EqualValues(enumsspb.TASK_TYPE_TRANSFER_CLOSE_EXECUTION, txTasks[2].TaskType)
	s.EqualValues(enumsspb.TASK_TYPE_TRANSFER_CANCEL_EXECUTION, txTasks[3].TaskType)
	s.EqualValues(enumsspb.TASK_TYPE_TRANSFER_SIGNAL_EXECUTION, txTasks[4].TaskType)
	s.EqualValues(enumsspb.TASK_TYPE_TRANSFER_START_CHILD_EXECUTION, txTasks[5].TaskType)
	s.Equal(int64(111), txTasks[0].Version)
	s.Equal(int64(222), txTasks[1].Version)
	s.Equal(int64(333), txTasks[2].Version)
	s.Equal(int64(444), txTasks[3].Version)
	s.Equal(int64(555), txTasks[4].Version)
	s.Equal(int64(666), txTasks[5].Version)
	s.Equal(currentTransferID+10001, txTasks[0].GetTaskId())
	s.Equal(currentTransferID+10002, txTasks[1].GetTaskId())
	s.Equal(currentTransferID+10003, txTasks[2].GetTaskId())
	s.Equal(currentTransferID+10004, txTasks[3].GetTaskId())
	s.Equal(currentTransferID+10005, txTasks[4].GetTaskId())
	s.Equal(currentTransferID+10006, txTasks[5].GetTaskId())

	err2 = s.RangeCompleteTransferTask(txTasks[0].GetTaskId()-1, txTasks[5].GetTaskId())
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

	now := time.Now().UTC()
	initialTasks := []p.Task{&p.WorkflowTaskTimeoutTask{now.Add(1 * time.Second), 1, 2, 3, enumspb.TIMEOUT_TYPE_START_TO_CLOSE, 11}}

	task0, err0 := s.CreateWorkflowExecution(namespaceID, workflowExecution, "taskQueue", "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, initialTasks)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedStats := copyExecutionStats(state0.ExecutionStats)
	updatedInfo.NextEventID = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)
	tasks := []p.Task{
		&p.WorkflowTimeoutTask{now.Add(2 * time.Second), 2, 12},
		&p.DeleteHistoryEventTask{now.Add(2 * time.Second), 3, 13},
		&p.ActivityTimeoutTask{now.Add(3 * time.Second), 4, enumspb.TIMEOUT_TYPE_START_TO_CLOSE, 7, 0, 14},
		&p.UserTimerTask{now.Add(3 * time.Second), 5, 7, 15},
	}
	err2 := s.UpdateWorkflowExecution(updatedInfo, updatedStats, nil, []int64{int64(4)}, nil, int64(3), tasks, nil, nil, nil, nil)
	s.NoError(err2)

	timerTasks, err1 := s.GetTimerIndexTasks(1, true) // use page size one to force pagination
	s.NoError(err1)
	s.NotNil(timerTasks, "expected valid list of tasks.")
	s.Equal(len(tasks)+len(initialTasks), len(timerTasks))
	s.EqualValues(enumsspb.TASK_TYPE_WORKFLOW_TASK_TIMEOUT, timerTasks[0].TaskType)
	s.EqualValues(enumsspb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT, timerTasks[1].TaskType)
	s.EqualValues(enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT, timerTasks[2].TaskType)
	s.EqualValues(enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT, timerTasks[3].TaskType)
	s.EqualValues(enumsspb.TASK_TYPE_USER_TIMER, timerTasks[4].TaskType)
	s.Equal(int64(11), timerTasks[0].Version)
	s.Equal(int64(12), timerTasks[1].Version)
	s.Equal(int64(13), timerTasks[2].Version)
	s.Equal(int64(14), timerTasks[3].Version)
	s.Equal(int64(15), timerTasks[4].Version)

	visTimer0 := timerTasks[0].VisibilityTime
	visTimer4 := timerTasks[4].VisibilityTime.Add(1 * time.Second)
	err2 = s.RangeCompleteTimerTask(*visTimer0, visTimer4)
	s.NoError(err2)

	timerTasks2, err2 := s.GetTimerIndexTasks(100, false)
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

	task0, err0 := s.CreateWorkflowExecution(namespaceID, workflowExecution, "taskQueue", "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedStats := copyExecutionStats(state0.ExecutionStats)
	updatedInfo.NextEventID = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)
	tasks := []p.Task{
		&p.WorkflowTaskTimeoutTask{time.Now().UTC(), 1, 2, 3, enumspb.TIMEOUT_TYPE_START_TO_CLOSE, 11},
		&p.WorkflowTimeoutTask{time.Now().UTC(), 2, 12},
		&p.DeleteHistoryEventTask{time.Now().UTC(), 3, 13},
		&p.ActivityTimeoutTask{time.Now().UTC(), 4, enumspb.TIMEOUT_TYPE_START_TO_CLOSE, 7, 0, 14},
		&p.UserTimerTask{time.Now().UTC(), 5, 7, 15},
	}
	err2 := s.UpdateWorkflowExecution(updatedInfo, updatedStats, nil, []int64{int64(4)}, nil, int64(3), tasks, nil, nil, nil, nil)
	s.NoError(err2)

	timerTasks, err1 := s.GetTimerIndexTasks(1, true) // use page size one to force pagination
	s.NoError(err1)
	s.NotNil(timerTasks, "expected valid list of tasks.")
	s.Equal(len(tasks), len(timerTasks))
	s.EqualValues(enumsspb.TASK_TYPE_WORKFLOW_TASK_TIMEOUT, timerTasks[0].TaskType)
	s.EqualValues(enumsspb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT, timerTasks[1].TaskType)
	s.EqualValues(enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT, timerTasks[2].TaskType)
	s.EqualValues(enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT, timerTasks[3].TaskType)
	s.EqualValues(enumsspb.TASK_TYPE_USER_TIMER, timerTasks[4].TaskType)
	s.Equal(int64(11), timerTasks[0].Version)
	s.Equal(int64(12), timerTasks[1].Version)
	s.Equal(int64(13), timerTasks[2].Version)
	s.Equal(int64(14), timerTasks[3].Version)
	s.Equal(int64(15), timerTasks[4].Version)

	err2 = s.UpdateWorkflowExecution(updatedInfo, updatedStats, nil, nil, nil, int64(5), nil, nil, nil, nil, nil)
	s.NoError(err2)

	err2 = s.CompleteTimerTask(*timerTasks[0].VisibilityTime, timerTasks[0].GetTaskId())
	s.NoError(err2)

	err2 = s.CompleteTimerTask(*timerTasks[1].VisibilityTime, timerTasks[1].GetTaskId())
	s.NoError(err2)

	err2 = s.CompleteTimerTask(*timerTasks[2].VisibilityTime, timerTasks[2].GetTaskId())
	s.NoError(err2)

	err2 = s.CompleteTimerTask(*timerTasks[3].VisibilityTime, timerTasks[3].GetTaskId())
	s.NoError(err2)

	err2 = s.CompleteTimerTask(*timerTasks[4].VisibilityTime, timerTasks[4].GetTaskId())
	s.NoError(err2)

	timerTasks2, err2 := s.GetTimerIndexTasks(100, false)
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

	state0, err1 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedStats := copyExecutionStats(state0.ExecutionStats)
	updatedInfo.NextEventID = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)
	currentTime := time.Now().UTC()

	activityInfos := []*persistenceblobs.ActivityInfo{{
		Version:                     7789,
		ScheduleId:                  1,
		ScheduledEventBatchId:       1,
		ScheduledEvent:              &historypb.HistoryEvent{EventId: 1},
		ScheduledTime:               &currentTime,
		ActivityId:                  uuid.New(),
		RequestId:                   uuid.New(),
		LastHeartbeatDetails:        payloads.EncodeString(uuid.New()),
		StartedId:                   2,
		StartedEvent:                &historypb.HistoryEvent{EventId: 2},
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
	err2 := s.UpdateWorkflowExecution(updatedInfo, updatedStats, nil, []int64{int64(4)}, nil, int64(3), nil, activityInfos, nil, nil, nil)
	s.NoError(err2)

	state, err1 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err1)
	s.NotNil(state, "expected valid state.")
	s.Equal(1, len(state.ActivityInfos))
	log.Printf("%+v", state.ActivityInfos)
	ai, ok := state.ActivityInfos[1]
	s.True(ok)
	s.NotNil(ai)
	s.Equal(int64(7789), ai.Version)
	s.Equal(int64(1), ai.ScheduleId)
	s.Equal(int64(1), ai.ScheduledEventBatchId)
	s.Equal(int64(1), ai.ScheduledEvent.EventId)
	s.EqualTimes(currentTime, *ai.ScheduledTime)
	s.Equal(activityInfos[0].ActivityId, ai.ActivityId)
	s.Equal(activityInfos[0].RequestId, ai.RequestId)
	s.Equal(activityInfos[0].LastHeartbeatDetails, ai.LastHeartbeatDetails)
	s.Equal(int64(2), ai.StartedId)
	s.Equal(int64(2), ai.StartedEvent.EventId)
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

	err2 = s.UpdateWorkflowExecution(updatedInfo, updatedStats, nil, nil, nil, int64(5), nil, nil, []int64{1}, nil, nil)
	s.NoError(err2)

	state, err2 = s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
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

	state0, err1 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedStats := copyExecutionStats(state0.ExecutionStats)
	updatedInfo.NextEventID = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)
	currentTime := timestamp.TimePtr(time.Date(2078, 8, 22, 12, 59, 59, 999999, time.UTC))
	timerID := "id_1"
	timerInfos := []*persistenceblobs.TimerInfo{{
		Version:    3345,
		TimerId:    timerID,
		ExpiryTime: currentTime,
		TaskStatus: 2,
		StartedId:  5,
	}}
	err2 := s.UpdateWorkflowExecution(updatedInfo, updatedStats, nil, []int64{int64(4)}, nil, int64(3), nil, nil, nil, timerInfos, nil)
	s.NoError(err2)

	state, err1 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err1)
	s.NotNil(state, "expected valid state.")
	s.Equal(1, len(state.TimerInfos))
	s.Equal(int64(3345), state.TimerInfos[timerID].Version)
	s.Equal(timerID, state.TimerInfos[timerID].GetTimerId())
	s.Equal(currentTime, state.TimerInfos[timerID].ExpiryTime)
	s.Equal(int64(2), state.TimerInfos[timerID].TaskStatus)
	s.Equal(int64(5), state.TimerInfos[timerID].GetStartedId())

	err2 = s.UpdateWorkflowExecution(updatedInfo, updatedStats, nil, nil, nil, int64(5), nil, nil, nil, nil, []string{timerID})
	s.NoError(err2)

	state, err2 = s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
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

	state0, err1 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")
	s.Equal(parentNamespaceID, info0.ParentNamespaceID)
	s.Equal(parentExecution.GetWorkflowId(), info0.ParentWorkflowID)
	s.Equal(parentExecution.GetRunId(), info0.ParentRunID)
	s.Equal(int64(1), info0.InitiatedID)

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedStats := copyExecutionStats(state0.ExecutionStats)
	updatedInfo.NextEventID = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)
	createRequestID := uuid.New()
	childExecutionInfos := []*persistenceblobs.ChildExecutionInfo{{
		Version:           1234,
		InitiatedId:       1,
		InitiatedEvent:    &historypb.HistoryEvent{EventId: 1},
		StartedId:         2,
		StartedEvent:      &historypb.HistoryEvent{EventId: 2},
		CreateRequestId:   createRequestID,
		ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_TERMINATE,
	}}
	err2 := s.UpsertChildExecutionsState(updatedInfo, updatedStats, nil, int64(3), childExecutionInfos)
	s.NoError(err2)

	state, err1 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err1)
	s.NotNil(state, "expected valid state.")
	s.Equal(1, len(state.ChildExecutionInfos))
	ci, ok := state.ChildExecutionInfos[1]
	s.True(ok)
	s.NotNil(ci)
	s.Equal(int64(1234), ci.Version)
	s.Equal(int64(1), ci.InitiatedId)
	s.Equal(enumspb.PARENT_CLOSE_POLICY_TERMINATE, ci.ParentClosePolicy)
	s.Equal(int64(1), ci.InitiatedEvent.EventId)
	s.Equal(int64(2), ci.StartedId)
	s.Equal(int64(2), ci.StartedEvent.EventId)
	s.Equal(createRequestID, ci.CreateRequestId)

	err2 = s.DeleteChildExecutionsState(updatedInfo, updatedStats, nil, int64(5), int64(1))
	s.NoError(err2)

	state, err2 = s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
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

	state0, err1 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedStats := copyExecutionStats(state0.ExecutionStats)
	updatedInfo.NextEventID = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)
	requestCancelInfo := &persistenceblobs.RequestCancelInfo{
		Version:               456,
		InitiatedId:           2,
		InitiatedEventBatchId: 1,
		CancelRequestId:       uuid.New(),
	}
	err2 := s.UpsertRequestCancelState(updatedInfo, updatedStats, nil, int64(3), []*persistenceblobs.RequestCancelInfo{requestCancelInfo})
	s.NoError(err2)

	state, err1 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err1)
	s.NotNil(state, "expected valid state.")
	s.Equal(1, len(state.RequestCancelInfos))
	ri, ok := state.RequestCancelInfos[requestCancelInfo.GetInitiatedId()]
	s.True(ok)
	s.NotNil(ri)
	s.Equal(requestCancelInfo, ri)

	err2 = s.DeleteCancelState(updatedInfo, updatedStats, nil, int64(5), requestCancelInfo.GetInitiatedId())
	s.NoError(err2)

	state, err2 = s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
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

	state0, err1 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedStats := copyExecutionStats(state0.ExecutionStats)
	updatedInfo.NextEventID = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)
	signalInfo := &persistenceblobs.SignalInfo{
		Version:               123,
		InitiatedId:           2,
		InitiatedEventBatchId: 1,
		RequestId:             uuid.New(),
		Name:                  "my signal",
		Input:                 payloads.EncodeString("test signal input"),
		Control:               uuid.New(),
	}
	err2 := s.UpsertSignalInfoState(updatedInfo, updatedStats, nil, int64(3), []*persistenceblobs.SignalInfo{signalInfo})
	s.NoError(err2)

	state, err1 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err1)
	s.NotNil(state, "expected valid state.")
	s.Equal(1, len(state.SignalInfos))
	si, ok := state.SignalInfos[signalInfo.GetInitiatedId()]
	s.True(ok)
	s.NotNil(si)
	s.Equal(signalInfo, si)

	err2 = s.DeleteSignalState(updatedInfo, updatedStats, nil, int64(5), signalInfo.GetInitiatedId())
	s.NoError(err2)

	state, err2 = s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
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

	state0, err1 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedStats := copyExecutionStats(state0.ExecutionStats)
	updatedInfo.NextEventID = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)
	signalRequestedID := uuid.New()
	signalsRequested := []string{signalRequestedID}
	err2 := s.UpsertSignalsRequestedState(updatedInfo, updatedStats, nil, int64(3), signalsRequested)
	s.NoError(err2)

	state, err1 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err1)
	s.NotNil(state, "expected valid state.")
	s.Equal(1, len(state.SignalRequestedIDs))
	ri, ok := state.SignalRequestedIDs[signalRequestedID]
	s.True(ok)
	s.NotNil(ri)

	err2 = s.DeleteSignalsRequestedState(updatedInfo, updatedStats, nil, int64(5), signalRequestedID)
	s.NoError(err2)

	state, err2 = s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err2)
	s.NotNil(state, "expected valid state.")
	s.Equal(0, len(state.SignalRequestedIDs))
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

	state0, err1 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedStats := copyExecutionStats(state0.ExecutionStats)
	updatedInfo.NextEventID = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)

	err2 := s.UpdateWorkflowExecution(updatedInfo, updatedStats, nil, []int64{int64(4)}, nil, int64(3), nil, nil, nil, nil, nil)
	s.NoError(err2)

	state, err1 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err1)
	s.NotNil(state, "expected valid state.")
	s.NotNil(state.ExecutionInfo, "expected valid MS Info state.")
	s.Equal(updatedInfo.NextEventID, state.ExecutionInfo.NextEventID)
	s.Equal(updatedInfo.State, state.ExecutionInfo.State)
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

	state0, err1 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	continueAsNewInfo := copyWorkflowExecutionInfo(info0)
	continueAsNewStats := copyExecutionStats(state0.ExecutionStats)
	continueAsNewInfo.State = enumsspb.WORKFLOW_EXECUTION_STATE_CREATED
	continueAsNewInfo.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	continueAsNewInfo.NextEventID = int64(5)
	continueAsNewInfo.LastProcessedEvent = int64(2)

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

	err2 := s.ContinueAsNewExecution(continueAsNewInfo, continueAsNewStats, info0.NextEventID, newWorkflowExecution, int64(3), int64(2), &testResetPoints)
	s.NoError(err2)

	prevExecutionState, err3 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err3)
	prevExecutionInfo := prevExecutionState.ExecutionInfo
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, prevExecutionInfo.State)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW, prevExecutionInfo.Status)
	s.Equal(int64(5), prevExecutionInfo.NextEventID)
	s.Equal(int64(2), prevExecutionInfo.LastProcessedEvent)
	s.True(reflect.DeepEqual(prevExecutionInfo.AutoResetPoints, &workflowpb.ResetPoints{}))

	newExecutionState, err4 := s.GetWorkflowExecutionInfo(namespaceID, newWorkflowExecution)
	s.NoError(err4)
	newExecutionInfo := newExecutionState.ExecutionInfo
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_CREATED, newExecutionInfo.State)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, newExecutionInfo.Status)
	s.Equal(int64(3), newExecutionInfo.NextEventID)
	s.Equal(common.EmptyEventID, newExecutionInfo.LastProcessedEvent)
	s.Equal(int64(2), newExecutionInfo.WorkflowTaskScheduleID)
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

	task0, err := s.CreateWorkflowExecution(namespaceID, workflowExecution, "queue1", "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, nil)
	s.NoError(err)
	s.NotNil(task0, "Expected non empty task identifier.")

	taskD, err := s.GetTransferTasks(1, false)
	s.Equal(1, len(taskD), "Expected 1 workflow task.")
	err = s.CompleteTransferTask(taskD[0].GetTaskId())
	s.NoError(err)

	state1, err := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err)
	info1 := state1.ExecutionInfo
	s.NotNil(info1, "Valid Workflow info expected.")
	updatedInfo1 := copyWorkflowExecutionInfo(info1)
	updatedStats1 := copyExecutionStats(state1.ExecutionStats)

	replicationTasks := []p.Task{&p.HistoryReplicationTask{
		TaskID:       s.GetNextSequenceNumber(),
		FirstEventID: int64(1),
		NextEventID:  int64(3),
		Version:      int64(9),
	}}
	err = s.UpdateWorklowStateAndReplication(updatedInfo1, updatedStats1, nil, int64(3), replicationTasks)
	s.NoError(err)

	tasks1, err := s.GetReplicationTasks(1, false)
	s.NoError(err)
	s.NotNil(tasks1, "expected valid list of tasks.")
	s.Equal(1, len(tasks1), "Expected 1 replication task.")
	task1 := tasks1[0]
	s.Equal(enumsspb.TASK_TYPE_REPLICATION_HISTORY, task1.TaskType)
	s.Equal(namespaceID, task1.GetNamespaceId())
	s.Equal(workflowExecution.GetWorkflowId(), task1.GetWorkflowId())
	s.Equal(workflowExecution.GetRunId(), task1.GetRunId())
	s.Equal(int64(1), task1.GetFirstEventId())
	s.Equal(int64(3), task1.GetNextEventId())
	s.Equal(int64(9), task1.Version)

	err = s.CompleteTransferTask(task1.GetTaskId())
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

	task0, err := s.CreateWorkflowExecution(namespaceID, workflowExecution, "queue1", "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, nil)
	s.NoError(err)
	s.NotNil(task0, "Expected non empty task identifier.")

	taskD, err := s.GetTransferTasks(1, false)
	s.Equal(1, len(taskD), "Expected 1 workflow task.")
	err = s.CompleteTransferTask(taskD[0].GetTaskId())
	s.NoError(err)

	state1, err := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err)
	info1 := state1.ExecutionInfo
	s.NotNil(info1, "Valid Workflow info expected.")
	updatedInfo1 := copyWorkflowExecutionInfo(info1)
	updatedStats1 := copyExecutionStats(state1.ExecutionStats)

	replicationTasks := []p.Task{
		&p.HistoryReplicationTask{
			TaskID:       s.GetNextSequenceNumber(),
			FirstEventID: int64(1),
			NextEventID:  int64(3),
			Version:      int64(9),
		},
		&p.HistoryReplicationTask{
			TaskID:       s.GetNextSequenceNumber(),
			FirstEventID: int64(4),
			NextEventID:  int64(5),
			Version:      int64(9),
		},
	}
	err = s.UpdateWorklowStateAndReplication(
		updatedInfo1,
		updatedStats1,
		nil,
		int64(3),
		replicationTasks,
	)
	s.NoError(err)

	tasks1, err := s.GetReplicationTasks(1, false)
	s.NoError(err)
	s.NotNil(tasks1, "expected valid list of tasks.")
	s.Equal(1, len(tasks1), "Expected 1 replication task.")
	task1 := tasks1[0]
	s.Equal(enumsspb.TASK_TYPE_REPLICATION_HISTORY, task1.TaskType)
	s.Equal(namespaceID, task1.GetNamespaceId())
	s.Equal(workflowExecution.GetWorkflowId(), task1.GetWorkflowId())
	s.Equal(workflowExecution.GetRunId(), task1.GetRunId())
	s.Equal(int64(1), task1.GetFirstEventId())
	s.Equal(int64(3), task1.GetNextEventId())
	s.Equal(int64(9), task1.Version)

	err = s.RangeCompleteReplicationTask(task1.GetTaskId())
	s.NoError(err)
	tasks2, err := s.GetReplicationTasks(1, false)
	s.NoError(err)
	s.NotNil(tasks2, "expected valid list of tasks.")
	task2 := tasks2[0]
	s.Equal(enumsspb.TASK_TYPE_REPLICATION_HISTORY, task2.TaskType)
	s.Equal(namespaceID, task2.GetNamespaceId())
	s.Equal(workflowExecution.GetWorkflowId(), task2.GetWorkflowId())
	s.Equal(workflowExecution.GetRunId(), task2.GetRunId())
	s.Equal(int64(4), task2.GetFirstEventId())
	s.Equal(int64(5), task2.GetNextEventId())
	s.Equal(int64(9), task2.Version)
	err = s.CompleteReplicationTask(task2.GetTaskId())
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
	shardInfo := &persistenceblobs.ShardInfo{
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
	createRequest := &p.CreateShardRequest{
		ShardInfo: shardInfo,
	}
	s.Nil(s.ShardMgr.CreateShard(createRequest))

	shardInfo.ClusterTransferAckLevel = map[string]int64{
		s.ClusterMetadata.GetCurrentClusterName(): currentClusterTransferAck,
	}
	shardInfo.ClusterTimerAckLevel = map[string]*time.Time{
		s.ClusterMetadata.GetCurrentClusterName(): currentClusterTimerAck,
	}
	resp, err := s.ShardMgr.GetShard(&p.GetShardRequest{ShardID: shardID})
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
	shardInfo := &persistenceblobs.ShardInfo{
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
	createRequest := &p.CreateShardRequest{
		ShardInfo: shardInfo,
	}
	s.Nil(s.ShardMgr.CreateShard(createRequest))
	resp, err := s.ShardMgr.GetShard(&p.GetShardRequest{ShardID: shardID})
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
	shardInfo = &persistenceblobs.ShardInfo{
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

	resp, err = s.ShardMgr.GetShard(&p.GetShardRequest{ShardID: shardID})
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
	taskInfo := &persistenceblobs.ReplicationTaskInfo{
		NamespaceId: uuid.New(),
		WorkflowId:  uuid.New(),
		RunId:       uuid.New(),
		TaskId:      0,
		TaskType:    0,
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

	taskInfo1 := &persistenceblobs.ReplicationTaskInfo{
		NamespaceId: uuid.New(),
		WorkflowId:  uuid.New(),
		RunId:       uuid.New(),
		TaskId:      1,
		TaskType:    0,
	}
	taskInfo2 := &persistenceblobs.ReplicationTaskInfo{
		NamespaceId: uuid.New(),
		WorkflowId:  uuid.New(),
		RunId:       uuid.New(),
		TaskId:      2,
		TaskType:    0,
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

func copyWorkflowExecutionInfo(sourceInfo *p.WorkflowExecutionInfo) *p.WorkflowExecutionInfo {
	return &p.WorkflowExecutionInfo{
		NamespaceID:                sourceInfo.NamespaceID,
		WorkflowID:                 sourceInfo.WorkflowID,
		RunID:                      sourceInfo.RunID,
		ParentNamespaceID:          sourceInfo.ParentNamespaceID,
		ParentWorkflowID:           sourceInfo.ParentWorkflowID,
		ParentRunID:                sourceInfo.ParentRunID,
		InitiatedID:                sourceInfo.InitiatedID,
		CompletionEvent:            sourceInfo.CompletionEvent,
		TaskQueue:                  sourceInfo.TaskQueue,
		WorkflowTypeName:           sourceInfo.WorkflowTypeName,
		WorkflowRunTimeout:         sourceInfo.WorkflowRunTimeout,
		DefaultWorkflowTaskTimeout: sourceInfo.DefaultWorkflowTaskTimeout,

		State:                  sourceInfo.State,
		Status:                 sourceInfo.Status,
		LastFirstEventID:       sourceInfo.LastFirstEventID,
		NextEventID:            sourceInfo.NextEventID,
		LastProcessedEvent:     sourceInfo.LastProcessedEvent,
		LastUpdatedTimestamp:   sourceInfo.LastUpdatedTimestamp,
		CreateRequestID:        sourceInfo.CreateRequestID,
		WorkflowTaskVersion:    sourceInfo.WorkflowTaskVersion,
		WorkflowTaskScheduleID: sourceInfo.WorkflowTaskScheduleID,
		WorkflowTaskStartedID:  sourceInfo.WorkflowTaskStartedID,
		WorkflowTaskRequestID:  sourceInfo.WorkflowTaskRequestID,
		WorkflowTaskTimeout:    sourceInfo.WorkflowTaskTimeout,
		BranchToken:            sourceInfo.BranchToken,
		AutoResetPoints:        sourceInfo.AutoResetPoints,
	}
}

func copyExecutionStats(sourceStats *persistenceblobs.ExecutionStats) *persistenceblobs.ExecutionStats {
	return &persistenceblobs.ExecutionStats{
		HistorySize: sourceStats.HistorySize,
	}
}

// Note: cassandra only provide millisecond precision timestamp
// ref: https://docs.datastax.com/en/cql/3.3/cql/cql_reference/timestamp_type_r.html
// so to use equal function, we need to do conversion, getting rid of sub milliseconds
func timestampConvertor(t time.Time) *time.Time {
	r := time.Unix(
		0,
		p.DBTimestampToUnixNano(p.UnixNanoToDBTimestamp(t.UnixNano()))).UTC()

	return &r
}

func timeComparator(r1, r2 *types.Timestamp, timeTolerance time.Duration) bool {
	t1, _ := types.TimestampFromProto(r1)
	t2, _ := types.TimestampFromProto(r2)

	return timeComparatorGo(t1, t2, timeTolerance)
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

func copyReplicationInfo(sourceInfo *replicationspb.ReplicationInfo) *replicationspb.ReplicationInfo {
	return &replicationspb.ReplicationInfo{
		Version:     sourceInfo.Version,
		LastEventId: sourceInfo.LastEventId,
	}
}
