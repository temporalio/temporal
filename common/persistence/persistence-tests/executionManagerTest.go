// Copyright (c) 2017 Uber Technologies, Inc.
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
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	gen "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cluster"
	p "github.com/uber/cadence/common/persistence"
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

// TestCreateWorkflowExecutionStateCloseStatus test
func (s *ExecutionManagerSuite) TestCreateWorkflowExecutionStateCloseStatus() {
	domainID := uuid.New()
	invalidCloseStatuses := []int{
		p.WorkflowCloseStatusCompleted,
		p.WorkflowCloseStatusFailed,
		p.WorkflowCloseStatusCanceled,
		p.WorkflowCloseStatusTerminated,
		p.WorkflowCloseStatusContinuedAsNew,
		p.WorkflowCloseStatusTimedOut,
	}
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(10)
	decisionTimeout := int32(14)
	lastProcessedEventID := int64(0)
	nextEventID := int64(3)

	req := &p.CreateWorkflowExecutionRequest{
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &p.WorkflowExecutionInfo{
				CreateRequestID:      uuid.New(),
				DomainID:             domainID,
				TaskList:             tasklist,
				WorkflowTypeName:     workflowType,
				WorkflowTimeout:      workflowTimeout,
				DecisionTimeoutValue: decisionTimeout,
				LastFirstEventID:     common.FirstEventID,
				NextEventID:          nextEventID,
				LastProcessedEvent:   lastProcessedEventID,
			},
			ExecutionStats: &p.ExecutionStats{},
		},
		RangeID: s.ShardInfo.RangeID,
		Mode:    p.CreateWorkflowModeBrandNew,
	}

	workflowExecutionStatusCreated := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("create-workflow-test-state-created"),
		RunId:      common.StringPtr(uuid.New()),
	}
	req.NewWorkflowSnapshot.ExecutionInfo.WorkflowID = workflowExecutionStatusCreated.GetWorkflowId()
	req.NewWorkflowSnapshot.ExecutionInfo.RunID = workflowExecutionStatusCreated.GetRunId()
	req.NewWorkflowSnapshot.ExecutionInfo.State = p.WorkflowStateCreated
	for _, invalidCloseStatus := range invalidCloseStatuses {
		req.NewWorkflowSnapshot.ExecutionInfo.CloseStatus = invalidCloseStatus
		_, err := s.ExecutionManager.CreateWorkflowExecution(req)
		s.IsType(&gen.InternalServiceError{}, err)
	}
	req.NewWorkflowSnapshot.ExecutionInfo.CloseStatus = p.WorkflowCloseStatusNone
	_, err := s.ExecutionManager.CreateWorkflowExecution(req)
	s.Nil(err)
	info, err := s.GetWorkflowExecutionInfo(domainID, workflowExecutionStatusCreated)
	s.Nil(err)
	s.Equal(p.WorkflowStateCreated, info.ExecutionInfo.State)
	s.Equal(p.WorkflowCloseStatusNone, info.ExecutionInfo.CloseStatus)

	workflowExecutionStatusRunning := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("create-workflow-test-state-running"),
		RunId:      common.StringPtr(uuid.New()),
	}
	req.NewWorkflowSnapshot.ExecutionInfo.WorkflowID = workflowExecutionStatusRunning.GetWorkflowId()
	req.NewWorkflowSnapshot.ExecutionInfo.RunID = workflowExecutionStatusRunning.GetRunId()
	req.NewWorkflowSnapshot.ExecutionInfo.State = p.WorkflowStateRunning
	for _, invalidCloseStatus := range invalidCloseStatuses {
		req.NewWorkflowSnapshot.ExecutionInfo.CloseStatus = invalidCloseStatus
		_, err := s.ExecutionManager.CreateWorkflowExecution(req)
		s.IsType(&gen.InternalServiceError{}, err)
	}
	req.NewWorkflowSnapshot.ExecutionInfo.CloseStatus = p.WorkflowCloseStatusNone
	_, err = s.ExecutionManager.CreateWorkflowExecution(req)
	s.Nil(err)
	info, err = s.GetWorkflowExecutionInfo(domainID, workflowExecutionStatusRunning)
	s.Nil(err)
	s.Equal(p.WorkflowStateRunning, info.ExecutionInfo.State)
	s.Equal(p.WorkflowCloseStatusNone, info.ExecutionInfo.CloseStatus)

	workflowExecutionStatusCompleted := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("create-workflow-test-state-completed"),
		RunId:      common.StringPtr(uuid.New()),
	}
	req.NewWorkflowSnapshot.ExecutionInfo.WorkflowID = workflowExecutionStatusCompleted.GetWorkflowId()
	req.NewWorkflowSnapshot.ExecutionInfo.RunID = workflowExecutionStatusCompleted.GetRunId()
	req.NewWorkflowSnapshot.ExecutionInfo.State = p.WorkflowStateCompleted
	for _, invalidCloseStatus := range invalidCloseStatuses {
		req.NewWorkflowSnapshot.ExecutionInfo.CloseStatus = invalidCloseStatus
		_, err := s.ExecutionManager.CreateWorkflowExecution(req)
		s.IsType(&gen.InternalServiceError{}, err)
	}
	req.NewWorkflowSnapshot.ExecutionInfo.CloseStatus = p.WorkflowCloseStatusNone
	_, err = s.ExecutionManager.CreateWorkflowExecution(req)
	s.IsType(&gen.InternalServiceError{}, err)

	// for zombie workflow creation, we must use existing workflow ID which got created
	// since we do not allow creation of zombie workflow without current record
	workflowExecutionStatusZombie := gen.WorkflowExecution{
		WorkflowId: workflowExecutionStatusRunning.WorkflowId,
		RunId:      common.StringPtr(uuid.New()),
	}
	req.Mode = p.CreateWorkflowModeZombie
	req.NewWorkflowSnapshot.ExecutionInfo.WorkflowID = workflowExecutionStatusZombie.GetWorkflowId()
	req.NewWorkflowSnapshot.ExecutionInfo.RunID = workflowExecutionStatusZombie.GetRunId()
	req.NewWorkflowSnapshot.ExecutionInfo.State = p.WorkflowStateZombie
	for _, invalidCloseStatus := range invalidCloseStatuses {
		req.NewWorkflowSnapshot.ExecutionInfo.CloseStatus = invalidCloseStatus
		_, err := s.ExecutionManager.CreateWorkflowExecution(req)
		s.IsType(&gen.InternalServiceError{}, err)
	}
	req.NewWorkflowSnapshot.ExecutionInfo.CloseStatus = p.WorkflowCloseStatusNone
	_, err = s.ExecutionManager.CreateWorkflowExecution(req)
	s.Nil(err)
	info, err = s.GetWorkflowExecutionInfo(domainID, workflowExecutionStatusZombie)
	s.Nil(err)
	s.Equal(p.WorkflowStateZombie, info.ExecutionInfo.State)
	s.Equal(p.WorkflowCloseStatusNone, info.ExecutionInfo.CloseStatus)
}

// TestCreateWorkflowExecutionWithZombieState test
func (s *ExecutionManagerSuite) TestCreateWorkflowExecutionWithZombieState() {
	domainID := uuid.New()
	workflowID := "create-workflow-test-with-zombie-state"
	workflowExecutionZombie1 := gen.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(uuid.New()),
	}
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(10)
	decisionTimeout := int32(14)
	lastProcessedEventID := int64(0)
	nextEventID := int64(3)

	req := &p.CreateWorkflowExecutionRequest{
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &p.WorkflowExecutionInfo{
				CreateRequestID:      uuid.New(),
				DomainID:             domainID,
				WorkflowID:           workflowID,
				RunID:                workflowExecutionZombie1.GetRunId(),
				TaskList:             tasklist,
				WorkflowTypeName:     workflowType,
				WorkflowTimeout:      workflowTimeout,
				DecisionTimeoutValue: decisionTimeout,
				NextEventID:          nextEventID,
				LastProcessedEvent:   lastProcessedEventID,
				State:                p.WorkflowStateZombie,
				CloseStatus:          p.WorkflowCloseStatusNone,
			},
			ExecutionStats: &p.ExecutionStats{},
		},
		RangeID: s.ShardInfo.RangeID,
		Mode:    p.CreateWorkflowModeZombie,
	}
	_, err := s.ExecutionManager.CreateWorkflowExecution(req)
	s.Nil(err) // allow creating a zombie workflow if no current running workflow
	_, err = s.GetCurrentWorkflowRunID(domainID, workflowID)
	s.IsType(&gen.EntityNotExistsError{}, err) // no current workflow

	workflowExecutionRunning := gen.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(uuid.New()),
	}
	req.NewWorkflowSnapshot.ExecutionInfo.RunID = workflowExecutionRunning.GetRunId()
	req.Mode = p.CreateWorkflowModeBrandNew
	req.NewWorkflowSnapshot.ExecutionInfo.State = p.WorkflowStateRunning
	req.NewWorkflowSnapshot.ExecutionInfo.CloseStatus = p.WorkflowCloseStatusNone
	_, err = s.ExecutionManager.CreateWorkflowExecution(req)
	s.Nil(err)
	currentRunID, err := s.GetCurrentWorkflowRunID(domainID, workflowID)
	s.Nil(err)
	s.Equal(workflowExecutionRunning.GetRunId(), currentRunID)

	workflowExecutionZombie := gen.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(uuid.New()),
	}
	req.NewWorkflowSnapshot.ExecutionInfo.RunID = workflowExecutionZombie.GetRunId()
	req.Mode = p.CreateWorkflowModeZombie
	req.NewWorkflowSnapshot.ExecutionInfo.State = p.WorkflowStateZombie
	req.NewWorkflowSnapshot.ExecutionInfo.CloseStatus = p.WorkflowCloseStatusNone
	_, err = s.ExecutionManager.CreateWorkflowExecution(req)
	s.Nil(err)
	// current run ID is still the prev running run ID
	currentRunID, err = s.GetCurrentWorkflowRunID(domainID, workflowExecutionRunning.GetWorkflowId())
	s.Nil(err)
	s.Equal(workflowExecutionRunning.GetRunId(), currentRunID)
	info, err := s.GetWorkflowExecutionInfo(domainID, workflowExecutionZombie)
	s.Nil(err)
	s.Equal(p.WorkflowStateZombie, info.ExecutionInfo.State)
	s.Equal(p.WorkflowCloseStatusNone, info.ExecutionInfo.CloseStatus)
}

// TestUpdateWorkflowExecutionStateCloseStatus test
func (s *ExecutionManagerSuite) TestUpdateWorkflowExecutionStateCloseStatus() {
	domainID := uuid.New()
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("update-workflow-test-state"),
		RunId:      common.StringPtr(uuid.New()),
	}
	closeStatuses := []int{
		p.WorkflowCloseStatusCompleted,
		p.WorkflowCloseStatusFailed,
		p.WorkflowCloseStatusCanceled,
		p.WorkflowCloseStatusTerminated,
		p.WorkflowCloseStatusContinuedAsNew,
		p.WorkflowCloseStatusTimedOut,
	}
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(10)
	decisionTimeout := int32(14)
	lastProcessedEventID := int64(0)
	nextEventID := int64(3)

	req := &p.CreateWorkflowExecutionRequest{
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &p.WorkflowExecutionInfo{
				CreateRequestID:      uuid.New(),
				DomainID:             domainID,
				WorkflowID:           workflowExecution.GetWorkflowId(),
				RunID:                workflowExecution.GetRunId(),
				TaskList:             tasklist,
				WorkflowTypeName:     workflowType,
				WorkflowTimeout:      workflowTimeout,
				DecisionTimeoutValue: decisionTimeout,
				LastFirstEventID:     common.FirstEventID,
				NextEventID:          nextEventID,
				LastProcessedEvent:   lastProcessedEventID,
			},
			ExecutionStats: &p.ExecutionStats{},
		},
		RangeID: s.ShardInfo.RangeID,
		Mode:    p.CreateWorkflowModeBrandNew,
	}

	req.NewWorkflowSnapshot.ExecutionInfo.State = p.WorkflowStateCreated
	req.NewWorkflowSnapshot.ExecutionInfo.CloseStatus = p.WorkflowCloseStatusNone
	_, err := s.ExecutionManager.CreateWorkflowExecution(req)
	s.Nil(err)
	info, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)
	s.Equal(p.WorkflowStateCreated, info.ExecutionInfo.State)
	s.Equal(p.WorkflowCloseStatusNone, info.ExecutionInfo.CloseStatus)

	updatedInfo := copyWorkflowExecutionInfo(info.ExecutionInfo)
	updatedStats := copyExecutionStats(info.ExecutionStats)
	updatedInfo.State = p.WorkflowStateRunning
	updatedInfo.CloseStatus = p.WorkflowCloseStatusNone
	_, err = s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		UpdateWorkflowMutation: p.WorkflowMutation{
			ExecutionInfo:  updatedInfo,
			ExecutionStats: updatedStats,
			Condition:      nextEventID,
		},
		RangeID: s.ShardInfo.RangeID,
		Mode:    p.UpdateWorkflowModeUpdateCurrent,
	})
	s.NoError(err)
	info, err = s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)
	s.Equal(p.WorkflowStateRunning, info.ExecutionInfo.State)
	s.Equal(p.WorkflowCloseStatusNone, info.ExecutionInfo.CloseStatus)

	updatedInfo = copyWorkflowExecutionInfo(info.ExecutionInfo)
	updatedStats = copyExecutionStats(info.ExecutionStats)
	updatedInfo.State = p.WorkflowStateRunning
	for _, closeStatus := range closeStatuses {
		updatedInfo.CloseStatus = closeStatus
		_, err = s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
			UpdateWorkflowMutation: p.WorkflowMutation{
				ExecutionInfo:  updatedInfo,
				ExecutionStats: updatedStats,
				Condition:      nextEventID,
			},
			RangeID: s.ShardInfo.RangeID,
			Mode:    p.UpdateWorkflowModeUpdateCurrent,
		})
		s.IsType(&gen.InternalServiceError{}, err)
	}

	updatedInfo = copyWorkflowExecutionInfo(info.ExecutionInfo)
	updatedStats = copyExecutionStats(info.ExecutionStats)
	updatedInfo.State = p.WorkflowStateCompleted
	updatedInfo.CloseStatus = p.WorkflowCloseStatusNone
	_, err = s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		UpdateWorkflowMutation: p.WorkflowMutation{
			ExecutionInfo:  updatedInfo,
			ExecutionStats: updatedStats,
			Condition:      nextEventID,
		},
		RangeID: s.ShardInfo.RangeID,
		Mode:    p.UpdateWorkflowModeUpdateCurrent,
	})
	s.IsType(&gen.InternalServiceError{}, err)

	for _, closeStatus := range closeStatuses {
		updatedInfo.CloseStatus = closeStatus
		_, err = s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
			UpdateWorkflowMutation: p.WorkflowMutation{
				ExecutionInfo:  updatedInfo,
				ExecutionStats: updatedStats,
				Condition:      nextEventID,
			},
			RangeID: s.ShardInfo.RangeID,
			Mode:    p.UpdateWorkflowModeUpdateCurrent,
		})
		s.Nil(err)
		info, err = s.GetWorkflowExecutionInfo(domainID, workflowExecution)
		s.Nil(err)
		s.Equal(p.WorkflowStateCompleted, info.ExecutionInfo.State)
		s.Equal(closeStatus, info.ExecutionInfo.CloseStatus)
	}

	// create a new workflow with same domain ID & workflow ID
	// to enable update workflow with zombie status
	workflowExecutionRunning := gen.WorkflowExecution{
		WorkflowId: workflowExecution.WorkflowId,
		RunId:      common.StringPtr(uuid.New()),
	}
	req.NewWorkflowSnapshot.ExecutionInfo.WorkflowID = workflowExecutionRunning.GetWorkflowId()
	req.NewWorkflowSnapshot.ExecutionInfo.RunID = workflowExecutionRunning.GetRunId()
	req.Mode = p.CreateWorkflowModeWorkflowIDReuse
	req.PreviousRunID = workflowExecution.GetRunId()
	req.PreviousLastWriteVersion = common.EmptyVersion
	req.NewWorkflowSnapshot.ExecutionInfo.State = p.WorkflowStateRunning
	req.NewWorkflowSnapshot.ExecutionInfo.CloseStatus = p.WorkflowCloseStatusNone
	_, err = s.ExecutionManager.CreateWorkflowExecution(req)
	s.Nil(err)

	updatedInfo = copyWorkflowExecutionInfo(info.ExecutionInfo)
	updatedStats = copyExecutionStats(info.ExecutionStats)
	updatedInfo.State = p.WorkflowStateZombie
	updatedInfo.CloseStatus = p.WorkflowCloseStatusNone
	_, err = s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		UpdateWorkflowMutation: p.WorkflowMutation{
			ExecutionInfo:  updatedInfo,
			ExecutionStats: updatedStats,
			Condition:      nextEventID,
		},
		RangeID: s.ShardInfo.RangeID,
		Mode:    p.UpdateWorkflowModeBypassCurrent,
	})
	s.NoError(err)
	info, err = s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)
	s.Equal(p.WorkflowStateZombie, info.ExecutionInfo.State)
	s.Equal(p.WorkflowCloseStatusNone, info.ExecutionInfo.CloseStatus)

	updatedInfo = copyWorkflowExecutionInfo(info.ExecutionInfo)
	updatedStats = copyExecutionStats(info.ExecutionStats)
	updatedInfo.State = p.WorkflowStateZombie
	for _, closeStatus := range closeStatuses {
		updatedInfo.CloseStatus = closeStatus
		_, err = s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
			UpdateWorkflowMutation: p.WorkflowMutation{
				ExecutionInfo:  updatedInfo,
				ExecutionStats: updatedStats,
				Condition:      nextEventID,
			},
			RangeID: s.ShardInfo.RangeID,
			Mode:    p.UpdateWorkflowModeBypassCurrent,
		})
		s.IsType(&gen.InternalServiceError{}, err)
	}
}

// TestUpdateWorkflowExecutionWithZombieState test
func (s *ExecutionManagerSuite) TestUpdateWorkflowExecutionWithZombieState() {
	domainID := uuid.New()
	workflowID := "create-workflow-test-with-zombie-state"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(uuid.New()),
	}
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(10)
	decisionTimeout := int32(14)
	lastProcessedEventID := int64(0)
	nextEventID := int64(3)

	// create and update a workflow to make it completed
	req := &p.CreateWorkflowExecutionRequest{
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &p.WorkflowExecutionInfo{
				CreateRequestID:      uuid.New(),
				DomainID:             domainID,
				WorkflowID:           workflowExecution.GetWorkflowId(),
				RunID:                workflowExecution.GetRunId(),
				TaskList:             tasklist,
				WorkflowTypeName:     workflowType,
				WorkflowTimeout:      workflowTimeout,
				DecisionTimeoutValue: decisionTimeout,
				NextEventID:          nextEventID,
				LastProcessedEvent:   lastProcessedEventID,
				State:                p.WorkflowStateRunning,
				CloseStatus:          p.WorkflowCloseStatusNone,
			},
			ExecutionStats: &p.ExecutionStats{},
		},
		RangeID: s.ShardInfo.RangeID,
		Mode:    p.CreateWorkflowModeBrandNew,
	}
	_, err := s.ExecutionManager.CreateWorkflowExecution(req)
	s.Nil(err)
	currentRunID, err := s.GetCurrentWorkflowRunID(domainID, workflowID)
	s.Nil(err)
	s.Equal(workflowExecution.GetRunId(), currentRunID)

	info, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)

	// try to turn current workflow into zombie state, this should end with an error
	updatedInfo := copyWorkflowExecutionInfo(info.ExecutionInfo)
	updateStats := copyExecutionStats(info.ExecutionStats)
	updatedInfo.State = p.WorkflowStateZombie
	updatedInfo.CloseStatus = p.WorkflowCloseStatusNone
	_, err = s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		UpdateWorkflowMutation: p.WorkflowMutation{
			ExecutionInfo:  updatedInfo,
			ExecutionStats: updateStats,
			Condition:      nextEventID,
		},
		RangeID: s.ShardInfo.RangeID,
		Mode:    p.UpdateWorkflowModeBypassCurrent,
	})
	s.NotNil(err)

	updatedInfo = copyWorkflowExecutionInfo(info.ExecutionInfo)
	updateStats = copyExecutionStats(info.ExecutionStats)
	updatedInfo.State = p.WorkflowStateCompleted
	updatedInfo.CloseStatus = p.WorkflowCloseStatusCompleted
	_, err = s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		UpdateWorkflowMutation: p.WorkflowMutation{
			ExecutionInfo:  updatedInfo,
			ExecutionStats: updateStats,
			Condition:      nextEventID,
		},
		RangeID: s.ShardInfo.RangeID,
		Mode:    p.UpdateWorkflowModeUpdateCurrent,
	})
	s.NoError(err)

	// create a new workflow with same domain ID & workflow ID
	workflowExecutionRunning := gen.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(uuid.New()),
	}
	req.NewWorkflowSnapshot.ExecutionInfo.WorkflowID = workflowExecutionRunning.GetWorkflowId()
	req.NewWorkflowSnapshot.ExecutionInfo.RunID = workflowExecutionRunning.GetRunId()
	req.Mode = p.CreateWorkflowModeWorkflowIDReuse
	req.PreviousRunID = workflowExecution.GetRunId()
	req.PreviousLastWriteVersion = common.EmptyVersion
	req.NewWorkflowSnapshot.ExecutionInfo.State = p.WorkflowStateRunning
	req.NewWorkflowSnapshot.ExecutionInfo.CloseStatus = p.WorkflowCloseStatusNone
	_, err = s.ExecutionManager.CreateWorkflowExecution(req)
	s.Nil(err)
	currentRunID, err = s.GetCurrentWorkflowRunID(domainID, workflowID)
	s.Nil(err)
	s.Equal(workflowExecutionRunning.GetRunId(), currentRunID)

	// get the workflow to be turned into a zombie
	info, err = s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)
	updatedInfo = copyWorkflowExecutionInfo(info.ExecutionInfo)
	updateStats = copyExecutionStats(info.ExecutionStats)
	updatedInfo.State = p.WorkflowStateZombie
	updatedInfo.CloseStatus = p.WorkflowCloseStatusNone
	_, err = s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		UpdateWorkflowMutation: p.WorkflowMutation{
			ExecutionInfo:  updatedInfo,
			ExecutionStats: updateStats,
			Condition:      nextEventID,
		},
		RangeID: s.ShardInfo.RangeID,
		Mode:    p.UpdateWorkflowModeBypassCurrent,
	})
	s.NoError(err)
	info, err = s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)
	s.Equal(p.WorkflowStateZombie, info.ExecutionInfo.State)
	s.Equal(p.WorkflowCloseStatusNone, info.ExecutionInfo.CloseStatus)
	// check current run ID is un touched
	currentRunID, err = s.GetCurrentWorkflowRunID(domainID, workflowID)
	s.Nil(err)
	s.Equal(workflowExecutionRunning.GetRunId(), currentRunID)
}

// TestCreateWorkflowExecutionBrandNew test
func (s *ExecutionManagerSuite) TestCreateWorkflowExecutionBrandNew() {
	domainID := uuid.New()
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("create-workflow-test-brand-new"),
		RunId:      common.StringPtr(uuid.New()),
	}
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(10)
	decisionTimeout := int32(14)
	lastProcessedEventID := int64(0)
	nextEventID := int64(3)

	req := &p.CreateWorkflowExecutionRequest{
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &p.WorkflowExecutionInfo{
				CreateRequestID:      uuid.New(),
				DomainID:             domainID,
				WorkflowID:           workflowExecution.GetWorkflowId(),
				RunID:                workflowExecution.GetRunId(),
				TaskList:             tasklist,
				WorkflowTypeName:     workflowType,
				WorkflowTimeout:      workflowTimeout,
				DecisionTimeoutValue: decisionTimeout,
				State:                p.WorkflowStateRunning,
				CloseStatus:          p.WorkflowCloseStatusNone,
				LastFirstEventID:     common.FirstEventID,
				NextEventID:          nextEventID,
				LastProcessedEvent:   lastProcessedEventID,
			},
			ExecutionStats: &p.ExecutionStats{},
		},
		RangeID: s.ShardInfo.RangeID,
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
	s.Equal(0, alreadyStartedErr.CloseStatus)
	s.Equal(p.WorkflowStateRunning, alreadyStartedErr.State)
}

// TestCreateWorkflowExecutionRunIDReuseWithReplication test
func (s *ExecutionManagerSuite) TestCreateWorkflowExecutionRunIDReuseWithReplication() {
	domainID := uuid.New()
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("create-workflow-test-run-id-reuse-with-replication"),
		RunId:      common.StringPtr(uuid.New()),
	}
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(10)
	decisionTimeout := int32(14)
	lastProcessedEventID := int64(0)
	nextEventID := int64(3)
	decisionScheduleID := int64(2)
	version := int64(0)
	replicationState := &p.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}

	task0, err0 := s.CreateWorkflowExecutionWithReplication(domainID, workflowExecution, tasklist,
		workflowType, workflowTimeout, decisionTimeout, nextEventID,
		lastProcessedEventID, decisionScheduleID, replicationState, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	newExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowExecution.GetWorkflowId()),
		RunId:      common.StringPtr(uuid.New()),
	}

	// try to create a workflow while the current workflow is still running
	_, err := s.ExecutionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &p.WorkflowExecutionInfo{
				CreateRequestID:      uuid.New(),
				DomainID:             domainID,
				WorkflowID:           newExecution.GetWorkflowId(),
				RunID:                newExecution.GetRunId(),
				TaskList:             tasklist,
				WorkflowTypeName:     workflowType,
				WorkflowTimeout:      workflowTimeout,
				DecisionTimeoutValue: decisionTimeout,
				State:                p.WorkflowStateRunning,
				CloseStatus:          p.WorkflowCloseStatusNone,
				LastFirstEventID:     common.FirstEventID,
				NextEventID:          nextEventID,
				LastProcessedEvent:   lastProcessedEventID,
			},
			ExecutionStats:   &p.ExecutionStats{},
			ReplicationState: replicationState,
		},
		RangeID:                  s.ShardInfo.RangeID,
		Mode:                     p.CreateWorkflowModeWorkflowIDReuse,
		PreviousRunID:            workflowExecution.GetRunId(),
		PreviousLastWriteVersion: version,
	})
	s.NotNil(err)
	s.IsType(&p.CurrentWorkflowConditionFailedError{}, err, err.Error())

	info, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err)

	testResetPoints := gen.ResetPoints{
		Points: []*gen.ResetPointInfo{
			&gen.ResetPointInfo{
				BinaryChecksum:           common.StringPtr("test-binary-checksum"),
				RunId:                    common.StringPtr("test-runID"),
				FirstDecisionCompletedId: common.Int64Ptr(123),
				CreatedTimeNano:          common.Int64Ptr(456),
				Resettable:               common.BoolPtr(true),
				ExpiringTimeNano:         common.Int64Ptr(789),
			},
		},
	}

	updatedInfo := copyWorkflowExecutionInfo(info.ExecutionInfo)
	updatedStats := copyExecutionStats(info.ExecutionStats)
	updatedInfo.State = p.WorkflowStateCompleted
	updatedInfo.CloseStatus = p.WorkflowCloseStatusCompleted
	updatedInfo.NextEventID = int64(6)
	updatedInfo.LastProcessedEvent = int64(2)
	updatedInfo.AutoResetPoints = &testResetPoints
	updateReplicationState := &p.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: updatedInfo.NextEventID - 1,
	}
	_, err = s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		RangeID: s.ShardInfo.RangeID,
		UpdateWorkflowMutation: p.WorkflowMutation{
			ExecutionInfo:       updatedInfo,
			ExecutionStats:      updatedStats,
			TransferTasks:       nil,
			TimerTasks:          nil,
			Condition:           nextEventID,
			UpsertActivityInfos: nil,
			DeleteActivityInfos: nil,
			UpsertTimerInfos:    nil,
			DeleteTimerInfos:    nil,
			ReplicationState:    updateReplicationState,
		},
	})
	s.NoError(err)

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err)
	s.NotNil(state.ExecutionInfo, "Valid Workflow response expected.")
	s.Equal(testResetPoints.String(), state.ExecutionInfo.AutoResetPoints.String())

	// try to create a workflow while the current workflow is complete but run ID is wrong
	_, err = s.ExecutionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &p.WorkflowExecutionInfo{
				CreateRequestID:      uuid.New(),
				DomainID:             domainID,
				WorkflowID:           newExecution.GetWorkflowId(),
				RunID:                newExecution.GetRunId(),
				TaskList:             tasklist,
				WorkflowTypeName:     workflowType,
				WorkflowTimeout:      workflowTimeout,
				DecisionTimeoutValue: decisionTimeout,
				State:                p.WorkflowStateRunning,
				CloseStatus:          p.WorkflowCloseStatusNone,
				LastFirstEventID:     common.FirstEventID,
				NextEventID:          nextEventID,
				LastProcessedEvent:   lastProcessedEventID,
			},
			ExecutionStats:   &p.ExecutionStats{},
			ReplicationState: replicationState,
		},
		RangeID:                  s.ShardInfo.RangeID,
		Mode:                     p.CreateWorkflowModeWorkflowIDReuse,
		PreviousRunID:            uuid.New(),
		PreviousLastWriteVersion: version,
	})
	s.NotNil(err)
	s.IsType(&p.CurrentWorkflowConditionFailedError{}, err, err.Error())

	// try to create a workflow while the current workflow is complete but version is wrong
	_, err = s.ExecutionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &p.WorkflowExecutionInfo{
				CreateRequestID:      uuid.New(),
				DomainID:             domainID,
				WorkflowID:           newExecution.GetWorkflowId(),
				RunID:                newExecution.GetRunId(),
				TaskList:             tasklist,
				WorkflowTypeName:     workflowType,
				WorkflowTimeout:      workflowTimeout,
				DecisionTimeoutValue: decisionTimeout,
				State:                p.WorkflowStateRunning,
				CloseStatus:          p.WorkflowCloseStatusNone,
				LastFirstEventID:     common.FirstEventID,
				NextEventID:          nextEventID,
				LastProcessedEvent:   lastProcessedEventID,
			},
			ExecutionStats:   &p.ExecutionStats{},
			ReplicationState: replicationState,
		},
		RangeID:                  s.ShardInfo.RangeID,
		Mode:                     p.CreateWorkflowModeWorkflowIDReuse,
		PreviousRunID:            workflowExecution.GetRunId(),
		PreviousLastWriteVersion: version - 1,
	})
	s.NotNil(err)
	s.IsType(&p.CurrentWorkflowConditionFailedError{}, err, err.Error())

	// try to create a workflow while the current workflow is complete with run ID & version correct
	_, err = s.ExecutionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &p.WorkflowExecutionInfo{
				CreateRequestID:      uuid.New(),
				DomainID:             domainID,
				WorkflowID:           newExecution.GetWorkflowId(),
				RunID:                newExecution.GetRunId(),
				TaskList:             tasklist,
				WorkflowTypeName:     workflowType,
				WorkflowTimeout:      workflowTimeout,
				DecisionTimeoutValue: decisionTimeout,
				State:                p.WorkflowStateRunning,
				CloseStatus:          p.WorkflowCloseStatusNone,
				LastFirstEventID:     common.FirstEventID,
				NextEventID:          nextEventID,
				LastProcessedEvent:   lastProcessedEventID,
			},
			ExecutionStats:   &p.ExecutionStats{},
			ReplicationState: replicationState,
		},
		RangeID:                  s.ShardInfo.RangeID,
		Mode:                     p.CreateWorkflowModeWorkflowIDReuse,
		PreviousRunID:            workflowExecution.GetRunId(),
		PreviousLastWriteVersion: version,
	})
	s.Nil(err)
}

// TestCreateWorkflowExecutionRunIDReuseWithoutReplication test
func (s *ExecutionManagerSuite) TestCreateWorkflowExecutionRunIDReuseWithoutReplication() {
	domainID := uuid.New()
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("create-workflow-test-run-id-reuse-without-replication"),
		RunId:      common.StringPtr(uuid.New()),
	}
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(10)
	decisionTimeout := int32(14)
	lastProcessedEventID := int64(0)
	nextEventID := int64(3)
	decisionScheduleID := int64(2)

	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, tasklist,
		workflowType, workflowTimeout, decisionTimeout, nil, nextEventID,
		lastProcessedEventID, decisionScheduleID, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	closeInfo := copyWorkflowExecutionInfo(info0)
	closeInfo.State = p.WorkflowStateCompleted
	closeInfo.CloseStatus = p.WorkflowCloseStatusCompleted
	closeInfo.NextEventID = int64(5)
	closeInfo.LastProcessedEvent = int64(2)

	err2 := s.UpdateWorkflowExecution(closeInfo, state0.ExecutionStats, nil, nil, nil, nextEventID,
		nil, nil, nil, nil, nil)
	s.NoError(err2)

	newExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowExecution.GetWorkflowId()),
		RunId:      common.StringPtr(uuid.New()),
	}
	// this create should work since we are relying the business logic in history engine
	// to check whether the existing running workflow has finished
	_, err3 := s.ExecutionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &p.WorkflowExecutionInfo{
				CreateRequestID:      uuid.New(),
				DomainID:             domainID,
				WorkflowID:           newExecution.GetWorkflowId(),
				RunID:                newExecution.GetRunId(),
				TaskList:             tasklist,
				WorkflowTypeName:     workflowType,
				WorkflowTimeout:      workflowTimeout,
				DecisionTimeoutValue: decisionTimeout,
				State:                p.WorkflowStateRunning,
				CloseStatus:          p.WorkflowCloseStatusNone,
				LastFirstEventID:     common.FirstEventID,
				NextEventID:          nextEventID,
				LastProcessedEvent:   lastProcessedEventID,
			},
			ExecutionStats: &p.ExecutionStats{},
		},
		RangeID:                  s.ShardInfo.RangeID,
		Mode:                     p.CreateWorkflowModeWorkflowIDReuse,
		PreviousRunID:            workflowExecution.GetRunId(),
		PreviousLastWriteVersion: common.EmptyVersion,
	})
	s.NoError(err3)
}

// TestCreateWorkflowExecutionConcurrentCreate test
func (s *ExecutionManagerSuite) TestCreateWorkflowExecutionConcurrentCreate() {
	domainID := uuid.New()
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("create-workflow-test-concurrent-create"),
		RunId:      common.StringPtr(uuid.New()),
	}
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(10)
	decisionTimeout := int32(14)
	lastProcessedEventID := int64(0)
	nextEventID := int64(3)
	decisionScheduleID := int64(2)

	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, tasklist,
		workflowType, workflowTimeout, decisionTimeout, nil, nextEventID,
		lastProcessedEventID, decisionScheduleID, nil)
	s.Nil(err0, "No error expected.")
	s.NotNil(task0, "Expected non empty task identifier.")

	times := 2
	var wg sync.WaitGroup
	wg.Add(times)
	var numOfErr int32
	var lastError error
	for i := 0; i < times; i++ {
		go func() {
			newExecution := gen.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowExecution.GetWorkflowId()),
				RunId:      common.StringPtr(uuid.New()),
			}

			state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
			s.NoError(err1)
			info0 := state0.ExecutionInfo
			continueAsNewInfo := copyWorkflowExecutionInfo(info0)
			continueAsNewInfo.State = p.WorkflowStateRunning
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
	domainID := "2d7994bf-9de8-459d-9c81-e723daedb246"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("start-workflow-test"),
		RunId:      common.StringPtr("7f9fe8a0-9237-11e6-ae22-56b6b6499611"),
	}
	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "queue1", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	task1, err1 := s.CreateWorkflowExecution(domainID, workflowExecution, "queue1", "wType1", 20, 14, nil, 3, 0, 2, nil)
	s.Error(err1, "Expected workflow creation to fail.")
	log.Infof("Unable to start workflow execution: %v", err1)
	startedErr, ok := err1.(*p.WorkflowExecutionAlreadyStartedError)
	s.True(ok, fmt.Sprintf("Expected WorkflowExecutionAlreadyStartedError, but actual is %v", err1))
	s.Equal(workflowExecution.GetRunId(), startedErr.RunID, startedErr.Msg)

	s.Equal(p.WorkflowStateRunning, startedErr.State, startedErr.Msg)
	s.Equal(p.WorkflowCloseStatusNone, startedErr.CloseStatus, startedErr.Msg)
	s.Equal(common.EmptyVersion, startedErr.LastWriteVersion, startedErr.Msg)
	s.Empty(task1, "Expected empty task identifier.")

	response, err2 := s.ExecutionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &p.WorkflowExecutionInfo{
				CreateRequestID:      uuid.New(),
				DomainID:             domainID,
				WorkflowID:           workflowExecution.GetWorkflowId(),
				RunID:                workflowExecution.GetRunId(),
				TaskList:             "queue1",
				WorkflowTypeName:     "workflow_type_test",
				WorkflowTimeout:      20,
				DecisionTimeoutValue: 13,
				ExecutionContext:     nil,
				State:                p.WorkflowStateRunning,
				CloseStatus:          p.WorkflowCloseStatusNone,
				LastFirstEventID:     common.FirstEventID,
				NextEventID:          int64(3),
				LastProcessedEvent:   0,
				DecisionScheduleID:   int64(2),
				DecisionStartedID:    common.EmptyEventID,
				DecisionTimeout:      1,
			},
			ExecutionStats: &p.ExecutionStats{},
			TransferTasks: []p.Task{
				&p.DecisionTask{
					TaskID:     s.GetNextSequenceNumber(),
					DomainID:   domainID,
					TaskList:   "queue1",
					ScheduleID: int64(2),
				},
			},
			TimerTasks: nil,
		},
		RangeID: s.ShardInfo.RangeID - 1,
	})

	s.Error(err2, "Expected workflow creation to fail.")
	s.Nil(response)
	log.Infof("Unable to start workflow execution: %v", err2)
	s.IsType(&p.ShardOwnershipLostError{}, err2)
}

// TestPersistenceStartWorkflowWithReplicationState test
func (s *ExecutionManagerSuite) TestPersistenceStartWorkflowWithReplicationState() {
	domainID := "2d7994bf-9de8-459d-9c81-e723daedb246"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("start-workflow-test-replication-state"),
		RunId:      common.StringPtr("7f9fe8a0-9237-11e6-ae22-56b6b6499611"),
	}
	startVersion := int64(144)
	lastWriteVersion := int64(1444)
	replicationState := &p.ReplicationState{
		StartVersion:     startVersion, // we are only testing this attribute
		CurrentVersion:   lastWriteVersion,
		LastWriteVersion: lastWriteVersion,
	}
	task0, err0 := s.CreateWorkflowExecutionWithReplication(domainID, workflowExecution, "queue1", "wType", 20, 13, 3, 0, 2, replicationState, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	task1, err1 := s.CreateWorkflowExecution(domainID, workflowExecution, "queue1", "wType1", 20, 14, nil, 3, 0, 2, nil)
	s.Error(err1, "Expected workflow creation to fail.")
	log.Infof("Unable to start workflow execution: %v", err1)
	startedErr, ok := err1.(*p.WorkflowExecutionAlreadyStartedError)
	s.True(ok)
	s.Equal(workflowExecution.GetRunId(), startedErr.RunID, startedErr.Msg)
	s.Equal(p.WorkflowStateRunning, startedErr.State, startedErr.Msg)
	s.Equal(p.WorkflowCloseStatusNone, startedErr.CloseStatus, startedErr.Msg)
	s.Equal(lastWriteVersion, startedErr.LastWriteVersion, startedErr.Msg)
	s.Empty(task1, "Expected empty task identifier.")

	response, err2 := s.ExecutionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &p.WorkflowExecutionInfo{
				CreateRequestID:      uuid.New(),
				DomainID:             domainID,
				WorkflowID:           workflowExecution.GetWorkflowId(),
				RunID:                workflowExecution.GetRunId(),
				TaskList:             "queue1",
				WorkflowTypeName:     "workflow_type_test",
				WorkflowTimeout:      20,
				DecisionTimeoutValue: 13,
				State:                p.WorkflowStateRunning,
				CloseStatus:          p.WorkflowCloseStatusNone,
				ExecutionContext:     nil,
				LastFirstEventID:     common.FirstEventID,
				NextEventID:          int64(3),
				LastProcessedEvent:   0,
				DecisionScheduleID:   int64(2),
				DecisionStartedID:    common.EmptyEventID,
				DecisionTimeout:      1,
			},
			ExecutionStats: &p.ExecutionStats{},
			TransferTasks: []p.Task{
				&p.DecisionTask{
					TaskID:     s.GetNextSequenceNumber(),
					DomainID:   domainID,
					TaskList:   "queue1",
					ScheduleID: int64(2),
				},
			},
			TimerTasks: nil,
		},
		RangeID: s.ShardInfo.RangeID - 1,
	})

	s.Error(err2, "Expected workflow creation to fail.")
	s.Nil(response)
	log.Infof("Unable to start workflow execution: %v", err2)
	s.IsType(&p.ShardOwnershipLostError{}, err2)
}

// TestGetWorkflow test
func (s *ExecutionManagerSuite) TestGetWorkflow() {
	testResetPoints := gen.ResetPoints{
		Points: []*gen.ResetPointInfo{
			&gen.ResetPointInfo{
				BinaryChecksum:           common.StringPtr("test-binary-checksum"),
				RunId:                    common.StringPtr("test-runID"),
				FirstDecisionCompletedId: common.Int64Ptr(123),
				CreatedTimeNano:          common.Int64Ptr(456),
				Resettable:               common.BoolPtr(true),
				ExpiringTimeNano:         common.Int64Ptr(789),
			},
		},
	}
	testSearchAttrKey := "env"
	testSearchAttrVal, _ := json.Marshal("test")
	testSearchAttr := map[string][]byte{
		testSearchAttrKey: testSearchAttrVal,
	}

	testMemoKey := "memoKey"
	testMemoVal, _ := json.Marshal("memoVal")
	testMemo := map[string][]byte{
		testMemoKey: testMemoVal,
	}

	createReq := &p.CreateWorkflowExecutionRequest{
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &p.WorkflowExecutionInfo{
				CreateRequestID:      uuid.New(),
				DomainID:             uuid.New(),
				WorkflowID:           "get-workflow-test",
				RunID:                uuid.New(),
				ParentDomainID:       uuid.New(),
				ParentWorkflowID:     "get-workflow-test-parent",
				ParentRunID:          uuid.New(),
				InitiatedID:          rand.Int63(),
				TaskList:             "get-wf-test-tasklist",
				WorkflowTypeName:     "code.uber.internal/test/workflow",
				WorkflowTimeout:      rand.Int31(),
				DecisionTimeoutValue: rand.Int31(),
				ExecutionContext:     []byte("test-execution-context"),
				State:                p.WorkflowStateRunning,
				CloseStatus:          p.WorkflowCloseStatusNone,
				LastFirstEventID:     common.FirstEventID,
				NextEventID:          rand.Int63(),
				LastProcessedEvent:   int64(rand.Int31()),
				SignalCount:          rand.Int31(),
				DecisionVersion:      int64(rand.Int31()),
				DecisionScheduleID:   int64(rand.Int31()),
				DecisionStartedID:    int64(rand.Int31()),
				DecisionTimeout:      rand.Int31(),
				Attempt:              rand.Int31(),
				HasRetryPolicy:       true,
				InitialInterval:      rand.Int31(),
				BackoffCoefficient:   7.78,
				MaximumInterval:      rand.Int31(),
				ExpirationTime:       time.Now(),
				MaximumAttempts:      rand.Int31(),
				NonRetriableErrors:   []string{"badRequestError", "accessDeniedError"},
				CronSchedule:         "* * * * *",
				ExpirationSeconds:    rand.Int31(),
				AutoResetPoints:      &testResetPoints,
				SearchAttributes:     testSearchAttr,
				Memo:                 testMemo,
			},
			ExecutionStats: &p.ExecutionStats{
				HistorySize: int64(rand.Int31()),
			},
			ReplicationState: &p.ReplicationState{
				CurrentVersion:   int64(rand.Int31()),
				StartVersion:     int64(rand.Int31()),
				LastWriteVersion: int64(rand.Int31()),
				LastWriteEventID: int64(rand.Int31()),
				LastReplicationInfo: map[string]*p.ReplicationInfo{
					"r2": &p.ReplicationInfo{Version: math.MaxInt32, LastEventID: math.MaxInt32},
				},
			},
		},
		Mode: p.CreateWorkflowModeBrandNew,
	}

	createResp, err := s.ExecutionManager.CreateWorkflowExecution(createReq)
	s.NoError(err)
	s.NotNil(createResp, "Expected non empty task identifier.")

	state, err := s.GetWorkflowExecutionInfo(createReq.NewWorkflowSnapshot.ExecutionInfo.DomainID,
		gen.WorkflowExecution{
			WorkflowId: common.StringPtr(createReq.NewWorkflowSnapshot.ExecutionInfo.WorkflowID),
			RunId:      common.StringPtr(createReq.NewWorkflowSnapshot.ExecutionInfo.RunID),
		})
	s.NoError(err)
	info := state.ExecutionInfo
	s.NotNil(info, "Valid Workflow response expected.")
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.CreateRequestID, info.CreateRequestID)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.DomainID, info.DomainID)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.WorkflowID, info.WorkflowID)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.RunID, info.RunID)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.ParentDomainID, info.ParentDomainID)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.ParentWorkflowID, info.ParentWorkflowID)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.ParentRunID, info.ParentRunID)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.InitiatedID, info.InitiatedID)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.TaskList, info.TaskList)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.WorkflowTypeName, info.WorkflowTypeName)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.WorkflowTimeout, info.WorkflowTimeout)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.DecisionTimeoutValue, info.DecisionTimeoutValue)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.ExecutionContext, info.ExecutionContext)
	s.Equal(p.WorkflowStateRunning, info.State)
	s.Equal(p.WorkflowCloseStatusNone, info.CloseStatus)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.NextEventID, info.NextEventID)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.LastProcessedEvent, info.LastProcessedEvent)
	s.Equal(true, s.validateTimeRange(info.LastUpdatedTimestamp, time.Hour))
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.DecisionVersion, info.DecisionVersion)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.DecisionScheduleID, info.DecisionScheduleID)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.DecisionStartedID, info.DecisionStartedID)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.DecisionTimeout, info.DecisionTimeout)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.SignalCount, info.SignalCount)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.Attempt, info.Attempt)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.HasRetryPolicy, info.HasRetryPolicy)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.InitialInterval, info.InitialInterval)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.BackoffCoefficient, info.BackoffCoefficient)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.MaximumAttempts, info.MaximumAttempts)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.MaximumInterval, info.MaximumInterval)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.ExpirationSeconds, info.ExpirationSeconds)
	s.EqualTimes(createReq.NewWorkflowSnapshot.ExecutionInfo.ExpirationTime, info.ExpirationTime)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.CronSchedule, info.CronSchedule)
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionInfo.NonRetriableErrors, info.NonRetriableErrors)
	s.Equal(testResetPoints.String(), info.AutoResetPoints.String())
	s.Equal(createReq.NewWorkflowSnapshot.ExecutionStats.HistorySize, state.ExecutionStats.HistorySize)
	val, ok := info.SearchAttributes[testSearchAttrKey]
	s.True(ok)
	s.Equal(testSearchAttrVal, val)
	val, ok = info.Memo[testMemoKey]
	s.True(ok)
	s.Equal(testMemoVal, val)

	s.Equal(createReq.NewWorkflowSnapshot.ReplicationState.LastWriteEventID, state.ReplicationState.LastWriteEventID)
	s.Equal(createReq.NewWorkflowSnapshot.ReplicationState.LastWriteVersion, state.ReplicationState.LastWriteVersion)
	s.Equal(createReq.NewWorkflowSnapshot.ReplicationState.StartVersion, state.ReplicationState.StartVersion)
	s.Equal(createReq.NewWorkflowSnapshot.ReplicationState.CurrentVersion, state.ReplicationState.CurrentVersion)
	s.NotNil(state.ReplicationState.LastReplicationInfo)
	for k, v := range createReq.NewWorkflowSnapshot.ReplicationState.LastReplicationInfo {
		v1, ok := state.ReplicationState.LastReplicationInfo[k]
		s.True(ok)
		s.Equal(v.Version, v1.Version)
		s.Equal(v.LastEventID, v1.LastEventID)
	}
}

// TestUpdateWorkflow test
func (s *ExecutionManagerSuite) TestUpdateWorkflow() {
	domainID := "b0a8571c-0257-40ea-afcd-3a14eae181c0"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("update-workflow-test"),
		RunId:      common.StringPtr("5ba5e531-e46b-48d9-b4b3-859919839553"),
	}
	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "queue1", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")
	s.Equal(domainID, info0.DomainID)
	s.Equal("update-workflow-test", info0.WorkflowID)
	s.Equal("5ba5e531-e46b-48d9-b4b3-859919839553", info0.RunID)
	s.Equal("queue1", info0.TaskList)
	s.Equal("wType", info0.WorkflowTypeName)
	s.Equal(int32(20), info0.WorkflowTimeout)
	s.Equal(int32(13), info0.DecisionTimeoutValue)
	s.Equal([]byte(nil), info0.ExecutionContext)
	s.Equal(p.WorkflowStateRunning, info0.State)
	s.Equal(p.WorkflowCloseStatusNone, info0.CloseStatus)
	s.Equal(int64(1), info0.LastFirstEventID)
	s.Equal(int64(3), info0.NextEventID)
	s.Equal(int64(0), info0.LastProcessedEvent)
	s.Equal(true, s.validateTimeRange(info0.LastUpdatedTimestamp, time.Hour))
	s.Equal(int64(0), info0.DecisionVersion)
	s.Equal(int64(2), info0.DecisionScheduleID)
	s.Equal(common.EmptyEventID, info0.DecisionStartedID)
	s.Equal(int32(1), info0.DecisionTimeout)
	s.Equal(int64(0), info0.DecisionAttempt)
	s.Equal(int64(0), info0.DecisionStartedTimestamp)
	s.Equal(int64(0), info0.DecisionScheduledTimestamp)
	s.Equal(int64(0), info0.DecisionOriginalScheduledTimestamp)
	s.Empty(info0.StickyTaskList)
	s.Equal(int32(0), info0.StickyScheduleToStartTimeout)
	s.Empty(info0.ClientLibraryVersion)
	s.Empty(info0.ClientFeatureVersion)
	s.Empty(info0.ClientImpl)
	s.Equal(int32(0), info0.SignalCount)
	s.True(info0.AutoResetPoints.Equals(&gen.ResetPoints{}))
	s.True(len(info0.SearchAttributes) == 0)
	s.True(len(info0.Memo) == 0)

	log.Infof("Workflow execution last updated: %v", info0.LastUpdatedTimestamp)

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedStats := state0.ExecutionStats
	updatedInfo.LastFirstEventID = int64(3)
	updatedInfo.NextEventID = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)
	updatedInfo.DecisionVersion = int64(666)
	updatedInfo.DecisionAttempt = int64(123)
	updatedInfo.DecisionStartedTimestamp = int64(321)
	updatedInfo.DecisionScheduledTimestamp = int64(654)
	updatedInfo.DecisionOriginalScheduledTimestamp = int64(655)
	updatedInfo.StickyTaskList = "random sticky tasklist"
	updatedInfo.StickyScheduleToStartTimeout = 876
	updatedInfo.ClientLibraryVersion = "random client library version"
	updatedInfo.ClientFeatureVersion = "random client feature version"
	updatedInfo.ClientImpl = "random client impl"
	updatedInfo.SignalCount = 9
	updatedInfo.InitialInterval = math.MaxInt32
	updatedInfo.BackoffCoefficient = 4.45
	updatedInfo.MaximumInterval = math.MaxInt32
	updatedInfo.MaximumAttempts = math.MaxInt32
	updatedInfo.ExpirationSeconds = math.MaxInt32
	updatedInfo.ExpirationTime = time.Now()
	updatedInfo.NonRetriableErrors = []string{"accessDenied", "badRequest"}
	searchAttrKey := "env"
	searchAttrVal := []byte("test")
	updatedInfo.SearchAttributes = map[string][]byte{searchAttrKey: searchAttrVal}
	memoKey := "memoKey"
	memoVal := []byte("memoVal")
	updatedInfo.Memo = map[string][]byte{memoKey: memoVal}
	updatedStats.HistorySize = math.MaxInt64

	err2 := s.UpdateWorkflowExecution(updatedInfo, updatedStats, nil, []int64{int64(4)}, nil, int64(3), nil, nil, nil, nil, nil)
	s.NoError(err2)

	state1, err3 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err3)
	info1 := state1.ExecutionInfo
	s.NotNil(info1, "Valid Workflow info expected.")
	s.Equal(domainID, info1.DomainID)
	s.Equal("update-workflow-test", info1.WorkflowID)
	s.Equal("5ba5e531-e46b-48d9-b4b3-859919839553", info1.RunID)
	s.Equal("queue1", info1.TaskList)
	s.Equal("wType", info1.WorkflowTypeName)
	s.Equal(int32(20), info1.WorkflowTimeout)
	s.Equal(int32(13), info1.DecisionTimeoutValue)
	s.Equal([]byte(nil), info1.ExecutionContext)
	s.Equal(p.WorkflowStateRunning, info1.State)
	s.Equal(p.WorkflowCloseStatusNone, info1.CloseStatus)
	s.Equal(int64(3), info1.LastFirstEventID)
	s.Equal(int64(5), info1.NextEventID)
	s.Equal(int64(2), info1.LastProcessedEvent)
	s.Equal(true, s.validateTimeRange(info1.LastUpdatedTimestamp, time.Hour))
	s.Equal(int64(666), info1.DecisionVersion)
	s.Equal(int64(2), info1.DecisionScheduleID)
	s.Equal(common.EmptyEventID, info1.DecisionStartedID)
	s.Equal(int32(1), info1.DecisionTimeout)
	s.Equal(int64(123), info1.DecisionAttempt)
	s.Equal(int64(321), info1.DecisionStartedTimestamp)
	s.Equal(int64(654), info1.DecisionScheduledTimestamp)
	s.Equal(int64(655), info1.DecisionOriginalScheduledTimestamp)
	s.Equal(updatedInfo.StickyTaskList, info1.StickyTaskList)
	s.Equal(updatedInfo.StickyScheduleToStartTimeout, info1.StickyScheduleToStartTimeout)
	s.Equal(updatedInfo.ClientLibraryVersion, info1.ClientLibraryVersion)
	s.Equal(updatedInfo.ClientFeatureVersion, info1.ClientFeatureVersion)
	s.Equal(updatedInfo.ClientImpl, info1.ClientImpl)
	s.Equal(updatedInfo.SignalCount, info1.SignalCount)
	s.EqualValues(updatedStats.HistorySize, state1.ExecutionStats.HistorySize)
	s.Equal(updatedInfo.InitialInterval, info1.InitialInterval)
	s.Equal(updatedInfo.BackoffCoefficient, info1.BackoffCoefficient)
	s.Equal(updatedInfo.MaximumInterval, info1.MaximumInterval)
	s.Equal(updatedInfo.MaximumAttempts, info1.MaximumAttempts)
	s.Equal(updatedInfo.ExpirationSeconds, info1.ExpirationSeconds)
	s.EqualTimes(updatedInfo.ExpirationTime, info1.ExpirationTime)
	s.Equal(updatedInfo.NonRetriableErrors, info1.NonRetriableErrors)
	searchAttrVal1, ok := info1.SearchAttributes[searchAttrKey]
	s.True(ok)
	s.Equal(searchAttrVal, searchAttrVal1)
	memoVal1, ok := info1.Memo[memoKey]
	s.True(ok)
	s.Equal(memoVal, memoVal1)

	log.Infof("Workflow execution last updated: %v", info1.LastUpdatedTimestamp)

	failedUpdateInfo := copyWorkflowExecutionInfo(updatedInfo)
	failedUpdateStats := copyExecutionStats(updatedStats)
	err4 := s.UpdateWorkflowExecution(failedUpdateInfo, failedUpdateStats, nil, []int64{int64(5)}, nil, int64(3), nil, nil, nil, nil, nil)
	s.Error(err4, "expected non nil error.")
	s.IsType(&p.ConditionFailedError{}, err4)
	log.Errorf("Conditional update failed with error: %v", err4)

	state2, err4 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err4)
	info2 := state2.ExecutionInfo
	s.NotNil(info2, "Valid Workflow info expected.")
	s.Equal(domainID, info2.DomainID)
	s.Equal("update-workflow-test", info2.WorkflowID)
	s.Equal("5ba5e531-e46b-48d9-b4b3-859919839553", info2.RunID)
	s.Equal("queue1", info2.TaskList)
	s.Equal("wType", info2.WorkflowTypeName)
	s.Equal(int32(20), info2.WorkflowTimeout)
	s.Equal(int32(13), info2.DecisionTimeoutValue)
	s.Equal([]byte(nil), info2.ExecutionContext)
	s.Equal(p.WorkflowStateRunning, info2.State)
	s.Equal(p.WorkflowCloseStatusNone, info2.CloseStatus)
	s.Equal(int64(5), info2.NextEventID)
	s.Equal(int64(2), info2.LastProcessedEvent)
	s.Equal(true, s.validateTimeRange(info2.LastUpdatedTimestamp, time.Hour))
	s.Equal(int64(666), info2.DecisionVersion)
	s.Equal(int64(2), info2.DecisionScheduleID)
	s.Equal(common.EmptyEventID, info2.DecisionStartedID)
	s.Equal(int32(1), info2.DecisionTimeout)
	s.Equal(int64(123), info2.DecisionAttempt)
	s.Equal(int64(321), info2.DecisionStartedTimestamp)
	s.Equal(int64(654), info2.DecisionScheduledTimestamp)
	s.Equal(int64(655), info2.DecisionOriginalScheduledTimestamp)
	s.Equal(updatedInfo.SignalCount, info2.SignalCount)
	s.EqualValues(updatedStats.HistorySize, state2.ExecutionStats.HistorySize)
	s.Equal(updatedInfo.InitialInterval, info2.InitialInterval)
	s.Equal(updatedInfo.BackoffCoefficient, info2.BackoffCoefficient)
	s.Equal(updatedInfo.MaximumInterval, info2.MaximumInterval)
	s.Equal(updatedInfo.MaximumAttempts, info2.MaximumAttempts)
	s.Equal(updatedInfo.ExpirationSeconds, info2.ExpirationSeconds)
	s.EqualTimes(updatedInfo.ExpirationTime, info2.ExpirationTime)
	s.Equal(updatedInfo.NonRetriableErrors, info2.NonRetriableErrors)
	searchAttrVal2, ok := info2.SearchAttributes[searchAttrKey]
	s.True(ok)
	s.Equal(searchAttrVal, searchAttrVal2)
	memoVal2, ok := info1.Memo[memoKey]
	s.True(ok)
	s.Equal(memoVal, memoVal2)
	log.Infof("Workflow execution last updated: %v", info2.LastUpdatedTimestamp)

	err5 := s.UpdateWorkflowExecutionWithRangeID(failedUpdateInfo, failedUpdateStats, nil, []int64{int64(5)}, nil, int64(12345), int64(5), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, "")
	s.Error(err5, "expected non nil error.")
	s.IsType(&p.ShardOwnershipLostError{}, err5)
	log.Errorf("Conditional update failed with error: %v", err5)

	state3, err6 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err6)
	info3 := state3.ExecutionInfo
	s.NotNil(info3, "Valid Workflow info expected.")
	s.Equal(domainID, info3.DomainID)
	s.Equal("update-workflow-test", info3.WorkflowID)
	s.Equal("5ba5e531-e46b-48d9-b4b3-859919839553", info3.RunID)
	s.Equal("queue1", info3.TaskList)
	s.Equal("wType", info3.WorkflowTypeName)
	s.Equal(int32(20), info3.WorkflowTimeout)
	s.Equal(int32(13), info3.DecisionTimeoutValue)
	s.Equal([]byte(nil), info3.ExecutionContext)
	s.Equal(p.WorkflowStateRunning, info3.State)
	s.Equal(p.WorkflowCloseStatusNone, info3.CloseStatus)
	s.Equal(int64(5), info3.NextEventID)
	s.Equal(int64(2), info3.LastProcessedEvent)
	s.Equal(true, s.validateTimeRange(info3.LastUpdatedTimestamp, time.Hour))
	s.Equal(int64(666), info3.DecisionVersion)
	s.Equal(int64(2), info3.DecisionScheduleID)
	s.Equal(common.EmptyEventID, info3.DecisionStartedID)
	s.Equal(int32(1), info3.DecisionTimeout)
	s.Equal(int64(123), info3.DecisionAttempt)
	s.Equal(int64(321), info3.DecisionStartedTimestamp)
	s.Equal(int64(654), info3.DecisionScheduledTimestamp)
	s.Equal(int64(655), info3.DecisionOriginalScheduledTimestamp)
	s.Equal(updatedInfo.SignalCount, info3.SignalCount)
	s.EqualValues(updatedStats.HistorySize, state3.ExecutionStats.HistorySize)
	s.Equal(updatedInfo.InitialInterval, info3.InitialInterval)
	s.Equal(updatedInfo.BackoffCoefficient, info3.BackoffCoefficient)
	s.Equal(updatedInfo.MaximumInterval, info3.MaximumInterval)
	s.Equal(updatedInfo.MaximumAttempts, info3.MaximumAttempts)
	s.Equal(updatedInfo.ExpirationSeconds, info3.ExpirationSeconds)
	s.EqualTimes(updatedInfo.ExpirationTime, info3.ExpirationTime)
	s.Equal(updatedInfo.NonRetriableErrors, info3.NonRetriableErrors)
	searchAttrVal3, ok := info3.SearchAttributes[searchAttrKey]
	s.True(ok)
	s.Equal(searchAttrVal, searchAttrVal3)
	memoVal3, ok := info1.Memo[memoKey]
	s.True(ok)
	s.Equal(memoVal, memoVal3)

	log.Infof("Workflow execution last updated: %v", info3.LastUpdatedTimestamp)

	//update with incorrect rangeID and condition(next_event_id)
	err7 := s.UpdateWorkflowExecutionWithRangeID(failedUpdateInfo, failedUpdateStats, nil, []int64{int64(5)}, nil, int64(12345), int64(3), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, "")
	s.Error(err7, "expected non nil error.")
	s.IsType(&p.ShardOwnershipLostError{}, err7)
	log.Errorf("Conditional update failed with error: %v", err7)

	state4, err8 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err8)
	info4 := state4.ExecutionInfo
	s.NotNil(info4, "Valid Workflow info expected.")
	s.Equal(domainID, info4.DomainID)
	s.Equal("update-workflow-test", info4.WorkflowID)
	s.Equal("5ba5e531-e46b-48d9-b4b3-859919839553", info4.RunID)
	s.Equal("queue1", info4.TaskList)
	s.Equal("wType", info4.WorkflowTypeName)
	s.Equal(int32(20), info4.WorkflowTimeout)
	s.Equal(int32(13), info4.DecisionTimeoutValue)
	s.Equal([]byte(nil), info4.ExecutionContext)
	s.Equal(p.WorkflowStateRunning, info4.State)
	s.Equal(p.WorkflowCloseStatusNone, info4.CloseStatus)
	s.Equal(int64(5), info4.NextEventID)
	s.Equal(int64(2), info4.LastProcessedEvent)
	s.Equal(true, s.validateTimeRange(info4.LastUpdatedTimestamp, time.Hour))
	s.Equal(int64(666), info4.DecisionVersion)
	s.Equal(int64(2), info4.DecisionScheduleID)
	s.Equal(common.EmptyEventID, info4.DecisionStartedID)
	s.Equal(int32(1), info4.DecisionTimeout)
	s.Equal(int64(123), info4.DecisionAttempt)
	s.Equal(int64(321), info4.DecisionStartedTimestamp)
	s.Equal(updatedInfo.SignalCount, info4.SignalCount)
	s.EqualValues(updatedStats.HistorySize, state4.ExecutionStats.HistorySize)
	s.Equal(updatedInfo.InitialInterval, info4.InitialInterval)
	s.Equal(updatedInfo.BackoffCoefficient, info4.BackoffCoefficient)
	s.Equal(updatedInfo.MaximumInterval, info4.MaximumInterval)
	s.Equal(updatedInfo.MaximumAttempts, info4.MaximumAttempts)
	s.Equal(updatedInfo.ExpirationSeconds, info4.ExpirationSeconds)
	s.EqualTimes(updatedInfo.ExpirationTime, info4.ExpirationTime)
	s.Equal(updatedInfo.NonRetriableErrors, info4.NonRetriableErrors)
	searchAttrVal4, ok := info4.SearchAttributes[searchAttrKey]
	s.True(ok)
	s.Equal(searchAttrVal, searchAttrVal4)
	memoVal4, ok := info1.Memo[memoKey]
	s.True(ok)
	s.Equal(memoVal, memoVal4)

	log.Infof("Workflow execution last updated: %v", info4.LastUpdatedTimestamp)
}

// TestDeleteWorkflow test
func (s *ExecutionManagerSuite) TestDeleteWorkflow() {
	domainID := "1d4abb23-b87b-457b-96ef-43aba0b9c44f"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("delete-workflow-test"),
		RunId:      common.StringPtr("4e0917f2-9361-4a14-b16f-1fafe09b287a"),
	}
	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "queue1", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")
	s.Equal(domainID, info0.DomainID)
	s.Equal("delete-workflow-test", info0.WorkflowID)
	s.Equal("4e0917f2-9361-4a14-b16f-1fafe09b287a", info0.RunID)
	s.Equal("queue1", info0.TaskList)
	s.Equal("wType", info0.WorkflowTypeName)
	s.Equal(int32(20), info0.WorkflowTimeout)
	s.Equal(int32(13), info0.DecisionTimeoutValue)
	s.Equal([]byte(nil), info0.ExecutionContext)
	s.Equal(p.WorkflowStateRunning, info0.State)
	s.Equal(p.WorkflowCloseStatusNone, info0.CloseStatus)
	s.Equal(int64(3), info0.NextEventID)
	s.Equal(int64(0), info0.LastProcessedEvent)
	s.Equal(true, s.validateTimeRange(info0.LastUpdatedTimestamp, time.Hour))
	s.Equal(int64(2), info0.DecisionScheduleID)
	s.Equal(common.EmptyEventID, info0.DecisionStartedID)
	s.Equal(int32(1), info0.DecisionTimeout)

	log.Infof("Workflow execution last updated: %v", info0.LastUpdatedTimestamp)

	err4 := s.DeleteWorkflowExecution(info0)
	s.NoError(err4)

	_, err3 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Error(err3, "expected non nil error.")
	s.IsType(&gen.EntityNotExistsError{}, err3)

	err5 := s.DeleteWorkflowExecution(info0)
	s.NoError(err5)
}

// TestDeleteCurrentWorkflow test
func (s *ExecutionManagerSuite) TestDeleteCurrentWorkflow() {
	if s.ExecutionManager.GetName() != "cassandra" {
		s.T().Skip("this test is only applicable for cassandra (uses TTL based deletes)")
	}
	domainID := "54d15308-e20e-4b91-a00f-a518a3892790"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("delete-current-workflow-test"),
		RunId:      common.StringPtr("6cae4054-6ba7-46d3-8755-e3c2db6f74ea"),
	}

	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "queue1", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	runID0, err1 := s.GetCurrentWorkflowRunID(domainID, workflowExecution.GetWorkflowId())
	s.NoError(err1)
	s.Equal(workflowExecution.GetRunId(), runID0)

	info0, err2 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err2)

	updatedInfo1 := copyWorkflowExecutionInfo(info0.ExecutionInfo)
	updatedStats1 := copyExecutionStats(info0.ExecutionStats)
	updatedInfo1.NextEventID = int64(6)
	updatedInfo1.LastProcessedEvent = int64(2)
	err3 := s.UpdateWorkflowExecutionAndFinish(updatedInfo1, updatedStats1, int64(3))
	s.NoError(err3)

	runID4, err4 := s.GetCurrentWorkflowRunID(domainID, workflowExecution.GetWorkflowId())
	s.NoError(err4)
	s.Equal(workflowExecution.GetRunId(), runID4)

	fakeInfo := &p.WorkflowExecutionInfo{
		DomainID:   info0.ExecutionInfo.DomainID,
		WorkflowID: info0.ExecutionInfo.WorkflowID,
		RunID:      uuid.New(),
	}

	// test wrong run id with conditional delete
	s.DeleteCurrentWorkflowExecution(fakeInfo)

	runID5, err5 := s.GetCurrentWorkflowRunID(domainID, workflowExecution.GetWorkflowId())
	s.NoError(err5)
	s.Equal(workflowExecution.GetRunId(), runID5)

	// simulate a timer_task deleting execution after retention
	s.DeleteCurrentWorkflowExecution(info0.ExecutionInfo)

	runID0, err1 = s.GetCurrentWorkflowRunID(domainID, workflowExecution.GetWorkflowId())
	s.Error(err1)
	s.Empty(runID0)
	_, ok := err1.(*gen.EntityNotExistsError)
	s.True(ok)

	// execution record should still be there
	info0, err2 = s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err2)
}

// TestUpdateDeleteWorkflow mocks the timer behavoir to clean up workflow.
func (s *ExecutionManagerSuite) TestUpdateDeleteWorkflow() {
	finishedCurrentExecutionRetentionTTL := int32(2)
	domainID := "54d15308-e20e-4b91-a00f-a518a3892790"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("update-delete-workflow-test"),
		RunId:      common.StringPtr("6cae4054-6ba7-46d3-8755-e3c2db6f74ea"),
	}

	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "queue1", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	runID0, err1 := s.GetCurrentWorkflowRunID(domainID, workflowExecution.GetWorkflowId())
	s.NoError(err1)
	s.Equal(workflowExecution.GetRunId(), runID0)

	info0, err2 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err2)

	updatedInfo1 := copyWorkflowExecutionInfo(info0.ExecutionInfo)
	updatedStats1 := copyExecutionStats(info0.ExecutionStats)
	updatedInfo1.NextEventID = int64(6)
	updatedInfo1.LastProcessedEvent = int64(2)
	err3 := s.UpdateWorkflowExecutionAndFinish(updatedInfo1, updatedStats1, int64(3))
	s.NoError(err3)

	runID4, err4 := s.GetCurrentWorkflowRunID(domainID, workflowExecution.GetWorkflowId())
	s.NoError(err4)
	s.Equal(workflowExecution.GetRunId(), runID4)

	// simulate a timer_task deleting execution after retention
	err5 := s.DeleteCurrentWorkflowExecution(info0.ExecutionInfo)
	s.NoError(err5)
	err6 := s.DeleteWorkflowExecution(info0.ExecutionInfo)
	s.NoError(err6)

	time.Sleep(time.Duration(finishedCurrentExecutionRetentionTTL*2) * time.Second)

	runID0, err1 = s.GetCurrentWorkflowRunID(domainID, workflowExecution.GetWorkflowId())
	s.Error(err1)
	s.Empty(runID0)
	_, ok := err1.(*gen.EntityNotExistsError)
	s.True(ok)

	// execution record should still be there
	info0, err2 = s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Error(err2)
	_, ok = err2.(*gen.EntityNotExistsError)
	s.True(ok)
}

// TestGetCurrentWorkflow test
func (s *ExecutionManagerSuite) TestGetCurrentWorkflow() {
	domainID := "54d15308-e20e-4b91-a00f-a518a3892790"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("get-current-workflow-test"),
		RunId:      common.StringPtr("6cae4054-6ba7-46d3-8755-e3c2db6f74ea"),
	}

	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "queue1", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	response, err := s.ExecutionManager.GetCurrentExecution(&p.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowExecution.GetWorkflowId(),
	})
	s.NoError(err)
	s.Equal(workflowExecution.GetRunId(), response.RunID)
	s.Equal(common.EmptyVersion, response.LastWriteVersion)

	info0, err2 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err2)

	updatedInfo1 := copyWorkflowExecutionInfo(info0.ExecutionInfo)
	updatedStats1 := copyExecutionStats(info0.ExecutionStats)
	updatedInfo1.NextEventID = int64(6)
	updatedInfo1.LastProcessedEvent = int64(2)
	err3 := s.UpdateWorkflowExecutionAndFinish(updatedInfo1, updatedStats1, int64(3))
	s.NoError(err3)

	runID4, err4 := s.GetCurrentWorkflowRunID(domainID, workflowExecution.GetWorkflowId())
	s.NoError(err4)
	s.Equal(workflowExecution.GetRunId(), runID4)

	workflowExecution2 := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("get-current-workflow-test"),
		RunId:      common.StringPtr("c3ff4bc6-de18-4643-83b2-037a33f45322"),
	}

	task1, err5 := s.CreateWorkflowExecution(domainID, workflowExecution2, "queue1", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.Error(err5, "Error expected.")
	s.Empty(task1, "Expected empty task identifier.")
}

// TestTransferTasksThroughUpdate test
func (s *ExecutionManagerSuite) TestTransferTasksThroughUpdate() {
	domainID := "b785a8ba-bd7d-4760-bb05-41b115f3e10a"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("get-transfer-tasks-through-update-test"),
		RunId:      common.StringPtr("30a9fa1f-0db1-4d7a-8c34-aa82c5dad3aa"),
	}

	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "queue1", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	tasks1, err1 := s.GetTransferTasks(1, false)
	s.NoError(err1)
	s.NotNil(tasks1, "expected valid list of tasks.")
	s.Equal(1, len(tasks1), "Expected 1 decision task.")
	task1 := tasks1[0]
	s.Equal(domainID, task1.DomainID)
	s.Equal(workflowExecution.GetWorkflowId(), task1.WorkflowID)
	s.Equal(workflowExecution.GetRunId(), task1.RunID)
	s.Equal("queue1", task1.TaskList)
	s.Equal(p.TransferTaskTypeDecisionTask, task1.TaskType)
	s.Equal(int64(2), task1.ScheduleID)
	s.Equal("", task1.TargetRunID)

	err3 := s.CompleteTransferTask(task1.TaskID)
	s.NoError(err3)

	state0, err11 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
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
	s.Equal(1, len(tasks2), "Expected 1 decision task.")
	task2 := tasks2[0]
	s.Equal(domainID, task2.DomainID)
	s.Equal(workflowExecution.GetWorkflowId(), task2.WorkflowID)
	s.Equal(workflowExecution.GetRunId(), task2.RunID)
	s.Equal("queue1", task2.TaskList)
	s.Equal(p.TransferTaskTypeActivityTask, task2.TaskType)
	s.Equal(int64(4), task2.ScheduleID)
	s.Equal("", task2.TargetRunID)

	err4 := s.CompleteTransferTask(task2.TaskID)
	s.NoError(err4)

	state1, _ := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	info1 := state1.ExecutionInfo
	updatedInfo1 := copyWorkflowExecutionInfo(info1)
	updatedStats1 := copyExecutionStats(state1.ExecutionStats)
	updatedInfo1.NextEventID = int64(6)
	updatedInfo1.LastProcessedEvent = int64(2)
	err5 := s.UpdateWorkflowExecutionAndFinish(updatedInfo1, updatedStats1, int64(5))
	s.NoError(err5)

	newExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowExecution.GetWorkflowId()),
		RunId:      common.StringPtr("2a038c8f-b575-4151-8d2c-d443e999ab5a"),
	}
	runID6, err6 := s.GetCurrentWorkflowRunID(domainID, newExecution.GetWorkflowId())
	s.NoError(err6)
	s.Equal(workflowExecution.GetRunId(), runID6)

	tasks3, err7 := s.GetTransferTasks(1, false)
	s.NoError(err7)
	s.NotNil(tasks3, "expected valid list of tasks.")
	s.Equal(1, len(tasks3), "Expected 1 decision task.")
	task3 := tasks3[0]
	s.Equal(domainID, task3.DomainID)
	s.Equal(workflowExecution.GetWorkflowId(), task3.WorkflowID)
	s.Equal(workflowExecution.GetRunId(), task3.RunID)
	s.Equal(p.TransferTaskTypeCloseExecution, task3.TaskType)
	s.Equal("", task3.TargetRunID)

	err8 := s.CompleteTransferTask(task3.TaskID)
	s.NoError(err8)

	_, err9 := s.CreateWorkflowExecution(domainID, newExecution, "queue1", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.Error(err9, "createWFExecution (brand_new) must fail when there is a previous instance of workflow state already in DB")

	err10 := s.DeleteWorkflowExecution(info1)
	s.NoError(err10)
}

// TestCancelTransferTaskTasks test
func (s *ExecutionManagerSuite) TestCancelTransferTaskTasks() {
	domainID := "aeac8287-527b-4b35-80a9-667cb47e7c6d"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("cancel-workflow-test"),
		RunId:      common.StringPtr("db20f7e2-1a1e-40d9-9278-d8b886738e05"),
	}

	task0, err := s.CreateWorkflowExecution(domainID, workflowExecution, "queue1", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.NoError(err)
	s.NotNil(task0, "Expected non empty task identifier.")

	taskD, err := s.GetTransferTasks(1, false)
	s.Equal(1, len(taskD), "Expected 1 decision task.")
	err = s.CompleteTransferTask(taskD[0].TaskID)
	s.NoError(err)

	state1, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err)
	info1 := state1.ExecutionInfo
	s.NotNil(info1, "Valid Workflow info expected.")
	updatedInfo1 := copyWorkflowExecutionInfo(info1)
	updatedStats1 := copyExecutionStats(state1.ExecutionStats)

	targetDomainID := "f2bfaab6-7e8b-4fac-9a62-17da8d37becb"
	targetWorkflowID := "target-workflow-cancellation-id-1"
	targetRunID := "0d00698f-08e1-4d36-a3e2-3bf109f5d2d6"
	targetChildWorkflowOnly := false
	transferTasks := []p.Task{&p.CancelExecutionTask{
		TaskID:                  s.GetNextSequenceNumber(),
		TargetDomainID:          targetDomainID,
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
	s.Equal(p.TransferTaskTypeCancelExecution, task1.TaskType)
	s.Equal(domainID, task1.DomainID)
	s.Equal(workflowExecution.GetWorkflowId(), task1.WorkflowID)
	s.Equal(workflowExecution.GetRunId(), task1.RunID)
	s.Equal(targetDomainID, task1.TargetDomainID)
	s.Equal(targetWorkflowID, task1.TargetWorkflowID)
	s.Equal(targetRunID, task1.TargetRunID)
	s.Equal(targetChildWorkflowOnly, task1.TargetChildWorkflowOnly)

	err = s.CompleteTransferTask(task1.TaskID)
	s.NoError(err)

	targetDomainID = "f2bfaab6-7e8b-4fac-9a62-17da8d37becb"
	targetWorkflowID = "target-workflow-cancellation-id-2"
	targetRunID = ""
	targetChildWorkflowOnly = true
	transferTasks = []p.Task{&p.CancelExecutionTask{
		TaskID:                  s.GetNextSequenceNumber(),
		TargetDomainID:          targetDomainID,
		TargetWorkflowID:        targetWorkflowID,
		TargetRunID:             targetRunID,
		TargetChildWorkflowOnly: targetChildWorkflowOnly,
		InitiatedID:             3,
	}}

	state2, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
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
	s.Equal(p.TransferTaskTypeCancelExecution, task2.TaskType)
	s.Equal(domainID, task2.DomainID)
	s.Equal(workflowExecution.GetWorkflowId(), task2.WorkflowID)
	s.Equal(workflowExecution.GetRunId(), task2.RunID)
	s.Equal(targetDomainID, task2.TargetDomainID)
	s.Equal(targetWorkflowID, task2.TargetWorkflowID)
	s.Equal(targetRunID, task2.TargetRunID)
	s.Equal(targetChildWorkflowOnly, task2.TargetChildWorkflowOnly)

	err = s.CompleteTransferTask(task2.TaskID)
	s.NoError(err)
}

// TestSignalTransferTaskTasks test
func (s *ExecutionManagerSuite) TestSignalTransferTaskTasks() {
	domainID := "aeac8287-527b-4b35-80a9-667cb47e7c6d"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("signal-workflow-test"),
		RunId:      common.StringPtr("db20f7e2-1a1e-40d9-9278-d8b886738e05"),
	}

	task0, err := s.CreateWorkflowExecution(domainID, workflowExecution, "queue1", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.NoError(err)
	s.NotNil(task0, "Expected non empty task identifier.")

	taskD, err := s.GetTransferTasks(1, false)
	s.Equal(1, len(taskD), "Expected 1 decision task.")
	err = s.CompleteTransferTask(taskD[0].TaskID)
	s.NoError(err)

	state1, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err)
	info1 := state1.ExecutionInfo
	s.NotNil(info1, "Valid Workflow info expected.")
	updatedInfo1 := copyWorkflowExecutionInfo(info1)
	updatedStats1 := copyExecutionStats(state1.ExecutionStats)

	targetDomainID := "f2bfaab6-7e8b-4fac-9a62-17da8d37becb"
	targetWorkflowID := "target-workflow-signal-id-1"
	targetRunID := "0d00698f-08e1-4d36-a3e2-3bf109f5d2d6"
	targetChildWorkflowOnly := false
	transferTasks := []p.Task{&p.SignalExecutionTask{
		TaskID:                  s.GetNextSequenceNumber(),
		TargetDomainID:          targetDomainID,
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
	s.Equal(p.TransferTaskTypeSignalExecution, task1.TaskType)
	s.Equal(domainID, task1.DomainID)
	s.Equal(workflowExecution.GetWorkflowId(), task1.WorkflowID)
	s.Equal(workflowExecution.GetRunId(), task1.RunID)
	s.Equal(targetDomainID, task1.TargetDomainID)
	s.Equal(targetWorkflowID, task1.TargetWorkflowID)
	s.Equal(targetRunID, task1.TargetRunID)
	s.Equal(targetChildWorkflowOnly, task1.TargetChildWorkflowOnly)

	err = s.CompleteTransferTask(task1.TaskID)
	s.NoError(err)

	targetDomainID = "f2bfaab6-7e8b-4fac-9a62-17da8d37becb"
	targetWorkflowID = "target-workflow-signal-id-2"
	targetRunID = ""
	targetChildWorkflowOnly = true
	transferTasks = []p.Task{&p.SignalExecutionTask{
		TaskID:                  s.GetNextSequenceNumber(),
		TargetDomainID:          targetDomainID,
		TargetWorkflowID:        targetWorkflowID,
		TargetRunID:             targetRunID,
		TargetChildWorkflowOnly: targetChildWorkflowOnly,
		InitiatedID:             3,
	}}

	state2, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
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
	s.Equal(p.TransferTaskTypeSignalExecution, task2.TaskType)
	s.Equal(domainID, task2.DomainID)
	s.Equal(workflowExecution.GetWorkflowId(), task2.WorkflowID)
	s.Equal(workflowExecution.GetRunId(), task2.RunID)
	s.Equal(targetDomainID, task2.TargetDomainID)
	s.Equal(targetWorkflowID, task2.TargetWorkflowID)
	s.Equal(targetRunID, task2.TargetRunID)
	s.Equal(targetChildWorkflowOnly, task2.TargetChildWorkflowOnly)

	err = s.CompleteTransferTask(task2.TaskID)
	s.NoError(err)
}

// TestReplicationTasks test
func (s *ExecutionManagerSuite) TestReplicationTasks() {
	domainID := "2466d7de-6602-4ad8-b939-fb8f8c36c711"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("get-replication-tasks-test"),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
	}

	task0, err := s.CreateWorkflowExecution(domainID, workflowExecution, "queue1", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.NoError(err)
	s.NotNil(task0, "Expected non empty task identifier.")
	taskD, err := s.GetTransferTasks(1, false)
	s.Equal(1, len(taskD), "Expected 1 decision task.")
	err = s.CompleteTransferTask(taskD[0].TaskID)
	s.NoError(err)

	state1, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
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
			LastReplicationInfo: map[string]*p.ReplicationInfo{
				"dc1": {
					Version:     int64(3),
					LastEventID: int64(1),
				},
			},
		},
		&p.HistoryReplicationTask{
			TaskID:       s.GetNextSequenceNumber(),
			FirstEventID: int64(1),
			NextEventID:  int64(3),
			Version:      456,
			LastReplicationInfo: map[string]*p.ReplicationInfo{
				"dc1": {
					Version:     int64(3),
					LastEventID: int64(1),
				},
			},
		},
		&p.SyncActivityTask{
			TaskID:      s.GetNextSequenceNumber(),
			Version:     789,
			ScheduledID: 99,
		},
	}
	err = s.UpdateWorklowStateAndReplication(updatedInfo1, updatedStats1, nil, nil, int64(3), replicationTasks)
	s.NoError(err)

	respTasks, err := s.GetReplicationTasks(1, true)
	s.NoError(err)
	s.Equal(len(replicationTasks), len(respTasks))

	for index := range replicationTasks {
		s.Equal(replicationTasks[index].GetTaskID(), respTasks[index].GetTaskID())
		s.Equal(replicationTasks[index].GetType(), respTasks[index].GetTaskType())
		s.Equal(replicationTasks[index].GetVersion(), respTasks[index].GetVersion())
		switch replicationTasks[index].GetType() {
		case p.ReplicationTaskTypeHistory:
			expected := replicationTasks[index].(*p.HistoryReplicationTask)
			s.Equal(expected.FirstEventID, respTasks[index].FirstEventID)
			s.Equal(expected.NextEventID, respTasks[index].NextEventID)
			s.Equal(expected.BranchToken, respTasks[index].BranchToken)
			s.Equal(expected.NewRunBranchToken, respTasks[index].NewRunBranchToken)
			s.Equal(expected.ResetWorkflow, respTasks[index].ResetWorkflow)
			s.Equal(len(expected.LastReplicationInfo), len(respTasks[index].LastReplicationInfo))
			for k, v := range expected.LastReplicationInfo {
				got, ok := respTasks[index].LastReplicationInfo[k]
				s.True(ok, "replication info missing key")
				s.Equal(v.Version, got.Version)
				s.Equal(v.LastEventID, got.LastEventID)
			}
		case p.ReplicationTaskTypeSyncActivity:
			expected := replicationTasks[index].(*p.SyncActivityTask)
			s.Equal(expected.ScheduledID, respTasks[index].ScheduledID)
		}
		err = s.CompleteReplicationTask(respTasks[index].GetTaskID())
		s.NoError(err)
	}
}

// TestTransferTasksComplete test
func (s *ExecutionManagerSuite) TestTransferTasksComplete() {
	domainID := "8bfb47be-5b57-4d55-9109-5fb35e20b1d7"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("get-transfer-tasks-test-complete"),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
	}
	tasklist := "some random tasklist"

	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, tasklist, "wType", 20, 13, nil, 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	tasks1, err1 := s.GetTransferTasks(1, false)
	s.NoError(err1)
	s.NotNil(tasks1, "expected valid list of tasks.")
	s.Equal(1, len(tasks1), "Expected 1 decision task.")
	task1 := tasks1[0]
	s.Equal(domainID, task1.DomainID)
	s.Equal(workflowExecution.GetWorkflowId(), task1.WorkflowID)
	s.Equal(workflowExecution.GetRunId(), task1.RunID)
	s.Equal(tasklist, task1.TaskList)
	s.Equal(p.TransferTaskTypeDecisionTask, task1.TaskType)
	s.Equal(int64(2), task1.ScheduleID)
	s.Equal(p.TransferTaskTransferTargetWorkflowID, task1.TargetWorkflowID)
	s.Equal("", task1.TargetRunID)
	err3 := s.CompleteTransferTask(task1.TaskID)
	s.NoError(err3)

	state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedStats := copyExecutionStats(state0.ExecutionStats)
	updatedInfo.NextEventID = int64(6)
	updatedInfo.LastProcessedEvent = int64(2)
	scheduleID := int64(123)
	targetDomainID := "8bfb47be-5b57-4d66-9109-5fb35e20b1d0"
	targetWorkflowID := "some random target domain ID"
	targetRunID := uuid.New()
	currentTransferID := s.GetTransferReadLevel()
	now := time.Now()
	tasks := []p.Task{
		&p.ActivityTask{now, currentTransferID + 10001, domainID, tasklist, scheduleID, 111},
		&p.DecisionTask{now, currentTransferID + 10002, domainID, tasklist, scheduleID, 222, false},
		&p.CloseExecutionTask{now, currentTransferID + 10003, 333},
		&p.CancelExecutionTask{now, currentTransferID + 10004, targetDomainID, targetWorkflowID, targetRunID, true, scheduleID, 444},
		&p.SignalExecutionTask{now, currentTransferID + 10005, targetDomainID, targetWorkflowID, targetRunID, true, scheduleID, 555},
		&p.StartChildExecutionTask{now, currentTransferID + 10006, targetDomainID, targetWorkflowID, scheduleID, 666},
	}
	err2 := s.UpdateWorklowStateAndReplication(updatedInfo, updatedStats, nil, nil, int64(3), tasks)
	s.NoError(err2)

	txTasks, err1 := s.GetTransferTasks(1, true) // use page size one to force pagination
	s.NoError(err1)
	s.NotNil(txTasks, "expected valid list of tasks.")
	s.Equal(len(tasks), len(txTasks))
	for index := range tasks {
		s.True(timeComparator(tasks[index].GetVisibilityTimestamp(), txTasks[index].VisibilityTimestamp, TimePrecision))
	}
	s.Equal(p.TransferTaskTypeActivityTask, txTasks[0].TaskType)
	s.Equal(p.TransferTaskTypeDecisionTask, txTasks[1].TaskType)
	s.Equal(p.TransferTaskTypeCloseExecution, txTasks[2].TaskType)
	s.Equal(p.TransferTaskTypeCancelExecution, txTasks[3].TaskType)
	s.Equal(p.TransferTaskTypeSignalExecution, txTasks[4].TaskType)
	s.Equal(p.TransferTaskTypeStartChildExecution, txTasks[5].TaskType)
	s.Equal(int64(111), txTasks[0].Version)
	s.Equal(int64(222), txTasks[1].Version)
	s.Equal(int64(333), txTasks[2].Version)
	s.Equal(int64(444), txTasks[3].Version)
	s.Equal(int64(555), txTasks[4].Version)
	s.Equal(int64(666), txTasks[5].Version)

	err2 = s.CompleteTransferTask(txTasks[0].TaskID)
	s.NoError(err2)

	err2 = s.CompleteTransferTask(txTasks[1].TaskID)
	s.NoError(err2)

	err2 = s.CompleteTransferTask(txTasks[2].TaskID)
	s.NoError(err2)

	err2 = s.CompleteTransferTask(txTasks[3].TaskID)
	s.NoError(err2)

	err2 = s.CompleteTransferTask(txTasks[4].TaskID)
	s.NoError(err2)

	err2 = s.CompleteTransferTask(txTasks[5].TaskID)
	s.NoError(err2)

	txTasks, err2 = s.GetTransferTasks(100, false)
	s.NoError(err2)
	s.Empty(txTasks, "expected empty task list.")
}

// TestTransferTasksRangeComplete test
func (s *ExecutionManagerSuite) TestTransferTasksRangeComplete() {
	domainID := "8bfb47be-5b57-4d55-9109-5fb35e20b1d7"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("get-transfer-tasks-test-range-complete"),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
	}
	tasklist := "some random tasklist"

	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, tasklist, "wType", 20, 13, nil, 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	tasks1, err1 := s.GetTransferTasks(1, false)
	s.NoError(err1)
	s.NotNil(tasks1, "expected valid list of tasks.")
	s.Equal(1, len(tasks1), "Expected 1 decision task.")
	task1 := tasks1[0]
	s.Equal(domainID, task1.DomainID)
	s.Equal(workflowExecution.GetWorkflowId(), task1.WorkflowID)
	s.Equal(workflowExecution.GetRunId(), task1.RunID)
	s.Equal(tasklist, task1.TaskList)
	s.Equal(p.TransferTaskTypeDecisionTask, task1.TaskType)
	s.Equal(int64(2), task1.ScheduleID)
	s.Equal(p.TransferTaskTransferTargetWorkflowID, task1.TargetWorkflowID)
	s.Equal("", task1.TargetRunID)
	err3 := s.CompleteTransferTask(task1.TaskID)
	s.NoError(err3)

	state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedStats := copyExecutionStats(state0.ExecutionStats)
	updatedInfo.NextEventID = int64(6)
	updatedInfo.LastProcessedEvent = int64(2)
	scheduleID := int64(123)
	targetDomainID := "8bfb47be-5b57-4d66-9109-5fb35e20b1d0"
	targetWorkflowID := "some random target domain ID"
	targetRunID := uuid.New()
	currentTransferID := s.GetTransferReadLevel()
	now := time.Now()
	tasks := []p.Task{
		&p.ActivityTask{now, currentTransferID + 10001, domainID, tasklist, scheduleID, 111},
		&p.DecisionTask{now, currentTransferID + 10002, domainID, tasklist, scheduleID, 222, false},
		&p.CloseExecutionTask{now, currentTransferID + 10003, 333},
		&p.CancelExecutionTask{now, currentTransferID + 10004, targetDomainID, targetWorkflowID, targetRunID, true, scheduleID, 444},
		&p.SignalExecutionTask{now, currentTransferID + 10005, targetDomainID, targetWorkflowID, targetRunID, true, scheduleID, 555},
		&p.StartChildExecutionTask{now, currentTransferID + 10006, targetDomainID, targetWorkflowID, scheduleID, 666},
	}
	err2 := s.UpdateWorklowStateAndReplication(updatedInfo, updatedStats, nil, nil, int64(3), tasks)
	s.NoError(err2)

	txTasks, err1 := s.GetTransferTasks(1, true) // use page size one to force pagination
	s.NoError(err1)
	s.NotNil(txTasks, "expected valid list of tasks.")
	s.Equal(len(tasks), len(txTasks))
	for index := range tasks {
		s.True(timeComparator(tasks[index].GetVisibilityTimestamp(), txTasks[index].VisibilityTimestamp, TimePrecision))
	}
	s.Equal(p.TransferTaskTypeActivityTask, txTasks[0].TaskType)
	s.Equal(p.TransferTaskTypeDecisionTask, txTasks[1].TaskType)
	s.Equal(p.TransferTaskTypeCloseExecution, txTasks[2].TaskType)
	s.Equal(p.TransferTaskTypeCancelExecution, txTasks[3].TaskType)
	s.Equal(p.TransferTaskTypeSignalExecution, txTasks[4].TaskType)
	s.Equal(p.TransferTaskTypeStartChildExecution, txTasks[5].TaskType)
	s.Equal(int64(111), txTasks[0].Version)
	s.Equal(int64(222), txTasks[1].Version)
	s.Equal(int64(333), txTasks[2].Version)
	s.Equal(int64(444), txTasks[3].Version)
	s.Equal(int64(555), txTasks[4].Version)
	s.Equal(int64(666), txTasks[5].Version)

	err2 = s.RangeCompleteTransferTask(txTasks[0].TaskID-1, txTasks[5].TaskID)
	s.NoError(err2)

	txTasks, err2 = s.GetTransferTasks(100, false)
	s.NoError(err2)
	s.Empty(txTasks, "expected empty task list.")
}

// TestTimerTasksComplete test
func (s *ExecutionManagerSuite) TestTimerTasksComplete() {
	domainID := "8bfb47be-5b57-4d66-9109-5fb35e20b1d7"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("get-timer-tasks-test-complete"),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
	}

	now := time.Now()
	initialTasks := []p.Task{&p.DecisionTimeoutTask{now.Add(1 * time.Second), 1, 2, 3, int(gen.TimeoutTypeStartToClose), 11}}

	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "taskList", "wType", 20, 13, nil, 3, 0, 2, initialTasks)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
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
		&p.ActivityTimeoutTask{now.Add(3 * time.Second), 4, int(gen.TimeoutTypeStartToClose), 7, 0, 14},
		&p.UserTimerTask{now.Add(3 * time.Second), 5, 7, 15},
	}
	err2 := s.UpdateWorkflowExecution(updatedInfo, updatedStats, nil, []int64{int64(4)}, nil, int64(3), tasks, nil, nil, nil, nil)
	s.NoError(err2)

	timerTasks, err1 := s.GetTimerIndexTasks(1, true) // use page size one to force pagination
	s.NoError(err1)
	s.NotNil(timerTasks, "expected valid list of tasks.")
	s.Equal(len(tasks)+len(initialTasks), len(timerTasks))
	s.Equal(p.TaskTypeDecisionTimeout, timerTasks[0].TaskType)
	s.Equal(p.TaskTypeWorkflowTimeout, timerTasks[1].TaskType)
	s.Equal(p.TaskTypeDeleteHistoryEvent, timerTasks[2].TaskType)
	s.Equal(p.TaskTypeActivityTimeout, timerTasks[3].TaskType)
	s.Equal(p.TaskTypeUserTimer, timerTasks[4].TaskType)
	s.Equal(int64(11), timerTasks[0].Version)
	s.Equal(int64(12), timerTasks[1].Version)
	s.Equal(int64(13), timerTasks[2].Version)
	s.Equal(int64(14), timerTasks[3].Version)
	s.Equal(int64(15), timerTasks[4].Version)

	err2 = s.RangeCompleteTimerTask(timerTasks[0].VisibilityTimestamp, timerTasks[4].VisibilityTimestamp.Add(1*time.Second))
	s.NoError(err2)

	timerTasks2, err2 := s.GetTimerIndexTasks(100, false)
	s.NoError(err2)
	s.Empty(timerTasks2, "expected empty task list.")
}

// TestTimerTasksRangeComplete test
func (s *ExecutionManagerSuite) TestTimerTasksRangeComplete() {
	domainID := "8bfb47be-5b57-4d66-9109-5fb35e20b1d7"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("get-timer-tasks-test-range-complete"),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
	}

	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "taskList", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedStats := copyExecutionStats(state0.ExecutionStats)
	updatedInfo.NextEventID = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)
	tasks := []p.Task{
		&p.DecisionTimeoutTask{time.Now(), 1, 2, 3, int(gen.TimeoutTypeStartToClose), 11},
		&p.WorkflowTimeoutTask{time.Now(), 2, 12},
		&p.DeleteHistoryEventTask{time.Now(), 3, 13},
		&p.ActivityTimeoutTask{time.Now(), 4, int(gen.TimeoutTypeStartToClose), 7, 0, 14},
		&p.UserTimerTask{time.Now(), 5, 7, 15},
	}
	err2 := s.UpdateWorkflowExecution(updatedInfo, updatedStats, nil, []int64{int64(4)}, nil, int64(3), tasks, nil, nil, nil, nil)
	s.NoError(err2)

	timerTasks, err1 := s.GetTimerIndexTasks(1, true) // use page size one to force pagination
	s.NoError(err1)
	s.NotNil(timerTasks, "expected valid list of tasks.")
	s.Equal(len(tasks), len(timerTasks))
	s.Equal(p.TaskTypeDecisionTimeout, timerTasks[0].TaskType)
	s.Equal(p.TaskTypeWorkflowTimeout, timerTasks[1].TaskType)
	s.Equal(p.TaskTypeDeleteHistoryEvent, timerTasks[2].TaskType)
	s.Equal(p.TaskTypeActivityTimeout, timerTasks[3].TaskType)
	s.Equal(p.TaskTypeUserTimer, timerTasks[4].TaskType)
	s.Equal(int64(11), timerTasks[0].Version)
	s.Equal(int64(12), timerTasks[1].Version)
	s.Equal(int64(13), timerTasks[2].Version)
	s.Equal(int64(14), timerTasks[3].Version)
	s.Equal(int64(15), timerTasks[4].Version)

	err2 = s.UpdateWorkflowExecution(updatedInfo, updatedStats, nil, nil, nil, int64(5), nil, nil, nil, nil, nil)
	s.NoError(err2)

	err2 = s.CompleteTimerTask(timerTasks[0].VisibilityTimestamp, timerTasks[0].TaskID)
	s.NoError(err2)

	err2 = s.CompleteTimerTask(timerTasks[1].VisibilityTimestamp, timerTasks[1].TaskID)
	s.NoError(err2)

	err2 = s.CompleteTimerTask(timerTasks[2].VisibilityTimestamp, timerTasks[2].TaskID)
	s.NoError(err2)

	err2 = s.CompleteTimerTask(timerTasks[3].VisibilityTimestamp, timerTasks[3].TaskID)
	s.NoError(err2)

	err2 = s.CompleteTimerTask(timerTasks[4].VisibilityTimestamp, timerTasks[4].TaskID)
	s.NoError(err2)

	timerTasks2, err2 := s.GetTimerIndexTasks(100, false)
	s.NoError(err2)
	s.Empty(timerTasks2, "expected empty task list.")
}

// TestWorkflowMutableStateActivities test
func (s *ExecutionManagerSuite) TestWorkflowMutableStateActivities() {
	domainID := "7fcf0aa9-e121-4292-bdad-0a75181b4aa3"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("test-workflow-mutable-test"),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
	}

	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "taskList", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedStats := copyExecutionStats(state0.ExecutionStats)
	updatedInfo.NextEventID = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)
	currentTime := time.Now()
	activityInfos := []*p.ActivityInfo{{
		Version:                  7789,
		ScheduleID:               1,
		ScheduledEventBatchID:    1,
		ScheduledEvent:           &gen.HistoryEvent{EventId: int64Ptr(1)},
		ScheduledTime:            currentTime,
		ActivityID:               uuid.New(),
		RequestID:                uuid.New(),
		Details:                  []byte(uuid.New()),
		StartedID:                2,
		StartedEvent:             &gen.HistoryEvent{EventId: int64Ptr(2)},
		StartedTime:              currentTime,
		ScheduleToCloseTimeout:   1,
		ScheduleToStartTimeout:   2,
		StartToCloseTimeout:      3,
		HeartbeatTimeout:         4,
		LastHeartBeatUpdatedTime: currentTime,
		TimerTaskStatus:          1,
		CancelRequested:          true,
		CancelRequestID:          math.MaxInt64,
		Attempt:                  math.MaxInt32,
		DomainID:                 domainID,
		StartedIdentity:          uuid.New(),
		TaskList:                 uuid.New(),
		HasRetryPolicy:           true,
		InitialInterval:          math.MaxInt32,
		MaximumInterval:          math.MaxInt32,
		MaximumAttempts:          math.MaxInt32,
		BackoffCoefficient:       5.55,
		ExpirationTime:           currentTime,
		NonRetriableErrors:       []string{"accessDenied", "badRequest"},
		LastFailureReason:        "some random error",
		LastWorkerIdentity:       uuid.New(),
		LastFailureDetails:       []byte(uuid.New()),
	}}
	err2 := s.UpdateWorkflowExecution(updatedInfo, updatedStats, nil, []int64{int64(4)}, nil, int64(3), nil, activityInfos, nil, nil, nil)
	s.NoError(err2)

	state, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err1)
	s.NotNil(state, "expected valid state.")
	s.Equal(1, len(state.ActivityInfos))
	log.Printf("%+v", state.ActivityInfos)
	ai, ok := state.ActivityInfos[1]
	s.True(ok)
	s.NotNil(ai)
	s.Equal(int64(7789), ai.Version)
	s.Equal(int64(1), ai.ScheduleID)
	s.Equal(int64(1), ai.ScheduledEventBatchID)
	s.Equal(int64(1), *ai.ScheduledEvent.EventId)
	s.EqualTimes(currentTime, ai.ScheduledTime)
	s.Equal(activityInfos[0].ActivityID, ai.ActivityID)
	s.Equal(activityInfos[0].RequestID, ai.RequestID)
	s.Equal(activityInfos[0].Details, ai.Details)
	s.Equal(int64(2), ai.StartedID)
	s.Equal(int64(2), *ai.StartedEvent.EventId)
	s.EqualTimes(currentTime, ai.StartedTime)
	s.Equal(int32(1), ai.ScheduleToCloseTimeout)
	s.Equal(int32(2), ai.ScheduleToStartTimeout)
	s.Equal(int32(3), ai.StartToCloseTimeout)
	s.Equal(int32(4), ai.HeartbeatTimeout)
	s.EqualTimes(currentTime, ai.LastHeartBeatUpdatedTime)
	s.Equal(int32(1), ai.TimerTaskStatus)
	s.Equal(activityInfos[0].CancelRequested, ai.CancelRequested)
	s.Equal(activityInfos[0].CancelRequestID, ai.CancelRequestID)
	s.Equal(activityInfos[0].Attempt, ai.Attempt)
	s.Equal(activityInfos[0].DomainID, ai.DomainID)
	s.Equal(activityInfos[0].StartedIdentity, ai.StartedIdentity)
	s.Equal(activityInfos[0].TaskList, ai.TaskList)
	s.Equal(activityInfos[0].HasRetryPolicy, ai.HasRetryPolicy)
	s.Equal(activityInfos[0].InitialInterval, ai.InitialInterval)
	s.Equal(activityInfos[0].MaximumInterval, ai.MaximumInterval)
	s.Equal(activityInfos[0].MaximumAttempts, ai.MaximumAttempts)
	s.Equal(activityInfos[0].BackoffCoefficient, ai.BackoffCoefficient)
	s.EqualTimes(activityInfos[0].ExpirationTime, ai.ExpirationTime)
	s.Equal(activityInfos[0].NonRetriableErrors, ai.NonRetriableErrors)
	s.Equal(activityInfos[0].LastFailureReason, ai.LastFailureReason)
	s.Equal(activityInfos[0].LastWorkerIdentity, ai.LastWorkerIdentity)
	s.Equal(activityInfos[0].LastFailureDetails, ai.LastFailureDetails)

	err2 = s.UpdateWorkflowExecution(updatedInfo, updatedStats, nil, nil, nil, int64(5), nil, nil, []int64{1}, nil, nil)
	s.NoError(err2)

	state, err2 = s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err2)
	s.NotNil(state, "expected valid state.")
	s.Equal(0, len(state.ActivityInfos))
}

// TestWorkflowMutableStateTimers test
func (s *ExecutionManagerSuite) TestWorkflowMutableStateTimers() {
	domainID := "025d178a-709b-4c07-8dd7-86dbf9bd2e06"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("test-workflow-mutable-timers-test"),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
	}

	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "taskList", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedStats := copyExecutionStats(state0.ExecutionStats)
	updatedInfo.NextEventID = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)
	currentTime := time.Now().UTC()
	timerID := "id_1"
	timerInfos := []*p.TimerInfo{{
		Version:    3345,
		TimerID:    timerID,
		ExpiryTime: currentTime,
		TaskID:     2,
		StartedID:  5,
	}}
	err2 := s.UpdateWorkflowExecution(updatedInfo, updatedStats, nil, []int64{int64(4)}, nil, int64(3), nil, nil, nil, timerInfos, nil)
	s.NoError(err2)

	state, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err1)
	s.NotNil(state, "expected valid state.")
	s.Equal(1, len(state.TimerInfos))
	s.Equal(int64(3345), state.TimerInfos[timerID].Version)
	s.Equal(timerID, state.TimerInfos[timerID].TimerID)
	s.EqualTimesWithPrecision(currentTime, state.TimerInfos[timerID].ExpiryTime, time.Millisecond*500)
	s.Equal(int64(2), state.TimerInfos[timerID].TaskID)
	s.Equal(int64(5), state.TimerInfos[timerID].StartedID)

	err2 = s.UpdateWorkflowExecution(updatedInfo, updatedStats, nil, nil, nil, int64(5), nil, nil, nil, nil, []string{timerID})
	s.NoError(err2)

	state, err1 = s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err2)
	s.NotNil(state, "expected valid state.")
	s.Equal(0, len(state.TimerInfos))
}

// TestWorkflowMutableStateChildExecutions test
func (s *ExecutionManagerSuite) TestWorkflowMutableStateChildExecutions() {
	domainID := "88236cd2-c439-4cec-9957-2748ce3be074"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("test-workflow-mutable-child-executions-parent-test"),
		RunId:      common.StringPtr("c63dba1e-929c-4fbf-8ec5-4533b16269a9"),
	}

	parentDomainID := "6036ded3-e541-42c9-8f69-3d9354dad081"
	parentExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("test-workflow-mutable-child-executions-child-test"),
		RunId:      common.StringPtr("73e89362-25ec-4305-bcb8-d9448b90856c"),
	}

	task0, err0 := s.CreateChildWorkflowExecution(domainID, workflowExecution, parentDomainID, parentExecution, 1, "taskList", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")
	s.Equal(parentDomainID, info0.ParentDomainID)
	s.Equal(parentExecution.GetWorkflowId(), info0.ParentWorkflowID)
	s.Equal(parentExecution.GetRunId(), info0.ParentRunID)
	s.Equal(int64(1), info0.InitiatedID)

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedStats := copyExecutionStats(state0.ExecutionStats)
	updatedInfo.NextEventID = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)
	createRequestID := uuid.New()
	childExecutionInfos := []*p.ChildExecutionInfo{{
		Version:           1234,
		InitiatedID:       1,
		InitiatedEvent:    &gen.HistoryEvent{EventId: int64Ptr(1)},
		StartedID:         2,
		StartedEvent:      &gen.HistoryEvent{EventId: int64Ptr(2)},
		CreateRequestID:   createRequestID,
		ParentClosePolicy: gen.ParentClosePolicyTerminate,
	}}
	err2 := s.UpsertChildExecutionsState(updatedInfo, updatedStats, nil, int64(3), childExecutionInfos)
	s.NoError(err2)

	state, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err1)
	s.NotNil(state, "expected valid state.")
	s.Equal(1, len(state.ChildExecutionInfos))
	ci, ok := state.ChildExecutionInfos[1]
	s.True(ok)
	s.NotNil(ci)
	s.Equal(int64(1234), ci.Version)
	s.Equal(int64(1), ci.InitiatedID)
	s.Equal(gen.ParentClosePolicyTerminate, ci.ParentClosePolicy)
	s.Equal(int64(1), *ci.InitiatedEvent.EventId)
	s.Equal(int64(2), ci.StartedID)
	s.Equal(int64(2), *ci.StartedEvent.EventId)
	s.Equal(createRequestID, ci.CreateRequestID)

	err2 = s.DeleteChildExecutionsState(updatedInfo, updatedStats, nil, int64(5), int64(1))
	s.NoError(err2)

	state, err1 = s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err2)
	s.NotNil(state, "expected valid state.")
	s.Equal(0, len(state.ChildExecutionInfos))
}

// TestWorkflowMutableStateRequestCancel test
func (s *ExecutionManagerSuite) TestWorkflowMutableStateRequestCancel() {
	domainID := "568b8d19-cf64-4aac-be1b-f8a3edbc1fa9"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("test-workflow-mutable-request-cancel-test"),
		RunId:      common.StringPtr("87f96253-b925-426e-90db-aa4ee89b5aca"),
	}

	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "taskList", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedStats := copyExecutionStats(state0.ExecutionStats)
	updatedInfo.NextEventID = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)
	requestCancelInfo := &p.RequestCancelInfo{
		Version:               456,
		InitiatedID:           2,
		InitiatedEventBatchID: 1,
		CancelRequestID:       uuid.New(),
	}
	err2 := s.UpsertRequestCancelState(updatedInfo, updatedStats, nil, int64(3), []*p.RequestCancelInfo{requestCancelInfo})
	s.NoError(err2)

	state, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err1)
	s.NotNil(state, "expected valid state.")
	s.Equal(1, len(state.RequestCancelInfos))
	ri, ok := state.RequestCancelInfos[requestCancelInfo.InitiatedID]
	s.True(ok)
	s.NotNil(ri)
	s.Equal(requestCancelInfo, ri)

	err2 = s.DeleteCancelState(updatedInfo, updatedStats, nil, int64(5), requestCancelInfo.InitiatedID)
	s.NoError(err2)

	state, err1 = s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err2)
	s.NotNil(state, "expected valid state.")
	s.Equal(0, len(state.RequestCancelInfos))
}

// TestWorkflowMutableStateSignalInfo test
func (s *ExecutionManagerSuite) TestWorkflowMutableStateSignalInfo() {
	domainID := uuid.New()
	runID := uuid.New()
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("test-workflow-mutable-signal-info-test"),
		RunId:      common.StringPtr(runID),
	}

	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "taskList", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedStats := copyExecutionStats(state0.ExecutionStats)
	updatedInfo.NextEventID = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)
	signalInfo := &p.SignalInfo{
		Version:               123,
		InitiatedID:           2,
		InitiatedEventBatchID: 1,
		SignalRequestID:       uuid.New(),
		SignalName:            "my signal",
		Input:                 []byte("test signal input"),
		Control:               []byte(uuid.New()),
	}
	err2 := s.UpsertSignalInfoState(updatedInfo, updatedStats, nil, int64(3), []*p.SignalInfo{signalInfo})
	s.NoError(err2)

	state, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err1)
	s.NotNil(state, "expected valid state.")
	s.Equal(1, len(state.SignalInfos))
	si, ok := state.SignalInfos[signalInfo.InitiatedID]
	s.True(ok)
	s.NotNil(si)
	s.Equal(signalInfo, si)

	err2 = s.DeleteSignalState(updatedInfo, updatedStats, nil, int64(5), signalInfo.InitiatedID)
	s.NoError(err2)

	state, err1 = s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err2)
	s.NotNil(state, "expected valid state.")
	s.Equal(0, len(state.SignalInfos))
}

// TestWorkflowMutableStateSignalRequested test
func (s *ExecutionManagerSuite) TestWorkflowMutableStateSignalRequested() {
	domainID := uuid.New()
	runID := uuid.New()
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("test-workflow-mutable-signal-requested-test"),
		RunId:      common.StringPtr(runID),
	}

	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "taskList", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
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

	state, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err1)
	s.NotNil(state, "expected valid state.")
	s.Equal(1, len(state.SignalRequestedIDs))
	ri, ok := state.SignalRequestedIDs[signalRequestedID]
	s.True(ok)
	s.NotNil(ri)

	err2 = s.DeleteSignalsRequestedState(updatedInfo, updatedStats, nil, int64(5), signalRequestedID)
	s.NoError(err2)

	state, err1 = s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err2)
	s.NotNil(state, "expected valid state.")
	s.Equal(0, len(state.SignalRequestedIDs))
}

// TestWorkflowMutableStateInfo test
func (s *ExecutionManagerSuite) TestWorkflowMutableStateInfo() {
	domainID := "9ed8818b-3090-4160-9f21-c6b70e64d2dd"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("test-workflow-mutable-state-test"),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
	}

	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "taskList", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedStats := copyExecutionStats(state0.ExecutionStats)
	updatedInfo.NextEventID = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)

	err2 := s.UpdateWorkflowExecution(updatedInfo, updatedStats, nil, []int64{int64(4)}, nil, int64(3), nil, nil, nil, nil, nil)
	s.NoError(err2)

	state, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err1)
	s.NotNil(state, "expected valid state.")
	s.NotNil(state.ExecutionInfo, "expected valid MS Info state.")
	s.Equal(updatedInfo.NextEventID, state.ExecutionInfo.NextEventID)
	s.Equal(updatedInfo.State, state.ExecutionInfo.State)
}

// TestContinueAsNew test
func (s *ExecutionManagerSuite) TestContinueAsNew() {
	domainID := "c1c0bb55-04e6-4a9c-89d0-1be7b96459f8"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("continue-as-new-workflow-test"),
		RunId:      common.StringPtr("551c88d2-d9e6-404f-8131-9eec14f36643"),
	}

	_, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "queue1", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.NoError(err0)

	state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	continueAsNewInfo := copyWorkflowExecutionInfo(info0)
	continueAsNewStats := copyExecutionStats(state0.ExecutionStats)
	continueAsNewInfo.State = p.WorkflowStateCreated
	continueAsNewInfo.CloseStatus = p.WorkflowCloseStatusNone
	continueAsNewInfo.NextEventID = int64(5)
	continueAsNewInfo.LastProcessedEvent = int64(2)

	newWorkflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("continue-as-new-workflow-test"),
		RunId:      common.StringPtr("64c7e15a-3fd7-4182-9c6f-6f25a4fa2614"),
	}

	testResetPoints := gen.ResetPoints{
		Points: []*gen.ResetPointInfo{
			&gen.ResetPointInfo{
				BinaryChecksum:           common.StringPtr("test-binary-checksum"),
				RunId:                    common.StringPtr("test-runID"),
				FirstDecisionCompletedId: common.Int64Ptr(123),
				CreatedTimeNano:          common.Int64Ptr(456),
				Resettable:               common.BoolPtr(true),
				ExpiringTimeNano:         common.Int64Ptr(789),
			},
		},
	}

	err2 := s.ContinueAsNewExecution(continueAsNewInfo, continueAsNewStats, info0.NextEventID, newWorkflowExecution, int64(3), int64(2), &testResetPoints)
	s.NoError(err2)

	prevExecutionState, err3 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err3)
	prevExecutionInfo := prevExecutionState.ExecutionInfo
	s.Equal(p.WorkflowStateCompleted, prevExecutionInfo.State)
	s.Equal(p.WorkflowCloseStatusContinuedAsNew, prevExecutionInfo.CloseStatus)
	s.Equal(int64(5), prevExecutionInfo.NextEventID)
	s.Equal(int64(2), prevExecutionInfo.LastProcessedEvent)
	s.True(prevExecutionInfo.AutoResetPoints.Equals(&gen.ResetPoints{}))

	newExecutionState, err4 := s.GetWorkflowExecutionInfo(domainID, newWorkflowExecution)
	s.NoError(err4)
	newExecutionInfo := newExecutionState.ExecutionInfo
	s.Equal(p.WorkflowStateCreated, newExecutionInfo.State)
	s.Equal(p.WorkflowCloseStatusNone, newExecutionInfo.CloseStatus)
	s.Equal(int64(3), newExecutionInfo.NextEventID)
	s.Equal(common.EmptyEventID, newExecutionInfo.LastProcessedEvent)
	s.Equal(int64(2), newExecutionInfo.DecisionScheduleID)
	s.Equal(testResetPoints.String(), newExecutionInfo.AutoResetPoints.String())

	newRunID, err5 := s.GetCurrentWorkflowRunID(domainID, workflowExecution.GetWorkflowId())
	s.NoError(err5)
	s.Equal(newWorkflowExecution.GetRunId(), newRunID)
}

// TestReplicationTransferTaskTasks test
func (s *ExecutionManagerSuite) TestReplicationTransferTaskTasks() {
	domainID := "2466d7de-6602-4ad8-b939-fb8f8c36c711"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("replication-transfer-task-test"),
		RunId:      common.StringPtr("dcde9d85-5d7a-43c7-8b18-cb2cae0e29e0"),
	}

	task0, err := s.CreateWorkflowExecution(domainID, workflowExecution, "queue1", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.NoError(err)
	s.NotNil(task0, "Expected non empty task identifier.")

	taskD, err := s.GetTransferTasks(1, false)
	s.Equal(1, len(taskD), "Expected 1 decision task.")
	err = s.CompleteTransferTask(taskD[0].TaskID)
	s.NoError(err)

	state1, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
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
		LastReplicationInfo: map[string]*p.ReplicationInfo{
			"dc1": {
				Version:     int64(3),
				LastEventID: int64(1),
			},
			"dc2": {
				Version:     int64(5),
				LastEventID: int64(2),
			},
		},
	}}
	err = s.UpdateWorklowStateAndReplication(updatedInfo1, updatedStats1, nil, nil, int64(3), replicationTasks)
	s.NoError(err)

	tasks1, err := s.GetReplicationTasks(1, false)
	s.NoError(err)
	s.NotNil(tasks1, "expected valid list of tasks.")
	s.Equal(1, len(tasks1), "Expected 1 replication task.")
	task1 := tasks1[0]
	s.Equal(p.ReplicationTaskTypeHistory, task1.TaskType)
	s.Equal(domainID, task1.DomainID)
	s.Equal(workflowExecution.GetWorkflowId(), task1.WorkflowID)
	s.Equal(workflowExecution.GetRunId(), task1.RunID)
	s.Equal(int64(1), task1.FirstEventID)
	s.Equal(int64(3), task1.NextEventID)
	s.Equal(int64(9), task1.Version)
	s.Equal(2, len(task1.LastReplicationInfo))
	for k, v := range task1.LastReplicationInfo {
		log.Infof("ReplicationInfo for %v: {Version: %v, LastEventID: %v}", k, v.Version, v.LastEventID)
		switch k {
		case "dc1":
			s.Equal(int64(3), v.Version)
			s.Equal(int64(1), v.LastEventID)
		case "dc2":
			s.Equal(int64(5), v.Version)
			s.Equal(int64(2), v.LastEventID)
		default:
			s.Fail("Unexpected key")
		}
	}

	err = s.CompleteTransferTask(task1.TaskID)
	s.NoError(err)
}

// TestWorkflowReplicationState test
func (s *ExecutionManagerSuite) TestWorkflowReplicationState() {
	domainID := uuid.New()
	runID := uuid.New()
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("test-workflow-replication-state-test"),
		RunId:      common.StringPtr(runID),
	}

	replicationTasks := []p.Task{&p.HistoryReplicationTask{
		TaskID:       s.GetNextSequenceNumber(),
		FirstEventID: int64(1),
		NextEventID:  int64(3),
		Version:      int64(9),
		LastReplicationInfo: map[string]*p.ReplicationInfo{
			"dc1": {
				Version:     int64(3),
				LastEventID: int64(1),
			},
			"dc2": {
				Version:     int64(5),
				LastEventID: int64(2),
			},
		},
	}}

	task0, err0 := s.CreateWorkflowExecutionWithReplication(domainID, workflowExecution, "taskList", "wType", 20, 13, 3,
		0, 2, &p.ReplicationState{
			CurrentVersion:   int64(9),
			StartVersion:     int64(8),
			LastWriteVersion: int64(7),
			LastWriteEventID: int64(6),
			LastReplicationInfo: map[string]*p.ReplicationInfo{
				"dc1": {
					Version:     int64(3),
					LastEventID: int64(1),
				},
				"dc2": {
					Version:     int64(5),
					LastEventID: int64(2),
				},
			},
		}, replicationTasks)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	taskD, err := s.GetTransferTasks(2, false)
	s.Equal(1, len(taskD), "Expected 1 decision task.")
	s.Equal(p.TransferTaskTypeDecisionTask, taskD[0].TaskType)
	err = s.CompleteTransferTask(taskD[0].TaskID)
	s.NoError(err)

	taskR, err := s.GetReplicationTasks(1, false)
	s.Equal(1, len(taskR), "Expected 1 replication task.")
	tsk := taskR[0]
	s.Equal(p.ReplicationTaskTypeHistory, tsk.TaskType)
	s.Equal(domainID, tsk.DomainID)
	s.Equal(workflowExecution.GetWorkflowId(), tsk.WorkflowID)
	s.Equal(workflowExecution.GetRunId(), tsk.RunID)
	s.Equal(int64(1), tsk.FirstEventID)
	s.Equal(int64(3), tsk.NextEventID)
	s.Equal(int64(9), tsk.Version)
	s.Equal(2, len(tsk.LastReplicationInfo))
	for k, v := range tsk.LastReplicationInfo {
		log.Infof("ReplicationInfo for %v: {Version: %v, LastEventID: %v}", k, v.Version, v.LastEventID)
		switch k {
		case "dc1":
			s.Equal(int64(3), v.Version)
			s.Equal(int64(1), v.LastEventID)
		case "dc2":
			s.Equal(int64(5), v.Version)
			s.Equal(int64(2), v.LastEventID)
		default:
			s.Fail("Unexpected key")
		}
	}
	err = s.CompleteReplicationTask(taskR[0].TaskID)
	s.NoError(err)

	state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	replicationState0 := state0.ReplicationState
	s.NotNil(info0, "Valid Workflow info expected.")
	s.NotNil(replicationState0, "Valid replication state expected.")
	s.Equal(domainID, info0.DomainID)
	s.Equal("taskList", info0.TaskList)
	s.Equal("wType", info0.WorkflowTypeName)
	s.Equal(int32(20), info0.WorkflowTimeout)
	s.Equal(int32(13), info0.DecisionTimeoutValue)
	s.Equal(int64(3), info0.NextEventID)
	s.Equal(int64(0), info0.LastProcessedEvent)
	s.Equal(int64(2), info0.DecisionScheduleID)
	s.Equal(int64(9), replicationState0.CurrentVersion)
	s.Equal(int64(8), replicationState0.StartVersion)
	s.Equal(int64(7), replicationState0.LastWriteVersion)
	s.Equal(int64(6), replicationState0.LastWriteEventID)
	s.Equal(2, len(replicationState0.LastReplicationInfo))
	for k, v := range replicationState0.LastReplicationInfo {
		log.Infof("ReplicationInfo for %v: {Version: %v, LastEventID: %v}", k, v.Version, v.LastEventID)
		switch k {
		case "dc1":
			s.Equal(int64(3), v.Version)
			s.Equal(int64(1), v.LastEventID)
		case "dc2":
			s.Equal(int64(5), v.Version)
			s.Equal(int64(2), v.LastEventID)
		default:
			s.Fail("Unexpected key")
		}
	}

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedStats := copyExecutionStats(state0.ExecutionStats)
	updatedInfo.NextEventID = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)
	updatedReplicationState := copyReplicationState(replicationState0)
	updatedReplicationState.CurrentVersion = int64(10)
	updatedReplicationState.StartVersion = int64(11)
	updatedReplicationState.LastWriteVersion = int64(12)
	updatedReplicationState.LastWriteEventID = int64(13)
	updatedReplicationState.LastReplicationInfo["dc1"].Version = int64(4)
	updatedReplicationState.LastReplicationInfo["dc1"].LastEventID = int64(2)

	replicationTasks1 := []p.Task{&p.HistoryReplicationTask{
		TaskID:       s.GetNextSequenceNumber(),
		FirstEventID: int64(3),
		NextEventID:  int64(5),
		Version:      int64(10),
		LastReplicationInfo: map[string]*p.ReplicationInfo{
			"dc1": {
				Version:     int64(4),
				LastEventID: int64(2),
			},
			"dc2": {
				Version:     int64(5),
				LastEventID: int64(2),
			},
		},
	}}
	err2 := s.UpdateWorklowStateAndReplication(updatedInfo, updatedStats, updatedReplicationState, nil, int64(3), replicationTasks1)
	s.NoError(err2)

	taskR1, err := s.GetReplicationTasks(1, false)
	s.Equal(1, len(taskR1), "Expected 1 replication task.")
	tsk1 := taskR1[0]
	s.Equal(p.ReplicationTaskTypeHistory, tsk1.TaskType)
	s.Equal(domainID, tsk1.DomainID)
	s.Equal(workflowExecution.GetWorkflowId(), tsk1.WorkflowID)
	s.Equal(workflowExecution.GetRunId(), tsk1.RunID)
	s.Equal(int64(3), tsk1.FirstEventID)
	s.Equal(int64(5), tsk1.NextEventID)
	s.Equal(int64(10), tsk1.Version)
	s.Equal(2, len(tsk1.LastReplicationInfo))
	for k, v := range tsk1.LastReplicationInfo {
		log.Infof("ReplicationInfo for %v: {Version: %v, LastEventID: %v}", k, v.Version, v.LastEventID)
		switch k {
		case "dc1":
			s.Equal(int64(4), v.Version)
			s.Equal(int64(2), v.LastEventID)
		case "dc2":
			s.Equal(int64(5), v.Version)
			s.Equal(int64(2), v.LastEventID)
		default:
			s.Fail("Unexpected key")
		}
	}
	err = s.CompleteReplicationTask(taskR1[0].TaskID)
	s.NoError(err)

	state1, err2 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err2)
	info1 := state1.ExecutionInfo
	replicationState1 := state1.ReplicationState
	s.NotNil(info1, "Valid Workflow info expected.")
	s.Equal(domainID, info1.DomainID)
	s.Equal("taskList", info1.TaskList)
	s.Equal("wType", info1.WorkflowTypeName)
	s.Equal(int32(20), info1.WorkflowTimeout)
	s.Equal(int32(13), info1.DecisionTimeoutValue)
	s.Equal(int64(5), info1.NextEventID)
	s.Equal(int64(2), info1.LastProcessedEvent)
	s.Equal(int64(2), info1.DecisionScheduleID)
	s.Equal(int64(10), replicationState1.CurrentVersion)
	s.Equal(int64(11), replicationState1.StartVersion)
	s.Equal(int64(12), replicationState1.LastWriteVersion)
	s.Equal(int64(13), replicationState1.LastWriteEventID)
	s.Equal(2, len(replicationState1.LastReplicationInfo))
	for k, v := range replicationState1.LastReplicationInfo {
		log.Infof("ReplicationInfo for %v: {Version: %v, LastEventID: %v}", k, v.Version, v.LastEventID)
		switch k {
		case "dc1":
			s.Equal(int64(4), v.Version)
			s.Equal(int64(2), v.LastEventID)
		case "dc2":
			s.Equal(int64(5), v.Version)
			s.Equal(int64(2), v.LastEventID)
		default:
			s.Fail("Unexpected key")
		}
	}
}

// TestUpdateAndClearBufferedEvents test
func (s *ExecutionManagerSuite) TestUpdateAndClearBufferedEvents() {
	domainID := "4ca1faac-1a3a-47af-8e51-fdaa2b3d45b9"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("test-update-and-clear-buffered-events"),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
	}

	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "taskList", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	stats0, state0, err1 := s.GetWorkflowExecutionInfoWithStats(domainID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")
	s.Equal(0, stats0.BufferedEventsCount)
	s.Equal(0, stats0.BufferedEventsSize)

	eventsBatch1 := []*gen.HistoryEvent{
		&gen.HistoryEvent{
			EventId:   common.Int64Ptr(5),
			EventType: gen.EventTypeDecisionTaskCompleted.Ptr(),
			Version:   common.Int64Ptr(11),
			DecisionTaskCompletedEventAttributes: &gen.DecisionTaskCompletedEventAttributes{
				ScheduledEventId: common.Int64Ptr(2),
				StartedEventId:   common.Int64Ptr(3),
				Identity:         common.StringPtr("test_worker"),
			},
		},
		&gen.HistoryEvent{
			EventId:   common.Int64Ptr(6),
			EventType: gen.EventTypeTimerStarted.Ptr(),
			Version:   common.Int64Ptr(11),
			TimerStartedEventAttributes: &gen.TimerStartedEventAttributes{
				TimerId:                      common.StringPtr("ID1"),
				StartToFireTimeoutSeconds:    common.Int64Ptr(101),
				DecisionTaskCompletedEventId: common.Int64Ptr(5),
			},
		},
	}

	eventsBatch2 := []*gen.HistoryEvent{
		&gen.HistoryEvent{
			EventId:   common.Int64Ptr(21),
			EventType: gen.EventTypeTimerFired.Ptr(),
			Version:   common.Int64Ptr(12),
			TimerFiredEventAttributes: &gen.TimerFiredEventAttributes{
				TimerId:        common.StringPtr("2"),
				StartedEventId: common.Int64Ptr(3),
			},
		},
	}

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedStats := copyExecutionStats(state0.ExecutionStats)
	updatedState := &p.WorkflowMutableState{
		ExecutionInfo:  updatedInfo,
		ExecutionStats: updatedStats,
	}

	err2 := s.UpdateAllMutableState(updatedState, int64(3))
	s.NoError(err2)

	partialState, err2 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err2)
	s.NotNil(partialState, "expected valid state.")
	partialInfo := partialState.ExecutionInfo
	s.NotNil(partialInfo, "Valid Workflow info expected.")

	bufferUpdateInfo := copyWorkflowExecutionInfo(partialInfo)
	bufferedUpdatedStats := copyExecutionStats(state0.ExecutionStats)
	err2 = s.UpdateWorklowStateAndReplication(bufferUpdateInfo, bufferedUpdatedStats, nil, nil, bufferUpdateInfo.NextEventID, nil)
	s.NoError(err2)
	err2 = s.UpdateWorklowStateAndReplication(bufferUpdateInfo, bufferedUpdatedStats, nil, nil, bufferUpdateInfo.NextEventID, nil)
	s.NoError(err2)
	err2 = s.UpdateWorkflowExecutionForBufferEvents(bufferUpdateInfo, bufferedUpdatedStats, nil, bufferUpdateInfo.NextEventID, eventsBatch1, false)
	s.NoError(err2)
	stats0, state0, err2 = s.GetWorkflowExecutionInfoWithStats(domainID, workflowExecution)
	s.NoError(err2)
	s.Equal(1, stats0.BufferedEventsCount)
	s.True(stats0.BufferedEventsSize > 0)
	history := &gen.History{Events: make([]*gen.HistoryEvent, 0)}
	history.Events = append(history.Events, eventsBatch1...)
	history0 := &gen.History{Events: state0.BufferedEvents}
	s.True(history.Equals(history0))
	history.Events = append(history.Events, eventsBatch2...)

	err2 = s.UpdateWorkflowExecutionForBufferEvents(bufferUpdateInfo, bufferedUpdatedStats, nil, bufferUpdateInfo.NextEventID, eventsBatch2, false)
	s.NoError(err2)

	stats1, state1, err1 := s.GetWorkflowExecutionInfoWithStats(domainID, workflowExecution)
	s.NoError(err1)
	s.NotNil(state1, "expected valid state.")
	info1 := state1.ExecutionInfo
	s.NotNil(info1, "Valid Workflow info expected.")
	s.Equal(2, stats1.BufferedEventsCount)
	s.True(stats1.BufferedEventsSize > 0)
	history1 := &gen.History{Events: state1.BufferedEvents}
	s.True(history.Equals(history1))

	err3 := s.UpdateWorkflowExecutionForBufferEvents(bufferUpdateInfo, bufferedUpdatedStats, nil, bufferUpdateInfo.NextEventID, nil, true)
	s.NoError(err3)

	stats3, state3, err3 := s.GetWorkflowExecutionInfoWithStats(domainID, workflowExecution)
	s.NoError(err3)
	s.NotNil(state3, "expected valid state.")
	info3 := state3.ExecutionInfo
	s.NotNil(info3, "Valid Workflow info expected.")
	s.Equal(0, stats3.BufferedEventsCount)
	s.Equal(0, stats3.BufferedEventsSize)
}

// TestConflictResolveWorkflowExecutionCurrentIsSelf test
func (s *ExecutionManagerSuite) TestConflictResolveWorkflowExecutionCurrentIsSelf() {
	domainID := "4ca1faac-1a3a-47af-8e51-fdaa2b3d45b9"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("test-reset-mutable-state-test-current-is-self"),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
	}
	version := int64(1234)
	nextEventID := int64(3)
	replicationState := &p.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	task0, err0 := s.CreateWorkflowExecutionWithReplication(domainID, workflowExecution, "taskList", "wType", 20, 13, 3, 0, 2, replicationState, nil)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	stats0, state0, err1 := s.GetWorkflowExecutionInfoWithStats(domainID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")
	s.Equal(0, stats0.BufferedEventsCount)
	s.Equal(0, stats0.BufferedEventsSize)

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedStats := copyExecutionStats(state0.ExecutionStats)
	updatedInfo.NextEventID = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)
	currentTime := time.Now().UTC()
	expiryTime := currentTime.Add(10 * time.Second)
	eventsBatch1 := []*gen.HistoryEvent{
		&gen.HistoryEvent{
			EventId:   common.Int64Ptr(5),
			EventType: gen.EventTypeDecisionTaskCompleted.Ptr(),
			Version:   common.Int64Ptr(11),
			DecisionTaskCompletedEventAttributes: &gen.DecisionTaskCompletedEventAttributes{
				ScheduledEventId: common.Int64Ptr(2),
				StartedEventId:   common.Int64Ptr(3),
				Identity:         common.StringPtr("test_worker"),
			},
		},
		&gen.HistoryEvent{
			EventId:   common.Int64Ptr(6),
			EventType: gen.EventTypeTimerStarted.Ptr(),
			Version:   common.Int64Ptr(11),
			TimerStartedEventAttributes: &gen.TimerStartedEventAttributes{
				TimerId:                      common.StringPtr("ID1"),
				StartToFireTimeoutSeconds:    common.Int64Ptr(101),
				DecisionTaskCompletedEventId: common.Int64Ptr(5),
			},
		},
	}

	eventsBatch2 := []*gen.HistoryEvent{
		&gen.HistoryEvent{
			EventId:   common.Int64Ptr(21),
			EventType: gen.EventTypeTimerFired.Ptr(),
			Version:   common.Int64Ptr(12),
			TimerFiredEventAttributes: &gen.TimerFiredEventAttributes{
				TimerId:        common.StringPtr("2"),
				StartedEventId: common.Int64Ptr(3),
			},
		},
	}

	updatedState := &p.WorkflowMutableState{
		ExecutionInfo:  updatedInfo,
		ExecutionStats: updatedStats,
		ActivityInfos: map[int64]*p.ActivityInfo{
			4: {
				Version:                  7789,
				ScheduleID:               4,
				ScheduledEventBatchID:    3,
				ScheduledEvent:           &gen.HistoryEvent{EventId: int64Ptr(40)},
				ScheduledTime:            currentTime,
				StartedID:                6,
				StartedEvent:             &gen.HistoryEvent{EventId: int64Ptr(60)},
				StartedTime:              currentTime,
				ScheduleToCloseTimeout:   1,
				ScheduleToStartTimeout:   2,
				StartToCloseTimeout:      3,
				HeartbeatTimeout:         4,
				LastHeartBeatUpdatedTime: currentTime,
				TimerTaskStatus:          1,
			},
			5: {
				Version:                  7789,
				ScheduleID:               5,
				ScheduledEventBatchID:    3,
				ScheduledEvent:           &gen.HistoryEvent{EventId: int64Ptr(50)},
				ScheduledTime:            currentTime,
				StartedID:                7,
				StartedEvent:             &gen.HistoryEvent{EventId: int64Ptr(70)},
				StartedTime:              currentTime,
				ScheduleToCloseTimeout:   1,
				ScheduleToStartTimeout:   2,
				StartToCloseTimeout:      3,
				HeartbeatTimeout:         4,
				LastHeartBeatUpdatedTime: currentTime,
				TimerTaskStatus:          1,
			}},

		TimerInfos: map[string]*p.TimerInfo{
			"t1": {
				Version:    2333,
				TimerID:    "t1",
				StartedID:  1,
				ExpiryTime: expiryTime,
				TaskID:     500,
			},
			"t2": {
				Version:    2333,
				TimerID:    "t2",
				StartedID:  2,
				ExpiryTime: expiryTime,
				TaskID:     501,
			},
			"t3": {
				Version:    2333,
				TimerID:    "t3",
				StartedID:  3,
				ExpiryTime: expiryTime,
				TaskID:     502,
			},
		},

		ChildExecutionInfos: map[int64]*p.ChildExecutionInfo{
			9: {
				Version:         2334,
				InitiatedID:     9,
				InitiatedEvent:  &gen.HistoryEvent{EventId: int64Ptr(123)},
				StartedID:       11,
				StartedEvent:    nil,
				CreateRequestID: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
			},
		},

		RequestCancelInfos: map[int64]*p.RequestCancelInfo{
			19: {
				Version:               2335,
				InitiatedID:           19,
				InitiatedEventBatchID: 17,
				CancelRequestID:       "cancel_requested_id",
			},
		},

		SignalInfos: map[int64]*p.SignalInfo{
			39: {
				Version:               2336,
				InitiatedID:           39,
				InitiatedEventBatchID: 38,
				SignalRequestID:       "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
				SignalName:            "signalA",
				Input:                 []byte("signal_input_A"),
				Control:               []byte("signal_control_A"),
			},
		},

		SignalRequestedIDs: map[string]struct{}{
			"00000000-0000-0000-0000-000000000001": {},
			"00000000-0000-0000-0000-000000000002": {},
			"00000000-0000-0000-0000-000000000003": {},
		},
	}

	err2 := s.UpdateAllMutableState(updatedState, int64(3))
	s.NoError(err2)

	partialState, err2 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err2)
	s.NotNil(partialState, "expected valid state.")
	partialInfo := partialState.ExecutionInfo
	s.NotNil(partialInfo, "Valid Workflow info expected.")

	bufferUpdateInfo := copyWorkflowExecutionInfo(partialInfo)
	bufferedUpdatedStats := copyExecutionStats(partialState.ExecutionStats)
	err2 = s.UpdateWorklowStateAndReplication(bufferUpdateInfo, bufferedUpdatedStats, nil, nil, bufferUpdateInfo.NextEventID, nil)
	s.NoError(err2)
	err2 = s.UpdateWorklowStateAndReplication(bufferUpdateInfo, bufferedUpdatedStats, nil, nil, bufferUpdateInfo.NextEventID, nil)
	s.NoError(err2)
	err2 = s.UpdateWorkflowExecutionForBufferEvents(bufferUpdateInfo, bufferedUpdatedStats, replicationState, bufferUpdateInfo.NextEventID, eventsBatch1, false)
	s.NoError(err2)
	stats0, state0, err2 = s.GetWorkflowExecutionInfoWithStats(domainID, workflowExecution)
	s.NoError(err2)
	s.Equal(1, stats0.BufferedEventsCount)
	s.True(stats0.BufferedEventsSize > 0)
	history := &gen.History{Events: make([]*gen.HistoryEvent, 0)}
	history.Events = append(history.Events, eventsBatch1...)
	history0 := &gen.History{Events: state0.BufferedEvents}
	s.True(history.Equals(history0))
	history.Events = append(history.Events, eventsBatch2...)

	err2 = s.UpdateWorkflowExecutionForBufferEvents(bufferUpdateInfo, bufferedUpdatedStats, replicationState, bufferUpdateInfo.NextEventID, eventsBatch2, false)
	s.NoError(err2)

	stats1, state1, err1 := s.GetWorkflowExecutionInfoWithStats(domainID, workflowExecution)
	s.NoError(err1)
	s.NotNil(state1, "expected valid state.")
	info1 := state1.ExecutionInfo
	s.NotNil(info1, "Valid Workflow info expected.")
	s.Equal(2, stats1.BufferedEventsCount)
	s.True(stats1.BufferedEventsSize > 0)
	history1 := &gen.History{Events: state1.BufferedEvents}
	s.True(history.Equals(history1))

	s.Equal(2, len(state1.ActivityInfos))
	ai, ok := state1.ActivityInfos[4]
	s.True(ok)
	s.NotNil(ai)
	s.Equal(int64(7789), ai.Version)
	s.Equal(int64(4), ai.ScheduleID)
	s.Equal(int64(3), ai.ScheduledEventBatchID)
	s.Equal(int64(40), *ai.ScheduledEvent.EventId)
	s.EqualTimes(currentTime, ai.ScheduledTime)
	s.Equal(int64(6), ai.StartedID)
	s.Equal(int64(60), *ai.StartedEvent.EventId)
	s.EqualTimes(currentTime, ai.StartedTime)
	s.Equal(int32(1), ai.ScheduleToCloseTimeout)
	s.Equal(int32(2), ai.ScheduleToStartTimeout)
	s.Equal(int32(3), ai.StartToCloseTimeout)
	s.Equal(int32(4), ai.HeartbeatTimeout)
	s.EqualTimes(currentTime, ai.LastHeartBeatUpdatedTime)
	s.Equal(int32(1), ai.TimerTaskStatus)

	ai, ok = state1.ActivityInfos[5]
	s.True(ok)
	s.NotNil(ai)
	s.Equal(int64(7789), ai.Version)
	s.Equal(int64(5), ai.ScheduleID)
	s.Equal(int64(3), ai.ScheduledEventBatchID)
	s.Equal(int64(50), *ai.ScheduledEvent.EventId)
	s.EqualTimes(currentTime, ai.ScheduledTime)
	s.Equal(int64(7), ai.StartedID)
	s.Equal(int64(70), *ai.StartedEvent.EventId)
	s.EqualTimes(currentTime, ai.StartedTime)
	s.Equal(int32(1), ai.ScheduleToCloseTimeout)
	s.Equal(int32(2), ai.ScheduleToStartTimeout)
	s.Equal(int32(3), ai.StartToCloseTimeout)
	s.Equal(int32(4), ai.HeartbeatTimeout)
	s.EqualTimes(currentTime, ai.LastHeartBeatUpdatedTime)
	s.Equal(int32(1), ai.TimerTaskStatus)

	s.Equal(3, len(state1.TimerInfos))
	ti, ok := state1.TimerInfos["t1"]
	s.True(ok)
	s.NotNil(ti)
	s.Equal(int64(2333), ti.Version)
	s.Equal("t1", ti.TimerID)
	s.Equal(int64(1), ti.StartedID)
	s.EqualTimes(expiryTime, ti.ExpiryTime)
	s.Equal(int64(500), ti.TaskID)

	ti, ok = state1.TimerInfos["t2"]
	s.True(ok)
	s.NotNil(ti)
	s.Equal(int64(2333), ti.Version)
	s.Equal("t2", ti.TimerID)
	s.Equal(int64(2), ti.StartedID)
	s.EqualTimes(expiryTime, ti.ExpiryTime)
	s.Equal(int64(501), ti.TaskID)

	ti, ok = state1.TimerInfos["t3"]
	s.True(ok)
	s.NotNil(ti)
	s.Equal(int64(2333), ti.Version)
	s.Equal("t3", ti.TimerID)
	s.Equal(int64(3), ti.StartedID)
	s.EqualTimes(expiryTime, ti.ExpiryTime)
	s.Equal(int64(502), ti.TaskID)

	s.Equal(1, len(state1.ChildExecutionInfos))
	ci, ok := state1.ChildExecutionInfos[9]
	s.True(ok)
	s.NotNil(ci)
	s.Equal(int64(2334), ci.Version)

	s.Equal(1, len(state1.RequestCancelInfos))
	rci, ok := state1.RequestCancelInfos[19]
	s.True(ok)
	s.NotNil(rci)
	s.Equal(int64(2335), rci.Version)

	s.Equal(1, len(state1.SignalInfos))
	si, ok := state1.SignalInfos[39]
	s.True(ok)
	s.NotNil(si)
	s.Equal(int64(2336), si.Version)

	s.Equal(3, len(state1.SignalRequestedIDs))
	_, contains := state1.SignalRequestedIDs["00000000-0000-0000-0000-000000000001"]
	s.True(contains)
	_, contains = state1.SignalRequestedIDs["00000000-0000-0000-0000-000000000002"]
	s.True(contains)
	_, contains = state1.SignalRequestedIDs["00000000-0000-0000-0000-000000000003"]
	s.True(contains)

	s.Equal(3, len(state1.BufferedEvents))

	updatedInfo1 := copyWorkflowExecutionInfo(info1)
	updatedStats1 := copyExecutionStats(state1.ExecutionStats)
	updatedInfo1.NextEventID = int64(3)
	resetActivityInfos := []*p.ActivityInfo{
		{
			Version:                  8789,
			ScheduleID:               40,
			ScheduledEventBatchID:    30,
			ScheduledEvent:           &gen.HistoryEvent{EventId: int64Ptr(400)},
			ScheduledTime:            currentTime,
			StartedID:                60,
			StartedEvent:             &gen.HistoryEvent{EventId: int64Ptr(600)},
			StartedTime:              currentTime,
			ScheduleToCloseTimeout:   10,
			ScheduleToStartTimeout:   20,
			StartToCloseTimeout:      30,
			HeartbeatTimeout:         40,
			LastHeartBeatUpdatedTime: currentTime,
			TimerTaskStatus:          1,
		}}

	resetTimerInfos := []*p.TimerInfo{
		{
			Version:    3333,
			TimerID:    "t1_new",
			StartedID:  1,
			ExpiryTime: expiryTime,
			TaskID:     600,
		},
		{
			Version:    3333,
			TimerID:    "t2_new",
			StartedID:  2,
			ExpiryTime: expiryTime,
			TaskID:     601,
		}}

	resetChildExecutionInfos := []*p.ChildExecutionInfo{
		{
			Version:         3334,
			InitiatedID:     10,
			InitiatedEvent:  &gen.HistoryEvent{EventId: common.Int64Ptr(10)},
			StartedID:       15,
			StartedEvent:    nil,
			CreateRequestID: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
		}}

	resetRequestCancelInfos := []*p.RequestCancelInfo{
		{
			Version:         3335,
			InitiatedID:     29,
			CancelRequestID: "new_cancel_requested_id",
		}}

	resetSignalInfos := []*p.SignalInfo{
		{
			Version:         3336,
			InitiatedID:     39,
			SignalRequestID: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
			SignalName:      "signalB",
			Input:           []byte("signal_input_b"),
			Control:         []byte("signal_control_b"),
		},
		{
			Version:         3336,
			InitiatedID:     42,
			SignalRequestID: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
			SignalName:      "signalC",
			Input:           []byte("signal_input_c"),
			Control:         []byte("signal_control_c"),
		}}

	rState := &p.ReplicationState{
		CurrentVersion: int64(8789),
		StartVersion:   int64(8780),
	}

	err3 := s.ConflictResolveWorkflowExecution(
		workflowExecution.GetRunId(), state1.ReplicationState.LastWriteVersion, state1.ExecutionInfo.State,
		updatedInfo1, updatedStats1, rState, int64(5), resetActivityInfos, resetTimerInfos,
		resetChildExecutionInfos, resetRequestCancelInfos, resetSignalInfos, nil)
	s.NoError(err3)

	stats4, state4, err4 := s.GetWorkflowExecutionInfoWithStats(domainID, workflowExecution)
	s.NoError(err4)
	s.NotNil(state4, "expected valid state.")
	s.Equal(0, stats4.BufferedEventsCount)
	s.Equal(0, stats4.BufferedEventsSize)

	info4 := state4.ExecutionInfo
	log.Printf("%+v", info4)
	s.NotNil(info4, "Valid Workflow info expected.")
	s.Equal(int64(3), info4.NextEventID)

	s.Equal(1, len(state4.ActivityInfos))
	ai, ok = state4.ActivityInfos[40]
	s.True(ok)
	s.NotNil(ai)
	s.Equal(int64(8789), ai.Version)
	s.Equal(int64(40), ai.ScheduleID)
	s.Equal(int64(30), ai.ScheduledEventBatchID)
	s.Equal(int64(400), *ai.ScheduledEvent.EventId)
	s.Equal(currentTime.Unix(), ai.ScheduledTime.Unix())
	s.Equal(int64(60), ai.StartedID)
	s.Equal(int64(600), *ai.StartedEvent.EventId)
	s.Equal(currentTime.Unix(), ai.StartedTime.Unix())
	s.Equal(int32(10), ai.ScheduleToCloseTimeout)
	s.Equal(int32(20), ai.ScheduleToStartTimeout)
	s.Equal(int32(30), ai.StartToCloseTimeout)
	s.Equal(int32(40), ai.HeartbeatTimeout)
	s.Equal(currentTime.Unix(), ai.LastHeartBeatUpdatedTime.Unix())
	s.Equal(int32(1), ai.TimerTaskStatus)

	s.Equal(2, len(state4.TimerInfos))
	ti, ok = state4.TimerInfos["t1_new"]
	s.True(ok)
	s.NotNil(ai)
	s.Equal(int64(3333), ti.Version)
	s.Equal("t1_new", ti.TimerID)
	s.Equal(int64(1), ti.StartedID)
	s.EqualTimes(expiryTime, ti.ExpiryTime)
	s.Equal(int64(600), ti.TaskID)

	ti, ok = state4.TimerInfos["t2_new"]
	s.True(ok)
	s.NotNil(ai)
	s.Equal(int64(3333), ti.Version)
	s.Equal("t2_new", ti.TimerID)
	s.Equal(int64(2), ti.StartedID)
	s.EqualTimes(expiryTime, ti.ExpiryTime)
	s.Equal(int64(601), ti.TaskID)

	s.Equal(1, len(state4.ChildExecutionInfos))
	ci, ok = state4.ChildExecutionInfos[10]
	s.True(ok)
	s.NotNil(ci)
	s.Equal(int64(3334), ci.Version)
	s.Equal(int64(10), ci.InitiatedID)
	s.Equal(int64(15), ci.StartedID)

	s.Equal(1, len(state4.RequestCancelInfos))
	rci, ok = state4.RequestCancelInfos[29]
	s.True(ok)
	s.NotNil(rci)
	s.Equal(int64(3335), rci.Version)
	s.Equal(int64(29), rci.InitiatedID)
	s.Equal("new_cancel_requested_id", rci.CancelRequestID)

	s.Equal(2, len(state4.SignalInfos))
	si, ok = state4.SignalInfos[39]
	s.True(ok)
	s.NotNil(si)
	s.Equal(int64(3336), si.Version)
	s.Equal(int64(39), si.InitiatedID)
	s.Equal("signalB", si.SignalName)
	s.Equal([]byte("signal_input_b"), si.Input)
	s.Equal([]byte("signal_control_b"), si.Control)

	si, ok = state4.SignalInfos[42]
	s.True(ok)
	s.NotNil(si)
	s.Equal(int64(3336), si.Version)
	s.Equal(int64(42), si.InitiatedID)
	s.Equal("signalC", si.SignalName)
	s.Equal([]byte("signal_input_c"), si.Input)
	s.Equal([]byte("signal_control_c"), si.Control)

	s.Equal(0, len(state4.SignalRequestedIDs))

	s.Equal(0, len(state4.BufferedEvents))

}

// TestConflictResolveWorkflowExecutionWithCASCurrentIsNotSelf test
func (s *ExecutionManagerSuite) TestConflictResolveWorkflowExecutionWithCASCurrentIsNotSelf() {
	domainID := "4ca1faac-1a3a-47af-8e51-fdaa2b3d45b9"
	workflowID := "test-reset-mutable-state-test-current-is-not-self"

	// first create a workflow and continue as new it
	workflowExecutionReset := gen.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa0"),
	}
	version := int64(1234)
	nextEventID := int64(3)
	replicationState := &p.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	resp, err := s.CreateWorkflowExecutionWithReplication(domainID, workflowExecutionReset, "taskList", "wType", 20, 13, nextEventID, 0, 2, replicationState, nil)
	s.NoError(err)
	s.NotNil(resp)

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecutionReset)
	s.NoError(err)

	info := state.ExecutionInfo
	continueAsNewInfo := copyWorkflowExecutionInfo(info)
	continueAsNewStats := copyExecutionStats(state.ExecutionStats)
	continueAsNewInfo.State = p.WorkflowStateRunning
	continueAsNewInfo.NextEventID = int64(5)
	continueAsNewInfo.LastProcessedEvent = int64(2)

	workflowExecutionCurrent := gen.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1"),
	}
	err = s.ContinueAsNewExecutionWithReplication(continueAsNewInfo, continueAsNewStats, info.NextEventID, workflowExecutionCurrent, int64(3), int64(2), nil, replicationState, replicationState)
	s.NoError(err)

	currentRunID, err := s.GetCurrentWorkflowRunID(domainID, workflowID)
	s.Equal(workflowExecutionCurrent.GetRunId(), currentRunID)
	state, err = s.GetWorkflowExecutionInfo(domainID, workflowExecutionCurrent)
	s.NoError(err)
	currentInfo := copyWorkflowExecutionInfo(state.ExecutionInfo)
	currentState := copyReplicationState(state.ReplicationState)

	resetExecutionInfo := &p.WorkflowExecutionInfo{
		DomainID:             domainID,
		WorkflowID:           workflowExecutionReset.GetWorkflowId(),
		RunID:                workflowExecutionReset.GetRunId(),
		ParentDomainID:       uuid.New(),
		ParentWorkflowID:     "some random parent workflow ID",
		ParentRunID:          uuid.New(),
		InitiatedID:          12345,
		TaskList:             "some random tasklist",
		WorkflowTypeName:     "some random workflow type name",
		WorkflowTimeout:      1112,
		DecisionTimeoutValue: 14,
		State:                p.WorkflowStateRunning,
		LastFirstEventID:     common.FirstEventID,
		NextEventID:          123,
		CreateRequestID:      uuid.New(),
		DecisionVersion:      common.EmptyVersion,
		DecisionScheduleID:   111,
		DecisionStartedID:    222,
		DecisionRequestID:    uuid.New(),
		DecisionTimeout:      0,
	}
	resetStats := &p.ExecutionStats{}
	resetActivityInfos := []*p.ActivityInfo{}
	resetTimerInfos := []*p.TimerInfo{}
	resetChildExecutionInfos := []*p.ChildExecutionInfo{}
	resetRequestCancelInfos := []*p.RequestCancelInfo{}
	resetSignalInfos := []*p.SignalInfo{}
	rState := &p.ReplicationState{
		CurrentVersion: int64(8789),
		StartVersion:   int64(8780),
	}
	err = s.ConflictResolveWorkflowExecution(
		currentRunID, currentState.LastWriteVersion, currentInfo.State,
		resetExecutionInfo, resetStats, rState, continueAsNewInfo.NextEventID, resetActivityInfos, resetTimerInfos,
		resetChildExecutionInfos, resetRequestCancelInfos, resetSignalInfos, nil)
	s.NoError(err)

	// this test only assert whether the current workflow execution record is reset
	runID, err := s.GetCurrentWorkflowRunID(domainID, workflowID)
	s.Equal(workflowExecutionReset.GetRunId(), runID)

	state, err = s.GetWorkflowExecutionInfo(domainID, workflowExecutionReset)
	s.NoError(err)
	info = state.ExecutionInfo
	continueAsNewInfo = copyWorkflowExecutionInfo(info)
	continueAsNewStats = copyExecutionStats(state.ExecutionStats)
	continueAsNewInfo.State = p.WorkflowStateRunning
	continueAsNewInfo.NextEventID += 3
	continueAsNewInfo.LastProcessedEvent += 2

	workflowExecutionCurrent2 := gen.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa2"),
	}
	err = s.ContinueAsNewExecutionWithReplication(continueAsNewInfo, continueAsNewStats, info.NextEventID, workflowExecutionCurrent2, int64(3), int64(2), nil, replicationState, replicationState)
	s.NoError(err)

	runID2, err := s.GetCurrentWorkflowRunID(domainID, workflowID)
	s.Equal(workflowExecutionCurrent2.GetRunId(), runID2)

	state, err = s.GetWorkflowExecutionInfo(domainID, workflowExecutionCurrent2)
	s.NoError(err)
	currentInfo = copyWorkflowExecutionInfo(state.ExecutionInfo)
	currentState = copyReplicationState(state.ReplicationState)

	err = s.ConflictResolveWorkflowExecution(
		workflowExecutionCurrent2.GetRunId(), currentState.LastWriteVersion, currentInfo.State,
		resetExecutionInfo, resetStats, rState, continueAsNewInfo.NextEventID, resetActivityInfos, resetTimerInfos,
		resetChildExecutionInfos, resetRequestCancelInfos, resetSignalInfos, nil)
	s.NoError(err)

	// this test only assert whether the current workflow execution record is reseted
	runID, err = s.GetCurrentWorkflowRunID(domainID, workflowID)
	s.Equal(workflowExecutionReset.GetRunId(), runID)
}

// TestConflictResolveWorkflowExecutionWithCASMismatch test
func (s *ExecutionManagerSuite) TestConflictResolveWorkflowExecutionWithCASMismatch() {
	domainID := "4ca1faac-1a3a-47af-8e51-fdaa2b3d45b9"
	workflowID := "test-reset-mutable-state-test-mismatch"

	// first create a workflow and continue as new it
	workflowExecutionReset := gen.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa0"),
	}
	version := int64(1234)
	nextEventID := int64(3)
	replicationState := &p.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	resp, err := s.CreateWorkflowExecutionWithReplication(domainID, workflowExecutionReset, "taskList", "wType", 20, 13, nextEventID, 0, 2, replicationState, nil)
	s.NoError(err)
	s.NotNil(resp)

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecutionReset)
	s.NoError(err)

	info := state.ExecutionInfo
	continueAsNewInfo := copyWorkflowExecutionInfo(info)
	continueAsNewStats := copyExecutionStats(state.ExecutionStats)
	continueAsNewInfo.State = p.WorkflowStateRunning
	continueAsNewInfo.NextEventID = int64(5)
	continueAsNewInfo.LastProcessedEvent = int64(2)

	workflowExecutionCurrent := gen.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1"),
	}
	err = s.ContinueAsNewExecutionWithReplication(continueAsNewInfo, continueAsNewStats, info.NextEventID, workflowExecutionCurrent, int64(3), int64(2), nil, replicationState, replicationState)
	s.NoError(err)

	runID1, err := s.GetCurrentWorkflowRunID(domainID, workflowID)
	s.Equal(workflowExecutionCurrent.GetRunId(), runID1)
	state, err = s.GetWorkflowExecutionInfo(domainID, workflowExecutionCurrent)
	s.NoError(err)
	currentInfo := copyWorkflowExecutionInfo(state.ExecutionInfo)
	currentStats := copyExecutionStats(state.ExecutionStats)
	currentState := copyReplicationState(state.ReplicationState)
	currentInfo.State = p.WorkflowStateCompleted
	currentInfo.CloseStatus = p.WorkflowCloseStatusCompleted
	currentInfo.NextEventID = int64(6)
	currentInfo.LastProcessedEvent = int64(2)
	err3 := s.UpdateWorkflowExecutionAndFinish(currentInfo, currentStats, int64(3))
	s.NoError(err3)
	runID1, err = s.GetCurrentWorkflowRunID(domainID, workflowID)
	s.Equal(workflowExecutionCurrent.GetRunId(), runID1)

	resetExecutionInfo := &p.WorkflowExecutionInfo{
		DomainID:             domainID,
		WorkflowID:           workflowExecutionReset.GetWorkflowId(),
		RunID:                workflowExecutionReset.GetRunId(),
		ParentDomainID:       uuid.New(),
		ParentWorkflowID:     "some random parent workflow ID",
		ParentRunID:          uuid.New(),
		InitiatedID:          12345,
		TaskList:             "some random tasklist",
		WorkflowTypeName:     "some random workflow type name",
		WorkflowTimeout:      1112,
		DecisionTimeoutValue: 14,
		State:                p.WorkflowStateRunning,
		LastFirstEventID:     common.FirstEventID,
		NextEventID:          123,
		CreateRequestID:      uuid.New(),
		DecisionVersion:      common.EmptyVersion,
		DecisionScheduleID:   111,
		DecisionStartedID:    222,
		DecisionRequestID:    uuid.New(),
		DecisionTimeout:      0,
	}
	resetStats := &p.ExecutionStats{}
	resetActivityInfos := []*p.ActivityInfo{}
	resetTimerInfos := []*p.TimerInfo{}
	resetChildExecutionInfos := []*p.ChildExecutionInfo{}
	resetRequestCancelInfos := []*p.RequestCancelInfo{}
	resetSignalInfos := []*p.SignalInfo{}
	rState := &p.ReplicationState{
		CurrentVersion: int64(8789),
		StartVersion:   int64(8780),
	}

	wrongPrevRunID := uuid.New()
	err = s.ConflictResolveWorkflowExecution(
		wrongPrevRunID, currentState.LastWriteVersion, currentInfo.State,
		resetExecutionInfo, resetStats, rState, continueAsNewInfo.NextEventID, resetActivityInfos, resetTimerInfos,
		resetChildExecutionInfos, resetRequestCancelInfos, resetSignalInfos, nil)
	s.NotNil(err)

	wrongLastWriteVersion := currentState.LastWriteVersion + 1
	err = s.ConflictResolveWorkflowExecution(
		workflowExecutionCurrent.GetRunId(), wrongLastWriteVersion, currentInfo.State,
		resetExecutionInfo, resetStats, rState, continueAsNewInfo.NextEventID, resetActivityInfos, resetTimerInfos,
		resetChildExecutionInfos, resetRequestCancelInfos, resetSignalInfos, nil)
	s.NotNil(err)

	wrongState := currentInfo.State + 1
	err = s.ConflictResolveWorkflowExecution(
		workflowExecutionCurrent.GetRunId(), currentState.LastWriteVersion, wrongState,
		resetExecutionInfo, resetStats, rState, continueAsNewInfo.NextEventID, resetActivityInfos, resetTimerInfos,
		resetChildExecutionInfos, resetRequestCancelInfos, resetSignalInfos, nil)
	s.NotNil(err)

	// this test only assert whether the current workflow execution record is reset
	runID, err := s.GetCurrentWorkflowRunID(domainID, workflowID)
	s.Equal(workflowExecutionCurrent.GetRunId(), runID)
}

// TestConflictResolveWorkflowExecutionWithTransactionCurrentIsNotSelf test
func (s *ExecutionManagerSuite) TestConflictResolveWorkflowExecutionWithTransactionCurrentIsNotSelf() {
	domainID := "4ca1faac-1a3a-47af-8e51-fdaa2b3d45b9"
	workflowID := "test-reset-mutable-state-test-with-transaction-current-is-not-self"

	// first create a workflow and continue as new it
	workflowExecutionReset := gen.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa0"),
	}
	version := int64(1234)
	nextEventID := int64(3)
	replicationState := &p.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	resp, err := s.CreateWorkflowExecutionWithReplication(domainID, workflowExecutionReset, "taskList", "wType", 20, 13, nextEventID, 0, 2, replicationState, nil)
	s.NoError(err)
	s.NotNil(resp)

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecutionReset)
	s.NoError(err)

	info := state.ExecutionInfo
	continueAsNewInfo := copyWorkflowExecutionInfo(info)
	continueAsNewStats := copyExecutionStats(state.ExecutionStats)
	continueAsNewInfo.State = p.WorkflowStateRunning
	continueAsNewInfo.NextEventID = int64(5)
	continueAsNewInfo.LastProcessedEvent = int64(2)

	workflowExecutionCurrent := gen.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1"),
	}
	err = s.ContinueAsNewExecutionWithReplication(continueAsNewInfo, continueAsNewStats, info.NextEventID, workflowExecutionCurrent, int64(3), int64(2), nil, replicationState, replicationState)
	s.NoError(err)

	currentRunID, err := s.GetCurrentWorkflowRunID(domainID, workflowID)
	s.Equal(workflowExecutionCurrent.GetRunId(), currentRunID)
	state, err = s.GetWorkflowExecutionInfo(domainID, workflowExecutionCurrent)
	s.NoError(err)
	currentInfo := copyWorkflowExecutionInfo(state.ExecutionInfo)
	currentState := copyReplicationState(state.ReplicationState)
	currentInfo.State = p.WorkflowStateCompleted
	currentInfo.CloseStatus = p.WorkflowCloseStatusTerminated

	resetExecutionInfo := &p.WorkflowExecutionInfo{
		DomainID:             domainID,
		WorkflowID:           workflowExecutionReset.GetWorkflowId(),
		RunID:                workflowExecutionReset.GetRunId(),
		ParentDomainID:       uuid.New(),
		ParentWorkflowID:     "some random parent workflow ID",
		ParentRunID:          uuid.New(),
		InitiatedID:          12345,
		TaskList:             "some random tasklist",
		WorkflowTypeName:     "some random workflow type name",
		WorkflowTimeout:      1112,
		DecisionTimeoutValue: 14,
		State:                p.WorkflowStateCompleted,
		CloseStatus:          p.WorkflowCloseStatusCompleted,
		LastFirstEventID:     common.FirstEventID,
		NextEventID:          123,
		CreateRequestID:      uuid.New(),
		DecisionVersion:      common.EmptyVersion,
		DecisionScheduleID:   111,
		DecisionStartedID:    222,
		DecisionRequestID:    uuid.New(),
		DecisionTimeout:      0,
		AutoResetPoints:      &gen.ResetPoints{},
	}
	resetReplicationState := &p.ReplicationState{
		CurrentVersion:      int64(8789),
		StartVersion:        int64(8780),
		LastWriteVersion:    int64(8912),
		LastReplicationInfo: map[string]*p.ReplicationInfo{},
	}

	resetReq := &p.ConflictResolveWorkflowExecutionRequest{
		RangeID: s.ShardInfo.RangeID,
		Mode:    p.ConflictResolveWorkflowModeBypassCurrent,
		ResetWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo:    resetExecutionInfo,
			ExecutionStats:   &p.ExecutionStats{},
			ReplicationState: resetReplicationState,
			Condition:        int64(5),

			ActivityInfos:       []*p.ActivityInfo{},
			TimerInfos:          []*p.TimerInfo{},
			ChildExecutionInfos: []*p.ChildExecutionInfo{},
			RequestCancelInfos:  []*p.RequestCancelInfo{},
			SignalInfos:         []*p.SignalInfo{},
			SignalRequestedIDs:  []string{},
		},
		NewWorkflowSnapshot: nil,
		CurrentWorkflowMutation: &p.WorkflowMutation{
			ExecutionInfo:    currentInfo,
			ExecutionStats:   &p.ExecutionStats{},
			ReplicationState: currentState,
			Condition:        int64(3),

			UpsertActivityInfos:       []*p.ActivityInfo{},
			UpsertTimerInfos:          []*p.TimerInfo{},
			UpsertChildExecutionInfos: []*p.ChildExecutionInfo{},
			UpsertRequestCancelInfos:  []*p.RequestCancelInfo{},
			UpsertSignalInfos:         []*p.SignalInfo{},
			UpsertSignalRequestedIDs:  []string{},
		},
		CurrentWorkflowCAS: nil,
		Encoding:           pickRandomEncoding(),
	}
	err = s.ExecutionManager.ConflictResolveWorkflowExecution(resetReq)
	s.Error(err)
	resetReq.Mode = p.ConflictResolveWorkflowModeUpdateCurrent
	err = s.ExecutionManager.ConflictResolveWorkflowExecution(resetReq)
	s.NoError(err)

	currentRecord, err := s.ExecutionManager.GetCurrentExecution(&p.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	})
	s.NoError(err)
	s.Equal(resetExecutionInfo.RunID, currentRecord.RunID)
	s.Equal(resetExecutionInfo.CreateRequestID, currentRecord.StartRequestID)
	s.Equal(resetExecutionInfo.State, currentRecord.State)
	s.Equal(resetExecutionInfo.CloseStatus, currentRecord.CloseStatus)
	s.Equal(resetReplicationState.LastWriteVersion, currentRecord.LastWriteVersion)

	state, err = s.GetWorkflowExecutionInfo(domainID, workflowExecutionReset)
	s.NoError(err)
	state.ExecutionInfo.StartTimestamp = resetReq.ResetWorkflowSnapshot.ExecutionInfo.StartTimestamp
	state.ExecutionInfo.LastUpdatedTimestamp = resetReq.ResetWorkflowSnapshot.ExecutionInfo.LastUpdatedTimestamp
	state.ExecutionInfo.ExpirationTime = resetReq.ResetWorkflowSnapshot.ExecutionInfo.ExpirationTime
	s.Equal(resetReq.ResetWorkflowSnapshot.ExecutionInfo, state.ExecutionInfo)
	s.Equal(resetReq.ResetWorkflowSnapshot.ReplicationState, state.ReplicationState)

	state, err = s.GetWorkflowExecutionInfo(domainID, workflowExecutionCurrent)
	s.NoError(err)
	state.ExecutionInfo.StartTimestamp = resetReq.CurrentWorkflowMutation.ExecutionInfo.StartTimestamp
	state.ExecutionInfo.LastUpdatedTimestamp = resetReq.CurrentWorkflowMutation.ExecutionInfo.LastUpdatedTimestamp
	state.ExecutionInfo.ExpirationTime = resetReq.CurrentWorkflowMutation.ExecutionInfo.ExpirationTime
	s.Equal(resetReq.CurrentWorkflowMutation.ExecutionInfo, state.ExecutionInfo)
	s.Equal(resetReq.CurrentWorkflowMutation.ReplicationState, state.ReplicationState)
}

// TestConflictResolveWorkflowExecutionWithTransactionCurrentIsNotSelfWithContinueAsNew test
func (s *ExecutionManagerSuite) TestConflictResolveWorkflowExecutionWithTransactionCurrentIsNotSelfWithContinueAsNew() {
	domainID := "4ca1faac-1a3a-47af-8e51-fdaa2b3d45b9"
	workflowID := "test-reset-mutable-state-test-with-transaction-current-is-not-self-with-continue-as-new"

	// first create a workflow and continue as new it
	workflowExecutionReset := gen.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa0"),
	}
	version := int64(1234)
	nextEventID := int64(3)
	replicationState := &p.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	resp, err := s.CreateWorkflowExecutionWithReplication(domainID, workflowExecutionReset, "taskList", "wType", 20, 13, nextEventID, 0, 2, replicationState, nil)
	s.NoError(err)
	s.NotNil(resp)

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecutionReset)
	s.NoError(err)
	continueAsNewStats := copyExecutionStats(state.ExecutionStats)

	info := state.ExecutionInfo
	continueAsNewInfo := copyWorkflowExecutionInfo(info)
	continueAsNewInfo.State = p.WorkflowStateRunning
	continueAsNewInfo.NextEventID = int64(5)
	continueAsNewInfo.LastProcessedEvent = int64(2)

	workflowExecutionCurrent := gen.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1"),
	}
	err = s.ContinueAsNewExecutionWithReplication(continueAsNewInfo, continueAsNewStats, info.NextEventID, workflowExecutionCurrent, int64(3), int64(2), nil, replicationState, replicationState)
	s.NoError(err)

	currentRunID, err := s.GetCurrentWorkflowRunID(domainID, workflowID)
	s.Equal(workflowExecutionCurrent.GetRunId(), currentRunID)
	state, err = s.GetWorkflowExecutionInfo(domainID, workflowExecutionCurrent)
	s.NoError(err)
	currentInfo := copyWorkflowExecutionInfo(state.ExecutionInfo)
	currentStats := copyExecutionStats(state.ExecutionStats)
	currentState := copyReplicationState(state.ReplicationState)
	currentInfo.State = p.WorkflowStateCompleted
	currentInfo.CloseStatus = p.WorkflowCloseStatusTerminated

	resetExecutionInfo := &p.WorkflowExecutionInfo{
		DomainID:             domainID,
		WorkflowID:           workflowExecutionReset.GetWorkflowId(),
		RunID:                workflowExecutionReset.GetRunId(),
		ParentDomainID:       uuid.New(),
		ParentWorkflowID:     "some random parent workflow ID",
		ParentRunID:          uuid.New(),
		InitiatedID:          12345,
		TaskList:             "some random tasklist",
		WorkflowTypeName:     "some random workflow type name",
		WorkflowTimeout:      1112,
		DecisionTimeoutValue: 14,
		State:                p.WorkflowStateCompleted,
		CloseStatus:          p.WorkflowCloseStatusContinuedAsNew,
		LastFirstEventID:     common.FirstEventID,
		NextEventID:          123,
		CreateRequestID:      uuid.New(),
		DecisionVersion:      common.EmptyVersion,
		DecisionScheduleID:   111,
		DecisionStartedID:    222,
		DecisionRequestID:    uuid.New(),
		DecisionTimeout:      0,
		AutoResetPoints:      &gen.ResetPoints{},
	}
	newWorkflowExecutionInfo := copyWorkflowExecutionInfo(resetExecutionInfo)
	newWorkflowExecutionStats := &p.ExecutionStats{}
	newWorkflowExecutionInfo.CreateRequestID = uuid.New()
	newWorkflowExecutionInfo.RunID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa2"
	newWorkflowExecutionInfo.State = p.WorkflowStateRunning
	newWorkflowExecutionInfo.CloseStatus = p.WorkflowCloseStatusNone
	newWorkflowExecutionState := &p.ReplicationState{
		CurrentVersion:      int64(8989),
		StartVersion:        int64(8980),
		LastWriteVersion:    int64(8912),
		LastReplicationInfo: map[string]*p.ReplicationInfo{},
	}

	resetReq := &p.ConflictResolveWorkflowExecutionRequest{
		RangeID: s.ShardInfo.RangeID,
		Mode:    p.ConflictResolveWorkflowModeBypassCurrent,
		ResetWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo:  resetExecutionInfo,
			ExecutionStats: &p.ExecutionStats{},
			ReplicationState: &p.ReplicationState{
				CurrentVersion:      int64(8789),
				StartVersion:        int64(8780),
				LastReplicationInfo: map[string]*p.ReplicationInfo{},
			},
			Condition: int64(5),

			ActivityInfos:       []*p.ActivityInfo{},
			TimerInfos:          []*p.TimerInfo{},
			ChildExecutionInfos: []*p.ChildExecutionInfo{},
			RequestCancelInfos:  []*p.RequestCancelInfo{},
			SignalInfos:         []*p.SignalInfo{},
			SignalRequestedIDs:  []string{},
		},
		NewWorkflowSnapshot: &p.WorkflowSnapshot{
			ExecutionInfo:    newWorkflowExecutionInfo,
			ExecutionStats:   newWorkflowExecutionStats,
			ReplicationState: newWorkflowExecutionState,
			Condition:        0,

			ActivityInfos:       []*p.ActivityInfo{},
			TimerInfos:          []*p.TimerInfo{},
			ChildExecutionInfos: []*p.ChildExecutionInfo{},
			RequestCancelInfos:  []*p.RequestCancelInfo{},
			SignalInfos:         []*p.SignalInfo{},
			SignalRequestedIDs:  []string{},
		},
		CurrentWorkflowMutation: &p.WorkflowMutation{
			ExecutionInfo:    currentInfo,
			ExecutionStats:   currentStats,
			ReplicationState: currentState,
			Condition:        int64(3),

			UpsertActivityInfos:       []*p.ActivityInfo{},
			UpsertTimerInfos:          []*p.TimerInfo{},
			UpsertChildExecutionInfos: []*p.ChildExecutionInfo{},
			UpsertRequestCancelInfos:  []*p.RequestCancelInfo{},
			UpsertSignalInfos:         []*p.SignalInfo{},
			UpsertSignalRequestedIDs:  []string{},
		},
		CurrentWorkflowCAS: nil,
		Encoding:           pickRandomEncoding(),
	}
	err = s.ExecutionManager.ConflictResolveWorkflowExecution(resetReq)
	s.Error(err)
	resetReq.Mode = p.ConflictResolveWorkflowModeUpdateCurrent
	err = s.ExecutionManager.ConflictResolveWorkflowExecution(resetReq)
	s.NoError(err)

	currentRecord, err := s.ExecutionManager.GetCurrentExecution(&p.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	})
	s.NoError(err)
	s.Equal(newWorkflowExecutionInfo.RunID, currentRecord.RunID)
	s.Equal(newWorkflowExecutionInfo.CreateRequestID, currentRecord.StartRequestID)
	s.Equal(newWorkflowExecutionInfo.State, currentRecord.State)
	s.Equal(newWorkflowExecutionInfo.CloseStatus, currentRecord.CloseStatus)
	s.Equal(newWorkflowExecutionState.LastWriteVersion, currentRecord.LastWriteVersion)

	state, err = s.GetWorkflowExecutionInfo(domainID, workflowExecutionReset)
	s.NoError(err)
	state.ExecutionInfo.StartTimestamp = resetReq.ResetWorkflowSnapshot.ExecutionInfo.StartTimestamp
	state.ExecutionInfo.LastUpdatedTimestamp = resetReq.ResetWorkflowSnapshot.ExecutionInfo.LastUpdatedTimestamp
	state.ExecutionInfo.ExpirationTime = resetReq.ResetWorkflowSnapshot.ExecutionInfo.ExpirationTime
	s.Equal(resetReq.ResetWorkflowSnapshot.ExecutionInfo, state.ExecutionInfo)
	s.Equal(resetReq.ResetWorkflowSnapshot.ReplicationState, state.ReplicationState)

	state, err = s.GetWorkflowExecutionInfo(domainID, workflowExecutionCurrent)
	s.NoError(err)
	state.ExecutionInfo.StartTimestamp = resetReq.CurrentWorkflowMutation.ExecutionInfo.StartTimestamp
	state.ExecutionInfo.LastUpdatedTimestamp = resetReq.CurrentWorkflowMutation.ExecutionInfo.LastUpdatedTimestamp
	state.ExecutionInfo.ExpirationTime = resetReq.CurrentWorkflowMutation.ExecutionInfo.ExpirationTime
	s.Equal(resetReq.CurrentWorkflowMutation.ExecutionInfo, state.ExecutionInfo)
	s.Equal(resetReq.CurrentWorkflowMutation.ReplicationState, state.ReplicationState)

	state, err = s.GetWorkflowExecutionInfo(domainID, gen.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(resetReq.NewWorkflowSnapshot.ExecutionInfo.RunID),
	})
	s.NoError(err)
	state.ExecutionInfo.StartTimestamp = resetReq.NewWorkflowSnapshot.ExecutionInfo.StartTimestamp
	state.ExecutionInfo.LastUpdatedTimestamp = resetReq.NewWorkflowSnapshot.ExecutionInfo.LastUpdatedTimestamp
	state.ExecutionInfo.ExpirationTime = resetReq.NewWorkflowSnapshot.ExecutionInfo.ExpirationTime
	s.Equal(resetReq.NewWorkflowSnapshot.ExecutionInfo, state.ExecutionInfo)
	s.Equal(resetReq.NewWorkflowSnapshot.ReplicationState, state.ReplicationState)
}

// TestConflictResolveWorkflowExecutionWithTransactionCurrentIsSelf test
func (s *ExecutionManagerSuite) TestConflictResolveWorkflowExecutionWithTransactionCurrentIsSelf() {
	domainID := "4ca1faac-1a3a-47af-8e51-fdaa2b3d45b9"
	workflowID := "test-reset-mutable-state-test-with-transaction-current-is-self"

	// first create a workflow and continue as new it
	workflowExecutionReset := gen.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa0"),
	}
	version := int64(1234)
	nextEventID := int64(3)
	replicationState := &p.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	resp, err := s.CreateWorkflowExecutionWithReplication(domainID, workflowExecutionReset, "taskList", "wType", 20, 13, nextEventID, 0, 2, replicationState, nil)
	s.NoError(err)
	s.NotNil(resp)

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecutionReset)
	s.NoError(err)

	resetExecutionInfo := &p.WorkflowExecutionInfo{
		DomainID:             domainID,
		WorkflowID:           workflowExecutionReset.GetWorkflowId(),
		RunID:                workflowExecutionReset.GetRunId(),
		ParentDomainID:       uuid.New(),
		ParentWorkflowID:     "some random parent workflow ID",
		ParentRunID:          uuid.New(),
		InitiatedID:          12345,
		TaskList:             "some random tasklist",
		WorkflowTypeName:     "some random workflow type name",
		WorkflowTimeout:      1112,
		DecisionTimeoutValue: 14,
		State:                p.WorkflowStateRunning,
		CloseStatus:          p.WorkflowCloseStatusNone,
		LastFirstEventID:     common.FirstEventID,
		NextEventID:          123,
		CreateRequestID:      uuid.New(),
		DecisionVersion:      common.EmptyVersion,
		DecisionScheduleID:   111,
		DecisionStartedID:    222,
		DecisionRequestID:    uuid.New(),
		DecisionTimeout:      0,
		AutoResetPoints:      &gen.ResetPoints{},
	}
	resetReplicationState := &p.ReplicationState{
		CurrentVersion:      int64(8789),
		StartVersion:        int64(8780),
		LastWriteVersion:    int64(8912),
		LastReplicationInfo: map[string]*p.ReplicationInfo{},
	}

	resetReq := &p.ConflictResolveWorkflowExecutionRequest{
		RangeID: s.ShardInfo.RangeID,
		Mode:    p.ConflictResolveWorkflowModeBypassCurrent,
		ResetWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo:    resetExecutionInfo,
			ExecutionStats:   &p.ExecutionStats{},
			ReplicationState: resetReplicationState,
			Condition:        nextEventID,

			ActivityInfos:       []*p.ActivityInfo{},
			TimerInfos:          []*p.TimerInfo{},
			ChildExecutionInfos: []*p.ChildExecutionInfo{},
			RequestCancelInfos:  []*p.RequestCancelInfo{},
			SignalInfos:         []*p.SignalInfo{},
			SignalRequestedIDs:  []string{},
		},
		NewWorkflowSnapshot:     nil,
		CurrentWorkflowMutation: nil,
		CurrentWorkflowCAS:      nil,
		Encoding:                pickRandomEncoding(),
	}
	err = s.ExecutionManager.ConflictResolveWorkflowExecution(resetReq)
	s.Error(err)
	resetReq.Mode = p.ConflictResolveWorkflowModeUpdateCurrent
	err = s.ExecutionManager.ConflictResolveWorkflowExecution(resetReq)
	s.NoError(err)

	currentRecord, err := s.ExecutionManager.GetCurrentExecution(&p.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	})
	s.NoError(err)
	s.Equal(resetExecutionInfo.RunID, currentRecord.RunID)
	s.Equal(resetExecutionInfo.CreateRequestID, currentRecord.StartRequestID)
	s.Equal(resetExecutionInfo.State, currentRecord.State)
	s.Equal(resetExecutionInfo.CloseStatus, currentRecord.CloseStatus)
	s.Equal(resetReplicationState.LastWriteVersion, currentRecord.LastWriteVersion)

	state, err = s.GetWorkflowExecutionInfo(domainID, workflowExecutionReset)
	s.NoError(err)
	state.ExecutionInfo.StartTimestamp = resetReq.ResetWorkflowSnapshot.ExecutionInfo.StartTimestamp
	state.ExecutionInfo.LastUpdatedTimestamp = resetReq.ResetWorkflowSnapshot.ExecutionInfo.LastUpdatedTimestamp
	state.ExecutionInfo.ExpirationTime = resetReq.ResetWorkflowSnapshot.ExecutionInfo.ExpirationTime
	s.Equal(resetReq.ResetWorkflowSnapshot.ExecutionInfo, state.ExecutionInfo)
	s.Equal(resetReq.ResetWorkflowSnapshot.ReplicationState, state.ReplicationState)
}

// TestConflictResolveWorkflowExecutionWithTransactionCurrentIsSelfWithContinueAsNew test
func (s *ExecutionManagerSuite) TestConflictResolveWorkflowExecutionWithTransactionCurrentIsSelfWithContinueAsNew() {
	domainID := "4ca1faac-1a3a-47af-8e51-fdaa2b3d45b9"
	workflowID := "test-reset-mutable-state-test-with-transaction-current-is-self-with-continue-as-new"

	// first create a workflow and continue as new it
	workflowExecutionReset := gen.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa0"),
	}
	version := int64(1234)
	nextEventID := int64(3)
	replicationState := &p.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	resp, err := s.CreateWorkflowExecutionWithReplication(domainID, workflowExecutionReset, "taskList", "wType", 20, 13, nextEventID, 0, 2, replicationState, nil)
	s.NoError(err)
	s.NotNil(resp)

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecutionReset)
	s.NoError(err)

	resetExecutionInfo := &p.WorkflowExecutionInfo{
		DomainID:             domainID,
		WorkflowID:           workflowExecutionReset.GetWorkflowId(),
		RunID:                workflowExecutionReset.GetRunId(),
		ParentDomainID:       uuid.New(),
		ParentWorkflowID:     "some random parent workflow ID",
		ParentRunID:          uuid.New(),
		InitiatedID:          12345,
		TaskList:             "some random tasklist",
		WorkflowTypeName:     "some random workflow type name",
		WorkflowTimeout:      1112,
		DecisionTimeoutValue: 14,
		State:                p.WorkflowStateCompleted,
		CloseStatus:          p.WorkflowCloseStatusContinuedAsNew,
		LastFirstEventID:     common.FirstEventID,
		NextEventID:          123,
		CreateRequestID:      uuid.New(),
		DecisionVersion:      common.EmptyVersion,
		DecisionScheduleID:   111,
		DecisionStartedID:    222,
		DecisionRequestID:    uuid.New(),
		DecisionTimeout:      0,
		AutoResetPoints:      &gen.ResetPoints{},
	}
	newWorkflowExecutionInfo := copyWorkflowExecutionInfo(resetExecutionInfo)
	newWorkflowExecutionStats := &p.ExecutionStats{}
	newWorkflowExecutionInfo.CreateRequestID = uuid.New()
	newWorkflowExecutionInfo.RunID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa2"
	newWorkflowExecutionInfo.State = p.WorkflowStateRunning
	newWorkflowExecutionInfo.CloseStatus = p.WorkflowCloseStatusNone
	newWorkflowExecutionState := &p.ReplicationState{
		CurrentVersion:      int64(8989),
		StartVersion:        int64(8980),
		LastWriteVersion:    int64(8912),
		LastReplicationInfo: map[string]*p.ReplicationInfo{},
	}

	resetReq := &p.ConflictResolveWorkflowExecutionRequest{
		RangeID: s.ShardInfo.RangeID,
		Mode:    p.ConflictResolveWorkflowModeBypassCurrent,
		ResetWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo:  resetExecutionInfo,
			ExecutionStats: &p.ExecutionStats{},
			ReplicationState: &p.ReplicationState{
				CurrentVersion:      int64(8789),
				StartVersion:        int64(8780),
				LastReplicationInfo: map[string]*p.ReplicationInfo{},
			},
			Condition: nextEventID,

			ActivityInfos:       []*p.ActivityInfo{},
			TimerInfos:          []*p.TimerInfo{},
			ChildExecutionInfos: []*p.ChildExecutionInfo{},
			RequestCancelInfos:  []*p.RequestCancelInfo{},
			SignalInfos:         []*p.SignalInfo{},
			SignalRequestedIDs:  []string{},
		},
		NewWorkflowSnapshot: &p.WorkflowSnapshot{
			ExecutionInfo:    newWorkflowExecutionInfo,
			ExecutionStats:   newWorkflowExecutionStats,
			ReplicationState: newWorkflowExecutionState,
			Condition:        0,

			ActivityInfos:       []*p.ActivityInfo{},
			TimerInfos:          []*p.TimerInfo{},
			ChildExecutionInfos: []*p.ChildExecutionInfo{},
			RequestCancelInfos:  []*p.RequestCancelInfo{},
			SignalInfos:         []*p.SignalInfo{},
			SignalRequestedIDs:  []string{},
		},
		CurrentWorkflowMutation: nil,
		CurrentWorkflowCAS:      nil,
		Encoding:                pickRandomEncoding(),
	}
	err = s.ExecutionManager.ConflictResolveWorkflowExecution(resetReq)
	s.Error(err)
	resetReq.Mode = p.ConflictResolveWorkflowModeUpdateCurrent
	err = s.ExecutionManager.ConflictResolveWorkflowExecution(resetReq)
	s.NoError(err)

	currentRecord, err := s.ExecutionManager.GetCurrentExecution(&p.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	})
	s.NoError(err)
	s.Equal(newWorkflowExecutionInfo.RunID, currentRecord.RunID)
	s.Equal(newWorkflowExecutionInfo.CreateRequestID, currentRecord.StartRequestID)
	s.Equal(newWorkflowExecutionInfo.State, currentRecord.State)
	s.Equal(newWorkflowExecutionInfo.CloseStatus, currentRecord.CloseStatus)
	s.Equal(newWorkflowExecutionState.LastWriteVersion, currentRecord.LastWriteVersion)

	state, err = s.GetWorkflowExecutionInfo(domainID, workflowExecutionReset)
	s.NoError(err)
	state.ExecutionInfo.StartTimestamp = resetReq.ResetWorkflowSnapshot.ExecutionInfo.StartTimestamp
	state.ExecutionInfo.LastUpdatedTimestamp = resetReq.ResetWorkflowSnapshot.ExecutionInfo.LastUpdatedTimestamp
	state.ExecutionInfo.ExpirationTime = resetReq.ResetWorkflowSnapshot.ExecutionInfo.ExpirationTime
	s.Equal(resetReq.ResetWorkflowSnapshot.ExecutionInfo, state.ExecutionInfo)
	s.Equal(resetReq.ResetWorkflowSnapshot.ReplicationState, state.ReplicationState)

	state, err = s.GetWorkflowExecutionInfo(domainID, gen.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(resetReq.NewWorkflowSnapshot.ExecutionInfo.RunID),
	})
	s.NoError(err)
	state.ExecutionInfo.StartTimestamp = resetReq.NewWorkflowSnapshot.ExecutionInfo.StartTimestamp
	state.ExecutionInfo.LastUpdatedTimestamp = resetReq.NewWorkflowSnapshot.ExecutionInfo.LastUpdatedTimestamp
	state.ExecutionInfo.ExpirationTime = resetReq.NewWorkflowSnapshot.ExecutionInfo.ExpirationTime
	s.Equal(resetReq.NewWorkflowSnapshot.ExecutionInfo, state.ExecutionInfo)
	s.Equal(resetReq.NewWorkflowSnapshot.ReplicationState, state.ReplicationState)
}

// TestConflictResolveWorkflowExecutionWithTransactionZombieIsSelf test
func (s *ExecutionManagerSuite) TestConflictResolveWorkflowExecutionWithTransactionZombieIsSelf() {
	domainID := "4ca1faac-1a3a-47af-8e51-fdaa2b3d45b9"
	workflowID := "test-reset-mutable-state-test-with-transaction-current-is-zombie"

	// first create a workflow and continue as new it
	workflowExecutionReset := gen.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa0"),
	}
	version := int64(1234)
	nextEventID := int64(3)
	replicationState := &p.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	resp, err := s.CreateWorkflowExecutionWithReplication(domainID, workflowExecutionReset, "taskList", "wType", 20, 13, nextEventID, 0, 2, replicationState, nil)
	s.NoError(err)
	s.NotNil(resp)

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecutionReset)
	s.NoError(err)

	info := state.ExecutionInfo
	continueAsNewInfo := copyWorkflowExecutionInfo(info)
	continueAsNewStats := copyExecutionStats(state.ExecutionStats)
	continueAsNewInfo.State = p.WorkflowStateRunning
	continueAsNewInfo.NextEventID = int64(5)
	continueAsNewInfo.LastProcessedEvent = int64(2)

	workflowExecutionCurrent := gen.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1"),
	}
	err = s.ContinueAsNewExecutionWithReplication(continueAsNewInfo, continueAsNewStats, info.NextEventID, workflowExecutionCurrent, int64(3), int64(2), nil, replicationState, replicationState)
	s.NoError(err)

	currentRunID, err := s.GetCurrentWorkflowRunID(domainID, workflowID)
	s.Equal(workflowExecutionCurrent.GetRunId(), currentRunID)

	resetExecutionInfo := &p.WorkflowExecutionInfo{
		DomainID:             domainID,
		WorkflowID:           workflowExecutionReset.GetWorkflowId(),
		RunID:                workflowExecutionReset.GetRunId(),
		ParentDomainID:       uuid.New(),
		ParentWorkflowID:     "some random parent workflow ID",
		ParentRunID:          uuid.New(),
		InitiatedID:          12345,
		TaskList:             "some random tasklist",
		WorkflowTypeName:     "some random workflow type name",
		WorkflowTimeout:      1112,
		DecisionTimeoutValue: 14,
		State:                p.WorkflowStateCompleted,
		CloseStatus:          p.WorkflowCloseStatusCompleted,
		LastFirstEventID:     common.FirstEventID,
		NextEventID:          123,
		CreateRequestID:      uuid.New(),
		DecisionVersion:      common.EmptyVersion,
		DecisionScheduleID:   111,
		DecisionStartedID:    222,
		DecisionRequestID:    uuid.New(),
		DecisionTimeout:      0,
		AutoResetPoints:      &gen.ResetPoints{},
	}
	resetReplicationState := &p.ReplicationState{
		CurrentVersion:      int64(8789),
		StartVersion:        int64(8780),
		LastWriteVersion:    int64(8912),
		LastReplicationInfo: map[string]*p.ReplicationInfo{},
	}

	resetReq := &p.ConflictResolveWorkflowExecutionRequest{
		RangeID: s.ShardInfo.RangeID,
		Mode:    p.ConflictResolveWorkflowModeUpdateCurrent,
		ResetWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo:    resetExecutionInfo,
			ExecutionStats:   &p.ExecutionStats{},
			ReplicationState: resetReplicationState,
			Condition:        int64(5),

			ActivityInfos:       []*p.ActivityInfo{},
			TimerInfos:          []*p.TimerInfo{},
			ChildExecutionInfos: []*p.ChildExecutionInfo{},
			RequestCancelInfos:  []*p.RequestCancelInfo{},
			SignalInfos:         []*p.SignalInfo{},
			SignalRequestedIDs:  []string{},
		},
		NewWorkflowSnapshot:     nil,
		CurrentWorkflowMutation: nil,
		CurrentWorkflowCAS:      nil,
		Encoding:                pickRandomEncoding(),
	}
	err = s.ExecutionManager.ConflictResolveWorkflowExecution(resetReq)
	s.Error(err)
	resetReq.Mode = p.ConflictResolveWorkflowModeBypassCurrent
	err = s.ExecutionManager.ConflictResolveWorkflowExecution(resetReq)
	s.NoError(err)

	currentRecord, err := s.ExecutionManager.GetCurrentExecution(&p.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	})
	s.NoError(err)
	s.Equal(currentRunID, currentRecord.RunID)

	state, err = s.GetWorkflowExecutionInfo(domainID, workflowExecutionReset)
	s.NoError(err)
	state.ExecutionInfo.StartTimestamp = resetReq.ResetWorkflowSnapshot.ExecutionInfo.StartTimestamp
	state.ExecutionInfo.LastUpdatedTimestamp = resetReq.ResetWorkflowSnapshot.ExecutionInfo.LastUpdatedTimestamp
	state.ExecutionInfo.ExpirationTime = resetReq.ResetWorkflowSnapshot.ExecutionInfo.ExpirationTime
	s.Equal(resetReq.ResetWorkflowSnapshot.ExecutionInfo, state.ExecutionInfo)
	s.Equal(resetReq.ResetWorkflowSnapshot.ReplicationState, state.ReplicationState)
}

// TestConflictResolveWorkflowExecutionWithTransactionZombieIsSelfWithContinueAsNew test
func (s *ExecutionManagerSuite) TestConflictResolveWorkflowExecutionWithTransactionZombieIsSelfWithContinueAsNew() {
	domainID := "4ca1faac-1a3a-47af-8e51-fdaa2b3d45b9"
	workflowID := "test-reset-mutable-state-test-with-transaction-current-is-zombie-with-continue-as-new"

	// first create a workflow and continue as new it
	workflowExecutionReset := gen.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa0"),
	}
	version := int64(1234)
	nextEventID := int64(3)
	replicationState := &p.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	resp, err := s.CreateWorkflowExecutionWithReplication(domainID, workflowExecutionReset, "taskList", "wType", 20, 13, nextEventID, 0, 2, replicationState, nil)
	s.NoError(err)
	s.NotNil(resp)

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecutionReset)
	s.NoError(err)
	continueAsNewStats := copyExecutionStats(state.ExecutionStats)

	info := state.ExecutionInfo
	continueAsNewInfo := copyWorkflowExecutionInfo(info)
	continueAsNewInfo.State = p.WorkflowStateRunning
	continueAsNewInfo.NextEventID = int64(5)
	continueAsNewInfo.LastProcessedEvent = int64(2)

	workflowExecutionCurrent := gen.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1"),
	}
	err = s.ContinueAsNewExecutionWithReplication(continueAsNewInfo, continueAsNewStats, info.NextEventID, workflowExecutionCurrent, int64(3), int64(2), nil, replicationState, replicationState)
	s.NoError(err)

	currentRunID, err := s.GetCurrentWorkflowRunID(domainID, workflowID)
	s.Equal(workflowExecutionCurrent.GetRunId(), currentRunID)

	resetExecutionInfo := &p.WorkflowExecutionInfo{
		DomainID:             domainID,
		WorkflowID:           workflowExecutionReset.GetWorkflowId(),
		RunID:                workflowExecutionReset.GetRunId(),
		ParentDomainID:       uuid.New(),
		ParentWorkflowID:     "some random parent workflow ID",
		ParentRunID:          uuid.New(),
		InitiatedID:          12345,
		TaskList:             "some random tasklist",
		WorkflowTypeName:     "some random workflow type name",
		WorkflowTimeout:      1112,
		DecisionTimeoutValue: 14,
		State:                p.WorkflowStateCompleted,
		CloseStatus:          p.WorkflowCloseStatusContinuedAsNew,
		LastFirstEventID:     common.FirstEventID,
		NextEventID:          123,
		CreateRequestID:      uuid.New(),
		DecisionVersion:      common.EmptyVersion,
		DecisionScheduleID:   111,
		DecisionStartedID:    222,
		DecisionRequestID:    uuid.New(),
		DecisionTimeout:      0,
		AutoResetPoints:      &gen.ResetPoints{},
	}
	newWorkflowExecutionInfo := copyWorkflowExecutionInfo(resetExecutionInfo)
	newWorkflowExecutionStats := &p.ExecutionStats{}
	newWorkflowExecutionInfo.CreateRequestID = uuid.New()
	newWorkflowExecutionInfo.RunID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa2"
	newWorkflowExecutionInfo.State = p.WorkflowStateZombie
	newWorkflowExecutionInfo.CloseStatus = p.WorkflowCloseStatusNone
	newWorkflowExecutionState := &p.ReplicationState{
		CurrentVersion:      int64(8989),
		StartVersion:        int64(8980),
		LastWriteVersion:    int64(8912),
		LastReplicationInfo: map[string]*p.ReplicationInfo{},
	}

	resetReq := &p.ConflictResolveWorkflowExecutionRequest{
		RangeID: s.ShardInfo.RangeID,
		Mode:    p.ConflictResolveWorkflowModeUpdateCurrent,
		ResetWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo:  resetExecutionInfo,
			ExecutionStats: &p.ExecutionStats{},
			ReplicationState: &p.ReplicationState{
				CurrentVersion:      int64(8789),
				StartVersion:        int64(8780),
				LastReplicationInfo: map[string]*p.ReplicationInfo{},
			},
			Condition: int64(5),

			ActivityInfos:       []*p.ActivityInfo{},
			TimerInfos:          []*p.TimerInfo{},
			ChildExecutionInfos: []*p.ChildExecutionInfo{},
			RequestCancelInfos:  []*p.RequestCancelInfo{},
			SignalInfos:         []*p.SignalInfo{},
			SignalRequestedIDs:  []string{},
		},
		NewWorkflowSnapshot: &p.WorkflowSnapshot{
			ExecutionInfo:    newWorkflowExecutionInfo,
			ExecutionStats:   newWorkflowExecutionStats,
			ReplicationState: newWorkflowExecutionState,
			Condition:        0,

			ActivityInfos:       []*p.ActivityInfo{},
			TimerInfos:          []*p.TimerInfo{},
			ChildExecutionInfos: []*p.ChildExecutionInfo{},
			RequestCancelInfos:  []*p.RequestCancelInfo{},
			SignalInfos:         []*p.SignalInfo{},
			SignalRequestedIDs:  []string{},
		},
		CurrentWorkflowMutation: nil,
		CurrentWorkflowCAS:      nil,
		Encoding:                pickRandomEncoding(),
	}
	err = s.ExecutionManager.ConflictResolveWorkflowExecution(resetReq)
	s.Error(err)
	resetReq.Mode = p.ConflictResolveWorkflowModeBypassCurrent
	err = s.ExecutionManager.ConflictResolveWorkflowExecution(resetReq)
	s.NoError(err)

	currentRecord, err := s.ExecutionManager.GetCurrentExecution(&p.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	})
	s.NoError(err)
	s.Equal(currentRunID, currentRecord.RunID)

	state, err = s.GetWorkflowExecutionInfo(domainID, workflowExecutionReset)
	s.NoError(err)
	state.ExecutionInfo.StartTimestamp = resetReq.ResetWorkflowSnapshot.ExecutionInfo.StartTimestamp
	state.ExecutionInfo.LastUpdatedTimestamp = resetReq.ResetWorkflowSnapshot.ExecutionInfo.LastUpdatedTimestamp
	state.ExecutionInfo.ExpirationTime = resetReq.ResetWorkflowSnapshot.ExecutionInfo.ExpirationTime
	s.Equal(resetReq.ResetWorkflowSnapshot.ExecutionInfo, state.ExecutionInfo)
	s.Equal(resetReq.ResetWorkflowSnapshot.ReplicationState, state.ReplicationState)

	state, err = s.GetWorkflowExecutionInfo(domainID, gen.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(resetReq.NewWorkflowSnapshot.ExecutionInfo.RunID),
	})
	s.NoError(err)
	state.ExecutionInfo.StartTimestamp = resetReq.NewWorkflowSnapshot.ExecutionInfo.StartTimestamp
	state.ExecutionInfo.LastUpdatedTimestamp = resetReq.NewWorkflowSnapshot.ExecutionInfo.LastUpdatedTimestamp
	state.ExecutionInfo.ExpirationTime = resetReq.NewWorkflowSnapshot.ExecutionInfo.ExpirationTime
	s.Equal(resetReq.NewWorkflowSnapshot.ExecutionInfo, state.ExecutionInfo)
	s.Equal(resetReq.NewWorkflowSnapshot.ReplicationState, state.ReplicationState)
}

// TestCreateGetShardBackfill test
func (s *ExecutionManagerSuite) TestCreateGetShardBackfill() {
	shardID := 4
	rangeID := int64(59)

	// test create && get
	currentReplicationAck := int64(27)
	currentClusterTransferAck := int64(21)
	currentClusterTimerAck := timestampConvertor(time.Now().Add(-10 * time.Second))
	shardInfo := &p.ShardInfo{
		ShardID:                 shardID,
		Owner:                   "some random owner",
		RangeID:                 rangeID,
		StolenSinceRenew:        12,
		UpdatedAt:               timestampConvertor(time.Now()),
		ReplicationAckLevel:     currentReplicationAck,
		TransferAckLevel:        currentClusterTransferAck,
		TimerAckLevel:           currentClusterTimerAck,
		ClusterReplicationLevel: map[string]int64{},
	}
	createRequest := &p.CreateShardRequest{
		ShardInfo: shardInfo,
	}
	s.Nil(s.ShardMgr.CreateShard(createRequest))

	shardInfo.ClusterTransferAckLevel = map[string]int64{
		s.ClusterMetadata.GetCurrentClusterName(): currentClusterTransferAck,
	}
	shardInfo.ClusterTimerAckLevel = map[string]time.Time{
		s.ClusterMetadata.GetCurrentClusterName(): currentClusterTimerAck,
	}
	resp, err := s.ShardMgr.GetShard(&p.GetShardRequest{ShardID: shardID})
	s.NoError(err)
	s.True(timeComparator(shardInfo.UpdatedAt, resp.ShardInfo.UpdatedAt, TimePrecision))
	s.True(timeComparator(shardInfo.ClusterTimerAckLevel[cluster.TestCurrentClusterName], resp.ShardInfo.ClusterTimerAckLevel[cluster.TestCurrentClusterName], TimePrecision))
	s.True(timeComparator(shardInfo.ClusterTimerAckLevel[cluster.TestAlternativeClusterName], resp.ShardInfo.ClusterTimerAckLevel[cluster.TestAlternativeClusterName], TimePrecision))
	s.Equal(shardInfo.TimerAckLevel.UnixNano(), resp.ShardInfo.TimerAckLevel.UnixNano())
	resp.ShardInfo.TimerAckLevel = shardInfo.TimerAckLevel
	resp.ShardInfo.UpdatedAt = shardInfo.UpdatedAt
	resp.ShardInfo.ClusterTimerAckLevel = shardInfo.ClusterTimerAckLevel
	s.Equal(shardInfo, resp.ShardInfo)
}

// TestCreateGetUpdateGetShard test
func (s *ExecutionManagerSuite) TestCreateGetUpdateGetShard() {
	shardID := 8
	rangeID := int64(59)

	// test create && get
	currentReplicationAck := int64(27)
	currentClusterTransferAck := int64(21)
	alternativeClusterTransferAck := int64(32)
	currentClusterTimerAck := timestampConvertor(time.Now().Add(-10 * time.Second))
	alternativeClusterTimerAck := timestampConvertor(time.Now().Add(-20 * time.Second))
	domainNotificationVersion := int64(8192)
	shardInfo := &p.ShardInfo{
		ShardID:             shardID,
		Owner:               "some random owner",
		RangeID:             rangeID,
		StolenSinceRenew:    12,
		UpdatedAt:           timestampConvertor(time.Now()),
		ReplicationAckLevel: currentReplicationAck,
		TransferAckLevel:    currentClusterTransferAck,
		TimerAckLevel:       currentClusterTimerAck,
		ClusterTransferAckLevel: map[string]int64{
			cluster.TestCurrentClusterName:     currentClusterTransferAck,
			cluster.TestAlternativeClusterName: alternativeClusterTransferAck,
		},
		ClusterTimerAckLevel: map[string]time.Time{
			cluster.TestCurrentClusterName:     currentClusterTimerAck,
			cluster.TestAlternativeClusterName: alternativeClusterTimerAck,
		},
		DomainNotificationVersion: domainNotificationVersion,
		ClusterReplicationLevel:   map[string]int64{},
	}
	createRequest := &p.CreateShardRequest{
		ShardInfo: shardInfo,
	}
	s.Nil(s.ShardMgr.CreateShard(createRequest))
	resp, err := s.ShardMgr.GetShard(&p.GetShardRequest{ShardID: shardID})
	s.NoError(err)
	s.True(timeComparator(shardInfo.UpdatedAt, resp.ShardInfo.UpdatedAt, TimePrecision))
	s.True(timeComparator(shardInfo.ClusterTimerAckLevel[cluster.TestCurrentClusterName], resp.ShardInfo.ClusterTimerAckLevel[cluster.TestCurrentClusterName], TimePrecision))
	s.True(timeComparator(shardInfo.ClusterTimerAckLevel[cluster.TestAlternativeClusterName], resp.ShardInfo.ClusterTimerAckLevel[cluster.TestAlternativeClusterName], TimePrecision))
	s.Equal(shardInfo.TimerAckLevel.UnixNano(), resp.ShardInfo.TimerAckLevel.UnixNano())
	resp.ShardInfo.TimerAckLevel = shardInfo.TimerAckLevel
	resp.ShardInfo.UpdatedAt = shardInfo.UpdatedAt
	resp.ShardInfo.ClusterTimerAckLevel = shardInfo.ClusterTimerAckLevel
	s.Equal(shardInfo, resp.ShardInfo)

	// test update && get
	currentReplicationAck = int64(270)
	currentClusterTransferAck = int64(210)
	alternativeClusterTransferAck = int64(320)
	currentClusterTimerAck = timestampConvertor(time.Now().Add(-100 * time.Second))
	alternativeClusterTimerAck = timestampConvertor(time.Now().Add(-200 * time.Second))
	domainNotificationVersion = int64(16384)
	shardInfo = &p.ShardInfo{
		ShardID:             shardID,
		Owner:               "some random owner",
		RangeID:             int64(28),
		StolenSinceRenew:    4,
		UpdatedAt:           timestampConvertor(time.Now()),
		ReplicationAckLevel: currentReplicationAck,
		TransferAckLevel:    currentClusterTransferAck,
		TimerAckLevel:       currentClusterTimerAck,
		ClusterTransferAckLevel: map[string]int64{
			cluster.TestCurrentClusterName:     currentClusterTransferAck,
			cluster.TestAlternativeClusterName: alternativeClusterTransferAck,
		},
		ClusterTimerAckLevel: map[string]time.Time{
			cluster.TestCurrentClusterName:     currentClusterTimerAck,
			cluster.TestAlternativeClusterName: alternativeClusterTimerAck,
		},
		DomainNotificationVersion: domainNotificationVersion,
		ClusterReplicationLevel:   map[string]int64{cluster.TestAlternativeClusterName: 12345},
	}
	updateRequest := &p.UpdateShardRequest{
		ShardInfo:       shardInfo,
		PreviousRangeID: rangeID,
	}
	s.Nil(s.ShardMgr.UpdateShard(updateRequest))

	resp, err = s.ShardMgr.GetShard(&p.GetShardRequest{ShardID: shardID})
	s.NoError(err)
	s.True(timeComparator(shardInfo.UpdatedAt, resp.ShardInfo.UpdatedAt, TimePrecision))
	s.True(timeComparator(shardInfo.ClusterTimerAckLevel[cluster.TestCurrentClusterName], resp.ShardInfo.ClusterTimerAckLevel[cluster.TestCurrentClusterName], TimePrecision))
	s.True(timeComparator(shardInfo.ClusterTimerAckLevel[cluster.TestAlternativeClusterName], resp.ShardInfo.ClusterTimerAckLevel[cluster.TestAlternativeClusterName], TimePrecision))
	s.Equal(shardInfo.TimerAckLevel.UnixNano(), resp.ShardInfo.TimerAckLevel.UnixNano())
	resp.ShardInfo.UpdatedAt = shardInfo.UpdatedAt
	resp.ShardInfo.TimerAckLevel = shardInfo.TimerAckLevel
	resp.ShardInfo.ClusterTimerAckLevel = shardInfo.ClusterTimerAckLevel
	s.Equal(shardInfo, resp.ShardInfo)
}

func copyWorkflowExecutionInfo(sourceInfo *p.WorkflowExecutionInfo) *p.WorkflowExecutionInfo {
	return &p.WorkflowExecutionInfo{
		DomainID:             sourceInfo.DomainID,
		WorkflowID:           sourceInfo.WorkflowID,
		RunID:                sourceInfo.RunID,
		ParentDomainID:       sourceInfo.ParentDomainID,
		ParentWorkflowID:     sourceInfo.ParentWorkflowID,
		ParentRunID:          sourceInfo.ParentRunID,
		InitiatedID:          sourceInfo.InitiatedID,
		CompletionEvent:      sourceInfo.CompletionEvent,
		TaskList:             sourceInfo.TaskList,
		WorkflowTypeName:     sourceInfo.WorkflowTypeName,
		WorkflowTimeout:      sourceInfo.WorkflowTimeout,
		DecisionTimeoutValue: sourceInfo.DecisionTimeoutValue,
		ExecutionContext:     sourceInfo.ExecutionContext,
		State:                sourceInfo.State,
		CloseStatus:          sourceInfo.CloseStatus,
		LastFirstEventID:     sourceInfo.LastFirstEventID,
		NextEventID:          sourceInfo.NextEventID,
		LastProcessedEvent:   sourceInfo.LastProcessedEvent,
		LastUpdatedTimestamp: sourceInfo.LastUpdatedTimestamp,
		CreateRequestID:      sourceInfo.CreateRequestID,
		DecisionVersion:      sourceInfo.DecisionVersion,
		DecisionScheduleID:   sourceInfo.DecisionScheduleID,
		DecisionStartedID:    sourceInfo.DecisionStartedID,
		DecisionRequestID:    sourceInfo.DecisionRequestID,
		DecisionTimeout:      sourceInfo.DecisionTimeout,
		BranchToken:          sourceInfo.BranchToken,
		AutoResetPoints:      sourceInfo.AutoResetPoints,
	}
}

func copyExecutionStats(sourceStats *p.ExecutionStats) *p.ExecutionStats {
	return &p.ExecutionStats{
		HistorySize: sourceStats.HistorySize,
	}
}

// Note: cassandra only provide millisecond precision timestamp
// ref: https://docs.datastax.com/en/cql/3.3/cql/cql_reference/timestamp_type_r.html
// so to use equal function, we need to do conversion, getting rid of sub milliseconds
func timestampConvertor(t time.Time) time.Time {
	return time.Unix(
		0,
		p.DBTimestampToUnixNano(p.UnixNanoToDBTimestamp(t.UnixNano())),
	).UTC()
}

func timeComparator(t1, t2 time.Time, timeTolerance time.Duration) bool {
	diff := t2.Sub(t1)
	if diff.Nanoseconds() <= timeTolerance.Nanoseconds() {
		return true
	}
	return false
}

func copyReplicationState(sourceState *p.ReplicationState) *p.ReplicationState {
	state := &p.ReplicationState{
		CurrentVersion:   sourceState.CurrentVersion,
		StartVersion:     sourceState.StartVersion,
		LastWriteVersion: sourceState.LastWriteVersion,
		LastWriteEventID: sourceState.LastWriteEventID,
	}
	if sourceState.LastReplicationInfo != nil {
		state.LastReplicationInfo = map[string]*p.ReplicationInfo{}
		for k, v := range sourceState.LastReplicationInfo {
			state.LastReplicationInfo[k] = copyReplicationInfo(v)
		}
	}

	return state
}

func copyReplicationInfo(sourceInfo *p.ReplicationInfo) *p.ReplicationInfo {
	return &p.ReplicationInfo{
		Version:     sourceInfo.Version,
		LastEventID: sourceInfo.LastEventID,
	}
}
