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

package persistence

import (
	"os"
	"testing"
	"time"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	gen "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cluster"
)

type (
	cassandraPersistenceSuite struct {
		suite.Suite
		TestBase
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
	}
)

// Cassandra only provides milliseconds timestamp precision, so
// we need to use tolerance when doing comparison
var timePrecision = 2 * time.Millisecond

func TestCassandraPersistenceSuite(t *testing.T) {
	s := new(cassandraPersistenceSuite)
	suite.Run(t, s)
}

func (s *cassandraPersistenceSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}

	s.SetupWorkflowStore()
}

func (s *cassandraPersistenceSuite) TearDownSuite() {
	s.TearDownWorkflowStore()
}

func (s *cassandraPersistenceSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
	s.ClearTasks()
}

func (s *cassandraPersistenceSuite) TestPersistenceStartWorkflow() {
	domainID := "2d7994bf-9de8-459d-9c81-e723daedb246"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("start-workflow-test"),
		RunId:      common.StringPtr("7f9fe8a0-9237-11e6-ae22-56b6b6499611"),
	}
	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "queue1", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotNil(task0, "Expected non empty task identifier.")

	task1, err1 := s.CreateWorkflowExecution(domainID, workflowExecution, "queue1", "wType1", 20, 14, nil, 3, 0, 2, nil)
	s.NotNil(err1, "Expected workflow creation to fail.")
	log.Infof("Unable to start workflow execution: %v", err1)
	startedErr, ok := err1.(*WorkflowExecutionAlreadyStartedError)
	s.True(ok)
	s.Equal(workflowExecution.GetRunId(), startedErr.RunID, startedErr.Msg)
	s.Equal(WorkflowStateRunning, startedErr.State, startedErr.Msg)
	s.Equal(WorkflowCloseStatusNone, startedErr.CloseStatus, startedErr.Msg)
	s.Equal(common.EmptyVersion, startedErr.StartVersion, startedErr.Msg)
	s.Empty(task1, "Expected empty task identifier.")

	response, err2 := s.WorkflowMgr.CreateWorkflowExecution(&CreateWorkflowExecutionRequest{
		RequestID:            uuid.New(),
		DomainID:             domainID,
		Execution:            workflowExecution,
		TaskList:             "queue1",
		WorkflowTypeName:     "workflow_type_test",
		WorkflowTimeout:      20,
		DecisionTimeoutValue: 13,
		ExecutionContext:     nil,
		NextEventID:          int64(3),
		LastProcessedEvent:   0,
		RangeID:              s.ShardInfo.RangeID - 1,
		TransferTasks: []Task{
			&DecisionTask{
				TaskID:     s.GetNextSequenceNumber(),
				DomainID:   domainID,
				TaskList:   "queue1",
				ScheduleID: int64(2),
			},
		},
		TimerTasks:                  nil,
		DecisionScheduleID:          int64(2),
		DecisionStartedID:           common.EmptyEventID,
		DecisionStartToCloseTimeout: 1,
	})

	s.NotNil(err2, "Expected workflow creation to fail.")
	s.Nil(response)
	log.Infof("Unable to start workflow execution: %v", err2)
	s.IsType(&ShardOwnershipLostError{}, err2)
}

func (s *cassandraPersistenceSuite) TestPersistenceStartWorkflowWithReplicationState() {
	domainID := "2d7994bf-9de8-459d-9c81-e723daedb246"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("start-workflow-test-replication-state"),
		RunId:      common.StringPtr("7f9fe8a0-9237-11e6-ae22-56b6b6499611"),
	}
	version := int64(144)
	replicationState := &ReplicationState{
		StartVersion:     version, // we are only testing this attribute
		CurrentVersion:   version,
		LastWriteVersion: version,
	}
	task0, err0 := s.CreateWorkflowExecutionWithReplication(domainID, workflowExecution, "queue1", "wType", 20, 13, 3, 0, 2, replicationState, nil)
	s.Nil(err0, "No error expected.")
	s.NotNil(task0, "Expected non empty task identifier.")

	task1, err1 := s.CreateWorkflowExecution(domainID, workflowExecution, "queue1", "wType1", 20, 14, nil, 3, 0, 2, nil)
	s.NotNil(err1, "Expected workflow creation to fail.")
	log.Infof("Unable to start workflow execution: %v", err1)
	startedErr, ok := err1.(*WorkflowExecutionAlreadyStartedError)
	s.True(ok)
	s.Equal(workflowExecution.GetRunId(), startedErr.RunID, startedErr.Msg)
	s.Equal(WorkflowStateRunning, startedErr.State, startedErr.Msg)
	s.Equal(WorkflowCloseStatusNone, startedErr.CloseStatus, startedErr.Msg)
	s.Equal(version, startedErr.StartVersion, startedErr.Msg)
	s.Empty(task1, "Expected empty task identifier.")

	response, err2 := s.WorkflowMgr.CreateWorkflowExecution(&CreateWorkflowExecutionRequest{
		RequestID:            uuid.New(),
		DomainID:             domainID,
		Execution:            workflowExecution,
		TaskList:             "queue1",
		WorkflowTypeName:     "workflow_type_test",
		WorkflowTimeout:      20,
		DecisionTimeoutValue: 13,
		ExecutionContext:     nil,
		NextEventID:          int64(3),
		LastProcessedEvent:   0,
		RangeID:              s.ShardInfo.RangeID - 1,
		TransferTasks: []Task{
			&DecisionTask{
				TaskID:     s.GetNextSequenceNumber(),
				DomainID:   domainID,
				TaskList:   "queue1",
				ScheduleID: int64(2),
			},
		},
		TimerTasks:                  nil,
		DecisionScheduleID:          int64(2),
		DecisionStartedID:           common.EmptyEventID,
		DecisionStartToCloseTimeout: 1,
	})

	s.NotNil(err2, "Expected workflow creation to fail.")
	s.Nil(response)
	log.Infof("Unable to start workflow execution: %v", err2)
	s.IsType(&ShardOwnershipLostError{}, err2)
}

func (s *cassandraPersistenceSuite) TestGetWorkflow() {
	domainID := "8f27f02b-ce22-4fd9-941b-65e1131b0bb5"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("get-workflow-test"),
		RunId:      common.StringPtr("918e7b1d-bfa4-4fe0-86cb-604858f90ce4"),
	}
	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "queue1", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotNil(task0, "Expected non empty task identifier.")

	state, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err1, "No error expected.")
	info := state.ExecutionInfo
	s.NotNil(info, "Valid Workflow response expected.")
	s.NotNil(info, "Valid Workflow info expected.")
	s.Equal(domainID, info.DomainID)
	s.Equal("get-workflow-test", info.WorkflowID)
	s.Equal("918e7b1d-bfa4-4fe0-86cb-604858f90ce4", info.RunID)
	s.Equal("queue1", info.TaskList)
	s.Equal("wType", info.WorkflowTypeName)
	s.Equal(int32(20), info.WorkflowTimeout)
	s.Equal(int32(13), info.DecisionTimeoutValue)
	s.Equal([]byte(nil), info.ExecutionContext)
	s.Equal(WorkflowStateCreated, info.State)
	s.Equal(int64(3), info.NextEventID)
	s.Equal(int64(0), info.LastProcessedEvent)
	s.Equal(true, validateTimeRange(info.LastUpdatedTimestamp, time.Hour))
	s.Equal(int64(2), info.DecisionScheduleID)
	s.Equal(common.EmptyEventID, info.DecisionStartedID)
	s.Equal(int32(1), info.DecisionTimeout)
	log.Infof("Workflow execution last updated: %v", info.LastUpdatedTimestamp)
}

func (s *cassandraPersistenceSuite) TestUpdateWorkflow() {
	domainID := "b0a8571c-0257-40ea-afcd-3a14eae181c0"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("update-workflow-test"),
		RunId:      common.StringPtr("5ba5e531-e46b-48d9-b4b3-859919839553"),
	}
	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "queue1", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err1, "No error expected.")
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
	s.Equal(WorkflowStateCreated, info0.State)
	s.Equal(int64(1), info0.LastFirstEventID)
	s.Equal(int64(3), info0.NextEventID)
	s.Equal(int64(0), info0.LastProcessedEvent)
	s.Equal(true, validateTimeRange(info0.LastUpdatedTimestamp, time.Hour))
	s.Equal(int64(0), info0.DecisionVersion)
	s.Equal(int64(2), info0.DecisionScheduleID)
	s.Equal(common.EmptyEventID, info0.DecisionStartedID)
	s.Equal(int32(1), info0.DecisionTimeout)
	s.Equal(int64(0), info0.DecisionAttempt)
	s.Equal(int64(0), info0.DecisionTimestamp)
	s.Empty(info0.StickyTaskList)
	s.Equal(int32(0), info0.StickyScheduleToStartTimeout)
	s.Empty(info0.ClientLibraryVersion)
	s.Empty(info0.ClientFeatureVersion)
	s.Empty(info0.ClientImpl)

	log.Infof("Workflow execution last updated: %v", info0.LastUpdatedTimestamp)

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedInfo.LastFirstEventID = int64(3)
	updatedInfo.NextEventID = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)
	updatedInfo.DecisionVersion = int64(666)
	updatedInfo.DecisionAttempt = int64(123)
	updatedInfo.DecisionTimestamp = int64(321)
	updatedInfo.StickyTaskList = "random sticky tasklist"
	updatedInfo.StickyScheduleToStartTimeout = 876
	updatedInfo.ClientLibraryVersion = "random client library version"
	updatedInfo.ClientFeatureVersion = "random client feature version"
	updatedInfo.ClientImpl = "random client impl"
	err2 := s.UpdateWorkflowExecution(updatedInfo, []int64{int64(4)}, nil, int64(3), nil, nil, nil, nil, nil, nil)
	s.Nil(err2, "No error expected.")

	state1, err3 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err3, "No error expected.")
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
	s.Equal(WorkflowStateCreated, info1.State)
	s.Equal(int64(3), info1.LastFirstEventID)
	s.Equal(int64(5), info1.NextEventID)
	s.Equal(int64(2), info1.LastProcessedEvent)
	s.Equal(true, validateTimeRange(info1.LastUpdatedTimestamp, time.Hour))
	s.Equal(int64(666), info1.DecisionVersion)
	s.Equal(int64(2), info1.DecisionScheduleID)
	s.Equal(common.EmptyEventID, info1.DecisionStartedID)
	s.Equal(int32(1), info1.DecisionTimeout)
	s.Equal(int64(123), info1.DecisionAttempt)
	s.Equal(int64(321), info1.DecisionTimestamp)
	s.Equal(updatedInfo.StickyTaskList, info1.StickyTaskList)
	s.Equal(updatedInfo.StickyScheduleToStartTimeout, info1.StickyScheduleToStartTimeout)
	s.Equal(updatedInfo.ClientLibraryVersion, info1.ClientLibraryVersion)
	s.Equal(updatedInfo.ClientFeatureVersion, info1.ClientFeatureVersion)
	s.Equal(updatedInfo.ClientImpl, info1.ClientImpl)

	log.Infof("Workflow execution last updated: %v", info1.LastUpdatedTimestamp)

	failedUpdatedInfo := copyWorkflowExecutionInfo(info0)
	failedUpdatedInfo.NextEventID = int64(6)
	failedUpdatedInfo.LastProcessedEvent = int64(3)
	failedUpdatedInfo.DecisionAttempt = int64(555)
	failedUpdatedInfo.DecisionTimestamp = int64(55)
	err4 := s.UpdateWorkflowExecution(updatedInfo, []int64{int64(5)}, nil, int64(3), nil, nil, nil, nil, nil, nil)
	s.NotNil(err4, "expected non nil error.")
	s.IsType(&ConditionFailedError{}, err4)
	log.Errorf("Conditional update failed with error: %v", err4)

	state2, err4 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err4, "No error expected.")
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
	s.Equal(WorkflowStateCreated, info2.State)
	s.Equal(int64(5), info2.NextEventID)
	s.Equal(int64(2), info2.LastProcessedEvent)
	s.Equal(true, validateTimeRange(info2.LastUpdatedTimestamp, time.Hour))
	s.Equal(int64(666), info2.DecisionVersion)
	s.Equal(int64(2), info2.DecisionScheduleID)
	s.Equal(common.EmptyEventID, info2.DecisionStartedID)
	s.Equal(int32(1), info2.DecisionTimeout)
	s.Equal(int64(123), info1.DecisionAttempt)
	s.Equal(int64(321), info1.DecisionTimestamp)

	log.Infof("Workflow execution last updated: %v", info2.LastUpdatedTimestamp)

	failedUpdatedInfo2 := copyWorkflowExecutionInfo(info1)
	failedUpdatedInfo2.NextEventID = int64(6)
	failedUpdatedInfo2.LastProcessedEvent = int64(3)
	failedUpdatedInfo.DecisionAttempt = int64(666)
	failedUpdatedInfo.DecisionTimestamp = int64(66)
	err5 := s.UpdateWorkflowExecutionWithRangeID(updatedInfo, []int64{int64(5)}, nil, int64(12345), int64(5), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, "")
	s.NotNil(err5, "expected non nil error.")
	s.IsType(&ShardOwnershipLostError{}, err5)
	log.Errorf("Conditional update failed with error: %v", err5)

	state3, err6 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err6, "No error expected.")
	info3 := state3.ExecutionInfo
	s.NotNil(info3, "Valid Workflow info expected.")
	s.Equal(domainID, info3.DomainID)
	s.Equal("update-workflow-test", info3.WorkflowID)
	s.Equal("5ba5e531-e46b-48d9-b4b3-859919839553", info3.RunID)
	s.Equal("queue1", info3.TaskList)
	s.Equal("wType", info3.WorkflowTypeName)
	s.Equal(int32(20), info1.WorkflowTimeout)
	s.Equal(int32(13), info3.DecisionTimeoutValue)
	s.Equal([]byte(nil), info3.ExecutionContext)
	s.Equal(WorkflowStateCreated, info3.State)
	s.Equal(int64(5), info3.NextEventID)
	s.Equal(int64(2), info3.LastProcessedEvent)
	s.Equal(true, validateTimeRange(info3.LastUpdatedTimestamp, time.Hour))
	s.Equal(int64(666), info3.DecisionVersion)
	s.Equal(int64(2), info3.DecisionScheduleID)
	s.Equal(common.EmptyEventID, info3.DecisionStartedID)
	s.Equal(int32(1), info1.DecisionTimeout)
	s.Equal(int64(123), info1.DecisionAttempt)
	s.Equal(int64(321), info1.DecisionTimestamp)

	log.Infof("Workflow execution last updated: %v", info3.LastUpdatedTimestamp)
}

func (s *cassandraPersistenceSuite) TestDeleteWorkflow() {
	domainID := "1d4abb23-b87b-457b-96ef-43aba0b9c44f"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("delete-workflow-test"),
		RunId:      common.StringPtr("4e0917f2-9361-4a14-b16f-1fafe09b287a"),
	}
	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "queue1", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err1, "No error expected.")
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
	s.Equal(WorkflowStateCreated, info0.State)
	s.Equal(int64(3), info0.NextEventID)
	s.Equal(int64(0), info0.LastProcessedEvent)
	s.Equal(true, validateTimeRange(info0.LastUpdatedTimestamp, time.Hour))
	s.Equal(int64(2), info0.DecisionScheduleID)
	s.Equal(common.EmptyEventID, info0.DecisionStartedID)
	s.Equal(int32(1), info0.DecisionTimeout)

	log.Infof("Workflow execution last updated: %v", info0.LastUpdatedTimestamp)

	err4 := s.DeleteWorkflowExecution(info0)
	s.Nil(err4, "No error expected.")

	_, err3 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NotNil(err3, "expected non nil error.")
	s.IsType(&gen.EntityNotExistsError{}, err3)

	err5 := s.DeleteWorkflowExecution(info0)
	s.Nil(err5)
}

func (s *cassandraPersistenceSuite) TestDeleteCurrentWorkflow() {
	finishedCurrentExecutionRetentionTTL := int32(3) // 3 seconds
	domainID := "54d15308-e20e-4b91-a00f-a518a3892790"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("get-current-workflow-test"),
		RunId:      common.StringPtr("6cae4054-6ba7-46d3-8755-e3c2db6f74ea"),
	}

	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "queue1", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotNil(task0, "Expected non empty task identifier.")

	runID0, err1 := s.GetCurrentWorkflowRunID(domainID, *workflowExecution.WorkflowId)
	s.Nil(err1, "No error expected.")
	s.Equal(*workflowExecution.RunId, runID0)

	info0, err2 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err2)

	updatedInfo1 := copyWorkflowExecutionInfo(info0.ExecutionInfo)
	updatedInfo1.NextEventID = int64(6)
	updatedInfo1.LastProcessedEvent = int64(2)
	err3 := s.UpdateWorkflowExecutionAndFinish(updatedInfo1, int64(3), finishedCurrentExecutionRetentionTTL)
	s.Nil(err3, "No error expected.")

	runID4, err4 := s.GetCurrentWorkflowRunID(domainID, *workflowExecution.WorkflowId)
	s.Nil(err4, "No error expected.")
	s.Equal(*workflowExecution.RunId, runID4)

	time.Sleep(time.Duration(finishedCurrentExecutionRetentionTTL*2) * time.Second)

	runID0, err1 = s.GetCurrentWorkflowRunID(domainID, *workflowExecution.WorkflowId)
	s.NotNil(err1)
	s.Empty(runID0)
	_, ok := err1.(*gen.EntityNotExistsError)
	s.True(ok)

	// execution record should still be there
	info0, err2 = s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err2)
}

func (s *cassandraPersistenceSuite) TestGetCurrentWorkflow() {
	domainID := "54d15308-e20e-4b91-a00f-a518a3892790"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("get-current-workflow-test"),
		RunId:      common.StringPtr("6cae4054-6ba7-46d3-8755-e3c2db6f74ea"),
	}

	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "queue1", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotNil(task0, "Expected non empty task identifier.")

	runID0, err1 := s.GetCurrentWorkflowRunID(domainID, *workflowExecution.WorkflowId)
	s.Nil(err1, "No error expected.")
	s.Equal(*workflowExecution.RunId, runID0)

	info0, err2 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err2)

	updatedInfo1 := copyWorkflowExecutionInfo(info0.ExecutionInfo)
	updatedInfo1.NextEventID = int64(6)
	updatedInfo1.LastProcessedEvent = int64(2)
	err3 := s.UpdateWorkflowExecutionAndFinish(updatedInfo1, int64(3), 10)
	s.Nil(err3, "No error expected.")

	runID4, err4 := s.GetCurrentWorkflowRunID(domainID, *workflowExecution.WorkflowId)
	s.Nil(err4, "No error expected.")
	s.Equal(*workflowExecution.RunId, runID4)

	workflowExecution2 := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("get-current-workflow-test"),
		RunId:      common.StringPtr("c3ff4bc6-de18-4643-83b2-037a33f45322"),
	}

	task1, err5 := s.CreateWorkflowExecution(domainID, workflowExecution2, "queue1", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.NotNil(err5, "Error expected.")
	s.Empty(task1, "Expected empty task identifier.")
}

func (s *cassandraPersistenceSuite) TestTransferTasksThroughUpdate() {
	domainID := "b785a8ba-bd7d-4760-bb05-41b115f3e10a"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("get-transfer-tasks-through-update-test"),
		RunId:      common.StringPtr("30a9fa1f-0db1-4d7a-8c34-aa82c5dad3aa"),
	}

	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "queue1", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotNil(task0, "Expected non empty task identifier.")

	tasks1, err1 := s.GetTransferTasks(1, false)
	s.Nil(err1, "No error expected.")
	s.NotNil(tasks1, "expected valid list of tasks.")
	s.Equal(1, len(tasks1), "Expected 1 decision task.")
	task1 := tasks1[0]
	s.Equal(domainID, task1.DomainID)
	s.Equal(*workflowExecution.WorkflowId, task1.WorkflowID)
	s.Equal(*workflowExecution.RunId, task1.RunID)
	s.Equal("queue1", task1.TaskList)
	s.Equal(TransferTaskTypeDecisionTask, task1.TaskType)
	s.Equal(int64(2), task1.ScheduleID)
	s.Equal("", task1.TargetRunID)

	err3 := s.CompleteTransferTask(task1.TaskID)
	s.Nil(err3)

	state0, _ := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	info0 := state0.ExecutionInfo
	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedInfo.NextEventID = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)
	err2 := s.UpdateWorkflowExecution(updatedInfo, nil, []int64{int64(4)}, int64(3), nil, nil, nil, nil, nil, nil)
	s.Nil(err2, "No error expected.")

	tasks2, err1 := s.GetTransferTasks(1, false)
	s.Nil(err1, "No error expected.")
	s.NotNil(tasks2, "expected valid list of tasks.")
	s.Equal(1, len(tasks2), "Expected 1 decision task.")
	task2 := tasks2[0]
	s.Equal(domainID, task2.DomainID)
	s.Equal(*workflowExecution.WorkflowId, task2.WorkflowID)
	s.Equal(*workflowExecution.RunId, task2.RunID)
	s.Equal("queue1", task2.TaskList)
	s.Equal(TransferTaskTypeActivityTask, task2.TaskType)
	s.Equal(int64(4), task2.ScheduleID)
	s.Equal("", task2.TargetRunID)

	err4 := s.CompleteTransferTask(task2.TaskID)
	s.Nil(err4)

	state1, _ := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	info1 := state1.ExecutionInfo
	updatedInfo1 := copyWorkflowExecutionInfo(info1)
	updatedInfo1.NextEventID = int64(6)
	updatedInfo1.LastProcessedEvent = int64(2)
	err5 := s.UpdateWorkflowExecutionAndFinish(updatedInfo1, int64(5), 10)
	s.Nil(err5, "No error expected.")

	newExecution := gen.WorkflowExecution{
		WorkflowId: workflowExecution.WorkflowId,
		RunId:      common.StringPtr("2a038c8f-b575-4151-8d2c-d443e999ab5a"),
	}
	runID6, err6 := s.GetCurrentWorkflowRunID(domainID, newExecution.GetWorkflowId())
	s.Nil(err6)
	s.Equal(*workflowExecution.RunId, runID6)

	tasks3, err7 := s.GetTransferTasks(1, false)
	s.Nil(err7, "No error expected.")
	s.NotNil(tasks3, "expected valid list of tasks.")
	s.Equal(1, len(tasks3), "Expected 1 decision task.")
	task3 := tasks3[0]
	s.Equal(domainID, task3.DomainID)
	s.Equal(*workflowExecution.WorkflowId, task3.WorkflowID)
	s.Equal(*workflowExecution.RunId, task3.RunID)
	s.Equal(TransferTaskTypeCloseExecution, task3.TaskType)
	s.Equal("", task3.TargetRunID)

	err8 := s.DeleteWorkflowExecution(info1)
	s.Nil(err8)

	err9 := s.CompleteTransferTask(task3.TaskID)
	s.Nil(err9)

	_, err10 := s.CreateWorkflowExecution(domainID, newExecution, "queue1", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.NotNil(err10, "Error expected.")
}

func (s *cassandraPersistenceSuite) TestCancelTransferTaskTasks() {
	domainID := "aeac8287-527b-4b35-80a9-667cb47e7c6d"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("cancel-workflow-test"),
		RunId:      common.StringPtr("db20f7e2-1a1e-40d9-9278-d8b886738e05"),
	}

	task0, err := s.CreateWorkflowExecution(domainID, workflowExecution, "queue1", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.Nil(err, "No error expected.")
	s.NotNil(task0, "Expected non empty task identifier.")

	taskD, err := s.GetTransferTasks(1, false)
	s.Equal(1, len(taskD), "Expected 1 decision task.")
	err = s.CompleteTransferTask(taskD[0].TaskID)
	s.Nil(err)

	state1, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err, "No error expected.")
	info1 := state1.ExecutionInfo
	s.NotNil(info1, "Valid Workflow info expected.")
	updatedInfo1 := copyWorkflowExecutionInfo(info1)

	targetDomainID := "f2bfaab6-7e8b-4fac-9a62-17da8d37becb"
	targetWorkflowID := "target-workflow-cancellation-id-1"
	targetRunID := "0d00698f-08e1-4d36-a3e2-3bf109f5d2d6"
	targetChildWorkflowOnly := false
	transferTasks := []Task{&CancelExecutionTask{
		TaskID:                  s.GetNextSequenceNumber(),
		TargetDomainID:          targetDomainID,
		TargetWorkflowID:        targetWorkflowID,
		TargetRunID:             targetRunID,
		TargetChildWorkflowOnly: targetChildWorkflowOnly,
		InitiatedID:             1,
	}}
	err = s.UpdateWorkflowExecutionWithTransferTasks(updatedInfo1, int64(3), transferTasks, nil)
	s.Nil(err, "No error expected.")

	tasks1, err := s.GetTransferTasks(1, false)
	s.Nil(err, "No error expected.")
	s.NotNil(tasks1, "expected valid list of tasks.")
	s.Equal(1, len(tasks1), "Expected 1 cancel task.")
	task1 := tasks1[0]
	s.Equal(TransferTaskTypeCancelExecution, task1.TaskType)
	s.Equal(domainID, task1.DomainID)
	s.Equal(*workflowExecution.WorkflowId, task1.WorkflowID)
	s.Equal(*workflowExecution.RunId, task1.RunID)
	s.Equal(targetDomainID, task1.TargetDomainID)
	s.Equal(targetWorkflowID, task1.TargetWorkflowID)
	s.Equal(targetRunID, task1.TargetRunID)
	s.Equal(targetChildWorkflowOnly, task1.TargetChildWorkflowOnly)

	err = s.CompleteTransferTask(task1.TaskID)
	s.Nil(err)

	targetDomainID = "f2bfaab6-7e8b-4fac-9a62-17da8d37becb"
	targetWorkflowID = "target-workflow-cancellation-id-2"
	targetRunID = ""
	targetChildWorkflowOnly = true
	transferTasks = []Task{&CancelExecutionTask{
		TaskID:                  s.GetNextSequenceNumber(),
		TargetDomainID:          targetDomainID,
		TargetWorkflowID:        targetWorkflowID,
		TargetRunID:             targetRunID,
		TargetChildWorkflowOnly: targetChildWorkflowOnly,
		InitiatedID:             3,
	}}

	state2, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err, "No error expected.")
	info2 := state2.ExecutionInfo
	s.NotNil(info2, "Valid Workflow info expected.")
	updatedInfo2 := copyWorkflowExecutionInfo(info2)

	err = s.UpdateWorkflowExecutionWithTransferTasks(updatedInfo2, int64(3), transferTasks, nil)
	s.Nil(err, "No error expected.")

	tasks2, err := s.GetTransferTasks(1, false)
	s.Nil(err, "No error expected.")
	s.NotNil(tasks2, "expected valid list of tasks.")
	s.Equal(1, len(tasks2), "Expected 1 cancel task.")
	task2 := tasks2[0]
	s.Equal(TransferTaskTypeCancelExecution, task2.TaskType)
	s.Equal(domainID, task2.DomainID)
	s.Equal(*workflowExecution.WorkflowId, task2.WorkflowID)
	s.Equal(*workflowExecution.RunId, task2.RunID)
	s.Equal(targetDomainID, task2.TargetDomainID)
	s.Equal(targetWorkflowID, task2.TargetWorkflowID)
	s.Equal(targetRunID, task2.TargetRunID)
	s.Equal(targetChildWorkflowOnly, task2.TargetChildWorkflowOnly)

	err = s.CompleteTransferTask(task2.TaskID)
	s.Nil(err)
}

func (s *cassandraPersistenceSuite) TestSignalTransferTaskTasks() {
	domainID := "aeac8287-527b-4b35-80a9-667cb47e7c6d"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("signal-workflow-test"),
		RunId:      common.StringPtr("db20f7e2-1a1e-40d9-9278-d8b886738e05"),
	}

	task0, err := s.CreateWorkflowExecution(domainID, workflowExecution, "queue1", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.Nil(err, "No error expected.")
	s.NotNil(task0, "Expected non empty task identifier.")

	taskD, err := s.GetTransferTasks(1, false)
	s.Equal(1, len(taskD), "Expected 1 decision task.")
	err = s.CompleteTransferTask(taskD[0].TaskID)
	s.Nil(err)

	state1, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err, "No error expected.")
	info1 := state1.ExecutionInfo
	s.NotNil(info1, "Valid Workflow info expected.")
	updatedInfo1 := copyWorkflowExecutionInfo(info1)

	targetDomainID := "f2bfaab6-7e8b-4fac-9a62-17da8d37becb"
	targetWorkflowID := "target-workflow-signal-id-1"
	targetRunID := "0d00698f-08e1-4d36-a3e2-3bf109f5d2d6"
	targetChildWorkflowOnly := false
	transferTasks := []Task{&SignalExecutionTask{
		TaskID:                  s.GetNextSequenceNumber(),
		TargetDomainID:          targetDomainID,
		TargetWorkflowID:        targetWorkflowID,
		TargetRunID:             targetRunID,
		TargetChildWorkflowOnly: targetChildWorkflowOnly,
		InitiatedID:             1,
	}}
	err = s.UpdateWorkflowExecutionWithTransferTasks(updatedInfo1, int64(3), transferTasks, nil)
	s.Nil(err, "No error expected.")

	tasks1, err := s.GetTransferTasks(1, false)
	s.Nil(err, "No error expected.")
	s.NotNil(tasks1, "expected valid list of tasks.")
	s.Equal(1, len(tasks1), "Expected 1 cancel task.")
	task1 := tasks1[0]
	s.Equal(TransferTaskTypeSignalExecution, task1.TaskType)
	s.Equal(domainID, task1.DomainID)
	s.Equal(*workflowExecution.WorkflowId, task1.WorkflowID)
	s.Equal(*workflowExecution.RunId, task1.RunID)
	s.Equal(targetDomainID, task1.TargetDomainID)
	s.Equal(targetWorkflowID, task1.TargetWorkflowID)
	s.Equal(targetRunID, task1.TargetRunID)
	s.Equal(targetChildWorkflowOnly, task1.TargetChildWorkflowOnly)

	err = s.CompleteTransferTask(task1.TaskID)
	s.Nil(err)

	targetDomainID = "f2bfaab6-7e8b-4fac-9a62-17da8d37becb"
	targetWorkflowID = "target-workflow-signal-id-2"
	targetRunID = ""
	targetChildWorkflowOnly = true
	transferTasks = []Task{&SignalExecutionTask{
		TaskID:                  s.GetNextSequenceNumber(),
		TargetDomainID:          targetDomainID,
		TargetWorkflowID:        targetWorkflowID,
		TargetRunID:             targetRunID,
		TargetChildWorkflowOnly: targetChildWorkflowOnly,
		InitiatedID:             3,
	}}

	state2, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err, "No error expected.")
	info2 := state2.ExecutionInfo
	s.NotNil(info2, "Valid Workflow info expected.")
	updatedInfo2 := copyWorkflowExecutionInfo(info2)

	err = s.UpdateWorkflowExecutionWithTransferTasks(updatedInfo2, int64(3), transferTasks, nil)
	s.Nil(err, "No error expected.")

	tasks2, err := s.GetTransferTasks(1, false)
	s.Nil(err, "No error expected.")
	s.NotNil(tasks2, "expected valid list of tasks.")
	s.Equal(1, len(tasks2), "Expected 1 cancel task.")
	task2 := tasks2[0]
	s.Equal(TransferTaskTypeSignalExecution, task2.TaskType)
	s.Equal(domainID, task2.DomainID)
	s.Equal(*workflowExecution.WorkflowId, task2.WorkflowID)
	s.Equal(*workflowExecution.RunId, task2.RunID)
	s.Equal(targetDomainID, task2.TargetDomainID)
	s.Equal(targetWorkflowID, task2.TargetWorkflowID)
	s.Equal(targetRunID, task2.TargetRunID)
	s.Equal(targetChildWorkflowOnly, task2.TargetChildWorkflowOnly)

	err = s.CompleteTransferTask(task2.TaskID)
	s.Nil(err)
}

func (s *cassandraPersistenceSuite) TestCreateTask() {
	domainID := "11adbd1b-f164-4ea7-b2f3-2e857a5048f1"
	workflowExecution := gen.WorkflowExecution{WorkflowId: common.StringPtr("create-task-test"),
		RunId: common.StringPtr("c949447a-691a-4132-8b2a-a5b38106793c")}
	task0, err0 := s.CreateDecisionTask(domainID, workflowExecution, "a5b38106793c", 5)
	s.Nil(err0, "No error expected.")
	s.NotNil(task0, "Expected non empty task identifier.")

	tasks1, err1 := s.CreateActivityTasks(domainID, workflowExecution, map[int64]string{
		10: "a5b38106793c"})
	s.Nil(err1, "No error expected.")
	s.NotNil(tasks1, "Expected valid task identifiers.")
	s.Equal(1, len(tasks1), "expected single valid task identifier.")
	for _, t := range tasks1 {
		s.NotEmpty(t, "Expected non empty task identifier.")
	}

	tasks2, err2 := s.CreateActivityTasks(domainID, workflowExecution, map[int64]string{
		20: "a5b38106793a",
		30: "a5b38106793b",
		40: "a5b38106793c",
		50: "a5b38106793d",
		60: "a5b38106793e",
	})
	s.Nil(err2, "No error expected.")
	s.Equal(5, len(tasks2), "expected single valid task identifier.")
	for _, t := range tasks2 {
		s.NotEmpty(t, "Expected non empty task identifier.")
	}
}

func (s *cassandraPersistenceSuite) TestGetDecisionTasks() {
	domainID := "aeac8287-527b-4b35-80a9-667cb47e7c6d"
	workflowExecution := gen.WorkflowExecution{WorkflowId: common.StringPtr("get-decision-task-test"),
		RunId: common.StringPtr("db20f7e2-1a1e-40d9-9278-d8b886738e05")}
	taskList := "d8b886738e05"
	task0, err0 := s.CreateDecisionTask(domainID, workflowExecution, taskList, 5)
	s.Nil(err0, "No error expected.")
	s.NotNil(task0, "Expected non empty task identifier.")

	tasks1Response, err1 := s.GetTasks(domainID, taskList, TaskListTypeDecision, 1)
	s.Nil(err1, "No error expected.")
	s.NotNil(tasks1Response.Tasks, "expected valid list of tasks.")
	s.Equal(1, len(tasks1Response.Tasks), "Expected 1 decision task.")
	s.Equal(int64(5), tasks1Response.Tasks[0].ScheduleID)
}

func (s *cassandraPersistenceSuite) TestCompleteDecisionTask() {
	domainID := "f1116985-d1f1-40e0-aba9-83344db915bc"
	workflowExecution := gen.WorkflowExecution{WorkflowId: common.StringPtr("complete-decision-task-test"),
		RunId: common.StringPtr("2aa0a74e-16ee-4f27-983d-48b07ec1915d")}
	taskList := "48b07ec1915d"
	tasks0, err0 := s.CreateActivityTasks(domainID, workflowExecution, map[int64]string{
		10: taskList,
		20: taskList,
		30: taskList,
		40: taskList,
		50: taskList,
	})
	s.Nil(err0, "No error expected.")
	s.NotNil(tasks0, "Expected non empty task identifier.")
	s.Equal(5, len(tasks0), "expected 5 valid task identifier.")
	for _, t := range tasks0 {
		s.NotEmpty(t, "Expected non empty task identifier.")
	}

	tasksWithID1Response, err1 := s.GetTasks(domainID, taskList, TaskListTypeActivity, 5)

	s.Nil(err1, "No error expected.")
	tasksWithID1 := tasksWithID1Response.Tasks
	s.NotNil(tasksWithID1, "expected valid list of tasks.")

	s.Equal(5, len(tasksWithID1), "Expected 5 activity tasks.")
	for _, t := range tasksWithID1 {
		s.Equal(domainID, t.DomainID)
		s.Equal(*workflowExecution.WorkflowId, t.WorkflowID)
		s.Equal(*workflowExecution.RunId, t.RunID)
		s.True(t.TaskID > 0)

		err2 := s.CompleteTask(domainID, taskList, TaskListTypeActivity, t.TaskID, 100)
		s.Nil(err2)
	}
}

func (s *cassandraPersistenceSuite) TestLeaseAndUpdateTaskList() {
	domainID := "00136543-72ad-4615-b7e9-44bca9775b45"
	taskList := "aaaaaaa"
	response, err := s.TaskMgr.LeaseTaskList(&LeaseTaskListRequest{
		DomainID: domainID,
		TaskList: taskList,
		TaskType: TaskListTypeActivity,
	})
	s.NoError(err)
	tli := response.TaskListInfo
	s.EqualValues(1, tli.RangeID)
	s.EqualValues(0, tli.AckLevel)

	response, err = s.TaskMgr.LeaseTaskList(&LeaseTaskListRequest{
		DomainID: domainID,
		TaskList: taskList,
		TaskType: TaskListTypeActivity,
	})
	s.NoError(err)
	tli = response.TaskListInfo
	s.EqualValues(2, tli.RangeID)
	s.EqualValues(0, tli.AckLevel)

	taskListInfo := &TaskListInfo{
		DomainID: domainID,
		Name:     taskList,
		TaskType: TaskListTypeActivity,
		RangeID:  2,
		AckLevel: 0,
		Kind:     TaskListKindNormal,
	}
	_, err = s.TaskMgr.UpdateTaskList(&UpdateTaskListRequest{
		TaskListInfo: taskListInfo,
	})
	s.NoError(err)

	taskListInfo.RangeID = 3
	_, err = s.TaskMgr.UpdateTaskList(&UpdateTaskListRequest{
		TaskListInfo: taskListInfo,
	})
	s.Error(err)
}

func (s *cassandraPersistenceSuite) TestLeaseAndUpdateTaskList_Sticky() {
	domainID := uuid.New()
	taskList := "aaaaaaa"
	response, err := s.TaskMgr.LeaseTaskList(&LeaseTaskListRequest{
		DomainID:     domainID,
		TaskList:     taskList,
		TaskType:     TaskListTypeDecision,
		TaskListKind: TaskListKindSticky,
	})
	s.NoError(err)
	tli := response.TaskListInfo
	s.EqualValues(1, tli.RangeID)
	s.EqualValues(0, tli.AckLevel)
	s.EqualValues(TaskListKindSticky, tli.Kind)

	taskListInfo := &TaskListInfo{
		DomainID: domainID,
		Name:     taskList,
		TaskType: TaskListTypeDecision,
		RangeID:  2,
		AckLevel: 0,
		Kind:     TaskListKindSticky,
	}
	_, err = s.TaskMgr.UpdateTaskList(&UpdateTaskListRequest{
		TaskListInfo: taskListInfo,
	})
	s.NoError(err) // because update with ttl doesn't check rangeID
}

func (s *cassandraPersistenceSuite) TestReplicationTasks() {
	domainID := "2466d7de-6602-4ad8-b939-fb8f8c36c711"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("get-replication-tasks-test"),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
	}

	task0, err := s.CreateWorkflowExecution(domainID, workflowExecution, "queue1", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.Nil(err, "No error expected.")
	s.NotNil(task0, "Expected non empty task identifier.")
	taskD, err := s.GetTransferTasks(1, false)
	s.Equal(1, len(taskD), "Expected 1 decision task.")
	err = s.CompleteTransferTask(taskD[0].TaskID)
	s.Nil(err)

	state1, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err, "No error expected.")
	info1 := state1.ExecutionInfo
	s.NotNil(info1, "Valid Workflow info expected.")
	updatedInfo1 := copyWorkflowExecutionInfo(info1)

	replicationTasks := []Task{
		&HistoryReplicationTask{
			TaskID:       s.GetNextSequenceNumber(),
			FirstEventID: int64(1),
			NextEventID:  int64(3),
			Version:      123,
			LastReplicationInfo: map[string]*ReplicationInfo{
				"dc1": &ReplicationInfo{
					Version:     int64(3),
					LastEventID: int64(1),
				},
			},
		},
		&HistoryReplicationTask{
			TaskID:       s.GetNextSequenceNumber(),
			FirstEventID: int64(1),
			NextEventID:  int64(3),
			Version:      456,
			LastReplicationInfo: map[string]*ReplicationInfo{
				"dc1": &ReplicationInfo{
					Version:     int64(3),
					LastEventID: int64(1),
				},
			},
		},
	}
	err = s.UpdateWorklowStateAndReplication(updatedInfo1, nil, nil, nil, int64(3), replicationTasks)
	s.Nil(err, "No error expected.")

	repTasks, err := s.GetReplicationTasks(1, true)
	s.Nil(err)
	s.Equal(len(replicationTasks), len(repTasks))

	for index := range replicationTasks {
		s.Equal(replicationTasks[index].GetTaskID(), repTasks[index].GetTaskID())
		s.Equal(replicationTasks[index].GetType(), repTasks[index].GetTaskType())
		s.Equal(replicationTasks[index].GetVersion(), repTasks[index].GetVersion())

		err = s.CompleteReplicationTask(repTasks[index].GetTaskID())
		s.Nil(err, "No error expected.")
	}
}

func (s *cassandraPersistenceSuite) TestTransferTasks_Complete() {
	domainID := "8bfb47be-5b57-4d55-9109-5fb35e20b1d7"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("get-transfer-tasks-test-complete"),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
	}
	tasklist := "some random tasklist"

	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, tasklist, "wType", 20, 13, nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotNil(task0, "Expected non empty task identifier.")

	tasks1, err1 := s.GetTransferTasks(1, false)
	s.Nil(err1, "No error expected.")
	s.NotNil(tasks1, "expected valid list of tasks.")
	s.Equal(1, len(tasks1), "Expected 1 decision task.")
	task1 := tasks1[0]
	s.Equal(domainID, task1.DomainID)
	s.Equal(workflowExecution.GetWorkflowId(), task1.WorkflowID)
	s.Equal(workflowExecution.GetRunId(), task1.RunID)
	s.Equal(tasklist, task1.TaskList)
	s.Equal(TransferTaskTypeDecisionTask, task1.TaskType)
	s.Equal(int64(2), task1.ScheduleID)
	s.Equal(transferTaskTransferTargetWorkflowID, task1.TargetWorkflowID)
	s.Equal("", task1.TargetRunID)
	err3 := s.CompleteTransferTask(task1.TaskID)
	s.Nil(err3)

	state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err1, "No error expected.")
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedInfo.NextEventID = int64(6)
	updatedInfo.LastProcessedEvent = int64(2)
	scheduleID := int64(123)
	targetDomainID := "8bfb47be-5b57-4d66-9109-5fb35e20b1d0"
	targetWorkflowID := "some random target domain ID"
	targetRunID := uuid.New()
	currentTransferID := s.GetTransferReadLevel()
	now := time.Now()
	tasks := []Task{
		&ActivityTask{now, currentTransferID + 10001, domainID, tasklist, scheduleID, 111},
		&DecisionTask{now, currentTransferID + 10002, domainID, tasklist, scheduleID, 222},
		&CloseExecutionTask{now, currentTransferID + 10003, 333},
		&CancelExecutionTask{now, currentTransferID + 10004, targetDomainID, targetWorkflowID, targetRunID, true, scheduleID, 444},
		&SignalExecutionTask{now, currentTransferID + 10005, targetDomainID, targetWorkflowID, targetRunID, true, scheduleID, 555},
		&StartChildExecutionTask{now, currentTransferID + 10006, targetDomainID, targetWorkflowID, scheduleID, 666},
	}
	err2 := s.UpdateWorklowStateAndReplication(updatedInfo, nil, nil, nil, int64(3), tasks)
	s.Nil(err2, "No error expected.")

	txTasks, err1 := s.GetTransferTasks(1, true) // use page size one to force pagination
	s.Nil(err1, "No error expected.")
	s.NotNil(txTasks, "expected valid list of tasks.")
	s.Equal(len(tasks), len(txTasks))
	for index := range tasks {
		s.True(timeComparator(tasks[index].GetVisibilityTimestamp(), txTasks[index].VisibilityTimestamp, timePrecision))
	}
	s.Equal(TransferTaskTypeActivityTask, txTasks[0].TaskType)
	s.Equal(TransferTaskTypeDecisionTask, txTasks[1].TaskType)
	s.Equal(TransferTaskTypeCloseExecution, txTasks[2].TaskType)
	s.Equal(TransferTaskTypeCancelExecution, txTasks[3].TaskType)
	s.Equal(TransferTaskTypeSignalExecution, txTasks[4].TaskType)
	s.Equal(TransferTaskTypeStartChildExecution, txTasks[5].TaskType)
	s.Equal(int64(111), txTasks[0].Version)
	s.Equal(int64(222), txTasks[1].Version)
	s.Equal(int64(333), txTasks[2].Version)
	s.Equal(int64(444), txTasks[3].Version)
	s.Equal(int64(555), txTasks[4].Version)
	s.Equal(int64(666), txTasks[5].Version)

	err2 = s.CompleteTransferTask(txTasks[0].TaskID)
	s.Nil(err2, "No error expected.")

	err2 = s.CompleteTransferTask(txTasks[1].TaskID)
	s.Nil(err2, "No error expected.")

	err2 = s.CompleteTransferTask(txTasks[2].TaskID)
	s.Nil(err2, "No error expected.")

	err2 = s.CompleteTransferTask(txTasks[3].TaskID)
	s.Nil(err2, "No error expected.")

	err2 = s.CompleteTransferTask(txTasks[4].TaskID)
	s.Nil(err2, "No error expected.")

	err2 = s.CompleteTransferTask(txTasks[5].TaskID)
	s.Nil(err2, "No error expected.")

	txTasks, err2 = s.GetTransferTasks(100, false)
	s.Nil(err2, "No error expected.")
	s.Empty(txTasks, "expected empty task list.")
}

func (s *cassandraPersistenceSuite) TestTransferTasks_RangeComplete() {
	domainID := "8bfb47be-5b57-4d55-9109-5fb35e20b1d7"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("get-transfer-tasks-test-range-complete"),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
	}
	tasklist := "some random tasklist"

	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, tasklist, "wType", 20, 13, nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotNil(task0, "Expected non empty task identifier.")

	tasks1, err1 := s.GetTransferTasks(1, false)
	s.Nil(err1, "No error expected.")
	s.NotNil(tasks1, "expected valid list of tasks.")
	s.Equal(1, len(tasks1), "Expected 1 decision task.")
	task1 := tasks1[0]
	s.Equal(domainID, task1.DomainID)
	s.Equal(workflowExecution.GetWorkflowId(), task1.WorkflowID)
	s.Equal(workflowExecution.GetRunId(), task1.RunID)
	s.Equal(tasklist, task1.TaskList)
	s.Equal(TransferTaskTypeDecisionTask, task1.TaskType)
	s.Equal(int64(2), task1.ScheduleID)
	s.Equal(transferTaskTransferTargetWorkflowID, task1.TargetWorkflowID)
	s.Equal("", task1.TargetRunID)
	err3 := s.CompleteTransferTask(task1.TaskID)
	s.Nil(err3)

	state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err1, "No error expected.")
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedInfo.NextEventID = int64(6)
	updatedInfo.LastProcessedEvent = int64(2)
	scheduleID := int64(123)
	targetDomainID := "8bfb47be-5b57-4d66-9109-5fb35e20b1d0"
	targetWorkflowID := "some random target domain ID"
	targetRunID := uuid.New()
	currentTransferID := s.GetTransferReadLevel()
	now := time.Now()
	tasks := []Task{
		&ActivityTask{now, currentTransferID + 10001, domainID, tasklist, scheduleID, 111},
		&DecisionTask{now, currentTransferID + 10002, domainID, tasklist, scheduleID, 222},
		&CloseExecutionTask{now, currentTransferID + 10003, 333},
		&CancelExecutionTask{now, currentTransferID + 10004, targetDomainID, targetWorkflowID, targetRunID, true, scheduleID, 444},
		&SignalExecutionTask{now, currentTransferID + 10005, targetDomainID, targetWorkflowID, targetRunID, true, scheduleID, 555},
		&StartChildExecutionTask{now, currentTransferID + 10006, targetDomainID, targetWorkflowID, scheduleID, 666},
	}
	err2 := s.UpdateWorklowStateAndReplication(updatedInfo, nil, nil, nil, int64(3), tasks)
	s.Nil(err2, "No error expected.")

	txTasks, err1 := s.GetTransferTasks(1, true) // use page size one to force pagination
	s.Nil(err1, "No error expected.")
	s.NotNil(txTasks, "expected valid list of tasks.")
	s.Equal(len(tasks), len(txTasks))
	for index := range tasks {
		s.True(timeComparator(tasks[index].GetVisibilityTimestamp(), txTasks[index].VisibilityTimestamp, timePrecision))
	}
	s.Equal(TransferTaskTypeActivityTask, txTasks[0].TaskType)
	s.Equal(TransferTaskTypeDecisionTask, txTasks[1].TaskType)
	s.Equal(TransferTaskTypeCloseExecution, txTasks[2].TaskType)
	s.Equal(TransferTaskTypeCancelExecution, txTasks[3].TaskType)
	s.Equal(TransferTaskTypeSignalExecution, txTasks[4].TaskType)
	s.Equal(TransferTaskTypeStartChildExecution, txTasks[5].TaskType)
	s.Equal(int64(111), txTasks[0].Version)
	s.Equal(int64(222), txTasks[1].Version)
	s.Equal(int64(333), txTasks[2].Version)
	s.Equal(int64(444), txTasks[3].Version)
	s.Equal(int64(555), txTasks[4].Version)
	s.Equal(int64(666), txTasks[5].Version)

	err2 = s.RangeCompleteTransferTask(txTasks[0].TaskID-1, txTasks[5].TaskID)
	s.Nil(err2, "No error expected.")

	txTasks, err2 = s.GetTransferTasks(100, false)
	s.Nil(err2, "No error expected.")
	s.Empty(txTasks, "expected empty task list.")
}

func (s *cassandraPersistenceSuite) TestTimerTasks_Complete() {
	domainID := "8bfb47be-5b57-4d66-9109-5fb35e20b1d7"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("get-timer-tasks-test-complete"),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
	}

	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "taskList", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err1, "No error expected.")
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedInfo.NextEventID = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)
	now := time.Now()
	tasks := []Task{
		&DecisionTimeoutTask{now.Add(1 * time.Second), 1, 2, 3, int(gen.TimeoutTypeStartToClose), 11},
		&WorkflowTimeoutTask{now.Add(2 * time.Second), 2, 12},
		&DeleteHistoryEventTask{now.Add(2 * time.Second), 3, 13},
		&ActivityTimeoutTask{now.Add(3 * time.Second), 4, int(gen.TimeoutTypeStartToClose), 7, 0, 14},
		&UserTimerTask{now.Add(3 * time.Second), 5, 7, 15},
	}
	err2 := s.UpdateWorkflowExecution(updatedInfo, []int64{int64(4)}, nil, int64(3), tasks, nil, nil, nil, nil, nil)
	s.Nil(err2, "No error expected.")

	timerTasks, err1 := s.GetTimerIndexTasks(1, true) // use page size one to force pagination
	s.Nil(err1, "No error expected.")
	s.NotNil(timerTasks, "expected valid list of tasks.")
	s.Equal(len(tasks), len(timerTasks))
	s.Equal(TaskTypeDecisionTimeout, timerTasks[0].TaskType)
	s.Equal(TaskTypeWorkflowTimeout, timerTasks[1].TaskType)
	s.Equal(TaskTypeDeleteHistoryEvent, timerTasks[2].TaskType)
	s.Equal(TaskTypeActivityTimeout, timerTasks[3].TaskType)
	s.Equal(TaskTypeUserTimer, timerTasks[4].TaskType)
	s.Equal(int64(11), timerTasks[0].Version)
	s.Equal(int64(12), timerTasks[1].Version)
	s.Equal(int64(13), timerTasks[2].Version)
	s.Equal(int64(14), timerTasks[3].Version)
	s.Equal(int64(15), timerTasks[4].Version)

	err2 = s.RangeCompleteTimerTask(timerTasks[0].VisibilityTimestamp, timerTasks[4].VisibilityTimestamp.Add(1*time.Second))
	s.Nil(err2, "No error expected.")

	timerTasks2, err2 := s.GetTimerIndexTasks(100, false)
	s.Nil(err2, "No error expected.")
	s.Empty(timerTasks2, "expected empty task list.")
}

func (s *cassandraPersistenceSuite) TestTimerTasks_RangeComplete() {
	domainID := "8bfb47be-5b57-4d66-9109-5fb35e20b1d7"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("get-timer-tasks-test-range-complete"),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
	}

	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "taskList", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err1, "No error expected.")
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedInfo.NextEventID = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)
	tasks := []Task{
		&DecisionTimeoutTask{time.Now(), 1, 2, 3, int(gen.TimeoutTypeStartToClose), 11},
		&WorkflowTimeoutTask{time.Now(), 2, 12},
		&DeleteHistoryEventTask{time.Now(), 3, 13},
		&ActivityTimeoutTask{time.Now(), 4, int(gen.TimeoutTypeStartToClose), 7, 0, 14},
		&UserTimerTask{time.Now(), 5, 7, 15},
	}
	err2 := s.UpdateWorkflowExecution(updatedInfo, []int64{int64(4)}, nil, int64(3), tasks, nil, nil, nil, nil, nil)
	s.Nil(err2, "No error expected.")

	timerTasks, err1 := s.GetTimerIndexTasks(1, true) // use page size one to force pagination
	s.Nil(err1, "No error expected.")
	s.NotNil(timerTasks, "expected valid list of tasks.")
	s.Equal(len(tasks), len(timerTasks))
	s.Equal(TaskTypeDecisionTimeout, timerTasks[0].TaskType)
	s.Equal(TaskTypeWorkflowTimeout, timerTasks[1].TaskType)
	s.Equal(TaskTypeDeleteHistoryEvent, timerTasks[2].TaskType)
	s.Equal(TaskTypeActivityTimeout, timerTasks[3].TaskType)
	s.Equal(TaskTypeUserTimer, timerTasks[4].TaskType)
	s.Equal(int64(11), timerTasks[0].Version)
	s.Equal(int64(12), timerTasks[1].Version)
	s.Equal(int64(13), timerTasks[2].Version)
	s.Equal(int64(14), timerTasks[3].Version)
	s.Equal(int64(15), timerTasks[4].Version)

	deleteTimerTask := &DecisionTimeoutTask{VisibilityTimestamp: timerTasks[0].VisibilityTimestamp, TaskID: timerTasks[0].TaskID}
	err2 = s.UpdateWorkflowExecution(updatedInfo, nil, nil, int64(5), nil, deleteTimerTask, nil, nil, nil, nil)
	s.Nil(err2, "No error expected.")

	err2 = s.CompleteTimerTask(timerTasks[1].VisibilityTimestamp, timerTasks[1].TaskID)
	s.Nil(err2, "No error expected.")

	err2 = s.CompleteTimerTask(timerTasks[2].VisibilityTimestamp, timerTasks[2].TaskID)
	s.Nil(err2, "No error expected.")

	err2 = s.CompleteTimerTask(timerTasks[3].VisibilityTimestamp, timerTasks[3].TaskID)
	s.Nil(err2, "No error expected.")

	err2 = s.CompleteTimerTask(timerTasks[4].VisibilityTimestamp, timerTasks[4].TaskID)
	s.Nil(err2, "No error expected.")

	timerTasks2, err2 := s.GetTimerIndexTasks(100, false)
	s.Nil(err2, "No error expected.")
	s.Empty(timerTasks2, "expected empty task list.")
}

func (s *cassandraPersistenceSuite) TestWorkflowMutableState_Activities() {
	domainID := "7fcf0aa9-e121-4292-bdad-0a75181b4aa3"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("test-workflow-mutable-test"),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
	}

	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "taskList", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err1, "No error expected.")
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedInfo.NextEventID = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)
	currentTime := time.Now().UTC()
	activityInfos := []*ActivityInfo{{
		Version:                  7789,
		ScheduleID:               1,
		ScheduledEvent:           []byte("scheduled_event_1"),
		ScheduledTime:            currentTime,
		StartedID:                2,
		StartedEvent:             []byte("started_event_1"),
		StartedTime:              currentTime,
		ScheduleToCloseTimeout:   1,
		ScheduleToStartTimeout:   2,
		StartToCloseTimeout:      3,
		HeartbeatTimeout:         4,
		LastHeartBeatUpdatedTime: currentTime,
		TimerTaskStatus:          1,
	}}
	err2 := s.UpdateWorkflowExecution(updatedInfo, []int64{int64(4)}, nil, int64(3), nil, nil, activityInfos, nil, nil, nil)
	s.Nil(err2, "No error expected.")

	state, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err1, "No error expected.")
	s.NotNil(state, "expected valid state.")
	s.Equal(1, len(state.ActivitInfos))
	ai, ok := state.ActivitInfos[1]
	s.True(ok)
	s.NotNil(ai)
	s.Equal(int64(7789), ai.Version)
	s.Equal(int64(1), ai.ScheduleID)
	s.Equal([]byte("scheduled_event_1"), ai.ScheduledEvent)
	s.Equal(currentTime.Unix(), ai.ScheduledTime.Unix())
	s.Equal(int64(2), ai.StartedID)
	s.Equal([]byte("started_event_1"), ai.StartedEvent)
	s.Equal(currentTime.Unix(), ai.StartedTime.Unix())
	s.Equal(int32(1), ai.ScheduleToCloseTimeout)
	s.Equal(int32(2), ai.ScheduleToStartTimeout)
	s.Equal(int32(3), ai.StartToCloseTimeout)
	s.Equal(int32(4), ai.HeartbeatTimeout)
	s.Equal(currentTime.Unix(), ai.LastHeartBeatUpdatedTime.Unix())
	s.Equal(int32(1), ai.TimerTaskStatus)

	err2 = s.UpdateWorkflowExecution(updatedInfo, nil, nil, int64(5), nil, nil, nil, []int64{1}, nil, nil)
	s.Nil(err2, "No error expected.")

	state, err1 = s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err2, "No error expected.")
	s.NotNil(state, "expected valid state.")
	s.Equal(0, len(state.ActivitInfos))
}

func (s *cassandraPersistenceSuite) TestWorkflowMutableState_Timers() {
	domainID := "025d178a-709b-4c07-8dd7-86dbf9bd2e06"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("test-workflow-mutable-timers-test"),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
	}

	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "taskList", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err1, "No error expected.")
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedInfo.NextEventID = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)
	currentTime := time.Now().UTC()
	timerID := "id_1"
	timerInfos := []*TimerInfo{{
		Version:    3345,
		TimerID:    timerID,
		ExpiryTime: currentTime,
		TaskID:     2,
		StartedID:  5,
	}}
	err2 := s.UpdateWorkflowExecution(updatedInfo, []int64{int64(4)}, nil, int64(3), nil, nil, nil, nil, timerInfos, nil)
	s.Nil(err2, "No error expected.")

	state, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err1, "No error expected.")
	s.NotNil(state, "expected valid state.")
	s.Equal(1, len(state.TimerInfos))
	s.Equal(int64(3345), state.TimerInfos[timerID].Version)
	s.Equal(timerID, state.TimerInfos[timerID].TimerID)
	s.Equal(currentTime.Unix(), state.TimerInfos[timerID].ExpiryTime.Unix())
	s.Equal(int64(2), state.TimerInfos[timerID].TaskID)
	s.Equal(int64(5), state.TimerInfos[timerID].StartedID)

	err2 = s.UpdateWorkflowExecution(updatedInfo, nil, nil, int64(5), nil, nil, nil, nil, nil, []string{timerID})
	s.Nil(err2, "No error expected.")

	state, err1 = s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err2, "No error expected.")
	s.NotNil(state, "expected valid state.")
	s.Equal(0, len(state.TimerInfos))
}

func (s *cassandraPersistenceSuite) TestWorkflowMutableState_ChildExecutions() {
	domainID := "88236cd2-c439-4cec-9957-2748ce3be074"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("test-workflow-mutable-child-executions-parent-test"),
		RunId:      common.StringPtr("c63dba1e-929c-4fbf-8ec5-4533b16269a9"),
	}

	parentDomainID := "6036ded3-e541-42c9-8f69-3d9354dad081"
	parentExecution := &gen.WorkflowExecution{
		WorkflowId: common.StringPtr("test-workflow-mutable-child-executions-child-test"),
		RunId:      common.StringPtr("73e89362-25ec-4305-bcb8-d9448b90856c"),
	}

	task0, err0 := s.CreateChildWorkflowExecution(domainID, workflowExecution, parentDomainID, parentExecution, 1, "taskList", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err1, "No error expected.")
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")
	s.Equal(parentDomainID, info0.ParentDomainID)
	s.Equal(*parentExecution.WorkflowId, info0.ParentWorkflowID)
	s.Equal(*parentExecution.RunId, info0.ParentRunID)
	s.Equal(int64(1), info0.InitiatedID)

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedInfo.NextEventID = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)
	createRequestID := uuid.New()
	childExecutionInfos := []*ChildExecutionInfo{{
		Version:         1234,
		InitiatedID:     1,
		InitiatedEvent:  []byte("initiated_event_1"),
		StartedID:       2,
		StartedEvent:    []byte("started_event_1"),
		CreateRequestID: createRequestID,
	}}
	err2 := s.UpsertChildExecutionsState(updatedInfo, int64(3), childExecutionInfos)
	s.Nil(err2, "No error expected.")

	state, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err1, "No error expected.")
	s.NotNil(state, "expected valid state.")
	s.Equal(1, len(state.ChildExecutionInfos))
	ci, ok := state.ChildExecutionInfos[1]
	s.True(ok)
	s.NotNil(ci)
	s.Equal(int64(1234), ci.Version)
	s.Equal(int64(1), ci.InitiatedID)
	s.Equal([]byte("initiated_event_1"), ci.InitiatedEvent)
	s.Equal(int64(2), ci.StartedID)
	s.Equal([]byte("started_event_1"), ci.StartedEvent)
	s.Equal(createRequestID, ci.CreateRequestID)

	err2 = s.DeleteChildExecutionsState(updatedInfo, int64(5), int64(1))
	s.Nil(err2, "No error expected.")

	state, err1 = s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err2, "No error expected.")
	s.NotNil(state, "expected valid state.")
	s.Equal(0, len(state.ChildExecutionInfos))
}

func (s *cassandraPersistenceSuite) TestWorkflowMutableState_RequestCancel() {
	domainID := "568b8d19-cf64-4aac-be1b-f8a3edbc1fa9"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("test-workflow-mutable-request-cancel-test"),
		RunId:      common.StringPtr("87f96253-b925-426e-90db-aa4ee89b5aca"),
	}

	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "taskList", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err1, "No error expected.")
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedInfo.NextEventID = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)
	cancelRequestID := uuid.New()
	requestCancelInfos := []*RequestCancelInfo{{
		Version:         456,
		InitiatedID:     1,
		CancelRequestID: cancelRequestID,
	}}
	err2 := s.UpsertRequestCancelState(updatedInfo, int64(3), requestCancelInfos)
	s.Nil(err2, "No error expected.")

	state, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err1, "No error expected.")
	s.NotNil(state, "expected valid state.")
	s.Equal(1, len(state.RequestCancelInfos))
	ri, ok := state.RequestCancelInfos[1]
	s.True(ok)
	s.NotNil(ri)
	s.Equal(int64(456), ri.Version)
	s.Equal(int64(1), ri.InitiatedID)
	s.Equal(cancelRequestID, ri.CancelRequestID)

	err2 = s.DeleteCancelState(updatedInfo, int64(5), int64(1))
	s.Nil(err2, "No error expected.")

	state, err1 = s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err2, "No error expected.")
	s.NotNil(state, "expected valid state.")
	s.Equal(0, len(state.RequestCancelInfos))
}

func (s *cassandraPersistenceSuite) TestWorkflowMutableState_SignalInfo() {
	domainID := uuid.New()
	runID := uuid.New()
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("test-workflow-mutable-signal-info-test"),
		RunId:      common.StringPtr(runID),
	}

	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "taskList", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err1, "No error expected.")
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedInfo.NextEventID = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)
	signalRequestID := uuid.New()
	signalName := "my signal"
	input := []byte("test signal input")
	control := []byte(uuid.New())
	signalInfos := []*SignalInfo{
		{
			Version:         123,
			InitiatedID:     1,
			SignalRequestID: signalRequestID,
			SignalName:      signalName,
			Input:           input,
			Control:         control,
		}}
	err2 := s.UpsertSignalInfoState(updatedInfo, int64(3), signalInfos)
	s.Nil(err2, "No error expected.")

	state, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err1, "No error expected.")
	s.NotNil(state, "expected valid state.")
	s.Equal(1, len(state.SignalInfos))
	ri, ok := state.SignalInfos[1]
	s.True(ok)
	s.NotNil(ri)
	s.Equal(int64(123), ri.Version)
	s.Equal(int64(1), ri.InitiatedID)
	s.Equal(signalRequestID, ri.SignalRequestID)
	s.Equal(signalName, ri.SignalName)
	s.Equal(input, ri.Input)
	s.Equal(control, ri.Control)

	err2 = s.DeleteSignalState(updatedInfo, int64(5), int64(1))
	s.Nil(err2, "No error expected.")

	state, err1 = s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err2, "No error expected.")
	s.NotNil(state, "expected valid state.")
	s.Equal(0, len(state.SignalInfos))
}

func (s *cassandraPersistenceSuite) TestWorkflowMutableState_SignalRequested() {
	domainID := uuid.New()
	runID := uuid.New()
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("test-workflow-mutable-signal-requested-test"),
		RunId:      common.StringPtr(runID),
	}

	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "taskList", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err1, "No error expected.")
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedInfo.NextEventID = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)
	signalRequestedID := uuid.New()
	signalsRequested := []string{signalRequestedID}
	err2 := s.UpsertSignalsRequestedState(updatedInfo, int64(3), signalsRequested)
	s.Nil(err2, "No error expected.")

	state, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err1, "No error expected.")
	s.NotNil(state, "expected valid state.")
	s.Equal(1, len(state.SignalRequestedIDs))
	ri, ok := state.SignalRequestedIDs[signalRequestedID]
	s.True(ok)
	s.NotNil(ri)

	err2 = s.DeleteSignalsRequestedState(updatedInfo, int64(5), signalRequestedID)
	s.Nil(err2, "No error expected.")

	state, err1 = s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err2, "No error expected.")
	s.NotNil(state, "expected valid state.")
	s.Equal(0, len(state.SignalRequestedIDs))
}

func (s *cassandraPersistenceSuite) TestWorkflowMutableState_BufferedReplicationTasks() {
	domainID := "714f8491-a34e-4301-a5af-f0cf5d8660c6"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("test-workflow-mutable-buffered-replication-tasks-test"),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
	}

	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "taskList", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err1, "No error expected.")
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")
	s.Equal(0, len(state0.BufferedReplicationTasks))

	updatedInfo := copyWorkflowExecutionInfo(info0)
	events := []*gen.HistoryEvent{
		&gen.HistoryEvent{
			EventId:   common.Int64Ptr(5),
			EventType: gen.EventTypeDecisionTaskCompleted.Ptr(),
			DecisionTaskCompletedEventAttributes: &gen.DecisionTaskCompletedEventAttributes{
				ScheduledEventId: common.Int64Ptr(2),
				StartedEventId:   common.Int64Ptr(3),
				Identity:         common.StringPtr("test_worker"),
			},
		},
		&gen.HistoryEvent{
			EventId:   common.Int64Ptr(6),
			EventType: gen.EventTypeTimerStarted.Ptr(),
			TimerStartedEventAttributes: &gen.TimerStartedEventAttributes{
				TimerId:                      common.StringPtr("ID1"),
				StartToFireTimeoutSeconds:    common.Int64Ptr(101),
				DecisionTaskCompletedEventId: common.Int64Ptr(5),
			},
		},
	}

	bufferedTask := &BufferedReplicationTask{
		FirstEventID: int64(5),
		NextEventID:  int64(7),
		Version:      int64(11),
		History:      s.serializeHistoryEvents(events),
	}
	err2 := s.UpdateWorklowStateAndReplication(updatedInfo, nil, bufferedTask, nil, int64(3), nil)
	s.Nil(err2, "No error expected.")

	state1, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err1, "No error expected.")
	s.NotNil(state1, "expected valid state.")
	s.Equal(1, len(state1.BufferedReplicationTasks))

	bufferedTask, ok := state1.BufferedReplicationTasks[5]
	s.True(ok)
	s.NotNil(bufferedTask)
	s.Equal(int64(5), bufferedTask.FirstEventID)
	s.Equal(int64(7), bufferedTask.NextEventID)
	s.Equal(int64(11), bufferedTask.Version)

	bufferedEvents := s.deserializedHistoryEvents(bufferedTask.History)
	s.Equal(2, len(bufferedEvents.Events))
	s.Equal(int64(5), bufferedEvents.Events[0].GetEventId())
	s.Equal(gen.EventTypeDecisionTaskCompleted, bufferedEvents.Events[0].GetEventType())
	s.Equal(int64(2), bufferedEvents.Events[0].DecisionTaskCompletedEventAttributes.GetScheduledEventId())
	s.Equal(int64(3), bufferedEvents.Events[0].DecisionTaskCompletedEventAttributes.GetStartedEventId())
	s.Equal("test_worker", bufferedEvents.Events[0].DecisionTaskCompletedEventAttributes.GetIdentity())
	s.Equal(int64(6), bufferedEvents.Events[1].GetEventId())
	s.Equal(gen.EventTypeTimerStarted, bufferedEvents.Events[1].GetEventType())
	s.Equal("ID1", bufferedEvents.Events[1].TimerStartedEventAttributes.GetTimerId())
	s.Equal(int64(101), bufferedEvents.Events[1].TimerStartedEventAttributes.GetStartToFireTimeoutSeconds())
	s.Equal(int64(5), bufferedEvents.Events[1].TimerStartedEventAttributes.GetDecisionTaskCompletedEventId())

	newExecutionRunID := "d83db48f-a63c-413d-a05a-bbf5a1ac1098"
	info1 := state1.ExecutionInfo
	updatedInfo = copyWorkflowExecutionInfo(info1)
	completionEvents := []*gen.HistoryEvent{
		&gen.HistoryEvent{
			EventId:   common.Int64Ptr(10),
			EventType: gen.EventTypeDecisionTaskCompleted.Ptr(),
			DecisionTaskCompletedEventAttributes: &gen.DecisionTaskCompletedEventAttributes{
				ScheduledEventId: common.Int64Ptr(8),
				StartedEventId:   common.Int64Ptr(9),
				Identity:         common.StringPtr("test_worker"),
			},
		},
		&gen.HistoryEvent{
			EventId:   common.Int64Ptr(11),
			EventType: gen.EventTypeWorkflowExecutionContinuedAsNew.Ptr(),
			WorkflowExecutionContinuedAsNewEventAttributes: &gen.WorkflowExecutionContinuedAsNewEventAttributes{
				NewExecutionRunId:                   common.StringPtr(newExecutionRunID),
				WorkflowType:                        &gen.WorkflowType{Name: common.StringPtr("wType")},
				TaskList:                            &gen.TaskList{Name: common.StringPtr("taskList")},
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(212),
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(312),
				DecisionTaskCompletedEventId:        common.Int64Ptr(10),
			},
		},
	}

	newRunEvents := []*gen.HistoryEvent{
		&gen.HistoryEvent{
			EventId:   common.Int64Ptr(1),
			EventType: gen.EventTypeWorkflowExecutionStarted.Ptr(),
			WorkflowExecutionStartedEventAttributes: &gen.WorkflowExecutionStartedEventAttributes{
				WorkflowType: &gen.WorkflowType{Name: common.StringPtr("wType")},
				TaskList:     &gen.TaskList{Name: common.StringPtr("taskList")},
			},
		},
		&gen.HistoryEvent{
			EventId:   common.Int64Ptr(2),
			EventType: gen.EventTypeDecisionTaskScheduled.Ptr(),
			DecisionTaskScheduledEventAttributes: &gen.DecisionTaskScheduledEventAttributes{
				TaskList:                   &gen.TaskList{Name: common.StringPtr("taskList")},
				StartToCloseTimeoutSeconds: common.Int32Ptr(201),
				Attempt:                    common.Int64Ptr(1),
			},
		},
	}

	bufferedTask = &BufferedReplicationTask{
		FirstEventID:  int64(10),
		NextEventID:   int64(12),
		Version:       int64(12),
		History:       s.serializeHistoryEvents(completionEvents),
		NewRunHistory: s.serializeHistoryEvents(newRunEvents),
	}
	err3 := s.UpdateWorklowStateAndReplication(updatedInfo, nil, bufferedTask, nil, int64(3), nil)
	s.Nil(err3, "No error expected.")

	state2, err4 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err4, "No error expected.")
	s.NotNil(state2, "expected valid state.")
	s.Equal(2, len(state2.BufferedReplicationTasks))

	bufferedTask, ok = state2.BufferedReplicationTasks[10]
	s.True(ok)
	s.NotNil(bufferedTask)
	s.Equal(int64(10), bufferedTask.FirstEventID)
	s.Equal(int64(12), bufferedTask.NextEventID)
	s.Equal(int64(12), bufferedTask.Version)

	bufferedEvents = s.deserializedHistoryEvents(bufferedTask.History)
	s.Equal(2, len(bufferedEvents.Events))
	s.Equal(int64(10), bufferedEvents.Events[0].GetEventId())
	s.Equal(gen.EventTypeDecisionTaskCompleted, bufferedEvents.Events[0].GetEventType())
	s.Equal(int64(8), bufferedEvents.Events[0].DecisionTaskCompletedEventAttributes.GetScheduledEventId())
	s.Equal(int64(9), bufferedEvents.Events[0].DecisionTaskCompletedEventAttributes.GetStartedEventId())
	s.Equal("test_worker", bufferedEvents.Events[0].DecisionTaskCompletedEventAttributes.GetIdentity())
	s.Equal(int64(11), bufferedEvents.Events[1].GetEventId())
	s.Equal(gen.EventTypeWorkflowExecutionContinuedAsNew, bufferedEvents.Events[1].GetEventType())
	s.Equal(newExecutionRunID, bufferedEvents.Events[1].WorkflowExecutionContinuedAsNewEventAttributes.GetNewExecutionRunId())
	s.Equal("wType", bufferedEvents.Events[1].WorkflowExecutionContinuedAsNewEventAttributes.WorkflowType.GetName())
	s.Equal("taskList", bufferedEvents.Events[1].WorkflowExecutionContinuedAsNewEventAttributes.TaskList.GetName())
	s.Equal(int32(212), bufferedEvents.Events[1].WorkflowExecutionContinuedAsNewEventAttributes.GetTaskStartToCloseTimeoutSeconds())
	s.Equal(int32(312), bufferedEvents.Events[1].WorkflowExecutionContinuedAsNewEventAttributes.GetExecutionStartToCloseTimeoutSeconds())
	s.Equal(int64(10), bufferedEvents.Events[1].WorkflowExecutionContinuedAsNewEventAttributes.GetDecisionTaskCompletedEventId())

	bufferedNewRunEvents := s.deserializedHistoryEvents(bufferedTask.NewRunHistory)
	s.Equal(2, len(bufferedNewRunEvents.Events))
	s.Equal(int64(1), bufferedNewRunEvents.Events[0].GetEventId())
	s.Equal(gen.EventTypeWorkflowExecutionStarted, bufferedNewRunEvents.Events[0].GetEventType())
	s.Equal("wType", bufferedNewRunEvents.Events[0].WorkflowExecutionStartedEventAttributes.WorkflowType.GetName())
	s.Equal("taskList", bufferedNewRunEvents.Events[0].WorkflowExecutionStartedEventAttributes.TaskList.GetName())
	s.Equal(int64(2), bufferedNewRunEvents.Events[1].GetEventId())
	s.Equal(gen.EventTypeDecisionTaskScheduled, bufferedNewRunEvents.Events[1].GetEventType())
	s.Equal("taskList", bufferedNewRunEvents.Events[1].DecisionTaskScheduledEventAttributes.TaskList.GetName())
	s.Equal(int32(201), bufferedNewRunEvents.Events[1].DecisionTaskScheduledEventAttributes.GetStartToCloseTimeoutSeconds())
	s.Equal(int64(1), bufferedNewRunEvents.Events[1].DecisionTaskScheduledEventAttributes.GetAttempt())

	deleteBufferedReplicationTask := int64(5)
	err5 := s.UpdateWorklowStateAndReplication(updatedInfo, nil, nil, &deleteBufferedReplicationTask, int64(3), nil)
	s.Nil(err5, "No error expected.")

	state3, err6 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err6, "No error expected.")
	s.NotNil(state3, "expected valid state.")
	s.Equal(1, len(state3.BufferedReplicationTasks))

	deleteBufferedReplicationTask2 := int64(10)
	err7 := s.UpdateWorklowStateAndReplication(updatedInfo, nil, nil, &deleteBufferedReplicationTask2, int64(3), nil)
	s.Nil(err7, "No error expected.")

	state4, err8 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err8, "No error expected.")
	s.NotNil(state4, "expected valid state.")
	s.Equal(0, len(state4.BufferedReplicationTasks))
}

func (s *cassandraPersistenceSuite) TestWorkflowMutableStateInfo() {
	domainID := "9ed8818b-3090-4160-9f21-c6b70e64d2dd"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("test-workflow-mutable-state-test"),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
	}

	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "taskList", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err1, "No error expected.")
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedInfo.NextEventID = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)

	err2 := s.UpdateWorkflowExecution(updatedInfo, []int64{int64(4)}, nil, int64(3), nil, nil, nil, nil, nil, nil)
	s.Nil(err2, "No error expected.")

	state, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err1, "No error expected.")
	s.NotNil(state, "expected valid state.")
	s.NotNil(state.ExecutionInfo, "expected valid MS Info state.")
	s.Equal(updatedInfo.NextEventID, state.ExecutionInfo.NextEventID)
	s.Equal(updatedInfo.State, state.ExecutionInfo.State)
}

func (s *cassandraPersistenceSuite) TestContinueAsNew() {
	domainID := "c1c0bb55-04e6-4a9c-89d0-1be7b96459f8"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("continue-as-new-workflow-test"),
		RunId:      common.StringPtr("551c88d2-d9e6-404f-8131-9eec14f36643"),
	}

	_, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "queue1", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")

	state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err1, "No error expected.")
	info0 := state0.ExecutionInfo
	continueAsNewInfo := copyWorkflowExecutionInfo(info0)
	continueAsNewInfo.State = WorkflowStateCompleted
	continueAsNewInfo.NextEventID = int64(5)
	continueAsNewInfo.LastProcessedEvent = int64(2)

	newWorkflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("continue-as-new-workflow-test"),
		RunId:      common.StringPtr("64c7e15a-3fd7-4182-9c6f-6f25a4fa2614"),
	}
	err2 := s.ContinueAsNewExecution(continueAsNewInfo, info0.NextEventID, newWorkflowExecution, int64(3), int64(2))
	s.Nil(err2, "No error expected.")

	prevExecutionState, err3 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err3)
	prevExecutionInfo := prevExecutionState.ExecutionInfo
	s.Equal(WorkflowStateCompleted, prevExecutionInfo.State)
	s.Equal(int64(5), prevExecutionInfo.NextEventID)
	s.Equal(int64(2), prevExecutionInfo.LastProcessedEvent)

	newExecutionState, err4 := s.GetWorkflowExecutionInfo(domainID, newWorkflowExecution)
	s.Nil(err4)
	newExecutionInfo := newExecutionState.ExecutionInfo
	s.Equal(WorkflowStateCreated, newExecutionInfo.State)
	s.Equal(int64(3), newExecutionInfo.NextEventID)
	s.Equal(common.EmptyEventID, newExecutionInfo.LastProcessedEvent)
	s.Equal(int64(2), newExecutionInfo.DecisionScheduleID)

	newRunID, err5 := s.GetCurrentWorkflowRunID(domainID, *workflowExecution.WorkflowId)
	s.Nil(err5)
	s.Equal(*newWorkflowExecution.RunId, newRunID)
}

func (s *cassandraPersistenceSuite) TestReplicationTransferTaskTasks() {
	domainID := "2466d7de-6602-4ad8-b939-fb8f8c36c711"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("replication-transfer-task-test"),
		RunId:      common.StringPtr("dcde9d85-5d7a-43c7-8b18-cb2cae0e29e0"),
	}

	task0, err := s.CreateWorkflowExecution(domainID, workflowExecution, "queue1", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.Nil(err, "No error expected.")
	s.NotNil(task0, "Expected non empty task identifier.")

	taskD, err := s.GetTransferTasks(1, false)
	s.Equal(1, len(taskD), "Expected 1 decision task.")
	err = s.CompleteTransferTask(taskD[0].TaskID)
	s.Nil(err)

	state1, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err, "No error expected.")
	info1 := state1.ExecutionInfo
	s.NotNil(info1, "Valid Workflow info expected.")
	updatedInfo1 := copyWorkflowExecutionInfo(info1)

	replicationTasks := []Task{&HistoryReplicationTask{
		TaskID:       s.GetNextSequenceNumber(),
		FirstEventID: int64(1),
		NextEventID:  int64(3),
		Version:      int64(9),
		LastReplicationInfo: map[string]*ReplicationInfo{
			"dc1": &ReplicationInfo{
				Version:     int64(3),
				LastEventID: int64(1),
			},
			"dc2": &ReplicationInfo{
				Version:     int64(5),
				LastEventID: int64(2),
			},
		},
	}}
	err = s.UpdateWorklowStateAndReplication(updatedInfo1, nil, nil, nil, int64(3), replicationTasks)
	s.Nil(err, "No error expected.")

	tasks1, err := s.GetReplicationTasks(1, false)
	s.Nil(err, "No error expected.")
	s.NotNil(tasks1, "expected valid list of tasks.")
	s.Equal(1, len(tasks1), "Expected 1 replication task.")
	task1 := tasks1[0]
	s.Equal(ReplicationTaskTypeHistory, task1.TaskType)
	s.Equal(domainID, task1.DomainID)
	s.Equal(*workflowExecution.WorkflowId, task1.WorkflowID)
	s.Equal(*workflowExecution.RunId, task1.RunID)
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
	s.Nil(err)
}

func (s *cassandraPersistenceSuite) TestWorkflowReplicationState() {
	domainID := uuid.New()
	runID := uuid.New()
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("test-workflow-replication-state-test"),
		RunId:      common.StringPtr(runID),
	}

	replicationTasks := []Task{&HistoryReplicationTask{
		TaskID:       s.GetNextSequenceNumber(),
		FirstEventID: int64(1),
		NextEventID:  int64(3),
		Version:      int64(9),
		LastReplicationInfo: map[string]*ReplicationInfo{
			"dc1": &ReplicationInfo{
				Version:     int64(3),
				LastEventID: int64(1),
			},
			"dc2": &ReplicationInfo{
				Version:     int64(5),
				LastEventID: int64(2),
			},
		},
	}}

	task0, err0 := s.CreateWorkflowExecutionWithReplication(domainID, workflowExecution, "taskList", "wType", 20, 13, 3,
		0, 2, &ReplicationState{
			CurrentVersion:   int64(9),
			StartVersion:     int64(8),
			LastWriteVersion: int64(7),
			LastWriteEventID: int64(6),
			LastReplicationInfo: map[string]*ReplicationInfo{
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
	s.Nil(err0, "No error expected.")
	s.NotNil(task0, "Expected non empty task identifier.")

	taskD, err := s.GetTransferTasks(2, false)
	s.Equal(1, len(taskD), "Expected 1 decision task.")
	s.Equal(TransferTaskTypeDecisionTask, taskD[0].TaskType)
	err = s.CompleteTransferTask(taskD[0].TaskID)
	s.Nil(err)

	taskR, err := s.GetReplicationTasks(1, false)
	s.Equal(1, len(taskR), "Expected 1 replication task.")
	tsk := taskR[0]
	s.Equal(ReplicationTaskTypeHistory, tsk.TaskType)
	s.Equal(domainID, tsk.DomainID)
	s.Equal(*workflowExecution.WorkflowId, tsk.WorkflowID)
	s.Equal(*workflowExecution.RunId, tsk.RunID)
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
	s.Nil(err)

	state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err1, "No error expected.")
	info0 := state0.ExecutionInfo
	replicationState0 := state0.ReplicationState
	s.NotNil(info0, "Valid Workflow info expected.")
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
	updatedInfo.NextEventID = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)
	updatedReplicationState := copyReplicationState(replicationState0)
	updatedReplicationState.CurrentVersion = int64(10)
	updatedReplicationState.StartVersion = int64(11)
	updatedReplicationState.LastWriteVersion = int64(12)
	updatedReplicationState.LastWriteEventID = int64(13)
	updatedReplicationState.LastReplicationInfo["dc1"].Version = int64(4)
	updatedReplicationState.LastReplicationInfo["dc1"].LastEventID = int64(2)

	replicationTasks1 := []Task{&HistoryReplicationTask{
		TaskID:       s.GetNextSequenceNumber(),
		FirstEventID: int64(3),
		NextEventID:  int64(5),
		Version:      int64(10),
		LastReplicationInfo: map[string]*ReplicationInfo{
			"dc1": &ReplicationInfo{
				Version:     int64(4),
				LastEventID: int64(2),
			},
			"dc2": &ReplicationInfo{
				Version:     int64(5),
				LastEventID: int64(2),
			},
		},
	}}
	err2 := s.UpdateWorklowStateAndReplication(updatedInfo, updatedReplicationState, nil, nil, int64(3), replicationTasks1)
	s.Nil(err2, "No error expected.")

	taskR1, err := s.GetReplicationTasks(1, false)
	s.Equal(1, len(taskR1), "Expected 1 replication task.")
	tsk1 := taskR1[0]
	s.Equal(ReplicationTaskTypeHistory, tsk1.TaskType)
	s.Equal(domainID, tsk1.DomainID)
	s.Equal(*workflowExecution.WorkflowId, tsk1.WorkflowID)
	s.Equal(*workflowExecution.RunId, tsk1.RunID)
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
	s.Nil(err)

	state1, err2 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err2, "No error expected.")
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

func (s *cassandraPersistenceSuite) TestResetMutableState_CurrentIsSelf() {
	domainID := "4ca1faac-1a3a-47af-8e51-fdaa2b3d45b9"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("test-reset-mutable-state-test-current-is-self"),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
	}

	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "taskList", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err1, "No error expected.")
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
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
	bufferedTask1 := &BufferedReplicationTask{
		FirstEventID: int64(5),
		NextEventID:  int64(7),
		Version:      int64(11),
		History:      s.serializeHistoryEvents(eventsBatch1),
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
	bufferedTask2 := &BufferedReplicationTask{
		FirstEventID: int64(21),
		NextEventID:  int64(22),
		Version:      int64(12),
		History:      s.serializeHistoryEvents(eventsBatch2),
	}
	updatedState := &WorkflowMutableState{
		ExecutionInfo: updatedInfo,
		ActivitInfos: map[int64]*ActivityInfo{
			4: {
				Version:                  7789,
				ScheduleID:               4,
				ScheduledEvent:           []byte("scheduled_event_4"),
				ScheduledTime:            currentTime,
				StartedID:                6,
				StartedEvent:             []byte("started_event_1"),
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
				ScheduledEvent:           []byte("scheduled_event_5"),
				ScheduledTime:            currentTime,
				StartedID:                7,
				StartedEvent:             []byte("started_event_2"),
				StartedTime:              currentTime,
				ScheduleToCloseTimeout:   1,
				ScheduleToStartTimeout:   2,
				StartToCloseTimeout:      3,
				HeartbeatTimeout:         4,
				LastHeartBeatUpdatedTime: currentTime,
				TimerTaskStatus:          1,
			}},

		TimerInfos: map[string]*TimerInfo{
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

		ChildExecutionInfos: map[int64]*ChildExecutionInfo{
			9: {
				Version:         2334,
				InitiatedID:     9,
				InitiatedEvent:  nil,
				StartedID:       11,
				StartedEvent:    nil,
				CreateRequestID: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
			},
		},

		RequestCancelInfos: map[int64]*RequestCancelInfo{
			19: {
				Version:         2335,
				InitiatedID:     19,
				CancelRequestID: "cancel_requested_id",
			},
		},

		SignalInfos: map[int64]*SignalInfo{
			39: {
				Version:         2336,
				InitiatedID:     39,
				SignalRequestID: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
				SignalName:      "signalA",
				Input:           []byte("signal_input_A"),
				Control:         []byte("signal_control_A"),
			},
		},

		SignalRequestedIDs: map[string]struct{}{
			"00000000-0000-0000-0000-000000000001": {},
			"00000000-0000-0000-0000-000000000002": {},
			"00000000-0000-0000-0000-000000000003": {},
		},
	}

	err2 := s.UpdateAllMutableState(updatedState, int64(3))
	s.Nil(err2, "No error expected.")

	partialState, err2 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err2, "No error expected.")
	s.NotNil(partialState, "expected valid state.")
	partialInfo := partialState.ExecutionInfo
	s.NotNil(partialInfo, "Valid Workflow info expected.")

	bufferUpdateInfo := copyWorkflowExecutionInfo(partialInfo)
	err2 = s.UpdateWorklowStateAndReplication(bufferUpdateInfo, nil, bufferedTask1, nil, bufferUpdateInfo.NextEventID, nil)
	s.Nil(err2, "No error expected.")
	err2 = s.UpdateWorklowStateAndReplication(bufferUpdateInfo, nil, bufferedTask2, nil, bufferUpdateInfo.NextEventID, nil)
	s.Nil(err2, "No error expected.")
	err2 = s.UpdateWorkflowExecutionForBufferEvents(bufferUpdateInfo, nil, bufferUpdateInfo.NextEventID,
		s.serializeHistoryEvents(eventsBatch1))
	s.Nil(err2, "No error expected.")
	err2 = s.UpdateWorkflowExecutionForBufferEvents(bufferUpdateInfo, nil, bufferUpdateInfo.NextEventID,
		s.serializeHistoryEvents(eventsBatch2))
	s.Nil(err2, "No error expected.")

	state1, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err1, "No error expected.")
	s.NotNil(state1, "expected valid state.")
	info1 := state1.ExecutionInfo
	s.NotNil(info1, "Valid Workflow info expected.")

	s.Equal(2, len(state1.ActivitInfos))
	ai, ok := state1.ActivitInfos[4]
	s.True(ok)
	s.NotNil(ai)
	s.Equal(int64(7789), ai.Version)
	s.Equal(int64(4), ai.ScheduleID)
	s.Equal([]byte("scheduled_event_4"), ai.ScheduledEvent)
	s.Equal(currentTime.Unix(), ai.ScheduledTime.Unix())
	s.Equal(int64(6), ai.StartedID)
	s.Equal([]byte("started_event_1"), ai.StartedEvent)
	s.Equal(currentTime.Unix(), ai.StartedTime.Unix())
	s.Equal(int32(1), ai.ScheduleToCloseTimeout)
	s.Equal(int32(2), ai.ScheduleToStartTimeout)
	s.Equal(int32(3), ai.StartToCloseTimeout)
	s.Equal(int32(4), ai.HeartbeatTimeout)
	s.Equal(currentTime.Unix(), ai.LastHeartBeatUpdatedTime.Unix())
	s.Equal(int32(1), ai.TimerTaskStatus)

	ai, ok = state1.ActivitInfos[5]
	s.True(ok)
	s.NotNil(ai)
	s.Equal(int64(7789), ai.Version)
	s.Equal(int64(5), ai.ScheduleID)
	s.Equal([]byte("scheduled_event_5"), ai.ScheduledEvent)
	s.Equal(currentTime.Unix(), ai.ScheduledTime.Unix())
	s.Equal(int64(7), ai.StartedID)
	s.Equal([]byte("started_event_2"), ai.StartedEvent)
	s.Equal(currentTime.Unix(), ai.StartedTime.Unix())
	s.Equal(int32(1), ai.ScheduleToCloseTimeout)
	s.Equal(int32(2), ai.ScheduleToStartTimeout)
	s.Equal(int32(3), ai.StartToCloseTimeout)
	s.Equal(int32(4), ai.HeartbeatTimeout)
	s.Equal(currentTime.Unix(), ai.LastHeartBeatUpdatedTime.Unix())
	s.Equal(int32(1), ai.TimerTaskStatus)

	s.Equal(3, len(state1.TimerInfos))
	ti, ok := state1.TimerInfos["t1"]
	s.True(ok)
	s.NotNil(ti)
	s.Equal(int64(2333), ti.Version)
	s.Equal("t1", ti.TimerID)
	s.Equal(int64(1), ti.StartedID)
	s.Equal(expiryTime.Unix(), ti.ExpiryTime.Unix())
	s.Equal(int64(500), ti.TaskID)

	ti, ok = state1.TimerInfos["t2"]
	s.True(ok)
	s.NotNil(ti)
	s.Equal(int64(2333), ti.Version)
	s.Equal("t2", ti.TimerID)
	s.Equal(int64(2), ti.StartedID)
	s.Equal(expiryTime.Unix(), ti.ExpiryTime.Unix())
	s.Equal(int64(501), ti.TaskID)

	ti, ok = state1.TimerInfos["t3"]
	s.True(ok)
	s.NotNil(ti)
	s.Equal(int64(2333), ti.Version)
	s.Equal("t3", ti.TimerID)
	s.Equal(int64(3), ti.StartedID)
	s.Equal(expiryTime.Unix(), ti.ExpiryTime.Unix())
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

	s.Equal(2, len(state1.BufferedReplicationTasks))
	s.Equal(2, len(state1.BufferedEvents))

	updatedInfo1 := copyWorkflowExecutionInfo(info1)
	updatedInfo1.NextEventID = int64(3)
	resetActivityInfos := []*ActivityInfo{
		{
			Version:                  8789,
			ScheduleID:               40,
			ScheduledEvent:           []byte("scheduled_event_40"),
			ScheduledTime:            currentTime,
			StartedID:                60,
			StartedEvent:             []byte("started_event_10"),
			StartedTime:              currentTime,
			ScheduleToCloseTimeout:   10,
			ScheduleToStartTimeout:   20,
			StartToCloseTimeout:      30,
			HeartbeatTimeout:         40,
			LastHeartBeatUpdatedTime: currentTime,
			TimerTaskStatus:          1,
		}}

	resetTimerInfos := []*TimerInfo{
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

	resetChildExecutionInfos := []*ChildExecutionInfo{
		{
			Version:         3334,
			InitiatedID:     10,
			InitiatedEvent:  nil,
			StartedID:       15,
			StartedEvent:    nil,
			CreateRequestID: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
		}}

	resetRequestCancelInfos := []*RequestCancelInfo{
		{
			Version:         3335,
			InitiatedID:     29,
			CancelRequestID: "new_cancel_requested_id",
		}}

	resetSignalInfos := []*SignalInfo{
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

	rState := &ReplicationState{
		CurrentVersion: int64(8789),
		StartVersion:   int64(8780),
	}

	err3 := s.ResetMutableState(workflowExecution.GetRunId(), updatedInfo1, rState, int64(5), resetActivityInfos, resetTimerInfos,
		resetChildExecutionInfos, resetRequestCancelInfos, resetSignalInfos, nil)
	s.Nil(err3, "No error expected.")

	state4, err4 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err4, "No error expected.")
	s.NotNil(state4, "expected valid state.")
	info4 := state4.ExecutionInfo
	s.NotNil(info4, "Valid Workflow info expected.")
	s.Equal(int64(3), info4.NextEventID)

	s.Equal(1, len(state4.ActivitInfos))
	ai, ok = state4.ActivitInfos[40]
	s.True(ok)
	s.NotNil(ai)
	s.Equal(int64(8789), ai.Version)
	s.Equal(int64(40), ai.ScheduleID)
	s.Equal([]byte("scheduled_event_40"), ai.ScheduledEvent)
	s.Equal(currentTime.Unix(), ai.ScheduledTime.Unix())
	s.Equal(int64(60), ai.StartedID)
	s.Equal([]byte("started_event_10"), ai.StartedEvent)
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
	s.Equal(expiryTime.Unix(), ti.ExpiryTime.Unix())
	s.Equal(int64(600), ti.TaskID)

	ti, ok = state4.TimerInfos["t2_new"]
	s.True(ok)
	s.NotNil(ai)
	s.Equal(int64(3333), ti.Version)
	s.Equal("t2_new", ti.TimerID)
	s.Equal(int64(2), ti.StartedID)
	s.Equal(expiryTime.Unix(), ti.ExpiryTime.Unix())
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

	s.Equal(0, len(state4.BufferedReplicationTasks))
	s.Equal(0, len(state4.BufferedEvents))
}

func (s *cassandraPersistenceSuite) TestResetMutableState_CurrentIsNotSelf() {
	domainID := "4ca1faac-1a3a-47af-8e51-fdaa2b3d45b9"
	workflowID := "test-reset-mutable-state-test-current-is-not-self"

	// first create a workflow and continue as new it
	workflowExecutionReset := gen.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa0"),
	}
	task, err := s.CreateWorkflowExecution(domainID, workflowExecutionReset, "taskList", "wType", 20, 13, nil, 3, 0, 2, nil)
	s.Nil(err, "No error expected.")
	s.NotNil(task, "Expected non empty task identifier.")

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecutionReset)
	s.Nil(err)

	info := state.ExecutionInfo
	continueAsNewInfo := copyWorkflowExecutionInfo(info)
	continueAsNewInfo.State = WorkflowStateCompleted
	continueAsNewInfo.NextEventID = int64(5)
	continueAsNewInfo.LastProcessedEvent = int64(2)

	workflowExecutionCurrent1 := gen.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1"),
	}
	err = s.ContinueAsNewExecution(continueAsNewInfo, info.NextEventID, workflowExecutionCurrent1, int64(3), int64(2))
	s.Nil(err, "No error expected.")

	runID1, err := s.GetCurrentWorkflowRunID(domainID, workflowID)
	s.Equal(workflowExecutionCurrent1.GetRunId(), runID1)
	state, err = s.GetWorkflowExecutionInfo(domainID, workflowExecutionCurrent1)
	s.Nil(err)
	updatedInfo1 := copyWorkflowExecutionInfo(state.ExecutionInfo)
	updatedInfo1.State = WorkflowStateCompleted
	updatedInfo1.CloseStatus = WorkflowCloseStatusCompleted
	updatedInfo1.NextEventID = int64(6)
	updatedInfo1.LastProcessedEvent = int64(2)
	err3 := s.UpdateWorkflowExecutionAndFinish(updatedInfo1, int64(3), 123)
	s.Nil(err3, "No error expected.")
	runID1, err = s.GetCurrentWorkflowRunID(domainID, workflowID)
	s.Equal(workflowExecutionCurrent1.GetRunId(), runID1)

	resetExecutionInfo := &WorkflowExecutionInfo{
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
		State:                WorkflowStateRunning,
		NextEventID:          123,
		CreateRequestID:      uuid.New(),
		DecisionVersion:      common.EmptyVersion,
		DecisionScheduleID:   111,
		DecisionStartedID:    222,
		DecisionRequestID:    uuid.New(),
		DecisionTimeout:      0,
	}
	resetActivityInfos := []*ActivityInfo{}
	resetTimerInfos := []*TimerInfo{}
	resetChildExecutionInfos := []*ChildExecutionInfo{}
	resetRequestCancelInfos := []*RequestCancelInfo{}
	resetSignalInfos := []*SignalInfo{}
	rState := &ReplicationState{
		CurrentVersion: int64(8789),
		StartVersion:   int64(8780),
	}

	err = s.ResetMutableState(workflowExecutionCurrent1.GetRunId(), resetExecutionInfo, rState, continueAsNewInfo.NextEventID, resetActivityInfos, resetTimerInfos,
		resetChildExecutionInfos, resetRequestCancelInfos, resetSignalInfos, nil)
	s.Nil(err, "No error expected.")

	// this test only assert whether the current workflow execution record is reseted
	runID, err := s.GetCurrentWorkflowRunID(domainID, workflowID)
	s.Equal(workflowExecutionReset.GetRunId(), runID)

	state, err = s.GetWorkflowExecutionInfo(domainID, workflowExecutionReset)
	s.Nil(err)
	info = state.ExecutionInfo
	continueAsNewInfo = copyWorkflowExecutionInfo(info)
	continueAsNewInfo.State = WorkflowStateCompleted
	continueAsNewInfo.NextEventID += 3
	continueAsNewInfo.LastProcessedEvent += 2

	workflowExecutionCurrent2 := gen.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa2"),
	}
	err = s.ContinueAsNewExecution(continueAsNewInfo, info.NextEventID, workflowExecutionCurrent2, int64(3), int64(2))
	s.Nil(err, "No error expected.")

	runID2, err := s.GetCurrentWorkflowRunID(domainID, workflowID)
	s.Equal(workflowExecutionCurrent2.GetRunId(), runID2)

	err = s.ResetMutableState(workflowExecutionCurrent2.GetRunId(), resetExecutionInfo, rState, continueAsNewInfo.NextEventID, resetActivityInfos, resetTimerInfos,
		resetChildExecutionInfos, resetRequestCancelInfos, resetSignalInfos, nil)
	s.Nil(err, "No error expected.")

	// this test only assert whether the current workflow execution record is reseted
	runID, err = s.GetCurrentWorkflowRunID(domainID, workflowID)
	s.Equal(workflowExecutionReset.GetRunId(), runID)
}

func copyWorkflowExecutionInfo(sourceInfo *WorkflowExecutionInfo) *WorkflowExecutionInfo {
	return &WorkflowExecutionInfo{
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
		NextEventID:          sourceInfo.NextEventID,
		LastProcessedEvent:   sourceInfo.LastProcessedEvent,
		LastUpdatedTimestamp: sourceInfo.LastUpdatedTimestamp,
		CreateRequestID:      sourceInfo.CreateRequestID,
		DecisionVersion:      sourceInfo.DecisionVersion,
		DecisionScheduleID:   sourceInfo.DecisionScheduleID,
		DecisionStartedID:    sourceInfo.DecisionStartedID,
		DecisionRequestID:    sourceInfo.DecisionRequestID,
		DecisionTimeout:      sourceInfo.DecisionTimeout,
	}
}

func (s *cassandraPersistenceSuite) TestCreateGetShard_Backfill() {
	shardID := 4
	rangeID := int64(59)

	// test create && get
	currentReplicationAck := int64(27)
	currentClusterTransferAck := int64(21)
	currentClusterTimerAck := timestampConvertor(time.Now().Add(-10 * time.Second))
	shardInfo := &ShardInfo{
		ShardID:             shardID,
		Owner:               "some random owner",
		RangeID:             rangeID,
		StolenSinceRenew:    12,
		UpdatedAt:           timestampConvertor(time.Now()),
		ReplicationAckLevel: currentReplicationAck,
		TransferAckLevel:    currentClusterTransferAck,
		TimerAckLevel:       currentClusterTimerAck,
	}
	createRequest := &CreateShardRequest{
		ShardInfo: shardInfo,
	}
	s.Nil(s.ShardMgr.CreateShard(createRequest))

	shardInfo.ClusterTransferAckLevel = map[string]int64{
		s.ClusterMetadata.GetCurrentClusterName(): currentClusterTransferAck,
	}
	shardInfo.ClusterTimerAckLevel = map[string]time.Time{
		s.ClusterMetadata.GetCurrentClusterName(): currentClusterTimerAck,
	}
	resp, err := s.ShardMgr.GetShard(&GetShardRequest{ShardID: shardID})
	s.Nil(err)
	s.True(timeComparator(shardInfo.UpdatedAt, resp.ShardInfo.UpdatedAt, timePrecision))
	s.True(timeComparator(shardInfo.ClusterTimerAckLevel[cluster.TestCurrentClusterName], resp.ShardInfo.ClusterTimerAckLevel[cluster.TestCurrentClusterName], timePrecision))
	s.True(timeComparator(shardInfo.ClusterTimerAckLevel[cluster.TestAlternativeClusterName], resp.ShardInfo.ClusterTimerAckLevel[cluster.TestAlternativeClusterName], timePrecision))
	resp.ShardInfo.UpdatedAt = shardInfo.UpdatedAt
	resp.ShardInfo.ClusterTimerAckLevel = shardInfo.ClusterTimerAckLevel
	s.Equal(shardInfo, resp.ShardInfo)
}

func (s *cassandraPersistenceSuite) TestCreateGetUpdateGetShard() {
	shardID := 8
	rangeID := int64(59)

	// test create && get
	currentReplicationAck := int64(27)
	currentClusterTransferAck := int64(21)
	alternativeClusterTransferAck := int64(32)
	currentClusterTimerAck := timestampConvertor(time.Now().Add(-10 * time.Second))
	alternativeClusterTimerAck := timestampConvertor(time.Now().Add(-20 * time.Second))
	domainNotificationVersion := int64(8192)
	shardInfo := &ShardInfo{
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
	}
	createRequest := &CreateShardRequest{
		ShardInfo: shardInfo,
	}
	s.Nil(s.ShardMgr.CreateShard(createRequest))

	resp, err := s.ShardMgr.GetShard(&GetShardRequest{ShardID: shardID})
	s.Nil(err)
	s.True(timeComparator(shardInfo.UpdatedAt, resp.ShardInfo.UpdatedAt, timePrecision))
	s.True(timeComparator(shardInfo.ClusterTimerAckLevel[cluster.TestCurrentClusterName], resp.ShardInfo.ClusterTimerAckLevel[cluster.TestCurrentClusterName], timePrecision))
	s.True(timeComparator(shardInfo.ClusterTimerAckLevel[cluster.TestAlternativeClusterName], resp.ShardInfo.ClusterTimerAckLevel[cluster.TestAlternativeClusterName], timePrecision))
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
	shardInfo = &ShardInfo{
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
	}
	updateRequest := &UpdateShardRequest{
		ShardInfo:       shardInfo,
		PreviousRangeID: rangeID,
	}
	s.Nil(s.ShardMgr.UpdateShard(updateRequest))

	resp, err = s.ShardMgr.GetShard(&GetShardRequest{ShardID: shardID})
	s.Nil(err)
	s.True(timeComparator(shardInfo.UpdatedAt, resp.ShardInfo.UpdatedAt, timePrecision))
	s.True(timeComparator(shardInfo.ClusterTimerAckLevel[cluster.TestCurrentClusterName], resp.ShardInfo.ClusterTimerAckLevel[cluster.TestCurrentClusterName], timePrecision))
	s.True(timeComparator(shardInfo.ClusterTimerAckLevel[cluster.TestAlternativeClusterName], resp.ShardInfo.ClusterTimerAckLevel[cluster.TestAlternativeClusterName], timePrecision))
	resp.ShardInfo.UpdatedAt = shardInfo.UpdatedAt
	resp.ShardInfo.ClusterTimerAckLevel = shardInfo.ClusterTimerAckLevel
	s.Equal(shardInfo, resp.ShardInfo)
}

// Note: cassandra only provide millisecond precision timestamp
// ref: https://docs.datastax.com/en/cql/3.3/cql/cql_reference/timestamp_type_r.html
// so to use equal function, we need to do conversion, getting rid of sub milliseconds
func timestampConvertor(t time.Time) time.Time {
	return time.Unix(
		0,
		common.CQLTimestampToUnixNano(common.UnixNanoToCQLTimestamp(t.UnixNano())),
	).UTC()
}

func timeComparator(t1 time.Time, t2 time.Time, timeTolerance time.Duration) bool {
	tolerance := timeTolerance.Nanoseconds()
	if t1.UnixNano()-t2.UnixNano() < tolerance && t2.UnixNano()-t1.UnixNano() < tolerance {
		return true
	}
	return false
}

func copyReplicationState(sourceState *ReplicationState) *ReplicationState {
	state := &ReplicationState{
		CurrentVersion:   sourceState.CurrentVersion,
		StartVersion:     sourceState.StartVersion,
		LastWriteVersion: sourceState.LastWriteVersion,
		LastWriteEventID: sourceState.LastWriteEventID,
	}
	if sourceState.LastReplicationInfo != nil {
		state.LastReplicationInfo = map[string]*ReplicationInfo{}
		for k, v := range sourceState.LastReplicationInfo {
			state.LastReplicationInfo[k] = copyReplicationInfo(v)
		}
	}

	return state
}

func copyReplicationInfo(sourceInfo *ReplicationInfo) *ReplicationInfo {
	return &ReplicationInfo{
		Version:     sourceInfo.Version,
		LastEventID: sourceInfo.LastEventID,
	}
}

func (s *cassandraPersistenceSuite) serializeHistoryEvents(events []*gen.HistoryEvent) *SerializedHistoryEventBatch {
	historySerializer := NewJSONHistorySerializer()
	bufferedBatch := NewHistoryEventBatch(GetDefaultHistoryVersion(), events)
	serializedEvents, _ := historySerializer.Serialize(bufferedBatch)

	return serializedEvents
}

func (s *cassandraPersistenceSuite) deserializedHistoryEvents(batch *SerializedHistoryEventBatch) *HistoryEventBatch {
	historySerializer := NewJSONHistorySerializer()
	events, err := historySerializer.Deserialize(batch)
	if err != nil {
		panic(err)
	}

	return events
}
