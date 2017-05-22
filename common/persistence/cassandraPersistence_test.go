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
	"math"
	"os"
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	gen "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
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
	s.ClearTransferQueue()
}

func (s *cassandraPersistenceSuite) TestPersistenceStartWorkflow() {
	domainID := "2d7994bf-9de8-459d-9c81-e723daedb246"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("start-workflow-test"),
		RunId:      common.StringPtr("7f9fe8a0-9237-11e6-ae22-56b6b6499611"),
	}
	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "queue1", "wType", 13, nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

	task1, err1 := s.CreateWorkflowExecution(domainID, workflowExecution, "queue1", "wType1", 14, nil, 3, 0, 2, nil)
	s.NotNil(err1, "Expected workflow creation to fail.")
	log.Infof("Unable to start workflow execution: %v", err1)
	startedErr, ok := err1.(*gen.WorkflowExecutionAlreadyStartedError)
	s.True(ok)
	s.Equal(workflowExecution.GetRunId(), startedErr.GetRunId(), startedErr.GetMessage())
	s.Empty(task1, "Expected empty task identifier.")

	response, err2 := s.WorkflowMgr.CreateWorkflowExecution(&CreateWorkflowExecutionRequest{
		RequestID:            uuid.New(),
		DomainID:             domainID,
		Execution:            workflowExecution,
		TaskList:             "queue1",
		WorkflowTypeName:     "workflow_type_test",
		DecisionTimeoutValue: 13,
		ExecutionContext:     nil,
		NextEventID:          int64(3),
		LastProcessedEvent:   0,
		RangeID:              s.ShardContext.GetRangeID() - 1,
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
	log.Infof("Unable to start workflow execution: %v", err1)
	s.IsType(&ShardOwnershipLostError{}, err2)
}

func (s *cassandraPersistenceSuite) TestGetWorkflow() {
	domainID := "8f27f02b-ce22-4fd9-941b-65e1131b0bb5"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("get-workflow-test"),
		RunId:      common.StringPtr("918e7b1d-bfa4-4fe0-86cb-604858f90ce4"),
	}
	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "queue1", "wType", 13, nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

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
	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "queue1", "wType", 13, nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err1, "No error expected.")
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")
	s.Equal(domainID, info0.DomainID)
	s.Equal("update-workflow-test", info0.WorkflowID)
	s.Equal("5ba5e531-e46b-48d9-b4b3-859919839553", info0.RunID)
	s.Equal("queue1", info0.TaskList)
	s.Equal("wType", info0.WorkflowTypeName)
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

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedInfo.NextEventID = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)
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
	s.Equal(int32(13), info1.DecisionTimeoutValue)
	s.Equal([]byte(nil), info1.ExecutionContext)
	s.Equal(WorkflowStateCreated, info1.State)
	s.Equal(int64(5), info1.NextEventID)
	s.Equal(int64(2), info1.LastProcessedEvent)
	s.Equal(true, validateTimeRange(info1.LastUpdatedTimestamp, time.Hour))
	s.Equal(int64(2), info1.DecisionScheduleID)
	s.Equal(common.EmptyEventID, info1.DecisionStartedID)
	s.Equal(int32(1), info1.DecisionTimeout)

	log.Infof("Workflow execution last updated: %v", info1.LastUpdatedTimestamp)

	failedUpdatedInfo := copyWorkflowExecutionInfo(info0)
	failedUpdatedInfo.NextEventID = int64(6)
	failedUpdatedInfo.LastProcessedEvent = int64(3)
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
	s.Equal(int32(13), info2.DecisionTimeoutValue)
	s.Equal([]byte(nil), info2.ExecutionContext)
	s.Equal(WorkflowStateCreated, info2.State)
	s.Equal(int64(5), info2.NextEventID)
	s.Equal(int64(2), info2.LastProcessedEvent)
	s.Equal(true, validateTimeRange(info2.LastUpdatedTimestamp, time.Hour))
	s.Equal(int64(2), info2.DecisionScheduleID)
	s.Equal(common.EmptyEventID, info2.DecisionStartedID)
	s.Equal(int32(1), info2.DecisionTimeout)

	log.Infof("Workflow execution last updated: %v", info2.LastUpdatedTimestamp)

	failedUpdatedInfo2 := copyWorkflowExecutionInfo(info1)
	failedUpdatedInfo2.NextEventID = int64(6)
	failedUpdatedInfo2.LastProcessedEvent = int64(3)
	err5 := s.UpdateWorkflowExecutionWithRangeID(updatedInfo, []int64{int64(5)}, nil, int64(12345), int64(5), nil, nil, nil, nil, nil, nil, nil, nil)
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
	s.Equal(int32(13), info3.DecisionTimeoutValue)
	s.Equal([]byte(nil), info3.ExecutionContext)
	s.Equal(WorkflowStateCreated, info3.State)
	s.Equal(int64(5), info3.NextEventID)
	s.Equal(int64(2), info3.LastProcessedEvent)
	s.Equal(true, validateTimeRange(info3.LastUpdatedTimestamp, time.Hour))
	s.Equal(int64(2), info3.DecisionScheduleID)
	s.Equal(common.EmptyEventID, info3.DecisionStartedID)
	s.Equal(int32(1), info1.DecisionTimeout)

	log.Infof("Workflow execution last updated: %v", info3.LastUpdatedTimestamp)
}

func (s *cassandraPersistenceSuite) TestDeleteWorkflow() {
	domainID := "1d4abb23-b87b-457b-96ef-43aba0b9c44f"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("delete-workflow-test"),
		RunId:      common.StringPtr("4e0917f2-9361-4a14-b16f-1fafe09b287a"),
	}
	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "queue1", "wType", 13, nil, 3, 0, 2, nil)
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

	state1, err3 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err3, "No error expected.")
	info1 := state1.ExecutionInfo
	s.NotNil(info1, "Valid Workflow info expected.")
	s.Equal(domainID, info1.DomainID)
	s.Equal("delete-workflow-test", info1.WorkflowID)
	s.Equal("4e0917f2-9361-4a14-b16f-1fafe09b287a", info1.RunID)
	s.Equal("queue1", info1.TaskList)
	s.Equal("wType", info1.WorkflowTypeName)
	s.Equal(int32(13), info1.DecisionTimeoutValue)
	s.Equal([]byte(nil), info1.ExecutionContext)
	s.Equal(WorkflowStateCreated, info1.State)
	s.Equal(int64(3), info1.NextEventID)
	s.Equal(int64(0), info1.LastProcessedEvent)
	s.Equal(true, validateTimeRange(info1.LastUpdatedTimestamp, time.Hour))
	s.Equal(int64(2), info1.DecisionScheduleID)
	s.Equal(common.EmptyEventID, info1.DecisionStartedID)
	s.Equal(int32(1), info1.DecisionTimeout)
	log.Infof("Workflow execution last updated: %v", info1.LastUpdatedTimestamp)

	err5 := s.DeleteWorkflowExecution(info1)
	s.Nil(err5, "No error expected.")
}

func (s *cassandraPersistenceSuite) TestGetCurrentWorkflow() {
	domainID := "54d15308-e20e-4b91-a00f-a518a3892790"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("get-current-workflow-test"),
		RunId:      common.StringPtr("6cae4054-6ba7-46d3-8755-e3c2db6f74ea"),
	}
	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "queue1", "wType", 13, nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

	runID0, err1 := s.GetCurrentWorkflow(domainID, workflowExecution.GetWorkflowId())
	s.Nil(err1, "No error expected.")
	s.Equal(workflowExecution.GetRunId(), runID0)

	info0, err2 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err2)

	updatedInfo1 := copyWorkflowExecutionInfo(info0.ExecutionInfo)
	updatedInfo1.NextEventID = int64(6)
	updatedInfo1.LastProcessedEvent = int64(2)
	err3 := s.UpdateWorkflowExecutionAndDelete(updatedInfo1, int64(3))
	s.Nil(err3, "No error expected.")

	_, err4 := s.GetCurrentWorkflow(domainID, workflowExecution.GetWorkflowId())
	s.NotNil(err4, "No error expected.")
	s.IsType(&gen.EntityNotExistsError{}, err4)

	workflowExecution2 := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("get-current-workflow-test"),
		RunId:      common.StringPtr("c3ff4bc6-de18-4643-83b2-037a33f45322"),
	}
	task1, err5 := s.CreateWorkflowExecution(domainID, workflowExecution2, "queue1", "wType", 13, nil, 3, 0, 2, nil)
	s.Nil(err5, "No error expected.")
	s.NotEmpty(task1, "Expected non empty task identifier.")

	runID1, err6 := s.GetCurrentWorkflow(domainID, workflowExecution2.GetWorkflowId())
	s.Nil(err6, "No error expected.")
	s.Equal(workflowExecution2.GetRunId(), runID1)
}

func (s *cassandraPersistenceSuite) TestTransferTasks() {
	domainID := "1eda632b-dde5-4cb2-94fd-5a6f04e6dfcd"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("get-transfer-tasks-test"),
		RunId:      common.StringPtr("93c87aff-ed89-4ecb-b0fd-d5d1e25dc46d"),
	}

	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "queue1", "wType", 13, nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

	tasks1, err1 := s.GetTransferTasks(1)
	s.Nil(err1, "No error expected.")
	s.NotNil(tasks1, "expected valid list of tasks.")
	s.Equal(1, len(tasks1), "Expected 1 decision task.")
	task1 := tasks1[0]
	s.Equal(domainID, task1.DomainID)
	s.Equal(workflowExecution.GetWorkflowId(), task1.WorkflowID)
	s.Equal(workflowExecution.GetRunId(), task1.RunID)
	s.Equal("queue1", task1.TaskList)
	s.Equal(TransferTaskTypeDecisionTask, task1.TaskType)
	s.Equal(int64(2), task1.ScheduleID)
	s.Equal(transferTaskTransferTargetWorkflowID, task1.TargetWorkflowID)
	s.Equal(transferTaskTypeTransferTargetRunID, task1.TargetRunID)

	err3 := s.CompleteTransferTask(task1.TaskID)
	s.Nil(err3)

	// no-op to complete the task again
	err4 := s.CompleteTransferTask(task1.TaskID)
	s.Nil(err4)
}

func (s *cassandraPersistenceSuite) TestTransferTasksThroughUpdate() {
	domainID := "b785a8ba-bd7d-4760-bb05-41b115f3e10a"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("get-transfer-tasks-through-update-test"),
		RunId:      common.StringPtr("30a9fa1f-0db1-4d7a-8c34-aa82c5dad3aa"),
	}

	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "queue1", "wType", 13, nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

	tasks1, err1 := s.GetTransferTasks(1)
	s.Nil(err1, "No error expected.")
	s.NotNil(tasks1, "expected valid list of tasks.")
	s.Equal(1, len(tasks1), "Expected 1 decision task.")
	task1 := tasks1[0]
	s.Equal(domainID, task1.DomainID)
	s.Equal(workflowExecution.GetWorkflowId(), task1.WorkflowID)
	s.Equal(workflowExecution.GetRunId(), task1.RunID)
	s.Equal("queue1", task1.TaskList)
	s.Equal(TransferTaskTypeDecisionTask, task1.TaskType)
	s.Equal(int64(2), task1.ScheduleID)

	err3 := s.CompleteTransferTask(task1.TaskID)
	s.Nil(err3)

	state0, _ := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	info0 := state0.ExecutionInfo
	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedInfo.NextEventID = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)
	err2 := s.UpdateWorkflowExecution(updatedInfo, nil, []int64{int64(4)}, int64(3), nil, nil, nil, nil, nil, nil)
	s.Nil(err2, "No error expected.")

	tasks2, err1 := s.GetTransferTasks(1)
	s.Nil(err1, "No error expected.")
	s.NotNil(tasks2, "expected valid list of tasks.")
	s.Equal(1, len(tasks2), "Expected 1 decision task.")
	task2 := tasks2[0]
	s.Equal(domainID, task2.DomainID)
	s.Equal(workflowExecution.GetWorkflowId(), task2.WorkflowID)
	s.Equal(workflowExecution.GetRunId(), task2.RunID)
	s.Equal("queue1", task2.TaskList)
	s.Equal(TransferTaskTypeActivityTask, task2.TaskType)
	s.Equal(int64(4), task2.ScheduleID)

	err4 := s.CompleteTransferTask(task2.TaskID)
	s.Nil(err4)

	state1, _ := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	info1 := state1.ExecutionInfo
	updatedInfo1 := copyWorkflowExecutionInfo(info1)
	updatedInfo1.NextEventID = int64(6)
	updatedInfo1.LastProcessedEvent = int64(2)
	err5 := s.UpdateWorkflowExecutionAndDelete(updatedInfo1, int64(5))
	s.Nil(err5, "No error expected.")

	newExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("get-transfer-tasks-through-update-test"),
		RunId:      common.StringPtr("2a038c8f-b575-4151-8d2c-d443e999ab5a"),
	}
	_, err6 := s.GetCurrentWorkflow(domainID, "get-transfer-tasks-through-update-test")
	s.NotNil(err6, "Entity exist error expected.")

	tasks3, err7 := s.GetTransferTasks(1)
	s.Nil(err7, "No error expected.")
	s.NotNil(tasks3, "expected valid list of tasks.")
	s.Equal(1, len(tasks3), "Expected 1 decision task.")
	task3 := tasks3[0]
	s.Equal(domainID, task3.DomainID)
	s.Equal(workflowExecution.GetWorkflowId(), task3.WorkflowID)
	s.Equal(workflowExecution.GetRunId(), task3.RunID)
	s.Equal(TransferTaskTypeDeleteExecution, task3.TaskType)

	err8 := s.DeleteWorkflowExecution(info1)
	s.Nil(err8)

	err9 := s.CompleteTransferTask(task3.TaskID)
	s.Nil(err9)

	_, err10 := s.CreateWorkflowExecution(domainID, newExecution, "queue1", "wType", 13, nil, 3, 0, 2, nil)
	s.Nil(err10, "No error expected.")
}

func (s *cassandraPersistenceSuite) TestCancelTransferTaskTasks() {
	domainID := "aeac8287-527b-4b35-80a9-667cb47e7c6d"
	workflowExecution := gen.WorkflowExecution{WorkflowId: common.StringPtr("get-decision-task-test"),
		RunId: common.StringPtr("db20f7e2-1a1e-40d9-9278-d8b886738e05")}

	task0, err := s.CreateWorkflowExecution(domainID, workflowExecution, "queue1", "wType", 13, nil, 3, 0, 2, nil)
	s.Nil(err, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

	taskD, err := s.GetTransferTasks(1)
	s.Equal(1, len(taskD), "Expected 1 decision task.")
	err = s.CompleteTransferTask(taskD[0].TaskID)
	s.Nil(err)

	state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err1, "No error expected.")
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")
	updatedInfo := copyWorkflowExecutionInfo(info0)

	targetDomainID := "f2bfaab6-7e8b-4fac-9a62-17da8d37becb"
	targetWorkflowID := "target-workflow_id"
	targetRunID := "0d00698f-08e1-4d36-a3e2-3bf109f5d2d6"
	transferTasks := []Task{&CancelExecutionTask{
		TaskID:           s.GetNextSequenceNumber(),
		TargetDomainID:   targetDomainID,
		TargetWorkflowID: targetWorkflowID,
		TargetRunID:      targetRunID,
		ScheduleID:       1,
	}}
	err1 = s.UpdateWorkflowExecutionWithTransferTasks(updatedInfo, int64(3), transferTasks, nil)
	s.Nil(err1, "No error expected.")

	tasks, err := s.GetTransferTasks(1)
	s.Nil(err, "No error expected.")
	s.NotNil(tasks, "expected valid list of tasks.")
	s.Equal(1, len(tasks), "Expected 1 cancel task.")
	task := tasks[0]
	s.Equal(TransferTaskTypeCancelExecution, task.TaskType)
	s.Equal(domainID, task.DomainID)
	s.Equal(workflowExecution.GetWorkflowId(), task.WorkflowID)
	s.Equal(workflowExecution.GetRunId(), task.RunID)
	s.Equal(targetDomainID, task.TargetDomainID)
	s.Equal(targetWorkflowID, task.TargetWorkflowID)
	s.Equal(targetRunID, task.TargetRunID)

	err = s.CompleteTransferTask(task.TaskID)
	s.Nil(err)
}

func (s *cassandraPersistenceSuite) TestCreateTask() {
	domainID := "11adbd1b-f164-4ea7-b2f3-2e857a5048f1"
	workflowExecution := gen.WorkflowExecution{WorkflowId: common.StringPtr("create-task-test"),
		RunId: common.StringPtr("c949447a-691a-4132-8b2a-a5b38106793c")}
	task0, err0 := s.CreateDecisionTask(domainID, workflowExecution, "a5b38106793c", 5)
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

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
	s.NotEmpty(task0, "Expected non empty task identifier.")

	tasks1Response, err1 := s.GetTasks(domainID, taskList, TaskListTypeDecision, 1)
	s.Nil(err1, "No error expected.")
	s.NotNil(tasks1Response.Tasks, "expected valid list of tasks.")
	s.Equal(1, len(tasks1Response.Tasks), "Expected 1 decision task.")
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
		s.Equal(workflowExecution.GetWorkflowId(), t.WorkflowID)
		s.Equal(workflowExecution.GetRunId(), t.RunID)
		s.True(t.TaskID > 0)

		err2 := s.CompleteTask(domainID, taskList, TaskListTypeActivity, t.TaskID, 100)
		s.Nil(err2)
	}
}

func (s *cassandraPersistenceSuite) TestLeaseTaskList() {
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
}

func (s *cassandraPersistenceSuite) TestTimerTasks() {
	domainID := "8bfb47be-5b57-4d66-9109-5fb35e20b1d7"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("get-timer-tasks-test"),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
	}

	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "taskList", "wType", 13, nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err1, "No error expected.")
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedInfo.NextEventID = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)
	tasks := []Task{&DecisionTimeoutTask{1, 2}}
	err2 := s.UpdateWorkflowExecution(updatedInfo, []int64{int64(4)}, nil, int64(3), tasks, nil, nil, nil, nil, nil)
	s.Nil(err2, "No error expected.")

	timerTasks, err1 := s.GetTimerIndexTasks(-1, math.MaxInt64)
	s.Nil(err1, "No error expected.")
	s.NotNil(timerTasks, "expected valid list of tasks.")

	err2 = s.UpdateWorkflowExecution(updatedInfo, nil, nil, int64(5), nil, &DecisionTimeoutTask{TaskID: timerTasks[0].TaskID}, nil, nil, nil, nil)
	s.Nil(err2, "No error expected.")

	timerTasks2, err2 := s.GetTimerIndexTasks(-1, math.MaxInt64)
	s.Nil(err2, "No error expected.")
	s.Empty(timerTasks2, "expected empty task list.")
}

func (s *cassandraPersistenceSuite) TestWorkflowMutableState_Activities() {
	domainID := "7fcf0aa9-e121-4292-bdad-0a75181b4aa3"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("test-workflow-mutable-test"),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
	}

	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "taskList", "wType", 13, nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err1, "No error expected.")
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedInfo.NextEventID = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)
	currentTime := time.Now().UTC()
	activityInfos := []*ActivityInfo{
		{
			ScheduleID:               1,
			ScheduledEvent:           []byte("scheduled_event_1"),
			StartedID:                2,
			StartedEvent:             []byte("started_event_1"),
			ScheduleToCloseTimeout:   1,
			ScheduleToStartTimeout:   2,
			StartToCloseTimeout:      3,
			HeartbeatTimeout:         4,
			LastHeartBeatUpdatedTime: currentTime,
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
	s.Equal(int64(1), ai.ScheduleID)
	s.Equal([]byte("scheduled_event_1"), ai.ScheduledEvent)
	s.Equal(int64(2), ai.StartedID)
	s.Equal([]byte("started_event_1"), ai.StartedEvent)
	s.Equal(int32(1), ai.ScheduleToCloseTimeout)
	s.Equal(int32(2), ai.ScheduleToStartTimeout)
	s.Equal(int32(3), ai.StartToCloseTimeout)
	s.Equal(int32(4), ai.HeartbeatTimeout)
	s.Equal(currentTime.Unix(), ai.LastHeartBeatUpdatedTime.Unix())

	err2 = s.UpdateWorkflowExecution(updatedInfo, nil, nil, int64(5), nil, nil, nil, common.Int64Ptr(1), nil, nil)
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

	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "taskList", "wType", 13, nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err1, "No error expected.")
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedInfo.NextEventID = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)
	currentTime := time.Now().UTC()
	timerID := "id_1"
	timerInfos := []*TimerInfo{{TimerID: timerID, ExpiryTime: currentTime, TaskID: 2, StartedID: 5}}
	err2 := s.UpdateWorkflowExecution(updatedInfo, []int64{int64(4)}, nil, int64(3), nil, nil, nil, nil, timerInfos, nil)
	s.Nil(err2, "No error expected.")

	state, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err1, "No error expected.")
	s.NotNil(state, "expected valid state.")
	s.Equal(1, len(state.TimerInfos))
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

	task0, err0 := s.CreateChildWorkflowExecution(domainID, workflowExecution, parentDomainID, parentExecution, 1, "taskList", "wType", 13, nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

	state0, err1 := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err1, "No error expected.")
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")
	s.Equal(parentDomainID, info0.ParentDomainID)
	s.Equal(parentExecution.GetWorkflowId(), info0.ParentWorkflowID)
	s.Equal(parentExecution.GetRunId(), info0.ParentRunID)
	s.Equal(int64(1), info0.InitiatedID)

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedInfo.NextEventID = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)
	createRequestID := uuid.New()
	childExecutionInfos := []*ChildExecutionInfo{
		{
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

func (s *cassandraPersistenceSuite) TestWorkflowMutableStateInfo() {
	domainID := "9ed8818b-3090-4160-9f21-c6b70e64d2dd"
	workflowExecution := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("test-workflow-mutable-state-test"),
		RunId:      common.StringPtr("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
	}

	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "taskList", "wType", 13, nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

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

	_, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, "queue1", "wType", 13, nil, 3, 0, 2, nil)
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

	newRunID, err5 := s.GetCurrentWorkflow(domainID, workflowExecution.GetWorkflowId())
	s.Nil(err5)
	s.Equal(newWorkflowExecution.GetRunId(), newRunID)
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
		DecisionTimeoutValue: sourceInfo.DecisionTimeoutValue,
		ExecutionContext:     sourceInfo.ExecutionContext,
		State:                sourceInfo.State,
		NextEventID:          sourceInfo.NextEventID,
		LastProcessedEvent:   sourceInfo.LastProcessedEvent,
		LastUpdatedTimestamp: sourceInfo.LastUpdatedTimestamp,
		CreateRequestID:      sourceInfo.CreateRequestID,
		DecisionScheduleID:   sourceInfo.DecisionScheduleID,
		DecisionStartedID:    sourceInfo.DecisionStartedID,
		DecisionRequestID:    sourceInfo.DecisionRequestID,
		DecisionTimeout:      sourceInfo.DecisionTimeout,
	}
}
