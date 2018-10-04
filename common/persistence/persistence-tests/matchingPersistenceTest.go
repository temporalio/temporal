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
	"os"
	"testing"
	"time"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	gen "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	p "github.com/uber/cadence/common/persistence"
)

type (
	// MatchingPersistenceSuite contains matching persistence tests
	MatchingPersistenceSuite struct {
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

// SetupSuite implementation
func (s *MatchingPersistenceSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}
}

// TearDownSuite implementation
func (s *MatchingPersistenceSuite) TearDownSuite() {
	s.TearDownWorkflowStore()
}

// SetupTest implementation
func (s *MatchingPersistenceSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
}

// TestCreateTask test
func (s *MatchingPersistenceSuite) TestCreateTask() {
	domainID := "11adbd1b-f164-4ea7-b2f3-2e857a5048f1"
	workflowExecution := gen.WorkflowExecution{WorkflowId: common.StringPtr("create-task-test"),
		RunId: common.StringPtr("c949447a-691a-4132-8b2a-a5b38106793c")}
	task0, err0 := s.CreateDecisionTask(domainID, workflowExecution, "a5b38106793c", 5)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	tasks1, err1 := s.CreateActivityTasks(domainID, workflowExecution, map[int64]string{
		10: "a5b38106793c"})
	s.NoError(err1)
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
	s.NoError(err2)
	s.Equal(5, len(tasks2), "expected single valid task identifier.")
	for _, t := range tasks2 {
		s.NotEmpty(t, "Expected non empty task identifier.")
	}
}

// TestGetDecisionTasks test
func (s *MatchingPersistenceSuite) TestGetDecisionTasks() {
	domainID := "aeac8287-527b-4b35-80a9-667cb47e7c6d"
	workflowExecution := gen.WorkflowExecution{WorkflowId: common.StringPtr("get-decision-task-test"),
		RunId: common.StringPtr("db20f7e2-1a1e-40d9-9278-d8b886738e05")}
	taskList := "d8b886738e05"
	task0, err0 := s.CreateDecisionTask(domainID, workflowExecution, taskList, 5)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	tasks1Response, err1 := s.GetTasks(domainID, taskList, p.TaskListTypeDecision, 1)
	s.NoError(err1)
	s.NotNil(tasks1Response.Tasks, "expected valid list of tasks.")
	s.Equal(1, len(tasks1Response.Tasks), "Expected 1 decision task.")
	s.Equal(int64(5), tasks1Response.Tasks[0].ScheduleID)
}

// TestCompleteDecisionTask test
func (s *MatchingPersistenceSuite) TestCompleteDecisionTask() {
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
	s.NoError(err0)
	s.NotNil(tasks0, "Expected non empty task identifier.")
	s.Equal(5, len(tasks0), "expected 5 valid task identifier.")
	for _, t := range tasks0 {
		s.NotEmpty(t, "Expected non empty task identifier.")
	}

	tasksWithID1Response, err1 := s.GetTasks(domainID, taskList, p.TaskListTypeActivity, 5)

	s.NoError(err1)
	tasksWithID1 := tasksWithID1Response.Tasks
	s.NotNil(tasksWithID1, "expected valid list of tasks.")

	s.Equal(5, len(tasksWithID1), "Expected 5 activity tasks.")
	for _, t := range tasksWithID1 {
		s.Equal(domainID, t.DomainID)
		s.Equal(*workflowExecution.WorkflowId, t.WorkflowID)
		s.Equal(*workflowExecution.RunId, t.RunID)
		s.True(t.TaskID > 0)

		err2 := s.CompleteTask(domainID, taskList, p.TaskListTypeActivity, t.TaskID, 100)
		s.NoError(err2)
	}
}

// TestLeaseAndUpdateTaskList test
func (s *MatchingPersistenceSuite) TestLeaseAndUpdateTaskList() {
	domainID := "00136543-72ad-4615-b7e9-44bca9775b45"
	taskList := "aaaaaaa"
	response, err := s.TaskMgr.LeaseTaskList(&p.LeaseTaskListRequest{
		DomainID: domainID,
		TaskList: taskList,
		TaskType: p.TaskListTypeActivity,
	})
	s.NoError(err)
	tli := response.TaskListInfo
	s.EqualValues(1, tli.RangeID)
	s.EqualValues(0, tli.AckLevel)

	response, err = s.TaskMgr.LeaseTaskList(&p.LeaseTaskListRequest{
		DomainID: domainID,
		TaskList: taskList,
		TaskType: p.TaskListTypeActivity,
	})
	s.NoError(err)
	tli = response.TaskListInfo
	s.EqualValues(2, tli.RangeID)
	s.EqualValues(0, tli.AckLevel)

	taskListInfo := &p.TaskListInfo{
		DomainID: domainID,
		Name:     taskList,
		TaskType: p.TaskListTypeActivity,
		RangeID:  2,
		AckLevel: 0,
		Kind:     p.TaskListKindNormal,
	}
	_, err = s.TaskMgr.UpdateTaskList(&p.UpdateTaskListRequest{
		TaskListInfo: taskListInfo,
	})
	s.NoError(err)

	taskListInfo.RangeID = 3
	_, err = s.TaskMgr.UpdateTaskList(&p.UpdateTaskListRequest{
		TaskListInfo: taskListInfo,
	})
	s.Error(err)
}

// TestLeaseAndUpdateTaskListSticky test
func (s *MatchingPersistenceSuite) TestLeaseAndUpdateTaskListSticky() {
	domainID := uuid.New()
	taskList := "aaaaaaa"
	response, err := s.TaskMgr.LeaseTaskList(&p.LeaseTaskListRequest{
		DomainID:     domainID,
		TaskList:     taskList,
		TaskType:     p.TaskListTypeDecision,
		TaskListKind: p.TaskListKindSticky,
	})
	s.NoError(err)
	tli := response.TaskListInfo
	s.EqualValues(1, tli.RangeID)
	s.EqualValues(0, tli.AckLevel)
	s.EqualValues(p.TaskListKindSticky, tli.Kind)

	taskListInfo := &p.TaskListInfo{
		DomainID: domainID,
		Name:     taskList,
		TaskType: p.TaskListTypeDecision,
		RangeID:  2,
		AckLevel: 0,
		Kind:     p.TaskListKindSticky,
	}
	_, err = s.TaskMgr.UpdateTaskList(&p.UpdateTaskListRequest{
		TaskListInfo: taskListInfo,
	})
	s.NoError(err) // because update with ttl doesn't check rangeID
}
