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

package tasklist

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/zap"

	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	p "github.com/uber/cadence/common/persistence"
)

type (
	ScavengerTestSuite struct {
		suite.Suite
		taskListTable *mockTaskListTable
		taskTables    map[string]*mockTaskTable
		taskMgr       *mocks.TaskManager
		scvgr         *Scavenger
	}
)

var errTest = errors.New("transient error")

func TestScavengerTestSuite(t *testing.T) {
	suite.Run(t, new(ScavengerTestSuite))
}

func (s *ScavengerTestSuite) SetupTest() {
	s.taskMgr = &mocks.TaskManager{}
	s.taskListTable = &mockTaskListTable{}
	s.taskTables = make(map[string]*mockTaskTable)
	zapLogger, err := zap.NewDevelopment()
	if err != nil {
		s.Require().NoError(err)
	}
	logger := loggerimpl.NewLogger(zapLogger)
	s.scvgr = NewScavenger(s.taskMgr, metrics.NewClient(tally.NoopScope, metrics.Worker), logger)
	maxTasksPerJob = 4
	executorPollInterval = time.Millisecond * 50
}

func (s *ScavengerTestSuite) TestAllExpiredTasks() {
	nTasks := 32
	nTaskLists := 3
	for i := 0; i < nTaskLists; i++ {
		name := fmt.Sprintf("test-expired-tl-%v", i)
		s.taskListTable.generate(name, true)
		tt := newMockTaskTable()
		tt.generate(nTasks, true)
		s.taskTables[name] = tt
	}
	s.setupTaskMgrMocks()
	s.runScavenger()
	for tl, tbl := range s.taskTables {
		tasks := tbl.get(100)
		s.Equal(0, len(tasks), "failed to delete all expired tasks")
		s.Nil(s.taskListTable.get(tl), "failed to delete expired executorTask list")
	}
}

func (s *ScavengerTestSuite) TestAllAliveTasks() {
	nTasks := 32
	nTaskLists := 3
	for i := 0; i < nTaskLists; i++ {
		name := fmt.Sprintf("test-Alive-tl-%v", i)
		s.taskListTable.generate(name, true)
		tt := newMockTaskTable()
		tt.generate(nTasks, false)
		s.taskTables[name] = tt
	}
	s.setupTaskMgrMocks()
	s.runScavenger()
	for tl, tbl := range s.taskTables {
		tasks := tbl.get(100)
		s.Equal(nTasks, len(tasks), "scavenger deleted a non-expired executorTask")
		s.NotNil(s.taskListTable.get(tl), "scavenger deleted a non-expired executorTask list")
	}
}

func (s *ScavengerTestSuite) TestExpiredTasksFollowedByAlive() {
	nTasks := 32
	nTaskLists := 3
	for i := 0; i < nTaskLists; i++ {
		name := fmt.Sprintf("test-Alive-tl-%v", i)
		s.taskListTable.generate(name, true)
		tt := newMockTaskTable()
		tt.generate(nTasks/2, true)
		tt.generate(nTasks/2, false)
		s.taskTables[name] = tt
	}
	s.setupTaskMgrMocks()
	s.runScavenger()
	for tl, tbl := range s.taskTables {
		tasks := tbl.get(100)
		s.Equal(nTasks/2, len(tasks), "scavenger deleted non-expired tasks")
		s.Equal(int64(nTasks/2), tasks[0].TaskID, "scavenger deleted wrong set of tasks")
		s.NotNil(s.taskListTable.get(tl), "scavenger deleted a non-expired executorTask list")
	}
}

func (s *ScavengerTestSuite) TestAliveTasksFollowedByExpired() {
	nTasks := 32
	nTaskLists := 3
	for i := 0; i < nTaskLists; i++ {
		name := fmt.Sprintf("test-Alive-tl-%v", i)
		s.taskListTable.generate(name, true)
		tt := newMockTaskTable()
		tt.generate(nTasks/2, false)
		tt.generate(nTasks/2, true)
		s.taskTables[name] = tt
	}
	s.setupTaskMgrMocks()
	s.runScavenger()
	for tl, tbl := range s.taskTables {
		tasks := tbl.get(100)
		s.Equal(nTasks, len(tasks), "scavenger deleted non-expired tasks")
		s.NotNil(s.taskListTable.get(tl), "scavenger deleted a non-expired executorTask list")
	}
}

func (s *ScavengerTestSuite) TestAllExpiredTasksWithErrors() {
	nTasks := 32
	nTaskLists := 3
	for i := 0; i < nTaskLists; i++ {
		name := fmt.Sprintf("test-expired-tl-%v", i)
		s.taskListTable.generate(name, true)
		tt := newMockTaskTable()
		tt.generate(nTasks, true)
		s.taskTables[name] = tt
	}
	s.setupTaskMgrMocksWithErrors()
	s.runScavenger()
	for _, tbl := range s.taskTables {
		tasks := tbl.get(100)
		s.Equal(0, len(tasks), "failed to delete all expired tasks")
	}
	result, _ := s.taskListTable.list(nil, 10)
	s.Equal(1, len(result), "expected partial deletion due to transient errors")
}

func (s *ScavengerTestSuite) runScavenger() {
	s.scvgr.Start()
	timer := time.NewTimer(10 * time.Second)
	select {
	case <-s.scvgr.stopC:
		timer.Stop()
		return
	case <-timer.C:
		s.Fail("timed out waiting for scavenger to finish")
	}
}

func (s *ScavengerTestSuite) setupTaskMgrMocks() {
	s.taskMgr.On("ListTaskList", mock.Anything).Return(
		func(req *p.ListTaskListRequest) *p.ListTaskListResponse {
			items, next := s.taskListTable.list(req.PageToken, req.PageSize)
			return &p.ListTaskListResponse{Items: items, NextPageToken: next}
		}, nil)
	s.taskMgr.On("DeleteTaskList", mock.Anything).Return(
		func(req *p.DeleteTaskListRequest) error {
			s.taskListTable.delete(req.TaskListName)
			return nil
		})
	s.taskMgr.On("GetTasks", mock.Anything).Return(
		func(req *p.GetTasksRequest) *p.GetTasksResponse {
			result := s.taskTables[req.TaskList].get(req.BatchSize)
			return &p.GetTasksResponse{Tasks: result}
		}, nil)
	s.taskMgr.On("CompleteTasksLessThan", mock.Anything).Return(
		func(req *p.CompleteTasksLessThanRequest) int {
			return s.taskTables[req.TaskListName].deleteLessThan(req.TaskID, req.Limit)
		}, nil)
}

func (s *ScavengerTestSuite) setupTaskMgrMocksWithErrors() {
	s.taskMgr.On("ListTaskList", mock.Anything).Return(nil, errTest).Once()
	s.taskMgr.On("GetTasks", mock.Anything).Return(nil, errTest).Once()
	s.taskMgr.On("CompleteTasksLessThan", mock.Anything).Return(0, errTest).Once()
	s.taskMgr.On("DeleteTaskList", mock.Anything).Return(errTest).Once()
	s.setupTaskMgrMocks()
}
