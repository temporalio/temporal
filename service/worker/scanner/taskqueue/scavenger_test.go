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

package taskqueue

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
)

type (
	ScavengerTestSuite struct {
		suite.Suite

		controller *gomock.Controller
		taskMgr    *p.MockTaskManager

		taskQueueTable *mockTaskQueueTable
		taskTables     map[string]*mockTaskTable
		scvgr          *Scavenger
	}
)

var errTest = errors.New("transient error")

func TestScavengerTestSuite(t *testing.T) {
	suite.Run(t, new(ScavengerTestSuite))
}

func (s *ScavengerTestSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.taskMgr = p.NewMockTaskManager(s.controller)
	s.taskQueueTable = &mockTaskQueueTable{}
	s.taskTables = make(map[string]*mockTaskTable)
	logger := log.NewTestLogger()
	s.scvgr = NewScavenger(s.taskMgr, metrics.NoopClient, logger)
	maxTasksPerJob = 4
	executorPollInterval = time.Millisecond * 50
}

func (s *ScavengerTestSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *ScavengerTestSuite) TestAllExpiredTasks() {
	nTasks := 32
	nTaskQueues := 3
	for i := 0; i < nTaskQueues; i++ {
		name := fmt.Sprintf("test-expired-tq-%v", i)
		s.taskQueueTable.generate(name, true)
		tt := newMockTaskTable()
		tt.generate(nTasks, true)
		s.taskTables[name] = tt
	}
	s.setupTaskMgrMocks()
	s.runScavenger()
	for tl, tbl := range s.taskTables {
		tasks := tbl.get(100)
		s.Equal(0, len(tasks), "failed to delete all expired tasks")
		s.Nil(s.taskQueueTable.get(tl), "failed to delete expired executorTask queue")
	}
}

func (s *ScavengerTestSuite) TestAllAliveTasks() {
	nTasks := 32
	nTaskQueues := 3
	for i := 0; i < nTaskQueues; i++ {
		name := fmt.Sprintf("test-Alive-tq-%v", i)
		s.taskQueueTable.generate(name, true)
		tt := newMockTaskTable()
		tt.generate(nTasks, false)
		s.taskTables[name] = tt
	}
	s.setupTaskMgrMocks()
	s.runScavenger()
	for tl, tbl := range s.taskTables {
		tasks := tbl.get(100)
		s.Equal(nTasks, len(tasks), "scavenger deleted a non-expired executorTask")
		s.NotNil(s.taskQueueTable.get(tl), "scavenger deleted a non-expired executorTask queue")
	}
}

func (s *ScavengerTestSuite) TestExpiredTasksFollowedByAlive() {
	nTasks := 32
	nTaskQueues := 3
	for i := 0; i < nTaskQueues; i++ {
		name := fmt.Sprintf("test-Alive-tq-%v", i)
		s.taskQueueTable.generate(name, true)
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
		s.Equal(int64(nTasks/2), tasks[0].GetTaskId(), "scavenger deleted wrong set of tasks")
		s.NotNil(s.taskQueueTable.get(tl), "scavenger deleted a non-expired executorTask queue")
	}
}

func (s *ScavengerTestSuite) TestAliveTasksFollowedByExpired() {
	nTasks := 32
	nTaskQueues := 3
	for i := 0; i < nTaskQueues; i++ {
		name := fmt.Sprintf("test-Alive-tl-%v", i)
		s.taskQueueTable.generate(name, true)
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
		s.NotNil(s.taskQueueTable.get(tl), "scavenger deleted a non-expired executorTask queue")
	}
}

func (s *ScavengerTestSuite) TestAllExpiredTasksWithErrors() {
	nTasks := 32
	nTaskQueues := 3
	for i := 0; i < nTaskQueues; i++ {
		name := fmt.Sprintf("test-expired-tl-%v", i)
		s.taskQueueTable.generate(name, true)
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
	result, _ := s.taskQueueTable.list(nil, 10)
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
	s.taskMgr.EXPECT().ListTaskQueue(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *p.ListTaskQueueRequest) (*p.ListTaskQueueResponse, error) {
			items, next := s.taskQueueTable.list(req.PageToken, req.PageSize)
			return &p.ListTaskQueueResponse{Items: items, NextPageToken: next}, nil
		}).AnyTimes()
	s.taskMgr.EXPECT().DeleteTaskQueue(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *p.DeleteTaskQueueRequest) error {
			s.taskQueueTable.delete(req.TaskQueue.TaskQueueName)
			return nil
		}).AnyTimes()
	s.taskMgr.EXPECT().GetTasks(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *p.GetTasksRequest) (*p.GetTasksResponse, error) {
			result := s.taskTables[req.TaskQueue].get(req.PageSize)
			return &p.GetTasksResponse{Tasks: result}, nil
		}).AnyTimes()
	s.taskMgr.EXPECT().CompleteTasksLessThan(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *p.CompleteTasksLessThanRequest) (int, error) {
			return s.taskTables[req.TaskQueueName].deleteLessThan(req.ExclusiveMaxTaskID, req.Limit), nil
		}).AnyTimes()
}

func (s *ScavengerTestSuite) setupTaskMgrMocksWithErrors() {
	s.taskMgr.EXPECT().ListTaskQueue(gomock.Any(), gomock.Any()).Return(nil, errTest)
	s.taskMgr.EXPECT().GetTasks(gomock.Any(), gomock.Any()).Return(nil, errTest)
	s.taskMgr.EXPECT().CompleteTasksLessThan(gomock.Any(), gomock.Any()).Return(0, errTest)
	s.taskMgr.EXPECT().DeleteTaskQueue(gomock.Any(), gomock.Any()).Return(errTest)
	s.setupTaskMgrMocks()
}
