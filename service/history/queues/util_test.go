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

package queues

import (
	"math/rand"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
)

type queueUtilsTestSuite struct {
	suite.Suite
	ctrl  *gomock.Controller
	shard *shard.MockContext
	task  tasks.Task
}

func TestQueueUtilsTestSuite(t *testing.T) {
	suite.Run(t, new(queueUtilsTestSuite))
}

func (s *queueUtilsTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.shard = shard.NewMockContext(s.ctrl)
	category := tasks.NewCategory(rand.Int31(), tasks.CategoryTypeUnspecified, "unspecified")
	workflowKey := definition.NewWorkflowKey(tests.NamespaceID.String(), tests.WorkflowID, tests.RunID)
	s.task = tasks.NewFakeTask(workflowKey, category, tasks.DefaultFireTime)
}

func (s *queueUtilsTestSuite) TearDownTest() {
	s.ctrl.Finish()
}

func (s *queueUtilsTestSuite) TestZeroTaskID() {
	s.task.SetTaskID(0)
	s.True(IsTaskDone(s.shard, s.task))
}

func (s *queueUtilsTestSuite) TestGlobalQueueAckLevel() {
	category := s.task.GetCategory()
	s.shard.EXPECT().GetQueueState(category).Return(nil, false).AnyTimes()
	ackLevel := int64(5)
	s.shard.EXPECT().GetQueueAckLevel(category).Return(tasks.NewImmediateKey(ackLevel)).AnyTimes()
	s.Run("TaskID < AckLevel", func() {
		s.task.SetTaskID(ackLevel - 1)
		s.True(IsTaskDone(s.shard, s.task))
	})
	s.Run("TaskID = AckLevel", func() {
		s.task.SetTaskID(ackLevel)
		s.True(IsTaskDone(s.shard, s.task))
	})
	s.Run("TaskID > AckLevel", func() {
		s.task.SetTaskID(ackLevel + 1)
		s.False(IsTaskDone(s.shard, s.task))
	})
}

func (s *queueUtilsTestSuite) TestTaskIDGeqHighWatermark() {
	highWatermark := tasks.Key{
		TaskID:   5,
		FireTime: tasks.DefaultFireTime,
	}
	qState := ToPersistenceQueueState(&queueState{
		exclusiveReaderHighWatermark: highWatermark,
	})
	s.shard.EXPECT().GetQueueState(s.task.GetCategory()).Return(qState, true).AnyTimes()
	s.Run("TaskID = HighWatermark", func() {
		s.task.SetTaskID(highWatermark.TaskID)
		s.False(IsTaskDone(s.shard, s.task))
	})
	s.Run("TaskID > HighWatermark", func() {
		s.task.SetTaskID(highWatermark.TaskID + 1)
		s.False(IsTaskDone(s.shard, s.task))
	})
}

func (s *queueUtilsTestSuite) TestNoReaderScopes() {
	qState := ToPersistenceQueueState(&queueState{
		readerScopes: map[int32][]Scope{
			DefaultReaderId: {},
		},
		exclusiveReaderHighWatermark: tasks.Key{
			TaskID:   5,
			FireTime: tasks.DefaultFireTime,
		},
	})
	s.shard.EXPECT().GetQueueState(s.task.GetCategory()).Return(qState, true).AnyTimes()
	s.task.SetTaskID(qState.ExclusiveReaderHighWatermark.TaskId - 1)
	acked := IsTaskDone(s.shard, s.task)
	s.True(acked)
}

func (s *queueUtilsTestSuite) TestReaderScopes() {
	scopes := NewRandomScopes(1)
	firstInclusiveMin := &scopes[0].Range.InclusiveMin
	fireTime := tasks.DefaultFireTime
	firstInclusiveMin.FireTime = fireTime
	highWatermark := tasks.Key{
		TaskID:   firstInclusiveMin.TaskID + 100,
		FireTime: fireTime,
	}
	qState := ToPersistenceQueueState(&queueState{
		readerScopes: map[int32][]Scope{
			DefaultReaderId: scopes,
		},
		exclusiveReaderHighWatermark: highWatermark,
	})
	s.shard.EXPECT().GetQueueState(s.task.GetCategory()).Return(qState, true).AnyTimes()
	s.Run("TaskID < InclusiveMin", func() {
		s.task.SetTaskID(firstInclusiveMin.TaskID - 1)
		s.True(IsTaskDone(s.shard, s.task))
	})
	s.Run("TaskID = InclusiveMin", func() {
		s.task.SetTaskID(firstInclusiveMin.TaskID)
		s.False(IsTaskDone(s.shard, s.task))
	})
	s.Run("TaskID > InclusiveMin", func() {
		s.task.SetTaskID(firstInclusiveMin.TaskID + 1)
		s.False(IsTaskDone(s.shard, s.task))
	})
}
