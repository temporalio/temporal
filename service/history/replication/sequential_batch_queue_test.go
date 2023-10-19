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

package replication

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

type (
	sequentialBatchQueueSuite struct {
		suite.Suite
		*require.Assertions
		controller     *gomock.Controller
		logger         log.Logger
		metricsHandler metrics.Handler
	}
)

func (s *sequentialBatchQueueSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
	s.logger = log.NewTestLogger()
}

func TestSequentialBatchQueueSuite(t *testing.T) {
	suite.Run(t, new(sequentialBatchQueueSuite))
}

func (s *sequentialBatchQueueSuite) TestAdd_EmptyQueue() {
	newTask := NewMockTrackableExecutableTask(s.controller)
	queueId := "abc"
	newTask.EXPECT().QueueID().Return(queueId)
	queue := NewSequentialBatchableTaskQueue(newTask, func(_ TrackableExecutableTask) {}, s.logger, s.metricsHandler)
	s.Equal(0, queue.Len())
	s.True(queue.IsEmpty())
}

func (s *sequentialBatchQueueSuite) TestAdd_EmptyQueue_AddNewBatchedTask() {
	newTask := NewMockTrackableExecutableTask(s.controller)
	queueId := "abc"
	newTask.EXPECT().QueueID().Return(queueId)
	queue := NewSequentialBatchableTaskQueue(newTask, func(_ TrackableExecutableTask) {}, s.logger, s.metricsHandler)
	queue.Add(newTask)
	s.Equal(1, queue.Len())
	s.False(queue.IsEmpty())

	taskRemoved := queue.Remove()
	// check queue
	s.True(queue.IsEmpty())
	s.Equal(0, queue.Len())

	// check task
	batchTask, _ := taskRemoved.(*batchedTask)
	s.True(batchTask.batchedTask == newTask)
}

func (s *sequentialBatchQueueSuite) TestAdd_NewTaskBatched() {
	task1 := NewMockBatchableTask(s.controller)
	task2 := NewMockBatchableTask(s.controller)
	task3 := NewMockBatchableTask(s.controller)
	id1 := int64(1)
	id2 := int64(2)
	id3 := int64(3)
	task1.EXPECT().TaskID().Return(id1).AnyTimes()
	task2.EXPECT().TaskID().Return(id2).AnyTimes()
	task3.EXPECT().TaskID().Return(id3).AnyTimes()
	queueId := "abc"
	task1.EXPECT().QueueID().Return(queueId)
	queue := NewSequentialBatchableTaskQueue(task1, func(_ TrackableExecutableTask) {}, s.logger, s.metricsHandler)

	task1.EXPECT().CanBatch().Return(true).Times(1)
	task2.EXPECT().CanBatch().Return(true).Times(1)
	task3.EXPECT().CanBatch().Return(true).Times(1)
	combinedTask := NewMockBatchableTask(s.controller)
	task1.EXPECT().BatchWith(task2).Return(combinedTask, true).Times(1)
	combinedTask.EXPECT().CanBatch().Return(true).Times(1)
	combinedTask.EXPECT().BatchWith(task3).Return(NewMockBatchableTask(s.controller), true).Times(1)
	queue.Add(task1)
	queue.Add(task2)
	queue.Add(task3)
	// check queue
	s.Equal(1, queue.Len())
	s.False(queue.IsEmpty())
	tr := queue.Remove()
	s.Equal(id1, tr.TaskID())
}

func (s *sequentialBatchQueueSuite) TestAdd_NewTaskCannotBatched() {
	task1 := NewMockBatchableTask(s.controller)
	task2 := NewMockBatchableTask(s.controller)
	queueId := "abc"
	task1.EXPECT().QueueID().Return(queueId)
	id1 := int64(1)
	id2 := int64(2)
	task1.EXPECT().TaskID().Return(id1).AnyTimes()
	task2.EXPECT().TaskID().Return(id2).AnyTimes()
	queue := NewSequentialBatchableTaskQueue(task1, func(_ TrackableExecutableTask) {}, s.logger, s.metricsHandler)

	task2.EXPECT().CanBatch().Return(false).Times(1)
	NewMockTrackableExecutableTask(s.controller)
	queue.Add(task1)
	queue.Add(task2)

	// check queue
	s.Equal(2, queue.Len())
	s.False(queue.IsEmpty())
	r1 := queue.Remove()
	r2 := queue.Remove()
	s.Equal(id1, r1.TaskID())
	s.Equal(id2, r2.TaskID())
}

func (s *sequentialBatchQueueSuite) TestAdd_NewTaskIsAddedToQueueWhenBatchFailed() {
	task1 := NewMockBatchableTask(s.controller)
	task2 := NewMockBatchableTask(s.controller)
	task3 := NewMockBatchableTask(s.controller)
	id1 := int64(1)
	id2 := int64(2)
	id3 := int64(3)
	task1.EXPECT().TaskID().Return(id1).AnyTimes()
	task2.EXPECT().TaskID().Return(id2).AnyTimes()
	task3.EXPECT().TaskID().Return(id3).AnyTimes()
	queueId := "abc"
	task1.EXPECT().QueueID().Return(queueId)
	queue := NewSequentialBatchableTaskQueue(task1, func(_ TrackableExecutableTask) {}, s.logger, s.metricsHandler)

	task1.EXPECT().CanBatch().Return(false).Times(1)
	task2.EXPECT().CanBatch().Return(true).Times(2)
	task3.EXPECT().CanBatch().Return(true).Times(1)
	task2.EXPECT().BatchWith(task3).Return(NewMockBatchableTask(s.controller), true).Times(1)
	queue.Add(task1)
	queue.Add(task2)
	queue.Add(task3)
	// check queue
	s.Equal(2, queue.Len())
	s.False(queue.IsEmpty())

	tr1 := queue.Remove()
	s.Equal(id1, tr1.TaskID())

	tr2 := queue.Remove()
	s.Equal(id2, tr2.TaskID())

	tr2BatchTask, _ := tr2.(*batchedTask)
	s.Len(tr2BatchTask.individualTasks, 2)
}

func (s *sequentialBatchQueueSuite) TestAdd_NewTaskTryToBatchWithLastTask() {
	task1 := NewMockBatchableTask(s.controller)
	task2 := NewMockBatchableTask(s.controller)
	task3 := NewMockBatchableTask(s.controller)
	id1 := int64(1)
	id2 := int64(2)
	id3 := int64(3)
	task1.EXPECT().TaskID().Return(id1).AnyTimes()
	task2.EXPECT().TaskID().Return(id2).AnyTimes()
	task3.EXPECT().TaskID().Return(id3).AnyTimes()
	queueId := "abc"
	task1.EXPECT().QueueID().Return(queueId)
	queue := NewSequentialBatchableTaskQueue(task1, func(_ TrackableExecutableTask) {}, s.logger, s.metricsHandler)

	task1.EXPECT().CanBatch().Return(true).Times(1)
	task2.EXPECT().CanBatch().Return(true).Times(1)
	task3.EXPECT().CanBatch().Return(false).Times(2)

	queue.Add(task3)
	queue.Add(task2)
	queue.Add(task1)
	// check queue
	s.Equal(3, queue.Len())

	s.Equal(id1, queue.Remove().TaskID())
	s.Equal(id2, queue.Remove().TaskID())
	s.Equal(id3, queue.Remove().TaskID())
}
