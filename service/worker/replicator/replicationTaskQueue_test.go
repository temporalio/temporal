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

package replicator

import (
	"testing"

	"github.com/uber/cadence/common/collection"

	"github.com/dgryski/go-farm"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/common/definition"
)

type (
	replicationSequentialTaskQueueSuite struct {
		suite.Suite

		queueID definition.WorkflowIdentifier
		queue   *replicationSequentialTaskQueue
	}
)

func TestReplicationSequentialTaskQueueSuite(t *testing.T) {
	s := new(replicationSequentialTaskQueueSuite)
	suite.Run(t, s)
}

func (s *replicationSequentialTaskQueueSuite) SetupSuite() {

}

func (s *replicationSequentialTaskQueueSuite) TearDownSuite() {

}

func (s *replicationSequentialTaskQueueSuite) SetupTest() {
	s.queueID = definition.NewWorkflowIdentifier(
		"some random domain ID",
		"some random workflow ID",
		"some random run ID",
	)
	s.queue = &replicationSequentialTaskQueue{
		id: s.queueID,
		taskQueue: collection.NewConcurrentPriorityQueue(
			replicationSequentialTaskQueueCompareLess,
		),
	}
}

func (s *replicationSequentialTaskQueueSuite) TearDownTest() {

}

func (s *replicationSequentialTaskQueueSuite) TestNewTaskQueue() {
	activityTask := s.generateActivityTask(0)
	activityTaskQueue := newReplicationSequentialTaskQueue(s.generateActivityTask(0))
	s.Equal(0, activityTaskQueue.Len())
	s.Equal(activityTask.queueID, activityTaskQueue.QueueID())

	historyTask := s.generateHistoryTask(0)
	historyTaskQueue := newReplicationSequentialTaskQueue(s.generateActivityTask(0))
	s.Equal(0, historyTaskQueue.Len())
	s.Equal(historyTask.queueID, historyTaskQueue.QueueID())

	historyMetadataTask := s.generateHistoryMetadataTask(0)
	historyMetadataTaskQueue := newReplicationSequentialTaskQueue(s.generateActivityTask(0))
	s.Equal(0, historyMetadataTaskQueue.Len())
	s.Equal(historyMetadataTask.queueID, historyMetadataTaskQueue.QueueID())
}

func (s *replicationSequentialTaskQueueSuite) TestQueueID() {
	s.Equal(s.queueID, s.queue.QueueID())
}

func (s *replicationSequentialTaskQueueSuite) TestAddRemoveIsEmptyLen() {
	taskID := int64(0)

	s.Equal(0, s.queue.Len())
	s.True(s.queue.IsEmpty())

	testTask1 := s.generateActivityTask(taskID)
	taskID++

	s.queue.Add(testTask1)
	s.Equal(1, s.queue.Len())
	s.False(s.queue.IsEmpty())

	testTask2 := s.generateHistoryTask(taskID)
	taskID++

	s.queue.Add(testTask2)
	s.Equal(2, s.queue.Len())
	s.False(s.queue.IsEmpty())

	testTask := s.queue.Remove()
	s.Equal(1, s.queue.Len())
	s.False(s.queue.IsEmpty())
	s.Equal(testTask1, testTask)

	testTask3 := s.generateHistoryTask(taskID)
	taskID++

	s.queue.Add(testTask3)
	s.Equal(2, s.queue.Len())
	s.False(s.queue.IsEmpty())

	testTask = s.queue.Remove()
	s.Equal(1, s.queue.Len())
	s.False(s.queue.IsEmpty())
	s.Equal(testTask2, testTask)

	testTask = s.queue.Remove()
	s.Equal(0, s.queue.Len())
	s.True(s.queue.IsEmpty())
	s.Equal(testTask3, testTask)

	testTask4 := s.generateActivityTask(taskID)

	s.queue.Add(testTask4)
	s.Equal(1, s.queue.Len())
	s.False(s.queue.IsEmpty())

	testTask = s.queue.Remove()
	s.Equal(0, s.queue.Len())
	s.True(s.queue.IsEmpty())
	s.Equal(testTask4, testTask)
}

func (s *replicationSequentialTaskQueueSuite) TestHashFn() {
	s.Equal(
		farm.Fingerprint32([]byte(s.queueID.WorkflowID)),
		replicationSequentialTaskQueueHashFn(s.queue),
	)
}

func (s *replicationSequentialTaskQueueSuite) TestCompareLess() {
	s.True(replicationSequentialTaskQueueCompareLess(
		s.generateActivityTask(1),
		s.generateActivityTask(2),
	))

	s.True(replicationSequentialTaskQueueCompareLess(
		s.generateActivityTask(1),
		s.generateHistoryMetadataTask(2),
	))

	s.True(replicationSequentialTaskQueueCompareLess(
		s.generateActivityTask(1),
		s.generateHistoryTask(2),
	))

	s.True(replicationSequentialTaskQueueCompareLess(
		s.generateHistoryMetadataTask(1),
		s.generateActivityTask(2),
	))

	s.True(replicationSequentialTaskQueueCompareLess(
		s.generateHistoryMetadataTask(1),
		s.generateHistoryMetadataTask(2),
	))

	s.True(replicationSequentialTaskQueueCompareLess(
		s.generateHistoryMetadataTask(1),
		s.generateHistoryTask(2),
	))

	s.True(replicationSequentialTaskQueueCompareLess(
		s.generateHistoryTask(1),
		s.generateActivityTask(2),
	))

	s.True(replicationSequentialTaskQueueCompareLess(
		s.generateHistoryTask(1),
		s.generateHistoryMetadataTask(2),
	))

	s.True(replicationSequentialTaskQueueCompareLess(
		s.generateHistoryTask(1),
		s.generateHistoryTask(2),
	))

	s.False(replicationSequentialTaskQueueCompareLess(
		s.generateActivityTask(10),
		s.generateActivityTask(2),
	))

	s.False(replicationSequentialTaskQueueCompareLess(
		s.generateActivityTask(10),
		s.generateHistoryMetadataTask(2),
	))

	s.False(replicationSequentialTaskQueueCompareLess(
		s.generateActivityTask(10),
		s.generateHistoryTask(2),
	))

	s.False(replicationSequentialTaskQueueCompareLess(
		s.generateHistoryMetadataTask(10),
		s.generateActivityTask(2),
	))

	s.False(replicationSequentialTaskQueueCompareLess(
		s.generateHistoryMetadataTask(10),
		s.generateHistoryMetadataTask(2),
	))

	s.False(replicationSequentialTaskQueueCompareLess(
		s.generateHistoryMetadataTask(10),
		s.generateHistoryTask(2),
	))

	s.False(replicationSequentialTaskQueueCompareLess(
		s.generateHistoryTask(10),
		s.generateActivityTask(2),
	))

	s.False(replicationSequentialTaskQueueCompareLess(
		s.generateHistoryTask(10),
		s.generateHistoryMetadataTask(2),
	))

	s.False(replicationSequentialTaskQueueCompareLess(
		s.generateHistoryTask(10),
		s.generateHistoryTask(2),
	))
}

func (s *replicationSequentialTaskQueueSuite) generateActivityTask(taskID int64) *activityReplicationTask {
	return &activityReplicationTask{
		workflowReplicationTask: workflowReplicationTask{
			queueID: s.queueID,
			taskID:  taskID,
		},
	}
}

func (s *replicationSequentialTaskQueueSuite) generateHistoryTask(taskID int64) *historyReplicationTask {
	return &historyReplicationTask{
		workflowReplicationTask: workflowReplicationTask{
			queueID: s.queueID,
			taskID:  taskID,
		},
	}
}

func (s *replicationSequentialTaskQueueSuite) generateHistoryMetadataTask(taskID int64) *historyMetadataReplicationTask {
	return &historyMetadataReplicationTask{
		workflowReplicationTask: workflowReplicationTask{
			queueID: s.queueID,
			taskID:  taskID,
		},
	}
}
