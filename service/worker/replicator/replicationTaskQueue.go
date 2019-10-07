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
	"github.com/dgryski/go-farm"
	"github.com/uber/cadence/common/collection"
	"github.com/uber/cadence/common/definition"
	"github.com/uber/cadence/common/task"
)

type (
	replicationSequentialTaskQueue struct {
		id        definition.WorkflowIdentifier
		taskQueue collection.Queue
	}
)

func newReplicationSequentialTaskQueue(task task.SequentialTask) task.SequentialTaskQueue {
	var id definition.WorkflowIdentifier
	switch t := task.(type) {
	case *historyMetadataReplicationTask:
		id = t.queueID
	case *historyReplicationTask:
		id = t.queueID
	case *historyReplicationV2Task:
		id = t.queueID
	case *activityReplicationTask:
		id = t.queueID
	default:
		panic("Unknown replication task type")
	}

	return &replicationSequentialTaskQueue{
		id: id,
		taskQueue: collection.NewConcurrentPriorityQueue(
			replicationSequentialTaskQueueCompareLess,
		),
	}
}

func (q *replicationSequentialTaskQueue) QueueID() interface{} {
	return q.id
}

func (q *replicationSequentialTaskQueue) Add(task task.SequentialTask) {
	q.taskQueue.Add(task)
}

func (q *replicationSequentialTaskQueue) Remove() task.SequentialTask {
	return q.taskQueue.Remove().(task.SequentialTask)
}

func (q *replicationSequentialTaskQueue) IsEmpty() bool {
	return q.taskQueue.IsEmpty()
}

func (q *replicationSequentialTaskQueue) Len() int {
	return q.taskQueue.Len()
}

func replicationSequentialTaskQueueHashFn(key interface{}) uint32 {
	queue, ok := key.(*replicationSequentialTaskQueue)
	if !ok {
		return 0
	}
	return farm.Fingerprint32([]byte(queue.id.WorkflowID))
}

func replicationSequentialTaskQueueCompareLess(this interface{}, that interface{}) bool {
	fnGetTaskID := func(object interface{}) int64 {
		switch task := object.(type) {
		case *activityReplicationTask:
			return task.taskID
		case *historyReplicationTask:
			return task.taskID
		case *historyMetadataReplicationTask:
			return task.taskID
		case *historyReplicationV2Task:
			return task.taskID
		default:
			panic("unknown task type")
		}
	}

	return fnGetTaskID(this) < fnGetTaskID(that)
}
