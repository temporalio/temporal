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
	"sync"

	"go.temporal.io/server/common/collection"
	ctasks "go.temporal.io/server/common/tasks"
)

// SequentialBatchableTaskQueue is not thread-safe. It is a 2-tiered queue. If incoming task is batchable,
// the batched task will be added to main queue or merged with last item in main queue. In above case, the incoming task
// will also be added into individualTaskQueue, so when batched task failed to execute, we can dispatch individual task and try again.
// If multiple threads polling this queue at same time, we may see an issue where batched task and individual tasks are executing at same time
type (
	SequentialBatchableTaskQueue struct {
		id interface{}

		sync.Mutex
		mainQueue           collection.Queue[TrackableExecutableTask]
		individualTaskQueue []BatchableTask
		lastTask            TrackableExecutableTask
		size                int
	}
)

func NewSequentialCombinableTaskQueue(task TrackableExecutableTask) ctasks.SequentialTaskQueue[TrackableExecutableTask] {
	return &SequentialBatchableTaskQueue{
		id: task.QueueID(),

		mainQueue: collection.NewPriorityQueue[TrackableExecutableTask](
			SequentialTaskQueueCompareLess,
		),
		individualTaskQueue: make([]BatchableTask, 0),
		size:                0,
	}
}

func (q *SequentialBatchableTaskQueue) ID() interface{} {
	return q.id
}

func (q *SequentialBatchableTaskQueue) Peek() TrackableExecutableTask {
	q.Lock()
	defer q.Unlock()
	if q.mainQueue.IsEmpty() {
		return q.individualTaskQueue[0]
	}

	if len(q.individualTaskQueue) == 0 {
		return q.mainQueue.Peek()
	}

	if SequentialTaskQueueCompareLess(q.individualTaskQueue[0], q.mainQueue.Peek()) {
		return q.individualTaskQueue[0]
	} else {
		return q.mainQueue.Peek()
	}
}

// Add will try to combine input task with the last task in the queue. Since most likely incoming task
// are ordered by task ID, so we only try to combine incoming task with last task in the queue.
func (q *SequentialBatchableTaskQueue) Add(task TrackableExecutableTask) {
	q.Lock()
	defer q.Unlock()

	q.size++
	// case 1: input task is not combinable: simply add task into the queue
	t, isCombinable := task.(BatchableTask)

	if !isCombinable {
		q.mainQueue.Add(task)
		q.updateLastTask(task)
		return
	}

	// case 2: lastTask is a batchedTask, try to combine, if success, put the task into individual task queue and return
	lt, lastTaskIsBatchedTask := q.lastTask.(*batchedTask)
	if lastTaskIsBatchedTask {
		err := lt.addTask(t)
		if err == nil {
			q.individualTaskQueue = append(q.individualTaskQueue, t)
			return
		}
	}

	// case 3: failed to combine: If the incoming task will be the last task, create new batchedTask
	if SequentialTaskQueueCompareLess(q.lastTask, task) {
		task = q.createBatchedTask(t)
		q.individualTaskQueue = append(q.individualTaskQueue, t)
	}

	q.mainQueue.Add(task)
	q.updateLastTask(task)
}

func (q *SequentialBatchableTaskQueue) Remove() (task TrackableExecutableTask) {
	q.Lock()
	defer q.Unlock()
	defer func() {
		if _, isBatchedTask := task.(*batchedTask); !isBatchedTask {
			q.size--
		}
	}()

	if q.mainQueue.IsEmpty() {
		return q.removeFromUnderlyingQueue()
	}

	if len(q.individualTaskQueue) == 0 {
		return q.removeFromMainQueue()
	}

	// if both mainQueue and individualTasks queue are not empty, will dispatch the smaller one
	if SequentialTaskQueueCompareLess(q.individualTaskQueue[0], q.mainQueue.Peek()) {
		return q.removeFromUnderlyingQueue()
	} else {
		return q.removeFromMainQueue()
	}
}

func (q *SequentialBatchableTaskQueue) removeFromUnderlyingQueue() TrackableExecutableTask {
	toBeRemoved := q.individualTaskQueue[0]
	q.individualTaskQueue = q.individualTaskQueue[1:]
	return toBeRemoved
}

func (q *SequentialBatchableTaskQueue) removeFromMainQueue() TrackableExecutableTask {
	toBeRemoved := q.mainQueue.Remove().(TrackableExecutableTask)
	if toBeRemoved == q.lastTask {
		q.lastTask = nil
	}
	return toBeRemoved
}

func (q *SequentialBatchableTaskQueue) removeNonPendingTasksFromUnderlyingQueue() {
	var firstPendingTask = -1
	for index, task := range q.individualTaskQueue {
		if (task).State() == ctasks.TaskStatePending {
			firstPendingTask = index
			break
		} else {
			q.size--
		}
	}
	if firstPendingTask >= 0 {
		q.individualTaskQueue = q.individualTaskQueue[firstPendingTask:]
	} else {
		q.individualTaskQueue = []BatchableTask{}
	}
}

func (q *SequentialBatchableTaskQueue) IsEmpty() bool {
	q.Lock()
	defer q.Unlock()

	q.removeNonPendingTasksFromUnderlyingQueue()
	return q.mainQueue.IsEmpty() && len(q.individualTaskQueue) == 0
}

func (q *SequentialBatchableTaskQueue) Len() int {
	q.Lock()
	defer q.Unlock()
	return q.size
}

func (q *SequentialBatchableTaskQueue) updateLastTask(task TrackableExecutableTask) {
	if q.lastTask == nil || SequentialTaskQueueCompareLess(q.lastTask, task) {
		q.lastTask = task
	}
}

func (q *SequentialBatchableTaskQueue) createBatchedTask(task BatchableTask) *batchedTask {
	return &batchedTask{
		batchedTask:     task,
		individualTasks: append([]BatchableTask{}, task),
	}
}
