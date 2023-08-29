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

type (
	SequentialBatchableTaskQueue struct {
		id interface{}

		sync.Mutex
		taskQueue         collection.Queue[TrackableExecutableTask]
		lastTask          TrackableExecutableTask
		reSubmitScheduler ctasks.Scheduler[TrackableExecutableTask]
	}
)

func NewSequentialBatchableTaskQueue(task TrackableExecutableTask, reSubmitScheduler ctasks.Scheduler[TrackableExecutableTask]) ctasks.SequentialTaskQueue[TrackableExecutableTask] {
	return &SequentialBatchableTaskQueue{
		id: task.QueueID(),

		taskQueue: collection.NewPriorityQueue[TrackableExecutableTask](
			SequentialTaskQueueCompareLess,
		),
		reSubmitScheduler: reSubmitScheduler,
	}
}

func (q *SequentialBatchableTaskQueue) ID() interface{} {
	return q.id
}

func (q *SequentialBatchableTaskQueue) Peek() TrackableExecutableTask {
	q.Lock()
	defer q.Unlock()
	return q.taskQueue.Peek()
}

// Add will try to batch input task with the last task in the queue. Since most likely incoming task
// are ordered by task ID, we only try to batch incoming task with last task in the queue.
func (q *SequentialBatchableTaskQueue) Add(task TrackableExecutableTask) {
	q.Lock()
	defer q.Unlock()
	q.updateLastTask(task)

	batchableTask, isBatchable := task.(BatchableTask)

	// case 1: input task is not a batchable task or input task does not want to be batched: simply add task into the queue
	if !isBatchable || !batchableTask.CanBatch() {
		q.updateLastTask(task)
		q.taskQueue.Add(task)
		return
	}

	// case 2: lastTask is a batchedTask, try to addTask
	batchedTask, lastTaskIsBatchedTask := q.lastTask.(*batchedTask)
	if lastTaskIsBatchedTask && batchedTask.addTask(batchableTask) {
		return
	}

	// case 3: If the incoming task will be the last task, create new batchedTask
	if SequentialTaskQueueCompareLess(q.lastTask, task) {
		task = q.createBatchedTask(batchableTask)
	}

	q.taskQueue.Add(task)
	q.updateLastTask(task)
}

func (q *SequentialBatchableTaskQueue) Remove() (task TrackableExecutableTask) {
	q.Lock()
	defer q.Unlock()
	taskToRemove := q.taskQueue.Remove()
	if taskToRemove == q.lastTask {
		q.lastTask = nil
	}
	return taskToRemove
}

func (q *SequentialBatchableTaskQueue) IsEmpty() bool {
	q.Lock()
	defer q.Unlock()

	return q.taskQueue.IsEmpty()
}

func (q *SequentialBatchableTaskQueue) Len() int {
	q.Lock()
	defer q.Unlock()
	return q.taskQueue.Len()
}

func (q *SequentialBatchableTaskQueue) updateLastTask(task TrackableExecutableTask) {
	if q.lastTask == nil || SequentialTaskQueueCompareLess(q.lastTask, task) {
		q.lastTask = task
	}
}

func (q *SequentialBatchableTaskQueue) createBatchedTask(task BatchableTask) *batchedTask {
	return &batchedTask{
		batchedTask:       task,
		individualTasks:   append([]BatchableTask{}, task),
		state:             batchStateOpen,
		reSubmitScheduler: q.reSubmitScheduler,
	}
}
