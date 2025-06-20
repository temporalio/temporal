package replication

import (
	"sync"

	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	ctasks "go.temporal.io/server/common/tasks"
)

type (
	SequentialBatchableTaskQueue struct {
		id interface{}

		sync.Mutex
		taskQueue                    collection.Queue[*batchedTask]
		lastTask                     *batchedTask
		batchedIndividualTaskHandler func(task TrackableExecutableTask)

		logger         log.Logger
		metricsHandler metrics.Handler
	}
)

func NewSequentialBatchableTaskQueue(
	task TrackableExecutableTask,
	batchedIndividualTaskHandler func(task TrackableExecutableTask),
	logger log.Logger,
	metricsHandler metrics.Handler,
) ctasks.SequentialTaskQueue[TrackableExecutableTask] {
	return &SequentialBatchableTaskQueue{
		id: task.QueueID(),

		taskQueue: collection.NewPriorityQueue[*batchedTask](
			sequentialBatchableTaskQueueCompareLess,
		),
		batchedIndividualTaskHandler: batchedIndividualTaskHandler,
		logger:                       logger,
		metricsHandler:               metricsHandler,
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

	if q.lastTask != nil && q.lastTask.AddTask(task) {
		return
	}

	incomingTask := q.createBatchedTask(task)
	q.taskQueue.Add(incomingTask)
	q.updateLastTask(incomingTask)
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

func (q *SequentialBatchableTaskQueue) updateLastTask(task *batchedTask) {
	if q.lastTask == nil || sequentialBatchableTaskQueueCompareLess(q.lastTask, task) {
		q.lastTask = task
	}
}

func (q *SequentialBatchableTaskQueue) createBatchedTask(task TrackableExecutableTask) *batchedTask {
	return &batchedTask{
		batchedTask:     task,
		individualTasks: []TrackableExecutableTask{task},
		state:           batchStateOpen,

		// This is to add individual task back to this queue, so it can be processed again. This is based on an assumption: only one thread is
		// interacting with the queue. And this is a shortcut because a proper way is to resubmit the individual tasks back to scheduler.
		// But that requires a refactor on scheduler and task lifecycle and could be risky to included in this feature implementation.
		//
		individualTaskHandler: func(task TrackableExecutableTask) {
			q.Add(task)
		},
		logger:         q.logger,
		metricsHandler: q.metricsHandler,
	}
}

func sequentialBatchableTaskQueueCompareLess(this *batchedTask, that *batchedTask) bool {
	return SequentialTaskQueueCompareLess(this, that)
}
