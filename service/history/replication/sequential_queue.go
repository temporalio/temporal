package replication

import (
	"sync"

	"github.com/dgryski/go-farm"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	ctasks "go.temporal.io/server/common/tasks"
)

type (
	SequentialTaskQueue struct {
		id interface{}

		sync.Mutex
		taskQueue collection.Queue[TrackableExecutableTask]
	}
)

func NewSequentialTaskQueue(task TrackableExecutableTask) ctasks.SequentialTaskQueue[TrackableExecutableTask] {
	return &SequentialTaskQueue{
		id: task.QueueID(),

		taskQueue: collection.NewPriorityQueue[TrackableExecutableTask](
			SequentialTaskQueueCompareLess,
		),
	}
}

func NewSequentialTaskQueueWithID(id interface{}) ctasks.SequentialTaskQueue[TrackableExecutableTask] {
	return &SequentialTaskQueue{
		id: id,

		taskQueue: collection.NewPriorityQueue[TrackableExecutableTask](
			SequentialTaskQueueCompareLess,
		),
	}
}

func (q *SequentialTaskQueue) ID() interface{} {
	return q.id
}

func (q *SequentialTaskQueue) Peek() TrackableExecutableTask {
	q.Lock()
	defer q.Unlock()
	return q.taskQueue.Peek()
}

func (q *SequentialTaskQueue) Add(task TrackableExecutableTask) {
	q.Lock()
	defer q.Unlock()
	q.taskQueue.Add(task)
}

func (q *SequentialTaskQueue) Remove() TrackableExecutableTask {
	q.Lock()
	defer q.Unlock()
	return q.taskQueue.Remove().(TrackableExecutableTask)
}

func (q *SequentialTaskQueue) IsEmpty() bool {
	q.Lock()
	defer q.Unlock()
	return q.taskQueue.IsEmpty()
}

func (q *SequentialTaskQueue) Len() int {
	q.Lock()
	defer q.Unlock()
	return q.taskQueue.Len()
}

func SequentialTaskQueueCompareLess(this TrackableExecutableTask, that TrackableExecutableTask) bool {
	return this.TaskID() < that.TaskID()
}

func WorkflowKeyHashFn(
	item interface{},
) uint32 {
	workflowKey, ok := item.(definition.WorkflowKey)
	if !ok {
		return 0
	}
	idBytes := []byte(workflowKey.NamespaceID + "_" + workflowKey.WorkflowID + "_" + workflowKey.RunID)
	return farm.Fingerprint32(idBytes)
}
