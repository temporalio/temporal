package replicator

import (
	"github.com/dgryski/go-farm"

	"github.com/temporalio/temporal/common/collection"
	"github.com/temporalio/temporal/common/definition"
	"github.com/temporalio/temporal/common/task"
)

type (
	replicationSequentialTaskQueue struct {
		id        definition.WorkflowIdentifier
		taskQueue collection.Queue
	}
)

func newReplicationSequentialTaskQueue(task task.Task) task.SequentialTaskQueue {
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

func (q *replicationSequentialTaskQueue) Add(task task.Task) {
	q.taskQueue.Add(task)
}

func (q *replicationSequentialTaskQueue) Remove() task.Task {
	return q.taskQueue.Remove().(task.Task)
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
