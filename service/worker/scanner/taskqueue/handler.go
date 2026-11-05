package taskqueue

import (
	"strings"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/log/tag"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/matching"
	"go.temporal.io/server/service/worker/scanner/executor"
)

type handlerStatus = executor.TaskStatus

const (
	handlerStatusDone  = executor.TaskStatusDone
	handlerStatusErr   = executor.TaskStatusErr
	handlerStatusDefer = executor.TaskStatusDefer
)

const scannerTaskQueuePrefix = "temporal-sys-tl-scanner"

// deleteHandler handles deletions for a given task queue
// this handler limits the amount of tasks deleted to maxTasksPerJob
// for fairness among all the task queue in the system - when there
// is more work to do subsequently, this handler will return StatusDefer
// with the assumption that the executor will schedule this task later
//
// Each loop of the handler proceeds as follows, for every subqueue of the
// task queue (tasks are partitioned into subqueues to implement priority and
// fairness, so expired tasks must be scavenged from all of them and the queue
// itself may only be deleted once all of its subqueues are empty)
//   - Retrieve the next batch of tasks sorted by task_id for this subqueue from persistence
//   - If there are 0 tasks for this subqueue, move on to the next subqueue
//   - If any of the tasks in the batch isn't expired, we are done with this subqueue.
//     Since tasks are retrieved in sorted order, if one of the tasks isn't expired,
//     chances are, none of the tasks above it are expired as well - so, we give up
//     and wait for the next run. A live task in any subqueue also keeps the task
//     queue itself alive
//   - Delete the entire batch of tasks
//   - If the number of tasks retrieved is less than batchSize, there are no more tasks
//     in the subqueue. Move on to the next subqueue, and try deleting the task queue
//     if its idle once all subqueues have been processed
func (s *Scavenger) deleteHandler(key *p.TaskQueueKey, state *taskQueueState) handlerStatus {
	var err error
	var nProcessed, nDeleted int

	defer func() { s.deleteHandlerLog(key, state, nProcessed, nDeleted, err) }()

	allSubqueuesEmpty := true
	for subqueue := 0; subqueue < state.subqueues; subqueue++ {
		for nDeleted < maxTasksPerJob {
			resp, err1 := s.getTasks(s.lifecycleCtx, key, subqueue, taskBatchSize)
			if err1 != nil {
				err = err1
				return handlerStatusErr
			}

			nTasks := len(resp.Tasks)
			if nTasks == 0 {
				// this subqueue is empty, continue with the next one
				break
			}

			subqueueHasLiveTask := false
			for _, task := range resp.Tasks {
				nProcessed++
				if !matching.IsTaskExpired(task) {
					subqueueHasLiveTask = true
					break
				}
			}
			if subqueueHasLiveTask {
				// a live task in any subqueue keeps the task queue alive, but
				// the remaining subqueues may still hold expired tasks
				allSubqueuesEmpty = false
				break
			}

			lastTaskID := resp.Tasks[nTasks-1].GetTaskId()
			if _, err = s.completeTasks(s.lifecycleCtx, key, subqueue, lastTaskID+1, nTasks); err != nil {
				return handlerStatusErr
			}

			nDeleted += nTasks
			if nTasks < taskBatchSize {
				// drained this subqueue, continue with the next one
				break
			}
		}
		if nDeleted >= maxTasksPerJob {
			// The budget is charged for deletions, not inspections: deferring
			// after inspecting tasks without deleting any would restart the scan
			// at subqueue 0 on the next run and never reach the later subqueues.
			return handlerStatusDefer
		}
	}

	if allSubqueuesEmpty {
		s.tryDeleteTaskQueue(key, state)
	}
	return handlerStatusDone
}

func (s *Scavenger) tryDeleteTaskQueue(key *p.TaskQueueKey, state *taskQueueState) {
	if strings.HasPrefix(key.TaskQueueName, scannerTaskQueuePrefix) {
		return // avoid deleting our own task queue
	}

	delta := time.Now().UTC().Sub(state.lastUpdated)
	if delta < taskQueueGracePeriod {
		return
	}
	// usually, matching engine is the authoritative owner of a taskqueue
	// and its incorrect for any other entity to mutate executorTask queues (including deleting it)
	// the delete here is safe because of two reasons:
	//   - we delete the executorTask queue only if the lastUpdated is > 48H. If a executorTask queue is idle for
	//     this amount of time, it will no longer be owned by any host in matching engine (because
	//     of idle timeout). If any new host has to take ownership of this at this time, it can only
	//     do so by updating the rangeID
	//   - deleteTaskQueue is a conditional delete where condition is the rangeID
	if err := s.deleteTaskQueue(s.lifecycleCtx, key, state.rangeID); err != nil {
		s.logger.Error("deleteTaskQueue error", tag.Error(err))
		return
	}
	atomic.AddInt64(&s.stats.taskqueue.nDeleted, 1)
	s.logger.Info("taskqueue deleted", tag.WorkflowNamespaceID(key.NamespaceID), tag.WorkflowTaskQueueName(key.TaskQueueName), tag.WorkflowTaskQueueType(key.TaskQueueType))
}

func (s *Scavenger) deleteHandlerLog(key *p.TaskQueueKey, state *taskQueueState, nProcessed int, nDeleted int, err error) {
	atomic.AddInt64(&s.stats.task.nDeleted, int64(nDeleted))
	atomic.AddInt64(&s.stats.task.nProcessed, int64(nProcessed))
	if err != nil {
		s.logger.Error("scavenger.deleteHandler processed.",
			tag.Error(err), tag.WorkflowNamespaceID(key.NamespaceID), tag.WorkflowTaskQueueName(key.TaskQueueName), tag.WorkflowTaskQueueType(key.TaskQueueType), tag.NumberProcessed(nProcessed), tag.NumberDeleted(nDeleted))
		return
	}
	if nProcessed > 0 {
		s.logger.Info("scavenger.deleteHandler processed.",
			tag.WorkflowNamespaceID(key.NamespaceID), tag.WorkflowTaskQueueName(key.TaskQueueName), tag.WorkflowTaskQueueType(key.TaskQueueType), tag.NumberProcessed(nProcessed), tag.NumberDeleted(nDeleted))
	}
}
