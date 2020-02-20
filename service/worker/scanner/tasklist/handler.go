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

package tasklist

import (
	"strings"
	"sync/atomic"
	"time"

	"github.com/uber/cadence/common/log/tag"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/worker/scanner/executor"
)

type handlerStatus = executor.TaskStatus

const (
	handlerStatusDone  = executor.TaskStatusDone
	handlerStatusErr   = executor.TaskStatusErr
	handlerStatusDefer = executor.TaskStatusDefer
)

const scannerTaskListPrefix = "cadence-sys-tl-scanner"

// deleteHandler handles deletions for a given task list
// this handler limits the amount of tasks deleted to maxTasksPerJob
// for fairness among all the task-list in the system - when there
// is more work to do subsequently, this handler will return StatusDefer
// with the assumption that the executor will schedule this task later
//
// Each loop of the handler proceeds as follows
//    - Retrieve the next batch of tasks sorted by task_id for this task-list from persistence
//    - If there are 0 tasks for this task-list, try deleting the task-list if its idle
//    - If any of the tasks in the batch isn't expired, we are done. Since tasks are retrieved
//      in sorted order, if one of the tasks isn't expired, chances are, none of the tasks above
//      it are expired as well - so, we give up and wait for the next run
//    - Delete the entire batch of tasks
//    - If the number of tasks retrieved is less than batchSize, there are no more tasks in the task-list
//      Try deleting the task-list if its idle
func (s *Scavenger) deleteHandler(key *taskListKey, state *taskListState) handlerStatus {
	var err error
	var nProcessed, nDeleted int

	defer func() { s.deleteHandlerLog(key, state, nProcessed, nDeleted, err) }()

	for nProcessed < maxTasksPerJob {
		resp, err1 := s.getTasks(key, taskBatchSize)
		if err1 != nil {
			err = err1
			return handlerStatusErr
		}

		nTasks := len(resp.Tasks)
		if nTasks == 0 {
			s.tryDeleteTaskList(key, state)
			return handlerStatusDone
		}

		for _, task := range resp.Tasks {
			nProcessed++
			if !s.isTaskExpired(task) {
				return handlerStatusDone
			}
		}

		taskID := resp.Tasks[nTasks-1].TaskID
		if _, err = s.completeTasks(key, taskID, nTasks); err != nil {
			return handlerStatusErr
		}

		nDeleted += nTasks
		if nTasks < taskBatchSize {
			s.tryDeleteTaskList(key, state)
			return handlerStatusDone
		}
	}

	return handlerStatusDefer
}

func (s *Scavenger) tryDeleteTaskList(key *taskListKey, state *taskListState) {
	if strings.HasPrefix(key.Name, scannerTaskListPrefix) {
		return // avoid deleting our own task list
	}
	delta := time.Now().Sub(state.lastUpdated)
	if delta < taskListGracePeriod {
		return
	}
	// usually, matching engine is the authoritative owner of a tasklist
	// and its incorrect for any other entity to mutate executorTask lists (including deleting it)
	// the delete here is safe because of two reasons:
	//   - we delete the executorTask list only if the lastUpdated is > 48H. If a executorTask list is idle for
	//     this amount of time, it will no longer be owned by any host in matching engine (because
	//     of idle timeout). If any new host has to take ownership of this at this time, it can only
	//     do so by updating the rangeID
	//   - deleteTaskList is a conditional delete where condition is the rangeID
	if err := s.deleteTaskList(key, state.rangeID); err != nil {
		s.logger.Error("deleteTaskList error", tag.Error(err))
		return
	}
	atomic.AddInt64(&s.stats.tasklist.nDeleted, 1)
	s.logger.Info("tasklist deleted", tag.WorkflowDomainID(key.DomainID), tag.WorkflowTaskListName(key.Name), tag.TaskType(key.TaskType))
}

func (s *Scavenger) deleteHandlerLog(key *taskListKey, state *taskListState, nProcessed int, nDeleted int, err error) {
	atomic.AddInt64(&s.stats.task.nDeleted, int64(nDeleted))
	atomic.AddInt64(&s.stats.task.nProcessed, int64(nProcessed))
	if err != nil {
		s.logger.Error("scavenger.deleteHandler processed.",
			tag.Error(err), tag.WorkflowDomainID(key.DomainID), tag.WorkflowTaskListName(key.Name), tag.TaskType(key.TaskType), tag.NumberProcessed(nProcessed), tag.NumberDeleted(nDeleted))
		return
	}
	if nProcessed > 0 {
		s.logger.Info("scavenger.deleteHandler processed.",
			tag.WorkflowDomainID(key.DomainID), tag.WorkflowTaskListName(key.Name), tag.TaskType(key.TaskType), tag.NumberProcessed(nProcessed), tag.NumberDeleted(nDeleted))
	}
}

func (s *Scavenger) isTaskExpired(t *p.TaskInfo) bool {
	return t.Expiry.After(epochStartTime) && time.Now().After(t.Expiry)
}
