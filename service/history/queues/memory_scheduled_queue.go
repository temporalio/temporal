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

package queues

import (
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	ctasks "go.temporal.io/server/common/tasks"
)

const (
	workerBusyRescheduleDelay = 1 * time.Second
)

type (
	memoryScheduledQueue struct {
		scheduler ctasks.Scheduler[ctasks.Task]

		taskQueue     collection.Queue[Executable]
		nextTaskTimer *time.Timer
		newTaskCh     chan Executable

		timeSource     clock.TimeSource
		logger         log.Logger
		metricsHandler metrics.Handler

		status     int32
		shutdownCh chan struct{}
		shutdownWG sync.WaitGroup
	}
)

func newMemoryScheduledQueue(
	scheduler ctasks.Scheduler[ctasks.Task],
	timeSource clock.TimeSource,
	logger log.Logger,
	metricsHandler metrics.Handler,
) *memoryScheduledQueue {

	nextTaskTimer := time.NewTimer(0)
	if !nextTaskTimer.Stop() {
		<-nextTaskTimer.C
	}

	return &memoryScheduledQueue{
		taskQueue:     collection.NewPriorityQueue[Executable](executableVisibilityTimeCompareLess),
		nextTaskTimer: nextTaskTimer,
		newTaskCh:     make(chan Executable),

		timeSource:     timeSource,
		logger:         logger,
		metricsHandler: metricsHandler,

		status:     common.DaemonStatusInitialized,
		shutdownCh: make(chan struct{}),

		scheduler: scheduler,
	}
}

func executableVisibilityTimeCompareLess(
	this Executable,
	that Executable,
) bool {
	return this.GetVisibilityTime().Before(that.GetVisibilityTime())
}

func (q *memoryScheduledQueue) Start() {
	if !atomic.CompareAndSwapInt32(&q.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	q.logger.Info("", tag.LifeCycleStarting)
	defer q.logger.Info("", tag.LifeCycleStarted)

	q.shutdownWG.Add(1)
	go q.processQueueLoop()
}

func (q *memoryScheduledQueue) Stop() {
	if !atomic.CompareAndSwapInt32(&q.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	q.logger.Info("", tag.LifeCycleStopping)
	defer q.logger.Info("", tag.LifeCycleStopped)

	close(q.shutdownCh)

	if success := common.AwaitWaitGroup(&q.shutdownWG, time.Minute); !success {
		q.logger.Warn("", tag.LifeCycleStopTimedout)
	}
}

func (q *memoryScheduledQueue) Add(task Executable) {
	q.newTaskCh <- task
}

//nolint:revive // cognitive complexity
func (q *memoryScheduledQueue) processQueueLoop() {
	defer q.shutdownWG.Done()

	for {
		select {
		case <-q.shutdownCh:
			return
		default:
		}

		select {
		case <-q.shutdownCh:
			return
		case newTask := <-q.newTaskCh:
			var nextTaskTime time.Time
			if !q.taskQueue.IsEmpty() {
				nextTaskTime = q.taskQueue.Peek().GetVisibilityTime()
			}
			q.taskQueue.Add(newTask)
			// If there is no timer set OR new time is earlier than the current one, then timer needs to be set to the new time.
			if nextTaskTime.IsZero() || newTask.GetVisibilityTime().Before(nextTaskTime) {
				// But before reset if timer is there it needs to be stopped.
				if !nextTaskTime.IsZero() {
					if !q.nextTaskTimer.Stop() {
						// Drain timer channel to prevent timer firing.
						<-q.nextTaskTimer.C
					}
				}
				q.nextTaskTimer.Reset(newTask.GetVisibilityTime().Sub(q.timeSource.Now()))
			}
			metrics.NewTimerNotifyCounter.With(q.metricsHandler).Record(1)
		case <-q.nextTaskTimer.C:
			taskToExecute := q.taskQueue.Remove()
			// Skip tasks which are already canceled. Majority of the tasks in the queue should be cancelled already.
			nextTask := q.purgeCanceledTasks()
			if nextTask != nil {
				q.nextTaskTimer.Reset(nextTask.GetVisibilityTime().Sub(q.timeSource.Now()))
			}
			// If current taskToExecute is also cancelled don't submit it to scheduler.
			if taskToExecute.State() == ctasks.TaskStateCancelled {
				continue
			}

			// For scheduler metrics to work properly.
			taskToExecute.SetScheduledTime(q.timeSource.Now())

			if !q.scheduler.TrySubmit(taskToExecute) {
				// If all workers are busy then put the task back to the queue.
				// Must be done in a separate goroutine to avoid deadlock.
				go func() {
					taskToExecute.SetVisibilityTime(taskToExecute.GetVisibilityTime().Add(workerBusyRescheduleDelay))
					q.Add(taskToExecute)
				}()
			}
		}
	}
}
func (q *memoryScheduledQueue) purgeCanceledTasks() (nextTask Executable) {
	if !q.taskQueue.IsEmpty() {
		nextTask = q.taskQueue.Peek()
	}
	for nextTask != nil {
		if nextTask.State() != ctasks.TaskStateCancelled {
			return
		}
		// Remove canceled task from queue.
		q.taskQueue.Remove()
		if q.taskQueue.IsEmpty() {
			return nil
		}
		nextTask = q.taskQueue.Peek()
	}
	return
}
