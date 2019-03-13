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

package matching

import (
	"context"
	"runtime"
	"time"

	"github.com/uber/cadence/common/persistence"

	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/metrics"
)

var epochStartTime = time.Unix(0, 0)

func (c *taskListManagerImpl) deliverBufferTasksForPoll() {
deliverBufferTasksLoop:
	for {
		err := c.rateLimiter.Wait(c.cancelCtx)
		if err != nil {
			if err == context.Canceled {
				c.logger.Info("Tasklist manager context is cancelled, shutting down")
				break deliverBufferTasksLoop
			}
			c.logger.Debugf(
				"Unable to add buffer task, rate limit failed, domainId: %s, tasklist: %s, error: %s",
				c.taskListID.domainID, c.taskListID.taskListName, err.Error(),
			)
			c.metricsClient.IncCounter(metrics.MatchingTaskListMgrScope, metrics.BufferThrottleCounter)
			// This is to prevent busy looping when throttling is set to 0
			runtime.Gosched()
			continue
		}
		select {
		case task, ok := <-c.taskBuffer:
			if !ok { // Task list getTasks pump is shutdown
				break deliverBufferTasksLoop
			}
			select {
			case c.tasksForPoll <- &getTaskResult{task: task}:
			case <-c.deliverBufferShutdownCh:
				break deliverBufferTasksLoop
			}
		case <-c.deliverBufferShutdownCh:
			break deliverBufferTasksLoop
		}
	}
}

func (c *taskListManagerImpl) getTasksPump() {
	defer close(c.taskBuffer)
	c.startWG.Wait()

	go c.deliverBufferTasksForPoll()
	updateAckTimer := time.NewTimer(c.config.UpdateAckInterval())
	checkIdleTaskListTimer := time.NewTimer(c.config.IdleTasklistCheckInterval())
	lastTimeWriteTask := time.Time{}
getTasksPumpLoop:
	for {
		select {
		case <-c.shutdownCh:
			break getTasksPumpLoop
		case <-c.notifyCh:
			{
				lastTimeWriteTask = time.Now()

				tasks, readLevel, isReadBatchDone, err := c.getTaskBatch()
				if err != nil {
					c.signalNewTask() // re-enqueue the event
					// TODO: Should we ever stop retrying on db errors?
					continue getTasksPumpLoop
				}

				if len(tasks) == 0 {
					c.taskAckManager.setReadLevel(readLevel)
					if !isReadBatchDone {
						c.signalNewTask()
					}
					continue getTasksPumpLoop
				}

				if !c.addTasksToBuffer(tasks, lastTimeWriteTask, checkIdleTaskListTimer) {
					break getTasksPumpLoop
				}
				// There maybe more tasks. We yield now, but signal pump to check again later.
				c.signalNewTask()
			}
		case <-updateAckTimer.C:
			{
				err := c.persistAckLevel()
				//var err error
				if err != nil {
					if _, ok := err.(*persistence.ConditionFailedError); ok {
						// This indicates the task list may have moved to another host.
						c.Stop()
					} else {
						logging.LogPersistantStoreErrorEvent(c.logger, logging.TagValueStoreOperationUpdateTaskList, err,
							"Persist AckLevel failed")
					}
					// keep going as saving ack is not critical
				}
				c.signalNewTask() // periodically signal pump to check persistence for tasks
				updateAckTimer = time.NewTimer(c.config.UpdateAckInterval())
			}
		case <-checkIdleTaskListTimer.C:
			{
				if c.isIdle(lastTimeWriteTask) {
					c.handleIdleTimeout()
					break getTasksPumpLoop
				}
				checkIdleTaskListTimer = time.NewTimer(c.config.IdleTasklistCheckInterval())
			}
		}
	}

	updateAckTimer.Stop()
	checkIdleTaskListTimer.Stop()
}

func (c *taskListManagerImpl) getTaskBatchWithRange(readLevel int64, maxReadLevel int64) ([]*persistence.TaskInfo, error) {
	response, err := c.executeWithRetry(func() (interface{}, error) {
		return c.db.GetTasks(readLevel, maxReadLevel, c.config.GetTasksBatchSize())
	})
	if err != nil {
		return nil, err
	}
	return response.(*persistence.GetTasksResponse).Tasks, err
}

// Returns a batch of tasks from persistence starting form current read level.
// Also return a number that can be used to update readLevel
// Also return a bool to indicate whether read is finished
func (c *taskListManagerImpl) getTaskBatch() ([]*persistence.TaskInfo, int64, bool, error) {
	var tasks []*persistence.TaskInfo
	readLevel := c.taskAckManager.getReadLevel()
	maxReadLevel := c.taskWriter.GetMaxReadLevel()

	// counter i is used to break and let caller check whether tasklist is still alive and need resume read.
	for i := 0; i < 10 && readLevel < maxReadLevel; i++ {
		upper := readLevel + c.config.RangeSize
		if upper > maxReadLevel {
			upper = maxReadLevel
		}
		tasks, err := c.getTaskBatchWithRange(readLevel, upper)
		if err != nil {
			return nil, readLevel, true, err
		}
		// return as long as it grabs any tasks
		if len(tasks) > 0 {
			return tasks, upper, true, nil
		}
		readLevel = upper
	}
	return tasks, readLevel, readLevel == maxReadLevel, nil // caller will update readLevel when no task grabbed
}

func (c *taskListManagerImpl) isTaskExpired(t *persistence.TaskInfo, now time.Time) bool {
	return t.Expiry.After(epochStartTime) && time.Now().After(t.Expiry)
}

func (c *taskListManagerImpl) isIdle(lastWriteTime time.Time) bool {
	return !c.isTaskAddedRecently(lastWriteTime) && len(c.GetAllPollerInfo()) == 0
}

func (c *taskListManagerImpl) handleIdleTimeout() {
	c.persistAckLevel()
	c.taskGC.RunNow(c.taskAckManager.getAckLevel())
	c.Stop()
}

func (c *taskListManagerImpl) addTasksToBuffer(
	tasks []*persistence.TaskInfo, lastWriteTime time.Time, idleTimer *time.Timer) bool {
	now := time.Now()
	for _, t := range tasks {
		if c.isTaskExpired(t, now) {
			c.metricsClient.IncCounter(metrics.MatchingTaskListMgrScope, metrics.ExpiredTasksCounter)
			continue
		}
		c.taskAckManager.addTask(t.TaskID)
		select {
		case c.taskBuffer <- t:
			return true
		case <-idleTimer.C:
			if c.isIdle(lastWriteTime) {
				c.handleIdleTimeout()
				return false
			}
		case <-c.shutdownCh:
			return false
		}
	}
	return true
}
