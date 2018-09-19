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

package history

import (
	"sort"
	"sync"
	"time"

	"github.com/uber-common/bark"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/metrics"
)

type (
	// queueAckMgr is created by QueueProcessor to keep track of the queue ackLevel for the shard.
	// It keeps track of read level when dispatching tasks to processor and maintains a map of outstanding tasks.
	// Outstanding tasks map uses the task id sequencer as the key, which is used by updateAckLevel to move the ack level
	// for the shard when all preceding tasks are acknowledged.
	queueAckMgrImpl struct {
		isFailover    bool
		shard         ShardContext
		options       *QueueProcessorOptions
		processor     processor
		logger        bark.Logger
		metricsClient metrics.Client
		finishedChan  chan struct{}

		sync.RWMutex
		outstandingTasks map[int64]bool
		readLevel        int64
		ackLevel         int64
		isReadFinished   bool
	}
)

const (
	warnPendingTasks = 1000
)

func newQueueAckMgr(shard ShardContext, options *QueueProcessorOptions, processor processor, ackLevel int64, logger bark.Logger) *queueAckMgrImpl {

	return &queueAckMgrImpl{
		isFailover:       false,
		shard:            shard,
		options:          options,
		processor:        processor,
		outstandingTasks: make(map[int64]bool),
		readLevel:        ackLevel,
		ackLevel:         ackLevel,
		logger:           logger,
		metricsClient:    shard.GetMetricsClient(),
		finishedChan:     nil,
	}
}

func newQueueFailoverAckMgr(shard ShardContext, options *QueueProcessorOptions, processor processor, ackLevel int64, logger bark.Logger) *queueAckMgrImpl {

	return &queueAckMgrImpl{
		isFailover:       true,
		shard:            shard,
		options:          options,
		processor:        processor,
		outstandingTasks: make(map[int64]bool),
		readLevel:        ackLevel,
		ackLevel:         ackLevel,
		logger:           logger,
		metricsClient:    shard.GetMetricsClient(),
		finishedChan:     make(chan struct{}, 1),
	}
}

func (a *queueAckMgrImpl) readQueueTasks() ([]queueTaskInfo, bool, error) {
	a.RLock()
	readLevel := a.readLevel
	a.RUnlock()

	var tasks []queueTaskInfo
	var morePage bool
	op := func() error {
		var err error
		tasks, morePage, err = a.processor.readTasks(readLevel)
		return err
	}

	err := backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
	if err != nil {
		return nil, false, err
	}

	a.Lock()
	defer a.Unlock()
	if a.isFailover && !morePage {
		a.isReadFinished = true
	}

TaskFilterLoop:
	for _, task := range tasks {
		_, isLoaded := a.outstandingTasks[task.GetTaskID()]
		if isLoaded {
			// task already loaded
			a.logger.Debugf("Skipping transfer task: %v.", task)
			continue TaskFilterLoop
		}

		if a.readLevel >= task.GetTaskID() {
			a.logger.Fatalf("Next task ID is less than current read level.  TaskID: %v, ReadLevel: %v", task.GetTaskID(),
				a.readLevel)
		}
		a.logger.Debugf("Moving read level: %v", task.GetTaskID())
		a.readLevel = task.GetTaskID()
		a.outstandingTasks[task.GetTaskID()] = false
	}

	return tasks, morePage, nil
}

func (a *queueAckMgrImpl) completeQueueTask(taskID int64) error {
	err := a.processor.completeTask(taskID)
	if err != nil {
		return err
	}

	a.Lock()
	if _, ok := a.outstandingTasks[taskID]; ok {
		a.outstandingTasks[taskID] = true
	}
	a.Unlock()
	return nil
}

func (a *queueAckMgrImpl) getQueueAckLevel() int64 {
	a.Lock()
	defer a.Unlock()
	return a.ackLevel
}

func (a *queueAckMgrImpl) getQueueReadLevel() int64 {
	a.Lock()
	defer a.Unlock()
	return a.readLevel
}

func (a *queueAckMgrImpl) getFinishedChan() <-chan struct{} {
	return a.finishedChan
}

func (a *queueAckMgrImpl) updateQueueAckLevel() {
	a.metricsClient.IncCounter(a.options.MetricScope, metrics.AckLevelUpdateCounter)

	a.Lock()
	ackLevel := a.ackLevel

	a.logger.Debugf("Moving timer ack level from %v, with %v.", ackLevel, a.outstandingTasks)

	// task ID is not sequancial, meaning there are a ton of missing chunks,
	// so to optimize the performance, a sort is required
	var taskIDs []int64
	for k := range a.outstandingTasks {
		taskIDs = append(taskIDs, k)
	}
	sort.Slice(taskIDs, func(i, j int) bool { return taskIDs[i] < taskIDs[j] })

	pendingTasks := len(taskIDs)
	if pendingTasks > warnPendingTasks {
		a.logger.Warn("Too many pendind tasks.")
	}
	switch a.options.MetricScope {
	case metrics.ReplicatorQueueProcessorScope:
		a.metricsClient.RecordTimer(metrics.ShardInfoScope, metrics.ShardInfoReplicationPendingTasksTimer, time.Duration(pendingTasks))
	case metrics.TransferActiveQueueProcessorScope:
		a.metricsClient.RecordTimer(metrics.ShardInfoScope, metrics.ShardInfoTransferActivePendingTasksTimer, time.Duration(pendingTasks))
	case metrics.TransferStandbyQueueProcessorScope:
		a.metricsClient.RecordTimer(metrics.ShardInfoScope, metrics.ShardInfoTransferStandbyPendingTasksTimer, time.Duration(pendingTasks))
	}

MoveAckLevelLoop:
	for _, current := range taskIDs {
		acked := a.outstandingTasks[current]
		if acked {
			ackLevel = current
			delete(a.outstandingTasks, current)
			a.logger.Debugf("Moving timer ack level to %v.", ackLevel)
		} else {
			break MoveAckLevelLoop
		}
	}
	a.ackLevel = ackLevel

	if a.isFailover && a.isReadFinished && len(a.outstandingTasks) == 0 {
		a.Unlock()
		// this means in failover mode, all possible failover transfer tasks
		// are processed and we are free to shundown
		a.logger.Debugf("Queue ack manager shutdown.")
		a.finishedChan <- struct{}{}
		a.processor.queueShutdown()
		return
	}

	a.Unlock()
	if err := a.processor.updateAckLevel(ackLevel); err != nil {
		a.metricsClient.IncCounter(a.options.MetricScope, metrics.AckLevelUpdateFailedCounter)
		logging.LogOperationFailedEvent(a.logger, "Error updating ack level for shard", err)
	}
}
