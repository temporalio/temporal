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

package history

import (
	"sync"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
	"golang.org/x/exp/maps"
)

type (
	// queueAckMgr is created by QueueProcessor to keep track of the queue ackLevel for the shard.
	// It keeps track of read level when dispatching tasks to processor and maintains a map of outstanding tasks.
	// Outstanding tasks map uses the task id sequencer as the key, which is used by updateAckLevel to move the ack level
	// for the shard when all preceding tasks are acknowledged.
	queueAckMgrImpl struct {
		isFailover    bool
		shard         shard.Context
		options       *QueueProcessorOptions
		processor     processor
		logger        log.Logger
		metricsClient metrics.Client
		finishedChan  chan struct{}

		sync.RWMutex
		outstandingExecutables map[int64]queues.Executable
		readLevel              int64
		ackLevel               int64
		isReadFinished         bool

		executableInitializer taskExecutableInitializer
	}
)

const (
	warnPendingTasks = 2000
)

var _ queueAckMgr = (*queueAckMgrImpl)(nil)

func newQueueAckMgr(
	shard shard.Context,
	options *QueueProcessorOptions,
	processor processor,
	ackLevel int64,
	logger log.Logger,
	executableInitializer taskExecutableInitializer,
) *queueAckMgrImpl {

	return &queueAckMgrImpl{
		isFailover:             false,
		shard:                  shard,
		options:                options,
		processor:              processor,
		outstandingExecutables: make(map[int64]queues.Executable),
		readLevel:              ackLevel,
		ackLevel:               ackLevel,
		logger:                 logger,
		metricsClient:          shard.GetMetricsClient(),
		finishedChan:           nil,
		executableInitializer:  executableInitializer,
	}
}

func newQueueFailoverAckMgr(
	shard shard.Context,
	options *QueueProcessorOptions,
	processor processor,
	ackLevel int64,
	logger log.Logger,
	executableInitializer taskExecutableInitializer,
) *queueAckMgrImpl {

	return &queueAckMgrImpl{
		isFailover:             true,
		shard:                  shard,
		options:                options,
		processor:              processor,
		outstandingExecutables: make(map[int64]queues.Executable),
		readLevel:              ackLevel,
		ackLevel:               ackLevel,
		logger:                 logger,
		metricsClient:          shard.GetMetricsClient(),
		finishedChan:           make(chan struct{}, 1),
		executableInitializer:  executableInitializer,
	}
}

func (a *queueAckMgrImpl) readQueueTasks() ([]queues.Executable, bool, error) {
	a.RLock()
	readLevel := a.readLevel
	a.RUnlock()

	var tasks []tasks.Task
	var morePage bool
	op := func() error {
		var err error
		tasks, morePage, err = a.processor.readTasks(readLevel)
		return err
	}

	err := backoff.ThrottleRetry(op, workflow.PersistenceOperationRetryPolicy, common.IsPersistenceTransientError)
	if err != nil {
		return nil, false, err
	}

	a.Lock()
	defer a.Unlock()
	if a.isFailover && !morePage {
		a.isReadFinished = true
	}

	filteredExecutables := make([]queues.Executable, 0, len(tasks))

TaskFilterLoop:
	for _, task := range tasks {
		_, isLoaded := a.outstandingExecutables[task.GetTaskID()]
		if isLoaded {
			// task already loaded
			a.logger.Debug("Skipping transfer task", tag.Task(task))
			continue TaskFilterLoop
		}

		if a.readLevel >= task.GetTaskID() {
			a.logger.Fatal("Next task ID is less than current read level.",
				tag.TaskID(task.GetTaskID()),
				tag.ReadLevel(a.readLevel))
		}
		a.logger.Debug("Moving read level", tag.TaskID(task.GetTaskID()))
		a.readLevel = task.GetTaskID()

		taskExecutable := a.executableInitializer(task)
		a.outstandingExecutables[task.GetTaskID()] = taskExecutable
		filteredExecutables = append(filteredExecutables, taskExecutable)
	}

	return filteredExecutables, morePage, nil
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

func (a *queueAckMgrImpl) updateQueueAckLevel() error {
	a.metricsClient.IncCounter(a.options.MetricScope, metrics.AckLevelUpdateCounter)

	a.Lock()
	ackLevel := a.ackLevel

	// task ID is not sequential, meaning there are a ton of missing chunks,
	// so to optimize the performance, a sort is required
	taskIDs := maps.Keys(a.outstandingExecutables)
	util.SortSlice(taskIDs)

	pendingTasks := len(taskIDs)
	if pendingTasks > warnPendingTasks {
		a.logger.Warn("Too many pending tasks")
	}

	metricsScope := a.metricsClient.Scope(metrics.ShardInfoScope)
	switch a.options.MetricScope {
	case metrics.ReplicatorQueueProcessorScope:
		metricsScope.RecordDistribution(metrics.ShardInfoReplicationPendingTasksTimer, pendingTasks)
	case metrics.TransferActiveQueueProcessorScope:
		metricsScope.RecordDistribution(metrics.ShardInfoTransferActivePendingTasksTimer, pendingTasks)
	case metrics.TransferStandbyQueueProcessorScope:
		metricsScope.RecordDistribution(metrics.ShardInfoTransferStandbyPendingTasksTimer, pendingTasks)
	case metrics.VisibilityQueueProcessorScope:
		metricsScope.RecordDistribution(metrics.ShardInfoVisibilityPendingTasksTimer, pendingTasks)
	}

MoveAckLevelLoop:
	for _, current := range taskIDs {
		acked := a.outstandingExecutables[current].State() == ctasks.TaskStateAcked
		if acked {
			ackLevel = current
			delete(a.outstandingExecutables, current)
			a.logger.Debug("Moving timer ack level to", tag.AckLevel(ackLevel))
		} else {
			break MoveAckLevelLoop
		}
	}
	a.ackLevel = ackLevel

	if a.isFailover && a.isReadFinished && len(a.outstandingExecutables) == 0 {
		a.Unlock()
		// this means in failover mode, all possible failover transfer tasks
		// are processed and we are free to shundown
		a.logger.Debug("Queue ack manager shutdown.")
		a.finishedChan <- struct{}{}
		err := a.processor.queueShutdown()
		if err != nil {
			a.logger.Error("Error shutdown queue", tag.Error(err))
		}
		return nil
	}

	a.Unlock()
	if err := a.processor.updateAckLevel(ackLevel); err != nil {
		a.metricsClient.IncCounter(a.options.MetricScope, metrics.AckLevelUpdateFailedCounter)
		a.logger.Error("Error updating ack level for shard", tag.Error(err), tag.OperationFailed)
		return err
	}
	return nil
}
