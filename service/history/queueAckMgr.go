package history

import (
	"sort"
	"sync"
	"time"

	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/backoff"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/metrics"
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
		logger        log.Logger
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
	warnPendingTasks = 2000
)

func newQueueAckMgr(shard ShardContext, options *QueueProcessorOptions, processor processor, ackLevel int64, logger log.Logger) *queueAckMgrImpl {

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

func newQueueFailoverAckMgr(shard ShardContext, options *QueueProcessorOptions, processor processor, ackLevel int64, logger log.Logger) *queueAckMgrImpl {

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
		_, isLoaded := a.outstandingTasks[task.GetTaskId()]
		if isLoaded {
			// task already loaded
			a.logger.Debug("Skipping transfer task", tag.Task(task))
			continue TaskFilterLoop
		}

		if a.readLevel >= task.GetTaskId() {
			a.logger.Fatal("Next task ID is less than current read level.",
				tag.TaskID(task.GetTaskId()),
				tag.ReadLevel(a.readLevel))
		}
		a.logger.Debug("Moving read level", tag.TaskID(task.GetTaskId()))
		a.readLevel = task.GetTaskId()
		a.outstandingTasks[task.GetTaskId()] = false
	}

	return tasks, morePage, nil
}

func (a *queueAckMgrImpl) completeQueueTask(taskID int64) {
	a.Lock()
	if _, ok := a.outstandingTasks[taskID]; ok {
		a.outstandingTasks[taskID] = true
	}
	a.Unlock()
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

	a.logger.Debug("Moving timer ack level", tag.AckLevel(ackLevel), tag.Tasks(a.outstandingTasks))

	// task ID is not sequancial, meaning there are a ton of missing chunks,
	// so to optimize the performance, a sort is required
	var taskIDs []int64
	for k := range a.outstandingTasks {
		taskIDs = append(taskIDs, k)
	}
	sort.Slice(taskIDs, func(i, j int) bool { return taskIDs[i] < taskIDs[j] })

	pendingTasks := len(taskIDs)
	if pendingTasks > warnPendingTasks {
		a.logger.Warn("Too many pending tasks")
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
			a.logger.Debug("Moving timer ack level to", tag.AckLevel(ackLevel))
		} else {
			break MoveAckLevelLoop
		}
	}
	a.ackLevel = ackLevel

	if a.isFailover && a.isReadFinished && len(a.outstandingTasks) == 0 {
		a.Unlock()
		// this means in failover mode, all possible failover transfer tasks
		// are processed and we are free to shundown
		a.logger.Debug("Queue ack manager shutdown.")
		a.finishedChan <- struct{}{}
		err := a.processor.queueShutdown()
		if err != nil {
			a.logger.Error("Error shutdown queue", tag.Error(err))
		}
		return
	}

	a.Unlock()
	if err := a.processor.updateAckLevel(ackLevel); err != nil {
		a.metricsClient.IncCounter(a.options.MetricScope, metrics.AckLevelUpdateFailedCounter)
		a.logger.Error("Error updating ack level for shard", tag.Error(err), tag.OperationFailed)
	}
}
