// Copyright (c) 2017-2020 Uber Technologies Inc.

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package queue

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/collection"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/quotas"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"github.com/uber/cadence/service/history/config"
	"github.com/uber/cadence/service/history/shard"
	"github.com/uber/cadence/service/history/task"
)

var (
	loadQueueTaskThrottleRetryDelay = 5 * time.Second

	persistenceOperationRetryPolicy = common.CreatePersistanceRetryPolicy()
)

type (
	transferTaskKey struct {
		taskID int64
	}

	transferQueueProcessorBase struct {
		shard           shard.Context
		taskProcessor   task.Processor
		redispatchQueue collection.Queue

		options               *queueProcessorOptions
		updateMaxReadLevel    updateMaxReadLevelFn
		updateClusterAckLevel updateClusterAckLevelFn
		queueShutdown         queueShutdownFn
		taskInitializer       task.Initializer

		logger        log.Logger
		metricsClient metrics.Client
		metricsScope  metrics.Scope

		rateLimiter  quotas.Limiter
		lastPollTime time.Time
		notifyCh     chan struct{}
		status       int32
		shutdownWG   sync.WaitGroup
		shutdownCh   chan struct{}

		lastSplitTime    time.Time
		lastMaxReadLevel int64

		queueCollectionsLock       sync.RWMutex
		processingQueueCollections []ProcessingQueueCollection
	}
)

func newTransferQueueProcessorBase(
	shard shard.Context,
	processingQueueStates []ProcessingQueueState,
	taskProcessor task.Processor,
	redispatchQueue collection.Queue,
	options *queueProcessorOptions,
	updateMaxReadLevel updateMaxReadLevelFn,
	updateClusterAckLevel updateClusterAckLevelFn,
	queueShutdown queueShutdownFn,
	taskInitializer task.Initializer,
	logger log.Logger,
	metricsClient metrics.Client,
) *transferQueueProcessorBase {

	return &transferQueueProcessorBase{
		shard:           shard,
		taskProcessor:   taskProcessor,
		redispatchQueue: redispatchQueue,

		options:               options,
		updateMaxReadLevel:    updateMaxReadLevel,
		updateClusterAckLevel: updateClusterAckLevel,
		queueShutdown:         queueShutdown,
		taskInitializer:       taskInitializer,

		logger:        logger.WithTags(tag.ComponentTransferQueue),
		metricsClient: metricsClient,
		metricsScope:  metricsClient.Scope(options.MetricScope),

		rateLimiter: quotas.NewDynamicRateLimiter(
			func() float64 {
				return float64(options.MaxPollRPS())
			},
		),
		lastPollTime: time.Time{},
		notifyCh:     make(chan struct{}, 1),
		status:       common.DaemonStatusInitialized,
		shutdownCh:   make(chan struct{}),

		lastSplitTime:    time.Time{},
		lastMaxReadLevel: 0,

		processingQueueCollections: newProcessingQueueCollections(
			processingQueueStates,
			logger,
			metricsClient,
		),
	}
}

func (t *transferQueueProcessorBase) Start() {
	if !atomic.CompareAndSwapInt32(&t.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	t.logger.Info("", tag.LifeCycleStarting)
	defer t.logger.Info("", tag.LifeCycleStarted)

	t.notifyNewTask()

	t.shutdownWG.Add(1)
	go t.processorPump()
}

func (t *transferQueueProcessorBase) Stop() {
	if !atomic.CompareAndSwapInt32(&t.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	t.logger.Info("", tag.LifeCycleStopping)
	defer t.logger.Info("", tag.LifeCycleStopped)

	close(t.shutdownCh)

	if success := common.AwaitWaitGroup(&t.shutdownWG, time.Minute); !success {
		t.logger.Warn("", tag.LifeCycleStopTimedout)
	}
}

func (t *transferQueueProcessorBase) notifyNewTask() {
	select {
	case t.notifyCh <- struct{}{}:
	default:
	}
}

func (t *transferQueueProcessorBase) processorPump() {
	defer t.shutdownWG.Done()

	pollTimer := time.NewTimer(backoff.JitDuration(
		t.options.MaxPollInterval(),
		t.options.MaxPollIntervalJitterCoefficient(),
	))
	defer pollTimer.Stop()

	updateAckTimer := time.NewTimer(backoff.JitDuration(
		t.options.UpdateAckInterval(),
		t.options.UpdateAckIntervalJitterCoefficient(),
	))
	defer updateAckTimer.Stop()

	redispatchTimer := time.NewTimer(backoff.JitDuration(
		t.options.RedispatchInterval(),
		t.options.RedispatchIntervalJitterCoefficient(),
	))
	defer redispatchTimer.Stop()

	splitQueueTimer := time.NewTimer(backoff.JitDuration(
		t.options.SplitQueueInterval(),
		t.options.SplitQueueIntervalJitterCoefficient(),
	))
	defer splitQueueTimer.Stop()

processorPumpLoop:
	for {
		select {
		case <-t.shutdownCh:
			break processorPumpLoop
		case <-t.notifyCh:
			if t.redispatchQueue.Len() <= t.options.MaxRedispatchQueueSize() {
				t.processBatch()
				continue
			}

			// has too many pending tasks in re-dispatch queue, block loading tasks from persistence
			RedispatchTasks(
				t.redispatchQueue,
				t.taskProcessor,
				t.logger,
				t.metricsScope,
				t.shutdownCh,
			)
			// re-enqueue the event to see if we need keep re-dispatching or load new tasks from persistence
			t.notifyNewTask()
		case <-pollTimer.C:
			if t.lastPollTime.Add(t.options.MaxPollInterval()).Before(t.shard.GetTimeSource().Now()) {
				t.processBatch()
			}
			pollTimer.Reset(backoff.JitDuration(
				t.options.MaxPollInterval(),
				t.options.MaxPollIntervalJitterCoefficient(),
			))
		case <-updateAckTimer.C:
			processFinished, err := t.updateAckLevel()
			if err == shard.ErrShardClosed || (err == nil && processFinished) {
				go t.Stop()
				break processorPumpLoop
			}
			updateAckTimer.Reset(backoff.JitDuration(
				t.options.UpdateAckInterval(),
				t.options.UpdateAckIntervalJitterCoefficient(),
			))
		case <-redispatchTimer.C:
			RedispatchTasks(
				t.redispatchQueue,
				t.taskProcessor,
				t.logger,
				t.metricsScope,
				t.shutdownCh,
			)
			redispatchTimer.Reset(backoff.JitDuration(
				t.options.RedispatchInterval(),
				t.options.RedispatchIntervalJitterCoefficient(),
			))
		case <-splitQueueTimer.C:
			t.splitQueue()
			splitQueueTimer.Reset(backoff.JitDuration(
				t.options.SplitQueueInterval(),
				t.options.SplitQueueIntervalJitterCoefficient(),
			))
		}
	}
}

func (t *transferQueueProcessorBase) processBatch() {
	ctx, cancel := context.WithTimeout(context.Background(), loadQueueTaskThrottleRetryDelay)
	if err := t.rateLimiter.Wait(ctx); err != nil {
		cancel()
		t.notifyNewTask()
		return
	}
	cancel()

	t.lastPollTime = t.shard.GetTimeSource().Now()

	t.queueCollectionsLock.RLock()
	processingQueueCollections := t.processingQueueCollections
	t.queueCollectionsLock.RUnlock()

	// TODO: create a feedback loop to slow down loading for non-default queues (queues with level > 0)
	for _, queueCollection := range processingQueueCollections {
		t.queueCollectionsLock.RLock()
		activeQueue := queueCollection.ActiveQueue()
		if activeQueue == nil {
			t.queueCollectionsLock.RUnlock()
			continue
		}

		readLevel := activeQueue.State().ReadLevel()
		maxReadLevel := activeQueue.State().MaxLevel()
		t.queueCollectionsLock.RUnlock()

		shardMaxReadLevel := t.updateMaxReadLevel()
		if shardMaxReadLevel.Less(maxReadLevel) {
			maxReadLevel = shardMaxReadLevel
			if !readLevel.Less(maxReadLevel) {
				continue
			}
		}

		transferTaskInfos, more, err := t.readTasks(readLevel, maxReadLevel)
		if err != nil {
			t.logger.Error("Processor unable to retrieve tasks", tag.Error(err))
			t.notifyNewTask() // re-enqueue the event
			return
		}

		tasks := make(map[task.Key]task.Task)
		domainFilter := activeQueue.State().DomainFilter()
		for _, taskInfo := range transferTaskInfos {
			if !domainFilter.Filter(taskInfo.GetDomainID()) {
				continue
			}

			task := t.taskInitializer(taskInfo)
			tasks[newTransferTaskKey(taskInfo.GetTaskID())] = task
			if submitted := t.submitTask(task); !submitted {
				// not submitted since processor has been shutdown
				return
			}
		}

		var newReadLevel task.Key
		if !more {
			newReadLevel = maxReadLevel
		} else {
			newReadLevel = newTransferTaskKey(transferTaskInfos[len(transferTaskInfos)-1].GetTaskID())
		}
		t.queueCollectionsLock.Lock()
		queueCollection.AddTasks(tasks, newReadLevel)
		newActiveQueue := queueCollection.ActiveQueue()
		t.queueCollectionsLock.Unlock()

		if more || (newActiveQueue != nil && newActiveQueue != activeQueue) {
			// more tasks for the current active queue or the active queue has changed
			t.notifyNewTask()
		}
	}
}

func (t *transferQueueProcessorBase) updateAckLevel() (bool, error) {
	// TODO: only for now, find the min ack level across all processing queues
	// and update DB with that value.
	// Once persistence layer is updated, we need to persist all queue states
	// instead of only the min ack level
	t.queueCollectionsLock.Lock()
	var minAckLevel task.Key
	for _, queueCollection := range t.processingQueueCollections {
		queueCollection.UpdateAckLevels()

		for _, queue := range queueCollection.Queues() {
			if minAckLevel == nil {
				minAckLevel = queue.State().AckLevel()
			} else {
				minAckLevel = minTaskKey(minAckLevel, queue.State().AckLevel())
			}
		}
	}
	t.queueCollectionsLock.Unlock()

	if minAckLevel == nil {
		// note that only failover processor will meet this condition
		err := t.queueShutdown()
		if err != nil {
			t.logger.Error("Error shutdown queue", tag.Error(err))
			// return error so that shutdown callback can be retried
			return false, err
		}
		return true, nil
	}

	// TODO: emit metrics for total # of pending tasks

	if err := t.updateClusterAckLevel(minAckLevel); err != nil {
		t.logger.Error("Error updating ack level for shard", tag.Error(err), tag.OperationFailed)
		t.metricsScope.IncCounter(metrics.AckLevelUpdateFailedCounter)
		return false, err
	}

	return false, nil
}

func (t *transferQueueProcessorBase) splitQueue() {
	currentTime := t.shard.GetTimeSource().Now()
	currentMaxReadLevel := t.updateMaxReadLevel().(transferTaskKey).taskID
	defer func() {
		t.lastSplitTime = currentTime
		t.lastMaxReadLevel = currentMaxReadLevel
	}()

	if t.lastSplitTime.IsZero() {
		// skip the first split as we can't estimate the look ahead taskID
		return
	}

	lookAhead := (currentMaxReadLevel - t.lastMaxReadLevel) / int64(currentTime.Sub(t.lastSplitTime))

	splitPolicy := initializeSplitPolicy(
		t.options,
		func(key task.Key, domainID string) task.Key {
			totalLookAhead := lookAhead * int64(t.options.SplitLookAheadDurationByDomainID(domainID))
			return newTransferTaskKey(key.(timerTaskKey).taskID + totalLookAhead)
		},
		t.logger,
	)
	if splitPolicy == nil {
		return
	}

	t.queueCollectionsLock.Lock()
	defer t.queueCollectionsLock.Unlock()

	t.processingQueueCollections = splitProcessingQueueCollection(
		t.processingQueueCollections,
		splitPolicy,
	)
}

func (t *transferQueueProcessorBase) getProcessingQueueStates() []ProcessingQueueState {
	t.queueCollectionsLock.RLock()
	defer t.queueCollectionsLock.RUnlock()

	var queueStates []ProcessingQueueState
	for _, queueCollection := range t.processingQueueCollections {
		for _, queue := range queueCollection.Queues() {
			queueStates = append(queueStates, copyQueueState(queue.State()))
		}
	}

	return queueStates
}

func (t *transferQueueProcessorBase) readTasks(
	readLevel task.Key,
	maxReadLevel task.Key,
) ([]*persistence.TransferTaskInfo, bool, error) {

	var response *persistence.GetTransferTasksResponse
	op := func() error {
		var err error
		response, err = t.shard.GetExecutionManager().GetTransferTasks(&persistence.GetTransferTasksRequest{
			ReadLevel:    readLevel.(transferTaskKey).taskID,
			MaxReadLevel: maxReadLevel.(transferTaskKey).taskID,
			BatchSize:    t.options.BatchSize(),
		})
		return err
	}

	err := backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
	if err != nil {
		return nil, false, err
	}

	return response.Tasks, len(response.NextPageToken) != 0, nil
}

func (t *transferQueueProcessorBase) submitTask(
	task task.Task,
) bool {
	submitted, err := t.taskProcessor.TrySubmit(task)
	if err != nil {
		select {
		case <-t.shutdownCh:
			// if error is due to shard shutdown
			return false
		default:
			// otherwise it might be error from domain cache etc, add
			// the task to redispatch queue so that it can be retried
			t.logger.Error("Failed to submit task", tag.Error(err))
		}
	}
	if err != nil || !submitted {
		t.redispatchQueue.Add(task)
	}

	return true
}

func newTransferTaskKey(
	taskID int64,
) task.Key {
	return transferTaskKey{
		taskID: taskID,
	}
}

func (k transferTaskKey) Less(
	key task.Key,
) bool {
	return k.taskID < key.(transferTaskKey).taskID
}

func newTransferQueueProcessorOptions(
	config *config.Config,
	isActive bool,
	isFailover bool,
) *queueProcessorOptions {
	options := &queueProcessorOptions{
		BatchSize:                           config.TransferTaskBatchSize,
		MaxPollRPS:                          config.TransferProcessorMaxPollRPS,
		MaxPollInterval:                     config.TransferProcessorMaxPollInterval,
		MaxPollIntervalJitterCoefficient:    config.TransferProcessorMaxPollIntervalJitterCoefficient,
		UpdateAckInterval:                   config.TransferProcessorUpdateAckInterval,
		UpdateAckIntervalJitterCoefficient:  config.TransferProcessorUpdateAckIntervalJitterCoefficient,
		RedispatchInterval:                  config.TransferProcessorRedispatchInterval,
		RedispatchIntervalJitterCoefficient: config.TransferProcessorRedispatchIntervalJitterCoefficient,
		MaxRedispatchQueueSize:              config.TransferProcessorMaxRedispatchQueueSize,
		SplitQueueInterval:                  config.TransferProcessorSplitQueueInterval,
		SplitQueueIntervalJitterCoefficient: config.TransferProcessorSplitQueueIntervalJitterCoefficient,
	}

	if isFailover {
		// disable queue split for failover processor
		options.EnableSplit = dynamicconfig.GetBoolPropertyFn(false)
	} else {
		options.EnableSplit = config.QueueProcessorEnableSplit
		options.SplitMaxLevel = config.QueueProcessorSplitMaxLevel
		options.EnableRandomSplitByDomainID = config.QueueProcessorEnableRandomSplitByDomainID
		options.RandomSplitProbability = config.QueueProcessorRandomSplitProbability
		options.EnablePendingTaskSplit = config.QueueProcessorEnablePendingTaskSplit
		options.PendingTaskSplitThreshold = config.QueueProcessorPendingTaskSplitThreshold
		options.EnableStuckTaskSplit = config.QueueProcessorEnableStuckTaskSplit
		options.StuckTaskSplitThreshold = config.QueueProcessorStuckTaskSplitThreshold
		options.SplitLookAheadDurationByDomainID = config.QueueProcessorSplitLookAheadDurationByDomainID
	}

	if isActive {
		options.MetricScope = metrics.TransferActiveQueueProcessorScope
	} else {
		options.MetricScope = metrics.TransferStandbyQueueProcessorScope
	}

	return options
}
