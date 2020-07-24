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
	"sync/atomic"
	"time"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"github.com/uber/cadence/service/history/config"
	"github.com/uber/cadence/service/history/shard"
	"github.com/uber/cadence/service/history/task"
)

var (
	loadQueueTaskThrottleRetryDelay = 5 * time.Second

	nonDefaultQueueBackoffDuration = 5 * time.Second

	persistenceOperationRetryPolicy = common.CreatePersistenceRetryPolicy()
)

type (
	transferTaskKey struct {
		taskID int64
	}

	transferQueueProcessorBase struct {
		*processorBase

		taskInitializer task.Initializer

		notifyCh      chan struct{}
		nextPollTime  map[int]time.Time
		nextPollTimer TimerGate

		lastSplitTime    time.Time
		lastMaxReadLevel int64
	}
)

func newTransferQueueProcessorBase(
	shard shard.Context,
	processingQueueStates []ProcessingQueueState,
	taskProcessor task.Processor,
	options *queueProcessorOptions,
	updateMaxReadLevel updateMaxReadLevelFn,
	updateClusterAckLevel updateClusterAckLevelFn,
	queueShutdown queueShutdownFn,
	taskFilter task.Filter,
	taskExecutor task.Executor,
	logger log.Logger,
	metricsClient metrics.Client,
) *transferQueueProcessorBase {
	processorBase := newProcessorBase(
		shard,
		processingQueueStates,
		taskProcessor,
		options,
		updateMaxReadLevel,
		updateClusterAckLevel,
		queueShutdown,
		logger.WithTags(tag.ComponentTransferQueue),
		metricsClient,
	)

	queueType := task.QueueTypeActiveTransfer
	if options.MetricScope == metrics.TransferStandbyQueueProcessorScope {
		queueType = task.QueueTypeStandbyTransfer
	}

	// read dynamic config only once on startup to avoid gc pressure caused by keeping reading dynamic config
	emitDomainTag := shard.GetConfig().QueueProcessorEnableDomainTaggedMetrics()

	return &transferQueueProcessorBase{
		processorBase: processorBase,

		taskInitializer: func(taskInfo task.Info) task.Task {
			return task.NewTransferTask(
				shard,
				taskInfo,
				queueType,
				task.InitializeLoggerForTask(shard.GetShardID(), taskInfo, logger),
				taskFilter,
				taskExecutor,
				processorBase.redispatcher.AddTask,
				shard.GetTimeSource(),
				shard.GetConfig().TransferTaskMaxRetryCount,
				emitDomainTag,
				nil,
			)
		},

		notifyCh:      make(chan struct{}, 1),
		nextPollTime:  make(map[int]time.Time),
		nextPollTimer: NewLocalTimerGate(shard.GetTimeSource()),

		lastSplitTime:    time.Time{},
		lastMaxReadLevel: 0,
	}
}

func (t *transferQueueProcessorBase) Start() {
	if !atomic.CompareAndSwapInt32(&t.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	t.logger.Info("", tag.LifeCycleStarting)
	defer t.logger.Info("", tag.LifeCycleStarted)

	t.redispatcher.Start()

	for _, queueCollections := range t.processingQueueCollections {
		t.upsertPollTime(queueCollections.Level(), time.Time{})
	}

	t.shutdownWG.Add(1)
	go t.processorPump()
}

func (t *transferQueueProcessorBase) Stop() {
	if !atomic.CompareAndSwapInt32(&t.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	t.logger.Info("", tag.LifeCycleStopping)
	defer t.logger.Info("", tag.LifeCycleStopped)

	t.nextPollTimer.Close()
	close(t.shutdownCh)

	if success := common.AwaitWaitGroup(&t.shutdownWG, time.Minute); !success {
		t.logger.Warn("", tag.LifeCycleStopTimedout)
	}

	t.redispatcher.Stop()
}

func (t *transferQueueProcessorBase) notifyNewTask() {
	select {
	case t.notifyCh <- struct{}{}:
	default:
	}
}

func (t *transferQueueProcessorBase) upsertPollTime(level int, newPollTime time.Time) {
	if currentPollTime, ok := t.nextPollTime[level]; !ok || newPollTime.Before(currentPollTime) {
		t.nextPollTime[level] = newPollTime
		t.nextPollTimer.Update(newPollTime)
	}
}

func (t *transferQueueProcessorBase) processorPump() {
	defer t.shutdownWG.Done()

	updateAckTimer := time.NewTimer(backoff.JitDuration(
		t.options.UpdateAckInterval(),
		t.options.UpdateAckIntervalJitterCoefficient(),
	))
	defer updateAckTimer.Stop()

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
			t.upsertPollTime(defaultProcessingQueueLevel, time.Time{})
		case <-t.nextPollTimer.FireChan():
			maxRedispatchQueueSize := t.options.MaxRedispatchQueueSize()
			if t.redispatcher.Size() > maxRedispatchQueueSize {
				// has too many pending tasks in re-dispatch queue, block loading tasks from persistence
				t.redispatcher.Redispatch(maxRedispatchQueueSize)
				if t.redispatcher.Size() > maxRedispatchQueueSize {
					// if redispatcher still has a large number of tasks
					// this only happens when system is under very high load
					// we should backoff here instead of keeping submitting tasks to task processor
					time.Sleep(loadQueueTaskThrottleRetryDelay)
				}
				// re-enqueue the event to see if we need keep re-dispatching or load new tasks from persistence
				t.nextPollTimer.Update(time.Time{})
				continue processorPumpLoop
			}

			levels := make(map[int]struct{})
			now := t.shard.GetTimeSource().Now()
			for level, pollTime := range t.nextPollTime {
				if !now.Before(pollTime) {
					levels[level] = struct{}{}
					delete(t.nextPollTime, level)
				} else {
					t.nextPollTimer.Update(pollTime)
				}
			}

			t.processQueueCollections(levels)
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
		case <-splitQueueTimer.C:
			t.splitQueue()
			splitQueueTimer.Reset(backoff.JitDuration(
				t.options.SplitQueueInterval(),
				t.options.SplitQueueIntervalJitterCoefficient(),
			))
		}
	}
}

func (t *transferQueueProcessorBase) processQueueCollections(levels map[int]struct{}) {
	t.queueCollectionsLock.RLock()
	processingQueueCollections := t.processingQueueCollections
	t.queueCollectionsLock.RUnlock()

	for _, queueCollection := range processingQueueCollections {
		t.queueCollectionsLock.RLock()
		level := queueCollection.Level()
		if _, ok := levels[level]; !ok {
			t.queueCollectionsLock.RUnlock()
			continue
		}

		activeQueue := queueCollection.ActiveQueue()
		if activeQueue == nil {
			// process for this queue collection has finished
			// it's possible that new queue will be added to this collection later though,
			// pollTime will be updated after split/merge
			t.queueCollectionsLock.RUnlock()
			continue
		}

		readLevel := activeQueue.State().ReadLevel()
		maxReadLevel := minTaskKey(activeQueue.State().MaxLevel(), t.updateMaxReadLevel())
		domainFilter := activeQueue.State().DomainFilter()
		t.queueCollectionsLock.RUnlock()

		if !readLevel.Less(maxReadLevel) {
			if level != defaultProcessingQueueLevel {
				// we only need to notify non default queue in this case
				// poll time for default queue will be updated through notifyNewTask
				t.upsertPollTime(level, t.shard.GetTimeSource().Now().Add(nonDefaultQueueBackoffDuration))
			}
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), loadQueueTaskThrottleRetryDelay)
		if err := t.rateLimiter.Wait(ctx); err != nil {
			cancel()
			backoff := time.Duration(0)
			if level != defaultProcessingQueueLevel {
				backoff = nonDefaultQueueBackoffDuration
			}
			t.upsertPollTime(level, t.shard.GetTimeSource().Now().Add(backoff))
			continue
		}
		cancel()

		transferTaskInfos, more, err := t.readTasks(readLevel, maxReadLevel)
		if err != nil {
			t.logger.Error("Processor unable to retrieve tasks", tag.Error(err))
			t.upsertPollTime(level, time.Time{}) // re-enqueue the event
			continue
		}

		pollTime := t.shard.GetTimeSource().Now()
		t.upsertPollTime(level, pollTime.Add(backoff.JitDuration(
			t.options.MaxPollInterval(),
			t.options.MaxPollIntervalJitterCoefficient(),
		)))

		tasks := make(map[task.Key]task.Task)
		taskChFull := false
		for _, taskInfo := range transferTaskInfos {
			if !domainFilter.Filter(taskInfo.GetDomainID()) {
				continue
			}

			task := t.taskInitializer(taskInfo)
			tasks[newTransferTaskKey(taskInfo.GetTaskID())] = task
			submitted, err := t.submitTask(task)
			if err != nil {
				// only err here is due to the fact that processor has been shutdown
				// return instead of continue
				return
			}
			taskChFull = taskChFull || !submitted
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
			backoff := time.Duration(0)
			if level != defaultProcessingQueueLevel && taskChFull {
				backoff = nonDefaultQueueBackoffDuration
			}
			t.upsertPollTime(level, pollTime.Add(backoff))
		} else if level != defaultProcessingQueueLevel {
			// no more task to read for now
			// update poll time only for non default queue
			// poll time for default queue will be updated through notifyNewTask
			t.upsertPollTime(level, pollTime.Add(nonDefaultQueueBackoffDuration))
		}
	}
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

	splitPolicy := t.initializeSplitPolicy(
		func(key task.Key, domainID string) task.Key {
			totalLookAhead := lookAhead * int64(t.options.SplitLookAheadDurationByDomainID(domainID))
			return newTransferTaskKey(key.(transferTaskKey).taskID + totalLookAhead)
		},
	)

	t.splitProcessingQueueCollection(splitPolicy, t.upsertPollTime)
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
		RedispatchInterval:                  config.TaskRedispatchInterval,
		RedispatchIntervalJitterCoefficient: config.TaskRedispatchIntervalJitterCoefficient,
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
