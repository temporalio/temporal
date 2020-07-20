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
	"math"
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
	maximumTimerTaskKey = newTimerTaskKey(
		time.Unix(0, math.MaxInt64),
		0,
	)
)

type (
	timerTaskKey struct {
		visibilityTimestamp time.Time
		taskID              int64
	}

	timeTaskReadProgress struct {
		currentQueue  ProcessingQueue
		readLevel     task.Key
		maxReadLevel  task.Key
		nextPageToken []byte
	}

	timerQueueProcessorBase struct {
		clusterName     string
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

		rateLimiter         quotas.Limiter
		pollTimeLock        sync.Mutex
		pollTimeUpdateTimer map[int]*time.Timer
		nextPollTime        map[int]time.Time
		timerGate           TimerGate

		status     int32
		shutdownWG sync.WaitGroup
		shutdownCh chan struct{}

		// timer notification
		newTimerCh  chan struct{}
		newTimeLock sync.Mutex
		newTime     time.Time

		queueCollectionsLock        sync.RWMutex
		processingQueueCollections  []ProcessingQueueCollection
		processingQueueReadProgress map[int]timeTaskReadProgress
	}
)

func newTimerQueueProcessorBase(
	clusterName string,
	shard shard.Context,
	processingQueueStates []ProcessingQueueState,
	taskProcessor task.Processor,
	redispatchQueue collection.Queue,
	timerGate TimerGate,
	options *queueProcessorOptions,
	updateMaxReadLevel updateMaxReadLevelFn,
	updateClusterAckLevel updateClusterAckLevelFn,
	queueShutdown queueShutdownFn,
	taskInitializer task.Initializer,
	logger log.Logger,
	metricsClient metrics.Client,
) *timerQueueProcessorBase {
	return &timerQueueProcessorBase{
		clusterName:     clusterName,
		shard:           shard,
		taskProcessor:   taskProcessor,
		redispatchQueue: redispatchQueue,

		options:               options,
		updateMaxReadLevel:    updateMaxReadLevel,
		updateClusterAckLevel: updateClusterAckLevel,
		queueShutdown:         queueShutdown,
		taskInitializer:       taskInitializer,

		logger:        logger.WithTags(tag.ComponentTimerQueue),
		metricsClient: metricsClient,
		metricsScope:  metricsClient.Scope(options.MetricScope),

		rateLimiter: quotas.NewDynamicRateLimiter(
			func() float64 {
				return float64(options.MaxPollRPS())
			},
		),
		pollTimeUpdateTimer: make(map[int]*time.Timer),
		nextPollTime:        make(map[int]time.Time),
		timerGate:           timerGate,

		status:     common.DaemonStatusInitialized,
		shutdownCh: make(chan struct{}),
		newTimerCh: make(chan struct{}, 1),

		processingQueueCollections: newProcessingQueueCollections(
			processingQueueStates,
			logger,
			metricsClient,
		),
		processingQueueReadProgress: make(map[int]timeTaskReadProgress),
	}
}

func (t *timerQueueProcessorBase) Start() {
	if !atomic.CompareAndSwapInt32(&t.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	t.logger.Info("", tag.LifeCycleStarting)
	defer t.logger.Info("", tag.LifeCycleStarted)

	t.notifyNewTimer(time.Time{})

	t.shutdownWG.Add(1)
	go t.processorPump()
}

func (t *timerQueueProcessorBase) Stop() {
	if !atomic.CompareAndSwapInt32(&t.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	t.logger.Info("", tag.LifeCycleStopping)
	defer t.logger.Info("", tag.LifeCycleStopped)

	t.timerGate.Close()
	close(t.shutdownCh)
	t.pollTimeLock.Lock()
	for _, timer := range t.pollTimeUpdateTimer {
		timer.Stop()
	}
	t.pollTimeLock.Unlock()

	if success := common.AwaitWaitGroup(&t.shutdownWG, time.Minute); !success {
		t.logger.Warn("", tag.LifeCycleStopTimedout)
	}
}

func (t *timerQueueProcessorBase) processorPump() {
	defer t.shutdownWG.Done()

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
		case <-t.timerGate.FireChan():
			if t.redispatchQueue.Len() > t.options.MaxRedispatchQueueSize() {
				RedispatchTasks(
					t.redispatchQueue,
					t.taskProcessor,
					t.logger,
					t.metricsScope,
					t.shutdownCh,
				)
				t.timerGate.Update(time.Time{})
				continue processorPumpLoop
			}

			t.pollTimeLock.Lock()
			levels := make(map[int]struct{})
			now := t.shard.GetCurrentTime(t.clusterName)
			for level, pollTime := range t.nextPollTime {
				if !now.Before(pollTime) {
					levels[level] = struct{}{}
					delete(t.nextPollTime, level)
				} else {
					t.timerGate.Update(pollTime)
				}
			}
			t.pollTimeLock.Unlock()

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
		case <-t.newTimerCh:
			t.newTimeLock.Lock()
			newTime := t.newTime
			t.newTime = time.Time{}
			t.newTimeLock.Unlock()

			// New Timer has arrived.
			t.metricsScope.IncCounter(metrics.NewTimerNotifyCounter)
			t.upsertPollTime(defaultProcessingQueueLevel, newTime)
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

func (t *timerQueueProcessorBase) processQueueCollections(levels map[int]struct{}) {
	t.queueCollectionsLock.RLock()
	queueCollections := t.processingQueueCollections
	t.queueCollectionsLock.RUnlock()

	for _, queueCollection := range queueCollections {
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

		var nextPageToken []byte
		readLevel := activeQueue.State().ReadLevel()
		maxReadLevel := minTaskKey(activeQueue.State().MaxLevel(), t.updateMaxReadLevel())
		lookAheadMaxLevel := activeQueue.State().MaxLevel()
		domainFilter := activeQueue.State().DomainFilter()
		t.queueCollectionsLock.RUnlock()

		if progress, ok := t.processingQueueReadProgress[level]; ok {
			if progress.currentQueue == activeQueue {
				readLevel = progress.readLevel
				maxReadLevel = progress.maxReadLevel
				nextPageToken = progress.nextPageToken
			}
			delete(t.processingQueueReadProgress, level)
		}

		if !readLevel.Less(maxReadLevel) {
			// notify timer gate about the min time
			t.upsertPollTime(level, readLevel.(timerTaskKey).visibilityTimestamp)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), loadQueueTaskThrottleRetryDelay)
		if err := t.rateLimiter.Wait(ctx); err != nil {
			cancel()
			if level == defaultProcessingQueueLevel {
				t.upsertPollTime(level, time.Time{})
			} else {
				t.upsertPollTimeUpdateTimer(level, nonDefaultQueueBackoffDuration)
			}
			continue
		}
		cancel()

		timerTaskInfos, lookAheadTask, nextPageToken, err := t.readAndFilterTasks(readLevel, maxReadLevel, nextPageToken, lookAheadMaxLevel)
		if err != nil {
			t.logger.Error("Processor unable to retrieve tasks", tag.Error(err))
			t.upsertPollTime(level, time.Time{}) // re-enqueue the event
			continue
		}

		// set up update timer here instead of modifying pollTime directly so that the interval is w.r.t
		// real time.
		t.upsertPollTimeUpdateTimer(level, backoff.JitDuration(
			t.options.MaxPollInterval(),
			t.options.MaxPollIntervalJitterCoefficient(),
		))

		tasks := make(map[task.Key]task.Task)
		taskChFull := false
		for _, taskInfo := range timerTaskInfos {
			if !domainFilter.Filter(taskInfo.GetDomainID()) {
				continue
			}

			task := t.taskInitializer(taskInfo)
			tasks[newTimerTaskKey(taskInfo.GetVisibilityTimestamp(), taskInfo.GetTaskID())] = task
			submitted, err := t.submitTask(task)
			if err != nil {
				// only err here is due to the fact that processor has been shutdown
				// return instead of continue
				return
			}
			taskChFull = taskChFull || !submitted
		}

		var newReadLevel task.Key
		if len(nextPageToken) == 0 {
			if lookAheadTask != nil {
				// lookAheadTask may exist only when nextPageToken is empty
				t.upsertPollTime(level, lookAheadTask.VisibilityTimestamp)
				newReadLevel = minTaskKey(maxReadLevel, newTimerTaskKey(lookAheadTask.GetVisibilityTimestamp(), 0))
			} else {
				// we have no idea when the next poll should happen
				if taskKeyEquals(maxReadLevel, lookAheadMaxLevel) {
					// this means processing queue's max level is less than shard max read level, so
					// no more tasks will be generated for this processing queue, enqueue an event
					// to process the next queue in the queue collection if exists.
					t.upsertPollTime(level, time.Time{})
				} else if level != defaultProcessingQueueLevel {
					// new tasks can be still generated for this processing queue's and we need to check later.
					// note that we only need to update the poll time for non-default queue. Poll time
					// for default queue will be updated when notifyNewTask are called.
					// since we are waiting for new tasks, we should use the cluster's time instead of real time. So
					// we don't need to create an update timer here.
					t.upsertPollTime(level, t.shard.GetCurrentTime(t.clusterName).Add(nonDefaultQueueBackoffDuration))
				}
				newReadLevel = maxReadLevel
			}
		} else {
			// more tasks should be loaded for this processing queue
			// record the current progress and update the poll time
			if level == defaultProcessingQueueLevel || !taskChFull {
				t.upsertPollTime(level, time.Time{})
			} else {
				t.upsertPollTimeUpdateTimer(level, nonDefaultQueueBackoffDuration)
			}
			t.processingQueueReadProgress[level] = timeTaskReadProgress{
				currentQueue:  activeQueue,
				readLevel:     readLevel,
				maxReadLevel:  maxReadLevel,
				nextPageToken: nextPageToken,
			}
			newReadLevel = newTimerTaskKey(timerTaskInfos[len(timerTaskInfos)-1].GetVisibilityTimestamp(), 0)
		}
		t.queueCollectionsLock.Lock()
		queueCollection.AddTasks(tasks, newReadLevel)
		t.queueCollectionsLock.Unlock()
	}
}

func (t *timerQueueProcessorBase) updateAckLevel() (bool, error) {
	t.metricsScope.IncCounter(metrics.AckLevelUpdateCounter)
	t.queueCollectionsLock.Lock()

	// TODO: only for now, find the min ack level across all processing queues
	// and update DB with that value.
	// Once persistence layer is updated, we need to persist all queue states
	// instead of only the min ack level
	var minAckLevel task.Key
	totalPengingTasks := 0
	for _, queueCollection := range t.processingQueueCollections {
		ackLevel, numPendingTasks := queueCollection.UpdateAckLevels()

		totalPengingTasks += numPendingTasks
		if minAckLevel == nil {
			minAckLevel = ackLevel
		} else {
			minAckLevel = minTaskKey(minAckLevel, ackLevel)
		}
	}
	t.queueCollectionsLock.Unlock()

	if minAckLevel == nil {
		if err := t.queueShutdown(); err != nil {
			t.logger.Error("Error shutdown queue", tag.Error(err))
			return false, err
		}
		return true, nil
	}

	if totalPengingTasks > warnPendingTasks {
		t.logger.Warn("Too many pending tasks.")
	}
	// TODO: consider move pendingTasksTime metrics from shardInfoScope to queue processor scope
	t.metricsClient.RecordTimer(metrics.ShardInfoScope, getPendingTasksMetricIdx(t.options.MetricScope), time.Duration(totalPengingTasks))

	if err := t.updateClusterAckLevel(minAckLevel); err != nil {
		t.logger.Error("Error updating ack level for shard", tag.Error(err), tag.OperationFailed)
		t.metricsScope.IncCounter(metrics.AckLevelUpdateFailedCounter)
		return false, err
	}

	return false, nil
}

func (t *timerQueueProcessorBase) splitQueue() {
	splitPolicy := initializeSplitPolicy(
		t.options,
		func(key task.Key, domainID string) task.Key {
			return newTimerTaskKey(
				key.(timerTaskKey).visibilityTimestamp.Add(
					t.options.SplitLookAheadDurationByDomainID(domainID),
				),
				0,
			)
		},
		t.logger,
		t.metricsScope,
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

	// there can be new queue collections created or new queues added to an existing collection
	for _, queueCollections := range t.processingQueueCollections {
		t.upsertPollTime(queueCollections.Level(), time.Time{})
	}
}

func (t *timerQueueProcessorBase) readAndFilterTasks(
	readLevel task.Key,
	maxReadLevel task.Key,
	nextPageToken []byte,
	lookAheadMaxLevel task.Key,
) ([]*persistence.TimerTaskInfo, *persistence.TimerTaskInfo, []byte, error) {
	timerTasks, nextPageToken, err := t.getTimerTasks(readLevel, maxReadLevel, nextPageToken, t.options.BatchSize())
	if err != nil {
		return nil, nil, nil, err
	}

	var lookAheadTask *persistence.TimerTaskInfo
	filteredTasks := []*persistence.TimerTaskInfo{}

	for _, timerTask := range timerTasks {
		if !t.isProcessNow(timerTask.GetVisibilityTimestamp()) {
			lookAheadTask = timerTask
			nextPageToken = nil
			break
		}
		filteredTasks = append(filteredTasks, timerTask)
	}

	if len(nextPageToken) == 0 && lookAheadTask == nil && maxReadLevel.Less(lookAheadMaxLevel) {
		// only look ahead within the processing queue boundary
		lookAheadTask, err = t.readLookAheadTask(maxReadLevel, lookAheadMaxLevel)
		if err != nil {
			return filteredTasks, nil, nil, nil
		}
	}

	return filteredTasks, lookAheadTask, nextPageToken, nil
}

func (t *timerQueueProcessorBase) readLookAheadTask(
	lookAheadStartLevel task.Key,
	lookAheadMaxLevel task.Key,
) (*persistence.TimerTaskInfo, error) {
	tasks, _, err := t.getTimerTasks(
		lookAheadStartLevel,
		lookAheadMaxLevel,
		nil,
		1,
	)
	if err != nil {
		return nil, err
	}

	if len(tasks) == 1 {
		return tasks[0], nil
	}
	return nil, nil
}

func (t *timerQueueProcessorBase) getTimerTasks(
	readLevel task.Key,
	maxReadLevel task.Key,
	nextPageToken []byte,
	batchSize int,
) ([]*persistence.TimerTaskInfo, []byte, error) {
	request := &persistence.GetTimerIndexTasksRequest{
		MinTimestamp:  readLevel.(timerTaskKey).visibilityTimestamp,
		MaxTimestamp:  maxReadLevel.(timerTaskKey).visibilityTimestamp,
		BatchSize:     batchSize,
		NextPageToken: nextPageToken,
	}

	var err error
	var response *persistence.GetTimerIndexTasksResponse
	retryCount := t.shard.GetConfig().TimerProcessorGetFailureRetryCount()
	for attempt := 0; attempt < retryCount; attempt++ {
		response, err = t.shard.GetExecutionManager().GetTimerIndexTasks(request)
		if err == nil {
			return response.Timers, response.NextPageToken, nil
		}
		backoff := time.Duration(attempt * 100)
		time.Sleep(backoff * time.Millisecond)
	}
	return nil, nil, err
}

func (t *timerQueueProcessorBase) isProcessNow(
	expiryTime time.Time,
) bool {
	if expiryTime.IsZero() {
		// return true, but somewhere probably have bug creating empty timerTask.
		t.logger.Warn("Timer task has timestamp zero")
	}
	return expiryTime.UnixNano() <= t.shard.GetCurrentTime(t.clusterName).UnixNano()
}

func (t *timerQueueProcessorBase) notifyNewTimers(
	timerTasks []persistence.Task,
) {
	if len(timerTasks) == 0 {
		return
	}

	isActive := t.options.MetricScope == metrics.TimerActiveQueueProcessorScope

	minNewTime := timerTasks[0].GetVisibilityTimestamp()
	for _, timerTask := range timerTasks {
		ts := timerTask.GetVisibilityTimestamp()
		if ts.Before(minNewTime) {
			minNewTime = ts
		}

		taskScopeIdx := task.GetTimerTaskMetricScope(
			timerTask.GetType(),
			isActive,
		)
		t.metricsClient.IncCounter(taskScopeIdx, metrics.NewTimerCounter)
	}

	t.notifyNewTimer(minNewTime)
}

func (t *timerQueueProcessorBase) notifyNewTimer(
	newTime time.Time,
) {
	t.newTimeLock.Lock()
	defer t.newTimeLock.Unlock()

	if t.newTime.IsZero() || newTime.Before(t.newTime) {
		t.newTime = newTime
		select {
		case t.newTimerCh <- struct{}{}:
			// Notified about new time.
		default:
			// Channel "full" -> drop and move on, this will happen only if service is in high load.
		}
	}
}

func (t *timerQueueProcessorBase) upsertPollTime(level int, newPollTime time.Time) {
	t.pollTimeLock.Lock()
	defer t.pollTimeLock.Unlock()

	if currentPollTime, ok := t.nextPollTime[level]; !ok || newPollTime.Before(currentPollTime) {
		if timer, ok := t.pollTimeUpdateTimer[level]; ok {
			// there's a pending poll for this processing queue collection, cancel it
			timer.Stop()
			delete(t.pollTimeUpdateTimer, level)
		}

		t.nextPollTime[level] = newPollTime
		t.timerGate.Update(newPollTime)
	}
}

// upsertPollTimeUpdateTimer will trigger a poll for the specified processing queue collection
// after a certain period of (real) time. This means for standby timer, even if the cluster time
// has not been updated, the poll will still be triggered when the timer fired. Use this function
// for delaying the load for processing queue. If a poll should be triggered immediately
// use upsertPollTime.
func (t *timerQueueProcessorBase) upsertPollTimeUpdateTimer(level int, delay time.Duration) {
	t.pollTimeLock.Lock()
	defer t.pollTimeLock.Unlock()

	if currentTimer, ok := t.pollTimeUpdateTimer[level]; ok {
		// there's a pending poll for this processing queue collection, cancel it
		currentTimer.Stop()
	}

	t.pollTimeUpdateTimer[level] = time.AfterFunc(delay, func() {
		select {
		case <-t.shutdownCh:
			return
		default:
		}

		t.pollTimeLock.Lock()
		defer t.pollTimeLock.Unlock()

		t.nextPollTime[level] = time.Time{}
		t.timerGate.Update(time.Time{})
		delete(t.pollTimeUpdateTimer, level)
	})
}

func (t *timerQueueProcessorBase) submitTask(
	task task.Task,
) (bool, error) {
	submitted, err := t.taskProcessor.TrySubmit(task)
	if err != nil {
		select {
		case <-t.shutdownCh:
			// if error is due to shard shutdown
			return false, err
		default:
			// otherwise it might be error from domain cache etc, add
			// the task to redispatch queue so that it can be retried
			t.logger.Error("Failed to submit task", tag.Error(err))
		}
	}
	if err != nil || !submitted {
		t.redispatchQueue.Add(task)
		return false, nil
	}

	return true, nil
}

func (t *timerQueueProcessorBase) getProcessingQueueStates() []ProcessingQueueState {
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

func newTimerTaskKey(
	visibilityTimestamp time.Time,
	taskID int64,
) task.Key {
	return timerTaskKey{
		visibilityTimestamp: visibilityTimestamp,
		taskID:              taskID,
	}
}

func (k timerTaskKey) Less(
	key task.Key,
) bool {
	timerKey := key.(timerTaskKey)
	if k.visibilityTimestamp.Equal(timerKey.visibilityTimestamp) {
		return k.taskID < timerKey.taskID
	}
	return k.visibilityTimestamp.Before(timerKey.visibilityTimestamp)
}

func newTimerQueueProcessorOptions(
	config *config.Config,
	isActive bool,
	isFailover bool,
) *queueProcessorOptions {
	options := &queueProcessorOptions{
		BatchSize:                           config.TimerTaskBatchSize,
		MaxPollRPS:                          config.TimerProcessorMaxPollRPS,
		MaxPollInterval:                     config.TimerProcessorMaxPollInterval,
		MaxPollIntervalJitterCoefficient:    config.TimerProcessorMaxPollIntervalJitterCoefficient,
		UpdateAckInterval:                   config.TimerProcessorUpdateAckInterval,
		UpdateAckIntervalJitterCoefficient:  config.TimerProcessorUpdateAckIntervalJitterCoefficient,
		RedispatchInterval:                  config.TimerProcessorRedispatchInterval,
		RedispatchIntervalJitterCoefficient: config.TimerProcessorRedispatchIntervalJitterCoefficient,
		MaxRedispatchQueueSize:              config.TimerProcessorMaxRedispatchQueueSize,
		SplitQueueInterval:                  config.TimerProcessorSplitQueueInterval,
		SplitQueueIntervalJitterCoefficient: config.TimerProcessorSplitQueueIntervalJitterCoefficient,
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
		options.MetricScope = metrics.TimerActiveQueueProcessorScope
	} else {
		options.MetricScope = metrics.TimerStandbyQueueProcessorScope
	}

	return options
}
