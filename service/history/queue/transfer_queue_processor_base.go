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
	"sort"
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
	"github.com/uber/cadence/service/history/shard"
	"github.com/uber/cadence/service/history/task"
)

var (
	loadQueueTaskThrottleRetryDelay = 5 * time.Second

	persistenceOperationRetryPolicy = common.CreatePersistanceRetryPolicy()
)

type (
	maxReadLevel           func() task.Key
	updateTransferAckLevel func(ackLevel int64) error
	transferQueueShutdown  func() error

	transferTaskKey struct {
		taskID int64
	}

	queueProcessorOptions struct {
		BatchSize                           dynamicconfig.IntPropertyFn
		MaxPollRPS                          dynamicconfig.IntPropertyFn
		MaxPollInterval                     dynamicconfig.DurationPropertyFn
		MaxPollIntervalJitterCoefficient    dynamicconfig.FloatPropertyFn
		UpdateAckInterval                   dynamicconfig.DurationPropertyFn
		UpdateAckIntervalJitterCoefficient  dynamicconfig.FloatPropertyFn
		SplitQueueInterval                  dynamicconfig.DurationPropertyFn
		SplitQueueIntervalJitterCoefficient dynamicconfig.FloatPropertyFn
		QueueSplitPolicy                    ProcessingQueueSplitPolicy
		RedispatchInterval                  dynamicconfig.DurationPropertyFn
		RedispatchIntervalJitterCoefficient dynamicconfig.FloatPropertyFn
		MaxRedispatchQueueSize              dynamicconfig.IntPropertyFn
		MetricScope                         int
	}

	transferQueueProcessorBase struct {
		shard           shard.Context
		taskProcessor   task.Processor
		redispatchQueue collection.Queue

		options                *queueProcessorOptions
		maxReadLevel           maxReadLevel
		updateTransferAckLevel updateTransferAckLevel
		transferQueueShutdown  transferQueueShutdown
		taskInitializer        task.Initializer

		logger        log.Logger
		metricsClient metrics.Client
		metricsScope  metrics.Scope

		rateLimiter  quotas.Limiter
		lastPollTime time.Time
		notifyCh     chan struct{}
		status       int32
		shutdownWG   sync.WaitGroup
		shutdownCh   chan struct{}

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
	maxReadLevel maxReadLevel,
	updateTransferAckLevel updateTransferAckLevel,
	transferQueueShutdown transferQueueShutdown,
	taskInitializer task.Initializer,
	logger log.Logger,
	metricsClient metrics.Client,
) *transferQueueProcessorBase {
	processingQueuesMap := make(map[int][]ProcessingQueue) // level -> state
	for _, queueState := range processingQueueStates {
		processingQueuesMap[queueState.Level()] = append(processingQueuesMap[queueState.Level()], NewProcessingQueue(
			queueState,
			logger,
			metricsClient,
		))
	}
	processingQueueCollections := make([]ProcessingQueueCollection, 0, len(processingQueuesMap))
	for level, queues := range processingQueuesMap {
		processingQueueCollections = append(processingQueueCollections, NewProcessingQueueCollection(
			level,
			queues,
		))
	}
	sort.Slice(processingQueueCollections, func(i, j int) bool {
		return processingQueueCollections[i].Level() < processingQueueCollections[j].Level()
	})

	return &transferQueueProcessorBase{
		shard:                      shard,
		taskProcessor:              taskProcessor,
		redispatchQueue:            redispatchQueue,
		processingQueueCollections: processingQueueCollections,
		options:                    options,
		maxReadLevel:               maxReadLevel,
		updateTransferAckLevel:     updateTransferAckLevel,
		transferQueueShutdown:      transferQueueShutdown,
		taskInitializer:            taskInitializer,

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
	}
}

func (t *transferQueueProcessorBase) Start() {
	if !atomic.CompareAndSwapInt32(&t.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	t.logger.Info("", tag.LifeCycleStarting)
	defer t.logger.Info("", tag.LifeCycleStarted)

	t.shutdownWG.Add(1)
	t.notifyNewTask()
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
			pollTimer.Reset(backoff.JitDuration(
				t.options.MaxPollInterval(),
				t.options.MaxPollIntervalJitterCoefficient(),
			))
			if t.lastPollTime.Add(t.options.MaxPollInterval()).Before(t.shard.GetTimeSource().Now()) {
				t.processBatch()
			}
		case <-updateAckTimer.C:
			updateAckTimer.Reset(backoff.JitDuration(
				t.options.UpdateAckInterval(),
				t.options.UpdateAckIntervalJitterCoefficient(),
			))
			processFinished, err := t.updateAckLevel()
			if err == shard.ErrShardClosed || (err == nil && processFinished) {
				go t.Stop()
				break processorPumpLoop
			}
		case <-redispatchTimer.C:
			redispatchTimer.Reset(backoff.JitDuration(
				t.options.RedispatchInterval(),
				t.options.RedispatchIntervalJitterCoefficient(),
			))
			RedispatchTasks(
				t.redispatchQueue,
				t.taskProcessor,
				t.logger,
				t.metricsScope,
				t.shutdownCh,
			)
		case <-splitQueueTimer.C:
			splitQueueTimer.Reset(backoff.JitDuration(
				t.options.SplitQueueInterval(),
				t.options.SplitQueueIntervalJitterCoefficient(),
			))
			t.splitQueue()
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

		shardMaxReadLevel := t.maxReadLevel()
		if shardMaxReadLevel.Less(maxReadLevel) {
			maxReadLevel = shardMaxReadLevel
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
			select {
			case <-t.shutdownCh:
				return
			default:
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
		err := t.transferQueueShutdown()
		if err != nil {
			t.logger.Error("Error shutdown queue", tag.Error(err))
			// return error so that shutdown callback can be retried
			return false, err
		}
		return true, nil
	}

	// TODO: emit metrics for total # of pending tasks

	if err := t.updateTransferAckLevel(minAckLevel.(*transferTaskKey).taskID); err != nil {
		t.logger.Error("Error updating ack level for shard", tag.Error(err), tag.OperationFailed)
		t.metricsScope.IncCounter(metrics.AckLevelUpdateFailedCounter)
		return false, err
	}

	return false, nil
}

func (t *transferQueueProcessorBase) splitQueue() {
	if t.options.QueueSplitPolicy == nil {
		return
	}

	t.queueCollectionsLock.Lock()
	defer t.queueCollectionsLock.Unlock()

	newQueuesMap := make(map[int][]ProcessingQueue)
	for _, queueCollection := range t.processingQueueCollections {
		newQueues := queueCollection.Split(t.options.QueueSplitPolicy)
		for _, newQueue := range newQueues {
			newQueueLevel := newQueue.State().Level()
			newQueuesMap[newQueueLevel] = append(newQueuesMap[newQueueLevel], newQueue)
		}

		if queuesToMerge, ok := newQueuesMap[queueCollection.Level()]; ok {
			queueCollection.Merge(queuesToMerge)
			delete(newQueuesMap, queueCollection.Level())
		}
	}

	for level, newQueues := range newQueuesMap {
		t.processingQueueCollections = append(t.processingQueueCollections, NewProcessingQueueCollection(
			level,
			newQueues,
		))
	}

	sort.Slice(t.processingQueueCollections, func(i, j int) bool {
		return t.processingQueueCollections[i].Level() < t.processingQueueCollections[j].Level()
	})
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
			ReadLevel:    readLevel.(*transferTaskKey).taskID,
			MaxReadLevel: maxReadLevel.(*transferTaskKey).taskID,
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
		return false
	}
	if !submitted {
		t.redispatchQueue.Add(task)
	}

	return true
}

// RedispatchTasks should be un-exported after the queue processing logic
// in history package is deprecated.
func RedispatchTasks(
	redispatchQueue collection.Queue,
	queueTaskProcessor task.Processor,
	logger log.Logger,
	metricsScope metrics.Scope,
	shutdownCh <-chan struct{},
) {
	queueLength := redispatchQueue.Len()
	metricsScope.RecordTimer(metrics.TaskRedispatchQueuePendingTasksTimer, time.Duration(queueLength))
	for i := 0; i != queueLength; i++ {
		queueTask := redispatchQueue.Remove().(task.Task)
		submitted, err := queueTaskProcessor.TrySubmit(queueTask)
		if err != nil {
			// the only reason error will be returned here is because
			// task processor has already shutdown. Just return in this case.
			logger.Error("failed to redispatch task", tag.Error(err))
			return
		}
		if !submitted {
			// failed to submit, enqueue again
			redispatchQueue.Add(queueTask)
		}

		select {
		case <-shutdownCh:
			return
		default:
		}
	}
}

func newTransferTaskKey(
	taskID int64,
) task.Key {
	return &transferTaskKey{
		taskID: taskID,
	}
}

func (k *transferTaskKey) Less(
	key task.Key,
) bool {
	return k.taskID < key.(*transferTaskKey).taskID
}
