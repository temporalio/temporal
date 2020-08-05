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
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/collection"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/quotas"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"github.com/uber/cadence/service/history/shard"
	"github.com/uber/cadence/service/history/task"
)

const (
	warnPendingTasks = 2000
)

type (
	updateMaxReadLevelFn    func() task.Key
	updateClusterAckLevelFn func(task.Key) error
	queueShutdownFn         func() error

	queueProcessorOptions struct {
		BatchSize                            dynamicconfig.IntPropertyFn
		MaxPollRPS                           dynamicconfig.IntPropertyFn
		MaxPollInterval                      dynamicconfig.DurationPropertyFn
		MaxPollIntervalJitterCoefficient     dynamicconfig.FloatPropertyFn
		UpdateAckInterval                    dynamicconfig.DurationPropertyFn
		UpdateAckIntervalJitterCoefficient   dynamicconfig.FloatPropertyFn
		RedispatchInterval                   dynamicconfig.DurationPropertyFn
		RedispatchIntervalJitterCoefficient  dynamicconfig.FloatPropertyFn
		MaxRedispatchQueueSize               dynamicconfig.IntPropertyFn
		SplitQueueInterval                   dynamicconfig.DurationPropertyFn
		SplitQueueIntervalJitterCoefficient  dynamicconfig.FloatPropertyFn
		EnableSplit                          dynamicconfig.BoolPropertyFn
		SplitMaxLevel                        dynamicconfig.IntPropertyFn
		EnableRandomSplitByDomainID          dynamicconfig.BoolPropertyFnWithDomainIDFilter
		RandomSplitProbability               dynamicconfig.FloatPropertyFn
		EnablePendingTaskSplit               dynamicconfig.BoolPropertyFn
		PendingTaskSplitThreshold            dynamicconfig.MapPropertyFn
		EnableStuckTaskSplit                 dynamicconfig.BoolPropertyFn
		StuckTaskSplitThreshold              dynamicconfig.MapPropertyFn
		SplitLookAheadDurationByDomainID     dynamicconfig.DurationPropertyFnWithDomainIDFilter
		PollBackoffInterval                  dynamicconfig.DurationPropertyFn
		PollBackoffIntervalJitterCoefficient dynamicconfig.FloatPropertyFn
		MetricScope                          int
	}

	actionNotification struct {
		action               *Action
		resultNotificationCh chan actionResultNotification
	}

	actionResultNotification struct {
		result *ActionResult
		err    error
	}

	processorBase struct {
		shard         shard.Context
		taskProcessor task.Processor
		redispatcher  task.Redispatcher

		options               *queueProcessorOptions
		updateMaxReadLevel    updateMaxReadLevelFn
		updateClusterAckLevel updateClusterAckLevelFn
		queueShutdown         queueShutdownFn

		logger        log.Logger
		metricsClient metrics.Client
		metricsScope  metrics.Scope

		rateLimiter quotas.Limiter

		status         int32
		shutdownWG     sync.WaitGroup
		shutdownCh     chan struct{}
		actionNotifyCh chan actionNotification

		queueCollectionsLock       sync.RWMutex
		processingQueueCollections []ProcessingQueueCollection
	}
)

func newProcessorBase(
	shard shard.Context,
	processingQueueStates []ProcessingQueueState,
	taskProcessor task.Processor,
	options *queueProcessorOptions,
	updateMaxReadLevel updateMaxReadLevelFn,
	updateClusterAckLevel updateClusterAckLevelFn,
	queueShutdown queueShutdownFn,
	logger log.Logger,
	metricsClient metrics.Client,
) *processorBase {
	metricsScope := metricsClient.Scope(options.MetricScope)
	return &processorBase{
		shard:         shard,
		taskProcessor: taskProcessor,
		redispatcher: task.NewRedispatcher(
			taskProcessor,
			&task.RedispatcherOptions{
				TaskRedispatchInterval:                  options.RedispatchInterval,
				TaskRedispatchIntervalJitterCoefficient: options.RedispatchIntervalJitterCoefficient,
			},
			logger,
			metricsScope,
		),

		options:               options,
		updateMaxReadLevel:    updateMaxReadLevel,
		updateClusterAckLevel: updateClusterAckLevel,
		queueShutdown:         queueShutdown,

		logger:        logger,
		metricsClient: metricsClient,
		metricsScope:  metricsScope,

		rateLimiter: quotas.NewDynamicRateLimiter(
			func() float64 {
				return float64(options.MaxPollRPS())
			},
		),

		status:         common.DaemonStatusInitialized,
		shutdownCh:     make(chan struct{}),
		actionNotifyCh: make(chan actionNotification),

		processingQueueCollections: newProcessingQueueCollections(
			processingQueueStates,
			logger,
			metricsClient,
		),
	}
}

func (p *processorBase) updateAckLevel() (bool, error) {
	// TODO: only for now, find the min ack level across all processing queues
	// and update DB with that value.
	// Once persistence layer is updated, we need to persist all queue states
	// instead of only the min ack level
	p.metricsScope.IncCounter(metrics.AckLevelUpdateCounter)
	p.queueCollectionsLock.Lock()
	var minAckLevel task.Key
	totalPengingTasks := 0
	for _, queueCollection := range p.processingQueueCollections {
		ackLevel, numPendingTasks := queueCollection.UpdateAckLevels()
		if ackLevel == nil {
			// ack level may be nil if the queueCollection doesn't contain any processing queue
			// after updating ack levels
			continue
		}

		totalPengingTasks += numPendingTasks
		if minAckLevel == nil {
			minAckLevel = ackLevel
		} else {
			minAckLevel = minTaskKey(minAckLevel, ackLevel)
		}
	}
	p.queueCollectionsLock.Unlock()

	if minAckLevel == nil {
		// note that only failover processor will meet this condition
		err := p.queueShutdown()
		if err != nil {
			p.logger.Error("Error shutdown queue", tag.Error(err))
			// return error so that shutdown callback can be retried
			return false, err
		}
		return true, nil
	}

	if totalPengingTasks > warnPendingTasks {
		p.logger.Warn("Too many pending tasks.")
	}
	// TODO: consider move pendingTasksTime metrics from shardInfoScope to queue processor scope
	p.metricsClient.RecordTimer(metrics.ShardInfoScope, getPendingTasksMetricIdx(p.options.MetricScope), time.Duration(totalPengingTasks))

	if err := p.updateClusterAckLevel(minAckLevel); err != nil {
		p.logger.Error("Error updating ack level for shard", tag.Error(err), tag.OperationFailed)
		p.metricsScope.IncCounter(metrics.AckLevelUpdateFailedCounter)
		return false, err
	}

	return false, nil
}

func (p *processorBase) initializeSplitPolicy(
	lookAheadFunc lookAheadFunc,
) ProcessingQueueSplitPolicy {
	if !p.options.EnableSplit() {
		return nil
	}

	// note the order of policies matters, check the comment for aggregated split policy
	var policies []ProcessingQueueSplitPolicy
	maxNewQueueLevel := p.options.SplitMaxLevel()

	if p.options.EnablePendingTaskSplit() {
		thresholds, err := common.ConvertDynamicConfigMapPropertyToIntMap(p.options.PendingTaskSplitThreshold())
		if err != nil {
			p.logger.Error("Failed to convert pending task threshold", tag.Error(err))
		} else {
			policies = append(policies, NewPendingTaskSplitPolicy(thresholds, lookAheadFunc, maxNewQueueLevel, p.logger, p.metricsScope))
		}
	}

	if p.options.EnableStuckTaskSplit() {
		thresholds, err := common.ConvertDynamicConfigMapPropertyToIntMap(p.options.StuckTaskSplitThreshold())
		if err != nil {
			p.logger.Error("Failed to convert stuck task threshold", tag.Error(err))
		} else {
			policies = append(policies, NewStuckTaskSplitPolicy(thresholds, maxNewQueueLevel, p.logger, p.metricsScope))
		}
	}

	randomSplitProbability := p.options.RandomSplitProbability()
	if randomSplitProbability != float64(0) {
		policies = append(policies, NewRandomSplitPolicy(
			randomSplitProbability,
			p.options.EnableRandomSplitByDomainID,
			maxNewQueueLevel,
			lookAheadFunc,
			p.logger,
			p.metricsScope,
		))
	}

	if len(policies) == 0 {
		return nil
	}

	return NewAggregatedSplitPolicy(policies...)
}

func (p *processorBase) splitProcessingQueueCollection(
	splitPolicy ProcessingQueueSplitPolicy,
	upsertPollTimeFn func(int, time.Time),
) {
	if splitPolicy == nil {
		return
	}

	p.queueCollectionsLock.Lock()
	defer p.queueCollectionsLock.Unlock()

	newQueuesMap := make(map[int][]ProcessingQueue)
	for _, queueCollection := range p.processingQueueCollections {
		newQueues := queueCollection.Split(splitPolicy)
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
		p.processingQueueCollections = append(p.processingQueueCollections, NewProcessingQueueCollection(
			level,
			newQueues,
		))
	}

	sort.Slice(p.processingQueueCollections, func(i, j int) bool {
		return p.processingQueueCollections[i].Level() < p.processingQueueCollections[j].Level()
	})

	// there can be new queue collections created or new queues added to an existing collection
	for _, queueCollections := range p.processingQueueCollections {
		upsertPollTimeFn(queueCollections.Level(), time.Time{})
	}
}

func (p *processorBase) addAction(action *Action) (chan actionResultNotification, bool) {
	resultNotificationCh := make(chan actionResultNotification, 1)
	select {
	case p.actionNotifyCh <- actionNotification{
		action:               action,
		resultNotificationCh: resultNotificationCh,
	}:
		return resultNotificationCh, true
	case <-p.shutdownCh:
		close(resultNotificationCh)
		return nil, false
	}
}

func (p *processorBase) handleActionNotification(
	notification actionNotification,
	postActionFn func(),
) {
	var result *ActionResult
	var err error
	switch notification.action.ActionType {
	case ActionTypeReset:
		result, err = p.resetProcessingQueueStates()
	default:
		err = fmt.Errorf("unknown queue action type: %v", notification.action.ActionType)
	}

	notification.resultNotificationCh <- actionResultNotification{
		result: result,
		err:    err,
	}

	close(notification.resultNotificationCh)

	if err == nil {
		// only run post action when the action complete successfully
		postActionFn()
	}
}

func (p *processorBase) resetProcessingQueueStates() (*ActionResult, error) {
	p.queueCollectionsLock.Lock()
	defer p.queueCollectionsLock.Unlock()

	var minAckLevel task.Key
	for _, queueCollection := range p.processingQueueCollections {
		ackLevel, _ := queueCollection.UpdateAckLevels()
		if ackLevel == nil {
			// ack level may be nil if the queueCollection doesn't contain any processing queue
			// after updating ack levels
			continue
		}

		if minAckLevel == nil {
			minAckLevel = ackLevel
		} else {
			minAckLevel = minTaskKey(minAckLevel, ackLevel)
		}
	}

	if minAckLevel == nil {
		// reset queue can't be invoked for failover queue, so if this happens, there's must be a
		// bug in the queue split implementation
		p.logger.Fatal("unable to find minAckLevel during reset", tag.Value(p.processingQueueCollections))
	}

	var maxReadLevel task.Key
	switch p.options.MetricScope {
	case metrics.TransferActiveQueueProcessorScope, metrics.TransferStandbyQueueProcessorScope:
		maxReadLevel = maxTransferReadLevel
	case metrics.TimerActiveQueueProcessorScope, metrics.TimerStandbyQueueProcessorScope:
		maxReadLevel = maximumTimerTaskKey
	}

	p.processingQueueCollections = newProcessingQueueCollections(
		[]ProcessingQueueState{
			NewProcessingQueueState(
				defaultProcessingQueueLevel,
				minAckLevel,
				maxReadLevel,
				NewDomainFilter(nil, false),
			),
		},
		p.logger,
		p.metricsClient,
	)

	return &ActionResult{
		ActionType:        ActionTypeReset,
		ResetActionResult: &ResetActionResult{},
	}, nil
}

func (p *processorBase) getProcessingQueueStates() []ProcessingQueueState {
	p.queueCollectionsLock.RLock()
	defer p.queueCollectionsLock.RUnlock()

	var queueStates []ProcessingQueueState
	for _, queueCollection := range p.processingQueueCollections {
		for _, queue := range queueCollection.Queues() {
			queueStates = append(queueStates, copyQueueState(queue.State()))
		}
	}

	return queueStates
}

func (p *processorBase) submitTask(
	task task.Task,
) (bool, error) {
	submitted, err := p.taskProcessor.TrySubmit(task)
	if err != nil {
		select {
		case <-p.shutdownCh:
			// if error is due to shard shutdown
			return false, err
		default:
			// otherwise it might be error from domain cache etc, add
			// the task to redispatch queue so that it can be retried
			p.logger.Error("Failed to submit task", tag.Error(err))
		}
	}
	if err != nil || !submitted {
		p.redispatcher.AddTask(task)
		return false, nil
	}

	return true, nil
}

func newProcessingQueueCollections(
	processingQueueStates []ProcessingQueueState,
	logger log.Logger,
	metricsClient metrics.Client,
) []ProcessingQueueCollection {
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

	return processingQueueCollections
}

// RedispatchTasks should be un-exported after the queue processing logic
// in history package is deprecated.
func RedispatchTasks(
	redispatchQueue collection.Queue,
	taskProcessor task.Processor,
	logger log.Logger,
	metricsScope metrics.Scope,
	shutdownCh <-chan struct{},
) {
	queueLength := redispatchQueue.Len()
	metricsScope.RecordTimer(metrics.TaskRedispatchQueuePendingTasksTimer, time.Duration(queueLength))
	for i := 0; i != queueLength; i++ {
		element := redispatchQueue.Remove()
		if element == nil {
			// queue is empty, may due to concurrent redispatch on the same queue
			return
		}
		queueTask := element.(task.Task)
		submitted, err := taskProcessor.TrySubmit(queueTask)
		if err != nil {
			select {
			case <-shutdownCh:
				// if error is due to shard shutdown
				return
			default:
				// otherwise it might be error from domain cache etc, add
				// the task to redispatch queue so that it can be retried
				logger.Error("failed to redispatch task", tag.Error(err))
			}
		}

		if err != nil || !submitted {
			// failed to submit, enqueue again
			redispatchQueue.Add(queueTask)
		}
	}
}

func getPendingTasksMetricIdx(
	scopeIdx int,
) int {
	switch scopeIdx {
	case metrics.TimerActiveQueueProcessorScope:
		return metrics.ShardInfoTimerActivePendingTasksTimer
	case metrics.TimerStandbyQueueProcessorScope:
		return metrics.ShardInfoTimerStandbyPendingTasksTimer
	case metrics.TransferActiveQueueProcessorScope:
		return metrics.ShardInfoTransferActivePendingTasksTimer
	case metrics.TransferStandbyQueueProcessorScope:
		return metrics.ShardInfoTransferStandbyPendingTasksTimer
	case metrics.ReplicatorQueueProcessorScope:
		return metrics.ShardInfoReplicationPendingTasksTimer
	default:
		panic("unknown queue processor metric scope")
	}
}
