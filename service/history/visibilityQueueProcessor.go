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
	"sync/atomic"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
)

type (
	updateVisibilityAckLevel func(ackLevel int64) error
	visibilityQueueShutdown  func() error

	visibilityQueueProcessorImpl struct {
		// from transferQueueActiveProcessorImpl (transferQueueProcessorImpl.activeTaskProcessor)
		*queueProcessorBase
		queueAckMgr
		shard                    shard.Context
		options                  *QueueProcessorOptions
		executionManager         persistence.ExecutionManager
		maxReadLevel             maxReadLevel
		updateVisibilityAckLevel updateVisibilityAckLevel
		visibilityQueueShutdown  visibilityQueueShutdown
		visibilityTaskFilter     taskFilter
		ownedScheduler           queues.Scheduler // this is the scheduler owned by this visibility queue processor
		logger                   log.Logger
		metricsClient            metrics.Client

		// from transferQueueProcessorImpl
		config   *configs.Config
		ackLevel int64

		isStarted    int32
		isStopped    int32
		shutdownChan chan struct{}
	}
)

func newVisibilityQueueProcessor(
	shard shard.Context,
	workflowCache workflow.Cache,
	scheduler queues.Scheduler,
	priorityAssigner queues.PriorityAssigner,
	visibilityMgr manager.VisibilityManager,
	metricProvider metrics.MetricsHandler,
	hostRateLimiter quotas.RateLimiter,
) queues.Queue {

	config := shard.GetConfig()
	logger := log.With(shard.GetLogger(), tag.ComponentVisibilityQueue)
	metricsClient := shard.GetMetricsClient()

	options := &QueueProcessorOptions{
		BatchSize:                          config.VisibilityTaskBatchSize,
		MaxPollInterval:                    config.VisibilityProcessorMaxPollInterval,
		MaxPollIntervalJitterCoefficient:   config.VisibilityProcessorMaxPollIntervalJitterCoefficient,
		UpdateAckInterval:                  config.VisibilityProcessorUpdateAckInterval,
		UpdateAckIntervalJitterCoefficient: config.VisibilityProcessorUpdateAckIntervalJitterCoefficient,
		MaxReschdulerSize:                  config.VisibilityProcessorMaxReschedulerSize,
		PollBackoffInterval:                config.VisibilityProcessorPollBackoffInterval,
		MetricScope:                        metrics.VisibilityQueueProcessorScope,
	}
	visibilityTaskFilter := func(taskInfo tasks.Task) bool {
		return true
	}
	maxReadLevel := func() int64 {
		return shard.GetQueueExclusiveHighReadWatermark(
			tasks.CategoryVisibility,
			shard.GetClusterMetadata().GetCurrentClusterName(),
			// the value doesn't actually used for immediate queue,
			// but logically visibility queue only has one processor
			true,
		).TaskID
	}
	updateVisibilityAckLevel := func(ackLevel int64) error {
		return shard.UpdateQueueAckLevel(tasks.CategoryVisibility, tasks.NewImmediateKey(ackLevel))
	}

	visibilityQueueShutdown := func() error {
		return nil
	}

	ackLevel := shard.GetQueueAckLevel(tasks.CategoryVisibility).TaskID
	retProcessor := &visibilityQueueProcessorImpl{
		shard:                    shard,
		options:                  options,
		maxReadLevel:             maxReadLevel,
		updateVisibilityAckLevel: updateVisibilityAckLevel,
		visibilityQueueShutdown:  visibilityQueueShutdown,
		visibilityTaskFilter:     visibilityTaskFilter,
		logger:                   logger,
		metricsClient:            metricsClient,

		config:       config,
		ackLevel:     ackLevel,
		shutdownChan: make(chan struct{}),

		queueAckMgr:        nil, // is set bellow
		queueProcessorBase: nil, // is set bellow
		executionManager:   shard.GetExecutionManager(),
	}

	taskExecutor := newVisibilityQueueTaskExecutor(
		shard,
		workflowCache,
		visibilityMgr,
		logger,
		metricProvider,
	)

	if scheduler == nil {
		scheduler = newVisibilityTaskShardScheduler(shard, logger)
		retProcessor.ownedScheduler = scheduler
	}

	rescheduler := queues.NewRescheduler(
		scheduler,
		shard.GetTimeSource(),
		logger,
		metricProvider.WithTags(metrics.OperationTag(queues.OperationVisibilityQueueProcessor)),
	)

	queueAckMgr := newQueueAckMgr(
		shard,
		options,
		retProcessor,
		ackLevel,
		logger,
		func(t tasks.Task) queues.Executable {
			return queues.NewExecutable(
				queues.DefaultReaderId,
				t,
				visibilityTaskFilter,
				taskExecutor,
				scheduler,
				rescheduler,
				priorityAssigner,
				shard.GetTimeSource(),
				shard.GetNamespaceRegistry(),
				logger,
				metricProvider,
				shard.GetConfig().VisibilityTaskMaxRetryCount,
				shard.GetConfig().NamespaceCacheRefreshInterval,
			)
		},
	)

	queueProcessorBase := newQueueProcessorBase(
		shard.GetClusterMetadata().GetCurrentClusterName(),
		shard,
		options,
		retProcessor,
		queueAckMgr,
		workflowCache,
		scheduler,
		rescheduler,
		newQueueProcessorRateLimiter(
			hostRateLimiter,
			config.VisibilityProcessorMaxPollRPS,
		),
		logger,
	)
	retProcessor.queueAckMgr = queueAckMgr
	retProcessor.queueProcessorBase = queueProcessorBase

	return retProcessor
}

// visibilityQueueProcessor implementation
func (t *visibilityQueueProcessorImpl) Start() {
	if !atomic.CompareAndSwapInt32(&t.isStarted, 0, 1) {
		return
	}
	if t.ownedScheduler != nil {
		t.ownedScheduler.Start()
	}
	t.queueProcessorBase.Start()
	go t.completeTaskLoop()
}

func (t *visibilityQueueProcessorImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&t.isStopped, 0, 1) {
		return
	}
	t.queueProcessorBase.Stop()
	if t.ownedScheduler != nil {
		t.ownedScheduler.Stop()
	}
	close(t.shutdownChan)
}

// NotifyNewTasks - Notify the processor about the new visibility task arrival.
// This should be called each time new visibility task arrives, otherwise tasks maybe delayed.
func (t *visibilityQueueProcessorImpl) NotifyNewTasks(
	_ string,
	visibilityTasks []tasks.Task,
) {
	if len(visibilityTasks) != 0 {
		t.notifyNewTask()
	}
}

func (t *visibilityQueueProcessorImpl) FailoverNamespace(
	namespaceIDs map[string]struct{},
) {
	// no-op
}

func (t *visibilityQueueProcessorImpl) LockTaskProcessing() {
	// no-op
}

func (t *visibilityQueueProcessorImpl) UnlockTaskProcessing() {
	// no-op
}

func (t *visibilityQueueProcessorImpl) Category() tasks.Category {
	return tasks.CategoryVisibility
}

func (t *visibilityQueueProcessorImpl) completeTaskLoop() {
	timer := time.NewTimer(t.config.VisibilityProcessorCompleteTaskInterval())
	defer timer.Stop()

	completeTaskRetryPolicy := common.CreateCompleteTaskRetryPolicy()

	for {
		select {
		case <-t.shutdownChan:
			// before shutdown, make sure the ack level is up to date
			if err := t.completeTask(); err != nil {
				t.logger.Error("Failed to complete visibility task", tag.Error(err))
			}
			return
		case <-timer.C:
			// TODO: We should have a better approach to handle shard and its component lifecycle
			_ = backoff.ThrottleRetry(func() error {
				err := t.completeTask()
				if err != nil {
					t.logger.Info("Failed to complete transfer task", tag.Error(err))
				}
				return err
			}, completeTaskRetryPolicy, func(err error) bool {
				select {
				case <-t.shutdownChan:
					return false
				default:
				}
				return !shard.IsShardOwnershipLostError(err)
			})

			timer.Reset(t.config.VisibilityProcessorCompleteTaskInterval())
		}
	}
}

func (t *visibilityQueueProcessorImpl) completeTask() error {
	lowerAckLevel := t.ackLevel
	upperAckLevel := t.queueAckMgr.getQueueAckLevel()

	t.logger.Debug("Start completing visibility task", tag.AckLevel(lowerAckLevel), tag.AckLevel(upperAckLevel))
	if lowerAckLevel >= upperAckLevel {
		return nil
	}

	t.metricsClient.IncCounter(metrics.VisibilityQueueProcessorScope, metrics.TaskBatchCompleteCounter)

	if lowerAckLevel < upperAckLevel {
		ctx, cancel := newQueueIOContext()
		defer cancel()

		err := t.shard.GetExecutionManager().RangeCompleteHistoryTasks(ctx, &persistence.RangeCompleteHistoryTasksRequest{
			ShardID:             t.shard.GetShardID(),
			TaskCategory:        tasks.CategoryVisibility,
			InclusiveMinTaskKey: tasks.NewImmediateKey(lowerAckLevel + 1),
			ExclusiveMaxTaskKey: tasks.NewImmediateKey(upperAckLevel + 1),
		})
		if err != nil {
			return err
		}
	}

	t.ackLevel = upperAckLevel

	return t.shard.UpdateQueueAckLevel(tasks.CategoryVisibility, tasks.NewImmediateKey(upperAckLevel))
}

// queueProcessor interface
func (t *visibilityQueueProcessorImpl) notifyNewTask() {
	t.queueProcessorBase.notifyNewTask()
}

// processor interfaces
func (t *visibilityQueueProcessorImpl) readTasks(
	readLevel int64,
) ([]tasks.Task, bool, error) {
	ctx, cancel := newQueueIOContext()
	defer cancel()

	response, err := t.executionManager.GetHistoryTasks(ctx, &persistence.GetHistoryTasksRequest{
		ShardID:             t.shard.GetShardID(),
		TaskCategory:        tasks.CategoryVisibility,
		InclusiveMinTaskKey: tasks.NewImmediateKey(readLevel + 1),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(t.maxReadLevel()),
		BatchSize:           t.options.BatchSize(),
	})
	if err != nil {
		return nil, false, err
	}

	return response.Tasks, len(response.NextPageToken) != 0, nil
}

func (t *visibilityQueueProcessorImpl) updateAckLevel(
	ackLevel int64,
) error {
	return t.updateVisibilityAckLevel(ackLevel)
}

func (t *visibilityQueueProcessorImpl) queueShutdown() error {
	return t.visibilityQueueShutdown()
}

func newVisibilityTaskShardScheduler(
	shard shard.Context,
	logger log.Logger,
) queues.Scheduler {
	config := shard.GetConfig()
	return queues.NewFIFOScheduler(
		queues.FIFOSchedulerOptions{
			WorkerCount: config.VisibilityTaskWorkerCount,
			QueueSize:   config.VisibilityTaskBatchSize(),
		},
		logger,
	)
}
