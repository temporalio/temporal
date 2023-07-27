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
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

const (
	// archivalQueuePersistenceMaxRPSRatio is the hard-coded ratio of archival queue persistence max RPS to the total
	// persistence max RPS.
	// In this case, the archival queue may not send requests at a rate higher than 15% of the global persistence max
	// RPS.
	archivalQueuePersistenceMaxRPSRatio = 0.15
)

var (
	// ArchivalTaskPriorities is the map of task priority to weight for the archival queue.
	// The archival queue only uses the low task priority, so we only define a weight for that priority.
	ArchivalTaskPriorities = configs.ConvertWeightsToDynamicConfigValue(map[ctasks.Priority]int{
		ctasks.PriorityLow: 10,
	})
)

type (
	// archivalQueueFactory implements QueueFactory for the archival queue.
	archivalQueueFactory struct {
		QueueFactoryBase
		QueueFactoryBaseParams
	}
)

// NewArchivalQueueFactory creates a new QueueFactory to construct archival queues.
func NewArchivalQueueFactory(
	params QueueFactoryBaseParams,
) QueueFactory {
	hostScheduler := newScheduler(params)
	queueFactoryBase := newQueueFactoryBase(params, hostScheduler)
	return &archivalQueueFactory{
		QueueFactoryBaseParams: params,
		QueueFactoryBase:       queueFactoryBase,
	}
}

// newScheduler creates a new task scheduler for tasks on the archival queue.
func newScheduler(params QueueFactoryBaseParams) queues.Scheduler {
	return queues.NewPriorityScheduler(
		queues.PrioritySchedulerOptions{
			WorkerCount:                 params.Config.ArchivalProcessorSchedulerWorkerCount,
			EnableRateLimiter:           params.Config.TaskSchedulerEnableRateLimiter,
			EnableRateLimiterShadowMode: params.Config.TaskSchedulerEnableRateLimiterShadowMode,
			DispatchThrottleDuration:    params.Config.TaskSchedulerThrottleDuration,
			Weight:                      dynamicconfig.GetMapPropertyFn(ArchivalTaskPriorities),
		},
		params.SchedulerRateLimiter,
		params.TimeSource,
		params.Logger,
		params.MetricsHandler.WithTags(metrics.OperationTag(metrics.OperationArchivalQueueProcessorScope)),
	)
}

// newQueueFactoryBase creates a new QueueFactoryBase for the archival queue, which contains common configurations
// like the task scheduler, task priority assigner, and rate limiters.
func newQueueFactoryBase(params QueueFactoryBaseParams, hostScheduler queues.Scheduler) QueueFactoryBase {
	return QueueFactoryBase{
		HostScheduler:        hostScheduler,
		HostPriorityAssigner: queues.NewPriorityAssigner(),
		HostReaderRateLimiter: queues.NewReaderPriorityRateLimiter(
			NewHostRateLimiterRateFn(
				params.Config.ArchivalProcessorMaxPollHostRPS,
				params.Config.PersistenceMaxQPS,
				archivalQueuePersistenceMaxRPSRatio,
			),
			int64(params.Config.QueueMaxReaderCount()),
		),
	}
}

// CreateQueue creates a new archival queue for the given shard.
func (f *archivalQueueFactory) CreateQueue(
	shardCtx shard.Context,
	workflowCache wcache.Cache,
) queues.Queue {
	logger := log.With(shardCtx.GetLogger(), tag.ComponentArchivalQueue)
	executor := f.ExecutorFactory.CreateArchivalExecutor(shardCtx, workflowCache, logger)
	return f.newScheduledQueue(shardCtx, executor, logger)
}

// newScheduledQueue creates a new scheduled queue for the given shard with archival-specific configurations.
func (f *archivalQueueFactory) newScheduledQueue(shardCtx shard.Context, executor queues.Executor, logger log.Logger) queues.Queue {
	metricsHandler := f.MetricsHandler.WithTags(metrics.OperationTag(metrics.OperationArchivalQueueProcessorScope))

	rescheduler := queues.NewRescheduler(
		f.HostScheduler,
		shardCtx.GetTimeSource(),
		logger,
		metricsHandler,
	)

	return queues.NewScheduledQueue(
		shardCtx,
		tasks.CategoryArchival,
		f.HostScheduler,
		rescheduler,
		f.HostPriorityAssigner,
		executor,
		&queues.Options{
			ReaderOptions: queues.ReaderOptions{
				BatchSize:            f.Config.ArchivalTaskBatchSize,
				MaxPendingTasksCount: f.Config.QueuePendingTaskMaxCount,
				PollBackoffInterval:  f.Config.ArchivalProcessorPollBackoffInterval,
			},
			MonitorOptions: queues.MonitorOptions{
				PendingTasksCriticalCount:   f.Config.QueuePendingTaskCriticalCount,
				ReaderStuckCriticalAttempts: f.Config.QueueReaderStuckCriticalAttempts,
				SliceCountCriticalThreshold: f.Config.QueueCriticalSlicesCount,
			},
			MaxPollRPS:                          f.Config.ArchivalProcessorMaxPollRPS,
			MaxPollInterval:                     f.Config.ArchivalProcessorMaxPollInterval,
			MaxPollIntervalJitterCoefficient:    f.Config.ArchivalProcessorMaxPollIntervalJitterCoefficient,
			CheckpointInterval:                  f.Config.ArchivalProcessorUpdateAckInterval,
			CheckpointIntervalJitterCoefficient: f.Config.ArchivalProcessorUpdateAckIntervalJitterCoefficient,
			MaxReaderCount:                      f.Config.QueueMaxReaderCount,
		},
		f.HostReaderRateLimiter,
		logger,
		metricsHandler,
	)
}
