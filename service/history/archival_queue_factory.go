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
	"go.uber.org/fx"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/archival"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
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
	// ArchivalQueueFactoryParams contains the necessary params to create a new archival queue factory.
	ArchivalQueueFactoryParams struct {
		// fx.In allows fx to construct this object without an explicitly defined constructor.
		fx.In

		// QueueFactoryBaseParams contains common params for all queue factories.
		QueueFactoryBaseParams
		// Archiver is the archival client used to archive history events and visibility records.
		Archiver archival.Archiver
		// RelocatableAttributesFetcher is the client used to fetch the memo and search attributes of a workflow.
		RelocatableAttributesFetcher workflow.RelocatableAttributesFetcher
	}

	// archivalQueueFactory implements QueueFactory for the archival queue.
	archivalQueueFactory struct {
		QueueFactoryBase
		ArchivalQueueFactoryParams
	}
)

// NewArchivalQueueFactory creates a new QueueFactory to construct archival queues.
func NewArchivalQueueFactory(
	params ArchivalQueueFactoryParams,
) QueueFactory {
	return &archivalQueueFactory{
		ArchivalQueueFactoryParams: params,
		QueueFactoryBase:           newQueueFactoryBase(params),
	}
}

// newHostScheduler creates a new task scheduler for tasks on the archival queue.
func newHostScheduler(params ArchivalQueueFactoryParams) queues.Scheduler {
	return queues.NewScheduler(
		params.ClusterMetadata.GetCurrentClusterName(),
		queues.SchedulerOptions{
			WorkerCount:             params.Config.ArchivalProcessorSchedulerWorkerCount,
			ActiveNamespaceWeights:  dynamicconfig.GetMapPropertyFnFilteredByNamespace(ArchivalTaskPriorities),
			StandbyNamespaceWeights: dynamicconfig.GetMapPropertyFnFilteredByNamespace(ArchivalTaskPriorities),
		},
		params.NamespaceRegistry,
		params.Logger,
	)
}

// newQueueFactoryBase creates a new QueueFactoryBase for the archival queue, which contains common configurations
// like the task scheduler, task priority assigner, and rate limiters.
func newQueueFactoryBase(params ArchivalQueueFactoryParams) QueueFactoryBase {
	return QueueFactoryBase{
		HostScheduler:        newHostScheduler(params),
		HostPriorityAssigner: queues.NewPriorityAssigner(),
		HostReaderRateLimiter: queues.NewReaderPriorityRateLimiter(
			NewHostRateLimiterRateFn(
				params.Config.ArchivalProcessorMaxPollHostRPS,
				params.Config.PersistenceMaxQPS,
				archivalQueuePersistenceMaxRPSRatio,
			),
			int64(params.Config.ArchivalQueueMaxReaderCount()),
		),
	}
}

// CreateQueue creates a new archival queue for the given shard.
func (f *archivalQueueFactory) CreateQueue(
	shard shard.Context,
	workflowCache wcache.Cache,
) queues.Queue {
	executor := f.newArchivalTaskExecutor(shard, workflowCache)
	if f.ExecutorWrapper != nil {
		executor = f.ExecutorWrapper.Wrap(executor)
	}
	return f.newScheduledQueue(shard, executor)
}

// newArchivalTaskExecutor creates a new archival task executor for the given shard.
func (f *archivalQueueFactory) newArchivalTaskExecutor(shard shard.Context, workflowCache wcache.Cache) queues.Executor {
	return NewArchivalQueueTaskExecutor(
		f.Archiver,
		shard,
		workflowCache,
		f.RelocatableAttributesFetcher,
		f.MetricsHandler,
		log.With(shard.GetLogger(), tag.ComponentArchivalQueue),
	)
}

// newScheduledQueue creates a new scheduled queue for the given shard with archival-specific configurations.
func (f *archivalQueueFactory) newScheduledQueue(shard shard.Context, executor queues.Executor) queues.Queue {
	logger := log.With(shard.GetLogger(), tag.ComponentArchivalQueue)
	metricsHandler := f.MetricsHandler.WithTags(metrics.OperationTag(metrics.OperationArchivalQueueProcessorScope))

	var shardScheduler = f.HostScheduler
	if f.Config.TaskSchedulerEnableRateLimiter() {
		shardScheduler = queues.NewRateLimitedScheduler(
			f.HostScheduler,
			queues.RateLimitedSchedulerOptions{
				EnableShadowMode: f.Config.TaskSchedulerEnableRateLimiterShadowMode,
				StartupDelay:     f.Config.TaskSchedulerRateLimiterStartupDelay,
			},
			f.ClusterMetadata.GetCurrentClusterName(),
			f.NamespaceRegistry,
			f.SchedulerRateLimiter,
			f.TimeSource,
			logger,
			metricsHandler,
		)
	}

	rescheduler := queues.NewRescheduler(
		shardScheduler,
		shard.GetTimeSource(),
		logger,
		metricsHandler,
	)

	factory := queues.NewExecutableFactory(
		executor,
		shardScheduler,
		rescheduler,
		f.HostPriorityAssigner,
		shard.GetTimeSource(),
		shard.GetNamespaceRegistry(),
		shard.GetClusterMetadata(),
		logger,
		metricsHandler,
		f.DLQWriter,
		f.Config.TaskDLQEnabled,
		f.Config.TaskDLQUnexpectedErrorAttempts,
		f.Config.TaskDLQInternalErrors,
		f.Config.TaskDLQErrorPattern,
	)
	return queues.NewScheduledQueue(
		shard,
		tasks.CategoryArchival,
		shardScheduler,
		rescheduler,
		factory,
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
			MaxReaderCount:                      f.Config.ArchivalQueueMaxReaderCount,
		},
		f.HostReaderRateLimiter,
		logger,
		metricsHandler,
	)
}
