// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/quotas"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

const outboundQueuePersistenceMaxRPSRatio = 0.3

type outboundQueueFactoryParams struct {
	fx.In

	QueueFactoryBaseParams
}

// TODO: get actual limits from dynamic config.
type groupLimiter struct {
	key queues.NamespaceIDAndDestination
}

func (groupLimiter) BufferSize() int {
	return 100
}

func (groupLimiter) Concurrency() int {
	return 100
}

var _ ctasks.DynamicWorkerPoolLimiter = groupLimiter{}

type outboundQueueFactory struct {
	outboundQueueFactoryParams
	hostReaderRateLimiter quotas.RequestRateLimiter
	// Shared rate limiter pool for all shards in the host.
	rateLimiterPool *collection.OnceMap[queues.NamespaceIDAndDestination, quotas.RateLimiter]
	// Shared scheduler across all shards in the host.
	hostScheduler queues.Scheduler
}

func NewOutboundQueueFactory(params outboundQueueFactoryParams) QueueFactory {
	rateLimiterPool := collection.NewOnceMap(func(queues.NamespaceIDAndDestination) quotas.RateLimiter {
		// TODO: get this value from dynamic config.
		return quotas.NewDefaultOutgoingRateLimiter(func() float64 { return 100.0 })
	})
	grouper := queues.GrouperNamespaceIDAndDestination{}
	f := &outboundQueueFactory{
		outboundQueueFactoryParams: params,
		hostReaderRateLimiter: queues.NewReaderPriorityRateLimiter(
			NewHostRateLimiterRateFn(
				params.Config.OutboundProcessorMaxPollHostRPS,
				params.Config.PersistenceMaxQPS,
				outboundQueuePersistenceMaxRPSRatio,
			),
			int64(params.Config.OutboundQueueMaxReaderCount()),
		),
		hostScheduler: &queues.CommonSchedulerWrapper{
			Scheduler: ctasks.NewGroupByScheduler[queues.NamespaceIDAndDestination, queues.Executable](
				ctasks.GroupBySchedulerOptions[queues.NamespaceIDAndDestination, queues.Executable]{
					Logger: params.Logger,
					KeyFn:  func(e queues.Executable) queues.NamespaceIDAndDestination { return grouper.KeyTyped(e.GetTask()) },
					RunnableFactory: func(e queues.Executable) ctasks.Runnable {
						return ctasks.RateLimitedTaskRunnable{
							Limiter:  rateLimiterPool.Get(grouper.KeyTyped(e.GetTask())),
							Runnable: ctasks.RunnableTask{Task: e},
						}
					},
					SchedulerFactory: func(key queues.NamespaceIDAndDestination) ctasks.RunnableScheduler {
						return ctasks.NewDynamicWorkerPoolScheduler(groupLimiter{key})
					},
				},
			),
			TaskKeyFn: func(e queues.Executable) queues.TaskChannelKey {
				// This key is used by the rescheduler and does not support destinations.
				// Destination is intentionally ignored here.
				return queues.TaskChannelKey{NamespaceID: e.GetNamespaceID(), Priority: e.GetPriority()}
			},
		},
	}
	return f
}

// Start implements QueueFactory.
func (f *outboundQueueFactory) Start() {
	f.hostScheduler.Start()
}

// Stop implements QueueFactory.
func (f *outboundQueueFactory) Stop() {
	f.hostScheduler.Stop()
}

func (f *outboundQueueFactory) CreateQueue(
	shardContext shard.Context,
	workflowCache wcache.Cache,
) queues.Queue {
	logger := log.With(shardContext.GetLogger(), tag.ComponentOutboundQueue)
	metricsHandler := f.MetricsHandler.WithTags(metrics.OperationTag(metrics.OperationOutboundQueueProcessorScope))

	currentClusterName := f.ClusterMetadata.GetCurrentClusterName()

	rescheduler := queues.NewRescheduler(
		f.hostScheduler,
		shardContext.GetTimeSource(),
		logger,
		metricsHandler,
	)

	activeExecutor := newOutboundQueueActiveTaskExecutor(
		shardContext,
		workflowCache,
		logger,
		metricsHandler,
	)

	// not implemented yet
	standbyExecutor := &outboundQueueStandbyTaskExecutor{}

	executor := queues.NewActiveStandbyExecutor(
		currentClusterName,
		f.NamespaceRegistry,
		activeExecutor,
		standbyExecutor,
		logger,
	)

	if f.ExecutorWrapper != nil {
		executor = f.ExecutorWrapper.Wrap(executor)
	}

	factory := queues.NewExecutableFactory(
		executor,
		f.hostScheduler,
		rescheduler,
		queues.NewNoopPriorityAssigner(),
		shardContext.GetTimeSource(),
		shardContext.GetNamespaceRegistry(),
		shardContext.GetClusterMetadata(),
		logger,
		metricsHandler,
		f.DLQWriter,
		f.Config.TaskDLQEnabled,
		f.Config.TaskDLQUnexpectedErrorAttempts,
		f.Config.TaskDLQInternalErrors,
	)
	return queues.NewImmediateQueue(
		shardContext,
		tasks.CategoryOutbound,
		f.hostScheduler,
		rescheduler,
		&queues.Options{
			ReaderOptions: queues.ReaderOptions{
				BatchSize:            f.Config.OutboundTaskBatchSize,
				MaxPendingTasksCount: f.Config.QueuePendingTaskMaxCount,
				PollBackoffInterval:  f.Config.OutboundProcessorPollBackoffInterval,
			},
			MonitorOptions: queues.MonitorOptions{
				PendingTasksCriticalCount:   f.Config.QueuePendingTaskCriticalCount,
				ReaderStuckCriticalAttempts: f.Config.QueueReaderStuckCriticalAttempts,
				SliceCountCriticalThreshold: f.Config.QueueCriticalSlicesCount,
			},
			MaxPollRPS:                          f.Config.OutboundProcessorMaxPollRPS,
			MaxPollInterval:                     f.Config.OutboundProcessorMaxPollInterval,
			MaxPollIntervalJitterCoefficient:    f.Config.OutboundProcessorMaxPollIntervalJitterCoefficient,
			CheckpointInterval:                  f.Config.OutboundProcessorUpdateAckInterval,
			CheckpointIntervalJitterCoefficient: f.Config.OutboundProcessorUpdateAckIntervalJitterCoefficient,
			MaxReaderCount:                      f.Config.OutboundQueueMaxReaderCount,
		},
		f.hostReaderRateLimiter,
		queues.GrouperNamespaceIDAndDestination{},
		logger,
		metricsHandler,
		factory,
	)
}
