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
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/service/history/deletemanager"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/tasks"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/fx"
)

const (
	timerQueuePersistenceMaxRPSRatio = 0.3
)

type (
	timerQueueFactoryParams struct {
		fx.In

		QueueFactoryBaseParams
		ClientBean        client.Bean
		MatchingRawClient resource.MatchingRawClient
		VisibilityManager manager.VisibilityManager
	}

	timerQueueFactory struct {
		timerQueueFactoryParams
		QueueFactoryBase
	}
)

func NewTimerQueueFactory(
	params timerQueueFactoryParams,
) QueueFactory {
	return &timerQueueFactory{
		timerQueueFactoryParams: params,
		QueueFactoryBase: QueueFactoryBase{
			HostScheduler: queues.NewScheduler(
				params.ClusterMetadata.GetCurrentClusterName(),
				queues.SchedulerOptions{
					WorkerCount:                    params.Config.TimerProcessorSchedulerWorkerCount,
					ActiveNamespaceWeights:         params.Config.TimerProcessorSchedulerActiveRoundRobinWeights,
					StandbyNamespaceWeights:        params.Config.TimerProcessorSchedulerStandbyRoundRobinWeights,
					InactiveNamespaceDeletionDelay: params.Config.TaskSchedulerInactiveChannelDeletionDelay,
				},
				params.NamespaceRegistry,
				params.Logger,
			),
			HostPriorityAssigner: queues.NewPriorityAssigner(),
			HostReaderRateLimiter: queues.NewReaderPriorityRateLimiter(
				NewHostRateLimiterRateFn(
					params.Config.TimerProcessorMaxPollHostRPS,
					params.Config.PersistenceMaxQPS,
					timerQueuePersistenceMaxRPSRatio,
				),
				int64(params.Config.TimerQueueMaxReaderCount()),
			),
			Tracer: params.TracerProvider.Tracer(telemetry.ComponentQueueTimer),
		},
	}
}

func (f *timerQueueFactory) CreateQueue(
	shardContext historyi.ShardContext,
	workflowCache wcache.Cache,
) queues.Queue {
	logger := log.With(shardContext.GetLogger(), tag.ComponentTimerQueue)
	metricsHandler := f.MetricsHandler.WithTags(metrics.OperationTag(metrics.OperationTimerQueueProcessorScope))

	currentClusterName := f.ClusterMetadata.GetCurrentClusterName()
	workflowDeleteManager := deletemanager.NewDeleteManager(
		shardContext,
		workflowCache,
		f.Config,
		shardContext.GetTimeSource(),
		f.VisibilityManager,
	)

	var shardScheduler = f.HostScheduler
	if f.Config.TaskSchedulerEnableRateLimiter() {
		shardScheduler = queues.NewRateLimitedScheduler(
			f.HostScheduler,
			queues.RateLimitedSchedulerOptions{
				EnableShadowMode: f.Config.TaskSchedulerEnableRateLimiterShadowMode,
				StartupDelay:     f.Config.TaskSchedulerRateLimiterStartupDelay,
			},
			currentClusterName,
			f.NamespaceRegistry,
			f.SchedulerRateLimiter,
			f.TimeSource,
			logger,
			metricsHandler,
		)
	}

	rescheduler := queues.NewRescheduler(
		shardScheduler,
		shardContext.GetTimeSource(),
		logger,
		metricsHandler,
	)

	activeExecutor := newTimerQueueActiveTaskExecutor(
		shardContext,
		workflowCache,
		workflowDeleteManager,
		logger,
		f.MetricsHandler,
		f.Config,
		f.MatchingRawClient,
	)

	standbyExecutor := newTimerQueueStandbyTaskExecutor(
		shardContext,
		workflowCache,
		workflowDeleteManager,
		f.MatchingRawClient,
		logger,
		f.MetricsHandler,
		// note: the cluster name is for calculating time for standby tasks,
		// here we are basically using current cluster time
		// this field will be deprecated soon, currently exists so that
		// we have the option of revert to old behavior
		currentClusterName,
		f.Config,
		f.ClientBean,
	)

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
		shardScheduler,
		rescheduler,
		f.HostPriorityAssigner,
		shardContext.GetTimeSource(),
		shardContext.GetNamespaceRegistry(),
		shardContext.GetClusterMetadata(),
		logger,
		metricsHandler,
		f.Tracer,
		f.DLQWriter,
		f.Config.TaskDLQEnabled,
		f.Config.TaskDLQUnexpectedErrorAttempts,
		f.Config.TaskDLQInternalErrors,
		f.Config.TaskDLQErrorPattern,
	)
	return queues.NewScheduledQueue(
		shardContext,
		tasks.CategoryTimer,
		shardScheduler,
		rescheduler,
		factory,
		&queues.Options{
			ReaderOptions: queues.ReaderOptions{
				BatchSize:            f.Config.TimerTaskBatchSize,
				MaxPendingTasksCount: f.Config.QueuePendingTaskMaxCount,
				PollBackoffInterval:  f.Config.TimerProcessorPollBackoffInterval,
				MaxPredicateSize:     f.Config.QueueMaxPredicateSize,
			},
			MonitorOptions: queues.MonitorOptions{
				PendingTasksCriticalCount:   f.Config.QueuePendingTaskCriticalCount,
				ReaderStuckCriticalAttempts: f.Config.QueueReaderStuckCriticalAttempts,
				SliceCountCriticalThreshold: f.Config.QueueCriticalSlicesCount,
			},
			MaxPollRPS:                          f.Config.TimerProcessorMaxPollRPS,
			MaxPollInterval:                     f.Config.TimerProcessorMaxPollInterval,
			MaxPollIntervalJitterCoefficient:    f.Config.TimerProcessorMaxPollIntervalJitterCoefficient,
			CheckpointInterval:                  f.Config.TimerProcessorUpdateAckInterval,
			CheckpointIntervalJitterCoefficient: f.Config.TimerProcessorUpdateAckIntervalJitterCoefficient,
			MaxReaderCount:                      f.Config.TimerQueueMaxReaderCount,
		},
		f.HostReaderRateLimiter,
		logger,
		metricsHandler,
	)
}
