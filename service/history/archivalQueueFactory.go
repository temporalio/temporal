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

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/archival"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
)

const (
	archivalQueuePersistenceMaxRPSRatio = 0.15
)

type (
	archivalQueueFactoryParams struct {
		fx.In

		QueueFactoryBaseParams
	}

	archivalQueueFactory struct {
		archivalQueueFactoryParams
		QueueFactoryBase
		archiver archival.Archiver
	}
)

func NewArchivalQueueFactory(
	params archivalQueueFactoryParams,
) QueueFactory {
	hostScheduler := queues.NewNamespacePriorityScheduler(
		params.ClusterMetadata.GetCurrentClusterName(),
		queues.NamespacePrioritySchedulerOptions{
			WorkerCount: params.Config.ArchivalProcessorSchedulerWorkerCount,
			// we don't need standby weights because we only run in the active cluster
			ActiveNamespaceWeights: params.Config.ArchivalProcessorSchedulerRoundRobinWeights,
		},
		params.NamespaceRegistry,
		params.TimeSource,
		params.MetricsHandler.WithTags(metrics.OperationTag(queues.OperationArchivalQueueProcessor)),
		params.Logger,
	)
	return &archivalQueueFactory{
		archivalQueueFactoryParams: params,
		QueueFactoryBase: QueueFactoryBase{
			HostScheduler:        hostScheduler,
			HostPriorityAssigner: queues.NewPriorityAssigner(),
			HostRateLimiter: NewQueueHostRateLimiter(
				params.Config.ArchivalProcessorMaxPollHostRPS,
				params.Config.PersistenceMaxQPS,
				archivalQueuePersistenceMaxRPSRatio,
			),
			HostReaderRateLimiter: queues.NewReaderPriorityRateLimiter(
				NewHostRateLimiterRateFn(
					params.Config.ArchivalProcessorMaxPollHostRPS,
					params.Config.PersistenceMaxQPS,
					archivalQueuePersistenceMaxRPSRatio,
				),
				params.Config.QueueMaxReaderCount(),
			),
		},
	}
}

func (f *archivalQueueFactory) CreateQueue(
	shard shard.Context,
	workflowCache workflow.Cache,
) queues.Queue {
	logger := log.With(shard.GetLogger(), tag.ComponentArchivalQueue)

	executor := newArchivalQueueTaskExecutor(f.archiver, shard, workflowCache, f.MetricsHandler, f.Logger)

	return queues.NewImmediateQueue(
		shard,
		tasks.CategoryArchival,
		f.HostScheduler,
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
			TaskMaxRetryCount:                   f.Config.ArchivalTaskMaxRetryCount,
		},
		f.HostReaderRateLimiter,
		logger,
		f.MetricsHandler.WithTags(metrics.OperationTag(queues.OperationArchivalQueueProcessor)),
	)
}
