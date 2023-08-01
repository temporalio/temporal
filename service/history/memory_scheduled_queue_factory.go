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

	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

type (
	memoryScheduledQueueFactoryParams struct {
		fx.In

		NamespaceRegistry namespace.Registry
		ClusterMetadata   cluster.Metadata
		Config            *configs.Config
		TimeSource        clock.TimeSource
		MetricsHandler    metrics.Handler
		Logger            log.SnTaggedLogger

		ExecutorWrapper queues.ExecutorWrapper `optional:"true"`
	}

	memoryScheduledQueueFactory struct {
		scheduler        ctasks.Scheduler[ctasks.Task]
		priorityAssigner queues.PriorityAssigner

		namespaceRegistry namespace.Registry
		clusterMetadata   cluster.Metadata
		timeSource        clock.TimeSource
		metricsHandler    metrics.Handler
		logger            log.SnTaggedLogger

		executorWrapper queues.ExecutorWrapper
	}
)

func NewMemoryScheduledQueueFactory(
	params memoryScheduledQueueFactoryParams,
) QueueFactory {
	logger := log.With(params.Logger, tag.ComponentMemoryScheduledQueue)
	metricsHandler := params.MetricsHandler.WithTags(metrics.OperationTag(metrics.OperationMemoryScheduledQueueProcessorScope))

	hostScheduler := ctasks.NewFIFOScheduler[ctasks.Task](
		&ctasks.FIFOSchedulerOptions{
			QueueSize:   0, // Don't buffer tasks in scheduler. If all workers are busy memoryScheduledQueue reschedules tasks into itself.
			WorkerCount: params.Config.MemoryTimerProcessorSchedulerWorkerCount,
		},
		logger,
	)

	return &memoryScheduledQueueFactory{
		scheduler:         hostScheduler,
		priorityAssigner:  queues.NewPriorityAssigner(),
		namespaceRegistry: params.NamespaceRegistry,
		clusterMetadata:   params.ClusterMetadata,
		timeSource:        params.TimeSource,
		metricsHandler:    metricsHandler,
		logger:            logger,
		executorWrapper:   params.ExecutorWrapper,
	}
}

func (f *memoryScheduledQueueFactory) Start() {
	f.scheduler.Start()
}

func (f *memoryScheduledQueueFactory) Stop() {
	f.scheduler.Stop()
}

func (f *memoryScheduledQueueFactory) CreateQueue(
	shardCtx shard.Context,
	workflowCache wcache.Cache,
) queues.Queue {

	// Reuse TimerQueueActiveTaskExecutor only to executeWorkflowTaskTimeoutTask.
	// Unused dependencies are nil.
	speculativeWorkflowTaskTimeoutExecutor := newTimerQueueActiveTaskExecutor(
		shardCtx,
		workflowCache,
		nil,
		f.logger,
		f.metricsHandler,
		shardCtx.GetConfig(),
		nil,
	)
	if f.executorWrapper != nil {
		speculativeWorkflowTaskTimeoutExecutor = f.executorWrapper.Wrap(speculativeWorkflowTaskTimeoutExecutor)
	}

	return queues.NewSpeculativeWorkflowTaskTimeoutQueue(
		f.scheduler,
		f.priorityAssigner,
		speculativeWorkflowTaskTimeoutExecutor,
		f.namespaceRegistry,
		f.clusterMetadata,
		f.timeSource,
		f.metricsHandler,
		f.logger,
	)
}
