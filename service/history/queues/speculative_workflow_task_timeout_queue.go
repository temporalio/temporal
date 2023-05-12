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

package queues

import (
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/tasks"
)

type (
	SpeculativeWorkflowTaskTimeoutQueue struct {
		timeoutQueue     *memoryScheduledQueue
		executor         Executor // *timerQueueActiveTaskExecutor
		priorityAssigner PriorityAssigner

		namespaceRegistry namespace.Registry
		clusterMetadata   cluster.Metadata
		timeSource        clock.TimeSource
		metricsHandler    metrics.Handler
		logger            log.SnTaggedLogger
	}
)

func NewSpeculativeWorkflowTaskTimeoutQueue(
	scheduler ctasks.Scheduler[ctasks.Task],
	priorityAssigner PriorityAssigner,
	executor Executor,
	namespaceRegistry namespace.Registry,
	clusterMetadata cluster.Metadata,
	timeSource clock.TimeSource,
	metricsHandler metrics.Handler,
	logger log.SnTaggedLogger,

) *SpeculativeWorkflowTaskTimeoutQueue {

	timeoutQueue := newMemoryScheduledQueue(
		scheduler,
		timeSource,
		logger,
		metricsHandler,
	)

	return &SpeculativeWorkflowTaskTimeoutQueue{
		timeoutQueue:      timeoutQueue,
		executor:          executor,
		priorityAssigner:  priorityAssigner,
		namespaceRegistry: namespaceRegistry,
		clusterMetadata:   clusterMetadata,
		timeSource:        timeSource,
		metricsHandler:    metricsHandler,
		logger:            logger,
	}
}

func (q SpeculativeWorkflowTaskTimeoutQueue) Start() {
	q.timeoutQueue.Start()
}

func (q SpeculativeWorkflowTaskTimeoutQueue) Stop() {
	q.timeoutQueue.Stop()
}

func (q SpeculativeWorkflowTaskTimeoutQueue) Category() tasks.Category {
	return tasks.CategoryMemoryTimer
}

func (q SpeculativeWorkflowTaskTimeoutQueue) NotifyNewTasks(ts []tasks.Task) {
	for _, task := range ts {
		if wttt, ok := task.(*tasks.WorkflowTaskTimeoutTask); ok {
			executable := newSpeculativeWorkflowTaskTimeoutExecutable(NewExecutable(
				0,
				wttt,
				q.executor,
				nil,
				nil,
				q.priorityAssigner,
				q.timeSource,
				q.namespaceRegistry,
				q.clusterMetadata,
				q.logger,
				q.metricsHandler,
			), wttt)
			q.timeoutQueue.Add(executable)
		}
	}
}

func (q SpeculativeWorkflowTaskTimeoutQueue) FailoverNamespace(_ string) {
}
