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
	"errors"

	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/tasks"
)

// ExecutableDLQObserver records telemetry like metrics and logs for an ExecutableDLQ.
type ExecutableDLQObserver struct {
	Executable
	timeSource     clock.TimeSource
	logger         log.Logger
	metricsHandler metrics.Handler

	isDLQ bool
}

func NewExecutableDLQObserver(
	executableDLQ Executable,
	numHistoryShards int,
	namespaceRegistry namespace.Registry,
	timeSource clock.TimeSource,
	logger log.Logger,
	metricsHandler metrics.Handler,
) *ExecutableDLQObserver {
	logger = log.NewLazyLogger(logger, func() []tag.Tag {
		return getTags(executableDLQ, numHistoryShards)
	})
	metricsHandler = getMetricsHandler(metricsHandler, executableDLQ, namespaceRegistry, logger)
	return &ExecutableDLQObserver{
		Executable:     executableDLQ,
		timeSource:     timeSource,
		logger:         logger,
		metricsHandler: metricsHandler,
		isDLQ:          false,
	}
}

func (o *ExecutableDLQObserver) Execute() error {
	now := o.timeSource.Now()
	err := o.Executable.Execute()
	if errors.Is(err, ErrTerminalTaskFailure) {
		o.getMetricsHandler().Counter(metrics.TaskTerminalFailures.GetMetricName()).Record(1)
		logger := o.getLogger()
		logger.Error("A terminal error occurred while processing this task", tag.Error(err))
		o.isDLQ = true
	} else if errors.Is(err, ErrSendTaskToDLQ) {
		o.getMetricsHandler().Counter(metrics.TaskDLQFailures.GetMetricName()).Record(1)
		logger := o.getLogger()
		logger.Error("Failed to send history task to the DLQ", tag.Error(err))
	} else if err == nil && o.isDLQ {
		latency := o.timeSource.Now().Sub(now)
		o.getMetricsHandler().Timer(metrics.TaskDLQSendLatency.GetMetricName()).Record(latency)
		logger := o.getLogger()
		logger.Info("Task sent to DLQ")
	}
	return err
}

func (o *ExecutableDLQObserver) getMetricsHandler() metrics.Handler {
	return o.metricsHandler
}

func (o *ExecutableDLQObserver) getLogger() log.Logger {
	return o.logger
}

func getMetricsHandler(
	metricsHandler metrics.Handler,
	executable Executable,
	namespaceRegistry namespace.Registry,
	logger log.Logger,
) metrics.Handler {
	handler := metricsHandler.WithTags(metrics.TaskTypeTag(executable.GetTask().GetType().String()))
	namespaceID := executable.GetTask().GetNamespaceID()
	ns, err := namespaceRegistry.GetNamespaceByID(namespace.ID(namespaceID))
	if err != nil {
		logger.Error("Unable to get namespace", tag.Error(err), tag.WorkflowNamespaceID(namespaceID))
		return handler
	}
	return handler.WithTags(
		metrics.NamespaceTag(string(ns.Name())),
	)
}

func getTags(executable Executable, numHistoryShards int) []tag.Tag {
	task := executable.GetTask()
	tags := tasks.Tags(task)
	shardID := tasks.GetShardIDForTask(task, numHistoryShards)
	tags = append(tags, tag.ShardID(int32(shardID)))
	return tags
}
