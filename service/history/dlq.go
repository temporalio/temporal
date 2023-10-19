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
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/queues"

	"go.uber.org/fx"
)

type (
	executableDLQWrapper struct {
		dlqWriter         *queues.DLQWriter
		useDLQ            dynamicconfig.BoolPropertyFn
		numHistoryShards  int
		clusterName       string
		namespaceRegistry namespace.Registry
		timeSource        clock.TimeSource
		logger            log.Logger
		metricsHandler    metrics.Handler
	}
	executableDLQWrapperParams struct {
		fx.In

		DLQWriter         *queues.DLQWriter
		Config            *configs.Config
		ClusterMetadata   cluster.Metadata
		TimeSource        clock.TimeSource
		Logger            log.Logger
		NamespaceRegistry namespace.Registry
		MetricsHandler    metrics.Handler
	}
	dlqToggle struct {
		queues.Executable
		executableDLQ queues.Executable
		useDLQ        dynamicconfig.BoolPropertyFn
	}
)

func NewExecutableDLQWrapper(params executableDLQWrapperParams) queues.ExecutableWrapper {
	return executableDLQWrapper{
		dlqWriter:         params.DLQWriter,
		useDLQ:            params.Config.TaskDLQEnabled,
		numHistoryShards:  int(params.Config.NumberOfShards),
		clusterName:       params.ClusterMetadata.GetCurrentClusterName(),
		namespaceRegistry: params.NamespaceRegistry,
		timeSource:        params.TimeSource,
		logger:            params.Logger,
		metricsHandler:    params.MetricsHandler,
	}
}

func (d executableDLQWrapper) Wrap(e queues.Executable) queues.Executable {
	executableDLQ := queues.NewExecutableDLQ(
		e,
		d.dlqWriter,
		d.timeSource,
		d.clusterName,
	)
	executableDLQObserver := queues.NewExecutableDLQObserver(
		executableDLQ,
		d.numHistoryShards,
		d.namespaceRegistry,
		d.timeSource,
		d.logger,
		d.metricsHandler,
	)
	return &dlqToggle{
		Executable:    e,
		executableDLQ: executableDLQObserver,
		useDLQ:        d.useDLQ,
	}
}

func (t *dlqToggle) Execute() error {
	if t.useDLQ() {
		return t.executableDLQ.Execute()
	}
	return t.Executable.Execute()
}
