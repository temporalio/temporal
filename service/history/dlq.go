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
	"go.temporal.io/server/service/history/configs"
	"go.uber.org/fx"

	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/queues"
)

type (
	executableDLQWrapper struct {
		historyTaskQueueManager persistence.HistoryTaskQueueManager
		clusterName             string
		timeSource              clock.TimeSource
		useDLQ                  dynamicconfig.BoolPropertyFn
	}
	executableDLQWrapperParams struct {
		fx.In

		HistoryTaskQueueManager persistence.HistoryTaskQueueManager
		ClusterMetadata         cluster.Metadata
		Config                  *configs.Config
		TimeSource              clock.TimeSource
	}
	dlqToggle struct {
		queues.Executable
		executableDLQ *queues.ExecutableDLQ
		useDLQ        dynamicconfig.BoolPropertyFn
	}
)

func NewExecutableDLQWrapper(params executableDLQWrapperParams) queues.ExecutableWrapper {
	return executableDLQWrapper{
		historyTaskQueueManager: params.HistoryTaskQueueManager,
		clusterName:             params.ClusterMetadata.GetCurrentClusterName(),
		timeSource:              params.TimeSource,
		useDLQ:                  params.Config.TaskDLQEnabled,
	}
}

func (d executableDLQWrapper) Wrap(e queues.Executable) queues.Executable {
	executableDLQ := queues.NewExecutableDLQ(
		e,
		d.historyTaskQueueManager,
		d.timeSource,
		d.clusterName,
	)
	return &dlqToggle{
		Executable:    e,
		executableDLQ: executableDLQ,
		useDLQ:        d.useDLQ,
	}
}

func (t *dlqToggle) Execute() error {
	if t.useDLQ() {
		return t.executableDLQ.Execute()
	}
	return t.Executable.Execute()
}
