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
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/tasks"
)

type (
	// ExecutableFactory and the related interfaces here are needed to make it possible to extend the functionality of
	// task processing without changing its core logic. This is similar to ExecutorWrapper.
	ExecutableFactory interface {
		NewExecutable(task tasks.Task, readerID int64) Executable
	}
	// ExecutableFactoryFn is a convenience type to avoid having to create a struct that implements ExecutableFactory.
	ExecutableFactoryFn   func(readerID int64, t tasks.Task) Executable
	executableFactoryImpl struct {
		executor                   Executor
		scheduler                  Scheduler
		rescheduler                Rescheduler
		priorityAssigner           PriorityAssigner
		timeSource                 clock.TimeSource
		namespaceRegistry          namespace.Registry
		clusterMetadata            cluster.Metadata
		logger                     log.Logger
		metricsHandler             metrics.Handler
		dlqWriter                  *DLQWriter
		dlqEnabled                 dynamicconfig.BoolPropertyFn
		attemptsBeforeSendingToDlq dynamicconfig.IntPropertyFn
		dlqInternalErrors          dynamicconfig.BoolPropertyFn
		dlqErrorPattern            dynamicconfig.StringPropertyFn
	}
)

func (f ExecutableFactoryFn) NewExecutable(task tasks.Task, readerID int64) Executable {
	return f(readerID, task)
}

func NewExecutableFactory(
	executor Executor,
	scheduler Scheduler,
	rescheduler Rescheduler,
	priorityAssigner PriorityAssigner,
	timeSource clock.TimeSource,
	namespaceRegistry namespace.Registry,
	clusterMetadata cluster.Metadata,
	logger log.Logger,
	metricsHandler metrics.Handler,
	dlqWriter *DLQWriter,
	dlqEnabled dynamicconfig.BoolPropertyFn,
	attemptsBeforeSendingToDlq dynamicconfig.IntPropertyFn,
	dlqInternalErrors dynamicconfig.BoolPropertyFn,
	dlqErrorPattern dynamicconfig.StringPropertyFn,
) *executableFactoryImpl {
	return &executableFactoryImpl{
		executor:                   executor,
		scheduler:                  scheduler,
		rescheduler:                rescheduler,
		priorityAssigner:           priorityAssigner,
		timeSource:                 timeSource,
		namespaceRegistry:          namespaceRegistry,
		clusterMetadata:            clusterMetadata,
		logger:                     logger,
		metricsHandler:             metricsHandler,
		dlqWriter:                  dlqWriter,
		dlqEnabled:                 dlqEnabled,
		attemptsBeforeSendingToDlq: attemptsBeforeSendingToDlq,
		dlqInternalErrors:          dlqInternalErrors,
		dlqErrorPattern:            dlqErrorPattern,
	}
}

func (f *executableFactoryImpl) NewExecutable(task tasks.Task, readerID int64) Executable {
	return NewExecutable(
		readerID,
		task,
		f.executor,
		f.scheduler,
		f.rescheduler,
		f.priorityAssigner,
		f.timeSource,
		f.namespaceRegistry,
		f.clusterMetadata,
		f.logger,
		f.metricsHandler,
		func(params *ExecutableParams) {
			params.DLQEnabled = f.dlqEnabled
			params.DLQWriter = f.dlqWriter
			params.MaxUnexpectedErrorAttempts = f.attemptsBeforeSendingToDlq
			params.DLQInternalErrors = f.dlqInternalErrors
			params.DLQErrorPattern = f.dlqErrorPattern
		},
	)
}
