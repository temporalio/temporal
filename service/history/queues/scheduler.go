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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination scheduler_mock.go

package queues

import (
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/configs"
)

const (
	// This is the task channel buffer size between
	// weighted round robin scheduler and the actual
	// worker pool (parallel processor).
	namespacePrioritySchedulerProcessorQueueSize = 10
)

type (
	// Scheduler is the component for scheduling and processing
	// task executables. Ack(), Nack() or Reschedule() will always
	// be called on all executables that have been successfully submited.
	// Reschedule() will only be called after the Scheduler has been stopped
	Scheduler interface {
		common.Daemon

		Submit(Executable)
		TrySubmit(Executable) bool

		TaskChannelKeyFn() TaskChannelKeyFn
		ChannelWeightFn() ChannelWeightFn
	}

	TaskChannelKey struct {
		NamespaceID string
		Priority    tasks.Priority
	}

	TaskChannelKeyFn = tasks.TaskChannelKeyFn[Executable, TaskChannelKey]
	ChannelWeightFn  = tasks.ChannelWeightFn[TaskChannelKey]

	NamespacePrioritySchedulerOptions struct {
		WorkerCount      dynamicconfig.IntPropertyFn
		NamespaceWeights dynamicconfig.MapPropertyFnWithNamespaceFilter
	}

	FIFOSchedulerOptions struct {
		WorkerCount dynamicconfig.IntPropertyFn
		QueueSize   int
	}

	schedulerImpl struct {
		tasks.Scheduler[Executable]

		taskChannelKeyFn TaskChannelKeyFn
		channelWeightFn  ChannelWeightFn
	}
)

func NewNamespacePriorityScheduler(
	options NamespacePrioritySchedulerOptions,
	namespaceRegistry namespace.Registry,
	logger log.Logger,
) Scheduler {
	taskChannelKeyFn := func(e Executable) TaskChannelKey {
		return TaskChannelKey{
			NamespaceID: e.GetNamespaceID(),
			Priority:    e.GetPriority(),
		}
	}
	channelWeightFn := func(key TaskChannelKey) int {
		namespaceName, _ := namespaceRegistry.GetNamespaceName(namespace.ID(key.NamespaceID))
		return configs.ConvertDynamicConfigValueToWeights(
			options.NamespaceWeights(namespaceName.String()),
			logger,
		)[key.Priority]
	}
	fifoSchedulerOptions := &tasks.FIFOSchedulerOptions{
		QueueSize:   namespacePrioritySchedulerProcessorQueueSize,
		WorkerCount: options.WorkerCount,
	}

	return &schedulerImpl{
		Scheduler: tasks.NewInterleavedWeightedRoundRobinScheduler(
			tasks.InterleavedWeightedRoundRobinSchedulerOptions[Executable, TaskChannelKey]{
				TaskChannelKeyFn: taskChannelKeyFn,
				ChannelWeightFn:  channelWeightFn,
			},
			tasks.Scheduler[Executable](tasks.NewFIFOScheduler[Executable](
				fifoSchedulerOptions,
				logger,
			)),
			logger,
		),
		taskChannelKeyFn: taskChannelKeyFn,
		channelWeightFn:  channelWeightFn,
	}
}

// NewFIFOScheduler is used to create shard level task scheduler
// and always schedule tasks in fifo order regardless
// which namespace the task belongs to.
func NewFIFOScheduler(
	options FIFOSchedulerOptions,
	logger log.Logger,
) Scheduler {
	taskChannelKeyFn := func(_ Executable) TaskChannelKey { return TaskChannelKey{} }
	channelWeightFn := func(_ TaskChannelKey) int { return 1 }
	fifoSchedulerOptions := &tasks.FIFOSchedulerOptions{
		QueueSize:   options.QueueSize,
		WorkerCount: options.WorkerCount,
	}

	return &schedulerImpl{
		Scheduler: tasks.NewFIFOScheduler[Executable](
			fifoSchedulerOptions,
			logger,
		),
		taskChannelKeyFn: taskChannelKeyFn,
		channelWeightFn:  channelWeightFn,
	}
}

func (s *schedulerImpl) TaskChannelKeyFn() TaskChannelKeyFn {
	return s.taskChannelKeyFn
}

func (s *schedulerImpl) ChannelWeightFn() ChannelWeightFn {
	return s.channelWeightFn
}
