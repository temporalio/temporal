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
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
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
		WorkerCount             dynamicconfig.IntPropertyFn
		ActiveNamespaceWeights  dynamicconfig.MapPropertyFnWithNamespaceFilter
		StandbyNamespaceWeights dynamicconfig.MapPropertyFnWithNamespaceFilter
	}

	FIFOSchedulerOptions struct {
		WorkerCount dynamicconfig.IntPropertyFn
		QueueSize   int
	}

	schedulerImpl struct {
		tasks.Scheduler[Executable]
		namespaceRegistry namespace.Registry

		taskChannelKeyFn      TaskChannelKeyFn
		channelWeightFn       ChannelWeightFn
		channelWeightUpdateCh chan struct{}
	}
)

func NewNamespacePriorityScheduler(
	currentClusterName string,
	options NamespacePrioritySchedulerOptions,
	namespaceRegistry namespace.Registry,
	timeSource clock.TimeSource,
	metricsHandler metrics.MetricsHandler,
	logger log.Logger,
) Scheduler {
	taskChannelKeyFn := func(e Executable) TaskChannelKey {
		return TaskChannelKey{
			NamespaceID: e.GetNamespaceID(),
			Priority:    e.GetPriority(),
		}
	}
	channelWeightFn := func(key TaskChannelKey) int {
		namespaceWeights := options.ActiveNamespaceWeights

		namespace, _ := namespaceRegistry.GetNamespaceByID(namespace.ID(key.NamespaceID))
		if !namespace.ActiveInCluster(currentClusterName) {
			namespaceWeights = options.StandbyNamespaceWeights
		}

		return configs.ConvertDynamicConfigValueToWeights(
			namespaceWeights(namespace.Name().String()),
			logger,
		)[key.Priority]
	}
	channelWeightUpdateCh := make(chan struct{}, 1)
	fifoSchedulerOptions := &tasks.FIFOSchedulerOptions{
		QueueSize:   namespacePrioritySchedulerProcessorQueueSize,
		WorkerCount: options.WorkerCount,
	}

	return &schedulerImpl{
		Scheduler: tasks.NewInterleavedWeightedRoundRobinScheduler(
			tasks.InterleavedWeightedRoundRobinSchedulerOptions[Executable, TaskChannelKey]{
				TaskChannelKeyFn:      taskChannelKeyFn,
				ChannelWeightFn:       channelWeightFn,
				ChannelWeightUpdateCh: channelWeightUpdateCh,
			},
			tasks.Scheduler[Executable](tasks.NewFIFOScheduler[Executable](
				newSchedulerMonitor(
					taskChannelKeyFn,
					namespaceRegistry,
					timeSource,
					metricsHandler,
					defaultSchedulerMonitorOptions,
				),
				fifoSchedulerOptions,
				logger,
			)),
			logger,
		),
		namespaceRegistry:     namespaceRegistry,
		taskChannelKeyFn:      taskChannelKeyFn,
		channelWeightFn:       channelWeightFn,
		channelWeightUpdateCh: channelWeightUpdateCh,
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
			noopScheduleMonitor,
			fifoSchedulerOptions,
			logger,
		),
		taskChannelKeyFn:      taskChannelKeyFn,
		channelWeightFn:       channelWeightFn,
		channelWeightUpdateCh: nil,
	}
}

func (s *schedulerImpl) Start() {
	if s.channelWeightUpdateCh != nil {
		s.namespaceRegistry.RegisterNamespaceChangeCallback(
			s,
			0,
			func() {}, // no-op
			func(oldNamespaces, newNamespaces []*namespace.Namespace) {
				namespaceFailover := false
				for idx := range oldNamespaces {
					if oldNamespaces[idx].FailoverVersion() != newNamespaces[idx].FailoverVersion() {
						namespaceFailover = true
						break
					}
				}

				if !namespaceFailover {
					return
				}

				select {
				case s.channelWeightUpdateCh <- struct{}{}:
				default:
				}
			},
		)
	}
	s.Scheduler.Start()
}

func (s *schedulerImpl) Stop() {
	if s.channelWeightUpdateCh != nil {
		s.namespaceRegistry.UnregisterNamespaceChangeCallback(s)

		// note we can't close the channelWeightUpdateCh here
		// as callback may still be triggered even after unregister returns
		// due to race condition
		//
		// channelWeightFn is only not nil when using host level scheduler
		// so Stop is only called when host is shutting down, and we don't need
		// to worry about open channels
	}
	s.Scheduler.Stop()
}

func (s *schedulerImpl) TaskChannelKeyFn() TaskChannelKeyFn {
	return s.taskChannelKeyFn
}

func (s *schedulerImpl) ChannelWeightFn() ChannelWeightFn {
	return s.channelWeightFn
}
