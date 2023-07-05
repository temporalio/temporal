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
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/configs"
)

const (
	// This is the task channel buffer size between
	// weighted round robin scheduler and the actual
	// worker pool (fifo processor).
	prioritySchedulerProcessorQueueSize = 10

	taskSchedulerToken = 1
)

type (
	// Scheduler is the component for scheduling and processing
	// task executables. Ack(), Nack() or Reschedule() will always
	// be called on all executables that have been successfully submited.
	// Reschedule() will only be called after the Scheduler has been stopped
	Scheduler interface {
		Submit(Executable)
		TrySubmit(Executable) bool

		TaskChannelKeyFn() TaskChannelKeyFn
		ChannelWeightFn() ChannelWeightFn
		Start()
		Stop()
	}

	TaskChannelKey struct {
		NamespaceID string
		Priority    tasks.Priority
	}

	TaskChannelKeyFn = tasks.TaskChannelKeyFn[Executable, TaskChannelKey]
	ChannelWeightFn  = tasks.ChannelWeightFn[TaskChannelKey]

	NamespacePrioritySchedulerOptions struct {
		WorkerCount                 dynamicconfig.IntPropertyFn
		ActiveNamespaceWeights      dynamicconfig.MapPropertyFnWithNamespaceFilter
		StandbyNamespaceWeights     dynamicconfig.MapPropertyFnWithNamespaceFilter
		EnableRateLimiter           dynamicconfig.BoolPropertyFn
		EnableRateLimiterShadowMode dynamicconfig.BoolPropertyFn
		DispatchThrottleDuration    dynamicconfig.DurationPropertyFn
	}

	PrioritySchedulerOptions struct {
		WorkerCount                 dynamicconfig.IntPropertyFn
		Weight                      dynamicconfig.MapPropertyFn
		EnableRateLimiter           dynamicconfig.BoolPropertyFn
		EnableRateLimiterShadowMode dynamicconfig.BoolPropertyFn
		DispatchThrottleDuration    dynamicconfig.DurationPropertyFn
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
	rateLimiter SchedulerRateLimiter,
	timeSource clock.TimeSource,
	metricsHandler metrics.Handler,
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
		namespaceName := namespace.EmptyName

		ns, err := namespaceRegistry.GetNamespaceByID(namespace.ID(key.NamespaceID))
		if err == nil {
			namespaceName = ns.Name()
			if !ns.ActiveInCluster(currentClusterName) {
				namespaceWeights = options.StandbyNamespaceWeights
			}
		} else {
			// if namespace not found, treat it as active namespace and
			// use default active namespace weight
			logger.Warn("Unable to find namespace, using active namespace task channel weight",
				tag.WorkflowNamespaceID(key.NamespaceID),
				tag.Error(err),
			)
		}

		return configs.ConvertDynamicConfigValueToWeights(
			namespaceWeights(namespaceName.String()),
			logger,
		)[key.Priority]
	}
	channelWeightUpdateCh := make(chan struct{}, 1)
	channelQuotaRequestFn := func(key TaskChannelKey) quotas.Request {
		namespaceName, err := namespaceRegistry.GetNamespaceName(namespace.ID(key.NamespaceID))
		if err != nil {
			namespaceName = namespace.EmptyName
		}
		return quotas.NewRequest("", taskSchedulerToken, namespaceName.String(), tasks.PriorityName[key.Priority], 0, "")
	}
	taskChannelMetricsTagsFn := func(key TaskChannelKey) []metrics.Tag {
		namespaceName, _ := namespaceRegistry.GetNamespaceName(namespace.ID(key.NamespaceID))
		return []metrics.Tag{
			metrics.NamespaceTag(string(namespaceName)),
			metrics.TaskPriorityTag(key.Priority.String()),
		}
	}
	fifoSchedulerOptions := &tasks.FIFOSchedulerOptions{
		QueueSize:   prioritySchedulerProcessorQueueSize,
		WorkerCount: options.WorkerCount,
	}

	return &schedulerImpl{
		Scheduler: tasks.NewInterleavedWeightedRoundRobinScheduler(
			tasks.InterleavedWeightedRoundRobinSchedulerOptions[Executable, TaskChannelKey]{
				TaskChannelKeyFn:            taskChannelKeyFn,
				ChannelWeightFn:             channelWeightFn,
				ChannelWeightUpdateCh:       channelWeightUpdateCh,
				ChannelQuotaRequestFn:       channelQuotaRequestFn,
				TaskChannelMetricTagsFn:     taskChannelMetricsTagsFn,
				EnableRateLimiter:           options.EnableRateLimiter,
				EnableRateLimiterShadowMode: options.EnableRateLimiterShadowMode,
				DispatchThrottleDuration:    options.DispatchThrottleDuration,
			},
			tasks.Scheduler[Executable](tasks.NewFIFOScheduler[Executable](
				fifoSchedulerOptions,
				logger,
			)),
			rateLimiter,
			timeSource,
			logger,
			metricsHandler,
		),
		namespaceRegistry:     namespaceRegistry,
		taskChannelKeyFn:      taskChannelKeyFn,
		channelWeightFn:       channelWeightFn,
		channelWeightUpdateCh: channelWeightUpdateCh,
	}
}

// NewPriorityScheduler ignores namespace when scheduleing tasks.
// currently only used for shard level task scheduler
func NewPriorityScheduler(
	options PrioritySchedulerOptions,
	rateLimiter SchedulerRateLimiter,
	timeSource clock.TimeSource,
	logger log.Logger,
	metricsHandler metrics.Handler,
) Scheduler {
	taskChannelKeyFn := func(e Executable) TaskChannelKey {
		return TaskChannelKey{
			NamespaceID: namespace.EmptyID.String(),
			Priority:    e.GetPriority(),
		}
	}
	channelWeightFn := func(key TaskChannelKey) int {
		weight := configs.DefaultActiveTaskPriorityWeight
		if options.Weight != nil {
			weight = configs.ConvertDynamicConfigValueToWeights(
				options.Weight(),
				logger,
			)
		}
		return weight[key.Priority]
	}
	channelQuotaRequestFn := func(key TaskChannelKey) quotas.Request {
		return quotas.NewRequest("", taskSchedulerToken, "", tasks.PriorityName[key.Priority], 0, "")
	}
	taskChannelMetricsTagsFn := func(key TaskChannelKey) []metrics.Tag {
		return []metrics.Tag{
			metrics.NamespaceUnknownTag(),
			metrics.TaskPriorityTag(key.Priority.String()),
		}
	}
	fifoSchedulerOptions := &tasks.FIFOSchedulerOptions{
		QueueSize:   prioritySchedulerProcessorQueueSize,
		WorkerCount: options.WorkerCount,
	}

	return &schedulerImpl{
		Scheduler: tasks.NewInterleavedWeightedRoundRobinScheduler(
			tasks.InterleavedWeightedRoundRobinSchedulerOptions[Executable, TaskChannelKey]{
				TaskChannelKeyFn:            taskChannelKeyFn,
				ChannelWeightFn:             channelWeightFn,
				ChannelWeightUpdateCh:       nil,
				ChannelQuotaRequestFn:       channelQuotaRequestFn,
				TaskChannelMetricTagsFn:     taskChannelMetricsTagsFn,
				EnableRateLimiter:           options.EnableRateLimiter,
				EnableRateLimiterShadowMode: options.EnableRateLimiterShadowMode,
				DispatchThrottleDuration:    options.DispatchThrottleDuration,
			},
			tasks.Scheduler[Executable](tasks.NewFIFOScheduler[Executable](
				fifoSchedulerOptions,
				logger,
			)),
			rateLimiter,
			timeSource,
			logger,
			metricsHandler,
		),
		taskChannelKeyFn:      taskChannelKeyFn,
		channelWeightFn:       channelWeightFn,
		channelWeightUpdateCh: nil,
	}
}

func (s *schedulerImpl) Start() {
	if s.channelWeightUpdateCh != nil {
		s.namespaceRegistry.RegisterStateChangeCallback(s, func(ns *namespace.Namespace, deletedFromDb bool) {
			select {
			case s.channelWeightUpdateCh <- struct{}{}:
			default:
			}
		})
	}
	s.Scheduler.Start()
}

func (s *schedulerImpl) Stop() {
	if s.channelWeightUpdateCh != nil {
		s.namespaceRegistry.UnregisterStateChangeCallback(s)

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
