//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination scheduler_mock.go

package queues

import (
	"go.temporal.io/server/chasm"
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
	// task executables, it's based on the common/tasks.Scheduler
	// interface and provide the additional information of how
	// tasks are grouped during scheduling.
	Scheduler interface {
		Start()
		Stop()
		Submit(Executable)
		TrySubmit(Executable) bool

		TaskChannelKeyFn() TaskChannelKeyFn
	}

	TaskChannelKey struct {
		NamespaceID string
		Priority    tasks.Priority
	}

	TaskChannelKeyFn = tasks.TaskChannelKeyFn[Executable, TaskChannelKey]
	ChannelWeightFn  = tasks.ChannelWeightFn[TaskChannelKey]

	SchedulerOptions struct {
		WorkerCount                    dynamicconfig.TypedSubscribable[int]
		ActiveNamespaceWeights         dynamicconfig.MapPropertyFnWithNamespaceFilter
		StandbyNamespaceWeights        dynamicconfig.MapPropertyFnWithNamespaceFilter
		InactiveNamespaceDeletionDelay dynamicconfig.DurationPropertyFn
	}

	RateLimitedSchedulerOptions struct {
		EnableShadowMode dynamicconfig.BoolPropertyFn
		StartupDelay     dynamicconfig.DurationPropertyFn
	}

	schedulerImpl struct {
		tasks.Scheduler[Executable]
		namespaceRegistry namespace.Registry

		taskChannelKeyFn      TaskChannelKeyFn
		channelWeightFn       ChannelWeightFn
		channelWeightUpdateCh chan struct{}
	}

	rateLimitedSchedulerImpl struct {
		tasks.Scheduler[Executable]

		baseScheduler Scheduler
	}
)

func NewScheduler(
	currentClusterName string,
	options SchedulerOptions,
	namespaceRegistry namespace.Registry,
	logger log.Logger,
) Scheduler {
	var scheduler tasks.Scheduler[Executable]

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

		weight, ok := configs.ConvertDynamicConfigValueToWeights(
			namespaceWeights(namespaceName.String()),
			logger,
		)[key.Priority]
		if !ok || weight <= 0 {
			logger.Warn("Task priority weight not specified or is invalid, using default weight",
				tag.TaskPriority(key.Priority.String()),
				tag.NewInt("priority-weight", weight),
				tag.NewInt("default-weight", configs.DefaultPriorityWeight),
			)
			weight = configs.DefaultPriorityWeight
		}
		return weight
	}
	channelWeightUpdateCh := make(chan struct{}, 1)
	fifoSchedulerOptions := &tasks.FIFOSchedulerOptions{
		QueueSize:   prioritySchedulerProcessorQueueSize,
		WorkerCount: options.WorkerCount,
	}

	scheduler = tasks.NewInterleavedWeightedRoundRobinScheduler(
		tasks.InterleavedWeightedRoundRobinSchedulerOptions[Executable, TaskChannelKey]{
			TaskChannelKeyFn:             taskChannelKeyFn,
			ChannelWeightFn:              channelWeightFn,
			ChannelWeightUpdateCh:        channelWeightUpdateCh,
			InactiveChannelDeletionDelay: options.InactiveNamespaceDeletionDelay,
		},
		tasks.Scheduler[Executable](tasks.NewFIFOScheduler[Executable](
			fifoSchedulerOptions,
			logger,
		)),
		logger,
	)

	return &schedulerImpl{
		Scheduler:             scheduler,
		namespaceRegistry:     namespaceRegistry,
		taskChannelKeyFn:      taskChannelKeyFn,
		channelWeightFn:       channelWeightFn,
		channelWeightUpdateCh: channelWeightUpdateCh,
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

// CommonSchedulerWrapper is an adapter that converts a common [task.Scheduler] to a [Scheduler] with an injectable
// TaskChannelKeyFn.
type CommonSchedulerWrapper struct {
	tasks.Scheduler[Executable]
	TaskKeyFn func(e Executable) TaskChannelKey
}

func (s *CommonSchedulerWrapper) TaskChannelKeyFn() TaskChannelKeyFn {
	return s.TaskKeyFn
}

func NewRateLimitedScheduler(
	baseScheduler Scheduler,
	options RateLimitedSchedulerOptions,
	currentClusterName string,
	namespaceRegistry namespace.Registry,
	rateLimiter SchedulerRateLimiter,
	timeSource clock.TimeSource,
	chasmRegistry *chasm.Registry,
	logger log.Logger,
	metricsHandler metrics.Handler,
) Scheduler {
	if delay := options.StartupDelay(); delay > 0 {
		delayedRateLimiter, err := quotas.NewDelayedRequestRateLimiter(
			rateLimiter,
			delay,
			timeSource,
		)
		if err != nil {
			logger.Error("Failed to create delayed rate limited scheduler", tag.Error(err))
			return baseScheduler
		}

		rateLimiter = delayedRateLimiter
	}

	taskQuotaRequestFn := func(e Executable) quotas.Request {
		namespaceName, err := namespaceRegistry.GetNamespaceName(namespace.ID(e.GetNamespaceID()))
		if err != nil {
			namespaceName = namespace.EmptyName
		}

		return quotas.NewRequest(e.GetType().String(), taskSchedulerToken, namespaceName.String(), e.GetPriority().CallerType(), 0, "")
	}
	taskMetricsTagsFn := func(e Executable) []metrics.Tag {
		return append(
			estimateTaskMetricTags(e.GetTask(), namespaceRegistry, currentClusterName, chasmRegistry, GetTaskTypeTagValue),
			metrics.TaskPriorityTag(e.GetPriority().String()),
		)
	}

	rateLimitedScheduler := tasks.NewRateLimitedScheduler[Executable](
		baseScheduler,
		rateLimiter,
		timeSource,
		taskQuotaRequestFn,
		taskMetricsTagsFn,
		tasks.RateLimitedSchedulerOptions{
			EnableShadowMode: options.EnableShadowMode(),
		},
		logger,
		metricsHandler,
	)

	return &rateLimitedSchedulerImpl{
		Scheduler:     rateLimitedScheduler,
		baseScheduler: baseScheduler,
	}
}

func (s *rateLimitedSchedulerImpl) Start() {
	s.baseScheduler.Start()
}

func (s *rateLimitedSchedulerImpl) Stop() {
	s.baseScheduler.Stop()
}

func (s *rateLimitedSchedulerImpl) TaskChannelKeyFn() TaskChannelKeyFn {
	return s.baseScheduler.TaskChannelKeyFn()
}
