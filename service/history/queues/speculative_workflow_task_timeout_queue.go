package queues

import (
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/server/chasm"
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
		chasmRegistry     *chasm.Registry
		metricsHandler    metrics.Handler
		tracer            trace.Tracer
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
	chasmRegistry *chasm.Registry,
	metricsHandler metrics.Handler,
	tracer trace.Tracer,
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
		chasmRegistry:     chasmRegistry,
		metricsHandler:    metricsHandler,
		tracer:            tracer,
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
				q.chasmRegistry,
				GetTaskTypeTagValue,
				q.logger,
				q.metricsHandler.WithTags(defaultExecutableMetricsTags...),
				q.tracer,
			), wttt)
			q.timeoutQueue.Add(executable)
		}
	}
}

func (q SpeculativeWorkflowTaskTimeoutQueue) FailoverNamespace(_ string) {
}
