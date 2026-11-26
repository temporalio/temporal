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
	// SpeculativeTaskQueue runs in-memory timer tasks: speculative workflow task timeouts and CHASM
	// wake-ups. Each kind has its own scheduler so CHASM wake-ups don't delay workflow task timeouts.
	SpeculativeTaskQueue struct {
		workflowTaskTimeoutQueue *memoryScheduledQueue
		chasmWakeupQueue         *memoryScheduledQueue
		executor                 Executor // *timerQueueActiveTaskExecutor
		priorityAssigner         PriorityAssigner

		namespaceRegistry namespace.Registry
		clusterMetadata   cluster.Metadata
		timeSource        clock.TimeSource
		chasmRegistry     *chasm.Registry
		metricsHandler    metrics.Handler
		tracer            trace.Tracer
		logger            log.SnTaggedLogger
	}
)

func NewSpeculativeTaskQueue(
	workflowTaskTimeoutScheduler ctasks.Scheduler[ctasks.Task],
	chasmWakeupScheduler ctasks.Scheduler[ctasks.Task],
	priorityAssigner PriorityAssigner,
	executor Executor,
	namespaceRegistry namespace.Registry,
	clusterMetadata cluster.Metadata,
	timeSource clock.TimeSource,
	chasmRegistry *chasm.Registry,
	metricsHandler metrics.Handler,
	tracer trace.Tracer,
	logger log.SnTaggedLogger,
) *SpeculativeTaskQueue {
	return &SpeculativeTaskQueue{
		workflowTaskTimeoutQueue: newMemoryScheduledQueue(
			workflowTaskTimeoutScheduler,
			timeSource,
			logger,
			metricsHandler,
		),
		chasmWakeupQueue: newMemoryScheduledQueue(
			chasmWakeupScheduler,
			timeSource,
			logger,
			metricsHandler,
		),
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

func (q SpeculativeTaskQueue) Start() {
	q.workflowTaskTimeoutQueue.Start()
	q.chasmWakeupQueue.Start()
}

func (q SpeculativeTaskQueue) Stop() {
	q.workflowTaskTimeoutQueue.Stop()
	q.chasmWakeupQueue.Stop()
}

func (q SpeculativeTaskQueue) Category() tasks.Category {
	return tasks.CategoryMemoryTimer
}

func (q SpeculativeTaskQueue) NotifyNewTasks(ts []tasks.Task) {
	for _, task := range ts {
		switch t := task.(type) {
		case *tasks.WorkflowTaskTimeoutTask:
			q.workflowTaskTimeoutQueue.Add(newSpeculativeWorkflowTaskTimeoutExecutable(q.newExecutable(t), t))
		case *tasks.ChasmMemoryWakeupTask:
			q.chasmWakeupQueue.Add(newChasmMemoryWakeupExecutable(q.newExecutable(t), t))
		default:
		}
	}
}

func (q SpeculativeTaskQueue) newExecutable(task tasks.Task) Executable {
	return NewExecutable(
		0,
		task,
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
		q.metricsHandler,
		q.tracer,
	)
}

func (q SpeculativeTaskQueue) FailoverNamespace(_ string) {
}
