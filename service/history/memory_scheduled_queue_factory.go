package history

import (
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/queues"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/fx"
)

type memoryImmediateQueueFactory struct {
	scheduler ctasks.RunnableScheduler
}

type memoryImmediateQueueLimiter struct {
	maxConcurrency dynamicconfig.IntPropertyFn
}

func (l memoryImmediateQueueLimiter) Concurrency() int { return l.maxConcurrency() }
func (l memoryImmediateQueueLimiter) BufferSize() int  { return 0 }

// NewMemoryImmediateQueueFactory provides a separate host-wide pool for best-effort immediate tasks.
// These tasks never wait behind speculative Workflow Task timers or their rescheduling delay.
func NewMemoryImmediateQueueFactory(params memoryScheduledQueueFactoryParams) QueueFactory {
	return &memoryImmediateQueueFactory{
		scheduler: ctasks.NewDynamicWorkerPoolScheduler(
			memoryImmediateQueueLimiter{maxConcurrency: params.Config.MemoryImmediateProcessorMaxConcurrentTasks},
			params.MetricsHandler.WithTags(metrics.OperationTag("MemoryImmediateQueueProcessor")),
		),
	}
}

func (f *memoryImmediateQueueFactory) Start() {}
func (f *memoryImmediateQueueFactory) Stop() {
	f.scheduler.InitiateShutdown()
	f.scheduler.WaitShutdown()
}
func (f *memoryImmediateQueueFactory) CreateQueue(historyi.ShardContext) queues.Queue {
	return queues.NewMemoryImmediateQueue(f.scheduler)
}

type (
	memoryScheduledQueueFactoryParams struct {
		fx.In

		NamespaceRegistry namespace.Registry
		ClusterMetadata   cluster.Metadata
		WorkflowCache     wcache.Cache
		Config            *configs.Config
		TimeSource        clock.TimeSource
		ChasmRegistry     *chasm.Registry
		MetricsHandler    metrics.Handler
		TracerProvider    trace.TracerProvider
		Logger            log.SnTaggedLogger

		ExecutorWrapper queues.ExecutorWrapper `optional:"true"`
	}

	memoryScheduledQueueFactory struct {
		scheduler        ctasks.Scheduler[ctasks.Task]
		priorityAssigner queues.PriorityAssigner

		namespaceRegistry namespace.Registry
		clusterMetadata   cluster.Metadata
		workflowCache     wcache.Cache
		timeSource        clock.TimeSource
		chasmRegistry     *chasm.Registry
		metricsHandler    metrics.Handler
		tracer            trace.Tracer
		logger            log.SnTaggedLogger

		executorWrapper queues.ExecutorWrapper
	}
)

func NewMemoryScheduledQueueFactory(
	params memoryScheduledQueueFactoryParams,
) QueueFactory {
	logger := log.With(params.Logger, tag.ComponentMemoryScheduledQueue)
	metricsHandler := params.MetricsHandler.WithTags(metrics.OperationTag(metrics.OperationMemoryScheduledQueueProcessorScope))

	hostScheduler := ctasks.NewFIFOScheduler[ctasks.Task](
		&ctasks.FIFOSchedulerOptions{
			QueueSize:   0, // Don't buffer tasks in scheduler. If all workers are busy memoryScheduledQueue reschedules tasks into itself.
			WorkerCount: params.Config.MemoryTimerProcessorSchedulerWorkerCount,
		},
		logger,
	)

	return &memoryScheduledQueueFactory{
		scheduler: hostScheduler,
		priorityAssigner: queues.NewPriorityAssigner(
			params.NamespaceRegistry,
			params.ClusterMetadata.GetCurrentClusterName(),
		),
		namespaceRegistry: params.NamespaceRegistry,
		clusterMetadata:   params.ClusterMetadata,
		workflowCache:     params.WorkflowCache,
		timeSource:        params.TimeSource,
		chasmRegistry:     params.ChasmRegistry,
		metricsHandler:    metricsHandler,
		tracer:            params.TracerProvider.Tracer(telemetry.ComponentQueueMemory),
		logger:            logger,
		executorWrapper:   params.ExecutorWrapper,
	}
}

func (f *memoryScheduledQueueFactory) Start() {
	f.scheduler.Start()
}

func (f *memoryScheduledQueueFactory) Stop() {
	f.scheduler.Stop()
}

func (f *memoryScheduledQueueFactory) CreateQueue(
	shardCtx historyi.ShardContext,
) queues.Queue {

	// Reuse TimerQueueActiveTaskExecutor only to executeWorkflowTaskTimeoutTask.
	// Unused dependencies are nil.
	speculativeWorkflowTaskTimeoutExecutor := newTimerQueueActiveTaskExecutor(
		shardCtx,
		f.workflowCache,
		nil,
		f.logger,
		f.metricsHandler,
		shardCtx.GetConfig(),
		nil,
		nil,
	)
	if f.executorWrapper != nil {
		speculativeWorkflowTaskTimeoutExecutor = f.executorWrapper.Wrap(speculativeWorkflowTaskTimeoutExecutor)
	}

	return queues.NewSpeculativeWorkflowTaskTimeoutQueue(
		f.scheduler,
		f.priorityAssigner,
		speculativeWorkflowTaskTimeoutExecutor,
		f.namespaceRegistry,
		f.clusterMetadata,
		f.timeSource,
		f.chasmRegistry,
		f.metricsHandler,
		f.tracer,
		f.logger,
	)
}
