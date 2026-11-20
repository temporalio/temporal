package history

import (
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
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
		workflowTaskTimeoutScheduler ctasks.Scheduler[ctasks.Task]
		chasmWakeupScheduler         ctasks.Scheduler[ctasks.Task]
		priorityAssigner             queues.PriorityAssigner

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

	// Schedulers don't buffer tasks. If all workers are busy, memoryScheduledQueue reschedules tasks into itself.
	workflowTaskTimeoutScheduler := ctasks.NewFIFOScheduler[ctasks.Task](
		&ctasks.FIFOSchedulerOptions{
			QueueSize:   0,
			WorkerCount: params.Config.MemoryTimerProcessorSchedulerWorkerCount,
		},
		logger,
	)
	chasmWakeupScheduler := ctasks.NewFIFOScheduler[ctasks.Task](
		&ctasks.FIFOSchedulerOptions{
			QueueSize:   0,
			WorkerCount: params.Config.MemoryTimerProcessorChasmWakeupSchedulerWorkerCount,
		},
		logger,
	)

	return &memoryScheduledQueueFactory{
		workflowTaskTimeoutScheduler: workflowTaskTimeoutScheduler,
		chasmWakeupScheduler:         chasmWakeupScheduler,
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
	f.workflowTaskTimeoutScheduler.Start()
	f.chasmWakeupScheduler.Start()
}

func (f *memoryScheduledQueueFactory) Stop() {
	f.workflowTaskTimeoutScheduler.Stop()
	f.chasmWakeupScheduler.Stop()
}

func (f *memoryScheduledQueueFactory) CreateQueue(
	shardCtx historyi.ShardContext,
) queues.Queue {

	// Reuse TimerQueueActiveTaskExecutor only for speculative workflow task timeouts and CHASM
	// wake-ups. Unused dependencies are nil.
	speculativeTaskExecutor := newTimerQueueActiveTaskExecutor(
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
		speculativeTaskExecutor = f.executorWrapper.Wrap(speculativeTaskExecutor)
	}

	return queues.NewSpeculativeTaskQueue(
		f.workflowTaskTimeoutScheduler,
		f.chasmWakeupScheduler,
		f.priorityAssigner,
		speculativeTaskExecutor,
		f.namespaceRegistry,
		f.clusterMetadata,
		f.timeSource,
		f.chasmRegistry,
		f.metricsHandler,
		f.tracer,
		f.logger,
	)
}
