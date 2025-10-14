package history

import (
	"fmt"

	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/quotas"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/service/history/circuitbreakerpool"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/fx"
)

// outboundQueuePersistenceMaxRPSRatio is meant to ensure queue loading doesn't consume more than 30% of the host's
// persistence tokens. This is especially important upon host restart when we need to perform a load for all shards.
// This value was copied from the transfer queue factory.
const outboundQueuePersistenceMaxRPSRatio = 0.3

type outboundQueueFactoryParams struct {
	fx.In

	QueueFactoryBaseParams
	CircuitBreakerPool *circuitbreakerpool.OutboundQueueCircuitBreakerPool
}

type groupLimiter struct {
	key tasks.TaskGroupNamespaceIDAndDestination

	namespaceRegistry namespace.Registry
	metricsHandler    metrics.Handler

	bufferSize  dynamicconfig.IntPropertyFnWithDestinationFilter
	concurrency dynamicconfig.IntPropertyFnWithDestinationFilter
}

var _ ctasks.DynamicWorkerPoolLimiter = (*groupLimiter)(nil)

func (l groupLimiter) BufferSize() int {
	// This is intentionally not failing the function in case of error. The task
	// scheduler doesn't expect errors to happen, and modifying to handle errors
	// would make it unnecessarily complex. Also, in this case, if the namespace
	// registry fails to get the name, then the task itself will fail when it is
	// processed and tries to get the namespace name.
	nsName := getNamespaceNameOrDefault(
		l.namespaceRegistry,
		l.key.NamespaceID,
		"",
		l.metricsHandler,
	)
	return l.bufferSize(nsName, l.key.Destination)
}

func (l groupLimiter) Concurrency() int {
	// Ditto comment above.
	nsName := getNamespaceNameOrDefault(
		l.namespaceRegistry,
		l.key.NamespaceID,
		"",
		l.metricsHandler,
	)
	return l.concurrency(nsName, l.key.Destination)
}

type outboundQueueFactory struct {
	outboundQueueFactoryParams
	hostReaderRateLimiter quotas.RequestRateLimiter
	// Shared scheduler across all shards in the host.
	hostScheduler queues.Scheduler
}

func NewOutboundQueueFactory(params outboundQueueFactoryParams) QueueFactory {
	metricsHandler := getOutbountQueueProcessorMetricsHandler(params.MetricsHandler)

	rateLimiterPool := collection.NewOnceMap(
		func(key tasks.TaskGroupNamespaceIDAndDestination) quotas.RateLimiter {
			return quotas.NewDefaultOutgoingRateLimiter(func() float64 {
				// This is intentionally not failing the function in case of error. The task
				// scheduler doesn't expect errors to happen, and modifying to handle errors
				// would make it unnecessarily complex. Also, in this case, if the namespace
				// registry fails to get the name, then the task itself will fail when it is
				// processed and tries to get the namespace name.
				nsName := getNamespaceNameOrDefault(
					params.NamespaceRegistry,
					key.NamespaceID,
					"",
					metricsHandler,
				)
				return params.Config.OutboundQueueHostSchedulerMaxTaskRPS(nsName, key.Destination)
			})
		},
	)

	grouper := queues.GrouperStateMachineNamespaceIDAndDestination{}
	f := &outboundQueueFactory{
		outboundQueueFactoryParams: params,
		hostReaderRateLimiter: queues.NewReaderPriorityRateLimiter(
			NewHostRateLimiterRateFn(
				params.Config.OutboundProcessorMaxPollHostRPS,
				params.Config.PersistenceMaxQPS,
				outboundQueuePersistenceMaxRPSRatio,
			),
			int64(params.Config.OutboundQueueMaxReaderCount()),
		),
		hostScheduler: &queues.CommonSchedulerWrapper{
			Scheduler: ctasks.NewGroupByScheduler(
				ctasks.GroupBySchedulerOptions[
					tasks.TaskGroupNamespaceIDAndDestination,
					queues.Executable,
				]{
					Logger: params.Logger,
					KeyFn: func(e queues.Executable) tasks.TaskGroupNamespaceIDAndDestination {
						return grouper.KeyTyped(e.GetTask())
					},
					RunnableFactory: func(e queues.Executable) ctasks.Runnable {
						key := grouper.KeyTyped(e.GetTask())
						nsName := getNamespaceNameOrDefault(
							params.NamespaceRegistry,
							key.NamespaceID,
							key.NamespaceID,
							metricsHandler,
						)
						taggedMetricsHandler := metricsHandler.WithTags(
							metrics.NamespaceTag(nsName),
							metrics.DestinationTag(key.Destination),
						)
						return ctasks.NewRateLimitedTaskRunnableFromTask(
							ctasks.RunnableTask{
								Task: queues.NewCircuitBreakerExecutable(
									e,
									params.CircuitBreakerPool.Get(key),
									taggedMetricsHandler,
								),
							},
							rateLimiterPool.Get(key),
							taggedMetricsHandler,
						)
					},
					SchedulerFactory: func(
						key tasks.TaskGroupNamespaceIDAndDestination,
					) ctasks.RunnableScheduler {
						nsName := getNamespaceNameOrDefault(
							params.NamespaceRegistry,
							key.NamespaceID,
							key.NamespaceID,
							metricsHandler,
						)
						return ctasks.NewDynamicWorkerPoolScheduler(
							groupLimiter{
								key:               key,
								namespaceRegistry: params.NamespaceRegistry,
								metricsHandler:    metricsHandler,
								bufferSize:        params.Config.OutboundQueueGroupLimiterBufferSize,
								concurrency:       params.Config.OutboundQueueGroupLimiterConcurrency,
							},
							metricsHandler.WithTags(
								metrics.NamespaceTag(nsName),
								metrics.DestinationTag(key.Destination),
							),
						)
					},
				},
			),
			TaskKeyFn: func(e queues.Executable) queues.TaskChannelKey {
				// This key is used by the rescheduler and does not support destinations.
				// Destination is intentionally ignored here.
				return queues.TaskChannelKey{NamespaceID: e.GetNamespaceID(), Priority: e.GetPriority()}
			},
		},
	}
	return f
}

// Start implements QueueFactory.
func (f *outboundQueueFactory) Start() {
	f.hostScheduler.Start()
}

// Stop implements QueueFactory.
func (f *outboundQueueFactory) Stop() {
	f.hostScheduler.Stop()
}

func (f *outboundQueueFactory) CreateQueue(
	shardContext historyi.ShardContext,
) queues.Queue {
	logger := log.With(shardContext.GetLogger(), tag.ComponentOutboundQueue)
	metricsHandler := getOutbountQueueProcessorMetricsHandler(f.MetricsHandler)

	currentClusterName := f.ClusterMetadata.GetCurrentClusterName()

	rescheduler := queues.NewRescheduler(
		f.hostScheduler,
		shardContext.GetTimeSource(),
		logger,
		metricsHandler,
	)

	activeExecutor := newOutboundQueueActiveTaskExecutor(
		shardContext,
		f.WorkflowCache,
		logger,
		metricsHandler,
		f.ChasmEngine,
	)

	standbyExecutor := newOutboundQueueStandbyTaskExecutor(
		shardContext,
		f.WorkflowCache,
		currentClusterName,
		logger,
		metricsHandler,
		f.ChasmEngine,
	)

	executor := queues.NewActiveStandbyExecutor(
		currentClusterName,
		f.NamespaceRegistry,
		activeExecutor,
		standbyExecutor,
		logger,
	)

	if f.ExecutorWrapper != nil {
		executor = f.ExecutorWrapper.Wrap(executor)
	}

	factory := queues.NewExecutableFactory(
		executor,
		f.hostScheduler,
		rescheduler,
		queues.NewNoopPriorityAssigner(),
		shardContext.GetTimeSource(),
		shardContext.GetNamespaceRegistry(),
		shardContext.GetClusterMetadata(),
		logger,
		metricsHandler,
		f.TracerProvider.Tracer(telemetry.ComponentQueueOutbound),
		f.DLQWriter,
		f.Config.TaskDLQEnabled,
		f.Config.TaskDLQUnexpectedErrorAttempts,
		f.Config.TaskDLQInternalErrors,
		f.Config.TaskDLQErrorPattern,
	)
	return queues.NewImmediateQueue(
		shardContext,
		tasks.CategoryOutbound,
		f.hostScheduler,
		rescheduler,
		&queues.Options{
			ReaderOptions: queues.ReaderOptions{
				BatchSize:            f.Config.OutboundTaskBatchSize,
				MaxPendingTasksCount: f.Config.OutboundQueuePendingTaskMaxCount,
				PollBackoffInterval:  f.Config.OutboundProcessorPollBackoffInterval,
				MaxPredicateSize:     f.Config.OutboundQueueMaxPredicateSize,
			},
			MonitorOptions: queues.MonitorOptions{
				PendingTasksCriticalCount: f.Config.OutboundQueuePendingTaskCriticalCount,
				// Shared configuration with other queues.
				ReaderStuckCriticalAttempts: f.Config.QueueReaderStuckCriticalAttempts,
				SliceCountCriticalThreshold: f.Config.QueueCriticalSlicesCount,
			},
			MaxPollRPS:                          f.Config.OutboundProcessorMaxPollRPS,
			MaxPollInterval:                     f.Config.OutboundProcessorMaxPollInterval,
			MaxPollIntervalJitterCoefficient:    f.Config.OutboundProcessorMaxPollIntervalJitterCoefficient,
			CheckpointInterval:                  f.Config.OutboundProcessorUpdateAckInterval,
			CheckpointIntervalJitterCoefficient: f.Config.OutboundProcessorUpdateAckIntervalJitterCoefficient,
			MaxReaderCount:                      f.Config.OutboundQueueMaxReaderCount,
			MoveGroupTaskCountBase:              f.Config.QueueMoveGroupTaskCountBase,
			MoveGroupTaskCountMultiplier:        f.Config.QueueMoveGroupTaskCountMultiplier,
		},
		f.hostReaderRateLimiter,
		queues.GrouperStateMachineNamespaceIDAndDestination{},
		logger,
		metricsHandler,
		factory,
	)
}

func getOutbountQueueProcessorMetricsHandler(handler metrics.Handler) metrics.Handler {
	return handler.WithTags(metrics.OperationTag(metrics.OperationOutboundQueueProcessorScope))
}

func StateMachineTask(smRegistry *hsm.Registry, task tasks.Task) (hsm.Ref, hsm.Task, error) {
	cbt, ok := task.(*tasks.StateMachineOutboundTask)
	if !ok {
		return hsm.Ref{}, nil, queues.NewUnprocessableTaskError("unknown task type")
	}
	def, ok := smRegistry.TaskSerializer(cbt.Info.Type)
	if !ok {
		return hsm.Ref{},
			nil,
			queues.NewUnprocessableTaskError(
				fmt.Sprintf("deserializer not registered for task type %v", cbt.Info.Type),
			)
	}
	smt, err := def.Deserialize(cbt.Info.Data, hsm.TaskAttributes{Destination: cbt.Destination})
	if err != nil {
		return hsm.Ref{},
			nil,
			fmt.Errorf(
				"%w: %w",
				queues.NewUnprocessableTaskError(fmt.Sprintf("cannot deserialize task %v", cbt.Info.Type)),
				err,
			)
	}
	return hsm.Ref{
		WorkflowKey:     taskWorkflowKey(task),
		StateMachineRef: cbt.Info.Ref,
		TaskID:          task.GetTaskID(),
		Validate:        smt.Validate,
	}, smt, nil
}

func getNamespaceNameOrDefault(
	registry namespace.Registry,
	namespaceID string,
	def string,
	metricsHandler metrics.Handler,
) string {
	nsName, err := registry.GetNamespaceName(namespace.ID(namespaceID))
	if err != nil {
		metrics.ReadNamespaceErrors.With(metricsHandler).
			Record(1, metrics.ReasonTag(metrics.ReasonString(err.Error())))
		return def
	}
	return nsName.String()
}
