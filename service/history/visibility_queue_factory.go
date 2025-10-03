package history

import (
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/telemetry"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/fx"
)

const (
	visibilityQueuePersistenceMaxRPSRatio = 0.15
)

type (
	visibilityQueueFactoryParams struct {
		fx.In

		QueueFactoryBaseParams

		VisibilityMgr manager.VisibilityManager
	}

	visibilityQueueFactory struct {
		visibilityQueueFactoryParams
		QueueFactoryBase
	}
)

func NewVisibilityQueueFactory(
	params visibilityQueueFactoryParams,
) QueueFactory {
	return &visibilityQueueFactory{
		visibilityQueueFactoryParams: params,
		QueueFactoryBase: QueueFactoryBase{
			HostScheduler: queues.NewScheduler(
				params.ClusterMetadata.GetCurrentClusterName(),
				queues.SchedulerOptions{
					WorkerCount:                    params.Config.VisibilityProcessorSchedulerWorkerCount,
					ActiveNamespaceWeights:         params.Config.VisibilityProcessorSchedulerActiveRoundRobinWeights,
					StandbyNamespaceWeights:        params.Config.VisibilityProcessorSchedulerStandbyRoundRobinWeights,
					InactiveNamespaceDeletionDelay: params.Config.TaskSchedulerInactiveChannelDeletionDelay,
				},
				params.NamespaceRegistry,
				params.Logger,
			),
			HostPriorityAssigner: queues.NewPriorityAssigner(),
			HostReaderRateLimiter: queues.NewReaderPriorityRateLimiter(
				NewHostRateLimiterRateFn(
					params.Config.VisibilityProcessorMaxPollHostRPS,
					params.Config.PersistenceMaxQPS,
					visibilityQueuePersistenceMaxRPSRatio,
				),
				int64(params.Config.VisibilityQueueMaxReaderCount()),
			),
			Tracer: params.TracerProvider.Tracer(telemetry.ComponentQueueVisibility),
		},
	}
}

func (f *visibilityQueueFactory) CreateQueue(
	shard historyi.ShardContext,
) queues.Queue {
	logger := log.With(shard.GetLogger(), tag.ComponentVisibilityQueue)
	metricsHandler := f.MetricsHandler.WithTags(metrics.OperationTag(metrics.OperationVisibilityQueueProcessorScope))

	var shardScheduler = f.HostScheduler
	if f.Config.TaskSchedulerEnableRateLimiter() {
		shardScheduler = queues.NewRateLimitedScheduler(
			f.HostScheduler,
			queues.RateLimitedSchedulerOptions{
				EnableShadowMode: f.Config.TaskSchedulerEnableRateLimiterShadowMode,
				StartupDelay:     f.Config.TaskSchedulerRateLimiterStartupDelay,
			},
			f.ClusterMetadata.GetCurrentClusterName(),
			f.NamespaceRegistry,
			f.SchedulerRateLimiter,
			f.TimeSource,
			logger,
			metricsHandler,
		)
	}

	rescheduler := queues.NewRescheduler(
		shardScheduler,
		shard.GetTimeSource(),
		logger,
		metricsHandler,
	)

	executor := newVisibilityQueueTaskExecutor(
		shard,
		f.WorkflowCache,
		f.VisibilityMgr,
		logger,
		f.MetricsHandler,
		f.Config.VisibilityProcessorEnsureCloseBeforeDelete,
		f.Config.VisibilityProcessorEnableCloseWorkflowCleanup,
		f.Config.VisibilityProcessorRelocateAttributesMinBlobSize,
	)
	if f.ExecutorWrapper != nil {
		executor = f.ExecutorWrapper.Wrap(executor)
	}

	factory := queues.NewExecutableFactory(
		executor,
		shardScheduler,
		rescheduler,
		f.HostPriorityAssigner,
		shard.GetTimeSource(),
		shard.GetNamespaceRegistry(),
		shard.GetClusterMetadata(),
		logger,
		metricsHandler,
		f.Tracer,
		f.DLQWriter,
		f.Config.TaskDLQEnabled,
		f.Config.TaskDLQUnexpectedErrorAttempts,
		f.Config.TaskDLQInternalErrors,
		f.Config.TaskDLQErrorPattern,
	)
	return queues.NewImmediateQueue(
		shard,
		tasks.CategoryVisibility,
		shardScheduler,
		rescheduler,
		&queues.Options{
			ReaderOptions: queues.ReaderOptions{
				BatchSize:            f.Config.VisibilityTaskBatchSize,
				MaxPendingTasksCount: f.Config.QueuePendingTaskMaxCount,
				PollBackoffInterval:  f.Config.VisibilityProcessorPollBackoffInterval,
				MaxPredicateSize:     f.Config.QueueMaxPredicateSize,
			},
			MonitorOptions: queues.MonitorOptions{
				PendingTasksCriticalCount:   f.Config.QueuePendingTaskCriticalCount,
				ReaderStuckCriticalAttempts: f.Config.QueueReaderStuckCriticalAttempts,
				SliceCountCriticalThreshold: f.Config.QueueCriticalSlicesCount,
			},
			MaxPollRPS:                          f.Config.VisibilityProcessorMaxPollRPS,
			MaxPollInterval:                     f.Config.VisibilityProcessorMaxPollInterval,
			MaxPollIntervalJitterCoefficient:    f.Config.VisibilityProcessorMaxPollIntervalJitterCoefficient,
			CheckpointInterval:                  f.Config.VisibilityProcessorUpdateAckInterval,
			CheckpointIntervalJitterCoefficient: f.Config.VisibilityProcessorUpdateAckIntervalJitterCoefficient,
			MaxReaderCount:                      f.Config.VisibilityQueueMaxReaderCount,
			MoveGroupTaskCountBase:              f.Config.QueueMoveGroupTaskCountBase,
			MoveGroupTaskCountMultiplier:        f.Config.QueueMoveGroupTaskCountMultiplier,
		},
		f.HostReaderRateLimiter,
		queues.GrouperNamespaceID{},
		logger,
		metricsHandler,
		factory,
	)
}
