package history

import (
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/sdk"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/worker_versioning"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/fx"
)

const (
	transferQueuePersistenceMaxRPSRatio = 0.3
)

type (
	transferQueueFactoryParams struct {
		fx.In

		QueueFactoryBaseParams

		ClientBean             client.Bean
		SdkClientFactory       sdk.ClientFactory
		HistoryRawClient       resource.HistoryRawClient
		MatchingRawClient      resource.MatchingRawClient
		VisibilityManager      manager.VisibilityManager
		VersionMembershipCache worker_versioning.VersionMembershipCache
	}

	transferQueueFactory struct {
		transferQueueFactoryParams
		QueueFactoryBase
	}
)

func NewTransferQueueFactory(
	params transferQueueFactoryParams,
) QueueFactory {
	return &transferQueueFactory{
		transferQueueFactoryParams: params,
		QueueFactoryBase: QueueFactoryBase{
			HostScheduler: queues.NewScheduler(
				params.ClusterMetadata.GetCurrentClusterName(),
				queues.SchedulerOptions{
					WorkerCount:                    params.Config.TransferProcessorSchedulerWorkerCount,
					ActiveNamespaceWeights:         params.Config.TransferProcessorSchedulerActiveRoundRobinWeights,
					StandbyNamespaceWeights:        params.Config.TransferProcessorSchedulerStandbyRoundRobinWeights,
					InactiveNamespaceDeletionDelay: params.Config.TaskSchedulerInactiveChannelDeletionDelay,
					ExecutionAwareSchedulerOptions: ctasks.ExecutionAwareSchedulerOptions{
						Enabled:          params.Config.TaskSchedulerEnableExecutionQueueScheduler,
						MaxQueues:        params.Config.TaskSchedulerExecutionQueueSchedulerMaxQueues,
						QueueTTL:         params.Config.TaskSchedulerExecutionQueueSchedulerQueueTTL,
						QueueConcurrency: params.Config.TaskSchedulerExecutionQueueSchedulerQueueConcurrency,
					},
				},
				params.NamespaceRegistry,
				params.Logger,
				params.MetricsHandler,
				params.TimeSource,
			),
			HostPriorityAssigner: queues.NewPriorityAssigner(
				params.NamespaceRegistry,
				params.ClusterMetadata.GetCurrentClusterName(),
			),
			HostReaderRateLimiter: queues.NewReaderPriorityRateLimiter(
				NewHostRateLimiterRateFn(
					params.Config.TransferProcessorMaxPollHostRPS,
					params.Config.PersistenceMaxQPS,
					transferQueuePersistenceMaxRPSRatio,
				),
				int64(params.Config.TransferQueueMaxReaderCount()),
			),
			Tracer: params.TracerProvider.Tracer(telemetry.ComponentQueueTransfer),
		},
	}
}

func (f *transferQueueFactory) CreateQueue(
	shardContext historyi.ShardContext,
) queues.Queue {
	logger := log.With(shardContext.GetLogger(), tag.ComponentTransferQueue)
	metricsHandler := f.MetricsHandler.WithTags(metrics.OperationTag(metrics.OperationTransferQueueProcessorScope))

	currentClusterName := f.ClusterMetadata.GetCurrentClusterName()

	shardScheduler := queues.NewRateLimitedScheduler(
		f.HostScheduler,
		queues.RateLimitedSchedulerOptions{
			Enabled:          f.Config.TaskSchedulerEnableRateLimiter,
			EnableShadowMode: f.Config.TaskSchedulerEnableRateLimiterShadowMode,
			StartupDelay:     f.Config.TaskSchedulerRateLimiterStartupDelay,
		},
		currentClusterName,
		f.NamespaceRegistry,
		f.SchedulerRateLimiter,
		f.TimeSource,
		f.ChasmRegistry,
		logger,
		metricsHandler,
	)

	rescheduler := queues.NewRescheduler(
		shardScheduler,
		shardContext.GetTimeSource(),
		logger,
		metricsHandler,
	)

	activeExecutor := newTransferQueueActiveTaskExecutor(
		shardContext,
		f.WorkflowCache,
		f.SdkClientFactory,
		logger,
		f.MetricsHandler,
		f.Config,
		f.HistoryRawClient,
		f.MatchingRawClient,
		f.VisibilityManager,
		f.ChasmEngine,
		f.VersionMembershipCache,
	)

	standbyExecutor := newTransferQueueStandbyTaskExecutor(
		shardContext,
		f.WorkflowCache,
		logger,
		f.MetricsHandler,
		currentClusterName,
		f.HistoryRawClient,
		f.MatchingRawClient,
		f.VisibilityManager,
		f.ChasmEngine,
		f.ClientBean,
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
		shardScheduler,
		rescheduler,
		f.HostPriorityAssigner,
		shardContext.GetTimeSource(),
		shardContext.GetNamespaceRegistry(),
		shardContext.GetClusterMetadata(),
		f.ChasmRegistry,
		queues.GetTaskTypeTagValue,
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
		shardContext,
		tasks.CategoryTransfer,
		shardScheduler,
		rescheduler,
		&queues.Options{
			ReaderOptions: queues.ReaderOptions{
				BatchSize:            f.Config.TransferTaskBatchSize,
				MaxPendingTasksCount: f.Config.QueuePendingTaskMaxCount,
				PollBackoffInterval:  f.Config.TransferProcessorPollBackoffInterval,
				MaxPredicateSize:     f.Config.QueueMaxPredicateSize,
			},
			MonitorOptions: queues.MonitorOptions{
				PendingTasksCriticalCount:   f.Config.QueuePendingTaskCriticalCount,
				ReaderStuckCriticalAttempts: f.Config.QueueReaderStuckCriticalAttempts,
				SliceCountCriticalThreshold: f.Config.QueueCriticalSlicesCount,
			},
			MaxPollRPS:                          f.Config.TransferProcessorMaxPollRPS,
			MaxPollInterval:                     f.Config.TransferProcessorMaxPollInterval,
			MaxPollIntervalJitterCoefficient:    f.Config.TransferProcessorMaxPollIntervalJitterCoefficient,
			CheckpointInterval:                  f.Config.TransferProcessorUpdateAckInterval,
			CheckpointIntervalJitterCoefficient: f.Config.TransferProcessorUpdateAckIntervalJitterCoefficient,
			MaxReaderCount:                      f.Config.TransferQueueMaxReaderCount,
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
