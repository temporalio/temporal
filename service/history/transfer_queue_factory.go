package history

import (
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/telemetry"
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

		ClientBean        client.Bean
		SdkClientFactory  sdk.ClientFactory
		HistoryRawClient  resource.HistoryRawClient
		MatchingRawClient resource.MatchingRawClient
		VisibilityManager manager.VisibilityManager
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
				},
				params.NamespaceRegistry,
				params.Logger,
			),
			HostPriorityAssigner: queues.NewPriorityAssigner(),
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

	var shardScheduler = f.HostScheduler
	if f.Config.TaskSchedulerEnableRateLimiter() {
		shardScheduler = queues.NewRateLimitedScheduler(
			f.HostScheduler,
			queues.RateLimitedSchedulerOptions{
				EnableShadowMode: f.Config.TaskSchedulerEnableRateLimiterShadowMode,
				StartupDelay:     f.Config.TaskSchedulerRateLimiterStartupDelay,
			},
			currentClusterName,
			f.NamespaceRegistry,
			f.SchedulerRateLimiter,
			f.TimeSource,
			logger,
			metricsHandler,
		)
	}

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
