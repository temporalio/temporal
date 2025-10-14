package history

import (
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/service/history/deletemanager"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/fx"
)

const (
	timerQueuePersistenceMaxRPSRatio = 0.3
)

type (
	timerQueueFactoryParams struct {
		fx.In

		QueueFactoryBaseParams
		ClientBean        client.Bean
		MatchingRawClient resource.MatchingRawClient
		VisibilityManager manager.VisibilityManager
	}

	timerQueueFactory struct {
		timerQueueFactoryParams
		QueueFactoryBase
	}
)

func NewTimerQueueFactory(
	params timerQueueFactoryParams,
) QueueFactory {
	return &timerQueueFactory{
		timerQueueFactoryParams: params,
		QueueFactoryBase: QueueFactoryBase{
			HostScheduler: queues.NewScheduler(
				params.ClusterMetadata.GetCurrentClusterName(),
				queues.SchedulerOptions{
					WorkerCount:                    params.Config.TimerProcessorSchedulerWorkerCount,
					ActiveNamespaceWeights:         params.Config.TimerProcessorSchedulerActiveRoundRobinWeights,
					StandbyNamespaceWeights:        params.Config.TimerProcessorSchedulerStandbyRoundRobinWeights,
					InactiveNamespaceDeletionDelay: params.Config.TaskSchedulerInactiveChannelDeletionDelay,
				},
				params.NamespaceRegistry,
				params.Logger,
			),
			HostPriorityAssigner: queues.NewPriorityAssigner(),
			HostReaderRateLimiter: queues.NewReaderPriorityRateLimiter(
				NewHostRateLimiterRateFn(
					params.Config.TimerProcessorMaxPollHostRPS,
					params.Config.PersistenceMaxQPS,
					timerQueuePersistenceMaxRPSRatio,
				),
				int64(params.Config.TimerQueueMaxReaderCount()),
			),
			Tracer: params.TracerProvider.Tracer(telemetry.ComponentQueueTimer),
		},
	}
}

func (f *timerQueueFactory) CreateQueue(
	shardContext historyi.ShardContext,
) queues.Queue {
	logger := log.With(shardContext.GetLogger(), tag.ComponentTimerQueue)
	metricsHandler := f.MetricsHandler.WithTags(metrics.OperationTag(metrics.OperationTimerQueueProcessorScope))

	currentClusterName := f.ClusterMetadata.GetCurrentClusterName()
	workflowDeleteManager := deletemanager.NewDeleteManager(
		shardContext,
		f.WorkflowCache,
		f.Config,
		shardContext.GetTimeSource(),
		f.VisibilityManager,
	)

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

	activeExecutor := newTimerQueueActiveTaskExecutor(
		shardContext,
		f.WorkflowCache,
		workflowDeleteManager,
		logger,
		f.MetricsHandler,
		f.Config,
		f.MatchingRawClient,
		f.ChasmEngine,
	)

	standbyExecutor := newTimerQueueStandbyTaskExecutor(
		shardContext,
		f.WorkflowCache,
		workflowDeleteManager,
		f.MatchingRawClient,
		f.ChasmEngine,
		logger,
		f.MetricsHandler,
		// note: the cluster name is for calculating time for standby tasks,
		// here we are basically using current cluster time
		// this field will be deprecated soon, currently exists so that
		// we have the option of revert to old behavior
		currentClusterName,
		f.Config,
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
	return queues.NewScheduledQueue(
		shardContext,
		tasks.CategoryTimer,
		shardScheduler,
		rescheduler,
		factory,
		&queues.Options{
			ReaderOptions: queues.ReaderOptions{
				BatchSize:            f.Config.TimerTaskBatchSize,
				MaxPendingTasksCount: f.Config.QueuePendingTaskMaxCount,
				PollBackoffInterval:  f.Config.TimerProcessorPollBackoffInterval,
				MaxPredicateSize:     f.Config.QueueMaxPredicateSize,
			},
			MonitorOptions: queues.MonitorOptions{
				PendingTasksCriticalCount:   f.Config.QueuePendingTaskCriticalCount,
				ReaderStuckCriticalAttempts: f.Config.QueueReaderStuckCriticalAttempts,
				SliceCountCriticalThreshold: f.Config.QueueCriticalSlicesCount,
			},
			MaxPollRPS:                          f.Config.TimerProcessorMaxPollRPS,
			MaxPollInterval:                     f.Config.TimerProcessorMaxPollInterval,
			MaxPollIntervalJitterCoefficient:    f.Config.TimerProcessorMaxPollIntervalJitterCoefficient,
			CheckpointInterval:                  f.Config.TimerProcessorUpdateAckInterval,
			CheckpointIntervalJitterCoefficient: f.Config.TimerProcessorUpdateAckIntervalJitterCoefficient,
			MaxReaderCount:                      f.Config.TimerQueueMaxReaderCount,
			MoveGroupTaskCountBase:              f.Config.QueueMoveGroupTaskCountBase,
			MoveGroupTaskCountMultiplier:        f.Config.QueueMoveGroupTaskCountMultiplier,
		},
		f.HostReaderRateLimiter,
		logger,
		metricsHandler,
	)
}
