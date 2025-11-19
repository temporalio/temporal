package history

import (
	"context"

	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/quotas/calculator"
	"go.temporal.io/server/service/history/circuitbreakerpool"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/replication/eventhandler"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/fx"
)

const QueueFactoryFxGroup = "queueFactory"

type (
	QueueFactory interface {
		Start()
		Stop()

		// TODO: Move this interface to queues package
		CreateQueue(shardContext historyi.ShardContext) queues.Queue
	}

	QueueFactoryBaseParams struct {
		fx.In

		NamespaceRegistry    namespace.Registry
		ClusterMetadata      cluster.Metadata
		WorkflowCache        wcache.Cache
		Config               *configs.Config
		TimeSource           clock.TimeSource
		MetricsHandler       metrics.Handler
		TracerProvider       trace.TracerProvider
		Logger               log.SnTaggedLogger
		SchedulerRateLimiter queues.SchedulerRateLimiter
		DLQWriter            *queues.DLQWriter
		ExecutorWrapper      queues.ExecutorWrapper `optional:"true"`
		Serializer           serialization.Serializer
		RemoteHistoryFetcher eventhandler.HistoryPaginatedFetcher
		ChasmEngine          chasm.Engine
		ChasmRegistry        *chasm.Registry
	}

	QueueFactoryBase struct {
		HostScheduler         queues.Scheduler
		HostPriorityAssigner  queues.PriorityAssigner
		HostReaderRateLimiter quotas.RequestRateLimiter
		Tracer                trace.Tracer
	}

	QueueFactoriesLifetimeHookParams struct {
		fx.In

		Lifecycle fx.Lifecycle
		Factories []QueueFactory `group:"queueFactory"`
	}
)

var QueueModule = fx.Options(
	circuitbreakerpool.Module,
	fx.Provide(
		QueueSchedulerRateLimiterProvider,
		func(tqm persistence.HistoryTaskQueueManager) queues.QueueWriter {
			return tqm
		},
		queues.NewDLQWriter,
		fx.Annotated{
			Group:  QueueFactoryFxGroup,
			Target: NewTransferQueueFactory,
		},
		fx.Annotated{
			Group:  QueueFactoryFxGroup,
			Target: NewTimerQueueFactory,
		},
		fx.Annotated{
			Group:  QueueFactoryFxGroup,
			Target: NewVisibilityQueueFactory,
		},
		fx.Annotated{
			Group:  QueueFactoryFxGroup,
			Target: NewMemoryScheduledQueueFactory,
		},
		getOptionalQueueFactories,
	),
	fx.Invoke(QueueFactoryLifetimeHooks),
)

// additionalQueueFactories is a container for a list of queue factories that are only added to the group if
// they are enabled. This exists because there is no way to conditionally add to a group with a provider that returns
// a single object. For example, this doesn't work because it will always add the factory to the group, which can
// cause NPEs:
//
//	fx.Annotated{
//	  Group: "queueFactory",
//	  Target: func() QueueFactory { return isEnabled ? NewQueueFactory() : nil },
//	},
type additionalQueueFactories struct {
	// This is what tells fx to add the factories to the group whenever this object is provided.
	fx.Out

	// Factories is a list of queue factories that will be added to the `group:"queueFactory"` group.
	Factories []QueueFactory `group:"queueFactory,flatten"`
}

// getOptionalQueueFactories returns an additionalQueueFactories which contains a list of queue factories that will be
// added to the `group:"queueFactory"` group. The factories are added to the group only if they are enabled, which
// is why we must return a list here.
func getOptionalQueueFactories(
	registry tasks.TaskCategoryRegistry,
	archivalParams ArchivalQueueFactoryParams,
	outboundParams outboundQueueFactoryParams,
	config *configs.Config,
) additionalQueueFactories {
	factories := []QueueFactory{}
	if _, ok := registry.GetCategoryByID(tasks.CategoryIDArchival); ok {
		factories = append(factories, NewArchivalQueueFactory(archivalParams))
	}
	if config.EnableNexus() {
		factories = append(factories, NewOutboundQueueFactory(outboundParams))
	}
	return additionalQueueFactories{
		Factories: factories,
	}
}

func QueueSchedulerRateLimiterProvider(
	ownershipBasedQuotaScaler shard.LazyLoadedOwnershipBasedQuotaScaler,
	serviceResolver membership.ServiceResolver,
	config *configs.Config,
	timeSource clock.TimeSource,
	logger log.SnTaggedLogger,
) (queues.SchedulerRateLimiter, error) {
	return queues.NewPrioritySchedulerRateLimiter(
		calculator.NewLoggedNamespaceCalculator(
			shard.NewOwnershipAwareNamespaceQuotaCalculator(
				ownershipBasedQuotaScaler,
				serviceResolver,
				config.TaskSchedulerNamespaceMaxQPS,
				config.TaskSchedulerGlobalNamespaceMaxQPS,
			),
			log.With(logger, tag.ComponentTaskScheduler, tag.ScopeNamespace),
		).GetQuota,
		calculator.NewLoggedCalculator(
			shard.NewOwnershipAwareQuotaCalculator(
				ownershipBasedQuotaScaler,
				serviceResolver,
				config.TaskSchedulerMaxQPS,
				config.TaskSchedulerGlobalMaxQPS,
			),
			log.With(logger, tag.ComponentTaskScheduler, tag.ScopeHost),
		).GetQuota,
		// TODO: reuse persistence rate limit calculator in PersistenceRateLimitingParamsProvider
		shard.NewOwnershipAwareNamespaceQuotaCalculator(
			ownershipBasedQuotaScaler,
			serviceResolver,
			config.PersistenceNamespaceMaxQPS,
			config.PersistenceGlobalNamespaceMaxQPS,
		).GetQuota,
		shard.NewOwnershipAwareQuotaCalculator(
			ownershipBasedQuotaScaler,
			serviceResolver,
			config.PersistenceMaxQPS,
			config.PersistenceGlobalMaxQPS,
		).GetQuota,
	)
}

func QueueFactoryLifetimeHooks(
	params QueueFactoriesLifetimeHookParams,
) {
	params.Lifecycle.Append(
		fx.Hook{
			OnStart: func(context.Context) error {
				for _, factory := range params.Factories {
					factory.Start()
				}
				return nil
			},
			OnStop: func(context.Context) error {
				for _, factory := range params.Factories {
					factory.Stop()
				}
				return nil
			},
		},
	)
}

func (f *QueueFactoryBase) Start() {
	if f.HostScheduler != nil {
		f.HostScheduler.Start()
	}
}

func (f *QueueFactoryBase) Stop() {
	if f.HostScheduler != nil {
		f.HostScheduler.Stop()
	}
}

func NewHostRateLimiterRateFn(
	hostRPS dynamicconfig.IntPropertyFn,
	persistenceMaxRPS dynamicconfig.IntPropertyFn,
	persistenceMaxRPSRatio float64,
) quotas.RateFn {
	// TODO: reuse persistence rate limit calculator in PersistenceRateLimitingParamsProvider

	return func() float64 {
		if maxPollHostRps := hostRPS(); maxPollHostRps > 0 {
			return float64(maxPollHostRps)
		}

		// ensure queue loading won't consume all persistence tokens
		// especially upon host restart when we need to perform a load
		// for all shards
		return float64(persistenceMaxRPS()) * persistenceMaxRPSRatio
	}
}
