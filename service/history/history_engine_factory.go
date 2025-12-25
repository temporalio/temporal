package history

import (
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/circuitbreakerpool"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/events"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/replication"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/fx"
)

type (
	HistoryEngineFactoryParams struct {
		fx.In

		ClientBean                      client.Bean
		MatchingClient                  resource.MatchingClient
		SdkClientFactory                sdk.ClientFactory
		EventNotifier                   events.Notifier
		Config                          *configs.Config
		RawMatchingClient               resource.MatchingRawClient
		WorkflowCache                   wcache.Cache
		ReplicationProgressCache        replication.ProgressCache
		EventSerializer                 serialization.Serializer
		QueueFactories                  []QueueFactory `group:"queueFactory"`
		ReplicationTaskFetcherFactory   replication.TaskFetcherFactory
		ReplicationTaskExecutorProvider replication.TaskExecutorProvider
		TracerProvider                  trace.TracerProvider
		PersistenceVisibilityMgr        manager.VisibilityManager
		EventBlobCache                  persistence.XDCCache
		TaskCategoryRegistry            tasks.TaskCategoryRegistry
		ReplicationDLQWriter            replication.DLQWriter
		CommandHandlerRegistry          *workflow.CommandHandlerRegistry
		OutboundQueueCBPool             *circuitbreakerpool.OutboundQueueCircuitBreakerPool
		PersistenceRateLimiter          replication.PersistenceRateLimiter
		TestHooks                       testhooks.TestHooks
		ChasmEngine                     chasm.Engine
		VersionMembershipCache          worker_versioning.VersionMembershipCache
	}

	historyEngineFactory struct {
		HistoryEngineFactoryParams
	}
)

func (f *historyEngineFactory) CreateEngine(
	shard historyi.ShardContext,
) historyi.Engine {
	return NewEngineWithShardContext(
		shard,
		f.ClientBean,
		f.MatchingClient,
		f.SdkClientFactory,
		f.EventNotifier,
		f.Config,
		f.VersionMembershipCache,
		f.RawMatchingClient,
		f.WorkflowCache,
		f.ReplicationProgressCache,
		f.EventSerializer,
		f.QueueFactories,
		f.ReplicationTaskFetcherFactory,
		f.ReplicationTaskExecutorProvider,
		api.NewWorkflowConsistencyChecker(shard, f.WorkflowCache),
		f.TracerProvider,
		f.PersistenceVisibilityMgr,
		f.EventBlobCache,
		f.TaskCategoryRegistry,
		f.ReplicationDLQWriter,
		f.CommandHandlerRegistry,
		f.OutboundQueueCBPool,
		f.PersistenceRateLimiter,
		f.TestHooks,
		f.ChasmEngine,
	)
}
