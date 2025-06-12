package history

import (
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/server/common/fxutil"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/service/history/circuitbreakerpool"
	"go.temporal.io/server/service/history/events"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/replication"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/fx"
)

type (
	HistoryEngineFactoryParams struct {
		fx.In

		MatchingClient                  resource.MatchingClient
		RawMatchingClient               resource.MatchingRawClient
		EventNotifier                   events.Notifier
		WorkflowCache                   wcache.Cache
		ReplicationProgressCache        replication.ProgressCache
		QueueFactories                  []QueueFactory `group:"queueFactory"`
		ReplicationTaskFetcherFactory   replication.TaskFetcherFactory
		ReplicationTaskExecutorProvider replication.TaskExecutorProvider
		TracerProvider                  trace.TracerProvider
		PersistenceVisibilityMgr        manager.VisibilityManager
		EventBlobCache                  persistence.XDCCache
		ReplicationDLQWriter            replication.DLQWriter
		CommandHandlerRegistry          *workflow.CommandHandlerRegistry
		OutboundQueueCBPool             *circuitbreakerpool.OutboundQueueCircuitBreakerPool
		TestHooks                       testhooks.TestHooks
	}

	historyEngineFactory struct {
		HistoryEngineFactoryParams
	}
)

func (f *historyEngineFactory) CreateEngine(
	shard historyi.ShardContext,
) historyi.Engine {
	globalOptions := fx.Options()
	if shard.GetClusterMetadata().IsGlobalNamespaceEnabled() {
		globalOptions = globalEngineFx
	}

	var engine *historyEngineImpl
	app := fx.New(
		engineFx,
		fx.Supply(fx.Annotate(shard, fx.As(new(historyi.ShardContext)))),
		shard.SupplyAllDependencies(),                        // reflection magic to supply all shard dependencies individually
		fxutil.SupplyAllFields(f.HistoryEngineFactoryParams), // reflection magic to supply all engine dependencies individually
		globalOptions,
		fx.Populate(&engine),
	)
	if app.Err() != nil {
		panic("history engine init: " + app.Err().Error())
	}
	// Note that we do not call app.Start() or app.Run(), we're not using the lifecycle features here (yet?).
	return engine
}
