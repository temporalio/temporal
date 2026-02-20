package replication

import (
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/serialization"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/replication/eventhandler"
	"go.temporal.io/server/service/history/shard"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/fx"
)

type (
	ProcessToolBox struct {
		fx.In

		Config                    *configs.Config
		ClusterMetadata           cluster.Metadata
		ClientBean                client.Bean
		ShardController           shard.Controller
		NamespaceCache            namespace.Registry
		EagerNamespaceRefresher   EagerNamespaceRefresher
		ResendHandler             eventhandler.ResendHandler
		HighPriorityTaskScheduler ctasks.Scheduler[TrackableExecutableTask] `name:"HighPriorityTaskScheduler"`
		// consider using a single TaskScheduler i.e. InterleavedWeightedRoundRobinScheduler instead of two
		LowPriorityTaskScheduler ctasks.Scheduler[TrackableExecutableTask] `name:"LowPriorityTaskScheduler"`
		MetricsHandler           metrics.Handler
		Logger                   log.Logger
		ThrottledLogger          log.ThrottledLogger
		Serializer               serialization.Serializer
		DLQWriter                DLQWriter
		HistoryEventsHandler     eventhandler.HistoryEventsHandler
		WorkflowCache            wcache.Cache
		RemoteHistoryFetcher     eventhandler.HistoryPaginatedFetcher
	}
)
