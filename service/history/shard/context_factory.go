package shard

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/fx"
)

//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination context_factory_mock.go

type (
	CloseCallback func(historyi.ControllableContext)

	ContextFactory interface {
		CreateContext(shardID int32, closeCallback CloseCallback) (historyi.ControllableContext, error)
	}

	ContextFactoryParams struct {
		fx.In

		ArchivalMetadata            archiver.ArchivalMetadata
		ClientBean                  client.Bean
		ClusterMetadata             cluster.Metadata
		Config                      *configs.Config
		PersistenceConfig           config.Persistence
		EngineFactory               EngineFactory
		HistoryClient               resource.HistoryClient
		HistoryServiceResolver      membership.ServiceResolver
		HostInfoProvider            membership.HostInfoProvider
		Logger                      log.Logger
		MetricsHandler              metrics.Handler
		NamespaceRegistry           namespace.Registry
		PayloadSerializer           serialization.Serializer
		PersistenceExecutionManager persistence.ExecutionManager
		PersistenceShardManager     persistence.ShardManager
		SaMapperProvider            searchattribute.MapperProvider
		SaProvider                  searchattribute.Provider
		ThrottledLogger             log.ThrottledLogger
		TimeSource                  clock.TimeSource
		TaskCategoryRegistry        tasks.TaskCategoryRegistry
		EventsCache                 events.Cache

		StateMachineRegistry *hsm.Registry
		ChasmRegistry        *chasm.Registry
	}

	contextFactoryImpl struct {
		*ContextFactoryParams
	}
)

func ContextFactoryProvider(params ContextFactoryParams) ContextFactory {
	return &contextFactoryImpl{
		ContextFactoryParams: &params,
	}
}

func (c *contextFactoryImpl) CreateContext(
	shardID int32,
	closeCallback CloseCallback,
) (historyi.ControllableContext, error) {
	shard, err := newContext(
		shardID,
		c.EngineFactory,
		c.Config,
		c.PersistenceConfig,
		closeCallback,
		c.Logger,
		c.ThrottledLogger,
		c.PersistenceExecutionManager,
		c.PersistenceShardManager,
		c.ClientBean,
		c.HistoryClient,
		c.MetricsHandler,
		c.PayloadSerializer,
		c.TimeSource,
		c.NamespaceRegistry,
		c.SaProvider,
		c.SaMapperProvider,
		c.ClusterMetadata,
		c.ArchivalMetadata,
		c.HostInfoProvider,
		c.TaskCategoryRegistry,
		c.EventsCache,
		c.StateMachineRegistry,
		c.ChasmRegistry,
	)
	if err != nil {
		return nil, err
	}
	shard.start()
	return shard, nil
}
