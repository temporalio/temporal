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

		ArchivalMetadata       archiver.ArchivalMetadata
		ClientBean             client.Bean
		ClusterMetadata        cluster.Metadata
		Config                 *configs.Config
		PersistenceConfig      config.Persistence
		EngineFactory          EngineFactory
		HistoryClient          resource.HistoryClient
		HistoryServiceResolver membership.ServiceResolver
		HostInfoProvider       membership.HostInfoProvider
		BaseLogger             log.Logger
		BaseThrottledLogger    log.ThrottledLogger
		MetricsHandler         metrics.Handler
		NamespaceRegistry      namespace.Registry
		PayloadSerializer      serialization.Serializer
		ExecutionManager       persistence.ExecutionManager
		ShardManager           persistence.ShardManager
		SaMapperProvider       searchattribute.MapperProvider
		SaProvider             searchattribute.Provider
		TimeSource             clock.TimeSource
		TaskCategoryRegistry   tasks.TaskCategoryRegistry
		HostLevelEventsCache   events.Cache
		StateMachineRegistry   *hsm.Registry
		ChasmRegistry          *chasm.Registry
	}

	contextFactoryImpl struct {
		params ContextFactoryParams
	}
)

func ContextFactoryProvider(params ContextFactoryParams) ContextFactory {
	return &contextFactoryImpl{params: params}
}

func (c *contextFactoryImpl) CreateContext(
	shardID int32,
	closeCallback CloseCallback,
) (historyi.ControllableContext, error) {
	var shard *ContextImpl
	app := fx.New(
		shardContextFx,
		fx.Supply(shardID),
		fx.Supply(closeCallback),
		fx.Supply(MakeFxOut(c.params)), // reflection magic to supply all fields individually
		fx.Populate(&shard),
	)
	// Note that we do not call app.Start() or app.Run(), we're not using the lifecycle
	// features here (yet?).
	if app.Err() != nil {
		return nil, app.Err()
	}
	shard.start()
	return shard, nil
}
