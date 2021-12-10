// FIXME: copyright

package shard

import (
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/history/configs"
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(ShardControllerProvider),
)

func ShardControllerProvider(
	config *configs.Config,
	logger log.Logger,
	throttledLogger log.Logger,
	persistenceExecutionManager persistence.ExecutionManager,
	persistenceShardManager persistence.ShardManager,
	clientBean client.Bean,
	historyClient historyservice.HistoryServiceClient,
	historyServiceResolver membership.ServiceResolver,
	metricsClient metrics.Client,
	payloadSerializer serialization.Serializer,
	timeSource clock.TimeSource,
	namespaceRegistry namespace.Registry,
	saProvider searchattribute.Provider,
	saMapper searchattribute.Mapper,
	clusterMetadata cluster.Metadata,
	archivalMetadata archiver.ArchivalMetadata,
	hostInfoProvider resource.HostInfoProvider,
) *ControllerImpl {
	hostIdentity := hostInfoProvider.HostInfo().Identity()
	return &ControllerImpl{
		status:                      common.DaemonStatusInitialized,
		membershipUpdateCh:          make(chan *membership.ChangedEvent, 10),
		historyShards:               make(map[int32]*ContextImpl),
		shutdownCh:                  make(chan struct{}),
		logger:                      logger,
		contextTaggedLogger:         log.With(logger, tag.ComponentShardController, tag.Address(hostIdentity)),
		throttledLogger:             log.With(throttledLogger, tag.ComponentShardController, tag.Address(hostIdentity)),
		config:                      config,
		metricsScope:                metricsClient.Scope(metrics.HistoryShardControllerScope),
		persistenceExecutionManager: persistenceExecutionManager,
		persistenceShardManager:     persistenceShardManager,
		clientBean:                  clientBean,
		historyClient:               historyClient,
		historyServiceResolver:      historyServiceResolver,
		metricsClient:               metricsClient,
		payloadSerializer:           payloadSerializer,
		timeSource:                  timeSource,
		namespaceRegistry:           namespaceRegistry,
		saProvider:                  saProvider,
		saMapper:                    saMapper,
		clusterMetadata:             clusterMetadata,
		archivalMetadata:            archivalMetadata,
		hostInfoProvider:            hostInfoProvider,
	}
}
