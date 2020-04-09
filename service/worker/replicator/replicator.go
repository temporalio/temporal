package replicator

import (
	"context"
	"fmt"

	"github.com/temporalio/temporal/.gen/proto/historyservice"
	"github.com/temporalio/temporal/client"
	"github.com/temporalio/temporal/client/admin"
	"github.com/temporalio/temporal/client/history"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/membership"
	"github.com/temporalio/temporal/common/messaging"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/namespace"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/service/config"
	"github.com/temporalio/temporal/common/service/dynamicconfig"
	"github.com/temporalio/temporal/common/task"
	"github.com/temporalio/temporal/common/xdc"
)

type (
	// Replicator is the processor for replication tasks
	Replicator struct {
		namespaceCache                   cache.NamespaceCache
		clusterMetadata                  cluster.Metadata
		namespaceReplicationTaskExecutor namespace.ReplicationTaskExecutor
		clientBean                       client.Bean
		historyClient                    history.Client
		config                           *Config
		client                           messaging.Client
		processors                       []*replicationTaskProcessor
		namespaceProcessors              []*namespaceReplicationMessageProcessor
		logger                           log.Logger
		metricsClient                    metrics.Client
		historySerializer                persistence.PayloadSerializer
		hostInfo                         *membership.HostInfo
		serviceResolver                  membership.ServiceResolver
		namespaceReplicationQueue        persistence.NamespaceReplicationQueue
	}

	// Config contains all the replication config for worker
	Config struct {
		PersistenceMaxQPS                  dynamicconfig.IntPropertyFn
		ReplicatorMetaTaskConcurrency      dynamicconfig.IntPropertyFn
		ReplicatorTaskConcurrency          dynamicconfig.IntPropertyFn
		ReplicatorMessageConcurrency       dynamicconfig.IntPropertyFn
		ReplicatorActivityBufferRetryCount dynamicconfig.IntPropertyFn
		ReplicatorHistoryBufferRetryCount  dynamicconfig.IntPropertyFn
		ReplicationTaskMaxRetryCount       dynamicconfig.IntPropertyFn
		ReplicationTaskMaxRetryDuration    dynamicconfig.DurationPropertyFn
		ReplicationTaskContextTimeout      dynamicconfig.DurationPropertyFn
	}
)

// NewReplicator creates a new replicator for processing replication tasks
func NewReplicator(
	clusterMetadata cluster.Metadata,
	metadataManagerV2 persistence.MetadataManager,
	namespaceCache cache.NamespaceCache,
	clientBean client.Bean,
	config *Config,
	client messaging.Client,
	logger log.Logger,
	metricsClient metrics.Client,
	hostInfo *membership.HostInfo,
	serviceResolver membership.ServiceResolver,
	namespaceReplicationQueue persistence.NamespaceReplicationQueue,
	namespaceReplicationTaskExecutor namespace.ReplicationTaskExecutor,
) *Replicator {

	logger = logger.WithTags(tag.ComponentReplicator)
	return &Replicator{
		hostInfo:                         hostInfo,
		serviceResolver:                  serviceResolver,
		namespaceCache:                   namespaceCache,
		clusterMetadata:                  clusterMetadata,
		namespaceReplicationTaskExecutor: namespaceReplicationTaskExecutor,
		clientBean:                       clientBean,
		historyClient:                    clientBean.GetHistoryClient(),
		config:                           config,
		client:                           client,
		logger:                           logger,
		metricsClient:                    metricsClient,
		historySerializer:                persistence.NewPayloadSerializer(),
		namespaceReplicationQueue:        namespaceReplicationQueue,
	}
}

// Start is called to start replicator
func (r *Replicator) Start() error {
	currentClusterName := r.clusterMetadata.GetCurrentClusterName()
	replicationConsumerConfig := r.clusterMetadata.GetReplicationConsumerConfig()
	for clusterName, info := range r.clusterMetadata.GetAllClusterInfo() {
		if !info.Enabled {
			continue
		}

		if clusterName != currentClusterName {
			if replicationConsumerConfig.Type == config.ReplicationConsumerTypeRPC {
				processor := newNamespaceReplicationMessageProcessor(
					clusterName,
					r.logger.WithTags(tag.ComponentReplicationTaskProcessor, tag.SourceCluster(clusterName)),
					r.clientBean.GetRemoteAdminClient(clusterName),
					r.metricsClient,
					r.namespaceReplicationTaskExecutor,
					r.hostInfo,
					r.serviceResolver,
					r.namespaceReplicationQueue,
				)
				r.namespaceProcessors = append(r.namespaceProcessors, processor)
			} else {
				r.createKafkaProcessors(currentClusterName, clusterName)
			}
		}
	}

	for _, processor := range r.processors {
		if err := processor.Start(); err != nil {
			return err
		}
	}

	for _, namespaceProcessor := range r.namespaceProcessors {
		namespaceProcessor.Start()
	}

	return nil
}

func (r *Replicator) createKafkaProcessors(currentClusterName string, clusterName string) {
	consumerName := getConsumerName(currentClusterName, clusterName)
	adminClient := admin.NewRetryableClient(
		r.clientBean.GetRemoteAdminClient(clusterName),
		common.CreateAdminServiceRetryPolicy(),
		common.IsWhitelistServiceTransientError,
	)
	historyClient := history.NewRetryableClient(
		r.historyClient,
		common.CreateHistoryServiceRetryPolicy(),
		common.IsWhitelistServiceTransientError,
	)
	logger := r.logger.WithTags(tag.ComponentReplicationTaskProcessor, tag.SourceCluster(clusterName), tag.KafkaConsumerName(consumerName))
	historyRereplicator := xdc.NewHistoryRereplicator(
		currentClusterName,
		r.namespaceCache,
		adminClient,
		func(ctx context.Context, request *historyservice.ReplicateRawEventsRequest) error {
			_, err := historyClient.ReplicateRawEvents(ctx, request)
			return err
		},
		r.historySerializer,
		r.config.ReplicationTaskContextTimeout(),
		r.logger,
	)
	nDCHistoryReplicator := xdc.NewNDCHistoryResender(
		r.namespaceCache,
		adminClient,
		func(ctx context.Context, request *historyservice.ReplicateEventsV2Request) error {
			_, err := historyClient.ReplicateEventsV2(ctx, request)
			return err
		},
		r.historySerializer,
		logger,
	)
	r.processors = append(r.processors, newReplicationTaskProcessor(
		currentClusterName,
		clusterName,
		consumerName,
		r.client,
		r.config,
		logger,
		r.metricsClient,
		r.namespaceReplicationTaskExecutor,
		historyRereplicator,
		nDCHistoryReplicator,
		r.historyClient,
		r.namespaceCache,
		task.NewSequentialTaskProcessor(
			r.config.ReplicatorTaskConcurrency(),
			replicationSequentialTaskQueueHashFn,
			newReplicationSequentialTaskQueue,
			r.metricsClient,
			logger,
		),
	))
}

// Stop is called to stop replicator
func (r *Replicator) Stop() {
	for _, processor := range r.processors {
		processor.Stop()
	}

	for _, namespaceProcessor := range r.namespaceProcessors {
		namespaceProcessor.Stop()
	}

	r.namespaceCache.Stop()
}

func getConsumerName(currentCluster, remoteCluster string) string {
	return fmt.Sprintf("%v_consumer_for_%v", currentCluster, remoteCluster)
}
