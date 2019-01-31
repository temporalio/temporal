// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package replicator

import (
	"context"
	"fmt"
	"time"

	h "github.com/uber/cadence/.gen/go/history"

	"github.com/uber/cadence/client"
	"github.com/uber/cadence/client/admin"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/xdc"

	"github.com/uber-common/bark"
	"github.com/uber/cadence/client/history"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

type (
	// Replicator is the processor for replication tasks
	Replicator struct {
		domainCache       cache.DomainCache
		clusterMetadata   cluster.Metadata
		domainReplicator  DomainReplicator
		clientBean        client.Bean
		historyClient     history.Client
		config            *Config
		client            messaging.Client
		processors        []*replicationTaskProcessor
		logger            bark.Logger
		metricsClient     metrics.Client
		historySerializer persistence.HistorySerializer
	}

	// Config contains all the replication config for worker
	Config struct {
		PersistenceMaxQPS                  dynamicconfig.IntPropertyFn
		ReplicatorConcurrency              dynamicconfig.IntPropertyFn
		ReplicatorActivityBufferRetryCount dynamicconfig.IntPropertyFn
		ReplicatorHistoryBufferRetryCount  dynamicconfig.IntPropertyFn
		ReplicationTaskMaxRetry            dynamicconfig.IntPropertyFn
	}
)

const (
	replicationTimeout = 30 * time.Second
)

// NewReplicator creates a new replicator for processing replication tasks
func NewReplicator(clusterMetadata cluster.Metadata, metadataManagerV2 persistence.MetadataManager,
	domainCache cache.DomainCache, clientBean client.Bean, config *Config,
	client messaging.Client, logger bark.Logger, metricsClient metrics.Client) *Replicator {

	logger = logger.WithFields(bark.Fields{
		logging.TagWorkflowComponent: logging.TagValueReplicatorComponent,
	})

	return &Replicator{
		domainCache:       domainCache,
		clusterMetadata:   clusterMetadata,
		domainReplicator:  NewDomainReplicator(metadataManagerV2, logger),
		clientBean:        clientBean,
		historyClient:     clientBean.GetHistoryClient(),
		config:            config,
		client:            client,
		logger:            logger,
		metricsClient:     metricsClient,
		historySerializer: persistence.NewHistorySerializer(),
	}
}

// Start is called to start replicator
func (r *Replicator) Start() error {
	currentClusterName := r.clusterMetadata.GetCurrentClusterName()
	for cluster := range r.clusterMetadata.GetAllClusterFailoverVersions() {
		if cluster != currentClusterName {
			consumerName := getConsumerName(currentClusterName, cluster)
			adminClient := admin.NewRetryableClient(
				r.clientBean.GetRemoteAdminClient(cluster),
				common.CreateAdminServiceRetryPolicy(),
				common.IsWhitelistServiceTransientError,
			)
			historyClient := history.NewRetryableClient(
				r.historyClient,
				common.CreateHistoryServiceRetryPolicy(),
				common.IsWhitelistServiceTransientError,
			)
			logger := r.logger.WithFields(bark.Fields{
				logging.TagWorkflowComponent: logging.TagValueReplicationTaskProcessorComponent,
				logging.TagSourceCluster:     cluster,
				logging.TagConsumerName:      consumerName,
			})
			historyRereplicator := xdc.NewHistoryRereplicator(
				currentClusterName,
				r.domainCache,
				adminClient,
				func(ctx context.Context, request *h.ReplicateRawEventsRequest) error {
					return historyClient.ReplicateRawEvents(ctx, request)
				},
				r.historySerializer,
				replicationTimeout,
				logger,
			)
			r.processors = append(r.processors, newReplicationTaskProcessor(currentClusterName, cluster, consumerName, r.client,
				r.config, logger, r.metricsClient, r.domainReplicator, historyRereplicator, r.historyClient))
		}
	}

	for _, processor := range r.processors {
		if err := processor.Start(); err != nil {
			return err
		}
	}

	return nil
}

// Stop is called to stop replicator
func (r *Replicator) Stop() {
	for _, processor := range r.processors {
		processor.Stop()
	}
	r.domainCache.Stop()
}

func getConsumerName(currentCluster, remoteCluster string) string {
	return fmt.Sprintf("%v_consumer_for_%v", currentCluster, remoteCluster)
}
