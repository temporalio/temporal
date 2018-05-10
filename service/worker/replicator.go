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

package worker

import (
	"fmt"

	"github.com/uber-common/bark"
	"github.com/uber/cadence/client/history"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

type (
	// Replicator is the processor for replication tasks
	Replicator struct {
		clusterMetadata  cluster.Metadata
		domainReplicator DomainReplicator
		historyClient    history.Client
		config           *Config
		client           messaging.Client
		processors       []*replicationTaskProcessor
		logger           bark.Logger
		metricsClient    metrics.Client
	}
)

// NewReplicator creates a new replicator for processing replication tasks
func NewReplicator(clusterMetadata cluster.Metadata, metadataManager persistence.MetadataManager,
	historyClient history.Client, config *Config, client messaging.Client, logger bark.Logger,
	metricsClient metrics.Client) *Replicator {
	logger = logger.WithFields(bark.Fields{
		logging.TagWorkflowComponent: logging.TagValueReplicatorComponent,
	})
	return &Replicator{
		clusterMetadata:  clusterMetadata,
		domainReplicator: NewDomainReplicator(metadataManager, logger),
		historyClient:    historyClient,
		config:           config,
		client:           client,
		logger:           logger,
		metricsClient:    metricsClient,
	}
}

// Start is called to start replicator
func (r *Replicator) Start() error {
	currentClusterName := r.clusterMetadata.GetCurrentClusterName()
	for cluster := range r.clusterMetadata.GetAllClusterFailoverVersions() {
		if cluster != currentClusterName {
			consumerName := getConsumerName(currentClusterName, cluster)
			r.processors = append(r.processors, newReplicationTaskProcessor(cluster, consumerName, r.client,
				r.config, r.logger, r.metricsClient, r.domainReplicator, r.historyClient))
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
}

func getConsumerName(currentCluster, remoteCluster string) string {
	return fmt.Sprintf("%v_consumer_for_%v", currentCluster, remoteCluster)
}
