// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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
	"fmt"
	"sync"
	"sync/atomic"

	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
)

type (
	// Replicator is the processor for replication tasks
	Replicator struct {
		status                           int32
		clusterMetadata                  cluster.Metadata
		namespaceReplicationTaskExecutor namespace.ReplicationTaskExecutor
		clientBean                       client.Bean
		logger                           log.Logger
		metricsClient                    metrics.Client
		hostInfo                         *membership.HostInfo
		serviceResolver                  membership.ServiceResolver
		namespaceReplicationQueue        persistence.NamespaceReplicationQueue

		namespaceProcessorsLock sync.Mutex
		namespaceProcessors     map[string]*namespaceReplicationMessageProcessor
	}

	// Config contains all the replication config for worker
	Config struct {
	}
)

// NewReplicator creates a new replicator for processing replication tasks
func NewReplicator(
	clusterMetadata cluster.Metadata,
	clientBean client.Bean,
	logger log.Logger,
	metricsClient metrics.Client,
	hostInfo *membership.HostInfo,
	serviceResolver membership.ServiceResolver,
	namespaceReplicationQueue persistence.NamespaceReplicationQueue,
	namespaceReplicationTaskExecutor namespace.ReplicationTaskExecutor,
) *Replicator {

	return &Replicator{
		status:                           common.DaemonStatusInitialized,
		hostInfo:                         hostInfo,
		serviceResolver:                  serviceResolver,
		clusterMetadata:                  clusterMetadata,
		namespaceReplicationTaskExecutor: namespaceReplicationTaskExecutor,
		namespaceProcessors:              make(map[string]*namespaceReplicationMessageProcessor),
		clientBean:                       clientBean,
		logger:                           log.With(logger, tag.ComponentReplicator),
		metricsClient:                    metricsClient,
		namespaceReplicationQueue:        namespaceReplicationQueue,
	}
}

// Start is called to start replicator
func (r *Replicator) Start() {
	if !atomic.CompareAndSwapInt32(
		&r.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	r.listenToClusterMetadataChange()
}

// Stop is called to stop replicator
func (r *Replicator) Stop() {
	if !atomic.CompareAndSwapInt32(
		&r.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	currentClusterName := r.clusterMetadata.GetCurrentClusterName()
	r.clusterMetadata.UnRegisterMetadataChangeCallback(currentClusterName)
	r.namespaceProcessorsLock.Lock()
	defer r.namespaceProcessorsLock.Unlock()
	for _, namespaceProcessor := range r.namespaceProcessors {
		namespaceProcessor.Stop()
	}
}

func (r *Replicator) listenToClusterMetadataChange() {
	r.clusterMetadata.RegisterMetadataChangeCallback(
		r,
		func(
			oldClusterMetadata map[string]*cluster.ClusterInformation,
			newClusterMetadata map[string]*cluster.ClusterInformation,
		) {
			currentClusterName := r.clusterMetadata.GetCurrentClusterName()
			r.namespaceProcessorsLock.Lock()
			defer r.namespaceProcessorsLock.Unlock()
			for clusterName := range newClusterMetadata {
				if clusterName == currentClusterName {
					continue
				}
				if processor, ok := r.namespaceProcessors[clusterName]; ok {
					processor.Stop()
					delete(r.namespaceProcessors, clusterName)
				}

				if clusterInfo := newClusterMetadata[clusterName]; clusterInfo != nil && clusterInfo.Enabled {
					remoteAdminClient, err := r.clientBean.GetRemoteAdminClient(clusterName)
					if err != nil {
						// Cannot find remote cluster info.
						// This should never happen as cluster metadata should have the up-to-date data.
						panic(fmt.Sprintf("Bug found in cluster metadata with error %v", err))
					}
					processor := newNamespaceReplicationMessageProcessor(
						currentClusterName,
						clusterName,
						log.With(r.logger, tag.ComponentReplicationTaskProcessor, tag.SourceCluster(clusterName)),
						remoteAdminClient,
						r.metricsClient,
						r.namespaceReplicationTaskExecutor,
						r.hostInfo,
						r.serviceResolver,
						r.namespaceReplicationQueue,
					)
					processor.Start()
					r.namespaceProcessors[clusterName] = processor
				}
			}
		},
	)
}
