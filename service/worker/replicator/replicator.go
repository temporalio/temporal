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
		namespaceProcessors              []*namespaceReplicationMessageProcessor
		logger                           log.Logger
		metricsClient                    metrics.Client
		hostInfo                         *membership.HostInfo
		serviceResolver                  membership.ServiceResolver
		namespaceReplicationQueue        persistence.NamespaceReplicationQueue
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

	logger = log.With(logger, tag.ComponentReplicator)
	var namespaceReplicationMessageProcessors []*namespaceReplicationMessageProcessor
	currentClusterName := clusterMetadata.GetCurrentClusterName()
	for targetClusterName, info := range clusterMetadata.GetAllClusterInfo() {
		if !info.Enabled {
			continue
		}

		if targetClusterName != currentClusterName {
			namespaceReplicationMessageProcessors = append(namespaceReplicationMessageProcessors, newNamespaceReplicationMessageProcessor(
				currentClusterName,
				targetClusterName,
				log.With(logger, tag.ComponentReplicationTaskProcessor, tag.SourceCluster(targetClusterName)),
				clientBean.GetRemoteAdminClient(targetClusterName),
				metricsClient,
				namespaceReplicationTaskExecutor,
				hostInfo,
				serviceResolver,
				namespaceReplicationQueue,
			))
		}
	}
	return &Replicator{
		status:                           common.DaemonStatusInitialized,
		hostInfo:                         hostInfo,
		serviceResolver:                  serviceResolver,
		clusterMetadata:                  clusterMetadata,
		namespaceReplicationTaskExecutor: namespaceReplicationTaskExecutor,
		namespaceProcessors:              namespaceReplicationMessageProcessors,
		clientBean:                       clientBean,
		logger:                           logger,
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

	for _, namespaceProcessor := range r.namespaceProcessors {
		namespaceProcessor.Start()
	}
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

	for _, namespaceProcessor := range r.namespaceProcessors {
		namespaceProcessor.Stop()
	}
}
