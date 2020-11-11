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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination replicationTaskFetcher_mock.go -self_package go.temporal.io/server/service/history

package history

import (
	"sync/atomic"
	"time"

	"go.temporal.io/server/api/adminservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/client/admin"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/rpc"
	serviceConfig "go.temporal.io/server/common/service/config"
	"go.temporal.io/server/service/history/configs"
)

const (
	fetchTaskRequestTimeout = time.Minute
	requestChanBufferSize   = 1000
	requestsMapSize         = 1000
)

type (
	// ReplicationTaskFetcherImpl is the implementation of fetching replication messages.
	ReplicationTaskFetcherImpl struct {
		status         int32
		currentCluster string
		sourceCluster  string
		config         *configs.Config
		logger         log.Logger
		remotePeer     admin.Client
		rateLimiter    *quotas.DynamicRateLimiter
		requestChan    chan *request
		requestByShard map[int32]*request
		shutdownChan   chan struct{}
	}
	// ReplicationTaskFetcher is responsible for fetching replication messages from remote DC.
	ReplicationTaskFetcher interface {
		common.Daemon

		GetSourceCluster() string
		GetRequestChan() chan<- *request
		GetRateLimiter() *quotas.DynamicRateLimiter
	}

	// ReplicationTaskFetchers is a group of fetchers, one per source DC.
	ReplicationTaskFetchers interface {
		common.Daemon

		GetFetchers() []ReplicationTaskFetcher
	}

	// ReplicationTaskFetchersImpl is a group of fetchers, one per source DC.
	ReplicationTaskFetchersImpl struct {
		status   int32
		logger   log.Logger
		fetchers []ReplicationTaskFetcher
	}
)

// NewReplicationTaskFetchers creates an instance of ReplicationTaskFetchers with given configs.
func NewReplicationTaskFetchers(
	logger log.Logger,
	config *configs.Config,
	consumerConfig *serviceConfig.ReplicationConsumerConfig,
	clusterMetadata cluster.Metadata,
	clientBean client.Bean,
) *ReplicationTaskFetchersImpl {

	var fetchers []ReplicationTaskFetcher
	if consumerConfig.Type == serviceConfig.ReplicationConsumerTypeRPC && config.EnableRPCReplication() {
		currentCluster := clusterMetadata.GetCurrentClusterName()
		for clusterName, info := range clusterMetadata.GetAllClusterInfo() {
			if !info.Enabled {
				continue
			}

			if clusterName != currentCluster {
				remoteFrontendClient := clientBean.GetRemoteAdminClient(clusterName)
				fetcher := newReplicationTaskFetcher(
					logger,
					clusterName,
					currentCluster,
					config,
					remoteFrontendClient,
				)
				fetchers = append(fetchers, fetcher)
			}
		}
	}

	return &ReplicationTaskFetchersImpl{
		fetchers: fetchers,
		status:   common.DaemonStatusInitialized,
		logger:   logger,
	}
}

// Start starts the fetchers
func (f *ReplicationTaskFetchersImpl) Start() {
	if !atomic.CompareAndSwapInt32(
		&f.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	for _, fetcher := range f.fetchers {
		fetcher.Start()
	}
	f.logger.Info("Replication task fetchers started.")
}

// Stop stops the fetchers
func (f *ReplicationTaskFetchersImpl) Stop() {
	if !atomic.CompareAndSwapInt32(
		&f.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	for _, fetcher := range f.fetchers {
		fetcher.Stop()
	}
	f.logger.Info("Replication task fetchers stopped.")
}

// GetFetchers returns all the fetchers
func (f *ReplicationTaskFetchersImpl) GetFetchers() []ReplicationTaskFetcher {
	return f.fetchers
}

// newReplicationTaskFetcher creates a new fetcher.
func newReplicationTaskFetcher(
	logger log.Logger,
	sourceCluster string,
	currentCluster string,
	config *configs.Config,
	sourceFrontend admin.Client,
) *ReplicationTaskFetcherImpl {

	return &ReplicationTaskFetcherImpl{
		status:         common.DaemonStatusInitialized,
		config:         config,
		logger:         logger.WithTags(tag.ClusterName(sourceCluster)),
		remotePeer:     sourceFrontend,
		currentCluster: currentCluster,
		sourceCluster:  sourceCluster,
		rateLimiter: quotas.NewDynamicRateLimiter(func() float64 {
			return config.ReplicationTaskProcessorHostQPS()
		}),
		requestChan:    make(chan *request, requestChanBufferSize),
		requestByShard: make(map[int32]*request, requestsMapSize),
		shutdownChan:   make(chan struct{}),
	}
}

// Start starts the fetcher
func (f *ReplicationTaskFetcherImpl) Start() {
	if !atomic.CompareAndSwapInt32(
		&f.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	for i := 0; i < f.config.ReplicationTaskFetcherParallelism(); i++ {
		go f.fetchTasks()
	}
	f.logger.Info("Replication task fetcher started.")
}

// Stop stops the fetcher
func (f *ReplicationTaskFetcherImpl) Stop() {
	if !atomic.CompareAndSwapInt32(
		&f.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	close(f.shutdownChan)
	f.logger.Info("Replication task fetcher stopped.")
}

// fetchTasks collects getReplicationTasks request from shards and send out aggregated request to source frontend.
func (f *ReplicationTaskFetcherImpl) fetchTasks() {
	timer := time.NewTimer(backoff.JitDuration(
		f.config.ReplicationTaskFetcherAggregationInterval(),
		f.config.ReplicationTaskFetcherTimerJitterCoefficient(),
	))
	defer timer.Stop()

	for {
		select {
		case request := <-f.requestChan:
			f.bufferRequests(request)

		case <-timer.C:
			// When timer fires, we collect all the requests we have so far and attempt to send them to remote.
			requests := f.requestByShard
			f.requestByShard = make(map[int32]*request, requestsMapSize)

			err := f.getMessages(requests)
			if err != nil {
				timer.Reset(backoff.JitDuration(
					f.config.ReplicationTaskFetcherErrorRetryWait(),
					f.config.ReplicationTaskFetcherTimerJitterCoefficient(),
				))
			} else {
				timer.Reset(backoff.JitDuration(
					f.config.ReplicationTaskFetcherAggregationInterval(),
					f.config.ReplicationTaskFetcherTimerJitterCoefficient(),
				))
			}

		case <-f.shutdownChan:
			return
		}
	}
}

func (f *ReplicationTaskFetcherImpl) bufferRequests(
	request *request,
) {
	// Here we only add the request to map. We will wait until timer fires to send the request to remote.
	if req, ok := f.requestByShard[request.token.GetShardId()]; ok && req != request {
		// since this replication task fetcher is per host
		// and replication task processor is per shard
		// during shard movement, duplicated requests can appear
		// if shard moved from this host, to this host.
		close(req.respChan)
	}
	f.requestByShard[request.token.GetShardId()] = request
}

func (f *ReplicationTaskFetcherImpl) getMessages(
	requests map[int32]*request,
) error {

	tokens := make([]*replicationspb.ReplicationToken, 0, len(requests))
	for _, request := range requests {
		tokens = append(tokens, request.token)
	}

	ctx, cancel := rpc.NewContextWithTimeoutAndHeaders(fetchTaskRequestTimeout)
	defer cancel()

	request := &adminservice.GetReplicationMessagesRequest{
		Tokens:      tokens,
		ClusterName: f.currentCluster,
	}
	response, err := f.remotePeer.GetReplicationMessages(ctx, request)
	if err != nil {
		f.logger.Error("Failed to get replication tasks", tag.Error(err))
		return err
	}

	for shardID, tasks := range response.GetShardMessages() {
		request, ok := requests[shardID]
		if !ok {
			f.logger.Error("No outstanding request found for shardId. Skipping Messages.", tag.ShardID(shardID))
			continue
		}
		request.respChan <- tasks
		close(request.respChan)
	}
	return nil
}

// GetSourceCluster returns the source cluster for the fetcher
func (f *ReplicationTaskFetcherImpl) GetSourceCluster() string {
	return f.sourceCluster
}

// GetRequestChan returns the request chan for the fetcher
func (f *ReplicationTaskFetcherImpl) GetRequestChan() chan<- *request {
	return f.requestChan
}

// GetRateLimiter returns the host level rate limiter for the fetcher
func (f *ReplicationTaskFetcherImpl) GetRateLimiter() *quotas.DynamicRateLimiter {
	return f.rateLimiter
}
