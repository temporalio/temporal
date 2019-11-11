// Copyright (c) 2019 Uber Technologies, Inc.
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

package history

import (
	"context"
	"sync/atomic"
	"time"

	r "github.com/temporalio/temporal/.gen/go/replicator"
	"github.com/temporalio/temporal/.gen/go/temporal/workflowserviceclient"
	"github.com/temporalio/temporal/client"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/backoff"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	serviceConfig "github.com/temporalio/temporal/common/service/config"
)

const (
	fetchTaskRequestTimeout = 60 * time.Second
	requestChanBufferSize   = 1000
)

type (
	// ReplicationTaskFetcher is responsible for fetching replication messages from remote DC.
	ReplicationTaskFetcher struct {
		status        int32
		sourceCluster string
		config        *Config
		logger        log.Logger
		remotePeer    workflowserviceclient.Interface
		requestChan   chan *request
		done          chan struct{}
	}

	// ReplicationTaskFetchers is a group of fetchers, one per source DC.
	ReplicationTaskFetchers struct {
		status   int32
		logger   log.Logger
		fetchers []*ReplicationTaskFetcher
	}
)

// NewReplicationTaskFetchers creates an instance of ReplicationTaskFetchers with given configs.
func NewReplicationTaskFetchers(
	logger log.Logger,
	config *Config,
	consumerConfig *serviceConfig.ReplicationConsumerConfig,
	clusterMetadata cluster.Metadata,
	clientBean client.Bean,
) *ReplicationTaskFetchers {

	var fetchers []*ReplicationTaskFetcher
	if consumerConfig.Type == serviceConfig.ReplicationConsumerTypeRPC {
		for clusterName, info := range clusterMetadata.GetAllClusterInfo() {
			if !info.Enabled {
				continue
			}

			if clusterName != clusterMetadata.GetCurrentClusterName() {
				remoteFrontendClient := clientBean.GetRemoteFrontendClient(clusterName)
				fetcher := newReplicationTaskFetcher(logger, clusterName, config, remoteFrontendClient)
				fetchers = append(fetchers, fetcher)
			}
		}
	}

	return &ReplicationTaskFetchers{
		fetchers: fetchers,
		status:   common.DaemonStatusInitialized,
		logger:   logger,
	}
}

// Start starts the fetchers
func (f *ReplicationTaskFetchers) Start() {
	if !atomic.CompareAndSwapInt32(&f.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	for _, fetcher := range f.fetchers {
		fetcher.Start()
	}
	f.logger.Info("Replication task fetchers started.")
}

// Stop stops the fetchers
func (f *ReplicationTaskFetchers) Stop() {
	if !atomic.CompareAndSwapInt32(&f.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	for _, fetcher := range f.fetchers {
		fetcher.Stop()
	}
	f.logger.Info("Replication task fetchers stopped.")
}

// GetFetchers returns all the fetchers
func (f *ReplicationTaskFetchers) GetFetchers() []*ReplicationTaskFetcher {
	return f.fetchers
}

// newReplicationTaskFetcher creates a new fetcher.
func newReplicationTaskFetcher(
	logger log.Logger,
	sourceCluster string,
	config *Config,
	sourceFrontend workflowserviceclient.Interface,
) *ReplicationTaskFetcher {

	return &ReplicationTaskFetcher{
		status:        common.DaemonStatusInitialized,
		config:        config,
		logger:        logger.WithTags(tag.ClusterName(sourceCluster)),
		remotePeer:    sourceFrontend,
		sourceCluster: sourceCluster,
		requestChan:   make(chan *request, requestChanBufferSize),
		done:          make(chan struct{}),
	}
}

// Start starts the fetcher
func (f *ReplicationTaskFetcher) Start() {
	if !atomic.CompareAndSwapInt32(&f.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	for i := 0; i < f.config.ReplicationTaskFetcherParallelism(); i++ {
		go f.fetchTasks()
	}
	f.logger.Info("Replication task fetcher started.", tag.Counter(f.config.ReplicationTaskFetcherParallelism()))
}

// Stop stops the fetcher
func (f *ReplicationTaskFetcher) Stop() {
	if !atomic.CompareAndSwapInt32(&f.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	close(f.done)
	f.logger.Info("Replication task fetcher stopped.")
}

// fetchTasks collects getReplicationTasks request from shards and send out aggregated request to source frontend.
func (f *ReplicationTaskFetcher) fetchTasks() {
	timer := time.NewTimer(backoff.JitDuration(
		f.config.ReplicationTaskFetcherAggregationInterval(),
		f.config.ReplicationTaskFetcherTimerJitterCoefficient(),
	))

	requestByShard := make(map[int32]*request)

	for {
		select {
		case request := <-f.requestChan:
			// Here we only add the request to map. We will wait until timer fires to send the request to remote.
			if req, ok := requestByShard[request.token.GetShardID()]; ok && req != request {
				// since this replication task fetcher is per host
				// and replication task processor is per shard
				// during shard movement, duplicated requests can appear
				// if shard moved from this host, to this host.
				f.logger.Error("Get replication task request already exist for shard.")
				close(req.respChan)
			}
			requestByShard[request.token.GetShardID()] = request

		case <-timer.C:
			// When timer fires, we collect all the requests we have so far and attempt to send them to remote.
			err := f.fetchAndDistributeTasks(requestByShard)
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
		case <-f.done:
			timer.Stop()
			return
		}
	}
}

func (f *ReplicationTaskFetcher) fetchAndDistributeTasks(requestByShard map[int32]*request) error {
	if len(requestByShard) == 0 {
		// We don't receive tasks from previous fetch so processors are all sleeping.
		f.logger.Debug("Skip fetching as no processor is asking for tasks.")
		return nil
	}

	messagesByShard, err := f.getMessages(requestByShard)
	if err != nil {
		f.logger.Error("Failed to get replication tasks", tag.Error(err))
		return err
	}

	f.logger.Debug("Successfully fetched replication tasks.", tag.Counter(len(messagesByShard)))

	for shardID, tasks := range messagesByShard {
		request := requestByShard[shardID]
		request.respChan <- tasks
		close(request.respChan)
		delete(requestByShard, shardID)
	}

	return nil
}

func (f *ReplicationTaskFetcher) getMessages(
	requestByShard map[int32]*request,
) (map[int32]*r.ReplicationMessages, error) {
	var tokens []*r.ReplicationToken
	for _, request := range requestByShard {
		tokens = append(tokens, request.token)
	}

	ctx, cancel := context.WithTimeout(context.Background(), fetchTaskRequestTimeout)
	defer cancel()

	request := &r.GetReplicationMessagesRequest{Tokens: tokens}
	response, err := f.remotePeer.GetReplicationMessages(ctx, request)
	if err != nil {
		return nil, err
	}

	return response.GetMessagesByShard(), err
}

// GetSourceCluster returns the source cluster for the fetcher
func (f *ReplicationTaskFetcher) GetSourceCluster() string {
	return f.sourceCluster
}

// GetRequestChan returns the request chan for the fetcher
func (f *ReplicationTaskFetcher) GetRequestChan() chan<- *request {
	return f.requestChan
}
