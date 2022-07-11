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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination task_fetcher_mock.go

package replication

import (
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/api/adminservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/service/history/configs"
	"golang.org/x/exp/maps"
)

const (
	fetchTaskRequestTimeout = time.Minute
	requestChanBufferSize   = 1000
	requestsMapSize         = 1000
)

type (
	// TaskFetcherFactory is a group of fetchers, one per source DC.
	TaskFetcherFactory interface {
		common.Daemon

		GetOrCreateFetcher(clusterName string) taskFetcher
	}

	// taskFetcher is responsible for fetching replication messages from remote DC.
	taskFetcher interface {
		common.Daemon

		getSourceCluster() string
		getRequestChan() chan<- *replicationTaskRequest
		getRateLimiter() quotas.RateLimiter
	}

	// taskFetcherFactoryImpl is a group of fetchers, one per source DC.
	taskFetcherFactoryImpl struct {
		status          int32
		config          *configs.Config
		clientBean      client.Bean
		clusterMetadata cluster.Metadata
		logger          log.Logger

		fetchersLock sync.Mutex
		fetchers     map[string]taskFetcher
	}

	// taskFetcherImpl is the implementation of fetching replication messages.
	taskFetcherImpl struct {
		status         int32
		currentCluster string
		sourceCluster  string
		config         *configs.Config
		numWorker      int
		logger         log.Logger
		rateLimiter    quotas.RateLimiter
		requestChan    chan *replicationTaskRequest
		shutdownChan   chan struct{}

		workers map[int]*replicationTaskFetcherWorker
	}

	replicationTaskFetcherWorker struct {
		status         int32
		currentCluster string
		sourceCluster  string
		config         *configs.Config
		logger         log.Logger
		clientBean     client.Bean
		rateLimiter    quotas.RateLimiter
		requestChan    chan *replicationTaskRequest
		shutdownChan   chan struct{}

		requestByShard map[int32]*replicationTaskRequest
	}
)

// NewTaskFetcherFactory creates an instance of TaskFetcherFactory with given configs.
func NewTaskFetcherFactory(
	logger log.Logger,
	config *configs.Config,
	clusterMetadata cluster.Metadata,
	clientBean client.Bean,
) TaskFetcherFactory {

	return &taskFetcherFactoryImpl{
		clusterMetadata: clusterMetadata,
		clientBean:      clientBean,
		config:          config,
		fetchers:        make(map[string]taskFetcher),
		status:          common.DaemonStatusInitialized,
		logger:          logger,
	}
}

// Start starts the fetchers
func (f *taskFetcherFactoryImpl) Start() {
	if !atomic.CompareAndSwapInt32(
		&f.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	f.listenClusterMetadataChange()
	f.logger.Info("Replication task fetchers started.")
}

// Stop stops the fetchers
func (f *taskFetcherFactoryImpl) Stop() {
	if !atomic.CompareAndSwapInt32(
		&f.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	f.clusterMetadata.UnRegisterMetadataChangeCallback(f)
	f.fetchersLock.Lock()
	defer f.fetchersLock.Unlock()
	for _, fetcher := range f.fetchers {
		fetcher.Stop()
	}
	f.logger.Info("Replication task fetchers stopped.")
}

func (f *taskFetcherFactoryImpl) GetOrCreateFetcher(clusterName string) taskFetcher {
	f.fetchersLock.Lock()
	defer f.fetchersLock.Unlock()

	fetcher, ok := f.fetchers[clusterName]
	if ok {
		return fetcher
	}
	return f.createReplicationFetcherLocked(clusterName)
}

func (f *taskFetcherFactoryImpl) createReplicationFetcherLocked(clusterName string) taskFetcher {
	currentCluster := f.clusterMetadata.GetCurrentClusterName()
	fetcher := newReplicationTaskFetcher(
		f.logger,
		clusterName,
		currentCluster,
		f.config,
		f.clientBean,
	)
	fetcher.Start()
	f.fetchers[clusterName] = fetcher
	return fetcher
}

func (f *taskFetcherFactoryImpl) listenClusterMetadataChange() {
	f.clusterMetadata.RegisterMetadataChangeCallback(
		f,
		func(oldClusterMetadata map[string]*cluster.ClusterInformation, newClusterMetadata map[string]*cluster.ClusterInformation) {
			f.fetchersLock.Lock()
			defer f.fetchersLock.Unlock()

			currentCluster := f.clusterMetadata.GetCurrentClusterName()
			// Fetcher is lazy init. The callback only need to handle remove case.
			for clusterName, newClusterInfo := range newClusterMetadata {
				if clusterName == currentCluster {
					continue
				}
				if fetcher, ok := f.fetchers[clusterName]; ok {
					if newClusterInfo == nil || !newClusterInfo.Enabled {
						fetcher.Stop()
						delete(f.fetchers, clusterName)
					}
				}
			}
		},
	)
}

// newReplicationTaskFetcher creates a new fetcher.
func newReplicationTaskFetcher(
	logger log.Logger,
	sourceCluster string,
	currentCluster string,
	config *configs.Config,
	clientBean client.Bean,
) *taskFetcherImpl {
	numWorker := config.ReplicationTaskFetcherParallelism()
	requestChan := make(chan *replicationTaskRequest, requestChanBufferSize)
	shutdownChan := make(chan struct{})
	rateLimiter := quotas.NewDefaultOutgoingRateLimiter(
		func() float64 { return config.ReplicationTaskProcessorHostQPS() },
	)

	workers := make(map[int]*replicationTaskFetcherWorker)
	for i := 0; i < numWorker; i++ {
		workers[i] = newReplicationTaskFetcherWorker(
			logger,
			sourceCluster,
			currentCluster,
			config,
			clientBean,
			rateLimiter,
			requestChan,
			shutdownChan,
		)
	}

	return &taskFetcherImpl{
		status:         common.DaemonStatusInitialized,
		config:         config,
		numWorker:      numWorker,
		logger:         log.With(logger, tag.ClusterName(sourceCluster)),
		currentCluster: currentCluster,
		sourceCluster:  sourceCluster,
		rateLimiter:    rateLimiter,
		requestChan:    requestChan,
		shutdownChan:   shutdownChan,
		workers:        workers,
	}
}

// Start starts the fetcher
func (f *taskFetcherImpl) Start() {
	if !atomic.CompareAndSwapInt32(
		&f.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	for _, worker := range f.workers {
		worker.Start()
	}
	f.logger.Info("Replication task fetcher started.")
}

// Stop stops the fetcher
func (f *taskFetcherImpl) Stop() {
	if !atomic.CompareAndSwapInt32(
		&f.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	close(f.shutdownChan)
	for _, worker := range f.workers {
		worker.Stop()
	}
	f.logger.Info("Replication task fetcher stopped.")
}

// GetSourceCluster returns the source cluster for the fetcher
func (f *taskFetcherImpl) getSourceCluster() string {
	return f.sourceCluster
}

// GetRequestChan returns the request chan for the fetcher
func (f *taskFetcherImpl) getRequestChan() chan<- *replicationTaskRequest {
	return f.requestChan
}

// GetRateLimiter returns the host level rate limiter for the fetcher
func (f *taskFetcherImpl) getRateLimiter() quotas.RateLimiter {
	return f.rateLimiter
}

func newReplicationTaskFetcherWorker(
	logger log.Logger,
	sourceCluster string,
	currentCluster string,
	config *configs.Config,
	clientBean client.Bean,
	rateLimiter quotas.RateLimiter,
	requestChan chan *replicationTaskRequest,
	shutdownChan chan struct{},
) *replicationTaskFetcherWorker {
	return &replicationTaskFetcherWorker{
		status:         common.DaemonStatusInitialized,
		currentCluster: currentCluster,
		sourceCluster:  sourceCluster,
		config:         config,
		logger:         logger,
		clientBean:     clientBean,
		rateLimiter:    rateLimiter,
		requestChan:    requestChan,
		shutdownChan:   shutdownChan,

		requestByShard: make(map[int32]*replicationTaskRequest, requestsMapSize),
	}
}

// Start starts the fetcher worker
func (f *replicationTaskFetcherWorker) Start() {
	if !atomic.CompareAndSwapInt32(
		&f.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	go f.fetchTasks()
	f.logger.Info("Replication task fetcher worker started.")
}

// Stop stops the fetcher worker
func (f *replicationTaskFetcherWorker) Stop() {
	if !atomic.CompareAndSwapInt32(
		&f.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	f.logger.Info("Replication task fetcher worker stopped.")
}

// fetchTasks collects getReplicationTasks request from shards and send out aggregated request to source frontend.
func (f *replicationTaskFetcherWorker) fetchTasks() {
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
			err := f.getMessages()
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

func (f *replicationTaskFetcherWorker) bufferRequests(
	request *replicationTaskRequest,
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

func (f *replicationTaskFetcherWorker) getMessages() error {

	requestByShard := f.requestByShard
	if len(requestByShard) == 0 {
		return nil
	}
	f.requestByShard = make(map[int32]*replicationTaskRequest, requestsMapSize)

	tokens := make([]*replicationspb.ReplicationToken, 0, len(requestByShard))
	for _, request := range requestByShard {
		tokens = append(tokens, request.token)
	}

	ctx, cancel := rpc.NewContextWithTimeoutAndHeaders(fetchTaskRequestTimeout)
	defer cancel()

	request := &adminservice.GetReplicationMessagesRequest{
		Tokens:      tokens,
		ClusterName: f.currentCluster,
	}
	remoteClient, err := f.clientBean.GetRemoteAdminClient(f.sourceCluster)
	if err != nil {
		return err
	}
	response, err := remoteClient.GetReplicationMessages(ctx, request)
	if err != nil {
		f.logger.Error("Failed to get replication tasks", tag.Error(err))
		for _, req := range requestByShard {
			close(req.respChan)
		}
		return err
	}

	shardReplicationTasks := maps.Clone(response.GetShardMessages())
	for shardID, req := range requestByShard {
		if resp, ok := shardReplicationTasks[shardID]; ok {
			req.respChan <- resp
		}
		close(req.respChan)
	}
	return nil
}
