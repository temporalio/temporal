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

package history

import (
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/messaging"
	"go.temporal.io/server/common/persistence"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	espersistence "go.temporal.io/server/common/persistence/elasticsearch"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/service/config"
	"go.temporal.io/server/common/service/dynamicconfig"
	"go.temporal.io/server/service/history/configs"
)

// Service represents the history service
type Service struct {
	resource.Resource

	status  int32
	handler *Handler
	params  *resource.BootstrapParams
	config  *configs.Config

	server *grpc.Server
}

// NewService builds a new history service
func NewService(
	params *resource.BootstrapParams,
) (resource.Resource, error) {
	serviceConfig := configs.NewConfig(dynamicconfig.NewCollection(params.DynamicConfig, params.Logger),
		params.PersistenceConfig.NumHistoryShards,
		params.PersistenceConfig.IsAdvancedVisibilityConfigExist())

	params.PersistenceConfig.VisibilityConfig = &config.VisibilityConfig{
		VisibilityOpenMaxQPS:   serviceConfig.VisibilityOpenMaxQPS,
		VisibilityClosedMaxQPS: serviceConfig.VisibilityClosedMaxQPS,
		EnableSampling:         serviceConfig.EnableVisibilitySampling,
	}

	visibilityManagerInitializer := func(
		persistenceBean persistenceClient.Bean,
		logger log.Logger,
	) (persistence.VisibilityManager, error) {
		visibilityFromDB := persistenceBean.GetVisibilityManager()

		var visibilityFromES persistence.VisibilityManager
		if params.ESConfig != nil {
			visibilityIndexName := params.ESConfig.Indices[common.VisibilityAppName]

			var visibilityProducer messaging.Producer
			var esProcessor espersistence.Processor
			if serviceConfig.VisibilityQueue() == common.VisibilityQueueInternal || serviceConfig.VisibilityQueue() == common.VisibilityQueueInternalWithDualProcessor {
				esProcessorConfig := &espersistence.ProcessorConfig{
					IndexerConcurrency:       serviceConfig.IndexerConcurrency,
					ESProcessorNumOfWorkers:  serviceConfig.ESProcessorNumOfWorkers,
					ESProcessorBulkActions:   serviceConfig.ESProcessorBulkActions,
					ESProcessorBulkSize:      serviceConfig.ESProcessorBulkSize,
					ESProcessorFlushInterval: serviceConfig.ESProcessorFlushInterval,
					ValidSearchAttributes:    serviceConfig.ValidSearchAttributes,
				}

				esProcessor = espersistence.NewProcessor(esProcessorConfig, params.ESClient, logger, params.MetricsClient)
				esProcessor.Start()
			}
			if serviceConfig.VisibilityQueue() == common.VisibilityQueueKafka || serviceConfig.VisibilityQueue() == common.VisibilityQueueInternalWithDualProcessor {
				var err error
				visibilityProducer, err = params.MessagingClient.NewProducer(common.VisibilityAppName)
				if err != nil {
					logger.Fatal("Creating visibility producer failed", tag.Error(err))
				}
			}

			visibilityConfigForES := &config.VisibilityConfig{
				ESIndexMaxResultWindow: serviceConfig.ESIndexMaxResultWindow,
				ValidSearchAttributes:  serviceConfig.ValidSearchAttributes,
				ESProcessorAckTimeout:  serviceConfig.ESProcessorAckTimeout,
			}
			visibilityFromES = espersistence.NewESVisibilityManager(visibilityIndexName, params.ESClient, visibilityConfigForES, visibilityProducer, esProcessor, params.MetricsClient, logger)
		}
		return persistence.NewVisibilityManagerWrapper(
			visibilityFromDB,
			visibilityFromES,
			dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false), // history visibility never read
			serviceConfig.AdvancedVisibilityWritingMode,
		), nil
	}

	serviceResource, err := resource.New(
		params,
		common.HistoryServiceName,
		serviceConfig.PersistenceMaxQPS,
		serviceConfig.PersistenceGlobalMaxQPS,
		serviceConfig.ThrottledLogRPS,
		visibilityManagerInitializer,
	)
	if err != nil {
		return nil, err
	}

	return &Service{
		Resource: serviceResource,
		status:   common.DaemonStatusInitialized,
		params:   params,
		config:   serviceConfig,
	}, nil
}

// Start starts the service
func (s *Service) Start() {
	if !atomic.CompareAndSwapInt32(&s.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	logger := s.GetLogger()
	logger.Info("elastic search config", tag.ESConfig(s.params.ESConfig))
	logger.Info("history starting")

	s.handler = NewHandler(s.Resource, s.config)

	// must start resource first
	s.Resource.Start()
	s.handler.Start()

	opts, err := s.params.RPCFactory.GetInternodeGRPCServerOptions()
	if err != nil {
		logger.Fatal("creating grpc server options failed", tag.Error(err))
	}
	opts = append(
		opts,
		grpc.ChainUnaryInterceptor(rpc.ServiceErrorInterceptor))
	s.server = grpc.NewServer(opts...)
	historyservice.RegisterHistoryServiceServer(s.server, s.handler)
	healthpb.RegisterHealthServer(s.server, s.handler)

	listener := s.GetGRPCListener()
	logger.Info("Starting to serve on history listener")
	if err := s.server.Serve(listener); err != nil {
		logger.Fatal("Failed to serve on history listener", tag.Error(err))
	}
}

// Stop stops the service
func (s *Service) Stop() {
	if !atomic.CompareAndSwapInt32(&s.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	// initiate graceful shutdown :
	// 1. remove self from the membership ring
	// 2. wait for other members to discover we are going down
	// 3. stop acquiring new shards (periodically or based on other membership changes)
	// 4. wait for shard ownership to transfer (and inflight requests to drain) while still accepting new requests
	// 5. Reject all requests arriving at rpc handler to avoid taking on more work except for RespondXXXCompleted and
	//    RecordXXStarted APIs - for these APIs, most of the work is already one and rejecting at last stage is
	//    probably not that desirable. If the shard is closed, these requests will fail anyways.
	// 6. wait for grace period
	// 7. force stop the whole world and return

	const gossipPropagationDelay = 400 * time.Millisecond
	const shardOwnershipTransferDelay = 5 * time.Second
	const gracePeriod = 2 * time.Second

	remainingTime := s.config.ShutdownDrainDuration()

	s.GetLogger().Info("ShutdownHandler: Evicting self from membership ring")
	s.GetMembershipMonitor().EvictSelf()

	s.GetLogger().Info("ShutdownHandler: Waiting for others to discover I am unhealthy")
	remainingTime = s.sleep(gossipPropagationDelay, remainingTime)

	s.GetLogger().Info("ShutdownHandler: Initiating shardController shutdown")
	s.handler.controller.PrepareToStop()
	s.GetLogger().Info("ShutdownHandler: Waiting for traffic to drain")
	remainingTime = s.sleep(shardOwnershipTransferDelay, remainingTime)

	s.GetLogger().Info("ShutdownHandler: No longer taking rpc requests")
	remainingTime = s.sleep(gracePeriod, remainingTime)

	// TODO: Change this to GracefulStop when integration tests are refactored.
	s.server.Stop()

	s.handler.Stop()
	s.Resource.Stop()

	s.GetLogger().Info("history stopped")
}

// sleep sleeps for the minimum of desired and available duration
// returns the remaining available time duration
func (s *Service) sleep(desired time.Duration, available time.Duration) time.Duration {
	d := common.MinDuration(desired, available)
	if d > 0 {
		time.Sleep(d)
	}
	return available - d
}
