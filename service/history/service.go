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
	"math/rand"
	"net"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/configs"
)

// Service represents the history service
type (
	Service struct {
		status            int32
		handler           *Handler
		visibilityManager manager.VisibilityManager
		config            *configs.Config

		server                         *grpc.Server
		logger                         log.Logger
		grpcListener                   net.Listener
		membershipMonitor              membership.Monitor
		faultInjectionDataStoreFactory *client.FaultInjectionDataStoreFactory
		metricsHandler                 metrics.MetricsHandler
	}
)

func NewService(
	grpcServerOptions []grpc.ServerOption,
	serviceConfig *configs.Config,
	visibilityMgr manager.VisibilityManager,
	handler *Handler,
	logger log.Logger,
	grpcListener net.Listener,
	membershipMonitor membership.Monitor,
	metricsHandler metrics.MetricsHandler,
	faultInjectionDataStoreFactory *client.FaultInjectionDataStoreFactory,
) *Service {
	return &Service{
		status:                         common.DaemonStatusInitialized,
		server:                         grpc.NewServer(grpcServerOptions...),
		handler:                        handler,
		visibilityManager:              visibilityMgr,
		config:                         serviceConfig,
		logger:                         logger,
		grpcListener:                   grpcListener,
		membershipMonitor:              membershipMonitor,
		metricsHandler:                 metricsHandler,
		faultInjectionDataStoreFactory: faultInjectionDataStoreFactory,
	}
}

// Start starts the service
func (s *Service) Start() {
	if !atomic.CompareAndSwapInt32(&s.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	logger := s.logger
	logger.Info("history starting")

	s.metricsHandler.Counter(metrics.RestartCount).Record(1)
	rand.Seed(time.Now().UnixNano())

	s.handler.Start()

	historyservice.RegisterHistoryServiceServer(s.server, s.handler)
	healthpb.RegisterHealthServer(s.server, s.handler)

	listener := s.grpcListener
	logger.Info("Starting to serve on history listener")
	if err := s.server.Serve(listener); err != nil {
		logger.Fatal("Failed to serve on history listener", tag.Error(err))
	}
}

// Stop stops the service
func (s *Service) Stop() {
	logger := s.logger
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

	logger.Info("ShutdownHandler: Evicting self from membership ring")
	_ = s.membershipMonitor.EvictSelf()

	logger.Info("ShutdownHandler: Waiting for others to discover I am unhealthy")
	remainingTime = s.sleep(gossipPropagationDelay, remainingTime)

	logger.Info("ShutdownHandler: Initiating shardController shutdown")
	s.handler.controller.Stop()
	logger.Info("ShutdownHandler: Waiting for traffic to drain")
	remainingTime = s.sleep(shardOwnershipTransferDelay, remainingTime)

	logger.Info("ShutdownHandler: No longer taking rpc requests")
	_ = s.sleep(gracePeriod, remainingTime)

	// TODO: Change this to GracefulStop when integration tests are refactored.
	s.server.Stop()

	s.handler.Stop()
	s.visibilityManager.Close()

	logger.Info("history stopped")
}

// sleep sleeps for the minimum of desired and available duration
// returns the remaining available time duration
func (s *Service) sleep(desired time.Duration, available time.Duration) time.Duration {
	d := util.Min(desired, available)
	if d > 0 {
		time.Sleep(d)
	}
	return available - d
}

func (s *Service) GetFaultInjection() *client.FaultInjectionDataStoreFactory {
	return s.faultInjectionDataStoreFactory
}
