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

package matching

import (
	"math/rand"
	"net"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/resource"
)

// Service represents the matching service
type Service struct {
	status  int32
	handler *Handler
	config  *Config

	server                         *grpc.Server
	logger                         resource.SnTaggedLogger
	membershipMonitor              membership.Monitor
	grpcListener                   net.Listener
	runtimeMetricsReporter         *metrics.RuntimeMetricsReporter
	metricsHandler                 metrics.MetricsHandler
	faultInjectionDataStoreFactory *client.FaultInjectionDataStoreFactory
}

func NewService(
	grpcServerOptions []grpc.ServerOption,
	serviceConfig *Config,
	logger resource.SnTaggedLogger,
	membershipMonitor membership.Monitor,
	grpcListener net.Listener,
	runtimeMetricsReporter *metrics.RuntimeMetricsReporter,
	handler *Handler,
	metricsHandler metrics.MetricsHandler,
	faultInjectionDataStoreFactory *client.FaultInjectionDataStoreFactory,
) *Service {
	return &Service{
		status:                         common.DaemonStatusInitialized,
		config:                         serviceConfig,
		server:                         grpc.NewServer(grpcServerOptions...),
		handler:                        handler,
		logger:                         logger,
		membershipMonitor:              membershipMonitor,
		grpcListener:                   grpcListener,
		runtimeMetricsReporter:         runtimeMetricsReporter,
		metricsHandler:                 metricsHandler,
		faultInjectionDataStoreFactory: faultInjectionDataStoreFactory,
	}
}

// Start starts the service
func (s *Service) Start() {
	if !atomic.CompareAndSwapInt32(&s.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	s.logger.Info("matching starting")

	// must start base service first
	s.metricsHandler.Counter(metrics.RestartCount).Record(1)
	rand.Seed(time.Now().UnixNano())

	s.handler.Start()

	matchingservice.RegisterMatchingServiceServer(s.server, s.handler)
	healthpb.RegisterHealthServer(s.server, s.handler)

	s.logger.Info("Starting to serve on matching listener")
	if err := s.server.Serve(s.grpcListener); err != nil {
		s.logger.Fatal("Failed to serve on matching listener", tag.Error(err))
	}
}

// Stop stops the service
func (s *Service) Stop() {
	if !atomic.CompareAndSwapInt32(&s.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	// remove self from membership ring and wait for traffic to drain
	s.logger.Info("ShutdownHandler: Evicting self from membership ring")
	s.membershipMonitor.EvictSelf()
	s.logger.Info("ShutdownHandler: Waiting for others to discover I am unhealthy")
	time.Sleep(s.config.ShutdownDrainDuration())

	// TODO: Change this to GracefulStop when integration tests are refactored.
	s.server.Stop()

	s.handler.Stop()

	s.logger.Info("matching stopped")
}

func (s *Service) GetFaultInjection() *client.FaultInjectionDataStoreFactory {
	return s.faultInjectionDataStoreFactory
}
