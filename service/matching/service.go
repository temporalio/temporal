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
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/util"
)

// Service represents the matching service
type Service struct {
	handler *Handler
	config  *Config

	server                 *grpc.Server
	logger                 log.SnTaggedLogger
	membershipMonitor      membership.Monitor
	grpcListener           net.Listener
	runtimeMetricsReporter *metrics.RuntimeMetricsReporter
	metricsHandler         metrics.Handler
	healthServer           *health.Server
	visibilityManager      manager.VisibilityManager
}

func NewService(
	grpcServerOptions []grpc.ServerOption,
	serviceConfig *Config,
	logger log.SnTaggedLogger,
	membershipMonitor membership.Monitor,
	grpcListener net.Listener,
	runtimeMetricsReporter *metrics.RuntimeMetricsReporter,
	handler *Handler,
	metricsHandler metrics.Handler,
	healthServer *health.Server,
	visibilityManager manager.VisibilityManager,
) *Service {
	return &Service{
		config:                 serviceConfig,
		server:                 grpc.NewServer(grpcServerOptions...),
		handler:                handler,
		logger:                 logger,
		membershipMonitor:      membershipMonitor,
		grpcListener:           grpcListener,
		runtimeMetricsReporter: runtimeMetricsReporter,
		metricsHandler:         metricsHandler,
		healthServer:           healthServer,
		visibilityManager:      visibilityManager,
	}
}

// Start starts the service
func (s *Service) Start() {
	s.logger.Info("matching starting")

	// must start base service first
	metrics.RestartCount.With(s.metricsHandler).Record(1)

	s.handler.Start()

	matchingservice.RegisterMatchingServiceServer(s.server, s.handler)
	healthpb.RegisterHealthServer(s.server, s.healthServer)
	s.healthServer.SetServingStatus(serviceName, healthpb.HealthCheckResponse_SERVING)

	go func() {
		s.logger.Info("Starting to serve on matching listener")
		if err := s.server.Serve(s.grpcListener); err != nil {
			s.logger.Fatal("Failed to serve on matching listener", tag.Error(err))
		}
	}()

	go s.membershipMonitor.Start()
}

// Stop stops the service
func (s *Service) Stop() {
	// remove self from membership ring and wait for traffic to drain
	var err error
	var waitTime time.Duration
	if align := s.config.AlignMembershipChange(); align > 0 {
		propagation := s.membershipMonitor.ApproximateMaxPropagationTime()
		asOf := util.NextAlignedTime(time.Now().Add(propagation), align)
		s.logger.Info("ShutdownHandler: Evicting self from membership ring as of", tag.Timestamp(asOf))
		waitTime, err = s.membershipMonitor.EvictSelfAt(asOf)
	} else {
		s.logger.Info("ShutdownHandler: Evicting self from membership ring immediately")
		err = s.membershipMonitor.EvictSelf()
	}
	if err != nil {
		s.logger.Error("ShutdownHandler: Failed to evict self from membership ring", tag.Error(err))
	}
	s.healthServer.SetServingStatus(serviceName, healthpb.HealthCheckResponse_NOT_SERVING)

	s.logger.Info("ShutdownHandler: Waiting for others to discover I am unhealthy")
	time.Sleep(max(s.config.ShutdownDrainDuration(), waitTime))

	// At this point we should not get any new rpcs since we removed ourself from the ring.
	// Additionally, the engine will notice the membership change and stop all task queues
	// after a delay. However, we can do it immediately by stopping the handler (which stops
	// the engine which stops all task queues).
	s.handler.Stop()

	// All grpc handlers should be cancelled now. Give them a little time to return.
	t := time.AfterFunc(2*time.Second, func() {
		s.logger.Info("ShutdownHandler: Drain time expired, stopping all traffic")
		s.server.Stop()
	})
	s.server.GracefulStop()
	t.Stop()

	s.visibilityManager.Close()

	s.logger.Info("matching stopped")
}
