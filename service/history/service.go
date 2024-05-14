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
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/service/history/configs"
)

// Service represents the history service
type (
	Service struct {
		handler           *Handler
		visibilityManager manager.VisibilityManager
		config            *configs.Config

		server            *grpc.Server
		logger            log.Logger
		grpcListener      net.Listener
		membershipMonitor membership.Monitor
		metricsHandler    metrics.Handler
		healthServer      *health.Server
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
	metricsHandler metrics.Handler,
	healthServer *health.Server,
) *Service {
	return &Service{
		server:            grpc.NewServer(grpcServerOptions...),
		handler:           handler,
		visibilityManager: visibilityMgr,
		config:            serviceConfig,
		logger:            logger,
		grpcListener:      grpcListener,
		membershipMonitor: membershipMonitor,
		metricsHandler:    metricsHandler,
		healthServer:      healthServer,
	}
}

// Start starts the service
func (s *Service) Start() {
	s.logger.Info("history starting")

	metrics.RestartCount.With(s.metricsHandler).Record(1)

	s.handler.Start()

	historyservice.RegisterHistoryServiceServer(s.server, s.handler)
	healthpb.RegisterHealthServer(s.server, s.healthServer)
	s.healthServer.SetServingStatus(serviceName, healthpb.HealthCheckResponse_SERVING)

	go func() {
		s.logger.Info("Starting to serve on history listener")
		if err := s.server.Serve(s.grpcListener); err != nil {
			s.logger.Fatal("Failed to serve on history listener", tag.Error(err))
		}
	}()

	// As soon as we join membership, other hosts will send requests for shards that we own,
	// so we should try to start this after starting the gRPC server.
	go func() {
		if delay := s.config.StartupMembershipJoinDelay(); delay > 0 {
			// In some situations, like rolling upgrades of the history service,
			// pausing before joining membership can help separate the shard movement
			// caused by another history instance terminating with this instance starting.
			s.logger.Info("history start: delaying before membership start",
				tag.NewDurationTag("startupMembershipJoinDelay", delay))
			time.Sleep(delay)
		}
		s.membershipMonitor.Start()
	}()
}

// Stop stops the service
func (s *Service) Stop() {
	s.logger.Info("ShutdownHandler: Evicting self from membership ring")
	_ = s.membershipMonitor.EvictSelf()

	if delay := s.config.ShutdownDrainDuration(); delay > 0 {
		s.logger.Info("ShutdownHandler: delaying for shutdown drain",
			tag.NewDurationTag("shutdownDrainDuration", delay))
		time.Sleep(delay)
	}

	s.healthServer.SetServingStatus(serviceName, healthpb.HealthCheckResponse_NOT_SERVING)

	s.logger.Info("ShutdownHandler: Initiating shardController shutdown")
	s.handler.controller.Stop()

	// TODO: Change this to GracefulStop when integration tests are refactored.
	s.server.Stop()

	s.handler.Stop()
	s.visibilityManager.Close()

	s.logger.Info("history stopped")
}
