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
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/matching/configs"
)

// Service represents the matching service
type Service struct {
	resource.Resource

	status  int32
	handler *Handler
	config  *Config

	server          *grpc.Server
	logger          log.Logger
	throttledLogger log.ThrottledLogger
}

// NewService builds a new matching service
// todomigryz: I'm ultimately replacing this method with service_providers.go:111 MatchingServiceProvider
func NewService(
	params *resource.BootstrapParams, //todomigryz: these should not be used
) (*Service, error) {
	logger := params.Logger

	serviceConfig := NewConfig(dynamicconfig.NewCollection(params.DynamicConfigClient, params.Logger))
	// todomigryz: resource.New overrides logger and creates new throttledLogger
	serviceResource, err := resource.New(
		params,
		common.MatchingServiceName,
		serviceConfig.PersistenceMaxQPS,
		serviceConfig.PersistenceGlobalMaxQPS,
		serviceConfig.ThrottledLogRPS,
		func(
			persistenceBean persistenceClient.Bean,
			searchAttributesProvider searchattribute.Provider,
			logger log.Logger,
		) (persistence.VisibilityManager, error) {
			return persistenceBean.GetVisibilityManager(), nil
		},
	)
	if err != nil {
		return nil, err
	}

	metricsInterceptor := interceptor.NewTelemetryInterceptor(
		serviceResource.GetNamespaceCache(),
		serviceResource.GetMetricsClient(),
		metrics.MatchingAPIMetricsScopes(),
		logger,
	)
	rateLimiterInterceptor := interceptor.NewRateLimitInterceptor(
		configs.NewPriorityRateLimiter(func() float64 { return float64(serviceConfig.RPS()) }),
		map[string]int{},
	)

	grpcServerOptions, err := params.RPCFactory.GetInternodeGRPCServerOptions()
	if err != nil {
		logger.Fatal("creating gRPC server options failed", tag.Error(err))
	}

	grpcServerOptions = append(
		grpcServerOptions,
		grpc.ChainUnaryInterceptor(
			rpc.ServiceErrorInterceptor,
			metrics.NewServerMetricsContextInjectorInterceptor(),
			metrics.NewServerMetricsTrailerPropagatorInterceptor(logger),
			metricsInterceptor.Intercept,
			rateLimiterInterceptor.Intercept,
		),
	)

	return &Service{
		Resource: serviceResource,
		status:   common.DaemonStatusInitialized,
		config:   serviceConfig,
		server:   grpc.NewServer(grpcServerOptions...),
		handler:  NewHandler(serviceResource, serviceConfig),
	}, nil
}

// Start starts the service
func (s *Service) Start() {
	if !atomic.CompareAndSwapInt32(&s.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	s.logger.Info("matching starting")

	// must start base service first
	s.Resource.Start()
	s.handler.Start()

	matchingservice.RegisterMatchingServiceServer(s.server, s.handler)
	healthpb.RegisterHealthServer(s.server, s.handler)

	listener := s.GetGRPCListener()
	s.logger.Info("Starting to serve on matching listener")
	if err := s.server.Serve(listener); err != nil {
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
	s.GetMembershipMonitor().EvictSelf()
	s.logger.Info("ShutdownHandler: Waiting for others to discover I am unhealthy")
	time.Sleep(s.config.ShutdownDrainDuration())

	// TODO: Change this to GracefulStop when integration tests are refactored.
	s.server.Stop()

	s.handler.Stop()

	//////////////////////////
	// Coming from resource //
	//////////////////////////
	s.Resource.Stop() // todomigryz: this should be brought into service.Stop()
	if !atomic.CompareAndSwapInt32(
		&h.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	h.namespaceCache.Stop()
	h.membershipMonitor.Stop()
	h.ringpopChannel.Close()
	h.runtimeMetricsReporter.Stop()
	h.persistenceBean.Close()
	if h.visibilityMgr != nil {
		h.visibilityMgr.Close()
	}
	//////////////////
	// resource end //
	//////////////////

	s.logger.Info("matching stopped")
}
