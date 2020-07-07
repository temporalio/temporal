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
	"context"
	"sync/atomic"
	"time"

	"go.temporal.io/temporal-proto/serviceerror"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/temporalio/temporal/api/matchingservice/v1"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/persistence"
	persistenceClient "github.com/temporalio/temporal/common/persistence/client"
	"github.com/temporalio/temporal/common/resource"
	"github.com/temporalio/temporal/common/service/dynamicconfig"
)

// Service represents the matching service
type Service struct {
	resource.Resource

	status  int32
	handler *Handler
	config  *Config
	params  *resource.BootstrapParams

	server *grpc.Server
}

// NewService builds a new matching service
func NewService(
	params *resource.BootstrapParams,
) (resource.Resource, error) {

	serviceConfig := NewConfig(dynamicconfig.NewCollection(params.DynamicConfig, params.Logger))
	serviceResource, err := resource.New(
		params,
		common.MatchingServiceName,
		serviceConfig.PersistenceMaxQPS,
		serviceConfig.PersistenceGlobalMaxQPS,
		serviceConfig.ThrottledLogRPS,
		func(
			persistenceBean persistenceClient.Bean,
			logger log.Logger,
		) (persistence.VisibilityManager, error) {
			return persistenceBean.GetVisibilityManager(), nil
		},
	)
	if err != nil {
		return nil, err
	}

	return &Service{
		Resource: serviceResource,
		status:   common.DaemonStatusInitialized,
		config:   serviceConfig,
		params:   params,
	}, nil
}

// Start starts the service
func (s *Service) Start() {
	if !atomic.CompareAndSwapInt32(&s.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	logger := s.GetLogger()
	logger.Info("matching starting")

	s.handler = NewHandler(s, s.config)

	// must start base service first
	s.Resource.Start()
	s.handler.Start()

	opts, err := s.params.RPCFactory.GetInternodeGRPCServerOptions()
	if err != nil {
		logger.Fatal("creating grpc server options failed", tag.Error(err))
	}
	opts = append(opts, grpc.UnaryInterceptor(interceptor))
	s.server = grpc.NewServer(opts...)
	nilCheckHandler := NewNilCheckHandler(s.handler)
	matchingservice.RegisterMatchingServiceServer(s.server, nilCheckHandler)
	healthpb.RegisterHealthServer(s.server, s.handler)

	listener := s.GetGRPCListener()
	logger.Info("Starting to serve on matching listener")
	if err := s.server.Serve(listener); err != nil {
		logger.Fatal("Failed to serve on matching listener", tag.Error(err))
	}
}

// Stop stops the service
func (s *Service) Stop() {
	if !atomic.CompareAndSwapInt32(&s.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	// remove self from membership ring and wait for traffic to drain
	s.GetLogger().Info("ShutdownHandler: Evicting self from membership ring")
	s.GetMembershipMonitor().EvictSelf()
	s.GetLogger().Info("ShutdownHandler: Waiting for others to discover I am unhealthy")
	time.Sleep(s.config.ShutdownDrainDuration())

	// TODO: Change this to GracefulStop when integration tests are refactored.
	s.server.Stop()

	s.handler.Stop()
	s.Resource.Stop()

	s.GetLogger().Info("matching stopped")
}

func interceptor(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	resp, err := handler(ctx, req)
	return resp, serviceerror.ToStatus(err).Err()
}
