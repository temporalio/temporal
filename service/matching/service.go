// Copyright (c) 2017 Uber Technologies, Inc.
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

	"google.golang.org/grpc"

	"github.com/temporalio/temporal/.gen/proto/healthservice"
	"github.com/temporalio/temporal/.gen/proto/matchingservice"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/persistence"
	persistenceClient "github.com/temporalio/temporal/common/persistence/client"
	"github.com/temporalio/temporal/common/resource"
	"github.com/temporalio/temporal/common/service"
	"github.com/temporalio/temporal/common/service/dynamicconfig"
)

// Service represents the cadence-matching service
type Service struct {
	resource.Resource

	status  int32
	handler *Handler
	config  *Config

	server *grpc.Server
}

// NewService builds a new cadence-matching service
func NewService(
	params *service.BootstrapParams,
) (resource.Resource, error) {

	serviceConfig := NewConfig(dynamicconfig.NewCollection(params.DynamicConfig, params.Logger))
	params.PersistenceConfig.SetMaxQPS(
		params.PersistenceConfig.DefaultStore,
		serviceConfig.PersistenceMaxQPS(),
	)
	serviceResource, err := resource.New(
		params,
		common.MatchingServiceName,
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
	s.handler.RegisterHandler()

	// must start base service first
	s.Resource.Start()
	s.handler.Start()

	s.server = grpc.NewServer()
	handlerGRPC := NewHandlerGRPC(s.handler)
	nilCheckHandler := NewNilCheckHandler(handlerGRPC)
	matchingservice.RegisterMatchingServiceServer(s.server, nilCheckHandler)
	healthservice.RegisterMetaServer(s.server, handlerGRPC)

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

	s.server.GracefulStop()

	s.handler.Stop()
	s.Resource.Stop()

	s.GetLogger().Info("matching stopped")
}
