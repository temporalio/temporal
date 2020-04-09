package matching

import (
	"context"
	"sync/atomic"

	"go.temporal.io/temporal-proto/serviceerror"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/temporalio/temporal/.gen/proto/matchingservice"
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

	// must start base service first
	s.Resource.Start()
	s.handler.Start()

	s.server = grpc.NewServer(grpc.UnaryInterceptor(interceptor))
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

	s.server.GracefulStop()

	s.handler.Stop()
	s.Resource.Stop()

	s.GetLogger().Info("matching stopped")
}

func interceptor(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	resp, err := handler(ctx, req)
	return resp, serviceerror.ToStatus(err).Err()
}
