package history

import (
	"context"
	"net"
	"time"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/configs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
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
		readinessCancel   context.CancelFunc
		chasmRegistry     *chasm.Registry
	}
)

func NewService(
	server *grpc.Server,
	serviceConfig *configs.Config,
	visibilityMgr manager.VisibilityManager,
	handler *Handler,
	logger log.Logger,
	grpcListener net.Listener,
	membershipMonitor membership.Monitor,
	metricsHandler metrics.Handler,
	healthServer *health.Server,
	chasmRegistry *chasm.Registry,
) *Service {
	return &Service{
		server:            server,
		handler:           handler,
		visibilityManager: visibilityMgr,
		config:            serviceConfig,
		logger:            logger,
		grpcListener:      grpcListener,
		membershipMonitor: membershipMonitor,
		metricsHandler:    metricsHandler,
		healthServer:      healthServer,
		chasmRegistry:     chasmRegistry,
	}
}

// Start starts the service
func (s *Service) Start() {
	s.logger.Info("history starting")

	metrics.RestartCount.With(s.metricsHandler).Record(1)

	s.handler.Start()

	historyservice.RegisterHistoryServiceServer(s.server, s.handler)
	healthpb.RegisterHealthServer(s.server, s.healthServer)
	s.chasmRegistry.RegisterServices(s.server)

	// start as NOT_SERVING, update to SERVING after initial shards acquired
	s.healthServer.SetServingStatus(serviceName, healthpb.HealthCheckResponse_NOT_SERVING)
	readinessCtx, readinessCancel := context.WithCancel(context.Background())
	s.readinessCancel = readinessCancel
	go func() {
		if s.handler.controller.InitialShardsAcquired(readinessCtx) == nil {
			// add a few seconds for stabilization
			if util.InterruptibleSleep(readinessCtx, 5*time.Second) == nil {
				s.healthServer.SetServingStatus(serviceName, healthpb.HealthCheckResponse_SERVING)
			}
		}
	}()

	reflection.Register(s.server)

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
	s.readinessCancel()

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

	s.logger.Info("ShutdownHandler: Waiting for drain")
	if waitTime > 0 {
		time.Sleep(
			waitTime + // wait for membership change
				s.config.ShardLingerTimeLimit() + // after membership change shards may linger before close
				s.config.ShardFinalizerTimeout(), // and then take this long to run a finalizer
		)
	} else {
		time.Sleep(s.config.ShutdownDrainDuration())
	}

	// Stop shard controller. We should have waited long enough for all shards to realize they
	// lost ownership and close, but if not, this will definitely close them.
	s.logger.Info("ShutdownHandler: Initiating shardController shutdown")
	s.handler.controller.Stop()

	// All grpc handlers should be cancelled now. Give them a little time to return.
	t := time.AfterFunc(2*time.Second, func() {
		s.logger.Info("ShutdownHandler: Drain time expired, stopping all traffic")
		s.server.Stop()
	})
	s.server.GracefulStop()
	t.Stop()

	s.handler.Stop()
	s.visibilityManager.Close()

	s.logger.Info("history stopped")
}
