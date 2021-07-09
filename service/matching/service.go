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

	"github.com/uber-go/tally"
	"github.com/uber/tchannel-go"
	"go.temporal.io/server/client"
	"go.temporal.io/server/client/history"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/membership"
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
	// logger          log.Logger
	taggedLogger    log.Logger // todomigryz: rename to logger, unless untagged is required.
	throttledLogger log.Logger // todomigryz: this should not be required. Transient dependency?

	// todomigryz: added from Resource Done
	// //////////////////////////////////////////////////

	status  int32
	handler *Handler
	config  *Config

	server *grpc.Server

	metricsScope           tally.Scope
	runtimeMetricsReporter *metrics.RuntimeMetricsReporter
	membershipMonitor      membership.Monitor
	namespaceCache         cache.NamespaceCache
	visibilityMgr          persistence.VisibilityManager
	persistenceBean        *persistenceClient.BeanImpl
	ringpopChannel         *tchannel.Channel
	grpcListener           net.Listener
}

// todomigryz: check if I can decouple BootstrapParams.
// todomigryz: current steps:
//  1. Flatten BootstrapParams into a list of arguments
//  2. Remove unused arguments
//  3. Extract leaf dependencies as arguments and respective providers
//  4. Repeat 3 while possible.
//  5. ...
//  6. Profit
// todomigryz: seems that we do not store BootstrapParams, so it should be possible to flatten it.
// todomigryz: keep in mind: providers are extracted from both: BootstrapParams, NewService and resource.New.
//  When debuging, compare all three for relevant initial implementation.
// NewService builds a new matching service
func NewService(
	logger log.Logger, // comes from BootstrapParams.Logger
	serviceConfig *Config,
	params *resource.BootstrapParams, // todomigryz: replace generic BootstrapParams with factory or constructed object
) (*Service, error) {
	metricsClient := params.MetricsClient // replaces Resource.GetMetricsClient
	throttledLoggerMaxRPS := serviceConfig.ThrottledLogRPS
	taggedLogger := log.With(logger, tag.Service(serviceName)) // replaces Resource.GetLogger
	throttledLogger := log.NewThrottledLogger(                 // replaces Resource.GetThrottledLogger
		taggedLogger,
		func() float64 { return float64(throttledLoggerMaxRPS()) })

	persistenceMaxQPS := serviceConfig.PersistenceMaxQPS
	persistenceGlobalMaxQPS := serviceConfig.PersistenceGlobalMaxQPS

	persistenceBean, err := persistenceClient.NewBeanFromFactory(persistenceClient.NewFactory(
		&params.PersistenceConfig,
		params.PersistenceServiceResolver,
		func(...dynamicconfig.FilterOption) int {
			if persistenceGlobalMaxQPS() > 0 {
				// TODO: We have a bootstrap issue to correctly find memberCount.  Membership relies on
				// persistence to bootstrap membership ring, so we cannot have persistence rely on membership
				// as it will cause circular dependency.
				// ringSize, err := membershipMonitor.GetMemberCount(serviceName)
				// if err == nil && ringSize > 0 {
				// 	avgQuota := common.MaxInt(persistenceGlobalMaxQPS()/ringSize, 1)
				// 	return common.MinInt(avgQuota, persistenceMaxQPS())
				// }
			}
			return persistenceMaxQPS()
		},
		params.AbstractDatastoreFactory,
		params.ClusterMetadataConfig.CurrentClusterName,
		metricsClient,
		taggedLogger,
	))
	if err != nil {
		return nil, err
	}

	clusterMetadata := cluster.NewMetadata(
		params.ClusterMetadataConfig.EnableGlobalNamespace,
		params.ClusterMetadataConfig.FailoverVersionIncrement,
		params.ClusterMetadataConfig.MasterClusterName,
		params.ClusterMetadataConfig.CurrentClusterName,
		params.ClusterMetadataConfig.ClusterInformation,
	)

	// todomigryz: inject NamespaceCache
	namespaceCache := cache.NewNamespaceCache(
		persistenceBean.GetMetadataManager(),
		clusterMetadata,
		metricsClient,
		taggedLogger,
	)

	metricsInterceptor := interceptor.NewTelemetryInterceptor(
		namespaceCache,
		metricsClient,
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

	grpcServer := grpc.NewServer(grpcServerOptions...)
	grpcListener := params.RPCFactory.GetGRPCListener()

	dynamicCollection := dynamicconfig.NewCollection(params.DynamicConfigClient, taggedLogger)

	membershipFactory, err := params.MembershipFactoryInitializer(persistenceBean, taggedLogger)
	if err != nil {
		return nil, err
	}

	membershipMonitor, err := membershipFactory.GetMembershipMonitor()
	if err != nil {
		return nil, err
	}

	numShards := params.PersistenceConfig.NumHistoryShards
	clientBean, err := client.NewClientBean(
		client.NewRPCClientFactory(
			params.RPCFactory,
			membershipMonitor,
			metricsClient, // replaced params.MetricsClient,
			dynamicCollection,
			numShards,
			taggedLogger,
		),
		clusterMetadata,
	)
	if err != nil {
		return nil, err
	}

	matchingRawClient, err := clientBean.GetMatchingClient(namespaceCache.GetNamespaceName)
	if err != nil {
		return nil, err
	}

	matchingServiceResolver, err := membershipMonitor.GetResolver(common.MatchingServiceName)
	if err != nil {
		return nil, err
	}

	// todomigryz: @Alex how does this work?
	ringpopChannel := params.RPCFactory.GetRingpopChannel()

	historyRawClient := clientBean.GetHistoryClient()
	historyClient := history.NewRetryableClient(
		historyRawClient,
		common.CreateHistoryServiceRetryPolicy(),
		common.IsWhitelistServiceTransientError,
	)

	runtimeMetricsReporter := metrics.NewRuntimeMetricsReporter(
		params.MetricsScope,
		time.Minute,
		taggedLogger,
		params.InstanceID,
	)


	visibilityManagerInitializer := func(
		persistenceBean persistenceClient.Bean,
		searchAttributesProvider searchattribute.Provider,
		logger log.Logger,
	) (persistence.VisibilityManager, error) {
		return persistenceBean.GetVisibilityManager(), nil
	}

	saProvider := persistence.NewSearchAttributesManager(clock.NewRealTimeSource(), persistenceBean.GetClusterMetadataManager())

	visibilityMgr, err := visibilityManagerInitializer(
		persistenceBean,
		saProvider,
		taggedLogger,
	)
	if err != nil {
		return nil, err
	}

	// /////////////////////////////////////
	// // todomigryz:  Removing resource //
	// // todomigryz: do we really need the rest of Resource???
	// serviceResource, err := resource.NewMatchingResource(
	// 	params,
	// 	taggedLogger,
	// 	throttledLogger,
	// 	common.MatchingServiceName,
	// 	persistenceBean,
	// 	namespaceCache,
	// 	clusterMetadata,
	// 	metricsClient,
	// 	membershipMonitor,
	// 	clientBean,
	// 	numShards,
	// 	matchingRawClient,
	// 	matchingServiceResolver,
	// 	historyClient,
	// 	historyRawClient,
	// 	runtimeMetricsReporter,
	// 	visibilityMgr,
	// 	saProvider,
	// 	ringpopChannel,
	// 	grpcListener,
	// 	)
	// if err != nil {
	// 	return nil, err
	// }
	// // Removing resource end //
	// ///////////////////////////


	engine := NewEngine(
		persistenceBean.GetTaskManager(), // todomigryz: replaced serviceResource.GetTaskManager(),
		historyClient, // todomigryz: replaced serviceResource.GetHistoryClient(),
		matchingRawClient, // todomigryz: replaced serviceResource.GetMatchingRawClient(), // Use non retry client inside matching
		serviceConfig,
		taggedLogger,
		metricsClient, // todomigryz: replaced serviceResource.GetMetricsClient(),
		namespaceCache, // todomigryz: replaced serviceResource.GetNamespaceCache(),
		matchingServiceResolver, // todomigryz: replaced serviceResource.GetMatchingServiceResolver(),
	)

	handler := NewHandler(
		serviceConfig,
		taggedLogger,
		throttledLogger,
		metricsClient,
		engine,
		namespaceCache,
	)


	return &Service{
		status:       common.DaemonStatusInitialized,
		config:       serviceConfig,
		server:       grpcServer,
		handler:      handler,

		// logger:       logger,
		taggedLogger: taggedLogger,
		throttledLogger: throttledLogger,

		metricsScope: params.MetricsScope,
		runtimeMetricsReporter: runtimeMetricsReporter,
		membershipMonitor: membershipMonitor,
		namespaceCache: namespaceCache,
		visibilityMgr: visibilityMgr,
		persistenceBean: persistenceBean,
		ringpopChannel: ringpopChannel,
		grpcListener: grpcListener,
	}, nil
}

// Start starts the service
func (s *Service) Start() {
	if !atomic.CompareAndSwapInt32(&s.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	logger := s.taggedLogger
	logger.Info("matching starting")


	//////////////////////////////////////
	// todomigryz: inline Resource.Start()
	// must start base service first
	// s.Resource.Start()
	s.metricsScope.Counter(metrics.RestartCount).Inc(1)
	s.runtimeMetricsReporter.Start()

	s.membershipMonitor.Start()
	s.namespaceCache.Start()

	// todomigryz: remove if hostInfo not used
	hostInfo, err := s.membershipMonitor.WhoAmI()
	if err != nil {
		s.taggedLogger.Fatal("fail to get host info from membership monitor", tag.Error(err))
	}
	// h.hostInfo = hostInfo

	// The service is now started up
	s.taggedLogger.Info("Service resources started", tag.Address(hostInfo.GetAddress()))

	// seed the random generator once for this service
	rand.Seed(time.Now().UnixNano())
	// todomigryz: inline Resource.Start() done
	//////////////////////////////////////

	s.handler.Start()

	matchingservice.RegisterMatchingServiceServer(s.server, s.handler)
	healthpb.RegisterHealthServer(s.server, s.handler)

	listener := s.grpcListener
	logger.Info("Starting to serve on matching listener")
	if err := s.server.Serve(listener); err != nil {
		logger.Fatal("Failed to serve on matching listener", tag.Error(err))
	}
}

// Stop stops the service
func (s *Service) Stop() {
	if !atomic.CompareAndSwapInt32(
		&s.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	// remove self from membership ring and wait for traffic to drain
	s.taggedLogger.Info("ShutdownHandler: Evicting self from membership ring")
	s.membershipMonitor.EvictSelf()
	s.taggedLogger.Info("ShutdownHandler: Waiting for others to discover I am unhealthy")
	time.Sleep(s.config.ShutdownDrainDuration())

	// TODO: Change this to GracefulStop when integration tests are refactored.
	s.server.Stop()

	s.handler.Stop()

	/////////////////////////////////////////////////
	// s.Resource.Stop() // todomigryz: inlined below

	s.namespaceCache.Stop()
	s.membershipMonitor.Stop()
	s.ringpopChannel.Close() // todomigryz: we do not start this in Start()
	s.runtimeMetricsReporter.Stop()
	s.persistenceBean.Close() // todomigryz: we do not start this in Start()

	// todomigryz: @Alex why do we need visibilityMgr in matching
	if s.visibilityMgr != nil {
		s.visibilityMgr.Close()
	}
	// todomigryz: done inlining Resource.Stop
	/////////////////////////////////////////////////

	s.taggedLogger.Info("matching stopped")
}
