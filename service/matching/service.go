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
	"go.temporal.io/server/common/rpc/interceptor"
)

// Service represents the matching service
type (
	Service struct {
		logger          log.Logger // todomigryz: rename to logger, unless untagged is required.
		throttledLogger log.Logger // todomigryz: this should not be required. Transient dependency?

		status  int32
		handler *Handler
		config  *Config

		server *grpc.Server

		metricsScope           tally.Scope
		runtimeMetricsReporter *metrics.RuntimeMetricsReporter
		membershipMonitor      membership.Monitor
		namespaceCache         cache.NamespaceCache
		visibilityMgr          persistence.VisibilityManager
		// todomigryz: trying to replace with persistenceClient.Bean
		// persistenceBean        *persistenceClient.BeanImpl
		persistenceBean        persistenceClient.Bean
		ringpopChannel         *tchannel.Channel
		grpcListener           net.Listener
		clientBean             client.Bean // needed for onebox. Should remove if possible.
	}

	TaggedLogger log.Logger
	// MatchingMetricsClient metrics.Client
)

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
	params *resource.BootstrapParams, // todomigryz: replace generic BootstrapParams with factory or constructed object
	logger log.Logger,
	taggedLogger TaggedLogger,
	throttledLogger log.ThrottledLogger,
	serviceConfig *Config,
	metricsClient metrics.Client,
	persistenceBean persistenceClient.Bean,
	clusterMetadata cluster.Metadata,
	namespaceCache cache.NamespaceCache,
	metricsInterceptor *interceptor.TelemetryInterceptor,
	rateLimitInterceptor *interceptor.RateLimitInterceptor,
	membershipFactory resource.MembershipMonitorFactory,
	rpcFactory common.RPCFactory,
	grpcServer *grpc.Server,
) (*Service, error) {

	// todomigryz: might need to close grpcListener, unless it is shared
	grpcListener := rpcFactory.GetGRPCListener()

	dynamicCollection := dynamicconfig.NewCollection(params.DynamicConfigClient, taggedLogger)

	membershipMonitor, err := membershipFactory.GetMembershipMonitor()
	if err != nil {
		return nil, err
	}

	numShards := params.PersistenceConfig.NumHistoryShards
	clientBean, err := client.NewClientBean(
		client.NewRPCClientFactory(
			rpcFactory,
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

	ringpopChannel := rpcFactory.GetRingpopChannel()

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

	// todomigryz: @Alex visibility should not be present in Matching
	// visibilityManagerInitializer := func(
	// 	persistenceBean persistenceClient.Bean,
	// 	searchAttributesProvider searchattribute.Provider,
	// 	logger log.Logger,
	// ) (persistence.VisibilityManager, error) {
	// 	return persistenceBean.GetVisibilityManager(), nil
	// }
	//
	// saProvider := persistence.NewSearchAttributesManager(clock.NewRealTimeSource(), persistenceBean.GetClusterMetadataManager())
	//
	// visibilityMgr, err := visibilityManagerInitializer(
	// 	persistenceBean,
	// 	saProvider,
	// 	taggedLogger,
	// )
	// if err != nil {
	// 	return nil, err
	// }

	// /////////////////////////////////////
	// // todomigryz:  Removing resource //
	// // todomigryz: do we really need the rest of Resource???
	// serviceResource, err := resource.NewMatchingResource(
	// 	params,
	// 	logger,
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
		historyClient,                    // todomigryz: replaced serviceResource.GetHistoryClient(),
		matchingRawClient,                // todomigryz: replaced serviceResource.GetMatchingRawClient(), // Use non retry client inside matching
		serviceConfig,
		taggedLogger,
		metricsClient,           // todomigryz: replaced serviceResource.GetMetricsClient(),
		namespaceCache,          // todomigryz: replaced serviceResource.GetNamespaceCache(),
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
		status:  common.DaemonStatusInitialized,
		config:  serviceConfig,
		server:  grpcServer,
		handler: handler,

		// logger:       logger,
		logger:          taggedLogger,
		throttledLogger: throttledLogger,

		metricsScope:           params.MetricsScope,
		runtimeMetricsReporter: runtimeMetricsReporter,
		membershipMonitor:      membershipMonitor,
		namespaceCache:         namespaceCache,
		persistenceBean:        persistenceBean,
		ringpopChannel:         ringpopChannel,
		grpcListener:           grpcListener,
		clientBean:             clientBean,
	}, nil
}

func (s *Service) GetClientBean() client.Bean {
	return s.clientBean
}

// Start starts the service
func (s *Service) Start() {
	if !atomic.CompareAndSwapInt32(&s.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	logger := s.logger
	logger.Info("matching starting")

	// ////////////////////////////////////
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
		s.logger.Fatal("fail to get host info from membership monitor", tag.Error(err))
	}
	// h.hostInfo = hostInfo

	// The service is now started up
	s.logger.Info("Service resources started", tag.Address(hostInfo.GetAddress()))

	// seed the random generator once for this service
	rand.Seed(time.Now().UnixNano())
	// todomigryz: inline Resource.Start() done
	// ////////////////////////////////////

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
	s.logger.Info("ShutdownHandler: Evicting self from membership ring")
	s.membershipMonitor.EvictSelf()
	s.logger.Info("ShutdownHandler: Waiting for others to discover I am unhealthy")
	time.Sleep(s.config.ShutdownDrainDuration())

	// TODO: Change this to GracefulStop when integration tests are refactored.
	s.server.Stop()

	s.handler.Stop()

	// ///////////////////////////////////////////////
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
	// ///////////////////////////////////////////////

	s.logger.Info("matching stopped")
}
