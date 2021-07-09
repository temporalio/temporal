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

	"github.com/uber-go/tally"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	esclient "go.temporal.io/server/common/persistence/elasticsearch/client"
	"go.temporal.io/server/common/resolver"
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

	// logger          log.Logger
	taggedLogger    log.Logger // todomigryz: rename to logger, unless untagged is required.
	throttledLogger log.Logger

	status  int32
	handler *Handler
	config  *Config

	server *grpc.Server
}

// BootstrapParams holds the set of parameters
// needed to bootstrap a service
type MatchingBootstrapParams struct {
Name            string
InstanceID      string
Logger          log.Logger
ThrottledLogger log.Logger
NamespaceLogger log.Logger

MetricsScope                 tally.Scope
MembershipFactoryInitializer resource.MembershipFactoryInitializerFunc
RPCFactory                   common.RPCFactory
AbstractDatastoreFactory     persistenceClient.AbstractDataStoreFactory
PersistenceConfig            config.Persistence
ClusterMetadataConfig        *config.ClusterMetadata
ReplicatorConfig             config.Replicator
ServerMetricsReporter        metrics.Reporter
SDKMetricsReporter           metrics.Reporter
MetricsClient                metrics.Client
ESClient                     esclient.Client
ESConfig                     *config.Elasticsearch
DynamicConfigClient          dynamicconfig.Client
DCRedirectionPolicy          config.DCRedirectionPolicy
SdkClient                    sdkclient.Client
ArchivalMetadata             archiver.ArchivalMetadata
ArchiverProvider             provider.ArchiverProvider
Authorizer                   authorization.Authorizer
ClaimMapper                  authorization.ClaimMapper
PersistenceServiceResolver   resolver.ServiceResolver
AudienceGetter               authorization.JWTAudienceMapper
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
	params *resource.BootstrapParams,
) (*Service, error) {
	metricsClient := params.MetricsClient
	throttledLoggerMaxRPS := serviceConfig.ThrottledLogRPS
	taggedLogger := log.With(logger, tag.Service(serviceName))
	throttledLogger := log.NewThrottledLogger(
		taggedLogger,
		func() float64 { return float64(throttledLoggerMaxRPS()) })

	persistenceMaxQPS := serviceConfig.PersistenceMaxQPS
	persistenceGlobalMaxQPS := serviceConfig.PersistenceGlobalMaxQPS

	// todomigryz: Injsect persistenceBean
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

	///////////////////////
	// Removing resource //
	serviceResource, err := resource.NewMatchingResource(
		params,
		taggedLogger,
		throttledLogger,
		common.MatchingServiceName,
		func(
			persistenceBean persistenceClient.Bean,
			searchAttributesProvider searchattribute.Provider,
			logger log.Logger,
		) (persistence.VisibilityManager, error) {
			return persistenceBean.GetVisibilityManager(), nil
		},
		persistenceBean,
		namespaceCache,
		clusterMetadata,
		metricsClient,
	)
	if err != nil {
		return nil, err
	}
	// Removing resource end //
	///////////////////////////


	// todomigryz: inject telemetry interceptor
	// todomigryz: Inject namespace cache, remove it from resource
	// todomigryz: Inject metricsClient, remove it from resource
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

	return &Service{
		Resource:     serviceResource,
		status:       common.DaemonStatusInitialized,
		config:       serviceConfig,
		server:       grpc.NewServer(grpcServerOptions...),
		handler:      NewHandler(serviceResource, serviceConfig),

		// logger:       logger,
		taggedLogger: taggedLogger,
		throttledLogger: throttledLogger,
	}, nil
}

// Start starts the service
func (s *Service) Start() {
	if !atomic.CompareAndSwapInt32(&s.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	logger := s.taggedLogger
	logger.Info("matching starting")

	// must start base service first
	s.Resource.Start()
	s.handler.Start()

	matchingservice.RegisterMatchingServiceServer(s.server, s.handler)
	healthpb.RegisterHealthServer(s.server, s.handler)

	listener := s.GetGRPCListener()
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
	s.GetMembershipMonitor().EvictSelf()
	s.taggedLogger.Info("ShutdownHandler: Waiting for others to discover I am unhealthy")
	time.Sleep(s.config.ShutdownDrainDuration())

	// TODO: Change this to GracefulStop when integration tests are refactored.
	s.server.Stop()

	s.handler.Stop()

	/////////////////////////////////////////////////
	// s.Resource.Stop() // todomigryz: inlined below
	//
	// todomigryz: ATTENTION this is another status coming from Resource
	// if !atomic.CompareAndSwapInt32(
	// 	&h.status,
	// 	common.DaemonStatusStarted,
	// 	common.DaemonStatusStopped,
	// ) {
	// 	return
	// }
	//
	// h.namespaceCache.Stop()
	// h.membershipMonitor.Stop()
	// h.ringpopChannel.Close()
	// h.runtimeMetricsReporter.Stop()
	// h.persistenceBean.Close()
	// if h.visibilityMgr != nil {
	// 	h.visibilityMgr.Close()
	// }
	// todomigryz: done inlining Resource.Stop
	/////////////////////////////////////////////////

	s.taggedLogger.Info("matching stopped")
}
