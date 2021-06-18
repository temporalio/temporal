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

package frontend

import (
	"math"
	"os"
	"sync/atomic"
	"time"

	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	espersistence "go.temporal.io/server/common/persistence/elasticsearch"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/frontend/configs"
)

// Config represents configuration for frontend service
type Config struct {
	NumHistoryShards             int32
	ESIndexName                  string
	PersistenceMaxQPS            dynamicconfig.IntPropertyFn
	PersistenceGlobalMaxQPS      dynamicconfig.IntPropertyFn
	VisibilityMaxPageSize        dynamicconfig.IntPropertyFnWithNamespaceFilter
	EnableVisibilitySampling     dynamicconfig.BoolPropertyFn
	VisibilityListMaxQPS         dynamicconfig.IntPropertyFnWithNamespaceFilter
	EnableReadVisibilityFromES   dynamicconfig.BoolPropertyFnWithNamespaceFilter
	ESVisibilityListMaxQPS       dynamicconfig.IntPropertyFnWithNamespaceFilter
	ESIndexMaxResultWindow       dynamicconfig.IntPropertyFn
	HistoryMaxPageSize           dynamicconfig.IntPropertyFnWithNamespaceFilter
	RPS                          dynamicconfig.IntPropertyFn
	MaxNamespaceRPSPerInstance   dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxNamespaceCountPerInstance dynamicconfig.IntPropertyFnWithNamespaceFilter
	GlobalNamespaceRPS           dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxIDLengthLimit             dynamicconfig.IntPropertyFn
	EnableClientVersionCheck     dynamicconfig.BoolPropertyFn
	MinRetention                 dynamicconfig.DurationPropertyFn
	DisallowQuery                dynamicconfig.BoolPropertyFnWithNamespaceFilter
	ShutdownDrainDuration        dynamicconfig.DurationPropertyFn

	MaxBadBinaries dynamicconfig.IntPropertyFnWithNamespaceFilter

	// security protection settings
	DisableListVisibilityByFilter dynamicconfig.BoolPropertyFnWithNamespaceFilter

	// size limit system protection
	BlobSizeLimitError dynamicconfig.IntPropertyFnWithNamespaceFilter
	BlobSizeLimitWarn  dynamicconfig.IntPropertyFnWithNamespaceFilter

	ThrottledLogRPS dynamicconfig.IntPropertyFn

	// Namespace specific config
	EnableNamespaceNotActiveAutoForwarding dynamicconfig.BoolPropertyFnWithNamespaceFilter

	SearchAttributesNumberOfKeysLimit dynamicconfig.IntPropertyFnWithNamespaceFilter
	SearchAttributesSizeOfValueLimit  dynamicconfig.IntPropertyFnWithNamespaceFilter
	SearchAttributesTotalSizeLimit    dynamicconfig.IntPropertyFnWithNamespaceFilter

	// DefaultWorkflowRetryPolicy represents default values for unset fields on a Workflow's
	// specified RetryPolicy
	DefaultWorkflowRetryPolicy dynamicconfig.MapPropertyFnWithNamespaceFilter

	// VisibilityArchival system protection
	VisibilityArchivalQueryMaxPageSize dynamicconfig.IntPropertyFn

	SendRawWorkflowHistory dynamicconfig.BoolPropertyFnWithNamespaceFilter

	// DefaultWorkflowTaskTimeout the default workflow task timeout
	DefaultWorkflowTaskTimeout dynamicconfig.DurationPropertyFnWithNamespaceFilter

	// EnableServerVersionCheck disables periodic version checking performed by the frontend
	EnableServerVersionCheck dynamicconfig.BoolPropertyFn

	// EnableTokenNamespaceEnforcement enables enforcement that namespace in completion token matches namespace of the request
	EnableTokenNamespaceEnforcement dynamicconfig.BoolPropertyFn

	// gRPC keep alive options
	// If a client pings too frequently, terminate the connection.
	KeepAliveMinTime dynamicconfig.DurationPropertyFn
	//  Allow pings even when there are no active streams (RPCs)
	KeepAlivePermitWithoutStream dynamicconfig.BoolPropertyFn
	// Close the connection if a client is idle.
	KeepAliveMaxConnectionIdle dynamicconfig.DurationPropertyFn
	// Close the connection if it is too old.
	KeepAliveMaxConnectionAge dynamicconfig.DurationPropertyFn
	// Additive period after MaxConnectionAge after which the connection will be forcibly closed.
	KeepAliveMaxConnectionAgeGrace dynamicconfig.DurationPropertyFn
	// Ping the client if it is idle to ensure the connection is still active.
	KeepAliveTime dynamicconfig.DurationPropertyFn
	// Wait for the ping ack before assuming the connection is dead.
	KeepAliveTimeout dynamicconfig.DurationPropertyFn
}

// NewConfig returns new service config with default values
func NewConfig(dc *dynamicconfig.Collection, numHistoryShards int32, esIndexName string, enableReadFromES bool) *Config {
	return &Config{
		NumHistoryShards:                       numHistoryShards,
		ESIndexName:                            esIndexName,
		PersistenceMaxQPS:                      dc.GetIntProperty(dynamicconfig.FrontendPersistenceMaxQPS, 2000),
		PersistenceGlobalMaxQPS:                dc.GetIntProperty(dynamicconfig.FrontendPersistenceGlobalMaxQPS, 0),
		VisibilityMaxPageSize:                  dc.GetIntPropertyFilteredByNamespace(dynamicconfig.FrontendVisibilityMaxPageSize, 1000),
		EnableVisibilitySampling:               dc.GetBoolProperty(dynamicconfig.EnableVisibilitySampling, true),
		VisibilityListMaxQPS:                   dc.GetIntPropertyFilteredByNamespace(dynamicconfig.FrontendVisibilityListMaxQPS, 30),
		EnableReadVisibilityFromES:             dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.EnableReadVisibilityFromES, enableReadFromES),
		ESVisibilityListMaxQPS:                 dc.GetIntPropertyFilteredByNamespace(dynamicconfig.FrontendESVisibilityListMaxQPS, 10),
		ESIndexMaxResultWindow:                 dc.GetIntProperty(dynamicconfig.FrontendESIndexMaxResultWindow, 10000),
		HistoryMaxPageSize:                     dc.GetIntPropertyFilteredByNamespace(dynamicconfig.FrontendHistoryMaxPageSize, common.GetHistoryMaxPageSize),
		RPS:                                    dc.GetIntProperty(dynamicconfig.FrontendRPS, 2400),
		MaxNamespaceRPSPerInstance:             dc.GetIntPropertyFilteredByNamespace(dynamicconfig.FrontendMaxNamespaceRPSPerInstance, 2400),
		MaxNamespaceCountPerInstance:           dc.GetIntPropertyFilteredByNamespace(dynamicconfig.FrontendMaxNamespaceCountPerInstance, 1200),
		GlobalNamespaceRPS:                     dc.GetIntPropertyFilteredByNamespace(dynamicconfig.FrontendGlobalNamespaceRPS, 0),
		MaxIDLengthLimit:                       dc.GetIntProperty(dynamicconfig.MaxIDLengthLimit, 1000),
		MaxBadBinaries:                         dc.GetIntPropertyFilteredByNamespace(dynamicconfig.FrontendMaxBadBinaries, namespace.MaxBadBinaries),
		DisableListVisibilityByFilter:          dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.DisableListVisibilityByFilter, false),
		BlobSizeLimitError:                     dc.GetIntPropertyFilteredByNamespace(dynamicconfig.BlobSizeLimitError, 2*1024*1024),
		BlobSizeLimitWarn:                      dc.GetIntPropertyFilteredByNamespace(dynamicconfig.BlobSizeLimitWarn, 256*1024),
		ThrottledLogRPS:                        dc.GetIntProperty(dynamicconfig.FrontendThrottledLogRPS, 20),
		ShutdownDrainDuration:                  dc.GetDurationProperty(dynamicconfig.FrontendShutdownDrainDuration, 0),
		EnableNamespaceNotActiveAutoForwarding: dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.EnableNamespaceNotActiveAutoForwarding, true),
		EnableClientVersionCheck:               dc.GetBoolProperty(dynamicconfig.EnableClientVersionCheck, true),
		SearchAttributesNumberOfKeysLimit:      dc.GetIntPropertyFilteredByNamespace(dynamicconfig.SearchAttributesNumberOfKeysLimit, 100),
		SearchAttributesSizeOfValueLimit:       dc.GetIntPropertyFilteredByNamespace(dynamicconfig.SearchAttributesSizeOfValueLimit, 2*1024),
		SearchAttributesTotalSizeLimit:         dc.GetIntPropertyFilteredByNamespace(dynamicconfig.SearchAttributesTotalSizeLimit, 40*1024),
		MinRetention:                           dc.GetDurationProperty(dynamicconfig.MinRetention, namespace.MinRetention),
		VisibilityArchivalQueryMaxPageSize:     dc.GetIntProperty(dynamicconfig.VisibilityArchivalQueryMaxPageSize, 10000),
		DisallowQuery:                          dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.DisallowQuery, false),
		SendRawWorkflowHistory:                 dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.SendRawWorkflowHistory, false),
		DefaultWorkflowRetryPolicy:             dc.GetMapPropertyFnWithNamespaceFilter(dynamicconfig.DefaultWorkflowRetryPolicy, common.GetDefaultRetryPolicyConfigOptions()),
		DefaultWorkflowTaskTimeout:             dc.GetDurationPropertyFilteredByNamespace(dynamicconfig.DefaultWorkflowTaskTimeout, common.DefaultWorkflowTaskTimeout),
		EnableServerVersionCheck:               dc.GetBoolProperty(dynamicconfig.EnableServerVersionCheck, os.Getenv("TEMPORAL_VERSION_CHECK_DISABLED") == ""),
		EnableTokenNamespaceEnforcement:        dc.GetBoolProperty(dynamicconfig.EnableTokenNamespaceEnforcement, false),
		KeepAliveMinTime:                       dc.GetDurationProperty(dynamicconfig.KeepAliveMinTime, 10*time.Second),
		KeepAlivePermitWithoutStream:           dc.GetBoolProperty(dynamicconfig.KeepAlivePermitWithoutStream, true),
		KeepAliveMaxConnectionIdle:             dc.GetDurationProperty(dynamicconfig.KeepAliveMaxConnectionIdle, 2*time.Minute),
		KeepAliveMaxConnectionAge:              dc.GetDurationProperty(dynamicconfig.KeepAliveMaxConnectionAge, 5*time.Minute),
		KeepAliveMaxConnectionAgeGrace:         dc.GetDurationProperty(dynamicconfig.KeepAliveMaxConnectionAgeGrace, 70*time.Second),
		KeepAliveTime:                          dc.GetDurationProperty(dynamicconfig.KeepAliveTime, 1*time.Minute),
		KeepAliveTimeout:                       dc.GetDurationProperty(dynamicconfig.KeepAliveTimeout, 10*time.Second),
	}
}

// Service represents the frontend service
type Service struct {
	resource.Resource

	status int32
	config *Config

	handler        Handler
	adminHandler   *AdminHandler
	versionChecker *VersionChecker
	server         *grpc.Server

	serverMetricsReporter metrics.Reporter
	sdkMetricsReporter    metrics.Reporter
}

// NewService builds a new frontend service
func NewService(
	params *resource.BootstrapParams,
) (*Service, error) {

	isAdvancedVisExistInConfig := len(params.PersistenceConfig.AdvancedVisibilityStore) != 0
	serviceConfig := NewConfig(
		dynamicconfig.NewCollection(params.DynamicConfigClient, params.Logger),
		params.PersistenceConfig.NumHistoryShards,
		params.ESConfig.GetVisibilityIndex(),
		isAdvancedVisExistInConfig)

	params.PersistenceConfig.VisibilityConfig = &config.VisibilityConfig{
		VisibilityListMaxQPS: serviceConfig.VisibilityListMaxQPS,
		EnableSampling:       serviceConfig.EnableVisibilitySampling,
	}

	visibilityManagerInitializer := func(
		persistenceBean persistenceClient.Bean,
		searchAttributesProvider searchattribute.Provider,
		logger log.Logger,
	) (persistence.VisibilityManager, error) {
		visibilityFromDB := persistenceBean.GetVisibilityManager()

		var visibilityFromES persistence.VisibilityManager
		if params.ESConfig != nil {
			visibilityIndexName := params.ESConfig.GetVisibilityIndex()
			visibilityConfigForES := &config.VisibilityConfig{
				MaxQPS:                 serviceConfig.PersistenceMaxQPS,
				VisibilityListMaxQPS:   serviceConfig.ESVisibilityListMaxQPS,
				ESIndexMaxResultWindow: serviceConfig.ESIndexMaxResultWindow,
			}
			visibilityFromES = espersistence.NewVisibilityManager(visibilityIndexName, params.ESClient, visibilityConfigForES,
				searchAttributesProvider, nil, params.MetricsClient, logger)
		}
		return persistence.NewVisibilityManagerWrapper(
			visibilityFromDB,
			visibilityFromES,
			serviceConfig.EnableReadVisibilityFromES,
			dynamicconfig.GetStringPropertyFn(common.AdvancedVisibilityWritingModeOff), // frontend visibility never write
		), nil
	}

	serviceResource, err := resource.New(
		params,
		common.FrontendServiceName,
		serviceConfig.PersistenceMaxQPS,
		serviceConfig.PersistenceGlobalMaxQPS,
		serviceConfig.ThrottledLogRPS,
		visibilityManagerInitializer,
	)
	if err != nil {
		return nil, err
	}

	var namespaceReplicationQueue persistence.NamespaceReplicationQueue
	clusterMetadata := serviceResource.GetClusterMetadata()
	if clusterMetadata.IsGlobalNamespaceEnabled() {
		namespaceReplicationQueue = serviceResource.GetNamespaceReplicationQueue()
	}

	metricsInterceptor := interceptor.NewTelemetryInterceptor(
		serviceResource.GetNamespaceCache(),
		serviceResource.GetMetricsClient(),
		metrics.FrontendAPIMetricsScopes(),
		serviceResource.GetLogger(),
	)
	rateLimiterInterceptor := interceptor.NewRateLimitInterceptor(
		configs.NewRequestToRateLimiter(func() float64 { return float64(serviceConfig.RPS()) }),
		map[string]int{},
	)
	namespaceRateLimiterInterceptor := interceptor.NewNamespaceRateLimitInterceptor(
		serviceResource.GetNamespaceCache(),
		quotas.NewNamespaceRateLimiter(
			func(req quotas.Request) quotas.RequestRateLimiter {
				return configs.NewRequestToRateLimiter(func() float64 {
					return namespaceRPS(
						serviceConfig,
						serviceResource.GetFrontendServiceResolver(),
						req.Caller,
					)
				})
			},
		),
		map[string]int{},
	)
	namespaceCountLimiterInterceptor := interceptor.NewNamespaceCountLimitInterceptor(
		serviceResource.GetNamespaceCache(),
		serviceConfig.MaxNamespaceCountPerInstance,
		configs.ExecutionAPICountLimitOverride,
	)

	namespaceLogger := params.NamespaceLogger
	namespaceLogInterceptor := interceptor.NewNamespaceLogInterceptor(
		serviceResource.GetNamespaceCache(),
		namespaceLogger)

	kep := keepalive.EnforcementPolicy{
		MinTime:             serviceConfig.KeepAliveMinTime(),
		PermitWithoutStream: serviceConfig.KeepAlivePermitWithoutStream(),
	}
	var kp = keepalive.ServerParameters{
		MaxConnectionIdle:     serviceConfig.KeepAliveMaxConnectionIdle(),
		MaxConnectionAge:      serviceConfig.KeepAliveMaxConnectionAge(),
		MaxConnectionAgeGrace: serviceConfig.KeepAliveMaxConnectionAgeGrace(),
		Time:                  serviceConfig.KeepAliveTime(),
		Timeout:               serviceConfig.KeepAliveTimeout(),
	}

	grpcServerOptions, err := params.RPCFactory.GetFrontendGRPCServerOptions()
	if err != nil {
		params.Logger.Fatal("creating gRPC server options failed", tag.Error(err))
	}
	grpcServerOptions = append(
		grpcServerOptions,
		grpc.KeepaliveParams(kp),
		grpc.KeepaliveEnforcementPolicy(kep),
		grpc.ChainUnaryInterceptor(
			namespaceLogInterceptor.Intercept,
			rpc.ServiceErrorInterceptor,
			metricsInterceptor.Intercept,
			rateLimiterInterceptor.Intercept,
			namespaceRateLimiterInterceptor.Intercept,
			namespaceCountLimiterInterceptor.Intercept,
			metrics.NewServerMetricsContextInjectorInterceptor(),
			authorization.NewAuthorizationInterceptor(
				params.ClaimMapper,
				params.Authorizer,
				serviceResource.GetMetricsClient(),
				params.Logger,
				params.AudienceGetter,
			),
		),
	)

	wfHandler := NewWorkflowHandler(serviceResource, serviceConfig, namespaceReplicationQueue)
	handler := NewDCRedirectionHandler(wfHandler, params.DCRedirectionPolicy)

	return &Service{
		Resource:       serviceResource,
		status:         common.DaemonStatusInitialized,
		config:         serviceConfig,
		server:         grpc.NewServer(grpcServerOptions...),
		handler:        handler,
		adminHandler:   NewAdminHandler(serviceResource, params, serviceConfig),
		versionChecker: NewVersionChecker(serviceConfig, params.MetricsClient, serviceResource.GetClusterMetadataManager()),
	}, nil
}

// Start starts the service
func (s *Service) Start() {
	if !atomic.CompareAndSwapInt32(&s.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	logger := s.GetLogger()
	logger.Info("frontend starting")

	workflowservice.RegisterWorkflowServiceServer(s.server, s.handler)
	healthpb.RegisterHealthServer(s.server, s.handler)

	adminservice.RegisterAdminServiceServer(s.server, s.adminHandler)

	reflection.Register(s.server)

	// must start resource first
	s.Resource.Start()
	s.adminHandler.Start()
	s.versionChecker.Start()

	listener := s.GetGRPCListener()
	logger.Info("Starting to serve on frontend listener")
	if err := s.server.Serve(listener); err != nil {
		logger.Fatal("Failed to serve on frontend listener", tag.Error(err))
	}
}

// Stop stops the service
func (s *Service) Stop() {
	logger := s.GetLogger()

	if !atomic.CompareAndSwapInt32(&s.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	// initiate graceful shutdown:
	// 1. Fail rpc health check, this will cause client side load balancer to stop forwarding requests to this node
	// 2. wait for failure detection time
	// 3. stop taking new requests by returning InternalServiceError
	// 4. Wait for a second
	// 5. Stop everything forcefully and return

	requestDrainTime := common.MinDuration(time.Second, s.config.ShutdownDrainDuration())
	failureDetectionTime := common.MaxDuration(0, s.config.ShutdownDrainDuration()-requestDrainTime)

	logger.Info("ShutdownHandler: Updating rpc health status to ShuttingDown")
	s.handler.UpdateHealthStatus(HealthStatusShuttingDown)

	logger.Info("ShutdownHandler: Waiting for others to discover I am unhealthy")
	time.Sleep(failureDetectionTime)

	s.adminHandler.Stop()
	s.versionChecker.Stop()

	logger.Info("ShutdownHandler: Draining traffic")
	time.Sleep(requestDrainTime)

	// TODO: Change this to GracefulStop when integration tests are refactored.
	s.server.Stop()
	s.Resource.Stop()

	if s.serverMetricsReporter != nil {
		s.serverMetricsReporter.Stop(logger)
	}

	if s.sdkMetricsReporter != nil {
		s.sdkMetricsReporter.Stop(logger)
	}

	logger.Info("frontend stopped")
}

func namespaceRPS(
	config *Config,
	frontendResolver membership.ServiceResolver,
	namespace string,
) float64 {
	hostRPS := float64(config.MaxNamespaceRPSPerInstance(namespace))
	globalRPS := float64(config.GlobalNamespaceRPS(namespace))
	hosts := float64(numFrontendHosts(frontendResolver))

	rps := hostRPS + globalRPS*math.Exp((1.0-hosts)/8.0)
	return rps
}

func numFrontendHosts(
	frontendResolver membership.ServiceResolver,
) int {

	defaultHosts := 1
	if frontendResolver == nil {
		return defaultHosts
	}

	ringSize := frontendResolver.MemberCount()
	if ringSize < defaultHosts {
		return defaultHosts
	}
	return ringSize
}
