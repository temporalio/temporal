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
	"os"
	"sync/atomic"
	"time"

	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"go.temporal.io/server/common/config"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	espersistence "go.temporal.io/server/common/persistence/elasticsearch"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/common/searchattribute"
)

// Config represents configuration for frontend service
type Config struct {
	NumHistoryShards             int32
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
	MinRetentionDays             dynamicconfig.IntPropertyFn
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

	// ValidSearchAttributes is legal indexed keys that can be used in list APIs
	ValidSearchAttributes             dynamicconfig.MapPropertyFn
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
}

// NewConfig returns new service config with default values
func NewConfig(dc *dynamicconfig.Collection, numHistoryShards int32, enableReadFromES bool) *Config {
	return &Config{
		NumHistoryShards:                       numHistoryShards,
		PersistenceMaxQPS:                      dc.GetIntProperty(dynamicconfig.FrontendPersistenceMaxQPS, 2000),
		PersistenceGlobalMaxQPS:                dc.GetIntProperty(dynamicconfig.FrontendPersistenceGlobalMaxQPS, 0),
		VisibilityMaxPageSize:                  dc.GetIntPropertyFilteredByNamespace(dynamicconfig.FrontendVisibilityMaxPageSize, 1000),
		EnableVisibilitySampling:               dc.GetBoolProperty(dynamicconfig.EnableVisibilitySampling, true),
		VisibilityListMaxQPS:                   dc.GetIntPropertyFilteredByNamespace(dynamicconfig.FrontendVisibilityListMaxQPS, 30),
		EnableReadVisibilityFromES:             dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.EnableReadVisibilityFromES, enableReadFromES),
		ESVisibilityListMaxQPS:                 dc.GetIntPropertyFilteredByNamespace(dynamicconfig.FrontendESVisibilityListMaxQPS, 10),
		ESIndexMaxResultWindow:                 dc.GetIntProperty(dynamicconfig.FrontendESIndexMaxResultWindow, 10000),
		HistoryMaxPageSize:                     dc.GetIntPropertyFilteredByNamespace(dynamicconfig.FrontendHistoryMaxPageSize, common.GetHistoryMaxPageSize),
		RPS:                                    dc.GetIntProperty(dynamicconfig.FrontendRPS, 1200),
		MaxNamespaceRPSPerInstance:             dc.GetIntPropertyFilteredByNamespace(dynamicconfig.FrontendMaxNamespaceRPSPerInstance, 1200),
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
		ValidSearchAttributes:                  dc.GetMapProperty(dynamicconfig.ValidSearchAttributes, searchattribute.GetDefaultTypeMap()),
		SearchAttributesNumberOfKeysLimit:      dc.GetIntPropertyFilteredByNamespace(dynamicconfig.SearchAttributesNumberOfKeysLimit, 100),
		SearchAttributesSizeOfValueLimit:       dc.GetIntPropertyFilteredByNamespace(dynamicconfig.SearchAttributesSizeOfValueLimit, 2*1024),
		SearchAttributesTotalSizeLimit:         dc.GetIntPropertyFilteredByNamespace(dynamicconfig.SearchAttributesTotalSizeLimit, 40*1024),
		MinRetentionDays:                       dc.GetIntProperty(dynamicconfig.MinRetentionDays, namespace.MinRetentionDays),
		VisibilityArchivalQueryMaxPageSize:     dc.GetIntProperty(dynamicconfig.VisibilityArchivalQueryMaxPageSize, 10000),
		DisallowQuery:                          dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.DisallowQuery, false),
		SendRawWorkflowHistory:                 dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.SendRawWorkflowHistory, false),
		DefaultWorkflowRetryPolicy:             dc.GetMapPropertyFnWithNamespaceFilter(dynamicconfig.DefaultWorkflowRetryPolicy, common.GetDefaultRetryPolicyConfigOptions()),
		DefaultWorkflowTaskTimeout:             dc.GetDurationPropertyFilteredByNamespace(dynamicconfig.DefaultWorkflowTaskTimeout, common.DefaultWorkflowTaskTimeout),
		EnableServerVersionCheck:               dc.GetBoolProperty(dynamicconfig.EnableServerVersionCheck, os.Getenv("TEMPORAL_VERSION_CHECK_DISABLED") == ""),
		EnableTokenNamespaceEnforcement:        dc.GetBoolProperty(dynamicconfig.EnableTokenNamespaceEnforcement, false),
	}
}

// Service represents the frontend service
type Service struct {
	resource.Resource

	status int32
	config *Config
	params *resource.BootstrapParams

	handler        Handler
	adminHandler   *AdminHandler
	versionChecker *VersionChecker
	server         *grpc.Server
}

// NewService builds a new frontend service
func NewService(
	params *resource.BootstrapParams,
) (resource.Resource, error) {

	isAdvancedVisExistInConfig := len(params.PersistenceConfig.AdvancedVisibilityStore) != 0
	serviceConfig := NewConfig(dynamicconfig.NewCollection(params.DynamicConfig, params.Logger), params.PersistenceConfig.NumHistoryShards, isAdvancedVisExistInConfig)

	params.PersistenceConfig.VisibilityConfig = &config.VisibilityConfig{
		VisibilityListMaxQPS:  serviceConfig.VisibilityListMaxQPS,
		EnableSampling:        serviceConfig.EnableVisibilitySampling,
		ValidSearchAttributes: serviceConfig.ValidSearchAttributes,
	}

	visibilityManagerInitializer := func(
		persistenceBean persistenceClient.Bean,
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
				ValidSearchAttributes:  serviceConfig.ValidSearchAttributes,
			}
			visibilityFromES = espersistence.NewVisibilityManager(visibilityIndexName, params.ESClient, visibilityConfigForES,
				nil, params.MetricsClient, logger)
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
	logger.Info("frontend starting")

	var namespaceReplicationQueue persistence.NamespaceReplicationQueue
	clusterMetadata := s.GetClusterMetadata()
	if clusterMetadata.IsGlobalNamespaceEnabled() {
		namespaceReplicationQueue = s.GetNamespaceReplicationQueue()
	}

	metricsInterceptor := interceptor.NewTelemetryInterceptor(
		s.Resource.GetMetricsClient(),
		metrics.FrontendAPIMetricsScopes(),
		s.Resource.GetLogger(),
	)
	rateLimiterInterceptor := interceptor.NewRateLimitInterceptor(
		func() float64 { return float64(s.config.RPS()) },
		APIRateLimitOverride,
	)
	namespaceRateLimiterInterceptor := interceptor.NewNamespaceRateLimitInterceptor(
		func(namespace string) float64 {
			return float64(s.config.MaxNamespaceRPSPerInstance(namespace))
		},
		APIRateLimitOverride,
	)
	namespaceCountLimiterInterceptor := interceptor.NewNamespaceCountLimitInterceptor(
		s.config.MaxNamespaceCountPerInstance,
		APICountLimitOverride,
	)

	opts, err := s.params.RPCFactory.GetFrontendGRPCServerOptions()
	if err != nil {
		logger.Fatal("creating grpc server options failed", tag.Error(err))
	}
	opts = append(
		opts,
		grpc.ChainUnaryInterceptor(
			rpc.ServiceErrorInterceptor,
			metricsInterceptor.Intercept,
			rateLimiterInterceptor.Intercept,
			namespaceRateLimiterInterceptor.Intercept,
			namespaceCountLimiterInterceptor.Intercept,
			authorization.NewAuthorizationInterceptor(
				s.params.ClaimMapper,
				s.params.Authorizer,
				s.Resource.GetMetricsClient(),
				s.GetLogger(),
			),
		),
	)
	s.server = grpc.NewServer(opts...)

	wfHandler := NewWorkflowHandler(s, s.config, namespaceReplicationQueue)
	s.handler = NewDCRedirectionHandler(wfHandler, s.params.DCRedirectionPolicy)

	workflowservice.RegisterWorkflowServiceServer(s.server, s.handler)
	healthpb.RegisterHealthServer(s.server, s.handler)

	s.adminHandler = NewAdminHandler(s, s.params, s.config)
	adminservice.RegisterAdminServiceServer(s.server, s.adminHandler)

	reflection.Register(s.server)

	s.versionChecker = NewVersionChecker(s, s.params, s.config)

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

	s.GetLogger().Info("ShutdownHandler: Updating rpc health status to ShuttingDown")
	s.handler.UpdateHealthStatus(HealthStatusShuttingDown)

	s.GetLogger().Info("ShutdownHandler: Waiting for others to discover I am unhealthy")
	time.Sleep(failureDetectionTime)

	s.adminHandler.Stop()
	s.versionChecker.Stop()

	s.GetLogger().Info("ShutdownHandler: Draining traffic")
	time.Sleep(requestDrainTime)

	// TODO: Change this to GracefulStop when integration tests are refactored.
	s.server.Stop()
	s.Resource.Stop()
	s.params.Logger.Info("frontend stopped")
}
