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
	"context"
	"os"
	"sync/atomic"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/messaging"
	"go.temporal.io/server/common/mocks"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	espersistence "go.temporal.io/server/common/persistence/elasticsearch"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/service/config"
	"go.temporal.io/server/common/service/dynamicconfig"
)

// Config represents configuration for frontend service
type Config struct {
	NumHistoryShards           int32
	PersistenceMaxQPS          dynamicconfig.IntPropertyFn
	PersistenceGlobalMaxQPS    dynamicconfig.IntPropertyFn
	VisibilityMaxPageSize      dynamicconfig.IntPropertyFnWithNamespaceFilter
	EnableVisibilitySampling   dynamicconfig.BoolPropertyFn
	VisibilityListMaxQPS       dynamicconfig.IntPropertyFnWithNamespaceFilter
	EnableReadVisibilityFromES dynamicconfig.BoolPropertyFnWithNamespaceFilter
	ESVisibilityListMaxQPS     dynamicconfig.IntPropertyFnWithNamespaceFilter
	ESIndexMaxResultWindow     dynamicconfig.IntPropertyFn
	HistoryMaxPageSize         dynamicconfig.IntPropertyFnWithNamespaceFilter
	RPS                        dynamicconfig.IntPropertyFn
	MaxNamespaceRPSPerInstance dynamicconfig.IntPropertyFnWithNamespaceFilter
	GlobalNamespaceRPS         dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxIDLengthLimit           dynamicconfig.IntPropertyFn
	EnableClientVersionCheck   dynamicconfig.BoolPropertyFn
	MinRetentionDays           dynamicconfig.IntPropertyFn
	DisallowQuery              dynamicconfig.BoolPropertyFnWithNamespaceFilter
	ShutdownDrainDuration      dynamicconfig.DurationPropertyFn

	// Persistence settings
	HistoryMgrNumConns dynamicconfig.IntPropertyFn

	MaxBadBinaries dynamicconfig.IntPropertyFnWithNamespaceFilter

	// security protection settings
	EnableAdminProtection         dynamicconfig.BoolPropertyFn
	AdminOperationToken           dynamicconfig.StringPropertyFn
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

	EnableRPCReplication         dynamicconfig.BoolPropertyFn
	EnableCleanupReplicationTask dynamicconfig.BoolPropertyFn

	// The execution timeout a workflow execution defaults to if not specified
	DefaultWorkflowExecutionTimeout dynamicconfig.DurationPropertyFnWithNamespaceFilter
	// The run timeout a workflow run defaults to if not specified
	DefaultWorkflowRunTimeout dynamicconfig.DurationPropertyFnWithNamespaceFilter

	// The execution timeout a workflow execution defaults to if not specified
	MaxWorkflowExecutionTimeout dynamicconfig.DurationPropertyFnWithNamespaceFilter
	// The run timeout a workflow run defaults to if not specified
	MaxWorkflowRunTimeout dynamicconfig.DurationPropertyFnWithNamespaceFilter

	// DefaultWorkflowTaskTimeout the default workflow task timeout
	DefaultWorkflowTaskTimeout dynamicconfig.DurationPropertyFnWithNamespaceFilter

	// EnableServerVersionCheck disables periodic version checking performed by the frontend
	EnableServerVersionCheck dynamicconfig.BoolPropertyFn
}

// NewConfig returns new service config with default values
func NewConfig(dc *dynamicconfig.Collection, numHistoryShards int32, enableReadFromES bool) *Config {
	return &Config{
		NumHistoryShards:                       numHistoryShards,
		PersistenceMaxQPS:                      dc.GetIntProperty(dynamicconfig.FrontendPersistenceMaxQPS, 2000),
		PersistenceGlobalMaxQPS:                dc.GetIntProperty(dynamicconfig.FrontendPersistenceGlobalMaxQPS, 0),
		VisibilityMaxPageSize:                  dc.GetIntPropertyFilteredByNamespace(dynamicconfig.FrontendVisibilityMaxPageSize, 1000),
		EnableVisibilitySampling:               dc.GetBoolProperty(dynamicconfig.EnableVisibilitySampling, true),
		VisibilityListMaxQPS:                   dc.GetIntPropertyFilteredByNamespace(dynamicconfig.FrontendVisibilityListMaxQPS, 1),
		EnableReadVisibilityFromES:             dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.EnableReadVisibilityFromES, enableReadFromES),
		ESVisibilityListMaxQPS:                 dc.GetIntPropertyFilteredByNamespace(dynamicconfig.FrontendESVisibilityListMaxQPS, 3),
		ESIndexMaxResultWindow:                 dc.GetIntProperty(dynamicconfig.FrontendESIndexMaxResultWindow, 10000),
		HistoryMaxPageSize:                     dc.GetIntPropertyFilteredByNamespace(dynamicconfig.FrontendHistoryMaxPageSize, common.GetHistoryMaxPageSize),
		RPS:                                    dc.GetIntProperty(dynamicconfig.FrontendRPS, 1200),
		MaxNamespaceRPSPerInstance:             dc.GetIntPropertyFilteredByNamespace(dynamicconfig.FrontendMaxNamespaceRPSPerInstance, 1200),
		GlobalNamespaceRPS:                     dc.GetIntPropertyFilteredByNamespace(dynamicconfig.FrontendGlobalNamespaceRPS, 0),
		MaxIDLengthLimit:                       dc.GetIntProperty(dynamicconfig.MaxIDLengthLimit, 1000),
		HistoryMgrNumConns:                     dc.GetIntProperty(dynamicconfig.FrontendHistoryMgrNumConns, 10),
		MaxBadBinaries:                         dc.GetIntPropertyFilteredByNamespace(dynamicconfig.FrontendMaxBadBinaries, namespace.MaxBadBinaries),
		EnableAdminProtection:                  dc.GetBoolProperty(dynamicconfig.EnableAdminProtection, false),
		AdminOperationToken:                    dc.GetStringProperty(dynamicconfig.AdminOperationToken, common.DefaultAdminOperationToken),
		DisableListVisibilityByFilter:          dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.DisableListVisibilityByFilter, false),
		BlobSizeLimitError:                     dc.GetIntPropertyFilteredByNamespace(dynamicconfig.BlobSizeLimitError, 2*1024*1024),
		BlobSizeLimitWarn:                      dc.GetIntPropertyFilteredByNamespace(dynamicconfig.BlobSizeLimitWarn, 256*1024),
		ThrottledLogRPS:                        dc.GetIntProperty(dynamicconfig.FrontendThrottledLogRPS, 20),
		ShutdownDrainDuration:                  dc.GetDurationProperty(dynamicconfig.FrontendShutdownDrainDuration, 0),
		EnableNamespaceNotActiveAutoForwarding: dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.EnableNamespaceNotActiveAutoForwarding, true),
		EnableClientVersionCheck:               dc.GetBoolProperty(dynamicconfig.EnableClientVersionCheck, false),
		ValidSearchAttributes:                  dc.GetMapProperty(dynamicconfig.ValidSearchAttributes, definition.GetDefaultIndexedKeys()),
		SearchAttributesNumberOfKeysLimit:      dc.GetIntPropertyFilteredByNamespace(dynamicconfig.SearchAttributesNumberOfKeysLimit, 100),
		SearchAttributesSizeOfValueLimit:       dc.GetIntPropertyFilteredByNamespace(dynamicconfig.SearchAttributesSizeOfValueLimit, 2*1024),
		SearchAttributesTotalSizeLimit:         dc.GetIntPropertyFilteredByNamespace(dynamicconfig.SearchAttributesTotalSizeLimit, 40*1024),
		MinRetentionDays:                       dc.GetIntProperty(dynamicconfig.MinRetentionDays, namespace.MinRetentionDays),
		VisibilityArchivalQueryMaxPageSize:     dc.GetIntProperty(dynamicconfig.VisibilityArchivalQueryMaxPageSize, 10000),
		DisallowQuery:                          dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.DisallowQuery, false),
		SendRawWorkflowHistory:                 dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.SendRawWorkflowHistory, false),
		EnableRPCReplication:                   dc.GetBoolProperty(dynamicconfig.FrontendEnableRPCReplication, false),
		EnableCleanupReplicationTask:           dc.GetBoolProperty(dynamicconfig.FrontendEnableCleanupReplicationTask, true),
		DefaultWorkflowRetryPolicy:             dc.GetMapPropertyFnWithNamespaceFilter(dynamicconfig.DefaultWorkflowRetryPolicy, common.GetDefaultRetryPolicyConfigOptions()),
		DefaultWorkflowExecutionTimeout:        dc.GetDurationPropertyFilteredByNamespace(dynamicconfig.DefaultWorkflowExecutionTimeout, common.DefaultWorkflowExecutionTimeout),
		DefaultWorkflowRunTimeout:              dc.GetDurationPropertyFilteredByNamespace(dynamicconfig.DefaultWorkflowRunTimeout, common.DefaultWorkflowRunTimeout),
		MaxWorkflowExecutionTimeout:            dc.GetDurationPropertyFilteredByNamespace(dynamicconfig.MaxWorkflowExecutionTimeout, common.DefaultWorkflowExecutionTimeout),
		MaxWorkflowRunTimeout:                  dc.GetDurationPropertyFilteredByNamespace(dynamicconfig.MaxWorkflowRunTimeout, common.DefaultWorkflowRunTimeout),
		DefaultWorkflowTaskTimeout:             dc.GetDurationPropertyFilteredByNamespace(dynamicconfig.DefaultWorkflowTaskTimeout, common.DefaultWorkflowTaskTimeout),
		EnableServerVersionCheck:               dc.GetBoolProperty(dynamicconfig.EnableServerVersionCheck, os.Getenv("TEMPORAL_VERSION_CHECK_DISABLED") == ""),
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

	params.PersistenceConfig.HistoryMaxConns = serviceConfig.HistoryMgrNumConns()
	params.PersistenceConfig.VisibilityConfig = &config.VisibilityConfig{
		VisibilityListMaxQPS: serviceConfig.VisibilityListMaxQPS,
		EnableSampling:       serviceConfig.EnableVisibilitySampling,
	}

	visibilityManagerInitializer := func(
		persistenceBean persistenceClient.Bean,
		logger log.Logger,
	) (persistence.VisibilityManager, error) {
		visibilityFromDB := persistenceBean.GetVisibilityManager()

		var visibilityFromES persistence.VisibilityManager
		if params.ESConfig != nil {
			visibilityIndexName := params.ESConfig.Indices[common.VisibilityAppName]
			visibilityConfigForES := &config.VisibilityConfig{
				MaxQPS:                 serviceConfig.PersistenceMaxQPS,
				VisibilityListMaxQPS:   serviceConfig.ESVisibilityListMaxQPS,
				ESIndexMaxResultWindow: serviceConfig.ESIndexMaxResultWindow,
				ValidSearchAttributes:  serviceConfig.ValidSearchAttributes,
			}
			visibilityFromES = espersistence.NewESVisibilityManager(visibilityIndexName, params.ESClient, visibilityConfigForES,
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

	var replicationMessageSink messaging.Producer
	clusterMetadata := s.GetClusterMetadata()
	if clusterMetadata.IsGlobalNamespaceEnabled() {
		consumerConfig := clusterMetadata.GetReplicationConsumerConfig()
		if consumerConfig != nil &&
			consumerConfig.Type == config.ReplicationConsumerTypeRPC &&
			s.config.EnableRPCReplication() {
			replicationMessageSink = s.GetNamespaceReplicationQueue()
		} else {
			var err error
			replicationMessageSink, err = s.GetMessagingClient().NewProducerWithClusterName(
				s.GetClusterMetadata().GetCurrentClusterName())
			if err != nil {
				logger.Fatal("Creating replicationMessageSink producer failed", tag.Error(err))
			}
		}
	} else {
		replicationMessageSink = &mocks.KafkaProducer{}
		replicationMessageSink.(*mocks.KafkaProducer).On("Publish", mock.Anything).Return(nil)
	}

	opts, err := s.params.RPCFactory.GetFrontendGRPCServerOptions()
	if err != nil {
		logger.Fatal("creating grpc server options failed", tag.Error(err))
	}
	opts = append(opts, grpc.UnaryInterceptor(interceptor))
	s.server = grpc.NewServer(opts...)

	wfHandler := NewWorkflowHandler(s, s.config, replicationMessageSink)
	s.handler = NewDCRedirectionHandler(wfHandler, s.params.DCRedirectionPolicy)
	if s.params.Authorizer != nil {
		s.handler = NewAccessControlledHandlerImpl(s.handler, s.params.Authorizer)
	}
	workflowNilCheckHandler := NewWorkflowNilCheckHandler(s.handler)

	workflowservice.RegisterWorkflowServiceServer(s.server, workflowNilCheckHandler)
	healthpb.RegisterHealthServer(s.server, s.handler)

	s.adminHandler = NewAdminHandler(s, s.params, s.config)
	adminNilCheckHandler := NewAdminNilCheckHandler(s.adminHandler)

	adminservice.RegisterAdminServiceServer(s.server, adminNilCheckHandler)
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

func interceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	resp, err := handler(ctx, req)
	return resp, serviceerror.ToStatus(err).Err()
}
