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
	"math/rand"
	"net"
	"os"
	"time"

	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/util"
)

// Config represents configuration for frontend service
type Config struct {
	NumHistoryShards                      int32
	PersistenceMaxQPS                     dynamicconfig.IntPropertyFn
	PersistenceGlobalMaxQPS               dynamicconfig.IntPropertyFn
	PersistenceNamespaceMaxQPS            dynamicconfig.IntPropertyFnWithNamespaceFilter
	PersistencePerShardNamespaceMaxQPS    dynamicconfig.IntPropertyFnWithNamespaceFilter
	EnablePersistencePriorityRateLimiting dynamicconfig.BoolPropertyFn
	PersistenceDynamicRateLimitingParams  dynamicconfig.MapPropertyFn

	VisibilityPersistenceMaxReadQPS   dynamicconfig.IntPropertyFn
	VisibilityPersistenceMaxWriteQPS  dynamicconfig.IntPropertyFn
	VisibilityMaxPageSize             dynamicconfig.IntPropertyFnWithNamespaceFilter
	EnableReadFromSecondaryVisibility dynamicconfig.BoolPropertyFnWithNamespaceFilter
	VisibilityDisableOrderByClause    dynamicconfig.BoolPropertyFnWithNamespaceFilter
	VisibilityEnableManualPagination  dynamicconfig.BoolPropertyFnWithNamespaceFilter

	HistoryMaxPageSize                                           dynamicconfig.IntPropertyFnWithNamespaceFilter
	RPS                                                          dynamicconfig.IntPropertyFn
	GlobalRPS                                                    dynamicconfig.IntPropertyFn
	OperatorRPSRatio                                             dynamicconfig.FloatPropertyFn
	NamespaceReplicationInducingAPIsRPS                          dynamicconfig.IntPropertyFn
	MaxNamespaceRPSPerInstance                                   dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxNamespaceBurstPerInstance                                 dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxNamespaceCountPerInstance                                 dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxNamespaceVisibilityRPSPerInstance                         dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxNamespaceVisibilityBurstPerInstance                       dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxNamespaceNamespaceReplicationInducingAPIsRPSPerInstance   dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxNamespaceNamespaceReplicationInducingAPIsBurstPerInstance dynamicconfig.IntPropertyFnWithNamespaceFilter
	GlobalNamespaceRPS                                           dynamicconfig.IntPropertyFnWithNamespaceFilter
	InternalFEGlobalNamespaceRPS                                 dynamicconfig.IntPropertyFnWithNamespaceFilter
	GlobalNamespaceVisibilityRPS                                 dynamicconfig.IntPropertyFnWithNamespaceFilter
	InternalFEGlobalNamespaceVisibilityRPS                       dynamicconfig.IntPropertyFnWithNamespaceFilter
	GlobalNamespaceNamespaceReplicationInducingAPIsRPS           dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxIDLengthLimit                                             dynamicconfig.IntPropertyFn
	WorkerBuildIdSizeLimit                                       dynamicconfig.IntPropertyFn
	ReachabilityTaskQueueScanLimit                               dynamicconfig.IntPropertyFn
	ReachabilityQueryBuildIdLimit                                dynamicconfig.IntPropertyFn
	ReachabilityQuerySetDurationSinceDefault                     dynamicconfig.DurationPropertyFn
	DisallowQuery                                                dynamicconfig.BoolPropertyFnWithNamespaceFilter
	ShutdownDrainDuration                                        dynamicconfig.DurationPropertyFn
	ShutdownFailHealthCheckDuration                              dynamicconfig.DurationPropertyFn

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

	// RPS per every parallel delete executions activity.
	// Total RPS is equal to DeleteNamespaceDeleteActivityRPS * DeleteNamespaceConcurrentDeleteExecutionsActivities.
	// Default value is 100.
	DeleteNamespaceDeleteActivityRPS dynamicconfig.IntPropertyFn
	// Page size to read executions from visibility for delete executions activity.
	// Default value is 1000.
	DeleteNamespacePageSize dynamicconfig.IntPropertyFn
	// Number of pages before returning ContinueAsNew from delete executions activity.
	// Default value is 256.
	DeleteNamespacePagesPerExecution dynamicconfig.IntPropertyFn
	// Number of concurrent delete executions activities.
	// Must be not greater than 256 and number of worker cores in the cluster.
	// Default is 4.
	DeleteNamespaceConcurrentDeleteExecutionsActivities dynamicconfig.IntPropertyFn
	// Duration for how long namespace stays in database
	// after all namespace resources (i.e. workflow executions) are deleted.
	// Default is 0, means, namespace will be deleted immediately.
	DeleteNamespaceNamespaceDeleteDelay dynamicconfig.DurationPropertyFn

	// Enable schedule-related RPCs
	EnableSchedules dynamicconfig.BoolPropertyFnWithNamespaceFilter

	// Enable batcher RPCs
	EnableBatcher dynamicconfig.BoolPropertyFnWithNamespaceFilter
	// Batch operation dynamic configs
	MaxConcurrentBatchOperation     dynamicconfig.IntPropertyFnWithNamespaceFilter
	MaxExecutionCountBatchOperation dynamicconfig.IntPropertyFnWithNamespaceFilter

	EnableUpdateWorkflowExecution              dynamicconfig.BoolPropertyFnWithNamespaceFilter
	EnableUpdateWorkflowExecutionAsyncAccepted dynamicconfig.BoolPropertyFnWithNamespaceFilter

	EnableWorkerVersioningData     dynamicconfig.BoolPropertyFnWithNamespaceFilter
	EnableWorkerVersioningWorkflow dynamicconfig.BoolPropertyFnWithNamespaceFilter
}

// NewConfig returns new service config with default values
func NewConfig(
	dc *dynamicconfig.Collection,
	numHistoryShards int32,
	visibilityStoreConfigExist bool,
	enableReadFromES bool,
) *Config {
	return &Config{
		NumHistoryShards:                      numHistoryShards,
		PersistenceMaxQPS:                     dc.GetIntProperty(dynamicconfig.FrontendPersistenceMaxQPS, 2000),
		PersistenceGlobalMaxQPS:               dc.GetIntProperty(dynamicconfig.FrontendPersistenceGlobalMaxQPS, 0),
		PersistenceNamespaceMaxQPS:            dc.GetIntPropertyFilteredByNamespace(dynamicconfig.FrontendPersistenceNamespaceMaxQPS, 0),
		PersistencePerShardNamespaceMaxQPS:    dynamicconfig.DefaultPerShardNamespaceRPSMax,
		EnablePersistencePriorityRateLimiting: dc.GetBoolProperty(dynamicconfig.FrontendEnablePersistencePriorityRateLimiting, true),
		PersistenceDynamicRateLimitingParams:  dc.GetMapProperty(dynamicconfig.FrontendPersistenceDynamicRateLimitingParams, dynamicconfig.DefaultDynamicRateLimitingParams),

		VisibilityPersistenceMaxReadQPS:   visibility.GetVisibilityPersistenceMaxReadQPS(dc, enableReadFromES),
		VisibilityPersistenceMaxWriteQPS:  visibility.GetVisibilityPersistenceMaxWriteQPS(dc, enableReadFromES),
		VisibilityMaxPageSize:             dc.GetIntPropertyFilteredByNamespace(dynamicconfig.FrontendVisibilityMaxPageSize, 1000),
		EnableReadFromSecondaryVisibility: visibility.GetEnableReadFromSecondaryVisibilityConfig(dc, visibilityStoreConfigExist, enableReadFromES),
		VisibilityDisableOrderByClause:    dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.VisibilityDisableOrderByClause, true),
		VisibilityEnableManualPagination:  dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.VisibilityEnableManualPagination, true),

		HistoryMaxPageSize:                  dc.GetIntPropertyFilteredByNamespace(dynamicconfig.FrontendHistoryMaxPageSize, common.GetHistoryMaxPageSize),
		RPS:                                 dc.GetIntProperty(dynamicconfig.FrontendRPS, 2400),
		GlobalRPS:                           dc.GetIntProperty(dynamicconfig.FrontendGlobalRPS, 0),
		OperatorRPSRatio:                    dc.GetFloat64Property(dynamicconfig.OperatorRPSRatio, common.DefaultOperatorRPSRatio),
		NamespaceReplicationInducingAPIsRPS: dc.GetIntProperty(dynamicconfig.FrontendNamespaceReplicationInducingAPIsRPS, 20),

		MaxNamespaceRPSPerInstance:                                   dc.GetIntPropertyFilteredByNamespace(dynamicconfig.FrontendMaxNamespaceRPSPerInstance, 2400),
		MaxNamespaceBurstPerInstance:                                 dc.GetIntPropertyFilteredByNamespace(dynamicconfig.FrontendMaxNamespaceBurstPerInstance, 4800),
		MaxNamespaceCountPerInstance:                                 dc.GetIntPropertyFilteredByNamespace(dynamicconfig.FrontendMaxNamespaceCountPerInstance, 1200),
		MaxNamespaceVisibilityRPSPerInstance:                         dc.GetIntPropertyFilteredByNamespace(dynamicconfig.FrontendMaxNamespaceVisibilityRPSPerInstance, 10),
		MaxNamespaceVisibilityBurstPerInstance:                       dc.GetIntPropertyFilteredByNamespace(dynamicconfig.FrontendMaxNamespaceVisibilityBurstPerInstance, 10),
		MaxNamespaceNamespaceReplicationInducingAPIsRPSPerInstance:   dc.GetIntPropertyFilteredByNamespace(dynamicconfig.FrontendMaxNamespaceNamespaceReplicationInducingAPIsRPSPerInstance, 1),
		MaxNamespaceNamespaceReplicationInducingAPIsBurstPerInstance: dc.GetIntPropertyFilteredByNamespace(dynamicconfig.FrontendMaxNamespaceNamespaceReplicationInducingAPIsBurstPerInstance, 10),

		GlobalNamespaceRPS:                     dc.GetIntPropertyFilteredByNamespace(dynamicconfig.FrontendGlobalNamespaceRPS, 0),
		InternalFEGlobalNamespaceRPS:           dc.GetIntPropertyFilteredByNamespace(dynamicconfig.InternalFrontendGlobalNamespaceRPS, 0),
		GlobalNamespaceVisibilityRPS:           dc.GetIntPropertyFilteredByNamespace(dynamicconfig.FrontendGlobalNamespaceVisibilityRPS, 0),
		InternalFEGlobalNamespaceVisibilityRPS: dc.GetIntPropertyFilteredByNamespace(dynamicconfig.InternalFrontendGlobalNamespaceVisibilityRPS, 0),
		// Overshoot since these low rate limits don't work well in an uncoordinated global limiter.
		GlobalNamespaceNamespaceReplicationInducingAPIsRPS: dc.GetIntPropertyFilteredByNamespace(dynamicconfig.FrontendGlobalNamespaceNamespaceReplicationInducingAPIsRPS, 10),
		MaxIDLengthLimit:                         dc.GetIntProperty(dynamicconfig.MaxIDLengthLimit, 1000),
		WorkerBuildIdSizeLimit:                   dc.GetIntProperty(dynamicconfig.WorkerBuildIdSizeLimit, 255),
		ReachabilityTaskQueueScanLimit:           dc.GetIntProperty(dynamicconfig.ReachabilityTaskQueueScanLimit, 20),
		ReachabilityQueryBuildIdLimit:            dc.GetIntProperty(dynamicconfig.ReachabilityQueryBuildIdLimit, 5),
		ReachabilityQuerySetDurationSinceDefault: dc.GetDurationProperty(dynamicconfig.ReachabilityQuerySetDurationSinceDefault, 5*time.Minute),
		MaxBadBinaries:                           dc.GetIntPropertyFilteredByNamespace(dynamicconfig.FrontendMaxBadBinaries, namespace.MaxBadBinaries),
		DisableListVisibilityByFilter:            dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.DisableListVisibilityByFilter, false),
		BlobSizeLimitError:                       dc.GetIntPropertyFilteredByNamespace(dynamicconfig.BlobSizeLimitError, 2*1024*1024),
		BlobSizeLimitWarn:                        dc.GetIntPropertyFilteredByNamespace(dynamicconfig.BlobSizeLimitWarn, 256*1024),
		ThrottledLogRPS:                          dc.GetIntProperty(dynamicconfig.FrontendThrottledLogRPS, 20),
		ShutdownDrainDuration:                    dc.GetDurationProperty(dynamicconfig.FrontendShutdownDrainDuration, 0*time.Second),
		ShutdownFailHealthCheckDuration:          dc.GetDurationProperty(dynamicconfig.FrontendShutdownFailHealthCheckDuration, 0*time.Second),
		EnableNamespaceNotActiveAutoForwarding:   dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.EnableNamespaceNotActiveAutoForwarding, true),
		SearchAttributesNumberOfKeysLimit:        dc.GetIntPropertyFilteredByNamespace(dynamicconfig.SearchAttributesNumberOfKeysLimit, 100),
		SearchAttributesSizeOfValueLimit:         dc.GetIntPropertyFilteredByNamespace(dynamicconfig.SearchAttributesSizeOfValueLimit, 2*1024),
		SearchAttributesTotalSizeLimit:           dc.GetIntPropertyFilteredByNamespace(dynamicconfig.SearchAttributesTotalSizeLimit, 40*1024),
		VisibilityArchivalQueryMaxPageSize:       dc.GetIntProperty(dynamicconfig.VisibilityArchivalQueryMaxPageSize, 10000),
		DisallowQuery:                            dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.DisallowQuery, false),
		SendRawWorkflowHistory:                   dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.SendRawWorkflowHistory, false),
		DefaultWorkflowRetryPolicy:               dc.GetMapPropertyFnWithNamespaceFilter(dynamicconfig.DefaultWorkflowRetryPolicy, common.GetDefaultRetryPolicyConfigOptions()),
		DefaultWorkflowTaskTimeout:               dc.GetDurationPropertyFilteredByNamespace(dynamicconfig.DefaultWorkflowTaskTimeout, common.DefaultWorkflowTaskTimeout),
		EnableServerVersionCheck:                 dc.GetBoolProperty(dynamicconfig.EnableServerVersionCheck, os.Getenv("TEMPORAL_VERSION_CHECK_DISABLED") == ""),
		EnableTokenNamespaceEnforcement:          dc.GetBoolProperty(dynamicconfig.EnableTokenNamespaceEnforcement, true),
		KeepAliveMinTime:                         dc.GetDurationProperty(dynamicconfig.KeepAliveMinTime, 10*time.Second),
		KeepAlivePermitWithoutStream:             dc.GetBoolProperty(dynamicconfig.KeepAlivePermitWithoutStream, true),
		KeepAliveMaxConnectionIdle:               dc.GetDurationProperty(dynamicconfig.KeepAliveMaxConnectionIdle, 2*time.Minute),
		KeepAliveMaxConnectionAge:                dc.GetDurationProperty(dynamicconfig.KeepAliveMaxConnectionAge, 5*time.Minute),
		KeepAliveMaxConnectionAgeGrace:           dc.GetDurationProperty(dynamicconfig.KeepAliveMaxConnectionAgeGrace, 70*time.Second),
		KeepAliveTime:                            dc.GetDurationProperty(dynamicconfig.KeepAliveTime, 1*time.Minute),
		KeepAliveTimeout:                         dc.GetDurationProperty(dynamicconfig.KeepAliveTimeout, 10*time.Second),

		DeleteNamespaceDeleteActivityRPS:                    dc.GetIntProperty(dynamicconfig.DeleteNamespaceDeleteActivityRPS, 100),
		DeleteNamespacePageSize:                             dc.GetIntProperty(dynamicconfig.DeleteNamespacePageSize, 1000),
		DeleteNamespacePagesPerExecution:                    dc.GetIntProperty(dynamicconfig.DeleteNamespacePagesPerExecution, 256),
		DeleteNamespaceConcurrentDeleteExecutionsActivities: dc.GetIntProperty(dynamicconfig.DeleteNamespaceConcurrentDeleteExecutionsActivities, 4),
		DeleteNamespaceNamespaceDeleteDelay:                 dc.GetDurationProperty(dynamicconfig.DeleteNamespaceNamespaceDeleteDelay, 0*time.Hour),

		EnableSchedules: dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.FrontendEnableSchedules, true),

		EnableBatcher:                   dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.FrontendEnableBatcher, true),
		MaxConcurrentBatchOperation:     dc.GetIntPropertyFilteredByNamespace(dynamicconfig.FrontendMaxConcurrentBatchOperationPerNamespace, 1),
		MaxExecutionCountBatchOperation: dc.GetIntPropertyFilteredByNamespace(dynamicconfig.FrontendMaxExecutionCountBatchOperationPerNamespace, 1000),

		EnableUpdateWorkflowExecution:              dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.FrontendEnableUpdateWorkflowExecution, false),
		EnableUpdateWorkflowExecutionAsyncAccepted: dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.FrontendEnableUpdateWorkflowExecutionAsyncAccepted, false),

		EnableWorkerVersioningData:     dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.FrontendEnableWorkerVersioningDataAPIs, false),
		EnableWorkerVersioningWorkflow: dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.FrontendEnableWorkerVersioningWorkflowAPIs, false),
	}
}

// Service represents the frontend service
type Service struct {
	config *Config

	healthServer      *health.Server
	handler           Handler
	adminHandler      *AdminHandler
	operatorHandler   *OperatorHandlerImpl
	versionChecker    *VersionChecker
	visibilityManager manager.VisibilityManager
	server            *grpc.Server

	logger                         log.Logger
	grpcListener                   net.Listener
	metricsHandler                 metrics.Handler
	faultInjectionDataStoreFactory *client.FaultInjectionDataStoreFactory
	membershipMonitor              membership.Monitor
}

func NewService(
	serviceConfig *Config,
	server *grpc.Server,
	healthServer *health.Server,
	handler Handler,
	adminHandler *AdminHandler,
	operatorHandler *OperatorHandlerImpl,
	versionChecker *VersionChecker,
	visibilityMgr manager.VisibilityManager,
	logger log.Logger,
	grpcListener net.Listener,
	metricsHandler metrics.Handler,
	faultInjectionDataStoreFactory *client.FaultInjectionDataStoreFactory,
	membershipMonitor membership.Monitor,
) *Service {
	return &Service{
		config:                         serviceConfig,
		server:                         server,
		healthServer:                   healthServer,
		handler:                        handler,
		adminHandler:                   adminHandler,
		operatorHandler:                operatorHandler,
		versionChecker:                 versionChecker,
		visibilityManager:              visibilityMgr,
		logger:                         logger,
		grpcListener:                   grpcListener,
		metricsHandler:                 metricsHandler,
		faultInjectionDataStoreFactory: faultInjectionDataStoreFactory,
		membershipMonitor:              membershipMonitor,
	}
}

// Start starts the service
func (s *Service) Start() {
	s.logger.Info("frontend starting")

	healthpb.RegisterHealthServer(s.server, s.healthServer)
	workflowservice.RegisterWorkflowServiceServer(s.server, s.handler)
	adminservice.RegisterAdminServiceServer(s.server, s.adminHandler)
	operatorservice.RegisterOperatorServiceServer(s.server, s.operatorHandler)

	reflection.Register(s.server)

	// must start resource first
	s.metricsHandler.Counter(metrics.RestartCount).Record(1)
	rand.Seed(time.Now().UnixNano())

	s.versionChecker.Start()
	s.adminHandler.Start()
	s.operatorHandler.Start()
	s.handler.Start()

	go func() {
		s.logger.Info("Starting to serve on frontend listener")
		if err := s.server.Serve(s.grpcListener); err != nil {
			s.logger.Fatal("Failed to serve on frontend listener", tag.Error(err))
		}
	}()

	go s.membershipMonitor.Start()
}

// Stop stops the service
func (s *Service) Stop() {
	// initiate graceful shutdown:
	// 1. Fail rpc health check, this will cause client side load balancer to stop forwarding requests to this node
	// 2. wait for failure detection time
	// 3. stop taking new requests by returning InternalServiceError
	// 4. Wait for X second
	// 5. Stop everything forcefully and return

	requestDrainTime := util.Max(time.Second, s.config.ShutdownDrainDuration())
	failureDetectionTime := util.Max(0, s.config.ShutdownFailHealthCheckDuration())

	s.logger.Info("ShutdownHandler: Updating gRPC health status to ShuttingDown")
	s.healthServer.Shutdown()

	s.logger.Info("ShutdownHandler: Waiting for others to discover I am unhealthy")
	time.Sleep(failureDetectionTime)

	s.handler.Stop()
	s.operatorHandler.Stop()
	s.adminHandler.Stop()
	s.versionChecker.Stop()
	s.visibilityManager.Close()

	s.logger.Info("ShutdownHandler: Draining traffic")
	t := time.AfterFunc(requestDrainTime, func() {
		s.logger.Info("ShutdownHandler: Drain time expired, stopping all traffic")
		s.server.Stop()
	})
	s.server.GracefulStop()
	t.Stop()

	if s.metricsHandler != nil {
		s.metricsHandler.Stop(s.logger)
	}

	s.logger.Info("frontend stopped")
}

func (s *Service) GetFaultInjection() *client.FaultInjectionDataStoreFactory {
	return s.faultInjectionDataStoreFactory
}
