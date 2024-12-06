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

package worker

import (
	"context"

	"go.temporal.io/api/serviceerror"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/service/worker/parentclosepolicy"
	"go.temporal.io/server/service/worker/replicator"
	"go.temporal.io/server/service/worker/scanner"
)

type (
	// Service represents the temporal-worker service. This service hosts all background processing needed for temporal cluster:
	// Replicator: Handles applying replication tasks generated by remote clusters.
	// Archiver: Handles archival of workflow histories.
	Service struct {
		logger                 log.Logger
		clusterMetadata        cluster.Metadata
		clientBean             client.Bean
		clusterMetadataManager persistence.ClusterMetadataManager
		metadataManager        persistence.MetadataManager
		membershipMonitor      membership.Monitor
		hostInfo               membership.HostInfo
		executionManager       persistence.ExecutionManager
		taskManager            persistence.TaskManager
		historyClient          resource.HistoryClient
		namespaceRegistry      namespace.Registry
		workerServiceResolver  membership.ServiceResolver
		visibilityManager      manager.VisibilityManager

		namespaceReplicationQueue persistence.NamespaceReplicationQueue

		metricsHandler metrics.Handler

		sdkClientFactory sdk.ClientFactory
		esClient         esclient.Client
		config           *Config

		workerManager                    *workerManager
		perNamespaceWorkerManager        *perNamespaceWorkerManager
		scanner                          *scanner.Scanner
		matchingClient                   matchingservice.MatchingServiceClient
		namespaceReplicationTaskExecutor namespace.ReplicationTaskExecutor
	}

	// Config contains all the service config for worker
	Config struct {
		ScannerCfg                           *scanner.Config
		ParentCloseCfg                       *parentclosepolicy.Config
		ThrottledLogRPS                      dynamicconfig.IntPropertyFn
		PersistenceMaxQPS                    dynamicconfig.IntPropertyFn
		PersistenceGlobalMaxQPS              dynamicconfig.IntPropertyFn
		PersistenceNamespaceMaxQPS           dynamicconfig.IntPropertyFnWithNamespaceFilter
		PersistenceGlobalNamespaceMaxQPS     dynamicconfig.IntPropertyFnWithNamespaceFilter
		PersistencePerShardNamespaceMaxQPS   dynamicconfig.IntPropertyFnWithNamespaceFilter
		PersistenceDynamicRateLimitingParams dynamicconfig.TypedPropertyFn[dynamicconfig.DynamicRateLimitingParams]
		PersistenceQPSBurstRatio             dynamicconfig.FloatPropertyFn
		OperatorRPSRatio                     dynamicconfig.FloatPropertyFn
		EnableBatcher                        dynamicconfig.BoolPropertyFn
		BatcherRPS                           dynamicconfig.IntPropertyFnWithNamespaceFilter
		BatcherConcurrency                   dynamicconfig.IntPropertyFnWithNamespaceFilter
		EnableParentClosePolicyWorker        dynamicconfig.BoolPropertyFn
		PerNamespaceWorkerCount              dynamicconfig.TypedSubscribableWithNamespaceFilter[int]
		PerNamespaceWorkerOptions            dynamicconfig.TypedSubscribableWithNamespaceFilter[sdkworker.Options]
		PerNamespaceWorkerStartRate          dynamicconfig.FloatPropertyFn

		VisibilityPersistenceMaxReadQPS         dynamicconfig.IntPropertyFn
		VisibilityPersistenceMaxWriteQPS        dynamicconfig.IntPropertyFn
		VisibilityPersistenceSlowQueryThreshold dynamicconfig.DurationPropertyFn
		EnableReadFromSecondaryVisibility       dynamicconfig.BoolPropertyFnWithNamespaceFilter
		VisibilityEnableShadowReadMode          dynamicconfig.BoolPropertyFn
		VisibilityDisableOrderByClause          dynamicconfig.BoolPropertyFnWithNamespaceFilter
		VisibilityEnableManualPagination        dynamicconfig.BoolPropertyFnWithNamespaceFilter
	}
)

func NewService(
	logger log.SnTaggedLogger,
	serviceConfig *Config,
	sdkClientFactory sdk.ClientFactory,
	esClient esclient.Client,
	clusterMetadata cluster.Metadata,
	clientBean client.Bean,
	clusterMetadataManager persistence.ClusterMetadataManager,
	namespaceRegistry namespace.Registry,
	executionManager persistence.ExecutionManager,
	membershipMonitor membership.Monitor,
	hostInfoProvider membership.HostInfoProvider,
	namespaceReplicationQueue persistence.NamespaceReplicationQueue,
	metricsHandler metrics.Handler,
	metadataManager persistence.MetadataManager,
	taskManager persistence.TaskManager,
	historyClient resource.HistoryClient,
	workerManager *workerManager,
	perNamespaceWorkerManager *perNamespaceWorkerManager,
	visibilityManager manager.VisibilityManager,
	matchingClient resource.MatchingClient,
	namespaceReplicationTaskExecutor namespace.ReplicationTaskExecutor,
) (*Service, error) {
	workerServiceResolver, err := membershipMonitor.GetResolver(primitives.WorkerService)
	if err != nil {
		return nil, err
	}

	s := &Service{
		config:                    serviceConfig,
		sdkClientFactory:          sdkClientFactory,
		esClient:                  esClient,
		logger:                    logger,
		clusterMetadata:           clusterMetadata,
		clientBean:                clientBean,
		clusterMetadataManager:    clusterMetadataManager,
		namespaceRegistry:         namespaceRegistry,
		executionManager:          executionManager,
		workerServiceResolver:     workerServiceResolver,
		membershipMonitor:         membershipMonitor,
		hostInfo:                  hostInfoProvider.HostInfo(),
		namespaceReplicationQueue: namespaceReplicationQueue,
		metricsHandler:            metricsHandler,
		metadataManager:           metadataManager,
		taskManager:               taskManager,
		historyClient:             historyClient,
		visibilityManager:         visibilityManager,

		workerManager:                    workerManager,
		perNamespaceWorkerManager:        perNamespaceWorkerManager,
		matchingClient:                   matchingClient,
		namespaceReplicationTaskExecutor: namespaceReplicationTaskExecutor,
	}
	if err := s.initScanner(); err != nil {
		return nil, err
	}
	return s, nil
}

// NewConfig builds the new Config for worker service
func NewConfig(
	dc *dynamicconfig.Collection,
	persistenceConfig *config.Persistence,
) *Config {
	config := &Config{
		ParentCloseCfg: &parentclosepolicy.Config{
			MaxConcurrentActivityExecutionSize:     dynamicconfig.WorkerParentCloseMaxConcurrentActivityExecutionSize.Get(dc),
			MaxConcurrentWorkflowTaskExecutionSize: dynamicconfig.WorkerParentCloseMaxConcurrentWorkflowTaskExecutionSize.Get(dc),
			MaxConcurrentActivityTaskPollers:       dynamicconfig.WorkerParentCloseMaxConcurrentActivityTaskPollers.Get(dc),
			MaxConcurrentWorkflowTaskPollers:       dynamicconfig.WorkerParentCloseMaxConcurrentWorkflowTaskPollers.Get(dc),
			NumParentClosePolicySystemWorkflows:    dynamicconfig.NumParentClosePolicySystemWorkflows.Get(dc),
		},
		ScannerCfg: &scanner.Config{
			MaxConcurrentActivityExecutionSize:     dynamicconfig.WorkerScannerMaxConcurrentActivityExecutionSize.Get(dc),
			MaxConcurrentWorkflowTaskExecutionSize: dynamicconfig.WorkerScannerMaxConcurrentWorkflowTaskExecutionSize.Get(dc),
			MaxConcurrentActivityTaskPollers:       dynamicconfig.WorkerScannerMaxConcurrentActivityTaskPollers.Get(dc),
			MaxConcurrentWorkflowTaskPollers:       dynamicconfig.WorkerScannerMaxConcurrentWorkflowTaskPollers.Get(dc),

			PersistenceMaxQPS:                       dynamicconfig.ScannerPersistenceMaxQPS.Get(dc),
			Persistence:                             persistenceConfig,
			TaskQueueScannerEnabled:                 dynamicconfig.TaskQueueScannerEnabled.Get(dc),
			BuildIdScavengerEnabled:                 dynamicconfig.BuildIdScavengerEnabled.Get(dc),
			HistoryScannerEnabled:                   dynamicconfig.HistoryScannerEnabled.Get(dc),
			ExecutionsScannerEnabled:                dynamicconfig.ExecutionsScannerEnabled.Get(dc),
			HistoryScannerDataMinAge:                dynamicconfig.HistoryScannerDataMinAge.Get(dc),
			HistoryScannerVerifyRetention:           dynamicconfig.HistoryScannerVerifyRetention.Get(dc),
			ExecutionScannerPerHostQPS:              dynamicconfig.ExecutionScannerPerHostQPS.Get(dc),
			ExecutionScannerPerShardQPS:             dynamicconfig.ExecutionScannerPerShardQPS.Get(dc),
			ExecutionDataDurationBuffer:             dynamicconfig.ExecutionDataDurationBuffer.Get(dc),
			ExecutionScannerWorkerCount:             dynamicconfig.ExecutionScannerWorkerCount.Get(dc),
			ExecutionScannerHistoryEventIdValidator: dynamicconfig.ExecutionScannerHistoryEventIdValidator.Get(dc),
			RemovableBuildIdDurationSinceDefault:    dynamicconfig.RemovableBuildIdDurationSinceDefault.Get(dc),
			BuildIdScavengerVisibilityRPS:           dynamicconfig.BuildIdScavengerVisibilityRPS.Get(dc),
		},
		BatcherRPS:                           dynamicconfig.BatcherRPS.Get(dc),
		BatcherConcurrency:                   dynamicconfig.BatcherConcurrency.Get(dc),
		EnableParentClosePolicyWorker:        dynamicconfig.EnableParentClosePolicyWorker.Get(dc),
		PerNamespaceWorkerCount:              dynamicconfig.WorkerPerNamespaceWorkerCount.Subscribe(dc),
		PerNamespaceWorkerOptions:            dynamicconfig.WorkerPerNamespaceWorkerOptions.Subscribe(dc),
		PerNamespaceWorkerStartRate:          dynamicconfig.WorkerPerNamespaceWorkerStartRate.Get(dc),
		ThrottledLogRPS:                      dynamicconfig.WorkerThrottledLogRPS.Get(dc),
		PersistenceMaxQPS:                    dynamicconfig.WorkerPersistenceMaxQPS.Get(dc),
		PersistenceGlobalMaxQPS:              dynamicconfig.WorkerPersistenceGlobalMaxQPS.Get(dc),
		PersistenceNamespaceMaxQPS:           dynamicconfig.WorkerPersistenceNamespaceMaxQPS.Get(dc),
		PersistenceGlobalNamespaceMaxQPS:     dynamicconfig.WorkerPersistenceGlobalNamespaceMaxQPS.Get(dc),
		PersistencePerShardNamespaceMaxQPS:   dynamicconfig.DefaultPerShardNamespaceRPSMax,
		PersistenceDynamicRateLimitingParams: dynamicconfig.WorkerPersistenceDynamicRateLimitingParams.Get(dc),
		PersistenceQPSBurstRatio:             dynamicconfig.PersistenceQPSBurstRatio.Get(dc),
		OperatorRPSRatio:                     dynamicconfig.OperatorRPSRatio.Get(dc),

		VisibilityPersistenceMaxReadQPS:   dynamicconfig.VisibilityPersistenceMaxReadQPS.Get(dc),
		VisibilityPersistenceMaxWriteQPS:  dynamicconfig.VisibilityPersistenceMaxWriteQPS.Get(dc),
		EnableReadFromSecondaryVisibility: dynamicconfig.EnableReadFromSecondaryVisibility.Get(dc),
		VisibilityEnableShadowReadMode:    dynamicconfig.VisibilityEnableShadowReadMode.Get(dc),
		VisibilityDisableOrderByClause:    dynamicconfig.VisibilityDisableOrderByClause.Get(dc),
		VisibilityEnableManualPagination:  dynamicconfig.VisibilityEnableManualPagination.Get(dc),
	}
	return config
}

// Start is called to start the service
func (s *Service) Start() {
	s.logger.Info(
		"worker starting",
		tag.ComponentWorker,
	)

	metrics.RestartCount.With(s.metricsHandler).Record(1)

	s.clusterMetadata.Start()
	s.namespaceRegistry.Start()

	s.membershipMonitor.Start()

	s.ensureSystemNamespaceExists(context.TODO())
	s.startScanner()

	if s.clusterMetadata.IsGlobalNamespaceEnabled() {
		s.startReplicator()
	}
	if s.config.EnableParentClosePolicyWorker() {
		s.startParentClosePolicyProcessor()
	}

	s.workerManager.Start()
	s.perNamespaceWorkerManager.Start(
		// TODO: get these from fx instead of passing through Start
		s.hostInfo,
		s.workerServiceResolver,
	)

	s.logger.Info(
		"worker service started",
		tag.ComponentWorker,
		tag.Address(s.hostInfo.GetAddress()),
	)
}

// Stop is called to stop the service
func (s *Service) Stop() {
	s.scanner.Stop()
	s.perNamespaceWorkerManager.Stop()
	s.workerManager.Stop()
	s.namespaceRegistry.Stop()
	s.clusterMetadata.Stop()
	s.visibilityManager.Close()

	s.logger.Info(
		"worker service stopped",
		tag.ComponentWorker,
		tag.Address(s.hostInfo.GetAddress()),
	)
}

func (s *Service) startParentClosePolicyProcessor() {
	params := &parentclosepolicy.BootstrapParams{
		Config:           *s.config.ParentCloseCfg,
		SdkClientFactory: s.sdkClientFactory,
		MetricsHandler:   s.metricsHandler,
		Logger:           s.logger,
		ClientBean:       s.clientBean,
		CurrentCluster:   s.clusterMetadata.GetCurrentClusterName(),
		HostInfo:         s.hostInfo,
	}
	processor := parentclosepolicy.New(params)
	if err := processor.Start(); err != nil {
		s.logger.Fatal(
			"error starting parentclosepolicy processor",
			tag.Error(err),
		)
	}
}

func (s *Service) initScanner() error {
	currentCluster := s.clusterMetadata.GetCurrentClusterName()
	adminClient, err := s.clientBean.GetRemoteAdminClient(currentCluster)
	if err != nil {
		return err
	}
	s.scanner = scanner.New(
		s.logger,
		s.config.ScannerCfg,
		s.sdkClientFactory,
		s.metricsHandler,
		s.executionManager,
		s.metadataManager,
		s.visibilityManager,
		s.taskManager,
		s.historyClient,
		adminClient,
		s.matchingClient,
		s.namespaceRegistry,
		currentCluster,
		s.hostInfo,
	)
	return nil
}

func (s *Service) startScanner() {
	if err := s.scanner.Start(); err != nil {
		s.logger.Fatal(
			"error starting scanner",
			tag.Error(err),
		)
	}
}

func (s *Service) startReplicator() {
	msgReplicator := replicator.NewReplicator(
		s.clusterMetadata,
		s.clientBean,
		s.logger,
		s.metricsHandler,
		s.hostInfo,
		s.workerServiceResolver,
		s.namespaceReplicationQueue,
		s.namespaceReplicationTaskExecutor,
		s.matchingClient,
		s.namespaceRegistry,
	)
	msgReplicator.Start()
}

func (s *Service) ensureSystemNamespaceExists(
	ctx context.Context,
) {
	_, err := s.metadataManager.GetNamespace(ctx, &persistence.GetNamespaceRequest{Name: primitives.SystemLocalNamespace})
	switch err.(type) {
	case nil:
		// noop
	case *serviceerror.NamespaceNotFound:
		s.logger.Fatal(
			"temporal-system namespace does not exist",
			tag.Error(err),
		)
	default:
		s.logger.Fatal(
			"failed to verify if temporal system namespace exists",
			tag.Error(err),
		)
	}
}
