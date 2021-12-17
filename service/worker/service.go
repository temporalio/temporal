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
	"math/rand"
	"sync/atomic"
	"time"

	"go.temporal.io/api/serviceerror"
	sdkclient "go.temporal.io/sdk/client"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	carchiver "go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/service/worker/archiver"
	"go.temporal.io/server/service/worker/batcher"
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
		archivalMetadata       carchiver.ArchivalMetadata
		clusterMetadata        cluster.Metadata
		metricsClient          metrics.Client
		clientBean             client.Bean
		clusterMetadataManager persistence.ClusterMetadataManager
		metadataManager        persistence.MetadataManager
		hostInfo               *membership.HostInfo
		executionManager       persistence.ExecutionManager
		taskManager            persistence.TaskManager
		historyClient          historyservice.HistoryServiceClient
		namespaceRegistry      namespace.Registry
		workerServiceResolver  membership.ServiceResolver

		archiverProvider provider.ArchiverProvider

		namespaceReplicationQueue persistence.NamespaceReplicationQueue

		persistenceBean persistenceClient.Bean

		membershipMonitor membership.Monitor

		userMetricsScope metrics.UserScope

		status           int32
		stopC            chan struct{}
		sdkClientFactory sdk.ClientFactory
		sdkSystemClient  sdkclient.Client
		esClient         esclient.Client
		config           *Config

		manager *workerManager
	}

	// Config contains all the service config for worker
	Config struct {
		ArchiverConfig                *archiver.Config
		ScannerCfg                    *scanner.Config
		ParentCloseCfg                *parentclosepolicy.Config
		BatcherCfg                    *batcher.Config
		ThrottledLogRPS               dynamicconfig.IntPropertyFn
		PersistenceMaxQPS             dynamicconfig.IntPropertyFn
		PersistenceGlobalMaxQPS       dynamicconfig.IntPropertyFn
		EnableBatcher                 dynamicconfig.BoolPropertyFn
		EnableParentClosePolicyWorker dynamicconfig.BoolPropertyFn
	}
)

func NewService(
	logger resource.SnTaggedLogger,
	serviceConfig *Config,
	sdkClientFactory sdk.ClientFactory,
	sdkSystemClient sdkclient.Client,
	esClient esclient.Client,
	archivalMetadata carchiver.ArchivalMetadata,
	clusterMetadata cluster.Metadata,
	metricsClient metrics.Client,
	clientBean client.Bean,
	clusterMetadataManager persistence.ClusterMetadataManager,
	namespaceRegistry namespace.Registry,
	executionManager persistence.ExecutionManager,
	archiverProvider provider.ArchiverProvider,
	persistenceBean persistenceClient.Bean,
	membershipMonitor membership.Monitor,
	namespaceReplicationQueue persistence.NamespaceReplicationQueue,
	metricsScope metrics.UserScope,
	metadataManager persistence.MetadataManager,
	taskManager persistence.TaskManager,
	historyClient historyservice.HistoryServiceClient,
	manager *workerManager,
) (*Service, error) {
	workerServiceResolver, err := membershipMonitor.GetResolver(common.WorkerServiceName)
	if err != nil {
		return nil, err
	}

	return &Service{
		status:                    common.DaemonStatusInitialized,
		config:                    serviceConfig,
		sdkSystemClient:           sdkSystemClient,
		sdkClientFactory:          sdkClientFactory,
		esClient:                  esClient,
		stopC:                     make(chan struct{}),
		logger:                    logger,
		archivalMetadata:          archivalMetadata,
		clusterMetadata:           clusterMetadata,
		metricsClient:             metricsClient,
		clientBean:                clientBean,
		clusterMetadataManager:    clusterMetadataManager,
		namespaceRegistry:         namespaceRegistry,
		executionManager:          executionManager,
		persistenceBean:           persistenceBean,
		workerServiceResolver:     workerServiceResolver,
		membershipMonitor:         membershipMonitor,
		archiverProvider:          archiverProvider,
		namespaceReplicationQueue: namespaceReplicationQueue,
		userMetricsScope:          metricsScope,
		metadataManager:           metadataManager,
		taskManager:               taskManager,
		historyClient:             historyClient,

		manager: manager,
	}, nil
}

// NewConfig builds the new Config for worker service
func NewConfig(dc dynamicconfig.Collection, params *resource.BootstrapParams) *Config {
	config := &Config{
		ArchiverConfig: &archiver.Config{
			MaxConcurrentActivityExecutionSize: dc.GetIntProperty(
				dynamicconfig.WorkerArchiverMaxConcurrentActivityExecutionSize,
				1000,
			),
			MaxConcurrentWorkflowTaskExecutionSize: dc.GetIntProperty(
				dynamicconfig.WorkerArchiverMaxConcurrentWorkflowTaskExecutionSize,
				1000,
			),
			MaxConcurrentActivityTaskPollers: dc.GetIntProperty(
				dynamicconfig.WorkerArchiverMaxConcurrentActivityTaskPollers,
				4,
			),
			MaxConcurrentWorkflowTaskPollers: dc.GetIntProperty(
				dynamicconfig.WorkerArchiverMaxConcurrentWorkflowTaskPollers,
				4,
			),

			ArchiverConcurrency: dc.GetIntProperty(
				dynamicconfig.WorkerArchiverConcurrency,
				50,
			),
			ArchivalsPerIteration: dc.GetIntProperty(
				dynamicconfig.WorkerArchivalsPerIteration,
				1000,
			),
			TimeLimitPerArchivalIteration: dc.GetDurationProperty(
				dynamicconfig.WorkerTimeLimitPerArchivalIteration,
				archiver.MaxArchivalIterationTimeout(),
			),
		},
		BatcherCfg: &batcher.Config{
			MaxConcurrentActivityExecutionSize: dc.GetIntProperty(
				dynamicconfig.WorkerBatcherMaxConcurrentActivityExecutionSize,
				1000,
			),
			MaxConcurrentWorkflowTaskExecutionSize: dc.GetIntProperty(
				dynamicconfig.WorkerBatcherMaxConcurrentWorkflowTaskExecutionSize,
				1000,
			),
			MaxConcurrentActivityTaskPollers: dc.GetIntProperty(
				dynamicconfig.WorkerBatcherMaxConcurrentActivityTaskPollers,
				4,
			),
			MaxConcurrentWorkflowTaskPollers: dc.GetIntProperty(
				dynamicconfig.WorkerBatcherMaxConcurrentWorkflowTaskPollers,
				4,
			),
		},
		ParentCloseCfg: &parentclosepolicy.Config{
			MaxConcurrentActivityExecutionSize: dc.GetIntProperty(
				dynamicconfig.WorkerParentCloseMaxConcurrentActivityExecutionSize,
				1000,
			),
			MaxConcurrentWorkflowTaskExecutionSize: dc.GetIntProperty(
				dynamicconfig.WorkerParentCloseMaxConcurrentWorkflowTaskExecutionSize,
				1000,
			),
			MaxConcurrentActivityTaskPollers: dc.GetIntProperty(
				dynamicconfig.WorkerParentCloseMaxConcurrentActivityTaskPollers,
				4,
			),
			MaxConcurrentWorkflowTaskPollers: dc.GetIntProperty(
				dynamicconfig.WorkerParentCloseMaxConcurrentWorkflowTaskPollers,
				4,
			),
			NumParentClosePolicySystemWorkflows: dc.GetIntProperty(
				dynamicconfig.NumParentClosePolicySystemWorkflows,
				10,
			),
		},
		ScannerCfg: &scanner.Config{
			MaxConcurrentActivityExecutionSize: dc.GetIntProperty(
				dynamicconfig.WorkerScannerMaxConcurrentActivityExecutionSize,
				10,
			),
			MaxConcurrentWorkflowTaskExecutionSize: dc.GetIntProperty(
				dynamicconfig.WorkerScannerMaxConcurrentWorkflowTaskExecutionSize,
				10,
			),
			MaxConcurrentActivityTaskPollers: dc.GetIntProperty(
				dynamicconfig.WorkerScannerMaxConcurrentActivityTaskPollers,
				8,
			),
			MaxConcurrentWorkflowTaskPollers: dc.GetIntProperty(
				dynamicconfig.WorkerScannerMaxConcurrentWorkflowTaskPollers,
				8,
			),

			PersistenceMaxQPS: dc.GetIntProperty(
				dynamicconfig.ScannerPersistenceMaxQPS,
				100,
			),
			Persistence: &params.PersistenceConfig,
			TaskQueueScannerEnabled: dc.GetBoolProperty(
				dynamicconfig.TaskQueueScannerEnabled,
				true,
			),
			HistoryScannerEnabled: dc.GetBoolProperty(
				dynamicconfig.HistoryScannerEnabled,
				true,
			),
			ExecutionsScannerEnabled: dc.GetBoolProperty(
				dynamicconfig.ExecutionsScannerEnabled,
				false,
			),
		},
		EnableBatcher: dc.GetBoolProperty(
			dynamicconfig.EnableBatcher,
			true,
		),
		EnableParentClosePolicyWorker: dc.GetBoolProperty(
			dynamicconfig.EnableParentClosePolicyWorker,
			true,
		),
		ThrottledLogRPS: dc.GetIntProperty(
			dynamicconfig.WorkerThrottledLogRPS,
			20,
		),
		PersistenceMaxQPS: dc.GetIntProperty(
			dynamicconfig.WorkerPersistenceMaxQPS,
			500,
		),
		PersistenceGlobalMaxQPS: dc.GetIntProperty(
			dynamicconfig.WorkerPersistenceGlobalMaxQPS,
			0,
		),
	}
	return config
}

// Start is called to start the service
func (s *Service) Start() {
	if !atomic.CompareAndSwapInt32(
		&s.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	s.logger.Info(
		"worker starting",
		tag.ComponentWorker,
	)

	// todo: introduce proper counter (same in resource.go)
	s.userMetricsScope.AddCounter(metrics.RestartCount, 1)

	s.clusterMetadata.Start()
	s.membershipMonitor.Start()
	s.namespaceRegistry.Start()

	hostInfo, err := s.membershipMonitor.WhoAmI()
	if err != nil {
		s.logger.Fatal(
			"fail to get host info from membership monitor",
			tag.Error(err),
		)
	}
	s.hostInfo = hostInfo

	// The service is now started up
	// seed the random generator once for this service
	rand.Seed(time.Now().UnixNano())

	s.ensureSystemNamespaceExists()
	s.startScanner()

	if s.clusterMetadata.IsGlobalNamespaceEnabled() {
		s.startReplicator()
	}
	if s.archivalMetadata.GetHistoryConfig().ClusterConfiguredForArchival() {
		s.startArchiver()
	}
	if s.config.EnableBatcher() {
		s.startBatcher()
	}
	if s.config.EnableParentClosePolicyWorker() {
		s.startParentClosePolicyProcessor()
	}

	s.manager.Start()

	s.logger.Info(
		"worker service started",
		tag.ComponentWorker,
		tag.Address(hostInfo.GetAddress()),
	)
	<-s.stopC
}

// Stop is called to stop the service
func (s *Service) Stop() {
	if !atomic.CompareAndSwapInt32(
		&s.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	close(s.stopC)

	s.manager.Stop()
	s.namespaceRegistry.Stop()
	s.membershipMonitor.Stop()
	s.clusterMetadata.Stop()
	s.persistenceBean.Close()

	s.logger.Info(
		"worker service stopped",
		tag.ComponentWorker,
		tag.Address(s.hostInfo.GetAddress()),
	)
}

func (s *Service) startParentClosePolicyProcessor() {
	params := &parentclosepolicy.BootstrapParams{
		Config:          *s.config.ParentCloseCfg,
		SdkSystemClient: s.sdkSystemClient,
		MetricsClient:   s.metricsClient,
		Logger:          s.logger,
		ClientBean:      s.clientBean,
		CurrentCluster:  s.clusterMetadata.GetCurrentClusterName(),
	}
	processor := parentclosepolicy.New(params)
	if err := processor.Start(); err != nil {
		s.logger.Fatal(
			"error starting parentclosepolicy processor",
			tag.Error(err),
		)
	}
}

func (s *Service) startBatcher() {
	if err := batcher.New(
		s.config.BatcherCfg,
		s.sdkSystemClient,
		s.metricsClient,
		s.logger,
		s.sdkClientFactory).Start(); err != nil {
		s.logger.Fatal(
			"error starting batcher",
			tag.Error(err),
		)
	}
}

func (s *Service) startScanner() {
	sc := scanner.New(
		s.logger,
		s.config.ScannerCfg,
		s.sdkSystemClient,
		s.metricsClient,
		s.executionManager,
		s.taskManager,
		s.historyClient,
	)
	if err := sc.Start(); err != nil {
		s.logger.Fatal(
			"error starting scanner",
			tag.Error(err),
		)
	}
}

func (s *Service) startReplicator() {
	namespaceReplicationTaskExecutor := namespace.NewReplicationTaskExecutor(
		s.metadataManager,
		s.logger,
	)
	msgReplicator := replicator.NewReplicator(
		s.clusterMetadata,
		s.clientBean,
		s.logger,
		s.metricsClient,
		s.hostInfo,
		s.workerServiceResolver,
		s.namespaceReplicationQueue,
		namespaceReplicationTaskExecutor,
	)
	msgReplicator.Start()
}

func (s *Service) startArchiver() {
	bc := &archiver.BootstrapContainer{
		SdkSystemClient:  s.sdkSystemClient,
		MetricsClient:    s.metricsClient,
		Logger:           s.logger,
		HistoryV2Manager: s.executionManager,
		NamespaceCache:   s.namespaceRegistry,
		Config:           s.config.ArchiverConfig,
		ArchiverProvider: s.archiverProvider,
	}
	clientWorker := archiver.NewClientWorker(bc)
	if err := clientWorker.Start(); err != nil {
		clientWorker.Stop()
		s.logger.Fatal(
			"failed to start archiver",
			tag.Error(err),
		)
	}
}

func (s *Service) ensureSystemNamespaceExists() {
	_, err := s.metadataManager.GetNamespace(&persistence.GetNamespaceRequest{Name: common.SystemLocalNamespace})
	switch err.(type) {
	case nil:
		// noop
	case *serviceerror.NotFound:
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
