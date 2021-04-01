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
	"sync/atomic"
	"time"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/service/worker/archiver"
	"go.temporal.io/server/service/worker/batcher"
	"go.temporal.io/server/service/worker/parentclosepolicy"
	"go.temporal.io/server/service/worker/replicator"
	"go.temporal.io/server/service/worker/scanner"
)

type (
	// Service represents the temporal-worker service. This service hosts all background processing needed for temporal cluster:
	// 1. Replicator: Handles applying replication tasks generated by remote clusters.
	// 2. Indexer: Handles uploading of visibility records to elastic search.
	// 3. Archiver: Handles archival of workflow histories.
	Service struct {
		resource.Resource

		status int32
		stopC  chan struct{}
		params *resource.BootstrapParams
		config *Config
	}

	// Config contains all the service config for worker
	Config struct {
		ReplicationCfg                *replicator.Config
		ArchiverConfig                *archiver.Config
		ScannerCfg                    *scanner.Config
		BatcherCfg                    *batcher.Config
		ThrottledLogRPS               dynamicconfig.IntPropertyFn
		PersistenceGlobalMaxQPS       dynamicconfig.IntPropertyFn
		EnableBatcher                 dynamicconfig.BoolPropertyFn
		EnableParentClosePolicyWorker dynamicconfig.BoolPropertyFn
	}
)

// NewService builds a new worker service
func NewService(
	params *resource.BootstrapParams,
) (resource.Resource, error) {

	serviceConfig := NewConfig(params)

	serviceResource, err := resource.New(
		params,
		common.WorkerServiceName,
		serviceConfig.ReplicationCfg.PersistenceMaxQPS,
		serviceConfig.PersistenceGlobalMaxQPS,
		serviceConfig.ThrottledLogRPS,
		func(
			persistenceBean persistenceClient.Bean,
			logger log.Logger,
		) (persistence.VisibilityManager, error) {
			return persistenceBean.GetVisibilityManager(), nil
		},
	)
	if err != nil {
		return nil, err
	}

	return &Service{
		Resource: serviceResource,
		status:   common.DaemonStatusInitialized,
		config:   serviceConfig,
		params:   params,
		stopC:    make(chan struct{}),
	}, nil
}

// NewConfig builds the new Config for worker service
func NewConfig(params *resource.BootstrapParams) *Config {
	dc := dynamicconfig.NewCollection(params.DynamicConfig, params.Logger)
	config := &Config{
		ReplicationCfg: &replicator.Config{
			PersistenceMaxQPS:                  dc.GetIntProperty(dynamicconfig.WorkerPersistenceMaxQPS, 500),
			ReplicatorMetaTaskConcurrency:      dc.GetIntProperty(dynamicconfig.WorkerReplicatorMetaTaskConcurrency, 64),
			ReplicatorTaskConcurrency:          dc.GetIntProperty(dynamicconfig.WorkerReplicatorTaskConcurrency, 256),
			ReplicatorMessageConcurrency:       dc.GetIntProperty(dynamicconfig.WorkerReplicatorMessageConcurrency, 2048),
			ReplicatorActivityBufferRetryCount: dc.GetIntProperty(dynamicconfig.WorkerReplicatorActivityBufferRetryCount, 8),
			ReplicatorHistoryBufferRetryCount:  dc.GetIntProperty(dynamicconfig.WorkerReplicatorHistoryBufferRetryCount, 8),
			ReplicationTaskMaxRetryCount:       dc.GetIntProperty(dynamicconfig.WorkerReplicationTaskMaxRetryCount, 400),
			ReplicationTaskMaxRetryDuration:    dc.GetDurationProperty(dynamicconfig.WorkerReplicationTaskMaxRetryDuration, 15*time.Minute),
			ReplicationTaskContextTimeout:      dc.GetDurationProperty(dynamicconfig.WorkerReplicationTaskContextDuration, 30*time.Second),
			ReReplicationContextTimeout:        dc.GetDurationPropertyFilteredByNamespaceID(dynamicconfig.WorkerReReplicationContextTimeout, 0*time.Second),
		},
		ArchiverConfig: &archiver.Config{
			ArchiverConcurrency:           dc.GetIntProperty(dynamicconfig.WorkerArchiverConcurrency, 50),
			ArchivalsPerIteration:         dc.GetIntProperty(dynamicconfig.WorkerArchivalsPerIteration, 1000),
			TimeLimitPerArchivalIteration: dc.GetDurationProperty(dynamicconfig.WorkerTimeLimitPerArchivalIteration, archiver.MaxArchivalIterationTimeout()),
		},
		ScannerCfg: &scanner.Config{
			PersistenceMaxQPS:        dc.GetIntProperty(dynamicconfig.ScannerPersistenceMaxQPS, 100),
			Persistence:              &params.PersistenceConfig,
			TaskQueueScannerEnabled:  dc.GetBoolProperty(dynamicconfig.TaskQueueScannerEnabled, true),
			HistoryScannerEnabled:    dc.GetBoolProperty(dynamicconfig.HistoryScannerEnabled, true),
			ExecutionsScannerEnabled: dc.GetBoolProperty(dynamicconfig.ExecutionsScannerEnabled, false),
		},
		BatcherCfg:                    &batcher.Config{},
		EnableBatcher:                 dc.GetBoolProperty(dynamicconfig.EnableBatcher, true),
		EnableParentClosePolicyWorker: dc.GetBoolProperty(dynamicconfig.EnableParentClosePolicyWorker, true),
		ThrottledLogRPS:               dc.GetIntProperty(dynamicconfig.WorkerThrottledLogRPS, 20),
		PersistenceGlobalMaxQPS:       dc.GetIntProperty(dynamicconfig.WorkerPersistenceGlobalMaxQPS, 0),
	}
	return config
}

// Start is called to start the service
func (s *Service) Start() {
	if !atomic.CompareAndSwapInt32(&s.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	logger := s.GetLogger()
	logger.Info("worker starting", tag.ComponentWorker)

	s.Resource.Start()

	s.ensureSystemNamespaceExists()
	s.startScanner()

	if s.GetClusterMetadata().IsGlobalNamespaceEnabled() {
		s.startReplicator()
	}
	if s.GetArchivalMetadata().GetHistoryConfig().ClusterConfiguredForArchival() {
		s.startArchiver()
	}
	if s.config.EnableBatcher() {
		s.startBatcher()
	}
	if s.config.EnableParentClosePolicyWorker() {
		s.startParentClosePolicyProcessor()
	}

	logger.Info("worker started", tag.ComponentWorker)
	<-s.stopC
}

// Stop is called to stop the service
func (s *Service) Stop() {
	if !atomic.CompareAndSwapInt32(&s.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	close(s.stopC)

	s.Resource.Stop()

	s.params.Logger.Info("worker stopped", tag.ComponentWorker)
}

func (s *Service) startParentClosePolicyProcessor() {
	params := &parentclosepolicy.BootstrapParams{
		ServiceClient: s.params.PublicClient,
		MetricsClient: s.GetMetricsClient(),
		Logger:        s.GetLogger(),
		ClientBean:    s.GetClientBean(),
	}
	processor := parentclosepolicy.New(params)
	if err := processor.Start(); err != nil {
		s.GetLogger().Fatal("error starting parentclosepolicy processor", tag.Error(err))
	}
}

func (s *Service) startBatcher() {
	params := &batcher.BootstrapParams{
		Config:        *s.config.BatcherCfg,
		ServiceClient: s.params.PublicClient,
		MetricsClient: s.GetMetricsClient(),
		Logger:        s.GetLogger(),
		ClientBean:    s.GetClientBean(),
	}
	if err := batcher.New(params).Start(); err != nil {
		s.GetLogger().Fatal("error starting batcher", tag.Error(err))
	}
}

func (s *Service) startScanner() {
	params := &scanner.BootstrapParams{
		Config: *s.config.ScannerCfg,
	}
	if err := scanner.New(s.Resource, params).Start(); err != nil {
		s.GetLogger().Fatal("error starting scanner", tag.Error(err))
	}
}

func (s *Service) startReplicator() {
	namespaceReplicationTaskExecutor := namespace.NewReplicationTaskExecutor(
		s.GetMetadataManager(),
		s.GetLogger(),
	)
	msgReplicator := replicator.NewReplicator(
		s.GetClusterMetadata(),
		s.GetClientBean(),
		s.config.ReplicationCfg,
		s.GetLogger(),
		s.GetMetricsClient(),
		s.GetHostInfo(),
		s.GetWorkerServiceResolver(),
		s.GetNamespaceReplicationQueue(),
		namespaceReplicationTaskExecutor,
	)
	msgReplicator.Start()
}

func (s *Service) startArchiver() {
	bc := &archiver.BootstrapContainer{
		PublicClient:     s.GetSDKClient(),
		MetricsClient:    s.GetMetricsClient(),
		Logger:           s.GetLogger(),
		HistoryV2Manager: s.GetHistoryManager(),
		NamespaceCache:   s.GetNamespaceCache(),
		Config:           s.config.ArchiverConfig,
		ArchiverProvider: s.GetArchiverProvider(),
	}
	clientWorker := archiver.NewClientWorker(bc)
	if err := clientWorker.Start(); err != nil {
		clientWorker.Stop()
		s.GetLogger().Fatal("failed to start archiver", tag.Error(err))
	}
}

func (s *Service) ensureSystemNamespaceExists() {
	_, err := s.GetMetadataManager().GetNamespace(&persistence.GetNamespaceRequest{Name: common.SystemLocalNamespace})
	switch err.(type) {
	case nil:
		// noop
	case *serviceerror.NotFound:
		s.GetLogger().Fatal("temporal-system namespace does not exist", tag.Error(err))
	default:
		s.GetLogger().Fatal("failed to verify if temporal system namespace exists", tag.Error(err))
	}
}
