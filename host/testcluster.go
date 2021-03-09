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

package host

import (
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/filestore"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	pes "go.temporal.io/server/common/persistence/elasticsearch"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/service/config"
	"go.temporal.io/server/common/service/dynamicconfig"
)

type (
	// TestCluster is a base struct for integration tests
	TestCluster struct {
		testBase     persistencetests.TestBase
		archiverBase *ArchiverBase
		host         Temporal
	}

	// ArchiverBase is a base struct for archiver provider being used in integration tests
	ArchiverBase struct {
		metadata                 archiver.ArchivalMetadata
		provider                 provider.ArchiverProvider
		historyStoreDirectory    string
		visibilityStoreDirectory string
		historyURI               string
		visibilityURI            string
	}

	// TestClusterConfig are config for a test cluster
	TestClusterConfig struct {
		FrontendAddress string
		EnableArchival  bool
		IsMasterCluster bool
		ClusterNo       int
		ClusterMetadata config.ClusterMetadata
		Persistence     persistencetests.TestBaseOptions
		HistoryConfig   *HistoryConfig
		ESConfig        *config.Elasticsearch
		WorkerConfig    *WorkerConfig
		MockAdminClient map[string]adminservice.AdminServiceClient
	}

	// WorkerConfig is the config for enabling/disabling Temporal worker
	WorkerConfig struct {
		EnableArchiver   bool
		EnableIndexer    bool
		EnableReplicator bool
	}
)

const (
	defaultTestValueOfESIndexMaxResultWindow = 5
	pprofTestPort                            = 7000
)

// NewCluster creates and sets up the test cluster
func NewCluster(options *TestClusterConfig, logger log.Logger) (*TestCluster, error) {

	clusterMetadata := cluster.GetTestClusterMetadata(
		options.ClusterMetadata.EnableGlobalNamespace,
		options.IsMasterCluster,
	)
	if !options.IsMasterCluster && options.ClusterMetadata.MasterClusterName != "" { // xdc cluster metadata setup
		clusterMetadata = cluster.NewMetadata(
			logger,
			options.ClusterMetadata.EnableGlobalNamespace,
			options.ClusterMetadata.FailoverVersionIncrement,
			options.ClusterMetadata.MasterClusterName,
			options.ClusterMetadata.CurrentClusterName,
			options.ClusterMetadata.ClusterInformation,
		)
	}

	options.Persistence.StoreType = TestFlags.PersistenceType
	switch TestFlags.PersistenceType {
	case config.StoreTypeSQL:
		var ops *persistencetests.TestBaseOptions
		switch TestFlags.PersistenceDriver {
		case mysql.PluginName:
			ops = persistencetests.GetMySQLTestClusterOption()
		case postgresql.PluginName:
			ops = persistencetests.GetPostgreSQLTestClusterOption()
		default:
			panic(fmt.Sprintf("unknown sql store drier: %v", TestFlags.PersistenceDriver))
		}
		options.Persistence.SQLDBPluginName = TestFlags.PersistenceDriver
		options.Persistence.DBUsername = ops.DBUsername
		options.Persistence.DBPassword = ops.DBPassword
		options.Persistence.DBHost = ops.DBHost
		options.Persistence.DBPort = ops.DBPort
		options.Persistence.SchemaDir = ops.SchemaDir
	case config.StoreTypeNoSQL:
		// noop for now
	default:
		panic(fmt.Sprintf("unknown store type: %v", options.Persistence.StoreType))
	}

	options.Persistence.ClusterMetadata = clusterMetadata
	testBase := persistencetests.NewTestBase(&options.Persistence)
	testBase.Setup()
	setupShards(testBase, options.HistoryConfig.NumHistoryShards, logger)
	archiverBase := newArchiverBase(options.EnableArchival, logger)
	var esClient pes.Client
	var esVisibilityMgr persistence.VisibilityManager
	advancedVisibilityWritingMode := dynamicconfig.GetStringPropertyFn(common.AdvancedVisibilityWritingModeOff)
	if options.WorkerConfig.EnableIndexer {
		advancedVisibilityWritingMode = dynamicconfig.GetStringPropertyFn(common.AdvancedVisibilityWritingModeOn)
		var err error
		esClient, err = pes.NewClient(options.ESConfig, nil, logger)
		if err != nil {
			return nil, err
		}

		esProcessorConfig := &pes.ProcessorConfig{
			IndexerConcurrency:       dynamicconfig.GetIntPropertyFn(32),
			ESProcessorNumOfWorkers:  dynamicconfig.GetIntPropertyFn(1),
			ESProcessorBulkActions:   dynamicconfig.GetIntPropertyFn(10),
			ESProcessorBulkSize:      dynamicconfig.GetIntPropertyFn(2 << 20),
			ESProcessorFlushInterval: dynamicconfig.GetDurationPropertyFn(1 * time.Minute),
			ValidSearchAttributes:    dynamicconfig.GetMapPropertyFn(searchattribute.GetDefaultTypeMap()),
		}
		esProcessor := pes.NewProcessor(esProcessorConfig, esClient, logger, &metrics.NoopMetricsClient{})
		esProcessor.Start()

		visConfig := &config.VisibilityConfig{
			VisibilityListMaxQPS:   dynamicconfig.GetIntPropertyFilteredByNamespace(2000),
			ESIndexMaxResultWindow: dynamicconfig.GetIntPropertyFn(defaultTestValueOfESIndexMaxResultWindow),
			ValidSearchAttributes:  dynamicconfig.GetMapPropertyFn(searchattribute.GetDefaultTypeMap()),
			ESProcessorAckTimeout:  dynamicconfig.GetDurationPropertyFn(1 * time.Minute),
		}
		indexName := options.ESConfig.Indices[common.VisibilityAppName]
		esVisibilityStore := pes.NewElasticSearchVisibilityStore(
			esClient, indexName, esProcessor, visConfig, logger, &metrics.NoopMetricsClient{},
		)
		esVisibilityMgr = persistence.NewVisibilityManagerImpl(esVisibilityStore, visConfig.ValidSearchAttributes, logger)
	}
	visibilityMgr := persistence.NewVisibilityManagerWrapper(testBase.VisibilityMgr, esVisibilityMgr,
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(options.WorkerConfig.EnableIndexer), advancedVisibilityWritingMode)

	pConfig := testBase.Config()
	pConfig.NumHistoryShards = options.HistoryConfig.NumHistoryShards

	temporalParams := &TemporalParams{
		ClusterMetadata:                  clusterMetadata,
		PersistenceConfig:                pConfig,
		MetadataMgr:                      testBase.MetadataManager,
		ShardMgr:                         testBase.ShardMgr,
		HistoryV2Mgr:                     testBase.HistoryV2Mgr,
		ExecutionMgrFactory:              testBase.ExecutionMgrFactory,
		NamespaceReplicationQueue:        testBase.NamespaceReplicationQueue,
		TaskMgr:                          testBase.TaskMgr,
		VisibilityMgr:                    visibilityMgr,
		Logger:                           logger,
		ClusterNo:                        options.ClusterNo,
		ESConfig:                         options.ESConfig,
		ESClient:                         esClient,
		ArchiverMetadata:                 archiverBase.metadata,
		ArchiverProvider:                 archiverBase.provider,
		HistoryConfig:                    options.HistoryConfig,
		WorkerConfig:                     options.WorkerConfig,
		MockAdminClient:                  options.MockAdminClient,
		NamespaceReplicationTaskExecutor: namespace.NewReplicationTaskExecutor(testBase.MetadataManager, logger),
	}

	err := newPProfInitializerImpl(logger, pprofTestPort).Start()
	if err != nil {
		logger.Fatal("Failed to start pprof", tag.Error(err))
	}

	cluster := NewTemporal(temporalParams)
	if err := cluster.Start(); err != nil {
		return nil, err
	}

	return &TestCluster{testBase: testBase, archiverBase: archiverBase, host: cluster}, nil
}

func newPProfInitializerImpl(logger log.Logger, port int) common.PProfInitializer {
	return &config.PProfInitializerImpl{
		PProf: &config.PProf{
			Port: port,
		},
		Logger: logger,
	}
}

func setupShards(testBase persistencetests.TestBase, numHistoryShards int32, logger log.Logger) {
	for shardID := int32(1); shardID <= numHistoryShards; shardID++ {
		err := testBase.CreateShard(int32(shardID), "", 0)
		if err != nil {
			logger.Fatal("Failed to create shard", tag.Error(err))
		}
	}
}

func newArchiverBase(enabled bool, logger log.Logger) *ArchiverBase {
	dcCollection := dynamicconfig.NewNopCollection()
	if !enabled {
		return &ArchiverBase{
			metadata: archiver.NewArchivalMetadata(dcCollection, "", false, "", false, &config.ArchivalNamespaceDefaults{}),
			provider: provider.NewArchiverProvider(nil, nil),
		}
	}

	historyStoreDirectory, err := ioutil.TempDir("", "test-history-archival")
	if err != nil {
		logger.Fatal("Failed to create temp dir for history archival", tag.Error(err))
	}
	visibilityStoreDirectory, err := ioutil.TempDir("", "test-visibility-archival")
	if err != nil {
		logger.Fatal("Failed to create temp dir for visibility archival", tag.Error(err))
	}
	cfg := &config.FilestoreArchiver{
		FileMode: "0666",
		DirMode:  "0766",
	}
	provider := provider.NewArchiverProvider(
		&config.HistoryArchiverProvider{
			Filestore: cfg,
		},
		&config.VisibilityArchiverProvider{
			Filestore: cfg,
		},
	)
	return &ArchiverBase{
		metadata: archiver.NewArchivalMetadata(dcCollection, "enabled", true, "enabled", true, &config.ArchivalNamespaceDefaults{
			History: config.HistoryArchivalNamespaceDefaults{
				State: "enabled",
				URI:   "testScheme://test/history/archive/path",
			},
			Visibility: config.VisibilityArchivalNamespaceDefaults{
				State: "enabled",
				URI:   "testScheme://test/visibility/archive/path",
			},
		}),
		provider:                 provider,
		historyStoreDirectory:    historyStoreDirectory,
		visibilityStoreDirectory: visibilityStoreDirectory,
		historyURI:               filestore.URIScheme + "://" + historyStoreDirectory,
		visibilityURI:            filestore.URIScheme + "://" + visibilityStoreDirectory,
	}
}

// TearDownCluster tears down the test cluster
func (tc *TestCluster) TearDownCluster() {
	tc.host.Stop()
	tc.host = nil
	tc.testBase.TearDownWorkflowStore()
	os.RemoveAll(tc.archiverBase.historyStoreDirectory)
	os.RemoveAll(tc.archiverBase.visibilityStoreDirectory)
}

// GetFrontendClient returns a frontend client from the test cluster
func (tc *TestCluster) GetFrontendClient() FrontendClient {
	return tc.host.GetFrontendClient()
}

// GetAdminClient returns an admin client from the test cluster
func (tc *TestCluster) GetAdminClient() AdminClient {
	return tc.host.GetAdminClient()
}

// GetHistoryClient returns a history client from the test cluster
func (tc *TestCluster) GetHistoryClient() HistoryClient {
	return tc.host.GetHistoryClient()
}

// GetExecutionManagerFactory returns an execution manager factory from the test cluster
func (tc *TestCluster) GetExecutionManagerFactory() persistence.ExecutionManagerFactory {
	return tc.host.GetExecutionManagerFactory()
}
