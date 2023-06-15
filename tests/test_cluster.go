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

package tests

import (
	"context"
	"fmt"
	"os"
	"path"

	"github.com/pborman/uuid"
	"go.temporal.io/api/operatorservice/v1"
	"go.uber.org/multierr"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/filestore"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/pprof"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/tests/testutils"
)

type (
	// TestCluster is a base struct for integration tests
	TestCluster struct {
		testBase     persistencetests.TestBase
		archiverBase *ArchiverBase
		host         *temporalImpl
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
		FrontendAddress        string
		EnableArchival         bool
		IsMasterCluster        bool
		ClusterNo              int
		ClusterMetadata        cluster.Config
		Persistence            persistencetests.TestBaseOptions
		HistoryConfig          *HistoryConfig
		ESConfig               *esclient.Config
		WorkerConfig           *WorkerConfig
		MockAdminClient        map[string]adminservice.AdminServiceClient
		FaultInjection         config.FaultInjection `yaml:"faultinjection"`
		DynamicConfigOverrides map[dynamicconfig.Key]interface{}
	}

	// WorkerConfig is the config for enabling/disabling Temporal worker
	WorkerConfig struct {
		EnableArchiver    bool
		EnableReplicator  bool
		StartWorkerAnyway bool
	}
)

const (
	defaultPageSize = 5
	pprofTestPort   = 7000
)

// NewCluster creates and sets up the test cluster
func NewCluster(options *TestClusterConfig, logger log.Logger) (*TestCluster, error) {

	clusterMetadataConfig := cluster.NewTestClusterMetadataConfig(
		options.ClusterMetadata.EnableGlobalNamespace,
		options.IsMasterCluster,
	)
	if !options.IsMasterCluster && options.ClusterMetadata.MasterClusterName != "" { // xdc cluster metadata setup
		clusterMetadataConfig = &cluster.Config{
			EnableGlobalNamespace:    options.ClusterMetadata.EnableGlobalNamespace,
			FailoverVersionIncrement: options.ClusterMetadata.FailoverVersionIncrement,
			MasterClusterName:        options.ClusterMetadata.MasterClusterName,
			CurrentClusterName:       options.ClusterMetadata.CurrentClusterName,
			ClusterInformation:       options.ClusterMetadata.ClusterInformation,
		}
	}

	options.Persistence.StoreType = TestFlags.PersistenceType
	switch TestFlags.PersistenceType {
	case config.StoreTypeSQL:
		var ops *persistencetests.TestBaseOptions
		switch TestFlags.PersistenceDriver {
		case mysql.PluginName:
			ops = persistencetests.GetMySQLTestClusterOption()
		case mysql.PluginNameV8:
			ops = persistencetests.GetMySQL8TestClusterOption()
		case postgresql.PluginName:
			ops = persistencetests.GetPostgreSQLTestClusterOption()
		case postgresql.PluginNameV12:
			ops = persistencetests.GetPostgreSQL12TestClusterOption()
		case sqlite.PluginName:
			ops = persistencetests.GetSQLiteMemoryTestClusterOption()
		default:
			panic(fmt.Sprintf("unknown sql store drier: %v", TestFlags.PersistenceDriver))
		}
		options.Persistence.SQLDBPluginName = TestFlags.PersistenceDriver
		options.Persistence.DBUsername = ops.DBUsername
		options.Persistence.DBPassword = ops.DBPassword
		options.Persistence.DBHost = ops.DBHost
		options.Persistence.DBPort = ops.DBPort
		options.Persistence.SchemaDir = ops.SchemaDir
		options.Persistence.ConnectAttributes = ops.ConnectAttributes
	case config.StoreTypeNoSQL:
		// noop for now
	default:
		panic(fmt.Sprintf("unknown store type: %v", options.Persistence.StoreType))
	}

	options.Persistence.FaultInjection = &options.FaultInjection
	// If the fault injection rate command line flag is set, override the fault injection rate in the config.
	if TestFlags.PersistenceFaultInjectionRate > 0 {
		options.Persistence.FaultInjection.Rate = TestFlags.PersistenceFaultInjectionRate
	}

	testBase := persistencetests.NewTestBase(&options.Persistence)
	testBase.Setup(clusterMetadataConfig)
	archiverBase := newArchiverBase(options.EnableArchival, logger)

	pConfig := testBase.DefaultTestCluster.Config()
	pConfig.NumHistoryShards = options.HistoryConfig.NumHistoryShards

	var (
		indexName string
		esClient  esclient.Client
	)
	if options.ESConfig != nil {
		err := setupIndex(options.ESConfig, logger)
		if err != nil {
			return nil, err
		}

		// Disable standard to elasticsearch dual visibility
		pConfig.VisibilityStore = ""
		indexName = options.ESConfig.GetVisibilityIndex()
		esClient, err = esclient.NewClient(options.ESConfig, nil, logger)
		if err != nil {
			return nil, err
		}
	} else {
		storeConfig := pConfig.DataStores[pConfig.VisibilityStore]
		if storeConfig.SQL != nil {
			switch storeConfig.SQL.PluginName {
			case mysql.PluginNameV8, postgresql.PluginNameV12, sqlite.PluginName:
				indexName = storeConfig.SQL.DatabaseName
			}
		}
	}

	clusterInfoMap := make(map[string]cluster.ClusterInformation)
	for clusterName, clusterInfo := range clusterMetadataConfig.ClusterInformation {
		clusterInfo.ShardCount = options.HistoryConfig.NumHistoryShards
		clusterInfoMap[clusterName] = clusterInfo
		_, err := testBase.ClusterMetadataManager.SaveClusterMetadata(context.Background(), &persistence.SaveClusterMetadataRequest{
			ClusterMetadata: persistencespb.ClusterMetadata{
				HistoryShardCount:        options.HistoryConfig.NumHistoryShards,
				ClusterName:              clusterName,
				ClusterId:                uuid.New(),
				IsConnectionEnabled:      clusterInfo.Enabled,
				IsGlobalNamespaceEnabled: clusterMetadataConfig.EnableGlobalNamespace,
				FailoverVersionIncrement: clusterMetadataConfig.FailoverVersionIncrement,
				ClusterAddress:           clusterInfo.RPCAddress,
				InitialFailoverVersion:   clusterInfo.InitialFailoverVersion,
			}})
		if err != nil {
			return nil, err
		}
	}
	clusterMetadataConfig.ClusterInformation = clusterInfoMap

	// This will save custom test search attributes to cluster metadata.
	// Actual Elasticsearch fields are created in setupIndex.
	err := testBase.SearchAttributesManager.SaveSearchAttributes(
		context.Background(),
		indexName,
		searchattribute.TestNameTypeMap.Custom(),
	)
	if err != nil {
		return nil, err
	}

	temporalParams := &TemporalParams{
		ClusterMetadataConfig:            clusterMetadataConfig,
		PersistenceConfig:                pConfig,
		MetadataMgr:                      testBase.MetadataManager,
		ClusterMetadataManager:           testBase.ClusterMetadataManager,
		ShardMgr:                         testBase.ShardMgr,
		ExecutionManager:                 testBase.ExecutionManager,
		NamespaceReplicationQueue:        testBase.NamespaceReplicationQueue,
		TaskMgr:                          testBase.TaskMgr,
		Logger:                           logger,
		ClusterNo:                        options.ClusterNo,
		ESConfig:                         options.ESConfig,
		ESClient:                         esClient,
		ArchiverMetadata:                 archiverBase.metadata,
		ArchiverProvider:                 archiverBase.provider,
		HistoryConfig:                    options.HistoryConfig,
		WorkerConfig:                     options.WorkerConfig,
		MockAdminClient:                  options.MockAdminClient,
		NamespaceReplicationTaskExecutor: namespace.NewReplicationTaskExecutor(options.ClusterMetadata.CurrentClusterName, testBase.MetadataManager, logger),
		DynamicConfigOverrides:           options.DynamicConfigOverrides,
	}

	err = newPProfInitializerImpl(logger, pprofTestPort).Start()
	if err != nil {
		logger.Fatal("Failed to start pprof", tag.Error(err))
	}

	cluster := newTemporal(temporalParams)
	if err := cluster.Start(); err != nil {
		return nil, err
	}

	return &TestCluster{testBase: testBase, archiverBase: archiverBase, host: cluster}, nil
}

func setupIndex(esConfig *esclient.Config, logger log.Logger) error {
	esClient, err := esclient.NewIntegrationTestsClient(esConfig, logger)
	if err != nil {
		return err
	}

	exists, err := esClient.IndexExists(context.Background(), esConfig.GetVisibilityIndex())
	if err != nil {
		return err
	}
	if exists {
		logger.Info("Index already exists.", tag.ESIndex(esConfig.GetVisibilityIndex()))
		err = deleteIndex(esConfig, logger)
		if err != nil {
			return err
		}
	}

	indexTemplateFile := path.Join(testutils.GetRepoRootDirectory(), "schema/elasticsearch/visibility/index_template_v7.json")
	logger.Info("Creating index template.", tag.NewStringTag("templatePath", indexTemplateFile))
	template, err := os.ReadFile(indexTemplateFile)
	if err != nil {
		return err
	}
	// Template name doesn't matter.
	// This operation is idempotent and won't return an error even if template already exists.
	_, err = esClient.IndexPutTemplate(context.Background(), "temporal_visibility_v1_template", string(template))
	if err != nil {
		return err
	}
	logger.Info("Index template created.")

	logger.Info("Creating index.", tag.ESIndex(esConfig.GetVisibilityIndex()))
	_, err = esClient.CreateIndex(context.Background(), esConfig.GetVisibilityIndex())
	if err != nil {
		return err
	}
	logger.Info("Index created.", tag.ESIndex(esConfig.GetVisibilityIndex()))

	logger.Info("Add custom search attributes for tests.")
	_, err = esClient.PutMapping(context.Background(), esConfig.GetVisibilityIndex(), searchattribute.TestNameTypeMap.Custom())
	if err != nil {
		return err
	}
	logger.Info("Index setup complete.", tag.ESIndex(esConfig.GetVisibilityIndex()))

	return nil
}

func deleteIndex(esConfig *esclient.Config, logger log.Logger) error {
	esClient, err := esclient.NewIntegrationTestsClient(esConfig, logger)
	if err != nil {
		return err
	}

	logger.Info("Deleting index.", tag.ESIndex(esConfig.GetVisibilityIndex()))
	_, err = esClient.DeleteIndex(context.Background(), esConfig.GetVisibilityIndex())
	if err != nil {
		return err
	}
	logger.Info("Index deleted.", tag.ESIndex(esConfig.GetVisibilityIndex()))
	return nil
}

func newPProfInitializerImpl(logger log.Logger, port int) *pprof.PProfInitializerImpl {
	return &pprof.PProfInitializerImpl{
		PProf: &config.PProf{
			Port: port,
		},
		Logger: logger,
	}
}

func newArchiverBase(enabled bool, logger log.Logger) *ArchiverBase {
	dcCollection := dynamicconfig.NewNoopCollection()
	if !enabled {
		return &ArchiverBase{
			metadata: archiver.NewArchivalMetadata(dcCollection, "", false, "", false, &config.ArchivalNamespaceDefaults{}),
			provider: provider.NewArchiverProvider(nil, nil),
		}
	}

	historyStoreDirectory, err := os.MkdirTemp("", "test-history-archival")
	if err != nil {
		logger.Fatal("Failed to create temp dir for history archival", tag.Error(err))
	}
	visibilityStoreDirectory, err := os.MkdirTemp("", "test-visibility-archival")
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

func (tc *TestCluster) SetFaultInjectionRate(rate float64) {
	if tc.testBase.FaultInjection != nil {
		tc.testBase.FaultInjection.UpdateRate(rate)
	}
	if tc.host.matchingService.GetFaultInjection() != nil {
		tc.host.matchingService.GetFaultInjection().UpdateRate(rate)
	}
	if tc.host.frontendService.GetFaultInjection() != nil {
		tc.host.frontendService.GetFaultInjection().UpdateRate(rate)
	}

	for _, s := range tc.host.historyServices {
		if s.GetFaultInjection() != nil {
			s.GetFaultInjection().UpdateRate(rate)
		}
	}
}

// TearDownCluster tears down the test cluster
func (tc *TestCluster) TearDownCluster() error {
	errs := tc.host.Stop()
	tc.testBase.TearDownWorkflowStore()
	if tc.host.esConfig != nil {
		if err := deleteIndex(tc.host.esConfig, tc.host.logger); err != nil {
			errs = multierr.Combine(errs, err)
		}
	}
	if err := os.RemoveAll(tc.archiverBase.historyStoreDirectory); err != nil {
		errs = multierr.Combine(errs, err)
	}
	if err := os.RemoveAll(tc.archiverBase.visibilityStoreDirectory); err != nil {
		errs = multierr.Combine(errs, err)
	}
	return errs
}

// GetFrontendClient returns a frontend client from the test cluster
func (tc *TestCluster) GetFrontendClient() FrontendClient {
	return tc.host.GetFrontendClient()
}

// GetAdminClient returns an admin client from the test cluster
func (tc *TestCluster) GetAdminClient() AdminClient {
	return tc.host.GetAdminClient()
}

func (tc *TestCluster) GetOperatorClient() operatorservice.OperatorServiceClient {
	return tc.host.GetOperatorClient()
}

// GetHistoryClient returns a history client from the test cluster
func (tc *TestCluster) GetHistoryClient() HistoryClient {
	return tc.host.GetHistoryClient()
}

// GetMatchingClient returns a matching client from the test cluster
func (tc *TestCluster) GetMatchingClient() matchingservice.MatchingServiceClient {
	return tc.host.GetMatchingClient()
}

// GetExecutionManager returns an execution manager factory from the test cluster
func (tc *TestCluster) GetExecutionManager() persistence.ExecutionManager {
	return tc.host.GetExecutionManager()
}

func (tc *TestCluster) GetHost() *temporalImpl {
	return tc.host
}
