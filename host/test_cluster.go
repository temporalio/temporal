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
	"context"
	"fmt"
	"os"

	"github.com/pborman/uuid"
	"go.temporal.io/api/operatorservice/v1"

	"go.temporal.io/server/api/adminservice/v1"
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
		case postgresql.PluginName:
			ops = persistencetests.GetPostgreSQLTestClusterOption()
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

	if TestFlags.PersistenceFaultInjectionRate > 0 {
		options.Persistence.FaultInjection = &config.FaultInjection{Rate: TestFlags.PersistenceFaultInjectionRate}
	} else if options.Persistence.FaultInjection == nil {
		options.Persistence.FaultInjection = &config.FaultInjection{Rate: 0}
	}

	testBase := persistencetests.NewTestBase(&options.Persistence)
	testBase.Setup(clusterMetadataConfig)
	archiverBase := newArchiverBase(options.EnableArchival, logger)

	pConfig := testBase.DefaultTestCluster.Config()
	pConfig.NumHistoryShards = options.HistoryConfig.NumHistoryShards

	var (
		esClient esclient.Client
	)
	if options.WorkerConfig.EnableIndexer {
		var err error
		esClient, err = esclient.NewClient(options.ESConfig, nil, logger)
		if err != nil {
			return nil, err
		}
	}

	for clusterName, clusterInfo := range clusterMetadataConfig.ClusterInformation {
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

	// This will save custom test search attributes to cluster metadata.
	// Actual Elasticsearch fields are created from index template (testdata/es_v7_index_template.json).
	err := testBase.SearchAttributesManager.SaveSearchAttributes(
		context.Background(),
		options.ESConfig.GetVisibilityIndex(),
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
	}

	err = newPProfInitializerImpl(logger, pprofTestPort).Start()
	if err != nil {
		logger.Fatal("Failed to start pprof", tag.Error(err))
	}

	cluster := NewTemporal(temporalParams)
	if err := cluster.Start(); err != nil {
		return nil, err
	}

	return &TestCluster{testBase: testBase, archiverBase: archiverBase, host: cluster}, nil
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
func (tc *TestCluster) TearDownCluster() {
	tc.SetFaultInjectionRate(0)
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

func (tc *TestCluster) GetOperatorClient() operatorservice.OperatorServiceClient {
	return tc.host.GetOperatorClient()
}

// GetHistoryClient returns a history client from the test cluster
func (tc *TestCluster) GetHistoryClient() HistoryClient {
	return tc.host.GetHistoryClient()
}

// GetExecutionManager returns an execution manager factory from the test cluster
func (tc *TestCluster) GetExecutionManager() persistence.ExecutionManager {
	return tc.host.GetExecutionManager()
}

func (tc *TestCluster) RefreshNamespaceCache() {
	tc.host.RefreshNamespaceCache()
}

func (tc *TestCluster) GetHost() *temporalImpl {
	return tc.host
}
