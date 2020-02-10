// Copyright (c) 2019 Uber Technologies, Inc.
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
	"io/ioutil"
	"os"

	"github.com/uber-go/tally"
	"go.uber.org/zap"

	"github.com/uber/cadence/client"
	adminClient "github.com/uber/cadence/client/admin"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/archiver/filestore"
	"github.com/uber/cadence/common/archiver/provider"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/definition"
	"github.com/uber/cadence/common/domain"
	"github.com/uber/cadence/common/elasticsearch"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	pes "github.com/uber/cadence/common/persistence/elasticsearch"
	persistencetests "github.com/uber/cadence/common/persistence/persistence-tests"
	"github.com/uber/cadence/common/persistence/sql/sqlplugin/mysql"
	"github.com/uber/cadence/common/service/config"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

type (
	// TestCluster is a base struct for integration tests
	TestCluster struct {
		testBase     persistencetests.TestBase
		archiverBase *ArchiverBase
		host         Cadence
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
		FrontendAddress       string
		EnableNDC             bool
		EnableArchival        bool
		IsMasterCluster       bool
		ClusterNo             int
		ClusterMetadata       config.ClusterMetadata
		MessagingClientConfig *MessagingClientConfig
		Persistence           persistencetests.TestBaseOptions
		HistoryConfig         *HistoryConfig
		ESConfig              *elasticsearch.Config
		WorkerConfig          *WorkerConfig
		MockAdminClient       map[string]adminClient.Client
	}

	// MessagingClientConfig is the config for messaging config
	MessagingClientConfig struct {
		UseMock     bool
		KafkaConfig *messaging.KafkaConfig
	}

	// WorkerConfig is the config for enabling/disabling cadence worker
	WorkerConfig struct {
		EnableArchiver   bool
		EnableIndexer    bool
		EnableReplicator bool
	}
)

const defaultTestValueOfESIndexMaxResultWindow = 5

// NewCluster creates and sets up the test cluster
func NewCluster(options *TestClusterConfig, logger log.Logger) (*TestCluster, error) {

	clusterMetadata := cluster.GetTestClusterMetadata(
		options.ClusterMetadata.EnableGlobalDomain,
		options.IsMasterCluster,
	)
	if !options.IsMasterCluster && options.ClusterMetadata.MasterClusterName != "" { // xdc cluster metadata setup
		clusterMetadata = cluster.NewMetadata(
			logger,
			dynamicconfig.GetBoolPropertyFn(options.ClusterMetadata.EnableGlobalDomain),
			options.ClusterMetadata.FailoverVersionIncrement,
			options.ClusterMetadata.MasterClusterName,
			options.ClusterMetadata.CurrentClusterName,
			options.ClusterMetadata.ClusterInformation,
			options.ClusterMetadata.ReplicationConsumer,
		)
	}

	options.Persistence.StoreType = TestFlags.PersistenceType
	if TestFlags.PersistenceType == config.StoreTypeSQL {
		var ops *persistencetests.TestBaseOptions
		if TestFlags.SQLPluginName == mysql.PluginName {
			ops = mysql.GetTestClusterOption()
		} else {
			panic("not supported plugin " + TestFlags.SQLPluginName)
		}
		options.Persistence.SQLDBPluginName = TestFlags.SQLPluginName
		options.Persistence.DBUsername = ops.DBUsername
		options.Persistence.DBPassword = ops.DBPassword
		options.Persistence.DBHost = ops.DBHost
		options.Persistence.DBPort = ops.DBPort
		options.Persistence.SchemaDir = ops.SchemaDir
	}
	options.Persistence.ClusterMetadata = clusterMetadata
	testBase := persistencetests.NewTestBase(&options.Persistence)
	testBase.Setup()
	setupShards(testBase, options.HistoryConfig.NumHistoryShards, logger)
	archiverBase := newArchiverBase(options.EnableArchival, logger)
	messagingClient := getMessagingClient(options.MessagingClientConfig, logger)
	var esClient elasticsearch.Client
	var esVisibilityMgr persistence.VisibilityManager
	advancedVisibilityWritingMode := dynamicconfig.GetStringPropertyFn(common.AdvancedVisibilityWritingModeOff)
	if options.WorkerConfig.EnableIndexer {
		advancedVisibilityWritingMode = dynamicconfig.GetStringPropertyFn(common.AdvancedVisibilityWritingModeOn)
		var err error
		esClient, err = elasticsearch.NewClient(options.ESConfig)
		if err != nil {
			return nil, err
		}

		indexName := options.ESConfig.Indices[common.VisibilityAppName]
		visProducer, err := messagingClient.NewProducer(common.VisibilityAppName)
		if err != nil {
			return nil, err
		}
		visConfig := &config.VisibilityConfig{
			VisibilityListMaxQPS:   dynamicconfig.GetIntPropertyFilteredByDomain(2000),
			ESIndexMaxResultWindow: dynamicconfig.GetIntPropertyFn(defaultTestValueOfESIndexMaxResultWindow),
			ValidSearchAttributes:  dynamicconfig.GetMapPropertyFn(definition.GetDefaultIndexedKeys()),
		}
		esVisibilityStore := pes.NewElasticSearchVisibilityStore(esClient, indexName, visProducer, visConfig, logger)
		esVisibilityMgr = persistence.NewVisibilityManagerImpl(esVisibilityStore, logger)
	}
	visibilityMgr := persistence.NewVisibilityManagerWrapper(testBase.VisibilityMgr, esVisibilityMgr,
		dynamicconfig.GetBoolPropertyFnFilteredByDomain(options.WorkerConfig.EnableIndexer), advancedVisibilityWritingMode)

	pConfig := testBase.Config()
	pConfig.NumHistoryShards = options.HistoryConfig.NumHistoryShards
	cadenceParams := &CadenceParams{
		ClusterMetadata:               clusterMetadata,
		PersistenceConfig:             pConfig,
		DispatcherProvider:            client.NewDNSYarpcDispatcherProvider(logger, 0),
		MessagingClient:               messagingClient,
		MetadataMgr:                   testBase.MetadataManager,
		ShardMgr:                      testBase.ShardMgr,
		HistoryV2Mgr:                  testBase.HistoryV2Mgr,
		ExecutionMgrFactory:           testBase.ExecutionMgrFactory,
		DomainReplicationQueue:        testBase.DomainReplicationQueue,
		TaskMgr:                       testBase.TaskMgr,
		VisibilityMgr:                 visibilityMgr,
		Logger:                        logger,
		ClusterNo:                     options.ClusterNo,
		EnableNDC:                     options.EnableNDC,
		ESConfig:                      options.ESConfig,
		ESClient:                      esClient,
		ArchiverMetadata:              archiverBase.metadata,
		ArchiverProvider:              archiverBase.provider,
		HistoryConfig:                 options.HistoryConfig,
		WorkerConfig:                  options.WorkerConfig,
		MockAdminClient:               options.MockAdminClient,
		DomainReplicationTaskExecutor: domain.NewReplicationTaskExecutor(testBase.MetadataManager, logger),
	}
	cluster := NewCadence(cadenceParams)
	if err := cluster.Start(); err != nil {
		return nil, err
	}

	return &TestCluster{testBase: testBase, archiverBase: archiverBase, host: cluster}, nil
}

func setupShards(testBase persistencetests.TestBase, numHistoryShards int, logger log.Logger) {
	// shard 0 is always created, we create additional shards if needed
	for shardID := 1; shardID < numHistoryShards; shardID++ {
		err := testBase.CreateShard(shardID, "", 0)
		if err != nil {
			logger.Fatal("Failed to create shard", tag.Error(err))
		}
	}
}

func newArchiverBase(enabled bool, logger log.Logger) *ArchiverBase {
	dcCollection := dynamicconfig.NewNopCollection()
	if !enabled {
		return &ArchiverBase{
			metadata: archiver.NewArchivalMetadata(dcCollection, "", false, "", false, &config.ArchivalDomainDefaults{}),
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
		metadata: archiver.NewArchivalMetadata(dcCollection, "enabled", true, "enabled", true, &config.ArchivalDomainDefaults{
			History: config.HistoryArchivalDomainDefaults{
				Status: "enabled",
				URI:    "testScheme://test/history/archive/path",
			},
			Visibility: config.VisibilityArchivalDomainDefaults{
				Status: "enabled",
				URI:    "testScheme://test/visibility/archive/path",
			},
		}),
		provider:                 provider,
		historyStoreDirectory:    historyStoreDirectory,
		visibilityStoreDirectory: visibilityStoreDirectory,
		historyURI:               filestore.URIScheme + "://" + historyStoreDirectory,
		visibilityURI:            filestore.URIScheme + "://" + visibilityStoreDirectory,
	}
}

func getMessagingClient(config *MessagingClientConfig, logger log.Logger) messaging.Client {
	if config == nil || config.UseMock {
		return mocks.NewMockMessagingClient(&mocks.KafkaProducer{}, nil)
	}
	checkCluster := len(config.KafkaConfig.ClusterToTopic) != 0
	checkApp := len(config.KafkaConfig.Applications) != 0
	return messaging.NewKafkaClient(config.KafkaConfig, nil, zap.NewNop(), logger, tally.NoopScope, checkCluster, checkApp)
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
