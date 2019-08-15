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
	"github.com/uber/cadence/client"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/archiver/filestore"
	"github.com/uber/cadence/common/archiver/provider"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/definition"
	"github.com/uber/cadence/common/elasticsearch"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	pes "github.com/uber/cadence/common/persistence/elasticsearch"
	persistencetests "github.com/uber/cadence/common/persistence/persistence-tests"
	"github.com/uber/cadence/common/service/config"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"go.uber.org/zap"
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
		metadata       archiver.ArchivalMetadata
		provider       provider.ArchiverProvider
		storeDirectory string
		historyURI     string
	}

	// TestClusterConfig are config for a test cluster
	TestClusterConfig struct {
		FrontendAddress       string
		EnableEventsV2        bool
		EnableArchival        bool
		IsMasterCluster       bool
		ClusterNo             int
		ClusterMetadata       config.ClusterMetadata
		MessagingClientConfig *MessagingClientConfig
		Persistence           persistencetests.TestBaseOptions
		HistoryConfig         *HistoryConfig
		ESConfig              elasticsearch.Config
		WorkerConfig          *WorkerConfig
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
	options.Persistence.ClusterMetadata = clusterMetadata
	testBase := persistencetests.NewTestBase(&options.Persistence)
	testBase.Setup()
	setupShards(testBase, options.HistoryConfig.NumHistoryShards, logger)
	archiverBase := newArchiverBase(options.EnableArchival, logger)
	messagingClient := getMessagingClient(options.MessagingClientConfig, logger)
	var esClient elasticsearch.Client
	var esVisibilityMgr persistence.VisibilityManager
	if options.WorkerConfig.EnableIndexer {
		var err error
		esClient, err = elasticsearch.NewClient(&options.ESConfig)
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
		dynamicconfig.GetBoolPropertyFnFilteredByDomain(options.WorkerConfig.EnableIndexer))

	pConfig := testBase.Config()
	pConfig.NumHistoryShards = options.HistoryConfig.NumHistoryShards
	cadenceParams := &CadenceParams{
		ClusterMetadata:     clusterMetadata,
		PersistenceConfig:   pConfig,
		DispatcherProvider:  client.NewDNSYarpcDispatcherProvider(logger, 0),
		MessagingClient:     messagingClient,
		MetadataMgr:         testBase.MetadataProxy,
		MetadataMgrV2:       testBase.MetadataManagerV2,
		ShardMgr:            testBase.ShardMgr,
		HistoryMgr:          testBase.HistoryMgr,
		HistoryV2Mgr:        testBase.HistoryV2Mgr,
		ExecutionMgrFactory: testBase.ExecutionMgrFactory,
		TaskMgr:             testBase.TaskMgr,
		VisibilityMgr:       visibilityMgr,
		Logger:              logger,
		ClusterNo:           options.ClusterNo,
		EnableEventsV2:      options.EnableEventsV2,
		ESConfig:            &options.ESConfig,
		ESClient:            esClient,
		ArchiverMetadata:    archiverBase.metadata,
		ArchiverProvider:    archiverBase.provider,
		HistoryConfig:       options.HistoryConfig,
		WorkerConfig:        options.WorkerConfig,
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

	storeDirectory, err := ioutil.TempDir("", "test-archiver")
	if err != nil {
		logger.Fatal("Failed to create temp dir for archiver", tag.Error(err))
	}
	cfg := &config.FilestoreHistoryArchiver{
		FileMode: "0700",
		DirMode:  "0600",
	}
	provider := provider.NewArchiverProvider(&config.HistoryArchiverProvider{
		Filestore: cfg,
	}, nil)
	return &ArchiverBase{
		metadata: archiver.NewArchivalMetadata(dcCollection, "enabled", true, "enabled", true, &config.ArchivalDomainDefaults{
			History: config.HistoryArchivalDomainDefaults{
				Status: "enabled",
				URI:    "testScheme://test/archive/path",
			},
			Visibility: config.VisibilityArchivalDomainDefaults{
				Status: "enabled",
				URI:    "testScheme://test/archive/path",
			},
		}),
		provider:       provider,
		storeDirectory: storeDirectory,
		historyURI:     filestore.URIScheme + "://" + storeDirectory,
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
	os.RemoveAll(tc.archiverBase.storeDirectory)
}

// GetFrontendClient returns a frontend client from the test cluster
func (tc *TestCluster) GetFrontendClient() FrontendClient {
	return tc.host.GetFrontendClient()
}

// GetAdminClient returns an admin client from the test cluster
func (tc *TestCluster) GetAdminClient() AdminClient {
	return tc.host.GetAdminClient()
}
