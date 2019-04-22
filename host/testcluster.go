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
	"github.com/uber/cadence/common/blobstore/filestore"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/elasticsearch"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/messaging"
	metricsmocks "github.com/uber/cadence/common/metrics/mocks"
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
		testBase  persistencetests.TestBase
		blobstore *BlobstoreBase
		host      Cadence
	}

	// TestClusterConfig are config for a test cluster
	TestClusterConfig struct {
		FrontendAddress       string
		EnableEventsV2        bool
		EnableArchival        bool
		IsMasterCluster       bool
		ClusterNo             int
		ClusterInfo           config.ClustersInfo
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

// NewCluster creates and sets up the test cluster
func NewCluster(options *TestClusterConfig, logger log.Logger) (*TestCluster, error) {
	clusterInfo := options.ClusterInfo
	clusterMetadata := cluster.GetTestClusterMetadata(clusterInfo.EnableGlobalDomain, options.IsMasterCluster, options.EnableArchival)
	if !options.IsMasterCluster && options.ClusterInfo.MasterClusterName != "" { // xdc cluster metadata setup
		clusterMetadata = cluster.NewMetadata(
			logger,
			&metricsmocks.Client{},
			dynamicconfig.GetBoolPropertyFn(clusterInfo.EnableGlobalDomain),
			clusterInfo.FailoverVersionIncrement,
			clusterInfo.MasterClusterName,
			clusterInfo.CurrentClusterName,
			clusterInfo.ClusterInitialFailoverVersions,
			clusterInfo.ClusterAddress,
			dynamicconfig.GetStringPropertyFn("disabled"),
			"",
			dynamicconfig.GetBoolPropertyFn(false),
		)
	}

	options.Persistence.StoreType = TestFlags.PersistenceType
	options.Persistence.ClusterMetadata = clusterMetadata
	testBase := persistencetests.NewTestBase(&options.Persistence)
	testBase.Setup()
	setupShards(testBase, options.HistoryConfig.NumHistoryShards, logger)
	blobstore := setupBlobstore(logger)
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
			ESIndexMaxResultWindow: dynamicconfig.GetIntPropertyFn(100),
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
		DispatcherProvider:  client.NewIPYarpcDispatcherProvider(),
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
		Blobstore:           blobstore.client,
		HistoryConfig:       options.HistoryConfig,
		WorkerConfig:        options.WorkerConfig,
	}
	cluster := NewCadence(cadenceParams)
	if err := cluster.Start(); err != nil {
		return nil, err
	}

	return &TestCluster{testBase: testBase, blobstore: blobstore, host: cluster}, nil
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

func setupBlobstore(logger log.Logger) *BlobstoreBase {
	bucketName := "default-test-bucket"
	storeDirectory, err := ioutil.TempDir("", "test-blobstore")
	if err != nil {
		logger.Fatal("Failed to create temp dir for blobstore", tag.Error(err))
	}
	cfg := &filestore.Config{
		StoreDirectory: storeDirectory,
		DefaultBucket: filestore.BucketConfig{
			Name:          bucketName,
			Owner:         "test-owner",
			RetentionDays: 10,
		},
	}
	client, err := filestore.NewClient(cfg)
	if err != nil {
		logger.Fatal("Failed to construct blobstore client", tag.Error(err))
	}
	return &BlobstoreBase{
		client:         client,
		storeDirectory: storeDirectory,
		bucketName:     bucketName,
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
	os.RemoveAll(tc.blobstore.storeDirectory)
}

// GetFrontendClient returns a frontend client from the test cluster
func (tc *TestCluster) GetFrontendClient() FrontendClient {
	return tc.host.GetFrontendClient()
}

// GetAdminClient returns an admin client from the test cluster
func (tc *TestCluster) GetAdminClient() AdminClient {
	return tc.host.GetAdminClient()
}
