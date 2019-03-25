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
	"github.com/uber-common/bark"
	wsc "github.com/uber/cadence/.gen/go/cadence/workflowserviceclient"
	"github.com/uber/cadence/client"
	"github.com/uber/cadence/common/blobstore/filestore"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/persistence/persistence-tests"
	"io/ioutil"
	"os"
	"sync"
)

type (
	// TestCluster is a base struct for integration tests
	TestCluster struct {
		persistencetests.TestBase
		BlobstoreBase
		sync.Mutex // This lock is to protect the race in ringpop initialization.

		Host   Cadence
		engine wsc.Interface
		Logger bark.Logger
	}

	// TestClusterOptions are options for test cluster
	TestClusterOptions struct {
		PersistOptions   *persistencetests.TestBaseOptions
		EnableWorker     bool
		EnableEventsV2   bool
		EnableArchival   bool
		ClusterNo        int
		NumHistoryShards int
		MessagingClient  messaging.Client
	}
)

// SetupCluster sets up the test cluster
func (tc *TestCluster) SetupCluster(options *TestClusterOptions) {
	tc.Lock()
	defer tc.Unlock()

	tc.TestBase = persistencetests.NewTestBaseWithCassandra(options.PersistOptions)
	tc.TestBase.Setup()
	tc.setupShards(options.NumHistoryShards)
	tc.setupBlobstore()

	cadenceParams := &CadenceParams{
		ClusterMetadata:               tc.ClusterMetadata,
		DispatcherProvider:            client.NewIPYarpcDispatcherProvider(),
		MessagingClient:               options.MessagingClient,
		MetadataMgr:                   tc.MetadataProxy,
		MetadataMgrV2:                 tc.MetadataManagerV2,
		ShardMgr:                      tc.ShardMgr,
		HistoryMgr:                    tc.HistoryMgr,
		HistoryV2Mgr:                  tc.HistoryV2Mgr,
		ExecutionMgrFactory:           tc.ExecutionMgrFactory,
		TaskMgr:                       tc.TaskMgr,
		VisibilityMgr:                 tc.VisibilityMgr,
		NumberOfHistoryShards:         options.NumHistoryShards,
		NumberOfHistoryHosts:          testNumberOfHistoryHosts,
		Logger:                        tc.Logger,
		ClusterNo:                     options.ClusterNo,
		EnableWorker:                  options.EnableWorker,
		EnableEventsV2:                options.EnableEventsV2,
		EnableVisibilityToKafka:       false,
		EnableReadHistoryFromArchival: true,
		Blobstore:                     tc.BlobstoreBase.client,
	}
	tc.Host = NewCadence(cadenceParams)
	tc.Host.Start()
	tc.engine = tc.Host.GetFrontendClient()
}

func (tc *TestCluster) setupShards(numHistoryShards int) {
	// shard 0 is always created, we create additional shards if needed
	for shardID := 1; shardID < numHistoryShards; shardID++ {
		err := tc.CreateShard(shardID, "", 0)
		if err != nil {
			tc.Logger.WithField("error", err).Fatal("Failed to create shard")
		}
	}
}

func (tc *TestCluster) setupBlobstore() {
	bucketName := "default-test-bucket"
	storeDirectory, err := ioutil.TempDir("", "test-blobstore")
	if err != nil {
		tc.Logger.WithField("error", err).Fatal("Failed to create temp dir for blobstore")
	}
	cfg := &filestore.Config{
		StoreDirectory: storeDirectory,
		DefaultBucket: filestore.BucketConfig{
			Name:          bucketName,
			Owner:         "test-owner",
			RetentionDays: 10,
		},
	}
	client, err := filestore.NewClient(cfg, tc.Logger)
	if err != nil {
		tc.Logger.WithField("error", err).Fatal("Failed to construct blobstore client")
	}
	tc.BlobstoreBase = BlobstoreBase{
		client:         client,
		storeDirectory: storeDirectory,
		bucketName:     bucketName,
	}
}

// TearDownCluster tears down the test cluster
func (tc *TestCluster) TearDownCluster() {
	tc.Lock()
	defer tc.Unlock()

	tc.Host.Stop()
	tc.Host = nil
	tc.TearDownWorkflowStore()
	os.RemoveAll(tc.BlobstoreBase.storeDirectory)
}
