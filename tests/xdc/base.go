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

package xdc

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/historyrequire"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/environment"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"gopkg.in/yaml.v3"
)

type (
	xdcBaseSuite struct {
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		protorequire.ProtoAssertions
		historyrequire.HistoryRequire
		clusterNames []string
		suite.Suite

		testClusterFactory testcore.TestClusterFactory

		cluster1               *testcore.TestCluster
		cluster2               *testcore.TestCluster
		logger                 log.Logger
		dynamicConfigOverrides map[dynamicconfig.Key]interface{}

		startTime          time.Time
		onceClusterConnect sync.Once

		enableTransitionHistory bool
	}
)

func (s *xdcBaseSuite) clusterReplicationConfig() []*replicationpb.ClusterReplicationConfig {
	config := make([]*replicationpb.ClusterReplicationConfig, len(s.clusterNames))
	for i, clusterName := range s.clusterNames {
		config[i] = &replicationpb.ClusterReplicationConfig{
			ClusterName: clusterName,
		}
	}
	return config
}

func (s *xdcBaseSuite) setupSuite(clusterNames []string, opts ...testcore.Option) {
	s.testClusterFactory = testcore.NewTestClusterFactory()

	params := testcore.ApplyTestClusterParams(opts)

	s.clusterNames = clusterNames
	if s.logger == nil {
		s.logger = log.NewTestLogger()
	}
	if s.dynamicConfigOverrides == nil {
		s.dynamicConfigOverrides = make(map[dynamicconfig.Key]interface{})
	}
	s.dynamicConfigOverrides[dynamicconfig.ClusterMetadataRefreshInterval.Key()] = time.Second * 5
	s.dynamicConfigOverrides[dynamicconfig.EnableTransitionHistory.Key()] = s.enableTransitionHistory

	fileName := "../testdata/xdc_clusters.yaml"
	if testcore.TestFlags.TestClusterConfigFile != "" {
		fileName = testcore.TestFlags.TestClusterConfigFile
	}
	environment.SetupEnv()

	confContent, err := os.ReadFile(fileName)
	s.Require().NoError(err)
	confContent = []byte(os.ExpandEnv(string(confContent)))

	var clusterConfigs []*testcore.TestClusterConfig
	s.Require().NoError(yaml.Unmarshal(confContent, &clusterConfigs))
	for i, config := range clusterConfigs {
		config.DynamicConfigOverrides = s.dynamicConfigOverrides
		clusterConfigs[i].ClusterMetadata.MasterClusterName = s.clusterNames[i]
		clusterConfigs[i].ClusterMetadata.CurrentClusterName = s.clusterNames[i]
		clusterConfigs[i].Persistence.DBName = "func_" + s.clusterNames[i]
		clusterConfigs[i].ClusterMetadata.ClusterInformation = map[string]cluster.ClusterInformation{
			s.clusterNames[i]: cluster.ClusterInformation{
				Enabled:                true,
				InitialFailoverVersion: int64(i + 1),
				// RPCAddress and HTTPAddress will be filled in
			},
		}
		clusterConfigs[i].ServiceFxOptions = params.ServiceOptions
		clusterConfigs[i].EnableMetricsCapture = true
	}

	s.cluster1, err = s.testClusterFactory.NewCluster(s.T(), clusterConfigs[0], log.With(s.logger, tag.ClusterName(s.clusterNames[0])))
	s.Require().NoError(err)

	s.cluster2, err = s.testClusterFactory.NewCluster(s.T(), clusterConfigs[1], log.With(s.logger, tag.ClusterName(s.clusterNames[1])))
	s.Require().NoError(err)

	s.startTime = time.Now()

	_, err = s.cluster1.AdminClient().AddOrUpdateRemoteCluster(
		testcore.NewContext(),
		&adminservice.AddOrUpdateRemoteClusterRequest{
			FrontendAddress:               s.cluster2.Host().RemoteFrontendGRPCAddress(),
			FrontendHttpAddress:           s.cluster2.Host().FrontendHTTPAddress(),
			EnableRemoteClusterConnection: true,
		})
	s.Require().NoError(err)

	_, err = s.cluster2.AdminClient().AddOrUpdateRemoteCluster(
		testcore.NewContext(),
		&adminservice.AddOrUpdateRemoteClusterRequest{
			FrontendAddress:               s.cluster1.Host().RemoteFrontendGRPCAddress(),
			FrontendHttpAddress:           s.cluster1.Host().FrontendHTTPAddress(),
			EnableRemoteClusterConnection: true,
		})
	s.Require().NoError(err)
	// Wait for cluster metadata to refresh new added clusters
	time.Sleep(time.Millisecond * 200)
}

func waitForClusterConnected(
	s *require.Assertions,
	logger log.Logger,
	sourceCluster *testcore.TestCluster,
	source string,
	target string,
	startTime time.Time,
) {
	logger.Info("wait for clusters to be synced", tag.SourceCluster(source), tag.TargetCluster(target))
	s.EventuallyWithT(func(c *assert.CollectT) {
		logger.Info("check if clusters are synced", tag.SourceCluster(source), tag.TargetCluster(target))
		resp, err := sourceCluster.HistoryClient().GetReplicationStatus(context.Background(), &historyservice.GetReplicationStatusRequest{})
		if !assert.NoError(c, err) {
			return
		}
		assert.Lenf(c, resp.Shards, 1, "test cluster has only one history shard")

		shard := resp.Shards[0]
		if !assert.NotNil(c, shard) {
			return
		}
		assert.Greater(c, shard.MaxReplicationTaskId, int64(0))
		assert.NotNil(c, shard.ShardLocalTime)
		assert.WithinRange(c, shard.ShardLocalTime.AsTime(), startTime, time.Now())
		assert.NotNil(c, shard.RemoteClusters)

		standbyAckInfo, ok := shard.RemoteClusters[target]
		if !assert.True(c, ok) || !assert.NotNil(c, standbyAckInfo) {
			return
		}
		assert.LessOrEqual(c, shard.MaxReplicationTaskId, standbyAckInfo.AckedTaskId)
		assert.NotNil(c, standbyAckInfo.AckedTaskVisibilityTime)
		assert.WithinRange(c, standbyAckInfo.AckedTaskVisibilityTime.AsTime(), startTime, time.Now())
	}, 90*time.Second, 1*time.Second)
	logger.Info("clusters synced", tag.SourceCluster(source), tag.TargetCluster(target))
}

func (s *xdcBaseSuite) tearDownSuite() {
	s.NoError(s.cluster1.TearDownCluster())
	s.NoError(s.cluster2.TearDownCluster())
}

func (s *xdcBaseSuite) waitForClusterSynced() {
	waitForClusterConnected(s.Assertions, s.logger, s.cluster1, s.clusterNames[0], s.clusterNames[1], s.startTime)
	waitForClusterConnected(s.Assertions, s.logger, s.cluster2, s.clusterNames[1], s.clusterNames[0], s.startTime)
}

func (s *xdcBaseSuite) setupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
	s.ProtoAssertions = protorequire.New(s.T())
	s.HistoryRequire = historyrequire.New(s.T())

	s.onceClusterConnect.Do(func() {
		s.waitForClusterSynced()
	})
}

func (s *xdcBaseSuite) createGlobalNamespace() string {
	ctx := testcore.NewContext()
	ns := "test-namespace-" + uuid.NewString()

	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        ns,
		IsGlobalNamespace:                true,
		Clusters:                         s.clusterReplicationConfig(),
		ActiveClusterName:                s.clusterNames[0],
		WorkflowExecutionRetentionPeriod: durationpb.New(7 * time.Hour * 24),
	}
	_, err := s.cluster1.FrontendClient().RegisterNamespace(ctx, regReq)
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		// Wait for namespace record to be replicated and loaded into memory.
		for _, r := range s.cluster2.Host().FrontendNamespaceRegistries() {
			_, err := r.GetNamespace(namespace.Name(ns))
			assert.NoError(t, err)
		}
	}, 15*time.Second, 500*time.Millisecond)

	return ns
}

func (s *xdcBaseSuite) failover(
	namespace string,
	targetCluster string,
	targetFailoverVersion int64,
	client workflowservice.WorkflowServiceClient,
) {
	// wait for replication task propagation
	time.Sleep(4 * time.Second)

	// update namespace to fail over
	updateReq := &workflowservice.UpdateNamespaceRequest{
		Namespace: namespace,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: targetCluster,
		},
	}
	updateResp, err := client.UpdateNamespace(testcore.NewContext(), updateReq)
	s.NoError(err)
	s.Equal(targetCluster, updateResp.ReplicationConfig.GetActiveClusterName())
	s.Equal(targetFailoverVersion, updateResp.GetFailoverVersion())

	// wait till failover completed
	time.Sleep(cacheRefreshInterval)
}

func (s *xdcBaseSuite) mustToPayload(v any) *commonpb.Payload {
	conv := converter.GetDefaultDataConverter()
	payload, err := conv.ToPayload(v)
	s.NoError(err)
	return payload
}
