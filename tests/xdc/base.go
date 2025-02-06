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
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
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

const (
	namespaceCacheWaitTime      = 2 * testcore.NamespaceCacheRefreshInterval
	namespaceCacheCheckInterval = testcore.NamespaceCacheRefreshInterval / 2
	replicationWaitTime         = 15 * time.Second
	replicationCheckInterval    = 500 * time.Millisecond

	testTimeout = 30 * time.Second
)

type (
	xdcBaseSuite struct {
		// TODO (alex): use FunctionalTestSuite
		suite.Suite
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		protorequire.ProtoAssertions
		historyrequire.HistoryRequire

		// TODO (alex): replace cluster1 and cluster2 with a slice of clusters.
		cluster1               *testcore.TestCluster
		cluster2               *testcore.TestCluster
		logger                 log.Logger
		dynamicConfigOverrides map[dynamicconfig.Key]interface{}

		startTime          time.Time
		onceClusterConnect sync.Once

		enableTransitionHistory bool
	}
)

// TODO (alex): this should be gone.
func (s *xdcBaseSuite) clusterReplicationConfig() []*replicationpb.ClusterReplicationConfig {
	config := make([]*replicationpb.ClusterReplicationConfig, 2)
	for i, cluster := range []*testcore.TestCluster{s.cluster1, s.cluster2} {
		config[i] = &replicationpb.ClusterReplicationConfig{
			ClusterName: cluster.ClusterName(),
		}
	}
	return config
}

func (s *xdcBaseSuite) setupSuite(opts ...testcore.TestClusterOption) {

	params := testcore.ApplyTestClusterOptions(opts)

	suffix := common.GenerateRandomString(5)
	clusterNames := []string{
		"active_" + suffix,
		"standby_" + suffix,
	}

	if s.logger == nil {
		s.logger = log.NewTestLogger()
	}
	if s.dynamicConfigOverrides == nil {
		s.dynamicConfigOverrides = make(map[dynamicconfig.Key]interface{})
	}
	s.dynamicConfigOverrides[dynamicconfig.ClusterMetadataRefreshInterval.Key()] = time.Second * 5
	s.dynamicConfigOverrides[dynamicconfig.NamespaceCacheRefreshInterval.Key()] = testcore.NamespaceCacheRefreshInterval
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
		clusterConfigs[i].ClusterMetadata.MasterClusterName = clusterNames[i]
		clusterConfigs[i].ClusterMetadata.CurrentClusterName = clusterNames[i]
		clusterConfigs[i].ClusterMetadata.EnableGlobalNamespace = true
		clusterConfigs[i].Persistence.DBName = "func_tests_" + clusterNames[i]
		clusterConfigs[i].ClusterMetadata.ClusterInformation = map[string]cluster.ClusterInformation{
			clusterNames[i]: cluster.ClusterInformation{
				Enabled:                true,
				InitialFailoverVersion: int64(i + 1),
				// RPCAddress and HTTPAddress will be filled in
			},
		}
		clusterConfigs[i].ServiceFxOptions = params.ServiceOptions
		clusterConfigs[i].EnableMetricsCapture = true
	}

	testClusterFactory := testcore.NewTestClusterFactory()
	s.cluster1, err = testClusterFactory.NewCluster(s.T(), clusterConfigs[0], log.With(s.logger, tag.ClusterName(clusterNames[0])))
	s.Require().NoError(err)

	s.cluster2, err = testClusterFactory.NewCluster(s.T(), clusterConfigs[1], log.With(s.logger, tag.ClusterName(clusterNames[1])))
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
	waitForClusterConnected(s.Assertions, s.logger, s.cluster1, s.cluster1.ClusterName(), s.cluster2.ClusterName(), s.startTime)
	waitForClusterConnected(s.Assertions, s.logger, s.cluster2, s.cluster1.ClusterName(), s.cluster2.ClusterName(), s.startTime)
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
	return s.createNamespace(true, []*testcore.TestCluster{s.cluster1, s.cluster2})
}

// TODO (alex): rename this to createLocalNamespace, and everywhere where it is called with isGlobal == true, add call to promoteNamespace.
func (s *xdcBaseSuite) createNamespaceInCluster0(isGlobal bool) string {
	return s.createNamespace(isGlobal, []*testcore.TestCluster{s.cluster1})
}

func (s *xdcBaseSuite) createNamespace(
	isGlobal bool,
	clusters []*testcore.TestCluster,
) string {
	ctx := testcore.NewContext()
	ns := "test-namespace-" + uuid.NewString()
	var replicationConfigs []*replicationpb.ClusterReplicationConfig
	if isGlobal {
		replicationConfigs = make([]*replicationpb.ClusterReplicationConfig, len(clusters))
		for i, cluster := range clusters {
			replicationConfigs[i] = &replicationpb.ClusterReplicationConfig{
				ClusterName: cluster.ClusterName(),
			}
		}
	}

	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        ns,
		IsGlobalNamespace:                isGlobal,
		Clusters:                         replicationConfigs,
		ActiveClusterName:                clusters[0].ClusterName(), // cluster 0 is always active.
		WorkflowExecutionRetentionPeriod: durationpb.New(7 * time.Hour * 24),
	}
	// namespace is always created in cluster 0.
	_, err := clusters[0].FrontendClient().RegisterNamespace(ctx, regReq)
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		for _, r := range clusters[0].Host().NamespaceRegistries() {
			resp, err := r.GetNamespace(namespace.Name(ns))
			assert.NoError(t, err)
			if assert.NotNil(t, resp) {
				assert.Equal(t, isGlobal, resp.IsGlobalNamespace())
			}
		}
	}, namespaceCacheWaitTime, namespaceCacheCheckInterval)

	if len(clusters) > 1 && isGlobal {
		// If namespace is global and config has more than 1 cluster, it should be replicated to these other clusters.
		// Check other clusters too.
		s.EventuallyWithT(func(t *assert.CollectT) {
			for _, c := range clusters[1:] {
				for _, r := range c.Host().NamespaceRegistries() {
					resp, err := r.GetNamespace(namespace.Name(ns))
					assert.NoError(t, err)
					if assert.NotNil(t, resp) {
						assert.Equal(t, isGlobal, resp.IsGlobalNamespace())
						var clusterNames []string
						for _, cl := range clusters {
							clusterNames = append(clusterNames, cl.ClusterName())
						}
						assert.Equal(t, clusterNames, resp.ClusterNames())
					}
				}
			}
		}, replicationWaitTime, replicationCheckInterval)
	}

	return ns
}

func updateNamespaceConfig(
	s *require.Assertions,
	ns string,
	newConfigFn func() *namespacepb.NamespaceConfig,
	clusters []*testcore.TestCluster,
	inClusterIndex int,
) {

	configVersion := int64(-1)
	s.EventuallyWithT(func(t *assert.CollectT) {
		for _, r := range clusters[inClusterIndex].Host().NamespaceRegistries() {
			// TODO(alex): here and everywere else in this file: instead of waiting for registry to be updated
			// r.RefreshNamespaceById() can be used. It will require to pass nsID everywhere.
			resp, err := r.GetNamespace(namespace.Name(ns))
			assert.NoError(t, err)
			if assert.NotNil(t, resp) {
				if configVersion == -1 {
					configVersion = resp.ConfigVersion()
				}
				assert.Equal(t, configVersion, resp.ConfigVersion(), "config version must be the same for all namespace registries")
			}
		}
	}, namespaceCacheWaitTime, namespaceCacheCheckInterval)
	s.NotEqual(int64(-1), configVersion)

	updateReq := &workflowservice.UpdateNamespaceRequest{
		Namespace: ns,
		Config:    newConfigFn(),
	}
	_, err := clusters[inClusterIndex].FrontendClient().UpdateNamespace(testcore.NewContext(), updateReq)
	s.NoError(err)

	// TODO (alex): This leaks implementation details of UpdateNamespace.
	// Consider returning configVersion in response or using persistence directly.
	configVersion++

	s.EventuallyWithT(func(t *assert.CollectT) {
		for _, r := range clusters[inClusterIndex].Host().NamespaceRegistries() {
			resp, err := r.GetNamespace(namespace.Name(ns))
			assert.NoError(t, err)
			if assert.NotNil(t, resp) {
				assert.Equal(t, configVersion, resp.ConfigVersion())
			}
		}
	}, namespaceCacheWaitTime, namespaceCacheCheckInterval)

	if len(clusters) > 1 {
		// check remote ns too
		s.EventuallyWithT(func(t *assert.CollectT) {
			for ci, c := range clusters {
				if ci == inClusterIndex {
					continue
				}
				for _, r := range c.Host().NamespaceRegistries() {
					resp, err := r.GetNamespace(namespace.Name(ns))
					assert.NoError(t, err)
					if assert.NotNil(t, resp) {
						assert.Equal(t, configVersion, resp.ConfigVersion())
					}
				}
			}
		}, replicationWaitTime, replicationCheckInterval)
	}
}

// TODO (alex): change cluster1 and cluster2 to clusters slice and remove this method.
func (s *xdcBaseSuite) clusterAt(i int) *testcore.TestCluster {
	if i == 0 {
		return s.cluster1
	}
	return s.cluster2
}

func (s *xdcBaseSuite) updateNamespaceClusters(
	ns string,
	inClusterIndex int,
	clusters []*testcore.TestCluster,
) {

	replicationConfigs := make([]*replicationpb.ClusterReplicationConfig, len(clusters))
	for i, cluster := range clusters {
		replicationConfigs[i] = &replicationpb.ClusterReplicationConfig{
			ClusterName: cluster.ClusterName(),
		}
	}

	_, err := clusters[inClusterIndex].FrontendClient().UpdateNamespace(testcore.NewContext(), &workflowservice.UpdateNamespaceRequest{
		Namespace: ns,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			Clusters: replicationConfigs,
		}})
	s.NoError(err)

	var clusterNames []string
	for _, cl := range clusters {
		clusterNames = append(clusterNames, cl.ClusterName())
	}

	var isGlobalNamespace bool
	s.EventuallyWithT(func(t *assert.CollectT) {
		for _, r := range clusters[inClusterIndex].Host().NamespaceRegistries() {
			resp, err := r.GetNamespace(namespace.Name(ns))
			assert.NoError(t, err)
			if assert.NotNil(t, resp) {
				assert.Equal(t, clusterNames, resp.ClusterNames())
				isGlobalNamespace = resp.IsGlobalNamespace()
			}
		}
	}, namespaceCacheWaitTime, namespaceCacheCheckInterval)

	if len(clusters) > 1 && isGlobalNamespace {
		// If namespace is global and config has more than 1 cluster, it should be replicated to these other clusters.
		// Check other clusters too.
		s.EventuallyWithT(func(t *assert.CollectT) {
			for ci, c := range clusters {
				if ci == inClusterIndex {
					continue
				}
				for _, r := range c.Host().NamespaceRegistries() {
					resp, err := r.GetNamespace(namespace.Name(ns))
					assert.NoError(t, err)
					if assert.NotNil(t, resp) {
						assert.Equal(t, clusterNames, resp.ClusterNames())
					}
				}
			}
		}, replicationWaitTime, replicationCheckInterval)
	}
}

func (s *xdcBaseSuite) promoteNamespace(
	ns string,
	inClusterIndex int,
) {

	_, err := s.clusterAt(inClusterIndex).FrontendClient().UpdateNamespace(testcore.NewContext(), &workflowservice.UpdateNamespaceRequest{
		Namespace:        ns,
		PromoteNamespace: true,
	})
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		for _, r := range s.clusterAt(inClusterIndex).Host().NamespaceRegistries() {
			resp, err := r.GetNamespace(namespace.Name(ns))
			assert.NoError(t, err)
			if assert.NotNil(t, resp) {
				assert.True(t, resp.IsGlobalNamespace())
			}
		}
	}, namespaceCacheWaitTime, namespaceCacheCheckInterval)
}

func (s *xdcBaseSuite) failover(
	ns string,
	inClusterIndex int,
	targetCluster string,
	targetFailoverVersion int64,
) {
	s.waitForClusterSynced()

	// update namespace to fail over
	updateReq := &workflowservice.UpdateNamespaceRequest{
		Namespace: ns,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: targetCluster,
		},
	}
	updateResp, err := s.clusterAt(inClusterIndex).FrontendClient().UpdateNamespace(testcore.NewContext(), updateReq)
	s.NoError(err)
	// TODO (alex): not clear why it matters.
	s.Equal(targetFailoverVersion, updateResp.GetFailoverVersion())

	// check local and remote clusters
	s.EventuallyWithT(func(t *assert.CollectT) {
		for _, c := range []*testcore.TestCluster{s.cluster1, s.cluster2} {
			for _, r := range c.Host().NamespaceRegistries() {
				resp, err := r.GetNamespace(namespace.Name(ns))
				assert.NoError(t, err)
				if assert.NotNil(t, resp) {
					assert.Equal(t, targetCluster, resp.ActiveClusterName())
				}
			}
		}
	}, replicationWaitTime, replicationCheckInterval)

	s.waitForClusterSynced()
}

func (s *xdcBaseSuite) mustToPayload(v any) *commonpb.Payload {
	conv := converter.GetDefaultDataConverter()
	payload, err := conv.ToPayload(v)
	s.NoError(err)
	return payload
}
