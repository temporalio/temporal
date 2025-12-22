package xdc

import (
	"cmp"
	"context"
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
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	sdkworker "go.temporal.io/sdk/worker"
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
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
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
		// TODO (alex): use FunctionalTestBase instead.
		suite.Suite
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		protorequire.ProtoAssertions
		historyrequire.HistoryRequire

		clusters               []*testcore.TestCluster
		logger                 log.Logger
		dynamicConfigOverrides map[dynamicconfig.Key]interface{}

		startTime          time.Time
		onceClusterConnect sync.Once

		enableTransitionHistory bool

		// TODO: add sdkClient and worker here and remove its creation in many tests.
	}
)

// TODO (alex): this should be gone.
func (s *xdcBaseSuite) clusterReplicationConfig() []*replicationpb.ClusterReplicationConfig {
	config := make([]*replicationpb.ClusterReplicationConfig, 2)
	for ci, c := range s.clusters {
		config[ci] = &replicationpb.ClusterReplicationConfig{
			ClusterName: c.ClusterName(),
		}
	}
	return config
}

func (s *xdcBaseSuite) setupSuite(opts ...testcore.TestClusterOption) {

	params := testcore.ApplyTestClusterOptions(opts)

	if s.logger == nil {
		s.logger = log.NewTestLogger()
	}
	if s.dynamicConfigOverrides == nil {
		s.dynamicConfigOverrides = make(map[dynamicconfig.Key]interface{})
	}
	s.dynamicConfigOverrides[dynamicconfig.ClusterMetadataRefreshInterval.Key()] = time.Second * 5
	s.dynamicConfigOverrides[dynamicconfig.NamespaceCacheRefreshInterval.Key()] = testcore.NamespaceCacheRefreshInterval
	s.dynamicConfigOverrides[dynamicconfig.EnableTransitionHistory.Key()] = s.enableTransitionHistory
	// TODO (prathyush): remove this after setting it to true by default.
	s.dynamicConfigOverrides[dynamicconfig.SendRawHistoryBetweenInternalServices.Key()] = true
	// Override checkpoint intervals to 3 seconds for faster testing
	s.dynamicConfigOverrides[dynamicconfig.TransferProcessorUpdateAckInterval.Key()] = time.Second * 3
	s.dynamicConfigOverrides[dynamicconfig.TimerProcessorUpdateAckInterval.Key()] = time.Second * 3
	s.dynamicConfigOverrides[dynamicconfig.VisibilityProcessorUpdateAckInterval.Key()] = time.Second * 3
	s.dynamicConfigOverrides[dynamicconfig.OutboundProcessorUpdateAckInterval.Key()] = time.Second * 3
	s.dynamicConfigOverrides[dynamicconfig.ArchivalProcessorUpdateAckInterval.Key()] = time.Second * 3
	// Override max poll intervals to 3 seconds for faster task discovery in tests
	s.dynamicConfigOverrides[dynamicconfig.TransferProcessorMaxPollInterval.Key()] = time.Second * 3
	s.dynamicConfigOverrides[dynamicconfig.TimerProcessorMaxPollInterval.Key()] = time.Second * 3
	s.dynamicConfigOverrides[dynamicconfig.VisibilityProcessorMaxPollInterval.Key()] = time.Second * 3
	s.dynamicConfigOverrides[dynamicconfig.OutboundProcessorMaxPollInterval.Key()] = time.Second * 3

	clusterConfigs := []*testcore.TestClusterConfig{
		{
			ClusterMetadata: cluster.Config{
				EnableGlobalNamespace:    true,
				FailoverVersionIncrement: 10,
			},
			HistoryConfig: testcore.HistoryConfig{
				NumHistoryShards: cmp.Or(params.NumHistoryShards, 1),
			},
		},
		{
			ClusterMetadata: cluster.Config{
				EnableGlobalNamespace:    true,
				FailoverVersionIncrement: 10,
			},
			HistoryConfig: testcore.HistoryConfig{
				NumHistoryShards: cmp.Or(params.NumHistoryShards, 1),
			},
		},
	}

	s.clusters = make([]*testcore.TestCluster, len(clusterConfigs))
	suffix := common.GenerateRandomString(5)

	testClusterFactory := testcore.NewTestClusterFactory()
	for clusterIndex, clusterName := range []string{"active_" + suffix, "standby_" + suffix} {
		clusterConfigs[clusterIndex].DynamicConfigOverrides = s.dynamicConfigOverrides
		clusterConfigs[clusterIndex].ClusterMetadata.MasterClusterName = clusterName
		clusterConfigs[clusterIndex].ClusterMetadata.CurrentClusterName = clusterName
		clusterConfigs[clusterIndex].ClusterMetadata.EnableGlobalNamespace = true
		clusterConfigs[clusterIndex].Persistence.DBName = "func_tests_" + clusterName
		clusterConfigs[clusterIndex].ClusterMetadata.ClusterInformation = map[string]cluster.ClusterInformation{
			clusterName: {
				Enabled:                true,
				InitialFailoverVersion: int64(clusterIndex + 1),
				// RPCAddress and HTTPAddress will be filled in
			},
		}
		clusterConfigs[clusterIndex].ServiceFxOptions = params.ServiceOptions
		clusterConfigs[clusterIndex].EnableMetricsCapture = true

		var err error
		s.clusters[clusterIndex], err = testClusterFactory.NewCluster(s.T(), clusterConfigs[clusterIndex], log.With(s.logger, tag.ClusterName(clusterName)))
		s.Require().NoError(err)
	}

	s.startTime = time.Now()

	for ci, c := range s.clusters {
		for remoteCi, remoteC := range s.clusters {
			if ci != remoteCi {
				_, err := c.AdminClient().AddOrUpdateRemoteCluster(
					testcore.NewContext(),
					&adminservice.AddOrUpdateRemoteClusterRequest{
						FrontendAddress:               remoteC.Host().RemoteFrontendGRPCAddress(),
						FrontendHttpAddress:           remoteC.Host().FrontendHTTPAddress(),
						EnableRemoteClusterConnection: true,
						EnableReplication:             true,
					})
				s.Require().NoError(err)
			}
		}
	}
	// TODO (alex): This looks suspicious. Why 200ms?
	// Wait for cluster metadata to refresh new added clusters
	time.Sleep(time.Millisecond * 200)
}

func (s *xdcBaseSuite) waitForClusterConnected(
	sourceCluster *testcore.TestCluster,
	targetClusterName string,
) {
	s.logger.Info("wait for clusters to be synced", tag.SourceCluster(sourceCluster.ClusterName()), tag.TargetCluster(targetClusterName))
	s.EventuallyWithT(func(c *assert.CollectT) {
		s.logger.Info("check if clusters are synced", tag.SourceCluster(sourceCluster.ClusterName()), tag.TargetCluster(targetClusterName))
		resp, err := sourceCluster.HistoryClient().GetReplicationStatus(context.Background(), &historyservice.GetReplicationStatusRequest{})
		require.NoError(c, err)
		require.Lenf(c, resp.Shards, 1, "test cluster has only one history shard")

		shard := resp.Shards[0]
		require.NotNil(c, shard)
		require.Greater(c, shard.MaxReplicationTaskId, int64(0))
		require.NotNil(c, shard.ShardLocalTime)
		require.WithinRange(c, shard.ShardLocalTime.AsTime(), s.startTime, time.Now())
		require.NotNil(c, shard.RemoteClusters)

		standbyAckInfo, ok := shard.RemoteClusters[targetClusterName]
		require.True(c, ok)
		require.NotNil(c, standbyAckInfo)
		require.LessOrEqual(c, shard.MaxReplicationTaskId, standbyAckInfo.AckedTaskId)
		require.NotNil(c, standbyAckInfo.AckedTaskVisibilityTime)
		require.WithinRange(c, standbyAckInfo.AckedTaskVisibilityTime.AsTime(), s.startTime, time.Now())
	}, 90*time.Second, 1*time.Second)
	s.logger.Info("clusters synced", tag.SourceCluster(sourceCluster.ClusterName()), tag.TargetCluster(targetClusterName))
}

func (s *xdcBaseSuite) tearDownSuite() {
	for _, c := range s.clusters {
		s.NoError(c.TearDownCluster())
	}
}

func (s *xdcBaseSuite) waitForClusterSynced() {
	for sourceClusterI, sourceCluster := range s.clusters {
		for targetClusterI, targetCluster := range s.clusters {
			if sourceClusterI != targetClusterI {
				s.waitForClusterConnected(sourceCluster, targetCluster.ClusterName())
			}
		}
	}
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
	return s.createNamespace(true, s.clusters)
}

// TODO (alex): rename this to createLocalNamespace, and everywhere where it is called with isGlobal == true, add call to promoteNamespace.
func (s *xdcBaseSuite) createNamespaceInCluster0(isGlobal bool) string {
	return s.createNamespace(isGlobal, s.clusters[:1])
}

func (s *xdcBaseSuite) createNamespace(
	isGlobal bool,
	clusters []*testcore.TestCluster,
) string {
	ctx := testcore.NewContext()
	ns := "test-namespace-" + uuid.NewString()
	var replicationConfigs []*replicationpb.ClusterReplicationConfig
	var clusterNames []string
	if isGlobal {
		replicationConfigs = make([]*replicationpb.ClusterReplicationConfig, len(clusters))
		clusterNames = make([]string, len(clusters))
		for ci, c := range clusters {
			replicationConfigs[ci] = &replicationpb.ClusterReplicationConfig{ClusterName: c.ClusterName()}
			clusterNames[ci] = c.ClusterName()
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
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Equal(t, isGlobal, resp.IsGlobalNamespace())
		}
	}, namespaceCacheWaitTime, namespaceCacheCheckInterval)

	if len(clusters) > 1 && isGlobal {
		// If namespace is global and config has more than 1 cluster, it should be replicated to these other clusters.
		// Check other clusters too.
		s.EventuallyWithT(func(t *assert.CollectT) {
			for _, c := range clusters[1:] {
				for _, r := range c.Host().NamespaceRegistries() {
					resp, err := r.GetNamespace(namespace.Name(ns))
					require.NoError(t, err)
					require.NotNil(t, resp)
					require.Equal(t, isGlobal, resp.IsGlobalNamespace())
					require.Equal(t, clusterNames, resp.ClusterNames(namespace.EmptyBusinessID))
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
			require.NoError(t, err)
			require.NotNil(t, resp)
			if configVersion == -1 {
				configVersion = resp.ConfigVersion()
			}
			require.Equal(t, configVersion, resp.ConfigVersion(), "config version must be the same for all namespace registries")
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
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Equal(t, configVersion, resp.ConfigVersion())
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
					require.NoError(t, err)
					require.NotNil(t, resp)
					require.Equal(t, configVersion, resp.ConfigVersion())
				}
			}
		}, replicationWaitTime, replicationCheckInterval)
	}
}

func (s *xdcBaseSuite) updateNamespaceClusters(
	ns string,
	inClusterIndex int,
	clusters []*testcore.TestCluster,
) {

	replicationConfigs := make([]*replicationpb.ClusterReplicationConfig, len(clusters))
	clusterNames := make([]string, len(clusters))
	for ci, c := range clusters {
		replicationConfigs[ci] = &replicationpb.ClusterReplicationConfig{ClusterName: c.ClusterName()}
		clusterNames[ci] = c.ClusterName()
	}

	_, err := clusters[inClusterIndex].FrontendClient().UpdateNamespace(testcore.NewContext(), &workflowservice.UpdateNamespaceRequest{
		Namespace: ns,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			Clusters: replicationConfigs,
		}})
	s.NoError(err)

	var isGlobalNamespace bool
	s.EventuallyWithT(func(t *assert.CollectT) {
		for _, r := range clusters[inClusterIndex].Host().NamespaceRegistries() {
			resp, err := r.GetNamespace(namespace.Name(ns))
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Equal(t, clusterNames, resp.ClusterNames(namespace.EmptyBusinessID))
			isGlobalNamespace = resp.IsGlobalNamespace()
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
					require.NoError(t, err)
					require.NotNil(t, resp)
					require.Equal(t, clusterNames, resp.ClusterNames(namespace.EmptyBusinessID))
				}
			}
		}, replicationWaitTime, replicationCheckInterval)
	}
}

func (s *xdcBaseSuite) promoteNamespace(
	ns string,
	inClusterIndex int,
) {

	_, err := s.clusters[inClusterIndex].FrontendClient().UpdateNamespace(testcore.NewContext(), &workflowservice.UpdateNamespaceRequest{
		Namespace:        ns,
		PromoteNamespace: true,
	})
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		for _, r := range s.clusters[inClusterIndex].Host().NamespaceRegistries() {
			resp, err := r.GetNamespace(namespace.Name(ns))
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.True(t, resp.IsGlobalNamespace())
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
	updateResp, err := s.clusters[inClusterIndex].FrontendClient().UpdateNamespace(testcore.NewContext(), updateReq)
	s.NoError(err)
	// TODO (alex): not clear why it matters.
	s.Equal(targetFailoverVersion, updateResp.GetFailoverVersion())

	// check local and remote clusters
	s.EventuallyWithT(func(t *assert.CollectT) {
		for _, c := range s.clusters {
			for _, r := range c.Host().NamespaceRegistries() {
				resp, err := r.GetNamespace(namespace.Name(ns))
				require.NoError(t, err)
				require.NotNil(t, resp)
				require.Equal(t, targetCluster, resp.ActiveClusterName(namespace.EmptyBusinessID))
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

func (s *xdcBaseSuite) newClientAndWorker(hostport, ns, taskqueue, identity string) (sdkclient.Client, sdkworker.Worker) {
	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  hostport,
		Namespace: ns,
	})
	s.NoError(err)

	worker := sdkworker.New(sdkClient, taskqueue, sdkworker.Options{
		Identity: identity,
	})

	return sdkClient, worker
}
