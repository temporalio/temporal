package xdc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/tests/testcore"
)

// TestStartTwoClustersForever is a helper test that starts two Temporal clusters
// and keeps them running indefinitely for manual testing and debugging.
// This test is skipped by default. Comment out the t.Skip() line to run it.
func TestStartTwoClustersForever(t *testing.T) {
	// t.Skip("This test runs indefinitely. Comment out this line to run it.")
	logger := log.NewTestLogger()

	// Dynamic config overrides
	dynamicConfigOverrides := map[dynamicconfig.Key]interface{}{
		dynamicconfig.ClusterMetadataRefreshInterval.Key():            time.Second * 5,
		dynamicconfig.NamespaceCacheRefreshInterval.Key():             testcore.NamespaceCacheRefreshInterval,
		dynamicconfig.EnableReplicationStream.Key():                   true,
		dynamicconfig.EnableReplicationTaskBatching.Key():             true,
		dynamicconfig.EnableWorkflowTaskStampIncrementOnFailure.Key(): true,
		dynamicconfig.SendRawHistoryBetweenInternalServices.Key():     true,
		dynamicconfig.TransferProcessorUpdateAckInterval.Key():        time.Second * 3,
		dynamicconfig.TimerProcessorUpdateAckInterval.Key():           time.Second * 3,
		dynamicconfig.VisibilityProcessorUpdateAckInterval.Key():      time.Second * 3,
		dynamicconfig.OutboundProcessorUpdateAckInterval.Key():        time.Second * 3,
		dynamicconfig.ArchivalProcessorUpdateAckInterval.Key():        time.Second * 3,
		dynamicconfig.TransferProcessorMaxPollInterval.Key():          time.Second * 3,
		dynamicconfig.TimerProcessorMaxPollInterval.Key():             time.Second * 3,
		dynamicconfig.VisibilityProcessorMaxPollInterval.Key():        time.Second * 3,
		dynamicconfig.OutboundProcessorMaxPollInterval.Key():          time.Second * 3,
	}

	clusterConfigs := []*testcore.TestClusterConfig{
		{
			ClusterMetadata: cluster.Config{
				EnableGlobalNamespace:    true,
				FailoverVersionIncrement: 10,
			},
			HistoryConfig: testcore.HistoryConfig{
				NumHistoryShards: 1,
			},
		},
		{
			ClusterMetadata: cluster.Config{
				EnableGlobalNamespace:    true,
				FailoverVersionIncrement: 10,
			},
			HistoryConfig: testcore.HistoryConfig{
				NumHistoryShards: 1,
			},
		},
	}

	clusters := make([]*testcore.TestCluster, len(clusterConfigs))
	clusterNames := []string{"local-cluster1", "local-cluster2"}

	testClusterFactory := testcore.NewTestClusterFactory()
	for clusterIndex, clusterName := range clusterNames {
		clusterConfigs[clusterIndex].DynamicConfigOverrides = dynamicConfigOverrides
		clusterConfigs[clusterIndex].ClusterMetadata.MasterClusterName = clusterName
		clusterConfigs[clusterIndex].ClusterMetadata.CurrentClusterName = clusterName
		clusterConfigs[clusterIndex].ClusterMetadata.EnableGlobalNamespace = true
		clusterConfigs[clusterIndex].Persistence.DBName = "test_cluster_" + clusterName
		clusterConfigs[clusterIndex].ClusterMetadata.ClusterInformation = map[string]cluster.ClusterInformation{
			clusterName: {
				Enabled:                true,
				InitialFailoverVersion: int64(clusterIndex + 1),
			},
		}
		clusterConfigs[clusterIndex].EnableMetricsCapture = true

		var err error
		clusters[clusterIndex], err = testClusterFactory.NewCluster(t, clusterConfigs[clusterIndex], log.With(logger, tag.ClusterName(clusterName)))
		if err != nil {
			t.Fatalf("Failed to create cluster %s: %v", clusterName, err)
		}
	}

	// Connect clusters to each other
	for ci, c := range clusters {
		for remoteCi, remoteC := range clusters {
			if ci != remoteCi {
				_, err := c.AdminClient().AddOrUpdateRemoteCluster(
					context.Background(),
					&adminservice.AddOrUpdateRemoteClusterRequest{
						FrontendAddress:               remoteC.Host().RemoteFrontendGRPCAddress(),
						FrontendHttpAddress:           remoteC.Host().FrontendHTTPAddress(),
						EnableRemoteClusterConnection: true,
						EnableReplication:             true,
					})
				if err != nil {
					t.Fatalf("Failed to connect clusters: %v", err)
				}
			}
		}
	}

	// Wait for cluster metadata to refresh
	time.Sleep(time.Millisecond * 200) //nolint:forbidigo

	// Output cluster addresses
	fmt.Println("\n=== Temporal Clusters Started Successfully ===")
	for i, c := range clusters {
		fmt.Printf("\nCluster: %s\n", clusterNames[i])
		fmt.Printf("  Frontend Address: %s\n", c.Host().FrontendGRPCAddress())
		fmt.Printf("  Replication Address: %s\n", c.Host().RemoteFrontendGRPCAddress())
		fmt.Printf("  Nexus Address: %s\n", c.Host().FrontendHTTPAddress())
	}
	fmt.Println("\n=== Clusters are running. Press Ctrl+C to stop ===")
	fmt.Println()

	// Sleep forever to keep clusters alive
	select {}
}
