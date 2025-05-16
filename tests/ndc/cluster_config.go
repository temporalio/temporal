package ndc

import (
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/tests/testcore"
)

func clustersConfig() []*testcore.TestClusterConfig {
	// TODO (alex): I don't know how many of these settings are really needed.

	clusterInformation := map[string]cluster.ClusterInformation{
		"cluster-a": {
			Enabled:                true,
			InitialFailoverVersion: 1,
			RPCAddress:             "nowhere1:7134",
		},
		"cluster-b": {
			Enabled:                true,
			InitialFailoverVersion: 2,
			RPCAddress:             "nowhere2:7134",
		},
		"cluster-c": {
			Enabled:                true,
			InitialFailoverVersion: 3,
			RPCAddress:             "nowhere3:7134",
		},
	}

	return []*testcore.TestClusterConfig{
		{
			ClusterMetadata: cluster.Config{
				EnableGlobalNamespace:    true,
				FailoverVersionIncrement: 10,
				MasterClusterName:        "cluster-a",
				CurrentClusterName:       "cluster-a",
				ClusterInformation:       clusterInformation,
			},
			HistoryConfig: testcore.HistoryConfig{
				NumHistoryShards: 1,
			},
		},
		{
			ClusterMetadata: cluster.Config{
				EnableGlobalNamespace:    true,
				FailoverVersionIncrement: 10,
				MasterClusterName:        "cluster-a",
				CurrentClusterName:       "cluster-b",
				ClusterInformation:       clusterInformation,
			},
			HistoryConfig: testcore.HistoryConfig{
				NumHistoryShards: 1,
			},
		},
		{
			ClusterMetadata: cluster.Config{
				EnableGlobalNamespace:    true,
				FailoverVersionIncrement: 10,
				MasterClusterName:        "cluster-a",
				CurrentClusterName:       "cluster-c",
				ClusterInformation:       clusterInformation,
			},
			HistoryConfig: testcore.HistoryConfig{
				NumHistoryShards: 1,
			},
		},
	}
}
