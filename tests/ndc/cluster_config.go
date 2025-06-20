package ndc

import (
	"fmt"

	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/tests/testcore"
)

func clustersConfig(clusterNames ...string) []*testcore.TestClusterConfig {
	// TODO (alex): I don't know how many of these settings are really needed.

	if len(clusterNames) < 2 {
		//nolint:forbidigo // test code
		panic("at least 2 clusters are needed for ndc tests")
	}

	clusterInformation := make(map[string]cluster.ClusterInformation, len(clusterNames))
	for i, cn := range clusterNames {
		clusterInformation[cn] = cluster.ClusterInformation{
			Enabled:                true,
			InitialFailoverVersion: int64(i + 1),
			RPCAddress:             fmt.Sprintf("nowhere%d:7134", i+1),
		}
	}

	var clusterConfigs []*testcore.TestClusterConfig

	for _, cn := range clusterNames {
		clusterConfig := &testcore.TestClusterConfig{
			ClusterMetadata: cluster.Config{
				EnableGlobalNamespace:    true,
				FailoverVersionIncrement: 10,
				MasterClusterName:        clusterNames[0], // the first one is always master.
				CurrentClusterName:       cn,
				ClusterInformation:       clusterInformation,
			},
			HistoryConfig: testcore.HistoryConfig{
				NumHistoryShards: 1,
			},
		}
		clusterConfigs = append(clusterConfigs, clusterConfig)
	}

	return clusterConfigs
}
