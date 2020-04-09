package persistence

// GetOrUseDefaultActiveCluster return the current cluster name or use the input if valid
func GetOrUseDefaultActiveCluster(currentClusterName string, activeClusterName string) string {
	if len(activeClusterName) == 0 {
		return currentClusterName
	}
	return activeClusterName
}

// GetOrUseDefaultClusters return the current cluster or use the input if valid
func GetOrUseDefaultClusters(currentClusterName string, clusters []*ClusterReplicationConfig) []*ClusterReplicationConfig {
	if len(clusters) == 0 {
		return []*ClusterReplicationConfig{
			&ClusterReplicationConfig{
				ClusterName: currentClusterName,
			},
		}
	}
	return clusters
}
