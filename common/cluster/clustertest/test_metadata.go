package clustertest

import (
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
)

// NewMetadataForTest returns a new [cluster.Metadata] instance for testing.
func NewMetadataForTest(
	config *cluster.Config,
) cluster.Metadata {
	return cluster.NewMetadata(
		config.EnableGlobalNamespace,
		config.FailoverVersionIncrement,
		config.MasterClusterName,
		config.CurrentClusterName,
		config.ClusterInformation,
		nil,
		nil,
		log.NewNoopLogger(),
	)
}
