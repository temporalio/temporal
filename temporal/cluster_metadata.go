package temporal

import (
	"fmt"

	"github.com/pborman/uuid"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	persistenceClient "go.temporal.io/server/common/persistence/client"
)

const (
	mismatchLogMessage = "Supplied configuration key/value mismatches persisted cluster metadata. Continuing with the persisted value as this value cannot be changed once initialized."
)

// initClusterMetadata performs a config check against the configured persistence store for cluster metadata.
// If there is a mismatch, the persisted values take precedence and will be written over in the config objects.
// This is to keep this check hidden from independent downstream daemons and keep this in a single place.
func initClusterMetadata(config *config.Config, factory persistenceClient.Factory, logger log.Logger) (cluster.Metadata, error) {
	logger = log.With(logger, tag.ComponentMetadataInitializer)

	clusterMetadataManager, err := factory.NewClusterMetadataManager()
	if err != nil {
		return nil, fmt.Errorf("error initializing cluster metadata manager: %w", err)
	}
	defer clusterMetadataManager.Close()

	applied, err := clusterMetadataManager.SaveClusterMetadata(
		&persistence.SaveClusterMetadataRequest{
			ClusterMetadata: persistencespb.ClusterMetadata{
				HistoryShardCount: config.Persistence.NumHistoryShards,
				ClusterName:       config.ClusterMetadata.CurrentClusterName,
				ClusterId:         uuid.New(),
			}})
	if err != nil {
		logger.Warn(fmt.Sprintf("Failed to save cluster metadata: %v", err))
	}
	if applied {
		logger.Info("Successfully saved cluster metadata.")
	} else {
		resp, err := clusterMetadataManager.GetClusterMetadata()
		if err != nil {
			return nil, fmt.Errorf("error while fetching cluster metadata: %w", err)
		}
		if config.ClusterMetadata.CurrentClusterName != resp.ClusterName {
			logger.Error(
				mismatchLogMessage,
				tag.Key("clusterMetadata.currentClusterName"),
				tag.IgnoredValue(config.ClusterMetadata.CurrentClusterName),
				tag.Value(resp.ClusterName))
			config.ClusterMetadata.CurrentClusterName = resp.ClusterName
		}

		var persistedShardCount = resp.HistoryShardCount
		if config.Persistence.NumHistoryShards != persistedShardCount {
			logger.Error(
				mismatchLogMessage,
				tag.Key("persistence.numHistoryShards"),
				tag.IgnoredValue(config.Persistence.NumHistoryShards),
				tag.Value(persistedShardCount))
			config.Persistence.NumHistoryShards = persistedShardCount
		}
	}

	metadata := cluster.NewMetadata(
		logger,
		config.ClusterMetadata.EnableGlobalNamespace,
		config.ClusterMetadata.FailoverVersionIncrement,
		config.ClusterMetadata.MasterClusterName,
		config.ClusterMetadata.CurrentClusterName,
		config.ClusterMetadata.ClusterInformation,
	)
	return metadata, nil
}
