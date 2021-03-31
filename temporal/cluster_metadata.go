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
	"go.temporal.io/server/common/resolver"
)

const (
	mismatchLogMessage = "Supplied configuration key/value mismatches persisted cluster metadata. Continuing with the persisted value as this value cannot be changed once initialized."
)

// initClusterMetadata performs a config check against the configured persistence store for cluster metadata.
// If there is a mismatch, the persisted values take precedence and will be written over in the config objects.
// This is to keep this check hidden from downstream call although they should not use config directly.
func initClusterMetadata(cfg *config.Config, persistenceServiceResolver resolver.ServiceResolver, logger log.Logger) (cluster.Metadata, error) {
	logger = log.With(logger, tag.ComponentMetadataInitializer)

	factory := persistenceClient.NewFactory(
		&cfg.Persistence,
		persistenceServiceResolver,
		nil,
		nil,
		cfg.ClusterMetadata.CurrentClusterName,
		nil,
		logger,
	)

	clusterMetadataManager, err := factory.NewClusterMetadataManager()
	if err != nil {
		return nil, fmt.Errorf("error initializing cluster metadata manager: %w", err)
	}
	defer clusterMetadataManager.Close()

	applied, err := clusterMetadataManager.SaveClusterMetadata(
		&persistence.SaveClusterMetadataRequest{
			ClusterMetadata: persistencespb.ClusterMetadata{
				HistoryShardCount: cfg.Persistence.NumHistoryShards,
				ClusterName:       cfg.ClusterMetadata.CurrentClusterName,
				ClusterId:         uuid.New(),
			}})
	if err != nil {
		logger.Warn("Failed to save cluster metadata.", tag.Error(err))
	}
	if applied {
		logger.Info("Successfully saved cluster metadata.")
	} else {
		resp, err := clusterMetadataManager.GetClusterMetadata()
		if err != nil {
			return nil, fmt.Errorf("error while fetching cluster metadata: %w", err)
		}
		if cfg.ClusterMetadata.CurrentClusterName != resp.ClusterName {
			logger.Error(
				mismatchLogMessage,
				tag.Key("clusterMetadata.currentClusterName"),
				tag.IgnoredValue(cfg.ClusterMetadata.CurrentClusterName),
				tag.Value(resp.ClusterName))
			cfg.ClusterMetadata.CurrentClusterName = resp.ClusterName
		}

		var persistedShardCount = resp.HistoryShardCount
		if cfg.Persistence.NumHistoryShards != persistedShardCount {
			logger.Error(
				mismatchLogMessage,
				tag.Key("persistence.numHistoryShards"),
				tag.IgnoredValue(cfg.Persistence.NumHistoryShards),
				tag.Value(persistedShardCount))
			cfg.Persistence.NumHistoryShards = persistedShardCount
		}
	}

	metadata := cluster.NewMetadata(
		logger,
		cfg.ClusterMetadata.EnableGlobalNamespace,
		cfg.ClusterMetadata.FailoverVersionIncrement,
		cfg.ClusterMetadata.MasterClusterName,
		cfg.ClusterMetadata.CurrentClusterName,
		cfg.ClusterMetadata.ClusterInformation,
	)
	return metadata, nil
}
