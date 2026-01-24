package nsmanager

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
)

type (
	Validator struct {
		clusterMetadata cluster.Metadata
	}
)

// NewValidator create a new namespace attribute Validator
func NewValidator(
	clusterMetadata cluster.Metadata,
) *Validator {
	return &Validator{
		clusterMetadata: clusterMetadata,
	}
}

func (d *Validator) ValidateNamespaceConfig(config *persistencespb.NamespaceConfig) error {
	if config.GetHistoryArchivalState() == enumspb.ARCHIVAL_STATE_ENABLED && len(config.GetHistoryArchivalUri()) == 0 {
		return errInvalidArchivalConfig
	}
	if config.GetVisibilityArchivalState() == enumspb.ARCHIVAL_STATE_ENABLED && len(config.GetVisibilityArchivalUri()) == 0 {
		return errInvalidArchivalConfig
	}
	return nil
}

func (d *Validator) ValidateNamespaceReplicationConfigForLocalNamespace(
	replicationConfig *persistencespb.NamespaceReplicationConfig,
) error {
	activeCluster := replicationConfig.GetActiveClusterName()
	clusters := replicationConfig.GetClusters()

	if err := d.validateClusterName(activeCluster); err != nil {
		return err
	}
	for _, clusterName := range clusters {
		if err := d.validateClusterName(clusterName); err != nil {
			return err
		}
	}

	if activeCluster != d.clusterMetadata.GetCurrentClusterName() {
		return serviceerror.NewInvalidArgument("Invalid local namespace active cluster")
	}

	if len(clusters) != 1 || clusters[0] != activeCluster {
		return serviceerror.NewInvalidArgument("Invalid local namespace clusters")
	}

	return nil
}

func (d *Validator) ValidateNamespaceReplicationConfigForGlobalNamespace(
	replicationConfig *persistencespb.NamespaceReplicationConfig,
) error {
	activeCluster := replicationConfig.GetActiveClusterName()
	clusters := replicationConfig.GetClusters()

	if err := d.validateClusterName(activeCluster); err != nil {
		return err
	}
	for _, clusterName := range clusters {
		if err := d.validateClusterName(clusterName); err != nil {
			return err
		}
	}

	activeClusterInClusters := false
	for _, clusterName := range clusters {
		if clusterName == activeCluster {
			activeClusterInClusters = true
			break
		}
	}
	if !activeClusterInClusters {
		return errActiveClusterNotInClusters
	}

	return nil
}

func (d *Validator) validateClusterName(
	clusterName string,
) error {
	if info, ok := d.clusterMetadata.GetAllClusterInfo()[clusterName]; !ok || !info.Enabled {
		return serviceerror.NewInvalidArgumentf("Invalid cluster name: %v", clusterName)
	}
	return nil
}
