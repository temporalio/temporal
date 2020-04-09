package namespace

import (
	"fmt"

	namespacepb "go.temporal.io/temporal-proto/namespace"
	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/persistence"
)

type (
	// AttrValidatorImpl is namespace attr validator
	AttrValidatorImpl struct {
		clusterMetadata  cluster.Metadata
		minRetentionDays int32
	}
)

// newAttrValidator create a new namespace attr validator
func newAttrValidator(
	clusterMetadata cluster.Metadata,
	minRetentionDays int32,
) *AttrValidatorImpl {

	return &AttrValidatorImpl{
		clusterMetadata:  clusterMetadata,
		minRetentionDays: minRetentionDays,
	}
}

func (d *AttrValidatorImpl) validateNamespaceConfig(config *persistence.NamespaceConfig) error {
	if config.Retention < int32(d.minRetentionDays) {
		return errInvalidRetentionPeriod
	}
	if config.HistoryArchivalStatus == namespacepb.ArchivalStatus_Enabled && len(config.HistoryArchivalURI) == 0 {
		return errInvalidArchivalConfig
	}
	if config.VisibilityArchivalStatus == namespacepb.ArchivalStatus_Enabled && len(config.VisibilityArchivalURI) == 0 {
		return errInvalidArchivalConfig
	}
	return nil
}

func (d *AttrValidatorImpl) validateNamespaceReplicationConfigForLocalNamespace(
	replicationConfig *persistence.NamespaceReplicationConfig,
) error {

	activeCluster := replicationConfig.ActiveClusterName
	clusters := replicationConfig.Clusters

	if err := d.validateClusterName(activeCluster); err != nil {
		return err
	}
	for _, clusterConfig := range clusters {
		if err := d.validateClusterName(clusterConfig.ClusterName); err != nil {
			return err
		}
	}

	if activeCluster != d.clusterMetadata.GetCurrentClusterName() {
		return serviceerror.NewInvalidArgument("Invalid local namespace active cluster")
	}

	if len(clusters) != 1 || clusters[0].ClusterName != activeCluster {
		return serviceerror.NewInvalidArgument("Invalid local namespace clusters")
	}

	return nil
}

func (d *AttrValidatorImpl) validateNamespaceReplicationConfigForGlobalNamespace(
	replicationConfig *persistence.NamespaceReplicationConfig,
) error {

	activeCluster := replicationConfig.ActiveClusterName
	clusters := replicationConfig.Clusters

	if err := d.validateClusterName(activeCluster); err != nil {
		return err
	}
	for _, clusterConfig := range clusters {
		if err := d.validateClusterName(clusterConfig.ClusterName); err != nil {
			return err
		}
	}

	activeClusterInClusters := false
	for _, clusterConfig := range clusters {
		if clusterConfig.ClusterName == activeCluster {
			activeClusterInClusters = true
			break
		}
	}
	if !activeClusterInClusters {
		return errActiveClusterNotInClusters
	}

	return nil
}

func (d *AttrValidatorImpl) validateNamespaceReplicationConfigClustersDoesNotRemove(
	clustersOld []*persistence.ClusterReplicationConfig,
	clustersNew []*persistence.ClusterReplicationConfig,
) error {

	clusterNamesOld := make(map[string]bool)
	for _, clusterConfig := range clustersOld {
		clusterNamesOld[clusterConfig.ClusterName] = true
	}
	clusterNamesNew := make(map[string]bool)
	for _, clusterConfig := range clustersNew {
		clusterNamesNew[clusterConfig.ClusterName] = true
	}

	if len(clusterNamesNew) < len(clusterNamesOld) {
		return errCannotRemoveClustersFromNamespace
	}

	for clusterName := range clusterNamesOld {
		if _, ok := clusterNamesNew[clusterName]; !ok {
			return errCannotRemoveClustersFromNamespace
		}
	}
	return nil
}

func (d *AttrValidatorImpl) validateClusterName(
	clusterName string,
) error {

	if info, ok := d.clusterMetadata.GetAllClusterInfo()[clusterName]; !ok || !info.Enabled {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("Invalid cluster name: %v", clusterName))
	}
	return nil
}
