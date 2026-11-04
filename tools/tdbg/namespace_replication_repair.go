package tdbg

import (
	"cmp"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
)

const (
	namespaceReplicationRepairActionNone          = "NONE"
	namespaceReplicationRepairActionCreate        = "CREATE"
	namespaceReplicationRepairActionApplySource   = "APPLY_SOURCE"
	namespaceReplicationRepairActionPrepareSource = "PREPARE_SOURCE"

	namespaceReplicationRepairStatusPlanned   = "PLANNED"
	namespaceReplicationRepairStatusNoChanges = "NO_CHANGES"
	namespaceReplicationRepairStatusBlocked   = "BLOCKED"
)

type namespaceReplicationRepairPlan struct {
	RepairID                    string                                  `json:"repair_id"`
	PlannedAt                   time.Time                               `json:"planned_at"`
	NamespaceName               string                                  `json:"namespace_name,omitempty"`
	NamespaceID                 string                                  `json:"namespace_id,omitempty"`
	SourceCluster               string                                  `json:"source_cluster,omitempty"`
	Status                      string                                  `json:"status"`
	StatusDetail                string                                  `json:"status_detail,omitempty"`
	SourceConfigVersion         int64                                   `json:"source_config_version"`
	ProposedSourceConfigVersion int64                                   `json:"proposed_source_config_version"`
	SourceAction                string                                  `json:"source_action"`
	Clusters                    []namespaceReplicationClusterRepairPlan `json:"clusters,omitempty"`
}

type namespaceReplicationClusterRepairPlan struct {
	Warnings             []string `json:"warnings,omitempty"`
	Cluster              string   `json:"cluster"`
	Presence             string   `json:"presence"`
	CurrentConfigVersion *int64   `json:"current_config_version,omitempty"`
	Action               string   `json:"action"`
}

func planNamespaceReplicationRepair(
	observation *namespaceReplicationObservation,
	repairID string,
	plannedAt time.Time,
) (*namespaceReplicationRepairPlan, error) {
	if observation == nil || observation.result == nil {
		return nil, errors.New("repair planning requires a complete namespace replication observation")
	}
	verification := observation.result
	plan := &namespaceReplicationRepairPlan{
		RepairID:                    repairID,
		PlannedAt:                   plannedAt.UTC(),
		NamespaceName:               verification.NamespaceName,
		NamespaceID:                 verification.NamespaceID,
		SourceCluster:               verification.SourceCluster,
		Status:                      namespaceReplicationRepairStatusBlocked,
		SourceConfigVersion:         observation.sourceSnapshot.GetConfigVersion(),
		ProposedSourceConfigVersion: observation.sourceSnapshot.GetConfigVersion(),
		SourceAction:                namespaceReplicationRepairActionNone,
	}
	if verification.Status != namespaceReplicationStatusHealthy &&
		verification.Status != namespaceReplicationStatusRepairRequired {
		plan.StatusDetail = verification.StatusDetail
		if plan.StatusDetail == "" {
			plan.StatusDetail = "verification did not produce an executable repair plan"
		}
		return plan, nil
	}

	versionPlan, err := inspectNamespaceRepairVersions(verification, plan.SourceConfigVersion)
	if err != nil {
		return nil, err
	}
	if versionPlan.requiresIncrement && versionPlan.maxDivergentConfigVersion == math.MaxInt64 {
		plan.StatusDetail = "cannot choose a repair config version without overflowing int64"
		return plan, nil
	}
	if versionPlan.requiresIncrement {
		plan.ProposedSourceConfigVersion = max(versionPlan.maxConfigVersion, versionPlan.maxDivergentConfigVersion+1)
	} else if versionPlan.maxConfigVersion > plan.SourceConfigVersion {
		plan.ProposedSourceConfigVersion = versionPlan.maxConfigVersion
	}
	if plan.ProposedSourceConfigVersion != plan.SourceConfigVersion {
		plan.SourceAction = namespaceReplicationRepairActionPrepareSource
	}

	for _, cluster := range verification.Clusters {
		if cluster.Role == namespaceReplicationRoleSource {
			continue
		}
		plan.Clusters = append(plan.Clusters, namespaceReplicationClusterRepairPlan{
			Warnings:             slices.Clone(cluster.Warnings),
			Cluster:              cluster.Cluster,
			Presence:             cluster.Presence,
			CurrentConfigVersion: cluster.ConfigVersion,
			Action:               namespaceRepairTargetAction(cluster),
		})
	}
	slices.SortFunc(plan.Clusters, func(left, right namespaceReplicationClusterRepairPlan) int {
		return cmp.Compare(left.Cluster, right.Cluster)
	})

	plan.Status = namespaceReplicationRepairStatusNoChanges
	if plan.SourceAction != namespaceReplicationRepairActionNone || slices.ContainsFunc(
		plan.Clusters,
		func(cluster namespaceReplicationClusterRepairPlan) bool {
			return cluster.Action != namespaceReplicationRepairActionNone
		},
	) {
		plan.Status = namespaceReplicationRepairStatusPlanned
	}
	return plan, nil
}

type namespaceRepairVersionPlan struct {
	maxConfigVersion          int64
	maxDivergentConfigVersion int64
	requiresIncrement         bool
}

func inspectNamespaceRepairVersions(
	verification *namespaceReplicationVerificationResult,
	sourceConfigVersion int64,
) (namespaceRepairVersionPlan, error) {
	result := namespaceRepairVersionPlan{maxConfigVersion: sourceConfigVersion}
	for _, cluster := range verification.Clusters {
		if cluster.Role == namespaceReplicationRoleSource || cluster.Presence != namespaceReplicationPresencePresent {
			continue
		}
		if cluster.ConfigVersion == nil {
			return result, fmt.Errorf("cluster %q has no config version", cluster.Cluster)
		}
		result.maxConfigVersion = max(result.maxConfigVersion, *cluster.ConfigVersion)
		if cluster.ConfigMatch != namespaceReplicationMatchMatch {
			result.maxDivergentConfigVersion = max(result.maxDivergentConfigVersion, *cluster.ConfigVersion)
			result.requiresIncrement = result.requiresIncrement || *cluster.ConfigVersion >= sourceConfigVersion
		}
	}
	return result, nil
}

func namespaceRepairTargetAction(
	cluster namespaceReplicationClusterVerification,
) string {
	if cluster.Presence == namespaceReplicationPresenceMissing {
		return namespaceReplicationRepairActionCreate
	}
	if cluster.Presence != namespaceReplicationPresencePresent || cluster.ConfigMatch == namespaceReplicationMatchMatch {
		return namespaceReplicationRepairActionNone
	}
	return namespaceReplicationRepairActionApplySource
}

func buildNamespaceReplicationRepairTask(
	source *adminservice.GetNamespaceResponse,
	target *adminservice.GetNamespaceResponse,
	dataKeysToIgnore map[string]struct{},
) (*replicationspb.NamespaceTaskAttributes, error) {
	configProjection, _, err := namespaceReplicationProjections(source, dataKeysToIgnore)
	if err != nil {
		return nil, fmt.Errorf("project repair source config: %w", err)
	}
	failoverSource := source
	if target != nil {
		failoverSource = target
	}
	_, failoverProjection, err := namespaceReplicationProjections(failoverSource, nil)
	if err != nil {
		return nil, fmt.Errorf("project repair failover state: %w", err)
	}

	task := configProjection
	task.NamespaceOperation = enumsspb.NAMESPACE_OPERATION_UPDATE
	task.ConfigVersion = source.GetConfigVersion()
	task.FailoverVersion = failoverSource.GetFailoverVersion()
	task.ReplicationConfig.ActiveClusterName = failoverProjection.GetReplicationConfig().GetActiveClusterName()
	task.ReplicationConfig.State = failoverProjection.GetReplicationConfig().GetState()
	task.FailoverHistory = failoverProjection.GetFailoverHistory()

	for key := range dataKeysToIgnore {
		if value, ok := target.GetInfo().GetData()[key]; ok {
			if task.Info.Data == nil {
				task.Info.Data = make(map[string]string)
			}
			task.Info.Data[key] = value
		}
	}

	return task, nil
}
