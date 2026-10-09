package matching

import (
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/proto"
)

type taskQueueUserDataMergeConflictType string

const (
	taskQueueUserDataMergeConflictTypeRoutingConfig taskQueueUserDataMergeConflictType = "routing_config"
	taskQueueUserDataMergeConflictTypeVersionData   taskQueueUserDataMergeConflictType = "version_data"
)

type taskQueueUserDataMergeConflict struct {
	conflictType   taskQueueUserDataMergeConflictType
	taskQueueType  int32
	deploymentName string
	buildID        string
	revisionNumber int64
}

type revisionedProto interface {
	proto.Message
	GetRevisionNumber() int64
}

// The parent HLC cannot order routing and version cells whose revisions advance independently,
// so preserve the parent-selected snapshot while unioning only those cells.
func mergeTaskQueueUserDataDeployments(
	preferred *persistencespb.TaskQueueUserData,
	other *persistencespb.TaskQueueUserData,
) (*persistencespb.TaskQueueUserData, []taskQueueUserDataMergeConflict) {
	var merged *persistencespb.TaskQueueUserData
	if preferred == nil {
		merged = &persistencespb.TaskQueueUserData{}
	} else {
		merged = common.CloneProto(preferred)
	}

	var conflicts []taskQueueUserDataMergeConflict
	for taskQueueType, otherPerType := range other.GetPerType() {
		otherDeployments := otherPerType.GetDeploymentData().GetDeploymentsData()
		if len(otherDeployments) == 0 {
			continue
		}

		mergedDeployments := ensureDeploymentsData(merged, taskQueueType)
		for deploymentName, otherDeployment := range otherDeployments {
			mergedDeployment, exists := mergedDeployments[deploymentName]
			if !exists || mergedDeployment == nil {
				mergedDeployments[deploymentName] = cloneWorkerDeploymentData(otherDeployment)
				continue
			}
			if otherDeployment == nil {
				continue
			}

			conflicts = append(
				conflicts,
				mergeWorkerDeploymentData(taskQueueType, deploymentName, mergedDeployment, otherDeployment)...,
			)
		}
	}

	return merged, conflicts
}

func ensureDeploymentsData(
	data *persistencespb.TaskQueueUserData,
	taskQueueType int32,
) map[string]*persistencespb.WorkerDeploymentData {
	if data.PerType == nil {
		data.PerType = make(map[int32]*persistencespb.TaskQueueTypeUserData)
	}
	perType := data.PerType[taskQueueType]
	if perType == nil {
		perType = &persistencespb.TaskQueueTypeUserData{}
		data.PerType[taskQueueType] = perType
	}
	if perType.DeploymentData == nil {
		perType.DeploymentData = &persistencespb.DeploymentData{}
	}
	if perType.DeploymentData.DeploymentsData == nil {
		perType.DeploymentData.DeploymentsData = make(map[string]*persistencespb.WorkerDeploymentData)
	}
	return perType.DeploymentData.DeploymentsData
}

func cloneWorkerDeploymentData(data *persistencespb.WorkerDeploymentData) *persistencespb.WorkerDeploymentData {
	if data == nil {
		return nil
	}
	return common.CloneProto(data)
}

func mergeWorkerDeploymentData(
	taskQueueType int32,
	deploymentName string,
	preferred *persistencespb.WorkerDeploymentData,
	other *persistencespb.WorkerDeploymentData,
) []taskQueueUserDataMergeConflict {
	var conflicts []taskQueueUserDataMergeConflict
	preferredRoutingConfig := preferred.GetRoutingConfig()
	otherRoutingConfig := other.GetRoutingConfig()
	if preferredRoutingConfig == nil {
		if otherRoutingConfig != nil {
			preferred.RoutingConfig = common.CloneProto(otherRoutingConfig)
		}
	} else if otherRoutingConfig != nil {
		selected, conflict := selectRevisionedProto(preferredRoutingConfig, otherRoutingConfig)
		preferred.RoutingConfig = selected
		if conflict {
			conflicts = append(conflicts, taskQueueUserDataMergeConflict{
				conflictType:   taskQueueUserDataMergeConflictTypeRoutingConfig,
				taskQueueType:  taskQueueType,
				deploymentName: deploymentName,
				revisionNumber: selected.GetRevisionNumber(),
			})
		}
	}

	if preferred.Versions == nil && len(other.GetVersions()) > 0 {
		preferred.Versions = make(map[string]*deploymentspb.WorkerDeploymentVersionData)
	}
	for buildID, otherVersion := range other.GetVersions() {
		preferredVersion, exists := preferred.GetVersions()[buildID]
		if !exists || preferredVersion == nil {
			if otherVersion == nil {
				preferred.Versions[buildID] = nil
			} else {
				preferred.Versions[buildID] = common.CloneProto(otherVersion)
			}
			continue
		}
		if otherVersion == nil {
			continue
		}

		selected, conflict := selectRevisionedProto(preferredVersion, otherVersion)
		preferred.Versions[buildID] = selected
		if conflict {
			conflicts = append(conflicts, taskQueueUserDataMergeConflict{
				conflictType:   taskQueueUserDataMergeConflictTypeVersionData,
				taskQueueType:  taskQueueType,
				deploymentName: deploymentName,
				buildID:        buildID,
				revisionNumber: selected.GetRevisionNumber(),
			})
		}
	}

	return conflicts
}

func selectRevisionedProto[T revisionedProto](preferred T, other T) (T, bool) {
	switch {
	case other.GetRevisionNumber() > preferred.GetRevisionNumber():
		return common.CloneProto(other), false
	case other.GetRevisionNumber() < preferred.GetRevisionNumber():
		return preferred, false
	case proto.Equal(preferred, other):
		return preferred, false
	default:
		return preferred, true
	}
}
