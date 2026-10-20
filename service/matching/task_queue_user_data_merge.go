package matching

import (
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
	"google.golang.org/protobuf/proto"
)

type taskQueueUserDataMergeConflictType string
type taskQueueUserDataSnapshotSelectionReason string

const (
	taskQueueUserDataMergeConflictTypeRoutingConfig taskQueueUserDataMergeConflictType = "routing_config"
	taskQueueUserDataMergeConflictTypeVersionData   taskQueueUserDataMergeConflictType = "version_data"

	taskQueueUserDataSelectionReasonCurrentParentClock  taskQueueUserDataSnapshotSelectionReason = "current_parent_clock"
	taskQueueUserDataSelectionReasonIncomingParentClock taskQueueUserDataSnapshotSelectionReason = "incoming_parent_clock"
	taskQueueUserDataSelectionReasonIncomingFallback    taskQueueUserDataSnapshotSelectionReason = "incoming_clock_fallback"
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

// selectTaskQueueUserDataSnapshots orders the current and incoming snapshots before their
// worker-deployment data is combined. The base snapshot supplies task queue config, fairness
// settings, versioning rules, and legacy deployment data. A clocked snapshot wins over a
// clockless snapshot, and otherwise the higher parent HLC wins. Incoming is used when the
// clocks are equal or both absent.
func selectTaskQueueUserDataSnapshots(
	current *persistencespb.TaskQueueUserData,
	incoming *persistencespb.TaskQueueUserData,
) (
	baseSnapshot *persistencespb.TaskQueueUserData,
	mergeSourceSnapshot *persistencespb.TaskQueueUserData,
	selectionReason taskQueueUserDataSnapshotSelectionReason,
) {
	currentClock := current.GetClock()
	incomingClock := incoming.GetClock()

	if currentClock != nil && (incomingClock == nil || hlc.Greater(currentClock, incomingClock)) {
		return current, incoming, taskQueueUserDataSelectionReasonCurrentParentClock
	}

	if incomingClock != nil && (currentClock == nil || hlc.Greater(incomingClock, currentClock)) {
		return incoming, current, taskQueueUserDataSelectionReasonIncomingParentClock
	}

	return incoming, current, taskQueueUserDataSelectionReasonIncomingFallback
}

func (reason taskQueueUserDataSnapshotSelectionReason) selectedSnapshotSource() string {
	if reason == taskQueueUserDataSelectionReasonCurrentParentClock {
		return "current"
	}
	return "incoming"
}

// mergeTaskQueueUserDataDeployments starts with a deep copy of baseSnapshot. Deployments and
// worker versions found only in mergeSourceSnapshot are added. When both snapshots contain the
// same routing configuration or worker version, the value with the higher revision is retained.
// Equal revisions retain the value from baseSnapshot.
func mergeTaskQueueUserDataDeployments(
	baseSnapshot *persistencespb.TaskQueueUserData,
	mergeSourceSnapshot *persistencespb.TaskQueueUserData,
) (*persistencespb.TaskQueueUserData, []taskQueueUserDataMergeConflict) {
	var merged *persistencespb.TaskQueueUserData
	if baseSnapshot == nil {
		merged = &persistencespb.TaskQueueUserData{}
	} else {
		merged = common.CloneProto(baseSnapshot)
	}

	var conflicts []taskQueueUserDataMergeConflict
	for taskQueueType, sourcePerType := range mergeSourceSnapshot.GetPerType() {
		sourceDeployments := sourcePerType.GetDeploymentData().GetDeploymentsData()
		if len(sourceDeployments) == 0 {
			continue
		}

		mergedDeployments := ensureDeploymentsData(merged, taskQueueType)
		for deploymentName, mergeSourceDeployment := range sourceDeployments {
			baseDeployment, exists := mergedDeployments[deploymentName]
			if !exists || baseDeployment == nil {
				mergedDeployments[deploymentName] = cloneWorkerDeploymentData(mergeSourceDeployment)
				continue
			}
			if mergeSourceDeployment == nil {
				continue
			}

			conflicts = append(
				conflicts,
				mergeWorkerDeploymentDataIntoBase(
					taskQueueType,
					deploymentName,
					baseDeployment,
					mergeSourceDeployment,
				)...,
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

func mergeWorkerDeploymentDataIntoBase(
	taskQueueType int32,
	deploymentName string,
	baseDeployment *persistencespb.WorkerDeploymentData,
	mergeSourceDeployment *persistencespb.WorkerDeploymentData,
) []taskQueueUserDataMergeConflict {
	conflicts := mergeWorkerDeploymentRoutingConfigIntoBase(
		taskQueueType,
		deploymentName,
		baseDeployment,
		mergeSourceDeployment,
	)
	versionConflicts := mergeWorkerDeploymentVersionsIntoBase(
		taskQueueType,
		deploymentName,
		baseDeployment,
		mergeSourceDeployment,
	)
	conflicts = append(conflicts, versionConflicts...)
	return conflicts
}

func mergeWorkerDeploymentRoutingConfigIntoBase(
	taskQueueType int32,
	deploymentName string,
	baseDeployment *persistencespb.WorkerDeploymentData,
	mergeSourceDeployment *persistencespb.WorkerDeploymentData,
) []taskQueueUserDataMergeConflict {
	baseRoutingConfig := baseDeployment.GetRoutingConfig()
	mergeSourceRoutingConfig := mergeSourceDeployment.GetRoutingConfig()
	if baseRoutingConfig == nil {
		if mergeSourceRoutingConfig != nil {
			baseDeployment.RoutingConfig = common.CloneProto(mergeSourceRoutingConfig)
		}
	} else if mergeSourceRoutingConfig != nil {
		selectedRoutingConfig, conflict := selectRevisionedProto(baseRoutingConfig, mergeSourceRoutingConfig)
		baseDeployment.RoutingConfig = selectedRoutingConfig
		if conflict {
			return []taskQueueUserDataMergeConflict{{
				conflictType:   taskQueueUserDataMergeConflictTypeRoutingConfig,
				taskQueueType:  taskQueueType,
				deploymentName: deploymentName,
				revisionNumber: selectedRoutingConfig.GetRevisionNumber(),
			}}
		}
	}
	return nil
}

func mergeWorkerDeploymentVersionsIntoBase(
	taskQueueType int32,
	deploymentName string,
	baseDeployment *persistencespb.WorkerDeploymentData,
	mergeSourceDeployment *persistencespb.WorkerDeploymentData,
) []taskQueueUserDataMergeConflict {
	if baseDeployment.Versions == nil && len(mergeSourceDeployment.GetVersions()) > 0 {
		baseDeployment.Versions = make(map[string]*deploymentspb.WorkerDeploymentVersionData)
	}

	var conflicts []taskQueueUserDataMergeConflict
	for buildID, mergeSourceVersion := range mergeSourceDeployment.GetVersions() {
		baseVersion, exists := baseDeployment.GetVersions()[buildID]
		if !exists || baseVersion == nil {
			if mergeSourceVersion == nil {
				baseDeployment.Versions[buildID] = nil
			} else {
				baseDeployment.Versions[buildID] = common.CloneProto(mergeSourceVersion)
			}
			continue
		}
		if mergeSourceVersion == nil {
			continue
		}

		selectedVersion, conflict := selectRevisionedProto(baseVersion, mergeSourceVersion)
		baseDeployment.Versions[buildID] = selectedVersion
		if conflict {
			conflicts = append(conflicts, taskQueueUserDataMergeConflict{
				conflictType:   taskQueueUserDataMergeConflictTypeVersionData,
				taskQueueType:  taskQueueType,
				deploymentName: deploymentName,
				buildID:        buildID,
				revisionNumber: selectedVersion.GetRevisionNumber(),
			})
		}
	}

	return conflicts
}

func selectRevisionedProto[T revisionedProto](baseValue T, mergeSourceValue T) (T, bool) {
	switch {
	case mergeSourceValue.GetRevisionNumber() > baseValue.GetRevisionNumber():
		return common.CloneProto(mergeSourceValue), false
	case mergeSourceValue.GetRevisionNumber() < baseValue.GetRevisionNumber():
		return baseValue, false
	case proto.Equal(baseValue, mergeSourceValue):
		return baseValue, false
	default:
		return baseValue, true
	}
}
