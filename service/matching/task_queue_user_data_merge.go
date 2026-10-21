package matching

import (
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
	"google.golang.org/protobuf/proto"
)

type taskQueueUserDataMergeConflictType string
type taskQueueUserDataSelectionReason string

const (
	taskQueueUserDataMergeConflictTypeRoutingConfig taskQueueUserDataMergeConflictType = "routing_config"
	taskQueueUserDataMergeConflictTypeVersionData   taskQueueUserDataMergeConflictType = "version_data"

	taskQueueUserDataSelectionReasonCurrentParentClock  taskQueueUserDataSelectionReason = "current_parent_clock"
	taskQueueUserDataSelectionReasonIncomingParentClock taskQueueUserDataSelectionReason = "incoming_parent_clock"
	taskQueueUserDataSelectionReasonIncomingFallback    taskQueueUserDataSelectionReason = "incoming_clock_fallback"
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

// selectTaskQueueUserData selects the base user data before combining worker-deployment data.
// The base supplies task queue config, fairness settings, versioning rules, and legacy deployment data.
// User data with a clock wins over user data without a clock. Otherwise, the higher parent HLC wins.
// Incoming is used when the clocks are equal or both absent.
func selectTaskQueueUserData(
	current *persistencespb.TaskQueueUserData,
	incoming *persistencespb.TaskQueueUserData,
) (
	baseUserData *persistencespb.TaskQueueUserData,
	otherUserData *persistencespb.TaskQueueUserData,
	selectionReason taskQueueUserDataSelectionReason,
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

func (reason taskQueueUserDataSelectionReason) selectedUserDataSource() string {
	if reason == taskQueueUserDataSelectionReasonCurrentParentClock {
		return "current"
	}
	return "incoming"
}

// mergeTaskQueueUserDataDeployments starts with a deep copy of baseUserData. Deployments and
// worker versions found only in otherUserData are added. When both contain the
// same routing configuration or worker version, the value with the higher revision is retained.
// Equal revisions retain the value from baseUserData.
func mergeTaskQueueUserDataDeployments(
	baseUserData *persistencespb.TaskQueueUserData,
	otherUserData *persistencespb.TaskQueueUserData,
) (*persistencespb.TaskQueueUserData, []taskQueueUserDataMergeConflict) {
	var merged *persistencespb.TaskQueueUserData
	if baseUserData == nil {
		merged = &persistencespb.TaskQueueUserData{}
	} else {
		merged = common.CloneProto(baseUserData)
	}

	var conflicts []taskQueueUserDataMergeConflict
	for taskQueueType, otherPerType := range otherUserData.GetPerType() {
		otherDeployments := otherPerType.GetDeploymentData().GetDeploymentsData()
		if len(otherDeployments) == 0 {
			continue
		}

		mergedDeployments := ensureDeploymentsData(merged, taskQueueType)
		for deploymentName, otherDeployment := range otherDeployments {
			baseDeployment, exists := mergedDeployments[deploymentName]
			if !exists || baseDeployment == nil {
				mergedDeployments[deploymentName] = cloneWorkerDeploymentData(otherDeployment)
				continue
			}
			if otherDeployment == nil {
				continue
			}

			conflicts = append(
				conflicts,
				mergeWorkerDeploymentDataIntoBase(
					taskQueueType,
					deploymentName,
					baseDeployment,
					otherDeployment,
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
	otherDeployment *persistencespb.WorkerDeploymentData,
) []taskQueueUserDataMergeConflict {
	conflicts := mergeWorkerDeploymentRoutingConfigIntoBase(
		taskQueueType,
		deploymentName,
		baseDeployment,
		otherDeployment,
	)
	versionConflicts := mergeWorkerDeploymentVersionsIntoBase(
		taskQueueType,
		deploymentName,
		baseDeployment,
		otherDeployment,
	)
	conflicts = append(conflicts, versionConflicts...)
	return conflicts
}

func mergeWorkerDeploymentRoutingConfigIntoBase(
	taskQueueType int32,
	deploymentName string,
	baseDeployment *persistencespb.WorkerDeploymentData,
	otherDeployment *persistencespb.WorkerDeploymentData,
) []taskQueueUserDataMergeConflict {
	baseRoutingConfig := baseDeployment.GetRoutingConfig()
	otherRoutingConfig := otherDeployment.GetRoutingConfig()
	if baseRoutingConfig == nil {
		if otherRoutingConfig != nil {
			baseDeployment.RoutingConfig = common.CloneProto(otherRoutingConfig)
		}
	} else if otherRoutingConfig != nil {
		selectedRoutingConfig, conflict := selectRevisionedProto(baseRoutingConfig, otherRoutingConfig)
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
	otherDeployment *persistencespb.WorkerDeploymentData,
) []taskQueueUserDataMergeConflict {
	if baseDeployment.Versions == nil && len(otherDeployment.GetVersions()) > 0 {
		baseDeployment.Versions = make(map[string]*deploymentspb.WorkerDeploymentVersionData)
	}

	var conflicts []taskQueueUserDataMergeConflict
	for buildID, otherVersion := range otherDeployment.GetVersions() {
		baseVersion, exists := baseDeployment.GetVersions()[buildID]
		if !exists || baseVersion == nil {
			if otherVersion == nil {
				baseDeployment.Versions[buildID] = nil
			} else {
				baseDeployment.Versions[buildID] = common.CloneProto(otherVersion)
			}
			continue
		}
		if otherVersion == nil {
			continue
		}

		selectedVersion, conflict := selectRevisionedProto(baseVersion, otherVersion)
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

func selectRevisionedProto[T revisionedProto](baseValue T, otherValue T) (T, bool) {
	switch {
	case otherValue.GetRevisionNumber() > baseValue.GetRevisionNumber():
		return common.CloneProto(otherValue), false
	case otherValue.GetRevisionNumber() < baseValue.GetRevisionNumber():
		return baseValue, false
	case proto.Equal(baseValue, otherValue):
		return baseValue, false
	default:
		return baseValue, true
	}
}
