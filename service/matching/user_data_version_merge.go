package matching

import (
	"errors"
	"fmt"

	deploymentspb "go.temporal.io/server/api/deployment/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
	"google.golang.org/protobuf/proto"
)

func mergeClockedWorkerDeploymentVersions(
	current *persistencespb.TaskQueueUserData,
	incoming *persistencespb.TaskQueueUserData,
	target *persistencespb.TaskQueueUserData,
) error {
	if err := mergeClockedWorkerDeploymentVersionsFrom(target, current); err != nil {
		return err
	}
	return mergeClockedWorkerDeploymentVersionsFrom(target, incoming)
}

// mergeClockedWorkerDeploymentVersionsFrom merges source version entries without changing other
// per-type data. Entries without a state clock remain governed by the enclosing snapshot.
func mergeClockedWorkerDeploymentVersionsFrom(
	target *persistencespb.TaskQueueUserData,
	source *persistencespb.TaskQueueUserData,
) error {
	for taskQueueType, sourcePerType := range source.GetPerType() {
		for deploymentName, sourceDeployment := range sourcePerType.GetDeploymentData().GetDeploymentsData() {
			for buildID, sourceVersion := range sourceDeployment.GetVersions() {
				if sourceVersion.GetStateUpdateClock() == nil {
					continue
				}

				targetDeployment := ensureWorkerDeploymentData(target, taskQueueType, deploymentName)
				selected, changed, err := selectClockedWorkerDeploymentVersionData(
					targetDeployment.GetVersions()[buildID],
					sourceVersion,
				)
				if err != nil {
					return fmt.Errorf(
						"merge task queue type %d deployment %q build ID %q: %w",
						taskQueueType,
						deploymentName,
						buildID,
						err,
					)
				}
				if changed {
					targetDeployment.Versions[buildID] = selected
				}
				if selected.GetDeleted() {
					removeDeploymentVersions(
						target.GetPerType()[taskQueueType].GetDeploymentData(),
						deploymentName,
						nil,
						[]string{buildID},
						true,
					)
				}
			}
		}
	}
	return nil
}

func ensureWorkerDeploymentData(
	data *persistencespb.TaskQueueUserData,
	taskQueueType int32,
	deploymentName string,
) *persistencespb.WorkerDeploymentData {
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
	deployment := perType.DeploymentData.DeploymentsData[deploymentName]
	if deployment == nil {
		deployment = &persistencespb.WorkerDeploymentData{}
		perType.DeploymentData.DeploymentsData[deploymentName] = deployment
	}
	if deployment.Versions == nil {
		deployment.Versions = make(map[string]*deploymentspb.WorkerDeploymentVersionData)
	}
	return deployment
}

func selectClockedWorkerDeploymentVersionData(
	current *deploymentspb.WorkerDeploymentVersionData,
	incoming *deploymentspb.WorkerDeploymentVersionData,
) (*deploymentspb.WorkerDeploymentVersionData, bool, error) {
	if incoming.GetStateUpdateClock() == nil {
		return current, false, nil
	}
	comparison, err := compareClockedWorkerDeploymentVersionData(current, incoming)
	if err != nil {
		return nil, false, err
	}
	if comparison < 0 {
		return common.CloneProto(incoming), true, nil
	}
	return current, false, nil
}

// compareClockedWorkerDeploymentVersionData returns a positive value when current is newer,
// a negative value when incoming is newer, and zero when both values are identical.
func compareClockedWorkerDeploymentVersionData(
	current *deploymentspb.WorkerDeploymentVersionData,
	incoming *deploymentspb.WorkerDeploymentVersionData,
) (int, error) {
	currentClock := current.GetStateUpdateClock()
	incomingClock := incoming.GetStateUpdateClock()
	switch {
	case currentClock == nil && incomingClock == nil:
		if proto.Equal(current, incoming) {
			return 0, nil
		}
		return 0, errors.New("cannot compare different version values without state update clocks")
	case currentClock == nil:
		return -1, nil
	case incomingClock == nil:
		return 1, nil
	}

	// hlc.Compare returns a negative value when its first argument is newer. Invert it so a
	// positive result means current is newer, matching this function's contract.
	comparison := -hlc.Compare(currentClock, incomingClock)
	if comparison == 0 && !proto.Equal(current, incoming) {
		return 0, errors.New("different version values have the same state update clock")
	}
	return comparison, nil
}

func maxTaskQueueUserDataClock(
	data *persistencespb.TaskQueueUserData,
	candidates ...*hlc.Clock,
) *hlc.Clock {
	maxClock := data.GetClock()
	for _, candidate := range candidates {
		maxClock = maxClockValue(maxClock, candidate)
	}
	for _, perType := range data.GetPerType() {
		for _, deployment := range perType.GetDeploymentData().GetDeploymentsData() {
			for _, version := range deployment.GetVersions() {
				maxClock = maxClockValue(maxClock, version.GetStateUpdateClock())
			}
		}
	}
	return common.CloneProto(maxClock)
}

func maxClockValue(a *hlc.Clock, b *hlc.Clock) *hlc.Clock {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	return hlc.Max(a, b)
}
