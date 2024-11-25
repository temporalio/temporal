package matching

import (
	"time"

	deploymentpb "go.temporal.io/api/deployment/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

var (
	errDeploymentsNotAllowed = serviceerror.NewPermissionDenied("Deployments are disabled on this namespace.", "")
	errMissingDeployment     = serviceerror.NewInvalidArgument("missing deployment")
	errBadDeploymentUpdate   = serviceerror.NewInvalidArgument("bad deployment update type")
)

func findDeployment(deployments []*persistencespb.DeploymentData_Deployment, deployment *deploymentpb.Deployment) int {
	for i, d := range deployments {
		if d.Deployment.SeriesName == deployment.SeriesName && d.Deployment.BuildId == deployment.BuildId {
			return i
		}
	}
	return -1
}

func findCurrentDeployment(deployments []*persistencespb.DeploymentData_Deployment) *deploymentpb.Deployment {
	maxCurrentIndex := -1
	var maxCurrentTime time.Time
	for i, d := range deployments {
		if d.Data.LastBecameCurrentTime != nil {
			if t := d.Data.LastBecameCurrentTime.AsTime(); t.After(maxCurrentTime) {
				maxCurrentIndex, maxCurrentTime = i, t
			}
		}
	}
	if maxCurrentIndex == -1 {
		return nil
	}
	return deployments[maxCurrentIndex].GetDeployment()
}
