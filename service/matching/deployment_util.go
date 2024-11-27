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

func findDeployment(deployments *persistencespb.DeploymentData, deployment *deploymentpb.Deployment) int {
	for i, d := range deployments.GetDeployments() {
		if d.Deployment.SeriesName == deployment.SeriesName && d.Deployment.BuildId == deployment.BuildId {
			return i
		}
	}
	return -1
}

func findCurrentDeployment(deployments *persistencespb.DeploymentData) *deploymentpb.Deployment {
	var currentDeployment *deploymentpb.Deployment
	var maxCurrentTime time.Time
	for _, d := range deployments.GetDeployments() {
		if d.Data.LastBecameCurrentTime != nil {
			if t := d.Data.LastBecameCurrentTime.AsTime(); t.After(maxCurrentTime) {
				currentDeployment, maxCurrentTime = d.GetDeployment(), t
			}
		}
	}
	return currentDeployment
}
