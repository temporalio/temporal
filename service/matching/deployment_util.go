package matching

import (
	deploymentpb "go.temporal.io/api/deployment/v1"
	"go.temporal.io/api/serviceerror"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/worker_versioning"
	"google.golang.org/protobuf/proto"
)

var (
	errDeploymentsNotAllowed = serviceerror.NewPermissionDenied("deployments are disabled on this namespace", "")
	// [cleanup-wv-pre-release]
	errMissingDeployment = serviceerror.NewInvalidArgument("missing deployment")

	errMissingDeploymentVersion  = serviceerror.NewInvalidArgument("missing deployment version")
	errDeploymentVersionNotReady = serviceerror.NewUnavailable("task queue is not ready to process polls from this deployment version, try again shortly")
)

// [cleanup-wv-pre-release]
func findDeployment(deployments *persistencespb.DeploymentData, deployment *deploymentpb.Deployment) int {
	for i, d := range deployments.GetDeployments() {
		if d.Deployment.Equal(deployment) {
			return i
		}
	}
	return -1
}

func findDeploymentVersion(deployments *persistencespb.DeploymentData, v *deploymentspb.WorkerDeploymentVersion) int {
	for i, vd := range deployments.GetVersions() {
		if proto.Equal(v, vd.GetVersion()) {
			return i
		}
	}
	return -1
}

//nolint:staticcheck
func hasDeploymentVersion(deployments *persistencespb.DeploymentData, v *deploymentspb.WorkerDeploymentVersion) bool {
	for _, d := range deployments.GetDeployments() {
		if d.Deployment.Equal(worker_versioning.DeploymentFromDeploymentVersion(v)) {
			return true
		}
	}

	for _, vd := range deployments.GetVersions() {
		if proto.Equal(v, vd.GetVersion()) {
			return true
		}
	}

	return false
}
