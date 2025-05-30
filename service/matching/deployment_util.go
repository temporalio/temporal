package matching

import (
	"go.temporal.io/api/serviceerror"
)

var (
	errDeploymentsNotAllowed = serviceerror.NewPermissionDenied("deployments are disabled on this namespace", "")
	// [cleanup-wv-pre-release]
	errMissingDeployment = serviceerror.NewInvalidArgument("missing deployment")

	errMissingDeploymentVersion = serviceerror.NewInvalidArgument("missing deployment version")
)
