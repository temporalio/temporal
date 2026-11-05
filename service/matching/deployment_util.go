package matching

import (
	"go.temporal.io/api/serviceerror"
)

var errMissingDeploymentVersion = serviceerror.NewInvalidArgument("missing deployment version")
