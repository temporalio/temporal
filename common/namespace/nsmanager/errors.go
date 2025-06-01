package nsmanager

import (
	"go.temporal.io/api/serviceerror"
)

var (
	errActiveClusterNotInClusters = serviceerror.NewInvalidArgument("Active cluster is not contained in all clusters.")
	errInvalidArchivalConfig      = serviceerror.NewInvalidArgument("Invalid to enable archival without specifying a uri.")
)
