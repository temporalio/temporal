package namespace

import (
	"go.temporal.io/temporal-proto/serviceerror"
)

var (
	// err indicating that this cluster is not the master, so cannot do namespace registration or update
	errNotMasterCluster                   = serviceerror.NewInvalidArgument("Cluster is not master cluster, cannot do namespace registration or namespace update.")
	errCannotRemoveClustersFromNamespace  = serviceerror.NewInvalidArgument("Cannot remove existing replicated clusters from a namespace.")
	errActiveClusterNotInClusters         = serviceerror.NewInvalidArgument("Active cluster is not contained in all clusters.")
	errCannotDoNamespaceFailoverAndUpdate = serviceerror.NewInvalidArgument("Cannot set active cluster to current cluster when other parameters are set.")
	errInvalidRetentionPeriod             = serviceerror.NewInvalidArgument("A valid retention period is not set on request.")
	errInvalidArchivalConfig              = serviceerror.NewInvalidArgument("Invalid to enable archival without specifying a uri.")
)
