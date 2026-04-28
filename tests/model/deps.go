package model

import (
	"context"

	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/tests/testcore/umpire"
)

// Deps bundles the cluster-wide dependencies every per-entity component
// needs: the Umpire, a context, the gRPC clients, the default namespace,
// and the per-iteration prefix used for unique naming. Per-component Deps
// embed this struct and add their own typed entity store plus any
// component-specific fields (e.g. MaxOperations, EndpointName).
type Deps struct {
	Umpire    *umpire.Umpire
	Context   context.Context
	Client    workflowservice.WorkflowServiceClient
	Operator  operatorservice.OperatorServiceClient
	Namespace string // default parent namespace
	Prefix    string // per-iteration prefix for unique entity names
}
