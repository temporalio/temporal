package deployment

import (
	"context"

	"go.temporal.io/sdk/activity"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/common/namespace"
)

type (
	DeploymentSeriesActivities struct {
		namespace        *namespace.Namespace
		deploymentClient DeploymentStoreClient
	}
)

func (a *DeploymentSeriesActivities) SyncDeployment(ctx context.Context, args *deploymentspb.SyncDeploymentStateActivityArgs) (*deploymentspb.SyncDeploymentStateActivityResult, error) {
	identity := "deployment series workflow " + activity.GetInfo(ctx).WorkflowExecution.ID
	res, err := a.deploymentClient.SyncDeploymentWorkflowFromSeries(
		ctx,
		a.namespace,
		args.Deployment,
		args.Args,
		identity,
		args.RequestId,
	)
	if err != nil {
		return nil, err
	}
	return &deploymentspb.SyncDeploymentStateActivityResult{
		State: res.DeploymentLocalState,
	}, nil
}
