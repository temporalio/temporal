package workerdeployment

import (
	"context"

	deploymentpb "go.temporal.io/api/deployment/v1"
	"go.temporal.io/sdk/activity"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/worker_versioning"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	DrainageActivities struct {
		namespace        *namespace.Namespace
		deploymentClient Client
	}
)

func (a *DrainageActivities) GetVersionDrainageStatus(ctx context.Context, version *deploymentspb.WorkerDeploymentVersion) (*deploymentpb.VersionDrainageInfo, error) {
	logger := activity.GetLogger(ctx)
	response, err := a.deploymentClient.GetVersionDrainageStatus(ctx, a.namespace, worker_versioning.WorkerDeploymentVersionToString(version))
	if err != nil {
		logger.Error("error counting workflows for drainage status", "error", err)
		return nil, err
	}
	return &deploymentpb.VersionDrainageInfo{
		Status:          response,
		LastChangedTime: nil, // ignored; whether Status changed will be evaluated by the receiver
		LastCheckedTime: timestamppb.Now(),
	}, nil
}
