package deployment

import (
	"cmp"
	"context"
	"sync"

	"go.temporal.io/sdk/activity"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/resource"
)

type (
	DeploymentActivities struct {
		namespace        *namespace.Namespace
		deploymentClient DeploymentStoreClient
		matchingClient   resource.MatchingClient
	}
)

func (a *DeploymentActivities) StartDeploymentSeriesWorkflow(ctx context.Context, input *deploymentspb.StartDeploymentSeriesRequest) error {
	logger := activity.GetLogger(ctx)
	logger.Info("starting deployment series workflow", "seriesName", input.SeriesName)
	identity := "deployment workflow " + activity.GetInfo(ctx).WorkflowExecution.ID
	return a.deploymentClient.StartDeploymentSeries(ctx, a.namespace, input.SeriesName, identity, input.RequestId)
}

func (a *DeploymentActivities) SyncUserData(ctx context.Context, input *deploymentspb.SyncUserDataRequest) (*deploymentspb.SyncUserDataResponse, error) {
	logger := activity.GetLogger(ctx)

	errs := make(chan error)

	var lock sync.Mutex
	maxVersionByName := make(map[string]int64)

	for _, sync := range input.Sync {
		go func() {
			logger.Info("syncing task queue userdata for deployment", "taskQueue", sync.Name, "type", sync.Type)
			res, err := a.matchingClient.SyncDeploymentUserData(ctx, &matchingservice.SyncDeploymentUserDataRequest{
				NamespaceId:   a.namespace.ID().String(),
				TaskQueue:     sync.Name,
				TaskQueueType: sync.Type,
				Deployment:    input.Deployment,
				Data:          sync.Data,
			})
			if err != nil {
				logger.Error("syncing task queue userdata", "taskQueue", sync.Name, "type", sync.Type, "error", err)
			} else {
				lock.Lock()
				maxVersionByName[sync.Name] = max(maxVersionByName[sync.Name], res.Version)
				lock.Unlock()
			}
			errs <- err
		}()
	}

	var err error
	for range input.Sync {
		err = cmp.Or(err, <-errs)
	}
	if err != nil {
		return nil, err
	}
	return &deploymentspb.SyncUserDataResponse{TaskQueueMaxVersions: maxVersionByName}, nil
}

func (a *DeploymentActivities) CheckUserDataPropagation(ctx context.Context, input *deploymentspb.CheckUserDataPropagationRequest) error {
	logger := activity.GetLogger(ctx)

	errs := make(chan error)

	for name, version := range input.TaskQueueMaxVersions {
		go func() {
			logger.Info("waiting for userdata propagation", "taskQueue", name, "version", version)
			_, err := a.matchingClient.CheckTaskQueueUserDataPropagation(ctx, &matchingservice.CheckTaskQueueUserDataPropagationRequest{
				NamespaceId: a.namespace.ID().String(),
				TaskQueue:   name,
				Version:     version,
			})
			if err != nil {
				logger.Error("waiting for userdata", "taskQueue", name, "type", version, "error", err)
			}
			errs <- err
		}()
	}

	var err error
	for range input.TaskQueueMaxVersions {
		err = cmp.Or(err, <-errs)
	}
	return err
}
