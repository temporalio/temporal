package workerdeployment

import (
	"cmp"
	"context"
	"sync"

	enumspb "go.temporal.io/api/enums/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/sdk/activity"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/worker_versioning"
)

type (
	Activities struct {
		activityDeps
		namespace *namespace.Namespace
	}
)

func (a *Activities) SyncWorkerDeploymentVersion(ctx context.Context, args *deploymentspb.SyncVersionStateActivityArgs) (*deploymentspb.SyncVersionStateActivityResult, error) {
	identity := "worker-deployment workflow " + activity.GetInfo(ctx).WorkflowExecution.ID
	res, err := a.WorkerDeploymentClient.SyncVersionWorkflowFromWorkerDeployment(
		ctx,
		a.namespace,
		args.DeploymentName,
		args.Version,
		args.UpdateArgs,
		identity,
		args.RequestId,
	)
	if err != nil {
		return nil, err
	}
	return &deploymentspb.SyncVersionStateActivityResult{
		//nolint:staticcheck // SA1019
		VersionState: res.GetVersionState(),
		Summary:      res.GetSummary(),
	}, nil
}

func (a *Activities) SyncUnversionedRamp(
	ctx context.Context,
	input *deploymentspb.SyncUnversionedRampActivityArgs,
) (*deploymentspb.SyncDeploymentVersionUserDataResponse, error) {
	logger := activity.GetLogger(ctx)
	// Get all the task queues in the current version and put them into SyncUserData format
	currVersionInfo, _, err := a.WorkerDeploymentClient.DescribeVersion(ctx, a.namespace, input.CurrentVersion, false)
	if err != nil {
		return nil, err
	}
	data := &deploymentspb.DeploymentVersionData{
		Version:           nil,
		RoutingUpdateTime: input.UpdateArgs.RoutingUpdateTime,
		RampingSinceTime:  input.UpdateArgs.RampingSinceTime,
		RampPercentage:    input.UpdateArgs.RampPercentage,
	}
	var taskQueueSyncs []*deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData
	for _, tqInfo := range currVersionInfo.GetTaskQueueInfos() {
		taskQueueSyncs = append(taskQueueSyncs, &deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData{
			Name:  tqInfo.GetName(),
			Types: []enumspb.TaskQueueType{tqInfo.GetType()},
			Data:  data,
		})
	}

	// For each task queue, sync the unversioned ramp data
	errs := make(chan error)
	var lock sync.Mutex
	maxVersionByTQName := make(map[string]int64)
	for _, e := range taskQueueSyncs {
		go func(syncData *deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData) {
			logger.Info("syncing unversioned ramp to task queue userdata", "taskQueue", syncData.Name, "types", syncData.Types)
			var res *matchingservice.SyncDeploymentUserDataResponse
			var err error
			res, err = a.MatchingClient.SyncDeploymentUserData(ctx, &matchingservice.SyncDeploymentUserDataRequest{
				NamespaceId:    a.namespace.ID().String(),
				TaskQueue:      syncData.Name,
				TaskQueueTypes: syncData.Types,
				Operation:      &matchingservice.SyncDeploymentUserDataRequest_UpdateVersionData{UpdateVersionData: syncData.Data},
			})
			if err != nil {
				logger.Error("syncing task queue userdata", "taskQueue", syncData.Name, "types", syncData.Types, "error", err)
			} else {
				lock.Lock()
				maxVersionByTQName[syncData.Name] = max(maxVersionByTQName[syncData.Name], res.Version)
				lock.Unlock()
			}
			errs <- err
		}(e)
	}
	for range taskQueueSyncs {
		err = cmp.Or(err, <-errs)
	}
	if err != nil {
		return nil, err
	}
	return &deploymentspb.SyncDeploymentVersionUserDataResponse{TaskQueueMaxVersions: maxVersionByTQName}, nil
}

func (a *Activities) CheckUnversionedRampUserDataPropagation(ctx context.Context, input *deploymentspb.CheckWorkerDeploymentUserDataPropagationRequest) error {
	logger := activity.GetLogger(ctx)
	errs := make(chan error)
	for n, v := range input.TaskQueueMaxVersions {
		go func(name string, version int64) {
			logger.Info("waiting for unversioned ramp userdata propagation", "taskQueue", name, "version", version)
			_, err := a.MatchingClient.CheckTaskQueueUserDataPropagation(ctx, &matchingservice.CheckTaskQueueUserDataPropagationRequest{
				NamespaceId: a.namespace.ID().String(),
				TaskQueue:   name,
				Version:     version,
			})
			if err != nil {
				logger.Error("waiting for unversioned ramp userdata propagation", "taskQueue", name, "type", version, "error", err)
			}
			errs <- err
		}(n, v)
	}
	var err error
	for range input.TaskQueueMaxVersions {
		err = cmp.Or(err, <-errs)
	}
	return err
}

func (a *Activities) IsVersionMissingTaskQueues(ctx context.Context, args *deploymentspb.IsVersionMissingTaskQueuesArgs) (*deploymentspb.IsVersionMissingTaskQueuesResult, error) {
	res, err := a.WorkerDeploymentClient.IsVersionMissingTaskQueues(
		ctx,
		a.namespace,
		args.PrevCurrentVersion,
		args.NewCurrentVersion,
	)
	if err != nil {
		return nil, err
	}
	return &deploymentspb.IsVersionMissingTaskQueuesResult{
		IsMissingTaskQueues: res,
	}, nil
}

func (a *Activities) DeleteWorkerDeploymentVersion(ctx context.Context, args *deploymentspb.DeleteVersionActivityArgs) error {
	identity := "worker-deployment workflow " + activity.GetInfo(ctx).WorkflowExecution.ID
	versionObj, err := worker_versioning.WorkerDeploymentVersionFromStringV31(args.Version)
	if err != nil {
		return err
	}

	workflowID := GenerateVersionWorkflowID(args.DeploymentName, versionObj.GetBuildId())
	updatePayload, err := sdk.PreferProtoDataConverter.ToPayloads(&deploymentspb.DeleteVersionArgs{
		Identity:         identity,
		Version:          args.Version,
		SkipDrainage:     args.SkipDrainage,
		AsyncPropagation: args.AsyncPropagation,
	})
	if err != nil {
		return err
	}

	outcome, err := updateWorkflow(
		ctx,
		a.HistoryClient,
		a.namespace,
		workflowID,
		&updatepb.Request{
			Input: &updatepb.Input{Name: DeleteVersion, Args: updatePayload},
			Meta:  &updatepb.Meta{UpdateId: args.RequestId, Identity: identity},
		},
	)
	if err != nil {
		return err
	}

	err = extractApplicationErrorOrInternal(outcome.GetFailure())
	if err != nil {
		return err
	}
	return nil
}

func (a *Activities) RegisterWorkerInVersion(ctx context.Context, args *deploymentspb.RegisterWorkerInVersionArgs) error {
	identity := "worker-deployment workflow " + activity.GetInfo(ctx).WorkflowExecution.ID
	err := a.WorkerDeploymentClient.RegisterWorkerInVersion(
		ctx,
		a.namespace,
		args,
		identity,
	)
	if err != nil {
		return err
	}
	return nil
}

func (a *Activities) DescribeVersionFromWorkerDeployment(ctx context.Context, args *deploymentspb.DescribeVersionFromWorkerDeploymentActivityArgs) (*deploymentspb.DescribeVersionFromWorkerDeploymentActivityResult, error) {
	res, _, err := a.WorkerDeploymentClient.DescribeVersion(ctx, a.namespace, args.Version, false)
	if err != nil {
		return nil, err
	}
	return &deploymentspb.DescribeVersionFromWorkerDeploymentActivityResult{
		TaskQueueInfos: res.TaskQueueInfos,
	}, nil
}

func (a *Activities) SyncDeploymentVersionUserDataFromWorkerDeployment(
	ctx context.Context,
	input *deploymentspb.SyncDeploymentVersionUserDataRequest,
) (*deploymentspb.SyncDeploymentVersionUserDataResponse, error) {
	logger := activity.GetLogger(ctx)

	errs := make(chan error)

	var lock sync.Mutex
	maxVersionByName := make(map[string]int64)

	for _, e := range input.Sync {
		go func(syncData *deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData) {
			logger.Info("syncing task queue userdata for deployment version", "taskQueue", syncData.Name, "types", syncData.Types)

			var res *matchingservice.SyncDeploymentUserDataResponse
			var err error

			if input.ForgetVersion {
				res, err = a.MatchingClient.SyncDeploymentUserData(ctx, &matchingservice.SyncDeploymentUserDataRequest{
					NamespaceId:    a.namespace.ID().String(),
					DeploymentName: input.GetDeploymentName(),
					TaskQueue:      syncData.Name,
					TaskQueueTypes: syncData.Types,
					Operation: &matchingservice.SyncDeploymentUserDataRequest_ForgetVersion{
						ForgetVersion: input.Version,
					},
				})
			} else {
				res, err = a.MatchingClient.SyncDeploymentUserData(ctx, &matchingservice.SyncDeploymentUserDataRequest{
					NamespaceId:    a.namespace.ID().String(),
					DeploymentName: input.GetDeploymentName(),
					TaskQueue:      syncData.Name,
					TaskQueueTypes: syncData.Types,
					Operation: &matchingservice.SyncDeploymentUserDataRequest_UpdateVersionData{
						UpdateVersionData: syncData.Data,
					},
				})
			}

			if err != nil {
				logger.Error("syncing task queue userdata", "taskQueue", syncData.Name, "types", syncData.Types, "error", err)
			} else {
				lock.Lock()
				maxVersionByName[syncData.Name] = max(maxVersionByName[syncData.Name], res.Version)
				lock.Unlock()
			}
			errs <- err
		}(e)
	}

	var err error
	for range input.Sync {
		err = cmp.Or(err, <-errs)
	}
	if err != nil {
		return nil, err
	}
	return &deploymentspb.SyncDeploymentVersionUserDataResponse{TaskQueueMaxVersions: maxVersionByName}, nil
}

func (a *Activities) StartWorkerDeploymentVersionWorkflow(
	ctx context.Context,
	input *deploymentspb.StartWorkerDeploymentVersionRequest,
) error {
	logger := activity.GetLogger(ctx)
	logger.Info("starting worker deployment version workflow", "deploymentName", input.DeploymentName, "buildID", input.BuildId)
	identity := "deployment workflow " + activity.GetInfo(ctx).WorkflowExecution.ID
	return a.WorkerDeploymentClient.StartWorkerDeploymentVersion(ctx, a.namespace, input.DeploymentName, input.BuildId, identity, input.RequestId)
}
