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
	"google.golang.org/protobuf/proto"
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
		args.GetDeploymentName(),
		args.GetVersion(),
		args.GetUpdateArgs(),
		identity,
		args.GetRequestId(),
	)
	if err != nil {
		return nil, err
	}
	return deploymentspb.SyncVersionStateActivityResult_builder{
		//nolint:staticcheck // SA1019
		VersionState: res.GetVersionState(),
		Summary:      res.GetSummary(),
	}.Build(), nil
}

func (a *Activities) SyncUnversionedRamp(
	ctx context.Context,
	input *deploymentspb.SyncUnversionedRampActivityArgs,
) (*deploymentspb.SyncDeploymentVersionUserDataResponse, error) {
	logger := activity.GetLogger(ctx)
	// Get all the task queues in the current version and put them into SyncUserData format
	currVersionInfo, _, err := a.WorkerDeploymentClient.DescribeVersion(ctx, a.namespace, input.GetCurrentVersion(), false)
	if err != nil {
		return nil, err
	}
	data := deploymentspb.DeploymentVersionData_builder{
		Version:           nil,
		RoutingUpdateTime: input.GetUpdateArgs().GetRoutingUpdateTime(),
		RampingSinceTime:  input.GetUpdateArgs().GetRampingSinceTime(),
		RampPercentage:    input.GetUpdateArgs().GetRampPercentage(),
	}.Build()
	var taskQueueSyncs []*deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData
	for _, tqInfo := range currVersionInfo.GetTaskQueueInfos() {
		taskQueueSyncs = append(taskQueueSyncs, deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData_builder{
			Name:  tqInfo.GetName(),
			Types: []enumspb.TaskQueueType{tqInfo.GetType()},
			Data:  data,
		}.Build())
	}

	// For each task queue, sync the unversioned ramp data
	errs := make(chan error)
	var lock sync.Mutex
	maxVersionByTQName := make(map[string]int64)
	for _, e := range taskQueueSyncs {
		go func(syncData *deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData) {
			logger.Info("syncing unversioned ramp to task queue userdata", "taskQueue", syncData.GetName(), "types", syncData.GetTypes())
			var res *matchingservice.SyncDeploymentUserDataResponse
			var err error
			res, err = a.MatchingClient.SyncDeploymentUserData(ctx, matchingservice.SyncDeploymentUserDataRequest_builder{
				NamespaceId:       a.namespace.ID().String(),
				TaskQueue:         syncData.GetName(),
				TaskQueueTypes:    syncData.GetTypes(),
				UpdateVersionData: proto.ValueOrDefault(syncData.GetData()),
			}.Build())
			if err != nil {
				logger.Error("syncing task queue userdata", "taskQueue", syncData.GetName(), "types", syncData.GetTypes(), "error", err)
			} else {
				lock.Lock()
				maxVersionByTQName[syncData.GetName()] = max(maxVersionByTQName[syncData.GetName()], res.GetVersion())
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
	return deploymentspb.SyncDeploymentVersionUserDataResponse_builder{TaskQueueMaxVersions: maxVersionByTQName}.Build(), nil
}

func (a *Activities) CheckUnversionedRampUserDataPropagation(ctx context.Context, input *deploymentspb.CheckWorkerDeploymentUserDataPropagationRequest) error {
	logger := activity.GetLogger(ctx)
	errs := make(chan error)
	for n, v := range input.GetTaskQueueMaxVersions() {
		go func(name string, version int64) {
			logger.Info("waiting for unversioned ramp userdata propagation", "taskQueue", name, "version", version)
			_, err := a.MatchingClient.CheckTaskQueueUserDataPropagation(ctx, matchingservice.CheckTaskQueueUserDataPropagationRequest_builder{
				NamespaceId: a.namespace.ID().String(),
				TaskQueue:   name,
				Version:     version,
			}.Build())
			if err != nil {
				logger.Error("waiting for unversioned ramp userdata propagation", "taskQueue", name, "type", version, "error", err)
			}
			errs <- err
		}(n, v)
	}
	var err error
	for range input.GetTaskQueueMaxVersions() {
		err = cmp.Or(err, <-errs)
	}
	return err
}

func (a *Activities) IsVersionMissingTaskQueues(ctx context.Context, args *deploymentspb.IsVersionMissingTaskQueuesArgs) (*deploymentspb.IsVersionMissingTaskQueuesResult, error) {
	res, err := a.WorkerDeploymentClient.IsVersionMissingTaskQueues(
		ctx,
		a.namespace,
		args.GetPrevCurrentVersion(),
		args.GetNewCurrentVersion(),
	)
	if err != nil {
		return nil, err
	}
	return deploymentspb.IsVersionMissingTaskQueuesResult_builder{
		IsMissingTaskQueues: res,
	}.Build(), nil
}

func (a *Activities) DeleteWorkerDeploymentVersion(ctx context.Context, args *deploymentspb.DeleteVersionActivityArgs) error {
	identity := "worker-deployment workflow " + activity.GetInfo(ctx).WorkflowExecution.ID
	versionObj, err := worker_versioning.WorkerDeploymentVersionFromStringV31(args.GetVersion())
	if err != nil {
		return err
	}

	workflowID := GenerateVersionWorkflowID(args.GetDeploymentName(), versionObj.GetBuildId())
	updatePayload, err := sdk.PreferProtoDataConverter.ToPayloads(deploymentspb.DeleteVersionArgs_builder{
		Identity:         identity,
		Version:          args.GetVersion(),
		SkipDrainage:     args.GetSkipDrainage(),
		AsyncPropagation: args.GetAsyncPropagation(),
	}.Build())
	if err != nil {
		return err
	}

	outcome, err := updateWorkflow(
		ctx,
		a.HistoryClient,
		a.namespace,
		workflowID,
		updatepb.Request_builder{
			Input: updatepb.Input_builder{Name: DeleteVersion, Args: updatePayload}.Build(),
			Meta:  updatepb.Meta_builder{UpdateId: args.GetRequestId(), Identity: identity}.Build(),
		}.Build(),
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
	res, _, err := a.WorkerDeploymentClient.DescribeVersion(ctx, a.namespace, args.GetVersion(), false)
	if err != nil {
		return nil, err
	}
	return deploymentspb.DescribeVersionFromWorkerDeploymentActivityResult_builder{
		TaskQueueInfos: res.GetTaskQueueInfos(),
	}.Build(), nil
}

func (a *Activities) SyncDeploymentVersionUserDataFromWorkerDeployment(
	ctx context.Context,
	input *deploymentspb.SyncDeploymentVersionUserDataRequest,
) (*deploymentspb.SyncDeploymentVersionUserDataResponse, error) {
	logger := activity.GetLogger(ctx)

	errs := make(chan error)

	var lock sync.Mutex
	maxVersionByName := make(map[string]int64)

	for _, e := range input.GetSync() {
		go func(syncData *deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData) {
			logger.Info("syncing task queue userdata for deployment version", "taskQueue", syncData.GetName(), "types", syncData.GetTypes())

			var res *matchingservice.SyncDeploymentUserDataResponse
			var err error

			if input.GetForgetVersion() {
				res, err = a.MatchingClient.SyncDeploymentUserData(ctx, matchingservice.SyncDeploymentUserDataRequest_builder{
					NamespaceId:    a.namespace.ID().String(),
					DeploymentName: input.GetDeploymentName(),
					TaskQueue:      syncData.GetName(),
					TaskQueueTypes: syncData.GetTypes(),
					ForgetVersion:  proto.ValueOrDefault(input.GetVersion()),
				}.Build())
			} else {
				res, err = a.MatchingClient.SyncDeploymentUserData(ctx, matchingservice.SyncDeploymentUserDataRequest_builder{
					NamespaceId:       a.namespace.ID().String(),
					DeploymentName:    input.GetDeploymentName(),
					TaskQueue:         syncData.GetName(),
					TaskQueueTypes:    syncData.GetTypes(),
					UpdateVersionData: proto.ValueOrDefault(syncData.GetData()),
				}.Build())
			}

			if err != nil {
				logger.Error("syncing task queue userdata", "taskQueue", syncData.GetName(), "types", syncData.GetTypes(), "error", err)
			} else {
				lock.Lock()
				maxVersionByName[syncData.GetName()] = max(maxVersionByName[syncData.GetName()], res.GetVersion())
				lock.Unlock()
			}
			errs <- err
		}(e)
	}

	var err error
	for range input.GetSync() {
		err = cmp.Or(err, <-errs)
	}
	if err != nil {
		return nil, err
	}
	return deploymentspb.SyncDeploymentVersionUserDataResponse_builder{TaskQueueMaxVersions: maxVersionByName}.Build(), nil
}

func (a *Activities) StartWorkerDeploymentVersionWorkflow(
	ctx context.Context,
	input *deploymentspb.StartWorkerDeploymentVersionRequest,
) error {
	logger := activity.GetLogger(ctx)
	logger.Info("starting worker deployment version workflow", "deploymentName", input.GetDeploymentName(), "buildID", input.GetBuildId())
	identity := "deployment workflow " + activity.GetInfo(ctx).WorkflowExecution.ID
	return a.WorkerDeploymentClient.StartWorkerDeploymentVersion(ctx, a.namespace, input.GetDeploymentName(), input.GetBuildId(), identity, input.GetRequestId())
}
