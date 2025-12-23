package workerdeployment

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/worker_versioning"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const SlowPropagationDelay = 10 * time.Second

type (
	VersionActivities struct {
		activityDeps
		namespace *namespace.Namespace
	}
)

func (a *VersionActivities) StartWorkerDeploymentWorkflow(
	ctx context.Context,
	input *deploymentspb.StartWorkerDeploymentRequest,
) error {
	logger := activity.GetLogger(ctx)
	logger.Info("starting worker-deployment workflow", "deploymentName", input.DeploymentName)
	identity := "deployment-version workflow " + activity.GetInfo(ctx).WorkflowExecution.ID
	err := a.WorkerDeploymentClient.StartWorkerDeployment(ctx, a.namespace, input.DeploymentName, identity, input.RequestId)
	var precond *serviceerror.FailedPrecondition
	if errors.As(err, &precond) {
		return temporal.NewNonRetryableApplicationError("failed to create deployment", errTooManyDeployments, err)
	}
	return err
}

func (a *VersionActivities) SyncDeploymentVersionUserData(
	ctx context.Context,
	input *deploymentspb.SyncDeploymentVersionUserDataRequest,
) (*deploymentspb.SyncDeploymentVersionUserDataResponse, error) {
	logger := activity.GetLogger(ctx)
	defer a.checkSlowPropagation(ctx, logger)

	errs := make(chan error)

	var lock sync.Mutex
	maxVersionByName := make(map[string]int64)

	for _, e := range input.Sync {
		go func(syncData *deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData) {
			logger.Info("syncing task queue userdata for deployment version", "taskQueue", syncData.Name, "types", syncData.Types)

			var res *matchingservice.SyncDeploymentUserDataResponse
			var err error

			req := &matchingservice.SyncDeploymentUserDataRequest{
				NamespaceId:         a.namespace.ID().String(),
				DeploymentName:      input.GetVersion().GetDeploymentName(),
				TaskQueue:           syncData.Name,
				TaskQueueTypes:      syncData.Types,
				UpdateRoutingConfig: input.GetUpdateRoutingConfig(),
			}

			if input.ForgetVersion {
				// TODO: Remove the old format once async apis are fully enabled
				req.Operation = &matchingservice.SyncDeploymentUserDataRequest_ForgetVersion{
					ForgetVersion: input.Version,
				}
				req.ForgetVersions = []string{input.GetVersion().GetBuildId()}
			} else if syncData.Data != nil {
				req.Operation = &matchingservice.SyncDeploymentUserDataRequest_UpdateVersionData{
					UpdateVersionData: syncData.Data,
				}
			}

			if vd := input.GetUpsertVersionData(); vd != nil {
				req.UpsertVersionsData = make(map[string]*deploymentspb.WorkerDeploymentVersionData, 1)
				req.UpsertVersionsData[input.GetVersion().GetBuildId()] = vd
			}

			res, err = a.MatchingClient.SyncDeploymentUserData(ctx, req)

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

func (a *VersionActivities) checkSlowPropagation(ctx context.Context, logger log.Logger) {
	firstAttemptScheduledTime := activity.GetInfo(ctx).ScheduledTime
	if firstAttemptScheduledTime.Add(SlowPropagationDelay).Before(time.Now()) {
		logger.Warn("Slow propagation detected", "duration", time.Since(firstAttemptScheduledTime))
		a.MetricsHandler.Counter(metrics.SlowVersioningDataPropagationCounter.Name()).Record(1)
	}
}

func (a *VersionActivities) CheckWorkerDeploymentUserDataPropagation(ctx context.Context, input *deploymentspb.CheckWorkerDeploymentUserDataPropagationRequest) error {
	logger := activity.GetLogger(ctx)
	defer a.checkSlowPropagation(ctx, logger)

	errs := make(chan error)

	for n, v := range input.TaskQueueMaxVersions {
		go func(name string, version int64) {
			logger.Info("waiting for userdata propagation", "taskQueue", name, "version", version)
			_, err := a.MatchingClient.CheckTaskQueueUserDataPropagation(ctx, &matchingservice.CheckTaskQueueUserDataPropagationRequest{
				NamespaceId: a.namespace.ID().String(),
				TaskQueue:   name,
				Version:     version,
			})
			if err != nil {
				logger.Error("waiting for userdata", "taskQueue", name, "type", version, "error", err)
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

// CheckIfTaskQueuesHavePollers returns true if any of the given task queues has any pollers
func (a *VersionActivities) CheckIfTaskQueuesHavePollers(ctx context.Context, args *deploymentspb.CheckTaskQueuesHavePollersActivityArgs) (bool, error) {
	versionStr := worker_versioning.ExternalWorkerDeploymentVersionToString(worker_versioning.ExternalWorkerDeploymentVersionFromVersion(args.WorkerDeploymentVersion))
	for tqName, tqTypes := range args.TaskQueuesAndTypes {
		res, err := a.MatchingClient.DescribeTaskQueue(ctx, &matchingservice.DescribeTaskQueueRequest{
			NamespaceId: a.namespace.ID().String(),
			DescRequest: &workflowservice.DescribeTaskQueueRequest{
				Namespace:      a.namespace.Name().String(),
				TaskQueue:      &taskqueuepb.TaskQueue{Name: tqName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				ApiMode:        enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED,
				Versions:       &taskqueuepb.TaskQueueVersionSelection{BuildIds: []string{versionStr}},
				ReportPollers:  true,
				TaskQueueType:  enumspb.TASK_QUEUE_TYPE_WORKFLOW,
				TaskQueueTypes: tqTypes.Types,
			},
		})
		if err != nil {
			return false, fmt.Errorf("error describing task queue with name %s: %s", tqName, err)
		}
		typesInfo := res.GetDescResponse().GetVersionsInfo()[versionStr].GetTypesInfo()
		if len(typesInfo[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].GetPollers()) > 0 {
			return true, nil
		}
		if len(typesInfo[int32(enumspb.TASK_QUEUE_TYPE_ACTIVITY)].GetPollers()) > 0 {
			return true, nil
		}
		if len(typesInfo[int32(enumspb.TASK_QUEUE_TYPE_NEXUS)].GetPollers()) > 0 {
			return true, nil
		}
	}
	return false, nil
}

func (a *VersionActivities) GetVersionDrainageStatus(ctx context.Context, version *deploymentspb.WorkerDeploymentVersion) (*deploymentpb.VersionDrainageInfo, error) {
	logger := activity.GetLogger(ctx)
	response, err := a.WorkerDeploymentClient.GetVersionDrainageStatus(ctx, a.namespace, worker_versioning.WorkerDeploymentVersionToStringV31(version))
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
