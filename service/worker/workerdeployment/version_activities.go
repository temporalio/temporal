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
	"google.golang.org/protobuf/proto"
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
	logger.Info("starting worker-deployment workflow", "deploymentName", input.GetDeploymentName())
	identity := "deployment-version workflow " + activity.GetInfo(ctx).WorkflowExecution.ID
	err := a.WorkerDeploymentClient.StartWorkerDeployment(ctx, a.namespace, input.GetDeploymentName(), identity, input.GetRequestId())
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

	for _, e := range input.GetSync() {
		go func(syncData *deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData) {
			logger.Info("syncing task queue userdata for deployment version", "taskQueue", syncData.GetName(), "types", syncData.GetTypes())

			var res *matchingservice.SyncDeploymentUserDataResponse
			var err error

			req := matchingservice.SyncDeploymentUserDataRequest_builder{
				NamespaceId:         a.namespace.ID().String(),
				DeploymentName:      input.GetVersion().GetDeploymentName(),
				TaskQueue:           syncData.GetName(),
				TaskQueueTypes:      syncData.GetTypes(),
				UpdateRoutingConfig: input.GetUpdateRoutingConfig(),
			}.Build()

			if input.GetForgetVersion() {
				// TODO: Remove the old format once async apis are fully enabled
				req.SetForgetVersion(proto.ValueOrDefault(input.GetVersion()))
				req.SetForgetVersions([]string{input.GetVersion().GetBuildId()})
			} else if syncData.HasData() {
				req.SetUpdateVersionData(proto.ValueOrDefault(syncData.GetData()))
			}

			if vd := input.GetUpsertVersionData(); vd != nil {
				req.SetUpsertVersionsData(make(map[string]*deploymentspb.WorkerDeploymentVersionData, 1))
				req.GetUpsertVersionsData()[input.GetVersion().GetBuildId()] = vd
			}

			res, err = a.MatchingClient.SyncDeploymentUserData(ctx, req)

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

	for n, v := range input.GetTaskQueueMaxVersions() {
		go func(name string, version int64) {
			logger.Info("waiting for userdata propagation", "taskQueue", name, "version", version)
			_, err := a.MatchingClient.CheckTaskQueueUserDataPropagation(ctx, matchingservice.CheckTaskQueueUserDataPropagationRequest_builder{
				NamespaceId: a.namespace.ID().String(),
				TaskQueue:   name,
				Version:     version,
			}.Build())
			if err != nil {
				logger.Error("waiting for userdata", "taskQueue", name, "type", version, "error", err)
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

// CheckIfTaskQueuesHavePollers returns true if any of the given task queues has any pollers
func (a *VersionActivities) CheckIfTaskQueuesHavePollers(ctx context.Context, args *deploymentspb.CheckTaskQueuesHavePollersActivityArgs) (bool, error) {
	versionStr := worker_versioning.ExternalWorkerDeploymentVersionToString(worker_versioning.ExternalWorkerDeploymentVersionFromVersion(args.GetWorkerDeploymentVersion()))
	for tqName, tqTypes := range args.GetTaskQueuesAndTypes() {
		res, err := a.MatchingClient.DescribeTaskQueue(ctx, matchingservice.DescribeTaskQueueRequest_builder{
			NamespaceId: a.namespace.ID().String(),
			DescRequest: workflowservice.DescribeTaskQueueRequest_builder{
				Namespace:      a.namespace.Name().String(),
				TaskQueue:      taskqueuepb.TaskQueue_builder{Name: tqName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
				ApiMode:        enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED,
				Versions:       taskqueuepb.TaskQueueVersionSelection_builder{BuildIds: []string{versionStr}}.Build(),
				ReportPollers:  true,
				TaskQueueType:  enumspb.TASK_QUEUE_TYPE_WORKFLOW,
				TaskQueueTypes: tqTypes.GetTypes(),
			}.Build(),
		}.Build())
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
	return deploymentpb.VersionDrainageInfo_builder{
		Status:          response,
		LastChangedTime: nil, // ignored; whether Status changed will be evaluated by the receiver
		LastCheckedTime: timestamppb.Now(),
	}.Build(), nil
}
