package hooks

import (
	"context"

	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/namespace"
)

type (
	TaskAddHookDetails struct {
		Namespace               *namespace.Namespace
		TaskQueueName           string
		TaskQueueType           enumspb.TaskQueueType
		DeploymentVersion       *deploymentpb.WorkerDeploymentVersion
		IsSyncMatch             bool
		TaskQueueUserData       *persistencespb.TaskQueueTypeUserData
		TaskQueueStatsRetriever func() *taskqueuepb.TaskQueueStats
	}
	TaskAddHook interface {
		ProcessTaskAdd(ctx context.Context, event *TaskAddHookDetails)
	}
)
